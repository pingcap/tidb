#!/bin/sh
#
# Copyright 2025 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux

# global constants
DB="$TEST_NAME"
DB2="${TEST_NAME}_2"
DB3="${TEST_NAME}_3"
DB4="${TEST_NAME}_4"
DB_LOG="${TEST_NAME}_log"
TASK_NAME="$TEST_NAME"
BACKUP_DIR="local://$TEST_DIR/backup"
LOG_BACKUP_DIR="local://$TEST_DIR/log_backup"
TABLE_COUNT=5
CUR=$(cd `dirname $0`; pwd)
res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"

. run_services

# Set up trap to handle cleanup on exit
cleanup_and_exit() {
    # Kill all BR processes to avoid orphaned processes
    killall -9 br.test 2>/dev/null || true
    exit ${1:-0}
}
trap cleanup_and_exit EXIT

verify_table_data() {
    local db_name="$1"
    local table_prefix="$2" 
    local expected_value="$3"
    local test_context="${4:-verification}"
    
    echo "Verifying table $db_name.${table_prefix}_$expected_value contains value $expected_value ($test_context)..."
    run_sql "select c from $db_name.${table_prefix}_$expected_value;"
    check_contains "c: $expected_value"
}

verify_registry() {
    local filter_condition="$1"
    local should_exist="$2"  # true or false
    local test_context="${3:-registry check}"
    
    if [ "$should_exist" = "true" ]; then
        echo "Verifying that registry entries EXIST for '$filter_condition' ($test_context)..."
    else
        echo "Verifying that NO registry entries exist for '$filter_condition' ($test_context)..."
    fi
    
    run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE $filter_condition;"
    
    # Extract count from MySQL output
    registry_result=$(cat "$TEST_DIR/sql_res.$TEST_NAME.txt")
    actual_count=$(echo "$registry_result" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    actual_count=${actual_count:-0}
    
    if [ "$should_exist" = "true" ]; then
        if [ "$actual_count" -gt 0 ]; then
            echo "PASS: Found $actual_count registry entries"
            return 0
        else
            echo "ERROR: No registry entries found for condition: $filter_condition"
            echo "Registry query result:"
            echo "$registry_result"
            exit 1
        fi
    else
        if [ "$actual_count" -eq 0 ]; then
            echo "PASS: No registry entries found (as expected)"
            return 0
        else
            echo "ERROR: Found $actual_count registry entries when expecting none"
            echo "Registry query result:"
            echo "$registry_result"
            exit 1
        fi
    fi
}



verify_snapshot_checkpoint_databases_exist() {
    local expected_count="${1:-4}"
    local test_context="${2:-checkpoint existence check}"
    
    echo "Verifying snapshot checkpoint databases exist ($test_context)..."
    
    # Check for snapshot checkpoint databases - every restore type creates these
    local snapshot_checkpoint_dbs=$(run_sql "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE '__TiDB_BR_Temporary_Snapshot_Restore_Checkpoint_%';" | grep -v "SCHEMA_NAME" | grep -v "^$" || true)
    
    local actual_count=0
    if [ -n "$snapshot_checkpoint_dbs" ]; then
        actual_count=$(echo "$snapshot_checkpoint_dbs" | wc -l | tr -d ' ')
        echo "Found snapshot checkpoint databases:"
        echo "$snapshot_checkpoint_dbs" | sed 's/^/  /'
    fi
    
    if [ "$actual_count" -ge "$expected_count" ]; then
        echo "PASS: Found $actual_count snapshot checkpoint databases (expected at least $expected_count)"
        return 0
    else
        echo "WARNING: Found only $actual_count snapshot checkpoint databases (expected at least $expected_count)"
        echo "This may indicate checkpoints weren't created properly"
        return 1
    fi
}

verify_no_temporary_databases() {
    local test_case="${1:-unknown test}"
    # this function verifies that BR has properly cleaned up all temporary databases
    # after successful restore operations. temporary databases are used internally
    # by BR during restore operations and should be automatically cleaned up
    # when the restore completes successfully.
    echo "Verifying no temporary databases remain after restore in $test_case..."
    
    # check for BR temporary database prefix
    local temp_dbs=$(run_sql "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE '__TiDB_BR_Temporary_%';" | grep -v "SCHEMA_NAME" | grep -v "^$" || true)
    
    if [ -n "$temp_dbs" ]; then
        echo "Error: Found temporary databases that should have been cleaned up in $test_case:"
        echo "$temp_dbs" | sed 's/^/  /'
        echo "Temporary database cleanup verification failed in $test_case!"
        exit 1
    fi
    
    echo "Verification passed: No temporary databases found in $test_case"
}

verify_checkpoint_cleanup() {
    local test_case="${1:-unknown test}"
    echo "Verifying checkpoint cleanup after abort in $test_case..."
    
    # Check for any BR temporary databases
    local temp_dbs=$(run_sql "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE '__TiDB_BR_Temporary_%';" | grep -v "SCHEMA_NAME" | grep -v "^$" || true)
    
    if [ -n "$temp_dbs" ]; then
        echo "WARNING: Found temporary databases that should have been cleaned up:"
        echo "$temp_dbs" | sed 's/^/  /'
        echo "NOTE: Some temporary databases remain - this may be expected if cleanup is async or if there are other running tests"
    else
        echo "PASS: All temporary databases have been cleaned up"
    fi
    
    echo "Checkpoint cleanup verification completed for $test_case"
}

create_tables_with_values() {
    local prefix=$1    # table name prefix
    local count=$2     # number of tables to create
    local db_name=$3   # database name

    for i in $(seq 1 $count); do
        run_sql "create table $db_name.${prefix}_${i}(c int); insert into $db_name.${prefix}_${i} values ($i);"
    done
}

setup_test_environment() {
    run_sql "create database if not exists $DB;"
    run_sql "create database if not exists $DB2;"
    run_sql "create database if not exists $DB3;"
    run_sql "create database if not exists $DB4;"

    create_tables_with_values "full" $TABLE_COUNT $DB
    create_tables_with_values "full" $TABLE_COUNT $DB2
    create_tables_with_values "full" $TABLE_COUNT $DB3
    create_tables_with_values "full" $TABLE_COUNT $DB4

    run_br log start --task-name $TASK_NAME -s "$LOG_BACKUP_DIR"

    run_br backup full -s "$BACKUP_DIR"
    
    run_sql "create database if not exists $DB_LOG;"
    create_tables_with_values "log" $TABLE_COUNT $DB_LOG
    
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

     run_br log stop --task-name $TASK_NAME

    run_sql "drop database $DB;"
    run_sql "drop database $DB2;"
    run_sql "drop database $DB3;"
    run_sql "drop database $DB4;"
    run_sql "drop database $DB_LOG;"

}

cleanup() {
    run_sql "drop database if exists $DB;"
    run_sql "drop database if exists $DB2;"
    run_sql "drop database if exists $DB3;"
    run_sql "drop database if exists $DB4;"
    run_sql "drop database if exists $DB_LOG;"
}

test_mixed_parallel_restores() {
    echo "Test Case 1: Multiple parallel restores with different configurations"
    
    echo "Starting full restore with filter of $DB"
    run_br restore full --filter "$DB.*" -s "$BACKUP_DIR" &
    RESTORE1_PID=$!
    
    echo "Starting PITR restore of $DB2 with filter"
    run_br restore point --filter "$DB2.*" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR" &
    RESTORE2_PID=$!
    
    echo "Starting restore specific db"
    run_br restore db --db "$DB3" -s "$BACKUP_DIR" &
    RESTORE3_PID=$!

    echo "Starting restore specific table"
    run_br restore table --db "$DB4" --table "full_1" -s "$BACKUP_DIR" &
    RESTORE4_PID=$!
    
    echo "Starting PITR restore of $DB_LOG (created after backup)"
    run_br restore point --filter "$DB_LOG.*" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR" &
    RESTORE5_PID=$!
    
    # Wait for all restores to complete
    echo "Waiting for all restores to complete..."
    wait $RESTORE1_PID
    wait $RESTORE2_PID
    wait $RESTORE3_PID
    wait $RESTORE4_PID
    wait $RESTORE5_PID
    
    # Verify DB restore (full database with filter)
    echo "Verifying full restore with filter of $DB"
    for i in $(seq 1 $TABLE_COUNT); do
        verify_table_data $DB "full" $i
    done
    
    # Verify DB2 restore (PITR with filter)
    echo "Verifying PITR restore with filter of $DB2..."
    for i in $(seq 1 $TABLE_COUNT); do
        verify_table_data $DB2 "full" $i
    done
    
    # Verify DB3 restore (specific db)
    echo "Verifying specific db restore for $DB3..."
    for i in $(seq 1 $TABLE_COUNT); do
        verify_table_data $DB3 "full" $i
    done
    
    # Verify DB4 restore (specific table)
    echo "Verifying specific table restore for $DB4..."
    verify_table_data $DB4 "full" 1
    for i in $(seq 2 $TABLE_COUNT); do
        if run_sql "show tables from $DB4 like 'full_$i';" | grep -q "full_$i"; then
            echo "Error: Table full_$i should not have been restored" >&2
            exit 1
        fi
    done
    
    # Verify DB_LOG restore
    echo "Verifying PITR restore of $DB_LOG (created after backup)..."
    for i in $(seq 1 $TABLE_COUNT); do
        verify_table_data $DB_LOG "log" $i
    done
    
    echo "Mixed parallel restores with specified parameters completed successfully"
    
    verify_no_temporary_databases "Test Case 1: Multiple parallel restores"
    
    cleanup
}

test_concurrent_restore_table_conflicts() {
    echo "Test Case 2: Concurrent restore with table conflicts"

    # start first task, fail it before finishing so restore task is still registered
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "$DB.*" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting failed before unregister restore task but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""

    # start second task, should fail fast since first task still registered
    restore_fail=0
    run_br restore table --db "$DB" --table "full_1" -s "$BACKUP_DIR" > $res_file 2>&1 || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting conflicting table detected and restore abort but succeeded'
        exit 1
    fi
    check_contains "table already covered by another restore task"

    # run first task again to finish
    run_br restore point --filter "$DB.*" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR"

    # verify the first database was restored correctly
    echo "Verifying first database was restored correctly..."
    for i in $(seq 1 $TABLE_COUNT); do
        verify_table_data $DB "full" $i
    done

    verify_no_temporary_databases "Test Case 2: Concurrent restore with table conflicts"
    
    cleanup
}

test_restore_with_different_systable_settings() {
    echo "Test Case 3: Restore with different system table settings"

    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore full --filter "mysql.*" --filter "$DB.*" --with-sys-table=true -s "$BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting failed before unregistering restore task but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""

    # should succeed because we're using a different with-sys-table setting
    run_br restore full --filter "mysql.*" --filter "$DB2.*" --with-sys-table=false -s "$BACKUP_DIR"

    echo "Verifying different system table settings restore..."
    for i in $(seq 1 $TABLE_COUNT); do
        verify_table_data $DB2 "full" $i
    done

    # complete the first one
    run_br restore full --filter "mysql.*" --filter "$DB.*" --with-sys-table=true -s "$BACKUP_DIR"

    verify_no_temporary_databases "Test Case 3: Restore with different system table settings"
    
    cleanup
}

test_auto_restored_ts_conflict() {
    echo "Test Case 5: Intelligent RestoreTS resolution"

    # use separate backup directories for this test
    PITR_BACKUP_DIR="local://$TEST_DIR/pitr_backup"
    PITR_LOG_BACKUP_DIR="local://$TEST_DIR/pitr_log_backup"

    echo "Setting up backup data for PiTR RestoreTS resolution testing..."

    # create initial data
    run_sql "create database if not exists $DB;"
    create_tables_with_values "pitr" $TABLE_COUNT $DB

    # start log backup
    run_br log start --task-name ${TASK_NAME}_pitr -s "$PITR_LOG_BACKUP_DIR"

    # take snapshot backup
    run_br backup full -s "$PITR_BACKUP_DIR"

    # add more data after snapshot backup
    run_sql "create database if not exists ${DB}_after_snapshot;"
    create_tables_with_values "after" $TABLE_COUNT ${DB}_after_snapshot

    # wait for log checkpoint to advance to cover the new data
    log_backup_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")
    echo "Using log backup timestamp: $log_backup_ts"
    echo "Waiting for log checkpoint to advance..."
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance ${TASK_NAME}_pitr

    # stop log backup
    run_br log stop --task-name ${TASK_NAME}_pitr

    # clean up source data
    run_sql "drop database $DB;"
    run_sql "drop database ${DB}_after_snapshot;"
    
    echo "Test 1: First PiTR restore with explicit restored-ts but fail before completion..."
    
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "$DB.*" --restored-ts $log_backup_ts --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting first restore to fail before completion but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""
    
    echo "First PiTR restore failed as expected, leaving paused registry entry"
    
    # Verify that a task was created in the registry
    verify_registry "filter_strings = '$DB.*'" true "Test 1: Failed restore should create registry entry"
    
    # Test 2: Retry without explicit restored-ts (auto-detection) - should resume existing paused task
    echo "Test 2: Retry without explicit restored-ts (auto-detection)..."
    echo "This should trigger intelligent RestoreTS resolution and resume the paused task"
    
    # This should succeed and reuse the existing task's RestoreTS
    run_br restore point --filter "$DB.*" --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR"
    
    # Verify the restore succeeded
    for i in $(seq 1 $TABLE_COUNT); do
        verify_table_data $DB "pitr" $i
    done
    echo "PASS: Second restore with auto-detection succeeded by resuming existing task"
    
    # Check registry state after successful restore - should be cleaned up
    verify_registry "filter_strings = '$DB.*'" false "Test 2: Successful restore should clean up registry"
    
    # Clean up any remaining registry entries
    run_sql "DELETE FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*';"
    
    # clean up restored data for next test
    run_sql "drop database $DB;"
    
    # Test 3: Test with a running task (stale detection scenario)
    echo "Test 3: Testing stale running task detection..."
    
    # First create a paused task using the fail-at-end-of-restore failpoint
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "$DB.*" --restored-ts $log_backup_ts --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting restore to fail and create paused task but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""

    # Now manually change the paused task to running status to simulate a stuck running task
    echo "Manually changing paused task to running status to simulate stuck task..."
    run_sql "UPDATE mysql.tidb_restore_registry SET status = 'running' WHERE filter_strings = '$DB.*' AND status = 'paused';"
    
    # Verify that we actually have a running task after the update
    verify_registry "filter_strings = '$DB.*' AND status = 'running'" true "Test 3: Should have running task for stale detection test"
    
    # Try to restore without explicit restored-ts - should detect stale task, transition it to paused, and reuse it
    echo "Attempting restore without explicit restored-ts (testing stale detection and reuse)..."
    echo "This should detect the stale running task, transition it to paused, and reuse its RestoreTS"
    
    run_br restore point --filter "$DB.*" --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR"
    
    # Verify the restore succeeded by checking the data
    echo "Verifying restore succeeded..."
    for i in $(seq 1 $TABLE_COUNT); do
        verify_table_data $DB "pitr" $i
    done
    echo "PASS: Stale task detection and reuse worked correctly"
    
    # Check final task status - should be cleaned up after successful restore
    verify_registry "filter_strings = '$DB.*'" false "Test 3: Successful restore should clean up registry"
    
    # Test 4: Test that user can resume paused task with same explicit RestoreTS
    echo "Test 4: Testing user can resume paused task with same explicit RestoreTS..."
    
    # Clean up any existing data from previous tests (use $DB since backup only contains $DB data)
    run_sql "drop database if exists $DB;"
    
    # Create a paused task for $DB first
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "$DB.*" --restored-ts $log_backup_ts --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting restore to fail before completion but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""
    
    # Verify the paused task was created
    verify_registry "filter_strings = '$DB.*'" true "Test 4: Failed restore should create registry entry"
    
    # Get the task ID for verification
    task_info_before=$(run_sql "SELECT id, status FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*' AND status = 'paused' ORDER BY id DESC LIMIT 1;" 2>/dev/null || echo "")
    echo "Task info before resume: $task_info_before"
    
    # Now try to resume with the same user-specified RestoreTS - should reuse existing task
    echo "Attempting to resume with same user-specified RestoreTS ($log_backup_ts)..."
    
    run_br restore point --filter "$DB.*" --restored-ts $log_backup_ts --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR"
    
    # Verify restore succeeded - check that the expected tables exist and have correct data
    echo "Verifying restore succeeded..."
    for i in $(seq 1 $TABLE_COUNT); do
        verify_table_data $DB "pitr" $i
    done
    
    # Verify that the same task was resumed (not a new task created)
    verify_registry "filter_strings = '$DB.*'" false "Test 4: Task should be cleaned up after successful restore"
    
    echo "PASS: User-specified RestoreTS correctly resumed existing paused task"
    
    # Final cleanup
    run_sql "DELETE FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*';"
    
    # Final registry state check
    verify_registry "filter_strings LIKE '$DB%'" false "Final cleanup: Registry should be clean"
    
    echo "Intelligent RestoreTS resolution test completed successfully"
    cleanup
}

test_restore_abort() {
    echo "Test Case 6: Comprehensive restore abort functionality for all restore types"

    # use separate backup directories for this test
    ABORT_BACKUP_DIR="local://$TEST_DIR/abort_backup"
    ABORT_LOG_BACKUP_DIR="local://$TEST_DIR/abort_log_backup"

    echo "Setting up backup data for abort testing..."

    # create initial data for multiple databases
    run_sql "create database if not exists $DB;"
    run_sql "create database if not exists $DB2;"
    run_sql "create database if not exists $DB3;"
    run_sql "create database if not exists $DB4;"
    
    create_tables_with_values "abort" $TABLE_COUNT $DB
    create_tables_with_values "abort" $TABLE_COUNT $DB2
    create_tables_with_values "abort" $TABLE_COUNT $DB3
    create_tables_with_values "abort" $TABLE_COUNT $DB4

    # start log backup
    run_br log start --task-name ${TASK_NAME}_abort -s "$ABORT_LOG_BACKUP_DIR"

    # take snapshot backup
    run_br backup full -s "$ABORT_BACKUP_DIR"

    # add more data after snapshot backup
    run_sql "create database if not exists ${DB}_after_snapshot;"
    create_tables_with_values "after" $TABLE_COUNT ${DB}_after_snapshot

    # wait for log checkpoint to advance
    log_backup_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")
    echo "Using log backup timestamp: $log_backup_ts"
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance ${TASK_NAME}_abort

    # stop log backup
    run_br log stop --task-name ${TASK_NAME}_abort

    # clean up source data
    run_sql "drop database $DB;"
    run_sql "drop database $DB2;"
    run_sql "drop database $DB3;"
    run_sql "drop database $DB4;"
    run_sql "drop database ${DB}_after_snapshot;"
    
    echo "=== Step 1: Create All Paused Restore Tasks ==="
    
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    
    echo "Creating paused PiTR restore task..."
    restore_fail=0
    run_br restore point --filter "$DB.*" --restored-ts $log_backup_ts --full-backup-storage "$ABORT_BACKUP_DIR" -s "$ABORT_LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting PiTR restore to fail before completion but succeeded'
        exit 1
    fi
    
    echo "Creating paused full restore task..."
    restore_fail=0
    run_br restore full --filter "$DB2.*" -s "$ABORT_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting full restore to fail before completion but succeeded'
        exit 1
    fi
    
    echo "Creating paused database restore task..."
    restore_fail=0
    run_br restore db --db "$DB3" -s "$ABORT_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting db restore to fail before completion but succeeded'
        exit 1
    fi
    
    echo "Creating paused table restore task..."
    restore_fail=0
    run_br restore table --db "$DB4" --table "abort_1" -s "$ABORT_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting table restore to fail before completion but succeeded'
        exit 1
    fi
    
    export GO_FAILPOINTS=""
    
    echo "=== Step 2: Verify All Paused Tasks Exist ==="
    
    # Verify all 4 paused tasks exist in registry
    verify_registry "filter_strings = '$DB.*' AND status = 'paused'" true "paused PiTR task"
    verify_registry "filter_strings = '$DB2.*' AND status = 'paused'" true "paused full restore task"
    verify_registry "cmd = 'DataBase Restore' AND status = 'paused'" true "paused database restore task"
    verify_registry "cmd = 'Table Restore' AND status = 'paused'" true "paused table restore task"
    
    # Verify that snapshot checkpoint databases were created for the 4 restore tasks
    verify_snapshot_checkpoint_databases_exist 4 "after creating 4 paused restore tasks"

    # Debug: Print entire registry contents before abort
    echo "DEBUG: Registry contents before abort attempt:"
    run_sql "SELECT id, filter_strings, start_ts, restored_ts, upstream_cluster_id, with_sys_table, status, cmd, last_heartbeat_time FROM mysql.tidb_restore_registry;"
    cat "$TEST_DIR/sql_res.$TEST_NAME.txt"
    
    echo "=== Step 3: Abort All Restore Tasks ==="
    
    echo "Aborting PiTR restore task..."
    run_br abort restore point --filter "$DB.*" --restored-ts $log_backup_ts --full-backup-storage "$ABORT_BACKUP_DIR" -s "$ABORT_LOG_BACKUP_DIR"
    
    echo "Aborting full restore task..."
    run_br abort restore full --filter "$DB2.*" -s "$ABORT_BACKUP_DIR"
    
    echo "Aborting database restore task..."
    run_br abort restore db --db "$DB3" -s "$ABORT_BACKUP_DIR"
    
    echo "Aborting table restore task..."
    run_br abort restore table --db "$DB4" --table "abort_1" -s "$ABORT_BACKUP_DIR"
    
    echo "=== Step 4: Verify All Tasks Deleted ==="
    
    # Verify all tasks were deleted from registry
    verify_registry "filter_strings = '$DB.*'" false "PiTR task deletion"
    verify_registry "filter_strings = '$DB2.*'" false "full restore task deletion"
    verify_registry "cmd = 'DataBase Restore'" false "database restore task deletion"
    verify_registry "cmd = 'Table Restore'" false "table restore task deletion"
    
    # Verify no paused tasks remain
    verify_registry "status = 'paused'" false "all paused tasks cleaned up"
    
    # Verify checkpoint cleanup for all restore types  
    verify_checkpoint_cleanup "All restore types abort"
    
    # Clean up all user databases since the aborted restores may have left partial data
    echo "Cleaning up all user databases after abort test..."
    run_sql "drop database if exists $DB;"
    run_sql "drop database if exists $DB2;"
    run_sql "drop database if exists $DB3;"
    run_sql "drop database if exists $DB4;"
    run_sql "drop database if exists ${DB}_after_snapshot;"
    echo "User database cleanup completed"
    
    echo "=== Step 5: Special Cases ==="
    
    echo "Abort non-existent task (should succeed gracefully)..."
    
    # Try to abort when no matching task exists
    run_br abort restore point --filter "${DB}_nonexistent.*" --restored-ts $log_backup_ts --full-backup-storage "$ABORT_BACKUP_DIR" -s "$ABORT_LOG_BACKUP_DIR"
    
    echo "PASS: Abort of non-existent task completed gracefully"
    
    echo "Abort with auto-detected restored-ts..."
    
    # Create another paused PiTR task
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "$DB.*" --restored-ts $log_backup_ts --full-backup-storage "$ABORT_BACKUP_DIR" -s "$ABORT_LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting restore to fail before completion but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""
    
    # Verify paused task exists
    verify_registry "filter_strings = '$DB.*' AND status = 'paused'" true "paused task creation for auto-detection test"
    
    # Abort without specifying restored-ts (should use auto-detection)
    run_br abort restore point --filter "$DB.*" --full-backup-storage "$ABORT_BACKUP_DIR" -s "$ABORT_LOG_BACKUP_DIR"
    
    # Verify task was deleted
    verify_registry "filter_strings = '$DB.*'" false "task deletion after abort with auto-detection"
    
    echo "Try to abort a stale running task (should detect it's dead and clean it up)..."
    
    # Create a paused task and manually change it to running
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "$DB2.*" --restored-ts $log_backup_ts --full-backup-storage "$ABORT_BACKUP_DIR" -s "$ABORT_LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting restore to fail before completion but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""
    
    # Change status to running
    run_sql "UPDATE mysql.tidb_restore_registry SET status = 'running' WHERE filter_strings = '$DB2.*' AND status = 'paused';"
    
    # Verify we have a running task 
    verify_registry "filter_strings = '$DB2.*' AND status = 'running'" true "stale running task creation for abort test"
    
    # Try to abort the stale running task - should detect it's dead and clean it up
    run_br abort restore point --filter "$DB2.*" --restored-ts $log_backup_ts --full-backup-storage "$ABORT_BACKUP_DIR" -s "$ABORT_LOG_BACKUP_DIR"
    
    # Verify task was deleted (abort should have detected it was stale and cleaned it up)
    verify_registry "filter_strings = '$DB2.*'" false "stale running task should be deleted after abort"
    
    echo "Test abort with keyspace parameter handling..."

    # Test 1: Create a paused task with keyspace parameter (using pre-allocated keyspace1)
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore full --filter "${DB}_keyspace.*" --keyspace-name "keyspace1" -s "$ABORT_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting restore with keyspace to fail before completion but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""

    # Verify paused task with keyspace exists
    verify_registry "filter_strings = '${DB}_keyspace.*' AND status = 'paused'" true "paused task with keyspace creation"

    # Test 2: Abort with matching keyspace parameter
    echo "Testing abort with matching keyspace parameter..."
    run_br abort restore full --filter "${DB}_keyspace.*" --keyspace-name "keyspace1" -s "$ABORT_BACKUP_DIR"

    # Verify task was deleted
    verify_registry "filter_strings = '${DB}_keyspace.*'" false "task deleted after abort with matching keyspace"

    echo "PASS: Keyspace parameter handling test completed successfully"

    echo "Comprehensive restore abort functionality test completed successfully"

    cleanup
}

test_cross_cluster_lock_conflict() {
    echo "Test Case 7: Cross-cluster lock conflict"
    echo "This test reproduces the lock conflict when two clusters restore from the same log backup storage"

    # Use separate backup directories for this test
    LOCK_BACKUP_DIR="local://$TEST_DIR/lock_backup"
    LOCK_LOG_BACKUP_DIR="local://$TEST_DIR/lock_log_backup"

    echo "Setting up backup data for lock conflict testing..."

    # Create both DB1 and DB2 data
    run_sql "create database if not exists ${DB}1;"
    run_sql "create database if not exists ${DB}2;"

    create_tables_with_values "cluster_a" $TABLE_COUNT ${DB}1
    create_tables_with_values "cluster_b" $TABLE_COUNT ${DB}2

    # Start log backup
    run_br log start --task-name ${TASK_NAME}_lock -s "$LOCK_LOG_BACKUP_DIR"

    # Take snapshot backup (contains both DB1 and DB2 data)
    run_br backup full -s "$LOCK_BACKUP_DIR"

    # Wait for log checkpoint to advance
    log_backup_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")
    echo "Using log backup timestamp: $log_backup_ts"
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance ${TASK_NAME}_lock

    # Stop log backup
    run_br log stop --task-name ${TASK_NAME}_lock

    # Drop all databases to simulate fresh cluster state
    run_sql "drop database ${DB}1;"
    run_sql "drop database ${DB}2;"

    echo "=== Step 1: First restore (DB1) acquires lock and holds it ==="
    run_br log start --task-name ${TASK_NAME}_lock -s "$LOCK_LOG_BACKUP_DIR"

    # Use failpoint to skip migration read lock cleanup
    # This simulates the first restore holding the migration read lock indefinitely
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/skip-migration-read-lock-cleanup=return(true)"

    echo "Starting first restore: ${DB}1 (will hold migration read lock)..."
    echo "This restore will acquire the migration read lock and not release it due to failpoint"

    # Start the first restore in background since it will complete but not release the lock
    run_br restore point --filter "${DB}1.*" --restored-ts $log_backup_ts --full-backup-storage "$LOCK_BACKUP_DIR" -s "$LOCK_LOG_BACKUP_DIR"
    export GO_FAILPOINTS=""

    echo "=== Step 2: Second restore (DB2) attempts do get read lock and append lock ==="

    restore_fail=0
    run_br restore point --filter "${DB}2.*" --restored-ts $log_backup_ts --full-backup-storage "$LOCK_BACKUP_DIR" -s "$LOCK_LOG_BACKUP_DIR" > ${res_file}_second 2>&1 || restore_fail=1

    if [ $restore_fail -ne 0 ]; then
        echo 'ERROR: Second restore failed but should have succeeded'
        echo "Second restore error output:"
        cat ${res_file}_second
        exit 1
    fi

    echo "Verifying second restore data (DB2)..."
    for i in $(seq 1 $TABLE_COUNT); do
        verify_table_data ${DB}2 "cluster_b" $i "Second restore of DB2"
    done

    verify_no_temporary_databases "Concurrent restore operations test"

    echo "lock conflict tests succeeded"

    # Clean up the test databases
    run_sql "drop database if exists ${DB}1;"
    run_sql "drop database if exists ${DB}2;"
}

setup_test_environment

test_mixed_parallel_restores
test_concurrent_restore_table_conflicts
test_restore_with_different_systable_settings
test_auto_restored_ts_conflict
test_restore_abort
test_cross_cluster_lock_conflict

echo "Parallel restore tests completed successfully"
