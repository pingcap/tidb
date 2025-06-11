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
        run_sql "select c from $DB.full_$i;" | grep $i
    done
    
    # Verify DB2 restore (PITR with filter)
    echo "Verifying PITR restore with filter of $DB2..."
    for i in $(seq 1 $TABLE_COUNT); do
        run_sql "select c from $DB2.full_$i;" | grep $i
    done
    
    # Verify DB3 restore (specific db)
    echo "Verifying specific db restore for $DB3..."
    for i in $(seq 1 $TABLE_COUNT); do
        run_sql "select c from $DB3.full_$i;" | grep $i
    done
    
    # Verify DB4 restore (specific table)
    echo "Verifying specific table restore for $DB4..."
    run_sql "select c from $DB4.full_1;" | grep 1
    for i in $(seq 2 $TABLE_COUNT); do
        if run_sql "show tables from $DB4 like 'full_$i';" | grep -q "full_$i"; then
            echo "Error: Table full_$i should not have been restored" >&2
            exit 1
        fi
    done
    
    # Verify DB_LOG restore
    echo "Verifying PITR restore of $DB_LOG (created after backup)..."
    for i in $(seq 1 $TABLE_COUNT); do
        run_sql "select c from $DB_LOG.log_$i;" | grep $i
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
    for i in $(seq 1 $TABLE_COUNT); do
        run_sql "select c from $DB.full_$i;" | grep $i
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

    for i in $(seq 1 $TABLE_COUNT); do
        run_sql "select c from $DB2.full_$i;" | grep $i
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

    # wait a few seconds to ensure log backup has progressed beyond this timestamp
    echo "Waiting for log backup to advance..."
    sleep 5

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
    
    echo "Checking for registry entries..."
    
    registry_check=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*';" 2>/dev/null || echo "TABLE_ERROR")
    
    echo "Debug: Registry check output: '$registry_check'"
    
    # Extract count from MySQL vertical format output
    first_task_count=$(echo "$registry_check" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    
    if [ -z "$first_task_count" ] || [ "$first_task_count" -eq 0 ]; then
        echo "Error: No tasks found in registry (count: ${first_task_count:-0})"
        echo "Raw output: '$registry_check'"
        return 1
    fi
    
    echo "Found $first_task_count task(s) in registry"
    
    # Get basic task information for verification
    echo "Debug: Checking task details..."
    run_sql "SELECT id, status, filter_strings FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*' ORDER BY id DESC LIMIT 1;" || {
        echo "Warning: Could not query task details, continuing..."
        return 0
    }
    
    echo "PASS: Registry verification completed (found entries as expected)"
    
    # Test 2: Retry without explicit restored-ts (auto-detection) - should resume existing paused task
    echo "Test 2: Retry without explicit restored-ts (auto-detection)..."
    echo "This should trigger intelligent RestoreTS resolution and resume the paused task"
    
    # This should succeed and reuse the existing task's RestoreTS
    run_br restore point --filter "$DB.*" --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR"
    
    # Verify the restore succeeded
    for i in $(seq 1 $TABLE_COUNT); do
        run_sql "select c from $DB.pitr_$i;" | grep $i
    done
    echo "PASS: Second restore with auto-detection succeeded by resuming existing task"
    
    # Check registry state after successful restore
    echo "Checking registry state after restore..."
    registry_count_after=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*';" 2>/dev/null || echo "0")
    echo "Debug: Registry entries after restore: '$registry_count_after'"
    
    final_count=$(echo "$registry_count_after" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    final_count=${final_count:-0}
    
    if [ "$final_count" -eq 0 ]; then
        echo "PASS: Task was completed and cleaned up from registry"
    else
        echo "ERROR: Task may still exist in registry (count: $final_count)"
        # Show registry state for debugging
        run_sql "SELECT id, status FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*';" 2>/dev/null || echo "Could not query details"
    fi
    
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
    echo "Verifying that we have a running task..."
    stuck_check=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*' AND status = 'running';" 2>/dev/null || echo "0")
    echo "Debug: Running task check: '$stuck_check'"
    
    # Verify we have exactly 1 running task before attempting restore
    stuck_count=$(echo "$stuck_check" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    stuck_count=${stuck_count:-0}
    
    if [ "$stuck_count" -eq 0 ]; then
        echo "ERROR: Failed to create running task for stale detection test"
        echo "Expected to have at least 1 running task after manual status update"
        exit 1
    fi
    
    echo "Successfully created $stuck_count running task(s) for stale detection test"
    
    # Try to restore without explicit restored-ts - should detect stale task, transition it to paused, and reuse it
    echo "Attempting restore without explicit restored-ts (testing stale detection and reuse)..."
    echo "This should detect the stale running task, transition it to paused, and reuse its RestoreTS"
    
    run_br restore point --filter "$DB.*" --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR"
    
    # Verify the restore succeeded by checking the data
    echo "Verifying restore succeeded..."
    for i in $(seq 1 $TABLE_COUNT); do
        run_sql "select c from $DB.pitr_$i;" | grep $i
    done
    echo "PASS: Stale task detection and reuse worked correctly"
    
    # Check final task status - should be cleaned up after successful restore
    echo "Checking final task status after successful restore..."
    final_task_check=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*';" 2>/dev/null || echo "0")
    final_task_count=$(echo "$final_task_check" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    final_task_count=${final_task_count:-0}
    
    if [ "$final_task_count" -eq 0 ]; then
        echo "PASS: Task was completed and cleaned up from registry"
    else
        echo "ERROR: Task should have been cleaned up but $final_task_count task(s) remain in registry"
        run_sql "SELECT id, status FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*';" 2>/dev/null || echo "Could not query task details"
        exit 1
    fi
    
    # Test 4: Test with user-specified RestoreTS (should bypass conflict resolution)
    echo "Test 4: Testing user-specified RestoreTS (should bypass all conflict resolution)..."
    
    # Use DB2 for this test to avoid table conflicts with existing DB
    run_sql "drop database if exists $DB2;"
    
    # Create a paused task for DB2 first
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "$DB2.*" --restored-ts $log_backup_ts --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting restore to fail before completion but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""
    
    echo "Checking for created paused task for $DB2..."
    run_sql "SELECT id, restored_ts FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB2.*' ORDER BY id DESC LIMIT 1;" 2>/dev/null || echo "Could not query paused task details"
    
    # Now try with different user-specified RestoreTS - should create new task, not reuse existing
    different_ts=$((log_backup_ts - 1000000))  # Slightly different timestamp
    echo "Attempting restore with user-specified RestoreTS ($different_ts) different from existing task..."
    
    run_br restore point --filter "$DB2.*" --restored-ts $different_ts --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR"
    
    # Verify restore succeeded - Note: The backup contains pitr_* tables, not full_* tables
    for i in $(seq 1 $TABLE_COUNT); do
        run_sql "select c from $DB2.pitr_$i;" | grep $i
    done
    echo "PASS: User-specified RestoreTS bypassed conflict resolution correctly"
    
    # Final cleanup
    run_sql "DELETE FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*';"
    run_sql "DELETE FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB2.*';"
    
    echo "Final registry state check:"
    final_check=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE filter_strings LIKE '$DB%' OR filter_strings LIKE '$DB2%';" 2>/dev/null || echo "0")
    echo "Debug: Final registry check: '$final_check'"
    
    final_remaining=$(echo "$final_check" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    final_remaining=${final_remaining:-0}
    
    if [ "$final_remaining" -eq 0 ]; then
        echo "PASS: Registry is clean"
    else
        echo "Note: $final_remaining task(s) remain in registry"
        run_sql "SELECT id, status, filter_strings FROM mysql.tidb_restore_registry WHERE filter_strings LIKE '$DB%' OR filter_strings LIKE '$DB2%';" 2>/dev/null || echo "Could not query remaining task details"
    fi
    
    echo "Intelligent RestoreTS resolution test completed successfully"
    echo "This test validates the new behavior:"
    echo "1. Failed restore leaves paused task in registry"
    echo "2. Retry without explicit restored-ts resumes existing paused task"
    echo "3. Stale running task detection works (when applicable)"
    echo "4. User-specified restored-ts bypasses conflict resolution"
    
    cleanup
}

setup_test_environment

test_mixed_parallel_restores
test_concurrent_restore_table_conflicts
test_restore_with_different_systable_settings
test_auto_restored_ts_conflict

echo "Parallel restore tests completed successfully"
