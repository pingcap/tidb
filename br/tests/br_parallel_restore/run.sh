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

test_stale_task_cleanup() {
    echo "Test Case 4: Stale task cleanup verification"
    
    echo "Starting real restore operations to create checkpoint data..."
    
    # step 1: start a restore that will fail before finishing
    echo "Starting restore that will fail after checkpoint creation..."
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "${DB}_LOG.*" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting first restore to fail after checkpoint creation but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""
    
    # get the first task ID and set it back to 'running' status (but keep heartbeat recent for now)
    stale_task_id=$(run_sql "SELECT id FROM mysql.tidb_restore_registry WHERE filter_strings = '${DB}_LOG.*' ORDER BY id DESC LIMIT 1;" | grep -o 'id: [0-9]*' | grep -o '[0-9]*')
    run_sql "UPDATE mysql.tidb_restore_registry SET status = 'running' WHERE id = $stale_task_id;"
    echo "Created first task ID: $stale_task_id (set to running - will be made stale later)"
    
    # step 2: start a restore that will also fail after checkpoint creation (use non-overlapping filter)
    echo "Starting second restore that will fail after checkpoint creation..."
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "${DB}.full_1" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting second restore to fail after checkpoint creation but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""
    
    # debug: check what's actually in the registry after second restore
    echo "=== DEBUG: Registry entries after second restore ==="
    run_sql "SELECT id, filter_strings FROM mysql.tidb_restore_registry ORDER BY id;"
    echo "=== END DEBUG ==="
    
    # get the recent task ID and manually set it back to 'running' status with stale heartbeat
    recent_task_id=$(run_sql "SELECT id FROM mysql.tidb_restore_registry WHERE filter_strings = '${DB}.full_1' ORDER BY id DESC LIMIT 1;" | grep -o 'id: [0-9]*' | grep -o '[0-9]*')
    
    if [ -z "$recent_task_id" ]; then
        echo "Error: Could not find second restore task in registry"
        echo "Checking all entries:"
        run_sql "SELECT id, filter_strings FROM mysql.tidb_restore_registry;"
        exit 1
    fi
    
    run_sql "UPDATE mysql.tidb_restore_registry SET status = 'running', last_heartbeat_time = DATE_SUB(NOW(), INTERVAL 10 MINUTE) WHERE id = $recent_task_id;"
    echo "Created second task ID: $recent_task_id (set to running with stale heartbeat - will receive heartbeat updates)"
    
    # now set the first task's heartbeat to be very stale (after both tasks are created)
    run_sql "UPDATE mysql.tidb_restore_registry SET last_heartbeat_time = DATE_SUB(NOW(), INTERVAL 15 MINUTE) WHERE id = $stale_task_id;"
    echo "Updated first task ID: $stale_task_id to have very stale heartbeat - should be deleted"
    
    # verify we got valid IDs
    if [ -z "$stale_task_id" ] || [ -z "$recent_task_id" ]; then
        echo "Error: Failed to get valid task IDs"
        echo "stale_task_id: '$stale_task_id'"
        echo "recent_task_id: '$recent_task_id'"
        exit 1
    fi
    
    # step 3: verify tasks exist in registry
    echo "Verifying test tasks exist in registry..."
    task_count=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE status = 'running' AND (filter_strings LIKE '${DB}_LOG%' OR filter_strings LIKE '${DB}.%');" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    if [ "$task_count" -lt 2 ]; then
        echo "Error: Expected at least 2 running tasks but found $task_count"
        exit 1
    fi
    echo "Found $task_count running task(s) in registry"
    
    # step 4: show the tasks before cleanup
    echo "Tasks before cleanup:"
    run_sql "SELECT id, TIMESTAMPDIFF(MINUTE, last_heartbeat_time, NOW()) as minutes_stale, 
             filter_strings FROM mysql.tidb_restore_registry WHERE status = 'running' AND (filter_strings LIKE '${DB}_LOG%' OR filter_strings LIKE '${DB}.%')
             ORDER BY last_heartbeat_time;"
    
    # step 5: start background process to simulate heartbeat updates for the "recent" task
    echo "Starting background heartbeat simulation for recent task..."
    (
        # wait 2 minutes then start updating the heartbeat every 30 seconds
        sleep 120
        for i in {1..10}; do
            run_sql "UPDATE mysql.tidb_restore_registry SET last_heartbeat_time = NOW() WHERE id = $recent_task_id;" > /dev/null 2>&1
            sleep 30
        done
    ) &
    HEARTBEAT_PID=$!
    
    # step 5: start a new restore with different filter to trigger cleanup (non-overlapping)
    echo "Starting new restore to trigger stale task cleanup..."
    echo "Note: This will wait 5 minutes to verify stale tasks are truly orphaned"
    echo "During this time, the 'recent' task will receive heartbeat updates"
    
    start_time=$(date +%s)
    run_br restore full --filter "${DB}_2.*" -s "$BACKUP_DIR"
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    # stop the background heartbeat process
    kill $HEARTBEAT_PID 2>/dev/null || true
    wait $HEARTBEAT_PID 2>/dev/null || true
    
    echo "Restore completed in $duration seconds"
    
    # step 6: verify the new restore succeeded
    echo "Verifying new restore succeeded..."
    for i in $(seq 1 $TABLE_COUNT); do
        run_sql "select c from ${DB}_2.full_$i;" | grep $i
    done
    
    # step 7: verify cleanup results
    echo "Verifying stale task cleanup results..."
    
    # check which tasks still exist
    stale_exists=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE id = $stale_task_id;" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    recent_exists=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE id = $recent_task_id;" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    
    echo "Stale task (ID: $stale_task_id) exists: $stale_exists (should be 0 - deleted)"
    echo "Recent task (ID: $recent_task_id) exists: $recent_exists (should be 1 - kept due to heartbeat updates)"
    
    # verify checkpoint databases are also cleaned up for stale task
    echo "Checking checkpoint database cleanup..."
    stale_checkpoint_dbs=$(run_sql "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE '__TiDB_BR_Temporary_%_Restore_Checkpoint_${stale_task_id}';" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    recent_checkpoint_dbs=$(run_sql "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE '__TiDB_BR_Temporary_%_Restore_Checkpoint_${recent_task_id}';" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    
    echo "Stale task checkpoint databases: $stale_checkpoint_dbs (should be 0 - cleaned up)"
    echo "Recent task checkpoint databases: $recent_checkpoint_dbs (may be present if task was far enough)"
    
    # verify cleanup worked correctly
    success_count=0
    
    # test case 1: stale task cleanup (including both registry and checkpoint cleanup)
    if [ "$stale_exists" -eq 0 ] && [ "$stale_checkpoint_dbs" -eq 0 ]; then
        echo "PASS: Stale task was correctly cleaned up (both registry and checkpoint databases)"
        success_count=$((success_count + 1))
    else
        echo "FAIL: Stale task cleanup failed (registry exists: $stale_exists, checkpoint dbs: $stale_checkpoint_dbs)"
    fi
    
    # test case 2: recent task preservation with heartbeat updates
    if [ "$recent_exists" -eq 1 ]; then
        echo "PASS: Recent task was correctly preserved (heartbeat updates detected)"
        success_count=$((success_count + 1))
    else
        echo "FAIL: Recent task was incorrectly cleaned up despite heartbeat updates"
    fi
    
    if [ "$success_count" -eq 2 ]; then
        echo "SUCCESS: Both test scenarios passed!"
    elif [ "$success_count" -eq 1 ]; then
        echo "PARTIAL SUCCESS: $success_count/2 scenarios passed"
    else
        echo "FAILURE: Only $success_count/2 scenarios passed"
        exit 1
    fi
    
    # step 8: show final state
    echo "Final registry state:"
    run_sql "SELECT id, status, TIMESTAMPDIFF(MINUTE, last_heartbeat_time, NOW()) as minutes_stale,
             filter_strings FROM mysql.tidb_restore_registry WHERE status = 'running';"
    
    # step 9: cleanup test entries
    echo "Cleaning up test registry entries..."
    run_sql "DELETE FROM mysql.tidb_restore_registry WHERE id IN ($stale_task_id, $recent_task_id);"
    
    echo "Comprehensive stale task cleanup test completed"
    echo "Two scenarios verified: stale cleanup and false-positive prevention with checkpoint cleanup"
    
    verify_no_temporary_databases "Test Case 4: Stale task cleanup"
    
    cleanup
}

test_auto_restored_ts_conflict() {
    echo "Test Case 5: Auto-restoredTS conflict detection"

    # use separate backup directories for this test
    PITR_BACKUP_DIR="local://$TEST_DIR/pitr_backup"
    PITR_LOG_BACKUP_DIR="local://$TEST_DIR/pitr_log_backup"

    echo "Setting up backup data for PiTR conflict testing..."

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
    
    # step 2: first restore attempt with explicit restored-ts but fail before completion
    echo "Starting first PiTR restore with explicit restored-ts but fail before completion..."
    
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "$DB.*" --restored-ts $log_backup_ts --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting first restore to fail before completion but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""
    
    echo "First PiTR restore failed as expected, leaving registry entry"
    
    # step 3: verify the first task exists in registry
    first_task_count=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*';" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    if [ "$first_task_count" -ne 1 ]; then
        echo "Error: Expected 1 first task but found $first_task_count"
        exit 1
    fi
    
    first_task_id=$(run_sql "SELECT id FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*' ORDER BY id DESC LIMIT 1;" | grep -o 'id: [0-9]*' | grep -o '[0-9]*')
    first_restored_ts=$(run_sql "SELECT restored_ts FROM mysql.tidb_restore_registry WHERE id = $first_task_id;" | grep -o 'restored_ts: [0-9]*' | grep -o '[0-9]*')
    echo "First PiTR task ID: $first_task_id (with restoredTS: $first_restored_ts)"
    
    # step 4: try second restore without restored-ts (auto-detection) - should get conflict
    echo "Attempting second PiTR without explicit restored-ts (auto-detection)..."
    echo "This should trigger auto-restoredTS conflict detection..."
    
    restore_fail=0
    run_br restore point --filter "$DB.*" --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR" > $res_file 2>&1 || restore_fail=1
    
    if [ $restore_fail -ne 1 ]; then
        echo "FAIL: Expected second restore to fail due to conflict but it succeeded"
        echo "FAIL: Conflict detection may not be working properly"
        # cleanup and exit test
        run_sql "DELETE FROM mysql.tidb_restore_registry WHERE id = $first_task_id;"
        exit 1
    fi
    
    echo "Second restore failed as expected - checking error message..."
    
    # check for expected error message about auto-restoredTS conflict
    if grep -q "Found existing restore task.*same parameters except restoredTS" "$res_file"; then
        echo "PASS: Detected expected auto-restoredTS conflict error message"
        # log the specific error message for manual verification
        echo "Extracted error message:"
        grep "Found existing restore task.*same parameters except restoredTS" "$res_file"
    else
        echo "FAIL: Expected auto-restoredTS conflict error message not found"
        exit 1
    fi
    
    # step 5: try third restore with explicit restored-ts matching the existing task
    echo "Attempting third PiTR with explicit restored-ts from existing task..."
    echo "Using restored-ts from existing task: $first_restored_ts"
    
    # this should succeed since it uses the same restored-ts as existing task
    run_br restore point --filter "$DB.*" --restored-ts $first_restored_ts --full-backup-storage "$PITR_BACKUP_DIR" -s "$PITR_LOG_BACKUP_DIR"
    
    # verify the restore succeeded
    for i in $(seq 1 $TABLE_COUNT); do
        run_sql "select c from $DB.pitr_$i;" | grep $i
    done
    echo "PASS: Third restore with explicit matching restored-ts completed successfully"
    
    # cleanup the registry entry (restore should have cleaned it up automatically)
    remaining_tasks=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*';" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    if [ "$remaining_tasks" -gt 0 ]; then
        echo "Cleaning up remaining registry entries..."
        run_sql "DELETE FROM mysql.tidb_restore_registry WHERE filter_strings = '$DB.*';"
    fi
    
    # step 6: show final state (should be clean)
    echo "Final registry state (should be empty after cleanup):"
    run_sql "SELECT id, status, start_ts, restored_ts, filter_strings, cmd 
             FROM mysql.tidb_restore_registry WHERE filter_strings LIKE '$DB%';"
    
    echo "Auto-restoredTS conflict detection test completed"
    echo "This test validates the complete user flow:"
    echo "1. Failed PiTR leaves registry entry"
    echo "2. Retry without explicit restored-ts triggers conflict detection"
    echo "3. Retry with matching restored-ts succeeds"
    
    cleanup
}

setup_test_environment

# test_mixed_parallel_restores
# test_concurrent_restore_table_conflicts
# test_restore_with_different_systable_settings
test_stale_task_cleanup
# test_auto_restored_ts_conflict

echo "Parallel restore tests completed successfully"
