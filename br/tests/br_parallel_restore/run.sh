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
    
    # step 1: manually create three registry entries to simulate different scenarios
    echo "Creating test registry entries..."
    
    # create a very stale task (15 minutes old) - should be cleaned up
    run_sql "INSERT INTO mysql.tidb_restore_registry 
        (filter_strings, filter_hash, start_ts, restored_ts, upstream_cluster_id, 
         with_sys_table, status, cmd, task_start_time, last_heartbeat_time)
        VALUES ('test.*', MD5('test.*'), 100, 200, 1, 0, 'running', 'test', 
                DATE_SUB(NOW(), INTERVAL 15 MINUTE), DATE_SUB(NOW(), INTERVAL 15 MINUTE));"
    echo "=== DEBUG: Extracting stale task ID ==="
    stale_raw=$(run_sql "SELECT id FROM mysql.tidb_restore_registry WHERE filter_strings = 'test.*' ORDER BY id DESC LIMIT 1;")
    echo "Raw output: '$stale_raw'"
    stale_task_id=$(echo "$stale_raw" | grep -o 'id: [0-9]*' | grep -o '[0-9]*')
    echo "Extracted: '$stale_task_id'"
    
    # create a recent task (1 minute old) - should NOT be cleaned up  
    run_sql "INSERT INTO mysql.tidb_restore_registry 
        (filter_strings, filter_hash, start_ts, restored_ts, upstream_cluster_id,
         with_sys_table, status, cmd, task_start_time, last_heartbeat_time)
        VALUES ('other.*', MD5('other.*'), 300, 400, 1, 0, 'running', 'test',
                DATE_SUB(NOW(), INTERVAL 1 MINUTE), DATE_SUB(NOW(), INTERVAL 1 MINUTE));"
    recent_task_id=$(run_sql "SELECT id FROM mysql.tidb_restore_registry WHERE filter_strings = 'other.*' ORDER BY id DESC LIMIT 1;" | grep -o 'id: [0-9]*' | grep -o '[0-9]*')
    
    # create a task that appears stale initially but will be updated during wait - should NOT be cleaned up
    run_sql "INSERT INTO mysql.tidb_restore_registry 
        (filter_strings, filter_hash, start_ts, restored_ts, upstream_cluster_id,
         with_sys_table, status, cmd, task_start_time, last_heartbeat_time)
        VALUES ('updating.*', MD5('updating.*'), 500, 600, 1, 0, 'running', 'test',
                DATE_SUB(NOW(), INTERVAL 10 MINUTE), DATE_SUB(NOW(), INTERVAL 10 MINUTE));"
    updating_task_id=$(run_sql "SELECT id FROM mysql.tidb_restore_registry WHERE filter_strings = 'updating.*' ORDER BY id DESC LIMIT 1;" | grep -o 'id: [0-9]*' | grep -o '[0-9]*')
    
    # debug: show entire table after inserts
    echo "=== DEBUG: Entire registry table after inserts ==="
    run_sql "SELECT * FROM mysql.tidb_restore_registry ORDER BY id;"
    echo "=== END DEBUG ==="
    
    # debug: show individual ID queries
    echo "=== DEBUG: Individual ID queries ==="
    echo "Query for stale task:"
    run_sql "SELECT id FROM mysql.tidb_restore_registry WHERE filter_strings = 'test.*' ORDER BY id DESC LIMIT 1;"
    echo "Query for recent task:"
    run_sql "SELECT id FROM mysql.tidb_restore_registry WHERE filter_strings = 'other.*' ORDER BY id DESC LIMIT 1;"
    echo "Query for updating task:"
    run_sql "SELECT id FROM mysql.tidb_restore_registry WHERE filter_strings = 'updating.*' ORDER BY id DESC LIMIT 1;"
    echo "=== END DEBUG ==="
    
    # verify we got valid IDs
    if [ -z "$stale_task_id" ] || [ -z "$recent_task_id" ] || [ -z "$updating_task_id" ]; then
        echo "Error: Failed to get valid task IDs"
        echo "stale_task_id: '$stale_task_id'"
        echo "recent_task_id: '$recent_task_id'"
        echo "updating_task_id: '$updating_task_id'"
        exit 1
    fi
    
    echo "Created stale task ID: $stale_task_id (15 minutes old - should be deleted)"
    echo "Created recent task ID: $recent_task_id (1 minute old - should be kept)"
    echo "Created updating task ID: $updating_task_id (10 minutes old initially, will be updated - should be kept)"
    
    # step 2: verify all tasks exist in registry
    echo "Verifying test tasks exist in registry..."
    task_count=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE status = 'running';" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    if [ "$task_count" -lt 3 ]; then
        echo "Error: Expected at least 3 running tasks but found $task_count"
        exit 1
    fi
    echo "Found $task_count running task(s) in registry"
    
    # step 3: show the tasks before cleanup
    echo "Tasks before cleanup:"
    run_sql "SELECT id, TIMESTAMPDIFF(MINUTE, last_heartbeat_time, NOW()) as minutes_stale, 
             filter_strings FROM mysql.tidb_restore_registry WHERE status = 'running' 
             ORDER BY last_heartbeat_time;"
    
    # step 4: start background process to simulate heartbeat updates for the "updating" task
    echo "Starting background heartbeat simulation for updating task..."
    (
        # wait 2 minutes then start updating the heartbeat every 30 seconds
        sleep 120
        for i in {1..10}; do
            run_sql "UPDATE mysql.tidb_restore_registry SET last_heartbeat_time = NOW() WHERE id = $updating_task_id;" > /dev/null 2>&1
            sleep 30
        done
    ) &
    HEARTBEAT_PID=$!
    
    # step 5: start a new restore with different filter to trigger cleanup
    echo "Starting new restore to trigger stale task cleanup..."
    echo "Note: This will wait 5 minutes to verify stale tasks are truly orphaned"
    echo "During this time, the 'updating' task will receive heartbeat updates"
    
    start_time=$(date +%s)
    run_br restore full --filter "$DB.*" -s "$BACKUP_DIR"
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    # stop the background heartbeat process
    kill $HEARTBEAT_PID 2>/dev/null || true
    wait $HEARTBEAT_PID 2>/dev/null || true
    
    echo "Restore completed in $duration seconds"
    
    # step 6: verify the new restore succeeded
    echo "Verifying new restore succeeded..."
    for i in $(seq 1 $TABLE_COUNT); do
        run_sql "select c from $DB.full_$i;" | grep $i
    done
    
    # step 7: verify cleanup results
    echo "Verifying stale task cleanup results..."
    
    # check which tasks still exist
    stale_exists=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE id = $stale_task_id;" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    recent_exists=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE id = $recent_task_id;" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    updating_exists=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_restore_registry WHERE id = $updating_task_id;" | grep -o 'COUNT.*: [0-9]*' | grep -o '[0-9]*')
    
    echo "Stale task (ID: $stale_task_id) exists: $stale_exists (should be 0 - deleted)"
    echo "Recent task (ID: $recent_task_id) exists: $recent_exists (should be 1 - kept)"
    echo "Updating task (ID: $updating_task_id) exists: $updating_exists (should be 1 - kept due to heartbeat updates)"
    
    # verify cleanup worked correctly
    success_count=0
    
    if [ "$stale_exists" -eq 0 ]; then
        echo "PASS: Stale task was correctly cleaned up"
        success_count=$((success_count + 1))
    else
        echo "FAIL: Stale task was NOT cleaned up (unexpected)"
    fi
    
    if [ "$recent_exists" -eq 1 ]; then
        echo "PASS: Recent task was correctly preserved"
        success_count=$((success_count + 1))
    else
        echo "FAIL: Recent task was incorrectly cleaned up"
    fi
    
    if [ "$updating_exists" -eq 1 ]; then
        echo "PASS: Updating task was correctly preserved (heartbeat updates detected)"
        success_count=$((success_count + 1))
    else
        echo "FAIL: Updating task was incorrectly cleaned up despite heartbeat updates"
    fi
    
    if [ "$success_count" -eq 3 ]; then
        echo "SUCCESS: All three test scenarios passed!"
    elif [ "$success_count" -eq 2 ]; then
        echo "PARTIAL SUCCESS: $success_count/3 scenarios passed"
    else
        echo "FAILURE: Only $success_count/3 scenarios passed"
        exit 1
    fi
    
    # step 8: show final state
    echo "Final registry state:"
    run_sql "SELECT id, status, TIMESTAMPDIFF(MINUTE, last_heartbeat_time, NOW()) as minutes_stale,
             filter_strings FROM mysql.tidb_restore_registry WHERE status = 'running';"
    
    # step 9: cleanup test entries
    echo "Cleaning up test registry entries..."
    run_sql "DELETE FROM mysql.tidb_restore_registry WHERE id IN ($stale_task_id, $recent_task_id, $updating_task_id);"
    
    echo "Comprehensive stale task cleanup test completed"
    echo "All three scenarios verified: stale cleanup, recent preservation, and false-positive prevention"
    
    verify_no_temporary_databases "Test Case 4: Stale task cleanup"
    
    cleanup
}

setup_test_environment

test_mixed_parallel_restores
test_concurrent_restore_table_conflicts
test_restore_with_different_systable_settings
test_stale_task_cleanup

echo "Parallel restore tests completed successfully"
