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

setup_test_environment

test_mixed_parallel_restores
test_concurrent_restore_table_conflicts
test_restore_with_different_systable_settings

echo "Parallel restore tests completed successfully"
