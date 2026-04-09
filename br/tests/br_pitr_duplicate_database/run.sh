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
DB1="${TEST_NAME}_1"
DB2="${TEST_NAME}_2"
DB3="${TEST_NAME}_3"
DB4="${TEST_NAME}_4"
DB5="${TEST_NAME}_5"
TASK_NAME="$TEST_NAME"
BACKUP_DIR="local://$TEST_DIR/backup"
LOG_BACKUP_DIR="local://$TEST_DIR/log_backup"
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

setup_test_environment() {
    run_br log start --task-name $TASK_NAME -s "$LOG_BACKUP_DIR"
    
    run_sql "create database if not exists $DB3;"
    run_sql "create database if not exists $DB4;"
    run_sql "create database if not exists $DB5;"
    run_sql "create table $DB5.t2(id int);"
    run_sql "insert into $DB5.t2 values (1);"

    run_br backup full -s "$BACKUP_DIR"

    # incremental update for case 1
    run_sql "create database if not exists $DB1;"
    run_sql "create table $DB1.t1(id int);"
    run_sql "insert into $DB1.t1 values (1);"
    # incremental update for case 2
    run_sql "create database if not exists $DB2;"
    run_sql "create table $DB2.t1(id int);"
    run_sql "insert into $DB2.t1 values (1);"
    run_sql "create table $DB2.t2(id int);"
    run_sql "insert into $DB2.t2 values (1);"
    # incremental update for case 3
    run_sql "create table $DB3.t1(id int);"
    run_sql "insert into $DB3.t1 values (1);"
    # incremental update for case 4
    run_sql "create table $DB4.t1(id int);"
    run_sql "insert into $DB4.t1 values (1);"
    run_sql "create table $DB4.t2(id int);"
    run_sql "insert into $DB4.t2 values (1);"
    # incremental update for case 5
    run_sql "create table $DB5.t1(id int);"
    run_sql "insert into $DB5.t1 values (1);"


    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    run_br log stop --task-name $TASK_NAME

    cleanup
}

cleanup() {
    run_sql "drop database $DB1;"
    run_sql "drop database $DB2;"
    run_sql "drop database $DB3;"
    run_sql "drop database $DB4;"
    run_sql "drop database $DB5;"
}

# test case 1: database $DB1 is created in log restore, but the cluster already has the database $DB1.
# result: the database $DB1 is reused, and won't be restored.
test_database_created_in_log_restore() {
    echo "Test Case 1: test_database_created_in_log_restore"

    run_sql "create database $DB1 collate = 'utf8mb4_general_ci';"
    run_sql "show create database $DB1;"
    check_contains "COLLATE utf8mb4_general_ci"

    run_br restore point --filter "$DB1.t1" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR"

    run_sql "show create database $DB1;"
    check_contains "COLLATE utf8mb4_general_ci"

    run_sql "SELECT COUNT(DISTINCT SCHEMA_ID) AS COUNT FROM INFORMATION_SCHEMA.DDL_JOBS WHERE DB_NAME = '$DB1';"
    check_contains "COUNT: 1"
}

# test case 2: database $DB2 is created in log restore. The table $DB2.t1 is restored and then the table $DB2.t2 is restored.
# result: the database $DB2 is reused, and won't be restored in the second restore.
test_database_created_in_log_restore_and_restore_twice_the_same_database() {
    echo "Test Case 2: test_database_created_in_log_restore_and_restore_twice_the_same_database"

    run_br restore point --filter "$DB2.t1" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR"
    run_br restore point --filter "$DB2.t2" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR"

    run_sql "SELECT COUNT(DISTINCT SCHEMA_ID) AS COUNT FROM INFORMATION_SCHEMA.DDL_JOBS WHERE DB_NAME = '$DB2';"
    check_contains "COUNT: 1"
}

# test case 3: database $DB3 is created in snapshot restore, but the cluster already has the database $DB3.
# result: the database $DB3 is reused, and won't be restored.
test_database_created_in_snapshot_restore() {
    echo "Test Case 3: test_database_created_in_snapshot_restore"

    run_sql "create database $DB3 collate = 'utf8mb4_general_ci';"
    run_sql "show create database $DB3;"
    check_contains "COLLATE utf8mb4_general_ci"

    run_br restore point --filter "$DB3.t1" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR"

    run_sql "SELECT COUNT(DISTINCT SCHEMA_ID) AS COUNT FROM INFORMATION_SCHEMA.DDL_JOBS WHERE DB_NAME = '$DB3';"
    check_contains "COUNT: 1"
}

# test case 4: The table $DB4.t1 is restored and concurrently the table $DB4.t2 is restored.
# result: failed to restore the table $DB4.t2 because the database $DB4 is already being restored.
test_concurrent_pitr_the_same_database() {
    echo "Test Case 4: test_concurrent_pitr_the_same_database"

    # start the first task, fail it before finishing so restore task is still registered
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "$DB4.t1" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting failed before unregister restore task but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""

    # start the second task, should fail fast since first task still registered
    restore_fail=0
    run_br restore point --filter "$DB4.t2" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR" > $res_file 2>&1 || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting conflicting database detected and restore abort but succeeded'
        exit 1
    fi
    check_contains "cannot be restored concurrently by current task"

    # run the first task again to finish
    run_br restore point --filter "$DB4.t1" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR"

    # run the second task again to finish
    run_br restore point --filter "$DB4.t2" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR"

    run_sql "SELECT COUNT(DISTINCT SCHEMA_ID) AS COUNT FROM INFORMATION_SCHEMA.DDL_JOBS WHERE DB_NAME = '$DB4';"
    check_contains "COUNT: 1"
}

# test case 5: The table $DB5.t1 is restored and concurrently the database $DB5 is snapshot restored.
# result: failed to restore the database $DB5 because the database $DB5 is already being restored.
test_concurrent_pitr_and_snapshot_restore_the_same_database() {
    echo "Test Case 5: test_concurrent_pitr_and_snapshot_restore_the_same_database"
    
    # start the first task, fail it before finishing so restore task is still registered
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/fail-at-end-of-restore=return(true)"
    restore_fail=0
    run_br restore point --filter "$DB5.t1" --full-backup-storage "$BACKUP_DIR" -s "$LOG_BACKUP_DIR" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting failed before unregister restore task but succeeded'
        exit 1
    fi
    export GO_FAILPOINTS=""

    # start the second task, should fail fast since first task still registered
    restore_fail=0
    run_br restore full --filter "$DB5.t2" -s "$BACKUP_DIR" > $res_file 2>&1 || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting conflicting database detected and restore abort but succeeded'
        exit 1
    fi
    check_contains "cannot be restored concurrently by current task"

    run_sql "SELECT COUNT(DISTINCT SCHEMA_ID) AS COUNT FROM INFORMATION_SCHEMA.DDL_JOBS WHERE DB_NAME = '$DB5';"
    check_contains "COUNT: 1"
}

setup_test_environment
test_database_created_in_log_restore
test_database_created_in_log_restore_and_restore_twice_the_same_database
test_database_created_in_snapshot_restore
test_concurrent_pitr_the_same_database
test_concurrent_pitr_and_snapshot_restore_the_same_database

echo "pitr restore duplicate database tests completed successfully"
