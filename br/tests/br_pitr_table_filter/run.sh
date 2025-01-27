#!/bin/sh
#
# Copyright 2024 PingCAP, Inc.
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
DB="$TEST_NAME"
CUR=$(cd `dirname $0`; pwd)
TASK_NAME="pitr_table_filter"
. run_services

# helper methods
create_tables_with_values() {
    local prefix=$1    # table name prefix
    local count=$2     # number of tables to create
    
    for i in $(seq 1 $count); do
        run_sql "create table $DB.${prefix}_${i}(c int); insert into $DB.${prefix}_${i} values ($i);"
    done
}

verify_tables() {
    local prefix=$1        # table name prefix
    local count=$2         # number of tables to verify
    local should_exist=$3  # true/false - whether tables should exist
    
    for i in $(seq 1 $count); do
        if [ "$should_exist" = "true" ]; then
            run_sql "select count(*) = 1 from $DB.${prefix}_${i} where c = $i" || {
                echo "Table $DB.${prefix}_${i} doesn't have expected value $i"
                exit 1
            }
        else
            if run_sql "select * from $DB.${prefix}_${i}" 2>/dev/null; then
                echo "Table $DB.${prefix}_${i} exists but should not"
                exit 1
            fi
        fi
    done
}

rename_tables() {
    local old_prefix=$1    # original table name prefix
    local new_prefix=$2    # new table name prefix
    local count=$3         # number of tables to rename
    
    for i in $(seq 1 $count); do
        run_sql "rename table $DB.${old_prefix}_${i} to $DB.${new_prefix}_${i};"
    done
}

drop_tables() {
    local prefix=$1    # table name prefix
    local count=$2     # number of tables to drop
    
    for i in $(seq 1 $count); do
        run_sql "drop table $DB.${prefix}_${i};"
    done
}

verify_other_db_tables() {
    local should_exist=$1  # true/false - whether tables should exist
    
    if [ "$should_exist" = "true" ]; then
        run_sql "select count(*) = 1 from ${DB}_other.test_table where c = 42" || {
            echo "Table ${DB}_other.test_table doesn't have expected value 42"
            exit 1
        }
    else
        if run_sql "select * from ${DB}_other.test_table" 2>/dev/null; then
            echo "Table ${DB}_other.test_table exists but should not"
            exit 1
        fi
    fi
}

test_basic_filter() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start basic filter testing"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"
    run_sql "create schema ${DB}_other;"

    echo "write initial data and do snapshot backup"
    create_tables_with_values "full_backup" 3
    create_tables_with_values "table_to_drop" 3

    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "write more data and wait for log backup to catch up"
    run_sql "create table ${DB}_other.test_table(c int); insert into ${DB}_other.test_table values (42);"
    create_tables_with_values "log_backup_lower" 3
    create_tables_with_values "LOG_BACKUP_UPPER" 3
    create_tables_with_values "other" 3
    drop_tables "table_to_drop" 3

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # restart services to clean up the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 1 sanity check, zero filter"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full"

    verify_tables "log_backup_lower" 3 true
    verify_tables "LOG_BACKUP_UPPER" 3 true
    verify_tables "full_backup" 3 true
    verify_tables "other" 3 true
    verify_tables "table_to_drop" 3 false
    verify_other_db_tables true

    echo "case 2 with log restore table filter"
    run_sql "drop schema $DB;"
    run_sql "drop schema ${DB}_other;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.log*"

    verify_tables "log_backup_lower" 3 true
    verify_tables "LOG_BACKUP_UPPER" 3 true
    verify_tables "full_backup" 3 false
    verify_tables "other" 3 false
    verify_tables "table_to_drop" 3 false
    verify_other_db_tables false

    echo "case 3 with multiple filters"
    run_sql "drop schema $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.log*" -f "$DB.full*"

    verify_tables "log_backup_lower" 3 true
    verify_tables "LOG_BACKUP_UPPER" 3 true
    verify_tables "full_backup" 3 true
    verify_tables "other" 3 false
    verify_tables "table_to_drop" 3 false
    verify_other_db_tables false

    echo "case 4 with negative filters"
    run_sql "drop schema $DB;"
    # have to use a match all filter before using negative filters
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "*.*" -f "!mysql.*" -f "!$DB.log*"

    verify_tables "log_backup_lower" 3 false
    verify_tables "LOG_BACKUP_UPPER" 3 false
    verify_tables "full_backup" 3 true
    verify_tables "other" 3 true
    verify_tables "table_to_drop" 3 false
    verify_other_db_tables true

    echo "case 5 restore dropped table"
    run_sql "drop schema $DB;"
    run_sql "drop schema ${DB}_other;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.table*"

    verify_tables "log_backup_lower" 3 false
    verify_tables "LOG_BACKUP_UPPER" 3 false
    verify_tables "full_backup" 3 false
    verify_tables "other" 3 false
    verify_tables "table_to_drop" 3 false
    verify_other_db_tables false

    echo "case 6 restore only other database"
    run_sql "drop schema $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_other.*"

    verify_tables "log_backup_lower" 3 false
    verify_tables "LOG_BACKUP_UPPER" 3 false
    verify_tables "full_backup" 3 false
    verify_tables "other" 3 false
    verify_tables "table_to_drop" 3 false
    verify_other_db_tables true

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "basic filter test cases passed"
}

test_with_full_backup_filter() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start with full backup filter testing"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"
    run_sql "create schema ${DB}_other;"

    echo "write initial data and do snapshot backup"
    create_tables_with_values "full_backup" 3

    run_br backup full -f "${DB}_other.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "write more data and wait for log backup to catch up"
    run_sql "create table ${DB}_other.test_table(c int); insert into ${DB}_other.test_table values (42);"
    create_tables_with_values "log_backup" 3

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # restart services to clean up the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 1 sanity check, zero filter"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full"

    verify_tables "log_backup" 3 false
    verify_tables "full_backup" 3 false
    verify_other_db_tables true

    echo "case 2 with log backup table same filter"
    run_sql "drop schema ${DB}_other;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_other.*"

    verify_tables "log_backup" 3 false
    verify_tables "full_backup" 3 false
    verify_other_db_tables true

    echo "case 3 with log backup filter include nothing"
    run_sql "drop schema ${DB}_other;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_nothing.*"

    verify_tables "log_backup" 3 false
    verify_tables "full_backup" 3 false
    verify_other_db_tables false

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "with full backup filter test cases passed"
}

test_table_rename() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start table rename with filter testing"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    # create multiple schemas for cross-db rename testing
    run_sql "create schema $DB;"
    run_sql "create schema ${DB}_other1;"
    run_sql "create schema ${DB}_other2;"

    echo "write initial data and do snapshot backup"
    create_tables_with_values "full_backup" 3
    create_tables_with_values "renamed_in" 3
    create_tables_with_values "log_renamed_out" 3
    # add table for multiple rename test
    run_sql "create table ${DB}_other1.multi_rename(c int); insert into ${DB}_other1.multi_rename values (42);"

    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "write more data and wait for log backup to catch up"
    create_tables_with_values "log_backup" 3
    rename_tables "full_backup" "full_backup_renamed" 3
    rename_tables "log_backup" "log_backup_renamed" 3
    rename_tables "renamed_in" "log_backup_renamed_in" 3
    rename_tables "log_renamed_out" "renamed_out" 3
    
    # multiple renames across different databases
    run_sql "rename table ${DB}_other1.multi_rename to ${DB}_other2.multi_rename;"
    run_sql "rename table ${DB}_other2.multi_rename to $DB.log_multi_rename;"

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # restart services to clean up the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }

    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.log*"

    verify_tables "log_backup" 3 false
    verify_tables "log_backup_renamed" 3 true
    verify_tables "log_backup_renamed_in" 3 true

    verify_tables "full_backup" 3 false
    # has been renamed, should not visible anymore
    verify_tables "renamed_in" 3 false
    # also renamed out of filter range, should not be visible for both
    verify_tables "renamed_out" 3 false
    verify_tables "log_renamed_out" 3 false

    # verify multi-renamed table
    run_sql "select count(*) = 1 from $DB.log_multi_rename where c = 42" || {
        echo "Table multi_rename doesn't have expected value after multiple renames"
        exit 1
    }

    # Verify table doesn't exist in intermediate databases
    if run_sql "select * from ${DB}_other1.multi_rename" 2>/dev/null; then
        echo "Table exists in ${DB}_other1 but should not"
        exit 1
    fi
    if run_sql "select * from ${DB}_other2.multi_rename" 2>/dev/null; then
        echo "Table exists in ${DB}_other2 but should not"
        exit 1
    fi

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "table rename with filter passed"
}

test_with_checkpoint() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start table filter with checkpoint"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    echo "write initial data and do snapshot backup"
    create_tables_with_values "full_backup" 3
    create_tables_with_values "renamed_in" 3
    create_tables_with_values "log_renamed_out" 3

    run_br backup full -f "$DB.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "write more data and wait for log backup to catch up"
    create_tables_with_values "log_backup" 3
    rename_tables "renamed_in" "log_backup_renamed_in" 3
    rename_tables "log_renamed_out" "renamed_out" 3

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # restart services to clean up the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }

    # Using single quotes to prevent shell interpretation
    export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/restore/snap_client/corrupt-files=return("corrupt-last-table-files")'
    restore_fail=0
    run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -s "local://$TEST_DIR/$TASK_NAME/log" -f "$DB.log*" || restore_fail=1
    export GO_FAILPOINTS=""
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting full backup last table corruption but success'
        exit 1
    fi

    # PITR with checkpoint but failed in the log restore metakv stage
    export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/restore/snap_client/corrupt-files=return("only-last-table-files");github.com/pingcap/tidb/br/pkg/restore/log_client/failed-after-id-maps-saved=return(true)'
    restore_fail=0
    run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -s "local://$TEST_DIR/$TASK_NAME/log" -f "$DB.log*" || restore_fail=1
    export GO_FAILPOINTS=""
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting failed after id map saved but success'
        exit 1
    fi

    # PITR with checkpoint but failed in the log restore datakv stage
    # skip the snapshot restore stage
    export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/task/corrupt-files=return("corrupt-last-table-files")'
    restore_fail=0
    run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -s "local://$TEST_DIR/$TASK_NAME/log" -f "$DB.log*" || restore_fail=1
    export GO_FAILPOINTS=""
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting log restore last table corruption but success'
        exit 1
    fi

    # PITR with checkpoint
    export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/task/corrupt-files=return("only-last-table-files")'
    run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -s "local://$TEST_DIR/$TASK_NAME/log" -f "$DB.log*"
    export GO_FAILPOINTS=""

    verify_tables "log_backup" 3 true
    verify_tables "log_backup_renamed_in" 3 true

    verify_tables "full_backup" 3 false
    # has been renamed, should not visible anymore
    verify_tables "renamed_in" 3 false
    # also renamed out of filter range, should not be visible for both
    verify_tables "renamed_out" 3 false
    verify_tables "log_renamed_out" 3 false

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "table filter checkpoint passed"
}

test_exchange_partition() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start testing exchange partition with filter"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    # create a partitioned table and a normal table for exchange
    run_sql "CREATE TABLE $DB.full_partitioned (
        id INT,
        value INT
    ) PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (200)
    );"

    run_sql "CREATE TABLE $DB.log_table (
        id INT,
        value INT
    );"

    run_sql "INSERT INTO $DB.full_partitioned VALUES (50, 1), (150, 2);"
    run_sql "INSERT INTO $DB.log_table VALUES (75, 3);"

    run_br backup full -f "$DB.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    # exchange partition and create some new tables with log_ prefix
    run_sql "ALTER TABLE $DB.full_partitioned EXCHANGE PARTITION p0 WITH TABLE $DB.log_table;"
    run_sql "CREATE TABLE $DB.log_after_exchange (id INT, value INT);"
    run_sql "INSERT INTO $DB.log_after_exchange VALUES (1, 1);"

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    restart_services || { echo "Failed to restart services"; exit 1; }

    # Test case 1: Restore with full* filter
    echo "Testing restoration with full* filter"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.full*"

    # Verify full_partitioned table has the exchanged data
    run_sql "SELECT count(*) = 1 FROM $DB.full_partitioned WHERE id = 75 AND value = 3" || {
        echo "full_partitioned doesn't have the exchanged partition data (75,3)"
        exit 1
    }
    run_sql "SELECT count(*) = 1 FROM $DB.full_partitioned WHERE id = 150 AND value = 2" || {
        echo "full_partitioned missing original data (150,2)"
        exit 1
    }

    # log_table and log_after_exchange should not exist with full* filter
    if run_sql "SELECT * FROM $DB.log_table" 2>/dev/null; then
        echo "log_table exists but should not with full* filter"
        exit 1
    fi
    if run_sql "SELECT * FROM $DB.log_after_exchange" 2>/dev/null; then
        echo "log_after_exchange exists but should not with full* filter"
        exit 1
    fi

    # Clean up for next test
    run_sql "drop schema $DB;"

    # Test case 2: Restore with log* filter
    echo "Testing restoration with log* filter"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.log*"

    # Verify log tables exist with correct data
    run_sql "SELECT count(*) = 1 FROM $DB.log_table WHERE id = 50 AND value = 1" || {
        echo "log_table doesn't have the exchanged partition data (50,1)"
        exit 1
    }
    run_sql "SELECT count(*) = 1 FROM $DB.log_after_exchange WHERE id = 1 AND value = 1" || {
        echo "log_after_exchange missing its data (1,1)"
        exit 1
    }

    # full_partitioned should not exist with log* filter
    if run_sql "SELECT * FROM $DB.full_partitioned" 2>/dev/null; then 
        echo "full_partitioned exists but should not with log* filter"
        exit 1
    fi

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "exchange partition with filter test passed"
}

test_system_tables() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start system tables testing"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    echo "write initial data and do snapshot backup"
    # create and populate a user table for reference
    run_sql "create table $DB.user_table(id int primary key);"
    run_sql "insert into $DB.user_table values (1);"
    
    # make some changes to system tables
    run_sql "create user 'test_user'@'%' identified by 'password';"
    run_sql "grant select on $DB.* to 'test_user'@'%';"

    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "make more changes to system tables and wait for log backup"
    run_sql "revoke select on $DB.* from 'test_user'@'%';"
    run_sql "grant insert on $DB.* to 'test_user'@'%';"
    run_sql "alter user 'test_user'@'%' identified by 'newpassword';"

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "PiTR should error out when system tables are included with explicit filter"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -f "*.*" -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Expected restore to fail when including system tables with filter"
        exit 1
    fi

    # Also verify that specific system table filters fail
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -f "mysql.*" -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Expected restore to fail when explicitly filtering system tables"
        exit 1
    fi

    rm -rf "$TEST_DIR/$TASK_NAME"
    echo "system tables test passed"
}

# run all test cases
test_basic_filter
test_with_full_backup_filter
test_table_rename
test_with_checkpoint
test_exchange_partition
test_system_tables

echo "br pitr table filter all tests passed"
