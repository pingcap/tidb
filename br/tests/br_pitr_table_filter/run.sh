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

# Helper function to verify no unexpected tables exist - add at the top level for all tests to use
verify_no_unexpected_tables() {
    local expected_count=$1
    local schema=$2
    
    # Get count of all tables and views, using awk to extract just the number
    actual_count=$(run_sql "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$schema' AND table_type IN ('BASE TABLE', 'VIEW')" | awk 'NR==2 {print $2}')
    
    if [ "$actual_count" -ne "$expected_count" ]; then
        echo "Found wrong number of tables in schema $schema. Expected: $expected_count, got: $actual_count"
        # Print the actual tables to help debugging
        run_sql "SELECT table_name FROM information_schema.tables WHERE table_schema='$schema' AND table_type IN ('BASE TABLE', 'VIEW')"
        return 1
    fi
    return 0
}

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
    verify_other_db_tables true
    verify_no_unexpected_tables 12 "$DB" || {
        echo "Found unexpected number of tables in case 1"
        exit 1
    }
    verify_no_unexpected_tables 1 "${DB}_other" || {
        echo "Found unexpected number of tables in ${DB}_other in case 1"
        exit 1
    }

    echo "case 2 with log restore table filter"
    run_sql "drop schema $DB;"
    run_sql "drop schema ${DB}_other;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.log*"

    verify_tables "log_backup_lower" 3 true
    verify_tables "LOG_BACKUP_UPPER" 3 true
    verify_no_unexpected_tables 6 "$DB" || {
        echo "Found unexpected number of tables in case 2"
        exit 1
    }
    verify_no_unexpected_tables 0 "${DB}_other" || {
        echo "Found unexpected number of tables in ${DB}_other in case 2"
        exit 1
    }

    echo "case 3 with multiple filters"
    run_sql "drop schema $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.log*" -f "$DB.full*"

    verify_tables "log_backup_lower" 3 true
    verify_tables "LOG_BACKUP_UPPER" 3 true
    verify_tables "full_backup" 3 true
    verify_no_unexpected_tables 9 "$DB" || {
        echo "Found unexpected number of tables in case 3"
        exit 1
    }
    verify_no_unexpected_tables 0 "${DB}_other" || {
        echo "Found unexpected number of tables in ${DB}_other in case 3"
        exit 1
    }

    echo "case 4 with negative filters"
    run_sql "drop schema $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "*.*" -f "!mysql.*" -f "!$DB.log*"

    verify_tables "full_backup" 3 true
    verify_tables "other" 3 true
    verify_other_db_tables true
    verify_no_unexpected_tables 6 "$DB" || {
        echo "Found unexpected number of tables in case 4"
        exit 1
    }
    verify_no_unexpected_tables 1 "${DB}_other" || {
        echo "Found unexpected number of tables in ${DB}_other in case 4"
        exit 1
    }

    echo "case 5 restore dropped table"
    run_sql "drop schema $DB;"
    run_sql "drop schema ${DB}_other;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.table*"

    verify_no_unexpected_tables 0 "$DB" || {
        echo "Found unexpected number of tables in case 5"
        exit 1
    }
    verify_no_unexpected_tables 0 "${DB}_other" || {
        echo "Found tables in ${DB}_other but none should exist in case 5"
        exit 1
    }

    echo "case 6 restore only other database"
    run_sql "drop schema $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_other.*"

    verify_other_db_tables true
    verify_no_unexpected_tables 0 "$DB" || {
        echo "Found tables in $DB but none should exist"
        exit 1
    }
    verify_no_unexpected_tables 1 "${DB}_other" || {
        echo "Found unexpected number of tables in ${DB}_other"
        exit 1
    }

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

    echo "case 7 sanity check, zero filter"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full"

    verify_other_db_tables true
    verify_no_unexpected_tables 0 "$DB" || {
        echo "Found unexpected number of tables in case 7"
        exit 1
    }
    verify_no_unexpected_tables 1 "${DB}_other" || {
        echo "Found unexpected number of tables in other database in case 7"
        exit 1
    }

    echo "case 8 with log backup table same filter"
    run_sql "drop schema ${DB}_other;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_other.*"

    verify_other_db_tables true
    verify_no_unexpected_tables 0 "$DB" || {
        echo "Found unexpected number of tables in case 8"
        exit 1
    }
    verify_no_unexpected_tables 1 "${DB}_other" || {
        echo "Found unexpected number of tables in other database in case 8"
        exit 1
    }

    echo "case 9 with log backup filter include nothing"
    run_sql "drop schema ${DB}_other;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_nothing.*"

    verify_no_unexpected_tables 0 "$DB" || {
        echo "Found unexpected number of tables in case 9"
        exit 1
    }
    verify_no_unexpected_tables 0 "${DB}_other" || {
        echo "Found unexpected number of tables in other database in case 9"
        exit 1
    }

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

    verify_tables "log_backup_renamed" 3 true
    verify_tables "log_backup_renamed_in" 3 true
    # verify multi-renamed table
    run_sql "select count(*) = 1 from $DB.log_multi_rename where c = 42" || {
        echo "Table multi_rename doesn't have expected value after multiple renames"
        exit 1
    }
    verify_no_unexpected_tables 7 "$DB" || {
        echo "Found unexpected number of tables after rename test"
        exit 1
    }
    verify_no_unexpected_tables 0 "${DB}_other1" || {
        echo "Found unexpected number of tables in other1 database in case 7"
        exit 1
    }
    verify_no_unexpected_tables 0 "${DB}_other2" || {
        echo "Found unexpected number of tables in other2 database in case 7"
        exit 1
    }

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

    verify_no_unexpected_tables 6 "$DB" || {
        echo "Found unexpected number of tables after checkpoint test"
        exit 1
    }

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

    # Verify table counts in both databases
    verify_no_unexpected_tables 2 "$DB" || {
        echo "Found unexpected number of tables in exchange partition test"
        exit 1
    }
    verify_no_unexpected_tables 0 "${DB}_other" || {
        echo "Found unexpected number of tables in ${DB}_other"
        exit 1
    }

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

test_foreign_keys() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start testing filters with foreign key relationships"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    run_sql "CREATE TABLE $DB.departments (
        dept_id INT PRIMARY KEY,
        dept_name VARCHAR(50)
    );"
    run_sql "CREATE TABLE $DB.employees (
        emp_id INT PRIMARY KEY,
        emp_name VARCHAR(50),
        dept_id INT,
        salary INT,
        FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
    );"
    
    run_sql "INSERT INTO $DB.departments VALUES (1, 'Engineering'), (2, 'Sales');"
    run_sql "INSERT INTO $DB.employees VALUES (1, 'John', 1, 5000), (2, 'Jane', 2, 6000);"

    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "make changes and wait for log backup"
    
    run_sql "INSERT INTO $DB.departments VALUES (3, 'Marketing');"
    run_sql "INSERT INTO $DB.employees VALUES (3, 'Bob', 3, 5500);"

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "Restore only employees table - should succeed but queries should fail"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.employees"

    # verify the employees table exists
    run_sql "SELECT COUNT(*) FROM $DB.employees" || {
        echo "Failed to select from employees table"
        exit 1
    }

    # verify queries involving foreign key fail
    query_fail=0
    run_sql "SELECT e.emp_name, d.dept_name FROM $DB.employees e JOIN $DB.departments d ON e.dept_id = d.dept_id" || query_fail=1
    if [ $query_fail -ne 1 ]; then
        echo "Expected JOIN query to fail due to missing departments table"
        exit 1
    fi

    # verify insert succeeds even without the referenced table
    run_sql "INSERT INTO $DB.employees VALUES (4, 'Alice', 1, 6000)" || {
        echo "Failed to insert into employees table"
        exit 1
    }
    # Verify the insert worked
    run_sql "SELECT COUNT(*) = 1 FROM $DB.employees WHERE emp_id = 4" || {
        echo "Newly inserted row not found in employees table"
        exit 1
    }

    run_sql "drop schema $DB;"

    echo "Test case 2: Restore both tables - should succeed"
    # restore both tables - should succeed
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.employees" -f "$DB.departments"

    # verify foreign key relationship works
    run_sql "SELECT COUNT(*) = 3 FROM $DB.employees e JOIN $DB.departments d ON e.dept_id = d.dept_id" || {
        echo "Foreign key relationship not working after restore"
        exit 1
    }

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "schema objects test passed"
}

test_index_filter() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start testing indexes with filter"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    # create tables with different index types
    run_sql "CREATE TABLE $DB.btree_index_table (
        id INT PRIMARY KEY,
        name VARCHAR(50),
        age INT,
        INDEX idx_name_age(name, age) USING BTREE
    );"

    run_sql "CREATE TABLE $DB.hash_index_table (
        id INT,
        value INT,
        PRIMARY KEY(id) USING HASH,
        INDEX idx_value(value) USING HASH
    );"

    run_sql "CREATE TABLE $DB.multi_index_table (
        id INT PRIMARY KEY,
        data JSON,
        INDEX idx_multi((CAST(data->'$.tags' AS CHAR(64) ARRAY)))
    );"

    run_sql "INSERT INTO $DB.btree_index_table VALUES (1, 'Alice', 25), (2, 'Bob', 30);"
    run_sql "INSERT INTO $DB.hash_index_table VALUES (1, 100), (2, 200);"
    run_sql "INSERT INTO $DB.multi_index_table VALUES 
        (1, '{\"tags\": [\"tag1\", \"tag2\"]}'),
        (2, '{\"tags\": [\"tag3\", \"tag4\"]}');"

    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    run_sql "INSERT INTO $DB.btree_index_table VALUES (3, 'Charlie', 35);"
    run_sql "INSERT INTO $DB.hash_index_table VALUES (3, 300);"
    run_sql "INSERT INTO $DB.multi_index_table VALUES (3, '{\"tags\": [\"tag5\", \"tag6\"]}');"
    
    # add new indexes after backup
    run_sql "ALTER TABLE $DB.btree_index_table ADD INDEX idx_age(age);"
    run_sql "ALTER TABLE $DB.hash_index_table ADD UNIQUE INDEX uniq_value(value);"

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    restart_services || { echo "Failed to restart services"; exit 1; }

    # Test case 1: Restore btree index table
    echo "Testing restoration of btree index table"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.btree*"

    # verify data and indexes
    run_sql "SELECT COUNT(*) = 3 FROM $DB.btree_index_table" || {
        echo "btree_index_table doesn't have expected row count"
        exit 1
    }

    # verify index structure exists
    run_sql "SHOW INDEX FROM $DB.btree_index_table WHERE Key_name = 'idx_name_age'" || {
        echo "idx_name_age index not found in btree_index_table"
        exit 1
    }
    run_sql "SHOW INDEX FROM $DB.btree_index_table WHERE Key_name = 'idx_age'" || {
        echo "idx_age index not found in btree_index_table"
        exit 1
    }

    # verify indexes are being used in queries
    run_sql "EXPLAIN SELECT * FROM $DB.btree_index_table WHERE name = 'Alice' AND age = 25" || {
        echo "Failed to use idx_name_age index on btree_index_table"
        exit 1
    }
    run_sql "EXPLAIN SELECT * FROM $DB.btree_index_table WHERE age = 25" || {
        echo "Failed to use idx_age index on btree_index_table"
        exit 1
    }

    # other tables should not exist
    if run_sql "SELECT * FROM $DB.hash_index_table" 2>/dev/null; then
        echo "hash_index_table exists but should not with btree* filter"
        exit 1
    fi

    run_sql "drop schema $DB;"

    # Test case 2: Restore all tables with indexes
    echo "Testing restoration of all tables with indexes"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.*"

    # verify all tables exist with correct data
    run_sql "SELECT COUNT(*) = 3 FROM $DB.btree_index_table" || {
        echo "btree_index_table doesn't have expected row count"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 3 FROM $DB.hash_index_table" || {
        echo "hash_index_table doesn't have expected row count"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 3 FROM $DB.multi_index_table" || {
        echo "multi_index_table doesn't have expected row count"
        exit 1
    }

    # verify index structures exist
    echo "Verifying index structures..."
    run_sql "SHOW INDEX FROM $DB.btree_index_table" || {
        echo "Failed to show indexes from btree_index_table"
        exit 1
    }
    run_sql "SHOW INDEX FROM $DB.hash_index_table" || {
        echo "Failed to show indexes from hash_index_table"
        exit 1
    }
    run_sql "SHOW INDEX FROM $DB.multi_index_table" || {
        echo "Failed to show indexes from multi_index_table"
        exit 1
    }

    # verify indexes are being used in queries
    echo "Verifying index usage in queries..."
    run_sql "EXPLAIN SELECT * FROM $DB.btree_index_table WHERE name = 'Alice' AND age = 25" || {
        echo "Failed to use idx_name_age index on btree_index_table"
        exit 1
    }
    run_sql "EXPLAIN SELECT * FROM $DB.hash_index_table WHERE value = 100" || {
        echo "Failed to use idx_value index on hash_index_table"
        exit 1
    }
    run_sql "EXPLAIN SELECT * FROM $DB.multi_index_table WHERE JSON_CONTAINS(data->'$.tags', '\"tag1\"')" || {
        echo "Failed to use idx_multi index on multi_index_table"
        exit 1
    }

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "Index filter test passed"
}

 run all test cases
 test_basic_filter
 test_with_full_backup_filter
 test_table_rename
 test_with_checkpoint
 test_exchange_partition
 test_system_tables
 test_foreign_keys
test_index_filter

echo "br pitr table filter all tests passed"
