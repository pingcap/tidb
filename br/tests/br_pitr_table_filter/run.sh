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

# disable global ENCRYPTION_ARGS and ENABLE_ENCRYPTION_CHECK for this script
ENCRYPTION_ARGS=""
ENABLE_ENCRYPTION_CHECK=false
export ENCRYPTION_ARGS
export ENABLE_ENCRYPTION_CHECK

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

test_partition_exchange() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start testing partition exchange with filter"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    # Create tables that will be in backup
    echo "creating tables for backup..."
    run_sql "CREATE TABLE $DB.backup_source (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    ) PARTITION BY RANGE (value) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (200)
    );"

    run_sql "CREATE TABLE $DB.backup_target1 (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    );"

    run_sql "CREATE TABLE $DB.backup_target2 (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    );"

    # Insert data into backup tables
    run_sql "INSERT INTO $DB.backup_source VALUES (1, 50), (2, 150);"
    run_sql "INSERT INTO $DB.backup_target1 VALUES (3, 50);"
    run_sql "INSERT INTO $DB.backup_target2 VALUES (4, 150);"

    # Take full backup
    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    # Create tables that will only exist in log
    echo "creating tables that will only exist in log..."
    run_sql "CREATE TABLE $DB.log_source (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    ) PARTITION BY RANGE (value) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (200)
    );"

    run_sql "CREATE TABLE $DB.log_target1 (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    );"

    run_sql "CREATE TABLE $DB.log_target2 (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    );"

    # Insert data into log-only tables
    run_sql "INSERT INTO $DB.log_source VALUES (5, 50), (6, 150);"
    run_sql "INSERT INTO $DB.log_target1 VALUES (7, 50);"
    run_sql "INSERT INTO $DB.log_target2 VALUES (8, 150);"

    echo "performing all partition exchange operations..."

    # Case 1: Exchange between backup tables
    run_sql "ALTER TABLE $DB.backup_source EXCHANGE PARTITION p0 WITH TABLE $DB.backup_target1;"

    # Case 2: Exchange between log-only tables
    run_sql "ALTER TABLE $DB.log_source EXCHANGE PARTITION p0 WITH TABLE $DB.log_target1;"

    # Case 3: Exchange between backup source and log target
    run_sql "ALTER TABLE $DB.backup_source EXCHANGE PARTITION p1 WITH TABLE $DB.log_target2;"

    # Case 4: Exchange between log source and backup target
    run_sql "ALTER TABLE $DB.log_source EXCHANGE PARTITION p1 WITH TABLE $DB.backup_target2;"

    # Wait for log backup to catch up with all operations
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # Stop log backup before starting restore tests
    run_br log stop --task-name $TASK_NAME

    echo "starting restore tests..."

    # Test 1: Backup source and all in filter - should succeed
    echo "test 1: backup source and all in filter"
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.backup_source" -f "$DB.backup_target1" -f "$DB.log_target2"|| {
        echo "Failed: backup source and all in filter should succeed"
        exit 1
    }
    # Verify data after restore
    run_sql "SELECT COUNT(*) = 1 FROM $DB.backup_source PARTITION (p0) WHERE id = 3 AND value = 50" || {
        echo "backup_source p0 doesn't have expected data after restore"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 1 FROM $DB.backup_target1 WHERE id = 1 AND value = 50" || {
        echo "backup_target1 doesn't have expected data after restore"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 1 FROM $DB.log_target2 WHERE id = 2 AND value = 150" || {
        echo "backup_target1 doesn't have expected data after restore"
        exit 1
    }

    # Test 2: Log source and all in filter - should succeed
    echo "test 2: log source and all in filter"
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.log_source" -f "$DB.log_target1" -f "$DB.backup_target2" || {
        echo "Failed: log source and all in filter should succeed"
        exit 1
    }
    # Verify data after restore
    run_sql "SELECT COUNT(*) = 1 FROM $DB.log_source PARTITION (p0) WHERE id = 7 AND value = 50" || {
        echo "log_source p0 doesn't have expected data after restore"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 1 FROM $DB.log_target1 WHERE id = 5 AND value = 50" || {
        echo "log_target1 doesn't have expected data after restore"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 1 FROM $DB.backup_target2 WHERE id = 6 AND value = 150" || {
        echo "backup_target1 doesn't have expected data after restore"
        exit 1
    }

    # Test 3: Only backup source in filter - should fail
    echo "test 3: only backup source in filter"
    run_sql "drop schema if exists $DB;"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.backup_source" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: backup source only in filter should fail"
        exit 1
    fi
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.backup_source" 2>&1 | grep "partition exchange detected" || {
        echo "Error message does not contain partition exchange information"
        exit 1
    }

    # Test 4: Only backup target in filter - should fail
    echo "test 4: only backup target in filter"
    run_sql "drop schema if exists $DB;"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.backup_target1" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: backup target only in filter should fail"
        exit 1
    fi

    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.backup_target2" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: backup target only in filter should fail"
        exit 1
    fi

    # Test 5: Only log source in filter - should fail
    echo "test 5: only log source in filter"
    run_sql "drop schema if exists $DB;"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.log_source" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: log source only in filter should fail"
        exit 1
    fi

    # Test 6: Only log target in filter - should fail
    echo "test 6: only log target in filter"
    run_sql "drop schema if exists $DB;"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.log_target1" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: log target only in filter should fail"
        exit 1
    fi

    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.log_target2" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: log target only in filter should fail"
        exit 1
    fi

    # Test 7: Neither table in filter - should succeed with no tables
    echo "test 7: neither table in filter"
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.unrelated_table" || {
        echo "Failed: neither table in filter should succeed"
        exit 1
    }
    # Verify no tables were restored
    verify_no_unexpected_tables 0 "$DB" || {
        echo "Found unexpected tables after neither table in filter"
        exit 1
    }

    # Test 8: Wildcard filter including all tables - should succeed
    echo "test 8: wildcard filter including all tables"
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.*" || {
        echo "Failed: wildcard filter should succeed"
        exit 1
    }
    # Verify all tables are restored
    verify_no_unexpected_tables 6 "$DB" || {
        echo "Wrong number of tables restored with wildcard filter"
        exit 1
    }

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "partition exchange test passed"
}

test_table_truncation() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start testing table truncation with filter"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    # Create tables for snapshot backup
    echo "creating tables for snapshot backup..."
    run_sql "CREATE TABLE $DB.snapshot_truncate (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    
    # Insert initial data
    run_sql "INSERT INTO $DB.snapshot_truncate VALUES (1, 'initial data 1'), (2, 'initial data 2');"
    
    # Take full backup
    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    # Create tables during log backup phase
    echo "creating tables during log backup phase..."
    run_sql "CREATE TABLE $DB.log_truncate (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    
    # Insert data into log-created table
    run_sql "INSERT INTO $DB.log_truncate VALUES (1, 'log data 1'), (2, 'log data 2');"
    
    # Add more data to snapshot table before truncation
    run_sql "INSERT INTO $DB.snapshot_truncate VALUES (3, 'pre-truncate data');"
    
    # Truncate both tables
    echo "truncating tables..."
    run_sql "TRUNCATE TABLE $DB.snapshot_truncate;"
    run_sql "TRUNCATE TABLE $DB.log_truncate;"
    
    # Insert new data after truncation
    run_sql "INSERT INTO $DB.snapshot_truncate VALUES (10, 'post-truncate data 1'), (20, 'post-truncate data 2');"
    run_sql "INSERT INTO $DB.log_truncate VALUES (10, 'post-truncate data 1'), (20, 'post-truncate data 2');"
    
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    # Stop log backup before starting restore operations
    run_br log stop --task-name $TASK_NAME --pd $PD_ADDR
    
    run_sql "drop schema if exists $DB;"

    echo "Test 1: Restore both truncated tables"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.*"
    
    # Verify data after restore - should only have post-truncate data
    run_sql "SELECT COUNT(*) = 2 FROM $DB.snapshot_truncate" || {
        echo "snapshot_truncate doesn't have expected row count after restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 2 FROM $DB.log_truncate" || {
        echo "log_truncate doesn't have expected row count after restore"
        exit 1
    }
    
    # Verify specific values to ensure we have post-truncate data
    run_sql "SELECT COUNT(*) = 1 FROM $DB.snapshot_truncate WHERE id = 10" || {
        echo "snapshot_truncate doesn't have expected post-truncate data"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 1 FROM $DB.log_truncate WHERE id = 10" || {
        echo "log_truncate doesn't have expected post-truncate data"
        exit 1
    }
    
    # Verify pre-truncate data is gone
    run_sql "SELECT COUNT(*) = 0 FROM $DB.snapshot_truncate WHERE id IN (1, 2, 3)" || {
        echo "snapshot_truncate still has pre-truncate data which should be gone"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 0 FROM $DB.log_truncate WHERE id IN (1, 2)" || {
        echo "log_truncate still has pre-truncate data which should be gone"
        exit 1
    }
    
    # Test 2: Restore only snapshot table
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.snapshot_truncate"
    
    # Verify only snapshot table exists
    verify_no_unexpected_tables 1 "$DB" || {
        echo "Wrong number of tables restored with snapshot_truncate filter"
        exit 1
    }
    
    # Verify data is correct
    run_sql "SELECT COUNT(*) = 2 FROM $DB.snapshot_truncate" || {
        echo "snapshot_truncate doesn't have expected row count after filtered restore"
        exit 1
    }
    
    # Test 3: Restore only log table
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.log_truncate"
    
    # Verify only log table exists
    verify_no_unexpected_tables 1 "$DB" || {
        echo "Wrong number of tables restored with log_truncate filter"
        exit 1
    }
    
    # Verify data is correct
    run_sql "SELECT COUNT(*) = 2 FROM $DB.log_truncate" || {
        echo "log_truncate doesn't have expected row count after filtered restore"
        exit 1
    }
    
    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"
    
    echo "table truncation test passed"
}

test_sequential_restore() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start testing sequential table restore with filter"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    # Create multiple tables with different data
    echo "creating tables for testing sequential restore..."
    run_sql "CREATE TABLE $DB.table1 (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    run_sql "CREATE TABLE $DB.table2 (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    run_sql "CREATE TABLE $DB.table3 (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    
    # Insert initial data
    run_sql "INSERT INTO $DB.table1 VALUES (1, 'table1 data 1'), (2, 'table1 data 2');"
    run_sql "INSERT INTO $DB.table2 VALUES (1, 'table2 data 1'), (2, 'table2 data 2');"
    run_sql "INSERT INTO $DB.table3 VALUES (1, 'table3 data 1'), (2, 'table3 data 2');"
    
    # Take full backup
    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    # Add more data after backup
    run_sql "INSERT INTO $DB.table1 VALUES (3, 'table1 data 3'), (4, 'table1 data 4');"
    run_sql "INSERT INTO $DB.table2 VALUES (3, 'table2 data 3'), (4, 'table2 data 4');"
    run_sql "INSERT INTO $DB.table3 VALUES (3, 'table3 data 3'), (4, 'table3 data 4');"
    
    # Wait for log backup to catch up with all operations
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    # Stop log backup before starting restore operations
    run_br log stop --task-name $TASK_NAME --pd $PD_ADDR
    
    # Clean up the database before starting the sequential restore tests
    run_sql "drop schema if exists $DB;"
    
    echo "Test 1: Restore first table"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.table1"
    
    # Verify only table1 exists with correct data
    verify_no_unexpected_tables 1 "$DB" || {
        echo "Wrong number of tables after restoring table1"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table1" || {
        echo "table1 doesn't have expected row count after restore"
        exit 1
    }
    
    echo "Test 2: Restore second table without cleaning up"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.table2"
    
    # Verify both table1 and table2 exist with correct data
    verify_no_unexpected_tables 2 "$DB" || {
        echo "Wrong number of tables after restoring table2"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table1" || {
        echo "table1 doesn't have expected row count after second restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table2" || {
        echo "table2 doesn't have expected row count after restore"
        exit 1
    }
    
    echo "Test 3: Restore third table without cleaning up"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.table3"
    
    # Verify all three tables exist with correct data
    verify_no_unexpected_tables 3 "$DB" || {
        echo "Wrong number of tables after restoring table3"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table1" || {
        echo "table1 doesn't have expected row count after third restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table2" || {
        echo "table2 doesn't have expected row count after third restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table3" || {
        echo "table3 doesn't have expected row count after restore"
        exit 1
    }
    
    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"
    
    echo "sequential restore test passed"
}

test_log_compaction() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start testing table filter with log compaction"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    # Create tables for snapshot backup
    echo "creating tables for snapshot backup..."
    run_sql "CREATE TABLE $DB.compaction_snapshot (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    
    # Insert initial data
    run_sql "INSERT INTO $DB.compaction_snapshot VALUES (1, 'initial data 1'), (2, 'initial data 2');"
    
    # Take full backup
    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    # Create tables during log backup phase
    echo "creating tables during log backup phase..."
    run_sql "CREATE TABLE $DB.compaction_log (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    
    # Insert data into log-created table
    run_sql "INSERT INTO $DB.compaction_log VALUES (1, 'log data 1'), (2, 'log data 2');"
    
    # Add more data to snapshot table
    run_sql "INSERT INTO $DB.compaction_snapshot VALUES (3, 'more data 1'), (4, 'more data 2');"
    
    # Wait for log backup to catch up with all operations
    current_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    # Verify no SST files exist before compaction
    pre_compaction_files=$(find "$TEST_DIR/$TASK_NAME/log" -name "*.sst" | wc -l)
    if [ "$pre_compaction_files" -ne 0 ]; then
        echo "Found $pre_compaction_files SST files before compaction, expected 0"
        exit 1
    fi
    echo "Verified no SST files exist before compaction"
    
    # Step 1: Get the Base64 encoded storage URL
    echo "Encoding storage URL to Base64"

    # Run the base64ify command and capture its output, redirecting stderr to stdout
    base64_output=$(run_br operator base64ify --storage "local://$TEST_DIR/$TASK_NAME/log" 2>&1)

    # Extract only lines that look like Base64 (long string of base64 chars)
    storage_base64=$(echo "$base64_output" | grep -o '[A-Za-z0-9+/]\{20,\}=\{0,2\}' | grep '^E' | head -1)

    # Verify that we got a valid Base64 string
    if [ -z "$storage_base64" ]; then
        echo "Failed to extract Base64 encoded storage URL. Full output:"
        echo "$base64_output"
        exit 1
    fi

    echo "Extracted Base64 encoded storage URL: $storage_base64"
    
    # Get current timestamp and a timestamp from 1 hour ago for compaction range
    one_hour_ago_ts=$(python3 -c "import time; print(int((time.time() - 3600) * 1000) << 18)")
    
    echo "Current timestamp: $current_ts"
    echo "One hour ago timestamp: $one_hour_ago_ts"
    
    echo "Compacting logs from $one_hour_ago_ts to $current_ts"
    
    # Run tikv-ctl to perform compaction
    tikv-ctl --log-level=info compact-log-backup --from "$one_hour_ago_ts" --until "$current_ts" -s "$storage_base64" -N 4 --minimal-compaction-size 0

    # Verify SST files exist after compaction
    post_compaction_files=$(find "$TEST_DIR/$TASK_NAME/log" -name "*.sst" | wc -l)
    if [ "$post_compaction_files" -eq 0 ]; then
        echo "No SST files found after compaction, expected at least 1"
        exit 1
    fi
    echo "Verified $post_compaction_files SST files exist after compaction"

    # Add more data after compaction
    run_sql "INSERT INTO $DB.compaction_snapshot VALUES (5, 'post-compaction data 1');"
    run_sql "INSERT INTO $DB.compaction_log VALUES (3, 'post-compaction data 2');"
    
    # Wait for log backup to catch up again
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    # Stop log backup before starting restore operations
    run_br log stop --task-name $TASK_NAME --pd $PD_ADDR
    
    run_sql "drop schema if exists $DB;"

    echo "Test 1: Restore both tables with compacted logs"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.*"
    
    # Verify data after restore - should have all data including post-compaction
    run_sql "SELECT COUNT(*) = 5 FROM $DB.compaction_snapshot" || {
        echo "compaction_snapshot doesn't have expected row count after restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 3 FROM $DB.compaction_log" || {
        echo "compaction_log doesn't have expected row count after restore"
        exit 1
    }
    
    # Verify specific values to ensure we have post-compaction data
    run_sql "SELECT COUNT(*) = 1 FROM $DB.compaction_snapshot WHERE id = 5" || {
        echo "compaction_snapshot doesn't have expected post-compaction data"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 1 FROM $DB.compaction_log WHERE id = 3" || {
        echo "compaction_log doesn't have expected post-compaction data"
        exit 1
    }
    
    # Test 2: Restore only snapshot table with filter
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.compaction_snapshot"
    
    # Verify only snapshot table exists
    verify_no_unexpected_tables 1 "$DB" || {
        echo "Wrong number of tables restored with compaction_snapshot filter"
        exit 1
    }
    
    # Verify data is correct
    run_sql "SELECT COUNT(*) = 5 FROM $DB.compaction_snapshot" || {
        echo "compaction_snapshot doesn't have expected row count after filtered restore"
        exit 1
    }
    
    # Test 3: Restore only log table with filter
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.compaction_log"
    
    # Verify only log table exists
    verify_no_unexpected_tables 1 "$DB" || {
        echo "Wrong number of tables restored with compaction_log filter"
        exit 1
    }
    
    # Verify data is correct
    run_sql "SELECT COUNT(*) = 3 FROM $DB.compaction_log" || {
        echo "compaction_log doesn't have expected row count after filtered restore"
        exit 1
    }
    
    rm -rf "$TEST_DIR/$TASK_NAME"
    
    echo "log compaction with filter test passed"
}

echo "run all test cases"
test_basic_filter
test_with_full_backup_filter
test_table_rename
test_with_checkpoint
test_system_tables
test_foreign_keys
test_index_filter
test_partition_exchange
test_table_truncation
test_sequential_restore
test_log_compaction

echo "br pitr table filter all tests passed"
