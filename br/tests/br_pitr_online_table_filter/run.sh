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
DB="$TEST_NAME"
CUR=$(cd `dirname $0`; pwd)
TASK_NAME="pitr_online_table_filter"
. run_services

# Helper function to verify no unexpected tables exist
verify_no_unexpected_tables() {
    local expected_count=$1
    local schema=$2
    
    # Get count of all tables and views
    actual_count=$(run_sql "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$schema' AND table_type IN ('BASE TABLE', 'VIEW')" | awk 'NR==2 {print $2}')
    
    if [ "$actual_count" -ne "$expected_count" ]; then
        echo "Found wrong number of tables in schema $schema. Expected: $expected_count, got: $actual_count"
        # Print the actual tables to help debugging
        run_sql "SELECT table_name FROM information_schema.tables WHERE table_schema='$schema' AND table_type IN ('BASE TABLE', 'VIEW')"
        return 1
    fi
    return 0
}

test_online_filter_restore() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start testing overlapping database restore"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    # Create and populate tables in the test database
    run_sql "CREATE TABLE $DB.table1 (id INT PRIMARY KEY, value INT);"
    run_sql "CREATE TABLE $DB.table2 (id INT PRIMARY KEY, value INT);"
    run_sql "INSERT INTO $DB.table1 VALUES (1, 100), (2, 200);"
    run_sql "INSERT INTO $DB.table2 VALUES (1, 1000), (2, 2000);"
    
    # add some ddl operations
    run_sql "ALTER TABLE $DB.table1 ADD COLUMN name VARCHAR(50);"
    run_sql "ALTER TABLE $DB.table2 MODIFY COLUMN value BIGINT;"
    run_sql "UPDATE $DB.table1 SET name = CONCAT('user', id) WHERE id IN (1, 2);"

    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    # Make some changes after backup
    run_sql "INSERT INTO $DB.table1 VALUES (3, 300, 'user3');"
    run_sql "INSERT INTO $DB.table2 VALUES (3, 3000);"
    run_sql "CREATE TABLE $DB.table3 (id INT PRIMARY KEY, value INT);"
    run_sql "INSERT INTO $DB.table3 VALUES (1, 10000), (2, 20000);"
    
    # add more ddl operations after backup
    run_sql "ALTER TABLE $DB.table1 ADD COLUMN age INT DEFAULT 25;"
    run_sql "ALTER TABLE $DB.table2 ADD INDEX idx_value(value);"

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    run_br log stop --task-name $TASK_NAME

    # Drop the original database first
    run_sql "DROP DATABASE $DB;"

    # Create a new database with the same name and different data
    # This simulates a user creating a database that would overlap with the restore
    run_sql "CREATE DATABASE $DB;"
    run_sql "CREATE TABLE $DB.new_table (id INT PRIMARY KEY, value INT);"
    run_sql "INSERT INTO $DB.new_table VALUES (1, -100);"

    echo "case 1: Full restore should fail"
    echo "Testing full restore with overlapping database"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" || restore_fail=1

    if [ $restore_fail -ne 1 ]; then
        echo "Expected restore to fail due to overlapping database but it succeeded"
        exit 1
    fi

    # Verify the existing database was not modified
    run_sql "SELECT COUNT(*) = 1 FROM $DB.new_table" || {
        echo "Existing database was modified when restore should have failed"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 1 FROM $DB.new_table WHERE id = 1 AND value = -100" || {
        echo "Data in existing database was modified when restore should have failed"
        exit 1
    }

    echo "case 2: Filtered restore should succeed since tables are not overlapped"
    echo "Testing filtered restore with non overlapping table"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.table*" --online

    run_sql "SELECT COUNT(*) = 1 FROM $DB.new_table" || {
        echo "Existing database was modified when filtered restore should have failed"
        exit 1
    }

    verify_no_unexpected_tables 4 "$DB" || {
        echo "Found unexpected number of tables after failed restore attempts"
        exit 1
    }

    echo "case 3: Filtered restore should fail when tables overlapped"
    run_sql "DROP DATABASE $DB;"
    echo "Testing filtered restore with overlapping table"
    run_sql "CREATE DATABASE $DB;"
    run_sql "CREATE TABLE $DB.table1 (id INT PRIMARY KEY, value INT);"
    run_sql "INSERT INTO $DB.table1 VALUES (1, -100);"

    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.table*" || restore_fail=1
        if [ $restore_fail -ne 1 ]; then
        echo "Expected restore to fail due to overlapping tables but it succeeded"
        exit 1
    fi

    run_sql "CREATE TABLE $DB.table3 (id INT PRIMARY KEY, value INT);"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.table3*" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Expected restore to fail due to overlapping tables during log backup but it succeeded"
        exit 1
    fi
    
    run_sql "DROP DATABASE $DB;"
    verify_no_unexpected_tables 0 "$DB" || {
        echo "Found unexpected number of tables after failed restore attempts"
        exit 1
    }

    echo "case 4: During online pitr user should not able to modify tables that are in restore (make sure table mode set to restore)"
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/before-set-table-mode-to-normal=return(true)"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.table*" || restore_fail=1
    
    if [ $restore_fail -ne 1 ]; then
        echo "Expected restore to fail due to failpoint"
    fi

    # verify tables are in restore mode by attempting to drop them
    drop_fail=0
    run_sql "DROP TABLE IF EXISTS $DB.table1" || drop_fail=$((drop_fail + 1))
    run_sql "DROP TABLE IF EXISTS $DB.table2" || drop_fail=$((drop_fail + 1))
    run_sql "DROP TABLE IF EXISTS $DB.table3" || drop_fail=$((drop_fail + 1))

    if [ $drop_fail -ne 3 ]; then
        echo "Expected all 3 tables to be protected in restore mode, but some tables could be dropped"
        exit 1
    fi
    echo "Verified all tables are protected in restore mode"

    export GO_FAILPOINTS=""

    echo "case 5: After online pitr user should be able to modify tables (make sure table mode set back to normal)"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.table*"
    run_sql "INSERT INTO $DB.table1 VALUES (4, 400, 'after restore', 30);"
    
    # verify table2 is working correctly with its modified structure (BIGINT value and index)
    run_sql "INSERT INTO $DB.table2 VALUES (4, 4000);" || {
        echo "Failed to insert into table2 after restore"
        exit 1
    }
    
    # verify we can query table2 with its index
    run_sql "SELECT * FROM $DB.table2 WHERE value = 4000" || {
        echo "Failed to query table2 using index after restore"
        exit 1
    }
    
    # verify table3 is working correctly
    run_sql "INSERT INTO $DB.table3 VALUES (3, 30000);" || {
        echo "Failed to insert into table3 after restore"
        exit 1
    }

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "online filter pitr restore test passed"
}

test_online_filter_restore

echo "br pitr online table filter all tests passed"
