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
TASK_NAME="pitr_schema_diff_reload"
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

test_basic_schema_table_diff_reload() {
    restart_services || { echo "Failed to restart services"; exit 1; }
    
    echo "start schema diff reload testing"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"
    run_sql "create schema ${DB}_to_drop;"
    
    # Add tables to the schema that will be dropped
    run_sql "create table ${DB}_to_drop.t1(id int primary key);"
    run_sql "create table ${DB}_to_drop.t2(id int primary key);"
    run_sql "insert into ${DB}_to_drop.t1 values (1), (2), (3);"
    run_sql "insert into ${DB}_to_drop.t2 values (4), (5), (6);"
    
    # tables that exist in both TiKV and InfoSchema
    create_tables_with_values "keep" 3
    
    # tables that exist in InfoSchema but not in TiKV (should be dropped)
    create_tables_with_values "to_drop" 3
    
    # Create a table that will have its schema changed
    run_sql "create table $DB.schema_change(id int primary key, name varchar(50));"
    run_sql "insert into $DB.schema_change values (1, 'old');"
    
    run_br backup full -f "$DB.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR
    
    drop_tables "to_drop" 3
    
    # create new tables (they'll exist in TiKV but not InfoSchema during restore)
    create_tables_with_values "new" 3
    
    # rename tables to test table ID mapping
    rename_tables "keep" "renamed" 3
    
    # modify table schema
    run_sql "alter table $DB.schema_change add column age int;"
    run_sql "insert into $DB.schema_change values (2, 'new', 25);"
    
    # drop a schema to test schema handling
    run_sql "drop schema ${DB}_to_drop;"
    
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    restart_services || { echo "Failed to restart services"; exit 1; }
    
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.*"
    # add a small delay to allow diff reload to get triggered
    sleep 2

    # Verify:
    # 1. tables that existed in both should be present with correct data
    verify_tables "renamed" 3 true
    
    # 2. tables that were only in InfoSchema should be dropped
    verify_tables "to_drop" 3 false
    
    # 3. new tables created after backup should be present
    verify_tables "new" 3 true
    
    # 4. dropped schema should not exist
    if run_sql "use ${DB}_to_drop" 2>/dev/null; then
        echo "Dropped schema still exists"
        exit 1
    fi
    
    # Verify the tables in dropped schema are also gone
    if run_sql "select * from ${DB}_to_drop.t1" 2>/dev/null; then
        echo "Table t1 in dropped schema still exists"
        exit 1
    fi
    
    if run_sql "select * from ${DB}_to_drop.t2" 2>/dev/null; then
        echo "Table t2 in dropped schema still exists"
        exit 1
    fi

    # 5. Verify schema changed table
    run_sql "select count(*) = 2 from $DB.schema_change" || {
        echo "Schema changed table data verification failed"
        exit 1
    }
    run_sql "select count(*) = 1 from $DB.schema_change where age = 25" || {
        echo "Schema changed table column verification failed"
        exit 1
    }

    # Cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"
    echo "test_schema_diff_reload passed"
}

test_partition_diff_reload() {
    restart_services || { echo "Failed to restart services"; exit 1; }
    
    echo "start partition diff reload testing"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"
    
    # 1. Create a partitioned table with fixed ranges (no MAXVALUE)
    run_sql "CREATE TABLE $DB.part_table (id INT) PARTITION BY RANGE(id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (200),
        PARTITION p2 VALUES LESS THAN (300));"
    run_sql "INSERT INTO $DB.part_table VALUES (50), (150), (250);"
    
    # 2. Create a table for exchange with data that matches partition p1's range (100-200)
    run_sql "CREATE TABLE $DB.exchange_table (id INT);"
    run_sql "INSERT INTO $DB.exchange_table VALUES (150);"
    
    # 3. Create another partitioned table that will be truncated entirely
    run_sql "CREATE TABLE $DB.part_table_to_truncate (id INT) PARTITION BY RANGE(id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN MAXVALUE);"
    run_sql "INSERT INTO $DB.part_table_to_truncate VALUES (50), (150);"
    
    run_br backup full -f "$DB.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR
    
    # 4. Perform various partition operations
    # Truncate a partition
    run_sql "ALTER TABLE $DB.part_table TRUNCATE PARTITION p0;"
    
    # Exchange a partition
    run_sql "ALTER TABLE $DB.part_table EXCHANGE PARTITION p1 WITH TABLE $DB.exchange_table;"
    
    # Truncate entire partitioned table
    run_sql "TRUNCATE TABLE $DB.part_table_to_truncate;"
    
    # Add a new partition
    run_sql "ALTER TABLE $DB.part_table ADD PARTITION (PARTITION p3 VALUES LESS THAN (400));"
    run_sql "INSERT INTO $DB.part_table VALUES (350);"
    
    # Drop a partition
    run_sql "ALTER TABLE $DB.part_table DROP PARTITION p2;"
    
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    restart_services || { echo "Failed to restart services"; exit 1; }
    
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.*"
    
    # add a small delay to allow diff reload to get triggered
    sleep 2

    # Verify partition operations
    # Check truncated partition
    run_sql "SELECT count(*) = 0 FROM $DB.part_table PARTITION (p0)" || {
        echo "Partition p0 should be empty after truncate"
        exit 1
    }
    
    # Check exchanged partition
    run_sql "SELECT count(*) = 1, sum(id) = 150 FROM $DB.part_table PARTITION (p1)" || {
        echo "Partition p1 should have exchanged data"
        exit 1
    }
    
    # Check new partition
    run_sql "SELECT count(*) = 1, sum(id) = 350 FROM $DB.part_table PARTITION (p3)" || {
        echo "New partition p3 should have correct data"
        exit 1
    }
    
    # Verify dropped partition doesn't exist
    if run_sql "SELECT * FROM $DB.part_table PARTITION (p2)" 2>/dev/null; then
        echo "Partition p2 should not exist"
        exit 1
    fi
    
    run_sql "SHOW TABLES FROM $DB"

    # Verify truncated partitioned table
    run_sql "SELECT count(*) = 0 FROM $DB.part_table_to_truncate" || {
        echo "Truncated partitioned table should be empty"
        exit 1
    }

    # Cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"
    echo "test_partition_diff_reload passed"
}

test_large_batch_diff_reload() {
    restart_services || { echo "Failed to restart services"; exit 1; }
    
    echo "start large batch diff reload testing"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"
    
    # Create 1500 tables to test batch processing
    create_tables_with_values "batch" 1500
    
    run_br backup full -f "$DB.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR
    
    # Drop every third table to create gaps
    for i in $(seq 3 3 1500); do
        run_sql "drop table $DB.batch_${i};"
    done
    
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    restart_services || { echo "Failed to restart services"; exit 1; }
    
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.*"
    
    # add a small delay to allow diff reload to get triggered
    sleep 2

    # Verify:
    # 1. Tables that weren't dropped should exist with correct data
    for i in $(seq 1 1500); do
        # Skip tables that were dropped (every third)
        if [ $((i % 3)) -eq 0 ]; then
            continue
        fi
        run_sql "select count(*) = 1 from $DB.batch_${i} where c = $i" || {
            echo "Table $DB.batch_${i} doesn't have expected value $i"
            exit 1
        }
    done

    # Cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"
    echo "test_large_batch_diff_reload passed"
}

# run all tests
test_basic_schema_table_diff_reload
test_partition_diff_reload
test_large_batch_diff_reload

echo "br pitr schema diff reload passed"
