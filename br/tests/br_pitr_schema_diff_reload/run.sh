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

test_basic() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start basic filter testing"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    echo "write initial data and do snapshot backup"
    create_tables_with_values "full_backup" 3
    create_tables_with_values "table_to_drop" 3

    run_br backup full -f "$DB.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "write more data and wait for log backup to catch up"
    create_tables_with_values "log_backup_lower" 3
    create_tables_with_values "LOG_BACKUP_UPPER" 3
    create_tables_with_values "other" 3
    drop_tables "table_to_drop" 3

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # restart services to clean up the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }

    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.*"

    verify_tables "log_backup_lower" 3 true
    verify_tables "LOG_BACKUP_UPPER" 3 true
    verify_tables "full_backup" 3 true
    verify_tables "other" 3 true
    verify_tables "table_to_drop" 3 false

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"
    echo "test_basic passed"
}

test_schema_diff_reload() {
    restart_services || { echo "Failed to restart services"; exit 1; }
    
    echo "start schema diff reload testing"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    # Create initial schemas and tables
    run_sql "create schema $DB;"
    run_sql "create schema ${DB}_to_drop;"
    
    # Case 1: Tables that exist in both TiKV and InfoSchema
    create_tables_with_values "keep" 3
    
    # Case 2: Tables that exist in InfoSchema but not in TiKV (should be dropped)
    create_tables_with_values "to_drop" 3
    
    # Take full backup
    run_br backup full -f "$DB.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR
    
    # Make schema changes after backup:
    # 1. Drop some tables (they'll exist in InfoSchema but not TiKV during restore)
    drop_tables "to_drop" 3
    
    # 2. Create new tables (they'll exist in TiKV but not InfoSchema during restore)
    create_tables_with_values "new" 3
    
    # 3. Rename some tables to test table ID mapping
    rename_tables "keep" "renamed" 3
    
    # 4. Drop a schema to test schema handling
    run_sql "drop schema ${DB}_to_drop;"
    
    # Wait for log backup to catch up
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    # Restart services to clean up cluster
    restart_services || { echo "Failed to restart services"; exit 1; }
    
    # Restore and verify:
    echo "Testing schema diff reload restore"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.*"
    
    # Verify:
    # 1. Tables that existed in both should be present with correct data
    verify_tables "renamed" 3 true
    
    # 2. Tables that were only in InfoSchema should be dropped
    verify_tables "to_drop" 3 false
    
    # 3. New tables created after backup should be present
    verify_tables "new" 3 true
    
    # 4. Dropped schema should not exist
    if run_sql "use ${DB}_to_drop" 2>/dev/null; then
        echo "Dropped schema still exists"
        exit 1
    fi

    # Cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"
    echo "test_schema_diff_reload passed"
}

test_table_diff_reload() {
    restart_services || { echo "Failed to restart services"; exit 1; }
    
    echo "start table diff reload testing"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    # Create initial schemas
    run_sql "create schema $DB;"
    
    # Case 1: Regular tables with different data types
    run_sql "CREATE TABLE $DB.t1 (
        id INT PRIMARY KEY,
        name VARCHAR(50),
        created_at TIMESTAMP
    );"
    run_sql "INSERT INTO $DB.t1 VALUES (1, 'original', NOW());"
    
    # Case 2: Partitioned table
    run_sql "CREATE TABLE $DB.t2 (
        id INT,
        created_date DATE
    ) PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (200)
    );"
    run_sql "INSERT INTO $DB.t2 VALUES (50, NOW()), (150, NOW());"
    
    # Case 3: Table with indexes
    run_sql "CREATE TABLE $DB.t3 (
        id INT PRIMARY KEY,
        email VARCHAR(50),
        INDEX idx_email(email)
    );"
    run_sql "INSERT INTO $DB.t3 VALUES (1, 'test@example.com');"
    
    # Case 4: Table with foreign keys
    run_sql "CREATE TABLE $DB.parent (id INT PRIMARY KEY);"
    run_sql "CREATE TABLE $DB.child (
        id INT PRIMARY KEY,
        parent_id INT,
        FOREIGN KEY (parent_id) REFERENCES parent(id)
    );"
    run_sql "INSERT INTO $DB.parent VALUES (1);"
    run_sql "INSERT INTO $DB.child VALUES (1, 1);"
    
    # Take full backup
    run_br backup full -f "$DB.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR
    
    # Make table changes after backup:
    
    # 1. Modify regular table
    run_sql "ALTER TABLE $DB.t1 ADD COLUMN status VARCHAR(20);"
    run_sql "UPDATE $DB.t1 SET status = 'active';"
    
    # 2. Add partition to partitioned table
    run_sql "ALTER TABLE $DB.t2 ADD PARTITION (PARTITION p2 VALUES LESS THAN (300));"
    run_sql "INSERT INTO $DB.t2 VALUES (250, NOW());"
    
    # 3. Add/Drop indexes
    run_sql "ALTER TABLE $DB.t3 DROP INDEX idx_email;"
    run_sql "ALTER TABLE $DB.t3 ADD INDEX idx_id_email(id, email);"
    
    # 4. Table with special attributes
    run_sql "CREATE TABLE $DB.t4 (id INT PRIMARY KEY) TTL = \"id + INTERVAL 1 DAY\";"
    
    # 5. Drop and recreate table with same name but different schema
    run_sql "DROP TABLE $DB.child;"
    run_sql "CREATE TABLE $DB.child (
        id INT PRIMARY KEY,
        parent_id INT,
        name VARCHAR(50)
    );"
    
    # Wait for log backup to catch up
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    # Restart services to clean up cluster
    restart_services || { echo "Failed to restart services"; exit 1; }
    
    # Restore and verify:
    echo "Testing table diff reload restore"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.*"
    
    # Verify:
    # 1. Regular table structure and data
    run_sql "SELECT count(*) = 1, count(status) = 1 FROM $DB.t1 WHERE id = 1" || {
        echo "Table t1 data verification failed"
        exit 1
    }
    
    # 2. Partitioned table structure and data
    run_sql "SELECT count(*) = 3 FROM $DB.t2" || {
        echo "Table t2 data verification failed"
        exit 1
    }
    run_sql "SELECT count(*) = 1 FROM information_schema.partitions WHERE table_name = 't2' AND partition_name = 'p2'" || {
        echo "Table t2 partition verification failed"
        exit 1
    }
    
    # 3. Verify indexes
    run_sql "SHOW INDEX FROM $DB.t3 WHERE Key_name = 'idx_id_email'" || {
        echo "Table t3 index verification failed"
        exit 1
    }
    
    # 4. Verify special attributes
    run_sql "SELECT TTL_ENABLE FROM information_schema.tables WHERE table_name = 't4'" || {
        echo "Table t4 TTL attribute verification failed"
        exit 1
    }
    
    # 5. Verify recreated table structure
    run_sql "SHOW CREATE TABLE $DB.child" | grep "name VARCHAR" || {
        echo "Table child structure verification failed"
        exit 1
    }

    # Cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"
    echo "test_table_diff_reload passed"
}

# Run all tests
test_basic
test_schema_diff_reload
test_table_diff_reload

echo "br pitr schema diff reload passed"
