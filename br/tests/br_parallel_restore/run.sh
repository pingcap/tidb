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

set -eu
DB="$TEST_NAME"
TASK_NAME="$TEST_NAME"

create_tables_with_values() {
    local prefix=$1    # table name prefix
    local count=$2     # number of tables to create

    for i in $(seq 1 $count); do
        run_sql "create table $DB.${prefix}_${i}(c int); insert into $DB.${prefix}_${i} values ($i);"
    done
}
run_br log start --task-name $TASK_NAME -s "local://$TEST_DIR/log_backup"

run_br backup full -s "local://$TEST_DIR/backup"

for i in $(seq 1 5); do
    for j in $(seq 11 20); do
        run_sql "INSERT INTO ${DB}_${i}.table1 VALUES ($j, 'updated_name_$j');"
        run_sql "INSERT INTO ${DB}_${i}.table2 VALUES ($j, $j);"
        run_sql "INSERT INTO ${DB}_${i}.table3 VALUES ($j, 'updated_data_$j');"
    done
    
    # Store post-update checksums
    for table in "table1" "table2" "table3"; do
        checksum=$(run_sql "ADMIN CHECKSUM TABLE ${DB}_${i}.${table};" | awk 'NR==2 {print $3}')
        eval "updated_checksum_${DB}_${i}_${table}=$checksum"
    done
    
    # Make some schema changes to test DDL handling
    if [ $i -eq 1 ]; then
        run_sql "ALTER TABLE ${DB}_${i}.table1 ADD COLUMN email VARCHAR(100);"
        run_sql "UPDATE ${DB}_${i}.table1 SET email = CONCAT('user', id, '@example.com');"
    fi
    
    if [ $i -eq 2 ]; then
        run_sql "CREATE TABLE ${DB}_${i}.table4 (id INT PRIMARY KEY, extra TEXT);"
        run_sql "INSERT INTO ${DB}_${i}.table4 VALUES (1, 'new_table_data');"
    fi
    
    if [ $i -eq 3 ]; then
        run_sql "DROP TABLE ${DB}_${i}.table3;"
    fi
done

# Wait for log backup to advance past our changes
. "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

# Get the current timestamp to use for PITR
current_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")

# Drop all databases to prepare for restore tests
for i in $(seq 1 5); do
    run_sql "DROP DATABASE ${DB}_${i};"
done

# Attempt concurrent PITR restores with different filters
echo "Attempting concurrent filtered PITR restores..."

# Store the timestamp before our updates
snapshot_ts=$(run_br validate decode --field="end-version" -s "local://$TEST_DIR/backup" | grep -oE "^[0-9]+")

# Start multiple PITR restores in background
RESTORE_PIDS=""

# Restore DB_1 with table filter for table1 and table2 (excluding table3)
run_br restore point --start-ts $snapshot_ts --end-ts $current_ts -f "${DB}_1.table[12]" -s "local://$TEST_DIR/log_backup" --full-backup-storage "local://$TEST_DIR/backup" --pd $PD_ADDR &
RESTORE_PIDS="$RESTORE_PIDS $!"

# Restore DB_2 with all tables 
run_br restore point --start-ts $snapshot_ts --end-ts $current_ts -f "${DB}_2.*" -s "local://$TEST_DIR/log_backup" --full-backup-storage "local://$TEST_DIR/backup" --pd $PD_ADDR &
RESTORE_PIDS="$RESTORE_PIDS $!"

# Restore DB_3 and DB_4 with negative filter (exclude table1)
run_br restore point --start-ts $snapshot_ts --end-ts $current_ts -f "${DB}_[34].*" -f "!${DB}_[34].table1" -s "local://$TEST_DIR/log_backup" --full-backup-storage "local://$TEST_DIR/backup" --pd $PD_ADDR &
RESTORE_PIDS="$RESTORE_PIDS $!"

# Restore DB_5 to original backup state (without applying log backup)
run_br restore full -f "${DB}_5.*" -s "local://$TEST_DIR/backup" --pd $PD_ADDR &
RESTORE_PIDS="$RESTORE_PIDS $!"

# Wait for all restores and check results
FAILED_COUNT=0
for pid in $RESTORE_PIDS; do
    if ! wait $pid; then
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
done

# Validate results - all restores should succeed
if [ $FAILED_COUNT -ne 0 ]; then
    echo "TEST FAILED: $FAILED_COUNT restore(s) failed, expected all to succeed"
    exit 1
fi

# Verify data integrity according to specific test cases
echo "Verifying data integrity..."

# Case 1: DB_1 should have table1 and table2 with updates, but no table3
if ! run_sql "USE ${DB}_1;" 2>/dev/null; then
    echo "TEST FAILED: Database ${DB}_1 was not restored"
    exit 1
fi

# Check that table1 and table2 exist with updates
for table in "table1" "table2"; do
    run_sql "SELECT COUNT(*) FROM ${DB}_1.${table};" | grep -q "20" || {
        echo "TEST FAILED: ${DB}_1.${table} does not have expected row count"
        exit 1
    }
    
    if [ "$table" = "table1" ]; then
        # Check the email column was added in table1
        run_sql "SHOW COLUMNS FROM ${DB}_1.${table};" | grep -q "email" || {
            echo "TEST FAILED: email column not found in ${DB}_1.${table}"
            exit 1
        }
    fi
done

# Check that table3 does not exist
run_sql "SHOW TABLES IN ${DB}_1 LIKE 'table3';" | grep -q "table3" && {
    echo "TEST FAILED: ${DB}_1.table3 exists but should not"
    exit 1
}

# Case 2: DB_2 should have all tables with updates including the new table4
if ! run_sql "USE ${DB}_2;" 2>/dev/null; then
    echo "TEST FAILED: Database ${DB}_2 was not restored"
    exit 1
fi

# Check that all tables exist with updates
for table in "table1" "table2" "table3"; do
    run_sql "SELECT COUNT(*) FROM ${DB}_2.${table};" | grep -q "20" || {
        echo "TEST FAILED: ${DB}_2.${table} does not have expected row count"
        exit 1
    }
done

# Check that table4 exists
run_sql "SHOW TABLES IN ${DB}_2 LIKE 'table4';" | grep -q "table4" || {
    echo "TEST FAILED: ${DB}_2.table4 does not exist but should"
    exit 1
}

# Case 3: DB_3 should have table2 and no table1 or table3 (which was dropped)
if ! run_sql "USE ${DB}_3;" 2>/dev/null; then
    echo "TEST FAILED: Database ${DB}_3 was not restored"
    exit 1
fi

# Check that table1 does not exist
run_sql "SHOW TABLES IN ${DB}_3 LIKE 'table1';" | grep -q "table1" && {
    echo "TEST FAILED: ${DB}_3.table1 exists but should not"
    exit 1
}

# Check that table2 exists with updates
run_sql "SELECT COUNT(*) FROM ${DB}_3.table2;" | grep -q "20" || {
    echo "TEST FAILED: ${DB}_3.table2 does not have expected row count"
    exit 1
}

# Check that table3 does not exist (was dropped during log backup)
run_sql "SHOW TABLES IN ${DB}_3 LIKE 'table3';" | grep -q "table3" && {
    echo "TEST FAILED: ${DB}_3.table3 exists but should not"
    exit 1
}

# Case 4: DB_4 should have table2 and table3 but no table1
if ! run_sql "USE ${DB}_4;" 2>/dev/null; then
    echo "TEST FAILED: Database ${DB}_4 was not restored"
    exit 1
fi

# Check that table1 does not exist
run_sql "SHOW TABLES IN ${DB}_4 LIKE 'table1';" | grep -q "table1" && {
    echo "TEST FAILED: ${DB}_4.table1 exists but should not"
    exit 1
}

# Check that table2 and table3 exist with updates
for table in "table2" "table3"; do
    run_sql "SELECT COUNT(*) FROM ${DB}_4.${table};" | grep -q "20" || {
        echo "TEST FAILED: ${DB}_4.${table} does not have expected row count"
        exit 1
    }
done

# Case 5: DB_5 should have all tables but with no updates (original backup only)
if ! run_sql "USE ${DB}_5;" 2>/dev/null; then
    echo "TEST FAILED: Database ${DB}_5 was not restored"
    exit 1
fi

# Check that all tables exist but only with initial data
for table in "table1" "table2" "table3"; do
    run_sql "SELECT COUNT(*) FROM ${DB}_5.${table};" | grep -q "10" || {
        echo "TEST FAILED: ${DB}_5.${table} does not have expected row count"
        exit 1
    }
    
    # Verify checksums match original data
    new_checksum=$(run_sql "ADMIN CHECKSUM TABLE ${DB}_5.${table};" | awk 'NR==2 {print $3}')
    eval "original_checksum=\$initial_checksum_${DB}_5_${table}"
    
    if [ "$new_checksum" != "$original_checksum" ]; then
        echo "TEST FAILED: Checksum mismatch for ${DB}_5.${table}"
        echo "Original: $original_checksum"
        echo "After restore: $new_checksum"
        exit 1
    fi
done

# Clean up
echo "Stopping log backup task..."
run_br log stop --task-name $TASK_NAME

# Drop all test databases
for i in $(seq 1 5); do
    run_sql "DROP DATABASE IF EXISTS ${DB}_${i};"
done

echo "Concurrent filtered PITR test completed successfully"
