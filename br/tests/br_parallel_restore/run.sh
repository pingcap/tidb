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

set -eu
DB="$TEST_NAME"

# Create more test databases and store checksums
echo "Creating test databases and tables..."
for i in $(seq 1 5); do
    run_sql "CREATE DATABASE ${DB}_${i};"
    
    # Create tables in each database
    run_sql "CREATE TABLE ${DB}_${i}.table1 (id INT PRIMARY KEY, name VARCHAR(50));"
    run_sql "CREATE TABLE ${DB}_${i}.table2 (id INT PRIMARY KEY, value INT);"
    run_sql "CREATE TABLE ${DB}_${i}.table3 (id INT PRIMARY KEY, data TEXT);"
    
    # Insert test data
    echo "Inserting test data into database ${i}..."
    for j in $(seq 1 10); do
        run_sql "INSERT INTO ${DB}_${i}.table1 VALUES ($j, 'name_$j');"
        run_sql "INSERT INTO ${DB}_${i}.table2 VALUES ($j, $j);"
        run_sql "INSERT INTO ${DB}_${i}.table3 VALUES ($j, 'data_$j');"
    done

    # Store checksums for each table
    for table in "table1" "table2" "table3"; do
        checksum=$(run_sql "ADMIN CHECKSUM TABLE ${DB}_${i}.${table};" | awk 'NR==2 {print $3}')
        eval "checksum_${DB}_${i}_${table}=$checksum"
    done
done

# Backup all databases
echo "Backing up databases..."
run_br backup full -s "local://$TEST_DIR/backup"

# Drop all databases
for i in $(seq 1 5); do
    run_sql "DROP DATABASE ${DB}_${i};"
done

# Attempt concurrent restores with different filters
echo "Attempting concurrent restores..."

# Start multiple restores in background
RESTORE_PIDS=""
for i in $(seq 1 5); do
    run_br restore full -f "${DB}_${i}.*" -s "local://$TEST_DIR/backup" --pd $PD_ADDR &
    RESTORE_PIDS="$RESTORE_PIDS $!"
done

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

# Verify data integrity by comparing checksums
echo "Verifying data integrity..."
for i in $(seq 1 5); do
    if ! run_sql "USE ${DB}_${i};" 2>/dev/null; then
        echo "TEST FAILED: Database ${DB}_${i} was not restored"
        exit 1
    fi
    
    # Compare checksums for all tables
    for table in "table1" "table2" "table3"; do
        new_checksum=$(run_sql "ADMIN CHECKSUM TABLE ${DB}_${i}.${table};" | awk 'NR==2 {print $3}')
        eval "original_checksum=\$checksum_${DB}_${i}_${table}"
        
        if [ "$new_checksum" != "$original_checksum" ]; then
            echo "TEST FAILED: Checksum mismatch for ${DB}_${i}.${table}"
            echo "Original: $original_checksum"
            echo "After restore: $new_checksum"
            exit 1
        fi
    done
done

# Clean up
for i in $(seq 1 5); do
    run_sql "DROP DATABASE IF EXISTS ${DB}_${i};"
done

echo "Concurrent restore test completed successfully"
