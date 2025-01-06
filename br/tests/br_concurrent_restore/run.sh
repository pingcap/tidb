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

# Create test databases and tables
echo "Creating test databases and tables..."
run_sql "CREATE DATABASE ${DB}_1;"
run_sql "CREATE DATABASE ${DB}_2;"

# Create tables in first database
run_sql "CREATE TABLE ${DB}_1.table1 (id INT PRIMARY KEY, name VARCHAR(50));"
run_sql "CREATE TABLE ${DB}_1.table2 (id INT PRIMARY KEY, value INT);"
run_sql "CREATE TABLE ${DB}_1.table3 (id INT PRIMARY KEY, data TEXT);"

# Create tables in second database
run_sql "CREATE TABLE ${DB}_2.table1 (id INT PRIMARY KEY, name VARCHAR(50));"
run_sql "CREATE TABLE ${DB}_2.table2 (id INT PRIMARY KEY, value INT);"
run_sql "CREATE TABLE ${DB}_2.table3 (id INT PRIMARY KEY, data TEXT);"

# Insert test data
echo "Inserting test data..."
for i in $(seq 1 10); do
    run_sql "INSERT INTO ${DB}_1.table1 VALUES ($i, 'name_$i');"
    run_sql "INSERT INTO ${DB}_1.table2 VALUES ($i, $i);"
    run_sql "INSERT INTO ${DB}_1.table3 VALUES ($i, 'data_$i');"
    
    run_sql "INSERT INTO ${DB}_2.table1 VALUES ($i, 'name_$i');"
    run_sql "INSERT INTO ${DB}_2.table2 VALUES ($i, $i);"
    run_sql "INSERT INTO ${DB}_2.table3 VALUES ($i, 'data_$i');"
done

# Backup both databases
echo "Backing up databases..."
run_br backup full -s "local://$TEST_DIR/backup"

# Drop all databases
run_sql "DROP DATABASE ${DB}_1;"
run_sql "DROP DATABASE ${DB}_2;"

# Attempt concurrent restores with different filters
echo "Attempting concurrent restores..."

# Start first restore in background (should fail or block)
run_br restore full -f "${DB}_1.*" -s "local://$TEST_DIR/backup" --pd $PD_ADDR &
RESTORE1_PID=$!

# Start second restore immediately after (should fail or block)
run_br restore full -f "${DB}_2.*" -s "local://$TEST_DIR/backup" --pd $PD_ADDR &
RESTORE2_PID=$!

# Wait for both restores
wait $RESTORE1_PID || RESTORE1_FAILED=1
wait $RESTORE2_PID || RESTORE2_FAILED=1

# Check results
if [ -z "${RESTORE1_FAILED:-}" ] && [ -z "${RESTORE2_FAILED:-}" ]; then
    echo "TEST FAILED: Both restores succeeded when they should have conflicted"
    exit 1
fi

# Clean up
run_sql "DROP DATABASE IF EXISTS ${DB}_1;"
run_sql "DROP DATABASE IF EXISTS ${DB}_2;"

echo "Test completed successfully - demonstrated that concurrent restores are not supported" 