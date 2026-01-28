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
TABLE="t_normal"

echo "=== Test: Normal table with merge_option ==="

# Create database and table
run_sql "CREATE DATABASE IF NOT EXISTS $DB;"
run_sql "USE $DB;"
run_sql "DROP TABLE IF EXISTS $DB.$TABLE;"
run_sql "CREATE TABLE $DB.$TABLE (a int PRIMARY KEY, b varchar(100));"
run_sql "INSERT INTO $DB.$TABLE VALUES (1, 'test1'), (2, 'test2'), (3, 'test3');"

# Set merge_option for the table
echo "Setting merge_option=allow for table $DB.$TABLE"
run_sql "ALTER TABLE $DB.$TABLE ATTRIBUTES='merge_option=allow';"

# Verify merge_option is visible in information_schema.attributes before backup
echo "Verifying merge_option in information_schema.attributes before backup..."
# Convert to lowercase for rule ID matching (rule IDs use lowercase)
db_lower=$(echo "$DB" | tr '[:upper:]' '[:lower:]')
table_lower=$(echo "$TABLE" | tr '[:upper:]' '[:lower:]')
attr_count_before=$(run_sql "SELECT COUNT(*) FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');" | awk '/COUNT/{print $2}')
if [ "$attr_count_before" != "1" ]; then
    echo "ERROR: Expected 1 attribute entry before backup, but got $attr_count_before"
    exit 1
fi

# Verify the attribute value contains merge_option=allow
attr_value=$(run_sql "SELECT ATTRIBUTES FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');" | awk '/merge_option=allow/')
if [ -z "$attr_value" ]; then
    echo "ERROR: merge_option=allow not found in attributes before backup"
    run_sql "SELECT * FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');"
    exit 1
fi

echo "✓ merge_option is visible in information_schema.attributes before backup"

# Backup full
echo "Starting BR full backup..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB"

# Drop database
echo "Dropping database $DB..."
run_sql "DROP DATABASE $DB;"

# Verify attributes are gone after drop
attr_count_after_drop=$(run_sql "SELECT COUNT(*) FROM information_schema.attributes WHERE LOWER(ID) LIKE LOWER('schema/$db_lower%');" | awk '/COUNT/{print $2}')
if [ "$attr_count_after_drop" != "0" ]; then
    echo "WARNING: Attributes still exist after drop (count: $attr_count_after_drop), this is expected if GC hasn't finished"
fi

# Restore full
echo "Starting BR full restore..."
run_br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

# Verify merge_option is still visible in information_schema.attributes after restore
echo "Verifying merge_option in information_schema.attributes after restore..."
# Wait a bit for information_schema.attributes to sync from PD label rules
sleep 2
attr_count_after=0
max_retries=10
retry_count=0
while [ $retry_count -lt $max_retries ]; do
    attr_count_after=$(run_sql "SELECT COUNT(*) FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');" | awk '/COUNT/{print $2}')
    if [ "$attr_count_after" = "1" ]; then
        break
    fi
    retry_count=$((retry_count + 1))
    echo "Waiting for attributes to sync... (attempt $retry_count/$max_retries)"
    sleep 1
done

if [ "$attr_count_after" != "1" ]; then
    echo "ERROR: Expected 1 attribute entry after restore, but got $attr_count_after"
    echo "Current attributes:"
    run_sql "SELECT * FROM information_schema.attributes WHERE LOWER(ID) LIKE LOWER('schema/$db_lower%');"
    echo "Checking PD label rules directly:"
    run_curl "https://$PD_ADDR/pd/api/v1/config/placement-rule/schema/$db_lower/$table_lower" || echo "Failed to query PD"
    exit 1
fi

# Verify the attribute value contains merge_option=allow
attr_value_after=$(run_sql "SELECT ATTRIBUTES FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');" | awk '/merge_option=allow/')
if [ -z "$attr_value_after" ]; then
    echo "ERROR: merge_option=allow not found in attributes after restore"
    echo "Current attributes:"
    run_sql "SELECT * FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');"
    exit 1
fi

echo "✓ merge_option is visible in information_schema.attributes after restore"

# Verify data integrity
row_count=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')
if [ "$row_count" != "3" ]; then
    echo "ERROR: Expected 3 rows, but got $row_count"
    exit 1
fi

echo "✓ Data integrity verified (3 rows)"

# Show final attributes
echo "Final attributes:"
run_sql "SELECT ID, TYPE, ATTRIBUTES FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');"

echo "=== Test passed: Normal table with merge_option ==="

run_sql "DROP DATABASE $DB;"

