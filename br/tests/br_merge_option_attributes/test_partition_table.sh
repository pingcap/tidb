#!/bin/sh
#
# Copyright 2026 PingCAP, Inc.
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
TABLE="t_partition"

echo "=== Test: Partition table with merge_option ==="

# Create database and partitioned table
run_sql "CREATE DATABASE IF NOT EXISTS $DB;"
run_sql "USE $DB;"
run_sql "DROP TABLE IF EXISTS $DB.$TABLE;"
run_sql "CREATE TABLE $DB.$TABLE (
    a int,
    b varchar(100)
) PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20),
    PARTITION p2 VALUES LESS THAN (30)
);"
run_sql "INSERT INTO $DB.$TABLE VALUES (5, 'test1'), (15, 'test2'), (25, 'test3');"

# Set merge_option for the table
echo "Setting merge_option=allow for table $DB.$TABLE"
run_sql "ALTER TABLE $DB.$TABLE ATTRIBUTES='merge_option=allow';"

# Set merge_option for partitions
echo "Setting merge_option=allow for partitions"
run_sql "ALTER TABLE $DB.$TABLE PARTITION p0 ATTRIBUTES='merge_option=allow';"
run_sql "ALTER TABLE $DB.$TABLE PARTITION p1 ATTRIBUTES='merge_option=allow';"
run_sql "ALTER TABLE $DB.$TABLE PARTITION p2 ATTRIBUTES='merge_option=allow';"

# Verify merge_option is visible in information_schema.attributes before backup
echo "Verifying merge_option in information_schema.attributes before backup..."
# Convert to lowercase for rule ID matching (rule IDs use lowercase)
db_lower=$(echo "$DB" | tr '[:upper:]' '[:lower:]')
table_lower=$(echo "$TABLE" | tr '[:upper:]' '[:lower:]')
# Check table-level attribute
table_attr_count=$(run_sql "SELECT COUNT(*) FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');" | awk '/COUNT/{print $2}')
if [ "$table_attr_count" != "1" ]; then
    echo "ERROR: Expected 1 table-level attribute entry before backup, but got $table_attr_count"
    exit 1
fi

# Check partition-level attributes (should have 3 partitions)
partition_attr_count=$(run_sql "SELECT COUNT(*) FROM information_schema.attributes WHERE LOWER(ID) LIKE LOWER('schema/$db_lower/$table_lower/%');" | awk '/COUNT/{print $2}')
if [ "$partition_attr_count" != "3" ]; then
    echo "ERROR: Expected 3 partition-level attribute entries before backup, but got $partition_attr_count"
    run_sql "SELECT * FROM information_schema.attributes WHERE LOWER(ID) LIKE LOWER('schema/$db_lower/$table_lower%');"
    exit 1
fi

# Verify the attribute values contain merge_option=allow
table_attr_value=$(run_sql "SELECT ATTRIBUTES FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');" | awk '/merge_option=allow/')
if [ -z "$table_attr_value" ]; then
    echo "ERROR: merge_option=allow not found in table attributes before backup"
    run_sql "SELECT * FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');"
    exit 1
fi

# Verify partition attributes
for partition in p0 p1 p2; do
    partition_lower=$(echo "$partition" | tr '[:upper:]' '[:lower:]')
    partition_attr_value=$(run_sql "SELECT ATTRIBUTES FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower/$partition_lower');" | awk '/merge_option=allow/')
    if [ -z "$partition_attr_value" ]; then
        echo "ERROR: merge_option=allow not found in partition $partition attributes before backup"
        run_sql "SELECT * FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower/$partition_lower');"
        exit 1
    fi
done

echo "✓ merge_option is visible in information_schema.attributes before backup (1 table + 3 partitions)"

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
# Check table-level attribute
table_attr_count_after=$(run_sql "SELECT COUNT(*) FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');" | awk '/COUNT/{print $2}')
if [ "$table_attr_count_after" != "1" ]; then
    echo "ERROR: Expected 1 table-level attribute entry after restore, but got $table_attr_count_after"
    echo "Current attributes:"
    run_sql "SELECT * FROM information_schema.attributes WHERE LOWER(ID) LIKE LOWER('schema/$db_lower%');"
    exit 1
fi

# Check partition-level attributes (should have 3 partitions)
partition_attr_count_after=$(run_sql "SELECT COUNT(*) FROM information_schema.attributes WHERE LOWER(ID) LIKE LOWER('schema/$db_lower/$table_lower/%');" | awk '/COUNT/{print $2}')
if [ "$partition_attr_count_after" != "3" ]; then
    echo "ERROR: Expected 3 partition-level attribute entries after restore, but got $partition_attr_count_after"
    echo "Current attributes:"
    run_sql "SELECT * FROM information_schema.attributes WHERE LOWER(ID) LIKE LOWER('schema/$db_lower/$table_lower%');"
    exit 1
fi

# Verify the attribute values contain merge_option=allow
table_attr_value_after=$(run_sql "SELECT ATTRIBUTES FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');" | awk '/merge_option=allow/')
if [ -z "$table_attr_value_after" ]; then
    echo "ERROR: merge_option=allow not found in table attributes after restore"
    echo "Current attributes:"
    run_sql "SELECT * FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower');"
    exit 1
fi

# Verify partition attributes
for partition in p0 p1 p2; do
    partition_lower=$(echo "$partition" | tr '[:upper:]' '[:lower:]')
    partition_attr_value_after=$(run_sql "SELECT ATTRIBUTES FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower/$partition_lower');" | awk '/merge_option=allow/')
    if [ -z "$partition_attr_value_after" ]; then
        echo "ERROR: merge_option=allow not found in partition $partition attributes after restore"
        echo "Current attributes:"
        run_sql "SELECT * FROM information_schema.attributes WHERE LOWER(ID) = LOWER('schema/$db_lower/$table_lower/$partition_lower');"
        exit 1
    fi
done

echo "✓ merge_option is visible in information_schema.attributes after restore (1 table + 3 partitions)"

# Verify data integrity
row_count=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')
if [ "$row_count" != "3" ]; then
    echo "ERROR: Expected 3 rows, but got $row_count"
    exit 1
fi

echo "✓ Data integrity verified (3 rows)"

# Show final attributes
echo "Final attributes:"
run_sql "SELECT ID, TYPE, ATTRIBUTES FROM information_schema.attributes WHERE LOWER(ID) LIKE LOWER('schema/$db_lower/$table_lower%') ORDER BY ID;"

echo "=== Test passed: Partition table with merge_option ==="

run_sql "DROP DATABASE $DB;"

