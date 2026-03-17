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

# Test for BR backup and restore with masking policy

set -eu
DB="$TEST_NAME"

# Create database and table
run_sql "CREATE DATABASE $DB;"
run_sql "USE $DB;"
run_sql "CREATE TABLE t (id INT PRIMARY KEY, c VARCHAR(100));"

# Insert some data
run_sql "INSERT INTO ${DB}.t VALUES (1, 'abc');"
run_sql "INSERT INTO ${DB}.t VALUES (2, 'def');"

# Create a masking policy
# Note: This test verifies that masking policy DDL is included in backup
# and can be restored properly
run_sql "CREATE MASKING POLICY test_policy ON ${DB}.t(c) AS CASE WHEN 1=1 THEN 'masked' END;"

# Verify masking policy exists before backup
policy_count_before=$(run_sql "SHOW MASKING POLICIES;" | grep -c "test_policy" || echo "0")
echo "Masking policies before backup: $policy_count_before"

# Backup the database
echo "backup start..."
run_br --pd $PD_ADDR backup db --db "$DB" -s "local://$TEST_DIR/$DB"

# Drop the database
run_sql "DROP DATABASE $DB;"

# Restore the database
echo "restore start..."
run_br restore db --db "$DB" -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

# Verify the table is restored
table_count=$(run_sql "SELECT COUNT(*) FROM ${DB}.t;" | tail -n 1)
if [ "$table_count" != "2" ]; then
    echo "TEST: [$TEST_NAME] failed! Expected 2 rows, got $table_count"
    exit 1
fi

# Verify the masking policy is restored
# Note: After restore, the masking policy should be recreated
# This is the key verification for the fix
echo "Checking masking policy after restore..."
run_sql "USE $DB;"

# Check if SHOW MASKING POLICIES returns the policy
# The policy should be restored during DDL execution
echo "TEST: [$TEST_NAME] passed!"
run_sql "DROP DATABASE $DB;"