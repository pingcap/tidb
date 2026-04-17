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
run_sql "CREATE TABLE ${DB}.t (id INT PRIMARY KEY, c VARCHAR(100));"

# Insert some data
run_sql "INSERT INTO ${DB}.t VALUES (1, 'abc');"
run_sql "INSERT INTO ${DB}.t VALUES (2, 'def');"

# Create a masking policy
run_sql "CREATE MASKING POLICY test_policy ON ${DB}.t(c) AS CASE WHEN 1=1 THEN 'masked' END;"

# Verify masking policy exists before backup
policy_output_before=$(run_sql "SHOW MASKING POLICIES FOR ${DB}.t;")
policy_count_before=$(printf '%s\n' "$policy_output_before" | grep -c "test_policy" || true)
if [ "$policy_count_before" != "1" ]; then
    echo "TEST: [$TEST_NAME] failed! Expected 1 masking policy before backup, got $policy_count_before"
    exit 1
fi
echo "Masking policies before backup: $policy_count_before"

check_restored_db() {
    stage="$1"

    # Verify the table is restored.
    # run_sql uses mysql -E output, so parse "COUNT(*): <num>" explicitly.
    table_count_output=$(run_sql "SELECT COUNT(*) FROM ${DB}.t;")
    table_count=$(printf '%s\n' "$table_count_output" | awk -F': ' '/COUNT\(\*\)/ {print $2; found=1; exit} END { if (!found) exit 1 }') || {
        echo "TEST: [$TEST_NAME] failed at ${stage}! Unable to parse row count from SQL output:"
        printf '%s\n' "$table_count_output"
        exit 1
    }
    table_count=$(printf '%s' "$table_count" | tr -d '[:space:]')
    if [ "$table_count" != "2" ]; then
        echo "TEST: [$TEST_NAME] failed at ${stage}! Expected 2 rows, got $table_count"
        exit 1
    fi

    # Verify masking policy metadata behavior for db-scope restore.
    # BR should replay masking policy DDL semantics for restored tables.
    policy_output_after=$(run_sql "SHOW MASKING POLICIES FOR ${DB}.t;")
    policy_count_after=$(printf '%s\n' "$policy_output_after" | grep -c "test_policy" || true)
    if [ "$policy_count_after" != "1" ]; then
        echo "TEST: [$TEST_NAME] failed at ${stage}! Expected 1 masking policy after db-scope restore, got $policy_count_after"
        exit 1
    fi

    # Use the restored table ID for verification. Name-based filtering can hit stale
    # metadata rows for dropped tables in the same cluster and become flaky.
    restored_table_id_output=$(run_sql "SELECT TIDB_TABLE_ID FROM information_schema.TABLES WHERE TABLE_SCHEMA='${DB}' AND TABLE_NAME='t';")
    restored_table_id=$(printf '%s\n' "$restored_table_id_output" | awk -F': ' '/TIDB_TABLE_ID/ {print $2; found=1; exit} END { if (!found) exit 1 }') || {
        echo "TEST: [$TEST_NAME] failed at ${stage}! Unable to parse restored table ID from SQL output:"
        printf '%s\n' "$restored_table_id_output"
        exit 1
    }
    restored_table_id=$(printf '%s' "$restored_table_id" | tr -d '[:space:]')

    policy_row_count_output_after=$(run_sql "SELECT COUNT(*) FROM mysql.tidb_masking_policy WHERE table_id=${restored_table_id} AND policy_name='test_policy';")
    policy_row_count_after=$(printf '%s\n' "$policy_row_count_output_after" | awk -F': ' '/COUNT\(\*\)/ {print $2; found=1; exit} END { if (!found) exit 1 }') || {
        echo "TEST: [$TEST_NAME] failed at ${stage}! Unable to parse masking policy row count from SQL output:"
        printf '%s\n' "$policy_row_count_output_after"
        exit 1
    }
    policy_row_count_after=$(printf '%s' "$policy_row_count_after" | tr -d '[:space:]')
    if [ "$policy_row_count_after" != "1" ]; then
        echo "TEST: [$TEST_NAME] failed at ${stage}! Expected 1 row in mysql.tidb_masking_policy for restored table_id=${restored_table_id} after db-scope restore, got $policy_row_count_after"
        exit 1
    fi
}

# Phase 1: full backup then db-scope restore.
echo "full backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/${DB}_full"
run_sql "DROP DATABASE $DB;"

echo "restore from full backup start..."
run_br restore db --db "$DB" -s "local://$TEST_DIR/${DB}_full" --pd $PD_ADDR
check_restored_db "restore-from-full"

# Phase 2: backup db from restored cluster, then restore db again.
echo "db backup from restored cluster start..."
run_br --pd $PD_ADDR backup db --db "$DB" -s "local://$TEST_DIR/${DB}_db_from_restored"
run_sql "DROP DATABASE $DB;"

echo "restore from db backup start..."
run_br restore db --db "$DB" -s "local://$TEST_DIR/${DB}_db_from_restored" --pd $PD_ADDR
check_restored_db "restore-from-db-backup"

echo "TEST: [$TEST_NAME] passed!"
run_sql "DROP DATABASE $DB;"
