#!/usr/bin/env bash
#
# Copyright 2019 PingCAP, Inc.
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
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DB="$TEST_NAME"
TABLE="usertable"
TABLE_COUNT=16
PATH="$CUR:bin:$PATH"

echo "load data..."
DB=$DB TABLE=$TABLE TABLE_COUNT=$TABLE_COUNT prepare.sh

declare -A row_count_ori
declare -A row_count_new
declare -A row_count_new_global_index
declare -A row_count_new_non_unique_global_index

for i in $(seq $TABLE_COUNT) _Hash _List; do
    row_count_ori[$i]=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE${i};" | awk '/COUNT/{print $2}')
done

# backup full
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB"

# Remove the source records so incorrect global index handles cannot read the
# dropped table's old partitions and accidentally pass the restore checks.
for i in $(seq $TABLE_COUNT) _Hash _List; do
    run_sql "DELETE FROM $DB.$TABLE${i};"
done

run_sql "DROP DATABASE $DB;"

# restore full
echo "restore start..."
export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/restore/failed-before-create-ingest-index=return(true)'
if run_br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR > "$TEST_DIR/global-index-interrupted.log" 2>&1; then
    echo "expected global index repair to be interrupted"
    exit 1
fi
unset GO_FAILPOINTS
grep 'failed before create ingest index' "$TEST_DIR/global-index-interrupted.log"
run_br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

for i in $(seq $TABLE_COUNT) _Hash _List; do
    run_sql "SHOW CREATE TABLE $DB.$TABLE${i};" | grep 'PARTITION'
    run_sql "SET tidb_enable_fast_table_check=0; ADMIN CHECK TABLE $DB.$TABLE${i};"
    row_count_new[$i]=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE${i};" | awk '/COUNT/{print $2}')
    row_count_new_global_index[$i]=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE${i} use index(idx);" | awk '/COUNT/{print $2}')
    row_count_new_non_unique_global_index[$i]=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE${i} use index(idx1);" | awk '/COUNT/{print $2}')
done

fail=false
for i in $(seq $TABLE_COUNT) _Hash _List; do
    if [ "${row_count_ori[$i]}" != "${row_count_new[$i]}" ] || \
       [ "${row_count_ori[$i]}" != "${row_count_new_global_index[$i]}" ] || \
       [ "${row_count_ori[$i]}" != "${row_count_new_non_unique_global_index[$i]}" ]; then
        fail=true
        echo "TEST: [$TEST_NAME] fail on table $DB.$TABLE${i}"
    fi
    echo "table $DB.$TABLE${i} [original] row count: ${row_count_ori[$i]}, [after br] row count: ${row_count_new[$i]}, global index row count: ${row_count_new_global_index[$i]}, non-unique global index row count: ${row_count_new_non_unique_global_index[$i]}"
done

if $fail; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"

# Exercise filtered restore without checkpoints, then schema/table name routing.
for mode in no-checkpoint rename-schema rename-table; do
    target_db=$DB
    target_table=${TABLE}1
    args=(--filter "$DB.${TABLE}1")
    case "$mode" in
        no-checkpoint) args+=(--use-checkpoint=false) ;;
        rename-schema)
            target_db=${DB}_renamed
            args+=(--with-sys-table=false --rename "$DB:$target_db")
            ;;
        rename-table)
            target_table=renamed_table
            args+=(--with-sys-table=false --rename "$DB.${TABLE}1:$DB.$target_table")
            ;;
    esac
    run_br restore full -s "local://$TEST_DIR/$DB" --pd "$PD_ADDR" "${args[@]}"
    run_sql "SET tidb_enable_fast_table_check=0; ADMIN CHECK TABLE $target_db.$target_table;"
    restored_count=$(run_sql "SELECT COUNT(*) FROM $target_db.$target_table;" | awk '/COUNT/{print $2}')
    test "$restored_count" = "${row_count_ori[1]}"
    table_count=$(run_sql "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$target_db';" | awk '/COUNT/{print $2}')
    test "$table_count" = 1
    run_sql "DROP DATABASE $target_db;"
done
