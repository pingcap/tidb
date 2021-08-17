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
DB="$TEST_NAME"
TABLE="usertable"
TABLE_COUNT=16
PATH="tests/$TEST_NAME:bin:$PATH"

echo "load data..."
DB=$DB TABLE=$TABLE TABLE_COUNT=$TABLE_COUNT prepare.sh

declare -A row_count_ori
declare -A row_count_new

for i in $(seq $TABLE_COUNT) _Hash _List; do
    row_count_ori[$i]=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE${i};" | awk '/COUNT/{print $2}')
done

# backup full
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB"

run_sql "DROP DATABASE $DB;"

# restore full
echo "restore start..."
run_br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

for i in $(seq $TABLE_COUNT) _Hash _List; do
    run_sql "SHOW CREATE TABLE $DB.$TABLE${i};" | grep 'PARTITION'
    row_count_new[$i]=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE${i};" | awk '/COUNT/{print $2}')
done

fail=false
for i in $(seq $TABLE_COUNT) _Hash _List; do
    if [ "${row_count_ori[$i]}" != "${row_count_new[$i]}" ];then
        fail=true
        echo "TEST: [$TEST_NAME] fail on table $DB.$TABLE${i}"
    fi
    echo "table $DB.$TABLE${i} [original] row count: ${row_count_ori[$i]}, [after br] row count: ${row_count_new[$i]}"
done

if $fail; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
