#!/bin/bash
#
# Copyright 2021 PingCAP, Inc.
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

# This test can't be triggered until ci support azurite
exit 0
set -eux
DB="$TEST_NAME"
TABLE="usertable"
DB_COUNT=3

AZBLOB_ENDPOINT="http://127.0.0.1:10000/devstoreaccount1"
# azurite default account
ACCOUNT_NAME="devstoreaccount1"
ACCOUNT_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
CONTAINER="test"

rm -rf "$TEST_DIR/$DB"
mkdir -p "$TEST_DIR/$DB"

# Fill in the database
for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
done

for i in $(seq $DB_COUNT); do
  row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

# new version backup full
echo "backup start..."
run_br --pd $PD_ADDR backup full \
    -s "azure://$CONTAINER/$DB?account-name=$ACCOUNT_NAME&account-key=$ACCOUNT_KEY&endpoint=$AZBLOB_ENDPOINT&access-tier=Cool"

# clean up
for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done

# new version restore full
echo "restore start..."
run_br restore full \
    -s "azure://$CONTAINER/$DB?" \
    --pd $PD_ADDR --azblob.endpoint="$AZBLOB_ENDPOINT" \
    --azblob.account-name="$ACCOUNT_NAME" \
    --azblob.account-key="$ACCOUNT_KEY"

for i in $(seq $DB_COUNT); do
    row_count_new[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

fail=false
for i in $(seq $DB_COUNT); do
    if [ "${row_count_ori[i]}" != "${row_count_new[i]}" ];then
        fail=true
        echo "TEST: [$TEST_NAME] fail on database $DB${i}"
    fi
    echo "database $DB${i} [original] row count: ${row_count_ori[i]}, [after br] row count: ${row_count_new[i]}"
done

if $fail; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
else
    echo "TEST: [$TEST_NAME] succeed!"
fi

# clean up
for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done