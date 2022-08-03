#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
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

set -eux
DB="$TEST_NAME"
TABLE="usertable"
LOG="not-leader.log"
DB_COUNT=3

for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
done

for i in $(seq $DB_COUNT); do
    row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done


# backup full
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB"

rm -f $LOG

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done


# restore full
echo "restore start..."

unset BR_LOG_TO_TERM
GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/restore/not-leader-error=1*return(true)->1*return(false);\
github.com/pingcap/tidb/br/pkg/restore/somewhat-retryable-error=3*return(true)" \
run_br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --ratelimit 1024 --log-file $LOG || true
BR_LOG_TO_TERM=1

grep "a error occurs on split region" $LOG && \
grep "split region meet not leader error" $LOG && \
grep "Full Restore success" $LOG && \
grep "find new leader" $LOG

if [ $? -ne 0 ]; then
    echo "failed to retry on failpoint."
    echo "full log:"
    cat $LOG
    exit 1
fi

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
fi

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i}"
done
