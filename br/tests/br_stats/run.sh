#!/bin/sh
#
# Copyright 2023 PingCAP, Inc.
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
DB_COUNT=3
LOG=/$TEST_DIR/backup.log
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    go-ycsb load mysql -P $CUR/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
done

unset BR_LOG_TO_TERM
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB" --log-file $LOG --ignore-stats=false --filter "${DB}1.*" || cat $LOG
dump_cnt=$(cat $LOG | grep "dump stats to json" | grep "${DB}1" | wc -l)
echo "dump stats count: ${dump_cnt}"

if [ "${dump_cnt}" -ne "1" ];then
    echo "TEST: [$TEST_NAME] fail on dump stats"
    echo $(cat $LOG | grep "dump stats to json")
    exit 1
fi

