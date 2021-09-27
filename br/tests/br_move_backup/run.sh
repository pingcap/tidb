#!/bin/sh
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

run_sql "CREATE DATABASE $DB;"

go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB

row_count_ori=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

# backup table
echo "backup start..."
run_br --pd $PD_ADDR backup table -s "local://$TEST_DIR/$DB" --db $DB -t $TABLE

run_sql "DROP TABLE $DB.$TABLE;"

# change backup path
mv $TEST_DIR/$DB $TEST_DIR/another$DB

# restore table with old path
echo "restore with old path start..."
run_br restore table --db $DB --table $TABLE -s "local://$TEST_DIR/$DB" --pd $PD_ADDR || restore_old_fail=1

if [ "$restore_old_fail" -ne "1" ];then
    echo "TEST: [$TEST_NAME] test restore with old path failed!"
    exit 1
fi

# restore table
echo "restore start..."
run_br restore table --db $DB --table $TABLE -s "local://$TEST_DIR/another$DB" --pd $PD_ADDR

row_count_new=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

echo "[original] row count: $row_count_ori, [after br] row count: $row_count_new"

if [ "$row_count_ori" -ne "$row_count_new" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
