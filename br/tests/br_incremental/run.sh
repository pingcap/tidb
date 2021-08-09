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
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu
DB="$TEST_NAME"
TABLE="usertable"

run_sql "CREATE DATABASE $DB;"

go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB
row_count_ori_full=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

# full backup
echo "full backup start..."
run_br --pd $PD_ADDR backup table -s "local://$TEST_DIR/$DB/full" --db $DB -t $TABLE

go-ycsb run mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB

# incremental backup
echo "incremental backup start..."
last_backup_ts=$(run_br validate decode --field="end-version" -s "local://$TEST_DIR/$DB/full" | grep -oE "^[0-9]+")
run_br --pd $PD_ADDR backup table -s "local://$TEST_DIR/$DB/inc" --db $DB -t $TABLE --lastbackupts $last_backup_ts
row_count_ori_inc=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

run_sql "DROP DATABASE $DB;"

# full restore
echo "full restore start..."
run_br restore table --db $DB --table $TABLE -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR
row_count_full=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')
# check full restore
if [ "${row_count_full}" != "${row_count_ori_full}" ];then
    echo "TEST: [$TEST_NAME] full restore fail on database $DB"
    exit 1
fi
# incremental restore
echo "incremental restore start..."
run_br restore table --db $DB --table $TABLE -s "local://$TEST_DIR/$DB/inc" --pd $PD_ADDR
row_count_inc=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')
# check full restore
if [ "${row_count_inc}" != "${row_count_ori_inc}" ];then
    echo "TEST: [$TEST_NAME] incremental restore fail on database $DB"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
