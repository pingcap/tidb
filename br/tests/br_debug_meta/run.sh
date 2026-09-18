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
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

run_sql "CREATE DATABASE $DB;"
go-ycsb load mysql -P $CUR/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB

table_region_sql="SELECT COUNT(*) FROM information_schema.tikv_region_status WHERE db_name = '$DB' AND table_name = '$TABLE';"
for i in $(seq 10); do
    regioncount=$(run_sql "$table_region_sql" | awk '/COUNT/{print $2}')
    [ $regioncount -ge 5 ] && break
    sleep 3
done
run_sql "$table_region_sql"

row_count_ori=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

# backup table with backupmetav2
echo "backup start..."
run_br --pd $PD_ADDR backup table --db $DB --table $TABLE -s "local://$TEST_DIR/$DB"

# Test validate decode
run_br validate decode -s "local://$TEST_DIR/$DB"

# should generate backupmeta.json
if [ ! -f "$TEST_DIR/$DB/jsons/backupmeta.json" ]; then
    echo "TEST: [$TEST_NAME] decode failed!"
    exit 1
fi

# backup table with backupmetav1
echo "backup start..."
run_br --pd $PD_ADDR backup table --db $DB --table $TABLE -s "local://$TEST_DIR/${DB}_2" --use-backupmeta-v2=false


# Test validate decode
run_br validate decode -s "local://$TEST_DIR/${DB}_2"

# should generate backupmeta.json
if [ ! -f "$TEST_DIR/${DB}_2/jsons/backupmeta.json" ]; then
    echo "TEST: [$TEST_NAME] decode failed!"
    exit 1
fi

# Test validate encode
run_br validate encode -s "local://$TEST_DIR/${DB}_2"

# should generate backupmeta_from_json
if [ ! -f "$TEST_DIR/${DB}_2/backupmeta_from_json" ]; then
    echo "TEST: [$TEST_NAME] encode failed!"
    exit 1
fi

# replace backupmeta
mv "$TEST_DIR/${DB}_2/backupmeta_from_json" "$TEST_DIR/${DB}_2/backupmeta"

# restore table
echo "restore start..."
run_sql "DROP DATABASE $DB;"
run_br --pd $PD_ADDR restore table --db $DB --table $TABLE -s "local://$TEST_DIR/${DB}_2"

row_count_new=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

if [ "${row_count_ori}" != "${row_count_new}" ];then
    echo "TEST: [$TEST_NAME] failed!, row count not equal after restore"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
