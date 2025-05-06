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

run_sql "CREATE TABLE ${DB}1.br_stats_partition (id INT NOT NULL, store_id INT NOT NULL, KEY i1(id)) PARTITION BY RANGE (store_id) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN MAXVALUE);"
for j in $(seq 9); do
    run_sql "INSERT INTO ${DB}1.br_stats_partition values ($j, $j);"
done

unset BR_LOG_TO_TERM
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB" --log-file $LOG --ignore-stats=false --filter "${DB}1.*" --filter "${DB}2.*" || cat $LOG
dump_cnt=$(cat $LOG | grep "dump stats to json" | wc -l)
dump_db1_cnt=$(cat $LOG | grep "dump stats to json" | grep "${DB}1" | wc -l)
dump_db2_cnt=$(cat $LOG | grep "dump stats to json" | grep "${DB}2" | wc -l)
dump_mark=$((${dump_cnt}+10*${dump_db1_cnt}+100*${dump_db2_cnt}))
echo "dump stats count: ${dump_cnt}; db1 count: ${dump_db1_cnt}; db2 count: ${dump_db2_cnt}; dump mark: ${dump_mark}"

if [ "${dump_mark}" -ne "123" ]; then
    echo "TEST: [$TEST_NAME] fail on dump stats"
    echo $(cat $LOG | grep "dump stats to json")
    exit 1
fi

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done

rm -f $LOG
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB" --log-file $LOG --filter "${DB}1.*" || cat $LOG
load_cnt=$(cat $LOG | grep "restore statistic data done" | wc -l)
load_db1_cnt=$(cat $LOG | grep "restore statistic data done" | grep "${DB}1" | wc -l)
load_mark=$((${load_cnt}+10*${load_db1_cnt}))
echo "load stats count: ${load_cnt}; db1 count: ${load_db1_cnt}; load mark: ${load_mark}"

if [ "${load_mark}" -ne "22" ]; then
    echo "TEST: [$TEST_NAME] fail on load stats"
    echo $(cat $LOG | grep "restore statistic data done")
    exit 1
fi

run_sql "DROP DATABASE ${DB}1;"

rm -f $LOG
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB" --log-file $LOG --load-stats=false --filter "${DB}1.*" || cat $LOG
table_count=$(run_sql "SELECT meta.count as count FROM mysql.stats_meta meta JOIN INFORMATION_SCHEMA.TABLES tables ON meta.table_id = tables.TIDB_TABLE_ID WHERE tables.TABLE_SCHEMA = '${DB}1' and modify_count = 0;" | awk '/count/{print $2}')
if [ "${table_count}" -ne 9 ]; then
    echo "table stats meta count does not equal to 9, but $count instead"
    exit 1
fi
p0_count=$(run_sql "SELECT meta.count as count FROM mysql.stats_meta meta JOIN INFORMATION_SCHEMA.PARTITIONS parts ON meta.table_id = parts.TIDB_PARTITION_ID WHERE parts.TABLE_SCHEMA = '${DB}1' and parts.PARTITION_NAME = 'p0' and modify_count = 0;" | awk '/count/{print $2}')
if [ "${p0_count}" -ne 5 ]; then
    echo "partition p0 stats meta count does not equal to 5, but $p0_count instead"
    exit 1
fi
p1_count=$(run_sql "SELECT meta.count as count FROM mysql.stats_meta meta JOIN INFORMATION_SCHEMA.PARTITIONS parts ON meta.table_id = parts.TIDB_PARTITION_ID WHERE parts.TABLE_SCHEMA = '${DB}1' and parts.PARTITION_NAME = 'p1' and modify_count = 0;" | awk '/count/{print $2}')
if [ "${p1_count}" -ne 4 ]; then
    echo "partition p1 stats meta count does not equal to 4, but $p1_count instead"
    exit 1
fi

# test auto analyze
run_sql "DROP DATABASE ${DB}1;"

rm -f $LOG
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB" --log-file $LOG --load-stats=false --filter "${DB}1.*" --auto-analyze || cat $LOG
table_count=$(run_sql "SELECT meta.count as count FROM mysql.stats_meta meta JOIN INFORMATION_SCHEMA.TABLES tables ON meta.table_id = tables.TIDB_TABLE_ID WHERE tables.TABLE_SCHEMA = '${DB}1' and modify_count = count and count > 0;" | awk '/count/{print $2}')
if [ "${table_count}" -ne 9 ]; then
    echo "table stats meta count does not equal to 9, but $count instead"
    exit 1
fi
p0_count=$(run_sql "SELECT meta.count as count FROM mysql.stats_meta meta JOIN INFORMATION_SCHEMA.PARTITIONS parts ON meta.table_id = parts.TIDB_PARTITION_ID WHERE parts.TABLE_SCHEMA = '${DB}1' and parts.PARTITION_NAME = 'p0' and modify_count = count and count > 0;" | awk '/count/{print $2}')
if [ "${p0_count}" -ne 5 ]; then
    echo "partition p0 stats meta count does not equal to 5, but $p0_count instead"
    exit 1
fi
p1_count=$(run_sql "SELECT meta.count as count FROM mysql.stats_meta meta JOIN INFORMATION_SCHEMA.PARTITIONS parts ON meta.table_id = parts.TIDB_PARTITION_ID WHERE parts.TABLE_SCHEMA = '${DB}1' and parts.PARTITION_NAME = 'p1' and modify_count = count and count > 0;" | awk '/count/{print $2}')
if [ "${p1_count}" -ne 4 ]; then
    echo "partition p1 stats meta count does not equal to 4, but $p1_count instead"
    exit 1
fi
