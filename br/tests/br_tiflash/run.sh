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

set -eu
DB="${TEST_NAME}_DATABASE"
RECORD_COUNT=1000


run_sql "CREATE DATABASE $DB"

run_sql "CREATE TABLE $DB.kv(k varchar(256) primary key, v int)"

run_sql "CREATE TABLE $DB.partition_kv(\
 k INT, \
 v INT, \
 PRIMARY KEY(k) CLUSTERED \
) PARTITION BY RANGE(k) (\
  PARTITION p0 VALUES LESS THAN (200), \
  PARTITION p1 VALUES LESS THAN (400), \
  PARTITION p2 VALUES LESS THAN MAXVALUE)"

stmt="INSERT INTO $DB.kv(k, v) VALUES ('1-record', 1)"
parition_stmt="INSERT INTO $DB.partition_kv(k, v) VALUES (1, 1)"
for i in $(seq 2 $RECORD_COUNT); do
    stmt="$stmt,('$i-record', $i)"
    parition_stmt="$parition_stmt,($i, $i)"
done
run_sql "$stmt"
run_sql "$parition_stmt"

if ! run_sql "ALTER TABLE $DB.kv SET TIFLASH REPLICA 1"; then
  # 10s should be enough for tiflash-proxy get started
  sleep 10
  run_sql "ALTER TABLE $DB.kv SET TIFLASH REPLICA 1"
  run_sql "ALTER TABLE $DB.partition_kv SET TIFLASH REPLICA 1"
fi


i=0
while ! [ $(run_sql "select * from information_schema.tiflash_replica" | grep "PROGRESS" | sed "s/[^0-9]//g") -eq 1 ]; do
    i=$(( i + 1 ))
    echo "Waiting for TiFlash synchronizing [$i]."
    if [ $i -gt 20 ]; then
        echo "Failed to sync data to tiflash."
        exit 1
    fi
    sleep 5
done

rm -rf "/${TEST_DIR}/$DB"
run_br backup full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

run_sql "DROP DATABASE $DB"
run_br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --wait-tiflash-ready=true

# check TiFlash sync
echo "wait 3 seconds for tiflash tick puller triggered"
sleep 3
if ! [ $(run_sql "select * from information_schema.tiflash_replica" | grep "PROGRESS" | sed "s/[^0-9]//g") -eq 1 ]; then
    echo "restore didn't wait tiflash synced after set --wait-tiflash-ready=true."
    exit 1
fi

AFTER_BR_COUNT=`run_sql "SELECT count(*) FROM $DB.kv;" | sed -n "s/[^0-9]//g;/^[0-9]*$/p" | tail -n1`
if [ "$AFTER_BR_COUNT" -ne "$RECORD_COUNT" ]; then
    echo "failed to restore, before: $RECORD_COUNT; after: $AFTER_BR_COUNT"
    exit 1
fi

run_sql "DROP DATABASE $DB"
