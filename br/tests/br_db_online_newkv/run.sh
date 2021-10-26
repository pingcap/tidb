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
DB="$TEST_NAME"

run_sql "CREATE DATABASE $DB;"

run_sql "CREATE TABLE $DB.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.usertable1 VALUES (\"a\", \"b\");"
run_sql "INSERT INTO $DB.usertable1 VALUES (\"aa\", \"b\");"

run_sql "CREATE TABLE $DB.usertable2 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.usertable2 VALUES (\"c\", \"d\");"

# backup db
echo "backup start..."
run_br --pd $PD_ADDR backup db --db "$DB" -s "local://$TEST_DIR/$DB"

run_sql "DROP DATABASE $DB;"

# enable placement rules
run_pd_ctl -u https://$PD_ADDR config set enable-placement-rules true

# add new tikv for restore
# actaul tikv_addr are TIKV_ADDR${i}
TIKV_ADDR="127.0.0.1:2017"
TIKV_STATUS_ADDR="127.0.0.1:2019"
TIKV_COUNT=3

echo "Starting restore TiKV..."
for i in $(seq $TIKV_COUNT); do
    tikv-server \
      --pd "$PD_ADDR" \
      -A "$TIKV_ADDR$i" \
      --status-addr "$TIKV_STATUS_ADDR$i" \
      --log-file "$TEST_DIR/restore-tikv${i}.log" \
      -C "tests/config/restore-tikv.toml" \
      -s "$TEST_DIR/restore-tikv${i}" &
done
sleep 5

# restore db
echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --online

# TODO we should check whether the restore RPCs are send to the new TiKV.
table_count=$(run_sql "use $DB; show tables;" | grep "Tables_in" | wc -l)
if [ "$table_count" -ne "2" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_pd_ctl -u https://$PD_ADDR config set enable-placement-rules false

run_sql "DROP DATABASE $DB;"
