#!/bin/bash
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

# before v4.0.5 tiflash doesn't support tls, so we should skip this test then
(check_cluster_version 4 0 5 'TiFlash' && [ -n "$TIFLASH" ]) || exit 0

set -euE
# Populate the mydumper source
DBPATH="$TEST_DIR/tiflash.mydump"
mkdir -p $DBPATH
DB=test_tiflash

cat > "$DBPATH/$DB.t1.0.sql" << _EOF_
INSERT INTO t1 (s, i, j) VALUES
  ("this_is_test1", 1, 1),
  ("this_is_test2", 2, 2),
  ("this_is_test3", 3, 3),
  ("this_is_test4", 4, 4),
  ("this_is_test5", 5, 5);
_EOF_

echo "INSERT INTO t2 VALUES (1, 1), (2, 2)" > "$DBPATH/$DB.t2.0.sql"
echo "INSERT INTO t2 VALUES (3, 3), (4, 4)" > "$DBPATH/$DB.t2.1.sql"

tiflash_replica_ready() {
  i=0
  run_sql "select sum(AVAILABLE) from information_schema.tiflash_replica WHERE TABLE_NAME = \"$1\""
  while ! check_contains "sum(AVAILABLE): 1" check; do
    i=$((i+1))
    if [ "$i" -gt 100 ]; then
        echo "wait tiflash replica ready timeout"
        return 1
    fi
    sleep 3
    run_sql "select sum(AVAILABLE) from information_schema.tiflash_replica WHERE TABLE_NAME = \"$1\""
  done
}

for BACKEND in tidb local; do
  run_sql "DROP DATABASE IF EXISTS $DB"
  run_sql "CREATE DATABASE $DB"
  run_sql "CREATE TABLE $DB.t1 (i INT, j INT, s varchar(32), PRIMARY KEY(s, i));"
  run_sql "ALTER TABLE $DB.t1 SET TIFLASH REPLICA 1;"
  tiflash_replica_ready t1
  run_sql "CREATE TABLE $DB.t2 (i INT, j TINYINT);"
  run_sql "ALTER TABLE $DB.t2 SET TIFLASH REPLICA 1;"
  tiflash_replica_ready t2

  run_lightning -d "$DBPATH" --backend $BACKEND 2> /dev/null

  run_sql "SELECT /*+ read_from_storage(tiflash[t1]) */ count(*), sum(i) FROM \`$DB\`.t1"
  check_contains "count(*): 5"
  check_contains "sum(i): 15"

  run_sql "SELECT /*+ read_from_storage(tiflash[t2]) */ count(*), sum(i) FROM \`$DB\`.t2"
  check_contains "count(*): 4"
  check_contains "sum(i): 10"

done
