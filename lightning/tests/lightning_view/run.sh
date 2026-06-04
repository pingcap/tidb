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

set -euE

run_sql "SELECT IF(@@global.sql_require_primary_key, 'SET GLOBAL sql_require_primary_key=ON;', 'SET GLOBAL sql_require_primary_key=OFF;') cmd;"
UNDO_CMD=$(read_result)
reset_sql_require_primary_key() {
  run_sql "$UNDO_CMD"
}
trap reset_sql_require_primary_key EXIT

# Dumpling emits placeholder CREATE TABLE files for views without primary keys.
# Lightning should still import the real views when downstream requires PKs.
run_sql 'SET GLOBAL sql_require_primary_key=ON'
value='OFF'
for _ in $(seq 60); do
  run_sql "SELECT IF(@@global.sql_require_primary_key, 'ON', 'OFF') value"
  value=$(read_result)
  if [ "$value" = 'ON' ]; then
    break
  fi
  sleep 1
done
[ "$value" = 'ON' ]

for BACKEND in local tidb; do
  if [ "$BACKEND" = 'local' ]; then
    check_cluster_version 4 0 0 'local backend' || continue
  fi

  run_sql 'DROP DATABASE IF EXISTS db0'
  run_sql 'DROP DATABASE IF EXISTS db1'

  # Start importing the tables.
  run_lightning --backend $BACKEND 2> /dev/null

  run_sql 'SELECT count(*), sum(i) FROM `db1`.v1'
  check_contains "count(*): 3"
  check_contains "sum(i): 6"

  run_sql 'SELECT count(*) FROM `db0`.v2'
  check_contains "count(*): 1"
  run_sql 'SELECT s FROM `db0`.v2'
  check_contains "s: test1"
done
