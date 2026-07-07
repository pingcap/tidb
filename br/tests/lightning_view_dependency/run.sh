#!/bin/bash
#
# Copyright 2024 PingCAP, Inc.
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

original_sql_require_primary_key=$(
  run_sql "SHOW VARIABLES LIKE 'sql_require_primary_key';" | awk '/sql_require_primary_key/{print $2}' | tail -n 1
)
reset_sql_require_primary_key() {
  run_sql "SET GLOBAL sql_require_primary_key='${original_sql_require_primary_key}'"
}
trap reset_sql_require_primary_key EXIT

run_sql "SET GLOBAL sql_require_primary_key='ON'"
value='OFF'
for _ in $(seq 60); do
  value=$(run_sql "SHOW VARIABLES LIKE 'sql_require_primary_key';" | awk '/sql_require_primary_key/{print $2}' | tail -n 1)
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

  run_sql 'DROP DATABASE IF EXISTS db1'
  run_sql 'DROP DATABASE IF EXISTS db2'
  run_sql 'DROP DATABASE IF EXISTS db3'

  run_lightning --backend $BACKEND 2> /dev/null

  run_sql 'SELECT count(*) FROM `db1`.v1'
  check_contains 'count(*): 2'
  run_sql 'SELECT sum(id) FROM `db1`.v1'
  check_contains 'sum(id): 5'

  run_sql 'SELECT count(*) FROM `db2`.v2'
  check_contains 'count(*): 2'

  run_sql 'SELECT count(*) FROM `db2`.v3'
  check_contains 'count(*): 2'

  run_sql 'SELECT count(*) FROM `db3`.v4'
  check_contains 'count(*): 2'
  run_sql 'SELECT sum(id) FROM `db3`.v4'
  check_contains 'sum(id): 5'

  run_sql 'SELECT count(*) FROM `db3`.v5'
  check_contains 'count(*): 2'
  run_sql 'SELECT sum(id) FROM `db3`.v5'
  check_contains 'sum(id): 5'

  run_sql 'SELECT count(*) FROM `db3`.v6'
  check_contains 'count(*): 2'
  run_sql 'SELECT sum(id) FROM `db3`.v6'
  check_contains 'sum(id): 5'
done
