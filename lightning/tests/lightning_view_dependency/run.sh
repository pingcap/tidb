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

# Test that Lightning imports views in topological dependency order across
# databases.
#
# Dependency graph:
#   db1.t1 -> db1.v1
#   db1.v1 -> db2.v2, db2.v3
#   db2.v2 -> db3.v4, db3.v5, db3.v6
#   db2.v3 -> db3.v5
#   db3.v4 -> db3.v6

set -euE

run_sql "SELECT IF(@@global.sql_require_primary_key, 'SET GLOBAL sql_require_primary_key=ON;', 'SET GLOBAL sql_require_primary_key=OFF;') cmd;"
UNDO_CMD=$(read_result)
reset_sql_require_primary_key() {
  run_sql "$UNDO_CMD"
}
trap reset_sql_require_primary_key EXIT

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

  run_sql 'DROP DATABASE IF EXISTS db1'
  run_sql 'DROP DATABASE IF EXISTS db2'
  run_sql 'DROP DATABASE IF EXISTS db3'

  run_lightning --backend $BACKEND 2> /dev/null

  # db1.v1: SELECT id, name FROM t1 WHERE id > 1 → (2,'b'), (3,'c')
  run_sql 'SELECT count(*) FROM `db1`.v1'
  check_contains 'count(*): 2'
  run_sql 'SELECT sum(id) FROM `db1`.v1'
  check_contains 'sum(id): 5'

  # db2.v2: SELECT id FROM db1.v1 → (2), (3)
  run_sql 'SELECT count(*) FROM `db2`.v2'
  check_contains 'count(*): 2'

  # db2.v3: SELECT id, name FROM db1.v1 → (2,'b'), (3,'c')
  run_sql 'SELECT count(*) FROM `db2`.v3'
  check_contains 'count(*): 2'

  # db3.v4: SELECT id FROM db2.v2 → (2), (3)  (3-level chain)
  run_sql 'SELECT count(*) FROM `db3`.v4'
  check_contains 'count(*): 2'
  run_sql 'SELECT sum(id) FROM `db3`.v4'
  check_contains 'sum(id): 5'

  # db3.v5: SELECT v2.id, v3.name FROM db2.v2 JOIN db2.v3 ON v2.id = v3.id
  # → (2,'b'), (3,'c')  (diamond merge of v2 and v3)
  run_sql 'SELECT count(*) FROM `db3`.v5'
  check_contains 'count(*): 2'
  run_sql 'SELECT sum(id) FROM `db3`.v5'
  check_contains 'sum(id): 5'

  # db3.v6: WITH cte AS (SELECT id FROM db2.v2) SELECT id FROM cte UNION SELECT id FROM db3.v4
  # → (2), (3)  (root set-operator with CTE should not treat cte as a table dependency)
  run_sql 'SELECT count(*) FROM `db3`.v6'
  check_contains 'count(*): 2'
  run_sql 'SELECT sum(id) FROM `db3`.v6'
  check_contains 'sum(id): 5'
done
