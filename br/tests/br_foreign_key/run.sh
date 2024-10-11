#!/bin/sh
#
# Copyright 2022 PingCAP, Inc.
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

for DDL_BATCH_SIZE in 1 2;
do
  run_sql "set @@global.foreign_key_checks=1;"
  run_sql "set @@foreign_key_checks=1;"
  run_sql "create schema $DB;"
  run_sql "create table $DB.t1 (id int key);"
  run_sql "create table $DB.t2 (id int key, a int, b int, foreign key fk_1 (a) references t1(id) ON UPDATE SET NULL ON DELETE SET NULL, foreign key fk_2 (b) references t1(id) ON DELETE CASCADE ON UPDATE CASCADE);"
  run_sql "insert into $DB.t1 values (1), (2), (3);"
  run_sql "insert into $DB.t2 values (1, 1, 1), (2, 2, 2), (3, 3, 3);"
  run_sql "update $DB.t1 set id=id+10 where id in (1, 3);"
  run_sql "delete from $DB.t1 where id = 2;"

  echo "backup start..."
  run_br backup db --db "$DB" -s "local://$TEST_DIR/$DB-$DDL_BATCH_SIZE" --pd $PD_ADDR

  run_sql "drop schema $DB;"

  echo "restore start..."
  run_br restore db --db $DB -s "local://$TEST_DIR/$DB-$DDL_BATCH_SIZE" --pd $PD_ADDR --ddl-batch-size=$DDL_BATCH_SIZE

  set -x

  run_sql "select count(*) from $DB.t1;"
  check_contains 'count(*): 2'

  run_sql "select count(*) from $DB.t2;"
  check_contains 'count(*): 2'

  run_sql "select id, a, b from $DB.t2;"
  check_contains 'id: 1'
  check_contains 'id: 3'
  check_contains 'a: NULL'
  check_contains 'b: 11'
  check_contains 'b: 13'

  run_sql "drop schema $DB"
done
