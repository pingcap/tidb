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

trim_sql_result() {
    tail -n1 | sed 's/[^0-9]//g'
}

run_sql "create schema $DB;"
run_sql "create view $DB.view_1 as select 331 as m;"
run_sql "create view $DB.view_2 as select * from $DB.view_1;"
run_sql "create sequence $DB.seq_1 nocache cycle maxvalue 40;"
run_sql "create table $DB.table_1 (m int primary key default next value for $DB.seq_1, b int);"
run_sql "insert into $DB.table_1 (b) values (8), (12), (16), (20);"
run_sql "create sequence $DB.seq_2;"
run_sql "create table $DB.table_2 (a int default next value for $DB.seq_1, b int default next value for $DB.seq_2, c int);"
run_sql "insert into $DB.table_2 (c) values (24), (28), (32);"
run_sql "create view $DB.view_3 as select m from $DB.table_1 union select a * b as m from $DB.table_2 union select m from $DB.view_2;"
run_sql "drop view $DB.view_1;"
run_sql "create view $DB.view_1 as select 133 as m;"

run_sql "create table $DB.auto_inc (n int primary key AUTO_INCREMENT);"
run_sql "insert into $DB.auto_inc values (), (), (), (), ();"
last_id=$(run_sql "select n from $DB.auto_inc order by n desc limit 1" | trim_sql_result)

run_sql "create table $DB.auto_rnd (n BIGINT primary key AUTO_RANDOM(8));"
last_rnd_id=$(run_sql "insert into $DB.auto_rnd values (), (), (), (), ();select last_insert_id() & 0x7fffffffffffff;" | trim_sql_result )

echo "backup start..."
run_br backup db --db "$DB" -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

run_sql "drop schema $DB;"

echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

set -x

views_count=$(run_sql "select count(*) c, sum(m) s from $DB.view_3;" | tail -2 | paste -sd ';' -)
[ "$views_count" = 'c: 8;s: 181' ]

run_sql "insert into $DB.table_2 (c) values (33);"
seq_val=$(run_sql "select a >= 8 and b >= 4 as g from $DB.table_2 where c = 33;" | tail -1)
[ "$seq_val" = 'g: 1' ]

run_sql "insert into $DB.auto_inc values ();"
last_id_after_restore=$(run_sql "select n from $DB.auto_inc order by n desc limit 1;" | trim_sql_result)
[ $last_id_after_restore -gt $last_id ]
rnd_last_id_after_restore=$(run_sql "insert into $DB.auto_rnd values ();select last_insert_id() & 0x7fffffffffffff;" | trim_sql_result )
[ $rnd_last_id_after_restore -gt $last_rnd_id ]
rnd_count_after_restore=$(run_sql "select count(*) from $DB.auto_rnd;" | trim_sql_result )
[ $rnd_count_after_restore -gt 5 ]


run_sql "drop schema $DB"
