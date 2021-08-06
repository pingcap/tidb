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
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux
DB="$TEST_NAME"

run_sql "create schema $DB;"

run_sql "create table $DB.one(c int);"
run_sql "create table $DB.two(c int);"
run_sql "create table $DB.three(c int);"
run_sql "create table $DB.four(c int);"
run_sql "create table $DB.FIVE(c int);"
run_sql "create table $DB.TEN(c int);"
run_sql 'create table '"$DB"'.`the,special,table`(c int);'

run_sql "insert into $DB.one values (1);"
run_sql "insert into $DB.two values (2);"
run_sql "insert into $DB.three values (3);"
run_sql "insert into $DB.four values (4);"
run_sql "insert into $DB.FIVE values (5);"
run_sql "insert into $DB.TEN values (10);"
run_sql 'insert into '"$DB"'.`the,special,table` values (375);'

echo 'Simple check'

run_br backup full -f "$DB.*" -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR
run_sql "drop schema $DB;"
run_br restore full -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR

run_sql "select c from $DB.one;"
run_sql "select c from $DB.two;"
run_sql "select c from $DB.three;"
run_sql "select c from $DB.four;"
run_sql "select c from $DB.FIVE;"
run_sql "select c from $DB.TEN;"
run_sql 'select c from '"$DB"'.`the,special,table`;'

echo 'Filtered backup check'

run_br backup full -f "$DB.t*" -s "local://$TEST_DIR/$DB/t" --pd $PD_ADDR
run_sql "drop schema $DB;"
run_br restore full -s "local://$TEST_DIR/$DB/t" --pd $PD_ADDR

! run_sql "select c from $DB.one;"
run_sql "select c from $DB.two;"
run_sql "select c from $DB.three;"
! run_sql "select c from $DB.four;"
! run_sql "select c from $DB.FIVE;"
run_sql "select c from $DB.TEN;"
run_sql 'select c from '"$DB"'.`the,special,table`;'

echo 'Filtered restore check'

run_sql "drop schema $DB;"
run_br restore full -f "*.f*" -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR

! run_sql "select c from $DB.one;"
! run_sql "select c from $DB.two;"
! run_sql "select c from $DB.three;"
run_sql "select c from $DB.four;"
run_sql "select c from $DB.FIVE;"
! run_sql "select c from $DB.TEN;"
! run_sql 'select c from '"$DB"'.`the,special,table`;'

echo 'Multiple filters check'

run_sql "drop schema $DB;"
run_br restore full -f '*.*' -f '!*.five' -f '!*.`the,special,table`' -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR

run_sql "select c from $DB.one;"
run_sql "select c from $DB.two;"
run_sql "select c from $DB.three;"
run_sql "select c from $DB.four;"
! run_sql "select c from $DB.FIVE;"
run_sql "select c from $DB.TEN;"
! run_sql 'select c from '"$DB"'.`the,special,table`;'

echo 'Case sensitive restore check'

run_sql "drop schema $DB;"
run_br restore full --case-sensitive -f '*.t*' -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR

! run_sql "select c from $DB.one;"
run_sql "select c from $DB.two;"
run_sql "select c from $DB.three;"
! run_sql "select c from $DB.four;"
! run_sql "select c from $DB.FIVE;"
! run_sql "select c from $DB.TEN;"
run_sql 'select c from '"$DB"'.`the,special,table`;'

echo 'Case sensitive backup check'

run_sql "drop schema $DB;"
run_br restore full --case-sensitive -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR
run_br backup full --case-sensitive -f "$DB.[oF]*" -s "local://$TEST_DIR/$DB/of" --pd $PD_ADDR
run_sql "drop schema $DB;"
run_br restore full --case-sensitive -s "local://$TEST_DIR/$DB/of" --pd $PD_ADDR

run_sql "select c from $DB.one;"
! run_sql "select c from $DB.two;"
! run_sql "select c from $DB.three;"
! run_sql "select c from $DB.four;"
run_sql "select c from $DB.FIVE;"
! run_sql "select c from $DB.TEN;"
! run_sql 'select c from '"$DB"'.`the,special,table`;'

run_sql "drop schema $DB;"
