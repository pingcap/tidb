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

run_sql "create schema $DB;"

run_sql "create table $DB.sbtest(id bigint primary key, c char(120) not null);"
run_sql "insert into $DB.sbtest values (9223372036854775807, 'test');"
run_sql "insert into $DB.sbtest values (9187343239835811840, 'test');"

run_sql "create table $DB.sbtest2(id bigint unsigned primary key, c char(120) not null);"
run_sql "insert into $DB.sbtest2 values (18446744073709551615, 'test');"
run_sql "insert into $DB.sbtest2 values (9223372036854775808, 'test');"

# backup db
echo "backup start..."
run_br backup db --db "$DB" -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

run_sql "drop schema $DB;"

# restore db
echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

run_sql "drop schema $DB;"
