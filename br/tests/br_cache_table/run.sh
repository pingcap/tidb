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
run_sql "create table $DB.cache_1 (id int);"
run_sql "insert into $DB.cache_1 values (1);"
run_sql "alter table $DB.cache_1 cache;"
run_sql "insert into $DB.cache_1 values (2),(3);"

echo "backup start..."
run_br backup db --db "$DB" -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

run_sql "drop schema $DB;"

echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

set -x

run_sql "select count(*) from $DB.cache_1;"
check_contains 'count(*): 3'

run_sql "select create_options from information_schema.tables where table_schema = '$DB' and table_name = 'cache_1';"
check_not_contains 'create_options: cached=on'

run_sql "drop schema $DB"
