#!/bin/sh
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

run_sql 'DROP DATABASE IF EXISTS sqlmodedb'

run_lightning --config "tests/$TEST_NAME/off.toml"

run_sql 'SELECT a, b, hex(c), d FROM sqlmodedb.t WHERE id = 1'
check_contains 'a: 0000-00-00 00:00:00'
check_contains 'b: 127'
check_contains 'hex(c): 74'
check_contains 'd: '

run_sql 'SELECT a, b, hex(c), d FROM sqlmodedb.t WHERE id = 2'
check_contains 'a: 0000-00-00 00:00:00'
check_contains 'b: -128'
check_contains 'hex(c): F0'
check_contains 'd: x,y'

run_sql 'SELECT a, b, hex(c), d FROM sqlmodedb.t WHERE id = 3'
check_contains 'a: 0000-00-00 00:00:00'
check_contains 'b: 0'
check_contains 'hex(c): 99'
check_contains 'd: '

run_sql 'SELECT a, b, hex(c), d FROM sqlmodedb.t WHERE id = 4'
check_contains 'a: 2000-01-01 00:00:00'
check_contains 'b: 100'
check_contains 'hex(c): '
check_contains 'd: x,y'

run_sql 'SELECT a, b, hex(c), d FROM sqlmodedb.t WHERE id = 5'
check_contains 'a: 0000-00-00 00:00:00'
check_contains 'b: 0'
check_contains 'hex(c): '
check_contains 'd: '

run_sql 'DROP DATABASE IF EXISTS sqlmodedb'
run_sql 'DROP DATABASE IF EXISTS sqlmodedb_lightning_task_info'

run_lightning --config "tests/$TEST_NAME/on.toml" --log-file "$TEST_DIR/sqlmode-error.log"

grep -q '\["kv convert failed"\].*\[original=.*kind=uint64,val=9.*\] \[originalCol=1\] \[colName=a\] \[colType="timestamp BINARY"\]' "$TEST_DIR/sqlmode-error.log"

run_sql 'SELECT min(id), max(id) FROM sqlmodedb.t'
check_contains 'min(id): 4'
check_contains 'max(id): 4'

run_sql 'SELECT count(*) FROM sqlmodedb_lightning_task_info.type_error_v1'
check_contains 'count(*): 4'

run_sql 'SELECT path, `offset`, error, row_data FROM sqlmodedb_lightning_task_info.type_error_v1 WHERE table_name = "`sqlmodedb`.`t`" AND row_data LIKE "(1,%";'
check_contains 'path: sqlmodedb.t.1.sql'
check_contains 'offset: 53'
check_contains 'cannot convert datum from unsigned bigint to type timestamp.'
check_contains "row_data: (1,9,128,'too long','x,y,z')"

run_sql 'SELECT path, `offset`, error, row_data FROM sqlmodedb_lightning_task_info.type_error_v1 WHERE table_name = "`sqlmodedb`.`t`" AND row_data LIKE "(2,%";'
check_contains 'path: sqlmodedb.t.1.sql'
check_contains 'offset: 100'
check_contains "Incorrect timestamp value: '2000-00-00 00:00:00'"
check_contains "row_data: (2,'2000-00-00 00:00:00',-99999,'🤩',3)"

run_sql 'SELECT path, `offset`, error, row_data FROM sqlmodedb_lightning_task_info.type_error_v1 WHERE table_name = "`sqlmodedb`.`t`" AND row_data LIKE "(3,%";'
check_contains 'path: sqlmodedb.t.1.sql'
check_contains 'offset: 149'
check_contains "Incorrect timestamp value: '9999-12-31 23:59:59'"
check_contains "row_data: (3,'9999-12-31 23:59:59','NaN',x'99','x+y')"

run_sql 'SELECT path, `offset`, error, row_data FROM sqlmodedb_lightning_task_info.type_error_v1 WHERE table_name = "`sqlmodedb`.`t`" AND row_data LIKE "(5,%";'
check_contains 'path: sqlmodedb.t.1.sql'
check_contains 'offset: 237'
check_contains "Column 'a' cannot be null"
check_contains "row_data: (5,NULL,NULL,NULL,NULL)"
