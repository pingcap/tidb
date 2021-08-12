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

set +e
run_lightning --config "tests/$TEST_NAME/on.toml" --log-file "$TEST_DIR/sqlmode-error.log"
[ $? -ne 0 ] || exit 1
set -e

grep -q '\["kv convert failed"\].*\[original=.*kind=uint64,val=9.*\] \[originalCol=1\] \[colName=a\] \[colType="timestamp BINARY"\]' "$TEST_DIR/sqlmode-error.log"
