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

set -eux

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

run_lightning_expecting_fail() {
    set +e
    run_lightning "$@"
    ERRCODE=$?
    set -e
    [ "$ERRCODE" != 0 ]
}

run_sql 'DROP DATABASE IF EXISTS charsets;'

# gb18030

run_lightning --config "$CUR/auto.toml" -d "$CUR/gb18030"
run_sql 'SELECT sum(`主键`) AS s FROM charsets.gb18030'
check_contains 's: 267'
run_sql 'DROP TABLE charsets.gb18030;'

run_lightning --config "$CUR/gb18030.toml" -d "$CUR/gb18030"
run_sql 'SELECT sum(`主键`) AS s FROM charsets.gb18030'
check_contains 's: 267'
run_sql 'DROP TABLE charsets.gb18030;'

run_lightning_expecting_fail --config "$CUR/utf8mb4.toml" -d "$CUR/gb18030"

run_lightning --config "$CUR/binary.toml" -d "$CUR/gb18030"
run_sql 'SELECT sum(`????`) AS s FROM charsets.gb18030'
check_contains 's: 267'

# utf8mb4

run_lightning --config "$CUR/auto.toml" -d "$CUR/utf8mb4"
run_sql 'SELECT sum(`主键`) AS s FROM charsets.utf8mb4'
check_contains 's: 1119'
run_sql 'DROP TABLE charsets.utf8mb4;'

run_lightning --config "$CUR/gb18030.toml" -d "$CUR/utf8mb4"
run_sql 'SELECT sum(`涓婚敭`) AS s FROM charsets.utf8mb4'
check_contains 's: 1119'
run_sql 'DROP TABLE charsets.utf8mb4;'

run_lightning --config "$CUR/utf8mb4.toml" -d "$CUR/utf8mb4"
run_sql 'SELECT sum(`主键`) AS s FROM charsets.utf8mb4'
check_contains 's: 1119'
run_sql 'DROP TABLE charsets.utf8mb4;'

run_lightning --config "$CUR/binary.toml" -d "$CUR/utf8mb4"
run_sql 'SELECT sum(`主键`) AS s FROM charsets.utf8mb4'
check_contains 's: 1119'

# mixed

run_lightning_expecting_fail --config "$CUR/auto.toml" -d "$CUR/mixed"
run_lightning_expecting_fail --config "$CUR/gb18030.toml" -d "$CUR/mixed"
run_lightning_expecting_fail --config "$CUR/utf8mb4.toml" -d "$CUR/mixed"

run_lightning --config "$CUR/binary.toml" -d "$CUR/mixed"
run_sql 'SELECT sum(`唯一键`) AS s FROM charsets.mixed'
check_contains 's: 5291'

# test about unsupported charset in UTF-8 encoding dump files
# test local backend
run_lightning --config "$CUR/greek.toml" -d "$CUR/greek" 2>&1 | grep -q "Unknown character set: 'greek'"
# check TiDB does not receive the DDL
check_not_contains "greek" $TEST_DIR/tidb.log
run_sql 'DROP DATABASE IF EXISTS charsets;'
run_sql 'CREATE DATABASE charsets;'
run_sql 'CREATE TABLE charsets.greek (c VARCHAR(20) PRIMARY KEY);'
run_lightning --config "$CUR/greek.toml" -d "$CUR/greek"
run_sql "SELECT count(*) FROM charsets.greek WHERE c = 'α';"
check_contains 'count(*): 1'
# test tidb backend
run_sql 'TRUNCATE TABLE charsets.greek;'
run_lightning --config "$CUR/greek.toml" -d "$CUR/greek" --backend tidb
run_sql "SELECT count(*) FROM charsets.greek WHERE c = 'α';"
check_contains 'count(*): 1'

# latin1
# wrong encoding will have wrong column name and data
run_lightning --config "$CUR/binary.toml" -d "$CUR/latin1" 2>&1 | grep -q "unknown columns in header"
run_sql 'DROP TABLE charsets.latin1;'
run_lightning --config "$CUR/utf8mb4.toml" -d "$CUR/latin1" 2>&1 | grep -q "invalid schema encoding"
run_lightning --config "$CUR/latin1-only-schema.toml" -d "$CUR/latin1" 2>&1 | grep -q "unknown columns in header"
run_lightning --config "$CUR/latin1.toml" -d "$CUR/latin1"
run_sql 'SELECT * FROM charsets.latin1'
check_contains 'ÏÐ: 1'
check_contains 'data: ‘’“”'
check_contains 'ÏÐ: 2'
check_contains 'data: ¡¢£¤'
