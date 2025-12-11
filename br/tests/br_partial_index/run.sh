#!/bin/sh
#
# Copyright 2025 PingCAP, Inc.
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

run_sql "CREATE DATABASE $DB;"

run_sql "
USE $DB;

CREATE TABLE t0 (
    id int primary key,
    col1 int,
    col2 int,
    key idx_col1 (col1) where col2 > 10
);
INSERT INTO t0 VALUES (1, 1, 1);
INSERT INTO t0 VALUES (2, 2, 15);
INSERT INTO t0 VALUES (3, 3, 1);
INSERT INTO t0 VALUES (4, 4, 20);
INSERT INTO t0 VALUES (5, 5, 1);
"

# backup table
echo "backup start..."
run_br --pd $PD_ADDR backup db -s "local://$TEST_DIR/$DB" --db $DB

run_sql "DROP DATABASE $DB;"
run_sql "CREATE DATABASE $DB;"

# restore table
echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

if run_sql "admin check table ${DB}.t0;" | grep -q 'inconsistency'; then
    echo "TEST: [$TEST_NAME] failed after restoring $DB.t0"
    exit 1
fi

run_sql "show create table $DB.t0;"
check_contains "WHERE \`col2\` > 10"

run_sql "DROP DATABASE $DB;"
