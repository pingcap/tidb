#!/bin/sh
#
# Copyright 2023 PingCAP, Inc.
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

# https://github.com/pingcap/tidb/issues/41559

set -eu
DB="$TEST_NAME"

run_sql "CREATE DATABASE $DB;"

run_sql "
USE $DB;

CREATE TABLE t0 (
    id BIGINT,
    data INT,
    PRIMARY KEY(id) CLUSTERED
) PARTITION BY RANGE(id) (
    PARTITION p0 VALUES LESS THAN (2),
    PARTITION p1 VALUES LESS THAN (4),
    PARTITION p2 VALUES LESS THAN (6)
);
INSERT INTO t0 VALUES (1, 1);
INSERT INTO t0 VALUES (2, 2);
INSERT INTO t0 VALUES (3, 3);
INSERT INTO t0 VALUES (4, 4);
INSERT INTO t0 VALUES (5, 5);
"

# backup table
echo "backup start..."
run_br --pd $PD_ADDR backup db -s "local://$TEST_DIR/$DB" --db $DB

run_sql "DROP DATABASE $DB;"
run_sql "CREATE DATABASE $DB;"

# restore table
echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

run_sql "ALTER TABLE $DB.t0 ADD INDEX idx(data);"

result=$(run_sql "ADMIN SHOW DDL JOBS 1 WHERE job_type LIKE '%ingest%';")

run_sql "ADMIN SHOW DDL JOBS 1;"

[ -n "$result" ] || { echo "adding index does not use ingest mode"; exit 1; }

run_sql "DROP DATABASE $DB;"
