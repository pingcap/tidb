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

set -eu
DB="$TEST_NAME"

# create schema and tables
run_sql "CREATE DATABASE $DB;"

run_sql "CREATE TABLE $DB.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.usertable1 VALUES (\"a\", \"b\");"
run_sql "INSERT INTO $DB.usertable1 VALUES (\"aa\", \"b\");"

run_sql "CREATE TABLE $DB.usertable2 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.usertable2 VALUES (\"c\", \"d\");"

# backup db with query
echo "backup db with query"
run_sql "BACKUP DATABASE $DB TO \"local://$TEST_DIR/${DB}_sql\";"

run_sql "DROP DATABASE $DB;"

# restore db with query
echo "restore db with query"
run_sql "RESTORE DATABASE $DB FROM \"local://$TEST_DIR/${DB}_sql\";"

table_count=$(run_sql "use $DB; show tables;" | grep "Tables_in" | wc -l)
if [ "$table_count" -ne "2" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

meta_count=($(run_sql "SELECT COUNT(*) FROM mysql.stats_meta where count > 0;" | awk '/COUNT/{print $2}'))
if [ "$meta_count" -ne "2" ];then
    echo "meta_count=$meta_count is incorrent"
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

# Test BR DDL query string
echo "testing DDL query..."
run_curl https://$TIDB_STATUS_ADDR/ddl/history | grep -E '/\*from\(br\)\*/CREATE TABLE'
run_curl https://$TIDB_STATUS_ADDR/ddl/history | grep -E '/\*from\(br\)\*/CREATE DATABASE'

run_sql "DROP DATABASE $DB;"
