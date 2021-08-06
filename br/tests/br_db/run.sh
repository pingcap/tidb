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

set -eu
DB="$TEST_NAME"

PROGRESS_FILE="$TEST_DIR/progress_unit_file"
rm -rf $PROGRESS_FILE

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
# backup db
echo "backup start..."
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/progress-call-back=return(\"$PROGRESS_FILE\")"
run_br --pd $PD_ADDR backup db --db "$DB" -s "local://$TEST_DIR/$DB"
export GO_FAILPOINTS=""

# check if we use the region unit
if [[ "$(wc -l <$PROGRESS_FILE)" == "1" ]] && [[ $(grep -c "region" $PROGRESS_FILE) == "1" ]];
then
  echo "use the correct progress unit"
else
  echo "use the wrong progress unit, expect region"
  cat $PROGRESS_FILE
  exit 1
fi
rm -rf $PROGRESS_FILE

run_sql "DROP DATABASE $DB;"

# restore db
echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

table_count=$(run_sql "use $DB; show tables;" | grep "Tables_in" | wc -l)
if [ "$table_count" -ne "2" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

# Test BR DDL query string
echo "testing DDL query..."
run_curl https://$TIDB_STATUS_ADDR/ddl/history | grep -E '/\*from\(br\)\*/CREATE TABLE'
run_curl https://$TIDB_STATUS_ADDR/ddl/history | grep -E '/\*from\(br\)\*/CREATE DATABASE'

run_sql "DROP DATABASE $DB;"
