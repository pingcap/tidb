#!/bin/sh
#
# Copyright 2022 PingCAP, Inc.
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

check_cluster_version 5 4 0 'new collation' || { echo 'TiDB does not support new collation! skipping test'; exit 0; }

set -eu

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
. $cur/../_utils/run_services

# restart cluster with new collation enabled
start_services --tidb-cfg $cur/tidb-new-collation.toml

DB="$TEST_NAME"
PROGRESS_FILE="$TEST_DIR/progress_unit_file"
rm -rf $PROGRESS_FILE

run_sql "CREATE DATABASE $DB;"

run_sql "CREATE TABLE $DB.测试 ( \
    a char(20) DEFAULT NULL, \
    b tinyblob, \
    c binary(100) DEFAULT NULL, \
    d json DEFAULT NULL, \
    e timestamp NULL DEFAULT NULL, \
    f set('a一','b二','c三','d四') DEFAULT NULL, \
    g text, \
    h enum('a一','b二','c三','d四') DEFAULT 'c三' \
) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci;"

run_sql "INSERT INTO $DB.测试 VALUES ('你好', '你好', '你好', '{\"测试\": \"你好\"}', '2018-10-13', 1, '你好', 'a一');"
run_sql "INSERT INTO $DB.测试 VALUES ('你好123', '你好', '你好', '{\"测试\": \"你好\"}', '2018-10-13', 1, '你好', 'a一');"

run_sql "CREATE TABLE $DB.测试2 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=gbk;"

run_sql "INSERT INTO $DB.测试2 VALUES (\"测试\", \"你\");"

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

meta_count=$(run_sql "SHOW STATS_META where Row_count > 0;")
if [ "$meta_count" -ne "2" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "SELECT * from $DB.测试;"
check_contains "你好123"
check_contains "{\"测试\": \"你好\"}"
run_sql "SELECT hex(a) from $DB.测试;"
check_contaions "C4E3BAC3"
run_sql "SELECT * from $DB.测试2;"
check_contains "你"
check_contains "测试"

# Test BR DDL query string
echo "testing DDL query..."
run_curl https://$TIDB_STATUS_ADDR/ddl/history | grep -E '/\*from\(br\)\*/CREATE TABLE'
run_curl https://$TIDB_STATUS_ADDR/ddl/history | grep -E '/\*from\(br\)\*/CREATE DATABASE'

run_sql "DROP DATABASE $DB;"
