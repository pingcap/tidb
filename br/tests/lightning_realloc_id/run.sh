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

# Basic check for whether partitioned tables work.

set -eu
check_cluster_version 4 0 0 'local backend'
LOG_FILE1="$TEST_DIR/lightning-realloc-import1.log"
LOG_FILE2="$TEST_DIR/lightning-realloc-import2.log"

function check_result() {
  run_sql 'SHOW DATABASES;'
  check_contains 'Database: db';
  run_sql 'SHOW TABLES IN db;'
  check_contains 'Tables_in_db: test'
  run_sql 'SELECT count(*) FROM db.test;'
  check_contains 'count(*): 20'
  run_sql 'SELECT * FROM db.test;'
  check_contains 'id: 15'
  check_contains 'id: 20'
}

function parallel_import() {
  run_lightning -d "tests/$TEST_NAME/data" \
    --sorted-kv-dir "$TEST_DIR/lightning_realloc_import.sorted1" \
    --log-file "$LOG_FILE1" \
    --config "tests/$TEST_NAME/config.toml" &
  pid1="$!"
  run_lightning -d "tests/$TEST_NAME/data1" \
  --sorted-kv-dir "$TEST_DIR/lightning_realloc_import.sorted2" \
  --log-file "$LOG_FILE2" \
  --config "tests/$TEST_NAME/config.toml" &
  pid2="$!"
  wait "$pid1" "$pid2"
}

function check_parallel_result() {
  run_sql 'SHOW DATABASES;'
  check_contains 'Database: db';
  run_sql 'SHOW TABLES IN db;'
  check_contains 'Tables_in_db: test'
  run_sql 'SELECT count(*) FROM db.test;'
  check_contains 'count(*): 40'
}

run_sql 'DROP DATABASE IF EXISTS db;'
export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/lightning/mydump/MockInaccurateRowID=return(true)'
run_lightning --config "tests/$TEST_NAME/config.toml"
check_result
run_sql 'DROP DATABASE IF EXISTS db;'
parallel_import
check_parallel_result
run_sql 'DROP DATABASE IF EXISTS db;'
unset GO_FAILPOINTS