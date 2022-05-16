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

function check_result() {
  run_sql 'SHOW DATABASES;'
  check_contains 'Database: db';
  run_sql 'SHOW TABLES IN db;'
  check_contains 'Tables_in_db: test'
  run_sql 'SELECT count(*) FROM db.test;'
  check_contains 'count(*): 20'
}

run_sql 'DROP DATABASE IF EXISTS db;'
export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/lightning/mydump/MockInaccurateRowID=return(true)'
run_lightning --config "tests/$TEST_NAME/config.toml"
check_result
run_sql 'DROP DATABASE IF EXISTS db;'
unset GO_FAILPOINTS