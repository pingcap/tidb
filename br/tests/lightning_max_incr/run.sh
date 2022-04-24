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

set -eux

check_cluster_version 4 0 0 'local backend' || exit 0

ENGINE_COUNT=6

check_result() {
    run_sql 'SHOW DATABASES;'
    check_contains 'Database: db';
    run_sql 'SHOW TABLES IN db;'
    check_contains 'Tables_in_db: test'
    check_contains 'Tables_in_db: test1'
    run_sql 'SELECT count(*) FROM db.test;'
    check_contains 'count(*): 2'
    run_sql 'SELECT count(*) FROM db.test1;'
    check_contains 'count(*): 2'
}

cleanup() {
    rm -f $TEST_DIR/lightning.log
    rm -rf $TEST_DIR/sst
    run_sql 'DROP DATABASE IF EXISTS db;'
}

cleanup

run_lightning --sorted-kv-dir "$TEST_DIR/sst" --config "tests/$TEST_NAME/config.toml" --log-file "$TEST_DIR/lightning.log"
check_result
# local-backend set auto_increment to 9223372036854775807 after importing
# sql fail because of of duplicate key
run_sql 'INSERT INTO db.test(b) VALUES(11);' 2>&1 | tee -a "$TEST_DIR/sql_res.$TEST_NAME.txt"
check_contains 'ERROR'
# duplicate key update
run_sql 'INSERT INTO db.test(b) VALUES(11) ON DUPLICATE KEY UPDATE b=10000;'
run_sql 'SELECT b FROM db.test;'
check_contains 'b: 10000'
cleanup
