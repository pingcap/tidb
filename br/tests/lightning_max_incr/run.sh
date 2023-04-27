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

# db.test contains key that is less than int64 - 1
# while db.test1 contains key that equals int64 - 1
run_lightning --sorted-kv-dir "$TEST_DIR/sst" --config "tests/$TEST_NAME/config.toml" --log-file "$TEST_DIR/lightning.log"
check_result
# successfully insert: max key has not reached maximum
run_sql 'INSERT INTO db.test(b) VALUES(11);'
# fail for insertion: db.test1 has key int64 - 1
run_sql 'INSERT INTO db.test1(b) VALUES(22);' 2>&1 | tee -a "$TEST_DIR/sql_res.$TEST_NAME.txt"
check_contains 'ERROR'
cleanup
