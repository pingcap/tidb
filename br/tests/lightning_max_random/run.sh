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
    check_contains 'Tables_in_db: test2'
    run_sql 'SELECT count(*) FROM db.test;'
    check_contains 'count(*): 2'
    run_sql 'SELECT count(*) FROM db.test1;'
    check_contains 'count(*): 2'
    run_sql 'SELECT count(*) FROM db.test2;'
    check_contains 'count(*): 2'
}

cleanup() {
    rm -f $TEST_DIR/lightning.log
    rm -rf $TEST_DIR/sst
    run_sql 'DROP DATABASE IF EXISTS db;'
}

cleanup

# auto_random_max = 2^{64-1-10}-1
# db.test contains key auto_random_max - 1
# db.test1 contains key auto_random_max
# db.test2 contains key auto_random_max + 1 (overflow)
run_lightning --sorted-kv-dir "$TEST_DIR/sst" --config "tests/$TEST_NAME/config.toml" --log-file "$TEST_DIR/lightning.log"
check_result
# successfully insert: d.test auto_random key has not reached maximum
run_sql 'INSERT INTO db.test(b) VALUES(11);'
# fail for further insertion
run_sql 'INSERT INTO db.test(b) VALUES(22);' 2>&1 | tee -a "$TEST_DIR/sql_res.$TEST_NAME.txt"
check_contains 'ERROR'
# fail: db.test1 has key auto_random_max
run_sql 'INSERT INTO db.test1(b) VALUES(11);'
run_sql 'INSERT INTO db.test1(b) VALUES(22);' 2>&1 | tee -a "$TEST_DIR/sql_res.$TEST_NAME.txt"
check_contains 'ERROR'
# successfully insert for overflow key
run_sql 'INSERT INTO db.test2(b) VALUES(33);'
run_sql 'INSERT INTO db.test2(b) VALUES(44);'
run_sql 'INSERT INTO db.test2(b) VALUES(55);'
cleanup
