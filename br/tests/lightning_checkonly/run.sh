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

set -euE

out_file_name="$TEST_DIR/sql_res.$TEST_NAME.txt"
check_db_sql="select count(1) as s from information_schema.schemata where schema_name='test_check';"

echo ">>> parameter check"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only > $out_file_name 2>&1 || true
check_contains "flag needs an argument: -check-only"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only xxx > $out_file_name 2>&1 || true
check_contains "invalid check-only config"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only 2,1000 > $out_file_name 2>&1 || true
check_contains "rate should be in range (0, 1]"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only -1,1000 > $out_file_name 2>&1 || true
check_contains "rate should be in range (0, 1]"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only ,1000 > $out_file_name 2>&1 || true
check_contains "invalid check-only config"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only 1,-1000 > $out_file_name 2>&1 || true
check_contains "rows should be positive or -1"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only 1, > $out_file_name 2>&1 || true
check_contains "invalid check-only config"

echo ">>> check-only=default"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only default > $out_file_name
check_contains "tidb lightning exit successfully"
run_sql "$check_db_sql"
check_contains "s: 0"

echo ">>> check-only=default & check-requirements=true"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-requirements=true --check-only default > $out_file_name
check_contains "tidb lightning exit successfully"
run_sql "$check_db_sql"
check_contains "s: 0"

echo ">>> check-only sample for sql files"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only 1,1 > $out_file_name
check_contains "All checks have been passed"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only 1,2 > $out_file_name
check_contains "Total sample of 2 rows of data checked, 1 errors found."
check_contains "Some checks failed, please check the log for more information."
check_contains "Log file location: /tmp/backup_restore_test/lightning.log"
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only 1,3 > $out_file_name
check_contains "Total sample of 3 rows of data checked, 2 errors found."
check_contains "Some checks failed, please check the log for more information."
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only 1,-1 > $out_file_name
check_contains "Total sample of 3 rows of data checked, 2 errors found."
check_contains "Some checks failed, please check the log for more information."
run_lightning -d "tests/$TEST_NAME/data_sql" --check-only 1,100 > $out_file_name
check_contains "Total sample of 3 rows of data checked, 2 errors found."
check_contains "Some checks failed, please check the log for more information."
run_sql "$check_db_sql"
check_contains "s: 0"

echo ">>> check-only sample for csv files"
# check using binary charset
run_lightning -d "tests/$TEST_NAME/data_csv" --check-only 1,2 > $out_file_name
check_contains "All checks have been passed"
# check using gbk charset
run_lightning --config "tests/$TEST_NAME/config_gbk.toml" -d "tests/$TEST_NAME/data_csv" --check-only 1,1 > $out_file_name
check_contains "All checks have been passed"
run_lightning --config "tests/$TEST_NAME/config_gbk.toml" -d "tests/$TEST_NAME/data_csv" --check-only 1,2 > $out_file_name
check_contains "Total sample of 2 rows of data checked, 1 errors found."
check_contains "Some checks failed, please check the log for more information."
check_contains "Log file location: /tmp/backup_restore_test/lightning.log"
run_lightning --config "tests/$TEST_NAME/config_gbk.toml" -d "tests/$TEST_NAME/data_csv" --check-only 1,3 > $out_file_name
check_contains "Total sample of 3 rows of data checked, 2 errors found."
check_contains "Some checks failed, please check the log for more information."
run_sql "$check_db_sql"
check_contains "s: 0"

echo ">>> check-only sample for csv files and table schema already exists in tidb"
run_sql "create database test_check;"
run_sql "create table test_check.tbl1(id varchar(100));"
run_lightning --config "tests/$TEST_NAME/config_gbk.toml" -d "tests/$TEST_NAME/data_csv" --check-only 1,3 > $out_file_name
check_contains "Total sample of 3 rows of data checked, 2 errors found."
check_contains "Some checks failed, please check the log for more information."
run_sql "select count(1) s from test_check.tbl1"
check_contains "s: 0"
run_sql "drop database test_check"

echo ">>> check-only sample for multiple csv files"
# default sample rate=0.01, so at most 1 file will be checked
run_lightning --config "tests/$TEST_NAME/config_gbk.toml" -d "tests/$TEST_NAME/data_multiple_csv" --check-only default > $out_file_name
check_contains "Total sample of 3 rows of data checked"
run_lightning --config "tests/$TEST_NAME/config_gbk.toml" -d "tests/$TEST_NAME/data_multiple_csv" --check-only 1,1000 > $out_file_name
check_contains "Total sample of 6 rows of data checked"
run_sql "$check_db_sql"
check_contains "s: 0"

echo ">>> check-only for ignored column"
run_lightning --config "tests/$TEST_NAME/config_gbk_ignore_columns.toml" -d "tests/$TEST_NAME/data_csv" --check-only 1,3 > $out_file_name
check_contains "Total sample of 3 rows of data checked, 1 errors found."
check_contains "Some checks failed, please check the log for more information."
run_sql "select count(1) s from test_check.tbl1"
check_contains "s: 0"
