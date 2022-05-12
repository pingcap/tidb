#!/bin/sh

set -eu


out_file_name="$TEST_DIR/sql_res.$TEST_NAME.txt"

function prepare() {
  run_sql 'DROP DATABASE IF EXISTS csv'
  # lightning appends log, so we remove it
  rm -rf $TEST_DIR/lightning.log
}

echo "--> default sql_mode is strict, will report error for local backend"
prepare
run_lightning --backend local > $out_file_name 2>&1 || true
check_contains "Truncated incorrect decimal(10,6)"
run_sql 'SELECT count(*) FROM csv.t1'
check_contains 'count(*): 0'

echo "--> default sql_mode is strict, will report error for tidb backend"
prepare
run_lightning --backend tidb > $out_file_name 2>&1 || true
check_contains "Incorrect decimal value"
run_sql 'SELECT count(*) FROM csv.t1'
check_contains 'count(*): 0'

echo "--> without strict mode, log warnings on local backend"
prepare
run_lightning --backend local --config tests/$TEST_NAME/config-no-strict.toml
lightning_log_contains "Truncated incorrect decimal(10,6)"
run_sql 'SELECT d FROM csv.t1'
check_contains 'd: 2.000000'

echo "--> without strict mode, log warnings on tidb backend"
prepare
run_lightning --backend tidb --config tests/$TEST_NAME/config-no-strict.toml
lightning_log_contains "Warning, 1292, Truncated incorrect decimal(10,6)"
run_sql 'SELECT d FROM csv.t1'
check_contains 'd: 2.000000'

echo "--> without strict mode but ignore on duplicate, do not log warnings on tidb backend"
prepare
run_lightning --backend tidb --config tests/$TEST_NAME/config-no-strict-ignore.toml
lightning_log_not_contains "Warning, 1292, Truncated incorrect decimal(10,6)"
run_sql 'SELECT d FROM csv.t1'
check_contains 'd: 2.000000'
