#!/bin/sh

set -eu

run_sql 'DROP DATABASE IF EXISTS csv'

out_file_name="$TEST_DIR/sql_res.$TEST_NAME.txt"
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace=return(true)"
start_time=$(date +'%s')
run_lightning --backend local > $out_file_name 2>&1 || true
export GO_FAILPOINTS=""
check_contains "The available disk of TiKV"
used_time=$(($(date +'%s') - $start_time))
echo "time used to run lightning ${used_time}s"
# writeAndIngestByRanges will retry for at least 810s on retryable error
if [ $used_time -ge 60 ]; then
  echo "lightning failed to fail fast"
  exit 1
fi

run_sql 'SELECT count(*) FROM csv.threads'
check_contains 'count(*): 0'


