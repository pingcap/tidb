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

set -eu

run_sql 'DROP DATABASE IF EXISTS csv'

out_file_name="$TEST_DIR/sql_res.$TEST_NAME.txt"
export GO_FAILPOINTS="github.com/pingcap/tidb/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace=return(true)"
start_time=$(date +'%s')
run_lightning --backend local > $out_file_name 2>&1 || true
export GO_FAILPOINTS=""
check_contains "the remaining storage capacity of TiKV"
used_time=$(($(date +'%s') - $start_time))
echo "time used to run lightning ${used_time}s"
# writeAndIngestByRanges will retry for at least 810s on retryable error
if [ $used_time -ge 60 ]; then
  echo "lightning failed to fail fast"
  exit 1
fi

run_sql 'SELECT count(*) FROM csv.threads'
check_contains 'count(*): 0'


