#!/bin/bash
#
# Copyright 2023 PingCAP, Inc.
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

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
mkdir -p "$TEST_DIR/data"

run_sql "DROP DATABASE IF EXISTS test;"
run_sql "DROP TABLE IF EXISTS test.t;"

cat <<EOF >"$TEST_DIR/data/test-schema-create.sql"
CREATE DATABASE test;
EOF
cat <<EOF >"$TEST_DIR/data/test.t-schema.sql"
CREATE TABLE test.t (a varchar(1024));
EOF

#
# test send-kv-pairs
#
set +x
for i in {1..100}; do
  echo "$i" >>"$TEST_DIR/data/test.t.0.csv"
done
set -x

# send-kv-pairs is deprecated, will not takes effect
rm -rf $TEST_DIR/lightning.log
export GO_FAILPOINTS="github.com/pingcap/tidb/pkg/lightning/backend/local/afterFlushKVs=return(true)"
run_lightning --backend local -d "$TEST_DIR/data" --config "$CUR/kv-count.toml"
check_contains 'afterFlushKVs count=100,' $TEST_DIR/lightning.log
check_not_contains 'afterFlushKVs count=20,' $TEST_DIR/lightning.log
check_contains 'send-kv-pairs\":20,' $TEST_DIR/lightning.log
check_contains 'send-kv-size\":16384,' $TEST_DIR/lightning.log

#
# test send-kv-size
#
rm -rf $TEST_DIR/data/test.t.0.csv
run_sql "truncate table test.t;"
set +x
for i in {1..5}; do
  echo "abcdefghijklmnopqrstuvwxyz0123456789" >>"$TEST_DIR/data/test.t.0.csv"
done
set -x

rm -rf $TEST_DIR/lightning.log
export GO_FAILPOINTS="github.com/pingcap/tidb/pkg/lightning/backend/local/afterFlushKVs=return(true)"
run_lightning --backend local -d "$TEST_DIR/data" --config "$CUR/kv-size.toml"
# each kv is 64b, so each kv is a batch
check_contains 'afterFlushKVs count=1,' $TEST_DIR/lightning.log
check_not_contains 'afterFlushKVs count=20,' $TEST_DIR/lightning.log
check_contains 'send-kv-pairs\":32768,' $TEST_DIR/lightning.log
check_contains 'send-kv-size\":10,' $TEST_DIR/lightning.log
