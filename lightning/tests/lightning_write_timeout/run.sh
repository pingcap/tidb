#!/bin/bash
#
# Copyright 2025 PingCAP, Inc.
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
CREATE TABLE test.t (
  id int,
  a int,
  b int,
  c int
);
EOF

# Generate 200k rows. Total size is about 5MiB.
set +x
for i in {1..200000}; do
  echo "$i,$i,$i,$i" >>"$TEST_DIR/data/test.t.0.csv"
done
set -x

export GO_FAILPOINTS="github.com/pingcap/tidb/pkg/lightning/backend/local/shortWaitNTimeout=100*return(1)" 

run_lightning --backend local -d "$TEST_DIR/data" --config "$CUR/config.toml"
check_lightning_log_contains 'Experiencing a wait timeout while writing to tikv'
