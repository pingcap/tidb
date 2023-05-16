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

mkdir -p "$TEST_DIR/data"

run_sql "DROP DATABASE IF EXISTS test;"
run_sql "DROP TABLE IF EXISTS test.t;"

cat <<EOF >"$TEST_DIR/data/test-schema-create.sql"
CREATE DATABASE test;
EOF
cat <<EOF >"$TEST_DIR/data/test.t-schema.sql"
CREATE TABLE test.t (a varchar(1024));
EOF

# Generate 5 rows.
set +x
for i in {1..5}; do
  echo "$i" >>"$TEST_DIR/data/test.t.0.csv"
done
set -x

rm -rf $TEST_DIR/lightning.log
start=$(date +%s)
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/lightning/backend/local/waitAfterWrite=sleep(1000)"
run_lightning --backend local -d "$TEST_DIR/data" --config "tests/$TEST_NAME/kv-count.toml"
end=$(date +%s)
take=$((end - start))
# there are 5 kvs, should take more than 5s
if [ $take -lt 5 ]; then
  echo "send-kv-pairs test failed."
  exit 1
fi
check_contains 'send-kv-pairs\":1,' $TEST_DIR/lightning.log
check_contains 'send-kv-size\":1,' $TEST_DIR/lightning.log
