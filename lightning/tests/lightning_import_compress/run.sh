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

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

export GO_FAILPOINTS="github.com/pingcap/tidb/pkg/lightning/backend/local/LoggingImportBytes=return"

mkdir -p "$TEST_DIR/data"

cat <<EOF >"$TEST_DIR/data/test-schema-create.sql"
CREATE DATABASE test;
EOF
cat <<EOF >"$TEST_DIR/data/test.t-schema.sql"
CREATE TABLE test.t (id int primary key, a int, b int, c int);
EOF

# Generate 200k rows. Total size is about 5MiB.
for i in {1..200000}; do
  echo "$i,$i,$i,$i" >>"$TEST_DIR/data/test.t.0.csv"
done

LOG_FILE1="$TEST_DIR/lightning-import-compress1.log"
LOG_FILE2="$TEST_DIR/lightning-import-compress2.log"
LOG_FILE3="$TEST_DIR/lightning-import-compress3.log"

run_lightning --backend local -d "$TEST_DIR/data" --config "$CUR/config.toml" --log-file "$LOG_FILE1" -L debug
run_sql 'DROP DATABASE test;'
run_lightning --backend local -d "$TEST_DIR/data" --config "$CUR/config_gz.toml" --log-file "$LOG_FILE2" -L debug
run_sql 'DROP DATABASE test;'
run_lightning --backend local -d "$TEST_DIR/data" --config "$CUR/config_gzip.toml" --log-file "$LOG_FILE3" -L debug

uncompress=$(grep "import write" $LOG_FILE1 |
  grep -Eo "bytes=[0-9]+" | sed 's/bytes=//g' | awk '{sum+=$1} END {print sum}')
gzip=$(grep "import write" $LOG_FILE2 |
  grep -Eo "bytes=[0-9]+" | sed 's/bytes=//g' | awk '{sum+=$1} END {print sum}')
gz=$(grep "import write" $LOG_FILE3 |
  grep -Eo "bytes=[0-9]+" | sed 's/bytes=//g' | awk '{sum+=$1} END {print sum}')

echo "uncompress: ${uncompress}, gzip: ${gzip}, gz: ${gz}"
if [ "$uncompress" -le "$gzip" ] || [ "$uncompress" -le "$gz" ]; then
  echo "compress is not working"
  exit 1
fi
