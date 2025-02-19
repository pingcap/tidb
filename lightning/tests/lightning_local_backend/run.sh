#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
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
check_cluster_version 4 0 0 'local backend' || exit 0

ENGINE_COUNT=6

res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"

# Test check table contains data
rm -f "/tmp/tidb_lightning_checkpoint_local_backend_test.pb"
rm -rf $TEST_DIR/lightning.log
run_sql 'DROP DATABASE IF EXISTS cpeng;'
run_sql 'CREATE DATABASE cpeng;'
run_sql 'CREATE TABLE cpeng.a (c int);'
run_sql 'CREATE TABLE cpeng.b (c int);'
run_sql "INSERT INTO cpeng.a values (1), (2);"
run_sql "INSERT INTO cpeng.b values (3);"
! run_lightning --backend local --enable-checkpoint=0
grep -Fq 'table(s) [`cpeng`.`a`, `cpeng`.`b`] are not empty' $TEST_DIR/lightning.log


# First, verify that inject with not leader error is fine.
export GO_FAILPOINTS='github.com/pingcap/tidb/pkg/lightning/backend/local/FailIngestMeta=1*return("notleader");github.com/pingcap/tidb/br/pkg/restore/split/failToSplit=5*return("");github.com/pingcap/tidb/pkg/lightning/backend/local/failToSplit=5*return("")'
rm -f "$TEST_DIR/lightning-local.log"
run_sql 'DROP DATABASE IF EXISTS cpeng;'
run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "$CUR/config.toml" -L debug
grep -q "retryable error" "$TEST_DIR/lightning-local.log"

# Check that everything is correctly imported
run_sql 'SELECT count(*), sum(c) FROM cpeng.a'
check_contains 'count(*): 4'
check_contains 'sum(c): 10'

run_sql 'SELECT count(*), sum(c) FROM cpeng.b'
check_contains 'count(*): 4'
check_contains 'sum(c): 46'

# Now, verify it works with epoch not match as well.
run_sql 'DROP DATABASE cpeng;'
rm -f "/tmp/tidb_lightning_checkpoint_local_backend_test.pb"

export GO_FAILPOINTS='github.com/pingcap/tidb/pkg/lightning/backend/local/FailIngestMeta=2*return("epochnotmatch");github.com/pingcap/tidb/pkg/lightning/backend/local/NoLeader=1*return()'

run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "$CUR/config.toml"

run_sql 'SELECT count(*), sum(c) FROM cpeng.a'
check_contains 'count(*): 4'
check_contains 'sum(c): 10'

run_sql 'SELECT count(*), sum(c) FROM cpeng.b'
check_contains 'count(*): 4'
check_contains 'sum(c): 46'


# Now, verify it works with checkpoints as well.
run_sql 'DROP DATABASE cpeng;'
rm -f "/tmp/tidb_lightning_checkpoint_local_backend_test.pb"

set +e
export GO_FAILPOINTS='github.com/pingcap/tidb/lightning/pkg/importer/FailIfStatusBecomes=return(90);'
for i in $(seq "$ENGINE_COUNT"); do
    echo "******** Importing Table Now (step $i/$ENGINE_COUNT) ********"
    run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "$CUR/config.toml"
    [ $? -ne 0 ] || exit 1
done
set -e

export GO_FAILPOINTS=''
echo "******** Verify checkpoint no-op ********"
run_lightning --backend local --enable-checkpoint=1 --config "$CUR/config.toml" --log-file $res_file -L debug
check_not_contains "failed to set system var"
check_not_contains "unknown system var"
check_contains "skip read-only variable"
check_contains "lc_time_names"

run_sql 'SELECT count(*), sum(c) FROM cpeng.a'
check_contains 'count(*): 4'
check_contains 'sum(c): 10'

run_sql 'SELECT count(*), sum(c) FROM cpeng.b'
check_contains 'count(*): 4'
check_contains 'sum(c): 46'

# Verify GetLocalStoringTables works
# failpoint works for per table not task, so we limit this test to task that allow one table
for ckpt in mysql file; do
  run_sql 'DROP DATABASE IF EXISTS cpeng;'
  run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_local_backend_test'
  rm -f "/tmp/tidb_lightning_checkpoint_local_backend_test.pb"

  # before chunk pos is updated, local files could handle lost
  set +e
  export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/FailAfterWriteRows=return"
  run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "$CUR/$ckpt.toml"
  set -e
  run_lightning_ctl --check-local-storage \
    --backend local \
    --enable-checkpoint=1 \
    --config=$CUR/$ckpt.toml >$TEST_DIR/lightning_ctl.output 2>&1
  grep -Fq "No table has lost intermediate files according to given config" $TEST_DIR/lightning_ctl.output

  # when position of chunk file doesn't equal to offset, intermediate file should exist
  run_sql 'DROP DATABASE IF EXISTS cpeng;'
  run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_local_backend_test'
  rm -f "/tmp/tidb_lightning_checkpoint_local_backend_test.pb"
  set +e
  export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/LocalBackendSaveCheckpoint=return;github.com/pingcap/tidb/lightning/pkg/importer/FailIfImportedChunk=return"
  run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "$CUR/$ckpt.toml"
  set -e
  run_lightning_ctl --check-local-storage \
    --backend local \
    --enable-checkpoint=1 \
    --config=$CUR/$ckpt.toml >$TEST_DIR/lightning_ctl.output 2>&1
  grep -Eq "These tables are missing intermediate files: \[.+\]" $TEST_DIR/lightning_ctl.output
  # don't distinguish whole sort-kv directory missing and table's directory missing for now
  ls -lA $TEST_DIR/$TEST_NAME.sorted

  # after index engine is imported, local file could handle lost
  set +e
  export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/FailIfIndexEngineImported=return(1)"
  run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-local.log" --config "$CUR/$ckpt.toml"
  set -e
  run_lightning_ctl --check-local-storage \
    --backend local \
    --enable-checkpoint=1 \
    --config=$CUR/$ckpt.toml >$TEST_DIR/lightning_ctl.output 2>&1
  grep -Fq "No table has lost intermediate files according to given config" $TEST_DIR/lightning_ctl.output
done
run_lightning_ctl \
  --backend local \
  --enable-checkpoint=1 \
  --checkpoint-dump $TEST_DIR \
  --config=$CUR/mysql.toml >$TEST_DIR/lightning_ctl.output 2>&1
# --checkpoint-dump option in lightning-ctl will dump checkpoint table as csv files to $TEST_DIR
for file in engines.csv chunks.csv tables.csv; do
  if [ ! -f "$TEST_DIR/$file" ]; then
    echo "$file does not exist in $TEST_DIR"
    exit 1
  fi
done
rm -r $TEST_DIR/$TEST_NAME.sorted
