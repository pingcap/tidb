#!/bin/sh
#
# Copyright 2019 PingCAP, Inc.
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
# clean env
rm -f "$TEST_DIR/lightning-checkpoint-dirty-tableid.log"
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint'

export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/InitializeCheckpointExit=return(true)"
run_lightning --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" --config "$CUR/mysql.toml" -d "$CUR/data"

run_sql 'DROP DATABASE IF EXISTS cpdt'

export GO_FAILPOINTS=""
set +e
# put stdout to log file for next grep
run_lightning --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" --config "$CUR/mysql.toml" -d "$CUR/data" >> "$TEST_DIR/lightning-checkpoint-dirty-tableid.log"
set -e

# some msg will split into two lines when put them into chart.
ILLEGAL_CP_COUNT=$(grep "TiDB Lightning has detected tables with illegal checkpoints. To prevent data loss, this run will stop now." "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" | wc -l)
TABLE_SUGGEST=$(grep "checkpoint-remove=" "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" | wc -l)

# we got same errors in three place:
# 1. run failed in step 2
# 2. the whole procedure failed
# 3. main
[ $ILLEGAL_CP_COUNT -eq 3 ]
[ $TABLE_SUGGEST -eq 3 ]

# Try again with the file checkpoints

# clean env
run_sql 'DROP DATABASE IF EXISTS cpdt'
rm -f "$TEST_DIR/lightning-checkpoint-dirty-tableid.log"
rm -f "/tmp/tidb_lightning_checkpoint.pb"

export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/InitializeCheckpointExit=return(true)"
run_lightning --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" --config "$CUR/file.toml" -d "$CUR/data"

run_sql 'DROP DATABASE IF EXISTS cpdt'

export GO_FAILPOINTS=""
set +e
# put stdout to log file for next grep
run_lightning --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" --config "$CUR/file.toml" -d "$CUR/data" >> "$TEST_DIR/lightning-checkpoint-dirty-tableid.log"
set -e

# some msg will split into two lines when put them into chart.
ILLEGAL_CP_COUNT=$(grep "TiDB Lightning has detected tables with illegal checkpoints. To prevent data loss, this run will stop now." "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" | wc -l)
TABLE_SUGGEST=$(grep "checkpoint-remove=" "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" | wc -l)

# we got same errors in three place:
# 1. run failed in step 2
# 2. the whole procedure failed
# 3. main
[ $ILLEGAL_CP_COUNT -eq 3 ]
[ $TABLE_SUGGEST -eq 3 ]
