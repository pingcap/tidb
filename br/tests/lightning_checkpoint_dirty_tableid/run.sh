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
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux

# clean env
rm -f "$TEST_DIR/lightning-checkpoint-dirty-tableid.log"
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint'

export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/lightning/restore/InitializeCheckpointExit=return(true)"
run_lightning --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" --config "tests/$TEST_NAME/mysql.toml" -d "tests/$TEST_NAME/data"

run_sql 'DROP DATABASE IF EXISTS cpdt'

export GO_FAILPOINTS=""
set +e
run_lightning --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" --config "tests/$TEST_NAME/mysql.toml" -d "tests/$TEST_NAME/data"
set -e

ILLEGAL_CP_COUNT=$(grep "TiDB Lightning has detected tables with illegal checkpoints. To prevent data mismatch, this run will stop now. Please remove these checkpoints first" "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" | wc -l)
TABLE_SUGGEST=$(grep "./tidb-lightning-ctl --checkpoint-remove=" "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" | wc -l)

[ $ILLEGAL_CP_COUNT -eq 1 ]
[ $TABLE_SUGGEST -eq 2 ]

# Try again with the file checkpoints

# clean env
run_sql 'DROP DATABASE IF EXISTS cpdt'
rm -f "$TEST_DIR/lightning-checkpoint-dirty-tableid.log"
rm -f "/tmp/tidb_lightning_checkpoint.pb"

export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/lightning/restore/InitializeCheckpointExit=return(true)"
run_lightning --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" --config "tests/$TEST_NAME/file.toml" -d "tests/$TEST_NAME/data"

run_sql 'DROP DATABASE IF EXISTS cpdt'

export GO_FAILPOINTS=""
set +e
run_lightning --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" --config "tests/$TEST_NAME/file.toml" -d "tests/$TEST_NAME/data"
set -e

ILLEGAL_CP_COUNT=$(grep "TiDB Lightning has detected tables with illegal checkpoints. To prevent data mismatch, this run will stop now. Please remove these checkpoints first" "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" | wc -l)
TABLE_SUGGEST=$(grep "./tidb-lightning-ctl --checkpoint-remove=" "$TEST_DIR/lightning-checkpoint-dirty-tableid.log" | wc -l)

[ $ILLEGAL_CP_COUNT -eq 1 ]
[ $TABLE_SUGGEST -eq 2 ]
