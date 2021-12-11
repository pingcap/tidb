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

# Check that error summary are written at the bottom of import.
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_error_summary;'

# The easiest way to induce error is to prepopulate the target table with conflicting content.
run_sql 'CREATE DATABASE IF NOT EXISTS error_summary;'
run_sql 'DROP TABLE IF EXISTS error_summary.a;'
run_sql 'DROP TABLE IF EXISTS error_summary.c;'
run_sql 'CREATE TABLE error_summary.a (id INT NOT NULL PRIMARY KEY, k INT NOT NULL);'
run_sql 'CREATE TABLE error_summary.c (id INT NOT NULL PRIMARY KEY, k INT NOT NULL);'
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/lightning/restore/InitializeCheckpointExit=return(true)"
run_lightning --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-error-summary.log"
run_sql 'INSERT INTO error_summary.a VALUES (2, 4), (6, 8);'
run_sql 'INSERT INTO error_summary.c VALUES (3, 9), (27, 81);'

set +e
export GO_FAILPOINTS=""
run_lightning --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-error-summary.log"
ERRORCODE=$?
set -e

[ "$ERRORCODE" -ne 0 ]

# Verify that table `b` is indeed imported
run_sql 'SELECT sum(id), sum(k) FROM error_summary.b'
check_contains 'sum(id): 28'
check_contains 'sum(k): 32'

# Verify the log contains the expected messages at the last few lines
tail -20 "$TEST_DIR/lightning-error-summary.log" > "$TEST_DIR/lightning-error-summary.tail"
grep -Fq '["tables failed to be imported"] [count=2]' "$TEST_DIR/lightning-error-summary.tail"
grep -Fq '[-] [table=`error_summary`.`a`] [status=checksum] [error="checksum mismatched' "$TEST_DIR/lightning-error-summary.tail"
grep -Fq '[-] [table=`error_summary`.`c`] [status=checksum] [error="checksum mismatched' "$TEST_DIR/lightning-error-summary.tail"
! grep -Fq '[-] [table=`error_summary`.`b`] [status=checksum] [error="checksum mismatched' "$TEST_DIR/lightning-error-summary.tail"

# Now check the error log when the checkpoint is not cleaned.

set +e
# put stdout to log for next grep
run_lightning --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-error-summary.log" >> "$TEST_DIR/lightning-error-summary.log"
ERRORCODE=$?
set -e

[ "$ERRORCODE" -ne 0 ]

tail -100 "$TEST_DIR/lightning-error-summary.log" > "$TEST_DIR/lightning-error-summary.tail"
grep -Fq 'TiDB Lightning has failed last time. To prevent data loss, this run will stop now' "$TEST_DIR/lightning-error-summary.tail"
grep -Fq './tidb-lightning-ctl --checkpoint-error-destroy='"'"'`error_summary`.`a`'"'"' --config=...' "$TEST_DIR/lightning-error-summary.tail"
grep -Fq './tidb-lightning-ctl --checkpoint-error-destroy='"'"'`error_summary`.`c`'"'"' --config=...' "$TEST_DIR/lightning-error-summary.tail"
! grep -Fq './tidb-lightning-ctl --checkpoint-error-destroy='"'"'`error_summary`.`b`'"'"' --config=...' "$TEST_DIR/lightning-error-summary.tail"
grep -Fq 'checkpoint-error-destroy=all --config=...` to start from scratch' "$TEST_DIR/lightning-error-summary.tail"
grep -Fq 'For details of this failure, read the log file' "$TEST_DIR/lightning-error-summary.tail"
grep -Fq 'PREVIOUS run' "$TEST_DIR/lightning-error-summary.tail"
