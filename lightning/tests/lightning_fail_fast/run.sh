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

export GO_FAILPOINTS='github.com/pingcap/tidb/lightning/pkg/importer/SlowDownWriteRows=sleep(100);github.com/pingcap/tidb/lightning/pkg/importer/SetMinDeliverBytes=return(1)'

for CFG in chunk engine; do
  rm -f "$TEST_DIR/lightning-tidb.log"
  run_sql 'DROP DATABASE IF EXISTS fail_fast;'

  ! run_lightning --backend tidb --enable-checkpoint=0 --log-file "$TEST_DIR/lightning-tidb.log" --config "$CUR/$CFG.toml"
  [ $? -eq 0 ]

  tail -n 10 $TEST_DIR/lightning-tidb.log | grep "ERROR" | tail -n 1 | grep -Fq "Error 1062 (23000): Duplicate entry '1-1' for key 'tb.uq'"

  check_not_contains "restore file completed" $TEST_DIR/lightning-tidb.log

  check_not_contains "restore engine completed" $TEST_DIR/lightning-tidb.log
done
