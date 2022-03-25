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

export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/lightning/restore/SlowDownWriteRows=sleep(100);github.com/pingcap/tidb/br/pkg/lightning/restore/SetMinDeliverBytes=return(1)'

for CFG in chunk engine; do
  rm -f "$TEST_DIR/lightning-tidb.log"
  run_sql 'DROP DATABASE IF EXISTS fail_fast;'

  ! run_lightning --backend tidb --enable-checkpoint=0 --log-file "$TEST_DIR/lightning-tidb.log" --config "tests/$TEST_NAME/$CFG.toml"
  [ $? -eq 0 ]

  tail -n 10 $TEST_DIR/lightning-tidb.log | grep "ERROR" | tail -n 1 | grep -Fq "Error 1062: Duplicate entry '1-1' for key 'uq'"

  ! grep -Fq "restore file completed" $TEST_DIR/lightning-tidb.log
  [ $? -eq 0 ]

  ! grep -Fq "restore engine completed" $TEST_DIR/lightning-tidb.log
  [ $? -eq 0 ]
done
