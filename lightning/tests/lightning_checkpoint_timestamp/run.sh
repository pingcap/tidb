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

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

run_sql 'DROP DATABASE IF EXISTS cpts'
rm -f "$TEST_DIR"/cpts.pb*

export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/server/SetTaskID=return(1234567890);github.com/pingcap/tidb/lightning/pkg/importer/FailIfImportedChunk=return"

for i in $(seq 5); do
    echo "******** Importing Chunk Now (file step $i) ********"
    run_lightning --enable-checkpoint=1 2> /dev/null && break
    sleep 1
done

run_sql 'SELECT COUNT(ts) a, COUNT(DISTINCT ts) b FROM cpts.cpts;'
check_contains 'a: 98'
check_contains 'b: 1'

run_sql 'DROP DATABASE IF EXISTS cpts'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_timestamp'
run_sql 'DROP DATABASE IF EXISTS `tidb_lightning_checkpoint_timestamp.1234567890.bak`'

for i in $(seq 5); do
    echo "******** Importing Chunk Now (mysql step $i) ********"
    run_lightning --enable-checkpoint=1 --config "$CUR/mysql.toml" 2> /dev/null && break
    sleep 1
done

run_sql 'SELECT COUNT(ts) a, COUNT(DISTINCT ts) b FROM cpts.cpts;'
check_contains 'a: 98'
check_contains 'b: 1'

