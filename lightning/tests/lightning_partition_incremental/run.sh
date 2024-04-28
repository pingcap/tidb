#!/bin/bash
#
# Copyright 2022 PingCAP, Inc.
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

run_lightning --backend=local -d "$CUR/data"

LOG_FILE1="$TEST_DIR/lightning_partition_incremental1.log"
LOG_FILE2="$TEST_DIR/lightning_partition_incremental2.log"

run_lightning --log-file "$LOG_FILE1" --config "$CUR/config1.toml" -d "$CUR/data1" &
pid1="$!"
run_lightning --log-file "$LOG_FILE2" --config "$CUR/config2.toml" -d "$CUR/data2" &
pid2="$!"

# Must wait for pid explicitly. Otherwise, we can't detect whether the process is exited with error.
wait "$pid1" "$pid2"

grep -F 'checksum pass' "$LOG_FILE1" | grep -Fq 'auto_rowid'
grep -F 'checksum pass' "$LOG_FILE1" | grep -Fq 'non_pk_auto_inc'

run_sql 'admin check table incr.auto_rowid'
run_sql 'admin check table incr.non_pk_auto_inc'

run_sql 'select count(*) from incr.auto_rowid'
check_contains 'count(*): 60'

run_sql 'select count(distinct _tidb_rowid) from incr.auto_rowid'
check_contains 'count(distinct _tidb_rowid): 60'

run_sql 'select count(*) from incr.non_pk_auto_inc'
check_contains 'count(*): 60'

run_sql 'select count(distinct _tidb_rowid) from incr.non_pk_auto_inc'
check_contains 'count(distinct _tidb_rowid): 60'
