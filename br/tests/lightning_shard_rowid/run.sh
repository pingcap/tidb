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

set -eu

run_sql 'show variables like "%tidb_enable_clustered_index%";'
is_on=$(grep 'Value: \S+' "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk '{print $2}')

# disable clustered index
if [ "$is_on" = "ON" ] || [ "$is_on" = "1" ]; then
  run_sql "set @@global.tidb_enable_clustered_index = 'OFF'";
  sleep 2
fi

check_cluster_version 4 0 0 'local backend'

run_sql 'DROP DATABASE IF EXISTS shard_rowid;'
run_lightning

run_sql "SELECT count(*) from shard_rowid.shr"
check_contains "count(*): 16"

run_sql "SELECT count(distinct _tidb_rowid & b'000001111111111111111111111111111111111111111111111111111111111') as count FROM shard_rowid.shr"
check_contains "count: 16"

# since we use random to generate the shard bits, with 16 record, there maybe less than 8 distinct value,
# but it should be bigger than 4
run_sql 'SELECT count between 5 and 8 as correct from (SELECT count(distinct _tidb_rowid >> 60) as count from shard_rowid.shr) _'
check_contains "correct: 1"

if [ -n "$is_on" ]; then
  run_sql "set @@global.tidb_enable_clustered_index = '$is_on'";
  sleep 2
fi
