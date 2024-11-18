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

check_cluster_version 5 2 0 'duplicate detection' || exit 0

LOG_FILE1="$TEST_DIR/lightning-duplicate-resolution1.log"
LOG_FILE2="$TEST_DIR/lightning-duplicate-resolution2.log"

# let lightning run a bit slow to avoid some table in the first lightning finish too fast.
export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/SlowDownCheckDupe=return(10)"
run_lightning --backend local --sorted-kv-dir "$TEST_DIR/lightning_duplicate_resolution_incremental.sorted1" \
  --enable-checkpoint=1 --log-file "$LOG_FILE1" --config "$CUR/config1.toml" &

counter=0
while [ $counter -lt 10 ]; do
    if grep -Fq "start to sleep several seconds before checking other dupe" "$LOG_FILE1"; then
        echo "lightning 1 already starts waiting for dupe"
        break
    fi
    ((counter += 1))
    echo "waiting for lightning 1 starts"
    sleep 1
done

if [ $counter -ge 10 ]; then
  echo "fail to wait for lightning 1 starts"
  exit 1
fi

run_lightning --backend local --sorted-kv-dir "$TEST_DIR/lightning_duplicate_resolution_incremental.sorted2" \
  --enable-checkpoint=1 --log-file "$LOG_FILE2" --config "$CUR/config2.toml" &

wait

export GO_FAILPOINTS=""

# Ensure table is consistent.
run_sql 'admin check table dup_resolve_detect.ta'

# Check data correctness
run_sql 'select count(*), sum(id) from dup_resolve_detect.ta where id < 100'
check_contains 'count(*): 15'
check_contains 'sum(id): 120'

run_sql 'select count(*), sum(id) from dup_resolve_detect.ta where id > 100'
check_contains 'count(*): 16'
check_contains 'sum(id): 1896'
