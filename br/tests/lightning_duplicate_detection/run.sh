#!/bin/bash
#
# Copyright 2021 PingCAP, Inc.
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

LOG_FILE1="$TEST_DIR/lightning-duplicate-detection1.log"
LOG_FILE2="$TEST_DIR/lightning-duplicate-detection2.log"

# let lightning run a bit slow to avoid some table in the first lightning finish too fast.
export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/SlowDownImport=sleep(250)"

run_lightning --backend local --sorted-kv-dir "$TEST_DIR/lightning_duplicate_detection.sorted1" \
  --enable-checkpoint=1 --log-file "$LOG_FILE1" --config "$CUR/config1.toml" &
run_lightning --backend local --sorted-kv-dir "$TEST_DIR/lightning_duplicate_detection.sorted2" \
  --enable-checkpoint=1 --log-file "$LOG_FILE2" --config "$CUR/config2.toml" &

wait

verify_detected_rows() {
  table=$1

  mapfile -t rows < <(grep "insert" $CUR/data/dup_detect."${table}".*.sql |
    sed 's/^.*(//' | sed 's/).*$//' | sed "s/'//g" | sed 's/, */,/g')
  set +x
  n=${#rows[@]}
  expect_rows=()
  for ((i = 0; i < n; i++)); do
    for ((j = i + 1; j < n; j++)); do
      pk1=$(echo "${rows[i]}" | awk -F',' '{print $1}')
      pk2=$(echo "${rows[j]}" | awk -F',' '{print $1}')
      uk1=$(echo "${rows[i]}" | awk -F',' '{print $2}')
      uk2=$(echo "${rows[j]}" | awk -F',' '{print $2}')
      if [[ "$pk1" == "$pk2" || "$uk1" == "$uk2" ]]; then
        expect_rows+=("${rows[i]}" "${rows[j]}")
      fi
    done
  done
  mapfile -t expect_rows < <(for row in "${expect_rows[@]}"; do echo "$row"; done | sort | uniq)
  mapfile -t actual_rows < <(run_sql "SELECT row_data FROM lightning_task_info.conflict_error_v3 WHERE table_name = \"\`dup_detect\`.\`${table}\`\"" |
    grep "row_data:" | sed 's/^.*(//' | sed 's/).*$//' | sed 's/"//g' | sed 's/, */,/g' | sort | uniq)
  equal=0
  if [ "${#actual_rows[@]}" = "${#expect_rows[@]}" ]; then
    equal=1
    n="${#actual_rows[@]}"
    for ((i = 0; i < n; i++)); do
      if [ "${#actual_rows[i]}" != "${#expect_rows[i]}" ]; then
        equal=0
        break
      fi
    done
  fi
  set -x
  if [ "$equal" = "0" ]; then
    echo "verify detected rows of ${table} fail, expect: ${expect_rows[*]}, actual: ${actual_rows[*]}"
    exit 1
  fi
}

## a. Primary key conflict in table `ta`. There are 10 pairs of conflicts in each file and 5 pairs of conflicts in both files.
verify_detected_rows "ta"

## b. Unique key conflict in table `tb`. There are 10 pairs of conflicts in each file and 5 pairs of conflicts in both files.
verify_detected_rows "tb"

## c. Primary key conflict in table `tc`. There are 10 rows with the same key in each file and 10 rows with the same key in both files.
verify_detected_rows "tc"

## d. Unique key conflict in table `td`. There are 10 rows with the same key in each file and 10 rows with the same key in both files.
verify_detected_rows "td"

## e. Identical rows in table `te`. There are 10 identical rows in each file and 10 identical rows in both files.
verify_detected_rows "te"

## f. No conflicts in table `tf`.
verify_detected_rows "tf"

## g. Primary key conflict in partitioned table `tg`.
verify_detected_rows "tg"
