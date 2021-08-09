#!/bin/sh
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
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux

check_cluster_version 4 0 0 'local backend' || exit 0

LOG_FILE1="$TEST_DIR/lightning-duplicate-detection1.log"
LOG_FILE2="$TEST_DIR/lightning-duplicate-detection2.log"

run_lightning --backend local --sorted-kv-dir "$TEST_DIR/lightning_duplicate_detection.sorted1" \
  --enable-checkpoint=1 --log-file "$LOG_FILE1" --config "tests/$TEST_NAME/config1.toml" && exit 1 &
run_lightning --backend local --sorted-kv-dir "$TEST_DIR/lightning_duplicate_detection.sorted2" \
  --enable-checkpoint=1 --log-file "$LOG_FILE2" --config "tests/$TEST_NAME/config2.toml" && exit 1 &

wait
## a. Primary key conflict in table `ta`. There are 10 pairs of conflicts in each file and 5 pairs of conflicts in both files.
#grep -q "restore table \`dup_detect\`.\`ta\` failed: .*duplicate detected" "$LOG_FILE"
#
## b. Unique key conflict in table `tb`. There are 10 pairs of conflicts in each file and 5 pairs of conflicts in both files.
#grep -q "restore table \`dup_detect\`.\`tb\` failed: .*duplicate detected" "$LOG_FILE"
#
## c. Primary key conflict in table `tc`. There are 10 rows with the same key in each file and 10 rows with the same key in both files.
#grep -q "restore table \`dup_detect\`.\`tc\` failed: .*duplicate detected" "$LOG_FILE"
#
## d. Unique key conflict in table `td`. There are 10 rows with the same key in each file and 10 rows with the same key in both files.
#grep -q "restore table \`dup_detect\`.\`td\` failed: .*duplicate detected" "$LOG_FILE"
#
## e. Identical rows in table `te`. There are 10 identical rows in each file and 10 identical rows in both files.
#grep -q "restore table \`dup_detect\`.\`te\` failed: .*duplicate detected" "$LOG_FILE"
#
## f. No conflicts in table `tf`.
#grep -Eq "restore table completed.*table=\`dup_detect\`.\`tf\`" "$LOG_FILE"
