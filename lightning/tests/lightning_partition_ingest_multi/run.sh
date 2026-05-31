#!/bin/bash
#
# Copyright 2026 PingCAP, Inc.
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

check_cluster_version 4 0 0 'local backend' || exit 0

# Table has 3 partitions: p_a, p_b, p_c
# CSV contains rows for p_a (alpha x3) and p_b (beta x2)
# target-partition = ["p_a", "p_b"] — Lightning scopes ingest, checksum,
# and ANALYZE to both named partitions; p_c must remain empty throughout.
run_sql 'DROP DATABASE IF EXISTS part_multi'

run_lightning \
    --config "$CUR/config.toml" \
    -d "$CUR/data" \
    --sorted-kv-dir "$TEST_DIR/lightning_partition_ingest_multi.sorted"

# p_a must have 3 rows (alpha)
run_sql "SELECT count(*) FROM part_multi.t PARTITION (p_a)"
check_contains "count(*): 3"

# p_b must have 2 rows (beta)
run_sql "SELECT count(*) FROM part_multi.t PARTITION (p_b)"
check_contains "count(*): 2"

# p_c must be empty — Lightning must not have touched it
run_sql "SELECT count(*) FROM part_multi.t PARTITION (p_c)"
check_contains "count(*): 0"

# Total: 5 rows across p_a and p_b
run_sql "SELECT count(*) FROM part_multi.t"
check_contains "count(*): 5"

# Spot-check values
run_sql "SELECT val FROM part_multi.t PARTITION (p_a) ORDER BY val"
check_contains "val: 1"
check_contains "val: 2"
check_contains "val: 3"

run_sql "SELECT val FROM part_multi.t PARTITION (p_b) ORDER BY val"
check_contains "val: 4"
check_contains "val: 5"
