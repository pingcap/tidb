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

check_cluster_version 4 0 0 'local backend' || exit 0

# Create partitioned table and pre-populate p_b so the table is non-empty
run_sql 'DROP DATABASE IF EXISTS part_ingest'
run_sql 'CREATE DATABASE part_ingest'
run_sql "CREATE TABLE part_ingest.t (
    region varchar(32) NOT NULL,
    val    int         NOT NULL
) PARTITION BY LIST COLUMNS (region) (
    PARTITION p_a VALUES IN ('alpha'),
    PARTITION p_b VALUES IN ('beta')
)"
run_sql "INSERT INTO part_ingest.t VALUES ('beta', 99)"

# Verify p_b has data before the ingest
run_sql "SELECT count(*) FROM part_ingest.t PARTITION (p_b)"
check_contains "count(*): 1"

# Run Lightning targeting only p_a — should succeed even though table is non-empty
run_lightning --backend local

# p_a should now have the 3 CSV rows
run_sql "SELECT count(*) FROM part_ingest.t PARTITION (p_a)"
check_contains "count(*): 3"

# p_b must be untouched
run_sql "SELECT count(*) FROM part_ingest.t PARTITION (p_b)"
check_contains "count(*): 1"
run_sql "SELECT val FROM part_ingest.t PARTITION (p_b)"
check_contains "val: 99"

# Total table count: 3 (p_a) + 1 (p_b) = 4
run_sql "SELECT count(*) FROM part_ingest.t"
check_contains "count(*): 4"
