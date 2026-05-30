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

# Create both partitioned tables and pre-populate p_b so the tables are non-empty.
# Lightning must target only p_a even when p_b already has data.
run_sql 'DROP DATABASE IF EXISTS part_conc'
run_sql 'CREATE DATABASE part_conc'
run_sql "CREATE TABLE part_conc.orders (
    region varchar(32) NOT NULL,
    id     int         NOT NULL,
    amount int         NOT NULL
) PARTITION BY LIST COLUMNS (region) (
    PARTITION p_a VALUES IN ('alpha'),
    PARTITION p_b VALUES IN ('beta')
)"
run_sql "CREATE TABLE part_conc.events (
    region varchar(32) NOT NULL,
    ts     bigint      NOT NULL,
    msg    varchar(64) NOT NULL
) PARTITION BY LIST COLUMNS (region) (
    PARTITION p_a VALUES IN ('alpha'),
    PARTITION p_b VALUES IN ('beta')
)"

# Pre-populate p_b on both tables to prove Lightning does not touch it
run_sql "INSERT INTO part_conc.orders VALUES ('beta', 99, 999)"
run_sql "INSERT INTO part_conc.events VALUES ('beta', 9999, 'existing')"

# Run Lightning: single run, both tables, scoped to p_a
run_lightning \
    --config "$CUR/config.toml" \
    -d "$CUR/data" \
    --sorted-kv-dir "$TEST_DIR/lightning_partition_ingest_concurrent.sorted" \
    --log-file "$TEST_DIR/lightning-partition-ingest-concurrent.log"


# orders: p_a should have 3 rows from CSV; p_b must be untouched
run_sql "SELECT count(*) FROM part_conc.orders PARTITION (p_a)"
check_contains "count(*): 3"
run_sql "SELECT count(*) FROM part_conc.orders PARTITION (p_b)"
check_contains "count(*): 1"
run_sql "SELECT amount FROM part_conc.orders PARTITION (p_b)"
check_contains "amount: 999"

# events: p_a should have 2 rows from CSV; p_b must be untouched
run_sql "SELECT count(*) FROM part_conc.events PARTITION (p_a)"
check_contains "count(*): 2"
run_sql "SELECT count(*) FROM part_conc.events PARTITION (p_b)"
check_contains "count(*): 1"
run_sql "SELECT msg FROM part_conc.events PARTITION (p_b)"
check_contains "msg: existing"
