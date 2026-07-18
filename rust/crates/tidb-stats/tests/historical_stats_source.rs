// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-backed tests for historical-statistics version selection.

use tidb_stats::historical_stats_version;

#[test]
fn source_historical_stats_uses_table_version_without_partitions() {
    assert_eq!(historical_stats_version(42, &[]), 42);
}

#[test]
fn source_historical_stats_uses_maximum_partition_version() {
    assert_eq!(historical_stats_version(99, &[10, 20, 15]), 20);
    assert_eq!(historical_stats_version(0, &[0, 0]), 0);
}

#[test]
fn source_historical_stats_partition_branch_ignores_table_version() {
    assert_eq!(historical_stats_version(u64::MAX, &[1]), 1);
}
