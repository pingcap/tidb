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

//! Source-backed tests for locked-statistics table payloads.

use std::collections::BTreeMap;

use tidb_stats::StatsLockTable;

#[test]
fn source_stats_lock_table_preserves_full_and_partition_names() {
    let mut partitions = BTreeMap::new();
    partitions.insert(4, "p1".to_owned());
    let table = StatsLockTable::new("test.t1", Some(partitions.clone()));

    assert_eq!(table.full_name, "test.t1");
    assert_eq!(table.partition_info, Some(partitions));
}

#[test]
fn source_stats_lock_table_preserves_nil_and_empty_partition_maps() {
    let nil_map = StatsLockTable::new("test.t2", None);
    let empty_map = StatsLockTable::new("test.t2", Some(BTreeMap::new()));

    assert_ne!(nil_map, empty_map);
    assert!(nil_map.partition_info.is_none());
    assert!(empty_map
        .partition_info
        .as_ref()
        .is_some_and(BTreeMap::is_empty));
}
