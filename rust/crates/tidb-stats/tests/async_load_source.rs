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

//! Source-backed tests for pending histogram-load metadata.

use tidb_stats::{NeededStatsMap, TableItemId, SHARD_COUNT};

fn column(table_id: i64, id: i64) -> TableItemId {
    TableItemId {
        table_id,
        id,
        is_index: false,
        is_sync_load_failed: false,
    }
}

fn index(table_id: i64, id: i64) -> TableItemId {
    TableItemId {
        table_id,
        id,
        is_index: true,
        is_sync_load_failed: false,
    }
}

#[test]
fn source_map_initializes_all_shards_and_tracks_length() {
    assert_eq!(SHARD_COUNT, 128);
    let map = NeededStatsMap::new();
    assert!(map.is_empty());
    assert_eq!(map.len(), 0);
}

#[test]
fn source_insert_keeps_full_load_when_partial_request_follows() {
    let map = NeededStatsMap::new();
    let item = column(42, 7);
    map.insert(item, true);
    map.insert(item, false);

    assert_eq!(map.len(), 1);
    assert_eq!(
        map.all_items(),
        vec![tidb_stats::StatsLoadItem {
            table_item_id: item,
            full_load: true,
        }]
    );
}

#[test]
fn source_insert_upgrades_partial_request_to_full_load() {
    let map = NeededStatsMap::new();
    let item = column(42, 7);
    map.insert(item, false);
    map.insert(item, true);
    assert!(map.all_items()[0].full_load);
}

#[test]
fn source_map_keeps_column_and_index_keys_distinct() {
    let map = NeededStatsMap::new();
    map.insert(column(42, 7), false);
    map.insert(index(42, 7), true);
    assert_eq!(map.len(), 2);
    assert!(map
        .all_items()
        .iter()
        .any(|item| !item.table_item_id.is_index));
    assert!(map
        .all_items()
        .iter()
        .any(|item| item.table_item_id.is_index));
}

#[test]
fn source_delete_removes_only_the_requested_item() {
    let map = NeededStatsMap::new();
    let kept = column(42, 7);
    let deleted = index(42, 7);
    map.insert(kept, false);
    map.insert(deleted, true);
    map.delete(deleted);

    assert_eq!(map.len(), 1);
    assert_eq!(map.all_items()[0].table_item_id, kept);
    map.delete(deleted);
    assert_eq!(map.len(), 1);
}
