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

//! Source-backed tests for sharded statistics-cache key metadata.

use tidb_stats::{StatsKeySetShards, KEY_SET_SHARD_COUNT};

#[test]
fn source_key_set_shards_route_and_aggregate_keys() {
    assert_eq!(KEY_SET_SHARD_COUNT, 256);
    let shards = StatsKeySetShards::new();
    shards.add_key_value(1, 10);
    shards.add_key_value(257, 20);
    shards.add_key_value(-1, 30);

    assert_eq!(shards.get(1), Some(10));
    assert_eq!(shards.get(257), Some(20));
    assert_eq!(shards.get(-1), Some(30));
    assert_eq!(shards.len(), 3);
    let mut keys = shards.keys();
    keys.sort_unstable();
    assert_eq!(keys, vec![-1, 1, 257]);
}

#[test]
fn source_key_set_shards_remove_and_clear_all_shards() {
    let shards = StatsKeySetShards::new();
    shards.add_key_value(1, 10);
    shards.add_key_value(2, 20);
    shards.remove(1);
    assert_eq!(shards.get(1), None);
    assert_eq!(shards.len(), 1);
    shards.clear();
    assert!(shards.keys().is_empty());
    assert_eq!(shards.len(), 0);
}
