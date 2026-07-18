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

//! Source-backed tests for map-backed statistics-cache state.

use tidb_stats::MapCache;

#[test]
fn source_map_cache_put_get_replace_tracks_cost() {
    let mut cache = MapCache::new();
    assert!(cache.is_empty());
    assert_eq!(cache.get(1), None);

    assert!(cache.put(1, "old".to_owned(), 10));
    assert_eq!(cache.get(1).map(String::as_str), Some("old"));
    assert_eq!(cache.cost(), 10);
    assert_eq!(cache.len(), 1);

    assert!(cache.put(1, "new".to_owned(), 25));
    assert_eq!(cache.get(1).map(String::as_str), Some("new"));
    assert_eq!(cache.cost(), 25);
    assert_eq!(cache.len(), 1);
}

#[test]
fn source_map_cache_delete_keys_values_and_copy_are_independent() {
    let mut cache = MapCache::new();
    cache.put(1, 10, 4);
    cache.put(2, 20, 6);

    let mut keys = cache.keys();
    keys.sort_unstable();
    assert_eq!(keys, vec![1, 2]);
    let mut values = cache.values().into_iter().copied().collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![10, 20]);

    let mut copied = cache.copy();
    cache.del(1);
    cache.del(99);
    assert_eq!(cache.cost(), 6);
    assert_eq!(cache.len(), 1);
    assert_eq!(copied.cost(), 10);
    assert_eq!(copied.len(), 2);

    copied.put(2, 200, 8);
    assert_eq!(copied.get(2), Some(&200));
    assert_eq!(copied.cost(), 12);
    assert_eq!(cache.get(2), Some(&20));
}

#[test]
fn source_map_cache_preserves_signed_cost_arithmetic_and_noop_hooks() {
    let mut cache = MapCache::new();
    cache.put(1, (), i64::MAX);
    cache.put(1, (), i64::MIN);
    assert_eq!(cache.cost(), i64::MIN);

    cache.set_capacity(1);
    cache.trigger_evict();
    cache.wait_for_async_updates();
    cache.close();
}
