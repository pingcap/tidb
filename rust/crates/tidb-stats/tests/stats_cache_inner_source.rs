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

//! Source-backed contract tests for `StatsCacheInner`.
//!
//! The concrete LFU test at
//! `pkg/statistics/handle/cache/internal/lfu/lfu_cache_test.go:49`
//! (`TestLFUFreshMemUsage`) exercises this interface's put, cost, and async
//! visibility path while table memory accounting stays implementation-owned.

use std::collections::BTreeMap;

use tidb_stats::StatsCacheInner;

#[derive(Clone, Default)]
struct MockCache<V> {
    values: BTreeMap<i64, V>,
    capacity: i64,
    closed: bool,
    evictions_requested: usize,
}

impl<V: Clone + 'static> StatsCacheInner<V> for MockCache<V> {
    fn get(&self, tid: i64) -> Option<&V> {
        self.values.get(&tid)
    }

    fn put(&mut self, tid: i64, value: V) -> bool {
        self.values.insert(tid, value);
        true
    }

    fn del(&mut self, tid: i64) {
        self.values.remove(&tid);
    }

    fn cost(&self) -> i64 {
        self.values.len() as i64
    }

    fn values(&self) -> Vec<&V> {
        self.values.values().collect()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn copy(&self) -> Box<dyn StatsCacheInner<V>> {
        Box::new(self.clone())
    }

    fn set_capacity(&mut self, capacity: i64) {
        self.capacity = capacity;
    }

    fn close(&mut self) {
        self.closed = true;
    }

    fn trigger_evict(&mut self) {
        self.evictions_requested += 1;
    }

    fn wait_for_async_updates(&self) {}
}

#[test]
fn source_stats_cache_inner_preserves_lifecycle_and_copy_contract() {
    let mut cache = MockCache::default();
    assert!(cache.put(1, "one"));
    assert!(cache.put(2, "two"));
    cache.wait_for_async_updates();

    assert_eq!(cache.get(1), Some(&"one"));
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.cost(), 2);
    assert_eq!(cache.values().len(), 2);

    let copy = cache.copy();
    assert_eq!(copy.get(1), Some(&"one"));
    assert_eq!(copy.len(), 2);
    assert_eq!(copy.cost(), 2);

    cache.set_capacity(8);
    cache.trigger_evict();
    cache.close();
    assert_eq!(cache.capacity, 8);
    assert_eq!(cache.evictions_requested, 1);
    assert!(cache.closed);
}

#[test]
fn source_stats_cache_inner_replaces_and_deletes_by_id() {
    let mut cache = MockCache::default();
    assert!(cache.put(7, 10));
    assert!(cache.put(7, 11));
    assert_eq!(cache.get(7), Some(&11));
    assert_eq!(cache.len(), 1);

    cache.del(7);
    cache.del(7);
    assert_eq!(cache.get(7), None);
    assert_eq!(cache.len(), 0);
    assert_eq!(cache.cost(), 0);
}
