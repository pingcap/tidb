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

//! Source-backed tests for the memory-budgeted apply cache.

use std::sync::{Arc, Barrier};
use std::thread;
use tidb_executor::apply_cache::{apply_cache_kv_mem, ApplyCache};

#[test]
fn apply_cache_memory_charge_and_lru_eviction_match_source() {
    // Source: pkg/executor/internal/applycache/apply_cache.go:35-43,
    // 76-101.
    // Direct Go coverage: pkg/executor/internal/applycache/apply_cache_test.go:30
    // (TestApplyCache), quota=100 and three 100-byte key/value entries.
    let cache = ApplyCache::new(100);
    let keys: Vec<Vec<u8>> = (0..3).map(|i| vec![b'0' + i; 100]).collect();
    for key in &keys {
        assert_eq!(apply_cache_kv_mem(key, 0), 100);
    }

    assert!(cache.set(keys[0].clone(), 0_i64, 0));
    assert_eq!(cache.get(&keys[0]).as_deref(), Some(&0));
    assert_eq!(cache.memory_consumed(), 100);

    assert!(cache.set(keys[1].clone(), 1_i64, 0));
    assert_eq!(cache.get(&keys[1]).as_deref(), Some(&1));
    assert!(cache.set(keys[2].clone(), 2_i64, 0));
    assert_eq!(cache.get(&keys[2]).as_deref(), Some(&2));

    // key 0 was touched before key 1 and key 2 arrived; key 0 and key 1 are
    // therefore evicted by the two subsequent 100-byte insertions.
    assert_eq!(cache.get(&keys[0]), None);
    assert_eq!(cache.get(&keys[1]), None);
    assert_eq!(cache.get(&keys[2]).as_deref(), Some(&2));
    assert_eq!(cache.memory_consumed(), 100);

    let cache = ApplyCache::new(200);
    assert!(cache.set(keys[0].clone(), 0_i64, 0));
    assert!(cache.set(keys[1].clone(), 1_i64, 0));
    assert_eq!(cache.get(&keys[0]).as_deref(), Some(&0));
    assert!(cache.set(keys[2].clone(), 2_i64, 0));
    assert_eq!(cache.get(&keys[0]).as_deref(), Some(&0));
    assert_eq!(cache.get(&keys[1]), None, "the untouched key is oldest");
    assert_eq!(cache.get(&keys[2]).as_deref(), Some(&2));
}

#[test]
fn apply_cache_rejects_an_item_larger_than_quota_without_mutation() {
    // Source: pkg/executor/internal/applycache/apply_cache.go:86-101.
    // Direct Go coverage: pkg/executor/internal/applycache/apply_cache_test.go:30
    // (TestApplyCache), Set's oversize admission boundary is source-owned even
    // though the fixture uses exactly-quota entries.
    let cache = ApplyCache::new(100);
    assert!(!cache.set(vec![b'x'; 101], 7_i64, 0));
    assert!(cache.is_empty());
    assert_eq!(cache.memory_consumed(), 0);
}

#[test]
fn apply_cache_counts_value_memory_at_the_exact_quota_boundary() {
    // Source: pkg/executor/internal/applycache/apply_cache.go:35-43,
    // 86-101. The Go fixture's chunk tracker currently reports zero, but the
    // production admission rule charges both the key and retained value.
    let cache = ApplyCache::new(12);
    assert!(cache.set(b"a0".to_vec(), 0_i64, 4));
    assert!(cache.set(b"a1".to_vec(), 1_i64, 4));
    assert_eq!(cache.memory_consumed(), 12);

    // A read promotes a0, so admitting one more six-byte entry evicts a1.
    assert_eq!(cache.get(b"a0").as_deref(), Some(&0));
    assert!(cache.set(b"a2".to_vec(), 2_i64, 4));
    assert_eq!(cache.get(b"a0").as_deref(), Some(&0));
    assert_eq!(cache.get(b"a1"), None);
    assert_eq!(cache.get(b"a2").as_deref(), Some(&2));
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.memory_consumed(), 12);

    assert!(!cache.set(b"x".to_vec(), 9_i64, 12));
    assert_eq!(cache.len(), 2, "oversize admission must not evict");
    assert_eq!(cache.memory_consumed(), 12);
}

#[test]
fn replacing_a_key_preserves_the_source_tracker_charge() {
    // Go ApplyCache.Set always consumes the incoming key/value charge after
    // its eviction loop. SimpleLRUCache.Put then replaces the value without
    // refunding the previous charge, so replacing a six-byte entry under a
    // twelve-byte quota leaves one entry and twelve charged bytes.
    let cache = ApplyCache::new(12);
    assert!(cache.set(b"a0".to_vec(), 0_i64, 4));
    assert!(cache.set(b"a0".to_vec(), 1_i64, 4));

    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get(b"a0").as_deref(), Some(&1));
    assert_eq!(cache.memory_consumed(), 12);

    // The next six-byte admission must evict a0 because the source tracker is
    // already at quota, even though only one physical entry is retained.
    assert!(cache.set(b"a1".to_vec(), 2_i64, 4));
    assert_eq!(cache.get(b"a0"), None);
    assert_eq!(cache.get(b"a1").as_deref(), Some(&2));
    assert_eq!(cache.memory_consumed(), 12);
}

#[test]
fn apply_cache_serializes_concurrent_get_and_set() {
    // Source: pkg/executor/internal/applycache/apply_cache_test.go:82-138
    // (TestApplyCacheConcurrent). Rust keeps the whole admission/eviction
    // transaction under one mutex, eliminating duplicate-owner races while
    // preserving one shared LRU and the 100-byte retained-memory ceiling.
    let cache = Arc::new(ApplyCache::new(100));
    let barrier = Arc::new(Barrier::new(3));
    let keys = [vec![b'0'; 100], vec![b'1'; 100]];
    let workers: [thread::JoinHandle<()>; 2] = std::array::from_fn(|index| {
        let cache = Arc::clone(&cache);
        let barrier = Arc::clone(&barrier);
        let key = keys[index].clone();
        thread::spawn(move || {
            for _ in 0..100 {
                barrier.wait();
                assert!(cache.set(key.clone(), index as i64, 0));
                barrier.wait();
            }
        })
    });

    for _ in 0..100 {
        barrier.wait();
        barrier.wait();
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.memory_consumed(), 100);
        assert_eq!(
            keys.iter().filter(|key| cache.get(key).is_some()).count(),
            1
        );
    }

    for worker in workers {
        worker.join().expect("apply-cache worker must not panic");
    }
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.memory_consumed(), 100);
}
