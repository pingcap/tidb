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

//! Transcreation of Go `pkg/util/kvcache/simple_lru_test.go`.
//!
//! Go reaches into the unexported `elements` map and the `container/list`
//! internals (`lru.cache.Front()`, `element.Value.(*cacheEntry)`); the Rust
//! port asserts the same observable state through the crate's public surface:
//! `keys()`/`values()` walk the list from the front (most recently used), and
//! `get`/`delete` answer the membership questions the element lookups did.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tidb_kvcache::{CacheKey, MemoryProbeError, SimpleLruCache, PROFILE_NAME};

/// Go `mockCacheKey`.
#[derive(Clone, Debug)]
struct MockCacheKey {
    hash: Vec<u8>,
    key: i64,
}

impl MockCacheKey {
    fn new(key: i64) -> Self {
        // Go builds the hash lazily inside `Hash()`; the byte layout is kept
        // exactly: `hash[0] = 0` (Go's unsigned shift by a negative multiple
        // of 8 yields zero) and `hash[i] = key >> ((i-1)*8)` for i in 1..8.
        let mut hash = vec![0u8; 8];
        for (i, slot) in hash.iter_mut().enumerate().skip(1) {
            *slot = ((key >> ((i - 1) * 8)) & 0xff) as u8;
        }
        Self { hash, key }
    }

    fn identity(&self) -> i64 {
        self.key
    }
}

impl CacheKey for MockCacheKey {
    fn hash_bytes(&self) -> &[u8] {
        &self.hash
    }
}

/// Go `memory.MemTotal()` (gopsutil's total physical RAM).
fn mem_total() -> u64 {
    let content = std::fs::read_to_string("/proc/meminfo").expect("read /proc/meminfo");
    let kib: u64 = content
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse().ok())
        .expect("MemTotal line in /proc/meminfo");
    kib * 1024
}

/// Go `memory.InstanceMemUsed()`; a real read keeps the guard arithmetic on
/// the same operating-system boundary as the source.
fn instance_mem_used() -> Result<u64, MemoryProbeError> {
    Ok(mem_total() / 2)
}

/// Go `TestPut`: capacity eviction fires `onEvict` with the oldest pairs and
/// leaves the newest `capacity` entries in MRU order.
#[test]
fn test_put() {
    let max_mem = mem_total();

    let mut lru_max_mem = SimpleLruCache::with_memory_guard(3, 0.0, max_mem, instance_mem_used);
    let mut lru_zero_quota = SimpleLruCache::with_memory_guard(3, 0.0, 0, instance_mem_used);
    assert_eq!(3, lru_max_mem.capacity());
    assert_eq!(3, lru_zero_quota.capacity());

    let keys: Vec<MockCacheKey> = (0..5).map(MockCacheKey::new).collect();
    let vals: Vec<i64> = (0..5).collect();
    let max_mem_dropped_kv = Arc::new(Mutex::new(HashMap::<i64, i64>::new()));
    let zero_quota_dropped_kv = Arc::new(Mutex::new(HashMap::<i64, i64>::new()));

    // test onEvict function
    let dropped = Arc::clone(&max_mem_dropped_kv);
    lru_max_mem.set_on_evict(move |key: &MockCacheKey, value: &i64| {
        dropped.lock().unwrap().insert(key.identity(), *value);
    });
    // test onEvict function on 0 value of quota
    let dropped = Arc::clone(&zero_quota_dropped_kv);
    lru_zero_quota.set_on_evict(move |key: &MockCacheKey, value: &i64| {
        dropped.lock().unwrap().insert(key.identity(), *value);
    });
    for i in 0..5 {
        lru_max_mem.put(keys[i].clone(), vals[i]);
        lru_zero_quota.put(keys[i].clone(), vals[i]);
    }
    assert_eq!(lru_max_mem.size(), 3);
    assert_eq!(lru_zero_quota.size(), lru_max_mem.size());

    // test for non-existent elements
    assert_eq!(max_mem_dropped_kv.lock().unwrap().len(), 2);
    for i in 0..2usize {
        assert!(!contains(&mut lru_max_mem, &keys[i]));
        assert_eq!(
            max_mem_dropped_kv.lock().unwrap().get(&vals[i]),
            Some(&vals[i])
        );
        assert_eq!(
            zero_quota_dropped_kv.lock().unwrap().get(&vals[i]),
            Some(&vals[i])
        );
    }

    // test for existent elements: walking from the front yields 4, 3, 2.
    let front_keys: Vec<i64> = lru_max_mem.keys().iter().map(|k| k.identity()).collect();
    assert_eq!(front_keys, vec![4, 3, 2]);
    for i in (2..5).rev() {
        let value = *lru_max_mem.get(&keys[i]).unwrap();
        assert_eq!(vals[i], value);
    }
}

/// Go's `_, exists := lru.elements[hash]` membership probe.
fn contains(cache: &mut SimpleLruCache<MockCacheKey, i64>, key: &MockCacheKey) -> bool {
    cache.get(key).is_some()
}

/// Go `TestZeroQuota`: quota 0 disables process-memory sampling entirely, so
/// only the capacity bound applies.
#[test]
fn test_zero_quota() {
    let mut lru: SimpleLruCache<MockCacheKey, i64> =
        SimpleLruCache::with_memory_guard(100, 0.0, 0, instance_mem_used);
    assert_eq!(100, lru.capacity());

    for i in 0..100 {
        lru.put(MockCacheKey::new(i), i);
    }
    assert_eq!(lru.size(), 100);
}

/// Go `TestOOMGuard`: guard 1.0 drives the usable memory down to zero, so
/// every insert is evicted immediately.
#[test]
fn test_oom_guard() {
    let max_mem = mem_total();

    let mut lru =
        SimpleLruCache::with_memory_guard(3, 1.0, max_mem, instance_mem_used);
    assert_eq!(3, lru.capacity());

    for i in 0..5 {
        lru.put(MockCacheKey::new(i), i);
    }
    assert_eq!(0, lru.size());

    // test for non-existent elements
    for i in 0..5 {
        let key = MockCacheKey::new(i);
        assert!(lru.get(&key).is_none());
    }
}

/// Go `TestGet`: misses return nothing, hits are promoted to the front.
#[test]
fn test_get() {
    let max_mem = mem_total();

    let mut lru = SimpleLruCache::with_memory_guard(3, 0.0, max_mem, instance_mem_used);

    let keys: Vec<MockCacheKey> = (0..5).map(MockCacheKey::new).collect();
    let vals: Vec<i64> = (0..5).collect();

    for i in 0..5 {
        lru.put(keys[i].clone(), vals[i]);
    }

    // test for non-existent elements
    for i in 0..2usize {
        assert!(lru.get(&keys[i]).is_none());
    }

    for i in 2..5usize {
        let value = *lru.get(&keys[i]).expect("hit expected");
        assert_eq!(vals[i], value);
        assert_eq!(3, lru.size());
        assert_eq!(3, lru.capacity());

        // The hit now sits at the front of the list carrying its key/value.
        let front_key = lru.keys()[0].identity();
        assert_eq!(keys[i].identity(), front_key);
        let front_value = lru.values()[0];
        assert_eq!(vals[i], *front_value);
    }
}

/// Go `TestDelete`.
#[test]
fn test_delete() {
    let max_mem = mem_total();

    let mut lru = SimpleLruCache::with_memory_guard(3, 0.0, max_mem, instance_mem_used);

    let keys: Vec<MockCacheKey> = (0..3).map(MockCacheKey::new).collect();
    let vals: Vec<i64> = (0..3).collect();

    for i in 0..3 {
        lru.put(keys[i].clone(), vals[i]);
    }
    assert_eq!(3, lru.size());

    lru.delete(&keys[1]);
    assert!(lru.get(&keys[1]).is_none());
    assert_eq!(2, lru.size());

    assert!(lru.get(&keys[0]).is_some());
    assert!(lru.get(&keys[2]).is_some());
}

/// Go `TestDeleteAll`.
#[test]
fn test_delete_all() {
    let max_mem = mem_total();

    let mut lru = SimpleLruCache::with_memory_guard(3, 0.0, max_mem, instance_mem_used);

    let keys: Vec<MockCacheKey> = (0..3).map(MockCacheKey::new).collect();
    let vals: Vec<i64> = (0..3).collect();

    for i in 0..3 {
        lru.put(keys[i].clone(), vals[i]);
    }
    assert_eq!(3, lru.size());

    lru.delete_all();

    for key in &keys {
        assert!(lru.get(key).is_none());
        assert_eq!(0, lru.size());
    }
}

/// Go `TestValues`: values come back most-recently-used first.
#[test]
fn test_values() {
    let max_mem = mem_total();

    let mut lru = SimpleLruCache::with_memory_guard(5, 0.0, max_mem, instance_mem_used);

    for i in 0..5 {
        lru.put(MockCacheKey::new(i), i);
    }

    let values = lru.values();
    assert_eq!(5, values.len());
    for (i, value) in values.iter().enumerate() {
        assert_eq!(4 - i as i64, **value);
    }
}

/// Go `TestPutProfileName`: the heap-profile name stays bound to the source
/// method path. Go rebuilds it through reflection over the type and its
/// `Put` method; Rust asserts the constant against that exact spelling.
#[test]
fn test_put_profile_name() {
    let mut _lru: SimpleLruCache<Vec<u8>, ()> = SimpleLruCache::new(3);
    assert_eq!(3, _lru.capacity());
    assert_eq!(
        PROFILE_NAME,
        "github.com/pingcap/tidb/pkg/util/kvcache.(*SimpleLRUCache).Put"
    );
}
