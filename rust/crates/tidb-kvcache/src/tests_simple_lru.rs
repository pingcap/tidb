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

//! Ports of Go `pkg/util/kvcache/simple_lru_test.go` from `origin/master`.
//!
//! Go obtains a total-memory quota and samples its heap. These tests inject
//! deterministic samples on the same side of the source thresholds: zero is
//! below a positive quota, while one is above the zero threshold produced by
//! guard `1.0`. This exercises the source branches without depending on host
//! RAM or Rust allocator accounting.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{CacheKey, SimpleLruCache, PROFILE_NAME};

/// Go `mockCacheKey`, including its unusual first zero hash byte.
#[derive(Clone, Debug, Eq, PartialEq)]
struct MockCacheKey {
    hash: [u8; 8],
    key: i64,
}

impl MockCacheKey {
    fn new(key: i64) -> Self {
        let mut hash = [0; 8];
        // In Go, i == 0 makes `(uint(i)-1)*8` a huge shift, which yields 0.
        for (i, byte) in hash.iter_mut().enumerate().skip(1) {
            *byte = ((key >> ((i - 1) * 8)) & 0xff) as u8;
        }
        Self { hash, key }
    }
}

impl CacheKey for MockCacheKey {
    fn hash_bytes(&self) -> &[u8] {
        &self.hash
    }
}

fn key_order(cache: &SimpleLruCache<MockCacheKey, i64>) -> Vec<i64> {
    cache.keys().into_iter().map(|key| key.key).collect()
}

fn value_order(cache: &SimpleLruCache<MockCacheKey, i64>) -> Vec<i64> {
    cache.values().into_iter().copied().collect()
}

/// Go source: `pkg/util/kvcache/simple_lru_test.go::TestPut`.
#[test]
fn put() {
    // `0 < quota * (1-guard)`, matching the source test's ordinary
    // `InstanceMemUsed() < MemTotal()` path.
    let mut lru_with_quota = SimpleLruCache::with_memory_guard(3, 0.0, 1, || Ok(0));
    // A zero quota must bypass memory sampling altogether.
    let mut lru_zero_quota =
        SimpleLruCache::with_memory_guard(3, 0.0, 0, || panic!("zero quota sampled memory"));
    assert_eq!(lru_with_quota.capacity(), 3);
    assert_eq!(lru_zero_quota.capacity(), 3);

    let keys: Vec<_> = (0..5).map(MockCacheKey::new).collect();
    let with_quota_dropped = Arc::new(Mutex::new(HashMap::<i64, i64>::new()));
    let zero_quota_dropped = Arc::new(Mutex::new(HashMap::<i64, i64>::new()));

    let dropped = Arc::clone(&with_quota_dropped);
    lru_with_quota.set_on_evict(move |key: &MockCacheKey, value: &i64| {
        dropped
            .lock()
            .expect("eviction log mutex poisoned")
            .insert(key.key, *value);
    });
    let dropped = Arc::clone(&zero_quota_dropped);
    lru_zero_quota.set_on_evict(move |key: &MockCacheKey, value: &i64| {
        dropped
            .lock()
            .expect("eviction log mutex poisoned")
            .insert(key.key, *value);
    });

    for (value, key) in keys.iter().cloned().enumerate() {
        lru_with_quota.put(key.clone(), value as i64);
        lru_zero_quota.put(key, value as i64);
    }

    assert_eq!(lru_with_quota.size(), lru_with_quota.capacity());
    assert_eq!(lru_zero_quota.size(), lru_zero_quota.capacity());
    assert_eq!(lru_with_quota.size(), 3);

    let with_quota_dropped = with_quota_dropped
        .lock()
        .expect("eviction log mutex poisoned");
    let zero_quota_dropped = zero_quota_dropped
        .lock()
        .expect("eviction log mutex poisoned");
    assert_eq!(with_quota_dropped.len(), 2);
    assert_eq!(zero_quota_dropped.len(), 2);
    for value in 0..2 {
        assert_eq!(with_quota_dropped.get(&value), Some(&value));
        assert_eq!(zero_quota_dropped.get(&value), Some(&value));
    }
    drop(with_quota_dropped);
    drop(zero_quota_dropped);

    for key in &keys[..2] {
        assert!(lru_with_quota.get(key).is_none());
        assert!(lru_zero_quota.get(key).is_none());
    }
    assert_eq!(key_order(&lru_with_quota), vec![4, 3, 2]);
    assert_eq!(value_order(&lru_with_quota), vec![4, 3, 2]);
}

/// Go source: `pkg/util/kvcache/simple_lru_test.go::TestZeroQuota`.
#[test]
fn zero_quota() {
    let mut lru =
        SimpleLruCache::with_memory_guard(100, 0.0, 0, || panic!("zero quota sampled memory"));
    assert_eq!(lru.capacity(), 100);

    for value in 0..100 {
        lru.put(MockCacheKey::new(value), value);
    }
    assert_eq!(lru.size(), lru.capacity());
    assert_eq!(lru.size(), 100);
}

/// Go source: `pkg/util/kvcache/simple_lru_test.go::TestOOMGuard`.
#[test]
fn oom_guard() {
    // Guard 1.0 makes the source threshold zero. A positive sample therefore
    // evicts each newly inserted item immediately.
    let mut lru = SimpleLruCache::with_memory_guard(3, 1.0, 1, || Ok(1));
    assert_eq!(lru.capacity(), 3);

    let keys: Vec<_> = (0..5).map(MockCacheKey::new).collect();
    for (value, key) in keys.iter().cloned().enumerate() {
        lru.put(key, value as i64);
    }
    assert_eq!(lru.size(), 0);
    for key in &keys {
        assert!(lru.get(key).is_none());
    }
}

/// Go source: `pkg/util/kvcache/simple_lru_test.go::TestGet` (supported
/// miss-and-promote assertions).
#[test]
fn get() {
    let mut lru = SimpleLruCache::with_memory_guard(3, 0.0, 1, || Ok(0));
    let keys: Vec<_> = (0..5).map(MockCacheKey::new).collect();

    for (value, key) in keys.iter().cloned().enumerate() {
        lru.put(key, value as i64);
    }

    for key in &keys[..2] {
        assert!(lru.get(key).is_none());
    }
    assert_eq!(key_order(&lru), vec![4, 3, 2]);

    for (value, key) in keys.iter().enumerate().skip(2) {
        assert_eq!(lru.get(key), Some(&(value as i64)));
        assert_eq!(lru.size(), 3);
        assert_eq!(lru.capacity(), 3);
        assert_eq!(lru.keys()[0], key);
        assert_eq!(*lru.values()[0], value as i64);
    }
}

// go-parity-gap: SimpleLruCache has no non-promoting `peek` operation.
/// Go source: `pkg/util/kvcache/simple_lru_test.go::TestGet` (the `Peek`
/// assertion that must leave key 4 at the front).
#[test]
#[ignore = "go-parity-gap: SimpleLruCache has no non-promoting peek operation"]
fn get_peek_does_not_promote_entry() {
    let mut lru = SimpleLruCache::new(3);
    for value in 0..5 {
        lru.put(MockCacheKey::new(value), value);
    }
    assert_eq!(key_order(&lru), vec![4, 3, 2]);

    // Once `peek` exists, this must assert that peeking key 2 returns value 2
    // while `key_order(&lru)` remains `[4, 3, 2]`.
    panic!("blocked on transcreating SimpleLRUCache.Peek");
}

/// Go source: `pkg/util/kvcache/simple_lru_test.go::TestDelete`.
#[test]
fn delete() {
    let mut lru = SimpleLruCache::with_memory_guard(3, 0.0, 1, || Ok(0));
    let keys: Vec<_> = (0..3).map(MockCacheKey::new).collect();

    for (value, key) in keys.iter().cloned().enumerate() {
        lru.put(key, value as i64);
    }
    assert_eq!(lru.size(), 3);

    let _ = lru.delete(&keys[1]);
    assert!(lru.get(&keys[1]).is_none());
    assert_eq!(lru.size(), 2);
    assert!(lru.get(&keys[0]).is_some());
    assert!(lru.get(&keys[2]).is_some());
}

/// Go source: `pkg/util/kvcache/simple_lru_test.go::TestDeleteAll`.
#[test]
fn delete_all() {
    let mut lru = SimpleLruCache::with_memory_guard(3, 0.0, 1, || Ok(0));
    let keys: Vec<_> = (0..3).map(MockCacheKey::new).collect();

    for (value, key) in keys.iter().cloned().enumerate() {
        lru.put(key, value as i64);
    }
    assert_eq!(lru.size(), 3);

    lru.delete_all();
    for key in &keys {
        assert!(lru.get(key).is_none());
        assert_eq!(lru.size(), 0);
    }
}

/// Go source: `pkg/util/kvcache/simple_lru_test.go::TestValues`.
#[test]
fn values() {
    let mut lru = SimpleLruCache::with_memory_guard(5, 0.0, 1, || Ok(0));
    for value in 0..5 {
        lru.put(MockCacheKey::new(value), value);
    }

    let values = value_order(&lru);
    assert_eq!(values.len(), 5);
    assert_eq!(values, vec![4, 3, 2, 1, 0]);
}

/// Go source: `pkg/util/kvcache/simple_lru_test.go::TestPutProfileName`.
#[test]
fn put_profile_name() {
    let lru: SimpleLruCache<MockCacheKey, i64> = SimpleLruCache::new(3);
    assert_eq!(lru.capacity(), 3);
    assert_eq!(
        PROFILE_NAME,
        "github.com/pingcap/tidb/pkg/util/kvcache.(*SimpleLRUCache).Put"
    );
}
