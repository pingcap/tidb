// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Semantic boundary tests for accepted Go package `pkg/util/kvcache`.

use std::io;
use std::sync::{Arc, Mutex};

use tidb_kvcache::{CacheKey, MemoryProbeError, SimpleLruCache, PROFILE_NAME};

#[derive(Debug, Eq, PartialEq)]
struct Key {
    hash: Vec<u8>,
    identity: u8,
}

impl Key {
    fn new(identity: u8) -> Self {
        Self {
            hash: vec![identity],
            identity,
        }
    }

    fn colliding(identity: u8) -> Self {
        Self {
            hash: vec![42],
            identity,
        }
    }
}

impl CacheKey for Key {
    fn hash_bytes(&self) -> &[u8] {
        &self.hash
    }
}

#[test]
fn put_get_and_capacity_eviction_preserve_lru_order() {
    let evicted = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&evicted);
    let mut cache = SimpleLruCache::new(3);
    cache.set_on_evict(move |key: &Key, value: &i64| {
        observed
            .lock()
            .expect("eviction log mutex must not be poisoned")
            .push((key.identity, *value));
    });

    for identity in 0..5 {
        cache.put(Key::new(identity), i64::from(identity));
    }
    assert_eq!(cache.len(), 3);
    assert_eq!(cache.size(), 3);
    assert_eq!(*evicted.lock().unwrap(), vec![(0, 0), (1, 1)]);
    assert_eq!(
        cache
            .keys()
            .iter()
            .map(|key| key.identity)
            .collect::<Vec<_>>(),
        vec![4, 3, 2]
    );
    assert_eq!(cache.values(), vec![&4, &3, &2]);

    assert_eq!(cache.get(&Key::new(2)), Some(&2));
    assert_eq!(
        cache
            .keys()
            .iter()
            .map(|key| key.identity)
            .collect::<Vec<_>>(),
        vec![2, 4, 3]
    );
    assert_eq!(cache.get(&Key::new(9)), None);
}

#[test]
fn equal_hash_updates_value_and_retains_the_first_key() {
    let mut cache = SimpleLruCache::new(2);
    cache.put(Key::colliding(1), "first");
    cache.put(Key::colliding(2), "second");

    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get(&Key::colliding(9)), Some(&"second"));
    assert_eq!(cache.keys()[0].identity, 1);
}

#[test]
fn explicit_removal_capacity_change_and_clear_do_not_call_on_evict() {
    let callbacks = Arc::new(Mutex::new(0));
    let observed = Arc::clone(&callbacks);
    let mut cache = SimpleLruCache::new(4);
    cache.set_on_evict(move |_: &Key, _: &i64| {
        *observed.lock().unwrap() += 1;
    });
    for identity in 0..4 {
        cache.put(Key::new(identity), i64::from(identity));
    }

    let (deleted_key, deleted_value) = cache.delete(&Key::new(2)).unwrap();
    assert_eq!((deleted_key.identity, deleted_value), (2, 2));
    assert_eq!(cache.delete(&Key::new(9)), None);

    assert_eq!(
        cache.set_capacity(0).unwrap_err().to_string(),
        "capacity of lru cache should be at least 1"
    );
    assert_eq!(cache.len(), 3, "invalid capacity must not mutate the cache");
    cache.set_capacity(2).unwrap();
    let (oldest_key, oldest_value) = cache.remove_oldest().unwrap();
    assert_eq!((oldest_key.identity, oldest_value), (1, 1));
    cache.delete_all();
    assert!(cache.is_empty());
    assert_eq!(*callbacks.lock().unwrap(), 0);
}

#[test]
fn memory_guard_evicts_until_the_probe_falls_below_threshold() {
    let samples = Arc::new(Mutex::new(vec![5_u64, 5, 20, 20, 5]));
    let probe_samples = Arc::clone(&samples);
    let evicted = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&evicted);
    let mut cache = SimpleLruCache::with_memory_guard(10, 0.5, 20, move || {
        Ok(probe_samples.lock().unwrap().remove(0))
    });
    cache.set_on_evict(move |key: &Key, _: &i64| {
        observed.lock().unwrap().push(key.identity);
    });
    cache.put(Key::new(1), 1);
    cache.put(Key::new(2), 2);
    cache.put(Key::new(3), 3);

    assert_eq!(*evicted.lock().unwrap(), vec![1, 2]);
    assert_eq!(cache.values(), vec![&3]);
}

#[test]
fn memory_guard_also_enforces_capacity_without_resampling() {
    let samples = Arc::new(Mutex::new(0));
    let probe_count = Arc::clone(&samples);
    let mut cache = SimpleLruCache::with_memory_guard(2, 0.0, 100, move || {
        *probe_count.lock().unwrap() += 1;
        Ok(0)
    });
    cache.put(Key::new(1), 1);
    cache.put(Key::new(2), 2);
    cache.put(Key::new(3), 3);

    assert_eq!(cache.values(), vec![&3, &2]);
    assert_eq!(*samples.lock().unwrap(), 3);
}

#[test]
fn memory_probe_failure_clears_remaining_entries_without_more_callbacks() {
    let probe_calls = Arc::new(Mutex::new(0));
    let observed_calls = Arc::clone(&probe_calls);
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&callbacks);
    let mut cache = SimpleLruCache::with_memory_guard(3, 0.5, 20, move || {
        let mut calls = observed_calls.lock().unwrap();
        *calls += 1;
        match *calls {
            1 | 2 => Ok(5),
            3 => Ok(20),
            _ => Err::<u64, MemoryProbeError>(Box::new(io::Error::other("probe failed"))),
        }
    });
    cache.set_on_evict(move |key: &Key, _: &i64| {
        observed.lock().unwrap().push(key.identity);
    });
    cache.put(Key::new(1), 1);
    cache.put(Key::new(2), 2);
    cache.put(Key::new(3), 3);

    assert!(cache.is_empty());
    assert_eq!(*callbacks.lock().unwrap(), vec![1]);
}

#[test]
#[should_panic(expected = "capacity of LRU Cache should be at least 1.")]
fn zero_capacity_panics() {
    let _: SimpleLruCache<Vec<u8>, ()> = SimpleLruCache::new(0);
}

#[test]
fn profile_name_is_stable() {
    assert_eq!(
        PROFILE_NAME,
        "github.com/pingcap/tidb/pkg/util/kvcache.(*SimpleLRUCache).Put"
    );
}
