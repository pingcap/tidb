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

//! Memory-budgeted apply-cache policy from `pkg/executor/internal/applycache`.
//!
//! The Go owner charges each key/value pair as `len(key) + value memory`,
//! rejects an item larger than the quota, evicts the oldest LRU entries until
//! the item fits, and then stores it. `tidb-kvcache` owns the generic LRU order;
//! this module owns Apply's retained-byte quota and synchronization.

use std::sync::{Arc, Mutex, MutexGuard};
use tidb_util::kvcache::SimpleLruCache;

/// Computes the source apply-cache memory charge for one key/value pair.
#[must_use]
pub fn apply_cache_kv_mem(key: &[u8], value_memory: i64) -> i64 {
    key.len() as i64 + value_memory
}

struct CacheEntry<V> {
    value: Arc<V>,
    memory: i64,
}

struct CacheState<V> {
    memory_consumed: i64,
    entries: SimpleLruCache<Vec<u8>, CacheEntry<V>>,
}

/// Thread-safe bounded LRU cache whose admission and eviction are driven by
/// retained bytes.
pub struct ApplyCache<V> {
    memory_capacity: i64,
    state: Mutex<CacheState<V>>,
}

impl<V> ApplyCache<V> {
    /// Creates an empty apply cache with the source memory quota.
    #[must_use]
    pub fn new(memory_capacity: i64) -> Self {
        Self {
            memory_capacity,
            state: Mutex::new(CacheState {
                memory_consumed: 0,
                entries: SimpleLruCache::new(usize::MAX),
            }),
        }
    }

    /// Looks up a shared immutable value and marks it most recently used.
    pub fn get(&self, key: &[u8]) -> Option<Arc<V>> {
        let mut state = self.lock();
        state.entries.get(key).map(|entry| Arc::clone(&entry.value))
    }

    /// Attempts to insert a value under the source memory-budget policy.
    ///
    /// Returns `false` without mutation when the item itself exceeds the
    /// quota. Otherwise, oldest entries are removed until the item fits;
    /// `true` means the value is retained.
    pub fn set(&self, key: impl Into<Vec<u8>>, value: V, value_memory: i64) -> bool {
        self.set_shared(key, Arc::new(value), value_memory)
    }

    /// Attempts to insert an already shared immutable value.
    ///
    /// Apply executors keep iterating a newly computed inner relation while
    /// the cache begins owning it. Sharing that one allocation avoids a
    /// second full relation copy without exposing mutable cached state.
    pub fn set_shared(&self, key: impl Into<Vec<u8>>, value: Arc<V>, value_memory: i64) -> bool {
        let key = key.into();
        let memory = apply_cache_kv_mem(&key, value_memory);
        if memory > self.memory_capacity {
            return false;
        }
        let mut state = self.lock();

        if let Some((_, previous)) = state.entries.delete(key.as_slice()) {
            state.memory_consumed -= previous.memory;
        }

        while memory + state.memory_consumed > self.memory_capacity {
            let Some((_, evicted)) = state.entries.remove_oldest() else {
                return false;
            };
            state.memory_consumed -= evicted.memory;
        }

        state.memory_consumed += memory;
        state.entries.put(key, CacheEntry { value, memory });
        true
    }

    /// Returns the current memory charge.
    #[must_use]
    pub fn memory_consumed(&self) -> i64 {
        self.lock().memory_consumed
    }

    /// Returns the number of retained values.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Returns whether the cache has no retained values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().entries.is_empty()
    }

    fn lock(&self) -> MutexGuard<'_, CacheState<V>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
