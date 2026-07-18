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
//! the item fits, and then stores it. This leaf ports that bounded policy for
//! already-owned values. Session quota lookup, `chunk.List` memory tracking,
//! the source mutex, and executor/application lifecycle remain external.

use std::collections::{HashMap, VecDeque};

/// Computes the source apply-cache memory charge for one key/value pair.
#[must_use]
pub fn apply_cache_kv_mem(key: &[u8], value_memory: i64) -> i64 {
    key.len() as i64 + value_memory
}

struct CacheEntry<V> {
    value: V,
    memory: i64,
}

/// Bounded LRU cache whose admission and eviction are driven by memory bytes.
pub struct ApplyCache<V> {
    memory_capacity: i64,
    memory_consumed: i64,
    entries: HashMap<Vec<u8>, CacheEntry<V>>,
    lru: VecDeque<Vec<u8>>,
}

impl<V> ApplyCache<V> {
    /// Creates an empty apply cache with the source memory quota.
    #[must_use]
    pub fn new(memory_capacity: i64) -> Self {
        Self {
            memory_capacity,
            memory_consumed: 0,
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    /// Looks up a value and marks it most recently used.
    pub fn get(&mut self, key: &[u8]) -> Option<&V> {
        if !self.entries.contains_key(key) {
            return None;
        }
        self.remove_from_lru(key);
        self.lru.push_back(key.to_vec());
        self.entries.get(key).map(|entry| &entry.value)
    }

    /// Attempts to insert a value under the source memory-budget policy.
    ///
    /// Returns `false` without mutation when the item itself exceeds the
    /// quota. Otherwise, oldest entries are removed until the item fits;
    /// `true` means the value is retained.
    pub fn set(&mut self, key: impl Into<Vec<u8>>, value: V, value_memory: i64) -> bool {
        let key = key.into();
        let memory = apply_cache_kv_mem(&key, value_memory);
        if memory > self.memory_capacity {
            return false;
        }

        if let Some(previous) = self.entries.remove(&key) {
            self.memory_consumed -= previous.memory;
            self.remove_from_lru(&key);
        }

        while memory + self.memory_consumed > self.memory_capacity {
            let Some(oldest) = self.lru.pop_front() else {
                return false;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.memory_consumed -= evicted.memory;
            }
        }

        self.memory_consumed += memory;
        self.entries
            .insert(key.clone(), CacheEntry { value, memory });
        self.lru.push_back(key);
        true
    }

    /// Returns the current memory charge.
    #[must_use]
    pub const fn memory_consumed(&self) -> i64 {
        self.memory_consumed
    }

    /// Returns the number of retained values.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the cache has no retained values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn remove_from_lru(&mut self, key: &[u8]) {
        if let Some(index) = self.lru.iter().position(|entry| entry.as_slice() == key) {
            self.lru.remove(index);
        }
    }
}
