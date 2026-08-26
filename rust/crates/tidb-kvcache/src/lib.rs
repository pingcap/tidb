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

//! O(1) least-recently-used cache from `pkg/util/kvcache`.
//!
//! The source cache identifies keys by their byte hash while retaining the
//! first key object stored for that hash. Rust owns nodes in stable indexed
//! slots instead of Go's `container/list`; the ordering and eviction contract
//! are unchanged. The cache is intentionally not synchronized.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

/// Source heap-profile function name exposed by the Go package.
pub const PROFILE_NAME: &str = "github.com/pingcap/tidb/pkg/util/kvcache.(*SimpleLRUCache).Put";

/// Error returned by an injected process-memory sampler.
pub type MemoryProbeError = Box<dyn Error + Send + Sync + 'static>;

/// A cache key whose byte hash defines identity.
pub trait CacheKey {
    /// Returns the stable bytes used for lookup.
    fn hash_bytes(&self) -> &[u8];
}

impl CacheKey for [u8] {
    fn hash_bytes(&self) -> &[u8] {
        self
    }
}

impl CacheKey for Vec<u8> {
    fn hash_bytes(&self) -> &[u8] {
        self
    }
}

impl CacheKey for str {
    fn hash_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl CacheKey for String {
    fn hash_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl CacheKey for Box<[u8]> {
    fn hash_bytes(&self) -> &[u8] {
        self
    }
}

impl<const N: usize> CacheKey for [u8; N] {
    fn hash_bytes(&self) -> &[u8] {
        self
    }
}

/// Capacity must remain positive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidCapacity;

impl fmt::Display for InvalidCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("capacity of lru cache should be at least 1")
    }
}

impl Error for InvalidCapacity {}

struct Node<K, V> {
    hash: Vec<u8>,
    key: K,
    value: V,
    previous: Option<usize>,
    next: Option<usize>,
}

struct MemoryGuard {
    guard: f64,
    quota: u64,
    probe: Box<dyn FnMut() -> Result<u64, MemoryProbeError> + Send>,
}

type EvictionCallback<K, V> = Box<dyn FnMut(&K, &V) + Send>;

/// A non-thread-safe least-recently-used cache.
pub struct SimpleLruCache<K, V> {
    elements: HashMap<Vec<u8>, usize>,
    nodes: Vec<Option<Node<K, V>>>,
    free: Vec<usize>,
    newest: Option<usize>,
    oldest: Option<usize>,
    capacity: usize,
    memory_guard: Option<MemoryGuard>,
    on_evict: Option<EvictionCallback<K, V>>,
}

impl<K: CacheKey, V> SimpleLruCache<K, V> {
    /// Creates a capacity-bounded cache without process-memory sampling.
    ///
    /// # Panics
    ///
    /// Panics when `capacity` is zero, matching the source constructor.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity of LRU Cache should be at least 1.");
        Self {
            elements: HashMap::new(),
            nodes: Vec::new(),
            free: Vec::new(),
            newest: None,
            oldest: None,
            capacity,
            memory_guard: None,
            on_evict: None,
        }
    }

    /// Creates a cache with the source process-memory guard policy.
    ///
    /// The caller supplies allocator/process sampling because Go's
    /// `runtime.MemStats.HeapAlloc` mechanism is runtime-specific. Its result,
    /// including errors, drives the same observable eviction policy.
    ///
    /// # Panics
    ///
    /// Panics when `capacity` is zero.
    pub fn with_memory_guard<F>(capacity: usize, guard: f64, quota: u64, probe: F) -> Self
    where
        F: FnMut() -> Result<u64, MemoryProbeError> + Send + 'static,
    {
        let mut cache = Self::new(capacity);
        if quota != 0 {
            cache.memory_guard = Some(MemoryGuard {
                guard,
                quota,
                probe: Box::new(probe),
            });
        }
        cache
    }

    /// Installs the callback invoked by automatic `put` eviction.
    pub fn set_on_evict<F>(&mut self, callback: F)
    where
        F: FnMut(&K, &V) + Send + 'static,
    {
        self.on_evict = Some(Box::new(callback));
    }

    /// Removes the automatic-eviction callback.
    pub fn clear_on_evict(&mut self) {
        self.on_evict = None;
    }

    /// Looks up a value and promotes it to most recently used.
    pub fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        Q: CacheKey + ?Sized,
    {
        let index = *self.elements.get(key.hash_bytes())?;
        self.move_to_front(index);
        Some(&self.node(index).value)
    }

    /// Inserts or updates a value.
    ///
    /// Updating an existing byte hash retains its original key object and
    /// does not run capacity or process-memory eviction, as in the source.
    pub fn put(&mut self, key: K, value: V) {
        let hash = key.hash_bytes().to_vec();
        if let Some(&index) = self.elements.get(hash.as_slice()) {
            self.node_mut(index).value = value;
            self.move_to_front(index);
            return;
        }

        self.insert_new(hash, key, value);
        if self.memory_guard.is_none() {
            if self.len() > self.capacity {
                self.evict_oldest();
            }
            return;
        }

        let Some(mut used) = self.sample_memory_or_clear() else {
            return;
        };
        loop {
            let above_memory_limit = used > self.memory_threshold();
            if !above_memory_limit && self.len() <= self.capacity {
                break;
            }
            if self.oldest.is_none() {
                break;
            }
            self.evict_oldest();
            if above_memory_limit {
                let Some(sample) = self.sample_memory_or_clear() else {
                    return;
                };
                used = sample;
            }
        }
    }

    /// Removes a key without invoking the eviction callback.
    pub fn delete<Q>(&mut self, key: &Q) -> Option<(K, V)>
    where
        Q: CacheKey + ?Sized,
    {
        let index = *self.elements.get(key.hash_bytes())?;
        let node = self.remove_index(index);
        Some((node.key, node.value))
    }

    /// Removes every entry without invoking the eviction callback.
    pub fn delete_all(&mut self) {
        while self.oldest.is_some() {
            let _ = self.remove_oldest();
        }
    }

    /// Returns the number of retained entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns the current cache size (`SimpleLRUCache.Size`).
    #[must_use]
    pub fn size(&self) -> usize {
        self.len()
    }

    /// Returns whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// The configured entry bound. Go's tests read the unexported
    /// `capacity` field directly; this accessor exposes the same value.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns values in most-recently-used order.
    #[must_use]
    pub fn values(&self) -> Vec<&V> {
        self.indices_from_newest()
            .map(|index| &self.node(index).value)
            .collect()
    }

    /// Returns keys in most-recently-used order.
    #[must_use]
    pub fn keys(&self) -> Vec<&K> {
        self.indices_from_newest()
            .map(|index| &self.node(index).key)
            .collect()
    }

    /// Changes the maximum entry count.
    ///
    /// Shrinking removes oldest entries without invoking the callback.
    pub fn set_capacity(&mut self, capacity: usize) -> Result<(), InvalidCapacity> {
        if capacity == 0 {
            return Err(InvalidCapacity);
        }
        self.capacity = capacity;
        while self.len() > self.capacity {
            let _ = self.remove_oldest();
        }
        Ok(())
    }

    /// Removes and returns the least-recently-used entry without invoking the
    /// callback.
    pub fn remove_oldest(&mut self) -> Option<(K, V)> {
        let index = self.oldest?;
        let node = self.remove_index(index);
        Some((node.key, node.value))
    }

    fn insert_new(&mut self, hash: Vec<u8>, key: K, value: V) {
        let index = if let Some(index) = self.free.pop() {
            index
        } else {
            self.nodes.push(None);
            self.nodes.len() - 1
        };
        self.nodes[index] = Some(Node {
            hash: hash.clone(),
            key,
            value,
            previous: None,
            next: None,
        });
        self.elements.insert(hash, index);
        self.link_front(index);
    }

    fn move_to_front(&mut self, index: usize) {
        if self.newest == Some(index) {
            return;
        }
        self.unlink(index);
        self.link_front(index);
    }

    fn link_front(&mut self, index: usize) {
        let former_newest = self.newest;
        {
            let node = self.node_mut(index);
            node.previous = None;
            node.next = former_newest;
        }
        if let Some(former_newest) = former_newest {
            self.node_mut(former_newest).previous = Some(index);
        } else {
            self.oldest = Some(index);
        }
        self.newest = Some(index);
    }

    fn unlink(&mut self, index: usize) {
        let (previous, next) = {
            let node = self.node(index);
            (node.previous, node.next)
        };
        if let Some(previous) = previous {
            self.node_mut(previous).next = next;
        } else {
            self.newest = next;
        }
        if let Some(next) = next {
            self.node_mut(next).previous = previous;
        } else {
            self.oldest = previous;
        }
        let node = self.node_mut(index);
        node.previous = None;
        node.next = None;
    }

    fn remove_index(&mut self, index: usize) -> Node<K, V> {
        self.unlink(index);
        let node = self.nodes[index]
            .take()
            .expect("linked LRU index must contain a node");
        self.elements.remove(node.hash.as_slice());
        self.free.push(index);
        node
    }

    fn evict_oldest(&mut self) {
        let Some(index) = self.oldest else {
            return;
        };
        if let Some(callback) = self.on_evict.as_mut() {
            let node = self.nodes[index]
                .as_ref()
                .expect("linked LRU index must contain a node");
            callback(&node.key, &node.value);
        }
        let _ = self.remove_index(index);
    }

    fn sample_memory_or_clear(&mut self) -> Option<u64> {
        let result = (self
            .memory_guard
            .as_mut()
            .expect("memory guard must exist")
            .probe)();
        match result {
            Ok(used) => Some(used),
            Err(_) => {
                self.delete_all();
                None
            }
        }
    }

    fn memory_threshold(&self) -> u64 {
        let guard = self.memory_guard.as_ref().expect("memory guard must exist");
        (guard.quota as f64 * (1.0 - guard.guard)) as u64
    }

    fn indices_from_newest(&self) -> Indices<'_, K, V> {
        Indices {
            cache: self,
            next: self.newest,
        }
    }

    fn node(&self, index: usize) -> &Node<K, V> {
        self.nodes[index]
            .as_ref()
            .expect("linked LRU index must contain a node")
    }

    fn node_mut(&mut self, index: usize) -> &mut Node<K, V> {
        self.nodes[index]
            .as_mut()
            .expect("linked LRU index must contain a node")
    }
}

struct Indices<'a, K, V> {
    cache: &'a SimpleLruCache<K, V>,
    next: Option<usize>,
}

impl<K, V> Iterator for Indices<'_, K, V> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.next?;
        self.next = self.cache.nodes[index]
            .as_ref()
            .expect("linked LRU index must contain a node")
            .next;
        Some(index)
    }
}

#[cfg(test)]
mod tests_simple_lru;
