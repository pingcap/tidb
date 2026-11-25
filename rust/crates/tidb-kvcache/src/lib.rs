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

use hashbrown::HashTable;
use std::error::Error;
use std::fmt;
use std::hash::BuildHasher;

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
#[derive(Debug)]
pub struct InvalidCapacity;

impl fmt::Display for InvalidCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("capacity of lru cache should be at least 1")
    }
}

impl Error for InvalidCapacity {}

struct Node<K, V> {
    hash: Box<[u8]>,
    hash_code: u64,
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

const SMALL_CACHE_SLOTS: usize = 8;
const LONG_KEY_THRESHOLD: usize = 64;
const WIDE_KEY_THRESHOLD: usize = 1024;

struct WideKeyBucket {
    sample: u64,
    count: usize,
    // When the count falls to one, the XOR is the remaining slot index.
    slot_xor: usize,
}

fn wide_keys_equal(stored: &[u8], requested: &[u8]) -> bool {
    const EDGE_BYTES: usize = 64;
    const BLOCK_BYTES: usize = 4096;
    debug_assert!(requested.len() >= WIDE_KEY_THRESHOLD);
    if stored.len() != requested.len() {
        return false;
    }
    let end = requested.len() - EDGE_BYTES;
    if stored[end..] != requested[end..] {
        return false;
    }
    if requested.len() >= 2 * BLOCK_BYTES {
        // A wider suffix check catches late mismatches beyond the edge.
        // Check the first bytes before reading that block for an early miss.
        let block = end - BLOCK_BYTES;
        return stored[..EDGE_BYTES] == requested[..EDGE_BYTES]
            && stored[block..end] == requested[block..end]
            && stored[EDGE_BYTES..block] == requested[EDGE_BYTES..block];
    }
    stored[..end] == requested[..end]
}

/// A non-thread-safe least-recently-used cache.
pub struct SimpleLruCache<K, V> {
    // Slots own the byte keys. The table indexes them by their saved hash.
    elements: HashTable<usize>,
    hash_builder: foldhash::fast::RandomState,
    wide_key_samples: HashTable<WideKeyBucket>,
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
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity of LRU Cache should be at least 1.");
        Self {
            elements: HashTable::new(),
            hash_builder: foldhash::fast::RandomState::default(),
            wide_key_samples: HashTable::new(),
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

    /// Looks up a value and promotes it to most recently used.
    pub fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        Q: CacheKey + ?Sized,
    {
        let index = self.find_index(key.hash_bytes())?;
        self.move_to_front(index);
        Some(&self.node(index).value)
    }

    /// Looks up a value without changing its least-recently-used position.
    ///
    /// This is Go `SimpleLRUCache.Peek`, added on the current master branch
    /// for read-only inspection of an entry's value and ordering.
    pub fn peek<Q>(&self, key: &Q) -> Option<&V>
    where
        Q: CacheKey + ?Sized,
    {
        let index = self.find_index(key.hash_bytes())?;
        Some(&self.node(index).value)
    }

    /// Inserts or updates a value.
    ///
    /// Updating an existing byte hash retains its original key object and
    /// does not run capacity or process-memory eviction, as in the source.
    pub fn put(&mut self, key: K, value: V) {
        let hash = key.hash_bytes();
        let mut hash_code = None;
        if let Some(index) = self.find_index_using(hash, || {
            let code = self.hash_builder.hash_one(hash);
            hash_code = Some(code);
            self.find_hashed(hash, code)
        }) {
            self.node_mut(index).value = value;
            self.move_to_front(index);
            return;
        }

        let hash_code = hash_code.unwrap_or_else(|| self.hash_builder.hash_one(hash));
        self.insert_new(hash.into(), hash_code, key, value);
        if self.memory_guard.is_none() {
            if self.entry_count() > self.capacity {
                self.evict_oldest();
            }
            return;
        }

        let Some(mut used) = self.sample_memory_or_clear() else {
            return;
        };
        loop {
            let above_memory_limit = used > self.memory_threshold();
            if !above_memory_limit && self.entry_count() <= self.capacity {
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
    pub fn delete<Q>(&mut self, key: &Q)
    where
        Q: CacheKey + ?Sized,
    {
        let Some(index) = self.find_index(key.hash_bytes()) else {
            return;
        };
        let _ = self.remove_index(index);
    }

    /// Removes every entry without invoking the eviction callback.
    pub fn delete_all(&mut self) {
        while self.oldest.is_some() {
            let _ = self.remove_oldest();
        }
    }

    /// Returns the current cache size (`SimpleLRUCache.Size`).
    pub fn size(&self) -> usize {
        self.entry_count()
    }

    /// Returns values in most-recently-used order.
    pub fn values(&self) -> Vec<&V> {
        self.collect_entries(|node| &node.value)
    }

    /// Returns keys in most-recently-used order.
    pub fn keys(&self) -> Vec<&K> {
        self.collect_entries(|node| &node.key)
    }

    /// Changes the maximum entry count.
    ///
    /// Shrinking removes oldest entries without invoking the callback.
    pub fn set_capacity(&mut self, capacity: usize) -> Result<(), InvalidCapacity> {
        if capacity == 0 {
            return Err(InvalidCapacity);
        }
        self.capacity = capacity;
        while self.entry_count() > self.capacity {
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

    fn find_index(&self, hash: &[u8]) -> Option<usize> {
        self.find_index_using(hash, || {
            self.find_hashed(hash, self.hash_builder.hash_one(hash))
        })
    }

    fn find_hashed(&self, hash: &[u8], hash_code: u64) -> Option<usize> {
        self.elements
            .find(hash_code, |&index| self.node(index).hash.as_ref() == hash)
            .copied()
    }

    fn find_index_using(
        &self,
        hash: &[u8],
        find_hashed: impl FnOnce() -> Option<usize>,
    ) -> Option<usize> {
        if hash.len() > LONG_KEY_THRESHOLD && self.nodes.len() <= SMALL_CACHE_SLOTS {
            return self.find_small_long_key(hash, find_hashed);
        }
        if hash.len() >= 256 {
            if hash.len() >= WIDE_KEY_THRESHOLD {
                let sample = self.wide_key_sample(hash);
                let bucket = self
                    .wide_key_samples
                    .find(sample, |bucket| bucket.sample == sample)?;
                if bucket.count == 1 {
                    let index = bucket.slot_xor;
                    return wide_keys_equal(&self.node(index).hash, hash).then_some(index);
                }
            }
            // A repeated long key can be cheaper to compare than to hash again.
            if let Some(index) = self.newest {
                let newest = &self.node(index).hash;
                if newest.last_chunk::<8>() == hash.last_chunk::<8>() && newest.as_ref() == hash {
                    return Some(index);
                }
            }
        }
        find_hashed()
    }

    // Sampled words narrow the set of candidates. Identity still depends on
    // exact equality of the stored and requested key bytes.
    fn wide_key_sample(&self, key: &[u8]) -> u64 {
        let width = key.len();
        let word = |bytes: &[u8]| u64::from_ne_bytes(*bytes.first_chunk::<8>().unwrap());
        let sample = (
            width,
            word(key),
            word(&key[width / 2..]),
            word(&key[width - 8..]),
        );
        self.hash_builder.hash_one(sample)
    }

    fn find_small_long_key(
        &self,
        hash: &[u8],
        find_hashed: impl FnOnce() -> Option<usize>,
    ) -> Option<usize> {
        // Match Go's small string map: test both ends, compare the whole key
        // for a single candidate, and hash if there are several candidates.
        let first = hash.first_chunk::<8>();
        let last = hash.last_chunk::<8>();
        let mut candidate = None;
        for (index, node) in self.nodes.iter().enumerate() {
            let Some(node) = node else { continue };
            let stored = &node.hash;
            if stored.len() == hash.len()
                && stored.first_chunk::<8>() == first
                && stored.last_chunk::<8>() == last
            {
                if candidate.is_some() {
                    return find_hashed();
                }
                candidate = Some(index);
            }
        }
        candidate.filter(|&index| self.node(index).hash.as_ref() == hash)
    }

    fn insert_new(&mut self, hash: Box<[u8]>, hash_code: u64, key: K, value: V) {
        let index = if let Some(index) = self.free.pop() {
            index
        } else {
            self.nodes.push(None);
            self.nodes.len() - 1
        };
        if hash.len() >= WIDE_KEY_THRESHOLD {
            let sample = self.wide_key_sample(&hash);
            let mut entry = self
                .wide_key_samples
                .entry(sample, |bucket| bucket.sample == sample, |bucket| bucket.sample)
                .or_insert(WideKeyBucket {
                    sample,
                    count: 0,
                    slot_xor: 0,
                });
            let bucket = entry.get_mut();
            bucket.count += 1;
            bucket.slot_xor ^= index;
        }
        self.nodes[index] = Some(Node {
            hash,
            hash_code,
            key,
            value,
            previous: None,
            next: None,
        });
        let nodes = &self.nodes;
        self.elements.insert_unique(hash_code, index, |&index| {
            nodes[index]
                .as_ref()
                .expect("table index must contain a node")
                .hash_code
        });
        self.link_front(index);
    }

    fn entry_count(&self) -> usize {
        self.elements.len()
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
        self.elements
            .find_entry(node.hash_code, |&stored| stored == index)
            .ok()
            .expect("linked LRU index must be in the table")
            .remove();
        if node.hash.len() >= WIDE_KEY_THRESHOLD {
            let sample = self.wide_key_sample(&node.hash);
            let mut entry = self
                .wide_key_samples
                .find_entry(sample, |bucket| bucket.sample == sample)
                .ok()
                .expect("wide entry must have a sample bucket");
            let bucket = entry.get_mut();
            if bucket.count == 1 {
                entry.remove();
            } else {
                bucket.count -= 1;
                bucket.slot_xor ^= index;
            }
        }
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

    fn collect_entries<T>(&self, field: impl Fn(&Node<K, V>) -> &T) -> Vec<&T> {
        let mut entries = Vec::with_capacity(self.entry_count());
        let mut next = self.newest;
        while let Some(index) = next {
            let node = self.node(index);
            entries.push(field(node));
            next = node.next;
        }
        entries
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

/// Go `GlobalLRUMemUsageTracker`: tracks all the memory usage of `SimpleLruCache`.
pub fn global_lru_mem_usage_tracker() -> &'static std::sync::Arc<tidb_memory::Tracker> {
    static TRACKER: std::sync::OnceLock<std::sync::Arc<tidb_memory::Tracker>> =
        std::sync::OnceLock::new();
    TRACKER.get_or_init(|| {
        tidb_memory::Tracker::new(tidb_memory::LABEL_FOR_GLOBAL_SIMPLE_LRU_CACHE, -1)
    })
}
