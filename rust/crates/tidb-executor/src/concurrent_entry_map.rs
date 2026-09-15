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

//! Dependency-closed sharded entry map from
//! `pkg/executor/join/concurrent_map.go`.
//!
//! TiDB's hash join map shards a `uint64 -> *entry` chain across 320 buckets.
//! Inserts prepend to the key's chain, reads return the chain head, and
//! iteration holds each shard's read lock while visiting it. This leaf keeps
//! that ownership and chain behavior typed. TiDB's `hack.MemAwareMap` ABI,
//! exact Go allocator byte counts, hash-join row containers, and caller-side
//! memory trackers remain external.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

/// Number of map shards in the source implementation.
pub const SHARD_COUNT: usize = 320;

/// Row identity carried by one hash-join entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RowPointer {
    /// Source chunk index.
    pub chunk_index: u32,
    /// Source row index within the chunk.
    pub row_index: u32,
}

/// One linked entry returned by a key lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
    /// Row identity stored by this entry.
    pub pointer: RowPointer,
    /// Older entry for the same hash key.
    pub next: Option<Box<Self>>,
}

impl Entry {
    /// Iterates this chain from newest to oldest insertion.
    pub fn iter(&self) -> impl Iterator<Item = RowPointer> + '_ {
        std::iter::successors(Some(self), |entry| entry.next.as_deref()).map(|entry| entry.pointer)
    }
}

/// A fixed-shard, lock-protected hash-key-to-entry-chain map.
pub struct ConcurrentEntryMap {
    shards: Vec<RwLock<BTreeMap<u64, Vec<RowPointer>>>>,
    entries: AtomicUsize,
}

impl Default for ConcurrentEntryMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrentEntryMap {
    /// Creates all source-sized shards empty.
    #[must_use]
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(RwLock::new(BTreeMap::new()));
        }
        Self {
            shards,
            entries: AtomicUsize::new(0),
        }
    }

    /// Prepends a row pointer to the key's chain and returns its accounting
    /// delta. Existing values are retained in source insertion order.
    pub fn insert(&self, key: u64, pointer: RowPointer) -> usize {
        let shard = &self.shards[self.shard_index(key)];
        let mut values = shard.write().expect("concurrent entry map shard poisoned");
        values.entry(key).or_default().insert(0, pointer);
        self.entries.fetch_add(1, Ordering::Relaxed);
        size_of::<RowPointer>()
    }

    /// Returns the newest entry and whether the key existed.
    #[must_use]
    pub fn get(&self, key: u64) -> (Option<Entry>, bool) {
        let shard = &self.shards[self.shard_index(key)];
        let values = shard.read().expect("concurrent entry map shard poisoned");
        let Some(values) = values.get(&key) else {
            return (None, false);
        };
        (Some(Self::chain(values)), true)
    }

    /// Returns a snapshot of every key and its newest-to-oldest chain.
    #[must_use]
    pub fn snapshot(&self) -> Vec<(u64, Entry)> {
        let mut result = Vec::new();
        for shard in &self.shards {
            let values = shard.read().expect("concurrent entry map shard poisoned");
            result.extend(
                values
                    .iter()
                    .map(|(&key, entries)| (key, Self::chain(entries))),
            );
        }
        result
    }

    /// Returns the number of inserted entries, including duplicate keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.load(Ordering::Relaxed)
    }

    /// Returns whether no entries have been inserted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns deterministic leaf accounting for inserted row pointers.
    ///
    /// Exact `MemAwareMap.Bytes`/`RealBytes` values depend on Go's map ABI and
    /// are intentionally not represented here.
    #[must_use]
    pub fn estimated_memory_bytes(&self) -> usize {
        self.len() * size_of::<RowPointer>()
    }

    fn shard_index(&self, key: u64) -> usize {
        (key % SHARD_COUNT as u64) as usize
    }

    fn chain(values: &[RowPointer]) -> Entry {
        let mut head = None;
        for &pointer in values.iter().rev() {
            head = Some(Box::new(Entry {
                pointer,
                next: head,
            }));
        }
        *head.expect("stored entry chain is never empty")
    }
}
