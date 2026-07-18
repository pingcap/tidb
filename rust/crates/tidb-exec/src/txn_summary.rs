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

//! Transaction-history summary metadata from `pkg/session/txninfo/summary.go`.
//!
//! TiDB identifies a completed transaction by the FNV-1a digest of its SQL
//! digest sequence and keeps the most recent distinct sequences in an LRU
//! cache. This leaf ports that deterministic digest/LRU boundary only. JSON
//! rendering, duration filtering, mutex/global-recorder ownership, and live
//! transaction/infoschema wiring remain external.

use std::collections::{hash_map::Entry, HashMap, VecDeque};

/// One retained SQL-digest sequence and its transaction digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionSummary {
    digest: u64,
    sql_digests: Vec<String>,
}

impl TransactionSummary {
    /// Returns the FNV-1a digest used as the source cache key.
    #[must_use]
    pub const fn digest(&self) -> u64 {
        self.digest
    }

    /// Returns the SQL digest sequence in source order.
    #[must_use]
    pub fn sql_digests(&self) -> &[String] {
        &self.sql_digests
    }

    /// Returns the lower-case hexadecimal digest shown by TiDB.
    #[must_use]
    pub fn digest_hex(&self) -> String {
        format!("{:x}", self.digest)
    }
}

/// LRU cache for completed transaction SQL-digest sequences.
#[derive(Clone, Debug, Default)]
pub struct TransactionSummaryCache {
    capacity: usize,
    entries: HashMap<u64, TransactionSummary>,
    /// Keys from most recently used to least recently used.
    lru: VecDeque<u64>,
}

impl TransactionSummaryCache {
    /// Creates an empty cache with the source capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    /// Returns the configured maximum number of distinct summaries.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Records a completed transaction and updates its LRU position.
    pub fn on_transaction_end<I, S>(&mut self, sql_digests: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let sql_digests: Vec<String> = sql_digests
            .into_iter()
            .map(|digest| digest.as_ref().to_owned())
            .collect();
        let key = fnv1a_digest(&sql_digests);

        let is_new = match self.entries.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(TransactionSummary {
                    digest: key,
                    sql_digests,
                });
                true
            }
            Entry::Occupied(_) => false,
        };
        if !is_new {
            self.remove_lru_key(key);
        }
        self.lru.push_front(key);

        if self.lru.len() > self.capacity {
            if let Some(evicted) = self.lru.pop_back() {
                self.entries.remove(&evicted);
            }
        }
    }

    /// Resizes the cache and evicts least-recently-used entries as needed.
    pub fn resize(&mut self, capacity: usize) {
        self.capacity = capacity;
        while self.lru.len() > self.capacity {
            if let Some(evicted) = self.lru.pop_back() {
                self.entries.remove(&evicted);
            }
        }
    }

    /// Returns summaries from most recently used to least recently used.
    #[must_use]
    pub fn summaries(&self) -> Vec<TransactionSummary> {
        self.lru
            .iter()
            .filter_map(|key| self.entries.get(key).cloned())
            .collect()
    }

    fn remove_lru_key(&mut self, key: u64) {
        if let Some(index) = self.lru.iter().position(|candidate| *candidate == key) {
            self.lru.remove(index);
        }
    }
}

/// Computes the source FNV-1a 64-bit digest over concatenated SQL digests.
fn fnv1a_digest(sql_digests: &[String]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for digest in sql_digests {
        for byte in digest.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3_u64);
        }
    }
    hash
}
