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

//! The approximate table-count cache of Go
//! `pkg/executor/internal/pdhelper/pd.go`.
//!
//! The Go `PDHelper` derives one underscore-delimited table/partition key,
//! returns cached values with `hasPD = true`, and otherwise invokes its PD or
//! restricted-SQL loader before inserting the value with a bounded TTL cache.
//! This leaf preserves that key, hit/miss, TTL, and LRU-capacity contract while
//! leaving the wall clock, PD/storage client and SQL executor to its callers:
//! the cluster node's PD-backed provider and the in-process catalog's
//! PD-less store.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

/// Builds the source approximate-count cache key.
///
/// This intentionally uses the source's direct underscore join. Escaping or
/// normalizing names would create a different cache identity and belongs to a
/// higher-level identifier contract.
#[must_use]
pub fn approximate_table_count_key(
    table_id: i64,
    db_name: &str,
    table_name: &str,
    partition_name: &str,
) -> String {
    format!("{table_id}_{db_name}_{table_name}_{partition_name}")
}

#[derive(Clone, Copy, Debug)]
struct CacheEntry {
    value: f64,
    expires_at: Duration,
}

/// Bounded TTL/LRU cache for already-computed approximate table counts.
///
/// Callers pass the current timestamp to keep the system clock outside this
/// value owner. A cache hit returns `(value, true)`, matching `PDHelper`'s
/// source path; the loader's own `has_pd` result is returned only on a miss.
#[derive(Clone, Debug)]
pub struct ApproximateTableCountCache {
    capacity: usize,
    ttl: Duration,
    entries: HashMap<String, CacheEntry>,
    lru: VecDeque<String>,
}

impl ApproximateTableCountCache {
    /// Creates an empty cache with the source capacity and TTL policy.
    #[must_use]
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            capacity,
            ttl,
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    /// Reads one unexpired cached value and refreshes its LRU position.
    pub fn get(&mut self, key: &str, now: Duration) -> Option<f64> {
        let entry = self.entries.get(key).copied()?;
        if now >= entry.expires_at {
            self.entries.remove(key);
            self.remove_from_lru(key);
            return None;
        }
        self.touch(key);
        Some(entry.value)
    }

    /// Inserts one freshly loaded value with this cache's TTL and capacity.
    pub fn insert(&mut self, key: String, now: Duration, value: f64) {
        if self.capacity == 0 {
            return;
        }
        self.remove_from_lru(&key);
        while self.entries.len() >= self.capacity && !self.entries.contains_key(&key) {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
        let expires_at = now.checked_add(self.ttl).unwrap_or(Duration::MAX);
        self.entries
            .insert(key.clone(), CacheEntry { value, expires_at });
        self.lru.push_back(key);
    }

    /// Deletes every entry whose TTL has elapsed.
    pub fn delete_expired(&mut self, now: Duration) {
        self.entries.retain(|_, entry| now < entry.expires_at);
        self.lru.retain(|key| self.entries.contains_key(key));
    }

    /// Returns how long the cleanup worker should wait for the next expiry.
    #[must_use]
    pub fn next_expiration_delay(&self, now: Duration) -> Duration {
        self.entries
            .values()
            .map(|entry| entry.expires_at.saturating_sub(now))
            .min()
            .unwrap_or(self.ttl)
    }

    fn touch(&mut self, key: &str) {
        self.remove_from_lru(key);
        self.lru.push_back(key.to_owned());
    }

    fn remove_from_lru(&mut self, key: &str) {
        if let Some(index) = self.lru.iter().position(|entry| entry == key) {
            self.lru.remove(index);
        }
    }
}
