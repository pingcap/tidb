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

//! Dependency-closed approximate table-count cache from `pd.go`.
//!
//! The Go `PDHelper` derives one underscore-delimited table/partition key,
//! returns cached values with `hasPD = true`, and otherwise invokes its PD or
//! restricted-SQL loader before inserting the value with a bounded TTL cache.
//! This leaf preserves that key, hit/miss, TTL, and LRU-capacity contract while
//! leaving the wall clock, PD/storage client, SQL executor, and global helper
//! lifecycle to the executor/session boundary.

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

    /// Looks up a key or loads/inserts one value on a miss.
    ///
    /// `now` is supplied by the caller so this state can be tested without
    /// sleeping and can share the session's statement clock when integrated.
    pub fn get_or_load<F>(&mut self, key: impl Into<String>, now: Duration, load: F) -> (f64, bool)
    where
        F: FnOnce() -> (f64, bool),
    {
        let key = key.into();
        if let Some(entry) = self.entries.get(&key).copied() {
            if now < entry.expires_at {
                self.touch(&key);
                return (entry.value, true);
            }
            self.entries.remove(&key);
            self.remove_from_lru(&key);
        }

        let (value, has_pd) = load();
        if self.capacity != 0 {
            self.remove_from_lru(&key);
            while self.entries.len() >= self.capacity {
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
        (value, has_pd)
    }

    /// Runs the source key derivation and cache lookup for one table.
    pub fn get_or_load_table<F>(
        &mut self,
        now: Duration,
        table_id: i64,
        db_name: &str,
        table_name: &str,
        partition_name: &str,
        load: F,
    ) -> (f64, bool)
    where
        F: FnOnce() -> (f64, bool),
    {
        let key = approximate_table_count_key(table_id, db_name, table_name, partition_name);
        self.get_or_load(key, now, load)
    }

    /// Returns the number of currently stored entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no entries are currently stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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
