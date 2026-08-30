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

/// The two PD region statistics consumed by Go's approximate-count helper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegionCountStats {
    /// Regions intersecting the table record-key range.
    pub count: usize,
    /// PD's approximate number of keys in that range.
    pub storage_keys: i64,
}

/// Go `Helper.GetPDRegionStats(..., noIndexStats=true)`: query PD's HTTP API
/// for the memcomparable-encoded record-key range of one physical table.
pub fn load_record_region_stats(
    endpoint: &str,
    table_id: i64,
    timeout: Duration,
) -> Result<RegionCountStats, String> {
    let start = tidb_codec::gen_table_record_prefix(table_id);
    let end = tidb_txnkv::Key::from_bytes(start.clone()).prefix_next();
    let mut encoded_start = Vec::new();
    let mut encoded_end = Vec::new();
    tidb_codec::encode_bytes(&mut encoded_start, &start);
    tidb_codec::encode_bytes(&mut encoded_end, end.as_bytes());
    let url = format!(
        "{}/pd/api/v1/stats/region?start_key={}&end_key={}",
        endpoint.trim_end_matches('/'),
        go_query_escape(&encoded_start),
        go_query_escape(&encoded_end)
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async move {
        let response = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| error.to_string())?
            .get(url)
            .send()
            .await
            .map_err(|error| error.to_string())?
            .error_for_status()
            .map_err(|error| error.to_string())?;
        let value: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
        let count = value
            .get("count")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| "PD region statistics omitted count".to_owned())?;
        let storage_keys = value
            .get("storage_keys")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| "PD region statistics omitted storage_keys".to_owned())?;
        Ok(RegionCountStats {
            count,
            storage_keys,
        })
    })
}

fn go_query_escape(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut escaped = String::with_capacity(bytes.len() * 3);
    for byte in bytes.iter().copied() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            escaped.push(char::from(byte));
        } else if byte == b' ' {
            escaped.push('+');
        } else {
            escaped.push('%');
            escaped.push(char::from(HEX[usize::from(byte >> 4)]));
            escaped.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    escaped
}

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
