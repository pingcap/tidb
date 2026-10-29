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

/// The two PD region statistics consumed by Go's approximate-count helper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegionCountStats {
    /// Regions intersecting the table record-key range.
    pub count: usize,
    /// PD's approximate number of keys in that range.
    pub storage_keys: i64,
}

/// Go `Helper.GetPDRegionStats(..., noIndexStats=true)`: PD's statistics for
/// the memcomparable-encoded record-key range of one physical table, through
/// the store's PD HTTP client (`TryGetPDHTTPClient`, caller ID
/// `tidb-store-helper`) and `GetRegionStatusByKeyRange(.., false)`. The
/// request is bounded by `timeout`, the caller's statement budget.
pub async fn load_record_region_stats(
    pd_http_client: &tikv_client::pd_http::Client,
    table_id: i64,
    timeout: Duration,
) -> Result<RegionCountStats, String> {
    let start = tidb_codec::gen_table_record_prefix(table_id);
    let end = tidb_txnkv::Key::from_bytes(start.clone()).prefix_next();
    let mut encoded_start = Vec::new();
    let mut encoded_end = Vec::new();
    tidb_codec::encode_bytes(&mut encoded_start, &start);
    tidb_codec::encode_bytes(&mut encoded_end, end.as_bytes());
    let pd_cli = pd_http_client.with_caller_id("tidb-store-helper");
    let key_range = tikv_client::pd_http::new_key_range(encoded_start, encoded_end);
    let request = pd_cli.get_region_status_by_key_range(&key_range, false);
    let stats = tokio::time::timeout(timeout, request)
        .await
        .map_err(|_| "PD region statistics request timed out".to_owned())?
        .map_err(|error| error.to_string())?;
    let count = usize::try_from(stats.count).map_err(|error| error.to_string())?;
    Ok(RegionCountStats {
        count,
        storage_keys: stats.storage_keys,
    })
}


/// Go `PDHelper.GetApproximateTableCountFromStorage` on a store with no PD
/// HTTP client: a cached count is `(count, true)`; a miss is the loader's
/// `(0, false)`, cached for the next 30 seconds (Go `defaultPDHelper`'s
/// capacity and TTL).
#[derive(Debug)]
pub struct NoPdApproximateTableCounts(std::sync::Mutex<ApproximateTableCountCache>);

impl Default for NoPdApproximateTableCounts {
    fn default() -> Self {
        Self(std::sync::Mutex::new(ApproximateTableCountCache::new(
            1024 * 1024,
            Duration::from_secs(30),
        )))
    }
}

impl crate::analyze::ApproximateTableCountProvider for NoPdApproximateTableCounts {
    fn approximate_table_count(
        &self,
        _resource_group: &str,
        physical_id: i64,
        database: &str,
        table: &str,
        partition: &str,
    ) -> (f64, bool) {
        let key = approximate_table_count_key(physical_id, database, table, partition);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let mut cache = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = cache.get(&key, now) {
            return (count, true);
        }
        cache.insert(key, now, 0.0);
        (0.0, false)
    }
}
