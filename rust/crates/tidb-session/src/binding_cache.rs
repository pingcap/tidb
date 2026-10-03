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

//! Go bindingCache and its live incremental updater. The shared Ristretto
//! implementation owns admission, publication, callbacks and shutdown. The
//! digest bi-map and storage watermark remain binding-owner responsibilities.
//! This is a finding repair, not whole-package acceptance of pkg/bindinfo.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tidb_executor::DriverError;

use crate::binding::{cross_db_match, Binding};

/// Go `Binding.size()` (`pkg/bindinfo/binding.go:97`): the byte cost this
/// cache charges for one binding.
///
/// `2*unsafe.Sizeof(b.CreateTime)` is `2*16` -- `types.Time` is a 16-byte
/// struct -- and `len(b.ID)` is 0 because [`Binding`] carries no `ID` field
/// (it is empty for every binding this tier builds). Go returns `float64` and
/// the ristretto `Cost` closure immediately casts to `int64`; the cast is
/// folded in here.
#[must_use]
pub fn binding_size(binding: &Binding) -> i64 {
    (binding.original_sql.len()
        + binding.db.len()
        + binding.bind_sql.len()
        + binding.status.len()
        + 2 * 16
        + binding.charset.len()
        + binding.collation.len()) as i64
}

/// Go `digestBiMap` (lines 168-185): a bidirectional map between `noDBDigest`
/// and `sqlDigest`, the index that makes cross-db binding lookup possible.
///
/// One `noDBDigest` maps to MANY `sqlDigest`s, but one `sqlDigest` maps to
/// exactly one `noDBDigest`.
pub trait DigestBiMap {
    /// Go `Add`. `no_db_digest` is the digest computed after eliminating all
    /// DB names (`select * from test.t` -> `select * from t` -> digest);
    /// `sql_digest` is the digest with DB names kept.
    fn add(&mut self, no_db_digest: &str, sql_digest: &str);

    /// Go `Del`.
    fn del(&mut self, sql_digest: &str);

    /// Go `All`: every `sqlDigest`. Sorted here; see the module narrowings.
    fn all(&self) -> Vec<String>;

    /// Go `NoDBDigest2SQLDigest`.
    fn no_db_digest_to_sql_digest(&self, no_db_digest: &str) -> &[String];

    /// Go `SQLDigest2NoDBDigest`. Go returns `""` for an absent key; `None`
    /// is that same answer without conflating it with a stored empty digest.
    fn sql_digest_to_no_db_digest(&self, sql_digest: &str) -> Option<&str>;
}

/// Go `digestBiMapImpl` (lines 187-198), minus its `sync.RWMutex`.
#[derive(Debug, Default)]
pub struct DigestBiMapImpl {
    no_db_digest_to_sql_digest: HashMap<String, Vec<String>>,
    sql_digest_to_no_db_digest: HashMap<String, String>,
}

impl DigestBiMapImpl {
    /// Go `newDigestBiMap`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `len(b.noDBDigest2SQLDigest)`, which the Go tests read directly off the
    /// concrete struct.
    #[must_use]
    pub fn no_db_digest_count(&self) -> usize {
        self.no_db_digest_to_sql_digest.len()
    }

    /// `len(b.sqlDigest2noDBDigest)`, likewise read directly by the Go tests.
    #[must_use]
    pub fn sql_digest_count(&self) -> usize {
        self.sql_digest_to_no_db_digest.len()
    }

    /// The `noDBDigest` keys, sorted. Go's tests iterate the raw map.
    #[must_use]
    pub fn no_db_digests(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.no_db_digest_to_sql_digest.keys().cloned().collect();
        keys.sort();
        keys
    }
}

impl DigestBiMap for DigestBiMapImpl {
    fn add(&mut self, no_db_digest: &str, sql_digest: &str) {
        let list = self
            .no_db_digest_to_sql_digest
            .entry(no_db_digest.to_owned())
            .or_default();
        // Go's explicit scan: avoid adding duplicated binding digests.
        if !list.iter().any(|d| d == sql_digest) {
            list.push(sql_digest.to_owned());
        }
        self.sql_digest_to_no_db_digest
            .insert(sql_digest.to_owned(), no_db_digest.to_owned());
    }

    fn del(&mut self, sql_digest: &str) {
        let Some(no_db_digest) = self.sql_digest_to_no_db_digest.remove(sql_digest) else {
            return;
        };
        if let Some(list) = self.no_db_digest_to_sql_digest.get_mut(&no_db_digest) {
            // Go: "Deleting binding is a low-frequently operation, so the O(n)
            // performance is enough."
            if let Some(at) = list.iter().position(|d| d == sql_digest) {
                list.remove(at);
            }
            if list.is_empty() {
                self.no_db_digest_to_sql_digest.remove(&no_db_digest);
            }
        }
    }

    fn all(&self) -> Vec<String> {
        let mut digests: Vec<String> = self.sql_digest_to_no_db_digest.keys().cloned().collect();
        digests.sort();
        digests
    }

    fn no_db_digest_to_sql_digest(&self, no_db_digest: &str) -> &[String] {
        self.no_db_digest_to_sql_digest
            .get(no_db_digest)
            .map_or(&[][..], Vec::as_slice)
    }

    fn sql_digest_to_no_db_digest(&self, sql_digest: &str) -> Option<&str> {
        self.sql_digest_to_no_db_digest
            .get(sql_digest)
            .map(String::as_str)
    }
}

/// Go's reject/evict observer, also used by the original tests.
pub type EvictCallback = Arc<dyn Fn(&Binding) + Send + Sync>;

#[derive(Default)]
struct BindingCallbacks {
    closed: std::sync::atomic::AtomicBool,
    observer: RwLock<Option<EvictCallback>>,
}
impl BindingCallbacks {
    fn rejected_or_evicted(&self, item: &tidb_ristretto::Item<Arc<Binding>>) {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let Some(binding) = &item.value else {
            return;
        };
        let observer = self.observer.read().unwrap().clone();
        if let Some(observer) = observer {
            observer(binding);
        }
        tidb_util::logutil::bg_logger().warn(
            &format!("binding cache memory limit reached, evict or reject binding: sqlDigest={}, bindSQL={}", binding.sql_digest, binding.bind_sql), &[],
        );
    }
}
impl tidb_ristretto::Callback<Arc<Binding>> for BindingCallbacks {
    fn on_evict(&self, item: &tidb_ristretto::Item<Arc<Binding>>) {
        self.rejected_or_evicted(item);
    }
    fn on_reject(&self, item: &tidb_ristretto::Item<Arc<Binding>>) {
        self.rejected_or_evicted(item);
    }
}

/// Every planner and refresh worker holds the same live cache owner.
#[derive(Clone)]
pub struct SharedBindingCache(Arc<BindingCache>);
impl Default for SharedBindingCache {
    fn default() -> Self {
        Self(Arc::new(BindingCache::new(
            tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_BINDING_CACHE,
        )))
    }
}
impl SharedBindingCache {
    /// Retain the live owner. Individual binding Arcs survive replacement.
    pub fn load(&self) -> Arc<BindingCache> {
        Arc::clone(&self.0)
    }
}

/// Go bindingCache and bindingCacheUpdater's shared state. The digest index
/// deliberately may outlive entries evicted by Ristretto.
pub struct BindingCache {
    digest_bi_map: RwLock<DigestBiMapImpl>,
    store: tidb_ristretto::Cache<Arc<Binding>>,
    callbacks: Arc<BindingCallbacks>,
    last_update_time: std::sync::Mutex<String>,
}
impl BindingCache {
    /// Go newBindingCache: independent budget, deferred Binding.size cost,
    /// policy metrics, no internal cost, and reject/evict logging.
    #[must_use]
    pub fn new(max_cost: i64) -> Self {
        let callbacks = Arc::new(BindingCallbacks::default());
        let mut config = tidb_ristretto::Config::new(1_000_000, max_cost, 64);
        config.cost = Some(Arc::new(|binding: &Arc<Binding>| binding_size(binding)));
        config.ignore_internal_cost = true;
        config.metrics = true;
        config.callback = callbacks.clone();
        Self {
            digest_bi_map: RwLock::new(DigestBiMapImpl::new()),
            store: tidb_ristretto::Cache::new(config).expect("binding cache configuration"),
            callbacks,
            last_update_time: std::sync::Mutex::new(String::new()),
        }
    }
    /// Initial load uses the same update path as subsequent refreshes.
    pub fn from_storage_rows(rows: Vec<Vec<tidb_datatype::Datum>>, max_cost: i64) -> Self {
        let cache = Self::new(max_cost);
        cache.update_storage_rows(rows, max_cost, true);
        cache
    }
    /// Go's ten-second clock-skew tolerance, expressed in the loader session's
    /// TIMESTAMP domain. None means the initial/full storage load.
    pub fn update_time_boundary(&self) -> Option<String> {
        let last = self.last_update_time.lock().unwrap();
        binding_time(&last).map(|last| {
            (last - chrono::Duration::seconds(10))
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string()
        })
    }
    /// Highest update_time returned by the validated storage loader.
    pub fn last_update_time(&self) -> String {
        self.last_update_time.lock().unwrap().clone()
    }
    /// Apply committed storage rows without replacing the live cache or its
    /// frequency sketch. Equal-version reloads preserve the old binding Arc,
    /// including usage timestamps. A same-version live row beats a tombstone.
    pub fn update_storage_rows(
        &self,
        mut rows: Vec<Vec<tidb_datatype::Datum>>,
        max_cost: i64,
        full_load: bool,
    ) {
        self.set_mem_capacity(max_cost);
        let mut last = self.last_update_time.lock().unwrap();
        if full_load {
            last.clear();
        }
        let boundary = if full_load {
            None
        } else {
            binding_time(&last).map(|v| v - chrono::Duration::seconds(10))
        };
        rows.sort_by_key(|row| {
            (
                row.get(5).and_then(crate::datum_text),
                row.get(4).and_then(crate::datum_text),
            )
        });
        for row in rows {
            let updated = row.get(5).and_then(crate::datum_text).unwrap_or_default();
            if boundary.is_some_and(|boundary| {
                binding_time(&updated).is_some_and(|value| value <= boundary)
            }) {
                continue;
            }
            if row.first().and_then(crate::datum_text).as_deref()
                == Some(crate::binding_utils::BUILTIN_PSEUDO_SQL_4_BIND_LOCK)
            {
                continue;
            }
            let Some(binding) = Binding::from_storage_row(&row, tidb_parser::SqlMode::default())
            else {
                continue;
            };
            if binding_time(&updated) > binding_time(&last) {
                *last = updated;
            }
            let old = self.get_binding(&binding.sql_digest);
            let keep_old = old.as_ref().is_some_and(|old| {
                binding_time(&old.update_time) >= binding_time(&binding.update_time)
            });
            if keep_old {
                self.insert_prepared(old.unwrap());
                continue;
            }
            if binding.status == crate::binding::STATUS_DELETED {
                self.remove_binding(&binding.sql_digest);
            } else {
                self.insert_prepared(Arc::new(binding));
            }
        }
        crate::metrics::BINDING_CACHE_MEM_USAGE.set(self.mem_usage() as f64);
        crate::metrics::BINDING_CACHE_MEM_LIMIT.set(self.mem_capacity() as f64);
        crate::metrics::BINDING_CACHE_NUM_BINDINGS.set(self.get_all_bindings().len() as f64);
    }
    /// Install the original Go test observer.
    pub fn set_evict_callback(&self, callback: EvictCallback) {
        *self.callbacks.observer.write().unwrap() = Some(callback);
    }
    /// Read the Go digest index under its owner lock.
    pub fn digest_bi_map(&self) -> std::sync::RwLockReadGuard<'_, DigestBiMapImpl> {
        self.digest_bi_map.read().unwrap()
    }
    /// Retain candidates while matching, so eviction/reload cannot invalidate
    /// references in a running planner.
    pub fn matching_binding(
        &self,
        no_db_digest: &str,
        table_names: &[(String, String)],
        current_db: &str,
        fuzzy_enabled: bool,
    ) -> Option<Arc<Binding>> {
        if self.size() == 0 {
            return None;
        }
        let digests = self
            .digest_bi_map
            .read()
            .unwrap()
            .no_db_digest_to_sql_digest(no_db_digest)
            .to_vec();
        let candidates: Vec<_> = digests
            .iter()
            .filter_map(|digest| self.get_binding(digest))
            .collect();
        let matched = cross_db_match(
            candidates.iter().map(Arc::as_ref),
            table_names,
            current_db,
            fuzzy_enabled,
        )?;
        candidates
            .iter()
            .find(|candidate| std::ptr::eq(candidate.as_ref(), matched))
            .cloned()
    }
    /// Go GetBinding, including a read-frequency observation on every access.
    pub fn get_binding(&self, digest: &str) -> Option<Arc<Binding>> {
        self.store.get(digest)
    }
    /// Go GetAllBindings, skipping evicted digests.
    pub fn get_all_bindings(&self) -> Vec<Arc<Binding>> {
        self.digest_bi_map
            .read()
            .unwrap()
            .all()
            .into_iter()
            .filter_map(|digest| self.store.get(digest.as_str()))
            .collect()
    }
    /// Go SetBinding parses its bound SQL, then issues Set and Wait.
    pub fn set_binding(&self, digest: &str, mut binding: Binding) -> Result<(), DriverError> {
        let stmt = tidb_parser::parse(&binding.bind_sql).map_err(|error| {
            DriverError::unsupported(format!(
                "cannot parse binding SQL {:?}: {}",
                binding.bind_sql,
                error.compatibility_message(&binding.bind_sql)
            ))
        })?;
        binding.no_db_digest = crate::binding::no_db_digest(&stmt);
        binding.sql_digest = digest.to_owned();
        self.insert_prepared(Arc::new(binding));
        Ok(())
    }
    fn insert_prepared(&self, binding: Arc<Binding>) {
        let digest = binding.sql_digest.clone();
        self.digest_bi_map
            .write()
            .unwrap()
            .add(&binding.no_db_digest, &digest);
        self.store.set(digest.as_str(), binding, 0);
        self.store.wait();
    }
    /// Go RemoveBinding. Ordered deletion retires pending writes too.
    pub fn remove_binding(&self, digest: &str) {
        self.digest_bi_map.write().unwrap().del(digest);
        self.store.del(digest);
    }
    /// Go SetMemCapacity does not immediately evict residents.
    pub fn set_mem_capacity(&self, capacity: i64) {
        self.store.update_max_cost(capacity);
    }
    /// Go policy cost accounting, independent of the digest index.
    pub fn mem_usage(&self) -> i64 {
        self.store
            .metrics()
            .cost_added()
            .wrapping_sub(self.store.metrics().cost_evicted()) as i64
    }
    /// Current budget.
    pub fn mem_capacity(&self) -> i64 {
        self.store.max_cost()
    }
    /// Go policy resident-key accounting.
    pub fn size(&self) -> usize {
        self.store
            .metrics()
            .keys_added()
            .wrapping_sub(self.store.metrics().keys_evicted()) as usize
    }
    /// Suppress eviction logs before joining both cache workers.
    pub fn close(&self) {
        self.callbacks
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        self.store.close();
    }
}
impl Drop for BindingCache {
    fn drop(&mut self) {
        self.close();
    }
}

fn binding_time(value: &str) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").ok()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Go `bindingNoDBDigest` (`binding_cache_test.go:30`).
    fn binding_no_db_digest(bind_sql: &str) -> String {
        let stmt = tidb_parser::parse(bind_sql).expect("binding SQL parses");
        crate::binding::no_db_digest(&stmt)
    }

    fn binding(bind_sql: &str, sql_digest: &str) -> Binding {
        Binding {
            bind_sql: bind_sql.to_owned(),
            sql_digest: sql_digest.to_owned(),
            ..Binding::default()
        }
    }

    #[test]
    fn stored_binding_versions_update_the_live_owner() {
        use tidb_datatype::Datum;
        let row = |status: &str, updated: &str| {
            [
                "select * from test.t",
                "SELECT * FROM test.t",
                "test",
                status,
                "2026-09-07 00:00:00",
                updated,
                "utf8mb4",
                "utf8mb4_bin",
                "manual",
                "digest",
            ]
            .into_iter()
            .map(|value| Datum::new_string(value.to_owned()))
            .collect::<Vec<_>>()
        };
        let old = "2026-09-07 00:00:00";
        let new = "2026-09-07 00:00:01";
        for reversed in [false, true] {
            for (versions, expected) in [
                (vec![row("enabled", old), row("deleted", new)], None),
                (
                    vec![row("deleted", old), row("enabled", new)],
                    Some("enabled"),
                ),
                (
                    vec![row("deleted", new), row("enabled", new)],
                    Some("enabled"),
                ),
                (
                    vec![row("enabled", old), row("disabled", new)],
                    Some("disabled"),
                ),
            ] {
                let mut versions = versions;
                if reversed {
                    versions.reverse();
                }
                let cache = BindingCache::from_storage_rows(versions, 1_000_000);
                assert_eq!(
                    cache.get_binding("digest").map(|binding| binding.status),
                    expected
                );
            }
        }
        let shared = SharedBindingCache::default();
        shared
            .load()
            .update_storage_rows(vec![row("enabled", old)], 1_000_000, true);
        let pinned = shared.load();
        shared
            .load()
            .update_storage_rows(vec![row("deleted", new)], 1_000_000, true);
        assert!(Arc::ptr_eq(&pinned, &shared.load()));
        pinned.store.wait();
        assert_eq!(pinned.size(), 0);
        assert_eq!(shared.load().size(), 0);
        assert_eq!(
            BindingCache::from_storage_rows(vec![row("enabled", old)], 1).size(),
            0
        );
        let mut invalid = row("enabled", new);
        invalid[1] = Datum::new_string("this is not SQL".to_owned());
        assert_eq!(
            BindingCache::from_storage_rows(vec![invalid], 1_000_000).size(),
            0
        );
    }

    #[test]
    fn frequently_read_binding_survives_cold_admission() {
        let one = binding("SELECT * FROM t1", "");
        let cache = BindingCache::new(binding_size(&one));
        cache.set_binding("hot", one.clone()).unwrap();
        for _ in 0..1024 {
            assert!(cache.get_binding("hot").is_some());
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        cache.set_binding("cold", one).unwrap();
        assert!(
            cache.get_binding("hot").is_some(),
            "hot binding lost to a never-read replacement"
        );
        assert!(cache.get_binding("cold").is_none());
    }

    #[test]
    fn incremental_refresh_keeps_references_watermark_and_read_history() {
        use tidb_datatype::Datum;
        let row = |digest: &str, status: &str, updated: &str| {
            [
                "select * from test.t",
                "SELECT * FROM test.t",
                "test",
                status,
                "2026-10-03 00:00:00.000000",
                updated,
                "utf8mb4",
                "utf8mb4_bin",
                "manual",
                digest,
                "plan",
            ]
            .into_iter()
            .map(|value| Datum::new_string(value.to_owned()))
            .collect::<Vec<_>>()
        };
        let quota = 1_000_000;
        let cache = BindingCache::from_storage_rows(
            vec![row("one", "enabled", "2026-10-03 00:00:20.000000")],
            quota,
        );
        let pinned = cache.get_binding("one").unwrap();
        pinned.mark_used();
        assert_eq!(
            cache.update_time_boundary().as_deref(),
            Some("2026-10-03 00:00:10.000000")
        );
        cache.update_storage_rows(
            vec![
                row("one", "enabled", "2026-10-03 00:00:20.000000"),
                row("skew", "enabled", "2026-10-03 00:00:11.000000"),
                row("outside", "enabled", "2026-10-03 00:00:10.000000"),
            ],
            quota,
            false,
        );
        assert!(Arc::ptr_eq(&pinned, &cache.get_binding("one").unwrap()));
        assert!(cache
            .get_binding("one")
            .unwrap()
            .usage_snapshot()
            .last_used_at
            .is_some());
        assert!(cache.get_binding("skew").is_some());
        assert!(cache.get_binding("outside").is_none());
        cache.update_storage_rows(
            vec![row("one", "deleted", "2026-10-03 00:00:21.000000")],
            quota,
            false,
        );
        assert!(cache.get_binding("one").is_none());
        assert_eq!(pinned.status, "enabled");
        assert_eq!(cache.last_update_time(), "2026-10-03 00:00:21.000000");
    }

    /// Go `TestCrossDBBindingCache`.
    #[test]
    fn cross_db_binding_cache() {
        let cache = BindingCache::new(1_000_000_000);
        let b1 = binding("SELECT * FROM db1.t1", "b1");
        let digest1 = binding_no_db_digest(&b1.bind_sql);
        let b2 = binding("SELECT * FROM db2.t1", "b2");
        let b3 = binding("SELECT * FROM db2.t3", "b3");
        let digest3 = binding_no_db_digest(&b3.bind_sql);

        // add 3 bindings; b1 and b2 have the same noDBDigest
        cache.set_binding("b1", b1).unwrap();
        cache.set_binding("b2", b2).unwrap();
        cache.set_binding("b3", b3).unwrap();
        assert_eq!(cache.digest_bi_map().no_db_digest_count(), 2);
        assert_eq!(
            cache
                .digest_bi_map()
                .no_db_digest_to_sql_digest(&digest1)
                .len(),
            2
        );
        assert_eq!(
            cache
                .digest_bi_map()
                .no_db_digest_to_sql_digest(&digest3)
                .len(),
            1
        );
        assert_eq!(cache.digest_bi_map().sql_digest_count(), 3);
        for digest in ["b1", "b2", "b3"] {
            assert!(cache
                .digest_bi_map()
                .sql_digest_to_no_db_digest(digest)
                .is_some());
        }

        // remove b2
        cache.remove_binding("b2");
        assert_eq!(cache.digest_bi_map().no_db_digest_count(), 2);
        assert_eq!(
            cache
                .digest_bi_map()
                .no_db_digest_to_sql_digest(&digest1)
                .len(),
            1
        );
        assert_eq!(
            cache
                .digest_bi_map()
                .no_db_digest_to_sql_digest(&digest3)
                .len(),
            1
        );
        assert_eq!(cache.digest_bi_map().sql_digest_count(), 2);
        assert!(cache
            .digest_bi_map()
            .sql_digest_to_no_db_digest("b1")
            .is_some());
        // can't find b2 now
        assert!(cache
            .digest_bi_map()
            .sql_digest_to_no_db_digest("b2")
            .is_none());
        assert!(cache
            .digest_bi_map()
            .sql_digest_to_no_db_digest("b3")
            .is_some());
    }

    /// Go `TestDuplicatedBinding`.
    #[test]
    fn duplicated_binding() {
        // 3 bindings with the same noDBDigest
        let db1 = binding("SELECT * FROM db1.t1", "");
        let db2 = binding("SELECT * FROM db2.t1", "");
        let db3 = binding("SELECT * FROM db3.t1", "");
        let cache = BindingCache::new(1_000_000_000);
        cache.set_binding("db1", db1.clone()).unwrap();
        cache.set_binding("db2", db2.clone()).unwrap();
        cache.set_binding("db3", db3.clone()).unwrap();

        let no_db_digests = cache.digest_bi_map().no_db_digests();
        assert_eq!(no_db_digests.len(), 1);
        let no_db_digest = no_db_digests[0].clone();
        assert!(!no_db_digest.is_empty());
        assert_eq!(
            cache
                .digest_bi_map()
                .no_db_digest_to_sql_digest(&no_db_digest)
                .len(),
            3
        );
        assert_eq!(cache.digest_bi_map().sql_digest_count(), 3);

        // put 3 duplicated bindings again
        cache.set_binding("db1", db1).unwrap();
        cache.set_binding("db2", db2).unwrap();
        cache.set_binding("db3", db3).unwrap();
        assert_eq!(
            cache
                .digest_bi_map()
                .no_db_digest_to_sql_digest(&no_db_digest)
                .len(),
            3
        );
        assert_eq!(cache.digest_bi_map().sql_digest_count(), 3);
    }

    /// Go `TestBindCache`.
    ///
    /// Preserve Go's resident count; no particular probabilistic victim is promised.
    #[test]
    fn bind_cache() {
        let one = binding("SELECT * FROM t1", "");
        let kv_size = binding_size(&one);
        assert_eq!(kv_size, 48);
        let cache = BindingCache::new(kv_size * 3 - 1);

        cache.set_binding("digest1", one.clone()).unwrap();
        assert!(cache.get_binding("digest1").is_some());
        cache.set_binding("digest2", one.clone()).unwrap();
        assert!(cache.get_binding("digest2").is_some());
        cache.set_binding("digest3", one).unwrap();
        assert!(cache.get_binding("digest3").is_some());

        let hit = ["digest1", "digest2", "digest3"]
            .into_iter()
            .filter(|digest| cache.get_binding(digest).is_some())
            .count();
        assert_eq!(hit, 2);

        cache.close();
        assert_eq!(cache.size(), 0);
    }

    /// Go `TestBindingCacheEvictLog`.
    #[test]
    fn binding_cache_evict_log() {
        let callback_count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&callback_count);

        let large = binding(
            &format!("SELECT * FROM t1 WHERE c = '{}'", "a".repeat(200)),
            "",
        );
        let one = binding("SELECT * FROM t1", "");
        let cache = BindingCache::new(binding_size(&one) * 3 - 1);
        cache.set_evict_callback(Arc::new(move |_binding: &Binding| {
            counter.fetch_add(1, Ordering::Relaxed);
        }));

        cache.set_binding("0", large.clone()).unwrap();
        assert_eq!(callback_count.load(Ordering::Relaxed), 1); // large binding, reject directly
        cache.set_binding("0", large).unwrap();
        assert_eq!(callback_count.load(Ordering::Relaxed), 2); // large binding, reject directly
        callback_count.store(0, Ordering::Relaxed); // reset callback count

        cache.set_binding("1", one.clone()).unwrap(); // insert the first binding four times
        cache.set_binding("1", one.clone()).unwrap();
        cache.set_binding("1", one.clone()).unwrap();
        cache.set_binding("1", one.clone()).unwrap();
        assert_eq!(cache.size(), 1);
        assert_eq!(cache.mem_usage(), binding_size(&one));
        assert_eq!(callback_count.load(Ordering::Relaxed), 0); // duplicated binding should not trigger eviction

        cache.set_binding("2", one.clone()).unwrap(); // insert the second binding
        cache.set_binding("2", one.clone()).unwrap();
        assert_eq!(callback_count.load(Ordering::Relaxed), 0); // cache size is enough

        cache.set_binding("3", one.clone()).unwrap(); // insert the third binding, triggers eviction
        assert_eq!(callback_count.load(Ordering::Relaxed), 1);

        for i in 1..=10 {
            cache.set_binding(&format!("3-{i}"), one.clone()).unwrap();
            assert_eq!(callback_count.load(Ordering::Relaxed), 1 + i);
        }

        assert_eq!(callback_count.load(Ordering::Relaxed), 11);
        cache.close(); // close doesn't trigger eviction log
        assert_eq!(callback_count.load(Ordering::Relaxed), 11);
    }

    /// New coverage: the bi-map's own `Del` contract when the last
    /// `sqlDigest` under a `noDBDigest` goes away -- Go deletes the whole
    /// entry (lines 235-239) rather than leaving an empty slice behind, and
    /// `TestCrossDBBindingCache` never reaches that branch.
    #[test]
    fn deleting_the_last_sql_digest_drops_the_no_db_entry() {
        let mut map = DigestBiMapImpl::new();
        map.add("no-db", "sql-a");
        map.add("no-db", "sql-b");
        assert_eq!(map.no_db_digest_count(), 1);
        map.del("sql-a");
        assert_eq!(map.no_db_digest_to_sql_digest("no-db"), ["sql-b"]);
        map.del("sql-b");
        assert_eq!(map.no_db_digest_count(), 0);
        assert!(map.no_db_digest_to_sql_digest("no-db").is_empty());
        // Deleting an unknown digest is a no-op, as in Go.
        map.del("sql-a");
        assert_eq!(map.sql_digest_count(), 0);
    }

    /// New coverage: `MatchingBinding` end to end through the bi-map, which
    /// no Go unit test in `binding_cache_test.go` exercises (its coverage is
    /// testkit-based, in `binding_match_test.go`).
    #[test]
    fn matching_binding_selects_the_candidate_for_the_current_schema() {
        let cache = BindingCache::new(1_000_000_000);
        let mut db1 = binding("SELECT * FROM db1.t1", "d1");
        db1.status = crate::binding::STATUS_ENABLED;
        db1.table_names = vec![("db1".to_owned(), "t1".to_owned())];
        let mut db2 = binding("SELECT * FROM db2.t1", "d2");
        db2.status = crate::binding::STATUS_ENABLED;
        db2.table_names = vec![("db2".to_owned(), "t1".to_owned())];
        let no_db_digest = binding_no_db_digest(&db1.bind_sql);
        cache.set_binding("d1", db1).unwrap();
        cache.set_binding("d2", db2).unwrap();

        let stmt_tables = vec![(String::new(), "t1".to_owned())];
        let matched = cache
            .matching_binding(&no_db_digest, &stmt_tables, "db2", false)
            .expect("the db2 binding matches while db2 is current");
        assert_eq!(matched.sql_digest, "d2");

        // A schema with no binding matches nothing without fuzzy binding.
        assert!(cache
            .matching_binding(&no_db_digest, &stmt_tables, "db9", false)
            .is_none());
        // An unknown no-DB digest never reaches the store.
        assert!(cache
            .matching_binding("nope", &stmt_tables, "db1", false)
            .is_none());
    }

    /// New coverage: `SetBinding` propagates a parse failure of `BindSQL`,
    /// the one error Go's signature can return (line 386).
    #[test]
    fn set_binding_rejects_unparsable_bind_sql() {
        let cache = BindingCache::new(1_000_000_000);
        let err = cache
            .set_binding("d", binding("NOT A STATEMENT", ""))
            .unwrap_err();
        assert!(format!("{err}").contains("NOT A STATEMENT"), "{err}");
    }

    /// New coverage: `SetMemCapacity` retunes the budget for the NEXT
    /// admission without evicting anything, which is `UpdateMaxCost`'s
    /// documented behaviour and what `LoadFromStorageToCache` relies on.
    #[test]
    fn set_mem_capacity_does_not_evict_immediately() {
        let one = binding("SELECT * FROM t1", "");
        let cache = BindingCache::new(binding_size(&one) * 4);
        cache.set_binding("a", one.clone()).unwrap();
        cache.set_binding("b", one.clone()).unwrap();
        assert_eq!(cache.size(), 2);
        assert_eq!(cache.mem_capacity(), binding_size(&one) * 4);

        cache.set_mem_capacity(binding_size(&one));
        assert_eq!(cache.size(), 2, "shrinking the budget evicts nothing");
        assert_eq!(cache.mem_capacity(), binding_size(&one));

        // The next admission is what enforces the new budget.
        cache.set_binding("c", one.clone()).unwrap();
        assert_eq!(cache.size(), 1);
        assert_eq!(cache.mem_usage(), binding_size(&one));
        assert_eq!(cache.get_all_bindings().len(), 1);
    }
}
