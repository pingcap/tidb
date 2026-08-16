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

//! Go `pkg/bindinfo`, covering the single file `binding_cache.go`.
//!
//! LABELING: this is a **COMPLETE port of one file** and therefore a **SEED
//! for the package**. `pkg/bindinfo` also holds `binding.go`,
//! `binding_handle.go`, `binding_operator.go`, `binding_match.go`,
//! `capture.go`, `session_handle.go`, `utils.go` and their tests; only
//! `binding_cache.go` (plus its own unit tests in `binding_cache_test.go`)
//! is claimed here, and `utils.go` is claimed by [`crate::binding_utils`].
//! Neither file makes `pkg/bindinfo` a transcreated package.
//!
//! # What this file is
//!
//! Two mechanisms, and only one of them is interesting:
//!
//! * **The digest bi-map** ([`DigestBiMapImpl`], Go lines 166-266) -- a
//!   many-to-one index from the *no-DB* digest (the statement normalized with
//!   every schema qualifier erased) to the *SQL* digests that share it. This
//!   is what makes cross-DB binding lookup one map probe instead of a scan:
//!   `select * from db1.t1` and `select * from db2.t1` collapse to the same
//!   no-DB digest, so both bindings are candidates for either statement, and
//!   [`crate::binding::cross_db_match`] picks between them. Pure logic, ported
//!   whole.
//! * **The memory-bounded store** ([`BindingCache`], Go lines 268-435) -- a
//!   cost-bounded cache keyed by SQL digest whose cost function is
//!   [`binding_size`] (Go `Binding.size()`, `binding.go:97`).
//!
//! # Narrowings
//!
//! * **`github.com/dgraph-io/ristretto` is NOT reproduced.** Go's cache is a
//!   ristretto instance with `NumCounters: 1e6`, `BufferItems: 64`,
//!   `IgnoreInternalCost`, async admission through a TinyLFU frequency sketch,
//!   sampled-LRU victim selection, and a `Wait()` the caller must issue
//!   because `Set` is a *hint* that may be silently dropped. None of that is
//!   ported. Instead the storage is narrowed behind [`BindingStore`], and the
//!   one implementation here, [`CostLruStore`], is a synchronous cost-bounded
//!   LRU. Concretely, what the narrowing does NOT reproduce:
//!   - **TinyLFU admission.** Ristretto may refuse to admit a *new* item whose
//!     estimated frequency is below the victim's; here every item that fits is
//!     admitted.
//!   - **Sampled victim choice.** Ristretto samples a handful of keys and
//!     evicts the cheapest-value one; here the victim is deterministic
//!     (see below).
//!   - **Reads do not promote.** Recency is INSERTION order only, so
//!     `GetBinding` has no side effect and can stay `&self`. Ristretto counts
//!     every `Get` into its frequency sketch.
//!   - **Asynchronous `Set` / `Wait()` / dropped writes.** `set_binding` here
//!     always takes effect immediately, so Go's "the Set might fail if the
//!     operation is too frequent" (Go line 393) cannot happen.
//!   - **`Metrics`.** `GetMemUsage`/`Size` in Go are derived from ristretto's
//!     `CostAdded-CostEvicted` / `KeysAdded-KeysEvicted` counters; here they
//!     are the store's own exact totals.
//!
//!   `TestBindingCacheEvictLog` is the test that fixes how much eviction
//!   fidelity is actually required, and it requires exactly this much: reject
//!   an item larger than the whole budget, never evict on a duplicate key, and
//!   evict exactly one victim per admission once the budget is full. All four
//!   Go tests pass against [`CostLruStore`] with byte-exact expectations, and
//!   `TestBindCache`'s `require.Eventually(... hit == 2)` -- written loose
//!   because ristretto is probabilistic -- is pinned here to the exact
//!   surviving pair.
//!
//!   Neither ported cache in the workspace fits: `tidb_util::sieve::Sieve` is
//!   the SIEVE policy with no reject-on-oversize and no per-drop callback, and
//!   `tidb_util::kvcache` is only a memory-tracker surface, not a store.
//! * **Locking.** Go guards both maps with a `sync.RWMutex` and the store is
//!   internally concurrent. This crate's session is single-threaded (`Rc` /
//!   `RefCell` throughout [`crate::Session`]), so the mutex is dropped and
//!   mutation takes `&mut self`.
//! * **`pkg/metrics`.** `metrics.BindingCacheMemUsage` /
//!   `BindingCacheMemLimit` / `BindingCacheNumBindings` are dropped by name;
//!   this workspace has no Prometheus registry.
//! * **`bindingLogger().Warn(...)`** on reject/evict is dropped; only the
//!   `bindingCacheTestKey` callback beside it survives, as
//!   [`BindingCache::set_evict_callback`]. That callback is Go's own test hook
//!   (`context.WithValue(ctx, bindingCacheTestKey, ...)`), so the narrowing
//!   preserves precisely what the tests observe.
//! * **`digestBiMap.All()` is sorted.** Go returns Go map order, i.e. random;
//!   sorting makes `get_all_bindings` reproducible. No test pins the order.
//! * **`p.ParseOneStmt(BindSQL, Charset, Collation)`** becomes
//!   [`tidb_parser::parse`]; this tier's parser takes no charset/collation
//!   pair.
//! * **`BindingCacheUpdater` / `bindingCacheUpdater`** (Go lines 39-164) is
//!   SKIPPED: it is storage plumbing over `util.DestroyableSessionPool`,
//!   `readBindingsFromStorage` and `pickCachedBinding` (both in other files of
//!   the package), plus a `lastUpdateTime` watermark and the metrics gauges
//!   above. This tier has no session pool and no background reload -- global
//!   bindings are read straight from `mysql.bind_info` by
//!   [`crate::Session`]'s binding arm.

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

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

/// The narrowing seam that stands in for `ristretto.Cache`: a cost-bounded
/// keyed store. See the module header for everything ristretto does that this
/// trait deliberately does not promise.
pub trait BindingStore {
    /// Ristretto `Get`. No side effect -- reads do not promote.
    fn get(&self, key: &str) -> Option<&Binding>;

    /// Ristretto `Set` + `Wait`, made synchronous. Returns every binding the
    /// store DROPPED as a result: the argument itself when it is rejected for
    /// exceeding the whole budget, or the victims evicted to make room.
    /// Ristretto reports both through `OnReject`/`OnEvict`, which Go wires to
    /// one closure.
    fn set(&mut self, key: &str, binding: Binding) -> Vec<Binding>;

    /// Ristretto `Del`.
    fn del(&mut self, key: &str);

    /// Ristretto `UpdateMaxCost`. Like Go, this does NOT evict immediately;
    /// the new budget applies to the next admission.
    fn set_max_cost(&mut self, max_cost: i64);

    /// Ristretto `MaxCost`.
    fn max_cost(&self) -> i64;

    /// Ristretto `Metrics.CostAdded() - Metrics.CostEvicted()`.
    fn used_cost(&self) -> i64;

    /// Ristretto `Metrics.KeysAdded() - Metrics.KeysEvicted()`.
    fn len(&self) -> usize;

    /// Whether the store holds nothing.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Ristretto `Close`, minus the callbacks: Go suppresses them behind its
    /// `closed` flag.
    fn clear(&mut self);
}

/// A synchronous cost-bounded LRU: the [`BindingStore`] this file ships.
///
/// Recency is INSERTION order (`order`, oldest at the front). Re-setting an
/// existing key removes the old entry first, so a duplicate write refreshes
/// the value and the position without charging cost twice and without
/// evicting anything -- which is exactly the property
/// `TestBindingCacheEvictLog` asserts ("duplicated binding should not trigger
/// eviction").
#[derive(Debug, Default)]
pub struct CostLruStore {
    entries: HashMap<String, Binding>,
    order: VecDeque<String>,
    used: i64,
    max_cost: i64,
}

impl CostLruStore {
    /// A store with the given cost budget (ristretto's `MaxCost`).
    #[must_use]
    pub fn new(max_cost: i64) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            used: 0,
            max_cost,
        }
    }

    fn forget(&mut self, key: &str) -> Option<Binding> {
        let removed = self.entries.remove(key)?;
        self.used -= binding_size(&removed);
        if let Some(at) = self.order.iter().position(|k| k == key) {
            self.order.remove(at);
        }
        Some(removed)
    }
}

impl BindingStore for CostLruStore {
    fn get(&self, key: &str) -> Option<&Binding> {
        self.entries.get(key)
    }

    fn set(&mut self, key: &str, binding: Binding) -> Vec<Binding> {
        let cost = binding_size(&binding);
        // A rewrite of the same key releases the old cost before the new one
        // is charged, so `SetBinding(k, b)` twice is a no-op on the budget.
        self.forget(key);
        if cost > self.max_cost {
            // Ristretto `OnReject`: the item never enters the cache.
            return vec![binding];
        }
        let mut dropped = Vec::new();
        while self.used + cost > self.max_cost {
            let Some(victim) = self.order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&victim) {
                self.used -= binding_size(&evicted);
                dropped.push(evicted);
            }
        }
        self.used += cost;
        self.entries.insert(key.to_owned(), binding);
        self.order.push_back(key.to_owned());
        dropped
    }

    fn del(&mut self, key: &str) {
        self.forget(key);
    }

    fn set_max_cost(&mut self, max_cost: i64) {
        self.max_cost = max_cost;
    }

    fn max_cost(&self) -> i64 {
        self.max_cost
    }

    fn used_cost(&self) -> i64 {
        self.used
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.used = 0;
    }
}

/// Go's `rejectOrEvict` hook, narrowed to the half the tests observe: the
/// `bindingCacheTestKey` callback. The `bindingLogger().Warn` beside it is
/// dropped (see the module narrowings).
pub type EvictCallback = Rc<dyn Fn(&Binding)>;

/// Go `bindingCache` (lines 292-299): the digest bi-map plus the cost-bounded
/// store.
///
/// Go's `BindingCache` interface (lines 268-290) is not a separate Rust trait:
/// it has exactly one implementation there, and the polymorphism this file
/// actually needs is over the STORE, which [`BindingStore`] supplies.
pub struct BindingCache<S: BindingStore = CostLruStore> {
    digest_bi_map: DigestBiMapImpl,
    store: S,
    closed: bool,
    on_reject_or_evict: Option<EvictCallback>,
}

impl BindingCache<CostLruStore> {
    /// Go `newBindingCache(ctx, maxCost)`. The `ctx` argument exists in Go
    /// only to smuggle the test callback in; use
    /// [`BindingCache::set_evict_callback`] for that.
    #[must_use]
    pub fn new(max_cost: i64) -> Self {
        Self::with_store(CostLruStore::new(max_cost))
    }
}

impl<S: BindingStore> BindingCache<S> {
    /// [`BindingCache::new`] over an arbitrary [`BindingStore`].
    pub fn with_store(store: S) -> Self {
        Self {
            digest_bi_map: DigestBiMapImpl::new(),
            store,
            closed: false,
            on_reject_or_evict: None,
        }
    }

    /// Installs Go's `bindingCacheTestKey` callback, fired once per binding
    /// the store rejects or evicts, and silenced after [`Self::close`].
    pub fn set_evict_callback(&mut self, callback: EvictCallback) {
        self.on_reject_or_evict = Some(callback);
    }

    /// The bi-map, which the Go tests reach into directly.
    #[must_use]
    pub fn digest_bi_map(&self) -> &DigestBiMapImpl {
        &self.digest_bi_map
    }

    /// Go `MatchingBinding` (lines 338-352): every binding sharing the
    /// statement's no-DB digest is a candidate, and `crossDBMatchBindings`
    /// picks among them.
    ///
    /// Go reads `@@tidb_opt_enable_fuzzy_binding` and the current schema off
    /// the `sessionctx.Context` it is handed; both are explicit parameters
    /// here, which is the same narrowing [`crate::binding`] already made.
    #[must_use]
    pub fn matching_binding(
        &self,
        no_db_digest: &str,
        table_names: &[(String, String)],
        current_db: &str,
        fuzzy_enabled: bool,
    ) -> Option<&Binding> {
        if self.size() == 0 {
            return None;
        }
        let possible = self
            .digest_bi_map
            .no_db_digest_to_sql_digest(no_db_digest)
            .iter()
            // Go's "TODO: handle cache miss safely" -- an evicted digest is
            // simply skipped.
            .filter_map(|sql_digest| self.store.get(sql_digest));
        cross_db_match(possible, table_names, current_db, fuzzy_enabled)
    }

    /// Go `GetBinding` (lines 354-363).
    #[must_use]
    pub fn get_binding(&self, sql_digest: &str) -> Option<&Binding> {
        self.store.get(sql_digest)
    }

    /// Go `GetAllBindings` (lines 365-379). A digest the bi-map still knows
    /// but the store has evicted is skipped, as in Go.
    #[must_use]
    pub fn get_all_bindings(&self) -> Vec<&Binding> {
        self.digest_bi_map
            .all()
            .into_iter()
            .filter_map(|sql_digest| self.store.get(&sql_digest))
            .collect()
    }

    /// Go `SetBinding` (lines 381-399).
    ///
    /// The no-DB digest is derived by PARSING `bind_sql` -- not the origin
    /// statement -- exactly as Go does, which is why a parse failure is the
    /// only error this can return.
    ///
    /// Go's note survives verbatim in behaviour: due to eviction the store may
    /// hold fewer digests than the bi-map, and that is acceptable because the
    /// optimizer reloads on a cache miss.
    pub fn set_binding(&mut self, sql_digest: &str, binding: Binding) -> Result<(), DriverError> {
        let stmt = tidb_parser::parse(&binding.bind_sql).map_err(|err| {
            DriverError::unsupported(format!(
                "cannot parse binding SQL {:?}: {}",
                binding.bind_sql,
                err.compatibility_message(&binding.bind_sql)
            ))
        })?;
        let no_db_digest = crate::binding::no_db_digest(&stmt);
        self.digest_bi_map.add(&no_db_digest, sql_digest);
        let dropped = self.store.set(sql_digest, binding);
        for binding in &dropped {
            self.reject_or_evict(binding);
        }
        Ok(())
    }

    /// Go `RemoveBinding` (lines 401-406).
    pub fn remove_binding(&mut self, sql_digest: &str) {
        self.digest_bi_map.del(sql_digest);
        self.store.del(sql_digest);
    }

    /// Go `SetMemCapacity` (lines 408-412).
    pub fn set_mem_capacity(&mut self, capacity: i64) {
        self.store.set_max_cost(capacity);
    }

    /// Go `GetMemUsage` (lines 414-418).
    #[must_use]
    pub fn mem_usage(&self) -> i64 {
        self.store.used_cost()
    }

    /// Go `GetMemCapacity` (lines 420-424).
    #[must_use]
    pub fn mem_capacity(&self) -> i64 {
        self.store.max_cost()
    }

    /// Go `Size` (line 426).
    #[must_use]
    pub fn size(&self) -> usize {
        self.store.len()
    }

    /// Go `Close` (lines 430-435): sets the `closed` flag FIRST so the drops
    /// it causes raise no callback, then empties the store.
    pub fn close(&mut self) {
        self.closed = true;
        self.store.clear();
    }

    /// Go's `rejectOrEvict` closure.
    fn reject_or_evict(&self, binding: &Binding) {
        if self.closed {
            // Go: "avoid unnecessary log when exiting".
            return;
        }
        if let Some(callback) = &self.on_reject_or_evict {
            callback(binding);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

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

    /// Go `TestCrossDBBindingCache`.
    #[test]
    fn cross_db_binding_cache() {
        let mut cache = BindingCache::new(1_000_000_000);
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
        let mut cache = BindingCache::new(1_000_000_000);
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
    /// Go's tail assertion is `require.Eventually(... hit == 2 ...)`, loose
    /// because ristretto is asynchronous and probabilistic. This store is
    /// neither, so the exact surviving pair is pinned: `digest1` is the oldest
    /// insertion and therefore the victim.
    #[test]
    fn bind_cache() {
        let one = binding("SELECT * FROM t1", "");
        let kv_size = binding_size(&one);
        assert_eq!(kv_size, 48);
        let mut cache = BindingCache::new(kv_size * 3 - 1);

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
        assert!(cache.get_binding("digest1").is_none());

        cache.close();
        assert_eq!(cache.size(), 0);
    }

    /// Go `TestBindingCacheEvictLog`.
    #[test]
    fn binding_cache_evict_log() {
        let callback_count = Rc::new(Cell::new(0usize));
        let counter = Rc::clone(&callback_count);

        let large = binding(
            &format!("SELECT * FROM t1 WHERE c = '{}'", "a".repeat(200)),
            "",
        );
        let one = binding("SELECT * FROM t1", "");
        let mut cache = BindingCache::new(binding_size(&one) * 3 - 1);
        cache.set_evict_callback(Rc::new(move |_binding: &Binding| {
            counter.set(counter.get() + 1);
        }));

        cache.set_binding("0", large.clone()).unwrap();
        assert_eq!(callback_count.get(), 1); // large binding, reject directly
        cache.set_binding("0", large).unwrap();
        assert_eq!(callback_count.get(), 2); // large binding, reject directly
        callback_count.set(0); // reset callback count

        cache.set_binding("1", one.clone()).unwrap(); // insert the first binding four times
        cache.set_binding("1", one.clone()).unwrap();
        cache.set_binding("1", one.clone()).unwrap();
        cache.set_binding("1", one.clone()).unwrap();
        assert_eq!(cache.size(), 1);
        assert_eq!(cache.mem_usage(), binding_size(&one));
        assert_eq!(callback_count.get(), 0); // duplicated binding should not trigger eviction

        cache.set_binding("2", one.clone()).unwrap(); // insert the second binding
        cache.set_binding("2", one.clone()).unwrap();
        assert_eq!(callback_count.get(), 0); // cache size is enough

        cache.set_binding("3", one.clone()).unwrap(); // insert the third binding, triggers eviction
        assert_eq!(callback_count.get(), 1);

        for i in 1..=10 {
            cache.set_binding(&format!("3-{i}"), one.clone()).unwrap();
            assert_eq!(callback_count.get(), 1 + i);
        }

        assert_eq!(callback_count.get(), 11);
        cache.close(); // close doesn't trigger eviction log
        assert_eq!(callback_count.get(), 11);
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
        let mut cache = BindingCache::new(1_000_000_000);
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
        let mut cache = BindingCache::new(1_000_000_000);
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
        let mut cache = BindingCache::new(binding_size(&one) * 4);
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
