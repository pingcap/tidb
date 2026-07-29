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

//! The client half of GC safe-point fidelity, transcreated from client-go's
//! `tikv/kv.go` (`UpdateTxnSafePointCache`, `loadTxnSafePoint`,
//! `CheckVisibility`, `runTxnSafePointUpdater`), `tikv/safepoint.go`, and
//! `tikv/compatible_txn_safe_point_loader.go`.
//!
//! # What a client owns, and what it does not
//!
//! GC has two halves. The *owner* half — advancing the txn safe point,
//! resolving locks below it, advancing the GC safe point, and deleting the
//! obsolete MVCC versions — lives in TiDB's GC worker and in PD, and is
//! deliberately **out of scope here**: a reader must never advance a safe point
//! or delete a version. The *client* half is exactly two obligations, and this
//! module implements both:
//!
//! 1. Keep a periodically refreshed copy of PD's txn safe point.
//! 2. Refuse to hand back data that was read at a `start_ts` below it, with a
//!    distinct, non-retryable error rather than a generic failure.
//!
//! Registering a GC barrier (formerly a "service safe point") to *hold GC back*
//! is also an owner-side act, and the pinned client-go does not do it on the
//! SQL read path: `UpdateServiceGCSafePoint` appears there only in
//! `unimplementedPDClient` and the mock store. TiDB registers barriers from
//! long-lived jobs such as the ingest checksum
//! (`pkg/ingestor/ingestctrl/checksum.go`), not from snapshot reads. So a
//! reading client protects itself by *detecting* that GC passed it, never by
//! blocking GC — and this module registers nothing.
//!
//! # Why the check is after the read, not before it
//!
//! client-go calls `CheckVisibility` *after* a Get or Scan returns
//! (`txnkv/txnsnapshot/snapshot.go:325,773` and `scan.go:287`), not before the
//! snapshot is opened. Checking first would prove nothing: GC can advance while
//! the RPC is in flight. Checking after means every value handed to the caller
//! was either read above the safe point known at that moment, or rejected. This
//! module preserves that ordering; callers must not "optimize" it into a
//! pre-check.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tidb_pd_client::{EtcdClient, PdClient};

use crate::StorageDriverError;

/// How long a cached txn safe point stays usable, from `GcStateCacheInterval`.
pub const GC_STATE_CACHE_INTERVAL: Duration = Duration::from_secs(100);

/// Slack subtracted from the cache interval, from `gcCPUTimeInaccuracyBound`.
///
/// The refresh loop and the freshness test read different clocks, and a stalled
/// process can lose CPU between them. Rejecting slightly early is the safe
/// direction: a stale cache cannot prove a `start_ts` is still above the safe
/// point.
pub const GC_CPU_TIME_INACCURACY_BOUND: Duration = Duration::from_secs(10);

/// Steady-state refresh period, from `pollTxnSafePointInterval`.
pub const POLL_TXN_SAFE_POINT_INTERVAL: Duration = Duration::from_secs(10);

/// Retry period after a failed refresh, from
/// `pollTxnSafePointQuickRepeatInterval`.
pub const POLL_TXN_SAFE_POINT_QUICK_REPEAT_INTERVAL: Duration = Duration::from_secs(1);

/// The deprecated etcd txn-safe-point key, from `GcSavedSafePoint` /
/// `unifiedTxnSafePointPath`.
///
/// Only read as a fallback for a PD older than the GC-state API. A client never
/// writes it: that is the GC owner's key.
pub const UNIFIED_TXN_SAFE_POINT_PATH: &str = "/tidb/store/gcworker/saved_safe_point";

/// The keyspace-scoped form of [`UNIFIED_TXN_SAFE_POINT_PATH`], from
/// `keyspaceLevelTxnSafePointPath`.
pub const KEYSPACE_LEVEL_TXN_SAFE_POINT_PATH_PREFIX: &str = "/keyspaces/tidb/";

/// Exact PD-timeout text used when the cached txn safe point is too old, from
/// `CheckVisibility`.
pub const STALE_GC_STATE_CACHE_MESSAGE: &str = "start timestamp may fall behind safe point";

/// The deprecated etcd key a compatible-mode load reads for `keyspace_id`.
///
/// `None` selects the unified (null-keyspace) key, which is what every
/// deployment without keyspace-level GC uses.
#[must_use]
pub fn compatible_txn_safe_point_key(keyspace_id: Option<u32>) -> String {
    match keyspace_id {
        None => UNIFIED_TXN_SAFE_POINT_PATH.to_owned(),
        Some(id) => {
            format!("{KEYSPACE_LEVEL_TXN_SAFE_POINT_PATH_PREFIX}{id}{UNIFIED_TXN_SAFE_POINT_PATH}")
        }
    }
}

/// Decodes the deprecated etcd txn-safe-point value.
///
/// A missing key and an empty value both mean "GC has never run", which is zero
/// — not an error. Anything else must parse as a decimal `u64`; a corrupt value
/// is never silently treated as zero, because zero would disable the check.
pub fn parse_compatible_txn_safe_point(raw: Option<&[u8]>) -> Result<u64, GcStateLoadError> {
    let Some(raw) = raw else {
        return Ok(0);
    };
    let text = std::str::from_utf8(raw).map_err(|_| GcStateLoadError::MalformedSafePoint {
        raw: String::from_utf8_lossy(raw).into_owned(),
    })?;
    if text.is_empty() {
        return Ok(0);
    }
    text.parse::<u64>()
        .map_err(|_| GcStateLoadError::MalformedSafePoint {
            raw: text.to_owned(),
        })
}

/// Why loading the txn safe point failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GcStateLoadError {
    /// PD or etcd was reachable but the load failed.
    ///
    /// A PD that does not implement the GC-state API is not one of these: the
    /// loader answers that by falling back to the deprecated etcd key, so it
    /// surfaces only whatever the fallback itself could not do.
    Unavailable {
        /// Concrete transport or protocol detail.
        message: String,
    },
    /// The deprecated etcd key held a value that is not a decimal timestamp.
    MalformedSafePoint {
        /// The rejected raw value.
        raw: String,
    },
}

impl fmt::Display for GcStateLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { message } => {
                write!(
                    formatter,
                    "failed to load current txn safe point: {message}"
                )
            }
            Self::MalformedSafePoint { raw } => {
                write!(
                    formatter,
                    "txn safe point {raw:?} is not a decimal timestamp"
                )
            }
        }
    }
}

impl std::error::Error for GcStateLoadError {}

/// Why a `start_ts` may not be used to hand data back to a caller.
///
/// Both variants are terminal for the transaction that raised them: retrying
/// the same `start_ts` can only fail again, because a safe point never moves
/// backwards.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VisibilityError {
    /// The cached txn safe point is too old to prove anything.
    StaleGcStateCache,
    /// GC has advanced past this transaction's `start_ts`.
    TxnAbortedByGc {
        /// The transaction's start timestamp.
        txn_start_ts: u64,
        /// The txn safe point that overtook it.
        txn_safe_point: u64,
    },
}

impl fmt::Display for VisibilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleGcStateCache => formatter.write_str(STALE_GC_STATE_CACHE_MESSAGE),
            Self::TxnAbortedByGc {
                txn_start_ts,
                txn_safe_point,
            } => write!(
                formatter,
                // Kept verbatim from client-go's `ErrTxnAbortedByGC`: the
                // wording is inaccurate (the usual cause is a long-running
                // transaction, not a short GC life time) but it is what
                // operators and diagnostic tooling already match on.
                "GC life time is shorter than transaction duration, transaction start ts is {txn_start_ts}, txn safe point is {txn_safe_point}"
            ),
        }
    }
}

impl std::error::Error for VisibilityError {}

impl VisibilityError {
    /// Classifies this failure into the storage-driver tier that carries it to
    /// SQL, mirroring `pkg/store/driver/error/error.go`.
    ///
    /// The times client-go renders with `oracle.GetTimeFromTS` are derived from
    /// the timestamps themselves; they are rendered by
    /// [`Self::into_storage_driver_error_with_times`] when a caller has a
    /// timestamp formatter, and elided to the numeric form otherwise.
    #[must_use]
    pub fn into_storage_driver_error(self) -> StorageDriverError {
        match self {
            Self::StaleGcStateCache => StorageDriverError::PdServerTimeout {
                message: STALE_GC_STATE_CACHE_MESSAGE.to_owned(),
            },
            Self::TxnAbortedByGc {
                txn_start_ts,
                txn_safe_point,
            } => StorageDriverError::TxnAbortedByGc {
                txn_start_ts,
                txn_start_ts_time: txn_start_ts.to_string(),
                txn_safe_point,
                txn_safe_point_time: txn_safe_point.to_string(),
            },
        }
    }

    /// As [`Self::into_storage_driver_error`], with wall-clock renderings of
    /// the two timestamps supplied by the caller's oracle.
    #[must_use]
    pub fn into_storage_driver_error_with_times(
        self,
        txn_start_ts_time: String,
        txn_safe_point_time: String,
    ) -> StorageDriverError {
        match self {
            Self::StaleGcStateCache => self.into_storage_driver_error(),
            Self::TxnAbortedByGc {
                txn_start_ts,
                txn_safe_point,
            } => StorageDriverError::TxnAbortedByGc {
                txn_start_ts,
                txn_start_ts_time,
                txn_safe_point,
                txn_safe_point_time,
            },
        }
    }
}

/// The process-wide cached txn safe point, from `KVStore.gcStateCacheMu`.
///
/// One instance is shared by every transaction reading through the same store,
/// exactly as client-go shares the field on `KVStore`. A fresh cache has never
/// been updated, and rejects every read until its first successful load — the
/// same state client-go avoids by loading once during store construction and
/// failing store construction if that load fails.
#[derive(Debug)]
pub struct GcStateCache {
    inner: RwLock<CachedGcState>,
}

#[derive(Clone, Copy, Debug)]
struct CachedGcState {
    txn_safe_point: u64,
    last_cache_time: Option<Instant>,
}

impl Default for GcStateCache {
    fn default() -> Self {
        Self::new()
    }
}

impl GcStateCache {
    /// A cache that has never observed a txn safe point.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: RwLock::new(CachedGcState {
                txn_safe_point: 0,
                last_cache_time: None,
            }),
        }
    }

    /// A cache seeded with one observation, as the initial load produces.
    #[must_use]
    pub fn seeded(txn_safe_point: u64, observed_at: Instant) -> Self {
        let cache = Self::new();
        cache.update(txn_safe_point, observed_at);
        cache
    }

    /// Records one observation, from `UpdateTxnSafePointCache`.
    pub fn update(&self, txn_safe_point: u64, observed_at: Instant) {
        let mut inner = self.inner.write().expect("GC state cache lock poisoned");
        inner.txn_safe_point = txn_safe_point;
        inner.last_cache_time = Some(observed_at);
    }

    /// The last observed txn safe point, or `None` before the first load.
    #[must_use]
    pub fn observed_txn_safe_point(&self) -> Option<u64> {
        let inner = self.inner.read().expect("GC state cache lock poisoned");
        inner.last_cache_time.map(|_| inner.txn_safe_point)
    }

    /// Whether `start_ts` may still be used, from `CheckVisibility`.
    ///
    /// `now` is the caller's clock reading, so a test can drive the freshness
    /// branch without sleeping.
    pub fn check_visibility_at(&self, start_ts: u64, now: Instant) -> Result<(), VisibilityError> {
        let inner = *self.inner.read().expect("GC state cache lock poisoned");
        let staleness = inner
            .last_cache_time
            .map_or(Duration::MAX, |last| now.saturating_duration_since(last));
        if staleness > GC_STATE_CACHE_INTERVAL.saturating_sub(GC_CPU_TIME_INACCURACY_BOUND) {
            return Err(VisibilityError::StaleGcStateCache);
        }
        if start_ts < inner.txn_safe_point {
            return Err(VisibilityError::TxnAbortedByGc {
                txn_start_ts: start_ts,
                txn_safe_point: inner.txn_safe_point,
            });
        }
        Ok(())
    }

    /// [`Self::check_visibility_at`] against the current clock.
    pub fn check_visibility(&self, start_ts: u64) -> Result<(), VisibilityError> {
        self.check_visibility_at(start_ts, Instant::now())
    }
}

/// Loads the txn safe point the way client-go's `KVStore.loadTxnSafePoint`
/// does: PD's GC-state API first, and the deprecated etcd key only after PD
/// has answered `Unimplemented` once.
///
/// The fallback latches. client-go keeps a `gcStatesAPIUnavailable` flag so a
/// pre-9.0 cluster is asked exactly once; without the latch every refresh would
/// pay a failing RPC — and, here, a full endpoint failover probe — before
/// reaching etcd.
pub struct TxnSafePointLoader {
    pd: PdClient,
    keyspace_id: Option<u32>,
    etcd_timeout: Duration,
    gc_states_api_unavailable: AtomicBool,
    etcd: Mutex<Option<EtcdClient>>,
}

impl fmt::Debug for TxnSafePointLoader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TxnSafePointLoader")
            .field("keyspace_id", &self.keyspace_id)
            .field(
                "gc_states_api_unavailable",
                &self.gc_states_api_unavailable.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl TxnSafePointLoader {
    /// Binds a loader to one PD client and keyspace scope.
    ///
    /// `None` is the null keyspace, which is the scope of every deployment
    /// without keyspace-level GC.
    #[must_use]
    pub fn new(pd: PdClient, keyspace_id: Option<u32>, etcd_timeout: Duration) -> Self {
        Self {
            pd,
            keyspace_id,
            etcd_timeout,
            gc_states_api_unavailable: AtomicBool::new(false),
            etcd: Mutex::new(None),
        }
    }

    /// Whether PD has already proved it does not implement the GC-state API.
    #[must_use]
    pub fn is_gc_states_api_unavailable(&self) -> bool {
        self.gc_states_api_unavailable.load(Ordering::Relaxed)
    }

    /// One load, following the source's PD-then-etcd order.
    pub fn load(&self) -> Result<u64, GcStateLoadError> {
        if self.is_gc_states_api_unavailable() {
            return self.load_compatible();
        }
        match self.pd.get_gc_state(self.keyspace_id) {
            Ok(state) => Ok(state.txn_safe_point),
            Err(error) if tidb_pd_client::is_unimplemented(&error) => {
                self.gc_states_api_unavailable
                    .store(true, Ordering::Relaxed);
                self.load_compatible()
            }
            Err(error) => Err(GcStateLoadError::Unavailable {
                message: error.to_string(),
            }),
        }
    }

    /// The deprecated etcd read, from `compatibleTxnSafePointLoader`.
    ///
    /// The etcd client is created lazily and then retained, because a
    /// pre-9.0 cluster takes this path on every refresh.
    fn load_compatible(&self) -> Result<u64, GcStateLoadError> {
        let key = compatible_txn_safe_point_key(self.keyspace_id);
        let mut held = self.etcd.lock().expect("etcd client lock poisoned");
        if held.is_none() {
            let endpoints = self.pd.member_set().member_urls;
            *held = Some(
                EtcdClient::connect(endpoints, self.etcd_timeout).map_err(|error| {
                    GcStateLoadError::Unavailable {
                        message: error.to_string(),
                    }
                })?,
            );
        }
        let client = held.as_ref().expect("etcd client created above");
        let raw = client
            .get(key.as_bytes())
            .map_err(|error| GcStateLoadError::Unavailable {
                message: error.to_string(),
            })?;
        parse_compatible_txn_safe_point(raw.as_deref())
    }
}

/// The next refresh delay after one poll, from `runTxnSafePointUpdater`.
///
/// A failed load repeats quickly because until it succeeds the cache is ageing
/// towards the point where it rejects every read.
#[must_use]
pub const fn next_poll_delay(loaded: bool) -> Duration {
    if loaded {
        POLL_TXN_SAFE_POINT_INTERVAL
    } else {
        POLL_TXN_SAFE_POINT_QUICK_REPEAT_INTERVAL
    }
}

/// The background refresh loop, from `KVStore.runTxnSafePointUpdater`.
///
/// One refresher serves one cache. It is stopped by dropping it, which is why
/// the owner keeps it inside an `Arc`: the loop must outlive every clone of the
/// authority that reads the cache, and must not outlive the last one.
pub struct TxnSafePointRefresher {
    cache: Arc<GcStateCache>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl fmt::Debug for TxnSafePointRefresher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TxnSafePointRefresher")
            .field("txn_safe_point", &self.cache.observed_txn_safe_point())
            .finish_non_exhaustive()
    }
}

impl TxnSafePointRefresher {
    /// Performs the initial load and then keeps refreshing until dropped.
    ///
    /// The initial load is not optional and its failure is returned: client-go
    /// fails `NewKVStore` outright when it cannot read the txn safe point,
    /// because a store whose cache was never seeded rejects every read anyway.
    pub fn start(loader: TxnSafePointLoader) -> Result<Self, GcStateLoadError> {
        let txn_safe_point = loader.load()?;
        let cache = Arc::new(GcStateCache::seeded(txn_safe_point, Instant::now()));
        let stop = Arc::new(AtomicBool::new(false));
        let worker = std::thread::Builder::new()
            .name("txn-safe-point-updater".to_owned())
            .spawn({
                let cache = Arc::clone(&cache);
                let stop = Arc::clone(&stop);
                move || refresh_loop(&loader, &cache, &stop)
            })
            .map_err(|error| GcStateLoadError::Unavailable {
                message: format!("cannot start the txn safe point updater: {error}"),
            })?;
        Ok(Self {
            cache,
            stop,
            worker: Some(worker),
        })
    }

    /// The cache this refresher keeps current.
    #[must_use]
    pub fn cache(&self) -> Arc<GcStateCache> {
        Arc::clone(&self.cache)
    }
}

impl Drop for TxnSafePointRefresher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Granularity at which a sleeping refresh loop notices it was stopped.
const REFRESH_STOP_POLL: Duration = Duration::from_millis(50);

fn refresh_loop(loader: &TxnSafePointLoader, cache: &GcStateCache, stop: &AtomicBool) {
    let mut delay = POLL_TXN_SAFE_POINT_INTERVAL;
    while !stop.load(Ordering::Relaxed) {
        let mut waited = Duration::ZERO;
        while waited < delay {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            let step = REFRESH_STOP_POLL.min(delay - waited);
            std::thread::sleep(step);
            waited += step;
        }
        // The observation time is taken after the load returns, so a slow load
        // cannot make the cache look fresher than the data in it.
        match loader.load() {
            Ok(txn_safe_point) => {
                cache.update(txn_safe_point, Instant::now());
                delay = next_poll_delay(true);
            }
            Err(_) => delay = next_poll_delay(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unified_key_matches_client_go() {
        assert_eq!(
            compatible_txn_safe_point_key(None),
            "/tidb/store/gcworker/saved_safe_point"
        );
        assert_eq!(
            compatible_txn_safe_point_key(Some(7)),
            "/keyspaces/tidb/7/tidb/store/gcworker/saved_safe_point"
        );
    }

    #[test]
    fn absent_and_empty_values_mean_gc_never_ran() {
        assert_eq!(parse_compatible_txn_safe_point(None), Ok(0));
        assert_eq!(parse_compatible_txn_safe_point(Some(b"")), Ok(0));
        assert_eq!(parse_compatible_txn_safe_point(Some(b"449")), Ok(449));
    }

    #[test]
    fn corrupt_value_is_rejected_rather_than_read_as_zero() {
        assert_eq!(
            parse_compatible_txn_safe_point(Some(b"-1")),
            Err(GcStateLoadError::MalformedSafePoint {
                raw: "-1".to_owned()
            })
        );
        assert!(parse_compatible_txn_safe_point(Some(&[0xff])).is_err());
    }

    #[test]
    fn a_never_loaded_cache_rejects_every_read() {
        let cache = GcStateCache::new();
        assert_eq!(cache.observed_txn_safe_point(), None);
        assert_eq!(
            cache.check_visibility(u64::MAX),
            Err(VisibilityError::StaleGcStateCache)
        );
    }

    #[test]
    fn a_start_ts_above_the_safe_point_is_visible() {
        let now = Instant::now();
        let cache = GcStateCache::seeded(100, now);
        assert_eq!(cache.check_visibility_at(100, now), Ok(()));
        assert_eq!(cache.check_visibility_at(101, now), Ok(()));
    }

    #[test]
    fn a_start_ts_below_the_safe_point_is_aborted_by_gc() {
        let now = Instant::now();
        let cache = GcStateCache::seeded(100, now);
        assert_eq!(
            cache.check_visibility_at(99, now),
            Err(VisibilityError::TxnAbortedByGc {
                txn_start_ts: 99,
                txn_safe_point: 100,
            })
        );
    }

    #[test]
    fn staleness_is_judged_against_the_interval_minus_the_inaccuracy_bound() {
        let seeded_at = Instant::now();
        let cache = GcStateCache::seeded(100, seeded_at);
        let usable = GC_STATE_CACHE_INTERVAL - GC_CPU_TIME_INACCURACY_BOUND;
        assert!(cache.check_visibility_at(99, seeded_at + usable).is_err());
        assert_eq!(
            cache.check_visibility_at(200, seeded_at + usable),
            Ok(()),
            "a cache exactly at the boundary is still usable"
        );
        assert_eq!(
            cache.check_visibility_at(200, seeded_at + usable + Duration::from_millis(1)),
            Err(VisibilityError::StaleGcStateCache),
            "one tick past the boundary the cache proves nothing"
        );
    }

    #[test]
    fn a_refresh_makes_a_stale_cache_usable_again() {
        let seeded_at = Instant::now();
        let cache = GcStateCache::seeded(100, seeded_at);
        let far_future = seeded_at + GC_STATE_CACHE_INTERVAL * 2;
        assert_eq!(
            cache.check_visibility_at(200, far_future),
            Err(VisibilityError::StaleGcStateCache)
        );
        cache.update(150, far_future);
        assert_eq!(cache.check_visibility_at(200, far_future), Ok(()));
        assert_eq!(cache.observed_txn_safe_point(), Some(150));
    }

    #[test]
    fn visibility_failures_classify_into_distinct_driver_tiers() {
        assert_eq!(
            VisibilityError::StaleGcStateCache.into_storage_driver_error(),
            StorageDriverError::PdServerTimeout {
                message: "start timestamp may fall behind safe point".to_owned()
            }
        );
        assert_eq!(
            VisibilityError::TxnAbortedByGc {
                txn_start_ts: 99,
                txn_safe_point: 100,
            }
            .into_storage_driver_error_with_times("t1".to_owned(), "t2".to_owned()),
            StorageDriverError::TxnAbortedByGc {
                txn_start_ts: 99,
                txn_start_ts_time: "t1".to_owned(),
                txn_safe_point: 100,
                txn_safe_point_time: "t2".to_owned(),
            }
        );
    }

    #[test]
    fn the_abort_message_keeps_client_gos_wording() {
        let rendered = VisibilityError::TxnAbortedByGc {
            txn_start_ts: 449,
            txn_safe_point: 450,
        }
        .to_string();
        assert!(
            rendered.starts_with("GC life time is shorter than transaction duration,"),
            "{rendered}"
        );
    }
}
