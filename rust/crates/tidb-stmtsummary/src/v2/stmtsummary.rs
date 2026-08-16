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

//! Go `pkg/util/stmtsummary/v2/stmtsummary.go`: lands complete.
//!
//! Every production symbol of `stmtsummary.go` is here — `GlobalStmtSummary`,
//! `timeNow`, `Setup`, `Close`, `Config`, `StmtSummary` with its whole option
//! and lifecycle surface, `NewStmtSummary`, `NewStmtSummary4Test`, `Add`,
//! `Evicted`, `Clear`, `ClearInternal`, `flush`, `rotateLoop`, `updateMetrics`,
//! `rotate`, `onEvict`, `evictedLogLoop`, `stmtWindow`, `onEvictFn`,
//! `newStmtWindow`, `stmtStorage`, `stmtEvicted`, `newStmtEvicted`,
//! `newEvictedAggregateRecord`, `lockedStmtRecord`, `mockStmtStorage`,
//! `cloneRecordForLog`, and the eleven v1/v2 proxy functions.
//!
//! What this file reuses from v1 rather than restating:
//!
//! - `stmtsummary.StmtDigestKey` and its `Init`/`Hash` are v1's
//!   [`StmtDigestKey`]; `kvcache.SimpleLRUCache` is v1's
//!   [`SimpleLruCache`].
//! - The v1 half of every proxy function is the real
//!   [`STMT_SUMMARY_BY_DIGEST_MAP`], so `Add`/`Enabled`/`SetGroupByUser` and
//!   friends dispatch to the same v1 object Go's do.
//! - `metrics.SetStmtSummaryWindowMetrics` reuses v1's
//!   [`WindowMetricsSink`] trait rather than declaring a second one.
//!
//! Where v2 genuinely diverges from v1:
//!
//! - v1 keeps a per-digest history of intervals inside one map; v2 keeps a
//!   single rotating window plus a durable log, so `SetHistorySize` is a
//!   deliberate no-op on the v2 side of the proxy.
//! - v2's eviction is per-record and optionally persisted
//!   (`optPersistEvicted`, `evictedCh`); v1's eviction only rolls into the
//!   `other` aggregate.
//! - v2's `stmtEvicted` keeps two aggregates (`other`, `otherForPersist`) so a
//!   record already handed to the evicted log is not counted twice; v1 has one.
//! - v2 groups by user through the window-wide `optGroupByUser` flag, clearing
//!   the window on a flip; v1's `SetGroupByUser` lives on the digest map.
//!
//! Narrowings:
//!
//! - `go.uber.org/atomic` option cells narrow to `std::sync::atomic` types.
//! - `context.Context` + `cancel` + `sync.WaitGroup` narrow to
//!   a private `Shutdown` cell (a `Mutex`/`Condvar` pair) plus joined
//!   `std::thread` handles.
//!   `Close` drops the evicted-log sender, which makes the log thread's
//!   `recv` return `Disconnected` after draining every buffered record — the
//!   same "no more `Add` can enqueue, drain and exit" contract Go gets from
//!   cancelling the context under `windowLock`.
//! - Go's `chan *StmtRecord` with a non-blocking `select`/`default` send narrows
//!   to `std::sync::mpsc::SyncSender::try_send`; the sender lives in the
//!   private `EvictShared` cell so the LRU eviction closure can reach the state
//!   Go's `s.onEvict` method reads, without the window→summary→window reference
//!   cycle a captured `&StmtSummary` would create.
//! - `metrics.StmtSummaryEvictedLogCounter` narrows to the
//!   [`EvictedLogMetricsSink`] trait, defaulting to
//!   [`NoopEvictedLogMetricsSink`]; `metrics.SetStmtSummaryWindowMetrics`
//!   narrows to v1's [`WindowMetricsSink`].
//! - Go's `timeNow` package variable narrows to [`set_time_now`] /
//!   [`reset_time_now`] over a process-global hook.
//! - Go's `s.window` pointer plus `windowLock` narrows to
//!   `Mutex<Arc<Mutex<StmtWindow>>>`: the outer mutex is `windowLock` and is
//!   held across each of Go's critical sections, the inner one guards the
//!   window a `rotate` may have already handed to a persisting thread.
//! - `stmtWindow.clear` resets the contents of the shared `evicted` cell
//!   instead of replacing the pointer, because the eviction closure holds the
//!   same cell; the two are observationally identical.
//! - `newStmtLogStorage` lives in Go `v2/logger.go`, which is NOT ported. The
//!   rotating-file sink is isolated behind the [`StmtLogWriter`] trait, and
//!   [`FileStmtLogWriter`] is a lazily-opened append-only writer: it performs
//!   **no** size/age/backup rotation, so `Config`'s `file_max_size`,
//!   `file_max_days` and `file_max_backups` are carried but unused. Tests never
//!   touch the filesystem; they pass their own [`StmtStorage`].
//!   [`StmtLogStorage`] and [`FileStmtLogWriter`] are SEED evidence for
//!   `logger.go`, not a port of it.
//! - `logutil.BgLogger()` has no boundary here: the sync failure and the
//!   dropped-record report Go logs are surfaced through
//!   [`StmtSummary::evicted_dropped`] instead of a logger.

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tidb_datatype::Datum;
use tidb_kvcache::SimpleLruCache;

use crate::statement_summary::{
    StmtDigestKey, StmtExecInfo, WindowMetricsSink, STMT_SUMMARY_BY_DIGEST_MAP,
};
use crate::v2::column::timestamp_datum;
use crate::v2::record::{
    marshal_evicted_stmt_record, marshal_stmt_record, new_stmt_record, StmtRecord,
};

/// Go `defaultEnabled`.
pub const DEFAULT_ENABLED: bool = true;
/// Go `defaultEnableInternalQuery`.
pub const DEFAULT_ENABLE_INTERNAL_QUERY: bool = false;
/// Go `defaultMaxStmtCount`.
pub const DEFAULT_MAX_STMT_COUNT: u32 = 3000;
/// Go `defaultMaxSQLLength`.
pub const DEFAULT_MAX_SQL_LENGTH: u32 = 32768;
/// Go `defaultRefreshInterval`: 30 min.
pub const DEFAULT_REFRESH_INTERVAL: u32 = 30 * 60;
/// Go `defaultRotateCheckInterval`: 1 s.
pub const DEFAULT_ROTATE_CHECK_INTERVAL: u64 = 1;

/// Go `evictedLogChanCap`: bounds the buffer of per-record evicted entries
/// waiting to be logged. When full, new evictions are dropped so `Add` never
/// blocks.
pub const EVICTED_LOG_CHAN_CAP: usize = 1024;
/// Go `evictedLogBatchSize`.
pub const EVICTED_LOG_BATCH_SIZE: usize = 64;
/// Go `evictedLogFlushInterval`.
pub const EVICTED_LOG_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
/// Go `evictedDropReportInterval`.
pub const EVICTED_DROP_REPORT_INTERVAL: Duration = Duration::from_secs(30);

/// Go `GlobalStmtSummary`: the global `StmtSummary` instance. We need to
/// explicitly call [`setup`] to initialize it. It will then be referenced by
/// `SessionVars.StmtSummary` for each session.
static GLOBAL_STMT_SUMMARY: RwLock<Option<Arc<StmtSummary>>> = RwLock::new(None);

type TimeNowFn = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// Go's `timeNow = time.Now` package variable, which the upstream tests
/// reassign.
static TIME_NOW: RwLock<Option<TimeNowFn>> = RwLock::new(None);

/// Go `timeNow()`.
#[must_use]
pub fn time_now() -> DateTime<Utc> {
    let hook = TIME_NOW.read().expect("time_now lock poisoned").clone();
    hook.map_or_else(Utc::now, |hook| hook())
}

/// Installs Go's `timeNow` replacement.
pub fn set_time_now(hook: TimeNowFn) {
    *TIME_NOW.write().expect("time_now lock poisoned") = Some(hook);
}

/// Restores Go's `timeNow = time.Now`.
pub fn reset_time_now() {
    *TIME_NOW.write().expect("time_now lock poisoned") = None;
}

/// Go `GlobalStmtSummary`, as an owned handle.
#[must_use]
pub fn global_stmt_summary() -> Option<Arc<StmtSummary>> {
    GLOBAL_STMT_SUMMARY
        .read()
        .expect("global stmt summary lock poisoned")
        .clone()
}

/// Installs (or clears) Go's `GlobalStmtSummary`.
pub fn set_global_stmt_summary(summary: Option<Arc<StmtSummary>>) {
    *GLOBAL_STMT_SUMMARY
        .write()
        .expect("global stmt summary lock poisoned") = summary;
}

/// Go `maxSQLLength`'s `GlobalStmtSummary` read, which `record.go` calls.
pub(crate) fn global_max_sql_length() -> u32 {
    global_stmt_summary().map_or(DEFAULT_MAX_SQL_LENGTH, |summary| summary.max_sql_length())
}

/// Go `errors.New("stmtsummary: empty filename")`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmptyFilename;

impl std::fmt::Display for EmptyFilename {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("stmtsummary: empty filename")
    }
}

impl std::error::Error for EmptyFilename {}

/// Go `Setup`: initializes the `GlobalStmtSummary`.
///
/// # Errors
///
/// Returns [`EmptyFilename`] when `cfg.filename` is empty, as Go does.
pub fn setup(cfg: &Config) -> Result<(), EmptyFilename> {
    let summary = new_stmt_summary(cfg)?;
    set_global_stmt_summary(Some(summary));
    Ok(())
}

/// Go `Close`: closes the `GlobalStmtSummary`.
pub fn close() {
    if let Some(summary) = global_stmt_summary() {
        summary.close();
    }
}

/// Go `Config`: the static configuration of [`StmtSummary`]. It cannot be
/// modified at runtime.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Config {
    /// Go `Filename`.
    pub filename: String,
    /// Go `FileMaxSize`. Carried for parity; [`FileStmtLogWriter`] does not
    /// rotate.
    pub file_max_size: i64,
    /// Go `FileMaxDays`. Carried for parity; [`FileStmtLogWriter`] does not
    /// rotate.
    pub file_max_days: i64,
    /// Go `FileMaxBackups`. Carried for parity; [`FileStmtLogWriter`] does not
    /// rotate.
    pub file_max_backups: i64,
}

/// Narrowing of `metrics.StmtSummaryEvictedLogCounter`.
pub trait EvictedLogMetricsSink: Send + Sync {
    /// Go's `…EvictedLogResultPersisted` counter `Add(n)`.
    fn add_persisted(&self, count: f64);
    /// Go's `…EvictedLogResultDropped` counter `Inc()`.
    fn inc_dropped(&self);
}

/// The default [`EvictedLogMetricsSink`]: publishes nowhere.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopEvictedLogMetricsSink;

impl EvictedLogMetricsSink for NoopEvictedLogMetricsSink {
    fn add_persisted(&self, _count: f64) {}
    fn inc_dropped(&self) {}
}

/// The state Go's `s.onEvict` method reads. Held in an [`Arc`] shared with the
/// LRU eviction closure so the window does not have to reference the summary
/// that owns it.
struct EvictShared {
    /// Go `optPersistEvicted`.
    opt_persist_evicted: AtomicBool,
    /// Go `evictedCh`. `None` is Go's nil channel; [`StmtSummary::close`] sets
    /// it so the log thread's receiver disconnects.
    evicted_tx: Mutex<Option<SyncSender<StmtRecord>>>,
    /// Go `evictedDropped`.
    evicted_dropped: AtomicU64,
    metrics: Arc<dyn EvictedLogMetricsSink>,
}

impl EvictShared {
    /// Go `(*StmtSummary).onEvict`: the LRU eviction hook installed on every
    /// `stmtWindow`.
    ///
    /// Called while the record's lock is held (see [`new_stmt_window`]). We copy
    /// the fields we need and hand the clone off to the async log thread. A
    /// non-blocking send is used so the hot `Add` path never stalls on log I/O.
    fn on_evict(&self, r: &StmtRecord, begin: DateTime<Utc>, end: DateTime<Utc>) -> bool {
        if !self.opt_persist_evicted.load(Ordering::SeqCst) {
            return false;
        }
        let guard = self
            .evicted_tx
            .lock()
            .expect("evicted sender lock poisoned");
        let Some(tx) = guard.as_ref() else {
            return false;
        };
        let mut clone = clone_record_for_log(r);
        clone.begin = begin.timestamp();
        clone.end = end.timestamp();
        if tx.try_send(clone).is_ok() {
            return true;
        }
        self.evicted_dropped.fetch_add(1, Ordering::SeqCst);
        self.metrics.inc_dropped();
        false
    }
}

/// Narrowing of Go's `context.Context` cancellation for the rotate loop.
#[derive(Debug, Default)]
struct Shutdown {
    flag: Mutex<bool>,
    signal: Condvar,
}

impl Shutdown {
    fn cancel(&self) {
        *self.flag.lock().expect("shutdown lock poisoned") = true;
        self.signal.notify_all();
    }

    /// Waits up to `timeout`, returning `true` when cancellation was signalled.
    fn wait(&self, timeout: Duration) -> bool {
        let guard = self.flag.lock().expect("shutdown lock poisoned");
        if *guard {
            return true;
        }
        let (guard, _) = self
            .signal
            .wait_timeout(guard, timeout)
            .expect("shutdown lock poisoned");
        *guard
    }
}

/// Go `StmtSummary`: the complete statements summary statistics. It controls
/// data rotation and persistence internally, and provides reading interface
/// through `MemReader` and `HistoryReader`.
pub struct StmtSummary {
    opt_enabled: AtomicBool,
    opt_enable_internal_query: AtomicBool,
    opt_max_stmt_count: AtomicU32,
    opt_max_sql_length: AtomicU32,
    opt_refresh_interval: AtomicU32,
    opt_group_by_user: AtomicBool,

    /// Go `window` guarded by `windowLock`.
    window: Mutex<Arc<Mutex<StmtWindow>>>,
    /// Go `storage`. Go's tests assign the field directly; the ported field is
    /// swapped through [`StmtSummary::set_storage`].
    storage: RwLock<Arc<dyn StmtStorage>>,
    /// Go `closed`.
    closed: AtomicBool,

    evict: Arc<EvictShared>,
    metrics: Arc<dyn WindowMetricsSink>,

    shutdown: Arc<Shutdown>,
    /// Go `closeWg`.
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for StmtSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StmtSummary")
            .field("enabled", &self.enabled())
            .field("max_stmt_count", &self.max_stmt_count())
            .field("refresh_interval", &self.refresh_interval())
            .field("closed", &self.closed.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl StmtSummary {
    /// The option block both Go constructors fill identically.
    fn with_options(
        max_stmt_count: u32,
        refresh_interval: u32,
        capacity: usize,
        storage: Arc<dyn StmtStorage>,
        metrics: Arc<dyn WindowMetricsSink>,
        evicted_metrics: Arc<dyn EvictedLogMetricsSink>,
    ) -> (Arc<Self>, Receiver<StmtRecord>) {
        let (tx, rx) = sync_channel(EVICTED_LOG_CHAN_CAP);
        let evict = Arc::new(EvictShared {
            opt_persist_evicted: AtomicBool::new(false),
            evicted_tx: Mutex::new(Some(tx)),
            evicted_dropped: AtomicU64::new(0),
            metrics: evicted_metrics,
        });
        let hook = {
            let evict = Arc::clone(&evict);
            OnEvictFn::new(move |_key, record, begin, end| evict.on_evict(record, begin, end))
        };
        let window = new_stmt_window(time_now(), capacity, Some(hook));
        let summary = Arc::new(Self {
            opt_enabled: AtomicBool::new(DEFAULT_ENABLED),
            opt_enable_internal_query: AtomicBool::new(DEFAULT_ENABLE_INTERNAL_QUERY),
            opt_max_stmt_count: AtomicU32::new(max_stmt_count),
            opt_max_sql_length: AtomicU32::new(DEFAULT_MAX_SQL_LENGTH),
            opt_refresh_interval: AtomicU32::new(refresh_interval),
            opt_group_by_user: AtomicBool::new(false),
            window: Mutex::new(Arc::new(Mutex::new(window))),
            storage: RwLock::new(storage),
            closed: AtomicBool::new(false),
            evict,
            metrics,
            shutdown: Arc::new(Shutdown::default()),
            threads: Mutex::new(Vec::new()),
        });
        (summary, rx)
    }

    fn spawn_evicted_log_loop(self: &Arc<Self>, rx: Receiver<StmtRecord>) {
        let summary = Arc::clone(self);
        let handle = std::thread::spawn(move || summary.evicted_log_loop(&rx));
        self.threads
            .lock()
            .expect("threads lock poisoned")
            .push(handle);
    }

    fn spawn_rotate_loop(self: &Arc<Self>) {
        let summary = Arc::clone(self);
        let handle = std::thread::spawn(move || summary.rotate_loop());
        self.threads
            .lock()
            .expect("threads lock poisoned")
            .push(handle);
    }

    /// Go `NewStmtSummary4Test`: creates a new `StmtSummary` for testing
    /// purposes.
    #[must_use]
    pub fn new_for_test(max_stmt_count: usize) -> Arc<Self> {
        let (summary, rx) = Self::with_options(
            DEFAULT_MAX_STMT_COUNT,
            60 * 60 * 24 * 365, // 1 year
            max_stmt_count,
            Arc::new(MockStmtStorage::default()),
            Arc::new(crate::statement_summary::NoopWindowMetricsSink),
            Arc::new(NoopEvictedLogMetricsSink),
        );
        summary.spawn_evicted_log_loop(rx);
        summary
    }

    /// Builds a test summary over caller-supplied sinks, so the upstream tests
    /// that read Go's process-global Prometheus metrics can read them here.
    #[must_use]
    pub fn new_for_test_with_sinks(
        max_stmt_count: usize,
        storage: Arc<dyn StmtStorage>,
        metrics: Arc<dyn WindowMetricsSink>,
        evicted_metrics: Arc<dyn EvictedLogMetricsSink>,
    ) -> Arc<Self> {
        let (summary, rx) = Self::with_options(
            DEFAULT_MAX_STMT_COUNT,
            60 * 60 * 24 * 365,
            max_stmt_count,
            storage,
            metrics,
            evicted_metrics,
        );
        summary.spawn_evicted_log_loop(rx);
        summary
    }

    /// Go's `s.storage = storage` in the upstream tests.
    pub fn set_storage(&self, storage: Arc<dyn StmtStorage>) {
        *self.storage.write().expect("storage lock poisoned") = storage;
    }

    /// The storage this summary persists through.
    #[must_use]
    pub fn storage(&self) -> Arc<dyn StmtStorage> {
        Arc::clone(&self.storage.read().expect("storage lock poisoned"))
    }

    /// Go's `s.window`, which the upstream tests read directly.
    #[must_use]
    pub fn window(&self) -> Arc<Mutex<StmtWindow>> {
        Arc::clone(&self.window.lock().expect("window lock poisoned"))
    }

    /// Go `evictedDropped`.
    #[must_use]
    pub fn evicted_dropped(&self) -> u64 {
        self.evict.evicted_dropped.load(Ordering::SeqCst)
    }

    /// Go `(*StmtSummary).Enabled`.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.opt_enabled.load(Ordering::SeqCst)
    }

    /// Go `(*StmtSummary).SetEnabled`: enables or disables the `StmtSummary`.
    /// If disabled, in-memory data will be cleared (persisted data will still be
    /// remained).
    pub fn set_enabled(&self, v: bool) {
        self.opt_enabled.store(v, Ordering::SeqCst);
        if !v {
            self.clear();
        }
    }

    /// Go `(*StmtSummary).EnableInternalQuery`.
    #[must_use]
    pub fn enable_internal_query(&self) -> bool {
        self.opt_enable_internal_query.load(Ordering::SeqCst)
    }

    /// Go `(*StmtSummary).SetEnableInternalQuery`: enables or disables the
    /// internal-query statistics. If disabled, in-memory internal queries will
    /// be cleared (persisted internal queries will still be remained).
    pub fn set_enable_internal_query(&self, v: bool) {
        self.opt_enable_internal_query.store(v, Ordering::SeqCst);
        if !v {
            self.clear_internal();
        }
    }

    /// Go `(*StmtSummary).MaxStmtCount`.
    #[must_use]
    pub fn max_stmt_count(&self) -> u32 {
        self.opt_max_stmt_count.load(Ordering::SeqCst)
    }

    /// Go `(*StmtSummary).SetMaxStmtCount`: sets the maximum number of
    /// statements. If the current number exceeds the maximum number, the excess
    /// will be evicted.
    pub fn set_max_stmt_count(&self, v: u32) {
        let v = if v < 1 { 1 } else { v };
        self.opt_max_stmt_count.store(v, Ordering::SeqCst);
        let guard = self.window.lock().expect("window lock poisoned");
        let mut window = guard.lock().expect("window contents lock poisoned");
        // Go discards SetCapacity's error the same way.
        let _ = window.lru.set_capacity(v as usize);
    }

    /// Go `(*StmtSummary).MaxSQLLength`.
    #[must_use]
    pub fn max_sql_length(&self) -> u32 {
        self.opt_max_sql_length.load(Ordering::SeqCst)
    }

    /// Go `(*StmtSummary).SetMaxSQLLength`.
    pub fn set_max_sql_length(&self, v: u32) {
        self.opt_max_sql_length.store(v, Ordering::SeqCst);
    }

    /// Go `(*StmtSummary).RefreshInterval`: the period (in seconds) at which
    /// the statistics window is refreshed (persisted).
    #[must_use]
    pub fn refresh_interval(&self) -> u32 {
        self.opt_refresh_interval.load(Ordering::SeqCst)
    }

    /// Go `(*StmtSummary).SetRefreshInterval`.
    pub fn set_refresh_interval(&self, v: u32) {
        let v = if v < 1 { 1 } else { v };
        self.opt_refresh_interval.store(v, Ordering::SeqCst);
    }

    /// Go `(*StmtSummary).PersistEvicted`: reports whether per-record evictions
    /// are persisted.
    #[must_use]
    pub fn persist_evicted(&self) -> bool {
        self.evict.opt_persist_evicted.load(Ordering::SeqCst)
    }

    /// Go `(*StmtSummary).SetPersistEvicted`.
    pub fn set_persist_evicted(&self, v: bool) {
        self.evict.opt_persist_evicted.store(v, Ordering::SeqCst);
    }

    /// Go `(*StmtSummary).GroupByUser`: reports whether statement summaries are
    /// grouped by the executing user in addition to the usual
    /// digest/schema/plan tuple.
    #[must_use]
    pub fn group_by_user(&self) -> bool {
        self.opt_group_by_user.load(Ordering::SeqCst)
    }

    /// Go `(*StmtSummary).SetGroupByUser`: toggles user-dimension grouping.
    /// Switching the flag clears the in-memory window because existing records
    /// were aggregated under a different grouping key; persisted records are
    /// unaffected.
    pub fn set_group_by_user(&self, v: bool) {
        // Hold windowLock across the flag flip and clear so Add (which reads
        // the flag under the same lock) cannot insert a record with the old
        // grouping mode after the window is cleared.
        let guard = self.window.lock().expect("window lock poisoned");
        if self.opt_group_by_user.load(Ordering::SeqCst) == v {
            return;
        }
        self.opt_group_by_user.store(v, Ordering::SeqCst);
        guard.lock().expect("window contents lock poisoned").clear();
    }

    /// Go `(*StmtSummary).Add`: adds a single `StmtExecInfo` to the current
    /// statistics window. Before adding, it will check whether the current
    /// window has expired, and if it has expired, the window will be persisted
    /// asynchronously and a new window will be created to replace the current
    /// one.
    pub fn add(&self, info: &StmtExecInfo) {
        if self.closed.load(Ordering::SeqCst) {
            return;
        }

        let mut k = StmtDigestKey::new();

        // Add info to the current statistics window.
        let guard = self.window.lock().expect("window lock poisoned");
        if self.closed.load(Ordering::SeqCst) {
            return;
        }
        // Decide userForKey under windowLock so SetGroupByUser's flag flip +
        // clear is atomic w.r.t. Add; otherwise a post-clear insert could land
        // under the wrong grouping mode.
        let user_for_key = if self.opt_group_by_user.load(Ordering::SeqCst) {
            info.user.as_str()
        } else {
            ""
        };
        k.init(
            &info.schema_name,
            &info.digest,
            &info.prev_sql_digest,
            &info.plan_digest,
            &info.resource_group_name,
            user_for_key,
        );
        let record = {
            let mut window = guard.lock().expect("window contents lock poisoned");
            let existing = window.lru.get(&k).map(Arc::clone);
            if let Some(existing) = existing {
                existing
            } else {
                let record = Arc::new(Mutex::new(new_stmt_record(info)));
                window.lru.put(k, Arc::clone(&record));
                record
            }
        };
        drop(guard);

        record.lock().expect("record lock poisoned").add(info);
    }

    /// Go `(*StmtSummary).Evicted`: returns the number of statements evicted
    /// for the current time window. The returned value is one row consisting of
    /// three columns: `[BEGIN_TIME, END_TIME, EVICTED_COUNT]`.
    ///
    /// Go reads `s.window.begin` after releasing `windowLock`; the port reads it
    /// under the same lock as the count.
    #[must_use]
    pub fn evicted(&self) -> Vec<Datum> {
        let (count, begin) = {
            let guard = self.window.lock().expect("window lock poisoned");
            let window = guard.lock().expect("window contents lock poisoned");
            (window.evicted_count_distinct() as i64, window.begin)
        };
        if count == 0 {
            return Vec::new();
        }
        vec![
            timestamp_datum(begin),
            timestamp_datum(time_now()),
            Datum::new_int(count),
        ]
    }

    /// Go `(*StmtSummary).Clear`: clears all data in the current window; the
    /// data that has been persisted is not cleared.
    pub fn clear(&self) {
        let guard = self.window.lock().expect("window lock poisoned");
        guard.lock().expect("window contents lock poisoned").clear();
    }

    /// Go `(*StmtSummary).ClearInternal`: clears all internal queries of the
    /// current window; the data that has been persisted is not cleared.
    pub fn clear_internal(&self) {
        let guard = self.window.lock().expect("window lock poisoned");
        let mut window = guard.lock().expect("window contents lock poisoned");
        let internal: Vec<StmtDigestKey> = window
            .lru
            .keys()
            .into_iter()
            .zip(window.lru.values())
            .filter(|(_, value)| value.lock().expect("record lock poisoned").is_internal)
            .map(|(key, _)| key.clone())
            .collect();
        for key in internal {
            window.lru.delete(&key);
        }
    }

    /// Go `(*StmtSummary).Close`: closes the work of `StmtSummary`.
    pub fn close(&self) {
        {
            let _guard = self.window.lock().expect("window lock poisoned");
            if self
                .closed
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                return;
            }
        }

        // Go's `s.cancel()` then `s.closeWg.Wait()`. Dropping the sender is what
        // lets the evicted-log thread see end-of-stream once it has drained.
        self.shutdown.cancel();
        self.evict
            .evicted_tx
            .lock()
            .expect("evicted sender lock poisoned")
            .take();
        let handles: Vec<JoinHandle<()>> =
            std::mem::take(&mut *self.threads.lock().expect("threads lock poisoned"));
        for handle in handles {
            let _ = handle.join();
        }

        self.flush();
    }

    /// Go `(*StmtSummary).flush`.
    fn flush(&self) {
        let now = time_now();

        let window = {
            let mut guard = self.window.lock().expect("window lock poisoned");
            let previous = Arc::clone(&guard);
            *guard = Arc::new(Mutex::new(self.new_window(now)));
            previous
        };

        let size = window
            .lock()
            .expect("window contents lock poisoned")
            .lru
            .size();
        let storage = self.storage();
        if size > 0 {
            storage.persist(&window, now);
        }
        // Go logs the sync failure; this port has no logger boundary here.
        let _ = storage.sync();
    }

    /// Go `(*StmtSummary).rotateLoop`.
    fn rotate_loop(&self) {
        let tick = Duration::from_secs(DEFAULT_ROTATE_CHECK_INTERVAL);
        loop {
            if self.shutdown.wait(tick) {
                return;
            }
            let now = time_now();
            let mut slot = self.window.lock().expect("window lock poisoned");
            // The current window has expired and needs to be refreshed and
            // persisted.
            let begin = slot.lock().expect("window contents lock poisoned").begin;
            let deadline = begin + chrono::TimeDelta::seconds(i64::from(self.refresh_interval()));
            if now > deadline {
                self.rotate_locked(&mut slot, now);
            }
            self.update_metrics_locked(&slot);
            drop(slot);
        }
    }

    /// Go `(*StmtSummary).updateMetrics`: reports the current window's record
    /// count and eviction count. Must be called with `windowLock` held.
    pub fn update_metrics(&self) {
        let guard = self.window.lock().expect("window lock poisoned");
        self.update_metrics_locked(&guard);
    }

    fn update_metrics_locked(&self, guard: &Arc<Mutex<StmtWindow>>) {
        let window = guard.lock().expect("window contents lock poisoned");
        #[allow(clippy::cast_precision_loss)]
        let record_count = window.lru.size() as f64;
        #[allow(clippy::cast_precision_loss)]
        let evicted_count = window.evicted_count() as f64;
        self.metrics.set_window_metrics(record_count, evicted_count);
    }

    /// Go `(*StmtSummary).rotate`.
    pub fn rotate(&self, now: DateTime<Utc>) {
        let mut slot = self.window.lock().expect("window lock poisoned");
        self.rotate_locked(&mut slot, now);
    }

    /// Go `(*StmtSummary).rotate`, with `windowLock` already held by the caller.
    fn rotate_locked(&self, slot: &mut MutexGuard<'_, Arc<Mutex<StmtWindow>>>, now: DateTime<Utc>) {
        let w = Arc::clone(slot);
        **slot = Arc::new(Mutex::new(self.new_window(now)));
        let size = w.lock().expect("window contents lock poisoned").lru.size();
        if size > 0 {
            // Persist window asynchronously.
            let storage = self.storage();
            let handle = std::thread::spawn(move || storage.persist(&w, now));
            self.threads
                .lock()
                .expect("threads lock poisoned")
                .push(handle);
        }
    }

    fn new_window(&self, begin: DateTime<Utc>) -> StmtWindow {
        let evict = Arc::clone(&self.evict);
        let hook =
            OnEvictFn::new(move |_key, record, begin, end| evict.on_evict(record, begin, end));
        new_stmt_window(begin, self.max_stmt_count() as usize, Some(hook))
    }

    /// Go `(*StmtSummary).evictedLogLoop`: drains `evictedCh` and writes each
    /// record to the stmt log. When `group_by_user` is also enabled, each logged
    /// record represents exactly one `(digest, user)` group that fell out of the
    /// LRU.
    fn evicted_log_loop(&self, rx: &Receiver<StmtRecord>) {
        let mut report_deadline = Instant::now() + EVICTED_DROP_REPORT_INTERVAL;
        let mut flush_deadline: Option<Instant> = None;
        let mut last_drop_report = 0u64;
        let mut batch: Vec<StmtRecord> = Vec::with_capacity(EVICTED_LOG_BATCH_SIZE);

        loop {
            let now = Instant::now();
            let mut timeout = report_deadline.saturating_duration_since(now);
            if let Some(deadline) = flush_deadline {
                timeout = timeout.min(deadline.saturating_duration_since(now));
            }
            match rx.recv_timeout(timeout) {
                Ok(record) => {
                    self.append_record(&mut batch, &mut flush_deadline, record);
                    // Go's drainAvailable.
                    while !batch.is_empty() && batch.len() < EVICTED_LOG_BATCH_SIZE {
                        match rx.try_recv() {
                            Ok(record) => {
                                self.append_record(&mut batch, &mut flush_deadline, record);
                            }
                            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                        }
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    let now = Instant::now();
                    if flush_deadline.is_some_and(|deadline| now >= deadline) {
                        self.flush_batch(&mut batch, &mut flush_deadline);
                    }
                    if now >= report_deadline {
                        last_drop_report = self.report_dropped(last_drop_report);
                        report_deadline = now + EVICTED_DROP_REPORT_INTERVAL;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    // Close sets `closed` while holding windowLock before
                    // dropping the sender, and Add rechecks `closed` under the
                    // same lock. At this point no Add can enqueue more evicted
                    // records, and the receiver has already yielded every
                    // buffered one.
                    self.flush_batch(&mut batch, &mut flush_deadline);
                    let _ = self.report_dropped(last_drop_report);
                    return;
                }
            }
        }
    }

    fn append_record(
        &self,
        batch: &mut Vec<StmtRecord>,
        flush_deadline: &mut Option<Instant>,
        record: StmtRecord,
    ) {
        batch.push(record);
        if batch.len() == 1 {
            *flush_deadline = Some(Instant::now() + EVICTED_LOG_FLUSH_INTERVAL);
        }
        if batch.len() >= EVICTED_LOG_BATCH_SIZE {
            self.flush_batch(batch, flush_deadline);
        }
    }

    fn flush_batch(&self, batch: &mut Vec<StmtRecord>, flush_deadline: &mut Option<Instant>) {
        if batch.is_empty() {
            return;
        }
        self.storage().log_evicted(batch);
        batch.clear();
        *flush_deadline = None;
    }

    /// Go's `report` closure. Go writes a warning through `logutil.BgLogger`;
    /// here the running total stays readable through
    /// [`StmtSummary::evicted_dropped`].
    fn report_dropped(&self, last: u64) -> u64 {
        let current = self.evict.evicted_dropped.load(Ordering::SeqCst);
        if current > last {
            current
        } else {
            last
        }
    }
}

/// Go `NewStmtSummary`: creates a new `StmtSummary` from [`Config`].
///
/// # Errors
///
/// Returns [`EmptyFilename`] when `cfg.filename` is empty, as Go does.
pub fn new_stmt_summary(cfg: &Config) -> Result<Arc<StmtSummary>, EmptyFilename> {
    if cfg.filename.is_empty() {
        return Err(EmptyFilename);
    }

    // These options can be changed dynamically at runtime. The default values
    // here are just placeholders, and the real values in
    // sessionctx/variables/tidb_vars.go will overwrite them after TiDB starts.
    let storage: Arc<dyn StmtStorage> = Arc::new(StmtLogStorage::new(Arc::new(
        FileStmtLogWriter::new(&cfg.filename),
    )));
    let (summary, rx) = StmtSummary::with_options(
        DEFAULT_MAX_STMT_COUNT,
        DEFAULT_REFRESH_INTERVAL,
        DEFAULT_MAX_STMT_COUNT as usize,
        storage,
        Arc::new(crate::statement_summary::NoopWindowMetricsSink),
        Arc::new(NoopEvictedLogMetricsSink),
    );
    summary.spawn_rotate_loop();
    summary.spawn_evicted_log_loop(rx);
    Ok(summary)
}

/// Go `onEvictFn`: invoked for every LRU eviction. The callback receives the
/// locked record (the caller holds the record's lock) so it can copy fields
/// cheaply. It returns `true` when the record has been handed off for
/// per-record persistence, in which case the caller can skip adding it to the
/// persisted aggregate. Must not block.
pub struct OnEvictFn(Box<OnEvictClosure>);

/// The boxed closure [`OnEvictFn`] wraps.
type OnEvictClosure =
    dyn Fn(&StmtDigestKey, &StmtRecord, DateTime<Utc>, DateTime<Utc>) -> bool + Send + Sync;

impl OnEvictFn {
    /// Wraps a closure as Go's `onEvictFn`.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&StmtDigestKey, &StmtRecord, DateTime<Utc>, DateTime<Utc>) -> bool
            + Send
            + Sync
            + 'static,
    {
        Self(Box::new(f))
    }
}

impl std::fmt::Debug for OnEvictFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OnEvictFn")
    }
}

/// Go `lockedStmtRecord`: Go's `sync.Mutex` embedded next to a `*StmtRecord`.
pub type LockedStmtRecord = Arc<Mutex<StmtRecord>>;

/// Go `stmtWindow`: a single statistical window, which has a begin time and an
/// end time. Data within a single window is eliminated according to the LRU
/// strategy. All evicted data is aggregated into [`StmtEvicted`].
pub struct StmtWindow {
    /// Go `begin`.
    pub begin: DateTime<Utc>,
    /// Go `lru` (`*StmtDigestKey => *lockedStmtRecord`).
    pub lru: SimpleLruCache<StmtDigestKey, LockedStmtRecord>,
    /// Go `evicted`. The eviction closure holds the same cell, so
    /// [`StmtWindow::clear`] resets its contents rather than replacing it.
    pub evicted: Arc<Mutex<StmtEvicted>>,
    /// Go `evictedCount`: the total number of LRU evictions in this window.
    pub evicted_count: Arc<AtomicI64>,
}

impl std::fmt::Debug for StmtWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StmtWindow")
            .field("begin", &self.begin)
            .field("size", &self.lru.size())
            .field("evicted_count", &self.evicted_count())
            .finish_non_exhaustive()
    }
}

impl StmtWindow {
    /// Go `w.evictedCount.Load()`.
    #[must_use]
    pub fn evicted_count(&self) -> i64 {
        self.evicted_count.load(Ordering::SeqCst)
    }

    /// Go `w.evicted.count()`: the number of distinct evicted digests.
    #[must_use]
    pub fn evicted_count_distinct(&self) -> usize {
        self.evicted.lock().expect("evicted lock poisoned").count()
    }

    /// Go `w.evicted.other`, cloned out from under its lock.
    #[must_use]
    pub fn evicted_other(&self) -> StmtRecord {
        self.evicted
            .lock()
            .expect("evicted lock poisoned")
            .other
            .clone()
    }

    /// Go `(*stmtWindow).clear`.
    pub fn clear(&mut self) {
        self.lru.delete_all();
        *self.evicted.lock().expect("evicted lock poisoned") = StmtEvicted::new();
        self.evicted_count.store(0, Ordering::SeqCst);
    }
}

/// Go `newStmtWindow`.
///
/// # Panics
///
/// Panics when `capacity` is zero, matching Go's `NewSimpleLRUCache`.
#[must_use]
pub fn new_stmt_window(
    begin: DateTime<Utc>,
    capacity: usize,
    on_evict: Option<OnEvictFn>,
) -> StmtWindow {
    let evicted = Arc::new(Mutex::new(StmtEvicted::new()));
    let evicted_count = Arc::new(AtomicI64::new(0));
    let mut lru: SimpleLruCache<StmtDigestKey, LockedStmtRecord> = SimpleLruCache::new(capacity);
    {
        let evicted = Arc::clone(&evicted);
        let evicted_count = Arc::clone(&evicted_count);
        lru.set_on_evict(move |k: &StmtDigestKey, v: &LockedStmtRecord| {
            evicted_count.fetch_add(1, Ordering::SeqCst);
            let record = v.lock().expect("record lock poisoned");
            let queued_for_evicted_log = on_evict
                .as_ref()
                .is_some_and(|hook| (hook.0)(k, &record, begin, time_now()));
            evicted
                .lock()
                .expect("evicted lock poisoned")
                .add(k, &record, queued_for_evicted_log);
        });
    }
    StmtWindow {
        begin,
        lru,
        evicted,
        evicted_count,
    }
}

/// Go `stmtStorage`.
pub trait StmtStorage: Send + Sync {
    /// Go `persist`.
    fn persist(&self, window: &Arc<Mutex<StmtWindow>>, end: DateTime<Utc>);
    /// Go `logEvicted`: writes evicted records to durable storage. It may be
    /// called concurrently with `persist`; implementations must be safe to call
    /// from the evicted-log thread.
    fn log_evicted(&self, records: &[StmtRecord]);
    /// Go `sync`.
    ///
    /// # Errors
    ///
    /// Returns whatever the underlying sink reports, as Go's `sync` does.
    fn sync(&self) -> std::io::Result<()>;
}

/// Go `stmtEvicted`. Go's embedded `sync.Mutex` moves outside the type: the
/// window holds it as `Arc<Mutex<..>>`, as v1's `evicted.rs` already does.
#[derive(Clone, Debug)]
pub struct StmtEvicted {
    /// Go `keys`.
    keys: HashSet<Vec<u8>>,
    /// Go `other`: all evicted records in the current window.
    pub other: StmtRecord,
    /// Go `otherForPersist`: records not covered by per-record evicted logs.
    /// When per-record evicted logging is disabled, it is equivalent to
    /// [`StmtEvicted::other`].
    pub other_for_persist: StmtRecord,
}

impl Default for StmtEvicted {
    fn default() -> Self {
        Self::new()
    }
}

impl StmtEvicted {
    /// Go `newStmtEvicted`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            keys: HashSet::new(),
            other: new_evicted_aggregate_record(),
            other_for_persist: new_evicted_aggregate_record(),
        }
    }

    /// Go `(*stmtEvicted).add`. Go's `key == nil || record == nil` guard has no
    /// Rust counterpart: both arrive as references.
    pub fn add(&mut self, key: &StmtDigestKey, record: &StmtRecord, queued_for_evicted_log: bool) {
        self.keys.insert(key.hash().to_vec());
        self.other.merge(record);
        if !queued_for_evicted_log {
            self.other_for_persist.merge(record);
        }
    }

    /// Go `(*stmtEvicted).count`.
    #[must_use]
    pub fn count(&self) -> usize {
        self.keys.len()
    }
}

/// Go `newEvictedAggregateRecord`.
#[must_use]
pub fn new_evicted_aggregate_record() -> StmtRecord {
    let now = Utc::now();
    StmtRecord {
        // Go `time.Duration(math.MaxInt64)`.
        min_latency: Duration::from_nanos(u64::try_from(i64::MAX).unwrap_or(u64::MAX)),
        first_seen: now,
        last_seen: now,
        ..StmtRecord::default()
    }
}

/// Go `cloneRecordForLog`: returns a copy of `r` so the async logger can
/// marshal the snapshot without racing with further updates on the retained
/// `StmtRecord`. Rust's `Clone` is already deep, so Go's explicit map copies
/// are implicit here.
#[must_use]
pub fn clone_record_for_log(r: &StmtRecord) -> StmtRecord {
    r.clone()
}

/// Go `mockStmtStorage`.
#[derive(Debug, Default)]
pub struct MockStmtStorage {
    inner: Mutex<MockStmtStorageInner>,
}

/// The state Go guards with `mockStmtStorage`'s embedded `sync.Mutex`.
#[derive(Debug, Default)]
struct MockStmtStorageInner {
    windows: Vec<Arc<Mutex<StmtWindow>>>,
    evicted: Vec<StmtRecord>,
}

impl MockStmtStorage {
    /// Go `s.windows`.
    #[must_use]
    pub fn windows(&self) -> Vec<Arc<Mutex<StmtWindow>>> {
        self.inner
            .lock()
            .expect("mock storage lock poisoned")
            .windows
            .clone()
    }

    /// Go `s.evicted`.
    #[must_use]
    pub fn evicted(&self) -> Vec<StmtRecord> {
        self.inner
            .lock()
            .expect("mock storage lock poisoned")
            .evicted
            .clone()
    }
}

impl StmtStorage for MockStmtStorage {
    fn persist(&self, window: &Arc<Mutex<StmtWindow>>, _end: DateTime<Utc>) {
        self.inner
            .lock()
            .expect("mock storage lock poisoned")
            .windows
            .push(Arc::clone(window));
    }

    fn log_evicted(&self, records: &[StmtRecord]) {
        self.inner
            .lock()
            .expect("mock storage lock poisoned")
            .evicted
            .extend_from_slice(records);
    }

    fn sync(&self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The rotating-file sink Go reaches through zap + lumberjack in
/// `v2/logger.go`, isolated so nothing here performs filesystem I/O in tests.
pub trait StmtLogWriter: Send + Sync {
    /// Appends one already-marshalled record line.
    fn write_line(&self, line: &str);
    /// Flushes whatever the sink buffered.
    ///
    /// # Errors
    ///
    /// Returns the sink's own error.
    fn sync(&self) -> std::io::Result<()>;
}

/// A [`StmtLogWriter`] that appends lines to a file, opening it lazily on the
/// first write. It performs **no** rotation: Go's size/age/backup limits are
/// lumberjack's, and `v2/logger.go` is not ported.
#[derive(Debug)]
pub struct FileStmtLogWriter {
    path: PathBuf,
}

impl FileStmtLogWriter {
    /// Records the path; the file is created on the first [`write_line`].
    ///
    /// [`write_line`]: StmtLogWriter::write_line
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl StmtLogWriter for FileStmtLogWriter {
    fn write_line(&self, line: &str) {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            let _ = writeln!(file, "{line}");
        }
    }

    fn sync(&self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Go `stmtLogStorage`, reduced to the marshalling and line-writing Go's zap
/// core performs. SEED evidence for `v2/logger.go`.
pub struct StmtLogStorage {
    writer: Arc<dyn StmtLogWriter>,
    metrics: Arc<dyn EvictedLogMetricsSink>,
}

impl std::fmt::Debug for StmtLogStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StmtLogStorage")
    }
}

impl StmtLogStorage {
    /// Builds a storage over `writer` with no metrics sink.
    #[must_use]
    pub fn new(writer: Arc<dyn StmtLogWriter>) -> Self {
        Self {
            writer,
            metrics: Arc::new(NoopEvictedLogMetricsSink),
        }
    }

    /// Builds a storage over `writer` publishing to `metrics`.
    #[must_use]
    pub fn with_metrics(
        writer: Arc<dyn StmtLogWriter>,
        metrics: Arc<dyn EvictedLogMetricsSink>,
    ) -> Self {
        Self { writer, metrics }
    }
}

impl StmtStorage for StmtLogStorage {
    fn persist(&self, window: &Arc<Mutex<StmtWindow>>, end: DateTime<Utc>) {
        let window = window.lock().expect("window contents lock poisoned");
        let begin = window.begin.timestamp();
        for value in window.lru.values() {
            let mut record = value.lock().expect("record lock poisoned");
            record.begin = begin;
            record.end = end.timestamp();
            if let Ok(bytes) = marshal_stmt_record(&record) {
                self.writer.write_line(&String::from_utf8_lossy(&bytes));
            }
        }
        let mut evicted = window.evicted.lock().expect("evicted lock poisoned");
        if evicted.other_for_persist.exec_count > 0 {
            evicted.other_for_persist.begin = begin;
            evicted.other_for_persist.end = end.timestamp();
            if let Ok(bytes) = marshal_stmt_record(&evicted.other_for_persist) {
                self.writer.write_line(&String::from_utf8_lossy(&bytes));
            }
        }
    }

    fn log_evicted(&self, records: &[StmtRecord]) {
        let mut builder = String::new();
        let mut persisted = 0usize;
        for record in records {
            let Ok(bytes) = marshal_evicted_stmt_record(record) else {
                continue;
            };
            if !builder.is_empty() {
                builder.push('\n');
            }
            builder.push_str(&String::from_utf8_lossy(&bytes));
            persisted += 1;
        }
        if builder.is_empty() {
            return;
        }
        self.writer.write_line(&builder);
        #[allow(clippy::cast_precision_loss)]
        self.metrics.add_persisted(persisted as f64);
    }

    fn sync(&self) -> std::io::Result<()> {
        self.writer.sync()
    }
}

/* Public proxy functions between v1 and v2 */

/// Go's `config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent`.
fn enable_persistent() -> bool {
    tidb_config::config_tree::config::get_global_config()
        .instance
        .stmt_summary_enable_persistent
}

/// Go's unchecked `GlobalStmtSummary` dereference in the proxy functions.
fn require_global() -> Arc<StmtSummary> {
    global_stmt_summary()
        .expect("GlobalStmtSummary must be set up before the v2 proxy functions are called")
}

/// Go `Add`: wraps `GlobalStmtSummary.Add` and
/// `stmtsummary.StmtSummaryByDigestMap.AddStatement`.
pub fn add(stmt_exec_info: &StmtExecInfo) {
    if enable_persistent() {
        require_global().add(stmt_exec_info);
    } else {
        STMT_SUMMARY_BY_DIGEST_MAP.add_statement(stmt_exec_info);
    }
}

/// Go `Enabled`.
#[must_use]
pub fn enabled() -> bool {
    if enable_persistent() {
        return require_global().enabled();
    }
    STMT_SUMMARY_BY_DIGEST_MAP.enabled()
}

/// Go `EnabledInternal`.
#[must_use]
pub fn enabled_internal() -> bool {
    if enable_persistent() {
        return require_global().enable_internal_query();
    }
    STMT_SUMMARY_BY_DIGEST_MAP.enabled_internal()
}

/// Go `SetEnabled`.
pub fn set_enabled(v: bool) {
    if enable_persistent() {
        require_global().set_enabled(v);
    } else {
        STMT_SUMMARY_BY_DIGEST_MAP.set_enabled(v);
    }
}

/// Go `SetEnableInternalQuery`.
pub fn set_enable_internal_query(v: bool) {
    if enable_persistent() {
        require_global().set_enable_internal_query(v);
    } else {
        STMT_SUMMARY_BY_DIGEST_MAP.set_enabled_internal_query(v);
    }
}

/// Go `SetRefreshInterval`.
pub fn set_refresh_interval(v: i64) {
    if enable_persistent() {
        require_global().set_refresh_interval(u32::try_from(v).unwrap_or(u32::MAX));
    } else {
        STMT_SUMMARY_BY_DIGEST_MAP.set_refresh_interval(v);
    }
}

/// Go `SetHistorySize`. v2 does not support a history, so the v2 branch is a
/// no-op, as Go's `return nil` is.
pub fn set_history_size(v: i32) {
    if enable_persistent() {
        return; // not support
    }
    STMT_SUMMARY_BY_DIGEST_MAP.set_history_size(v);
}

/// Go `SetMaxStmtCount`.
pub fn set_max_stmt_count(v: i64) {
    if enable_persistent() {
        require_global().set_max_stmt_count(u32::try_from(v).unwrap_or(u32::MAX));
    } else {
        let _ = STMT_SUMMARY_BY_DIGEST_MAP.set_max_stmt_count(u32::try_from(v).unwrap_or(u32::MAX));
    }
}

/// Go `SetMaxSQLLength`.
pub fn set_max_sql_length(v: i32) {
    if enable_persistent() {
        require_global().set_max_sql_length(u32::try_from(v).unwrap_or(u32::MAX));
    } else {
        STMT_SUMMARY_BY_DIGEST_MAP.set_max_sql_length(v);
    }
}

/// Go `SetPersistEvicted`: only v2 (persistent) honors this flag; v1 has no log
/// sink, so the call is a no-op for it.
pub fn set_persist_evicted(v: bool) {
    if let Some(summary) = global_stmt_summary() {
        summary.set_persist_evicted(v);
    }
}

/// Go `SetGroupByUser`: toggles the user dimension on both v1 and v2 so the
/// sysvar setter can call one entry point regardless of which backend is
/// active.
pub fn set_group_by_user(v: bool) {
    STMT_SUMMARY_BY_DIGEST_MAP.set_group_by_user(v);
    if let Some(summary) = global_stmt_summary() {
        summary.set_group_by_user(v);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64 as StdAtomicU64;

    use chrono::TimeZone;

    use super::*;
    use crate::v2::record::generate_stmt_exec_info_4_test;

    /// Go's process-global window gauges, kept per-summary here.
    #[derive(Debug, Default)]
    struct RecordingWindowMetrics {
        record_count: Mutex<f64>,
        evicted_count: Mutex<f64>,
    }

    impl WindowMetricsSink for RecordingWindowMetrics {
        fn set_window_metrics(&self, record_count: f64, evicted_count: f64) {
            *self.record_count.lock().unwrap() = record_count;
            *self.evicted_count.lock().unwrap() = evicted_count;
        }
    }

    /// Go's process-global `StmtSummaryEvictedLogCounter`.
    #[derive(Debug, Default)]
    struct RecordingEvictedMetrics {
        persisted: StdAtomicU64,
        dropped: StdAtomicU64,
    }

    impl EvictedLogMetricsSink for RecordingEvictedMetrics {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        fn add_persisted(&self, count: f64) {
            self.persisted.fetch_add(count as u64, Ordering::SeqCst);
        }

        fn inc_dropped(&self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A [`StmtLogWriter`] collecting lines in memory, standing in for Go's
    /// zap core over a `bytes.Buffer`.
    #[derive(Debug, Default)]
    struct BufferWriter {
        lines: Mutex<Vec<String>>,
    }

    impl StmtLogWriter for BufferWriter {
        fn write_line(&self, line: &str) {
            self.lines.lock().unwrap().push(line.to_owned());
        }

        fn sync(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Polls `condition` until it holds or `timeout` elapses, as Go's
    /// `require.Eventually` does.
    fn eventually(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if condition() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Polls `condition` for `duration`, as Go's `require.Never` does.
    fn never(duration: Duration, mut condition: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + duration;
        while Instant::now() < deadline {
            if condition() {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        true
    }

    /// Go `TestStmtWindow`.
    #[test]
    fn test_stmt_window() {
        let ss = StmtSummary::new_for_test(5);
        for digest in [
            "digest1", "digest1", "digest2", "digest2", "digest3", "digest4", "digest5", "digest6",
            "digest7",
        ] {
            ss.add(&generate_stmt_exec_info_4_test(digest));
        }
        {
            let window = ss.window();
            let window = window.lock().unwrap();
            assert_eq!(window.lru.size(), 5);
            assert_eq!(window.evicted_count_distinct(), 2);
            // digest1 digest1 digest2 digest2
            assert_eq!(window.evicted_other().exec_count, 4);
            assert_eq!(window.evicted_count(), 2);
            // Go asserts `json.Marshal(other)` succeeds.
            assert!(marshal_stmt_record(&window.evicted_other()).is_ok());
        }
        ss.clear();
        {
            let window = ss.window();
            let window = window.lock().unwrap();
            assert_eq!(window.lru.size(), 0);
            assert_eq!(window.evicted_count_distinct(), 0);
            assert_eq!(window.evicted_other().exec_count, 0);
            assert_eq!(window.evicted_count(), 0);
        }
        ss.close();
    }

    /// Go `TestStmtSummary`.
    #[test]
    fn test_stmt_summary() {
        let ss = StmtSummary::new_for_test(3);

        let w = ss.window();
        for digest in ["digest1", "digest2", "digest3", "digest4", "digest5"] {
            ss.add(&generate_stmt_exec_info_4_test(digest));
        }
        {
            let w = w.lock().unwrap();
            assert_eq!(w.lru.size(), 3);
            assert_eq!(w.evicted_count_distinct(), 2);
        }

        ss.rotate(time_now());

        ss.add(&generate_stmt_exec_info_4_test("digest6"));
        ss.add(&generate_stmt_exec_info_4_test("digest7"));
        let w2 = ss.window();
        {
            let w2 = w2.lock().unwrap();
            assert_eq!(w2.lru.size(), 2);
            assert_eq!(w2.evicted_count_distinct(), 0);
        }

        ss.clear();
        assert_eq!(w2.lock().unwrap().lru.size(), 0);
        ss.close();
    }

    /// Go `TestStmtSummaryPersistEvicted`.
    #[test]
    fn test_stmt_summary_persist_evicted() {
        let begin = Utc.with_ymd_and_hms(2026, 5, 25, 10, 0, 0).unwrap();
        let evict_at = begin + chrono::TimeDelta::seconds(42);
        let now = Arc::new(Mutex::new(begin));
        {
            let now = Arc::clone(&now);
            set_time_now(Arc::new(move || *now.lock().unwrap()));
        }

        let storage = Arc::new(MockStmtStorage::default());
        let ss = StmtSummary::new_for_test(2);
        ss.set_storage(Arc::clone(&storage) as Arc<dyn StmtStorage>);
        ss.set_persist_evicted(true);

        // With capacity 2, the 3rd and later distinct digests evict older
        // entries and should each land in storage.evicted().
        ss.add(&generate_stmt_exec_info_4_test("digest1"));
        ss.add(&generate_stmt_exec_info_4_test("digest2"));
        *now.lock().unwrap() = evict_at;
        ss.add(&generate_stmt_exec_info_4_test("digest3")); // evicts digest1
        ss.add(&generate_stmt_exec_info_4_test("digest4")); // evicts digest2

        // The log is async; wait briefly for drain.
        assert!(
            eventually(Duration::from_secs(1), || storage.evicted().len() == 2),
            "expected 2 evicted records to be logged"
        );

        let evicted = storage.evicted();
        let mut digests: Vec<String> = evicted.iter().map(|r| r.digest.clone()).collect();
        for record in &evicted {
            assert_eq!(record.begin, begin.timestamp());
            assert_eq!(record.end, evict_at.timestamp());
        }
        digests.sort();
        assert_eq!(digests, vec!["digest1".to_owned(), "digest2".to_owned()]);

        // Disable and verify no further log writes.
        ss.set_persist_evicted(false);
        ss.add(&generate_stmt_exec_info_4_test("digest5")); // evicts digest3
        assert!(
            never(Duration::from_millis(100), || storage.evicted().len() != 2),
            "evicted count should remain 2 after disabling"
        );

        ss.close();
        reset_time_now();
    }

    /// Go `TestStmtSummaryPersistEvictedDoesNotPersistLoggedRecordsAsAggregate`.
    ///
    /// Go builds a `stmtLogStorage` over a zap core writing into a
    /// `bytes.Buffer` and reads `metrics.StmtSummaryEvictedLogCounter`. Here the
    /// same [`StmtLogStorage`] writes into a [`BufferWriter`] and publishes to a
    /// [`RecordingEvictedMetrics`]; the assertions are unchanged.
    #[test]
    fn test_stmt_summary_persist_evicted_does_not_persist_logged_records_as_aggregate() {
        let writer = Arc::new(BufferWriter::default());
        let metrics = Arc::new(RecordingEvictedMetrics::default());
        let storage = Arc::new(StmtLogStorage::with_metrics(
            Arc::clone(&writer) as Arc<dyn StmtLogWriter>,
            Arc::clone(&metrics) as Arc<dyn EvictedLogMetricsSink>,
        ));

        let ss = StmtSummary::new_for_test(2);
        ss.set_storage(Arc::clone(&storage) as Arc<dyn StmtStorage>);
        ss.set_persist_evicted(true);

        ss.add(&generate_stmt_exec_info_4_test("digest1"));
        ss.add(&generate_stmt_exec_info_4_test("digest2"));
        ss.add(&generate_stmt_exec_info_4_test("digest3")); // evicts digest1
        ss.add(&generate_stmt_exec_info_4_test("digest4")); // evicts digest2
        let persisted_before = metrics.persisted.load(Ordering::SeqCst);
        ss.close();
        let persisted_after = metrics.persisted.load(Ordering::SeqCst);
        assert_eq!(persisted_after - persisted_before, 2);

        let mut total_exec_count = 0i64;
        let mut evicted_digests: Vec<String> = Vec::new();
        let lines = writer.lines.lock().unwrap().clone();
        for chunk in &lines {
            for line in chunk.split('\n') {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                total_exec_count += value["exec_count"].as_i64().unwrap();
                let digest = value["digest"].as_str().unwrap().to_owned();
                if value["evicted"].as_bool().unwrap_or(false) {
                    evicted_digests.push(digest);
                    continue;
                }
                assert!(
                    !digest.is_empty(),
                    "logged evicted records should not also be persisted as the aggregate row"
                );
            }
        }

        evicted_digests.sort();
        assert_eq!(
            evicted_digests,
            vec!["digest1".to_owned(), "digest2".to_owned()]
        );
        assert_eq!(total_exec_count, 4);
    }

    /// Go `stmtExecInfoWithUser`.
    fn stmt_exec_info_with_user(digest: &str, user: &str) -> StmtExecInfo {
        let mut info = generate_stmt_exec_info_4_test(digest);
        info.user = user.to_owned();
        info
    }

    /// Go `TestStmtSummaryGroupByUser`.
    #[test]
    fn test_stmt_summary_group_by_user() {
        let ss = StmtSummary::new_for_test(100);

        // Two statements, same digest, different users: without the flag they
        // should merge into one record.
        ss.add(&stmt_exec_info_with_user("digest1", "alice"));
        ss.add(&stmt_exec_info_with_user("digest1", "bob"));
        assert_eq!(ss.window().lock().unwrap().lru.size(), 1);

        // Switching the flag on clears the window. Re-emitting produces two
        // rows.
        ss.set_group_by_user(true);
        assert_eq!(ss.window().lock().unwrap().lru.size(), 0);
        ss.add(&stmt_exec_info_with_user("digest1", "alice"));
        ss.add(&stmt_exec_info_with_user("digest1", "bob"));
        ss.add(&stmt_exec_info_with_user("digest1", "alice"));
        assert_eq!(ss.window().lock().unwrap().lru.size(), 2);

        // When grouping by user, each record's AuthUsers must hold exactly one
        // user — the one that groups it — so SAMPLE_USER naturally reflects the
        // grouping dimension without a dedicated column.
        let mut users = std::collections::HashMap::new();
        {
            let window = ss.window();
            let window = window.lock().unwrap();
            for value in window.lru.values() {
                let record = value.lock().unwrap();
                assert_eq!(record.auth_users.len(), 1);
                for user in &record.auth_users {
                    users.insert(user.clone(), record.exec_count);
                }
            }
        }
        assert_eq!(users["alice"], 2);
        assert_eq!(users["bob"], 1);

        // Turning the flag off again clears and reverts to single-record
        // merging.
        ss.set_group_by_user(false);
        ss.add(&stmt_exec_info_with_user("digest1", "alice"));
        ss.add(&stmt_exec_info_with_user("digest1", "bob"));
        {
            let window = ss.window();
            let window = window.lock().unwrap();
            assert_eq!(window.lru.size(), 1);
            for value in window.lru.values() {
                // Both users merged when grouping is off.
                assert_eq!(value.lock().unwrap().auth_users.len(), 2);
            }
        }
        ss.close();
    }

    /// Go `TestWindowEvictedCountResetOnRotate`.
    #[test]
    fn test_window_evicted_count_reset_on_rotate() {
        let metrics = Arc::new(RecordingWindowMetrics::default());
        let ss = StmtSummary::new_for_test_with_sinks(
            2,
            Arc::new(MockStmtStorage::default()),
            Arc::clone(&metrics) as Arc<dyn WindowMetricsSink>,
            Arc::new(NoopEvictedLogMetricsSink),
        );
        ss.set_max_stmt_count(2);

        // Fill the LRU cache and trigger evictions.
        ss.add(&generate_stmt_exec_info_4_test("digest1"));
        ss.add(&generate_stmt_exec_info_4_test("digest2"));
        ss.add(&generate_stmt_exec_info_4_test("digest3")); // evicts digest1
        ss.add(&generate_stmt_exec_info_4_test("digest4")); // evicts digest2
        {
            let window = ss.window();
            let window = window.lock().unwrap();
            assert_eq!(window.lru.size(), 2);
            assert_eq!(window.evicted_count(), 2);
        }
        ss.update_metrics();
        assert!((*metrics.record_count.lock().unwrap() - 2.0).abs() < f64::EPSILON);
        assert!((*metrics.evicted_count.lock().unwrap() - 2.0).abs() < f64::EPSILON);

        // Rotate creates a new window with a fresh counter.
        ss.rotate(time_now());
        assert_eq!(ss.window().lock().unwrap().evicted_count(), 0);
        ss.update_metrics();
        assert!((*metrics.evicted_count.lock().unwrap()).abs() < f64::EPSILON);

        // Add more records in the new window.
        ss.add(&generate_stmt_exec_info_4_test("digest5"));
        ss.add(&generate_stmt_exec_info_4_test("digest6"));
        ss.add(&generate_stmt_exec_info_4_test("digest7")); // evicts digest5
        {
            let window = ss.window();
            let window = window.lock().unwrap();
            assert_eq!(window.evicted_count(), 1);
            assert_eq!(window.lru.size(), 2);
        }
        ss.update_metrics();
        assert!((*metrics.record_count.lock().unwrap() - 2.0).abs() < f64::EPSILON);
        assert!((*metrics.evicted_count.lock().unwrap() - 1.0).abs() < f64::EPSILON);
        ss.close();
    }

    /// Go `TestStmtSummaryFlush`.
    #[test]
    fn test_stmt_summary_flush() {
        let storage = Arc::new(MockStmtStorage::default());
        let ss = StmtSummary::new_for_test(1000);
        ss.set_storage(Arc::clone(&storage) as Arc<dyn StmtStorage>);

        for _ in 0..2 {
            ss.add(&generate_stmt_exec_info_4_test("digest1"));
            ss.add(&generate_stmt_exec_info_4_test("digest2"));
            ss.add(&generate_stmt_exec_info_4_test("digest3"));
            ss.rotate(time_now());
        }
        ss.add(&generate_stmt_exec_info_4_test("digest1"));
        ss.add(&generate_stmt_exec_info_4_test("digest2"));
        ss.add(&generate_stmt_exec_info_4_test("digest3"));

        ss.close();

        assert_eq!(storage.windows().len(), 3);
    }

    /// Go `TestDefaultConfig`.
    ///
    /// Go points `Filename` at `t.TempDir()`; the ported
    /// [`FileStmtLogWriter`] opens lazily, so this test creates no file.
    #[test]
    fn test_default_config() {
        let cfg = Config {
            filename: "/nonexistent-by-design/tidb-stmtsummary-test.log".to_owned(),
            ..Config::default()
        };
        let ss = new_stmt_summary(&cfg).unwrap();

        // Verify RefreshInterval (should be 1800 = 30 min).
        assert_eq!(ss.refresh_interval(), 1800);
        ss.close();
    }

    /// Go's `NewStmtSummary` error path, which `TestDefaultConfig`'s
    /// `require.NoError` only exercises positively.
    #[test]
    fn test_new_stmt_summary_rejects_empty_filename() {
        assert!(new_stmt_summary(&Config::default()).is_err());
    }
}
