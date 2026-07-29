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

//! The statistics supply line into a node: a shared, atomically swapped table
//! of [`ClusterTableStats`](crate::cluster_stats_load::ClusterTableStats),
//! plumbing only -- no estimation logic lives here.
//!
//! # Shape
//!
//! This mirrors [`crate::catalog_watch::SharedCatalog`] exactly: one
//! `RwLock<Arc<StatsSnapshot>>`, a reader takes an owned `Arc` and holds it
//! for a statement's whole lifetime, and a publish replaces the map whole
//! rather than mutating a live one in place. The reasons are the same reasons
//! `SharedCatalog` gives: a statement must see one consistent stats snapshot
//! even if a reload lands mid-flight, and a publish must never block a
//! reader.
//!
//! # Absent stats is a first-class state
//!
//! A table can be in exactly one of three states here:
//!
//! * Not a key in the map at all: this node has not attempted to load that
//!   table's statistics yet.
//! * [`TableStatsState::Pseudo`]: the cluster was asked and had no
//!   `mysql.stats_meta` row for the table -- never analyzed. This is Go's
//!   `HistogramFromStorageWithPriority`/`StatsMetaCountAndModifyCount`
//!   returning nothing, which is exactly what makes Go's estimator fall back
//!   to `statistics.PseudoTable` (`pkg/statistics/handle/storage/read.go`,
//!   `pkg/statistics/pseudo.go`). This crate must preserve that distinction
//!   rather than inventing a zero-row histogram, so the estimator (a parallel
//!   unit) can make the same fallback decision Go makes.
//! * [`TableStatsState::Loaded`]: real statistics, current as of
//!   `ClusterTableStats::version`.
//!
//! # Refresh cadence
//!
//! Go's stats handle re-reads `mysql.stats_meta` on a plain ticker at
//! `statsLease` (`pkg/domain/domain.go`'s `UpdateStatsWorker`, wired from
//! `NewDomain`'s `statsLease` parameter, commonly `3s`) -- there is no etcd
//! key for a stats change the way `/tidb/ddl/global_schema_version` notifies
//! schema changes and `/tidb/privilege` notifies grants (checked: neither
//! `pkg/statistics/handle` nor `pkg/domain/domain.go`'s stats worker touch an
//! `etcdCli.Watch`; `ANALYZE` becomes visible to a follower only when its own
//! next tick re-reads `stats_meta.version`). [`StatsReloader`] is therefore
//! tick-only, matching Go's real mechanism rather than inventing a watch Go
//! does not have.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::cluster_stats_load::ClusterTableStats;

/// One table's statistics state, as this node currently knows it.
///
/// See the module doc for why a missing map entry, [`Self::Pseudo`], and
/// [`Self::Loaded`] are three different things.
#[derive(Clone, Debug)]
pub enum TableStatsState {
    /// The cluster has no `mysql.stats_meta` row for this table: never
    /// analyzed. Not the same as a table with zero rows analyzed -- that
    /// table would have a `stats_meta` row and simply no histograms.
    Pseudo,
    /// Loaded statistics, current as of `ClusterTableStats::version`.
    Loaded(Arc<ClusterTableStats>),
}

impl TableStatsState {
    /// `mysql.stats_meta.version`, when this table's stats were loaded rather
    /// than pseudo.
    #[must_use]
    pub fn version(&self) -> Option<u64> {
        match self {
            Self::Pseudo => None,
            Self::Loaded(stats) => Some(stats.version),
        }
    }

    /// The loaded statistics, when this table is not pseudo.
    #[must_use]
    pub fn loaded(&self) -> Option<&Arc<ClusterTableStats>> {
        match self {
            Self::Pseudo => None,
            Self::Loaded(stats) => Some(stats),
        }
    }
}

/// The statistics every table this node has attempted to load has, as of one
/// snapshot. Keyed by physical table ID.
pub type StatsSnapshot = BTreeMap<i64, TableStatsState>;

/// How many of a snapshot's known tables are loaded vs pseudo -- the receipt
/// a node's ready/reload event reports so a consumer can assert the supply
/// line actually delivered something.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatsReceipt {
    /// Tables with real, loaded statistics.
    pub loaded: usize,
    /// Tables the cluster reports as never analyzed.
    pub pseudo: usize,
}

impl StatsReceipt {
    /// The number of tables this snapshot has attempted at all.
    #[must_use]
    pub fn total(&self) -> usize {
        self.loaded + self.pseudo
    }
}

/// Counts a snapshot's [`TableStatsState`]s into a [`StatsReceipt`].
#[must_use]
pub fn receipt_of(snapshot: &StatsSnapshot) -> StatsReceipt {
    let mut receipt = StatsReceipt::default();
    for state in snapshot.values() {
        match state {
            TableStatsState::Loaded(_) => receipt.loaded += 1,
            TableStatsState::Pseudo => receipt.pseudo += 1,
        }
    }
    receipt
}

/// The statistics snapshot every query reads, replaced whole by the reload
/// thread. Same shared/atomic-swap shape as
/// [`crate::catalog_watch::SharedCatalog`] -- see the module doc.
#[derive(Debug)]
pub struct SharedStats {
    published: RwLock<Arc<StatsSnapshot>>,
}

impl SharedStats {
    /// Publishes an initial snapshot, normally the node's startup load.
    #[must_use]
    pub fn new(snapshot: StatsSnapshot) -> Self {
        Self {
            published: RwLock::new(Arc::new(snapshot)),
        }
    }

    /// The statistics in force now. A poisoned lock still yields the value: a
    /// panicking publisher cannot leave the node unable to plan queries.
    #[must_use]
    pub fn load(&self) -> Arc<StatsSnapshot> {
        match self.published.read() {
            Ok(guard) => Arc::clone(&guard),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        }
    }

    /// Replaces the published snapshot atomically.
    pub fn store(&self, snapshot: StatsSnapshot) {
        let mut guard = match self.published.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = Arc::new(snapshot);
    }

    /// The receipt for the snapshot in force now.
    #[must_use]
    pub fn receipt(&self) -> StatsReceipt {
        receipt_of(&self.load())
    }
}

/// Why a stats reload thread could not be started or stopped.
#[derive(Debug)]
pub enum StatsReloadError {
    /// A zero tick would spin the reload thread against PD without pause.
    ZeroInterval,
    /// The reload thread could not be created.
    Spawn(std::io::Error),
    /// The reload thread panicked.
    WorkerPanicked,
}

impl std::fmt::Display for StatsReloadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroInterval => formatter.write_str("stats reload interval must be nonzero"),
            Self::Spawn(error) => write!(formatter, "failed to spawn stats reloader: {error}"),
            Self::WorkerPanicked => formatter.write_str("stats reloader panicked"),
        }
    }
}

impl std::error::Error for StatsReloadError {}

#[derive(Debug, Default)]
struct StatsReloadCounters {
    passes: AtomicU64,
    reloads: AtomicU64,
    failures: AtomicU64,
}

/// What the reload thread has done so far, for tests and for operators.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatsReloadStats {
    /// Passes that ran to a decision, successful or not.
    pub passes: u64,
    /// Passes that found at least one table's `version` had moved and
    /// published the re-read snapshot.
    pub reloads: u64,
    /// Passes whose read failed; the previously published snapshot stays in
    /// force.
    pub failures: u64,
}

#[derive(Debug, Default)]
struct StatsReloadSignal {
    shutdown: bool,
}

/// One reload pass's read step: read every tracked table's statistics fresh
/// and answer the new snapshot, or a reason it could not be read.
///
/// Kept as an injectable closure, the same way
/// [`crate::catalog_watch::ReloadPass`] and `PrivilegeReloadRead` are, so the
/// thread's condvar/shutdown machinery can be tested without PD or TiKV.
pub type StatsReloadRead = Box<dyn FnMut() -> Result<StatsSnapshot, String> + Send + 'static>;

/// Re-reads a node's tracked tables' statistics on a plain tick.
///
/// See the module doc for why this has no watch/nudge half the way
/// [`crate::catalog_watch::CatalogReloader`] does: Go's own stats refresh is
/// tick-only.
#[derive(Debug)]
pub struct StatsReloader {
    signal: Arc<(Mutex<StatsReloadSignal>, Condvar)>,
    stats: Arc<StatsReloadCounters>,
    worker: Option<JoinHandle<()>>,
}

impl StatsReloader {
    /// Starts the reload thread ticking every `interval`, publishing into
    /// `shared` whenever a pass's read reports a snapshot that differs from
    /// the one currently published.
    pub fn spawn(
        shared: Arc<SharedStats>,
        interval: Duration,
        mut read: StatsReloadRead,
    ) -> Result<Self, StatsReloadError> {
        if interval.is_zero() {
            return Err(StatsReloadError::ZeroInterval);
        }
        let signal = Arc::new((Mutex::new(StatsReloadSignal::default()), Condvar::new()));
        let stats = Arc::new(StatsReloadCounters::default());
        let worker_signal = Arc::clone(&signal);
        let worker_stats = Arc::clone(&stats);
        let worker = std::thread::Builder::new()
            .name("stats-reloader".to_owned())
            .spawn(move || {
                let (lock, condvar) = &*worker_signal;
                loop {
                    // Waiting on the condvar rather than sleeping is what
                    // makes shutdown prompt: a stop does not wait out the
                    // interval.
                    let mut state = match lock.lock() {
                        Ok(state) => state,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    if !state.shutdown {
                        state = match condvar.wait_timeout(state, interval) {
                            Ok((state, _)) => state,
                            Err(poisoned) => poisoned.into_inner().0,
                        };
                    }
                    let stopping = state.shutdown;
                    drop(state);
                    if stopping {
                        return;
                    }
                    run_one_stats_reload_pass(&shared, read.as_mut(), &worker_stats);
                }
            })
            .map_err(StatsReloadError::Spawn)?;
        Ok(Self {
            signal,
            stats,
            worker: Some(worker),
        })
    }

    /// What the thread has done so far.
    #[must_use]
    pub fn stats(&self) -> StatsReloadStats {
        StatsReloadStats {
            passes: self.stats.passes.load(Ordering::Acquire),
            reloads: self.stats.reloads.load(Ordering::Acquire),
            failures: self.stats.failures.load(Ordering::Acquire),
        }
    }

    /// Stops the thread and waits for it, reporting a panicking worker.
    ///
    /// Idempotent: [`Drop`] calls it, so an explicit call is only needed when
    /// the caller wants to observe the failure.
    pub fn shutdown(&mut self) -> Result<(), StatsReloadError> {
        let (lock, condvar) = &*self.signal;
        {
            let mut state = match lock.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.shutdown = true;
        }
        condvar.notify_all();
        match self.worker.take() {
            Some(worker) => worker.join().map_err(|_| StatsReloadError::WorkerPanicked),
            None => Ok(()),
        }
    }
}

impl Drop for StatsReloader {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Whether two snapshots' loaded versions differ, table by table.
///
/// A table whose presence/absence or `Pseudo`/`Loaded` state itself changed
/// counts as a difference too -- a table analyzed for the first time since
/// the last pass must publish just as much as a table whose version moved.
fn snapshots_differ(current: &StatsSnapshot, next: &StatsSnapshot) -> bool {
    if current.len() != next.len() {
        return true;
    }
    current
        .iter()
        .any(|(table_id, state)| match next.get(table_id) {
            Some(next_state) => state.version() != next_state.version(),
            // A table dropped from the tracked set between passes.
            None => true,
        })
}

/// Runs one pass: read every tracked table fresh, and publish only when at
/// least one table's version actually moved -- exactly as Go re-reads
/// `stats_meta` every tick but only replaces its cached `*statistics.Table`
/// when the read version differs from the cached one.
fn run_one_stats_reload_pass(
    shared: &SharedStats,
    read: &mut dyn FnMut() -> Result<StatsSnapshot, String>,
    stats: &StatsReloadCounters,
) {
    stats.passes.fetch_add(1, Ordering::AcqRel);
    match read() {
        Ok(next) => {
            let current = shared.load();
            if snapshots_differ(&current, &next) {
                let receipt = receipt_of(&next);
                shared.store(next);
                stats.reloads.fetch_add(1, Ordering::AcqRel);
                eprintln!(
                    "{{\"event\":\"stats_reloaded\",\"loaded\":{},\"pseudo\":{}}}",
                    receipt.loaded, receipt.pseudo
                );
            }
        }
        Err(message) => {
            stats.failures.fetch_add(1, Ordering::AcqRel);
            eprintln!(
                "{{\"event\":\"stats_reload_failed\",\"error\":{}}}",
                serde_json::to_string(&message).unwrap_or_else(|_| "\"unprintable\"".to_owned())
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Instant;

    use tidb_stats::histogram::Histogram;

    use super::*;

    fn loaded_at(table_id: i64, version: u64) -> (i64, TableStatsState) {
        (
            table_id,
            TableStatsState::Loaded(Arc::new(ClusterTableStats {
                table_id,
                version,
                modify_count: 0,
                row_count: 0,
                columns: Vec::new(),
                indexes: Vec::new(),
            })),
        )
    }

    #[test]
    fn a_published_snapshot_replaces_the_previous_one_whole() {
        let shared = SharedStats::new(StatsSnapshot::from([loaded_at(1, 10)]));
        let held = shared.load();
        shared.store(StatsSnapshot::from([loaded_at(1, 20)]));
        // The in-flight reader keeps its own version; the next reader sees
        // the new one -- the whole consistency contract of the swap.
        assert_eq!(held[&1].version(), Some(10));
        assert_eq!(shared.load()[&1].version(), Some(20));
    }

    #[test]
    fn a_table_absent_from_the_map_is_distinct_from_pseudo() {
        let snapshot = StatsSnapshot::from([(1, TableStatsState::Pseudo)]);
        assert!(snapshot.contains_key(&1));
        assert!(!snapshot.contains_key(&2));
        assert_eq!(snapshot[&1].version(), None);
    }

    #[test]
    fn a_receipt_counts_loaded_and_pseudo_tables_separately() {
        let snapshot = StatsSnapshot::from([loaded_at(1, 5), (2, TableStatsState::Pseudo)]);
        let receipt = receipt_of(&snapshot);
        assert_eq!(
            receipt,
            StatsReceipt {
                loaded: 1,
                pseudo: 1
            }
        );
        assert_eq!(receipt.total(), 2);
    }

    #[test]
    fn a_histogram_carrying_table_still_reports_its_version() {
        // Guards against the receipt/version accessors only ever having been
        // exercised on an empty `ClusterTableStats`.
        let stats = ClusterTableStats {
            table_id: 9,
            version: 42,
            modify_count: 3,
            row_count: 100,
            columns: vec![crate::cluster_stats_load::ClusterStatsItem {
                id: 1,
                is_index: false,
                stats_ver: 2,
                flag: 0,
                histogram: Histogram {
                    id: 1,
                    ndv: 10,
                    null_count: 0,
                    last_update_version: 42,
                    tot_col_size: 0,
                    correlation: 0.0,
                    buckets: Vec::new(),
                },
                topn: None,
                cms: None,
            }],
            indexes: Vec::new(),
        };
        let state = TableStatsState::Loaded(Arc::new(stats));
        assert_eq!(state.version(), Some(42));
        assert!(state.loaded().is_some());
    }

    #[test]
    fn a_zero_interval_is_refused_rather_than_spinning() {
        let error = StatsReloader::spawn(
            Arc::new(SharedStats::new(StatsSnapshot::new())),
            Duration::ZERO,
            Box::new(|| Ok(StatsSnapshot::new())),
        )
        .unwrap_err();
        assert!(matches!(error, StatsReloadError::ZeroInterval));
    }

    #[test]
    fn the_thread_publishes_only_when_a_version_moves_and_stops_promptly_on_shutdown() {
        let shared = Arc::new(SharedStats::new(StatsSnapshot::from([loaded_at(1, 1)])));
        let (sender, receiver) = mpsc::channel();
        let mut version = 1u64;
        let mut reloader = StatsReloader::spawn(
            Arc::clone(&shared),
            Duration::from_millis(5),
            Box::new(move || {
                version += 1;
                sender.send(version).unwrap();
                Ok(StatsSnapshot::from([loaded_at(1, version)]))
            }),
        )
        .unwrap();

        assert_eq!(receiver.recv().unwrap(), 2);
        assert_eq!(receiver.recv().unwrap(), 3);

        let stopping = Instant::now();
        reloader.shutdown().unwrap();
        assert!(stopping.elapsed() < Duration::from_secs(5));
        assert!(shared.load()[&1].version().unwrap() >= 2);
        assert!(reloader.stats().reloads >= 2);
        drop(receiver);
    }

    #[test]
    fn an_unchanged_read_publishes_nothing() {
        let shared = Arc::new(SharedStats::new(StatsSnapshot::from([loaded_at(7, 3)])));
        let published = shared.load();
        let stats = Arc::new(StatsReloadCounters::default());
        let mut read: Box<dyn FnMut() -> Result<StatsSnapshot, String>> =
            Box::new(|| Ok(StatsSnapshot::from([loaded_at(7, 3)])));
        run_one_stats_reload_pass(&shared, read.as_mut(), &stats);
        assert!(Arc::ptr_eq(&published, &shared.load()));
        assert_eq!(stats.passes.load(Ordering::Acquire), 1);
        assert_eq!(stats.reloads.load(Ordering::Acquire), 0);
    }

    #[test]
    fn a_failed_pass_keeps_the_previous_snapshot_published() {
        let shared = Arc::new(SharedStats::new(StatsSnapshot::from([loaded_at(7, 3)])));
        let stats = Arc::new(StatsReloadCounters::default());
        let mut read: Box<dyn FnMut() -> Result<StatsSnapshot, String>> =
            Box::new(|| Err("snapshot read failed".to_owned()));
        run_one_stats_reload_pass(&shared, read.as_mut(), &stats);
        assert_eq!(shared.load()[&7].version(), Some(3));
        assert_eq!(stats.failures.load(Ordering::Acquire), 1);
    }

    #[test]
    fn a_newly_analyzed_table_publishes_even_though_no_prior_version_moved() {
        // A table that goes from `Pseudo` to `Loaded` between passes has no
        // "previous version" to compare against; the presence/state change
        // itself must count as a difference.
        let shared = Arc::new(SharedStats::new(StatsSnapshot::from([(
            1,
            TableStatsState::Pseudo,
        )])));
        let stats = Arc::new(StatsReloadCounters::default());
        let mut read: Box<dyn FnMut() -> Result<StatsSnapshot, String>> =
            Box::new(|| Ok(StatsSnapshot::from([loaded_at(1, 1)])));
        run_one_stats_reload_pass(&shared, read.as_mut(), &stats);
        assert_eq!(shared.load()[&1].version(), Some(1));
        assert_eq!(stats.reloads.load(Ordering::Acquire), 1);
    }

    #[test]
    fn dropping_the_reloader_stops_its_thread() {
        let shared = Arc::new(SharedStats::new(StatsSnapshot::new()));
        let (sender, receiver) = mpsc::channel();
        let reloader = StatsReloader::spawn(
            Arc::clone(&shared),
            Duration::from_millis(5),
            Box::new(move || {
                let _ = sender.send(());
                Ok(StatsSnapshot::new())
            }),
        )
        .unwrap();
        receiver.recv().unwrap();
        drop(reloader);
        while receiver.recv_timeout(Duration::from_millis(200)).is_ok() {}
        assert!(receiver.recv_timeout(Duration::from_millis(200)).is_err());
    }
}
