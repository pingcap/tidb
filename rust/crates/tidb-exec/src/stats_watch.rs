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
//! of canonical [`tidb_stats::Table`] values,
//! plumbing only -- no estimation logic lives here.
//!
//! # Shape
//!
//! [`StatsCacheImpl`] is the table authority, matching Go's statistics handle.
//! The accompanying `Arc<StatsSnapshot>` is only a statement-facing index of
//! the exact `Arc<Table>` objects already published by that cache; refresh and
//! sync-load never mutate a second table representation. Replacing the index
//! whole lets an in-flight statement retain the table pointers it already
//! obtained while later statements observe the cache update.
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
//!   `tidb_stats::Table::version`.
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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::cluster_stats_load::ClusterStatsItem;
use tidb_stats::{Column, CopyIntent, Index, Table};
use tidb_stats_handle_cache::{CacheUpdate, StatsCacheImpl, StatsRefreshSource, UpdateError};

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
    /// Loaded statistics, current as of `Table::version`.
    Loaded(Arc<Table>),
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
    pub fn loaded(&self) -> Option<&Arc<Table>> {
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

/// Go's statistics cache plus a statement-facing index of its table objects.
pub struct SharedStats {
    cache: StatsCacheImpl,
    published: RwLock<Arc<StatsSnapshot>>,
}

impl std::fmt::Debug for SharedStats {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedStats")
            .field("cache_len", &self.cache.len())
            .field("receipt", &self.receipt())
            .finish_non_exhaustive()
    }
}

impl SharedStats {
    /// Publishes an initial snapshot, normally the node's startup load.
    pub fn new(snapshot: StatsSnapshot) -> Result<Self, String> {
        let cache = StatsCacheImpl::new()?;
        cache.update_stats_cache(CacheUpdate {
            updated: snapshot
                .values()
                .filter_map(TableStatsState::loaded)
                .cloned()
                .collect(),
            deleted: Vec::new(),
            skip_move_forward: false,
        });
        let result = Self {
            cache,
            published: RwLock::new(Arc::new(snapshot)),
        };
        result.publish_cache_objects();
        Ok(result)
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

    /// Go `StatsHandle.GetNextCheckVersionWithOffset` over the canonical
    /// cache owned by this shared statistics image.
    #[must_use]
    pub fn next_check_version_with_offset(&self, lease: Duration) -> u64 {
        self.cache.next_check_version_with_offset(lease)
    }

    /// Replaces the published snapshot atomically.
    pub fn store(&self, snapshot: StatsSnapshot) {
        self.store_with_version_policy(snapshot, false);
    }

    /// Go ANALYZE's targeted cache publication: replace table objects without
    /// advancing the cache lifecycle version in quota mode.
    pub fn store_after_analyze(&self, snapshot: StatsSnapshot) {
        self.store_with_version_policy(snapshot, true);
    }

    /// Runs pinned Go `StatsCacheImpl.Update` and republishes its canonical
    /// table objects into the statement-facing index.
    ///
    /// Loaded entries outside `tracked_ids` are retained because Go's cache
    /// update is incremental (and explicit temporary-table ANALYZE may own
    /// such an entry); obsolete pseudo attempts are removed. The DDL
    /// subscriber remains responsible for deleting dropped loaded tables.
    pub fn update_from_source<S>(
        &self,
        source: &S,
        physical_ids: Vec<i64>,
        tracked_ids: &[i64],
        is_cancelled: impl Fn() -> bool,
    ) -> Result<bool, UpdateError<S::Error>>
    where
        S: StatsRefreshSource,
    {
        self.cache
            .update_from_source(source, physical_ids, is_cancelled)?;

        let tracked = tracked_ids.iter().copied().collect::<BTreeSet<_>>();
        let mut guard = self
            .published
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut next = guard.as_ref().clone();
        next.retain(|table_id, state| tracked.contains(table_id) || state.loaded().is_some());
        for table_id in tracked_ids {
            let state = self
                .cache
                .get(*table_id)
                .map_or(TableStatsState::Pseudo, TableStatsState::Loaded);
            next.insert(*table_id, state);
        }
        let changed = snapshots_differ_by_object(guard.as_ref(), &next);
        if changed {
            *guard = Arc::new(next);
        }
        Ok(changed)
    }

    fn store_with_version_policy(&self, snapshot: StatsSnapshot, skip_move_forward: bool) {
        let mut guard = match self.published.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let deleted = guard
            .iter()
            .filter_map(|(table_id, state)| {
                matches!(state, TableStatsState::Loaded(_))
                    .then_some(*table_id)
                    .filter(|table_id| {
                        !matches!(snapshot.get(table_id), Some(TableStatsState::Loaded(_)))
                    })
            })
            .collect();
        let updated = snapshot
            .iter()
            .filter_map(|(table_id, state)| {
                let next = state.loaded()?;
                let unchanged = guard
                    .get(table_id)
                    .and_then(TableStatsState::loaded)
                    .is_some_and(|current| Arc::ptr_eq(current, next));
                (!unchanged).then(|| Arc::clone(next))
            })
            .collect();
        self.cache.update_stats_cache(CacheUpdate {
            updated,
            deleted,
            skip_move_forward,
        });
        *guard = Arc::new(self.snapshot_with_cache_objects(snapshot));
    }

    /// Go sync-load's `updateCachedItem`: copy the cached table, replace only
    /// the requested column/index, and atomically publish a new cache image.
    /// Applies Go sync-load's cache update with the current schema metadata.
    /// A fully loaded item is never downgraded, and a metadata-only request
    /// does not replace an item already present.
    pub fn update_item(
        &self,
        table_id: i64,
        item: ClusterStatsItem,
        table_info: &tidb_model::table_info::TableInfo,
    ) -> bool {
        let mut guard = match self.published.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(current) = self.cache.get(table_id) else {
            return false;
        };
        let full_loaded = item.load_status.is_full_load();
        let mut table = current.copy_as(if item.is_index {
            CopyIntent::IndexMapWritable
        } else {
            CopyIntent::ColumnMapWritable
        });
        if item.is_index {
            let replacement = if let Some(existing) = current.hist_coll.get_index(item.id) {
                let existing = existing
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // Go returns true for an already-satisfied index request even
                // though it does not publish another table.
                if existing.is_full_load() || !full_loaded {
                    return true;
                }
                Index {
                    cmsketch: item.cms.clone(),
                    top_n: item.topn.clone(),
                    fm_sketch: None,
                    info: existing.info.clone(),
                    histogram: item.histogram.clone(),
                    stats_loaded_status: item.load_status,
                    stats_version: item.stats_ver,
                    physical_id: existing.physical_id,
                }
            } else {
                let Some(replacement) = item.to_index(table_id, table_info) else {
                    return false;
                };
                replacement
            };
            table.hist_coll.set_index(item.id, replacement);
            if item.stats_ver > 0 {
                if let Some(existence) = &table.existence_map {
                    existence
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert_index(item.id, true);
                }
                table.hist_coll.stats_version = i32::try_from(item.stats_ver).unwrap_or(i32::MAX);
            }
        } else {
            let available =
                item.stats_ver != 0 || item.histogram.ndv > 0 || item.histogram.null_count > 0;
            let replacement = if let Some(existing) = current.hist_coll.get_column(item.id) {
                let existing = existing
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if existing.is_full_load() || !full_loaded {
                    return false;
                }
                Column {
                    cmsketch: item.cms.clone(),
                    top_n: item.topn.clone(),
                    fm_sketch: None,
                    info: existing.info.clone(),
                    histogram: item.histogram.clone(),
                    stats_loaded_status: item.load_status,
                    physical_id: existing.physical_id,
                    stats_version: item.stats_ver,
                    is_handle: existing.is_handle,
                }
            } else {
                let Some(replacement) = item.to_column(table_id, table_info) else {
                    return false;
                };
                replacement
            };
            table.hist_coll.set_column(item.id, replacement);
            if let Some(existence) = &table.existence_map {
                existence
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert_column(item.id, available);
            }
            if item.stats_ver > 0 {
                table.hist_coll.stats_version = i32::try_from(item.stats_ver).unwrap_or(i32::MAX);
            }
        }
        let table = Arc::new(table);
        self.cache.update_stats_cache(CacheUpdate {
            updated: vec![Arc::clone(&table)],
            deleted: Vec::new(),
            skip_move_forward: false,
        });
        let mut snapshot = guard.as_ref().clone();
        snapshot.insert(table_id, TableStatsState::Loaded(table));
        *guard = Arc::new(snapshot);
        true
    }

    fn snapshot_with_cache_objects(&self, mut snapshot: StatsSnapshot) -> StatsSnapshot {
        for (table_id, state) in &mut snapshot {
            if matches!(state, TableStatsState::Loaded(_)) {
                let table = self
                    .cache
                    .get(*table_id)
                    .expect("a published loaded table must remain in the statistics cache");
                *state = TableStatsState::Loaded(table);
            }
        }
        snapshot
    }

    fn publish_cache_objects(&self) {
        let mut guard = self
            .published
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let snapshot = guard.as_ref().clone();
        *guard = Arc::new(self.snapshot_with_cache_objects(snapshot));
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

/// The result of one statistics reload read/update pass.
pub enum StatsReloadReadResult {
    /// The source proved that no cache object moved.
    Unchanged,
    /// Startup produced a complete initial snapshot for publication.
    Publish(StatsSnapshot),
    /// Pinned `StatsCacheImpl.Update` already updated the shared canonical
    /// cache and statement-facing index incrementally.
    Updated,
}

/// One reload pass's source step.
///
/// Kept as an injectable closure, the same way
/// [`crate::catalog_watch::ReloadPass`] and `PrivilegeReloadRead` are, so the
/// thread's condvar/shutdown machinery can be tested without PD or TiKV.
pub type StatsReloadRead =
    Box<dyn FnMut() -> Result<StatsReloadReadResult, String> + Send + 'static>;

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
    /// A lifecycle guard with no worker, used when Go's stats lease is
    /// negative and `UpdateTableStatsLoop` deliberately skips
    /// `loadStatsWorker`.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            signal: Arc::new((Mutex::new(StatsReloadSignal::default()), Condvar::new())),
            stats: Arc::new(StatsReloadCounters::default()),
            worker: None,
        }
    }

    /// Starts the reload thread ticking every `interval`, publishing into
    /// `shared` whenever a pass's read reports a snapshot that differs from
    /// the one currently published.
    pub fn spawn(
        shared: Arc<SharedStats>,
        interval: Duration,
        read: StatsReloadRead,
    ) -> Result<Self, StatsReloadError> {
        Self::spawn_impl(shared, interval, read, false)
    }

    /// Starts the reload thread with one immediate pass before its first
    /// tick, matching Go's `loadStatsWorker` call to `initStats`.
    pub fn spawn_with_initial_pass(
        shared: Arc<SharedStats>,
        interval: Duration,
        read: StatsReloadRead,
    ) -> Result<Self, StatsReloadError> {
        Self::spawn_impl(shared, interval, read, true)
    }

    fn spawn_impl(
        shared: Arc<SharedStats>,
        interval: Duration,
        mut read: StatsReloadRead,
        initial_pass: bool,
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
                if initial_pass {
                    run_one_stats_reload_pass(&shared, read.as_mut(), &worker_stats);
                }
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

fn snapshots_differ_by_object(current: &StatsSnapshot, next: &StatsSnapshot) -> bool {
    if current.len() != next.len() {
        return true;
    }
    current.iter().any(|(table_id, state)| {
        let Some(next_state) = next.get(table_id) else {
            return true;
        };
        match (state, next_state) {
            (TableStatsState::Pseudo, TableStatsState::Pseudo) => false,
            (TableStatsState::Loaded(left), TableStatsState::Loaded(right)) => {
                !Arc::ptr_eq(left, right)
            }
            _ => true,
        }
    })
}

/// Runs one Go-shaped load-worker pass. Startup may publish its initial
/// snapshot; later passes have already applied `StatsCacheImpl.Update` and
/// report only whether a canonical table object moved.
fn run_one_stats_reload_pass(
    shared: &SharedStats,
    read: &mut dyn FnMut() -> Result<StatsReloadReadResult, String>,
    stats: &StatsReloadCounters,
) {
    stats.passes.fetch_add(1, Ordering::AcqRel);
    match read() {
        // The ordered stats_meta read proved nothing moved, so the published
        // index stays unchanged.
        Ok(StatsReloadReadResult::Unchanged) => {}
        Ok(StatsReloadReadResult::Publish(next)) => {
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
        Ok(StatsReloadReadResult::Updated) => {
            let receipt = shared.receipt();
            stats.reloads.fetch_add(1, Ordering::AcqRel);
            eprintln!(
                "{{\"event\":\"stats_reloaded\",\"loaded\":{},\"pseudo\":{}}}",
                receipt.loaded, receipt.pseudo
            );
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
    use tidb_stats::{ColAndIdxExistenceMap, HistColl, Table};
    use tidb_stats_handle_cache::StatsMetaRow;

    use super::*;

    fn loaded_at(table_id: i64, version: u64) -> (i64, TableStatsState) {
        (
            table_id,
            TableStatsState::Loaded(Arc::new(Table {
                existence_map: Some(Arc::new(RwLock::new(ColAndIdxExistenceMap::new(0, 0)))),
                hist_coll: HistColl::new(table_id, 0, 0, 0, 0),
                version,
                last_analyze_version: 0,
                last_stats_hist_version: 0,
                table_info_update_ts: 0,
                is_pk_handle: false,
            })),
        )
    }

    fn item(id: i64, status: tidb_stats::StatsLoadedStatus) -> ClusterStatsItem {
        ClusterStatsItem {
            id,
            is_index: false,
            stats_ver: 2,
            flag: 0,
            load_status: status,
            histogram: Histogram {
                id,
                ndv: 10,
                last_update_version: 42,
                ..Histogram::default()
            },
            topn: None,
            cms: None,
            fm_sketch: None,
        }
    }

    fn shared_stats(snapshot: StatsSnapshot) -> SharedStats {
        tidb_vardef::STATS_CACHE_MEM_QUOTA.store(1024 * 1024, std::sync::atomic::Ordering::SeqCst);
        SharedStats::new(snapshot).expect("statistics cache")
    }

    struct RefreshSource {
        rows: Vec<StatsMetaRow>,
        loaded: Arc<Table>,
        loads: Mutex<usize>,
    }

    impl StatsRefreshSource for RefreshSource {
        type Error = ();

        fn lease(&self) -> Duration {
            Duration::ZERO
        }

        fn stats_meta_rows(
            &self,
            _after_version: u64,
            _physical_ids: &[i64],
        ) -> Result<Vec<StatsMetaRow>, Self::Error> {
            Ok(self.rows.clone())
        }

        fn table_info_update_ts(&self, physical_id: i64) -> Option<u64> {
            (physical_id == 1).then_some(0)
        }

        fn table_stats_from_storage(
            &self,
            _physical_id: i64,
        ) -> Result<Option<Arc<Table>>, Self::Error> {
            *self.loads.lock().unwrap() += 1;
            Ok(Some(Arc::clone(&self.loaded)))
        }
    }

    fn table_info() -> tidb_model::table_info::TableInfo {
        tidb_model::table_info::TableInfo {
            id: 1,
            columns: tidb_model::GoSharedPointerSlice::from_handles(vec![Some(
                tidb_model::GoShared::new(tidb_model::column::ColumnInfo {
                    id: 1,
                    name: tidb_ast::CiString::new("a"),
                    field_type: tidb_datatype::FieldType::new(
                        tidb_datatype::FieldTypeCode::LongLong,
                    ),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::column::ColumnInfo::default()
                }),
            )]),
            indices: vec![tidb_model::index::IndexInfo {
                id: 2,
                name: tidb_ast::CiString::new("idx_a"),
                state: tidb_model::SchemaState::PUBLIC,
                columns: vec![tidb_model::index::IndexColumn {
                    name: tidb_ast::CiString::new("a"),
                    offset: 0,
                    ..tidb_model::index::IndexColumn::default()
                }]
                .into(),
                ..tidb_model::index::IndexInfo::default()
            }]
            .into(),
            ..tidb_model::table_info::TableInfo::default()
        }
    }

    #[test]
    fn a_published_snapshot_replaces_the_previous_one_whole() {
        let shared = shared_stats(StatsSnapshot::from([loaded_at(1, 10)]));
        let held = shared.load();
        shared.store(StatsSnapshot::from([loaded_at(1, 20)]));
        // The in-flight reader keeps its own version; the next reader sees
        // the new one -- the whole consistency contract of the swap.
        assert_eq!(held[&1].version(), Some(10));
        assert_eq!(shared.load()[&1].version(), Some(20));
    }

    #[test]
    fn analyze_publication_does_not_advance_the_cache_lifecycle_version() {
        let shared = shared_stats(StatsSnapshot::from([loaded_at(1, 10)]));
        assert_eq!(shared.next_check_version_with_offset(Duration::ZERO), 10);
        shared.store_after_analyze(StatsSnapshot::from([loaded_at(1, 20)]));
        assert_eq!(shared.load()[&1].version(), Some(20));
        assert_eq!(shared.next_check_version_with_offset(Duration::ZERO), 10);
    }

    #[test]
    fn published_snapshots_reference_the_cache_objects_and_drop_deleted_tables() {
        let shared = shared_stats(StatsSnapshot::from([loaded_at(1, 10)]));
        let published = shared.load()[&1].loaded().unwrap().clone();
        let cached = shared.cache.get(1).expect("cached table");
        assert!(Arc::ptr_eq(&published, &cached));

        shared.store(StatsSnapshot::from([(1, TableStatsState::Pseudo)]));
        assert!(matches!(shared.load()[&1], TableStatsState::Pseudo));
        assert!(shared.cache.get(1).is_none());
    }

    #[test]
    fn sync_load_replaces_only_the_evicted_item_without_mutating_held_snapshots() {
        let table = match loaded_at(1, 42).1 {
            TableStatsState::Loaded(table) => table.as_ref().clone(),
            TableStatsState::Pseudo => unreachable!(),
        };
        let evicted = item(1, tidb_stats::StatsLoadedStatus::all_evicted());
        table.hist_coll.set_column(
            1,
            Column {
                info: Some(tidb_stats::ColumnInfo {
                    id: 1,
                    name: "a".to_owned(),
                    primary_key: false,
                }),
                histogram: evicted.histogram,
                stats_loaded_status: evicted.load_status,
                stats_version: evicted.stats_ver,
                physical_id: 1,
                ..Column::default()
            },
        );
        let shared = shared_stats(StatsSnapshot::from([(
            1,
            TableStatsState::Loaded(Arc::new(table)),
        )]));
        let held = shared.load();

        let table_info = table_info();
        assert!(shared.update_item(
            1,
            item(1, tidb_stats::StatsLoadedStatus::full_load()),
            &table_info,
        ));
        assert!(held[&1]
            .loaded()
            .unwrap()
            .hist_coll
            .get_column(1)
            .unwrap()
            .read()
            .unwrap()
            .is_all_evicted());
        assert!(shared.load()[&1]
            .loaded()
            .unwrap()
            .hist_coll
            .get_column(1)
            .unwrap()
            .read()
            .unwrap()
            .is_full_load());
        assert!(!shared.update_item(
            1,
            item(1, tidb_stats::StatsLoadedStatus::all_evicted()),
            &table_info,
        ));
        assert!(shared.load()[&1]
            .loaded()
            .unwrap()
            .hist_coll
            .get_column(1)
            .unwrap()
            .read()
            .unwrap()
            .is_full_load());
    }

    #[test]
    fn sync_load_installs_go_empty_column_for_known_unanalyzed_metadata() {
        let table = match loaded_at(1, 42).1 {
            TableStatsState::Loaded(table) => table.as_ref().clone(),
            TableStatsState::Pseudo => unreachable!(),
        };
        table
            .existence_map
            .as_ref()
            .unwrap()
            .write()
            .unwrap()
            .insert_column(1, false);
        let shared = shared_stats(StatsSnapshot::from([(
            1,
            TableStatsState::Loaded(Arc::new(table)),
        )]));
        let table_info = table_info();
        let empty = ClusterStatsItem {
            id: 1,
            is_index: false,
            stats_ver: 0,
            flag: 0,
            load_status: tidb_stats::StatsLoadedStatus::default(),
            histogram: Histogram {
                id: 1,
                ..Histogram::default()
            },
            topn: None,
            cms: None,
            fm_sketch: None,
        };

        assert!(shared.update_item(1, empty, &table_info));
        let current = shared.load();
        let current = current[&1].loaded().unwrap();
        assert!(current.hist_coll.get_column(1).is_some());
        let (column, load_needed, analyzed) = current.column_load_needed(1, true);
        assert!(column.is_none());
        assert!(!load_needed);
        assert!(!analyzed);
    }

    #[test]
    fn sync_load_inserts_analyzed_column_without_a_resident_object() {
        let table = match loaded_at(1, 42).1 {
            TableStatsState::Loaded(table) => table.as_ref().clone(),
            TableStatsState::Pseudo => unreachable!(),
        };
        table
            .existence_map
            .as_ref()
            .unwrap()
            .write()
            .unwrap()
            .insert_column(1, true);
        let shared = shared_stats(StatsSnapshot::from([(
            1,
            TableStatsState::Loaded(Arc::new(table)),
        )]));
        let table_info = table_info();

        assert!(shared.update_item(
            1,
            item(1, tidb_stats::StatsLoadedStatus::full_load()),
            &table_info,
        ));
        let current = shared.load();
        let current = current[&1].loaded().unwrap();
        let (column, load_needed, analyzed) = current.column_load_needed(1, true);
        assert!(column.unwrap().read().unwrap().is_full_load());
        assert!(!load_needed);
        assert!(analyzed);
    }

    #[test]
    fn sync_load_inserts_analyzed_index_without_a_resident_object() {
        let table = match loaded_at(1, 42).1 {
            TableStatsState::Loaded(table) => table.as_ref().clone(),
            TableStatsState::Pseudo => unreachable!(),
        };
        table
            .existence_map
            .as_ref()
            .unwrap()
            .write()
            .unwrap()
            .insert_index(2, true);
        let shared = shared_stats(StatsSnapshot::from([(
            1,
            TableStatsState::Loaded(Arc::new(table)),
        )]));
        let table_info = table_info();
        let mut index = item(2, tidb_stats::StatsLoadedStatus::full_load());
        index.is_index = true;

        assert!(shared.update_item(1, index, &table_info));
        let current = shared.load();
        let current = current[&1].loaded().unwrap();
        let (index, load_needed) = current.index_load_needed(2);
        assert!(index.unwrap().read().unwrap().is_full_load());
        assert!(!load_needed);
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

    /// Pinned root handletest `TestVersion`: once the cache watermark/table
    /// is at version four, a manually regressed version-one row neither loads
    /// payload nor replaces the previously published table.
    #[test]
    fn an_older_stats_version_cannot_move_the_shared_cache_backward() {
        let shared = shared_stats(StatsSnapshot::from([loaded_at(1, 4)]));
        let published = shared.load();
        let source = RefreshSource {
            rows: vec![StatsMetaRow {
                version: 1,
                physical_id: 1,
                count: 2,
                ..StatsMetaRow::default()
            }],
            loaded: match loaded_at(1, 1).1 {
                TableStatsState::Loaded(table) => table,
                TableStatsState::Pseudo => unreachable!(),
            },
            loads: Mutex::new(0),
        };

        assert!(!shared
            .update_from_source(&source, Vec::new(), &[1], || false)
            .unwrap());
        assert!(Arc::ptr_eq(&published, &shared.load()));
        assert_eq!(shared.load()[&1].version(), Some(4));
        assert_eq!(*source.loads.lock().unwrap(), 0);
    }

    #[test]
    fn a_histogram_carrying_table_still_reports_its_version() {
        // Guards against the receipt/version accessors only ever having been
        // exercised on an empty canonical table.
        let hist_coll = HistColl::new(9, 100, 3, 1, 0);
        hist_coll.set_column(
            1,
            Column {
                info: Some(tidb_stats::ColumnInfo {
                    id: 1,
                    name: "a".to_owned(),
                    primary_key: false,
                }),
                histogram: Histogram {
                    id: 1,
                    ndv: 10,
                    last_update_version: 42,
                    ..Histogram::default()
                },
                stats_loaded_status: tidb_stats::StatsLoadedStatus::full_load(),
                stats_version: 2,
                physical_id: 9,
                ..Column::default()
            },
        );
        let stats = Table {
            existence_map: None,
            hist_coll,
            version: 42,
            last_analyze_version: 42,
            last_stats_hist_version: 42,
            table_info_update_ts: 0,
            is_pk_handle: false,
        };
        let state = TableStatsState::Loaded(Arc::new(stats));
        assert_eq!(state.version(), Some(42));
        assert!(state.loaded().is_some());
    }

    #[test]
    fn a_zero_interval_is_refused_rather_than_spinning() {
        let error = StatsReloader::spawn(
            Arc::new(shared_stats(StatsSnapshot::new())),
            Duration::ZERO,
            Box::new(|| Ok(StatsReloadReadResult::Publish(StatsSnapshot::new()))),
        )
        .unwrap_err();
        assert!(matches!(error, StatsReloadError::ZeroInterval));
    }

    #[test]
    fn a_negative_go_lease_has_no_reload_worker() {
        let mut reloader = StatsReloader::disabled();
        assert_eq!(reloader.stats(), StatsReloadStats::default());
        reloader.shutdown().unwrap();
        assert_eq!(reloader.stats(), StatsReloadStats::default());
    }

    #[test]
    fn go_load_worker_runs_initial_pass_before_first_tick() {
        let previous_quota = tidb_vardef::STATS_CACHE_MEM_QUOTA
            .swap(1024 * 1024, std::sync::atomic::Ordering::SeqCst);
        let shared = Arc::new(shared_stats(StatsSnapshot::new()));
        let (sender, receiver) = mpsc::channel();
        let mut reloader = StatsReloader::spawn_with_initial_pass(
            Arc::clone(&shared),
            Duration::from_secs(60),
            Box::new(move || {
                sender.send(()).unwrap();
                Ok(StatsReloadReadResult::Unchanged)
            }),
        )
        .unwrap();
        receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        reloader.shutdown().unwrap();
        assert_eq!(reloader.stats().passes, 1);
        tidb_vardef::STATS_CACHE_MEM_QUOTA
            .store(previous_quota, std::sync::atomic::Ordering::SeqCst);
    }

    #[test]
    fn the_thread_publishes_only_when_a_version_moves_and_stops_promptly_on_shutdown() {
        let shared = Arc::new(shared_stats(StatsSnapshot::from([loaded_at(1, 1)])));
        let (sender, receiver) = mpsc::channel();
        let mut version = 1u64;
        let mut reloader = StatsReloader::spawn(
            Arc::clone(&shared),
            Duration::from_millis(5),
            Box::new(move || {
                version += 1;
                sender.send(version).unwrap();
                Ok(StatsReloadReadResult::Publish(StatsSnapshot::from([
                    loaded_at(1, version),
                ])))
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
    fn a_proven_unchanged_pass_costs_no_reload_and_publishes_nothing() {
        let shared = Arc::new(shared_stats(StatsSnapshot::from([loaded_at(7, 3)])));
        let published = shared.load();
        let stats = Arc::new(StatsReloadCounters::default());
        // The version probe answered "nothing moved" without re-reading any
        // statistics -- the whole cost of this pass was the probe itself.
        let mut read: Box<dyn FnMut() -> Result<StatsReloadReadResult, String>> =
            Box::new(|| Ok(StatsReloadReadResult::Unchanged));
        run_one_stats_reload_pass(&shared, read.as_mut(), &stats);
        assert!(Arc::ptr_eq(&published, &shared.load()));
        assert_eq!(stats.passes.load(Ordering::Acquire), 1);
        assert_eq!(stats.reloads.load(Ordering::Acquire), 0);
    }

    #[test]
    fn an_unchanged_read_publishes_nothing() {
        let shared = Arc::new(shared_stats(StatsSnapshot::from([loaded_at(7, 3)])));
        let published = shared.load();
        let stats = Arc::new(StatsReloadCounters::default());
        let mut read: Box<dyn FnMut() -> Result<StatsReloadReadResult, String>> = Box::new(|| {
            Ok(StatsReloadReadResult::Publish(StatsSnapshot::from([
                loaded_at(7, 3),
            ])))
        });
        run_one_stats_reload_pass(&shared, read.as_mut(), &stats);
        assert!(Arc::ptr_eq(&published, &shared.load()));
        assert_eq!(stats.passes.load(Ordering::Acquire), 1);
        assert_eq!(stats.reloads.load(Ordering::Acquire), 0);
    }

    #[test]
    fn a_failed_pass_keeps_the_previous_snapshot_published() {
        let shared = Arc::new(shared_stats(StatsSnapshot::from([loaded_at(7, 3)])));
        let stats = Arc::new(StatsReloadCounters::default());
        let mut read: Box<dyn FnMut() -> Result<StatsReloadReadResult, String>> =
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
        let shared = Arc::new(shared_stats(StatsSnapshot::from([(
            1,
            TableStatsState::Pseudo,
        )])));
        let stats = Arc::new(StatsReloadCounters::default());
        let mut read: Box<dyn FnMut() -> Result<StatsReloadReadResult, String>> = Box::new(|| {
            Ok(StatsReloadReadResult::Publish(StatsSnapshot::from([
                loaded_at(1, 1),
            ])))
        });
        run_one_stats_reload_pass(&shared, read.as_mut(), &stats);
        assert_eq!(shared.load()[&1].version(), Some(1));
        assert_eq!(stats.reloads.load(Ordering::Acquire), 1);
    }

    #[test]
    fn dropping_the_reloader_stops_its_thread() {
        let shared = Arc::new(shared_stats(StatsSnapshot::new()));
        let (sender, receiver) = mpsc::channel();
        let reloader = StatsReloader::spawn(
            Arc::clone(&shared),
            Duration::from_millis(5),
            Box::new(move || {
                let _ = sender.send(());
                Ok(StatsReloadReadResult::Publish(StatsSnapshot::new()))
            }),
        )
        .unwrap();
        receiver.recv().unwrap();
        drop(reloader);
        while receiver.recv_timeout(Duration::from_millis(200)).is_ok() {}
        assert!(receiver.recv_timeout(Duration::from_millis(200)).is_err());
    }
}
