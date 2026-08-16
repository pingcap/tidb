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

//! Go `aggregator.go`: the background collector that drains every registered
//! [`StatementStats`], merges the results, and pushes them to the registered
//! collectors once a second.

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use super::rustats::{
    default_ru_version, normalize_ru_version, RuIncrementMap, RuVersion, RuVersionProvider,
};
use super::stmtstats::{StatementStats, StatementStatsMap};
use crate::topsql_state::{top_ru_enabled, top_sql_enabled};

/// Go `maxStmtStatsSize`.
pub const MAX_STMT_STATS_SIZE: u32 = 1_000_000;

/// Go `maxRUKeysPerAggregate`: the hard cap on distinct RU keys per
/// aggregation cycle. Excess keys are dropped early to protect hot paths.
pub const MAX_RU_KEYS_PER_AGGREGATE: usize = 10_000;

/// Go's `tick := time.NewTicker(time.Second)` in `aggregator.run`.
const TICK_INTERVAL: Duration = Duration::from_secs(1);

/// Go `Collector`: collects a [`StatementStatsMap`].
pub trait Collector: Send + Sync {
    /// Go `Collector.CollectStmtStatsMap`.
    ///
    /// Go hands every collector the same map value; the borrow here says the
    /// same thing without the aliasing.
    fn collect_stmt_stats_map(&self, data: &StatementStatsMap);
}

/// Go `RUCollector`: collects RU increments for the Top-RU pipeline.
///
/// It is separate from [`Collector`] to keep Top-SQL and Top-RU decoupled.
pub trait RuCollector: Send + Sync {
    /// Go `RUCollector.CollectRUIncrements`, called by the aggregator every 1s
    /// with merged RU deltas from all sessions, aggregated by
    /// `(user, sql_digest, plan_digest)`.
    fn collect_ru_increments(&self, data: &RuIncrementMap, version: RuVersion);

    /// Go `RUCollector.OnRUVersionChange`: clears version-sensitive RU state
    /// when the aggregator detects a version handover.
    fn on_ru_version_change(&self, version: RuVersion);
}

/// The RU keys and RU total that one [`Aggregator::drain_and_push_ru`] cycle
/// dropped because of [`MAX_RU_KEYS_PER_AGGREGATE`].
///
/// boundary: Go publishes these into
/// `topsql/reporter/metrics.IgnoreExceedRUKeysCounter` and
/// `IgnoreExceedRUTotalCounter`. Reporter telemetry is out of scope for this
/// crate, so the same two numbers are returned to the caller instead of being
/// counted into Prometheus; the drop policy itself is unchanged.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RuDropStats {
    /// Go's `droppedKeys`.
    pub keys: i64,
    /// Go's `droppedRU`.
    pub total_ru: f64,
}

/// Go `aggregator`: collects and aggregates data from all
/// [`StatementStats`], uploads it, and regularly cleans up closed sessions.
///
/// Go's `sync.Map` sets become plain `Mutex`-guarded vectors: registration is
/// rare, iteration snapshots the set before touching any element (Go's
/// `sync.Map.Range` likewise tolerates concurrent deletes), and identity is
/// pointer identity either way.
pub struct Aggregator {
    running: AtomicBool,
    ru_version_provider: Mutex<Option<Arc<dyn RuVersionProvider>>>,
    stats_set: Mutex<Vec<Arc<StatementStats>>>,
    collectors: Mutex<Vec<Arc<dyn Collector>>>,
    ru_collectors: Mutex<Vec<Arc<dyn RuCollector>>>,
    stats_len: AtomicU32,
    /// Go `aggregator.lastRUVersion`, a plain field written only by the run
    /// goroutine; an atomic here so `closed()`-style reads stay well-defined.
    last_ru_version: AtomicI32,
    /// Go's `ctx`/`cancel`/`wg` triple, which exists only to stop and join the
    /// run goroutine.
    stop: (Mutex<bool>, Condvar),
    worker: Mutex<Option<JoinHandle<()>>>,
}

/// Go's package-level `globalAggregator`.
pub(super) fn global_aggregator() -> &'static Arc<Aggregator> {
    static GLOBAL: OnceLock<Arc<Aggregator>> = OnceLock::new();
    GLOBAL.get_or_init(Aggregator::new)
}

impl Aggregator {
    /// Go `newAggregator`: an empty aggregator.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            running: AtomicBool::new(false),
            ru_version_provider: Mutex::new(None),
            stats_set: Mutex::new(Vec::new()),
            collectors: Mutex::new(Vec::new()),
            ru_collectors: Mutex::new(Vec::new()),
            stats_len: AtomicU32::new(0),
            last_ru_version: AtomicI32::new(RuVersion::UNSPECIFIED.0),
            stop: (Mutex::new(false), Condvar::new()),
            worker: Mutex::new(None),
        })
    }

    fn lock_stats(&self) -> std::sync::MutexGuard<'_, Vec<Arc<StatementStats>>> {
        self.stats_set.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `aggregator.setRUVersionProvider`.
    pub fn set_ru_version_provider(&self, provider: Option<Arc<dyn RuVersionProvider>>) {
        *self
            .ru_version_provider
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = provider;
    }

    /// The currently bound provider, Go's `m.ruVersionProvider` field read.
    #[must_use]
    pub fn ru_version_provider(&self) -> Option<Arc<dyn RuVersionProvider>> {
        self.ru_version_provider
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Go `aggregator.currentRUVersion`.
    #[must_use]
    pub fn current_ru_version(&self) -> RuVersion {
        match self.ru_version_provider() {
            Some(provider) => normalize_ru_version(provider.get_ru_version()),
            None => default_ru_version(),
        }
    }

    /// Go's `m.lastRUVersion` field read.
    #[must_use]
    pub fn last_ru_version(&self) -> RuVersion {
        RuVersion(self.last_ru_version.load(Ordering::SeqCst))
    }

    /// Go's `m.lastRUVersion = ...` field write.
    pub fn set_last_ru_version(&self, version: RuVersion) {
        self.last_ru_version.store(version.0, Ordering::SeqCst);
    }

    /// Go `aggregator.start`: begins the once-a-second run loop.
    ///
    /// Go's goroutine plus `context.WithCancel` plus `sync.WaitGroup` is one
    /// joined thread here, woken early by [`Aggregator::close`] through a
    /// condition variable instead of a cancelled context.
    pub fn start(self: &Arc<Self>) {
        let mut worker = self.worker.lock().unwrap_or_else(|e| e.into_inner());
        if self.running.load(Ordering::SeqCst) {
            return;
        }
        self.set_last_ru_version(self.current_ru_version());
        *self.stop.0.lock().unwrap_or_else(|e| e.into_inner()) = false;
        self.running.store(true, Ordering::SeqCst);
        let this = Arc::clone(self);
        *worker = Some(std::thread::spawn(move || this.run()));
    }

    /// Go `aggregator.run`: blocks the current thread and executes the main
    /// loop.
    fn run(&self) {
        let (stopped, wake) = &self.stop;
        loop {
            let guard = stopped.lock().unwrap_or_else(|e| e.into_inner());
            let (guard, _timeout) = wake
                .wait_timeout_while(guard, TICK_INTERVAL, |stopped| !*stopped)
                .unwrap_or_else(|e| e.into_inner());
            if *guard {
                return;
            }
            drop(guard);
            self.aggregate_all();
        }
    }

    /// Go `aggregator.aggregateAll`: a single tick of data collection.
    ///
    /// The ordering matters: RU must be drained before stmt stats to avoid
    /// losing RU deltas from sessions that become finished and get
    /// unregistered during the stmt stats phase.
    pub fn aggregate_all(&self) {
        let _drops = self.drain_and_push_ru();
        self.drain_and_push_stmt_stats();
    }

    /// Go `aggregator.drainAndPushStmtStats`: collects Top-SQL data from all
    /// associated [`StatementStats`]. Finished sessions are unregistered here.
    pub fn drain_and_push_stmt_stats(&self) {
        let mut total = StatementStatsMap::new();
        let snapshot: Vec<_> = self.lock_stats().clone();
        for stats in &snapshot {
            if stats.finished() {
                self.unregister(stats);
            }
            total.merge(&stats.take());
        }
        if !total.is_empty() && top_sql_enabled() {
            let collectors: Vec<_> = self
                .collectors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            for collector in &collectors {
                collector.collect_stmt_stats_map(&total);
            }
        }
    }

    /// Go `aggregator.drainAndPushRU`: drains RU increments from all sessions,
    /// applies the key cap, and pushes merged data to the RU collectors when
    /// Top-RU is enabled.
    ///
    /// Returns what Go instead counts into the reporter's ignore-counters; see
    /// [`RuDropStats`].
    pub fn drain_and_push_ru(&self) -> RuDropStats {
        let current_ru_version = self.current_ru_version();
        if current_ru_version != self.last_ru_version() {
            let snapshot: Vec<_> = self.lock_stats().clone();
            for stats in &snapshot {
                stats.reset_ru_state_on_version_change(current_ru_version);
            }
            let collectors: Vec<_> = self
                .ru_collectors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            for collector in &collectors {
                collector.on_ru_version_change(current_ru_version);
            }
            self.set_last_ru_version(current_ru_version);
            return RuDropStats::default();
        }

        let mut total = RuIncrementMap::with_capacity(MAX_RU_KEYS_PER_AGGREGATE);
        let mut drops = RuDropStats::default();
        let snapshot: Vec<_> = self.lock_stats().clone();
        for stats in &snapshot {
            let session_ru = stats.merge_ru_into();
            for (key, incr) in session_ru.iter() {
                if let Some(existing) = total.0.get_mut(key) {
                    existing.merge(incr);
                    continue;
                }
                if total.len() >= MAX_RU_KEYS_PER_AGGREGATE {
                    drops.keys += 1;
                    drops.total_ru += incr.total_ru;
                } else {
                    total.0.insert(key.clone(), *incr);
                }
            }
        }

        if top_ru_enabled() && !total.is_empty() {
            let collectors: Vec<_> = self
                .ru_collectors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            for collector in &collectors {
                collector.collect_ru_increments(&total, current_ru_version);
            }
        }
        drops
    }

    /// Go `aggregator.register`: binds a [`StatementStats`]. Thread-safe.
    pub fn register(&self, stats: &Arc<StatementStats>) {
        if self.stats_len.load(Ordering::SeqCst) > MAX_STMT_STATS_SIZE {
            return;
        }
        self.stats_len.fetch_add(1, Ordering::SeqCst);
        self.lock_stats().push(Arc::clone(stats));
    }

    /// Go `aggregator.unregister`: removes a [`StatementStats`]. Thread-safe.
    ///
    /// Go's `sync.Map.Delete` plus unconditional `statsLen.Dec()` is kept
    /// as-is, including the wrap that a surplus unregister would cause.
    pub fn unregister(&self, stats: &Arc<StatementStats>) {
        self.lock_stats().retain(|s| !Arc::ptr_eq(s, stats));
        self.stats_len.fetch_sub(1, Ordering::SeqCst);
    }

    /// Whether the given session is currently registered, Go's
    /// `m.statsSet.Load(stats)`.
    #[must_use]
    pub fn contains_stats(&self, stats: &Arc<StatementStats>) -> bool {
        self.lock_stats().iter().any(|s| Arc::ptr_eq(s, stats))
    }

    /// Go `aggregator.registerCollector`. Thread-safe.
    pub fn register_collector(&self, collector: Arc<dyn Collector>) {
        self.collectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(collector);
    }

    /// Go `aggregator.unregisterCollector`. Thread-safe.
    pub fn unregister_collector(&self, collector: &Arc<dyn Collector>) {
        self.collectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|c| !Arc::ptr_eq(c, collector));
    }

    /// Whether the given collector is registered, Go's
    /// `m.collectors.Load(collector)`.
    #[must_use]
    pub fn contains_collector(&self, collector: &Arc<dyn Collector>) -> bool {
        self.collectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|c| Arc::ptr_eq(c, collector))
    }

    /// Go `aggregator.registerRUCollector`. Thread-safe.
    pub fn register_ru_collector(&self, collector: Arc<dyn RuCollector>) {
        self.ru_collectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(collector);
    }

    /// Go `aggregator.unregisterRUCollector`. Thread-safe.
    pub fn unregister_ru_collector(&self, collector: &Arc<dyn RuCollector>) {
        self.ru_collectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|c| !Arc::ptr_eq(c, collector));
    }

    /// Whether the given RU collector is registered, Go's
    /// `m.ruCollectors.Load(collector)`.
    #[must_use]
    pub fn contains_ru_collector(&self, collector: &Arc<dyn RuCollector>) -> bool {
        self.ru_collectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|c| Arc::ptr_eq(c, collector))
    }

    /// Go `aggregator.close`: ends the execution of the current aggregator.
    pub fn close(&self) {
        let mut worker = self.worker.lock().unwrap_or_else(|e| e.into_inner());
        if !self.running.load(Ordering::SeqCst) {
            return;
        }
        self.running.store(false, Ordering::SeqCst);
        {
            let mut stopped = self.stop.0.lock().unwrap_or_else(|e| e.into_inner());
            *stopped = true;
        }
        self.stop.1.notify_all();
        if let Some(handle) = worker.take() {
            let _ = handle.join();
        }
    }

    /// Go `aggregator.closed`.
    #[must_use]
    pub fn closed(&self) -> bool {
        !self.running.load(Ordering::SeqCst)
    }
}

/// Go `SetupAggregator`: initializes the background aggregator of the
/// `stmtstats` module. **Not** thread-safe.
pub fn setup_aggregator() {
    global_aggregator().start();
}

/// Go `BindRUVersionProvider`: updates the global Top-RU RU-version provider.
pub fn bind_ru_version_provider(provider: Option<Arc<dyn RuVersionProvider>>) {
    global_aggregator().set_ru_version_provider(provider);
}

/// Go `CloseAggregator`: stops the background aggregator of the `stmtstats`
/// module. **Not** thread-safe.
pub fn close_aggregator() {
    global_aggregator().close();
}

/// Go `RegisterCollector`. Thread-safe.
pub fn register_collector(collector: Arc<dyn Collector>) {
    global_aggregator().register_collector(collector);
}

/// Go `UnregisterCollector`. Thread-safe.
pub fn unregister_collector(collector: &Arc<dyn Collector>) {
    global_aggregator().unregister_collector(collector);
}

/// Go `RegisterRUCollector`. Thread-safe.
pub fn register_ru_collector(collector: Arc<dyn RuCollector>) {
    global_aggregator().register_ru_collector(collector);
}

/// Go `UnregisterRUCollector`. Thread-safe.
pub fn unregister_ru_collector(collector: &Arc<dyn RuCollector>) {
    global_aggregator().unregister_ru_collector(collector);
}

#[cfg(test)]
mod tests {
    use super::super::rustats::{ExecutionContext, RuIncrement};
    use super::super::stmtstats::{StatementObserver, StatementStatsItem};
    use super::super::test_support::{
        assert_in_delta, exec_begin, exec_finish, global_test_guard, reset_topsql_state, ru_key,
        sql_plan_digest, MockCollector, MockRuCollector, MockRuVersionProvider,
    };
    use super::*;
    use crate::topsql_state::{disable_top_sql, enable_top_ru, enable_top_sql};

    const SECOND_NS: u64 = 1_000_000_000;
    const MILLISECOND_NS: u64 = 1_000_000;

    // Go `TestSetupCloseAggregator`: the global aggregator lifecycle helpers.
    #[test]
    fn setup_close_aggregator() {
        let _guard = global_test_guard();
        for _ in 0..3 {
            setup_aggregator();
            std::thread::sleep(Duration::from_millis(100));
            assert!(!global_aggregator().closed());
            close_aggregator();
            std::thread::sleep(Duration::from_millis(100));
            assert!(global_aggregator().closed());
        }
    }

    // Go `TestBindRUVersionProviderAfterCloseAggregator`.
    #[test]
    fn bind_ru_version_provider_after_close_aggregator() {
        let _guard = global_test_guard();
        let original_provider = global_aggregator().ru_version_provider();
        let provider = Arc::new(MockRuVersionProvider::new(RuVersion::V2));
        let provider_dyn: Arc<dyn RuVersionProvider> = provider.clone();

        bind_ru_version_provider(Some(provider_dyn.clone()));
        setup_aggregator();
        // Go's require.Eventually over `!closed()`.
        for _ in 0..100 {
            if !global_aggregator().closed() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!global_aggregator().closed());
        assert_eq!(RuVersion::V2, global_aggregator().last_ru_version());

        close_aggregator();
        assert!(global_aggregator().closed());
        let bound = global_aggregator().ru_version_provider().unwrap();
        assert!(Arc::ptr_eq(&provider_dyn, &bound));

        bind_ru_version_provider(None);
        assert!(global_aggregator().ru_version_provider().is_none());

        // Go's t.Cleanup.
        close_aggregator();
        bind_ru_version_provider(original_provider);
    }

    // Go `TestRegisterUnregisterCollector`.
    #[test]
    fn register_unregister_collector() {
        let _guard = global_test_guard();
        setup_aggregator();
        std::thread::sleep(Duration::from_millis(100));
        let collector: Arc<dyn Collector> = Arc::new(MockCollector::new(|_| {}));
        register_collector(collector.clone());
        assert!(global_aggregator().contains_collector(&collector));
        unregister_collector(&collector);
        assert!(!global_aggregator().contains_collector(&collector));
        close_aggregator();
    }

    // Go `TestRegisterUnregisterRUCollector`.
    #[test]
    fn register_unregister_ru_collector() {
        let _guard = global_test_guard();
        setup_aggregator();
        std::thread::sleep(Duration::from_millis(100));
        let collector: Arc<dyn RuCollector> = Arc::new(MockRuCollector::new(|_| {}));
        register_ru_collector(collector.clone());
        assert!(global_aggregator().contains_ru_collector(&collector));
        unregister_ru_collector(&collector);
        assert!(!global_aggregator().contains_ru_collector(&collector));
        close_aggregator();
    }

    // Go `TestAggregatorRegisterCollect`.
    #[test]
    fn aggregator_register_collect() {
        let _guard = global_test_guard();
        reset_topsql_state();
        enable_top_sql();
        let aggregator = Aggregator::new();
        let stats = StatementStats::detached();
        aggregator.register(&stats);
        stats.on_execution_begin(b"SQL-1", b"", Some(&exec_begin(0)));
        stats.on_execution_finished(b"SQL-1", b"", Some(&exec_finish(MILLISECOND_NS as i64)));
        let total = Arc::new(Mutex::new(StatementStatsMap::new()));
        let sink = Arc::clone(&total);
        aggregator.register_collector(Arc::new(MockCollector::new(move |data| {
            sink.lock().unwrap().merge(data);
        })));
        aggregator.drain_and_push_stmt_stats();
        let total = total.lock().unwrap();
        assert!(!total.is_empty());
        assert_eq!(total[&sql_plan_digest("SQL-1", "")].exec_count, 1);
        assert_eq!(
            total[&sql_plan_digest("SQL-1", "")].sum_duration_ns,
            MILLISECOND_NS
        );
        disable_top_sql();
    }

    // Go `TestAggregatorRunClose`: start/close idempotence on a standalone
    // aggregator.
    #[test]
    fn aggregator_run_close() {
        let _guard = global_test_guard();
        let aggregator = Aggregator::new();
        assert!(aggregator.closed());
        aggregator.start();
        std::thread::sleep(Duration::from_millis(100));
        assert!(!aggregator.closed());
        aggregator.close();
        assert!(aggregator.closed());

        // Randomly start and close.
        let mut rng = standard_fastrand::Rng::new();
        for _ in 0..100 {
            if rng.usize(0..2) == 0 {
                aggregator.start();
            } else {
                aggregator.close();
            }
        }
        aggregator.close();
    }

    // Go `TestAggregatorDisableAggregate`.
    #[test]
    fn aggregator_disable_aggregate() {
        let _guard = global_test_guard();
        reset_topsql_state();
        let total = Arc::new(Mutex::new(StatementStatsMap::new()));
        let aggregator = Aggregator::new();
        let sink = Arc::clone(&total);
        aggregator.register_collector(Arc::new(MockCollector::new(move |data| {
            sink.lock().unwrap().merge(data);
        })));

        disable_top_sql();
        let stats = StatementStats::detached();
        stats
            .lock()
            .data
            .insert(sql_plan_digest("", ""), StatementStatsItem::default());
        aggregator.register(&stats);
        aggregator.drain_and_push_stmt_stats();
        // The drain takes all data even when Top-SQL is not enabled ...
        assert!(stats.lock().data.is_empty());
        // ... but just drops it.
        assert!(total.lock().unwrap().is_empty());

        enable_top_sql();
        let stats = StatementStats::detached();
        stats
            .lock()
            .data
            .insert(sql_plan_digest("", ""), StatementStatsItem::default());
        aggregator.register(&stats);
        aggregator.drain_and_push_stmt_stats();
        assert!(stats.lock().data.is_empty());
        assert_eq!(total.lock().unwrap().len(), 1);
        disable_top_sql();
    }

    // Go `TestAggregatorDisableAggregateRUNoEmit`.
    #[test]
    fn aggregator_disable_aggregate_ru_no_emit() {
        let _guard = global_test_guard();
        reset_topsql_state();

        let aggregator = Aggregator::new();
        aggregator.set_last_ru_version(aggregator.current_ru_version());
        let stats = StatementStats::detached();
        let key = ru_key("u1", "s1", "");
        stats.lock().finished_ru_buffer.insert(
            key,
            RuIncrement {
                total_ru: 1.0,
                ..RuIncrement::default()
            },
        );
        aggregator.register(&stats);

        let collected = Arc::new(Mutex::new(RuIncrementMap::new()));
        let call_count = Arc::new(Mutex::new(0));
        let (sink, counter) = (Arc::clone(&collected), Arc::clone(&call_count));
        aggregator.register_ru_collector(Arc::new(MockRuCollector::new(move |m| {
            *counter.lock().unwrap() += 1;
            sink.lock().unwrap().merge(m);
        })));

        aggregator.drain_and_push_ru();

        // Housekeeping drain is allowed.
        assert!(stats.lock().finished_ru_buffer.is_empty());
        // Disabled => no RU output.
        assert_eq!(*call_count.lock().unwrap(), 0);
        assert!(collected.lock().unwrap().is_empty());
    }

    // Go `TestAggregatorRunOrderKeepsFinishedRU`.
    #[test]
    fn aggregator_run_order_keeps_finished_ru() {
        let _guard = global_test_guard();
        reset_topsql_state();
        enable_top_ru();

        let aggregator = Aggregator::new();
        aggregator.set_last_ru_version(aggregator.current_ru_version());
        let stats = StatementStats::detached_finished(true);
        let key = ru_key("u1", "s1", "");
        stats.lock().finished_ru_buffer.insert(
            key.clone(),
            RuIncrement {
                total_ru: 1.0,
                ..RuIncrement::default()
            },
        );
        aggregator.register(&stats);

        let collected = Arc::new(Mutex::new(RuIncrementMap::new()));
        let sink = Arc::clone(&collected);
        aggregator.register_ru_collector(Arc::new(MockRuCollector::new(move |m| {
            sink.lock().unwrap().merge(m);
        })));
        aggregator.aggregate_all();

        assert_eq!(collected.lock().unwrap().len(), 1);
        assert_in_delta(1.0, collected.lock().unwrap()[&key].total_ru);
        assert!(!aggregator.contains_stats(&stats));

        reset_topsql_state();
    }

    // Go `TestAggregatorDetectsRUVersionHandover`.
    #[test]
    fn aggregator_detects_ru_version_handover() {
        let _guard = global_test_guard();
        reset_topsql_state();
        enable_top_ru();

        let key = ru_key("u1", "sql1", "plan1");
        let stats = StatementStats::detached();
        {
            let mut inner = stats.lock();
            inner.finished_ru_buffer.insert(
                key.clone(),
                RuIncrement {
                    total_ru: 10.0,
                    ..RuIncrement::default()
                },
            );
            inner.exec_ctx = Some(ExecutionContext {
                key: key.clone(),
                ru_version: RuVersion::V1,
                ..ExecutionContext::default()
            });
        }

        let provider = Arc::new(MockRuVersionProvider::new(RuVersion::V1));
        let aggregator = Aggregator::new();
        aggregator.set_ru_version_provider(Some(provider.clone()));
        aggregator.set_last_ru_version(aggregator.current_ru_version());
        aggregator.register(&stats);

        let collected = Arc::new(Mutex::new(RuIncrementMap::new()));
        let collected_version = Arc::new(Mutex::new(RuVersion::UNSPECIFIED));
        let changes = Arc::new(Mutex::new(Vec::new()));
        {
            let (sink, version_sink, change_sink) = (
                Arc::clone(&collected),
                Arc::clone(&collected_version),
                Arc::clone(&changes),
            );
            aggregator.register_ru_collector(Arc::new(
                MockRuCollector::with_version(move |m, version| {
                    sink.lock().unwrap().merge(m);
                    *version_sink.lock().unwrap() = version;
                })
                .on_change(move |version| change_sink.lock().unwrap().push(version)),
            ));
        }

        aggregator.drain_and_push_ru();
        assert_eq!(collected.lock().unwrap().len(), 1);
        assert_eq!(RuVersion::V1, *collected_version.lock().unwrap());

        {
            let mut inner = stats.lock();
            inner.finished_ru_buffer.insert(
                key.clone(),
                RuIncrement {
                    total_ru: 5.0,
                    ..RuIncrement::default()
                },
            );
            inner.exec_ctx = Some(ExecutionContext {
                key: key.clone(),
                ru_version: RuVersion::V1,
                ..ExecutionContext::default()
            });
        }
        provider.set(RuVersion::V2);
        *collected.lock().unwrap() = RuIncrementMap::new();
        *collected_version.lock().unwrap() = RuVersion::UNSPECIFIED;

        aggregator.drain_and_push_ru();
        assert!(collected.lock().unwrap().is_empty());
        assert_eq!(vec![RuVersion::V2], *changes.lock().unwrap());
        assert!(stats.lock().exec_ctx.is_none());
        assert!(stats.lock().finished_ru_buffer.is_empty());

        stats.lock().finished_ru_buffer.insert(
            key,
            RuIncrement {
                total_ru: 7.0,
                ..RuIncrement::default()
            },
        );
        aggregator.drain_and_push_ru();
        assert_eq!(collected.lock().unwrap().len(), 1);
        assert_eq!(RuVersion::V2, *collected_version.lock().unwrap());

        reset_topsql_state();
    }

    // Go `TestAggregatorTopSQLTopRUCoexistenceMatrix`.
    #[test]
    fn aggregator_top_sql_top_ru_coexistence_matrix() {
        let _guard = global_test_guard();
        struct Case {
            name: &'static str,
            enable_top_sql: bool,
            enable_top_ru: bool,
            expect_stmt_data: bool,
            expect_ru_data: bool,
        }
        let cases = [
            Case {
                name: "both-disabled",
                enable_top_sql: false,
                enable_top_ru: false,
                expect_stmt_data: false,
                expect_ru_data: false,
            },
            Case {
                name: "topsql-only",
                enable_top_sql: true,
                enable_top_ru: false,
                expect_stmt_data: true,
                expect_ru_data: false,
            },
            Case {
                name: "topru-only",
                enable_top_sql: false,
                enable_top_ru: true,
                expect_stmt_data: false,
                expect_ru_data: true,
            },
            Case {
                name: "both-enabled",
                enable_top_sql: true,
                enable_top_ru: true,
                expect_stmt_data: true,
                expect_ru_data: true,
            },
        ];

        for case in cases {
            reset_topsql_state();
            if case.enable_top_sql {
                enable_top_sql();
            }
            if case.enable_top_ru {
                enable_top_ru();
            }

            let aggregator = Aggregator::new();
            aggregator.set_last_ru_version(aggregator.current_ru_version());
            let stats = StatementStats::detached();
            let ru_key = ru_key("u1", "sql1", "plan1");
            {
                let mut inner = stats.lock();
                inner.data.insert(
                    sql_plan_digest("sql1", "plan1"),
                    StatementStatsItem {
                        exec_count: 1,
                        sum_duration_ns: SECOND_NS,
                        ..StatementStatsItem::default()
                    },
                );
                inner.finished_ru_buffer.insert(
                    ru_key.clone(),
                    RuIncrement {
                        total_ru: 42.0,
                        exec_count: 1,
                        exec_duration: SECOND_NS,
                    },
                );
            }
            aggregator.register(&stats);

            let stmt_collected = Arc::new(Mutex::new(StatementStatsMap::new()));
            let sink = Arc::clone(&stmt_collected);
            aggregator.register_collector(Arc::new(MockCollector::new(move |data| {
                sink.lock().unwrap().merge(data);
            })));
            let ru_collected = Arc::new(Mutex::new(RuIncrementMap::new()));
            let ru_sink = Arc::clone(&ru_collected);
            aggregator.register_ru_collector(Arc::new(MockRuCollector::new(move |m| {
                ru_sink.lock().unwrap().merge(m);
            })));

            aggregator.aggregate_all();

            let stmt_collected = stmt_collected.lock().unwrap();
            if case.expect_stmt_data {
                assert_eq!(stmt_collected.len(), 1, "{}", case.name);
                let item = &stmt_collected[&sql_plan_digest("sql1", "plan1")];
                assert_eq!(item.exec_count, 1, "{}", case.name);
            } else {
                assert!(stmt_collected.is_empty(), "{}", case.name);
            }

            let ru_collected = ru_collected.lock().unwrap();
            if case.expect_ru_data {
                assert_eq!(ru_collected.len(), 1, "{}", case.name);
                assert_in_delta(42.0, ru_collected[&ru_key].total_ru);
                assert_eq!(ru_collected[&ru_key].exec_count, 1, "{}", case.name);
            } else {
                assert!(ru_collected.is_empty(), "{}", case.name);
            }
        }
        reset_topsql_state();
    }

    // Go `TestAggregatorDrainTailIncrementMatrix`: concurrent tick/finish
    // ordering must not double count begin-based exec deltas.
    #[test]
    fn aggregator_drain_tail_increment_matrix() {
        let _guard = global_test_guard();
        struct Case {
            name: &'static str,
            tail_ru: f64,
            concurrent_unreg: bool,
            concurrent_unreg_ruc: bool,
        }
        let cases = [
            Case {
                name: "set-finished-before-tick",
                tail_ru: 5.0,
                concurrent_unreg: false,
                concurrent_unreg_ruc: false,
            },
            Case {
                name: "tick-with-unregister-race",
                tail_ru: 7.0,
                concurrent_unreg: true,
                concurrent_unreg_ruc: true,
            },
        ];

        for case in cases {
            reset_topsql_state();
            enable_top_ru();

            let aggregator = Aggregator::new();
            aggregator.set_last_ru_version(aggregator.current_ru_version());
            let key = ru_key("u1", "sql1", "plan1");
            // Session close.
            let stats = StatementStats::detached_finished(true);
            stats.lock().finished_ru_buffer.insert(
                key.clone(),
                RuIncrement {
                    total_ru: case.tail_ru,
                    exec_count: 1,
                    exec_duration: SECOND_NS,
                },
            );
            aggregator.register(&stats);

            let collected = Arc::new(Mutex::new(RuIncrementMap::new()));
            let (enter_tx, enter_rx) = crossbeam_channel::bounded::<()>(1);
            let (release_tx, release_rx) = crossbeam_channel::bounded::<()>(0);
            let sink = Arc::clone(&collected);
            let collector: Arc<dyn RuCollector> = Arc::new(MockRuCollector::new(move |m| {
                sink.lock().unwrap().merge(m);
                enter_tx.send(()).unwrap();
                let _ = release_rx.recv();
            }));
            aggregator.register_ru_collector(collector.clone());

            let handle = {
                let aggregator = Arc::clone(&aggregator);
                // Tick path under test: drain RU first, then unregister
                // finished stats.
                std::thread::spawn(move || aggregator.aggregate_all())
            };

            // Wait until the collector is entered before injecting unregister
            // races.
            enter_rx.recv().unwrap();
            if case.concurrent_unreg {
                aggregator.unregister(&stats);
            }
            if case.concurrent_unreg_ruc {
                aggregator.unregister_ru_collector(&collector);
            }
            drop(release_tx);
            handle.join().unwrap();

            let collected = collected.lock().unwrap();
            assert_eq!(collected.len(), 1, "{}", case.name);
            assert_in_delta(case.tail_ru, collected[&key].total_ru);
            assert_eq!(collected[&key].exec_count, 1, "{}", case.name);
            assert!(stats.lock().finished_ru_buffer.is_empty(), "{}", case.name);
            assert!(!aggregator.contains_stats(&stats), "{}", case.name);
        }
        reset_topsql_state();
    }

    // Go `TestDrainPushRUCapsAtMax`.
    #[test]
    fn drain_push_ru_caps_at_max() {
        let _guard = global_test_guard();
        reset_topsql_state();
        enable_top_ru();

        let aggregator = Aggregator::new();
        aggregator.set_last_ru_version(aggregator.current_ru_version());
        let hot_key = ru_key("hot-user", "hot-sql", "hot-plan");

        // >10000 distinct keys.
        const TOTAL_DISTINCT_KEYS: usize = MAX_RU_KEYS_PER_AGGREGATE + 51;
        const HOT_RU_PER_SESSION: f64 = 1000.0;
        const LOW_RU_PER_KEY: f64 = 1.0;

        // TOTAL_DISTINCT_KEYS = (N low unique keys) + 1 hot key.
        let low_unique_keys = TOTAL_DISTINCT_KEYS - 1;
        for i in 0..low_unique_keys {
            let stats = StatementStats::detached();
            let unique_key = ru_key(&format!("u{i:05}"), &format!("sql{i:05}"), "plan");
            let mut inner = stats.lock();
            inner.finished_ru_buffer.insert(
                unique_key,
                RuIncrement {
                    total_ru: LOW_RU_PER_KEY,
                    exec_count: 1,
                    exec_duration: 1,
                },
            );
            inner.finished_ru_buffer.insert(
                hot_key.clone(),
                RuIncrement {
                    total_ru: HOT_RU_PER_SESSION,
                    exec_count: 1,
                    exec_duration: 1,
                },
            );
            drop(inner);
            aggregator.register(&stats);
        }

        let collected = Arc::new(Mutex::new(RuIncrementMap::new()));
        let sink = Arc::clone(&collected);
        aggregator.register_ru_collector(Arc::new(MockRuCollector::new(move |m| {
            sink.lock().unwrap().merge(m);
        })));

        let drops = aggregator.drain_and_push_ru();

        let collected = collected.lock().unwrap();
        assert_eq!(
            collected.len(),
            MAX_RU_KEYS_PER_AGGREGATE,
            "push size should be capped at MAX_RU_KEYS_PER_AGGREGATE"
        );
        let expected_dropped_keys = (TOTAL_DISTINCT_KEYS - MAX_RU_KEYS_PER_AGGREGATE) as i64;
        assert_eq!(expected_dropped_keys, drops.keys);
        assert_in_delta(
            expected_dropped_keys as f64 * LOW_RU_PER_KEY,
            drops.total_ru,
        );

        let hot = collected
            .get(&hot_key)
            .expect("hot key should be retained after cap/drop");
        assert_in_delta(HOT_RU_PER_SESSION * low_unique_keys as f64, hot.total_ru);

        reset_topsql_state();
    }
}
