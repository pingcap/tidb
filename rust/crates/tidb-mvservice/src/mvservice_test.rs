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

//! Go master `pkg/mvservice/mvservice_test.go`, with the mock helper of
//! `test_helpers_test.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};
use tidb_gocontext::Context;
use tidb_timer::go_time::{GoTime, HOUR, MINUTE, SECOND};

use crate::server_maintainer::{ServerDiscovery, ServerInfo};
use crate::service::*;
use crate::service_settings::*;
use crate::task_handler::*;
use crate::test_helpers_test::*;
use crate::time_proxy::{mvs_now, unix_milli};

const MS: i64 = SECOND / 1000;

/// Go `mockSessionPool`.
pub(crate) struct MockSessionPool;

impl SessionPool for MockSessionPool {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// The mutable knobs of Go's `mockMVServiceHelper`.
#[derive(Default)]
pub(crate) struct MockKnobs {
    pub(crate) refresh_next: GoTime,
    pub(crate) purge_next: GoTime,
    pub(crate) refresh_err: Option<MvsError>,
    pub(crate) purge_err: Option<MvsError>,
    pub(crate) fetch_logs: HashMap<i64, Arc<MlogPurgeTask>>,
    pub(crate) fetch_views: HashMap<i64, Arc<MViewTask>>,
    pub(crate) fetch_accumulation_tasks: HashMap<i64, MlogAccumulationTask>,
    pub(crate) fetch_accumulation_row_counts: HashMap<i64, u64>,
    pub(crate) fetch_logs_err: Option<MvsError>,
    pub(crate) fetch_views_err: Option<MvsError>,
    pub(crate) fetch_accumulation_tasks_err: Option<MvsError>,
    pub(crate) fetch_accumulation_row_counts_err: Option<MvsError>,
    pub(crate) sync_refresh_alert_err: Option<MvsError>,
    pub(crate) cleanup_stale_refresh_alert_err: Option<MvsError>,
    pub(crate) last_refresh_alert_sync: Vec<RefreshAlertTask>,
    /// Go `lastRefreshID` / `lastPurgeID`.
    pub(crate) last_refresh_id: i64,
    pub(crate) last_purge_id: i64,
    /// Go `refreshPanic` / `purgePanic`.
    pub(crate) refresh_panic: bool,
    pub(crate) purge_panic: bool,
    /// Go `lastRefreshAlertAt`.
    pub(crate) last_refresh_alert_at: GoTime,
    /// Go `accumulationRowCountPanic`.
    pub(crate) accumulation_row_count_panic: bool,
    /// Go `accumulationRowCountEntered`.
    pub(crate) accumulation_row_count_entered: Option<Sender<()>>,
    /// Go `blockAccumulationRowCount`: a scan waits until its sender drops.
    pub(crate) block_accumulation_row_count: Option<Receiver<()>>,
    pub(crate) last_accumulation_row_count_task_ids: Vec<i64>,
    pub(crate) task_duration_counts: HashMap<String, i64>,
    pub(crate) run_event_counts: HashMap<String, i64>,
    pub(crate) refresh_manual_cancel_backoff: (bool, Option<GoTime>),
    pub(crate) purge_manual_cancel_backoff: (bool, Option<GoTime>),
    /// Go `fullChainMVServiceHelper`'s pending metadata, when set.
    pub(crate) full_chain: Option<(HashMap<i64, GoTime>, HashMap<i64, GoTime>)>,
}

/// Go `mockMVServiceHelper` (and, with `full_chain`, Go
/// `fullChainMVServiceHelper`).
#[derive(Default)]
pub(crate) struct MockMVServiceHelper {
    pub(crate) knobs: Mutex<MockKnobs>,
    pub(crate) fetch_logs_calls: AtomicI32,
    pub(crate) fetch_view_calls: AtomicI32,
    pub(crate) fetch_accumulation_task_calls: AtomicI32,
    pub(crate) fetch_accumulation_row_count_calls: AtomicI32,
    pub(crate) sync_refresh_alert_calls: AtomicI32,
    pub(crate) cleanup_stale_refresh_alert_calls: AtomicI32,
    pub(crate) server_refresh_calls: AtomicI32,
    pub(crate) refresh_manual_cancel_backoff_calls: AtomicI32,
    pub(crate) purge_manual_cancel_backoff_calls: AtomicI32,
    pub(crate) refresh_signal: Option<(Sender<i64>, Receiver<i64>)>,
    pub(crate) purge_signal: Option<(Sender<i64>, Receiver<i64>)>,
}

impl MockMVServiceHelper {
    /// Go `runEventCount`.
    pub(crate) fn run_event_count(&self, event_type: &str) -> i64 {
        self.knobs.lock().unwrap().run_event_counts.get(event_type).copied().unwrap_or(0)
    }

    pub(crate) fn task_duration_count(&self, task_type: &str, result: &str) -> i64 {
        self.knobs.lock().unwrap().task_duration_counts.get(&format!("{task_type}/{result}")).copied().unwrap_or(0)
    }
}

impl ServerDiscovery for MockMVServiceHelper {
    fn server_filter(&self, _: &ServerInfo) -> bool {
        true
    }

    fn get_server_info(&self) -> Result<ServerInfo, String> {
        Ok(ServerInfo { id: "test-server".to_owned(), ..ServerInfo::default() })
    }

    fn get_all_server_info(&self, _: &Context) -> Result<HashMap<String, ServerInfo>, String> {
        self.server_refresh_calls.fetch_add(1, SeqCst);
        Ok(HashMap::from([("test-server".to_owned(), ServerInfo { id: "test-server".to_owned(), ..ServerInfo::default() })]))
    }
}

fn emit(signal: &Option<(Sender<i64>, Receiver<i64>)>, id: i64) {
    if let Some((tx, _)) = signal {
        let _ = tx.try_send(id);
    }
}

impl MVTaskHandler for MockMVServiceHelper {
    fn refresh_mv(&self, _: &Context, _: &dyn SessionPool, mview_id: i64) -> Result<GoTime, MvsError> {
        emit(&self.refresh_signal, mview_id);
        let mut knobs = self.knobs.lock().unwrap();
        knobs.last_refresh_id = mview_id;
        if knobs.refresh_panic {
            drop(knobs);
            panic!("mock refresh panic");
        }
        match &knobs.refresh_err {
            Some(err) => Err(err.clone()),
            None => Ok(knobs.refresh_next.clone()),
        }
    }

    fn purge_mv_log(&self, _: &Context, _: &dyn SessionPool, mlog_id: i64) -> Result<GoTime, MvsError> {
        emit(&self.purge_signal, mlog_id);
        let mut knobs = self.knobs.lock().unwrap();
        knobs.last_purge_id = mlog_id;
        if knobs.purge_panic {
            drop(knobs);
            panic!("mock purge panic");
        }
        match &knobs.purge_err {
            Some(err) => Err(err.clone()),
            None => Ok(knobs.purge_next.clone()),
        }
    }

    /// Go mock: the configured outcome, the cooldown time when none is set.
    fn try_backoff_refresh_manual_cancel(&self, _: &Context, _: &dyn SessionPool, _: i64, next: &GoTime) -> (bool, GoTime, Option<MvsError>) {
        self.refresh_manual_cancel_backoff_calls.fetch_add(1, SeqCst);
        let (applied, applied_next) = self.knobs.lock().unwrap().refresh_manual_cancel_backoff.clone();
        (applied, applied_next.unwrap_or_else(|| next.clone()), None)
    }

    fn try_backoff_purge_manual_cancel(&self, _: &Context, _: &dyn SessionPool, _: i64, next: &GoTime) -> (bool, GoTime, Option<MvsError>) {
        self.purge_manual_cancel_backoff_calls.fetch_add(1, SeqCst);
        let (applied, applied_next) = self.knobs.lock().unwrap().purge_manual_cancel_backoff.clone();
        (applied, applied_next.unwrap_or_else(|| next.clone()), None)
    }

    fn sync_mv_refresh_alert_states(
        &self,
        _: &Context,
        _: &dyn SessionPool,
        updated_at: &GoTime,
        states: &[RefreshAlertTask],
    ) -> Result<(), MvsError> {
        self.sync_refresh_alert_calls.fetch_add(1, SeqCst);
        let mut knobs = self.knobs.lock().unwrap();
        knobs.last_refresh_alert_at = updated_at.clone();
        knobs.last_refresh_alert_sync = states.to_vec();
        knobs.sync_refresh_alert_err.clone().map_or(Ok(()), Err)
    }

    fn cleanup_stale_mv_refresh_alerts(&self, _: &Context, _: &dyn SessionPool) -> Result<(), MvsError> {
        self.cleanup_stale_refresh_alert_calls.fetch_add(1, SeqCst);
        self.knobs.lock().unwrap().cleanup_stale_refresh_alert_err.clone().map_or(Ok(()), Err)
    }

    fn load_all_tidb_mv_log_purge(&self, _: &Context, _: &dyn SessionPool) -> Result<HashMap<i64, Arc<MlogPurgeTask>>, MvsError> {
        self.fetch_logs_calls.fetch_add(1, SeqCst);
        let knobs = self.knobs.lock().unwrap();
        if let Some(err) = &knobs.fetch_logs_err {
            return Err(err.clone());
        }
        if let Some((logs, _)) = &knobs.full_chain {
            return Ok(logs
                .iter()
                .map(|(id, next)| (*id, MlogPurgeTask::new(*id, next.clone(), unix_milli(next))))
                .collect());
        }
        Ok(knobs.fetch_logs.clone())
    }

    fn load_all_tidb_mv_log_accumulation_tasks(
        &self,
        _: &Context,
        _: &dyn SessionPool,
    ) -> Result<HashMap<i64, MlogAccumulationTask>, MvsError> {
        self.fetch_accumulation_task_calls.fetch_add(1, SeqCst);
        let knobs = self.knobs.lock().unwrap();
        if let Some(err) = &knobs.fetch_accumulation_tasks_err {
            return Err(err.clone());
        }
        Ok(knobs.fetch_accumulation_tasks.clone())
    }

    fn load_tidb_mv_log_accumulation_row_counts(
        &self,
        _: &Context,
        _: &dyn SessionPool,
        tasks: &HashMap<i64, MlogAccumulationTask>,
    ) -> Result<HashMap<i64, u64>, MvsError> {
        self.fetch_accumulation_row_count_calls.fetch_add(1, SeqCst);
        let (panic, err, entered, block) = {
            let knobs = self.knobs.lock().unwrap();
            (
                knobs.accumulation_row_count_panic,
                knobs.fetch_accumulation_row_counts_err.clone(),
                knobs.accumulation_row_count_entered.clone(),
                knobs.block_accumulation_row_count.clone(),
            )
        };
        assert!(!panic, "mock accumulation scan panicked");
        if let Some(err) = err {
            return Err(err);
        }
        if let Some(entered) = entered {
            let _ = entered.try_send(());
        }
        if let Some(block) = block {
            let _ = block.recv();
        }
        let mut knobs = self.knobs.lock().unwrap();
        let mut ids: Vec<i64> = tasks.keys().copied().collect();
        ids.sort_unstable();
        let ret = ids
            .iter()
            .filter_map(|id| knobs.fetch_accumulation_row_counts.get(id).map(|count| (*id, *count)))
            .collect();
        knobs.last_accumulation_row_count_task_ids = ids;
        Ok(ret)
    }

    fn load_all_tidb_mv_log_analyze_tasks(&self, _: &Context, _: &dyn SessionPool) -> Result<HashMap<i64, MlogAnalyzeTask>, MvsError> {
        Ok(HashMap::new())
    }

    fn analyze_mv_log(&self, _: &Context, _: &dyn SessionPool, _: i64) -> Result<(), MvsError> {
        Ok(())
    }

    fn load_all_tidb_mv_refresh(&self, _: &Context, _: &dyn SessionPool) -> Result<HashMap<i64, Arc<MViewTask>>, MvsError> {
        self.fetch_view_calls.fetch_add(1, SeqCst);
        let knobs = self.knobs.lock().unwrap();
        if let Some(err) = &knobs.fetch_views_err {
            return Err(err.clone());
        }
        if let Some((_, mvs)) = &knobs.full_chain {
            return Ok(mvs
                .iter()
                .map(|(id, next)| {
                    let state = MViewTaskState { next_refresh: next.clone(), ..MViewTaskState::default() };
                    (*id, MViewTask::new(*id, state, unix_milli(next)))
                })
                .collect());
        }
        Ok(knobs.fetch_views.clone())
    }

    fn get_current_tso(&self, _: &Context, _: &dyn SessionPool) -> Result<u64, MvsError> {
        Ok(1)
    }

    fn purge_mv_history_before_tso(&self, _: &Context, _: &dyn SessionPool, _: u64, _: i64, _: i64) -> Result<(), MvsError> {
        Ok(())
    }
}

impl MVMetricsReporter for MockMVServiceHelper {
    fn report_metrics(&self, _: &MVService) {}

    fn observe_task_duration(&self, task_type: &str, result: &str, duration: i64) {
        if duration < 0 {
            return;
        }
        *self.knobs.lock().unwrap().task_duration_counts.entry(format!("{task_type}/{result}")).or_default() += 1;
    }

    fn observe_run_event(&self, event_type: &str) {
        if event_type.is_empty() {
            return;
        }
        *self.knobs.lock().unwrap().run_event_counts.entry(event_type.to_owned()).or_default() += 1;
    }
}

pub(crate) fn new_service(helper: Arc<MockMVServiceHelper>, cfg: Config) -> Arc<MVService> {
    new_mv_service(Context::background(), Arc::new(MockSessionPool), helper, cfg)
}

fn max_concurrency(exec: &crate::executor::TaskExecutor) -> i64 {
    exec.max_concurrency.load(SeqCst)
}

fn new_mv_service_config_applied() {
    let mut cfg = default_mv_service_config();
    cfg.task_max_concurrency = 3;
    cfg.refresh_task_concurrency_ratio = 0.6;
    cfg.refresh_task_timeout = 42 * SECOND;
    cfg.purge_task_timeout = 84 * SECOND;
    cfg.mlog_analyze_task_concurrency = 3;
    cfg.fetch_interval = 37 * SECOND;
    cfg.basic_interval = 2 * SECOND;
    cfg.server_refresh_interval = 9 * SECOND;
    cfg.retry_base_delay = 3 * SECOND;
    cfg.retry_max_delay = 21 * SECOND;
    cfg.mview_refresh_hist_retention = 10 * 24 * HOUR;
    cfg.mlog_purge_hist_retention = 20 * 24 * HOUR;
    cfg.server_consistent_hash_replicas = 17;
    cfg.task_backpressure = TaskBackpressureConfig { cpu_threshold: 0.7, mem_threshold: 0.8, delay: 500 * MS };

    let svc = new_service(Arc::default(), cfg);
    let (refresh, purge) = split_task_concurrency(cfg.task_max_concurrency, cfg.refresh_task_concurrency_ratio);
    assert_eq!(max_concurrency(&svc.refresh_executor), refresh);
    assert_eq!(max_concurrency(&svc.purge_executor), purge);
    assert_eq!(max_concurrency(&svc.mlog_analyze_executor), cfg.mlog_analyze_task_concurrency);
    assert_eq!(svc.get_refresh_task_concurrency_ratio(), cfg.refresh_task_concurrency_ratio);
    assert_eq!(svc.retry_delay_config(), (cfg.retry_base_delay, cfg.retry_max_delay));
    assert_eq!(
        svc.history_gc_interval(),
        derive_history_gc_interval(cfg.mview_refresh_hist_retention, cfg.mlog_purge_hist_retention)
    );
    assert_eq!(svc.history_gc_retention_config(), (cfg.mview_refresh_hist_retention, cfg.mlog_purge_hist_retention));
    assert_eq!(svc.get_task_backpressure_config(), cfg.task_backpressure);
    assert_eq!(svc.fetch_interval(), cfg.fetch_interval);
    assert_eq!(svc.basic_interval, cfg.basic_interval);
    assert_eq!(svc.server_refresh_interval, cfg.server_refresh_interval);
}

fn new_mv_service_config_normalized_defaults() {
    let mut cfg = default_mv_service_config();
    cfg.task_max_concurrency = -1;
    cfg.mlog_analyze_task_concurrency = -1;
    cfg.refresh_task_concurrency_ratio = -1.0;
    cfg.mview_refresh_hist_retention = -HOUR;
    cfg.mlog_purge_hist_retention = -HOUR;
    let svc = new_service(Arc::default(), cfg);
    assert_eq!(svc.retry_delay_config(), (cfg.retry_base_delay, cfg.retry_max_delay));
    assert_eq!(svc.history_gc_interval(), DEFAULT_MV_HISTORY_GC_INTERVAL);
    assert_eq!(svc.history_gc_retention_config(), (DEFAULT_MV_HISTORY_GC_RETENTION, DEFAULT_MV_HISTORY_GC_RETENTION));
    assert_eq!(svc.get_task_backpressure_config(), cfg.task_backpressure);
    assert_eq!(svc.get_refresh_task_concurrency_ratio(), DEFAULT_MV_REFRESH_TASK_CONCURRENCY_RATIO);
    assert_eq!(max_concurrency(&svc.mlog_analyze_executor), DEFAULT_MLOG_ANALYZE_TASK_CONCURRENCY);
}

fn new_mv_service_config_invalid_panics() {
    let mut cfg = default_mv_service_config();
    cfg.retry_base_delay = 10 * SECOND;
    cfg.retry_max_delay = 2 * SECOND;
    assert!(std::panic::catch_unwind(|| new_service(Arc::default(), cfg)).is_err(), "invalid retry");

    let mut cfg = default_mv_service_config();
    cfg.task_backpressure = TaskBackpressureConfig { cpu_threshold: -1.0, mem_threshold: 0.8, delay: 0 };
    assert!(std::panic::catch_unwind(|| new_service(Arc::default(), cfg)).is_err(), "invalid backpressure");
}

/// Go `TestMVServiceUpdateConfigs`.
#[test]
fn test_mv_service_update_configs() {
    let _serial = serial();
    // fetch_interval
    let svc = new_service(Arc::default(), default_mv_service_config());
    svc.set_fetch_interval(2 * SECOND).unwrap();
    assert_eq!(svc.fetch_interval(), 2 * SECOND);
    assert!(svc.set_fetch_interval(0).is_err());
    assert!(svc.set_fetch_interval(500 * 1000).is_err());

    // task_backpressure
    svc.set_task_backpressure_config(TaskBackpressureConfig { cpu_threshold: 0.7, mem_threshold: 0.8, delay: SECOND }).unwrap();
    assert_eq!(svc.get_task_backpressure_config(), TaskBackpressureConfig { cpu_threshold: 0.7, mem_threshold: 0.8, delay: SECOND });
    svc.set_task_backpressure_config(TaskBackpressureConfig::default()).unwrap();
    assert_eq!(svc.get_task_backpressure_config(), TaskBackpressureConfig::default());
    assert_eq!(max_concurrency(&svc.mlog_analyze_executor), DEFAULT_MLOG_ANALYZE_TASK_CONCURRENCY);
    assert!(svc.set_task_backpressure_config(TaskBackpressureConfig { cpu_threshold: -1.0, mem_threshold: 0.8, delay: 0 }).is_err());

    // retry_delay
    let svc = new_service(Arc::default(), default_mv_service_config());
    assert_eq!(svc.retry_delay_config(), (DEFAULT_MV_TASK_RETRY_BASE, DEFAULT_MV_TASK_RETRY_MAX));
    svc.set_retry_delay_config(2 * SECOND, 10 * SECOND).unwrap();
    assert_eq!(svc.retry_delay_config(), (2 * SECOND, 10 * SECOND));
    assert_eq!(svc.retry_delay(0), 2 * SECOND);
    assert_eq!(svc.retry_delay(2), 4 * SECOND);
    assert_eq!(svc.retry_delay(10), 10 * SECOND);
    assert!(svc.set_retry_delay_config(0, 10 * SECOND).is_err());
    assert!(svc.set_retry_delay_config(10 * SECOND, 2 * SECOND).is_err());

    // task_concurrency_split
    let svc = new_service(Arc::default(), default_mv_service_config());
    svc.set_task_max_concurrency(10);
    svc.set_refresh_task_concurrency_ratio(0.6);
    assert_eq!((max_concurrency(&svc.refresh_executor), max_concurrency(&svc.purge_executor)), (6, 4));
    svc.set_refresh_task_concurrency_ratio(0.9);
    assert_eq!((max_concurrency(&svc.refresh_executor), max_concurrency(&svc.purge_executor)), (9, 1));
    svc.set_task_max_concurrency(2);
    assert_eq!((max_concurrency(&svc.refresh_executor), max_concurrency(&svc.purge_executor)), (1, 1));

    // history_gc
    let svc = new_service(Arc::default(), default_mv_service_config());
    assert_eq!(svc.history_gc_interval(), DEFAULT_MV_HISTORY_GC_INTERVAL);
    svc.set_mview_refresh_hist_retention(10 * 24 * HOUR).unwrap();
    svc.set_mlog_purge_hist_retention(20 * 24 * HOUR).unwrap();
    assert_eq!(svc.history_gc_interval(), 20 * MINUTE);
    assert_eq!(svc.history_gc_retention_config(), (10 * 24 * HOUR, 20 * 24 * HOUR));
    svc.set_mview_refresh_hist_retention(30 * SECOND).unwrap();
    svc.set_mlog_purge_hist_retention(90 * SECOND).unwrap();
    assert_eq!(svc.history_gc_interval(), SECOND);
    assert!(svc.set_mview_refresh_hist_retention(0).is_err());
    assert!(svc.set_mlog_purge_hist_retention(0).is_err());
    // Go `history_gc_reschedule_earlier_only`.
    mv_service_history_gc_reschedule_earlier_only();
}

fn mv_service_history_gc_reschedule_earlier_only() {
    let _clock = install_mock_time_for_test();
    let svc = new_service(Arc::default(), default_mv_service_config());
    let now = mvs_now();
    svc.next_history_gc_at_millis.store(unix_milli(&now.add(10 * MINUTE)), SeqCst);
    svc.set_mview_refresh_hist_retention(2 * MINUTE).unwrap();
    svc.set_mlog_purge_hist_retention(2 * MINUTE).unwrap();
    let after_shrink = svc.next_history_gc_at_millis.load(SeqCst) - unix_milli(&now);
    assert!((0..=61_000).contains(&after_shrink), "{after_shrink}");

    svc.next_history_gc_at_millis.store(unix_milli(&now.add(5 * SECOND)), SeqCst);
    svc.set_mview_refresh_hist_retention(12 * HOUR).unwrap();
    svc.set_mlog_purge_hist_retention(12 * HOUR).unwrap();
    assert_eq!(svc.next_history_gc_at_millis.load(SeqCst), unix_milli(&now.add(1200 * MS)));
}

/// Go `TestMVServiceFullChainSimulation`.
#[test]
fn test_mv_service_full_chain_simulation() {
    let _serial = serial();
    let (module, _clock) = install_mock_time_for_test();
    let helper = Arc::new(MockMVServiceHelper {
        refresh_signal: Some(bounded(16)),
        purge_signal: Some(bounded(16)),
        ..MockMVServiceHelper::default()
    });
    {
        let mut knobs = helper.knobs.lock().unwrap();
        knobs.refresh_next = mvs_now().add(30 * SECOND);
        knobs.purge_next = mvs_now().add(30 * SECOND);
        knobs.full_chain = Some((HashMap::new(), HashMap::new()));
    }
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let svc = new_mv_service(ctx, Arc::new(MockSessionPool), helper.clone(), default_mv_service_config());
    let calls = |h: &MockMVServiceHelper| (h.fetch_logs_calls.load(SeqCst), h.fetch_view_calls.load(SeqCst));
    let wait_fetch_cycle_since = |(logs, views): (i32, i32)| {
        eventually(
            || helper.fetch_logs_calls.load(SeqCst) > logs && helper.fetch_view_calls.load(SeqCst) > views,
            TEST_EVENTUALLY_WAIT,
            TEST_EVENTUALLY_TICK,
        );
    };
    let wait_signal = |signal: &Option<(Sender<i64>, Receiver<i64>)>, expected: i64| {
        let rx = &signal.as_ref().unwrap().1;
        loop {
            match rx.recv_timeout(TEST_EVENTUALLY_WAIT) {
                Ok(got) if got == expected => return,
                Ok(_) => {}
                Err(_) => panic!("timeout waiting for {expected}"),
            }
        }
    };
    let drain = |h: &MockMVServiceHelper| {
        while h.refresh_signal.as_ref().unwrap().1.try_recv().is_ok() {}
        while h.purge_signal.as_ref().unwrap().1.try_recv().is_ok() {}
    };
    let set_meta = |logs: &[(i64, GoTime)], mvs: &[(i64, GoTime)]| {
        helper.knobs.lock().unwrap().full_chain =
            Some((logs.iter().cloned().collect(), mvs.iter().cloned().collect()));
    };

    let startup = calls(&helper);
    let (run_done_tx, run_done) = bounded::<()>(0);
    let run_svc = Arc::clone(&svc);
    std::thread::spawn(move || {
        run_svc.run();
        drop(run_done_tx);
    });
    wait_fetch_cycle_since(startup);

    // schedule_and_execute
    drain(&helper);
    let base = calls(&helper);
    let now = mvs_now();
    set_meta(&[(2001, now.add(-SECOND))], &[(1001, now.add(-SECOND))]);
    svc.notify_ddl_change();
    wait_fetch_cycle_since(base);
    wait_signal(&helper.purge_signal, 2001);
    wait_signal(&helper.refresh_signal, 1001);
    eventually(|| svc.combined_task_executor_metrics().finished_count == 2, TEST_EVENTUALLY_WAIT, TEST_EVENTUALLY_TICK);
    let metrics = svc.combined_task_executor_metrics();
    assert_eq!(
        [metrics.submitted_count, metrics.finished_count, metrics.failed_count, metrics.timeout_count, metrics.rejected_count],
        [2, 2, 0, 0, 0]
    );
    assert_eq!(helper.task_duration_count(MV_TASK_DURATION_TYPE_REFRESH, MV_DURATION_RESULT_SUCCESS), 1);
    assert_eq!(helper.task_duration_count(MV_TASK_DURATION_TYPE_PURGE, MV_DURATION_RESULT_SUCCESS), 1);

    // retry_then_success
    drain(&helper);
    {
        let mut knobs = helper.knobs.lock().unwrap();
        knobs.refresh_err = Some(MvsError::Other("refresh failed".to_owned()));
        knobs.purge_err = Some(MvsError::Other("purge failed".to_owned()));
    }
    let base_metrics = svc.combined_task_executor_metrics();
    let base = calls(&helper);
    let now = mvs_now();
    set_meta(&[(2002, now.add(-SECOND))], &[(1002, now.add(-SECOND))]);
    svc.notify_ddl_change();
    wait_fetch_cycle_since(base);
    wait_signal(&helper.purge_signal, 2002);
    wait_signal(&helper.refresh_signal, 1002);
    eventually(
        || {
            let m = svc.combined_task_executor_metrics();
            m.finished_count >= base_metrics.finished_count + 2
                && m.failed_count >= base_metrics.failed_count + 2
                && m.submitted_count >= base_metrics.submitted_count + 2
        },
        TEST_EVENTUALLY_WAIT,
        TEST_EVENTUALLY_TICK,
    );
    {
        let mut knobs = helper.knobs.lock().unwrap();
        knobs.refresh_err = None;
        knobs.purge_err = None;
    }
    module.advance(DEFAULT_MV_TASK_RETRY_BASE + MS);
    wait_signal(&helper.purge_signal, 2002);
    wait_signal(&helper.refresh_signal, 1002);
    eventually(
        || {
            let m = svc.combined_task_executor_metrics();
            m.finished_count >= base_metrics.finished_count + 4
                && m.failed_count >= base_metrics.failed_count + 2
                && m.submitted_count >= base_metrics.submitted_count + 4
        },
        TEST_EVENTUALLY_WAIT,
        TEST_EVENTUALLY_TICK,
    );
    assert_eq!(helper.task_duration_count(MV_TASK_DURATION_TYPE_REFRESH, MV_DURATION_RESULT_SUCCESS), 2);
    assert_eq!(helper.task_duration_count(MV_TASK_DURATION_TYPE_PURGE, MV_DURATION_RESULT_SUCCESS), 2);
    assert_eq!(helper.task_duration_count(MV_TASK_DURATION_TYPE_REFRESH, MV_DURATION_RESULT_FAILED), 1);
    assert_eq!(helper.task_duration_count(MV_TASK_DURATION_TYPE_PURGE, MV_DURATION_RESULT_FAILED), 1);

    // remove_after_meta_deleted
    drain(&helper);
    let base = calls(&helper);
    set_meta(&[], &[]);
    svc.notify_ddl_change();
    wait_fetch_cycle_since(base);
    eventually(
        || svc.mlog_purge_mu.lock().unwrap().pending.is_empty() && svc.mview_refresh_mu.lock().unwrap().pending.is_empty(),
        TEST_EVENTUALLY_WAIT,
        TEST_EVENTUALLY_TICK,
    );

    cancel.cancel();
    assert!(
        matches!(run_done.recv_timeout(Duration::from_secs(1)), Err(crossbeam_channel::RecvTimeoutError::Disconnected)),
        "mv service did not stop in time"
    );
}

/// Go `TestNewMVServiceConfig` (its `applied`, `normalized_defaults` and
/// `invalid_*_panics` subtests).
#[test]
fn test_new_mv_service_config() {
    let _serial = serial();
    new_mv_service_config_applied();
    new_mv_service_config_normalized_defaults();
    new_mv_service_config_invalid_panics();
}

// ---------------------------------------------------------------------------
// Go master `mvservice_test.go`'s refresh alert and MV log accumulation tests.

/// A task as the Go cases write one: `mviewTask{ID, ...}`.
fn mview_task(id: i64, state: MViewTaskState) -> Arc<MViewTask> {
    let order_ts = if state.next_refresh.is_zero() { 0 } else { unix_milli(&state.next_refresh) };
    MViewTask::new(id, state, order_ts)
}

/// Go `addMVRefreshAlertTasksForTest`.
fn add_mv_refresh_alert_tasks_for_test(svc: &MVService, tasks: &[&Arc<MViewTask>]) {
    let pending = tasks.iter().map(|task| (task.id, Arc::clone(task))).collect();
    svc.build_mview_refresh_alert_tasks(pending);
}

/// Go `addMVRefreshExecutionTasksForTest`.
fn add_mv_refresh_execution_tasks_for_test(svc: &MVService, tasks: &[&Arc<MViewTask>]) {
    let mut queue = svc.mview_refresh_mu.lock().unwrap();
    for task in tasks {
        let item = queue.prio.push(Arc::clone(task));
        queue.pending.insert(task.id, item);
    }
}

/// Go `setServiceHashOverridesForTest`: two servers, this one `nodeA`.
fn set_service_hash_overrides_for_test(svc: &MVService, overrides: &[(Vec<u8>, u32)]) {
    let mut mapping: HashMap<Vec<u8>, u32> = HashMap::from([
        (b"nodeA#0".to_vec(), 10),
        (b"nodeB#0".to_vec(), 30),
        (MV_HISTORY_GC_OWNER_KEY.as_bytes().to_vec(), 10),
        (MV_REFRESH_ALERT_CLEANUP_OWNER_KEY.as_bytes().to_vec(), 10),
    ]);
    for (key, hash) in overrides {
        mapping.insert(key.clone(), *hash);
    }
    rebuild_owners_for_test(svc, "nodeA", mapping, &["nodeA", "nodeB"]);
}

fn rebuild_owners_for_test(svc: &MVService, self_id: &str, mapping: HashMap<Vec<u8>, u32>, servers: &[&str]) {
    let mut state = svc.sch.mu.write().unwrap();
    state.id = self_id.to_owned();
    state.chash = crate::consistenthash::ConsistentHash::new(1);
    state.chash.set_hash_func(move |data: &[u8]| {
        *mapping.get(data).unwrap_or_else(|| panic!("missing hash mapping for {}", String::from_utf8_lossy(data)))
    });
    crate::server_maintainer::ServerConsistentHash::rebuild_locked(
        &mut state,
        servers
            .iter()
            .map(|id| ((*id).to_owned(), crate::server_maintainer::ServerInfo { id: (*id).to_owned(), ..Default::default() }))
            .collect(),
    );
}

/// Go `setRefreshAlertCleanupOwnerForTest`.
fn set_refresh_alert_cleanup_owner_for_test(svc: &MVService, owner_hash: u32) {
    set_service_hash_overrides_for_test(svc, &[(MV_REFRESH_ALERT_CLEANUP_OWNER_KEY.as_bytes().to_vec(), owner_hash)]);
}

/// Go `setTaskOwnersForTest`.
fn set_task_owners_for_test(svc: &MVService, task_hashes: &[(i64, u32)]) {
    let overrides: Vec<(Vec<u8>, u32)> = task_hashes
        .iter()
        .map(|(id, hash)| (crate::server_maintainer::int64_key_to_binary_bytes(*id).to_vec(), *hash))
        .collect();
    set_service_hash_overrides_for_test(svc, &overrides);
}

/// Go `setThreeNodeRefreshAlertOwnersForTest`.
fn set_three_node_refresh_alert_owners_for_test(svc: &MVService, self_id: &str) {
    let mapping = HashMap::from([
        (b"nodeA#0".to_vec(), 10),
        (b"nodeB#0".to_vec(), 20),
        (b"nodeC#0".to_vec(), 30),
        (MV_REFRESH_ALERT_CLEANUP_OWNER_KEY.as_bytes().to_vec(), 25),
    ]);
    rebuild_owners_for_test(svc, self_id, mapping, &["nodeA", "nodeB", "nodeC"]);
}

fn set_fetch_views(helper: &MockMVServiceHelper, tasks: &[&Arc<MViewTask>]) {
    helper.knobs.lock().unwrap().fetch_views = tasks.iter().map(|task| (task.id, Arc::clone(task))).collect();
}

fn last_refresh_alert_sync(helper: &MockMVServiceHelper) -> (GoTime, Vec<RefreshAlertTask>) {
    let knobs = helper.knobs.lock().unwrap();
    (knobs.last_refresh_alert_at.clone(), knobs.last_refresh_alert_sync.clone())
}

#[test]
fn test_mv_service_collect_refresh_alert_tasks_by_alert_level() {
    let _serial = serial();
    let svc = new_service(Arc::default(), default_mv_service_config());
    let now = mvs_now();
    let warn_task = mview_task(101, MViewTaskState {
        next_refresh: now.add(-2 * MINUTE),
        last_success_time: now.add(-40 * SECOND),
        alert_warning_sec: 30,
        alert_overdue_sec: 120,
        ..MViewTaskState::default()
    });
    let overdue_task = mview_task(102, MViewTaskState {
        next_refresh: now.add(-2 * MINUTE),
        last_success_time: now.add(-70 * SECOND),
        alert_warning_sec: 30,
        alert_overdue_sec: 60,
        ..MViewTaskState::default()
    });
    // "simulate running task"
    overdue_task.order_ts.store(MAX_NEXT_SCHEDULE_TS, SeqCst);
    let disabled_task = mview_task(103, MViewTaskState {
        next_refresh: now.add(-2 * MINUTE),
        last_success_time: now.add(-5 * MINUTE),
        ..MViewTaskState::default()
    });
    add_mv_refresh_alert_tasks_for_test(&svc, &[&warn_task, &overdue_task, &disabled_task]);
    add_mv_refresh_execution_tasks_for_test(&svc, &[&warn_task, &overdue_task, &disabled_task]);

    let (got, warning_count, overdue_count) = svc.collect_refresh_alert_tasks(&now);
    assert_eq!(got.len(), 2);
    assert_eq!(1, warning_count);
    assert_eq!(1, overdue_count);
    let by_id: HashMap<i64, &RefreshAlertTask> = got.iter().map(|task| (task.mview_id, task)).collect();
    assert!(!by_id[&101].overdue);
    assert_eq!("queued", by_id[&101].task_state);
    assert_eq!(30, by_id[&101].alert_warning_sec);
    assert_eq!(120, by_id[&101].alert_overdue_sec);
    assert!(by_id[&102].overdue);
    assert_eq!("running", by_id[&102].task_state);
    assert_eq!(30, by_id[&102].alert_warning_sec);
    assert_eq!(60, by_id[&102].alert_overdue_sec);
    svc.close_task_executors();
}

#[test]
fn test_mv_service_refresh_alert_scan_interval() {
    let _serial = serial();
    let svc = new_service(Arc::default(), default_mv_service_config());
    let now = mvs_now();
    let task = mview_task(1, MViewTaskState {
        next_refresh: now.add(-2 * MINUTE),
        last_success_time: now.add(-2 * MINUTE),
        alert_warning_sec: 30,
        ..MViewTaskState::default()
    });
    add_mv_refresh_execution_tasks_for_test(&svc, &[&task]);

    svc.next_refresh_alert_scan_millis.store(0, SeqCst);
    svc.maybe_log_refresh_alert_tasks(&now);
    assert_eq!(unix_milli(&now.add(MV_REFRESH_ALERT_SCAN_INTERVAL)), svc.next_refresh_alert_scan_millis.load(SeqCst));

    // "Within scan interval: should not trigger another scan."
    svc.maybe_log_refresh_alert_tasks(&now.add(MV_REFRESH_ALERT_SCAN_INTERVAL / 2));
    assert_eq!(unix_milli(&now.add(MV_REFRESH_ALERT_SCAN_INTERVAL)), svc.next_refresh_alert_scan_millis.load(SeqCst));

    // "After interval: one more full scan and next scan time moves forward."
    svc.maybe_log_refresh_alert_tasks(&now.add(MV_REFRESH_ALERT_SCAN_INTERVAL));
    assert_eq!(unix_milli(&now.add(2 * MV_REFRESH_ALERT_SCAN_INTERVAL)), svc.next_refresh_alert_scan_millis.load(SeqCst));
    svc.close_task_executors();
}

#[test]
fn test_mv_service_collect_refresh_alert_tasks_dedup_by_last_success_read_tso() {
    let _serial = serial();
    struct Step {
        at_offset: i64,
        before_collect: Option<fn(&MViewTask, &GoTime)>,
        expected_count: usize,
        expected_id: i64,
        expected_overdue: bool,
        warning_count: i64,
        overdue_count: i64,
    }
    let step = |at_offset, expected_count, expected_id, expected_overdue, warning_count, overdue_count| Step {
        at_offset,
        before_collect: None,
        expected_count,
        expected_id,
        expected_overdue,
        warning_count,
        overdue_count,
    };
    let now = mvs_now();
    let cases: Vec<(&str, Arc<MViewTask>, Vec<Step>)> = vec![
        (
            "dedup_on_same_tso",
            mview_task(201, MViewTaskState {
                next_refresh: now.add(-2 * MINUTE),
                last_success_read_tso: 123_456,
                last_success_time: now.add(-2 * MINUTE),
                alert_warning_sec: 30,
                ..MViewTaskState::default()
            }),
            vec![
                step(0, 1, 201, false, 1, 0),
                step(10 * SECOND, 0, 0, false, 1, 0),
                Step {
                    before_collect: Some(|task, now| {
                        let mut state = task.state.lock().unwrap();
                        state.last_success_read_tso = 223_456;
                        state.last_success_time = now.add(-2 * MINUTE);
                    }),
                    ..step(20 * SECOND, 1, 201, false, 1, 0)
                },
            ],
        ),
        (
            "dedup_by_alert_level",
            mview_task(202, MViewTaskState {
                next_refresh: now.add(-2 * MINUTE),
                last_success_read_tso: 123_456,
                last_success_time: now.add(-40 * SECOND),
                alert_warning_sec: 30,
                alert_overdue_sec: 60,
                ..MViewTaskState::default()
            }),
            vec![
                step(0, 1, 202, false, 1, 0),
                step(10 * SECOND, 0, 0, false, 1, 0),
                step(30 * SECOND, 1, 202, true, 0, 1),
                step(40 * SECOND, 0, 0, false, 0, 1),
            ],
        ),
    ];
    for (name, task, steps) in cases {
        let svc = new_service(Arc::default(), default_mv_service_config());
        add_mv_refresh_alert_tasks_for_test(&svc, &[&task]);
        for s in steps {
            let at = now.add(s.at_offset);
            if let Some(before_collect) = s.before_collect {
                before_collect(&task, &at);
            }
            let (got, warning_count, overdue_count) = svc.collect_refresh_alert_tasks(&at);
            assert_eq!(got.len(), s.expected_count, "{name}");
            assert_eq!(s.warning_count, warning_count, "{name}");
            assert_eq!(s.overdue_count, overdue_count, "{name}");
            if s.expected_count > 0 {
                assert_eq!(s.expected_id, got[0].mview_id, "{name}");
                assert_eq!(s.expected_overdue, got[0].overdue, "{name}");
            }
        }
        svc.close_task_executors();
    }
}

#[test]
fn test_mv_service_maybe_log_refresh_alert_tasks_syncs_alert_states() {
    let _serial = serial();
    let helper = Arc::new(MockMVServiceHelper::default());
    let svc = new_service(helper.clone(), default_mv_service_config());
    set_refresh_alert_cleanup_owner_for_test(&svc, 10);
    let now = mvs_now();
    let state = |name: &str, tso: u64, since_success: i64, warning: i64, overdue: i64| MViewTaskState {
        schema_name: "test".to_owned(),
        mview_name: name.to_owned(),
        next_refresh: now.add(-2 * MINUTE),
        last_success_read_tso: tso,
        last_success_time: now.add(-since_success),
        alert_warning_sec: warning,
        alert_overdue_sec: overdue,
        ..MViewTaskState::default()
    };
    let warn_task = mview_task(301, state("mv_warn", 12345, 40 * SECOND, 30, 120));
    let overdue_task = mview_task(302, state("mv_overdue", 22345, 90 * SECOND, 30, 60));
    let healthy_task = mview_task(303, state("mv_healthy", 32345, 10 * SECOND, 30, 60));
    set_fetch_views(&helper, &[&warn_task, &overdue_task, &healthy_task]);

    svc.next_refresh_alert_scan_millis.store(0, SeqCst);
    svc.maybe_log_refresh_alert_tasks(&now);
    assert_eq!(1, helper.sync_refresh_alert_calls.load(SeqCst));
    assert_eq!(1, svc.metrics.alert_warning_count.load(SeqCst));
    assert_eq!(1, svc.metrics.alert_overdue_count.load(SeqCst));

    let (synced_at, synced) = last_refresh_alert_sync(&helper);
    assert_eq!(synced_at, now);
    assert_eq!(synced.len(), 3);
    let by_id: HashMap<i64, &RefreshAlertTask> = synced.iter().map(|state| (state.mview_id, state)).collect();
    assert_eq!(MV_REFRESH_ALERT_LEVEL_WARNING, by_id[&301].alert_level);
    assert_eq!(MV_REFRESH_ALERT_LEVEL_OVERDUE, by_id[&302].alert_level);
    assert!(by_id[&303].alert_level.is_empty());

    svc.next_refresh_alert_scan_millis.store(0, SeqCst);
    svc.maybe_log_refresh_alert_tasks(&now);
    assert_eq!(1, helper.sync_refresh_alert_calls.load(SeqCst));
    assert_eq!(2, helper.cleanup_stale_refresh_alert_calls.load(SeqCst));
    svc.close_task_executors();
}

#[test]
fn test_mv_service_maybe_log_refresh_alert_tasks_cleans_up_stale_rows_without_local_pending_tasks() {
    let _serial = serial();
    let helper = Arc::new(MockMVServiceHelper::default());
    let svc = new_service(helper.clone(), default_mv_service_config());
    set_refresh_alert_cleanup_owner_for_test(&svc, 10);
    let now = mvs_now();

    svc.next_refresh_alert_scan_millis.store(0, SeqCst);
    svc.maybe_log_refresh_alert_tasks(&now);
    assert_eq!(1, helper.cleanup_stale_refresh_alert_calls.load(SeqCst));
    assert_eq!(0, helper.sync_refresh_alert_calls.load(SeqCst));

    svc.maybe_log_refresh_alert_tasks(&now.add(MV_REFRESH_ALERT_SCAN_INTERVAL / 2));
    assert_eq!(1, helper.cleanup_stale_refresh_alert_calls.load(SeqCst));

    svc.maybe_log_refresh_alert_tasks(&now.add(MV_REFRESH_ALERT_SCAN_INTERVAL));
    assert_eq!(2, helper.cleanup_stale_refresh_alert_calls.load(SeqCst));
    svc.close_task_executors();
}

#[test]
fn test_mv_service_build_mv_refresh_tasks_does_not_clear_removed_alert_states() {
    let _serial = serial();
    let helper = Arc::new(MockMVServiceHelper::default());
    let svc = new_service(helper.clone(), default_mv_service_config());
    let now = mvs_now();
    let m = mview_task(401, MViewTaskState { next_refresh: now.add(MINUTE), ..MViewTaskState::default() });

    svc.build_mview_refresh_tasks(HashMap::from([(m.id, Arc::clone(&m))]));
    assert_eq!(0, helper.sync_refresh_alert_calls.load(SeqCst));

    svc.build_mview_refresh_tasks(HashMap::new());
    assert_eq!(0, helper.sync_refresh_alert_calls.load(SeqCst));
    svc.close_task_executors();
}

#[test]
fn test_mv_service_maybe_log_refresh_alert_tasks_resyncs_when_last_success_read_tso_changes() {
    let _serial = serial();
    let helper = Arc::new(MockMVServiceHelper::default());
    let svc = new_service(helper.clone(), default_mv_service_config());
    set_refresh_alert_cleanup_owner_for_test(&svc, 10);
    let now = mvs_now();
    let task = mview_task(501, MViewTaskState {
        schema_name: "test".to_owned(),
        mview_name: "mv_warn".to_owned(),
        next_refresh: now.add(-2 * MINUTE),
        last_success_read_tso: 12345,
        last_success_time: now.add(-40 * SECOND),
        alert_warning_sec: 30,
        ..MViewTaskState::default()
    });
    set_fetch_views(&helper, &[&task]);

    svc.next_refresh_alert_scan_millis.store(0, SeqCst);
    svc.maybe_log_refresh_alert_tasks(&now);
    assert_eq!(1, helper.sync_refresh_alert_calls.load(SeqCst));

    {
        let mut state = task.state.lock().unwrap();
        state.last_success_read_tso = 22345;
        state.last_success_time = now.add(-35 * SECOND);
    }
    svc.next_refresh_alert_scan_millis.store(0, SeqCst);
    svc.maybe_log_refresh_alert_tasks(&now);
    assert_eq!(2, helper.sync_refresh_alert_calls.load(SeqCst));
    svc.close_task_executors();
}

#[test]
fn test_mv_service_maybe_log_refresh_alert_tasks_skips_unresolved_metadata() {
    let _serial = serial();
    let helper = Arc::new(MockMVServiceHelper::default());
    let svc = new_service(helper.clone(), default_mv_service_config());
    set_refresh_alert_cleanup_owner_for_test(&svc, 10);
    let now = mvs_now();
    let task = mview_task(601, MViewTaskState {
        next_refresh: now.add(-2 * MINUTE),
        last_success_read_tso: 12345,
        last_success_time: now.add(-40 * SECOND),
        alert_warning_sec: 30,
        metadata_unresolved: true,
        ..MViewTaskState::default()
    });
    set_fetch_views(&helper, &[&task]);

    svc.next_refresh_alert_scan_millis.store(0, SeqCst);
    svc.maybe_log_refresh_alert_tasks(&now);
    assert_eq!(0, helper.sync_refresh_alert_calls.load(SeqCst));
    assert_eq!(1, helper.cleanup_stale_refresh_alert_calls.load(SeqCst));
    assert_eq!(0, svc.metrics.alert_warning_count.load(SeqCst));
    assert_eq!(0, svc.metrics.alert_overdue_count.load(SeqCst));
    svc.close_task_executors();
}

#[test]
fn test_mv_service_build_mv_refresh_tasks_does_not_delete_alerts_on_temporary_metadata_miss() {
    let _serial = serial();
    let helper = Arc::new(MockMVServiceHelper::default());
    let svc = new_service(helper.clone(), default_mv_service_config());
    set_refresh_alert_cleanup_owner_for_test(&svc, 10);
    let now = mvs_now();
    let existing = mview_task(602, MViewTaskState {
        next_refresh: now.add(-2 * MINUTE),
        schema_name: "test".to_owned(),
        mview_name: "mv".to_owned(),
        last_success_read_tso: 12345,
        last_success_time: now.add(-40 * SECOND),
        alert_warning_sec: 30,
        last_synced_alert_level: MV_REFRESH_ALERT_LEVEL_WARNING.to_owned(),
        last_synced_alert_read_tso: 12345,
        alert_state_initialized: true,
        ..MViewTaskState::default()
    });
    add_mv_refresh_alert_tasks_for_test(&svc, &[&existing]);
    let fetched = mview_task(602, MViewTaskState {
        next_refresh: existing.state.lock().unwrap().next_refresh.clone(),
        last_success_read_tso: 12345,
        last_success_time: now.add(-40 * SECOND),
        metadata_unresolved: true,
        ..MViewTaskState::default()
    });
    set_fetch_views(&helper, &[&fetched]);

    svc.next_refresh_alert_scan_millis.store(0, SeqCst);
    svc.maybe_log_refresh_alert_tasks(&now);
    assert_eq!(0, helper.sync_refresh_alert_calls.load(SeqCst));
    assert_eq!(1, helper.cleanup_stale_refresh_alert_calls.load(SeqCst));

    let preserved = svc.mview_refresh_alert_mu.lock().unwrap().get(&602).cloned().expect("preserved");
    let preserved = preserved.state.lock().unwrap();
    assert!(preserved.alert_state_initialized);
    assert_eq!(MV_REFRESH_ALERT_LEVEL_WARNING, preserved.last_synced_alert_level);
    assert_eq!(12345, preserved.last_synced_alert_read_tso);
    drop(preserved);
    svc.close_task_executors();
}

#[test]
fn test_mv_service_maybe_log_refresh_alert_tasks_skips_cleanup_when_not_owner() {
    let _serial = serial();
    let helper = Arc::new(MockMVServiceHelper::default());
    let svc = new_service(helper.clone(), default_mv_service_config());
    set_refresh_alert_cleanup_owner_for_test(&svc, 20);
    let now = mvs_now();

    svc.next_refresh_alert_scan_millis.store(0, SeqCst);
    svc.maybe_log_refresh_alert_tasks(&now);
    assert_eq!(0, helper.cleanup_stale_refresh_alert_calls.load(SeqCst));
    assert_eq!(0, helper.sync_refresh_alert_calls.load(SeqCst));
    svc.close_task_executors();
}

#[test]
fn test_mv_service_refresh_alert_checker_owners_scan_global_tasks() {
    let _serial = serial();
    let now = mvs_now();
    let new_task = || {
        mview_task(701, MViewTaskState {
            schema_name: "test".to_owned(),
            mview_name: "mv_overdue".to_owned(),
            next_refresh: now.add(-2 * MINUTE),
            last_success_read_tso: 12345,
            last_success_time: now.add(-90 * SECOND),
            alert_warning_sec: 30,
            alert_overdue_sec: 60,
            ..MViewTaskState::default()
        })
    };
    // (name, self_id, checker_owner, metrics_owner, fetch_calls, sync_calls, overdue_count, task_state)
    let cases = [
        ("first_server_reports_metrics", "nodeA", true, true, 1, 1, 1, MV_REFRESH_ALERT_TASK_STATE_UNKNOWN),
        ("second_server_does_not_report_metrics", "nodeB", true, false, 1, 1, 0, MV_REFRESH_ALERT_TASK_STATE_UNKNOWN),
        ("third_server_is_not_checker_owner", "nodeC", false, false, 0, 0, 0, ""),
    ];
    for (name, self_id, checker_owner, metrics_owner, fetch_calls, sync_calls, overdue_count, task_state) in cases {
        let task = new_task();
        let helper = Arc::new(MockMVServiceHelper::default());
        set_fetch_views(&helper, &[&task]);
        let svc = new_service(helper.clone(), default_mv_service_config());
        set_three_node_refresh_alert_owners_for_test(&svc, self_id);
        assert_eq!(checker_owner, svc.is_refresh_alert_checker_owner(), "{name}");
        assert_eq!(metrics_owner, svc.is_refresh_alert_metrics_owner(), "{name}");
        svc.metrics.alert_warning_count.store(7, SeqCst);
        svc.metrics.alert_overdue_count.store(7, SeqCst);
        let (reported_warning, reported_overdue) = svc.refresh_alert_metric_counts();
        let expected = if metrics_owner { 7 } else { 0 };
        assert_eq!((expected, expected), (reported_warning, reported_overdue), "{name}");

        svc.next_refresh_alert_scan_millis.store(0, SeqCst);
        svc.maybe_log_refresh_alert_tasks(&now);
        assert_eq!(fetch_calls, helper.fetch_view_calls.load(SeqCst), "{name}");
        assert_eq!(sync_calls, helper.sync_refresh_alert_calls.load(SeqCst), "{name}");
        assert_eq!(0, svc.metrics.alert_warning_count.load(SeqCst), "{name}");
        assert_eq!(overdue_count, svc.metrics.alert_overdue_count.load(SeqCst), "{name}");
        if sync_calls > 0 {
            let (_, synced) = last_refresh_alert_sync(&helper);
            assert_eq!(synced.len(), 1, "{name}");
            assert_eq!(task.id, synced[0].mview_id, "{name}");
            assert_eq!(MV_REFRESH_ALERT_LEVEL_OVERDUE, synced[0].alert_level, "{name}");
            assert_eq!(task_state, synced[0].task_state, "{name}");
        }
        svc.close_task_executors();
    }
}

#[test]
fn test_mv_service_refresh_alert_checker_owner_single_server() {
    let _serial = serial();
    let svc = new_service(Arc::default(), default_mv_service_config());
    {
        let mut state = svc.sch.mu.write().unwrap();
        state.id = "nodeA".to_owned();
        crate::server_maintainer::ServerConsistentHash::rebuild_locked(
            &mut state,
            HashMap::from([(
                "nodeA".to_owned(),
                crate::server_maintainer::ServerInfo { id: "nodeA".to_owned(), ..Default::default() },
            )]),
        );
    }
    assert!(svc.is_refresh_alert_checker_owner());
    assert!(svc.is_refresh_alert_metrics_owner());

    svc.sch.mu.write().unwrap().id = "nodeB".to_owned();
    assert!(!svc.is_refresh_alert_checker_owner());
    assert!(!svc.is_refresh_alert_metrics_owner());
    svc.close_task_executors();
}

fn accumulation_task(name: &str, alert_rows: u64) -> MlogAccumulationTask {
    MlogAccumulationTask { schema_name: "test".to_owned(), mlog_name: name.to_owned(), alert_rows }
}

fn accumulation_scan_idle(svc: &MVService) -> bool {
    !svc.mv_log_accumulation_scan_running.load(SeqCst)
}

#[test]
fn test_mv_service_maybe_scan_mv_log_accumulation_alerts_filters_unowned_tasks() {
    let _serial = serial();
    let helper = Arc::new(MockMVServiceHelper::default());
    {
        let mut knobs = helper.knobs.lock().unwrap();
        knobs.fetch_accumulation_tasks =
            HashMap::from([(101, accumulation_task("mlog_101", 1000)), (102, accumulation_task("mlog_102", 1000))]);
        knobs.fetch_accumulation_row_counts = HashMap::from([(101, 1500), (102, 2500)]);
    }
    let svc = new_service(helper.clone(), default_mv_service_config());
    set_task_owners_for_test(&svc, &[(101, 10), (102, 30)]);

    let now = mvs_now();
    svc.next_mv_log_accumulation_scan_millis.store(0, SeqCst);
    svc.maybe_scan_mv_log_accumulation_alerts(&now);
    eventually(
        || {
            accumulation_scan_idle(&svc)
                && helper.fetch_accumulation_task_calls.load(SeqCst) == 1
                && helper.fetch_accumulation_row_count_calls.load(SeqCst) == 1
                && svc.metrics.mv_log_accumulation_count.load(SeqCst) == 1
        },
        TEST_EVENTUALLY_WAIT,
        TEST_EVENTUALLY_TICK,
    );
    assert_eq!(vec![101], helper.knobs.lock().unwrap().last_accumulation_row_count_task_ids);

    svc.maybe_scan_mv_log_accumulation_alerts(&now.add(MV_LOG_ACCUMULATION_ALERT_SCAN_INTERVAL / 2));
    assert_eq!(1, helper.fetch_accumulation_task_calls.load(SeqCst));

    svc.next_mv_log_accumulation_scan_millis.store(0, SeqCst);
    {
        let mut knobs = helper.knobs.lock().unwrap();
        knobs.fetch_accumulation_tasks =
            HashMap::from([(101, accumulation_task("mlog_101", 1000)), (102, accumulation_task("mlog_102", 1000))]);
        knobs.fetch_accumulation_row_counts = HashMap::from([(101, 1800), (102, 2800)]);
    }
    svc.maybe_scan_mv_log_accumulation_alerts(&now.add(MV_LOG_ACCUMULATION_ALERT_SCAN_INTERVAL));
    eventually(
        || {
            accumulation_scan_idle(&svc)
                && helper.fetch_accumulation_task_calls.load(SeqCst) == 2
                && helper.fetch_accumulation_row_count_calls.load(SeqCst) == 2
                && svc.metrics.mv_log_accumulation_count.load(SeqCst) == 1
        },
        TEST_EVENTUALLY_WAIT,
        TEST_EVENTUALLY_TICK,
    );
    assert_eq!(vec![101], helper.knobs.lock().unwrap().last_accumulation_row_count_task_ids);
    svc.close_task_executors();
}

#[test]
fn test_mv_service_maybe_scan_mv_log_accumulation_alerts_preserves_last_value_on_error() {
    let _serial = serial();
    // (name, tasks_err, row_counts_err, row_count_panic)
    let cases = [
        ("task_fetch_error", Some(MvsError::Other("mock accumulation task fetch failed".to_owned())), None, false),
        ("row_count_error", None, Some(MvsError::Other("mock accumulation scan failed".to_owned())), false),
        ("row_count_panic", None, None, true),
    ];
    for (name, tasks_err, row_counts_err, row_count_panic) in cases {
        let (clock, _guard) = install_mock_time_for_test();
        let helper = Arc::new(MockMVServiceHelper::default());
        {
            let mut knobs = helper.knobs.lock().unwrap();
            knobs.fetch_accumulation_tasks = HashMap::from([(101, accumulation_task("mlog_101", 1000))]);
            knobs.fetch_accumulation_row_counts = HashMap::from([(101, 1500)]);
        }
        let svc = new_service(helper.clone(), default_mv_service_config());
        set_task_owners_for_test(&svc, &[(101, 10)]);

        svc.maybe_scan_mv_log_accumulation_alerts(&clock.now());
        eventually(
            || accumulation_scan_idle(&svc) && svc.metrics.mv_log_accumulation_count.load(SeqCst) == 1,
            TEST_EVENTUALLY_WAIT,
            TEST_EVENTUALLY_TICK,
        );

        {
            let mut knobs = helper.knobs.lock().unwrap();
            knobs.fetch_accumulation_tasks_err = tasks_err;
            knobs.fetch_accumulation_row_counts_err = row_counts_err;
            knobs.accumulation_row_count_panic = row_count_panic;
            knobs.fetch_accumulation_row_counts.insert(101, 500);
        }
        clock.advance(MV_LOG_ACCUMULATION_ALERT_SCAN_INTERVAL);
        svc.maybe_scan_mv_log_accumulation_alerts(&clock.now());
        eventually(
            || accumulation_scan_idle(&svc) && helper.fetch_accumulation_task_calls.load(SeqCst) == 2,
            TEST_EVENTUALLY_WAIT,
            TEST_EVENTUALLY_TICK,
        );
        assert_eq!(1, svc.metrics.mv_log_accumulation_count.load(SeqCst), "{name}");
        assert_eq!(
            unix_milli(&clock.now().add(MV_LOG_ACCUMULATION_ALERT_SCAN_INTERVAL)),
            svc.next_mv_log_accumulation_scan_millis.load(SeqCst),
            "{name}"
        );

        clock.advance(svc.basic_interval);
        svc.maybe_scan_mv_log_accumulation_alerts(&clock.now());
        assert!(accumulation_scan_idle(&svc), "{name}");
        assert_eq!(2, helper.fetch_accumulation_task_calls.load(SeqCst), "{name}");

        {
            let mut knobs = helper.knobs.lock().unwrap();
            knobs.fetch_accumulation_tasks_err = None;
            knobs.fetch_accumulation_row_counts_err = None;
            knobs.accumulation_row_count_panic = false;
        }
        clock.advance(MV_LOG_ACCUMULATION_ALERT_SCAN_INTERVAL - svc.basic_interval);
        svc.maybe_scan_mv_log_accumulation_alerts(&clock.now());
        eventually(
            || accumulation_scan_idle(&svc) && helper.fetch_accumulation_task_calls.load(SeqCst) == 3,
            TEST_EVENTUALLY_WAIT,
            TEST_EVENTUALLY_TICK,
        );
        assert_eq!(0, svc.metrics.mv_log_accumulation_count.load(SeqCst), "{name}");
        svc.close_task_executors();
    }
}

#[test]
fn test_mv_service_maybe_scan_mv_log_accumulation_alerts_uses_strict_greater_than() {
    let _serial = serial();
    let helper = Arc::new(MockMVServiceHelper::default());
    {
        let mut knobs = helper.knobs.lock().unwrap();
        knobs.fetch_accumulation_tasks = HashMap::from([(101, accumulation_task("mlog_101", 50))]);
        knobs.fetch_accumulation_row_counts = HashMap::from([(101, 50)]);
    }
    let svc = new_service(helper.clone(), default_mv_service_config());
    set_task_owners_for_test(&svc, &[(101, 10)]);

    let now = mvs_now();
    svc.next_mv_log_accumulation_scan_millis.store(0, SeqCst);
    svc.maybe_scan_mv_log_accumulation_alerts(&now);
    eventually(
        || accumulation_scan_idle(&svc) && helper.fetch_accumulation_row_count_calls.load(SeqCst) == 1,
        TEST_EVENTUALLY_WAIT,
        TEST_EVENTUALLY_TICK,
    );
    assert_eq!(0, svc.metrics.mv_log_accumulation_count.load(SeqCst));
    assert_eq!(vec![101], helper.knobs.lock().unwrap().last_accumulation_row_count_task_ids);
    svc.close_task_executors();
}

#[test]
fn test_mv_service_maybe_scan_mv_log_accumulation_alerts_async_and_non_overlapping() {
    let _serial = serial();
    let (entered_tx, entered_rx) = crossbeam_channel::bounded::<()>(2);
    let (block_tx, block_rx) = crossbeam_channel::bounded::<()>(0);
    let helper = Arc::new(MockMVServiceHelper::default());
    {
        let mut knobs = helper.knobs.lock().unwrap();
        knobs.fetch_accumulation_tasks = HashMap::from([(101, accumulation_task("mlog_101", 1000))]);
        knobs.fetch_accumulation_row_counts = HashMap::from([(101, 1500)]);
        knobs.accumulation_row_count_entered = Some(entered_tx);
        knobs.block_accumulation_row_count = Some(block_rx);
    }
    let svc = new_service(helper.clone(), default_mv_service_config());
    set_task_owners_for_test(&svc, &[(101, 10)]);

    let now = mvs_now();
    svc.next_mv_log_accumulation_scan_millis.store(0, SeqCst);
    let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(1);
    {
        let svc = Arc::clone(&svc);
        let now = now.clone();
        std::thread::spawn(move || {
            svc.maybe_scan_mv_log_accumulation_alerts(&now);
            let _ = done_tx.send(());
        });
    }
    entered_rx.recv_timeout(TEST_EVENTUALLY_WAIT).expect("the row count scan starts");
    done_rx.recv_timeout(TEST_EVENTUALLY_WAIT).expect("the scan runs asynchronously");
    assert_eq!(0, svc.metrics.mv_log_accumulation_count.load(SeqCst));

    svc.next_mv_log_accumulation_scan_millis.store(0, SeqCst);
    svc.maybe_scan_mv_log_accumulation_alerts(&now.add(MV_LOG_ACCUMULATION_ALERT_SCAN_INTERVAL));
    assert_eq!(1, helper.fetch_accumulation_task_calls.load(SeqCst));
    assert_eq!(1, helper.fetch_accumulation_row_count_calls.load(SeqCst));

    // Go `close(helper.blockAccumulationRowCount)`.
    drop(block_tx);
    eventually(
        || {
            accumulation_scan_idle(&svc)
                && svc.metrics.mv_log_accumulation_count.load(SeqCst) == 1
                && helper.fetch_accumulation_task_calls.load(SeqCst) == 1
                && helper.fetch_accumulation_row_count_calls.load(SeqCst) == 1
        },
        TEST_EVENTUALLY_WAIT,
        TEST_EVENTUALLY_TICK,
    );
    svc.close_task_executors();
}
