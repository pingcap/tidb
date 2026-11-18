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
        let knobs = self.knobs.lock().unwrap();
        match &knobs.refresh_err {
            Some(err) => Err(err.clone()),
            None => Ok(knobs.refresh_next.clone()),
        }
    }

    fn purge_mv_log(&self, _: &Context, _: &dyn SessionPool, mlog_id: i64) -> Result<GoTime, MvsError> {
        emit(&self.purge_signal, mlog_id);
        let knobs = self.knobs.lock().unwrap();
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
        _: &GoTime,
        states: &[RefreshAlertTask],
    ) -> Result<(), MvsError> {
        self.sync_refresh_alert_calls.fetch_add(1, SeqCst);
        let mut knobs = self.knobs.lock().unwrap();
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
        let mut knobs = self.knobs.lock().unwrap();
        if let Some(err) = &knobs.fetch_accumulation_row_counts_err {
            return Err(err.clone());
        }
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

#[test]
fn new_mv_service_config_applied() {
    let _serial = serial();
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

#[test]
fn new_mv_service_config_normalized_defaults() {
    let _serial = serial();
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

#[test]
fn new_mv_service_config_invalid_panics() {
    let _serial = serial();
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
fn mv_service_update_configs() {
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
}

#[test]
fn mv_service_history_gc_reschedule_earlier_only() {
    let _serial = serial();
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
fn mv_service_full_chain_simulation() {
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
