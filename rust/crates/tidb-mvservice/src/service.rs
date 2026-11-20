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

//! Go master `pkg/mvservice/service.go`: the in-memory scheduler of MV
//! refresh, MV log purge and MV log analyze tasks.

use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;
use tidb_timer::go_time::{GoTime, MINUTE, SECOND};

use crate::backpressure::TaskBackpressureController;
use crate::executor::TaskExecutor;
use crate::server_maintainer::{int64_key_to_binary_bytes, ServerConsistentHash, ServerDiscovery, ServerInfo};
use crate::task_handler::{is_mv_task_canceled_manually, MVTaskHandler, MvsError, SessionPool};
use crate::time_proxy::{mvs_now, mvs_since, mvs_unix_milli, mvs_until, unix_milli, MvsTimer};
use crate::utils::{Item, Lessable, Notifier, PriorityQueue};

/// Go `MVMetricsReporter`: the service's runtime metrics.
pub trait MVMetricsReporter: Send + Sync {
    /// Go `reportMetrics`.
    fn report_metrics(&self, service: &MVService);
    /// Go `observeTaskDuration`.
    fn observe_task_duration(&self, task_type: &str, result: &str, duration: i64);
    /// Go `observeRunEvent`.
    fn observe_run_event(&self, event_type: &str);
}

/// Go `Helper`: every external dependency of the service.
pub trait Helper: ServerDiscovery + MVTaskHandler + MVMetricsReporter {}

impl<T: ServerDiscovery + MVTaskHandler + MVMetricsReporter> Helper for T {}

/// The helper as the hash ring's [`ServerDiscovery`].
struct HelperDiscovery(Arc<dyn Helper>);

impl ServerDiscovery for HelperDiscovery {
    fn server_filter(&self, server: &ServerInfo) -> bool {
        self.0.server_filter(server)
    }

    fn get_server_info(&self) -> Result<ServerInfo, String> {
        self.0.get_server_info()
    }

    fn get_all_server_info(&self, ctx: &Context) -> Result<HashMap<String, ServerInfo>, String> {
        self.0.get_all_server_info(ctx)
    }
}

/// Go `DefaultMVRefreshTaskTimeout`.
pub const DEFAULT_MV_REFRESH_TASK_TIMEOUT: i64 = 5 * MINUTE;
/// Go `DefaultMVPurgeTaskTimeout`.
pub const DEFAULT_MV_PURGE_TASK_TIMEOUT: i64 = 10 * MINUTE;
pub(crate) const DEFAULT_MV_REFRESH_TASK_CONCURRENCY_RATIO: f64 = 0.6;
pub(crate) const DEFAULT_MV_FETCH_INTERVAL: i64 = 30 * SECOND;
pub(crate) const DEFAULT_MV_BASIC_INTERVAL: i64 = SECOND;
pub(crate) const DEFAULT_SERVER_REFRESH_INTERVAL: i64 = 5 * SECOND;
pub(crate) const DEFAULT_MV_HISTORY_GC_INTERVAL: i64 = 20 * MINUTE;
pub(crate) const DEFAULT_MV_HISTORY_GC_RETENTION: i64 = 365 * 24 * 60 * MINUTE;
pub(crate) const DEFAULT_MV_HISTORY_GC_MAX_RECORDS: u64 = 1_000_000;
pub(crate) const DEFAULT_MV_TASK_RETRY_BASE: i64 = 10 * SECOND;
pub(crate) const DEFAULT_MV_TASK_RETRY_MAX: i64 = 120 * SECOND;
pub(crate) const MANUAL_CANCEL_BACKOFF_DELAY: i64 = 2 * MINUTE;
pub(crate) const MV_REFRESH_ALERT_SCAN_INTERVAL: i64 = 30 * SECOND;
pub(crate) const MV_LOG_ACCUMULATION_ALERT_SCAN_INTERVAL: i64 = 20 * MINUTE;
pub(crate) const MLOG_ANALYZE_SCAN_INTERVAL: i64 = 3 * MINUTE;
pub(crate) const DEFAULT_MLOG_ANALYZE_TASK_CONCURRENCY: i64 = 2;
/// Go `maxNextScheduleTs` (`9e18`): a task being run.
pub(crate) const MAX_NEXT_SCHEDULE_TS: i64 = 9_000_000_000_000_000_000;

pub(crate) const DEFAULT_CH_REPLICAS: i64 = 100;

pub(crate) const MV_TASK_DURATION_TYPE_REFRESH: &str = "mv_refresh";
pub(crate) const MV_TASK_DURATION_TYPE_PURGE: &str = "mvlog_purge";
pub(crate) const MV_TASK_DURATION_TYPE_MLOG_ANALYZE: &str = "mvlog_analyze";

pub(crate) const MV_FETCH_TYPE_MLOG_PURGE: &str = "fetch_mlog";
pub(crate) const MV_FETCH_TYPE_MLOG_ACCUMULATION: &str = "fetch_mlog_accumulation";
pub(crate) const MV_FETCH_TYPE_MVIEW_REFRESH: &str = "fetch_mviews";
pub(crate) const MV_FETCH_TYPE_MLOG_ANALYZE: &str = "fetch_mlog_analyze";

pub(crate) const MV_TASK_DURATION_TYPE_HISTORY_GC: &str = "history_gc";

pub(crate) const MV_DURATION_RESULT_SUCCESS: &str = "success";
pub(crate) const MV_DURATION_RESULT_FAILED: &str = "failed";

pub(crate) const MV_RUN_EVENT_INIT_FAILED: &str = "init_failed";
pub(crate) const MV_RUN_EVENT_SERVER_CHANGED: &str = "server_changed";
pub(crate) const MV_RUN_EVENT_SERVER_REFRESH_ERROR: &str = "server_refresh_error";
pub(crate) const MV_RUN_EVENT_FETCH_BY_DDL: &str = "fetch_meta_by_ddl";
pub(crate) const MV_RUN_EVENT_FETCH_BY_INTERVAL: &str = "fetch_meta_by_interval";
pub(crate) const MV_RUN_EVENT_GET_TSO_ERR: &str = "get_tso_error";

pub(crate) const MV_HISTORY_GC_OWNER_KEY: &str = "gc-mv-op-hist";
/// A single hash-ring owner performs stale refresh-alert cleanup to avoid
/// repeating the same global delete on every TiDB node.
pub(crate) const MV_REFRESH_ALERT_CLEANUP_OWNER_KEY: &str = "gc-mv-refresh-alert";

pub(crate) const HISTORY_GC_RETRY_MAX_ATTEMPTS: i64 = 8;
pub(crate) const MV_REFRESH_ALERT_LEVEL_WARNING: &str = "warning";
pub(crate) const MV_REFRESH_ALERT_LEVEL_OVERDUE: &str = "overdue";

pub(crate) const MV_REFRESH_ALERT_TASK_STATE_RUNNING: &str = "running";
pub(crate) const MV_REFRESH_ALERT_TASK_STATE_QUEUED: &str = "queued";
pub(crate) const MV_REFRESH_ALERT_TASK_STATE_UNKNOWN: &str = "unknown";

/// The mutable fields of Go's `mviewTask`, which Go guards with the queue
/// lock that holds the task.
#[derive(Clone, Debug, Default)]
pub struct MViewTaskState {
    /// Go `nextRefresh`.
    pub next_refresh: GoTime,
    /// Go `schemaName`.
    pub schema_name: String,
    /// Go `mviewName`.
    pub mview_name: String,
    /// Go `metadataUnresolved`.
    pub metadata_unresolved: bool,
    /// Go `lastSuccessReadTSO`.
    pub last_success_read_tso: u64,
    /// Go `lastSuccessTime`.
    pub last_success_time: GoTime,
    /// Go `alertWarningSec`.
    pub alert_warning_sec: i64,
    /// Go `alertOverdueSec`.
    pub alert_overdue_sec: i64,
    /// Suppress repeated overdue logs for the same last success read TSO.
    pub(crate) last_logged_warning_tso: u64,
    pub(crate) last_logged_overdue_tso: u64,
    pub(crate) last_synced_alert_level: String,
    pub(crate) last_synced_alert_read_tso: u64,
    pub(crate) alert_state_initialized: bool,
}

/// Go `mviewTask`.
#[derive(Debug)]
pub struct MViewTask {
    /// Go `ID`.
    pub id: i64,
    pub(crate) state: Mutex<MViewTaskState>,
    /// Go `orderTs`: unix milliseconds.
    pub(crate) order_ts: AtomicI64,
    pub(crate) retry_count: AtomicI64,
}

impl MViewTask {
    /// A task as Go's loader builds one.
    #[must_use]
    pub fn new(id: i64, state: MViewTaskState, order_ts: i64) -> Arc<Self> {
        Arc::new(Self { id, state: Mutex::new(state), order_ts: AtomicI64::new(order_ts), retry_count: AtomicI64::new(0) })
    }

    /// A copy of the task's fields.
    #[must_use]
    pub fn snapshot(&self) -> MViewTaskState {
        self.state.lock().unwrap().clone()
    }

    /// Go `orderTs`.
    #[must_use]
    pub fn order_ts(&self) -> i64 {
        self.order_ts.load(Ordering::SeqCst)
    }

    /// Go `retryCount`.
    #[must_use]
    pub fn retry_count(&self) -> i64 {
        self.retry_count.load(Ordering::SeqCst)
    }
}

impl Lessable for Arc<MViewTask> {
    fn less(&self, other: &Self) -> bool {
        self.order_ts() < other.order_ts()
    }
}

/// Go `mlogPurgeTask`: the scheduling state of one MV log purge.
#[derive(Debug)]
pub struct MlogPurgeTask {
    /// Go `ID`.
    pub id: i64,
    pub(crate) next_purge: Mutex<GoTime>,
    /// Go `orderTs`: unix milliseconds.
    pub(crate) order_ts: AtomicI64,
    pub(crate) retry_count: AtomicI64,
}

impl MlogPurgeTask {
    /// A task as Go's loader builds one.
    #[must_use]
    pub fn new(id: i64, next_purge: GoTime, order_ts: i64) -> Arc<Self> {
        Arc::new(Self { id, next_purge: Mutex::new(next_purge), order_ts: AtomicI64::new(order_ts), retry_count: AtomicI64::new(0) })
    }

    /// Go `nextPurge`.
    #[must_use]
    pub fn next_purge(&self) -> GoTime {
        self.next_purge.lock().unwrap().clone()
    }

    /// Go `orderTs`.
    #[must_use]
    pub fn order_ts(&self) -> i64 {
        self.order_ts.load(Ordering::SeqCst)
    }

    /// Go `retryCount`.
    #[must_use]
    pub fn retry_count(&self) -> i64 {
        self.retry_count.load(Ordering::SeqCst)
    }
}

impl Lessable for Arc<MlogPurgeTask> {
    fn less(&self, other: &Self) -> bool {
        self.order_ts() < other.order_ts()
    }
}

/// Go `TaskBackpressureConfig`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TaskBackpressureConfig {
    /// Go `CPUThreshold`.
    pub cpu_threshold: f64,
    /// Go `MemThreshold`.
    pub mem_threshold: f64,
    /// Go `Delay`, in nanoseconds.
    pub delay: i64,
}

/// Go `taskExecutorMetricsSnapshot`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TaskExecutorMetricsSnapshot {
    /// Go `SubmittedCount`.
    pub submitted_count: i64,
    /// Go `FinishedCount`.
    pub finished_count: i64,
    /// Go `FailedCount`.
    pub failed_count: i64,
    /// Go `TimeoutCount`.
    pub timeout_count: i64,
    /// Go `RejectedCount`.
    pub rejected_count: i64,
    /// Go `BackpressureCount`.
    pub backpressure_count: i64,
    /// Go `RunningCount`.
    pub running_count: i64,
    /// Go `WaitingCount`.
    pub waiting_count: i64,
    /// Go `TimedOutRunningCount`.
    pub timed_out_running_count: i64,
    /// Go `BackpressureBlocked`.
    pub backpressure_blocked: i64,
}

/// Go `snapshotTaskExecutorMetrics`.
#[must_use]
pub fn snapshot_task_executor_metrics(exec: &TaskExecutor) -> TaskExecutorMetricsSnapshot {
    let load = |value: &AtomicI64| value.load(Ordering::SeqCst);
    let counters = &exec.metrics.counters;
    let gauges = &exec.metrics.gauges;
    TaskExecutorMetricsSnapshot {
        submitted_count: load(&counters.submitted_count),
        finished_count: load(&counters.finished_count),
        failed_count: load(&counters.failed_count),
        timeout_count: load(&counters.timeout_count),
        rejected_count: load(&counters.rejected_count),
        backpressure_count: load(&counters.backpressure_count),
        running_count: load(&gauges.running_count),
        waiting_count: load(&gauges.waiting_count),
        timed_out_running_count: load(&gauges.timed_out_running_count),
        backpressure_blocked: load(&gauges.backpressure_blocked),
    }
}

impl TaskExecutorMetricsSnapshot {
    /// Go `add`.
    #[must_use]
    pub fn add(self, other: Self) -> Self {
        Self {
            submitted_count: self.submitted_count + other.submitted_count,
            finished_count: self.finished_count + other.finished_count,
            failed_count: self.failed_count + other.failed_count,
            timeout_count: self.timeout_count + other.timeout_count,
            rejected_count: self.rejected_count + other.rejected_count,
            backpressure_count: self.backpressure_count + other.backpressure_count,
            running_count: self.running_count + other.running_count,
            waiting_count: self.waiting_count + other.waiting_count,
            timed_out_running_count: self.timed_out_running_count + other.timed_out_running_count,
            backpressure_blocked: self.backpressure_blocked + other.backpressure_blocked,
        }
    }
}

/// Go `MVService.metrics`.
#[derive(Debug, Default)]
pub struct ServiceMetrics {
    /// Go `MVCount`.
    pub mv_count: AtomicI64,
    /// Go `MVLogCount`.
    pub mv_log_count: AtomicI64,
    /// Go `RunningMVRefreshCount`.
    pub running_mv_refresh_count: AtomicI64,
    /// Go `RunningMVLogPurgeCount`.
    pub running_mv_log_purge_count: AtomicI64,
    /// Go `RunningMVLogAnalyzeCount`.
    pub running_mv_log_analyze_count: AtomicI64,
    /// Go `AlertWarningCount`.
    pub alert_warning_count: AtomicI64,
    /// Go `AlertOverdueCount`.
    pub alert_overdue_count: AtomicI64,
    /// Go `MVLogAccumulationCount`.
    pub mv_log_accumulation_count: AtomicI64,
    /// Go `MVLogAnalyzeTaskCount`.
    pub mv_log_analyze_task_count: AtomicI64,
}

/// Go `mviewRefreshMu` / `mlogPurgeMu`: the pending tasks and their heap.
pub(crate) struct TaskQueue<T> {
    pub(crate) pending: HashMap<i64, Item>,
    pub(crate) prio: PriorityQueue<Arc<T>>,
}

impl<T> Default for TaskQueue<T> {
    fn default() -> Self {
        Self { pending: HashMap::new(), prio: PriorityQueue::default() }
    }
}

impl<T> TaskQueue<T>
where
    Arc<T>: Lessable,
{
    /// Go `pending[id]` and `it.Value == task`: the handle of exactly this task.
    fn item_of(&self, id: i64, task: &Arc<T>) -> Option<Item> {
        let item = *self.pending.get(&id)?;
        self.prio.get(item).filter(|value| Arc::ptr_eq(value, task)).map(|_| item)
    }
}

/// Go `refreshAlertTask`.
#[derive(Clone, Debug, Default)]
pub struct RefreshAlertTask {
    /// Go `MViewId`.
    pub mview_id: i64,
    /// Go `SchemaName`.
    pub schema_name: String,
    /// Go `MViewName`.
    pub mview_name: String,
    /// Go `NextRefresh`.
    pub next_refresh: GoTime,
    /// Go `MetadataUnresolved`.
    pub metadata_unresolved: bool,
    /// Go `LastSuccessTime`.
    pub last_success_time: GoTime,
    /// Go `LastSuccessReadTSO`.
    pub last_success_read_tso: u64,
    /// Go `AlertWarningSec`.
    pub alert_warning_sec: i64,
    /// Go `AlertOverdueSec`.
    pub alert_overdue_sec: i64,
    /// Go `RetryCount`.
    pub retry_count: i64,
    /// Go `TaskState`.
    pub task_state: String,
    /// Go `Overdue`.
    pub overdue: bool,
    /// Go `AlertLevel`.
    pub alert_level: String,
}

/// Go `refreshAlertExecutionState`.
#[derive(Clone, Copy, Debug, Default)]
struct RefreshAlertExecutionState {
    running: bool,
    retry_count: i64,
}

/// Go `MVService`: the in-memory scheduler and executor for MV
/// refresh/purge tasks.
pub struct MVService {
    pub(crate) sys_session_pool: Arc<dyn SessionPool>,
    pub(crate) last_meta_fetch_millis: AtomicI64,
    pub(crate) ctx: Context,
    pub(crate) sch: ServerConsistentHash,

    pub(crate) next_refresh_alert_scan_millis: AtomicI64,
    pub(crate) next_mv_log_accumulation_scan_millis: AtomicI64,

    pub(crate) refresh_executor: Arc<TaskExecutor>,
    pub(crate) purge_executor: Arc<TaskExecutor>,
    pub(crate) mlog_analyze_executor: Arc<TaskExecutor>,
    pub(crate) refresh_task_concurrency_ratio_bits: AtomicU64,
    pub(crate) notifier: Notifier,
    ddl_dirty: AtomicBool,

    pub(crate) mh: Arc<dyn Helper>,

    pub(crate) fetch_interval_millis: AtomicI64,
    pub(crate) basic_interval: i64,
    pub(crate) server_refresh_interval: i64,
    pub(crate) mview_refresh_hist_retention_millis: AtomicI64,
    pub(crate) mlog_purge_hist_retention_millis: AtomicI64,
    pub(crate) next_history_gc_at_millis: AtomicI64,
    history_gc_retry_count: AtomicI64,
    history_gc_running: AtomicBool,
    pub(crate) mv_log_accumulation_scan_running: AtomicBool,
    next_mlog_analyze_scan_millis: AtomicI64,
    mlog_analyze_scan_running: AtomicBool,

    pub(crate) retry_base_delay_millis: AtomicI64,
    pub(crate) retry_max_delay_millis: AtomicI64,
    pub(crate) backpressure_cfg: RwLock<TaskBackpressureConfig>,

    /// Go `metrics`.
    pub metrics: ServiceMetrics,

    pub(crate) mview_refresh_mu: Mutex<TaskQueue<MViewTask>>,
    pub(crate) mview_refresh_alert_mu: Mutex<HashMap<i64, Arc<MViewTask>>>,
    pub(crate) mlog_purge_mu: Mutex<TaskQueue<MlogPurgeTask>>,
    mlog_analyze_tasks: Mutex<HashSet<i64>>,
}

impl MVService {
    pub(crate) fn construct(
        ctx: Context,
        se: Arc<dyn SessionPool>,
        helper: Arc<dyn Helper>,
        replicas: i64,
        executors: (Arc<TaskExecutor>, Arc<TaskExecutor>, Arc<TaskExecutor>),
        basic_interval: i64,
        server_refresh_interval: i64,
    ) -> Self {
        Self {
            sys_session_pool: se,
            last_meta_fetch_millis: AtomicI64::new(0),
            sch: ServerConsistentHash::new(ctx.clone(), replicas, Arc::new(HelperDiscovery(Arc::clone(&helper)))),
            ctx,
            next_refresh_alert_scan_millis: AtomicI64::new(0),
            next_mv_log_accumulation_scan_millis: AtomicI64::new(0),
            refresh_executor: executors.0,
            purge_executor: executors.1,
            mlog_analyze_executor: executors.2,
            refresh_task_concurrency_ratio_bits: AtomicU64::new(0),
            notifier: Notifier::new(),
            ddl_dirty: AtomicBool::new(false),
            mh: helper,
            fetch_interval_millis: AtomicI64::new(0),
            basic_interval,
            server_refresh_interval,
            mview_refresh_hist_retention_millis: AtomicI64::new(0),
            mlog_purge_hist_retention_millis: AtomicI64::new(0),
            next_history_gc_at_millis: AtomicI64::new(0),
            history_gc_retry_count: AtomicI64::new(0),
            history_gc_running: AtomicBool::new(false),
            mv_log_accumulation_scan_running: AtomicBool::new(false),
            next_mlog_analyze_scan_millis: AtomicI64::new(0),
            mlog_analyze_scan_running: AtomicBool::new(false),
            retry_base_delay_millis: AtomicI64::new(0),
            retry_max_delay_millis: AtomicI64::new(0),
            backpressure_cfg: RwLock::new(TaskBackpressureConfig::default()),
            metrics: ServiceMetrics::default(),
            mview_refresh_mu: Mutex::new(TaskQueue::default()),
            mview_refresh_alert_mu: Mutex::new(HashMap::new()),
            mlog_purge_mu: Mutex::new(TaskQueue::default()),
            mlog_analyze_tasks: Mutex::new(HashSet::new()),
        }
    }

    /// Go `MVService.ctx`: the context the service runs until.
    #[must_use]
    pub fn context(&self) -> Context {
        self.ctx.clone()
    }

    /// The service's server ownership view.
    #[must_use]
    pub fn server_consistent_hash(&self) -> &ServerConsistentHash {
        &self.sch
    }

    /// Go `combinedTaskExecutorMetrics`.
    #[must_use]
    pub fn combined_task_executor_metrics(&self) -> TaskExecutorMetricsSnapshot {
        snapshot_task_executor_metrics(&self.refresh_executor)
            .add(snapshot_task_executor_metrics(&self.purge_executor))
            .add(snapshot_task_executor_metrics(&self.mlog_analyze_executor))
    }

    /// The refresh, purge and MV log analyze executors' metrics.
    #[must_use]
    pub fn task_executor_metrics(&self) -> [TaskExecutorMetricsSnapshot; 3] {
        [
            snapshot_task_executor_metrics(&self.refresh_executor),
            snapshot_task_executor_metrics(&self.purge_executor),
            snapshot_task_executor_metrics(&self.mlog_analyze_executor),
        ]
    }

    pub(crate) fn run_task_executors(&self) {
        self.refresh_executor.run();
        self.purge_executor.run();
        self.mlog_analyze_executor.run();
    }

    pub(crate) fn close_task_executors(&self) {
        self.refresh_executor.close();
        self.purge_executor.close();
        self.mlog_analyze_executor.close();
    }

    /// Go `fetchExecTasks`: the due tasks of both queues, marked running.
    pub(crate) fn fetch_exec_tasks(&self, now: &GoTime) -> (Vec<Arc<MlogPurgeTask>>, Vec<Arc<MViewTask>>) {
        let now_ts = unix_milli(now);
        let mut mlogs_to_purge = Vec::new();
        {
            let mut queue = self.mlog_purge_mu.lock().unwrap();
            while let Some((item, l)) = queue.prio.front().map(|(item, l)| (item, Arc::clone(l))) {
                let order_ts = l.order_ts();
                if order_ts == MAX_NEXT_SCHEDULE_TS || order_ts > now_ts {
                    break;
                }
                // set to max to avoid being picked again before reschedule
                l.order_ts.store(MAX_NEXT_SCHEDULE_TS, Ordering::SeqCst);
                queue.prio.update(item, Arc::clone(&l));
                mlogs_to_purge.push(l);
            }
        }
        let mut mviews_to_refresh = Vec::new();
        {
            let mut queue = self.mview_refresh_mu.lock().unwrap();
            while let Some((item, m)) = queue.prio.front().map(|(item, m)| (item, Arc::clone(m))) {
                let order_ts = m.order_ts();
                if order_ts == MAX_NEXT_SCHEDULE_TS || order_ts > now_ts {
                    break;
                }
                // set to max to avoid being picked again before reschedule
                m.order_ts.store(MAX_NEXT_SCHEDULE_TS, Ordering::SeqCst);
                queue.prio.update(item, Arc::clone(&m));
                mviews_to_refresh.push(m);
            }
        }
        (mlogs_to_purge, mviews_to_refresh)
    }

    /// Go `refreshMViews`: submits due refresh tasks.
    pub(crate) fn refresh_mviews(self: &Arc<Self>, mviews_to_refresh: Vec<Arc<MViewTask>>) {
        for task in mviews_to_refresh {
            let service = Arc::clone(self);
            let id = task.id;
            self.refresh_executor
                .submit(format!("mv-refresh/{id}"), Box::new(move || service.execute_refresh_task(&task)));
        }
    }

    /// Go `maybeLogRefreshAlertTasks`: on the maintenance tick, at most once
    /// per scan interval, persists alert states and logs warning/overdue
    /// alerts.
    pub(crate) fn maybe_log_refresh_alert_tasks(&self, now: &GoTime) {
        let now_millis = unix_milli(now);
        if self.next_refresh_alert_scan_millis.load(Ordering::SeqCst) > now_millis {
            return;
        }
        self.next_refresh_alert_scan_millis
            .store(unix_milli(&now.add(MV_REFRESH_ALERT_SCAN_INTERVAL)), Ordering::SeqCst);

        if !self.is_refresh_alert_checker_owner() {
            self.clear_refresh_alert_tasks();
            self.clear_refresh_alert_metrics();
            self.cleanup_stale_refresh_alerts();
            return;
        }
        if !self.is_refresh_alert_metrics_owner() {
            self.clear_refresh_alert_metrics();
        }
        if self.refresh_all_mv_refresh_alert_tasks().is_err() {
            self.cleanup_stale_refresh_alerts();
            return;
        }

        let (alert_states, warning_count, overdue_count) = self.collect_refresh_alert_states(now);
        self.update_refresh_alert_metrics(warning_count, overdue_count);
        if self.sync_refresh_alert_states(now, &alert_states) {
            self.mark_refresh_alert_states_synced(&alert_states);
        }
        self.cleanup_stale_refresh_alerts();

        let (alert_tasks, _, _) = self.collect_refresh_alert_tasks(now);
        if alert_tasks.is_empty() {
            return;
        }
        self.log_mv_refresh_alerts(&alert_tasks);
    }

    fn refresh_all_mv_refresh_alert_tasks(&self) -> Result<(), MvsError> {
        match self.mh.load_all_tidb_mv_refresh(&self.ctx, self.sys_session_pool.as_ref()) {
            Ok(new_pending) => {
                self.build_mview_refresh_alert_tasks(new_pending);
                Ok(())
            }
            Err(err) => {
                let mut fields = self.runtime_log_fields();
                fields.push(Field::new("error", Value::Str(err.to_string())));
                bg_logger().warn("fetch all materialized view refresh alert tasks failed", &fields);
                Err(err)
            }
        }
    }

    fn snapshot_mv_refresh_execution_states(&self) -> HashMap<i64, RefreshAlertExecutionState> {
        let queue = self.mview_refresh_mu.lock().unwrap();
        queue
            .pending
            .iter()
            .filter_map(|(id, item)| {
                let value = queue.prio.get(*item)?;
                Some((
                    *id,
                    RefreshAlertExecutionState {
                        running: value.order_ts() == MAX_NEXT_SCHEDULE_TS,
                        retry_count: value.retry_count(),
                    },
                ))
            })
            .collect()
    }

    /// Go `maybeScanMVLogAccumulationAlerts`.
    pub(crate) fn maybe_scan_mv_log_accumulation_alerts(self: &Arc<Self>, now: &GoTime) {
        if self.next_mv_log_accumulation_scan_millis.load(Ordering::SeqCst) > unix_milli(now) {
            return;
        }
        if self
            .mv_log_accumulation_scan_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let service = Arc::clone(self);
        std::thread::spawn(move || service.run_mv_log_accumulation_alert_scan());
    }

    fn run_mv_log_accumulation_alert_scan(&self) {
        let result = catch_unwind(AssertUnwindSafe(|| self.fetch_all_mv_log_accumulation_alerts()));
        match result {
            Ok(Ok(alerted_count)) => {
                self.metrics.mv_log_accumulation_count.store(alerted_count as i64, Ordering::SeqCst);
            }
            Ok(Err(_)) => {}
            Err(panic) => self.log_panic("MVService mvlog accumulation scan panicked", &panic),
        }
        self.next_mv_log_accumulation_scan_millis
            .store(unix_milli(&mvs_now().add(MV_LOG_ACCUMULATION_ALERT_SCAN_INTERVAL)), Ordering::SeqCst);
        self.mv_log_accumulation_scan_running.store(false, Ordering::SeqCst);
    }

    /// Go `maybeScanMLogAnalyze`.
    pub(crate) fn maybe_scan_mlog_analyze(self: &Arc<Self>, now: &GoTime) {
        if self.next_mlog_analyze_scan_millis.load(Ordering::SeqCst) > unix_milli(now) {
            return;
        }
        if self.mlog_analyze_scan_running.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return;
        }
        self.next_mlog_analyze_scan_millis
            .store(unix_milli(&now.add(MLOG_ANALYZE_SCAN_INTERVAL)), Ordering::SeqCst);
        let service = Arc::clone(self);
        std::thread::spawn(move || service.run_mlog_analyze_scan());
    }

    fn run_mlog_analyze_scan(self: Arc<Self>) {
        let service = Arc::clone(&self);
        let result = catch_unwind(AssertUnwindSafe(move || {
            if let Ok(mlog_ids) = service.fetch_mlog_analyze_tasks() {
                service.analyze_mv_log(mlog_ids);
            }
        }));
        if let Err(panic) = result {
            self.log_panic("MVService mlog analyze scan panicked", &panic);
        }
        self.mlog_analyze_scan_running.store(false, Ordering::SeqCst);
    }

    fn log_panic(&self, message: &str, panic: &Box<dyn std::any::Any + Send>) {
        let text = panic
            .downcast_ref::<&str>()
            .map(|s| (*s).to_owned())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        let mut fields = self.runtime_log_fields();
        fields.push(Field::new("panic", Value::Str(text)));
        bg_logger().error(message, &fields);
    }

    fn collect_refresh_alert_states(&self, now: &GoTime) -> (Vec<RefreshAlertTask>, i64, i64) {
        let exec_states = self.snapshot_mv_refresh_execution_states();
        let pending = self.mview_refresh_alert_mu.lock().unwrap();
        let mut alert_states = Vec::with_capacity(pending.len());
        let (mut warning_count, mut overdue_count) = (0, 0);
        for (&mview_id, task) in pending.iter() {
            let task = task.state.lock().unwrap();
            if task.next_refresh.is_zero() || task.metadata_unresolved {
                continue;
            }
            let alert_level =
                classify_refresh_alert_level(now, &task.last_success_time, task.alert_warning_sec, task.alert_overdue_sec);
            match alert_level {
                MV_REFRESH_ALERT_LEVEL_OVERDUE => overdue_count += 1,
                MV_REFRESH_ALERT_LEVEL_WARNING => warning_count += 1,
                _ => {}
            }
            if task.alert_state_initialized
                && task.last_synced_alert_level == alert_level
                && task.last_synced_alert_read_tso == task.last_success_read_tso
            {
                continue;
            }
            let (task_state, retry_count) = get_refresh_alert_execution_state(&exec_states, mview_id);
            alert_states.push(RefreshAlertTask {
                mview_id,
                schema_name: task.schema_name.clone(),
                mview_name: task.mview_name.clone(),
                next_refresh: task.next_refresh.clone(),
                metadata_unresolved: false,
                last_success_time: task.last_success_time.clone(),
                last_success_read_tso: task.last_success_read_tso,
                alert_warning_sec: task.alert_warning_sec,
                alert_overdue_sec: task.alert_overdue_sec,
                retry_count,
                task_state: task_state.to_owned(),
                overdue: alert_level == MV_REFRESH_ALERT_LEVEL_OVERDUE,
                alert_level: alert_level.to_owned(),
            });
        }
        (alert_states, warning_count, overdue_count)
    }

    fn mark_refresh_alert_states_synced(&self, states: &[RefreshAlertTask]) {
        if states.is_empty() {
            return;
        }
        let pending = self.mview_refresh_alert_mu.lock().unwrap();
        for state in states {
            let Some(task) = pending.get(&state.mview_id) else {
                continue;
            };
            let mut task = task.state.lock().unwrap();
            task.last_synced_alert_level = state.alert_level.clone();
            task.last_synced_alert_read_tso = state.last_success_read_tso;
            task.alert_state_initialized = true;
        }
    }

    pub(crate) fn collect_refresh_alert_tasks(&self, now: &GoTime) -> (Vec<RefreshAlertTask>, i64, i64) {
        let exec_states = self.snapshot_mv_refresh_execution_states();
        let pending = self.mview_refresh_alert_mu.lock().unwrap();
        let mut alert_tasks = Vec::new();
        let (mut warning_count, mut overdue_count) = (0, 0);
        for (&mview_id, task) in pending.iter() {
            let mut task = task.state.lock().unwrap();
            if task.next_refresh.is_zero() || task.metadata_unresolved {
                continue;
            }
            let last_success_time = task.last_success_time.clone();
            let alert_level =
                classify_refresh_alert_level(now, &last_success_time, task.alert_warning_sec, task.alert_overdue_sec);
            if alert_level.is_empty() {
                continue;
            }
            let overdue = alert_level == MV_REFRESH_ALERT_LEVEL_OVERDUE;
            if overdue {
                overdue_count += 1;
            } else {
                warning_count += 1;
            }
            let tso = task.last_success_read_tso;
            if tso > 0 {
                if overdue {
                    if task.last_logged_overdue_tso == tso {
                        continue;
                    }
                    task.last_logged_overdue_tso = tso;
                } else {
                    // If overdue has already been logged for this tso, skip warning downgrade logs.
                    if task.last_logged_warning_tso == tso || task.last_logged_overdue_tso == tso {
                        continue;
                    }
                    task.last_logged_warning_tso = tso;
                }
            }
            let (task_state, retry_count) = get_refresh_alert_execution_state(&exec_states, mview_id);
            alert_tasks.push(RefreshAlertTask {
                mview_id,
                schema_name: task.schema_name.clone(),
                mview_name: task.mview_name.clone(),
                next_refresh: task.next_refresh.clone(),
                metadata_unresolved: false,
                last_success_time,
                last_success_read_tso: task.last_success_read_tso,
                alert_warning_sec: task.alert_warning_sec,
                alert_overdue_sec: task.alert_overdue_sec,
                retry_count,
                task_state: task_state.to_owned(),
                overdue,
                alert_level: alert_level.to_owned(),
            });
        }
        (alert_tasks, warning_count, overdue_count)
    }

    fn sync_refresh_alert_states(&self, updated_at: &GoTime, states: &[RefreshAlertTask]) -> bool {
        if states.is_empty() {
            return true;
        }
        match self.mh.sync_mv_refresh_alert_states(&self.ctx, self.sys_session_pool.as_ref(), updated_at, states) {
            Ok(()) => true,
            Err(MvsError::Canceled | MvsError::DeadlineExceeded) => false,
            Err(err) => {
                let mut fields = self.runtime_log_fields();
                fields.push(Field::new("alert_state_count", Value::I64(states.len() as i64)));
                fields.push(Field::new("updated_at", Value::Str(updated_at.format_date_time())));
                fields.push(Field::new("error", Value::Str(err.to_string())));
                bg_logger().warn("sync mv refresh alert states failed", &fields);
                false
            }
        }
    }

    fn update_refresh_alert_metrics(&self, warning_count: i64, overdue_count: i64) {
        if !self.is_refresh_alert_metrics_owner() {
            self.clear_refresh_alert_metrics();
            return;
        }
        self.metrics.alert_warning_count.store(warning_count, Ordering::SeqCst);
        self.metrics.alert_overdue_count.store(overdue_count, Ordering::SeqCst);
    }

    /// Go `refreshAlertMetricCounts`.
    #[must_use]
    pub fn refresh_alert_metric_counts(&self) -> (i64, i64) {
        if !self.is_refresh_alert_metrics_owner() {
            return (0, 0);
        }
        (
            self.metrics.alert_warning_count.load(Ordering::SeqCst),
            self.metrics.alert_overdue_count.load(Ordering::SeqCst),
        )
    }

    fn clear_refresh_alert_metrics(&self) {
        self.metrics.alert_warning_count.store(0, Ordering::SeqCst);
        self.metrics.alert_overdue_count.store(0, Ordering::SeqCst);
    }

    fn cleanup_stale_refresh_alerts(&self) {
        if !self.is_refresh_alert_cleanup_owner() {
            return;
        }
        match self.mh.cleanup_stale_mv_refresh_alerts(&self.ctx, self.sys_session_pool.as_ref()) {
            Ok(()) | Err(MvsError::Canceled | MvsError::DeadlineExceeded) => {}
            Err(err) => {
                let mut fields = self.runtime_log_fields();
                fields.push(Field::new("error", Value::Str(err.to_string())));
                bg_logger().warn("cleanup stale mv refresh alerts failed", &fields);
            }
        }
    }

    pub(crate) fn is_refresh_alert_checker_owner(&self) -> bool {
        let state = self.sch.mu.read().unwrap();
        let n = state.server_ids.len().min(2);
        state.server_ids[..n].iter().any(|id| *id == state.id)
    }

    pub(crate) fn is_refresh_alert_metrics_owner(&self) -> bool {
        let state = self.sch.mu.read().unwrap();
        state.server_ids.first().is_some_and(|id| *id == state.id)
    }

    fn is_refresh_alert_cleanup_owner(&self) -> bool {
        let state = self.sch.mu.read().unwrap();
        if state.id.is_empty() || state.servers.is_empty() {
            return false;
        }
        state.chash.get_node(MV_REFRESH_ALERT_CLEANUP_OWNER_KEY.as_bytes()) == state.id
    }

    /// Go `logMVRefreshAlerts`.
    fn log_mv_refresh_alerts(&self, alert_tasks: &[RefreshAlertTask]) {
        for task in alert_tasks {
            let mut fields = self.runtime_log_fields();
            fields.extend([
                Field::new("mview_id", Value::I64(task.mview_id)),
                Field::new("schema", Value::Str(task.schema_name.clone())),
                Field::new("mview", Value::Str(task.mview_name.clone())),
                Field::new("next_refresh", Value::Str(task.next_refresh.format_date_time())),
                Field::new("last_success_time", Value::Str(task.last_success_time.format_date_time())),
                Field::new("alert_warning_sec", Value::I64(task.alert_warning_sec)),
                Field::new("alert_overdue_sec", Value::I64(task.alert_overdue_sec)),
                Field::new("failed_retry_count", Value::I64(task.retry_count)),
                Field::new("state", Value::Str(task.task_state.clone())),
            ]);
            if task.overdue {
                bg_logger().error("Materialized_view_refresh_time_overdue", &fields);
            } else {
                bg_logger().error("Materialized_view_refresh_time_warning", &fields);
            }
        }
    }

    /// Go `purgeMLogs`: submits purge jobs.
    pub(crate) fn purge_mlogs(self: &Arc<Self>, mlogs_to_purge: Vec<Arc<MlogPurgeTask>>) {
        for l in mlogs_to_purge {
            let service = Arc::clone(self);
            let id = l.id;
            self.purge_executor.submit(format!("mvlog-purge/{id}"), Box::new(move || service.execute_purge_task(&l)));
        }
    }

    pub(crate) fn execute_refresh_task(&self, m: &Arc<MViewTask>) -> Result<(), String> {
        if !self.has_pending_mview_task(m) {
            return Ok(());
        }
        self.metrics.running_mv_refresh_count.fetch_add(1, Ordering::SeqCst);
        let task_start = mvs_now();
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.mh.refresh_mv(&self.ctx, self.sys_session_pool.as_ref(), m.id)
        }));
        let (next_refresh, err) = match result {
            Ok(Ok(next)) => (next, None),
            Ok(Err(err)) => (GoTime::zero(), Some(err)),
            Err(panic) => {
                self.log_panic("refresh MV task panicked", &panic);
                (GoTime::zero(), Some(MvsError::Other(format!("refresh MV task panicked: {}", panic_text(&panic)))))
            }
        };
        self.observe_task_duration(MV_TASK_DURATION_TYPE_REFRESH, &task_start, err.as_ref());
        self.handle_refresh_task_result(m, &next_refresh, err.clone());
        self.metrics.running_mv_refresh_count.fetch_sub(1, Ordering::SeqCst);
        err.map_or(Ok(()), |err| Err(err.to_string()))
    }

    pub(crate) fn execute_purge_task(&self, l: &Arc<MlogPurgeTask>) -> Result<(), String> {
        if !self.has_pending_mlog_purge_task(l) {
            return Ok(());
        }
        self.metrics.running_mv_log_purge_count.fetch_add(1, Ordering::SeqCst);
        let task_start = mvs_now();
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.mh.purge_mv_log(&self.ctx, self.sys_session_pool.as_ref(), l.id)
        }));
        let (next_purge, err) = match result {
            Ok(Ok(next)) => (next, None),
            Ok(Err(err)) => (GoTime::zero(), Some(err)),
            Err(panic) => {
                self.log_panic("purge MV log task panicked", &panic);
                (GoTime::zero(), Some(MvsError::Other(format!("purge MV log task panicked: {}", panic_text(&panic)))))
            }
        };
        self.observe_task_duration(MV_TASK_DURATION_TYPE_PURGE, &task_start, err.as_ref());
        self.handle_purge_task_result(l, &next_purge, err.clone());
        self.metrics.running_mv_log_purge_count.fetch_sub(1, Ordering::SeqCst);
        err.map_or(Ok(()), |err| Err(err.to_string()))
    }

    /// Go `analyzeMVLog`: submits MV log analyze tasks.
    pub(crate) fn analyze_mv_log(self: &Arc<Self>, mlog_ids: Vec<i64>) {
        for mlog_id in mlog_ids {
            if !self.mark_mlog_analyze_task_submitted(mlog_id) {
                continue;
            }
            let service = Arc::clone(self);
            let submitted = self.mlog_analyze_executor.submit(
                format!("mvlog-analyze/{mlog_id}"),
                Box::new(move || service.execute_mlog_analyze_task(mlog_id)),
            );
            if !submitted {
                self.unmark_mlog_analyze_task_submitted(mlog_id);
            }
        }
    }

    fn execute_mlog_analyze_task(&self, mlog_id: i64) -> Result<(), String> {
        self.metrics.running_mv_log_analyze_count.fetch_add(1, Ordering::SeqCst);
        let task_start = mvs_now();
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.mh.analyze_mv_log(&self.ctx, self.sys_session_pool.as_ref(), mlog_id)
        }));
        let err = match result {
            Ok(Ok(())) => None,
            Ok(Err(err)) => {
                let mut fields = self.runtime_log_fields();
                fields.push(Field::new("mvlog_id", Value::I64(mlog_id)));
                fields.push(Field::new("error", Value::Str(err.to_string())));
                bg_logger().warn("analyze MV log task failed", &fields);
                Some(err)
            }
            Err(panic) => {
                self.log_panic("analyze MV log task panicked", &panic);
                Some(MvsError::Other(format!("analyze MV log task panicked: {}", panic_text(&panic))))
            }
        };
        self.observe_task_duration(MV_TASK_DURATION_TYPE_MLOG_ANALYZE, &task_start, err.as_ref());
        self.unmark_mlog_analyze_task_submitted(mlog_id);
        self.metrics.running_mv_log_analyze_count.fetch_sub(1, Ordering::SeqCst);
        err.map_or(Ok(()), |err| Err(err.to_string()))
    }

    /// Go `hasPendingMViewTask`: whether this exact refresh task is tracked.
    fn has_pending_mview_task(&self, m: &Arc<MViewTask>) -> bool {
        self.mview_refresh_mu.lock().unwrap().item_of(m.id, m).is_some()
    }

    /// Go `hasPendingMLogPurgeTask`: whether this exact purge task is tracked.
    fn has_pending_mlog_purge_task(&self, l: &Arc<MlogPurgeTask>) -> bool {
        self.mlog_purge_mu.lock().unwrap().item_of(l.id, l).is_some()
    }

    fn mark_mlog_analyze_task_submitted(&self, mlog_id: i64) -> bool {
        let mut running = self.mlog_analyze_tasks.lock().unwrap();
        if !running.insert(mlog_id) {
            return false;
        }
        self.metrics.mv_log_analyze_task_count.store(running.len() as i64, Ordering::SeqCst);
        true
    }

    fn unmark_mlog_analyze_task_submitted(&self, mlog_id: i64) {
        let mut running = self.mlog_analyze_tasks.lock().unwrap();
        running.remove(&mlog_id);
        self.metrics.mv_log_analyze_task_count.store(running.len() as i64, Ordering::SeqCst);
    }

    fn observe_task_duration(&self, task_type: &str, task_start: &GoTime, err: Option<&MvsError>) {
        let result = if err.is_some() { MV_DURATION_RESULT_FAILED } else { MV_DURATION_RESULT_SUCCESS };
        self.mh.observe_task_duration(task_type, result, mvs_since(task_start));
    }

    /// Go `runtimeLogFields`.
    pub(crate) fn runtime_log_fields(&self) -> Vec<Field> {
        let load = |value: &AtomicI64| value.load(Ordering::SeqCst);
        let waiting = |exec: &TaskExecutor| load(&exec.metrics.gauges.waiting_count);
        vec![
            Field::new("server_id", Value::Str(self.sch.id())),
            Field::new("mv_count", Value::I64(load(&self.metrics.mv_count))),
            Field::new("mvlog_count", Value::I64(load(&self.metrics.mv_log_count))),
            Field::new("running_refresh_count", Value::I64(load(&self.metrics.running_mv_refresh_count))),
            Field::new("running_purge_count", Value::I64(load(&self.metrics.running_mv_log_purge_count))),
            Field::new("running_mlog_analyze_count", Value::I64(load(&self.metrics.running_mv_log_analyze_count))),
            Field::new(
                "waiting_count",
                Value::I64(
                    waiting(&self.refresh_executor) + waiting(&self.purge_executor) + waiting(&self.mlog_analyze_executor),
                ),
            ),
        ]
    }

    /// Go `handleRefreshTaskResult`.
    fn handle_refresh_task_result(&self, m: &Arc<MViewTask>, next_refresh: &GoTime, err: Option<MvsError>) {
        self.handle_refresh_task_result_inner(m, next_refresh, err);
        self.notifier.wake();
    }

    fn handle_refresh_task_result_inner(&self, m: &Arc<MViewTask>, next_refresh: &GoTime, err: Option<MvsError>) {
        if let Some(err) = err {
            if is_mv_task_canceled_manually(&err) {
                m.retry_count.store(0, Ordering::SeqCst);
                let next_retry_at = mvs_now().add(MANUAL_CANCEL_BACKOFF_DELAY);
                let (applied, applied_next, backoff_err) =
                    self.mh.try_backoff_refresh_manual_cancel(&self.ctx, self.sys_session_pool.as_ref(), m.id, &next_retry_at);
                if let Some(backoff_err) = backoff_err {
                    let mut fields = self.runtime_log_fields();
                    fields.push(Field::new("mview_id", Value::I64(m.id)));
                    fields.push(Field::new("manual_cancel_backoff_at", Value::Str(next_retry_at.format_date_time())));
                    fields.push(Field::new("error", Value::Str(backoff_err.to_string())));
                    bg_logger().warn("refresh MV manual cancel backoff persist failed", &fields);
                }
                if applied {
                    if applied_next.is_zero() {
                        self.remove_mview_task(m);
                        return;
                    }
                    self.reschedule_mview_success(m, &applied_next);
                    return;
                }
                self.reschedule_mview(m, unix_milli(&next_retry_at));
                return;
            }
            let retry_count = m.retry_count.fetch_add(1, Ordering::SeqCst) + 1;
            let retry_delay = self.retry_delay(retry_count);
            let next_retry_at = mvs_now().add(retry_delay);
            self.reschedule_mview(m, unix_milli(&next_retry_at));
            let mut fields = self.runtime_log_fields();
            fields.extend([
                Field::new("mview_id", Value::I64(m.id)),
                Field::new("failed_retry_count", Value::I64(retry_count)),
                Field::new("retry_delay", Value::Duration(retry_delay)),
                Field::new("next_retry_at", Value::Str(next_retry_at.format_date_time())),
                Field::new("error", Value::Str(err.to_string())),
            ]);
            bg_logger().warn("refresh MV task failed, rescheduled for retry", &fields);
            return;
        }
        m.retry_count.store(0, Ordering::SeqCst);
        if next_refresh.is_zero() {
            self.remove_mview_task(m);
            return;
        }
        self.reschedule_mview_success(m, next_refresh);
    }

    /// Go `handlePurgeTaskResult`.
    fn handle_purge_task_result(&self, l: &Arc<MlogPurgeTask>, next_purge: &GoTime, err: Option<MvsError>) {
        self.handle_purge_task_result_inner(l, next_purge, err);
        self.notifier.wake();
    }

    fn handle_purge_task_result_inner(&self, l: &Arc<MlogPurgeTask>, next_purge: &GoTime, err: Option<MvsError>) {
        if let Some(err) = err {
            if is_mv_task_canceled_manually(&err) {
                l.retry_count.store(0, Ordering::SeqCst);
                let next_retry_at = mvs_now().add(MANUAL_CANCEL_BACKOFF_DELAY);
                let (applied, applied_next, backoff_err) =
                    self.mh.try_backoff_purge_manual_cancel(&self.ctx, self.sys_session_pool.as_ref(), l.id, &next_retry_at);
                if let Some(backoff_err) = backoff_err {
                    let mut fields = self.runtime_log_fields();
                    fields.push(Field::new("mvlog_id", Value::I64(l.id)));
                    fields.push(Field::new("manual_cancel_backoff_at", Value::Str(next_retry_at.format_date_time())));
                    fields.push(Field::new("error", Value::Str(backoff_err.to_string())));
                    bg_logger().warn("purge MV log manual cancel backoff persist failed", &fields);
                }
                if applied {
                    if applied_next.is_zero() {
                        self.remove_mlog_purge_task(l);
                        return;
                    }
                    self.reschedule_mlog_purge_success(l, &applied_next);
                    return;
                }
                self.reschedule_mlog_purge(l, unix_milli(&next_retry_at));
                return;
            }
            let retry_count = l.retry_count.fetch_add(1, Ordering::SeqCst) + 1;
            let retry_delay = self.retry_delay(retry_count);
            let next_retry_at = mvs_now().add(retry_delay);
            self.reschedule_mlog_purge(l, unix_milli(&next_retry_at));
            let mut fields = self.runtime_log_fields();
            fields.extend([
                Field::new("mvlog_id", Value::I64(l.id)),
                Field::new("failed_retry_count", Value::I64(retry_count)),
                Field::new("retry_delay", Value::Duration(retry_delay)),
                Field::new("next_retry_at", Value::Str(next_retry_at.format_date_time())),
                Field::new("error", Value::Str(err.to_string())),
            ]);
            bg_logger().warn("purge MV log task failed, rescheduled for retry", &fields);
            return;
        }
        l.retry_count.store(0, Ordering::SeqCst);
        if next_purge.is_zero() {
            self.remove_mlog_purge_task(l);
            return;
        }
        self.reschedule_mlog_purge_success(l, next_purge);
    }

    /// Go `removeMLogPurgeTask`.
    fn remove_mlog_purge_task(&self, l: &Arc<MlogPurgeTask>) {
        let mut queue = self.mlog_purge_mu.lock().unwrap();
        if let Some(item) = queue.item_of(l.id, l) {
            queue.pending.remove(&l.id);
            queue.prio.remove(item);
        }
        self.metrics.mv_log_count.store(queue.pending.len() as i64, Ordering::SeqCst);
    }

    /// Go `removeMViewTask`.
    fn remove_mview_task(&self, m: &Arc<MViewTask>) {
        let mut queue = self.mview_refresh_mu.lock().unwrap();
        if let Some(item) = queue.item_of(m.id, m) {
            queue.pending.remove(&m.id);
            queue.prio.remove(item);
        }
        self.metrics.mv_count.store(queue.pending.len() as i64, Ordering::SeqCst);
    }

    /// Go `rescheduleMView`: reschedules at a unix millisecond.
    fn reschedule_mview(&self, m: &Arc<MViewTask>, next: i64) {
        let mut queue = self.mview_refresh_mu.lock().unwrap();
        if let Some(item) = queue.item_of(m.id, m) {
            m.order_ts.store(next, Ordering::SeqCst);
            queue.prio.fix_item(item);
        }
    }

    /// Go `rescheduleMViewSuccess`.
    fn reschedule_mview_success(&self, m: &Arc<MViewTask>, next_refresh: &GoTime) {
        let order_ts = unix_milli(next_refresh);
        let mut queue = self.mview_refresh_mu.lock().unwrap();
        if let Some(item) = queue.item_of(m.id, m) {
            m.state.lock().unwrap().next_refresh = next_refresh.clone();
            m.order_ts.store(order_ts, Ordering::SeqCst);
            queue.prio.fix_item(item);
        }
    }

    /// Go `rescheduleMLogPurge`: reschedules at a unix millisecond.
    fn reschedule_mlog_purge(&self, l: &Arc<MlogPurgeTask>, next: i64) {
        let mut queue = self.mlog_purge_mu.lock().unwrap();
        if let Some(item) = queue.item_of(l.id, l) {
            l.order_ts.store(next, Ordering::SeqCst);
            queue.prio.fix_item(item);
        }
    }

    /// Go `rescheduleMLogPurgeSuccess`.
    fn reschedule_mlog_purge_success(&self, l: &Arc<MlogPurgeTask>, next_purge: &GoTime) {
        let order_ts = unix_milli(next_purge);
        let mut queue = self.mlog_purge_mu.lock().unwrap();
        if let Some(item) = queue.item_of(l.id, l) {
            *l.next_purge.lock().unwrap() = next_purge.clone();
            l.order_ts.store(order_ts, Ordering::SeqCst);
            queue.prio.fix_item(item);
        }
    }

    /// Go `buildMLogPurgeTasks`: rebuilds purge task states from fetched
    /// metadata. A known task takes the new next purge time; when it is
    /// not running its heap position follows, while a running task
    /// (`orderTs == maxNextScheduleTs`) waits for its completion.
    pub(crate) fn build_mlog_purge_tasks(&self, new_pending: HashMap<i64, Arc<MlogPurgeTask>>) {
        let mut queue = self.mlog_purge_mu.lock().unwrap();
        for (&id, nl) in &new_pending {
            if let Some(&item) = queue.pending.get(&id) {
                let ol = Arc::clone(queue.prio.get(item).expect("a pending purge task is queued"));
                let new_next = nl.next_purge();
                let changed = ol.next_purge() != new_next;
                *ol.next_purge.lock().unwrap() = new_next.clone();
                // not running
                if ol.order_ts() != MAX_NEXT_SCHEDULE_TS && changed {
                    ol.order_ts.store(unix_milli(&new_next), Ordering::SeqCst);
                    queue.prio.fix_item(item);
                }
                continue;
            }
            let item = queue.prio.push(Arc::clone(nl));
            queue.pending.insert(id, item);
        }
        let stale: Vec<(i64, Item)> =
            queue.pending.iter().filter(|(id, _)| !new_pending.contains_key(id)).map(|(id, item)| (*id, *item)).collect();
        for (id, item) in stale {
            queue.pending.remove(&id);
            queue.prio.remove(item);
        }
        self.metrics.mv_log_count.store(queue.pending.len() as i64, Ordering::SeqCst);
    }

    /// Go `buildMViewRefreshTasks`: rebuilds refresh task states.
    pub(crate) fn build_mview_refresh_tasks(&self, new_pending: HashMap<i64, Arc<MViewTask>>) {
        let mut queue = self.mview_refresh_mu.lock().unwrap();
        for (&id, nm) in &new_pending {
            if let Some(&item) = queue.pending.get(&id) {
                let om = Arc::clone(queue.prio.get(item).expect("a pending refresh task is queued"));
                let nm = nm.snapshot();
                let changed;
                {
                    let mut om_state = om.state.lock().unwrap();
                    om_state.metadata_unresolved = nm.metadata_unresolved;
                    if !nm.metadata_unresolved {
                        om_state.schema_name = nm.schema_name.clone();
                        om_state.mview_name = nm.mview_name.clone();
                        om_state.alert_warning_sec = nm.alert_warning_sec;
                        om_state.alert_overdue_sec = nm.alert_overdue_sec;
                    }
                    om_state.last_success_read_tso = nm.last_success_read_tso;
                    om_state.last_success_time = nm.last_success_time.clone();
                    changed = om_state.next_refresh != nm.next_refresh;
                    om_state.next_refresh = nm.next_refresh.clone();
                }
                // not running
                if om.order_ts() != MAX_NEXT_SCHEDULE_TS && changed {
                    om.order_ts.store(unix_milli(&nm.next_refresh), Ordering::SeqCst);
                    queue.prio.fix_item(item);
                }
            } else {
                let item = queue.prio.push(Arc::clone(nm));
                queue.pending.insert(id, item);
            }
        }
        let stale: Vec<(i64, Item)> =
            queue.pending.iter().filter(|(id, _)| !new_pending.contains_key(id)).map(|(id, item)| (*id, *item)).collect();
        for (id, item) in stale {
            queue.pending.remove(&id);
            queue.prio.remove(item);
        }
        self.metrics.mv_count.store(queue.pending.len() as i64, Ordering::SeqCst);
    }

    /// Go `buildMViewRefreshAlertTasks`: rebuilds the global refresh alert
    /// state from fetched metadata.
    pub(crate) fn build_mview_refresh_alert_tasks(&self, new_pending: HashMap<i64, Arc<MViewTask>>) {
        let mut pending = self.mview_refresh_alert_mu.lock().unwrap();
        for (&id, nm) in &new_pending {
            let nm_state = nm.snapshot();
            if nm.order_ts() == 0 && !nm_state.next_refresh.is_zero() {
                nm.order_ts.store(unix_milli(&nm_state.next_refresh), Ordering::SeqCst);
            }
            if let Some(om) = pending.get(&id) {
                {
                    let mut om_state = om.state.lock().unwrap();
                    om_state.metadata_unresolved = nm_state.metadata_unresolved;
                    if !nm_state.metadata_unresolved {
                        om_state.schema_name = nm_state.schema_name.clone();
                        om_state.mview_name = nm_state.mview_name.clone();
                        om_state.alert_warning_sec = nm_state.alert_warning_sec;
                        om_state.alert_overdue_sec = nm_state.alert_overdue_sec;
                    }
                    om_state.last_success_read_tso = nm_state.last_success_read_tso;
                    om_state.last_success_time = nm_state.last_success_time.clone();
                    om_state.next_refresh = nm_state.next_refresh.clone();
                }
                om.order_ts.store(nm.order_ts(), Ordering::SeqCst);
                continue;
            }
            pending.insert(id, Arc::clone(nm));
        }
        pending.retain(|id, _| new_pending.contains_key(id));
    }

    fn clear_refresh_alert_tasks(&self) {
        self.mview_refresh_alert_mu.lock().unwrap().clear();
    }

    /// Go `fetchAllTiDBMVLogPurge`: purge metadata this node owns.
    fn fetch_all_tidb_mv_log_purge(&self) -> Result<HashMap<i64, Arc<MlogPurgeTask>>, MvsError> {
        let start = mvs_now();
        let result = self.mh.load_all_tidb_mv_log_purge(&self.ctx, self.sys_session_pool.as_ref());
        let outcome = match result {
            Ok(mut new_pending) => {
                filter_unowned_tasks(&self.sch, &mut new_pending);
                Ok(new_pending)
            }
            Err(err) => {
                let mut fields = self.runtime_log_fields();
                fields.push(Field::new("error", Value::Str(err.to_string())));
                bg_logger().warn("fetch all mvlog purge tasks failed", &fields);
                Err(err)
            }
        };
        self.observe_fetch(MV_FETCH_TYPE_MLOG_PURGE, &start, outcome.is_ok());
        outcome
    }

    fn observe_fetch(&self, fetch_type: &str, start: &GoTime, ok: bool) {
        let result = if ok { MV_DURATION_RESULT_SUCCESS } else { MV_DURATION_RESULT_FAILED };
        self.mh.observe_task_duration(fetch_type, result, mvs_since(start));
    }

    /// Go `fetchAllMVLogAccumulationAlerts`: how many MV logs this node owns
    /// hold more rows than their alert threshold.
    fn fetch_all_mv_log_accumulation_alerts(&self) -> Result<usize, MvsError> {
        let start = mvs_now();
        let outcome = (|| {
            let candidates = self.fetch_all_tidb_mv_log_accumulation_tasks()?;
            let row_counts = self
                .mh
                .load_tidb_mv_log_accumulation_row_counts(&self.ctx, self.sys_session_pool.as_ref(), &candidates)
                .map_err(|err| {
                    let mut fields = self.runtime_log_fields();
                    fields.push(Field::new("error", Value::Str(err.to_string())));
                    bg_logger().warn("fetch mvlog accumulation row counts failed", &fields);
                    err
                })?;
            Ok(row_counts
                .iter()
                .filter(|(mlog_id, row_count)| candidates.get(mlog_id).is_some_and(|task| **row_count > task.alert_rows))
                .count())
        })();
        self.observe_fetch(MV_FETCH_TYPE_MLOG_ACCUMULATION, &start, outcome.is_ok());
        outcome
    }

    /// Go `fetchAllTiDBMVLogAccumulationTasks`.
    fn fetch_all_tidb_mv_log_accumulation_tasks(
        &self,
    ) -> Result<HashMap<i64, crate::task_handler::MlogAccumulationTask>, MvsError> {
        match self.mh.load_all_tidb_mv_log_accumulation_tasks(&self.ctx, self.sys_session_pool.as_ref()) {
            Ok(mut new_pending) => {
                filter_unowned_tasks(&self.sch, &mut new_pending);
                Ok(new_pending)
            }
            Err(err) => {
                let mut fields = self.runtime_log_fields();
                fields.push(Field::new("error", Value::Str(err.to_string())));
                bg_logger().warn("fetch all mvlog accumulation tasks failed", &fields);
                Err(err)
            }
        }
    }

    /// Go `fetchMLogAnalyzeTasks`: the MV logs this node owns that need
    /// analyzing.
    fn fetch_mlog_analyze_tasks(&self) -> Result<Vec<i64>, MvsError> {
        let start = mvs_now();
        let outcome = match self.mh.load_all_tidb_mv_log_analyze_tasks(&self.ctx, self.sys_session_pool.as_ref()) {
            Ok(mut candidates) => {
                filter_unowned_tasks(&self.sch, &mut candidates);
                let mut mlog_ids = Vec::with_capacity(candidates.len());
                for (mlog_id, task) in candidates {
                    bg_logger().info(
                        "mv service: mlog analyze needed",
                        &[
                            Field::new("mvlog_id", Value::I64(mlog_id)),
                            Field::new("schema", Value::Str(task.schema_name)),
                            Field::new("mlog", Value::Str(task.mlog_name)),
                            Field::new("reason", Value::Str(task.reason)),
                            Field::new("ratio", Value::F64(task.ratio)),
                        ],
                    );
                    mlog_ids.push(mlog_id);
                }
                Ok(mlog_ids)
            }
            Err(err) => {
                let mut fields = self.runtime_log_fields();
                fields.push(Field::new("error", Value::Str(err.to_string())));
                bg_logger().warn("fetch mvlog analyze tasks failed", &fields);
                Err(err)
            }
        };
        self.observe_fetch(MV_FETCH_TYPE_MLOG_ANALYZE, &start, outcome.is_ok());
        outcome
    }

    /// Go `fetchAllTiDBMVRefresh`: refresh metadata this node owns.
    fn fetch_all_tidb_mv_refresh(&self) -> Result<HashMap<i64, Arc<MViewTask>>, MvsError> {
        let start = mvs_now();
        let outcome = match self.mh.load_all_tidb_mv_refresh(&self.ctx, self.sys_session_pool.as_ref()) {
            Ok(mut new_pending) => {
                filter_unowned_tasks(&self.sch, &mut new_pending);
                Ok(new_pending)
            }
            Err(err) => {
                let mut fields = self.runtime_log_fields();
                fields.push(Field::new("error", Value::Str(err.to_string())));
                bg_logger().warn("fetch all materialized view refresh tasks failed", &fields);
                Err(err)
            }
        };
        self.observe_fetch(MV_FETCH_TYPE_MVIEW_REFRESH, &start, outcome.is_ok());
        outcome
    }

    /// Go `fetchAllMVMeta`: refreshes both task queues from the metadata
    /// tables.
    pub(crate) fn fetch_all_mv_meta(&self) -> Result<(), MvsError> {
        let new_mlog_pending = self
            .fetch_all_tidb_mv_log_purge()
            .map_err(|err| MvsError::Other(format!("fetch mvlog purge metadata failed: {err}")))?;
        let new_mview_pending = self
            .fetch_all_tidb_mv_refresh()
            .map_err(|err| MvsError::Other(format!("fetch mview refresh metadata failed: {err}")))?;
        self.build_mlog_purge_tasks(new_mlog_pending);
        self.build_mview_refresh_tasks(new_mview_pending);
        self.last_meta_fetch_millis.store(unix_milli(&mvs_now()), Ordering::SeqCst);
        Ok(())
    }

    /// Go `maybeGCOperationHistory`.
    pub(crate) fn maybe_gc_operation_history(self: &Arc<Self>, now: &GoTime) {
        let history_gc_interval = self.history_gc_interval();
        if self.next_history_gc_at_millis.load(Ordering::SeqCst) > unix_milli(now) {
            return;
        }
        if self.history_gc_running.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return;
        }
        if !self.sch.available_string(MV_HISTORY_GC_OWNER_KEY) {
            self.history_gc_retry_count.store(0, Ordering::SeqCst);
            self.schedule_next_history_gc(now, self.server_refresh_interval);
            self.history_gc_running.store(false, Ordering::SeqCst);
            return;
        }
        let service = Arc::clone(self);
        let now = now.clone();
        std::thread::spawn(move || service.run_gc_operation_history(&now, history_gc_interval));
    }

    fn run_gc_operation_history(&self, now: &GoTime, history_gc_interval: i64) {
        let (mview_refresh_retention, mlog_purge_retention) = self.history_gc_retention_config();
        let start_at = mvs_now();
        let outcome = catch_unwind(AssertUnwindSafe(|| -> bool {
            let current_tso = match self.mh.get_current_tso(&self.ctx, self.sys_session_pool.as_ref()) {
                Ok(tso) => tso,
                Err(err) => {
                    self.schedule_history_gc_failure(now, history_gc_interval);
                    self.mh.observe_run_event(MV_RUN_EVENT_GET_TSO_ERR);
                    let mut fields = self.runtime_log_fields();
                    fields.push(Field::new("error", Value::Str(err.to_string())));
                    bg_logger().warn("get current tso failed when GC MV/MVLOG operation history", &fields);
                    return false;
                }
            };
            if let Err(err) = self.mh.purge_mv_history_before_tso(
                &self.ctx,
                self.sys_session_pool.as_ref(),
                current_tso,
                mview_refresh_retention,
                mlog_purge_retention,
            ) {
                self.schedule_history_gc_failure(now, history_gc_interval);
                let mut fields = self.runtime_log_fields();
                fields.extend([
                    Field::new("current_tso", Value::U64(current_tso)),
                    Field::new("mview_refresh_hist_retention", Value::Duration(mview_refresh_retention)),
                    Field::new("mlog_purge_hist_retention", Value::Duration(mlog_purge_retention)),
                    Field::new("mview_refresh_hist_max_records", Value::U64(DEFAULT_MV_HISTORY_GC_MAX_RECORDS)),
                    Field::new("mlog_purge_hist_max_records", Value::U64(DEFAULT_MV_HISTORY_GC_MAX_RECORDS)),
                    Field::new("error", Value::Str(err.to_string())),
                ]);
                bg_logger().warn("GC MV/MVLOG operation history failed", &fields);
                return false;
            }
            self.schedule_history_gc_success(now, history_gc_interval);
            true
        }));
        let ok = match outcome {
            Ok(ok) => ok,
            Err(panic) => {
                self.schedule_history_gc_failure(now, history_gc_interval);
                self.log_panic("MVService history GC panicked", &panic);
                false
            }
        };
        let result = if ok { MV_DURATION_RESULT_SUCCESS } else { MV_DURATION_RESULT_FAILED };
        self.mh.observe_task_duration(MV_TASK_DURATION_TYPE_HISTORY_GC, result, mvs_since(&start_at));
        self.history_gc_running.store(false, Ordering::SeqCst);
    }

    fn schedule_next_history_gc(&self, now: &GoTime, delay: i64) {
        let delay = if delay <= 0 { DEFAULT_MV_BASIC_INTERVAL } else { delay };
        self.next_history_gc_at_millis.store(unix_milli(&now.add(delay)), Ordering::SeqCst);
    }

    fn schedule_history_gc_success(&self, now: &GoTime, history_gc_interval: i64) {
        self.history_gc_retry_count.store(0, Ordering::SeqCst);
        self.schedule_next_history_gc(now, history_gc_interval);
    }

    fn schedule_history_gc_failure(&self, now: &GoTime, history_gc_interval: i64) {
        if self.ctx.err().is_some() {
            self.history_gc_retry_count.store(0, Ordering::SeqCst);
            self.schedule_next_history_gc(now, history_gc_interval);
            return;
        }
        let retry_count = self.history_gc_retry_count.fetch_add(1, Ordering::SeqCst) + 1;
        if retry_count > HISTORY_GC_RETRY_MAX_ATTEMPTS {
            self.history_gc_retry_count.store(0, Ordering::SeqCst);
            self.schedule_next_history_gc(now, history_gc_interval);
            return;
        }
        let base_delay = if self.basic_interval <= 0 { DEFAULT_MV_BASIC_INTERVAL } else { self.basic_interval };
        let max_delay = MINUTE.min(history_gc_interval).max(base_delay);
        self.schedule_next_history_gc(now, crate::service_settings::calc_retry_delay(retry_count, base_delay, max_delay));
    }

    /// Go `rescheduleHistoryGCEarlier`.
    pub(crate) fn reschedule_history_gc_earlier(&self, now: &GoTime, interval: i64) {
        let interval = if interval <= 0 { DEFAULT_MV_HISTORY_GC_INTERVAL } else { interval };
        let desired = unix_milli(&now.add(interval));
        loop {
            let current = self.next_history_gc_at_millis.load(Ordering::SeqCst);
            if current > 0 && current <= desired {
                return;
            }
            if self
                .next_history_gc_at_millis
                .compare_exchange(current, desired, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Go `NotifyDDLChange`: marks MV metadata dirty and wakes the loop.
    pub fn notify_ddl_change(&self) {
        self.ddl_dirty.store(true, Ordering::SeqCst);
        self.notifier.wake();
    }

    /// Go `Run`: the scheduler loop. It refreshes the server topology,
    /// fetches metadata, dispatches due tasks and reports metrics until the
    /// context ends.
    pub fn run(self: &Arc<Self>) {
        if !self.sch.init() {
            self.mh.observe_run_event(MV_RUN_EVENT_INIT_FAILED);
            return;
        }
        self.run_task_executors();
        let timer = MvsTimer::new(0);
        let maintenance_timer = MvsTimer::new(self.basic_interval);

        let mut last_srv_refresh: Option<GoTime> = None;
        let mut saw_initial_server_refresh = false;
        loop {
            let mut ddl_dirty = false;
            let mut maintenance_tick = false;
            crossbeam_channel::select! {
                recv(timer.c()) -> _ => {}
                recv(maintenance_timer.c()) -> _ => maintenance_tick = true,
                recv(self.notifier.c) -> _ => {
                    self.notifier.clear();
                    ddl_dirty = self.ddl_dirty.swap(false, Ordering::SeqCst);
                }
                recv(self.ctx.done()) -> _ => break,
            }

            let now = mvs_now();
            if maintenance_tick {
                self.mh.report_metrics(self);
                self.maybe_gc_operation_history(&now);
                self.maybe_log_refresh_alert_tasks(&now);
                self.maybe_scan_mv_log_accumulation_alerts(&now);
                self.maybe_scan_mlog_analyze(&now);
                maintenance_timer.reset(self.basic_interval.max(0));
            }

            let mut server_changed = false;
            if last_srv_refresh.as_ref().is_none_or(|last| now.sub(last) >= self.server_refresh_interval) {
                match self.sch.refresh() {
                    Err(err) => {
                        self.mh.observe_run_event(MV_RUN_EVENT_SERVER_REFRESH_ERROR);
                        let mut fields = self.runtime_log_fields();
                        fields.push(Field::new("error", Value::Str(err)));
                        bg_logger().warn("refresh all TiDB server info failed", &fields);
                    }
                    Ok(changed) => {
                        if saw_initial_server_refresh && changed {
                            server_changed = true;
                            self.mh.observe_run_event(MV_RUN_EVENT_SERVER_CHANGED);
                        }
                        saw_initial_server_refresh = true;
                    }
                }
                last_srv_refresh = Some(now.clone());
            }

            let need_fetch = self.should_fetch_mv_meta(&now);
            if ddl_dirty || server_changed || need_fetch {
                if ddl_dirty {
                    self.mh.observe_run_event(MV_RUN_EVENT_FETCH_BY_DDL);
                }
                if need_fetch {
                    self.mh.observe_run_event(MV_RUN_EVENT_FETCH_BY_INTERVAL);
                }
                // Fetch metadata on demand; errors are throttled via lastMetaFetchMillis update below.
                if let Err(err) = self.fetch_all_mv_meta() {
                    let mut fields = self.runtime_log_fields();
                    fields.extend([
                        Field::new("ddl_dirty", Value::Bool(ddl_dirty)),
                        Field::new("server_changed", Value::Bool(server_changed)),
                        Field::new("periodic_fetch", Value::Bool(need_fetch)),
                        Field::new("error", Value::Str(err.to_string())),
                    ]);
                    bg_logger().warn("fetch materialized view metadata failed", &fields);
                    // Keep retries bounded:
                    // - periodic fetch failure keeps existing fetchInterval throttling.
                    // - DDL/topology-triggered failure retries sooner to reduce stale-window.
                    self.mark_fetch_failure(&now, ddl_dirty || server_changed);
                }
            }

            let (mlogs_to_purge, mviews_to_refresh) = self.fetch_exec_tasks(&now);
            self.purge_mlogs(mlogs_to_purge);
            self.refresh_mviews(mviews_to_refresh);

            let next = self.next_schedule_time(&now);
            timer.reset(mvs_until(&next).max(0));
        }
        timer.stop();
        maintenance_timer.stop();
        self.close_task_executors();
        self.mh.report_metrics(self);
    }

    /// Go `markFetchFailure`: a synthetic last fetch time that sets the
    /// next fetch.
    pub(crate) fn mark_fetch_failure(&self, now: &GoTime, urgent: bool) {
        let fetch_interval = self.fetch_interval();
        if !urgent {
            self.last_meta_fetch_millis.store(unix_milli(now), Ordering::SeqCst);
            return;
        }
        let retry_delay = if self.basic_interval <= 0 { DEFAULT_MV_BASIC_INTERVAL } else { self.basic_interval };
        let retry_delay = retry_delay.min(fetch_interval);
        // next fetch time = lastMetaFetchMillis + fetchInterval = now + retryDelay
        self.last_meta_fetch_millis.store(unix_milli(&now.add(retry_delay - fetch_interval)), Ordering::SeqCst);
    }

    /// Go `shouldFetchMVMeta`.
    pub(crate) fn should_fetch_mv_meta(&self, now: &GoTime) -> bool {
        let last = self.last_meta_fetch_millis.load(Ordering::SeqCst);
        if last == 0 {
            return true;
        }
        now.sub(&mvs_unix_milli(last)) >= self.fetch_interval()
    }

    /// Go `nextFetchTime`.
    pub(crate) fn next_fetch_time(&self, now: &GoTime) -> GoTime {
        let last = self.last_meta_fetch_millis.load(Ordering::SeqCst);
        if last == 0 {
            return now.clone();
        }
        let next = mvs_unix_milli(last).add(self.fetch_interval());
        if next.before(now) {
            return now.clone();
        }
        next
    }

    /// Go `nextDueTime`: the earliest due time of both queues.
    pub(crate) fn next_due_time(&self) -> Option<GoTime> {
        let mut next: Option<GoTime> = None;
        if let Some((_, item)) = self.mview_refresh_mu.lock().unwrap().prio.front() {
            next = Some(mvs_unix_milli(item.order_ts()));
        }
        if let Some((_, item)) = self.mlog_purge_mu.lock().unwrap().prio.front() {
            let due = mvs_unix_milli(item.order_ts());
            if next.as_ref().is_none_or(|next| due.before(next)) {
                next = Some(due);
            }
        }
        next
    }

    /// Go `nextScheduleTime`: the loop's next wake-up.
    pub(crate) fn next_schedule_time(&self, now: &GoTime) -> GoTime {
        let mut next = self.next_fetch_time(now);
        if let Some(due) = self.next_due_time() {
            if due.before(&next) {
                next = due;
            }
        }
        if next.before(now) {
            return now.clone();
        }
        next
    }

    /// Go `SetTaskBackpressureController`.
    pub fn set_task_backpressure_controller(&self, controller: Option<Arc<dyn TaskBackpressureController>>) {
        self.refresh_executor.set_backpressure_controller(controller.clone());
        self.purge_executor.set_backpressure_controller(controller.clone());
        self.mlog_analyze_executor.set_backpressure_controller(controller);
    }
}

fn panic_text(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_default()
}

/// Go `getRefreshAlertExecutionState`.
fn get_refresh_alert_execution_state(
    exec_states: &HashMap<i64, RefreshAlertExecutionState>,
    mview_id: i64,
) -> (&'static str, i64) {
    match exec_states.get(&mview_id) {
        Some(state) if state.running => (MV_REFRESH_ALERT_TASK_STATE_RUNNING, state.retry_count),
        Some(state) => (MV_REFRESH_ALERT_TASK_STATE_QUEUED, state.retry_count),
        None => (MV_REFRESH_ALERT_TASK_STATE_UNKNOWN, 0),
    }
}

/// Go `classifyRefreshAlertLevel`.
#[must_use]
pub fn classify_refresh_alert_level(
    now: &GoTime,
    last_success_time: &GoTime,
    alert_warning_sec: i64,
    alert_overdue_sec: i64,
) -> &'static str {
    if last_success_time.is_zero() || now.before(last_success_time) {
        return "";
    }
    let overdue_duration = now.sub(last_success_time);
    if alert_overdue_sec > 0 && overdue_duration >= alert_overdue_sec.saturating_mul(SECOND) {
        return MV_REFRESH_ALERT_LEVEL_OVERDUE;
    }
    if alert_warning_sec > 0 && overdue_duration >= alert_warning_sec.saturating_mul(SECOND) {
        return MV_REFRESH_ALERT_LEVEL_WARNING;
    }
    ""
}

/// Go `filterUnownedTasks`: drops the tasks another server owns, under
/// one hash-ring read lock.
fn filter_unowned_tasks<T>(sch: &ServerConsistentHash, new_pending: &mut HashMap<i64, T>) {
    if new_pending.is_empty() {
        return;
    }
    let state = sch.mu.read().unwrap();
    new_pending.retain(|id, _| state.chash.get_node(&int64_key_to_binary_bytes(*id)) == state.id);
}
