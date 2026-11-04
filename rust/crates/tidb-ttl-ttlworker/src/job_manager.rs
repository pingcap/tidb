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

//! Go `pkg/ttl/ttlworker/job_manager.go`.
//!
//! `// boundary:` Go's `extworkload` option (`WithExternalWorkloadManager`)
//! makes an external TTL task worker campaign its own owner and recycle
//! finished tasks; no external workload manager exists in this workspace, so
//! `IsTTLTaskWorker` is always false and that path has no counterpart, as in
//! a Go build without the option.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use crossbeam_channel::Sender;
use serde::{Deserialize, Serialize};
use tidb_datatype::{Datum, SessionTimeZone, Time};
use tidb_logutil::{bg_logger, Logger};
use tidb_pd_client::EtcdClient;
use tidb_timer::client::new_default_timer_client;
use tidb_timer::go_time::GoTime;
use tidb_timer::store::TimerStore;
use tidb_timeutil::TimeZone;
use tidb_ttl::cache::infoschema::InfoSchemaCache;
use tidb_ttl::cache::table::{KeyLocation, PhysicalTable, RegionCache};
use tidb_ttl::cache::task::{self as cache_task, TaskStatus};
use tidb_ttl::cache::ttlstatus::{
    row_to_table_status, select_from_ttl_table_status_with_id, JobStatus, TableStatus, TableStatusCache,
};
use tidb_ttl::session::{ResultRow, SessionError, TxnMode};
use tidb_ttl_client::command::{
    new_command_client, new_mock_command_client, CmdRequest, CommandClient, TriggerNewTTLJobRequest,
    TriggerNewTTLJobResponse, TriggerNewTTLJobTableResult,
};
use tidb_ttl_client::context::Context;
use tidb_ttl_client::notification::{new_mock_notification_client, new_notification_client, NotificationClient};
use tidb_ttl_metrics::{clear_delay_metrics, update_delay_metrics, DelayMetricsRecord, CANCELLING_JOBS_CNT, RUNNING_JOBS_CNT};

use crate::config::*;
use crate::job::{create_job_history_sql, TtlJob};
use crate::scan::Task;
use crate::session::{eval_expire_time, with_session, SessionPool, SysSessionPool};
use crate::task_manager::{cache_args, get_max_running_tasks_limit, new_task_manager, TaskManager};
use crate::timer::{new_ttl_timer_runtime, within_ttl_job_schedule_window, TtlJobAdapter, TtlJobTrace, TtlTimerRuntime};
use crate::timer_sync::{new_ttl_timer_syncer, TtlTimersSyncer};
use crate::worker::{BaseWorker, WorkerStatus};
use crate::{
    field, go_time_to_mysql, int_field, mysql_time_to_go, session_now, str_arg, uint_field, wrap_execute_sql, Result,
    Session, Store, SysSessionContext,
};

/// Go `scanTaskNotificationType`.
pub const SCAN_TASK_NOTIFICATION_TYPE: &str = "scan";
/// Go `ttlJobManagerLeaderPath`.
pub const TTL_JOB_MANAGER_LEADER_PATH: &str = "/tidb/ttl_job_manager/leader";
/// Go `ttlJobManagerPrompt`.
pub const TTL_JOB_MANAGER_PROMPT: &str = "ttl_job_manager";

/// Go `insertNewTableIntoStatusTemplate`.
pub const INSERT_NEW_TABLE_INTO_STATUS_TEMPLATE: &str =
    "INSERT INTO mysql.tidb_ttl_table_status (table_id,parent_table_id) VALUES (%?, %?)";
/// Go `setTableStatusOwnerTemplate`.
pub const SET_TABLE_STATUS_OWNER_TEMPLATE: &str = "UPDATE mysql.tidb_ttl_table_status
	SET current_job_id = %?,
		current_job_owner_id = %?,
		current_job_start_time = %?,
		current_job_status = 'running',
		current_job_status_update_time = %?,
		current_job_ttl_expire = %?,
		current_job_owner_hb_time = %?
	WHERE table_id = %?";
/// Go `updateHeartBeatTemplate`.
pub const UPDATE_HEART_BEAT_TEMPLATE: &str =
    "UPDATE mysql.tidb_ttl_table_status SET current_job_owner_hb_time = %? WHERE table_id = %? AND current_job_owner_id = %?";
/// Go `taskGCTemplate`.
pub const TASK_GC_TEMPLATE: &str = "DELETE task FROM
		mysql.tidb_ttl_task task
	left join
		mysql.tidb_ttl_table_status job
	ON task.job_id = job.current_job_id
	WHERE job.table_id IS NULL";
/// Go `ttlJobHistoryGCTemplate`.
pub const TTL_JOB_HISTORY_GC_TEMPLATE: &str =
    "DELETE FROM mysql.tidb_ttl_job_history WHERE create_time < CURDATE() - INTERVAL 90 DAY";
/// Go `ttlTableStatusGCWithoutIDTemplate`: a job is only collected when it
/// has no owner and no running job, so the heartbeat needs no check here.
pub const TTL_TABLE_STATUS_GC_WITHOUT_ID_TEMPLATE: &str =
    "DELETE FROM mysql.tidb_ttl_table_status WHERE current_job_status IS NULL";

/// Go `insertNewTableIntoStatusSQL`.
pub fn insert_new_table_into_status_sql(table_id: i64, parent_table_id: i64) -> (&'static str, Vec<Datum>) {
    (
        INSERT_NEW_TABLE_INTO_STATUS_TEMPLATE,
        vec![Datum::new_int(table_id), Datum::new_int(parent_table_id)],
    )
}

/// Go `setTableStatusOwnerSQL`.
pub fn set_table_status_owner_sql(
    uuid: &str,
    table_id: i64,
    job_start: &GoTime,
    now: &GoTime,
    current_job_ttl_expire: &GoTime,
    id: &str,
) -> (&'static str, Vec<Datum>) {
    (
        SET_TABLE_STATUS_OWNER_TEMPLATE,
        vec![
            str_arg(uuid),
            str_arg(id),
            str_arg(&job_start.format_date_time()),
            str_arg(&now.format_date_time()),
            str_arg(&current_job_ttl_expire.format_date_time()),
            str_arg(&now.format_date_time()),
            Datum::new_int(table_id),
        ],
    )
}

/// Go `updateHeartBeatSQL`.
pub fn update_heart_beat_sql(table_id: i64, now: &GoTime, id: &str) -> (&'static str, Vec<Datum>) {
    (
        UPDATE_HEART_BEAT_TEMPLATE,
        vec![str_arg(&now.format_date_time()), Datum::new_int(table_id), str_arg(id)],
    )
}

/// Go `gcTTLTableStatusGCSQL`.
pub fn gc_ttl_table_status_gc_sql(exist_ids: &[i64]) -> String {
    if exist_ids.is_empty() {
        return TTL_TABLE_STATUS_GC_WITHOUT_ID_TEMPLATE.to_owned();
    }
    let ids: Vec<String> = exist_ids.iter().map(|id| id.to_string()).collect();
    format!("{TTL_TABLE_STATUS_GC_WITHOUT_ID_TEMPLATE} AND table_id NOT IN ({})", ids.join(","))
}

/// Go `TTLSummary`: the summary of a TTL job.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtlSummary {
    /// Go `TotalRows`.
    #[serde(default)]
    pub total_rows: u64,
    /// Go `SuccessRows`.
    #[serde(default)]
    pub success_rows: u64,
    /// Go `ErrorRows`.
    #[serde(default)]
    pub error_rows: u64,
    /// Go `TotalScanTask`.
    #[serde(default)]
    pub total_scan_task: i64,
    /// Go `ScheduledScanTask`.
    #[serde(default)]
    pub scheduled_scan_task: i64,
    /// Go `FinishedScanTask`.
    #[serde(default)]
    pub finished_scan_task: i64,
    /// Go `ScanTaskErr`, `omitempty`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub scan_task_err: String,
    /// Go `SummaryText`, `json:"-"`.
    #[serde(skip)]
    pub summary_text: String,
}

/// Go `summarizeTaskResultWithError`.
pub fn summarize_task_result_with_error(tasks: &[Task], err: Option<String>) -> Result<TtlSummary> {
    let mut summary = TtlSummary::default();
    // Go `multierr.Append`, whose message joins the errors with "; ".
    let mut all_err: Vec<String> = err.into_iter().collect();
    for t in tasks {
        if let Some(state) = &t.task.state {
            summary.total_rows += state.total_rows;
            summary.success_rows += state.success_rows;
            summary.error_rows += state.error_rows;
            if !state.scan_task_err.is_empty() {
                all_err.push(state.scan_task_err.clone());
            }
        }

        summary.total_scan_task += 1;
        if t.task.status.0 != TaskStatus::WAITING {
            summary.scheduled_scan_task += 1;
        }
        if t.task.status.0 == TaskStatus::FINISHED {
            summary.finished_scan_task += 1;
        }
    }
    if !all_err.is_empty() {
        summary.scan_task_err = all_err.join("; ");
    }

    summary.summary_text = serde_json::to_string(&summary).map_err(|err| err.to_string())?;
    Ok(summary)
}

/// Go `SubmitTTLManagerJobRequest`.
pub struct SubmitTtlManagerJobRequest {
    /// Go `TableID`: the parent table id.
    pub table_id: i64,
    /// Go `PhysicalID`.
    pub physical_id: i64,
    /// Go `RequestID`.
    pub request_id: String,
    /// Go `RespCh`.
    pub resp_ch: Sender<Result<()>>,
}

/// A table status datetime, resolved as Go's `RowToTableStatus(loc, row)`
/// resolves it; Go's zero time for NULL.
fn status_time(time: &Option<Time>, location: &TimeZone) -> GoTime {
    time.map_or_else(GoTime::zero, |time| mysql_time_to_go(time, location))
}

/// Go's `*time.Location` as `codec.EncodeKey` takes it.
fn session_time_zone(location: &TimeZone) -> SessionTimeZone {
    match location {
        TimeZone::Local => SessionTimeZone::Local,
        TimeZone::Named(zone) => SessionTimeZone::Named(*zone),
        TimeZone::Fixed { name, offset_secs } => SessionTimeZone::Fixed {
            name: name.clone(),
            offset_secs: *offset_secs,
        },
    }
}

/// The store's region cache as `PhysicalTable.SplitScanRanges` reads it.
struct StoreRegionCache<'a>(&'a dyn tidb_kv::region_map::RegionStore);

impl RegionCache for StoreRegionCache<'_> {
    fn locate_key_range(&self, start_key: &[u8], end_key: &[u8]) -> tidb_ttl::cache::Result<Vec<KeyLocation>> {
        self.0
            .locate_key_range(start_key, end_key)
            .map(|regions| {
                regions
                    .into_iter()
                    .map(|(start_key, end_key)| KeyLocation { start_key, end_key })
                    .collect()
            })
            .map_err(tidb_ttl::cache::CacheError)
    }
}

/// Go `JobManager`: schedules and manages the TTL jobs on this instance.
pub struct JobManager {
    base: Arc<BaseWorker<(), ()>>,
    cmd_cli: Arc<dyn CommandClient>,
    notification_cli: Arc<dyn NotificationClient>,
}

/// The fields Go's `JobManager` loop goroutine owns.
pub struct JobManagerState {
    ctx: Context,
    logger: Logger,
    sess_pool: Arc<dyn SessionPool>,
    /// Go `id`: the DDL id of this instance.
    pub id: String,
    store: Store,
    cmd_cli: Arc<dyn CommandClient>,
    notification_cli: Arc<dyn NotificationClient>,
    /// Go `ttltablestore.NewTableTimerStore(1, m.sessPool, "mysql", "tidb_timers", m.etcd)`.
    new_timer_store: Box<dyn Fn() -> TimerStore + Send + Sync>,
    /// Go `infoSchemaCache`.
    pub info_schema_cache: Arc<Mutex<InfoSchemaCache>>,
    /// Go `tableStatusCache`.
    pub table_status_cache: TableStatusCache,
    /// Go `runningJobs`: the jobs this instance runs. A job spawns scan
    /// tasks; those the local scan workers cannot take at once stay here to
    /// be polled later.
    pub running_jobs: Vec<TtlJob>,
    /// Go `taskManager`.
    pub task_manager: TaskManager,
    last_report_delay_metrics_time: Option<Instant>,
    leader_func: Box<dyn Fn() -> bool + Send + Sync>,
}

impl JobManager {
    /// Go `NewJobManager`.
    pub fn new<C: SysSessionContext + ?Sized + 'static>(
        id: &str,
        sess_pool: Arc<dyn tidb_syssession::Pool<C>>,
        store: Store,
        etcd_cli: Option<Arc<EtcdClient>>,
        leader_func: Box<dyn Fn() -> bool + Send + Sync>,
    ) -> Arc<Self> {
        let base = BaseWorker::new(());
        let mut logger = bg_logger().with_fields(&[field("ttl-worker", "job-manager")]);
        if tidb_intest::IN_TEST {
            // in test environment, in the same log there will be multiple ttl managers, so we need to distinguish them
            logger = logger.with_fields(&[field("ttl-worker", id)]);
        }

        let (cmd_cli, notification_cli) = match &etcd_cli {
            Some(etcd_cli) => (new_command_client(Arc::clone(etcd_cli)), new_notification_client(Arc::clone(etcd_cli))),
            None => (new_mock_command_client(), new_mock_notification_client()),
        };

        let ttl_pool: Arc<dyn SessionPool> = Arc::new(SysSessionPool(Arc::clone(&sess_pool)));
        let info_schema_cache = Arc::new(Mutex::new(InfoSchemaCache::new(get_update_info_schema_cache_interval())));
        let task_manager = new_task_manager(
            &base.ctx,
            &logger,
            Arc::clone(&ttl_pool),
            Arc::clone(&info_schema_cache),
            id,
            store.clone(),
        );
        let timer_pool = Arc::clone(&sess_pool);
        let state = JobManagerState {
            ctx: base.ctx.clone(),
            logger,
            sess_pool: ttl_pool,
            id: id.to_owned(),
            store,
            cmd_cli: Arc::clone(&cmd_cli),
            notification_cli: Arc::clone(&notification_cli),
            new_timer_store: Box::new(move || {
                tidb_timer_tablestore::store::new_table_timer_store(
                    1,
                    Arc::clone(&timer_pool),
                    "mysql",
                    "tidb_timers",
                    etcd_cli.clone(),
                )
            }),
            info_schema_cache,
            table_status_cache: TableStatusCache::new(get_update_ttl_table_status_cache_interval()),
            running_jobs: Vec::new(),
            task_manager,
            last_report_delay_metrics_time: None,
            leader_func,
        };
        let state = Mutex::new(Some(state));
        base.set_loop(Box::new(move || {
            let state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
            state.map_or(Ok(()), |mut state| state.job_loop())
        }));
        Arc::new(Self {
            base,
            cmd_cli,
            notification_cli,
        })
    }

    /// Go `Start`.
    pub fn start(&self) {
        self.base.start();
    }

    /// Go `Stop`.
    pub fn stop(&self) {
        self.base.stop();
    }

    /// Go `Status`.
    pub fn status(&self) -> WorkerStatus {
        self.base.status()
    }

    /// Go `WaitStopped`.
    pub fn wait_stopped(&self, ctx: &Context, timeout: Duration) -> std::result::Result<(), tidb_ttl_client::CtxError> {
        self.base.wait_stopped(ctx, timeout)
    }

    /// Go `GetCommandCli`.
    pub fn get_command_cli(&self) -> Arc<dyn CommandClient> {
        Arc::clone(&self.cmd_cli)
    }

    /// Go `GetNotificationCli`.
    pub fn get_notification_cli(&self) -> Arc<dyn NotificationClient> {
        Arc::clone(&self.notification_cli)
    }
}

impl JobManagerState {
    fn cache(&self) -> std::sync::MutexGuard<'_, InfoSchemaCache> {
        self.info_schema_cache.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn exec(&self, se: &Session, sql: &str, args: &[Datum]) -> Result<Vec<crate::Row>> {
        se.execute_sql(sql, args).map_err(|err| wrap_execute_sql(err, sql))
    }

    /// Go `isLeader`.
    fn is_leader(&self) -> bool {
        (self.leader_func)()
    }

    /// Go `jobLoop`.
    fn job_loop(&mut self) -> Result<()> {
        let pool = Arc::clone(&self.sess_pool);
        let result = with_session(pool.as_ref(), &mut |se| self.job_loop_with_session(se));
        self.logger.info("ttlJobManager loop exited.", &[]);
        result
    }

    /// Go `jobLoopWithSession`.
    fn job_loop_with_session(&mut self, se: &Session) -> Result<()> {
        let timer_store = (self.new_timer_store)();
        let (job_request_tx, job_request_rx) = crossbeam_channel::bounded::<SubmitTtlManagerJobRequest>(0);
        let adapter: Arc<dyn TtlJobAdapter> = Arc::new(ManagerJobAdapter {
            store: self.store.clone(),
            sess_pool: Arc::clone(&self.sess_pool),
            request_ch: job_request_tx,
        });
        let timer_rt = new_ttl_timer_runtime(timer_store.clone(), adapter);
        let mut timer_syncer = new_ttl_timer_syncer(
            Arc::clone(&self.sess_pool),
            Arc::new(new_default_timer_client(timer_store.clone())),
        );

        let result = self.job_loop_select(se, &timer_store, &timer_rt, &mut timer_syncer, &job_request_rx);

        timer_rt.pause();
        timer_store.close();
        let resize_scan = self.task_manager.resize_scan_workers(0);
        let resize_del = self.task_manager.resize_del_workers(0);
        self.logger.info("ttlJobManager loop exited.", &[]);
        let errs: Vec<String> = [result.err(), resize_scan.err(), resize_del.err()].into_iter().flatten().collect();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(errs.join("; "))
        }
    }

    fn job_loop_select(
        &mut self,
        se: &Session,
        timer_store: &TimerStore,
        timer_rt: &TtlTimerRuntime,
        timer_syncer: &mut TtlTimersSyncer,
        job_request_rx: &crossbeam_channel::Receiver<SubmitTtlManagerJobRequest>,
    ) -> Result<()> {
        let tick = crossbeam_channel::tick;
        let info_schema_cache_update_ticker = tick(self.cache().get_interval());
        let table_status_cache_update_ticker = tick(self.table_status_cache.get_interval());
        let resize_workers_ticker = tick(get_resize_workers_interval());
        let gc_ticker = tick(get_ttl_gc_interval());

        let schedule_job_ticker = tick(get_check_job_interval());
        let job_check_ticker = tick(get_check_job_interval());
        let update_job_heart_beat_ticker = tick(get_heartbeat_interval());
        let timer_ticker = tick(get_job_manager_loop_sync_timer_interval());

        let schedule_task_ticker = tick(get_task_manager_loop_ticker_interval());
        let update_task_heart_beat_ticker = tick(get_task_manager_heart_beat_interval());
        let task_check_ticker = tick(get_task_manager_loop_check_task_interval());
        let check_scan_task_finished_ticker = tick(get_task_manager_loop_ticker_interval());

        let mut cmd_watcher = self.cmd_cli.watch_command(&self.ctx);
        let mut scan_task_notification_watcher =
            self.notification_cli.watch_notification(&self.ctx, SCAN_TASK_NOTIFICATION_TYPE);
        self.task_manager.resize_workers_with_sys_var();
        let notify_state_rx = self.task_manager.notify_state_ch.1.clone();
        let done = self.ctx.done();
        let mut last_leader_state = false;
        loop {
            self.report_metrics(se);
            self.task_manager.report_metrics();
            let now = session_now(se);

            let current_leader_state = self.is_leader();
            if current_leader_state && !last_leader_state {
                self.do_gc(se, &now);
            }
            last_leader_state = current_leader_state;

            let mut rewatch_cmd = false;
            let mut rewatch_notification = false;
            crossbeam_channel::select! {
                // misc
                recv(done) -> _ => return Ok(()),
                recv(timer_ticker) -> _ => self.on_timer_tick(se, timer_rt, timer_syncer, &now),
                recv(job_request_rx) -> job_req => {
                    if let Ok(job_req) = job_req {
                        self.handle_submit_job_request(se, job_req);
                    }
                }
                recv(info_schema_cache_update_ticker) -> _ => {
                    if let Err(err) = self.update_info_schema_cache(se) {
                        self.logger.warn("fail to update info schema cache", &[field("error", err)]);
                    }
                }
                recv(table_status_cache_update_ticker) -> _ => {
                    if let Err(err) = self.update_table_status_cache(se) {
                        self.logger.warn("fail to update table status cache", &[field("error", err)]);
                    }
                }
                recv(gc_ticker) -> _ => self.do_gc(se, &now),
                // Job Schedule loop:
                recv(update_job_heart_beat_ticker) -> _ => self.update_heart_beat(se, &now),
                recv(job_check_ticker) -> _ => {
                    self.check_finished_job(se);
                    self.check_not_own_job();
                }
                recv(schedule_job_ticker) -> _ => self.reschedule_jobs(se, &now),
                recv(cmd_watcher) -> cmd => match cmd {
                    Ok(cmd) => {
                        if let Some(trigger_job_cmd) = cmd.get_trigger_ttl_job_request() {
                            self.trigger_ttl_job(&cmd, &trigger_job_cmd, se, timer_store);
                        }
                    }
                    Err(_) => {
                        if self.ctx.err().is_some() {
                            return Ok(());
                        }
                        bg_logger().warn("The TTL cmd watcher is closed unexpectedly, re-watch it again", &[]);
                        rewatch_cmd = true;
                    }
                },
                // Task Manager Loop
                recv(schedule_task_ticker) -> _ => self.task_manager.reschedule_tasks(se, &now),
                recv(scan_task_notification_watcher) -> notification => match notification {
                    Ok(()) => self.task_manager.reschedule_tasks(se, &now),
                    Err(_) => {
                        if self.ctx.err().is_some() {
                            return Ok(());
                        }
                        bg_logger().warn(
                            "The TTL scan task notification watcher is closed unexpectedly, re-watch it again",
                            &[],
                        );
                        rewatch_notification = true;
                    }
                },
                recv(task_check_ticker) -> _ => {
                    self.task_manager.check_invalid_task(se);
                    self.task_manager.check_finished_task(se, &now);
                }
                recv(resize_workers_ticker) -> _ => self.task_manager.resize_workers_with_sys_var(),
                recv(update_task_heart_beat_ticker) -> _ => self.task_manager.update_heart_beat(se, &now),
                recv(check_scan_task_finished_ticker) -> _ => {
                    if self.task_manager.handle_scan_finished_task() {
                        self.task_manager.reschedule_tasks(se, &now);
                    }
                }
                recv(notify_state_rx) -> _ => {
                    if self.task_manager.handle_scan_finished_task() {
                        self.task_manager.reschedule_tasks(se, &now);
                    }
                }
            }
            if rewatch_cmd {
                cmd_watcher = self.cmd_cli.watch_command(&self.ctx);
            }
            if rewatch_notification {
                scan_task_notification_watcher =
                    self.notification_cli.watch_notification(&self.ctx, SCAN_TASK_NOTIFICATION_TYPE);
            }
        }
    }

    /// Go `onTimerTick`.
    pub fn on_timer_tick(&self, se: &Session, rt: &TtlTimerRuntime, syncer: &mut TtlTimersSyncer, now: &GoTime) {
        if !self.is_leader() {
            rt.pause();
            syncer.reset();
            return;
        }

        rt.resume();
        let (last_sync_time, last_sync_ver) = syncer.get_last_sync_info();
        let since_last_sync = now.sub(&last_sync_time);
        // in test, we can set the minSyncDuration to 1ms to boost the test speed
        let min_sync_duration = if tidb_intest::IN_TEST { 1_000_000 } else { 5 * tidb_timer::go_time::SECOND };
        if since_last_sync < min_sync_duration {
            // limit timer sync frequency by every 5 seconds
            return;
        }

        let is = se.get_latest_info_schema();
        if is.0.schema_meta_version() > last_sync_ver || since_last_sync > 2 * tidb_timer::go_time::MINUTE {
            // only sync timer when information schema version upgraded, or it has not been synced for more than 2 minutes.
            syncer.sync_timers(&self.ctx, is.0.as_ref());
        }
    }

    /// Go `handleSubmitJobRequest`.
    fn handle_submit_job_request(&mut self, se: &Session, job_req: SubmitTtlManagerJobRequest) {
        if !self.is_leader() {
            let _ = job_req.resp_ch.send(Err("current TTL manager is not the leader".to_owned()));
            return;
        }

        if let Err(err) = self.update_info_schema_cache(se) {
            self.logger.warn("failed to update info schema cache", &[field("error", err)]);
        }

        let Some(tbl) = self.cache().tables.get(&job_req.physical_id).cloned() else {
            let _ = job_req.resp_ch.send(Err(format!(
                "physical id: {} not exists in information schema",
                job_req.physical_id
            )));
            return;
        };

        let parent_id = tbl.table_info.read().id;
        if parent_id != job_req.table_id {
            let _ = job_req.resp_ch.send(Err(format!(
                "table id '{}' != '{}' for physical table with id: {}",
                job_req.table_id, parent_id, job_req.physical_id
            )));
            return;
        }

        let result = self.lock_new_job(se, &tbl, &session_now(se), &job_req.request_id, false).map(drop);
        let _ = job_req.resp_ch.send(result);
    }

    /// Go `triggerTTLJob`.
    fn trigger_ttl_job(&mut self, cmd: &CmdRequest, trigger: &TriggerNewTTLJobRequest, se: &Session, store: &TimerStore) {
        let request_id = cmd.request_id.clone();
        let fields = [
            field("requestID", request_id.clone()),
            field("database", trigger.db_name.clone()),
            field("table", trigger.table_name.clone()),
        ];
        let ok = match self.cmd_cli.take_command(&self.ctx, &request_id) {
            Ok(ok) => ok,
            Err(_) => {
                bg_logger().error("failed to take TTL trigger job command", &fields);
                return;
            }
        };

        if !ok {
            return;
        }

        bg_logger().info("Get a command to trigger a new TTL job", &fields);

        let cmd_cli = Arc::clone(&self.cmd_cli);
        let ctx = self.ctx.clone();
        let response_err = move |err: String| {
            if let Err(err) = cmd_cli.response_command(&ctx, &request_id, Err(err)) {
                bg_logger().warn("encountered error", &[field("error", err)]);
            }
        };

        if !tidb_vardef::ENABLE_TTL_JOB.load(Ordering::SeqCst) {
            response_err("tidb_ttl_job_enable is disabled".to_owned());
            return;
        }

        if !within_ttl_job_schedule_window(&session_now(se)) {
            response_err("not in TTL job window".to_owned());
            return;
        }

        if let Err(err) = self.cache().update(se) {
            response_err(err.to_string());
            return;
        }

        if let Err(err) = self.table_status_cache.update(se) {
            response_err(err.to_string());
            return;
        }

        let db_name = trigger.db_name.to_lowercase();
        let table_name = trigger.table_name.to_lowercase();
        let tables: Vec<PhysicalTable> = self
            .cache()
            .tables
            .values()
            .filter(|tbl| tbl.schema.lowercase() == db_name && tbl.name().lowercase() == table_name)
            .cloned()
            .collect();

        if tables.is_empty() {
            response_err(format!("table {}.{} not exists", trigger.db_name, trigger.table_name));
            return;
        }

        let mut syncer = new_ttl_timer_syncer(
            Arc::clone(&self.sess_pool),
            Arc::new(new_default_timer_client(store.clone())),
        );
        let mut table_results = Vec::with_capacity(tables.len());
        let mut get_job_fns = Vec::with_capacity(tables.len());
        let (ctx, cancel) = Context::with_timeout(&self.ctx, Duration::from_secs(5 * 60));
        for tbl in &tables {
            let mut tbl_result = TriggerNewTTLJobTableResult {
                table_id: tbl.id,
                db_name: trigger.db_name.clone(),
                table_name: trigger.table_name.clone(),
                partition_name: tbl.partition.original().to_owned(),
                ..Default::default()
            };
            match syncer.manual_trigger_ttl_timer(&self.ctx, tbl) {
                Err(err) => {
                    tbl_result.error_message = err;
                    get_job_fns.push(None);
                }
                Ok(f) => get_job_fns.push(Some(f)),
            }
            table_results.push(tbl_result);
        }

        let cmd_cli = Arc::clone(&self.cmd_cli);
        let manager_ctx = self.ctx.clone();
        let request_id = cmd.request_id.clone();
        let database = trigger.db_name.clone();
        let table = trigger.table_name.clone();
        std::thread::Builder::new()
            .name("ttl-trigger-job".to_owned())
            .spawn(move || {
                let ticker = crossbeam_channel::tick(get_check_job_triggered_interval());
                'outer: loop {
                    crossbeam_channel::select! {
                        recv(ctx.done()) -> _ => break 'outer,
                        recv(ticker) -> _ => {}
                    }

                    for (i, slot) in get_job_fns.iter_mut().enumerate() {
                        let Some(f) = slot else {
                            continue;
                        };
                        match f() {
                            Err(err) => {
                                table_results[i].error_message = err;
                                *slot = None;
                            }
                            Ok((job_id, true)) => {
                                table_results[i].job_id = job_id;
                                *slot = None;
                            }
                            Ok((_, false)) => {}
                        }
                    }

                    if get_job_fns.iter().all(Option::is_none) {
                        break;
                    }
                }
                cancel.cancel();

                let mut all_error = true;
                for r in &mut table_results {
                    if r.job_id.is_empty() && r.error_message.is_empty() {
                        r.error_message = "timeout".to_owned();
                    }
                    if r.error_message.is_empty() {
                        all_error = false;
                    }
                }

                if all_error {
                    response_err(table_results[0].error_message.clone());
                    return;
                }

                let response = TriggerNewTTLJobResponse {
                    table_result: Some(table_results.clone()),
                };
                let response_json = serde_json::to_string(&response).unwrap_or_default();
                if let Err(err) = cmd_cli.response_command(&manager_ctx, &request_id, Ok(response_json)) {
                    bg_logger().warn("encountered error", &[field("error", err)]);
                }

                let table_results_json = serde_json::to_vec(&table_results).unwrap_or_default();
                bg_logger().info(
                    "Done to trigger a new TTL job",
                    &[
                        field("requestID", request_id),
                        field("database", database),
                        field("table", table),
                        tidb_log::Field::new("tableResults", tidb_log::Value::ByteString(table_results_json)),
                    ],
                );
            })
            .expect("ttl trigger job thread spawns");
    }

    /// Go `reportMetrics`.
    fn report_metrics(&mut self, se: &Session) {
        let mut running_jobs = 0.0;
        let mut cancelling_jobs = 0.0;
        for job in &self.running_jobs {
            match job.status.0.as_str() {
                JobStatus::RUNNING => running_jobs += 1.0,
                JobStatus::CANCELLING => cancelling_jobs += 1.0,
                _ => {}
            }
        }
        RUNNING_JOBS_CNT.set(running_jobs);
        CANCELLING_JOBS_CNT.set(cancelling_jobs);

        if !self.is_leader() {
            // only the leader can do collect delay metrics to reduce the performance overhead
            clear_delay_metrics();
            return;
        }

        if self
            .last_report_delay_metrics_time
            .is_none_or(|last| last.elapsed() > Duration::from_secs(10 * 60))
        {
            self.last_report_delay_metrics_time = Some(Instant::now());
            self.logger.info("TTL leader to collect delay metrics", &[]);
            match get_delay_metric_records(se, &GoTime::now()) {
                Err(err) => self.logger.info("failed to get TTL delay metrics", &[field("error", err)]),
                Ok(records) => update_delay_metrics(&records),
            }
        }
    }

    /// Go `checkNotOwnJob`: drops the jobs another node took over.
    fn check_not_own_job(&mut self) {
        // reverse iteration so that we could remove the job safely in the loop
        for i in (0..self.running_jobs.len()).rev() {
            let job = self.running_jobs[i].clone();
            let table_status = self.table_status_cache.tables.get(&job.table_id);
            if table_status.is_none_or(|status| status.current_job_owner_id != self.id) {
                let mut fields = vec![field("jobID", job.id.clone())];
                if let Some(status) = table_status {
                    fields.push(field("newJobOwnerID", status.current_job_owner_id.clone()));
                }
                self.logger.info("job has been taken over by another node", &fields);
                self.remove_job(&job);
            }
        }
    }

    /// Go `findAllTasksForJob`.
    fn find_all_tasks_for_job(&self, se: &Session, job_id: &str) -> Result<Vec<Task>> {
        let (sql, args) = cache_task::select_from_ttl_task_with_job_id(job_id);
        let rows = match se.execute_sql(&sql, &cache_args(args)) {
            Ok(rows) => rows,
            Err(err) => {
                self.logger.warn("fail to execute sql", &[field("sql", sql), field("error", err.to_string())]);
                return Err(err.to_string());
            }
        };

        let location = se.get_session_vars().location();
        let mut all_tasks = Vec::with_capacity(rows.len());
        for r in &rows {
            match Task::from_row(&location, r) {
                Ok(task) => all_tasks.push(task),
                Err(err) => {
                    self.logger.warn("fail to read task", &[field("error", err.clone()), field("jobID", job_id)]);
                    return Err(err);
                }
            }
        }
        Ok(all_tasks)
    }

    /// Go `checkFinishedJob`.
    fn check_finished_job(&mut self, se: &Session) {
        // reverse iteration so that we could remove the job safely in the loop
        for i in (0..self.running_jobs.len()).rev() {
            let job = self.running_jobs[i].clone();
            let all_tasks = match self.find_all_tasks_for_job(se, &job.id) {
                Ok(all_tasks) => all_tasks,
                Err(err) => {
                    self.logger.warn(
                        "fail to find all tasks for job. Skip check finished",
                        &[field("jobID", job.id.clone()), field("error", err)],
                    );
                    continue;
                }
            };

            let all_finished = all_tasks.iter().all(|task| task.task.status.0 == TaskStatus::FINISHED);
            if !all_finished {
                continue;
            }

            let logger = self.job_logger(&job);
            logger.info("job has finished", &[]);
            let summary = summarize_task_result_with_error(&all_tasks, None).unwrap_or_else(|err| {
                logger.info("fail to summarize job", &[field("error", err)]);
                TtlSummary::default()
            });
            logger.info(
                "job has finished",
                &[
                    field("summary", summary.summary_text.clone()),
                    uint_field("totalRows", summary.total_rows),
                    uint_field("successRows", summary.success_rows),
                    uint_field("errorRows", summary.error_rows),
                    field("scanTaskError", summary.scan_task_err.clone()),
                ],
            );
            if let Err(err) = job.finish(se, &session_now(se), &summary) {
                logger.warn("fail to finish job", &[field("error", err)]);
                continue;
            }
            self.remove_job(&job);
        }
    }

    /// Go `finish`es `job` as cancelled for `reason`.
    fn cancel_job(&mut self, se: &Session, job: &TtlJob, reason: &str, now: &GoTime) {
        let logger = self.job_logger(job);
        let all_tasks = self.find_all_tasks_for_job(se, &job.id).unwrap_or_else(|err| {
            logger.warn(
                "fail to find all tasks for job. Summarize nothing for cancel job",
                &[field("jobID", job.id.clone()), field("error", err)],
            );
            Vec::new()
        });
        let summary = summarize_task_result_with_error(&all_tasks, Some(reason.to_owned())).unwrap_or_else(|err| {
            logger.warn("fail to summarize job", &[field("error", err)]);
            TtlSummary::default()
        });
        if let Err(err) = job.finish(se, now, &summary) {
            logger.warn("fail to finish job", &[field("error", err)]);
            return;
        }
        self.remove_job(job);
    }

    /// Go `rescheduleJobs`.
    fn reschedule_jobs(&mut self, se: &Session, now: &GoTime) {
        // Try to lock HB timeout jobs, to avoid the case that when the `tidb_ttl_job_enable = 'OFF'`, the HB timeout job will
        // never be cancelled.
        let job_tables = self.ready_for_lock_hb_timeout_job_tables(now);
        // TODO: also consider to resume tables, but it's fine to left them there, as other nodes will take this job
        // when the heart beat is not sent
        for table in job_tables {
            let logger = self.logger.with_fields(&[int_field("tableID", table.table_id)]);
            logger.info("try lock new job", &[]);
            if let Err(err) = self.lock_hb_timeout_job(se, table.table_id, table.parent_table_id, now) {
                logger.warn("failed to lock heartbeat timeout job", &[field("error", err)]);
            }
        }

        let cancel_reason = if !tidb_vardef::ENABLE_TTL_JOB.load(Ordering::SeqCst) {
            Some("tidb_ttl_job_enable turned off")
        } else if !within_ttl_job_schedule_window(now) {
            Some("out of TTL job schedule window")
        } else {
            None
        };

        if let Some(cancel_reason) = cancel_reason {
            // reverse iteration so that we could remove the job safely in the loop
            for i in (0..self.running_jobs.len()).rev() {
                let job = self.running_jobs[i].clone();
                self.job_logger(&job).info(&format!("cancel job because {cancel_reason}"), &[]);
                self.cancel_job(se, &job, cancel_reason, now);
            }
            return;
        }

        // if the table of a running job disappears, also cancel it
        // reverse iteration so that we could remove the job safely in the loop
        for i in (0..self.running_jobs.len()).rev() {
            let job = self.running_jobs[i].clone();
            if self.cache().tables.contains_key(&job.table_id) {
                continue;
            }

            // when the job is locked, it can be found in `infoSchemaCache`. Therefore, it must have been dropped.
            self.job_logger(&job)
                .info("cancel job because the table has been dropped or it's no longer TTL table", &[]);
            self.cancel_job(
                se,
                &job,
                "TTL table has been removed or the TTL on this table has been stopped",
                now,
            );
        }
    }

    /// Go `localJobs`.
    fn local_jobs(&self) -> Vec<TtlJob> {
        self.running_jobs
            .iter()
            .filter(|job| {
                // the others will be removed in `checkNotOwnJob`
                self.table_status_cache
                    .tables
                    .get(&job.table_id)
                    .is_some_and(|status| status.current_job_owner_id == self.id)
            })
            .cloned()
            .collect()
    }

    /// Go `readyForLockHBTimeoutJobTables`: the tables whose job missed its
    /// heartbeat and should be taken over.
    fn ready_for_lock_hb_timeout_job_tables(&self, now: &GoTime) -> Vec<TableStatus> {
        let mut tables = Vec::new();
        for status in self.table_status_cache.tables.values() {
            // If this node already has a job for this table, just ignore.
            // Actually, the logic should ensure this condition never meet, we still add the check here to keep safety
            // (especially when the content of the status table is incorrect)
            if self.running_jobs.iter().any(|job| job.table_id == status.table_id) {
                continue;
            }

            if self.could_lock_job_for_exist_job(Some(status), now) {
                tables.push(status.clone());
            }
        }
        tables
    }

    /// Go `couldLockJobForCreate`.
    fn could_lock_job_for_create(
        &self,
        table_status: Option<&TableStatus>,
        table: Option<&PhysicalTable>,
        now: &GoTime,
        check_schedule_interval: bool,
    ) -> bool {
        let Some(table_status) = table_status else {
            return true;
        };

        if !table_status.current_job_id.is_empty() {
            return false;
        }

        if !check_schedule_interval {
            return true;
        }

        let start_time = status_time(&table_status.last_job_start_time, now.location());
        let Some(table) = table else {
            return false;
        };
        let interval = table
            .table_info
            .read()
            .ttl_info
            .as_ref()
            .map(|ttl| ttl.read().get_job_interval().map_err(|err| err.to_string()))
            .unwrap_or_else(|| Err("the table has no TTL info".to_owned()));
        match interval {
            Err(err) => {
                self.logger.warn(
                    "illegal job interval",
                    &[field("error", err), int_field("tableID", table.id), field("table", table.full_name())],
                );
                false
            }
            Ok(interval) => start_time.add(interval).before(now),
        }
    }

    /// Go `couldLockJobForExistJob`: whether an existing job should be taken
    /// over.
    fn could_lock_job_for_exist_job(&self, table_status: Option<&TableStatus>, now: &GoTime) -> bool {
        let Some(table_status) = table_status.filter(|status| !status.current_job_id.is_empty()) else {
            return false;
        };

        if table_status.current_job_owner_id.is_empty() {
            return true;
        }

        // see whether it's heart beat time is expired
        let hb_time = status_time(&table_status.current_job_owner_hb_time, now.location());
        // jobManagerLoopTickerInterval is used to do heartbeat periodically.
        // Use twice the time to detect the heartbeat timeout.
        let mut hb_timeout = get_heartbeat_interval() * 2;
        let interval = get_update_ttl_table_status_cache_interval() * 2;
        if interval > hb_timeout {
            // tableStatus is get from the cache which may contain stale data.
            // So if cache update interval > heartbeat interval, use the cache update interval instead.
            hb_timeout = interval;
        }
        if hb_time.add(hb_timeout.as_nanos() as i64).before(now) {
            self.logger.info(
                "job heartbeat has stopped",
                &[
                    field("jobID", table_status.current_job_id.clone()),
                    int_field("tableID", table_status.table_id),
                    field("hbTime", hb_time.format_date_time()),
                    field("now", now.format_date_time()),
                ],
            );
            return true;
        }
        false
    }

    /// Go `lockHBTimeoutJob`.
    fn lock_hb_timeout_job(&mut self, se: &Session, table_id: i64, parent_table_id: i64, now: &GoTime) -> Result<TtlJob> {
        let mut job_id = String::new();
        let mut job_start = GoTime::zero();
        let mut expire_time = GoTime::zero();
        se.run_in_txn(
            &mut || {
                let table_status = self
                    .get_table_status_for_update_not_wait(se, table_id, parent_table_id, false)
                    .map_err(SessionError)?;

                let Some(table_status) =
                    table_status.filter(|status| self.could_lock_job_for_exist_job(Some(status), now))
                else {
                    return Err(SessionError(format!("couldn't lock timeout TTL job for table id '{table_id}'")));
                };

                let location = se.get_session_vars().location();
                job_id = table_status.current_job_id.clone();
                job_start = status_time(&table_status.current_job_start_time, &location);
                expire_time = status_time(&table_status.current_job_ttl_expire, &location);
                debug_assert_eq!(location, *now.location());
                let (sql, args) =
                    set_table_status_owner_sql(&table_status.current_job_id, table_id, &job_start, now, &expire_time, &self.id);
                self.exec(se, sql, &args).map(drop).map_err(SessionError)
            },
            TxnMode::PESSIMISTIC,
            None,
        )
        .map_err(|err| err.to_string())?;

        self.append_locked_job(&job_id, se, &job_start, &expire_time, table_id)
    }

    /// Go `lockNewJob`.
    pub fn lock_new_job(
        &mut self,
        se: &Session,
        table: &PhysicalTable,
        now: &GoTime,
        job_id: &str,
        check_schedule_interval: bool,
    ) -> Result<TtlJob> {
        let mut expire_time = GoTime::zero();
        se.run_in_txn(
            &mut || {
                let table_status = self
                    .get_table_status_for_update_not_wait(se, table.id, table.table_info.read().id, true)
                    .map_err(SessionError)?;

                let cached_table = table_status
                    .as_ref()
                    .and_then(|status| self.cache().tables.get(&status.table_id).cloned());
                if !self.could_lock_job_for_create(table_status.as_ref(), cached_table.as_ref(), now, check_schedule_interval)
                {
                    return Err(SessionError("couldn't schedule ttl job".to_owned()));
                }

                expire_time = eval_expire_time(table, se, now).map_err(SessionError)?;

                debug_assert_eq!(se.get_session_vars().location(), *now.location());
                debug_assert_eq!(se.get_session_vars().location(), *expire_time.location());

                let (sql, args) = set_table_status_owner_sql(job_id, table.id, now, now, &expire_time, &self.id);
                self.exec(se, sql, &args).map_err(SessionError)?;

                let (sql, args) = create_job_history_sql(job_id, table, &expire_time, now);
                self.exec(se, sql, &args).map_err(SessionError)?;

                let region_cache = self.store.as_deref().map(StoreRegionCache);
                let ranges = table
                    .split_scan_ranges(
                        region_cache.as_ref().map(|cache| cache as &dyn RegionCache),
                        get_scan_split_cnt(&se.get_store()) as i64,
                    )
                    .map_err(|err| SessionError(format!("split scan ranges: {err}")))?;
                let time_zone = session_time_zone(&se.get_session_vars().location());
                for (scan_id, r) in ranges.iter().enumerate() {
                    let (sql, args) = cache_task::insert_into_ttl_task(
                        &time_zone,
                        job_id,
                        table.id,
                        scan_id as i32,
                        &r.start,
                        &r.end,
                        go_time_to_mysql(&expire_time),
                        go_time_to_mysql(now),
                    )
                    .map_err(|err| SessionError(format!("encode scan task: {err}")))?;
                    self.exec(se, sql, &cache_args(args)).map_err(SessionError)?;
                }

                Ok(())
            },
            TxnMode::PESSIMISTIC,
            None,
        )
        .map_err(|err| err.to_string())?;

        self.append_locked_job(job_id, se, now, &expire_time, table.id)
    }

    /// Go `getTableStatusForUpdateNotWait`.
    fn get_table_status_for_update_not_wait(
        &self,
        se: &Session,
        physical_id: i64,
        parent_table_id: i64,
        create_if_not_exist: bool,
    ) -> Result<Option<TableStatus>> {
        let int_args = |args: Vec<i64>| args.into_iter().map(Datum::new_int).collect::<Vec<_>>();
        let (sql, args) = select_from_ttl_table_status_with_id(physical_id);
        // use ` FOR UPDATE NOWAIT`, then if the new job has been locked by other nodes, it will return:
        // [tikv:3572]Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set.
        // Then this tidb node will not waste resource in calculating the ranges.
        let mut rows = se
            .execute_sql(&format!("{sql} FOR UPDATE NOWAIT"), &int_args(args))
            .map_err(|err| wrap_execute_sql(err, &sql))?;
        if rows.is_empty() && !create_if_not_exist {
            return Ok(None);
        }

        if rows.is_empty() {
            // cannot find the row, insert the status row
            let (insert_sql, args) = insert_new_table_into_status_sql(physical_id, parent_table_id);
            self.exec(se, insert_sql, &args)?;
            let (sql, args) = select_from_ttl_table_status_with_id(physical_id);
            rows = se
                .execute_sql(&format!("{sql} FOR UPDATE NOWAIT"), &int_args(args))
                .map_err(|err| wrap_execute_sql(err, &sql))?;
            if rows.is_empty() {
                return Err("table status row still doesn't exist after insertion".to_owned());
            }
        }

        row_to_table_status(&rows[0]).map(Some).map_err(|err| err.to_string())
    }

    /// Go `appendLockedJob`.
    fn append_locked_job(
        &mut self,
        id: &str,
        se: &Session,
        create_time: &GoTime,
        expire_time: &GoTime,
        table_id: i64,
    ) -> Result<TtlJob> {
        // successfully update the table status, will need to refresh the cache.
        self.update_info_schema_cache(se)?;
        self.update_table_status_cache(se)?;

        let logger = self.logger.with_fields(&[field("jobID", id), int_field("tableID", table_id)]);

        // job is created, notify every scan managers to fetch new tasks
        if let Err(err) = self.notification_cli.notify(&self.ctx, SCAN_TASK_NOTIFICATION_TYPE, id) {
            logger.warn("fail to trigger scan tasks", &[field("error", err)]);
        }

        let job = TtlJob {
            id: id.to_owned(),
            owner_id: self.id.clone(),
            create_time: create_time.clone(),
            assign_time: GoTime::now(),
            ttl_expire_time: expire_time.clone(),
            table_id,
            status: JobStatus(JobStatus::RUNNING.to_owned()),
        };

        self.job_logger(&job).info("append new running job", &[]);
        self.append_job(job.clone());

        Ok(job)
    }

    /// Go `updateHeartBeat`: heartbeats the jobs this instance owns.
    fn update_heart_beat(&mut self, se: &Session, now: &GoTime) {
        for job in self.local_jobs() {
            if let Err(err) = self.update_heart_beat_for_job(se, now, &job) {
                self.job_logger(&job).warn("fail to update heartbeat for job", &[field("error", err)]);
            }
        }
    }

    /// Go `updateHeartBeatForJob`.
    fn update_heart_beat_for_job(&mut self, se: &Session, now: &GoTime, job: &TtlJob) -> Result<()> {
        if job.create_time.add(TTL_JOB_TIMEOUT.as_nanos() as i64).before(now) {
            self.job_logger(job).info("job is timeout", &[]);
            let tasks = self.find_all_tasks_for_job(se, &job.id).unwrap_or_else(|err| {
                self.job_logger(job).warn(
                    "fail to find all tasks for job. Summarize nothing for timeout job",
                    &[field("jobID", job.id.clone()), field("error", err)],
                );
                Vec::new()
            });
            let summary = summarize_task_result_with_error(&tasks, Some("job is timeout".to_owned()))
                .map_err(|err| format!("fail to summarize job: {err}"))?;
            job.finish(se, now, &summary).map_err(|err| format!("fail to finish job: {err}"))?;
            self.remove_job(job);
            return Ok(());
        }

        debug_assert_eq!(se.get_session_vars().location(), *now.location());
        let (sql, args) = update_heart_beat_sql(job.table_id, now, &self.id);
        self.exec(se, sql, &args)?;
        let affected_rows = se.get_session_vars().affected_rows();
        if affected_rows != 1 {
            return Err(format!(
                "fail to update job heartbeat, maybe the owner is not myself ({}), affected rows: {affected_rows}",
                self.id
            ));
        }
        Ok(())
    }

    /// Go `updateInfoSchemaCache`.
    fn update_info_schema_cache(&self, se: &Session) -> Result<()> {
        self.cache().update(se).map_err(|err| err.to_string())
    }

    /// Go `updateTableStatusCache`.
    fn update_table_status_cache(&mut self, se: &Session) -> Result<()> {
        self.table_status_cache.update(se).map_err(|err| err.to_string())
    }

    /// Go `removeJob`.
    fn remove_job(&mut self, finished_job: &TtlJob) {
        if let Some(index) = self.running_jobs.iter().position(|job| job.id == finished_job.id) {
            self.running_jobs.remove(index);
        }
    }

    /// Go `appendJob`.
    fn append_job(&mut self, job: TtlJob) {
        self.running_jobs.push(job);
    }

    /// Go `DoGC`: deletes old job histories and redundant scan tasks.
    pub fn do_gc(&self, se: &Session, _now: &GoTime) {
        if !self.is_leader() {
            // only the leader can do the GC to reduce the performance impact
            return;
        }

        self.logger.info("TTL leader to DoGC", &[]);
        // Remove the table not exist in info schema cache.
        // Delete the table status before deleting the tasks. Therefore the related tasks
        match self.update_info_schema_cache(se) {
            Ok(()) => {
                // only remove table status after updating info schema without error
                let exist_ids: Vec<i64> = self.cache().tables.keys().copied().collect();
                let sql = gc_ttl_table_status_gc_sql(&exist_ids);
                if let Err(err) = se.execute_sql(&sql, &[]) {
                    self.logger.warn("fail to gc ttl table status", &[field("error", err.to_string())]);
                }
            }
            Err(err) => self.logger.warn("failed to update info schema cache", &[field("error", err)]),
        }

        if let Err(err) = se.execute_sql(TASK_GC_TEMPLATE, &[]) {
            self.logger.warn("fail to gc redundant scan task", &[field("error", err.to_string())]);
        }

        if let Err(err) = se.execute_sql(TTL_JOB_HISTORY_GC_TEMPLATE, &[]) {
            self.logger.warn("fail to gc ttl job history", &[field("error", err.to_string())]);
        }
    }

    /// Go `jobLogger`.
    fn job_logger(&self, job: &TtlJob) -> Logger {
        let table_name = self
            .cache()
            .tables
            .get(&job.table_id)
            .map_or_else(|| "unknown".to_owned(), PhysicalTable::full_name);
        self.logger.with_fields(&[
            field("jobID", job.id.clone()),
            int_field("tableID", job.table_id),
            field("tableName", table_name),
        ])
    }
}

/// Go `GetDelayMetricRecords`.
pub fn get_delay_metric_records(se: &Session, now: &GoTime) -> Result<HashMap<i64, DelayMetricsRecord>> {
    let sql = r#"SELECT
    parent_table_id as tid,
    CAST(UNIX_TIMESTAMP(MIN(create_time)) AS SIGNED) as job_ts
FROM
    (
        SELECT
            table_id,
            parent_table_id,
            MAX(create_time) AS create_time
        FROM
            mysql.tidb_ttl_job_history
        WHERE
            create_time > CURDATE() - INTERVAL 7 DAY
            AND status = 'finished'
            AND JSON_VALID(summary_text)
            AND summary_text ->> "$.scan_task_err" IS NULL
        GROUP BY
            table_id,
            parent_table_id
    ) t
GROUP BY
    parent_table_id;"#;

    let rows = se.execute_sql(sql, &[]).map_err(|err| err.to_string())?;

    let unix = |seconds: i64| SystemTime::UNIX_EPOCH + Duration::from_secs(seconds.max(0) as u64);
    let now_system = unix(now.unix());
    let mut records = HashMap::with_capacity(rows.len());
    for row in &rows {
        let mut r = DelayMetricsRecord {
            table_id: row.get_int64(0),
            last_job_time: unix(row.get_int64(1)),
            absolute_delay: Duration::ZERO,
            schedule_relative_delay: Duration::ZERO,
        };
        if let Ok(delay) = now_system.duration_since(r.last_job_time) {
            r.absolute_delay = delay;
        }
        records.insert(r.table_id, r);
    }

    let is = se.get_latest_info_schema();
    let mut no_record_tables = Vec::new();
    for (db_name, tables) in is.0.list_ttl_tables() {
        for tbl_info in tables {
            let info = tbl_info.read();
            let interval = info
                .ttl_info
                .as_ref()
                .map(|ttl| ttl.read().get_job_interval().map_err(|err| err.to_string()))
                .unwrap_or_else(|| Err("the table has no TTL info".to_owned()));
            let interval = match interval {
                Ok(interval) => Duration::from_nanos(interval.max(0) as u64),
                Err(err) => {
                    bg_logger().error(
                        "failed to get table's job interval",
                        &[field("error", err), field("db", db_name.original()), field("table", info.name.original())],
                    );
                    Duration::from_secs(3600)
                }
            };

            let Some(record) = records.get_mut(&info.id) else {
                no_record_tables.push(info.id.to_string());
                continue;
            };

            if record.absolute_delay > interval {
                record.schedule_relative_delay = record.absolute_delay - interval;
            }
        }
    }

    if !no_record_tables.is_empty() {
        let sql = format!(
            "select TIDB_TABLE_ID, CAST(UNIX_TIMESTAMP(CREATE_TIME) AS SIGNED) from information_schema.tables WHERE TIDB_TABLE_ID in ({})",
            no_record_tables.join(", ")
        );
        match se.execute_sql(&sql, &[]) {
            Err(err) => bg_logger().error("failed to exec sql", &[field("error", err.to_string()), field("sql", sql)]),
            Ok(rows) => {
                for row in &rows {
                    let tbl_id = row.get_int64(0);
                    let tbl_create_time = unix(row.get_int64(1));
                    let mut r = DelayMetricsRecord {
                        table_id: tbl_id,
                        last_job_time: SystemTime::UNIX_EPOCH,
                        absolute_delay: Duration::ZERO,
                        schedule_relative_delay: Duration::ZERO,
                    };
                    if let Ok(delay) = now_system.duration_since(tbl_create_time) {
                        r.absolute_delay = delay;
                        r.schedule_relative_delay = delay;
                    }
                    records.insert(tbl_id, r);
                }
            }
        }
    }

    Ok(records)
}

/// Go `managerJobAdapter`.
pub struct ManagerJobAdapter {
    store: Store,
    sess_pool: Arc<dyn SessionPool>,
    request_ch: Sender<SubmitTtlManagerJobRequest>,
}

/// Go `NewManagerJobAdapter`.
pub fn new_manager_job_adapter(
    store: Store,
    sess_pool: Arc<dyn SessionPool>,
    request_ch: Sender<SubmitTtlManagerJobRequest>,
) -> Arc<dyn TtlJobAdapter> {
    Arc::new(ManagerJobAdapter {
        store,
        sess_pool,
        request_ch,
    })
}

impl ManagerJobAdapter {
    /// Go `canSubmitJobWithSession`.
    fn can_submit_job_with_session(&self, table_id: i64, physical_id: i64, se: &Session) -> Result<bool> {
        let is = se.get_latest_info_schema();
        let Some(tbl_info) = is.0.table_by_id(table_id) else {
            return Ok(false);
        };

        let info = tbl_info.read();
        let enabled = info.ttl_info.as_ref().is_some_and(|ttl| ttl.read().enable);
        if !enabled {
            return Ok(false);
        }

        if physical_id != table_id {
            let has_partition = info.partition.as_ref().is_some_and(|partition| {
                partition
                    .read()
                    .definitions
                    .snapshot()
                    .iter()
                    .any(|definition| definition.id == physical_id)
            });
            if !has_partition {
                return Ok(false);
            }
        }
        drop(info);

        let select_tasks_cnt_sql = "select LOW_PRIORITY COUNT(1) FROM mysql.tidb_ttl_task WHERE status IN ('waiting', 'running')";
        let fields = |extra: Vec<tidb_log::Field>| {
            let mut fields = vec![int_field("physicalID", physical_id), int_field("tableID", table_id)];
            fields.extend(extra);
            fields.push(field("SQL", select_tasks_cnt_sql));
            fields
        };
        let rs = match se.execute_sql(select_tasks_cnt_sql, &[]) {
            Ok(rs) => rs,
            Err(err) => {
                bg_logger().error("error to query ttl task count", &fields(vec![field("error", err.to_string())]));
                return Err(err.to_string());
            }
        };
        let Some(row) = rs.first() else {
            bg_logger().error("selectTasksCntSQL returns no row", &fields(Vec::new()));
            return Ok(false);
        };

        let cnt = row.get_int64(0);
        let tasks_limit = get_max_running_tasks_limit(&self.store);
        if cnt >= tasks_limit {
            bg_logger().warn(
                "current TTL tasks count exceeds limit, delay create new job temporarily",
                &[
                    int_field("physicalID", physical_id),
                    int_field("tableID", table_id),
                    int_field("count", cnt),
                    int_field("limit", tasks_limit),
                ],
            );
            return Ok(false);
        }

        Ok(true)
    }

    /// Go `getJobWithSession`.
    fn get_job_with_session(
        &self,
        se: &Session,
        table_id: i64,
        physical_id: i64,
        request_id: &str,
    ) -> Result<Option<TtlJobTrace>> {
        let rows = se
            .execute_sql(
                "select summary_text, status from mysql.tidb_ttl_job_history where table_id=%? AND parent_table_id=%? AND job_id=%?",
                &[Datum::new_int(physical_id), Datum::new_int(table_id), str_arg(request_id)],
            )
            .map_err(|err| err.to_string())?;

        let Some(row) = rows.first() else {
            return Ok(None);
        };

        let mut job_trace = TtlJobTrace {
            request_id: request_id.to_owned(),
            ..TtlJobTrace::default()
        };

        if !row.is_null(0) {
            let summary_bytes = row.get_bytes(0);
            if !summary_bytes.is_empty() {
                let ttl_summary: TtlSummary = serde_json::from_slice(&summary_bytes).map_err(|err| err.to_string())?;
                job_trace.summary = Some(ttl_summary);
            }
        }

        if !row.is_null(1) {
            let status_text = row.get_string(1);
            if matches!(
                status_text.as_str(),
                JobStatus::FINISHED | JobStatus::TIMEOUT | JobStatus::CANCELLED
            ) {
                job_trace.finished = true;
            }
        }

        Ok(Some(job_trace))
    }
}

impl TtlJobAdapter for ManagerJobAdapter {
    fn can_submit_job(&self, table_id: i64, physical_id: i64) -> bool {
        let mut ok = false;
        let result = with_session(self.sess_pool.as_ref(), &mut |se| {
            ok = self.can_submit_job_with_session(table_id, physical_id, se)?;
            Ok(())
        });
        if let Err(err) = result {
            bg_logger().error("fail to check whether can submit job", &[field("error", err)]);
            return false;
        }
        ok
    }

    fn submit_job(
        &self,
        ctx: &Context,
        table_id: i64,
        physical_id: i64,
        request_id: &str,
        _watermark: &GoTime,
    ) -> Result<TtlJobTrace> {
        let (resp_tx, resp_rx) = crossbeam_channel::bounded(1);
        let req = SubmitTtlManagerJobRequest {
            table_id,
            physical_id,
            request_id: request_id.to_owned(),
            resp_ch: resp_tx,
        };
        let ctx_err = || ctx.err().map(|err| err.to_string()).unwrap_or_default();
        crossbeam_channel::select! {
            recv(ctx.done()) -> _ => Err(ctx_err()),
            send(self.request_ch, req) -> sent => {
                if sent.is_err() {
                    // Go's send on a channel nobody reads blocks until the
                    // context ends.
                    let _ = ctx.done().recv();
                    return Err(ctx_err());
                }
                crossbeam_channel::select! {
                    recv(ctx.done()) -> _ => Err(ctx_err()),
                    recv(resp_rx) -> err => match err {
                        Ok(Err(err)) => Err(err),
                        _ => Ok(TtlJobTrace {
                            request_id: request_id.to_owned(),
                            finished: false,
                            summary: None,
                        }),
                    },
                }
            }
        }
    }

    fn get_job(&self, _ctx: &Context, table_id: i64, physical_id: i64, request_id: &str) -> Result<Option<TtlJobTrace>> {
        let mut job = None;
        let result = with_session(self.sess_pool.as_ref(), &mut |se| {
            job = self.get_job_with_session(se, table_id, physical_id, request_id)?;
            Ok(())
        });
        if let Err(err) = result {
            bg_logger().error("fail to get job", &[field("error", err.clone())]);
            return Err(err);
        }
        Ok(job)
    }

    fn now(&self) -> Result<GoTime> {
        let mut now = GoTime::zero();
        with_session(self.sess_pool.as_ref(), &mut |se| {
            let tz = se.global_time_zone().map_err(|err| err.to_string())?;
            now = session_now(se).in_location(&tz);
            Ok(())
        })?;
        Ok(now)
    }
}
