// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/scheduler.go`: the import task's scheduler,
//! which plans each step's subtasks and keeps the job row in step.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_br::utils::register::{RegisterEtcd, RegisterTaskType, TaskRegister};
use tidb_ingestor_ingestctrl::tikv_mode::{new_tikv_mode_switcher, TikvModeSwitcher, DEFAULT_SWITCH_TIKV_MODE_INTERVAL};

use tidb_dxf::*;
use tidb_dxf_scheduler::{BaseScheduler, Scheduler};
use tidb_dxf_storage::{SessionContext, SessionExecutor, TaskHandle};
use tidb_executor::importer::job as importer_job;
use tidb_gocontext::Context;

use crate::planner::{get_step_of_encode, LogicalPlan};
use crate::proto::{CollectConflictsStepMeta, TaskMeta};
use crate::{plain, Error};

/// Go `registerTaskTTL`.
const REGISTER_TASK_TTL: Duration = Duration::from_secs(10 * 60);
/// Go `refreshTaskTTLInterval`.
const REFRESH_TASK_TTL_INTERVAL: Duration = Duration::from_secs(3 * 60);

/// Go `taskInfo`: the task's registration in PD's etcd.
struct TaskInfo {
    task_id: i64,
    last_register_time: Option<Instant>,
    /// Initialized lazily in `register`.
    task_register: Option<TaskRegister>,
}

/// The server's etcd client as Go `clientv3` calls of a task register.
struct RegisterEtcdOps(Arc<dyn tidb_domain_serverinfo::serverinfo_syncer::EtcdOps>);

impl RegisterEtcd for RegisterEtcdOps {
    fn get_key_lease(&self, key: &str) -> Result<Option<i64>, String> {
        self.0.get_key_lease(key)
    }
    fn lease_grant(&self, ttl_seconds: i64) -> Result<i64, String> {
        self.0.lease_grant(ttl_seconds)
    }
    fn put_with_lease(&self, key: &str, value: &[u8], lease: i64) -> Result<(), String> {
        self.0.put_with_lease(key, value, lease)
    }
    fn lease_keep_alive_once(&self, lease: i64) -> Result<(), String> {
        self.0.lease_keep_alive_once(lease)
    }
    fn lease_revoke(&self, lease: i64) -> Result<(), String> {
        self.0.lease_revoke(lease)
    }
}

fn str_field(key: &'static str, value: impl Into<String>) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::Str(value.into()))
}

impl TaskInfo {
    /// Go `taskInfo.register`: refreshes the registration every
    /// `refreshTaskTTLInterval`; a failure is only logged.
    fn register(&mut self, logger: &tidb_logutil::Logger) {
        if self.last_register_time.is_some_and(|t| t.elapsed() < REFRESH_TASK_TTL_INTERVAL) {
            return;
        }
        if self.task_register.is_none() {
            let etcd = crate::import_store().ok().and_then(|s| s.etcd());
            let Some(etcd) = etcd else {
                logger.warn("get etcd client failed", &[]);
                return;
            };
            self.task_register = Some(TaskRegister::new_with_ttl(
                Arc::new(RegisterEtcdOps(etcd)),
                REGISTER_TASK_TTL,
                RegisterTaskType::ImportInto,
                &self.task_id.to_string(),
            ));
        }
        let register = self.task_register.as_mut().expect("the register is set above");
        match register.register_task_once() {
            Ok(()) => logger.info("register task to pd or refresh lease success", &[]),
            Err(e) => logger.warn("register task failed", &[str_field("error", e)]),
        }
        // Go sets it even when the register failed: the TTL is 10min and the
        // refresh interval 3min, so there are 2 more tries before it expires.
        self.last_register_time = Some(Instant::now());
    }

    /// Go `taskInfo.close`.
    fn close(&mut self, logger: &tidb_logutil::Logger) {
        if let Some(mut register) = self.task_register.take() {
            match register.close() {
                Ok(()) => logger.info("unregister task success", &[]),
                Err(e) => logger.warn("unregister task failed", &[str_field("error", e)]),
            }
        }
    }
}

/// Go `importScheduler`.
pub struct ImportScheduler {
    base: BaseScheduler,
    /// Go `GlobalSort` and `sourceStep`.
    state: Mutex<(bool, Step)>,
    /// Go `mu`, held while switching TiKV's mode.
    mu: Mutex<()>,
    /// Go `lastSwitchTime`: TiKV's import mode is global, so one time
    /// serves every task.
    last_switch_time: Mutex<Option<Instant>>,
    /// Go `taskInfoMap`.
    task_infos: Mutex<HashMap<i64, TaskInfo>>,
    /// Go `currTaskID`.
    curr_task_id: AtomicI64,
    /// Go `disableTiKVImportMode`.
    disable_tikv_import_mode: AtomicBool,
}

/// Go `importinto`'s `init`: the scheduler factory.
pub(crate) fn register() {
    tidb_dxf_scheduler::register_scheduler_factory(
        IMPORT_INTO,
        Arc::new(|ctx, task, param| {
            let scheduler = Arc::new(ImportScheduler {
                base: BaseScheduler::new(ctx, task, param),
                state: Mutex::new((false, IMPORT_STEP_IMPORT)),
                mu: Mutex::new(()),
                last_switch_time: Mutex::new(None),
                task_infos: Mutex::new(HashMap::new()),
                curr_task_id: AtomicI64::new(0),
                disable_tikv_import_mode: AtomicBool::new(false),
            });
            let extension: Arc<dyn tidb_dxf_scheduler::Extension> = scheduler.clone();
            scheduler.base.set_extension(Arc::downgrade(&extension));
            scheduler
        }),
    );
}

fn storage_err(e: Error) -> tidb_dxf_storage::StorageError {
    match e.downcast_ref::<tidb_terror::TerrorError>() {
        Some(t) => tidb_dxf_storage::StorageError::Terror(t.clone()),
        None => tidb_dxf_storage::StorageError::Other(e.to_string()),
    }
}

/// Runs `f` on a new session, retried like Go's `handle.RunWithRetry`.
fn with_retry(
    sch: &ImportScheduler,
    ctx: &Context,
    new_txn: bool,
    f: &mut dyn FnMut(&dyn SessionContext) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut backoffer = tidb_dxf_scheduler::new_retry_backoffer();
    let mut last: Option<Error> = None;
    let result = tidb_dxf_handle::run_with_retry(
        ctx,
        tidb_dxf_scheduler::retry_sql_times(),
        &mut backoffer,
        sch.base.get_logger(),
        |ctx| {
            let mut run = |se: &dyn SessionContext| {
                f(se).map_err(|e| {
                    last = Some(e.clone());
                    storage_err(e)
                })
            };
            let result = if new_txn { sch.base.with_new_txn(ctx, &mut run) } else { sch.base.with_new_session(&mut run) };
            (true, result)
        },
    );
    result.map_err(|e| last.take().unwrap_or_else(|| Arc::new(std::io::Error::other(e.to_string()))))
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl ImportScheduler {
    /// Go `isImporting2TiKV`.
    fn is_importing_2_tikv(task: &Task) -> bool {
        task.base.step == IMPORT_STEP_IMPORT || task.base.step == IMPORT_STEP_WRITE_AND_INGEST
    }

    /// Go `updateCurrentTask`: under Raft-KV2 the local backend switches the
    /// mode itself.
    fn update_current_task(&self, task: &Task) {
        if self.curr_task_id.swap(task.base.id, Ordering::SeqCst) != task.base.id {
            if let Ok(meta) = TaskMeta::from_json(&task.meta) {
                self.disable_tikv_import_mode
                    .store(meta.plan.disable_tikv_import_mode || meta.plan.is_raft_kv2, Ordering::SeqCst);
            }
        }
    }

    /// The mode switcher over the store's import client.
    fn tikv_mode_switcher(&self) -> Option<impl TikvModeSwitcher> {
        let client = crate::import_store().ok().and_then(|s| s.import_client().ok());
        if client.is_none() {
            self.base.get_logger().warn("get tikv mode switcher failed", &[]);
        }
        client.map(new_tikv_mode_switcher)
    }

    /// Go `switchTiKVMode`: only the steps that write to TiKV run in import
    /// mode (in import mode the post-process checksum's coprocessor requests
    /// time out), refreshed every `DefaultSwitchTiKVModeInterval`.
    fn switch_tikv_mode(&self, ctx: &Context, task: &Task) {
        self.update_current_task(task);
        if self.disable_tikv_import_mode.load(Ordering::SeqCst) || !Self::is_importing_2_tikv(task) {
            return;
        }
        let due = || lock(&self.last_switch_time).is_none_or(|t| t.elapsed() >= DEFAULT_SWITCH_TIKV_MODE_INTERVAL);
        if !due() {
            return;
        }
        let _guard = lock(&self.mu);
        if !due() {
            return;
        }
        let Some(switcher) = self.tikv_mode_switcher() else { return };
        switcher.to_import_mode(ctx, &[]);
        *lock(&self.last_switch_time) = Some(Instant::now());
    }

    /// Go `switchTiKV2NormalMode`.
    fn switch_tikv_2_normal_mode(&self, ctx: &Context, task: &Task) {
        self.update_current_task(task);
        if self.disable_tikv_import_mode.load(Ordering::SeqCst) {
            return;
        }
        let _guard = lock(&self.mu);
        let Some(switcher) = self.tikv_mode_switcher() else { return };
        switcher.to_normal_mode(ctx, &[]);
        // Clear it, so the next task can switch TiKV mode again.
        *lock(&self.last_switch_time) = None;
    }

    /// Go `registerTask`.
    fn register_task(&self, task: &Task) {
        let mut infos = lock(&self.task_infos);
        let info = infos.entry(task.base.id).or_insert_with(|| TaskInfo {
            task_id: task.base.id,
            last_register_time: None,
            task_register: None,
        });
        info.register(self.base.get_logger());
    }

    /// Go `unregisterTask`.
    fn unregister_task(&self, task: &Task) {
        let info = lock(&self.task_infos).remove(&task.base.id);
        if let Some(mut info) = info {
            info.close(self.base.get_logger());
        }
    }

    fn global_sort(&self) -> bool {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).0
    }

    fn source_step(&self) -> Step {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).1
    }

    /// Go `startJob`.
    fn start_job(&self, ctx: &Context, meta: &TaskMeta, step: &str) -> Result<(), Error> {
        with_retry(self, ctx, false, &mut |se| {
            importer_job::start_job(ctx, se.sql_executor().as_ref(), meta.job_id, step).map_err(crate::driver)
        })
    }

    /// Go `job2Step`.
    fn job_2_step(&self, ctx: &Context, meta: &TaskMeta, step: &str) -> Result<(), Error> {
        with_retry(self, ctx, false, &mut |se| {
            importer_job::job_2_step(ctx, se.sql_executor().as_ref(), meta.job_id, step).map_err(crate::driver)
        })
    }

    /// Go `checkImportTableEmpty` (`ddl.CheckImportIntoTableIsEmpty`).
    fn check_import_table_empty(&self, _ctx: &Context, meta: &TaskMeta) -> Result<(), Error> {
        let table = meta.plan.table_info.as_ref().ok_or_else(|| plain("import plan has no table info"))?;
        let empty = crate::import_store()?.table_is_empty(table).map_err(plain)?;
        if empty {
            return Ok(());
        }
        Err(crate::driver(tidb_driver_error::DriverError::from(
            tidb_exeerrors::ERR_LOAD_DATA_PRE_CHECK_FAILED.fast_gen_by_args(&["target table is not empty".into()]),
        )))
    }

    /// Go `finishJob`: the table's stats meta gets the imported rows, and
    /// the job finishes, in one transaction.
    fn finish_job(&self, ctx: &Context, task: &Task, meta: &TaskMeta) -> Result<(), Error> {
        self.unregister_task(task);
        let table_id = meta.plan.table_info.as_ref().map_or(0, |t| t.id);
        let rows = meta.summary.imported_rows;
        with_retry(self, ctx, true, &mut |se| {
            // Go `statsstorage.UpdateStatsMeta` at the transaction's start
            // TS; a failure is only logged.
            let _ = update_stats_meta(ctx, se, table_id, rows);
            importer_job::finish_job(ctx, se.sql_executor().as_ref(), meta.job_id, Some(&meta.summary))
                .map_err(crate::driver)
        })
    }

    /// Go `failJob`.
    fn fail_job(&self, ctx: &Context, task: &Task, meta: &TaskMeta, error_msg: &str) -> Result<(), Error> {
        self.switch_tikv_2_normal_mode(ctx, task);
        self.unregister_task(task);
        with_retry(self, ctx, false, &mut |se| {
            importer_job::fail_job(ctx, se.sql_executor().as_ref(), meta.job_id, error_msg, Some(&meta.summary))
                .map_err(crate::driver)
        })
    }

    /// Go `cancelJob`.
    fn cancel_job(&self, ctx: &Context, task: &Task, meta: &TaskMeta) -> Result<(), Error> {
        self.switch_tikv_2_normal_mode(ctx, task);
        self.unregister_task(task);
        with_retry(self, ctx, false, &mut |se| {
            importer_job::cancel_job(ctx, se.sql_executor().as_ref(), meta.job_id).map_err(crate::driver)
        })
    }

    /// Go `switchTableMode2NormalMode`: a failure is only logged.
    fn switch_table_mode_to_normal(&self, ctx: &Context, meta: &TaskMeta) {
        if tidb_config_kerneltype::is_next_gen() {
            return;
        }
        let Some(table) = meta.plan.table_info.as_ref() else { return };
        if meta.plan.db_id == 0 || table.id == 0 {
            return;
        }
        let target = tidb_model::AlterTableModeTarget {
            schema_id: meta.plan.db_id,
            table_id: table.id,
            target_mode: tidb_model::TableMode::NORMAL,
            ..Default::default()
        };
        let result = self.base.with_new_txn(ctx, &mut |se| {
            se.sql_server()
                .get_runtime()
                .alter_table_mode(ctx, &target)
                .map_err(tidb_dxf_storage::StorageError::Other)
        });
        if let Err(e) = result {
            self.base.get_logger().warn(
                "alter table mode to normal failure",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(e.to_string()))],
            );
        }
    }
}

/// Go `statsstorage.UpdateStatsMeta` of one unlocked table's delta.
fn update_stats_meta(ctx: &Context, se: &dyn SessionContext, table_id: i64, rows: i64) -> tidb_dxf_storage::Result<()> {
    let start_ts = tidb_dxf_storage::exec_sql(ctx, se, "select @@tidb_current_ts", &[])?
        .first()
        .and_then(|r| r.first())
        .and_then(|d| d.sql_string().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_default();
    tidb_dxf_storage::exec_sql(ctx, se, &format!("select * from mysql.stats_meta where table_id in ({table_id}) for update"), &[])?;
    let sql = format!(
        "insert into mysql.stats_meta (version, table_id, modify_count, count) values ({start_ts}, {table_id}, {rows}, {rows}) \
         on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), \
         count = count + values(count)"
    );
    tidb_dxf_storage::exec_sql(ctx, se, &sql, &[]).map(|_| ())
}
impl Scheduler for ImportScheduler {
    /// Go `Init`.
    fn init(&self) -> Result<(), Error> {
        let task = self.base.get_task();
        let meta = TaskMeta::from_json(&task.meta)
            .map_err(|e| plain(format!("unmarshal task meta failed: {e}")))?;
        *self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
            (!meta.plan.cloud_storage_uri.is_empty(), get_step_of_encode(&meta.plan));
        self.base.init()
    }
    fn schedule_task(&self) {
        self.base.schedule_task();
    }
    /// Go `Close`: a new owner may have adopted the registration lease, so
    /// only the local state is released; the terminal job paths revoke it.
    fn close(&self) {
        lock(&self.task_infos).clear();
        self.base.close();
    }
    fn get_task(&self) -> Arc<Task> {
        self.base.get_task()
    }
}

/// Go `updateTaskSummary`.
fn update_task_summary(
    h: &dyn TaskHandle,
    task: &mut Task,
    meta: &mut TaskMeta,
    next_step: Step,
    p: &LogicalPlan,
) -> Result<(), Error> {
    match next_step {
        IMPORT_STEP_ENCODE_AND_SORT | IMPORT_STEP_IMPORT | IMPORT_STEP_QUERY => meta.summary.encode_summary = p.summary.get(),
        IMPORT_STEP_MERGE_SORT => meta.summary.merge_summary = p.summary.get(),
        IMPORT_STEP_WRITE_AND_INGEST => meta.summary.ingest_summary = p.summary.get(),
        IMPORT_STEP_COLLECT_CONFLICTS => meta.summary.collect_conflicts_summary = p.summary.get(),
        IMPORT_STEP_CONFLICT_RESOLUTION => meta.summary.resolve_conflicts_summary = p.summary.get(),
        IMPORT_STEP_POST_PROCESS => {
            let summaries = h
                .get_previous_subtask_summary(task.base.id, get_step_of_encode(&meta.plan))
                .map_err(|e| Arc::new(e) as Error)?;
            for s in &summaries {
                meta.summary.imported_rows += s.row_cnt.load(std::sync::atomic::Ordering::SeqCst);
            }
            if meta.plan.is_global_sort() {
                let metas = h
                    .get_previous_subtask_metas(task.base.id, IMPORT_STEP_COLLECT_CONFLICTS)
                    .map_err(|e| Arc::new(e) as Error)?;
                let mut conflicted = 0u64;
                for bs in metas {
                    let m: CollectConflictsStepMeta = crate::proto::unmarshal(&bs)?;
                    if m.too_many_conflicts_from_index {
                        meta.summary.too_many_conflicts = true;
                        continue;
                    }
                    conflicted += m.conflicted_row_count as u64;
                }
                meta.summary.imported_rows -= conflicted as i64;
                meta.summary.conflict_row_cnt = conflicted;
            }
        }
        _ => {}
    }
    task.meta = meta.to_json();
    Ok(())
}

/// Go `GenerateExecID` of a server info JSON: `ip:port`.
fn exec_id_of(info: &serde_json::Value) -> String {
    let ip = info.get("ip").and_then(serde_json::Value::as_str).unwrap_or_default();
    let port = info.get("listening_port").and_then(serde_json::Value::as_u64).unwrap_or_default();
    if ip.contains(':') {
        format!("[{ip}]:{port}")
    } else {
        format!("{ip}:{port}")
    }
}
impl tidb_dxf_scheduler::Extension for ImportScheduler {
    /// Go `OnTick`: only a running task switches TiKV's mode and registers
    /// itself.
    fn on_tick(&self, ctx: &Context, task: &Task) {
        if task.base.state != TASK_STATE_RUNNING {
            return;
        }
        self.switch_tikv_mode(ctx, task);
        self.register_task(task);
    }

    /// Go `OnNextSubtasksBatch`.
    fn on_next_subtasks_batch(
        &self,
        ctx: &Context,
        h: &dyn TaskHandle,
        task: &mut Task,
        exec_ids: &[String],
        next_step: Step,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let node_cnt = if tidb_config_kerneltype::is_next_gen() { task.base.max_node_count } else { exec_ids.len() as isize };
        let mut meta = TaskMeta::from_json(&task.meta)?;
        if !tidb_config_kerneltype::is_next_gen() && task.base.step == STEP_INIT {
            self.check_import_table_empty(ctx, &meta)?;
        }
        let source_step = self.source_step();
        let global_sort = self.global_sort();
        let mut previous = std::collections::HashMap::new();
        match next_step {
            IMPORT_STEP_IMPORT | IMPORT_STEP_ENCODE_AND_SORT | IMPORT_STEP_QUERY => {
                let step = if global_sort { importer_job::JOB_STEP_GLOBAL_SORTING } else { importer_job::JOB_STEP_IMPORTING };
                self.start_job(ctx, &meta, step)?;
            }
            IMPORT_STEP_POST_PROCESS => {
                self.switch_tikv_2_normal_mode(ctx, task);
                self.job_2_step(ctx, &meta, importer_job::JOB_STEP_VALIDATING)?;
                let metas = h.get_previous_subtask_metas(task.base.id, source_step).map_err(|e| Arc::new(e) as Error)?;
                let conflicts = h
                    .get_previous_subtask_metas(task.base.id, IMPORT_STEP_COLLECT_CONFLICTS)
                    .map_err(|e| Arc::new(e) as Error)?;
                previous.insert(source_step, metas);
                previous.insert(IMPORT_STEP_COLLECT_CONFLICTS, conflicts);
            }
            STEP_DONE => return Ok(Vec::new()),
            step => return Err(plain(format!("unknown step {}", step.0))),
        }
        let plan_ctx = tidb_dxf_planner::PlanCtx {
            ctx: ctx.clone(),
            task_id: task.base.id,
            previous_subtask_metas: previous,
            global_sort,
            next_task_step: next_step,
            execute_nodes_cnt: node_cnt,
            thread_cnt: task.base.required_slots,
            ..Default::default()
        };
        let mut logical = LogicalPlan::default();
        tidb_dxf_planner::LogicalPlan::from_task_meta(&mut logical, &task.meta).map_err(crate::planner::unbox)?;
        let physical =
            tidb_dxf_planner::LogicalPlan::to_physical_plan(&logical, &plan_ctx).map_err(crate::planner::unbox)?;
        let metas = physical.to_subtask_metas(&plan_ctx, next_step).map_err(crate::planner::unbox)?;
        update_task_summary(h, task, &mut meta, next_step, &logical)?;
        Ok(metas)
    }

    /// Go `OnDone`.
    fn on_done(&self, ctx: &Context, _: &dyn TaskHandle, task: &mut Task) -> Result<(), Error> {
        let meta = TaskMeta::from_json(&task.meta)?;
        self.switch_table_mode_to_normal(ctx, &meta);
        if task.base.state == TASK_STATE_REVERTING {
            if let Some(error) = &task.error {
                if tidb_dxf_storage::is_cancelled_err(Some(error.as_ref())) {
                    return self.cancel_job(ctx, task, &meta);
                }
                return self.fail_job(ctx, task, &meta, &error.to_string());
            }
            return self.fail_job(ctx, task, &meta, "");
        }
        self.finish_job(ctx, task, &meta)
    }

    /// Go `GetEligibleInstances`.
    fn get_eligible_instances(&self, _: &Context, task: &Task) -> Result<Vec<String>, Error> {
        let meta = TaskMeta::from_json(&task.meta)?;
        Ok(meta.eligible_instances.unwrap_or_default().iter().map(exec_id_of).collect())
    }

    /// Go `IsRetryableErr`.
    fn is_retryable_err(&self, err: &(dyn std::error::Error + 'static)) -> bool {
        tidb_lightning_common::is_retryable_error(err)
    }

    /// Go `GetNextStep`.
    fn get_next_step(&self, task: &TaskBase) -> Step {
        match task.step {
            STEP_INIT | STEP_PREPARED => self.source_step(),
            IMPORT_STEP_ENCODE_AND_SORT | IMPORT_STEP_QUERY => IMPORT_STEP_MERGE_SORT,
            IMPORT_STEP_MERGE_SORT => IMPORT_STEP_WRITE_AND_INGEST,
            IMPORT_STEP_WRITE_AND_INGEST => IMPORT_STEP_COLLECT_CONFLICTS,
            IMPORT_STEP_COLLECT_CONFLICTS => IMPORT_STEP_CONFLICT_RESOLUTION,
            IMPORT_STEP_IMPORT | IMPORT_STEP_CONFLICT_RESOLUTION => IMPORT_STEP_POST_PROCESS,
            _ => STEP_DONE,
        }
    }

    /// Go `OnPrepare`: only a next-gen global-sort import prepares
    /// asynchronously; this port does not submit one.
    fn on_prepare(&self, _: &Context, _: &dyn TaskHandle, _: &mut Task) -> Result<(), Error> {
        Ok(())
    }

    /// Go `ModifyMeta`.
    fn modify_meta(&self, old: &[u8], _: &[Modification]) -> Result<Vec<u8>, Error> {
        Ok(old.to_vec())
    }
}
