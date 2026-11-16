// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/scheduler.go`: the import task's scheduler,
//! which plans each step's subtasks and keeps the job row in step.

use std::sync::{Arc, Mutex};

use tidb_dxf::*;
use tidb_dxf_scheduler::{BaseScheduler, Scheduler};
use tidb_dxf_storage::{SessionContext, SessionExecutor, TaskHandle};
use tidb_executor::importer::job as importer_job;
use tidb_gocontext::Context;

use crate::planner::{get_step_of_encode, LogicalPlan};
use crate::proto::{CollectConflictsStepMeta, TaskMeta};
use crate::{plain, Error};

/// Go `importScheduler`.
pub struct ImportScheduler {
    base: BaseScheduler,
    /// Go `GlobalSort` and `sourceStep`.
    state: Mutex<(bool, Step)>,
}

/// Go `importinto`'s `init`: the scheduler factory.
pub(crate) fn register() {
    tidb_dxf_scheduler::register_scheduler_factory(
        IMPORT_INTO,
        Arc::new(|ctx, task, param| {
            let scheduler = Arc::new(ImportScheduler {
                base: BaseScheduler::new(ctx, task, param),
                state: Mutex::new((false, IMPORT_STEP_IMPORT)),
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

impl ImportScheduler {
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
    fn finish_job(&self, ctx: &Context, meta: &TaskMeta) -> Result<(), Error> {
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
    fn fail_job(&self, ctx: &Context, meta: &TaskMeta, error_msg: &str) -> Result<(), Error> {
        with_retry(self, ctx, false, &mut |se| {
            importer_job::fail_job(ctx, se.sql_executor().as_ref(), meta.job_id, error_msg, Some(&meta.summary))
                .map_err(crate::driver)
        })
    }

    /// Go `cancelJob`.
    fn cancel_job(&self, ctx: &Context, meta: &TaskMeta) -> Result<(), Error> {
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
    fn close(&self) {
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
    /// Go `OnTick`: Go also switches TiKV to import mode and registers the
    /// task in PD for the PiTR and changefeed checks; see the module note.
    fn on_tick(&self, _: &Context, _: &Task) {}

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
                    return self.cancel_job(ctx, &meta);
                }
                return self.fail_job(ctx, &meta, &error.to_string());
            }
            return self.fail_job(ctx, &meta, "");
        }
        self.finish_job(ctx, &meta)
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
