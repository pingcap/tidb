// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/task_executor.go` and `subtask_executor.go`: the
//! import task's executor. For local sort, the `import` step encodes each
//! subtask's chunks into a data and an index engine and ingests them; the
//! `post-process` step rebases the allocators and verifies the checksum.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use tidb_dxf::*;
use tidb_dxf_execute::{FrameworkInfoSlot, StepExecFrameworkInfo, StepExecutor, SubtaskSummary};
use tidb_dxf_storage::SessionContext;
use tidb_dxf_taskexecutor::{BaseTaskExecutor, TaskExecutor};
use tidb_executor::importer::table_import::{self, TableImporter, INDEX_ENGINE_ID};
use tidb_gocontext::Context;

use crate::planner::build_controller_for_plan;
use crate::proto::{self, allocator_type_code, Checksum, ImportStepMeta, PostProcessStepMeta, TaskMeta};
use crate::{plain, Error};

type StepResult<T> = tidb_dxf_execute::Result<T>;

fn step_err(e: Error) -> tidb_dxf_execute::Error {
    // Go returns the error value itself: a `*errors.Error` or
    // `errors.Annotate` stays one, so DXF's `serializeErr` reads its
    // `GetErrStackMsg` instead of the coded text.
    if let Some(e) = e.downcast_ref::<tidb_terror::TerrorError>() {
        return Box::new(e.clone());
    }
    if let Some(e) = e.downcast_ref::<tidb_terror::WithMessage>() {
        return Box::new(e.clone());
    }
    Box::new(ArcStepError(e))
}

/// An [`Error`] as a step executor's error, keeping its source chain.
#[derive(Debug)]
struct ArcStepError(Error);

impl std::fmt::Display for ArcStepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ArcStepError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Go `importinto`'s `init`: the task executor factory.
pub(crate) fn register() {
    tidb_dxf_taskexecutor::register_task_type(
        IMPORT_INTO,
        Arc::new(|ctx, task, param| {
            let executor = Arc::new(ImportExecutor { base: BaseTaskExecutor::new(ctx, task, param) });
            let extension: Arc<dyn tidb_dxf_taskexecutor::Extension> = executor.clone();
            executor.base.set_extension(Arc::downgrade(&extension));
            executor
        }),
    );
}

/// Go `importExecutor`.
pub struct ImportExecutor {
    base: BaseTaskExecutor,
}

impl TaskExecutor for ImportExecutor {
    fn init(&self, ctx: &Context) -> Result<(), Error> {
        self.base.init(ctx)
    }
    fn run(&self) {
        self.base.run();
    }
    fn get_task_base(&self) -> TaskBase {
        self.base.get_task_base()
    }
    fn cancel_running_subtask(&self) {
        self.base.cancel_running_subtask();
    }
    fn cancel(&self) {
        self.base.cancel();
    }
    fn close(&self) {
        self.base.close();
    }
    fn is_retryable_error(&self, e: &(dyn std::error::Error + 'static)) -> bool {
        tidb_dxf_taskexecutor::Extension::is_retryable_error(self, e)
    }
}

impl tidb_dxf_taskexecutor::Extension for ImportExecutor {
    /// Go `IsIdempotent`.
    fn is_idempotent(&self, _: &Subtask) -> bool {
        true
    }

    /// Go `GetStepExecutor`.
    fn get_step_executor(&self, task: &Task) -> Result<Arc<dyn StepExecutor>, Error> {
        let meta = TaskMeta::from_json(&task.meta)?;
        match task.base.step {
            IMPORT_STEP_IMPORT => Ok(Arc::new(ImportStepExecutor {
                info: FrameworkInfoSlot::default(),
                task_id: task.base.id,
                meta,
                table_importer: Mutex::new(None),
                summary: Arc::new(SubtaskSummary::default()),
            })),
            IMPORT_STEP_POST_PROCESS => Ok(Arc::new(PostProcessStepExecutor {
                info: FrameworkInfoSlot::default(),
                meta,
                task_table: self.base.get_task_table().clone(),
            })),
            step => Err(plain(format!("unknown step {} for import task {}", step.0, task.base.id))),
        }
    }

    /// Go `IsRetryableError`.
    fn is_retryable_error(&self, e: &(dyn std::error::Error + 'static)) -> bool {
        tidb_lightning_common::is_retryable_error(e)
    }
}
/// Go `importStepExecutor`, for local sort.
struct ImportStepExecutor {
    info: FrameworkInfoSlot,
    task_id: i64,
    meta: TaskMeta,
    table_importer: Mutex<Option<Arc<TableImporter>>>,
    summary: Arc<SubtaskSummary>,
}

impl StepExecFrameworkInfo for ImportStepExecutor {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.info
    }
}

/// Go `importStepExecutor`'s `execute.Collector`: bytes read are the
/// processed units, encoded rows the row count.
struct SummaryCollector(Arc<SubtaskSummary>);

impl tidb_dxf_execute::Collector for SummaryCollector {
    fn accepted(&self, bytes: i64) {
        self.0.processed.fetch_add(bytes, Ordering::SeqCst);
    }
    fn processed(&self, _: i64, rows: i64) {
        self.0.row_cnt.fetch_add(rows, Ordering::SeqCst);
    }
}

impl ImportStepExecutor {
    fn importer(&self) -> Result<Arc<TableImporter>, Error> {
        self.table_importer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| plain("import step executor is not initialized"))
    }

    /// Go `RunSubtask` and `onFinished` for local sort.
    fn run(&self, ctx: &Context, subtask: &mut Subtask) -> Result<(), Error> {
        let ti = self.importer()?;
        let mut meta: ImportStepMeta = proto::unmarshal(&subtask.meta)?;
        let chunks = meta.chunks.clone().unwrap_or_default();
        let data_engine = ti.open_data_engine(ctx, meta.id).map_err(plain)?;
        let index_engine = match ti.open_index_engine(ctx, INDEX_ENGINE_ID - meta.id) {
            Ok(e) => e,
            Err(e) => {
                cleanup_engine(ctx, data_engine);
                return Err(plain(e));
            }
        };
        let concurrency = (self.get_resource().cpu.capacity() as usize).max(1);
        let checksum = Mutex::new(tidb_lightning_verification::KvGroupChecksum::with_keyspace(ti.key_space()));
        let collector = SummaryCollector(self.summary.clone());
        let next = std::sync::atomic::AtomicUsize::new(0);
        let failure: Mutex<Option<Error>> = Mutex::new(None);
        let (worker_ctx, cancel) = Context::with_cancel(ctx);
        std::thread::scope(|scope| {
            for _ in 0..concurrency.min(chunks.len().max(1)) {
                scope.spawn(|| loop {
                    let i = next.fetch_add(1, Ordering::SeqCst);
                    if i >= chunks.len() || worker_ctx.err().is_some() {
                        return;
                    }
                    let mut chunk_checksum = tidb_lightning_verification::KvGroupChecksum::with_keyspace(ti.key_space());
                    let result = tidb_executor::importer::engine_process::process_chunk(
                        &worker_ctx,
                        &chunks[i],
                        &ti,
                        &data_engine,
                        &index_engine,
                        &mut chunk_checksum,
                        Some(&collector),
                    );
                    match result {
                        Ok(()) => checksum.lock().unwrap_or_else(std::sync::PoisonError::into_inner).add(&chunk_checksum),
                        Err(e) => {
                            failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get_or_insert(e.0);
                            cancel.cancel();
                            return;
                        }
                    }
                });
            }
        });
        if let Some(e) = failure.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner) {
            cleanup_engine(ctx, data_engine);
            cleanup_engine(ctx, index_engine);
            return Err(e);
        }
        for engine in [data_engine, index_engine] {
            let closed = engine.close(ctx).map_err(|e| plain(e.to_string()))?;
            ti.import_and_cleanup(ctx, &closed).map_err(plain)?;
        }
        let checksum = checksum.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
        meta.checksum = Some(checksum.get_inner_checksums().iter().map(|(id, c)| (*id, Checksum::from_kv_checksum(c))).collect());
        meta.max_ids = Some(ti.max_ids().into_iter().map(|(tp, v)| (allocator_type_code(tp), v)).collect());
        subtask.meta = proto::marshal(&meta)?;
        Ok(())
    }
}

/// Go `closeAndCleanupEngine`: a failure is only logged.
fn cleanup_engine(ctx: &Context, engine: tidb_lightning_backend::OpenedEngine<'_>) {
    if let Ok(closed) = engine.close(ctx) {
        let _ = closed.cleanup(ctx);
    }
}
impl StepExecutor for ImportStepExecutor {
    /// Go `Init`: the table importer over a new local backend.
    fn init(&self, ctx: &Context) -> StepResult<()> {
        let store = crate::import_store().map_err(step_err)?;
        let mut controller = build_controller_for_plan(&self.meta.plan, &self.meta.stmt).map_err(step_err)?;
        controller.init_data_store(ctx).map_err(|e| step_err(crate::driver(e)))?;
        let client = store.import_client().map_err(|e| step_err(plain(e)))?;
        let ti = TableImporter::new(controller, &self.task_id.to_string(), client, store.keyspace())
            .map_err(|e| step_err(plain(e)))?;
        *self.table_importer.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(ti));
        Ok(())
    }

    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> StepResult<()> {
        self.run(ctx, subtask).map_err(step_err)
    }

    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        Some(self.summary.clone())
    }

    fn reset_summary(&self) {
        self.summary.reset();
    }

    /// Go `Cleanup`.
    fn cleanup(&self, _: &Context) -> StepResult<()> {
        if let Some(ti) = self.table_importer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() {
            ti.controller.close();
            ti.close();
        }
        Ok(())
    }

    fn task_meta_modified(&self, _: &Context, _: &[u8]) -> StepResult<()> {
        Ok(())
    }

    fn resource_modified(&self, _: &Context, _: &StepResource) -> StepResult<()> {
        Ok(())
    }
}

/// Go `postProcessStepExecutor`.
struct PostProcessStepExecutor {
    info: FrameworkInfoSlot,
    meta: TaskMeta,
    task_table: Arc<dyn tidb_dxf_taskexecutor::TaskTable>,
}

impl StepExecFrameworkInfo for PostProcessStepExecutor {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.info
    }
}

/// The session `RemoteChecksumTableBySQL` runs in.
struct SessionChecksum<'a> {
    ctx: &'a Context,
    se: &'a dyn SessionContext,
}

fn storage_driver_err(e: tidb_dxf_storage::StorageError) -> tidb_driver_error::DriverError {
    match e {
        tidb_dxf_storage::StorageError::Terror(t) => tidb_driver_error::DriverError::from(t),
        e => tidb_driver_error::DriverError::from(tidb_terror::MysqlError::unknown(e.to_string())),
    }
}

impl table_import::ChecksumSession for SessionChecksum<'_> {
    fn exec_sql(&self, sql: &str) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_driver_error::DriverError> {
        tidb_sqlexec::execute_sql(self.ctx, self.se.sql_executor().as_ref(), sql, &[])
            .map_err(tidb_executor::memtable_reader::sql_exec_driver_error)
    }
    fn set_system_var(&self, name: &str, value: &str) -> Result<(), tidb_driver_error::DriverError> {
        self.se.set_system_var(name, value).map_err(storage_driver_err)
    }
    fn distsql_scan_concurrency(&self) -> usize {
        tidb_dxf_storage::exec_sql(self.ctx, self.se, "select @@tidb_distsql_scan_concurrency", &[])
            .ok()
            .and_then(|rows| rows.first().and_then(|r| r.first()).and_then(|d| d.sql_string().ok()))
            .and_then(|s| s.parse().ok())
            .unwrap_or(15)
    }
    fn set_distsql_scan_concurrency(&self, n: usize) {
        let _ = self.se.set_system_var("tidb_distsql_scan_concurrency", &n.to_string());
    }
}

/// The store's allocators, as `RebaseAllocatorBases` rebases them.
struct StoreRebaser(Arc<dyn crate::ImportStore>);

impl table_import::AllocatorRebaser for StoreRebaser {
    fn rebase(
        &self,
        db_id: i64,
        table: &tidb_model::TableInfo,
        tp: tidb_lightning_backend_kv::AllocatorType,
        _unsigned: bool,
        _cache: i64,
        base: i64,
    ) -> Result<(), tidb_driver_error::DriverError> {
        self.0
            .rebase_allocator(db_id, table, tp, base)
            .map_err(|e| tidb_driver_error::DriverError::from(tidb_terror::MysqlError::unknown(e)))
    }
}
impl PostProcessStepExecutor {
    /// Go `postProcess`.
    fn post_process(&self, ctx: &Context, meta: &PostProcessStepMeta) -> Result<(), Error> {
        let plan = &self.meta.plan;
        let store = crate::import_store()?;
        let max_ids = meta
            .max_ids
            .clone()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(code, v)| proto::allocator_type_of(code).map(|tp| (tp, v)))
            .collect();
        table_import::rebase_allocator_bases(&StoreRebaser(store), &max_ids, plan).map_err(crate::driver)?;
        let mut local = tidb_lightning_verification::KvGroupChecksum::new_kv_group_checksum_for_add();
        for (id, c) in meta.checksum.clone().unwrap_or_default() {
            local.add_raw_group(id, c.size, c.kvs, c.sum);
        }
        let mut final_checksum = local.merged_checksum();
        final_checksum.sub(&meta.deleted_rows_checksum.to_kv_checksum());
        if meta.too_many_conflicts_from_index {
            return Ok(());
        }
        let ctx = tidb_dxf_storage::with_internal_source_type(ctx, tidb_txnkv::INTERNAL_DIST_TASK);
        let mut failure: Option<Error> = None;
        let result = self.task_table.with_new_session(&mut |se| {
            let session = SessionChecksum { ctx: &ctx, se };
            table_import::verify_checksum(plan, &final_checksum, || table_import::remote_checksum_table_by_sql(&session, plan))
                .map_err(|e| {
                    let e = crate::verify_checksum_error(e);
                    let text = e.to_string();
                    failure = Some(e);
                    tidb_dxf_storage::StorageError::Other(text)
                })
        });
        match result {
            Ok(()) => Ok(()),
            Err(e) => Err(failure.unwrap_or_else(|| Arc::new(e))),
        }
    }
}

impl StepExecutor for PostProcessStepExecutor {
    fn init(&self, _: &Context) -> StepResult<()> {
        Ok(())
    }

    /// Go `RunSubtask`.
    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> StepResult<()> {
        let meta: PostProcessStepMeta = proto::unmarshal(&subtask.meta).map_err(step_err)?;
        self.post_process(ctx, &meta).map_err(step_err)
    }

    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        None
    }

    fn reset_summary(&self) {}

    fn cleanup(&self, _: &Context) -> StepResult<()> {
        Ok(())
    }

    fn task_meta_modified(&self, _: &Context, _: &[u8]) -> StepResult<()> {
        Ok(())
    }

    fn resource_modified(&self, _: &Context, _: &StepResource) -> StepResult<()> {
        Ok(())
    }
}
