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
use crate::proto::{
    self, allocator_type_code, Checksum, ImportStepMeta, MergeSortStepMeta, PostProcessStepMeta, TaskMeta, WriteIngestStepMeta,
};
use tidb_ingestor_engineapi::OnDuplicateKey;
use crate::{plain, Error};

type StepResult<T> = tidb_dxf_execute::Result<T>;

pub(crate) fn step_err(e: Error) -> tidb_dxf_execute::Error {
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
            IMPORT_STEP_IMPORT | IMPORT_STEP_ENCODE_AND_SORT => Ok(Arc::new(ImportStepExecutor {
                info: FrameworkInfoSlot::default(),
                task_id: task.base.id,
                meta,
                table_importer: Mutex::new(None),
                summary: Arc::new(SubtaskSummary::default()),
            })),
            IMPORT_STEP_MERGE_SORT => Ok(Arc::new(MergeSortStepExecutor {
                info: FrameworkInfoSlot::default(),
                task_id: task.base.id,
                meta,
                summary: Arc::new(SubtaskSummary::default()),
            })),
            IMPORT_STEP_WRITE_AND_INGEST => Ok(Arc::new(WriteAndIngestStepExecutor {
                info: FrameworkInfoSlot::default(),
                task_id: task.base.id,
                meta,
                table_importer: Mutex::new(None),
                summary: Arc::new(SubtaskSummary::default()),
            })),
            IMPORT_STEP_COLLECT_CONFLICTS => Ok(Arc::new(crate::collect_conflicts::CollectConflictsStepExecutor {
                info: FrameworkInfoSlot::default(),
                task_id: task.base.id,
                meta,
                table_importer: Mutex::new(None),
                summary: Arc::new(SubtaskSummary::default()),
            })),
            IMPORT_STEP_CONFLICT_RESOLUTION => Ok(Arc::new(crate::conflict_resolution::ConflictResolutionStepExecutor {
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

    /// Go `RunSubtask` and `onFinished`.
    fn run(&self, ctx: &Context, subtask: &mut Subtask) -> Result<(), Error> {
        let ti = self.importer()?;
        let mut meta: ImportStepMeta = proto::unmarshal(&subtask.meta)?;
        let recorded_store = if ti.controller.plan.is_global_sort() {
            Some(crate::planner::new_obj_store_with_recording(
                ctx,
                &ti.controller.plan,
                &self.summary,
                self.get_meter_recorder(),
            )?)
        } else {
            None
        };
        let sort_store = if let Some(recorded) = &recorded_store {
            let store = Arc::clone(&recorded.store);
            meta.read_external(ctx, store.as_ref())?;
            Some(store)
        } else {
            None
        };
        let chunks = meta.chunks.clone().unwrap_or_default();
        let concurrency = (self.get_resource().cpu.capacity() as usize).max(1);
        let checksum = Mutex::new(tidb_lightning_verification::KvGroupChecksum::with_keyspace(ti.key_space()));
        let collector = SummaryCollector(self.summary.clone());
        let shared = Arc::new(Mutex::new(SortedMetas::default()));
        match &sort_store {
            None => self.encode_local(ctx, &ti, &meta, &chunks, concurrency, &checksum, &collector)?,
            Some(store) => {
                self.encode_and_sort(ctx, &ti, subtask.base.id, store, &chunks, concurrency, &checksum, &collector, &shared)?;
            }
        }
        let checksum = checksum.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
        meta.checksum = Some(checksum.get_inner_checksums().iter().map(|(id, c)| (*id, Checksum::from_kv_checksum(c))).collect());
        meta.max_ids = Some(ti.max_ids().into_iter().map(|(tp, v)| (allocator_type_code(tp), v)).collect());
        let shared = std::mem::take(&mut *shared.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        meta.sorted_data_meta = Some(shared.data);
        meta.sorted_index_metas = Some(shared.indexes);
        meta.recorded_conflict_kv_count = shared.recorded_conflict_kv_count;
        // With global sort, the external fields go to external storage.
        if let Some(store) = &sort_store {
            meta.external_path = tidb_ingestor_globalsort::subtask_meta_path(self.task_id, subtask.base.id);
            meta.write_external(ctx, store.as_ref())?;
        }
        subtask.meta = meta.marshal();
        Ok(())
    }

    /// Local sort: each subtask's chunks into a data and an index engine,
    /// then imported.
    #[allow(clippy::too_many_arguments)]
    fn encode_local(
        &self,
        ctx: &Context,
        ti: &Arc<TableImporter>,
        meta: &ImportStepMeta,
        chunks: &[tidb_executor::importer::chunk_process::Chunk],
        concurrency: usize,
        checksum: &Mutex<tidb_lightning_verification::KvGroupChecksum>,
        collector: &SummaryCollector,
    ) -> Result<(), Error> {
        let data_engine = ti.open_data_engine(ctx, meta.id).map_err(plain)?;
        let index_engine = match ti.open_index_engine(ctx, INDEX_ENGINE_ID - meta.id) {
            Ok(e) => e,
            Err(e) => {
                cleanup_engine(ctx, data_engine);
                return Err(plain(e));
            }
        };
        let result = run_chunks(ctx, chunks, concurrency, |worker_ctx, chunk| {
            let mut chunk_checksum = tidb_lightning_verification::KvGroupChecksum::with_keyspace(ti.key_space());
            tidb_executor::importer::engine_process::process_chunk(
                worker_ctx,
                chunk,
                ti,
                &data_engine,
                &index_engine,
                &mut chunk_checksum,
                Some(collector),
            )
            .map_err(|e| e.0)?;
            checksum.lock().unwrap_or_else(std::sync::PoisonError::into_inner).add(&chunk_checksum);
            Ok(())
        });
        if let Err(e) = result {
            cleanup_engine(ctx, data_engine);
            cleanup_engine(ctx, index_engine);
            return Err(e);
        }
        for engine in [data_engine, index_engine] {
            let closed = engine.close(ctx).map_err(|e| plain(e.to_string()))?;
            ti.import_and_cleanup(ctx, &closed).map_err(plain)?;
        }
        Ok(())
    }

    /// Go `encodeAndSortOperator`: each worker encodes its chunks into a
    /// data writer and an index route writer under `{task}/{subtask}`.
    #[allow(clippy::too_many_arguments)]
    fn encode_and_sort(
        &self,
        ctx: &Context,
        ti: &Arc<TableImporter>,
        subtask_id: i64,
        store: &Arc<dyn tidb_objstore::storeapi::Storage>,
        chunks: &[tidb_executor::importer::chunk_process::Chunk],
        concurrency: usize,
        checksum: &Mutex<tidb_lightning_verification::KvGroupChecksum>,
        collector: &SummaryCollector,
        shared: &Arc<Mutex<SortedMetas>>,
    ) -> Result<(), Error> {
        let plan = &self.meta.plan;
        let (data_mem, index_mem) = get_writer_memory_size_limit(&self.get_resource(), plan);
        let data_block_size = tidb_ingestor_simplesst::get_adjusted_block_size(data_mem, MAX_TXN_ENTRY_SIZE_LIMIT);
        let index_block_size =
            tidb_ingestor_simplesst::get_adjusted_block_size(index_mem, tidb_ingestor_simplesst::DEFAULT_BLOCK_SIZE);
        let on_dup_key = plan.get_on_dup_key_mode().to_owned();
        let indices_gen_kv = Arc::new(tidb_executor::importer::kv_encode::get_indices_gen_kv(
            plan.table_info.as_ref().ok_or_else(|| plain("import plan has no table info"))?,
        ));
        let prefix = subtask_prefix(self.task_id, subtask_id);
        // Go's worker pool starts every one of its `concurrency` chunk
        // workers, each with its own data and index writers.
        let workers = concurrency.max(1);
        let next = std::sync::atomic::AtomicUsize::new(0);
        let failure: Mutex<Option<Error>> = Mutex::new(None);
        let (worker_ctx, cancel) = Context::with_cancel(ctx);
        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| {
                    // Go `newChunkWorker`.
                    let worker_uuid = uuid::Uuid::new_v4().to_string();
                    let sink = Arc::clone(shared);
                    let mut data_writer = tidb_ingestor_simplesst::EngineWriter::new(
                        tidb_ingestor_simplesst::WriterBuilder::new()
                            .set_on_close_func(Arc::new(move |s: &tidb_ingestor_simplesst::WriterSummary| {
                                sink.lock().unwrap_or_else(std::sync::PoisonError::into_inner).merge_data_summary(s);
                            }))
                            .set_memory_size_limit(data_mem)
                            .set_block_size(data_block_size)
                            .set_on_dup(get_on_dup_for_conflicted_kv(&on_dup_key))
                            .build(
                                Arc::clone(store),
                                &prefix,
                                &tidb_objstore::gopath::join(&[tidb_ingestor_globalsort::DATA_KV_GROUP, &worker_uuid]),
                            ),
                    );
                    let (store, prefix, sink) = (Arc::clone(store), prefix.clone(), Arc::clone(shared));
                    let (indices_gen_kv, on_dup_key, worker_uuid2) =
                        (Arc::clone(&indices_gen_kv), on_dup_key.clone(), worker_uuid.clone());
                    let mut index_writer = tidb_executor::importer::chunk_process::IndexRouteWriter::new(Box::new(
                        move |index_id| {
                            let on_dup = get_on_dup_for_index(&indices_gen_kv, index_id, &on_dup_key).map_err(|e| e.to_string())?;
                            let sink = Arc::clone(&sink);
                            let writer_id = tidb_objstore::gopath::join(&[
                                "index",
                                &tidb_ingestor_globalsort::index_id_2_kv_group(index_id),
                                &worker_uuid2,
                            ]);
                            Ok(tidb_ingestor_simplesst::WriterBuilder::new()
                                .set_on_close_func(Arc::new(move |s: &tidb_ingestor_simplesst::WriterSummary| {
                                    sink.lock().unwrap_or_else(std::sync::PoisonError::into_inner).merge_index_summary(index_id, s);
                                }))
                                .set_memory_size_limit(index_mem)
                                .set_block_size(index_block_size)
                                .set_on_dup(on_dup)
                                .build(Arc::clone(&store), &prefix, &writer_id))
                        },
                    ));
                    let mut result = Ok(());
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if i >= chunks.len() || worker_ctx.err().is_some() {
                            break;
                        }
                        let mut chunk_checksum = tidb_lightning_verification::KvGroupChecksum::with_keyspace(ti.key_space());
                        let r = tidb_executor::importer::engine_process::process_chunk_with_writer(
                            &worker_ctx,
                            &chunks[i],
                            ti,
                            &mut data_writer,
                            &mut index_writer,
                            &mut chunk_checksum,
                            Some(collector),
                        );
                        match r {
                            Ok(()) => checksum.lock().unwrap_or_else(std::sync::PoisonError::into_inner).add(&chunk_checksum),
                            Err(e) => {
                                result = Err(e.0);
                                break;
                            }
                        }
                    }
                    // Go `chunkWorker.Close`, with a fresh context when canceled.
                    let close_ctx = if worker_ctx.err().is_some() { Context::background() } else { worker_ctx.clone() };
                    use tidb_lightning_backend::EngineWriter as _;
                    let closed = data_writer
                        .close(&close_ctx)
                        .map(|_| ())
                        .and_then(|()| index_writer.close(&close_ctx).map(|_| ()))
                        .map_err(|e| Arc::new(e) as Error);
                    if let Err(e) = result.and(closed) {
                        failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get_or_insert(e);
                        cancel.cancel();
                    }
                });
            }
        });
        if let Some(e) = failure.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner) {
            return Err(e);
        }
        Ok(())
    }
}

/// Runs `f` on every chunk with up to `concurrency` workers; the first
/// failure cancels the rest.
fn run_chunks<C: Sync>(
    ctx: &Context,
    chunks: &[C],
    concurrency: usize,
    f: impl Fn(&Context, &C) -> Result<(), Error> + Sync,
) -> Result<(), Error> {
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
                if let Err(e) = f(&worker_ctx, &chunks[i]) {
                    failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get_or_insert(e);
                    cancel.cancel();
                    return;
                }
            });
        }
    });
    failure.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner).map_or(Ok(()), Err)
}

/// Go `SharedVars`' sorted metas of an encode subtask.
#[derive(Default)]
struct SortedMetas {
    data: tidb_ingestor_globalsort::SortedKVMeta,
    indexes: std::collections::BTreeMap<i64, tidb_ingestor_globalsort::SortedKVMeta>,
    recorded_conflict_kv_count: u64,
}

impl SortedMetas {
    /// Go `mergeDataSummary`.
    fn merge_data_summary(&mut self, s: &tidb_ingestor_simplesst::WriterSummary) {
        self.data.merge_summary(s);
        self.recorded_conflict_kv_count += s.conflict_info.count;
    }

    /// Go `mergeIndexSummary`.
    fn merge_index_summary(&mut self, index_id: i64, s: &tidb_ingestor_simplesst::WriterSummary) {
        self.recorded_conflict_kv_count += s.conflict_info.count;
        match self.indexes.get_mut(&index_id) {
            Some(meta) => meta.merge_summary(s),
            None => {
                self.indexes.insert(index_id, tidb_ingestor_globalsort::SortedKVMeta::new(Some(s)));
            }
        }
    }
}

/// Go `config.MaxTxnEntrySizeLimit`: 120 MiB.
const MAX_TXN_ENTRY_SIZE_LIMIT: usize = 120 << 20;
/// Go `writerMemBudgetRatio`.
const WRITER_MEM_BUDGET_RATIO: f64 = 0.5;

/// Go `getWriterMemorySizeLimit`: the data writer's and each index
/// writer's memory, from half the memory per core in `index + 3` shares.
fn get_writer_memory_size_limit(resource: &StepResource, plan: &tidb_executor::importer::Plan) -> (u64, u64) {
    let index_kv_group_cnt = plan
        .desired_table_info
        .as_ref()
        .map_or(0, tidb_executor::importer::kv_encode::get_num_of_index_gen_kv);
    let mem_per_share = resource.memory_per_core() as f64 * WRITER_MEM_BUDGET_RATIO / (index_kv_group_cnt + 3) as f64;
    ((mem_per_share * 3.0) as u64, mem_per_share as u64)
}

/// Go `subtaskPrefix`.
fn subtask_prefix(task_id: i64, subtask_id: i64) -> String {
    tidb_objstore::gopath::join(&[&task_id.to_string(), &subtask_id.to_string()])
}

/// Go `getOnDupForConflictedKV`.
fn get_on_dup_for_conflicted_kv(on_dup_key_mode: &str) -> OnDuplicateKey {
    if on_dup_key_mode == "capture" {
        OnDuplicateKey::Record
    } else {
        OnDuplicateKey::Error
    }
}

/// Go `getOnDupForKVGroup`.
fn get_on_dup_for_kv_group(
    indices_gen_kv: &std::collections::BTreeMap<i64, tidb_executor::importer::kv_encode::GenKVIndex>,
    kv_group: &str,
    on_dup_key_mode: &str,
) -> Result<OnDuplicateKey, Error> {
    if kv_group == tidb_ingestor_globalsort::DATA_KV_GROUP {
        return Ok(get_on_dup_for_conflicted_kv(on_dup_key_mode));
    }
    let index_id = tidb_ingestor_globalsort::kv_group_2_index_id(kv_group).map_err(crate::planner::global_sort_error)?;
    get_on_dup_for_index(indices_gen_kv, index_id, on_dup_key_mode)
}

/// Go `getOnDupForIndex`: a unique index conflicts like the data; a
/// non-unique one only repeats the same row.
fn get_on_dup_for_index(
    indices_gen_kv: &std::collections::BTreeMap<i64, tidb_executor::importer::kv_encode::GenKVIndex>,
    index_id: i64,
    on_dup_key_mode: &str,
) -> Result<OnDuplicateKey, Error> {
    let Some(info) = indices_gen_kv.get(&index_id) else {
        return Err(plain(format!("unknown index {index_id}")));
    };
    if on_dup_key_mode == "error" {
        return Ok(OnDuplicateKey::Error);
    }
    if info.unique {
        return Ok(get_on_dup_for_conflicted_kv(on_dup_key_mode));
    }
    Ok(OnDuplicateKey::Remove)
}

/// Go `normalizeSubtaskErr`: a duplicate key becomes
/// `ErrLoadDataDuplicateKeyConflict`. Go's `ErrFoundDuplicateKeys.Equal`
/// compares the error's cause; the writers keep it as the error's source.
fn normalize_subtask_err(e: Error) -> Error {
    let dup = &*tidb_lightning_common::errors::ERR_FOUND_DUPLICATE_KEYS;
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(e.as_ref());
    let mut found = false;
    while let Some(err) = source {
        if err.downcast_ref::<tidb_terror::TerrorError>().is_some_and(|t| t.equal(Some(dup as &(dyn std::error::Error + 'static)))) {
            found = true;
            break;
        }
        source = err.source();
    }
    if found {
        return Arc::new(tidb_exeerrors::ERR_LOAD_DATA_DUPLICATE_KEY_CONFLICT.fast_gen_by_args(&[]));
    }
    e
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
        let ti = new_table_importer(ctx, self.task_id, &self.meta).map_err(step_err)?;
        *self.table_importer.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(ti));
        Ok(())
    }

    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> StepResult<()> {
        self.run(ctx, subtask).map_err(|e| step_err(normalize_subtask_err(e)))
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

/// Go `mergeSortStepExecutor`.
struct MergeSortStepExecutor {
    info: FrameworkInfoSlot,
    task_id: i64,
    meta: TaskMeta,
    summary: Arc<SubtaskSummary>,
}

impl StepExecFrameworkInfo for MergeSortStepExecutor {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.info
    }
}

impl MergeSortStepExecutor {
    /// Go `RunSubtask` and `onFinished`.
    fn run(&self, ctx: &Context, subtask: &mut Subtask) -> Result<(), Error> {
        let mut sm: MergeSortStepMeta = proto::unmarshal(&subtask.meta)?;
        let recorded = crate::planner::new_obj_store_with_recording(
            ctx,
            &self.meta.plan,
            &self.summary,
            self.get_meter_recorder(),
        )?;
        let store = Arc::clone(&recorded.store);
        sm.read_external(ctx, store.as_ref())?;
        let sorted = Arc::new(Mutex::new(tidb_ingestor_globalsort::SortedKVMeta::default()));
        let sink = Arc::clone(&sorted);
        let on_writer_close: tidb_ingestor_simplesst::OnWriterCloseFunc =
            Arc::new(move |s| sink.lock().unwrap_or_else(std::sync::PoisonError::into_inner).merge_summary(s));
        let indices_gen_kv = tidb_executor::importer::kv_encode::get_indices_gen_kv(
            self.meta.plan.table_info.as_ref().ok_or_else(|| plain("import plan has no table info"))?,
        );
        let on_dup = get_on_dup_for_kv_group(&indices_gen_kv, &sm.kv_group, self.meta.plan.get_on_dup_key_mode())?;
        let res = self.get_resource();
        let op = tidb_ingestor_globalsort::MergeOperator::new(
            Arc::clone(&store),
            res.memory_per_core(),
            &subtask_prefix(self.task_id, subtask.base.id),
            tidb_ingestor_simplesst::DEFAULT_ONE_WRITER_BLOCK_SIZE,
            on_writer_close,
            Some(Arc::new(tidb_ingestor_globalsort::MergeCollector::new(Some(self.summary.clone())))),
            res.cpu.capacity().max(1) as usize,
            false,
            on_dup,
        );
        tidb_ingestor_globalsort::merge_overlapping_files(ctx, sm.data_files.as_deref().unwrap_or_default(), &op)
            .map_err(crate::planner::global_sort_error)?;
        let sorted = std::mem::take(&mut *sorted.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        tidb_log::info(
            "merge sort finished",
            &[
                tidb_log::Field::new("total-kv-size", tidb_log::Value::U64(sorted.total_kv_size)),
                tidb_log::Field::new("total-kv-count", tidb_log::Value::U64(sorted.total_kv_cnt)),
            ],
        );
        // Go `onFinished`: the subtask's sorted meta always goes to external
        // storage.
        sm.recorded_conflict_kv_count = sorted.conflict_info.count;
        sm.sorted_kv_meta = sorted;
        sm.external_path = tidb_ingestor_globalsort::subtask_meta_path(self.task_id, subtask.base.id);
        sm.write_external(ctx, store.as_ref())?;
        subtask.meta = sm.marshal();
        Ok(())
    }
}

impl StepExecutor for MergeSortStepExecutor {
    fn init(&self, _: &Context) -> StepResult<()> {
        Ok(())
    }

    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> StepResult<()> {
        self.run(ctx, subtask).map_err(|e| step_err(normalize_subtask_err(e)))
    }

    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        Some(self.summary.clone())
    }

    fn reset_summary(&self) {
        self.summary.reset();
    }

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

/// Go `writeAndIngestStepExecutor`.
struct WriteAndIngestStepExecutor {
    info: FrameworkInfoSlot,
    task_id: i64,
    meta: TaskMeta,
    table_importer: Mutex<Option<Arc<TableImporter>>>,
    summary: Arc<SubtaskSummary>,
}

impl StepExecFrameworkInfo for WriteAndIngestStepExecutor {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.info
    }
}

/// Go `config.SplitRegionSize` and `SplitRegionKeys`.
const SPLIT_REGION_SIZE: u64 = 96 << 20;
const SPLIT_REGION_KEYS: usize = 960_000;

impl WriteAndIngestStepExecutor {
    /// Go `RunSubtask` and `onFinished`.
    fn run(&self, ctx: &Context, subtask: &mut Subtask) -> Result<(), Error> {
        let ti = self
            .table_importer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| plain("write and ingest step executor is not initialized"))?;
        let mut sm: WriteIngestStepMeta = proto::unmarshal(&subtask.meta)?;
        let recorded = crate::planner::new_obj_store_with_recording(
            ctx,
            &self.meta.plan,
            &self.summary,
            self.get_meter_recorder(),
        )?;
        let store = Arc::clone(&recorded.store);
        sm.read_external(ctx, store.as_ref())?;
        // Compatible with metas from before range job keys.
        let job_keys = sm.range_job_keys.clone().or_else(|| sm.range_split_keys.clone()).unwrap_or_default();
        let indices_gen_kv = tidb_executor::importer::kv_encode::get_indices_gen_kv(
            self.meta.plan.table_info.as_ref().ok_or_else(|| plain("import plan has no table info"))?,
        );
        let on_dup = get_on_dup_for_kv_group(&indices_gen_kv, &sm.kv_group, self.meta.plan.get_on_dup_key_mode())?;
        // Go `ingestCollector`.
        let (summary, is_data) = (self.summary.clone(), sm.kv_group == tidb_ingestor_globalsort::DATA_KV_GROUP);
        let meter_rec = self.get_meter_recorder();
        ti.backend.set_collector(Some(Arc::new(move |bytes, rows| {
            summary.processed.fetch_add(bytes, Ordering::SeqCst);
            if is_data {
                summary.row_cnt.fetch_add(rows, Ordering::SeqCst);
            }
            // since the region job might be retried, this value might be larger than
            // the total KV size.
            meter_rec.inc_cluster_write_bytes(bytes as u64);
        })));
        let (_, engine_uuid) = tidb_lightning_backend::make_uuid("", subtask.base.id);
        let engine_id = *engine_uuid.as_bytes();
        let result = ti
            .backend
            .close_external_engine(
                engine_id,
                &tidb_lightning_backend::ExternalEngineConfig {
                    ext_store: Arc::clone(&store),
                    data_files: sm.data_files.clone().unwrap_or_default(),
                    stat_files: sm.stat_files.clone().unwrap_or_default(),
                    start_key: sm.sorted_kv_meta.start_key.clone(),
                    end_key: sm.sorted_kv_meta.end_key.clone(),
                    job_keys,
                    split_keys: sm.range_split_keys.clone().unwrap_or_default(),
                    total_file_size: sm.sorted_kv_meta.total_kv_size as i64,
                    total_kv_count: 0,
                    check_hotspot: false,
                    mem_capacity: self.get_resource().mem.capacity(),
                    on_dup,
                    file_prefix: subtask_prefix(self.task_id, subtask.base.id),
                },
                sm.ts,
            )
            .and_then(|()| ti.backend.import_engine(ctx, engine_id, SPLIT_REGION_SIZE, SPLIT_REGION_KEYS));
        result.map_err(ingest_error)?;
        // Go `onFinished`: only a recorded conflict rewrites the meta.
        let conflict_info = ti.backend.get_external_engine_conflict_info(engine_id);
        if let Err(e) = ti.backend.cleanup_engine(engine_id) {
            tidb_log::warn("failed to cleanup engine", &[tidb_log::Field::new("error", tidb_log::Value::Str(e.to_string()))]);
        }
        if conflict_info.count == 0 {
            return Ok(());
        }
        sm.recorded_conflict_kv_count = conflict_info.count;
        sm.sorted_kv_meta.conflict_info = conflict_info;
        sm.external_path = tidb_ingestor_globalsort::subtask_meta_path(self.task_id, subtask.base.id);
        sm.write_external(ctx, store.as_ref())?;
        subtask.meta = sm.marshal();
        Ok(())
    }
}

/// An ingest error, keeping a Go `*errors.Error`.
fn ingest_error(e: tidb_ingestor_ingestctrl::Error) -> Error {
    match e {
        tidb_ingestor_ingestctrl::Error::Ingest(t) => Arc::new(t),
        tidb_ingestor_ingestctrl::Error::Source(e) => e,
        e => plain(e.to_string()),
    }
}

impl StepExecutor for WriteAndIngestStepExecutor {
    /// Go `Init`: the table importer and its local backend.
    fn init(&self, ctx: &Context) -> StepResult<()> {
        let ti = new_table_importer(ctx, self.task_id, &self.meta).map_err(step_err)?;
        *self.table_importer.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(ti));
        Ok(())
    }

    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> StepResult<()> {
        self.run(ctx, subtask).map_err(|e| step_err(normalize_subtask_err(e)))
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

/// Go `getTableImporter`.
pub(crate) fn new_table_importer(ctx: &Context, task_id: i64, meta: &TaskMeta) -> Result<TableImporter, Error> {
    let store = crate::import_store()?;
    let mut controller = build_controller_for_plan(&meta.plan, &meta.stmt)?;
    controller.init_data_store(ctx).map_err(crate::driver)?;
    let client = store.import_client().map_err(plain)?;
    TableImporter::new(controller, &task_id.to_string(), client, store.keyspace()).map_err(plain)
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

/// Go `importer.PostProcess` for an import outside DXF (IMPORT INTO ...
/// FROM SELECT): rebases the allocators, then verifies the checksum over a
/// new session of the task manager (Go's `newSCtx`).
///
/// # Errors
/// The rebase failed, the checksum could not be read or did not match.
pub fn post_process_with_new_session(
    ctx: &Context,
    plan: &tidb_executor::importer::Plan,
    max_ids: &std::collections::BTreeMap<tidb_lightning_backend_kv::AllocatorType, i64>,
    local_checksum: &tidb_lightning_verification::KvGroupChecksum,
) -> Result<(), Error> {
    let store = crate::import_store()?;
    table_import::rebase_allocator_bases(&StoreRebaser(store), max_ids, plan).map_err(crate::driver)?;
    let manager = tidb_dxf_storage::get_task_manager().map_err(|e| Arc::new(e) as Error)?;
    let local = local_checksum.merged_checksum();
    let mut failure: Option<Error> = None;
    let result = manager.with_new_session(&mut |se| {
        let session = SessionChecksum { ctx, se };
        table_import::verify_checksum(plan, &local, || table_import::remote_checksum_table_by_sql(&session, plan))
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

#[cfg(test)]
mod global_sort_tests {
    use super::*;
    use tidb_executor::importer::kv_encode::GenKVIndex;

    fn indices() -> std::collections::BTreeMap<i64, GenKVIndex> {
        std::collections::BTreeMap::from([
            (1, GenKVIndex { name: "u".into(), unique: true }),
            (2, GenKVIndex { name: "k".into(), unique: false }),
        ])
    }

    /// Go `getOnDupForKVGroup` / `getOnDupForIndex`.
    #[test]
    fn on_dup_for_kv_groups() {
        let i = indices();
        assert_eq!(get_on_dup_for_kv_group(&i, "data", "error").unwrap(), OnDuplicateKey::Error);
        assert_eq!(get_on_dup_for_kv_group(&i, "data", "capture").unwrap(), OnDuplicateKey::Record);
        assert_eq!(get_on_dup_for_kv_group(&i, "1", "capture").unwrap(), OnDuplicateKey::Record);
        assert_eq!(get_on_dup_for_kv_group(&i, "1", "error").unwrap(), OnDuplicateKey::Error);
        assert_eq!(get_on_dup_for_kv_group(&i, "2", "capture").unwrap(), OnDuplicateKey::Remove);
        assert_eq!(get_on_dup_for_kv_group(&i, "2", "error").unwrap(), OnDuplicateKey::Error);
        assert_eq!(get_on_dup_for_kv_group(&i, "3", "error").unwrap_err().to_string(), "unknown index 3");
        assert!(get_on_dup_for_kv_group(&i, "x", "error").is_err());
    }

    /// Go `normalizeSubtaskErr`: `ErrFoundDuplicateKeys`, as itself or as
    /// the cause of a writer's error, becomes `ErrLoadDataDuplicateKeyConflict`;
    /// its text alone does not.
    #[test]
    fn duplicate_key_errors_are_normalized() {
        let dup = tidb_ingestor_simplesst::found_duplicate_keys(b"k", b"v");
        let want = "[executor:8167]Duplicate key conflict found. Please resolve conflicts in the input dataset, or set on_duplicate_key to a strategy that can handle conflicts, for example 'capture'";
        assert_eq!(normalize_subtask_err(Arc::new(dup.clone())).to_string(), want);
        let tidb_ingestor_simplesst::Error::FoundDuplicateKeys { error: t, .. } = dup else { unreachable!() };
        assert_eq!(normalize_subtask_err(Arc::new(t.clone())).to_string(), want);
        let writer_error = tidb_lightning_backend::Error::from_cause(tidb_ingestor_simplesst::Error::Terror(t.clone()));
        assert_eq!(normalize_subtask_err(Arc::new(writer_error)).to_string(), want);
        let text = format!("when writing: {t}");
        assert_eq!(normalize_subtask_err(plain(text.clone())).to_string(), text);
        assert_eq!(normalize_subtask_err(plain("other")).to_string(), "other");
    }

    /// Go `getWriterMemorySizeLimit` and `subtaskPrefix`.
    #[test]
    fn writer_memory_and_prefix() {
        let res = StepResource { cpu: tidb_dxf::Allocatable::new(4), mem: tidb_dxf::Allocatable::new(8 << 30) };
        let plan = tidb_executor::importer::Plan::default();
        // No index: half of 2 GiB per core in 3 float shares, 3 of them for
        // the data writer.
        assert_eq!(get_writer_memory_size_limit(&res, &plan), (1 << 30, 357_913_941));
        assert_eq!(subtask_prefix(7, 30), "7/30");
    }
}
