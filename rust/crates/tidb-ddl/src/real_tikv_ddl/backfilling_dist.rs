// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go distributed backfill over the owner's native store and session pool.
use super::*;
use crate::backfilling_dist_executor::{
    BackfillSubTaskMeta, BackfillTaskMeta, decode_backfill_sub_task_meta,
};
use crate::backfilling_dist_scheduler::{LitBackfillScheduler, calculate_region_batch};
use std::sync::RwLock;
use tidb_ddl_ingest::backend::{BackendCtx, UnregisterOpt};
use tidb_ddl_ingest::backend_mgr::{BackendCtxBuilder, create_local_backend};
use tidb_dxf::*;
use tidb_dxf_execute::{FrameworkInfoSlot, StepExecFrameworkInfo, StepExecutor, SubtaskSummary};
use tidb_dxf_scheduler::{BaseScheduler, Scheduler};
use tidb_dxf_taskexecutor::{BaseTaskExecutor, TaskExecutor};
use tidb_gocontext::Context;
type Error = Arc<dyn std::error::Error + Send + Sync>;
fn plain(message: impl Into<String>) -> Error {
    Arc::new(std::io::Error::other(message.into()))
}

/// The `ddl` fields reached by Go's backfill scheduler and executor.
pub(crate) struct BackfillDdl<S: StoreClient> {
    pub opener: Arc<RealOptimisticTransactionOpener<S>>,
    pub timeout: Duration,
    pub backfiller: Arc<dyn Fn() -> Arc<dyn IndexBackfiller> + Send + Sync>,
    pub etcd: Option<Arc<tidb_pd_client::EtcdClient>>,
}
/// Go `ddl.registerDistTask`'s backfill factories.
pub(crate) fn register_backfill_task<S: StoreClient>(ddl: Arc<BackfillDdl<S>>) {
    let scheduler_ddl = ddl.clone();
    tidb_dxf_scheduler::register_scheduler_factory(
        BACKFILL,
        Arc::new(move |ctx, task, param| {
            let scheduler = Arc::new(BackfillScheduler {
                base: BaseScheduler::new(ctx, task, param),
                ddl: scheduler_ddl.clone(),
                meta: RwLock::new(None),
            });
            let extension: Arc<dyn tidb_dxf_scheduler::Extension> = scheduler.clone();
            scheduler.base.set_extension(Arc::downgrade(&extension));
            scheduler
        }),
    );
    tidb_dxf_taskexecutor::register_task_type(
        BACKFILL,
        Arc::new(move |ctx, task, param| {
            let executor = Arc::new(BackfillDistExecutor {
                base: BaseTaskExecutor::new(ctx, task.clone(), param),
                task,
                ddl: ddl.clone(),
                meta: RwLock::new(None),
            });
            let extension: Arc<dyn tidb_dxf_taskexecutor::Extension> = executor.clone();
            executor.base.set_extension(Arc::downgrade(&extension));
            executor
        }),
    );
}
/// Go `getTblInfo` and `getUserTableFromTaskStore`'s current metadata read.
fn get_tbl_info<S: StoreClient>(
    ddl: &BackfillDdl<S>,
    job: &Job,
) -> Result<tidb_model::TableInfo, Error> {
    reorg_transaction(
        &ddl.opener,
        ddl.timeout,
        &crate::job_ctx::internal_ddl_source(),
        |meta, _, _, _| {
            let raw = meta
                .get(&tidb_meta::key::table_kv_key(job.schema_id, job.table_id))
                .map_err(DdlPlanError::from)?
                .ok_or_else(|| {
                    ClusterDdlError::Backfill(format!("table {} not found", job.table_id))
                })?;
            let table = serde_json::from_slice(&raw)
                .map_err(|e| ClusterDdlError::Backfill(e.to_string()))?;
            Ok((table, Vec::new()))
        },
    )
    .map_err(|e| Arc::new(e) as Error)
}
fn reorg_for_task(meta: &BackfillTaskMeta, table: tidb_model::TableInfo) -> AddIndexReorg {
    let physical_ids = table.get_partition_info().map_or_else(
        || vec![table.id],
        |p| {
            p.read()
                .definitions
                .snapshot()
                .iter()
                .map(|d| d.id)
                .collect()
        },
    );
    AddIndexReorg {
        job_id: meta.job.id,
        query: meta.job.query.clone(),
        stage: ReorgStageRequest::Init,
        table: Box::new(table),
        index_ids: meta.ele_ids.clone().unwrap_or_default(),
        merging: meta.merge_temp_index,
        physical_ids,
        batch_size: crate::cluster_ddl::add_index::job_batch_size(&meta.job),
        concurrency: crate::cluster_ddl::add_index::job_concurrency(&meta.job),
        session_vars: tidb_ddl_build::session_pool::ReorgSessionVars::of_job(&meta.job),
        row_count: 0,
        column: None,
        sub_job: None,
        clean_up_global_index: false,
        partition_phase: None,
        schema_id: meta.job.schema_id,
    }
}
/// Go `LitBackfillScheduler` with the framework base.
struct BackfillScheduler<S: StoreClient> {
    base: BaseScheduler,
    ddl: Arc<BackfillDdl<S>>,
    meta: RwLock<Option<BackfillTaskMeta>>,
}
impl<S: StoreClient> Scheduler for BackfillScheduler<S> {
    fn init(&self) -> Result<(), Error> {
        self.base.init()?;
        let meta =
            serde_json::from_slice(&self.base.get_task().meta).map_err(|e| Arc::new(e) as Error)?;
        *self.meta.write().unwrap() = Some(meta);
        Ok(())
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
impl<S: StoreClient> tidb_dxf_scheduler::Extension for BackfillScheduler<S> {
    fn on_tick(&self, _: &Context, _: &Task) {}
    fn on_next_subtasks_batch(
        &self,
        ctx: &Context,
        _: &dyn tidb_dxf_storage::TaskHandle,
        task: &mut Task,
        exec_ids: &[String],
        next_step: Step,
    ) -> Result<Vec<Vec<u8>>, Error> {
        if next_step == STEP_DONE {
            return Ok(Vec::new());
        }
        let meta: BackfillTaskMeta =
            serde_json::from_slice(&task.meta).map_err(|e| Arc::new(e) as Error)?;
        let table = get_tbl_info(&self.ddl, &meta.job)?;
        let reorg = reorg_for_task(&meta, table);
        match next_step {
            BACKFILL_STEP_READ_INDEX => {
                generate_read_index_plan(ctx, &self.ddl, &meta, &reorg, exec_ids.len())
            }
            BACKFILL_STEP_MERGE_TEMP_INDEX => {
                generate_merge_temp_index_plan(ctx, &self.ddl, &reorg, exec_ids.len())
            }
            _ => Err(plain(format!(
                "unknown backfill planning step {}",
                next_step.0
            ))),
        }
    }
    fn on_done(
        &self,
        _: &Context,
        _: &dyn tidb_dxf_storage::TaskHandle,
        _: &mut Task,
    ) -> Result<(), Error> {
        Ok(())
    }
    fn get_eligible_instances(&self, _: &Context, _: &Task) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }
    fn is_retryable_err(&self, _: &(dyn std::error::Error + 'static)) -> bool {
        true
    }
    fn get_next_step(&self, task: &TaskBase) -> Step {
        let guard = self.meta.read().unwrap();
        let meta = guard.as_ref().expect("initialized backfill scheduler");
        LitBackfillScheduler {
            global_sort: !meta.cloud_storage_uri.is_empty(),
            merge_temp_index: meta.merge_temp_index,
        }
        .get_next_step(task)
    }
    fn on_prepare(
        &self,
        _: &Context,
        _: &dyn tidb_dxf_storage::TaskHandle,
        _: &mut Task,
    ) -> Result<(), Error> {
        Ok(())
    }
    fn modify_meta(&self, old: &[u8], modifies: &[Modification]) -> Result<Vec<u8>, Error> {
        let meta: BackfillTaskMeta =
            serde_json::from_slice(old).map_err(|e| Arc::new(e) as Error)?;
        if let Some(reorg) = &meta.job.reorg_meta {
            for modify in modifies {
                if modify.tp == MODIFY_BATCH_SIZE {
                    reorg.read().set_batch_size(modify.to);
                } else if modify.tp == MODIFY_MAX_WRITE_SPEED {
                    reorg.read().set_max_write_speed(modify.to);
                }
            }
        }
        serde_json::to_vec(&meta).map_err(|e| Arc::new(e) as Error)
    }
}
/// Go `generateReadIndexPlan` / `generatePlanForPhysicalTable`.
fn generate_read_index_plan<S: StoreClient>(
    ctx: &Context,
    ddl: &BackfillDdl<S>,
    meta: &BackfillTaskMeta,
    reorg: &AddIndexReorg,
    node_count: usize,
) -> Result<Vec<Vec<u8>>, Error> {
    let backfiller = (ddl.backfiller)();
    let mut metas = Vec::new();
    for id in &reorg.physical_ids {
        let range = reorg_transaction(
            &ddl.opener,
            ddl.timeout,
            &crate::job_ctx::backfill_request_source(reorg.job_id),
            |_, snapshot, buffer, _| {
                Ok((
                    table_range(backfiller.as_ref(), reorg, *id, snapshot, buffer)?,
                    Vec::new(),
                ))
            },
        )
        .map_err(|e| Arc::new(e) as Error)?
        .map_err(|e| plain(e.message))?;
        let Some((start, end)) = range else {
            continue;
        };
        let mut emitted = false;
        for attempt in 0..8 {
            if let Some(error) = ctx.err() {
                return Err(Arc::new(error));
            }
            let mut ranges = match backfiller.region_store() {
                Some(store) => store
                    .load_regions_in_key_range(&start, &end)
                    .map_err(plain)?
                    .into_iter()
                    .map(|r| (r.start_key, r.end_key))
                    .collect::<Vec<_>>(),
                None => vec![(start.clone(), end.clone())],
            };
            ranges.sort_by(|a, b| a.0.cmp(&b.0));
            if ranges.is_empty() || ranges.windows(2).any(|pair| pair[0].1 != pair[1].0) {
                ctx.sleep(Duration::from_millis((200u64 << attempt).min(2000)));
                continue;
            }
            let batch =
                calculate_region_batch(ranges.len(), node_count, meta.cloud_storage_uri.is_empty());
            let count = ranges.len();
            let mut subtasks = Vec::new();
            let mut retry = false;
            for (offset, range) in ranges.chunks(batch).enumerate() {
                let ts = match ddl.opener.store().get_timestamp_async().wait() {
                    Ok(ts) => ts,
                    Err(_) => {
                        retry = true;
                        break;
                    }
                };
                let sm = BackfillSubTaskMeta {
                    physical_table_id: *id,
                    row_start: if offset == 0 {
                        start.clone()
                    } else {
                        range[0].0.clone()
                    },
                    row_end: if offset * batch + range.len() == count {
                        end.clone()
                    } else {
                        range.last().unwrap().1.clone()
                    },
                    ts,
                    ..Default::default()
                };
                subtasks.push(sm.marshal().map_err(|e| Arc::new(e) as Error)?);
            }
            if retry {
                ctx.sleep(Duration::from_millis((200u64 << attempt).min(2000)));
                continue;
            }
            metas.extend(subtasks);
            emitted = true;
            break;
        }
        if !emitted {
            return Err(plain("regions are not continuous"));
        }
    }
    Ok(metas)
}

/// Go `encodeTempIndexRange(physicalID, idxID, idxID)`.
fn encode_temp_index_range(physical_id: i64, index_id: i64) -> (Vec<u8>, Vec<u8>) {
    let temp_id = tidb_tablecodec::table_key::TEMP_INDEX_PREFIX | index_id;
    (
        tidb_tablecodec::table_key::encode_index_seek_key(physical_id, temp_id, &[]),
        tidb_tablecodec::table_key::encode_index_seek_key(physical_id, temp_id, &[255]),
    )
}

/// Go `calculateTempIndexRegionBatch`.
fn calculate_temp_index_region_batch(total_region_cnt: usize, node_cnt: usize) -> usize {
    total_region_cnt.div_ceil(node_cnt.max(1)).max(1)
}

/// Go `generateMergeTempIndexPlan`: each index's temporary range -- per
/// partition, except for a global index -- split into region batches.
fn generate_merge_temp_index_plan<S: StoreClient>(
    ctx: &Context,
    ddl: &BackfillDdl<S>,
    reorg: &AddIndexReorg,
    node_count: usize,
) -> Result<Vec<Vec<u8>>, Error> {
    let backfiller = (ddl.backfiller)();
    let mut all_meta = Vec::new();
    for id in &reorg.index_ids {
        let index = reorg
            .table
            .indices
            .iter_deref()
            .find(|index| index.read().id == *id)
            .ok_or_else(|| plain(format!("index ID {id} not found")))?;
        let global = index.read().global;
        let physical_ids: Vec<i64> = match reorg.table.get_partition_info() {
            Some(partition) if !global => {
                partition.read().definitions.snapshot().iter().map(|def| def.id).collect()
            }
            _ => vec![reorg.table.id],
        };
        for physical_id in physical_ids {
            all_meta.extend(gen_merge_temp_plan_for_one_index(
                ctx,
                backfiller.as_ref(),
                physical_id,
                *id,
                node_count,
            )?);
        }
    }
    Ok(all_meta)
}

/// Go `genMergeTempPlanForOneIndex`.
fn gen_merge_temp_plan_for_one_index(
    ctx: &Context,
    backfiller: &dyn IndexBackfiller,
    physical_id: i64,
    index_id: i64,
    node_count: usize,
) -> Result<Vec<Vec<u8>>, Error> {
    let (start, end) = encode_temp_index_range(physical_id, index_id);
    for attempt in 0..8 {
        if let Some(error) = ctx.err() {
            return Err(Arc::new(error));
        }
        let mut regions = match backfiller.region_store() {
            Some(store) => store
                .load_regions_in_key_range(&start, &end)
                .map_err(plain)?
                .into_iter()
                .map(|region| (region.start_key, region.end_key))
                .collect::<Vec<_>>(),
            None => vec![(start.clone(), end.clone())],
        };
        regions.sort_by(|a, b| a.0.cmp(&b.0));
        if regions.is_empty() || regions.windows(2).any(|pair| pair[0].1 != pair[1].0) {
            ctx.sleep(Duration::from_millis((200u64 << attempt).min(2000)));
            continue;
        }
        let region_batch = calculate_temp_index_region_batch(regions.len(), node_count);
        tidb_ddl_logutil::ddl_logger().info(
            "calculate temp index region batch",
            &[
                tidb_log::Field::new("physicalTableID", tidb_log::Value::I64(physical_id)),
                tidb_log::Field::new("totalRegionCnt", tidb_log::Value::I64(regions.len() as i64)),
                tidb_log::Field::new("regionBatch", tidb_log::Value::I64(region_batch as i64)),
                tidb_log::Field::new("instanceCnt", tidb_log::Value::I64(node_count as i64)),
            ],
        );
        let mut metas = Vec::new();
        let mut i = 0;
        while i < regions.len() {
            let end_idx = (i + region_batch).min(regions.len());
            let batch = &regions[i..end_idx];
            let mut meta = BackfillSubTaskMeta {
                physical_table_id: physical_id,
                ..Default::default()
            };
            meta.sorted_kv_meta.start_key =
                if i == 0 { start.clone() } else { batch[0].0.clone() };
            meta.sorted_kv_meta.end_key = if end_idx == regions.len() {
                end.clone()
            } else {
                batch[batch.len() - 1].1.clone()
            };
            metas.push(meta.marshal().map_err(|e| Arc::new(e) as Error)?);
            i = end_idx;
        }
        return Ok(metas);
    }
    Err(plain("regions are not continuous"))
}

/// Go `mergeTempIndexExecutor`: each subtask merges one index's temporary
/// range in batches, as the owner's merge does.
struct MergeTempIndexStepExecutor<S: StoreClient> {
    info: FrameworkInfoSlot,
    ddl: Arc<BackfillDdl<S>>,
    table: tidb_model::TableInfo,
    meta: RwLock<BackfillTaskMeta>,
    summary: Arc<SubtaskSummary>,
}
impl<S: StoreClient> StepExecFrameworkInfo for MergeTempIndexStepExecutor<S> {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.info
    }
}
impl<S: StoreClient> StepExecutor for MergeTempIndexStepExecutor<S> {
    fn init(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        tidb_logutil::bg_logger().info("merge temp index executor init subtask exec env", &[]);
        Ok(())
    }
    /// Go `mergeTempIndexExecutor.RunSubtask`.
    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> tidb_dxf_execute::Result<()> {
        self.summary.reset();
        let meta = self.meta.read().unwrap().clone();
        let sm = decode_backfill_sub_task_meta(&subtask.meta, None)?;
        // Go `initializeByMeta`: the index and physical table from the key.
        let start = sm.sorted_kv_meta.start_key.clone();
        let end = sm.sorted_kv_meta.end_key.clone();
        let physical_id = tidb_tablecodec::table_key::decode_table_id(&start);
        let index_id = tidb_tablecodec::table_key::decode_index_id(&start)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as tidb_dxf_execute::Error)?
            & tidb_tablecodec::INDEX_ID_MASK;
        if !self.table.indices.iter_deref().any(|index| index.read().id == index_id) {
            return Err(Box::new(std::io::Error::other(format!("index (id={index_id}) not found"))));
        }
        let mut reorg = reorg_for_task(&meta, self.table.clone_like_go());
        reorg.merging = true;
        let backfiller = (self.ddl.backfiller)();
        let check_owner = || ctx.err().map_or(Ok(()), |e| Err(e.to_string()));
        let stopped = AtomicBool::new(false);
        let mut next = start;
        loop {
            match super::run_batch(
                &self.ddl.opener,
                self.ddl.timeout,
                backfiller.as_ref(),
                &reorg,
                physical_id,
                &next,
                &end,
                reorg.batch_size,
                Some(index_id),
                &check_owner,
                &stopped,
            )
            .map_err(|e| Box::new(e) as tidb_dxf_execute::Error)?
            {
                BatchStep::Advanced { next_key, done, added, .. } => {
                    self.summary.row_cnt.fetch_add(added as i64, Ordering::SeqCst);
                    if done {
                        break;
                    }
                    next = next_key;
                }
                BatchStep::Interrupted => {
                    return Err(Box::new(tidb_gocontext::CtxError::Canceled));
                }
                BatchStep::Failed(error) => {
                    return Err(Box::new(tidb_terror::MysqlError::sql_error(
                        error.code,
                        error.message,
                    )));
                }
            }
        }
        tidb_logutil::bg_logger().info(
            "merge temp index executor finish subtask",
            &[tidb_log::Field::new(
                "added",
                tidb_log::Value::I64(self.summary.row_cnt.load(Ordering::SeqCst)),
            )],
        );
        Ok(())
    }
    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        Some(self.summary.clone())
    }
    fn reset_summary(&self) {
        self.summary.reset();
    }
    fn cleanup(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        Ok(())
    }
    /// Go: "Will be added in the future PR".
    fn task_meta_modified(&self, _: &Context, _: &[u8]) -> tidb_dxf_execute::Result<()> {
        Ok(())
    }
    /// Go: "Will be added in the future PR".
    fn resource_modified(&self, _: &Context, _: &StepResource) -> tidb_dxf_execute::Result<()> {
        Ok(())
    }
}

/// Go `checkDuplicateForUniqueIndex`: after the DXF read step, each unique
/// index's remote duplicates through a duplicate-check backend.
fn check_duplicate_for_unique_index<S: StoreClient>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    reorg: &AddIndexReorg,
    job: &Job,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ctx = Context::background();
    let mut backend = None;
    let mut backend_ctx: Option<Arc<dyn BackendCtx>> = None;
    let result = (|| {
        for id in &reorg.index_ids {
            let index = reorg
                .table
                .indices
                .iter_deref()
                .find(|index| index.read().id == *id)
                .ok_or_else(|| std::io::Error::other("unexpected error, can't find index info"))?;
            if !index.read().unique {
                continue;
            }
            if backend_ctx.is_none() {
                if tidb_config::config_tree::config::get_global_config().store.0
                    == tidb_config::store::STORE_TYPE_TIKV
                {
                    backend = Some(create_local_backend(
                        opener.store().clone(),
                        job,
                        true,
                        true,
                        crate::cluster_ddl::add_index::job_concurrency(job),
                        0,
                    )?);
                }
                backend_ctx = Some(
                    BackendCtxBuilder::new(ctx.clone(), opener.store().clone(), job.clone())
                        .for_duplicate_check()
                        .build(backend.clone())?,
                );
            }
            if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockCheckDuplicateForUniqueIndexError").is_some() {
                return Err(Box::new(tidb_gocontext::CtxError::DeadlineExceeded) as Box<dyn std::error::Error + Send + Sync>);
            }
            backend_ctx
                .as_ref()
                .expect("built above")
                .collect_remote_duplicate_rows(&ctx, *id, &reorg.table)?;
        }
        Ok(())
    })();
    if let Some(backend_ctx) = backend_ctx {
        backend_ctx.close();
    }
    if let Some(backend) = backend {
        let _ = backend.close();
    }
    result
}

/// Go `backfillDistExecutor`.
struct BackfillDistExecutor<S: StoreClient> {
    base: BaseTaskExecutor,
    task: Task,
    ddl: Arc<BackfillDdl<S>>,
    meta: RwLock<Option<BackfillTaskMeta>>,
}
impl<S: StoreClient> TaskExecutor for BackfillDistExecutor<S> {
    fn init(&self, ctx: &Context) -> Result<(), Error> {
        self.base.init(ctx)?;
        *self.meta.write().unwrap() =
            Some(serde_json::from_slice(&self.task.meta).map_err(|e| Arc::new(e) as Error)?);
        Ok(())
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
impl<S: StoreClient> tidb_dxf_taskexecutor::Extension for BackfillDistExecutor<S> {
    fn is_idempotent(&self, _: &Subtask) -> bool {
        true
    }
    fn get_step_executor(&self, task: &Task) -> Result<Arc<dyn StepExecutor>, Error> {
        let meta: BackfillTaskMeta =
            serde_json::from_slice(&task.meta).map_err(|e| Arc::new(e) as Error)?;
        let table = get_tbl_info(&self.ddl, &meta.job)?;
        for id in meta.ele_ids.as_deref().unwrap_or_default() {
            if !table.indices.iter_deref().any(|i| i.read().id == *id) {
                return Err(Arc::new(IndexInfoNotFound {
                    index_id: *id,
                    table_id: table.id,
                    job_id: meta.job.id,
                }));
            }
        }
        match task.base.step {
            BACKFILL_STEP_READ_INDEX => {
                crate::job_ctx::attach_top_profiling_info(
                    meta.job.id,
                    &meta.job.query.to_utf8_lossy_go(),
                );
                crate::job_ctx::set_ddl_source_for_diagnosis(meta.job.id, meta.job.type_);
                Ok(Arc::new(ReadIndexStepExecutor {
                    info: FrameworkInfoSlot::default(),
                    ddl: self.ddl.clone(),
                    table,
                    meta: RwLock::new(meta),
                    backend: Mutex::new(None),
                    summary: Arc::new(SubtaskSummary::default()),
                }))
            }
            BACKFILL_STEP_MERGE_TEMP_INDEX => Ok(Arc::new(MergeTempIndexStepExecutor {
                info: FrameworkInfoSlot::default(),
                ddl: self.ddl.clone(),
                table,
                meta: RwLock::new(meta),
                summary: Arc::new(SubtaskSummary::default()),
            })),
            BACKFILL_STEP_WRITE_AND_INGEST if meta.cloud_storage_uri.is_empty() => {
                Err(plain("local import does not have write & ingest step"))
            }
            _ => Err(plain(format!(
                "unknown backfill step {} for task {}",
                task.base.step.0, task.base.id
            ))),
        }
    }
    fn is_retryable_error(&self, e: &(dyn std::error::Error + 'static)) -> bool {
        if e.downcast_ref::<IndexInfoNotFound>().is_some() {
            return false;
        }
        tidb_lightning_common::is_retryable_error(e) || crate::index::is_retryable_error(e, true)
    }
}
#[derive(Debug)]
struct IndexInfoNotFound {
    index_id: i64,
    table_id: i64,
    job_id: i64,
}
impl std::fmt::Display for IndexInfoNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "eid: {}, table ID: {}, job ID: {}: index info not found",
            self.index_id, self.table_id, self.job_id
        )
    }
}
impl std::error::Error for IndexInfoNotFound {}

/// Go `readIndexStepExecutor`.
struct ReadIndexStepExecutor<S: StoreClient> {
    info: FrameworkInfoSlot,
    ddl: Arc<BackfillDdl<S>>,
    table: tidb_model::TableInfo,
    meta: RwLock<BackfillTaskMeta>,
    backend: Mutex<Option<Arc<tidb_ingestor_ingestctrl::local::Backend>>>,
    summary: Arc<SubtaskSummary>,
}
impl<S: StoreClient> StepExecFrameworkInfo for ReadIndexStepExecutor<S> {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.info
    }
}
impl<S: StoreClient> StepExecutor for ReadIndexStepExecutor<S> {
    fn init(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        let meta = self.meta.read().unwrap();
        if tidb_config::config_tree::config::get_global_config()
            .store
            .0
            == tidb_config::store::STORE_TYPE_TIKV
        {
            let unique = self.table.indices.iter_deref().any(|i| {
                meta.ele_ids
                    .as_deref()
                    .unwrap_or_default()
                    .contains(&i.read().id)
                    && i.read().unique
            });
            *self.backend.lock().unwrap() = Some(create_local_backend(
                self.ddl.opener.store().clone(),
                &meta.job,
                unique,
                false,
                crate::cluster_ddl::add_index::job_concurrency(&meta.job),
                0,
            )?);
        }
        Ok(())
    }
    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> tidb_dxf_execute::Result<()> {
        self.summary.reset();
        let meta = self.meta.read().unwrap().clone();
        let sm = decode_backfill_sub_task_meta(&subtask.meta, None)?;
        let mut reorg = reorg_for_task(&meta, self.table.clone_like_go());
        reorg.concurrency = self.get_resource().cpu.capacity() as usize;
        let get = self.get_checkpoint_func();
        let update = self.get_checkpoint_update_func();
        let get_ctx = ctx.clone();
        let update_ctx = ctx.clone();
        let checkpoint = tidb_ddl_ingest::checkpoint::DistTaskCheckpointStorage {
            subtask_id: subtask.base.id,
            get_func: get.map(|get| {
                Arc::new(move |id| {
                    get(&get_ctx, id).map_err(|e| tidb_ddl_ingest::Error::Other(e.to_string()))
                }) as _
            }),
            update_func: update.map(|update| {
                Arc::new(
                    move |id, cp: &tidb_ddl_ingest::checkpoint::ReorgCheckpoint| {
                        update(&update_ctx, id, &serde_json::to_value(cp)?)
                            .map_err(|e| tidb_ddl_ingest::Error::Other(e.to_string()))
                    },
                ) as _
            }),
        };
        let mut builder = BackendCtxBuilder::new(
            ctx.clone(),
            self.ddl.opener.store().clone(),
            meta.job.clone(),
        )
        .with_dist_task_checkpoint_manager_param(checkpoint, sm.physical_table_id);
        if let Some(etcd) = &self.ddl.etcd {
            let etcd = etcd.clone();
            builder = builder.with_import_distributed_lock(
                Arc::new(move |id| {
                    tidb_owner::acquire_distributed_lock(
                        &tidb_owner::Context::background(),
                        etcd.clone(),
                        &format!("/tidb/distributeLock/{id}"),
                        10,
                    )
                    .map_err(tidb_ddl_ingest::Error::Other)
                }),
                sm.ts,
            );
        }
        let bc = builder.build(self.backend.lock().unwrap().clone())?;
        let result = (|| {
            let uniques = reorg
                .index_ids
                .iter()
                .map(|id| {
                    self.table
                        .indices
                        .iter_deref()
                        .find(|i| i.read().id == *id)
                        .unwrap()
                        .read()
                        .unique
                })
                .collect::<Vec<_>>();
            let engines = bc.register(&reorg.index_ids, &uniques, &self.table)?;
            let backfiller = (self.ddl.backfiller)();
            let check = || ctx.err().map_or(Ok(()), |e| Err(e.to_string()));
            let stopped = AtomicBool::new(false);
            let mut warnings = Vec::new();
            let result = super::ingest::execute_and_close_pipeline(
                &self.ddl.opener,
                self.ddl.timeout,
                backfiller.as_ref(),
                &reorg,
                sm.physical_table_id,
                &bc.next_start_key().max(sm.row_start),
                &sm.row_end,
                &bc,
                meta.job.reorg_meta.as_ref(),
                &engines,
                &check,
                &stopped,
                &mut warnings,
            );
            match result {
                Ok(()) => bc
                    .finish_and_unregister_engines(
                        ctx,
                        UnregisterOpt(UnregisterOpt::CLEAN_DATA.0 | UnregisterOpt::CHECK_DUP.0),
                    )
                    .map_err(|e| Box::new(e) as tidb_dxf_execute::Error),
                Err(error) => {
                    let _ = bc.finish_and_unregister_engines(ctx, UnregisterOpt::CLEAN_DATA);
                    Err(pipeline_error(error))
                }
            }
        })();
        self.summary
            .row_cnt
            .store(bc.total_key_count(), Ordering::SeqCst);
        bc.close();
        result
    }
    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        Some(self.summary.clone())
    }
    fn reset_summary(&self) {
        self.summary.reset();
    }
    fn cleanup(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        if let Some(backend) = self.backend.lock().unwrap().take() {
            backend.close()?;
        }
        Ok(())
    }
    fn task_meta_modified(&self, _: &Context, new: &[u8]) -> tidb_dxf_execute::Result<()> {
        *self.meta.write().unwrap() = serde_json::from_slice(new)?;
        Ok(())
    }
    fn resource_modified(&self, _: &Context, _: &StepResource) -> tidb_dxf_execute::Result<()> {
        Err(Box::new(std::io::Error::other("no subtask running")))
    }
}
fn pipeline_error(error: super::ingest::PipelineError) -> tidb_dxf_execute::Error {
    match error {
        // Go hands DXF the `*errors.Error` itself (its message, not the
        // `[class:code]` text, is what `serializeErr` keeps).
        super::ingest::PipelineError::Ingest(tidb_ddl_ingest::Error::Database(e)) => Box::new(e),
        super::ingest::PipelineError::Ingest(e) => Box::new(e),
        super::ingest::PipelineError::Storage(e) => Box::new(e),
        super::ingest::PipelineError::Sql(e) => {
            Box::new(tidb_terror::MysqlError::sql_error(e.code, e.message))
        }
        super::ingest::PipelineError::Interrupted => Box::new(tidb_gocontext::CtxError::Canceled),
    }
}

/// Go `worker.executeDistTask`, waiting on DXF while forwarding job controls.
pub(super) fn execute_dist_task<S: StoreClient>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    reorg: &AddIndexReorg,
    job: &Job,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    step_cancelled: &AtomicBool,
) -> Result<ReorgResult, ClusterDdlError> {
    let ctx = Context::background();
    let builder = crate::index::TaskKeyBuilder::new().set_merge_temp_index(reorg.merging);
    let key = match &job.multi_schema_info {
        Some(m) => builder.set_multi_schema(Some(&m.read())).build(job.id),
        None => builder.build(job.id),
    };
    let manager = tidb_dxf_storage::get_dxf_svc_task_mgr()
        .map_err(|e| ClusterDdlError::Backfill(e.to_string()))?;
    let task = match manager.get_task_by_key_with_history(&ctx, &key) {
        Ok(task) => {
            if crate::index::should_auto_pause_existing_kv_disk_full_task(job, &task) {
                return Ok(auto_pause_result(&task));
            }
            if task.base.state != TASK_STATE_SUCCEED {
                tidb_dxf_handle::resume_task(&ctx, &key)
                    .map_err(|e| ClusterDdlError::Backfill(e.to_string()))?;
            }
            task
        }
        Err(error) if error.is(&tidb_dxf_storage::StorageError::TaskNotFound) => {
            let required_slots = (reorg.concurrency as isize).min(
                manager
                    .get_cpu_count_of_node(&ctx)
                    .map_err(|e| ClusterDdlError::Backfill(e.to_string()))?,
            );
            let meta = BackfillTaskMeta {
                job: job.clone(),
                ele_ids: Some(reorg.index_ids.clone()),
                ele_type_key: INDEX_ELEMENT_KEY.to_vec(),
                merge_temp_index: reorg.merging,
                version: crate::backfilling_dist_executor::BACKFILL_TASK_META_VERSION_1,
                ..Default::default()
            };
            let config = job.reorg_meta.as_ref().unwrap().read();
            tidb_dxf_handle::submit_task_with_extra_params(
                &ctx,
                &key,
                &BACKFILL,
                opener.store().get_keyspace(),
                required_slots,
                &config.target_scope.to_utf8_lossy_go(),
                config.max_node_count as isize,
                &ExtraParams {
                    pause_on_kv_disk_full: true,
                    ..Default::default()
                },
                &serde_json::to_vec(&meta).map_err(|e| ClusterDdlError::Backfill(e.to_string()))?,
            )
            .map_err(|e| ClusterDdlError::Backfill(e.to_string()))?
        }
        Err(error) => return Err(ClusterDdlError::Backfill(error.to_string())),
    };
    loop {
        if !reorg_runnable(step_cancelled, check_owner)? {
            if let Some(job) = latest_job(sess_pool, reorg.job_id)? {
                if job.state == JobState::PAUSING || job.state == JobState::PAUSED {
                    let _ = tidb_dxf_handle::pause_task(&ctx, &key);
                } else if job.state == JobState::CANCELLING {
                    let _ = tidb_dxf_handle::cancel_task(&ctx, &key);
                }
            }
            return Ok(ReorgResult::Interrupted);
        }
        let current = manager
            .get_task_by_id_with_history(&ctx, task.base.id)
            .map_err(|e| ClusterDdlError::Backfill(e.to_string()))?;
        if current.base.state == TASK_STATE_PAUSED
            && current
                .error
                .as_ref()
                .is_some_and(|error| tidb_ingestor_errdef::is_kv_disk_full_error(error.as_ref()))
        {
            return Ok(auto_pause_result(&current));
        }
        if current.base.is_done() || current.base.state == TASK_STATE_PAUSED {
            let rows = manager
                .get_subtask_row_count(&ctx, task.base.id, BACKFILL_STEP_READ_INDEX)
                .map_err(|e| ClusterDdlError::Backfill(e.to_string()))?;
            let found = tidb_dxf_handle::wait_task_done_or_paused_with_result(&ctx, task.base.id);
            match found {
                Ok(_) => {
                    // Go `addTableIndex`: after the read step (not under
                    // global sort, not the temp-index merge) a unique index's
                    // remote duplicates fail the job.
                    let cloud = job.reorg_meta.as_ref().is_some_and(|m| m.read().use_cloud_storage);
                    if !cloud && !reorg.merging {
                        if let Err(error) = check_duplicate_for_unique_index(opener, reorg, job) {
                            let mut cause = error.as_ref() as &(dyn std::error::Error + 'static);
                            while let Some(source) = cause.source() {
                                cause = source;
                            }
                            if let Some(error) = cause.downcast_ref::<tidb_terror::TerrorError>() {
                                let error = tidb_terror::MysqlError::from_terror(error);
                                return Ok(ReorgResult::Failed { code: error.code, message: error.message, rows });
                            }
                            return Err(ClusterDdlError::Backfill(error.to_string()));
                        }
                    }
                    return Ok(ReorgResult::Done(rows));
                }
                Err(tidb_dxf_handle::HandleError::Task(error)) => {
                    let mut cause = error.as_ref() as &(dyn std::error::Error + 'static);
                    while let Some(source) = cause.source() {
                        cause = source;
                    }
                    if let Some(error) = cause.downcast_ref::<tidb_terror::TerrorError>() {
                        let error = tidb_terror::MysqlError::from_terror(error);
                        return Ok(ReorgResult::Failed {
                            code: error.code,
                            message: error.message,
                            rows,
                        });
                    }
                    if let Some(error) = cause.downcast_ref::<tidb_terror::MysqlError>() {
                        return Ok(ReorgResult::Failed {
                            code: error.code,
                            message: error.message.clone(),
                            rows,
                        });
                    }
                    return Err(ClusterDdlError::Backfill(error.to_string()));
                }
                Err(error) => return Err(ClusterDdlError::Backfill(error.to_string())),
            }
        }
        ctx.sleep(Duration::from_millis(300));
    }
}
fn auto_pause_result(task: &Task) -> ReorgResult {
    ReorgResult::AutoPaused {
        task_id: task.base.id,
        error: task.error.clone(),
    }
}
