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
    /// Go `infosync`'s server infos, which `scheduler.GetLiveExecIDs` reads.
    pub server_info: Arc<dyn tidb_dxf_scheduler::ServerInfoSource>,
}
/// Go `ddl.registerDistTask`'s backfill factories and its
/// `RegisterCleanerFactory(proto.Backfill, newBackfillCleaner)`.
pub(crate) fn register_backfill_task<S: StoreClient>(ddl: Arc<BackfillDdl<S>>) {
    tidb_dxf_scheduler::register_cleaner_factory(
        BACKFILL,
        Arc::new(|| Arc::new(BackfillCleaner) as Arc<dyn tidb_dxf_scheduler::Cleaner>),
    );
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
                    ClusterDdlError::backfill_message(format!("table {} not found", job.table_id))
                })?;
            let table = serde_json::from_slice(&raw)
                .map_err(|e| ClusterDdlError::backfill(&e))?;
            Ok((table, Vec::new()))
        },
    )
    .map_err(|e| Arc::new(e) as Error)
}
pub(super) fn reorg_for_task(meta: &BackfillTaskMeta, table: tidb_model::TableInfo) -> AddIndexReorg {
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
        task_handle: &dyn tidb_dxf_storage::TaskHandle,
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
            BACKFILL_STEP_MERGE_SORT => {
                let metas = generate_merge_sort_plan(ctx, task_handle, task, exec_ids.len(), &meta.cloud_storage_uri)?;
                Ok(metas)
            }
            BACKFILL_STEP_WRITE_AND_INGEST if !meta.cloud_storage_uri.is_empty() => {
                let (metas, total_kv_size) =
                    generate_global_sort_ingest_plan(ctx, &self.ddl, task_handle, task, &meta.cloud_storage_uri)?;
                let mut meta = meta;
                meta.summary = Some(crate::backfilling_dist_executor::BackfillTaskSummary {
                    index_kv_size: total_kv_size,
                    ..Default::default()
                });
                task.meta = serde_json::to_vec(&meta).map_err(|e| Arc::new(e) as Error)?;
                Ok(metas)
            }
            BACKFILL_STEP_WRITE_AND_INGEST => Ok(Vec::new()),
            BACKFILL_STEP_MERGE_TEMP_INDEX => {
                generate_merge_temp_index_plan(ctx, &self.ddl, &reorg, exec_ids.len())
            }
            _ => Ok(Vec::new()),
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
    /// Go `LitBackfillScheduler.IsRetryableErr`.
    fn is_retryable_err(&self, error: &(dyn std::error::Error + 'static)) -> bool {
        !tidb_ingestor_errdef::ERR_TOO_MANY_DATA_FILES.equal(Some(error))
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

fn boxed(error: Box<dyn std::error::Error + Send + Sync>) -> Error {
    Arc::from(error)
}

/// Go `forEachBackfillSubtaskMeta`: each subtask meta of `step`, with its
/// external fields read from `ext_store`.
fn for_each_backfill_subtask_meta(
    ctx: &Context,
    ext_store: &dyn tidb_objstore::storeapi::Storage,
    task_handle: &dyn tidb_dxf_storage::TaskHandle,
    task_id: i64,
    step: Step,
    mut f: impl FnMut(BackfillSubTaskMeta),
) -> Result<(), Error> {
    let metas = task_handle.get_previous_subtask_metas(task_id, step).map_err(|e| Arc::new(e) as Error)?;
    let read = |path: &str| {
        ext_store.read_file(ctx, path).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    };
    for raw in metas {
        let subtask = decode_backfill_sub_task_meta(&raw, Some(&read)).map_err(|error| {
            tidb_ddl_logutil::ddl_logger()
                .error("unmarshal error", &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))]);
            boxed(error)
        })?;
        f(subtask);
    }
    Ok(())
}

/// The merged groups of every subtask: Go's `kvMetaGroups`, `eleIDs` and,
/// for the merge plan, `multiStatsGroup`.
#[derive(Default)]
struct SubtaskGroups {
    kv_meta_groups: Option<Vec<tidb_ingestor_globalsort::SortedKVMeta>>,
    multi_stats_group: Vec<Vec<tidb_ingestor_simplesst::MultipleFilesStat>>,
    ele_ids: Vec<i64>,
}

impl SubtaskGroups {
    fn add(&mut self, subtask: &BackfillSubTaskMeta) {
        let groups = self.kv_meta_groups.get_or_insert_with(|| {
            self.ele_ids = subtask.ele_ids.clone();
            self.multi_stats_group = vec![Vec::new(); subtask.meta_groups.len()];
            vec![tidb_ingestor_globalsort::SortedKVMeta::default(); subtask.meta_groups.len()]
        });
        for (i, group) in subtask.meta_groups.iter().enumerate() {
            if i < groups.len() {
                groups[i].merge(group);
                self.multi_stats_group[i].extend(group.stats().iter().cloned());
            }
        }
    }
}

/// Go `generateMergeSortPlan`: when the read step's files overlap too much,
/// one merge-sort subtask per group of each index's data files.
fn generate_merge_sort_plan(
    ctx: &Context,
    task_handle: &dyn tidb_dxf_storage::TaskHandle,
    task: &Task,
    node_cnt: usize,
    cloud_storage_uri: &str,
) -> Result<Vec<Vec<u8>>, Error> {
    let obj_store = tidb_dxf_handle::new_obj_store(ctx, cloud_storage_uri).map_err(plain)?;
    let mut groups = SubtaskGroups::default();
    for_each_backfill_subtask_meta(ctx, obj_store.as_ref(), task_handle, task.base.id, BACKFILL_STEP_READ_INDEX, |subtask| {
        groups.add(&subtask)
    })?;
    let concurrency = task.base.get_runtime_slots();
    let all_skip = groups
        .multi_stats_group
        .iter()
        .all(|stats| crate::backfilling_dist_scheduler::skip_merge_sort(stats, concurrency as i64));
    if all_skip {
        tidb_ddl_logutil::ddl_logger().info("skip merge sort", &[]);
        return Ok(Vec::new());
    }
    let mut meta_arr = Vec::new();
    for (i, group) in groups.kv_meta_groups.unwrap_or_default().iter().enumerate() {
        let data_files = group.get_data_files();
        let ele_id = groups.ele_ids.get(i).map(|id| vec![*id]).unwrap_or_default();
        let data_files_group =
            tidb_ingestor_globalsort::divide_merge_sort_data_files(&data_files, node_cnt.max(1), concurrency.max(1) as usize)
                .map_err(boxed)?;
        for files in data_files_group {
            meta_arr.push(BackfillSubTaskMeta { data_files: files, ele_ids: ele_id.clone(), ..Default::default() });
        }
    }
    // Write the external meta to storage when using global sort.
    let step = tidb_dxf::step_2_str(&BACKFILL, BACKFILL_STEP_MERGE_SORT);
    for (i, meta) in meta_arr.iter_mut().enumerate() {
        crate::backfilling_dist_executor::write_external_backfill_sub_task_meta(
            ctx,
            obj_store.as_ref(),
            meta,
            tidb_ingestor_globalsort::plan_meta_path(task.base.id, &step, i + 1),
        )
        .map_err(boxed)?;
    }
    meta_arr.iter().map(|meta| meta.marshal().map_err(|e| Arc::new(e) as Error)).collect()
}

/// Go `allocNewTS`.
fn alloc_new_ts<S: StoreClient>(ddl: &BackfillDdl<S>) -> Result<u64, Error> {
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockAllocNewTSError") {
        if value.bool() {
            return Err(plain("mock alloc new TS error"));
        }
    }
    ddl.opener.store().get_timestamp_async().wait().map_err(|e| plain(e.to_string()))
}

/// Go `generateGlobalSortIngestPlan`: the merged groups of the merge-sort
/// step (or of the read step when it was skipped), each split into
/// write-and-ingest subtasks. Answers the subtasks and the total KV size.
fn generate_global_sort_ingest_plan<S: StoreClient>(
    ctx: &Context,
    ddl: &BackfillDdl<S>,
    task_handle: &dyn tidb_dxf_storage::TaskHandle,
    task: &Task,
    cloud_storage_uri: &str,
) -> Result<(Vec<Vec<u8>>, u64), Error> {
    let obj_store = tidb_dxf_handle::new_obj_store(ctx, cloud_storage_uri).map_err(plain)?;
    let mut groups = SubtaskGroups::default();
    for step in [BACKFILL_STEP_MERGE_SORT, BACKFILL_STEP_READ_INDEX] {
        let mut has_subtasks = false;
        for_each_backfill_subtask_meta(ctx, obj_store.as_ref(), task_handle, task.base.id, step, |subtask| {
            has_subtasks = true;
            groups.add(&subtask);
        })?;
        if has_subtasks {
            break;
        }
        // No subtask of the merge sort step: it was skipped.
    }
    let kv_meta_groups = groups.kv_meta_groups.unwrap_or_default();
    let total_kv_size = kv_meta_groups.iter().map(|group| group.total_kv_size).sum();
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockGlobalSortIngestPlanErr").is_some() {
        return Err(plain("mock global-sort ingest planning error"));
    }
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockWriteIngest").is_some() {
        let meta = BackfillSubTaskMeta::default().marshal().map_err(|e| Arc::new(e) as Error)?;
        return Ok((vec![meta], total_kv_size));
    }
    let instance_ids = tidb_dxf_scheduler::get_live_exec_ids(ddl.server_info.as_ref()).map_err(plain)?;
    let instance_cnt = instance_ids.len().max(1) as i64;
    let mut meta_arr = Vec::new();
    for (i, group) in kv_meta_groups.iter().enumerate() {
        // An old TiDB may have written the subtask meta without element IDs.
        let ele_id = groups.ele_ids.get(i).copied().unwrap_or(0);
        meta_arr.extend(split_subtask_meta_for_one_kv_meta_group(
            ctx,
            ddl,
            group,
            ele_id,
            cloud_storage_uri,
            instance_cnt,
        )?);
    }
    let step = tidb_dxf::step_2_str(&BACKFILL, BACKFILL_STEP_WRITE_AND_INGEST);
    for (i, meta) in meta_arr.iter_mut().enumerate() {
        crate::backfilling_dist_executor::write_external_backfill_sub_task_meta(
            ctx,
            obj_store.as_ref(),
            meta,
            tidb_ingestor_globalsort::plan_meta_path(task.base.id, &step, i + 1),
        )
        .map_err(boxed)?;
    }
    let metas = meta_arr.iter().map(|meta| meta.marshal().map_err(|e| Arc::new(e) as Error)).collect::<Result<_, _>>()?;
    Ok((metas, total_kv_size))
}

/// Go `splitSubtaskMetaForOneKVMetaGroup`: one index's sorted data cut into
/// range groups of about `1/instance_cnt` of its size.
fn split_subtask_meta_for_one_kv_meta_group<S: StoreClient>(
    ctx: &Context,
    ddl: &BackfillDdl<S>,
    kv_meta: &tidb_ingestor_globalsort::SortedKVMeta,
    ele_id: i64,
    cloud_storage_uri: &str,
    instance_cnt: i64,
) -> Result<Vec<BackfillSubTaskMeta>, Error> {
    if kv_meta.start_key.is_empty() && kv_meta.end_key.is_empty() {
        // Skip global sort for an empty table.
        return Ok(Vec::new());
    }
    let mut import_ts = alloc_new_ts(ddl)?;
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockTSForGlobalSort") {
        import_ts = value.int() as u64;
    }
    let mut splitter = get_range_splitter(
        ctx,
        ddl,
        cloud_storage_uri,
        kv_meta.total_kv_size as i64,
        instance_cnt,
        kv_meta.stats().to_vec(),
    )?;
    let result = (|| {
        let mut meta_arr = Vec::new();
        let mut start_key = kv_meta.start_key.clone();
        loop {
            let group = splitter.split_one_ranges_group().map_err(boxed)?;
            let end_key =
                if group.end_key_of_group.is_empty() { kv_meta.end_key.clone() } else { group.end_key_of_group.clone() };
            tidb_ddl_logutil::ddl_logger().info(
                "split subtask range",
                &[
                    tidb_log::Field::new("startKey", tidb_log::Value::Str(hex::encode(&start_key))),
                    tidb_log::Field::new("endKey", tidb_log::Value::Str(hex::encode(&end_key))),
                    tidb_log::Field::new("dataFilesCnt", tidb_log::Value::I64(group.data_files.len() as i64)),
                    tidb_log::Field::new("rangeJobKeysCnt", tidb_log::Value::I64(group.interior_range_job_keys.len() as i64)),
                    tidb_log::Field::new("regionSplitKeysCnt", tidb_log::Value::I64(group.interior_region_split_keys.len() as i64)),
                ],
            );
            if start_key >= end_key {
                return Err(plain(format!(
                    "invalid range, startKey: {}, endKey: {}",
                    hex::encode(&start_key),
                    hex::encode(&end_key)
                )));
            }
            let mut range_job_keys = Vec::with_capacity(group.interior_range_job_keys.len() + 2);
            range_job_keys.push(start_key.clone());
            range_job_keys.extend(group.interior_range_job_keys.iter().cloned());
            range_job_keys.push(end_key.clone());
            let mut range_split_keys = Vec::with_capacity(group.interior_region_split_keys.len() + 2);
            range_split_keys.push(start_key.clone());
            range_split_keys.extend(group.interior_region_split_keys.iter().cloned());
            range_split_keys.push(end_key.clone());
            meta_arr.push(BackfillSubTaskMeta {
                meta_groups: vec![tidb_ingestor_globalsort::SortedKVMeta {
                    start_key: start_key.clone(),
                    end_key: end_key.clone(),
                    total_kv_size: kv_meta.total_kv_size / instance_cnt as u64,
                    ..Default::default()
                }],
                data_files: group.data_files,
                stat_files: group.stat_files,
                range_job_keys,
                range_split_keys,
                ts: import_ts,
                ele_ids: if ele_id > 0 { vec![ele_id] } else { Vec::new() },
                ..Default::default()
            });
            if group.end_key_of_group.is_empty() {
                break;
            }
            start_key = end_key;
        }
        Ok(meta_arr)
    })();
    if let Err(error) = splitter.close() {
        tidb_ddl_logutil::ddl_logger().error(
            "failed to close range splitter",
            &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
        );
    }
    result
}

/// Go `getRangeSplitter`: range groups of `total_size / instance_cnt`, range
/// jobs of `CalRangeSize`, and TiKV's region split size and keys, at least
/// the defaults.
fn get_range_splitter<S: StoreClient>(
    ctx: &Context,
    ddl: &BackfillDdl<S>,
    cloud_storage_uri: &str,
    total_size: i64,
    instance_cnt: i64,
    multi_file_stat: Vec<tidb_ingestor_simplesst::MultipleFilesStat>,
) -> Result<tidb_ingestor_globalsort::RangeSplitter, Error> {
    let ext_store = tidb_dxf_handle::new_obj_store(ctx, cloud_storage_uri).map_err(plain)?;
    let range_group_size = total_size / instance_cnt;
    let range_group_keys = i64::MAX;
    let (mut region_split_size, mut region_split_keys) = tidb_dxf_handle::get_default_region_split_config();
    match get_region_split_size_keys(ctx, ddl) {
        Ok((size, keys)) => {
            region_split_size = region_split_size.max(size);
            region_split_keys = region_split_keys.max(keys);
        }
        Err(error) => tidb_ddl_logutil::ddl_logger().warn(
            "fail to get region split keys and size",
            &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
        ),
    }
    let node = tidb_dxf_storage::get_node_resource();
    let (range_size, range_keys) = tidb_ingestor_globalsort::cal_range_size(
        node.total_mem / node.total_cpu.max(1) as i64,
        region_split_size,
        region_split_keys,
    );
    tidb_ddl_logutil::ddl_ingest_logger().info(
        "split kv range with split size and keys",
        &[
            tidb_log::Field::new("region-split-size", tidb_log::Value::I64(region_split_size)),
            tidb_log::Field::new("region-split-keys", tidb_log::Value::I64(region_split_keys)),
            tidb_log::Field::new("range-size", tidb_log::Value::I64(range_size)),
            tidb_log::Field::new("range-keys", tidb_log::Value::I64(range_keys)),
        ],
    );
    tidb_ingestor_globalsort::RangeSplitter::new(
        ctx,
        multi_file_stat,
        ext_store,
        range_group_size,
        range_group_keys,
        range_size,
        range_keys,
        region_split_size,
        region_split_keys,
    )
    .map_err(boxed)
}

/// Go `ingestctrl.GetRegionSplitSizeKeys(ctx, pdCli, ingest.NewDDLTLS())`.
fn get_region_split_size_keys<S: StoreClient>(
    ctx: &Context,
    ddl: &BackfillDdl<S>,
) -> Result<(i64, i64), tidb_ingestor_ingestctrl::Error> {
    use tidb_ingestor_ingestctrl::region_job::ImportClient as _;
    let security = tidb_config::config_tree::config::get_global_config().security.clone();
    let security = tidb_pd_client::ClusterSecurity::new(
        security.cluster_ssl_ca,
        security.cluster_ssl_cert,
        security.cluster_ssl_key,
        security.cluster_verify_cn,
    );
    let client = tidb_ingestor_ingestctrl::region_job::RemoteImportClient::new(ddl.opener.store().clone(), security);
    let stores = client.all_stores(ctx)?;
    tidb_ingestor_ingestctrl::local::get_region_split_size_keys(&stores, |host| client.split_conf_from_store(ctx, host))
}

/// Go `BackfillCleaner`: deletes a global-sort task's files under its task
/// ID and redacts the storage URI in the task meta.
struct BackfillCleaner;

impl tidb_dxf_scheduler::Cleaner for BackfillCleaner {
    /// Go `BackfillCleaner.Clean`. A next-gen kernel also sends the metering
    /// data of succeeded tasks here; metering is not ported.
    fn clean(&self, ctx: &Context, task: &mut Task) -> Result<(), Error> {
        let mut task_meta: BackfillTaskMeta = serde_json::from_slice(&task.meta).map_err(|e| Arc::new(e) as Error)?;
        // No cleanup is needed when the task does not use cloud storage.
        if task_meta.cloud_storage_uri.is_empty() {
            return Ok(());
        }
        let logger = tidb_ddl_logutil::ddl_logger();
        let task_id = tidb_log::Field::new("task-id", tidb_log::Value::I64(task.base.id));
        let ext_store = tidb_dxf_handle::new_obj_store(ctx, &task_meta.cloud_storage_uri).map_err(|error| {
            logger.warn(
                "failed to create cloud storage",
                &[task_id.clone(), tidb_log::Field::new("error", tidb_log::Value::Str(error.clone()))],
            );
            plain(error)
        })?;
        let clean = |prefix: &str| {
            tidb_ingestor_globalsort::clean_up_files(ctx, ext_store.as_ref(), &[prefix]).map_err(|error| {
                logger.warn(
                    "cannot cleanup cloud storage files",
                    &[task_id.clone(), tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
                );
                Arc::from(error)
            })
        };
        clean(&task.base.id.to_string())?;
        // For an old task meta version, the job ID is the files' prefix.
        if task_meta.version < crate::backfilling_dist_executor::BACKFILL_TASK_META_VERSION_1 {
            clean(&task_meta.job.id.to_string())?;
        }
        redact_cloud_storage_uri(task, &mut task_meta);
        Ok(())
    }
}

/// Go `redactCloudStorageURI`.
fn redact_cloud_storage_uri(task: &mut Task, origin: &mut BackfillTaskMeta) {
    origin.cloud_storage_uri = tidb_ast::redact_url(&origin.cloud_storage_uri);
    match serde_json::to_vec(origin) {
        Ok(meta) => task.meta = meta,
        Err(error) => tidb_ddl_logutil::ddl_logger().warn(
            "failed to marshal task meta",
            &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
        ),
    }
}

/// Go `getIndexInfoAndID`: the subtask's index, if it names one, and its
/// ID.
fn get_index_info_and_id(
    ele_ids: &[i64],
    indexes: &[tidb_model::IndexInfo],
) -> Result<(Option<tidb_model::IndexInfo>, i64), Error> {
    match ele_ids.len() {
        1 => Ok(indexes
            .iter()
            .find(|index| index.id == ele_ids[0])
            .map_or((None, 0), |index| (Some(index.clone()), index.id))),
        // Maybe this subtask was generated by an old version TiDB.
        0 => Ok((
            (indexes.len() == 1).then(|| indexes[0].clone()),
            indexes.first().map_or(0, |index| index.id),
        )),
        _ => Err(plain(format!("unexpected EleIDs count {ele_ids:?}"))),
    }
}

/// The task's indexes: Go's `indexes` of `getStepExecutor`.
fn task_indexes(table: &tidb_model::TableInfo, meta: &BackfillTaskMeta) -> Vec<tidb_model::IndexInfo> {
    meta.ele_ids
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|id| table.indices.iter_deref().find(|index| index.read().id == *id).map(|index| index.read().clone()))
        .collect()
}

/// Go `handle.NewObjStoreWithRecording` and its deferred merge into the
/// subtask summary and the meter recorder.
struct RecordedObjStore {
    store: Arc<dyn tidb_objstore::storeapi::Storage>,
    rec: Arc<tidb_objstore_recording::AccessStats>,
    summary: Arc<SubtaskSummary>,
    meter_rec: Arc<tidb_dxf_metering::Recorder>,
}

impl RecordedObjStore {
    fn new(
        ctx: &Context,
        uri: &str,
        summary: &Arc<SubtaskSummary>,
        meter_rec: Arc<tidb_dxf_metering::Recorder>,
    ) -> Result<Self, Error> {
        let (rec, store) = tidb_dxf_handle::new_obj_store_with_recording(ctx, uri).map_err(plain)?;
        Ok(Self { store, rec, summary: Arc::clone(summary), meter_rec })
    }
}

impl Drop for RecordedObjStore {
    fn drop(&mut self) {
        self.summary.merge_obj_store_requests(&self.rec.requests);
        self.meter_rec.merge_obj_store_access(&self.rec);
    }
}

fn read_external_with<'a>(
    ctx: &Context,
    store: &'a dyn tidb_objstore::storeapi::Storage,
) -> impl Fn(&str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> + 'a {
    let ctx = ctx.clone();
    move |path| store.read_file(&ctx, path).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
}

/// Go `mergeSortExecutor`.
struct MergeSortStepExecutor<S: StoreClient> {
    info: FrameworkInfoSlot,
    ddl: Arc<BackfillDdl<S>>,
    table: tidb_model::TableInfo,
    indexes: Vec<tidb_model::IndexInfo>,
    cloud_store_uri: String,
    summary: Arc<SubtaskSummary>,
}

impl<S: StoreClient> StepExecFrameworkInfo for MergeSortStepExecutor<S> {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.info
    }
}

impl<S: StoreClient> StepExecutor for MergeSortStepExecutor<S> {
    fn init(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        tidb_ddl_logutil::ddl_logger().info("merge sort executor init subtask exec env", &[]);
        Ok(())
    }

    /// Go `mergeSortExecutor.RunSubtask` and `onFinished`.
    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> tidb_dxf_execute::Result<()> {
        tidb_ddl_logutil::ddl_logger().info("merge sort executor run subtask", &[]);
        let _ = &self.ddl;
        let recorded = RecordedObjStore::new(ctx, &self.cloud_store_uri, &self.summary, self.get_meter_recorder())?;
        let obj_store = Arc::clone(&recorded.store);
        let read = read_external_with(ctx, obj_store.as_ref());
        let mut sm = decode_backfill_sub_task_meta(&subtask.meta, Some(&read))?;
        let sorted = Arc::new(Mutex::new(tidb_ingestor_globalsort::SortedKVMeta::default()));
        let on_writer_close: tidb_ingestor_simplesst::OnWriterCloseFunc = {
            let sorted = Arc::clone(&sorted);
            Arc::new(move |summary| sorted.lock().unwrap_or_else(std::sync::PoisonError::into_inner).merge_summary(summary))
        };
        let prefix = format!("{}/{}", subtask.base.task_id, subtask.base.id);
        let res = self.get_resource();
        let op = tidb_ingestor_globalsort::MergeOperator::new(
            Arc::clone(&obj_store),
            res.memory_per_core(),
            &prefix,
            tidb_ingestor_simplesst::DEFAULT_BLOCK_SIZE,
            on_writer_close,
            Some(Arc::new(tidb_ingestor_globalsort::MergeCollector::new(None))),
            res.cpu.capacity().max(1) as usize,
            true,
            tidb_ingestor_engineapi::OnDuplicateKey::Error,
        );
        let mut result = tidb_ingestor_globalsort::merge_overlapping_files(ctx, &sm.data_files, &op);
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockMergeSortRunSubtaskError").is_some() {
            result = Err(Box::new(std::io::Error::other("context deadline exceeded")));
        }
        if let Err(error) = result {
            if let Ok((Some(index), _)) = get_index_info_and_id(&sm.ele_ids, &self.indexes) {
                if let Some(converted) = tidb_ddl_ingest::util::key_exists_err_of(error.as_ref(), &index, &self.table) {
                    return Err(Box::new(converted));
                }
            }
            return Err(error);
        }
        // Go `onFinished`.
        tidb_ddl_logutil::ddl_logger().info("merge sort finish subtask", &[]);
        sm.meta_groups = vec![std::mem::take(&mut *sorted.lock().unwrap_or_else(std::sync::PoisonError::into_inner))];
        crate::backfilling_dist_executor::write_external_backfill_sub_task_meta(
            ctx,
            obj_store.as_ref(),
            &mut sm,
            tidb_ingestor_globalsort::subtask_meta_path(subtask.base.task_id, subtask.base.id),
        )?;
        subtask.meta = sm.marshal()?;
        Ok(())
    }

    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        Some(self.summary.clone())
    }

    fn reset_summary(&self) {
        self.summary.reset();
    }

    fn cleanup(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        tidb_ddl_logutil::ddl_logger().info("merge cleanup subtask exec env", &[]);
        Ok(())
    }

    fn task_meta_modified(&self, _: &Context, _: &[u8]) -> tidb_dxf_execute::Result<()> {
        Ok(())
    }

    /// Go `mergeSortExecutor.ResourceModified` tunes the running merge
    /// operator's pool; the ported `MergeOperator` has no tunable pool, so
    /// the framework retries until the subtask ends.
    fn resource_modified(&self, _: &Context, _: &StepResource) -> tidb_dxf_execute::Result<()> {
        Err(Box::new(std::io::Error::other("no subtask running")))
    }
}

/// Go `config.SplitRegionSize` and `SplitRegionKeys`.
const SPLIT_REGION_SIZE: u64 = 96 << 20;
const SPLIT_REGION_KEYS: usize = 960_000;

/// Go `cloudImportExecutor`.
struct CloudImportStepExecutor<S: StoreClient> {
    info: FrameworkInfoSlot,
    ddl: Arc<BackfillDdl<S>>,
    job: Job,
    table: tidb_model::TableInfo,
    indexes: Vec<tidb_model::IndexInfo>,
    cloud_store_uri: String,
    backend: Mutex<Option<Arc<tidb_ingestor_ingestctrl::local::Backend>>>,
    summary: Arc<SubtaskSummary>,
}

impl<S: StoreClient> StepExecFrameworkInfo for CloudImportStepExecutor<S> {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.info
    }
}

impl<S: StoreClient> StepExecutor for CloudImportStepExecutor<S> {
    /// Go `cloudImportExecutor.Init`.
    fn init(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        tidb_ddl_logutil::ddl_logger().info("cloud import executor init subtask exec env", &[]);
        let concurrency = self.get_resource().cpu.capacity() as usize;
        *self.backend.lock().unwrap() = Some(create_local_backend(
            self.ddl.opener.store().clone(),
            &self.job,
            self.indexes.iter().any(|index| index.unique),
            false,
            crate::cluster_ddl::add_index::job_concurrency(&self.job),
            concurrency,
        )?);
        Ok(())
    }

    /// Go `cloudImportExecutor.RunSubtask`.
    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> tidb_dxf_execute::Result<()> {
        tidb_ddl_logutil::ddl_logger().info("cloud import executor run subtask", &[]);
        let meter_rec = self.get_meter_recorder();
        let recorded = RecordedObjStore::new(ctx, &self.cloud_store_uri, &self.summary, Arc::clone(&meter_rec))?;
        let obj_store = Arc::clone(&recorded.store);
        let read = read_external_with(ctx, obj_store.as_ref());
        let sm = decode_backfill_sub_task_meta(&subtask.meta, Some(&read))?;
        let local_backend =
            self.backend.lock().unwrap().clone().ok_or_else(|| std::io::Error::other("local backend not found"))?;
        // Go `ingestCollector`: since the region job might be retried, this
        // value might be larger than the total KV size.
        local_backend.set_collector(Some(Arc::new(move |bytes: i64, _rows: i64| {
            meter_rec.inc_cluster_write_bytes(bytes as u64);
        })));
        let (current_idx, _) = get_index_info_and_id(&sm.ele_ids, &self.indexes)?;
        let mut all = tidb_ingestor_globalsort::SortedKVMeta::default();
        for group in &sm.meta_groups {
            all.merge(group);
        }
        // Compatible with an old version's task meta.
        let job_keys = if sm.range_job_keys.is_empty() { sm.range_split_keys.clone() } else { sm.range_job_keys.clone() };
        let ts = if sm.ts == 0 {
            // A subtask meta of an old version TiDB has no TS.
            alloc_new_ts(&self.ddl)?
        } else {
            sm.ts
        };
        let res = self.get_resource();
        let mut engine = tidb_ingestor_globalsort::Engine::new(
            tidb_ingestor_globalsort::EngineConfig {
                ext_store: Arc::clone(&obj_store),
                data_files: sm.data_files.clone(),
                stat_files: sm.stat_files.clone(),
                start_key: all.start_key.clone(),
                end_key: all.end_key.clone(),
                job_keys,
                split_keys: sm.range_split_keys.clone(),
                total_file_size: all.total_kv_size as i64,
                total_kv_count: 0,
                check_hotspot: true,
                mem_capacity: res.mem.capacity(),
                on_dup: tidb_ingestor_engineapi::OnDuplicateKey::Error,
                file_prefix: String::new(),
            },
            res.cpu.capacity().max(1) as usize,
            ts,
        );
        let mut result = local_backend.ingest_external_engine(
            ctx,
            &mut engine,
            SPLIT_REGION_SIZE,
            SPLIT_REGION_KEYS,
        );
        if let Err(error) = tidb_ingestor_engineapi::Engine::close(&mut engine) {
            tidb_ddl_logutil::ddl_logger().warn(
                "close external engine failed",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
            );
        }
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockCloudImportRunSubtaskError").is_some() {
            result = Err(tidb_ingestor_ingestctrl::Error::Other("context deadline exceeded".into()));
        }
        let Err(error) = result else {
            return Ok(());
        };
        let error = tidb_ddl_ingest::Error::Backend(error);
        if let Some(index) = current_idx {
            return Err(Box::new(tidb_ddl_ingest::util::try_convert_to_key_exists_err(error, &index, &self.table)));
        }
        // An old version TiDB's subtask cannot name the index.
        if tidb_ingestor_simplesst::duplicate_key_and_value(&error).is_some() {
            return Err(Box::new(tidb_kv::kv_errors::ERR_KEY_EXISTS.clone()));
        }
        Err(Box::new(error))
    }

    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        Some(self.summary.clone())
    }

    fn reset_summary(&self) {
        self.summary.reset();
    }

    /// Go `cloudImportExecutor.Cleanup`.
    fn cleanup(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        tidb_ddl_logutil::ddl_logger().info("cloud import executor clean up subtask env", &[]);
        if let Some(backend) = self.backend.lock().unwrap().take() {
            backend.close()?;
        }
        Ok(())
    }

    /// Go `cloudImportExecutor.TaskMetaModified`: the max write speed.
    fn task_meta_modified(&self, _: &Context, new: &[u8]) -> tidb_dxf_execute::Result<()> {
        tidb_ddl_logutil::ddl_logger().info("cloud import executor update task meta", &[]);
        let new: BackfillTaskMeta = serde_json::from_slice(new)?;
        if let (Some(old), Some(new)) = (self.job.reorg_meta.as_ref(), new.job.reorg_meta.as_ref()) {
            let new_max_write_speed = new.read().get_max_write_speed();
            if new_max_write_speed != old.read().get_max_write_speed() {
                old.read().set_max_write_speed(new_max_write_speed);
                if let Some(backend) = self.backend.lock().unwrap().as_ref() {
                    backend.update_write_speed_limit(new_max_write_speed);
                }
            }
        }
        Ok(())
    }

    /// Go `cloudImportExecutor.ResourceModified` resizes the running
    /// engine and the backend's worker concurrency. The ported backend's
    /// concurrency and the external engine's memory are fixed when they are
    /// created, so a new resource applies from the next subtask.
    fn resource_modified(&self, _: &Context, _: &StepResource) -> tidb_dxf_execute::Result<()> {
        tidb_ddl_logutil::ddl_logger().info("cloud import executor update resource", &[]);
        Ok(())
    }
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
                    // Go hands DXF the `*errors.Error` itself.
                    return Err(Box::new(
                        tidb_terror::MysqlError::sql_error(error.code, error.message)
                            .with_identity_opt(error.identity),
                    ));
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
                    curr_pipe: Mutex::new(None),
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
            BACKFILL_STEP_MERGE_SORT => Ok(Arc::new(MergeSortStepExecutor {
                info: FrameworkInfoSlot::default(),
                ddl: self.ddl.clone(),
                indexes: task_indexes(&table, &meta),
                table,
                cloud_store_uri: meta.cloud_storage_uri.clone(),
                summary: Arc::new(SubtaskSummary::default()),
            })),
            BACKFILL_STEP_WRITE_AND_INGEST if meta.cloud_storage_uri.is_empty() => {
                Err(plain("local import does not have write & ingest step"))
            }
            BACKFILL_STEP_WRITE_AND_INGEST => Ok(Arc::new(CloudImportStepExecutor {
                info: FrameworkInfoSlot::default(),
                ddl: self.ddl.clone(),
                indexes: task_indexes(&table, &meta),
                table,
                cloud_store_uri: meta.cloud_storage_uri.clone(),
                job: meta.job,
                backend: Mutex::new(None),
                summary: Arc::new(SubtaskSummary::default()),
            })),
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
    curr_pipe: Mutex<Option<Arc<operator::AsyncPipeline>>>,
    summary: Arc<SubtaskSummary>,
}
impl<S: StoreClient> ReadIndexStepExecutor<S> {
    /// Go `readIndexStepExecutor.isGlobalSort`.
    fn is_global_sort(&self) -> bool {
        !self.meta.read().unwrap().cloud_storage_uri.is_empty()
    }

    /// Go `readIndexStepExecutor.onFinished`: a global-sort subtask's meta
    /// gets the sorted KV meta of each index, in external storage.
    fn on_finished(
        &self,
        ctx: &Context,
        subtask: &mut Subtask,
        sm: &mut BackfillSubTaskMeta,
        global: Option<(&dyn tidb_objstore::storeapi::Storage, Vec<tidb_ingestor_globalsort::SortedKVMeta>)>,
    ) -> tidb_dxf_execute::Result<()> {
        let mut backend = self.backend.lock().unwrap().clone();
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish", &mut backend);
        let Some((ext_store, meta_groups)) = global else {
            return Ok(());
        };
        // Rewrite the subtask meta to record statistics.
        sm.ele_ids = self.meta.read().unwrap().ele_ids.clone().unwrap_or_default();
        let mut all = tidb_ingestor_globalsort::SortedKVMeta::default();
        for group in &meta_groups {
            all.merge(group);
        }
        sm.meta_groups = meta_groups;
        tidb_ddl_logutil::ddl_logger().info(
            "get key boundary on subtask finished",
            &[
                tidb_log::Field::new("start", tidb_log::Value::Str(hex::encode(&all.start_key))),
                tidb_log::Field::new("end", tidb_log::Value::Str(hex::encode(&all.end_key))),
                tidb_log::Field::new("fileCount", tidb_log::Value::I64(all.stats().len() as i64)),
                tidb_log::Field::new("totalKVSize", tidb_log::Value::U64(all.total_kv_size)),
            ],
        );
        // Write the external meta to storage when using global sort.
        crate::backfilling_dist_executor::write_external_backfill_sub_task_meta(
            ctx,
            ext_store,
            sm,
            tidb_ingestor_globalsort::subtask_meta_path(subtask.base.task_id, subtask.base.id),
        )?;
        subtask.meta = sm.marshal()?;
        Ok(())
    }

    /// Go `readIndexStepExecutor.runGlobalPipeline` and
    /// `buildExternalStorePipeline`.
    fn run_global_pipeline(
        &self,
        ctx: &Context,
        wctx: &Arc<operator::Context>,
        subtask: &mut Subtask,
        sm: &mut BackfillSubTaskMeta,
        reorg: AddIndexReorg,
        ext_store: Arc<dyn tidb_objstore::storeapi::Storage>,
    ) -> tidb_dxf_execute::Result<()> {
        let meta = self.meta.read().unwrap().clone();
        let meta_groups: Arc<Mutex<Vec<Option<tidb_ingestor_globalsort::SortedKVMeta>>>> =
            Arc::new(Mutex::new(vec![None; reorg.index_ids.len()]));
        let on_writer_close: tidb_ingestor_simplesst::OnWriterCloseFunc = {
            let meta_groups = Arc::clone(&meta_groups);
            Arc::new(move |summary| {
                let mut groups = meta_groups.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(group) = groups.get_mut(summary.group_offset as usize) {
                    group.get_or_insert_with(Default::default).merge_summary(summary);
                }
            })
        };
        let concurrency = reorg.concurrency;
        let scan = Arc::new(super::operators::ScanContext {
            opener: Arc::clone(&self.ddl.opener),
            timeout: self.ddl.timeout,
            backfiller: (self.ddl.backfiller)().shared(),
            reorg: Arc::new(reorg),
            physical_id: sm.physical_table_id,
            warnings: Arc::default(),
        });
        let pipe = super::operators::new_write_index_to_external_storage_pipeline(
            wctx,
            scan,
            Arc::clone(&ext_store),
            subtask.base.task_id,
            subtask.base.id,
            sm.row_start.clone(),
            sm.row_end.clone(),
            on_writer_close,
            meta.job.reorg_meta.clone(),
            meta.estimate_row_size,
            concurrency,
            &self.get_resource(),
            Arc::new(DistTaskRowCntCollector { summary: Arc::clone(&self.summary) }),
        );
        *self.curr_pipe.lock().unwrap() = Some(Arc::clone(&pipe));
        let check = || ctx.err().map_or(Ok(()), |e| Err(e.to_string()));
        let stopped = AtomicBool::new(false);
        let result =
            super::ingest::execute_and_close_pipeline(wctx, &pipe, None, None, meta.estimate_row_size, &check, &stopped);
        self.curr_pipe.lock().unwrap().take();
        result.map_err(pipeline_error)?;
        let groups = std::mem::take(&mut *meta_groups.lock().unwrap_or_else(std::sync::PoisonError::into_inner))
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect();
        self.on_finished(ctx, subtask, sm, Some((ext_store.as_ref(), groups)))
    }
}
impl<S: StoreClient> StepExecFrameworkInfo for ReadIndexStepExecutor<S> {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.info
    }
}
impl<S: StoreClient> StepExecutor for ReadIndexStepExecutor<S> {
    fn init(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        tidb_ddl_logutil::ddl_logger().info("read index executor init subtask exec env", &[]);
        let meta = self.meta.read().unwrap();
        if !meta.cloud_storage_uri.is_empty() {
            // Multi-schema proxy jobs may not carry UseCloudStorage. Set it
            // before the framework starts its parameter-modify loop, which
            // reads the flag through ResourceModified.
            if let Some(reorg_meta) = &meta.job.reorg_meta {
                reorg_meta.write().use_cloud_storage = true;
            }
        }
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
        let mut err: Option<String> = None;
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/beforeReadIndexStepExecRunSubtask", &mut err);
        if let Some(err) = err {
            return Err(err.into());
        }
        let meta = self.meta.read().unwrap().clone();
        let global_sort = self.is_global_sort();
        tidb_ddl_logutil::ddl_logger()
            .info("read index executor run subtask", &[tidb_log::Field::new("use cloud", tidb_log::Value::Bool(global_sort))]);
        let recorded = if global_sort {
            Some(RecordedObjStore::new(ctx, &meta.cloud_storage_uri, &self.summary, self.get_meter_recorder())?)
        } else {
            None
        };
        let mut sm = match &recorded {
            Some(recorded) => {
                let read = read_external_with(ctx, recorded.store.as_ref());
                decode_backfill_sub_task_meta(&subtask.meta, Some(&read))?
            }
            None => decode_backfill_sub_task_meta(&subtask.meta, None)?,
        };
        let mut reorg = reorg_for_task(&meta, self.table.clone_like_go());
        reorg.concurrency = self.get_resource().cpu.capacity() as usize;
        if let Some(recorded) = &recorded {
            // Go `workerpool.NewContext(ctx)` over the subtask's context.
            let wctx = Arc::new(operator::Context::with_parent(ctx.done()));
            let result =
                self.run_global_pipeline(ctx, &wctx, subtask, &mut sm, reorg, Arc::clone(&recorded.store));
            wctx.cancel();
            return result;
        }
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
            // Go `workerpool.NewContext(ctx)` over the subtask's context.
            let wctx = Arc::new(operator::Context::with_parent(ctx.done()));
            let scan = Arc::new(super::operators::ScanContext {
                opener: Arc::clone(&self.ddl.opener),
                timeout: self.ddl.timeout,
                backfiller: (self.ddl.backfiller)().shared(),
                reorg: Arc::new(reorg.clone()),
                physical_id: sm.physical_table_id,
                warnings: Arc::default(),
            });
            // Go `buildLocalStorePipeline`.
            let pipe = super::operators::new_add_index_ingest_pipeline(
                &wctx,
                scan,
                Arc::clone(&bc),
                engines,
                meta.job.id,
                sm.row_start.clone(),
                sm.row_end.clone(),
                meta.job.reorg_meta.clone(),
                meta.estimate_row_size,
                reorg.concurrency,
                Arc::new(DistTaskRowCntCollector { summary: Arc::clone(&self.summary) }),
            );
            *self.curr_pipe.lock().unwrap() = Some(Arc::clone(&pipe));
            let check = || ctx.err().map_or(Ok(()), |e| Err(e.to_string()));
            let stopped = AtomicBool::new(false);
            let result = super::ingest::execute_and_close_pipeline(
                &wctx,
                &pipe,
                None,
                None,
                meta.estimate_row_size,
                &check,
                &stopped,
            );
            self.curr_pipe.lock().unwrap().take();
            wctx.cancel();
            match result {
                Ok(()) => {
                    bc.finish_and_unregister_engines(
                        ctx,
                        UnregisterOpt(UnregisterOpt::CLEAN_DATA.0 | UnregisterOpt::CHECK_DUP.0),
                    )
                    .map_err(|e| Box::new(e) as tidb_dxf_execute::Error)?;
                    self.on_finished(ctx, subtask, &mut sm, None)
                }
                Err(error) => {
                    let _ = bc.finish_and_unregister_engines(ctx, UnregisterOpt::CLEAN_DATA);
                    Err(pipeline_error(error))
                }
            }
        })();
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
    /// Go `readIndexStepExecutor.TaskMetaModified`: mutate shared reorg metadata,
    /// so a running scanner observes the new batch size at its next chunk.
    fn task_meta_modified(&self, _: &Context, new: &[u8]) -> tidb_dxf_execute::Result<()> {
        modify_read_index_task_meta(&self.meta.read().unwrap(), new, |limit| {
            if let Some(backend) = self.backend.lock().unwrap().as_ref() {
                backend.update_write_speed_limit(limit);
            }
        })
    }

    /// Go `readIndexStepExecutor.ResourceModified`.
    fn resource_modified(&self, _: &Context, new: &StepResource) -> tidb_dxf_execute::Result<()> {
        let pipe = self
            .curr_pipe
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| std::io::Error::other("no subtask running"))?;
        let meta = self.meta.read().unwrap();
        let global_sort = meta
            .job
            .reorg_meta
            .as_ref()
            .is_some_and(|meta| meta.read().use_cloud_storage);
        let avg_row_size = meta.estimate_row_size;
        drop(meta);
        super::ingest::tune_pipeline(&pipe, new.cpu.capacity() as usize, avg_row_size, global_sort, true)
            .map_err(|error| Box::new(std::io::Error::other(error)) as tidb_dxf_execute::Error)
    }
}
/// Go `readIndexStepExecutor.TaskMetaModified`: update the shared job in place.
fn modify_read_index_task_meta(
    meta: &BackfillTaskMeta,
    new: &[u8],
    mut update_write_speed: impl FnMut(i64),
) -> tidb_dxf_execute::Result<()> {
    let new: BackfillTaskMeta = serde_json::from_slice(new)?;
    if let (Some(old), Some(new)) = (meta.job.reorg_meta.as_ref(), new.job.reorg_meta.as_ref()) {
        let old = old.read();
        let new = new.read();
        let defaults = crate::index::reorg_defaults();
        let batch_size = new.get_batch_size(defaults);
        if old.get_batch_size(defaults) != batch_size {
            old.set_batch_size(batch_size);
        }
        if meta.cloud_storage_uri.is_empty()
            && old.get_max_write_speed() != new.get_max_write_speed()
        {
            old.set_max_write_speed(new.get_max_write_speed());
            update_write_speed(new.get_max_write_speed());
        }
    }
    Ok(())
}

#[cfg(test)]
mod cloud_storage_uri_tests {
    use super::*;

    /// Go `TestResolveCloudStorageURI`.
    #[test]
    fn test_resolve_cloud_storage_uri() {
        let original = tidb_vardef::CLOUD_STORAGE_URI.read().unwrap().clone();
        const JOB_ID: i64 = 900001;
        let new_job = |use_cloud_storage: bool| {
            let mut meta = tidb_model::DDLReorgMeta::default();
            meta.use_cloud_storage = use_cloud_storage;
            let mut job = Job::default();
            job.id = JOB_ID;
            job.reorg_meta = Some(tidb_model::GoShared::new(meta));
            job
        };
        let set_uri = |uri: &str| *tidb_vardef::CLOUD_STORAGE_URI.write().unwrap() = uri.to_owned();
        let with_cache = |uri: &str| {
            crate::job_ctx::remove_job_ctx(JOB_ID);
            crate::job_ctx::with_job_ctx(JOB_ID, |jc| jc.cloud_storage_uri = uri.to_owned());
        };

        // A configured URI recovers an empty owner cache.
        set_uri("s3://bucket");
        with_cache("");
        let uri = resolve_cloud_storage_uri(&new_job(true), false).unwrap();
        assert_eq!(uri, "s3://bucket/dxf/");
        assert_eq!(crate::job_ctx::with_job_ctx(JOB_ID, |jc| jc.cloud_storage_uri.clone()), uri);

        // A missing configured URI is unretryable.
        set_uri("");
        with_cache("");
        let Err(ClusterDdlError::Backfill(error)) = resolve_cloud_storage_uri(&new_job(true), false) else {
            panic!("an empty URI fails the job");
        };
        assert!(
            error.to_string().contains("cloud storage URI is empty for add-index job 900001 with cloud storage enabled"),
            "{error}"
        );
        assert!(!crate::real_tikv_ddl::is_retryable_job_error(&error, 0, tidb_vardef::ddl_error_count_limit()));

        // The cached URI wins over a changed configuration.
        set_uri("s3://new-bucket");
        with_cache("s3://cached-bucket/dxf/");
        assert_eq!(resolve_cloud_storage_uri(&new_job(true), false).unwrap(), "s3://cached-bucket/dxf/");

        // Local sort permits an empty URI.
        set_uri("");
        with_cache("");
        assert_eq!(resolve_cloud_storage_uri(&new_job(false), false).unwrap(), "");

        // Merge temp index does not require cloud storage.
        with_cache("");
        assert_eq!(resolve_cloud_storage_uri(&new_job(true), true).unwrap(), "");

        crate::job_ctx::remove_job_ctx(JOB_ID);
        set_uri(&original);
    }
}

#[cfg(test)]
mod modification_tests {
    use super::*;
    /// Go `addindextest2.TestAlterJobOnDXF`: 32->256 batch, 16->1024 write
    /// speed on the already-running job; global sort only changes the batch.
    #[test]
    fn test_alter_job_on_dxf() {
        for global in [false, true] {
            let mut meta = BackfillTaskMeta::default();
            let reorg = tidb_model::DDLReorgMeta::default();
            reorg.set_batch_size(32);
            reorg.set_max_write_speed(16);
            meta.job.reorg_meta = Some(tidb_model::GoShared::new(reorg));
            if global {
                meta.cloud_storage_uri = "file:///tmp/global-sort".into();
            }
            let running = meta.job.reorg_meta.clone().unwrap();
            let mut updated = BackfillTaskMeta::default();
            let reorg = tidb_model::DDLReorgMeta::default();
            reorg.set_batch_size(256);
            reorg.set_max_write_speed(1024);
            updated.job.reorg_meta = Some(tidb_model::GoShared::new(reorg));
            let bytes = serde_json::to_vec(&updated).unwrap();
            let limiter = tidb_ingestor_ingestctrl::localhelper::StoreWriteLimiter::new(16);
            let mut calls = 0;
            for _ in 0..2 {
                modify_read_index_task_meta(&meta, &bytes, |limit| {
                    calls += 1;
                    limiter.update_limit(limit);
                })
                .unwrap();
            }
            assert_eq!(
                running
                    .read()
                    .get_batch_size(tidb_model::DDLReorgProcessDefaults::new(|| 4, || 256)),
                256
            );
            assert_eq!(
                running.read().get_max_write_speed(),
                if global { 16 } else { 1024 }
            );
            assert_eq!(limiter.limit(), if global { 16 } else { 1024 });
            assert_eq!(calls, usize::from(!global));
            assert!(
                modify_read_index_task_meta(&meta, b"invalid json", |_| panic!(
                    "invalid metadata reached backend"
                ))
                .is_err()
            );
        }
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
            Box::new(tidb_terror::MysqlError::sql_error(e.code, e.message).with_identity_opt(e.identity))
        }
        super::ingest::PipelineError::Interrupted => Box::new(tidb_gocontext::CtxError::Canceled),
        super::ingest::PipelineError::Operator(e) => Box::new(e),
    }
}

/// Go `distTaskRowCntCollector`: the subtask's realtime summary.
struct DistTaskRowCntCollector {
    summary: Arc<SubtaskSummary>,
}

impl tidb_dxf_execute::Collector for DistTaskRowCntCollector {
    /// Go `distTaskRowCntCollector.Accepted`.
    fn accepted(&self, bytes: i64) {
        self.summary.read_bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    /// Go `distTaskRowCntCollector.Processed`.
    fn processed(&self, bytes: i64, rows: i64) {
        self.summary.processed.fetch_add(bytes, Ordering::SeqCst);
        self.summary.row_cnt.fetch_add(rows, Ordering::SeqCst);
    }
}

/// Go `worker.resolveCloudStorageURI`: the URI `loadCloudStorageURI` kept
/// in the job context, reloaded when a new DDL owner resumes the job. An
/// empty URI fails the job: recovering needs the user to restore it, and
/// retrying would leave the job in write reorganization.
pub(crate) fn resolve_cloud_storage_uri(job: &Job, merge_temp_index: bool) -> Result<String, ClusterDdlError> {
    let cached = crate::job_ctx::with_job_ctx(job.id, |jc| jc.cloud_storage_uri.clone());
    if merge_temp_index
        || !job.reorg_meta.as_ref().is_some_and(|meta| meta.read().use_cloud_storage)
        || !cached.is_empty()
    {
        return Ok(cached);
    }
    let cloud_storage_uri = tidb_dxf_handle::get_cloud_storage_uri(tidb_dxf_handle::cluster_id());
    crate::job_ctx::with_job_ctx(job.id, |jc| jc.cloud_storage_uri = cloud_storage_uri.clone());
    if cloud_storage_uri.is_empty() {
        return Err(ClusterDdlError::backfill(&tidb_dbterror::ERR_INGEST_FAILED.gen_with_stack_by_args(&[format!(
            "cloud storage URI is empty for add-index job {} with cloud storage enabled",
            job.id
        )
        .into()])));
    }
    Ok(cloud_storage_uri)
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
        .map_err(|e| ClusterDdlError::backfill(&e))?;
    // Go `lastRequiredSlots, lastBatchSize, lastMaxWriteSpeed`.
    let mut last = (0, 0, 0);
    let task = match manager.get_task_by_key_with_history(&ctx, &key) {
        Ok(task) => {
            if crate::index::should_auto_pause_existing_kv_disk_full_task(job, &task) {
                return Ok(auto_pause_result(&task));
            }
            if task.base.state != TASK_STATE_SUCCEED {
                tidb_dxf_handle::resume_task(&ctx, &key)
                    .map_err(|e| ClusterDdlError::backfill(&e))?;
            }
            let task_meta: BackfillTaskMeta = serde_json::from_slice(&task.meta)
                .map_err(|e| ClusterDdlError::backfill(&e))?;
            let reorg_meta = task_meta.job.reorg_meta.as_ref().expect("a backfill task's job has reorg meta").read();
            last = (
                task.base.required_slots as i64,
                reorg_meta.get_batch_size(crate::index::reorg_defaults()),
                reorg_meta.get_max_write_speed(),
            );
            drop(reorg_meta);
            task
        }
        Err(error) if error.is(&tidb_dxf_storage::StorageError::TaskNotFound) => {
            let required_slots = (reorg.concurrency as isize).min(
                manager
                    .get_cpu_count_of_node(&ctx)
                    .map_err(|e| ClusterDdlError::backfill(&e))?,
            );
            let meta = BackfillTaskMeta {
                job: job.clone(),
                ele_ids: Some(reorg.index_ids.clone()),
                ele_type_key: INDEX_ELEMENT_KEY.to_vec(),
                merge_temp_index: reorg.merging,
                estimate_row_size: crate::index::estimate_table_row_size(
                    opener.store(),
                    sess_pool,
                    reorg.table.id,
                ) as _,
                cloud_storage_uri: resolve_cloud_storage_uri(job, reorg.merging)?,
                version: crate::backfilling_dist_executor::BACKFILL_TASK_META_VERSION_1,
                ..Default::default()
            };
            let config = job.reorg_meta.as_ref().unwrap().read();
            last = (
                required_slots as i64,
                config.get_batch_size(crate::index::reorg_defaults()),
                config.get_max_write_speed(),
            );
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
                &serde_json::to_vec(&meta).map_err(|e| ClusterDdlError::backfill(&e))?,
            )
            .map_err(|e| ClusterDdlError::backfill(&e))?
        }
        Err(error) => return Err(ClusterDdlError::backfill(&error)),
    };
    let pool = sess_pool
        .sess_pool()
        .ok_or_else(|| ClusterDdlError::backfill_message("the DDL session pool has no sessions".to_owned()))?;
    let sys_tbl_mgr = tidb_ddl_systable::SystemTableManager::new(pool);
    // Go `done`: closed when the task wait returns, which ends the modify loop.
    let (done_tx, done) = crossbeam_channel::bounded::<()>(0);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            crate::index::modify_task_param_loop(
                &ctx,
                &sys_tbl_mgr,
                manager.as_ref(),
                &done,
                job.id,
                task.base.id,
                last.0,
                last.1,
                last.2,
            );
        });
        let result = wait_dist_task(opener, sess_pool, reorg, job, check_owner, step_cancelled, &ctx, &key, &manager, &task);
        drop(done_tx);
        result
    })
}

/// Go `executeDistTask`'s wait and its `checkRunnableOrHandlePauseOrCanceled`
/// loop, while [`crate::index::modify_task_param_loop`] runs beside it.
#[allow(clippy::too_many_arguments)]
fn wait_dist_task<S: StoreClient>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    reorg: &AddIndexReorg,
    job: &Job,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    step_cancelled: &AtomicBool,
    ctx: &Context,
    key: &str,
    manager: &Arc<tidb_dxf_storage::TaskManager>,
    task: &Task,
) -> Result<ReorgResult, ClusterDdlError> {
    loop {
        if !reorg_runnable(step_cancelled, check_owner)? {
            if let Some(job) = latest_job(sess_pool, reorg.job_id)? {
                if job.state == JobState::PAUSING || job.state == JobState::PAUSED {
                    if tidb_dxf_handle::pause_task(&ctx, &key).is_ok() {
                        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/syncDDLTaskPause", &mut ());
                    }
                } else if matches!(job.state, JobState::CANCELLING | JobState::CANCELLED | JobState::ROLLINGBACK) {
                    // Go `checkRunnableOrHandlePauseOrCanceled` on
                    // `ErrCancelledDDLJob`, from the cancel checker or from a
                    // step of the job's rollback.
                    let _ = tidb_dxf_handle::cancel_task(&ctx, &key);
                }
            }
            return Ok(ReorgResult::Interrupted);
        }
        let current = manager
            .get_task_by_id_with_history(&ctx, task.base.id)
            .map_err(|e| ClusterDdlError::backfill(&e))?;
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
                .map_err(|e| ClusterDdlError::backfill(&e))?;
            let found = tidb_dxf_handle::wait_task_done_or_paused_with_result(&ctx, task.base.id);
            match found {
                Ok(_) => {
                    // Go's wait goroutine: a pause that arrives once the task
                    // finished still pauses the job (`ErrPausedDDLJob`).
                    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/pauseAfterDistTaskFinished", &mut ());
                    if step_cancelled.load(Ordering::SeqCst)
                        && latest_job(sess_pool, reorg.job_id)?
                            .is_some_and(|job| job.state == JobState::PAUSING || job.state == JobState::PAUSED)
                    {
                        return Ok(ReorgResult::Interrupted);
                    }
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
                                return Ok(ReorgResult::Failed { code: error.code, message: error.message, rows, identity: error.identity });
                            }
                            return Err(ClusterDdlError::backfill(error.as_ref()));
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
                            identity: error.identity,
                        });
                    }
                    if let Some(error) = cause.downcast_ref::<tidb_terror::MysqlError>() {
                        return Ok(ReorgResult::Failed {
                            code: error.code,
                            message: error.message.clone(),
                            rows,
                            identity: error.identity,
                        });
                    }
                    return Err(ClusterDdlError::backfill(&error));
                }
                Err(error) => return Err(ClusterDdlError::backfill(&error)),
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
