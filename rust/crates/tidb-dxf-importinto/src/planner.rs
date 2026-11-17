// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/planner.go`: the logical plan of an import and
//! the subtask metas of each step. Local sort has the `import` and
//! `post-process` steps.

use std::collections::BTreeMap;

use tidb_dxf::{
    ExtraParams, Step, IMPORT_STEP_COLLECT_CONFLICTS, IMPORT_STEP_CONFLICT_RESOLUTION, IMPORT_STEP_ENCODE_AND_SORT,
    IMPORT_STEP_IMPORT, IMPORT_STEP_MERGE_SORT, IMPORT_STEP_POST_PROCESS, IMPORT_STEP_WRITE_AND_INGEST,
};
use tidb_dxf_planner::{LinkSpec, OutputSpec, PhysicalPlan, PipelineSpec, PlanCtx, PlanError, ProcessorSpec};
use tidb_executor::importer::chunk_process::Chunk;
use tidb_executor::importer::job::StepSummary;
use tidb_executor::importer::{ASTArgs, LoadDataController, Plan};

use crate::proto::{
    self, Checksum, CollectConflictsStepMeta, ConflictResolutionStepMeta, ImportStepMeta, KVGroupConflictInfos,
    MergeSortStepMeta, PostProcessStepMeta, TaskMeta, WriteIngestStepMeta,
};
use crate::{plain, Error};

/// Go `LogicalPlan`.
#[derive(Clone, Debug, Default)]
pub struct LogicalPlan {
    /// Go `JobID`.
    pub job_id: i64,
    /// Go `Plan`.
    pub plan: Plan,
    /// Go `Stmt`.
    pub stmt: String,
    /// Go `EligibleInstances`.
    pub eligible_instances: Option<Vec<serde_json::Value>>,
    /// Go `ChunkMap`.
    pub chunk_map: Option<BTreeMap<i32, Vec<Chunk>>>,
    /// Go `PrepareMode`.
    pub prepare_mode: tidb_dxf::PrepareMode,
    /// Go `PreparedChunkMapExternalPath`: the chunk map the prepare stage
    /// wrote to the sort store.
    pub prepared_chunk_map_external_path: String,
    /// Go `summary`: the next step's summary, set while its specs are
    /// generated.
    pub summary: std::cell::Cell<StepSummary>,
}

fn boxed(e: Error) -> PlanError {
    Box::new(ArcError(e))
}

/// An [`Error`] as a planner error.
#[derive(Debug)]
struct ArcError(Error);

impl std::fmt::Display for ArcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ArcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// A planner error as an [`Error`].
pub(crate) fn unbox(e: PlanError) -> Error {
    match e.downcast::<ArcError>() {
        Ok(e) => e.0,
        Err(e) => std::sync::Arc::from(e),
    }
}

impl LogicalPlan {
    /// The task meta of this plan.
    #[must_use]
    pub fn task_meta(&self) -> TaskMeta {
        TaskMeta {
            job_id: self.job_id,
            plan: self.plan.clone(),
            stmt: self.stmt.clone(),
            eligible_instances: self.eligible_instances.clone(),
            chunk_map: self.chunk_map.clone(),
            prepared_meta_external_path: self.prepared_chunk_map_external_path.clone(),
            ..TaskMeta::default()
        }
    }
}
impl tidb_dxf_planner::LogicalPlan for LogicalPlan {
    fn get_task_extra_params(&self) -> ExtraParams {
        ExtraParams { manual_recovery: self.plan.manual_recovery, prepare_mode: self.prepare_mode, ..ExtraParams::default() }
    }

    fn to_task_meta(&self) -> Result<Vec<u8>, PlanError> {
        Ok(self.task_meta().to_json())
    }

    fn from_task_meta(&mut self, meta: &[u8]) -> Result<(), PlanError> {
        let m = TaskMeta::from_json(meta).map_err(boxed)?;
        self.job_id = m.job_id;
        self.plan = m.plan;
        self.stmt = m.stmt;
        self.eligible_instances = m.eligible_instances;
        self.chunk_map = m.chunk_map;
        self.prepared_chunk_map_external_path = m.prepared_meta_external_path;
        Ok(())
    }

    fn to_physical_plan(&self, ctx: &PlanCtx<'_>) -> Result<PhysicalPlan, PlanError> {
        let mut physical = PhysicalPlan::default();
        let mut input_links = Vec::new();
        let specs = match ctx.next_task_step {
            IMPORT_STEP_IMPORT | IMPORT_STEP_ENCODE_AND_SORT => Some(generate_import_specs(ctx, self).map_err(boxed)?),
            IMPORT_STEP_MERGE_SORT => Some(generate_merge_sort_specs(ctx, self).map_err(boxed)?),
            IMPORT_STEP_WRITE_AND_INGEST => Some(generate_write_ingest_specs(ctx, self).map_err(boxed)?),
            IMPORT_STEP_COLLECT_CONFLICTS => Some(generate_collect_conflicts_specs(ctx, self).map_err(boxed)?),
            IMPORT_STEP_CONFLICT_RESOLUTION => Some(generate_conflict_resolution_specs(ctx, self).map_err(boxed)?),
            IMPORT_STEP_POST_PROCESS => None,
            step => return Err(boxed(plain(format!("unsupported import step {}", step.0)))),
        };
        match specs {
            Some(mut specs) => {
                self.write_external_plan_meta(ctx, &mut specs).map_err(boxed)?;
                let n = specs.len() as isize;
                for (i, spec) in specs.into_iter().enumerate() {
                    physical.add_processor(ProcessorSpec {
                        id: i as isize,
                        input: Default::default(),
                        pipeline: Box::new(spec),
                        output: OutputSpec { links: vec![LinkSpec { processor_id: n }] },
                        step: ctx.next_task_step,
                    });
                    input_links.push(LinkSpec { processor_id: i as isize });
                }
            }
            None => {
                physical.add_processor(ProcessorSpec {
                    id: input_links.len() as isize,
                    input: tidb_dxf_planner::InputSpec { column_types: Vec::new(), links: input_links },
                    pipeline: Box::new(PostProcessSpec { encode_step: get_step_of_encode(&self.plan) }),
                    output: OutputSpec::default(),
                    step: ctx.next_task_step,
                });
            }
        }
        Ok(physical)
    }
}

impl LogicalPlan {
    /// Go `writeExternalPlanMeta`: with global sort, each spec's external
    /// fields go to `{task}/plan/{step}/{i+1}/meta.json`.
    fn write_external_plan_meta(&self, ctx: &PlanCtx<'_>, specs: &mut [StepSpec]) -> Result<(), Error> {
        if !ctx.global_sort {
            return Ok(());
        }
        let store = sort_store(&ctx.ctx, &self.plan)?;
        let step = tidb_dxf::step_2_str(&tidb_dxf::IMPORT_INTO, ctx.next_task_step);
        for (i, spec) in specs.iter_mut().enumerate() {
            let path = tidb_ingestor_globalsort::plan_meta_path(ctx.task_id, &step, i + 1);
            let store = store.as_ref();
            match spec {
                StepSpec::Import(m) => {
                    m.external_path = path;
                    m.write_external(&ctx.ctx, store)?;
                }
                StepSpec::MergeSort(m) => {
                    m.external_path = path;
                    m.write_external(&ctx.ctx, store)?;
                }
                StepSpec::WriteIngest(m) => {
                    m.external_path = path;
                    m.write_external(&ctx.ctx, store)?;
                }
                StepSpec::CollectConflicts(m) => {
                    m.external_path = path;
                    m.write_external(&ctx.ctx, store)?;
                }
                StepSpec::ConflictResolution(m) => {
                    m.external_path = path;
                    m.write_external(&ctx.ctx, store)?;
                }
            }
        }
        Ok(())
    }
}

/// Go `importer.GetSortStore`.
pub(crate) fn sort_store(ctx: &tidb_gocontext::Context, plan: &Plan) -> Result<std::sync::Arc<dyn tidb_objstore::storeapi::Storage>, Error> {
    tidb_executor::importer::get_sort_store(ctx, &plan.cloud_storage_uri).map_err(crate::driver)
}

/// Go `handle.NewObjStoreWithRecording` with a step's deferred merge: the
/// plan's global-sort storage, whose requests go into the subtask summary
/// and whose access goes to the meter recorder when it is dropped.
pub(crate) struct RecordedObjStore {
    pub(crate) store: std::sync::Arc<dyn tidb_objstore::storeapi::Storage>,
    rec: std::sync::Arc<tidb_objstore_recording::AccessStats>,
    summary: std::sync::Arc<tidb_dxf_execute::SubtaskSummary>,
    meter_rec: std::sync::Arc<tidb_dxf_metering::Recorder>,
}

impl Drop for RecordedObjStore {
    fn drop(&mut self) {
        self.summary.merge_obj_store_requests(&self.rec.requests);
        self.meter_rec.merge_obj_store_access(&self.rec);
    }
}

/// Go `handle.NewObjStoreWithRecording(ctx, plan.CloudStorageURI)`.
pub(crate) fn new_obj_store_with_recording(
    ctx: &tidb_gocontext::Context,
    plan: &Plan,
    summary: &std::sync::Arc<tidb_dxf_execute::SubtaskSummary>,
    meter_rec: std::sync::Arc<tidb_dxf_metering::Recorder>,
) -> Result<RecordedObjStore, Error> {
    let (rec, store) = tidb_dxf_handle::new_obj_store_with_recording(ctx, &plan.cloud_storage_uri).map_err(plain)?;
    Ok(RecordedObjStore { store, rec, summary: std::sync::Arc::clone(summary), meter_rec })
}

/// Go `ImportSpec`, `MergeSortSpec`, `WriteIngestSpec`,
/// `CollectConflictsSpec` and `ConflictResolutionSpec`.
pub enum StepSpec {
    /// Go `ImportSpec`.
    Import(ImportStepMeta),
    /// Go `MergeSortSpec`.
    MergeSort(MergeSortStepMeta),
    /// Go `WriteIngestSpec`.
    WriteIngest(WriteIngestStepMeta),
    /// Go `CollectConflictsSpec`.
    CollectConflicts(CollectConflictsStepMeta),
    /// Go `ConflictResolutionSpec`.
    ConflictResolution(ConflictResolutionStepMeta),
}

impl PipelineSpec for StepSpec {
    /// Go `ToSubtaskMeta`: the meta's `Marshal`.
    fn to_subtask_meta(&self, _: &PlanCtx<'_>) -> Result<Vec<u8>, PlanError> {
        Ok(match self {
            Self::Import(m) => m.marshal(),
            Self::MergeSort(m) => m.marshal(),
            Self::WriteIngest(m) => m.marshal(),
            Self::CollectConflicts(m) => m.marshal(),
            Self::ConflictResolution(m) => m.marshal(),
        })
    }
}

/// Go `getStepOfEncode`: local sort encodes in the `import` step.
#[must_use]
pub fn get_step_of_encode(plan: &Plan) -> Step {
    if plan.query_source.is_some() {
        return tidb_dxf::IMPORT_STEP_QUERY;
    }
    if plan.is_global_sort() {
        return tidb_dxf::IMPORT_STEP_ENCODE_AND_SORT;
    }
    IMPORT_STEP_IMPORT
}

/// Go `PostProcessSpec`.
pub struct PostProcessSpec {
    encode_step: Step,
}

impl PipelineSpec for PostProcessSpec {
    /// Go `ToSubtaskMeta`: the encode step's checksums and maximum IDs
    /// merged, less the checksum of rows the conflict steps deleted.
    fn to_subtask_meta(&self, ctx: &PlanCtx<'_>) -> Result<Vec<u8>, PlanError> {
        let empty = Vec::new();
        let metas = ctx.previous_subtask_metas.get(&self.encode_step).unwrap_or(&empty);
        let mut deleted = tidb_lightning_verification::KvChecksum::new();
        let mut too_many_conflicts_from_index = false;
        for bs in ctx.previous_subtask_metas.get(&IMPORT_STEP_COLLECT_CONFLICTS).unwrap_or(&empty) {
            let m: CollectConflictsStepMeta = proto::unmarshal(bs).map_err(boxed)?;
            let c = m.checksum.unwrap_or_default();
            deleted.add(&c.to_kv_checksum());
            too_many_conflicts_from_index |= m.too_many_conflicts_from_index;
        }
        let mut local = tidb_lightning_verification::KvGroupChecksum::new_kv_group_checksum_for_add();
        let mut max_ids: BTreeMap<i32, i64> = BTreeMap::new();
        for bs in metas {
            let m: ImportStepMeta = proto::unmarshal(bs).map_err(boxed)?;
            for (id, c) in m.checksum.unwrap_or_default() {
                local.add_raw_group(id, c.size, c.kvs, c.sum);
            }
            for (k, v) in m.max_ids.unwrap_or_default() {
                let e = max_ids.entry(k).or_insert(0);
                *e = (*e).max(v);
            }
        }
        let checksum = local
            .get_inner_checksums()
            .iter()
            .map(|(id, c)| (*id, Checksum::from_kv_checksum(c)))
            .collect();
        let meta = PostProcessStepMeta {
            checksum: Some(checksum),
            deleted_rows_checksum: Checksum::from_kv_checksum(&deleted),
            too_many_conflicts_from_index,
            max_ids: Some(max_ids),
        };
        proto::marshal(&meta).map_err(boxed)
    }
}

/// Go `ASTArgsFromStmt`: the column list and SET clause, re-parsed from
/// the task's statement.
///
/// # Errors
/// The statement is not an IMPORT INTO.
pub fn ast_args_from_stmt(stmt: &str) -> Result<ASTArgs, Error> {
    let parsed = tidb_parser::parse(stmt).map_err(|e| plain(e.message))?;
    let tidb_ast::Stmt::Dml(dml) = parsed else {
        return Err(plain(format!("stmt {stmt} is not import into stmt")));
    };
    let tidb_ast::DmlStmt::ImportInto(import) = dml.as_ref() else {
        return Err(plain(format!("stmt {stmt} is not import into stmt")));
    };
    Ok(ASTArgs {
        columns_and_user_vars: import.columns_and_user_vars.clone(),
        column_assignments: import.column_assignments.clone(),
    })
}

/// Go `buildControllerForPlan` (and `getTableImporter`'s controller): the
/// table rebuilt from the plan's metadata over panicking allocators.
///
/// # Errors
/// The plan's table or statement is not usable.
pub fn build_controller_for_plan(plan: &Plan, stmt: &str) -> Result<LoadDataController, Error> {
    let meta = plan.table_info.clone().ok_or_else(|| plain("import plan has no table info"))?;
    let new_collate = plan.get_use_new_collate_or_default(tidb_datatype::new_collation_enabled());
    let table = tidb_table::table_from_meta::table_from_meta(&meta, &tidb_table::table_from_meta::AutoIdSource::Unavailable)
        .map_err(plain)?
        .with_new_collation_mode(new_collate);
    let ast_args = ast_args_from_stmt(stmt)?;
    LoadDataController::new(plan.clone(), table, ast_args).map_err(crate::driver)
}

/// Go `generateImportSpecs`: one subtask per data engine, from the chunk
/// map the submitter made, or split now.
fn generate_import_specs(ctx: &PlanCtx<'_>, p: &LogicalPlan) -> Result<Vec<StepSpec>, Error> {
    let chunk_map = match &p.chunk_map {
        _ if !p.prepared_chunk_map_external_path.is_empty() => {
            read_prepared_chunk_map(&ctx.ctx, &p.plan, &p.prepared_chunk_map_external_path)?
        }
        Some(m) if !m.is_empty() => m.clone(),
        _ => {
            let mut controller = build_controller_for_plan(&p.plan, &p.stmt)?;
            let result = controller.init_data_files(&ctx.ctx).map_err(crate::driver).and_then(|()| {
                controller.set_execute_node_cnt(usize::try_from(ctx.execute_nodes_cnt).unwrap_or(0));
                controller.populate_chunks(&ctx.ctx).map_err(plain)
            });
            controller.close();
            result?
        }
    };
    let mut summary = StepSummary::default();
    let mut specs = Vec::with_capacity(chunk_map.len());
    for (id, chunks) in chunk_map {
        if id == tidb_executor::importer::table_import::INDEX_ENGINE_ID {
            continue;
        }
        summary.bytes = p.plan.total_file_size;
        for chunk in &chunks {
            summary.row_cnt = summary.row_cnt.max(chunk.row_id_max);
        }
        specs.push(StepSpec::Import(ImportStepMeta { id, chunks: Some(chunks), ..ImportStepMeta::default() }));
    }
    p.summary.set(summary);
    Ok(specs)
}

/// Go `readPreparedChunkMap`.
fn read_prepared_chunk_map(
    ctx: &tidb_gocontext::Context,
    plan: &tidb_executor::importer::Plan,
    external_path: &str,
) -> Result<BTreeMap<i32, Vec<Chunk>>, Error> {
    let store = sort_store(ctx, plan)?;
    let mut meta = crate::proto::PreparedMeta { external_path: external_path.to_owned(), ..Default::default() };
    let result = meta.read_external(ctx, store.as_ref());
    store.close();
    result?;
    Ok(meta.chunk_map.unwrap_or_default())
}

/// Go `config.DefaultBatchSize`: 100 GiB.
const DEFAULT_BATCH_SIZE: i64 = 100 << 30;

/// Go `skipMergeSort`: a group whose files overlap little enough is
/// ingested without a merge sort.
fn skip_merge_sort(kv_group: &str, stats: &[tidb_ingestor_simplesst::MultipleFilesStat], concurrency: isize) -> bool {
    if let Some(v) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/dxf/importinto/forceMergeSort") {
        let input = v.string();
        if input == kv_group || input == "*" {
            return false;
        }
    }
    tidb_ingestor_simplesst::get_max_overlapping_total(stats)
        <= tidb_ingestor_simplesst::get_adjusted_merge_sort_overlap_threshold(concurrency as i64)
}

/// Go `getSortedKVMetasOfEncodeStep`: the data group and each index group.
fn get_sorted_kv_metas_of_encode_step(
    ctx: &tidb_gocontext::Context,
    metas: &[Vec<u8>],
    store: &dyn tidb_objstore::storeapi::Storage,
) -> Result<BTreeMap<String, SortedKVMeta>, Error> {
    let mut data = SortedKVMeta::default();
    let mut indexes: BTreeMap<i64, SortedKVMeta> = BTreeMap::new();
    for bs in metas {
        let mut m: ImportStepMeta = proto::unmarshal(bs)?;
        m.read_external(ctx, store)?;
        data.merge(&m.sorted_data_meta.unwrap_or_default());
        for (id, meta) in m.sorted_index_metas.unwrap_or_default() {
            match indexes.get_mut(&id) {
                Some(item) => item.merge(&meta),
                None => {
                    indexes.insert(id, meta);
                }
            }
        }
    }
    let mut res = BTreeMap::new();
    res.insert(tidb_ingestor_globalsort::DATA_KV_GROUP.to_owned(), data);
    for (id, meta) in indexes {
        res.insert(tidb_ingestor_globalsort::index_id_2_kv_group(id), meta);
    }
    Ok(res)
}

/// Go `getSortedKVMetasOfMergeStep`.
fn get_sorted_kv_metas_of_merge_step(
    ctx: &tidb_gocontext::Context,
    metas: &[Vec<u8>],
    store: &dyn tidb_objstore::storeapi::Storage,
) -> Result<BTreeMap<String, SortedKVMeta>, Error> {
    let mut result: BTreeMap<String, SortedKVMeta> = BTreeMap::new();
    for bs in metas {
        let mut m: MergeSortStepMeta = proto::unmarshal(bs)?;
        m.read_external(ctx, store)?;
        match result.get_mut(&m.kv_group) {
            Some(meta) => meta.merge(&m.sorted_kv_meta),
            None => {
                result.insert(m.kv_group.clone(), m.sorted_kv_meta);
            }
        }
    }
    Ok(result)
}

fn previous<'a>(ctx: &'a PlanCtx<'_>, step: Step) -> &'a [Vec<u8>] {
    ctx.previous_subtask_metas.get(&step).map_or(&[], Vec::as_slice)
}

/// Go `generateMergeSortSpecs`: the groups that overlap too much, their
/// data files divided by `DivideMergeSortDataFiles`.
fn generate_merge_sort_specs(ctx: &PlanCtx<'_>, p: &LogicalPlan) -> Result<Vec<StepSpec>, Error> {
    let store = sort_store(&ctx.ctx, &p.plan)?;
    let kv_metas = get_sorted_kv_metas_of_encode_step(&ctx.ctx, previous(ctx, get_step_of_encode(&p.plan)), store.as_ref())?;
    let mut summary = StepSummary::default();
    let mut result = Vec::new();
    for (kv_group, kv_meta) in &kv_metas {
        if kv_meta.stats().is_empty() {
            tidb_log::info("skip merge-sort for empty kv group", &[str_field("kv-group", kv_group)]);
            continue;
        }
        if !p.plan.force_merge_step && skip_merge_sort(kv_group, kv_meta.stats(), ctx.thread_cnt) {
            tidb_log::info("skip merge sort for kv group", &[int_field("task-id", ctx.task_id), str_field("kv-group", kv_group)]);
            continue;
        }
        summary.bytes += kv_meta.total_kv_size as i64;
        if kv_group == tidb_ingestor_globalsort::DATA_KV_GROUP {
            summary.row_cnt += kv_meta.total_kv_cnt as i64;
        }
        let node_cnt = ctx.execute_nodes_cnt.max(1) as usize;
        let groups = tidb_ingestor_globalsort::divide_merge_sort_data_files(
            &kv_meta.get_data_files(),
            node_cnt,
            ctx.thread_cnt.max(0) as usize,
        )
        .map_err(global_sort_error)?;
        for files in groups {
            result.push(StepSpec::MergeSort(MergeSortStepMeta {
                kv_group: kv_group.clone(),
                data_files: Some(files),
                ..MergeSortStepMeta::default()
            }));
        }
    }
    p.summary.set(summary);
    Ok(result)
}

/// A globalsort error, keeping a Go `*errors.Error`.
pub(crate) fn global_sort_error(e: tidb_ingestor_globalsort::Error) -> Error {
    if let Some(t) = e.downcast_ref::<tidb_terror::TerrorError>() {
        return std::sync::Arc::new(t.clone());
    }
    if let Some(tidb_ingestor_simplesst::Error::Terror(t) | tidb_ingestor_simplesst::Error::FoundDuplicateKeys { error: t, .. }) =
        e.downcast_ref::<tidb_ingestor_simplesst::Error>()
    {
        return std::sync::Arc::new(t.clone());
    }
    plain(e.to_string())
}

/// Go `getSortedKVMetasForIngest`: the merge-sorted groups, and the encode
/// step's groups that skipped the merge sort.
fn get_sorted_kv_metas_for_ingest(
    ctx: &PlanCtx<'_>,
    p: &LogicalPlan,
    store: &dyn tidb_objstore::storeapi::Storage,
) -> Result<BTreeMap<String, SortedKVMeta>, Error> {
    let mut merged = get_sorted_kv_metas_of_merge_step(&ctx.ctx, previous(ctx, IMPORT_STEP_MERGE_SORT), store)?;
    let encoded = get_sorted_kv_metas_of_encode_step(&ctx.ctx, previous(ctx, get_step_of_encode(&p.plan)), store)?;
    for (kv_group, kv_meta) in encoded {
        if !p.plan.force_merge_step && skip_merge_sort(&kv_group, kv_meta.stats(), ctx.thread_cnt) {
            if merged.contains_key(&kv_group) {
                tidb_log::error("kv group of encode step conflict with merge sort step", &[]);
                return Err(plain("kv group of encode step conflict with merge sort step"));
            }
            merged.insert(kv_group, kv_meta);
        }
    }
    Ok(merged)
}

/// Go `generateWriteIngestSpecs`.
fn generate_write_ingest_specs(ctx: &PlanCtx<'_>, p: &LogicalPlan) -> Result<Vec<StepSpec>, Error> {
    let store = sort_store(&ctx.ctx, &p.plan)?;
    let kv_metas = get_sorted_kv_metas_for_ingest(ctx, p, store.as_ref())?;
    // Go `planCtx.Store.CurrentVersion`.
    let import_store = crate::import_store()?;
    let ts = import_store.import_client().map_err(plain)?.get_ts().map_err(|e| plain(e.to_string()))?;
    let mut summary = StepSummary::default();
    let mut specs = Vec::new();
    for (kv_group, kv_meta) in &kv_metas {
        if kv_meta.stats().is_empty() {
            tidb_log::info("skip ingest for empty kv group", &[str_field("kv-group", kv_group)]);
            continue;
        }
        summary.bytes += kv_meta.total_kv_size as i64;
        if kv_group == tidb_ingestor_globalsort::DATA_KV_GROUP {
            summary.row_cnt += kv_meta.total_kv_cnt as i64;
        }
        specs.extend(split_for_one_subtask(&ctx.ctx, &store, kv_group, kv_meta, ts, import_store.as_ref())?);
    }
    p.summary.set(summary);
    Ok(specs)
}

/// Go `splitForOneSubtask`: one write-and-ingest subtask per ranges group.
fn split_for_one_subtask(
    ctx: &tidb_gocontext::Context,
    store: &std::sync::Arc<dyn tidb_objstore::storeapi::Storage>,
    kv_group: &str,
    kv_meta: &SortedKVMeta,
    ts: u64,
    import_store: &dyn crate::ImportStore,
) -> Result<Vec<StepSpec>, Error> {
    let mut splitter = get_range_splitter(ctx, store, kv_meta, import_store)?;
    let result = (|| {
        let mut ret = Vec::new();
        let (mut subtasks, mut total_data_files, mut total_job_keys, mut total_region_keys) = (0, 0, 0, 0);
        let mut start_key = kv_meta.start_key.clone();
        loop {
            let g = splitter.split_one_ranges_group().map_err(global_sort_error)?;
            let end_key = if g.end_key_of_group.is_empty() { kv_meta.end_key.clone() } else { g.end_key_of_group.clone() };
            if start_key >= end_key {
                return Err(plain(format!(
                    "invalid kv range, startKey: {}, endKey: {}",
                    hex::encode(&start_key),
                    hex::encode(&end_key)
                )));
            }
            let mut range_job_keys = Vec::with_capacity(g.interior_range_job_keys.len() + 2);
            range_job_keys.push(start_key.clone());
            range_job_keys.extend(g.interior_range_job_keys.iter().cloned());
            range_job_keys.push(end_key.clone());
            let mut region_split_keys = Vec::with_capacity(g.interior_region_split_keys.len() + 2);
            region_split_keys.push(start_key.clone());
            region_split_keys.extend(g.interior_region_split_keys.iter().cloned());
            region_split_keys.push(end_key.clone());
            subtasks += 1;
            total_data_files += g.data_files.len();
            total_job_keys += g.interior_range_job_keys.len();
            total_region_keys += g.interior_region_split_keys.len();
            ret.push(StepSpec::WriteIngest(WriteIngestStepMeta {
                kv_group: kv_group.to_owned(),
                sorted_kv_meta: SortedKVMeta {
                    start_key: start_key.clone(),
                    end_key: end_key.clone(),
                    total_kv_size: DEFAULT_BATCH_SIZE as u64,
                    ..SortedKVMeta::default()
                },
                data_files: Some(g.data_files),
                stat_files: Some(g.stat_files),
                range_job_keys: Some(range_job_keys),
                range_split_keys: Some(region_split_keys),
                ts,
                ..WriteIngestStepMeta::default()
            }));
            start_key = end_key;
            if g.end_key_of_group.is_empty() {
                break;
            }
        }
        tidb_log::info(
            "kv range split summary",
            &[
                str_field("kvGroup", kv_group),
                int_field("subtasks", subtasks),
                int_field("dataFiles", total_data_files as i64),
                int_field("rangeJobKeys", total_job_keys as i64),
                int_field("regionSplitKeys", total_region_keys as i64),
            ],
        );
        Ok(ret)
    })();
    if let Err(e) = splitter.close() {
        tidb_log::warn("close range splitter failed", &[str_field("error", &e.to_string())]);
    }
    result
}

/// Go `getRangeSplitter`: ranges groups of `DefaultBatchSize`, range jobs
/// of `CalRangeSize`, and the region split size and keys of TiKV, at least
/// the defaults.
fn get_range_splitter(
    ctx: &tidb_gocontext::Context,
    store: &std::sync::Arc<dyn tidb_objstore::storeapi::Storage>,
    kv_meta: &SortedKVMeta,
    import_store: &dyn crate::ImportStore,
) -> Result<tidb_ingestor_globalsort::RangeSplitter, Error> {
    let (mut region_split_size, mut region_split_keys) = import_store.region_split_size_keys().unwrap_or_else(|e| {
        tidb_log::warn("fail to get region split size and keys", &[str_field("error", &e)]);
        (0, 0)
    });
    let (def_size, def_keys) = tidb_dxf_handle::get_default_region_split_config();
    region_split_size = region_split_size.max(def_size);
    region_split_keys = region_split_keys.max(def_keys);
    let node = tidb_dxf_storage::get_node_resource();
    let (range_size, range_keys) = tidb_ingestor_globalsort::cal_range_size(
        node.total_mem / node.total_cpu.max(1) as i64,
        region_split_size,
        region_split_keys,
    );
    tidb_log::info(
        "split kv range with split size and keys",
        &[
            int_field("region-split-size", region_split_size),
            int_field("region-split-keys", region_split_keys),
            int_field("range-size", range_size),
            int_field("range-keys", range_keys),
        ],
    );
    tidb_ingestor_globalsort::RangeSplitter::new(
        ctx,
        kv_meta.stats().to_vec(),
        std::sync::Arc::clone(store),
        DEFAULT_BATCH_SIZE,
        i64::MAX,
        range_size,
        range_keys,
        region_split_size,
        region_split_keys,
    )
    .map_err(global_sort_error)
}

/// Go `generateCollectConflictsSpecs`: no subtask when nothing conflicted.
fn generate_collect_conflicts_specs(ctx: &PlanCtx<'_>, p: &LogicalPlan) -> Result<Vec<StepSpec>, Error> {
    let store = sort_store(&ctx.ctx, &p.plan)?;
    let infos = collect_conflict_infos(ctx, store.as_ref(), get_step_of_encode(&p.plan))?;
    p.summary.set(StepSummary { row_cnt: total_conflicts(&infos), ..StepSummary::default() });
    if infos.infos().is_empty() {
        return Ok(Vec::new());
    }
    let recorded_data_kv_conflicts =
        infos.infos().get(tidb_ingestor_globalsort::DATA_KV_GROUP).map_or(0, |i| i.count as i64);
    Ok(vec![StepSpec::CollectConflicts(CollectConflictsStepMeta {
        infos,
        recorded_data_kv_conflicts,
        ..CollectConflictsStepMeta::default()
    })])
}

/// Go `generateConflictResolutionSpecs`.
fn generate_conflict_resolution_specs(ctx: &PlanCtx<'_>, p: &LogicalPlan) -> Result<Vec<StepSpec>, Error> {
    let store = sort_store(&ctx.ctx, &p.plan)?;
    let infos = collect_conflict_infos(ctx, store.as_ref(), get_step_of_encode(&p.plan))?;
    p.summary.set(StepSummary { row_cnt: total_conflicts(&infos), ..StepSummary::default() });
    if infos.infos().is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![StepSpec::ConflictResolution(ConflictResolutionStepMeta { infos, ..ConflictResolutionStepMeta::default() })])
}

/// Go `collectConflictInfos`: the conflicts the encode, merge-sort and
/// write-and-ingest steps recorded, by KV group.
fn collect_conflict_infos(
    ctx: &PlanCtx<'_>,
    store: &dyn tidb_objstore::storeapi::Storage,
    encode_step: Step,
) -> Result<KVGroupConflictInfos, Error> {
    let mut m = KVGroupConflictInfos::default();
    for bs in previous(ctx, encode_step) {
        let mut meta: ImportStepMeta = proto::unmarshal(bs)?;
        if meta.recorded_conflict_kv_count == 0 {
            continue;
        }
        meta.read_external(&ctx.ctx, store)?;
        m.add_data_conflict_info(&meta.sorted_data_meta.unwrap_or_default().conflict_info);
        for (id, kv_meta) in meta.sorted_index_metas.unwrap_or_default() {
            m.add_index_conflict_info(id, &kv_meta.conflict_info);
        }
    }
    for bs in previous(ctx, IMPORT_STEP_MERGE_SORT) {
        let mut meta: MergeSortStepMeta = proto::unmarshal(bs)?;
        if meta.recorded_conflict_kv_count == 0 {
            continue;
        }
        meta.read_external(&ctx.ctx, store)?;
        m.add_conflict_info(&meta.kv_group, &meta.sorted_kv_meta.conflict_info);
    }
    for bs in previous(ctx, IMPORT_STEP_WRITE_AND_INGEST) {
        let mut meta: WriteIngestStepMeta = proto::unmarshal(bs)?;
        if meta.recorded_conflict_kv_count == 0 {
            continue;
        }
        meta.read_external(&ctx.ctx, store)?;
        m.add_conflict_info(&meta.kv_group, &meta.sorted_kv_meta.conflict_info);
    }
    Ok(m)
}

/// Go `totalConflicts`, saturating at `i64::MAX`.
fn total_conflicts(infos: &KVGroupConflictInfos) -> i64 {
    let mut total: i64 = 0;
    for info in infos.infos().values() {
        if info.count > (i64::MAX - total) as u64 {
            return i64::MAX;
        }
        total += info.count as i64;
    }
    total
}

fn str_field(key: &'static str, value: &str) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::Str(value.to_owned()))
}

fn int_field(key: &'static str, value: i64) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::I64(value))
}

use tidb_ingestor_globalsort::SortedKVMeta;
