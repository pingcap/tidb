// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/planner.go`: the logical plan of an import and
//! the subtask metas of each step. Local sort has the `import` and
//! `post-process` steps.

use std::collections::BTreeMap;

use tidb_dxf::{ExtraParams, Step, IMPORT_STEP_COLLECT_CONFLICTS, IMPORT_STEP_IMPORT, IMPORT_STEP_POST_PROCESS};
use tidb_dxf_planner::{LinkSpec, OutputSpec, PhysicalPlan, PipelineSpec, PlanCtx, PlanError, ProcessorSpec};
use tidb_executor::importer::chunk_process::Chunk;
use tidb_executor::importer::job::StepSummary;
use tidb_executor::importer::{ASTArgs, LoadDataController, Plan};

use crate::proto::{self, Checksum, CollectConflictsStepMeta, ImportStepMeta, PostProcessStepMeta, TaskMeta};
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
            ..TaskMeta::default()
        }
    }
}
impl tidb_dxf_planner::LogicalPlan for LogicalPlan {
    fn get_task_extra_params(&self) -> ExtraParams {
        ExtraParams { manual_recovery: self.plan.manual_recovery, ..ExtraParams::default() }
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
        Ok(())
    }

    fn to_physical_plan(&self, ctx: &PlanCtx<'_>) -> Result<PhysicalPlan, PlanError> {
        let mut physical = PhysicalPlan::default();
        let mut input_links = Vec::new();
        match ctx.next_task_step {
            IMPORT_STEP_IMPORT => {
                let specs = generate_import_specs(ctx, self).map_err(boxed)?;
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
            IMPORT_STEP_POST_PROCESS => {
                physical.add_processor(ProcessorSpec {
                    id: input_links.len() as isize,
                    input: tidb_dxf_planner::InputSpec { column_types: Vec::new(), links: input_links },
                    pipeline: Box::new(PostProcessSpec { encode_step: get_step_of_encode(&self.plan) }),
                    output: OutputSpec::default(),
                    step: ctx.next_task_step,
                });
            }
            step => return Err(boxed(plain(format!("unsupported import step {}", step.0)))),
        }
        Ok(physical)
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

/// Go `ImportSpec`.
pub struct ImportSpec {
    /// Go `ImportStepMeta`.
    pub meta: ImportStepMeta,
}

impl PipelineSpec for ImportSpec {
    fn to_subtask_meta(&self, _: &PlanCtx<'_>) -> Result<Vec<u8>, PlanError> {
        proto::marshal(&self.meta).map_err(boxed)
    }
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
fn generate_import_specs(ctx: &PlanCtx<'_>, p: &LogicalPlan) -> Result<Vec<ImportSpec>, Error> {
    let chunk_map = match &p.chunk_map {
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
        specs.push(ImportSpec { meta: ImportStepMeta { id, chunks: Some(chunks), ..ImportStepMeta::default() } });
    }
    p.summary.set(summary);
    Ok(specs)
}
