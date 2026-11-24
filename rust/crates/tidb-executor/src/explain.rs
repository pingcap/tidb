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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go-shaped `EXPLAIN` and `EXPLAIN ANALYZE` over retained physical plans.
//!
//! Fresh queries, prepared cache hits, and DML all render the same physical
//! tree that `physical_builder` lowers through the ordinary executor switch.
//! `EXPLAIN ANALYZE` attaches row counters while draining that executor tree;
//! plain `EXPLAIN` only renders it and never executes the target statement.


mod exec;
pub use exec::*;
use tidb_planner::stmt_ctx::PlannerStmtCtx as _;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_planner::explain::{
    Explain as PlannerExplain, ExplainContext as PlannerExplainContext, ExplainOperator,
};
use tidb_planner_core_operator_physicalop::PhysicalPlan;

use tidb_planner::find_best_task::coster::Ver2Coster;

use crate::driver::{run_delete_stmt_with_physical, run_insert_stmt_with_physical_and_visits, run_update_stmt_with_physical, DriverError, SelectMeta};
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_sessionctx::explain_format::ExplainFormat;
#[allow(unused_imports)]
use tidb_planner::flat_plan::*;
pub use tidb_planner::flat_plan::{brief_binary_plan, normalize_flat_plan};















































































/// The plan-derived fields Go publishes while the ordinary executor is
/// constructed.
#[must_use]
pub fn process_plan_info(
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
) -> crate::ProcessPlanInfo {
    process_plan_info_with_brief(statement_context, physical, catalog, true)
}

/// Go `getEncodedPlan`'s `EncodeFlatPlan` and `getBinaryPlan`'s
/// `BinaryPlanStrFromFlatPlan(flat, false)` of `physical`, each operator with
/// the runtime information the statement's `RuntimeStatsColl` and trackers
/// hold for it (`getRuntimeInfoStr`, `binaryOpFromFlatOp`).
fn plan_encodings(
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
) -> (String, String) {
    let full = physical_explain_roots(
        None,
        statement_context,
        statement_context,
        false,
        physical,
        catalog,
        statement_context.executor_runtime_stats_source().is_some(),
        false,
        &[],
    );
    let mut encoded = String::new();
    for root in &full {
        encode_observed_operator(root, 0, &mut encoded);
    }
    let encoded_plan = if encoded.is_empty() {
        String::new()
    } else {
        tidb_plancodec::compress(encoded.as_bytes())
    };
    let coster = Ver2Coster::from_env(statement_context.optimizer_cost_env())
        .with_scan_penalty_vars(
            statement_context.allow_prefer_range_scan(),
            statement_context.get_index_force(),
        )
        .with_explain_cache();
    let costed = physical_explain_roots(
        Some(&coster),
        statement_context,
        statement_context,
        false,
        physical,
        catalog,
        false,
        false,
        &[],
    );
    (encoded_plan, explain_binary_plan(statement_context, costed))
}

/// Derives and publishes the process-info fields for `physical`, rendering
/// the brief binary plan only when this statement publishes one. Go
/// `ExecStmt.Exec`'s runaway check runs on the plan digest before the
/// executor is built.
///
/// # Errors
/// The quarantine error of a KILL runaway watch.
pub fn publish_physical_process_info(
    ctx: &crate::StmtContext,
    physical: &tidb_planner_core_operator_physicalop::PhysicalPlan,
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
) -> Result<(), tidb_driver_error::DriverError> {
    let Some(execution_started) = ctx.begin_physical_process_info() else {
        return Ok(());
    };
    let plan = process_plan_info_with_brief(
        ctx,
        physical,
        catalog,
        ctx.publishes_brief_binary_plan(),
    );
    ctx.check_runaway_before_executor(&plan.plan_digest)?;
    ctx.finish_physical_process_info(plan, Some(execution_started));
    Ok(())
}

/// [`process_plan_info`] with the brief binary plan published only when
/// `with_brief` is set. Summary plans and table/index/statistics metadata remain
/// independent of binary prepared process-list visibility.
#[must_use]
pub fn process_plan_info_with_brief(
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    with_brief: bool,
) -> crate::ProcessPlanInfo {
    let stored = std::sync::Arc::new(physical.deep_clone());
    // The summary and the slow log render the encodings when they sample or
    // write the plan (Go `LazyInfo`), after the statement ran.
    let plan_source = {
        let ctx = statement_context.clone().without_process_plan_info();
        let catalog = catalog.clone();
        let plan = std::sync::Arc::clone(&stored);
        crate::PlanSource(std::sync::Arc::new(move || {
            Some(plan_encodings(&ctx, &plan, &catalog))
        }))
    };
    let (normalized_plan, plan_digest) = match statement_context.cached_plan_digest() {
        Some(cached) => cached
            .get_or_init(|| normalize_flat_plan(statement_context, physical, catalog))
            .clone(),
        None => normalize_flat_plan(statement_context, physical, catalog),
    };
    let fast_candidate = match physical {
        PhysicalPlan::Projection(projection) => {
            projection.base.children().first().unwrap_or(physical)
        }
        _ => physical,
    };
    let mut info = crate::ProcessPlanInfo {
        // The process list reads the brief binary plan while the statement
        // runs (Go `SetProcessInfo` before `ExecStmt.Exec`).
        brief_binary_plan: if with_brief {
            brief_binary_plan(statement_context, physical, catalog)
        } else {
            String::new()
        },
        brief_plan_source: with_brief.then(|| {
            let ctx = statement_context.clone().without_process_plan_info();
            let catalog = catalog.clone();
            let plan = std::sync::Arc::clone(&stored);
            tidb_stmtctx::BriefPlanSource(std::sync::Arc::new(move || {
                brief_binary_plan(&ctx, &plan, &catalog)
            }))
        }),
        normalized_plan,
        plan_digest,
        plan_hints: tidb_planner::plan_hint::gen_hints_from_physical_plan(
            physical,
            &CatalogHints(catalog),
            &statement_context.block_as_names(),
        ),
        is_fast_plan: matches!(
            fast_candidate,
            PhysicalPlan::PointGet(_) | PhysicalPlan::TableDual(_)
        ),
        // Go `runStmt`: a binary-protocol EXECUTE's `currentPlan` is nil, so
        // its process info publishes no plan.
        stmt_plan: Some(crate::StoredPlan::new(std::sync::Arc::clone(&stored))),
        plan: with_brief.then(|| crate::StoredPlan::new(stored)),
        // Go `SetProcessInfo`'s `RuntimeStatsColl: sc.RuntimeStatsColl`.
        runtime_stats_coll: statement_context
            .executor_runtime_stats_source()
            .filter(|_| with_brief)
            .map(|source| crate::StoredPlan::new(std::sync::Arc::new(std::sync::Arc::clone(source)))),
        statement_memory: Some(crate::StoredPlan::new(std::sync::Arc::new(statement_context.statement_memory()))),
        plan_source: Some(plan_source),
        in_execute: statement_context.in_execute(),
        ..crate::ProcessPlanInfo::default()
    };
    collect_executor_process_fields(
        physical,
        catalog,
        &mut info.table_ids,
        &mut info.index_names,
    );
    collect_stats_info(physical, statement_context, catalog, &mut info.stats_info);
    info
}



/// Go `ExplainExec.executeAnalyzeExec`'s `RegisterStats(targetPlan.ID(),
/// ruRuntimeStats)`: the statement's RU, registered on the analyzed plan
/// once it ran -- before a read's executors close, after a write's.
pub(crate) fn register_ru_runtime_stats(statement_context: &crate::StmtContext, plan_id: i64) {
    let Some(source) = statement_context.executor_runtime_stats_source() else {
        return;
    };
    let ru_details = statement_context.ru_details();
    let metrics = statement_context.ruv2_metrics();
    if let Some(metrics) = metrics {
        tidb_execdetails::ruv2_metrics::sync_ruv_2_metrics_from_ru_details(
            Some(metrics.as_ref()),
            ru_details.as_deref(),
        );
    }
    if ru_details.is_some() || metrics.is_some() {
        source.register_stats(
            plan_id,
            Box::new(tidb_execdetails::ruv2_metrics::RuRuntimeStats {
                ru_details: ru_details.as_ref().map(|details| details.cloned()),
                metrics: metrics.map(|metrics| (**metrics).clone()),
                weights: tidb_variable::slow_log::ruv2_weights(),
                // Go `do.GetRUVersion()`.
                ru_version: tidb_topsql_stmtstats::bound_ru_version(),
            }),
        );
    }
}

fn render_physical_plan(
    eval_ctx: &dyn tidb_exprctx::Columns,
    statement_context: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    format: ExplainFormat,
    analyze: bool,
    runtime: bool,
    scalar_subqueries: &[std::sync::Arc<crate::driver::planner_bridge::ScalarSubqueryEvalCtx>],
) -> Result<SelectMeta, DriverError> {
    if matches!(format, ExplainFormat::Dot) {
        return Ok(explain_dot_rows(physical));
    }
    if matches!(format, ExplainFormat::TidbJson | ExplainFormat::Binary) {
        // Go's binary operator carries `getPlanCost` (cost model ver2).
        let coster = matches!(format, ExplainFormat::Binary).then(|| {
            Ver2Coster::from_env(statement_context.optimizer_cost_env())
                .with_scan_penalty_vars(
                    statement_context.allow_prefer_range_scan(),
                    statement_context.get_index_force(),
                )
                .with_explain_cache()
        });
        let roots = physical_explain_roots(
            coster.as_ref(),
            eval_ctx,
            statement_context,
            false,
            physical,
            catalog,
            runtime,
            false,
            scalar_subqueries,
        );
        let field_type = FieldType::new(FieldTypeCode::VarString);
        return Ok(if matches!(format, ExplainFormat::TidbJson) {
            (
                vec![("TiDB_JSON".to_owned(), field_type)],
                vec![vec![text(&explain_tidb_json(&roots, analyze))]],
            )
        } else {
            (
                vec![("binary plan".to_owned(), field_type)],
                vec![vec![text(&explain_binary_plan(statement_context, roots))]],
            )
        });
    }
    if matches!(format, ExplainFormat::Hint) {
        // Go `ExplainFormatHint`: the hints that reproduce the plan, then the
        // hints the statement itself wrote.
        let mut hints = tidb_planner::plan_hint::gen_hints_from_physical_plan(
            physical,
            &CatalogHints(catalog),
            &statement_context.block_as_names(),
        );
        hints.extend(statement_context.explain_table_hints().iter().cloned());
        let varchar = FieldType::new(FieldTypeCode::VarString);
        return Ok((
            vec![("hint".to_owned(), varchar)],
            vec![vec![text(&tidb_hint::restore_optimizer_hints(&hints))]],
        ));
    }
    let ignore_explain_id_suffix = matches!(format, ExplainFormat::Brief | ExplainFormat::PlanTree);
    let coster = (matches!(
        format,
        ExplainFormat::Verbose | ExplainFormat::CostTrace
    ))
    .then(|| {
        let coster = Ver2Coster::from_env(statement_context.optimizer_cost_env())
            .with_scan_penalty_vars(
                statement_context.allow_prefer_range_scan(),
                statement_context.get_index_force(),
            )
            .with_explain_cache();
        if matches!(format, ExplainFormat::CostTrace) {
            // go prices `ExplainFormatCostTrace` with
            // `CostFlagTrace` so every operator's `CostVer2` carries
            // its formula decomposition.
            let mut option = tidb_planner_costusage::PlanCostOption::new();
            option.with_cost_flag(tidb_planner_costusage::COST_FLAG_TRACE);
            coster.with_cost_option(option)
        } else {
            coster
        }
    });
    let roots = physical_explain_roots(
        coster.as_ref(),
        eval_ctx,
        statement_context,
        false,
        physical,
        catalog,
        runtime,
        ignore_explain_id_suffix,
        scalar_subqueries,
    );

    let mut rows = Vec::new();
    let mut columns = Vec::new();
    for root in roots {
        let mut explain = PlannerExplain::new(planner_explain_format(format), analyze, root);
        let mut explain_context = PlannerExplainContext::default();
        let schema = explain
            .prepare_schema(&mut explain_context)
            .map_err(|error| DriverError::unsupported(error.to_string()))?;
        let rendered = explain
            .render_result(&mut explain_context)
            .map_err(|error| DriverError::unsupported(error.to_string()))?
            .to_vec();
        if columns.is_empty() {
            let field_type = FieldType::new(FieldTypeCode::VarString);
            columns = schema
                .field_names
                .into_iter()
                .map(|name| (name.to_owned(), field_type.clone()))
                .collect();
        }
        rows.extend(
            rendered
                .iter()
                .map(|row| row.iter().map(|value| text(value)).collect::<Vec<_>>()),
        );
    }
    Ok((columns, rows))
}







/// go `Explain.prepareDotInfo` (`pkg/planner/core/common_plans.go:1341`): the
/// plan rendered as one GraphViz `digraph` document in a single TEXT cell.
/// Each task opens a `cluster<N>` subgraph labelled with its task type, then
/// walks its operators breadth-first writing one `"parent" -> "child"` edge
/// per child; reader/cop splits (go's `copTasks`/`pipelines` queues) do not
/// arise at this tier, where a reader is an ordinary node of the same tree.
fn explain_dot_rows(physical: &PhysicalPlan) -> SelectMeta {
    let mut buffer = String::new();
    buffer.push_str(&format!("\ndigraph {} {{\n", physical.explain_id(false)));
    prepare_task_dot(physical, "root", &mut buffer);
    buffer.push_str("}\n");
    let field_type = FieldType::new(FieldTypeCode::VarString);
    (
        vec![("dot contents".to_owned(), field_type)],
        vec![vec![text(&buffer)]],
    )
}

/// go `Explain.prepareTaskDot`: one cluster per task, breadth-first edges
/// inside it.
fn prepare_task_dot(plan: &PhysicalPlan, task_tp: &str, buffer: &mut String) {
    buffer.push_str(&format!("subgraph cluster{}{{\n", plan.id()));
    buffer.push_str("node [style=filled, color=lightgrey]\n");
    buffer.push_str("color=black\n");
    buffer.push_str(&format!("label = \"{}\"\n", task_tp));
    if plan.children().is_empty() {
        buffer.push_str(&format!("\"{}\"\n", plan.explain_id(false)));
    }
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(plan);
    while let Some(current) = queue.pop_front() {
        for child in current.children() {
            buffer.push_str(&format!(
                "\"{}\" -> \"{}\"\n",
                current.explain_id(false),
                child.explain_id(false)
            ));
            queue.push_back(child);
        }
    }
    buffer.push_str("}\n");
}

/// Go `buildExplain` for a plain EXPLAIN: the target plan is built, not run
/// (`analyzeExec = b.build(v.TargetPlan)`), and then reported.
fn render_built_plan(
    ctx: &crate::StmtContext,
    physical: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    format: ExplainFormat,
    scalar_subqueries: &[std::sync::Arc<crate::driver::planner_bridge::ScalarSubqueryEvalCtx>],
) -> Result<SelectMeta, DriverError> {
    // Go `ExecStmt.Exec` over the `Explain` plan: its target's plan digest
    // is published (and runaway-checked) before the executor is built.
    publish_physical_process_info(ctx, physical, catalog)?;
    crate::driver::physical_builder::build_for_explain(physical, catalog, ctx)?;
    render_physical_plan(ctx, ctx, physical, catalog, format, false, false, scalar_subqueries)
}

fn render_physical_query(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
    analyze: bool,
) -> Result<SelectMeta, DriverError> {
    tidb_planner::preprocess::validate_query_usage(query, ctx)?;
    let (mut physical, scalar_subqueries) =
        crate::driver::optimize_query_stmt_with_scalar_subqueries(query, catalog, current_db, ctx)?;
    if analyze {
        crate::driver::physical_builder::prepare_execution_plan(&mut physical, catalog, ctx)?;
    } else {
        crate::driver::physical_builder::prepare_plan_for_explain(&mut physical, catalog, ctx)?;
        publish_physical_process_info(ctx, &physical, catalog)?;
        crate::driver::physical_builder::build_for_explain(&physical, catalog, ctx)?;
    }
    if analyze {
        crate::driver::physical_builder::execute_for_explain(
            &mut physical,
            catalog,
            ctx,
            &explain_field_types(format, true)?,
        )?;
    }
    render_physical_plan(
        ctx,
        ctx,
        &physical,
        catalog,
        format,
        analyze,
        analyze,
        &scalar_subqueries,
    )
}

/// Go master `Explain.RenderResult` over `MViewCompleteDeltaApply`, the
/// plan of the internal `RefreshMaterializedViewImplementStmt`: the root
/// with Go's `ExplainInfo` over the optimized diff source.
pub fn explain_mview_complete_delta_apply(
    plan: &tidb_planner::common_plans::MViewCompleteDeltaApply,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut physical = plan
        .root
        .clone()
        .ok_or_else(|| DriverError::from(tidb_terror::MysqlError::unknown("MViewCompleteDeltaApply: the plan is not optimized")))?;
    crate::driver::physical_builder::prepare_plan_for_explain(&mut physical, catalog, ctx)?;
    render_built_plan(ctx, &physical, catalog, format, &[])
}

/// Go master `Explain.RenderResult` over `MViewDeltaMerge`, FAST refresh's
/// internal `RefreshMaterializedViewImplementStmt` plan: the operator with
/// Go's `ExplainInfo` over the optimized merge source.
pub fn explain_mview_delta_merge(
    plan: &tidb_planner::common_plans::MViewDeltaMerge,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut physical = crate::driver::mview_delta_merge::planned(plan)?.root.clone();
    crate::driver::physical_builder::prepare_plan_for_explain(&mut physical, catalog, ctx)?;
    render_built_plan(ctx, &physical, catalog, format, &[])
}

/// Reports a plan the statement already has -- EXPLAIN FORMAT =
/// 'plan_cache' over the plan the non-prepared cache gave it -- as EXPLAIN
/// rows, executing nothing.
pub fn explain_physical_plan(
    physical: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut physical = physical.deep_clone();
    crate::driver::physical_builder::prepare_plan_for_explain(&mut physical, catalog, ctx)?;
    render_built_plan(ctx, &physical, catalog, format, &[])
}

/// `EXPLAIN [FORMAT = f] FOR CONNECTION`: Go renders the target's retained
/// plan with the runtime statistics the target collected (`ctx` carries the
/// target's `RuntimeStatsColl`), so the result has the analyze columns.
pub fn explain_connection_plan(
    physical: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let mut physical = physical.deep_clone();
    crate::driver::physical_builder::prepare_plan_for_explain(&mut physical, catalog, ctx)?;
    // Go `Explain.RenderResult`: the analyze rows, from the target's
    // `RuntimeStatsColl`, when it has one (`ctx` carries it).
    let runtime = ctx.executor_runtime_stats_source().is_some();
    render_physical_plan(ctx, ctx, &physical, catalog, format, runtime, runtime, &[])
}

/// Go `Explain.RenderResult` for a target whose brief binary plan it decodes
/// (`shouldDecodeBriefBinaryPlan`): `prepareSchema`'s columns for the format
/// and whether the target has a collector, and `DecodeBinaryPlan4Connection`'s
/// rows.
///
/// Go `PhysicalExplain` schema's field types: one string per field name of
/// the format.
fn explain_field_types(format: ExplainFormat, analyze: bool) -> Result<Vec<FieldType>, DriverError> {
    let explain = PlannerExplain::new(
        planner_explain_format(format),
        analyze,
        ExplainOperator::new(String::new(), 0),
    );
    let schema = explain
        .prepare_schema(&mut PlannerExplainContext::default())
        .map_err(|error| DriverError::unsupported(error.to_string()))?;
    Ok(vec![FieldType::new(FieldTypeCode::VarString); schema.field_names.len()])
}

/// # Errors
/// A brief plan that does not decode.
pub fn explain_connection_brief_plan(
    brief_binary_plan: &str,
    format: ExplainFormat,
    runtime: bool,
) -> Result<SelectMeta, DriverError> {
    let mut explain = PlannerExplain::new(
        planner_explain_format(format),
        false,
        ExplainOperator::new(String::new(), 0),
    );
    explain.runtime_stats = runtime;
    let schema = explain
        .prepare_schema(&mut PlannerExplainContext::default())
        .map_err(|error| DriverError::unsupported(error.to_string()))?;
    let field_type = FieldType::new(FieldTypeCode::VarString);
    let columns = schema
        .field_names
        .into_iter()
        .map(|name| (name.to_owned(), field_type.clone()))
        .collect();
    let rows = tidb_plancodec::decode_binary_plan_for_connection(
        brief_binary_plan,
        planner_explain_format(format).as_str(),
        false,
    )
    .map_err(|error| DriverError::unsupported(error.to_string()))?
    .iter()
    .map(|row| row.iter().map(|value| text(value)).collect())
    .collect();
    Ok((columns, rows))
}

/// Go `buildExplain`: an analyzed statement renders from the statement's
/// `RuntimeStatsColl`, a fresh one whatever `tidb_enable_collect_execution_info`
/// says when the statement collects none, fed by its coprocessor responses.
pub(crate) fn explain_analyze_context(ctx: &crate::StmtContext) -> crate::StmtContext {
    if ctx.executor_runtime_stats_source().is_some() {
        return ctx.clone();
    }
    let coll = std::sync::Arc::new(tidb_execdetails::runtime_stats::RuntimeStatsColl::new(None));
    let sink: std::sync::Arc<dyn crate::CopResponseSink> = std::sync::Arc::new(
        crate::statement_cop_stats::RuntimeStatsCollSink::new(
            std::sync::Arc::clone(&coll),
            ctx.cop_response_sink(),
        ),
    );
    ctx.clone()
        .with_executor_runtime_stats_source(Some(coll))
        .with_cop_response_sink(Some(sink))
}

/// `EXPLAIN ANALYZE FORMAT = 'plan_cache'` over the plan the non-prepared
/// cache gave the statement: that plan RUNS, and each operator reports what
/// it produced, as [`explain_analyze_select_stmt`] does for a fresh plan.
pub fn explain_analyze_physical_plan(
    physical: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let analyzed = explain_analyze_context(ctx);
    let ctx = &analyzed;
    let mut physical = physical.deep_clone();
    crate::driver::physical_builder::prepare_execution_plan(&mut physical, catalog, ctx)?;
    crate::driver::physical_builder::execute_for_explain(
        &mut physical,
        catalog,
        ctx,
        &explain_field_types(format, true)?,
    )?;
    render_physical_plan(ctx, ctx, &physical, catalog, format, true, true, &[])
}

/// Plans `select` and reports the plan as EXPLAIN rows, executing nothing:
/// the driver builds the pipeline, the trace records it, and it is dropped
/// undrained.
pub fn explain_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    // Go `PlanBuilder.Build` for the EXPLAIN itself (`buildExplain`).
    crate::driver::record_plan_builder_metric(ctx);
    render_physical_query(
        &tidb_ast::QueryStmt::Select(Box::new(select.clone())),
        catalog,
        current_db,
        ctx,
        format,
        false,
    )
}

/// The Index Advisor's `queryPlanCost`
/// ([`tidb_planner::statement_plan::query_plan_cost`]).
pub fn query_plan_cost(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<f64, DriverError> {
    tidb_planner::statement_plan::query_plan_cost(
        &crate::driver::planner_bridge::driver_hooks(catalog),
        query,
        catalog,
        current_db,
        ctx,
    )
}




/// Plain `EXPLAIN` over a supported set operation.
pub fn explain_set_opr_stmt(
    set_opr: &tidb_ast::SetOprStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    // Go `PlanBuilder.Build` for the EXPLAIN itself (`buildExplain`).
    crate::driver::record_plan_builder_metric(ctx);
    render_physical_query(
        &tidb_ast::QueryStmt::SetOpr(Box::new(set_opr.clone())),
        catalog,
        current_db,
        ctx,
        format,
        false,
    )
}

/// `EXPLAIN ANALYZE <select>`: the same plan [`explain_select_stmt`] records,
/// but the query actually RUNS (real `EXPLAIN ANALYZE` executes the wrapped
/// statement to gather its runtime counters, confirmed by capture), and each
/// operator's `actRows` is the REAL row count that stage produced -- metered
/// by the trace on the executor it recorded, during that one execution.
/// Nothing is estimated, and nothing is run a second time to be counted.
pub fn explain_analyze_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let analyzed = explain_analyze_context(ctx);
    let ctx = &analyzed;
    // Go `PlanBuilder.Build` for the EXPLAIN itself (`buildExplain`).
    crate::driver::record_plan_builder_metric(ctx);
    render_physical_query(
        &tidb_ast::QueryStmt::Select(Box::new(select.clone())),
        catalog,
        current_db,
        ctx,
        format,
        true,
    )
}

/// `EXPLAIN ANALYZE` over a supported set operation.
pub fn explain_analyze_set_opr_stmt(
    set_opr: &tidb_ast::SetOprStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    let analyzed = explain_analyze_context(ctx);
    let ctx = &analyzed;
    // Go `PlanBuilder.Build` for the EXPLAIN itself (`buildExplain`).
    crate::driver::record_plan_builder_metric(ctx);
    render_physical_query(
        &tidb_ast::QueryStmt::SetOpr(Box::new(set_opr.clone())),
        catalog,
        current_db,
        ctx,
        format,
        true,
    )
}

/// Plans an `INSERT` and reports the plan as EXPLAIN rows, executing nothing:
/// the driver returns before it writes. Go's `Insert_N` row carries none of
/// the estimate/access/info a read operator would (captured:
/// `[Insert_1 N/A root  N/A]`, both for a plain `VALUES` insert and for the
/// `Insert ... SELECT` form, where the select's own plan appears as
/// `Insert`'s one child).
pub fn explain_insert_stmt(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    // Go `PlanBuilder.Build` for the EXPLAIN itself (`buildExplain`).
    crate::driver::record_plan_builder_metric(ctx);
    // Go `buildInsert` resolves the target columns and checks a VALUES list's
    // width before it builds an INSERT ... SELECT's source.
    crate::driver::validate_insert_for_explain(insert, catalog, current_db, ctx, None)?;
    let (physical, planned) =
        crate::driver::build_insert_plan(insert, catalog, current_db, ctx, false)?;
    crate::table_privilege::check_plan_privilege(ctx, catalog, &planned.visits)?;
    // The source's own width: Go appends ON DUPLICATE fields past it.
    let select_width = match &physical {
        PhysicalPlan::Dml(root) => root.select_plan.as_deref().and_then(|select| {
            root.insert_select
                .as_ref()
                .and_then(|output| output.actual_col_len)
                .or_else(|| select.schema().map(|schema| schema.columns.len()))
        }),
        _ => None,
    };
    crate::driver::validate_insert_for_explain(insert, catalog, current_db, ctx, select_width)?;
    render_built_plan(ctx, &physical, catalog, format, &planned.scalar_subqueries)
}

/// An `EXPLAIN ANALYZE` write whose analyze child ran (Go `handleNoDelay`
/// drained the `ExplainExec`'s write): its RU stats register and its rows
/// render when its record set is read (`ExplainExec.executeAnalyzeExec`,
/// `RenderResult`), which for an autocommit statement is after `runStmt`
/// committed it (`IsExplainAnalyzeDML`).
pub struct AnalyzedWrite {
    ctx: crate::StmtContext,
    physical: PhysicalPlan,
    catalog: SessionExtendedInfoSchema,
    format: ExplainFormat,
    /// Go `SessionVars.MapScalarSubQ`: what the write's read evaluated.
    scalar_subqueries: Vec<std::sync::Arc<crate::driver::planner_bridge::ScalarSubqueryEvalCtx>>,
}

impl AnalyzedWrite {
    /// Go `executeAnalyzeExec`'s `RURuntimeStats` registration, then
    /// `RenderResult`.
    ///
    /// # Errors
    /// A plan that does not render.
    pub fn render(self) -> Result<SelectMeta, DriverError> {
        register_ru_runtime_stats(&self.ctx, i64::from(self.physical.id()));
        render_physical_plan(
            &self.ctx,
            &self.ctx,
            &self.physical,
            &self.catalog,
            self.format,
            true,
            true,
            &self.scalar_subqueries,
        )
    }
}

/// `EXPLAIN ANALYZE <insert>`: unlike [`explain_insert_stmt`], this really
/// inserts the row(s) -- real `EXPLAIN ANALYZE INSERT` executes the
/// statement (captured: the table has the new row afterward). The `Insert_N`
/// node's `actRows` is always `0`: Go's own `Insert_1` row shows `actRows`
/// `0` too (captured), because the insert executor's `Next()` produces no
/// rows of its own -- the write is a side effect, not this operator's
/// row-producing interface.
///
/// An `INSERT ... SELECT` source's `actRows` is metered on the SAME run that
/// feeds the insert, so a row this statement is about to write can never be
/// counted as one it read, and the source is read exactly once.
pub fn explain_analyze_insert_stmt(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<AnalyzedWrite, DriverError> {
    let analyzed = explain_analyze_context(ctx);
    let ctx = &analyzed;
    // Go `PlanBuilder.Build` for the EXPLAIN itself (`buildExplain`).
    crate::driver::record_plan_builder_metric(ctx);
    let (mut physical, planned) =
        crate::driver::build_insert_plan(insert, catalog, current_db, ctx, false)?;
    let scalar_subqueries = planned.scalar_subqueries.clone();
    // The executor consumes the expressions this builder already planned.
    run_insert_stmt_with_physical_and_visits(
        insert,
        catalog,
        current_db,
        ctx,
        Some(&mut physical),
        Some(planned),
    )?;
    // The write ran and closed (Go `handleNoDelayExecutor`).
    Ok(AnalyzedWrite {
        ctx: analyzed.clone(),
        physical,
        catalog: catalog.clone(),
        format,
        scalar_subqueries,
    })
}

/// Plans an `UPDATE` and reports the plan as EXPLAIN rows, executing nothing.
/// `Update_N`'s one child is the read the driver performs to find the rows to
/// update -- a `Point_Get`, a `TableRangeScan` or a `TableFullScan`, whichever
/// `driver::access::write_read_path` chose -- with a `Selection` above it for
/// the `WHERE` (divergences 7 and 8).
pub fn explain_update_stmt(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    // Go `PlanBuilder.Build` for the EXPLAIN itself (`buildExplain`).
    crate::driver::record_plan_builder_metric(ctx);
    if let Some((physical, scalar_subqueries)) = crate::driver::multi_dml_explain_plan(&crate::driver::planner_bridge::driver_hooks(catalog), crate::driver::MultiDmlRef::Update(update),
        catalog,
        current_db,
        ctx,
    )? {
        return render_built_plan(ctx, &physical, catalog, format, &scalar_subqueries);
    }
    let source = crate::driver::update_source_query(update).ok_or_else(|| {
        DriverError::unsupported("UPDATE has no single-table source")
    })?;
    let fk_spec = crate::driver::build_tbl_2_update_columns(update, current_db)?;
    let (physical, scalar_subqueries) = crate::driver::physical_dml_plan_and_scalar_subqueries(&crate::driver::planner_bridge::driver_hooks(catalog), "Update",
        Some(&source),
        crate::driver::MultiDmlRef::Update(update),
        catalog,
        current_db,
        ctx,
        &fk_spec,
    )?;
    render_built_plan(ctx, &physical, catalog, format, &scalar_subqueries)
}

/// Plans a `DELETE` and reports the plan as EXPLAIN rows, executing nothing.
/// See [`explain_update_stmt`]: `Delete_N`'s child is the same read plan.
pub fn explain_delete_stmt(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<SelectMeta, DriverError> {
    // Go `PlanBuilder.Build` for the EXPLAIN itself (`buildExplain`).
    crate::driver::record_plan_builder_metric(ctx);
    if let Some((physical, scalar_subqueries)) = crate::driver::multi_dml_explain_plan(&crate::driver::planner_bridge::driver_hooks(catalog), crate::driver::MultiDmlRef::Delete(delete),
        catalog,
        current_db,
        ctx,
    )? {
        return render_built_plan(ctx, &physical, catalog, format, &scalar_subqueries);
    }
    let source = crate::driver::delete_source_query(delete).ok_or_else(|| {
        DriverError::unsupported("DELETE has no single-table source")
    })?;
    let fk_spec = crate::driver::fk_spec_for_delete(delete, current_db)?;
    let (physical, scalar_subqueries) = crate::driver::physical_dml_plan_and_scalar_subqueries(&crate::driver::planner_bridge::driver_hooks(catalog), "Delete",
        Some(&source),
        crate::driver::MultiDmlRef::Delete(delete),
        catalog,
        current_db,
        ctx,
        &fk_spec,
    )?;
    render_built_plan(ctx, &physical, catalog, format, &scalar_subqueries)
}

/// `EXPLAIN ANALYZE <update>`: unlike [`explain_update_stmt`], this really
/// runs the `UPDATE` (captured: the table's rows change afterward, both for a
/// primary-key `WHERE` and an ordinary-column one). The read child's
/// `actRows` come off that same run: the read node's is the number of records
/// the update's own read examined, and `Selection`'s the number its own
/// `WHERE` passed -- both confirmed by capture. Like `Insert_1`, `Update_N`'s
/// own `actRows` is always `0` (captured): the write is a side effect, not a
/// row this operator's `Next()` produces.
pub fn explain_analyze_update_stmt(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<AnalyzedWrite, DriverError> {
    let analyzed = explain_analyze_context(ctx);
    let ctx = &analyzed;
    // Go `PlanBuilder.Build` for the EXPLAIN itself (`buildExplain`).
    crate::driver::record_plan_builder_metric(ctx);
    let (mut physical, scalar_subqueries) = match crate::driver::multi_dml_explain_plan(&crate::driver::planner_bridge::driver_hooks(catalog), crate::driver::MultiDmlRef::Update(update),
        catalog,
        current_db,
        ctx,
    )? {
        Some(planned) => planned,
        None => {
            let source = crate::driver::update_source_query(update).ok_or_else(|| {
                DriverError::unsupported("UPDATE has no single-table source")
            })?;
            crate::driver::physical_dml_plan_and_scalar_subqueries(&crate::driver::planner_bridge::driver_hooks(catalog), "Update",
                Some(&source),
                crate::driver::MultiDmlRef::Update(update),
                catalog,
                current_db,
                ctx,
                &crate::driver::build_tbl_2_update_columns(update, current_db)?,
            )?
        }
    };
    // The writes add their affected rows to the statement (Go `AddAffectedRows`).
    run_update_stmt_with_physical(
        update,
        catalog,
        current_db,
        ctx,
        Some(&mut physical),
    )?;
    // The write ran and closed (Go `handleNoDelayExecutor`).
    Ok(AnalyzedWrite {
        ctx: analyzed.clone(),
        physical,
        catalog: catalog.clone(),
        format,
        scalar_subqueries,
    })
}

/// `EXPLAIN ANALYZE <delete>`: see [`explain_analyze_update_stmt`] -- the
/// same real read-then-write shape, over the delete driver.
pub fn explain_analyze_delete_stmt(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    format: ExplainFormat,
) -> Result<AnalyzedWrite, DriverError> {
    let analyzed = explain_analyze_context(ctx);
    let ctx = &analyzed;
    // Go `PlanBuilder.Build` for the EXPLAIN itself (`buildExplain`).
    crate::driver::record_plan_builder_metric(ctx);
    let (mut physical, scalar_subqueries) = match crate::driver::multi_dml_explain_plan(&crate::driver::planner_bridge::driver_hooks(catalog), crate::driver::MultiDmlRef::Delete(delete),
        catalog,
        current_db,
        ctx,
    )? {
        Some(planned) => planned,
        None => {
            let source = crate::driver::delete_source_query(delete).ok_or_else(|| {
                DriverError::unsupported("DELETE has no single-table source")
            })?;
            crate::driver::physical_dml_plan_and_scalar_subqueries(&crate::driver::planner_bridge::driver_hooks(catalog), "Delete",
                Some(&source),
                crate::driver::MultiDmlRef::Delete(delete),
                catalog,
                current_db,
                ctx,
                &crate::driver::fk_spec_for_delete(delete, current_db)?,
            )?
        }
    };
    // The writes add their affected rows to the statement (Go `AddAffectedRows`).
    run_delete_stmt_with_physical(
        delete,
        catalog,
        current_db,
        ctx,
        Some(&mut physical),
    )?;
    // The write ran and closed (Go `handleNoDelayExecutor`).
    Ok(AnalyzedWrite {
        ctx: analyzed.clone(),
        physical,
        catalog: catalog.clone(),
        format,
        scalar_subqueries,
    })
}

fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}

/// Go `genBriefPlanWithSCtx` (`pkg/planner/optimize.go`): the optimized
/// plan's digest (`NormalizeFlatPlan`), the hints that reproduce it
/// (`GenHintsFromFlatPlan`, restored) and its `EXPLAIN FORMAT='brief'` rows.
pub fn gen_brief_plan(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(String, String, Vec<Vec<String>>), DriverError> {
    tidb_planner::optimize::gen_brief_plan(&crate::driver::planner_bridge::driver_hooks(catalog), query, catalog, current_db, ctx)
}

/// Go `calculatePlanDigestFunc` (`pkg/planner/optimize.go`): the digest of
/// the plan the optimizer picks for `query`.
pub fn calculate_plan_digest(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<String, DriverError> {
    tidb_planner::optimize::calculate_plan_digest(&crate::driver::planner_bridge::driver_hooks(catalog), query, catalog, current_db, ctx)
}

/// Go `recordRelevantOptVarsAndFixes` (`pkg/planner/optimize.go`): the
/// optimizer variables and fixes `query`'s optimization consults, sorted.
pub fn record_relevant_opt_vars_and_fixes(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(Vec<String>, Vec<u64>), DriverError> {
    tidb_planner::optimize::record_relevant_opt_vars_and_fixes(&crate::driver::planner_bridge::driver_hooks(catalog), query, catalog, current_db, ctx)
}
