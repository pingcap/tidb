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

//! The live executor seam to the ported Go logical planner.
//!
//! The executor still owns physical operator construction, but it must not
//! independently rediscover logical properties from the SQL AST. This module
//! builds and optimizes one logical tree, runs Go's possible-property pass,
//! and translates the aggregation property back to stable relation-qualified
//! column identities understood by the executor's physical source builder.

pub(crate) use tidb_planner::dml_optimize::update_target_columns;
pub(crate) use tidb_planner::plan_cache::{CachedSelectPlan, cached_query_plan};
pub(crate) use tidb_planner::plan_cache_rebuild::{
    CachedPlanBindFailure, deferred_rebuild_context, rebuild_evaluate,
};
pub(crate) use tidb_planner::statement_plan::*;
use tidb_planner::stmt_ctx::PlannerStmtCtx as _;

use super::PlannerCatalogContext;
use tidb_expr::expr_util::RealFunctionBuilder;
use tidb_expr::expression::Expression;
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_planner::plan_builder::PlanBuilder;
use tidb_planner_core_base::ColumnIdAllocator;
use tidb_planner_core_base::PlanIdAllocator;
use tidb_planner_core_operator_physicalop::PhysicalPlan;

use tidb_infoschema::SessionExtendedInfoSchema;

/// Go `partitionpruning.PartitionPruning` over the table `table_id` names
/// in the statement's catalog.
pub(crate) fn partition_pruning(
    catalog: &SessionExtendedInfoSchema,
    table_id: i64,
    conds: &[Expression],
    partition_names: &[String],
    table_columns: &[tidb_expr::column::Column],
    context: &crate::StmtContext,
) -> Result<Vec<usize>, tidb_planner_core_base::PlanError> {
    let table = catalog.kv_table_by_id(table_id).ok_or_else(|| {
        tidb_planner_core_base::PlanError::internal(
            "partitioned logical table is absent from the catalog",
        )
    })?;
    let partition = table.partition().ok_or_else(|| {
        tidb_planner_core_base::PlanError::internal(
            "partition processor received an unpartitioned table",
        )
    })?;
    let pi = table.meta().read().get_partition_info().ok_or_else(|| {
        tidb_planner_core_base::PlanError::internal(
            "partition processor received an unpartitioned table",
        )
    })?;
    let pi = pi.read();
    let builder = RealFunctionBuilder::new(context);
    tidb_planner_core_rule::rule_partition_processor::prune::partition_pruning(
        partition,
        &pi,
        conds,
        partition_names,
        table_columns,
        &tidb_planner_core_rule::rule_partition_processor::prune::PruneContext {
            builder: &builder,
            eval: context,
            range_max_size: context.range_max_size(),
            range_fallback_handler: Some(context.range_fallback_handler()),
            ranger_options: context.ranger_options(),
        },
    )
}

pub(super) use tidb_planner::point_get_plan::is_cte;

/// Go `PlanBuilder.buildAdminCheckTable` over the statement's catalog.
pub(crate) fn admin_check_table_plan(
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    db_name: &str,
    table_name: &str,
    index: Option<&str>,
) -> Result<tidb_planner::plan_builder::admin_check::CheckTable, super::DriverError> {
    let source = catalog
        .planner_catalog(current_db, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .build_admin_check_table(db_name, table_name, index)
        .map_err(super::planner_error_to_driver)
}

/// Detaches parameter/deferred markers from an executor-owned expression
/// after its current value has been materialized. The cache-owned physical
/// tree keeps its markers and is rebuilt before this executor copy is made.
pub(super) fn materialize_physical_expression(expression: &mut Expression) {
    match expression {
        Expression::Column(_) | Expression::CorrelatedColumn(_) | Expression::ScalarSubQuery(_) => {
        }
        Expression::Constant(constant) => {
            constant.param_marker = None;
            constant.deferred_expr = None;
        }
        Expression::ScalarFunction(function) => {
            for argument in &mut function.args {
                materialize_physical_expression(argument);
            }
        }
    }
}

pub(crate) use tidb_planner::optimize::{BuildVisits, ScalarSubqueryEvalCtx};

/// Go `optimizeForTest`'s build and optimize: the logical plan (whose
/// output names Go's tests read) and the physical plan.
#[cfg(test)]
pub(crate) fn planner_optimized_query_with_allocators_for_test(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<
    (
        tidb_planner_core_operator_logicalop::LogicalPlan,
        PhysicalPlan,
    ),
    tidb_planner_core_base::PlanError,
> {
    planner_optimized_query_with_allocators(
        &crate::driver::planner_bridge::driver_hooks(catalog),
        query,
        catalog,
        current_database,
        ctx,
        false,
        plan_ids,
        column_ids,
        &tidb_planner::optimize::ScalarSubqueryRegistry::default(),
        BuildVisits::Discard,
        tidb_planner::optimize::QueryBuild::Statement,
    )
}

/// Go `evalFieldDefaultValue` (`expression_rewriter.go:3303`) for the
/// planner's [`tidb_planner::plan_builder::DefaultEvaluator`]: the public
/// column's `table.GetColDefaultValue` under the statement's context -- a
/// CURRENT_TIMESTAMP default reads the statement clock, a column with no
/// default is NULL or 1364 -- as a constant of the column's type.
fn default_evaluator<'a>(
    catalog: &'a SessionExtendedInfoSchema,
    ctx: &'a crate::StmtContext,
) -> impl Fn(&str, &str, &str) -> Result<Expression, tidb_exprctx::EvalError> + 'a {
    default_evaluator_using(catalog, ctx, super::dml::DefaultUse::Expression)
}

/// [`default_evaluator`] for a given use of the default: an INSERT's own
/// `DEFAULT` (Go `getDefaultValueForInsert`) answers differently from
/// `DEFAULT(col)` inside an expression.
fn default_evaluator_using<'a>(
    catalog: &'a SessionExtendedInfoSchema,
    ctx: &'a crate::StmtContext,
    default_use: super::dml::DefaultUse,
) -> impl Fn(&str, &str, &str) -> Result<Expression, tidb_exprctx::EvalError> + 'a {
    move |database, table, column| {
        let entry = catalog.table_in(database, table).ok_or_else(|| {
            // Go `is.TableByName` failing.
            tidb_exprctx::EvalError::Conversion(tidb_terror::TerrorError::compatible(
                tidb_terror::TerrorCode::new(1146),
                format!("Table '{database}.{table}' doesn't exist"),
            ))
        })?;
        let meta = super::dml::column_metadata(entry)
            .into_iter()
            .find(|meta| meta.name.go_lower_eq(column))
            .ok_or_else(|| {
                // Go `ErrUnknownColumn.GenWithStackByArgs(colName, "field_list")`.
                tidb_exprctx::EvalError::UnknownColumnInClause(column.go_to_lower(), "field_list")
            })?;
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty_chunk(&[]);
        chunk.set_num_virtual_rows(1);
        let value =
            super::dml::materialize_column_default(&meta, default_use, ctx, chunk.get_row(0))
                .map_err(|error| match error {
                    crate::DriverError::Exec(crate::ExecError::Eval(error)) => error,
                    // Go `table.ErrNoDefaultValue.GenWithStackByArgs(colName)`.
                    error if error.equal(&tidb_table::table_errors::ERR_NO_DEFAULT_VALUE) => {
                        tidb_exprctx::EvalError::Conversion(
                            tidb_table::table_errors::ERR_NO_DEFAULT_VALUE
                                .generate(error.to_string()),
                        )
                    }
                    // Any other refusal (a default whose own conversion truncates, for
                    // instance) keeps the code the client would have seen.
                    error => {
                        let error = error.to_mysql_error();
                        tidb_exprctx::EvalError::Conversion(tidb_terror::TerrorError::registered(
                            tidb_terror::TerrorClass::Types,
                            tidb_terror::TerrorCode::new(error.code as isize),
                            error.message,
                        ))
                    }
                })?;
        Ok(Expression::Constant(tidb_expr::constant::Constant::new(
            value,
            meta.field_type,
        )))
    }
}

/// The executor's half of one statement's planning
/// ([`tidb_planner::optimize::PlanningHooks`]): Go's function
/// variable `EvalSubqueryFirstRow` and the column default evaluators.
pub(crate) struct DriverHooks<'a> {
    catalog: &'a SessionExtendedInfoSchema,
}

/// The executor's [`tidb_planner::optimize::PlanningHooks`] over
/// `catalog`.
pub(crate) fn driver_hooks(catalog: &SessionExtendedInfoSchema) -> DriverHooks<'_> {
    DriverHooks { catalog }
}

impl tidb_planner::optimize::PlanningHooks for DriverHooks<'_> {
    fn eval_subquery_first_row(&self) -> tidb_planner::optimize::EvalSubqueryFirstRow {
        crate::driver::physical_builder::eval_subquery_first_row
    }

    fn prepare_plan(
        &self,
        plan: &mut PhysicalPlan,
        ctx: &crate::StmtContext,
    ) -> Result<(), crate::DriverError> {
        crate::driver::physical_builder::prepare_plan_for_explain(plan, self.catalog, ctx)
    }

    fn field_default(
        &self,
        ctx: &crate::StmtContext,
        database: &str,
        table: &str,
        column: &str,
    ) -> Result<Expression, tidb_exprctx::EvalError> {
        default_evaluator(self.catalog, ctx)(database, table, column)
    }

    fn insert_default(
        &self,
        ctx: &crate::StmtContext,
        database: &str,
        table: &str,
        column: &str,
    ) -> Result<Expression, tidb_exprctx::EvalError> {
        default_evaluator_using(self.catalog, ctx, super::dml::DefaultUse::Insert)(
            database, table, column,
        )
    }
}

#[cfg(test)]
pub(crate) fn statistics_usage_before_and_after_logical_optimization(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    (
        tidb_stats::table_statistics::ColumnStatsUsage,
        tidb_stats::table_statistics::ColumnStatsUsage,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let session_zone = ctx.session_zone();
    let prune_fallback = |table_id: i64, table_name: &str| {
        tidb_planner::optimize::dynamic_prune_fallback(catalog, ctx, table_id, table_name)
    };
    let mut builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, session_zone.clone());
    builder.enable_pipelined_window_exec = ctx.enable_pipelined_window_exec();
    builder.enable_full_outer_join = ctx.enable_full_outer_join();
    builder.enable_cascades_planner = ctx.enable_cascades_planner();
    builder.new_only_full_group_by_check = ctx.new_only_full_group_by_check();
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.enable_unsafe_substitute = ctx.enable_unsafe_substitute();
    builder.enable_force_inline_cte = ctx.enable_force_inline_cte();
    builder.enable_mpp_shared_cte_execution = ctx.enable_mpp_shared_cte_execution();
    builder.skip_cte_consumer_counts = ctx.skip_cte_consumer_counts();
    builder.oracle_mode = ctx.ddl_sql_mode() & tidb_mysql::consts::ModeOracle.0 != 0;
    builder.remove_orderby_in_subquery = ctx.remove_orderby_in_subquery();
    builder.set_isolation_read_engines(ctx.isolation_read_engines());
    builder.set_tiflash_engine_removed_due_to_strict_sql_mode(
        ctx.tiflash_engine_removed_due_to_strict_sql_mode(),
    );
    builder.advanced_join_hint = ctx.enable_advanced_join_hint();
    builder.in_explain_stmt = ctx.in_explain_stmt();
    builder.in_explain_analyze_stmt = ctx.in_explain_analyze_stmt();
    builder.explain_non_evaled_sub_query = ctx.explain_non_evaled_sub_query();
    builder.parser_config = ctx.parser_config();
    builder.set_partition_processor_enabled(ctx.static_partition_prune());
    builder.set_dynamic_prune_fallback(&prune_fallback);
    builder.flags.allow_in_subq_to_join_and_agg = ctx.allow_in_subq_to_join_and_agg();
    builder.flags.enable_semi_join_rewrite = ctx.enable_semi_join_rewrite();
    builder.flags.enable_correlate_subquery = ctx.enable_correlate_subquery();
    builder.flags.enable_no_decorrelate_in_select = ctx.enable_no_decorrelate_in_select();
    builder.enable_skew_distinct_agg = ctx.enable_skew_distinct_agg();
    builder.index_lookup_push_down_session = ctx.index_lookup_push_down_session();
    let node = tidb_resolve::NodeW::new(query.clone());
    let plan = builder.build_query_node(&node, false)?;
    let flags = builder.get_opt_flag();
    let (plan, before) =
        tidb_planner_core_rule::rule_collect_plan_stats::collect_column_stats_usage(
            plan,
            ctx.opt_index_prune_threshold(),
        );
    let optimized = tidb_planner::optimizer::optimize_built_logical(
        plan,
        flags,
        catalog,
        ctx,
        false,
        &plan_ids,
        &column_ids,
        &session_zone,
    )?;
    let (_, after) = tidb_planner_core_rule::rule_collect_plan_stats::collect_column_stats_usage(
        optimized,
        ctx.opt_index_prune_threshold(),
    );
    Ok((before, after))
}
