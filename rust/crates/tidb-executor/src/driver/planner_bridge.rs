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

use std::cell::RefCell;
use std::rc::Rc;
pub(crate) use tidb_planner::dml_optimize::update_target_columns;
use tidb_planner::physical_plan_cache::RebuildPlanForCache;
pub(crate) use tidb_planner::statement_plan::*;
use tidb_planner::stmt_ctx::PlannerStmtCtx as _;

use super::PlannerCatalogContext;
use tidb_expr::expr_util::RealFunctionBuilder;
use tidb_expr::expression::Expression;
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_infoschema::table_source::TableSource;
pub(crate) use tidb_planner::do_optimize::{optimize_built_logical, physical_plan_for_logical};
use tidb_planner::plan_builder::PlanBuilder;
use tidb_planner_core_base::ColumnIdAllocator;
use tidb_planner_core_base::PlanIdAllocator;
use tidb_planner_core_operator_logicalop::LogicalPlan;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_planner_core_rule::flags;

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

pub(crate) use tidb_planner::statement_optimize::{
    BuildVisits, QueryBuild, RegisteredScalarSubquery, ScalarSubqueryRegistry,
};
use tidb_planner::statement_optimize::{DiscardedSubquery, subquery_output_width};

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
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
    planner_optimized_query_with_allocators(
        &crate::driver::planner_bridge::driver_hooks(catalog),
        query,
        catalog,
        current_database,
        ctx,
        false,
        plan_ids,
        column_ids,
        &ScalarSubqueryRegistry::default(),
        BuildVisits::Discard,
        QueryBuild::Statement,
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
/// ([`tidb_planner::statement_optimize::PlanningHooks`]): Go's function
/// variable `EvalSubqueryFirstRow` and the column default evaluators.
pub(crate) struct DriverHooks<'a> {
    catalog: &'a SessionExtendedInfoSchema,
}

/// The executor's [`tidb_planner::statement_optimize::PlanningHooks`] over
/// `catalog`.
pub(crate) fn driver_hooks(catalog: &SessionExtendedInfoSchema) -> DriverHooks<'_> {
    DriverHooks { catalog }
}

impl tidb_planner::statement_optimize::PlanningHooks for DriverHooks<'_> {
    fn eval_subquery_first_row(&self) -> tidb_planner::statement_optimize::EvalSubqueryFirstRow {
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

/// The complete physical tree retained by the prepared-plan cache. A hit
/// recursively rebuilds every parameter-dependent range in place and derives
/// the executor receipt from that rebuilt tree. No access,
/// aggregation, join, sort, or reader policy is re-run in the executor.
#[derive(Debug)]
pub(crate) struct CachedSelectPlan {
    statement: tidb_ast::Stmt,
    physical: PhysicalPlan,
    generation: u64,
    /// Go `PlanCacheStmt.NormalizedPlan` / `PlanDigest`, shared by every
    /// execution of this entry.
    digest: std::sync::Arc<std::sync::OnceLock<(String, String)>>,
}

impl CachedSelectPlan {
    pub(super) fn clone_for_execution(&self) -> Self {
        Self {
            statement: self.statement.clone(),
            physical: self.physical.deep_clone(),
            generation: self.generation,
            digest: std::sync::Arc::clone(&self.digest),
        }
    }

    pub(super) fn memory_usage(&self) -> i64 {
        tidb_planner::physical_plan_cache::cached_plan_memory_usage(&self.physical) as i64
    }

    pub(crate) fn bind(
        &mut self,
        values: &[tidb_datatype::Datum],
        ctx: Option<&crate::StmtContext>,
    ) -> Result<u64, CachedPlanBindFailure> {
        super::bind_prepared_statement_in_place(&mut self.statement, values)
            .map_err(|_| CachedPlanBindFailure::unbound())?;
        // Rebuild through the current execute parameters, never the datum
        // cached when the marker-bearing expression was first planned.
        let needs_statement = std::sync::atomic::AtomicBool::new(false);
        let statement = deferred_rebuild_context(ctx, values);
        let parameters = tidb_planner::physical_plan_cache::CachedPlanRebuildContext::new(values);
        let evaluator = |expression: &tidb_expr::expression::Expression| {
            rebuild_evaluate(
                expression,
                statement.as_ref(),
                &parameters,
                &needs_statement,
            )
        };
        let rebuilt = self.physical.rebuild_plan_for_cache_in_place(
            &tidb_planner::physical_plan_cache::CachedPlanRebuildContext::new(values)
                .with_deferred_evaluator(&evaluator),
        );
        if needs_statement.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(CachedPlanBindFailure::NeedsStatement);
        }
        rebuilt.map_err(CachedPlanBindFailure::rebuild_failed)?;
        self.generation = self.generation.wrapping_add(1);
        Ok(self.generation)
    }

    pub(crate) fn digest(&self) -> std::sync::Arc<std::sync::OnceLock<(String, String)>> {
        std::sync::Arc::clone(&self.digest)
    }

    pub(crate) fn execution_mut(
        &mut self,
        generation: u64,
    ) -> Option<(&tidb_ast::Stmt, &mut PhysicalPlan)> {
        (self.generation == generation).then_some((&self.statement, &mut self.physical))
    }
}

/// Why a cached plan could not be rebuilt for an execute.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CachedPlanBindFailure {
    /// The rebuild reached a deferred expression (`NOW()`) and was given no
    /// statement to evaluate it in: the entry stays, and a bind with the
    /// executing statement rebuilds it.
    NeedsStatement,
    /// Go rejects an entry whose in-place rebuild fails.
    Rejected {
        /// Go `RebuildPlan4CachedPlan`'s `rebuildRange` error, which it
        /// reports as "skip plan-cache: plan rebuild failed, %s".
        rebuild_error: Option<String>,
        /// The reason Go's ranger passed to `SetSkipPlanCache` while
        /// rebuilding: the hit is refused and the replanned statement is not
        /// cached either.
        skip_plan_cache: Option<String>,
    },
}

impl CachedPlanBindFailure {
    pub(crate) fn rebuild_failed(
        error: tidb_planner::physical_plan_cache::PlanCacheRebuildError,
    ) -> Self {
        match error {
            tidb_planner::physical_plan_cache::PlanCacheRebuildError::SkipPlanCache(reason) => {
                Self::Rejected {
                    rebuild_error: None,
                    skip_plan_cache: Some(reason),
                }
            }
            tidb_planner::physical_plan_cache::PlanCacheRebuildError::SkippedThenFailed {
                reason,
                error,
            } => Self::Rejected {
                rebuild_error: Some(error.to_string()),
                skip_plan_cache: Some(reason),
            },
            error => Self::Rejected {
                rebuild_error: Some(error.to_string()),
                skip_plan_cache: None,
            },
        }
    }

    /// The statement-binding failure, which carries no rebuild reason.
    pub(crate) const fn unbound() -> Self {
        Self::Rejected {
            rebuild_error: None,
            skip_plan_cache: None,
        }
    }

    /// Go `RebuildPlan4CachedPlan` warns and `generateNewPlan` replans. The
    /// entry stays: a cacheable new plan replaces it under the same key, an
    /// uncacheable one leaves it for the next execute. A context-free probe
    /// answers `false`, so the bind that carries the statement replans.
    pub(crate) fn replan(&self, ctx: Option<&crate::StmtContext>) -> bool {
        let (
            Self::Rejected {
                rebuild_error,
                skip_plan_cache,
            },
            Some(ctx),
        ) = (self, ctx)
        else {
            return false;
        };
        if let Some(reason) = skip_plan_cache {
            ctx.start_prepared_range_tracking();
            ctx.set_skip_plan_cache(reason.as_str());
        }
        if let Some(error) = rebuild_error {
            ctx.append_warning_parts(
                1105,
                &format!("skip plan-cache: plan rebuild failed, {error}"),
            );
        }
        true
    }
}

/// One evaluation during a cached plan's rebuild. A bound parameter or a
/// literal is its own value, so a context-free probe rebuilds the same
/// ranges the statement would. Anything else -- a deferred expression
/// (`NOW()`) or a function of a parameter (`FROM_UNIXTIME(?)`, which reads
/// the session time zone) -- is evaluated in the executing statement, and
/// the probe reports `NeedsStatement` instead of building from a value
/// computed without it.
pub(super) fn rebuild_evaluate(
    expression: &tidb_expr::expression::Expression,
    statement: Option<&crate::StmtContext>,
    parameters: &tidb_planner::physical_plan_cache::CachedPlanRebuildContext<'_>,
    needs_statement: &std::sync::atomic::AtomicBool,
) -> Result<tidb_datatype::Datum, tidb_exprctx::EvalError> {
    if let Some(statement) = statement {
        return tidb_expr::eval_expression_once(expression, statement);
    }
    match expression {
        tidb_expr::expression::Expression::Constant(constant)
            if constant.deferred_expr.is_none() =>
        {
            tidb_expr::eval_expression_once(expression, parameters)
        }
        _ => {
            needs_statement.store(true, std::sync::atomic::Ordering::Relaxed);
            Err(tidb_exprctx::EvalError::Unsupported(
                "the expression needs the executing statement",
            ))
        }
    }
}

/// The context a cached plan's rebuild evaluates a deferred expression in:
/// Go re-evaluates `NOW()` and its kin under the executing statement's own
/// session context (clock, time zone, variables) with this execute's
/// parameters. Without a statement there is no clock to read.
pub(super) fn deferred_rebuild_context(
    ctx: Option<&crate::StmtContext>,
    values: &[tidb_datatype::Datum],
) -> Option<crate::StmtContext> {
    ctx.map(|ctx| {
        ctx.clone()
            .with_prepared_params(std::sync::Arc::from(values))
    })
}

/// Builds the same logical and physical plan as ordinary execution, with
/// plan-cache-safe logical rewrites enabled so parameter markers are not
/// folded into a value-specific shape.
pub(crate) fn cached_query_plan(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    cacheability: tidb_planner::physical_plan_cache::PlanCacheabilityContext,
) -> Result<Option<(CachedSelectPlan, bool)>, crate::DriverError> {
    let Some((physical, cacheable)) =
        cached_physical_query_plan(query, catalog, current_database, ctx, cacheability)?
    else {
        return Ok(None);
    };
    Ok(Some((
        CachedSelectPlan {
            statement: tidb_ast::Stmt::Query(tidb_ast::NodeBox::new(query.clone())),
            physical,
            generation: 0,
            digest: std::sync::Arc::default(),
        },
        cacheable,
    )))
}

/// Builds and admits the physical source tree shared by cached queries and
/// cached DML roots.
///
/// # Errors
/// The optimizer's error: Go `generateNewPlan` returns it as the statement's.
pub(crate) fn cached_physical_query_plan(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    cacheability: tidb_planner::physical_plan_cache::PlanCacheabilityContext,
) -> Result<Option<(PhysicalPlan, bool)>, crate::DriverError> {
    if matches!(query, tidb_ast::QueryStmt::Select(select) if select.rollup) {
        return Ok(None);
    }
    ctx.start_prepared_range_tracking();
    // Go `optimize` tries `TryFastPlan` before the optimizer, for a cached
    // statement too; the point plan keeps its parameter markers
    // (`HandleConstant` / `IndexConstants`) so a hit rebuilds its keys.
    let fast = match query {
        tidb_ast::QueryStmt::Select(select) if ctx.isolation_read_contains_tikv() => {
            super::point_get_plan::try_fast_plan(select, catalog, current_database, ctx)?
        }
        tidb_ast::QueryStmt::Select(_) | tidb_ast::QueryStmt::SetOpr(_) => None,
    };
    let physical = match fast {
        Some(physical) => physical,
        None => {
            planner_physical_query(
                &crate::driver::planner_bridge::driver_hooks(catalog),
                query,
                catalog,
                current_database,
                ctx,
                true,
            )
            .map_err(super::planner_error_to_driver)?
            .1
        }
    };
    if ctx.skip_plan_cache() {
        return Ok(Some((physical, false)));
    }
    // Go `generateNewPlan`: an uncacheable physical plan still executes, and
    // `SetSkipPlanCache` says why.
    if let Err(reason) =
        tidb_planner::physical_plan_cache::is_plan_cacheable(&physical, cacheability)
    {
        ctx.set_skip_plan_cache(reason);
        return Ok(Some((physical, false)));
    }
    Ok(Some((physical, true)))
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
        tidb_planner::statement_optimize::dynamic_prune_fallback(catalog, ctx, table_id, table_name)
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
    let optimized = optimize_built_logical(
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
