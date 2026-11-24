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
use tidb_planner::physical_plan_cache::RebuildPlanForCache;
use tidb_planner::stmt_ctx::PlannerStmtCtx as _;

use super::PlannerCatalogContext;
use tidb_expr::expr_util::RealFunctionBuilder;
use tidb_expr::expression::Expression;
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_infoschema::table_source::TableSource;
pub(crate) use tidb_planner::do_optimize::{
    optimize_built_logical, physical_plan_for_logical, physical_plan_for_logical_with_update_refs,
};
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

pub(super) fn update_target_columns(
    update: &tidb_ast::UpdateStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<(String, String, String)>, super::DriverError> {
    let source = catalog
        .planner_catalog(current_db, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let targets = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .resolve_update_targets(update)
        .map_err(super::planner_error_to_driver)?;
    Ok(targets
        .into_iter()
        .map(|target| (target.database, target.table, target.column))
        .collect())
}

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

fn planner_physical_select(
    select: &tidb_ast::SelectStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
    let query = tidb_ast::QueryStmt::Select(Box::new(select.clone()));
    planner_optimized_query_with_allocators(
        &query,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        &PlanIdAllocator::new(),
        &ColumnIdAllocator::new(),
        &ScalarSubqueryRegistry::default(),
        BuildVisits::Discard,
        QueryBuild::Statement,
    )
}

pub(crate) use tidb_planner::statement_optimize::{
    BuildVisits, QueryBuild, RegisteredScalarSubquery, ScalarSubqueryRegistry,
};
use tidb_planner::statement_optimize::{DiscardedSubquery, subquery_output_width};

fn planner_physical_query(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    planner_physical_query_with_allocators(
        query,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        &plan_ids,
        &column_ids,
        BuildVisits::Discard,
        QueryBuild::Statement,
    )
}

#[allow(clippy::too_many_arguments)]
fn planner_physical_query_with_allocators(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    visits: BuildVisits<'_>,
    build: QueryBuild,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
    planner_physical_query_with_registry(
        query,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        visits,
        build,
    )
    .map(|(logical, physical, _)| (logical, physical))
}

/// [`planner_physical_query_with_allocators`], additionally handing back the
/// uncorrelated subqueries the build evaluated so EXPLAIN can append their
/// registered roots.
#[allow(clippy::too_many_arguments)]
fn planner_physical_query_with_registry(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    visits: BuildVisits<'_>,
    build: QueryBuild,
) -> Result<
    (
        LogicalPlan,
        PhysicalPlan,
        Vec<std::sync::Arc<RegisteredScalarSubquery>>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let registry = ScalarSubqueryRegistry::default();
    let (logical, physical) = planner_optimized_query_with_allocators(
        query,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        &registry,
        visits,
        build,
    )?;
    let registered = Rc::try_unwrap(registry)
        .map(RefCell::into_inner)
        .unwrap_or_default();
    Ok((logical, physical, registered))
}

/// Builds the ordinary physical SELECT tree consumed by the common executor
/// builder. Go passes both fresh and cache-rebuilt plans to
/// `executorBuilder.build`; keeping the full tree here prevents ordinary
/// execution from rediscovering its operators from the SQL AST.
pub(crate) fn physical_select_plan(
    select: &tidb_ast::SelectStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    planner_physical_select(select, catalog, current_database, ctx, false)
        .map(|(_, physical)| physical)
}

/// [`physical_select_plan`] drawing plan and column ids from the given
/// allocators, as Go's session-wide `PlanID` / `PlanColumnID` counters do
/// for several selects optimized under one statement.
pub(crate) fn physical_select_plan_with_allocators(
    select: &tidb_ast::SelectStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    let query = tidb_ast::QueryStmt::Select(Box::new(select.clone()));
    planner_optimized_query_with_allocators(
        &query,
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
    .map(|(_, physical)| physical)
}

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

/// Builds the ordinary physical tree for either Go query-statement shape.
/// Set operations, their CTEs, and plain SELECTs therefore all enter the same
/// logical optimizer, physical search, and executor-builder switch.
pub(crate) fn physical_query_plan(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    planner_physical_query(query, catalog, current_database, ctx, false)
        .map(|(_, physical)| physical)
}

/// [`physical_query_plan`], additionally handing back the uncorrelated
/// subqueries the build evaluated. Go keeps them in `StmtCtx` and EXPLAIN
/// appends each one's optimized child as an extra root.
pub(crate) fn physical_query_plan_with_scalar_subqueries(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    visits: BuildVisits<'_>,
) -> Result<
    (PhysicalPlan, Vec<std::sync::Arc<RegisteredScalarSubquery>>),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    planner_physical_query_with_registry(
        query,
        catalog,
        current_database,
        ctx,
        false,
        &plan_ids,
        &column_ids,
        visits,
        QueryBuild::Statement,
    )
    .map(|(_, physical, registered)| (physical, registered))
}

/// Builds a query below a non-query physical root using that statement's
/// plan and column allocators. Go's DML builders allocate the write root and
/// its `SelectPlan` from the same session counters; keeping those counters
/// shared preserves both tree ownership and explain identities.
#[allow(clippy::too_many_arguments)]
pub(crate) fn physical_query_plan_with_allocators(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    visits: BuildVisits<'_>,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    planner_physical_query_with_allocators(
        query,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        visits,
        QueryBuild::DmlRead,
    )
    .map(|(_, physical)| physical)
}

use tidb_planner::statement_optimize::driver_error_to_plan;

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
struct DriverHooks<'a> {
    catalog: &'a SessionExtendedInfoSchema,
}

impl tidb_planner::statement_optimize::PlanningHooks for DriverHooks<'_> {
    fn eval_subquery_first_row(&self) -> tidb_planner::statement_optimize::EvalSubqueryFirstRow {
        crate::driver::physical_builder::eval_subquery_first_row
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

/// Go `planner.Optimize` for a query statement
/// ([`tidb_planner::statement_optimize::optimize_query_stmt`]), handing back
/// the uncorrelated subqueries the build evaluated: plain EXPLAIN appends
/// each registered child as its own `ScalarSubQuery` root.
pub(crate) fn optimize_query_stmt(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<(PhysicalPlan, Vec<std::sync::Arc<RegisteredScalarSubquery>>), crate::DriverError> {
    let (plan_ids, column_ids) = (PlanIdAllocator::new(), ColumnIdAllocator::new());
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    let physical = parts
        .planning(|parts| tidb_planner::statement_optimize::optimize_query_stmt(parts, query))?;
    let registered = Rc::try_unwrap(registry)
        .map(RefCell::into_inner)
        .unwrap_or_default();
    Ok((physical, registered))
}

/// Go `planner.optimize` over a query
/// ([`tidb_planner::statement_optimize::optimize_query`]) with this
/// statement's hooks.
#[allow(clippy::too_many_arguments)]
fn planner_optimized_query_with_allocators(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    registry: &ScalarSubqueryRegistry,
    visits: BuildVisits<'_>,
    build: QueryBuild,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        registry,
        session_ctx: None,
    };
    parts.planning(|parts| {
        tidb_planner::statement_optimize::optimize_query(parts, query, visits, build)
    })
}

/// Go `planner.Optimize` for a `ShowStmt`: `PlanBuilder.Build`'s
/// `buildShow` over the preprocessed statement, then the logical and
/// physical optimization every plan goes through.
pub(crate) fn physical_show_plan(
    show: &tidb_ast::ShowStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    (
        tidb_planner::common_plans::Plan,
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    let (plan, opt_flag, visits) = parts.build(None, None, |builder| {
        let plan = builder.build_show(show)?;
        Ok((plan, builder.get_opt_flag(), builder.visit_info.clone()))
    })?;
    let tidb_planner::common_plans::Plan::Logical(plan) = plan else {
        return Ok((plan, visits));
    };
    let logical = optimize_built_logical(
        plan,
        opt_flag,
        catalog,
        ctx,
        false,
        &plan_ids,
        &column_ids,
        &ctx.session_zone(),
    )?;
    physical_plan_for_logical(&logical, &plan_ids, &column_ids, catalog, ctx)
        .map(|physical| (tidb_planner::common_plans::Plan::Physical(physical), visits))
}

/// Go `buildSet` for a `SET` statement: the plan and its visit infos.
pub(crate) fn set_plan(
    set: &tidb_ast::SetStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    (
        tidb_planner::common_plans::Set,
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    parts.build(None, None, |builder| {
        let tidb_planner::common_plans::Plan::Set(plan) = builder.build_set(set)? else {
            unreachable!("buildSet answers a Set plan");
        };
        Ok((plan, builder.visit_info.clone()))
    })
}

/// Go `PlanBuilder.Build` for a statement that is not a query or DML: the
/// common plan and its visit infos.
pub(crate) fn statement_plan(
    stmt: &tidb_ast::Stmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    session_ctx: &dyn tidb_planner::plan_builder::stmt_build::SessionPlanContext,
) -> Result<
    (
        tidb_planner::common_plans::Plan,
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: Some(session_ctx),
    };
    let (mut plan, visits, opt_flag) = parts.build(None, None, |builder| {
        let plan = builder.build_stmt(stmt)?;
        Ok((plan, builder.visit_info.clone(), builder.get_opt_flag()))
    })?;
    if let tidb_planner::common_plans::Plan::ImportInto(import) = &mut plan {
        if let Some(select) = import.select_plan.take() {
            let tidb_planner::common_plans::Plan::Logical(logical) = *select else {
                unreachable!("buildImportInto supplies a logical SELECT");
            };
            let logical = optimize_built_logical(
                logical,
                opt_flag,
                catalog,
                ctx,
                false,
                &plan_ids,
                &column_ids,
                &ctx.session_zone(),
            )?;
            let physical =
                physical_plan_for_logical(&logical, &plan_ids, &column_ids, catalog, ctx)?;
            import.select_plan = Some(Box::new(tidb_planner::common_plans::Plan::Physical(
                physical,
            )));
        }
    }
    Ok((plan, visits))
}

/// Go `Compiler.Compile` of the SELECT of an import query
/// (`buildImportQueryExecutor`): the query built as `buildImportInto` builds
/// its source, then optimized into a physical plan.
pub(crate) fn import_query_plan(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    session_ctx: &dyn tidb_planner::plan_builder::stmt_build::SessionPlanContext,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, tidb_planner_core_base::PlanError>
{
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: Some(session_ctx),
    };
    let (logical, opt_flag) = parts.build(None, None, |builder| {
        let logical = builder.build_query_stmt(query, false)?;
        Ok((logical, builder.get_opt_flag()))
    })?;
    let logical = optimize_built_logical(
        logical,
        opt_flag,
        catalog,
        ctx,
        false,
        &plan_ids,
        &column_ids,
        &ctx.session_zone(),
    )?;
    physical_plan_for_logical(&logical, &plan_ids, &column_ids, catalog, ctx)
}

/// Go `planner.Optimize` for `ADMIN SHOW DDL JOBS [n] [WHERE ...]`:
/// `buildAdmin`'s `LogicalShowDDLJobs` under its selection, optimized.
pub(crate) fn physical_admin_show_ddl_jobs_plan(
    job_number: i64,
    where_clause: Option<&tidb_ast::Expr>,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    let (plan, opt_flag) = parts.build(None, None, |builder| {
        let plan = builder.build_admin_show_ddl_jobs(job_number, where_clause)?;
        Ok((plan, builder.get_opt_flag()))
    })?;
    let logical = optimize_built_logical(
        plan,
        opt_flag,
        catalog,
        ctx,
        false,
        &plan_ids,
        &column_ids,
        &ctx.session_zone(),
    )?;
    physical_plan_for_logical(&logical, &plan_ids, &column_ids, catalog, ctx)
}

/// Go `buildDDL`'s `*ast.CreateViewStmt` arm over this catalog: the body is
/// built, never run; `create.query` is left holding the expanded body
/// `ddl.BuildViewInfo` restores and `create.columns` the view's column names.
/// Returns the types of the body's output columns.
pub(crate) fn build_create_view(
    create: &mut tidb_ast::CreateViewStmt,
    view_schema: &str,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<tidb_datatype::FieldType>, tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    let body = (*create.query).clone();
    parts.build(Some(&body), None, |builder| {
        let plan = builder.build_create_view(view_schema, create)?;
        Ok(logical_column_types(&plan))
    })
}

/// What [`QueryBuilderParts::build`] configures a query's `PlanBuilder` from.
struct QueryBuilderParts<'a> {
    catalog: &'a SessionExtendedInfoSchema,
    current_database: &'a str,
    ctx: &'a crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &'a PlanIdAllocator,
    column_ids: &'a ColumnIdAllocator,
    registry: &'a ScalarSubqueryRegistry,
    /// Go `PlanContext`'s session-side reach for the statement builders.
    session_ctx: Option<&'a dyn tidb_planner::plan_builder::stmt_build::SessionPlanContext>,
}

impl QueryBuilderParts<'_> {
    /// The planner's [`tidb_planner::statement_optimize::BuilderParts`] over
    /// these parts, with this statement's [`DriverHooks`].
    fn planning<R>(
        &self,
        plan: impl FnOnce(&tidb_planner::statement_optimize::BuilderParts<'_>) -> R,
    ) -> R {
        self.planning_with(None, plan)
    }

    /// [`Self::planning`] for an UPDATE/DELETE whose root allocation
    /// `on_delete_root` observes.
    fn planning_with<R>(
        &self,
        on_delete_root: Option<&dyn Fn()>,
        plan: impl FnOnce(&tidb_planner::statement_optimize::BuilderParts<'_>) -> R,
    ) -> R {
        let hooks = DriverHooks {
            catalog: self.catalog,
        };
        plan(&tidb_planner::statement_optimize::BuilderParts {
            catalog: self.catalog,
            current_database: self.current_database,
            ctx: self.ctx,
            use_plan_cache: self.use_plan_cache,
            plan_ids: self.plan_ids,
            column_ids: self.column_ids,
            session_ctx: self.session_ctx,
            registry: self.registry,
            hooks: &hooks,
            on_delete_root,
        })
    }

    /// Go `NewPlanBuilder().Init(sctx, is, ...)` for a query
    /// ([`tidb_planner::statement_optimize::BuilderParts::build`]).
    fn build<R>(
        &self,
        query: Option<&tidb_ast::QueryStmt>,
        building_view: Option<(&str, &str)>,
        build: impl FnOnce(
            &mut PlanBuilder<'_, tidb_infoschema::PlannerCatalog, crate::StmtContext>,
        ) -> Result<R, tidb_planner_core_base::PlanError>,
    ) -> Result<R, tidb_planner_core_base::PlanError> {
        self.planning(|parts| parts.build(query, building_view, build))
    }
}

/// Go `tryFillViewColumnType`'s build: `BuildDataSourceFromView` over the
/// view `database`.`view_name`, whose body is parsed and built -- never run --
/// under the view's own recursion guard, privilege rule and `ErrViewInvalid`
/// mapping. Each of `columns` takes the static type of the output column of
/// its name (`expression.FindFieldNameIdxByColName`), `None` when there is
/// none.
pub(crate) fn view_column_types(
    database: &str,
    view_name: &str,
    columns: &[String],
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Vec<Option<tidb_datatype::FieldType>>, tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database: database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    parts.build(None, None, |builder| {
        let view = builder
            .source
            .find_view(database, view_name)
            .cloned()
            .ok_or_else(|| {
                tidb_planner_core_base::PlanError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .fast_gen_by_args(&[database.into(), view_name.into()]),
                )
            })?;
        let plan = builder.build_data_source_from_view(&view)?;
        let types = logical_column_types(&plan);
        let names = plan.output_names();
        Ok(columns
            .iter()
            .map(|column| {
                names
                    .iter()
                    .position(|name| tidb_mysql::lowercase_equal(&name.names.column.lower, column))
                    .and_then(|index| types.get(index).cloned())
            })
            .collect())
    })
}

/// The static type of each output column of a built logical plan.
fn logical_column_types(plan: &LogicalPlan) -> Vec<tidb_datatype::FieldType> {
    plan.schema()
        .map(|schema| {
            schema
                .columns
                .iter()
                .map(|column| {
                    column.ret_type.clone().unwrap_or_else(|| {
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Go's `visitInfo` for a query: the privileges its plan needs, collected by
/// the plan builder while it resolves names and expands views (a DEFINER view's
/// body already verified against its definer, an INVOKER view's appended).
///
/// The query is only planned logically; nothing runs.
pub fn query_visit_info(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<tidb_planner::plan_builder::visit_info::VisitInfo>, super::DriverError> {
    // Planning raises warnings (a folded cast, a hint); this pass plans the
    // statement a second time, so its warnings must not reach the statement's.
    let _no_folding = tidb_expr::constant_fold::suspend_constant_folding();
    let warning_bookmark = ctx.warning_count();
    let planned = query_visit_info_inner(query, catalog, current_database, ctx);
    ctx.truncate_warnings(warning_bookmark);
    planned
}

fn query_visit_info_inner(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<tidb_planner::plan_builder::visit_info::VisitInfo>, super::DriverError> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let session_zone = ctx.session_zone();
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    // The plan is discarded, so an uncorrelated subquery is not RUN (the
    // catalog planned against is not the statement's execution image, whose
    // virtual tables are materialised per statement): the builder has
    // already visited its tables by the time it asks for a value, and a row of
    // NULLs lets the outer plan finish building.
    let evaluator = |inner: &LogicalPlan,
                     kind: tidb_planner::plan_builder::SubqueryKind,
                     _opt_flag: u64,
                     evaluate: bool| {
        let width = subquery_output_width(
            inner.schema().map_or(0, |schema| schema.len()),
            kind,
            evaluate,
        );
        Ok(tidb_planner::plan_builder::EvaluatedSubquery {
            column_ids: (0..width).map(|_| column_ids.alloc()).collect(),
            row: vec![tidb_datatype::Datum::Null; width],
            has_row: false,
            eval_ctx: std::sync::Arc::new(DiscardedSubquery),
        })
    };
    let defaults = default_evaluator(catalog, ctx);
    let mut builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, session_zone.clone())
        .with_subquery_evaluator(&evaluator)
        .with_default_evaluator(&defaults);
    builder.only_full_group_by = ctx.only_full_group_by();
    builder.enable_unsafe_substitute = ctx.enable_unsafe_substitute();
    builder.enable_force_inline_cte = ctx.enable_force_inline_cte();
    builder.enable_mpp_shared_cte_execution = ctx.enable_mpp_shared_cte_execution();
    builder.skip_cte_consumer_counts = ctx.skip_cte_consumer_counts();
    builder.oracle_mode = ctx.ddl_sql_mode() & tidb_mysql::consts::ModeOracle.0 != 0;
    builder.in_explain_stmt = ctx.in_explain_stmt();
    builder.in_explain_analyze_stmt = ctx.in_explain_analyze_stmt();
    builder.explain_non_evaled_sub_query = ctx.explain_non_evaled_sub_query();
    builder.parser_config = ctx.parser_config();
    let node = tidb_resolve::NodeW::new(query.clone());
    builder
        .build_query_node(&node, false)
        .map_err(super::planner_error_to_driver)?;
    Ok(std::mem::take(&mut builder.visit_info))
}

/// Go `PlanBuilder.buildInsert`, using one configured builder and one scalar
/// subquery registry for the target, SELECT, VALUES and ON DUPLICATE.
pub(crate) fn insert_plan(
    insert: &tidb_ast::InsertStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
) -> Result<
    (
        PhysicalPlan,
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
        Vec<std::sync::Arc<RegisteredScalarSubquery>>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    super::record_plan_builder_metric(ctx);
    if insert.source.is_some() {
        super::record_plan_builder_metric(ctx);
    }
    if insert.source.is_some() {
        ctx.phase_durations().reset_rewrite_phase_info();
    }
    let begin_rewrite = std::time::Instant::now();
    let (built, visits) = parts.build(insert.source.as_deref(), None, |builder| {
        let built = builder.build_insert(
            insert,
            |builder, logical, opt_flag| {
                ctx.set_block_as_names(builder.statement_block_as_names());
                ctx.phase_durations()
                    .set_duration_rewrite(begin_rewrite.elapsed());
                ctx.phase_durations().begin_optimize();
                if ctx.enable_alternative_logical_plans()
                    && opt_flag & flags::PUSH_DOWN_TOPN != 0
                    && opt_flag & flags::JOIN_REORDER != 0
                {
                    ctx.alternative_logical_plan_signals()
                        .order_aware_join_reorder
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                let logical = optimize_built_logical(
                    logical,
                    opt_flag,
                    catalog,
                    ctx,
                    use_plan_cache,
                    &plan_ids,
                    &column_ids,
                    &ctx.session_zone(),
                )?;
                physical_plan_for_logical(&logical, &plan_ids, &column_ids, catalog, ctx)
            },
            |target, on_duplicate_columns| {
                let spec = super::fk_trigger_plan::FkPlanSpec {
                    database: target.database.clone(),
                    table: target.table.clone(),
                    updated_cols: Vec::new(),
                    replace: insert.replace,
                    on_duplicate_cols: on_duplicate_columns.to_vec(),
                };
                super::fk_trigger_plan::build_fk_triggers(catalog, ctx, "Insert", &spec, &plan_ids)
                    .map_err(driver_error_to_plan)
            },
        )?;
        ctx.set_block_as_names(builder.statement_block_as_names());
        Ok((built, std::mem::take(&mut builder.visit_info)))
    })?;
    let registered = registry.borrow().clone();
    Ok((built, visits, registered))
}

/// Go `buildInsert`'s expression phase over a SELECT supplied by the prepared
/// plan cache. A cache hit does not build or optimize the source again.
pub(crate) fn cached_insert_expressions(
    insert: &tidb_ast::InsertStmt,
    physical: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner::plan_builder::insert::InsertExpressions, tidb_planner_core_base::PlanError>
{
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    parts.build(insert.source.as_deref(), None, |builder| {
        let target = builder.resolve_insert_target(insert)?;
        let select = match physical {
            PhysicalPlan::Dml(root) => root
                .select_plan
                .as_deref()
                .and_then(PhysicalPlan::schema)
                .zip(root.insert_select.as_ref()),
            _ => None,
        };
        builder.build_insert_expressions(insert, &target, select)
    })
}

/// Go `buildUpdateLists`'s resolution of a single-table UPDATE's SET items.
pub(crate) fn single_update_assignments(
    update: &tidb_ast::UpdateStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    Vec<tidb_planner::plan_builder::single_update::SingleUpdateAssignment>,
    tidb_planner_core_base::PlanError,
> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .resolve_single_update_assignments(update, ctx.allow_write_row_id())
}

/// Go `getAffectCols` of `buildInsert`: the columns an INSERT writes.
pub(crate) fn insert_target(
    insert: &tidb_ast::InsertStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    tidb_planner::plan_builder::insert_values::InsertTarget,
    tidb_planner_core_base::PlanError,
> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx);
    tidb_planner::plan_builder::insert_values::resolve_insert_target(&source, ctx, insert)
}

/// The shared DML-source build, following Go's allocation order:
/// the source builds logically, `buildSelectLock` wraps the single-table
/// UPDATE/DELETE source (`logical_plan_builder.go:6117/6552`, pessimistic
/// mode), the `Update`/`Delete` root allocates, and only then `DoOptimize`
/// lowers the tree. Returns the physical source, the UPDATE assignment
/// expressions, and the allocated DML root base for `PhysicalDmlRoot`.
///
/// `write` is the statement whose `visitInfo` the build collects, Go's
/// `buildUpdate` / `buildDelete` over one `PlanBuilder`: the read's (a
/// non-qualified DELETE pops its last, `popVisitInfo`), then the write's
/// (`UpdatePriv` per assigned table, `DeletePriv` per target); `None` for a
/// cached plan's rebuild, whose statement checks the visit infos PREPARE
/// kept.
#[allow(clippy::too_many_arguments)]
pub(crate) fn physical_dml_source_plan_with_allocators(
    select: &tidb_ast::SelectStmt,
    single_update: Option<&tidb_ast::UpdateStmt>,
    multi_update: Option<&tidb_ast::UpdateStmt>,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    operator: &str,
    delete_prune_has_fk: Option<bool>,
    delete_multi: Option<tidb_planner::plan_builder::delete_pruning::DeleteMultiInput>,
    on_delete_root: Option<&dyn Fn()>,
    write: Option<super::multi_dml::MultiDmlRef<'_>>,
) -> Result<DmlSourcePlan, tidb_planner_core_base::PlanError> {
    // Go builds an UPDATE/DELETE inside `builder.Build` (`buildUpdate` /
    // `buildDelete` run the read plan's `DoOptimize` themselves), so its
    // `DurationRewrite` covers the read plan's optimization too, and the
    // non-logical `Update`/`Delete` never starts `optimize`'s total clock.
    ctx.phase_durations().reset_rewrite_phase_info();
    let begin_rewrite = std::time::Instant::now();
    let registry = ScalarSubqueryRegistry::default();
    let parts = QueryBuilderParts {
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        registry: &registry,
        session_ctx: None,
    };
    // Go `buildUpdate` / `buildDelete` run inside `PlanBuilder.Build`.
    let built = parts.planning_with(on_delete_root, |parts| {
        parts.build(None, None, |builder| {
            let source = builder.source;
            let session_zone = ctx.session_zone();
            builder.delete_prune_has_fk = delete_prune_has_fk;
            builder.delete_multi = delete_multi;
            builder.allocate_delete_root = operator.eq_ignore_ascii_case("Delete");
            // Go `buildUpdate`/`buildDelete` lock the read only for a single-table
            // source (`TableRefs.Right == nil` / `!IsMultiTable`); a multi-table
            // write locks the rows it writes instead.
            let single_source = select.from.as_ref().is_some_and(|from| {
                from.right.is_none() && matches!(from.left, tidb_ast::JoinNode::Table(_))
            });
            let wrap_lock = ctx.pessimistic_transaction() && single_source;
            let mut single_assignments = None;
            let (plan, mut update_expressions, mut plan_flags) = match (single_update, multi_update)
            {
                (Some(update), _) => {
                    let (plan, expressions, assignments, flags) = builder.build_update_dml_source(
                        select,
                        update,
                        ctx.allow_write_row_id(),
                        wrap_lock,
                    )?;
                    single_assignments = Some(assignments);
                    (plan, expressions, flags)
                }
                (None, Some(update)) => builder.build_multi_update_dml_source(
                    select,
                    update,
                    ctx.allow_write_row_id(),
                )?,
                (None, None) => {
                    let (plan, build_flags) = builder.build_dml_source(select, wrap_lock)?;
                    (plan, Vec::new(), build_flags)
                }
            };
            ctx.set_block_as_names(builder.statement_block_as_names());
            // Go `buildUpdate` / `buildDelete`: the write's visit infos follow the
            // read's, before the read is optimized. The write targets resolve on
            // their own allocators: resolving a joined UPDATE's columns walks its
            // table refs again, which must not shift this build's plan ids.
            let mut visit_info = std::mem::take(&mut builder.visit_info);
            if let Some(write) = write {
                let (target_plan_ids, target_column_ids) =
                    (PlanIdAllocator::new(), ColumnIdAllocator::new());
                let mut targets = PlanBuilder::new(
                    source,
                    ctx,
                    &target_plan_ids,
                    &target_column_ids,
                    session_zone.clone(),
                );
                match write {
                    super::multi_dml::MultiDmlRef::Update(update) => {
                        visit_info.extend(targets.update_write_visits(update)?);
                    }
                    super::multi_dml::MultiDmlRef::Delete(delete) => {
                        // "If the delete is non-qualified it does not require Select
                        // Priv".
                        if delete.where_clause.is_none() && delete.order_by.is_empty() {
                            visit_info.pop();
                        }
                        visit_info.extend(targets.delete_write_visits(delete)?);
                    }
                }
            }
            // Go `buildUpdate`: "We cannot apply projection elimination when building
            // the subplan, because columns in orderedList cannot be resolved" -- the
            // assignments live outside the plan, so a rewrite that substitutes a
            // projected column never reaches them (`DoOptimize(..., b.optFlag &^
            // rule.FlagEliminateProjection, p)`).
            if operator.eq_ignore_ascii_case("Update") {
                plan_flags &= !tidb_planner_core_rule::flags::ELIMINATE_PROJECTION;
            }
            // Go `buildUpdate`/`buildDelete` allocate the `Update`/`Delete` root
            // after the logical build (which now includes `buildSelectLock` and the
            // freezing projection) and before `DoOptimize`. The optimize pass then
            // allocates the physical projection candidate (Go id 6 in the captured
            // W51 receipt) before the physical `SelectLock` — reproducing Go's
            // exact id sequence without a filler.
            let root = builder.dml_root.take().unwrap_or_else(|| {
                tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(plan_ids, operator, 0)
            });
            let logical = optimize_built_logical(
                plan,
                plan_flags,
                catalog,
                ctx,
                use_plan_cache,
                plan_ids,
                column_ids,
                &session_zone,
            )?;
            // Go `buildUpdateLists`: the columns a non-constant assignment reads
            // join `StmtCtx.ColRefFromUpdatePlan`.
            let col_ref_from_update_plan: std::collections::BTreeSet<i64> = update_expressions
                .iter()
                .flatten()
                .flat_map(tidb_expr::simple_expr::extract_columns)
                .map(|column| column.unique_id)
                .collect();
            let physical = physical_plan_for_logical_with_update_refs(
                &logical,
                plan_ids,
                column_ids,
                catalog,
                ctx,
                &col_ref_from_update_plan,
            )?;
            let schema = physical.schema().ok_or_else(|| {
                tidb_planner_core_base::PlanError::internal("physical DML source has no schema")
            })?;
            for expression in update_expressions.iter_mut().flatten() {
                tidb_expr::simple_expr::resolve_indices_in_place(expression, schema).map_err(
                    |_| {
                        tidb_planner_core_base::PlanError::internal(
                            "UPDATE assignment does not resolve in its physical source",
                        )
                    },
                )?;
            }
            ctx.phase_durations()
                .set_duration_rewrite(begin_rewrite.elapsed());
            Ok(DmlSourcePlan {
                plan: physical,
                update_expressions,
                single_assignments,
                root,
                delete_col_pos: builder.delete_col_pos.take(),
                delete_tables: std::mem::take(&mut builder.delete_tables),
                update_tables: std::mem::take(&mut builder.update_tables),
                update_assigns: std::mem::take(&mut builder.update_assigns),
                visit_info,
                scalar_subqueries: Vec::new(),
            })
        })
    })?;
    let scalar_subqueries = registry.borrow().clone();
    Ok(DmlSourcePlan {
        scalar_subqueries,
        ..built
    })
}

/// What the shared DML-source build produces for the `Update`/`Delete` root.
pub(crate) struct DmlSourcePlan {
    /// The optimized physical read.
    pub plan: PhysicalPlan,
    /// The statement's visit infos (Go `builder.GetVisitInfo()`): the read's,
    /// then the write's.
    pub visit_info: Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
    /// The UPDATE assignments' expressions, resolved against the read.
    pub update_expressions: Vec<Option<tidb_expr::expression::Expression>>,
    /// The allocated root base.
    pub root: tidb_planner_core_operator_physicalop::BasePhysicalPlan,
    /// A pruned single-table DELETE's layout.
    pub delete_col_pos: Option<tidb_planner_core_operator_physicalop::DeleteColPos>,
    /// A multi-table DELETE's layouts.
    pub delete_tables: Vec<tidb_planner_core_operator_physicalop::DeleteTablePos>,
    /// A multi-table UPDATE's layouts.
    pub update_tables: Vec<tidb_planner_core_operator_physicalop::UpdateTablePos>,
    /// A multi-table UPDATE's assignments.
    pub update_assigns: Vec<tidb_planner_core_operator_physicalop::UpdateAssign>,
    /// A single-table UPDATE's SET items, resolved over the read (Go
    /// `buildUpdateLists`).
    pub single_assignments:
        Option<Vec<tidb_planner::plan_builder::single_update::SingleUpdateAssignment>>,
    /// The scalar subqueries the read evaluated while it was built (Go
    /// `SessionVars.MapScalarSubQ`), which EXPLAIN renders after the plan.
    pub scalar_subqueries: Vec<std::sync::Arc<RegisteredScalarSubquery>>,
}

/// Go `buildOrderedList` for the fast point UPDATE (`tryUpdatePointPlan`): the
/// assignments rewritten over the point plan's schema and cast to their
/// target columns, resolved to that schema's row.
pub(crate) fn point_update_ordered_list(
    table_ref: &tidb_ast::TableRef,
    schema: &tidb_expr::schema::Schema,
    assignments: &[(Vec<String>, Option<tidb_ast::Expr>)],
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<Vec<Option<Expression>>, tidb_planner_core_base::PlanError> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let defaults = default_evaluator(catalog, ctx);
    let builder = PlanBuilder::new(&source, ctx, plan_ids, column_ids, ctx.session_zone())
        .with_default_evaluator(&defaults);
    let mut list = builder.build_point_ordered_list(table_ref, schema, assignments)?;
    for expression in list.iter_mut().flatten() {
        tidb_expr::simple_expr::resolve_indices_in_place(expression, schema).map_err(|_| {
            tidb_planner_core_base::PlanError::internal(
                "a fast UPDATE assignment does not resolve in its point plan",
            )
        })?;
    }
    Ok(list)
}

/// Go `buildDelete`'s resolution of a multi-table DELETE's targets.
pub(crate) fn delete_targets(
    delete: &tidb_ast::DeleteStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Result<
    Vec<tidb_planner::plan_builder::delete_pruning::DeleteTarget>,
    tidb_planner_core_base::PlanError,
> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx)
        .with_statement_related_tables_for_mdl(ctx);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, ctx.session_zone())
        .resolve_delete_targets(delete)
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
            planner_physical_query(query, catalog, current_database, ctx, true)
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
