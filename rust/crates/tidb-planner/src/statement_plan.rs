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

//! Go `planner.Optimize` over the statements other than UPDATE and DELETE:
//! queries (with their scalar subqueries), SHOW, SET, the other statements
//! `PlanBuilder.Build` plans, IMPORT INTO's query, ADMIN SHOW DDL JOBS,
//! CREATE VIEW and a view's column types, INSERT, and the visit infos a
//! discarded build collects.

use std::cell::RefCell;
use std::rc::Rc;

use tidb_driver_error::DriverError;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_planner_core_base::{ColumnIdAllocator, PlanIdAllocator};
use tidb_planner_core_operator_logicalop::LogicalPlan;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_sessionctx::StmtContext;

use crate::catalog_ctx::PlannerCatalogContext as _;
use crate::optimize::{
    BuildVisits, BuilderParts, DiscardedSubquery, QueryBuild, ScalarSubqueryEvalCtx,
    ScalarSubqueryRegistry, driver_error_to_plan, subquery_output_width,
};
use crate::optimizer::{optimize_built_logical, physical_plan_for_logical};
use crate::plan_builder::PlanBuilder;
use crate::stmt_ctx::PlannerStmtCtx as _;
use tidb_infoschema::table_source::TableSource as _;
use tidb_planner_core_rule::flags;

fn planner_physical_select(
    hooks: &dyn crate::optimize::PlanningHooks,
    select: &tidb_ast::SelectStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    use_plan_cache: bool,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
    let query = tidb_ast::QueryStmt::Select(Box::new(select.clone()));
    planner_optimized_query_with_allocators(
        hooks,
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

/// Go `planner.optimize` over a query with fresh allocators.
///
/// # Errors
///
/// The planning's failure.
pub fn planner_physical_query(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    use_plan_cache: bool,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    planner_physical_query_with_allocators(
        hooks,
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
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    visits: BuildVisits<'_>,
    build: QueryBuild,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
    planner_physical_query_with_registry(
        hooks,
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
pub fn planner_physical_query_with_registry(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    visits: BuildVisits<'_>,
    build: QueryBuild,
) -> Result<
    (
        LogicalPlan,
        PhysicalPlan,
        Vec<std::sync::Arc<ScalarSubqueryEvalCtx>>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let registry = ScalarSubqueryRegistry::default();
    let (logical, physical) = planner_optimized_query_with_allocators(
        hooks,
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
pub fn physical_select_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    select: &tidb_ast::SelectStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    planner_physical_select(hooks, select, catalog, current_database, ctx, false)
        .map(|(_, physical)| physical)
}

/// [`physical_select_plan`] drawing plan and column ids from the given
/// allocators, as Go's session-wide `PlanID` / `PlanColumnID` counters do
/// for several selects optimized under one statement.
pub fn physical_select_plan_with_allocators(
    hooks: &dyn crate::optimize::PlanningHooks,
    select: &tidb_ast::SelectStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    let query = tidb_ast::QueryStmt::Select(Box::new(select.clone()));
    planner_optimized_query_with_allocators(
        hooks,
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

/// Builds the ordinary physical tree for either Go query-statement shape.
/// Set operations, their CTEs, and plain SELECTs therefore all enter the same
/// logical optimizer, physical search, and executor-builder switch.
pub fn physical_query_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    planner_physical_query(hooks, query, catalog, current_database, ctx, false)
        .map(|(_, physical)| physical)
}

/// [`physical_query_plan`], additionally handing back the uncorrelated
/// subqueries the build evaluated. Go keeps them in `StmtCtx` and EXPLAIN
/// appends each one's optimized child as an extra root.
pub fn physical_query_plan_with_scalar_subqueries(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    visits: BuildVisits<'_>,
) -> Result<
    (PhysicalPlan, Vec<std::sync::Arc<ScalarSubqueryEvalCtx>>),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    planner_physical_query_with_registry(
        hooks,
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

/// Go `planner.Optimize` for a query statement
/// ([`crate::optimize::optimize_query_stmt`]), handing back
/// the uncorrelated subqueries the build evaluated: plain EXPLAIN appends
/// each registered child as its own `ScalarSubQuery` root.
pub fn optimize_query_stmt(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<(PhysicalPlan, Vec<std::sync::Arc<ScalarSubqueryEvalCtx>>), DriverError> {
    let (plan_ids, column_ids) = (PlanIdAllocator::new(), ColumnIdAllocator::new());
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        hooks,
        on_delete_root: None,
        catalog,
        current_database,
        ctx,
        use_plan_cache: false,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    let physical = crate::optimize::optimize_query_stmt(&parts, query)?;
    let registered = Rc::try_unwrap(registry)
        .map(RefCell::into_inner)
        .unwrap_or_default();
    Ok((physical, registered))
}

/// Go `planner.optimize` over a query
/// ([`crate::optimize::optimize_query`]) with this
/// statement's hooks.
#[allow(clippy::too_many_arguments)]
pub fn planner_optimized_query_with_allocators(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    registry: &ScalarSubqueryRegistry,
    visits: BuildVisits<'_>,
    build: QueryBuild,
) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
    let parts = BuilderParts {
        hooks,
        on_delete_root: None,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids,
        column_ids,
        registry,
        session_ctx: None,
    };
    crate::optimize::optimize_query(&parts, query, visits, build)
}

/// Go `planner.Optimize` for a `ShowStmt`: `PlanBuilder.Build`'s
/// `buildShow` over the preprocessed statement, then the logical and
/// physical optimization every plan goes through.
pub fn physical_show_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    show: &tidb_ast::ShowStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<
    (
        crate::common_plans::Plan,
        Vec<crate::plan_builder::visit_info::VisitInfo>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        hooks,
        on_delete_root: None,
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
    let crate::common_plans::Plan::Logical(plan) = plan else {
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
        .map(|physical| (crate::common_plans::Plan::Physical(physical), visits))
}

/// Go `buildSet` for a `SET` statement: the plan and its visit infos.
pub fn set_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    set: &tidb_ast::SetStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<
    (
        crate::common_plans::Set,
        Vec<crate::plan_builder::visit_info::VisitInfo>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        hooks,
        on_delete_root: None,
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
        let crate::common_plans::Plan::Set(plan) = builder.build_set(set)? else {
            unreachable!("buildSet answers a Set plan");
        };
        Ok((plan, builder.visit_info.clone()))
    })
}

/// Go `PlanBuilder.Build` for a statement that is not a query or DML: the
/// common plan and its visit infos.
pub fn statement_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    stmt: &tidb_ast::Stmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    session_ctx: &dyn crate::plan_builder::stmt_build::SessionPlanContext,
) -> Result<
    (
        crate::common_plans::Plan,
        Vec<crate::plan_builder::visit_info::VisitInfo>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        hooks,
        on_delete_root: None,
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
    if let crate::common_plans::Plan::ImportInto(import) = &mut plan {
        if let Some(select) = import.select_plan.take() {
            let crate::common_plans::Plan::Logical(logical) = *select else {
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
            import.select_plan = Some(Box::new(crate::common_plans::Plan::Physical(physical)));
        }
    }
    Ok((plan, visits))
}

/// Go `Compiler.Compile` of the SELECT of an import query
/// (`buildImportQueryExecutor`): the query built as `buildImportInto` builds
/// its source, then optimized into a physical plan.
pub fn import_query_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    session_ctx: &dyn crate::plan_builder::stmt_build::SessionPlanContext,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, tidb_planner_core_base::PlanError>
{
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        hooks,
        on_delete_root: None,
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
pub fn physical_admin_show_ddl_jobs_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    job_number: i64,
    where_clause: Option<&tidb_ast::Expr>,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        hooks,
        on_delete_root: None,
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
pub fn build_create_view(
    hooks: &dyn crate::optimize::PlanningHooks,
    create: &mut tidb_ast::CreateViewStmt,
    view_schema: &str,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<Vec<tidb_datatype::FieldType>, tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        hooks,
        on_delete_root: None,
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

/// Go `tryFillViewColumnType`'s build: `BuildDataSourceFromView` over the
/// view `database`.`view_name`, whose body is parsed and built -- never run --
/// under the view's own recursion guard, privilege rule and `ErrViewInvalid`
/// mapping. Each of `columns` takes the static type of the output column of
/// its name (`expression.FindFieldNameIdxByColName`), `None` when there is
/// none.
pub fn try_fill_view_column_type(
    hooks: &dyn crate::optimize::PlanningHooks,
    database: &str,
    view_name: &str,
    columns: &[String],
    catalog: &SessionExtendedInfoSchema,
    ctx: &StmtContext,
) -> Result<Vec<Option<tidb_datatype::FieldType>>, tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        hooks,
        on_delete_root: None,
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
pub fn logical_column_types(plan: &LogicalPlan) -> Vec<tidb_datatype::FieldType> {
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
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<Vec<crate::plan_builder::visit_info::VisitInfo>, DriverError> {
    // Planning raises warnings (a folded cast, a hint); this pass plans the
    // statement a second time, so its warnings must not reach the statement's.
    let _no_folding = tidb_expr::constant_fold::suspend_constant_folding();
    let warning_bookmark = ctx.warning_count();
    let planned = query_visit_info_inner(hooks, query, catalog, current_database, ctx);
    ctx.truncate_warnings(warning_bookmark);
    planned
}

fn query_visit_info_inner(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<Vec<crate::plan_builder::visit_info::VisitInfo>, DriverError> {
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
                     kind: crate::plan_builder::SubqueryKind,
                     _opt_flag: u64,
                     evaluate: bool| {
        let width = subquery_output_width(
            inner.schema().map_or(0, |schema| schema.len()),
            kind,
            evaluate,
        );
        Ok(crate::plan_builder::EvaluatedSubquery {
            column_ids: (0..width).map(|_| column_ids.alloc()).collect(),
            row: vec![tidb_datatype::Datum::Null; width],
            has_row: false,
            eval_ctx: std::sync::Arc::new(DiscardedSubquery),
        })
    };
    let defaults = |database: &str, table: &str, column: &str| {
        hooks.field_default(ctx, database, table, column)
    };
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
        .map_err(crate::optimize::planner_error_to_driver)?;
    Ok(std::mem::take(&mut builder.visit_info))
}

/// Go `PlanBuilder.buildInsert`, using one configured builder and one scalar
/// subquery registry for the target, SELECT, VALUES and ON DUPLICATE.
pub fn insert_plan(
    hooks: &dyn crate::optimize::PlanningHooks,
    insert: &tidb_ast::InsertStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
    use_plan_cache: bool,
) -> Result<
    (
        PhysicalPlan,
        Vec<crate::plan_builder::visit_info::VisitInfo>,
        Vec<std::sync::Arc<ScalarSubqueryEvalCtx>>,
    ),
    tidb_planner_core_base::PlanError,
> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        hooks,
        on_delete_root: None,
        catalog,
        current_database,
        ctx,
        use_plan_cache,
        plan_ids: &plan_ids,
        column_ids: &column_ids,
        registry: &registry,
        session_ctx: None,
    };
    crate::optimize::record_plan_builder_metric(ctx);
    if insert.source.is_some() {
        crate::optimize::record_plan_builder_metric(ctx);
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
                let spec = crate::dml_optimize::FkPlanSpec {
                    database: target.database.clone(),
                    table: target.table.clone(),
                    updated_cols: Vec::new(),
                    replace: insert.replace,
                    on_duplicate_cols: on_duplicate_columns.to_vec(),
                };
                crate::dml_optimize::build_fk_triggers(catalog, ctx, "Insert", &spec, &plan_ids)
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
pub fn cached_insert_expressions(
    hooks: &dyn crate::optimize::PlanningHooks,
    insert: &tidb_ast::InsertStmt,
    physical: &PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<crate::plan_builder::insert::InsertExpressions, tidb_planner_core_base::PlanError> {
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let registry = ScalarSubqueryRegistry::default();
    let parts = BuilderParts {
        hooks,
        on_delete_root: None,
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

/// Go `getAffectCols` of `buildInsert`: the columns an INSERT writes.
pub fn insert_target(
    insert: &tidb_ast::InsertStmt,
    catalog: &SessionExtendedInfoSchema,
    current_database: &str,
    ctx: &StmtContext,
) -> Result<crate::plan_builder::insert_values::InsertTarget, tidb_planner_core_base::PlanError> {
    let source = catalog
        .planner_catalog(current_database, ctx.latest_index_schema())
        .with_statement_hypo_state(ctx)
        .with_statement_dirty_tables(ctx)
        .with_statement_privilege_verifier(ctx);
    crate::plan_builder::insert_values::resolve_insert_target(&source, ctx, insert)
}

/// The Index Advisor's `queryPlanCost` (`pkg/planner/optimize.go`): the
/// root cost of the plan the optimizer picks for `query`, with the
/// `tidb_opt_fix_control` 43817 rule the advisor turns on -- a plan that would
/// evaluate an uncorrelated scalar subquery while planning is refused.
/// # Errors
///
/// The planning's failure, or a plan that evaluated a subquery.
pub fn query_plan_cost(
    hooks: &dyn crate::optimize::PlanningHooks,
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &StmtContext,
) -> Result<f64, DriverError> {
    crate::preprocess::validate_query_usage(query, ctx)?;
    let (mut physical, scalar_subqueries) =
        optimize_query_stmt(hooks, query, catalog, current_db, ctx)?;
    if !scalar_subqueries.is_empty() {
        return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown("evaluate non-correlated sub-queries during optimization phase is not allowed by fix-control 43817"
                .to_owned(),
        )));
    }
    hooks.prepare_plan(&mut physical, ctx)?;
    let coster = crate::find_best_task::coster::Ver2Coster::from_env(ctx.optimizer_cost_env())
        .with_scan_penalty_vars(ctx.allow_prefer_range_scan(), ctx.get_index_force())
        .with_explain_cache();
    Ok(coster
        .plan_cost(
            &physical,
            tidb_planner_property::task_type::TaskType::Root,
            false,
        )
        .value())
}
