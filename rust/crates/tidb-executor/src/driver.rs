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

//! A minimal query driver: parse a SQL string, rewrite its expressions, wire the
//! executors, and run it -- the first end-to-end parse -> plan -> execute of a
//! SQL string.
//!
//! SCOPE: `SELECT <exprs | *> [FROM <table>] [WHERE <pred>] [ORDER BY ...]
//! [LIMIT ...]` over a single in-memory [`SessionExtendedInfoSchema`] table or the implicit dual
//! row. It parses via `tidb-parser`, resolves `FROM` against the catalog,
//! rewrites fields/predicates/by-items through
//! [`tidb_expr::rewriter::rewrite_expr_resolved`] (columns bound by the
//! [`TableResolver`]), and wires `MemTableSource|TableDual ->
//! [Selection] -> [Sort] -> Projection -> [Limit]`.
//!
//! Storage-backed reads use [`TableScanExec`] and [`crate::remote_scan`];
//! [`MemTableSourceExec`] remains the source for catalog-backed in-memory
//! tables.

use tidb_executor_internal_exec::Executor;
use tidb_driver_error::ExecError;
use tidb_table::tables::TableHandle;
use tidb_variable::mem_quota;
use std::sync::Arc;
use tidb_ast::{QueryStmt, Stmt, Visitable, Visitor};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver};

use tidb_expr::schema::Schema;
use tidb_planner::plan_builder::field_name::default_field_display_name;

pub mod access;
use tidb_planner::point_get_plan;
mod catalog_ctx;
#[cfg(test)]
mod catalog_planner_view_tests;
#[cfg(test)]
mod planner_mview_tests;
#[cfg(test)]
mod casetest_mview_tests;
#[cfg(test)]
mod catalog_statistics_tests;
mod table_resolver;
pub(crate) use table_resolver::TableResolver;
mod dml;
pub(crate) use tidb_planner::from_scope as from;
mod multi_dml;
pub mod mview_delta_apply;
pub mod mview_delta_merge;
pub(crate) mod mview_delta_merge_agg_builder;
pub(crate) use multi_dml::{multi_dml_explain_plan, MultiDmlRef};

pub use dml::dml_visit_info;
pub use planner_bridge::query_visit_info;
mod record_set;
pub use record_set::RecordSet;
pub(crate) mod fk_trigger_plan;
pub(crate) mod params;
pub(crate) mod physical_builder;
pub mod plan_cache;
pub(crate) mod planner_bridge;
pub(crate) use tidb_planner::point_get_key;
pub(crate) mod set_opr;
mod subquery;
#[cfg(test)]
mod tests;
pub(crate) mod write_cast;

// Re-exported flat, so every caller inside this module keeps naming these as
// `driver::…` exactly as before the split.
pub(crate) use access::*;
pub(crate) use point_get_plan::*;
use tidb_infoschema::*;
pub use catalog_ctx::CatalogStatisticsLoad;
pub(crate) use catalog_ctx::PlannerCatalogContext;
pub use dml::*;
pub(crate) use from::*;
pub use params::*;
pub use set_opr::*;
pub(crate) use write_cast::*;

pub mod error;
pub(crate) use error::coprocessor_evaluation_error;
pub(crate) use error::{DriverError, MysqlError};

/// Parses and runs a `FROM`-less `SELECT`, returning its rows as `Datum`s.
pub fn run_select(sql: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
    run_select_on(sql, &tidb_infoschema_perfschema::mock_catalog(), &crate::StmtContext::for_query())
}

/// Parses and runs a single-table (or `FROM`-less) `SELECT` against `catalog`,
/// returning its rows as `Datum`s.
pub fn run_select_on(
    sql: &str,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    run_select_meta_on(sql, catalog, ctx).map(|(_, rows)| rows)
}

/// A `SELECT` result with metadata: the output columns as `(name, type)`, then
/// the rows.
pub type SelectMeta = (Vec<(String, FieldType)>, Vec<Vec<Datum>>);

/// Like [`run_select_on`], but also returns the result-column metadata the
/// wire protocol needs: one `(name, type)` per output column.
///
/// Naming follows Go's result-field resolution in spirit, simplified for the
/// seed driver: an `AS` alias wins; a plain column reference uses the column's
/// own name; any other expression uses the text it was WRITTEN with (Go's
/// `SelectField.Text`, see [`default_field_display_name`]), falling back to
/// its restored text when the parser recorded no source span for the field;
/// `*` expands to the table's column names.
pub fn run_select_meta_on(
    sql: &str,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_select_meta_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_select_meta_on`] resolving unqualified names in `current_db`.
pub fn run_select_meta_in(
    sql: &str,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    let stmt = ctx.parse(sql)?;

    let query = match &stmt {
        Stmt::Query(query) => query.as_ref(),
        _ => return Err(DriverError::unsupported("only SELECT is supported")),
    };
    run_query_stmt(query, catalog, current_db, ctx)
}

/// Go `preprocess` and `buildDDL`'s `*ast.CreateViewStmt` arm for `CREATE
/// VIEW` (`planbuilder.go`): the preprocessor's set-operation and sequence
/// checks visit the whole body, then the planner builds it -- never runs it --
/// leaving the expanded body in `create.query` and the view's column names in
/// `create.columns`. Returns the body's output column types.
pub(crate) fn build_create_view(
    create: &mut tidb_ast::CreateViewStmt,
    view_schema: &str,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<FieldType>, DriverError> {
    validate_query_sequence_names(&create.query, ctx)?;
    set_opr::validate_query_usage(&create.query, ctx)?;
    planner_bridge::build_create_view(create, view_schema, catalog, current_db, ctx)
        .map_err(planner_error_to_driver)
}

/// Go `tryFillViewColumnType`: the static type of each of the view's
/// `columns`, from `BuildDataSourceFromView` over `database`.`view_name`; see
/// [`planner_bridge::view_column_types`].
pub(crate) fn view_column_types(
    database: &str,
    view_name: &str,
    columns: &[String],
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<Vec<Option<FieldType>>, DriverError> {
    planner_bridge::view_column_types(database, view_name, columns, catalog, ctx)
        .map_err(planner_error_to_driver)
}

/// Go's expression builder resolves the name argument of every sequence
/// builtin before it constructs a physical executor. Keep that ordering for
/// Rust's row/chunk evaluators: a later invalid projection item must not allow
/// an earlier `NEXTVAL` item to consume a value first.
fn validate_query_sequence_names(
    query: &QueryStmt,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    struct SequenceNameValidator<'a> {
        ctx: &'a crate::StmtContext,
        error: Option<DriverError>,
    }

    impl Visitor for SequenceNameValidator<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
                return self.error.is_some();
            };
            let (tidb_ast::Expr::Func { name, args, .. }
            | tidb_ast::Expr::GenericFuncCall { name, args, .. }) = expr
            else {
                return self.error.is_some();
            };
            if !matches!(
                name.to_ascii_lowercase().as_str(),
                "nextval" | "lastval" | "setval"
            ) {
                return self.error.is_some();
            }
            let Some(tidb_ast::Expr::Column(path)) = args.first() else {
                return self.error.is_some();
            };
            if let Err(error) = self.ctx.validate_sequence_path(path) {
                self.error = Some(DriverError::Exec(crate::ExecError::Eval(error)));
                return true;
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            self.error.is_none()
        }
    }

    let mut query = query.clone();
    let mut validator = SequenceNameValidator { ctx, error: None };
    query.accept(&mut validator);
    validator.error.map_or(Ok(()), Err)
}

/// Runs a `QueryStmt` of either shape against the catalog: the same dispatch
/// [`build_derived_source`] makes over a derived table's subquery, factored
/// out so the lateral-over-set-operation path can share it.
pub(crate) fn run_query_stmt(
    query: &QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    open_query_meta_stmt_with_physical(query, None, catalog, current_db, ctx)?.collect()
}

// An EXECUTE's `planner.Optimize`: Go plans the prepared statement inside
// the EXECUTE's one `planner.Optimize` (`GetPlanFromPlanCache`), so while the
// session holds it the planning steps open no region of their own, and the
// handover to execution ends it.
thread_local! {
    static HELD_OPTIMIZE_REGION: std::cell::RefCell<Option<tidb_tracing::ThreadRegion>> =
        const { std::cell::RefCell::new(None) };
}

/// Holds Go `planner.Optimize`'s region over a prepared statement's planning
/// (the EXECUTE that runs it is the statement Go compiles); see
/// [`end_held_optimize_region`].
pub fn hold_optimize_region() {
    let region = tidb_tracing::start_thread_region("planner.Optimize");
    if region.span().is_some() {
        HELD_OPTIMIZE_REGION.with(|held| *held.borrow_mut() = Some(region));
    }
}

/// Ends the region [`hold_optimize_region`] holds, if any.
pub fn end_held_optimize_region() {
    let held = HELD_OPTIMIZE_REGION.with(|held| held.borrow_mut().take());
    drop(held);
}

/// Go `planner.Optimize`'s region for one statement's planning; none while
/// an EXECUTE holds it ([`hold_optimize_region`]).
pub(crate) fn start_optimize_region() -> OptimizeRegion {
    let held = HELD_OPTIMIZE_REGION.with(|held| held.borrow().is_some());
    OptimizeRegion((!held).then(|| tidb_tracing::start_thread_region("planner.Optimize")))
}

/// See [`start_optimize_region`].
#[must_use = "the region ends when it drops"]
pub(crate) struct OptimizeRegion(Option<tidb_tracing::ThreadRegion>);

impl OptimizeRegion {
    /// Go `Region.End`.
    pub(crate) fn end(self) {}
}

/// Go `session.ExecuteStmt` handing the compiled statement from
/// `executor.Compile` to `session.runStmt`, which logs the statement's text
/// (`r.Span.LogKV("sql", s.Text())`). The session opens `executor.Compile`
/// with the text; once it has ended this does nothing, nor on an untraced
/// thread.
pub fn begin_run_stmt_region() {
    end_held_optimize_region();
    if let Some(text) = tidb_tracing::end_thread_region("executor.Compile") {
        if let Some(span) = tidb_tracing::begin_thread_region("session.runStmt", None) {
            span.log_kv("sql", text.as_deref().unwrap_or_default());
        }
    }
}

/// Go `session.ExecuteStmt`'s point-plan short path: a prepared point get in
/// autocommit runs `ExecStmt.PointGet` (which logs the statement's text)
/// where any other statement runs `session.runStmt`.
pub fn begin_point_get_region() {
    end_held_optimize_region();
    if let Some(text) = tidb_tracing::end_thread_region("executor.Compile") {
        if let Some(span) = tidb_tracing::begin_thread_region("ExecStmt.PointGet", None) {
            span.log_kv("sql", text.as_deref().unwrap_or_default());
        }
    }
}

/// Go `ExecStmt.handleNoDelayExecutor`'s region and, under it, the `exec.Next`
/// region of the DML executor it drives (`executor` is Go's type name); the
/// DML runs inside both until the value drops.
pub(crate) struct DmlExecutionRegions {
    // Dropped in declaration order: the executor's call ends first.
    _next: tidb_tracing::ThreadRegion,
    _handle: tidb_tracing::ThreadRegion,
}

/// [`begin_run_stmt_region`], then [`DmlExecutionRegions`] for `executor`.
pub(crate) fn begin_dml_execution(executor: &'static str) -> DmlExecutionRegions {
    begin_run_stmt_region();
    let handle = tidb_tracing::start_thread_region_ex("executor.handleNoDelayExecutor");
    let next = tidb_tracing::start_thread_region_ex(executor);
    DmlExecutionRegions {
        _next: next,
        _handle: handle,
    }
}

/// Go `planner.optimize`: try the complete fast physical plan first, then
/// build and optimize the ordinary logical tree exactly once.
/// Go `PlanBuilder.recordPlanBuilderMetric`: one `PlanBuilder.Build` call,
/// RU v2's `plan_cnt`.
pub(crate) fn record_plan_builder_metric(ctx: &crate::StmtContext) {
    if let Some(metrics) = ctx.ruv2_metrics() {
        metrics.add_plan_cnt(1);
    }
}

pub(crate) fn optimize_query_stmt(
    query: &QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    optimize_query_stmt_with_scalar_subqueries(query, catalog, current_db, ctx)
        .map(|(physical, _)| physical)
}

/// [`optimize_query_stmt`], additionally handing back the uncorrelated
/// subqueries the build evaluated. Plain EXPLAIN appends each registered
/// child as its own `ScalarSubQuery` root, which is why only the EXPLAIN
/// entry point needs them.
pub(crate) fn optimize_query_stmt_with_scalar_subqueries(
    query: &QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<
    (
        tidb_planner_core_operator_physicalop::PhysicalPlan,
        Vec<std::sync::Arc<planner_bridge::RegisteredScalarSubquery>>,
    ),
    DriverError,
> {
    if let (QueryStmt::Select(select), true) = (query, ctx.isolation_read_contains_tikv()) {
        // Go `TryFastPlan`'s `checkFastPlanPrivilege(..., mysql.SelectPriv)`.
        let fast_plan_privilege = |database: &str, table: &str| {
            crate::table_privilege::check_fast_plan_privilege(
                ctx,
                catalog,
                database,
                table,
                &[tidb_mysql::SelectPriv],
            )
        };
        let fast = match dml::handed_point_plan(ctx, dml::PointPlanKind::Select) {
            // Go `Optimize`: the point plan a multi-statement query's
            // prefetch already tried (`PointPlanKey`), not `TryFastPlan`
            // again.
            Some(handed) => handed.plan.clone(),
            None => point_get_plan::try_fast_plan_checked(
                select,
                catalog,
                current_db,
                ctx,
                &fast_plan_privilege,
            )?,
        };
        if let Some(plan) = fast {
            return Ok((plan, Vec::new()));
        }
    }
    // Go `optimize` -> `buildLogicalPlan` -> `PlanBuilder.Build`, past
    // `TryFastPlan`: the statement's one build, whose visit infos
    // `CheckPrivilege` reads before the optimizer runs.
    record_plan_builder_metric(ctx);
    let planned = planner_bridge::physical_query_plan_with_scalar_subqueries(
        query,
        catalog,
        current_db,
        ctx,
        planner_bridge::BuildVisits::Check,
    )
    .map_err(planner_error_to_driver)?;
    // Go `buildSelectInto` optimizes the target query (whose own
    // `CheckPrivilege` ran above) and only then appends FILE, which the
    // statement's `CheckPrivilege` checks.
    if matches!(query, QueryStmt::Select(select) if select.into_outfile.is_some()) {
        crate::table_privilege::check_plan_privilege_requests(
            ctx,
            catalog,
            &[crate::table_privilege::select_into_file_request()],
        )?;
    }
    Ok(planned)
}

pub(super) fn run_physical_set_opr_stmt(
    set_opr: &tidb_ast::SetOprStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_query_stmt(
        &QueryStmt::SetOpr(Box::new(set_opr.clone())),
        catalog,
        current_db,
        ctx,
    )
}

pub(super) fn planner_error_to_driver(error: tidb_planner_core_base::PlanError) -> DriverError {
    match error.kind() {
        tidb_planner_core_base::PlanErrorKind::Mysql(error) => DriverError::Mysql(error.clone()),
        tidb_planner_core_base::PlanErrorKind::Eval(tidb_exprctx::EvalError::InvalidGroupFuncUse) => {
            DriverError::from(
                tidb_plannererrors::ERR_INVALID_GROUP_FUNC_USE
                    .gen_with_stack_by_args(&[]),
            )
        }
        tidb_planner_core_base::PlanErrorKind::Eval(
            tidb_exprctx::EvalError::FieldInGroupingNotGroupBy(index),
        ) => DriverError::from(
            tidb_plannererrors::ERR_FIELD_IN_GROUPING_NOT_GROUP_BY
                .gen_with_stack_by_args(&[format!("#{}", *index).into()]),
        ),
        tidb_planner_core_base::PlanErrorKind::Eval(eval) => {
            DriverError::Exec(ExecError::Eval(eval.clone()))
        }
        tidb_planner_core_base::PlanErrorKind::Internal => {
            DriverError::unsupported(error.to_string())
        }
        tidb_planner_core_base::PlanErrorKind::MemTableRetrievalNeeded => {
            DriverError::MemTableRetrievalNeeded
        }
    }
}

/// Runs one parsed `SELECT` against the catalog, for a caller that has
/// already rewritten the statement (session-variable binding, for instance)
/// and must not go back through SQL text.
pub fn run_select_meta_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_select_meta_stmt_with_physical(select, None, catalog, current_db, ctx)
}

/// Runs one parsed `SELECT` through the ordinary executor builder, optionally
/// consuming a physical tree already selected by the planner. Go's fresh and
/// cached plans both enter `executorBuilder.build`; this is that common seam.
pub fn run_select_meta_stmt_with_physical(
    select: &tidb_ast::SelectStmt,
    physical: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_query_meta_stmt_with_physical(
        &QueryStmt::Select(Box::new(select.clone())),
        physical,
        catalog,
        current_db,
        ctx,
    )
}

/// Runs one parsed query through the ordinary physical executor builder,
/// optionally consuming a cache-rebuilt tree selected by the planner.
pub fn run_query_meta_stmt_with_physical(
    query: &QueryStmt,
    physical: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    open_query_meta_stmt_with_physical(query, physical, catalog, current_db, ctx)?.collect()
}

/// Opens a fresh or cache-rebuilt query without draining its result. The
/// statement owner retains its snapshot until this record set finishes.
pub fn open_query_meta_stmt_with_physical(
    query: &QueryStmt,
    physical: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<RecordSet, DriverError> {
    validate_query_sequence_names(query, ctx)?;
    match physical {
        Some(physical) => physical_builder::open_query(query, physical, catalog, ctx),
        None => {
            set_opr::validate_query_usage(query, ctx)?;
            let optimize = start_optimize_region();
            let mut physical = optimize_query_stmt(query, catalog, current_db, ctx)?;
            optimize.end();
            physical_builder::open_query(query, &mut physical, catalog, ctx)
        }
    }
}

/// Opens a prepared query whose sequence expressions were checked when the
/// cache plan was created. Non-sequence prepared statements skip the AST copy
/// and visitor walk on every cache hit.
pub fn open_query_meta_stmt_with_physical_without_sequence_validation(
    query: &QueryStmt,
    physical: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<RecordSet, DriverError> {
    match physical {
        Some(physical) => physical_builder::open_query(query, physical, catalog, ctx),
        None => open_query_meta_stmt_with_physical(query, None, catalog, current_db, ctx),
    }
}

/// Go `Compile` for a `ShowStmt`: the preprocessor
/// ([`tidb_planner::preprocess::resolve_show_stmt`], `CURRENT_USER` as
/// `auth_user`) and `planner.Optimize`.
///
/// # Errors
///
/// The preprocessor's `ErrNoDB`, `buildShow`'s errors and any optimization
/// failure.
pub fn plan_show_stmt(
    show: &tidb_ast::ShowStmt,
    auth_user: Option<(&str, &str)>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<
    (
        tidb_planner::common_plans::Plan,
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
    ),
    DriverError,
> {
    let mut show = show.clone();
    tidb_planner::preprocess::resolve_show_stmt(&mut show, current_db, auth_user)
        .map_err(planner_error_to_driver)?;
    planner_bridge::physical_show_plan(&show, catalog, current_db, ctx)
        .map_err(planner_error_to_driver)
}

/// Go `Compile`'s build for a statement that is not a query or DML:
/// `PlanBuilder.Build` over this catalog, answering Go's common plan and the
/// visit infos `CheckPrivilege` reads.
///
/// # Errors
///
/// The builder's own refusals.
pub fn plan_statement(
    stmt: &tidb_ast::Stmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    session_ctx: &dyn tidb_planner::plan_builder::stmt_build::SessionPlanContext,
) -> Result<
    (
        tidb_planner::common_plans::Plan,
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
    ),
    DriverError,
> {
    planner_bridge::statement_plan(stmt, catalog, current_db, ctx, session_ctx)
        .map_err(planner_error_to_driver)
}

/// Go `Compiler.Compile` of the SELECT of an import query: its physical
/// plan over this catalog.
///
/// # Errors
///
/// The builder's or optimizer's own refusals.
pub fn plan_import_query(
    query: &tidb_ast::QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    session_ctx: &dyn tidb_planner::plan_builder::stmt_build::SessionPlanContext,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    planner_bridge::import_query_plan(query, catalog, current_db, ctx, session_ctx).map_err(planner_error_to_driver)
}

/// Go `Compile` for `SET`: `buildSet` over this catalog, with the
/// statement's visit infos (Go `CheckPrivilege` reads them before the
/// executor runs). `set` is the statement with its system variables already
/// read, as Go's `rewriteSystemVariable` reads them while building.
///
/// # Errors
///
/// A value's rewrite or subquery-evaluation error.
pub fn plan_set_stmt(
    set: &tidb_ast::SetStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<
    (
        tidb_planner::common_plans::Set,
        Vec<tidb_planner::plan_builder::visit_info::VisitInfo>,
    ),
    DriverError,
> {
    planner_bridge::set_plan(set, catalog, current_db, ctx).map_err(planner_error_to_driver)
}

/// The `PhysicalShow` a planned SHOW reads: the node whose rows the session
/// fetches (Go `ShowExec.fetchAll`) and supplies through
/// [`crate::StmtContext::show_retrieval`] before [`open_show_plan`].
#[must_use]
pub fn physical_show_of(
    plan: &tidb_planner_core_operator_physicalop::PhysicalPlan,
) -> Option<&tidb_planner_core_operator_physicalop::PhysicalShow> {
    match plan {
        tidb_planner_core_operator_physicalop::PhysicalPlan::Show(show) => Some(show),
        other => other.children().iter().find_map(physical_show_of),
    }
}

/// Go `Compile` for `ADMIN SHOW DDL JOBS [n] [WHERE ...]`.
///
/// # Errors
///
/// The WHERE's build error and any optimization failure.
pub fn plan_admin_show_ddl_jobs(
    job_number: i64,
    where_clause: Option<&tidb_ast::Expr>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    planner_bridge::physical_admin_show_ddl_jobs_plan(
        job_number,
        where_clause,
        catalog,
        current_db,
        ctx,
    )
    .map_err(planner_error_to_driver)
}

/// The `PhysicalShowDDLJobs` a planned ADMIN SHOW DDL JOBS reads, whose rows
/// the session supplies through [`crate::StmtContext::show_retrieval`].
#[must_use]
pub fn physical_show_ddl_jobs_of(
    plan: &tidb_planner_core_operator_physicalop::PhysicalPlan,
) -> Option<&tidb_planner_core_operator_physicalop::PhysicalShowDDLJobs> {
    match plan {
        tidb_planner_core_operator_physicalop::PhysicalPlan::ShowDDLJobs(show) => Some(show),
        other => other.children().iter().find_map(physical_show_ddl_jobs_of),
    }
}

/// Go `executorBuilder.build` + `ExecStmt.Exec` for a planned SHOW whose
/// rows the session supplied.
///
/// # Errors
///
/// A build failure, including a `PhysicalShow` with no supplied rows.
pub fn open_show_plan(
    physical: &mut tidb_planner_core_operator_physicalop::PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<RecordSet, DriverError> {
    physical_builder::open_show(physical, catalog, ctx)
}

/// Builds the ordinary physical query tree without opening an executor.
///
/// Information-schema execution uses this to inspect the resolved, pruned
/// `PhysicalMemTable.Columns` before Go's retriever decides whether its
/// process-wide table-size cache needs a fresh restricted read.
pub fn plan_query_meta_stmt(
    query: &QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    validate_query_sequence_names(query, ctx)?;
    planner_bridge::physical_query_plan(query, catalog, current_db, ctx)
        .map_err(planner_error_to_driver)
}

/// [`plan_query_meta_stmt`], also handing back the optimized child of every
/// uncorrelated subquery the build evaluated: Go's `EvalSubqueryFirstRow`
/// runs each one, memory-table retrievers included, while the statement is
/// still being planned.
pub fn plan_query_meta_stmt_with_subqueries(
    query: &QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<
    (
        tidb_planner_core_operator_physicalop::PhysicalPlan,
        Vec<tidb_planner_core_operator_physicalop::PhysicalPlan>,
    ),
    DriverError,
> {
    validate_query_sequence_names(query, ctx)?;
    planner_bridge::physical_query_plan_with_scalar_subqueries(
        query,
        catalog,
        current_db,
        ctx,
        planner_bridge::BuildVisits::Discard,
    )
    .map(|(physical, registered)| {
        let subqueries = registered
            .into_iter()
            .map(|subquery| subquery.physical.clone())
            .collect();
        (physical, subqueries)
    })
    .map_err(planner_error_to_driver)
}

/// Plans one parsed `SELECT` and returns only its result-column metadata.
///
/// The ordinary planner still builds the exact executor pipeline, including
/// access-path selection and output type derivation, but a plan-only trace
/// stops before the pipeline is opened or drained.
pub fn plan_query_result_columns(
    query: &QueryStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<PlannedResultColumns, DriverError> {
    let physical = plan_query_meta_stmt(query, catalog, current_db, ctx)?;
    physical_builder::planned_result_columns(query, &physical)
}

/// A planned query's result columns and the plan's output names (Go
/// `p.Schema()` and `p.OutputNames()`), which `colNames2ResultFields` turns
/// into the result fields.
pub type PlannedResultColumns = (Vec<(String, FieldType)>, Vec<tidb_datatype::FieldNameMetadata>);

/// Go turns a subquery's result `Datum` into an `expression.Constant`; the
/// same value has to travel back through the AST here, so it becomes the
/// literal that parses to it.
/// A byte string as a literal expression: readable text stays a string, and
/// anything that is not UTF-8 becomes a hex literal so no byte is lost.
pub(crate) fn bytes_to_literal(bytes: &[u8]) -> tidb_ast::Expr {
    match std::str::from_utf8(bytes) {
        Ok(text) => tidb_ast::Expr::String(text.to_owned()),
        Err(_) => tidb_ast::Expr::Hex(hex_digits(bytes)),
    }
}

/// The lowercase, even-length hex digits an `Expr::Hex` carries.
fn hex_digits(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn datum_to_literal(value: &Datum) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match value {
        Datum::Null => Expr::Null,
        Datum::Int(v) => {
            // A negative literal is a unary minus over a positive one, which
            // is how the parser itself represents it.
            if *v < 0 {
                Expr::Unary(
                    tidb_ast::UnaryOp::Minus,
                    Box::new(Expr::Int(v.unsigned_abs().to_string())),
                )
            } else {
                Expr::Int(v.to_string())
            }
        }
        Datum::UInt(v) => Expr::Int(v.to_string()),
        Datum::Real(v) => Expr::Float(*v),
        Datum::Decimal(d) => Expr::Decimal(d.to_string()),
        // A byte string that is not UTF-8 becomes a hex literal, which is
        // lossless where a lossy string conversion would corrupt it.
        Datum::String(s) => bytes_to_literal(s.bytes()),
        Datum::Bytes(b) => bytes_to_literal(b),
        Datum::BinaryLiteral(literal) | Datum::Bit(literal) => {
            Expr::Hex(hex_digits(literal.as_bytes()))
        }
        _ => {
            return Err(DriverError::unsupported(
                "this subquery result kind is not supported yet",
            ))
        }
    })
}

pub(crate) use tidb_planner::from_scope::FromTable;

/// [`visit_executor_rows`] for a write executor's source.
///
/// With `dml`, the reading is that write executor's: its `Open` and `Close`
/// open and close the source, and the caller times the read as (part of)
/// its `Next` ([`physical_builder::DmlExec::next`]). `charge_chunks` is Go
/// `DeleteExec.deleteMultiTablesByChunk`'s read: the child chunk is charged
/// to `dml`'s tracker while it is read. (Go takes a new chunk per call,
/// `exec.TryNewCacheChunk`, because its rows alias the chunk; these own
/// their values.)
fn visit_executor_row_chunks(
    mut exec: Box<dyn Executor>,
    types: &[FieldType],
    memory: &crate::StatementMemory,
    dml: Option<&physical_builder::DmlExec>,
    charge_chunks: bool,
    mut visit: impl FnMut(Vec<Datum>) -> Result<(), DriverError>,
) -> Result<(), DriverError> {
    let tracker = dml.filter(|_| charge_chunks).map(|dml| &dml.mem_tracker);
    let timed = |phase: fn(&physical_builder::DmlExec, &mut dyn FnMut() -> Result<(), DriverError>) -> Result<(), DriverError>,
                 run: &mut dyn FnMut() -> Result<(), DriverError>| match dml {
        Some(dml) => phase(dml, run),
        None => run(),
    };
    let result = (|| {
        timed(|dml, run| dml.open(run), &mut || exec.open().map_err(DriverError::from))?;
        let mut req = exec.new_chunk();
        let mut mem_usage_of_chk = 0;
        loop {
            if let Some(tracker) = tracker {
                tracker.consume_checked(-mem_usage_of_chk)?;
            }
            next_executor(exec.as_mut(), &mut req, memory)?;
            if req.num_rows() == 0 {
                break;
            }
            if let Some(tracker) = tracker {
                mem_usage_of_chk = req.memory_usage();
                tracker.consume_checked(mem_usage_of_chk)?;
            }
            for r in 0..req.num_rows() {
                visit(req.get_row(r).get_datum_row(types))?;
            }
        }
        Ok(())
    })();
    // Close even after partial open, next, conversion or quota failure.
    // Preserve the primary read/write error if cleanup also fails.
    let close = timed(|dml, run| dml.close(run), &mut || exec.close().map_err(DriverError::from));
    result.and(close)
}

/// [`visit_executor_row_chunks`] for a DML executor that writes each row as
/// it is delivered, as Go's `UpdateExec.updateRows` and `DeleteExec` do:
/// `state` is handed back to `visit`, so the writer can borrow what the read
/// side only borrowed while the source executor was built.
///
/// The source is asked for its next chunk only after every row of the
/// previous one was written, which is what puts the source's own end-of-input
/// work (`SelectLockExec`'s lock) after the last chunk's writes.
///
/// `dml` is the write executor: its `Open` and `Close` open and close the
/// source, and its one `Next` is this loop, which holds one source chunk at a
/// time on the executor's `memTracker`. Go renews that chunk after each one
/// (`chunk.Renew`) because its datum rows alias the chunk; these rows own
/// their values, so the chunk is reused.
fn visit_executor_rows_writing<C: ?Sized>(
    mut exec: Box<dyn Executor>,
    types: &[FieldType],
    memory: &crate::StatementMemory,
    dml: &physical_builder::DmlExec,
    state: &mut C,
    mut visit: impl FnMut(&mut C, Vec<Datum>) -> Result<(), DriverError>,
) -> Result<(), DriverError> {
    let result = (|| {
        dml.open(|| exec.open())?;
        dml.next(|| {
            let max_chunk_size = dml.max_chunk_size;
            let mut req =
                new_dml_child_chunk(dml.alloc_pool.as_ref(), types, max_chunk_size, exec.init_cap());
            let mut mem_usage_of_chk = 0;
            loop {
                dml.mem_tracker.consume_checked(-mem_usage_of_chk)?;
                next_executor(exec.as_mut(), &mut req, memory)?;
                if req.num_rows() == 0 {
                    break;
                }
                mem_usage_of_chk = req.memory_usage();
                dml.mem_tracker.consume_checked(mem_usage_of_chk)?;
                for r in 0..req.num_rows() {
                    visit(state, req.get_row(r).get_datum_row(types))?;
                }
            }
            Ok(())
        })
    })();
    // Close even after partial open, next, conversion or write failure.
    // Preserve the primary read/write error if cleanup also fails.
    let close = dml.close(|| exec.close()).map_err(DriverError::from);
    result.and(close)
}

/// Go `insertRowsFromSelect`'s read of `selectExec`: the source executor,
/// read one chunk at a time into the one chunk it keeps
/// (`exec.TryNewCacheChunk(selectExec)`), so the write executor writes each
/// chunk's rows before it reads the next. Its `Open` and `Close` are timed
/// as `dml`'s; dropping it closes the source.
pub(crate) struct DmlSourceCursor<'a> {
    exec: Box<dyn Executor>,
    chunk: tidb_chunk::chunk::Chunk,
    types: Vec<FieldType>,
    memory: crate::StatementMemory,
    dml: Option<&'a physical_builder::DmlExec>,
    closed: bool,
}

impl<'a> DmlSourceCursor<'a> {
    /// Opens `exec` (Go `exec.Open(ctx, e.SelectExec)` in the write
    /// executor's `Open`), then takes the chunk the write executor's `Next`
    /// reads it into from the statement's allocator
    /// (`exec.TryNewCacheChunk(selectExec)`).
    pub(crate) fn open(
        exec: Box<dyn Executor>,
        memory: crate::StatementMemory,
        dml: Option<&'a physical_builder::DmlExec>,
        alloc_pool: Option<std::sync::Arc<dyn tidb_chunk::alloc::Allocator>>,
    ) -> Result<Self, DriverError> {
        let types = exec.ret_field_types().to_vec();
        let mut cursor = Self {
            chunk: tidb_chunk::chunk::Chunk::new(&types, 0, 0),
            types,
            exec,
            memory,
            dml,
            closed: false,
        };
        let opened = match cursor.dml {
            Some(dml) => dml.open(|| cursor.exec.open()),
            None => cursor.exec.open(),
        };
        opened?;
        cursor.chunk = tidb_executor_internal_exec::new_chunk_with_alloc_pool(
            alloc_pool.as_ref(),
            &cursor.types,
            cursor.exec.init_cap(),
            cursor.exec.max_chunk_size(),
        );
        Ok(cursor)
    }

    /// The next chunk's rows and the chunk's `MemoryUsage`, or `None` once
    /// the source is drained.
    pub(crate) fn next_chunk(&mut self) -> Result<Option<(Vec<Vec<Datum>>, i64)>, DriverError> {
        next_executor(self.exec.as_mut(), &mut self.chunk, &self.memory)?;
        if self.chunk.num_rows() == 0 {
            return Ok(None);
        }
        let rows = (0..self.chunk.num_rows())
            .map(|row| self.chunk.get_row(row).get_datum_row(&self.types))
            .collect();
        Ok(Some((rows, self.chunk.memory_usage())))
    }

    /// The source's result field types.
    pub(crate) fn field_types(&self) -> &[FieldType] {
        &self.types
    }

    /// Go `exec.Close(e.SelectExec)` in the write executor's `Close`.
    pub(crate) fn close(&mut self) -> Result<(), DriverError> {
        if std::mem::replace(&mut self.closed, true) {
            return Ok(());
        }
        let exec = &mut self.exec;
        match self.dml {
            Some(dml) => dml.close(|| exec.close()),
            None => exec.close(),
        }
        .map_err(DriverError::from)
    }
}

impl Drop for DmlSourceCursor<'_> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Go `ImportIntoExec`'s `selectExec`, built from the planned SELECT of an
/// `IMPORT INTO ... FROM SELECT` and read chunk by chunk
/// (`sendQueryChunks`). Dropping it closes the executor.
pub struct ImportSelectSource {
    cursor: DmlSourceCursor<'static>,
}

impl ImportSelectSource {
    /// Builds and opens the executor of `physical` (Go
    /// `executorBuilder.build(plan.SelectPlan)` and `exec.Open`).
    ///
    /// # Errors
    /// The executor could not be built or opened.
    pub fn open(
        physical: &mut tidb_planner_core_operator_physicalop::PhysicalPlan,
        catalog: &SessionExtendedInfoSchema,
        ctx: &crate::StmtContext,
    ) -> Result<Self, DriverError> {
        let exec = physical_builder::build_dml_source(physical, catalog, ctx)?;
        let cursor = DmlSourceCursor::open(exec, ctx.statement_memory(), None, ctx.chunk_allocator())?;
        Ok(Self { cursor })
    }

    /// Go `exec.Next`: the next chunk's rows, or `None` once drained.
    ///
    /// # Errors
    /// The executor failed.
    pub fn next_chunk(&mut self) -> Result<Option<Vec<Vec<Datum>>>, DriverError> {
        Ok(self.cursor.next_chunk()?.map(|(rows, _)| rows))
    }
}

/// Go `dmlChildChunkTargetBytes`: the bytes a write executor's child chunk is
/// sized to start at.
const DML_CHILD_CHUNK_TARGET_BYTES: i64 = 256 * 1024;

/// Go `newDMLChildChunk(dmlExec, fields, maxInitCap)`: the chunk a write
/// executor reads its child into, starting no larger than the child's
/// `InitCap` nor than [`DML_CHILD_CHUNK_TARGET_BYTES`] of estimated rows.
fn new_dml_child_chunk(
    alloc_pool: Option<&std::sync::Arc<dyn tidb_chunk::alloc::Allocator>>,
    fields: &[FieldType],
    max_chunk_size: usize,
    max_init_cap: usize,
) -> tidb_chunk::chunk::Chunk {
    let init_cap = estimate_dml_child_chunk_init_cap(fields, max_chunk_size, max_init_cap);
    // Go `dmlExec.NewChunkWithCapacity`: the DML executor's `AllocPool`.
    tidb_executor_internal_exec::new_chunk_with_alloc_pool(alloc_pool, fields, init_cap, max_chunk_size)
}

/// Go `estimateDMLChildChunkInitCap`.
fn estimate_dml_child_chunk_init_cap(fields: &[FieldType], max_chunk_size: usize, max_init_cap: usize) -> usize {
    if max_chunk_size == 0 || max_init_cap == 0 {
        // Go `chunk.ZeroCapacity`.
        return 0;
    }
    let row_width: i64 = fields.iter().map(tidb_chunk::codec::estimate_type_width).sum();
    if row_width <= 0 {
        return max_chunk_size.min(max_init_cap);
    }
    let by_width = usize::try_from(DML_CHILD_CHUNK_TARGET_BYTES / row_width).unwrap_or(0);
    max_chunk_size.min(max_init_cap).min(by_width).max(1)
}

#[cfg(test)]
mod dml_batch_tests {
    use super::*;
    use tidb_executor_internal_exec::ExecutorMeta;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use tidb_expr::schema::Schema;
    struct Source {
        meta: ExecutorMeta,
        nexts: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        fail_next: bool,
    }
    impl Executor for Source {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut tidb_chunk::chunk::Chunk) -> Result<(), ExecError> {
            req.reset();
            let n = self.nexts.fetch_add(1, SeqCst);
            if self.fail_next {
                return Err(ExecError::internal("read failed"));
            }
            if n < 100 {
                req.append_bytes(0, &[b'x'; 4096]);
            }
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
            self.closes.fetch_add(1, SeqCst);
            Ok(())
        }
        fn schema(&self) -> &Schema {
            self.meta.schema()
        }
        fn ret_field_types(&self) -> &[FieldType] {
            self.meta.ret_field_types()
        }
        fn init_cap(&self) -> usize {
            1
        }
        fn max_chunk_size(&self) -> usize {
            1
        }
        fn new_chunk(&self) -> tidb_chunk::chunk::Chunk {
            self.meta.new_chunk()
        }
    }
    fn source(
        fail_next: bool,
    ) -> (
        Box<dyn Executor>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Vec<FieldType>,
    ) {
        let ft = FieldType::new(FieldTypeCode::VarString);
        let nexts = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let exec = Source {
            meta: ExecutorMeta::new(Schema::new(vec![Column::new(1, ft.clone())]), 1, 1, 1),
            nexts: nexts.clone(),
            closes: closes.clone(),
            fail_next,
        };
        (Box::new(exec), nexts, closes, vec![ft])
    }
}

fn next_executor(
    exec: &mut dyn Executor,
    req: &mut tidb_chunk::chunk::Chunk,
    memory: &crate::StatementMemory,
) -> Result<(), DriverError> {
    memory.check()?;
    exec.next(req)?;
    memory.check()?;
    Ok(())
}


#[cfg(test)]
mod limit_bound_tests {
    use super::eval_limit_bound;
    use tidb_ast::Expr;
    use tidb_datatype::Datum;

    #[test]
    fn accepts_execute_time_integer_parameter() {
        let marker = Expr::ParamMarker {
            offset: 0,
            order: 0,
            in_execute: true,
            value: Some(tidb_ast::ParamValue::new(Datum::Int(7))),
            projection_offset: 0,
        };
        assert_eq!(eval_limit_bound(&marker).unwrap(), 7);
    }

    #[test]
    fn rejects_unbound_or_negative_parameter() {
        let unbound = Expr::ParamMarker {
            offset: 0,
            order: 0,
            in_execute: false,
            value: None,
            projection_offset: 0,
        };
        assert!(eval_limit_bound(&unbound).is_err());
        let negative = Expr::ParamMarker {
            offset: 0,
            order: 0,
            in_execute: true,
            value: Some(tidb_ast::ParamValue::new(Datum::Int(-1))),
            projection_offset: 0,
        };
        assert!(eval_limit_bound(&negative).is_err());
    }
}

#[cfg(test)]
mod field_name_tests {
    use tidb_planner::plan_builder::field_name::strip_spec_field_comment_markers;

    #[test]
    fn strip_spec_field_comment_markers_matches_go_spec_field_pattern() {
        assert_eq!(
            strip_spec_field_comment_markers("(select /*+ INL_JOIN(x2) */ x2.a from t)"),
            "(select /*+ INL_JOIN(x2)  x2.a from t)"
        );
        assert_eq!(
            strip_spec_field_comment_markers("a /*!40101 + 1 */"),
            "a  + 1 "
        );
        assert_eq!(strip_spec_field_comment_markers("/*! x */"), " x ");
        assert_eq!(strip_spec_field_comment_markers("列 + 1"), "列 + 1");
    }
}
