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

//! Go `pkg/planner/optimize.go`'s statement planning over a query:
//! `NewPlanBuilder().Init` ([`BuilderParts::build`]), `buildLogicalPlan`
//! ([`build_logical_plan`]), `buildAndOptimizeLogicalPlanRound`
//! ([`build_and_optimize_logical_plan_round`]) and `optimize`'s rounds
//! ([`optimize_query`]). What Go's executor hands the planner through
//! function variables -- evaluating an uncorrelated subquery, a column's
//! default, the statistics `DoOptimize` loads, `CheckPrivilege` -- is
//! [`PlanningHooks`].

use std::cell::RefCell;
use std::rc::Rc;

use tidb_expr::expression::Expression;
use tidb_planner_core_base::{ColumnIdAllocator, PlanError, PlanIdAllocator};
use tidb_planner_core_operator_logicalop::LogicalPlan;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_sessionctx::StmtContext;

use crate::catalog_ctx::PlannerCatalogContext as _;
use crate::plan_builder::visit_info::VisitInfo;
use crate::plan_builder::{EvaluatedSubquery, PlanBuilder, SubqueryKind};
use crate::stmt_ctx::PlannerStmtCtx as _;

/// What Go's executor gives the planner for one statement's planning.
pub trait PlanningHooks {
    /// Go's `EvalSubqueryFirstRow` function variable, which the executor
    /// sets: run a subquery's physical plan for its first row.
    fn eval_subquery_first_row(&self) -> EvalSubqueryFirstRow;
    /// The executor's preparation of a built DML plan before it runs or is
    /// explained (the virtual tables it reads requested for retrieval).
    ///
    /// # Errors
    ///
    /// The preparation's failure.
    fn prepare_plan(
        &self,
        plan: &mut PhysicalPlan,
        ctx: &StmtContext,
    ) -> Result<(), tidb_driver_error::DriverError>;
    /// Go `evalFieldDefaultValue`: `DEFAULT(column)` as a constant.
    ///
    /// # Errors
    ///
    /// The default's failure (1364 for a column with none).
    fn field_default(
        &self,
        ctx: &StmtContext,
        database: &str,
        table: &str,
        column: &str,
    ) -> Result<Expression, tidb_exprctx::EvalError>;
    /// Go `getDefaultValueForInsert`: an INSERT's own `DEFAULT`.
    ///
    /// # Errors
    ///
    /// The default's failure.
    fn insert_default(
        &self,
        ctx: &StmtContext,
        database: &str,
        table: &str,
        column: &str,
    ) -> Result<Expression, tidb_exprctx::EvalError>;
}

/// What Go `NewPlanBuilder().Init(sctx, is, ...)` configures a builder from.
#[derive(Clone, Copy)]
pub struct BuilderParts<'a> {
    /// The statement's infoschema.
    pub catalog: &'a tidb_infoschema::SessionExtendedInfoSchema,
    /// Go `SessionVars.CurrentDB`.
    pub current_database: &'a str,
    /// The statement's context (a round's, under an alternative round).
    pub ctx: &'a StmtContext,
    /// Whether the plan may be cached.
    pub use_plan_cache: bool,
    /// The statement's plan ids.
    pub plan_ids: &'a PlanIdAllocator,
    /// The statement's column ids.
    pub column_ids: &'a ColumnIdAllocator,
    /// Go `PlanContext`'s session-side reach for the statement builders.
    pub session_ctx: Option<&'a dyn crate::plan_builder::stmt_build::SessionPlanContext>,
    /// The statement's registered scalar subqueries.
    pub registry: &'a ScalarSubqueryRegistry,
    /// The executor's half.
    pub hooks: &'a dyn PlanningHooks,
    /// Called when an UPDATE/DELETE build allocates its root (see
    /// [`PlanBuilder::on_delete_root`]).
    pub on_delete_root: Option<&'a dyn Fn()>,
}

impl BuilderParts<'_> {
    /// Go `NewPlanBuilder().Init(sctx, is, ...)` for a query: a builder over
    /// this catalog with the session's planner variables, handed to `build`.
    /// `query` is the statement the build is for, when there is one, which
    /// decides static pruning's global-index exception.
    ///
    /// # Errors
    ///
    /// `build`'s.
    pub fn build<R>(
        &self,
        query: Option<&tidb_ast::QueryStmt>,
        building_view: Option<(&str, &str)>,
        build: impl FnOnce(
            &mut PlanBuilder<'_, tidb_infoschema::PlannerCatalog, StmtContext>,
        ) -> Result<R, PlanError>,
    ) -> Result<R, PlanError> {
        let (catalog, current_database, ctx) = (self.catalog, self.current_database, self.ctx);
        let source = catalog
            .planner_catalog(current_database, ctx.latest_index_schema())
            .with_statement_hypo_state(ctx)
            .with_statement_dirty_tables(ctx)
            .with_statement_privilege_verifier(ctx)
            .with_statement_related_tables_for_mdl(ctx);
        let session_zone = ctx.session_zone();
        let hooks = self.hooks;
        let evaluator = |inner: &LogicalPlan, kind: SubqueryKind, opt_flag: u64, evaluate: bool| {
            evaluate_subquery(self, inner, kind, opt_flag, evaluate)
        };
        let prune_fallback = |table_id: i64, table_name: &str| {
            dynamic_prune_fallback(catalog, ctx, table_id, table_name)
        };
        let defaults = |database: &str, table: &str, column: &str| {
            hooks.field_default(ctx, database, table, column)
        };
        let insert_defaults = |database: &str, table: &str, column: &str| {
            hooks.insert_default(ctx, database, table, column)
        };
        let mut builder =
            PlanBuilder::new(&source, ctx, self.plan_ids, self.column_ids, session_zone)
                .with_subquery_evaluator(&evaluator)
                .with_default_evaluator(&defaults)
                .with_insert_default_evaluator(&insert_defaults)
                .with_mpp_warning_sink(ctx);
        if let Some(session_ctx) = self.session_ctx {
            builder = builder.with_session_ctx(session_ctx);
        }
        // Go `PlanBuilder.Init(sctx, ...)`: the session variables the build reads.
        builder.init_session_vars();
        builder.set_partition_processor_enabled(
            ctx.static_partition_prune()
                && !query.is_some_and(|query| {
                    crate::point_get_plan::static_global_index_point_lookup(query, &source, ctx)
                }),
        );
        builder.set_dynamic_prune_fallback(&prune_fallback);
        builder.on_delete_root = self.on_delete_root;
        // Go `PlanBuilder.Build`: `b.optFlag |= FlagPruneColumns` for every
        // statement, which each evaluated subquery's `DoOptimize` reads.
        builder.add_opt_flag(tidb_planner_core_rule::flags::PRUNE_COLUMNS);
        if let Some((database, view)) = building_view {
            builder.building_view_stack.insert((
                tidb_mysql::to_lowercase(database),
                tidb_mysql::to_lowercase(view),
            ));
        }
        build(&mut builder)
    }
}

/// Go `buildDataSource`'s dynamic-mode check for a partitioned table
/// (`logical_plan_builder.go:4997`): a table whose global statistics were
/// never analyzed (`Table.IsAnalyzed`) turns this statement's pruning static
/// -- `UseDynamicPruneMode = false` and the 1105 warning -- unless missing
/// partition statistics may be skipped (`tidb_skip_missing_partition_stats`,
/// which fix control 44262 overrides).
pub fn dynamic_prune_fallback(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    ctx: &StmtContext,
    table_id: i64,
    table_name: &str,
) -> bool {
    let global_stats_ready = crate::stats::stats_table::table_statistics(ctx, catalog, table_id)
        .is_some_and(|statistics| statistics.last_analyze_version > 0);
    let allow_dynamic_without_stats = ctx.optimizer_fix_control().get_bool_with_default(
        tidb_fixcontrol::FIX_44262,
        ctx.skip_missing_partition_stats(),
    );
    let mut use_partition_processor = !global_stats_ready && !allow_dynamic_without_stats;
    tidb_failpoint::inject(
        "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune",
        |val| {
            if val.bool() {
                use_partition_processor = false;
            }
        },
    );
    if !use_partition_processor {
        return false;
    }
    ctx.disable_dynamic_prune();
    tidb_exprctx::Columns::append_warning(
        ctx,
        1105,
        &format!("disable dynamic pruning due to {table_name} has no global stats"),
    );
    true
}

/// Go `handleScalarSubquery` / `handleExistSubquery` over an uncorrelated
/// subquery: `DoOptimize` the child, allocate its output column ids,
/// register it for EXPLAIN, then (`evaluate`) run it through
/// `EvalSubqueryFirstRow` and report the first row.
fn evaluate_subquery(
    parts: &BuilderParts<'_>,
    inner: &LogicalPlan,
    kind: SubqueryKind,
    opt_flag: u64,
    evaluate: bool,
) -> Result<EvaluatedSubquery, PlanError> {
    let (catalog, ctx, plan_ids, column_ids) =
        (parts.catalog, parts.ctx, parts.plan_ids, parts.column_ids);
    let run = parts.hooks.eval_subquery_first_row();
    // Go `DoOptimize(ctx, planCtx.builder.ctx, planCtx.builder.optFlag, np)`.
    let logical = crate::do_optimize::optimize_built_logical(
        inner.clone(),
        opt_flag,
        catalog,
        ctx,
        parts.use_plan_cache,
        plan_ids,
        column_ids,
        &ctx.session_zone(),
    )?;
    let mut physical = crate::do_optimize::physical_plan_for_logical(
        &logical, plan_ids, column_ids, catalog, ctx,
    )?;
    // Go allocates `outputColIDs` from `np.Schema()` before it runs the
    // child, so the ids precede any execution-time allocation.
    let width = subquery_output_width(
        logical.schema().map_or(0, |schema| schema.len()),
        kind,
        evaluate,
    );
    let output_col_ids = (0..width).map(|_| column_ids.alloc()).collect::<Vec<_>>();
    let plan_id = plan_ids.alloc();
    let register = |physical: PhysicalPlan| {
        let registered = std::sync::Arc::new(RegisteredScalarSubquery {
            output_col_ids: output_col_ids.clone(),
            physical,
            plan_id,
            catalog: catalog.clone(),
            ctx: ctx.clone(),
            run,
            evaluated: std::sync::OnceLock::new(),
        });
        parts
            .registry
            .borrow_mut()
            .push(std::sync::Arc::clone(&registered));
        registered
    };
    if !evaluate {
        // Go's non-eval branch registers the child and stops there.
        return Ok(EvaluatedSubquery {
            column_ids: output_col_ids.clone(),
            row: Vec::new(),
            has_row: false,
            eval_ctx: register(physical),
        });
    }
    // Go executor.EvalSubqueryFirstRow prevents evaluated constants from
    // surviving into a later EXECUTE with a different snapshot.
    if parts.use_plan_cache {
        ctx.set_skip_plan_cache("query has uncorrelated sub-queries is un-cacheable");
    }
    // Go `EvalSubqueryFirstRow`'s deferred `PreprocessSubQueries++` and
    // `DurationPreprocessSubQuery += time.Since(begin)`, error or not.
    let begin_evaluation = std::time::Instant::now();
    // Go `EvalSubqueryFirstRow`'s region: the subquery's executors run
    // under it.
    let region = tidb_tracing::start_thread_region_ex("executor.EvalSubQuery");
    let evaluated = run(&mut physical, catalog, ctx);
    region.end();
    ctx.phase_durations()
        .add_preprocess_sub_query(begin_evaluation.elapsed());
    let (row, has_row) = evaluated.map_err(driver_error_to_plan)?;
    let eval_ctx = register(physical);
    Ok(EvaluatedSubquery {
        column_ids: output_col_ids,
        row,
        has_row,
        eval_ctx,
    })
}

/// Go's `EvalSubqueryFirstRow` function variable
/// (`pkg/planner/core/expression_rewriter.go:55`), which the executor sets.
pub type EvalSubqueryFirstRow =
    fn(
        &mut PhysicalPlan,
        &tidb_infoschema::SessionExtendedInfoSchema,
        &StmtContext,
    ) -> Result<(Vec<tidb_datatype::Datum>, bool), tidb_driver_error::DriverError>;

/// One uncorrelated subquery Go registers in the statement context while
/// `handleScalarSubquery` / `handleExistSubquery` evaluates it.
///
/// Go's `ScalarSubqueryEvalCtx` (`pkg/planner/core/scalar_subq_expression.go`)
/// keeps the OPTIMIZED child plan and the output column ids its placeholder
/// expressions carry; `FlatPhysicalPlan.flatten` appends the plan as an extra
/// EXPLAIN root whose `ExplainInfo` names those ids.
///
/// The non-eval `ScalarSubQueryExpr` placeholders hold the same object and
/// evaluate through it (Go `selfEval`), so it keeps the statement's catalog
/// and context the way Go's keeps `is` and `ctx`.
pub struct RegisteredScalarSubquery {
    /// Go `ScalarSubqueryEvalCtx.outputColIDs`.
    pub output_col_ids: Vec<i64>,
    /// Go `ScalarSubqueryEvalCtx.scalarSubQuery`, the optimized child.
    pub physical: PhysicalPlan,
    /// Go `ScalarSubqueryEvalCtx.ID()`: `Init` allocates it from the plan id
    /// counter once the child has been optimized, and EXPLAIN prints it.
    pub plan_id: i32,
    /// Go `ScalarSubqueryEvalCtx.is`.
    catalog: tidb_infoschema::SessionExtendedInfoSchema,
    /// Go `ScalarSubqueryEvalCtx.SCtx()`.
    ctx: StmtContext,
    /// Go's `EvalSubqueryFirstRow`, which `selfEval` calls.
    run: EvalSubqueryFirstRow,
    /// Go `evaled` / `evalErr` / `colsData`.
    evaluated: std::sync::OnceLock<Result<Vec<tidb_datatype::Datum>, tidb_exprctx::EvalError>>,
}

impl std::fmt::Debug for RegisteredScalarSubquery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredScalarSubquery")
            .field("output_col_ids", &self.output_col_ids)
            .field("plan_id", &self.plan_id)
            .finish_non_exhaustive()
    }
}

impl tidb_expr::expression::ScalarSubqueryEval for RegisteredScalarSubquery {
    /// Go `ScalarSubqueryEvalCtx.getColVal`, running `selfEval` once.
    fn get_col_val(&self, col_id: i64) -> Result<tidb_datatype::Datum, tidb_exprctx::EvalError> {
        let row = self
            .evaluated
            .get_or_init(|| {
                let mut physical = self.physical.clone();
                (self.run)(&mut physical, &self.catalog, &self.ctx)
                    .map(|(row, _)| row)
                    .map_err(driver_error_to_eval)
            })
            .as_ref()
            .map_err(Clone::clone)?;
        self.output_col_ids
            .iter()
            .position(|id| *id == col_id)
            .and_then(|index| row.get(index).cloned())
            .ok_or_else(|| {
                tidb_exprctx::EvalError::Conversion(tidb_terror::TerrorError::compatible(
                    tidb_terror::TerrorCode::new(1105),
                    format!(
                        "Could not found the ScalarSubQueryExpr#{col_id} in the ScalarSubquery_{}",
                        self.plan_id
                    ),
                ))
            })
    }
}

/// The per-statement list of [`RegisteredScalarSubquery`]s, Go's
/// `SessionVars.MapScalarSubQ` narrowed to one statement.
pub type ScalarSubqueryRegistry = Rc<RefCell<Vec<std::sync::Arc<RegisteredScalarSubquery>>>>;

/// The subquery hook of a build whose plan is discarded (the privilege
/// passes): nothing is run, and a placeholder reads NULL.
#[derive(Debug)]
pub struct DiscardedSubquery;

impl tidb_expr::expression::ScalarSubqueryEval for DiscardedSubquery {
    fn get_col_val(&self, _col_id: i64) -> Result<tidb_datatype::Datum, tidb_exprctx::EvalError> {
        Ok(tidb_datatype::Datum::Null)
    }
}

/// Go's output-id count for one subquery: a single id for a non-eval
/// `EXISTS` (`handleExistSubquery` reads only its first column), one per
/// output column otherwise.
#[must_use]
pub fn subquery_output_width(
    inner_width: usize,
    kind: crate::plan_builder::SubqueryKind,
    evaluate: bool,
) -> usize {
    if !evaluate && kind == crate::plan_builder::SubqueryKind::Exists {
        1
    } else {
        inner_width
    }
}

/// [`driver_error_to_plan`]'s counterpart for a placeholder evaluated as an
/// expression: Go's `getColVal` hands `EvalSubqueryFirstRow`'s error back as
/// it is.
#[must_use]
pub fn driver_error_to_eval(error: tidb_driver_error::DriverError) -> tidb_exprctx::EvalError {
    match error {
        tidb_driver_error::DriverError::Exec(tidb_driver_error::ExecError::Eval(eval)) => eval,
        other => {
            let error = other.to_mysql_error();
            tidb_exprctx::EvalError::Conversion(tidb_terror::TerrorError::compatible(
                tidb_terror::TerrorCode::new(error.code as isize),
                error.message,
            ))
        }
    }
}

/// Go `buildLogicalPlan` for a query: the logical plan, its optimizer
/// flags and its visit infos. `building_view` is a view's body built from
/// `BuildDataSourceFromView` with the view on its building stack.
///
/// # Errors
///
/// The build's failure.
pub fn build_logical_plan(
    parts: &BuilderParts<'_>,
    query: &tidb_ast::QueryStmt,
    building_view: Option<(&str, &str)>,
) -> Result<(LogicalPlan, u64, Vec<VisitInfo>), PlanError> {
    let ctx = parts.ctx;
    parts.build(Some(query), building_view, |builder| {
        let node = tidb_resolve::NodeW::new(query.clone());
        let plan = builder.build_query_node(&node, false)?;
        ctx.set_block_as_names(builder.statement_block_as_names());
        Ok((
            plan,
            builder.get_opt_flag(),
            std::mem::take(&mut builder.visit_info),
        ))
    })
}

/// Whose build a query's is.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum QueryBuild {
    /// Go `planner.optimize` of a statement that is a query, with the
    /// alternative rounds only a logical plan gets.
    Statement,
    /// The read of a DML, which Go builds inside the DML's `PlanBuilder`:
    /// the DML's plan is no logical plan, so its one round is the only one.
    DmlRead,
}

/// What one query build does with its `PlanBuilder`'s visit infos (Go
/// `builder.GetVisitInfo()`).
pub enum BuildVisits<'a> {
    /// The statement's own build: Go `planner.optimize` checks them
    /// (`CheckPrivilege`, `CheckTableLock`) between `buildLogicalPlan` and
    /// `DoOptimize`.
    Check,
    /// The read of a DML, which Go builds inside the same `PlanBuilder` as
    /// the write: its visits join the DML's, checked once the DML is built.
    Collect(&'a mut Vec<VisitInfo>),
    /// A build that is no statement's: a cached plan's (whose statement
    /// checks the visit infos PREPARE kept, Go `checkPreparedPriv`) or an
    /// inspection's.
    Discard,
}

/// The part of Go `buildAndOptimizeLogicalPlanRound` every query build runs,
/// a DML's read included: `buildLogicalPlan`, the statement's privilege
/// check (once: `checked`), and `core.DoOptimize` with the plan's cost.
///
/// # Errors
///
/// The build's, the check's or the optimization's failure.
pub fn build_and_optimize_logical_plan_round(
    parts: &BuilderParts<'_>,
    query: &tidb_ast::QueryStmt,
    visits: &mut BuildVisits<'_>,
    checked: &mut bool,
    opt_flag_adjust: fn(u64) -> u64,
) -> Result<(LogicalPlan, PhysicalPlan, f64), PlanError> {
    use std::sync::atomic::Ordering;
    let ctx = parts.ctx;
    // Go `buildLogicalPlan`: `RewritePhaseInfo.Reset()`, then
    // `DurationRewrite` is the time `builder.Build` took.
    ctx.phase_durations().reset_rewrite_phase_info();
    let begin_rewrite = std::time::Instant::now();
    let (plan, opt_flag, built_visits) = build_logical_plan(parts, query, None)?;
    ctx.phase_durations()
        .set_duration_rewrite(begin_rewrite.elapsed());
    // Go `buildAndOptimizeLogicalPlanRound`: `CheckPrivilege` and
    // `CheckTableLock` over `builder.GetVisitInfo()`, once for the statement,
    // after the build (whose errors, and whose evaluated subqueries', come
    // first) and before `DoOptimize` (whose errors come after).
    if !*checked {
        match visits {
            BuildVisits::Check => {
                crate::privilege_check::check_plan_privilege(ctx, parts.catalog, &built_visits)
                    .map_err(driver_error_to_plan)?
            }
            BuildVisits::Collect(collected) => collected.extend(built_visits),
            BuildVisits::Discard => {}
        }
        *checked = true;
    }
    // Go `buildAndOptimizeLogicalPlanRound`: the optimization clock starts
    // before the first `DoOptimize` of a logical plan.
    ctx.phase_durations().begin_optimize();
    let signals = ctx.alternative_logical_plan_signals();
    if ctx.enable_alternative_logical_plans()
        && opt_flag & tidb_planner_core_rule::flags::PUSH_DOWN_TOPN > 0
        && opt_flag & tidb_planner_core_rule::flags::JOIN_REORDER > 0
    {
        signals
            .order_aware_join_reorder
            .store(true, Ordering::Release);
    }
    // Go `core.DoOptimize`: `logicalOptimize`, then `physicalOptimize` and
    // `postOptimize`.
    let logical = crate::do_optimize::optimize_built_logical(
        plan,
        opt_flag_adjust(opt_flag),
        parts.catalog,
        ctx,
        parts.use_plan_cache,
        parts.plan_ids,
        parts.column_ids,
        &ctx.session_zone(),
    )?;
    let (physical, cost) = crate::optimize::physical_optimize(
        &logical,
        parts.plan_ids,
        parts.column_ids,
        ctx,
        &std::collections::BTreeSet::new(),
        &crate::do_optimize::StatementUsedStats {
            catalog: parts.catalog,
            context: ctx,
        },
    )?;
    Ok((logical, physical, cost))
}

/// One statement's planning rounds over its builder parts (Go
/// `buildAndOptimizeLogicalPlanRound`, which
/// [`crate::planner_optimize::optimize`] runs once per round).
struct QueryRounds<'q, 'v> {
    parts: &'q BuilderParts<'q>,
    query: &'q tidb_ast::QueryStmt,
    visits: BuildVisits<'v>,
    /// The statement's visit infos are checked once, after its first build.
    checked: bool,
    /// The plan id, column id and scalar-subquery count the first round
    /// started from.
    round_start: (i32, i64, usize),
}

impl crate::planner_optimize::LogicalPlanRounds for QueryRounds<'_, '_> {
    fn build_and_optimize(
        &mut self,
        ctx: &StmtContext,
        opt_flag_adjust: fn(u64) -> u64,
    ) -> Result<(LogicalPlan, PhysicalPlan, f64), PlanError> {
        // An alternative round plans under its own context.
        let parts = BuilderParts { ctx, ..*self.parts };
        build_and_optimize_logical_plan_round(
            &parts,
            self.query,
            &mut self.visits,
            &mut self.checked,
            opt_flag_adjust,
        )
    }

    fn save_round_start(&mut self) {
        self.round_start = (
            self.parts.plan_ids.current(),
            self.parts.column_ids.current(),
            self.parts.registry.borrow().len(),
        );
    }

    fn restore_round_start(&mut self) {
        let (plan_id, column_id, scalar_subqueries) = self.round_start;
        self.parts.plan_ids.store(plan_id);
        self.parts.column_ids.store(column_id);
        self.parts.registry.borrow_mut().truncate(scalar_subqueries);
    }
}

/// Go `planner.optimize` over a query: its rounds
/// ([`crate::planner_optimize::optimize`]), or the one build of a DML's
/// read.
///
/// # Errors
///
/// The planning's failure.
pub fn optimize_query(
    parts: &BuilderParts<'_>,
    query: &tidb_ast::QueryStmt,
    visits: BuildVisits<'_>,
    build: QueryBuild,
) -> Result<(LogicalPlan, PhysicalPlan), PlanError> {
    use crate::planner_optimize::LogicalPlanRounds as _;
    let mut rounds = QueryRounds {
        parts,
        query,
        visits,
        checked: false,
        round_start: (0, 0, 0),
    };
    if build == QueryBuild::DmlRead {
        return rounds
            .build_and_optimize(parts.ctx, |flag| flag)
            .map(|(logical, physical, _)| (logical, physical));
    }
    crate::planner_optimize::optimize(parts.ctx, &mut rounds)
}

/// Go `planner.Optimize` for a query statement past binding and the plan
/// cache: `TryFastPlan` (or the point plan a multi-statement query's
/// prefetch already tried, `PointPlanKey`), else `optimize`, then
/// `buildSelectInto`'s FILE privilege.
///
/// # Errors
///
/// The planning's or a privilege check's failure.
pub fn optimize_query_stmt(
    parts: &BuilderParts<'_>,
    query: &tidb_ast::QueryStmt,
) -> Result<PhysicalPlan, tidb_driver_error::DriverError> {
    let (catalog, current_db, ctx) = (parts.catalog, parts.current_database, parts.ctx);
    if let (tidb_ast::QueryStmt::Select(select), true) = (query, ctx.isolation_read_contains_tikv())
    {
        // Go `TryFastPlan`'s `checkFastPlanPrivilege(..., mysql.SelectPriv)`.
        let fast_plan_privilege = |database: &str, table: &str| {
            crate::privilege_check::check_fast_plan_privilege(
                ctx,
                catalog,
                database,
                table,
                &[tidb_mysql::SelectPriv],
            )
        };
        let fast = match crate::point_get_plan::handed_point_plan(
            ctx,
            crate::point_get_plan::PointPlanKind::Select,
        ) {
            // Go `Optimize`: the point plan a multi-statement query's
            // prefetch already tried (`PointPlanKey`), not `TryFastPlan`
            // again.
            Some(handed) => handed.plan.clone(),
            None => crate::point_get_plan::try_fast_plan_checked(
                select,
                catalog,
                current_db,
                ctx,
                &fast_plan_privilege,
            )?,
        };
        if let Some(plan) = fast {
            return Ok(plan);
        }
    }
    // Go `optimize` -> `buildLogicalPlan` -> `PlanBuilder.Build`, past
    // `TryFastPlan`: the statement's one build, whose visit infos
    // `CheckPrivilege` reads before the optimizer runs.
    record_plan_builder_metric(ctx);
    let (_, physical) = optimize_query(parts, query, BuildVisits::Check, QueryBuild::Statement)
        .map_err(planner_error_to_driver)?;
    // Go `buildSelectInto` optimizes the target query (whose own
    // `CheckPrivilege` ran above) and only then appends FILE, which the
    // statement's `CheckPrivilege` checks.
    if matches!(query, tidb_ast::QueryStmt::Select(select) if select.into_outfile.is_some()) {
        crate::privilege_check::check_plan_privilege_requests(
            ctx,
            catalog,
            &[crate::privilege_check::select_into_file_request()],
        )?;
    }
    Ok(physical)
}

/// Go `PlanBuilder.Build`'s `planBuilderCnt` (RU v2's plan count).
pub fn record_plan_builder_metric(ctx: &StmtContext) {
    if let Some(metrics) = ctx.ruv2_metrics() {
        metrics.add_plan_cnt(1);
    }
}

/// The statement layer's error for a planner failure.
#[must_use]
pub fn planner_error_to_driver(error: PlanError) -> tidb_driver_error::DriverError {
    use tidb_driver_error::{DriverError, ExecError};
    use tidb_planner_core_base::PlanErrorKind;
    match error.kind() {
        PlanErrorKind::Mysql(error) => DriverError::Mysql(error.clone()),
        PlanErrorKind::Eval(tidb_exprctx::EvalError::InvalidGroupFuncUse) => DriverError::from(
            tidb_plannererrors::ERR_INVALID_GROUP_FUNC_USE.gen_with_stack_by_args(&[]),
        ),
        PlanErrorKind::Eval(tidb_exprctx::EvalError::FieldInGroupingNotGroupBy(index)) => {
            DriverError::from(
                tidb_plannererrors::ERR_FIELD_IN_GROUPING_NOT_GROUP_BY
                    .gen_with_stack_by_args(&[format!("#{}", *index).into()]),
            )
        }
        PlanErrorKind::Eval(eval) => DriverError::Exec(ExecError::Eval(eval.clone())),
        PlanErrorKind::Internal => DriverError::unsupported(error.to_string()),
        PlanErrorKind::MemTableRetrievalNeeded => DriverError::MemTableRetrievalNeeded,
    }
}

/// The planner's error for a statement-layer failure inside planning (an
/// evaluated subquery's, a privilege check's).
#[must_use]
pub fn driver_error_to_plan(error: tidb_driver_error::DriverError) -> PlanError {
    use tidb_driver_error::{DriverError, ExecError};
    match error {
        // Go's `EvalSubqueryFirstRow` returns the executor's error as it is.
        DriverError::Mysql(error) => PlanError::from(error),
        DriverError::MemTableRetrievalNeeded => PlanError::mem_table_retrieval_needed(),
        DriverError::Exec(ExecError::Eval(eval)) => PlanError::eval(eval),
        other => PlanError::internal(other.to_string()),
    }
}

#[cfg(test)]
mod subquery_error_identity_tests {
    use super::driver_error_to_plan;

    /// Go's `EvalSubqueryFirstRow` returns the executor's error as it is, so
    /// any Go error a planning-time subquery raises -- here a recursive CTE
    /// that never stops -- reaches the client with its own code, not 1105.
    #[test]
    fn a_planning_time_subquery_keeps_the_go_error_it_raised() {
        let raised =
            tidb_exeerrors::ERR_CTE_MAX_RECURSION_DEPTH.gen_with_stack_by_args(&[1000_i64.into()]);
        let planned = driver_error_to_plan(tidb_driver_error::DriverError::from(raised.clone()));
        assert!(planned.equal(&tidb_exeerrors::ERR_CTE_MAX_RECURSION_DEPTH));
        let tidb_driver_error::DriverError::Mysql(back) = super::planner_error_to_driver(planned)
        else {
            panic!("the Go error was flattened");
        };
        assert_eq!(back.code, 3636);
        assert_eq!(
            back.message,
            tidb_terror::MysqlError::from_terror(&raised).message
        );
    }
}
