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

//! Go `pkg/planner/optimize.go`.
//!
//! Go `pkg/planner/optimize.go`'s `optimize`: the statement's first
//! logical-plan round and, under `tidb_opt_enable_alternative_logical_plans`,
//! the alternative rounds (`alternativeRounds`) the first round's signals
//! ask for; the cheapest round's plan is the statement's
//! (`buildAndOptimizeLogicalPlanRound`).
//!
//! One round -- Go's `buildLogicalPlan`, `CheckPrivilege` /
//! `CheckTableLock` and `core.DoOptimize` with the plan's cost -- is the
//! caller's [`LogicalPlanRounds::build_and_optimize`], over the statement's
//! catalog and its subquery evaluation.
//!
//! Go `pkg/planner/optimize.go`'s statement planning over a query:
//! `NewPlanBuilder().Init` ([`BuilderParts::build`]), `buildLogicalPlan`
//! ([`build_logical_plan`]), `buildAndOptimizeLogicalPlanRound`
//! ([`build_and_optimize_logical_plan_round`]) and `optimize`'s rounds
//! ([`optimize_query`]). What Go's executor hands the planner through
//! function variables -- evaluating an uncorrelated subquery, a column's
//! default, the statistics `DoOptimize` loads, `CheckPrivilege` -- is
//! [`PlanningHooks`].
//!
//! Go `planner.Optimize`'s binding step (`pkg/planner/optimize.go`): the
//! statement's matched binding (`bindinfo.MatchSQLBinding`) replaces its own
//! hints, and the session records the match.

use std::sync::atomic::{AtomicBool, Ordering};

use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_logicalop::LogicalPlan;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_sessionctx::StmtContext;

/// The statement's rounds as Go's `optimize` runs them.
pub trait LogicalPlanRounds {
    /// One build of the statement's plan under `ctx` with Go `adjustFlag`
    /// applied to the builder's optimization flag: Go `buildLogicalPlan`,
    /// the statement's privilege check (once), and `core.DoOptimize`, with
    /// the physical plan's cost.
    fn build_and_optimize(
        &mut self,
        ctx: &StmtContext,
        opt_flag_adjust: fn(u64) -> u64,
    ) -> Result<(LogicalPlan, PhysicalPlan, f64), PlanError>;

    /// Go `buildLogicalPlan` starts every round's `PlanID`, `PlanColumnID`
    /// and `MapScalarSubQ` where the first round started: remembers them.
    fn save_round_start(&mut self);

    /// Puts the plan and column ids and the scalar subqueries back where the
    /// first round started.
    fn restore_round_start(&mut self);
}

/// Go `alternativeRound`: one more build of the statement's plan that
/// `optimize` costs against the first.
pub struct AlternativeRound {
    /// Go `name`.
    pub name: &'static str,
    /// Go `adjustFlag`.
    pub adjust_flag: fn(u64) -> u64,
    /// Go `enabled`, read off the signals the first round left.
    pub enabled: fn(&StmtContext) -> bool,
    /// Go `setup` and the cleanup it returns: the context the round builds
    /// under, which shares the statement's state, and what to undo on the
    /// statement once the round is over.
    #[allow(clippy::type_complexity)]
    pub setup: for<'a> fn(&'a StmtContext) -> (StmtContext, Option<Box<dyn FnOnce() + 'a>>),
}

fn signal(signal: &AtomicBool) -> bool {
    signal.load(Ordering::Acquire)
}

/// Go `shouldTryEngineRestrictedRounds`.
#[must_use]
pub fn should_try_engine_restricted_rounds(ctx: &StmtContext) -> bool {
    let signals = ctx.alternative_logical_plan_signals();
    signal(&signals.mixed_storage_engines)
        && !signal(&signals.has_store_type_hint)
        && !crate::optimizer::OptimizeContext::optimizer_cost_env(ctx)
            .session
            .mpp_enforced
}

/// Go `alternativeRounds`, in the order `optimize` runs them.
pub const ALTERNATIVE_ROUNDS: [AlternativeRound; 7] = [
    AlternativeRound {
        name: "non-decorrelate",
        adjust_flag: |flag| flag & !tidb_planner_core_rule::flags::DECORRELATE,
        // Go `shouldTryNonDecorrelationRound`.
        enabled: |ctx| {
            let signals = ctx.alternative_logical_plan_signals();
            signal(&signals.decorrelated_apply) && !signal(&signals.same_order_index_join)
        },
        setup: |ctx| (ctx.clone(), None),
    },
    AlternativeRound {
        name: "order-aware-reorder",
        adjust_flag: |flag| flag | tidb_planner_core_rule::flags::ORDER_AWARE_JOIN_REORDER,
        // Go `shouldTryOrderAwareReorderRound`.
        enabled: |ctx| {
            signal(
                &ctx.alternative_logical_plan_signals()
                    .order_aware_join_reorder,
            )
        },
        setup: |ctx| (ctx.clone(), None),
    },
    AlternativeRound {
        name: "correlate",
        adjust_flag: |flag| flag | tidb_planner_core_rule::flags::CORRELATE,
        // Go `shouldTryCorrelateRound`.
        enabled: |ctx| signal(&ctx.alternative_logical_plan_signals().prefer_correlate),
        setup: |ctx| (ctx.clone().with_enable_correlate_subquery(true), None),
    },
    AlternativeRound {
        name: "semi-join-rewrite",
        adjust_flag: |flag| flag,
        // Go `shouldTrySemiJoinRewriteRound`.
        enabled: |ctx| {
            signal(&ctx.alternative_logical_plan_signals().semi_join_rewrite)
                && !ctx.enable_semi_join_rewrite()
        },
        setup: |ctx| (ctx.clone().with_enable_semi_join_rewrite(true), None),
    },
    AlternativeRound {
        name: "fts-like-fallback",
        adjust_flag: |flag| flag,
        enabled: |ctx| {
            let signals = ctx.alternative_logical_plan_signals();
            signal(&signals.fts_like_fallback) || signal(&signals.has_predicate_context_match)
        },
        setup: |ctx| {
            let fts_like_fallback = &ctx.alternative_logical_plan_signals().fts_like_fallback;
            let saved = fts_like_fallback.swap(true, Ordering::AcqRel);
            (
                ctx.clone(),
                Some(Box::new(move || {
                    fts_like_fallback.store(saved, Ordering::Release)
                })),
            )
        },
    },
    AlternativeRound {
        name: "tikv-only",
        adjust_flag: |flag| flag,
        // Go `shouldTryTiKVOnlyRound`.
        enabled: should_try_engine_restricted_rounds,
        setup: |ctx| {
            (
                ctx.clone()
                    .with_isolation_read_engines("tikv,tidb".to_owned()),
                None,
            )
        },
    },
    AlternativeRound {
        name: "tiflash-only",
        adjust_flag: |flag| flag,
        // Go `shouldTryTiFlashOnlyRound`.
        enabled: |ctx| {
            should_try_engine_restricted_rounds(ctx)
                && !signal(&ctx.alternative_logical_plan_signals().missing_tiflash_path)
                && crate::optimizer::OptimizeContext::optimizer_cost_env(ctx)
                    .session
                    .mpp_allowed
        },
        setup: |ctx| {
            (
                ctx.clone()
                    .with_isolation_read_engines("tiflash,tidb".to_owned()),
                None,
            )
        },
    },
];

/// Go `optimize`'s `bestPlan`, `bestCost` and `bestLogicalPlanCtx`.
struct BestPlan {
    logical: LogicalPlan,
    physical: PhysicalPlan,
    cost: f64,
    logical_plan_ctx: tidb_sessionctx::LogicalPlanBuildState,
}

/// Go `buildAndOptimizeLogicalPlanRound`: one build of the statement's plan,
/// kept in `best_plan` when it is the first or the cheapest so far.
fn keep_cheapest_round(
    ctx: &StmtContext,
    rounds: &mut dyn LogicalPlanRounds,
    best_plan: &mut Option<BestPlan>,
    opt_flag_adjust: fn(u64) -> u64,
) -> Result<(), PlanError> {
    let (logical, physical, cost) = rounds.build_and_optimize(ctx, opt_flag_adjust)?;
    let signals = ctx.alternative_logical_plan_signals();
    if signals.predicate_match.load(Ordering::Acquire) {
        signals
            .has_predicate_context_match
            .store(true, Ordering::Release);
    }
    // A predicate-context MATCH whose native form cannot run: the plan is no
    // candidate, and every round that follows rewrites it to ILIKE.
    if signals.non_viable_fts_match.load(Ordering::Acquire) {
        signals.fts_like_fallback.store(true, Ordering::Release);
        return Ok(());
    }
    if best_plan.as_ref().is_none_or(|best| cost < best.cost) {
        *best_plan = Some(BestPlan {
            logical,
            physical,
            cost,
            logical_plan_ctx: ctx.save_logical_plan_build_ctx(),
        });
    }
    Ok(())
}

/// Go `optimize`'s logical-plan rounds: the first round, then each enabled
/// alternative round, the cheapest plan winning. Every alternative round is
/// off without `tidb_opt_enable_alternative_logical_plans`.
pub fn optimize(
    ctx: &StmtContext,
    rounds: &mut dyn LogicalPlanRounds,
) -> Result<(LogicalPlan, PhysicalPlan), PlanError> {
    if !ctx.enable_alternative_logical_plans() {
        return rounds
            .build_and_optimize(ctx, |flag| flag)
            .map(|(logical, physical, _)| (logical, physical));
    }
    let initial_logical_plan_ctx = ctx.save_logical_plan_build_ctx();
    rounds.save_round_start();
    ctx.reset_alternative_logical_plan_signals();
    let signals = ctx.alternative_logical_plan_signals();
    let mut best_plan = None;
    keep_cheapest_round(ctx, rounds, &mut best_plan, |flag| flag)?;
    // The engine-restricted rounds arm on the shape of the first round's
    // chosen plan: only a plan that mixes TiKV and TiFlash reads leaves a
    // homogeneous alternative uncosted, and a single-scan index join is left
    // as the first round built it.
    if let Some(best) = &best_plan {
        let (has_tikv, has_tiflash) =
            tidb_planner_core_operator_physicalop::storage_engine_usage::storage_engine_usage(
                &best.physical,
            );
        if has_tikv
            && has_tiflash
            && !tidb_planner_core_operator_physicalop::single_scan_index_join::has_single_scan_index_join(
                &best.physical,
            )
        {
            signals.mixed_storage_engines.store(true, Ordering::Release);
        }
    }
    // Which rounds run is decided by the first round's signals alone: a later
    // round's build sets signals too.
    let enabled_rounds: Vec<&AlternativeRound> = ALTERNATIVE_ROUNDS
        .iter()
        .filter(|round| (round.enabled)(ctx))
        .collect();
    let mut last_alt_round_err = None;
    for alternative in enabled_rounds {
        ctx.restore_logical_plan_build_ctx(&initial_logical_plan_ctx);
        ctx.reset_alternative_logical_plan_marks();
        rounds.restore_round_start();
        let (round_ctx, cleanup) = (alternative.setup)(ctx);
        let result =
            keep_cheapest_round(&round_ctx, rounds, &mut best_plan, alternative.adjust_flag);
        if let Some(cleanup) = cleanup {
            cleanup();
        }
        if let Err(error) = result {
            // The session answers this one by retrieving the rows and
            // planning the statement again; it is no failure of the round.
            if matches!(
                error.kind(),
                tidb_planner_core_base::PlanErrorKind::MemTableRetrievalNeeded
            ) {
                return Err(error);
            }
            // An alternative round is optional: the first round's plan
            // stands. The fts-like-fallback round's error is the statement's
            // when the first round's plan was discarded as non-executable.
            tracing::warn!(round = alternative.name, %error, "alternative logical plan round failed");
            last_alt_round_err = Some(error);
        }
    }
    let Some(best) = best_plan else {
        return Err(last_alt_round_err
            .unwrap_or_else(|| PlanError::internal("failed to build logical plan")));
    };
    ctx.restore_logical_plan_build_ctx(&best.logical_plan_ctx);
    Ok((best.logical, best.physical))
}

use std::cell::RefCell;
use std::rc::Rc;

use tidb_expr::expression::Expression;
use tidb_planner_core_base::{ColumnIdAllocator, PlanIdAllocator};

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
    let logical = crate::optimizer::optimize_built_logical(
        inner.clone(),
        opt_flag,
        catalog,
        ctx,
        parts.use_plan_cache,
        plan_ids,
        column_ids,
        &ctx.session_zone(),
    )?;
    let mut physical =
        crate::optimizer::physical_plan_for_logical(&logical, plan_ids, column_ids, catalog, ctx)?;
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
        let registered = std::sync::Arc::new(ScalarSubqueryEvalCtx {
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
pub struct ScalarSubqueryEvalCtx {
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

impl std::fmt::Debug for ScalarSubqueryEvalCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScalarSubqueryEvalCtx")
            .field("output_col_ids", &self.output_col_ids)
            .field("plan_id", &self.plan_id)
            .finish_non_exhaustive()
    }
}

impl tidb_expr::expression::ScalarSubqueryEval for ScalarSubqueryEvalCtx {
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

/// The per-statement list of [`ScalarSubqueryEvalCtx`]s, Go's
/// `SessionVars.MapScalarSubQ` narrowed to one statement.
pub type ScalarSubqueryRegistry = Rc<RefCell<Vec<std::sync::Arc<ScalarSubqueryEvalCtx>>>>;

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
    let logical = crate::optimizer::optimize_built_logical(
        plan,
        opt_flag_adjust(opt_flag),
        parts.catalog,
        ctx,
        parts.use_plan_cache,
        parts.plan_ids,
        parts.column_ids,
        &ctx.session_zone(),
    )?;
    let (physical, cost) = crate::optimizer::physical_optimize(
        &logical,
        parts.plan_ids,
        parts.column_ids,
        ctx,
        &std::collections::BTreeSet::new(),
        &crate::optimizer::StatementUsedStats {
            catalog: parts.catalog,
            context: ctx,
        },
    )?;
    Ok((logical, physical, cost))
}

/// One statement's planning rounds over its builder parts (Go
/// `buildAndOptimizeLogicalPlanRound`, which
/// [`crate::optimize::optimize`] runs once per round).
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

impl crate::optimize::LogicalPlanRounds for QueryRounds<'_, '_> {
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
/// ([`crate::optimize::optimize`]), or the one build of a DML's
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
    use crate::optimize::LogicalPlanRounds as _;
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
    crate::optimize::optimize(parts.ctx, &mut rounds)
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

use tidb_ast::Stmt;
use tidb_bindinfo::binding::{self, BindingMatchContext};

/// What Go `Optimize`'s binding step reads and writes on `sctx` beyond the
/// match itself.
pub trait PlannerBindingContext: BindingMatchContext {
    /// Go `SessionVars.UsePlanBaselines`.
    fn use_plan_baselines(&self) -> bool;
    /// Go `matchSQLBindingCore`'s deferred `DurationOptimizer.BindingMatch`.
    fn set_binding_match_duration(&mut self, elapsed: std::time::Duration);
    /// Go `StmtCtx.AppendWarning` of a 1105 warning.
    fn append_binding_warning(&mut self, message: String);
    /// Go `SessionVars.FoundInBinding = true`.
    fn set_found_in_binding(&mut self);
}

/// Go `Optimize`'s binding step: the statement with its matched binding's
/// hints and the binding's `BindSQL`, `None` when no binding matched (the
/// caller keeps the original, so a session with no bindings pays one
/// map-emptiness test and nothing else).
pub fn bind_sql_binding<C: PlannerBindingContext + ?Sized>(
    ctx: &mut C,
    stmt: &Stmt,
) -> Option<(Stmt, String)> {
    if !binding::may_have_sql_binding(stmt) {
        return None;
    }
    // Go gates the whole step on `SessionVars.UsePlanBaselines`.
    if !ctx.use_plan_baselines() {
        return None;
    }
    let begin_binding_match = std::time::Instant::now();
    let matched = binding::match_sql_binding_core(ctx, stmt);
    ctx.set_binding_match_duration(begin_binding_match.elapsed());
    let (hints, bind_sql) = matched?;
    let mut bound = stmt.clone();
    binding::bind_hints(&mut bound, &hints);
    for warning in filter_sem_restricted_hints(&mut bound) {
        ctx.append_binding_warning(warning);
    }
    ctx.set_found_in_binding();
    // Go `optimize`: `originStmtHints.QueryHasHints` -- the statement wrote
    // hints of its own, which the binding's replace.
    if !tidb_hint::extract_table_hints_from_stmt_node(stmt)
        .0
        .is_empty()
    {
        ctx.append_binding_warning(format!(
            "The system ignores the hints in the current query and uses the hints \
             specified in the bindSQL: {bind_sql}"
        ));
    }
    Some((bound, bind_sql))
}

/// Go `hint.RegisterRestrictedHintChecker(semv2.IsRestrictedHint)`
/// (`planner/core/core_init.go`): a hint SEM restricts is dropped from
/// the statement with its warning.
#[must_use]
pub fn filter_sem_restricted_hints(stmt: &mut Stmt) -> Vec<String> {
    struct Filter {
        warnings: Vec<String>,
    }

    impl Filter {
        fn retain(&mut self, hints: &mut Vec<tidb_ast::Hint>) {
            hints.retain(
                |hint| match tidb_sem_v2::is_restricted_hint(&hint.lower_name()) {
                    Ok(()) => true,
                    Err(warning) => {
                        self.warnings.push(warning);
                        false
                    }
                },
            );
        }
    }

    impl tidb_ast::Visitor for Filter {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(select) = node.downcast_mut::<tidb_ast::SelectStmt>() {
                self.retain(&mut select.hints);
            } else if let Some(insert) = node.downcast_mut::<tidb_ast::InsertStmt>() {
                self.retain(&mut insert.hints);
            } else if let Some(update) = node.downcast_mut::<tidb_ast::UpdateStmt>() {
                self.retain(&mut update.hints);
            } else if let Some(delete) = node.downcast_mut::<tidb_ast::DeleteStmt>() {
                self.retain(&mut delete.hints);
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut filter = Filter {
        warnings: Vec::new(),
    };
    tidb_ast::Visitable::accept(stmt, &mut filter);
    filter.warnings
}

use crate::plan_cache::PlanCacheStmt;
use crate::plan_cacheable_checker::{
    MAX_NUM_PARAM, ParameterizedSelect, Refusal, get_param_sql_from_ast, parameterize_dml,
};
use tidb_charset::Collation;
use tidb_driver_error::DriverError;
use tidb_kvcache::SimpleLruCache;
use tidb_planner_core_metrics::get_non_prep_plan_cache_unsupported_counter;

/// Go SessionVars.nonPreparedPlanCacheStmts: syntax metadata only, with one
/// definition LRU shared by SELECT and DML. Physical entries live in the
/// session owner independently of metadata eviction.
///
/// Each entry keeps the privilege requests of its statement, Go's
/// `PlanCacheStmt.VisitInfos` (`GeneratePlanCacheStmtWithAST`), which every
/// use checks (`checkPreparedPriv`) before it binds a plan.
#[derive(Clone)]
pub enum NonPreparedPlan {
    /// A parameterized query.
    Select(std::sync::Arc<PlanCacheStmt>, NonPreparedPrivileges),
    /// A parameterized INSERT, UPDATE or DELETE.
    Dml(std::sync::Arc<PlanCacheStmt>, NonPreparedPrivileges),
}

/// Go `PlanCacheStmt.VisitInfos` of a non-prepared plan-cache entry.
pub type NonPreparedPrivileges = std::sync::Arc<[crate::privilege_check::TablePrivilegeRequest]>;

#[derive(Default)]
/// Go `SessionVars.nonPreparedPlanCacheStmts`.
pub struct NonPreparedPlanCache {
    plans: Option<SimpleLruCache<String, NonPreparedPlan>>,
}

impl NonPreparedPlanCache {
    /// The entry of a parameterized statement text.
    pub fn get(&mut self, key: &str) -> Option<NonPreparedPlan> {
        self.plans.as_mut()?.get(key).cloned()
    }

    /// Admits an entry.
    pub fn put(&mut self, key: String, plan: NonPreparedPlan, capacity: usize) {
        // Go allocates the metadata LRU at the first admitted insertion.
        self.plans
            .get_or_insert_with(|| SimpleLruCache::new(capacity))
            .put(key, plan);
    }
}

/// Go `getPlanFromNonPreparedPlanCache` and `NonPreparedPlanCacheableWithCtx` over the session.
/// What Go `getPlanFromNonPreparedPlanCache` reads and writes on `sctx`.
pub trait NonPreparedPlanCacheContext: crate::plan_cache_utils::PlanCacheSessionContext {
    /// Go `SessionVars`' non-prepared plan cache.
    fn non_prepared_plan_cache_mut(&mut self) -> &mut NonPreparedPlanCache;
    /// The session's physical plan cache (Go `GetSessionPlanCache`).
    fn plan_cache_rebuild(&self) -> &crate::plan_cache::SessionPlanCache;
    /// The statement's hints, parsed without catalog metadata.
    fn parse_statement_hints_without_catalog(
        &self,
        stmt: &Stmt,
        current_database: &str,
    ) -> tidb_hint::StmtHints;
    /// Sizes the session plan cache from its variables.
    fn configure_session_plan_cache(&self);
    /// Go `tidb_session_plan_cache_size`.
    fn session_plan_cache_capacity(&self) -> usize;
    /// Go `PlanBuilder`'s visit infos of `stmt` (`GeneratePlanCacheStmtWithAST`).
    ///
    /// # Errors
    ///
    /// The build's failure.
    fn collect_table_privileges(
        &self,
        stmt: &Stmt,
        preparing: bool,
    ) -> Result<Vec<crate::privilege_check::TablePrivilegeRequest>, DriverError>;
    /// Go `checkPreparedPriv`.
    ///
    /// # Errors
    ///
    /// The first refused request.
    fn check_table_privilege_requests(
        &self,
        requests: &[crate::privilege_check::TablePrivilegeRequest],
    ) -> Result<(), DriverError>;
    /// The statement's infoschema, locked.
    ///
    /// # Errors
    ///
    /// When the catalog is unavailable.
    fn plan_cache_catalog(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, tidb_infoschema::SessionExtendedInfoSchema>, DriverError>;
    /// The domain's infoschema.
    fn plan_cache_domain_info_schema(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<tidb_infoschema::SessionExtendedInfoSchema>>;
    /// A statement context for a non-DML statement.
    fn non_prepared_statement_context(&self) -> StmtContext;
    /// The executor's half of planning over `catalog`.
    fn planning_hooks<'a>(
        &self,
        catalog: &'a tidb_infoschema::SessionExtendedInfoSchema,
    ) -> Box<dyn PlanningHooks + 'a>;

    /// Parameterizes one admitted non-prepared SELECT. Refused statements use
    /// the ordinary planner and publish no cache hit.
    fn parameterize_non_prepared_select(&mut self, stmt: &Stmt) -> Option<ParameterizedSelect> {
        if !self.non_prepared_plan_cache_enabled() {
            return None;
        }
        self.parameterize_non_prepared_select_or_refusal(stmt).ok()
    }

    /// [`Self::parameterize_non_prepared_select`] with Go
    /// `NonPreparedPlanCacheableWithCtx`'s refusal reason.
    fn parameterize_non_prepared_select_or_refusal(
        &mut self,
        stmt: &Stmt,
    ) -> Result<ParameterizedSelect, Refusal> {
        // Go's unsupported counter increments only for checker-walk refusals
        // (cacheable=false after `Accept`); clause-level fast-check refusals
        // return their reason without touching any counter. The
        // classification rides on `Refusal::counted`.
        let enable_param_limit = self
            .get_session_vars()
            .system_bool("tidb_enable_plan_cache_for_param_limit", true);
        let string_collation = self
            .get_session_vars()
            .get_system("collation_connection")
            .ok()
            .and_then(|name| Collation::from_name(&name))
            .unwrap_or(Collation::Utf8Mb4Bin);
        let configured_max = self
            .get_session_vars()
            .optimizer_fix_control()
            .get_int_with_default(tidb_fixcontrol::FIX_44823, MAX_NUM_PARAM as i64);
        let max_num_param = match configured_max {
            0 => usize::MAX,
            value if value > 0 => usize::try_from(value).unwrap_or(usize::MAX),
            _ => MAX_NUM_PARAM,
        };
        let domain = self.plan_cache_domain_info_schema();
        let catalog = domain
            .lock()
            .map_err(|_| Refusal::fast("the catalog is unavailable"))?;
        get_param_sql_from_ast(
            stmt,
            &catalog,
            &self.get_session_vars().current_db,
            enable_param_limit,
            self.non_prepared_plan_cache_for_dml_enabled(),
            string_collation,
            max_num_param,
        )
        .inspect_err(|refusal| {
            // Go `NonPreparedPlanCacheableWithCtx`'s unsupported counter:
            // only checker-walk refusals count; clause-level fast-check
            // refusals return their reason without touching the counter.
            if refusal.counted {
                get_non_prep_plan_cache_unsupported_counter().inc();
            }
        })
    }

    /// Go's DML switch: `EnableNonPreparedPlanCacheForDML`, true by default.
    fn non_prepared_plan_cache_for_dml_enabled(&self) -> bool {
        self.get_session_vars()
            .system_bool("tidb_enable_non_prepared_plan_cache_for_dml", true)
    }

    /// Parameterizes one non-prepared UPDATE/INSERT/DELETE statement.
    fn parameterize_non_prepared_dml(&mut self, stmt: &Stmt) -> Option<ParameterizedSelect> {
        if !self.non_prepared_plan_cache_enabled()
            || !self.non_prepared_plan_cache_for_dml_enabled()
        {
            return None;
        }
        let enable_param_limit = self
            .get_session_vars()
            .system_bool("tidb_enable_plan_cache_for_param_limit", true);
        let string_collation = self
            .get_session_vars()
            .get_system("collation_connection")
            .ok()
            .and_then(|name| Collation::from_name(&name))
            .unwrap_or(Collation::Utf8Mb4Bin);
        let configured_max = self
            .get_session_vars()
            .optimizer_fix_control()
            .get_int_with_default(tidb_fixcontrol::FIX_44823, MAX_NUM_PARAM as i64);
        let max_num_param = match configured_max {
            0 => usize::MAX,
            value if value > 0 => usize::try_from(value).unwrap_or(usize::MAX),
            _ => MAX_NUM_PARAM,
        };
        let domain = self.plan_cache_domain_info_schema();
        let catalog = domain.lock().ok()?;
        parameterize_dml(
            stmt,
            &catalog,
            &self.get_session_vars().current_db,
            enable_param_limit,
            string_collation,
            max_num_param,
        )
        .ok()
    }

    /// Go `GeneratePlanCacheStmtWithAST`'s `VisitInfos` for a new
    /// non-prepared entry over its parameterized statement; `None` when they
    /// cannot be collected, which leaves the statement to the ordinary path
    /// (whose build reports why).
    fn non_prepared_privilege_requests(&self, statement: &Stmt) -> Option<NonPreparedPrivileges> {
        self.collect_table_privileges(statement, true)
            .ok()
            .map(std::sync::Arc::from)
    }

    /// Generates or recursively rebuilds the retained DML root for one
    /// parameterized non-prepared statement, through the same plan object
    /// SQL and binary PREPARE use. Answers the entry's privilege requests,
    /// already checked (Go `GetPlanFromPlanCache`'s `checkPreparedPriv`),
    /// with the execution.
    fn bind_non_prepared_dml(
        &mut self,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        binding_sql: Option<&str>,
    ) -> Result<Option<(crate::plan_cache::PlanCacheExecution, NonPreparedPrivileges)>, DriverError>
    {
        if !self.non_prepared_plan_cache_allowed(effective_statement) {
            return Ok(None);
        }
        let Some(environment) =
            crate::plan_cache_utils::prepared_plan_cache_environment_for_binding(self, binding_sql)
        else {
            return Ok(None);
        };
        let lookup_start = std::time::Instant::now();
        let (plan, requests) = match self.non_prepared_plan_cache_mut().get(&parameterized.key) {
            Some(NonPreparedPlan::Dml(plan, requests)) => (plan, requests),
            Some(_) => return Ok(None),
            None => {
                let plan = {
                    let Ok(catalog) = self.plan_cache_catalog() else {
                        return Ok(None);
                    };
                    let Ok(Some(plan)) = crate::plan_cache::build_prepared_dml_plan(
                        &parameterized.statement,
                        parameterized.values.len(),
                        &catalog,
                        self.get_session_vars().current_db.as_str(),
                    ) else {
                        return Ok(None);
                    };
                    plan
                };
                let Some(requests) = self.non_prepared_privilege_requests(&parameterized.statement)
                else {
                    return Ok(None);
                };
                let plan = std::sync::Arc::new(
                    plan.with_sql(&parameterized.key)
                        .with_non_prepared_stats_tables(),
                );
                let capacity = self.session_plan_cache_capacity();
                self.non_prepared_plan_cache_mut().put(
                    parameterized.key.clone(),
                    NonPreparedPlan::Dml(
                        std::sync::Arc::clone(&plan),
                        std::sync::Arc::clone(&requests),
                    ),
                    capacity,
                );
                (plan, requests)
            }
        };
        self.check_table_privilege_requests(&requests)?;
        Ok(self
            .bind_non_prepared_dml_plan(
                &plan,
                parameterized,
                effective_statement,
                &environment,
                lookup_start,
            )
            .map(|execution| (execution, requests)))
    }

    /// [`Self::bind_non_prepared_dml`]'s lookup or replan of the entry.
    fn bind_non_prepared_dml_plan(
        &mut self,
        plan: &std::sync::Arc<PlanCacheStmt>,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        environment: &crate::plan_cache_utils::PreparedPlanCacheEnvironment,
        lookup_start: std::time::Instant,
    ) -> Option<crate::plan_cache::PlanCacheExecution> {
        self.configure_session_plan_cache();
        {
            let catalog = self.plan_cache_catalog().ok()?;
            let hooks = self.planning_hooks(&catalog);
            if let Some(execution) = plan.bind_cached_for_statement(
                &*hooks,
                self.plan_cache_rebuild(),
                &parameterized.values,
                &catalog,
                self.get_session_vars().current_db.as_str(),
                &crate::plan_cache_utils::plan_cache_dirty_tables(self),
                &environment,
                effective_statement,
            ) {
                // Go `GetPlanFromPlanCache`'s hit arm; `lookupPlanCache`'s
                // defer observes the lookup duration on hits only.
                tidb_planner_core_metrics::inc_plan_cache_hit(true);
                tidb_planner_core_metrics::get_plan_cache_lookup_duration(
                    self.plan_cache_rebuild().uses_instance(),
                )
                .observe(lookup_start.elapsed().as_secs_f64());
                return Some(execution);
            }
        }
        // Go's miss arm re-plans; its miss counter records the replan.
        tidb_planner_core_metrics::plan_cache_miss_counter(true).inc();
        let ctx = self
            .non_prepared_statement_context()
            .with_non_prepared_plan_cache();
        let catalog = self.plan_cache_catalog().ok()?;
        let hooks = self.planning_hooks(&catalog);
        plan.bind_for_statement(
            &*hooks,
            self.plan_cache_rebuild(),
            &parameterized.values,
            &catalog,
            self.get_session_vars().current_db.as_str(),
            &ctx,
            &crate::plan_cache_utils::plan_cache_dirty_tables(self),
            &environment,
            effective_statement,
        )
        .ok()
        .flatten()
    }

    /// Generates or recursively rebuilds the physical plan owned by one
    /// parameterized non-prepared statement. This is Go's
    /// `GetPlanFromPlanCache(..., isNonPrepared=true)` over the same Rust plan
    /// object SQL and binary PREPARE use.
    fn bind_non_prepared_select(
        &mut self,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        binding_sql: Option<&str>,
    ) -> Result<Option<crate::plan_cache::PlanCacheExecution>, DriverError> {
        self.bind_non_prepared_select_warning(
            parameterized,
            effective_statement,
            binding_sql,
            false,
        )
    }

    /// [`Self::bind_non_prepared_select`]; `always_warn` is Go's
    /// `SetAlwaysWarnSkipCache`, which EXPLAIN FORMAT = 'plan_cache' sets.
    /// The entry's privilege requests are checked first (Go
    /// `GetPlanFromPlanCache`'s `checkPreparedPriv`).
    fn bind_non_prepared_select_warning(
        &mut self,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        binding_sql: Option<&str>,
        always_warn: bool,
    ) -> Result<Option<crate::plan_cache::PlanCacheExecution>, DriverError> {
        if !self.non_prepared_plan_cache_allowed(effective_statement) {
            return Ok(None);
        }
        let lookup_start = std::time::Instant::now();
        let Some(environment) =
            crate::plan_cache_utils::prepared_plan_cache_environment_for_binding(self, binding_sql)
        else {
            return Ok(None);
        };
        let (plan, requests) = match self.non_prepared_plan_cache_mut().get(&parameterized.key) {
            Some(NonPreparedPlan::Select(plan, requests)) => (plan, requests),
            Some(_) => return Ok(None),
            None => {
                let ctx = self.non_prepared_statement_context();
                let plan = {
                    let Ok(catalog) = self.plan_cache_catalog() else {
                        return Ok(None);
                    };
                    let Some(plan) = crate::plan_cache::build_prepared_select_plan(
                        &parameterized.statement,
                        parameterized.values.len(),
                        &catalog,
                        self.get_session_vars().current_db.as_str(),
                        &ctx,
                    ) else {
                        return Ok(None);
                    };
                    plan
                };
                let Some(requests) = self.non_prepared_privilege_requests(&parameterized.statement)
                else {
                    return Ok(None);
                };
                let plan = std::sync::Arc::new(
                    plan.with_sql(&parameterized.key)
                        .with_non_prepared_stats_tables(),
                );
                let capacity = self.session_plan_cache_capacity();
                self.non_prepared_plan_cache_mut().put(
                    parameterized.key.clone(),
                    NonPreparedPlan::Select(
                        std::sync::Arc::clone(&plan),
                        std::sync::Arc::clone(&requests),
                    ),
                    capacity,
                );
                (plan, requests)
            }
        };
        self.check_table_privilege_requests(&requests)?;
        Ok(self.bind_non_prepared_select_plan(
            &plan,
            parameterized,
            effective_statement,
            &environment,
            lookup_start,
            always_warn,
        ))
    }

    /// [`Self::bind_non_prepared_select_warning`]'s lookup or replan of the
    /// entry.
    fn bind_non_prepared_select_plan(
        &mut self,
        plan: &std::sync::Arc<PlanCacheStmt>,
        parameterized: &ParameterizedSelect,
        effective_statement: &Stmt,
        environment: &crate::plan_cache_utils::PreparedPlanCacheEnvironment,
        lookup_start: std::time::Instant,
        always_warn: bool,
    ) -> Option<crate::plan_cache::PlanCacheExecution> {
        self.configure_session_plan_cache();
        let observe = |start: std::time::Instant| {
            tidb_planner_core_metrics::get_plan_cache_lookup_duration(
                self.plan_cache_rebuild().uses_instance(),
            )
            .observe(start.elapsed().as_secs_f64());
        };
        {
            let catalog = self.plan_cache_catalog().ok()?;
            let hooks = self.planning_hooks(&catalog);
            if let Some(execution) = plan.bind_cached_for_statement(
                &*hooks,
                self.plan_cache_rebuild(),
                &parameterized.values,
                &catalog,
                self.get_session_vars().current_db.as_str(),
                &crate::plan_cache_utils::plan_cache_dirty_tables(self),
                &environment,
                effective_statement,
            ) {
                // Go `GetPlanFromPlanCache`'s hit arm. Go's `lookupPlanCache`
                // defer observes the lookup duration ONLY when it hit; a
                // miss/re-plan records the miss counter and nothing else.
                tidb_planner_core_metrics::inc_plan_cache_hit(true);
                observe(lookup_start);
                return Some(execution);
            }
        }
        // Go's miss arm re-plans; its miss counter records the replan.
        tidb_planner_core_metrics::plan_cache_miss_counter(true).inc();
        let mut ctx = self
            .non_prepared_statement_context()
            .with_non_prepared_plan_cache();
        if always_warn {
            ctx = ctx.with_always_warn_skip_plan_cache();
        }
        let catalog = self.plan_cache_catalog().ok()?;
        let hooks = self.planning_hooks(&catalog);
        let execution = plan.bind_for_statement(
            &*hooks,
            self.plan_cache_rebuild(),
            &parameterized.values,
            &catalog,
            self.get_session_vars().current_db.as_str(),
            &ctx,
            &crate::plan_cache_utils::plan_cache_dirty_tables(self),
            &environment,
            effective_statement,
        );
        execution.ok().flatten()
    }

    /// Whether `@@last_plan_from_cache` should report a hit, which is the
    /// PRECEDING statement's outcome (Go `PrevFoundInPlanCache`).
    fn last_plan_from_cache(&self) -> bool {
        self.get_session_vars().prev_found_in_plan_cache
    }

    /// Go `EnableNonPreparedPlanCache`.
    fn non_prepared_plan_cache_enabled(&self) -> bool {
        self.get_session_vars()
            .system_bool("tidb_enable_non_prepared_plan_cache", false)
    }

    /// Go `getPlanFromNonPreparedPlanCache`'s strategy gate: `hint_only`
    /// admits only a statement carrying `use_plan_cache()`.
    fn non_prepared_hint_only_refuses(&self, statement: &Stmt) -> bool {
        let hint_only = self
            .get_session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_STRATEGY)
            .is_ok_and(|strategy| {
                strategy.eq_ignore_ascii_case(
                    tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_STRATEGY_HINT_ONLY,
                )
            });
        hint_only
            && !self
                .parse_statement_hints_without_catalog(
                    statement,
                    self.get_session_vars().current_db.as_str(),
                )
                .use_plan_cache
    }

    /// Whether the statement may use the non-prepared cache now.
    fn non_prepared_plan_cache_allowed(&self, statement: &Stmt) -> bool {
        // Go `getPlanFromNonPreparedPlanCache`: a statement of a
        // multi-statement query never reaches the cache.
        if self.get_session_vars().in_multi_stmts {
            return false;
        }
        let hints = self.parse_statement_hints_without_catalog(
            statement,
            self.get_session_vars().current_db.as_str(),
        );
        if hints.ignore_plan_cache {
            return false;
        }
        let hint_only = self
            .get_session_vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_STRATEGY)
            .is_ok_and(|strategy| {
                strategy.eq_ignore_ascii_case(
                    tidb_vardef::tidb_vars::TIDB_PLAN_CACHE_STRATEGY_HINT_ONLY,
                )
            });
        !hint_only || hints.use_plan_cache
    }
}
