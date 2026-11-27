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

//! Go `pkg/planner/core/optimizer.go`.
//!
//! Go `pkg/planner/core/optimizer.go`'s `DoOptimize`: the logical half
//! (`adjustOptimizationFlags`, the statistics initialization, `RecheckCTE`,
//! `logicalOptimize`, `RecursiveDeriveStats`) and the physical half
//! (`physicalOptimize` and `postOptimize`).
//!
//! The planner owns the drive. What it needs from below -- the statement's
//! session settings, the statistics the domain loads, a partitioned table's
//! pruning, a CTE's lazy optimization -- reaches it through
//! [`OptimizeContext`] and [`OptimizeServices`], as Go's `base.PlanContext`
//! and the statistics handle reach `pkg/planner/core`.
//!
//! Go `DoOptimize` (`pkg/planner/core/optimizer.go`) over one statement:
//! `logicalOptimize` with the statistics its rules load (`initStats`,
//! `CollectPredicateColumnsPoint` / `SyncWaitStatsLoadPoint`), a CTE's lazy
//! optimization from `LogicalCTE.DeriveStats`, and `physicalOptimize` with
//! `recordUsedItemStatsStatus`.

use crate::resolve_indices::ResolveIndices;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};
use tidb_planner_core_base::PlanCacheMarker;

use crate::find_best_task::coster::{CostEnv, Ver2Coster};
use crate::find_best_task::dispatch::{DispatchContext, find_best_task};
use crate::rule::{logical_optimize, normalize_optimize};
use tidb_fixcontrol::OptimizerFixControl;
use tidb_planner_core_base::ColumnIdAllocator;
use tidb_planner_core_base::{PlanError, PlanIdAllocator};
use tidb_planner_core_operator_logicalop::plan_context::StatisticsLoadRequester;
use tidb_planner_core_operator_logicalop::plan_context::{
    CteOptimizer, DeriveStatsSink, DisabledLogicalRules, HintWarningSink, RuleContext,
    UsedStatsRecorder,
};
use tidb_planner_core_operator_logicalop::{LogicalPlan, prepare_possible_properties};
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_planner_core_operator_physicalop::enforce::MppWarningSink;
use tidb_planner_core_rule::flags;
use tidb_planner_property::PhysicalProperty;

/// Go `base.PlanContext` as the optimizer reads it: the statement's session
/// settings (`SessionVars`), its warnings and phase clocks (`StmtCtx`).
pub trait OptimizeContext:
    tidb_exprctx::Columns + PlanCacheMarker + HintWarningSink + MppWarningSink + DeriveStatsSink
{
    /// Go `SessionVars.Location()` as the expression resolver reads it.
    fn session_zone(&self) -> tidb_datatype::SessionTimeZone;
    /// Go `SessionVars.RUV2Metrics` of the statement being planned.
    fn ruv2_metrics(&self) -> Option<std::sync::Arc<tidb_execdetails::ruv2_metrics::RUV2Metrics>> {
        None
    }
    /// Go `!SessionVars.UseDynamicPartitionPrune()`.
    fn static_partition_prune(&self) -> bool;
    /// Go `SessionVars.EnableStableResultMode`.
    fn stable_result_mode(&self) -> bool;
    /// The cost model's session inputs.
    fn optimizer_cost_env(&self) -> &CostEnv;
    /// Go `SessionVars.AllowDeriveTopN`.
    fn allow_derive_topn(&self) -> bool;
    /// Go `optRuleBlacklist`: whether a logical rule is disabled.
    fn logical_rule_disabled(&self, name: &str) -> bool;
    /// Go `SessionVars.OptIndexPruneThreshold`.
    fn opt_index_prune_threshold(&self) -> i32;
    /// Go `SessionVars.OptPrefixIndexSingleScan`.
    fn opt_prefix_index_single_scan(&self) -> bool;
    /// Go `SessionVars.GetAllowPreferRangeScan()`.
    fn allow_prefer_range_scan(&self) -> bool;
    /// Go `SessionVars`' inputs to the index-lookup-pushdown support check.
    fn index_lookup_push_down_session(&self) -> tidb_sessionctx::IndexLookupPushDownSession;
    /// Go `SessionVars.IsolationReadEngines`.
    fn isolation_read_engines(&self) -> &str;
    /// Go `StmtCtx.TiFlashEngineRemovedDueToStrictSQLMode`.
    fn tiflash_engine_removed_due_to_strict_sql_mode(&self) -> bool;
    /// Go `SessionVars`' inputs to `handleFineGrainedShuffle`.
    fn tiflash_fine_grained_shuffle(&self) -> tidb_sessionctx::TiFlashFineGrainedShuffleSession;
    /// Go `GetMPPClient().GetMPPStoreCount()`.
    fn get_mpp_store_count(&self) -> Result<usize, String>;
    /// The TiFlash cluster `handleFineGrainedShuffle` sizes streams from.
    fn tiflash_cluster_info(
        &self,
    ) -> Option<&std::sync::Arc<dyn tidb_sessionctx::TiFlashClusterInfo>>;
    /// Go `SessionVars.GetEnableIndexMerge()`.
    fn get_enable_index_merge(&self) -> bool;
    /// Go `StmtCtx.NoIndexMergeHint`.
    fn no_index_merge_hint(&self) -> bool;
    /// Go `StmtCtx.InExplainStmt`.
    fn in_explain_stmt(&self) -> bool;
    /// Go `SessionVars.RangeMaxSize`.
    fn range_max_size(&self) -> i64;
    /// Go `SessionVars.RangeMaxCount`.
    fn range_max_count(&self) -> i64;
    /// Go `SessionVars.SelectivityFactor`.
    fn selectivity_factor(&self) -> f64;
    /// Go's range-fallback warning hook (`StmtCtx.RecordRangeFallback`).
    fn range_fallback_handler(&self) -> &tidb_context::RangeFallbackHandler;
    /// Go `StmtCtx.MemTracker`: the statement tracker the planner charges
    /// (`indexJoinPathBuildTmpRange`).
    fn stmt_mem_tracker(&self) -> std::sync::Arc<tidb_memory::Tracker>;
    /// Go `SessionVars.AlwaysKeepJoinKey`.
    fn always_keep_join_key(&self) -> bool;
    /// Go `SessionVars.EnableUnsafeSubstitute`.
    fn enable_unsafe_substitute(&self) -> bool;
    /// Go `SessionVars.EnableSemiJoinRewrite`.
    fn enable_semi_join_rewrite(&self) -> bool;
    /// Go `SessionVars.OptimizerEnableNAAJ`.
    fn optimizer_enable_naaj(&self) -> bool;
    /// Go `SessionVars.EnableNoDecorrelateInSelect`.
    fn enable_no_decorrelate_in_select(&self) -> bool;
    /// Go `SessionVars.OptimizerFixControl`.
    fn optimizer_fix_control(&self) -> &OptimizerFixControl;
    /// Go `SessionVars.TiDBOptJoinReorderThreshold`.
    fn tidb_opt_join_reorder_threshold(&self) -> i32;
    /// Go `SessionVars.AllowAggPushDown`.
    fn allow_agg_push_down(&self) -> bool;
    /// Go `SessionVars.TiDBOptEnableAdvancedJoinReorder`.
    fn tidb_opt_enable_advanced_join_reorder(&self) -> bool;
    /// Go `SessionVars.CartesianJoinOrderThreshold`.
    fn cartesian_join_order_threshold(&self) -> f64;
    /// Go `SessionVars.TiDBOptJoinReorderThroughProj`.
    fn tidb_opt_join_reorder_through_proj(&self) -> bool;
    /// Go `SessionVars.TiDBOptJoinReorderThroughSel`.
    fn tidb_opt_join_reorder_through_sel(&self) -> bool;
    /// Go `SessionVars.EnableOuterJoinReorder`.
    fn enable_outer_join_reorder(&self) -> bool;
    /// Go `SessionVars.EnableAdvancedJoinHint`.
    fn enable_advanced_join_hint(&self) -> bool;
    /// Go `StmtCtx.GetIndexForce()`.
    fn get_index_force(&self) -> bool;
    /// Go `SessionVars.EnableSkewDistinctAgg`.
    fn enable_skew_distinct_agg(&self) -> bool;
    /// Go `SessionVars.Enable3StageDistinctAgg`.
    fn enable_3_stage_distinct_agg(&self) -> bool;
    /// Go `SessionVars.Enable3StageMultiDistinctAgg`.
    fn enable_3_stage_multi_distinct_agg(&self) -> bool;
    /// Go `SessionVars.TiFlashPreAggMode`.
    fn tiflash_pre_agg_mode(&self) -> &str;
    /// Go `SessionVars.OptPartialOrderedIndexForTopN`.
    fn is_partial_ordered_index_for_top_n_enabled(&self) -> bool;
    /// Go `SessionVars.OptOrderingIdxSelRatio`.
    fn opt_ordering_idx_sel_ratio(&self) -> f64;
    /// Go `SessionVars.IndexJoinMaxScanRowsRatio`.
    fn index_join_max_scan_rows_ratio(&self) -> f64;
    /// Go `SessionVars.OptOrderingIdxSelThresh`.
    fn opt_ordering_idx_sel_thresh(&self) -> f64;
    /// Go `SessionVars.AllowProjectionPushDown`.
    fn allow_projection_push_down(&self) -> bool;
    /// Go `SessionVars.AllowDistinctAggPushDown`.
    fn allow_distinct_agg_push_down(&self) -> bool;
    /// Go `SessionVars.EnableCascadesPlanner`: `doOptimize` takes
    /// `CascadesOptimize` instead of `VolcanoOptimize`.
    fn enable_cascades_planner(&self) -> bool;
    /// Go `SessionVars.EnableINLJoinInnerMultiPattern`.
    fn enable_inl_join_inner_multi_pattern(&self) -> bool;
    /// Go `SessionVars.LimitPushDownThreshold`.
    fn limit_push_down_threshold(&self) -> u64;
    /// Go `!SessionVars.DisableHashJoin`.
    fn enable_hash_join(&self) -> bool;
    /// Go `SessionVars.ApplyCacheCapacity`.
    fn apply_cache_capacity(&self) -> i64;
    /// Go `SessionVars.EnableParallelApply`.
    fn enable_parallel_apply(&self) -> bool;
    /// Go `SessionVars.ExecutorConcurrency`.
    fn executor_concurrency(&self) -> usize;
    /// Go `SessionVars.TxnCtx.IsPessimistic || ...`: whether a transaction
    /// is active (the UnionScan elimination reads it).
    fn in_transaction(&self) -> bool;
    /// Go `SessionVars.IsAllocValid()`.
    fn is_alloc_valid(&self) -> bool;
    /// Go `SessionVars.SetAlloc(nil)` / `ClearAlloc`.
    fn set_alloc(&self, valid: bool);
    /// Go `SessionVars.MaxChunkSize`.
    fn max_chunk_size(&self) -> usize;
    /// Go `StmtCtx.AppendWarning` / `AppendNote` for a task warning.
    fn append_task_warning(&self, note: bool, code: u16, message: &str);
    /// Go `DurationOptimizer.LogicalOpt`.
    fn set_logical_opt(&self, duration: Duration);
    /// Go `DurationOptimizer.StatsDerive`, from `begin`.
    fn finish_stats_derive(&self, begin: Instant);
    /// The clock `physicalOptimize` starts before `RecursiveDeriveStats`, if
    /// the logical half started it.
    fn take_begin_physical_opt(&self) -> Option<Instant>;
    /// Go `DurationOptimizer.PhysicalOpt`.
    fn set_physical_opt(&self, duration: Duration);
}

/// What the optimizer reaches below the planner: the statistics handle's
/// loading, a partitioned table's pruning, and a CTE's lazy `DoOptimize`.
pub struct OptimizeServices<'a> {
    /// Go's statistics loading (`CollectPredicateColumnsPoint`,
    /// `SyncWaitStatsLoadPoint`) and `DataSource.InitStats`.
    pub statistics_load: &'a dyn StatisticsLoadRequester,
    /// The statistics initialization `DoOptimize` runs before the rule
    /// list, with used-item recording suspended for its walk.
    pub initialize_statistics:
        &'a dyn Fn(LogicalPlan) -> Result<LogicalPlan, (LogicalPlan, PlanError)>,
    /// Go `recordUsedItemStatsStatus`.
    pub used_stats_recorder: &'a dyn UsedStatsRecorder,
    /// Go `LogicalCTE.DeriveStats`'s `DoOptimize`.
    pub cte_optimizer: &'a dyn CteOptimizer,
}

/// Go `adjustOptimizationFlags` (`VolcanoOptimize`, run for the statement
/// and for each CTE part): stable result mode adds `ResultReorder`, an
/// `FTS_MATCH_WORD()` under the starter deployment adds the four full-text
/// index resolvers, the statistics points are on, and `FlagPruneColumnsAgain`
/// follows.
#[must_use]
pub fn adjust_optimization_flags<C: OptimizeContext>(mut flags: u64, ctx: &C) -> u64 {
    if ctx.stable_result_mode() {
        flags |= flags::STABILIZE_RESULTS;
    }
    if ctx.fts_function_is_used() && tidb_config_deploymode::is_starter() {
        flags |= flags::FULL_TEXT_INDEX_RESOLVE_WHERE
            | flags::FULL_TEXT_INDEX_RESOLVE_TOPN
            | flags::FULL_TEXT_INDEX_RESOLVE_PROJECTION
            | flags::FULL_TEXT_INDEX_RESOLVE_REJECT;
    }
    // Go enables both statistics rules for every ordinary (non-restricted)
    // statement; the builder never owns these flags because they are
    // session/execution policy, not AST shape.
    flags |= flags::COLLECT_PREDICATE_COLUMNS_POINT | flags::SYNC_WAIT_STATS_LOAD_POINT;
    // A second column-prune pass whenever a rule after column pruning runs.
    crate::rule::add_second_column_prune(flags)
}

/// Go `DoOptimize`'s logical half over a built plan.
///
/// # Errors
///
/// A rule's or the statistics derivation's error.
#[allow(clippy::too_many_arguments)]
pub fn optimize_logical<C: OptimizeContext>(
    plan: LogicalPlan,
    flags: u64,
    ctx: &C,
    services: &OptimizeServices<'_>,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
) -> Result<LogicalPlan, PlanError> {
    let flags = if ctx.static_partition_prune() {
        flags
    } else {
        flags & !flags::PARTITION_PROCESSOR
    };
    let flags = adjust_optimization_flags(flags, ctx);
    let function_builder = tidb_expr::expr_util::RealFunctionBuilder::new(ctx);
    let optimizer_cost_env = ctx.optimizer_cost_env();
    let rule_context = RuleContext {
        estimator_options: optimizer_cost_env.session.estimator_options.clone(),
        allocator: plan_ids,
        column_allocator: column_ids,
        builder: &function_builder,
        use_plan_cache,
        eval_context: ctx,
        plan_cache_marker: Some(ctx),
        allow_derive_topn: ctx.allow_derive_topn(),
        expr_pushdown_blacklist: ctx.expr_pushdown_blacklist().clone(),
        disabled_rules: DisabledLogicalRules::from_names(
            crate::rule::OPT_RULE_LIST
                .iter()
                .map(|rule| rule.name())
                .filter(|name| ctx.logical_rule_disabled(name)),
        ),
        statistics_load: Some(services.statistics_load),
        opt_index_prune_threshold: ctx.opt_index_prune_threshold(),
        opt_prefix_index_single_scan: ctx.opt_prefix_index_single_scan(),
        index_merge_enabled: ctx.get_enable_index_merge(),
        no_index_merge_hint: ctx.no_index_merge_hint(),
        in_explain_stmt: ctx.in_explain_stmt(),
        range_max_size: ctx.range_max_size(),
        selectivity_factor: ctx.selectivity_factor(),
        range_fallback_handler: Some(ctx.range_fallback_handler()),
        always_keep_join_key: ctx.always_keep_join_key(),
        enable_unsafe_substitute: ctx.enable_unsafe_substitute(),
        enable_semi_join_rewrite: ctx.enable_semi_join_rewrite(),
        enable_null_aware_anti_join: ctx.optimizer_enable_naaj(),
        enable_no_decorrelate_in_select: ctx.enable_no_decorrelate_in_select(),
        allow_eliminate_apply: ctx
            .optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_45822, true),
        prefer_index_merge_by_fix_control: ctx
            .optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_52869, false),
        index_merge_skip_range_scan_check: ctx
            .optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_52869, true),
        index_lookup_push_down_session: ctx.index_lookup_push_down_session(),
        isolation_read_engines: ctx.isolation_read_engines(),
        tiflash_engine_removed_due_to_strict_sql_mode: ctx
            .tiflash_engine_removed_due_to_strict_sql_mode(),
        join_reorder_threshold: ctx.tidb_opt_join_reorder_threshold(),
        group_ndv_skew_ratio: optimizer_cost_env.session.group_ndv_skew_ratio,
        scale_ndv_skew_ratio: optimizer_cost_env.session.scale_ndv_skew_ratio,
        allow_agg_push_down: ctx.allow_agg_push_down(),
        advanced_join_reorder: ctx.tidb_opt_enable_advanced_join_reorder(),
        cartesian_join_order_threshold: ctx.cartesian_join_order_threshold(),
        join_reorder_through_proj: ctx.tidb_opt_join_reorder_through_proj(),
        join_reorder_through_sel: ctx.tidb_opt_join_reorder_through_sel(),
        outer_join_reorder: ctx.enable_outer_join_reorder(),
        advanced_join_hint: ctx.enable_advanced_join_hint(),
        hint_warning_sink: Some(ctx),
        used_stats_recorder: Some(services.used_stats_recorder),
        ruv2_metrics: ctx.ruv2_metrics(),
        derive_stats_sink: Some(ctx),
        mpp_enforced: optimizer_cost_env.session.mpp_enforced,
        cte_optimizer: Some(services.cte_optimizer),
        data_source_stats: &crate::stats::CoreDataSourceStats,
        predicate_simplifier:
            &tidb_planner_core_rule::rule_predicate_simplification::RulePredicateSimplifier,
        access_path_resolver: &crate::access_path::CoreAccessPathResolver,
    };
    // Go DataSource.InitStats is available to logical rules themselves;
    // join reorder derives candidate statistics while logical optimization
    // is still running. Attach real-or-pseudo base statistics before entering
    // that rule list, rather than delaying them until physical optimization.
    let mut plan = (services.initialize_statistics)(plan).map_err(|(_, error)| error)?;
    // Go `core.RecheckCTE(logic)` (`optimize.go:553`): fill
    // `IsOuterMostCTE` on every CTE class before ANY optimization runs --
    // `LogicalCTE.PredicatePushDown` refuses to record predicates for a
    // non-outermost CTE.
    tidb_planner_core_operator_logicalop::cte::recheck_cte(&mut plan);
    // Go `planner/optimize.go:525`: the result's field names are the logical
    // plan's `OutputNames()` captured BEFORE optimization. A rule may replace
    // the root (an always-false filter makes a TableDual) with a node that
    // names nothing.
    let built_names = plan.output_names().to_vec();
    // No CTE class is optimized here: Go's `LogicalCTE.DeriveStats` runs
    // `DoOptimize` the first time a reference's statistics are derived, which
    // `rule_context.cte_optimizer` does from inside the stats walk.
    // Go `doOptimize`: `CascadesOptimize` normalizes with
    // `normalizeOptimize` (which keeps no optimizer clock) where
    // `VolcanoOptimize` runs `logicalOptimize`.
    let cascades = ctx.enable_cascades_planner();
    let mut optimized = if cascades {
        normalize_optimize(&rule_context, flags, plan)
            .map_err(|(_, error)| error)?
            .plan
    } else {
        let begin_logical_opt = Instant::now();
        let logical_result = logical_optimize(&rule_context, flags, plan);
        // Go `logicalOptimize`'s deferred `DurationOptimizer.LogicalOpt`.
        ctx.set_logical_opt(begin_logical_opt.elapsed());
        logical_result.map_err(|(_, error)| error)?.plan
    };
    // SyncWaitStatsLoadPoint has initialized ordinary and statically pruned
    // sources from the loaded snapshot before join reorder.
    optimized = check_partial_index_paths(optimized, ctx, use_plan_cache);
    if !ctx.static_partition_prune() {
        attach_dynamic_partition_access(&mut optimized, &rule_context)?;
    }
    if optimized.output_names().is_empty()
        && optimized
            .schema()
            .is_some_and(|schema| schema.columns.len() == built_names.len())
    {
        optimized.set_output_names(built_names);
    }
    if cascades {
        // Go `cascades.NewOptimizer` -> `Memo.CopyIn`: each new group
        // expression derives its group's logical property
        // (`DeriveLogicalProp`: the operator's `DeriveStats` with nil
        // reloads, then `PreparePossibleProperties`), bottom-up.
        optimized.memo_derive_stats_with_context(&rule_context)?;
        return Ok(prepare_possible_properties_for(ctx, optimized));
    }
    // Go `physicalOptimize` starts its clock before `RecursiveDeriveStats`
    // and records `StatsDerive` after it; the physical search that follows
    // this function takes the start over.
    let begin_physical_opt = Instant::now();
    optimized.recursive_derive_stats_with_context(&[], &rule_context)?;
    ctx.finish_stats_derive(begin_physical_opt);
    let logical = prepare_possible_properties_for(ctx, optimized);
    Ok(logical)
}

/// Go `preparePossibleProperties` with the session's reads: TiFlash in
/// `IsolationReadEngines`, and `IsMPPAllowed()`.
pub fn prepare_possible_properties_for<C: OptimizeContext>(
    ctx: &C,
    plan: LogicalPlan,
) -> LogicalPlan {
    let tiflash_in_isolation_read = crate::access_path::IsolationRead::new(
        ctx.isolation_read_engines(),
        ctx.tiflash_engine_removed_due_to_strict_sql_mode(),
    )
    .tiflash;
    prepare_possible_properties(
        plan,
        tiflash_in_isolation_read,
        ctx.optimizer_cost_env().session.mpp_allowed,
    )
    .0
}

/// Go `DataSource.CheckPartialIndexes` over every source, which Go's
/// `DataSource.DeriveStats` runs with the statement's resolver.
pub fn check_partial_index_paths<C: OptimizeContext>(
    plan: LogicalPlan,
    ctx: &C,
    use_plan_cache: bool,
) -> LogicalPlan {
    use tidb_planner_core_operator_logicalop::fold::{Descend, OwnedRewrite, fold_owned};

    struct PartialIndexChecker<'a> {
        resolver: &'a dyn tidb_expr::column_resolver::ColumnResolver,
        use_plan_cache: bool,
        opt_prefix_index_single_scan: bool,
    }

    impl OwnedRewrite for PartialIndexChecker<'_> {
        type Down = ();
        type Up = ();

        fn descend(&mut self, node: &mut LogicalPlan, (): ()) -> Descend<(), ()> {
            if let LogicalPlan::DataSource(source) = node {
                source.check_partial_indexes(
                    self.resolver,
                    self.use_plan_cache,
                    self.opt_prefix_index_single_scan,
                );
            }
            Descend::Children(vec![(); node.children().len()])
        }

        fn ascend(&mut self, node: LogicalPlan, _child_ups: Vec<()>) -> (LogicalPlan, ()) {
            (node, ())
        }
    }

    // Go parses the predicate with `ds.SCtx().GetExprCtx()`: the session's
    // SQL mode and zone fold its constants as they fold the filters'.
    let resolver = tidb_expr::column_resolver::ContextResolver(ctx);
    fold_owned(
        &mut PartialIndexChecker {
            resolver: &resolver,
            use_plan_cache,
            opt_prefix_index_single_scan: ctx.opt_prefix_index_single_scan(),
        },
        plan,
        (),
    )
    .0
}

/// Go's dynamic-prune access object: under dynamic pruning, each
/// partitioned source names the partitions it reads (for EXPLAIN's access
/// object), computed by the partition processor's pruning.
fn attach_dynamic_partition_access(
    plan: &mut LogicalPlan,
    rule_context: &RuleContext<'_>,
) -> Result<(), PlanError> {
    if let LogicalPlan::DataSource(source) = plan {
        if !source.partition_definition_ids().is_empty() {
            let indices =
                tidb_planner_core_rule::rule_partition_processor::prune::partition_indices(
                    rule_context,
                    source,
                )?;
            let all_partitions = source.partition_names.is_empty()
                && indices.len() == source.partition_definition_ids().len();
            let partitions = if all_partitions {
                Vec::new()
            } else {
                indices
                    .into_iter()
                    .filter_map(|index| source.partition_definition_names().get(index).cloned())
                    .collect()
            };
            source.dynamic_partition_access =
                Some(tidb_planner_core_access::DynamicPartitionAccessObject {
                    database: source.db_name.clone(),
                    table: source
                        .table_as_name
                        .clone()
                        .unwrap_or_else(|| source.table_name()),
                    all_partitions,
                    partitions,
                    error: String::new(),
                });
        }
        return Ok(());
    }
    for child in plan.base_mut().children_mut() {
        attach_dynamic_partition_access(child, rule_context)?;
    }
    Ok(())
}

/// Go `physicalOptimize` and `postOptimize`: the cheapest physical plan for
/// the optimized logical plan with its cost (`getPlanCost` of the best task's
/// plan), then the post-optimization rewrites.
/// `col_ref_from_update_plan` is Go's `StmtCtx.ColRefFromUpdatePlan` for an
/// UPDATE's read plan (empty otherwise).
///
/// # Errors
///
/// No physical plan found, or a resolution error.
pub fn physical_optimize<C: OptimizeContext>(
    logical: &LogicalPlan,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    ctx: &C,
    col_ref_from_update_plan: &BTreeSet<i64>,
    used_stats_recorder: &dyn UsedStatsRecorder,
) -> Result<(PhysicalPlan, f64), PlanError> {
    let begin_physical_opt = ctx.take_begin_physical_opt().unwrap_or_else(Instant::now);
    let optimizer_cost_env = ctx.optimizer_cost_env();
    let coster = Ver2Coster::from_env(optimizer_cost_env)
        .with_scan_penalty_vars(ctx.allow_prefer_range_scan(), ctx.get_index_force());
    let evaluate = tidb_ranger::points::StatementRangerEvaluator(ctx);
    let get_mpp_store_count = || ctx.get_mpp_store_count();
    let mut dispatch = DispatchContext::new(
        plan_ids,
        &coster,
        optimizer_cost_env.session.scale_ndv_skew_ratio,
    )
    .with_mpp_store_count(&get_mpp_store_count)
    .with_used_stats_recorder(used_stats_recorder)
    .with_estimator_options(optimizer_cost_env.session.estimator_options.clone())
    .with_group_ndv_skew_ratio(optimizer_cost_env.session.group_ndv_skew_ratio)
    .with_correlation_options(optimizer_cost_env.session.correlation_options)
    .with_expression_evaluator(&evaluate)
    .with_mpp_allowed(optimizer_cost_env.session.mpp_allowed)
    .with_tiflash_switches(
        optimizer_cost_env.session.tiflash_cop_banned,
        optimizer_cost_env.session.tiflash_fast_scan,
        optimizer_cost_env.session.allow_batch_cop,
        optimizer_cost_env.session.disaggregated_tiflash,
    )
    .with_enable_late_materialization(optimizer_cost_env.session.enable_late_materialization)
    .with_mpp_join_inputs(
        optimizer_cost_env.session.mpp_join,
        crate::access_path::IsolationRead::new(
            ctx.isolation_read_engines(),
            ctx.tiflash_engine_removed_due_to_strict_sql_mode(),
        )
        .tiflash,
        ctx.tiflash_engine_removed_due_to_strict_sql_mode(),
        optimizer_cost_env.session.enable_chunk_rpc,
    )
    .with_enable_skew_distinct_agg(ctx.enable_skew_distinct_agg())
    .with_enable_3_stage_distinct_agg(ctx.enable_3_stage_distinct_agg())
    .with_enable_3_stage_multi_distinct_agg(ctx.enable_3_stage_multi_distinct_agg())
    .with_tiflash_pre_agg_mode(ctx.tiflash_pre_agg_mode())
    .with_partial_ordered_index_for_topn(ctx.is_partial_ordered_index_for_top_n_enabled())
    .with_opt_prefix_index_single_scan(ctx.opt_prefix_index_single_scan())
    .with_allow_prefer_range_scan(ctx.allow_prefer_range_scan())
    .with_range_max_count(ctx.range_max_count())
    // Go `compareCandidates`: `fixcontrol.GetIntWithDefault(..., Fix45132, 1000)`.
    .with_index_join_skyline_threshold(
        ctx.optimizer_fix_control()
            .get_int_with_default(tidb_fixcontrol::FIX_45132, 1000) as f64,
    )
    .with_prefer_index_merge_by_fix_control(
        ctx.optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_52869, false),
    )
    .with_explore_enforced_plan(
        ctx.optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_46177, true),
    )
    .with_mpp_warning_sink(ctx)
    .with_for_update_read_session(
        tidb_exprctx::Columns::txn_is_explicit(ctx),
        crate::access_path::IsolationRead::new(
            ctx.isolation_read_engines(),
            ctx.tiflash_engine_removed_due_to_strict_sql_mode(),
        ),
    )
    .with_plan_cache(
        tidb_exprctx::Columns::use_plan_cache(ctx),
        ctx,
        ctx.optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_44830, false),
    )
    .with_range_quota(ctx.range_max_size(), ctx.range_fallback_handler())
    .with_stmt_mem_tracker(ctx.stmt_mem_tracker())
    .with_selectivity_factor(ctx.selectivity_factor())
    .with_ordering_index_selectivity_ratio(ctx.opt_ordering_idx_sel_ratio())
    .with_index_join_max_scan_rows_ratio(ctx.index_join_max_scan_rows_ratio())
    .with_ordering_index_selectivity_threshold(ctx.opt_ordering_idx_sel_thresh())
    .with_projection_push_down(ctx.allow_projection_push_down())
    .with_distinct_agg_push_down(ctx.allow_distinct_agg_push_down())
    .with_heavy_function_optimize(
        ctx.optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_56318, true),
    )
    .with_inl_join_inner_multi_pattern(ctx.enable_inl_join_inner_multi_pattern())
    .with_limit_push_down_threshold(ctx.limit_push_down_threshold())
    .with_paging(optimizer_cost_env.session.enable_paging)
    .with_hash_join_concurrency(optimizer_cost_env.session.hash_join_concurrency.max(1.0) as usize)
    .with_use_hash_join_v2(optimizer_cost_env.session.use_hash_join_v2)
    .with_disable_hash_join(!ctx.enable_hash_join())
    .with_apply_cache_capacity(ctx.apply_cache_capacity())
    .with_point_get_conversion(
        !ctx.optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_52592, false),
    )
    .with_index_join_row_count_upper_bound(
        ctx.optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_44855, false),
    )
    .with_column_ids(column_ids);
    dispatch.expr_pushdown_blacklist = ctx.expr_pushdown_blacklist().clone();
    dispatch.shuffle_options = optimizer_cost_env.session.shuffle_options;
    let cascades = ctx.enable_cascades_planner();
    let task = if cascades {
        // Go `CascadesOptimize`: `cascades.NewOptimizer(logic)`, `Execute()`,
        // then `impl.ImplementMemoAndCost(rootGroup)`, which keeps no
        // optimizer clock.
        let mut cas = crate::cascades::Optimizer::new(logical, plan_ids)?;
        cas.execute()?;
        let root_group = cas
            .get_memo()
            .get_root_group()
            .expect("an initialized memo has a root group");
        crate::cascades::impl_and_cost::implement_memo_and_cost(
            cas.get_memo(),
            root_group,
            &mut dispatch,
        )?
    } else {
        // Go `physicalOptimize` records `DurationOptimizer.PhysicalOpt` on
        // every return, an error one included.
        find_best_task(logical, &PhysicalProperty::default(), &mut dispatch)
            .inspect_err(|_| ctx.set_physical_opt(begin_physical_opt.elapsed()))?
    };
    let mut physical =
        task.plan().cloned().ok_or_else(|| {
            PlanError::from(tidb_plannererrors::ERR_INTERNAL.gen_with_stack_by_args(&[
                "Can't find a proper physical plan for this query".into(),
            ]))
        })?;
    // Go physicalOptimize: "cost, err = getPlanCost(t.Plan(), ...)". Each
    // operator keeps the cost it was first priced with (`PlanCostInit`), while
    // `findBestTask` compared tasks or here, and EXPLAIN shows that cost even
    // where `postOptimize` later puts an operator between a parent and its
    // child.
    let cost = coster
        .plan_cost(
            &physical,
            tidb_planner_property::task_type::TaskType::Root,
            false,
        )
        .value();
    // Go physicalOptimize: "collect the warnings from task". Every task
    // warning is a planner `ErrInternal` (hint warnings), code 1815.
    for warning in task.warnings().get_warnings() {
        let note = matches!(warning.level, tidb_context::WarningLevel::Note);
        ctx.append_task_warning(note, 1815, &warning.message);
    }
    // Go physicalOptimize binds positions before postOptimize removes aliases.
    let resolved = physical.resolve_indices();
    if !cascades {
        ctx.set_physical_opt(begin_physical_opt.elapsed());
    }
    resolved?;
    let physical = crate::post_optimize::eliminate_physical_projection_for_update(
        physical,
        col_ref_from_update_plan,
    );
    // Go postOptimize: eliminatePhysicalProjection -> InjectExtraProjection.
    // The projection re-injection restores the purposeful projections
    // (scalar aggregate arguments, scalar order-by items, expression
    // nominal sorts) that the elimination pass removed.
    let mut physical = crate::inject_extra_projection::inject_extra_projection(
        ctx, physical, plan_ids, column_ids,
    );
    // Go postOptimize: mergeContinuousSelections.
    crate::post_optimize::merge_continuous_selections(&mut physical);
    // Go postOptimize: eliminateUnionScanAndLock (the UnionScan half).
    let mut physical =
        crate::post_optimize::eliminate_union_scan_and_lock(physical, ctx.in_transaction());
    // Go postOptimize: avoidColumnEvaluatorForProjBelowUnion.
    crate::post_optimize::avoid_column_evaluator_for_proj_below_union(&mut physical);
    tidb_planner_core_operator_physicalop::shuffle::install_receivers(&mut physical, plan_ids)?;
    if ctx.enable_parallel_apply() {
        crate::post_optimize::enable_parallel_apply(ctx, &mut physical);
    }
    // Go postOptimize: handleFineGrainedShuffle.
    crate::fine_grained_shuffle::handle_fine_grained_shuffle(ctx, &mut physical);
    // Go postOptimize: countStarRewrite.
    crate::post_optimize::count_star_rewrite(&mut physical, column_ids);
    // Go postOptimize: disableReuseChunkIfNeeded.
    if ctx.is_alloc_valid()
        && crate::chunk_reuse::should_disable_reuse_chunk(
            &physical,
            ctx.max_chunk_size(),
            tidb_memory::mem_total().ok(),
        )
    {
        ctx.set_alloc(false);
    }
    physical
        .base_mut()
        .base
        .set_output_names(logical.output_names().to_vec());
    Ok((physical, cost))
}

use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;
use tidb_expr::expr_util::RealFunctionBuilder;
use tidb_expr::expr_util::normal_form::extract_filters_from_dn_fs;
use tidb_expr::simple_expr::compose_dnf_condition;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_planner_core_operator_logicalop::cte::CteClass;
use tidb_planner_core_operator_logicalop::fold::{Descend, OwnedRewrite, fold_owned};
use tidb_planner_core_operator_logicalop::{BaseLogicalPlan, LogicalSelection};
use tidb_planner_property::stats_info::StatsInfo;
use tidb_sessionctx::StmtContext;
use tidb_stats_handle::catalog_statistics::CatalogStatistics;
use tidb_stats_handle::stmt_ctx::StatsStmtCtx as _;

struct InitStats<'a> {
    error: Option<tidb_planner_core_base::PlanError>,
    catalog: &'a SessionExtendedInfoSchema,
    /// Go's statistics demand for this statement (`statistics_demand`), once
    /// `CollectPredicateColumnsPoint` has seen the plan: the columns and
    /// indexes whose payloads the synchronous load makes available. `None`
    /// before that point.
    demanded: Option<&'a std::collections::HashMap<tidb_model::TableItemID, bool>>,
    context: &'a StmtContext,
    /// Whether this walk records the statement's used statistics (Go
    /// `initStats`' `RecordUsedInfo` and `recordUsedItemStatsStatus`). Go
    /// initializes a source when its statistics are derived, after the
    /// partition processor; the walk this port runs before the logical rules
    /// would record a statically pruned table's logical id, so it does not.
    record_used_stats: bool,
}

/// Go `recordUsedItemStatsStatus` (`cardinality/trace.go`) over the table
/// `GetStatsTable` answered with. `statistics` is `None` for a fresh
/// `PseudoTable`, whose pseudo column and index objects carry a zero
/// `StatsLoadedStatus` -- `unInitialized` -- for every public, non-hidden
/// column and public index (`in_pseudo_table`), while anything else is a nil
/// object its all-false existence map reports `missing`. The table's ledger
/// entry carries what Go's `initStats` records for it: the pseudo version
/// (zero) and the `PseudoTable` counts.
pub fn record_used_item_stats_status(
    context: &StmtContext,
    table_name: &str,
    physical_table_id: i64,
    statistics: Option<&tidb_stats::Table>,
    in_pseudo_table: bool,
    item_id: i64,
    is_index: bool,
) {
    // Go `recordUsedItemStatsStatus`: `_tidb_rowid` (id -1) and other internal
    // pseudo columns have no statistics to report.
    if item_id <= 0 {
        return;
    }
    let Some(statistics) = statistics else {
        context.record_used_stats_status(
            physical_table_id,
            table_name,
            tidb_stats::PSEUDO_VERSION,
            tidb_stats::PSEUDO_ROW_COUNT,
            0,
            item_id,
            is_index,
            if in_pseudo_table {
                tidb_stats::StatsLoadedStatus::default().status_to_string()
            } else {
                "missing"
            },
        );
        return;
    };
    let loaded_status = if is_index {
        statistics
            .hist_coll
            .get_index(item_id)
            .map(|index| read(&index).stats_loaded_status)
    } else {
        statistics
            .hist_coll
            .get_column(item_id)
            .map(|column| read(&column).stats_loaded_status)
    };
    let analyzed = statistics
        .existence_map
        .as_ref()
        .is_some_and(|map| read(map).has_analyzed(item_id, is_index));
    if loaded_status.is_some_and(tidb_stats::StatsLoadedStatus::is_full_load) {
        return;
    }
    let status = loaded_status.map_or_else(
        || {
            if analyzed { "unInitialized" } else { "missing" }
        },
        tidb_stats::StatsLoadedStatus::status_to_string,
    );
    context.record_used_stats_status(
        physical_table_id,
        table_name,
        if statistics.hist_coll.pseudo {
            0
        } else {
            statistics.version
        },
        statistics.hist_coll.realtime_count,
        statistics.hist_coll.modify_count,
        item_id,
        is_index,
        status,
    );
}

fn read<T>(lock: &std::sync::RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl InitStats<'_> {
    /// The `(column ids, index ids)` Go's lite statistics initialization
    /// leaves loaded for this source: what `CollectPredicateColumnsPoint`'s
    /// demand (read off the plan after predicate pushdown) names for its
    /// physical table and the cache holds in full.
    ///
    /// `None` before that demand exists or when it names no column of this
    /// source, which leaves the caller on the cache's fully loaded subset.
    fn predicate_loaded_items(
        &self,
        source: &tidb_planner_core_operator_logicalop::DataSource,
        statistics: Option<&tidb_stats::Table>,
    ) -> Option<(BTreeSet<i64>, BTreeSet<i64>)> {
        statistics?;
        let demanded = self.demanded?;
        let mut columns = BTreeSet::new();
        let mut indexes = BTreeSet::new();
        for (item, full_load) in demanded {
            if item.table_id != source.physical_table_id {
                continue;
            }
            if item.is_index {
                indexes.insert(item.id);
            } else if *full_load {
                // Go `addPredicateColumnsFromExpressions(..., false)`: a join
                // key asks for the column's essential statistics only, so its
                // histogram stays evicted.
                columns.insert(item.id);
            }
        }
        if columns.is_empty() {
            return None;
        }
        // Payload shape is not load state: fully loaded TopN-only statistics
        // have no buckets, while evicted statistics retain metadata.
        // Use the same retained load status as the statistics cache.
        columns.retain(|id| {
            statistics
                .and_then(|statistics| statistics.hist_coll.get_column(*id))
                .is_some_and(|column| read(&column).is_full_load())
        });
        indexes.retain(|id| {
            statistics
                .and_then(|statistics| statistics.hist_coll.get_index(*id))
                .is_some_and(|index| read(&index).is_full_load())
        });
        if columns.is_empty() {
            return None;
        }
        Some((columns, indexes))
    }
}

impl InitStats<'_> {
    fn initialize_source(
        &mut self,
        node: &mut LogicalPlan,
    ) -> Result<Descend<(), ()>, tidb_planner_core_base::PlanError> {
        let LogicalPlan::DataSource(source) = node else {
            return Ok(Descend::Children(vec![(); node.children().len()]));
        };
        source.base.base.set_stats(None);
        source.possible_access_paths = None;
        for path in &mut source.all_possible_access_paths {
            // The fill's outputs; derivation fills them again.
            path.row_estimate = None;
            path.idx_cols.clear();
            path.ranges.clear();
            path.access_conds.clear();
            path.eq_cond_count = 0;
            path.eq_or_in_cond_count = 0;
            path.is_dnf_cond = false;
            path.min_access_conds_for_dnf_cond = 0;
            path.index_filters.clear();
            path.table_filters.clear();
            path.count_after_index = None;
            path.correlated_access_count = 0;
            path.const_col_ids.clear();
            path.is_single_scan = None;
        }
        source.access_path_min_selectivity = 1.0;
        // Go `deriveStats4DataSource`: `EliminateNoPrecisionLossCast` over the
        // pushed conditions before any index path is filled, so `cast(a AS
        // SIGNED) = 1` on an INT key -- a UNION branch's widening cast --
        // builds a point range.
        let builder = tidb_expr::expr_util::RealFunctionBuilder::new(self.context);
        for condition in &mut source.pushed_down_conds {
            *condition =
                tidb_expr::expr_util::eliminate_no_precision_loss_cast(condition, &builder);
        }
        // Go `initStats` calls `GetStatsTable(..., ds.PhysicalTableID)`: a
        // static-pruning child owns one physical partition's statistics,
        // while an ordinary/dynamic source keeps the logical table ID here.
        // `None` is the fresh `PseudoTable` -- a table without statistics,
        // and rule 2's answer to a zero `RealtimeCount`.
        let statistic_table = crate::stats::stats_table::get_stats_table(
            self.context,
            self.catalog,
            source.physical_table_id,
        );
        let statistics = statistic_table.as_deref();
        let row_count = crate::stats::stats_table::realtime_row_count(statistics);
        // Go `buildDataSource`: a table with an available TiFlash replica
        // marks the statement as reading TiFlash whenever MPP is allowed.
        if source.has_available_tiflash_replica()
            && crate::stmt_ctx::PlannerStmtCtx::optimizer_cost_env(self.context)
                .session
                .mpp_allowed
        {
            self.context.mark_is_tiflash();
        }
        // Go `initStats`: the statement's `UsedStatsInfo` entry for this
        // source's physical table, `PseudoVersion` (0) for a pseudo table.
        if self.record_used_stats {
            self.context.record_used_stats_info(
                source.physical_table_id,
                self.catalog
                    .get_tbl_info_for_used_stats_by_physical_id(source.physical_table_id),
                statistics
                    .filter(|statistics| !statistics.hist_coll.pseudo)
                    .map_or(0, |statistics| statistics.version),
                row_count as i64,
                statistics.map_or(0, |statistics| statistics.hist_coll.modify_count),
            );
        }
        // Retain Go's loaded subset, including empty and TopN-only payloads.
        // The cache's explicit status excludes evicted metadata from NDV
        // scaling and histogram estimates without inferring state from buckets.
        let predicate_loaded = self.predicate_loaded_items(source, statistics);
        // Go `ds.TableInfo`.
        let table_info = source
            .table_info
            .clone()
            .expect("a planned data source carries its TableInfo");
        crate::stats::init_stats::init_stats(
            source,
            &table_info.read(),
            statistic_table.as_ref(),
            row_count,
            predicate_loaded,
        );
        tidb_planner_core_rule::rule_collect_plan_stats::refresh_source_group_ndvs_with_scale_ratio(
            source,
            crate::stmt_ctx::PlannerStmtCtx::optimizer_cost_env(self.context)
                .session
                .scale_ndv_skew_ratio,
        );
        Ok(Descend::Stop(()))
    }
}

impl OwnedRewrite for InitStats<'_> {
    type Down = ();
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, (): Self::Down) -> Descend<Self::Down, Self::Up> {
        if self.error.is_some() {
            return Descend::Stop(());
        }
        let result = self.initialize_source(node);
        match result {
            Ok(descend) => descend,
            Err(error) => {
                self.error = Some(error);
                Descend::Stop(())
            }
        }
    }

    fn ascend(
        &mut self,
        mut node: LogicalPlan,
        _children: Vec<Self::Up>,
    ) -> (LogicalPlan, Self::Up) {
        // Clear EVERY node's cached derived statistics, DataSources included.
        // The sources' cached `stats_info` was derived from whatever histogram
        // snapshot was loaded when the first derive ran -- commonly BEFORE
        // `SyncWaitStatsLoadPoint` finishes the synchronous load (Go derives
        // lazily at physical time, after the wait, so it never caches a cold
        // estimate). Preserving the source's cached estimate here made every
        // downstream derive early-return the cold value: TPC-DS q7's
        // `eq(cd_education_status, 'Primary')` estimated 1.00 instead of
        // 276934.43 and collapsed the whole plan's row counts.
        node.set_stats(None);
        (node, ())
    }
}

/// Go `recordUsedItemStatsStatus` over the statement: the statistics item
/// an estimate of `physical_table_id` read, recorded against the table the
/// planner reads statistics from (`GetStatsTable`).
pub struct StatementUsedStats<'a> {
    /// The statement's infoschema.
    pub catalog: &'a SessionExtendedInfoSchema,
    /// The statement's context.
    pub context: &'a StmtContext,
}

impl tidb_planner_core_operator_logicalop::plan_context::UsedStatsRecorder
    for StatementUsedStats<'_>
{
    fn record_used_item(
        &self,
        physical_table_id: i64,
        table_name: &str,
        in_pseudo_table: bool,
        item_id: i64,
        is_index: bool,
    ) {
        let statistics = crate::stats::stats_table::get_stats_table(
            self.context,
            self.catalog,
            physical_table_id,
        );
        record_used_item_stats_status(
            self.context,
            table_name,
            physical_table_id,
            statistics.as_deref(),
            in_pseudo_table,
            item_id,
            is_index,
        );
    }

    fn used_info(
        &self,
        physical_table_id: i64,
    ) -> Option<tidb_stmtctx::used_stats::SharedUsedStatsInfo> {
        self.context.used_stats_info_shared(physical_table_id)
    }
}

/// Go `physicalOptimize` and `postOptimize` of an optimized logical plan.
///
/// # Errors
///
/// The optimization's failure.
pub fn physical_plan_for_logical(
    logical: &LogicalPlan,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    catalog: &SessionExtendedInfoSchema,
    ctx: &StmtContext,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    physical_plan_for_logical_with_update_refs(
        logical,
        plan_ids,
        column_ids,
        catalog,
        ctx,
        &std::collections::BTreeSet::new(),
    )
}

/// [`physical_plan_for_logical`] for an UPDATE's read plan: Go's
/// `StmtCtx.ColRefFromUpdatePlan`, the columns its assignments read, keeps
/// the projection that names them.
pub fn physical_plan_for_logical_with_update_refs(
    logical: &LogicalPlan,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    catalog: &SessionExtendedInfoSchema,
    ctx: &StmtContext,
    col_ref_from_update_plan: &std::collections::BTreeSet<i64>,
) -> Result<PhysicalPlan, tidb_planner_core_base::PlanError> {
    crate::optimizer::physical_optimize(
        logical,
        plan_ids,
        column_ids,
        ctx,
        col_ref_from_update_plan,
        &StatementUsedStats {
            catalog,
            context: ctx,
        },
    )
    .map(|(physical, _)| physical)
}

/// Go `utilfuncp.DoOptimize` as `LogicalCTE.DeriveStats` calls it
/// (`logical_cte.go:172`): the CTE class is optimized lazily, the first time a
/// reference's statistics are derived, with the DNF of every predicate its
/// references have recorded by then. It is never re-optimized.
struct StatementCteOptimizer<'a> {
    catalog: &'a SessionExtendedInfoSchema,
    ctx: &'a StmtContext,
    plan_ids: &'a PlanIdAllocator,
    column_ids: &'a ColumnIdAllocator,
    /// Classes being optimized right now; a class reaching itself is a cycle.
    visiting: RefCell<HashSet<usize>>,
    parallel_apply_allowed: std::cell::Cell<bool>,
}

impl tidb_planner_core_operator_logicalop::plan_context::CteOptimizer
    for StatementCteOptimizer<'_>
{
    fn optimize_cte(
        &self,
        class: &Arc<std::sync::RwLock<CteClass>>,
        seed_stat: Option<&Arc<std::sync::RwLock<StatsInfo>>>,
        rule_context: &RuleContext<'_>,
    ) -> Result<(), tidb_planner_core_base::PlanError> {
        if class.read().unwrap().seed_part_physical_plan.is_some() {
            return Ok(());
        }
        let identity = Arc::as_ptr(class) as usize;
        if !self.visiting.borrow_mut().insert(identity) {
            return Err(tidb_planner_core_base::PlanError::internal(
                "LogicalCTE.DeriveStats: cyclic CTE class optimization",
            ));
        }
        let result = self.optimize_class(class, seed_stat, rule_context);
        self.visiting.borrow_mut().remove(&identity);
        result
    }
}

impl StatementCteOptimizer<'_> {
    fn optimize_class(
        &self,
        class: &Arc<std::sync::RwLock<CteClass>>,
        seed_stat: Option<&Arc<std::sync::RwLock<StatsInfo>>>,
        rule_context: &RuleContext<'_>,
    ) -> Result<(), tidb_planner_core_base::PlanError> {
        let (mut seed, mut opt_flag, predicates) = {
            let class = class.read().unwrap();
            let seed = class
                .seed_part_logical_plan
                .as_deref()
                .ok_or_else(|| {
                    tidb_planner_core_base::PlanError::internal(
                        "LogicalCTE.DeriveStats: seed logical plan is nil",
                    )
                })?
                .deep_clone();
            (seed, class.opt_flag, class.push_down_predicates.clone())
        };
        // Go composes the predicates recorded by every reference as one DNF,
        // extracts common conjuncts, and puts that Selection above the seed
        // before running the CTE's own optimizer pass.
        if let Some(dnf) = compose_dnf_condition(predicates) {
            let conditions = extract_filters_from_dn_fs(vec![dnf]);
            let query_block_offset = seed.base().base.query_block_offset();
            let mut selection = LogicalSelection::new(
                BaseLogicalPlan::new(self.plan_ids, LogicalSelection::TYPE, query_block_offset),
                conditions,
            );
            selection.base.set_children(vec![seed]);
            seed = LogicalPlan::Selection(selection);
            opt_flag = tidb_planner_core_rule::set_predicate_push_down_flag(opt_flag);
            class.write().unwrap().opt_flag = opt_flag;
        }
        let (seed_logical, seed_physical) = self.optimize_tree(seed, opt_flag, rule_context)?;
        // Go `*p.SeedStat = *resStat` precedes the recursive DoOptimize.
        if let (Some(seed_stat), Some(stats)) = (seed_stat, seed_physical.stats_info()) {
            *seed_stat.write().unwrap() = stats.clone();
        }
        {
            let mut class = class.write().unwrap();
            class.seed_part_logical_plan = Some(Box::new(seed_logical));
            class.seed_part_physical_plan = Some(Box::new(seed_physical));
        }
        let recursive = {
            let class = class.read().unwrap();
            match (
                class.recursive_part_logical_plan.as_deref(),
                class.recursive_part_physical_plan.is_none(),
            ) {
                (Some(recursive), true) => Some(recursive.deep_clone()),
                _ => None,
            }
        };
        if let Some(recursive) = recursive {
            // Go temporarily disables parallel Apply throughout recursive
            // body optimization, including lazily optimized nested CTE parts.
            let previous = self.parallel_apply_allowed.replace(false);
            let optimized = self.optimize_tree(recursive, opt_flag, rule_context);
            self.parallel_apply_allowed.set(previous);
            let (logical, physical) = optimized?;
            let mut class = class.write().unwrap();
            class.recursive_part_logical_plan = Some(Box::new(logical));
            class.recursive_part_physical_plan = Some(Box::new(physical));
        }
        Ok(())
    }

    /// Go `DoOptimize` over one CTE part: logical rules, statistics (which
    /// optimize nested CTE classes lazily through the same hook), and the
    /// physical search.
    fn optimize_tree(
        &self,
        plan: LogicalPlan,
        opt_flag: u64,
        rule_context: &RuleContext<'_>,
    ) -> Result<(LogicalPlan, PhysicalPlan), tidb_planner_core_base::PlanError> {
        let context = self
            .ctx
            .clone()
            .with_parallel_apply(self.parallel_apply_allowed.get());
        let ctx = &context;
        // Go DataSource.DeriveStats initializes base statistics even when
        // called from a logical rule; CTE roots need the same preinitialization
        // as the outer query before join reorder can inspect their sources.
        let mut initializer = InitStats {
            error: None,
            catalog: self.catalog,
            demanded: None,
            context: ctx,
            record_used_stats: true,
        };
        let (plan, ()) = fold_owned(&mut initializer, plan, ());
        if let Some(error) = initializer.error {
            return Err(error);
        }
        let opt_flag = crate::optimizer::adjust_optimization_flags(opt_flag, ctx);
        // Go's CTE `DoOptimize` is `doOptimize`: `CascadesOptimize`
        // (`normalizeOptimize`, then the memo's derivation, no optimizer
        // clock) under `tidb_enable_cascades_planner`, else
        // `logicalOptimize` and `physicalOptimize`, which set the session's
        // optimizer clocks like any other pass.
        let cascades = crate::optimizer::OptimizeContext::enable_cascades_planner(ctx);
        let optimized = if cascades {
            crate::rule::normalize_optimize(rule_context, opt_flag, plan)
                .map_err(|(_, error)| error)?
                .plan
        } else {
            let begin_logical_opt = std::time::Instant::now();
            let logical_result = logical_optimize(rule_context, opt_flag, plan);
            ctx.phase_durations()
                .set_logical_opt(begin_logical_opt.elapsed());
            logical_result.map_err(|(_, error)| error)?.plan
        };
        let mut optimized = crate::optimizer::check_partial_index_paths(
            optimized,
            ctx,
            rule_context.use_plan_cache,
        );
        if cascades {
            optimized.memo_derive_stats_with_context(rule_context)?;
        } else {
            let begin_physical_opt = std::time::Instant::now();
            optimized.recursive_derive_stats_with_context(&[], rule_context)?;
            ctx.phase_durations()
                .finish_stats_derive(begin_physical_opt);
        }
        let logical = crate::optimizer::prepare_possible_properties_for(ctx, optimized);
        let physical =
            physical_plan_for_logical(&logical, self.plan_ids, self.column_ids, self.catalog, ctx)?;
        Ok((logical, physical))
    }
}

/// Go `DoOptimize`'s `logicalOptimize` over a built plan with `flags`,
/// loading the statistics its rules demand.
///
/// # Errors
///
/// The optimization's failure.
#[allow(clippy::too_many_arguments)]
pub fn optimize_built_logical(
    plan: LogicalPlan,
    flags: u64,
    catalog: &SessionExtendedInfoSchema,
    ctx: &StmtContext,
    use_plan_cache: bool,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    _session_zone: &tidb_datatype::SessionTimeZone,
) -> Result<LogicalPlan, tidb_planner_core_base::PlanError> {
    struct PlannerStatisticsLoad<'a> {
        catalog: &'a SessionExtendedInfoSchema,
        context: &'a StmtContext,
        /// The statement's accumulated statistics demand; see
        /// [`InitStats::demanded`]. A CTE body optimized on its own adds its
        /// own demand.
        demanded: RefCell<Option<std::collections::HashMap<tidb_model::TableItemID, bool>>>,
        /// See [`InitStats::record_used_stats`].
        record_used_stats: std::cell::Cell<bool>,
        /// Whether the planner's own statistics derivation records the items
        /// it reads. Go records them when it derives a source, after the
        /// partition processor and the synchronous statistics load; the first
        /// initialization walk runs before both.
        records_used_items: std::cell::Cell<bool>,
    }

    impl tidb_planner_core_operator_logicalop::plan_context::UsedStatsRecorder
        for PlannerStatisticsLoad<'_>
    {
        fn record_used_item(
            &self,
            physical_table_id: i64,
            table_name: &str,
            in_pseudo_table: bool,
            item_id: i64,
            is_index: bool,
        ) {
            if !self.records_used_items.get() {
                return;
            }
            tidb_planner_core_operator_logicalop::plan_context::UsedStatsRecorder::record_used_item(
                &StatementUsedStats {
                    catalog: self.catalog,
                    context: self.context,
                },
                physical_table_id,
                table_name,
                in_pseudo_table,
                item_id,
                is_index,
            );
        }
    }

    impl tidb_planner_core_operator_logicalop::plan_context::StatisticsLoadRequester
        for PlannerStatisticsLoad<'_>
    {
        fn request(
            &self,
            usage: &tidb_stats::table_statistics::ColumnStatsUsage,
        ) -> Result<(), tidb_planner_core_base::PlanError> {
            self.context.set_operator_num(usage.operator_count);
            self.context
                .update_col_stats_usage(usage.predicate_columns.keys().copied());
            self.context.record_table_runtime_statistics(
                usage.visited_logical_table_ids.iter().copied(),
                |table_id| {
                    crate::stats::stats_table::table_statistics(
                        self.context,
                        self.catalog,
                        table_id,
                    )
                },
            );
            self.demanded
                .borrow_mut()
                .get_or_insert_with(Default::default)
                .extend(
                    self.catalog
                        .statistics_demand(self.context.stats_handle(), usage),
                );
            tidb_planner_core_rule::rule_collect_plan_stats::load_needed_items(
                self.catalog,
                self.context,
                usage,
            )
        }

        fn wait(&self) -> Result<(), tidb_planner_core_base::PlanError> {
            tidb_planner_core_rule::rule_collect_plan_stats::sync_wait_stats_load(self.context)
        }

        fn initialize(
            &self,
            plan: LogicalPlan,
        ) -> Result<LogicalPlan, (LogicalPlan, tidb_planner_core_base::PlanError)> {
            let demanded = self.demanded.borrow();
            let mut initializer = InitStats {
                error: None,
                catalog: self.catalog,
                demanded: demanded.as_ref(),
                context: self.context,
                record_used_stats: self.record_used_stats.get(),
            };
            let (plan, ()) = fold_owned(&mut initializer, plan, ());
            if let Some(error) = initializer.error {
                return Err((plan, error));
            }
            if self.record_used_stats.get() {
                self.records_used_items.set(true);
            }
            Ok(plan)
        }
    }

    let function_builder = RealFunctionBuilder::new(ctx);
    let statistics_load = PlannerStatisticsLoad {
        catalog,
        context: ctx,
        demanded: RefCell::new(None),
        record_used_stats: std::cell::Cell::new(true),
        records_used_items: std::cell::Cell::new(false),
    };
    let cte_optimizer = StatementCteOptimizer {
        catalog,
        ctx,
        plan_ids,
        column_ids,
        visiting: RefCell::new(HashSet::new()),
        parallel_apply_allowed: std::cell::Cell::new(ctx.enable_parallel_apply()),
    };
    // The base statistics are attached before the rule list with used-item
    // recording suspended for that walk.
    let initialize_statistics = |plan: LogicalPlan| {
        statistics_load.record_used_stats.set(false);
        let initialized =
            tidb_planner_core_operator_logicalop::plan_context::StatisticsLoadRequester::initialize(
                &statistics_load,
                plan,
            );
        statistics_load.record_used_stats.set(true);
        initialized
    };
    let services = crate::optimizer::OptimizeServices {
        statistics_load: &statistics_load,
        initialize_statistics: &initialize_statistics,
        used_stats_recorder: &statistics_load,
        cte_optimizer: &cte_optimizer,
    };
    crate::optimizer::optimize_logical(
        plan,
        flags,
        ctx,
        &services,
        use_plan_cache,
        plan_ids,
        column_ids,
    )
}

#[cfg(test)]
mod predicate_column_tests {
    use super::*;
    use tidb_model::TableItemID;
    use tidb_planner_core_operator_logicalop::DataSource;

    /// The loaded set follows `CollectPredicateColumnsPoint`'s demand over the
    /// plan: a column the source filters on loads, and a column only a join
    /// key asked for (Go's `needFullStats` false) stays evicted, so it borrows
    /// a loaded index's analyzed row count.
    #[test]
    fn filter_columns_load_and_join_keys_do_not() {
        let mut source = DataSource::new(Default::default(), 7, "history");
        source.physical_table_id = 7;
        let statistics = tidb_stats::Table::from_analyzed_items(
            7,
            0,
            0,
            [1, 2].map(|id| (id, tidb_stats::column::Column::default())),
            [],
        );
        let item = |id| TableItemID {
            table_id: 7,
            id,
            is_index: false,
            is_sync_load_failed: false,
        };
        let demanded = std::collections::HashMap::from([(item(1), true), (item(2), false)]);
        let catalog = tidb_infoschema_perfschema::mock_catalog();
        let context = tidb_sessionctx::StmtContext::default();
        let initializer = InitStats {
            error: None,
            catalog: &catalog,
            demanded: Some(&demanded),
            context: &context,
            record_used_stats: false,
        };
        let (columns, indexes) = initializer
            .predicate_loaded_items(&source, Some(&statistics))
            .expect("the demand names a column of the source");
        assert_eq!(columns, BTreeSet::from([1]));
        assert!(indexes.is_empty());
        // Before the demand exists, the cache's fully loaded subset stands.
        let initializer = InitStats {
            demanded: None,
            ..initializer
        };
        assert!(
            initializer
                .predicate_loaded_items(&source, Some(&statistics))
                .is_none()
        );
    }
}

#[cfg(test)]
mod statistics_initialization_tests {
    use super::*;
    use crate::stmt_ctx::PlannerStmtCtx as _;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::{Constant, ParamMarker};
    use tidb_expr::expression::Expression;
    use tidb_expr::scalar_function::ScalarFunction;
    use tidb_expr::schema::Schema;
    use tidb_planner_cardinality::derive_stats::StatsInfoScale as _;
    use tidb_planner_core_operator_logicalop::DataSource;
    use tidb_planner_property::stats_info::HistCollExt as _;
    use tidb_table::tables::Index;
    use tidb_table::tables::TableCommon;

    #[test]
    fn unfiltered_table_path_count_uses_realtime_row_count() {
        let table = TableCommon::new(
            7,
            vec![tidb_table::column::Column {
                info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
                    id: 1,
                    name: tidb_ast::CiString::new("a"),
                    field_type: FieldType::new(FieldTypeCode::LongLong),
                    version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::ColumnInfo::default()
                }),
                generated: None,
                default_value: None,
                origin_default: None,
            }],
        );
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        let table_info = table.meta();
        catalog.register_kv("t", table);
        let context = tidb_sessionctx::StmtContext::default();
        context.stats_handle().set_table_statistics(
            7,
            std::sync::Arc::new(tidb_stats::Table::from_analyzed_items(7, 1000, 0, [], [])),
        );
        let mut source = DataSource::new(Default::default(), 7, "t");
        // Go `ds.TableInfo`.
        source.table_info = Some(table_info);
        source.db_name = "test".to_owned();
        source.physical_table_id = 7;
        let index = |id: i64| {
            tidb_model::GoShared::new(tidb_model::IndexInfo {
                id,
                ..tidb_model::IndexInfo::default()
            })
        };
        source
            .all_possible_access_paths
            .push(tidb_planner_util::path::AccessPath {
                row_estimate: Some(tidb_stats::RowEstimate::new(900.0, 800.0, 1000.0)),
                is_single_scan: Some(true),
                ..tidb_planner_util::path::AccessPath::index_path(index(99))
            });
        source
            .all_possible_access_paths
            .push(tidb_planner_util::path::AccessPath {
                full_idx_cols: Some(vec![Some((
                    Column::new(1, FieldType::new(FieldTypeCode::LongLong)),
                    -1,
                ))]),
                row_estimate: Some(tidb_stats::RowEstimate::new(900.0, 800.0, 1000.0)),
                is_single_scan: Some(true),
                ..tidb_planner_util::path::AccessPath::index_path(index(77))
            });
        source.possible_access_paths = Some(Vec::new());

        let mut initializer = InitStats {
            error: None,
            catalog: &catalog,
            demanded: None,
            context: &context,
            record_used_stats: true,
        };
        let (plan, ()) = fold_owned(&mut initializer, LogicalPlan::DataSource(source), ());
        let LogicalPlan::DataSource(returned) = plan else {
            panic!("lost datasource");
        };
        assert!(
            returned.possible_access_paths.is_none(),
            "statistics refresh invalidates candidate derivation"
        );
        assert!(initializer.error.is_none(), "{:?}", initializer.error);
        // InitStats discards stale estimates without erasing declared keys.
        let stale = returned.path_by_index_id(99).unwrap();
        assert_eq!((stale.row_estimate, stale.is_single_scan), (None, None));
        let retained = returned.path_by_index_id(77).unwrap();
        assert_eq!(
            retained.full_idx_cols.as_ref().unwrap()[0]
                .as_ref()
                .unwrap()
                .0
                .unique_id,
            1
        );
        assert_eq!(retained.row_estimate, None);
        assert_eq!(retained.is_single_scan, None);
    }

    #[test]
    fn statistics_collection_preserves_estimation_validity() {
        let field_type = FieldType::new(FieldTypeCode::LongLong);
        let mut table = TableCommon::new(
            7,
            vec![tidb_table::column::Column {
                info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
                    id: 1,
                    name: tidb_ast::CiString::new("a"),
                    field_type: field_type.clone(),
                    version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::ColumnInfo::default()
                }),
                generated: None,
                default_value: None,
                origin_default: None,
            }],
        );
        table.add_index(
            Index {
                info: tidb_model::GoShared::new(tidb_model::IndexInfo {
                    id: 9,
                    name: tidb_ast::CiString::new("ia"),
                    columns: (vec![0])
                        .iter()
                        .enumerate()
                        .map(|(position, _)| tidb_model::IndexColumn {
                            length: (vec![-1])
                                .get(position)
                                .copied()
                                .unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH),
                            ..tidb_model::IndexColumn::default()
                        })
                        .collect::<Vec<_>>()
                        .into(),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::IndexInfo::default()
                }),
                column_offsets: vec![0],
                clustered_primary: false,
            },
            false,
        );
        for id in [10, 11] {
            let mut index = table.indexes()[0].clone();
            let detached = tidb_model::GoShared::new(index.read().clone_like_go());
            index.info = detached;
            index.info.write().id = id;
            index.info.write().name = tidb_ast::CiString::new(format!("ia_{id}"));
            table.add_index(index, false);
        }
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        let table_info = table.meta();
        catalog.register_kv("t", table);
        let mut source = DataSource::new(Default::default(), 7, "t");
        // Go `ds.TableInfo`.
        source.table_info = Some(table_info.clone());
        source.db_name = "test".to_owned();
        source.columns = vec![tidb_model::GoShared::new({
            let info = tidb_model::ColumnInfo {
                id: 1,
                name: tidb_ast::CiString::new("a"),
                state: tidb_model::SchemaState::PUBLIC,
                ..tidb_model::ColumnInfo::default()
            };
            info
        })];
        let mut column = Column::new(1, field_type.clone());
        column.id = 1;
        column.index = 0;
        source
            .base
            .base
            .set_schema(Some(Schema::new(vec![column.clone()])));
        source.set_table_indices(vec![tidb_model::GoShared::new(tidb_model::IndexInfo {
            id: 9,
            name: tidb_ast::CiString::new("ia"),
            columns: vec![tidb_model::IndexColumn {
                name: tidb_ast::CiString::new("a"),
                offset: 0,
                length: -1,
                ..tidb_model::IndexColumn::default()
            }]
            .into(),
            state: tidb_model::SchemaState::PUBLIC,
            ..tidb_model::IndexInfo::default()
        })]);

        for id in [10, 11] {
            let index = tidb_model::GoShared::new(source.table_indices()[0].read().clone_like_go());
            index.write().id = id;
            index.write().name = tidb_ast::CiString::new(format!("ia_{id}"));
            {
                let mut indexes = source.table_indices();
                indexes.push(index);
                source.set_table_indices(indexes);
            }
        }
        source.table_columns = vec![column.clone()];
        source.physical_table_id = 7;
        let histogram = tidb_stats::Histogram {
            id: 1,
            ndv: 2,
            null_count: 3,
            ..Default::default()
        };
        let column_stats = tidb_stats::column::Column {
            histogram: histogram.clone(),
            top_n: None,
            cmsketch: None,
            stats_version: 2,
            ..Default::default()
        };
        let index_stats = tidb_stats::index::Index {
            histogram: tidb_stats::Histogram { id: 9, ..histogram },
            top_n: None,
            cmsketch: None,
            stats_version: 2,
            info: Some(tidb_model::GoShared::new(tidb_model::IndexInfo {
                columns: (vec![String::new(); 1])
                    .into_iter()
                    .map(|name: String| tidb_model::IndexColumn {
                        name: tidb_ast::CiString::new(name),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>()
                    .into(),
                ..tidb_model::IndexInfo::default()
            })),
            ..Default::default()
        };
        let mut empty_index = index_stats.clone();
        empty_index.histogram = tidb_stats::Histogram {
            id: 11,
            ..Default::default()
        };
        let mut statistics = tidb_stats::Table::from_analyzed_items(
            7,
            100,
            0,
            [(1, column_stats)],
            [(9, index_stats), (11, empty_index)],
        );
        let evicted = tidb_stats::StatsLoadedStatus::all_evicted();
        statistics
            .hist_coll
            .get_column(1)
            .unwrap()
            .write()
            .unwrap()
            .stats_loaded_status = evicted;
        statistics
            .hist_coll
            .get_index(9)
            .unwrap()
            .write()
            .unwrap()
            .stats_loaded_status = evicted;
        // Index 11 is an unloaded placeholder that storage never recorded.
        statistics
            .hist_coll
            .get_index(11)
            .unwrap()
            .write()
            .unwrap()
            .stats_loaded_status = tidb_stats::StatsLoadedStatus::default();
        statistics
            .col_and_idx_existence_map()
            .write()
            .unwrap()
            .delete_index_not_found(11);
        for ndv in [2, 0] {
            let published = statistics.copy_as(tidb_stats::CopyIntent::AllDataWritable);
            let cache_usable = {
                let column = published.hist_coll.get_column(1).unwrap();
                let mut column = column.write().unwrap();
                column.histogram.ndv = ndv;
                // Go `ColumnStatsIsInvalid`'s estimation gate.
                let essential = column.stats_loaded_status.is_essential_stats_loaded();
                column.is_valid_for_estimation(published.hist_coll.pseudo, essential)
            };
            statistics = published;
            let context = tidb_sessionctx::StmtContext::default();
            context.stats_handle().set_table_statistics(
                7,
                std::sync::Arc::new(statistics.copy_as(tidb_stats::CopyIntent::MetaOnly)),
            );
            let mut initializer = InitStats {
                error: None,
                catalog: &catalog,
                demanded: None,
                context: &context,
                record_used_stats: true,
            };
            let (plan, ()) = fold_owned(
                &mut initializer,
                LogicalPlan::DataSource(source.clone()),
                (),
            );
            assert!(initializer.error.is_none(), "{:?}", initializer.error);
            let LogicalPlan::DataSource(returned) = plan else {
                panic!("lost datasource");
            };
            let hist = returned.table_stats.as_ref().unwrap().hist_coll().unwrap();
            assert_eq!(hist.index_ids_for_column(1), &[9, 11]);
            assert!(hist.index_columns(10).is_empty());
            assert_eq!(
                hist.index_histogram(11)
                    .unwrap()
                    .read()
                    .unwrap()
                    .total_row_count(),
                0.0
            );
            assert!(
                hist.histogram(column.unique_id).is_some(),
                "raw metadata survives eviction"
            );
            assert_eq!(
                hist.histogram_for_estimation(column.unique_id).is_some(),
                cache_usable,
                "snapshot must preserve column validity for NDV {ndv}"
            );
            assert!(
                hist.index_histogram(9).is_some(),
                "Go retains nonzero index payload regardless of full-load status"
            );
            let estimate =
                tidb_planner_cardinality::row_count_estimator::get_row_count_by_column_ranges(
                    hist.histogram_for_estimation(column.unique_id)
                        .as_ref()
                        .map(|column| column.read().unwrap())
                        .as_deref(),
                    &[
                        tidb_planner_cardinality::row_count_estimator::ColumnRange::point(
                            Datum::Null,
                        ),
                    ],
                    tidb_charset::Collation::Binary,
                    hist.realtime_count,
                    hist.modify_count,
                    false,
                    &Default::default(),
                )
                .unwrap();
            assert_eq!(
                estimate.est,
                if ndv > 0 { 0.1 } else { 100.0 },
                "Go NULL estimate after snapshot for NDV {ndv}"
            );
            let scaled = returned.table_stats.as_ref().unwrap().scale(0.1, 1.0);
            assert_eq!(
                scaled
                    .hist_coll()
                    .unwrap()
                    .histogram_for_estimation(column.unique_id)
                    .is_some(),
                cache_usable
            );
        }
    }

    #[test]
    fn statistics_initialization_preserves_range_evaluation_error() {
        let field_type = FieldType::new(FieldTypeCode::LongLong);
        let mut table = TableCommon::new(
            7,
            vec![tidb_table::column::Column {
                info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
                    id: 1,
                    name: tidb_ast::CiString::new("a"),
                    field_type: field_type.clone(),
                    version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::ColumnInfo::default()
                }),
                generated: None,
                default_value: None,
                origin_default: None,
            }],
        );
        table.add_index(
            Index {
                info: tidb_model::GoShared::new(tidb_model::IndexInfo {
                    id: 9,
                    name: tidb_ast::CiString::new("ia"),
                    columns: (vec![0])
                        .iter()
                        .enumerate()
                        .map(|(position, _)| tidb_model::IndexColumn {
                            length: (vec![-1])
                                .get(position)
                                .copied()
                                .unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH),
                            ..tidb_model::IndexColumn::default()
                        })
                        .collect::<Vec<_>>()
                        .into(),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::IndexInfo::default()
                }),
                column_offsets: vec![0],
                clustered_primary: false,
            },
            false,
        );
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        let table_info = table.meta();
        catalog.register_kv("t", table);
        let mut source = DataSource::new(Default::default(), 7, "t");
        // Go `ds.TableInfo`.
        source.table_info = Some(table_info.clone());
        source.db_name = "test".to_owned();
        source.columns = vec![tidb_model::GoShared::new({
            let info = tidb_model::ColumnInfo {
                id: 1,
                name: tidb_ast::CiString::new("a"),
                state: tidb_model::SchemaState::PUBLIC,
                ..tidb_model::ColumnInfo::default()
            };
            info
        })];
        let mut column = Column::new(1, field_type.clone());
        column.id = 1;
        column.index = 0;
        source
            .base
            .base
            .set_schema(Some(Schema::new(vec![column.clone()])));
        source.set_table_indices(vec![tidb_model::GoShared::new(tidb_model::IndexInfo {
            id: 9,
            name: tidb_ast::CiString::new("ia"),
            columns: vec![tidb_model::IndexColumn {
                name: tidb_ast::CiString::new("a"),
                offset: 0,
                length: -1,
                ..tidb_model::IndexColumn::default()
            }]
            .into(),
            state: tidb_model::SchemaState::PUBLIC,
            ..tidb_model::IndexInfo::default()
        })]);
        source.all_possible_access_paths = vec![tidb_planner_util::path::AccessPath::index_path(
            source.table_indices()[0].clone(),
        )];
        let mut parameter = Constant::new(Datum::Int(1), field_type);
        parameter.param_marker = Some(ParamMarker { order: 0 });
        let comparison = tidb_expr::new_function_base(
            &tidb_exprctx::PlanCacheParams(&[
                tidb_datatype::Datum::Int(0),
                tidb_datatype::Datum::Int(0),
            ]),
            "eq",
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(column.clone()),
                Expression::Constant(parameter.clone()),
            ],
        )
        .unwrap();
        let conditions = [
            Expression::Constant(parameter.clone()),
            tidb_expr::new_function_base(
                &tidb_exprctx::PlanCacheParams(&[
                    tidb_datatype::Datum::Int(0),
                    tidb_datatype::Datum::Int(0),
                ]),
                "in",
                FieldType::new(FieldTypeCode::Tiny),
                vec![Expression::Column(column), Expression::Constant(parameter)],
            )
            .unwrap(),
        ];
        for condition in conditions {
            source.pushed_down_conds = vec![condition];
            for bound in [false, true] {
                let context = if bound {
                    tidb_sessionctx::StmtContext::default()
                        .with_prepared_params(vec![Datum::Int(2)].into())
                } else {
                    tidb_sessionctx::StmtContext::default()
                };
                let column = source.base.base.schema().unwrap().columns[0].clone();
                let evaluate =
                    |expression: &Expression| tidb_expr::eval_expression_once(expression, &context);
                if !bound {
                    // Go buildFromBinOp deliberately returns no points on Eval error.
                    let comparison_ranges =
                        tidb_ranger::detacher::detach_index_range_with_fallback_handler_in(
                            std::slice::from_ref(&comparison),
                            std::slice::from_ref(&column),
                            &[-1],
                            context.range_max_size(),
                            context.range_fallback_handler(),
                            &evaluate,
                            context.ranger_options(),
                        )
                        .unwrap();
                    assert!(comparison_ranges.ranges.is_empty());
                }
                let direct = tidb_ranger::detacher::detach_index_range_with_fallback_handler_in(
                    &source.pushed_down_conds,
                    &[column],
                    &[-1],
                    context.range_max_size(),
                    context.range_fallback_handler(),
                    &evaluate,
                    context.ranger_options(),
                );
                assert_eq!(direct.is_err(), !bound, "direct result: {direct:?}");
                let expected_error = direct.err().map(|error| match error {
                    tidb_ranger::points::PointBuilderError::Eval(error) => error.into(),
                    tidb_ranger::points::PointBuilderError::Unsupported(message) => {
                        assert!(message.ends_with("is not evaluated"));
                        tidb_planner_core_base::PlanError::from(
                            tidb_plannererrors::ERR_UNSUPPORTED_TYPE.gen_with_stack(message),
                        )
                    }
                    other => panic!("unexpected error: {other:?}"),
                });
                // Go builds the ranges and the row count in `fillIndexPath`,
                // which the planner's statistics derivation runs.
                let plan_ids = PlanIdAllocator::new();
                let column_ids = ColumnIdAllocator::new();
                let optimized = optimize_built_logical(
                    LogicalPlan::DataSource(source.clone()),
                    0,
                    &catalog,
                    &context,
                    false,
                    &plan_ids,
                    &column_ids,
                    &tidb_datatype::SessionTimeZone::utc(),
                );
                if bound {
                    let plan = optimized.expect("bound parameter plans");
                    let LogicalPlan::DataSource(returned) = plan else {
                        panic!("lost datasource");
                    };
                    assert_eq!(returned.table_id(), 7);
                    assert!(
                        returned
                            .path_by_index_id(9)
                            .and_then(|path| path.count_after_access())
                            .is_some()
                    );
                } else {
                    assert_eq!(optimized.err(), expected_error.clone());
                    if let Some(error) = expected_error {
                        if error.equal(&tidb_plannererrors::ERR_UNSUPPORTED_TYPE) {
                            let expected_message = error.to_string();
                            let tidb_driver_error::DriverError::Mysql(mysql) =
                                crate::optimize::planner_error_to_driver(error)
                            else {
                                panic!("lost unsupported-type error identity");
                            };
                            assert_eq!(mysql.code, 8108);
                            assert_eq!(mysql.message, expected_message);
                        }
                    }
                }
            }
        }
    }
}

/// The table names Go's `core.CheckTableMode`
/// (`pkg/planner/core/optimizer.go:304`) checks for `stmt`: none for a SHOW
/// and none for ADMIN CHECKSUM TABLE (which IMPORT INTO runs on its own
/// table); an EXPLAIN is not checked itself, so its names are the ones the
/// optimization of the statement it explains checks.
#[must_use]
pub fn check_table_mode_names(stmt: &tidb_ast::Stmt) -> Vec<Vec<String>> {
    use tidb_ast::{AdminStmt, ExplainTarget};
    if let tidb_ast::Stmt::Admin(admin) = stmt {
        match &**admin {
            AdminStmt::Show(_) | AdminStmt::AdminChecksum(_) => return Vec::new(),
            AdminStmt::Explain(explain) => {
                return match &explain.target {
                    ExplainTarget::Statement(inner) | ExplainTarget::ExploreStatement(inner) => {
                        check_table_mode_names(inner)
                    }
                    ExplainTarget::PlanDigest(_)
                    | ExplainTarget::ExploreDigest(_)
                    | ExplainTarget::ExploreReplayer(_) => Vec::new(),
                };
            }
            _ => {}
        }
    }
    crate::preprocess::resolve_ctx_table_names(stmt)
}
