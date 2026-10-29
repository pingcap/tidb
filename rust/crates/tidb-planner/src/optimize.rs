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

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::expression_rewriter::ColumnIdAllocator;
use crate::find_best_task::coster::{CostEnv, Ver2Coster};
use crate::find_best_task::dispatch::{find_best_task, DispatchContext, MppWarningSink};
use tidb_fixcontrol::OptimizerFixControl;
use crate::logical::rule::{
    flags, logical_optimize, CteOptimizer, DisabledLogicalRules, HintWarningSink, PlanCacheMarker,
    RuleContext, UsedStatsRecorder,
};
use crate::logical::rule_collect_plan_stats::StatisticsLoadRequester;
use crate::logical::rule_partition_processor::PartitionPruning;
use crate::logical::{prepare_possible_properties, LogicalPlan};
use crate::physical::PhysicalPlan;
use crate::physical_property::PhysicalProperty;
use crate::plan_base::{PlanError, PlanIdAllocator};

/// Go `base.PlanContext` as the optimizer reads it: the statement's session
/// settings (`SessionVars`), its warnings and phase clocks (`StmtCtx`).
pub trait OptimizeContext: tidb_exprctx::Columns + PlanCacheMarker + HintWarningSink + MppWarningSink {
    /// Go `SessionVars.Location()` as the expression resolver reads it.
    fn session_zone(&self) -> tidb_datatype::SessionTimeZone;
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
    /// Go `SessionVars.GetEnableIndexMerge()`.
    fn index_merge(&self) -> bool;
    /// Go `StmtCtx.NoIndexMergeHint`.
    fn no_index_merge_hint(&self) -> bool;
    /// Go `StmtCtx.InExplainStmt`.
    fn in_explain_stmt(&self) -> bool;
    /// Go `SessionVars.RangeMaxSize`.
    fn range_max_size(&self) -> i64;
    /// Go `SessionVars.SelectivityFactor`.
    fn selectivity_factor(&self) -> f64;
    /// Go's range-fallback warning hook (`StmtCtx.RecordRangeFallback`).
    fn range_fallback_handler(&self) -> &tidb_context::RangeFallbackHandler;
    /// Go `SessionVars.AlwaysKeepJoinKey`.
    fn always_keep_join_key(&self) -> bool;
    /// Go `SessionVars.EnableUnsafeSubstitute`.
    fn enable_unsafe_substitute(&self) -> bool;
    /// Go `SessionVars.EnableSemiJoinRewrite`.
    fn enable_semi_join_rewrite(&self) -> bool;
    /// Go `SessionVars.OptimizerEnableNAAJ`.
    fn enable_null_aware_anti_join(&self) -> bool;
    /// Go `SessionVars.EnableNoDecorrelateInSelect`.
    fn enable_no_decorrelate_in_select(&self) -> bool;
    /// Go `SessionVars.OptimizerFixControl`.
    fn optimizer_fix_control(&self) -> &OptimizerFixControl;
    /// Go `SessionVars.TiDBOptJoinReorderThreshold`.
    fn join_reorder_threshold(&self) -> i32;
    /// Go `SessionVars.AllowAggPushDown`.
    fn allow_agg_push_down(&self) -> bool;
    /// Go `SessionVars.TiDBOptEnableAdvancedJoinReorder`.
    fn advanced_join_reorder(&self) -> bool;
    /// Go `SessionVars.CartesianJoinOrderThreshold`.
    fn cartesian_join_order_threshold(&self) -> f64;
    /// Go `SessionVars.TiDBOptJoinReorderThroughProj`.
    fn join_reorder_through_proj(&self) -> bool;
    /// Go `SessionVars.TiDBOptJoinReorderThroughSel`.
    fn join_reorder_through_sel(&self) -> bool;
    /// Go `SessionVars.EnableOuterJoinReorder`.
    fn outer_join_reorder(&self) -> bool;
    /// Go `SessionVars.EnableAdvancedJoinHint`.
    fn advanced_join_hint(&self) -> bool;
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
    fn partial_ordered_index_for_topn(&self) -> bool;
    /// Go `SessionVars.OptOrderingIdxSelRatio`.
    fn ordering_index_selectivity_ratio(&self) -> f64;
    /// Go `SessionVars.OptOrderingIdxSelThresh`.
    fn ordering_index_selectivity_threshold(&self) -> f64;
    /// Go `SessionVars.AllowProjectionPushDown`.
    fn allow_projection_push_down(&self) -> bool;
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
    fn is_chunk_alloc_valid(&self) -> bool;
    /// Go `SessionVars.SetAlloc(nil)` / `ClearAlloc`.
    fn set_chunk_alloc_valid(&self, valid: bool);
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
    /// Go `PartitionProcessor`'s pruning over the table's definition.
    pub partition_pruning: &'a dyn PartitionPruning,
    /// Go `LogicalCTE.DeriveStats`'s `DoOptimize`.
    pub cte_optimizer: &'a dyn CteOptimizer,
}

/// Go `adjustOptimizationFlags` (`VolcanoOptimize`, run for the statement
/// and for each CTE part): stable result mode adds `ResultReorder`.
#[must_use]
pub fn adjust_optimization_flags<C: OptimizeContext>(flags: u64, ctx: &C) -> u64 {
    if ctx.stable_result_mode() {
        flags | flags::STABILIZE_RESULTS
    } else {
        flags
    }
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
    // Go `adjustOptimizationFlags` enables both statistics rules for every
    // ordinary (non-restricted) statement; the builder never owns these
    // flags because they are session/execution policy, not AST shape.
    let flags = flags | flags::COLLECT_PREDICATE_COLUMNS_POINT | flags::SYNC_WAIT_STATS_LOAD_POINT;
    let flags = if ctx.static_partition_prune() {
        flags
    } else {
        flags & !flags::PARTITION_PROCESSOR
    };
    let flags = crate::logical::rule::add_second_column_prune(flags);
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
            crate::logical::rule::OPT_RULE_LIST
                .iter()
                .map(|rule| rule.name())
                .filter(|name| ctx.logical_rule_disabled(name)),
        ),
        statistics_load: Some(services.statistics_load),
        partition_pruning: Some(services.partition_pruning),
        opt_index_prune_threshold: ctx.opt_index_prune_threshold(),
        opt_prefix_index_single_scan: ctx.opt_prefix_index_single_scan(),
        index_merge_enabled: ctx.index_merge(),
        no_index_merge_hint: ctx.no_index_merge_hint(),
        in_explain_stmt: ctx.in_explain_stmt(),
        range_max_size: ctx.range_max_size(),
        selectivity_factor: ctx.selectivity_factor(),
        range_fallback_handler: Some(ctx.range_fallback_handler()),
        always_keep_join_key: ctx.always_keep_join_key(),
        enable_unsafe_substitute: ctx.enable_unsafe_substitute(),
        enable_semi_join_rewrite: ctx.enable_semi_join_rewrite(),
        enable_null_aware_anti_join: ctx.enable_null_aware_anti_join(),
        enable_no_decorrelate_in_select: ctx.enable_no_decorrelate_in_select(),
        allow_eliminate_apply: ctx
            .optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_45822, true),
        join_reorder_threshold: ctx.join_reorder_threshold(),
        group_ndv_skew_ratio: optimizer_cost_env.session.group_ndv_skew_ratio,
        scale_ndv_skew_ratio: optimizer_cost_env.session.scale_ndv_skew_ratio,
        allow_agg_push_down: ctx.allow_agg_push_down(),
        advanced_join_reorder: ctx.advanced_join_reorder(),
        cartesian_join_order_threshold: ctx.cartesian_join_order_threshold(),
        join_reorder_through_proj: ctx.join_reorder_through_proj(),
        join_reorder_through_sel: ctx.join_reorder_through_sel(),
        outer_join_reorder: ctx.outer_join_reorder(),
        advanced_join_hint: ctx.advanced_join_hint(),
        hint_warning_sink: Some(ctx),
        used_stats_recorder: Some(services.used_stats_recorder),
        cte_optimizer: Some(services.cte_optimizer),
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
    crate::logical::cte::recheck_cte(&mut plan);
    // Go `planner/optimize.go:525`: the result's field names are the logical
    // plan's `OutputNames()` captured BEFORE optimization. A rule may replace
    // the root (an always-false filter makes a TableDual) with a node that
    // names nothing.
    let built_names = plan.output_names().to_vec();
    // No CTE class is optimized here: Go's `LogicalCTE.DeriveStats` runs
    // `DoOptimize` the first time a reference's statistics are derived, which
    // `rule_context.cte_optimizer` does from inside the stats walk.
    let begin_logical_opt = Instant::now();
    let logical_result = logical_optimize(&rule_context, flags, plan);
    // Go `logicalOptimize`'s deferred `DurationOptimizer.LogicalOpt`.
    ctx.set_logical_opt(begin_logical_opt.elapsed());
    let mut optimized = logical_result.map_err(|(_, error)| error)?.plan;
    // SyncWaitStatsLoadPoint has initialized ordinary and statically pruned
    // sources from the loaded snapshot before join reorder.
    optimized = check_partial_index_paths(optimized, ctx, use_plan_cache);
    if !ctx.static_partition_prune() {
        attach_dynamic_partition_access(&mut optimized, services.partition_pruning)?;
    }
    if optimized.output_names().is_empty()
        && optimized
            .schema()
            .is_some_and(|schema| schema.columns.len() == built_names.len())
    {
        optimized.set_output_names(built_names);
    }
    // Go `physicalOptimize` starts its clock before `RecursiveDeriveStats`
    // and records `StatsDerive` after it; the physical search that follows
    // this function takes the start over.
    let begin_physical_opt = Instant::now();
    optimized.recursive_derive_stats_with_context(&[], &rule_context)?;
    ctx.finish_stats_derive(begin_physical_opt);
    let logical = prepare_possible_properties(optimized).0;
    Ok(logical)
}

/// Go `DataSource.CheckPartialIndexes` over every source, which Go's
/// `DataSource.DeriveStats` runs with the statement's resolver.
pub fn check_partial_index_paths<C: OptimizeContext>(
    plan: LogicalPlan,
    ctx: &C,
    use_plan_cache: bool,
) -> LogicalPlan {
    use crate::logical::fold::{fold_owned, Descend, OwnedRewrite};

    struct PartialIndexChecker<'a> {
        resolver: &'a tidb_expr::rewriter::ZonedNoResolver,
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

    let resolver = tidb_expr::rewriter::ZonedNoResolver::with_like_default_escape(
        ctx.session_zone(),
        tidb_exprctx::Columns::like_default_escape(ctx),
    );
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
    pruning: &dyn PartitionPruning,
) -> Result<(), PlanError> {
    if let LogicalPlan::DataSource(source) = plan {
        if !source.partition_definition_ids.is_empty() {
            let indices = pruning.partition_indices(source)?;
            let all_partitions = source.partition_names.is_empty()
                && indices.len() == source.partition_definition_ids.len();
            let partitions = if all_partitions {
                Vec::new()
            } else {
                indices
                    .into_iter()
                    .filter_map(|index| source.partition_definition_names.get(index).cloned())
                    .collect()
            };
            source.dynamic_partition_access = Some(crate::access::DynamicPartitionAccessObject {
                database: source.db_name.clone(),
                table: source
                    .table_as_name
                    .clone()
                    .unwrap_or_else(|| source.table_name.clone()),
                all_partitions,
                partitions,
                error: String::new(),
            });
        }
        return Ok(());
    }
    for child in plan.base_mut().children_mut() {
        attach_dynamic_partition_access(child, pruning)?;
    }
    Ok(())
}

/// Go `physicalOptimize` and `postOptimize`: the cheapest physical plan for
/// the optimized logical plan, then the post-optimization rewrites.
/// `col_ref_from_update_plan` is Go's `StmtCtx.ColRefFromUpdatePlan` for an
/// UPDATE's read plan (empty otherwise).
///
/// # Errors
///
/// No physical plan found, or a resolution error.
pub fn optimize_physical<C: OptimizeContext>(
    logical: &LogicalPlan,
    plan_ids: &PlanIdAllocator,
    column_ids: &ColumnIdAllocator,
    ctx: &C,
    col_ref_from_update_plan: &BTreeSet<i64>,
) -> Result<PhysicalPlan, PlanError> {
    let begin_physical_opt = ctx.take_begin_physical_opt().unwrap_or_else(Instant::now);
    let optimizer_cost_env = ctx.optimizer_cost_env();
    let coster = Ver2Coster::from_env(optimizer_cost_env);
    let evaluate = tidb_ranger::points::StatementRangerEvaluator(ctx);
    let mut dispatch = DispatchContext::new(
        plan_ids,
        &coster,
        optimizer_cost_env.session.scale_ndv_skew_ratio,
    )
    .with_estimator_options(optimizer_cost_env.session.estimator_options.clone())
    .with_group_ndv_skew_ratio(optimizer_cost_env.session.group_ndv_skew_ratio)
    .with_correlation_options(optimizer_cost_env.session.correlation_options)
    .with_expression_evaluator(&evaluate)
    .with_mpp_allowed(optimizer_cost_env.session.mpp_allowed)
    .with_enable_skew_distinct_agg(ctx.enable_skew_distinct_agg())
    .with_enable_3_stage_distinct_agg(ctx.enable_3_stage_distinct_agg())
    .with_enable_3_stage_multi_distinct_agg(ctx.enable_3_stage_multi_distinct_agg())
    .with_tiflash_pre_agg_mode(ctx.tiflash_pre_agg_mode())
    .with_partial_ordered_index_for_topn(ctx.partial_ordered_index_for_topn())
    .with_opt_prefix_index_single_scan(ctx.opt_prefix_index_single_scan())
    .with_mpp_warning_sink(ctx)
    .with_plan_cache(
        tidb_exprctx::Columns::use_plan_cache(ctx),
        ctx,
        ctx.optimizer_fix_control()
            .get_bool_with_default(tidb_fixcontrol::FIX_44830, false),
    )
    .with_range_quota(ctx.range_max_size(), ctx.range_fallback_handler())
    .with_selectivity_factor(ctx.selectivity_factor())
    .with_ordering_index_selectivity_ratio(ctx.ordering_index_selectivity_ratio())
    .with_ordering_index_selectivity_threshold(ctx.ordering_index_selectivity_threshold())
    .with_projection_push_down(ctx.allow_projection_push_down())
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
    // Go `physicalOptimize` records `DurationOptimizer.PhysicalOpt` on every
    // return, an error one included.
    let task = find_best_task(logical, &PhysicalProperty::default(), &mut dispatch)
        .inspect_err(|_| ctx.set_physical_opt(begin_physical_opt.elapsed()))?;
    let mut physical = task.plan().cloned().ok_or_else(|| {
        PlanError::from(
            tidb_plannererrors::ERR_INTERNAL.generate_with_stack_by_args(&[
                "Can't find a proper physical plan for this query".into(),
            ]),
        )
    })?;
    // Go caches each operator's cost while `findBestTask` compares tasks
    // (`PlanCostInit`), and EXPLAIN shows that cost for the finished plan even
    // where `postOptimize` later put an operator between a parent and its
    // child. Keep the enumeration-time costs on the nodes the same way.
    Ver2Coster::from_env(optimizer_cost_env)
        .with_explain_cache()
        .record_enumeration_costs(&mut physical);
    // Go physicalOptimize: "collect the warnings from task". Every task
    // warning is a planner `ErrInternal` (hint warnings), code 1815.
    for warning in task.warnings().get_warnings() {
        let note = matches!(warning.level, tidb_context::WarningLevel::Note);
        ctx.append_task_warning(note, 1815, &warning.message);
    }
    // Go physicalOptimize binds positions before postOptimize removes aliases.
    let resolved = physical.resolve_indices();
    ctx.set_physical_opt(begin_physical_opt.elapsed());
    resolved?;
    let physical = crate::physical::eliminate_physical_projection_for_update(
        physical,
        col_ref_from_update_plan,
    );
    // Go postOptimize: eliminatePhysicalProjection -> InjectExtraProjection.
    // The projection re-injection restores the purposeful projections
    // (scalar aggregate arguments, scalar order-by items, expression
    // nominal sorts) that the elimination pass removed.
    let mut physical = crate::physical::inject_extra_projection(physical, plan_ids, column_ids);
    // Go postOptimize: mergeContinuousSelections.
    crate::physical::merge_continuous_selections(&mut physical);
    // Go postOptimize: eliminateUnionScanAndLock (the UnionScan half).
    let mut physical =
        crate::physical::eliminate_union_scan_over_point_get(physical, ctx.in_transaction());
    crate::physical::shuffle::install_receivers(&mut physical, plan_ids)?;
    if ctx.enable_parallel_apply() {
        crate::physical::enable_parallel_apply(&mut physical, ctx.executor_concurrency());
    }
    // Go postOptimize: disableReuseChunkIfNeeded.
    if ctx.is_chunk_alloc_valid()
        && crate::physical::chunk_reuse::should_disable_reuse_chunk(
            &physical,
            ctx.max_chunk_size(),
            tidb_memory::mem_total().ok(),
        )
    {
        ctx.set_chunk_alloc_valid(false);
    }
    physical
        .base_mut()
        .base
        .set_output_names(logical.output_names().to_vec());
    Ok(physical)
}
