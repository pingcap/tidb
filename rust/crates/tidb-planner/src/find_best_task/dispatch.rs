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

//! The GENERAL `findBestTask` dispatcher: Go's volcano search over
//! `(logical operator, required property)` for the whole ported operator
//! set, not only joins.
//!
//! Go sources, ported body by body:
//! * `findBestTask` (`pkg/planner/core/find_best_task.go:605`) — the
//!   memoized entry: the task-map lookup, the non-root task-type gate, the
//!   un-enforced exhaust, the enforcer branch re-exhausting under the empty
//!   property, and the two-pass enumeration.
//! * `enumeratePhysicalPlans4Task` (`:112`) and its helper (`:156`) — the
//!   candidate loop: plan every child under the candidate's child property,
//!   attach, convert to root, enforce.
//! * `compareTaskCost` (`:479`) / `getTaskPlanCost` (`:498`) — an invalid
//!   task prices at `MaxFloat64` and never wins; pricing itself arrives
//!   through [`TaskCoster`], which keeps cost inputs at the dispatch boundary:
//!   the cost formulas live in [`crate::plan_cost_ver2`] but the profile
//!   inputs are the caller's.
//!
//! # Narrowings, each naming its Go symbol
//!
//! * `checkOpSelfSatisfyPropTaskTypeRequirement` and the MPP property
//!   fields the enforcer branch resets: no TiFlash tier.
//! * The task map is keyed by the property's ESSENTIAL fields
//!   ([`prop_key`]) per plan id, standing in for Go's `prop.HashCode()`
//!   over fields this port does not carry.
//!
//! # Operator routing
//!
//! Go gives some operators their own `findBestTask` override instead of an
//! exhaust: the dual, the CTE table, and the two shows are born directly in
//! root tasks. [`find_best_task`] routes them to those ported bodies first,
//! exactly as Go's function-pointer wiring does. `DataSource` and
//! `LogicalMemTable` use their own source-specific task builders.
//! Joins refuse toward [`crate::find_best_task`]'s own specialized search,
//! which owns candidate enumeration for them.

use crate::logical::data_source::{index_covers_condition, index_path_is_single_scan};
use std::collections::{HashMap, HashSet};

use crate::enforce::enforce_property_in;
use crate::logical::LogicalPlan;
use crate::physical::{self, PhysicalPlan};
use crate::physical_property::PhysicalProperty;
use crate::plan_base::{PlanError, PlanIdAllocator};
use crate::task::Task;
use crate::task_type::TaskType;

/// Go `getTaskPlanCost`'s pricing half: what a built task costs.
///
/// Cost formulas are in [`crate::plan_cost_ver2`], while row counts and factor
/// profiles are supplied by the caller.
pub trait TaskCoster {
    /// The task's plan cost; called only on VALID tasks — the invalid-task
    /// `MaxFloat64` arm is [`compare_task_cost`]'s own, as in Go.
    fn task_cost(&self, task: &Task) -> Result<f64, PlanError>;
}

/// Go `SessionVars.RaiseWarningWhenMPPEnforced`'s statement-context sink.
///
/// Physical enumeration stays in the planner crate, while warning buffers
/// and the `InExplainStmt`/extra-warning split belong to the session
/// executor. This trait carries that side effect across the crate boundary
/// without making the planner depend on a session implementation.
pub trait MppWarningSink {
    /// Raises one source-shaped MPP refusal warning when enforcement is on.
    fn raise_mpp_warning(&self, message: &str);
}

/// Go `compareTaskCost` (`find_best_task.go:479`): whether `cur` beats
/// `best`. An invalid current task never wins; an invalid best always
/// loses; otherwise strictly-lower cost wins.
pub fn compare_task_cost(
    coster: &dyn TaskCoster,
    cur: &Task,
    best: &Task,
) -> Result<bool, PlanError> {
    if cur.invalid() {
        return Ok(false);
    }
    if best.invalid() {
        return Ok(true);
    }
    Ok(coster.task_cost(cur)? < coster.task_cost(best)?)
}

/// Everything one search shares: the id allocator the built operators draw
/// from, the coster, the NDV skew ratio the stats scaling reads, and Go's
/// per-operator task map.
pub struct DispatchContext<'a> {
    /// Published expression policy used throughout physical task attachment.
    pub expr_pushdown_blacklist: tidb_expr::infer_pushdown::ExprPushDownBlacklist,
    /// Session inputs to Go's histogram cardinality estimators.
    pub estimator_options: crate::cardinality::row_count_estimator::EstimatorOptions,
    /// Session controls for Go's correlated scan-limit estimator.
    pub correlation_options: crate::cardinality::cross_estimation::CorrelationOptions,
    /// The plan id allocator for built physical operators.
    pub allocator: &'a PlanIdAllocator,
    /// The pricing seam.
    pub coster: &'a dyn TaskCoster,
    /// The `tidb_opt_scale_ndv_skew_ratio` the NDV scaling reads; 1.0 is Go's default.
    pub skew_ratio: f64,
    /// Session range memory limit used during initial access-path construction.
    pub range_max_size: i64,
    /// Go `SessionVars.SelectivityFactor`, the pseudo estimator's starting
    /// factor (`tidb_opt_selectivity_factor`).
    pub selectivity_factor: f64,
    /// Statement warning and plan-cache fallback state.
    pub range_fallback_handler: Option<&'a tidb_util::context::RangeFallbackHandler>,
    /// Statement parameter values, warnings and coercion settings used by ranger.
    pub expression_evaluator: &'a crate::ranger::points::ExpressionEvaluator<'a>,
    /// Go `SessionVars.OptOrderingIdxSelRatio`, used by the ordered LIMIT
    /// row-count adjustment for table and index scans.
    pub ordering_index_selectivity_ratio: f64,
    /// Go `SessionVars.OptOrderingIdxSelThresh`, which can disable expected-
    /// count adjustment when any datasource path is sufficiently selective.
    pub ordering_index_selectivity_threshold: f64,
    /// Go `SessionVars.AllowProjectionPushDown`, used when Projection
    /// enumerates its TiKV coprocessor candidate.
    pub allow_projection_push_down: bool,
    /// Go fix-control 56318, which enables the heavy-function TopN rewrite.
    pub heavy_function_optimize: bool,
    /// Go `SessionVars.EnableINLJoinInnerMultiPattern`
    /// (`tidb_enable_inl_join_inner_multi_pattern`, default ON): whether
    /// Selection, Projection, an inner-type Join, or a matching Aggregation
    /// may sit between an index join's inner `DataSource` and the join
    /// itself. Read by [`admits_index_join_inner_child_pattern`].
    pub enable_inl_join_inner_multi_pattern: bool,
    /// Go `SessionVars.LimitPushDownThreshold`, used by TopN's normal
    /// coprocessor preference. A physical Limit always prefers cop.
    pub limit_push_down_threshold: u64,
    /// Go `SessionVars.EnablePaging`. Reader costing writes the selected
    /// paging mode back onto `PhysicalIndexLookUpReader`.
    pub enable_paging: bool,
    /// Go `SessionVars.HashJoinConcurrency()`, stamped onto every hash-join
    /// candidate by `NewPhysicalHashJoin`.
    pub hash_join_concurrency: usize,
    /// Resolved session settings for the shuffle rewrite.
    pub shuffle_options: crate::physical::shuffle_optimize::ShuffleOptions,
    /// Go `SessionVars.RiskGroupNDVSkewRatio` for cardinality estimates.
    pub group_ndv_skew_ratio: f64,
    /// Go `SessionVars.UseHashJoinV2`; see `CostSessionOpts::use_hash_join_v2`.
    pub use_hash_join_v2: bool,
    /// Go `SessionVars.MemQuotaApplyCache`, used by
    /// `exhaustPhysicalPlans4LogicalApply` after estimating the correlated
    /// value hit ratio.
    pub apply_cache_capacity: i64,
    /// Go `fixcontrol.Fix44855`, which raises an IndexJoin probe's scan-row
    /// floor when the chosen access path can use only a prefix of the equality
    /// join keys. Go reads this floor with `GetBoolWithDefault(..., true)`;
    /// its distinct upper bound below uses the false default.
    pub index_join_probe_row_count_fix: bool,
    /// Go Fix44855's separate NDV upper bound, disabled by default.
    pub index_join_row_count_upper_bound: bool,
    /// Whether normal optimization may convert scans to PointGet/BatchPointGet.
    /// Go fix 52592 disables both conversions when enabled by the session.
    pub enable_point_get_conversion: bool,
    /// Go `fixcontrol.Fix45132`: the row-count ratio at which skyline pruning
    /// prefers one IndexJoin inner access path over another. A non-positive
    /// value disables the empirical rule, matching Go's fix-control getter.
    pub index_join_skyline_threshold: f64,
    /// Go `SessionVars.IsMPPAllowed()`, which controls MPP candidates.
    pub mpp_allowed: bool,
    /// Go `SessionVars.EnableSkewDistinctAgg`, which disables one-phase MPP
    /// aggregation so skew-aware distinct rewrites can retain their exchange.
    pub enable_skew_distinct_agg: bool,
    /// Go `SessionVars.Enable3StageDistinctAgg`.
    pub enable_3_stage_distinct_agg: bool,
    /// Go `SessionVars.Enable3StageMultiDistinctAgg`.
    pub enable_3_stage_multi_distinct_agg: bool,
    /// Go `SessionVars.TiFlashPreAggMode`.
    pub tiflash_pre_agg_mode: String,
    /// Go `SessionVars.IsPartialOrderedIndexForTopNEnabled`.
    pub partial_ordered_index_for_topn: bool,
    /// Go SessionVars.OptPrefixIndexSingleScan.
    pub opt_prefix_index_single_scan: bool,
    /// Statement-context sink for Go's enforced-MPP refusal warnings.
    pub mpp_warning_sink: Option<&'a dyn MppWarningSink>,
    /// Go `SessionVars.GetAllowPreferRangeScan()` (`tidb_opt_prefer_range_scan`,
    /// default ON): under unreliable statistics a range-scan path carrying an
    /// `=`/`IN` prefix wins over a full table scan even when its estimated
    /// cost is higher. The executor does not expose the session override yet,
    /// so callers use Go's default.
    pub prefer_range_scan: bool,
    /// Go `BaseLogicalPlan.taskMap`, keyed by the logical plan object and
    /// property. Numeric plan IDs are explain identities and are deliberately
    /// shared by static-partition DataSource copies.
    task_map: HashMap<(usize, Vec<u8>), Task>,
    /// Go AccessPath.ForcePartialOrder survives the partial candidate search.
    /// DataSource object identity keeps separate table occurrences independent.
    pub(super) forced_partial_order_paths: HashSet<(usize, i64)>,
    /// Go `SessionVars.AllocPlanColumnID`: the column-id allocator the
    /// aggregate partial/final split draws fresh columns from. `None` keeps
    /// pre-split searches working; a search that can push aggregates sets it
    /// through [`DispatchContext::with_column_ids`].
    pub column_ids: Option<&'a crate::expression_rewriter::ColumnIdAllocator>,
}

impl<'a> DispatchContext<'a> {
    /// A fresh search context.
    #[must_use]
    pub fn new(
        allocator: &'a PlanIdAllocator,
        coster: &'a dyn TaskCoster,
        skew_ratio: f64,
    ) -> Self {
        Self {
            expr_pushdown_blacklist: Default::default(),
            estimator_options: Default::default(),
            correlation_options: Default::default(),
            allocator,
            coster,
            skew_ratio,
            range_max_size: 64 * 1024 * 1024,
            // Go `vardef.DefOptSelectivityFactor`.
            selectivity_factor: crate::cost_factors::SELECTION_FACTOR,
            range_fallback_handler: None,
            expression_evaluator: &crate::ranger::points::evaluate_static,
            ordering_index_selectivity_ratio: 0.01,
            ordering_index_selectivity_threshold:
                tidb_vardef::defaults::DEF_TIDB_OPT_ORDERING_IDX_SEL_THRESH,
            allow_projection_push_down: true,
            heavy_function_optimize: true,
            // Go `vardef.DefTiDBEnableINLJoinMultiPattern` is true.
            enable_inl_join_inner_multi_pattern: true,
            // Go `vardef.DefOptLimitPushDownThreshold`.
            limit_push_down_threshold: 5_000,
            enable_paging: true,
            hash_join_concurrency: 5,
            shuffle_options: Default::default(),
            group_ndv_skew_ratio: tidb_vardef::defaults::DEF_OPT_RISK_GROUP_NDV_SKEW_RATIO,
            use_hash_join_v2: true,
            apply_cache_capacity: 0,
            index_join_probe_row_count_fix: true,
            index_join_row_count_upper_bound: false,
            enable_point_get_conversion: true,
            index_join_skyline_threshold: 1_000.0,
            // Go `vardef.DefTiDBAllowMPPExecution` is true.
            mpp_allowed: true,
            enable_skew_distinct_agg: false,
            enable_3_stage_distinct_agg: true,
            enable_3_stage_multi_distinct_agg: false,
            tiflash_pre_agg_mode: tidb_vardef::defaults::DEF_TIFLASH_PRE_AGG_MODE.to_owned(),
            partial_ordered_index_for_topn: false,
            opt_prefix_index_single_scan: true,
            mpp_warning_sink: None,
            // Go `tidb_opt_prefer_range_scan` defaults ON.
            prefer_range_scan: true,
            task_map: HashMap::new(),
            forced_partial_order_paths: HashSet::new(),
            column_ids: None,
        }
    }

    /// Uses the session's resolved histogram-estimator options.
    #[must_use]
    pub const fn with_estimator_options(
        mut self,
        options: crate::cardinality::row_count_estimator::EstimatorOptions,
    ) -> Self {
        self.estimator_options = options;
        self
    }

    /// Estimate only the predicates retained in the remote scan.
    fn pushed_filter_stats(
        &self,
        source: &crate::logical::DataSource,
        input: Option<&crate::stats_info::StatsInfo>,
        filters: &[tidb_expr::expression::Expression],
    ) -> Option<crate::stats_info::StatsInfo> {
        let selectivity = source
            .table_stats
            .as_ref()
            .and_then(|stats| {
                crate::logical::rewrite::analyzed_filter_selectivity_with_evaluator(
                    stats,
                    filters,
                    self.estimator_options,
                    self.expression_evaluator,
                )
            })
            .unwrap_or(self.selectivity_factor);
        input.map(|stats| stats.scale(selectivity, self.skew_ratio))
    }

    /// Retain the same statement settings for ordinary and merge task conversion.
    pub(crate) fn task_stats_context(
        &self,
        source: &crate::logical::DataSource,
        root_filters: &[tidb_expr::expression::Expression],
    ) -> crate::task::TaskStatsContext {
        crate::task::TaskStatsContext {
            root_filter_stats: if root_filters.is_empty() {
                None
            } else {
                source.table_stats.clone().map(std::sync::Arc::new)
            },
            estimator_options: self.estimator_options,
            scale_ndv_skew_ratio: self.skew_ratio,
        }
    }

    /// Uses the session's group-NDV blend ratio.
    #[must_use]
    pub const fn with_group_ndv_skew_ratio(mut self, ratio: f64) -> Self {
        self.group_ndv_skew_ratio = ratio;
        self
    }

    /// Uses the session's correlation-adjustment settings.
    #[must_use]
    pub const fn with_correlation_options(
        mut self,
        options: crate::cardinality::cross_estimation::CorrelationOptions,
    ) -> Self {
        self.correlation_options = options;
        self
    }

    pub(crate) fn detach_index_range(
        &self,
        conditions: &[tidb_expr::expression::Expression],
        columns: &[tidb_expr::column::Column],
        lengths: &[i64],
    ) -> Result<crate::ranger::detacher::DetachRangeResult, crate::ranger::points::PointBuilderError>
    {
        self.access_path_derivation_context()
            .detach_index_range(conditions, columns, lengths)
    }

    pub(crate) fn access_path_derivation_context(
        &self,
    ) -> crate::access_path::AccessPathDerivationContext<'_> {
        crate::access_path::AccessPathDerivationContext {
            opt_prefix_index_single_scan: self.opt_prefix_index_single_scan,
            expr_pushdown_blacklist: &self.expr_pushdown_blacklist,
            selectivity_factor: self.selectivity_factor,
            estimator_options: self.estimator_options,
            range_max_size: self.range_max_size,
            range_fallback_handler: self.range_fallback_handler,
            expression_evaluator: self.expression_evaluator,
        }
    }

    /// Attach `tidb_opt_selectivity_factor`, the pseudo estimator's start.
    #[must_use]
    pub const fn with_selectivity_factor(mut self, selectivity_factor: f64) -> Self {
        self.selectivity_factor = selectivity_factor;
        self
    }

    /// Use the live statement evaluator for every candidate's range bounds.
    #[must_use]
    pub fn with_expression_evaluator(
        mut self,
        evaluator: &'a crate::ranger::points::ExpressionEvaluator<'a>,
    ) -> Self {
        self.expression_evaluator = evaluator;
        self
    }

    /// Attach the statement's range quota and shared fallback state.
    #[must_use]
    pub const fn with_range_quota(
        mut self,
        quota: i64,
        handler: &'a tidb_util::context::RangeFallbackHandler,
    ) -> Self {
        self.range_max_size = quota;
        self.range_fallback_handler = Some(handler);
        self
    }

    /// The same context with the statement's ordering-index selectivity
    /// ratio attached.
    #[must_use]
    pub const fn with_ordering_index_selectivity_ratio(mut self, ratio: f64) -> Self {
        self.ordering_index_selectivity_ratio = ratio;
        self
    }

    /// Attaches `tidb_opt_ordering_index_selectivity_threshold`.
    #[must_use]
    pub const fn with_ordering_index_selectivity_threshold(mut self, threshold: f64) -> Self {
        self.ordering_index_selectivity_threshold = threshold;
        self
    }

    /// The same context with the session's projection-pushdown switch.
    #[must_use]
    pub const fn with_projection_push_down(mut self, allow: bool) -> Self {
        self.allow_projection_push_down = allow;
        self
    }

    /// Sets Go fix-control 56318 for heavy-function TopN planning.
    pub const fn with_heavy_function_optimize(mut self, enabled: bool) -> Self {
        self.heavy_function_optimize = enabled;
        self
    }

    /// The same context with the session's index-join inner multi-pattern
    /// switch (`tidb_enable_inl_join_inner_multi_pattern`).
    #[must_use]
    pub const fn with_inl_join_inner_multi_pattern(mut self, enable: bool) -> Self {
        self.enable_inl_join_inner_multi_pattern = enable;
        self
    }

    /// The same context with the session's TopN pushdown threshold.
    #[must_use]
    pub const fn with_limit_push_down_threshold(mut self, threshold: u64) -> Self {
        self.limit_push_down_threshold = threshold;
        self
    }

    /// The same context with the session's coprocessor-paging switch.
    #[must_use]
    pub const fn with_paging(mut self, enable: bool) -> Self {
        self.enable_paging = enable;
        self
    }

    /// The same context with the resolved hash-join concurrency.
    #[must_use]
    pub const fn with_use_hash_join_v2(mut self, enabled: bool) -> Self {
        self.use_hash_join_v2 = enabled;
        self
    }

    pub const fn with_hash_join_concurrency(mut self, concurrency: usize) -> Self {
        self.hash_join_concurrency = concurrency;
        self
    }

    /// The same context with Go's per-session Apply-cache quota.
    #[must_use]
    pub const fn with_apply_cache_capacity(mut self, capacity: i64) -> Self {
        self.apply_cache_capacity = capacity;
        self
    }

    /// Uses the session's resolved Fix44855 value for IndexJoin probe sizing.
    #[must_use]
    pub const fn with_index_join_probe_row_count_fix(mut self, enabled: bool) -> Self {
        self.index_join_probe_row_count_fix = enabled;
        self
    }

    /// Uses the session's Fix44855 value with the upper-bound default (false).
    #[must_use]
    pub const fn with_index_join_row_count_upper_bound(mut self, enabled: bool) -> Self {
        self.index_join_row_count_upper_bound = enabled;
        self
    }

    /// Uses the session's resolved Fix45132 ratio for IndexJoin skyline
    /// pruning. Values at or below zero disable the empirical comparison.
    #[must_use]
    pub const fn with_index_join_skyline_threshold(mut self, threshold: f64) -> Self {
        self.index_join_skyline_threshold = threshold;
        self
    }

    /// Carries the session's resolved Go fix 52592 conversion permission.
    #[must_use]
    pub const fn with_point_get_conversion(mut self, enabled: bool) -> Self {
        self.enable_point_get_conversion = enabled;
        self
    }

    /// The same context with Go's resolved `IsMPPAllowed()` value.
    #[must_use]
    pub const fn with_mpp_allowed(mut self, allowed: bool) -> Self {
        self.mpp_allowed = allowed;
        self
    }

    /// Carries Go's `EnableSkewDistinctAgg` session switch into physical
    /// aggregation enumeration.
    #[must_use]
    pub const fn with_enable_skew_distinct_agg(mut self, enabled: bool) -> Self {
        self.enable_skew_distinct_agg = enabled;
        self
    }

    /// Carries Go's three-stage distinct aggregate switch into physical
    /// aggregation enumeration and attachment.
    #[must_use]
    pub const fn with_enable_3_stage_distinct_agg(mut self, enabled: bool) -> Self {
        self.enable_3_stage_distinct_agg = enabled;
        self
    }

    /// Carries Go's three-stage multi-distinct aggregate switch.
    #[must_use]
    pub const fn with_enable_3_stage_multi_distinct_agg(mut self, enabled: bool) -> Self {
        self.enable_3_stage_multi_distinct_agg = enabled;
        self
    }

    /// Carries Go's TiFlash hash-aggregate pre-aggregation mode.
    #[must_use]
    pub fn with_tiflash_pre_agg_mode(mut self, mode: impl Into<String>) -> Self {
        self.tiflash_pre_agg_mode = mode.into();
        self
    }

    /// Carries the session's prefix-index partial-order TopN switch.
    #[must_use]
    pub const fn with_partial_ordered_index_for_topn(mut self, enabled: bool) -> Self {
        self.partial_ordered_index_for_topn = enabled;
        self
    }

    /// Carries the session's prefix-index single-scan switch.
    #[must_use]
    pub const fn with_opt_prefix_index_single_scan(mut self, enabled: bool) -> Self {
        self.opt_prefix_index_single_scan = enabled;
        self
    }

    /// Attach the session statement-context sink used by physical refusal
    /// warnings. The planner remains usable without a session in unit tests.
    #[must_use]
    pub const fn with_mpp_warning_sink(mut self, sink: &'a dyn MppWarningSink) -> Self {
        self.mpp_warning_sink = Some(sink);
        self
    }

    /// Go `SessionVars.GetAllowPreferRangeScan()`.
    #[must_use]
    pub const fn with_prefer_range_scan(mut self, value: bool) -> Self {
        self.prefer_range_scan = value;
        self
    }

    /// The same context with the session's column-id allocator attached.
    #[must_use]
    pub fn with_column_ids(
        mut self,
        column_ids: &'a crate::expression_rewriter::ColumnIdAllocator,
    ) -> Self {
        self.column_ids = Some(column_ids);
        self
    }
}

/// Go memoizes the complete physical-property hash, not a formatted subset.
fn prop_key(prop: &PhysicalProperty) -> Vec<u8> {
    prop.hash_code()
}

fn exhaust_physical_plans(
    plan: &LogicalPlan,
    prop: &PhysicalProperty,
    ctx: &DispatchContext<'_>,
) -> Result<(Vec<Vec<PhysicalPlan>>, bool), PlanError> {
    // Go also returns hintWorksWithProp: candidates may exist while none
    // satisfies the hint. The caller then retries with an enforced order.
    let one = |plans: Vec<PhysicalPlan>| {
        let slices = if plans.is_empty() {
            Vec::new()
        } else {
            vec![plans]
        };
        (slices, true)
    };
    match plan {
        LogicalPlan::Selection(op) => Ok(one(
            physical::exhaust_physical_plans_4_logical_selection_with_mpp(
                op,
                prop,
                ctx.allocator,
                ctx.skew_ratio,
                ctx.mpp_allowed,
                &ctx.expr_pushdown_blacklist,
            ),
        )),
        LogicalPlan::Projection(op) => Ok(one(
            physical::exhaust_physical_plans_4_logical_projection_with_mpp(
                op,
                prop,
                ctx.allocator,
                ctx.skew_ratio,
                ctx.allow_projection_push_down,
                ctx.mpp_allowed,
                &ctx.expr_pushdown_blacklist,
            ),
        )),
        LogicalPlan::Sort(op) => Ok(one(physical::exhaust_physical_plans_4_logical_sort(
            op,
            prop,
            ctx.allocator,
            ctx.skew_ratio,
        ))),
        LogicalPlan::Limit(op) => Ok(one(physical::exhaust_physical_plans_4_logical_limit(
            op,
            prop,
            ctx.allocator,
        ))),
        LogicalPlan::Lock(op) => Ok(one(physical::exhaust_physical_plans_4_logical_lock(
            op,
            prop,
            ctx.allocator,
            ctx.skew_ratio,
        ))),
        LogicalPlan::MaxOneRow(op) => {
            if !prop.is_sort_item_empty() || prop.is_flash_prop() {
                if let Some(sink) = ctx.mpp_warning_sink {
                    sink.raise_mpp_warning(
                        "MPP mode may be blocked because operator `MaxOneRow` is not supported now.",
                    );
                }
            }
            Ok(one(physical::exhaust_physical_plans_4_logical_max_one_row(
                op,
                prop,
                ctx.allocator,
            )))
        }
        LogicalPlan::UnionAll(op) => Ok(one(physical::exhaust_physical_plans_4_logical_union_all(
            op,
            prop,
            ctx.allocator,
            ctx.skew_ratio,
        ))),
        LogicalPlan::PartitionUnionAll(op) => Ok(one(
            physical::exhaust_physical_plans_4_logical_partition_union_all(
                op,
                prop,
                ctx.allocator,
                ctx.skew_ratio,
            ),
        )),
        LogicalPlan::Sequence(op) => Ok(one(physical::exhaust_physical_plans_4_logical_sequence(
            op,
            prop,
            ctx.allocator,
        ))),
        LogicalPlan::TopN(op) => {
            // `ExhaustPhysicalPlans4LogicalTopN` (`physical_topn.go:55`):
            // admitted only when the required order matches the by-items
            // (`MatchItems`); TWO preference slices — the TopN operators
            // and the LIMIT half. The TopN half needs the expression-borne
            // push-down machinery (`getPhysTopN`/`CanExprsPushDown`) and
            // refuses as an EMPTY slice here, named; the LIMIT half rides
            // the keep-order paths (`getPhysLimits`).
            if !physical::match_items(prop, &op.by_items) {
                return Ok((Vec::new(), true));
            }
            // Go's two preference slices, in order: the TopN operators
            // (`getPhysTopN`), then the LIMIT half (`getPhysLimits`).
            let mut slices = Vec::with_capacity(2);
            let topns = physical::get_phys_topn(
                op,
                prop,
                ctx.allocator,
                ctx.mpp_allowed,
                ctx.allow_projection_push_down,
                ctx.heavy_function_optimize,
                ctx.partial_ordered_index_for_topn,
            );
            if !topns.is_empty() {
                slices.push(topns);
            }
            let limits = physical::get_phys_limits(op, prop, ctx.allocator);
            if !limits.is_empty() {
                slices.push(limits);
            }
            Ok((slices, true))
        }
        LogicalPlan::Expand(op) => Ok((
            vec![physical::expand::exhaust(
                op,
                prop,
                ctx.allocator,
                ctx.skew_ratio,
                ctx.mpp_allowed,
            )],
            prop.sort_items.is_empty(),
        )),
        LogicalPlan::Window(op) => Ok(one(physical::exhaust_physical_plans_4_logical_window(
            op,
            prop,
            ctx.allocator,
            ctx.skew_ratio,
        ))),
        LogicalPlan::Aggregation(op) => {
            // `ExhaustPhysicalPlans4LogicalAggregation`
            // (`base_physical_agg.go:935`): Go enumerates HashAgg first and
            // immediately returns it when HASH_AGG applies, then enumerates
            // StreamAgg and immediately returns it when STREAM_AGG applies.
            // With no applicable hint both families share one cost search in
            // that same hash-then-stream order.
            let mut hash_aggs = physical::get_hash_aggs_with_mpp_options(
                op,
                prop,
                ctx.allocator,
                ctx.skew_ratio,
                ctx.mpp_allowed,
                ctx.enable_skew_distinct_agg,
                ctx.enable_3_stage_distinct_agg,
                ctx.enable_3_stage_multi_distinct_agg,
                &ctx.tiflash_pre_agg_mode,
                &ctx.expr_pushdown_blacklist,
            );
            if !hash_aggs.is_empty()
                && op.prefer_agg_type & crate::expression_rewriter::PREFER_HASH_AGG != 0
            {
                return Ok((vec![hash_aggs], true));
            }
            let stream_aggs = physical::get_stream_aggs(op, prop, ctx.allocator, ctx.skew_ratio);
            if !stream_aggs.is_empty()
                && op.prefer_agg_type & crate::expression_rewriter::PREFER_STREAM_AGG != 0
            {
                return Ok((vec![stream_aggs], true));
            }
            hash_aggs.extend(stream_aggs);
            Ok((one(hash_aggs).0, op.prefer_agg_type == 0))
        }
        LogicalPlan::Join(op) => {
            use crate::find_best_task::JoinStrategy;
            use crate::plan_builder::from::join_hint_flags;

            let reduced = crate::find_best_task::project_one_join(op, plan)?;
            let (left_columns, right_columns, is_null_eq, _) = op.get_join_keys();
            let column = |columns: &[tidb_expr::column::Column], id: i64| {
                columns
                    .iter()
                    .find(|column| column.unique_id == id)
                    .cloned()
            };
            let mut joins = Vec::new();
            for candidate in
                crate::find_best_task::exhaust_join(&reduced, prop, ctx.use_hash_join_v2)
            {
                let strategy = candidate.strategy.clone();
                // Go's INL_JOIN/INL_HASH_JOIN/INL_MERGE_JOIN hints are
                // family-and-side selectors.  An index candidate that points
                // at the opposite side is not an ordinary fallback: when no
                // candidate satisfies the requested side, Go falls back to a
                // non-index join and reports the hint as inapplicable.  Drop
                // those opposite-side candidates before child enumeration so
                // an invalid force hint cannot silently choose a normal
                // index join.
                if let (crate::logical::LogicalPlan::Join(join), JoinStrategy::Index { .. }) =
                    (plan, &strategy)
                {
                    let has_force_index_hint = join.prefer_any(&[
                        join_hint_flags::LEFT_AS_INLJ_INNER,
                        join_hint_flags::RIGHT_AS_INLJ_INNER,
                        join_hint_flags::LEFT_AS_INLHJ_INNER,
                        join_hint_flags::RIGHT_AS_INLHJ_INNER,
                        join_hint_flags::LEFT_AS_INLMJ_INNER,
                        join_hint_flags::RIGHT_AS_INLMJ_INNER,
                    ]);
                    if has_force_index_hint && !index_join_candidate_matches_hint(join, &strategy) {
                        continue;
                    }
                }
                let filtered = match &strategy {
                    JoinStrategy::Hash(_) => op.prefer_any(&[join_hint_flags::NO_HASH_JOIN]),
                    JoinStrategy::Merge { .. } => op.prefer_any(&[join_hint_flags::NO_MERGE_JOIN]),
                    JoinStrategy::Index { kind, .. } => match kind {
                        crate::plan_cost_ver2::IndexJoinKind::IndexJoin => {
                            op.prefer_any(&[join_hint_flags::NO_INDEX_JOIN])
                        }
                        crate::plan_cost_ver2::IndexJoinKind::IndexHashJoin => {
                            op.prefer_any(&[join_hint_flags::NO_INDEX_HASH_JOIN])
                        }
                        crate::plan_cost_ver2::IndexJoinKind::IndexMergeJoin => {
                            op.prefer_any(&[join_hint_flags::NO_INDEX_MERGE_JOIN])
                        }
                    },
                };
                if filtered {
                    continue;
                }
                let mut child_props = candidate.child_props;
                if matches!(&strategy, JoinStrategy::Merge { .. })
                    && !child_props[0].can_add_enforcer
                {
                    // GetMergeJoin adjusts naturally ordered children; Go's
                    // getEnforcedMergeJoin deliberately keeps unbounded inputs.
                    let join_rows = op.base.base.stats_info().map_or(
                        0.0, crate::stats_info::StatsInfo::row_count,
                    );
                    if prop.expected_cnt < join_rows {
                        for (child_prop, child) in child_props.iter_mut().zip(plan.children()) {
                            let child_rows = child.stats_info().map_or(
                                0.0, crate::stats_info::StatsInfo::row_count,
                            );
                            child_prop.expected_cnt = calc_child_expected_cnt(
                                prop, child_rows, join_rows, ctx.ordering_index_selectivity_ratio,
                            );
                        }
                    }
                }
                if let JoinStrategy::Index {
                    outer_idx,
                    table_range_scan,
                    ..
                } = &strategy
                {
                    let inner_idx = 1 - *outer_idx;
                    let (outer_join_keys, inner_join_keys) = if *outer_idx == 0 {
                        (left_columns.clone(), right_columns.clone())
                    } else {
                        (right_columns.clone(), left_columns.clone())
                    };
                    let outer_rows = plan
                        .children()
                        .get(*outer_idx)
                        .and_then(LogicalPlan::stats_info)
                        .map_or(0.0, crate::stats_info::StatsInfo::row_count);
                    // Go `enumerateIndexJoinByOuterIdx`: `avgInnerRowCnt =
                    // p.EqualCondOutCnt / buildRows`. The equal-condition
                    // output is what the per-outer-row probe sees; the join's
                    // own profile is already scaled by every OTHER condition.
                    let joined_rows = op.equal_cond_out_cnt;
                    // Go `constructIndexJoin` (`exhaust_physical_plans.go`):
                    // the outer side's expectation is
                    // `CalcChildExpectedCnt(prop, outerRows, joinRows)`, i.e.
                    // unbounded unless the join is asked for fewer rows than
                    // it estimates, and scaled in proportion when it is. The
                    // enumerator's placeholder handed the join's own
                    // expectation to the outer scan, which (under a
                    // MaxOneRow's expectation of 2 grown through a stream
                    // aggregate) clipped an 800k-row scan to 64k rows and
                    // let an IndexJoin underprice the hash join Go picks.
                    let join_rows = op
                        .base
                        .base
                        .stats_info()
                        .map_or(0.0, crate::stats_info::StatsInfo::row_count);
                    child_props[*outer_idx].expected_cnt = calc_child_expected_cnt(
                        prop,
                        outer_rows,
                        join_rows,
                        ctx.ordering_index_selectivity_ratio,
                    );
                    child_props[inner_idx].index_join_prop =
                        Some(crate::physical_property::IndexJoinRuntimeProp {
                            other_conditions: op.other_conditions.clone(),
                            outer_join_keys,
                            inner_join_keys,
                            avg_inner_row_count: if outer_rows > 0.0 {
                                joined_rows / outer_rows
                            } else {
                                0.0
                            },
                            table_range_scan: *table_range_scan,
                        });
                }
                let mut base = physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    op.base.base.tp(),
                    op.base.base.query_block_offset(),
                );
                base.base.set_stats(op.base.base.stats_info().map(|stats| {
                    if matches!(&strategy, JoinStrategy::Merge { .. }) {
                        stats.scale_by_expect_cnt(prop.expected_cnt, ctx.skew_ratio)
                    } else {
                        stats.clone()
                    }
                }));
                base.base.set_schema(op.base.base.schema().cloned());
                base.set_children_req_props(child_props.into_iter().map(Some).collect());
                let physical = match strategy {
                    JoinStrategy::Hash(shape) => {
                        PhysicalPlan::HashJoin(physical::PhysicalHashJoin {
                            base,
                            concurrency: ctx.hash_join_concurrency,
                            join_type: op.join_type,
                            inner_child_idx: shape.inner_idx,
                            use_outer_to_build: shape.use_outer_to_build,
                            left_join_keys: left_columns.clone(),
                            right_join_keys: right_columns.clone(),
                            is_null_eq: is_null_eq.clone(),
                            equal_conditions: op.equal_conditions.clone(),
                            na_equal_conditions: op.na_eq_conditions.clone(),
                            left_conditions: op.left_conditions.clone(),
                            right_conditions: op.right_conditions.clone(),
                            other_conditions: op.other_conditions.clone(),
                            default_values: op.default_values.clone(),
                        })
                    }
                    JoinStrategy::Merge {
                        left_keys,
                        right_keys,
                        desc,
                    } => {
                        let Some(left_join_keys) = left_keys
                            .into_iter()
                            .map(|id| column(&left_columns, id))
                            .collect::<Option<Vec<_>>>()
                        else {
                            continue;
                        };
                        let Some(right_join_keys) = right_keys
                            .into_iter()
                            .map(|id| column(&right_columns, id))
                            .collect::<Option<Vec<_>>>()
                        else {
                            continue;
                        };
                        PhysicalPlan::MergeJoin(physical::PhysicalMergeJoin {
                            base,
                            join_type: op.join_type,
                            left_join_keys,
                            right_join_keys,
                            is_null_eq: Vec::new(),
                            left_conditions: op.left_conditions.clone(),
                            right_conditions: op.right_conditions.clone(),
                            other_conditions: op.other_conditions.clone(),
                            default_values: op.default_values.clone(),
                            desc,
                        })
                    }
                    JoinStrategy::Index {
                        outer_idx,
                        kind,
                        keep_outer_order,
                        ..
                    } => {
                        let (outer_join_keys, inner_join_keys) = if outer_idx == 0 {
                            (left_columns.clone(), right_columns.clone())
                        } else {
                            (right_columns.clone(), left_columns.clone())
                        };
                        PhysicalPlan::IndexJoin(physical::PhysicalIndexJoin {
                            base,
                            join_type: op.join_type,
                            inner_child_idx: 1 - outer_idx,
                            kind,
                            keep_outer_order,
                            inner_access_table_id: None,
                            inner_access_index_id: None,
                            inner_range_bare: false,
                            inner_access_conditions: Vec::new(),
                            left_join_keys: left_columns.clone(),
                            right_join_keys: right_columns.clone(),
                            outer_join_keys,
                            inner_join_keys,
                            is_null_eq: is_null_eq.clone(),
                            left_conditions: op.left_conditions.clone(),
                            right_conditions: op.right_conditions.clone(),
                            other_conditions: op.other_conditions.clone(),
                            default_values: op.default_values.clone(),
                            outer_hash_keys: Vec::new(),
                            inner_hash_keys: Vec::new(),
                            equal_conditions: op.equal_conditions.clone(),
                            ranges: crate::ranger::types::Ranges::default(),
                            key_off2_idx_off: Vec::new(),
                            idx_col_lens: Vec::new(),
                            compare_filters: None,
                            range_rebuild: None,
                        })
                    }
                };
                joins.push(physical);
            }
            // Go returns a forced HashJoin/MergeJoin family immediately.
            // IndexJoin hints remain undecided until the inner task builds.
            for hash in [true, false] {
                if joins.iter().any(|candidate| {
                    (if hash {
                        matches!(candidate, PhysicalPlan::HashJoin(_))
                    } else {
                        matches!(candidate, PhysicalPlan::MergeJoin(_))
                    }) && logical_join_hint_applies(plan, candidate)
                }) {
                    joins.retain(|candidate| {
                        if hash {
                            matches!(candidate, PhysicalPlan::HashJoin(_))
                        } else {
                            matches!(candidate, PhysicalPlan::MergeJoin(_))
                        }
                    });
                    return Ok(one(joins));
                }
            }
            Ok((one(joins).0, op.prefer_join_type == 0))
        }
        LogicalPlan::Apply(op) => {
            // Go `exhaustPhysicalPlans4LogicalApply`: Apply can preserve only
            // an order supplied by its OUTER child and never runs as MPP.
            let outer_schema = plan
                .children()
                .first()
                .and_then(LogicalPlan::schema)
                .ok_or_else(|| PlanError::internal("LogicalApply has no outer schema"))?;
            if prop.task_tp == TaskType::Mpp
                || !prop.sort_items.iter().all(|item| {
                    outer_schema
                        .columns
                        .iter()
                        .any(|column| column.unique_id == item.col.unique_id)
                })
            {
                return Ok((Vec::new(), true));
            }

            let outer_stats = plan.children().first().and_then(LogicalPlan::stats_info);
            let outer_rows = outer_stats.map_or(0.0, crate::stats_info::StatsInfo::row_count);
            let stats = op.base().base.stats_info().cloned();
            let apply_rows = stats
                .as_ref()
                .map_or(0.0, crate::stats_info::StatsInfo::row_count);
            // Go only adjusts an Apply's outer requirement for ordered output.
            // Reuse the same helper as index and natural merge joins.
            let outer_expected_cnt = if prop.is_sort_item_empty() {
                f64::MAX
            } else {
                calc_child_expected_cnt(
                    prop, outer_rows, apply_rows, ctx.ordering_index_selectivity_ratio,
                )
            };
            let outer_prop = PhysicalProperty {
                sort_items: prop.sort_items.clone(),
                task_tp: TaskType::Root,
                expected_cnt: outer_expected_cnt,
                can_add_enforcer: false,
                mpp_partition_cols: Vec::new(),
                mpp_partition_tp: Default::default(),
                sort_items_for_partition: Vec::new(),
                cte_producer_status: prop.cte_producer_status,
                vector_prop: Default::default(),
                no_cop_push_down: true,
                advisory_sort_items: Vec::new(),
                index_join_prop: None,
                partial_order_info: prop.partial_order_info.clone(),
            };
            let inner_prop = PhysicalProperty {
                cte_producer_status: prop.cte_producer_status,
                no_cop_push_down: prop.no_cop_push_down,
                ..PhysicalProperty::default()
            };

            // Cache hits reuse correlated keys across outer rows. Apply output
            // may shrink (semi join) or multiply (LATERAL), so it is not the
            // population whose NDV determines reuse.
            let can_use_cache = outer_stats.is_some_and(|stats| {
                if stats.row_count() == 0.0 || ctx.apply_cache_capacity <= 0 {
                    return false;
                }
                let ids = op
                    .cor_cols
                    .iter()
                    .map(|column| column.column.unique_id)
                    .collect::<Vec<_>>();
                let (ndv, _) = crate::cardinality::derive_stats::
                    estimate_cols_ndv_with_matched_len_and_skew_ratio(
                        &ids,
                        stats,
                        ctx.group_ndv_skew_ratio,
                    );
                1.0 - ndv / stats.row_count() > 0.1
            });
            let (left_join_keys, right_join_keys, is_null_eq, _) = op.join.get_join_keys();
            let mut base = physical::BasePhysicalPlan::new(
                ctx.allocator,
                crate::logical::LogicalApply::TYPE,
                op.base().base.query_block_offset(),
            );
            base.base.set_stats(
                stats.map(|stats| stats.scale_by_expect_cnt(prop.expected_cnt, ctx.skew_ratio)),
            );
            base.base.set_schema(op.base().base.schema().cloned());
            base.set_children_req_props(vec![Some(outer_prop), Some(inner_prop)]);
            let apply = physical::PhysicalApply {
                hash_join: physical::PhysicalHashJoin {
                    base,
                    concurrency: ctx.hash_join_concurrency,
                    join_type: op.join.join_type,
                    inner_child_idx: 1,
                    use_outer_to_build: false,
                    left_join_keys,
                    right_join_keys,
                    is_null_eq,
                    equal_conditions: op.join.equal_conditions.clone(),
                    na_equal_conditions: op.join.na_eq_conditions.clone(),
                    left_conditions: op.join.left_conditions.clone(),
                    right_conditions: op.join.right_conditions.clone(),
                    other_conditions: op.join.other_conditions.clone(),
                    default_values: op.join.default_values.clone(),
                },
                can_use_cache,
                concurrency: 0,
                keep_order: !prop.is_sort_item_empty(),
                outer_schema: op.cor_cols.clone(),
                no_decorrelate: op.no_decorrelate,
            };
            Ok(one(vec![PhysicalPlan::Apply(apply)]))
        }
        other => Err(PlanError::internal(format!(
            "exhaustPhysicalPlans over {} is not ported to the dispatcher",
            other.tp()
        ))),
    }
}

/// Go `findBestTask` (`find_best_task.go:605`) over the enum world.
pub fn find_best_task(
    plan: &LogicalPlan,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    // Operators with their own findBestTask override are routed first, as
    // Go's function-pointer wiring does. They are born in root tasks and
    // are never memoized poorly: the general tail below still stores them.
    let key = (std::ptr::from_ref(plan).addr(), prop_key(prop));
    if let Some(cached) = ctx.task_map.get(&key) {
        return Ok(cached.copy());
    }
    let mut best = find_best_task_uncached(plan, prop, ctx)?;
    // `IndexJoinProp` is not merely a costing hint: Go's selected inner task
    // must return `IndexJoinInfo` from the access path that accepted the
    // runtime probe. A structurally valid task that lost this bottom-up
    // receipt is not an index-join inner candidate. Mark it invalid here so
    // enumeration can continue with MergeJoin/HashJoin instead of aborting
    // later in `completePhysicalIndexJoin`.
    if prop.index_join_prop.is_some() && !task_has_index_join_info(&best) {
        best = Task::invalid_task();
    }
    if let Some(plan) = best.plan_mut() {
        apply_reader_cost_side_effects(plan, ctx.enable_paging);
    }
    ctx.task_map.insert(key, best.copy());
    Ok(best)
}

/// Go's lookup-reader cost functions set `PhysicalIndexLookUpReader.Paging`
/// on the winning physical tree. The Rust coster is deliberately read-only,
/// so apply that same cost side effect before memoizing the selected task.
fn apply_reader_cost_side_effects(plan: &mut PhysicalPlan, enable_paging: bool) {
    match plan {
        PhysicalPlan::TableReader(reader) => {
            if let Some(child) = reader.table_plan.as_deref_mut() {
                apply_reader_cost_side_effects(child, enable_paging);
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(child) = reader.index_plan.as_deref_mut() {
                apply_reader_cost_side_effects(child, enable_paging);
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            reader.paging = enable_paging
                && reader.expect_cnt > 0
                && reader.expect_cnt <= crate::plan_cost_ver2::PAGING_THRESHOLD;
            if let Some(child) = reader.index_plan.as_deref_mut() {
                apply_reader_cost_side_effects(child, enable_paging);
            }
            if let Some(child) = reader.table_plan.as_deref_mut() {
                apply_reader_cost_side_effects(child, enable_paging);
            }
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            for child in &mut reader.partial_plans_raw {
                apply_reader_cost_side_effects(child, enable_paging);
            }
            if let Some(child) = reader.table_plan.as_deref_mut() {
                apply_reader_cost_side_effects(child, enable_paging);
            }
        }
        _ => {}
    }
    for child in plan.base_mut().children_mut() {
        apply_reader_cost_side_effects(child, enable_paging);
    }
}

fn task_has_index_join_info(task: &Task) -> bool {
    match task {
        Task::Root(root) => root.index_join_info.is_some(),
        Task::Cop(cop) => cop.index_join_info.is_some(),
        Task::Mpp(_) => false,
    }
}

/// Go `admitIndexJoinInnerChildPattern` (`exhaust_physical_plans.go:645`):
/// whether `plan` may appear on the inner side of an index join. `DataSource`
/// is unconditionally admitted here (barring a TiFlash-preferred read, which
/// this port's TiFlash-less DataSource never carries); Selection, Projection,
/// an inner-type Join and a group-key-matching Aggregation are admitted only
/// when `multi_pattern` (Go's `tidb_enable_inl_join_inner_multi_pattern`,
/// default ON) holds; everything else, including Sort/Limit/TopN/Window and
/// any non-inner Join, is refused -- Go's own comment: "index join inner side
/// couldn't allow join, sort, limit, because they are Optimization Fence."
fn admits_index_join_inner_child_pattern(
    plan: &LogicalPlan,
    property: &crate::physical_property::IndexJoinRuntimeProp,
    multi_pattern: bool,
) -> bool {
    match plan {
        LogicalPlan::DataSource(ds) => {
            ds.prefer_store_type & crate::logical::data_source::PREFER_TIFLASH == 0
        }
        LogicalPlan::Selection(_) | LogicalPlan::Projection(_) => multi_pattern,
        LogicalPlan::Join(join) => {
            multi_pattern && join.join_type == crate::find_best_task::LogicalJoinType::Inner
        }
        LogicalPlan::Aggregation(agg) => {
            if !multi_pattern {
                return false;
            }
            // Go `checkIndexJoinInnerTaskWithAgg`: an inner join key that
            // reaches the DataSource must be a bare GROUP BY column, so
            // grouping cannot split rows the probe expects to find intact.
            let groups =
                tidb_expr::simple_expr::extract_columns_from_expressions(&agg.group_by_items, None);
            let mut child = plan;
            let schema = loop {
                if let LogicalPlan::DataSource(ds) = child {
                    break ds.base.base.schema();
                }
                let [next] = child.children() else {
                    return false;
                };
                child = next;
            };
            let Some(schema) = schema else {
                return false;
            };
            property
                .inner_join_keys()
                .iter()
                .filter(|key| schema.contains(key))
                .all(|key| {
                    groups
                        .iter()
                        .any(|column| column.unique_id == key.unique_id)
                })
        }
        LogicalPlan::UnionScan(_) => true,
        _ => false,
    }
}

fn find_best_task_uncached(
    plan: &LogicalPlan,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    match plan {
        LogicalPlan::TableDual(op) => {
            return Ok(physical::find_best_task_4_logical_table_dual(
                op,
                prop,
                ctx.allocator,
            ));
        }
        LogicalPlan::CTETable(op) => {
            return Ok(physical::find_best_task_4_logical_cte_table(
                op,
                prop,
                ctx.allocator,
            ));
        }
        LogicalPlan::CTE(op) if op.base.children().is_empty() => {
            return physical::find_best_task_4_logical_cte(op, prop, ctx.allocator);
        }
        LogicalPlan::Show(op) => {
            return Ok(physical::find_best_task_4_logical_show(
                op,
                prop,
                ctx.allocator,
            ));
        }
        LogicalPlan::ShowDDLJobs(op) => {
            return Ok(physical::find_best_task_4_logical_show_ddl_jobs(
                op,
                prop,
                ctx.allocator,
            ));
        }
        LogicalPlan::DataSource(op) => {
            return find_best_task_4_logical_data_source(op, prop, ctx);
        }
        LogicalPlan::MemTable(op) => {
            return find_best_task_4_logical_mem_table(plan, op, prop, ctx);
        }
        _ => {}
    }

    // `findBestTask` (`find_best_task.go:624`): before anything else, an
    // operator asked to serve as an index join's inner side must match one
    // of Go's admitted walk-through patterns (Selection, Projection, an
    // inner-type Join, a group-key-matching Aggregation, or UnionScan --
    // `admitIndexJoinInnerChildPattern`). Everything else (Sort, Limit,
    // TopN, Window, a non-inner Join, ...) is an "optimization fence" and is
    // refused immediately, even before `exhaustPhysicalPlans` runs, exactly
    // as Go's own comment states. `DataSource` has its own override above
    // and never reaches this generic tail.
    if let Some(index_join_prop) = prop.index_join_prop.as_ref() {
        if !admits_index_join_inner_child_pattern(
            plan,
            index_join_prop,
            ctx.enable_inl_join_inner_multi_pattern,
        ) {
            return Ok(Task::invalid_task());
        }
    }

    // Go checks whether the operator itself can satisfy a coprocessor or MPP
    // property before applying the generic non-root refusal. `LogicalMaxOneRow`
    // fails that check and raises its enforced-MPP warning even though no
    // physical plan is enumerated. Keep that side effect before the same
    // invalid-task result; otherwise the warning disappears at this early
    // return.
    if matches!(
        prop.task_tp,
        TaskType::CopSingleRead | TaskType::CopMultiRead | TaskType::Mpp
    ) && matches!(plan, LogicalPlan::MaxOneRow(_))
    {
        if let Some(sink) = ctx.mpp_warning_sink {
            sink.raise_mpp_warning(
                "MPP mode may be blocked because operator `MaxOneRow` is not supported now.",
            );
        }
    }

    // `prop.TaskTp != RootTaskType && !IsFlashProp()` — with no TiFlash
    // tier: any non-root requirement is the invalid task, Go's own early
    // answer ("Currently all plan cannot totally push down to TiKV").
    if prop.task_tp != TaskType::Root {
        return Ok(Task::invalid_task());
    }

    let mut can_add_enforcer = prop.can_add_enforcer;

    let mut new_prop = prop.clone_essential_fields();
    // Go restores `IndexJoinProp` immediately after
    // `CloneEssentialFields`, whose contract deliberately omits it. The
    // operator-specific `admitIndexJoinProp(s)` functions then decide which
    // child may inherit it.
    new_prop.index_join_prop = prop.index_join_prop.clone();
    let (mut plans_fits_prop, hint_works_with_prop) = exhaust_physical_plans(plan, &new_prop, ctx)?;
    if !hint_works_with_prop && !new_prop.is_sort_item_empty() && new_prop.index_join_prop.is_none()
    {
        can_add_enforcer = true;
    }

    let plans_need_enforce = if can_add_enforcer {
        let mut empty = new_prop;
        empty.sort_items = Vec::new();
        empty.sort_items_for_partition = Vec::new();
        empty.expected_cnt = f64::MAX;
        empty.mpp_partition_cols.clear();
        empty.mpp_partition_tp = Default::default();
        let (mut enforced, hint_can_work) = exhaust_physical_plans(plan, &empty, ctx)?;
        // IndexJoin hint applicability is known only after child tasks build.
        let contains_index_join = |slices: &[Vec<PhysicalPlan>]| {
            slices
                .iter()
                .flatten()
                .any(|p| matches!(p, PhysicalPlan::IndexJoin(_)))
        };
        if !contains_index_join(&plans_fits_prop) && !contains_index_join(&enforced) {
            if hint_can_work && !hint_works_with_prop {
                plans_fits_prop.clear();
            }
            if !hint_can_work && !hint_works_with_prop && !prop.can_add_enforcer {
                enforced.clear();
            }
        }
        enforced
    } else {
        Vec::new()
    };

    let (best_task, preferred) =
        enumerate_physical_plans_4_task(plan, &plans_fits_prop, prop, false, ctx)?;
    if preferred && !best_task.invalid() {
        return Ok(best_task);
    }
    let (cur_task, preferred) =
        enumerate_physical_plans_4_task(plan, &plans_need_enforce, prop, true, ctx)?;
    if (preferred && !cur_task.invalid()) || compare_task_cost(ctx.coster, &cur_task, &best_task)? {
        return Ok(cur_task);
    }
    Ok(best_task)
}

fn find_best_task_4_logical_mem_table(
    logical: &LogicalPlan,
    mem_table: &crate::logical::LogicalMemTable,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    if prop.index_join_prop.is_some()
        || prop.mpp_partition_tp != crate::physical_property::MppPartitionType::Any
    {
        return Ok(Task::invalid_task());
    }

    if prop.can_add_enforcer {
        let mut direct_prop = prop.clone();
        direct_prop.can_add_enforcer = false;
        let direct = find_best_task(logical, &direct_prop, ctx)?;
        if !direct.invalid() {
            return Ok(direct);
        }

        let mut unordered_prop = direct_prop;
        unordered_prop.sort_items.clear();
        let unordered = find_best_task(logical, &unordered_prop, ctx)?;
        return enforce_property_in(prop, unordered, ctx.allocator, ctx.expression_evaluator);
    }

    if !prop.sort_items.is_empty() {
        return Ok(Task::invalid_task());
    }

    let mut base = physical::BasePhysicalPlan::new(
        ctx.allocator,
        crate::logical::LogicalMemTable::TYPE,
        mem_table.base.base.query_block_offset(),
    );
    base.base
        .set_stats(mem_table.base.base.stats_info().cloned());
    base.base.set_schema(mem_table.base.base.schema().cloned());
    // Go's `buildMemTable` preserves the logical source's output names on the
    // physical scan. The result-field adapter uses these names for wildcard
    // queries; without them, virtual INFORMATION_SCHEMA tables expose
    // synthetic `Column#N` headers instead of their declared columns.
    base.base
        .set_output_names(mem_table.base.base.output_names().to_vec());
    let physical = PhysicalPlan::MemTable(physical::PhysicalMemTable {
        base,
        db_name: mem_table.db_name.clone(),
        table_name: mem_table.table_name.clone(),
        columns: mem_table.columns.clone(),
        query_time_range: mem_table.query_time_range.clone(),
    });
    let mut root = crate::task::RootTask::default();
    root.set_plan(physical);
    Ok(Task::Root(root))
}

/// Go `tryToGetDualTask` (`find_best_task.go:749`): a pushed-down constant
/// that evaluates false turns the whole source into a dual — Go's
/// `WHERE FALSE` short-circuit, before any path is considered.
///
/// Go runs `expression.EvalBool`, which coerces EVERY constant type; this
/// port answers the spellings the rewriter produces for a constant-false
/// predicate — integer zero and NULL — and leaves other constant types
/// unshort-circuited rather than mis-coercing them (the source still plans,
/// just without the dual fast path).
fn try_to_get_dual_task(
    ds: &crate::logical::DataSource,
    ctx: &DispatchContext<'_>,
) -> Option<Task> {
    use tidb_expr::expression::Expression;
    for cond in &ds.pushed_down_conds {
        let Expression::Constant(constant) = cond else {
            continue;
        };
        if constant.deferred_expr.is_some() || constant.param_marker.is_some() {
            continue;
        }
        let is_false = matches!(constant.value, tidb_datatype::Datum::Null)
            || constant.value.as_int() == Some(0);
        if is_false {
            return Some(empty_range_dual_task(ds, ctx));
        }
    }
    None
}

/// Go's empty-range branch returns an uncacheable `PhysicalTableDual` before
/// converting the selected access path into a scan.  Keep the same shape for
/// both constant-false predicates and ranger-proven empty ranges.
fn empty_range_dual_task(ds: &crate::logical::DataSource, ctx: &DispatchContext<'_>) -> Task {
    let mut base = crate::physical::BasePhysicalPlan::new(
        ctx.allocator,
        crate::logical::LogicalTableDual::TYPE,
        ds.base.base.query_block_offset(),
    );
    base.base.set_stats(ds.base.base.stats_info().cloned());
    base.base.set_schema(ds.base.base.schema().cloned());
    let dual = PhysicalPlan::TableDual(crate::physical::PhysicalTableDual { base, row_count: 0 });
    let mut root = crate::task::RootTask::default();
    root.set_plan(dual);
    Task::Root(root)
}

/// Go `findBestTask4LogicalDataSource` (`find_best_task.go:2027`), the
/// TABLE-PATH slice: the dual short-circuit, then one cop-task candidate
/// per TABLE access path (`convertToTableScan` without ranger — the scan is
/// the full range the enumerated path carries), finished through
/// [`crate::task::Task::convert_to_root_task`]'s table branch.
///
/// # Narrowings, each naming its Go symbol
///
/// * A non-empty required order answers the invalid task: the
///   `isMatchProp` handle-order admission of `convertToTableScan`
///   (`:2834`) is keep-order work over ranges this slice does not build.
/// * INDEX paths (`convertToIndexScan`), `PointGet`/`BatchPointGet`, and
///   index merge enumerate NO candidate here — fewer candidates than Go,
///   the same class of narrowing as the projection's cop branch. The
///   skyline prune (`skylinePruning`) has nothing to prune with one
///   candidate shape.
/// * `isolation read engines`, `IsForUpdateRead` filtering (`:2036`), and
///   the TiFlash arms narrow with the absent tiers.
/// Go `matchProperty`'s INT-HANDLE arm (`find_best_task.go:1082`): a table
/// path over an integer handle delivers the required order exactly when the
/// property is ONE sort item on the handle column (asc or desc; Go's
/// TiFlash-desc refusal narrows with the tier). Cluster tables, vector
/// properties, and the index-column prefix walk (`:1095`) are later slices,
/// named here.
///
/// The handle column is Go `ds.GetPKIsHandleCol()`: through
/// `getPKIsHandleColFromSchema` (`logical_datasource.go:578`) it is the pk
/// column when `PKIsHandle`, ELSE the schema's extra-handle column — so a
/// no-PK table's implicit `_tidb_rowid` walk satisfies
/// `ORDER BY _tidb_rowid` for free. Verified live (Go master fdfadb96b2):
/// Whether `condition` is an `eq`/`in` scalar function whose FIRST argument
/// is the column with `unique_id` and whose remaining arguments are
/// constants — the shape of a point range. Used by the table-path skyline
/// prune: only point-range predicates prove index dominance.
fn is_eq_or_in_on_column(condition: &tidb_expr::expression::Expression, unique_id: i64) -> bool {
    let tidb_expr::expression::Expression::ScalarFunction(function) = condition else {
        return false;
    };
    let name = function.func_name.lowercase();
    if name != "eq" && name != "in" {
        return false;
    }
    let Some(first) = function.args.first() else {
        return false;
    };
    let tidb_expr::expression::Expression::Column(column) = first else {
        return false;
    };
    if column.unique_id != unique_id {
        return false;
    }
    function.args[1..].iter().all(|argument| {
        matches!(
            argument,
            tidb_expr::expression::Expression::Constant(_)
        )
    })
}

/// `where a > 10 order by _tidb_rowid` on a no-PK table reads
/// `TableFullScan ... keep order:true`. [`DataSource::handle_is_int`] only
/// stays true while that handle column survives pruning, so the liveness
/// half of Go's schema scan is this port's flag reset.
pub(super) fn table_path_matches_order(ds: &crate::logical::DataSource, prop: &PhysicalProperty) -> bool {
    if ds.handle_is_int {
        let Some(pk_col) = ds.handle_cols.first() else {
            return false;
        };
        let [item] = prop.sort_items.as_slice() else {
            return false;
        };
        return item.col.unique_id == pk_col.unique_id;
    }
    let (all_same, _) = prop.all_same_order();
    if !all_same || prop.sort_items.is_empty() {
        return false;
    }
    let fixed = equality_fixed_ids(ds);
    let mut handle_offset = 0;
    for item in &prop.sort_items {
        let mut found = false;
        while let Some(column) = ds.common_handle_cols.get(handle_offset) {
            let length = ds.common_handle_lens.get(handle_offset).copied();
            handle_offset += 1;
            if length == Some(tidb_datatype::UNSPECIFIED_LENGTH)
                && column.unique_id == item.col.unique_id
            {
                found = true;
                break;
            }
            if fixed.contains(&column.unique_id) {
                continue;
            }
            return false;
        }
        if !found {
            return false;
        }
    }
    true
}

pub(super) fn index_path_matches_order(
    ds: &crate::logical::DataSource,
    index: &crate::plan_builder::catalog::SourceIndex,
    prop: &PhysicalProperty,
) -> bool {
    let (all_same, _) = prop.all_same_order();
    if prop.is_sort_item_empty() || !all_same {
        return false;
    }
    let fixed = equality_fixed_ids(ds);
    let mut index_offset = 0;
    for item in &prop.sort_items {
        let mut found = false;
        while let Some(index_column) = index.columns.get(index_offset) {
            index_offset += 1;
            let schema_column = ds.schema_column_for_index_column(index_column);
            if index_column.length < 0
                && schema_column.is_some_and(|column| column.unique_id == item.col.unique_id)
            {
                found = true;
                break;
            }
            if schema_column.is_some_and(|column| fixed.contains(&column.unique_id)) {
                continue;
            }
            return false;
        }
        if !found {
            return false;
        }
    }
    true
}

/// Go `matchPartialOrderProperty` (`find_best_task.go:1260`). A prefix index
/// can provide the requested TopN prefix only when all index-definition
/// columns line up with the ORDER BY columns, the final definition column is
/// a prefix column, and no earlier definition column is truncated.
pub(super) fn match_partial_order_property(
    ds: &crate::logical::DataSource,
    index: &crate::plan_builder::catalog::SourceIndex,
    partial: &crate::physical_property::PartialOrderInfo,
) -> Option<crate::physical_property::PartialOrderMatchResult> {
    if partial.sort_items.is_empty() || !partial.all_same_order().0 {
        return None;
    }
    let index_columns = &index.columns;
    if index_columns.is_empty() || index_columns.len() > partial.sort_items.len() {
        return None;
    }
    if index_columns.last()?.length == tidb_datatype::UNSPECIFIED_LENGTH {
        return None;
    }
    let mut prefix = None;
    for (position, index_column) in index_columns.iter().enumerate() {
        let column = ds.schema_column_for_index_column(index_column)?;
        let order = partial.sort_items.get(position)?;
        if column.unique_id != order.col.unique_id {
            return None;
        }
        // Go IndexInfo2FullCols normalizes a declared full-length prefix.
        let length = if column
            .ret_type
            .as_ref()
            .is_some_and(|field_type| field_type.flen() == index_column.length)
        {
            tidb_datatype::UNSPECIFIED_LENGTH
        } else {
            index_column.length
        };
        if length != tidb_datatype::UNSPECIFIED_LENGTH {
            if position + 1 != index_columns.len() {
                return None;
            }
            prefix = Some((column.clone(), length));
        }
    }
    let Some((prefix_col, prefix_len)) = prefix else {
        return None;
    };
    Some(crate::physical_property::PartialOrderMatchResult {
        matched: true,
        prefix_col: Some(prefix_col),
        prefix_len: usize::try_from(prefix_len).ok()?,
    })
}

fn equality_fixed_ids(ds: &crate::logical::DataSource) -> Vec<i64> {
    use tidb_expr::expression::Expression;
    let mut ids = Vec::new();
    for condition in &ds.pushed_down_conds {
        let Expression::ScalarFunction(function) = condition else {
            continue;
        };
        let id = match function.get_args() {
            [Expression::Column(column), Expression::Constant(_)]
            | [Expression::Constant(_), Expression::Column(column)]
                if function.func_name.lowercase() == "eq" =>
            {
                Some(column.unique_id)
            }
            _ => None,
        };
        if let Some(id) = id {
            ids.push(id);
        }
    }
    ids
}

// IN fixes a finite access prefix, but does not make an ordering column or
// a unique lookup single-valued. Keep that distinction from equality_fixed_ids.
fn index_join_fixed_ids(ds: &crate::logical::DataSource) -> Vec<i64> {
    use tidb_expr::expression::Expression;
    let mut ids = equality_fixed_ids(ds);
    for condition in &ds.pushed_down_conds {
        let Expression::ScalarFunction(function) = condition else {
            continue;
        };
        if function.func_name.lowercase() != "in" {
            continue;
        }
        if let [Expression::Column(column), values @ ..] = function.args.as_slice() {
            if !values.is_empty()
                && values
                    .iter()
                    .all(|value| matches!(value, Expression::Constant(_)))
            {
                ids.push(column.unique_id);
            }
        }
    }
    ids
}

/// Computes Go's `indexJoinProbeAccessRowsFloor` for one admitted access
/// prefix (`exhaust_physical_plans.go:824`).  IndexJoin's post-join row count
/// uses every equality key, while a probe path may only build ranges from a
/// leading subset.  In that case the scan still reads approximately one
/// table row per distinct value of the keys that *were* used by the path.
///
/// The source skips this correction for pseudo statistics, complete key
/// coverage, and a trailing range (the latter is not an equality-prefix
/// estimate).  Unknown or non-positive NDVs fail closed at the same boundary
/// rather than making a low-confidence cost estimate look precise.
fn index_join_probe_access_rows_floor(
    ds: &crate::logical::DataSource,
    access_columns: &[tidb_expr::column::Column],
    runtime: &crate::physical_property::IndexJoinRuntimeProp,
    enabled: bool,
    group_ndv_skew_ratio: f64,
) -> Option<f64> {
    if !enabled || runtime.inner_join_keys.is_empty() || ds.handle_is_int {
        return None;
    }
    let table_stats = ds.table_stats.as_ref()?;
    if table_stats.stats_version() == 0 {
        return None;
    }

    // `lastColIsRange`/`lastColManager` in Go identifies a non-equality tail.
    // The Rust runtime property carries the source's residual conditions, so
    // conservatively decline the floor whenever one of them is a range
    // comparison.  Equality residuals remain eligible.
    if runtime.other_conditions.iter().any(|condition| {
        let tidb_expr::expression::Expression::ScalarFunction(function) = condition else {
            return false;
        };
        let name = function.func_name.lowercase();
        name == "ge" || name == "gt" || name == "lt" || name == "le"
    }) {
        return None;
    }

    let inner_ids = runtime
        .inner_join_keys
        .iter()
        .map(|column| column.unique_id)
        .collect::<std::collections::BTreeSet<_>>();
    let fixed = equality_fixed_ids(ds)
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut used_columns = Vec::new();
    let mut used_runtime_keys = 0usize;
    for column in access_columns {
        if inner_ids.contains(&column.unique_id) {
            used_runtime_keys += 1;
            used_columns.push(column.clone());
        } else if fixed.contains(&column.unique_id) {
            used_columns.push(column.clone());
        } else {
            break;
        }
    }
    if used_runtime_keys == 0 || used_runtime_keys >= runtime.inner_join_keys.len() {
        return None;
    }

    let ids = used_columns
        .iter()
        .map(|column| column.unique_id)
        .collect::<Vec<_>>();
    let (ndv, _) = crate::cardinality::derive_stats::
        estimate_cols_ndv_with_matched_len_and_skew_ratio(
            &ids,
            table_stats,
            group_ndv_skew_ratio,
        );
    if !ndv.is_finite() || ndv <= 0.0 {
        return None;
    }
    let floor = table_stats.row_count() / ndv;
    floor.is_finite().then_some(floor.max(0.0))
}

/// Computes Go's `indexJoinPathCountAfterAccess4Compare` for one secondary
/// index candidate. IndexJoin's runtime equality is invisible while ordinary
/// access statistics are derived, so a stable single-column NDV lets the
/// skyline comparison divide that estimate by the per-probe key cardinality.
/// Multiple runtime keys, prefix columns, pseudo statistics, and invalid NDVs
/// fail closed just as Go's helper does.
fn index_join_skyline_count_for_index(
    ds: &crate::logical::DataSource,
    runtime: &crate::physical_property::IndexJoinRuntimeProp,
    index_prefix: &[(tidb_expr::column::Column, i64)],
    count_after_access: Option<f64>,
) -> Option<f64> {
    let count_after_access = count_after_access?;
    if !count_after_access.is_finite() || count_after_access <= 0.0 {
        return None;
    }
    let table_stats = ds
        .table_stats
        .as_ref()
        .or_else(|| ds.base.base.stats_info())?;
    if table_stats.stats_version() == 0 {
        return None;
    }
    let runtime_ids = runtime
        .inner_join_keys
        .iter()
        .map(|column| column.unique_id)
        .collect::<std::collections::BTreeSet<_>>();
    if runtime_ids.is_empty() {
        return None;
    }
    let fixed = equality_fixed_ids(ds)
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut runtime_column = None;
    for (column, length) in index_prefix {
        if runtime_ids.contains(&column.unique_id) {
            if *length != tidb_datatype::UNSPECIFIED_LENGTH || runtime_column.is_some() {
                return None;
            }
            runtime_column = Some(column.unique_id);
        } else if fixed.contains(&column.unique_id) {
            continue;
        } else {
            break;
        }
    }
    let runtime_column = runtime_column?;
    let ndv = table_stats.col_ndv(runtime_column);
    if !ndv.is_finite() || ndv <= 0.0 {
        return None;
    }
    let adjusted = count_after_access / ndv;
    adjusted.is_finite().then_some(adjusted)
}

/// Applies Go's `Fix45132` ratio rule to two IndexJoin inner candidates.
/// `Some(true)` means the current candidate wins, `Some(false)` means the
/// existing best wins, and `None` delegates to ordinary task costing.
fn index_join_skyline_prefers_current(
    current: Option<f64>,
    best: Option<f64>,
    expected_cnt: f64,
    threshold: f64,
) -> Option<bool> {
    let (current, best) = (current?, best?);
    if expected_cnt != f64::MAX
        || threshold <= 0.0
        || !current.is_finite()
        || !best.is_finite()
        || current <= 100.0
        || best <= 100.0
    {
        return None;
    }
    if current / best > threshold {
        return Some(false);
    }
    if best / current > threshold {
        return Some(true);
    }
    None
}

/// The path admission half of Go's
/// `buildDataSource2{Table,Index}ScanByIndexJoinProp`: a table-range
/// candidate must probe the clustered handle; an index-range candidate must
/// cover a leading run made of runtime join keys and equality-fixed columns.
fn path_matches_index_join_runtime(
    ds: &crate::logical::DataSource,
    path: &crate::access_path::PossiblePath,
    runtime: &crate::physical_property::IndexJoinRuntimeProp,
) -> bool {
    let inner_ids: Vec<i64> = runtime
        .inner_join_keys
        .iter()
        .map(|column| column.unique_id)
        .collect();
    match path {
        crate::access_path::PossiblePath::TiFlashTable => false,
        crate::access_path::PossiblePath::Table { .. } => {
            if !runtime.table_range_scan {
                return false;
            }
            if ds.handle_is_int {
                return ds
                    .handle_cols
                    .first()
                    .is_some_and(|column| inner_ids.contains(&column.unique_id));
            }
            // Go's common-handle table-range builder follows the PRIMARY KEY
            // from its first column. Runtime join keys and equality-fixed
            // columns may jointly cover that leading run, but a later handle
            // column cannot be probed across an unfixed earlier one.
            let fixed = index_join_fixed_ids(ds);
            let mut matched_runtime_key = false;
            for column in &ds.common_handle_cols {
                if inner_ids.contains(&column.unique_id) {
                    matched_runtime_key = true;
                } else if !fixed.contains(&column.unique_id) {
                    break;
                }
            }
            matched_runtime_key
        }
        crate::access_path::PossiblePath::Index { index } => {
            if runtime.table_range_scan {
                return false;
            }
            let Some(index) = ds.indexes.get(*index) else {
                return false;
            };
            let fixed = index_join_fixed_ids(ds);
            let mut matched_runtime_key = false;
            for index_column in &index.columns {
                // `IndexColumn.Offset` indexes the TABLE's column list, not
                // the DataSource's pruned schema; resolve by name as
                // `schema_column_for_index_column` (Go's `ds.Columns`
                // alignment) does.
                let Some(column) = ds.schema_column_for_index_column(index_column) else {
                    // Go IndexInfo2Cols truncates IdxCols at the first
                    // pruned column, retaining any usable leading join keys.
                    break;
                };
                if inner_ids.contains(&column.unique_id) {
                    matched_runtime_key = true;
                } else if !fixed.contains(&column.unique_id) {
                    break;
                }
            }
            matched_runtime_key
        }
    }
}


/// Go `indexJoinPathGetRangeInfoAndMaxOneRow` (`index_join_path.go:588`): a
/// UNIQUE access path whose complete key is covered by equality access
/// conditions -- the runtime join keys plus any equality-fixed columns --
/// reads at most one row per outer row. The inner scan's row count is capped
/// at 1.0 for such a path, which is what lets plain `IndexJoin` (whose hash
/// table is built over `probeRowsOne * buildRows`) beat `IndexHashJoin` when
/// the per-probe average exceeds one row.
fn index_join_path_is_max_one_row(
    ds: &crate::logical::DataSource,
    path: &crate::access_path::PossiblePath,
    runtime: &crate::physical_property::IndexJoinRuntimeProp,
) -> bool {
    let (access_columns, unique) = match path {
        crate::access_path::PossiblePath::TiFlashTable => return false,
        crate::access_path::PossiblePath::Table { .. } if ds.handle_is_int => (
            ds.handle_cols.iter().take(1).cloned().collect::<Vec<_>>(),
            true,
        ),
        crate::access_path::PossiblePath::Table { .. } => (
            ds.common_handle_cols.clone(),
            // The clustered primary key is unique by definition.
            true,
        ),
        crate::access_path::PossiblePath::Index { index } => {
            let Some(source_index) = ds.indexes.get(*index) else {
                return false;
            };
            let columns = source_index
                .columns
                .iter()
                .filter_map(|column| ds.schema_column_for_index_column(column).cloned())
                .collect::<Vec<_>>();
            (columns, source_index.unique)
        }
    };
    if !unique || access_columns.is_empty() {
        return false;
    }
    let fixed = equality_fixed_ids(ds);
    let mut matched_runtime_key = false;
    for column in &access_columns {
        if runtime
            .inner_join_keys
            .iter()
            .any(|key| key.unique_id == column.unique_id)
        {
            matched_runtime_key = true;
        } else if !fixed.contains(&column.unique_id) {
            return false;
        }
    }
    matched_runtime_key
}

/// Go `completeIndexJoinFeedBackInfo`: return the selected access's complete
/// prefix lengths and map every logical inner key to the chosen key column.
/// A key left at `-1` becomes a residual equality when the parent completes
/// `PhysicalIndexJoin`.
fn index_join_feedback(
    ds: &crate::logical::DataSource,
    path: &crate::access_path::PossiblePath,
    runtime: &crate::physical_property::IndexJoinRuntimeProp,
    ctx: &DispatchContext<'_>,
) -> Option<crate::task::IndexJoinInfo> {
    let (access_columns, idx_col_lens) = match path {
        crate::access_path::PossiblePath::TiFlashTable => (Vec::new(), Vec::new()),
        crate::access_path::PossiblePath::Table { .. } if ds.handle_is_int => (
            ds.handle_cols.iter().take(1).collect::<Vec<_>>(),
            Vec::new(),
        ),
        crate::access_path::PossiblePath::Table { .. } => (
            ds.common_handle_cols.iter().collect::<Vec<_>>(),
            ds.common_handle_lens.clone(),
        ),
        crate::access_path::PossiblePath::Index { index } => {
            let source_index = ds.indexes.get(*index);
            let columns = source_index
                .into_iter()
                .flat_map(|index| &index.columns)
                .map_while(|column| ds.schema_column_for_index_column(column))
                .collect::<Vec<_>>();
            let lengths = source_index
                .map(|index| {
                    index
                        .columns
                        .iter()
                        .take(columns.len())
                        .map(|column| column.length)
                        .collect()
                })
                .unwrap_or_default();
            (columns, lengths)
        }
    };
    let fixed = index_join_fixed_ids(ds);
    let mut key_off2_idx_off = vec![-1; runtime.inner_join_keys.len()];
    let mut matched_runtime_key = false;
    let mut first_unmatched_index_offset = access_columns.len();
    for (idx_off, column) in access_columns.iter().copied().enumerate() {
        if let Some(key_off) = runtime
            .inner_join_keys
            .iter()
            .position(|key| key.unique_id == column.unique_id)
        {
            key_off2_idx_off[key_off] = i64::try_from(idx_off).unwrap_or(i64::MAX);
            matched_runtime_key = true;
        } else if !fixed.contains(&column.unique_id) {
            first_unmatched_index_offset = idx_off;
            break;
        }
    }
    let (compare_filters, last_col_access) = matched_runtime_key
        .then(|| access_columns.get(first_unmatched_index_offset).copied())
        .flatten()
        .and_then(|target_col| {
            index_join_compare_filters(
                ds,
                runtime,
                target_col,
                first_unmatched_index_offset,
                &idx_col_lens,
            )
        })
        .map_or((None, Vec::new()), |(filters, access)| {
            (Some(filters), access)
        });
    // RangeInfo lists chosen access predicates, not every join residual.
    // Equality-fixed non-join prefix columns precede the final range column.
    let mut access_conditions = Vec::new();
    for (offset, column) in access_columns.iter().enumerate() {
        if offset > first_unmatched_index_offset {
            break;
        }
        if key_off2_idx_off.iter().any(|index| *index == offset as i64) {
            continue;
        }
        let is_prefix = offset < first_unmatched_index_offset;
        if !is_prefix && compare_filters.is_some() {
            break;
        }
        let checker = crate::ranger::checker::ConditionChecker {
            checker_col: Some(column),
            length: idx_col_lens
                .get(offset)
                .copied()
                .unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH),
            opt_prefix_index_single_scan: false,
        };
        for condition in &ds.pushed_down_conds {
            let prefix_equality = matches!(condition, tidb_expr::expression::Expression::ScalarFunction(function) if matches!(function.func_name.lowercase(), "eq" | "in"));
            if (!is_prefix || prefix_equality)
                && !tidb_expr::simple_expr::extract_columns(condition).is_empty()
                && checker.check(condition).0
            {
                access_conditions.push(condition.clone());
            }
        }
    }
    // Go's template ranges contain each static EQ/IN combination, with
    // placeholders where the per-outer-row join keys will be substituted.
    // Build over only static prefix columns; the ordinary scan ranges cannot
    // describe a static column separated from the front by a runtime key.
    let static_offsets = (0..first_unmatched_index_offset)
        .filter(|offset| !key_off2_idx_off.contains(&(*offset as i64)))
        .collect::<Vec<_>>();
    let static_columns = static_offsets
        .iter()
        .map(|offset| access_columns[*offset].clone())
        .collect::<Vec<_>>();
    let static_lengths = static_offsets
        .iter()
        .map(|offset| {
            idx_col_lens
                .get(*offset)
                .copied()
                .unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH)
        })
        .collect::<Vec<_>>();
    let prefix_conditions = access_conditions
        .iter()
        .filter(|condition| {
            tidb_expr::simple_expr::extract_columns(condition)
                .iter()
                .all(|column| {
                    static_columns
                        .iter()
                        .any(|fixed| fixed.unique_id == column.unique_id)
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    let (ranges, range_rebuild) = if static_columns.is_empty() {
        (vec![crate::ranger::types::Range::default()], None)
    } else {
        let ranges = ctx
            .detach_index_range(&prefix_conditions, &static_columns, &static_lengths)
            .ok()?
            .ranges;
        let rebuild = crate::physical_plan_cache::PointRangeRebuild::IndexJoin {
            access: crate::physical_plan_cache::IndexRangeRebuild::new(
                prefix_conditions,
                static_columns,
                static_lengths,
            ),
            offsets: static_offsets.clone(),
            width: first_unmatched_index_offset,
        };
        (ranges, Some(rebuild))
    };
    let ranges = crate::physical_plan_cache::index_join_template_ranges(
        ranges,
        &static_offsets,
        first_unmatched_index_offset,
    )?;
    access_conditions.extend(last_col_access);
    Some(crate::task::IndexJoinInfo {
        // Bare outer-key spelling only when the probe is the integer handle
        // (`constructDS2TableScanTask`'s int-PK branch); rowid tables without
        // a matching handle and index probes render the `eq(inner, outer)`
        // pairs of `indexJoinPathRangeInfo`.
        range_bare: matches!(path, crate::access_path::PossiblePath::Table { .. })
            && ds.handle_is_int,
        table_id: ds.physical_table_id,
        index_id: match path {
            crate::access_path::PossiblePath::Index { index } => {
                ds.indexes.get(*index).map(|index| index.id)
            }
            crate::access_path::PossiblePath::Table { .. } => None,
            crate::access_path::PossiblePath::TiFlashTable => None,
        },
        ranges,
        idx_col_lens,
        key_off2_idx_off,
        access_conditions,
        range_rebuild,
        compare_filters,
    })
}

/// Go `indexJoinPathBuildColManager`: comparisons against the first index
/// column after the equality prefix become one retained per-outer-row range
/// manager. The expression on the other side must depend on the outer schema
/// and must not reference the inner data source.
fn index_join_compare_filters(
    ds: &crate::logical::DataSource,
    runtime: &crate::physical_property::IndexJoinRuntimeProp,
    target_col: &tidb_expr::column::Column,
    target_index_offset: usize,
    idx_col_lens: &[i64],
) -> Option<(
    crate::physical::IndexJoinCompareFilters,
    Vec<tidb_expr::expression::Expression>,
)> {
    let inner_schema = ds.base.base.schema()?;
    let mut ops = Vec::new();
    let mut args = Vec::new();
    let mut access_conditions = Vec::new();
    for condition in &runtime.other_conditions {
        let tidb_expr::expression::Expression::ScalarFunction(function) = condition else {
            continue;
        };
        let [left, right] = function.args.as_slice() else {
            continue;
        };
        let name = function.func_name.lowercase();
        let (op, argument) = if left
            .as_column()
            .is_some_and(|column| column.unique_id == target_col.unique_id)
        {
            let op = match name {
                "ge" => crate::physical::IndexJoinCompareOp::Ge,
                "gt" => crate::physical::IndexJoinCompareOp::Gt,
                "lt" => crate::physical::IndexJoinCompareOp::Lt,
                "le" => crate::physical::IndexJoinCompareOp::Le,
                _ => continue,
            };
            (op, right)
        } else if right
            .as_column()
            .is_some_and(|column| column.unique_id == target_col.unique_id)
        {
            let op = match name {
                "ge" => crate::physical::IndexJoinCompareOp::Le,
                "gt" => crate::physical::IndexJoinCompareOp::Lt,
                "lt" => crate::physical::IndexJoinCompareOp::Gt,
                "le" => crate::physical::IndexJoinCompareOp::Ge,
                _ => continue,
            };
            (op, left)
        } else {
            continue;
        };
        let affected = tidb_expr::simple_expr::extract_columns(argument);
        if affected.is_empty() || affected.iter().any(|column| inner_schema.contains(column)) {
            continue;
        }
        ops.push(op);
        args.push(argument.clone());
        access_conditions.push(condition.clone());
    }
    (!ops.is_empty()).then(|| {
        (
            crate::physical::IndexJoinCompareFilters {
                target_col: target_col.clone(),
                target_index_offset,
                col_length: idx_col_lens
                    .get(target_index_offset)
                    .copied()
                    .unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH),
                ops,
                args,
            },
            access_conditions,
        )
    })
}

fn find_best_task_4_logical_data_source(
    ds: &crate::logical::DataSource,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    // Go `findBestTask4LogicalDataSource` handles `CanAddEnforcer` inside
    // the DataSource override, before entering the path loop.  First price
    // a path that satisfies the requested order directly.  Then clear the
    // order, price the best unordered path, and enforce the original
    // property above it.  This branch is what makes
    // `getEnforcedStreamAggs`' sorted child property executable when no
    // access path naturally provides the group order.
    //
    // IndexJoinProp takes Go's earlier, dedicated runtime-range return and
    // therefore never enters the enforcer branch.
    if prop.can_add_enforcer && prop.index_join_prop.is_none() {
        let mut direct_prop = prop.clone();
        direct_prop.can_add_enforcer = false;
        let direct = find_best_task_4_logical_data_source_without_enforcer(ds, &direct_prop, ctx)?;

        let mut unordered_prop = prop.clone();
        unordered_prop.can_add_enforcer = false;
        unordered_prop.sort_items.clear();
        let unordered =
            find_best_task_4_logical_data_source_without_enforcer(ds, &unordered_prop, ctx)?;
        let enforced = enforce_property_in(prop, unordered, ctx.allocator, ctx.expression_evaluator)?;
        if compare_task_cost(ctx.coster, &direct, &enforced)? {
            return Ok(direct);
        }
        return Ok(enforced);
    }

    find_best_task_4_logical_data_source_without_enforcer(ds, prop, ctx)
}

fn filter_column_correlation(
    stats: &crate::stats_info::StatsInfo,
    filters: &[tidb_expr::expression::Expression],
    threshold: f64,
) -> (usize, Option<tidb_expr::column::Column>, f64) {
    let mut columns = std::collections::BTreeMap::new();
    for filter in filters {
        for column in tidb_expr::simple_expr::extract_columns(filter) {
            columns.entry(column.unique_id).or_insert(column);
        }
    }

    let column_count = columns.len();
    let mut max_correlation = 0.0_f64;
    let mut most_correlated_column = None;
    let mut found_histogram = false;
    if let Some(hist_coll) = stats.hist_coll() {
        for unique_id in columns.keys() {
            let Some(histogram) = hist_coll.histogram(*unique_id) else {
                continue;
            };
            let correlation = histogram.histogram.correlation;
            if !found_histogram || max_correlation.abs() < correlation.abs() {
                max_correlation = correlation;
                most_correlated_column = columns.get(unique_id).cloned();
                found_histogram = true;
            }
        }
    }
    let correlation_column = (column_count == 1 && max_correlation.abs() >= threshold)
        .then_some(most_correlated_column)
        .flatten();
    (column_count, correlation_column, max_correlation)
}

/// Applies Go's `AdjustRowCountForIndexScanByLimit` uniform scan estimate and
/// residual-filter ordering ratio. The caller decides whether this physical
/// index path is eligible for expected-count adjustment.
pub(super) fn adjust_index_scan_count_by_expected(
    count_after_access: f64,
    expected_count: f64,
    datasource_rows: f64,
    has_residual_filters: bool,
    ordering_ratio: f64,
) -> f64 {
    if count_after_access <= 0.0 || expected_count >= datasource_rows {
        return count_after_access;
    }
    let selectivity = datasource_rows / count_after_access;
    let mut row_count = count_after_access.min(expected_count / selectivity);
    if count_after_access > row_count && ordering_ratio > 0.0 && has_residual_filters {
        row_count += (count_after_access - row_count) * ordering_ratio;
    }
    row_count
}

pub(super) fn ignore_index_scan_expected_count(
    ordering_index_selectivity_threshold: f64,
    access_path_min_selectivity: f64,
    has_residual_filters: bool,
) -> bool {
    ordering_index_selectivity_threshold != 0.0
        && access_path_min_selectivity <= ordering_index_selectivity_threshold
        && has_residual_filters
}

#[cfg(test)]
mod index_scan_limit_adjustment_tests {
    use super::{adjust_index_scan_count_by_expected, ignore_index_scan_expected_count};

    #[test]
    fn residual_filter_ratio_applies_when_go_adjusts_an_unordered_index_scan() {
        // Go invokes AdjustRowCountForIndexScanByLimit for sort-empty
        // properties as well as matched ordering properties. Its final ratio
        // adjustment depends on residual filters, not on IsMatchProp.
        let with_residual = adjust_index_scan_count_by_expected(100.0, 10.0, 100.0, true, 0.2);
        let without_residual = adjust_index_scan_count_by_expected(100.0, 10.0, 100.0, false, 0.2);

        assert!((with_residual - 28.0).abs() < 1e-12);
        assert_eq!(without_residual, 10.0);
        assert_eq!(
            adjust_index_scan_count_by_expected(100.0, 100.0, 100.0, true, 0.2),
            100.0
        );
    }

    #[test]
    fn selective_access_path_disables_limit_adjustment_only_with_residual_filters() {
        let adjusted = |threshold, path_selectivity, has_residual_filters| {
            if ignore_index_scan_expected_count(
                threshold,
                path_selectivity,
                has_residual_filters,
            ) {
                100.0
            } else {
                adjust_index_scan_count_by_expected(
                    100.0,
                    10.0,
                    100.0,
                    has_residual_filters,
                    0.2,
                )
            }
        };

        // Go leaves the access estimate untouched if any path is selective
        // and this index scan still evaluates filters after range access.
        assert_eq!(adjusted(0.1, 0.05, true), 100.0);
        // A disabled threshold, a nonselective path, or no residual filter
        // keeps the ordinary expected-count cost adjustment.
        assert_eq!(adjusted(0.0, 0.05, true), 28.0);
        assert_eq!(adjusted(0.1, 0.2, true), 28.0);
        assert_eq!(adjusted(0.1, 0.05, false), 10.0);
    }
}

#[cfg(test)]
mod correlation_tests {
    use super::filter_column_correlation;
    use crate::cardinality::row_count_estimator::ColumnStats;
    use crate::stats_info::{HistColl, StatsInfo};
    use std::sync::Arc;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::expression::Expression;
    use tidb_stats::histogram::Histogram;

    #[test]
    fn filter_correlation_uses_unique_columns_and_largest_loaded_correlation() {
        let column_a = Column::new(7, FieldType::new(FieldTypeCode::LongLong));
        let column_b = Column::new(8, FieldType::new(FieldTypeCode::LongLong));
        let histogram = |id, correlation| {
            Arc::new(ColumnStats {
                histogram: Histogram {
                    id,
                    correlation,
                    ..Histogram::default()
                },
                topn: None,
                cms: None,
                stats_ver: 2,
                unsigned: false,
            })
        };
        let stats = StatsInfo::new(100.0, []).with_hist_coll(
            HistColl::new(false, 100, [])
                .with_histograms([(7, histogram(7, 0.4)), (8, histogram(8, -0.9))]),
        );

        let (both_count, both_column, largest) = filter_column_correlation(
            &stats,
            &[
                Expression::Column(column_a.clone()),
                Expression::Column(column_b.clone()),
            ],
            0.9,
        );
        assert_eq!(both_count, 2);
        assert!(both_column.is_none());
        assert_eq!(largest, -0.9);

        let (one_count, one_column, one_correlation) = filter_column_correlation(
            &stats,
            &[
                Expression::Column(column_a.clone()),
                Expression::Column(column_a),
            ],
            0.9,
        );
        assert_eq!(one_count, 1);
        assert!(one_column.is_none());
        assert_eq!(one_correlation, 0.4);

        let (one_count, one_column, one_correlation) =
            filter_column_correlation(&stats, &[Expression::Column(column_b)], 0.9);
        assert_eq!(one_count, 1);
        assert_eq!(one_column.map(|column| column.unique_id), Some(8));
        assert_eq!(one_correlation, -0.9);
    }
}

#[cfg(test)]
mod cross_estimate_dispatch_tests {
    use super::{
        DispatchContext, PhysicalProperty, Task, TaskType,
        find_best_task_4_logical_data_source_without_enforcer,
    };
    use crate::access_path::PossiblePath;
    use crate::cardinality::row_count_estimator::ColumnStats;
    use crate::find_best_task::coster::Ver2Coster;
    use crate::logical::data_source::DataSourceColumn;
    use crate::logical::{BaseLogicalPlan, DataSource};
    use crate::plan_base::PlanIdAllocator;
    use crate::stats_info::{HistColl, StatsInfo};
    use std::sync::Arc;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::expression::Expression;
    use tidb_expr::scalar_function::ScalarFunction;
    use tidb_expr::schema::Schema;
    use tidb_stats::histogram::Histogram;

    #[test]
    fn table_limit_costing_uses_cross_estimation_for_one_correlated_filter() {
        let allocator = PlanIdAllocator::new();
        let coster = Ver2Coster::default();
        let mut context = DispatchContext::new(&allocator, &coster, 1.0);
        let mut handle_type = FieldType::new(FieldTypeCode::LongLong);
        handle_type.set_flags(handle_type.flags() | tidb_datatype::FieldTypeFlags::PRI_KEY);
        let handle = Column::new(1, handle_type);
        let filtered = Column::new(2, FieldType::new(FieldTypeCode::LongLong));
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("eq"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![
                Expression::Column(filtered.clone()),
                Expression::Constant(Constant::new(
                    Datum::Int(50),
                    FieldType::new(FieldTypeCode::LongLong),
                )),
            ],
        ));
        let histogram = Arc::new(ColumnStats {
            histogram: Histogram {
                id: 2,
                ndv: 100,
                correlation: 0.95,
                buckets: vec![tidb_stats::Bucket {
                    count: 100,
                    repeat: 1,
                    ndv: 100,
                    lower_bound: Datum::Int(1),
                    upper_bound: Datum::Int(100),
                }],
                ..Histogram::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        });
        let hist_coll = HistColl::new(false, 100, []).with_histograms([(2, histogram)]);
        let stats = StatsInfo::new(100.0, []).with_hist_coll(hist_coll.clone());
        let expected_cross_count =
            crate::cardinality::cross_estimation::estimate_table_cross_row_count(
                &hist_coll,
                &hist_coll,
                std::slice::from_ref(&filter),
                &filtered,
                0.95,
                1.0,
                100.0,
                false,
                0,
                &crate::ranger::points::evaluate_static,
                Default::default(),
            );
        let mut base = BaseLogicalPlan::new(&allocator, DataSource::TYPE, 0);
        base.base.set_stats(Some(stats.clone()));
        base.base
            .set_schema(Some(Schema::new(vec![handle.clone(), filtered.clone()])));
        let source = DataSource {
            base,
            physical_table_id: 7,
            columns: vec![
                DataSourceColumn {
                    id: 1,
                    name: "pk".to_owned(),
                    is_primary_key: true,
                    is_not_null: true,
                },
                DataSourceColumn {
                    id: 2,
                    name: "a".to_owned(),
                    ..DataSourceColumn::default()
                },
            ],
            table_columns: vec![handle, filtered],
            handle_cols: vec![Column::new(1, FieldType::new(FieldTypeCode::LongLong))],
            handle_is_int: true,
            pk_is_handle: true,
            pushed_down_conds: vec![filter],
            enumerated_paths: vec![PossiblePath::Table {
                is_int_handle: true,
                primary_index: None,
            }],
            table_stats: Some(stats),
            ..DataSource::default()
        };
        let property = PhysicalProperty {
            task_tp: TaskType::CopSingleRead,
            expected_cnt: 1.0,
            ..PhysicalProperty::default()
        };

        let task =
            find_best_task_4_logical_data_source_without_enforcer(&source, &property, &mut context)
                .expect("the table scan candidate is built");
        let Task::Cop(cop) = task else {
            panic!("the single-read table path remains a cop task");
        };
        let mut node = cop.table_plan.as_deref().expect("table plan");
        while !matches!(node, crate::physical::PhysicalPlan::TableScan(_)) {
            node = node.children().first().expect("table scan below selection");
        }
        let crate::physical::PhysicalPlan::TableScan(scan) = node else {
            unreachable!();
        };
        let estimated_rows = scan.base.base.stats_info().expect("scan stats").row_count();
        assert!(
            estimated_rows > 1.0 && estimated_rows < 100.0,
            "cross-estimation should price a prefix of the table, got {estimated_rows}"
        );
        assert!(expected_cross_count.1);
        assert!(
            (estimated_rows - expected_cross_count.0).abs() < 1e-9,
            "dispatcher must apply Go's cross estimate: expected {}, got {estimated_rows}",
            expected_cross_count.0
        );
    }
}

fn find_best_task_4_logical_data_source_without_enforcer(
    ds: &crate::logical::DataSource,
    prop: &PhysicalProperty,
    ctx: &mut DispatchContext<'_>,
) -> Result<Task, PlanError> {
    // Go's DataSource findBestTask serves both COP property kinds. The
    // single-read property admits table and covering-index scans; the
    // multi-read property admits only a non-covering index path whose COP
    // task still owns both the index and table halves. Aggregation needs the
    // latter so `attach2Task4PhysicalHashAgg` can put its partial stage on
    // the lookup's table half before converting the task to root.
    let cop_answer = match prop.task_tp {
        TaskType::Root => false,
        TaskType::CopSingleRead | TaskType::CopMultiRead => true,
        _ => return Ok(Task::invalid_task()),
    };
    let cop_multi_read = prop.task_tp == TaskType::CopMultiRead;
    let table_pseudo = ds
        .table_stats
        .as_ref()
        .or_else(|| ds.base.base.stats_info())
        .and_then(crate::stats_info::StatsInfo::hist_coll)
        .is_none_or(crate::stats_info::HistColl::pseudo);
    if let Some(dual) = try_to_get_dual_task(ds, ctx) {
        return Ok(dual);
    }
    if let Some(sample_info) = &ds.sample_info {
        if prop.task_tp != TaskType::Root || !prop.is_sort_item_empty() {
            return Ok(Task::invalid_task());
        }
        if !ds
            .enumerated_paths
            .iter()
            .any(|path| matches!(path, crate::access_path::PossiblePath::Table { .. }))
        {
            return Ok(Task::invalid_task());
        }
        let mut base = crate::physical::BasePhysicalPlan::new(
            ctx.allocator,
            "TableSample",
            ds.base.base.query_block_offset(),
        );
        base.base.set_schema(ds.base.base.schema().cloned());
        base.base
            .set_stats(Some(crate::stats_info::StatsInfo::new(1.0, [])));
        let mut root = crate::task::RootTask::default();
        root.set_plan(PhysicalPlan::TableSample(
            crate::physical::PhysicalTableSample {
                base,
                table_sample_info: sample_info.clone(),
                physical_table_id: ds.physical_table_id,
                desc: false,
            },
        ));
        return Ok(Task::Root(root));
    }
    // Per-path admission, the shape of Go's candidate loop: an empty
    // property admits every path unordered; a required order admits the
    // paths that MATCH it — the int-handle arm for the table path
    // (`matchProperty:1082`), the basic prefix arm for an index
    // (`matchProperty:1095`) — and the admitted scan carries
    // `KeepOrder`/`Desc` (`convertToTableScan:2834`).
    let mut ordinary_paths = ds.derived_access_paths.as_ref().map_or_else(
        || ds.enumerated_paths.iter().collect::<Vec<_>>(),
        |paths| {
            paths
                .paths
                .iter()
                .filter_map(|path| match path {
                    crate::access_path::DerivedAccessPath::Ordinary(path) => Some(path),
                    _ => None,
                })
                .collect()
        },
    );
    let preparation = super::candidate_preparation::prepare_access_paths(
        ds, &ordinary_paths, prop, ctx, table_pseudo,
    );
    let merge_candidates = preparation.merges;
    let preparation = preparation.ordinary;
    let prepared = preparation.is_some();
    let mut prepared_missing_stats = false;
    let mut heuristic_selected = false;
    if let Some(preparation) = preparation {
        ordinary_paths = preparation.paths;
        prepared_missing_stats = preparation.idx_missing_stats;
        heuristic_selected = preparation.heuristic_selected;
    }
    let access_path_min_selectivity = ds
        .derived_access_paths
        .as_ref()
        .map_or(ds.access_path_min_selectivity, |paths| {
            paths.min_selectivity
        });
    let ordered = !prop.is_sort_item_empty();
    let partial_order = prop.partial_order_info.as_ref();
    let desc = if ordered {
        prop.sort_items[0].desc
    } else {
        partial_order.map_or(false, |info| info.all_same_order().1)
    };
    let mut best = Task::invalid_task();
    let mut best_index_join_skyline_count = None;
    let mut best_preferred_range: Option<Task> = None;
    let mut best_is_preferred_range = false;
    let mut best_is_full_range = true;
    let mut ordinary_candidates = Vec::new();
    'paths: for path in &ordinary_paths {
        if (ds.prefer_store_type & crate::logical::data_source::PREFER_TIFLASH != 0
            && !matches!(path, crate::access_path::PossiblePath::TiFlashTable))
            || (ds.prefer_store_type & crate::logical::data_source::PREFER_TIKV != 0
                && matches!(path, crate::access_path::PossiblePath::TiFlashTable))
        {
            continue;
        }
        if let Some(runtime) = &prop.index_join_prop {
            if !path_matches_index_join_runtime(ds, path, runtime) {
                continue;
            }
        }
        let mut index_join_skyline_count = None;
        let mut cur_preferred_range = false;
        let mut cur_is_full_range = true;
        let mut candidate_metrics = None;
        let mut heuristic = None;
        let cop = match path {
            crate::access_path::PossiblePath::Table { primary_index, .. } => {
                if partial_order.is_some() {
                    // Go's skyline pruning deliberately omits table paths for
                    // partial-order TopN; a full table scan cannot provide the
                    // prefix order required by the executor.
                    continue 'paths;
                }
                if (!ordered && ds.force_keep_order_table_path)
                    || (ordered && ds.force_no_keep_order_table_path)
                {
                    continue 'paths;
                }
                if cop_multi_read {
                    continue 'paths;
                }
                let keep_order = ordered;
                if keep_order && !table_path_matches_order(ds, prop) {
                    continue 'paths;
                }
                let fallback_table_path;
                let table_path = if let Some(path) = ds
                    .derived_access_paths
                    .as_ref()
                    .and_then(|paths| paths.table_path.as_ref())
                    .filter(|_| prop.index_join_prop.is_none())
                {
                    path
                } else {
                    fallback_table_path = crate::access_path::ordinary::fill_table_path(
                        ds,
                        *primary_index,
                        &ctx.access_path_derivation_context(),
                    )?;
                    &fallback_table_path
                };
                let handle_column = table_path.handle_column.as_ref();
                let handle_type = &table_path.handle_type;
                let common_handle = primary_index.and_then(|index| ds.indexes.get(index));
                let (common_columns, common_lengths): (Vec<_>, Vec<_>) =
                    table_path.common_columns.iter().cloned().unzip();
                let ranges = table_path.detached.ranges.clone();
                if ranges.is_empty() {
                    return Ok(empty_range_dual_task(ds, ctx));
                }
                let mut base = crate::physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    "TableScan",
                    ds.base.base.query_block_offset(),
                );
                base.base.set_schema(ds.base.base.schema().cloned());
                cur_is_full_range = crate::ranger::types::has_full_range(&ranges, false);
                let table_access_conds = table_path.detached.access_conds.clone();
                let mut table_filters = table_path.detached.remained_conds.clone();
                // Go `constructDS2TableScanTask` computes the residual
                // selectivity from `chosenRemained` BEFORE the inner-only
                // access conditions are re-attached to the Selection.
                let mut residual_table_filters = table_filters.clone();
                if prop.index_join_prop.is_some() {
                    // Go `constructDS2TableScanTask` re-attaches every
                    // inner-only access condition as an explicit probe-side
                    // Selection (`exhaust_physical_plans.go:913-917`); the
                    // runtime ranges come from the join keys, so a static
                    // predicate such as `o_w_id = 1` is a residual filter.
                    if let Some(schema) = ds.base.base.schema() {
                        for condition in &table_access_conds {
                            if tidb_expr::expr_util::normal_form::expr_from_schema(
                                condition, schema,
                            ) && !table_filters
                                .iter()
                                .any(|existing| existing.equal(condition))
                            {
                                table_filters.push(condition.clone());
                            }
                        }
                    }
                }
                if let Some(runtime) = &prop.index_join_prop {
                    for condition in &table_access_conds {
                        let columns = tidb_expr::simple_expr::extract_columns(condition);
                        if columns.iter().any(|column| {
                            runtime
                                .inner_join_keys
                                .iter()
                                .any(|key| key.unique_id == column.unique_id)
                        }) && !residual_table_filters
                            .iter()
                            .any(|existing| existing.equal(condition))
                        {
                            residual_table_filters.push(condition.clone());
                        }
                    }
                }
                let table_stats = ds
                    .table_stats
                    .clone()
                    .or_else(|| ds.base.base.stats_info().cloned());
                let probe_access_rows_floor = prop.index_join_prop.as_ref().and_then(|runtime| {
                    index_join_probe_access_rows_floor(
                        ds,
                        &common_columns,
                        runtime,
                        ctx.index_join_probe_row_count_fix,
                        ctx.group_ndv_skew_ratio,
                    )
                });
                heuristic = Some(crate::find_best_task::candidate::HeuristicPath {
                    range_count: ranges.len(),
                    only_points: ranges.iter().all(|range| {
                        if let Some(index) = common_handle {
                            range.is_point_non_nullable()
                                && range.low_val.len() == index.columns.len()
                        } else {
                            range.is_point_nullable()
                        }
                    }),
                    unique: true,
                    single_scan: true,
                    table_filter_count: residual_table_filters.len(),
                    access_columns: crate::column_length::Col2Len::from_pairs(
                        table_access_conds.iter().flat_map(|condition| {
                            tidb_expr::simple_expr::extract_columns(condition)
                                .into_iter()
                                .map(|column| (column.unique_id, -1))
                        }),
                    ),
                });
                let mut count_after_access = table_path.count_after_access;
                if let (Some(count), Some(floor)) =
                    (count_after_access.as_mut(), probe_access_rows_floor)
                {
                    *count = count.max(floor);
                }
                if let (Some(count), Some(ds_stats), Some(base_stats)) = (
                    count_after_access.as_mut(),
                    ds.base.base.stats_info(),
                    table_stats.as_ref(),
                ) {
                    if *count + crate::cost_factors::TOLERANCE_FACTOR < ds_stats.row_count() {
                        *count = (ds_stats.row_count() / crate::cost_factors::SELECTION_FACTOR)
                            .min(base_stats.row_count());
                    }
                }
                if let Some(access) = count_after_access.filter(|_| !prepared) {
                    let columns = crate::column_length::Col2Len::from_pairs(
                        table_access_conds.iter().flat_map(|condition| {
                            tidb_expr::simple_expr::extract_columns(condition)
                                .into_iter()
                                .map(|column| (column.unique_id, -1))
                        }),
                    );
                    candidate_metrics = Some(
                        crate::find_best_task::candidate::CandidateMetrics {
                            access_columns: columns.clone(),
                            index_columns: columns,
                            table_path: true,
                            single_scan: true,
                            pseudo: table_pseudo,
                            matches_property: !ordered || keep_order,
                            count_after_access: access,
                            count_after_index: access,
                            ..Default::default()
                        },
                    );
                }
                let mut stats = table_stats.as_ref().map(|stats| {
                    stats.scale_by_expect_cnt(
                        count_after_access.unwrap_or_else(|| stats.row_count()),
                        ctx.skew_ratio,
                    )
                });
                if prop.index_join_prop.is_none() {
                    if let (Some(path_stats), Some(ds_stats), Some(table_stats)) = (
                        stats.as_ref(),
                        ds.base.base.stats_info(),
                        ds.table_stats.as_ref(),
                    ) {
                        let count_after_access = path_stats.row_count();
                        let original_row_count = ds_stats.row_count();
                        if prop.expected_cnt + crate::cost_factors::TOLERANCE_FACTOR
                            < original_row_count
                            || (keep_order
                                && original_row_count.min(prop.expected_cnt) < count_after_access
                                && !table_access_conds.is_empty())
                        {
                            let mut row_count = count_after_access;
                            if prop.expected_cnt < original_row_count {
                                let selectivity = original_row_count / count_after_access;
                                let uniform_est =
                                    count_after_access.min(prop.expected_cnt / selectivity);
                                let correlation_options = ctx.correlation_options;
                                let (column_count, correlation_column, correlation) =
                                    if correlation_options.enabled
                                        && !table_filters.is_empty()
                                        && !table_stats
                                            .hist_coll()
                                            .is_some_and(crate::stats_info::HistColl::pseudo)
                                    {
                                        filter_column_correlation(
                                            table_stats,
                                            &table_filters,
                                            correlation_options.threshold,
                                        )
                                    } else {
                                        (0, None, 0.0)
                                    };
                                let abs_correlation = correlation.abs();
                                let cross_estimate = if column_count == 1
                                    && correlation_column.is_some()
                                    && table_access_conds.is_empty()
                                {
                                    let source_hist_coll = ds
                                        .base
                                        .base
                                        .stats_info()
                                        .and_then(|stats| stats.hist_coll());
                                    let table_hist_coll = table_stats.hist_coll();
                                    match (source_hist_coll, table_hist_coll, correlation_column) {
                                        (Some(source), Some(table), Some(column)) => Some(
                                            crate::cardinality::cross_estimation::estimate_table_cross_row_count(
                                                source,
                                                table,
                                                &table_filters,
                                                &column,
                                                correlation,
                                                prop.expected_cnt,
                                                count_after_access,
                                                desc,
                                                ctx.range_max_size,
                                                ctx.expression_evaluator,
                                                ctx.estimator_options,
                                            ),
                                        ),
                                        _ => None,
                                    }
                                } else {
                                    None
                                };
                                let (cross_count, cross_ok, fallback_correlation) =
                                    cross_estimate.unwrap_or((0.0, false, correlation));
                                if cross_ok {
                                    row_count = uniform_est.max(cross_count);
                                } else if fallback_correlation.abs() < 1.0 {
                                    let correlation_factor = (1.0 - abs_correlation)
                                        .powf(correlation_options.exponent as f64);
                                    row_count =
                                        count_after_access.min(uniform_est / correlation_factor);
                                }
                            }
                            if keep_order && count_after_access > row_count {
                                let ratio = ctx.ordering_index_selectivity_ratio;
                                if ratio > 0.0 {
                                    row_count += (count_after_access - row_count).max(0.0) * ratio;
                                }
                            }
                            stats =
                                Some(table_stats.scale_by_expect_cnt(row_count, ctx.skew_ratio));
                        }
                    }
                }
                let probe_selectivity =
                    if prop.index_join_prop.is_none() || residual_table_filters.is_empty() {
                        1.0
                    } else {
                        table_stats
                        .as_ref()
                        .and_then(|stats| {
                            if ds.table_scan_penalty.pseudo_stats {
                                crate::logical::rewrite::pseudo_range_filter_selectivity(
                                    ds,
                                    stats,
                                    &residual_table_filters,
                                    ds.base.base.schema()?,
                                    &ctx.access_path_derivation_context(),
                                    ctx.selectivity_factor,
                                )
                            } else {
                                crate::logical::rewrite::analyzed_filter_selectivity_in(
                                    stats,
                                    &residual_table_filters,
                                    &ctx.access_path_derivation_context(),
                                )
                            }
                        })
                        .filter(|value| *value > 0.0)
                        .unwrap_or(crate::cost_factors::SELECTION_FACTOR)
                    };
                if let Some(runtime) = &prop.index_join_prop {
                    // Go `constructDS2TableScanTask`: the runtime row count is
                    // the per-outer-row average, capped at one row for a
                    // complete unique equality probe.
                    let output_rows = if runtime.avg_inner_row_count > 0.0 {
                        runtime.avg_inner_row_count
                    } else {
                        1.0
                    };
                    let mut runtime_rows = (output_rows / probe_selectivity)
                        .max(probe_access_rows_floor.unwrap_or(0.0));
                    if index_join_path_is_max_one_row(ds, path, runtime) {
                        runtime_rows = runtime_rows.min(1.0);
                    }
                    stats = probe_access_rows_floor
                        .and_then(|_| {
                            table_stats.as_ref().map(|table_stats| {
                                table_stats.scale_by_expect_cnt(runtime_rows, ctx.skew_ratio)
                            })
                        })
                        .or_else(|| {
                            Some(
                                crate::stats_info::StatsInfo::new(runtime_rows, [])
                                    .with_stats_version(
                                        ds.base
                                            .base
                                            .stats_info()
                                            .map_or(0, |stats| stats.stats_version()),
                                    ),
                            )
                        });
                }
                base.base.set_stats(stats.clone());
                let table_range_rebuild = if table_access_conds.is_empty() {
                    None
                } else if common_handle.is_some() {
                    Some(
                        crate::physical_plan_cache::TableRangeRebuild::common_handle(
                            table_access_conds.clone(),
                            common_columns.clone(),
                            common_lengths.clone(),
                        ),
                    )
                } else {
                    Some(crate::physical_plan_cache::TableRangeRebuild::int_handle(
                        table_access_conds.clone(),
                        handle_type.clone(),
                        handle_type.is_unsigned(),
                    ))
                };
                let explicit_physical_id = match ds.partition_names.as_slice() {
                    [name] => ds
                        .partition_definition_names
                        .iter()
                        .position(|definition| definition.eq_ignore_ascii_case(name))
                        .and_then(|index| ds.partition_definition_ids.get(index).copied()),
                    _ => None,
                };
                // Go `canConvertPointGet`: an integer handle, or a complete
                // non-prefix UNIQUE common handle whose range covers every key
                // column.
                let point_handle_ok = common_handle.map_or(handle_column.is_some(), |index| {
                    index.unique
                        && !crate::ranger::ranger::has_prefix(
                            &index
                                .columns
                                .iter()
                                .map(|column| column.length)
                                .collect::<Vec<_>>(),
                        )
                        && ranges
                            .iter()
                            .all(|range| range.low_val.len() == index.columns.len())
                });
                if prop.index_join_prop.is_none()
                    && ctx.enable_point_get_conversion
                    && point_handle_ok
                    // Go disallows ordinary partitioned batches in dynamic
                    // prune mode, including an explicit PARTITION(p).
                    && (ranges.len() == 1 || ds.dynamic_partition_access.is_none())
                    && !ranges.is_empty()
                    && ranges.iter().all(|range| {
                        !range.low_exclude
                            && !range.high_exclude
                            && range.low_val == range.high_val
                            && range.low_val.iter().all(|value| !value.is_null())
                    })
                    && (ds.partition_definition_ids.is_empty()
                        || ds.physical_table_id != ds.table_id
                        || explicit_physical_id.is_some()
                        || (ranges.len() == 1 && (ds.pk_is_handle || common_handle.is_some())))
                {
                    let mut point_base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator,
                        if ranges.len() == 1 {
                            "Point_Get"
                        } else {
                            "Batch_Point_Get"
                        },
                        ds.base.base.query_block_offset(),
                    );
                    point_base.base.set_schema(ds.base.base.schema().cloned());
                    // Go caps PointGet at one row and BatchPointGet at the
                    // number of point ranges (convertToBatchPointGet).
                    point_base.base.set_stats(
                        table_stats
                            .as_ref()
                            .map(|table_stats| {
                                table_stats.scale_by_expect_cnt(
                                    count_after_access
                                        .unwrap_or(ranges.len() as f64)
                                        .min(ranges.len() as f64),
                                    ctx.skew_ratio,
                                )
                            })
                            .or(stats),
                    );
                    let mut point = if ranges.len() == 1 {
                        PhysicalPlan::PointGet(crate::physical::PhysicalPointGet {
                            base: point_base,
                            table_id: if ds.pk_is_handle || common_handle.is_some() {
                                ds.physical_table_id
                            } else {
                                explicit_physical_id.unwrap_or(ds.physical_table_id)
                            },
                            partition: (!ds.partition_definition_ids.is_empty()
                                && ds.physical_table_id == ds.table_id
                                && (ds.pk_is_handle || common_handle.is_some()))
                            .then(|| crate::physical::PointGetPartition {
                                names: ds.partition_names.clone(),
                                physical_table_id: None,
                            }),
                            index_id: None,
                            access_cols: Some(ds.table_columns.clone()),
                            ranges,
                            range_rebuild: table_range_rebuild
                                .clone()
                                .map(crate::physical_plan_cache::PointRangeRebuild::Table),
                            lock: false,
                        })
                    } else {
                        PhysicalPlan::BatchPointGet(crate::physical::PhysicalBatchPointGet {
                            base: point_base,
                            table_id: explicit_physical_id.unwrap_or(ds.physical_table_id),
                            index_id: None,
                            access_cols: Some(ds.table_columns.clone()),
                            ranges,
                            unsigned_handle: handle_type.is_unsigned(),
                            partition_ids: None,
                            range_rebuild: table_range_rebuild
                                .clone()
                                .map(crate::physical_plan_cache::PointRangeRebuild::Table),
                            keep_order,
                            desc,
                        })
                    };
                    if !table_filters.is_empty() {
                        let mut selection_base = crate::physical::BasePhysicalPlan::new(
                            ctx.allocator,
                            "Selection",
                            ds.base.base.query_block_offset(),
                        );
                        selection_base
                            .base
                            .set_schema(ds.base.base.schema().cloned());
                        // Go reuses the datasource's final filtered profile;
                        // residual conditions have already contributed to it.
                        selection_base.base.set_stats(
                            ds.base.base.stats_info().map(|stats| {
                                stats.scale_by_expect_cnt(prop.expected_cnt, ctx.skew_ratio)
                            }),
                        );
                        selection_base.set_children(vec![point]);
                        point = PhysicalPlan::Selection(crate::physical::PhysicalSelection {
                            base: selection_base,
                            conditions: table_filters,
                            from_data_source: true,
                        });
                    }
                    let mut root = crate::task::RootTask::default();
                    root.set_plan(point);
                    return Ok(Task::Root(root));
                }
                let (table_filters, root_task_conds) = crate::pushdown::split_scan_filters(
                    table_filters, tidb_expr::infer_pushdown::PushDownStore::TiKv,
                    &ctx.expr_pushdown_blacklist,
                );
                // Go `PhysicalTableScan.IsFullScan`: `len(p.RangeInfo) > 0 ||
                // p.haveCorCol()` short-circuits to "not full" before the
                // ranges are even inspected. An index-join inner probe's
                // access condition is built against the outer row's join key
                // (a correlated column), so `haveCorCol()` is always true
                // there regardless of what the plan-time placeholder range
                // happens to look like -- an int-handle placeholder
                // (`full_int_range`) and a common-handle one (`full_range`)
                // are equally "whatever the outer row supplies", but only the
                // former happened to fail `is_full_range`'s boundary check,
                // so gate on the index-join probe directly rather than on
                // that coincidence. Outside an index join, the name is
                // decided by the RANGES, not by whether an access condition
                // exists: a predicate on a handle component can leave every
                // per-partition range full (this table's `part > 199999`
                // after partition pruning), and Go still renders
                // `TableFullScan`.
                let unsigned_int_handle = ds.pk_is_handle && handle_type.is_unsigned();
                let scan_kind = if prop.index_join_prop.is_some()
                    || !ranges
                        .iter()
                        .all(|range| range.is_full_range(unsigned_int_handle))
                {
                    crate::access_path::ResolvedTableScanKind::Range
                } else {
                    crate::access_path::ResolvedTableScanKind::Full
                };
                let scan = PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
                    base,
                    table_id: ds.physical_table_id,
                    table_as_name: ds.table_as_name.clone(),
                    dynamic_partition_access: ds.dynamic_partition_access.clone(),
                    cost_columns: ds.table_columns.clone(),
                    store_type: crate::physical_table_reader::StoreType::TiKv,
                    keep_order,
                    desc,
                    ranges: ranges.clone(),
                    range_rebuild: table_range_rebuild,
                    table_scan_penalty: ds.table_scan_penalty,
                    tikv_pushdown: None,
                    resolved_descriptor: Some(crate::access_path::ResolvedTableDescriptor::new(
                        ds.physical_table_id,
                        common_handle.is_some(),
                        scan_kind,
                        crate::access_path::TableScanExplainIdSuffix::IncludePlanId,
                    )),
                });
                let table_plan = if table_filters.is_empty() {
                    scan
                } else {
                    // Go `addPushedDownSelection`: access cardinality belongs
                    // to the scan; the DataSource's post-filter cardinality
                    // belongs to the Selection above it.
                    let mut selection_base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator,
                        "Selection",
                        ds.base.base.query_block_offset(),
                    );
                    selection_base
                        .base
                        .set_schema(ds.base.base.schema().cloned());
                    // Go `addPushedDownSelection4PhysicalTableScan`:
                    // `selStats = ts.StatsInfo().Scale(selectivity)`. For an
                    // IndexJoin inner scan the scan carries the per-probe
                    // ACCESS rows, so the Selection applies the residual
                    // filters' selectivity once more; an ordinary scan starts
                    // from the DataSource's post-filter estimate.
                    selection_base
                        .base
                        .set_stats(if !root_task_conds.is_empty() {
                            ctx.pushed_filter_stats(ds, scan.stats_info(), &table_filters)
                        } else if prop.index_join_prop.is_some() {
                            stats
                                .as_ref()
                                .map(|stats| stats.scale(probe_selectivity, ctx.skew_ratio))
                        } else {
                            ds.base.base.stats_info().map(|stats| {
                                stats.scale_by_expect_cnt(prop.expected_cnt, ctx.skew_ratio)
                            })
                        });
                    selection_base.set_children(vec![scan]);
                    PhysicalPlan::Selection(crate::physical::PhysicalSelection {
                        base: selection_base,
                        conditions: table_filters,
                        from_data_source: true,
                    })
                };
                Task::Cop(crate::task::CopTask {
                    table_plan: Some(Box::new(table_plan)),
                    index_plan_finished: true,
                    keep_order,
                    expect_cnt: prop.expected_cnt as u64,
                    stats_context: ctx.task_stats_context(ds, &root_task_conds),
                    root_task_conds,
                    index_join_info: match &prop.index_join_prop {
                        Some(runtime) => Some(match index_join_feedback(ds, path, runtime, ctx) {
                            Some(info) => info,
                            None => continue 'paths,
                        }),
                        None => None,
                    },
                    ..crate::task::CopTask::default()
                })
            }
            crate::access_path::PossiblePath::TiFlashTable => {
                if partial_order.is_some() || cop_multi_read || prop.index_join_prop.is_some() {
                    continue 'paths;
                }
                let keep_order = ordered;
                if keep_order && !table_path_matches_order(ds, prop) {
                    continue 'paths;
                }
                let mut base = crate::physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    "TableScan",
                    ds.base.base.query_block_offset(),
                );
                base.base.set_schema(ds.base.base.schema().cloned());
                base.base.set_stats(
                    ds.table_stats
                        .clone()
                        .or_else(|| ds.base.base.stats_info().cloned()),
                );
                let ranges = crate::ranger::points::full_int_range(false);
                let scan = PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
                    base,
                    table_id: ds.physical_table_id,
                    table_as_name: ds.table_as_name.clone(),
                    dynamic_partition_access: ds.dynamic_partition_access.clone(),
                    cost_columns: ds.table_columns.clone(),
                    store_type: crate::physical_table_reader::StoreType::TiFlash,
                    keep_order,
                    desc,
                    ranges,
                    range_rebuild: None,
                    table_scan_penalty: ds.table_scan_penalty,
                    tikv_pushdown: None,
                    resolved_descriptor: Some(crate::access_path::ResolvedTableDescriptor::new(
                        ds.physical_table_id,
                        ds.is_common_handle,
                        crate::access_path::ResolvedTableScanKind::Full,
                        crate::access_path::TableScanExplainIdSuffix::IncludePlanId,
                    )),
                });
                let (table_filters, root_task_conds) = crate::pushdown::split_scan_filters(
                    ds.pushed_down_conds.clone(), tidb_expr::infer_pushdown::PushDownStore::TiFlash,
                    &ctx.expr_pushdown_blacklist,
                );
                let table_plan = if table_filters.is_empty() {
                    scan
                } else {
                    let mut selection_base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator,
                        "Selection",
                        ds.base.base.query_block_offset(),
                    );
                    selection_base
                        .base
                        .set_schema(ds.base.base.schema().cloned());
                    // Go `PhysicalSelection.Init` (physical_selection.go:78):
                    // the selection's stats scale by the required property's
                    // ExpectedCnt -- a Limit above the scan caps the
                    // selection's estimate.
                    selection_base.base.set_stats(
                        if !root_task_conds.is_empty() {
                            ctx.pushed_filter_stats(ds, scan.stats_info(), &table_filters)
                        } else { ds.base.base.stats_info().cloned().map(|stats| {
                            stats.scale_by_expect_cnt(prop.expected_cnt, ctx.skew_ratio)
                        }) },
                    );
                    selection_base.set_children(vec![scan]);
                    PhysicalPlan::Selection(crate::physical::PhysicalSelection {
                        base: selection_base,
                        conditions: table_filters,
                        from_data_source: true,
                    })
                };
                Task::Cop(crate::task::CopTask {
                    table_plan: Some(Box::new(table_plan)),
                    index_plan_finished: true,
                    keep_order,
                    expect_cnt: prop.expected_cnt as u64,
                    stats_context: ctx.task_stats_context(ds, &root_task_conds),
                    root_task_conds,
                    ..crate::task::CopTask::default()
                })
            }
            crate::access_path::PossiblePath::Index { index } => 'index_path: {
                let Some(source_index) = ds.indexes.get(*index) else {
                    continue 'paths;
                };
                let path_key = (std::ptr::from_ref(ds).addr(), source_index.id);
                if partial_order.is_none() && ctx.forced_partial_order_paths.contains(&path_key) {
                    // The earlier match already proved this immutable path's
                    // ranges nonempty; do not rebuild them for a refused task.
                    continue 'paths;
                }
                // Go `detachCondAndBuildRangeForPath`: the index columns
                // (schema columns at the index's offsets) detach the pushed
                // conditions into this path's ranges.
                // Go retains the usable leading `IdxCols` after logical
                // column pruning. A missing later index column does not
                // invalidate ranges on an earlier prefix; only the first
                // unresolved position ends that prefix.
                let filled_path = prop.index_join_prop.is_none().then(|| {
                    ds.derived_index_paths.get(&source_index.id).and_then(|path| path.filled.as_ref())
                }).flatten();
                let resolved_index_prefix = filled_path.map_or_else(
                    || ds.index_range_columns(source_index), |path| path.columns.clone(),
                );
                let declared_index_prefix_complete =
                    resolved_index_prefix.len() >= source_index.columns.len();
                // Go `find_best_task.go:2204`: `canConvertPointGet =
                // path.Index.Unique && !path.Index.HasPrefixIndex()`. A prefix
                // entry holds `'abc'` where the row holds `'abcdef'`, and a
                // point get has no residual predicate to notice, so a prefix
                // index must stay an IndexReader/IndexLookUp candidate.
                let declared_index_has_prefix = crate::ranger::ranger::has_prefix(
                    &source_index
                        .columns
                        .iter()
                        .map(|column| column.length)
                        .collect::<Vec<_>>(),
                );
                let index_cols = resolved_index_prefix
                    .iter()
                    .map(|(column, _)| column.clone())
                    .collect::<Vec<_>>();
                let index_lengths = resolved_index_prefix
                    .iter()
                    .map(|(_, length)| *length)
                    .collect::<Vec<_>>();
                let detach = if let Some(path) = filled_path {
                    Some(path.detached.clone())
                } else if ds.pushed_down_conds.is_empty() || index_cols.is_empty() {
                    None
                } else {
                    match ctx.detach_index_range(&ds.pushed_down_conds, &index_cols, &index_lengths)
                    {
                        Ok(result) => Some(result),
                        Err(_) => None,
                    }
                };
                let ranges = detach
                    .as_ref()
                    .map_or_else(crate::ranger::points::full_range, |result| {
                        result.ranges.clone()
                    });
                if detach
                    .as_ref()
                    .is_some_and(|result| result.ranges.is_empty())
                {
                    return Ok(empty_range_dual_task(ds, ctx));
                }
                let partial_order_match = partial_order
                    .and_then(|info| match_partial_order_property(ds, source_index, info));
                if partial_order.is_some() && partial_order_match.is_none() {
                    continue 'paths;
                }
                if ctx.partial_ordered_index_for_topn
                    && partial_order_match.is_some()
                    && ds.forced_index_ids.contains(&source_index.id)
                    && !ds.force_no_keep_order_index_ids.contains(&source_index.id)
                {
                    // Go skylinePruning marks the path before conversion, even
                    // if this candidate's single/double-read task is refused.
                    ctx.forced_partial_order_paths.insert(path_key);
                }
                let keep_order = ordered || partial_order_match.is_some();
                if (!keep_order && ds.force_keep_order_index_ids.contains(&source_index.id))
                    || (keep_order && ds.force_no_keep_order_index_ids.contains(&source_index.id))
                {
                    continue 'paths;
                }
                if ordered && !index_path_matches_order(ds, source_index, prop) {
                    continue 'paths;
                }
                // `convertToIndexScan`: a path that is NOT a single scan
                // reads the table rows back through an IndexLookUp double
                // read (`BuildIndexLookUpTask` at conversion) — the cop task
                // carries BOTH halves, exactly Go's shape.
                let single_scan = ds
                    .derived_index_paths
                    .get(&source_index.id)
                    .and_then(|path| path.is_single_scan)
                    .unwrap_or_else(|| {
                        index_path_is_single_scan(
                            ds,
                            source_index,
                            ctx.opt_prefix_index_single_scan,
                        )
                    });
                // The two COP property kinds are disjoint: a covering index
                // is single-read, while a lookup is multi-read.
                if (prop.task_tp == TaskType::CopSingleRead && !single_scan)
                    || (cop_multi_read && single_scan)
                {
                    continue 'paths;
                }
                // Go `skylinePruning` keepIndex (`find_best_task.go:1812`):
                // an index path with no access conditions, no order match, no
                // force hint, and no single-scan coverage is pruned before
                // candidates are built. `PartialOrderInfo` is separate from
                // `SortItems`, so its successful prefix match must be tested
                // explicitly here.
                let access_conds_empty = detach
                    .as_ref()
                    .is_none_or(|detached| detached.access_conds.is_empty());
                if access_conds_empty
                    && prop.index_join_prop.is_none()
                    && prop.is_sort_item_empty()
                    && partial_order_match.is_none()
                    && !ds.forced_index_ids.contains(&source_index.id)
                    && !single_scan
                {
                    continue 'paths;
                }
                if !prepared && prop.index_join_prop.is_none() {
                    candidate_metrics = ds
                        .derived_index_paths
                        .get(&source_index.id)
                        .and_then(|path| {
                            super::candidate::index_candidate_metrics(
                                ds,
                                source_index,
                                path,
                                single_scan,
                                !ordered || keep_order,
                            )
                        });
                }
                let mut base = crate::physical::BasePhysicalPlan::new(
                    ctx.allocator,
                    "IndexScan",
                    ds.base.base.query_block_offset(),
                );
                let index_schema = super::index_merge_union::index_scan_schema(
                    ds, source_index, ctx, !single_scan,
                )?;
                if ds.partial_index_noncacheable_ids.contains(&source_index.id) {
                    base.base
                        .set_noncacheable_reason("IndexScan of partial index is uncacheable");
                }
                base.base.set_schema(Some(tidb_expr::schema::Schema::new(index_schema)));
                // Go `indexFilters := c.eqOrInCount > 0 || ...` plus
                // `!c.isFullRange`: this candidate is what the prefer-range
                // override keeps.
                cur_is_full_range = crate::ranger::types::has_full_range(&ranges, false);
                cur_preferred_range = detach
                    .as_ref()
                    .is_some_and(|result| result.eq_or_in_count > 0)
                    && !cur_is_full_range;
                let mut remained_conds = detach.as_ref().map_or_else(
                    || ds.pushed_down_conds.clone(),
                    |result| result.remained_conds.clone(),
                );
                if prop.index_join_prop.is_some() {
                    // Go `constructDS2IndexScanTask` re-attaches every
                    // inner-only access condition as an explicit probe-side
                    // Selection: the runtime ranges come from the join keys,
                    // so a static predicate such as `h_w_id = 1` is a residual
                    // filter that keeps the `IndexRangeScan -> Selection`
                    // shape (`exhaust_physical_plans.go:913-917`).
                    if let (Some(result), Some(schema)) = (detach.as_ref(), ds.base.base.schema()) {
                        for condition in &result.access_conds {
                            if tidb_expr::expr_util::normal_form::expr_from_schema(
                                condition, schema,
                            ) && !remained_conds
                                .iter()
                                .any(|existing| existing.equal(condition))
                            {
                                remained_conds.push(condition.clone());
                            }
                        }
                    }
                }
                // Go retains `AccessCondition` on PhysicalIndexScan, not
                // every pushed predicate. Rebuilding the latter would feed
                // residual filters back into the ranger and make a safe
                // parameter change look uncacheable.
                let index_range_rebuild = declared_index_prefix_complete
                    .then_some(detach.as_ref())
                    .flatten()
                    .filter(|result| !result.access_conds.is_empty())
                    .map(|result| {
                        crate::physical_plan_cache::IndexRangeRebuild::new(
                            result.access_conds.clone(),
                            index_cols.clone(),
                            index_lengths.clone(),
                        )
                    });
                let covered = |condition: &tidb_expr::expression::Expression| {
                    index_covers_condition(
                        ds,
                        source_index,
                        condition,
                        ctx.opt_prefix_index_single_scan,
                    )
                };
                heuristic = detach.as_ref().map(|detached| {
                    crate::find_best_task::candidate::HeuristicPath {
                        range_count: ranges.len(),
                        only_points: declared_index_prefix_complete
                            && !declared_index_has_prefix
                            && ranges.iter().all(|range| {
                                range.is_point_non_nullable()
                                    && range.low_val.len() == source_index.columns.len()
                            }),
                        unique: source_index.unique,
                        single_scan,
                        table_filter_count: remained_conds
                            .iter()
                            .filter(|condition| !covered(condition))
                            .count(),
                        access_columns: crate::column_length::Col2Len::from_pairs(
                            detached.access_conds.iter().flat_map(|condition| {
                                tidb_expr::simple_expr::extract_columns(condition)
                                    .into_iter()
                                    .filter_map(|column| {
                                        index_cols
                                            .iter()
                                            .position(|index| index.unique_id == column.unique_id)
                                            .map(|index| (column.unique_id, index_lengths[index]))
                                    })
                            }),
                        ),
                    }
                });
                // Go `tryConvertToPointGet`: a complete non-NULL point range
                // on a unique index is a complete root point plan, not an
                // IndexReader/IndexLookUp candidate. Residual conditions stay
                // as the ordinary root Selection above it.
                if prop.index_join_prop.is_none()
                    && ctx.enable_point_get_conversion
                    && (ds.partition_definition_ids.is_empty()
                        || ds.physical_table_id != ds.table_id
                        // A GLOBAL index is stored in the logical table's
                        // keyspace and carries each row's partition ID, so a
                        // unique-key point read does not need static pruning
                        // to choose one physical table first.
                        || source_index.global)
                    && source_index.unique
                    && !declared_index_has_prefix
                    && declared_index_prefix_complete
                    && !ranges.is_empty()
                    && ranges.iter().all(|range| {
                        range.is_point_non_nullable()
                            && range.high_val.len() == source_index.columns.len()
                    })
                {
                    let mut point_base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator,
                        if ranges.len() == 1 {
                            "Point_Get"
                        } else {
                            "Batch_Point_Get"
                        },
                        ds.base.base.query_block_offset(),
                    );
                    if ds.partial_index_noncacheable_ids.contains(&source_index.id) {
                        point_base
                            .base
                            .set_noncacheable_reason("IndexScan of partial index is uncacheable");
                    }
                    point_base.base.set_schema(ds.base.base.schema().cloned());
                    let access_rows = ds
                        .derived_index_paths
                        .get(&source_index.id)
                        .and_then(|path| path.count_after_access())
                        .unwrap_or(ranges.len() as f64)
                        .min(ranges.len() as f64);
                    point_base.base.set_stats(
                        ds.table_stats
                            .as_ref()
                            .or_else(|| ds.base.base.stats_info())
                            .map(|stats| stats.scale_by_expect_cnt(access_rows, ctx.skew_ratio)),
                    );
                    let mut point = if ranges.len() == 1 {
                        PhysicalPlan::PointGet(crate::physical::PhysicalPointGet {
                            base: point_base,
                            table_id: ds.physical_table_id,
                            partition: None,
                            index_id: Some(source_index.id),
                            access_cols: Some(if single_scan {
                                index_cols.clone()
                            } else {
                                ds.table_columns.clone()
                            }),
                            ranges: ranges.clone(),
                            range_rebuild: index_range_rebuild
                                .clone()
                                .map(crate::physical_plan_cache::PointRangeRebuild::Index),
                            lock: false,
                        })
                    } else {
                        PhysicalPlan::BatchPointGet(crate::physical::PhysicalBatchPointGet {
                            base: point_base,
                            table_id: ds.physical_table_id,
                            index_id: Some(source_index.id),
                            access_cols: Some(if single_scan {
                                index_cols.clone()
                            } else {
                                ds.table_columns.clone()
                            }),
                            ranges: ranges.clone(),
                            unsigned_handle: false,
                            partition_ids: None,
                            range_rebuild: index_range_rebuild
                                .clone()
                                .map(crate::physical_plan_cache::PointRangeRebuild::Index),
                            keep_order,
                            desc,
                        })
                    };
                    if !remained_conds.is_empty() {
                        let mut selection_base = crate::physical::BasePhysicalPlan::new(
                            ctx.allocator,
                            "Selection",
                            ds.base.base.query_block_offset(),
                        );
                        selection_base
                            .base
                            .set_schema(ds.base.base.schema().cloned());
                        selection_base
                            .base
                            .set_stats(ds.base.base.stats_info().cloned().map(|stats| {
                                stats.scale_by_expect_cnt(prop.expected_cnt, ctx.skew_ratio)
                            }));
                        selection_base.set_children(vec![point]);
                        point = PhysicalPlan::Selection(crate::physical::PhysicalSelection {
                            base: selection_base,
                            conditions: remained_conds,
                            from_data_source: true,
                        });
                    }
                    let mut root = crate::task::RootTask::default();
                    root.set_plan(point);
                    break 'index_path Task::Root(root);
                }
                let table_stats = ds
                    .table_stats
                    .clone()
                    .or_else(|| ds.base.base.stats_info().cloned());
                let probe_access_rows_floor = prop.index_join_prop.as_ref().and_then(|runtime| {
                    index_join_probe_access_rows_floor(
                        ds,
                        &index_cols,
                        runtime,
                        ctx.index_join_probe_row_count_fix,
                        ctx.group_ndv_skew_ratio,
                    )
                });
                let mut count_after_access = table_stats.as_ref().map(|stats| stats.row_count());
                let ranges_include_appended_handle =
                    resolved_index_prefix.len() > source_index.columns.len()
                        && ranges.iter().any(|range| {
                            range.low_val.len() > source_index.columns.len()
                                || range.high_val.len() > source_index.columns.len()
                        });
                if detach.is_some() {
                    count_after_access = ds
                        .derived_index_paths
                        .get(&source_index.id)
                        .and_then(|path| path.count_after_access())
                        .or_else(|| {
                            let base_stats = table_stats.as_ref()?;
                            // Go `deriveIndexPathStats` trims the signed handle
                            // appended by `fillIndexPath` back to the declared
                            // index columns before estimating CountAfterAccess.
                            // The handle remains in `ranges` for execution.
                            let estimate_ranges =
                                if resolved_index_prefix.len() > source_index.columns.len() {
                                    ranges
                                        .iter()
                                        .cloned()
                                        .map(|mut range| {
                                            range.low_val.truncate(source_index.columns.len());
                                            range.high_val.truncate(source_index.columns.len());
                                            range.collators.truncate(source_index.columns.len());
                                            range
                                        })
                                        .collect()
                                } else {
                                    ranges.clone()
                                };
                            Some(crate::ranger::stats_bridge::pseudo_count_by_ranges(
                                &estimate_ranges,
                                base_stats.row_count(),
                            ))
                        });
                }
                index_join_skyline_count = prop.index_join_prop.as_ref().and_then(|runtime| {
                    index_join_skyline_count_for_index(
                        ds,
                        runtime,
                        &resolved_index_prefix,
                        count_after_access,
                    )
                });
                if let (Some(count), Some(floor)) =
                    (count_after_access.as_mut(), probe_access_rows_floor)
                {
                    *count = count.max(floor);
                }
                if let (Some(count), Some(ds_stats), Some(base_stats)) = (
                    count_after_access.as_mut(),
                    ds.base.base.stats_info(),
                    table_stats.as_ref(),
                ) {
                    // Filled ordinary paths were adjusted during logical derivation.
                    // Runtime probes and unfilled sources retain this fallback.
                    let logically_adjusted = filled_path.is_some()
                        && ds
                            .derived_index_paths
                            .get(&source_index.id)
                            .is_some_and(|path| path.row_estimate.is_some());
                    if !logically_adjusted
                        && *count + crate::cost_factors::TOLERANCE_FACTOR < ds_stats.row_count()
                    {
                        if ranges_include_appended_handle {
                            // Go's `adjustCountAfterAccess` aligns estimates that
                            // already credit appended-handle predicates to the
                            // datasource row count without applying the generic
                            // SelectionFactor penalty a second time.
                            *count = ds_stats.row_count();
                        } else {
                            *count = (ds_stats.row_count()
                                / crate::cost_factors::SELECTION_FACTOR)
                                .min(base_stats.row_count());
                        }
                    }
                }
                // Go `GetOriginalPhysicalIndexScan` calls
                // `AdjustRowCountForIndexScanByLimit` before pricing the
                // scan. With pseudo statistics its cross-estimation arm
                // reduces to the uniform estimate below; residual filters on
                // an ordering index then add the session's risk ratio. The
                // missing adjustment made an ordered LIMIT price the whole
                // index, so Rust chose `TopN -> TableReader` where Go chooses
                // the bounded IndexLookUp path.
                if let (Some(count), Some(ds_stats)) =
                    (count_after_access.as_mut(), ds.base.base.stats_info())
                {
                    let ignore_expected_count = ignore_index_scan_expected_count(
                        ctx.ordering_index_selectivity_threshold,
                        access_path_min_selectivity,
                        !remained_conds.is_empty(),
                    );
                    if (keep_order || prop.is_sort_item_empty())
                        && prop.expected_cnt < ds_stats.row_count()
                        && *count > 0.0
                        && !ignore_expected_count
                    {
                        *count = adjust_index_scan_count_by_expected(
                            *count,
                            prop.expected_cnt,
                            ds_stats.row_count(),
                            !remained_conds.is_empty(),
                            ctx.ordering_index_selectivity_ratio,
                        );
                    }
                }
                let stats = table_stats.as_ref().map(|stats| {
                    stats.scale_by_expect_cnt(
                        count_after_access.unwrap_or_else(|| stats.row_count()),
                        ctx.skew_ratio,
                    )
                });
                // Go `constructDS2IndexScanTask` keeps TWO counts apart: the
                // IndexScan carries `tmpPath.CountAfterAccess` (the static
                // access estimate), while the pushed-down Selection and the
                // lookup's table side carry `finalStats`/`CountAfterIndex`,
                // the per-outer-row runtime count.
                // Go splitIndexFilterConditions uses the same handle/full-column
                // coverage as IsSingleScan, including appended primary-key columns.
                let (index_filters, table_filters): (Vec<_>, Vec<_>) = filled_path.map_or_else(
                    || remained_conds.into_iter().partition(covered),
                    |path| (path.index_filters.clone(), path.table_filters.clone()),
                );
                // Go `constructDS2IndexScanTask` sets the scan to
                // `tmpPath.CountAfterAccess`, which for a runtime probe is the
                // per-outer-row count divided by the residual index-filter
                // selectivity; the static access estimate is used otherwise.
                let runtime_counts = prop.index_join_prop.as_ref().map(|runtime| {
                    let upper_bound = if ctx.index_join_row_count_upper_bound {
                        let fixed = index_join_fixed_ids(ds);
                        let used_columns = resolved_index_prefix.iter().take_while(|(column, _)| {
                            fixed.contains(&column.unique_id)
                                || runtime.inner_join_keys.iter().any(|key| key.unique_id == column.unique_id)
                        }).filter_map(|(column, length)| {
                            ((*length == tidb_datatype::UNSPECIFIED_LENGTH
                                || column.ret_type.as_ref().is_some_and(|ty| ty.flen() == *length))
                                && runtime.inner_join_keys.iter().any(|key| key.unique_id == column.unique_id))
                                .then_some(column.unique_id)
                        }).collect::<Vec<_>>();
                        table_stats.as_ref().and_then(|stats| {
                            let ndv = stats.hist_coll()?.ndv_lower_bound(&used_columns)?;
                            (ndv > 0).then(|| stats.row_count() / ndv as f64)
                        }).filter(|bound| *bound > 0.0)
                    } else {
                        None
                    };
                    let max_one_row = index_join_path_is_max_one_row(ds, path, runtime);
                    let cap = |count: f64| {
                        let count = upper_bound.map_or(count, |bound| count.min(bound));
                        if max_one_row { count.min(1.0) } else { count }
                    };
                    let selectivity = |filters: &[tidb_expr::expression::Expression]| {
                        if filters.is_empty() {
                            return 1.0;
                        }
                        table_stats.as_ref().and_then(|stats| {
                            crate::logical::rewrite::analyzed_filter_selectivity_in(
                                stats, filters, &ctx.access_path_derivation_context(),
                            )
                        }).filter(|ratio| *ratio > 0.0)
                            .unwrap_or(crate::cost_factors::SELECTION_FACTOR)
                    };
                    let final_rows = cap(runtime.avg_inner_row_count);
                    let mut index_rows = cap(final_rows / selectivity(&table_filters));
                    let mut access_rows = cap(index_rows / selectivity(&index_filters));
                    // The access floor runs after the upper bounds and retains
                    // the residual index-filter ratio, exactly as Go does.
                    if !max_one_row {
                        if let Some(floor) = probe_access_rows_floor.filter(|floor| *floor > access_rows) {
                            let ratio = if access_rows > 0.0 { index_rows / access_rows } else { 1.0 };
                            access_rows = floor;
                            index_rows = floor * ratio;
                        }
                    }
                    let scale = |rows| table_stats.as_ref()
                        .map(|stats| stats.scale_by_expect_cnt(rows, ctx.skew_ratio))
                        .unwrap_or_else(|| crate::stats_info::StatsInfo::new(rows, []));
                    (scale(final_rows), scale(index_rows), scale(access_rows))
                });
                let runtime_final_stats = runtime_counts.as_ref().map(|counts| counts.0.clone());
                let runtime_probe_stats = runtime_counts.as_ref().map(|counts| counts.1.clone());
                let scan_stats = runtime_counts.as_ref().map(|counts| counts.2.clone()).or(stats.clone());
                base.base.set_stats(scan_stats.clone());
                let mut cost_columns = source_index
                    .columns
                    .iter()
                    .filter_map(|column| ds.table_columns.get(column.offset).cloned())
                    .collect::<Vec<_>>();
                // Go `convertToIndexScan` appends the table handle columns to
                // the physical index schema even when the same logical
                // columns already occur in the secondary index.  Cost model
                // v2 prices that physical schema verbatim: for a four-column
                // secondary index over a three-column common handle this is
                // seven INT slots, not the four-column set-union.  Retaining
                // the duplicates is therefore both an execution-schema and
                // a plan-cost contract, not an index-width heuristic.
                cost_columns.extend(ds.common_handle_cols.iter().cloned());
                // Go `InitSchema` (`physical_index_scan.go:363`) appends the
                // handle only when the index schema does not already carry
                // one (`setHandle`). A common handle was just appended, and
                // this port keeps `handle_cols` as the SAME columns as
                // `common_handle_cols` for such a table, so appending both
                // would price three extra INT slots and flip the range-path
                // choice back to the table scan.
                if ds.common_handle_cols.is_empty() {
                    cost_columns.extend(ds.handle_cols.iter().cloned());
                }
                let scan = PhysicalPlan::IndexScan(crate::physical::PhysicalIndexScan {
                    base,
                    table_id: ds.physical_table_id,
                    table_as_name: ds.table_as_name.clone(),
                    dynamic_partition_access: ds.dynamic_partition_access.clone(),
                    cost_columns,
                    data_source_schema: ds.base.base.schema().cloned().map(Box::new),
                    index_id: source_index.id,
                    index_name: source_index.name.clone(),
                    keep_order,
                    desc,
                    ranges: ranges.clone(),
                    range_rebuild: index_range_rebuild,
                    covering_ranges: Vec::new(),
                    tikv_pushdown: None,
                });
                let (table_filters, mut virtual_table_filters) =
                    crate::pushdown::split_virtual_column_filters(table_filters);
                let (index_filters, mut rejected_index_filters) = crate::pushdown::split_scan_filters(
                    index_filters, tidb_expr::infer_pushdown::PushDownStore::TiKv,
                    &ctx.expr_pushdown_blacklist,
                );
                let (table_filters, rejected_table_filters) = if single_scan {
                    (Vec::new(), table_filters)
                } else {
                    crate::pushdown::split_scan_filters(table_filters,
                        tidb_expr::infer_pushdown::PushDownStore::TiKv, &ctx.expr_pushdown_blacklist)
                };
                let has_root_filters = !virtual_table_filters.is_empty()
                    || !rejected_index_filters.is_empty() || !rejected_table_filters.is_empty();
                let (mut table_side, mut root_task_conds) = if single_scan {
                    // A condition that is not covered by the index key (for
                    // example an unsigned integer handle predicate) remains
                    // above the covering IndexReader as Go's
                    // `CopTask.RootTaskConds`; it must never disappear merely
                    // because no table probe is required.
                    (None, rejected_table_filters.clone())
                } else {
                    // Go `convertToIndexScan` builds the lookup's table side
                    // over the source's schema and stats.
                    let mut table_base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator,
                        "TableScan",
                        ds.base.base.query_block_offset(),
                    );
                    table_base
                        .base
                        .set_stats(runtime_probe_stats.clone().or_else(|| {
                            scan.base().base.stats_info().cloned().map(|stats| {
                                // Go `convertToIndexScan`:
                                // `ts.SetStats(&property.StatsInfo{StatsVersion:
                                // ds.TableStats.StatsVersion})` — the table
                                // side carries the TABLE stats' VERSION (the
                                // row counts are filled from the index plan
                                // in `(*copTask).finishIndexPlan`). Inheriting
                                // the cop Limit's fresh stats verbatim left
                                // the pseudo version on an analyzed table.
                                let version = ds
                                    .table_stats
                                    .as_ref()
                                    .map(|table_stats| table_stats.stats_version())
                                    .or_else(|| {
                                        ds.base
                                            .base
                                            .stats_info()
                                            .map(|ds_stats| ds_stats.stats_version())
                                    });
                                if let Some(version) = version {
                                    stats.with_stats_version(version)
                                } else {
                                    stats
                                }
                            })
                        }));
                    table_base.base.set_schema(ds.base.base.schema().cloned());
                    let table_scan = PhysicalPlan::TableScan(crate::physical::PhysicalTableScan {
                        base: table_base,
                        table_id: ds.physical_table_id,
                        table_as_name: ds.table_as_name.clone(),
                        dynamic_partition_access: ds.dynamic_partition_access.clone(),
                        cost_columns: ds.table_columns.clone(),
                        store_type: crate::physical_table_reader::StoreType::TiKv,
                        keep_order: false,
                        desc: false,
                        // The lookup's table side reads BY HANDLE from
                        // the index rows, not by its own ranges.
                        ranges: crate::ranger::types::Ranges::new(),
                        range_rebuild: None,
                        table_scan_penalty: ds.table_scan_penalty,
                        tikv_pushdown: None,
                        resolved_descriptor: Some(
                            crate::access_path::ResolvedTableDescriptor::new(
                                ds.physical_table_id,
                                !ds.common_handle_cols.is_empty(),
                                crate::access_path::ResolvedTableScanKind::RowId,
                                crate::access_path::TableScanExplainIdSuffix::IncludePlanId,
                            ),
                        ),
                    });
                    (Some(Box::new(table_scan)), rejected_table_filters)
                };
                // Go records virtual table predicates first, then rejected
                // index predicates, then the remaining rejected table predicates.
                virtual_table_filters.append(&mut rejected_index_filters);
                virtual_table_filters.extend(root_task_conds);
                root_task_conds = virtual_table_filters;
                let index_plan = if index_filters.is_empty() {
                    scan
                } else {
                    let mut selection_base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator,
                        "Selection",
                        ds.base.base.query_block_offset(),
                    );
                    selection_base
                        .base
                        .set_schema(scan.schema().cloned());
                    selection_base.base.set_stats(
                        if let Some(runtime_stats) = &runtime_probe_stats {
                            Some(runtime_stats.clone())
                        } else if let Some(count_after_index) =
                            filled_path.and_then(|path| path.count_after_index)
                        {
                            let access_rows = ds
                                .derived_index_paths
                                .get(&source_index.id)
                                .and_then(|path| path.count_after_access())
                                .unwrap_or(0.0);
                            let selectivity = if access_rows > 0.0 {
                                count_after_index / access_rows
                            } else {
                                0.0
                            };
                            scan_stats
                                .as_ref()
                                .map(|stats| stats.scale(selectivity, ctx.skew_ratio))
                        } else if table_filters.is_empty() {
                            // Go `addPushedDownSelection4PhysicalIndexScan`
                            // (`find_best_task.go:2762`): count =
                            // is.StatsInfo().RowCount x (path.CountAfterIndex
                            // / path.CountAfterAccess), then
                            // `ds.TableStats.ScaleByExpectCnt(count)`. The
                            // ratio IS the histogram selectivity of the index
                            // filters, so derive it from the analyzed profile
                            // instead of keeping the raw DataSource profile.
                            let selectivity = ds
                                .table_stats
                                .as_ref()
                                .and_then(|table_stats| {
                                    crate::logical::rewrite::analyzed_filter_selectivity(
                                        table_stats,
                                        &index_filters,
                                    )
                                })
                                .filter(|value| *value > 0.0);
                            let count = scan_stats
                                .as_ref()
                                .map(crate::stats_info::StatsInfo::row_count)
                                .zip(selectivity)
                                .map(|(rows, selectivity)| rows * selectivity);
                            match count {
                                Some(count) => ds
                                    .table_stats
                                    .as_ref()
                                    .map(|table_stats| {
                                        table_stats.scale_by_expect_cnt(
                                            count,
                                            ctx.skew_ratio,
                                        )
                                    })
                                    .or_else(|| {
                                        Some(crate::stats_info::StatsInfo::new(count, []))
                                    }),
                                None => stats.clone(),
                            }
                        } else {
                            stats.clone()
                        },
                    );
                    selection_base.set_children(vec![scan]);
                    PhysicalPlan::Selection(crate::physical::PhysicalSelection {
                        base: selection_base,
                        conditions: index_filters,
                        from_data_source: true,
                    })
                };
                // Go `convertToIndexScan` (`find_best_task.go:2665`): a
                // keep-order read on a non-common-handle table appends the
                // extra handle column to the table side
                // (`AppendExtraHandleCol`, isNew) and projects it back away
                // above the reader — `cop.NeedExtraProj = true` with
                // `cop.OriginSchema = ds.Schema()`. The common-handle
                // missing-handle-column append (`:2652`) is a no-op here:
                // this tier's table side already carries the full schema.
                // Without the appended column the reader schema equals the
                // origin schema, so Go's postOptimize
                // eliminatePhysicalProjection would erase the projection
                // `BuildIndexLookUpTask` builds (the reader's schema is the
                // table side's, `physical_indexlookup_reader.go:205`).
                let extra_handle_proj =
                    keep_order && table_side.is_some() && ds.common_handle_cols.is_empty()
                        && ds.handle_cols.is_empty();
                if extra_handle_proj {
                    if let Some(table_plan) = table_side.as_deref_mut() {
                        if let Some(schema_shared) = table_plan.base_mut().base.schema_shared() {
                            let mut schema = schema_shared.as_ref().clone();
                            let next_id = schema
                                .columns
                                .iter()
                                .map(|column| column.unique_id)
                                .max()
                                .unwrap_or(0)
                                + 1;
                            let mut extra = tidb_expr::column::Column::new(
                                ctx.column_ids.map(|ids| ids.alloc()).unwrap_or(next_id),
                                tidb_datatype::FieldType::new(
                                    tidb_datatype::FieldTypeCode::LongLong,
                                )
                                .with_flags(
                                    tidb_datatype::FieldTypeFlags::NOT_NULL
                                        | tidb_datatype::FieldTypeFlags::PRI_KEY,
                                ),
                            );
                            extra.id = crate::logical::data_source::EXTRA_HANDLE_ID;
                            extra.orig_name = "_tidb_rowid".to_owned();
                            schema.columns.push(extra);
                            table_plan.base_mut().base.set_schema(Some(schema));
                        }
                    }
                }
                let mut cop = crate::task::CopTask {
                    index_plan: Some(Box::new(index_plan)),
                    table_plan: table_side,
                    need_extra_proj: extra_handle_proj,
                    origin_schema: extra_handle_proj
                        .then(|| ds.base.base.schema().cloned())
                        .flatten(),
                    common_handle_cols: ds.common_handle_cols.clone(),
                    index_lookup_push_down_by: if single_scan {
                        crate::access_path::IndexLookupPushDownBy::None
                    } else {
                        ds.index_lookup_push_down_by
                            .get(&source_index.id)
                            .copied()
                            .unwrap_or(crate::access_path::IndexLookupPushDownBy::None)
                    },
                    stats_context: ctx.task_stats_context(ds, &root_task_conds),
                    root_task_conds,
                    index_plan_finished: false,
                    keep_order,
                    expect_cnt: prop.expected_cnt as u64,
                    index_join_info: match &prop.index_join_prop {
                        Some(runtime) => Some(match index_join_feedback(ds, path, runtime, ctx) {
                            Some(info) => info,
                            None => continue 'paths,
                        }),
                        None => None,
                    },
                    partial_order_match_result: partial_order_match,
                    ..crate::task::CopTask::default()
                };
                // Go finishes the index phase before building the table
                // Selection: its input rows are the completed index plan's
                // output, including any retained index-side filters.
                if !single_scan && !table_filters.is_empty() {
                    cop.finish_index_plan();
                    let table_scan = cop.table_plan.take().expect("lookup has a table scan");
                    let mut selection_base = crate::physical::BasePhysicalPlan::new(
                        ctx.allocator, "Selection", ds.base.base.query_block_offset(),
                    );
                    selection_base.base.set_schema(table_scan.schema().cloned());
                    selection_base.base.set_stats(if has_root_filters {
                        ctx.pushed_filter_stats(ds, table_scan.stats_info(), &table_filters)
                    } else {
                        runtime_final_stats.clone().or_else(|| {
                            ds.base.base.stats_info().map(|stats| {
                                stats.scale_by_expect_cnt(prop.expected_cnt, ctx.skew_ratio)
                            })
                        })
                    });
                    selection_base.set_children(vec![*table_scan]);
                    cop.table_plan = Some(Box::new(PhysicalPlan::Selection(
                        crate::physical::PhysicalSelection {
                            base: selection_base,
                            conditions: table_filters,
                            from_data_source: true,
                        },
                    )));
                }
                Task::Cop(cop)
            }
        };
        let cur = if cop_answer {
            cop
        } else {
            cop.convert_to_root_task_in(ctx.allocator, ctx.expression_evaluator)?
        };
        if prop.index_join_prop.is_none() {
            ordinary_candidates.push((
                cur,
                candidate_metrics,
                cur_preferred_range,
                cur_is_full_range,
                heuristic,
            ));
            continue;
        }
        let skyline_choice = if prop.index_join_prop.is_some() {
            index_join_skyline_prefers_current(
                index_join_skyline_count,
                best_index_join_skyline_count,
                prop.expected_cnt,
                ctx.index_join_skyline_threshold,
            )
        } else {
            None
        };
        let current_wins = match skyline_choice {
            Some(wins) => wins,
            None => best.invalid() || compare_task_cost(ctx.coster, &cur, &best)?,
        };
        let better_than_preferred_range = cur_preferred_range
            && best_preferred_range.as_ref().is_none_or(|best_range| {
                compare_task_cost(ctx.coster, &cur, best_range).unwrap_or(false)
            });
        if current_wins {
            best_is_preferred_range = cur_preferred_range;
            best_is_full_range = cur_is_full_range;
            if better_than_preferred_range {
                best_preferred_range = Some(cur.clone());
            }
            best = cur;
            best_index_join_skyline_count = index_join_skyline_count;
        } else if better_than_preferred_range {
            best_preferred_range = Some(cur);
        }
    }
    // The unordered root enumeration contains all candidates. Ordered and
    // runtime join enumerations may omit paths, so cannot apply this rule here.
    if !prepared && !ordered && prop.task_tp == TaskType::Root && prop.index_join_prop.is_none() {
        if let Some(paths) = ordinary_candidates
            .iter()
            .map(|candidate| candidate.4.clone())
            .collect::<Option<Vec<_>>>()
        {
            if let Some(selected) = crate::find_best_task::candidate::choose_heuristic_path(&paths)
            {
                return Ok(ordinary_candidates.swap_remove(selected).0);
            }
        }
    }
    let mut skyline_candidates = Vec::new();
    let mut idx_missing_stats = prepared_missing_stats;
    for candidate in ordinary_candidates {
        if prepared {
            skyline_candidates.push(candidate);
            continue;
        }
        let (_, missing_stats, _) = crate::find_best_task::candidate::insert_skyline_candidate(
            &mut skyline_candidates,
            candidate,
            |candidate| candidate.1.as_ref(),
            table_pseudo,
            prop.expected_cnt,
            ctx.prefer_range_scan,
            ctx.index_join_skyline_threshold,
        );
        idx_missing_stats |= missing_stats;
    }
    for (cur, _, preferred, full_range, _) in skyline_candidates {
        let better_range = if preferred {
            match best_preferred_range.as_ref() {
                Some(best_range) => compare_task_cost(ctx.coster, &cur, best_range)?,
                None => true,
            }
        } else {
            false
        };
        if better_range {
            best_preferred_range = Some(cur.clone());
        }
        if best.invalid() || compare_task_cost(ctx.coster, &cur, &best)? {
            best = cur;
            best_is_preferred_range = preferred;
            best_is_full_range = full_range;
        }
    }
    if heuristic_selected {
        return Ok(best);
    }
    // All alternative choices were fixed before ordinary physical construction.
    for candidate in &merge_candidates {
        let mut merge_task = match candidate {
            super::candidate_preparation::PreparedMerge::Union(path) =>
                super::index_merge_union::build_converged_union_index_merge_task(ds, path, ctx)?,
            super::candidate_preparation::PreparedMerge::Intersection(path) =>
                super::index_merge_intersection::build_prepared_intersection_index_merge_task(ds, path, ctx)?,
        };
        if prop.task_tp == TaskType::Root {
            if let Task::Cop(cop) = &mut merge_task {
                cop.index_plan_finished = true;
            }
            merge_task = merge_task.into_root_task_in(ctx.allocator, ctx.expression_evaluator)?;
        }
        if best.invalid() || compare_task_cost(ctx.coster, &merge_task, &best)? {
            best = merge_task;
            best_is_full_range = false;
        }
    }
    // Go keeps preferRange enabled only when the skyline saw an unanalyzed
    // index winner, the table statistics are pseudo, or the table is empty.
    // This decision comes after candidate comparisons; making it before
    // enumeration misses idxMissingStats for a new index on analyzed columns.
    let prefer_range = ctx.prefer_range_scan
        && prop.index_join_prop.is_none()
        && (idx_missing_stats
            || table_pseudo
            || ds
                .table_stats
                .as_ref()
                .or_else(|| ds.base.base.stats_info())
                .is_none_or(|stats| stats.row_count() < 1.0));
    if prefer_range && best_is_full_range {
        if let Some(range_task) = best_preferred_range {
            if !best_is_preferred_range {
                best = range_task;
            }
        }
    }
    Ok(best)
}

#[derive(Default)]
struct EnumerateState {
    topn_cop_exists: bool,
    limit_cop_exists: bool,
}

/// Go `enumeratePhysicalPlans4Task` + helper (`find_best_task.go:112,156`):
/// preserve preference slices, hint priority, and the normal cop preference
/// for pushed TopN/Limit candidates while comparing peers by cost.
fn enumerate_physical_plans_4_task(
    plan: &LogicalPlan,
    physical_plans_slice: &[Vec<PhysicalPlan>],
    prop: &PhysicalProperty,
    add_enforcer: bool,
    ctx: &mut DispatchContext<'_>,
) -> Result<(Task, bool), PlanError> {
    if physical_plans_slice.is_empty() {
        return Ok((Task::invalid_task(), false));
    }
    let mut outer_normal_task = Task::invalid_task();
    let mut outer_hint_task = Task::invalid_task();
    for ops in physical_plans_slice {
        let mut normal_iter_task = Task::invalid_task();
        let mut normal_prefer_task = Task::invalid_task();
        let mut hint_task = Task::invalid_task();
        let mut state = EnumerateState::default();
        for pp in ops {
            let child_len = plan.children().len();
            let mut child_tasks = Vec::with_capacity(child_len);
            for (i, child) in plan.children().iter().enumerate() {
                let Some(child_prop) = pp.base().child_req_prop(i) else {
                    break;
                };
                let child_prop = child_prop.clone();
                let child_task = find_best_task(child, &child_prop, ctx)?;
                if child_task.invalid() || !task_type_satisfied(&child_prop, &child_task) {
                    break;
                }
                child_tasks.push(child_task);
            }
            // "This check makes sure that there is no invalid child task."
            if child_tasks.len() != child_len {
                continue;
            }
            let child_is_cop = matches!(child_tasks.first(), Some(Task::Cop(_)));
            let child_is_root = matches!(child_tasks.first(), Some(Task::Root(_)));
            let child_is_mpp = matches!(child_tasks.first(), Some(Task::Mpp(_)));
            // Merge hints have already restricted the datasource candidates.
            // Promoting only root readers here would discard a hinted merge's
            // cop task before its parent can push LIMIT into the partials.
            let hint_applicable = logical_hint_applies(plan, pp, child_is_cop);
            let normally_preferred = normal_preference_applies(
                plan,
                pp,
                child_is_cop,
                child_is_root,
                child_is_mpp,
                ctx.limit_push_down_threshold,
                &mut state,
            );
            let mut cur_task = match crate::task::attach2_task_in(
                pp.clone_shallow(),
                child_tasks,
                ctx.column_ids,
                ctx.allocator,
                ctx.group_ndv_skew_ratio,
                ctx.expression_evaluator,
                &ctx.expr_pushdown_blacklist,
            ) {
                Ok(task) => task,
                // An unported attach body refuses; Go has no such arm, so a
                // refusal must SURFACE rather than silently skip a
                // candidate Go would have priced.
                Err(error) => return Err(error),
            };
            if cur_task.invalid() {
                continue;
            }
            if !matches!(cur_task, Task::Root(_)) && prop.task_tp == TaskType::Root {
                cur_task = cur_task.convert_to_root_task_in(ctx.allocator, ctx.expression_evaluator)?;
            }
            if add_enforcer {
                cur_task = enforce_property_in(prop, cur_task, ctx.allocator, ctx.expression_evaluator)?;
            }
            // A column-only NominalSort returns its ordered child task directly.
            // Rewriting that child here would invalidate the ORDER BY contract
            // whose enforcement the nominal node has just discharged.
            if !matches!(pp, PhysicalPlan::NominalSort(_))
                && !matches!(cur_task, Task::Mpp(_))
                && prop.is_sort_item_empty()
            {
                cur_task = crate::physical::shuffle_optimize::optimize_by_shuffle_in(
                    cur_task,
                    ctx.shuffle_options,
                    ctx.allocator,
                    ctx.expression_evaluator,
                )?;
            }
            if hint_applicable {
                if hint_task.invalid() || compare_task_cost(ctx.coster, &cur_task, &hint_task)? {
                    hint_task = cur_task;
                }
            } else if hint_task.invalid() && normally_preferred {
                if normal_prefer_task.invalid()
                    || compare_task_cost(ctx.coster, &cur_task, &normal_prefer_task)?
                {
                    normal_prefer_task = cur_task;
                }
            } else if hint_task.invalid() && normal_prefer_task.invalid() {
                if normal_iter_task.invalid()
                    || compare_task_cost(ctx.coster, &cur_task, &normal_iter_task)?
                {
                    normal_iter_task = cur_task;
                }
            }
        }
        let (slice_task, slice_is_hint) = if !hint_task.invalid() {
            (hint_task, true)
        } else if !normal_prefer_task.invalid() {
            (normal_prefer_task, false)
        } else {
            (normal_iter_task, false)
        };
        if slice_is_hint {
            if outer_hint_task.invalid()
                || compare_task_cost(ctx.coster, &slice_task, &outer_hint_task)?
            {
                outer_hint_task = slice_task;
            }
        } else if outer_normal_task.invalid()
            || compare_task_cost(ctx.coster, &slice_task, &outer_normal_task)?
        {
            outer_normal_task = slice_task;
        }
    }
    if outer_hint_task.invalid() {
        Ok((outer_normal_task, false))
    } else {
        Ok((outer_hint_task, true))
    }
}

fn task_type_satisfied(required: &PhysicalProperty, task: &Task) -> bool {
    match required.task_tp {
        TaskType::Root => matches!(task, Task::Root(_) | Task::Cop(_) | Task::Mpp(_)),
        TaskType::CopSingleRead | TaskType::CopMultiRead => matches!(task, Task::Cop(_)),
        TaskType::Mpp => matches!(task, Task::Mpp(_)),
        TaskType::Unknown(_) => false,
    }
}

fn logical_hint_applies(plan: &LogicalPlan, physical: &PhysicalPlan, child_is_cop: bool) -> bool {
    if logical_join_hint_applies(plan, physical) {
        return true;
    }
    match plan {
        LogicalPlan::TopN(topn) => topn.prefer_limit_to_cop && child_is_cop,
        LogicalPlan::Limit(limit) => limit.prefer_limit_to_cop && child_is_cop,
        LogicalPlan::Aggregation(aggregation) => aggregation.prefer_agg_to_cop && child_is_cop,
        _ => false,
    }
}

/// Whether an index-join candidate satisfies one of the requested Go force
/// hints.  This is deliberately separate from `logical_hint_applies`: the
/// latter marks a built candidate as preferred, while this gate prevents an
/// opposite-side candidate from becoming the normal-cost fallback when the
/// requested family is unavailable.
fn index_join_candidate_matches_hint(
    join: &crate::logical::LogicalJoin,
    strategy: &crate::find_best_task::JoinStrategy,
) -> bool {
    let crate::find_best_task::JoinStrategy::Index {
        outer_idx, kind, ..
    } = strategy
    else {
        return true;
    };
    let inner_is_left = *outer_idx == 1;
    use crate::plan_builder::from::join_hint_flags as hint;
    match kind {
        crate::plan_cost_ver2::IndexJoinKind::IndexJoin => {
            (inner_is_left && join.prefer_any(&[hint::LEFT_AS_INLJ_INNER]))
                || (!inner_is_left && join.prefer_any(&[hint::RIGHT_AS_INLJ_INNER]))
        }
        crate::plan_cost_ver2::IndexJoinKind::IndexHashJoin => {
            (inner_is_left && join.prefer_any(&[hint::LEFT_AS_INLHJ_INNER]))
                || (!inner_is_left && join.prefer_any(&[hint::RIGHT_AS_INLHJ_INNER]))
        }
        crate::plan_cost_ver2::IndexJoinKind::IndexMergeJoin => {
            (inner_is_left && join.prefer_any(&[hint::LEFT_AS_INLMJ_INNER]))
                || (!inner_is_left && join.prefer_any(&[hint::RIGHT_AS_INLMJ_INNER]))
        }
    }
}

fn normal_preference_applies(
    plan: &LogicalPlan,
    physical: &PhysicalPlan,
    child_is_cop: bool,
    child_is_root: bool,
    child_is_mpp: bool,
    limit_push_down_threshold: u64,
    state: &mut EnumerateState,
) -> bool {
    let meets_threshold = match plan {
        LogicalPlan::Limit(_) => true,
        LogicalPlan::TopN(topn) => {
            matches!(physical, PhysicalPlan::Limit(_))
                || topn.count.saturating_add(topn.offset) <= limit_push_down_threshold
        }
        _ => false,
    };
    if !meets_threshold {
        return false;
    }
    let cop_exists = if matches!(physical, PhysicalPlan::TopN(_)) {
        &mut state.topn_cop_exists
    } else {
        &mut state.limit_cop_exists
    };
    if *cop_exists {
        return child_is_cop || child_is_mpp;
    }
    if child_is_cop {
        *cop_exists = true;
        return true;
    }
    child_is_root || child_is_mpp
}

/// Go `applyLogicalJoinHint`: a hint becomes applicable only after the
/// candidate and both child tasks have been built successfully.  A valid
/// hinted task outranks ordinary candidates regardless of cost; multiple
/// valid hinted tasks still compare by cost.
fn logical_join_hint_applies(plan: &LogicalPlan, physical: &PhysicalPlan) -> bool {
    use crate::plan_builder::from::join_hint_flags as hint;
    use crate::plan_cost_ver2::IndexJoinKind;

    let LogicalPlan::Join(join) = plan else {
        return false;
    };
    match physical {
        PhysicalPlan::MergeJoin(_) => join.prefer_any(&[hint::MERGE_JOIN]),
        PhysicalPlan::IndexJoin(index) => {
            let inner_is_left = index.inner_child_idx == 0;
            match index.kind {
                IndexJoinKind::IndexJoin => {
                    (inner_is_left && join.prefer_any(&[hint::LEFT_AS_INLJ_INNER]))
                        || (!inner_is_left && join.prefer_any(&[hint::RIGHT_AS_INLJ_INNER]))
                }
                IndexJoinKind::IndexHashJoin => {
                    (inner_is_left && join.prefer_any(&[hint::LEFT_AS_INLHJ_INNER]))
                        || (!inner_is_left && join.prefer_any(&[hint::RIGHT_AS_INLHJ_INNER]))
                }
                IndexJoinKind::IndexMergeJoin => {
                    (inner_is_left && join.prefer_any(&[hint::LEFT_AS_INLMJ_INNER]))
                        || (!inner_is_left && join.prefer_any(&[hint::RIGHT_AS_INLMJ_INNER]))
                }
            }
        }
        PhysicalPlan::HashJoin(hash) => {
            let mut force_left_to_build =
                join.prefer_any(&[hint::LEFT_AS_HJ_BUILD, hint::RIGHT_AS_HJ_PROBE]);
            let mut force_right_to_build =
                join.prefer_any(&[hint::RIGHT_AS_HJ_BUILD, hint::LEFT_AS_HJ_PROBE]);
            if force_left_to_build && force_right_to_build {
                force_left_to_build = false;
                force_right_to_build = false;
            }
            let hash_hint = join.prefer_any(&[hint::HASH_JOIN]);
            if hash_hint && !force_left_to_build && !force_right_to_build {
                return true;
            }
            (force_left_to_build && hash.inner_child_idx == 0)
                || (force_right_to_build && hash.inner_child_idx == 1)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    // Go's coverage for this loop is planner-integration bound (casetest
    // plans); these pin the transcreated control flow over a fixture coster.

    use super::*;
    use crate::logical::mem_table::MemTableColumn;
    use crate::logical::{
        BaseLogicalPlan, LogicalMaxOneRow, LogicalMemTable, LogicalSelection, LogicalTableDual,
    };
    use crate::physical_property::SortItem;
    use crate::stats_info::StatsInfo;
    use std::cell::RefCell;
    use tidb_datatype::{
        FieldName, FieldNameMetadata, FieldType, FieldTypeCode, IdentifierMetadata,
    };
    use tidb_expr::column::Column;
    use tidb_expr::schema::Schema;

    struct CountCoster;
    impl TaskCoster for CountCoster {
        fn task_cost(&self, task: &Task) -> Result<f64, PlanError> {
            fn count(plan: &PhysicalPlan) -> f64 {
                1.0 + plan.children().iter().map(count).sum::<f64>()
            }
            Ok(task.plan().map_or(f64::MAX, count))
        }
    }

    struct RecordingMppWarningSink(RefCell<Vec<String>>);

    impl MppWarningSink for RecordingMppWarningSink {
        fn raise_mpp_warning(&self, message: &str) {
            self.0.borrow_mut().push(message.to_owned());
        }
    }

    #[test]
    fn max_one_row_refusal_raises_the_source_warning_once_per_refusal() {
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let sink = RecordingMppWarningSink(RefCell::new(Vec::new()));
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0).with_mpp_warning_sink(&sink);
        let mut base = BaseLogicalPlan::new(&allocator, LogicalMaxOneRow::TYPE, 0);
        let mut dual_base = BaseLogicalPlan::new(&allocator, LogicalTableDual::TYPE, 0);
        dual_base.base.set_stats(Some(StatsInfo::new(1.0, [])));
        base.set_children(vec![LogicalPlan::TableDual(LogicalTableDual::new(
            dual_base, 1,
        ))]);
        let logical = LogicalPlan::MaxOneRow(LogicalMaxOneRow::new(base));

        let ordered = PhysicalProperty::new(TaskType::Root, &[1], false, f64::MAX, false);
        assert!(
            find_best_task(&logical, &ordered, &mut ctx)
                .expect("ordered refusal is a valid search result")
                .invalid()
        );
        assert_eq!(
            sink.0.borrow().as_slice(),
            ["MPP mode may be blocked because operator `MaxOneRow` is not supported now."]
        );

        let mpp = PhysicalProperty {
            task_tp: TaskType::Mpp,
            ..PhysicalProperty::default()
        };
        assert!(
            find_best_task(&logical, &mpp, &mut ctx)
                .expect("MPP refusal is a valid search result")
                .invalid()
        );
        assert_eq!(sink.0.borrow().len(), 2);

        let cop = PhysicalProperty {
            task_tp: TaskType::CopSingleRead,
            ..PhysicalProperty::default()
        };
        assert!(
            find_best_task(&logical, &cop, &mut ctx)
                .expect("cop refusal is a valid search result")
                .invalid()
        );
        assert_eq!(sink.0.borrow().len(), 3);

        let root = find_best_task(&logical, &PhysicalProperty::default(), &mut ctx)
            .expect("root MaxOneRow remains supported");
        assert!(!root.invalid());
        assert_eq!(sink.0.borrow().len(), 3);
    }

    fn dual(allocator: &PlanIdAllocator, rows: f64) -> LogicalPlan {
        let mut base = BaseLogicalPlan::new(allocator, LogicalTableDual::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        LogicalPlan::TableDual(LogicalTableDual::new(base, 1))
    }

    #[test]
    fn a_mem_table_physical_plan_keeps_declared_output_names() {
        // Go's `buildMemTable` carries the information-schema field names from
        // the logical source into the physical scan. Losing them makes a
        // wildcard query expose synthetic `Column#N` headers.
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let mut base = BaseLogicalPlan::new(&allocator, LogicalMemTable::TYPE, 0);
        let names = ["GRANTEE", "TABLE_SCHEMA"]
            .into_iter()
            .map(|name| {
                FieldName::new(FieldNameMetadata {
                    column: IdentifierMetadata::new(name),
                    ..FieldNameMetadata::default()
                })
            })
            .collect::<Vec<_>>();
        base.base.set_schema(Some(Schema::new(vec![
            Column::new(1, FieldType::new(FieldTypeCode::LongLong)),
            Column::new(2, FieldType::new(FieldTypeCode::LongLong)),
        ])));
        base.base.set_output_names(names.clone());
        let mut mem = LogicalMemTable::new(base, "information_schema", "SCHEMA_PRIVILEGES");
        mem.columns = vec![
            MemTableColumn {
                id: 1,
                name: "GRANTEE".to_owned(),
            },
            MemTableColumn {
                id: 2,
                name: "TABLE_SCHEMA".to_owned(),
            },
        ];
        let logical = LogicalPlan::MemTable(mem);

        let task = find_best_task(&logical, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let Some(PhysicalPlan::MemTable(physical)) = task.plan() else {
            panic!("expected a physical memory table");
        };
        assert_eq!(physical.base.base.output_names(), names);
    }

    #[test]
    fn a_selection_over_a_dual_plans_end_to_end() {
        // The dispatcher's whole loop on the smallest real tree: exhaust the
        // selection, recurse into the dual (its own findBestTask override),
        // attach, and answer a root task.
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(10.0, [])));
        base.set_children(vec![dual(&allocator, 10.0)]);
        let selection = LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()));

        let task =
            find_best_task(&selection, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Selection(_)));
        assert!(matches!(
            plan.children().first(),
            Some(PhysicalPlan::TableDual(_))
        ));
    }

    #[test]
    fn a_one_row_dual_satisfies_the_order_without_a_sort() {
        // The fits-prop pass wins WITHOUT an enforcer: a Selection passes
        // the required order down (`CloneEssentialFields` keeps SortItems)
        // and a 1-row dual satisfies any order vacuously
        // (`findBestTask4LogicalTableDual`), so Go's cheaper sort-free plan
        // is the answer even with `CanAddEnforcer` set.
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(10.0, [])));
        base.set_children(vec![dual(&allocator, 1.0)]);
        let selection = LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()));

        let prop = PhysicalProperty {
            sort_items: vec![SortItem::new(7, false)],
            can_add_enforcer: true,
            ..PhysicalProperty::default()
        };
        let task = find_best_task(&selection, &prop, &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(
            matches!(plan, PhysicalPlan::Selection(_)),
            "no Sort: the order rode down to the 1-row dual, got {plan:?}"
        );
    }

    #[test]
    fn an_unsatisfiable_order_is_enforced_with_a_sort_on_top() {
        // A CTE table refuses EVERY required order
        // (`findBestTask4LogicalCTETable`), so the fits-prop pass dies at
        // the child and the ENFORCED branch — empty-property re-exhaust
        // plus `EnforceProperty` — produces the Sort that wins by validity.
        use crate::logical::LogicalCTETable;
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let cte = {
            let mut base = BaseLogicalPlan::new(&allocator, LogicalCTETable::TYPE, 0);
            base.base.set_stats(Some(StatsInfo::new(4.0, [])));
            LogicalPlan::CTETable(LogicalCTETable {
                base,
                seed_stat: None,
                name: "c".to_owned(),
                id_for_storage: 1,
                seed_schema: None,
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(4.0, [])));
        base.set_children(vec![cte]);
        let selection = LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()));

        let prop = PhysicalProperty {
            sort_items: vec![SortItem::new(7, false)],
            can_add_enforcer: true,
            ..PhysicalProperty::default()
        };
        let task = find_best_task(&selection, &prop, &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(
            matches!(plan, PhysicalPlan::Sort(_)),
            "the enforcer Sort tops the plan, got {plan:?}"
        );
        assert!(matches!(
            plan.children().first(),
            Some(PhysicalPlan::Selection(_))
        ));
        assert!(matches!(
            plan.children()[0].children().first(),
            Some(PhysicalPlan::CTETable(_))
        ));
    }

    #[test]
    fn a_data_source_plans_a_table_reader_through_the_dispatcher() {
        // The table-path slice end to end: DataSource -> TableScan in a cop
        // task -> convertToRootTaskImpl's reader -> the Selection attaches
        // above it. The first table query plannable start to finish.
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(50.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 42,
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(10.0, [])));
        base.set_children(vec![source]);
        let selection = LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()));

        let task =
            find_best_task(&selection, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Selection(_)));
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!(
                "a TableReader under the selection, got {:?}",
                plan.children()
            );
        };
        let Some(PhysicalPlan::TableScan(scan)) = reader.table_plan.as_deref() else {
            panic!("the scan hangs off TablePlan");
        };
        assert_eq!(scan.table_id, 42);
    }

    fn apply_candidate_with_outer_stats(
        outer_ndv: f64,
        apply_rows: f64,
        ordered: bool,
    ) -> physical::PhysicalApply {
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let ctx = DispatchContext::new(&allocator, &coster, 1.0)
            .with_apply_cache_capacity(1024)
            .with_ordering_index_selectivity_ratio(0.5);
        let column = tidb_expr::column::Column::new(
            1, tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let mut outer = dual(&allocator, 100.0);
        outer.base_mut().base.set_schema(Some(tidb_expr::schema::Schema::new(vec![column.clone()])));
        outer.base_mut().base.set_stats(Some(StatsInfo::new(100.0, [(1, outer_ndv)])));
        let mut base = BaseLogicalPlan::new(&allocator, "Apply", 0);
        base.base.set_schema(outer.schema().cloned());
        base.base.set_stats(Some(StatsInfo::new(apply_rows, [(1, outer_ndv)])));
        base.set_children(vec![outer, dual(&allocator, 10.0)]);
        let mut apply = crate::logical::LogicalApply::new(base, crate::find_best_task::LogicalJoinType::Inner);
        apply.is_lateral = true;
        apply.cor_cols = vec![tidb_expr::column::CorrelatedColumn::new(column)];
        let prop = PhysicalProperty {
            expected_cnt: 5.0,
            sort_items: if ordered { vec![SortItem::new(1, false)] } else { vec![] },
            ..Default::default()
        };
        let (mut candidates, _) = exhaust_physical_plans(&LogicalPlan::Apply(apply), &prop, &ctx).unwrap();
        let PhysicalPlan::Apply(apply) = candidates.remove(0).remove(0) else { panic!("Apply candidate") };
        apply
    }

    #[test]
    fn apply_cache_eligibility_uses_outer_input_not_apply_output() {
        assert!(!apply_candidate_with_outer_stats(100.0, 1000.0, true).can_use_cache);
        assert!(apply_candidate_with_outer_stats(70.0, 10.0, true).can_use_cache);
    }

    #[test]
    fn unordered_apply_keeps_outer_expected_count_unbounded() {
        let unordered = apply_candidate_with_outer_stats(70.0, 10.0, false);
        let ordered = apply_candidate_with_outer_stats(70.0, 10.0, true);
        assert_eq!(unordered.hash_join.base.child_req_prop(0).unwrap().expected_cnt, f64::MAX);
        assert_eq!(ordered.hash_join.base.child_req_prop(0).unwrap().expected_cnt, 95.0);
    }

    #[test]
    fn a_hash_join_candidate_retains_the_sessions_concurrency() {
        // Go `NewPhysicalHashJoin` copies
        // `SessionVars.HashJoinConcurrency()` onto every candidate, and the
        // plan cost later reads the candidate field. This must not fall back
        // to the default after the shared physical tree is cached or cloned.
        use crate::logical::LogicalJoin;

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0).with_hash_join_concurrency(13);
        let mut base = BaseLogicalPlan::new(&allocator, LogicalJoin::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(5.0, [])));
        base.set_children(vec![dual(&allocator, 10.0), dual(&allocator, 20.0)]);
        let join = LogicalPlan::Join(LogicalJoin {
            base,
            ..LogicalJoin::default()
        });

        let task = find_best_task(&join, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let Some(PhysicalPlan::HashJoin(hash_join)) = task.plan() else {
            panic!("a hash join candidate, got {:?}", task.plan());
        };
        assert_eq!(hash_join.concurrency, 13);
        let cloned = task.plan().expect("plan").clone_shallow();
        let PhysicalPlan::HashJoin(cloned) = cloned else {
            unreachable!();
        };
        assert_eq!(cloned.concurrency, 13, "cached-plan clones retain it");
    }

    #[test]
    fn index_join_probe_average_preserves_fractional_outer_rows() {
        use crate::logical::LogicalJoin;
        use tidb_expr::expression::Expression;
        use tidb_expr::scalar_function::ScalarFunction;

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let left = Column::new(1, FieldType::new(FieldTypeCode::LongLong));
        let right = Column::new(2, FieldType::new(FieldTypeCode::LongLong));
        for outer_rows in [0.8, 0.0] {
            let mut outer = dual(&allocator, outer_rows);
            outer
                .base_mut()
                .base
                .set_schema(Some(Schema::new(vec![left.clone()])));
            let mut inner = dual(&allocator, 8.0);
            inner
                .base_mut()
                .base
                .set_schema(Some(Schema::new(vec![right.clone()])));
            let mut base = BaseLogicalPlan::new(&allocator, LogicalJoin::TYPE, 0);
            base.base.set_stats(Some(StatsInfo::new(0.8, [])));
            base.set_children(vec![outer, inner]);
            let join = LogicalPlan::Join(LogicalJoin {
                base,
                equal_cond_out_cnt: 0.8,
                left_properties: vec![vec![left.clone()]],
                right_properties: vec![vec![right.clone()]],
                equal_conditions: vec![ScalarFunction::new(
                    tidb_ast::CiString::new("eq"),
                    FieldType::new(FieldTypeCode::Tiny),
                    vec![
                        Expression::Column(left.clone()),
                        Expression::Column(right.clone()),
                    ],
                )],
                ..LogicalJoin::default()
            });
            let (candidates, _) =
                exhaust_physical_plans(&join, &PhysicalProperty::default(), &ctx).unwrap();
            let mut checked = 0;
            for candidate in candidates.iter().flatten() {
                let Some(runtime) = candidate
                    .base()
                    .child_req_prop(1)
                    .and_then(|prop| prop.index_join_prop.as_ref())
                else {
                    continue;
                };
                assert_eq!(
                    runtime.avg_inner_row_count,
                    if outer_rows > 0.0 { 1.0 } else { 0.0 }
                );
                checked += 1;
            }
            assert!(checked > 0, "must inspect actual IndexJoin candidates");
        }
    }

    #[test]
    fn a_false_pushed_constant_short_circuits_into_a_dual() {
        // `tryToGetDualTask` (`find_best_task.go:749`): WHERE FALSE never
        // touches a path.
        use crate::logical::DataSource;
        use tidb_datatype::Datum;
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(50.0, [])));
            LogicalPlan::DataSource(DataSource {
                base,
                pushed_down_conds: vec![Expression::Constant(Constant::new(
                    Datum::Int(0),
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                ))],
                ..DataSource::default()
            })
        };
        let task =
            find_best_task(&source, &PhysicalProperty::default(), &mut ctx).expect("answers");
        assert!(
            matches!(task.plan(), Some(PhysicalPlan::TableDual(_))),
            "the dual short-circuit, got {:?}",
            task.plan()
        );
    }

    #[test]
    fn a_handle_ordered_requirement_admits_a_keep_order_scan() {
        // `matchProperty`'s int-handle arm (`find_best_task.go:1082`): ONE
        // sort item on the pk-is-handle column admits the scan with
        // KeepOrder (and Desc for a descending item); any other order
        // refuses.
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::column::Column;

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = |ctx_alloc: &PlanIdAllocator| {
            let mut base = BaseLogicalPlan::new(ctx_alloc, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(50.0, [])));
            LogicalPlan::DataSource(DataSource {
                base,
                pk_is_handle: true,
                handle_is_int: true,
                handle_cols: vec![Column::new(9, FieldType::new(FieldTypeCode::LongLong))],
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };

        // ORDER BY pk DESC: admitted, KeepOrder + Desc.
        let prop = PhysicalProperty::new(TaskType::Root, &[9], true, f64::MAX, false);
        let task = find_best_task(&source(&allocator), &prop, &mut ctx).expect("plans");
        let Some(PhysicalPlan::TableReader(reader)) = task.plan() else {
            panic!("a TableReader, got {:?}", task.plan());
        };
        let Some(PhysicalPlan::TableScan(scan)) = reader.table_plan.as_deref() else {
            panic!("the scan hangs off TablePlan");
        };
        assert!(scan.keep_order && scan.desc);

        // ORDER BY a non-handle column: refused.
        let prop = PhysicalProperty::new(TaskType::Root, &[1], false, f64::MAX, false);
        let task = find_best_task(&source(&allocator), &prop, &mut ctx).expect("answers");
        assert!(task.invalid());
    }

    #[test]
    fn read_from_storage_tiflash_selects_the_tiflash_table_path() {
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;
        use crate::logical::data_source::PREFER_TIFLASH;

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
        base.base.set_stats(Some(StatsInfo::new(50.0, [])));
        let source = LogicalPlan::DataSource(DataSource {
            base,
            physical_table_id: 7,
            enumerated_paths: vec![
                PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                },
                PossiblePath::TiFlashTable,
            ],
            prefer_store_type: PREFER_TIFLASH,
            has_tiflash_replica: true,
            ..DataSource::default()
        });

        let task = find_best_task(&source, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let Some(PhysicalPlan::TableReader(reader)) = task.plan() else {
            panic!("a TableReader, got {:?}", task.plan());
        };
        assert_eq!(
            reader.store_type,
            crate::physical_table_reader::StoreType::TiFlash
        );
        // Go `adjustReadReqType` (`physical_table_reader.go:299`): a TiFlash
        // reader whose table plan is an ExchangeSender reads through MPP, and
        // the single-fragment root shape is
        // TableReader -> ExchangeSender(PassThrough) -> TableScan
        // (`GenerateRootMPPTasks`, `fragment.go:167`).
        assert_eq!(
            reader.read_req_type,
            crate::physical_table_reader::ReadReqType::Mpp
        );
        let Some(PhysicalPlan::ExchangeSender(sender)) = reader.table_plan.as_deref() else {
            panic!("the TiFlash fragment hangs off TablePlan");
        };
        assert_eq!(
            sender.exchange_type,
            crate::physical::ExchangeType::PassThrough
        );
        let Some(PhysicalPlan::TableScan(scan)) = sender.base.children().first() else {
            panic!("the TiFlash scan hangs off the exchange sender");
        };
        assert_eq!(
            scan.store_type,
            crate::physical_table_reader::StoreType::TiFlash
        );
    }

    #[test]
    fn pushed_conditions_become_scan_ranges() {
        // The ranger wire-in: `WHERE pk > 5` on the int handle fills the
        // table scan's ranges with `(5, +inf]`; an indexed `b = 7` fills
        // the index scan's ranges with the point.
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;
        use crate::logical::data_source::DataSourceColumn;
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;
        use tidb_expr::scalar_function::ScalarFunction;
        use tidb_expr::schema::Schema;

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let pk = Column::new(11, {
            let mut ft = FieldType::new(FieldTypeCode::LongLong);
            ft.set_flags(ft.flags() | tidb_datatype::FieldTypeFlags::PRI_KEY);
            ft
        });
        let b = Column::new(12, FieldType::new(FieldTypeCode::LongLong));
        let cmp = |name: &str, col: &Column, v: i64| {
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new(name),
                FieldType::new(FieldTypeCode::LongLong),
                vec![
                    Expression::Column(col.clone()),
                    Expression::Constant(Constant::new(
                        Datum::Int(v),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                ],
            ))
        };
        let build = |conds: Vec<Expression>, paths: Vec<PossiblePath>| {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(100.0, [])));
            let mut schema = Schema::default();
            schema.columns = vec![pk.clone(), b.clone()];
            base.base.set_schema(Some(schema));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 7,
                pk_is_handle: true,
                columns: vec![
                    DataSourceColumn {
                        id: 1,
                        name: "pk".to_owned(),
                        is_primary_key: true,
                        is_not_null: true,
                    },
                    DataSourceColumn {
                        id: 2,
                        name: "b".to_owned(),
                        is_primary_key: false,
                        is_not_null: false,
                    },
                ],
                pushed_down_conds: conds,
                enumerated_paths: paths,
                indexes: vec![SourceIndex {
                    id: 3,
                    name: "ib".to_owned(),
                    columns: vec![SourceIndexColumn {
                        name: "b".to_owned(),
                        offset: 1,
                        length: -1,
                    }],
                    ..SourceIndex::default()
                }],
                ..DataSource::default()
            })
        };

        // Table path with pk > 5.
        let source = build(
            vec![cmp("gt", &pk, 5)],
            vec![PossiblePath::Table {
                is_int_handle: true,
                primary_index: None,
            }],
        );
        let task = find_best_task(&source, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let Some(PhysicalPlan::TableReader(reader)) = task.plan() else {
            panic!("a TableReader, got {:?}", task.plan());
        };
        let scan = match reader.table_plan.as_deref() {
            Some(PhysicalPlan::TableScan(scan)) => scan,
            Some(PhysicalPlan::Selection(selection)) => {
                let Some(PhysicalPlan::TableScan(scan)) = selection.base.children().first() else {
                    panic!("the residual selection's scan");
                };
                scan
            }
            other => panic!("the scan, got {other:?}"),
        };
        assert_eq!(scan.ranges.len(), 1);
        assert_eq!(scan.ranges[0].to_display_string(), "(5,+inf]");
        assert!(!crate::ranger::types::has_full_range(&scan.ranges, false));
        // Go's `adjustCountAfterAccess` raises the pseudo range estimate to
        // the logical DataSource row count when the access estimate is lower.
        let scanned = scan.base.base.stats_info().expect("stats").row_count();
        assert!((scanned - 100.0).abs() < 1e-9, "{scanned}");

        // A predicate on an ordinary column is residual to the table path.
        // It must not be interpreted as an integer-handle point/range merely
        // because it was pushed into the DataSource.
        let source = build(
            vec![cmp("eq", &b, 7)],
            vec![PossiblePath::Table {
                is_int_handle: true,
                primary_index: None,
            }],
        );
        let task = find_best_task(&source, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let Some(PhysicalPlan::TableReader(reader)) = task.plan() else {
            panic!("a TableReader, got {:?}", task.plan());
        };
        let scan = match reader.table_plan.as_deref() {
            Some(PhysicalPlan::TableScan(scan)) => scan,
            Some(PhysicalPlan::Selection(selection)) => {
                let Some(PhysicalPlan::TableScan(scan)) = selection.base.children().first() else {
                    panic!("the residual selection's scan");
                };
                scan
            }
            other => panic!("the scan, got {other:?}"),
        };
        assert!(crate::ranger::types::has_full_range(&scan.ranges, false));
        assert_eq!(
            scan.base.base.stats_info().expect("stats").row_count(),
            100.0,
            "a non-handle predicate must not scale table-range cardinality",
        );
        assert!(scan.range_rebuild.is_none());

        // Go's ranger reads PlanCacheParams, not Constant.Value saved at
        // PREPARE. Cover both table ranges and index detachment in this funnel.
        struct Parameters(Datum);
        impl tidb_expr::Columns for Parameters {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn param_value(&self, order: usize) -> Result<Datum, tidb_expr::EvalError> {
                std::slice::from_ref(&self.0).get(order).cloned().ok_or(
                    tidb_expr::EvalError::Unsupported("unbound prepared parameter"),
                )
            }
        }
        let mut parameter_predicate = cmp("gt", &pk, 0);
        let Expression::ScalarFunction(comparison) = &mut parameter_predicate else {
            unreachable!()
        };
        let Expression::Constant(parameter) = &mut comparison.args[1] else {
            unreachable!()
        };
        parameter.param_marker = Some(tidb_expr::constant::ParamMarker { order: 0 });
        for value in [5, 9] {
            let parameters = Parameters(Datum::Int(value));
            let evaluate =
                |expression: &Expression| tidb_expr::eval_expression_once(expression, &parameters);
            let mut context =
                DispatchContext::new(&allocator, &coster, 1.0).with_expression_evaluator(&evaluate);
            let source = build(
                vec![parameter_predicate.clone()],
                vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
            );
            let task = find_best_task(&source, &PhysicalProperty::default(), &mut context).unwrap();
            let Some(PhysicalPlan::TableReader(reader)) = task.plan() else {
                panic!("table reader")
            };
            let mut scan = reader.table_plan.as_deref().unwrap();
            while let [child] = scan.children() {
                scan = child;
            }
            let PhysicalPlan::TableScan(scan) = scan else {
                panic!("table scan")
            };
            assert_eq!(
                scan.ranges[0].to_display_string(),
                format!("({value},+inf]")
            );
            let detached = context
                .detach_index_range(
                    std::slice::from_ref(&parameter_predicate),
                    std::slice::from_ref(&pk),
                    &[-1],
                )
                .unwrap();
            assert_eq!(
                detached.ranges[0].to_display_string(),
                format!("({value},+inf]")
            );
        }
    }

    #[test]
    fn version_zero_binary_common_handle_keeps_its_index_suffix() {
        use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let declared = tidb_expr::column::Column::new(1, ty);
        let mut binary = FieldType::new(FieldTypeCode::VarString);
        binary.add_flags(FieldTypeFlags::BINARY);
        let handle = tidb_expr::column::Column::new(2, binary);
        let source = crate::logical::DataSource {
            is_common_handle: true,
            common_handle_version: 0,
            common_handle_cols: vec![handle],
            common_handle_lens: vec![3],
            ..Default::default()
        };
        let index = crate::plan_builder::catalog::SourceIndex {
            columns: vec![crate::plan_builder::catalog::SourceIndexColumn {
                name: "a".into(),
                offset: 0,
                length: -1,
            }],
            ..Default::default()
        };
        let mut prefix = vec![(declared, -1)];
        let suffix = source.handle_cols_to_append(&index, &prefix);
        prefix.extend(suffix);
        assert_eq!(prefix.len(), 2);
        assert_eq!(prefix[1].1, 3);
    }

    #[test]
    fn a_secondary_index_range_reaches_common_handle_columns() {
        // Go's `fillIndexPath` appends the complete clustered common handle
        // to a non-unique secondary index.  A tuple comparison therefore
        // becomes one lexicographic range per deciding handle column rather
        // than stopping after the declared `a` key part.
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;
        use crate::logical::data_source::DataSourceColumn;
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::{Expression, ScalarFunction};

        let column = |unique_id| Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong));
        let (a, b, c) = (column(11), column(12), column(13));
        let cmp = |name: &str, left: &Column, value: i64| {
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new(name),
                FieldType::new(FieldTypeCode::LongLong),
                vec![
                    Expression::Column(left.clone()),
                    Expression::Constant(Constant::new(
                        Datum::Int(value),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                ],
            ))
        };
        let and = |items: Vec<Expression>| {
            tidb_expr::simple_expr::compose_cnf_condition(items).expect("non-empty CNF")
        };
        let cond = tidb_expr::simple_expr::compose_dnf_condition(vec![
            cmp("gt", &a, 1),
            and(vec![cmp("eq", &a, 1), cmp("gt", &b, 2)]),
            and(vec![cmp("eq", &a, 1), cmp("eq", &b, 2), cmp("gt", &c, 3)]),
        ])
        .expect("non-empty DNF");

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
        base.base.set_stats(Some(StatsInfo::new(
            100.0,
            [(11, 100.0), (12, 100.0), (13, 100.0)],
        )));
        base.base
            .set_schema(Some(Schema::new(vec![a.clone(), b.clone(), c.clone()])));
        let source = LogicalPlan::DataSource(DataSource {
            base,
            table_id: 7,
            physical_table_id: 7,
            is_common_handle: true,
            handle_is_int: false,
            columns: vec![
                DataSourceColumn {
                    id: 1,
                    name: "a".to_owned(),
                    ..DataSourceColumn::default()
                },
                DataSourceColumn {
                    id: 2,
                    name: "b".to_owned(),
                    ..DataSourceColumn::default()
                },
                DataSourceColumn {
                    id: 3,
                    name: "c".to_owned(),
                    ..DataSourceColumn::default()
                },
            ],
            common_handle_cols: vec![b, c],
            common_handle_lens: vec![UNSPECIFIED_LENGTH; 2],
            pushed_down_conds: vec![cond],
            enumerated_paths: vec![PossiblePath::Index { index: 0 }],
            indexes: vec![SourceIndex {
                id: 3,
                name: "ia".to_owned(),
                columns: vec![SourceIndexColumn {
                    name: "a".to_owned(),
                    offset: 0,
                    length: UNSPECIFIED_LENGTH,
                }],
                ..SourceIndex::default()
            }],
            ..DataSource::default()
        });
        let task = find_best_task(&source, &PhysicalProperty::default(), &mut ctx).expect("plans");

        let scan = match task.plan().expect("physical plan") {
            PhysicalPlan::IndexLookUpReader(reader) => match reader.index_plan.as_deref() {
                Some(PhysicalPlan::IndexScan(scan)) => scan,
                other => panic!("expected an index scan child, got {other:?}"),
            },
            PhysicalPlan::IndexReader(reader) => match reader.index_plan.as_deref() {
                Some(PhysicalPlan::IndexScan(scan)) => scan,
                other => panic!("expected an index scan child, got {other:?}"),
            },
            other => panic!("expected an index reader, got {other:?}"),
        };
        let rendered = scan
            .ranges
            .iter()
            .map(crate::ranger::types::Range::to_display_string)
            .collect::<Vec<_>>();
        assert_eq!(rendered, ["(1 2 3,1 2 +inf]", "(1 2,1 +inf]", "(1,+inf]",]);
    }

    #[test]
    fn prefix_single_scan_distinguishes_unpruned_full_values_and_null_arguments() {
        use crate::logical::data_source::{DataSource, DataSourceColumn};
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_expr::expression::{Expression, ScalarFunction};
        let allocator = PlanIdAllocator::new();
        let a = Column::new(11, FieldType::new(FieldTypeCode::Varchar).with_flen(20));
        let b = Column::new(12, FieldType::new(FieldTypeCode::LongLong));
        let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
        base.base
            .set_schema(Some(Schema::new(vec![a.clone(), b.clone()])));
        let mut ds = DataSource {
            base,
            columns: vec![
                DataSourceColumn {
                    id: 1,
                    name: "a".into(),
                    ..DataSourceColumn::default()
                },
                DataSourceColumn {
                    id: 2,
                    name: "b".into(),
                    ..DataSourceColumn::default()
                },
            ],
            ..DataSource::default()
        };
        let index = SourceIndex {
            columns: vec![
                SourceIndexColumn {
                    name: "a".into(),
                    offset: 0,
                    length: 2,
                },
                SourceIndexColumn {
                    name: "b".into(),
                    offset: 1,
                    length: -1,
                },
            ],
            ..SourceIndex::default()
        };
        let call = |name: &str, args| {
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new(name),
                FieldType::new(FieldTypeCode::LongLong),
                args,
            ))
        };
        let null = call("isnull", vec![Expression::Column(a.clone())]);
        assert!(
            !index_path_is_single_scan(&ds, &index, true),
            "an unpruned nil list requires full schema coverage"
        );
        ds.cols_requiring_full_len = Some(Vec::new());
        assert!(index_path_is_single_scan(&ds, &index, true));
        assert!(!index_path_is_single_scan(&ds, &index, false));
        ds.cols_requiring_full_len = Some(vec![b.clone()]);
        for (condition, covered) in [
            (null.clone(), true),
            (call("not", vec![null.clone()]), true),
            (call("or", vec![null.clone(), Expression::Column(b)]), true),
            (
                call("or", vec![null.clone(), Expression::Column(a.clone())]),
                false,
            ),
            (
                call(
                    "isnull",
                    vec![call("length", vec![Expression::Column(a.clone())])],
                ),
                false,
            ),
        ] {
            assert_eq!(
                index_covers_condition(&ds, &index, &condition, true),
                covered
            );
            assert!(!index_covers_condition(&ds, &index, &condition, false));
            ds.all_conds = vec![condition];
            assert_eq!(index_path_is_single_scan(&ds, &index, true), covered);
        }
        ds.all_conds = vec![null];
        ds.cols_requiring_full_len = Some(vec![a]);
        assert!(
            !index_path_is_single_scan(&ds, &index, true),
            "full parent output cannot use a prefix"
        );

        ds.indexes = vec![index];
        ds.table_stats = Some(StatsInfo::new(10.0, []));
        let estimate = crate::cardinality::row_count_column::RowEstimate::new(2.0, 1.0, 4.0);
        ds.derived_index_paths.entry(0).or_default().row_estimate = Some(estimate);
        let mut plan = LogicalPlan::DataSource(ds);
        let context = crate::logical::rule_tests::test_context(&allocator);
        plan.recursive_derive_stats_with_context(&[], &context)
            .unwrap();
        let LogicalPlan::DataSource(source) = &mut plan else {
            unreachable!()
        };
        assert_eq!(
            source
                .derived_index_paths
                .get(&0)
                .and_then(|path| path.is_single_scan),
            Some(false)
        );
        source.cols_requiring_full_len = Some(Vec::new());
        plan.recursive_derive_stats_with_context(&[], &context)
            .unwrap();
        let LogicalPlan::DataSource(source) = &mut plan else {
            unreachable!()
        };
        assert_eq!(
            source.derived_index_paths.get(&0).and_then(|path| path.is_single_scan),
            Some(false),
            "final pruning must not change the path retained by join reorder"
        );
        assert_eq!(
            source.clone_shallow().derived_index_paths[&0].row_estimate,
            source.derived_index_paths[&0].row_estimate
        );
        assert_eq!(
            source.clone_shallow().derived_index_paths[&0].is_single_scan,
            source.derived_index_paths[&0].is_single_scan
        );
        let mut cloned = source.clone_shallow();
        cloned.derived_index_paths.get_mut(&0).unwrap().row_estimate = None;
        assert_eq!(
            source.derived_index_paths[&0].row_estimate,
            Some(estimate),
            "a cloned datasource owns its derived state"
        );
        source.base.base.set_stats(None);
        plan.recursive_derive_stats_with_context(&[], &context)
            .unwrap();
        let LogicalPlan::DataSource(source) = &mut plan else {
            unreachable!()
        };
        assert_eq!(
            source.derived_index_paths.get(&0).and_then(|path| path.is_single_scan),
            Some(true),
            "reinitialized statistics derive a new path from current requirements"
        );
        assert_eq!(
            source.derived_index_paths[&0].row_estimate,
            Some(estimate),
            "covering derivation preserves access estimates and risk bounds"
        );
        source.base.base.set_stats(None);
        let mut context = context;
        context.opt_prefix_index_single_scan = false;
        plan.recursive_derive_stats_with_context(&[], &context)
            .unwrap();
        let LogicalPlan::DataSource(source) = &plan else {
            unreachable!()
        };
        assert_eq!(
            source
                .derived_index_paths
                .get(&0)
                .and_then(|path| path.is_single_scan),
            Some(false)
        );
    }

    #[test]
    fn a_non_covering_index_plans_the_lookup_double_read() {
        // `IsSingleScan` end to end: with the catalog's column list filled,
        // an index that lacks a schema column is NOT a single scan, so its
        // cop task carries BOTH halves and converts through
        // `BuildIndexLookUpTask` — while a covering index still plans the
        // plain IndexReader.
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;
        use crate::logical::data_source::DataSourceColumn;
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::schema::Schema;

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let column_ids = crate::expression_rewriter::ColumnIdAllocator::new();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0).with_column_ids(&column_ids);
        let build = |index_columns: Vec<SourceIndexColumn>| {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(50.0, [])));
            let mut schema = Schema::default();
            schema.columns = vec![
                Column::new(11, FieldType::new(FieldTypeCode::LongLong)),
                Column::new(12, FieldType::new(FieldTypeCode::LongLong)),
            ];
            base.base.set_schema(Some(schema));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 7,
                columns: vec![
                    DataSourceColumn {
                        id: 1,
                        name: "a".to_owned(),
                        is_primary_key: false,
                        is_not_null: false,
                    },
                    DataSourceColumn {
                        id: 2,
                        name: "b".to_owned(),
                        is_primary_key: false,
                        is_not_null: false,
                    },
                ],
                enumerated_paths: vec![
                    PossiblePath::Table {
                        is_int_handle: true,
                        primary_index: None,
                    },
                    PossiblePath::Index { index: 0 },
                ],
                indexes: vec![SourceIndex {
                    id: 3,
                    name: "ib".to_owned(),
                    columns: index_columns,
                    ..SourceIndex::default()
                }],
                ..DataSource::default()
            })
        };
        let order_by_b = PhysicalProperty::new(TaskType::Root, &[12], false, f64::MAX, false);

        // Index on (b) alone: column `a` is uncovered — the double read
        // plans, its index side keeping the order and its table side reading
        // the rows back.
        let narrow = build(vec![SourceIndexColumn {
            name: "b".to_owned(),
            offset: 1,
            length: -1,
        }]);
        let task = find_best_task(&narrow, &order_by_b, &mut ctx).expect("plans");
        let reader = task.plan().and_then(|plan| match plan {
            PhysicalPlan::Projection(_) => plan.children().first(),
            _ => Some(plan),
        });
        let Some(PhysicalPlan::IndexLookUpReader(lookup)) = reader else {
            panic!("an IndexLookUpReader, got {:?}", task.plan());
        };
        assert!(lookup.keep_order, "the double read carries the order");
        assert!(
            matches!(lookup.index_plan.as_deref(), Some(PhysicalPlan::IndexScan(scan)) if scan.keep_order),
            "the index side keeps order"
        );
        assert!(
            matches!(
                lookup.table_plan.as_deref(),
                Some(PhysicalPlan::TableScan(_))
            ),
            "the table side reads rows back"
        );

        // Index on (b, a): covering, the reader plans as before.
        let covering = build(vec![
            SourceIndexColumn {
                name: "b".to_owned(),
                offset: 1,
                length: -1,
            },
            SourceIndexColumn {
                name: "a".to_owned(),
                offset: 0,
                length: -1,
            },
        ]);
        let task = find_best_task(&covering, &order_by_b, &mut ctx).expect("plans");
        assert!(
            matches!(task.plan(), Some(PhysicalPlan::IndexReader(_))),
            "got {:?}",
            task.plan()
        );

        // A PREFIX index column neither covers its own column
        // (`isIndexColsCoveringCol` requires full length) nor carries an
        // order (`isMatchProp` walks `FullIdxCols`): the ordered property
        // has no server at all.
        let prefix = build(vec![
            SourceIndexColumn {
                name: "b".to_owned(),
                offset: 1,
                length: 10,
            },
            SourceIndexColumn {
                name: "a".to_owned(),
                offset: 0,
                length: -1,
            },
        ]);
        let task = find_best_task(&prefix, &order_by_b, &mut ctx).expect("answers");
        assert!(task.invalid(), "a prefix column serves no order");

        // Go DataSource.handleCoveringColumn: the secondary index carries
        // the common primary key even when it is not a declared index part.
        let LogicalPlan::DataSource(mut common) = narrow else {
            unreachable!()
        };
        common.is_common_handle = true;
        common.common_handle_cols = vec![common.base.base.schema().unwrap().columns[0].clone()];
        common.common_handle_lens = vec![-1];
        assert!(index_path_is_single_scan(&common, &common.indexes[0], true));
        common.common_handle_lens = vec![1];
        assert!(!index_path_is_single_scan(
            &common,
            &common.indexes[0],
            true
        ));
    }

    #[test]
    fn a_prefix_index_matches_partial_order_and_carries_the_match_result() {
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;
        use crate::logical::data_source::DataSourceColumn;
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{FieldType, FieldTypeCode};

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut field_type = FieldType::new(FieldTypeCode::Varchar);
        field_type.set_flen(20);
        let column = Column::new(11, field_type);
        let mut base = BaseLogicalPlan::new(&allocator, DataSource::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(100.0, [])));
        base.base
            .set_schema(Some(Schema::new(vec![column.clone()])));
        let mut source = DataSource {
            base,
            physical_table_id: 7,
            columns: vec![DataSourceColumn {
                id: 1,
                name: "a".to_owned(),
                ..DataSourceColumn::default()
            }],
            enumerated_paths: vec![PossiblePath::Index { index: 0 }],
            indexes: vec![SourceIndex {
                id: 3,
                name: "ia_prefix".to_owned(),
                columns: vec![SourceIndexColumn {
                    name: "a".to_owned(),
                    offset: 0,
                    length: 4,
                }],
                ..SourceIndex::default()
            }],
            ..DataSource::default()
        };
        let partial = PhysicalProperty {
            task_tp: TaskType::CopMultiRead,
            partial_order_info: Some(crate::physical_property::PartialOrderInfo {
                sort_items: vec![SortItem::from_column(column.clone(), false)],
            }),
            ..PhysicalProperty::default()
        };
        let column_ids = crate::expression_rewriter::ColumnIdAllocator::new();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0)
            .with_column_ids(&column_ids)
            .with_partial_ordered_index_for_topn(true);
        let task =
            find_best_task_4_logical_data_source_without_enforcer(&source, &partial, &mut ctx)
                .expect("partial-order index path plans");
        let task_kind = match &task {
            Task::Root(_) => "root",
            Task::Cop(_) => "cop",
            Task::Mpp(_) => "mpp",
        };
        let Task::Cop(cop) = task else {
            panic!("the partial-order child remains a cop task, got {task_kind}");
        };
        assert!(cop.keep_order);
        let result = cop
            .partial_order_match_result
            .expect("the prefix match is carried to attach2Task");
        assert!(result.matched);
        assert_eq!(result.prefix_col.expect("prefix column").unique_id, 11);
        assert_eq!(result.prefix_len, 4);
        assert!(matches!(
            cop.index_plan.as_deref(),
            Some(PhysicalPlan::IndexScan(scan)) if scan.keep_order && !scan.desc
        ));

        let mut full_length = source.indexes[0].clone();
        full_length.columns[0].length = 20;
        assert!(
            match_partial_order_property(
                &source,
                &full_length,
                partial.partial_order_info.as_ref().unwrap(),
            )
            .is_none(),
            "a declared prefix at the full field length is not truncated"
        );

        for (force_order, force_no_order, valid) in [(true, false, true), (false, true, false)] {
            source.force_keep_order_index_ids = if force_order {
                [3].into_iter().collect()
            } else {
                Default::default()
            };
            source.force_no_keep_order_index_ids = if force_no_order {
                [3].into_iter().collect()
            } else {
                Default::default()
            };
            let task =
                find_best_task_4_logical_data_source_without_enforcer(&source, &partial, &mut ctx)
                    .unwrap();
            assert_eq!(
                !task.invalid(),
                valid,
                "order hint: {force_order}, no-order hint: {force_no_order}"
            );
        }

        // Master marks matching forced paths during the partial-order search,
        // before ordinary TopN candidates are costed. NO_ORDER_INDEX and an
        // unmatched property must preserve ordinary forced-index behavior.
        source.force_keep_order_index_ids.clear();
        for (forced, no_order, matches) in [
            (true, false, true),
            (false, false, true),
            (true, true, true),
            (true, false, false),
        ] {
            source.forced_index_ids = if forced {
                [3].into_iter().collect()
            } else {
                Default::default()
            };
            source.force_no_keep_order_index_ids = if no_order {
                [3].into_iter().collect()
            } else {
                Default::default()
            };
            let mut partial = partial.clone();
            if !matches {
                partial.partial_order_info.as_mut().unwrap().sort_items[0]
                    .col
                    .unique_id = 99;
            }
            let mut ctx = DispatchContext::new(&allocator, &coster, 1.0)
                .with_column_ids(&column_ids)
                .with_partial_ordered_index_for_topn(true);
            let _ =
                find_best_task_4_logical_data_source_without_enforcer(&source, &partial, &mut ctx)
                    .unwrap();
            let ordinary = PhysicalProperty {
                task_tp: TaskType::CopMultiRead,
                ..PhysicalProperty::default()
            };
            let task =
                find_best_task_4_logical_data_source_without_enforcer(&source, &ordinary, &mut ctx)
                    .unwrap();
            assert_eq!(
                task.invalid(),
                !forced || (!no_order && matches),
                "forced={forced}, no-order={no_order}, matched={matches}"
            );
        }

        // Separate occurrences can share a Go plan ID and an index ID. A
        // mark on one occurrence must not affect an already-existing other.
        source.forced_index_ids = [3].into_iter().collect();
        source.force_no_keep_order_index_ids.clear();
        let other = source.clone();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0)
            .with_column_ids(&column_ids)
            .with_partial_ordered_index_for_topn(true);
        let _ = find_best_task_4_logical_data_source_without_enforcer(&source, &partial, &mut ctx)
            .unwrap();
        let ordinary = PhysicalProperty {
            task_tp: TaskType::CopMultiRead,
            ..PhysicalProperty::default()
        };
        assert!(
            !find_best_task_4_logical_data_source_without_enforcer(&other, &ordinary, &mut ctx)
                .unwrap()
                .invalid()
        );

        // Skyline marks the path even when CopMultiRead conversion is later
        // refused because the common handle makes the index covering.
        let mut covering = source.clone();
        covering.is_common_handle = true;
        covering.common_handle_version = 1;
        covering.common_handle_cols = covering.base.base.schema().unwrap().columns.clone();
        covering.common_handle_lens = vec![-1];
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0)
            .with_column_ids(&column_ids)
            .with_partial_ordered_index_for_topn(true);
        assert!(
            find_best_task_4_logical_data_source_without_enforcer(&covering, &partial, &mut ctx)
                .unwrap()
                .invalid()
        );
        let single = PhysicalProperty {
            task_tp: TaskType::CopSingleRead,
            ..PhysicalProperty::default()
        };
        assert!(
            find_best_task_4_logical_data_source_without_enforcer(&covering, &single, &mut ctx)
                .unwrap()
                .invalid()
        );

        // Go returns an empty-range dual before matching order or marking
        // forced paths. An impossible predicate needs no index ordering.
        let filter_column = source.base.base.schema().unwrap().columns[0].clone();
        let mut equality = tidb_expr::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new("eq"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![
                tidb_expr::expression::Expression::Column(filter_column.clone()),
                tidb_expr::expression::Expression::Constant(tidb_expr::constant::Constant::new(
                    tidb_datatype::Datum::Null,
                    FieldType::new(FieldTypeCode::Varchar),
                )),
            ],
        );
        equality.collation.set_charset_and_collation(
            "utf8mb4",
            filter_column.ret_type.as_ref().unwrap().collation_name(),
        );
        source.pushed_down_conds =
            vec![tidb_expr::expression::Expression::ScalarFunction(equality)];
        let mut unmatched = partial.clone();
        unmatched.partial_order_info.as_mut().unwrap().sort_items[0]
            .col
            .unique_id = 99;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0)
            .with_column_ids(&column_ids)
            .with_partial_ordered_index_for_topn(true);
        let empty =
            find_best_task_4_logical_data_source_without_enforcer(&source, &unmatched, &mut ctx)
                .unwrap();
        assert!(matches!(empty.plan(), Some(PhysicalPlan::TableDual(dual)) if dual.row_count == 0));
    }

    #[test]
    fn a_constant_index_prefix_is_skipped_when_matching_order() {
        use crate::logical::DataSource;
        use crate::logical::data_source::DataSourceColumn;
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;
        use tidb_expr::scalar_function::ScalarFunction;
        use tidb_expr::schema::Schema;

        let a = Column::new(11, FieldType::new(FieldTypeCode::LongLong));
        let b = Column::new(12, FieldType::new(FieldTypeCode::LongLong));
        let mut base = BaseLogicalPlan::with_id(1, "DataSource", 0);
        base.base.set_stats(Some(StatsInfo::new(100.0, [])));
        base.base.set_schema(Some(Schema::new(vec![a.clone(), b])));
        let source = DataSource {
            base,
            columns: vec![
                DataSourceColumn {
                    id: 1,
                    name: "a".to_owned(),
                    is_primary_key: false,
                    is_not_null: false,
                },
                DataSourceColumn {
                    id: 2,
                    name: "b".to_owned(),
                    is_primary_key: false,
                    is_not_null: false,
                },
            ],
            pushed_down_conds: vec![Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("eq"),
                FieldType::new(FieldTypeCode::LongLong),
                vec![
                    Expression::Column(a),
                    Expression::Constant(Constant::new(
                        Datum::Int(1),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                ],
            ))],
            indexes: vec![SourceIndex {
                columns: vec![
                    SourceIndexColumn {
                        name: "a".to_owned(),
                        offset: 0,
                        length: -1,
                    },
                    SourceIndexColumn {
                        name: "b".to_owned(),
                        offset: 1,
                        length: -1,
                    },
                ],
                ..SourceIndex::default()
            }],
            ..DataSource::default()
        };
        let order_by_b = PhysicalProperty::new(TaskType::Root, &[12], false, f64::MAX, false);

        assert!(index_path_matches_order(
            &source,
            &source.indexes[0],
            &order_by_b
        ));
    }

    #[test]
    fn a_constant_common_handle_prefix_is_skipped_when_matching_order() {
        use crate::logical::DataSource;
        use tidb_datatype::{Datum, FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;
        use tidb_expr::scalar_function::ScalarFunction;
        use tidb_expr::schema::Schema;

        let warehouse = Column::new(11, FieldType::new(FieldTypeCode::LongLong));
        let district = Column::new(12, FieldType::new(FieldTypeCode::LongLong));
        let mut base = BaseLogicalPlan::with_id(1, "DataSource", 0);
        base.base
            .set_schema(Some(Schema::new(vec![warehouse.clone(), district.clone()])));
        let source = DataSource {
            base,
            pushed_down_conds: vec![Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("eq"),
                FieldType::new(FieldTypeCode::LongLong),
                vec![
                    Expression::Column(warehouse.clone()),
                    Expression::Constant(Constant::new(
                        Datum::Int(1),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                ],
            ))],
            common_handle_cols: vec![warehouse, district],
            common_handle_lens: vec![UNSPECIFIED_LENGTH; 2],
            ..DataSource::default()
        };
        let order_by_district =
            PhysicalProperty::new(TaskType::Root, &[12], false, f64::MAX, false);

        assert!(table_path_matches_order(&source, &order_by_district));
    }

    #[test]
    fn index_join_keeps_a_usable_prefix_when_trailing_columns_are_pruned() {
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;
        use crate::logical::data_source::DataSourceColumn;
        use crate::physical_property::IndexJoinRuntimeProp;
        use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::schema::Schema;

        let key = Column::new(11, FieldType::new(FieldTypeCode::LongLong));
        let mut base = BaseLogicalPlan::with_id(1, "DataSource", 0);
        base.base.set_schema(Some(Schema::new(vec![key.clone()])));
        let mut source = DataSource {
            base,
            columns: vec![DataSourceColumn {
                id: 1,
                name: "key".to_owned(),
                is_primary_key: false,
                is_not_null: true,
            }],
            indexes: vec![SourceIndex {
                columns: ["key", "pruned"]
                    .into_iter()
                    .enumerate()
                    .map(|(offset, name)| SourceIndexColumn {
                        name: name.to_owned(),
                        offset,
                        length: -1,
                    })
                    .collect(),
                ..SourceIndex::default()
            }],
            ..DataSource::default()
        };
        let runtime = IndexJoinRuntimeProp {
            other_conditions: Vec::new(),
            outer_join_keys: vec![Column::new(21, FieldType::new(FieldTypeCode::LongLong))],
            inner_join_keys: vec![key],
            avg_inner_row_count: 1.0,
            table_range_scan: false,
        };
        let path = PossiblePath::Index { index: 0 };
        assert!(path_matches_index_join_runtime(&source, &path, &runtime));

        // A missing leading column still prevents probing a later join key.
        source.indexes[0].columns.swap(0, 1);
        assert!(!path_matches_index_join_runtime(&source, &path, &runtime));
    }

    #[test]
    fn an_index_join_cannot_probe_only_a_later_common_handle_column() {
        use crate::access_path::PossiblePath;
        use crate::logical::DataSource;
        use crate::physical_property::IndexJoinRuntimeProp;
        use tidb_datatype::{FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
        use tidb_expr::column::Column;

        let part = Column::new(11, FieldType::new(FieldTypeCode::LongLong));
        let supplier = Column::new(12, FieldType::new(FieldTypeCode::LongLong));
        let source = DataSource {
            handle_cols: vec![part.clone(), supplier.clone()],
            handle_is_int: false,
            common_handle_cols: vec![part, supplier.clone()],
            common_handle_lens: vec![UNSPECIFIED_LENGTH; 2],
            ..DataSource::default()
        };
        let table_path = PossiblePath::Table {
            is_int_handle: false,
            primary_index: Some(0),
        };
        let runtime = IndexJoinRuntimeProp {
            other_conditions: Vec::new(),
            outer_join_keys: vec![Column::new(21, FieldType::new(FieldTypeCode::LongLong))],
            inner_join_keys: vec![supplier],
            avg_inner_row_count: 1.0,
            table_range_scan: true,
        };

        assert!(!path_matches_index_join_runtime(
            &source,
            &table_path,
            &runtime,
        ));
    }

    #[test]
    fn index_join_probe_floor_uses_only_the_accessed_equality_prefix() {
        use crate::logical::DataSource;
        use crate::physical_property::IndexJoinRuntimeProp;
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::column::Column;

        let first = Column::new(11, FieldType::new(FieldTypeCode::LongLong));
        let second = Column::new(12, FieldType::new(FieldTypeCode::LongLong));
        let source = DataSource {
            table_stats: Some(
                StatsInfo::new(2_000.0, [(11, 1_000.0), (12, 2_000.0)]).with_stats_version(1),
            ),
            ..DataSource::default()
        };
        let runtime = IndexJoinRuntimeProp {
            other_conditions: Vec::new(),
            outer_join_keys: vec![Column::new(21, FieldType::new(FieldTypeCode::LongLong))],
            inner_join_keys: vec![first.clone(), second.clone()],
            avg_inner_row_count: 1.0,
            table_range_scan: false,
        };

        // A path that can only range on the first of two equality keys still
        // scans one row per distinct first-key value: 2000 / 1000 = 2. The
        // floor disappears once all equality keys are usable, when Fix44855
        // is disabled, or when statistics are pseudo.
        assert_eq!(
            index_join_probe_access_rows_floor(&source, &[first.clone()], &runtime, true, 0.0),
            Some(2.0)
        );
        assert_eq!(
            index_join_probe_access_rows_floor(&source, &[first, second], &runtime, true, 0.0),
            None
        );
        assert_eq!(
            index_join_probe_access_rows_floor(
                &source,
                &[Column::new(11, FieldType::new(FieldTypeCode::LongLong))],
                &runtime,
                false,
                0.0,
            ),
            None
        );
        let pseudo = DataSource {
            table_stats: Some(StatsInfo::new(2_000.0, [(11, 1_000.0)])),
            ..DataSource::default()
        };
        assert_eq!(
            index_join_probe_access_rows_floor(
                &pseudo,
                &[Column::new(11, FieldType::new(FieldTypeCode::LongLong))],
                &runtime,
                true,
                0.0,
            ),
            None
        );
    }

    #[test]
    fn index_join_skyline_count_uses_one_stable_runtime_key() {
        use crate::logical::DataSource;
        use crate::physical_property::IndexJoinRuntimeProp;
        use tidb_datatype::{FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
        use tidb_expr::column::Column;

        let first = Column::new(11, FieldType::new(FieldTypeCode::LongLong));
        let second = Column::new(12, FieldType::new(FieldTypeCode::LongLong));
        let source = DataSource {
            table_stats: Some(
                StatsInfo::new(200_000.0, [(11, 100.0), (12, 200.0)]).with_stats_version(1),
            ),
            ..DataSource::default()
        };
        let runtime = IndexJoinRuntimeProp {
            other_conditions: Vec::new(),
            outer_join_keys: vec![Column::new(21, FieldType::new(FieldTypeCode::LongLong))],
            inner_join_keys: vec![first.clone()],
            avg_inner_row_count: 1.0,
            table_range_scan: false,
        };

        // Go divides the ordinary access estimate by the one runtime-key NDV
        // before applying Fix45132: 200000 / 100 = 2000.
        assert_eq!(
            index_join_skyline_count_for_index(
                &source,
                &runtime,
                &[
                    (first.clone(), UNSPECIFIED_LENGTH),
                    (second.clone(), UNSPECIFIED_LENGTH)
                ],
                Some(200_000.0),
            ),
            Some(2_000.0)
        );
        // Prefix-index runtime keys, multiple runtime keys, pseudo statistics,
        // and absent NDV all decline the strong skyline comparison.
        assert_eq!(
            index_join_skyline_count_for_index(
                &source,
                &runtime,
                &[(first.clone(), 4)],
                Some(200_000.0),
            ),
            None
        );
        let two_keys = IndexJoinRuntimeProp {
            inner_join_keys: vec![first.clone(), second.clone()],
            ..runtime.clone()
        };
        assert_eq!(
            index_join_skyline_count_for_index(
                &source,
                &two_keys,
                &[
                    (first.clone(), UNSPECIFIED_LENGTH),
                    (second, UNSPECIFIED_LENGTH)
                ],
                Some(200_000.0),
            ),
            None
        );
        let pseudo = DataSource {
            table_stats: Some(StatsInfo::new(200_000.0, [(11, 100.0)])),
            ..source.clone()
        };
        assert_eq!(
            index_join_skyline_count_for_index(
                &pseudo,
                &runtime,
                &[(first, UNSPECIFIED_LENGTH)],
                Some(200_000.0),
            ),
            None
        );
    }

    #[test]
    fn index_join_skyline_ratio_respects_fix_control_and_row_floor() {
        assert_eq!(
            index_join_skyline_prefers_current(Some(200.0), Some(2_000_000.0), f64::MAX, 1_000.0),
            Some(true)
        );
        assert_eq!(
            index_join_skyline_prefers_current(Some(2_000_000.0), Some(200.0), f64::MAX, 1_000.0),
            Some(false)
        );
        // Go's strict `> 100` guard and non-positive Fix45132 disable value.
        assert_eq!(
            index_join_skyline_prefers_current(Some(100.0), Some(200_000.0), f64::MAX, 1_000.0),
            None
        );
        assert_eq!(
            index_join_skyline_prefers_current(Some(200.0), Some(2_000_000.0), f64::MAX, 0.0),
            None
        );
        assert_eq!(
            index_join_skyline_prefers_current(Some(200.0), Some(2_000_000.0), 1.0, 1_000.0),
            None
        );
    }

    #[test]
    fn a_limit_pushes_its_partial_half_into_the_reader() {
        // The single-read push-down chain end to end
        // (`attach2Task4PhysicalLimit`, `task.go:619`): the Limit's cop
        // child property reaches the DataSource, which answers the raw COP
        // task; the attach pushes a partial limit — Count = Offset + Count,
        // offset removed — under the reader, and the ROOT limit above keeps
        // the offset. `derive_limit_stats`' recorded verdict ("awaiting
        // core/task.go") is hereby closed: its Go caller arrived.
        use crate::access_path::PossiblePath;
        use crate::logical::{DataSource, LogicalLimit};

        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(100.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 5,
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, LogicalLimit::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(7.0, [])));
        base.set_children(vec![source]);
        let limit = LogicalPlan::Limit(LogicalLimit::new(base, 2, 5));

        let task = find_best_task(&limit, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        let PhysicalPlan::Limit(root_limit) = plan else {
            panic!("the root Limit tops the plan, got {plan:?}");
        };
        assert_eq!((root_limit.offset, root_limit.count), (2, 5));
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!(
                "a TableReader under the root limit, got {:?}",
                plan.children()
            );
        };
        let Some(PhysicalPlan::Limit(pushed)) = reader.table_plan.as_deref() else {
            panic!("the pushed partial limit inside the reader");
        };
        assert_eq!(
            (pushed.offset, pushed.count),
            (0, 7),
            "offset removed, Count = Offset + Count"
        );
        assert!(
            (pushed.base.base.stats_info().expect("stats").row_count() - 7.0).abs() < f64::EPSILON,
            "DeriveLimitStats caps the pushed profile at the new count"
        );
        assert!(matches!(
            pushed.base.children().first(),
            Some(PhysicalPlan::TableScan(_))
        ));
    }

    #[test]
    fn a_topn_over_the_handle_plans_as_a_keep_order_limit_chain() {
        // TopN's LIMIT half end to end (`getPhysLimits`,
        // `physical_limit.go:198`): ORDER BY pk LIMIT plans the keep-order
        // scan through the child property's order, the pushed partial limit
        // inside the reader, and the root limit above — no Sort anywhere.
        use crate::access_path::PossiblePath;
        use crate::logical::{DataSource, LogicalTopN};
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::aggregation::ByItems;
        use tidb_expr::column::Column;
        use tidb_expr::expression::Expression;

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(100.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 5,
                pk_is_handle: true,
                handle_is_int: true,
                handle_cols: vec![Column::new(9, FieldType::new(FieldTypeCode::LongLong))],
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, LogicalTopN::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(3.0, [])));
        base.set_children(vec![source]);
        let topn = LogicalPlan::TopN(LogicalTopN {
            base,
            by_items: vec![ByItems::new(
                Expression::Column(Column::new(9, FieldType::new(FieldTypeCode::LongLong))),
                false,
            )],
            offset: 1,
            count: 2,
            ..LogicalTopN::default()
        });

        let task = find_best_task(&topn, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        let PhysicalPlan::Limit(root_limit) = plan else {
            panic!("the root Limit tops the plan, got {plan:?}");
        };
        assert_eq!((root_limit.offset, root_limit.count), (1, 2));
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!("a TableReader, got {:?}", plan.children());
        };
        let Some(PhysicalPlan::Limit(pushed)) = reader.table_plan.as_deref() else {
            panic!("the pushed partial limit inside the reader");
        };
        assert_eq!((pushed.offset, pushed.count), (0, 3));
        let Some(PhysicalPlan::TableScan(scan)) = pushed.base.children().first() else {
            panic!("the scan under the pushed limit");
        };
        assert!(scan.keep_order, "the child property's order rode down");
    }

    #[test]
    fn a_topn_pushes_its_partial_half_and_wins_over_the_limit_slice() {
        // Batches 33-34 end to end: ORDER BY a non-handle column LIMIT n
        // cannot ride keep-order (the LIMIT slice dies at the child), so
        // the TOPN slice wins — the pushed partial TopN sits inside the
        // reader (`getPushedDownTopN`'s simple half: Count = Offset +
        // Count, offset removed, DeriveLimitStats) and the root TopN keeps
        // the offset. Go's exact plan for this query shape.
        use crate::access_path::PossiblePath;
        use crate::logical::{DataSource, LogicalTopN};
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::aggregation::ByItems;
        use tidb_expr::column::Column;
        use tidb_expr::expression::Expression;

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(100.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 5,
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, LogicalTopN::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(4.0, [])));
        base.set_children(vec![source]);
        let topn = LogicalPlan::TopN(LogicalTopN {
            base,
            by_items: vec![ByItems::new(
                Expression::Column(Column::new(77, FieldType::new(FieldTypeCode::LongLong))),
                true,
            )],
            offset: 1,
            count: 3,
            ..LogicalTopN::default()
        });

        let task = find_best_task(&topn, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        let PhysicalPlan::TopN(root_topn) = plan else {
            panic!("the root TopN tops the plan, got {plan:?}");
        };
        assert_eq!((root_topn.offset, root_topn.count), (1, 3));
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!("a TableReader, got {:?}", plan.children());
        };
        let Some(PhysicalPlan::TopN(pushed)) = reader.table_plan.as_deref() else {
            panic!("the pushed partial TopN inside the reader");
        };
        assert_eq!((pushed.offset, pushed.count), (0, 4));
        assert!(
            (pushed.base.base.stats_info().expect("stats").row_count() - 4.0).abs() < f64::EPSILON,
            "DeriveLimitStats caps the pushed profile"
        );
    }

    #[test]
    fn an_aggregate_plans_above_the_reader() {
        // GROUP BY over a table, end to end: the cop arm of
        // `attach2Task4PhysicalHashAgg` now SPLITS — the partial half rides
        // inside the TableReader next to the scan, and the final half
        // merges above it.
        use crate::access_path::PossiblePath;
        use crate::logical::{DataSource, LogicalAggregation};

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let column_ids = crate::expression_rewriter::ColumnIdAllocator::new();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0).with_column_ids(&column_ids);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(100.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 5,
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, "HashAgg", 0);
        base.base.set_stats(Some(StatsInfo::new(10.0, [])));
        base.set_children(vec![source]);
        let agg = LogicalPlan::Aggregation(LogicalAggregation {
            base,
            ..LogicalAggregation::default()
        });

        let task = find_best_task(&agg, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::HashAgg(_)), "got {plan:?}");
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!("a TableReader, got {:?}", plan.children());
        };
        let Some(PhysicalPlan::HashAgg(_)) = reader.table_plan.as_deref() else {
            panic!("the partial aggregate rides inside the reader");
        };

        // A required order enumerates nothing (getHashAggs' first gate);
        // with CanAddEnforcer the enforcer branch sorts ABOVE the agg.
        let prop = PhysicalProperty {
            sort_items: vec![crate::physical_property::SortItem::new(1, false)],
            can_add_enforcer: true,
            ..PhysicalProperty::default()
        };
        let task = find_best_task(&agg, &prop, &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Sort(_)), "got {plan:?}");
        assert!(matches!(
            plan.children().first(),
            Some(PhysicalPlan::HashAgg(_))
        ));

        // Go findBestTask retries without the order when the hint cannot
        // satisfy it, even if the parent did not permit an enforcer.
        let prop = PhysicalProperty {
            can_add_enforcer: false,
            ..prop
        };
        assert!(find_best_task(&agg, &prop, &mut ctx).unwrap().invalid());
        let LogicalPlan::Aggregation(mut hinted) = agg.clone() else {
            unreachable!();
        };
        hinted.prefer_agg_type = crate::expression_rewriter::PREFER_HASH_AGG;
        let hinted = LogicalPlan::Aggregation(hinted);
        let task = find_best_task(&hinted, &prop, &mut ctx).expect("hinted plans");
        let plan = task.plan().expect("HASH_AGG requires a sorted hash plan");
        assert!(matches!(plan, PhysicalPlan::Sort(_)), "got {plan:?}");
        assert!(matches!(
            plan.children().first(),
            Some(PhysicalPlan::HashAgg(_))
        ));
    }

    #[test]
    fn a_global_aggregate_costs_stream_and_hash_over_the_same_child() {
        use crate::access_path::PossiblePath;
        use crate::logical::{DataSource, LogicalAggregation};
        use crate::plan_base::PossiblePropertiesInfo;

        let allocator = PlanIdAllocator::new();
        let coster = crate::find_best_task::coster::Ver2Coster::default();
        let column_ids = crate::expression_rewriter::ColumnIdAllocator::new();
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0).with_column_ids(&column_ids);
        let source = {
            let mut base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
            base.base.set_stats(Some(StatsInfo::new(250.0, [])));
            base.base
                .set_schema(Some(tidb_expr::schema::Schema::default()));
            LogicalPlan::DataSource(DataSource {
                base,
                physical_table_id: 5,
                enumerated_paths: vec![PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                }],
                ..DataSource::default()
            })
        };
        let mut base = BaseLogicalPlan::new(&allocator, "Aggregation", 0);
        base.base.set_stats(Some(StatsInfo::new(1.0, [])));
        base.set_children(vec![source]);
        let agg = LogicalPlan::Aggregation(LogicalAggregation {
            base,
            input_count: 250.0,
            possible_properties: PossiblePropertiesInfo {
                orders: vec![Vec::new()],
                has_tiflash: false,
            },
            ..LogicalAggregation::default()
        });

        let task = find_best_task(&agg, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let plan = task.plan().expect("a plan");
        assert!(
            matches!(plan, PhysicalPlan::StreamAgg(_)),
            "Go's global StreamAgg avoids HashAgg's fixed start cost: {plan:?}"
        );
        let Some(PhysicalPlan::TableReader(reader)) = plan.children().first() else {
            panic!(
                "a TableReader under the global aggregate: {:?}",
                plan.children()
            );
        };
        assert!(
            matches!(
                reader.table_plan.as_deref(),
                Some(PhysicalPlan::StreamAgg(_))
            ),
            "the partial stage keeps the same family"
        );
    }

    #[test]
    fn a_non_root_requirement_is_the_invalid_task() {
        // Go's early answer: "Currently all plan cannot totally push down
        // to TiKV."
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let selection = {
            let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
            base.set_children(vec![dual(&allocator, 1.0)]);
            LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()))
        };
        let prop = PhysicalProperty {
            task_tp: crate::task_type::TaskType::CopSingleRead,
            ..PhysicalProperty::default()
        };
        let task = find_best_task(&selection, &prop, &mut ctx).expect("answers");
        assert!(task.invalid());
    }

    #[test]
    fn the_task_map_memoizes_per_plan_and_property() {
        // Go's taskMap lookup: the second ask answers from the map. Pinned
        // by planning twice and checking the map holds entries for both the
        // selection and its child.
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let selection = {
            let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
            base.base.set_stats(Some(StatsInfo::new(10.0, [])));
            base.set_children(vec![dual(&allocator, 1.0)]);
            LogicalPlan::Selection(LogicalSelection::new(base, Vec::new()))
        };
        let first =
            find_best_task(&selection, &PhysicalProperty::default(), &mut ctx).expect("plans");
        let entries = ctx.task_map.len();
        assert!(entries >= 2, "the selection and its child are both stored");
        let second = find_best_task(&selection, &PhysicalProperty::default(), &mut ctx)
            .expect("answers from the map");
        assert_eq!(ctx.task_map.len(), entries, "no new entries on a hit");
        assert_eq!(
            format!("{:?}", first.plan().map(super::PhysicalPlan::tp)),
            format!("{:?}", second.plan().map(super::PhysicalPlan::tp)),
        );
    }

    #[test]
    fn the_task_map_distinguishes_nodes_that_share_a_go_plan_id() {
        // Go's static partition processor shallow-copies a DataSource and
        // deliberately retains its numeric plan ID. Go's task map belongs to
        // each plan object, so equal IDs must not alias one Rust memo entry.
        let allocator = PlanIdAllocator::new();
        let coster = CountCoster;
        let mut ctx = DispatchContext::new(&allocator, &coster, 1.0);
        let make_dual = |rows: usize| {
            let mut base = BaseLogicalPlan::with_id(77, LogicalTableDual::TYPE, 0);
            base.base.set_stats(Some(StatsInfo::new(rows as f64, [])));
            LogicalPlan::TableDual(LogicalTableDual::new(base, rows))
        };
        let first = make_dual(1);
        let second = make_dual(3);

        let first_task = find_best_task(&first, &PhysicalProperty::default(), &mut ctx)
            .expect("the first node plans");
        let second_task = find_best_task(&second, &PhysicalProperty::default(), &mut ctx)
            .expect("the second node plans independently");
        assert!(matches!(
            first_task.plan(),
            Some(PhysicalPlan::TableDual(dual)) if dual.row_count == 1
        ));
        assert!(matches!(
            second_task.plan(),
            Some(PhysicalPlan::TableDual(dual)) if dual.row_count == 3
        ));
    }

    /// Go `admitIndexJoinInnerChildPattern` (`exhaust_physical_plans.go:645`),
    /// branch by branch: `DataSource` (barring a TiFlash-preferred read),
    /// Selection/Projection/an inner Join/a matching Aggregation gated on
    /// `multi_pattern`, `UnionScan` unconditionally, everything else (the
    /// `Sort` used here stands in for the whole "Optimization Fence" set --
    /// Sort, Limit, TopN, Window, a non-inner Join) always refused.
    #[test]
    fn admits_index_join_inner_child_pattern_matches_go() {
        use crate::find_best_task::LogicalJoinType;
        use crate::logical::{
            DataSource, LogicalAggregation, LogicalJoin, LogicalSort, LogicalUnionScan,
            data_source::PREFER_TIFLASH,
        };
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::expression::Expression;
        use tidb_expr::schema::Schema;

        let allocator = PlanIdAllocator::new();
        let col = |id: i64| Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let runtime_prop = |inner_keys: Vec<Column>| {
            crate::physical_property::IndexJoinRuntimeProp::new(
                Vec::new(),
                Vec::new(),
                inner_keys,
                1.0,
                false,
            )
        };

        // DataSource: admitted regardless of `multi_pattern`, refused only
        // when it prefers a TiFlash read.
        let mut ds_base = BaseLogicalPlan::new(&allocator, "DataSource", 0);
        ds_base.base.set_schema(Some(Schema::new(vec![col(1)])));
        let plain_ds = LogicalPlan::DataSource(DataSource {
            base: ds_base.clone(),
            ..Default::default()
        });
        let prop = runtime_prop(vec![col(1)]);
        assert!(admits_index_join_inner_child_pattern(
            &plain_ds, &prop, false
        ));
        assert!(admits_index_join_inner_child_pattern(
            &plain_ds, &prop, true
        ));
        let tiflash_ds = LogicalPlan::DataSource(DataSource {
            base: ds_base,
            prefer_store_type: PREFER_TIFLASH,
            ..Default::default()
        });
        assert!(!admits_index_join_inner_child_pattern(
            &tiflash_ds,
            &prop,
            false
        ));

        // Selection / Projection: gated on `multi_pattern` alone.
        let mut sel_base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 0);
        sel_base.set_children(vec![plain_ds.clone()]);
        let selection = LogicalPlan::Selection(LogicalSelection::new(sel_base, Vec::new()));
        assert!(!admits_index_join_inner_child_pattern(
            &selection, &prop, false
        ));
        assert!(admits_index_join_inner_child_pattern(
            &selection, &prop, true
        ));

        // Join: gated on BOTH `multi_pattern` and `JoinType == Inner`.
        let mut inner_join_base = BaseLogicalPlan::new(&allocator, LogicalJoin::TYPE, 0);
        inner_join_base.set_children(vec![plain_ds.clone(), plain_ds.clone()]);
        let inner_join =
            LogicalPlan::Join(LogicalJoin::new(inner_join_base, LogicalJoinType::Inner));
        assert!(!admits_index_join_inner_child_pattern(
            &inner_join,
            &prop,
            false
        ));
        assert!(admits_index_join_inner_child_pattern(
            &inner_join,
            &prop,
            true
        ));

        let mut outer_join_base = BaseLogicalPlan::new(&allocator, LogicalJoin::TYPE, 0);
        outer_join_base.set_children(vec![plain_ds.clone(), plain_ds.clone()]);
        let outer_join = LogicalPlan::Join(LogicalJoin::new(
            outer_join_base,
            LogicalJoinType::LeftOuter,
        ));
        assert!(
            !admits_index_join_inner_child_pattern(&outer_join, &prop, true),
            "a non-inner join is refused even with multi_pattern on -- Go's \
             own comment names join as an Optimization Fence"
        );

        // Aggregation: gated on `multi_pattern`, AND every inner join key
        // that reaches the DataSource must be a bare GROUP BY column.
        let mut agg_base = BaseLogicalPlan::new(&allocator, LogicalAggregation::TYPE, 0);
        agg_base.set_children(vec![plain_ds.clone()]);
        let matching_agg = LogicalPlan::Aggregation(LogicalAggregation::new(
            agg_base,
            Vec::new(),
            vec![Expression::Column(col(1))],
        ));
        assert!(!admits_index_join_inner_child_pattern(
            &matching_agg,
            &prop,
            false
        ));
        assert!(admits_index_join_inner_child_pattern(
            &matching_agg,
            &prop,
            true
        ));

        let mut mismatched_agg_base = BaseLogicalPlan::new(&allocator, LogicalAggregation::TYPE, 0);
        mismatched_agg_base.set_children(vec![plain_ds.clone()]);
        let mismatched_agg = LogicalPlan::Aggregation(LogicalAggregation::new(
            mismatched_agg_base,
            Vec::new(),
            vec![Expression::Column(col(2))],
        ));
        assert!(
            !admits_index_join_inner_child_pattern(&mismatched_agg, &prop, true),
            "the inner join key is not a GROUP BY column, so grouping may \
             split rows the probe expects intact -- Go `checkIndexJoinInnerTaskWithAgg`"
        );

        // UnionScan: unconditionally admitted.
        let mut union_scan_base = BaseLogicalPlan::new(&allocator, LogicalUnionScan::TYPE, 0);
        union_scan_base.set_children(vec![plain_ds.clone()]);
        let union_scan = LogicalPlan::UnionScan(LogicalUnionScan::new(union_scan_base, Vec::new()));
        assert!(admits_index_join_inner_child_pattern(
            &union_scan,
            &prop,
            false
        ));

        // Everything else -- Sort stands in for the whole Optimization Fence
        // set (Sort, Limit, TopN, Window, a non-inner Join) -- is refused
        // even with multi_pattern on.
        let mut sort_base = BaseLogicalPlan::new(&allocator, "Sort", 0);
        sort_base.set_children(vec![plain_ds]);
        let sort = LogicalPlan::Sort(LogicalSort {
            base: sort_base,
            by_items: Vec::new(),
        });
        assert!(!admits_index_join_inner_child_pattern(&sort, &prop, true));
    }
}

/// Go `physicalop.CalcChildExpectedCnt` (`physical_utils.go`): what a join
/// or apply asks of its outer child when the parent wants fewer rows than
/// the operator estimates. Unbounded unless `prop.expected_cnt` is below the
/// operator's own estimate (or, under an ordering requirement with
/// `tidb_opt_ordering_index_selectivity_ratio` set, below the child's), then
/// the child's rows scaled in the same proportion plus the ordered rows that
/// must be read before the first match.
fn calc_child_expected_cnt(
    prop: &PhysicalProperty,
    child_rows: f64,
    estimated_rows: f64,
    ordering_ratio: f64,
) -> f64 {
    let ordered = !prop.is_sort_item_empty();
    let ratio = if ordered { ordering_ratio } else { 0.0 };
    if prop.expected_cnt < estimated_rows
        || (ordered
            && ratio > 0.0
            && child_rows > estimated_rows
            && prop.expected_cnt < child_rows
            && estimated_rows > 0.0)
    {
        let rows_to_meet_first = if ordered && ratio > 0.0 {
            ((child_rows - estimated_rows) * ratio).max(0.0)
        } else {
            0.0
        };
        return child_rows * (prop.expected_cnt / estimated_rows) + rows_to_meet_first;
    }
    f64::MAX
}
