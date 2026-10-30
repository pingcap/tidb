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

//! Go `pkg/planner/core/rule`: the logical rules that live in their own
//! package -- column pruning and key-info building, predicate column
//! collection, constant propagation, join-key casts, max/min elimination,
//! order-aware join reorder, outer-to-semi joins, partition pruning,
//! predicate simplification and index pruning -- and the rule flags
//! (`logical_rules.go`). Core's rule list (`optRuleList`) runs them.

pub mod rule_collect_plan_stats;
pub mod rule_constant_propagation;
pub mod rule_join_key_type_cast;
pub mod rule_max_min_elimination;
pub mod rule_order_aware_join_reorder;
pub mod rule_outer_join_to_semi_join;
pub mod rule_partition_processor;
pub mod rule_predicate_simplification;
pub mod rule_prune_indexes;
#[cfg(test)]
pub(crate) mod test_support;

use tidb_expr::column::Column;
use tidb_expr::schema::Schema;
use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_logicalop::plan_context::{LogicalOptRule, RuleContext};
use tidb_planner_core_operator_logicalop::LogicalPlan;

/// Go's optimizer rule bitmask values (`rule/logical_rules.go:20-55`).
///
/// The order here is Go's `1 << iota` order, which is DELIBERATELY not the
/// execution order; see this module's header.
pub mod flags {
    /// Go `FlagGcSubstitute`.
    pub const GC_SUBSTITUTE: u64 = 1 << 0;
    /// Go `FlagPruneColumns`.
    pub const PRUNE_COLUMNS: u64 = 1 << 1;
    /// Go `FlagStabilizeResults`.
    pub const STABILIZE_RESULTS: u64 = 1 << 2;
    /// Go `FlagBuildKeyInfo`.
    pub const BUILD_KEY_INFO: u64 = 1 << 3;
    /// Go `FlagDecorrelate`.
    pub const DECORRELATE: u64 = 1 << 4;
    /// Go `FlagSemiJoinRewrite`.
    pub const SEMI_JOIN_REWRITE: u64 = 1 << 5;
    /// Go `FlagEliminateAgg`.
    pub const ELIMINATE_AGG: u64 = 1 << 6;
    /// Go `FlagSkewDistinctAgg`.
    pub const SKEW_DISTINCT_AGG: u64 = 1 << 7;
    /// Go `FlagEliminateProjection`.
    pub const ELIMINATE_PROJECTION: u64 = 1 << 8;
    /// Go `FlagMaxMinEliminate`.
    pub const MAX_MIN_ELIMINATE: u64 = 1 << 9;
    /// Go `FlagConstantPropagation`.
    pub const CONSTANT_PROPAGATION: u64 = 1 << 10;
    /// Go `FlagConvertOuterToInnerJoin`.
    pub const CONVERT_OUTER_TO_INNER_JOIN: u64 = 1 << 11;
    /// Go `FlagPredicatePushDown`.
    pub const PREDICATE_PUSH_DOWN: u64 = 1 << 12;
    /// Go `FlagJoinKeyTypeCast`.
    pub const JOIN_KEY_TYPE_CAST: u64 = 1 << 13;
    /// Go `FlagEliminateOuterJoin`.
    pub const ELIMINATE_OUTER_JOIN: u64 = 1 << 14;
    /// Go `FlagPartitionProcessor`.
    pub const PARTITION_PROCESSOR: u64 = 1 << 15;
    /// Go `FlagCollectPredicateColumnsPoint`.
    pub const COLLECT_PREDICATE_COLUMNS_POINT: u64 = 1 << 16;
    /// Go `FlagPushDownAgg`.
    pub const PUSH_DOWN_AGG: u64 = 1 << 17;
    /// Go `FlagDeriveTopNFromWindow`.
    pub const DERIVE_TOPN_FROM_WINDOW: u64 = 1 << 18;
    /// Go `FlagPredicateSimplification`.
    pub const PREDICATE_SIMPLIFICATION: u64 = 1 << 19;
    /// Go `FlagPushDownTopN`.
    pub const PUSH_DOWN_TOPN: u64 = 1 << 20;
    /// Go `FlagOrderAwareJoinReorder`.
    pub const ORDER_AWARE_JOIN_REORDER: u64 = 1 << 21;
    /// Go `FlagSyncWaitStatsLoadPoint`.
    pub const SYNC_WAIT_STATS_LOAD_POINT: u64 = 1 << 22;
    /// Go `FlagJoinReOrder`.
    pub const JOIN_REORDER: u64 = 1 << 23;
    /// Go `FlagOuterJoinToSemiJoin`.
    pub const OUTER_JOIN_TO_SEMI_JOIN: u64 = 1 << 24;
    /// Go `FlagCorrelate`.
    pub const CORRELATE: u64 = 1 << 25;
    /// Go `FlagPruneColumnsAgain`.
    pub const PRUNE_COLUMNS_AGAIN: u64 = 1 << 26;
    /// Go `FlagPushDownSequence`.
    pub const PUSH_DOWN_SEQUENCE: u64 = 1 << 27;
    /// Go `FlagEliminateUnionAllDualItem`.
    pub const ELIMINATE_UNION_ALL_DUAL_ITEM: u64 = 1 << 28;
    /// Go `FlagEmptySelectionEliminator`.
    pub const EMPTY_SELECTION_ELIMINATOR: u64 = 1 << 29;
    /// Go `FlagResolveExpand`.
    pub const RESOLVE_EXPAND: u64 = 1 << 30;
    /// Go `FlagFullTextIndexResolveWhere`.
    pub const FULL_TEXT_INDEX_RESOLVE_WHERE: u64 = 1 << 31;
    /// Go `FlagFullTextIndexResolveTopN`.
    pub const FULL_TEXT_INDEX_RESOLVE_TOPN: u64 = 1 << 32;
    /// Go `FlagFullTextIndexResolveProjection`.
    pub const FULL_TEXT_INDEX_RESOLVE_PROJECTION: u64 = 1 << 33;
    /// Go `FlagFullTextIndexResolveReject`.
    pub const FULL_TEXT_INDEX_RESOLVE_REJECT: u64 = 1 << 34;
}

/// Go `setPredicatePushDownFlag(u)` (`rule/logical_rules.go:57`), the hook
/// `ruleutil.SetPredicatePushDownFlag` points at.
#[must_use]
pub const fn set_predicate_push_down_flag(flag: u64) -> u64 {
    flag | flags::PREDICATE_PUSH_DOWN
}

/// Go `rule.ColumnPruner` (`rule/rule_column_pruning.go:31`), Go rules #1 and
/// #29.
///
/// Go's body is `lp.PruneColumns(slices.Clone(lp.Schema().Columns))` followed
/// by the `noUnexpectedZeroColumnSchema` `intest` assertion.
#[derive(Debug)]
pub struct ColumnPruner;

/// Go `noUnexpectedZeroColumnSchema` (`rule_column_pruning.go:52`).
///
/// `LogicalPlan::schema` returns the first child's schema by reference when
/// this node does not own one, preserving Go's schema-pointer identity check
/// without a second, normalized plan representation.
pub fn no_unexpected_zero_column_schema(plan: &LogicalPlan) -> bool {
    for child in plan.children() {
        if !no_unexpected_zero_column_schema(child) {
            return false;
        }
    }
    if plan.schema().is_none_or(Schema::is_empty) {
        if !plan.children().is_empty() && plan.base().base.schema().is_none() {
            return true;
        }
        if !matches!(plan, LogicalPlan::TableDual(_)) {
            return false;
        }
    }
    true
}

impl LogicalOptRule for ColumnPruner {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let root_cols: Vec<Column> = plan
            .schema()
            .map(|schema| schema.columns.clone())
            .unwrap_or_default();
        let (plan, failure) = tidb_planner_core_operator_logicalop::rewrite::prune_columns(ctx, plan, root_cols);
        match failure {
            // Go's `planChanged` is hard-coded `false` in this rule.
            None => {
                debug_assert!(
                    no_unexpected_zero_column_schema(&plan),
                    "After column pruning, some operator got an unexpected zero-column output schema. Please fix it."
                );
                Ok((plan, false))
            }
            Some(error) => Err((plan, error)),
        }
    }

    fn name(&self) -> &'static str {
        "column_prune"
    }
}

/// Go `rule.BuildKeySolver` (`rule/rule_build_key_info.go:25`), Go rule #3.
///
/// Go's body is `ruleutil.BuildKeyInfoPortal(p)` — the post-order recursion at
/// `rule/util/misc.go:222`.
#[derive(Debug)]
pub struct BuildKeySolver;

impl LogicalOptRule for BuildKeySolver {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        _ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        Ok((
            tidb_planner_core_operator_logicalop::rewrite::build_key_info_portal_with_allocator(plan, _ctx.column_allocator),
            false,
        ))
    }

    fn name(&self) -> &'static str {
        "build_keys"
    }
}
