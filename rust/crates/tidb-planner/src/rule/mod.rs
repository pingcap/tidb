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

//! The logical optimization rule DRIVER.
//!
//! Go sources:
//! * `pkg/planner/core/base/rule_base.go` — the `LogicalOptRule` interface.
//! * `pkg/planner/core/optimizer.go:88-124` — `optRuleList`, the EXECUTION
//!   ORDER, which is semantic.
//! * `pkg/planner/core/optimizer.go:126-162` — `optRuleFlags`, index-aligned
//!   with `optRuleList`.
//! * `pkg/planner/core/optimizer.go:169` — `optInteractionRuleList`.
//! * `pkg/planner/core/optimizer.go:1076-1108` — `logicalOptimize` /
//!   `normalizeOptimize`.
//! * `pkg/planner/core/optimizer.go:1111` — `isLogicalRuleDisabled`.
//! * `pkg/planner/core/rule/logical_rules.go:20-55` — the flag BIT values.
//!
//! # Two arrays, not one
//!
//! Go's flag constants are declared with `1 << iota` in `logical_rules.go`,
//! and that declaration order is NOT the execution order in `optRuleList`.
//! `FlagFullTextIndexResolveWhere` is bit 31 but runs 12th;
//! `FlagResolveExpand` is bit 30 but runs last. Collapsing the two into one
//! table would silently renumber the flag bits, which are persisted in
//! `Plan.Flag` and compared against by callers outside the optimizer. So the
//! bit values live in [`flags`] and the execution order lives in
//! [`OPT_RULE_LIST`], with [`OPT_RULE_FLAGS`] index-aligned to it exactly as
//! Go has it.
//!
//! # Which rules actually run
//!
//! Go's list has 35 entries. Four rule bodies live in this file because their
//! tree walks are [`tidb_planner_core_operator_logicalop::rewrite`]'s:
//! [`ColumnPruner`] (#1 and #29), [`BuildKeySolver`] (#3), [`PpdSolver`] (#13)
//! and [`PushDownTopNOptimizer`] (#21). The remaining implemented bodies live
//! in their owning `rule_*.rs` modules beside this one, including:
//!
//! * [`rule_result_reorder::ResultReorder`] (#2)
//! * [`rule_aggregation_elimination::AggregationEliminator`] (#6)
//! * [`rule_constant_propagation::ConstantPropagationSolver`] (#10)
//! * [`rule_max_min_elimination::MaxMinEliminator`] (#12)
//! * [`rule_join_key_type_cast::JoinKeyTypeCastRewriter`] (#14)
//! * [`rule_partition_processor::PartitionProcessor`] (#16)
//! * [`rule_collect_plan_stats::CollectPredicateColumnsPoint`] (#17)
//! * [`rule_order_aware_join_reorder::OrderAwareJoinReorder`] (#22)
//! * [`rule_collect_plan_stats::SyncWaitStatsLoadPoint`] (#24)
//! * [`rule_join_reorder::JoinReOrderSolver`] (#26)
//! * [`rule_outer_join_to_semi_join::OuterJoinToSemiJoin`] (#27)
//! * [`rule_derive_topn_from_window::DeriveTopNFromWindow`] (#19)
//! * [`rule_push_down_sequence::PushDownSequenceSolver`] (#30)
//! * [`rule_eliminate_unionall_dual_item::EliminateUnionAllDualItem`]
//!   (#31)
//! * [`rule_eliminate_empty_selection::EmptySelectionEliminator`] (#32)
//! * [`rule_resolve_expand::ResolveExpand`] (#34)
//!
//! Entries whose [`RuleId::body`] is `None` are present as their name and flag
//! but have no body yet. The table is still ported because its order is
//! semantic.
//!
//! [`logical_optimize`] does NOT silently skip those. It records each one it
//! walked past in [`OptimizeOutcome::skipped`], so a caller can see exactly
//! which of Go's rules did not run against its plan. A rule that is missing is
//! visible in the result, never absorbed into it.
//!
//! # The rule trait's error and ownership shape
//!
//! Go: `Optimize(ctx, LogicalPlan) (LogicalPlan, bool, error)`. The plan is a
//! pointer, so Go's caller still holds it when `err != nil`.
//!
//! In a by-value IR that is not automatic: `Result<(LogicalPlan, bool),
//! PlanError>` would CONSUME the plan into the error path and lose it, and a
//! `?` inside a rewrite would additionally drop every half-rebuilt subtree.
//! [`LogicalOptRule::optimize`] therefore returns
//!
//! ```text
//! Result<(LogicalPlan, bool), (LogicalPlan, PlanError)>
//! ```
//!
//! — the plan comes back on BOTH arms. That is the by-value equivalent of Go
//! still holding its pointer, and it is what lets [`logical_optimize`] return
//! the plan as it stood when the failing rule started. The rewrites themselves
//! are infallible; see [`tidb_planner_core_operator_logicalop::fold`] for why, and for the
//! [`tidb_planner_core_operator_logicalop::fold::RewriteFailure`] slot that carries Go's `error` out of a
//! fold without unwinding it.
//!
//! # The arm-listing convention
//!
//! `super`'s enum has 27 variants and NO `_ =>` arm is allowed — the rule has
//! caught four real wiring gaps. Restating 27 arms in each of ~20 rules would
//! be about two thousand lines of pure pattern.
//!
//! THE CONVENTION, used by every rule here and required of later ones: write
//! explicit arms only for the variants with a Go override, and collect the
//! variants that take Go's BASE body into one [`base_arms!`] invocation. The
//! macro expands its ident list into an or-pattern, so the base set costs one
//! comma-separated line per few operators instead of one arm each, and the
//! match stays exhaustive — adding a variant to [`tidb_planner_core_operator_logicalop::LogicalPlan`] still
//! fails to compile until it is placed in some rule's explicit arm or its
//! `base_arms!` list. Do NOT invent a second convention.

use tidb_expr::column::Column;
use tidb_expr::schema::Schema;

use tidb_planner_core_base::PlanError;

use tidb_planner_core_operator_logicalop::LogicalPlan;
use tidb_planner_core_operator_logicalop::plan_context::{DisabledLogicalRules, LogicalOptRule, RuleContext};

pub mod rule_aggregation_elimination;
pub mod rule_aggregation_push_down;
pub mod rule_aggregation_skew_rewrite;
pub mod rule_collect_plan_stats;
pub mod rule_constant_propagation;
pub mod rule_decorrelate;
pub mod rule_derive_topn_from_window;
pub mod rule_eliminate_empty_selection;
pub mod rule_eliminate_unionall_dual_item;
pub mod rule_generate_column_substitute;
pub mod rule_join_elimination;
pub mod rule_join_key_type_cast;
pub mod rule_join_reorder;
pub mod rule_max_min_elimination;
pub mod rule_order_aware_join_reorder;
pub mod rule_outer_join_to_semi_join;
pub mod rule_outer_to_inner_join;
pub mod rule_partition_processor;
pub mod rule_predicate_simplification;
pub mod rule_projection_elimination;
pub mod rule_prune_indexes;
pub mod rule_push_down_sequence;
pub mod rule_resolve_expand;
pub mod rule_result_reorder;
pub mod rule_semi_join_rewrite;
#[cfg(test)]
pub(crate) mod rule_tail_tests;
#[cfg(test)]
pub(crate) mod rule_tests;

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

/// The rule-independent tail of Go `adjustOptimizationFlags`: when column
/// pruning is enabled together with any later logical rule, run the second
/// pruning pass after those rules have introduced temporary columns.
#[must_use]
pub const fn add_second_column_prune(flag: u64) -> u64 {
    if flag & flags::PRUNE_COLUMNS == 0 {
        return flag;
    }
    let above_prune_columns = !(flags::PRUNE_COLUMNS | (flags::PRUNE_COLUMNS - 1));
    if flag & above_prune_columns != 0 {
        flag | flags::PRUNE_COLUMNS_AGAIN
    } else {
        flag
    }
}

/// Every rule in `optRuleList`, named by the Go type that implements it.
///
/// The discriminant order is Go's EXECUTION order, so
/// `OPT_RULE_LIST[i as usize].id == i`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RuleId {
    /// Go `GcSubstituter`.
    GcSubstituter,
    /// Go `rule.ColumnPruner`.
    ColumnPruner,
    /// Go `ResultReorder`.
    ResultReorder,
    /// Go `rule.BuildKeySolver`.
    BuildKeySolver,
    /// Go `DecorrelateSolver`.
    DecorrelateSolver,
    /// Go `SemiJoinRewriter`.
    SemiJoinRewriter,
    /// Go `AggregationEliminator`.
    AggregationEliminator,
    /// Go `SkewDistinctAggRewriter`.
    SkewDistinctAggRewriter,
    /// Go `ProjectionEliminator`.
    ProjectionEliminator,
    /// Go `rule.MaxMinEliminator`.
    MaxMinEliminator,
    /// Go `rule.ConstantPropagationSolver`.
    ConstantPropagationSolver,
    /// Go `FullTextIndexResolverWhere`.
    FullTextIndexResolverWhere,
    /// Go `ConvertOuterToInnerJoin`.
    ConvertOuterToInnerJoin,
    /// Go `PPDSolver`.
    PpdSolver,
    /// Go `rule.JoinKeyTypeCastRewriter`.
    JoinKeyTypeCastRewriter,
    /// Go `OuterJoinEliminator`.
    OuterJoinEliminator,
    /// Go `rule.PartitionProcessor`.
    PartitionProcessor,
    /// Go `rule.CollectPredicateColumnsPoint`.
    CollectPredicateColumnsPoint,
    /// Go `AggregationPushDownSolver`.
    AggregationPushDownSolver,
    /// Go `DeriveTopNFromWindow`.
    DeriveTopNFromWindow,
    /// Go `rule.PredicateSimplification`.
    PredicateSimplification,
    /// Go `PushDownTopNOptimizer`.
    PushDownTopNOptimizer,
    /// Go `FullTextIndexResolverTopN`.
    FullTextIndexResolverTopN,
    /// Go `FullTextIndexResolverProjection`.
    FullTextIndexResolverProjection,
    /// Go `rule.OrderAwareJoinReorder`.
    OrderAwareJoinReorder,
    /// Go `rule.SyncWaitStatsLoadPoint`.
    SyncWaitStatsLoadPoint,
    /// Go `JoinReOrderSolver`.
    JoinReOrderSolver,
    /// Go `rule.OuterJoinToSemiJoin`.
    OuterJoinToSemiJoin,
    /// Go `CorrelateSolver`.
    CorrelateSolver,
    /// Go `rule.ColumnPruner`, run a SECOND time. Go's own comment: "column
    /// pruning again at last, note it will mess up the results of
    /// buildKeySolver".
    ColumnPrunerAgain,
    /// Go `PushDownSequenceSolver`.
    PushDownSequenceSolver,
    /// Go `EliminateUnionAllDualItem`.
    EliminateUnionAllDualItem,
    /// Go `EmptySelectionEliminator`.
    EmptySelectionEliminator,
    /// Go `FullTextIndexResolverRejectRemaining`.
    FullTextIndexResolverRejectRemaining,
    /// Go `ResolveExpand`.
    ResolveExpand,
}

impl RuleId {
    /// Go `LogicalOptRule.Name()`, the string `isLogicalRuleDisabled` matches
    /// against `DefaultDisabledLogicalRulesList`.
    ///
    /// Only the rules whose bodies are ported can be named authoritatively
    /// from their Go `Name()` method; the rest carry the name Go's
    /// `tidb_opt_disable_rules` documentation lists for them.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::GcSubstituter => "generate_column_substitute",
            Self::ColumnPruner | Self::ColumnPrunerAgain => "column_prune",
            // Go's `(*ResultReorder).Name()`; "stabilize_results" is the FLAG
            // constant's spelling, not the rule's name.
            Self::ResultReorder => "result_reorder",
            Self::BuildKeySolver => "build_keys",
            Self::DecorrelateSolver => "decorrelate",
            Self::SemiJoinRewriter => "semi_join_rewrite",
            Self::AggregationEliminator => "aggregation_eliminate",
            Self::SkewDistinctAggRewriter => "skew_distinct_agg_rewrite",
            Self::ProjectionEliminator => "projection_eliminate",
            Self::MaxMinEliminator => "max_min_eliminate",
            Self::ConstantPropagationSolver => "constant_propagation",
            Self::FullTextIndexResolverWhere => "full_text_index_resolve_where",
            Self::ConvertOuterToInnerJoin => "convert_outer_to_inner_join",
            Self::PpdSolver => "predicate_push_down",
            Self::JoinKeyTypeCastRewriter => "join_key_type_cast",
            Self::OuterJoinEliminator => "outer_join_eliminate",
            Self::PartitionProcessor => "partition_processor",
            Self::CollectPredicateColumnsPoint => "collect_predicate_columns_point",
            Self::AggregationPushDownSolver => "aggregation_push_down",
            Self::DeriveTopNFromWindow => "derive_topn_from_window",
            Self::PredicateSimplification => "predicate_simplification",
            Self::PushDownTopNOptimizer => "topn_push_down",
            Self::FullTextIndexResolverTopN => "full_text_index_resolve_topn",
            Self::FullTextIndexResolverProjection => "full_text_index_resolve_projection",
            Self::OrderAwareJoinReorder => "order_aware_join_reorder",
            Self::SyncWaitStatsLoadPoint => "sync_wait_stats_load_point",
            Self::JoinReOrderSolver => "join_reorder",
            Self::OuterJoinToSemiJoin => "outer_join_to_semi_join",
            Self::CorrelateSolver => "correlate",
            Self::PushDownSequenceSolver => "push_down_sequence",
            // Both of these are Go's own `Name()`, which differs from the flag
            // constant's spelling; `isLogicalRuleDisabled` matches `Name()`.
            Self::EliminateUnionAllDualItem => "union_all_eliminate_dual_item",
            Self::EmptySelectionEliminator => "eliminate_empty_selection",
            Self::FullTextIndexResolverRejectRemaining => "full_text_index_resolve_reject",
            Self::ResolveExpand => "resolve_expand",
        }
    }

    /// The rule's body, when this crate has one.
    ///
    /// `None` is not "this rule does nothing"; it is "this rule is not ported
    /// yet", and [`logical_optimize`] reports it as skipped rather than
    /// treating it as a no-op.
    #[must_use]
    pub fn body(self) -> Option<&'static dyn LogicalOptRule> {
        match self {
            Self::ColumnPruner | Self::ColumnPrunerAgain => Some(&ColumnPruner),
            Self::BuildKeySolver => Some(&BuildKeySolver),
            Self::AggregationEliminator => {
                Some(&rule_aggregation_elimination::AggregationEliminator)
            }
            Self::ProjectionEliminator => {
                Some(&rule_projection_elimination::ProjectionEliminator)
            }
            Self::PpdSolver => Some(&PpdSolver),
            Self::JoinKeyTypeCastRewriter => {
                Some(&rule_join_key_type_cast::JoinKeyTypeCastRewriter)
            }
            Self::PushDownTopNOptimizer => Some(&PushDownTopNOptimizer),
            Self::ResultReorder => Some(&rule_result_reorder::ResultReorder),
            Self::DeriveTopNFromWindow => {
                Some(&rule_derive_topn_from_window::DeriveTopNFromWindow)
            }
            Self::PushDownSequenceSolver => {
                Some(&rule_push_down_sequence::PushDownSequenceSolver)
            }
            Self::EliminateUnionAllDualItem => {
                Some(&rule_eliminate_unionall_dual_item::EliminateUnionAllDualItem)
            }
            Self::EmptySelectionEliminator => {
                Some(&rule_eliminate_empty_selection::EmptySelectionEliminator)
            }
            Self::ResolveExpand => Some(&rule_resolve_expand::ResolveExpand),
            Self::CollectPredicateColumnsPoint => {
                Some(&rule_collect_plan_stats::CollectPredicateColumnsPoint)
            }
            Self::SyncWaitStatsLoadPoint => {
                Some(&rule_collect_plan_stats::SyncWaitStatsLoadPoint)
            }
            Self::PartitionProcessor => Some(&rule_partition_processor::PartitionProcessor),
            Self::ConstantPropagationSolver => {
                Some(&rule_constant_propagation::ConstantPropagationSolver)
            }
            Self::MaxMinEliminator => Some(&rule_max_min_elimination::MaxMinEliminator),
            Self::PredicateSimplification => {
                Some(&rule_predicate_simplification::PredicateSimplification)
            }
            Self::OrderAwareJoinReorder => {
                Some(&rule_order_aware_join_reorder::OrderAwareJoinReorder)
            }
            Self::JoinReOrderSolver => Some(&rule_join_reorder::JoinReOrderSolver),
            Self::AggregationPushDownSolver => {
                Some(&rule_aggregation_push_down::AggregationPushDownSolver)
            }
            Self::OuterJoinToSemiJoin => {
                Some(&rule_outer_join_to_semi_join::OuterJoinToSemiJoin)
            }
            Self::GcSubstituter => Some(&rule_generate_column_substitute::GcSubstituter),
            Self::SemiJoinRewriter => Some(&rule_semi_join_rewrite::SemiJoinRewriter),
            Self::SkewDistinctAggRewriter => {
                Some(&rule_aggregation_skew_rewrite::SkewDistinctAggRewriter)
            }
            Self::ConvertOuterToInnerJoin => {
                Some(&rule_outer_to_inner_join::ConvertOuterToInnerJoin)
            }
            Self::OuterJoinEliminator => Some(&rule_join_elimination::OuterJoinEliminator),
            Self::DecorrelateSolver => Some(&rule_decorrelate::DecorrelateSolver),
            Self::FullTextIndexResolverWhere
            | Self::FullTextIndexResolverTopN
            | Self::FullTextIndexResolverProjection
            | Self::CorrelateSolver
            | Self::FullTextIndexResolverRejectRemaining => None,
        }
    }
}

/// Go `optRuleList` (`optimizer.go:88`). THE ORDER IS SEMANTIC.
pub const OPT_RULE_LIST: [RuleId; 35] = [
    RuleId::GcSubstituter,
    RuleId::ColumnPruner,
    RuleId::ResultReorder,
    RuleId::BuildKeySolver,
    RuleId::DecorrelateSolver,
    RuleId::SemiJoinRewriter,
    RuleId::AggregationEliminator,
    RuleId::SkewDistinctAggRewriter,
    RuleId::ProjectionEliminator,
    RuleId::MaxMinEliminator,
    RuleId::ConstantPropagationSolver,
    RuleId::FullTextIndexResolverWhere,
    RuleId::ConvertOuterToInnerJoin,
    RuleId::PpdSolver,
    RuleId::JoinKeyTypeCastRewriter,
    RuleId::OuterJoinEliminator,
    RuleId::PartitionProcessor,
    RuleId::CollectPredicateColumnsPoint,
    RuleId::AggregationPushDownSolver,
    RuleId::DeriveTopNFromWindow,
    RuleId::PredicateSimplification,
    RuleId::PushDownTopNOptimizer,
    RuleId::FullTextIndexResolverTopN,
    RuleId::FullTextIndexResolverProjection,
    RuleId::OrderAwareJoinReorder,
    RuleId::SyncWaitStatsLoadPoint,
    RuleId::JoinReOrderSolver,
    RuleId::OuterJoinToSemiJoin,
    RuleId::CorrelateSolver,
    RuleId::ColumnPrunerAgain,
    RuleId::PushDownSequenceSolver,
    RuleId::EliminateUnionAllDualItem,
    RuleId::EmptySelectionEliminator,
    RuleId::FullTextIndexResolverRejectRemaining,
    RuleId::ResolveExpand,
];

/// Go `optRuleFlags` (`optimizer.go:126`), INDEX-ALIGNED with
/// [`OPT_RULE_LIST`].
pub const OPT_RULE_FLAGS: [u64; 35] = [
    flags::GC_SUBSTITUTE,
    flags::PRUNE_COLUMNS,
    flags::STABILIZE_RESULTS,
    flags::BUILD_KEY_INFO,
    flags::DECORRELATE,
    flags::SEMI_JOIN_REWRITE,
    flags::ELIMINATE_AGG,
    flags::SKEW_DISTINCT_AGG,
    flags::ELIMINATE_PROJECTION,
    flags::MAX_MIN_ELIMINATE,
    flags::CONSTANT_PROPAGATION,
    flags::FULL_TEXT_INDEX_RESOLVE_WHERE,
    flags::CONVERT_OUTER_TO_INNER_JOIN,
    flags::PREDICATE_PUSH_DOWN,
    flags::JOIN_KEY_TYPE_CAST,
    flags::ELIMINATE_OUTER_JOIN,
    flags::PARTITION_PROCESSOR,
    flags::COLLECT_PREDICATE_COLUMNS_POINT,
    flags::PUSH_DOWN_AGG,
    flags::DERIVE_TOPN_FROM_WINDOW,
    flags::PREDICATE_SIMPLIFICATION,
    flags::PUSH_DOWN_TOPN,
    flags::FULL_TEXT_INDEX_RESOLVE_TOPN,
    flags::FULL_TEXT_INDEX_RESOLVE_PROJECTION,
    flags::ORDER_AWARE_JOIN_REORDER,
    flags::SYNC_WAIT_STATS_LOAD_POINT,
    flags::JOIN_REORDER,
    flags::OUTER_JOIN_TO_SEMI_JOIN,
    flags::CORRELATE,
    flags::PRUNE_COLUMNS_AGAIN,
    flags::PUSH_DOWN_SEQUENCE,
    flags::ELIMINATE_UNION_ALL_DUAL_ITEM,
    flags::EMPTY_SELECTION_ELIMINATOR,
    flags::FULL_TEXT_INDEX_RESOLVE_REJECT,
    flags::RESOLVE_EXPAND,
];

/// Go `normalizeRuleList = optRuleList` (`optimizer.go:83`).
///
/// Go's own comment: "note this two list will differ when some trade-off rules
/// is moved out of norm phase for cascades." Today they are the same list, and
/// aliasing rather than copying is what keeps them the same when Go changes
/// only one.
pub const NORMALIZE_RULE_LIST: [RuleId; 35] = OPT_RULE_LIST;

/// Go `normalizeRuleFlags = optRuleFlags` (`optimizer.go:84`).
pub const NORMALIZE_RULE_FLAGS: [u64; 35] = OPT_RULE_FLAGS;

/// Go `optInteractionRuleList` (`optimizer.go:169`).
///
/// Go's declaration today is `map[base.LogicalOptRule]base.LogicalOptRule{}` —
/// an EMPTY map. The SHAPE is what is ported: a rule that changed the plan may
/// schedule exactly one other rule to run again afterwards. The table is empty
/// here because it is empty in Go, not because entries were dropped.
#[must_use]
pub const fn opt_interaction_rule(_rule: RuleId) -> Option<RuleId> {
    None
}

/// Go `isLogicalRuleDisabled(r)` (`optimizer.go:1111`).
#[must_use]
pub fn is_logical_rule_disabled(disabled: &DisabledLogicalRules, rule: RuleId) -> bool {
    disabled.contains(rule.name())
}

/// What [`logical_optimize`] did, rule by rule.
#[derive(Debug)]
pub struct OptimizeOutcome {
    /// The optimized plan.
    pub plan: LogicalPlan,
    /// The rules that ran, in the order they ran.
    pub applied: Vec<RuleId>,
    /// The rules whose flag bit was set and which are NOT disabled, but whose
    /// body this crate does not have yet. Never silently dropped; see this
    /// module's header.
    pub skipped: Vec<RuleId>,
}

/// Go `logicalOptimize(ctx, flag, logic)` (`optimizer.go:1076`).
///
/// # Errors
///
/// Returns the plan as the failing rule handed it back, plus the error.
#[allow(clippy::result_large_err)]
pub fn logical_optimize(
    ctx: &RuleContext<'_>,
    flag: u64,
    plan: LogicalPlan,
) -> Result<OptimizeOutcome, (LogicalPlan, PlanError)> {
    run_rule_list(ctx, flag, plan, &OPT_RULE_LIST, &OPT_RULE_FLAGS)
}

/// Go `normalizeOptimize(ctx, flag, logic)` (`optimizer.go:1076`'s sibling),
/// which walks `normalizeRuleList` instead.
///
/// # Errors
///
/// As [`logical_optimize`].
#[allow(clippy::result_large_err)]
pub fn normalize_optimize(
    ctx: &RuleContext<'_>,
    flag: u64,
    plan: LogicalPlan,
) -> Result<OptimizeOutcome, (LogicalPlan, PlanError)> {
    run_rule_list(ctx, flag, plan, &NORMALIZE_RULE_LIST, &NORMALIZE_RULE_FLAGS)
}

#[allow(clippy::result_large_err)]
fn run_rule_list(
    ctx: &RuleContext<'_>,
    flag: u64,
    mut plan: LogicalPlan,
    rules: &[RuleId; 35],
    rule_flags: &[u64; 35],
) -> Result<OptimizeOutcome, (LogicalPlan, PlanError)> {
    let mut applied = Vec::new();
    let mut skipped = Vec::new();
    let mut again: Vec<RuleId> = Vec::new();

    for (i, &rule) in rules.iter().enumerate() {
        // Go: "The rule list defines the execution order. The parallel flag
        // list maps each rule to its stable bitmask value."
        if flag & rule_flags[i] == 0 || is_logical_rule_disabled(&ctx.disabled_rules, rule) {
            continue;
        }
        let Some(body) = rule.body() else {
            skipped.push(rule);
            continue;
        };
        let (next, plan_changed) = body.optimize(ctx, plan)?;
        plan = next;
        applied.push(rule);
        // Go: compute interaction rules that should be optimized again.
        if let Some(interaction) = opt_interaction_rule(rule) {
            if plan_changed && is_logical_rule_disabled(&ctx.disabled_rules, interaction) {
                again.push(interaction);
            }
        }
    }

    // Go: trigger the interaction rule.
    for rule in again {
        match rule.body() {
            Some(body) => {
                let (next, _) = body.optimize(ctx, plan)?;
                plan = next;
                applied.push(rule);
            }
            None => skipped.push(rule),
        }
    }

    Ok(OptimizeOutcome {
        plan,
        applied,
        skipped,
    })
}

// ***** the four ported rules *****

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
pub(super) fn no_unexpected_zero_column_schema(plan: &LogicalPlan) -> bool {
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

/// Go `PPDSolver` (`rule_predicate_push_down.go:31`), Go rule #13.
///
/// Go's body is `_, p, err := lp.PredicatePushDown(nil)`.
#[derive(Debug)]
pub struct PpdSolver;

impl LogicalOptRule for PpdSolver {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let (plan, _remaining, failure) =
            tidb_planner_core_operator_logicalop::rewrite::predicate_push_down(ctx, plan, Vec::new());
        match failure {
            None => Ok((plan, false)),
            Some(error) => Err((plan, error)),
        }
    }

    fn name(&self) -> &'static str {
        "predicate_push_down"
    }
}

/// Go `PushDownTopNOptimizer` (`rule_topn_push_down.go:24`), Go rule #21.
///
/// Go's body is `p.PushDownTopN(nil)`.
#[derive(Debug)]
pub struct PushDownTopNOptimizer;

impl LogicalOptRule for PushDownTopNOptimizer {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        Ok((
            tidb_planner_core_operator_logicalop::rewrite::push_down_topn_with_builder(ctx.builder, ctx.allocator, plan, None),
            false,
        ))
    }

    fn name(&self) -> &'static str {
        "topn_push_down"
    }
}
