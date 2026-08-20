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
//! Go's list has 35 entries and TEN of them have a body here. Four live in
//! this file, because their tree walks are [`super::rewrite`]'s:
//! [`ColumnPruner`] (#1 and #29), [`BuildKeySolver`] (#3), [`PpdSolver`] (#13)
//! and [`PushDownTopNOptimizer`] (#21). Six more live in their own
//! `rule_*.rs` beside this one, each one fold and one file:
//!
//! * [`super::rule_result_reorder::ResultReorder`] (#2)
//! * [`super::rule_derive_topn_from_window::DeriveTopNFromWindow`] (#19)
//! * [`super::rule_push_down_sequence::PushDownSequenceSolver`] (#30)
//! * [`super::rule_eliminate_unionall_dual_item::EliminateUnionAllDualItem`]
//!   (#31)
//! * [`super::rule_eliminate_empty_selection::EmptySelectionEliminator`] (#32)
//! * [`super::rule_resolve_expand::ResolveExpand`] (#34)
//!
//! The remaining 25 are present in [`OPT_RULE_LIST`] as their name and flag —
//! the TABLE is ported, because the order is the semantics — but they have no
//! body yet.
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
//! are infallible; see [`super::fold`] for why, and for the
//! [`super::fold::RewriteFailure`] slot that carries Go's `error` out of a
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
//! match stays exhaustive — adding a variant to [`super::LogicalPlan`] still
//! fails to compile until it is placed in some rule's explicit arm or its
//! `base_arms!` list. Do NOT invent a second convention.

use tidb_expr::column::Column;
use tidb_expr::expr_util::builder::FunctionBuilder;
use tidb_expr::expr_util::predicates::{is_const_null, maybe_over_optimized_4_plan_cache};
use tidb_expr::expr_util::push_not::push_down_not;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::plan_base::{PlanError, PlanIdAllocator};

use super::selection::LogicalSelection;
use super::table_dual::LogicalTableDual;
use super::{BaseLogicalPlan, LogicalPlan};

/// Expands an ident list of [`super::LogicalPlan`] variants into an
/// or-pattern binding nothing.
///
/// See this module's header for why this exists and when to use it.
///
/// Fenced `text` and not `ignore`: the `...` placeholders make this a shape
/// sketch rather than Rust that could compile, while `ignore` would file it
/// as a doctest that merely goes unrun -- which is the thing
/// `difftest-result-tests::doctest_gate` counts and asks to have reviewed.
///
/// ```text
/// match plan {
///     LogicalPlan::Selection(op) => ...,
///     base_arms![Sort, Limit, TopN] => ...,
///     ...
/// }
/// ```
#[macro_export]
macro_rules! base_arms {
    ($($variant:ident),+ $(,)?) => {
        $( $crate::logical::LogicalPlan::$variant(..) )|+
    };
}

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
            Self::PpdSolver => Some(&PpdSolver),
            Self::PushDownTopNOptimizer => Some(&PushDownTopNOptimizer),
            Self::ResultReorder => Some(&super::rule_result_reorder::ResultReorder),
            Self::DeriveTopNFromWindow => {
                Some(&super::rule_derive_topn_from_window::DeriveTopNFromWindow)
            }
            Self::PushDownSequenceSolver => {
                Some(&super::rule_push_down_sequence::PushDownSequenceSolver)
            }
            Self::EliminateUnionAllDualItem => {
                Some(&super::rule_eliminate_unionall_dual_item::EliminateUnionAllDualItem)
            }
            Self::EmptySelectionEliminator => {
                Some(&super::rule_eliminate_empty_selection::EmptySelectionEliminator)
            }
            Self::ResolveExpand => Some(&super::rule_resolve_expand::ResolveExpand),
            Self::GcSubstituter
            | Self::DecorrelateSolver
            | Self::SemiJoinRewriter
            | Self::AggregationEliminator
            | Self::SkewDistinctAggRewriter
            | Self::ProjectionEliminator
            | Self::MaxMinEliminator
            | Self::ConstantPropagationSolver
            | Self::FullTextIndexResolverWhere
            | Self::ConvertOuterToInnerJoin
            | Self::JoinKeyTypeCastRewriter
            | Self::OuterJoinEliminator
            | Self::PartitionProcessor
            | Self::CollectPredicateColumnsPoint
            | Self::AggregationPushDownSolver
            | Self::PredicateSimplification
            | Self::FullTextIndexResolverTopN
            | Self::FullTextIndexResolverProjection
            | Self::OrderAwareJoinReorder
            | Self::SyncWaitStatsLoadPoint
            | Self::JoinReOrderSolver
            | Self::OuterJoinToSemiJoin
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

/// Go `DefaultDisabledLogicalRulesList` (`optimizer.go`), the session-level
/// set of rule NAMES that `isLogicalRuleDisabled` consults.
#[derive(Clone, Debug, Default)]
pub struct DisabledLogicalRules {
    names: Vec<String>,
}

impl DisabledLogicalRules {
    /// A set built from Go's comma-separated `tidb_opt_disable_rules` value.
    #[must_use]
    pub fn from_names<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            names: names.into_iter().map(Into::into).collect(),
        }
    }

    /// Go `isLogicalRuleDisabled(r)` (`optimizer.go:1111`).
    #[must_use]
    pub fn is_logical_rule_disabled(&self, rule: RuleId) -> bool {
        self.names.iter().any(|name| name == rule.name())
    }
}

/// What a rule body needs that Go reads off `p.SCtx()`.
///
/// Go's rules reach the session through the plan; this enum tree has no
/// back-pointer to a session, so the context is threaded explicitly. Every
/// field here is a Go `sessionctx` read, named.
pub struct RuleContext<'a> {
    /// Go `sessionVars.PlanID`, for the operators a rule CREATES —
    /// `logicalop.AddSelection`'s `LogicalSelection` and `Conds2TableDual`'s
    /// `LogicalTableDual`.
    pub allocator: &'a PlanIdAllocator,
    /// Go `SCtx().GetExprCtx()`'s construction half; see
    /// [`FunctionBuilder`].
    pub builder: &'a dyn FunctionBuilder,
    /// Go `SCtx().GetSessionVars().StmtCtx.UseCache`, which
    /// `MaybeOverOptimized4PlanCache` gates on.
    pub use_plan_cache: bool,
    /// Go `SCtx().GetSessionVars().AllowDeriveTopN`, which
    /// `BaseLogicalPlan.DeriveTopN` (`base_logical_plan.go:169`) gates its
    /// whole recursion on.
    pub allow_derive_topn: bool,
    /// Go `DefaultDisabledLogicalRulesList`.
    pub disabled_rules: DisabledLogicalRules,
}

/// Go `base.LogicalOptRule` (`base/rule_base.go`).
///
/// # Why the plan comes back on the error arm
///
/// See this module's header: Go's `(LogicalPlan, bool, error)` keeps the plan
/// alive across the error because it is a pointer. `Err((LogicalPlan,
/// PlanError))` is the by-value form of the same guarantee, and rules are
/// required to return a STRUCTURALLY VALID plan on that arm — the tree as it
/// stood when the failure was recorded, fully rebuilt, never a fragment.
pub trait LogicalOptRule {
    /// Go `Optimize(ctx, lp) (base.LogicalPlan, bool, error)`. The `bool` is
    /// Go's `planChanged`.
    ///
    /// # Errors
    ///
    /// Returns the plan alongside the error; see the trait's header.
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)>;

    /// Go `Name() string`.
    fn name(&self) -> &'static str;
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
        if flag & rule_flags[i] == 0 || ctx.disabled_rules.is_logical_rule_disabled(rule) {
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
            if plan_changed && ctx.disabled_rules.is_logical_rule_disabled(interaction) {
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

// ***** logicalop.AddSelection and its predicate-simplification dependency *****

/// Go `ruleutil.ApplyPredicateSimplification(sctx, predicates,
/// propagateConstant=false, filter=nil)`, the SUBSET that
/// `logicalop.AddSelection` needs.
///
/// # Why a subset lands here and not in a `PredicateSimplification` batch
///
/// Go schedules `rule.PredicateSimplification` SEVEN positions after
/// `PPDSolver`, yet `logicalop.AddSelection` (`logical_plans_misc.go:85`) —
/// which predicate pushdown calls on every child — calls
/// `ruleutil.ApplyPredicateSimplification`. That is not a phase-ordering
/// statement: `rule/util/misc.go:214` declares the symbol as a FUNCTION
/// POINTER that `rule_init.go`'s `init()` fills in, purely so `logicalop` does
/// not import `rule` and create a package cycle. The dependency is real and
/// immediate; only the Go linkage is indirect.
///
/// So the pushdown-visible half lands here, and the `PredicateSimplification`
/// RULE (Go #21) still belongs to a later batch, which will complete this
/// function rather than replace it.
///
/// # What this subset does, and what it does not
///
/// Ported, from `applyPredicateSimplificationHelper`
/// (`rule_predicate_simplification.go:199`), in Go's order:
/// * `PushDownNot` over each predicate;
/// * `constraint.DeleteTrueExprs` — a predicate that is a constant TRUE is
///   dropped, since `WHERE TRUE` filters nothing.
///
/// NOT ported, each blocked on a named Go symbol that is not transcreated:
/// * `expression.PropagateConstant` / `PropagateConstantForJoin` — this is the
///   `propagateConstant` half the parameter name refers to, and it is exactly
///   what Go's #11 `ConstantPropagationSolver` and #21 own. `AddSelection`
///   passes `propagateConstant=true`, so this subset is a NARROWING of that
///   call, not an implementation of it.
/// * `shortCircuitLogicalConstants`, `mergeInAndNotEQLists`,
///   `removeRedundantORBranch`, `pruneEmptyORBranches`
///   (`rule_predicate_simplification.go`).
///
/// The narrowing direction is safe: every omitted step only ever REMOVES or
/// weakens predicates, so keeping a predicate that Go would have simplified
/// away yields a plan that filters at least as much, never less.
#[must_use]
pub fn apply_predicate_simplification(
    ctx: &RuleContext<'_>,
    predicates: Vec<Expression>,
) -> Vec<Expression> {
    if predicates.is_empty() {
        return predicates;
    }
    predicates
        .iter()
        .map(|expr| push_down_not(expr, ctx.builder))
        .filter(|expr| !is_const_true(expr))
        .collect()
}

/// Go `constraint.DeleteTrueExprs`'s per-expression test, for the constants
/// this crate can decide without an evaluation context.
///
/// A `Constant` with a deferred expression or a parameter marker is NEVER
/// treated as true, because its value is not known at plan time — that is
/// Go's `ConstLevel` guard, conservatively.
fn is_const_true(expr: &Expression) -> bool {
    let Expression::Constant(constant) = expr else {
        return false;
    };
    if constant.deferred_expr.is_some() || constant.param_marker.is_some() {
        return false;
    }
    matches!(constant.value, tidb_datatype::Datum::Int(v) if v != 0)
}

/// Go `IsConstFalse(sc, cond)`'s decidable half: a constant that converts to
/// boolean false.
fn is_const_false(expr: &Expression) -> bool {
    let Expression::Constant(constant) = expr else {
        return false;
    };
    if constant.deferred_expr.is_some() || constant.param_marker.is_some() {
        return false;
    }
    match &constant.value {
        tidb_datatype::Datum::Null => true,
        tidb_datatype::Datum::Int(v) => *v == 0,
        tidb_datatype::Datum::UInt(v) => *v == 0,
        _ => false,
    }
}

/// Go `logicalop.Conds2TableDual(p, conds)`
/// (`operator/logicalop/expression_util.go:24`), over real
/// [`Expression`]s.
///
/// `schema` is Go's `p.Schema()`, which the dual inherits so the parent's
/// column references still resolve.
///
/// Returns `None` when Go returns `nil`, i.e. the caller keeps the plan it
/// has. [`crate::condition_to_dual`] carries the same decision over normalized
/// truth tokens and is KEPT — `difftests/planner-tests` consumes it from
/// outside this crate.
#[must_use]
pub fn conds_to_table_dual(
    ctx: &RuleContext<'_>,
    conds: &[Expression],
    schema: Option<&Schema>,
    query_block_offset: i32,
) -> Option<LogicalPlan> {
    if conds.is_empty() {
        return None;
    }
    let over_optimized = maybe_over_optimized_4_plan_cache(ctx.use_plan_cache, conds);
    let make_dual = || {
        let mut base = BaseLogicalPlan::new(ctx.allocator, "TableDual", query_block_offset);
        base.base.set_schema(schema.cloned());
        Some(LogicalPlan::TableDual(LogicalTableDual::new(base, 0)))
    };
    if conds.iter().any(is_const_null) {
        return if over_optimized { None } else { make_dual() };
    }
    if conds.len() != 1 || over_optimized {
        return None;
    }
    if is_const_false(&conds[0]) {
        return make_dual();
    }
    None
}

/// Go `logicalop.AddSelection(p, child, conditions, chIdx)`
/// (`logical_plans_misc.go:85`), returning the node to install as the child
/// instead of writing it through `p.Children()[chIdx]`.
///
/// The four early returns are Go's, in Go's order:
/// 1. no conditions — the child goes back untouched;
/// 2. simplification consumed every condition — likewise;
/// 3. the child is already an empty `LogicalTableDual` — nothing can filter
///    fewer than zero rows;
/// 4. the conditions are constant false/null — the child becomes a dual.
///
/// Otherwise a `LogicalSelection` is built over the child.
#[must_use]
pub fn add_selection(
    ctx: &RuleContext<'_>,
    child: LogicalPlan,
    conditions: Vec<Expression>,
    query_block_offset: i32,
) -> LogicalPlan {
    if conditions.is_empty() {
        return child;
    }
    let conditions = apply_predicate_simplification(ctx, conditions);
    if conditions.is_empty() {
        return child;
    }
    if let LogicalPlan::TableDual(dual) = &child {
        if dual.row_count == 0 {
            return child;
        }
    }
    if let Some(dual) = conds_to_table_dual(ctx, &conditions, child.schema(), query_block_offset) {
        child.dismantle();
        return dual;
    }
    let base = BaseLogicalPlan::new(ctx.allocator, "Selection", query_block_offset);
    let mut selection = LogicalPlan::Selection(LogicalSelection::new(base, conditions));
    selection.set_children(vec![child]);
    selection
}

// ***** the four ported rules *****

/// Go `rule.ColumnPruner` (`rule/rule_column_pruning.go:31`), Go rules #1 and
/// #29.
///
/// Go's body is four lines: `lp.PruneColumns(slices.Clone(lp.Schema().Columns))`
/// plus an `intest` assertion. The assertion —
/// `noUnexpectedZeroColumnSchema` — is [`crate::column_pruning`], which is
/// KEPT as its own module because `difftests/planner-tests` consumes it.
#[derive(Debug)]
pub struct ColumnPruner;

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
        let (plan, failure) = super::rewrite::prune_columns(ctx, plan, root_cols);
        match failure {
            // Go's `planChanged` is hard-coded `false` in this rule.
            None => Ok((plan, false)),
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
        Ok((super::rewrite::build_key_info_portal(plan), false))
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
            super::rewrite::predicate_push_down(ctx, plan, Vec::new());
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
        _ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        Ok((super::rewrite::push_down_topn(plan, None), false))
    }

    fn name(&self) -> &'static str {
        "topn_push_down"
    }
}
