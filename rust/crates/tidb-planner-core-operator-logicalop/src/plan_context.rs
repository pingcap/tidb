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

//! What an operator reads off Go's `p.SCtx()` while a logical rule runs over
//! it, and the operator-side helpers Go keeps in `logicalop`: the rule
//! context (Go `base.PlanContext` with the hooks `core` and the rule package
//! fill -- `utilfuncp`'s, `ruleutil`'s), `base.LogicalOptRule`, and
//! `logicalop.AddSelection` / `Conds2TableDual`. The rule list and the rules
//! themselves are the planner's `rule` module (Go `core` and `core/rule`).

use tidb_planner_core_base::PlanCacheMarker;
use tidb_expr::expr_util::builder::FunctionBuilder;
use tidb_expr::expr_util::predicates::{is_const_null, maybe_over_optimized_4_plan_cache};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use tidb_planner_core_base::{PlanError, PlanIdAllocator};

use super::selection::LogicalSelection;
use super::table_dual::LogicalTableDual;
use super::{BaseLogicalPlan, LogicalPlan};

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

    /// Whether `name` is one of the disabled rule names.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.names.iter().any(|disabled| disabled == name)
    }
}

/// What a rule body needs that Go reads off `p.SCtx()`.
///
/// Go's rules reach the session through the plan; this enum tree has no
/// back-pointer to a session, so the context is threaded explicitly. Every
/// field here is a Go `sessionctx` read, named.
pub struct RuleContext<'a> {
    /// Go `SessionVars.RiskRangeSkewRatio`, `RiskEqSkewRatio`, and
    /// `GetOptObjective()` inputs used by histogram cardinality estimation.
    pub estimator_options: tidb_planner_cardinality::row_count_estimator::EstimatorOptions,
    /// Go `sessionVars.PlanID`, for the operators a rule CREATES —
    /// `logicalop.AddSelection`'s `LogicalSelection` and `Conds2TableDual`'s
    /// `LogicalTableDual`.
    pub allocator: &'a PlanIdAllocator,
    /// Go `SessionVars.AllocPlanColumnID()`, used by rules that append a
    /// computed expression to an existing projection.
    pub column_allocator: &'a tidb_planner_core_base::ColumnIdAllocator,
    /// Go `SCtx().GetExprCtx()`'s construction half; see
    /// [`FunctionBuilder`].
    pub builder: &'a dyn FunctionBuilder,
    /// The statement evaluation context used by Go's constraint conversions.
    pub eval_context: &'a dyn tidb_exprctx::Columns,
    /// Go `SCtx().GetSessionVars().StmtCtx.UseCache`, which
    /// `MaybeOverOptimized4PlanCache` gates on.
    pub use_plan_cache: bool,
    /// Go `StmtCtx.SetSkipPlanCache`, used when simplification consumes a
    /// mutable constant and would otherwise freeze its current value.
    pub plan_cache_marker: Option<&'a dyn PlanCacheMarker>,
    /// Go `SCtx().GetSessionVars().AllowDeriveTopN`, which
    /// `BaseLogicalPlan.DeriveTopN` (`base_logical_plan.go:169`) gates its
    /// whole recursion on.
    pub allow_derive_topn: bool,
    /// Go `DefaultDisabledLogicalRulesList`.
    pub disabled_rules: DisabledLogicalRules,
    /// Published expression policy, read before datasource range derivation.
    pub expr_pushdown_blacklist: tidb_expr::infer_pushdown::ExprPushDownBlacklist,
    /// Go's domain `StatsHandle`, reached at this rule's exact position.
    pub statistics_load: Option<&'a dyn StatisticsLoadRequester>,
    /// Go's partition metadata/ranger access at the partition-processor rule.
    pub partition_pruning: Option<&'a dyn PartitionPruning>,
    /// Go `SessionVars.OptIndexPruneThreshold`.
    pub opt_index_prune_threshold: i32,
    /// Go `SessionVars.OptPrefixIndexSingleScan`, read while deriving access paths.
    pub opt_prefix_index_single_scan: bool,
    /// Go EnableIndexMerge, used before physical property search.
    pub index_merge_enabled: bool,
    /// Go `StmtCtx.NoIndexMergeHint`, which overrides `USE_INDEX_MERGE`.
    pub no_index_merge_hint: bool,
    /// Go `StmtCtx.InExplainStmt`: a `PushDownContext` built from the session
    /// sends its warnings to the statement only under EXPLAIN (the extra
    /// warnings otherwise).
    pub in_explain_stmt: bool,
    /// Go `SessionVars.RangeMaxSize`; zero means unlimited.
    pub range_max_size: i64,
    /// Go SessionVars.SelectivityFactor for uncovered or partially covered predicates.
    pub selectivity_factor: f64,
    /// The statement's shared construction-time ranger fallback handler.
    pub range_fallback_handler: Option<&'a tidb_context::RangeFallbackHandler>,
    /// Go `SessionVars.AlwaysKeepJoinKey`.
    pub always_keep_join_key: bool,
    /// Go `SessionVars.EnableUnsafeSubstitute`.
    pub enable_unsafe_substitute: bool,
    /// Go `SessionVars.EnableSemiJoinRewrite`.
    pub enable_semi_join_rewrite: bool,
    /// Go `SessionVars.OptimizerEnableNAAJ` (`@@tidb_enable_null_aware_anti_join`,
    /// default ON): gates `LogicalJoin.updateEQCond`'s NAAJ conversion.
    pub enable_null_aware_anti_join: bool,
    /// Go `SessionVars.EnableNoDecorrelateInSelect`.
    pub enable_no_decorrelate_in_select: bool,
    /// Go fix control 45822 (default true): `LogicalApply.PruneColumns` may
    /// drop a left outer apply whose inner columns nothing uses.
    pub allow_eliminate_apply: bool,
    /// Go `SessionVars.TiDBOptJoinReorderThreshold`.
    pub join_reorder_threshold: i32,
    /// Go `SessionVars.RiskGroupNDVSkewRatio` for join and aggregation stats.
    pub group_ndv_skew_ratio: f64,
    /// Go SessionVars.RiskScaleNDVSkewRatio for filtered/scaled profiles.
    pub scale_ndv_skew_ratio: f64,
    /// Go `SessionVars.AllowAggPushDown` (`@@tidb_opt_agg_push_down`, default
    /// off): gates the aggregation-push-down rule's join and union arms; the
    /// projection-crossing arm runs regardless, as in Go.
    pub allow_agg_push_down: bool,
    /// Go `SessionVars.TiDBOptEnableAdvancedJoinReorder`.
    pub advanced_join_reorder: bool,
    /// Go `SessionVars.CartesianJoinOrderThreshold`.
    pub cartesian_join_order_threshold: f64,
    /// Go `SessionVars.TiDBOptJoinReorderThroughProj`.
    pub join_reorder_through_proj: bool,
    /// Go `SessionVars.TiDBOptJoinReorderThroughSel`.
    pub join_reorder_through_sel: bool,
    /// Go `SessionVars.EnableOuterJoinReorder`.
    pub outer_join_reorder: bool,
    /// Go `SessionVars.EnableAdvancedJoinHint`.
    pub advanced_join_hint: bool,
    /// Go `StmtCtx.SetHintWarning`.
    pub hint_warning_sink: Option<&'a dyn HintWarningSink>,
    /// Go `StmtCtx.GetUsedStatsInfo(true)`'s `RecordUsedItemStatus`, reached
    /// from the statistics derivation (`recordUsedItemStatsStatus`).
    pub used_stats_recorder: Option<&'a dyn UsedStatsRecorder>,
    /// Go `utilfuncp.DoOptimize` as `LogicalCTE.DeriveStats` reaches it: the
    /// driver-owned whole-query optimizer a CTE class runs the first time its
    /// statistics are asked for (`logical_cte.go:172`).
    pub cte_optimizer: Option<&'a dyn CteOptimizer>,
    /// Go `utilfuncp.DeriveStats4DataSource`: core's `deriveStats4DataSource`.
    pub data_source_stats: &'a dyn super::rewrite::DataSourceStatsDeriver,
    /// Go `ruleutil.ApplyPredicateSimplification` /
    /// `ApplyPredicateSimplificationForJoin`, which the rule package fills.
    pub predicate_simplifier: &'a dyn PredicateSimplifier,
}

/// Go `ruleutil.ApplyPredicateSimplification` and
/// `ApplyPredicateSimplificationForJoin` (`rule/util/misc.go`): function
/// variables the rule package fills (`rule/rule_init.go`), so an operator's
/// predicate pushdown reaches the rule's body without importing it.
pub trait PredicateSimplifier {
    /// Go `applyPredicateSimplification`.
    fn apply_predicate_simplification(
        &self,
        ctx: &RuleContext<'_>,
        predicates: Vec<Expression>,
        propagate_constant: bool,
        valid: Option<&dyn Fn(&Expression) -> bool>,
    ) -> Vec<Expression>;

    /// Go `applyPredicateSimplificationForJoin`.
    fn apply_predicate_simplification_for_join(
        &self,
        ctx: &RuleContext<'_>,
        predicates: Vec<Expression>,
        left_schema: &Schema,
        right_schema: &Schema,
        propagate_constant: bool,
        valid: Option<&dyn Fn(&Expression) -> bool>,
    ) -> Vec<Expression>;
}

/// Go `LogicalCTE.DeriveStats`' lazy `DoOptimize` of a CTE class: the seed
/// (with the DNF of every reference's recorded predicates) and, for a
/// recursive CTE, the recursive part are optimized into logical + physical
/// plans the FIRST time the class's statistics are needed, and never again.
pub trait CteOptimizer {
    /// Fill `class.seed_part_{logical,physical}_plan` (and the recursive
    /// pair) when the seed physical plan is still absent. Go publishes the
    /// optimized seed's statistics into the calling reference's `SeedStat`
    /// BEFORE the recursive part is optimized, so its `CTETable` sees them.
    fn optimize_cte(
        &self,
        class: &std::rc::Rc<std::cell::RefCell<super::cte::CteClass>>,
        seed_stat: Option<&std::rc::Rc<std::cell::RefCell<tidb_planner_property::stats_info::StatsInfo>>>,
        context: &RuleContext<'_>,
    ) -> Result<(), PlanError>;
}

/// The optimizer-hint warning side effect exposed by Go's statement context.
pub trait HintWarningSink {
    /// Go `StmtCtx.SetHintWarning`: a planner `ErrInternal` (1815).
    fn set_hint_warning(&self, message: &str);

    /// Go `StmtCtx.SetHintWarningFromError` / `AppendWarning` of an error the
    /// caller built: the warning carries that error's code, ErrUnknown (1105)
    /// for a plain `fmt.Errorf` / `errors.NewNoStackError`.
    fn set_hint_warning_from_error(&self, code: u16, message: &str);
}


/// Go `recordUsedItemStatsStatus` (`cardinality/trace.go`): the statement's
/// ledger of the column and index statistics an estimate read.
pub trait UsedStatsRecorder {
    /// Records the statistics item (`item_id`, a column or index id) the
    /// estimate of `physical_table_id` just read. `in_pseudo_table` says
    /// whether a fresh `PseudoTable` holds the item (a public, non-hidden
    /// column or a public index).
    fn record_used_item(
        &self,
        physical_table_id: i64,
        table_name: &str,
        in_pseudo_table: bool,
        item_id: i64,
        is_index: bool,
    );
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

// ***** logicalop.AddSelection and its predicate-simplification dependency *****

/// Go `ruleutil.ApplyPredicateSimplification`.
#[must_use]
pub fn apply_predicate_simplification(
    ctx: &RuleContext<'_>,
    predicates: Vec<Expression>,
    propagate_constant: bool,
    valid: Option<&dyn Fn(&Expression) -> bool>,
) -> Vec<Expression> {
    ctx.predicate_simplifier
        .apply_predicate_simplification(ctx, predicates, propagate_constant, valid)
}

/// Go `ruleutil.ApplyPredicateSimplificationForJoin(...,
/// propagateConstant=true, ...)` for inner and semi joins.
///
/// The important distinction from [`apply_predicate_simplification`] is
/// `expression.PropagateConstantForJoin`: equality keys form equivalence
/// classes, and every deterministic predicate over one member is reproduced
/// for the other members. Thus `a = b AND b = 1` yields `a = 1` while retaining
/// `a = b` as a physical join key (`tidb_opt_always_keep_join_key` defaults to
/// ON in Go).
///
/// Go's solver also substitutes constants repeatedly to fold expressions that
/// become constant. That affects simplification quality but not condition
/// attribution; this planner retains those original conditions and adds the
/// same equality-derived predicates. Keeping an unfurled original is
/// conservative, while the derived predicates are what let predicate
/// pushdown, range building, cardinality estimation and physical join
/// enumeration see the same constraints as Go.
#[must_use]
pub fn apply_predicate_simplification_for_join(
    ctx: &RuleContext<'_>,
    predicates: Vec<Expression>,
    left_schema: &Schema,
    right_schema: &Schema,
    propagate_constant: bool,
    valid: Option<&dyn Fn(&Expression) -> bool>,
) -> Vec<Expression> {
    ctx.predicate_simplifier.apply_predicate_simplification_for_join(
        ctx,
        predicates,
        left_schema,
        right_schema,
        propagate_constant,
        valid,
    )
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
/// has.
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
    let conditions = apply_predicate_simplification(ctx, conditions, true, None);
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

/// The domain statistics handle reached by Go
/// `CollectPredicateColumnsPoint.Optimize`.
pub trait StatisticsLoadRequester {
    /// Requests the collected items before later logical rules and physical
    /// enumeration read their statistics.
    fn request(&self, usage: &tidb_stats::table_statistics::ColumnStatsUsage) -> Result<(), PlanError>;

    /// Waits at Go `SyncWaitStatsLoadPoint`, after the intervening logical
    /// rules have had a chance to run while the workers load in parallel.
    fn wait(&self) -> Result<(), PlanError>;

    /// Attach the loaded snapshot before join reorder derives path costs.
    /// An estimation failure retains the plan and its original error, as
    /// required by the logical-rule error boundary.
    fn initialize(&self, plan: LogicalPlan) -> Result<LogicalPlan, (LogicalPlan, PlanError)> {
        Ok(plan)
    }
}

/// The ranger-backed half of Go's partition processor. The planner owns the
/// tree rewrite; the catalog implementation owns partition expressions and
/// therefore answers which definition ordinals survive.
pub trait PartitionPruning {
    /// Go `PartitionProcessor.prune`, reduced to its surviving definition
    /// ordinals in table-definition order.
    fn partition_indices(&self, source: &super::data_source::DataSource) -> Result<Vec<usize>, PlanError>;

    /// Go's static pruner `PlanID.Add(1)`s before it parses the partition
    /// expression (`generateHashPartitionExpr`, `MakePartitionByFnCol`); the
    /// dynamic-mode access object does not run that code. The default spends
    /// nothing.
    fn spend_partition_expression_plan_id(&self, _source: &super::data_source::DataSource) {}
}

/// The statement context is where Go's `StmtCtx.SetHintWarning` writes.
impl HintWarningSink for tidb_sessionctx::StmtContext {
    fn set_hint_warning(&self, message: &str) {
        self.append_warning_parts(1815, message);
    }

    fn set_hint_warning_from_error(&self, code: u16, message: &str) {
        self.append_warning_parts(code, message);
    }
}
