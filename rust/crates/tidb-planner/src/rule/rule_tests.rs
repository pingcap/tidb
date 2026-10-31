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

//! WRITTEN tests (not transcreated from Go's test files) for the logical
//! optimization rule driver, the owned-rewrite fold, and the per-operator arms
//! of predicate pushdown, column pruning and TopN pushdown.
//!
//! Go's own coverage for these rules is end-to-end plan-string comparison
//! (`planner/core/casetest`), which needs a parser, a catalogue and a session
//! — none reachable from this crate. These tests instead assert the SEMANTIC
//! decisions directly on hand-built plans: which node ends up where, which
//! predicate travels how far, which column survives.

use tidb_planner_core_base::PlanCacheMarker;
use std::cell::RefCell;

use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expression_aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expr_util::builder::PreservingFunctionBuilder;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;

use tidb_planner_core_base::LogicalJoinType;
use tidb_planner_core_base::PlanIdAllocator;

use tidb_planner_core_operator_logicalop::aggregation::{LogicalAggregation, AGG_FUNC_COUNT, AGG_FUNC_FIRST_ROW};
use tidb_planner_core_operator_logicalop::apply::LogicalApply;
use tidb_planner_core_operator_logicalop::data_source::DataSource;
use tidb_planner_core_operator_logicalop::fold::{fold_owned, Descend, OwnedRewrite};
use tidb_planner_core_operator_logicalop::join::LogicalJoin;
use tidb_planner_core_operator_logicalop::limit::LogicalLimit;
use tidb_planner_core_operator_logicalop::projection::LogicalProjection;
use crate::rule;
use super::{logical_optimize, PpdSolver, PushDownTopNOptimizer, RuleId, OPT_RULE_FLAGS, OPT_RULE_LIST};
use tidb_planner_core_rule::{flags, no_unexpected_zero_column_schema, BuildKeySolver, ColumnPruner};
use tidb_planner_core_operator_logicalop::plan_context::{add_selection, DisabledLogicalRules, LogicalOptRule, RuleContext};
use tidb_planner_core_operator_logicalop::plan_context::PartitionPruning;
use tidb_planner_core_rule::rule_partition_processor::PartitionProcessor;
use tidb_planner_core_operator_logicalop::selection::LogicalSelection;
use tidb_planner_core_operator_logicalop::sort::LogicalSort;
use tidb_planner_core_operator_logicalop::topn::LogicalTopN;
use tidb_planner_core_operator_logicalop::{BaseLogicalPlan, LogicalPartitionUnionAll, LogicalPlan, LogicalUnionScan};

pub(crate) const TEST_BUILDER: PreservingFunctionBuilder = PreservingFunctionBuilder;

/// A [`RuleContext`] over a caller-owned allocator, for tests elsewhere in the
/// crate as well as this file.
pub(crate) fn test_context(allocator: &PlanIdAllocator) -> RuleContext<'_> {
    static COLUMN_ALLOCATOR: tidb_planner_core_base::ColumnIdAllocator =
        tidb_planner_core_base::ColumnIdAllocator::new();
    RuleContext {
        estimator_options: Default::default(),
        allocator,
        column_allocator: &COLUMN_ALLOCATOR,
        builder: &TEST_BUILDER,
        eval_context: &tidb_exprctx::NoColumns,
        use_plan_cache: false,
        plan_cache_marker: None,
        // Go's `AllowDeriveTopN` defaults ON in `sessionVars`.
        allow_derive_topn: true,
        expr_pushdown_blacklist: Default::default(),
        disabled_rules: DisabledLogicalRules::default(),
        statistics_load: None,
        partition_pruning: None,
        opt_index_prune_threshold: 20,
        opt_prefix_index_single_scan: true,
        index_merge_enabled: true,
        no_index_merge_hint: false,
        in_explain_stmt: false,
        range_max_size: 0,
        selectivity_factor: tidb_planner_core_cost::SELECTION_FACTOR,
        range_fallback_handler: None,
        always_keep_join_key: true,
        enable_unsafe_substitute: false,
        enable_semi_join_rewrite: false,
        // Go's `OptimizerEnableNAAJ` defaults ON in `sessionVars`.
        enable_null_aware_anti_join: true,
        enable_no_decorrelate_in_select: false,
        allow_eliminate_apply: true,
        join_reorder_threshold: 0,
        group_ndv_skew_ratio: tidb_vardef::defaults::DEF_OPT_RISK_GROUP_NDV_SKEW_RATIO,
        scale_ndv_skew_ratio: tidb_planner_cardinality::derive_stats::DEF_SCALE_NDV_SKEW_RATIO,
        allow_agg_push_down: false,
        advanced_join_reorder: true,
        cartesian_join_order_threshold: 0.0,
        join_reorder_through_proj: false,
        join_reorder_through_sel: false,
        outer_join_reorder: true,
        advanced_join_hint: true,
        hint_warning_sink: None,
        used_stats_recorder: None,
        cte_optimizer: None,
        data_source_stats: &crate::stats::CoreDataSourceStats,
        predicate_simplifier: &tidb_planner_core_rule::rule_predicate_simplification::RulePredicateSimplifier,
        access_path_resolver: &crate::access_path::CoreAccessPathResolver,
    }
}

fn int_type() -> FieldType {
    FieldType::new(FieldTypeCode::Long)
}

fn column(id: i64) -> Column {
    let mut col = Column::default();
    col.unique_id = id;
    col.id = id;
    col.ret_type = Some(int_type());
    col
}

fn col_expr(id: i64) -> Expression {
    Expression::Column(column(id))
}

fn schema_of(ids: &[i64]) -> Schema {
    Schema::new(ids.iter().copied().map(column).collect())
}

/// `col(id) = <constant>`, the shape `extractOnCondition` files onto one side.
fn eq_const(id: i64, value: i64) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("eq"),
        int_type(),
        vec![
            col_expr(id),
            Expression::Constant(Constant::new(Datum::Int(value), int_type())),
        ],
    ))
}

fn not_ne_const(id: i64, value: i64) -> Expression {
    let not_equal = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("ne"),
        int_type(),
        vec![
            col_expr(id),
            Expression::Constant(Constant::new(Datum::Int(value), int_type())),
        ],
    ));
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("not"),
        int_type(),
        vec![not_equal],
    ))
}

/// `col(left) = col(right)`, the shape that becomes an `EqualCondition`.
fn eq_cols(left: i64, right: i64) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("eq"),
        int_type(),
        vec![col_expr(left), col_expr(right)],
    ))
}

fn gt_const(id: i64, value: i64) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("gt"),
        int_type(),
        vec![
            col_expr(id),
            Expression::Constant(Constant::new(Datum::Int(value), int_type())),
        ],
    ))
}

fn const_true() -> Expression {
    Expression::Constant(Constant::new_one())
}

fn const_false() -> Expression {
    Expression::Constant(Constant::new_zero())
}

fn base(allocator: &PlanIdAllocator, tp: &str, schema: Option<Schema>) -> BaseLogicalPlan {
    let mut base = BaseLogicalPlan::new(allocator, tp, 0);
    base.base.set_schema(schema);
    base
}

fn data_source(allocator: &PlanIdAllocator, ids: &[i64]) -> LogicalPlan {
    let source = DataSource::new(base(allocator, "DataSource", Some(schema_of(ids))), 1, "t");
    LogicalPlan::DataSource(source)
}

fn selection_over(
    allocator: &PlanIdAllocator,
    conditions: Vec<Expression>,
    child: LogicalPlan,
) -> LogicalPlan {
    let mut node = LogicalPlan::Selection(LogicalSelection::new(
        base(allocator, "Selection", None),
        conditions,
    ));
    node.set_children(vec![child]);
    node
}

fn unary(
    allocator: &PlanIdAllocator,
    tp: &str,
    node: LogicalPlan,
    child: LogicalPlan,
) -> LogicalPlan {
    let _ = (allocator, tp);
    let mut node = node;
    node.set_children(vec![child]);
    node
}

// ***** the fold helper *****

/// Counts every node, rebuilding the tree by value, to prove the fold is a
/// real owned rewrite and not a read-only walk.
struct CountingRewrite {
    seen: usize,
}

impl OwnedRewrite for CountingRewrite {
    type Down = usize;
    type Up = usize;

    fn descend(&mut self, node: &mut LogicalPlan, depth: usize) -> Descend<usize, usize> {
        Descend::Children(vec![depth + 1; node.children().len()])
    }

    fn ascend(&mut self, node: LogicalPlan, child_ups: Vec<usize>) -> (LogicalPlan, usize) {
        self.seen += 1;
        let deepest = child_ups.into_iter().max().unwrap_or(0);
        (node, deepest + 1)
    }
}

/// Replaces every `LogicalSelection` by its child, which is the sharpest thing
/// an owned rewrite does: the parked parent is DISCARDED and its child moved
/// up in its place.
struct DropSelections;

impl OwnedRewrite for DropSelections {
    type Down = ();
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, (): ()) -> Descend<(), ()> {
        Descend::Children(vec![(); node.children().len()])
    }

    fn ascend(&mut self, mut node: LogicalPlan, _ups: Vec<()>) -> (LogicalPlan, ()) {
        if matches!(node, LogicalPlan::Selection(_)) {
            let mut children = node.base_mut().take_children();
            if !children.is_empty() {
                let child = children.remove(0);
                node.dismantle();
                return (child, ());
            }
        }
        (node, ())
    }
}

fn deep_chain(allocator: &PlanIdAllocator, depth: usize) -> LogicalPlan {
    let mut node = data_source(allocator, &[1]);
    for _ in 0..depth {
        node = selection_over(allocator, vec![eq_const(1, 7)], node);
    }
    node
}

#[test]
fn fold_owned_visits_every_node_in_post_order() {
    let allocator = PlanIdAllocator::new();
    let tree = deep_chain(&allocator, 3);
    let mut rewrite = CountingRewrite { seen: 0 };
    let (tree, height) = fold_owned(&mut rewrite, tree, 0);
    assert_eq!(rewrite.seen, 4);
    assert_eq!(height, 4);
    assert_eq!(tree.plan_count(), 4);
    tree.dismantle();
}

#[test]
fn fold_owned_survives_a_depth_that_would_overflow_a_recursive_rewrite() {
    // The module header measures a recursive `match` walk aborting by 50,000
    // levels. 60,000 is past that, and the explicit stack does not care.
    let allocator = PlanIdAllocator::new();
    let tree = deep_chain(&allocator, 60_000);
    let mut rewrite = CountingRewrite { seen: 0 };
    let (tree, height) = fold_owned(&mut rewrite, tree, 0);
    assert_eq!(rewrite.seen, 60_001);
    assert_eq!(height, 60_001);
    tree.dismantle();
}

#[test]
fn fold_owned_lets_a_rewrite_replace_a_node_by_its_child() {
    let allocator = PlanIdAllocator::new();
    let tree = deep_chain(&allocator, 5);
    let (tree, ()) = fold_owned(&mut DropSelections, tree, ());
    assert_eq!(tree.plan_count(), 1);
    assert!(matches!(tree, LogicalPlan::DataSource(_)));
}

#[test]
fn fold_owned_stop_leaves_the_children_untouched() {
    struct StopAtTop;
    impl OwnedRewrite for StopAtTop {
        type Down = ();
        type Up = usize;
        fn descend(&mut self, _node: &mut LogicalPlan, (): ()) -> Descend<(), usize> {
            Descend::Stop(99)
        }
        fn ascend(&mut self, node: LogicalPlan, _ups: Vec<usize>) -> (LogicalPlan, usize) {
            (node, 0)
        }
    }
    let allocator = PlanIdAllocator::new();
    let tree = deep_chain(&allocator, 3);
    let (tree, up) = fold_owned(&mut StopAtTop, tree, ());
    assert_eq!(up, 99);
    assert_eq!(tree.plan_count(), 4, "children were not rewritten away");
    tree.dismantle();
}

// ***** the rule list and the driver *****

#[test]
fn the_rule_list_and_the_flag_list_are_index_aligned() {
    assert_eq!(OPT_RULE_LIST.len(), 35);
    assert_eq!(OPT_RULE_FLAGS.len(), 35);
    assert_eq!(OPT_RULE_LIST[1], RuleId::ColumnPruner);
    assert_eq!(OPT_RULE_FLAGS[1], flags::PRUNE_COLUMNS);
    assert_eq!(OPT_RULE_LIST[13], RuleId::PpdSolver);
    assert_eq!(OPT_RULE_FLAGS[13], flags::PREDICATE_PUSH_DOWN);
    assert_eq!(OPT_RULE_LIST[21], RuleId::PushDownTopNOptimizer);
    assert_eq!(OPT_RULE_FLAGS[21], flags::PUSH_DOWN_TOPN);
    assert_eq!(OPT_RULE_LIST[29], RuleId::ColumnPrunerAgain);
    assert_eq!(OPT_RULE_FLAGS[29], flags::PRUNE_COLUMNS_AGAIN);
}

#[test]
fn the_flag_bit_order_deliberately_differs_from_the_execution_order() {
    // `FlagFullTextIndexResolveWhere` is bit 31 but runs 12th, and
    // `FlagResolveExpand` is bit 30 but runs last. One table cannot express
    // both, which is why there are two.
    assert_eq!(OPT_RULE_LIST[11], RuleId::FullTextIndexResolverWhere);
    assert_eq!(OPT_RULE_FLAGS[11], 1 << 31);
    assert_eq!(OPT_RULE_LIST[34], RuleId::ResolveExpand);
    assert_eq!(OPT_RULE_FLAGS[34], 1 << 30);
}

#[test]
fn set_predicate_push_down_flag_only_sets_its_bit() {
    assert_eq!(
        tidb_planner_core_rule::set_predicate_push_down_flag(0),
        flags::PREDICATE_PUSH_DOWN
    );
    let existing = flags::PRUNE_COLUMNS | flags::PREDICATE_PUSH_DOWN;
    assert_eq!(tidb_planner_core_rule::set_predicate_push_down_flag(existing), existing);
}

#[test]
fn a_disabled_rule_is_skipped_by_name() {
    let disabled = DisabledLogicalRules::from_names(["predicate_push_down"]);
    assert!(crate::rule::is_logical_rule_disabled(&disabled, RuleId::PpdSolver));
    assert!(!crate::rule::is_logical_rule_disabled(&disabled, RuleId::BuildKeySolver));
    // Go names both `ColumnPruner` entries identically, so disabling one
    // disables both.
    let disabled = DisabledLogicalRules::from_names(["column_prune"]);
    assert!(crate::rule::is_logical_rule_disabled(&disabled, RuleId::ColumnPruner));
    assert!(crate::rule::is_logical_rule_disabled(&disabled, RuleId::ColumnPrunerAgain));
}

#[test]
fn the_interaction_rule_table_is_empty_exactly_as_go_declares_it() {
    for rule in OPT_RULE_LIST {
        assert_eq!(rule::opt_interaction_rule(rule), None);
    }
}

#[test]
fn logical_optimize_runs_the_ported_rules_in_order_and_reports_the_rest() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    // A Selection over a DataSource, with a TopN above.
    let source = data_source(&allocator, &[1, 2]);
    let filtered = selection_over(&allocator, vec![eq_const(1, 7)], source);
    let mut topn = LogicalPlan::TopN(LogicalTopN::new(
        base(&allocator, "TopN", None),
        Vec::new(),
        0,
        5,
    ));
    topn.set_children(vec![filtered]);

    let flag = flags::PRUNE_COLUMNS
        | flags::BUILD_KEY_INFO
        | flags::CONSTANT_PROPAGATION
        | flags::PREDICATE_PUSH_DOWN
        | flags::PREDICATE_SIMPLIFICATION
        | flags::PUSH_DOWN_TOPN
        | flags::ELIMINATE_AGG;
    let outcome = logical_optimize(&ctx, flag, topn).expect("no rule fails on this plan");
    assert_eq!(
        outcome.applied,
        vec![
            RuleId::ColumnPruner,
            RuleId::BuildKeySolver,
            RuleId::AggregationEliminator,
            RuleId::ConstantPropagationSolver,
            RuleId::PpdSolver,
            RuleId::PredicateSimplification,
            RuleId::PushDownTopNOptimizer,
        ],
        "Go's execution order, not the flag-bit order"
    );
    assert!(outcome.skipped.is_empty());
    outcome.plan.dismantle();
}

#[test]
fn logical_optimize_honours_the_flag_mask() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let plan = data_source(&allocator, &[1]);
    let outcome = logical_optimize(&ctx, 0, plan).expect("nothing runs");
    assert!(outcome.applied.is_empty());
    assert!(outcome.skipped.is_empty());
    outcome.plan.dismantle();
}

#[test]
fn aggregation_elimination_requires_the_complete_strong_unique_key() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let mut source_schema = schema_of(&[1, 2, 3]);
    source_schema.pk_or_uk = vec![vec![column(1), column(2)]];
    let source = LogicalPlan::DataSource(DataSource::new(
        base(&allocator, "DataSource", Some(source_schema)),
        1,
        "t",
    ));
    let count = tidb_expression_aggregation::AggFuncDesc {
        base: tidb_expression_aggregation::BaseFuncDesc {
            name: AGG_FUNC_COUNT.to_owned(),
            args: vec![col_expr(3)],
            ret_type: int_type(),
        },
        mode: tidb_expression_aggregation::AggFunctionMode::Complete,
        has_distinct: false,
        order_by_items: Vec::new(),
        grouping_id: 0,
    };
    let mut aggregation = LogicalPlan::Aggregation(LogicalAggregation::new(
        base(&allocator, "Aggregation", Some(schema_of(&[9]))),
        vec![count],
        vec![col_expr(1), col_expr(2)],
    ));
    aggregation.set_children(vec![source]);

    let outcome = logical_optimize(&ctx, flags::ELIMINATE_AGG, aggregation)
        .expect("aggregation elimination must build the replacement projection");
    assert!(matches!(outcome.plan, LogicalPlan::Projection(_)));
    outcome.plan.dismantle();

    let mut incomplete_schema = schema_of(&[1, 2, 3]);
    incomplete_schema.pk_or_uk = vec![vec![column(1), column(2)]];
    let source = LogicalPlan::DataSource(DataSource::new(
        base(&allocator, "DataSource", Some(incomplete_schema)),
        1,
        "t",
    ));
    let count = tidb_expression_aggregation::AggFuncDesc {
        base: tidb_expression_aggregation::BaseFuncDesc {
            name: AGG_FUNC_COUNT.to_owned(),
            args: vec![col_expr(3)],
            ret_type: int_type(),
        },
        mode: tidb_expression_aggregation::AggFunctionMode::Complete,
        has_distinct: false,
        order_by_items: Vec::new(),
        grouping_id: 0,
    };
    let mut aggregation = LogicalPlan::Aggregation(LogicalAggregation::new(
        base(&allocator, "Aggregation", Some(schema_of(&[9]))),
        vec![count],
        vec![col_expr(1)],
    ));
    aggregation.set_children(vec![source]);
    let outcome = logical_optimize(&ctx, flags::ELIMINATE_AGG, aggregation)
        .expect("an ineligible aggregation remains intact");
    assert!(matches!(outcome.plan, LogicalPlan::Aggregation(_)));
    outcome.plan.dismantle();
}

#[test]
fn every_ported_rule_names_itself_as_go_does() {
    assert_eq!(ColumnPruner.name(), RuleId::ColumnPruner.name());
    assert_eq!(BuildKeySolver.name(), RuleId::BuildKeySolver.name());
    assert_eq!(
        tidb_planner_core_rule::rule_constant_propagation::ConstantPropagationSolver.name(),
        RuleId::ConstantPropagationSolver.name()
    );
    assert_eq!(PpdSolver.name(), RuleId::PpdSolver.name());
    assert_eq!(
        tidb_planner_core_rule::rule_predicate_simplification::PredicateSimplification.name(),
        RuleId::PredicateSimplification.name()
    );
    assert_eq!(
        PushDownTopNOptimizer.name(),
        RuleId::PushDownTopNOptimizer.name()
    );
}

// ***** AddSelection and predicate simplification *****

#[test]
fn add_selection_returns_the_child_when_there_is_nothing_to_filter() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let child = data_source(&allocator, &[1]);
    let id = child.id();
    let out = add_selection(&ctx, child, Vec::new(), 0);
    assert_eq!(out.id(), id);
}

#[test]
fn add_selection_drops_a_true_condition() {
    // Go's `constraint.DeleteTrueExprs`: `WHERE TRUE` filters nothing, so the
    // whole `LogicalSelection` never appears.
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let child = data_source(&allocator, &[1]);
    let id = child.id();
    let out = add_selection(&ctx, child, vec![const_true()], 0);
    assert_eq!(out.id(), id, "no Selection was built");
}

#[test]
fn add_selection_builds_a_selection_for_a_real_condition() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let child = data_source(&allocator, &[1]);
    let out = add_selection(&ctx, child, vec![eq_const(1, 7)], 0);
    match &out {
        LogicalPlan::Selection(op) => assert_eq!(op.conditions.len(), 1),
        other => panic!("expected a Selection, got {other:?}"),
    }
    assert_eq!(out.children().len(), 1);
    out.dismantle();
}

#[test]
fn add_selection_collapses_a_constant_false_filter_to_a_table_dual() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let child = data_source(&allocator, &[1, 2]);
    let out = add_selection(&ctx, child, vec![const_false()], 0);
    match &out {
        LogicalPlan::TableDual(dual) => {
            assert_eq!(dual.row_count, 0);
            assert_eq!(
                out.schema().map(|s| s.len()),
                Some(2),
                "Go's `dual.SetSchema(p.Schema())`"
            );
        }
        other => panic!("expected a TableDual, got {other:?}"),
    }
}

#[test]
fn add_selection_leaves_an_already_empty_dual_alone() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let dual = LogicalPlan::TableDual(tidb_planner_core_operator_logicalop::table_dual::LogicalTableDual::new(
        base(&allocator, "TableDual", Some(schema_of(&[1]))),
        0,
    ));
    let id = dual.id();
    let out = add_selection(&ctx, dual, vec![eq_const(1, 7)], 0);
    assert_eq!(out.id(), id);
}

#[test]
fn predicate_simplification_keeps_a_nonconstant_condition() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let kept =
        tidb_planner_core_operator_logicalop::plan_context::apply_predicate_simplification(&ctx, vec![eq_const(1, 7), const_true()], false, None);
    assert_eq!(kept.len(), 1, "only the constant TRUE is deleted");
}

#[test]
fn ordinary_predicate_simplification_forwards_the_validity_filter() {
    fn is_gt_on_column(condition: &Expression, id: i64) -> bool {
        let Expression::ScalarFunction(function) = condition else {
            return false;
        };
        matches!(
            function.get_args(),
            [Expression::Column(column), Expression::Constant(_)]
                if function.func_name.lowercase() == "gt" && column.unique_id == id
        )
    }

    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let conditions = tidb_planner_core_operator_logicalop::plan_context::apply_predicate_simplification(
        &ctx,
        vec![eq_cols(1, 2), gt_const(1, 7)],
        true,
        Some(&|_| false),
    );

    assert!(conditions
        .iter()
        .any(|condition| is_gt_on_column(condition, 1)));
    assert!(
        !conditions.iter().any(|condition| is_gt_on_column(condition, 2)),
        "Go forwards the validity filter to constant propagation, so it rejects the derived predicate"
    );
}

#[test]
fn convert_outer_to_inner_join_uses_the_parent_selection_predicate() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let left = data_source(&allocator, &[1]);
    let right = data_source(&allocator, &[2]);
    let mut join = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::LeftOuter,
    ));
    join.set_children(vec![left, right]);
    let mut selection = LogicalPlan::Selection(LogicalSelection::new(
        base(&allocator, "Selection", Some(schema_of(&[1, 2]))),
        vec![eq_const(2, 7)],
    ));
    selection.set_children(vec![join]);

    let (plan, changed) = super::rule_outer_to_inner_join::ConvertOuterToInnerJoin
        .optimize(&ctx, selection)
        .expect("outer-to-inner conversion succeeds");
    assert!(!changed, "pinned Go keeps planChanged false");
    let LogicalPlan::Selection(selection) = plan else {
        panic!("the Selection must remain");
    };
    let [LogicalPlan::Join(join)] = selection.base.children() else {
        panic!("expected the converted Join below the Selection");
    };
    assert_eq!(join.join_type, LogicalJoinType::Inner);
}

#[test]
fn convert_outer_to_inner_join_preserves_a_non_null_rejecting_outer_join() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let mut join = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::LeftOuter,
    ));
    join.set_children(vec![
        data_source(&allocator, &[1]),
        data_source(&allocator, &[2]),
    ]);
    let is_null = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("isnull"),
        int_type(),
        vec![col_expr(2)],
    ));
    let mut selection = LogicalPlan::Selection(LogicalSelection::new(
        base(&allocator, "Selection", Some(schema_of(&[1, 2]))),
        vec![is_null],
    ));
    selection.set_children(vec![join]);

    let (plan, _) = super::rule_outer_to_inner_join::ConvertOuterToInnerJoin
        .optimize(&ctx, selection)
        .expect("outer-to-inner conversion succeeds");
    let LogicalPlan::Selection(selection) = plan else {
        panic!("the Selection must remain");
    };
    let [LogicalPlan::Join(join)] = selection.base.children() else {
        panic!("expected the Join below the Selection");
    };
    assert_eq!(join.join_type, LogicalJoinType::LeftOuter);
}

#[test]
fn convert_outer_to_inner_join_maps_predicates_through_projection() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let mut join = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::LeftOuter,
    ));
    join.set_children(vec![
        data_source(&allocator, &[1]),
        data_source(&allocator, &[2]),
    ]);
    let mut projection = LogicalPlan::Projection(LogicalProjection::new(
        base(&allocator, "Projection", Some(schema_of(&[3]))),
        vec![col_expr(2)],
    ));
    projection.set_children(vec![join]);
    let mut selection = LogicalPlan::Selection(LogicalSelection::new(
        base(&allocator, "Selection", Some(schema_of(&[3]))),
        vec![gt_const(3, 0)],
    ));
    selection.set_children(vec![projection]);

    let (plan, _) = super::rule_outer_to_inner_join::ConvertOuterToInnerJoin
        .optimize(&ctx, selection)
        .expect("outer-to-inner conversion succeeds");
    let LogicalPlan::Selection(selection) = plan else {
        panic!("the Selection must remain");
    };
    let [LogicalPlan::Projection(projection)] = selection.base.children() else {
        panic!("expected the Projection below the Selection");
    };
    let [LogicalPlan::Join(join)] = projection.base.children() else {
        panic!("expected the Join below the Projection");
    };
    assert_eq!(join.join_type, LogicalJoinType::Inner);
}

#[test]
fn convert_outer_to_inner_join_propagates_parent_on_clause_to_nested_join() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let mut nested = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::LeftOuter,
    ));
    nested.set_children(vec![
        data_source(&allocator, &[1]),
        data_source(&allocator, &[2]),
    ]);
    let mut parent = LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2, 3]))),
        LogicalJoinType::Inner,
    );
    parent.other_conditions = vec![gt_const(2, 0)];
    parent
        .base
        .set_children(vec![nested, data_source(&allocator, &[3])]);

    let (plan, _) = super::rule_outer_to_inner_join::ConvertOuterToInnerJoin
        .optimize(&ctx, LogicalPlan::Join(parent))
        .expect("outer-to-inner conversion succeeds");
    let LogicalPlan::Join(parent) = plan else {
        panic!("expected the parent Join");
    };
    let [LogicalPlan::Join(nested), _] = parent.base.children() else {
        panic!("expected the nested Join");
    };
    assert_eq!(nested.join_type, LogicalJoinType::Inner);
}

#[test]
fn convert_outer_to_inner_join_handles_right_outer_inner_side() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let mut join = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::RightOuter,
    ));
    join.set_children(vec![
        data_source(&allocator, &[1]),
        data_source(&allocator, &[2]),
    ]);
    let mut selection = LogicalPlan::Selection(LogicalSelection::new(
        base(&allocator, "Selection", Some(schema_of(&[1, 2]))),
        vec![gt_const(1, 0)],
    ));
    selection.set_children(vec![join]);

    let (plan, _) = super::rule_outer_to_inner_join::ConvertOuterToInnerJoin
        .optimize(&ctx, selection)
        .expect("outer-to-inner conversion succeeds");
    let LogicalPlan::Selection(selection) = plan else {
        panic!("the Selection must remain");
    };
    let [LogicalPlan::Join(join)] = selection.base.children() else {
        panic!("expected the Join below the Selection");
    };
    assert_eq!(join.join_type, LogicalJoinType::Inner);
}

#[test]
fn convert_outer_to_inner_join_matches_go_promoted_apply_method() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let mut apply = LogicalApply::new(
        base(&allocator, LogicalApply::TYPE, Some(schema_of(&[1, 2]))),
        LogicalJoinType::LeftOuter,
    );
    apply.join.base.set_children(vec![
        data_source(&allocator, &[1]),
        data_source(&allocator, &[2]),
    ]);

    let (plan, _) = super::rule_outer_to_inner_join::ConvertOuterToInnerJoin
        .optimize(&ctx, LogicalPlan::Apply(apply))
        .expect("outer-to-inner conversion succeeds");
    let LogicalPlan::Join(join) = plan else {
        panic!("Go's promoted LogicalJoin method returns its embedded Join receiver");
    };
    assert_eq!(join.join_type, LogicalJoinType::LeftOuter);
}

// ***** predicate pushdown, per operator *****

fn push(ctx: &RuleContext<'_>, plan: LogicalPlan) -> LogicalPlan {
    let (plan, remaining, failure) = tidb_planner_core_operator_logicalop::rewrite::predicate_push_down(ctx, plan, Vec::new());
    assert!(failure.is_none());
    assert!(
        remaining.is_empty(),
        "the root reports nothing to the caller"
    );
    plan
}

#[test]
fn predicate_push_down_moves_a_selection_below_a_projection() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    // Selection(a = 7) / Projection(a) / DataSource
    let source = data_source(&allocator, &[1, 2]);
    let projection = unary(
        &allocator,
        "Projection",
        LogicalPlan::Projection(LogicalProjection::new(
            base(&allocator, "Projection", Some(schema_of(&[1]))),
            vec![col_expr(1)],
        )),
        source,
    );
    let root = selection_over(&allocator, vec![eq_const(1, 7)], projection);

    let out = push(&ctx, root);
    // Go's `LogicalProjection` arm rewrites the predicate through the
    // projection's expressions and hands it to the child; the DataSource
    // records it in `AllConds`, which is the proof that it crossed. Equality
    // is in TiKV's expression catalogue, so the Selection disappears and the
    // predicate is owned by the DataSource.
    assert!(matches!(out, LogicalPlan::Projection(_)));
    let projection = &out;
    let LogicalPlan::DataSource(source) = &projection.children()[0] else {
        panic!("expected a DataSource under the Projection");
    };
    assert_eq!(
        source.all_conds.len(),
        1,
        "the predicate was rewritten through the Projection and reached the source"
    );
    out.dismantle();
}

#[test]
fn predicate_push_down_does_not_cross_a_limit() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    // Selection(a = 7) / Limit / DataSource — Go's LogicalLimit forbids every
    // condition, so the Selection must survive ABOVE the Limit.
    let source = data_source(&allocator, &[1]);
    let limit = unary(
        &allocator,
        "Limit",
        LogicalPlan::Limit(LogicalLimit::new(base(&allocator, "Limit", None), 0, 10)),
        source,
    );
    let root = selection_over(&allocator, vec![eq_const(1, 7)], limit);

    let out = push(&ctx, root);
    assert!(matches!(out, LogicalPlan::Selection(_)));
    assert!(matches!(out.children()[0], LogicalPlan::Limit(_)));
    assert!(
        matches!(out.children()[0].children()[0], LogicalPlan::DataSource(_)),
        "nothing was re-attached under the Limit"
    );
    out.dismantle();
}

/// Go `BaseLogicalPlan.PredicatePushDown`'s tail (`base_logical_plan.go:137`):
/// a projection rewrites the predicate through its expressions, but when the
/// CHILD refuses the result, the leftover is attached as a `Selection` above
/// the child (below the projection), not above the projection. The substituted
/// predicate references the child's columns, so leaving it above the
/// projection would leave them unavailable.
#[test]
fn predicate_push_down_attaches_a_projection_leftover_below_the_projection() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    // Selection(a = 7) / Projection(a) / Limit / DataSource: the Limit
    // forbids every condition.
    let source = data_source(&allocator, &[1]);
    let limit = unary(
        &allocator,
        "Limit",
        LogicalPlan::Limit(LogicalLimit::new(base(&allocator, "Limit", None), 0, 10)),
        source,
    );
    let projection = unary(
        &allocator,
        "Projection",
        LogicalPlan::Projection(LogicalProjection::new(
            base(&allocator, "Projection", Some(schema_of(&[1]))),
            vec![col_expr(1)],
        )),
        limit,
    );
    let root = selection_over(&allocator, vec![eq_const(1, 7)], projection);

    let out = push(&ctx, root);
    assert!(
        matches!(out, LogicalPlan::Projection(_)),
        "the original Selection must disappear: {out:?}"
    );
    let LogicalPlan::Selection(selection) = &out.children()[0] else {
        panic!("expected the leftover Selection below the Projection");
    };
    assert_eq!(selection.conditions.len(), 1);
    assert!(matches!(
        selection.base.children()[0],
        LogicalPlan::Limit(_)
    ));
    out.dismantle();
}

#[test]
fn predicate_push_down_collapses_a_constant_false_selection_to_a_dual() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[1]);
    // A Limit blocks the push, so the leftover comes back to the Selection and
    // takes Go's `Conds2TableDual` arm.
    let limit = unary(
        &allocator,
        "Limit",
        LogicalPlan::Limit(LogicalLimit::new(base(&allocator, "Limit", None), 0, 10)),
        source,
    );
    let root = selection_over(&allocator, vec![const_false()], limit);
    let out = push(&ctx, root);
    assert!(matches!(out, LogicalPlan::TableDual(_)));
}

#[test]
fn predicate_push_down_splits_an_inner_join_condition_by_side() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let left = data_source(&allocator, &[1, 2]);
    let right = data_source(&allocator, &[3, 4]);
    let mut join = LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2, 3, 4]))),
        LogicalJoinType::Inner,
    );
    join.other_conditions = vec![eq_cols(1, 3)];
    let mut join = LogicalPlan::Join(join);
    join.set_children(vec![left, right]);
    // WHERE left.a = 7 AND right.c = 9
    let root = selection_over(&allocator, vec![eq_const(1, 7), eq_const(3, 9)], join);

    let out = push(&ctx, root);
    let LogicalPlan::Join(join) = &out else {
        panic!("the Selection should have collapsed into the Join, got {out:?}");
    };
    assert_eq!(
        join.equal_conditions.len(),
        1,
        "`left.a = right.c` became a join key"
    );
    assert!(join.left_conditions.is_empty() && join.right_conditions.is_empty());
    for (side, child) in ["left", "right"].iter().zip(out.children()) {
        let LogicalPlan::DataSource(source) = child else {
            panic!("the {side} filter should be owned by its DataSource, got {child:?}");
        };
        assert!(
            !source.pushed_down_conds.is_empty(),
            "the {side} source should carry its pushed predicate"
        );
    }
    out.dismantle();
}

#[test]
fn predicate_push_down_drops_not_is_null_for_a_not_null_child_column() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let mut left_schema = schema_of(&[1]);
    left_schema.columns[0]
        .ret_type
        .as_mut()
        .unwrap()
        .add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let left_column = left_schema.columns[0].clone();
    let left = LogicalPlan::DataSource(DataSource::new(
        base(&allocator, "DataSource", Some(left_schema)),
        1,
        "t",
    ));
    let right = data_source(&allocator, &[2]);
    let mut join = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::Inner,
    ));
    join.set_children(vec![left, right]);
    let is_null = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("isnull"),
        int_type(),
        vec![Expression::Column(left_column)],
    ));
    let not_is_null = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("not"),
        int_type(),
        vec![is_null],
    ));
    let root = selection_over(&allocator, vec![not_is_null], join);

    let out = push(&ctx, root);
    let LogicalPlan::DataSource(left) = &out.children()[0] else {
        panic!("the left child should remain a DataSource")
    };
    assert!(
        left.pushed_down_conds.is_empty(),
        "Go deletes NOT(ISNULL(col)) after resolving a NOT NULL child column"
    );
    out.dismantle();
}

#[test]
fn predicate_push_down_propagates_a_constant_across_an_inner_join_key() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let left = data_source(&allocator, &[1]);
    let right = data_source(&allocator, &[2]);
    let mut join = LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::Inner,
    );
    join.other_conditions = vec![eq_cols(1, 2)];
    let mut join = LogicalPlan::Join(join);
    join.set_children(vec![left, right]);
    let root = selection_over(&allocator, vec![eq_const(2, 7)], join);

    let out = push(&ctx, root);
    let LogicalPlan::Join(join) = &out else {
        panic!("the Selection should have collapsed into the Join, got {out:?}");
    };
    assert_eq!(
        join.equal_conditions.len(),
        1,
        "constant propagation must retain the cross-side equality as a join key"
    );
    for ((side, column_id), child) in [("left", 1), ("right", 2)].iter().zip(out.children()) {
        let LogicalPlan::DataSource(source) = child else {
            panic!("the {side} filter should be owned by its DataSource, got {child:?}");
        };
        assert!(
            source.pushed_down_conds.iter().any(|condition| {
                let Expression::ScalarFunction(function) = condition else {
                    return false;
                };
                let [Expression::Column(column), Expression::Constant(constant)] =
                    function.get_args()
                else {
                    return false;
                };
                function.func_name.lowercase() == "eq"
                    && column.unique_id == *column_id
                    && matches!(constant.value, Datum::Int(7))
            }),
            "Go PropagateConstantForJoin derives the equality filter on the {side} side"
        );
    }
    out.dismantle();
}

#[test]
fn predicate_push_down_derives_an_inner_filter_for_a_left_outer_join() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let left = data_source(&allocator, &[1]);
    let right = data_source(&allocator, &[2]);
    let mut join = LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::LeftOuter,
    );
    join.other_conditions = vec![eq_cols(1, 2)];
    let mut join = LogicalPlan::Join(join);
    join.set_children(vec![left, right]);
    let root = selection_over(&allocator, vec![gt_const(1, 7)], join);

    let out = push(&ctx, root);
    let LogicalPlan::Join(join) = &out else {
        panic!("the Selection should have collapsed into the Join, got {out:?}");
    };
    assert_eq!(join.equal_conditions.len(), 1);
    let LogicalPlan::DataSource(inner) = &out.children()[1] else {
        panic!("the inner filter should reach the right DataSource");
    };
    assert!(inner.pushed_down_conds.iter().any(|condition| {
        let Expression::ScalarFunction(function) = condition else {
            return false;
        };
        matches!(
            function.get_args(),
            [Expression::Column(column), Expression::Constant(constant)]
                if function.func_name.lowercase() == "gt"
                    && column.unique_id == 2
                    && matches!(constant.value, Datum::Int(7))
        )
    }));
    out.dismantle();
}

/// Go `simplifyOuterJoin` (`logical_join.go:306`) runs at the top of
/// `LogicalJoin.PredicatePushDown`: a WHERE predicate on the null-supplying
/// side rejects the null-extended rows, so the outer join becomes inner and
/// the predicate is attributed by the inner arm instead of staying above.
#[test]
fn predicate_push_down_turns_a_null_rejected_left_outer_join_into_an_inner_join() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let left = data_source(&allocator, &[1]);
    let right = data_source(&allocator, &[2]);
    let mut join = LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::LeftOuter,
    );
    join.other_conditions = vec![eq_cols(1, 2)];
    let mut join = LogicalPlan::Join(join);
    join.set_children(vec![left, right]);
    // `gt(col2, 7)` reads only the inner (right) side, so it null-rejects it.
    let root = selection_over(&allocator, vec![gt_const(2, 7)], join);

    let out = push(&ctx, root);
    let LogicalPlan::Join(join) = &out else {
        panic!("the Selection should have collapsed into the Join, got {out:?}");
    };
    assert_eq!(
        join.join_type,
        LogicalJoinType::Inner,
        "a null-rejecting predicate on the inner side makes the outer join inner"
    );
    out.dismantle();
}

#[test]
fn constant_propagation_pulls_a_projected_child_predicate_above_an_inner_join() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[1]);
    let selection = selection_over(&allocator, vec![eq_const(1, 7)], source);
    let mut projection = LogicalPlan::Projection(LogicalProjection::new(
        base(&allocator, "Projection", Some(schema_of(&[10]))),
        vec![col_expr(1)],
    ));
    projection.set_children(vec![selection]);
    let right = data_source(&allocator, &[20]);
    let mut join = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[10, 20]))),
        LogicalJoinType::Inner,
    ));
    join.set_children(vec![projection, right]);

    let out = tidb_planner_core_rule::rule_constant_propagation::constant_propagation(&ctx, join);
    let LogicalPlan::Selection(pulled) = &out else {
        panic!("the join must gain a parent Selection, got {out:?}")
    };
    assert!(matches!(
        pulled.conditions.as_slice(),
        [Expression::ScalarFunction(function)]
            if matches!(function.get_args(), [Expression::Column(column), Expression::Constant(_)] if column.unique_id == 10)
    ));
    assert!(matches!(out.children(), [LogicalPlan::Join(_)]));
    out.dismantle();
}

#[test]
fn constant_propagation_keeps_go_preorder_for_nested_joins() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let selected = selection_over(
        &allocator,
        vec![eq_const(1, 7)],
        data_source(&allocator, &[1]),
    );
    let mut inner = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::Inner,
    ));
    inner.set_children(vec![selected, data_source(&allocator, &[2])]);
    let mut outer = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2, 3]))),
        LogicalJoinType::Inner,
    ));
    outer.set_children(vec![inner, data_source(&allocator, &[3])]);

    let out = tidb_planner_core_rule::rule_constant_propagation::constant_propagation(&ctx, outer);
    assert!(matches!(out, LogicalPlan::Join(_)));
    assert!(matches!(out.children()[0], LogicalPlan::Selection(_)));
    assert!(matches!(
        out.children()[0].children(),
        [LogicalPlan::Join(_)]
    ));
    out.dismantle();
}

#[test]
fn predicate_push_down_does_not_synthesize_a_transitive_join_key() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);

    let mut lower_join = LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::Inner,
    );
    lower_join.other_conditions = vec![eq_cols(1, 2)];
    let mut lower_join = LogicalPlan::Join(lower_join);
    lower_join.set_children(vec![
        data_source(&allocator, &[1]),
        data_source(&allocator, &[2]),
    ]);

    let mut upper_join = LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2, 3]))),
        LogicalJoinType::Inner,
    );
    upper_join.other_conditions = vec![eq_cols(1, 3)];
    let mut root = LogicalPlan::Join(upper_join);
    root.set_children(vec![lower_join, data_source(&allocator, &[3])]);

    let out = push(&ctx, root);
    let LogicalPlan::Join(upper_join) = &out else {
        panic!("the upper join should remain the root, got {out:?}");
    };
    assert_eq!(
        upper_join.equal_conditions.len(),
        1,
        "Go uses column equalities to build equivalence classes but does not substitute them into additional join keys"
    );
    let LogicalPlan::Join(lower_join) = &out.children()[0] else {
        panic!("the lower join should remain the left child");
    };
    assert_eq!(lower_join.equal_conditions.len(), 1);
    out.dismantle();
}

#[test]
fn join_simplification_does_not_substitute_column_equalities() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let conditions = tidb_planner_core_operator_logicalop::plan_context::apply_predicate_simplification_for_join(
        &ctx,
        vec![eq_cols(1, 2), eq_cols(1, 3)],
        &schema_of(&[1]),
        &schema_of(&[2, 3]),
        true,
        None,
    );

    assert!(
        !conditions.iter().any(|condition| {
            let Expression::ScalarFunction(function) = condition else {
                return false;
            };
            let [Expression::Column(left), Expression::Column(right)] = function.get_args() else {
                return false;
            };
            function.func_name.lowercase() == "eq"
                && [left.unique_id, right.unique_id]
                    .into_iter()
                    .all(|id| id == 2 || id == 3)
        }),
        "Go marks column-column equalities visited before substitution, so they cannot synthesize the transitive 2 = 3 join key"
    );
}

#[test]
fn join_key_retention_follows_the_session_variable() {
    fn is_join_key(condition: &Expression) -> bool {
        let Expression::ScalarFunction(function) = condition else {
            return false;
        };
        matches!(
            function.get_args(),
            [Expression::Column(left), Expression::Column(right)]
                if function.func_name.lowercase() == "eq"
                    && left.unique_id == 1
                    && right.unique_id == 2
        )
    }
    let allocator = PlanIdAllocator::new();
    let left = schema_of(&[1]);
    let right = schema_of(&[2]);
    let input = vec![eq_cols(1, 2), eq_const(1, 7)];

    let keep_context = test_context(&allocator);
    let kept = tidb_planner_core_operator_logicalop::plan_context::apply_predicate_simplification_for_join(
        &keep_context,
        input.clone(),
        &left,
        &right,
        true,
        None,
    );
    assert!(kept.iter().any(is_join_key));

    let mut drop_context = test_context(&allocator);
    drop_context.always_keep_join_key = false;
    let dropped = tidb_planner_core_operator_logicalop::plan_context::apply_predicate_simplification_for_join(
        &drop_context,
        input,
        &left,
        &right,
        true,
        None,
    );
    assert!(!dropped.iter().any(is_join_key), "{dropped:#?}");
}

#[test]
fn predicate_push_down_keeps_an_aggregate_filter_above_a_non_group_by_column() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[1, 2]);
    // One aggregate function per output column, as Go's builder produces:
    // `first_row(a)` for the group-by column and `count(b)` for column 9.
    let desc = |name: &str, arg: i64| tidb_expression_aggregation::AggFuncDesc {
        base: tidb_expression_aggregation::BaseFuncDesc {
            name: name.to_owned(),
            args: vec![col_expr(arg)],
            ret_type: int_type(),
        },
        mode: tidb_expression_aggregation::AggFunctionMode::Complete,
        has_distinct: false,
        order_by_items: Vec::new(),
        grouping_id: 0,
    };
    let agg = LogicalAggregation::new(
        base(&allocator, "Aggregation", Some(schema_of(&[1, 9]))),
        vec![desc(AGG_FUNC_FIRST_ROW, 1), desc(AGG_FUNC_COUNT, 2)],
        vec![col_expr(1)],
    );
    let mut agg = LogicalPlan::Aggregation(agg);
    agg.set_children(vec![source]);
    // `a` IS a group-by column and so may be pushed; column 9 is the aggregate
    // output and may not.
    let root = selection_over(&allocator, vec![eq_const(1, 7), eq_const(9, 3)], agg);

    let out = push(&ctx, root);
    assert!(
        matches!(out, LogicalPlan::Selection(_)),
        "the aggregate-output filter stayed above, got {out:?}"
    );
    let LogicalPlan::Selection(kept) = &out else {
        unreachable!()
    };
    assert_eq!(kept.conditions.len(), 1);
    let agg = &out.children()[0];
    assert!(matches!(agg, LogicalPlan::Aggregation(_)));
    let LogicalPlan::DataSource(source) = &agg.children()[0] else {
        panic!("the group-by filter should reach the aggregate's DataSource");
    };
    assert_eq!(source.pushed_down_conds.len(), 1);
    out.dismantle();
}

fn logical_fn(name: &str, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), int_type(), args))
}

/// An aggregation over a two-column source whose outputs are `first_row(a)`
/// (column 1, also the group-by column) and `count(b)` (column 9).
fn grouped_count_over_source(allocator: &PlanIdAllocator) -> LogicalPlan {
    let desc = |name: &str, arg: i64| tidb_expression_aggregation::AggFuncDesc {
        base: tidb_expression_aggregation::BaseFuncDesc {
            name: name.to_owned(),
            args: vec![col_expr(arg)],
            ret_type: int_type(),
        },
        mode: tidb_expression_aggregation::AggFunctionMode::Complete,
        has_distinct: false,
        order_by_items: Vec::new(),
        grouping_id: 0,
    };
    let mut agg = LogicalPlan::Aggregation(LogicalAggregation::new(
        base(allocator, "Aggregation", Some(schema_of(&[1, 9]))),
        vec![desc(AGG_FUNC_FIRST_ROW, 1), desc(AGG_FUNC_COUNT, 2)],
        vec![col_expr(1)],
    ));
    agg.set_children(vec![data_source(allocator, &[1, 2])]);
    agg
}

#[test]
fn predicate_push_down_relaxes_a_dnf_with_an_unpushable_branch_below_an_aggregate() {
    // Go `pushDownDNFPredicates`: `(a = 7 and count > 3) or (a = 8)` cannot move
    // whole, but its group-by parts relax to `a = 7 or a = 8`, which can; the
    // original stays above.
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let dnf = logical_fn(
        "or",
        vec![
            logical_fn("and", vec![eq_const(1, 7), gt_const(9, 3)]),
            eq_const(1, 8),
        ],
    );
    let root = selection_over(&allocator, vec![dnf], grouped_count_over_source(&allocator));

    let out = push(&ctx, root);
    let LogicalPlan::Selection(kept) = &out else {
        panic!("the original disjunction must stay above the aggregate, got {out:?}");
    };
    assert_eq!(kept.conditions.len(), 1);
    let agg = &out.children()[0];
    let LogicalPlan::DataSource(source) = &agg.children()[0] else {
        panic!("the relaxed disjunction should reach the DataSource");
    };
    assert_eq!(source.pushed_down_conds.len(), 1);
    let Expression::ScalarFunction(pushed) = &source.pushed_down_conds[0] else {
        panic!("the pushed condition is a disjunction");
    };
    assert_eq!(pushed.func_name.lowercase(), "or");
    out.dismantle();
}

#[test]
fn predicate_push_down_moves_a_fully_pushable_dnf_below_an_aggregate() {
    // Every branch reads only group-by columns, so the whole disjunction moves
    // and nothing is left above.
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let dnf = logical_fn("or", vec![eq_const(1, 7), eq_const(1, 8)]);
    let root = selection_over(&allocator, vec![dnf], grouped_count_over_source(&allocator));

    let out = push(&ctx, root);
    assert!(
        matches!(out, LogicalPlan::Aggregation(_)),
        "no filter should remain above the aggregate, got {out:?}"
    );
    let LogicalPlan::DataSource(source) = &out.children()[0] else {
        panic!("the disjunction should reach the DataSource");
    };
    assert_eq!(source.pushed_down_conds.len(), 1);
    out.dismantle();
}

#[test]
fn predicate_push_down_moves_supported_conditions_into_a_data_source() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[1]);
    let root = selection_over(&allocator, vec![eq_const(1, 7)], source);
    let out = push(&ctx, root);
    assert!(matches!(out, LogicalPlan::DataSource(_)));
    let LogicalPlan::DataSource(source) = &out else {
        panic!("expected a DataSource");
    };
    assert_eq!(source.all_conds.len(), 1);
    assert_eq!(source.pushed_down_conds.len(), 1);
    out.dismantle();
}

struct FixedPartitionPruning(Vec<usize>);

impl PartitionPruning for FixedPartitionPruning {
    fn partition_indices(
        &self,
        _source: &DataSource,
    ) -> Result<Vec<usize>, tidb_planner_core_base::PlanError> {
        Ok(self.0.clone())
    }
}

struct UnexpectedPartitionPruning;

impl PartitionPruning for UnexpectedPartitionPruning {
    fn partition_indices(
        &self,
        _source: &DataSource,
    ) -> Result<Vec<usize>, tidb_planner_core_base::PlanError> {
        panic!("constant-false conditions must become TableDual before partition pruning")
    }
}

#[derive(Default)]
struct RecordingPlanCacheMarker(RefCell<Option<String>>);

impl PlanCacheMarker for RecordingPlanCacheMarker {
    fn set_skip_plan_cache(&self, reason: &str) {
        *self.0.borrow_mut() = Some(reason.to_owned());
    }
}

#[test]
fn partition_processor_puts_union_scan_inside_each_partition_branch() {
    let allocator = PlanIdAllocator::new();
    let mut source = data_source(&allocator, &[1, 2]);
    let LogicalPlan::DataSource(source_ref) = &mut source else {
        unreachable!("the helper builds a DataSource");
    };
    source_ref.partition_definition_ids = vec![101, 102, 103];

    let conditions = vec![eq_const(2, 7)];
    let handles = vec![column(1)];
    let mut union_scan = LogicalUnionScan::new(
        base(&allocator, LogicalUnionScan::TYPE, None),
        handles.clone(),
    );
    union_scan.conditions.clone_from(&conditions);
    union_scan.base.set_children(vec![source]);

    let pruning = FixedPartitionPruning(vec![0, 2]);
    let marker = RecordingPlanCacheMarker::default();
    let mut ctx = test_context(&allocator);
    ctx.partition_pruning = Some(&pruning);
    ctx.plan_cache_marker = Some(&marker);

    let (plan, changed) = PartitionProcessor
        .optimize(&ctx, LogicalPlan::UnionScan(union_scan))
        .expect("static partition rewrite");
    assert!(!changed);
    let LogicalPlan::PartitionUnionAll(union) = &plan else {
        panic!("expected a PartitionUnionAll, got {plan:?}");
    };
    let physical_ids = union
        .union_all
        .base
        .children()
        .iter()
        .map(|branch| {
            let LogicalPlan::UnionScan(branch) = branch else {
                panic!("each partition must be wrapped by UnionScan");
            };
            assert!(matches!(
                branch.conditions.as_slice(),
                [Expression::ScalarFunction(condition)] if condition.func_name.lowercase() == "eq"
            ));
            assert_eq!(
                branch
                    .handle_cols
                    .iter()
                    .map(|column| column.unique_id)
                    .collect::<Vec<_>>(),
                vec![1]
            );
            let [LogicalPlan::DataSource(source)] = branch.base.children() else {
                panic!("UnionScan must directly own its partition DataSource");
            };
            source.physical_table_id
        })
        .collect::<Vec<_>>();
    assert_eq!(physical_ids, vec![101, 103]);
    assert_eq!(
        marker.0.borrow().as_deref(),
        Some("Static partition pruning mode")
    );
    plan.dismantle();
}

#[test]
fn partition_processor_normalizes_both_condition_lists_and_folds_false() {
    let allocator = PlanIdAllocator::new();
    let pruning = FixedPartitionPruning(vec![1]);
    let mut ctx = test_context(&allocator);
    ctx.partition_pruning = Some(&pruning);

    let mut source = data_source(&allocator, &[1, 2]);
    let LogicalPlan::DataSource(source_ref) = &mut source else {
        unreachable!("the helper builds a DataSource");
    };
    source_ref.partition_definition_ids = vec![101, 102];
    source_ref.all_conds = vec![not_ne_const(2, 7)];
    source_ref.pushed_down_conds = vec![not_ne_const(2, 7)];

    let (plan, _) = PartitionProcessor
        .optimize(&ctx, source)
        .expect("static partition rewrite");
    let LogicalPlan::DataSource(source) = &plan else {
        panic!("one surviving partition must remain a DataSource");
    };
    for conditions in [&source.all_conds, &source.pushed_down_conds] {
        assert!(matches!(
            conditions.as_slice(),
            [Expression::ScalarFunction(condition)] if condition.func_name.lowercase() == "eq"
        ));
    }
    assert_eq!(source.physical_table_id, 102);
    plan.dismantle();

    let unexpected = UnexpectedPartitionPruning;
    let mut ctx = test_context(&allocator);
    ctx.partition_pruning = Some(&unexpected);
    let mut source = data_source(&allocator, &[1]);
    let LogicalPlan::DataSource(source_ref) = &mut source else {
        unreachable!("the helper builds a DataSource");
    };
    source_ref.partition_definition_ids = vec![101, 102];
    source_ref.all_conds = vec![const_false()];
    source_ref.pushed_down_conds = vec![const_false()];
    let (plan, _) = PartitionProcessor
        .optimize(&ctx, source)
        .expect("constant false fold");
    assert!(matches!(plan, LogicalPlan::TableDual(_)));
    plan.dismantle();
}

// ***** column pruning, per operator *****

#[test]
fn column_pruning_zero_column_schema_exemptions_match_go() {
    let allocator = PlanIdAllocator::new();

    let empty_child = LogicalPlan::TableDual(tidb_planner_core_operator_logicalop::table_dual::LogicalTableDual::new(
        base(&allocator, "TableDual", Some(Schema::default())),
        1,
    ));
    let inherited = selection_over(&allocator, Vec::new(), empty_child);
    assert!(no_unexpected_zero_column_schema(&inherited));

    let dual = LogicalPlan::TableDual(tidb_planner_core_operator_logicalop::table_dual::LogicalTableDual::new(
        base(&allocator, "TableDual", Some(Schema::default())),
        1,
    ));
    assert!(no_unexpected_zero_column_schema(&dual));

    let own_empty_schema = data_source(&allocator, &[]);
    assert!(!no_unexpected_zero_column_schema(&own_empty_schema));
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(
    expected = "After column pruning, some operator got an unexpected zero-column output schema. Please fix it."
)]
fn column_pruner_checks_the_real_pruned_plan() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[]);
    let _ = ColumnPruner.optimize(&ctx, source);
}

#[test]
fn column_pruning_drops_an_unused_data_source_column() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[1, 2, 3]);
    let (plan, failure) = tidb_planner_core_operator_logicalop::rewrite::prune_columns(&ctx, source, vec![column(2)]);
    assert!(failure.is_none());
    assert_eq!(
        plan.schema()
            .map(|s| s.columns.iter().map(|c| c.unique_id).collect::<Vec<_>>()),
        Some(vec![2])
    );
}

#[test]
fn column_pruning_keeps_a_column_a_selection_reads() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[1, 2, 3]);
    let root = selection_over(&allocator, vec![eq_const(3, 7)], source);
    let (plan, failure) = tidb_planner_core_operator_logicalop::rewrite::prune_columns(&ctx, root, vec![column(2)]);
    assert!(failure.is_none());
    let ids: Vec<i64> = plan.children()[0]
        .schema()
        .expect("the source owns a schema")
        .columns
        .iter()
        .map(|c| c.unique_id)
        .collect();
    assert_eq!(
        ids,
        vec![2, 3],
        "column 3 survives because the filter reads it"
    );
    plan.dismantle();
}

#[test]
fn column_pruning_projects_condition_only_columns_below_a_partition_union() {
    // Go LogicalUnionAll.PruneColumns adds an identity projection when a
    // child keeps a column solely to evaluate its pushed condition. Without
    // it, the child row shape is wider than PartitionUnion's row shape.
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let child = || {
        let mut source = data_source(&allocator, &[1, 2, 3]);
        if let LogicalPlan::DataSource(data_source) = &mut source {
            data_source.all_conds = vec![eq_const(2, 7)];
        } else {
            unreachable!("the helper builds a DataSource");
        }
        source
    };
    let mut union = LogicalPartitionUnionAll::new(base(
        &allocator,
        LogicalPartitionUnionAll::TYPE,
        Some(schema_of(&[1, 2, 3])),
    ));
    union.union_all.base.set_children(vec![child(), child()]);

    let (plan, failure) = tidb_planner_core_operator_logicalop::rewrite::prune_columns(
        &ctx,
        LogicalPlan::PartitionUnionAll(union),
        vec![column(1), column(3)],
    );
    assert!(failure.is_none());
    assert_eq!(
        plan.schema()
            .expect("the union has a schema")
            .columns
            .iter()
            .map(|column| column.unique_id)
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    for child in plan.children() {
        let LogicalPlan::Projection(projection) = child else {
            panic!("a condition-only column needs a projection, got {child:?}")
        };
        assert!(matches!(
            projection.exprs.as_slice(),
            [Expression::Column(first), Expression::Column(second)]
                if first.unique_id == 1 && second.unique_id == 3
        ));
        assert_eq!(
            projection.base.children()[0]
                .schema()
                .expect("the source has a schema")
                .columns
                .iter()
                .map(|column| column.unique_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }
    plan.dismantle();
}

#[test]
fn column_pruning_repairs_an_aggregate_that_lost_every_function() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[1, 2]);
    let count_desc = tidb_expression_aggregation::AggFuncDesc {
        base: tidb_expression_aggregation::BaseFuncDesc {
            name: AGG_FUNC_COUNT.to_owned(),
            args: vec![col_expr(2)],
            ret_type: int_type(),
        },
        mode: tidb_expression_aggregation::AggFunctionMode::Complete,
        has_distinct: false,
        order_by_items: Vec::new(),
        grouping_id: 0,
    };
    let mut agg = LogicalPlan::Aggregation(LogicalAggregation::new(
        base(&allocator, "Aggregation", Some(schema_of(&[9]))),
        vec![count_desc],
        vec![col_expr(1)],
    ));
    agg.set_children(vec![source]);

    // The parent uses nothing, so the single `count` output is pruned and Go's
    // `count(1)` repair has to fire — an aggregate with no function would
    // return no rows where Go's returns one.
    let (plan, failure) = tidb_planner_core_operator_logicalop::rewrite::prune_columns(&ctx, agg, Vec::new());
    assert!(failure.is_none());
    let LogicalPlan::Aggregation(agg) = &plan else {
        panic!("expected an Aggregation");
    };
    assert_eq!(agg.agg_funcs.len(), 1);
    assert_eq!(agg.agg_funcs[0].name(), AGG_FUNC_COUNT);
    plan.dismantle();
}

#[test]
fn column_pruning_splits_a_join_used_set_by_side() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let left = data_source(&allocator, &[1, 2]);
    let right = data_source(&allocator, &[3, 4]);
    let mut join = LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2, 3, 4]))),
        LogicalJoinType::Inner,
    );
    join.other_conditions = vec![eq_cols(2, 4)];
    let mut join = LogicalPlan::Join(join);
    join.set_children(vec![left, right]);

    let (plan, failure) = tidb_planner_core_operator_logicalop::rewrite::prune_columns(&ctx, join, vec![column(1)]);
    assert!(failure.is_none());
    let left_ids: Vec<i64> = plan.children()[0]
        .schema()
        .expect("owns a schema")
        .columns
        .iter()
        .map(|c| c.unique_id)
        .collect();
    let right_ids: Vec<i64> = plan.children()[1]
        .schema()
        .expect("owns a schema")
        .columns
        .iter()
        .map(|c| c.unique_id)
        .collect();
    assert_eq!(
        left_ids,
        vec![1, 2],
        "column 2 survives for the join condition"
    );
    assert_eq!(right_ids, vec![4]);
    assert_eq!(
        plan.schema().map(|s| s.len()),
        Some(1),
        "the join inline-projects to the parent's set"
    );
    plan.dismantle();
}

// ***** TopN pushdown *****

#[test]
fn topn_push_down_absorbs_a_sort_below_a_topn() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let _ = &ctx;
    let source = data_source(&allocator, &[1]);
    let sort = unary(
        &allocator,
        "Sort",
        LogicalPlan::Sort(LogicalSort::new(base(&allocator, "Sort", None), Vec::new())),
        source,
    );
    let mut topn = LogicalPlan::TopN(LogicalTopN::new(
        base(&allocator, "TopN", None),
        Vec::new(),
        0,
        5,
    ));
    topn.set_children(vec![sort]);

    let out = push_down_topn(topn, None);
    // The Sort disappears into the TopN it feeds.
    assert!(out.plan_count() <= 3);
    out.dismantle();
}

#[test]
fn topn_keeps_travelling_after_it_absorbs_a_sort() {
    // Go `LogicalSort.PushDownTopN` returns
    // `ls.Children()[0].PushDownTopN(topN)`: with a Projection below the Sort,
    // the Projection stays above the pushed TopN. Stopping at the removed
    // Sort leaves `TopN -> Projection`, which prevents a cop LIMIT candidate
    // from reaching a double-read DataSource.
    let allocator = PlanIdAllocator::new();
    let source = data_source(&allocator, &[1]);
    let mut projection = LogicalPlan::Projection(LogicalProjection::new(
        base(&allocator, "Projection", Some(schema_of(&[1]))),
        vec![Expression::Column(column(1))],
    ));
    projection.set_children(vec![source]);
    let sort = unary(
        &allocator,
        "Sort",
        LogicalPlan::Sort(LogicalSort::new(
            base(&allocator, "Sort", None),
            vec![ByItems::new(Expression::Column(column(1)), false)],
        )),
        projection,
    );
    let mut limit = LogicalPlan::Limit(LogicalLimit::new(
        base(&allocator, "Limit", Some(schema_of(&[1]))),
        0,
        3,
    ));
    limit.set_children(vec![sort]);

    let out = push_down_topn(limit, None);
    let LogicalPlan::Projection(projection) = &out else {
        panic!("Projection should remain above the pushed TopN: {out:#?}");
    };
    assert!(matches!(
        projection.base.children().first(),
        Some(LogicalPlan::TopN(_))
    ));
    out.dismantle();
}

#[test]
fn topn_push_down_substitutes_projection_columns() {
    // Go `LogicalProjection.PushDownTopN` writes the expressions produced by
    // `ColumnSubstitute` back to `topN.ByItems` before descending.
    let allocator = PlanIdAllocator::new();
    let source = data_source(&allocator, &[1]);
    let mut projection = LogicalPlan::Projection(LogicalProjection::new(
        base(&allocator, "Projection", Some(schema_of(&[2]))),
        vec![Expression::Column(column(1))],
    ));
    projection.set_children(vec![source]);
    let topn = LogicalTopN::new(
        base(&allocator, "TopN", Some(schema_of(&[2]))),
        vec![ByItems::new(Expression::Column(column(2)), false)],
        0,
        5,
    );

    let out = push_down_topn(projection, Some(topn));
    let LogicalPlan::Projection(projection) = &out else {
        panic!("Projection should remain above the pushed TopN: {out:#?}");
    };
    let Some(LogicalPlan::TopN(topn)) = projection.base.children().first() else {
        panic!("TopN should be pushed beneath the Projection: {out:#?}");
    };
    assert!(matches!(
        topn.by_items.as_slice(),
        [ByItems {
            expr: Expression::Column(column),
            ..
        }] if column.unique_id == 1
    ));
    out.dismantle();
}

#[test]
fn topn_push_down_leaves_a_join_alone() {
    // Go `LogicalJoin.PushDownTopN` (`logical_join.go:428`): without a unique
    // inner side the offset cannot travel, so the limit re-attaches above the
    // join as count+offset over zero offset.
    let allocator = PlanIdAllocator::new();
    let left = data_source(&allocator, &[1]);
    let right = data_source(&allocator, &[2]);
    let mut join = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::LeftOuter,
    ));
    join.set_children(vec![left, right]);
    let mut topn = LogicalPlan::TopN(LogicalTopN::new(
        base(&allocator, "TopN", None),
        Vec::new(),
        0,
        5,
    ));
    topn.set_children(vec![join]);

    let out = push_down_topn(topn, None);
    assert!(matches!(out, LogicalPlan::TopN(_) | LogicalPlan::Limit(_)));
    assert_eq!(out.plan_count(), 4);
    out.dismantle();
}

/// `col(left) = col(right)` as the ScalarFunction shape `EqualConditions` keeps.
fn eq_scalar(left: i64, right: i64) -> ScalarFunction {
    match eq_cols(left, right) {
        Expression::ScalarFunction(sf) => sf,
        _ => unreachable!("eq_cols builds a scalar function"),
    }
}

#[test]
fn topn_push_down_enters_a_left_join_whose_inner_side_is_unique() {
    // Go `pushDownTopNToChild` (`logical_join.go:1272`) with a LEFT OUTER JOIN
    // whose inner side carries the join key as PKOrUK: the offset travels down
    // (`count, offset = topN.Count, topN.Offset`) and `topnEliminated` drops
    // the root TopN entirely — the limit lands on the PRESERVED child.
    let allocator = PlanIdAllocator::new();
    let left_schema = schema_of(&[1]);
    let mut right_schema = schema_of(&[2]);
    right_schema.pk_or_uk = vec![vec![column(2)]];
    let left = DataSource::new(
        base(&allocator, "DataSource", Some(left_schema.clone())),
        1,
        "t",
    );
    let left = LogicalPlan::DataSource(left);
    let right = DataSource::new(base(&allocator, "DataSource", Some(right_schema)), 1, "t");
    let right = LogicalPlan::DataSource(right);
    let mut join = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::LeftOuter,
    ));
    join.set_children(vec![left, right]);
    let join_ref = match &mut join {
        LogicalPlan::Join(op) => op,
        _ => unreachable!(),
    };
    join_ref.equal_conditions.push(eq_scalar(1, 2));
    let mut topn = LogicalPlan::Limit(LogicalLimit::new(
        base(&allocator, "Limit", Some(schema_of(&[1, 2]))),
        0,
        5,
    ));
    topn.set_children(vec![join]);

    let out = push_down_topn(topn, None);
    // The root limit is ELIMINATED; the join is what comes back.
    fn dump(plan: &LogicalPlan, depth: usize) -> String {
        let mut me = format!(
            "{}{:p} tp={} kids={}\n",
            " ".repeat(depth * 2),
            plan,
            plan.base().base.tp(),
            plan.children().len()
        );
        for child in plan.children() {
            me.push_str(&dump(child, depth + 1));
        }
        me
    }
    assert!(
        matches!(out, LogicalPlan::Join(_)),
        "expected the bare join, got {} | tree: {}",
        out.explain_info(),
        dump(&out, 0)
    );
    // ...and a LIMIT sits directly above the preserved (left) child.
    let out_join_children = out.children();
    assert_eq!(out_join_children.len(), 2);
    assert!(
        matches!(out_join_children[0], LogicalPlan::Limit(_)),
        "expected the pushed limit on the outer side"
    );
    assert!(matches!(out_join_children[1], LogicalPlan::DataSource(_)));
    out.dismantle();
}

#[test]
fn topn_push_down_keeps_the_limit_above_a_left_join_with_a_wide_inner_side() {
    // Go `pushDownTopNToChild` (`logical_join.go:1298`): when the inner side
    // is NOT unique on the join key, one preserved row may fan out, so only
    // `Count+Offset` rows are safe to keep and the limit RE-ATTACHES above
    // the join.
    let allocator = PlanIdAllocator::new();
    let left = data_source(&allocator, &[1]);
    let right = data_source(&allocator, &[2]);
    let mut join = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::LeftOuter,
    ));
    join.set_children(vec![left, right]);
    let join_ref = match &mut join {
        LogicalPlan::Join(op) => op,
        _ => unreachable!(),
    };
    join_ref.equal_conditions.push(eq_scalar(1, 2));
    let mut limit = LogicalPlan::Limit(LogicalLimit::new(
        base(&allocator, "Limit", Some(schema_of(&[1, 2]))),
        3,
        5,
    ));
    limit.set_children(vec![join]);

    let out = push_down_topn(limit, None);
    // The original limit stays ABOVE the join...
    let LogicalPlan::Limit(root) = &out else {
        panic!(
            "expected the limit above the join, got {}",
            out.explain_info()
        )
    };
    assert_eq!(root.offset, 3, "the incoming limit keeps its own offset");
    assert_eq!(root.count, 5);
    // ...and a PARTIAL limit with count+offset sits on the preserved child.
    let join = &root.base.children()[0];
    let LogicalPlan::Join(join_op) = join else {
        panic!("expected the join below the limit")
    };
    let LogicalPlan::Limit(pushed) = &join_op.base.children()[0] else {
        panic!("expected the pushed partial limit on the outer side")
    };
    assert_eq!(pushed.offset, 0, "the pushed half loses its offset");
    assert_eq!(pushed.count, 8, "count absorbs offset when not unique");
    out.dismantle();
}

#[test]
fn topn_push_down_over_an_inner_join_takes_the_base_body() {
    // Go `LogicalJoin.PushDownTopN` (`logical_join.go:436-438`): only OUTER
    // joins forward a TopN into a child; an inner join falls to
    // `pushDownTopNForBaseLogicalPlan`, which re-attaches above.
    let allocator = PlanIdAllocator::new();
    let mut right_schema = schema_of(&[2]);
    right_schema.pk_or_uk = vec![vec![column(2)]];
    let left = data_source(&allocator, &[1]);
    let right = DataSource::new(base(&allocator, "DataSource", Some(right_schema)), 1, "t");
    let right = LogicalPlan::DataSource(right);
    let mut join = LogicalPlan::Join(LogicalJoin::new(
        base(&allocator, "Join", Some(schema_of(&[1, 2]))),
        LogicalJoinType::Inner,
    ));
    join.set_children(vec![left, right]);
    let mut topn = LogicalPlan::TopN(LogicalTopN::new(
        base(&allocator, "TopN", None),
        Vec::new(),
        0,
        5,
    ));
    topn.set_children(vec![join]);

    let out = push_down_topn(topn, None);
    // The TopN stays above the join (as the limit it becomes on attach).
    assert!(matches!(out, LogicalPlan::TopN(_) | LogicalPlan::Limit(_)));
    out.dismantle();
}

// ***** build key info *****

#[test]
fn build_key_info_portal_runs_bottom_up_over_the_whole_tree() {
    let allocator = PlanIdAllocator::new();
    let mut source_schema = schema_of(&[1, 2]);
    source_schema.pk_or_uk = vec![vec![column(1)]];
    let mut source = DataSource::new(base(&allocator, "DataSource", Some(source_schema)), 1, "t");
    source.pk_is_handle = true;
    source.columns = vec![
        tidb_model::GoShared::new({
                let mut info = tidb_model::ColumnInfo {
                    id: 1,
                    name: tidb_ast::CiString::new("a"),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::ColumnInfo::default()
                };
                info.add_flag(u64::from(tidb_datatype::FieldTypeFlags::PRI_KEY));
                info.add_flag(u64::from(tidb_datatype::FieldTypeFlags::NOT_NULL));
                info
            }),
        tidb_model::GoShared::new({
                let info = tidb_model::ColumnInfo {
                    id: 2,
                    name: tidb_ast::CiString::new("b"),
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::ColumnInfo::default()
                };
                info
            }),
    ];
    let source = LogicalPlan::DataSource(source);

    let mut limit = LogicalPlan::Limit(LogicalLimit::new(
        base(&allocator, "Limit", Some(schema_of(&[1, 2]))),
        0,
        1,
    ));
    limit.set_children(vec![source]);

    let out = tidb_planner_core_operator_logicalop::rewrite::build_key_info_portal(limit);
    assert!(
        out.max_one_row(),
        "Go's LogicalLimit.BuildKeyInfo marks `LIMIT 1` as at most one row"
    );
    assert_eq!(
        out.schema().map(|s| s.pk_or_uk.len()),
        Some(1),
        "the child's key was carried up"
    );
    out.dismantle();
}

/// A leaf takes Go's base `PredicatePushDown` body: nothing is pushed.
#[test]
fn predicate_push_down_returns_the_predicates_unpushed_at_a_leaf() {
    let allocator = tidb_planner_core_base::PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let mut base = BaseLogicalPlan::with_id(1, "DataSource", 0);
    base.base.set_schema(Some(tidb_expr::schema::Schema::new(vec![
        tidb_expr::column::Column::new(1, tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)),
    ])));
    let leaf = LogicalPlan::DataSource(tidb_planner_core_operator_logicalop::data_source::DataSource {
        base,
        table_id: 1,
        ..tidb_planner_core_operator_logicalop::data_source::DataSource::default()
    });
    let (rest, root) = leaf
        .predicate_push_down(&ctx, Vec::new())
        .map_err(|(_, error)| error)
        .expect("the childless arm is Go's base body");
    assert!(rest.is_empty());
    assert_eq!(root.id(), 1);
}

/// The TopN rule body with the preserving test builder.
pub(crate) fn push_down_topn(
    plan: LogicalPlan,
    topn: Option<tidb_planner_core_operator_logicalop::topn::LogicalTopN>,
) -> LogicalPlan {
    let allocator = PlanIdAllocator::new();
    tidb_planner_core_operator_logicalop::rewrite::push_down_topn_with_builder(
        &TEST_BUILDER,
        &allocator,
        plan,
        topn,
    )
}

/// Partition-scoped index hints run through core's access-path hint
/// resolution (`CoreAccessPathResolver`).
mod partition_index_hints {
    use tidb_planner_util::path::PossiblePath;
    use tidb_planner_core_operator_logicalop::data_source::{DataSource, DataSourceIndexHint};

    #[test]
    fn ordinary_index_hints_are_resolved_per_static_partition() {
        let hinted = |name: &str, partitions: &[&str]| DataSourceIndexHint {
            kind: tidb_ast::IndexHintKind::Use,
            index_names: vec![name.to_owned()],
            partitions: partitions.iter().map(|name| (*name).to_owned()).collect(),
            push_down_lookup: false,
            force_keep_order: false,
            force_no_keep_order: false,
            restored: format!("/*+ USE_INDEX(t, {name}) */"),
        };
        let mut source = DataSource {
            table_name: "t".to_owned(),
            indexes: vec![
                tidb_model::GoShared::new(tidb_model::IndexInfo {
                    id: 1,
                    name: tidb_ast::CiString::new("idx_all"),
                    ..tidb_model::IndexInfo::default()
                }),
                tidb_model::GoShared::new(tidb_model::IndexInfo {
                    id: 2,
                    name: tidb_ast::CiString::new("idx_p0"),
                    ..tidb_model::IndexInfo::default()
                }),
                tidb_model::GoShared::new(tidb_model::IndexInfo {
                    id: 3,
                    name: tidb_ast::CiString::new("idx_p1"),
                    ..tidb_model::IndexInfo::default()
                }),
            ],
            public_enumerated_paths: vec![
                PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                },
                PossiblePath::Index { index: 0 },
                PossiblePath::Index { index: 1 },
                PossiblePath::Index { index: 2 },
            ],
            index_hints: vec![
                hinted("idx_all", &[]),
                hinted("idx_p0", &["p0"]),
                hinted("idx_p1", &["P1"]),
            ],
            // These are comment hints (`/*+ USE_INDEX(t, ...) */`), which
            // live in `index_hints` only; `ast_index_hints` holds the
            // table-syntax list, empty for this statement.
            ast_index_hints: Vec::new(),
            isolation_read_engines_value: "tikv".to_owned(),
            tikv_in_isolation_read: true,
            ..DataSource::default()
        };

        // `PARTITION(P1)` applies to the partition written `P1` only.
        let mut folded = source.clone_shallow();
        tidb_planner_core_rule::rule_partition_processor::resolve_index_hints_for_partition(&crate::rule::rule_tests::test_context(&tidb_planner_core_base::PlanIdAllocator::new()), &mut folded, "p1").expect("paths resolve");
        assert_eq!(folded.index_hints.len(), 1);
        tidb_planner_core_rule::rule_partition_processor::resolve_index_hints_for_partition(&crate::rule::rule_tests::test_context(&tidb_planner_core_base::PlanIdAllocator::new()), &mut source, "P1").expect("paths resolve");
        assert_eq!(
            source.enumerated_paths,
            vec![
                PossiblePath::Index { index: 0 },
                PossiblePath::Index { index: 2 }
            ]
        );
        assert_eq!(
            source.forced_index_ids,
            std::collections::BTreeSet::from([1, 3])
        );
        assert_eq!(source.index_hints.len(), 2);
    }

    /// Go `resolveAccessPaths` passes `ds.AstIndexHints` back into
    /// `getPossibleAccessPaths` for each partition: a table-syntax
    /// `t USE INDEX (ia)` keeps the table path out of every partition.
    #[test]
    fn table_syntax_index_hints_survive_per_partition_resolution() {
        let mut source = DataSource {
            table_name: "t".to_owned(),
            indexes: vec![tidb_model::GoShared::new(tidb_model::IndexInfo {
                id: 1,
                name: tidb_ast::CiString::new("ia"),
                ..tidb_model::IndexInfo::default()
            })],
            public_enumerated_paths: vec![
                PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                },
                PossiblePath::Index { index: 0 },
            ],
            ast_index_hints: vec![tidb_ast::IndexHint {
                kind: tidb_ast::IndexHintKind::Use,
                scope: tidb_ast::IndexHintScope::All,
                indexes: vec!["ia".to_owned()],
            }],
            isolation_read_engines_value: "tikv".to_owned(),
            tikv_in_isolation_read: true,
            ..DataSource::default()
        };

        tidb_planner_core_rule::rule_partition_processor::resolve_index_hints_for_partition(&crate::rule::rule_tests::test_context(&tidb_planner_core_base::PlanIdAllocator::new()), &mut source, "p0").expect("paths resolve");
        assert_eq!(
            source.enumerated_paths,
            vec![PossiblePath::Index { index: 0 }]
        );
        assert_eq!(
            source.forced_index_ids,
            std::collections::BTreeSet::from([1])
        );
    }
}
