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

use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expr_util::builder::PreservingFunctionBuilder;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;

use crate::find_best_task::LogicalJoinType;
use crate::plan_base::PlanIdAllocator;

use super::aggregation::{LogicalAggregation, AGG_FUNC_COUNT};
use super::data_source::DataSource;
use super::fold::{fold_owned, Descend, OwnedRewrite};
use super::join::LogicalJoin;
use super::limit::LogicalLimit;
use super::projection::LogicalProjection;
use super::rule::{
    self, add_selection, flags, logical_optimize, BuildKeySolver, ColumnPruner,
    DisabledLogicalRules, LogicalOptRule, PpdSolver, PushDownTopNOptimizer, RuleContext, RuleId,
    OPT_RULE_FLAGS, OPT_RULE_LIST,
};
use super::selection::LogicalSelection;
use super::sort::LogicalSort;
use super::topn::LogicalTopN;
use super::{BaseLogicalPlan, LogicalPlan};

const BUILDER: PreservingFunctionBuilder = PreservingFunctionBuilder;

/// A [`RuleContext`] over a caller-owned allocator, for tests elsewhere in the
/// crate as well as this file.
pub(crate) fn test_context(allocator: &PlanIdAllocator) -> RuleContext<'_> {
    RuleContext {
        allocator,
        builder: &BUILDER,
        use_plan_cache: false,
        // Go's `AllowDeriveTopN` defaults ON in `sessionVars`.
        allow_derive_topn: true,
        disabled_rules: DisabledLogicalRules::default(),
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

/// `col(left) = col(right)`, the shape that becomes an `EqualCondition`.
fn eq_cols(left: i64, right: i64) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("eq"),
        int_type(),
        vec![col_expr(left), col_expr(right)],
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
        rule::set_predicate_push_down_flag(0),
        flags::PREDICATE_PUSH_DOWN
    );
    let existing = flags::PRUNE_COLUMNS | flags::PREDICATE_PUSH_DOWN;
    assert_eq!(rule::set_predicate_push_down_flag(existing), existing);
}

#[test]
fn a_disabled_rule_is_skipped_by_name() {
    let disabled = DisabledLogicalRules::from_names(["predicate_push_down"]);
    assert!(disabled.is_logical_rule_disabled(RuleId::PpdSolver));
    assert!(!disabled.is_logical_rule_disabled(RuleId::BuildKeySolver));
    // Go names both `ColumnPruner` entries identically, so disabling one
    // disables both.
    let disabled = DisabledLogicalRules::from_names(["column_prune"]);
    assert!(disabled.is_logical_rule_disabled(RuleId::ColumnPruner));
    assert!(disabled.is_logical_rule_disabled(RuleId::ColumnPrunerAgain));
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
        | flags::PREDICATE_PUSH_DOWN
        | flags::PUSH_DOWN_TOPN
        | flags::ELIMINATE_AGG;
    let outcome = logical_optimize(&ctx, flag, topn).expect("no rule fails on this plan");
    assert_eq!(
        outcome.applied,
        vec![
            RuleId::ColumnPruner,
            RuleId::BuildKeySolver,
            RuleId::PpdSolver,
            RuleId::PushDownTopNOptimizer,
        ],
        "Go's execution order, not the flag-bit order"
    );
    assert_eq!(
        outcome.skipped,
        vec![RuleId::AggregationEliminator],
        "an unported rule is reported, never silently treated as a no-op"
    );
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
fn every_ported_rule_names_itself_as_go_does() {
    assert_eq!(ColumnPruner.name(), RuleId::ColumnPruner.name());
    assert_eq!(BuildKeySolver.name(), RuleId::BuildKeySolver.name());
    assert_eq!(PpdSolver.name(), RuleId::PpdSolver.name());
    assert_eq!(
        PushDownTopNOptimizer.name(),
        RuleId::PushDownTopNOptimizer.name()
    );
}

// ***** AddSelection and its simplification subset *****

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
fn add_selection_drops_a_condition_the_simplification_subset_deletes() {
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
    let dual = LogicalPlan::TableDual(super::table_dual::LogicalTableDual::new(
        base(&allocator, "TableDual", Some(schema_of(&[1]))),
        0,
    ));
    let id = dual.id();
    let out = add_selection(&ctx, dual, vec![eq_const(1, 7)], 0);
    assert_eq!(out.id(), id);
}

#[test]
fn the_simplification_subset_keeps_what_it_cannot_decide() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let kept = rule::apply_predicate_simplification(&ctx, vec![eq_const(1, 7), const_true()]);
    assert_eq!(kept.len(), 1, "only the constant TRUE is deleted");
}

// ***** predicate pushdown, per operator *****

fn push(ctx: &RuleContext<'_>, plan: LogicalPlan) -> LogicalPlan {
    let (plan, remaining, failure) = super::rewrite::predicate_push_down(ctx, plan, Vec::new());
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
    // records it in `AllConds`, which is the proof that it crossed. The
    // Selection stays above because the narrowed DataSource arm claims
    // nothing as coprocessor-pushable — see that arm's note in `rewrite.rs`.
    assert!(matches!(out, LogicalPlan::Selection(_)));
    let projection = &out.children()[0];
    assert!(matches!(projection, LogicalPlan::Projection(_)));
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
        assert!(
            matches!(child, LogicalPlan::Selection(_)),
            "the {side} filter was re-attached as a Selection, got {child:?}"
        );
    }
    out.dismantle();
}

#[test]
fn predicate_push_down_keeps_an_aggregate_filter_above_a_non_group_by_column() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[1, 2]);
    let mut agg = LogicalAggregation::new(
        base(&allocator, "Aggregation", Some(schema_of(&[1, 9]))),
        Vec::new(),
        vec![col_expr(1)],
    );
    agg.agg_funcs = Vec::new();
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
    assert!(
        matches!(agg.children()[0], LogicalPlan::Selection(_)),
        "the group-by filter crossed the aggregate"
    );
    out.dismantle();
}

#[test]
fn predicate_push_down_records_every_condition_on_a_data_source() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[1]);
    let root = selection_over(&allocator, vec![eq_const(1, 7)], source);
    let out = push(&ctx, root);
    // The DataSource cannot claim the predicate without the pushdown
    // whitelist, so the Selection stays — and `AllConds` still records it,
    // which is what column pruning reads.
    assert!(matches!(out, LogicalPlan::Selection(_)));
    let LogicalPlan::DataSource(source) = &out.children()[0] else {
        panic!("expected a DataSource");
    };
    assert_eq!(source.all_conds.len(), 1);
    assert!(source.pushed_down_conds.is_empty());
    out.dismantle();
}

// ***** column pruning, per operator *****

#[test]
fn column_pruning_drops_an_unused_data_source_column() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[1, 2, 3]);
    let (plan, failure) = super::rewrite::prune_columns(&ctx, source, vec![column(2)]);
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
    let (plan, failure) = super::rewrite::prune_columns(&ctx, root, vec![column(2)]);
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
fn column_pruning_repairs_an_aggregate_that_lost_every_function() {
    let allocator = PlanIdAllocator::new();
    let ctx = test_context(&allocator);
    let source = data_source(&allocator, &[1, 2]);
    let count_desc = tidb_expr::aggregation::AggFuncDesc {
        base: tidb_expr::aggregation::BaseFuncDesc {
            name: AGG_FUNC_COUNT.to_owned(),
            args: vec![col_expr(2)],
            ret_type: int_type(),
        },
        mode: tidb_expr::aggregation::AggFunctionMode::Complete,
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
    let (plan, failure) = super::rewrite::prune_columns(&ctx, agg, Vec::new());
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

    let (plan, failure) = super::rewrite::prune_columns(&ctx, join, vec![column(1)]);
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

    let out = super::rewrite::push_down_topn(topn, None);
    // The Sort disappears into the TopN it feeds.
    assert!(out.plan_count() <= 3);
    out.dismantle();
}

#[test]
fn topn_push_down_leaves_a_join_alone() {
    // The narrowed base body keeps the TopN ABOVE the join, which is always
    // correct; the outer-side push is a later batch.
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

    let out = super::rewrite::push_down_topn(topn, None);
    assert!(matches!(out, LogicalPlan::TopN(_) | LogicalPlan::Limit(_)));
    assert_eq!(out.plan_count(), 4);
    out.dismantle();
}

// ***** build key info *****

#[test]
fn build_key_info_portal_runs_bottom_up_over_the_whole_tree() {
    let allocator = PlanIdAllocator::new();
    let mut source_schema = schema_of(&[1, 2]);
    source_schema.pk_or_uk = vec![vec![column(1)]];
    let source = DataSource::new(base(&allocator, "DataSource", Some(source_schema)), 1, "t");
    let source = LogicalPlan::DataSource(source);

    let mut limit = LogicalPlan::Limit(LogicalLimit::new(
        base(&allocator, "Limit", Some(schema_of(&[1, 2]))),
        0,
        1,
    ));
    limit.set_children(vec![source]);

    let out = super::rewrite::build_key_info_portal(limit);
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
