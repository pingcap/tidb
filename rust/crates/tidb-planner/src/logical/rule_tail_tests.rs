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

//! WRITTEN tests (not transcreated from Go's test files) for the six rules in
//! [`super::rule_push_down_sequence`], [`super::rule_eliminate_unionall_dual_item`],
//! [`super::rule_eliminate_empty_selection`], [`super::rule_resolve_expand`],
//! [`super::rule_result_reorder`] and [`super::rule_derive_topn_from_window`].
//!
//! Go covers every one of them through whole-statement plan strings
//! (`planner/core/casetest`), which needs a parser, a catalogue and a session.
//! These tests assert the same decisions structurally: what each rule DOES to
//! the shape it recognises, what it REFUSES to touch, and that it survives a
//! tree far deeper than a recursive walk would.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::aggregation::{BaseFuncDesc, ByItems, WindowFuncDesc};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;

use crate::plan_base::PlanIdAllocator;

use crate::find_best_task::LogicalJoinType;

use super::cte::{CteClass, LogicalCTE};
use super::data_source::DataSource;
use super::expand::{LogicalExpand, RollupGroupingSet};
use super::join::LogicalJoin;
use super::projection::LogicalProjection;
use super::rule_derive_topn_from_window::derive_topn;
use super::rule_eliminate_empty_selection::eliminate_empty_selection;
use super::rule_eliminate_unionall_dual_item::union_all_eliminate_dual_item;
use super::rule_push_down_sequence::push_down_sequence;
use super::rule_resolve_expand::gen_expand;
use super::rule_result_reorder::result_reorder;
use super::rule_tests::test_context;
use super::selection::LogicalSelection;
use super::sequence::LogicalSequence;
use super::sort::LogicalSort;
use super::table_dual::LogicalTableDual;
use super::union_all::LogicalUnionAll;
use super::window::{BoundType, FrameBound, FrameType, LogicalWindow, WindowFrame, WindowSortItem};
use super::{BaseLogicalPlan, LogicalPlan};

/// A tree this deep would abort a recursive walk; see [`super::fold`].
const DEEP: usize = 20_000;

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

fn schema_of(ids: &[i64]) -> Schema {
    Schema::new(ids.iter().copied().map(column).collect())
}

fn base(allocator: &PlanIdAllocator, tp: &str, schema: Option<Schema>) -> BaseLogicalPlan {
    let mut base = BaseLogicalPlan::new(allocator, tp, 0);
    base.base.set_schema(schema);
    base
}

fn with_children(mut node: LogicalPlan, children: Vec<LogicalPlan>) -> LogicalPlan {
    node.set_children(children);
    node
}

fn data_source(allocator: &PlanIdAllocator, ids: &[i64]) -> LogicalPlan {
    LogicalPlan::DataSource(DataSource::new(
        base(allocator, DataSource::TYPE, Some(schema_of(ids))),
        1,
        "t",
    ))
}

fn dual(allocator: &PlanIdAllocator, ids: &[i64], row_count: usize) -> LogicalPlan {
    LogicalPlan::TableDual(LogicalTableDual::new(
        base(allocator, LogicalTableDual::TYPE, Some(schema_of(ids))),
        row_count,
    ))
}

fn projection(allocator: &PlanIdAllocator, ids: &[i64], child: LogicalPlan) -> LogicalPlan {
    let exprs = ids.iter().copied().map(|id| Expression::Column(column(id)));
    with_children(
        LogicalPlan::Projection(LogicalProjection::new(
            base(allocator, "Projection", Some(schema_of(ids))),
            exprs.collect(),
        )),
        vec![child],
    )
}

fn selection(
    allocator: &PlanIdAllocator,
    conditions: Vec<Expression>,
    child: LogicalPlan,
) -> LogicalPlan {
    with_children(
        LogicalPlan::Selection(LogicalSelection::new(
            base(allocator, "Selection", None),
            conditions,
        )),
        vec![child],
    )
}

fn sequence(allocator: &PlanIdAllocator, children: Vec<LogicalPlan>) -> LogicalPlan {
    with_children(
        LogicalPlan::Sequence(LogicalSequence::new(base(
            allocator,
            LogicalSequence::TYPE,
            None,
        ))),
        children,
    )
}

fn cte(allocator: &PlanIdAllocator) -> LogicalPlan {
    LogicalPlan::CTE(LogicalCTE::new(
        base(allocator, LogicalCTE::TYPE, Some(schema_of(&[9]))),
        Rc::new(RefCell::new(CteClass::default())),
    ))
}

/// `col(id) <= value`, the shape `expression.FindUpperBound` recognises.
fn le_const(id: i64, value: i64) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("le"),
        int_type(),
        vec![
            Expression::Column(column(id)),
            Expression::Constant(Constant::new(Datum::Int(value), int_type())),
        ],
    ))
}

/// A unary chain of `depth` projections above `leaf`, built ITERATIVELY.
fn deep_projection_chain(
    allocator: &PlanIdAllocator,
    depth: usize,
    leaf: LogicalPlan,
) -> LogicalPlan {
    let mut node = leaf;
    for _ in 0..depth {
        node = projection(allocator, &[1], node);
    }
    node
}

fn kind(node: &LogicalPlan) -> &'static str {
    match node {
        LogicalPlan::Sequence(_) => "Sequence",
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::Selection(_) => "Selection",
        LogicalPlan::DataSource(_) => "DataSource",
        LogicalPlan::TableDual(_) => "TableDual",
        LogicalPlan::UnionAll(_) => "UnionAll",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Sort(_) => "Sort",
        LogicalPlan::TopN(_) => "TopN",
        LogicalPlan::Window(_) => "Window",
        LogicalPlan::Expand(_) => "Expand",
        LogicalPlan::CTE(_) => "CTE",
        _ => "other",
    }
}

// ***************************************************************************
// PushDownSequenceSolver — Go rule #30
// ***************************************************************************

#[test]
fn a_sequence_is_pushed_through_a_unary_operator() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let plan = sequence(
        &allocator,
        vec![
            cte(&allocator),
            projection(&allocator, &[1], data_source(&allocator, &[1])),
        ],
    );

    let out = push_down_sequence(&ctx, plan);

    // Go: "return select->...", i.e. the unary operator floats ABOVE the
    // sequence and the sequence sits directly on the data source.
    assert_eq!(kind(&out), "Projection");
    let inner = &out.children()[0];
    assert_eq!(kind(inner), "Sequence");
    assert_eq!(inner.children().len(), 2);
    assert_eq!(kind(&inner.children()[0]), "CTE");
    assert_eq!(kind(&inner.children()[1]), "DataSource");

    out.dismantle();
}

#[test]
fn nested_sequences_merge_their_producers_in_order() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let outer_cte = cte(&allocator);
    let inner_cte = data_source(&allocator, &[7]);
    let plan = sequence(
        &allocator,
        vec![
            outer_cte,
            sequence(&allocator, vec![inner_cte, data_source(&allocator, &[1])]),
        ],
    );

    let out = push_down_sequence(&ctx, plan);

    // One sequence, with the OUTER producer first: a producer may only
    // reference the producers before it.
    assert_eq!(kind(&out), "Sequence");
    assert_eq!(out.children().len(), 3);
    assert_eq!(kind(&out.children()[0]), "CTE");
    assert_eq!(kind(&out.children()[1]), "DataSource");
    assert_eq!(kind(&out.children()[2]), "DataSource");
    assert!(!matches!(out.children()[1], LogicalPlan::Sequence(_)));

    out.dismantle();
}

#[test]
fn a_sequence_refuses_to_cross_a_multi_child_operator() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let join = with_children(
        LogicalPlan::Join(LogicalJoin::new(
            base(&allocator, "Join", Some(schema_of(&[1, 2]))),
            LogicalJoinType::Inner,
        )),
        vec![data_source(&allocator, &[1]), data_source(&allocator, &[2])],
    );
    let plan = sequence(&allocator, vec![cte(&allocator), join]);

    let out = push_down_sequence(&ctx, plan);

    // Go's `default` branch with `len(lp.Children()) != 1` attaches and stops.
    assert_eq!(kind(&out), "Sequence");
    assert_eq!(kind(&out.children()[1]), "Join");

    out.dismantle();
}

#[test]
fn a_sequence_travels_a_tree_deeper_than_recursion_survives() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let plan = sequence(
        &allocator,
        vec![
            cte(&allocator),
            deep_projection_chain(&allocator, DEEP, data_source(&allocator, &[1])),
        ],
    );

    let out = push_down_sequence(&ctx, plan);

    assert_eq!(kind(&out), "Projection");
    // The sequence is now at the BOTTOM of the chain, so the tree is one
    // level taller than it was.
    assert_eq!(out.max_depth(), DEEP + 2);
    let mut node = &out;
    for _ in 0..DEEP {
        node = &node.children()[0];
    }
    assert_eq!(kind(node), "Sequence");

    out.dismantle();
}

// ***************************************************************************
// EliminateUnionAllDualItem — Go rule #31
// ***************************************************************************

#[test]
fn an_empty_union_branch_is_dropped_in_both_of_gos_shapes() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let plan = with_children(
        LogicalPlan::UnionAll(LogicalUnionAll::new(base(
            &allocator,
            "UnionAll",
            Some(schema_of(&[1])),
        ))),
        vec![
            dual(&allocator, &[1], 0),
            data_source(&allocator, &[1]),
            projection(&allocator, &[1], dual(&allocator, &[1], 0)),
        ],
    );

    let (out, changed) = union_all_eliminate_dual_item(&ctx, plan);

    assert_eq!(kind(&out), "UnionAll");
    assert_eq!(out.children().len(), 1);
    assert_eq!(kind(&out.children()[0]), "DataSource");
    // Go's `flag` is reset to false AFTER the branch drop; only the
    // whole-union replacement reports a change.
    assert!(!changed);

    out.dismantle();
}

#[test]
fn a_union_of_only_empty_branches_becomes_a_dual_that_keeps_the_schema() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let plan = with_children(
        LogicalPlan::UnionAll(LogicalUnionAll::new(base(
            &allocator,
            "UnionAll",
            Some(schema_of(&[1, 2])),
        ))),
        vec![dual(&allocator, &[1, 2], 0), dual(&allocator, &[1, 2], 0)],
    );

    let (out, changed) = union_all_eliminate_dual_item(&ctx, plan);

    assert!(changed);
    let LogicalPlan::TableDual(replacement) = &out else {
        panic!("an empty union all becomes a dual");
    };
    assert_eq!(replacement.row_count, 0);
    assert_eq!(
        out.schema().map(|schema| schema.columns.len()),
        Some(2),
        "the parent's column references must still resolve"
    );

    out.dismantle();
}

#[test]
fn a_non_empty_dual_branch_is_kept() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let plan = with_children(
        LogicalPlan::UnionAll(LogicalUnionAll::new(base(
            &allocator,
            "UnionAll",
            Some(schema_of(&[1])),
        ))),
        vec![dual(&allocator, &[1], 1), data_source(&allocator, &[1])],
    );

    let (out, changed) = union_all_eliminate_dual_item(&ctx, plan);

    assert!(!changed);
    assert_eq!(out.children().len(), 2);
    assert_eq!(kind(&out.children()[0]), "TableDual");

    out.dismantle();
}

#[test]
fn a_union_deep_under_a_chain_is_still_reached() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let union = with_children(
        LogicalPlan::UnionAll(LogicalUnionAll::new(base(
            &allocator,
            "UnionAll",
            Some(schema_of(&[1])),
        ))),
        vec![dual(&allocator, &[1], 0), dual(&allocator, &[1], 0)],
    );
    let plan = deep_projection_chain(&allocator, DEEP, union);

    let (out, changed) = union_all_eliminate_dual_item(&ctx, plan);

    assert!(changed, "the change flag travels all the way up");
    let mut node = &out;
    for _ in 0..DEEP {
        node = &node.children()[0];
    }
    assert_eq!(kind(node), "TableDual");

    out.dismantle();
}

// ***************************************************************************
// EmptySelectionEliminator — Go rule #32
// ***************************************************************************

#[test]
fn an_empty_selection_below_the_root_is_spliced_out() {
    let allocator = PlanIdAllocator::default();
    let plan = projection(
        &allocator,
        &[1],
        selection(&allocator, Vec::new(), data_source(&allocator, &[1])),
    );

    let out = eliminate_empty_selection(plan);

    assert_eq!(kind(&out), "Projection");
    assert_eq!(kind(&out.children()[0]), "DataSource");

    out.dismantle();
}

#[test]
fn a_selection_with_conditions_is_kept() {
    let allocator = PlanIdAllocator::default();
    let plan = projection(
        &allocator,
        &[1],
        selection(
            &allocator,
            vec![le_const(1, 5)],
            data_source(&allocator, &[1]),
        ),
    );

    let out = eliminate_empty_selection(plan);

    assert_eq!(kind(&out.children()[0]), "Selection");

    out.dismantle();
}

#[test]
fn the_root_selection_is_never_eliminated_and_neither_is_a_chained_one() {
    let allocator = PlanIdAllocator::default();
    // Go tests only CHILDREN, so a root empty selection survives.
    let root_only = eliminate_empty_selection(selection(
        &allocator,
        Vec::new(),
        data_source(&allocator, &[1]),
    ));
    assert_eq!(kind(&root_only), "Selection");
    root_only.dismantle();

    // And `recursivePlan(sel.Children()[0])` skips the test on the selection's
    // own child, so the LOWER of two chained empty selections survives.
    let chained = projection(
        &allocator,
        &[1],
        selection(
            &allocator,
            Vec::new(),
            selection(&allocator, Vec::new(), data_source(&allocator, &[1])),
        ),
    );
    let out = eliminate_empty_selection(chained);
    assert_eq!(kind(&out), "Projection");
    assert_eq!(kind(&out.children()[0]), "Selection");
    assert_eq!(kind(&out.children()[0].children()[0]), "DataSource");

    out.dismantle();
}

#[test]
fn empty_selections_are_eliminated_at_depth() {
    let allocator = PlanIdAllocator::default();
    let mut node = data_source(&allocator, &[1]);
    for _ in 0..DEEP {
        node = projection(&allocator, &[1], selection(&allocator, Vec::new(), node));
    }
    let depth_before = node.max_depth();

    let out = eliminate_empty_selection(node);

    // Every selection but none of the projections is gone.
    assert_eq!(depth_before, 2 * DEEP + 1);
    assert_eq!(out.max_depth(), DEEP + 1);

    out.dismantle();
}

// ***************************************************************************
// ResolveExpand — Go rule #34
// ***************************************************************************

fn expand_with_two_sets(allocator: &PlanIdAllocator, schema: Schema) -> LogicalPlan {
    let mut expand = LogicalExpand::new(base(allocator, LogicalExpand::TYPE, Some(schema)));
    expand.distinct_group_by_col = vec![column(1)];
    expand.distinct_size = 2;
    expand.rollup_grouping_sets = vec![
        RollupGroupingSet::new([1]),
        RollupGroupingSet::new(BTreeSet::new()),
    ];
    with_children(
        LogicalPlan::Expand(expand),
        vec![data_source(allocator, &[1])],
    )
}

#[test]
fn resolve_expand_generates_one_projection_per_grouping_set() {
    let allocator = PlanIdAllocator::default();
    let plan = projection(
        &allocator,
        &[1],
        expand_with_two_sets(&allocator, schema_of(&[1, 2])),
    );

    let out = gen_expand(plan);

    let LogicalPlan::Expand(expand) = &out.children()[0] else {
        panic!("the expand survives the rule");
    };
    let levels = expand
        .level_exprs
        .as_ref()
        .expect("GenLevelProjections ran");
    assert_eq!(levels.len(), 2);
    // Each level projects the non-generated column plus the grouping id.
    assert_eq!(levels[0].len(), 2);
    // The set that does NOT group by column 1 projects it as a typed NULL.
    assert!(matches!(levels[1][0], Expression::Constant(_)));
    assert!(matches!(levels[0][0], Expression::Column(_)));

    out.dismantle();
}

#[test]
fn a_plan_without_an_expand_is_untouched() {
    let allocator = PlanIdAllocator::default();
    // A schema too narrow to hold the generated columns is Go's other refusal;
    // the level list stays nil.
    let plan = expand_with_two_sets(&allocator, Schema::default());

    let out = gen_expand(plan);

    let LogicalPlan::Expand(expand) = &out else {
        panic!("the expand survives");
    };
    assert!(expand.level_exprs.is_none());

    out.dismantle();
}

#[test]
fn resolve_expand_reaches_an_expand_at_depth() {
    let allocator = PlanIdAllocator::default();
    let plan = deep_projection_chain(
        &allocator,
        DEEP,
        expand_with_two_sets(&allocator, schema_of(&[1, 2])),
    );

    let out = gen_expand(plan);

    let mut node = &out;
    for _ in 0..DEEP {
        node = &node.children()[0];
    }
    let LogicalPlan::Expand(expand) = node else {
        panic!("the expand is at the bottom of the chain");
    };
    assert_eq!(expand.level_exprs.as_ref().map(Vec::len), Some(2));

    out.dismantle();
}

// ***************************************************************************
// ResultReorder — Go rule #2
// ***************************************************************************

fn sort_over(allocator: &PlanIdAllocator, by: Vec<ByItems>, child: LogicalPlan) -> LogicalPlan {
    let schema = child.schema().cloned();
    with_children(
        LogicalPlan::Sort(LogicalSort::new(
            base(allocator, LogicalSort::TYPE, schema),
            by,
        )),
        vec![child],
    )
}

#[test]
fn a_sort_absorbs_the_output_columns_it_does_not_already_order_by() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let plan = sort_over(
        &allocator,
        vec![ByItems {
            expr: Expression::Column(column(1)),
            desc: true,
        }],
        // A projection under the sort keeps `extractHandleCol` from finding a
        // handle, so Go orders by every output column.
        projection(&allocator, &[1, 2], data_source(&allocator, &[1, 2])),
    );

    let out = result_reorder(&ctx, plan);

    let LogicalPlan::Sort(sort) = &out else {
        panic!("no sort is injected above an existing one");
    };
    assert_eq!(sort.by_items.len(), 2);
    // The existing item keeps its direction; only the missing column is added.
    assert!(sort.by_items[0].desc);
    assert!(!sort.by_items[1].desc);

    out.dismantle();
}

#[test]
fn a_sort_is_injected_above_the_first_non_keeper() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let plan = selection(
        &allocator,
        vec![le_const(1, 5)],
        with_children(
            LogicalPlan::UnionAll(LogicalUnionAll::new(base(
                &allocator,
                "UnionAll",
                Some(schema_of(&[1, 2])),
            ))),
            vec![data_source(&allocator, &[1, 2])],
        ),
    );

    let out = result_reorder(&ctx, plan);

    // The keeper stays on top and the sort lands between it and the union.
    assert_eq!(kind(&out), "Selection");
    let injected = &out.children()[0];
    assert_eq!(kind(injected), "Sort");
    let LogicalPlan::Sort(sort) = injected else {
        unreachable!()
    };
    assert_eq!(sort.by_items.len(), 2);
    assert_eq!(kind(&injected.children()[0]), "UnionAll");

    out.dismantle();
}

#[test]
fn a_keeper_chain_that_ends_without_children_is_already_ordered() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let plan = projection(&allocator, &[1], dual(&allocator, &[1], 1));

    let out = result_reorder(&ctx, plan);

    // Go's `completeSort` returns true here, so `injectSort` never runs.
    assert_eq!(kind(&out), "Projection");
    assert_eq!(kind(&out.children()[0]), "TableDual");

    out.dismantle();
}

#[test]
fn the_spine_walk_survives_a_deep_keeper_chain() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let plan = deep_projection_chain(
        &allocator,
        DEEP,
        with_children(
            LogicalPlan::UnionAll(LogicalUnionAll::new(base(
                &allocator,
                "UnionAll",
                Some(schema_of(&[1])),
            ))),
            vec![data_source(&allocator, &[1])],
        ),
    );

    let out = result_reorder(&ctx, plan);

    assert_eq!(out.max_depth(), DEEP + 3);
    let mut node = &out;
    for _ in 0..DEEP {
        node = &node.children()[0];
    }
    assert_eq!(kind(node), "Sort");

    out.dismantle();
}

// ***************************************************************************
// DeriveTopNFromWindow — Go rule #19
// ***************************************************************************

fn row_number_window(allocator: &PlanIdAllocator, name: &str, child: LogicalPlan) -> LogicalPlan {
    let mut window = LogicalWindow::new(
        base(allocator, LogicalWindow::TYPE, Some(schema_of(&[1, 2]))),
        vec![WindowFuncDesc {
            base: BaseFuncDesc {
                name: name.to_owned(),
                args: Vec::new(),
                ret_type: int_type(),
            },
        }],
    );
    window.order_by = vec![WindowSortItem::new(column(1), true)];
    window.frame = Some(WindowFrame {
        frame_type: FrameType::Rows,
        start: Some(FrameBound {
            bound_type: BoundType::CurrentRow,
            ..FrameBound::default()
        }),
        end: Some(FrameBound {
            bound_type: BoundType::CurrentRow,
            ..FrameBound::default()
        }),
    });
    with_children(LogicalPlan::Window(window), vec![child])
}

#[test]
fn a_row_number_upper_bound_derives_a_topn_under_the_window() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let plan = selection(
        &allocator,
        vec![le_const(2, 5)],
        row_number_window(&allocator, "row_number", data_source(&allocator, &[1])),
    );

    let out = derive_topn(&ctx, plan);

    // Go: "return select->datasource->topN->window".
    assert_eq!(kind(&out), "Selection");
    let window = &out.children()[0];
    assert_eq!(kind(window), "Window");
    let topn = &window.children()[0];
    assert_eq!(kind(topn), "TopN");
    let LogicalPlan::TopN(derived) = topn else {
        unreachable!()
    };
    assert_eq!(derived.count, 5);
    assert_eq!(derived.offset, 0);
    // The window's ORDER BY becomes the TopN's, direction included.
    assert_eq!(derived.by_items.len(), 1);
    assert!(derived.by_items[0].desc);
    assert_eq!(kind(&topn.children()[0]), "DataSource");

    out.dismantle();
}

#[test]
fn a_window_function_other_than_row_number_derives_nothing() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);
    let plan = selection(
        &allocator,
        vec![le_const(2, 5)],
        row_number_window(&allocator, "rank", data_source(&allocator, &[1])),
    );

    let out = derive_topn(&ctx, plan);

    assert_eq!(kind(&out.children()[0]), "Window");
    assert_eq!(kind(&out.children()[0].children()[0]), "DataSource");

    out.dismantle();
}

#[test]
fn two_conditions_or_a_bound_on_the_wrong_column_derive_nothing() {
    let allocator = PlanIdAllocator::default();
    let ctx = test_context(&allocator);

    let two_conditions = selection(
        &allocator,
        vec![le_const(2, 5), le_const(1, 9)],
        row_number_window(&allocator, "row_number", data_source(&allocator, &[1])),
    );
    let out = derive_topn(&ctx, two_conditions);
    assert_eq!(kind(&out.children()[0].children()[0]), "DataSource");
    out.dismantle();

    // Column 1 is not the window's result column, so the bound says nothing
    // about how many rows the window must see.
    let wrong_column = selection(
        &allocator,
        vec![le_const(1, 5)],
        row_number_window(&allocator, "row_number", data_source(&allocator, &[1])),
    );
    let out = derive_topn(&ctx, wrong_column);
    assert_eq!(kind(&out.children()[0].children()[0]), "DataSource");
}

#[test]
fn the_derivation_walks_past_a_deep_chain_and_respects_allow_derive_topn() {
    let allocator = PlanIdAllocator::default();
    let mut ctx = test_context(&allocator);
    let build = || {
        deep_projection_chain(
            &allocator,
            DEEP,
            selection(
                &allocator,
                vec![le_const(2, 5)],
                row_number_window(&allocator, "row_number", data_source(&allocator, &[1])),
            ),
        )
    };

    let derived = derive_topn(&ctx, build());
    let mut node = &derived;
    for _ in 0..DEEP {
        node = &node.children()[0];
    }
    assert_eq!(kind(&node.children()[0].children()[0]), "TopN");
    derived.dismantle();

    // Go's `AllowDeriveTopN` gates the whole recursion.
    ctx.allow_derive_topn = false;
    let untouched = derive_topn(&ctx, build());
    let mut node = &untouched;
    for _ in 0..DEEP {
        node = &node.children()[0];
    }
    assert_eq!(kind(&node.children()[0].children()[0]), "DataSource");
    untouched.dismantle();
}
