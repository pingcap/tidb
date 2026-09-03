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

//! Semantic tests for the logical plan tree.
//!
//! These are WRITTEN, not transcreated: Go covers `BaseLogicalPlan` only
//! through the full optimizer (`pkg/planner/core/logical_plans_test.go` runs
//! whole statements), which needs a session this crate does not have. What is
//! checked here is the contract the tree itself owes: construction, child
//! get/set, the base accessors, schema and stats propagation, and the
//! recursion depth the module header measures.

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::schema::Schema;

use crate::find_best_task::LogicalJoinType;

use super::*;

fn column(unique_id: i64) -> Column {
    Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
}

fn source(id: i32, columns: &[i64]) -> LogicalPlan {
    let mut base = BaseLogicalPlan::with_id(id, "DataSource", 0);
    base.base.set_schema(Some(Schema::new(
        columns.iter().copied().map(column).collect(),
    )));
    LogicalPlan::DataSource(DataSource {
        base,
        table_id: i64::from(id),
        ..DataSource::default()
    })
}

fn selection(id: i32, child: LogicalPlan) -> LogicalPlan {
    let mut base = BaseLogicalPlan::with_id(id, "Selection", 0);
    base.set_children(vec![child]);
    LogicalPlan::Selection(LogicalSelection {
        base,
        conditions: Vec::new(),
    })
}

fn join(id: i32, left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
    let mut base = BaseLogicalPlan::with_id(id, "Join", 0);
    base.set_children(vec![left, right]);
    LogicalPlan::Join(LogicalJoin {
        base,
        ..LogicalJoin::default()
    })
}

/// A left-deep chain of `depth` selections over one data source.
fn chain(depth: usize) -> LogicalPlan {
    let mut node = source(0, &[1]);
    for i in 1..depth {
        node = selection(i as i32, node);
    }
    node
}

#[test]
fn tree_construction_reports_shape() {
    let tree = join(3, selection(2, source(1, &[1])), source(4, &[2]));
    assert_eq!(tree.plan_count(), 4);
    assert_eq!(tree.max_depth(), 3);
    assert_eq!(tree.children().len(), 2);
    assert_eq!(tree.tp(), "Join");
    assert_eq!(tree.explain_id(false), "Join_3");
    assert_eq!(tree.explain_id(true), "Join");
    tree.dismantle();
}

#[test]
fn walk_preorder_visits_parent_before_left_before_right() {
    let tree = join(3, selection(2, source(1, &[1])), source(4, &[2]));
    let mut order = Vec::new();
    tree.walk_preorder(&mut |node| order.push(node.id()));
    assert_eq!(order, vec![3, 2, 1, 4]);
    tree.dismantle();
}

#[test]
fn set_child_replaces_and_returns_the_previous_node() {
    let mut tree = selection(2, source(1, &[1]));
    let previous = tree.set_child(0, source(9, &[7])).expect("child 0 exists");
    assert_eq!(previous.id(), 1);
    assert_eq!(tree.children()[0].id(), 9);
    assert_eq!(tree.base().child_len(), 1);
    tree.dismantle();
    previous.dismantle();
}

#[test]
#[should_panic(expected = "index out of bounds")]
fn set_child_panics_on_an_out_of_range_index_like_go() {
    let mut tree = selection(2, source(1, &[1]));
    let _ = tree.set_child(4, source(10, &[8]));
}

#[test]
fn set_children_replaces_the_whole_vector() {
    let mut tree = selection(2, source(1, &[1]));
    tree.set_children(vec![source(5, &[3]), source(6, &[4])]);
    assert_eq!(tree.base().child_len(), 2);
    assert_eq!(
        tree.children()
            .iter()
            .map(LogicalPlan::id)
            .collect::<Vec<_>>(),
        vec![5, 6]
    );
    tree.dismantle();
}

#[test]
fn schema_falls_through_to_the_first_child() {
    let tree = selection(2, source(1, &[11, 12]));
    let schema = tree.schema().expect("inherited from the data source");
    assert_eq!(
        schema
            .columns
            .iter()
            .map(|c| c.unique_id)
            .collect::<Vec<_>>(),
        vec![11, 12]
    );
    // A schema-producing leaf may have no materialized schema yet.
    let bare = LogicalPlan::TableDual(LogicalTableDual {
        base: BaseLogicalPlan::with_id(1, "TableDual", 0),
        row_count: 1,
    });
    assert!(bare.schema().is_none());
    tree.dismantle();
}

#[test]
fn sequence_schema_uses_the_last_main_query_child_like_go() {
    let mut base = BaseLogicalPlan::with_id(3, LogicalSequence::TYPE, 0);
    base.set_children(vec![source(1, &[11]), source(2, &[22, 23])]);
    let sequence = LogicalPlan::Sequence(LogicalSequence::new(base));
    let schema = sequence.schema().expect("main query child schema");
    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| column.unique_id)
            .collect::<Vec<_>>(),
        vec![22, 23]
    );
    sequence.dismantle();
}

#[test]
fn schema_producer_leaf_keeps_output_names_locally() {
    let mut bare = LogicalPlan::TableDual(LogicalTableDual {
        base: BaseLogicalPlan::with_id(1, "TableDual", 0),
        row_count: 1,
    });
    let names = vec![tidb_datatype::FieldName::default()];
    bare.set_output_names(names.clone());
    assert_eq!(bare.output_names(), names.as_slice());
}

#[test]
fn schema_producer_leaf_without_names_returns_empty_output_names() {
    let bare = LogicalPlan::TableDual(LogicalTableDual {
        base: BaseLogicalPlan::with_id(1, "TableDual", 0),
        row_count: 1,
    });
    assert!(bare.output_names().is_empty());
}

#[test]
fn schema_producer_parent_keeps_output_names_off_its_child() {
    let mut base = BaseLogicalPlan::with_id(2, "Projection", 0);
    base.set_children(vec![source(1, &[11])]);
    let mut projection = LogicalPlan::Projection(LogicalProjection {
        base,
        ..LogicalProjection::default()
    });
    let names = vec![tidb_datatype::FieldName::default()];
    projection.set_output_names(names.clone());
    assert_eq!(projection.output_names(), names.as_slice());
    assert!(projection.children()[0].output_names().is_empty());
    projection.dismantle();
}

#[test]
fn derive_stats_adopts_a_single_childs_profile() {
    let mut tree = selection(2, source(1, &[1]));
    let child_stats = StatsInfo::new(42.0, [(1_i64, 7.0)]);
    let schema = Schema::new(vec![column(1)]);

    let (stats, reloaded) = tree
        .derive_stats(std::slice::from_ref(&child_stats), &schema, &[], &[false])
        .expect("one child is the ported base body");
    assert!(reloaded);
    assert_eq!(stats.row_count(), 42.0);
    assert_eq!(tree.stats_info().map(StatsInfo::row_count), Some(42.0));
    tree.dismantle();
}

#[test]
fn derive_stats_refuses_more_than_one_child() {
    let mut tree = join(3, source(1, &[1]), source(2, &[2]));
    let schema = Schema::new(vec![column(1)]);
    let err = tree
        .derive_stats(
            &[StatsInfo::new(1.0, []), StatsInfo::new(1.0, [])],
            &schema,
            &[],
            &[false, false],
        )
        .expect_err("Go raises ErrInternal here");
    assert!(err.message().contains("more than one child"));
    tree.dismantle();
}

#[test]
fn derive_stats_on_a_leaf_is_one_row_at_ndv_one() {
    let mut leaf = source(1, &[5, 6]);
    let schema = Schema::new(vec![column(5), column(6)]);
    let (stats, reloaded) = leaf
        .derive_stats(&[], &schema, &[], &[])
        .expect("the leaf body is dependency-closed");
    assert!(reloaded);
    assert_eq!(stats.row_count(), 1.0);
    assert_eq!(stats.col_ndvs().get(&5), Some(&1.0));
    assert_eq!(stats.col_ndvs().get(&6), Some(&1.0));

    // Without a reload signal the second call reuses what is stored.
    let (again, reloaded) = leaf
        .derive_stats(&[], &schema, &[], &[false])
        .expect("cached");
    assert!(!reloaded);
    assert_eq!(again.row_count(), 1.0);
}

#[test]
fn prepare_possible_properties_needs_every_child_to_agree() {
    let mut tree = join(3, source(1, &[1]), source(2, &[2]));
    let schema = Schema::new(vec![column(1)]);

    let both = tree.prepare_possible_properties(
        &schema,
        &[
            Some(PossiblePropertiesInfo {
                has_tiflash: true,
                ..PossiblePropertiesInfo::default()
            }),
            Some(PossiblePropertiesInfo {
                has_tiflash: true,
                ..PossiblePropertiesInfo::default()
            }),
        ],
    );
    assert!(both.has_tiflash);
    assert!(tree.base().has_tiflash());

    let one_missing = tree.prepare_possible_properties(
        &schema,
        &[
            Some(PossiblePropertiesInfo {
                has_tiflash: true,
                ..PossiblePropertiesInfo::default()
            }),
            None,
        ],
    );
    assert!(!one_missing.has_tiflash);

    // No children at all is Go's `len(info) > 0` guard: false.
    assert!(!tree.prepare_possible_properties(&schema, &[]).has_tiflash);
    tree.dismantle();
}

#[test]
fn hash_code_is_the_plan_id_and_equals_compares_it() {
    let a = source(1, &[1]);
    let b = source(1, &[2]);
    let c = source(2, &[1]);
    assert_eq!(a.hash_code(), b.hash_code());
    assert_ne!(a.hash_code(), c.hash_code());
    assert!(a.equals(&b));
    assert!(!a.equals(&c));
}

#[test]
fn flags_and_plan_ids_hash_round_trip() {
    let mut tree = selection(2, source(1, &[1]));
    assert!(!tree
        .base()
        .has_flag(APPLY_GEN_FROM_XF_DECORRELATE_RULE_FLAG));
    tree.base_mut()
        .set_flag(APPLY_GEN_FROM_XF_DECORRELATE_RULE_FLAG);
    assert!(tree
        .base()
        .has_flag(APPLY_GEN_FROM_XF_DECORRELATE_RULE_FLAG));

    tree.set_plan_ids_hash(0xdead_beef);
    assert_eq!(tree.plan_ids_hash(), 0xdead_beef);
    tree.dismantle();
}

#[test]
fn predicate_push_down_returns_the_predicates_unpushed_at_a_leaf() {
    let allocator = crate::plan_base::PlanIdAllocator::new();
    let ctx = crate::logical::rule_tests::test_context(&allocator);
    let leaf = source(1, &[1]);
    let (rest, root) = leaf
        .predicate_push_down(&ctx, Vec::new())
        .map_err(|(_, error)| error)
        .expect("the childless arm is Go's base body");
    assert!(rest.is_empty());
    assert_eq!(root.id(), 1);
}

#[test]
fn join_child_stats_and_schema_only_answers_for_a_join() {
    let mut left = source(1, &[1]);
    left.set_stats(Some(StatsInfo::new(3.0, [])));
    let mut right = source(2, &[2]);
    right.set_stats(Some(StatsInfo::new(5.0, [])));
    let tree = join(3, left, right);

    let (l, r, ls, rs) = tree
        .get_join_child_stats_and_schema()
        .expect("a Join has two children");
    assert_eq!(l.map(StatsInfo::row_count), Some(3.0));
    assert_eq!(r.map(StatsInfo::row_count), Some(5.0));
    assert!(ls.is_some() && rs.is_some());

    // Go's base body panics for a non-join; preserve that boundary.
    let unary = selection(4, source(5, &[1]));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        unary.get_join_child_stats_and_schema()
    }))
    .is_err());
    // And GetChildStatsAndSchema answers for the unary node.
    assert!(unary.get_child_stats_and_schema().is_some());
    let leaf = source(6, &[1]);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        leaf.get_child_stats_and_schema()
    }))
    .is_err());
    tree.dismantle();
    unary.dismantle();
}

/// The recursion finding from the module header, exercised.
///
/// 40,000 levels is past the ~30,000 at which a recursive `match` walk aborts
/// on a 2 MiB stack and past the ~20,000 at which recursive `Drop` glue does.
/// The walk below and the teardown are both stack-explicit, so this passes on
/// a default test thread; replacing either with recursion aborts the process
/// rather than failing the assertion.
#[test]
fn deep_chain_walks_and_tears_down_without_recursion() {
    const DEPTH: usize = 40_000;
    let tree = chain(DEPTH);
    assert_eq!(tree.plan_count(), DEPTH);
    assert_eq!(tree.max_depth(), DEPTH);

    let mut seen = 0_usize;
    tree.walk_preorder(&mut |_| seen += 1);
    assert_eq!(seen, DEPTH);

    tree.dismantle();
}

/// `deep_clone` copies a tree deeper than the derived `Clone` can recurse.
///
/// The derived `Clone` overflows on a chain of a few thousand in an
/// unoptimised build, which is why the module offers a stack-explicit one.
#[test]
fn deep_clone_copies_a_deep_tree_and_the_copy_is_independent() {
    const DEPTH: usize = 40_000;
    let tree = chain(DEPTH);
    let mut copy = tree.deep_clone();
    assert_eq!(copy.plan_count(), DEPTH);
    assert_eq!(copy.max_depth(), DEPTH);

    copy.base_mut().base.set_id(-1);
    assert_eq!(tree.id(), (DEPTH - 1) as i32);
    assert_eq!(copy.id(), -1);
    tree.dismantle();
    copy.dismantle();
}

/// A shallow clone keeps the node's own fields and drops its children, which
/// is what a rule rebuilding a node around moved children wants.
#[test]
fn clone_shallow_keeps_the_node_and_drops_the_children() {
    let mut tree = selection(2, source(1, &[1]));
    tree.base_mut()
        .set_flag(APPLY_GEN_FROM_XF_DECORRELATE_RULE_FLAG);
    let shallow = tree.clone_shallow();
    assert_eq!(shallow.id(), 2);
    assert_eq!(shallow.tp(), "Selection");
    assert!(shallow
        .base()
        .has_flag(APPLY_GEN_FROM_XF_DECORRELATE_RULE_FLAG));
    assert_eq!(shallow.base().child_len(), 0);
    assert_eq!(tree.base().child_len(), 1);
    tree.dismantle();
}

// ***************************************************************************
// ExtractFD child-access boundaries — Go's operator overrides index children
// (or answer empty for the default join types) before anything else.
// ***************************************************************************

#[test]
fn a_childless_outer_join_panics_when_extracting_fd_like_go() {
    let join = LogicalPlan::Join(LogicalJoin::new(
        BaseLogicalPlan::with_id(1, LogicalJoin::TYPE, 0),
        LogicalJoinType::LeftOuter,
    ));
    // Go `ExtractFDForOuterJoin` reads `p.Children()[0]` and `[1]`.
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { join.extract_fd() })).is_err()
    );
}

#[test]
fn a_single_child_inner_join_panics_when_extracting_fd_like_go() {
    let mut base = BaseLogicalPlan::with_id(1, LogicalJoin::TYPE, 0);
    base.set_children(vec![source(1, &[11])]);
    let join = LogicalPlan::Join(LogicalJoin::new(base, LogicalJoinType::Inner));
    // Go `ExtractFDForInnerJoin` reads `child[1]` before `child[0]`.
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { join.extract_fd() })).is_err()
    );
    join.dismantle();
}

#[test]
fn a_default_join_type_answers_an_empty_fdset_without_touching_children_like_go() {
    let join = LogicalPlan::Join(LogicalJoin::new(
        BaseLogicalPlan::with_id(1, LogicalJoin::TYPE, 0),
        LogicalJoinType::LeftOuterSemi,
    ));
    // Go's switch default returns the empty set before any child access.
    let fd = join.extract_fd();
    assert!(fd.not_null_cols.is_empty());
    assert!(fd.group_by_cols.is_empty());
}

#[test]
fn a_childless_apply_panics_when_extracting_fd_like_go() {
    let apply = LogicalPlan::Apply(LogicalApply::new(
        BaseLogicalPlan::with_id(1, LogicalApply::TYPE, 0),
        LogicalJoinType::LeftOuter,
    ));
    // Go indexes `la.Children()[1]` before its join-type switch.
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { apply.extract_fd() })).is_err()
    );
}

#[test]
fn a_childless_selection_panics_when_extracting_fd_like_go() {
    let selection = LogicalPlan::Selection(LogicalSelection::new(
        BaseLogicalPlan::with_id(1, LogicalSelection::TYPE, 0),
        Vec::new(),
    ));
    // Go indexes `p.Children()[0]` before the `*LogicalJoin` assertion.
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { selection.extract_fd() }))
            .is_err()
    );
}
