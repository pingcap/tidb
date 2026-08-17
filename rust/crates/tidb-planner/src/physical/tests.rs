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

//! Semantic tests for the physical plan tree. Written, not transcreated; see
//! the note in `crate::logical::tests`.

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::schema::Schema;

use super::*;

fn scan(id: i32, columns: &[i64]) -> PhysicalPlan {
    let mut base = BasePhysicalPlan::with_id(id, "TableFullScan", 0);
    base.base.set_schema(Some(Schema::new(
        columns
            .iter()
            .map(|&unique_id| Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong)))
            .collect(),
    )));
    PhysicalPlan::TableScan(PhysicalTableScan {
        base,
        table_id: i64::from(id),
    })
}

fn selection(id: i32, child: PhysicalPlan) -> PhysicalPlan {
    let mut base = BasePhysicalPlan::with_id(id, "Selection", 0);
    base.set_children(vec![child]);
    PhysicalPlan::Selection(PhysicalSelection { base })
}

fn hash_join(id: i32, left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
    let mut base = BasePhysicalPlan::with_id(id, "HashJoin", 0);
    base.set_children(vec![left, right]);
    PhysicalPlan::HashJoin(PhysicalHashJoin {
        base,
        ..PhysicalHashJoin::default()
    })
}

#[test]
fn tree_construction_and_base_accessors() {
    let tree = hash_join(3, selection(2, scan(1, &[1])), scan(4, &[2]));
    assert_eq!(tree.plan_count(), 4);
    assert_eq!(tree.tp(), "HashJoin");
    assert_eq!(tree.explain_id(false), "HashJoin_3");
    assert_eq!(tree.explain_id(true), "HashJoin");
    assert_eq!(tree.query_block_offset(), 0);

    let mut order = Vec::new();
    tree.walk_preorder(&mut |node| order.push(node.id()));
    assert_eq!(order, vec![3, 2, 1, 4]);
    tree.dismantle();
}

#[test]
fn set_child_replaces_and_returns_the_previous_node() {
    let mut tree = selection(2, scan(1, &[1]));
    let previous = tree.set_child(0, scan(9, &[7])).expect("child 0 exists");
    assert_eq!(previous.id(), 1);
    assert_eq!(tree.children()[0].id(), 9);
    assert!(tree.set_child(3, scan(10, &[8])).is_none());
    tree.dismantle();
    previous.dismantle();
}

#[test]
fn schema_falls_through_to_the_first_child() {
    let tree = selection(2, scan(1, &[11, 12]));
    let schema = tree.schema().expect("inherited from the scan");
    assert_eq!(schema.columns.len(), 2);

    let bare = PhysicalPlan::Todo(TodoPhysicalOp {
        base: BasePhysicalPlan::with_id(1, "PhysicalWindow", 0),
        go_operator: "physicalop.PhysicalWindow".to_owned(),
    });
    assert!(bare.schema().is_none());
    tree.dismantle();
}

#[test]
fn stats_propagate_and_stats_count_reads_the_row_count() {
    let mut tree = selection(2, scan(1, &[1]));
    assert!(tree.stats_count().is_none());
    tree.set_stats(Some(StatsInfo::new(17.0, [(1_i64, 4.0)])));
    assert_eq!(tree.stats_count(), Some(17.0));
    // With no probe parents the display count is the raw count.
    assert_eq!(tree.est_row_count_for_display(), Some(17.0));
    assert_eq!(tree.actual_probe_cnt(), Some(1));

    tree.set_probe_parents(vec![99]);
    assert!(tree.est_row_count_for_display().is_none());
    assert!(tree.actual_probe_cnt().is_none());
    tree.dismantle();
}

#[test]
fn plan_cost_ver1_sums_the_children_and_caches() {
    let mut tree = hash_join(3, selection(2, scan(1, &[1])), scan(4, &[2]));
    let cost = tree
        .get_plan_cost_ver1(TaskType::Root, PlanCostOption::new(), false)
        .expect("the base body is dependency-closed");
    // The base body prices no operator, so the sum of nothing is zero.
    assert_eq!(cost, 0.0);
    assert!(tree.base().plan_cost_init);

    // A cached cost is returned without re-walking.
    tree.base_mut().plan_cost = 12.5;
    let cached = tree
        .get_plan_cost_ver1(TaskType::Root, PlanCostOption::new(), false)
        .expect("cached");
    assert_eq!(cached, 12.5);
    let recalculated = tree
        .get_plan_cost_ver1(TaskType::Root, PlanCostOption::new(), true)
        .expect("recalculated");
    assert_eq!(recalculated, 0.0);
    tree.dismantle();
}

#[test]
fn child_req_props_round_trip() {
    let mut tree = hash_join(3, scan(1, &[1]), scan(2, &[2]));
    assert!(tree.child_req_prop(0).is_none());
    tree.base_mut()
        .set_children_req_props(vec![Some(PhysicalProperty::default()), None]);
    assert!(tree.child_req_prop(0).is_some());
    assert!(tree.child_req_prop(1).is_none());
    tree.base_mut()
        .set_xth_child_req_props(1, Some(PhysicalProperty::default()));
    assert!(tree.child_req_prop(1).is_some());
    assert!(tree.child_req_prop(7).is_none());
    tree.dismantle();
}

#[test]
fn to_pb_refuses_exactly_as_the_go_base_body_does() {
    let tree = scan(4, &[1]);
    let err = tree.to_pb(StoreType::TiKv).expect_err("base body errors");
    assert_eq!(err.message(), "plan TableFullScan_4 fails converts to PB");
}

#[test]
fn join_accessors_answer_only_for_a_physical_join() {
    let tree = hash_join(3, scan(1, &[1]), scan(2, &[2]));
    assert_eq!(tree.join_type(), Some(LogicalJoinType::Inner));
    assert_eq!(tree.inner_child_idx(), Some(1));

    let scan_only = scan(5, &[1]);
    assert!(scan_only.join_type().is_none());
    assert!(scan_only.inner_child_idx().is_none());
    tree.dismantle();
}

#[test]
fn memory_usage_sums_the_whole_subtree() {
    let tree = selection(2, scan(1, &[1]));
    let own = tree.base().base.memory_usage();
    assert!(tree.memory_usage() > own);
    assert_eq!(
        tree.memory_usage(),
        own + tree.children()[0].base().base.memory_usage()
    );
    tree.dismantle();
}

#[test]
fn deep_chain_walks_and_tears_down_without_recursion() {
    const DEPTH: usize = 40_000;
    let mut node = scan(0, &[1]);
    for i in 1..DEPTH {
        node = selection(i as i32, node);
    }
    assert_eq!(node.plan_count(), DEPTH);
    node.resolve_indices().expect("the base body cannot fail");

    let copy = node.deep_clone();
    assert_eq!(copy.plan_count(), DEPTH);
    copy.dismantle();
    node.dismantle();
}

#[test]
fn clone_shallow_keeps_the_node_and_drops_the_children() {
    let tree = hash_join(3, scan(1, &[1]), scan(2, &[2]));
    let shallow = tree.clone_shallow();
    assert_eq!(shallow.id(), 3);
    assert_eq!(shallow.join_type(), Some(LogicalJoinType::Inner));
    assert_eq!(shallow.children().len(), 0);
    assert_eq!(tree.children().len(), 2);
    tree.dismantle();
}
