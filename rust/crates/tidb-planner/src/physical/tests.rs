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
        store_type: Default::default(),
        keep_order: false,
        desc: false,
        ranges: crate::ranger::types::Ranges::new(),
    })
}

fn selection(id: i32, child: PhysicalPlan) -> PhysicalPlan {
    let mut base = BasePhysicalPlan::with_id(id, "Selection", 0);
    base.set_children(vec![child]);
    PhysicalPlan::Selection(PhysicalSelection { base, ..PhysicalSelection::default() })
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

#[test]
fn max_one_row_enumeration_needs_an_orderless_root_property() {
    // `ExhaustPhysicalPlans4LogicalMaxOneRow`: a required order or a
    // TiFlash (MPP) property enumerates nothing; the admitted arm builds
    // exactly one PhysicalMaxOneRow whose child property caps ExpectedCnt
    // at 2 — one row to keep, one to prove the violation.
    use crate::logical::{BaseLogicalPlan, LogicalMaxOneRow};
    use crate::physical_property::SortItem;
    use crate::stats_info::StatsInfo;
    use crate::task_type::TaskType;

    let allocator = PlanIdAllocator::new();
    let mut base = BaseLogicalPlan::new(&allocator, LogicalMaxOneRow::TYPE, 7);
    base.base.set_stats(Some(StatsInfo::new(1.0, [])));
    let logical = LogicalMaxOneRow::new(base);

    let ordered = PhysicalProperty {
        sort_items: vec![SortItem::new(3, false)],
        ..PhysicalProperty::default()
    };
    assert!(exhaust_physical_plans_4_logical_max_one_row(&logical, &ordered, &allocator).is_empty());

    let mpp = PhysicalProperty {
        task_tp: TaskType::Mpp,
        ..PhysicalProperty::default()
    };
    assert!(exhaust_physical_plans_4_logical_max_one_row(&logical, &mpp, &allocator).is_empty());

    let plans = exhaust_physical_plans_4_logical_max_one_row(
        &logical,
        &PhysicalProperty::default(),
        &allocator,
    );
    assert_eq!(plans.len(), 1);
    let PhysicalPlan::MaxOneRow(mor) = &plans[0] else {
        panic!("a PhysicalMaxOneRow, got {:?}", plans[0]);
    };
    assert_eq!(mor.base.base.query_block_offset(), 7);
    assert!(
        (mor.base.base.stats_info().expect("stats").row_count() - 1.0).abs() < f64::EPSILON
    );
    let child_prop = mor.base.child_req_prop(0).expect("the child property");
    assert!((child_prop.expected_cnt - 2.0).abs() < f64::EPSILON);
    assert_eq!(child_prop.task_tp, TaskType::Root);
    assert!(child_prop.sort_items.is_empty());
}

#[test]
fn a_table_dual_is_born_inside_its_own_root_task() {
    // `findBestTask4LogicalTableDual` (`physical_table_dual.go:79`): a
    // 0/1-row dual satisfies any order vacuously, so only a required order
    // over MORE than one row answers the invalid task; the built plan
    // carries the logical dual's stats, offset, schema, names and RowCount.
    use crate::logical::{BaseLogicalPlan, LogicalTableDual};
    use crate::physical_property::SortItem;
    use crate::stats_info::StatsInfo;
    use tidb_datatype::FieldName;

    let allocator = PlanIdAllocator::new();
    let mut base = BaseLogicalPlan::new(&allocator, LogicalTableDual::TYPE, 3);
    base.base.set_stats(Some(StatsInfo::new(1.0, [])));
    base.base.set_schema(Some(Schema::default()));
    base.base.set_output_names(vec![FieldName::default()]);
    let mut dual = LogicalTableDual::new(base, 1);

    let sorted = PhysicalProperty {
        sort_items: vec![SortItem::new(3, false)],
        ..PhysicalProperty::default()
    };
    // One row: the order is vacuous, the task is real.
    let task = find_best_task_4_logical_table_dual(&dual, &sorted, &allocator);
    assert!(!task.invalid(), "a 1-row dual satisfies any order");

    // More than one row under a required order: Go's invalid task.
    dual.row_count = 2;
    let task = find_best_task_4_logical_table_dual(&dual, &sorted, &allocator);
    assert!(task.invalid());

    // The unordered build carries everything across.
    dual.row_count = 1;
    let task =
        find_best_task_4_logical_table_dual(&dual, &PhysicalProperty::default(), &allocator);
    let Some(PhysicalPlan::TableDual(built)) = task.plan() else {
        panic!("a PhysicalTableDual, got {:?}", task.plan());
    };
    assert_eq!(built.row_count, 1);
    assert_eq!(built.explain_info(), "rows:1");
    assert_eq!(built.base.base.query_block_offset(), 3);
    assert!(built.base.base.schema().is_some());
    assert_eq!(built.base.base.output_names().len(), 1);
    assert!(
        (built.base.base.stats_info().expect("stats").row_count() - 1.0).abs() < f64::EPSILON
    );
}

#[test]
fn a_cte_table_promises_no_order_and_is_born_in_a_root_task() {
    // `findBestTask4LogicalCTETable` (`physical_cte_table.go:56`): ANY
    // required sort answers the invalid task (unlike the dual there is no
    // 1-row escape); the built plan carries stats, schema, IDForStorage and
    // Go's fixed query-block offset 0, and explains as `Scan on CTE_N`.
    use crate::logical::{BaseLogicalPlan, LogicalCTETable};
    use crate::physical_property::SortItem;
    use crate::stats_info::StatsInfo;

    let allocator = PlanIdAllocator::new();
    let mut base = BaseLogicalPlan::new(&allocator, LogicalCTETable::TYPE, 5);
    base.base.set_stats(Some(StatsInfo::new(4.0, [])));
    base.base.set_schema(Some(Schema::default()));
    let cte = LogicalCTETable {
        base,
        seed_stat: None,
        name: "cte0".to_owned(),
        id_for_storage: 7,
        seed_schema: None,
    };

    let sorted = PhysicalProperty {
        sort_items: vec![SortItem::new(1, false)],
        ..PhysicalProperty::default()
    };
    assert!(find_best_task_4_logical_cte_table(&cte, &sorted, &allocator).invalid());

    let task = find_best_task_4_logical_cte_table(&cte, &PhysicalProperty::default(), &allocator);
    let Some(PhysicalPlan::CTETable(built)) = task.plan() else {
        panic!("a PhysicalCTETable, got {:?}", task.plan());
    };
    assert_eq!(built.id_for_storage, 7);
    assert_eq!(built.explain_info(), "Scan on CTE_7");
    assert_eq!(built.base.base.query_block_offset(), 0, "Go inits at offset 0");
    assert!(built.base.base.schema().is_some());
    assert!(
        (built.base.base.stats_info().expect("stats").row_count() - 4.0).abs() < f64::EPSILON
    );
}

#[test]
fn a_show_and_its_ddl_jobs_twin_are_born_in_root_tasks() {
    // `findBestTask4LogicalShow{,DDLJobs}` (`physical_show.go:75,91`): any
    // required sort answers the invalid task; the built plans carry the
    // logical contents/extractor/JobNumber and schema over Go's fixed
    // one-row pseudo stats at query-block offset 0.
    use crate::logical::{
        BaseLogicalPlan, LogicalShow, LogicalShowDDLJobs, ShowContents,
        ShowStatsMetaPredicateExtractor,
    };
    use crate::physical_property::SortItem;

    let allocator = PlanIdAllocator::new();
    let mut base = BaseLogicalPlan::new(&allocator, LogicalShow::TYPE, 0);
    base.base.set_schema(Some(Schema::default()));
    let mut show = LogicalShow::new(base, ShowContents::default());
    show.extractor = Some(ShowStatsMetaPredicateExtractor::default());

    let sorted = PhysicalProperty {
        sort_items: vec![SortItem::new(1, false)],
        ..PhysicalProperty::default()
    };
    assert!(find_best_task_4_logical_show(&show, &sorted, &allocator).invalid());

    let task = find_best_task_4_logical_show(&show, &PhysicalProperty::default(), &allocator);
    let Some(PhysicalPlan::Show(built)) = task.plan() else {
        panic!("a PhysicalShow, got {:?}", task.plan());
    };
    assert!(built.extractor.is_some(), "the extractor rides across");
    assert!(built.base.base.schema().is_some());
    assert!(
        (built.base.base.stats_info().expect("stats").row_count() - 1.0).abs() < f64::EPSILON,
        "Go's one-row pseudo stats"
    );

    let mut base = BaseLogicalPlan::new(&allocator, LogicalShowDDLJobs::TYPE, 0);
    base.base.set_schema(Some(Schema::default()));
    let jobs = LogicalShowDDLJobs::new(base, 30);
    assert!(find_best_task_4_logical_show_ddl_jobs(&jobs, &sorted, &allocator).invalid());
    let task =
        find_best_task_4_logical_show_ddl_jobs(&jobs, &PhysicalProperty::default(), &allocator);
    let Some(PhysicalPlan::ShowDDLJobs(built)) = task.plan() else {
        panic!("a PhysicalShowDDLJobs, got {:?}", task.plan());
    };
    assert_eq!(built.job_number, 30);
    assert_eq!(built.base.base.query_block_offset(), 0);
}

#[test]
fn a_selection_enumerates_one_root_candidate_with_scaled_stats() {
    // `ExhaustPhysicalPlans4LogicalSelection` (`physical_selection.go:54`):
    // one root-side candidate; stats scale to the parent's expected count
    // (ScaleByExpectCnt) and the child property drops CanAddEnforcer
    // (CloneEssentialFields does not copy it).
    use crate::logical::{BaseLogicalPlan, LogicalSelection};
    use crate::stats_info::StatsInfo;

    let allocator = PlanIdAllocator::new();
    let mut base = BaseLogicalPlan::new(&allocator, LogicalSelection::TYPE, 2);
    base.base.set_stats(Some(StatsInfo::new(100.0, [])));
    let selection = LogicalSelection::new(base, Vec::new());

    let prop = PhysicalProperty {
        expected_cnt: 10.0,
        can_add_enforcer: true,
        ..PhysicalProperty::default()
    };
    let plans = exhaust_physical_plans_4_logical_selection(&selection, &prop, &allocator, 1.0);
    assert_eq!(plans.len(), 1);
    let PhysicalPlan::Selection(built) = &plans[0] else {
        panic!("a Selection, got {:?}", plans[0]);
    };
    assert!(
        (built.base.base.stats_info().expect("stats").row_count() - 10.0).abs() < f64::EPSILON,
        "scaled 100 -> 10"
    );
    let child_prop = built.base.child_req_prop(0).expect("child prop");
    assert!(
        !child_prop.can_add_enforcer,
        "CloneEssentialFields drops CanAddEnforcer"
    );
    assert!((child_prop.expected_cnt - 10.0).abs() < f64::EPSILON);
}

#[test]
fn a_projection_maps_the_order_or_refuses_and_drops_constant_items() {
    // `TryToGetChildProp` + `tryTransformSortItems`
    // (`logical_projection.go:524,553`): a bare column maps through, a
    // scalar function refuses the enumeration, and Go's switch silently
    // DROPS an item projected from a Constant — a quirk reproduced, not
    // fixed.
    use crate::logical::{BaseLogicalPlan, LogicalProjection};
    use tidb_datatype::Datum;
    use tidb_expr::column::Column;
    use tidb_expr::expression::{Constant, Expression, ScalarFunction};
    use tidb_expr::schema::Schema;

    let allocator = PlanIdAllocator::new();
    let out = |id| Column::new(id, tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
    let mut schema = Schema::default();
    schema.columns = vec![out(101), out(102), out(103)];
    let mut base = BaseLogicalPlan::new(&allocator, LogicalProjection::TYPE, 0);
    base.base.set_schema(Some(schema));
    let exprs = vec![
        Expression::Column(out(1)),
        Expression::Constant(Constant::new(Datum::Null, tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong))),
        Expression::ScalarFunction(ScalarFunction::default()),
    ];
    let projection = LogicalProjection::new(base, exprs);

    // Column item 101 maps to child column 1; constant item 102 is DROPPED.
    let prop = PhysicalProperty::new(TaskType::Root, &[101, 102], false, f64::MAX, false);
    let child = projection
        .try_to_get_child_prop(&prop)
        .expect("column+constant order crosses");
    assert_eq!(child.sort_items.len(), 1, "the constant item vanished");
    assert_eq!(child.sort_items[0].col, 1);

    // A scalar-function item refuses the enumeration entirely.
    let prop = PhysicalProperty::new(TaskType::Root, &[103], false, f64::MAX, false);
    assert!(
        exhaust_physical_plans_4_logical_projection(&projection, &prop, &allocator, 1.0)
            .is_empty()
    );
}

#[test]
fn a_limit_enumerates_the_three_task_types_in_order() {
    // `ExhaustPhysicalPlans4LogicalLimit` (`physical_limit.go:53`): no
    // required order admitted; one candidate per task type in Go's fixed
    // order, each child property capped at Count + Offset.
    use crate::logical::{BaseLogicalPlan, LogicalLimit};

    let allocator = PlanIdAllocator::new();
    let base = BaseLogicalPlan::new(&allocator, LogicalLimit::TYPE, 0);
    let limit = LogicalLimit::new(base, 5, 20);

    let sorted = PhysicalProperty::new(TaskType::Root, &[1], false, f64::MAX, false);
    assert!(exhaust_physical_plans_4_logical_limit(&limit, &sorted, &allocator).is_empty());

    let plans =
        exhaust_physical_plans_4_logical_limit(&limit, &PhysicalProperty::default(), &allocator);
    assert_eq!(plans.len(), 3);
    let expected = [TaskType::CopSingleRead, TaskType::CopMultiRead, TaskType::Root];
    for (plan, tp) in plans.iter().zip(expected) {
        let PhysicalPlan::Limit(built) = plan else {
            panic!("a Limit, got {plan:?}");
        };
        assert_eq!(built.offset, 5);
        assert_eq!(built.count, 20);
        let child = built.base.child_req_prop(0).expect("child prop");
        assert_eq!(child.task_tp, tp);
        assert!((child.expected_cnt - 25.0).abs() < f64::EPSILON, "Count + Offset");
    }
}

#[test]
fn a_lock_enumerates_one_candidate_and_explains_go_style() {
    // `ExhaustPhysicalPlans4LogicalLock` (`physical_lock.go:44`) + the
    // `{LockType} {WaitSec}` ExplainInfo — Go prints WaitSec even for lock
    // types that carry none.
    use crate::logical::{BaseLogicalPlan, LogicalLock, SelectLockType};
    use crate::stats_info::StatsInfo;

    let allocator = PlanIdAllocator::new();
    let mut base = BaseLogicalPlan::new(&allocator, LogicalLock::TYPE, 0);
    base.base.set_stats(Some(StatsInfo::new(50.0, [])));
    let mut lock = LogicalLock::new(base, SelectLockType::ForUpdate);
    lock.wait_sec = 3;

    let mut mpp = PhysicalProperty::default();
    mpp.task_tp = TaskType::Mpp;
    assert!(exhaust_physical_plans_4_logical_lock(&lock, &mpp, &allocator, 1.0).is_empty());

    let plans =
        exhaust_physical_plans_4_logical_lock(&lock, &PhysicalProperty::default(), &allocator, 1.0);
    assert_eq!(plans.len(), 1);
    let PhysicalPlan::Lock(built) = &plans[0] else {
        panic!("a Lock, got {:?}", plans[0]);
    };
    assert_eq!(built.explain_info(), "for update 3");
    assert_eq!(built.base.base.query_block_offset(), 0, "Go inits at offset 0");
}

#[test]
fn a_union_all_fans_one_child_property_per_child() {
    // `ExhaustPhysicalPlans4LogicalUnionAll` (`physical_union_all.go:77`):
    // no order promised; one candidate with one per-child property carrying
    // the parent's expected count. The partition form re-stamps the type.
    use crate::logical::{
        BaseLogicalPlan, LogicalPartitionUnionAll, LogicalPlan, LogicalTableDual,
        LogicalUnionAll,
    };

    let allocator = PlanIdAllocator::new();
    let child = |offset| {
        LogicalPlan::TableDual(LogicalTableDual::new(
            BaseLogicalPlan::new(&allocator, LogicalTableDual::TYPE, offset),
            1,
        ))
    };
    let mut base = BaseLogicalPlan::new(&allocator, LogicalUnionAll::TYPE, 0);
    base.set_children(vec![child(0), child(0)]);
    let union = LogicalUnionAll::new(base);

    let sorted = PhysicalProperty::new(TaskType::Root, &[1], false, f64::MAX, false);
    assert!(
        exhaust_physical_plans_4_logical_union_all(&union, &sorted, &allocator, 1.0).is_empty()
    );

    let prop = PhysicalProperty {
        expected_cnt: 7.0,
        ..PhysicalProperty::default()
    };
    let plans = exhaust_physical_plans_4_logical_union_all(&union, &prop, &allocator, 1.0);
    assert_eq!(plans.len(), 1);
    let PhysicalPlan::UnionAll(built) = &plans[0] else {
        panic!("a UnionAll, got {:?}", plans[0]);
    };
    assert!(!built.mpp);
    assert!(built.base.child_req_prop(0).is_some() && built.base.child_req_prop(1).is_some());
    assert!(
        (built.base.child_req_prop(1).expect("prop").expected_cnt - 7.0).abs() < f64::EPSILON
    );

    let partition = LogicalPartitionUnionAll { union_all: union };
    let plans = exhaust_physical_plans_4_logical_partition_union_all(
        &partition,
        &prop,
        &allocator,
        1.0,
    );
    assert_eq!(plans[0].base().base.tp(), "PartitionUnion", "re-stamped");
}

#[test]
fn a_sequence_plans_producers_at_root_and_stamps_some_cte_failed_mpp() {
    // `ExhaustPhysicalPlans4LogicalSequence` (`physical_sequence.go:95`),
    // non-MPP arm: every producer child gets `{Root, MaxFloat64,
    // SomeCTEFailedMpp}`, the LAST child gets the parent's essential
    // property with SomeCTEFailedMpp stamped on, and the sequence's schema
    // is the last child's.
    use crate::logical::{BaseLogicalPlan, LogicalPlan, LogicalSequence, LogicalTableDual};
    use crate::physical_property::CteProducerStatus;

    let allocator = PlanIdAllocator::new();
    let child = || {
        LogicalPlan::TableDual(LogicalTableDual::new(
            BaseLogicalPlan::new(&allocator, LogicalTableDual::TYPE, 0),
            1,
        ))
    };
    let mut base = BaseLogicalPlan::new(&allocator, LogicalSequence::TYPE, 0);
    base.set_children(vec![child(), child(), child()]);
    let sequence = LogicalSequence::new(base);

    let prop = PhysicalProperty::new(TaskType::Root, &[9], false, 100.0, false);
    let plans = exhaust_physical_plans_4_logical_sequence(&sequence, &prop, &allocator);
    assert_eq!(plans.len(), 1);
    let PhysicalPlan::Sequence(built) = &plans[0] else {
        panic!("a Sequence, got {:?}", plans[0]);
    };
    for producer_idx in 0..2 {
        let producer = built.base.child_req_prop(producer_idx).expect("prop");
        assert_eq!(producer.task_tp, TaskType::Root);
        assert!((producer.expected_cnt - f64::MAX).abs() < f64::EPSILON);
        assert_eq!(
            producer.cte_producer_status,
            CteProducerStatus::SomeCteFailedMpp
        );
        assert!(producer.sort_items.is_empty());
    }
    let main = built.base.child_req_prop(2).expect("prop");
    assert_eq!(main.sort_items, prop.sort_items, "the main query keeps the order");
    assert!((main.expected_cnt - 100.0).abs() < f64::EPSILON);
    assert_eq!(main.cte_producer_status, CteProducerStatus::SomeCteFailedMpp);
    assert_eq!(PhysicalSequence::explain_info(), "Sequence Node");
}

#[test]
fn an_apply_copies_all_its_own_fields() {
    // `physical_apply.go`'s Clone: the embedded hash join plus
    // CanUseCache/Concurrency/KeepOrder/OuterSchema/NoDecorrelate all ride
    // the copy.
    use tidb_expr::expression::CorrelatedColumn;

    let apply = PhysicalPlan::Apply(PhysicalApply {
        hash_join: PhysicalHashJoin::default(),
        can_use_cache: true,
        concurrency: 4,
        keep_order: true,
        outer_schema: vec![CorrelatedColumn::default()],
        no_decorrelate: true,
    });
    let copied = apply.clone_shallow();
    let PhysicalPlan::Apply(copy) = &copied else {
        panic!("an Apply, got {copied:?}");
    };
    assert!(copy.can_use_cache);
    assert_eq!(copy.concurrency, 4);
    assert!(copy.keep_order);
    assert_eq!(copy.outer_schema.len(), 1);
    assert!(copy.no_decorrelate);
}
