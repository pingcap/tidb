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

//! Vectors for the wired PhysicalUnionAll planning gates.
//!
//! The Go anchor is `TestMppUnionAll` at
//! `pkg/planner/core/casetest/mpp/mpp_test.go:446`.

use tidb_planner_core_operator_logicalop::{BaseLogicalPlan, LogicalUnionAll};
use tidb_planner_core_operator_physicalop::{exhaust_physical_plans_4_logical_union_all, PhysicalPlan};
use tidb_planner_property::{PhysicalProperty, SortItem, TaskType};
use tidb_planner_core_base::PlanIdAllocator;

/// Go `ExhaustPhysicalPlans4LogicalUnionAll` under an MPP requirement: no
/// candidate under a partition requirement, else one candidate, the MPP
/// union over MPP children when `IsMPPAllowed`.
#[test]
fn mpp_requirement_is_the_mpp_union_over_mpp_children() {
    let allocator = PlanIdAllocator::new();
    let mut base = BaseLogicalPlan::new(&allocator, LogicalUnionAll::TYPE, 0);
    base.set_children(vec![
        tidb_planner_core_operator_logicalop::LogicalPlan::TableDual(
            tidb_planner_core_operator_logicalop::LogicalTableDual::new(
                BaseLogicalPlan::new(&allocator, "TableDual", 0),
                1,
            ),
        ),
    ]);
    let union = LogicalUnionAll::new(base);
    let mpp = PhysicalProperty {
        task_tp: TaskType::Mpp,
        ..PhysicalProperty::default()
    };
    for (mpp_allowed, child_task_tp) in [(true, TaskType::Mpp), (false, TaskType::Root)] {
        let plans = exhaust_physical_plans_4_logical_union_all(&union, &mpp, &allocator, 1.0, mpp_allowed);
        assert_eq!(plans.len(), 1);
        let PhysicalPlan::UnionAll(plan) = &plans[0] else {
            panic!("a wired PhysicalUnionAll, got {:?}", plans[0]);
        };
        assert_eq!(plan.mpp, mpp_allowed);
        assert_eq!(plan.base.child_req_prop(0).expect("prop").task_tp, child_task_tp);
    }
    for mpp_partition_tp in [
        tidb_planner_property::MppPartitionType::Hash,
        tidb_planner_property::MppPartitionType::Broadcast,
        tidb_planner_property::MppPartitionType::SinglePartition,
    ] {
        let partitioned = PhysicalProperty { mpp_partition_tp, ..mpp.clone() };
        assert!(
            exhaust_physical_plans_4_logical_union_all(&union, &partitioned, &allocator, 1.0, true).is_empty()
        );
    }
}

#[test]
fn root_union_all_emits_source_candidate_order() {
    let allocator = PlanIdAllocator::new();
    let union = LogicalUnionAll::new(BaseLogicalPlan::new(&allocator, LogicalUnionAll::TYPE, 7));
    let plans = exhaust_physical_plans_4_logical_union_all(
        &union,
        &PhysicalProperty::default(),
        &allocator,
        1.0,
        false,
    );
    assert_eq!(plans.len(), 1, "MPP disabled yields one root candidate");
    let PhysicalPlan::UnionAll(plan) = &plans[0] else {
        panic!("a wired PhysicalUnionAll, got {:?}", plans[0]);
    };
    assert_eq!(plan.base.base.tp(), "Union");
    assert_eq!(plan.base.base.query_block_offset(), 7);
    assert!(!plan.mpp);
}

#[test]
fn sort_requests_are_rejected() {
    let allocator = PlanIdAllocator::new();
    let union = LogicalUnionAll::new(BaseLogicalPlan::new(&allocator, LogicalUnionAll::TYPE, 0));
    let sorted = PhysicalProperty {
        sort_items: vec![SortItem::from_unique_id(1, false)],
        ..PhysicalProperty::default()
    };
    assert!(
        exhaust_physical_plans_4_logical_union_all(&union, &sorted, &allocator, 1.0, false)
            .is_empty()
    );
}
