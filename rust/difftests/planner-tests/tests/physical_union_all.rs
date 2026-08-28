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

use tidb_planner::logical::{BaseLogicalPlan, LogicalUnionAll};
use tidb_planner::physical::{exhaust_physical_plans_4_logical_union_all, PhysicalPlan};
use tidb_planner::physical_property::{PhysicalProperty, SortItem, TaskType};
use tidb_planner::plan_base::PlanIdAllocator;

#[test]
fn unsupported_mpp_union_is_not_claimed_without_the_tiflash_tier() {
    let allocator = PlanIdAllocator::new();
    let union = LogicalUnionAll::new(BaseLogicalPlan::new(&allocator, LogicalUnionAll::TYPE, 0));
    let mpp = PhysicalProperty {
        task_tp: TaskType::Mpp,
        ..PhysicalProperty::default()
    };
    assert!(exhaust_physical_plans_4_logical_union_all(&union, &mpp, &allocator, 1.0).is_empty());
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
    );
    assert_eq!(
        plans.len(),
        1,
        "the TiFlash-less tier has one root candidate"
    );
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
        sort_items: vec![SortItem::new(1, false)],
        ..PhysicalProperty::default()
    };
    assert!(
        exhaust_physical_plans_4_logical_union_all(&union, &sorted, &allocator, 1.0).is_empty()
    );
}
