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

//! Vectors for the wired PhysicalMaxOneRow planning contract.
//!
//! The Go anchor is `TestMaxOneRow` at
//! `pkg/executor/test/executor/executor_test.go:2157`; it proves the
//! user-visible subquery error that motivates this physical operator.

use tidb_planner::logical::{BaseLogicalPlan, LogicalMaxOneRow};
use tidb_planner::physical::{exhaust_physical_plans_4_logical_max_one_row, PhysicalPlan};
use tidb_planner::physical_property::{CteProducerStatus, PhysicalProperty, SortItem, TaskType};
use tidb_planner::plan_base::PlanIdAllocator;

#[test]
fn unsupported_sort_or_flash_requirements_do_not_emit_a_plan() {
    let allocator = PlanIdAllocator::new();
    let logical =
        LogicalMaxOneRow::new(BaseLogicalPlan::new(&allocator, LogicalMaxOneRow::TYPE, 0));
    let sorted = PhysicalProperty {
        sort_items: vec![SortItem::new(1, false)],
        ..PhysicalProperty::default()
    };
    assert!(exhaust_physical_plans_4_logical_max_one_row(&logical, &sorted, &allocator).is_empty());
    let mpp = PhysicalProperty {
        task_tp: TaskType::Mpp,
        ..PhysicalProperty::default()
    };
    assert!(exhaust_physical_plans_4_logical_max_one_row(&logical, &mpp, &allocator).is_empty());
}

#[test]
fn supported_plan_requests_two_rows_and_forwards_property_fields() {
    let allocator = PlanIdAllocator::new();
    let logical =
        LogicalMaxOneRow::new(BaseLogicalPlan::new(&allocator, LogicalMaxOneRow::TYPE, 0));
    let required = PhysicalProperty {
        cte_producer_status: CteProducerStatus::AllCteCanMpp,
        no_cop_push_down: true,
        ..PhysicalProperty::default()
    };
    let plans = exhaust_physical_plans_4_logical_max_one_row(&logical, &required, &allocator);
    let PhysicalPlan::MaxOneRow(plan) = &plans[0] else {
        panic!("a wired PhysicalMaxOneRow, got {:?}", plans[0]);
    };
    let child = plan.base.child_req_prop(0).expect("child property");
    assert_eq!(child.expected_cnt, 2.0);
    assert_eq!(child.cte_producer_status, CteProducerStatus::AllCteCanMpp);
    assert!(child.no_cop_push_down);
}
