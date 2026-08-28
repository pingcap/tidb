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

//! Vectors over the wired PhysicalTableDual planner path.
//!
//! The Go anchor is `TestTableDual` at
//! `pkg/planner/core/casetest/cbotest/cbo_test.go:367`.

use tidb_planner::{
    logical::{BaseLogicalPlan, LogicalTableDual},
    physical::{find_best_task_4_logical_table_dual, PhysicalPlan},
    physical_property::{PhysicalProperty, SortItem},
    plan_base::PlanIdAllocator,
};

fn logical_dual(allocator: &PlanIdAllocator, row_count: usize, offset: i32) -> LogicalTableDual {
    LogicalTableDual::new(
        BaseLogicalPlan::new(allocator, LogicalTableDual::TYPE, offset),
        row_count,
    )
}

#[test]
fn table_dual_explain_and_kind_match_source_plan_tree() {
    let allocator = PlanIdAllocator::new();
    let task = find_best_task_4_logical_table_dual(
        &logical_dual(&allocator, 0, 0),
        &PhysicalProperty::default(),
        &allocator,
    );
    let Some(PhysicalPlan::TableDual(plan)) = task.plan() else {
        panic!("the logical dual must produce the wired physical dual");
    };
    assert_eq!(plan.plan_type(), "TableDual");
    assert_eq!(plan.query_block_offset(), 0);
    assert_eq!(plan.explain_info(), "rows:0");
}

#[test]
fn one_row_dual_accepts_required_sort_property() {
    let allocator = PlanIdAllocator::new();
    let property = PhysicalProperty {
        sort_items: vec![SortItem::new(1, false)],
        ..PhysicalProperty::default()
    };
    let task = find_best_task_4_logical_table_dual(
        &logical_dual(&allocator, 1, 12),
        &property,
        &allocator,
    );
    assert!(!task.invalid());
    let Some(PhysicalPlan::TableDual(plan)) = task.plan() else {
        panic!("the one-row logical dual must produce a physical dual");
    };
    assert_eq!(plan.row_count(), 1);
    assert_eq!(plan.query_block_offset(), 12);
}

#[test]
fn multirow_dual_rejects_sort() {
    let allocator = PlanIdAllocator::new();
    let property = PhysicalProperty {
        sort_items: vec![SortItem::new(1, false)],
        ..PhysicalProperty::default()
    };
    assert!(find_best_task_4_logical_table_dual(
        &logical_dual(&allocator, 2, 0),
        &property,
        &allocator,
    )
    .invalid());
}
