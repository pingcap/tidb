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

//! Vectors for the wired physical SHOW operators.
//!
//! The Go anchor is `TestShow` at `pkg/planner/core/planbuilder_test.go:63`.

use tidb_planner::logical::{BaseLogicalPlan, LogicalShow, LogicalShowDDLJobs, ShowContents};
use tidb_planner::physical::{
    find_best_task_4_logical_show, find_best_task_4_logical_show_ddl_jobs, PhysicalPlan,
};
use tidb_planner::physical_property::{PhysicalProperty, SortItem};
use tidb_planner::plan_base::PlanIdAllocator;

#[test]
fn regular_show_uses_pseudo_one_row_stats() {
    let allocator = PlanIdAllocator::new();
    let show = LogicalShow::new(
        BaseLogicalPlan::new(&allocator, LogicalShow::TYPE, 8),
        ShowContents::default(),
    );
    let task = find_best_task_4_logical_show(&show, &PhysicalProperty::default(), &allocator);
    let Some(PhysicalPlan::Show(plan)) = task.plan() else {
        panic!("a wired PhysicalShow, got {:?}", task.plan());
    };
    assert_eq!(plan.base.base.tp(), "Show");
    assert_eq!(
        plan.base
            .base
            .stats_info()
            .expect("pseudo stats")
            .row_count(),
        1.0
    );
}

#[test]
fn ddl_jobs_show_keeps_job_number_and_gate_behavior() {
    let allocator = PlanIdAllocator::new();
    let jobs = LogicalShowDDLJobs::new(
        BaseLogicalPlan::new(&allocator, LogicalShowDDLJobs::TYPE, 8),
        12,
    );
    let task =
        find_best_task_4_logical_show_ddl_jobs(&jobs, &PhysicalProperty::default(), &allocator);
    let Some(PhysicalPlan::ShowDDLJobs(plan)) = task.plan() else {
        panic!("a wired PhysicalShowDDLJobs, got {:?}", task.plan());
    };
    assert_eq!(plan.job_number, 12);

    let sorted = PhysicalProperty {
        sort_items: vec![SortItem::new(1, false)],
        ..PhysicalProperty::default()
    };
    assert!(find_best_task_4_logical_show_ddl_jobs(&jobs, &sorted, &allocator).invalid());
}
