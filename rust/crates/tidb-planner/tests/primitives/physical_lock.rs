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

//! Vectors for the wired PhysicalLock planner.
//!
//! The Go anchor is `TestIssue52592ForNextGen` at
//! `pkg/planner/core/tests/pointget/point_get_plan_test.go:407`.

use tidb_planner::logical::{BaseLogicalPlan, LogicalLock, SelectLockType};
use tidb_planner::physical::{exhaust_physical_plans_4_logical_lock, PhysicalPlan};
use tidb_planner::physical_property::{PhysicalProperty, TaskType};
use tidb_planner::plan_base::PlanIdAllocator;

#[test]
fn mpp_lock_is_rejected_before_plan_creation() {
    let allocator = PlanIdAllocator::new();
    let lock = LogicalLock::new(
        BaseLogicalPlan::new(&allocator, LogicalLock::TYPE, 4),
        SelectLockType::ForUpdate,
    );
    let prop = PhysicalProperty {
        task_tp: TaskType::Mpp,
        ..PhysicalProperty::default()
    };
    assert!(exhaust_physical_plans_4_logical_lock(&lock, &prop, &allocator, 1.0).is_empty());
}

#[test]
fn point_get_lock_explain_info_preserves_source_text() {
    let allocator = PlanIdAllocator::new();
    let lock = LogicalLock::new(
        BaseLogicalPlan::new(&allocator, LogicalLock::TYPE, 4),
        SelectLockType::ForUpdate,
    );
    let plans =
        exhaust_physical_plans_4_logical_lock(&lock, &PhysicalProperty::default(), &allocator, 1.0);
    let PhysicalPlan::Lock(plan) = &plans[0] else {
        panic!("a wired PhysicalLock, got {:?}", plans[0]);
    };
    assert_eq!(plan.base.base.tp(), "SelectLock");
    assert_eq!(plan.base.base.query_block_offset(), 0);
    assert_eq!(plan.explain_info(), "for update 0");
}

#[test]
fn nonzero_wait_seconds_and_lock_type_are_lossless() {
    let allocator = PlanIdAllocator::new();
    let mut lock = LogicalLock::new(
        BaseLogicalPlan::new(&allocator, LogicalLock::TYPE, 0),
        SelectLockType::ForShare,
    );
    lock.wait_sec = 42;
    let plans =
        exhaust_physical_plans_4_logical_lock(&lock, &PhysicalProperty::default(), &allocator, 1.0);
    let PhysicalPlan::Lock(plan) = &plans[0] else {
        panic!("a wired PhysicalLock, got {:?}", plans[0]);
    };
    assert_eq!(plan.lock_type, SelectLockType::ForShare);
    assert_eq!(plan.wait_sec, 42);
    assert_eq!(plan.explain_info(), "for share 42");
}
