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

//! Dependency-closed vectors for PhysicalApply's join-dispatch boundary.
//!
//! The Go anchor is `TestPhysicalApplyIsNotPhysicalJoin` at
//! `pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1537`.

use tidb_planner::physical_apply::PhysicalApplyPlan;

#[test]
fn apply_keeps_apply_plan_identity_and_is_not_a_physical_join() {
    let plan = PhysicalApplyPlan::init(3);
    assert_eq!(plan.plan_type(), "Apply");
    assert_eq!(plan.query_block_offset(), 3);
    assert!(!plan.physical_join_implement());
}

#[test]
fn join_dispatch_boundary_does_not_depend_on_offset() {
    assert!(!PhysicalApplyPlan::init(i32::MIN).physical_join_implement());
    assert!(!PhysicalApplyPlan::init(i32::MAX).physical_join_implement());
}
