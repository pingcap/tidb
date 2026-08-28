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

//! Vectors for the wired PhysicalApply join-dispatch boundary.
//!
//! The Go anchor is `TestPhysicalApplyIsNotPhysicalJoin` at
//! `pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1537`.

use tidb_planner::physical::{BasePhysicalPlan, PhysicalApply, PhysicalPlan};

fn apply(offset: i32) -> PhysicalPlan {
    let mut apply = PhysicalApply::default();
    apply.hash_join.base = BasePhysicalPlan::with_id(1, "Apply", offset);
    PhysicalPlan::Apply(apply)
}

#[test]
fn apply_keeps_apply_plan_identity_and_is_not_a_physical_join() {
    let plan = apply(3);
    assert_eq!(plan.base().base.tp(), "Apply");
    assert_eq!(plan.base().base.query_block_offset(), 3);
    assert_eq!(plan.join_type(), None);
    assert_eq!(plan.inner_child_idx(), None);
}

#[test]
fn join_dispatch_boundary_does_not_depend_on_offset() {
    for offset in [i32::MIN, i32::MAX] {
        let plan = apply(offset);
        assert_eq!(plan.join_type(), None);
        assert_eq!(plan.inner_child_idx(), None);
    }
}
