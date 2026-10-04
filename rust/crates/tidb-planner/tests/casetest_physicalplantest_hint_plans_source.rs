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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use tidb_planner::physical::{PhysicalApply, PhysicalHashJoin, PhysicalPlan};

/// GO PORT of `physical_plan_test.go:1537 TestPhysicalApplyIsNotPhysicalJoin`.
///
/// RUNNING PORT. Go source:
/// `require.NotImplements(t, (*base.PhysicalJoin)(nil), new(physicalop.PhysicalApply))`
/// i.e. PhysicalApply embeds a hash join but deliberately does NOT satisfy
/// the `base.PhysicalJoin` interface (GetJoinType/GetInnerChildIdx,
/// plan_base.go:378-380). In this crate's enum model the same fact is asserted
/// structurally:
/// - [`PhysicalPlan::join_type`] answers Some ONLY for true join variants
///   (src/physical/mod.rs), mirroring the interface boundary Go enforces by
///   making Apply omit those methods.
#[test]
fn physical_apply_is_not_physical_join() {
    let apply = PhysicalPlan::Apply(PhysicalApply::default());
    // Go: new(physicalop.PhysicalApply) does not implement base.PhysicalJoin.
    assert_eq!(apply.join_type(), None);
    assert_eq!(apply.inner_child_idx(), None);

    // And a genuine hash join still DOES answer GetJoinType, showing the
    // boundary separates Apply from the join interface rather than being
    // absent entirely.
    let join = PhysicalPlan::HashJoin(PhysicalHashJoin::default());
    assert_eq!(
        join.join_type(),
        Some(tidb_planner::find_best_task::LogicalJoinType::Inner)
    );
}

// Go TestAllocMPPID runs in tidb-executor/src/mpp_query.rs, alongside the
// statement owner. Keeping the executable test there avoids a dependency
// cycle from the planner back to the executor's statement context.
