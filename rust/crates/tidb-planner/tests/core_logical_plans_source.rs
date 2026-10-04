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

use tidb_planner::logical::{data_source::DataSource, BaseLogicalPlan};
use tidb_planner::plan_base::PlanIdAllocator;

/// GO PORT of `pkg/planner/core/logical_plans_test.go:761 TestAllocID`.
///
/// Go builds two DataSources against ONE mock session
/// (`coretestsdk.MockContext()`, :762) with
/// `pA := logicalop.DataSource{}.Init(ctx, 0)` and `pB := ...` (:767-768),
/// asserting `require.Equal(t, pB.ID(), pA.ID()+1)` (:769). `Init` draws each
/// id from `ctx.GetSessionVars().PlanID.Add(1)`, so the pin is: the allocator
/// is shared across operators, monotonic, and consecutive (no gaps between two
/// fresh inits). Rust keeps the same contract explicitly: one
/// [`PlanIdAllocator`] stands in for the session counter (its doc cites the
/// identical Go call) and `DataSource::new` over `BaseLogicalPlan::new`
/// mirrors `DataSource{}.Init`.
#[test]
fn alloc_id_two_inits_get_consecutive_ids_from_one_allocator() {
    let allocator = PlanIdAllocator::new();
    let p_a = DataSource::new(BaseLogicalPlan::new(&allocator, DataSource::TYPE, 0), 48, "t");
    let p_b = DataSource::new(BaseLogicalPlan::new(&allocator, DataSource::TYPE, 0), 48, "t");
    assert_eq!(p_b.base.base.id(), p_a.base.base.id() + 1);
}
