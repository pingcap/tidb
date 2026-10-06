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

//! Vectors for the wired PhysicalProjection operator.
//!
//! The Go anchor is `TestPushDownProjectionForMPP` at
//! `pkg/planner/core/casetest/mpp/mpp_test.go:710`.

use tidb_planner::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalProjection};

#[test]
fn projection_identity_and_stream_count_live_on_the_physical_tree() {
    let mut base = BasePhysicalPlan::with_id(1, "Projection", 4);
    base.tiflash_fine_grained_shuffle_stream_count = 8;
    let plan = PhysicalPlan::Projection(PhysicalProjection {
        base,
        ..PhysicalProjection::default()
    });
    assert_eq!(plan.tp(), "Projection");
    assert_eq!(plan.query_block_offset(), 4);
    assert_eq!(plan.base().tiflash_fine_grained_shuffle_stream_count, 8);
    assert_eq!(
        plan.clone_plan()
            .base()
            .tiflash_fine_grained_shuffle_stream_count,
        8
    );
}
