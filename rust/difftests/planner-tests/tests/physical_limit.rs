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

//! Dependency-closed vectors for PhysicalLimit metadata.
//!
//! The Go anchor is `TestLimitPushdown` at
//! `pkg/planner/core/casetest/physicalplantest/physical_plan_test.go:1600`.

use tidb_planner::physical_limit::{PhysicalLimitPlan, RedactMode};

#[test]
fn limit_plan_tree_metadata_matches_source() {
    let plan = PhysicalLimitPlan::init(0, 100, 2);
    assert_eq!(plan.plan_type(), "Limit");
    assert_eq!(plan.query_block_offset(), 2);
    assert_eq!(
        plan.explain_info(RedactMode::Disable),
        "offset:0, count:100"
    );
}

#[test]
fn redaction_modes_preserve_source_limit_shape() {
    let plan = PhysicalLimitPlan::init(4, 8, 0);
    assert_eq!(
        plan.explain_info(RedactMode::Marker),
        "offset:‹4›, count:‹8›"
    );
    assert_eq!(plan.explain_info(RedactMode::Enable), "offset:?, count:?");
}

#[test]
fn partition_and_prefix_metadata_are_caller_owned() {
    let plan = PhysicalLimitPlan::init(1, 2, 0)
        .with_partition_explain("partition by a")
        .with_prefix_col("a", 3);
    assert_eq!(
        plan.explain_info(RedactMode::Disable),
        "partition by a, offset:1, count:2, prefix_col:a, prefix_len:3"
    );
}
