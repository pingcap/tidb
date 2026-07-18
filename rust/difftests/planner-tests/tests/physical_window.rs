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

//! Dependency-closed vectors for PhysicalWindow metadata.
//!
//! The Go anchor is `TestCloneFineGrainedShuffleStreamCount` at
//! `pkg/planner/core/plan_test.go:681`.

use tidb_planner::physical_window::PhysicalWindowPlan;

#[test]
fn clone_keeps_window_stream_count_zero() {
    let plan = PhysicalWindowPlan::init("window", 0, 0);
    let cloned = plan.clone_plan();
    assert_eq!(cloned.plan_type(), "Window");
    assert_eq!(cloned.stream_count(), plan.stream_count());
}

#[test]
fn clone_keeps_window_stream_count_eight() {
    let plan = PhysicalWindowPlan::init("window", 0, 8);
    let cloned = plan.clone_plan();
    assert_eq!(cloned.stream_count(), 8);
    assert_eq!(cloned.stream_count(), plan.stream_count());
}

#[test]
fn init_and_explain_preserve_window_identity_and_suffix() {
    let plan = PhysicalWindowPlan::init("row_number() over(order by a)", 4, 8);
    assert_eq!(plan.plan_type(), "Window");
    assert_eq!(plan.query_block_offset(), 4);
    assert_eq!(
        plan.explain_info(),
        "row_number() over(order by a), stream_count: 8"
    );
}
