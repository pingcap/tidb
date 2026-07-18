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

//! Dependency-closed vectors for PhysicalSelection metadata.
//!
//! The Go anchor is `TestPushDownSelectionForMPP` at
//! `pkg/planner/core/casetest/mpp/mpp_test.go:673`.

use tidb_planner::physical_selection::PhysicalSelectionPlan;

#[test]
fn mpp_selection_explain_preserves_condition_and_stream_count() {
    let plan = PhysicalSelectionPlan::init("gt(test.t.a, 1)", 4, 8);
    assert_eq!(plan.plan_type(), "Selection");
    assert_eq!(plan.query_block_offset(), 4);
    assert_eq!(plan.explain_info(), "gt(test.t.a, 1), stream_count: 8");
}

#[test]
fn non_mpp_selection_explain_has_no_stream_suffix() {
    assert_eq!(
        PhysicalSelectionPlan::init("eq(test.t.a, 1)", 0, 0).explain_info(),
        "eq(test.t.a, 1)"
    );
}

#[test]
fn empty_condition_text_keeps_positive_stream_separator() {
    assert_eq!(
        PhysicalSelectionPlan::init("", 0, 1).explain_info(),
        ", stream_count: 1"
    );
}
