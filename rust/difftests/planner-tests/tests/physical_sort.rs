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

//! Dependency-closed vectors for PhysicalSort metadata.
//!
//! Source anchors:
//! - `TestPhysicalPlanMemoryTrace` at `pkg/planner/core/physical_plan_test.go:582`
//! - `TestCloneFineGrainedShuffleStreamCount` at `pkg/planner/core/plan_test.go:681`
//! - `TestDeepClone` at `pkg/planner/core/planbuilder_test.go:277`

use tidb_planner::physical_sort::{PhysicalSortPlan, SortItem};

#[test]
fn sort_memory_usage_grows_when_by_item_is_added() {
    let empty = PhysicalSortPlan::init(vec![], false, 0, 0);
    let with_item = PhysicalSortPlan::init(vec![SortItem::new("a", false)], false, 0, 0);
    assert!(with_item.memory_usage() > empty.memory_usage());
}

#[test]
fn clone_preserves_sort_stream_count_for_zero_and_eight() {
    for stream_count in [0, 8] {
        let plan = PhysicalSortPlan::init(vec![], false, 0, stream_count);
        let cloned = plan.clone_plan();
        assert_eq!(cloned.stream_count(), stream_count);
        assert_eq!(cloned.stream_count(), plan.stream_count());
    }
}

#[test]
fn deep_clone_keeps_sort_items_independent() {
    let plan = PhysicalSortPlan::init(vec![SortItem::new("a", false)], false, 0, 0);
    let mut cloned = plan.clone_plan();
    cloned.by_items_mut()[0].set_explain_text("b");
    assert_eq!(plan.by_items()[0].explain_text(), "a");
    assert_eq!(cloned.by_items()[0].explain_text(), "b");
}

#[test]
fn explain_info_matches_source_sort_text_and_partial_metadata() {
    let plan = PhysicalSortPlan::init(
        vec![
            SortItem::new("test.t.a", false),
            SortItem::new("test.t.b", true),
        ],
        true,
        4,
        8,
    );
    assert_eq!(plan.plan_type(), "Sort");
    assert_eq!(plan.query_block_offset(), 4);
    assert!(plan.is_partial_sort());
    assert_eq!(
        plan.explain_info(),
        "test.t.a, test.t.b:desc, stream_count: 8"
    );
}
