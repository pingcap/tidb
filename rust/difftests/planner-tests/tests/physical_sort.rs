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

//! Vectors for the wired PhysicalSort operator.
//!
//! Source anchors:
//! - `TestPhysicalPlanMemoryTrace` at `pkg/planner/core/physical_plan_test.go:582`
//! - `TestCloneFineGrainedShuffleStreamCount` at `pkg/planner/core/plan_test.go:681`
//! - `TestDeepClone` at `pkg/planner/core/planbuilder_test.go:277`

use tidb_planner::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalSort};
use tidb_planner::physical_property::SortItem;

fn sort(items: Vec<SortItem>, partial: bool, offset: i32, stream_count: u64) -> PhysicalSort {
    let mut base = BasePhysicalPlan::with_id(1, "Sort", offset);
    base.tiflash_fine_grained_shuffle_stream_count = stream_count;
    PhysicalSort {
        base,
        by_items: items,
        is_partial_sort: partial,
    }
}

#[test]
fn sort_memory_usage_grows_when_by_item_is_added() {
    let empty = PhysicalPlan::Sort(sort(vec![], false, 0, 0));
    let with_item = PhysicalPlan::Sort(sort(vec![SortItem::new(1, false)], false, 0, 0));
    assert!(with_item.memory_usage() > empty.memory_usage());
}

#[test]
fn clone_preserves_sort_stream_count_for_zero_and_eight() {
    for stream_count in [0, 8] {
        let plan = PhysicalPlan::Sort(sort(vec![], false, 0, stream_count));
        let cloned = plan.clone_plan();
        assert_eq!(
            cloned.base().tiflash_fine_grained_shuffle_stream_count,
            stream_count
        );
        assert_eq!(
            cloned.base().tiflash_fine_grained_shuffle_stream_count,
            plan.base().tiflash_fine_grained_shuffle_stream_count
        );
    }
}

#[test]
fn deep_clone_keeps_sort_items_independent() {
    let plan = PhysicalPlan::Sort(sort(vec![SortItem::new(1, false)], false, 0, 0));
    let mut cloned = plan.clone_plan();
    let PhysicalPlan::Sort(cloned_sort) = &mut cloned else {
        unreachable!();
    };
    cloned_sort.by_items[0].col = 2;
    let PhysicalPlan::Sort(original_sort) = &plan else {
        unreachable!();
    };
    assert_eq!(original_sort.by_items[0].col, 1);
    assert_eq!(cloned_sort.by_items[0].col, 2);
}

#[test]
fn sort_identity_partial_flag_and_stream_count_live_on_the_physical_tree() {
    let plan = sort(
        vec![SortItem::new(1, false), SortItem::new(2, true)],
        true,
        4,
        8,
    );
    assert_eq!(plan.base.base.tp(), "Sort");
    assert_eq!(plan.base.base.query_block_offset(), 4);
    assert!(plan.is_partial_sort);
    assert_eq!(plan.base.tiflash_fine_grained_shuffle_stream_count, 8);
}
