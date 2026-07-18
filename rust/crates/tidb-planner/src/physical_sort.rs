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

//! Dependency-closed metadata for the physical sort operator.
//!
//! The Go `PhysicalSort` owns typed expressions, schema/context and child
//! properties, task attachment, cost, index resolution, and protobuf
//! conversion. This leaf ports the stable plan metadata, source `ByItems`
//! explain formatting, clone field preservation, and the monotonic memory
//! accounting contract. Expression evaluation and execution remain external.

/// The source plan-codec type assigned by `PhysicalSort.Init`.
pub const PLAN_TYPE: &str = "Sort";

/// Caller-owned text and direction for one source `util.ByItems` entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortItem {
    explain_text: String,
    desc: bool,
}

impl SortItem {
    /// Creates a source-shaped sort item from already-rendered expression text.
    #[must_use]
    pub fn new(explain_text: impl Into<String>, desc: bool) -> Self {
        Self {
            explain_text: explain_text.into(),
            desc,
        }
    }

    /// Returns the caller-supplied expression text.
    #[must_use]
    pub fn explain_text(&self) -> &str {
        &self.explain_text
    }

    /// Returns whether the source item is descending.
    #[must_use]
    pub const fn is_desc(&self) -> bool {
        self.desc
    }

    /// Replaces expression text while retaining the source direction.
    pub fn set_explain_text(&mut self, explain_text: impl Into<String>) {
        self.explain_text = explain_text.into();
    }
}

/// Minimal initialized physical Sort metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalSortPlan {
    by_items: Vec<SortItem>,
    is_partial_sort: bool,
    query_block_offset: i32,
    stream_count: u64,
}

impl PhysicalSortPlan {
    /// Initializes source-shaped Sort metadata.
    #[must_use]
    pub fn init(
        by_items: Vec<SortItem>,
        is_partial_sort: bool,
        query_block_offset: i32,
        stream_count: u64,
    ) -> Self {
        Self {
            by_items,
            is_partial_sort,
            query_block_offset,
            stream_count,
        }
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(&self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the caller-owned query-block offset passed to `Init`.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        self.query_block_offset
    }

    /// Returns the source sort items.
    #[must_use]
    pub fn by_items(&self) -> &[SortItem] {
        &self.by_items
    }

    /// Returns mutable sort items for callers that own expression state.
    pub fn by_items_mut(&mut self) -> &mut [SortItem] {
        &mut self.by_items
    }

    /// Returns whether this sort is restricted to one partition.
    #[must_use]
    pub const fn is_partial_sort(&self) -> bool {
        self.is_partial_sort
    }

    /// Returns TiFlash's inherited fine-grained shuffle stream count.
    #[must_use]
    pub const fn stream_count(&self) -> u64 {
        self.stream_count
    }

    /// Clones plan metadata, including deep-owned sort-item text.
    #[must_use]
    pub fn clone_plan(&self) -> Self {
        self.clone()
    }

    /// Returns source `ExplainInfo` for the caller-supplied sort expressions.
    #[must_use]
    pub fn explain_info(&self) -> String {
        let mut result = String::new();
        for (index, item) in self.by_items.iter().enumerate() {
            if index > 0 {
                result.push_str(", ");
            }
            result.push_str(item.explain_text());
            if item.is_desc() {
                result.push_str(":desc");
            }
        }
        if self.stream_count > 0 {
            result.push_str(&format!(", stream_count: {}", self.stream_count));
        }
        result
    }

    /// Returns a monotonic metadata-size estimate matching the source growth
    /// contract used by `TestPhysicalPlanMemoryTrace`.
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        // The base-plan allocation is opaque here; preserve the source
        // observable property that every ByItems entry increases usage.
        1 + self
            .by_items
            .iter()
            .map(|item| 1 + item.explain_text.len())
            .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalSortPlan, SortItem, PLAN_TYPE};

    #[test]
    fn init_preserves_sort_kind_offset_and_partial_flag() {
        let plan = PhysicalSortPlan::init(vec![SortItem::new("a", false)], true, -2, 0);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "Sort");
        assert_eq!(plan.query_block_offset(), -2);
        assert!(plan.is_partial_sort());
    }

    #[test]
    fn explain_items_match_source_direction_and_stream_suffix() {
        let plan = PhysicalSortPlan::init(
            vec![SortItem::new("a", false), SortItem::new("b", true)],
            false,
            0,
            8,
        );
        assert_eq!(plan.explain_info(), "a, b:desc, stream_count: 8");
    }

    #[test]
    fn zero_stream_count_has_no_suffix_and_empty_items_are_valid() {
        assert_eq!(
            PhysicalSortPlan::init(vec![], false, 0, 0).explain_info(),
            ""
        );
    }

    #[test]
    fn clone_preserves_stream_count_and_partial_sort() {
        let plan = PhysicalSortPlan::init(vec![], true, 0, 8);
        let cloned = plan.clone_plan();
        assert_eq!(cloned.stream_count(), plan.stream_count());
        assert_eq!(cloned.is_partial_sort(), plan.is_partial_sort());
    }

    #[test]
    fn clone_deep_copies_sort_item_text() {
        let plan = PhysicalSortPlan::init(vec![SortItem::new("a", false)], false, 0, 0);
        let mut cloned = plan.clone_plan();
        cloned.by_items_mut()[0].set_explain_text("b");
        assert_eq!(plan.by_items()[0].explain_text(), "a");
        assert_eq!(cloned.by_items()[0].explain_text(), "b");
    }

    #[test]
    fn each_sort_item_increases_memory_usage() {
        let empty = PhysicalSortPlan::init(vec![], false, 0, 0);
        let with_item = PhysicalSortPlan::init(vec![SortItem::new("a", false)], false, 0, 0);
        assert!(with_item.memory_usage() > empty.memory_usage());
    }
}
