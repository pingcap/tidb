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

//! Source-shaped priority queue metadata from
//! `pkg/statistics/handle/autoanalyze/priorityqueue/heap.go`.
//!
//! The Go queue stores objects in a max heap while indexing them by table ID
//! so that an update can call `heap.Fix` and a delete can remove an arbitrary
//! element. This leaf keeps the same ownership boundary with a small scalar
//! item; construction of `AnalysisJob`, scheduling, and worker lifecycle stay
//! with the future auto-analyze owner.

use std::collections::HashMap;
use std::fmt;

use crate::{AnalysisIndicators, AnalysisJobKind};

#[path = "auto_analyze_queue.rs"]
mod live_queue;

pub use live_queue::DmlJobChange;
pub use live_queue::{LiveAnalysisQueue, LiveQueueSnapshot};

/// An item keyed by table ID and ordered by descending priority weight.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PriorityHeapItem {
    /// The table (or partition) identifier used as the queue key.
    pub table_id: i64,
    /// Owning logical table ID. For a static-partition job this differs from
    /// `table_id`, which is the physical partition ID returned by Go's
    /// `GetTableID`; other job families use the same value for both fields.
    pub global_table_id: i64,
    /// The source `AnalysisJob.GetWeight` value.
    pub weight: f64,
    /// Concrete source job family carried through the live queue.
    pub kind: AnalysisJobKind,
    /// Mutable ranking inputs owned by the analysis job.
    pub indicators: AnalysisIndicators,
}

impl PriorityHeapItem {
    /// Creates a queue item from source-owned metadata.
    #[must_use]
    pub const fn new(table_id: i64, weight: f64) -> Self {
        Self {
            table_id,
            global_table_id: table_id,
            weight,
            kind: AnalysisJobKind::NonPartitioned,
            indicators: AnalysisIndicators {
                change_percentage: 0.0,
                table_size: 0.0,
                last_analysis_duration_nanos: 0,
            },
        }
    }

    /// Creates a static-partition item with Go's exact split identity:
    /// heap/running state is keyed by the partition while a whole-table lock
    /// is still able to identify the owning table.
    #[must_use]
    pub const fn new_static_partition(
        global_table_id: i64,
        partition_id: i64,
        weight: f64,
    ) -> Self {
        let mut item = Self::new(partition_id, weight);
        item.global_table_id = global_table_id;
        item.kind = AnalysisJobKind::StaticPartitioned;
        item
    }

    /// Attaches the concrete job family and source priority indicators.
    #[must_use]
    pub const fn with_job_metadata(
        mut self,
        kind: AnalysisJobKind,
        indicators: AnalysisIndicators,
    ) -> Self {
        self.kind = kind;
        self.indicators = indicators;
        self
    }

    /// Source `AnalysisJob.GetTableID` identity.
    #[must_use]
    pub const fn table_id(self) -> i64 {
        self.table_id
    }

    /// Source `AnalysisJob.GetWeight` value.
    #[must_use]
    pub const fn weight(self) -> f64 {
        self.weight
    }

    /// Replaces the mutable source weight.
    pub fn set_weight(&mut self, weight: f64) {
        self.weight = weight;
    }

    /// Replaces the mutable source indicators.
    pub fn set_indicators(&mut self, indicators: AnalysisIndicators) {
        self.indicators = indicators;
    }
}

/// Errors returned by priority-queue operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PriorityHeapError {
    /// The queue has no items to inspect or remove.
    Empty,
    /// No item exists for the requested table ID.
    ObjectNotFound,
    /// A live queue API was called before initialization or after close.
    NotInitialized,
}

impl fmt::Display for PriorityHeapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("heap is empty"),
            Self::ObjectNotFound => formatter.write_str("object not found"),
            Self::NotInitialized => {
                formatter.write_str("analysis priority queue is not initialized")
            }
        }
    }
}

impl std::error::Error for PriorityHeapError {}

#[derive(Clone, Copy, Debug)]
struct HeapEntry {
    item: PriorityHeapItem,
    index: usize,
}

/// A max heap with O(1) table-ID lookup and indexed update/delete.
#[derive(Clone, Debug, Default)]
pub struct PriorityHeap {
    entries: HashMap<i64, HeapEntry>,
    queue: Vec<i64>,
}

impl PriorityHeap {
    /// Creates an empty priority queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an item, or replaces and re-heaps the item with the same key.
    pub fn add_or_update(&mut self, item: PriorityHeapItem) -> Result<(), PriorityHeapError> {
        if let Some(index) = self.entries.get(&item.table_id).map(|entry| entry.index) {
            self.entries
                .get_mut(&item.table_id)
                .expect("heap index must reference its entry")
                .item = item;
            self.fix(index);
            return Ok(());
        }

        let index = self.queue.len();
        self.queue.push(item.table_id);
        self.entries
            .insert(item.table_id, HeapEntry { item, index });
        self.sift_up(index);
        Ok(())
    }

    /// Alias for [`Self::add_or_update`], matching the Go queue API.
    pub fn update(&mut self, item: PriorityHeapItem) -> Result<(), PriorityHeapError> {
        self.add_or_update(item)
    }

    /// Removes an item by table ID.
    pub fn delete(&mut self, table_id: i64) -> Result<(), PriorityHeapError> {
        let index = self
            .entries
            .get(&table_id)
            .map(|entry| entry.index)
            .ok_or(PriorityHeapError::ObjectNotFound)?;
        self.remove_at(index);
        Ok(())
    }

    /// Returns the highest-weight item without removing it.
    pub fn peek(&self) -> Result<PriorityHeapItem, PriorityHeapError> {
        let table_id = *self.queue.first().ok_or(PriorityHeapError::Empty)?;
        Ok(self
            .entries
            .get(&table_id)
            .expect("heap queue must reference its entry")
            .item)
    }

    /// Removes and returns the highest-weight item.
    pub fn pop(&mut self) -> Result<PriorityHeapItem, PriorityHeapError> {
        if self.queue.is_empty() {
            return Err(PriorityHeapError::Empty);
        }
        Ok(self.remove_at(0))
    }

    /// Returns all items. Queue order is deterministic; the source map's
    /// iteration order is intentionally unspecified.
    #[must_use]
    pub fn list(&self) -> Vec<PriorityHeapItem> {
        self.queue
            .iter()
            .map(|table_id| {
                self.entries
                    .get(table_id)
                    .expect("heap queue must reference its entry")
                    .item
            })
            .collect()
    }

    /// Returns all table-ID keys in queue order.
    #[must_use]
    pub fn list_keys(&self) -> Vec<i64> {
        self.queue.clone()
    }

    /// Looks up an item by table ID.
    #[must_use]
    pub fn get(&self, table_id: i64) -> Option<PriorityHeapItem> {
        self.entries.get(&table_id).map(|entry| entry.item)
    }

    /// Alias for [`Self::get`], matching the Go helper name.
    #[must_use]
    pub fn get_by_key(&self, table_id: i64) -> Option<PriorityHeapItem> {
        self.get(table_id)
    }

    /// Returns the number of queued items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns whether no items are queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn fix(&mut self, index: usize) {
        if !self.sift_up(index) {
            self.sift_down(index);
        }
    }

    fn sift_up(&mut self, mut index: usize) -> bool {
        let original_index = index;
        while index > 0 {
            let parent = (index - 1) / 2;
            if !self.higher_priority(index, parent) {
                break;
            }
            self.swap_positions(index, parent);
            index = parent;
        }
        index != original_index
    }

    fn sift_down(&mut self, mut index: usize) {
        loop {
            let left = index * 2 + 1;
            if left >= self.queue.len() {
                return;
            }
            let right = left + 1;
            let mut child = left;
            if right < self.queue.len() && self.higher_priority(right, left) {
                child = right;
            }
            if !self.higher_priority(child, index) {
                return;
            }
            self.swap_positions(index, child);
            index = child;
        }
    }

    fn higher_priority(&self, left: usize, right: usize) -> bool {
        let left_entry = self
            .entries
            .get(&self.queue[left])
            .expect("heap queue must reference its entry");
        let right_entry = self
            .entries
            .get(&self.queue[right])
            .expect("heap queue must reference its entry");
        // Go's `>` comparison also returns false for NaN, so avoid
        // `partial_cmp(...).unwrap()` and keep IEEE behavior visible.
        left_entry.item.weight > right_entry.item.weight
    }

    fn swap_positions(&mut self, left: usize, right: usize) {
        self.queue.swap(left, right);
        self.entries
            .get_mut(&self.queue[left])
            .expect("heap queue must reference its entry")
            .index = left;
        self.entries
            .get_mut(&self.queue[right])
            .expect("heap queue must reference its entry")
            .index = right;
    }

    fn remove_at(&mut self, index: usize) -> PriorityHeapItem {
        let removed_table_id = self.queue[index];
        self.queue.swap_remove(index);
        if index < self.queue.len() {
            self.entries
                .get_mut(&self.queue[index])
                .expect("heap queue must reference its entry")
                .index = index;
            self.fix(index);
        }
        self.entries
            .remove(&removed_table_id)
            .expect("heap queue must reference its entry")
            .item
    }
}
