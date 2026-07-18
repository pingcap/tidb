// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-shaped BatchCommands priority queue.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::mem;

/// A request that can be ordered and retired by the batch scheduler.
pub trait PriorityItem {
    /// Returns the source request priority used by the max heap.
    fn priority(&self) -> u64;
    /// Whether the scheduler should discard this item before batching.
    fn is_canceled(&self) -> bool;
}

#[derive(Debug)]
struct HeapItem<T> {
    value: T,
}

impl<T: PriorityItem> PartialEq for HeapItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value.priority() == other.value.priority()
    }
}

impl<T: PriorityItem> Eq for HeapItem<T> {}

impl<T: PriorityItem> PartialOrd for HeapItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PriorityItem> Ord for HeapItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.priority().cmp(&other.value.priority())
    }
}

/// Source-shaped highest-priority-first queue.
#[derive(Debug)]
pub struct PriorityQueue<T> {
    heap: BinaryHeap<HeapItem<T>>,
}

impl<T> Default for PriorityQueue<T> {
    fn default() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }
}

impl<T: PriorityItem> PriorityQueue<T> {
    /// Creates an empty priority queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of queued items.
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Whether the queue contains no items.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Inserts one item according to its source priority.
    pub fn push(&mut self, value: T) {
        self.heap.push(HeapItem { value });
    }

    /// Returns the current maximum priority, or zero for an empty queue.
    pub fn highest_priority(&self) -> u64 {
        self.heap.peek().map_or(0, |entry| entry.value.priority())
    }

    /// Removes and returns the highest-priority item.
    pub fn pop(&mut self) -> Option<T> {
        self.heap.pop().map(|entry| entry.value)
    }

    /// Removes at most `count` items with client-go's exact `Take` ordering.
    ///
    /// Partial takes pop in priority order. A full take preserves the heap's
    /// raw backing order, matching the source fast path. Moving the heap out
    /// also clears every queue-owned reference.
    pub fn take(&mut self, count: usize) -> Vec<T> {
        if count >= self.len() {
            return mem::take(&mut self.heap)
                .into_vec()
                .into_iter()
                .map(|entry| entry.value)
                .collect();
        }

        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(value) = self.pop() {
                values.push(value);
            }
        }
        values
    }

    /// Removes every item while preserving the source full-take layout.
    pub fn drain(&mut self) -> Vec<T> {
        self.take(self.len())
    }

    /// Removes canceled items and releases their queue-owned references.
    pub fn clean_canceled(&mut self) {
        if self.heap.iter().any(|entry| entry.value.is_canceled()) {
            self.heap.retain(|entry| !entry.value.is_canceled());
        }
    }

    /// Clears every queued item without returning it.
    pub fn reset(&mut self) {
        self.heap.clear();
    }
}
