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

/// A request that can be ordered and retired by the batch scheduler.
pub trait PriorityItem {
    fn priority(&self) -> u64;
    fn is_canceled(&self) -> bool;
}

#[derive(Debug)]
struct HeapItem<T> {
    value: T,
    sequence: u64,
}

impl<T: PriorityItem> PartialEq for HeapItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value.priority() == other.value.priority() && self.sequence == other.sequence
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
        self.value
            .priority()
            .cmp(&other.value.priority())
            // Earlier arrivals win ties. Go's heap keeps the first equal item at
            // the root; making the tie explicit avoids depending on heap layout.
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

/// Highest-priority-first queue with stable ordering for equal priorities.
#[derive(Debug)]
pub struct PriorityQueue<T> {
    heap: BinaryHeap<HeapItem<T>>,
    next_sequence: u64,
}

impl<T> Default for PriorityQueue<T> {
    fn default() -> Self {
        Self {
            heap: BinaryHeap::new(),
            next_sequence: 0,
        }
    }
}

impl<T: PriorityItem> PriorityQueue<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn push(&mut self, value: T) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        self.heap.push(HeapItem { value, sequence });
    }

    pub fn highest_priority(&self) -> u64 {
        self.heap.peek().map_or(0, |entry| entry.value.priority())
    }

    pub fn pop(&mut self) -> Option<T> {
        self.heap.pop().map(|entry| entry.value)
    }

    /// Removes at most `count` items in priority order.
    ///
    /// Moving each item out of the heap also clears the queue's owning
    /// reference. This is the Rust equivalent of the explicit backing-array
    /// clearing required by client-go's `PriorityQueue.Take`.
    pub fn take(&mut self, count: usize) -> Vec<T> {
        let count = count.min(self.len());
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(value) = self.pop() {
                values.push(value);
            }
        }
        values
    }

    pub fn drain(&mut self) -> Vec<T> {
        self.take(self.len())
    }

    pub fn clean_canceled(&mut self) {
        if self.heap.iter().any(|entry| entry.value.is_canceled()) {
            self.heap.retain(|entry| !entry.value.is_canceled());
        }
    }

    pub fn reset(&mut self) {
        self.heap.clear();
    }
}
