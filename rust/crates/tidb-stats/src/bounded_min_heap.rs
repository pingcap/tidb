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

//! Fixed-capacity best-N heap from
//! `pkg/util/generic/bounded_min_heap.go`.
//!
//! The source keeps the worst item at the root of a comparator-defined
//! min-heap, replacing it only when a strictly better item arrives.  This
//! leaf preserves that direction, tie behavior, zero-capacity handling, and
//! non-mutating best-to-worst snapshots.  The generic item type is caller
//! owned; statistics TopN range tracking and histogram construction remain
//! external consumers.

use std::cmp::Ordering;

type Comparator<T> = Box<dyn Fn(&T, &T) -> i32>;

/// Maintains the best `max_size` items according to a caller comparator.
pub struct BoundedMinHeap<T> {
    items: Vec<T>,
    comparator: Comparator<T>,
    max_size: usize,
}

impl<T> BoundedMinHeap<T> {
    /// Creates a bounded heap.
    ///
    /// `None` models the source's nil comparator and panics with the same
    /// constructor error.  Negative capacities are rejected before allocation;
    /// zero is valid and makes [`Self::add`] a no-op.
    #[must_use]
    pub fn new<F>(max_size: isize, comparator: Option<F>) -> Self
    where
        F: Fn(&T, &T) -> i32 + 'static,
    {
        let comparator = comparator
            .map(|cmp| Box::new(cmp) as Comparator<T>)
            .unwrap_or_else(|| panic!("comparison function cannot be nil"));
        assert!(max_size >= 0, "maxSize cannot be negative");
        Self {
            items: Vec::with_capacity(max_size as usize),
            comparator,
            max_size: max_size as usize,
        }
    }

    /// Returns the number of retained items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns whether the heap currently retains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Adds an item, replacing the root only when the new item is strictly
    /// better than the current worst item.
    pub fn add(&mut self, item: T) {
        if self.max_size == 0 {
            return;
        }
        if self.items.len() < self.max_size {
            self.items.push(item);
            self.sift_up(self.items.len() - 1);
            return;
        }

        if (self.comparator)(&item, &self.items[0]) > 0 {
            self.items[0] = item;
            self.sift_down(0);
        }
    }

    /// Returns retained items sorted best to worst without changing the heap.
    #[must_use]
    pub fn to_sorted_slice(&self) -> Option<Vec<T>>
    where
        T: Clone,
    {
        if self.items.is_empty() {
            return None;
        }
        let mut result = self.items.clone();
        // The source uses `slices.SortFunc(result, func(a, b) int {
        // return -cmp(a, b)
        // })`. Negate the original comparison directly instead of calling it
        // in reverse: the API only promises sign semantics, so comparator
        // magnitudes need not be antisymmetric.
        result.sort_by(|a, b| match (self.comparator)(a, b).wrapping_neg() {
            value if value < 0 => Ordering::Less,
            value if value > 0 => Ordering::Greater,
            _ => Ordering::Equal,
        });
        Some(result)
    }

    fn sift_up(&mut self, mut index: usize) {
        while index > 0 {
            let parent = (index - 1) / 2;
            if (self.comparator)(&self.items[index], &self.items[parent]) >= 0 {
                break;
            }
            self.items.swap(index, parent);
            index = parent;
        }
    }

    fn sift_down(&mut self, mut index: usize) {
        loop {
            let left = index * 2 + 1;
            if left >= self.items.len() {
                break;
            }
            let right = left + 1;
            let child = if right < self.items.len()
                && (self.comparator)(&self.items[right], &self.items[left]) < 0
            {
                right
            } else {
                left
            };
            if (self.comparator)(&self.items[child], &self.items[index]) >= 0 {
                break;
            }
            self.items.swap(index, child);
            index = child;
        }
    }
}
