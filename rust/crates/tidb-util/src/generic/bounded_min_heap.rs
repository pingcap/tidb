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

//! Transcreation of Go `pkg/util/generic/bounded_min_heap.go`.
//!
//! Go backs `BoundedMinHeap` with `container/heap` driven by an unexported
//! `internalHeap` that implements `heap.Interface`. Rust has no
//! `container/heap`, so [`InternalHeap`] carries the sift-up/sift-down logic
//! directly (mirroring `container/heap`'s `up`/`down`, and `Fix` = `down` else
//! `up`). Only `heap.Push` and `heap.Fix` are ever used by the bounded heap, so
//! there is no `Pop`; Go's `Push`/`Pop` exist only to satisfy the interface.
//!
//! The comparator is a runtime `Fn(&T, &T) -> i32` (Go's `func(T, T) int`),
//! taken by reference so non-`Copy` payloads work. Go panics when the
//! comparator is nil; a Rust `Fn` value cannot be nil, so that case is enforced
//! by the type system rather than a runtime check.

/// An unexported binary heap that keeps the worst item (smallest per `cmp`) at
/// the root, backing [`BoundedMinHeap`].
struct InternalHeap<T, F> {
    cmp: F,
    items: Vec<T>,
}

impl<T, F: Fn(&T, &T) -> i32> InternalHeap<T, F> {
    fn less(&self, i: usize, j: usize) -> bool {
        (self.cmp)(&self.items[i], &self.items[j]) < 0
    }

    /// Sifts the element at `j` up towards the root.
    fn up(&mut self, mut j: usize) {
        while j > 0 {
            let parent = (j - 1) / 2;
            if !self.less(j, parent) {
                break;
            }
            self.items.swap(j, parent);
            j = parent;
        }
    }

    /// Sifts the element at `i0` down; returns whether it moved.
    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            // Pick the smaller of the two children (left by default).
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && self.less(j2, j1) {
                j = j2;
            }
            if !self.less(j, i) {
                break;
            }
            self.items.swap(i, j);
            i = j;
        }
        i > i0
    }

    /// Equivalent of `heap.Push`: append then sift up.
    fn push(&mut self, item: T) {
        self.items.push(item);
        self.up(self.items.len() - 1);
    }

    /// Equivalent of `heap.Fix`: re-establish the heap invariant after the item
    /// at `i` changed.
    fn fix(&mut self, i: usize) {
        let n = self.items.len();
        if !self.down(i, n) {
            self.up(i);
        }
    }
}

/// Maintains the best `max_size` items efficiently using an internal min-heap.
///
/// It keeps the `max_size` best items according to the comparison function. The
/// root of the internal heap is always the worst item, making it easy to remove
/// when a better item arrives.
pub struct BoundedMinHeap<T, F> {
    data: InternalHeap<T, F>,
    max_size: usize,
}

impl<T, F: Fn(&T, &T) -> i32> BoundedMinHeap<T, F> {
    /// Creates a new bounded min-heap with the specified maximum size and
    /// comparison function.
    ///
    /// # Panics
    ///
    /// Panics if `max_size` is negative.
    #[must_use]
    pub fn new(max_size: i64, cmp_func: F) -> Self {
        assert!(max_size >= 0, "maxSize cannot be negative");
        Self {
            data: InternalHeap {
                cmp: cmp_func,
                items: Vec::with_capacity(max_size as usize),
            },
            max_size: max_size as usize,
        }
    }

    /// Returns the number of items in the heap.
    #[must_use]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.data.items.len()
    }

    /// Adds an item to the bounded min-heap. If the heap is full and the new
    /// item is better than the worst item, it replaces the worst item.
    pub fn add(&mut self, item: T) {
        // Handle the zero-capacity case.
        if self.max_size == 0 {
            return;
        }

        if self.data.items.len() < self.max_size {
            // Heap not full, just add the item.
            self.data.push(item);
            return;
        }

        // Heap is full; check if the new item is better than the worst (root of
        // the min-heap).
        if (self.data.cmp)(&item, &self.data.items[0]) > 0 {
            // New item is better, replace the worst.
            self.data.items[0] = item;
            self.data.fix(0);
        }
    }

    /// Returns all items in the heap as a sorted slice (best to worst).
    #[must_use]
    pub fn to_sorted_slice(&self) -> Vec<T>
    where
        T: Clone,
    {
        if self.data.items.is_empty() {
            return Vec::new();
        }

        // Copy items to avoid modifying the heap, then sort best-to-worst with a
        // negated comparator.
        let mut result = self.data.items.clone();
        result.sort_by(|a, b| {
            let ordering = -(self.data.cmp)(a, b);
            ordering.cmp(&0)
        });
        result
    }
}

#[cfg(test)]
mod tests {
    use super::BoundedMinHeap;

    // A simple test item with a value for comparison.
    #[derive(Clone)]
    struct TestItem {
        value: i32,
        name: String,
    }

    // Compares integers (for max-heap behavior, return negative for smaller
    // values).
    fn int_comparator(a: &i32, b: &i32) -> i32 {
        if a < b {
            -1
        } else if a > b {
            1
        } else {
            0
        }
    }

    // Compares TestItems by value.
    fn test_item_comparator(a: &TestItem, b: &TestItem) -> i32 {
        int_comparator(&a.value, &b.value)
    }

    // Go `TestBoundedMinHeapBasic`.
    #[test]
    fn bounded_min_heap_basic() {
        let mut bmh = BoundedMinHeap::new(3, int_comparator);

        // Test empty state.
        assert_eq!(bmh.len(), 0);
        assert!(bmh.to_sorted_slice().is_empty());

        // Test basic adding within capacity.
        bmh.add(5);
        bmh.add(3);
        bmh.add(8);
        assert_eq!(bmh.len(), 3);
        assert_eq!(bmh.to_sorted_slice(), vec![8, 5, 3]);

        // Test over capacity - should keep only top 3.
        for item in [1, 9, 2, 7, 4] {
            bmh.add(item);
        }
        assert_eq!(bmh.len(), 3);
        assert_eq!(bmh.to_sorted_slice(), vec![9, 8, 7]);

        // Test duplicate values.
        let mut bmh2 = BoundedMinHeap::new(3, int_comparator);
        for item in [5, 5, 3, 8, 5] {
            bmh2.add(item);
        }
        assert_eq!(bmh2.len(), 3);
        assert_eq!(bmh2.to_sorted_slice(), vec![8, 5, 5]);
    }

    // Go `TestBoundedMinHeapEdgeCases`.
    #[test]
    fn bounded_min_heap_edge_cases() {
        // Test single item capacity.
        let mut bmh1 = BoundedMinHeap::new(1, int_comparator);
        bmh1.add(3);
        bmh1.add(1);
        bmh1.add(7);
        bmh1.add(2);
        assert_eq!(bmh1.len(), 1);
        assert_eq!(bmh1.to_sorted_slice(), vec![7]);

        // Test zero capacity.
        let mut bmh0 = BoundedMinHeap::new(0, int_comparator);
        bmh0.add(5);
        bmh0.add(10);
        assert_eq!(bmh0.len(), 0);
        assert!(bmh0.to_sorted_slice().is_empty());
    }

    // Go `TestBoundedMinHeapCustomStruct`.
    #[test]
    fn bounded_min_heap_custom_struct() {
        let mut bmh = BoundedMinHeap::new(3, test_item_comparator);

        // Add custom struct items.
        bmh.add(TestItem {
            value: 10,
            name: "ten".to_string(),
        });
        bmh.add(TestItem {
            value: 5,
            name: "five".to_string(),
        });
        bmh.add(TestItem {
            value: 15,
            name: "fifteen".to_string(),
        });
        bmh.add(TestItem {
            value: 8,
            name: "eight".to_string(),
        });
        bmh.add(TestItem {
            value: 12,
            name: "twelve".to_string(),
        });

        assert_eq!(bmh.len(), 3);
        let result = bmh.to_sorted_slice();

        // Should have top 3 by value: 15, 12, 10.
        assert_eq!(result[0].value, 15);
        assert_eq!(result[0].name, "fifteen");
        assert_eq!(result[1].value, 12);
        assert_eq!(result[1].name, "twelve");
        assert_eq!(result[2].value, 10);
        assert_eq!(result[2].name, "ten");
    }

    // Go `TestBoundedMinHeapReverseComparator`.
    #[test]
    fn bounded_min_heap_reverse_comparator() {
        // Reverse comparator for min-heap behavior (keeping smallest values).
        let reverse_comparator = |a: &i32, b: &i32| -int_comparator(a, b);

        let mut bmh = BoundedMinHeap::new(3, reverse_comparator);

        for item in [9, 2, 7, 1, 8, 3] {
            bmh.add(item);
        }

        assert_eq!(bmh.len(), 3);
        // With the reverse comparator, should keep the smallest 3: 1, 2, 3.
        assert_eq!(bmh.to_sorted_slice(), vec![1, 2, 3]);
    }

    // Go `TestBoundedMinHeapItemReplacement`.
    #[test]
    fn bounded_min_heap_item_replacement() {
        let mut bmh = BoundedMinHeap::new(2, int_comparator);

        bmh.add(5);
        bmh.add(3);

        // Add better items - should replace worse ones.
        bmh.add(10);
        bmh.add(8);
        assert_eq!(bmh.len(), 2);
        assert_eq!(bmh.to_sorted_slice(), vec![10, 8]);

        // Try to add worse items - should be ignored.
        bmh.add(2);
        bmh.add(1);
        bmh.add(4);
        assert_eq!(bmh.len(), 2);
        assert_eq!(bmh.to_sorted_slice(), vec![10, 8]);

        // Test equal values behavior.
        let mut bmh2 = BoundedMinHeap::new(3, int_comparator);
        bmh2.add(5);
        bmh2.add(5);
        bmh2.add(5);
        bmh2.add(5); // Should not be added since heap is full and item is not better.
        assert_eq!(bmh2.len(), 3);
        assert_eq!(bmh2.to_sorted_slice(), vec![5, 5, 5]);
    }

    // Go `TestBoundedMinHeap_LargeDataset`.
    #[test]
    fn bounded_min_heap_large_dataset() {
        const CAPACITY: usize = 10;
        const DATA_SIZE: i32 = 1000;

        let mut bmh = BoundedMinHeap::new(CAPACITY as i64, int_comparator);

        // Add many items.
        for i in 0..DATA_SIZE {
            bmh.add(i);
        }

        assert_eq!(bmh.len(), CAPACITY);
        let result = bmh.to_sorted_slice();

        // Should have the top 10 values: 999, 998, ..., 990.
        assert_eq!(result.len(), CAPACITY);
        for (i, &v) in result.iter().enumerate() {
            assert_eq!(v, DATA_SIZE - 1 - i as i32);
        }
    }

    // Go `TestNewBoundedMinHeapSafetyChecks`. Go also asserts a panic on a nil
    // comparator; a Rust `Fn` value cannot be nil, so that case is enforced by
    // the type system and has no runtime test.
    #[test]
    fn new_bounded_min_heap_safety_checks() {
        // Negative maxSize panics.
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            let _ = BoundedMinHeap::new(-1, int_comparator);
        });
        std::panic::set_hook(prev);
        assert!(result.is_err());

        // Zero and positive maxSize do not panic.
        let _ = BoundedMinHeap::new(0, int_comparator);
        let _ = BoundedMinHeap::new(10, int_comparator);

        // Verify that a zero-capacity heap works correctly.
        let mut bmh = BoundedMinHeap::new(0, int_comparator);
        bmh.add(5);
        assert_eq!(bmh.len(), 0);
    }
}
