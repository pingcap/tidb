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

//! Complete transcreation of Go `pkg/util/selection` (`selection.go`).
//!
//! `introselect`: quickselect that falls back to the linear-time
//! median-of-medians algorithm when it recurses too deeply. Go operates over
//! `sort.Interface`; the Rust equivalent is the [`Selectable`] trait
//! (`len`/`less`/`swap`). Go's `-1`-for-empty sentinel becomes `None`.
//!
//! `main_test.go` is a goroutine-leak `TestMain` with no observable behavior of
//! its own; it has no Rust equivalent.

use crate::fastrand;

/// A collection whose elements can be compared and swapped by index, mirroring
/// Go's `sort.Interface`.
pub trait Selectable {
    /// The number of elements.
    fn len(&self) -> usize;
    /// Reports whether the element at `i` is less than the element at `j`.
    fn less(&self, i: usize, j: usize) -> bool;
    /// Swaps the elements at `i` and `j`.
    fn swap(&mut self, i: usize, j: usize);
    /// Reports whether the collection is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Performs the introselect algorithm on `data` and returns the index of the
/// `k`-th smallest value (`k` starts from 1), or `None` if `data` is empty.
pub fn select<T: Selectable + ?Sized>(data: &mut T, k: usize) -> Option<usize> {
    if data.len() > 0 {
        Some(introselect(data, 0, data.len() - 1, k - 1, 6))
    } else {
        None
    }
}

/// Performs quickselect at the beginning, switching to the linear-time
/// algorithm if it recurses too many times.
///
/// Source paper: <http://www.cs.rpi.edu/~musser/gp/introsort.ps>
fn introselect<T: Selectable + ?Sized>(
    data: &mut T,
    left: usize,
    right: usize,
    k: usize,
    depth: i32,
) -> usize {
    if left == right {
        return left;
    }
    if depth <= 0 {
        // Use the median-of-medians algorithm (linear-time selection) when it
        // recurses too many times.
        return median_of_medians(data, left, right, k);
    }
    // TODO: use a better pivot function
    let pivot_index = random_pivot(left, right);
    let pivot_index = partition(data, left, right, pivot_index);
    if k == pivot_index {
        k
    } else if k < pivot_index {
        introselect(data, left, pivot_index - 1, k, depth - 1)
    } else {
        introselect(data, pivot_index + 1, right, k, depth - 1)
    }
}

/// Plain quickselect, used only for test/benchmark comparison (Go marks it
/// `//nolint: unused`).
#[allow(dead_code)]
fn quickselect<T: Selectable + ?Sized>(data: &mut T, left: usize, right: usize, k: usize) -> usize {
    if left == right {
        return left;
    }
    let pivot_index = random_pivot(left, right);
    let pivot_index = partition(data, left, right, pivot_index);
    if k == pivot_index {
        k
    } else if k < pivot_index {
        quickselect(data, left, pivot_index - 1, k)
    } else {
        quickselect(data, pivot_index + 1, right, k)
    }
}

fn median_of_medians<T: Selectable + ?Sized>(
    data: &mut T,
    left: usize,
    right: usize,
    k: usize,
) -> usize {
    if left == right {
        return left;
    }
    let pivot_index = median_of_medians_pivot(data, left, right);
    let pivot_index = partition_intro(data, left, right, pivot_index, k);
    if k == pivot_index {
        k
    } else if k < pivot_index {
        median_of_medians(data, left, pivot_index - 1, k)
    } else {
        median_of_medians(data, pivot_index + 1, right, k)
    }
}

fn random_pivot(left: usize, right: usize) -> usize {
    left + fastrand::uint32_n((right - left + 1) as u32) as usize
}

fn median_of_medians_pivot<T: Selectable + ?Sized>(
    data: &mut T,
    left: usize,
    right: usize,
) -> usize {
    if right - left < 5 {
        return partition5(data, left, right);
    }
    let mut i = left;
    while i <= right {
        let sub_right = (i + 4).min(right);
        let median5 = partition5(data, i, sub_right);
        data.swap(median5, left + (i - left) / 5);
        i += 5;
    }
    let mid = (right - left) / 10 + left + 1;
    median_of_medians(data, left, left + (right - left) / 5, mid)
}

fn partition<T: Selectable + ?Sized>(
    data: &mut T,
    left: usize,
    right: usize,
    pivot_index: usize,
) -> usize {
    data.swap(pivot_index, right);
    let mut store_index = left;
    for i in left..right {
        if data.less(i, right) {
            data.swap(store_index, i);
            store_index += 1;
        }
    }
    data.swap(right, store_index);
    store_index
}

fn partition_intro<T: Selectable + ?Sized>(
    data: &mut T,
    left: usize,
    right: usize,
    pivot_index: usize,
    k: usize,
) -> usize {
    data.swap(pivot_index, right);
    let mut store_index = left;
    // Move all elements smaller than the pivot to the left side.
    for i in left..right {
        if data.less(i, right) {
            data.swap(store_index, i);
            store_index += 1;
        }
    }
    let mut store_index_eq = store_index;
    // Move all elements equal to the pivot right after.
    for i in store_index..right {
        // data[i] == data[right]
        if !data.less(i, right) && !data.less(right, i) {
            data.swap(store_index_eq, i);
            store_index_eq += 1;
        }
    }
    // Move the pivot to its final place.
    data.swap(right, store_index_eq);
    if k < store_index {
        return store_index;
    }
    if k <= store_index_eq {
        return k;
    }
    store_index_eq
}

fn partition5<T: Selectable + ?Sized>(data: &mut T, left: usize, right: usize) -> usize {
    let mut i = left + 1;
    while i <= right {
        let mut j = i;
        while j > left && data.less(j, j - 1) {
            data.swap(j, j - 1);
            j -= 1;
        }
        i += 1;
    }
    (left + right) / 2
}

#[cfg(test)]
mod tests {
    use super::{quickselect, select, Selectable};
    use crate::fastrand;

    struct TestSlice(Vec<i32>);

    impl Selectable for TestSlice {
        fn len(&self) -> usize {
            self.0.len()
        }
        fn less(&self, i: usize, j: usize) -> bool {
            self.0[i] < self.0[j]
        }
        fn swap(&mut self, i: usize, j: usize) {
            self.0.swap(i, j);
        }
    }

    fn random_test_case(size: usize) -> TestSlice {
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push((fastrand::uint32_n(100)) as i32);
        }
        TestSlice(data)
    }

    fn serial_test_case(size: usize) -> TestSlice {
        TestSlice((0..size as i32).collect())
    }

    // Go `TestSelection`.
    #[test]
    fn selection() {
        let mut data = TestSlice(vec![1, 2, 3, 4, 5]);
        let index = select(&mut data, 3).unwrap();
        assert_eq!(data.0[index], 3);
    }

    // Go `TestSelectionWithDuplicate`.
    #[test]
    fn selection_with_duplicate() {
        let mut data = TestSlice(vec![1, 2, 3, 3, 5]);
        let index = select(&mut data, 3).unwrap();
        assert_eq!(data.0[index], 3);
        let index = select(&mut data, 5).unwrap();
        assert_eq!(data.0[index], 5);
    }

    // Go `TestSelectionWithRandomCase`.
    #[test]
    fn selection_with_random_case() {
        let mut data = random_test_case(1_000_000);
        let index = select(&mut data, 500_000).unwrap();
        let actual = data.0[index];
        data.0.sort_unstable();
        let expected = data.0[499_999];
        assert_eq!(expected, actual);
    }

    // Go `TestSelectionWithSerialCase`.
    #[test]
    fn selection_with_serial_case() {
        let mut data = serial_test_case(1_000_000);
        // sort in reverse order
        data.0.sort_unstable_by(|a, b| b.cmp(a));
        let index = select(&mut data, 500_000).unwrap();
        let actual = data.0[index];
        data.0.sort_unstable();
        let expected = data.0[499_999];
        assert_eq!(expected, actual);
    }

    // Exercises `quickselect` (Go covers it only via `BenchmarkSelection`),
    // verifying its result matches a full sort.
    #[test]
    fn quickselect_matches_sort() {
        let mut data = random_test_case(10_000);
        let k = 5_000;
        let index = quickselect(&mut data, 0, 9_999, k - 1);
        let actual = data.0[index];
        data.0.sort_unstable();
        assert_eq!(data.0[k - 1], actual);
    }
}
