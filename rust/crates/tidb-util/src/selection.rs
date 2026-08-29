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

//! Complete transcreation of Go `pkg/util/selection`.
//!
//! Introselect starts with quickselect and falls back to median-of-medians when
//! recursion becomes deep. [`Selectable`] is the Rust-native equivalent of
//! Go's `sort.Interface`.

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
/// `k`-th smallest value (`k` starts from 1), or `-1` if `data` is empty.
pub fn select<T: Selectable + ?Sized>(data: &mut T, k: isize) -> isize {
    if data.is_empty() {
        return -1;
    }
    introselect(data, 0, data.len() as isize - 1, k - 1, 6)
}

/// Performs quickselect at the beginning, switching to the linear-time
/// algorithm if it recurses too many times.
///
/// Source paper: <http://www.cs.rpi.edu/~musser/gp/introsort.ps>
fn introselect<T: Selectable + ?Sized>(
    data: &mut T,
    left: isize,
    right: isize,
    k: isize,
    depth: i32,
) -> isize {
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

/// Go's test-only comparison implementation used by `BenchmarkSelection`.
#[cfg(any(test, feature = "testexport"))]
#[doc(hidden)]
pub fn quickselect<T: Selectable + ?Sized>(
    data: &mut T,
    left: isize,
    right: isize,
    k: isize,
) -> isize {
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
    left: isize,
    right: isize,
    k: isize,
) -> isize {
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

fn random_pivot(left: isize, right: isize) -> isize {
    left + fastrand::uint64_n((right - left + 1) as u64) as isize
}

fn median_of_medians_pivot<T: Selectable + ?Sized>(
    data: &mut T,
    left: isize,
    right: isize,
) -> isize {
    if right - left < 5 {
        return partition5(data, left, right);
    }
    let mut i = left;
    while i <= right {
        let sub_right = (i + 4).min(right);
        let median5 = partition5(data, i, sub_right);
        data.swap(median5 as usize, (left + (i - left) / 5) as usize);
        i += 5;
    }
    let mid = (right - left) / 10 + left + 1;
    median_of_medians(data, left, left + (right - left) / 5, mid)
}

fn partition<T: Selectable + ?Sized>(
    data: &mut T,
    left: isize,
    right: isize,
    pivot_index: isize,
) -> isize {
    data.swap(pivot_index as usize, right as usize);
    let mut store_index = left;
    for i in left..right {
        if data.less(i as usize, right as usize) {
            data.swap(store_index as usize, i as usize);
            store_index += 1;
        }
    }
    data.swap(right as usize, store_index as usize);
    store_index
}

fn partition_intro<T: Selectable + ?Sized>(
    data: &mut T,
    left: isize,
    right: isize,
    pivot_index: isize,
    k: isize,
) -> isize {
    data.swap(pivot_index as usize, right as usize);
    let mut store_index = left;
    // Move all elements smaller than the pivot to the left side.
    for i in left..right {
        if data.less(i as usize, right as usize) {
            data.swap(store_index as usize, i as usize);
            store_index += 1;
        }
    }
    let mut store_index_eq = store_index;
    // Move all elements equal to the pivot right after.
    for i in store_index..right {
        // data[i] == data[right]
        if !data.less(i as usize, right as usize) && !data.less(right as usize, i as usize) {
            data.swap(store_index_eq as usize, i as usize);
            store_index_eq += 1;
        }
    }
    // Move the pivot to its final place.
    data.swap(right as usize, store_index_eq as usize);
    if k < store_index {
        return store_index;
    }
    if k <= store_index_eq {
        return k;
    }
    store_index_eq
}

fn partition5<T: Selectable + ?Sized>(data: &mut T, left: isize, right: isize) -> isize {
    let mut i = left + 1;
    while i <= right {
        let mut j = i;
        while j > left && data.less(j as usize, (j - 1) as usize) {
            data.swap(j as usize, (j - 1) as usize);
            j -= 1;
        }
        i += 1;
    }
    (left + right) / 2
}

#[cfg(test)]
mod tests {
    use super::{select, Selectable};
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
        let index = select(&mut data, 3);
        assert_eq!(data.0[index as usize], 3);
    }

    // Go `TestSelectionWithDuplicate`.
    #[test]
    fn selection_with_duplicate() {
        let mut data = TestSlice(vec![1, 2, 3, 3, 5]);
        let index = select(&mut data, 3);
        assert_eq!(data.0[index as usize], 3);
        let index = select(&mut data, 5);
        assert_eq!(data.0[index as usize], 5);
    }

    // Go `TestSelectionWithRandomCase`.
    #[test]
    fn selection_with_random_case() {
        let mut data = random_test_case(1_000_000);
        let index = select(&mut data, 500_000);
        let actual = data.0[index as usize];
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
        let index = select(&mut data, 500_000);
        let actual = data.0[index as usize];
        data.0.sort_unstable();
        let expected = data.0[499_999];
        assert_eq!(expected, actual);
    }
}
