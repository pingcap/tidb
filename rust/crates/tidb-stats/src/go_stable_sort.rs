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

//! Comparison schedule used by Go `slices.SortStableFunc`.
//!
//! A total ordering only needs the final permutation to match. TiDB's
//! `sortSampleItems`, however, stores the error from every comparator call in
//! one outer variable, so the returned error also depends on the comparison
//! sequence. This is Go's insertion-block plus SymMerge algorithm, not an
//! interchangeable Rust stable sort.

use std::cmp::Ordering;

pub(crate) fn go_stable_sort_by<T>(data: &mut [T], mut compare: impl FnMut(&T, &T) -> Ordering) {
    stable(data, &mut compare);
}

fn insertion_sort<T>(
    data: &mut [T],
    start: usize,
    end: usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) {
    for index in start + 1..end {
        let mut cursor = index;
        while cursor > start && compare(&data[cursor], &data[cursor - 1]) == Ordering::Less {
            data.swap(cursor, cursor - 1);
            cursor -= 1;
        }
    }
}

fn stable<T>(data: &mut [T], compare: &mut impl FnMut(&T, &T) -> Ordering) {
    let length = data.len();
    let mut block_size = 20_usize;
    let mut start = 0_usize;
    let mut end = block_size;
    while end <= length {
        insertion_sort(data, start, end, compare);
        start = end;
        end += block_size;
    }
    insertion_sort(data, start, length, compare);

    while block_size < length {
        start = 0;
        end = 2 * block_size;
        while end <= length {
            sym_merge(data, start, start + block_size, end, compare);
            start = end;
            end += 2 * block_size;
        }
        let middle = start + block_size;
        if middle < length {
            sym_merge(data, start, middle, length, compare);
        }
        block_size *= 2;
    }
}

fn sym_merge<T>(
    data: &mut [T],
    start_bound: usize,
    middle: usize,
    end_bound: usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) {
    if middle - start_bound == 1 {
        let mut start = middle;
        let mut end = end_bound;
        while start < end {
            let half = start.wrapping_add(end) >> 1;
            if compare(&data[half], &data[start_bound]) == Ordering::Less {
                start = half + 1;
            } else {
                end = half;
            }
        }
        for cursor in start_bound..start.saturating_sub(1) {
            data.swap(cursor, cursor + 1);
        }
        return;
    }

    if end_bound - middle == 1 {
        let mut start = start_bound;
        let mut end = middle;
        while start < end {
            let half = start.wrapping_add(end) >> 1;
            if compare(&data[middle], &data[half]) != Ordering::Less {
                start = half + 1;
            } else {
                end = half;
            }
        }
        for cursor in (start + 1..=middle).rev() {
            data.swap(cursor, cursor - 1);
        }
        return;
    }

    let midpoint = start_bound.wrapping_add(end_bound) >> 1;
    let combined = midpoint + middle;
    let (mut start, mut right) = if middle > midpoint {
        (combined - end_bound, midpoint)
    } else {
        (start_bound, middle)
    };
    let pivot = combined - 1;

    while start < right {
        let candidate = start.wrapping_add(right) >> 1;
        if compare(&data[pivot - candidate], &data[candidate]) != Ordering::Less {
            start = candidate + 1;
        } else {
            right = candidate;
        }
    }

    let end = combined - start;
    if start < middle && middle < end {
        rotate(data, start, middle, end);
    }
    if start_bound < start && start < midpoint {
        sym_merge(data, start_bound, start, midpoint, compare);
    }
    if midpoint < end && end < end_bound {
        sym_merge(data, midpoint, end, end_bound, compare);
    }
}

fn rotate<T>(data: &mut [T], start: usize, middle: usize, end: usize) {
    let mut left = middle - start;
    let mut right = end - middle;
    while left != right {
        if left > right {
            swap_range(data, middle - left, middle, right);
            left -= right;
        } else {
            swap_range(data, middle - left, middle + right - left, left);
            right -= left;
        }
    }
    swap_range(data, middle - left, middle, left);
}

fn swap_range<T>(data: &mut [T], left: usize, right: usize, length: usize) {
    for offset in 0..length {
        data.swap(left + offset, right + offset);
    }
}
