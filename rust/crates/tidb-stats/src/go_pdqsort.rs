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

//! Comparator schedule used by Go 1.25.10 `slices.SortFunc`.
//!
//! An equivalent total ordering is insufficient for TiDB TopN metadata:
//! duplicate encoded values can carry different counts, and lower-bound lookup
//! returns the first equal entry. This is a direct native transcription of the
//! Go 1.25.10 generated pdqsort implementation, including its insertion-sort,
//! heap fallback, pivot, pattern-breaking, and equal-partition schedules.
//!
//! Source authority:
//! `src/slices/zsortanyfunc.go` SHA-256
//! `50398783e085ac9cc1d7c6249ebe75a5cc4425dcdc6a4cc448ea6e7179126492` and
//! `src/slices/sort.go` SHA-256
//! `e6cd273b637e7dd2f547596c22e6236b92ae9b376f31ceb7b3c3269dd60a38df`.

use std::cmp::Ordering;

pub(crate) fn go_sort_func_by<T>(data: &mut [T], mut compare: impl FnMut(&T, &T) -> Ordering) {
    let limit = usize::BITS as usize - data.len().leading_zeros() as usize;
    pdqsort(data, 0, data.len(), limit, &mut compare);
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

fn sift_down<T>(
    data: &mut [T],
    low: usize,
    high: usize,
    first: usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) {
    let mut root = low;
    loop {
        let mut child = 2 * root + 1;
        if child >= high {
            break;
        }
        if child + 1 < high
            && compare(&data[first + child], &data[first + child + 1]) == Ordering::Less
        {
            child += 1;
        }
        if compare(&data[first + root], &data[first + child]) != Ordering::Less {
            return;
        }
        data.swap(first + root, first + child);
        root = child;
    }
}

fn heap_sort<T>(
    data: &mut [T],
    start: usize,
    end: usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) {
    let high = end - start;
    for root in (0..=(high - 1) / 2).rev() {
        sift_down(data, root, high, start, compare);
    }
    for index in (0..high).rev() {
        data.swap(start, start + index);
        sift_down(data, 0, index, start, compare);
    }
}

fn pdqsort<T>(
    data: &mut [T],
    mut start: usize,
    mut end: usize,
    mut limit: usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) {
    const MAX_INSERTION: usize = 12;

    let mut was_balanced = true;
    let mut was_partitioned = true;
    loop {
        let length = end - start;
        if length <= MAX_INSERTION {
            insertion_sort(data, start, end, compare);
            return;
        }
        if limit == 0 {
            heap_sort(data, start, end, compare);
            return;
        }
        if !was_balanced {
            break_patterns(data, start, end);
            limit -= 1;
        }

        let (mut pivot, mut hint) = choose_pivot(data, start, end, compare);
        if hint == SortedHint::Decreasing {
            reverse_range(data, start, end);
            pivot = (end - 1) - (pivot - start);
            hint = SortedHint::Increasing;
        }

        if was_balanced
            && was_partitioned
            && hint == SortedHint::Increasing
            && partial_insertion_sort(data, start, end, compare)
        {
            return;
        }

        if start > 0 && compare(&data[start - 1], &data[pivot]) != Ordering::Less {
            start = partition_equal(data, start, end, pivot, compare);
            continue;
        }

        let (middle, already_partitioned) = partition(data, start, end, pivot, compare);
        was_partitioned = already_partitioned;
        let left_length = middle - start;
        let right_length = end - middle;
        let balance_threshold = length / 8;
        if left_length < right_length {
            was_balanced = left_length >= balance_threshold;
            pdqsort(data, start, middle, limit, compare);
            start = middle + 1;
        } else {
            was_balanced = right_length >= balance_threshold;
            pdqsort(data, middle + 1, end, limit, compare);
            end = middle;
        }
    }
}

fn partition<T>(
    data: &mut [T],
    start: usize,
    end: usize,
    pivot: usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) -> (usize, bool) {
    data.swap(start, pivot);
    let mut left = start + 1;
    let mut right = end - 1;

    while left <= right && compare(&data[left], &data[start]) == Ordering::Less {
        left += 1;
    }
    while left <= right && compare(&data[right], &data[start]) != Ordering::Less {
        right -= 1;
    }
    if left > right {
        data.swap(right, start);
        return (right, true);
    }
    data.swap(left, right);
    left += 1;
    right -= 1;

    loop {
        while left <= right && compare(&data[left], &data[start]) == Ordering::Less {
            left += 1;
        }
        while left <= right && compare(&data[right], &data[start]) != Ordering::Less {
            right -= 1;
        }
        if left > right {
            break;
        }
        data.swap(left, right);
        left += 1;
        right -= 1;
    }
    data.swap(right, start);
    (right, false)
}

fn partition_equal<T>(
    data: &mut [T],
    start: usize,
    end: usize,
    pivot: usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) -> usize {
    data.swap(start, pivot);
    let mut left = start + 1;
    let mut right = end - 1;
    loop {
        while left <= right && compare(&data[start], &data[left]) != Ordering::Less {
            left += 1;
        }
        while left <= right && compare(&data[start], &data[right]) == Ordering::Less {
            right -= 1;
        }
        if left > right {
            break;
        }
        data.swap(left, right);
        left += 1;
        right -= 1;
    }
    left
}

fn partial_insertion_sort<T>(
    data: &mut [T],
    start: usize,
    end: usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) -> bool {
    const MAX_STEPS: usize = 5;
    const SHORTEST_SHIFTING: usize = 50;

    let mut index = start + 1;
    for _ in 0..MAX_STEPS {
        while index < end && compare(&data[index], &data[index - 1]) != Ordering::Less {
            index += 1;
        }
        if index == end {
            return true;
        }
        if end - start < SHORTEST_SHIFTING {
            return false;
        }

        data.swap(index, index - 1);
        if index - start >= 2 {
            let mut cursor = index - 1;
            while cursor >= 1 {
                if compare(&data[cursor], &data[cursor - 1]) != Ordering::Less {
                    break;
                }
                data.swap(cursor, cursor - 1);
                cursor -= 1;
            }
        }
        if end - index >= 2 {
            let mut cursor = index + 1;
            while cursor < end {
                if compare(&data[cursor], &data[cursor - 1]) != Ordering::Less {
                    break;
                }
                data.swap(cursor, cursor - 1);
                cursor += 1;
            }
        }
    }
    false
}

fn break_patterns<T>(data: &mut [T], start: usize, end: usize) {
    let length = end - start;
    if length >= 8 {
        let mut random = XorShift(length as u64);
        let modulus = next_power_of_two(length);
        let middle = start + (length / 4) * 2;
        for index in middle - 1..=middle + 1 {
            let mut other = random.next() as usize & (modulus - 1);
            if other >= length {
                other -= length;
            }
            data.swap(index, start + other);
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SortedHint {
    Unknown,
    Increasing,
    Decreasing,
}

fn choose_pivot<T>(
    data: &mut [T],
    start: usize,
    end: usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) -> (usize, SortedHint) {
    const SHORTEST_NINTHER: usize = 50;
    const MAX_SWAPS: usize = 4 * 3;

    let length = end - start;
    let mut swaps = 0;
    let mut left = start + length / 4;
    let mut middle = start + length / 4 * 2;
    let mut right = start + length / 4 * 3;
    if length >= 8 {
        if length >= SHORTEST_NINTHER {
            left = median_adjacent(data, left, &mut swaps, compare);
            middle = median_adjacent(data, middle, &mut swaps, compare);
            right = median_adjacent(data, right, &mut swaps, compare);
        }
        middle = median(data, left, middle, right, &mut swaps, compare);
    }
    let hint = match swaps {
        0 => SortedHint::Increasing,
        MAX_SWAPS => SortedHint::Decreasing,
        _ => SortedHint::Unknown,
    };
    (middle, hint)
}

fn order_two<T>(
    data: &[T],
    left: usize,
    right: usize,
    swaps: &mut usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) -> (usize, usize) {
    if compare(&data[right], &data[left]) == Ordering::Less {
        *swaps += 1;
        (right, left)
    } else {
        (left, right)
    }
}

fn median<T>(
    data: &[T],
    mut left: usize,
    mut middle: usize,
    right: usize,
    swaps: &mut usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) -> usize {
    (left, middle) = order_two(data, left, middle, swaps, compare);
    middle = order_two(data, middle, right, swaps, compare).0;
    middle = order_two(data, left, middle, swaps, compare).1;
    middle
}

fn median_adjacent<T>(
    data: &[T],
    index: usize,
    swaps: &mut usize,
    compare: &mut impl FnMut(&T, &T) -> Ordering,
) -> usize {
    median(data, index - 1, index, index + 1, swaps, compare)
}

fn reverse_range<T>(data: &mut [T], start: usize, end: usize) {
    let mut left = start;
    let mut right = end - 1;
    while left < right {
        data.swap(left, right);
        left += 1;
        right -= 1;
    }
}

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn next_power_of_two(length: usize) -> usize {
    1 << (usize::BITS as usize - length.leading_zeros() as usize)
}
