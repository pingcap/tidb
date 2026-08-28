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

//! Range validation and diagnostic helpers from
//! `pkg/store/copr/range_diagnostics.go` and its adjacent classification
//! helpers in `coprocessor.go`.

use std::cmp::Ordering;

use crate::{Key, KeyRange, KeyRanges};

/// Counts every source diagnostic category for one key-range sequence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RangeIssueStats {
    /// Identical adjacent ranges.
    pub duplicate: usize,
    /// Intersecting adjacent ranges where neither contains the other.
    pub overlap: usize,
    /// One adjacent range contains the other.
    pub contain: usize,
    /// A later range sorts before the previous range without overlap.
    pub out_of_order: usize,
    /// A finite end key is below its start key.
    pub invalid_bound: usize,
    /// A range follows an infinite-end range.
    pub infinite_tail: usize,
}

impl RangeIssueStats {
    /// Whether no issue was found.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.duplicate == 0
            && self.overlap == 0
            && self.contain == 0
            && self.out_of_order == 0
            && self.invalid_bound == 0
            && self.infinite_tail == 0
    }
}

fn compare_range_end(left: &Key, right: &Key) -> Ordering {
    match (left.is_empty(), right.is_empty()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.cmp(right),
    }
}

fn range_contains(left: &KeyRange, right: &KeyRange) -> bool {
    left.start_key <= right.start_key
        && compare_range_end(&left.end_key, &right.end_key) != Ordering::Less
}

fn ranges_overlap(left: &KeyRange, right: &KeyRange) -> bool {
    (left.end_key.is_empty() || left.end_key > right.start_key)
        && (right.end_key.is_empty() || right.end_key > left.start_key)
}

fn classify_pair(previous: &KeyRange, current: &KeyRange, stats: &mut RangeIssueStats) {
    if previous == current {
        stats.duplicate += 1;
    } else if range_contains(previous, current) || range_contains(current, previous) {
        stats.contain += 1;
    } else if ranges_overlap(previous, current) {
        stats.overlap += 1;
    } else {
        stats.out_of_order += 1;
    }
}

/// Go `rangeIssuesForKeyRanges`.
#[must_use]
pub fn range_issues_for_key_ranges(ranges: &KeyRanges) -> RangeIssueStats {
    let mut stats = RangeIssueStats::default();
    if ranges.is_empty() {
        return stats;
    }
    let validate = |range: &KeyRange, stats: &mut RangeIssueStats| {
        if !range.end_key.is_empty() && range.start_key > range.end_key {
            stats.invalid_bound += 1;
        }
    };
    let mut previous = ranges.ref_at(0);
    validate(previous, &mut stats);
    for index in 1..ranges.len() {
        let current = ranges.ref_at(index);
        validate(current, &mut stats);
        if previous.end_key.is_empty() {
            stats.infinite_tail += 1;
        } else if previous.end_key > current.start_key {
            classify_pair(previous, current, &mut stats);
        }
        previous = current;
    }
    stats
}

/// Go `minStartAndMaxEndKeyOfKeyRanges`.
#[must_use]
pub fn min_start_and_max_end_key(ranges: &KeyRanges) -> Option<(Key, Key)> {
    let first = ranges.get(0)?;
    let mut min_start = first.start_key.clone();
    let mut max_end = first.end_key.clone();
    for index in 1..ranges.len() {
        let range = ranges.ref_at(index);
        min_start = min_start.min(range.start_key.clone());
        if compare_range_end(&range.end_key, &max_end) == Ordering::Greater {
            max_end = range.end_key.clone();
        }
    }
    Some((min_start, max_end))
}

/// Exact source reasons returned by `firstOutOfBoundKeyRangeInLocation`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutOfBoundReason {
    /// Range start is below the location start.
    StartBeforeLocationStart,
    /// Range start is at or above a finite location end.
    StartAfterOrEqLocationEnd,
    /// Range end is infinite while the location end is finite.
    EndInfiniteButLocationFinite,
    /// Range end is above the finite location end.
    EndAfterLocationEnd,
    /// Range start is greater than its finite end.
    InvalidStartGreaterThanEnd,
}

/// Go `firstOutOfBoundKeyRangeInLocation`.
#[must_use]
pub fn first_out_of_bound_key_range(
    ranges: &KeyRanges,
    location_start: &Key,
    location_end: &Key,
) -> Option<(usize, KeyRange, OutOfBoundReason)> {
    for index in 0..ranges.len() {
        let range = ranges.ref_at(index);
        let reason = if range.start_key < *location_start {
            Some(OutOfBoundReason::StartBeforeLocationStart)
        } else if !location_end.is_empty() && range.start_key >= *location_end {
            Some(OutOfBoundReason::StartAfterOrEqLocationEnd)
        } else if range.end_key.is_empty() && !location_end.is_empty() {
            Some(OutOfBoundReason::EndInfiniteButLocationFinite)
        } else if !location_end.is_empty() && range.end_key > *location_end {
            Some(OutOfBoundReason::EndAfterLocationEnd)
        } else if !range.end_key.is_empty() && range.start_key > range.end_key {
            Some(OutOfBoundReason::InvalidStartGreaterThanEnd)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Some((index, range.clone(), reason));
        }
    }
    None
}

/// Go `ensureMonotonicKeyRanges`: detects invalid/order issues and sorts a
/// copied flat sequence only when necessary. Returns whether it reordered.
pub fn ensure_monotonic_key_ranges(ranges: &mut KeyRanges) -> bool {
    if range_issues_for_key_ranges(ranges).is_empty() {
        return false;
    }
    let mut sorted = ranges.to_ranges();
    sorted.sort_by(|left, right| {
        left.start_key
            .cmp(&right.start_key)
            .then_with(|| left.end_key.cmp(&right.end_key))
    });
    ranges.reset(sorted);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(start: &[u8], end: &[u8]) -> KeyRange {
        KeyRange::new(Key::from(start), Key::from(end))
    }

    #[test]
    fn classifies_and_repairs_the_source_range_issue_categories() {
        let mut ranges = KeyRanges::new(vec![
            range(b"d", b"f"),
            range(b"d", b"f"),
            range(b"e", b"g"),
            range(b"b", b"c"),
            range(b"z", b""),
            range(b"y", b"x"),
        ]);
        let issues = range_issues_for_key_ranges(&ranges);
        assert_eq!(issues.duplicate, 1);
        assert_eq!(issues.overlap, 1);
        assert_eq!(issues.out_of_order, 1);
        assert_eq!(issues.infinite_tail, 1);
        assert_eq!(issues.invalid_bound, 1);
        assert!(ensure_monotonic_key_ranges(&mut ranges));
        assert_eq!(ranges.ref_at(0).start_key.as_bytes(), b"b");
    }

    #[test]
    fn bounds_and_minmax_treat_empty_end_as_infinity() {
        let ranges = KeyRanges::new(vec![range(b"b", b"d"), range(b"e", b"")]);
        let (start, end) = min_start_and_max_end_key(&ranges).unwrap();
        assert_eq!(start.as_bytes(), b"b");
        assert!(end.is_empty());
        assert_eq!(
            first_out_of_bound_key_range(
                &ranges,
                &Key::from(b"a".as_slice()),
                &Key::from(b"z".as_slice())
            )
            .unwrap()
            .2,
            OutOfBoundReason::EndInfiniteButLocationFinite
        );
    }
}
