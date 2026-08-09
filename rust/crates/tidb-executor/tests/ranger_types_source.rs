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

//! Complete top-level tables from `pkg/util/ranger/types_test.go`, translated
//! one for one after the inventory's fuzzy matches were checked against code.

use tidb_datatype::{Collation, Datum};
use tidb_executor::kv_table::{
    index_ranges_estimated_memory_usage, intersect_index_ranges, IndexRange,
};

fn datums(values: &[i64]) -> Vec<Datum> {
    values
        .iter()
        .map(|value| match *value {
            i64::MIN => Datum::MinNotNull,
            i64::MAX => Datum::MaxValue,
            value => Datum::new_int(value),
        })
        .collect()
}

fn range(low: &[i64], high: &[i64], low_exclusive: bool, high_exclusive: bool) -> IndexRange {
    IndexRange {
        low: datums(low),
        high: datums(high),
        low_exclusive,
        high_exclusive,
    }
}

fn optional_range_text(range: Option<&IndexRange>) -> String {
    range.map_or_else(|| "<nil>".to_owned(), ToString::to_string)
}

#[test]
fn test_range() {
    let rows = [
        (range(&[1], &[1], false, false), "[1,1]"),
        (range(&[1], &[1], false, true), "[1,1)"),
        (range(&[1], &[2], true, true), "(1,2)"),
        (
            IndexRange {
                low: vec![Datum::new_real(1.1)],
                high: vec![Datum::new_real(1.9)],
                low_exclusive: false,
                high_exclusive: true,
            },
            "[1.1,1.9)",
        ),
        (range(&[i64::MIN], &[1], false, true), "[-inf,1)"),
    ];
    for (range, expected) in rows {
        assert_eq!(range.to_string(), expected);
    }

    let point_rows = [
        (range(&[1], &[1], false, false), true),
        (
            IndexRange {
                low: vec![Datum::new_string("abc")],
                high: vec![Datum::new_string("abc")],
                low_exclusive: false,
                high_exclusive: false,
            },
            true,
        ),
        (
            IndexRange {
                low: vec![Datum::new_int(1)],
                high: vec![Datum::new_int(1), Datum::new_int(1)],
                low_exclusive: false,
                high_exclusive: false,
            },
            false,
        ),
        (range(&[1], &[1], true, false), false),
        (range(&[1], &[1], false, true), false),
        (range(&[1], &[2], false, false), false),
    ];
    for (range, expected) in point_rows {
        assert_eq!(range.is_point(false), expected, "{range}");
    }
}

#[test]
fn test_is_full_range() {
    let rows = [
        (range(&[i64::MIN], &[i64::MAX], false, false), false, true),
        (range(&[i64::MAX], &[i64::MIN], false, false), false, false),
        (
            IndexRange {
                low: vec![Datum::new_int(1)],
                high: vec![Datum::new_uint(u64::MAX)],
                low_exclusive: false,
                high_exclusive: false,
            },
            false,
            false,
        ),
        (
            IndexRange {
                low: vec![Datum::Null],
                high: vec![Datum::new_uint(u64::MAX)],
                low_exclusive: false,
                high_exclusive: false,
            },
            false,
            true,
        ),
        (
            IndexRange {
                low: vec![Datum::Null],
                high: vec![Datum::Null],
                low_exclusive: false,
                high_exclusive: false,
            },
            false,
            false,
        ),
        (range(&[i64::MIN], &[i64::MAX], false, false), false, true),
        (
            IndexRange {
                low: vec![Datum::new_uint(0)],
                high: vec![Datum::new_uint(u64::MAX)],
                low_exclusive: false,
                high_exclusive: false,
            },
            true,
            true,
        ),
    ];
    for (range, unsigned, expected) in rows {
        assert_eq!(range.is_full_range(unsigned), expected, "{range}");
    }
}

#[test]
fn test_range_mem_usage() {
    let r1 = range(&[0], &[1], false, false);
    let mem1 = std::mem::size_of::<IndexRange>() + 2 * std::mem::size_of::<Datum>();
    assert_eq!(r1.estimated_memory_usage(), mem1);

    let r2 = IndexRange {
        low: vec![Datum::new_string("abcde")],
        high: vec![Datum::new_string("fghij")],
        low_exclusive: false,
        high_exclusive: false,
    };
    let mem2 = mem1 + 10 + 2 * Collation::DEFAULT.name().len();
    assert_eq!(r2.estimated_memory_usage(), mem2);
    assert_eq!(index_ranges_estimated_memory_usage(&[r1, r2]), mem1 + mem2);
}

#[test]
fn test_intersection_list() {
    let left = [
        range(&[100, 0], &[100, i64::MAX], true, false),
        range(&[100], &[i64::MAX], true, false),
    ];
    let right = [
        range(&[i64::MIN], &[101], false, true),
        range(&[101, i64::MIN], &[101, 10], false, true),
    ];
    let actual = intersect_index_ranges(&left, &right)
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(actual, "(100 0,100 +inf],(100,101),[101 -inf,101 10)");
}

#[test]
fn test_intersection_empty() {
    let rows: &[(IndexRange, IndexRange, &str)] = &[
        (
            range(&[1], &[2], false, false),
            range(&[3], &[4], false, false),
            "<nil>",
        ),
        (
            range(&[1], &[2], true, false),
            range(&[3], &[4], true, false),
            "<nil>",
        ),
        (
            range(&[1], &[2], false, true),
            range(&[3], &[4], false, true),
            "<nil>",
        ),
        (
            range(&[1], &[2], true, true),
            range(&[3], &[4], true, true),
            "<nil>",
        ),
        (
            range(&[1, 2], &[1, 3], false, false),
            range(&[1, 3], &[1, 4], true, false),
            "<nil>",
        ),
        (
            range(&[i64::MIN], &[1], false, false),
            range(&[2], &[i64::MAX], false, false),
            "<nil>",
        ),
        (
            range(&[1, 2], &[1, 3], false, false),
            range(&[1, 3], &[1, 4], false, false),
            "[1 3,1 3]",
        ),
        (
            range(&[1, 1, 2], &[1, 1, 5], false, false),
            range(&[1, 2], &[1, 3], true, true),
            "<nil>",
        ),
        (
            range(&[100, 0], &[100, i64::MAX], true, false),
            range(&[i64::MIN, i64::MIN], &[100, i64::MIN], false, false),
            "<nil>",
        ),
        (
            range(&[100, 0], &[100, i64::MAX], true, false),
            range(&[i64::MIN], &[100], false, true),
            "<nil>",
        ),
        (
            range(&[5], &[5], false, false),
            range(&[5], &[i64::MAX], true, false),
            "<nil>",
        ),
        (
            range(&[1], &[1], false, false),
            range(&[5], &[i64::MAX], true, false),
            "<nil>",
        ),
        (
            range(&[5], &[5], false, false),
            range(&[5, 1], &[5, i64::MAX], true, false),
            "(5 1,5 +inf]",
        ),
        (
            range(&[1], &[1], false, false),
            range(&[5, 1], &[5, i64::MAX], true, false),
            "<nil>",
        ),
    ];

    for (left, right, expected) in rows {
        let first = left.intersect(right);
        let second = right.intersect(left);
        assert_eq!(first, second, "{left} {right}");
        assert_eq!(
            optional_range_text(first.as_ref()),
            *expected,
            "{left} {right}"
        );
    }
}

#[test]
fn test_intersection_subset() {
    let rows: &[(IndexRange, IndexRange, &str)] = &[
        (
            range(&[1], &[5], false, false),
            range(&[2], &[4], false, false),
            "[2,4]",
        ),
        (
            range(&[1], &[5], true, false),
            range(&[2], &[4], true, false),
            "(2,4]",
        ),
        (
            range(&[1], &[5], false, true),
            range(&[2], &[4], false, true),
            "[2,4)",
        ),
        (
            range(&[1], &[5], true, true),
            range(&[2], &[4], true, true),
            "(2,4)",
        ),
        (
            range(&[i64::MIN], &[5], false, false),
            range(&[2], &[4], false, false),
            "[2,4]",
        ),
        (
            range(&[1, 2], &[1, 5], false, false),
            range(&[1, 3], &[1, 4], true, false),
            "(1 3,1 4]",
        ),
        (
            range(&[1, 1, i64::MIN], &[1, 1, 15], false, false),
            range(&[1, 1], &[1, 1], false, false),
            "[1 1 -inf,1 1 15]",
        ),
    ];

    for (left, right, expected) in rows {
        let first = left.intersect(right);
        let second = right.intersect(left);
        assert_eq!(first, second, "{left} {right}");
        assert_eq!(
            optional_range_text(first.as_ref()),
            *expected,
            "{left} {right}"
        );
    }
}

#[test]
fn test_intersection_overlap() {
    let rows: &[(IndexRange, IndexRange, &str)] = &[
        (
            range(&[1], &[5], false, false),
            range(&[2], &[7], false, false),
            "[2,5]",
        ),
        (
            range(&[1], &[5], true, false),
            range(&[2], &[7], true, false),
            "(2,5]",
        ),
        (
            range(&[1], &[5], false, true),
            range(&[2], &[7], false, true),
            "[2,5)",
        ),
        (
            range(&[1], &[5], true, true),
            range(&[2], &[7], true, true),
            "(2,5)",
        ),
        (
            range(&[i64::MIN], &[5], false, false),
            range(&[2], &[14], false, false),
            "[2,5]",
        ),
        (
            range(&[1, 2], &[1, 5], false, false),
            range(&[1, 3], &[1, 4], true, false),
            "(1 3,1 4]",
        ),
        (
            range(&[1, 1, i64::MIN], &[1, 1, 15], false, false),
            range(&[1, 1, 4], &[1, 1, 25], false, false),
            "[1 1 4,1 1 15]",
        ),
    ];

    for (left, right, expected) in rows {
        let first = left.intersect(right);
        let second = right.intersect(left);
        assert_eq!(first, second, "{left} {right}");
        assert_eq!(
            optional_range_text(first.as_ref()),
            *expected,
            "{left} {right}"
        );
    }
}
