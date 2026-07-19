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

#![allow(missing_docs)]

use tidb_planner::{
    physical_selection::{BigIntComparison, ComparisonOp, ComparisonOperand},
    signed_bigint_ranger::{detach_clustered_signed_bigint_ranges, SignedBigIntRange},
};

fn column_comparison(op: ComparisonOp, input_offset: u32, value: i64) -> BigIntComparison {
    BigIntComparison::new(
        op,
        ComparisonOperand::InputOffset(input_offset),
        ComparisonOperand::Int(value),
    )
    .unwrap()
}

fn literal_comparison(op: ComparisonOp, value: i64, input_offset: u32) -> BigIntComparison {
    BigIntComparison::new(
        op,
        ComparisonOperand::Int(value),
        ComparisonOperand::InputOffset(input_offset),
    )
    .unwrap()
}

fn range(start: i64, end: i64) -> SignedBigIntRange {
    SignedBigIntRange::new(start, end).unwrap()
}

#[test]
fn all_six_comparisons_produce_closed_signed_handle_ranges() {
    let cases = [
        (ComparisonOp::Eq, vec![range(0, 0)]),
        (
            ComparisonOp::Ne,
            vec![range(i64::MIN, -1), range(1, i64::MAX)],
        ),
        (ComparisonOp::Lt, vec![range(i64::MIN, -1)]),
        (ComparisonOp::Le, vec![range(i64::MIN, 0)]),
        (ComparisonOp::Gt, vec![range(1, i64::MAX)]),
        (ComparisonOp::Ge, vec![range(0, i64::MAX)]),
    ];

    for (op, expected) in cases {
        let result = detach_clustered_signed_bigint_ranges(&[column_comparison(op, 0, 0)], 0);
        assert_eq!(result.ranges(), expected);
        assert_eq!(result.access_condition_indices(), [0]);
        assert!(result.residual_conditions().is_empty());
    }
}

#[test]
fn literal_left_comparisons_reverse_the_operator_without_changing_semantics() {
    let cases = [
        (ComparisonOp::Eq, vec![range(7, 7)]),
        (
            ComparisonOp::Ne,
            vec![range(i64::MIN, 6), range(8, i64::MAX)],
        ),
        (ComparisonOp::Lt, vec![range(8, i64::MAX)]),
        (ComparisonOp::Le, vec![range(7, i64::MAX)]),
        (ComparisonOp::Gt, vec![range(i64::MIN, 6)]),
        (ComparisonOp::Ge, vec![range(i64::MIN, 7)]),
    ];

    for (op, expected) in cases {
        let result = detach_clustered_signed_bigint_ranges(&[literal_comparison(op, 7, 0)], 0);
        assert_eq!(result.ranges(), expected);
    }
}

#[test]
fn min_and_max_boundaries_never_overflow() {
    let cases = [
        (column_comparison(ComparisonOp::Lt, 0, i64::MIN), vec![]),
        (
            column_comparison(ComparisonOp::Le, 0, i64::MIN),
            vec![range(i64::MIN, i64::MIN)],
        ),
        (column_comparison(ComparisonOp::Gt, 0, i64::MAX), vec![]),
        (
            column_comparison(ComparisonOp::Ge, 0, i64::MAX),
            vec![range(i64::MAX, i64::MAX)],
        ),
        (
            column_comparison(ComparisonOp::Ne, 0, i64::MIN),
            vec![range(i64::MIN + 1, i64::MAX)],
        ),
        (
            column_comparison(ComparisonOp::Ne, 0, i64::MAX),
            vec![range(i64::MIN, i64::MAX - 1)],
        ),
    ];

    for (condition, expected) in cases {
        assert_eq!(
            detach_clustered_signed_bigint_ranges(&[condition], 0).ranges(),
            expected
        );
    }
}

#[test]
fn ordered_and_intersection_keeps_normalized_non_overlapping_ranges() {
    let conditions = [
        column_comparison(ComparisonOp::Ge, 0, -7),
        column_comparison(ComparisonOp::Le, 0, 77),
        column_comparison(ComparisonOp::Ne, 0, 0),
    ];
    let result = detach_clustered_signed_bigint_ranges(&conditions, 0);

    assert_eq!(result.ranges(), [range(-7, -1), range(1, 77)]);
    assert_eq!(result.access_condition_indices(), [0, 1, 2]);
}

#[test]
fn contradictory_access_conditions_produce_no_ranges() {
    let conditions = [
        column_comparison(ComparisonOp::Gt, 0, 5),
        column_comparison(ComparisonOp::Lt, 0, 5),
    ];
    let result = detach_clustered_signed_bigint_ranges(&conditions, 0);

    assert!(result.ranges().is_empty());
    assert_eq!(result.access_condition_indices(), [0, 1]);
    assert!(result.residual_conditions().is_empty());
}

#[test]
fn only_clustered_handle_conditions_are_detached_and_order_is_preserved() {
    let stored_first = column_comparison(ComparisonOp::Gt, 1, 100);
    let pk_first = column_comparison(ComparisonOp::Ge, 0, -7);
    let stored_second = column_comparison(ComparisonOp::Ne, 2, 0);
    let pk_second = column_comparison(ComparisonOp::Le, 0, 77);
    let conditions = [stored_first, pk_first, stored_second, pk_second];

    let result = detach_clustered_signed_bigint_ranges(&conditions, 0);

    assert_eq!(result.ranges(), [range(-7, 77)]);
    assert_eq!(result.access_condition_indices(), [1, 3]);
    assert_eq!(result.residual_conditions(), [stored_first, stored_second]);
}

#[test]
fn no_clustered_access_condition_keeps_the_full_range_and_all_residuals() {
    let conditions = [
        column_comparison(ComparisonOp::Gt, 1, 10),
        column_comparison(ComparisonOp::Lt, 2, 20),
    ];
    let result = detach_clustered_signed_bigint_ranges(&conditions, 0);

    assert_eq!(result.ranges(), [SignedBigIntRange::full()]);
    assert!(result.access_condition_indices().is_empty());
    assert_eq!(result.residual_conditions(), conditions);
}

#[test]
fn redundant_intersections_do_not_duplicate_or_overlap_ranges() {
    let conditions = [
        column_comparison(ComparisonOp::Ne, 0, 0),
        column_comparison(ComparisonOp::Ne, 0, 0),
        column_comparison(ComparisonOp::Ge, 0, i64::MIN),
        column_comparison(ComparisonOp::Le, 0, i64::MAX),
    ];
    let result = detach_clustered_signed_bigint_ranges(&conditions, 0);

    assert_eq!(result.ranges(), [range(i64::MIN, -1), range(1, i64::MAX)]);
    assert!(result
        .ranges()
        .windows(2)
        .all(|pair| pair[0].end() < pair[1].start()));
}
