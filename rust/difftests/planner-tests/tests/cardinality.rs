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

//! Direct translation of `pkg/planner/cardinality/exponential_test.go`.

use tidb_planner::cardinality::{
    apply_exponential_backoff,
    ndv::{estimate_cols_ndv_with_matched_len, scale_ndv, GroupNdv},
    pseudo::{
        pseudo_avg_count_per_value, pseudo_between_count, pseudo_equal_count, pseudo_less_count,
        pseudo_row_count_by_index_ranges, pseudo_row_count_by_scalar_ranges,
        pseudo_row_count_by_signed_int_ranges, pseudo_row_count_by_unsigned_int_ranges, IndexRange,
        PseudoBoundKind, ScalarRange, SignedIntRange, UnsignedIntRange, PSEUDO_BETWEEN_RATE,
        PSEUDO_EQUAL_RATE, PSEUDO_LESS_RATE,
    },
};

fn assert_close(
    name: &str,
    values: &[f64],
    lower_bound: f64,
    upper_bound: f64,
    expected: f64,
    tolerance: f64,
) {
    let got = apply_exponential_backoff(values, lower_bound, upper_bound);
    assert!(
        (got - expected).abs() <= tolerance,
        "test case: {name}: got {got}, want {expected}"
    );
    assert!(
        got >= lower_bound,
        "result should respect lower bound for {name}"
    );
    assert!(
        got <= upper_bound,
        "result should respect upper bound for {name}"
    );
}

fn apply_exponential_backoff_ndv_cases() {
    assert_close("Single NDV", &[100.0], 10.0, 10_000.0, 100.0, 0.1);

    let expected_two = 1_000.0 * 500.0_f64.sqrt();
    assert_close(
        "Two NDVs",
        &[1_000.0, 500.0],
        100.0,
        100_000.0,
        expected_two,
        0.1,
    );

    let expected_three = 1_000.0 * 500.0_f64.sqrt() * 100.0_f64.sqrt().sqrt();
    assert_close(
        "Three NDVs",
        &[1_000.0, 500.0, 100.0],
        100.0,
        100_000.0,
        expected_three,
        0.1,
    );

    let expected_four =
        1_000.0 * 500.0_f64.sqrt() * 100.0_f64.sqrt().sqrt() * 10.0_f64.sqrt().sqrt().sqrt();
    assert_close(
        "Four NDVs",
        &[1_000.0, 500.0, 100.0, 10.0],
        10.0,
        100_000.0,
        expected_four,
        0.1,
    );
    assert_close(
        "Five NDVs (cap at 4)",
        &[1_000.0, 500.0, 100.0, 10.0, 5.0],
        5.0,
        100_000.0,
        expected_four,
        0.1,
    );
}

fn apply_exponential_backoff_selectivity_cases() {
    assert_close("Single selectivity", &[0.1], 0.001, 1.0, 0.1, 0.001);

    let expected_two = 0.01 * 0.02_f64.sqrt();
    assert_close(
        "Two selectivities",
        &[0.01, 0.02],
        0.001,
        1.0,
        expected_two,
        0.001,
    );

    let expected_three = 0.01 * 0.02_f64.sqrt() * 0.05_f64.sqrt().sqrt();
    assert_close(
        "Three selectivities",
        &[0.01, 0.02, 0.05],
        0.001,
        1.0,
        expected_three,
        0.001,
    );

    let expected_four =
        0.01 * 0.02_f64.sqrt() * 0.05_f64.sqrt().sqrt() * 0.1_f64.sqrt().sqrt().sqrt();
    assert_close(
        "Four selectivities",
        &[0.01, 0.02, 0.05, 0.1],
        0.001,
        1.0,
        expected_four,
        0.001,
    );
}

fn apply_exponential_backoff_bound_cases() {
    assert_close(
        "Below lower bound",
        &[0.001, 0.0005],
        0.01,
        1.0,
        0.01,
        0.001,
    );
    assert_close("Above upper bound", &[100.0, 50.0], 1.0, 10.0, 10.0, 0.1);
    assert_close("Empty input", &[], 5.0, 100.0, 5.0, 0.1);
}

/// Direct translation of `TestApplyExponentialBackoff`, preserving all source
/// vectors and the source's bound checks.
#[test]
fn test_apply_exponential_backoff() {
    apply_exponential_backoff_ndv_cases();
    apply_exponential_backoff_selectivity_cases();
    apply_exponential_backoff_bound_cases();
}

/// Source subtest `TestApplyExponentialBackoff/NDV Cases`.
#[test]
fn test_apply_exponential_backoff_ndv_cases() {
    apply_exponential_backoff_ndv_cases();
}

/// Source subtest `TestApplyExponentialBackoff/Selectivity Cases`.
#[test]
fn test_apply_exponential_backoff_selectivity_cases() {
    apply_exponential_backoff_selectivity_cases();
}

/// Source subtest `TestApplyExponentialBackoff/Bounds Enforcement`.
#[test]
fn test_apply_exponential_backoff_bound_cases() {
    apply_exponential_backoff_bound_cases();
}

#[test]
fn test_apply_exponential_backoff_preserves_go_math_bounds() {
    // The source vectors are finite, but Go's math.Max/math.Min are part of
    // the production contract. Pin their NaN behavior so Rust's primitive
    // f64::max/min cannot silently change it.
    assert!(apply_exponential_backoff(&[f64::NAN], 1.0, 2.0).is_nan());
    assert!(apply_exponential_backoff(&[1.0], f64::NAN, 2.0).is_nan());
    assert!(apply_exponential_backoff(&[1.0], 1.0, f64::NAN).is_nan());
    assert_eq!(
        apply_exponential_backoff(&[1.0], f64::INFINITY, f64::NAN),
        f64::INFINITY
    );
    assert!(apply_exponential_backoff(&[1.0], f64::NEG_INFINITY, f64::NAN).is_nan());
}

#[test]
fn test_apply_exponential_backoff_preserves_go_signed_zero_bounds() {
    // Go's math.Min/math.Max choose a deterministic zero sign.  Keep these
    // vectors at the public API boundary so a Rust primitive min/max shortcut
    // cannot silently change the planner contract.
    let negative_zero = apply_exponential_backoff(&[-0.0], -0.0, 0.0);
    assert_eq!(negative_zero, -0.0);
    assert!(negative_zero.is_sign_negative());

    let positive_zero = apply_exponential_backoff(&[0.0], -0.0, 0.0);
    assert_eq!(positive_zero, 0.0);
    assert!(!positive_zero.is_sign_negative());

    let clamped_negative_zero = apply_exponential_backoff(&[0.0], -1.0, -0.0);
    assert_eq!(clamped_negative_zero, -0.0);
    assert!(clamped_negative_zero.is_sign_negative());

    let lower_bound_positive_zero = apply_exponential_backoff(&[-0.0], 0.0, 1.0);
    assert_eq!(lower_bound_positive_zero, 0.0);
    assert!(!lower_bound_positive_zero.is_sign_negative());

    let empty_negative_zero = apply_exponential_backoff(&[], -0.0, 1.0);
    assert_eq!(empty_negative_zero, -0.0);
    assert!(empty_negative_zero.is_sign_negative());
}

/// Direct translation of `TestScaleNDV` from
/// `pkg/planner/cardinality/ndv_test.go`. The Go test formats results to two
/// decimals; retain that oracle rather than comparing platform-dependent
/// floating-point tails.
#[test]
fn test_scale_ndv() {
    for (original_ndv, original_rows, selected_rows, expected) in [
        (0.0, 0.0, 0.0, 0.0),
        (10.0, 0.0, 100.0, 0.0),
        (10.0, 100.0, 100.0, 10.0),
        (10.0, 100.0, 1.0, 1.0),
        (10.0, 100.0, 2.0, 1.83),
        (10.0, 100.0, 10.0, 6.51),
        (10.0, 100.0, 50.0, 9.99),
        (10.0, 100.0, 80.0, 10.0),
        (10.0, 100.0, 90.0, 10.0),
    ] {
        let got = scale_ndv(original_ndv, original_rows, selected_rows, 0.0);
        assert_eq!(format!("{got:.2}"), format!("{expected:.2}"));
    }
}

/// Direct translation of the dependency-closed arithmetic in
/// `TestEstimateColsNDVWithExponentialBackoff` from
/// `pkg/planner/cardinality/ndv_test.go`.
#[test]
fn test_estimate_cols_ndv_with_exponential_backoff() {
    let column_ndvs = [(1, 1_000.0), (2, 500.0), (3, 10.0)];
    let groups = [GroupNdv {
        columns: vec![1, 2, 3],
        ndv: 5_000.0,
    }];

    let (ndv, matched_len) =
        estimate_cols_ndv_with_matched_len(&[1], &column_ndvs, 100_000.0, &groups, 0.0);
    assert_eq!((ndv, matched_len), (1_000.0, 1));

    let (ndv, matched_len) =
        estimate_cols_ndv_with_matched_len(&[1, 2, 3], &column_ndvs, 100_000.0, &groups, 0.0);
    assert_eq!((ndv, matched_len), (5_000.0, 3));

    let expected_ab_exponential = 1_000.0 * 500.0_f64.sqrt();
    let (conservative, matched_len) =
        estimate_cols_ndv_with_matched_len(&[1, 2], &column_ndvs, 100_000.0, &groups, 0.0);
    assert_eq!((conservative, matched_len), (1_000.0, 1));

    let (exponential, matched_len) =
        estimate_cols_ndv_with_matched_len(&[2, 1], &column_ndvs, 100_000.0, &groups, 1.0);
    assert!((exponential - expected_ab_exponential).abs() < 0.1);
    assert_eq!(matched_len, 1);

    let expected_blended = 1_000.0 + (expected_ab_exponential - 1_000.0) * 0.5;
    let (blended, matched_len) =
        estimate_cols_ndv_with_matched_len(&[1, 2], &column_ndvs, 100_000.0, &groups, 0.5);
    assert!((blended - expected_blended).abs() < 0.1);
    assert_eq!(matched_len, 1);

    for (ids, expected) in [
        (&[1, 3][..], 1_000.0 * 10.0_f64.sqrt()),
        (&[2, 3][..], 500.0 * 10.0_f64.sqrt()),
        (
            &[1, 2, 3][..],
            1_000.0 * 500.0_f64.sqrt() * 10.0_f64.sqrt().sqrt(),
        ),
    ] {
        let group_rows = if ids == [1, 2, 3] {
            &[][..]
        } else {
            &groups[..]
        };
        let (got, matched_len) =
            estimate_cols_ndv_with_matched_len(ids, &column_ndvs, 100_000.0, group_rows, 1.0);
        assert!((got - expected).abs() < 0.1, "ids={ids:?}: got {got}");
        assert_eq!(matched_len, 1);
    }

    let (empty, matched_len) =
        estimate_cols_ndv_with_matched_len(&[], &column_ndvs, 100_000.0, &[], 1.0);
    assert_eq!((empty, matched_len), (1.0, 1));

    let (unknown, matched_len) =
        estimate_cols_ndv_with_matched_len(&[99, 100], &column_ndvs, 100_000.0, &[], 1.0);
    assert_eq!((unknown, matched_len), (1.0, 1));
}

/// Source-shaped arithmetic coverage for the pseudo constants and estimators
/// in `pkg/planner/cardinality/pseudo.go`.
#[test]
fn test_pseudo_rates_and_integer_ranges() {
    let table_rows = 10_000.0;
    assert_eq!(PSEUDO_EQUAL_RATE, 1_000.0);
    assert_eq!(PSEUDO_LESS_RATE, 3.0);
    assert_eq!(PSEUDO_BETWEEN_RATE, 40.0);
    assert_eq!(pseudo_avg_count_per_value(table_rows), 10.0);
    assert_eq!(pseudo_equal_count(table_rows), 10.0);
    assert_eq!(pseudo_less_count(table_rows), table_rows / 3.0);
    assert_eq!(pseudo_between_count(table_rows), 250.0);

    let signed = [
        SignedIntRange::new(0, 0, PseudoBoundKind::Null, PseudoBoundKind::MaxValue),
        SignedIntRange::new(7, 7, PseudoBoundKind::Value, PseudoBoundKind::Value),
        SignedIntRange::new(0, 100, PseudoBoundKind::Value, PseudoBoundKind::Value),
    ];
    assert_eq!(
        pseudo_row_count_by_signed_int_ranges(&signed[..1], table_rows),
        table_rows
    );
    assert_eq!(
        pseudo_row_count_by_signed_int_ranges(&signed[1..2], table_rows),
        1.0
    );
    assert_eq!(
        pseudo_row_count_by_signed_int_ranges(&signed[2..], table_rows),
        100.0
    );

    let unsigned = [
        UnsignedIntRange::new(0, 0, PseudoBoundKind::Null, PseudoBoundKind::MaxValue),
        UnsignedIntRange::new(7, 7, PseudoBoundKind::Value, PseudoBoundKind::Value),
        UnsignedIntRange::new(0, 100, PseudoBoundKind::Value, PseudoBoundKind::Value),
    ];
    assert_eq!(
        pseudo_row_count_by_unsigned_int_ranges(&unsigned[..1], table_rows),
        table_rows
    );
    assert_eq!(
        pseudo_row_count_by_unsigned_int_ranges(&unsigned[1..2], table_rows),
        1.0
    );
    assert_eq!(
        pseudo_row_count_by_unsigned_int_ranges(&unsigned[2..], table_rows),
        100.0
    );
}

/// Source-shaped coverage for generic between/null ranges and the composite
/// index prefix correction.  Session/catalog/statistics owners stay outside.
#[test]
fn test_pseudo_scalar_and_index_ranges() {
    let table_rows = 10_000.0;
    let scalar = [
        ScalarRange::new(0.0, 0.0, PseudoBoundKind::Null, PseudoBoundKind::MaxValue),
        ScalarRange::new(
            0.0,
            0.0,
            PseudoBoundKind::MinNotNull,
            PseudoBoundKind::MaxValue,
        ),
        ScalarRange::new(0.0, 10.0, PseudoBoundKind::Value, PseudoBoundKind::Value),
        ScalarRange::new(3.0, 3.0, PseudoBoundKind::Value, PseudoBoundKind::Value),
    ];
    assert_eq!(
        pseudo_row_count_by_scalar_ranges(&scalar[..1], table_rows),
        table_rows
    );
    assert_eq!(
        pseudo_row_count_by_scalar_ranges(&scalar[1..2], table_rows),
        table_rows - 10.0
    );
    assert_eq!(
        pseudo_row_count_by_scalar_ranges(&scalar[2..3], table_rows),
        250.0
    );
    assert_eq!(
        pseudo_row_count_by_scalar_ranges(&scalar[3..], table_rows),
        10.0
    );

    let unique_equal = IndexRange::new(vec![scalar[3], scalar[3]], 2, false, false);
    assert_eq!(
        pseudo_row_count_by_index_ranges(&[unique_equal], table_rows, Some(2)),
        1.0
    );

    let prefix_range = IndexRange::new(vec![scalar[3], scalar[2]], 1, false, false);
    assert_eq!(
        pseudo_row_count_by_index_ranges(&[prefix_range], table_rows, None),
        2.5
    );
}
