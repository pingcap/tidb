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

//! Direct source-contract tests for `convertRangeFromExpectedCnt`.

use tidb_datatype::{Collation, Datum};
use tidb_planner::cardinality::cross_estimation::{convert_range_from_expected_cnt, CountedRange};
use tidb_planner::ranger::types::Range;

fn counted(low: i64, high: i64, low_exclude: bool, high_exclude: bool, rows: f64) -> CountedRange {
    CountedRange::new(
        Range {
            low_val: vec![Datum::Int(low)],
            high_val: vec![Datum::Int(high)],
            low_exclude,
            high_exclude,
            collators: vec![Collation::Binary],
        },
        rows,
    )
}

/// Source anchor: `TestOrderingIdxSelectivityThreshold` in
/// `pkg/planner/cardinality/selectivity_test.go:1869` exercises this helper
/// through ordered index-scan limit estimation. These vectors pin the helper's
/// range/arithmetic contract using the live Datum range owner.
#[test]
fn ascending_conversion_selects_the_first_range_reaching_expected_count() {
    let ranges = [
        counted(10, 20, false, false, 2.0),
        counted(30, 40, true, true, 3.0),
        counted(50, 60, false, false, 10.0),
    ];
    let converted = convert_range_from_expected_cnt(&ranges, 5.0, false);

    assert!(!converted.is_full_scan());
    assert_eq!(converted.skipped_rows(), 2.0);
    let range = converted.converted_range().expect("converted range");
    assert_eq!(range.low_val, [Datum::Null]);
    assert_eq!(range.high_val, [Datum::Int(30)]);
    assert!(!range.low_exclude);
    assert!(!range.high_exclude);
    assert_eq!(range.collators, [Collation::Binary]);
}

#[test]
fn descending_conversion_selects_from_the_high_end_and_inverts_exclusion() {
    let ranges = [
        counted(10, 20, false, false, 2.0),
        counted(30, 40, false, true, 3.0),
        counted(50, 60, false, false, 10.0),
    ];
    let converted = convert_range_from_expected_cnt(&ranges, 11.0, true);

    assert!(!converted.is_full_scan());
    assert_eq!(converted.skipped_rows(), 10.0);
    let range = converted.converted_range().expect("converted range");
    assert_eq!(range.low_val, [Datum::Int(40)]);
    assert_eq!(range.high_val, [Datum::MaxValue]);
    assert!(!range.low_exclude);
    assert!(!range.high_exclude);
    assert_eq!(range.collators, [Collation::Binary]);
}

#[test]
fn expected_count_beyond_all_ranges_requests_the_full_scan() {
    let ranges = [
        counted(10, 20, false, false, 2.0),
        counted(30, 40, false, false, 3.0),
    ];
    for descending in [false, true] {
        let converted = convert_range_from_expected_cnt(&ranges, 6.0, descending);
        assert!(converted.is_full_scan());
        assert!(converted.converted_range().is_none());
        assert_eq!(converted.skipped_rows(), 0.0);
    }
}

#[test]
fn source_boundary_and_empty_inputs_follow_the_loop_contract() {
    let ranges = [counted(10, 20, true, false, 2.0)];
    let exact = convert_range_from_expected_cnt(&ranges, 2.0, false);
    assert_eq!(exact.skipped_rows(), 0.0);
    let range = exact.converted_range().expect("converted range");
    assert_eq!(range.low_val, [Datum::Null]);
    assert_eq!(range.high_val, [Datum::Int(10)]);
    assert!(!range.low_exclude);
    assert!(!range.high_exclude);
    assert_eq!(range.collators, [Collation::Binary]);

    let empty = convert_range_from_expected_cnt(&[], 1.0, false);
    assert!(empty.is_full_scan());
    assert!(empty.converted_range().is_none());
    assert_eq!(empty.skipped_rows(), 0.0);
}

#[test]
fn floating_point_comparisons_keep_source_nan_and_negative_contracts() {
    let ranges = [
        counted(10, 20, false, false, 2.0),
        counted(30, 40, false, false, 3.0),
    ];

    let negative = convert_range_from_expected_cnt(&ranges, -1.0, false);
    assert!(!negative.is_full_scan());
    assert_eq!(negative.skipped_rows(), 0.0);
    assert_eq!(
        negative
            .converted_range()
            .expect("negative count prefix")
            .high_val,
        [Datum::Int(10)]
    );

    let nan = convert_range_from_expected_cnt(&ranges, f64::NAN, false);
    assert!(nan.is_full_scan());
    assert!(nan.converted_range().is_none());
    assert_eq!(nan.skipped_rows(), 0.0);
}
