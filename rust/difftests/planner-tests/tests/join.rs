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

//! Source-shaped tests for `pkg/planner/cardinality/join.go`.

use tidb_planner::cardinality::join::{
    estimate_full_join_row_count, FullJoinRowCountInput, JoinKeyEstimate,
};

fn estimate(input: FullJoinRowCountInput) -> f64 {
    estimate_full_join_row_count(&input)
}

fn input(
    rows: (f64, f64),
    is_cartesian: bool,
    keys: [JoinKeyEstimate; 4],
    threshold: i32,
) -> FullJoinRowCountInput {
    FullJoinRowCountInput {
        left_row_count: rows.0,
        right_row_count: rows.1,
        is_cartesian,
        left_join_keys: keys[0],
        right_join_keys: keys[1],
        left_non_equi_keys: keys[2],
        right_non_equi_keys: keys[3],
        join_reorder_threshold: threshold,
    }
}

#[test]
fn cartesian_product_bypasses_ndv() {
    let got = estimate(input(
        (12.0, 4.0),
        true,
        [
            JoinKeyEstimate::new(100.0, 2, 2),
            JoinKeyEstimate::new(10.0, 1, 2),
            JoinKeyEstimate::empty(),
            JoinKeyEstimate::empty(),
        ],
        5,
    ));
    assert_eq!(got, 48.0);
}

#[test]
fn equi_keys_use_larger_ndv_and_threshold_disabled() {
    let got = estimate(input(
        (100.0, 50.0),
        false,
        [
            JoinKeyEstimate::new(10.0, 1, 1),
            JoinKeyEstimate::new(20.0, 1, 1),
            JoinKeyEstimate::new(2.0, 1, 1),
            JoinKeyEstimate::new(2.0, 1, 1),
        ],
        0,
    ));
    assert_eq!(got, 250.0);
}

#[test]
fn either_equi_side_selects_equi_ndvs() {
    let got = estimate(input(
        (30.0, 20.0),
        false,
        [
            JoinKeyEstimate::new(3.0, 1, 1),
            JoinKeyEstimate::empty(),
            JoinKeyEstimate::new(99.0, 1, 1),
            JoinKeyEstimate::new(99.0, 1, 1),
        ],
        0,
    ));
    assert_eq!(got, 200.0);
}

#[test]
fn non_equi_keys_are_used_when_both_equi_sides_are_empty() {
    let got = estimate(input(
        (100.0, 50.0),
        false,
        [
            JoinKeyEstimate::empty(),
            JoinKeyEstimate::empty(),
            JoinKeyEstimate::new(5.0, 1, 1),
            JoinKeyEstimate::new(10.0, 2, 1),
        ],
        0,
    ));
    assert_eq!(got, 500.0);
}

#[test]
fn threshold_applies_correlation_for_remaining_equi_keys() {
    let got = estimate(input(
        (100.0, 50.0),
        false,
        [
            JoinKeyEstimate::new(10.0, 1, 3),
            JoinKeyEstimate::new(20.0, 2, 2),
            JoinKeyEstimate::empty(),
            JoinKeyEstimate::empty(),
        ],
        1,
    ));
    // 100*50/20 * 0.9^(3-max(1,2)) = 225.
    assert!((got - 225.0).abs() < f64::EPSILON);
}

#[test]
fn threshold_preserves_source_negative_exponent_on_non_equi_fallback() {
    let got = estimate(input(
        (100.0, 50.0),
        false,
        [
            JoinKeyEstimate::empty(),
            JoinKeyEstimate::empty(),
            JoinKeyEstimate::new(5.0, 1, 1),
            JoinKeyEstimate::new(10.0, 2, 1),
        ],
        1,
    ));
    // The Go formula uses len(leftJoinKeys), which is zero here, even though
    // the fallback key estimates matched columns.
    assert!((got - (500.0 / 0.9)).abs() < 1e-10);
}

#[test]
fn ndv_nan_is_not_silently_replaced() {
    let got = estimate(input(
        (10.0, 10.0),
        false,
        [
            JoinKeyEstimate::new(f64::NAN, 1, 1),
            JoinKeyEstimate::new(2.0, 1, 1),
            JoinKeyEstimate::empty(),
            JoinKeyEstimate::empty(),
        ],
        0,
    ));
    assert!(got.is_nan());
}
