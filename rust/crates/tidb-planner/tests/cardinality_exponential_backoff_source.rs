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

//! GO PORT of `pkg/planner/cardinality/exponential_test.go::TestApplyExponentialBackoff`
//! (item 1 of the pkg/planner.part1 slice).
//!
//! Pins the backoff formula exercised through production code
//! `pkg/planner/cardinality/exponential.go:28 ApplyExponentialBackoff`
//! (transcreated as [`tidb_planner::cardinality::apply_exponential_backoff`]):
//! `values[0] * values[1]^(1/2) * values[2]^(1/4) * values[3]^(1/8)`, capped at
//! `MaxExponentialBackoffCols = 4` inputs and clamped into
//! `[lower_bound, upper_bound]`; empty input returns the lower bound.

use tidb_planner::cardinality::apply_exponential_backoff;

/// Go `exponential_test.go:26 testExponentialBackoffHelper`.
fn check(name: &'static str, values: &[f64], lower: f64, upper: f64, expected: f64, tolerance: f64) {
    let result = apply_exponential_backoff(values, lower, upper);
    assert!(
        (result - expected).abs() <= tolerance,
        "test case {name}: expected {expected}, got {result}"
    );
    assert!(
        result >= lower,
        "test case {name}: result {result} must respect lower bound {lower}"
    );
    assert!(
        result <= upper,
        "test case {name}: result {result} must respect upper bound {upper}"
    );
}

#[test]
fn apply_exponential_backoff_ndv_selectivity_and_bounds_cases() {
    // Go subtest "NDV Cases" (exponential_test.go:38-61).
    // Single value.
    check("Single NDV", &[100.0], 10.0, 10000.0, 100.0, 0.1);

    // Two values: 1000 * sqrt(500).
    let expected_two = 1000.0 * 500_f64.sqrt();
    check(
        "Two NDVs",
        &[1000.0, 500.0],
        100.0,
        100000.0,
        expected_two,
        0.1,
    );

    // Three values: previous * sqrt(sqrt(100)).
    let expected_three = expected_two * 100_f64.sqrt().sqrt();
    check(
        "Three NDVs",
        &[1000.0, 500.0, 100.0],
        100.0,
        100000.0,
        expected_three,
        0.1,
    );

    // Four values (max limit): previous * sqrt(sqrt(sqrt(10))).
    let expected_four = expected_three * 10_f64.sqrt().sqrt().sqrt();
    check("Four NDVs", &[1000.0, 500.0, 100.0, 10.0], 10.0, 100000.0, expected_four, 0.1);

    // Five values are capped at MaxExponentialBackoffCols=4, so the fifth is
    // ignored and the expectation equals the four-value case.
    check(
        "Five NDVs (cap at 4)",
        &[1000.0, 500.0, 100.0, 10.0, 5.0],
        5.0,
        100000.0,
        expected_four,
        0.1,
    );

    // Go subtest "Selectivity Cases" (exponential_test.go:64-84).
    check("Single selectivity", &[0.1], 0.001, 1.0, 0.1, 0.001);
    let sel_two = 0.01 * 0.02_f64.sqrt();
    check("Two selectivities", &[0.01, 0.02], 0.001, 1.0, sel_two, 0.001);
    let sel_three = sel_two * 0.05_f64.sqrt().sqrt();
    check(
        "Three selectivities",
        &[0.01, 0.02, 0.05],
        0.001,
        1.0,
        sel_three,
        0.001,
    );
    let sel_four = sel_three * 0.1_f64.sqrt().sqrt().sqrt();
    check(
        "Four selectivities",
        &[0.01, 0.02, 0.05, 0.1],
        0.001,
        1.0,
        sel_four,
        0.001,
    );

    // Go subtest "Bounds Enforcement" (exponential_test.go:87-99).
    // A product below the lower bound is clamped up to it.
    check("Below lower bound", &[0.001, 0.000_5], 0.01, 1.0, 0.01, 0.001);
    // A product above the upper bound is clamped down to it.
    check("Above upper bound", &[100.0, 50.0], 1.0, 10.0, 10.0, 0.1);
    // Empty input returns the lower bound.
    check("Empty input", &[], 5.0, 100.0, 5.0, 0.1);
}
