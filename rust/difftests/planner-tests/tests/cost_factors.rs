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

//! Direct source-contract tests for `factors_thresholds.go`.

use tidb_planner::cost_factors::{
    aggregation_factor, aggregation_factor_or_default, DEFAULT_AGGREGATION_FACTOR, DISTINCT_FACTOR,
    SELECTION_FACTOR, SMALL_SCAN_THRESHOLD, TOLERANCE_FACTOR,
};

#[test]
fn source_constants_are_stable() {
    assert_eq!(SELECTION_FACTOR, 0.8);
    assert_eq!(DISTINCT_FACTOR, 0.8);
    assert_eq!(TOLERANCE_FACTOR, 0.00001);
    assert_eq!(SMALL_SCAN_THRESHOLD, 10_000);
    assert_eq!(DEFAULT_AGGREGATION_FACTOR, 1.5);
}

#[test]
fn every_source_aggregate_has_its_factor() {
    for (name, expected) in [
        ("count", 1.0),
        ("sum", 1.0),
        ("sum_int", 1.0),
        ("avg", 2.0),
        ("firstrow", 0.1),
        ("max", 1.0),
        ("min", 1.0),
        ("group_concat", 1.0),
        ("bit_or", 0.9),
        ("bit_xor", 0.9),
        ("bit_and", 0.9),
        ("var_pop", 3.0),
        ("var_samp", 3.0),
        ("stddev_pop", 3.0),
        ("stddev_samp", 3.0),
        ("default", 1.5),
    ] {
        assert_eq!(aggregation_factor(name), Some(expected), "aggregate {name}");
        assert_eq!(
            aggregation_factor_or_default(name),
            expected,
            "aggregate {name}"
        );
    }
}

#[test]
fn unknown_and_uppercase_names_use_source_fallback() {
    assert_eq!(aggregation_factor("approx_count_distinct"), None);
    assert_eq!(aggregation_factor("COUNT"), None);
    assert_eq!(aggregation_factor_or_default("approx_count_distinct"), 1.5);
    assert_eq!(aggregation_factor_or_default("COUNT"), 1.5);
}
