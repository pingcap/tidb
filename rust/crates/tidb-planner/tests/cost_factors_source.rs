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

//! Source-backed contract for `pkg/planner/core/cost`.

use tidb_planner::cardinality::derive_stats::DeriveStatsContext;
use tidb_planner::cost_factors::{
    DEFAULT_AGGREGATION_FACTOR, DISTINCT_FACTOR, SELECTION_FACTOR, SMALL_SCAN_THRESHOLD,
    TOLERANCE_FACTOR, aggregation_factor, aggregation_factor_or_default,
};

#[test]
fn planner_cost_constants_match_the_complete_go_package() {
    assert_eq!(SELECTION_FACTOR, 0.8);
    assert_eq!(DISTINCT_FACTOR, 0.8);
    assert_eq!(TOLERANCE_FACTOR, 0.00001);
    assert_eq!(SMALL_SCAN_THRESHOLD, 10_000.0);
    assert_eq!(DEFAULT_AGGREGATION_FACTOR, 1.5);

    // The live logical-stats consumer must use the package-owned constant,
    // not a second literal that can drift independently.
    assert_eq!(
        DeriveStatsContext::default().selection_factor,
        SELECTION_FACTOR
    );
}

#[test]
fn aggregation_factor_lookup_matches_every_source_entry() {
    let expected = [
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
    ];

    for (name, factor) in expected {
        assert_eq!(aggregation_factor(name), Some(factor), "{name}");
    }
    assert_eq!(aggregation_factor("COUNT"), None);
    assert_eq!(aggregation_factor("unknown"), None);
    assert_eq!(
        aggregation_factor_or_default("unknown"),
        DEFAULT_AGGREGATION_FACTOR
    );
}
