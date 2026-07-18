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

//! Source-backed tests for histogram order correlation.

use tidb_stats::calc_correlation;

#[test]
fn source_single_item_is_perfectly_correlated() {
    assert_eq!(calc_correlation(1, 0.0), 1.0);
}

#[test]
fn source_identity_and_reverse_orders_match_pearson_formula() {
    // For five samples, the identity cross-sum is sum(i*i) = 30.
    assert_eq!(calc_correlation(5, 30.0), 1.0);

    // The reverse order [4, 3, 2, 1, 0] has cross-sum 10.
    assert_eq!(calc_correlation(5, 10.0), -1.0);
}

#[test]
fn source_partial_correlation_preserves_fractional_result() {
    // Six samples with cross-sum 52 produce 87/105, the same value surfaced
    // by the Go handle-level TestCorrelation fixture.
    assert_eq!(calc_correlation(6, 52.0), 0.8285714285714286);
}

#[test]
fn source_zero_sample_keeps_undefined_correlation() {
    assert!(calc_correlation(0, 0.0).is_nan());
}
