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

//! Source-backed tests for average rows-per-value arithmetic.

use tidb_stats::avg_count_per_not_null_value;

#[test]
fn source_average_scales_nonnull_count_and_ndv_together() {
    // 90 non-null rows / 10 NDV at histogram time, scaled by 150/100.
    assert_eq!(avg_count_per_not_null_value(150, 100.0, 90.0, 10.0), 9.0);
}

#[test]
fn source_average_uses_one_factor_for_empty_histogram() {
    assert_eq!(avg_count_per_not_null_value(150, 0.0, 0.0, 0.0), 0.0);
    assert_eq!(avg_count_per_not_null_value(150, 0.0, 12.0, 0.0), 12.0);
}

#[test]
fn source_average_clamps_scaled_ndv_to_one() {
    // NDV below one after scaling is clamped before division.
    assert_eq!(avg_count_per_not_null_value(50, 100.0, 20.0, 0.5), 10.0);
}

#[test]
fn source_average_preserves_nan_from_invalid_ndv() {
    assert!(avg_count_per_not_null_value(100, 100.0, 10.0, f64::NAN).is_nan());
}
