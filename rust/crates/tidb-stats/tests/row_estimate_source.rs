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

//! Source-backed tests for RowEstimate arithmetic.

use tidb_stats::{calculate_skew_ratio_counts, default_row_est, RowEstimate};

#[test]
fn source_default_estimate_repeats_value() {
    assert_eq!(
        default_row_est(7.5),
        RowEstimate {
            est: 7.5,
            min_est: 7.5,
            max_est: 7.5,
        }
    );
}

#[test]
fn source_arithmetic_methods_update_all_fields_in_place() {
    let mut estimate = RowEstimate {
        est: 10.0,
        min_est: 4.0,
        max_est: 20.0,
    };
    estimate.add(RowEstimate {
        est: 2.0,
        min_est: 3.0,
        max_est: 5.0,
    });
    assert_eq!(
        estimate,
        RowEstimate {
            est: 12.0,
            min_est: 7.0,
            max_est: 25.0
        }
    );
    estimate.add_all(1.0);
    assert_eq!(
        estimate,
        RowEstimate {
            est: 13.0,
            min_est: 8.0,
            max_est: 26.0
        }
    );
    estimate.subtract(default_row_est(3.0));
    assert_eq!(
        estimate,
        RowEstimate {
            est: 10.0,
            min_est: 5.0,
            max_est: 23.0
        }
    );
    estimate.multiply_all(2.0);
    assert_eq!(
        estimate,
        RowEstimate {
            est: 20.0,
            min_est: 10.0,
            max_est: 46.0
        }
    );
    estimate.divide_all(2.0);
    assert_eq!(
        estimate,
        RowEstimate {
            est: 10.0,
            min_est: 5.0,
            max_est: 23.0
        }
    );
}

#[test]
fn source_clamp_keeps_default_between_min_and_max() {
    let mut estimate = RowEstimate {
        est: 100.0,
        min_est: 200.0,
        max_est: 0.0,
    };
    estimate.clamp(10.0, 90.0);
    assert_eq!(
        estimate,
        RowEstimate {
            est: 90.0,
            min_est: 90.0,
            max_est: 90.0
        }
    );

    let mut bounded = RowEstimate {
        est: 50.0,
        min_est: 20.0,
        max_est: 80.0,
    };
    bounded.clamp(0.0, 100.0);
    assert_eq!(
        bounded,
        RowEstimate {
            est: 50.0,
            min_est: 20.0,
            max_est: 80.0
        }
    );
}

#[test]
fn source_skew_ratio_matches_default_min_max_formula() {
    assert_eq!(
        calculate_skew_ratio_counts(10.0, 30.0, 0.5),
        RowEstimate {
            est: 20.0,
            min_est: 10.0,
            max_est: 30.0,
        }
    );
    // The source does not clamp a negative skew difference before computing
    // maxSkewAmt; retain that exact arithmetic shape.
    assert_eq!(
        calculate_skew_ratio_counts(30.0, 10.0, 0.5),
        RowEstimate {
            est: 30.0,
            min_est: 30.0,
            max_est: 10.0,
        }
    );
}

#[test]
fn source_ordered_helpers_keep_nan_and_signed_zero_boundaries() {
    let mut nan = RowEstimate {
        est: f64::NAN,
        min_est: f64::NAN,
        max_est: f64::NAN,
    };
    nan.clamp(0.0, 1.0);
    assert!(nan.est.is_nan());
    assert!(nan.min_est.is_nan());
    assert!(nan.max_est.is_nan());

    let result = calculate_skew_ratio_counts(-0.0, 0.0, 0.0);
    assert_eq!(result.est, 0.0);
    assert!(result.est.is_sign_positive());
}
