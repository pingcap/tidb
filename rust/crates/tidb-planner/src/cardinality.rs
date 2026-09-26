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

//! Cardinality-estimation helpers from `pkg/planner/cardinality`.

pub mod cross_estimation;
pub mod derive_stats;
pub mod index_range_policy;
pub mod join;
pub mod live_index_optimizer;
pub mod ndv;
pub mod out_of_range;
pub mod pseudo;
pub mod row_count_column;
pub mod row_count_estimator;
pub mod row_size;
pub mod uniform;

/// Maximum number of columns considered by exponential backoff.
///
/// This is `MaxExponentialBackoffCols` from
/// `pkg/planner/cardinality/exponential.go`.  Once the exponent reaches
/// `1/8`, additional columns have little impact and the Go implementation
/// deliberately caps the calculation at four values.
pub const MAX_EXPONENTIAL_BACKOFF_COLS: usize = 4;

/// Go `AdjustRowCountForAppendedHandleColumns`' row-estimate arithmetic.
///
/// `handle_selectivities` contains the valid per-column estimates for the
/// appended handle dimensions, sorted here in the source's ascending order.
/// `full_point_cap` is present only when every range covers the declared
/// index columns and complete appended handle as a Go point under the
/// default `tidb_regard_null_as_point=ON` ranger setting.
#[must_use]
pub fn adjust_row_count_for_appended_handle_columns(
    prefix_count: row_count_column::RowEstimate,
    handle_selectivities: &[f64],
    full_point_cap: Option<f64>,
) -> row_count_column::RowEstimate {
    let mut selectivities = handle_selectivities.to_vec();
    selectivities.sort_by(f64::total_cmp);

    let mut adjusted = prefix_count;
    if !selectivities.is_empty() {
        // The declared-index prefix occupies the weight-one slot. The first
        // handle selectivity therefore starts at one-half, followed by one-
        // quarter and one-eighth; any further handle columns affect the
        // independence lower estimate but not the damped estimate.
        let mut backoff_inputs = Vec::with_capacity(selectivities.len() + 1);
        backoff_inputs.push(1.0);
        backoff_inputs.extend(selectivities.iter().copied());
        let factor = apply_exponential_backoff(&backoff_inputs, 0.0, 1.0);
        adjusted.est *= factor;
        adjusted.est = go_max(adjusted.est, go_min(prefix_count.est, 1.0));

        let independent_factor = selectivities.iter().product::<f64>();
        adjusted.min_est = go_min(adjusted.min_est * independent_factor, adjusted.est);
    }

    if let Some(point_cap) = full_point_cap {
        adjusted.est = go_min(adjusted.est, point_cap);
        adjusted.min_est = go_min(adjusted.min_est, adjusted.est);
        adjusted.max_est = go_min(adjusted.max_est, point_cap);
    }
    adjusted
}

// Go's math.Max/math.Min differ from Rust's primitive methods for NaN and
// signed zero. Keep the source operation's special cases local to this leaf
// instead of silently inheriting a different clamp contract.
fn go_max(x: f64, y: f64) -> f64 {
    if x == f64::INFINITY || y == f64::INFINITY {
        return f64::INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && x == y {
        return if x.is_sign_negative() { y } else { x };
    }
    if x > y {
        x
    } else {
        y
    }
}

fn go_min(x: f64, y: f64) -> f64 {
    if x == f64::NEG_INFINITY || y == f64::NEG_INFINITY {
        return f64::NEG_INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && x == y {
        return if x.is_sign_negative() { x } else { y };
    }
    if x < y {
        x
    } else {
        y
    }
}

#[cfg(test)]
mod appended_handle_tests {
    use super::adjust_row_count_for_appended_handle_columns;
    use crate::cardinality::row_count_column::RowEstimate;

    fn close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
    }

    #[test]
    fn appended_handle_selectivities_use_damping_and_independence_bounds() {
        let estimate = adjust_row_count_for_appended_handle_columns(
            RowEstimate::new(100.0, 70.0, 140.0),
            &[0.25, 0.01],
            None,
        );

        close(
            estimate.est,
            100.0 * 0.01_f64.sqrt() * 0.25_f64.sqrt().sqrt(),
        );
        close(estimate.min_est, 70.0 * 0.01 * 0.25);
        assert_eq!(estimate.max_est, 140.0);
    }

    #[test]
    fn full_appended_handle_points_cap_the_estimate_to_range_count() {
        let estimate = adjust_row_count_for_appended_handle_columns(
            RowEstimate::new(100.0, 70.0, 140.0),
            &[0.01],
            Some(2.0),
        );

        assert_eq!(estimate.est, 2.0);
        close(estimate.min_est, 0.7);
        assert_eq!(estimate.max_est, 2.0);
    }

    #[test]
    fn damping_does_not_floor_a_sub_one_prefix_estimate_upward() {
        let estimate = adjust_row_count_for_appended_handle_columns(
            RowEstimate::new(0.5, 0.25, 0.75),
            &[0.01],
            None,
        );

        assert_eq!(estimate.est, 0.5);
        close(estimate.min_est, 0.0025);
        assert_eq!(estimate.max_est, 0.75);
    }
}

/// Apply exponential backoff to pre-sorted values, then enforce bounds.
///
/// This is a direct port of Go's `ApplyExponentialBackoff`.  The first value
/// has weight one, the second `1/2`, the third `1/4`, and the fourth `1/8`.
/// Values after the fourth are ignored.  Empty input returns the lower bound;
/// a single value is clamped without taking a root.
pub fn apply_exponential_backoff(sorted_values: &[f64], lower_bound: f64, upper_bound: f64) -> f64 {
    let len = sorted_values.len();
    if len == 0 {
        return lower_bound;
    }

    if len == 1 {
        return go_max(lower_bound, go_min(sorted_values[0], upper_bound));
    }

    let mut result = sorted_values[0];
    let max_cols = MAX_EXPONENTIAL_BACKOFF_COLS.min(len);
    for (index, value) in sorted_values.iter().take(max_cols).enumerate().skip(1) {
        // Go performs `i` successive Sqrt calls rather than computing a
        // fractional power.  Preserve that operation order exactly.
        let mut backed_off = *value;
        for _ in 0..index {
            backed_off = backed_off.sqrt();
        }
        result *= backed_off;
    }

    go_max(lower_bound, go_min(result, upper_bound))
}
