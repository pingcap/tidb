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
