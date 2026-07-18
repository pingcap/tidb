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

//! Row-count estimate arithmetic from `pkg/statistics/histogram.go`.
//!
//! The value object is independent of Datum encoding, histograms, planner
//! contexts, and statistics handles.  Callers own the estimates' source and
//! decide which SQL/cardinality operation each arithmetic method represents.

/// The minimum, default, and maximum estimate for a row-count operation.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RowEstimate {
    /// Default estimate used for normal planning.
    pub est: f64,
    /// Lower estimate used for risk bounds.
    pub min_est: f64,
    /// Upper estimate used for risk bounds.
    pub max_est: f64,
}

/// Creates an estimate with the same value in all three fields.
#[must_use]
pub const fn default_row_est(est: f64) -> RowEstimate {
    RowEstimate {
        est,
        min_est: est,
        max_est: est,
    }
}

/// Calculates the default/min/max skew estimates used by TiDB.
#[must_use]
pub fn calculate_skew_ratio_counts(
    estimate: f64,
    skew_estimate: f64,
    skew_ratio: f64,
) -> RowEstimate {
    let skew_diff = skew_estimate - estimate;
    let skew_amount = source_max(0.0, skew_diff * skew_ratio);
    let max_skew_amount = source_min(skew_diff, 2.0 * skew_amount);
    RowEstimate {
        est: estimate + skew_amount,
        min_est: estimate,
        max_est: estimate + max_skew_amount,
    }
}

impl RowEstimate {
    /// Adds another estimate field by field.
    pub fn add(&mut self, other: Self) {
        self.est += other.est;
        self.min_est += other.min_est;
        self.max_est += other.max_est;
    }

    /// Adds one scalar to every estimate field.
    pub fn add_all(&mut self, value: f64) {
        self.est += value;
        self.min_est += value;
        self.max_est += value;
    }

    /// Subtracts another estimate field by field.
    pub fn subtract(&mut self, other: Self) {
        self.est -= other.est;
        self.min_est -= other.min_est;
        self.max_est -= other.max_est;
    }

    /// Multiplies every estimate field by one scalar.
    pub fn multiply_all(&mut self, value: f64) {
        self.est *= value;
        self.min_est *= value;
        self.max_est *= value;
    }

    /// Divides every estimate field by one scalar.
    pub fn divide_all(&mut self, value: f64) {
        self.est /= value;
        self.min_est /= value;
        self.max_est /= value;
    }

    /// Clamps all fields and preserves the source min/default/max ordering.
    pub fn clamp(&mut self, lower: f64, upper: f64) {
        self.est = source_clamp(self.est, lower, upper);
        self.min_est = source_min(self.min_est, self.est);
        self.min_est = source_clamp(self.min_est, lower, upper);
        self.max_est = source_max(self.max_est, self.est);
        self.max_est = source_clamp(self.max_est, lower, upper);
    }
}

// Go's mathutil.Clamp uses ordered comparisons, so NaN passes through rather
// than being replaced by an endpoint.  The explicit form keeps that behavior
// instead of adopting Rust's f64::clamp NaN policy.
fn source_clamp(value: f64, lower: f64, upper: f64) -> f64 {
    if value >= upper {
        upper
    } else if value <= lower {
        lower
    } else {
        value
    }
}

// Go's built-in min/max return NaN when either operand is NaN.
fn source_min(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left == 0.0 && right == 0.0 {
        if left.is_sign_negative() || right.is_sign_negative() {
            -0.0
        } else {
            0.0
        }
    } else if left < right {
        left
    } else {
        right
    }
}

fn source_max(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left == 0.0 && right == 0.0 {
        if left.is_sign_positive() || right.is_sign_positive() {
            0.0
        } else {
            -0.0
        }
    } else if left > right {
        left
    } else {
        right
    }
}
