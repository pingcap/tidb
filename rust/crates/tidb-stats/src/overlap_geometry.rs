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

//! Out-of-range histogram overlap geometry from `pkg/statistics/histogram.go`.
//!
//! These helpers are pure arithmetic for the source's triangular density
//! model. They take already-converted scalar bounds and do not inspect Datum,
//! histograms, planner context, or statistics handles.

/// Calculates left-triangle overlap percentage for `[left, right]`.
#[must_use]
pub fn left_overlap_percent(
    mut left: f64,
    mut right: f64,
    bound_left: f64,
    histogram_left: f64,
    histogram_width: f64,
) -> f64 {
    if histogram_width <= 0.0 {
        return 0.0;
    }
    left = source_max(left, bound_left);
    right = source_min(right, histogram_left);
    if left >= right {
        return 0.0;
    }
    let width_squared = histogram_width.powi(2);
    let right_range = (right - bound_left).powi(2);
    let left_range = (left - bound_left).powi(2);
    (right_range - left_range) / width_squared
}

/// Calculates right-triangle overlap percentage for `[left, right]`.
#[must_use]
pub fn right_overlap_percent(
    mut left: f64,
    mut right: f64,
    histogram_right: f64,
    bound_right: f64,
    histogram_width: f64,
) -> f64 {
    if histogram_width <= 0.0 {
        return 0.0;
    }
    left = source_max(left, histogram_right);
    right = source_min(right, bound_right);
    if left >= right {
        return 0.0;
    }
    let width_squared = histogram_width.powi(2);
    let left_range = (bound_right - left).powi(2);
    let right_range = (bound_right - right).powi(2);
    (left_range - right_range) / width_squared
}

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
