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

//! Selectivity-scaled NDV estimation from `pkg/planner/cardinality/ndv.go`.
//!
//! The Go entrypoint receives session variables, but the arithmetic itself is
//! dependency-closed.  The caller supplies the source skew-ratio value here;
//! the future planner can adapt its real `SessionVars` owner without adding a
//! second formula or guessing context from runtime values.

use super::apply_exponential_backoff;

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

/// A dependency-closed group NDV row copied from `property.GroupNDV`.
///
/// The full Go planner stores this beside histograms and session statistics.
/// The Rust seed keeps only the source fields needed by the estimator so the
/// arithmetic can be exercised before those owners are migrated.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupNdv {
    /// Sorted column unique IDs participating in the group.
    pub columns: Vec<i64>,
    /// Observed number of distinct values for the group.
    pub ndv: f64,
}

/// Estimates the NDV for a set of column IDs using the source's conservative
/// and exponential-backoff paths.
///
/// `column_ndvs` is the source `StatsInfo.ColNDVs` map represented as pairs;
/// `row_count` is `StatsInfo.RowCount`; and `skew_ratio` is the caller-owned
/// `RiskGroupNDVSkewRatio`. IDs and group columns are sorted before matching,
/// exactly as `GetGroupNDV4Cols` sorts expression columns in Go.
#[must_use]
pub fn estimate_cols_ndv_with_matched_len(
    column_ids: &[i64],
    column_ndvs: &[(i64, f64)],
    row_count: f64,
    group_ndvs: &[GroupNdv],
    skew_ratio: f64,
) -> (f64, usize) {
    if column_ids.is_empty() {
        return (1.0, 1);
    }

    let mut ids = column_ids.to_vec();
    ids.sort_unstable();

    if let Some(group) = group_ndvs.iter().find(|group| {
        let mut group_columns = group.columns.clone();
        group_columns.sort_unstable();
        group_columns == ids
    }) {
        return (go_max(group.ndv, 1.0), group.columns.len());
    }

    let conservative_ndv = ids
        .iter()
        .filter_map(|id| {
            column_ndvs
                .iter()
                .find(|(column_id, _)| column_id == id)
                .map(|(_, ndv)| *ndv)
        })
        .filter(|ndv| *ndv > 0.0)
        .fold(1.0, go_max);

    if ids.len() == 1 {
        return (conservative_ndv, 1);
    }

    let mut values: Vec<f64> = ids
        .iter()
        .filter_map(|id| {
            column_ndvs
                .iter()
                .find(|(column_id, _)| column_id == id)
                .map(|(_, ndv)| *ndv)
        })
        .filter(|ndv| *ndv > 0.0)
        .collect();
    values.sort_by(|left, right| right.total_cmp(left));

    let exponential_ndv = if values.is_empty() {
        1.0
    } else {
        let lower_bound = go_max(values[0], 1.0);
        if row_count <= lower_bound {
            lower_bound
        } else {
            apply_exponential_backoff(&values, lower_bound, row_count)
        }
    };

    let estimate = if skew_ratio > 0.0 {
        conservative_ndv + (exponential_ndv - conservative_ndv) * skew_ratio
    } else {
        conservative_ndv
    };
    (estimate, 1)
}

/// Scales an original NDV by selected rows using Go's uniform/skewed blend.
///
/// This is the arithmetic body of `ScaleNDV`. `skew_ratio` is the source
/// `SessionVars.RiskScaleNDVSkewRatio` value (`0` selects uniform estimation;
/// `1` selects skewed estimation). No validation is added because the Go
/// implementation blends the supplied value directly.
#[must_use]
pub fn scale_ndv(
    original_ndv: f64,
    original_rows: f64,
    selected_rows: f64,
    skew_ratio: f64,
) -> f64 {
    let uniform_ndv = estimate_uniform_ndv(original_ndv, original_rows, selected_rows);
    let skewed_ndv = estimate_skewed_ndv(original_ndv, original_rows, selected_rows);
    skewed_ndv * skew_ratio + uniform_ndv * (1.0 - skew_ratio)
}

/// Uniformly scales NDV, preserving Go's lower/upper result clamps.
fn estimate_uniform_ndv(original_ndv: f64, original_rows: f64, selected_rows: f64) -> f64 {
    if original_rows <= 0.0 || selected_rows <= 0.0 || original_ndv <= 0.0 {
        return 0.0;
    }
    let mut new_ndv = original_ndv;
    if selected_rows >= original_rows {
        return new_ndv;
    }
    let selectivity = selected_rows / original_rows;
    let rows_per_value = original_rows / original_ndv;
    let not_selected_probability_per_row = 1.0 - selectivity;
    let not_selected_probability_per_value = not_selected_probability_per_row.powf(rows_per_value);
    new_ndv = original_ndv * (1.0 - not_selected_probability_per_value);
    new_ndv = go_max(new_ndv, 1.0);
    go_min(new_ndv, selected_rows)
}

/// Applies the source's skewed linear NDV estimate.
fn estimate_skewed_ndv(original_ndv: f64, original_rows: f64, selected_rows: f64) -> f64 {
    if original_rows <= 0.0 {
        return 0.0;
    }
    original_ndv * selected_rows / original_rows
}
