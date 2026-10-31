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

use tidb_planner_property::stats_info::GroupNdv;
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


/// Estimates the NDV for a set of column IDs using the source's conservative
/// and exponential-backoff paths.
///
/// `column_ndvs` is the source `StatsInfo.ColNDVs` map represented as pairs;
/// `row_count` is `StatsInfo.RowCount`; and `skew_ratio` is the caller-owned
/// `RiskGroupNDVSkewRatio`. IDs and group columns are sorted before matching,
/// exactly as `GetGroupNDV4Cols` sorts expression columns in Go. Production
/// planner calls use [`estimate_cols_ndv_with_stats`] to avoid converting the
/// profile map into a temporary vector.
#[must_use]
pub fn estimate_cols_ndv_with_matched_len(
    column_ids: &[i64],
    column_ndvs: &[(i64, f64)],
    row_count: f64,
    group_ndvs: &[GroupNdv],
    skew_ratio: f64,
) -> (f64, usize) {
    estimate_cols_ndv_with_matched_len_lookup(
        column_ids,
        row_count,
        group_ndvs,
        skew_ratio,
        |id| {
            column_ndvs
                .iter()
                .find(|(column_id, _)| *column_id == id)
                .map(|(_, ndv)| *ndv)
        },
    )
}

/// Estimates a column group directly from a live planner statistics profile.
///
/// This is the production entrypoint: looking up each column in the profile's
/// map avoids materializing an intermediate column-NDV vector.
#[must_use]
pub fn estimate_cols_ndv_with_stats(
    column_ids: &[i64],
    profile: &tidb_planner_property::stats_info::StatsInfo,
    skew_ratio: f64,
) -> (f64, usize) {
    estimate_cols_ndv_with_matched_len_lookup(
        column_ids,
        profile.row_count(),
        profile.group_ndvs(),
        skew_ratio,
        |id| profile.col_ndvs().get(&id).copied(),
    )
}

fn estimate_cols_ndv_with_matched_len_lookup(
    column_ids: &[i64],
    row_count: f64,
    group_ndvs: &[GroupNdv],
    skew_ratio: f64,
    mut column_ndv: impl FnMut(i64) -> Option<f64>,
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

    if ids.len() == 1 {
        let ndv = column_ndv(ids[0])
            .filter(|ndv| *ndv > 0.0)
            .map_or(1.0, |ndv| go_max(ndv, 1.0));
        return (ndv, 1);
    }

    let values = ids
        .iter()
        .filter_map(|id| column_ndv(*id))
        .filter(|ndv| *ndv > 0.0)
        .collect::<Vec<_>>();
    let conservative_ndv = values.iter().copied().fold(1.0, go_max);

    let mut values = values;
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

/// Go `cardinality.EstimateColumnNDV` and its `getTotalRowCount` helper.
///
/// Lite statistics initialization keeps every histogram's NDV metadata but
/// loads buckets and TopN only for the items the query needs. An evicted
/// column therefore borrows the analyzed row count from the first fully
/// loaded, same-version index, then from a column. The Rust catalog keeps
/// the payloads eagerly, so callers pass the items Go's predicate-statistics
/// collection would have marked fully loaded for this query.
pub fn estimate_column_ndv(
    s: &tidb_stats::Table,
    column_id: i64,
    full_loaded_columns: &std::collections::BTreeSet<i64>,
    full_loaded_indexes: &std::collections::BTreeSet<i64>,
) -> Option<f64> {
    let column = s.hist_coll.get_column(column_id)?;
    let column = read(&column);
    // Go reads the histogram's NDV only when `IsStatsInitialized()`; an
    // `EmptyColumn` placeholder -- a never-analyzed column a load request
    // published -- takes the `RealtimeCount * distinctFactor` branch.
    if !column.is_stats_initialized() {
        return None;
    }
    let version = column.histogram.last_update_version;
    let analyzed_count = if full_loaded_columns.contains(&column_id) {
        column.total_row_count() as i64
    } else {
        full_loaded_indexes
            .iter()
            .find_map(|id| {
                s.hist_coll.get_index(*id).and_then(|index| {
                    let index = read(&index);
                    (index.histogram.last_update_version == version)
                        .then(|| index.total_row_count() as i64)
                })
            })
            .unwrap_or_else(|| {
                full_loaded_columns
                    .iter()
                    .filter(|id| **id != column_id)
                    .find_map(|id| {
                        s.hist_coll.get_column(*id).and_then(|candidate| {
                            let candidate = read(&candidate);
                            let same_version = candidate.histogram.last_update_version == version;
                            // A candidate whose histogram never got
                            // buckets stores its rows in TopN only, and
                            // the persisted TopN total can sit BELOW the
                            // analyze-time row count (TPC-H SF50
                            // lineitem.l_quantity is 26 rows short of
                            // 300,005,811). Charging the factor with
                            // that short total skewed every borrowed
                            // NDV; an evicted column must then fall
                            // through to no-factor (Go's shape, which
                            // loads the needed column itself).
                            // REVERTED to this guard after an experiment
                            // borrowing from any allLoaded column (the
                            // literal Go-source shape): TPC-DS q13/q48/
                            // q49/q91 flipped inconsistent and the full
                            // capture dropped to 67/83. The guard is
                            // empirically load-bearing on this capture.
                            let buckets_loaded = !candidate.histogram.buckets.is_empty();
                            (same_version && buckets_loaded)
                                .then(|| candidate.total_row_count() as i64)
                        })
                    })
                    .unwrap_or(0)
            })
    };
    let mut ndv = column.histogram.ndv as f64;
    if analyzed_count > 0 {
        ndv *= s.hist_coll.realtime_count as f64 / analyzed_count as f64;
    }
    Some(ndv)
}

fn read<T>(lock: &std::sync::RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}
