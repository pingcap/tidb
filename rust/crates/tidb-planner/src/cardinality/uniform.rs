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

//! Uniform equality estimates from `pkg/planner/cardinality/row_count_index.go`.
//!
//! `estimateRowCountWithUniformDistribution` is shared by index and column
//! equality paths in Go, but its inputs are owned by the statistics and
//! session layers.  This leaf accepts the normalized histogram/TopN metadata
//! and preserves only the source arithmetic.  It does not create a histogram,
//! inspect a session, or publish the risk variable to a plan context.

use super::{go_max, go_min, out_of_range::out_of_range_full_ndv, row_count_column::RowEstimate};

/// Caller-owned statistics required by the source uniform equality helper.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct UniformEqualityStats {
    /// Histogram NDV before removing TopN values.
    pub histogram_ndv: i64,
    /// Number of entries represented by TopN.
    pub topn_len: usize,
    /// Number of rows when the histogram was analyzed.
    pub total_row_count: f64,
    /// Number of non-NULL histogram rows. A non-positive value is replaced by
    /// `total_row_count - null_count`, matching the source empty-histogram
    /// fallback.
    pub not_null_count: f64,
    /// Number of NULL rows in the histogram.
    pub null_count: f64,
    /// Current realtime table row count.
    pub realtime_row_count: f64,
    /// Scale from analyzed rows to realtime rows.
    pub increase_factor: f64,
    /// Modification delta used by the out-of-range NDV derivation.
    pub modify_count: i64,
    /// Session `RiskEqSkewRatio` value supplied by the caller.
    pub risk_eq_skew_ratio: f64,
    /// Minimum TopN frequency, when TopN is non-empty.
    pub topn_min_count: Option<f64>,
}

/// Estimates one equality value not represented by TopN or a histogram.
///
/// This ports `estimateRowCountWithUniformDistribution`, including the
/// empty-histogram branch, source out-of-range NDV derivation, and optional
/// risk-skew interpolation.  A `None` TopN minimum means the source TopN is
/// empty and therefore imposes no skew cap.
#[must_use]
pub fn estimate_uniform_equality(stats: UniformEqualityStats) -> RowEstimate {
    let hist_ndv = stats.histogram_ndv as f64 - stats.topn_len as f64;
    let mut not_null_count = stats.not_null_count;

    let avg_row_estimate = if hist_ndv <= 0.0 || not_null_count == 0.0 {
        // Sampling can leave a positive histogram NDV with no loaded bucket
        // rows. With no modifications, the source uses one less than the
        // smallest TopN value as the conservative estimate.
        if hist_ndv > 0.0 && stats.modify_count == 0 {
            let min_topn = stats.topn_min_count.unwrap_or(0.0);
            return RowEstimate::default_est(go_max(min_topn - 1.0, 1.0));
        }

        // An empty histogram has no non-NULL count; derive it from the
        // analyzed total after removing NULL rows before the NDV smoothing.
        if not_null_count <= 0.0 {
            not_null_count = stats.total_row_count - stats.null_count;
        }
        out_of_range_full_ndv(
            stats.histogram_ndv as f64,
            stats.total_row_count,
            not_null_count,
            stats.realtime_row_count,
            stats.increase_factor,
            stats.modify_count,
        )
    } else {
        not_null_count / hist_ndv
    };

    if stats.risk_eq_skew_ratio > 0.0 {
        let mut skew_estimate = not_null_count - (hist_ndv - 1.0);
        if let Some(min_topn) = stats.topn_min_count {
            if min_topn > 0.0 {
                skew_estimate = go_min(skew_estimate, min_topn);
            }
        }
        return calculate_skew_ratio_counts(
            avg_row_estimate,
            skew_estimate,
            stats.risk_eq_skew_ratio,
        );
    }

    RowEstimate::default_est(avg_row_estimate)
}

/// Re-exports the shared `statistics.CalculateSkewRatioCounts` arithmetic at
/// the planner's `RowEstimate` boundary. The implementation stays owned by
/// `tidb-stats`; this adapter keeps the planner leaf from duplicating the
/// histogram package's source formula.
#[must_use]
pub fn calculate_skew_ratio_counts(
    estimate: f64,
    skew_estimate: f64,
    skew_ratio: f64,
) -> RowEstimate {
    let source = tidb_stats::calculate_skew_ratio_counts(estimate, skew_estimate, skew_ratio);
    RowEstimate::new(source.est, source.min_est, source.max_est)
}
