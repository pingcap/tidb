// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded live index scan-cost and task-choice path.
//!
//! This keeps the isolated `Index.QueryBytes` equality adapter separate from
//! an access path's source-owned `CountAfterAccess`.  Real Go access-path
//! derivation computes that scalar before physical conversion; only the
//! explicitly proven point-estimate constructor may use the adapter here.
//! Ranger, expression encoding, physical-property attachment, and non-index
//! alternatives remain with their owning milestones.

use tidb_stats::query_index_bytes;

use super::index_range_policy::{IndexRangeShape, RangeBoundKind};

/// Resolved stats-v1 equality lookups for one index key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexPointStatistics {
    /// Matching TopN count when present.
    pub topn_count: Option<u64>,
    /// Matching CMSketch count when TopN has no match.
    pub cms_count: Option<u64>,
    /// Histogram equal-row fallback.
    pub histogram_count: u64,
}

/// Source-shaped input carried from an index scan into cost selection.
#[derive(Clone, Debug, PartialEq)]
pub struct LiveIndexCandidate {
    /// Stable index identity used only as Go's deterministic tie-breaker.
    pub index_id: i64,
    /// Normalized index ranges.
    pub ranges: Vec<IndexRangeShape>,
    /// Upstream ranger/statistics proof that the sole range is an equality
    /// lookup for the encoded key tuple.
    ///
    /// `IndexRangeShape` deliberately omits Datum values, so endpoint shape
    /// alone cannot prove equality.  The source adapter must provide this
    /// admission before the isolated point-statistics calculation is allowed.
    pub proven_equality_range: bool,
    /// Already-resolved point statistics for a bounded equality range.
    pub point_statistics: IndexPointStatistics,
    /// Existing row-size adapter output for this index's stored columns.
    pub row_size: f64,
    /// Existing TiKV task scan factor.
    pub scan_factor: f64,
    /// Session `IndexScanCostFactor`.
    pub index_scan_cost_factor: f64,
}

/// Live result passed to task comparison.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LiveIndexChoice {
    /// Candidate index identity.
    pub index_id: i64,
    /// Cardinality feeding the physical index scan.
    pub rows: f64,
    /// Source cost plus the deterministic index-ID tie-breaker.
    pub cost: f64,
}

/// Computes the isolated source stats-v1 equality estimate.
///
/// This is deliberately not a general access-path estimator.  Callers must
/// first prove that the candidate represents one inclusive value tuple; all
/// other range forms require their upstream Go `CountAfterAccess` result.
#[must_use]
pub fn estimate_proven_point_rows(candidate: &LiveIndexCandidate) -> Option<f64> {
    let [range] = candidate.ranges.as_slice() else {
        return None;
    };
    if !candidate.proven_equality_range
        || range.low_exclude()
        || range.high_exclude()
        || range.low().is_empty()
        || range.low().len() != range.high().len()
        || !range
            .low()
            .iter()
            .all(|bound| *bound == RangeBoundKind::Value)
        || !range
            .high()
            .iter()
            .all(|bound| *bound == RangeBoundKind::Value)
    {
        return None;
    }
    Some(query_index_bytes(
        candidate.point_statistics.topn_count,
        candidate.point_statistics.cms_count,
        candidate.point_statistics.histogram_count,
    ) as f64)
}

/// Computes the physical-index-scan cost formula and its source tie-breaker.
#[must_use]
pub fn live_index_choice(candidate: &LiveIndexCandidate, rows: f64) -> LiveIndexChoice {
    let row_size = candidate.row_size.max(1.0);
    let cost =
        rows * row_size.log2().max(0.0) * candidate.scan_factor * candidate.index_scan_cost_factor
            + (candidate.index_id % 100) as f64 / 1_000_000.0;
    LiveIndexChoice {
        index_id: candidate.index_id,
        rows,
        cost,
    }
}

/// Returns the strictly lower-cost candidate, preserving the current task on
/// equal cost exactly as `compareTaskCost` does before source tie-breakers.
#[must_use]
pub fn choose_lower_cost(current: LiveIndexChoice, challenger: LiveIndexChoice) -> LiveIndexChoice {
    if challenger.cost < current.cost {
        challenger
    } else {
        current
    }
}
