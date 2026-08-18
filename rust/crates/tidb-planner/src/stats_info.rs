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

//! Dependency-closed statistics-property arithmetic from
//! `pkg/planner/property/stats_info.go`.
//!
//! The Go `StatsInfo` also owns histogram handles and session-driven NDV
//! scaling. This leaf keeps row-count truncation and limit-derived NDV caps
//! over caller-supplied scalar maps, without reconstructing those owners.

use std::collections::BTreeMap;

use crate::cardinality::ndv::GroupNdv;

/// Go `property.StatsInfo` — the ONE port, after the unification.
///
/// This is the profile that travels in a plan (`BasePlan.stats`), the one
/// the per-operator `DeriveStats` bodies build, and — since the merge — the
/// one the DP cardinality rules read too. Its `i64` key is Go's
/// `expression.Column.UniqueID`, matching
/// [`tidb_expr::column::Column::unique_id`] and
/// [`crate::cardinality::ndv::GroupNdv::columns`]. The former second port in
/// `cardinality::derive_stats` (keyed `u64`) is deleted; that module
/// re-exports this type.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StatsInfo {
    row_count: f64,
    col_ndvs: BTreeMap<i64, f64>,
    /// Go `StatsInfo.GroupNDVs`: exact NDVs of composite column groups
    /// supplied by indexes. Empty for every profile whose source has no
    /// loaded index statistics, which is Go's nil.
    group_ndvs: Vec<GroupNdv>,
}

impl StatsInfo {
    // `derive_limit_stats` below: no caller yet -- verdict: awaiting Go
    // `pkg/planner/core/task.go`. Its five production call sites are all
    // `attach2Task` arms for Limit/TopN (lines 633-894 at this branch's
    // pin), the physical-task layer this crate has not ported. The live
    // tier's own limit costing sits in `tidb_executor::access_cost`
    // (`scan_limit_cap`), a DIFFERENT ranger by design -- see the tier
    // topology note -- so nothing should call this until task.go lands
    // here.

    /// Creates a profile from row count and column NDVs.
    #[must_use]
    pub fn new(row_count: f64, col_ndvs: impl IntoIterator<Item = (i64, f64)>) -> Self {
        Self {
            row_count,
            col_ndvs: col_ndvs.into_iter().collect(),
            group_ndvs: Vec::new(),
        }
    }

    /// The same profile carrying group NDVs — Go's `GroupNDVs` field, set by
    /// the stats sources that have index statistics to give.
    #[must_use]
    pub fn with_group_ndvs(mut self, group_ndvs: Vec<GroupNdv>) -> Self {
        self.group_ndvs = group_ndvs;
        self
    }

    /// Returns the composite-group NDVs.
    #[must_use]
    pub fn group_ndvs(&self) -> &[GroupNdv] {
        &self.group_ndvs
    }

    /// Replaces the composite-group NDVs in place.
    pub fn set_group_ndvs(&mut self, group_ndvs: Vec<GroupNdv>) {
        self.group_ndvs = group_ndvs;
    }

    /// Go `StatsInfo.Scale` (`property/stats_info.go:69-86`).
    ///
    /// Every column NDV is re-scaled through
    /// [`scale_ndv`](crate::cardinality::derive_stats::scale_ndv) against the
    /// row count BEFORE the factor was applied — not multiplied by the
    /// factor. At the default skew ratio of `1.0` the two happen to coincide,
    /// because the skewed branch is `ndv * selectedRows / originalRows` and
    /// `selectedRows` is exactly `originalRows * factor`. That equivalence is
    /// a property of the default, not of the rule, which is why the source
    /// expression is kept rather than folded into a multiplication.
    /// Go `StatsInfo.ScaleByExpectCnt` (`property/stats_info.go:91`): scale
    /// down to `expect_cnt` — but only when it is genuinely smaller, and only
    /// when the row count is above 1.0, Go's own overflow guard ("if
    /// s.RowCount is too small, it will cause overflow").
    #[must_use]
    pub fn scale_by_expect_cnt(&self, expect_cnt: f64, skew_ratio: f64) -> Self {
        if expect_cnt >= self.row_count {
            return self.clone();
        }
        if self.row_count > 1.0 {
            return self.scale(expect_cnt / self.row_count, skew_ratio);
        }
        self.clone()
    }

    #[must_use]
    pub fn scale(&self, factor: f64, skew_ratio: f64) -> Self {
        let scale_ndv = crate::cardinality::derive_stats::scale_ndv;
        let scaled_row_count = self.row_count * factor;
        let col_ndvs = self
            .col_ndvs
            .iter()
            .map(|(id, ndv)| {
                (
                    *id,
                    scale_ndv(*ndv, self.row_count, scaled_row_count, skew_ratio),
                )
            })
            .collect();
        let group_ndvs = self
            .group_ndvs
            .iter()
            .map(|group| GroupNdv {
                columns: group.columns.clone(),
                ndv: scale_ndv(group.ndv, self.row_count, scaled_row_count, skew_ratio),
            })
            .collect();
        Self {
            row_count: scaled_row_count,
            col_ndvs,
            group_ndvs,
        }
    }

    /// Returns the source row count.
    #[must_use]
    pub const fn row_count(&self) -> f64 {
        self.row_count
    }

    /// Returns a deterministic column-NDV map.
    #[must_use]
    pub const fn col_ndvs(&self) -> &BTreeMap<i64, f64> {
        &self.col_ndvs
    }

    /// Returns the source `int64(RowCount)` truncation toward zero.
    #[must_use]
    pub fn count(&self) -> i64 {
        self.row_count as i64
    }

    /// Derives limit statistics by capping row count and every column NDV.
    #[must_use]
    pub fn derive_limit_stats(&self, limit_count: f64) -> Self {
        let row_count = source_min(limit_count, self.row_count);
        let col_ndvs = self
            .col_ndvs
            .iter()
            .map(|(id, ndv)| (*id, source_min(*ndv, row_count)))
            .collect();
        Self {
            row_count,
            col_ndvs,
            // Go's DeriveLimitStats builds a fresh StatsInfo and never copies
            // GroupNDVs into it.
            group_ndvs: Vec::new(),
        }
    }
}

// Go's math.Min returns NaN for NaN inputs and otherwise follows ordered
// comparison semantics, including signed zero.
fn source_min(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        return f64::NAN;
    }
    if left == 0.0 && right == 0.0 {
        return if left.is_sign_negative() || right.is_sign_negative() {
            -0.0
        } else {
            0.0
        };
    }
    if left < right {
        left
    } else {
        right
    }
}
