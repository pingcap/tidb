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
use crate::cardinality::row_size::RowSizeColumnStats;

/// Go `statistics.HistColl`, narrowed to the fields cost model v2 reads.
///
/// `property.StatsInfo.HistColl` is not interchangeable with the scalar NDV
/// map: its PRESENCE changes `getAvgRowSize`. A base table carries a
/// collection even when it is pseudo, while joins, projections, and
/// aggregations construct a fresh `StatsInfo` with a nil collection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HistColl {
    pseudo: bool,
    realtime_count: i64,
    columns: BTreeMap<i64, RowSizeColumnStats>,
}

impl HistColl {
    /// Builds the row-size portion of one histogram collection. Column keys
    /// are planner `Column.UniqueID`s, matching Go's generated HistColl.
    #[must_use]
    pub fn new(
        pseudo: bool,
        realtime_count: i64,
        columns: impl IntoIterator<Item = (i64, RowSizeColumnStats)>,
    ) -> Self {
        Self {
            pseudo,
            realtime_count,
            columns: columns.into_iter().collect(),
        }
    }

    /// Go `HistColl.Pseudo`.
    #[must_use]
    pub const fn pseudo(&self) -> bool {
        self.pseudo
    }

    /// Go `HistColl.RealtimeCount`.
    #[must_use]
    pub const fn realtime_count(&self) -> i64 {
        self.realtime_count
    }

    /// The histogram for one planner column, when it is loaded.
    #[must_use]
    pub fn column(&self, unique_id: i64) -> Option<RowSizeColumnStats> {
        self.columns.get(&unique_id).copied()
    }
}

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
    /// Go `StatsInfo.HistColl`. Its presence is preserved only by operators
    /// whose Go derivation copies the child profile or calls `Scale`.
    hist_coll: Option<HistColl>,
    /// Go `StatsInfo.GroupNDVs`: exact NDVs of composite column groups
    /// supplied by indexes. Empty for every profile whose source has no
    /// loaded index statistics, which is Go's nil.
    group_ndvs: Vec<GroupNdv>,
}

impl StatsInfo {
    // `derive_limit_stats`: its awaited Go caller arrived --
    // `attach2Task4PhysicalLimit`'s single-read push-down
    // (`crate::task::attach2_task`'s Limit arm) derives the pushed partial
    // limit's profile through it, exactly the `task.go:633` call site the
    // old verdict named. The TopN arms remain future callers. The live
    // tier's own limit costing stays `tidb_executor::access_cost`
    // (`scan_limit_cap`), a DIFFERENT ranger by design.

    /// Creates a profile from row count and column NDVs.
    #[must_use]
    pub fn new(row_count: f64, col_ndvs: impl IntoIterator<Item = (i64, f64)>) -> Self {
        Self {
            row_count,
            col_ndvs: col_ndvs.into_iter().collect(),
            hist_coll: None,
            group_ndvs: Vec::new(),
        }
    }

    /// The same profile carrying Go's base-table histogram collection.
    #[must_use]
    pub fn with_hist_coll(mut self, hist_coll: HistColl) -> Self {
        self.hist_coll = Some(hist_coll);
        self
    }

    /// Returns Go `StatsInfo.HistColl`.
    #[must_use]
    pub const fn hist_coll(&self) -> Option<&HistColl> {
        self.hist_coll.as_ref()
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
            // Go `StatsInfo.Scale` retains the exact HistColl pointer.
            hist_coll: self.hist_coll.clone(),
            group_ndvs,
        }
    }

    /// Returns the source row count.
    /// One column's NDV, exactly Go's `ColNDVs[id]` map read: `0.0` when the
    /// profile carries no entry for the column.
    #[must_use]
    pub fn col_ndv(&self, id: i64) -> f64 {
        self.col_ndvs.get(&id).copied().unwrap_or(0.0)
    }

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
            // Go `DeriveLimitStats` retains HistColl but not GroupNDVs.
            hist_coll: self.hist_coll.clone(),
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

#[cfg(test)]
mod tests {
    use super::{HistColl, StatsInfo};
    use crate::cardinality::row_size::{RowSizeColumnStats, RowSizeType};

    #[test]
    fn scale_and_limit_retain_hist_coll_like_go() {
        let hist_coll = HistColl::new(
            true,
            100,
            [(
                7,
                RowSizeColumnStats::new(RowSizeType::Long, 800, 0, 100.0, false),
            )],
        );
        let profile = StatsInfo::new(100.0, [(7, 80.0)]).with_hist_coll(hist_coll);

        for derived in [profile.scale(0.5, 1.0), profile.derive_limit_stats(10.0)] {
            let retained = derived.hist_coll().expect("Go retains HistColl");
            assert!(retained.pseudo());
            assert_eq!(retained.realtime_count(), 100);
            assert!(retained.column(7).is_some());
        }
    }
}
