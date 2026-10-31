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
//! This module owns the complete value behavior of Go `property.StatsInfo`.

use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_datatype::FieldTypeCode;

/// Go `statistics.HistColl`, the collection `GenerateHistCollFromColumnInfo`
/// builds for a data source (`StatsInfo.HistColl`).
///
/// Its PRESENCE changes `getAvgRowSize`: a base table carries a collection
/// even when it is pseudo, while joins, projections, and aggregations
/// construct a fresh `StatsInfo` with a nil collection.
pub use tidb_stats::HistColl;

/// The reads planner cardinality and costing make of Go's `HistColl`: each is
/// a Go field read (`GetCol`, `GetIdx`, `Idx2ColUniqueIDs`, ...) or the
/// cardinality helper Go spells inline at its call sites.
pub trait HistCollExt {
    /// Go `HistColl.Pseudo`.
    fn pseudo(&self) -> bool;
    /// Go `HistColl.RealtimeCount`.
    fn realtime_count(&self) -> i64;
    /// Go `HistColl.ModifyCount`.
    fn modify_count(&self) -> i64;
    /// Go `coll.GetCol(uniqueID) != nil`.
    fn has_column(&self, unique_id: i64) -> bool;
    /// Go `coll.GetCol(uniqueID)`.
    fn histogram(&self, unique_id: i64) -> Option<tidb_stats::SharedColumn>;
    /// Go `ColumnStatsIsInvalid`'s estimation gate over `coll.GetCol`.
    fn histogram_for_estimation(&self, unique_id: i64) -> Option<tidb_stats::SharedColumn>;
    /// Go `coll.GetCol(uniqueID).IsFullLoad()`.
    fn column_is_full_load(&self, unique_id: i64) -> bool;
    /// Go `coll.GetCol(colID).Info`, as `pseudoSelectivity` reads it.
    fn column_info(&self, unique_id: i64) -> Option<PseudoColumn>;
    /// Go `getAvgRowSize`'s reads of `coll.GetCol(uniqueID)`.
    fn column(&self, unique_id: i64) -> Option<RowSizeColumnStats>;
    /// Go `coll.GetIdx(id)`.
    fn index_histogram(&self, index_id: i64) -> Option<tidb_stats::SharedIndex>;
    /// Go `coll.GetIdx(id).IsFullLoad()`.
    fn index_is_full_load(&self, index_id: i64) -> bool;
    /// Go `coll.GetIdx(id).Info`.
    fn index_info(&self, index_id: i64) -> Option<tidb_stats::IndexInfo>;
    /// Go `coll.ForEachIndexImmutable`'s `Info`s, in index-ID order.
    fn index_infos(&self) -> Vec<tidb_stats::IndexInfo>;
    /// Go `coll.ForEachIndexImmutable`'s index IDs, sorted as `Selectivity`
    /// stabilizes them.
    fn index_ids(&self) -> Vec<i64>;
    /// Go `coll.ColNum() == 0 && coll.IdxNum() == 0`.
    fn has_no_column_or_index(&self) -> bool;
    /// Go `HistColl.ColUniqueID2IdxIDs[uniqueID]`.
    fn index_ids_for_column(&self, unique_id: i64) -> &[i64];
    /// Go `HistColl.Idx2ColUniqueIDs[id]`.
    fn index_columns(&self, index_id: i64) -> &[i64];
    /// Go `HistColl.MVIdx2Columns[id]`.
    fn mv_index_columns(&self, index_id: i64) -> &[tidb_expr::column::Column];
    /// `canSkipIndexEstimation`'s reads of `idx.Info`.
    fn index_range_policy(&self, index_id: i64) -> IndexRangePolicy;
    /// Go `HistColl.GetScaledRealtimeAndModifyCnt(coll.GetIdx(id))`.
    fn index_row_counts(&self, index_id: i64) -> Option<IndexRowCounts>;
    /// Go `getGroupNDVs`' `(Idx2ColUniqueIDs[id], NDV)` per index covering
    /// all its columns.
    fn index_ndvs(&self) -> BTreeMap<i64, (Vec<i64>, f64)>;
    /// Go `getColsNDVLowerBoundFromHistColl`: initialized column, exact index
    /// column set, then the maximum initialized individual-column NDV.
    fn ndv_lower_bound(&self, columns: &[i64]) -> Option<i64>;
}

/// Test fixture: Go `NewHistCollWithColsAndIdxs` over fully loaded column
/// statistics keyed by planner unique ID and fully loaded index statistics
/// with their `Idx2ColUniqueIDs` (which also fill `ColUniqueID2IdxIDs`).
#[doc(hidden)]
#[must_use]
pub fn loaded_hist_coll_for_test(
    pseudo: bool,
    realtime_count: i64,
    modify_count: i64,
    columns: impl IntoIterator<Item = (i64, tidb_stats::column::Column)>,
    indexes: impl IntoIterator<Item = (i64, tidb_stats::index::Index, Vec<i64>)>,
) -> Arc<HistColl> {
    let mut index_columns = Vec::new();
    let mut coll = HistColl::new_with_cols_and_idxs(
        0,
        realtime_count,
        modify_count,
        columns.into_iter().map(|(id, mut column)| {
            column.stats_loaded_status = tidb_stats::StatsLoadedStatus::full_load();
            (id, column)
        }),
        indexes.into_iter().map(|(id, mut index, columns)| {
            index.stats_loaded_status = tidb_stats::StatsLoadedStatus::full_load();
            index_columns.push((id, columns));
            (id, index)
        }),
    );
    coll.pseudo = pseudo;
    for (id, columns) in index_columns {
        if let Some(first) = columns.first() {
            coll.col_unique_id_to_idx_ids.entry(*first).or_default().push(id);
        }
        coll.idx_to_col_unique_ids.insert(id, columns);
    }
    for ids in coll.col_unique_id_to_idx_ids.values_mut() {
        ids.sort_unstable();
    }
    Arc::new(coll)
}

fn read<T>(lock: &std::sync::RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl HistCollExt for HistColl {
    fn pseudo(&self) -> bool {
        self.pseudo
    }

    fn realtime_count(&self) -> i64 {
        self.realtime_count
    }

    fn modify_count(&self) -> i64 {
        self.modify_count
    }

    fn has_column(&self, unique_id: i64) -> bool {
        self.get_column(unique_id).is_some()
    }

    fn histogram(&self, unique_id: i64) -> Option<tidb_stats::SharedColumn> {
        self.get_column(unique_id)
    }

    fn histogram_for_estimation(&self, unique_id: i64) -> Option<tidb_stats::SharedColumn> {
        let column = self.get_column(unique_id)?;
        let valid = {
            let stats = read(&column);
            let essential = stats.stats_loaded_status.is_essential_stats_loaded();
            stats.is_valid_for_estimation(self.pseudo, essential)
        };
        valid.then_some(column)
    }

    fn column_is_full_load(&self, unique_id: i64) -> bool {
        self.get_column(unique_id)
            .is_some_and(|column| read(&column).is_full_load())
    }

    fn column_info(&self, unique_id: i64) -> Option<PseudoColumn> {
        let column = self.get_column(unique_id)?;
        let column = read(&column);
        let info = column.info.as_ref()?.read();
        Some(PseudoColumn {
            lower_name: info.name.lowercase().to_owned(),
            unique_key_flag: info
                .field_type
                .has_flag(tidb_datatype::FieldTypeFlags::UNIQUE_KEY),
        })
    }

    fn column(&self, unique_id: i64) -> Option<RowSizeColumnStats> {
        let column = self.get_column(unique_id)?;
        let column = read(&column);
        let code = column
            .info
            .as_ref()
            .map_or(FieldTypeCode::Unspecified, |info| info.read().field_type.code());
        Some(RowSizeColumnStats::new(
            RowSizeType::from_field_type_code(code),
            column.histogram.tot_col_size,
            column.histogram.null_count,
            column.total_row_count(),
            column.is_handle,
        ))
    }

    fn index_histogram(&self, index_id: i64) -> Option<tidb_stats::SharedIndex> {
        self.get_index(index_id)
    }

    fn index_is_full_load(&self, index_id: i64) -> bool {
        self.get_index(index_id)
            .is_some_and(|index| read(&index).is_full_load())
    }

    fn index_info(&self, index_id: i64) -> Option<tidb_stats::IndexInfo> {
        let index = self.get_index(index_id)?;
        let info = read(&index).info.clone();
        info
    }

    fn index_infos(&self) -> Vec<tidb_stats::IndexInfo> {
        self.index_ids()
            .into_iter()
            .filter_map(|id| self.index_info(id))
            .collect()
    }

    fn index_ids(&self) -> Vec<i64> {
        let mut ids = Vec::with_capacity(self.index_count());
        self.for_each_index(|id, _| {
            ids.push(id);
            false
        });
        ids.sort_unstable();
        ids
    }

    fn has_no_column_or_index(&self) -> bool {
        self.column_count() == 0 && self.index_count() == 0
    }

    fn index_ids_for_column(&self, unique_id: i64) -> &[i64] {
        self.col_unique_id_to_idx_ids
            .get(&unique_id)
            .map_or(&[], Vec::as_slice)
    }

    fn index_columns(&self, index_id: i64) -> &[i64] {
        self.idx_to_col_unique_ids
            .get(&index_id)
            .map_or(&[], Vec::as_slice)
    }

    fn mv_index_columns(&self, index_id: i64) -> &[tidb_expr::column::Column] {
        self.mv_idx_to_columns.get(&index_id).map_or(&[], Vec::as_slice)
    }

    fn index_range_policy(&self, index_id: i64) -> IndexRangePolicy {
        self.index_info(index_id)
            .map(|info| {
                let info = info.read();
                IndexRangePolicy {
                    has_condition: !info.condition_expr_string.is_empty(),
                    is_multi_value: info.mv_index,
                }
            })
            .unwrap_or_default()
    }

    fn index_row_counts(&self, index_id: i64) -> Option<IndexRowCounts> {
        let index = self.get_index(index_id)?;
        let (index_realtime, index_modify) =
            self.scaled_realtime_and_modify_count(Some(&read(&index)));
        Some(IndexRowCounts {
            table_realtime: self.realtime_count,
            table_modify: self.modify_count,
            index_realtime,
            index_modify,
        })
    }

    fn index_ndvs(&self) -> BTreeMap<i64, (Vec<i64>, f64)> {
        let mut ndvs = BTreeMap::new();
        self.for_each_index(|id, index| {
            let columns = self.index_columns(id);
            let declared = index.info.as_ref().map_or(0, |info| info.read().columns.len());
            if index.histogram.ndv > 0 && !columns.is_empty() && columns.len() == declared {
                ndvs.insert(id, (columns.to_vec(), index.histogram.ndv as f64));
            }
            false
        });
        ndvs
    }

    fn ndv_lower_bound(&self, columns: &[i64]) -> Option<i64> {
        let initialized_column_ndv = |unique_id: i64| {
            let column = self.get_column(unique_id)?;
            let column = read(&column);
            column
                .is_stats_initialized()
                .then_some(column.histogram.ndv)
        };
        if columns.is_empty() {
            return None;
        }
        if columns.len() == 1 {
            if let Some(ndv) = initialized_column_ndv(columns[0]) {
                return Some(ndv);
            }
        }
        let mut columns = columns.to_vec();
        columns.sort_unstable();
        for id in self.index_ids() {
            let Some(index) = self.get_index(id) else {
                continue;
            };
            let index = read(&index);
            if !index.stats_loaded_status.stats_initialized() {
                continue;
            }
            let mut index_columns = self.index_columns(id).to_vec();
            let declared = index.info.as_ref().map_or(0, |info| info.read().columns.len());
            if index_columns.len() != declared || index_columns.len() != columns.len() {
                continue;
            }
            index_columns.sort_unstable();
            if index_columns == columns {
                return Some(index.histogram.ndv);
            }
        }
        columns
            .iter()
            .filter_map(|id| initialized_column_ndv(*id))
            .max()
    }
}

/// Go `property.StatsInfo` — the ONE port, after the unification.
///
/// This is the profile that travels in a plan (`BasePlan.stats`), the one
/// the per-operator `DeriveStats` bodies build, and — since the merge — the
/// one the DP cardinality rules read too. Its `i64` key is Go's
/// `expression.Column.UniqueID`, matching
/// [`tidb_expr::column::Column::unique_id`] and
/// [`GroupNdv::columns`]. The former second port in
/// `cardinality::derive_stats` (keyed `u64`) is deleted; that module
/// re-exports this type.
#[derive(Clone, Debug, Default)]
pub struct StatsInfo {
    row_count: f64,
    col_ndvs: BTreeMap<i64, f64>,
    /// Go `StatsInfo.HistColl`. Its presence is preserved only by operators
    /// whose Go derivation copies the child profile or calls `Scale`.
    hist_coll: Option<Arc<HistColl>>,
    /// Go `StatsInfo.StatsVersion`.
    stats_version: u64,
    /// Go `StatsInfo.GroupNDVs`: exact NDVs of composite column groups
    /// supplied by indexes. Empty for every profile whose source has no
    /// loaded index statistics, which is Go's nil.
    group_ndvs: Vec<GroupNdv>,
}

/// Value equality; `HistColl` compares as Go's `*HistColl` pointer.
impl PartialEq for StatsInfo {
    fn eq(&self, other: &Self) -> bool {
        self.row_count == other.row_count
            && self.col_ndvs == other.col_ndvs
            && match (&self.hist_coll, &other.hist_coll) {
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                (None, None) => true,
                _ => false,
            }
            && self.stats_version == other.stats_version
            && self.group_ndvs == other.group_ndvs
    }
}

impl StatsInfo {
    // `derive_limit_stats`: its awaited Go caller arrived --
    // `attach2Task4PhysicalLimit`'s single-read push-down
    // (`tidb_planner_core_operator_physicalop::task::attach2_task`'s Limit arm) derives the pushed partial
    // limit's profile through it, exactly the `task.go:633` call site the
    // old verdict named. The TopN arms remain future callers. The live
    // tier's own limit costing stays `tidb_executor::access_cost`
    // (`scan_limit_cap`), a DIFFERENT ranger by design.

    /// Changes only row count, as Go does when Expand replicates input rows.
    pub fn set_row_count(&mut self, row_count: f64) {
        self.row_count = row_count;
    }

    /// Creates a profile from row count and column NDVs.
    #[must_use]
    pub fn new(row_count: f64, col_ndvs: impl IntoIterator<Item = (i64, f64)>) -> Self {
        Self {
            row_count,
            col_ndvs: col_ndvs.into_iter().collect(),
            hist_coll: None,
            stats_version: 0,
            group_ndvs: Vec::new(),
        }
    }

    /// The same profile carrying Go's base-table histogram collection.
    #[must_use]
    pub fn with_hist_coll(mut self, hist_coll: Arc<HistColl>) -> Self {
        self.hist_coll = Some(hist_coll);
        self
    }

    /// Returns Go `StatsInfo.HistColl`.
    #[must_use]
    pub const fn hist_coll(&self) -> Option<&Arc<HistColl>> {
        self.hist_coll.as_ref()
    }

    /// The same profile carrying Go's statistics version.
    #[must_use]
    pub const fn with_stats_version(mut self, stats_version: u64) -> Self {
        self.stats_version = stats_version;
        self
    }

    /// Go `StatsInfo.StatsVersion`.
    #[must_use]
    pub const fn stats_version(&self) -> u64 {
        self.stats_version
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
    /// `scale_ndv` (the planner's `cardinality::derive_stats::scale_ndv`) against the
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
    pub fn scale_by_expect_cnt_with(
        &self,
        expect_cnt: f64,
        skew_ratio: f64,
        scale_ndv: ScaleNdvFunc,
    ) -> Self {
        if expect_cnt >= self.row_count {
            return self.clone();
        }
        if self.row_count > 1.0 {
            return self.scale_with(expect_cnt / self.row_count, skew_ratio, scale_ndv);
        }
        self.clone()
    }

    /// Scale row count and every NDV through `scale_ndv` (Go's `ScaleNDVFunc`,
    /// which `cardinality` registers to avoid the import cycle).
    pub fn scale_with(&self, factor: f64, skew_ratio: f64, scale_ndv: ScaleNdvFunc) -> Self {
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
            stats_version: self.stats_version,
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
    /// Go `StatsInfo.RowCount`.
    pub const fn row_count(&self) -> f64 {
        self.row_count
    }

    /// Returns a deterministic column-NDV map.
    #[must_use]
    pub const fn col_ndvs(&self) -> &BTreeMap<i64, f64> {
        &self.col_ndvs
    }

    /// Returns the source `int64(RowCount)` truncation toward zero.
    pub fn count(&self) -> i64 {
        self.row_count as i64
    }

    /// Go `GetGroupNDV4Cols`: exact match after sorting requested column IDs.
    pub fn group_ndv_for_cols(&self, cols: &[tidb_expr::column::Column]) -> Option<&GroupNdv> {
        if cols.is_empty() || self.group_ndvs.is_empty() {
            return None;
        }
        let mut ids: Vec<_> = cols.iter().map(|col| col.unique_id).collect();
        ids.sort_unstable();
        self.group_ndvs
            .iter()
            .find(|group| group.columns.len() == ids.len() && group.columns == ids)
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
            // Go `DeriveLimitStats` leaves StatsVersion at its zero value.
            stats_version: 0,
            group_ndvs: Vec::new(),
        }
    }
}

impl std::fmt::Display for StatsInfo {
    /// Go `StatsInfo.String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "count {}, ColNDVs map[", self.row_count)?;
        for (index, (id, ndv)) in self.col_ndvs.iter().enumerate() {
            if index > 0 {
                f.write_str(" ")?;
            }
            write!(f, "{id}:{ndv}")?;
        }
        f.write_str("]")
    }
}

/// Go `ToString`, used by statistics tests.
pub fn group_ndvs_to_string(groups: &[GroupNdv]) -> String {
    let values = groups
        .iter()
        .map(|group| {
            let cols = group
                .columns
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" ");
            format!("{{[{cols}] {}}}", group.ndv)
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!("[{values}]")
}

/// Go `DeriveLimitStats`.
pub fn derive_limit_stats(child: &StatsInfo, limit_count: f64) -> StatsInfo {
    child.derive_limit_stats(limit_count)
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
    if left < right { left } else { right }
}

#[cfg(test)]
mod tests {
    use super::{loaded_hist_coll_for_test, HistCollExt as _, StatsInfo};
    use super::GroupNdv;
    use tidb_expr::column::Column;

    fn column_stats(ndv: i64) -> tidb_stats::column::Column {
        tidb_stats::column::Column {
            histogram: tidb_stats::Histogram {
                ndv,
                ..tidb_stats::Histogram::default()
            },
            stats_version: 2,
            ..tidb_stats::column::Column::default()
        }
    }

    /// Index statistics whose `Info` declares `columns` key columns.
    fn index_stats(id: i64, ndv: i64, columns: usize) -> tidb_stats::index::Index {
        tidb_stats::index::Index {
            histogram: tidb_stats::Histogram {
                id,
                ndv,
                ..tidb_stats::Histogram::default()
            },
            info: Some(tidb_model::GoShared::new(tidb_model::IndexInfo {
                id,
                columns: vec![tidb_model::IndexColumn::default(); columns].into(),
                ..tidb_model::IndexInfo::default()
            })),
            stats_version: 2,
            ..tidb_stats::index::Index::default()
        }
    }

    /// An NDV that scales with the selected rows, the planner's `scale_ndv`
    /// at skew ratio 1.0.
    fn proportional(ndv: f64, rows: f64, selected: f64, _skew_ratio: f64) -> f64 {
        ndv * selected / rows
    }

    #[test]
    fn scale_and_limit_retain_hist_coll_like_go() {
        let hist_coll = loaded_hist_coll_for_test(true, 100, 0, [(7, column_stats(10))], []);
        let profile = StatsInfo::new(100.0, [(7, 80.0)]).with_hist_coll(hist_coll);

        for derived in [profile.scale_with(0.5, 1.0, proportional), profile.derive_limit_stats(10.0)] {
            let retained = derived.hist_coll().expect("Go retains HistColl");
            assert!(retained.pseudo);
            assert_eq!(retained.realtime_count, 100);
            assert!(retained.column(7).is_some());
        }
    }

    #[test]
    fn hist_coll_sorts_column_index_lookup_by_index_id() {
        let hist_coll = loaded_hist_coll_for_test(
            false,
            100,
            0,
            [],
            [(11, index_stats(11, 1, 2), vec![7, 8]), (4, index_stats(4, 1, 1), vec![7])],
        );

        assert_eq!(hist_coll.index_ids_for_column(7), [4, 11]);
        assert!(hist_coll.index_ids_for_column(8).is_empty());
        assert!(hist_coll.index_ids_for_column(9).is_empty());
        assert_eq!(hist_coll.index_columns(11), [7, 8]);
    }

    #[test]
    fn scale_retains_version_and_groups_but_limit_drops_them() {
        let profile = StatsInfo::new(100.0, [(7, 80.0)])
            .with_stats_version(42)
            .with_group_ndvs(vec![GroupNdv {
                columns: vec![7, 9],
                ndv: 60.0,
            }]);
        let scaled = profile.scale_with(0.5, 1.0, proportional);
        assert_eq!(scaled.stats_version(), 42);
        assert_eq!(scaled.group_ndvs()[0].ndv, 30.0);

        let limited = profile.derive_limit_stats(10.0);
        assert_eq!(limited.stats_version(), 0);
        assert!(limited.group_ndvs().is_empty());
        assert_eq!(limited.col_ndv(7), 10.0);
    }

    #[test]
    fn group_ndv_lookup_sorts_requested_columns_and_matches_exactly() {
        let profile = StatsInfo::new(100.0, []).with_group_ndvs(vec![GroupNdv {
            columns: vec![7, 9],
            ndv: 12.0,
        }]);
        let mut nine = Column::default();
        nine.unique_id = 9;
        let mut seven = Column::default();
        seven.unique_id = 7;
        assert_eq!(
            profile
                .group_ndv_for_cols(&[nine.clone(), seven])
                .map(|g| g.ndv),
            Some(12.0)
        );
        assert!(profile.group_ndv_for_cols(&[nine]).is_none());
        assert!(profile.group_ndv_for_cols(&[]).is_none());
    }

    #[test]
    fn initialized_ndv_bounds_use_column_and_index_evidence() {
        let hist = loaded_hist_coll_for_test(
            false,
            1000,
            0,
            [(1, column_stats(10)), (2, column_stats(20)), (3, column_stats(0))],
            [
                (4, index_stats(4, 50, 2), vec![2, 1]),
                (5, index_stats(5, 99, 1), vec![1]),
                (6, index_stats(6, 100, 1), vec![3]),
            ],
        );
        // A single initialized column is authoritative, including NDV zero.
        assert_eq!(hist.ndv_lower_bound(&[1]), Some(10));
        assert_eq!(hist.ndv_lower_bound(&[3]), Some(0));
        // Index key order does not change the NDV of the same column set.
        assert_eq!(hist.ndv_lower_bound(&[1, 2]), Some(50));
        assert_eq!(hist.ndv_lower_bound(&[2, 1]), Some(50));
        // Missing dimensions retain a safe lower bound from known columns.
        assert_eq!(hist.ndv_lower_bound(&[1, 2, 99]), Some(20));
        assert_eq!(hist.ndv_lower_bound(&[99]), None);
        assert_eq!(hist.ndv_lower_bound(&[]), None);
        assert_eq!(
            loaded_hist_coll_for_test(false, 1000, 0, [], []).ndv_lower_bound(&[1]),
            None
        );
    }
}

/// Go `property.ScaleNDVFunc`: how an NDV scales with the rows it was
/// observed over (`original_ndv`, `original_rows`, `selected_rows`, skew ratio).
pub type ScaleNdvFunc = fn(f64, f64, f64, f64) -> f64;

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

/// The column a pseudo predicate resolved to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PseudoColumn {
    /// Lowercased column name, the key of the source's `colExists` map.
    pub lower_name: String,
    /// Whether `mysql.HasUniKeyFlag(col.Info.GetFlag())` holds.
    pub unique_key_flag: bool,
}

/// The MySQL storage types whose row-size encoding has a fixed or special
/// planner width in TiDB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowSizeType {
    /// 32-bit floating-point values.
    Float,
    /// 64-bit floating-point values.
    Double,
    /// Duration/time values.
    Duration,
    /// Date values.
    Date,
    /// Datetime values.
    Datetime,
    /// Timestamp values.
    Timestamp,
    /// TINYINT values.
    Tiny,
    /// SMALLINT values.
    Short,
    /// MEDIUMINT values.
    Int24,
    /// INT values.
    Long,
    /// BIGINT values.
    LongLong,
    /// YEAR values.
    Year,
    /// ENUM values.
    Enum,
    /// BIT values.
    Bit,
    /// SET values.
    Set,
    /// DECIMAL values.
    Decimal,
    /// Variable-length string/blob-like values.
    Variable,
}

impl RowSizeType {
    /// Maps Go `FieldType.GetType()` onto the branches in `GetFixedLen`,
    /// `AvgColSize`, and `AvgColSizeDataInDiskByRows`.
    #[must_use]
    pub const fn from_field_type_code(code: FieldTypeCode) -> Self {
        match code {
            FieldTypeCode::Float => Self::Float,
            FieldTypeCode::Double => Self::Double,
            FieldTypeCode::Duration => Self::Duration,
            FieldTypeCode::Date | FieldTypeCode::NewDate => Self::Date,
            FieldTypeCode::Datetime => Self::Datetime,
            FieldTypeCode::Timestamp => Self::Timestamp,
            FieldTypeCode::Tiny => Self::Tiny,
            FieldTypeCode::Short => Self::Short,
            FieldTypeCode::Int24 => Self::Int24,
            FieldTypeCode::Long => Self::Long,
            FieldTypeCode::LongLong => Self::LongLong,
            FieldTypeCode::Year => Self::Year,
            FieldTypeCode::Enum => Self::Enum,
            FieldTypeCode::Bit => Self::Bit,
            FieldTypeCode::Set => Self::Set,
            FieldTypeCode::NewDecimal => Self::Decimal,
            FieldTypeCode::Unspecified
            | FieldTypeCode::Varchar
            | FieldTypeCode::Json
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
            | FieldTypeCode::String
            | FieldTypeCode::Geometry
            | FieldTypeCode::VectorFloat32
            | FieldTypeCode::Null
            | FieldTypeCode::VarString
            | FieldTypeCode::Blob
            | FieldTypeCode::Unknown(_) => Self::Variable,
        }
    }

    /// Returns the chunk-memory width used by `GetFixedLen`.
    #[must_use]
    pub const fn fixed_len(self) -> Option<f64> {
        match self {
            Self::Float => Some(4.0),
            Self::Tiny
            | Self::Short
            | Self::Int24
            | Self::Long
            | Self::LongLong
            | Self::Double
            | Self::Year
            | Self::Duration => Some(8.0),
            // `unsafe.Sizeof(types.ZeroTime)` in the Go source is 8 bytes.
            Self::Date | Self::Datetime | Self::Timestamp => Some(8.0),
            // `types.MyDecimalStructSize` in the Go source is 40 bytes.
            Self::Decimal => Some(40.0),
            Self::Enum | Self::Bit | Self::Set | Self::Variable => None,
        }
    }

    /// Returns whether `AvgColSize` uses the 8-byte encoded-key/value rule.
    #[must_use]
    pub const fn uses_eight_byte_encoding(self, is_key: bool) -> bool {
        matches!(
            self,
            Self::Float
                | Self::Double
                | Self::Duration
                | Self::Date
                | Self::Datetime
                | Self::Timestamp
        ) || (is_key
            && matches!(
                self,
                Self::Tiny
                    | Self::Short
                    | Self::Int24
                    | Self::Long
                    | Self::LongLong
                    | Self::Year
                    | Self::Enum
                    | Self::Bit
                    | Self::Set
            ))
    }
}

/// The statistics fields consumed by the row-size formulas.
///
/// `total_row_count` mirrors Go's `Column.TotalRowCount()` (`float64`, because
/// histogram and TopN counts are summed as floating-point values). The
/// histogram's source integer fields retain their integer widths; arithmetic
/// conversion happens only where the Go implementation divides them.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RowSizeColumnStats {
    /// Source histogram type.
    pub kind: RowSizeType,
    /// Whether this column is the handle column.
    pub is_handle: bool,
    /// Source `Histogram.TotColSize`.
    pub total_column_size: i64,
    /// Source `Histogram.NullCount`.
    pub null_count: i64,
    /// Source `Column.TotalRowCount()`.
    pub total_row_count: f64,
}

impl RowSizeColumnStats {
    /// Builds one source-shaped statistics record.
    #[must_use]
    pub const fn new(
        kind: RowSizeType,
        total_column_size: i64,
        null_count: i64,
        total_row_count: f64,
        is_handle: bool,
    ) -> Self {
        Self {
            kind,
            is_handle,
            total_column_size,
            null_count,
            total_row_count,
        }
    }
}

/// Table counts retained for column backoff alongside the possibly scaled
/// index-entry counts from Go `HistColl.GetScaledRealtimeAndModifyCnt`.
#[derive(Clone, Copy, Debug)]
pub struct IndexRowCounts {
    /// Rows currently in the table.
    pub table_realtime: i64,
    /// Modified table rows since analysis.
    pub table_modify: i64,
    /// Current index entries (multiple entries per row for MV indexes).
    pub index_realtime: i64,
    /// Modified index entries since analysis.
    pub index_modify: i64,
}

impl IndexRowCounts {
    /// Counts for an ordinary index or an index ineligible for scaling.
    #[must_use]
    pub const fn unscaled(realtime: i64, modify: i64) -> Self {
        Self {
            table_realtime: realtime,
            table_modify: modify,
            index_realtime: realtime,
            index_modify: modify,
        }
    }
}

/// The index metadata used by `canSkipIndexEstimation`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexRangePolicy {
    /// A non-empty partial-index predicate means the index does not cover all
    /// table rows, even when the key range itself is full.
    pub has_condition: bool,
    /// Multi-valued indexes can contain multiple entries per row and therefore
    /// cannot use the realtime-row-count fast path.
    pub is_multi_value: bool,
}
