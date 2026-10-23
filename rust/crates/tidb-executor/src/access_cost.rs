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

//! Statistics and predicate-estimation support shared by executor lowering.
//!
//! Access-path enumeration, skyline pruning, reader costing, and path choice
//! live in `tidb-planner`, matching Go's `DataSource.findBestTask` boundary.
//! This module deliberately contains no second executor-local access planner.

use std::collections::{BTreeMap, BTreeSet};

use tidb_planner::cardinality::row_count_estimator::{ColumnStats, IndexStats};

use crate::plan_trace::PSEUDO_ROW_COUNT;

/// One table's loaded statistics, in the shape the estimator reads them.
///
/// This is `statistics.Table` reduced to the fields
/// [`tidb_planner::cardinality`] actually consumes. It is built once, at the
/// place the catalog is built from the cluster, so the executor never has to
/// know how `mysql.stats_*` is stored.
#[derive(Clone, Debug, Default)]
pub struct TableStatistics {
    /// Go `HistColl.Pseudo`: this table's DISTRIBUTION is the pseudo one, and
    /// `EXPLAIN` says so with `stats:pseudo`. Decided by
    /// [`TableStatistics::new`], which is where the rule and its capture live.
    ///
    /// The row count is a separate field because the two halves move
    /// independently and Go prints them independently: a table with a
    /// `stats_meta` row but no analyzed histogram shows a real `estRows` on
    /// its `TableFullScan` and still says `stats:pseudo`, because the scan's
    /// row count came from `stats_meta` while every selectivity below it came
    /// from the pseudo rates. Collapsing the two would either invent a
    /// distribution or throw away a row count the cluster really knows.
    pub pseudo: bool,
    /// Go's `statistics.Table.HistColl.Pseudo` on the shared cache object.
    /// Unlike [`Self::pseudo`], this does not change when the planner makes a
    /// pseudo copy for an empty or outdated table.
    pub cache_pseudo: bool,
    /// Go `HistColl.RealtimeCount`, from `mysql.stats_meta.count`.
    pub row_count: i64,
    /// Go `HistColl.ModifyCount`, from `mysql.stats_meta.modify_count`.
    pub modify_count: i64,
    /// Go `statistics.Table.Version`: the TSO of the `mysql.stats_meta` row
    /// these statistics were loaded from. Zero only for statistics that never
    /// touched storage (tests, in-process construction).
    pub version: u64,
    /// Go `statistics.Table.LastAnalyzeVersion`, from
    /// `mysql.stats_meta.last_stats_histograms_version`. Zero means the table
    /// has a stats row but was never ANALYZEd, which is what makes
    /// `SHOW STATS_META` report a NULL `Last_analyze_time`.
    pub last_analyze_version: u64,
    /// Go `HistColl.StatsVer`: the statistics FORMAT version of the loaded
    /// objects (1 or 2). It is what `statistics.Table.StatsInfo` publishes as
    /// `StatsInfo.StatsVersion`, and therefore what `EXPLAIN` reads to decide
    /// `stats:pseudo` -- a zero here is `statistics.PseudoVersion`. It is a
    /// different clock from [`Self::version`], the stats_meta TSO.
    pub stats_ver: i64,
    /// Column statistics by column ID.
    pub columns: BTreeMap<i64, ColumnStats>,
    /// Index statistics by index ID.
    pub indexes: BTreeMap<i64, IndexStats>,
    /// Go `Column.FMSketch`, retained for global partition-stat merges.
    pub column_fm_sketches: BTreeMap<i64, tidb_stats::FmSketch>,
    /// Go `Index.FMSketch`, retained for global partition-stat merges.
    pub index_fm_sketches: BTreeMap<i64, tidb_stats::FmSketch>,
    /// Go `Column.StatsLoadedStatus`, retained separately from the estimator's
    /// reduced column shape.
    pub column_load_status: BTreeMap<i64, tidb_stats::StatsLoadedStatus>,
    /// Go `Index.StatsLoadedStatus`, retained separately from the estimator's
    /// reduced index shape.
    pub index_load_status: BTreeMap<i64, tidb_stats::StatsLoadedStatus>,
    /// Go `ColAndIdxExistenceMap`'s column entries. The value says whether
    /// the persisted histogram row is analyzed.
    pub column_stats_existence: BTreeMap<i64, bool>,
    /// Go `ColAndIdxExistenceMap`'s index entries. The value says whether the
    /// persisted histogram row is analyzed.
    pub index_stats_existence: BTreeMap<i64, bool>,
    /// The indexes whose Go `Index.Info.MVIndex` is set: a multi-valued
    /// index's histogram counts array elements, not rows.
    pub mv_index_ids: BTreeSet<i64>,
}

impl TableStatistics {
    /// Go `ColumnStatsIsInvalid`'s estimation gate, separate from its load
    /// request. Evicted NULL-only statistics remain usable when NDV is zero.
    #[cfg(test)]
    pub(crate) fn column_for_estimation(&self, id: i64) -> Option<&ColumnStats> {
        let column = self.columns.get(&id)?;
        let essential = self
            .column_load_status
            .get(&id)
            .is_some_and(|status| status.is_essential_stats_loaded());
        column.is_valid_for_estimation(self.pseudo, essential).then_some(column)
    }

    /// Whether this is Go's synthetic cached `PseudoTable`, as opposed to a
    /// real cached table that the planner copied and marked pseudo because it
    /// is empty or outdated.
    ///
    /// Go's SHOW paths read the cache object and suppress only the former.
    /// An analyzed empty table still has initialized item statuses, so its
    /// zero-NDV histogram rows remain visible even though `GetStatsTable`
    /// returns a separate pseudo table for optimization.
    #[must_use]
    pub fn is_synthetic_pseudo(&self) -> bool {
        self.cache_pseudo
    }

    /// Go `HistColl.GetAnalyzeRowCount`: the row count observed by the first
    /// fully loaded analyzed histogram, or `-1` when the table has none.
    #[must_use]
    pub(crate) fn analyze_row_count(&self) -> f64 {
        for (id, column) in &self.columns {
            if self
                .column_load_status
                .get(id)
                .is_some_and(|status| status.is_full_load())
            {
                return column.total_row_count();
            }
        }
        for (id, index) in &self.indexes {
            if self.mv_index_ids.contains(id) {
                continue;
            }
            if self
                .index_load_status
                .get(id)
                .is_some_and(|status| status.is_full_load())
            {
                return index.total_row_count();
            }
        }
        -1.0
    }

    /// Go `statistics.Table.GetStatsHealthy`.
    #[must_use]
    pub fn stats_healthy(&self) -> (i64, bool) {
        // Go SHOW reads the canonical cache table, not GetStatsTable's
        // planner copy, which can be pseudo even for an analyzed empty table.
        if self.cache_pseudo {
            return (0, false);
        }
        if self.last_analyze_version == 0 {
            return (0, true);
        }
        let analyzed = self.analyze_row_count();
        let count = if analyzed > 0.0 {
            analyzed
        } else {
            self.row_count as f64
        };
        let healthy = if (self.modify_count as f64) < count {
            ((1.0 - self.modify_count as f64 / count) * 100.0) as i64
        } else if self.modify_count == 0 {
            100
        } else {
            0
        };
        (healthy, true)
    }

    /// Go `statistics.Table.IsOutdated`: compare modifications with the
    /// analyzed histogram count, falling back to realtime count when no
    /// histogram is initialized, using the process-wide 0.7 policy value.
    #[must_use]
    pub fn is_outdated(&self) -> bool {
        let analyzed = self.analyze_row_count();
        let row_count = if analyzed < 0.0 {
            self.row_count as f64
        } else {
            analyzed
        };
        row_count > 0.0
            && self.modify_count as f64 / row_count > tidb_stats::RATIO_OF_PSEUDO_ESTIMATE.load()
    }

    /// One table's statistics with [`Self::pseudo`] decided the way Go decides
    /// it, so no caller has to decide it again.
    ///
    /// `pkg/planner/core/stats.GetStatsTable` reaches `PseudoTable` by three
    /// routes, and the two that survive default settings are both here:
    ///
    /// * **`RealtimeCount == 0`** (its step 2). An ANALYZED EMPTY table is
    ///   pseudo -- captured: `CREATE TABLE e(a INT, KEY(a)); ANALYZE TABLE e;
    ///   EXPLAIN SELECT * FROM e WHERE a > 2` prints `stats:pseudo` and
    ///   estimates 3333.33, which is the pseudo row count through the pseudo
    ///   selectivity, NOT the zero the `stats_meta` row states. Go declines to
    ///   plan against a count of zero because an empty table is the state a
    ///   table is least likely to STAY in.
    /// * **Nothing initialized** (its step 3, `!Table.IsInitialized()`): no
    ///   column and no index has an analyzed histogram. The row count can
    ///   still be real, which is why the two halves are separate fields.
    ///
    /// Its third route, `IsOutdated`, is session-dependent and is therefore
    /// applied by the common planner entry instead of this cached object.
    #[must_use]
    pub fn new(
        row_count: i64,
        modify_count: i64,
        columns: BTreeMap<i64, ColumnStats>,
        indexes: BTreeMap<i64, IndexStats>,
    ) -> Self {
        let column_load_status = columns
            .keys()
            .map(|id| (*id, tidb_stats::StatsLoadedStatus::full_load()))
            .collect();
        let index_load_status = indexes
            .keys()
            .map(|id| (*id, tidb_stats::StatsLoadedStatus::full_load()))
            .collect();
        let column_stats_existence = columns.keys().map(|id| (*id, true)).collect();
        let index_stats_existence = indexes.keys().map(|id| (*id, true)).collect();
        let pseudo = row_count == 0 || (columns.is_empty() && indexes.is_empty());
        Self {
            pseudo,
            cache_pseudo: pseudo,
            row_count,
            modify_count,
            version: 0,
            last_analyze_version: 0,
            stats_ver: 0,
            columns,
            indexes,
            column_fm_sketches: BTreeMap::new(),
            index_fm_sketches: BTreeMap::new(),
            column_load_status,
            index_load_status,
            column_stats_existence,
            index_stats_existence,
            mv_index_ids: BTreeSet::new(),
        }
    }

    /// Preserves the per-item load state carried by Go's `Column` and
    /// `Index` objects when storage statistics are translated for planning.
    #[must_use]
    pub fn with_load_statuses(
        mut self,
        columns: BTreeMap<i64, tidb_stats::StatsLoadedStatus>,
        indexes: BTreeMap<i64, tidb_stats::StatsLoadedStatus>,
    ) -> Self {
        self.column_load_status = columns;
        self.index_load_status = indexes;
        self
    }

    /// Preserves Go's system-table existence map independently of the
    /// in-memory column/index objects.
    #[must_use]
    pub fn with_stats_existence(
        mut self,
        columns: BTreeMap<i64, bool>,
        indexes: BTreeMap<i64, bool>,
    ) -> Self {
        self.column_stats_existence = columns;
        self.index_stats_existence = indexes;
        self
    }

    /// Retains per-item FM sketches used when Go merges partition histograms.
    pub fn with_fm_sketches(
        mut self,
        columns: BTreeMap<i64, tidb_stats::FmSketch>,
        indexes: BTreeMap<i64, tidb_stats::FmSketch>,
    ) -> Self {
        self.column_fm_sketches = columns;
        self.index_fm_sketches = indexes;
        self
    }

    /// Go `statistics.Table.IsInitialized`: some column or index object has
    /// initialized statistics.
    #[must_use]
    pub fn is_initialized(&self) -> bool {
        self.columns
            .keys()
            .filter_map(|id| self.column_load_status.get(id))
            .chain(
                self.indexes
                    .keys()
                    .filter_map(|id| self.index_load_status.get(id)),
            )
            .any(|status| status.stats_initialized())
    }

    /// Go `Table.ColumnIsLoadNeeded`.
    #[must_use]
    pub fn column_is_load_needed(&self, id: i64, full_load: bool) -> bool {
        // Go checks the canonical cache table, not the planner's temporary
        // pseudo estimate while analyzed histograms have not been loaded.
        if self.cache_pseudo {
            return false;
        }
        let Some(analyzed) = self.column_stats_existence.get(&id).copied() else {
            return false;
        };
        let Some(status) = self.column_load_status.get(&id).copied() else {
            return true;
        };
        if !analyzed {
            return false;
        }
        if full_load {
            !status.is_full_load()
        } else {
            !status.stats_initialized()
        }
    }

    /// Go `Table.IndexIsLoadNeeded`.
    #[must_use]
    pub fn index_is_load_needed(&self, id: i64) -> bool {
        let analyzed = self
            .index_stats_existence
            .get(&id)
            .copied()
            .unwrap_or(false);
        match self.index_load_status.get(&id).copied() {
            None => analyzed,
            Some(status) => analyzed && !status.is_full_load(),
        }
    }

    /// Stamps the two TSOs the loaded `mysql.stats_meta` row carries, which
    /// are exactly the two clocks `SHOW STATS_META` renders as `Update_time`
    /// and `Last_analyze_time`. The cluster bridge is the one caller that has
    /// them, because it is the one place statistics meet their stored row.
    #[must_use]
    pub fn with_stat_versions(mut self, version: u64, last_analyze_version: u64) -> Self {
        self.version = version;
        self.last_analyze_version = last_analyze_version;
        self
    }

    /// Stamps Go `HistColl.StatsVer`, the stats-format version of the loaded
    /// objects. It becomes `StatsInfo.StatsVersion`.
    #[must_use]
    pub const fn with_stats_ver(mut self, stats_ver: i64) -> Self {
        self.stats_ver = stats_ver;
        self
    }

    /// Go `cardinality.EstimateColumnNDV` and its `getTotalRowCount` helper.
    ///
    /// Lite statistics initialization keeps every histogram's NDV metadata but
    /// loads buckets and TopN only for the items the query needs. An evicted
    /// column therefore borrows the analyzed row count from the first fully
    /// loaded, same-version index, then from a column. The Rust catalog keeps
    /// the payloads eagerly, so callers pass the items Go's predicate-statistics
    /// collection would have marked fully loaded for this query.
    pub(crate) fn estimate_column_ndv(
        &self,
        column_id: i64,
        full_loaded_columns: &BTreeSet<i64>,
        full_loaded_indexes: &BTreeSet<i64>,
    ) -> Option<f64> {
        let column = self.columns.get(&column_id)?;
        // Go reads the histogram's NDV only when `IsStatsInitialized()`; an
        // `EmptyColumn` placeholder -- a never-analyzed column a load request
        // published -- takes the `RealtimeCount * distinctFactor` branch.
        if !self
            .column_load_status
            .get(&column_id)
            .is_some_and(|status| status.stats_initialized())
        {
            return None;
        }
        let version = column.histogram.last_update_version;
        let analyzed_count = if full_loaded_columns.contains(&column_id) {
            column.total_row_count() as i64
        } else {
            full_loaded_indexes
                .iter()
                .find_map(|id| {
                    self.indexes.get(id).and_then(|index| {
                        (index.histogram.last_update_version == version)
                            .then(|| index.total_row_count() as i64)
                    })
                })
                .unwrap_or_else(|| {
                    full_loaded_columns
                        .iter()
                        .filter(|id| **id != column_id)
                        .find_map(|id| {
                            self.columns.get(id).and_then(|candidate| {
                                let same_version =
                                    candidate.histogram.last_update_version == version;
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
            ndv *= self.row_count as f64 / analyzed_count as f64;
        }
        Some(ndv)
    }
}

impl tidb_planner::logical::init_stats::TableStatsSnapshot for TableStatistics {
    fn pseudo(&self) -> bool {
        self.pseudo
    }
    fn row_count(&self) -> i64 {
        self.row_count
    }
    fn modify_count(&self) -> i64 {
        self.modify_count
    }
    fn version(&self) -> u64 {
        self.version
    }
    fn stats_ver(&self) -> i64 {
        self.stats_ver
    }
    fn columns(&self) -> &BTreeMap<i64, ColumnStats> {
        &self.columns
    }
    fn indexes(&self) -> &BTreeMap<i64, IndexStats> {
        &self.indexes
    }
    fn column_load_status(&self) -> &BTreeMap<i64, tidb_stats::StatsLoadedStatus> {
        &self.column_load_status
    }
    fn index_load_status(&self) -> &BTreeMap<i64, tidb_stats::StatsLoadedStatus> {
        &self.index_load_status
    }
    fn index_stats_existence(&self) -> &BTreeMap<i64, bool> {
        &self.index_stats_existence
    }
    fn analyze_row_count(&self) -> f64 {
        TableStatistics::analyze_row_count(self)
    }
    fn estimate_column_ndv(
        &self,
        column_id: i64,
        full_loaded_columns: &BTreeSet<i64>,
        full_loaded_indexes: &BTreeSet<i64>,
    ) -> Option<f64> {
        TableStatistics::estimate_column_ndv(self, column_id, full_loaded_columns, full_loaded_indexes)
    }
}

/// Go `pkg/planner/core/stats.GetStatsTable`: the statistics one statement
/// plans `physical_table_id` from. `None` is a fresh `statistics.PseudoTable`
/// (`RealtimeCount` 10000, `ModifyCount` 0, uninitialized pseudo items).
///
/// The caller passes the id Go's `pid == tblInfo.ID ||
/// UseDynamicPartitionPrune()` choice resolves to: the logical id for an
/// ordinary or dynamic-pruning source, one partition's id for a
/// static-pruning child. Rule 1 (no statistics handle yet) has no
/// counterpart, since the catalog always answers; a table the cache does not
/// hold is the same `PseudoTable`.
///
/// The cached object is never mutated: the determinate-objective and pseudo
/// decisions are session-local, so they land on a copy as Go's `CopyAs` and
/// `tbl := *statsTbl` do.
#[must_use]
pub fn get_stats_table(
    ctx: &crate::StmtContext,
    catalog: &crate::driver::Catalog,
    physical_table_id: i64,
) -> Option<std::sync::Arc<TableStatistics>> {
    let opt_objective_determinate = !ctx
        .optimizer_cost_env()
        .session
        .estimator_options
        .allow_use_modify_count;
    stats_table_for_session(
        catalog.table_statistics(physical_table_id)?,
        opt_objective_determinate,
        ctx.enable_pseudo_for_outdated_stats(),
    )
}

/// The session rules of [`get_stats_table`] over one cached table.
fn stats_table_for_session(
    mut statistics: std::sync::Arc<TableStatistics>,
    opt_objective_determinate: bool,
    enable_pseudo_for_outdated_stats: bool,
) -> Option<std::sync::Arc<TableStatistics>> {
    // `OptObjectiveDeterminate` ignores the real-time statistics: the copy's
    // `RealtimeCount` is the row count the loaded histograms observed at
    // ANALYZE time, and its `ModifyCount` is zero.
    if opt_objective_determinate {
        let analyze_count = statistics.analyze_row_count().max(0.0) as i64;
        if statistics.row_count != analyze_count || statistics.modify_count != 0 {
            let copied = std::sync::Arc::make_mut(&mut statistics);
            copied.row_count = analyze_count;
            copied.modify_count = 0;
            // The cached `pseudo` folds rule 2 in for the cached count; the
            // copy's count is a different one, so only the cached
            // `HistColl.Pseudo` and rule 3's `!IsInitialized()` carry over.
            copied.pseudo = copied.cache_pseudo || !copied.is_initialized();
        }
    }
    // Rule 2: a zero `RealtimeCount` plans from a fresh `PseudoTable`; none of
    // the cached objects survive.
    if statistics.row_count == 0 {
        return None;
    }
    // Rule 3: an uninitialized table is already `pseudo` in the cache
    // (`TableStatistics::new` and the loaders decide it). Outdatedness is
    // judged on the table this statement plans from, so the determinate
    // copy's zero `ModifyCount` is never outdated.
    if enable_pseudo_for_outdated_stats && !statistics.pseudo && statistics.is_outdated() {
        std::sync::Arc::make_mut(&mut statistics).pseudo = true;
    }
    Some(statistics)
}

/// Go `getLatestVersionFromStatsTable` (`logical_plan_builder.go`), the
/// plan-cache key's statistics version for one table: zero for any table
/// `GetStatsTable` answers with a pseudo table by rule 2, otherwise the
/// largest `LastUpdateVersion` among the cached column and index objects.
///
/// It is NOT the `stats_meta` version: a stats-delta flush moves that, and Go
/// keeps the cached plan across it.
#[must_use]
pub fn get_latest_version_from_stats_table(
    catalog: &crate::driver::Catalog,
    table_id: i64,
    opt_objective_determinate: bool,
) -> u64 {
    let Some(statistics) = catalog.table_statistics(table_id) else {
        return 0;
    };
    let realtime_row_count = if opt_objective_determinate {
        statistics.analyze_row_count().max(0.0) as i64
    } else {
        statistics.row_count
    };
    if realtime_row_count == 0 {
        return 0;
    }
    statistics
        .columns
        .values()
        .map(|column| column.histogram.last_update_version)
        .chain(
            statistics
                .indexes
                .values()
                .map(|index| index.histogram.last_update_version),
        )
        .max()
        .unwrap_or(0)
}

/// The estimate one committed access path carries into `EXPLAIN`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScanEstimate {
    /// Go's `estRows`: the rows the READ itself produces, before any filter
    /// above it.
    pub rows: f64,
    /// Whether these numbers came from `statistics.PseudoTable` rather than
    /// from loaded statistics -- what makes `EXPLAIN` print `stats:pseudo`.
    pub pseudo: bool,
}

/// The table's realtime row count: `stats_meta.count` when analyzed, Go's
/// `statistics.PseudoRowCount` when not.
pub(crate) fn realtime_row_count(stats: Option<&TableStatistics>) -> f64 {
    match stats {
        Some(stats) if stats.row_count > 0 => stats.row_count as f64,
        // A count of zero on a table with no histograms is not the statement
        // "this table is empty" -- it is `mysql.stats_meta` not having caught
        // up with the rows yet, and Go answers it with
        // `PseudoHistColl`'s `RealtimeCount: PseudoRowCount`
        // (`pkg/statistics/table.go`). Taking the zero literally makes every
        // path cost nothing and the choice arbitrary; a live playground shows
        // exactly that on a freshly loaded table, where Go still says 10000.
        //
        // An ANALYZED table that really is empty lands here too, because a
        // zero count is one of Go's own routes to `PseudoTable` -- see
        // [`TableStatistics::new`], where that rule lives, and the capture
        // that pins it.
        Some(stats) if stats.pseudo => PSEUDO_ROW_COUNT,
        Some(stats) => stats.row_count.max(0) as f64,
        None => PSEUDO_ROW_COUNT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::Datum;


    fn histogram_with_count(id: i64, ndv: i64, count: i64, version: u64) -> tidb_stats::Histogram {
        tidb_stats::Histogram {
            id,
            ndv,
            last_update_version: version,
            buckets: vec![tidb_stats::Bucket {
                count,
                repeat: 1,
                ndv,
                lower_bound: Datum::Int(1),
                upper_bound: Datum::Int(ndv),
            }],
            ..tidb_stats::Histogram::default()
        }
    }

    #[test]
    fn initialized_empty_stats_are_not_a_synthetic_cache_pseudo() {
        let synthetic = TableStatistics::new(0, 0, BTreeMap::new(), BTreeMap::new());
        assert!(synthetic.is_synthetic_pseudo());
        assert_eq!(synthetic.stats_healthy(), (0, false));

        let mut analyzed_empty = synthetic;
        analyzed_empty.cache_pseudo = false;
        analyzed_empty
            .index_load_status
            .insert(1, tidb_stats::StatsLoadedStatus::full_load());
        assert!(analyzed_empty.pseudo);
        assert!(!analyzed_empty.is_synthetic_pseudo());
        assert_eq!(analyzed_empty.stats_healthy(), (0, true));
        analyzed_empty.last_analyze_version = 42;
        assert_eq!(analyzed_empty.stats_healthy(), (100, true));
    }

    /// Go `EstimateColumnNDV` reads the histogram NDV only when the column
    /// `IsStatsInitialized()`. A never-analyzed column a load request
    /// published is Go's `EmptyColumn` (zero NDV, zero load status), and the
    /// estimate falls back to `RealtimeCount * distinctFactor`.
    ///
    /// Captured live: on a never-analyzed table with `stats_meta` count 8,
    /// Go's `GROUP BY k` estimates 6.40 groups where the NDV of the
    /// placeholder made this engine estimate 1.00.
    #[test]
    fn estimate_column_ndv_skips_an_uninitialized_column() {
        let column = ColumnStats {
            histogram: tidb_stats::Histogram {
                id: 3,
                ..tidb_stats::Histogram::default()
            },
            topn: None,
            cms: None,
            stats_ver: 0,
            unsigned: false,
        };
        let mut stats = TableStatistics::new(8, 8, BTreeMap::from([(3, column)]), BTreeMap::new());
        stats
            .column_load_status
            .insert(3, tidb_stats::StatsLoadedStatus::default());
        assert_eq!(
            stats.estimate_column_ndv(3, &BTreeSet::new(), &BTreeSet::new()),
            None
        );
        stats
            .column_load_status
            .insert(3, tidb_stats::StatsLoadedStatus::full_load());
        assert_eq!(
            stats.estimate_column_ndv(3, &BTreeSet::new(), &BTreeSet::new()),
            Some(0.0)
        );
    }

    /// Go `HistColl.GetAnalyzeRowCount` skips a multi-valued index, whose
    /// histogram counts array elements rather than rows.
    #[test]
    fn analyze_row_count_skips_multi_valued_indexes() {
        let index = |id, count| IndexStats {
            histogram: histogram_with_count(id, 1, count, 42),
            topn: None,
            cms: None,
            stats_ver: 2,
            num_columns: 1,
            unique: false,
        };
        let mut statistics = TableStatistics::new(
            4,
            0,
            BTreeMap::new(),
            [(1, index(1, 10)), (2, index(2, 4))].into_iter().collect(),
        );
        assert_eq!(statistics.analyze_row_count(), 10.0);
        statistics.mv_index_ids.insert(1);
        assert_eq!(statistics.analyze_row_count(), 4.0);
    }

    #[test]
    fn estimate_column_ndv_borrows_a_same_version_full_load_count() {
        let target_id = 1;
        let predicate_id = 2;
        let index_id = 3;
        let version = 42;
        let column = |id, ndv, count| ColumnStats {
            histogram: histogram_with_count(id, ndv, count, version),
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        let statistics = TableStatistics::new(
            300_000,
            0,
            [
                (target_id, column(target_id, 3_000, 300_000)),
                (predicate_id, column(predicate_id, 10, 299_995)),
            ]
            .into_iter()
            .collect(),
            [(
                index_id,
                IndexStats {
                    histogram: histogram_with_count(index_id, 10, 299_995, version),
                    topn: None,
                    cms: None,
                    stats_ver: 2,
                    num_columns: 1,
                    unique: false,
                },
            )]
            .into_iter()
            .collect(),
        );
        let full_columns = [predicate_id].into_iter().collect();
        let full_indexes = [index_id].into_iter().collect();
        let ndv = statistics
            .estimate_column_ndv(target_id, &full_columns, &full_indexes)
            .unwrap();
        assert!((ndv - 3000.0500008333474).abs() < 1e-12, "{ndv:.17}");

        let target_full = [target_id].into_iter().collect();
        assert_eq!(
            statistics.estimate_column_ndv(target_id, &target_full, &BTreeSet::new()),
            Some(3000.0)
        );
    }

}
