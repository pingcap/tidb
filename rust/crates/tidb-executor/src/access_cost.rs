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

use tidb_datatype::Datum;
use tidb_planner::cardinality::pseudo::{
    IndexRange as PseudoIndexRange, PseudoBoundKind, ScalarRange, pseudo_row_count_by_index_ranges,
};
use tidb_planner::cardinality::row_count_column::RowEstimate;
use tidb_planner::cardinality::row_count_estimator::{
    ColumnStats, EstimatorOptions, IndexRangeDatums, IndexRowCounts, IndexStats,
    RecursiveIndexStats, IndexEstimationStats, get_index_row_count,
};

use crate::kv_table::{IndexRange, KvIndex, KvTable};
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

fn queue_column_stats_load_if_invalid(
    table: &KvTable,
    stats: &TableStatistics,
    column_id: i64,
    column_stats: Option<&ColumnStats>,
) {
    if column_id <= 0 {
        return;
    }
    // Go: `colStats == nil || !colStats.IsStatsInitialized() ||
    // colStats.IsLoadNeeded()`. The reduced column shape makes "missing"
    // stand in for both nil and uninitialized.
    let unloaded = column_stats.is_none() || stats.column_is_load_needed(column_id, true);
    if unloaded {
        tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.insert(
            tidb_model::TableItemID {
                table_id: table.table_id,
                id: column_id,
                is_index: false,
                is_sync_load_failed: false,
            },
            true,
        );
    }
}

/// Go scales only fully loaded MV indexes. Their own entry counts must not
/// be mistaken for analyzed table rows when selecting the scaling baseline.
fn index_row_counts(stats: &TableStatistics, table: &KvTable, index: &KvIndex) -> IndexRowCounts {
    let mut counts = IndexRowCounts::unscaled(stats.row_count, stats.modify_count);
    if table.mv_key_part_source(index.id).is_none()
        || !stats
            .index_load_status
            .get(&index.id)
            .is_some_and(|status| status.is_full_load())
    {
        return counts;
    }
    let analyzed = stats
        .columns
        .iter()
        .find_map(|(id, column)| {
            stats
                .column_load_status
                .get(id)
                .is_some_and(|status| status.is_full_load())
                .then(|| column.total_row_count())
        })
        .or_else(|| {
            stats.indexes.iter().find_map(|(id, index)| {
                (table.mv_key_part_source(*id).is_none()
                    && stats
                        .index_load_status
                        .get(id)
                        .is_some_and(|status| status.is_full_load()))
                .then(|| index.total_row_count())
            })
        })
        .unwrap_or(-1.0);
    let total = stats
        .indexes
        .get(&index.id)
        .map_or(0.0, IndexStats::total_row_count);
    if analyzed > 0.0 && total > 0.0 {
        let scale = total / analyzed;
        counts.index_realtime = (stats.row_count as f64 * scale) as i64;
        counts.index_modify = (stats.modify_count as f64 * scale) as i64;
    }
    counts
}

#[cfg(test)]
fn index_row_count(
    index: &KvIndex,
    table: &KvTable,
    ranges: &[IndexRange],
    stats: Option<&TableStatistics>,
    realtime: f64,
    trigger_load: bool,
) -> Result<RowEstimate, tidb_planner::cardinality::row_count_estimator::EstimationError> {
    index_row_count_with_options(
        index,
        table,
        ranges,
        stats,
        realtime,
        trigger_load,
        EstimatorOptions::default(),
    )
}

pub(crate) fn index_row_count_with_options(
    index: &KvIndex,
    table: &KvTable,
    ranges: &[IndexRange],
    stats: Option<&TableStatistics>,
    realtime: f64,
    trigger_load: bool,
    options: EstimatorOptions,
) -> Result<RowEstimate, tidb_planner::cardinality::row_count_estimator::EstimationError> {
    let Some(stats) = stats.filter(|stats| !stats.pseudo) else {
        return Ok(RowEstimate::default_est(pseudo_index_row_count(
            index, ranges, realtime,
        )));
    };
    // Go `GetRowCountByIndexRanges` checks this before `IndexStatsIsInvalid`:
    // a full range over an ordinary index includes every table row, so the
    // table's realtime count is exact and an evicted index histogram must not
    // be queued just to rediscover it.
    if can_skip_index_estimation(index, table, ranges) {
        return Ok(RowEstimate::default_est(stats.row_count as f64));
    }
    let index_column_offsets = index_column_offsets_for_ranges(index, table, ranges);
    // Go `IndexStatsIsInvalid` queues incomplete statistics for loading, but
    // rejects estimation only when the object is missing or TotalRowCount is
    // zero. A load-status flag alone does not make a retained payload invalid.
    //
    // `trigger_load` is false only for the executor's eager precompute, which
    // runs before `CollectPredicateColumnsPoint` has pruned the paths; that
    // rule owns the demand so a pruned index is never queued.
    if trigger_load && stats.index_is_load_needed(index.id) {
        tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.insert(
            tidb_model::TableItemID {
                table_id: table.table_id,
                id: index.id,
                is_index: true,
                is_sync_load_failed: false,
            },
            true,
        );
    }
    let Some(index_stats) = stats
        .indexes
        .get(&index.id)
        .filter(|index| index.total_row_count() != 0.0)
    else {
        // A table WITH statistics whose index has no usable payload: Go's
        // `GetRowCountByIndexRanges` (`row_count_index.go:57`) first tries
        // the index columns' OWN histograms
        // (`getPseudoRowCountWithPartialStats`) and only a path with no
        // column statistics either falls to the pseudo rate. The clustered
        // PRIMARY this crate synthesizes ([`crate::handle_range::
        // clustered_primary_metadata`]) always lands here on an analyzed
        // table: its key columns are analyzed, its index id is stored
        // nowhere.
        if let Some(estimate) = partial_stats_index_row_count(
            table,
            &index_column_offsets,
            ranges,
            stats,
            realtime,
            options,
        )? {
            return Ok(estimate);
        }
        return Ok(RowEstimate::default_est(pseudo_index_row_count(
            index, ranges, realtime,
        )));
    };
    let datum_ranges: Vec<IndexRangeDatums> = ranges
        .iter()
        .map(|range| IndexRangeDatums {
            collators: index_column_offsets
                .iter()
                .take(range.low.len())
                .map(|offset| table.columns[*offset].field_type.collation())
                .collect(),
            low_val: range.low.clone(),
            high_val: range.high.clone(),
            low_exclude: range.low_exclusive,
            high_exclude: range.high_exclusive,
        })
        .collect();
    // The per-column histograms `expBackoffEstimation` consults, positionally
    // by physical index key column -- Go's `HistColl.ColUniqueID2IdxIDs` walk.
    // A non-unique secondary key can include the clustered handle after its
    // declared columns; those dimensions participate when the range contains
    // them, even though the index histogram itself covers only the declaration.
    let context = index_estimation_stats(table, stats, index);
    let column_stats = &context.columns;
    let virtual_columns = index_column_offsets
        .iter()
        .map(|offset| {
            table
                .columns
                .get(*offset)
                .is_some_and(crate::generated_column::is_virtual)
        })
        .collect::<Vec<_>>();
    let has_multicolumn_range = ranges.iter().any(|range| range.low.len() > 1);
    let recursive_indexes: Vec<Vec<RecursiveIndexStats<'_>>> = index_column_offsets
        .iter()
        .enumerate()
        .map(|(position, offset)| {
            // V2 consults alternatives only after column statistics fail;
            // V1 prefers the first suffix index even with usable columns.
            if position >= column_stats.len()
                || !has_multicolumn_range
                || (column_stats[position].is_some()
                    && !(index_stats.stats_ver == 1 && index_stats.cms.is_some()))
            {
                return Vec::new();
            }
            let mut candidates = table
                .indexes()
                .iter()
                .filter_map(|candidate| {
                    // HistColl.ColUniqueID2IdxIDs maps only leading columns:
                    // one-dimensional bounds cannot query a suffix histogram.
                    if candidate.column_offsets.first() != Some(offset) {
                        return None;
                    }
                    stats.indexes.get(&candidate.id)?;
                    Some((
                        candidate.id,
                        index_estimation_stats(table, stats, candidate),
                    ))
                })
                .collect::<Vec<_>>();
            candidates.sort_by_key(|(id, _)| *id);
            candidates
                .into_iter()
                .map(|(_, candidate)| candidate)
                .collect()
        })
        .collect();
    get_index_row_count(
        &context,
        &virtual_columns,
        &recursive_indexes,
        &datum_ranges,
        &options,
    )
}

fn index_estimation_stats<'a>(
    table: &KvTable,
    stats: &'a TableStatistics,
    index: &KvIndex,
) -> IndexEstimationStats<'a> {
    // Go fillIndexPath extends Idx2ColUniqueIDs with eligible handle columns
    // before logical selectivity or cloned merge paths consult the collection.
    let mut mapped_offsets = index.column_offsets.clone();
    mapped_offsets.extend(appended_handle_offsets_for_index(index, table));
    let columns = mapped_offsets.iter()
        .map(|offset| {
            table
                .columns
                .get(*offset)
                .and_then(|column| stats.column_for_estimation(column.id))
        })
        .collect();
    let mut context = IndexEstimationStats::new(
        stats.indexes.get(&index.id),
        columns,
        index_row_counts(stats, table, index),
    );
    if context
        .index
        .is_some_and(|index| index.stats_ver == 1 && index.cms.is_some())
    {
        context.column_ndvs = mapped_offsets.iter()
            .map(|offset| {
                table
                    .columns
                    .get(*offset)
                    .and_then(|column| stats.columns.get(&column.id))
                    .map(|column| column.histogram.ndv)
            })
            .collect();
        context.column_is_handle = mapped_offsets.iter()
            .map(|offset| table.pk_handle_offset() == Some(*offset))
            .collect();
    }
    context.policy = tidb_planner::cardinality::index_range_policy::IndexRangePolicy {
        has_condition: table
            .partial_index_condition_string(index.id)
            .is_some_and(|condition| !condition.is_empty()),
        is_multi_value: table.mv_key_part_source(index.id).is_some(),
    };
    context
}

/// Go `canSkipIndexEstimation` at the live statistics boundary.
fn can_skip_index_estimation(index: &KvIndex, table: &KvTable, ranges: &[IndexRange]) -> bool {
    use tidb_planner::cardinality::index_range_policy::{
        can_skip_datum_index_estimation, IndexRangePolicy,
    };

    let policy = IndexRangePolicy {
        has_condition: table
            .partial_index_condition_string(index.id)
            .is_some_and(|condition| !condition.is_empty()),
        is_multi_value: table.mv_key_part_source(index.id).is_some(),
    };
    can_skip_datum_index_estimation(
        policy,
        ranges.iter().map(|range| {
            (
                range.low.as_slice(),
                range.high.as_slice(),
                range.low_exclusive,
                range.high_exclusive,
            )
        }),
    )
}

/// Go `fillIndexPath`'s suffix (`stats.go:177-197`) as table offsets: the
/// signed integer handle after a complete non-unique index's declared key
/// parts. A clustered common handle is never appended.
fn appended_handle_offsets_for_index(index: &KvIndex, table: &KvTable) -> Vec<usize> {
    if index.unique || index.clustered_primary {
        return Vec::new();
    }
    if index
        .column_offsets
        .iter()
        .any(|offset| table.columns.get(*offset).is_none())
    {
        return Vec::new();
    }
    let Some(handle_offset) = table.pk_handle_offset() else {
        return Vec::new();
    };
    let Some(handle) = table.columns.get(handle_offset) else {
        return Vec::new();
    };
    if handle.field_type.is_unsigned() || index.column_offsets.contains(&handle_offset) {
        return Vec::new();
    }
    vec![handle_offset]
}

/// Go `pruneEstimateRange` (`core/stats.go:450-469`): each range cut to its
/// first `keep_columns` columns, its exclusivity kept and no union taken, the
/// ranges `detachCondAndBuildRangeForPath` estimates once an appended handle
/// follows the declared index columns.
pub(crate) fn prune_estimate_ranges(ranges: &[IndexRange], keep_columns: usize) -> Vec<IndexRange> {
    ranges
        .iter()
        .map(|range| IndexRange {
            low: range.low[..keep_columns.min(range.low.len())].to_vec(),
            high: range.high[..keep_columns.min(range.low.len())].to_vec(),
            low_exclusive: range.low_exclusive,
            high_exclusive: range.high_exclusive,
        })
        .collect()
}

/// Index key columns represented by these ranges: declared columns, followed
/// by as many eligible handle dimensions as the range set contains.
fn index_column_offsets_for_ranges(
    index: &KvIndex,
    table: &KvTable,
    ranges: &[IndexRange],
) -> Vec<usize> {
    let declared_count = index.column_offsets.len();
    let range_width = ranges
        .iter()
        .map(|range| range.low.len().max(range.high.len()))
        .max()
        .unwrap_or(0);
    let mut offsets = index.column_offsets.clone();
    if range_width <= declared_count {
        return offsets;
    }
    let appended = appended_handle_offsets_for_index(index, table);
    let appended_count = range_width - declared_count;
    if appended_count <= appended.len() {
        offsets.extend(appended.into_iter().take(appended_count));
    }
    offsets
}

/// The stats-less index estimate: Go `getPseudoRowCountByIndexRanges`
/// (`pkg/planner/cardinality/pseudo.go`), through its port.
///
/// `colsLen` there is the index's column count for a UNIQUE index and `-1`
/// otherwise, which is what decides whether a full-length point range is
/// worth exactly one row; that is the `unique_columns` argument.
/// Go `getPseudoRowCountWithPartialStats` (`pkg/planner/cardinality/
/// row_count_column.go:254`): the index has no statistics, but its columns
/// do, so each range position is estimated as a single-column range and the
/// per-column selectivities multiply (independence), with the CORRELATED
/// floor -- the minimum single-column selectivity -- carried as the maximum
/// estimate.
///
/// `None` reproduces the two gates in `GetRowCountByIndexRanges`
/// (`row_count_index.go:58`): no index column has statistics
/// (`hasColumnStats`), or some range is the full one
/// (`ranger.HasFullRange(indexRanges, false)`); both fall to the pseudo rate.
fn partial_stats_index_row_count(
    table: &KvTable,
    index_column_offsets: &[usize],
    ranges: &[IndexRange],
    stats: &TableStatistics,
    realtime: f64,
    options: EstimatorOptions,
) -> Result<Option<RowEstimate>, tidb_planner::cardinality::row_count_estimator::EstimationError> {
    if realtime <= 0.0 {
        return Ok(Some(RowEstimate::default_est(0.0)));
    }
    let columns: Vec<(Option<&ColumnStats>, tidb_datatype::Collation)> = index_column_offsets
        .iter()
        .map(|offset| {
            let column = table.columns.get(*offset);
            (
                column.and_then(|column| stats.column_for_estimation(column.id)),
                column.map_or(tidb_datatype::Collation::Binary, |column| {
                    column.field_type.collation()
                }),
            )
        })
        .collect();
    if index_column_offsets.iter().all(|offset| {
        table
            .columns
            .get(*offset)
            .is_none_or(|column| !stats.columns.contains_key(&column.id))
    }) {
        return Ok(None);
    }
    // Go `ColumnStatsIsInvalid`: every indexed column that is missing or not
    // fully loaded is queued for the async loader while the estimate proceeds.
    for (offset, (column_stats, _)) in columns.iter().enumerate() {
        let column_id = table
            .columns
            .get(index_column_offsets[offset])
            .map(|column| column.id)
            .unwrap_or(0);
        queue_column_stats_load_if_invalid(table, stats, column_id, *column_stats);
    }
    if ranges.iter().any(|range| range.is_full_range(false)) {
        return Ok(None);
    }
    let ranges = ranges
        .iter()
        .map(|range| IndexRangeDatums {
            collators: columns
                .iter()
                .take(range.low.len())
                .map(|(_, collation)| *collation)
                .collect(),
            low_val: range.low.clone(),
            high_val: range.high.clone(),
            low_exclude: range.low_exclusive,
            high_exclude: range.high_exclusive,
        })
        .collect::<Vec<_>>();
    tidb_planner::cardinality::row_count_estimator::get_index_row_count_with_partial_stats(
        &columns,
        &ranges,
        stats.row_count,
        stats.modify_count,
        &options,
    )
}

fn pseudo_index_row_count(index: &KvIndex, ranges: &[IndexRange], realtime: f64) -> f64 {
    let pseudo_ranges: Vec<PseudoIndexRange> = ranges
        .iter()
        .map(|range| {
            let width = range.low.len().max(range.high.len());
            let columns: Vec<ScalarRange> = (0..width)
                .map(|position| scalar_range(range.low.get(position), range.high.get(position)))
                .collect();
            PseudoIndexRange::new(
                columns,
                equal_prefix_len(range),
                range.low_exclusive,
                range.high_exclusive,
            )
        })
        .collect();
    pseudo_row_count_by_index_ranges(
        &pseudo_ranges,
        realtime,
        index.unique.then_some(index.column_offsets.len()),
    )
}

/// Go `ranger.Range.PrefixEqualLen`: how many leading columns the range pins
/// to one concrete, non-NULL value.
fn equal_prefix_len(range: &IndexRange) -> usize {
    range
        .low
        .iter()
        .zip(&range.high)
        .take_while(|(low, high)| low == high && **low != Datum::Null)
        .count()
}

/// One index column's bounds as the pseudo formula reads them.
///
/// The formula only ever compares the two bounds for equality and classifies
/// the infinities, so the numeric values exist solely to answer
/// "low == high"; a missing bound is the corresponding infinity, which is how
/// a range that pins a PREFIX of the index is spelled.
fn scalar_range(low: Option<&Datum>, high: Option<&Datum>) -> ScalarRange {
    let (low_value, low_kind) = pseudo_bound(low, PseudoBoundKind::MinNotNull);
    let (high_value, high_kind) = pseudo_bound(high, PseudoBoundKind::MaxValue);
    ScalarRange::new(low_value, high_value, low_kind, high_kind)
}

/// A bound as a `(numeric, kind)` pair; `absent` is the infinity that side
/// takes when the range does not reach this column.
fn pseudo_bound(bound: Option<&Datum>, absent: PseudoBoundKind) -> (f64, PseudoBoundKind) {
    match bound {
        None => (
            if absent == PseudoBoundKind::MaxValue {
                f64::INFINITY
            } else {
                f64::NEG_INFINITY
            },
            absent,
        ),
        Some(Datum::MinNotNull) => (f64::NEG_INFINITY, PseudoBoundKind::MinNotNull),
        Some(Datum::MaxValue) => (f64::INFINITY, PseudoBoundKind::MaxValue),
        Some(Datum::Null) => (0.0, PseudoBoundKind::Null),
        // The value itself is only ever tested for equality against the other
        // bound, and two bounds are equal exactly when their datums are, so a
        // stable per-datum number is all the formula needs.
        Some(value) => (datum_ordinal(value), PseudoBoundKind::Value),
    }
}

/// A stable number for one datum, for the equality test above only.
fn datum_ordinal(value: &Datum) -> f64 {
    use std::hash::{Hash, Hasher};
    match value {
        Datum::Int(v) => *v as f64,
        Datum::UInt(v) => *v as f64,
        Datum::Real(v) => *v,
        other => {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            format!("{other:?}").hash(&mut hasher);
            hasher.finish() as f64
        }
    }
}

// Sized forwarding adapter for the expression rewriter's generic API.

// Adapt Go MergeDNFItems4Col to the AST retained by this estimator. Use
// the native checker, while keeping original ASTs for recursive estimation.
#[cfg(test)]
mod tests {
    use super::*;


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

#[cfg(test)]
mod index_async_load_queue_tests {
    use super::*;
    use crate::kv_table::KvColumn;
    use crate::kv_table::KvTable;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_model::TableItemID;
    use tidb_stats::Histogram;


    fn queue_test_table(columns: Vec<KvColumn>) -> KvTable {
        use std::sync::atomic::{AtomicI64, Ordering};
        // The loader queue is process-global and keyed by physical table ID.
        static NEXT_TABLE_ID: AtomicI64 = AtomicI64::new(1 << 60);
        KvTable::new(NEXT_TABLE_ID.fetch_add(1, Ordering::Relaxed), columns)
    }

    #[test]
    fn cleaning_loaded_fixture_preserves_other_fixture_pending_load() {
        let unloaded_table = queue_test_table(vec![long_column("a", 1)]);
        let loaded_table = queue_test_table(vec![long_column("a", 1)]);
        let mut stats = TableStatistics::new(10, 0, BTreeMap::new(), BTreeMap::new());
        stats.pseudo = false;
        stats.cache_pseudo = false;
        stats.column_stats_existence.insert(1, true);
        let unloaded_item = TableItemID {
            table_id: unloaded_table.table_id,
            id: 1,
            is_index: false,
            is_sync_load_failed: false,
        };
        let loaded_item = TableItemID {
            table_id: loaded_table.table_id,
            ..unloaded_item
        };
        let needed = &tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
        queue_column_stats_load_if_invalid(&unloaded_table, &stats, 1, None);
        // Replay the cleanup of a second fixture between enqueue and assertion.
        needed.delete(loaded_item);
        stats
            .column_load_status
            .insert(1, tidb_stats::StatsLoadedStatus::full_load());
        let loaded = ColumnStats {
            histogram: Histogram::default(),
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        queue_column_stats_load_if_invalid(&loaded_table, &stats, 1, Some(&loaded));
        let items = needed.all_items();
        needed.delete(unloaded_item);
        needed.delete(loaded_item);
        assert!(items.iter().any(|item| item.table_item_id == unloaded_item));
        assert!(!items.iter().any(|item| item.table_item_id == loaded_item));
    }

    fn long_column(name: &str, id: i64) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        }
    }

    #[test]
    fn recursive_index_statistics_only_estimate_the_leading_column() {
        let mut table = queue_test_table(vec![
            long_column("a", 1),
            long_column("b", 2),
            long_column("c", 3),
        ]);
        let index = |id, offsets| KvIndex {
            id,
            name: format!("idx_{id}"),
            comment: String::new(),
            unique: false,
            column_offsets: offsets,
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 2],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        };
        let histogram = |id, first_low, first_high| IndexStats {
            histogram: Histogram {
                id,
                ndv: 100,
                buckets: vec![tidb_stats::Bucket {
                    count: 1_000,
                    repeat: 1,
                    ndv: 100,
                    lower_bound: Datum::Bytes(
                        tidb_codec::encode_key(&[Datum::Int(first_low), Datum::Int(1)]).unwrap(),
                    ),
                    upper_bound: Datum::Bytes(
                        tidb_codec::encode_key(&[Datum::Int(first_high), Datum::Int(100)]).unwrap(),
                    ),
                }],
                ..Histogram::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            num_columns: 2,
            unique: false,
        };
        let main_index = index(7, vec![0, 1]);
        table.add_index(main_index.clone(), false);
        let mut stats = TableStatistics::new(
            1_000,
            0,
            BTreeMap::new(),
            BTreeMap::from([(7, histogram(7, 1, 100))]),
        );
        stats.pseudo = false;
        stats.cache_pseudo = false;
        let ranges = [IndexRange {
            low: vec![Datum::Int(1), Datum::Int(2)],
            high: vec![Datum::Int(1), Datum::Int(8)],
            low_exclusive: false,
            high_exclusive: false,
        }];
        let baseline =
            index_row_count(&main_index, &table, &ranges, Some(&stats), 1_000.0, false).unwrap();
        // Go ColUniqueID2IdxIDs only maps c to (c,b). Its byte histogram
        // cannot answer a standalone bound on b, even though b is in the key.
        table.add_index(index(1, vec![2, 1]), false);
        stats.indexes.insert(1, histogram(1, 1_000, 2_000));
        let with_nonleading_index =
            index_row_count(&main_index, &table, &ranges, Some(&stats), 1_000.0, false).unwrap();
        assert_eq!(with_nonleading_index.est, baseline.est);
        assert_eq!(with_nonleading_index.min_est, baseline.min_est);
        assert_eq!(with_nonleading_index.max_est, baseline.max_est);
        // With b leading, the same distribution is eligible and supplies
        // the missing column estimate.
        table.add_index(index(2, vec![1, 2]), false);
        stats.indexes.insert(2, histogram(2, 1_000, 2_000));
        let with_leading_index =
            index_row_count(&main_index, &table, &ranges, Some(&stats), 1_000.0, false).unwrap();
        // Two contributing columns activate Go's index-NDV bound: 1000 / 100.
        assert_eq!(with_leading_index.max_est, 10.0);
        assert_ne!(with_leading_index.max_est, baseline.max_est);
    }

    #[test]
    fn unknown_values_in_composite_index_ranges_match_go() {
        let mut table = queue_test_table(vec![long_column("a", 1), long_column("b", 2)]);
        let index = KvIndex {
            id: 7,
            name: "idx_ab".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![0, 1],
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 2],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        };
        table.add_index(index.clone(), false);
        let mut topn = tidb_stats::TopN::new(10);
        for value in 0..10 {
            let encoded = tidb_codec::encode_key(&[Datum::Int(value), Datum::Int(value)]).unwrap();
            topn.append(&encoded, 1);
        }
        topn.sort();
        let index_stats = IndexStats {
            histogram: tidb_stats::Histogram {
                id: 7,
                ndv: 10,
                ..Default::default()
            },
            topn: Some(topn),
            cms: None,
            stats_ver: 2,
            num_columns: 2,
            unique: false,
        };
        let mut stats =
            TableStatistics::new(20, 10, BTreeMap::new(), BTreeMap::from([(7, index_stats)]));
        stats.pseudo = false;
        stats.cache_pseudo = false;
        let range = |low, high| {
            [IndexRange {
                low: vec![Datum::Int(low)],
                high: vec![Datum::Int(high)],
                low_exclusive: false,
                high_exclusive: false,
            }]
        };
        assert_eq!(
            index_row_count(&index, &table, &range(30, 30), Some(&stats), 20.0, false)
                .unwrap()
                .est,
            1.0
        );
        assert_eq!(
            index_row_count(&index, &table, &range(9, 30), Some(&stats), 20.0, false)
                .unwrap()
                .est,
            2.0
        );
    }

    #[test]
    fn multivalued_index_counts_scale_with_table_growth() {
        let mut json_column = long_column("j", 2);
        json_column.field_type = FieldType::new(FieldTypeCode::Json);
        let mut table = queue_test_table(vec![long_column("a", 1), json_column]);
        let index = KvIndex {
            id: 7,
            name: "mv_j".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![1],
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        };
        table.add_index(index.clone(), false);
        let column = ColumnStats {
            histogram: Histogram {
                id: 1,
                ndv: 100,
                last_update_version: 1,
                buckets: vec![tidb_stats::Bucket {
                    count: 1_000,
                    repeat: 1,
                    ndv: 100,
                    lower_bound: Datum::Int(1),
                    upper_bound: Datum::Int(100),
                }],
                ..Histogram::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        let mut topn = tidb_stats::cmsketch::TopN::new(1);
        topn.append(&tidb_codec::encode_key(&[Datum::Int(1)]).unwrap(), 50);
        let index_stats = IndexStats {
            histogram: Histogram {
                id: 7,
                ndv: 100,
                buckets: vec![tidb_stats::Bucket {
                    count: 4_950,
                    repeat: 50,
                    ndv: 99,
                    lower_bound: Datum::Bytes(tidb_codec::encode_key(&[Datum::Int(2)]).unwrap()),
                    upper_bound: Datum::Bytes(tidb_codec::encode_key(&[Datum::Int(100)]).unwrap()),
                }],
                ..Histogram::default()
            },
            topn: Some(topn),
            cms: None,
            stats_ver: 2,
            num_columns: 1,
            unique: false,
        };
        let mut stats = TableStatistics::new(
            1_500,
            100,
            BTreeMap::from([(1, column)]),
            BTreeMap::from([(7, index_stats)]),
        );
        stats.pseudo = false;
        stats
            .column_load_status
            .insert(1, tidb_stats::StatsLoadedStatus::full_load());
        stats
            .index_load_status
            .insert(7, tidb_stats::StatsLoadedStatus::full_load());
        let point = [IndexRange {
            low: vec![Datum::Int(1)],
            high: vec![Datum::Int(1)],
            low_exclusive: false,
            high_exclusive: false,
        }];
        // Ordinary indexes deliberately keep Go's unscaled-count behavior.
        assert_eq!(
            index_row_count(&index, &table, &point, Some(&stats), 1_500.0, false)
                .unwrap()
                .est,
            15.0
        );
        table.set_mv_key_part_source(7, "j".to_owned());
        let counts = index_row_counts(&stats, &table, &index);
        assert_eq!((counts.table_realtime, counts.table_modify), (1_500, 100));
        assert_eq!((counts.index_realtime, counts.index_modify), (7_500, 500));
        stats.index_load_status.remove(&7);
        assert_eq!(
            index_row_count(&index, &table, &point, Some(&stats), 1_500.0, false)
                .unwrap()
                .est,
            15.0
        );
        stats
            .index_load_status
            .insert(7, tidb_stats::StatsLoadedStatus::full_load());
        // 1000 table rows became 1500: 5000 index entries scale to 7500,
        // and the 50-entry TopN point scales by the same 1.5 growth factor.
        assert_eq!(
            index_row_count(&index, &table, &point, Some(&stats), 1_500.0, false)
                .unwrap()
                .est,
            75.0
        );
        let full = [IndexRange {
            low: vec![Datum::Null],
            high: vec![Datum::MaxValue],
            low_exclusive: false,
            high_exclusive: false,
        }];
        assert_eq!(
            index_row_count(&index, &table, &full, Some(&stats), 1_500.0, false)
                .unwrap()
                .est,
            7_500.0
        );
        stats.column_load_status.clear();
        // The MV index itself cannot serve as the analyzed TABLE row count.
        assert_eq!(
            index_row_count(&index, &table, &point, Some(&stats), 1_500.0, false)
                .unwrap()
                .est,
            15.0
        );
    }

    /// Go `IndexStatsIsInvalid` (`pkg/statistics/index.go:132`): estimating
    /// over an index whose statistics are NOT fully loaded queues that index
    /// into `AsyncLoadHistogramNeededItems` so the async loader can fetch the
    /// real histogram; the estimate itself still answers the pseudo rate.
    #[test]
    fn an_index_with_unloaded_stats_is_queued_for_async_load() {
        let index = KvIndex {
            id: 7,
            name: "idx_a".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![0],
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        };
        let table = queue_test_table(vec![long_column("a", 1)]);
        // No index_load_status entry: the index was never loaded. The
        // existence map DOES know it (the persisted stats row exists), which
        // is what makes the load genuinely needed.
        let mut stats = TableStatistics::new(10, 0, BTreeMap::new(), BTreeMap::new());
        // The table itself is real (not pseudo); only THIS index's stats are
        // evicted.
        stats.pseudo = false;
        stats.cache_pseudo = false;
        stats.index_stats_existence.insert(7, true);
        let item = TableItemID {
            table_id: table.table_id,
            id: 7,
            is_index: true,
            is_sync_load_failed: false,
        };
        let needed = &tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
        needed.delete(item);

        let estimate = index_row_count(&index, &table, &[], Some(&stats), 10.0, true).unwrap();
        assert!(
            estimate.est >= 0.0,
            "the estimate still answers the pseudo rate"
        );
        assert!(
            needed
                .all_items()
                .iter()
                .any(|loaded| loaded.table_item_id == item),
            "the unloaded index is queued for the async loader"
        );
        // Go IndexStatsIsInvalid also rejects a retained metadata-only
        // object: TotalRowCount() == 0, not merely a missing map entry.
        stats.indexes.insert(
            7,
            IndexStats {
                histogram: tidb_stats::Histogram {
                    id: 7,
                    ndv: 10,
                    ..Default::default()
                },
                topn: None,
                cms: None,
                stats_ver: 2,
                num_columns: 1,
                unique: false,
            },
        );
        stats
            .index_load_status
            .insert(7, tidb_stats::StatsLoadedStatus::all_evicted());
        let ranges = [IndexRange {
            low: vec![Datum::Int(1)],
            high: vec![Datum::Int(1)],
            low_exclusive: false,
            high_exclusive: false,
        }];
        let estimate = index_row_count(&index, &table, &ranges, Some(&stats), 10.0, true).unwrap();
        assert_eq!(estimate.est, pseudo_index_row_count(&index, &ranges, 10.0));
        needed.delete(item);
    }

    #[test]
    fn ordinary_index_estimation_dispatches_version_one_cms_ranges() {
        let mut table = queue_test_table(vec![long_column("a", 1)]);
        let index = KvIndex {
            id: 7,
            name: "ia".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![0],
            prefix_lengths: vec![-1],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        };
        table.add_index(index.clone(), false);
        let key = |value| tidb_codec::encode_key(&[Datum::Int(value)]).unwrap();
        let mut cms = tidb_stats::CmsSketch::new(5, 2048);
        for (value, count) in [1, 2, 7, 13, 77].into_iter().enumerate() {
            cms.insert_bytes_by_count(&key(value as i64), count);
        }
        let index_stats = IndexStats {
            histogram: tidb_stats::Histogram {
                id: 7,
                ndv: 5,
                buckets: vec![tidb_stats::Bucket {
                    count: 100,
                    repeat: 77,
                    ndv: 5,
                    lower_bound: Datum::Bytes(key(0)),
                    upper_bound: Datum::Bytes(key(4)),
                }],
                ..Default::default()
            },
            cms: Some(cms),
            topn: None,
            stats_ver: 1,
            num_columns: 1,
            unique: false,
        };
        let mut stats = TableStatistics::new(
            200,
            100,
            BTreeMap::new(),
            BTreeMap::from([(7, index_stats)]),
        );
        let ranges = [IndexRange {
            low: vec![Datum::Int(1)],
            high: vec![Datum::Int(3)],
            low_exclusive: false,
            high_exclusive: false,
        }];
        let estimate = index_row_count(&index, &table, &ranges, Some(&stats), 200.0, false).unwrap();
        assert!((estimate.est - 22.0).abs() < 1e-10, "{estimate:?}");
        stats.row_count = 10;
        let estimate = index_row_count(&index, &table, &ranges, Some(&stats), 10.0, false).unwrap();
        assert!((estimate.est - 22.0).abs() < 1e-10, "V1 retains its analyzed-count cap after deletes: {estimate:?}");
    }

    /// Go `TestCanSkipIndexEstimation`: a full ordinary-index range already
    /// has an exact row count, so estimating its histogram must neither run
    /// nor enqueue an evicted histogram for loading.
    #[test]
    fn full_index_range_skips_evicted_histogram_load() {
        let mut table = queue_test_table(vec![long_column("a", 1)]);
        let index = KvIndex {
            id: 7,
            name: "idx_a".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![0],
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        };
        table.add_index(index.clone(), false);
        let index_stats = IndexStats {
            histogram: tidb_stats::Histogram {
                id: 7,
                ndv: 50,
                null_count: 10,
                buckets: vec![tidb_stats::Bucket {
                    count: 50,
                    repeat: 1,
                    ndv: 50,
                    lower_bound: Datum::Bytes(vec![1]),
                    upper_bound: Datum::Bytes(vec![50]),
                }],
                ..Default::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            num_columns: 1,
            unique: false,
        };
        let mut stats =
            TableStatistics::new(60, 0, BTreeMap::new(), BTreeMap::from([(7, index_stats)]));
        stats.pseudo = false;
        stats.cache_pseudo = false;
        stats
            .index_load_status
            .insert(7, tidb_stats::StatsLoadedStatus::all_evicted());
        let item = TableItemID {
            table_id: table.table_id,
            id: 7,
            is_index: true,
            is_sync_load_failed: false,
        };
        let needed = &tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
        needed.delete(item);

        let full_range = [IndexRange {
            low: vec![Datum::Null],
            high: vec![Datum::MaxValue],
            low_exclusive: false,
            high_exclusive: false,
        }];
        let estimate =
            index_row_count(&index, &table, &full_range, Some(&stats), 60.0, true).unwrap();
        assert_eq!(estimate.est, 60.0);
        assert!(
            !needed
                .all_items()
                .iter()
                .any(|loaded| loaded.table_item_id == item),
            "a full-range estimate must return before queueing an evicted index"
        );

        let not_null_range = [IndexRange {
            low: vec![Datum::MinNotNull],
            high: vec![Datum::MaxValue],
            low_exclusive: false,
            high_exclusive: false,
        }];
        let _ = index_row_count(&index, &table, &not_null_range, Some(&stats), 60.0, true).unwrap();
        assert!(
            needed
                .all_items()
                .iter()
                .any(|loaded| loaded.table_item_id == item),
            "a full not-null range excludes NULLs and must use the ordinary load path"
        );
        needed.delete(item);

        for narrowed_range in [
            IndexRange {
                low: vec![Datum::Int(1)],
                high: vec![Datum::Int(10)],
                low_exclusive: false,
                high_exclusive: false,
            },
            IndexRange {
                low: vec![Datum::Null],
                high: vec![Datum::MaxValue],
                low_exclusive: true,
                high_exclusive: false,
            },
        ] {
            let narrowed = [narrowed_range];
            let _ = index_row_count(&index, &table, &narrowed, Some(&stats), 60.0, true).unwrap();
            assert!(
                needed
                    .all_items()
                    .iter()
                    .any(|loaded| loaded.table_item_id == item),
                "bounded and exclusive-NULL ranges must not use the full-range shortcut"
            );
            needed.delete(item);
        }

        table.set_mv_key_part_source(7, "j".to_owned());
        let _ = index_row_count(&index, &table, &full_range, Some(&stats), 60.0, true).unwrap();
        assert!(
            needed
                .all_items()
                .iter()
                .any(|loaded| loaded.table_item_id == item),
            "a multi-valued index can contain multiple entries per row"
        );
        needed.delete(item);

        let mut partial_table = queue_test_table(vec![long_column("a", 1)]);
        partial_table.add_index(index.clone(), false);
        let partial_item = TableItemID {
            table_id: partial_table.table_id,
            id: 7,
            is_index: true,
            is_sync_load_failed: false,
        };
        needed.delete(partial_item);
        let statement = tidb_parser::parse("SELECT * FROM t WHERE a > 1").unwrap();
        let tidb_ast::Stmt::Query(query) = statement else {
            panic!("query");
        };
        let tidb_ast::QueryStmt::Select(select) = &*query else {
            panic!("select");
        };
        partial_table
            .add_partial_index_condition(
                7,
                "idx_a",
                select.where_clause.as_ref().unwrap(),
                &tidb_datatype::SessionTimeZone::utc(),
                b'\\',
            )
            .unwrap();
        let _ = index_row_count(
            &index,
            &partial_table,
            &full_range,
            Some(&stats),
            60.0,
            true,
        )
        .unwrap();
        assert!(
            needed
                .all_items()
                .iter()
                .any(|loaded| loaded.table_item_id == partial_item),
            "a partial index does not contain every table row"
        );
        needed.delete(partial_item);
    }

    /// A fully loaded column is not queued.
    #[test]
    fn a_fully_loaded_column_is_not_queued() {
        let table = queue_test_table(vec![long_column("a", 1)]);
        let mut stats = TableStatistics::new(10, 0, BTreeMap::new(), BTreeMap::new());
        stats.pseudo = false;
        stats.cache_pseudo = false;
        stats.column_stats_existence.insert(1, true);
        stats
            .column_load_status
            .insert(1, tidb_stats::StatsLoadedStatus::full_load());
        let item = TableItemID {
            table_id: table.table_id,
            id: 1,
            is_index: false,
            is_sync_load_failed: false,
        };
        let needed = &tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
        needed.delete(item);

        // The column's stats are present AND fully loaded.
        let loaded = ColumnStats {
            histogram: Histogram::default(),
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        queue_column_stats_load_if_invalid(&table, &stats, 1, Some(&loaded));
        assert!(
            !needed
                .all_items()
                .iter()
                .any(|loaded| loaded.table_item_id == item),
            "a fully loaded column needs no async load"
        );
        needed.delete(item);
    }

    /// Internal pseudo columns (`_tidb_rowid`, ID <= 0) are never queued.
    #[test]
    fn internal_pseudo_columns_are_never_queued() {
        let table = queue_test_table(Vec::new());
        let stats = TableStatistics::new(10, 0, BTreeMap::new(), BTreeMap::new());
        let item = TableItemID {
            table_id: table.table_id,
            id: -1,
            is_index: false,
            is_sync_load_failed: false,
        };
        let needed = &tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
        needed.delete(item);

        queue_column_stats_load_if_invalid(&table, &stats, -1, None);
        assert!(
            !needed
                .all_items()
                .iter()
                .any(|loaded| loaded.table_item_id == item),
            "internal pseudo columns never reach the loader"
        );
        needed.delete(item);
    }

    /// Go `Selectivity`: a hidden column backing an expression index is
    /// skipped as well — only the index statistics are used for it.
    #[test]
    fn a_hidden_column_is_skipped_by_the_selectivity_engine() {
        let mut table = queue_test_table(vec![long_column("a", 1)]);
        let mut hidden_field = FieldType::new(FieldTypeCode::LongLong);
        hidden_field.set_collation_name("binary");
        let hidden = KvColumn {
            name: "_v$_idx_a".to_owned(),
            id: 9,
            field_type: hidden_field,
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        };
        table.add_hidden_column(hidden);
        let hidden_offset = table.columns.len() - 1;
        assert!(
            table.is_hidden(hidden_offset),
            "fixture: the tail column is hidden"
        );
    }

    #[test]
    fn a_fully_loaded_index_is_not_queued() {
        let index = KvIndex {
            id: 8,
            name: "idx_b".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![0],
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
            primary: false,
        };
        let table = queue_test_table(vec![long_column("a", 1)]);
        let mut stats = TableStatistics::new(10, 0, BTreeMap::new(), BTreeMap::new());
        stats.pseudo = false;
        stats.cache_pseudo = false;
        stats
            .index_load_status
            .insert(8, tidb_stats::StatsLoadedStatus::full_load());
        let item = TableItemID {
            table_id: table.table_id,
            id: 8,
            is_index: true,
            is_sync_load_failed: false,
        };
        let needed = &tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
        needed.delete(item);

        let _ = index_row_count(&index, &table, &[], Some(&stats), 10.0, true).unwrap();
        assert!(
            !needed
                .all_items()
                .iter()
                .any(|loaded| loaded.table_item_id == item),
            "a fully loaded index needs no async load"
        );
        needed.delete(item);
    }
}
