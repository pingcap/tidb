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

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use tidb_datatype::Datum;
use tidb_planner::cardinality::pseudo::{
    pseudo_row_count_by_index_ranges, pseudo_selectivity, IndexRange as PseudoIndexRange,
    PseudoBoundKind, PseudoColumn, PseudoFunctionKind, PseudoIndex, PseudoPredicate, ScalarRange,
};
use tidb_planner::cardinality::row_count_column::RowEstimate;
use tidb_planner::cardinality::row_count_estimator::{
    get_index_row_count_for_stats_v2, get_row_count_by_column_ranges, ColumnRange, ColumnStats,
    EstimatorOptions, IndexRangeDatums, IndexStats,
};
use tidb_planner::selectivity_greedy::{
    combine_selectivity, ConditionKind, SelectivityDefaults, StatsNode, StatsNodeType,
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
    /// Column statistics by column ID.
    pub columns: BTreeMap<i64, ColumnStats>,
    /// Index statistics by index ID.
    pub indexes: BTreeMap<i64, IndexStats>,
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
}

impl TableStatistics {
    /// Go `HistColl.GetAnalyzeRowCount`: the row count observed by the first
    /// fully loaded analyzed histogram, or `-1` when the table has none.
    #[must_use]
    pub(crate) fn analyze_row_count(&self) -> f64 {
        if let Some(column) = self.columns.values().next() {
            return column.total_row_count();
        }
        if let Some(index) = self.indexes.values().next() {
            return index.total_row_count();
        }
        -1.0
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
        Self {
            pseudo: row_count == 0 || (columns.is_empty() && indexes.is_empty()),
            row_count,
            modify_count,
            version: 0,
            last_analyze_version: 0,
            columns,
            indexes,
            column_load_status,
            index_load_status,
            column_stats_existence,
            index_stats_existence,
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

    /// Go `Table.ColumnIsLoadNeeded`.
    #[must_use]
    pub fn column_is_load_needed(&self, id: i64, full_load: bool) -> bool {
        if self.pseudo {
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
                        .find_map(|id| {
                            self.columns.get(id).and_then(|candidate| {
                                (candidate.histogram.last_update_version == version)
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

/// The session inputs Go reads off `PlanContext`; this tier touches none of
/// the risk variables, so they stay at their documented defaults.
fn estimator_options() -> EstimatorOptions {
    EstimatorOptions::default()
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

/// Go `isCoveringIndex`: every column the statement reads is stored in the
/// Go `getIndexRowCountForStatsV2` over the index's ranges, falling back to
/// the pseudo formula when the table -- or just this index -- has no
/// analyzed histogram.
fn index_row_count(
    index: &KvIndex,
    table: &KvTable,
    ranges: &[IndexRange],
    stats: Option<&TableStatistics>,
    realtime: f64,
) -> RowEstimate {
    let Some(stats) = stats.filter(|stats| !stats.pseudo) else {
        return RowEstimate::default_est(pseudo_index_row_count(index, ranges, realtime));
    };
    let Some(index_stats) = stats.indexes.get(&index.id) else {
        // A table WITH statistics whose index was never analyzed: Go's
        // `GetRowCountByIndexRanges` (`row_count_index.go:57`) first tries
        // the index columns' OWN histograms
        // (`getPseudoRowCountWithPartialStats`) and only a path with no
        // column statistics either falls to the pseudo rate. The clustered
        // PRIMARY this crate synthesizes ([`crate::handle_range::
        // clustered_primary_metadata`]) always lands here on an analyzed
        // table: its key columns are analyzed, its index id is stored
        // nowhere.
        if let Some(estimate) = partial_stats_index_row_count(index, table, ranges, stats, realtime)
        {
            return estimate;
        }
        return RowEstimate::default_est(pseudo_index_row_count(index, ranges, realtime));
    };
    let datum_ranges: Vec<IndexRangeDatums> = ranges
        .iter()
        .map(|range| IndexRangeDatums {
            low: range.low.clone(),
            high: range.high.clone(),
            low_exclude: range.low_exclusive,
            high_exclude: range.high_exclusive,
        })
        .collect();
    // The per-column histograms `expBackoffEstimation` consults, positionally
    // by index column -- Go's `HistColl.ColUniqueID2IdxIDs` walk. Without
    // them a range that pins a PREFIX of a composite index has only the index
    // histogram to go on and misses the leading column's TopN, which is an
    // exact count where it hits. A column with no histogram is `None`, which
    // the estimator skips exactly as the source does.
    let column_stats: Vec<Option<&ColumnStats>> = index
        .column_offsets
        .iter()
        .map(|offset| {
            table
                .columns
                .get(*offset)
                .and_then(|column| stats.columns.get(&column.id))
        })
        .collect();
    let mut estimate = get_index_row_count_for_stats_v2(
        index_stats,
        &column_stats,
        &datum_ranges,
        stats.row_count,
        stats.modify_count,
        estimator_options(),
    );
    estimate.clamp(0.0, realtime.max(0.0));
    estimate
}

/// Go's path `CountAfterAccess` for an already-built index range set.
pub(crate) fn index_range_row_count(
    index: &KvIndex,
    table: &KvTable,
    ranges: &[IndexRange],
    stats: Option<&TableStatistics>,
    realtime: f64,
) -> f64 {
    index_row_count(index, table, ranges, stats, realtime).est
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
    index: &KvIndex,
    table: &KvTable,
    ranges: &[IndexRange],
    stats: &TableStatistics,
    realtime: f64,
) -> Option<RowEstimate> {
    if realtime <= 0.0 {
        return Some(RowEstimate::default_est(0.0));
    }
    let columns: Vec<(Option<&ColumnStats>, tidb_datatype::Collation)> = index
        .column_offsets
        .iter()
        .map(|offset| {
            let column = table.columns.get(*offset);
            (
                column.and_then(|column| stats.columns.get(&column.id)),
                column.map_or(tidb_datatype::Collation::Binary, |column| {
                    column.field_type.collation()
                }),
            )
        })
        .collect();
    if columns.iter().all(|(stats, _)| stats.is_none()) {
        return None;
    }
    if ranges.iter().any(|range| range.is_full_range(false)) {
        return None;
    }
    // Go: a single-column index is directly the column estimate.
    if index.column_offsets.len() == 1 {
        let (column_stats, collation) = &columns[0];
        let column_ranges: Vec<ColumnRange> = ranges
            .iter()
            .map(|range| ColumnRange {
                low: range.low.first().cloned().unwrap_or(Datum::MinNotNull),
                high: range.high.first().cloned().unwrap_or(Datum::MaxValue),
                low_exclude: range.low_exclusive,
                high_exclude: range.high_exclusive,
            })
            .collect();
        return Some(RowEstimate::default_est(
            get_row_count_by_column_ranges(
                *column_stats,
                &column_ranges,
                *collation,
                stats.row_count,
                stats.modify_count,
                false,
                estimator_options(),
            )
            .est,
        ));
    }
    let mut total = 0.0;
    let mut max_count = 0.0;
    for range in ranges {
        let mut selectivity = 1.0;
        let mut corr_selectivity = 1.0f64;
        for (position, low) in range.low.iter().enumerate() {
            let last = position == range.low.len() - 1;
            let column_range = ColumnRange {
                low: low.clone(),
                high: range.high.get(position).cloned().unwrap_or(Datum::MaxValue),
                low_exclude: last && range.low_exclusive,
                high_exclude: last && range.high_exclusive,
            };
            let (column_stats, collation) = columns.get(position)?;
            // Go: "GetRowCountByColumnRanges handles invalid stats
            // internally by using pseudo estimation".
            let count = get_row_count_by_column_ranges(
                *column_stats,
                &[column_range],
                *collation,
                stats.row_count,
                stats.modify_count,
                false,
                estimator_options(),
            )
            .est;
            let temp_selectivity = count / realtime;
            selectivity *= temp_selectivity;
            corr_selectivity = corr_selectivity.min(temp_selectivity);
        }
        total += selectivity * realtime;
        max_count += corr_selectivity * realtime;
    }
    total = total.clamp(1.0, realtime);
    Some(RowEstimate::new(total, total, max_count))
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

/// Go `cardinality.Selectivity` for the conjuncts of a single-table `WHERE`,
/// as far as the shapes this tier extracts ranges for.
///
/// Go's version builds one statistics node per column or index the conditions
/// touch, greedily covers the condition mask with them, and multiplies the
/// covered nodes' selectivities (`selectivity.go`, the `ret *= ...` loop) --
/// the independence assumption. This computes the same product over the
/// conjuncts it can turn into single-column ranges, and gives every other
/// conjunct Go's `selectionFactor` fallback for an uncovered condition.
///
/// DEFERRED (documented): `crossValidationSelectivity`.
#[cfg(test)]
fn selectivity(
    predicate: &tidb_ast::Expr,
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
) -> f64 {
    selectivity_with_default_string_match_selectivity(predicate, table, resolver, stats, 0.0)
}

/// [`selectivity`] with the session's raw
/// `tidb_default_string_match_selectivity` value. A non-zero value disables
/// Go's TopN-assisted string-match estimation and uses that value as the
/// fallback selectivity instead.
pub(crate) fn selectivity_with_default_string_match_selectivity(
    predicate: &tidb_ast::Expr,
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
    default_string_match_selectivity: f64,
) -> f64 {
    let mut conjuncts = Vec::new();
    crate::plan_trace::collect_and(predicate, &mut conjuncts);
    selectivity_of_conjuncts_with_default_string_match_selectivity(
        &conjuncts,
        table,
        resolver,
        stats,
        default_string_match_selectivity,
    )
}

/// [`selectivity`] over conditions already split out of the `AND` tree, which
/// is the shape Go's `cardinality.Selectivity` takes and the shape the index
/// filters arrive in.
///
/// One statistics node PER COLUMN, not per conjunct. Go builds its nodes by
/// walking the DEDUPLICATED columns the conditions extract
/// (`selectivity.go:98-113`) and running `getMaskAndRanges` over the WHOLE
/// condition list for each one, so every predicate on a column is intersected
/// into that column's single range set before it is estimated. A per-conjunct
/// product instead treats `a >= 3 AND a <= 7` as two independent half-lines
/// and multiplies their rates: 1107.78 rows where TiDB prints 250.00, because
/// `[3,7]` is one BETWEEN range and neither `[3,+inf]` nor `[-inf,7]` is.
#[cfg(test)]
pub(crate) fn selectivity_of_conjuncts(
    conjuncts: &[&tidb_ast::Expr],
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
) -> f64 {
    selectivity_of_conjuncts_with_default_string_match_selectivity(
        conjuncts, table, resolver, stats, 0.0,
    )
}

/// [`selectivity_of_conjuncts`] with the session's raw string-match setting.
pub(crate) fn selectivity_of_conjuncts_with_default_string_match_selectivity(
    conjuncts: &[&tidb_ast::Expr],
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
    default_string_match_selectivity: f64,
) -> f64 {
    selectivity_of_conjuncts_with_defaults(
        conjuncts,
        table,
        resolver,
        stats,
        SelectivityDefaults::from_session(
            default_string_match_selectivity,
            tidb_planner::cost_factors::SELECTION_FACTOR,
        ),
    )
}

fn selectivity_of_conjuncts_with_defaults(
    conjuncts: &[&tidb_ast::Expr],
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
    defaults: SelectivityDefaults,
) -> f64 {
    selectivity_of_conjuncts_with_path_context(conjuncts, table, resolver, stats, true, defaults)
}

/// Go `cardinality.Selectivity(..., nil)` for filters estimated after an
/// access path has already been selected. Without `filledPaths`, clustered
/// PRIMARY statistics participate like every other index statistic.
#[cfg(test)]
fn selectivity_of_conjuncts_without_paths(
    conjuncts: &[&tidb_ast::Expr],
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
) -> f64 {
    selectivity_of_conjuncts_with_path_context(
        conjuncts,
        table,
        resolver,
        stats,
        false,
        SelectivityDefaults::default(),
    )
}

/// Materializes planner-owned constant columns before the ranger sees them.
///
/// Go's expression rewriter replaces an evaluated scalar subquery with an
/// `expression.Constant` before `cardinality.Selectivity` runs. Rust retains a
/// column-shaped AST node so EXPLAIN can print `ScalarQueryCol#N(value)`, while
/// [`tidb_expr::rewriter::ColumnResolver::resolve_constant`] exposes the same
/// typed value to expression consumers. The ranger still reads the AST, so it
/// needs an estimation-only copy with those nodes replaced by equivalent
/// literals; the statement AST remains unchanged for execution and display.
fn materialize_resolved_constants<'a>(
    expr: &'a tidb_ast::Expr,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> Cow<'a, tidb_ast::Expr> {
    if !resolver.has_resolved_constants() {
        return Cow::Borrowed(expr);
    }
    struct Materializer<'a> {
        resolver: &'a dyn tidb_expr::rewriter::ColumnResolver,
    }

    impl tidb_ast::Visitor for Materializer<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
                return false;
            };
            let tidb_ast::Expr::Column(path) = expr else {
                return false;
            };
            let path = path.clone();
            let Some(tidb_expr::expression::Expression::Constant(constant)) =
                self.resolver.resolve_constant(&path)
            else {
                return false;
            };
            if let Ok(literal) = crate::driver::datum_to_literal(&constant.value) {
                *expr = literal;
            }
            true
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut materialized = expr.clone();
    tidb_ast::Visitable::accept(&mut materialized, &mut Materializer { resolver });
    Cow::Owned(materialized)
}

fn selectivity_of_conjuncts_with_path_context(
    conjuncts: &[&tidb_ast::Expr],
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
    has_filled_paths: bool,
    defaults: SelectivityDefaults,
) -> f64 {
    let realtime = realtime_row_count(stats);
    // `selectivity.go:61`: no rows or no conditions is 100% selectivity.
    if conjuncts.is_empty() || realtime <= 0.0 {
        return 1.0;
    }
    let materialized = conjuncts
        .iter()
        .map(|conjunct| materialize_resolved_constants(conjunct, resolver))
        .collect::<Vec<_>>();
    // Go has no parenthesis node -- `ParenthesesExpr` is gone before
    // `Selectivity` runs -- so strip them once, here, and every structural
    // test downstream sees the shape the source sees.
    let conjuncts: Vec<&tidb_ast::Expr> = materialized
        .iter()
        .map(|conjunct| strip_parens(conjunct.as_ref()))
        .collect();

    // `selectivity.go:69-73`: past 63 conditions the mask no longer fits an
    // int64, and the source gives up on nodes entirely -- for a loaded
    // histogram collection just as much as for a pseudo one.
    if conjuncts.len() > 63 {
        let predicates: Vec<PseudoPredicate> = conjuncts
            .iter()
            .map(|conjunct| pseudo_predicate(conjunct, table, resolver))
            .collect();
        return pseudo_selectivity(
            &predicates,
            &pseudo_unique_indexes(table),
            realtime as i64,
            defaults.selectivity_factor,
        );
    }

    // A pseudo table is not a table without statistics as far as
    // `Selectivity` is concerned: `statistics.PseudoTable` fills a
    // `NewPseudoHistogram` entry for every public column and index
    // (`pkg/statistics/table.go:1034-1061`), so `coll.ColNum()` is non-zero
    // and `Selectivity` takes its ORDINARY body. Only the per-node row count
    // differs: the pseudo equal/less/between rates rather than a histogram.
    // (`pseudoSelectivity`, a MINIMUM over per-operator rates, is reached only
    // down the `len(exprs) > 63` arm above -- it cannot compound, and says
    // 10.00 rows for `a = 1 and b = 2` where TiDB prints 1.00.)
    let loaded = stats.filter(|stats| !stats.pseudo);

    let extracted_offsets = extracted_column_offsets(&conjuncts, table, resolver);
    let extracted_offset_set: BTreeSet<usize> = extracted_offsets.iter().copied().collect();
    let mut nodes = Vec::new();
    for offset in extracted_offsets {
        let Some(column) = table.columns.get(offset) else {
            continue;
        };
        // `conditionChecker` correctly refuses `IS NOT NULL` as an INDEX
        // access condition because `(NULL,+inf]` does not narrow the scan.
        // `cardinality.Selectivity` uses a different ranger entry point: the
        // same interval is still a COLUMN statistics node and removes the
        // NULL bucket. Recover that mask here instead of treating the
        // condition as the generic 0.8 fallback.
        let not_null_mask = conjuncts
            .iter()
            .enumerate()
            .filter(|(_, conjunct)| is_not_null_on_column(conjunct, offset, table, resolver))
            .fold(0_i64, |mask, (index, _)| mask | (1_i64 << index));
        // Go `getMaskAndRanges` down the `ranger.ColumnRangeType` arm: a range
        // over the COLUMN itself, so no index prefix length is in play.
        let built = crate::index_range::detach_conds_for_column(
            &crate::index_range::RangeColumn::whole(column.name.clone(), column.field_type.clone()),
            &conjuncts,
            &resolver.time_zone(),
        );
        // `BuildColumnRange` with no access condition returns the full range
        // and an empty mask, which the greedy cover can never select. `IS NOT
        // NULL` is the one full range the statistics path still consumes.
        if built.access_count == 0 && not_null_mask == 0 {
            continue;
        }
        let ranges: Vec<ColumnRange> = if built.access_count == 0 {
            vec![ColumnRange {
                low: Datum::MinNotNull,
                high: Datum::MaxValue,
                low_exclude: false,
                high_exclude: false,
            }]
        } else {
            built
                .ranges
                .iter()
                .map(|range| ColumnRange {
                    low: range.low.first().cloned().unwrap_or(Datum::MinNotNull),
                    high: range.high.first().cloned().unwrap_or(Datum::MaxValue),
                    low_exclude: range.low_exclusive,
                    high_exclude: range.high_exclusive,
                })
                .collect()
        };
        let is_handle = table.pk_handle_offset() == Some(offset);
        let row_count = match loaded {
            Some(stats) => {
                get_row_count_by_column_ranges(
                    stats.columns.get(&column.id),
                    &ranges,
                    column.field_type.collation(),
                    stats.row_count,
                    stats.modify_count,
                    is_handle,
                    estimator_options(),
                )
                .est
            }
            // A pseudo handle column uses Go's signed/unsigned integer range
            // estimator, not the ordinary scalar BETWEEN rate. The former
            // clamps a finite handle span to its numeric width: `[1,100]`
            // estimates 99 rows on a 10,000-row pseudo table, while a normal
            // column estimates 250. `GetRowCountByColumnRanges` owns that
            // `pkIsHandle` dispatch for both loaded and pseudo statistics.
            None => {
                get_row_count_by_column_ranges(
                    None,
                    &ranges,
                    column.field_type.collation(),
                    realtime as i64,
                    0,
                    is_handle,
                    estimator_options(),
                )
                .est
            }
        };
        nodes.push(StatsNode {
            selectivity: (row_count / realtime).clamp(0.0, 1.0),
            ..StatsNode::new(
                // `selectivity.go:120`: a handle column's node is a `PkType`,
                // which outranks a plain column in the greedy tie break.
                if is_handle {
                    StatsNodeType::PrimaryKey
                } else {
                    StatsNodeType::Column
                },
                column.id,
                covered_mask(&conjuncts, &built.residual) | not_null_mask,
                1,
            )
        });
    }

    // `selectivity.go:144-211`: build one statistics node for every index
    // whose leading column prefix occurs in the extracted condition columns.
    // The range and row-count estimators are the same ones used by physical
    // access paths; only the resulting mask participates in this greedy
    // logical selectivity cover.
    let mut indexes: Vec<&KvIndex> = table.indexes().iter().collect();
    indexes.sort_by_key(|index| index.id);
    for index in indexes {
        if has_filled_paths
            && index.name.eq_ignore_ascii_case("PRIMARY")
            && index.column_offsets == table.common_handle_offsets()
        {
            continue;
        }
        let prefix_len = index
            .column_offsets
            .iter()
            .take_while(|offset| extracted_offset_set.contains(offset))
            .count();
        if prefix_len == 0 {
            continue;
        }
        let index_columns: Option<Vec<crate::index_range::RangeColumn>> = index
            .column_offsets
            .iter()
            .take(prefix_len)
            .enumerate()
            .map(|(position, offset)| {
                let column = table.columns.get(*offset)?;
                Some(crate::index_range::RangeColumn {
                    name: column.name.clone(),
                    field_type: column.field_type.clone(),
                    prefix_len: index.prefix_length(position),
                })
            })
            .collect();
        let Some(index_columns) = index_columns else {
            continue;
        };
        let Some(built) = crate::index_range::detach_conjuncts_and_build_range_for_index(
            &index_columns,
            &conjuncts,
            &resolver.time_zone(),
        ) else {
            continue;
        };
        let row_count = index_row_count(index, table, &built.ranges, stats, realtime).est;
        nodes.push(StatsNode {
            selectivity: (row_count / realtime).clamp(0.0, 1.0),
            ..StatsNode::new(
                StatsNodeType::Index,
                index.id,
                covered_mask(&conjuncts, &built.residual),
                index.column_offsets.len(),
            )
        });
    }

    let conditions: Vec<ConditionKind> = conjuncts
        .iter()
        .map(|conjunct| condition_kind(conjunct, table, resolver, stats, defaults))
        .collect();
    combine_selectivity(&mut nodes, &conditions, 1.0, realtime as i64, defaults)
}

/// Whether one statistics condition is `column IS NOT NULL` for `offset`.
fn is_not_null_on_column(
    conjunct: &tidb_ast::Expr,
    offset: usize,
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> bool {
    let tidb_ast::Expr::Is {
        expr,
        target: tidb_ast::IsTarget::Null,
        not: true,
    } = strip_parens(conjunct)
    else {
        return false;
    };
    let tidb_ast::Expr::Column(path) = strip_parens(expr) else {
        return false;
    };
    physical_column_offset(path, table, resolver).is_some_and(|resolved| resolved == offset)
}

/// Go's `expression.ExtractColumnsMapFromExpressions` over the whole condition
/// list, deduplicated and sorted (`selectivity.go:98-104`).
///
/// The order matters: it decides the order the statistics nodes are built in,
/// and the greedy cover breaks ties on node ID. Go sorts by `Column.ID`, which
/// for this tier is the table column's own id.
fn extracted_column_offsets(
    conjuncts: &[&tidb_ast::Expr],
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> Vec<usize> {
    let mut offsets: Vec<usize> = conjuncts
        .iter()
        .filter_map(|conjunct| physical_column_offsets(conjunct, table, resolver))
        .flatten()
        .collect();
    offsets.sort_by_key(|offset| table.columns.get(*offset).map(|column| column.id));
    offsets.dedup();
    offsets
}

/// Resolves the columns an expression reads to the base table's physical
/// offsets. A statement scope may already be compacted by column pruning, so
/// its row offsets are execution-layout identities and cannot index the
/// unpruned [`KvTable`] or its histograms. Go keeps `Column.UniqueID` stable
/// across pruning; rebinding the validated AST name to the physical table is
/// the equivalent identity at this boundary.
fn physical_column_offsets(
    expr: &tidb_ast::Expr,
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> Option<Vec<usize>> {
    struct Paths(Vec<Vec<String>>);

    impl tidb_ast::Visitor for Paths {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(tidb_ast::Expr::Column(path)) = node.downcast_ref::<tidb_ast::Expr>() {
                self.0.push(path.clone());
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut paths = Paths(Vec::new());
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut paths);
    paths
        .0
        .iter()
        .map(|path| physical_column_offset(path, table, resolver))
        .collect()
}

fn physical_column_offset(
    path: &[String],
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> Option<usize> {
    // Preserve the statement resolver's unknown/ambiguous/qualifier checks.
    resolver.resolve(path)?;
    let name = path.last()?;
    table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(name))
}

/// Go's `mask` for one statistics node: the bit of every condition the column
/// took as an access condition (`selectivity.go:498-506`).
///
/// The source matches access conditions back against the condition list by
/// expression equality; the range builder here hands back the conjuncts it did
/// NOT take, borrowed from the very slice that went in, so the same answer
/// falls out of pointer identity -- and identity, unlike equality, keeps the
/// two halves of `a = 1 AND a = 1` distinct.
fn covered_mask(conjuncts: &[&tidb_ast::Expr], residual: &[&tidb_ast::Expr]) -> i64 {
    let mut mask = 0_i64;
    for (index, conjunct) in conjuncts.iter().enumerate() {
        if !residual.iter().any(|left| std::ptr::eq(*left, *conjunct)) {
            mask |= 1_i64 << index;
        }
    }
    mask
}

/// An expression with every enclosing `(...)` removed.
///
/// Go has no parenthesis node: `ParenthesesExpr` is gone by the time
/// `Selectivity` runs, so `and(a<8, (b>10 or c<3))` reaches it as a plain
/// `LogicAnd` over a `LogicOr`. This port keeps the node, so every structural
/// test on a condition has to look through it first.
fn strip_parens(expr: &tidb_ast::Expr) -> &tidb_ast::Expr {
    let mut expr = expr;
    while let tidb_ast::Expr::Paren(inner) = expr {
        expr = inner;
    }
    expr
}

/// Go `expression.FlattenDNFConditions`: the `OR` terms of one condition.
fn collect_or<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    let expr = strip_parens(expr);
    if let tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, lhs, rhs) = expr {
        collect_or(lhs, out);
        collect_or(rhs, out);
        return;
    }
    out.push(expr);
}

/// Go's `notCoveredDNF` branch of `Selectivity` (`selectivity.go:324-385`).
///
/// ```text
/// // sel(condA or condB) = sel(condA) + sel(condB) - sel(condA) * sel(condB)
/// for _, cond := range dnfItems {
///     cnfItems := FlattenCNFConditions(cond)          // when it is an AND
///     curSelectivity, err := Selectivity(ctx, coll, cnfItems, nil)
///     selectivity = selectivity + curSelectivity - selectivity*curSelectivity
/// }
/// ```
///
/// Inclusion-exclusion under independence, with each term estimated by the
/// SAME entry point -- so an `AND` inside an `OR` inside an `AND` is handled
/// by recursion rather than by a second rule. `None` when the condition is
/// not a disjunction, or when the terms estimate to exactly zero, which is
/// Go's `if selectivity != 0` guard against covering the condition with a
/// number that would zero the whole result.
///
/// NOT MODELLED: Go's pre-guard at `selectivity.go:328-334`, which abandons
/// the DNF branch when any column it names has no statistics. Direction: this
/// port estimates such a disjunction from the columns it does have instead of
/// charging the flat 0.8 default.
fn dnf_selectivity(
    conjunct: &tidb_ast::Expr,
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
    defaults: SelectivityDefaults,
) -> Option<f64> {
    let mut items = Vec::new();
    collect_or(conjunct, &mut items);
    if items.len() <= 1 {
        return None;
    }
    let mut selectivity = 0.0_f64;
    for item in items {
        let mut cnf = Vec::new();
        crate::plan_trace::collect_and(strip_parens(item), &mut cnf);
        let current =
            selectivity_of_conjuncts_with_defaults(&cnf, table, resolver, stats, defaults);
        selectivity = selectivity + current - selectivity * current;
    }
    (selectivity != 0.0).then_some(selectivity)
}

/// Go's row-valued `IN` rewrite as the DNF estimator sees it.
///
/// The expression rewriter lowers
///
/// ```text
/// (a, b) IN ((1, 2), (3, 4))
/// ```
///
/// to `(a = 1 AND b = 2) OR (a = 3 AND b = 4)` before
/// `cardinality.Selectivity` classifies the condition.  This AST deliberately
/// retains the compact row-valued `IN`, so reproduce that rewrite only for
/// estimation.  Each tuple is estimated through the ordinary CNF entry point;
/// importantly, that applies Go's one-row floor *per tuple* before the DNF
/// inclusion-exclusion step.  On a 10,000-row pseudo table, ten distinct
/// common-handle prefixes therefore produce
/// `10000 * (1 - (1 - 1/10000)^10) = 9.995501...` logical rows, which is the
/// lower bound consumed by `adjustCountAfterAccess`.
fn row_in_selectivity(
    conjunct: &tidb_ast::Expr,
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
    defaults: SelectivityDefaults,
) -> Option<f64> {
    let tidb_ast::Expr::In { expr, list, not } = strip_parens(conjunct) else {
        return None;
    };
    if *not {
        return None;
    }
    let tidb_ast::Expr::Row(left) = strip_parens(expr) else {
        return None;
    };
    if left.is_empty() || list.is_empty() {
        return None;
    }

    let mut selectivity = 0.0_f64;
    for candidate in list {
        let tidb_ast::Expr::Row(right) = strip_parens(candidate) else {
            return None;
        };
        if right.len() != left.len() {
            return None;
        }
        let equalities: Vec<tidb_ast::Expr> = left
            .iter()
            .zip(right)
            .map(|(lhs, rhs)| {
                tidb_ast::Expr::Binary(
                    tidb_ast::BinaryOp::Eq,
                    Box::new(lhs.clone()),
                    Box::new(rhs.clone()),
                )
            })
            .collect();
        let conjuncts: Vec<&tidb_ast::Expr> = equalities.iter().collect();
        let current =
            selectivity_of_conjuncts_with_defaults(&conjuncts, table, resolver, stats, defaults);
        selectivity = selectivity + current - selectivity * current;
    }
    (selectivity != 0.0).then_some(selectivity)
}

/// Which leftover bucket one uncovered condition falls in
/// (`selectivity.go:238-304`).
///
/// The kind is computed for every condition, not just the uncovered ones,
/// because the mask decides which ones are read; a covered condition's kind
/// is never consulted.
fn condition_kind(
    conjunct: &tidb_ast::Expr,
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
    defaults: SelectivityDefaults,
) -> ConditionKind {
    match conjunct {
        // Go's `NOT LIKE` is `unaryNot(like(...))`, which the source unwraps
        // before classifying; this AST carries the negation on the node.
        tidb_ast::Expr::Like { not, .. } => {
            let selectivity = defaults
                .eval_topn_string_match
                .then(|| string_match_selectivity(conjunct, table, resolver, stats))
                .flatten();
            if *not {
                ConditionKind::NegatedStringMatch(selectivity)
            } else {
                ConditionKind::StringMatch(selectivity)
            }
        }
        tidb_ast::Expr::Regexp { not, .. } => {
            if *not {
                ConditionKind::NegatedStringMatch(None)
            } else {
                ConditionKind::StringMatch(None)
            }
        }
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, _, _) => {
            ConditionKind::Disjunction(dnf_selectivity(conjunct, table, resolver, stats, defaults))
        }
        tidb_ast::Expr::In { .. } => ConditionKind::Disjunction(row_in_selectivity(
            conjunct, table, resolver, stats, defaults,
        )),
        _ => ConditionKind::Other,
    }
}

/// Go `GetSelectivityByFilter` for a direct-column `LIKE`/`ILIKE` over a
/// fully available StatsVer2 column. This tier eagerly loads every statistics
/// payload, so the presence of the column histogram and TopN is its
/// `IsFullLoad` proof.
fn string_match_selectivity(
    predicate: &tidb_ast::Expr,
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
) -> Option<f64> {
    let stats = stats.filter(|stats| !stats.pseudo)?;
    let tidb_ast::Expr::Like {
        expr,
        pattern,
        not,
        ilike,
        escape,
    } = strip_parens(predicate)
    else {
        return None;
    };
    let tidb_ast::Expr::Column(path) = strip_parens(expr) else {
        return None;
    };
    let pattern = match strip_parens(pattern) {
        tidb_ast::Expr::String(pattern) | tidb_ast::Expr::RawString(pattern) => pattern.as_bytes(),
        _ => return None,
    };
    let offset = physical_column_offset(path, table, resolver)?;
    let column = table.columns.get(offset)?;
    if !tidb_datatype::is_bin_collation(column.field_type.collation_name()) {
        return None;
    }
    let column_stats = stats.columns.get(&column.id)?;
    if column_stats.stats_ver != 2 {
        return None;
    }

    let datum_matches = |datum: &Datum| -> Option<bool> {
        let value = match datum {
            Datum::Bytes(value) => value.as_slice(),
            Datum::String(value) => value.bytes(),
            _ => return None,
        };
        let matched = if *ilike {
            tidb_expr::ilike_match(value, pattern, escape.unwrap_or(b'\\'))
        } else {
            tidb_expr::like_match_with_collation(
                value,
                pattern,
                *escape,
                column.field_type.collation(),
            )
        };
        Some(if *not { !matched } else { matched })
    };

    let topn_total = column_stats
        .topn
        .as_ref()
        .map_or(0_u64, tidb_stats::cmsketch::TopN::total_count);
    let histogram_total = column_stats.histogram.not_null_count();
    let total =
        topn_total as f64 + histogram_total + column_stats.histogram.null_count.max(0) as f64;
    if total <= 0.0 {
        return None;
    }

    let mut topn_selected = 0_u64;
    if let Some(topn) = &column_stats.topn {
        for (index, entry) in topn.entries().iter().enumerate() {
            let encoded = topn.entry_bytes(index)?;
            let (remaining, datum) = tidb_codec::decode_one(&encoded).ok()?;
            if !remaining.is_empty() {
                return None;
            }
            if datum_matches(&datum)? {
                topn_selected = topn_selected.wrapping_add(entry.count);
            }
        }
    }

    let buckets = &column_stats.histogram.buckets;
    let mut repeat_total = 0_i64;
    let mut repeat_selected = 0_i64;
    let mut lower_selected = 0_usize;
    for bucket in buckets {
        repeat_total = repeat_total.wrapping_add(bucket.repeat);
        if datum_matches(&bucket.lower_bound)? {
            lower_selected += 1;
        }
        if datum_matches(&bucket.upper_bound)? {
            repeat_selected = repeat_selected.wrapping_add(bucket.repeat);
        }
    }

    let histogram_selectivity = if histogram_total > 0.0 && !buckets.is_empty() {
        let upper_ratio = (repeat_total.max(0) as f64 / histogram_total).min(1.0);
        let lower_ratio = 1.0 - upper_ratio;
        let lower_selectivity = lower_selected as f64 / buckets.len() as f64;
        let upper_selectivity = if repeat_total > 0 {
            repeat_selected as f64 / repeat_total as f64
        } else {
            0.0
        };
        (lower_selectivity * lower_ratio + upper_selectivity * upper_ratio) * histogram_total
            / total
    } else {
        0.0
    };
    Some(topn_selected as f64 / total + histogram_selectivity)
}

/// One condition as `pseudoSelectivity` reads it (`pseudo.go:44-67`).
///
/// `getConstantColumnID` (`selectivity.go:615`) demands EXACTLY two arguments,
/// one a column and one a constant, so a multi-value `IN` -- whose argument
/// list is the column plus every value -- resolves to no column at all and
/// cannot lower `minFactor`, even though `ast.In` has its own switch arm.
fn pseudo_predicate(
    conjunct: &tidb_ast::Expr,
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> PseudoPredicate {
    let (kind, args): (PseudoFunctionKind, [&tidb_ast::Expr; 2]) = match conjunct {
        tidb_ast::Expr::Binary(op, lhs, rhs) => {
            let kind = match op {
                tidb_ast::BinaryOp::Eq | tidb_ast::BinaryOp::NullEq => PseudoFunctionKind::Equality,
                tidb_ast::BinaryOp::Ge
                | tidb_ast::BinaryOp::Gt
                | tidb_ast::BinaryOp::Le
                | tidb_ast::BinaryOp::Lt => PseudoFunctionKind::Ordering,
                // Every other scalar function reaches the switch and matches
                // no arm: it still resolves a column, and still changes
                // nothing. `ne` is the shape the >63 capture uses.
                _ => PseudoFunctionKind::Other,
            };
            (kind, [lhs.as_ref(), rhs.as_ref()])
        }
        tidb_ast::Expr::In { expr, list, not } if !not && list.len() == 1 => {
            (PseudoFunctionKind::Equality, [expr.as_ref(), &list[0]])
        }
        _ => return PseudoPredicate::Unresolved,
    };
    let column = match (is_constant_literal(args[0]), is_constant_literal(args[1])) {
        (false, true) => args[0],
        (true, false) => args[1],
        _ => return PseudoPredicate::Unresolved,
    };
    let Some(offsets) = physical_column_offsets(column, table, resolver) else {
        return PseudoPredicate::Unresolved;
    };
    let [offset] = offsets[..] else {
        return PseudoPredicate::Unresolved;
    };
    let Some(column) = table.columns.get(offset) else {
        return PseudoPredicate::Unresolved;
    };
    PseudoPredicate::Resolved {
        kind,
        // `coll.GetCol(colID)` never returns nil for a public column of a
        // pseudo table -- `PseudoTable` inserted one histogram per column --
        // so the source's nil arm is unreachable from here.
        column: Some(PseudoColumn {
            lower_name: column.name.to_lowercase(),
            unique_key_flag: has_unique_key_flag(table, offset),
        }),
    }
}

/// Go `mysql.HasUniKeyFlag` for one column, from the indexes that set it.
///
/// `pkg/ddl/index.go:625-628` and `pkg/ddl/create_table.go:1231-1235` agree:
/// only a UNIQUE index over exactly ONE column gives that column
/// `UniqueKeyFlag`; the first column of a composite unique index gets
/// `MultipleKeyFlag` instead, which is why `pseudoSelectivity` has a separate
/// whole-index check for the composite case. A clustered/handle primary key
/// carries `PriKeyFlag`, which `HasUniKeyFlag` does not match.
fn has_unique_key_flag(table: &KvTable, offset: usize) -> bool {
    table
        .indexes()
        .iter()
        .any(|index| index.unique && index.column_offsets == [offset])
}

/// The unique indexes `pseudoSelectivity`'s `ForEachIndexImmutable` walk can
/// return `1/RealtimeCount` from, in the shape its port takes.
fn pseudo_unique_indexes(table: &KvTable) -> Vec<PseudoIndex> {
    table
        .indexes()
        .iter()
        .filter(|index| index.unique)
        .map(|index| PseudoIndex {
            unique: true,
            column_lower_names: index
                .column_offsets
                .iter()
                .filter_map(|offset| table.columns.get(*offset))
                .map(|column| column.name.to_lowercase())
                .collect(),
        })
        .collect()
}

/// Whether an expression is one of Go's `expression.Constant`s, for
/// `getConstantColumnID`'s two-argument test.
fn is_constant_literal(expr: &tidb_ast::Expr) -> bool {
    matches!(
        expr,
        tidb_ast::Expr::Int(_)
            | tidb_ast::Expr::Decimal(_)
            | tidb_ast::Expr::Float(_)
            | tidb_ast::Expr::Hex(_)
            | tidb_ast::Expr::Bit(_)
            | tidb_ast::Expr::String(_)
            | tidb_ast::Expr::RawString(_)
            | tidb_ast::Expr::Null
            | tidb_ast::Expr::Bool(_)
            | tidb_ast::Expr::ParamMarker {
                in_execute: true,
                value: Some(_),
                ..
            }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_table::KvColumn;
    use crate::storage::MemTableStorage;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::rewriter::ColumnResolver;

    struct NamedColumnResolver<'a> {
        table: &'a KvTable,
    }

    impl ColumnResolver for NamedColumnResolver<'_> {
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            let name = path.last()?;
            self.table
                .columns
                .iter()
                .enumerate()
                .find(|(_, column)| column.name.eq_ignore_ascii_case(name))
                .map(|(offset, column)| (offset, column.field_type.clone(), column.id))
        }

        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            tidb_datatype::SessionTimeZone::utc()
        }
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

    /// Go lowers row-valued IN to DNF before cardinality estimation and
    /// applies the one-row floor independently to every tuple.  Keeping the
    /// compact AST must not lose that floor: this is the exact logical row
    /// count behind TPCC delivery's 12.49-row common-handle range scan.
    #[test]
    fn row_in_selectivity_applies_the_one_row_floor_per_tuple() {
        let table = KvTable::with_storage(
            80,
            vec![
                long_column("w_id", 1),
                long_column("d_id", 2),
                long_column("o_id", 3),
            ],
            Box::new(MemTableStorage::new()),
        );
        let left = tidb_ast::Expr::Row(
            ["w_id", "d_id", "o_id"]
                .into_iter()
                .map(|name| tidb_ast::Expr::Column(vec![name.to_owned()]))
                .collect(),
        );
        let list = (1..=10)
            .map(|district| {
                tidb_ast::Expr::Row(vec![
                    tidb_ast::Expr::Int("1".to_owned()),
                    tidb_ast::Expr::Int(district.to_string()),
                    tidb_ast::Expr::Int("1".to_owned()),
                ])
            })
            .collect();
        let predicate = tidb_ast::Expr::In {
            expr: Box::new(left),
            list,
            not: false,
        };

        let actual = selectivity(
            &predicate,
            &table,
            &NamedColumnResolver { table: &table },
            None,
        );
        let expected = 1.0_f64 - (1.0_f64 - 1.0_f64 / 10_000.0_f64).powi(10);
        assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
    }

    /// Go `GetSelectivityByFilter` evaluates one-column string predicates
    /// against every StatsVer2 TopN value and both histogram bounds. Lower
    /// bounds are equal-weight samples while upper bounds retain `Repeat`.
    #[test]
    fn stats_v2_string_match_evaluates_topn_and_histogram_bounds() {
        let mut field_type = FieldType::new(FieldTypeCode::Varchar);
        field_type.set_charset_name("utf8mb4");
        field_type.set_collation_name("utf8mb4_bin");
        let table = KvTable::with_storage(
            81,
            vec![KvColumn {
                name: "name".to_owned(),
                id: 1,
                field_type,
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                comment: String::new(),
                generated: None,
            }],
            Box::new(MemTableStorage::new()),
        );
        let mut topn = tidb_stats::cmsketch::TopN::new(2);
        for (value, count) in [("needle top", 20), ("other top", 10)] {
            topn.append(
                &tidb_codec::encode_key(&[Datum::new_bytes(value.as_bytes().to_vec())]).unwrap(),
                count,
            );
        }
        topn.sort();
        let stats = TableStatistics::new(
            100,
            0,
            BTreeMap::from([(
                1,
                ColumnStats {
                    histogram: tidb_stats::Histogram {
                        id: 1,
                        ndv: 6,
                        buckets: vec![
                            tidb_stats::Bucket {
                                count: 30,
                                repeat: 5,
                                ndv: 2,
                                lower_bound: Datum::new_bytes(b"hist needle lower".to_vec()),
                                upper_bound: Datum::new_bytes(b"hist needle upper".to_vec()),
                            },
                            tidb_stats::Bucket {
                                count: 70,
                                repeat: 10,
                                ndv: 2,
                                lower_bound: Datum::new_bytes(b"other lower".to_vec()),
                                upper_bound: Datum::new_bytes(b"other upper".to_vec()),
                            },
                        ],
                        ..tidb_stats::Histogram::default()
                    },
                    topn: Some(topn),
                    cms: None,
                    stats_ver: 2,
                    unsigned: false,
                },
            )]),
            BTreeMap::new(),
        );
        let predicate = tidb_ast::Expr::Like {
            expr: Box::new(tidb_ast::Expr::Column(vec!["name".to_owned()])),
            pattern: Box::new(tidb_ast::Expr::String("%needle%".to_owned())),
            not: false,
            ilike: false,
            escape: None,
        };

        let actual = selectivity(
            &predicate,
            &table,
            &NamedColumnResolver { table: &table },
            Some(&stats),
        );
        assert!((actual - 0.525).abs() < 1e-12, "{actual} != 0.525");
    }

    #[test]
    fn pseudo_integer_handle_range_uses_its_numeric_span() {
        let mut table = KvTable::with_storage(
            81,
            vec![long_column("id", 1)],
            Box::new(MemTableStorage::new()),
        );
        table.set_pk_handle_offset(0);
        let predicate = tidb_ast::Expr::Between {
            expr: Box::new(tidb_ast::Expr::Column(vec!["id".to_owned()])),
            low: Box::new(tidb_ast::Expr::Int("1".to_owned())),
            high: Box::new(tidb_ast::Expr::Int("100".to_owned())),
            not: false,
        };

        let actual = selectivity(
            &predicate,
            &table,
            &NamedColumnResolver { table: &table },
            None,
        );
        assert!((actual - 99.0 / 10_000.0).abs() < 1e-12, "{actual}");
    }

    #[test]
    fn execute_bound_markers_use_the_same_handle_selectivity_as_literals() {
        let mut table = KvTable::with_storage(
            82,
            vec![long_column("id", 1)],
            Box::new(MemTableStorage::new()),
        );
        table.set_pk_handle_offset(0);
        let marker = |order, value| tidb_ast::Expr::ParamMarker {
            offset: order,
            order,
            in_execute: true,
            value: Some(Datum::Int(value)),
            projection_offset: 0,
        };
        let predicate = tidb_ast::Expr::Between {
            expr: Box::new(tidb_ast::Expr::Column(vec!["id".to_owned()])),
            low: Box::new(marker(0, 1)),
            high: Box::new(marker(1, 100)),
            not: false,
        };

        let actual = selectivity(
            &predicate,
            &table,
            &NamedColumnResolver { table: &table },
            None,
        );
        assert!((actual - 99.0 / 10_000.0).abs() < 1e-12, "{actual}");
    }

    /// Go's DataSource call supplies `filledPaths`, so a clustered PRIMARY
    /// mapped to the not-yet-filled table path cannot replace the secondary
    /// composite estimate. The later IndexFilters call supplies nil paths and
    /// therefore does use that same PRIMARY statistic.
    #[test]
    fn secondary_index_statistics_combine_with_a_remaining_column_node() {
        let mut table = KvTable::with_storage(
            81,
            vec![
                long_column("w_id", 1),
                long_column("d_id", 2),
                long_column("c_id", 3),
            ],
            Box::new(MemTableStorage::new()),
        );
        table.set_common_handle_offsets(vec![0, 1, 2]);
        table.add_index(
            KvIndex {
                id: 7,
                name: "PRIMARY".to_owned(),
                comment: String::new(),
                unique: true,
                column_offsets: vec![0, 1, 2],
                prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 3],
                visible: true,
                global: false,
                clustered_primary: false,
            },
            false,
        );
        table.add_index(
            KvIndex {
                id: 8,
                name: "idx_ab".to_owned(),
                comment: String::new(),
                unique: false,
                column_offsets: vec![0, 1],
                prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 2],
                visible: true,
                global: false,
                clustered_primary: false,
            },
            false,
        );

        let column_stats = |id| ColumnStats {
            histogram: tidb_stats::Histogram {
                id,
                ndv: 10,
                buckets: vec![tidb_stats::Bucket {
                    count: 100,
                    repeat: 10,
                    ndv: 10,
                    lower_bound: Datum::Int(1),
                    upper_bound: Datum::Int(10),
                }],
                ..tidb_stats::Histogram::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        let index_stats = |id, point: &[Datum], count, num_columns, unique| {
            let mut topn = tidb_stats::cmsketch::TopN::new(1);
            topn.append(&tidb_codec::encode_key(point).unwrap(), count);
            topn.sort();
            IndexStats {
                histogram: tidb_stats::Histogram {
                    id,
                    ndv: 20,
                    buckets: vec![tidb_stats::Bucket {
                        count: 100 - count as i64,
                        repeat: 5,
                        ndv: 20,
                        lower_bound: Datum::Bytes(
                            tidb_codec::encode_key(
                                &std::iter::repeat_n(Datum::Int(1), num_columns)
                                    .collect::<Vec<_>>(),
                            )
                            .unwrap(),
                        ),
                        upper_bound: Datum::Bytes(
                            tidb_codec::encode_key(
                                &std::iter::repeat_n(Datum::Int(10), num_columns)
                                    .collect::<Vec<_>>(),
                            )
                            .unwrap(),
                        ),
                    }],
                    ..tidb_stats::Histogram::default()
                },
                topn: Some(topn),
                cms: None,
                stats_ver: 2,
                num_columns,
                unique,
            }
        };
        let stats = TableStatistics::new(
            100,
            0,
            BTreeMap::from([
                (1, column_stats(1)),
                (2, column_stats(2)),
                (3, column_stats(3)),
            ]),
            BTreeMap::from([
                (
                    7,
                    index_stats(
                        7,
                        &[Datum::Int(1), Datum::Int(2), Datum::Int(3)],
                        1,
                        3,
                        true,
                    ),
                ),
                (
                    8,
                    index_stats(8, &[Datum::Int(1), Datum::Int(2)], 20, 2, false),
                ),
            ]),
        );
        let equality = |name: &str, value: i64| {
            tidb_ast::Expr::Binary(
                tidb_ast::BinaryOp::Eq,
                Box::new(tidb_ast::Expr::Column(vec![name.to_owned()])),
                Box::new(tidb_ast::Expr::Int(value.to_string())),
            )
        };
        let conditions = [
            equality("w_id", 1),
            equality("d_id", 2),
            equality("c_id", 3),
        ];
        let conjuncts: Vec<&tidb_ast::Expr> = conditions.iter().collect();

        let actual = selectivity_of_conjuncts(
            &conjuncts,
            &table,
            &NamedColumnResolver { table: &table },
            Some(&stats),
        );
        assert!((actual - 0.02).abs() < 1e-12, "{actual} != 0.02");

        let without_paths = selectivity_of_conjuncts_without_paths(
            &conjuncts,
            &table,
            &NamedColumnResolver { table: &table },
            Some(&stats),
        );
        assert!(
            (without_paths - 0.01).abs() < 1e-12,
            "{without_paths} != 0.01"
        );
    }

    /// `cardinality.Selectivity` treats `IS NOT NULL` as the pseudo column
    /// range `(NULL,+inf]`. It is not an index access condition, but it still
    /// owns a statistics node and therefore removes one pseudo NULL bucket.
    #[test]
    fn pseudo_not_null_is_a_column_statistics_range() {
        let table = KvTable::with_storage(
            82,
            vec![long_column("a", 1), long_column("b", 2)],
            Box::new(MemTableStorage::new()),
        );
        let predicate = tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new(tidb_ast::Expr::Binary(
                tidb_ast::BinaryOp::Eq,
                Box::new(tidb_ast::Expr::Column(vec!["a".to_owned()])),
                Box::new(tidb_ast::Expr::Int("1".to_owned())),
            )),
            Box::new(tidb_ast::Expr::Is {
                expr: Box::new(tidb_ast::Expr::Column(vec!["b".to_owned()])),
                target: tidb_ast::IsTarget::Null,
                not: true,
            }),
        );

        let actual = selectivity(
            &predicate,
            &table,
            &NamedColumnResolver { table: &table },
            None,
        );
        let expected = 1.0 / 1_000.0 * (1.0 - 1.0 / 1_000.0);
        assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
    }
}
