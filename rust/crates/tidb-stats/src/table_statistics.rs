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

//! Go `pkg/statistics`' table, column and index statistics as the estimator
//! reads them (`statistics.Table`, `Column`, `Index`, `HistColl`), with
//! `HistColl.GetAnalyzeRowCount`. The planner's cardinality estimation over
//! them stays in tidb-planner.

use std::collections::{BTreeMap, BTreeSet};

use crate::column::{Column, ColumnInfo};
use crate::index::{Index, IndexInfo};

/// Go `statistics.Version2`.
pub const VERSION2: i64 = 2;

/// The fields of Go's `statistics.Table` (as `GetStatsTable` answers it) that
/// `initStats` reads. `None` at the call site is a fresh `PseudoTable`.
pub trait TableStatsSnapshot {
    /// Go `HistColl.Pseudo` of the table this statement plans from.
    fn pseudo(&self) -> bool;
    /// Go `HistColl.RealtimeCount`.
    fn row_count(&self) -> i64;
    /// Go `HistColl.ModifyCount`.
    fn modify_count(&self) -> i64;
    /// Go `statistics.Table.Version`.
    fn version(&self) -> u64;
    /// Go `HistColl.StatsVer`.
    fn stats_ver(&self) -> i64;
    /// Go `HistColl.Columns`, by column ID.
    fn columns(&self) -> &BTreeMap<i64, Column>;
    /// Go `HistColl.Indices`, by index ID.
    fn indexes(&self) -> &BTreeMap<i64, Index>;
    /// Go `Column.StatsLoadedStatus`, by column ID.
    fn column_load_status(&self) -> &BTreeMap<i64, crate::StatsLoadedStatus>;
    /// Go `Index.StatsLoadedStatus`, by index ID.
    fn index_load_status(&self) -> &BTreeMap<i64, crate::StatsLoadedStatus>;
    /// Go `ColAndIdxExistenceMap`'s index entries: whether the histogram row
    /// is analyzed.
    fn index_stats_existence(&self) -> &BTreeMap<i64, bool>;
    /// The indexes whose Go `Index.Info.MVIndex` is set: their histograms
    /// count array elements, not rows.
    fn mv_index_ids(&self) -> &BTreeSet<i64>;
}

/// Go `HistColl.GetAnalyzeRowCount`: the row count observed by the first
/// fully loaded analyzed histogram, or `-1` when the table has none.
#[must_use]
pub fn analyze_row_count(s: &dyn TableStatsSnapshot) -> f64 {
    for (id, column) in s.columns() {
        if s.column_load_status()
            .get(id)
            .is_some_and(|status| status.is_full_load())
        {
            return column.total_row_count();
        }
    }
    for (id, index) in s.indexes() {
        if s.mv_index_ids().contains(id) {
            continue;
        }
        if s.index_load_status()
            .get(id)
            .is_some_and(|status| status.is_full_load())
        {
            return index.total_row_count();
        }
    }
    -1.0
}

/// One table's loaded statistics, in the shape the estimator reads them.
///
/// This is `statistics.Table` reduced to the fields
/// [`crate::cardinality`] actually consumes. It is built once, at the
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
    pub columns: BTreeMap<i64, Column>,
    /// Index statistics by index ID.
    pub indexes: BTreeMap<i64, Index>,
    /// Go `Column.FMSketch`, retained for global partition-stat merges.
    pub column_fm_sketches: BTreeMap<i64, crate::FmSketch>,
    /// Go `Index.FMSketch`, retained for global partition-stat merges.
    pub index_fm_sketches: BTreeMap<i64, crate::FmSketch>,
    /// Go `Column.StatsLoadedStatus`, retained separately from the estimator's
    /// reduced column shape.
    pub column_load_status: BTreeMap<i64, crate::StatsLoadedStatus>,
    /// Go `Index.StatsLoadedStatus`, retained separately from the estimator's
    /// reduced index shape.
    pub index_load_status: BTreeMap<i64, crate::StatsLoadedStatus>,
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
    pub fn column_for_estimation(&self, id: i64) -> Option<&Column> {
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
    pub fn analyze_row_count(&self) -> f64 {
        analyze_row_count(self)
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
            && self.modify_count as f64 / row_count > crate::RATIO_OF_PSEUDO_ESTIMATE.load()
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
        columns: BTreeMap<i64, Column>,
        indexes: BTreeMap<i64, Index>,
    ) -> Self {
        let column_load_status = columns
            .keys()
            .map(|id| (*id, crate::StatsLoadedStatus::full_load()))
            .collect();
        let index_load_status = indexes
            .keys()
            .map(|id| (*id, crate::StatsLoadedStatus::full_load()))
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
        columns: BTreeMap<i64, crate::StatsLoadedStatus>,
        indexes: BTreeMap<i64, crate::StatsLoadedStatus>,
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
        columns: BTreeMap<i64, crate::FmSketch>,
        indexes: BTreeMap<i64, crate::FmSketch>,
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

}

impl TableStatsSnapshot for TableStatistics {
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
    fn columns(&self) -> &BTreeMap<i64, Column> {
        &self.columns
    }
    fn indexes(&self) -> &BTreeMap<i64, Index> {
        &self.indexes
    }
    fn column_load_status(&self) -> &BTreeMap<i64, crate::StatsLoadedStatus> {
        &self.column_load_status
    }
    fn index_load_status(&self) -> &BTreeMap<i64, crate::StatsLoadedStatus> {
        &self.index_load_status
    }
    fn index_stats_existence(&self) -> &BTreeMap<i64, bool> {
        &self.index_stats_existence
    }
    fn mv_index_ids(&self) -> &BTreeSet<i64> {
        &self.mv_index_ids
    }
}

/// Go `CollectColumnStatsUsage`'s statistics-loading result.
#[derive(Debug, Default)]
pub struct ColumnStatsUsage {
    /// Base-table column -> whether its complete histogram is required.
    pub predicate_columns: std::collections::HashMap<tidb_model::TableItemID, bool>,
    /// Logical table IDs visited by data sources.
    pub visited_logical_table_ids: std::collections::HashSet<i64>,
    /// Logical table ID -> physical partition IDs, for static pruning.
    pub table_partition_ids: std::collections::HashMap<i64, Vec<i64>>,
    /// Physical table ID -> index IDs retained by Go's first pruning phase.
    /// A table is present only when pruning actually removed a path.
    pub kept_index_ids: std::collections::HashMap<i64, std::collections::HashSet<i64>>,
    /// Number of logical operators visited.
    pub operator_count: u64,
}

/// The common planner view when the caller owns Go `TableInfo` rather than
/// an executor `KvTable`. Tuple fields are column `(id, unsigned)` and index
/// `(id, column_count, unique)` metadata.
pub fn table_statistics_from_table_schema(
    stats: &crate::Table,
    schema_columns: &[(i64, bool)],
    schema_indexes: &[(i64, usize, bool)],
) -> TableStatistics {
    let existence = stats.existence_map.as_ref().map(|map| {
        map.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    });
    // Go keeps ColAndIdxExistenceMap independently of resident histograms.
    // Unloaded analyzed items must remain discoverable by the load collector.
    let mut column_stats_existence = schema_columns
        .iter()
        .filter(|(id, _)| existence.as_ref().is_some_and(|map| map.has(*id, false)))
        .map(|(id, _)| {
            (
                *id,
                existence
                    .as_ref()
                    .is_some_and(|map| map.has_analyzed(*id, false)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut index_stats_existence = schema_indexes
        .iter()
        .filter(|(id, _, _)| existence.as_ref().is_some_and(|map| map.has(*id, true)))
        .map(|(id, _, _)| {
            (
                *id,
                existence
                    .as_ref()
                    .is_some_and(|map| map.has_analyzed(*id, true)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut columns = BTreeMap::new();
    let mut indexes = BTreeMap::new();
    let mut column_fm_sketches = BTreeMap::new();
    let mut index_fm_sketches = BTreeMap::new();
    let mut column_load_status = BTreeMap::new();
    let mut index_load_status = BTreeMap::new();
    let mut mv_index_ids = std::collections::BTreeSet::new();
    for column in stats.hist_coll.stable_columns() {
        let column = column
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = column.item_id();
        column_stats_existence.insert(
            id,
            existence
                .as_ref()
                .is_some_and(|map| map.has_analyzed(id, false)),
        );
        // Go keeps EmptyColumn objects: presence selects the ordinary
        // Selectivity path even when the histogram is not initialized.
        let unsigned = schema_columns
            .iter()
            .find(|schema| schema.0 == id)
            .is_some_and(|schema| schema.1);
        columns.insert(
            id,
            Column {
                info: Some(ColumnInfo {
                    flag: if unsigned {
                        tidb_mysql::UnsignedFlag
                    } else {
                        0
                    },
                    ..column.info.clone().unwrap_or_default()
                }),
                fm_sketch: None,
                ..column.clone()
            },
        );
        if let Some(sketch) = column.fm_sketch.clone() {
            column_fm_sketches.insert(id, sketch);
        }
        column_load_status.insert(id, column.stats_loaded_status);
    }
    for index in stats.hist_coll.stable_indices() {
        let index = index
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = index.item_id();
        index_stats_existence.insert(
            id,
            existence
                .as_ref()
                .is_some_and(|map| map.has_analyzed(id, true)),
        );
        // Preserve unloaded index placeholders and their original load state.
        let Some(schema) = schema_indexes.iter().find(|schema| schema.0 == id) else {
            continue;
        };
        indexes.insert(
            id,
            Index {
                info: Some(IndexInfo {
                    unique: schema.2,
                    ..index.info.clone().unwrap_or_default()
                }),
                fm_sketch: None,
                ..index.clone()
            },
        );
        if let Some(sketch) = index.fm_sketch.clone() {
            index_fm_sketches.insert(id, sketch);
        }
        index_load_status.insert(id, index.stats_loaded_status);
        if index.info.as_ref().is_some_and(|info| info.mv_index) {
            mv_index_ids.insert(id);
        }
    }
    let mut statistics = TableStatistics::new(
        stats.hist_coll.realtime_count,
        stats.hist_coll.modify_count,
        columns,
        indexes,
    )
    .with_stat_versions(stats.version, stats.last_analyze_version)
    // Go `StatsInfo.StatsVersion` is `HistColl.StatsVer`, the per-object
    // stats version, NOT `Table.Version` (the stats_meta TSO). A loaded dump
    // whose columns carry `stats_ver = 2` therefore has a real distribution
    // even when its meta version is unset.
    .with_stats_ver(i64::from(stats.hist_coll.stats_version))
    .with_load_statuses(column_load_status, index_load_status)
    .with_stats_existence(column_stats_existence, index_stats_existence)
    .with_fm_sketches(column_fm_sketches, index_fm_sketches);
    // Go `GetStatsTable` marks the planner copy pseudo when the canonical
    // table has no initialized column or index statistics. Do not infer this
    // from the reduced maps above: unloaded placeholder items can still have
    // histogram metadata and therefore survive that conversion.
    // Pinned `GetStatsTable` rule 2 returns a query-time pseudo table whenever
    // the canonical cache object's realtime count is zero, including an
    // initialized, explicitly analyzed empty table. Keep the cache identity
    // (`cache_pseudo`) separate from that planner decision.
    statistics.pseudo =
        stats.hist_coll.pseudo || stats.hist_coll.realtime_count == 0 || !stats.is_initialized();
    statistics.cache_pseudo = stats.hist_coll.pseudo;
    statistics.mv_index_ids = mv_index_ids;
    statistics
}

