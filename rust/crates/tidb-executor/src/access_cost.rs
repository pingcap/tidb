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

//! Which way to read one base table, decided by cost rather than by
//! declaration order: Go's `findBestTask` over a `DataSource`'s access paths,
//! narrowed to the paths this tier can execute.
//!
//! # The shape, and where each half comes from
//!
//! 1. **Enumerate.** [`enumerate_paths`] builds one candidate per readable
//!    path: the full scan, plus every index whose leading column the `WHERE`
//!    constrains (Go's `DeriveStats`/`deriveIndexPathStats` over
//!    `ranger.DetachCondAndBuildRangeForIndex`, `pkg/util/ranger/detacher.go`).
//! 2. **Estimate.** Each candidate's `countAfterAccess` comes from the ONE
//!    estimator this rewrite has, [`tidb_planner::cardinality`]: an index
//!    range through `get_index_row_count_for_stats_v2`
//!    (`row_count_index.go`), a full scan through the table's realtime row
//!    count (`stats_meta.count`, Go's `HistColl.RealtimeCount`). No second
//!    estimation path is written here.
//! 3. **Cost.** [`AccessPath::cost`] is `plan_cost_ver2.go`'s formula for the
//!    reader the path lowers to -- `PhysicalTableReader` over a
//!    `PhysicalTableScan`, `PhysicalIndexReader` over a covering
//!    `PhysicalIndexScan`, `PhysicalIndexLookUpReader` otherwise -- because
//!    the double read is exactly what makes a broad index lose to a full
//!    scan, and a model without it would always pick the index.
//! 4. **Prune.** [`crate::skyline`] runs Go's `skylinePruning` /
//!    `compareCandidates` BEFORE the minimum is taken, dropping any candidate
//!    another one dominates on a partial order of structural facts -- access
//!    columns, index-back scan, `=`/`IN` count. Two candidates that both
//!    estimate one row are separable there and nowhere else. That module's
//!    doc names which of Go's dimensions are live and which are excluded.
//! 5. **Choose.** [`choose_access_path`] takes the strict minimum over the
//!    survivors, keeping the incumbent on a tie the way Go's
//!    `compareTaskCost` does.
//!
//! # Pseudo statistics
//!
//! With no [`TableStatistics`] the table is Go's `statistics.PseudoTable`:
//! `PSEUDO_ROW_COUNT` rows, and the pseudo selectivity rates in
//! [`crate::plan_trace`]. The candidates and the cost formula are the same;
//! only the numbers feeding them change, and the chosen path is then marked
//! [`ScanEstimate::pseudo`] so `EXPLAIN` still prints `stats:pseudo` exactly
//! where Go prints it (`physical_table_scan.go`'s
//! `StatsVersion == statistics.PseudoVersion`).
//!
//! # Excluded from enumeration, deliberately
//!
//! Go enumerates more paths than this. Each one below is left OUT rather
//! than costed with a formula this tier cannot honour:
//!
//! * **Index merge** (`generateIndexMergePath`): needs a union/intersection
//!   reader this tier has no executor for, so a costed candidate could be
//!   chosen and then not run.
//! * **Multi-valued indexes** (`generateMVIndexPartialPath`): the JSON path
//!   extraction and its `IndexMerge`-only lowering are both absent.
//! * **Partitioned tables**: the session catalog refuses them upstream
//!   (`cluster_session.rs`), so there is never a partition to prune.
//! * **TiFlash / MPP paths**: no columnar replica is read by this node, and
//!   `getTaskScanFactorVer2` would have to pick a store this tier cannot
//!   address.
//! * **Point get / batch point get**: Go decides these in `TryFastPlan`,
//!   BEFORE cost-based selection, and so does [`crate::driver::access`].
//!   They are not costed here for the same reason Go does not cost them.
//!
//! # The case that made pruning necessary
//!
//! On `t(id PK, bucket, rare, payload, KEY idx_bucket(bucket),
//! KEY idx_rare(rare), KEY idx_cover(bucket, rare))` with 2000 analyzed rows,
//! a v8.5.6 playground answers
//!
//! ```text
//! SELECT * FROM t WHERE bucket = 1 AND rare = 7
//!   IndexRangeScan  1.00  index:idx_cover(bucket, rare)  range:[1 7,1 7]
//! ```
//!
//! while costing every candidate picked `idx_rare(rare)`. Both estimate one
//! row, so no cost formula can separate them: the narrower single-column
//! index simply has the smaller index row size and wins. Go never costs
//! `idx_rare` at all, because `idx_cover`'s access conditions are a STRICT
//! SUPERSET of it. That is a pruning answer, not a costing one, which is why
//! it is [`crate::skyline`] and not a tie-break rule here.
//!
//! # The covering index the ranger narrowed nothing on
//!
//! Go keeps an index path with NO access conditions when it COVERS the
//! statement (`keepIndex := len(path.AccessConds) > 0 || ... ||
//! path.IsSingleScan`), gives it `ranger.FullRange()`, and prints it as
//! `IndexFullScan`. [`full_scan_candidate`] builds that candidate
//! here, including for a statement with no `WHERE` at all. Captured on a v8.5
//! `gorun` session over
//! `q(id BIGINT PRIMARY KEY, score BIGINT, note VARCHAR(8), KEY s(score))`:
//!
//! ```text
//! explain SELECT id FROM q
//!   IndexFullScan_6  cop[tikv]  table:q, index:s(score)  keep order:false, stats:pseudo
//! explain SELECT id FROM q WHERE note = 'x'
//!   TableFullScan_9  cop[tikv]  table:q                  keep order:false, stats:pseudo
//! ```
//!
//! -- the second one shows the coverage test is what decides: `note` is
//! neither in `s` nor the handle, so no index path can answer it alone.
//!
//! This changes the ROW ORDER of such a read, from handle order to index-key
//! order, which is TiDB's own order and was a silent divergence before.
//! Captured on the same session over
//! `t(a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT, KEY kb(b))` holding
//! `(1,'xy'),(2,'Yz'),(3,'z')`:
//!
//! ```text
//! SELECT group_concat(b) FROM t WHERE b LIKE '%'  ->  Yz,xy,z
//! SELECT group_concat(a) FROM t WHERE b LIKE '%'  ->  2,1,3
//! ```
//!
//! A covering index can also supply descending order. Like Go, the executor
//! reads it backwards for `ORDER BY ... DESC`, preserves that physical
//! property (`keep order:true, desc`), and can stop at the pushed `LIMIT`.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use tidb_chunk::codec::estimate_type_width;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_planner::cardinality::pseudo::{
    pseudo_row_count_by_index_ranges, pseudo_selectivity, IndexRange as PseudoIndexRange,
    PseudoBoundKind, PseudoColumn, PseudoFunctionKind, PseudoIndex, PseudoPredicate, ScalarRange,
};
use tidb_planner::cardinality::row_count_column::RowEstimate;
use tidb_planner::cardinality::row_count_estimator::{
    get_index_row_count_for_stats_v2, get_row_count_by_column_ranges, ColumnRange, ColumnStats,
    EstimatorOptions, IndexRangeDatums, IndexStats,
};
use tidb_planner::cardinality::row_size::{
    get_index_avg_row_size, get_table_avg_row_size, RowSizeColumn, RowSizeColumnStats,
    RowSizeStore, RowSizeType,
};
use tidb_planner::cost_factors::TOLERANCE_FACTOR;
use tidb_planner::selectivity_greedy::{
    combine_selectivity, ConditionKind, SelectivityDefaults, StatsNode, StatsNodeType,
};

use crate::kv_table::{IndexRange, KvColumn, KvIndex, KvTable};
use crate::plan_trace::PSEUDO_ROW_COUNT;
use crate::skyline::{skyline_pruning, Candidate, ColSet, PruningContext};

mod handle_key_part;
use handle_key_part::{appended_handle_column, prune_estimate_range};

/// Go `defaultVer2Factors.TiKVScan`.
const TIKV_SCAN_FACTOR: f64 = 40.70;
/// Go `defaultVer2Factors.TiDB2KVNet`.
const TIDB_KV_NET_FACTOR: f64 = 3.96;
/// Go `defaultVer2Factors.TiKVCPU`.
const TIKV_CPU_FACTOR: f64 = 49.90;
/// Go `defaultVer2Factors.TiDBRequest`.
const TIDB_REQUEST_FACTOR: f64 = 6_000_000.00;
/// Go `vardef.DefDistSQLScanConcurrency`.
const DIST_SQL_SCAN_CONCURRENCY: f64 = 15.0;
/// Go `SessionVars.IndexLookupConcurrency()`, which falls back to
/// `DefExecutorConcurrency` because `DefIndexLookupConcurrency` is unset.
const INDEX_LOOKUP_CONCURRENCY: f64 = 5.0;
/// The table-side requests one double read issues PER INDEX ROW, as Go's
/// cost model counts them.
///
/// Go writes this in `getPlanCostVer24PhysicalIndexLookUpReader` as
/// `doubleReadTasks = doubleReadRows / batchSize * taskPerBatch`, with
/// `batchSize = SessionVars.IndexLookupSize` (`vardef.DefIndexLookupSize`,
/// 20000) and `taskPerBatch = 32`: `32/20000` requests per row.
///
/// It is not a free constant -- it is a claim about Go's reader, and Go's
/// reader honours it. `IndexLookUpExecutor.fetchHandles` gathers handles
/// until it has `IndexLookupSize` of them, `buildTableReader` hands the whole
/// batch to `buildTableReaderFromHandles`, and that turns the batch into ONE
/// distsql request (`RequestBuilder.SetTableHandles`, `pkg/executor/
/// builder.go`). Per row, the request count is the batching factor's
/// reciprocal.
///
/// # THE READER THIS PRICES IS NOT THE READER WE RUN
///
/// `IndexRangeSourceExec` in [`crate::access_path`] calls
/// `KvTable::get_row_by_handle` once per index entry -- one
/// `TableStorage::get`, and against `ClusterTableStorage` one snapshot round
/// trip to TiKV, PER ROW. `access_path::tests::
/// the_double_read_issues_one_point_get_per_index_row` pins that: N index
/// rows, N point gets, where Go issues one request for the batch.
///
/// The obvious repricing -- charge `1.0` request per row, since that is what
/// we issue -- was tried against a live v8.5.6 playground and is WRONG. It
/// makes a one-row lookup cost more than a full scan of 2000 rows, and
/// `scripts/run-realtikv-access-path.sh` reported five cases where Go reads
/// through an index and this node would then have chosen a full scan
/// (`SELECT * FROM t WHERE rare = 7` among them). One round trip cannot cost
/// more than reading two thousand rows, and a model that says so is a
/// different lie, not a repair.
///
/// The reason it breaks there and only there is the term's two regimes, both
/// pinned by `tests::the_table_side_not_the_request_term_is_what_costs_a_
/// double_read`. On a SELECTIVE path the request term is nearly the entire
/// price of the lookup, so multiplying it by the batching factor moves that
/// path by the same factor. At the CROSSOVER it is the minority: the table
/// side's per-row scan and net costs dominate, and those are ported at full
/// rate with no batching discount of any kind. So the crossover is Go's
/// wherever the constant sits, and inflating the constant cannot improve it
/// -- it can only push selective paths, the ones Go and this node already
/// agree on, over to the scan.
///
/// The same differential confirms it from the other end: across a 50000-row
/// table at one row, 1/50th and a quarter, both planners choose the SAME path
/// at every point. The under-counted term costs LATENCY on plans that are
/// correctly chosen, not wrong plans.
///
/// That makes the fix an executor change, not a cost change: batch the
/// handles and issue one read per batch, as Go does. When that lands this
/// constant already describes the reader, and nothing here has to move.
const DOUBLE_READ_REQUESTS_PER_ROW: f64 = 32.0 / 20000.0;
/// Go `plan_cost_ver2.go`'s `MinNumRows`.
const MIN_NUM_ROWS: f64 = 1.0;
/// Go `plan_cost_ver2.go`'s `MinRowSize`.
const MIN_ROW_SIZE: f64 = 2.0;
/// Go `plan_cost_ver2.go`'s `MaxPenaltyRowCount`: the floor on the extra rows
/// [`table_scan_penalty_rows`] charges a risky full scan.
const MAX_PENALTY_ROW_COUNT: f64 = 1000.0;

/// The full statistics items resident in one stats-cache table version.
///
/// Go keeps this state on the domain's shared `statistics.Table`, so a load
/// triggered while planning one statement is visible to later statements and
/// connections until that table version is replaced. Rust eagerly carries the
/// histogram payloads, but this state preserves the same residency contract
/// for cardinality decisions that depend on `IsFullLoad`.
#[derive(Debug, Default)]
pub struct StatsLoadState {
    loaded: Mutex<LoadedStatistics>,
}

/// A statement-planning checkpoint for one table's shared statistics cache.
///
/// Physical candidates are speculative: Go derives the logical statistics once
/// and only publishes physical-access loads after the statement. Rust's
/// StreamAgg/HashAgg and derived-source probes can otherwise make one branch's
/// residency visible while another branch is still being costed.
#[derive(Clone, Debug)]
pub(crate) struct StatsLoadCheckpoint {
    loaded: LoadedStatistics,
}

#[derive(Clone, Debug, Default)]
struct LoadedStatistics {
    columns: BTreeSet<i64>,
    indexes: BTreeSet<i64>,
    column_order: Vec<i64>,
    index_order: Vec<i64>,
    pending_columns: BTreeSet<i64>,
    pending_indexes: BTreeSet<i64>,
    pending_column_order: Vec<i64>,
    pending_index_order: Vec<i64>,
}

impl StatsLoadState {
    pub(crate) fn checkpoint(&self) -> StatsLoadCheckpoint {
        let loaded = self
            .loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        StatsLoadCheckpoint {
            loaded: loaded.clone(),
        }
    }

    pub(crate) fn restore(&self, checkpoint: &StatsLoadCheckpoint) {
        *self
            .loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = checkpoint.loaded.clone();
    }

    /// Atomically publishes one statement's loads and returns the cumulative
    /// cache contents. `fallback_column` implements Go's determinate-mode rule:
    /// when the statement has no analyzed full-load predicate and the table has
    /// no resident full item at all, load its first analyzed public column.
    pub(crate) fn mark_and_snapshot(
        &self,
        mut columns: BTreeSet<i64>,
        indexes: BTreeSet<i64>,
        fallback_column: Option<i64>,
    ) -> (BTreeSet<i64>, BTreeSet<i64>) {
        let mut loaded = self
            .loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if columns.is_empty() && loaded.columns.is_empty() && loaded.indexes.is_empty() {
            columns.extend(fallback_column);
        }
        for id in columns {
            if loaded.columns.insert(id) {
                loaded.column_order.push(id);
            }
        }
        for id in indexes {
            if loaded.indexes.insert(id) {
                loaded.index_order.push(id);
            }
        }
        (loaded.columns.clone(), loaded.indexes.clone())
    }

    /// Queues statistics touched while costing a physical access path.
    /// Logical derivation has already taken its snapshot when this runs, so
    /// these items affect later statements without retroactively changing the
    /// statement that triggered Go's asynchronous load.
    fn mark_accessed(&self, columns: BTreeSet<i64>, indexes: BTreeSet<i64>) {
        let mut loaded = self
            .loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for id in columns {
            if loaded.pending_columns.insert(id) {
                loaded.pending_column_order.push(id);
            }
        }
        for id in indexes {
            if loaded.pending_indexes.insert(id) {
                loaded.pending_index_order.push(id);
            }
        }
    }

    fn advance(&self) {
        let mut loaded = self
            .loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending_columns = std::mem::take(&mut loaded.pending_columns);
        let pending_indexes = std::mem::take(&mut loaded.pending_indexes);
        let pending_column_order = std::mem::take(&mut loaded.pending_column_order);
        let pending_index_order = std::mem::take(&mut loaded.pending_index_order);
        for id in pending_column_order {
            if pending_columns.contains(&id) && loaded.columns.insert(id) {
                loaded.column_order.push(id);
            }
        }
        for id in pending_index_order {
            if pending_indexes.contains(&id) && loaded.indexes.insert(id) {
                loaded.index_order.push(id);
            }
        }
    }

    #[cfg(test)]
    fn snapshot(&self) -> (BTreeSet<i64>, BTreeSet<i64>) {
        let loaded = self
            .loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (loaded.columns.clone(), loaded.indexes.clone())
    }

    #[cfg(test)]
    fn pending(&self) -> (BTreeSet<i64>, BTreeSet<i64>) {
        let loaded = self
            .loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            loaded.pending_columns.clone(),
            loaded.pending_indexes.clone(),
        )
    }

    fn ordered_columns(&self, requested: &BTreeSet<i64>) -> Vec<i64> {
        self.ordered_ids(requested, false)
    }

    fn ordered_indexes(&self, requested: &BTreeSet<i64>) -> Vec<i64> {
        self.ordered_ids(requested, true)
    }

    fn ordered_ids(&self, requested: &BTreeSet<i64>, indexes: bool) -> Vec<i64> {
        let loaded = self
            .loaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (resident, order) = if indexes {
            (&loaded.indexes, &loaded.index_order)
        } else {
            (&loaded.columns, &loaded.column_order)
        };
        let mut ordered = Vec::with_capacity(requested.len());
        for id in order {
            if requested.contains(id) {
                ordered.push(*id);
            }
        }
        // Callers that deliberately model an eager payload (for example the
        // physical index-join floor) may request IDs that were never observed
        // by the shared residency state. Keep those IDs deterministic.
        for id in requested {
            if !resident.contains(id) || !ordered.contains(id) {
                ordered.push(*id);
            }
        }
        ordered
    }
}

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
    /// Column statistics by column ID.
    pub columns: BTreeMap<i64, ColumnStats>,
    /// Index statistics by index ID.
    pub indexes: BTreeMap<i64, IndexStats>,
    /// Go domain-cache residency for this exact statistics-table version.
    load_state: Arc<StatsLoadState>,
}

impl TableStatistics {
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
    /// Its third route, `IsOutdated`, is gated on
    /// `tidb_enable_pseudo_for_outdated_stats`, whose default is `false`
    /// (`vardef.DefTiDBEnablePseudoForOutdatedStats`), so it never fires here.
    #[must_use]
    pub fn new(
        row_count: i64,
        modify_count: i64,
        columns: BTreeMap<i64, ColumnStats>,
        indexes: BTreeMap<i64, IndexStats>,
    ) -> Self {
        Self {
            pseudo: row_count == 0 || (columns.is_empty() && indexes.is_empty()),
            row_count,
            modify_count,
            columns,
            indexes,
            load_state: Arc::default(),
        }
    }

    /// Installs the state owned by the shared cluster statistics snapshot.
    #[must_use]
    pub fn with_shared_load_state(mut self, load_state: Arc<StatsLoadState>) -> Self {
        self.load_state = load_state;
        self
    }

    pub(crate) fn mark_loaded_statistics(
        &self,
        columns: BTreeSet<i64>,
        indexes: BTreeSet<i64>,
        fallback_column: Option<i64>,
    ) -> (BTreeSet<i64>, BTreeSet<i64>) {
        self.load_state
            .mark_and_snapshot(columns, indexes, fallback_column)
    }

    pub(crate) fn mark_accessed_statistics(&self, columns: BTreeSet<i64>, indexes: BTreeSet<i64>) {
        self.load_state.mark_accessed(columns, indexes);
    }

    pub(crate) fn advance_statistics_loads(&self) {
        self.load_state.advance();
    }

    pub(crate) fn load_checkpoint(&self) -> StatsLoadCheckpoint {
        self.load_state.checkpoint()
    }

    pub(crate) fn restore_load_checkpoint(&self, checkpoint: &StatsLoadCheckpoint) {
        self.load_state.restore(checkpoint);
    }

    #[cfg(test)]
    pub(crate) fn loaded_statistics(&self) -> (BTreeSet<i64>, BTreeSet<i64>) {
        self.load_state.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn pending_statistics(&self) -> (BTreeSet<i64>, BTreeSet<i64>) {
        self.load_state.pending()
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
        let ordered_indexes = self.load_state.ordered_indexes(full_loaded_indexes);
        let ordered_columns = self.load_state.ordered_columns(full_loaded_columns);
        let analyzed_count = if full_loaded_columns.contains(&column_id) {
            column.total_row_count() as i64
        } else {
            ordered_indexes
                .iter()
                .find_map(|id| {
                    self.indexes.get(id).and_then(|index| {
                        (index.histogram.last_update_version == version)
                            .then(|| index.total_row_count() as i64)
                    })
                })
                .unwrap_or_else(|| {
                    ordered_columns
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

impl ScanEstimate {
    /// The stats-less estimate, for a table that has never been analyzed.
    #[must_use]
    pub(crate) const fn pseudo(rows: f64) -> Self {
        Self { rows, pseudo: true }
    }
}

/// Go `StatsInfo::ScaleByExpectCnt`'s row-count result for a physical scan.
///
/// The access path retains its own (possibly fractional) estimate for path
/// comparison. The physical TableScan/IndexScan is initialized from the
/// table's statistics and Go deliberately refuses to scale a table of at
/// most one estimated row any lower, avoiding an unstable division by a
/// tiny cardinality.
fn physical_scan_row_count(table_rows: f64, path_rows: f64) -> f64 {
    if path_rows >= table_rows || table_rows <= 1.0 {
        table_rows
    } else {
        path_rows
    }
}

/// The `LIMIT` a candidate index path may carry into its cost, Go's
/// `PhysicalIndexLookUpReader.PushedLimit` / `PhysicalIndexReader`'s
/// `ExpectedCnt` property.
///
/// This is what makes a small `LIMIT` swing the choice back to an index: Go
/// costs the index side at `min(rows, count)` because the reader stops there,
/// while the full scan under a root `Limit` still pays for every row it reads
/// (`getPlanCostVer24PhysicalTableScan` uses the scan's own cardinality). The
/// asymmetry is the model's, not an approximation.
pub(crate) struct PushedLimit<'a> {
    /// `offset + count`: the rows the reader must still produce.
    pub(crate) cap: f64,
    /// Whether the property carries an ORDER BY. A plain LIMIT still lowers
    /// the expected count but does not activate Go's ordering-risk ratio.
    pub(crate) has_order: bool,
    /// Go `SessionVars.OptOrderingIdxSelRatio`.
    pub(crate) ordering_selectivity_ratio: f64,
    /// Whether an index over these column offsets and exact ranges
    /// establishes the order the `LIMIT` selects from -- Go's physical
    /// `SortItems` property on the index task.
    pub(crate) satisfied_by: &'a dyn Fn(&[usize], &[IndexRange]) -> bool,
}

/// One way of reading the table, with the rows and the cost it was chosen by.
#[derive(Clone, Debug)]
pub(crate) struct AccessPath {
    /// The index this path reads, and the ranges it reads of it; `None` is a
    /// table path, which reads through the row handle.
    pub(crate) index: Option<(i64, Vec<IndexRange>)>,
    /// A table path's ranges over the CLUSTERED INTEGER HANDLE -- Go's
    /// `path.Ranges` for a `tablePath`, which `ranger.BuildTableRange` builds
    /// (see [`crate::handle_range`]).
    ///
    /// `None` is Go's full range, the whole table, which `EXPLAIN` prints as
    /// `TableFullScan`. `Some` is its `TableRangeScan` -- including
    /// `Some(vec![])`, the contradictory `WHERE` no handle satisfies, which
    /// reads nothing rather than everything. Always `None` on an index path,
    /// whose ranges live in [`AccessPath::index`].
    pub(crate) table_ranges: Option<Vec<IndexRange>>,
    /// The estimate `EXPLAIN` prints for the scan node.
    pub(crate) estimate: ScanEstimate,
    /// Go `AccessPath.CountAfterAccess`, before the physical scan applies
    /// `StatsInfo::ScaleByExpectCnt` to produce [`Self::estimate`].
    pub(crate) count_after_access: f64,
    /// `plan_cost_ver2.go`'s cost of the reader this path lowers to.
    pub(crate) cost: f64,
    /// The same scan/reader task before its cost is folded to [`Self::cost`].
    /// Parent physical alternatives use this receipt when they insert a
    /// pushed coprocessor operator between the scan and reader.
    pub(crate) planner_candidate: tidb_planner::candidate_cost::Candidate,
    /// The logical DataSource row count shared by every candidate in this
    /// enumeration. Keeping it on the chosen path lets the driver reuse the
    /// value that was already computed while costing the path.
    pub(crate) source_rows: f64,
    /// How far the estimator admits [`ScanEstimate::rows`] could be wrong, and
    /// the rows left after the index's own filters -- Go's `AccessPath`
    /// `MinCountAfterAccess` / `MaxCountAfterAccess` / `CountAfterIndex`. Only
    /// skyline pruning's risk dimension reads these; the cost formula does
    /// not, exactly as in Go.
    pub(crate) risk: RowRisk,
}

/// Go `GetPlanCostVer24PhysicalIndexMergeReader` for a non-MV, root-level
/// merge. Each partial is an index-only handle path; the merge then performs
/// one table-side read for its deduplicated handles.
pub(crate) fn index_merge_cost(
    table: &KvTable,
    needed_columns: &[usize],
    stats: Option<&TableStatistics>,
    merged_rows: f64,
    partials: &[AccessPath],
) -> f64 {
    let table_columns: Vec<RowSizeColumn> = needed_columns
        .iter()
        .filter_map(|offset| table.columns.get(*offset))
        .map(|column| row_size_column(column, stats))
        .collect();
    let row_size = get_table_avg_row_size(
        &table_columns,
        is_pseudo(stats),
        realtime_row_count(stats) as i64,
        RowSizeStore::TiKv,
        table.pk_handle_offset().is_some(),
        false,
    )
    .max(MIN_ROW_SIZE);
    let rows = merged_rows.max(MIN_NUM_ROWS);
    let table_side =
        (scan_cost(rows, row_size) + net_cost(rows, row_size)) / DIST_SQL_SCAN_CONCURRENCY;
    table_side + partials.iter().map(|path| path.cost).sum::<f64>()
}

/// The bounds around one path's access estimate, Go's `AccessPath`
/// `MinCountAfterAccess` / `MaxCountAfterAccess` / `CountAfterIndex`.
///
/// `Default` is Go's own default: bounds of zero mean the estimator
/// identified no risk for this path, which `compareRiskRatio` reads as the
/// LOWEST risk rather than as a missing value.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct RowRisk {
    /// Go `MinCountAfterAccess`.
    pub(crate) min: f64,
    /// Go `MaxCountAfterAccess`.
    pub(crate) max: f64,
    /// Go `CountAfterIndex`.
    pub(crate) after_index: f64,
}

/// The session inputs Go reads off `PlanContext`; this tier touches none of
/// the risk variables, so they stay at their documented defaults.
fn estimator_options() -> EstimatorOptions {
    EstimatorOptions::default()
}

/// `getAvgRowSize(stats, cols)` for an operator whose `StatsInfo().HistColl`
/// is nil -- Go's own comment on that branch is "Estimate using just the type
/// info", and it sums `chunk.EstimateTypeWidth` over the schema with none of
/// the per-column record overhead the histogram branch adds.
///
/// A join's child is exactly that operator in this tier: no histogram is
/// carried up past a scan, so the type widths are the whole of the estimate.
/// Used by the index-join naming in [`crate::plan_trace`].
pub(crate) fn schema_avg_row_size(types: &[FieldType]) -> f64 {
    let columns: Vec<RowSizeColumn> = types
        .iter()
        .map(|field_type| RowSizeColumn::without_stats(estimate_type_width(field_type) as f64))
        .collect();
    tidb_planner::plan_cost_ver2::plan_avg_row_size(&columns, None)
}

/// Go `getAvgRowSize(ds.StatsInfo(), ds.Schema().Columns)` after logical
/// column pruning. A base-table leaf still owns its `HistColl`, so its
/// physical candidate must use analyzed average widths rather than the
/// static widths used above for operators that no longer carry histograms.
pub(crate) fn data_source_avg_row_size(
    table: &KvTable,
    needed_columns: &[usize],
    stats: Option<&TableStatistics>,
) -> f64 {
    let columns = needed_columns
        .iter()
        .filter_map(|offset| table.columns.get(*offset))
        .map(|column| row_size_column(column, stats))
        .collect::<Vec<_>>();
    tidb_planner::plan_cost_ver2::plan_avg_row_size(
        &columns,
        Some((is_pseudo(stats), realtime_row_count(stats) as i64)),
    )
}

/// Costs the reader task below one dynamic IndexJoin range. `rows` is the
/// number of storage rows one outer key reads before residual filters and
/// `after_filter` is what leaves the coprocessor. `output_row_size` is the
/// pruned logical schema the reader transfers; it is deliberately independent
/// of the scan's on-disk row width.
///
/// This is the same scan, Selection, net, and double-read arithmetic used by
/// [`table_scan_path`] and [`index_path`], without the full-scan penalty because
/// a dynamic range is never full.
pub(crate) fn index_join_probe_cost(
    table: &KvTable,
    object: &crate::access_path::LookupObject,
    rows: f64,
    after_filter: f64,
    needed_columns: &[usize],
    table_scan_columns: &[usize],
    output_row_size: f64,
    filter_count: usize,
    stats: Option<&TableStatistics>,
) -> f64 {
    let realtime = realtime_row_count(stats);
    let rows = rows.max(MIN_NUM_ROWS);
    let selection = rows * filter_count as f64 * TIKV_CPU_FACTOR;
    let transferred = net_cost(after_filter, output_row_size);
    match object {
        crate::access_path::LookupObject::Handle
        | crate::access_path::LookupObject::CommonHandle => {
            // Go's TiKV PhysicalTableScan prices `DataSource.TblCols`, which
            // LogicalSchemaProducer.PruneColumns has already reduced to the
            // source columns this physical probe needs.
            let columns = table_scan_columns
                .iter()
                .filter_map(|offset| table.columns.get(*offset))
                .map(|column| row_size_column(column, stats))
                .collect::<Vec<_>>();
            let row_size = get_table_avg_row_size(
                &columns,
                is_pseudo(stats),
                realtime as i64,
                RowSizeStore::TiKv,
                table.pk_handle_offset().is_some(),
                false,
            )
            .max(MIN_ROW_SIZE);
            (scan_cost(rows, row_size) + selection + transferred) / DIST_SQL_SCAN_CONCURRENCY
        }
        crate::access_path::LookupObject::Index(index_id) => {
            let Some(index) = table.indexes().iter().find(|index| index.id == *index_id) else {
                return f64::INFINITY;
            };
            let index_columns = index
                .column_offsets
                .iter()
                .filter_map(|offset| table.columns.get(*offset))
                .map(|column| row_size_column(column, stats))
                .collect::<Vec<_>>();
            let index_row_size = get_index_avg_row_size(
                &index_columns,
                is_pseudo(stats),
                realtime as i64,
                index.unique,
                false,
            );
            let index_scan =
                scan_cost(rows, index_row_size) + (index.id % 100) as f64 / 1_000_000.0;
            let index_side = (index_scan + selection + transferred) / DIST_SQL_SCAN_CONCURRENCY;
            if index_is_covering(table, *index_id, needed_columns) {
                return index_side;
            }

            // The table side of an IndexLookUp is another TiKV table scan and
            // therefore uses the same unpruned `TblCols` row width.
            let table_columns = needed_columns
                .iter()
                .filter_map(|offset| table.columns.get(*offset))
                .map(|column| row_size_column(column, stats))
                .collect::<Vec<_>>();
            let table_row_size = get_table_avg_row_size(
                &table_columns,
                is_pseudo(stats),
                realtime as i64,
                RowSizeStore::TiKv,
                table.pk_handle_offset().is_some(),
                false,
            )
            .max(MIN_ROW_SIZE);
            let table_side = scan_cost(after_filter, table_row_size) / DIST_SQL_SCAN_CONCURRENCY;
            let double_read_cpu = after_filter * TIKV_CPU_FACTOR;
            let double_read_request =
                after_filter * DOUBLE_READ_REQUESTS_PER_ROW * TIDB_REQUEST_FACTOR;
            index_side
                + (table_side + double_read_cpu + double_read_request) / INDEX_LOOKUP_CONCURRENCY
        }
    }
}

/// Go `getAvgRowSize`'s per-column input for one table column.
fn row_size_column(column: &KvColumn, stats: Option<&TableStatistics>) -> RowSizeColumn {
    let kind = row_size_type(&column.field_type);
    let width = estimate_type_width(&column.field_type) as f64;
    match stats.and_then(|stats| stats.columns.get(&column.id)) {
        Some(column_stats) => RowSizeColumn::with_stats(
            RowSizeColumnStats::new(
                kind,
                column_stats.histogram.tot_col_size,
                column_stats.histogram.null_count,
                column_stats.total_row_count(),
                false,
            ),
            width,
        ),
        None => RowSizeColumn::without_stats(width),
    }
}

/// Go `HistColl.RealtimeCount`, which is what a full scan reads.
///
/// Kept public to the crate so `EXPLAIN`'s full-scan row and the cost that
/// chose the full scan cannot answer this question differently.
/// Go `chunk.GetFixedLen`'s type classification, as `row_size` names it.
fn row_size_type(field_type: &FieldType) -> RowSizeType {
    match field_type.code() {
        FieldTypeCode::Float => RowSizeType::Float,
        FieldTypeCode::Double => RowSizeType::Double,
        FieldTypeCode::Duration => RowSizeType::Duration,
        FieldTypeCode::Date => RowSizeType::Date,
        FieldTypeCode::Datetime => RowSizeType::Datetime,
        FieldTypeCode::Timestamp => RowSizeType::Timestamp,
        FieldTypeCode::Tiny => RowSizeType::Tiny,
        FieldTypeCode::Short => RowSizeType::Short,
        FieldTypeCode::Int24 => RowSizeType::Int24,
        FieldTypeCode::Long => RowSizeType::Long,
        FieldTypeCode::LongLong => RowSizeType::LongLong,
        FieldTypeCode::Year => RowSizeType::Year,
        FieldTypeCode::Enum => RowSizeType::Enum,
        FieldTypeCode::Bit => RowSizeType::Bit,
        FieldTypeCode::Set => RowSizeType::Set,
        FieldTypeCode::NewDecimal => RowSizeType::Decimal,
        FieldTypeCode::NewDate => RowSizeType::Variable,
        _ => RowSizeType::Variable,
    }
}

/// The table's realtime row count: `stats_meta.count` when analyzed, Go's
/// `statistics.PseudoRowCount` when not.
fn is_pseudo(stats: Option<&TableStatistics>) -> bool {
    stats.is_none_or(|stats| stats.pseudo)
}

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

/// Go `scanCostVer2`: `rows * max(log2(row-size), 0) * scan-factor`.
fn scan_cost(rows: f64, row_size: f64) -> f64 {
    let row_size = row_size.max(1.0);
    rows * row_size.log2().max(0.0) * TIKV_SCAN_FACTOR
}

/// Go `netCostVer2`: `rows * row-size * net-factor`.
fn net_cost(rows: f64, row_size: f64) -> f64 {
    rows * row_size * TIDB_KV_NET_FACTOR
}

/// Go `HistColl.GetAnalyzeRowCount` (`pkg/statistics/table.go`): the row count
/// an ANALYZE actually observed, which -- unlike `RealtimeCount` -- does not
/// move with `modify_count`. The first analyzed column by ascending id, then
/// the first analyzed index, then `-1` for a table that has neither.
///
/// Every histogram this tier loads is a full one, so Go's `IsFullLoad` guard
/// is satisfied by presence; the `BTreeMap`s are already in Go's id order.
fn analyze_row_count(stats: &TableStatistics) -> f64 {
    if let Some(column) = stats.columns.values().next() {
        return column.total_row_count();
    }
    if let Some(index) = stats.indexes.values().next() {
        return index.total_row_count();
    }
    -1.0
}

/// Go `getTableScanPenalty` (`pkg/planner/core/plan_cost_ver2.go`): the EXTRA
/// rows a full-range table scan is costed at on top of the ones it reads.
///
/// # Why a full scan is not priced at what it reads
///
/// A full table scan is the plan whose estimate cannot be wrong in the
/// planner's favour and can be wrong badly against it: when the statistics are
/// pseudo, or stale enough that `modify_count` outruns the analyzed count, the
/// row count feeding every OTHER path is a guess, and the scan is the path
/// that guess makes look cheapest. Go answers by charging the scan a second
/// scan's worth of rows -- `max(MaxPenaltyRowCount, rows)`, raised again to
/// `modify_count` on an unpartitioned table -- so a covering index has to be
/// beaten on its own merits rather than by an estimate nobody trusts.
///
/// This is why `EXPLAIN SELECT * FROM t` over `t(a bigint, b bigint, KEY
/// idx(a,b))` reads `IndexFullScan` in real TiDB: with no statistics the two
/// paths cost within a few percent of each other, and the penalty is what
/// decides it. Captured through `rust/difftests/gorun`.
///
/// Returns `0.0` -- no penalty -- for the three shapes Go exempts:
///
/// * a NARROWED table path (`ranger.HasFullRange` is false, and the
///   index-join inner side's `RangeInfo`), because the range is the evidence
///   the penalty exists to demand;
/// * statistics that are neither pseudo, stale, nor suspiciously low;
/// * `tidb_opt_prefer_range_scan` off, which this tier has no `SET` for and
///   so is always on.
///
/// `partition_scan` is Go's `hasPartitionScan`: a scan of a PRUNED partition
/// already proved it reads a fraction of the table, so it stops at the row
/// floor instead of being raised to the whole table's `modify_count`.
///
/// # Two of Go's conditions are ported but not yet OBSERVABLE here
///
/// Both are kept because they are Go's own, and a rule this tier reaches by a
/// different route than Go would be a second answer to disagree with; neither
/// is what produces the plans above. Recording that they are inert is what
/// stops a later reader from mistaking either for load-bearing:
///
/// * **`hasFullRangeScan`** (the caller's `handle_ranges.is_none()` gate).
///   MEASURED: charging a NARROWED table path the penalty as well changes no
///   test in this workspace and no statement of the integration replay -- the
///   `net_cost` term, which the penalty does not touch, is what decides a
///   narrowed path, and the widest table and narrowest covering index that
///   could be built (fifteen `BIGINT` columns against `KEY kb(b)`) still does
///   not cross over. The gate stays because penalizing a narrowed path is
///   simply not Go's rule.
/// * **`index_force`**. Unreachable from a SINGLE-table chooser by
///   construction: Go's flag is statement-wide, and within one table a
///   `USE`/`FORCE INDEX` that names an index DELETES the table path
///   (`AvailablePaths::allows_table`), so no full scan survives for the
///   penalty to charge. It becomes observable the day path selection runs at
///   each leaf of a multi-table `FROM`, which is where a hint on one table
///   penalizes another table's scan.
fn table_scan_penalty_rows(
    stats: Option<&TableStatistics>,
    rows: f64,
    index_force: bool,
    partition_scan: bool,
) -> f64 {
    let pseudo = is_pseudo(stats);
    let original_rows = stats.map_or(-1.0, analyze_row_count);
    let modify_count = stats.map_or(0, |stats| stats.modify_count) as f64;
    // Go's `hasUnreliableStats`, `hasHighModifyCount` and `hasLowEstimate`.
    let unreliable = pseudo || original_rows < 1.0;
    let high_modify_count = modify_count > original_rows;
    let low_estimate = rows > 1.0 && modify_count < original_rows && rows.trunc() <= modify_count;
    // Go's `allowPreferRangeScan` is `tidb_opt_prefer_range_scan`, whose
    // default is on and which this tier has no `SET` for.
    let prefer_range_scan = unreliable || high_modify_count || low_estimate;
    if !index_force && !prefer_range_scan {
        return 0.0;
    }
    let min_rows = MAX_PENALTY_ROW_COUNT.max(rows);
    if partition_scan {
        return min_rows;
    }
    min_rows.max(modify_count)
}

/// Every candidate way of reading `table` under `where_clause`.
///
/// The table path is always a candidate -- Go's `tablePath` is built
/// unconditionally in `DeriveStats` -- and each index contributes one
/// candidate when the detacher produced ranges for it.
///
/// The table path is a FULL scan only when the ranger built nothing over the
/// clustered integer handle; with a handle bound it carries its own ranges
/// (see [`crate::handle_range`]) and lowers to Go's `TableRangeScan`.
///
/// `needed_columns` are the row offsets the statement actually reads, which
/// decides whether an index path is covering (Go's `isCoveringIndex`): a
/// covering path lowers to a `PhysicalIndexReader`, a non-covering one to a
/// `PhysicalIndexLookUpReader` and pays for the double read.
#[allow(clippy::too_many_arguments)]
pub(crate) fn enumerate_paths(
    table: &KvTable,
    columns: &[(String, FieldType)],
    where_clause: Option<&tidb_ast::Expr>,
    needed_columns: &[usize],
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    limit: Option<&PushedLimit<'_>>,
    stats: Option<&TableStatistics>,
    hints: &crate::index_hints::AvailablePaths,
    // Go's `!prop.IsSortItemEmpty()`: whether the caller re-entered path
    // selection carrying an ORDER the read could establish itself. Go reaches
    // this as the physical property of a second `findBestTask` invocation;
    // this tier has no second invocation, so the statement's own `ORDER BY`
    // is passed in directly.
    sort_property: bool,
    // Go `getTableScanPenalty`'s `hasPartitionScan`: the statement carries
    // partition pruning conditions, so its table scan is of the partitions
    // that survived rather than of the whole table.
    partition_scan: bool,
    // Go `StmtCtx.GetIndexForce()`: whether ANY table of the whole statement
    // carries a `USE`/`FORCE INDEX`. STATEMENT-wide, not this table's -- see
    // [`crate::driver::leaf_demand::LeafDemand::forces_index`].
    index_force: bool,
    // Whether to run Go's `derivePathStatsAndTryHeuristics` point-range
    // selection over the enumeration (see [`heuristic_point_path`]). True for
    // the callers that model a whole `DataSource`'s path selection -- the
    // single-table read, the write's read, and a join leaf -- because Go runs
    // the heuristic once per `DataSource` in `DeriveStats`, BEFORE skyline
    // pruning or any cost. False for the per-branch IndexMerge enumerations:
    // those model `generateIndexMergePath`'s PARTIAL construction, which in
    // Go runs after the heuristic and never through it, and a heuristic that
    // fired inside a branch could hand back the table path where the branch
    // needs an index partial.
    try_heuristics: bool,
    // The already-derived logical DataSource row count, when the caller owns
    // that derivation. Go stores this on `DataSource.StatsInfo()` and every
    // physical path reads the same value; accepting it here avoids running
    // the complete selectivity/ranger pass a second time in the single-table
    // planner. Other callers that have no such owner pass `None`.
    source_rows: Option<f64>,
) -> Vec<Candidate<AccessPath>> {
    let realtime = realtime_row_count(stats);
    let source_rows = source_rows
        .unwrap_or_else(|| source_row_count(table, where_clause, resolver, stats, realtime));
    // Go's `deriveTablePathStats`: the table path's own ranges, over the
    // clustered integer handle. Nothing here narrows what is EVALUATED -- the
    // `WHERE` stays in the pipeline above the source -- so a range only ever
    // decides how much is read and what the path is costed at.
    let handle = where_clause.and_then(|clause| {
        crate::handle_range::build_handle_ranges(table, clause, &resolver.time_zone())
    });
    let handle_ranges = handle.as_ref().map(|built| built.ranges.clone());
    let table_order: Vec<usize> = if table.common_handle_offsets().is_empty() {
        table.pk_handle_offset().into_iter().collect()
    } else {
        table.common_handle_offsets().to_vec()
    };
    let full_range = [IndexRange::full()];
    let table_ranges_for_order = handle_ranges.as_deref().unwrap_or(&full_range);
    let table_limit_matches_order = limit.is_some_and(|limit| {
        limit.has_order
            && !table_order.is_empty()
            && (limit.satisfied_by)(&table_order, table_ranges_for_order)
    });
    let table_scan = table_scan_path(
        table,
        needed_columns,
        where_clause,
        resolver,
        stats,
        realtime,
        handle_ranges.as_deref(),
        limit,
        table_limit_matches_order,
        // Go's `hasFullRangeScan`: only a table path the ranger narrowed
        // NOTHING on is penalized -- with a handle bound the path carries its
        // own ranges, and the range is the evidence the penalty demands.
        if handle_ranges.is_none() {
            table_scan_penalty_rows(
                stats,
                realtime.max(MIN_NUM_ROWS),
                index_force || hints.has_forced_path(),
                partition_scan,
            )
        } else {
            0.0
        },
    );
    // Go's `getTableCandidate`: a table path is always a single scan, and it
    // is the full range exactly when the ranger built nothing. Its access
    // columns are the handle KEY PARTS actually constrained by its access
    // conditions. A common handle `(w,d,o)` narrowed only on `w` must not
    // claim `d` and `o`: doing so lets the table path skyline-prune an index
    // that has the same `w` range but is materially cheaper to scan.
    let handle_key_offsets = table.pk_handle_offset().map_or_else(
        || table.common_handle_offsets().to_vec(),
        |offset| vec![offset],
    );
    let handle_access_columns: ColSet = handle
        .as_ref()
        .map(|built| {
            built
                .access_columns
                .iter()
                .filter_map(|position| handle_key_offsets.get(*position).copied())
                .collect()
        })
        .unwrap_or_default();
    // Go's `available`: a `USE`/`FORCE INDEX` that named an index deletes the
    // table path outright, so the cost model never gets to prefer the scan
    // (or the point get, which is the same path) over the hinted index.
    let mut candidates = Vec::new();
    if hints.allows_table() {
        candidates.push(Candidate {
            access_columns: handle_access_columns,
            index_columns: ColSet::new(),
            single_scan: true,
            eq_or_in_count: handle.as_ref().map_or(0, |built| built.eq_or_in_count),
            full_range: handle_ranges.is_none(),
            count_after_access: table_scan.count_after_access,
            max_count_after_access: table_scan.risk.max,
            min_count_after_access: table_scan.risk.min,
            count_after_index: table_scan.risk.after_index,
            pseudo: is_pseudo(stats),
            index_width: 0,
            empty_range: handle_ranges.as_ref().is_some_and(Vec::is_empty),
            index_filter_count: 0,
            table_filter_count: 0,
            forced: false,
            path: table_scan,
        });
    }
    // Go's `GcSubstituter`, run once per table: an expression an indexed
    // generated column already stores becomes a reference to that column, so
    // the ranger can narrow on it (`WHERE a+1 = 3` over `KEY((a+1))` reads
    // `range:[3,3]`, captured). The rewritten copy is used for RANGE BUILDING
    // ALONE -- the caller leaves the original `WHERE` in the pipeline above
    // the source, so a substitution can only narrow what is read, never
    // change what is evaluated. See [`crate::generated_column_substitute`].
    let substitution = crate::generated_column_substitute::SubstitutionMap::collect(table);
    let substituted = where_clause.and_then(|clause| substitution.substitute_condition(clause));
    let range_clause = substituted.as_ref().or(where_clause);
    let common_handle_primary =
        crate::handle_range::common_handle_primary(table).map(|index| index.id);
    for index in table.plan_indexes() {
        // Go represents a clustered common-handle PRIMARY as the table path:
        // its ranges read `_r<common_handle>` records directly. Treating the
        // same metadata as a secondary index creates an index lookup and one
        // point read per row, which is both the wrong physical plan and
        // catastrophic for broad primary-key prefixes.
        if common_handle_primary == Some(index.id) {
            continue;
        }
        // Go's restricted `available` and `removeIgnoredPaths`: an index a
        // `USE`/`FORCE` did not name, or an `IGNORE` did, is not a path.
        if !hints.allows_index(index.id) {
            continue;
        }
        // Go `path.Forced`, read by `skylinePruning`'s `keepIndex`.
        let forced = hints.forces_index(index.id);
        // An index key part is named by the TABLE, not by the query's scope:
        // the hidden column an expression index was rewritten into is a
        // column of the table that no query can see, so the scope has no
        // entry at its offset.
        let mut index_columns: Vec<crate::index_range::RangeColumn> = index
            .column_offsets
            .iter()
            .enumerate()
            .filter_map(|(position, offset)| {
                let (name, field_type) = columns.get(*offset).cloned().or_else(|| {
                    table
                        .columns
                        .get(*offset)
                        .map(|column| (column.name.clone(), column.field_type.clone()))
                })?;
                Some(crate::index_range::RangeColumn {
                    name,
                    field_type,
                    prefix_len: index.prefix_length(position),
                })
            })
            .collect();
        if index_columns.len() != index.column_offsets.len() {
            continue;
        }
        // Go `fillIndexPath` (`pkg/planner/core/stats.go`): the clustered
        // integer handle is a REAL trailing key part of every non-unique
        // secondary index, so it joins `path.IdxCols` before the ranger runs.
        // `range_offsets` carries the same append for the position -> row
        // offset map the access columns are read through.
        let mut range_offsets: Vec<usize> = index.column_offsets.clone();
        if let Some(handle) = appended_handle_column(index, table) {
            index_columns.push(handle.0);
            range_offsets.push(handle.1);
        }
        let built = range_clause.and_then(|range_clause| {
            crate::index_range::detach_cond_and_build_range_for_index_with_like_default_escape(
                &index_columns,
                range_clause,
                &resolver.time_zone(),
                resolver.like_default_escape(),
                true,
            )
        });
        let Some(built) = built else {
            // Go's `keepIndex := len(path.AccessConds) > 0 || ... ||
            // path.IsSingleScan` in `skylinePruning`: an index the ranger
            // narrowed nothing on is still a candidate when it COVERS the
            // statement's columns, because reading the whole of a narrow
            // index is cheaper than reading the whole of the table. Go
            // builds such a path with `ranger.FullRange()` and prints it as
            // `IndexFullScan`.
            //
            // An index that does not cover is pruned here exactly as Go
            // prunes it: a full index scan plus a row lookup per entry can
            // never beat the table scan it would have to do anyway.
            if let Some(candidate) = full_scan_candidate(
                table,
                index,
                where_clause,
                needed_columns,
                resolver,
                limit,
                stats,
                realtime,
                forced,
                sort_property,
            ) {
                candidates.push(candidate);
            }
            continue;
        };
        let empty_range = built.ranges.is_empty();
        let access_columns: ColSet = built
            .access_columns
            .iter()
            .filter_map(|position| range_offsets.get(*position).copied())
            .collect();
        let full_index_columns = full_index_columns(index, table);
        let (index_columns_map, index_filters, table_filter_count) =
            split_index_filter_conditions(&built.residual, &full_index_columns, resolver);
        let index_filter_count = index_filters.len();
        let index_filter_selectivity = (!index_filters.is_empty()).then(|| {
            selectivity_of_conjuncts_without_paths(&index_filters, table, resolver, stats)
        });
        let path = index_path(
            table,
            index,
            built.ranges,
            needed_columns,
            limit,
            stats,
            realtime,
            source_rows,
            index_filter_selectivity,
        );
        candidates.push(Candidate {
            // Go `indexCondsColMap` is `ExtractCol2Len(AccessConds ++
            // IndexFilters, FullIdxCols)`, so the access columns join the
            // index filters' columns in one map.
            index_columns: access_columns.union(&index_columns_map).copied().collect(),
            access_columns,
            single_scan: is_covering(index, table, needed_columns),
            eq_or_in_count: built.eq_or_in_count,
            full_range: false,
            count_after_access: path.count_after_access,
            max_count_after_access: path.risk.max,
            min_count_after_access: path.risk.min,
            count_after_index: path.risk.after_index,
            // Go `isCandidatesPseudo`: an index with no loaded histogram is
            // pseudo even on a table whose other statistics are real.
            pseudo: stats.is_none_or(|stats| !stats.indexes.contains_key(&index.id)),
            index_width: index.column_offsets.len(),
            empty_range,
            index_filter_count,
            table_filter_count,
            forced,
            path,
        });
    }
    // Go binds `derivePathStats` and `tryHeuristics` into one pass: a path
    // matching a heuristic rule PRUNES every other path before skyline
    // pruning or the cost model sees them (`ds.PossibleAccessPaths[:1]`).
    if try_heuristics {
        if let Some(position) = heuristic_point_path(table, &candidates) {
            let selected = candidates.swap_remove(position);
            return vec![selected];
        }
    }
    candidates
}

/// How many ranges a candidate reads -- Go's `len(path.Ranges)`, where a path
/// the ranger never narrowed carries `ranger.FullRange()`, ONE range.
fn candidate_range_count(candidate: &Candidate<AccessPath>) -> usize {
    match &candidate.path.index {
        Some((_, ranges)) => ranges.len(),
        None => candidate.path.table_ranges.as_ref().map_or(1, Vec::len),
    }
}

/// Go `AccessPath.OnlyPointRange`: every range is a point. The INT-handle
/// table path asks `IsPointNullable` -- an integer handle cannot be NULL, so
/// the tolerance is free -- while every other path asks `IsPointNonNullable`
/// (a unique index admits duplicate NULL rows, so a NULL point pins nothing)
/// and additionally requires the point to pin EVERY declared column
/// (`len(ran.HighVal) != len(path.Index.Columns)`). A COMMON-HANDLE table
/// path takes the second arm too: its `path.Index` is the clustered PRIMARY,
/// so `o_w_id = 1` over `PRIMARY KEY (o_w_id, o_d_id, o_id)` is a PREFIX, not
/// a point, and must not fire the heuristic.
fn candidate_only_point_range(table: &KvTable, candidate: &Candidate<AccessPath>) -> bool {
    match &candidate.path.index {
        Some((_, ranges)) => ranges
            .iter()
            .all(|range| range.is_point(false) && range.low.len() == candidate.index_width),
        None => {
            let common_handle_width = table.common_handle_offsets().len();
            candidate.path.table_ranges.as_ref().is_some_and(|ranges| {
                ranges.iter().all(|range| {
                    if common_handle_width == 0 {
                        range.is_point(true)
                    } else {
                        range.is_point(false) && range.low.len() == common_handle_width
                    }
                })
            })
        }
    }
}

/// Go `derivePathStatsAndTryHeuristics` (`pkg/planner/core/stats.go`), the
/// heuristic half: which candidate -- if any -- is selected OUTRIGHT, before
/// skyline pruning and before any cost, with every other path pruned.
///
/// The rules, in Go's own order over the hint-filtered path list (table path
/// first, then the indexes as the table declares them):
///
/// 1. A path with NO ranges wins immediately: the conditions are
///    contradictory, and no other path could do better than reading nothing.
/// 2. The FIRST path with only point ranges that is the table path or a
///    unique index AND is a single scan wins immediately. This -- not the
///    cost model -- is why `WHERE i = 1 AND j = 1` over `t (i int key, j
///    int, unique key (i, j))` reads `Point_Get table:t handle:1` with the
///    `j` conjunct as a root filter: the int-handle path is iterated first,
///    its `[1,1]` is a point, a table path is always a single scan, and the
///    unique `(i, j)` index is never even examined.
/// 3. Point-range unique indexes that need a row lookup are collected, and
///    the best of them (`uniqueBest`: fewest ranges, then fewest table
///    filters -- most likely to fetch zero index rows) is selected UNLESS a
///    single-scan path whose access columns strictly dominate some unique
///    index's (`refinedBest`, e.g. `idx_b_c` over unique `idx_b` when both
///    `b` and `c` are constrained) reads fewer than twice `uniqueBest`'s
///    ranges -- twice, because the unique double scan pays one index point
///    and one row lookup per range.
///
/// Go appends an EXPLAIN note here ("handle of t is selected since the path
/// only has point ranges"). NOT MODELLED: this tier has no note channel from
/// path selection; the direction is a missing informational warning, never a
/// different plan.
///
/// Access-column dominance reuses [`crate::skyline::compare_col_sets`], the
/// same `util.CompareCol2Len` collapse skyline pruning runs on (Go calls the
/// full `CompareCol2Len` with `GetCol2LenFromAccessConds` here; see
/// [`crate::skyline::ColSet`] for what dropping the lengths costs).
fn heuristic_point_path(table: &KvTable, candidates: &[Candidate<AccessPath>]) -> Option<usize> {
    let mut selected: Option<usize> = None;
    let mut unique_with_double_scan: Vec<usize> = Vec::new();
    let mut single_scan_paths: Vec<usize> = Vec::new();
    for (position, candidate) in candidates.iter().enumerate() {
        // Go: `if len(path.Ranges) == 0 { selected = path; break }`.
        if candidate.empty_range {
            selected = Some(position);
            break;
        }
        if candidate_only_point_range(table, candidate) {
            let unique_index = candidate.path.index.as_ref().is_some_and(|(index_id, _)| {
                table
                    .plan_indexes()
                    .find(|index| index.id == *index_id)
                    .is_some_and(|index| index.unique)
            });
            if candidate.path.index.is_none() || unique_index {
                if candidate.single_scan {
                    selected = Some(position);
                    break;
                }
                unique_with_double_scan.push(position);
            }
        } else if candidate.single_scan {
            single_scan_paths.push(position);
        }
    }
    if selected.is_none() && !unique_with_double_scan.is_empty() {
        let mut unique_best: Option<usize> = None;
        for &position in &unique_with_double_scan {
            let better = unique_best.is_none_or(|best| {
                let (candidate_ranges, best_ranges) = (
                    candidate_range_count(&candidates[position]),
                    candidate_range_count(&candidates[best]),
                );
                candidate_ranges < best_ranges
                    || (candidate_ranges == best_ranges
                        && candidates[position].table_filter_count
                            < candidates[best].table_filter_count)
            });
            if better {
                unique_best = Some(position);
            }
        }
        let mut refined_best: Option<usize> = None;
        for &position in &single_scan_paths {
            let dominates_a_unique = unique_with_double_scan.iter().any(|&unique| {
                let (result, comparable) = crate::skyline::compare_col_sets(
                    &candidates[position].access_columns,
                    &candidates[unique].access_columns,
                );
                comparable && result == 1
            });
            if dominates_a_unique
                && refined_best.is_none_or(|best| {
                    candidate_range_count(&candidates[position])
                        < candidate_range_count(&candidates[best])
                })
            {
                refined_best = Some(position);
            }
        }
        // Go: `refinedBest` wins only while its scan count stays under twice
        // `uniqueBest`'s -- each unique range is one index point plus one row
        // lookup, so at 2x the refined single scan stops being cheaper.
        selected = match (refined_best, unique_best) {
            (Some(refined), Some(unique))
                if candidate_range_count(&candidates[refined])
                    < 2 * candidate_range_count(&candidates[unique]) =>
            {
                Some(refined)
            }
            (Some(refined), None) => Some(refined),
            (_, unique_best) => unique_best,
        };
    }
    selected
}

/// The candidate for reading the WHOLE of an index: Go's access path over
/// `ranger.FullRange()`, kept by `skylinePruning`'s `path.IsSingleScan` arm
/// even though the ranger produced no access condition for it.
///
/// `None` when the index does not cover `needed_columns`, because such a path
/// reads every index entry AND looks up every row, which the table scan
/// dominates. Go reaches the same answer through `keepIndex` -- whose OTHER
/// arm is `path.Forced`, which is why `forced` overrides the refusal. That
/// override is not a special case bolted on: it is the only way
/// `FORCE INDEX(idx_c) WHERE b = 2` can plan at all, since with the table
/// path deleted by the same hint there would otherwise be no candidate left.
/// Captured TiDB reads `IndexFullScan(idx_c)` under a `TableRowIDScan` there.
///
/// MEASURED: dropping that refusal changes NO test in this workspace -- the
/// double read in [`AccessPath::cost`] already makes a non-covering full index
/// scan lose. It is kept because it is Go's own condition and because a path
/// Go never costs should not be costed here; it is a fidelity guard, not the
/// thing producing the answer.
///
/// Its skyline coordinates are the table scan's own -- no access columns, no
/// `=`/`IN` prefix, a single scan over the full range -- so neither dominates
/// the other and the choice falls to [`AccessPath::cost`], where the index's
/// narrower row wins exactly when it is narrower.
/// No access condition is not the same as no index FILTER. Go's
/// `deriveIndexPathStats` splits the residual conditions over every index
/// path, full-range or not, and a condition the index can evaluate on its own
/// narrows what leaves the cop task. `b LIKE '%'` over `KEY kb(b)` is exactly
/// that shape: the ranger builds no range, but the index still evaluates the
/// predicate and returns a tenth of the entries. Costing this path as though
/// it shipped all 10000 while the table path was costed after its own filter
/// made a covering index lose to a full table read the moment the filter's
/// selectivity became Go's -- captured TiDB reads `IndexFullScan` there.
#[allow(clippy::too_many_arguments)]
fn full_scan_candidate(
    table: &KvTable,
    index: &KvIndex,
    where_clause: Option<&tidb_ast::Expr>,
    needed_columns: &[usize],
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    limit: Option<&PushedLimit<'_>>,
    stats: Option<&TableStatistics>,
    realtime: f64,
    forced: bool,
    sort_property: bool,
) -> Option<Candidate<AccessPath>> {
    let covering = is_covering(index, table, needed_columns);
    if !covering && !forced && !sort_property {
        return None;
    }
    // The ranger consumed nothing, so every conjunct is residual.
    let mut residual = Vec::new();
    if let Some(where_clause) = where_clause {
        crate::plan_trace::collect_and(where_clause, &mut residual);
    }
    let (index_columns_map, index_filters, table_filter_count) =
        split_index_filter_conditions(&residual, &full_index_columns(index, table), resolver);
    let index_filter_count = index_filters.len();
    let index_filter_selectivity = (!index_filters.is_empty())
        .then(|| selectivity_of_conjuncts_without_paths(&index_filters, table, resolver, stats));
    let source_rows = source_row_count(table, where_clause, resolver, stats, realtime);
    let ranges = vec![IndexRange::full()];
    let path = index_path(
        table,
        index,
        ranges,
        needed_columns,
        limit,
        stats,
        realtime,
        source_rows,
        index_filter_selectivity,
    );
    Some(Candidate {
        access_columns: ColSet::new(),
        index_columns: index_columns_map,
        single_scan: covering,
        eq_or_in_count: 0,
        full_range: true,
        count_after_access: path.count_after_access,
        max_count_after_access: path.risk.max,
        min_count_after_access: path.risk.min,
        count_after_index: path.risk.after_index,
        pseudo: stats.is_none_or(|stats| !stats.indexes.contains_key(&index.id)),
        index_width: index.column_offsets.len(),
        empty_range: false,
        index_filter_count,
        table_filter_count,
        forced,
        path,
    })
}

/// Go `fillIndexPath`'s `FullIdxCols`: the index's own columns, plus the
/// integer primary key that every secondary index entry carries as its
/// handle. Go appends it only for a non-unique, non-primary index whose
/// handle column is signed, and only when the index does not already name it.
fn full_index_columns(index: &KvIndex, table: &KvTable) -> ColSet {
    let mut columns: ColSet = index.column_offsets.iter().copied().collect();
    if !index.unique {
        if let Some(handle) = table.pk_handle_offset() {
            columns.insert(handle);
        }
    }
    columns
}

/// Go `splitIndexFilterConditions`: a residual condition the index can
/// evaluate on its own -- every column it reads is stored in the index -- is
/// an index filter; anything else waits for the row lookup.
///
/// Returns the index filters' columns, the index filters themselves, and how
/// many table filters there are. A condition whose columns
/// [`crate::column_prune`] refuses to classify counts as a table filter, which
/// is the conservative side: it cannot make an index look like it filters more
/// than it does.
fn split_index_filter_conditions<'a>(
    residual: &[&'a tidb_ast::Expr],
    full_index_columns: &ColSet,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> (ColSet, Vec<&'a tidb_ast::Expr>, usize) {
    let mut columns = ColSet::new();
    let mut index_filters = Vec::new();
    let mut table_filters = 0;
    for condition in residual {
        let read = crate::column_prune::expr_column_offsets(condition, resolver);
        match read {
            Some(read)
                if read
                    .iter()
                    .all(|offset| full_index_columns.contains(offset)) =>
            {
                columns.extend(read);
                index_filters.push(*condition);
            }
            _ => table_filters += 1,
        }
    }
    (columns, index_filters, table_filters)
}

/// The predicates left after the chosen index path's access conditions have
/// been detached. Go keeps exactly this set as `IndexFilters` plus
/// `TableFilters`; none of the access conditions are evaluated by a second
/// `Selection`.
pub(crate) fn index_residual_filters_for_path(
    table: &KvTable,
    index_id: i64,
    where_clause: Option<&tidb_ast::Expr>,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> Vec<tidb_ast::Expr> {
    let Some(where_clause) = where_clause else {
        return Vec::new();
    };
    let Some(index) = table.indexes().iter().find(|index| index.id == index_id) else {
        let mut all = Vec::new();
        crate::plan_trace::collect_and(where_clause, &mut all);
        return all.into_iter().cloned().collect();
    };
    let index_columns = index
        .column_offsets
        .iter()
        .enumerate()
        .map(|(position, offset)| {
            let Some(column) = table.columns.get(*offset) else {
                return None;
            };
            Some(crate::index_range::RangeColumn {
                name: column.name.clone(),
                field_type: column.field_type.clone(),
                prefix_len: index.prefix_length(position),
            })
        })
        .collect::<Option<Vec<_>>>();
    let Some(mut index_columns) = index_columns else {
        let mut all = Vec::new();
        crate::plan_trace::collect_and(where_clause, &mut all);
        return all.into_iter().cloned().collect();
    };
    if let Some((handle, _)) = appended_handle_column(index, table) {
        index_columns.push(handle);
    }
    let residual =
        match crate::index_range::detach_cond_and_build_range_for_index_with_like_default_escape(
            &index_columns,
            where_clause,
            &resolver.time_zone(),
            resolver.like_default_escape(),
            true,
        ) {
            Some(built) => built.residual,
            None => {
                let mut all = Vec::new();
                crate::plan_trace::collect_and(where_clause, &mut all);
                all
            }
        };
    residual.into_iter().cloned().collect()
}

/// The residual predicates Go places in the cop-side Selection over one
/// chosen index path. Access-path costing already classifies this set; the
/// physical leaf builder asks for it again after the winning path is known so
/// its executor trace describes the same Selection rather than only the
/// narrowed ranges.
pub(crate) fn index_filter_for_path(
    table: &KvTable,
    index_id: i64,
    where_clause: Option<&tidb_ast::Expr>,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> Option<tidb_ast::Expr> {
    let index = table.indexes().iter().find(|index| index.id == index_id)?;
    let residual = index_residual_filters_for_path(table, index_id, where_clause, resolver);
    let residual = residual.iter().collect::<Vec<_>>();
    let (_, index_filters, _) =
        split_index_filter_conditions(&residual, &full_index_columns(index, table), resolver);
    index_filters.into_iter().cloned().reduce(|left, right| {
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new(left),
            Box::new(right),
        )
    })
}

/// The table candidate: a `PhysicalTableReader` over a `PhysicalTableScan`,
/// reading the handle ranges `where_clause` implies and every row when it
/// implies none.
///
/// `ranges` is [`crate::handle_range::build_handle_ranges`]' output. Go's
/// `deriveTablePathStats` reads exactly two numbers off it, and this builds
/// `ranges` is [`crate::handle_range::build_handle_ranges`]' output, and Go's
/// `deriveTablePathStats` turns it into `path.CountAfterAccess` -- the rows
/// the scan reads, the rows it is costed at, and the rows `EXPLAIN` prints,
/// which are one number for a table path.
fn table_scan_path(
    table: &KvTable,
    needed_columns: &[usize],
    where_clause: Option<&tidb_ast::Expr>,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
    realtime: f64,
    ranges: Option<&[IndexRange]>,
    limit: Option<&PushedLimit<'_>>,
    limit_matches_order: bool,
    // Go `getTableScanPenalty`'s answer, computed by the caller because the
    // hint and partition facts it reads are the caller's. Always `0.0` for a
    // NARROWED table path -- Go gates the penalty on `hasFullRangeScan`.
    penalty_rows: f64,
) -> AccessPath {
    // Cost model v2 prices two physical schemas here. The TiKV table scan
    // reads every stored column plus the hidden row ID on a nonclustered
    // table, while the reader transfers only the projected output columns.
    let mut scan_columns: Vec<RowSizeColumn> = table
        .columns
        .iter()
        .map(|column| row_size_column(column, stats))
        .collect();
    if table.pk_handle_offset().is_none() && table.common_handle_offsets().is_empty() {
        scan_columns.push(RowSizeColumn::without_stats(8.0));
    }
    let output_columns: Vec<RowSizeColumn> = needed_columns
        .iter()
        .filter_map(|offset| table.columns.get(*offset))
        .map(|column| row_size_column(column, stats))
        .collect();
    let hist_coll = Some((is_pseudo(stats), realtime as i64));
    let scan_row_size = tidb_planner::plan_cost_ver2::plan_avg_row_size(&scan_columns, hist_coll);
    let output_row_size =
        tidb_planner::plan_cost_ver2::plan_avg_row_size(&output_columns, hist_coll);
    // Go `deriveTablePathStats`: with no access condition the range is the
    // full one and `CountAfterAccess` is the realtime count, taken without
    // consulting the estimator at all ("Skip the expensive
    // GetRowCountByIntColumnRanges call in this case"). With one, it is the
    // estimate over the CONVERTED ranges.
    let unadjusted_count_after_access = match ranges {
        None => realtime,
        Some(ranges) => crate::handle_range::handle_range_row_count(table, ranges, stats),
    };
    let after_filter = source_row_count(table, where_clause, resolver, stats, realtime);
    let count_after_access =
        adjust_count_after_access(unadjusted_count_after_access, after_filter, realtime);
    let physical_count = limit.map_or(count_after_access, |limit| {
        adjust_table_scan_row_count_by_limit(
            count_after_access,
            after_filter,
            limit.cap,
            limit_matches_order,
            limit.ordering_selectivity_ratio,
        )
    });
    // Go costs the scan at the rows it READS and the reader's net transfer at
    // the rows that leave the cop task, which is after the pushed Selection.
    let scan_rows = physical_count.max(MIN_NUM_ROWS);
    let scanned = scan_cost(scan_rows + penalty_rows, scan_row_size.max(MIN_ROW_SIZE));
    let output_rows = limit.map_or(after_filter, |limit| after_filter.min(limit.cap));
    let transferred = net_cost(output_rows, output_row_size.max(MIN_ROW_SIZE));
    let num_ranges = ranges.map_or(1, <[IndexRange]>::len);
    let planner_candidate = tidb_planner::candidate_cost::Candidate::Reader {
        child: Box::new(tidb_planner::candidate_cost::Candidate::Fixed {
            rows: scan_rows,
            row_size: scan_row_size,
            cost: scanned,
            num_ranges,
        }),
        rows: output_rows,
        row_size: tidb_planner::candidate_cost::RowSize::Fixed(output_row_size),
        kind: tidb_planner::candidate_cost::ReaderKind::Table,
    };
    AccessPath {
        index: None,
        table_ranges: ranges.map(<[IndexRange]>::to_vec),
        estimate: ScanEstimate {
            rows: physical_scan_row_count(realtime, physical_count),
            pseudo: is_pseudo(stats),
        },
        count_after_access,
        cost: (scanned + transferred) / DIST_SQL_SCAN_CONCURRENCY,
        planner_candidate,
        source_rows: after_filter,
        // Go's skyline comparison does not use the index-risk interval for a
        // table path; `CountAfterIndex` is meaningless there as well.
        risk: RowRisk::default(),
    }
}

/// Go `cardinality.AdjustRowCountForTableScanByLimit` without histogram
/// cross-correlation. The expected count is expanded by the DataSource/path
/// selectivity, then an order-preserving scan pays the configured fraction of
/// the remaining range as protection against finding the first row late.
pub(crate) fn adjust_table_scan_row_count_by_limit(
    count_after_access: f64,
    source_rows: f64,
    expected_count: f64,
    matches_order: bool,
    ordering_selectivity_ratio: f64,
) -> f64 {
    let mut rows = count_after_access;
    if expected_count < source_rows && count_after_access > 0.0 && source_rows > 0.0 {
        let selectivity = source_rows / count_after_access;
        rows = count_after_access.min(expected_count / selectivity);
    }
    if matches_order && count_after_access > rows && ordering_selectivity_ratio > 0.0 {
        rows += (count_after_access - rows) * ordering_selectivity_ratio;
    }
    rows
}

/// Go `cardinality.AdjustRowCountForIndexScanByLimit`'s uniform branch.
/// An ordered lookup with a residual table filter may need to read more index
/// entries than the visible LIMIT, so scale the expected count by the logical
/// selectivity and then apply the configured ordering-risk ratio.
fn adjust_index_scan_rows_for_limit(
    scan_rows: f64,
    logical_rows: f64,
    expected_rows: f64,
    ordering_selectivity_ratio: f64,
) -> f64 {
    let mut adjusted = scan_rows;
    if expected_rows < logical_rows && logical_rows > 0.0 && scan_rows > 0.0 {
        let selectivity = logical_rows / scan_rows;
        adjusted = (expected_rows / selectivity).min(scan_rows);
    }
    if ordering_selectivity_ratio > 0.0 && scan_rows > adjusted {
        adjusted += (scan_rows - adjusted) * ordering_selectivity_ratio;
    }
    adjusted
}

/// The rows the whole data source is estimated to produce, Go's
/// `ds.StatsInfo().RowCount`: the table's realtime count narrowed by EVERY
/// pushed condition, estimated per column.
///
/// This feeds [`adjust_count_after_access`]. It uses the same ordinary
/// selectivity-node combination as Go, including pseudo histograms and the
/// one-row floor, so the lower-bound adjustment compares estimates formed
/// under the same assumptions.
fn source_row_count(
    table: &KvTable,
    where_clause: Option<&tidb_ast::Expr>,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
    realtime: f64,
) -> f64 {
    match where_clause {
        Some(predicate) => realtime * selectivity(predicate, table, resolver, stats),
        None => realtime,
    }
}

/// Go `adjustCountAfterAccess`: all access paths must estimate at least the
/// logical data source's row count (with the planner's selection-factor
/// tolerance), even when the range estimator used a more optimistic set of
/// assumptions. The unadjusted value remains in [`RowRisk`] for skyline
/// comparison; this return value is the physical scan's `estRows` and cost.
pub(crate) fn adjust_count_after_access(count: f64, source_rows: f64, realtime: f64) -> f64 {
    if count + TOLERANCE_FACTOR < source_rows {
        (source_rows / crate::plan_trace::SELECTIVITY_FACTOR).min(realtime)
    } else {
        count
    }
}

/// One index candidate, costed as the reader it lowers to.
#[allow(clippy::too_many_arguments)]
fn index_path(
    table: &KvTable,
    index: &KvIndex,
    ranges: Vec<IndexRange>,
    needed_columns: &[usize],
    limit: Option<&PushedLimit<'_>>,
    stats: Option<&TableStatistics>,
    realtime: f64,
    source_row_count: f64,
    index_filter_selectivity: Option<f64>,
) -> AccessPath {
    // Go `detachCondAndBuildRangeForPath` estimates over `pruneEstimateRange`
    // -- the ranges trimmed back to the index's DECLARED columns -- and never
    // over the appended handle dimension:
    //
    // ```go
    // if len(indexCols) > len(path.Index.Columns) {
    //     // Trim appended handle dimensions and keep only real
    //     // index-definition columns for stats estimation.
    //     indexCols = indexCols[0:len(path.Index.Columns)]
    // }
    // ...
    // estimateRanges = pruneEstimateRange(path.Ranges, len(indexCols))
    // count, err := cardinality.GetRowCountByIndexRanges(..., estimateRanges, indexCols)
    // ```
    //
    // The statistics are keyed by the index as DECLARED, so estimating over a
    // dimension the histogram never had is reading a column the estimator does
    // not own. It also matters under pseudo rates, which is what this tier
    // runs on: `getPseudoRowCountByIndexRanges` divides by 100 once per
    // EQUAL-PREFIX column, so the untrimmed `(1 1,1 +inf]` estimates 33.33
    // where the trimmed `[1,1]` estimates 10 -- and 33.33 costs the index path
    // out of the plan that TiDB's own recording chooses.
    let estimate_ranges = prune_estimate_range(&ranges, index.column_offsets.len());
    let bounds = index_row_count(index, table, &estimate_ranges, stats, realtime);
    let mut estimated = adjust_count_after_access(bounds.est, source_row_count, realtime);
    if let Some(limit) =
        limit.filter(|limit| (limit.satisfied_by)(index.ordered_column_offsets(), &ranges))
    {
        estimated = adjust_index_scan_rows_for_limit(
            estimated,
            source_row_count,
            limit.cap,
            limit.ordering_selectivity_ratio,
        );
    }
    // Go `deriveIndexPathStats`: with index filters the post-index count is
    // the access count narrowed by them, floored at the data source's own row
    // count; with none it is the access count itself.
    let after_index = match index_filter_selectivity {
        Some(selectivity) => (estimated * selectivity).max(source_row_count),
        None => estimated,
    };
    // Go costs the index side at `min(rows, PushedLimit.Count)` when the
    // reader stops at the cap; `estimated` is still what EXPLAIN prints for
    // the scan, because the cop-side `Limit` is its own operator there.
    let rows = match limit {
        Some(limit) if (limit.satisfied_by)(index.ordered_column_offsets(), &ranges) => {
            estimated.min(limit.cap)
        }
        _ => estimated,
    };
    let covering = is_covering(index, table, needed_columns);
    // Go cost model v2 prices `PhysicalIndexScan.Schema().Columns`, not the
    // encoded key size returned by `PhysicalIndexScan.GetScanRowSize`. Build
    // that schema the same way `PhysicalIndexScan.InitSchema` does: declared
    // index columns, followed by the handle when the index entry or a double
    // read needs it.
    let mut physical_offsets = index.column_offsets.clone();
    for offset in table.common_handle_offsets() {
        if !physical_offsets.contains(offset) {
            physical_offsets.push(*offset);
        }
    }
    if let Some((_, offset)) = appended_handle_column(index, table) {
        if !physical_offsets.contains(&offset) {
            physical_offsets.push(offset);
        }
    }
    if let Some(offset) = table.pk_handle_offset() {
        if (!covering || needed_columns.contains(&offset)) && !physical_offsets.contains(&offset) {
            physical_offsets.push(offset);
        }
    }
    let mut index_row_columns: Vec<RowSizeColumn> = physical_offsets
        .iter()
        .filter_map(|offset| table.columns.get(*offset))
        .map(|column| row_size_column(column, stats))
        .collect();
    if !covering && table.pk_handle_offset().is_none() && table.common_handle_offsets().is_empty() {
        index_row_columns.push(RowSizeColumn::without_stats(8.0));
    }
    let index_row_size = tidb_planner::plan_cost_ver2::plan_avg_row_size(
        &index_row_columns,
        Some((is_pseudo(stats), realtime as i64)),
    );
    let output_columns: Vec<RowSizeColumn> = needed_columns
        .iter()
        .filter_map(|offset| table.columns.get(*offset))
        .map(|column| row_size_column(column, stats))
        .collect();
    let output_row_size = tidb_planner::plan_cost_ver2::plan_avg_row_size(
        &output_columns,
        Some((is_pseudo(stats), realtime as i64)),
    )
    .max(MIN_ROW_SIZE);
    let index_scan = scan_cost(rows.max(MIN_NUM_ROWS), index_row_size)
        // Go's deterministic tie-breaker between two indexes that cost the
        // same, from the last two digits of the index ID.
        + (index.id % 100) as f64 / 1_000_000.0;
    // Go costs a reader's NET transfer at the rows that leave the cop task,
    // which on the index side is `getCardinality(indexPlan)` -- the top of the
    // cop plan tree, so the cop-side `Selection` over the index filters when
    // there is one. That is the same rule [`table_scan_path`] applies with its
    // own `after_filter`; applying it to only one of the two made an index
    // that filters look no cheaper to ship than one that does not.
    // A covering path lowers to PhysicalIndexReader: the index scan reads its
    // complete physical key schema, but the reader transfers only the
    // projected output schema. A double read still transfers its index-plan
    // schema (including the handle) before fetching table rows.
    let index_net_row_size = if covering {
        output_row_size
    } else {
        index_row_size
    };
    let index_net = net_cost(after_index.min(rows), index_net_row_size);
    let index_side = (index_scan + index_net) / DIST_SQL_SCAN_CONCURRENCY;

    let (cost, planner_candidate) = if covering {
        (
            index_side,
            tidb_planner::candidate_cost::Candidate::Reader {
                child: Box::new(tidb_planner::candidate_cost::Candidate::Fixed {
                    rows,
                    row_size: index_row_size,
                    cost: index_scan,
                    num_ranges: ranges.len(),
                }),
                rows: after_index.min(rows),
                row_size: tidb_planner::candidate_cost::RowSize::Fixed(output_row_size),
                kind: tidb_planner::candidate_cost::ReaderKind::Index,
            },
        )
    } else {
        let table_row_columns: Vec<RowSizeColumn> = needed_columns
            .iter()
            .filter_map(|offset| table.columns.get(*offset))
            .map(|column| row_size_column(column, stats))
            .collect();
        let table_row_size = get_table_avg_row_size(
            &table_row_columns,
            is_pseudo(stats),
            realtime as i64,
            RowSizeStore::TiKv,
            table.pk_handle_offset().is_some(),
            false,
        )
        .max(MIN_ROW_SIZE);
        // The table side reads one row per index row it looked up.
        let table_scan = scan_cost(rows.max(MIN_NUM_ROWS), table_row_size);
        let table_net = net_cost(rows, table_row_size);
        let table_side = (table_scan + table_net) / DIST_SQL_SCAN_CONCURRENCY;
        let double_read_cpu = rows * TIKV_CPU_FACTOR;
        let double_read_tasks = rows * DOUBLE_READ_REQUESTS_PER_ROW;
        let double_read_request = double_read_tasks * TIDB_REQUEST_FACTOR;
        let cost = index_side
            + (table_side + double_read_cpu + double_read_request) / INDEX_LOOKUP_CONCURRENCY;
        (
            cost,
            tidb_planner::candidate_cost::Candidate::Fixed {
                rows: physical_scan_row_count(realtime, estimated),
                row_size: table_row_size,
                cost,
                num_ranges: ranges.len(),
            },
        )
    };
    AccessPath {
        index: Some((index.id, ranges)),
        table_ranges: None,
        estimate: ScanEstimate {
            rows: physical_scan_row_count(realtime, estimated),
            pseudo: is_pseudo(stats),
        },
        count_after_access: estimated,
        cost,
        planner_candidate,
        source_rows: source_row_count,
        risk: RowRisk {
            min: bounds.min_est,
            max: bounds.max_est,
            after_index,
        },
    }
}

/// Go `isCoveringIndex`: every column the statement reads is stored in the
/// index, so no row lookup is needed.
///
/// An integer primary key is stored in every index entry as its handle, so a
/// path over such a table covers the handle column too. A clustered common
/// handle is carried in the same way and covers each of its component
/// columns.
///
/// A PREFIX key part does not count: its entry holds `'abc'` where the row
/// holds `'abcdef'`, so answering from it is the truncation this feature
/// exists to avoid. Go says the same in `isIndexColsCoveringCol`
/// (`pkg/planner/core/operator/logicalop/logical_datasource.go`), which
/// accepts a key part only when its length is unspecified or already reaches
/// the column's `Flen`. Captured: `explain select a from t where a =
/// 'abcdef'` over `key idx(a(3))` is an `IndexLookUp`, not an `IndexReader`.
/// Go's `path.IsSingleScan` for a named index of `table`: whether reading
/// `index_id` answers the statement without a row lookup.
///
/// This is the same [`is_covering`] the cost model runs, re-asked by the
/// caller that has to know which READER a chosen index path lowers to --
/// Go's `PhysicalIndexReader` (one scan) or `PhysicalIndexLookUpReader` (the
/// double read, whose handle batch is reordered). An unknown index id is not
/// covering, which is the fail-closed answer: it costs a lookup that returns
/// the same rows.
pub(crate) fn index_is_covering(table: &KvTable, index_id: i64, needed_columns: &[usize]) -> bool {
    table
        .indexes()
        .iter()
        .find(|index| index.id == index_id)
        .is_some_and(|index| is_covering(index, table, needed_columns))
}

fn is_covering(index: &KvIndex, table: &KvTable, needed_columns: &[usize]) -> bool {
    needed_columns.iter().all(|offset| {
        let flen = table
            .columns
            .get(*offset)
            .map_or(-1, |column| column.field_type.flen());
        index.covers(*offset, flen)
            || table.pk_handle_offset() == Some(*offset)
            || table.common_handle_offsets().contains(offset)
    })
}

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
        // A table WITH statistics whose index was never analyzed: Go falls
        // back to the pseudo rate for that path only, not for the table.
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

/// The primary-index estimate used by a common-handle table path.
///
/// Go's `deriveCommonHandleTablePathStats` estimates its table path through
/// the PRIMARY index histogram even though execution scans record keys.
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
pub(crate) fn selectivity(
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
pub(crate) fn selectivity_of_conjuncts_without_paths(
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
        // With DataSource `filledPaths`, Go maps a clustered PRIMARY to its
        // table path. That path has no cached `IdxCols` yet, so the index node
        // is unavailable. Calls with nil paths (notably IndexFilters) use
        // `Idx2ColUniqueIDs` instead and must retain the PRIMARY statistic.
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
    )
}

/// The cheapest candidate, keeping the incumbent on an exact tie -- Go
/// `compareTaskCost`, which replaces the best task only on a strictly lower
/// cost.
pub(crate) fn choose_access_path(
    candidates: Vec<Candidate<AccessPath>>,
    stats: Option<&TableStatistics>,
    has_limit: bool,
) -> Option<AccessPath> {
    let context = PruningContext {
        table_pseudo: is_pseudo(stats),
        row_count: realtime_row_count(stats),
        // Go `vardef.DefOptPreferRangeScan`, and this tier has no `SET` that
        // moves `tidb_opt_prefer_range_scan` off its default.
        prefer_range: true,
        has_limit,
    };
    let mut best: Option<AccessPath> = None;
    for candidate in skyline_pruning(candidates, &context) {
        let path = candidate.path;
        let better = best.as_ref().is_none_or(|current| path.cost < current.cost);
        if better {
            best = Some(path);
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_table::KvColumn;
    use crate::storage::MemTableStorage;
    use tidb_expr::rewriter::{ColumnResolver, NoResolver};

    #[test]
    fn ordered_limit_scan_estimate_applies_the_go_risk_ratio() {
        assert_eq!(
            adjust_table_scan_row_count_by_limit(1_000.0, 1_000.0, 1.0, true, 0.01),
            10.99,
        );
        assert_eq!(
            adjust_table_scan_row_count_by_limit(1_000.0, 1_000.0, 1.0, true, 0.0),
            1.0,
        );
        assert_eq!(
            adjust_table_scan_row_count_by_limit(1_000.0, 1_000.0, 1.0, false, 0.01),
            1.0,
        );
    }

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

    #[test]
    fn estimate_column_ndv_preserves_resident_load_precedence() {
        let later_handle_id = 1;
        let target_id = 2;
        let first_predicate_id = 3;
        let version = 42;
        let column = |id, ndv, count| ColumnStats {
            histogram: histogram_with_count(id, ndv, count, version),
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        let statistics = TableStatistics::new(
            150_000,
            0,
            [
                (later_handle_id, column(later_handle_id, 149_568, 150_000)),
                (target_id, column(target_id, 25, 150_000)),
                (first_predicate_id, column(first_predicate_id, 5, 149_998)),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
        );

        statistics.mark_loaded_statistics(
            [first_predicate_id].into_iter().collect(),
            BTreeSet::new(),
            None,
        );
        statistics
            .mark_accessed_statistics([later_handle_id].into_iter().collect(), BTreeSet::new());
        statistics.advance_statistics_loads();
        let (full_columns, full_indexes) =
            statistics.mark_loaded_statistics(BTreeSet::new(), BTreeSet::new(), None);

        let ndv = statistics
            .estimate_column_ndv(target_id, &full_columns, &full_indexes)
            .unwrap();
        assert!((ndv - 25.000_333_337_777_837).abs() < 1e-12, "{ndv:.17}");
        let joined_rows = 5.0 * statistics.row_count as f64 / ndv;
        assert!((joined_rows - 29_999.6).abs() < 1e-9, "{joined_rows:.17}");
    }

    #[test]
    fn new_date_uses_chunk_estimate_type_width() {
        let field_type = FieldType::new(FieldTypeCode::NewDate);
        assert_eq!(schema_avg_row_size(&[field_type]), 32.0);
    }

    /// Go `isIndexColsCoveringCol`: a key part that stores only a PREFIX of
    /// its column cannot answer a read of that column, so a path over it is
    /// never a single scan.
    ///
    /// Pinned here rather than through rows on purpose. `IndexRangeSourceExec`
    /// looks the row up unconditionally today -- the double-read test in
    /// `crate::access_path` pins exactly that -- so a wrong answer here costs
    /// an unnecessary lookup rather than a truncated value, and would cost the
    /// truncated value the day a covering-only reader lands. This assertion is
    /// what would go red then, so the rule cannot be lost in the meantime.
    #[test]
    fn a_prefix_key_part_never_covers_its_column() {
        let mut column = KvColumn {
            name: "s".to_owned(),
            id: 1,
            field_type: FieldType::new(FieldTypeCode::Varchar),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        };
        column.field_type.set_flen(20);
        let table = KvTable::with_storage(78, vec![column], Box::new(MemTableStorage::new()));
        let index = |prefix_len: i64| KvIndex {
            id: 1,
            name: "idx".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![0],
            prefix_lengths: vec![prefix_len],
            visible: true,
            global: false,
        };
        assert!(!is_covering(&index(3), &table, &[0]));
        // No declared length, or one that already reaches the column's own
        // width, holds the whole value -- both of which Go accepts.
        assert!(is_covering(
            &index(crate::ddl::index_prefix::UNSPECIFIED_LENGTH),
            &table,
            &[0]
        ));
        assert!(is_covering(&index(20), &table, &[0]));
    }

    /// Every secondary-index entry carries the clustered common handle, so
    /// its primary-key columns are covering even when they are not declared
    /// as secondary-index key parts. Go relies on this for TPCC's
    /// `idx_customer(..., c_first)` to return `c_id` as an IndexReader.
    #[test]
    fn a_secondary_index_covers_common_handle_columns() {
        let mut table = KvTable::with_storage(
            79,
            vec![
                long_column("w_id", 1),
                long_column("d_id", 2),
                long_column("c_id", 3),
                long_column("last", 4),
                long_column("balance", 5),
            ],
            Box::new(MemTableStorage::new()),
        );
        table.set_common_handle_offsets(vec![0, 1, 2]);
        let secondary = KvIndex {
            id: 2,
            name: "idx_customer".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![0, 1, 3],
            prefix_lengths: vec![
                crate::ddl::index_prefix::UNSPECIFIED_LENGTH,
                crate::ddl::index_prefix::UNSPECIFIED_LENGTH,
                crate::ddl::index_prefix::UNSPECIFIED_LENGTH,
            ],
            visible: true,
            global: false,
        };

        assert!(is_covering(&secondary, &table, &[0, 1, 2, 3]));
        assert!(!is_covering(&secondary, &table, &[4]));
    }

    #[test]
    fn count_after_access_uses_the_logical_row_count_lower_bound() {
        assert_eq!(adjust_count_after_access(0.001, 1.0, 10_000.0), 1.25);
        assert_eq!(adjust_count_after_access(2.0, 1.0, 10_000.0), 2.0);
        assert_eq!(adjust_count_after_access(0.0, 1.0, 1.0), 1.0);
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
        table.add_index(KvIndex {
            id: 7,
            name: "PRIMARY".to_owned(),
            comment: String::new(),
            unique: true,
            column_offsets: vec![0, 1, 2],
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 3],
            visible: true,
            global: false,
        });
        table.add_index(KvIndex {
            id: 8,
            name: "idx_ab".to_owned(),
            comment: String::new(),
            unique: false,
            column_offsets: vec![0, 1],
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 2],
            visible: true,
            global: false,
        });

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

    /// `t(a, b, c)` with a non-unique index on `b`, the shape
    /// [`crate::access_path`]'s tests use.
    fn table_with_index() -> KvTable {
        let mut table = KvTable::with_storage(
            77,
            vec![
                long_column("a", 1),
                long_column("b", 2),
                long_column("c", 3),
            ],
            Box::new(MemTableStorage::new()),
        );
        table
            .create_index_with_context(
                KvIndex {
                    id: 1,
                    name: "ib".to_owned(),
                    comment: String::new(),
                    unique: false,
                    column_offsets: vec![1],
                    prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
                    visible: true,
                    global: false,
                },
                &crate::StmtContext::for_query(),
            )
            .unwrap();
        table
    }

    /// The single point range `b = 42`.
    fn point_range() -> IndexRange {
        IndexRange {
            low: vec![Datum::Int(42)],
            high: vec![Datum::Int(42)],
            low_exclusive: false,
            high_exclusive: false,
        }
    }

    /// `StatsInfo::ScaleByExpectCnt` refuses to scale a one-row table below
    /// one. The access path still owns its fractional `CountAfterAccess`, but
    /// the physical scan displayed by EXPLAIN owns the scaled table stats.
    #[test]
    fn a_physical_scan_keeps_gos_one_row_estimate_floor() {
        assert_eq!(physical_scan_row_count(1.0, 0.025), 1.0);
        assert_eq!(physical_scan_row_count(10_000.0, 250.0), 250.0);
        assert_eq!(physical_scan_row_count(10_000.0, 20_000.0), 10_000.0);
    }

    /// Go's `keepIndex` in `skylinePruning` (`find_best_task.go:1830`):
    ///
    /// ```go
    /// keepIndex := len(path.AccessConds) > 0 || !prop.IsSortItemEmpty() ||
    ///     path.Forced || path.IsSingleScan || matchPartialOrderIndex
    /// ```
    ///
    /// The `!prop.IsSortItemEmpty()` arm is what lets a NON-covering index
    /// survive for `SELECT * FROM t ORDER BY b LIMIT n`: captured TiDB reads
    ///
    /// ```text
    /// IndexLookUp_23        limit embedded(offset:0, count:3)
    /// ├─Limit_22(Build)     cop[tikv]  offset:0, count:3
    /// │ └─IndexFullScan_20  cop[tikv]  table:tt, index:ka(a)  keep order:true
    /// └─TableRowIDScan_21   cop[tikv]  table:tt
    /// ```
    ///
    /// even though `ka(a)` covers nothing of `SELECT *`. Without an order to
    /// establish, the same index is pruned -- reading every entry and then
    /// looking up every row can never beat the table scan it would do anyway.
    ///
    /// SCOPE, measured: closing this ENUMERATION gap does not yet move a
    /// chosen plan. `crate::skyline` holds Go's `matchResult` dimension at 0,
    /// so the table path -- a single scan with the same access columns and no
    /// more rows -- still dominates this candidate in pruning, and the tier
    /// has no keep-order scan for the plan to exploit if it survived. This
    /// asserts the enumeration itself, which is the half that is fixed.
    #[test]
    fn a_sort_property_keeps_a_non_covering_index_go_would_keep() {
        let table = table_with_index();
        let index_id = table.plan_indexes().next().unwrap().id;
        let columns: Vec<(String, FieldType)> = table
            .columns
            .iter()
            .map(|column| (column.name.clone(), column.field_type.clone()))
            .collect();
        // `SELECT *` over `t(a, b, c)` with only `ib(b)`: not covering.
        let needed: Vec<usize> = (0..columns.len()).collect();
        let hints = crate::index_hints::AvailablePaths::unrestricted();
        let enumerate = |sort_property| {
            enumerate_paths(
                &table,
                &columns,
                None,
                &needed,
                &NoResolver,
                None,
                None,
                &hints,
                sort_property,
                false,
                false,
                // The subject is the ENUMERATION; no `WHERE` means no point
                // ranges, so the heuristic could not fire anyway.
                false,
                None,
            )
            .into_iter()
            .filter_map(|candidate| candidate.path.index.map(|(id, _)| id))
            .collect::<Vec<_>>()
        };
        assert_eq!(enumerate(false), Vec::<i64>::new());
        assert_eq!(enumerate(true), vec![index_id]);
    }

    /// A `WHERE` for the heuristic tests, parsed rather than hand-built so
    /// the ranges under test are the detacher's own.
    fn parse_where(expression: &str) -> tidb_ast::Expr {
        let statement = tidb_parser::parse(&format!("SELECT * FROM t WHERE {expression}"))
            .expect("the predicate parses");
        let tidb_ast::Stmt::Query(query) = statement else {
            panic!("expected query")
        };
        let tidb_ast::QueryStmt::Select(select) = &*query else {
            panic!("expected select")
        };
        select.where_clause.clone().expect("expected WHERE")
    }

    /// The heuristic-selected path of an enumeration, as `path.index` ids
    /// (`None` = the table path), plus how many candidates survived -- so a
    /// test can tell "the heuristic fired and pruned" from "the cost model
    /// would have picked the same path anyway".
    fn heuristic_enumeration(
        table: &KvTable,
        needed: &[usize],
        predicate: &str,
    ) -> Vec<Option<i64>> {
        let columns: Vec<(String, FieldType)> = table
            .columns
            .iter()
            .map(|column| (column.name.clone(), column.field_type.clone()))
            .collect();
        let where_clause = parse_where(predicate);
        let hints = crate::index_hints::AvailablePaths::unrestricted();
        enumerate_paths(
            table,
            &columns,
            Some(&where_clause),
            needed,
            &NamedColumnResolver { table },
            None,
            None,
            &hints,
            false,
            false,
            false,
            true,
            None,
        )
        .into_iter()
        .map(|candidate| candidate.path.index.map(|(id, _)| id))
        .collect()
    }

    /// Go's `uniqueBest`-vs-`refinedBest` arbitration, on the example
    /// `derivePathStatsAndTryHeuristics`'s own comment carries:
    ///
    /// ```text
    /// create table t(a int, b int, c int, unique index idx_b(b), index idx_b_c(b, c));
    /// select b, c from t where b = 5 and c > 10;
    /// ```
    ///
    /// "In the case, `uniqueBest` is `idx_b`. However, `idx_b_c` is better
    /// than `idx_b`" -- the covering single scan whose access columns
    /// strictly dominate the unique index's is the refined path, and with
    /// one range against one it stays under the 2x bound and wins.
    #[test]
    fn a_dominating_single_scan_refines_the_unique_double_scan_like_go() {
        let mut table = KvTable::with_storage(
            78,
            vec![
                long_column("a", 1),
                long_column("b", 2),
                long_column("c", 3),
            ],
            Box::new(MemTableStorage::new()),
        );
        let context = crate::StmtContext::for_query();
        for (id, name, unique, offsets) in [
            (1, "idx_b", true, vec![1]),
            (2, "idx_b_c", false, vec![1, 2]),
        ] {
            table
                .create_index_with_context(
                    KvIndex {
                        id,
                        name: name.to_owned(),
                        comment: String::new(),
                        unique,
                        column_offsets: offsets.clone(),
                        prefix_lengths: vec![
                            crate::ddl::index_prefix::UNSPECIFIED_LENGTH;
                            offsets.len()
                        ],
                        visible: true,
                        global: false,
                    },
                    &context,
                )
                .unwrap();
        }
        // `SELECT b, c`: `idx_b_c` covers, unique `idx_b` needs a row lookup.
        assert_eq!(
            heuristic_enumeration(&table, &[1, 2], "b = 5 and c > 10"),
            vec![Some(2)]
        );
    }

    /// Go's 2x range bound, on the second example the same comment carries:
    ///
    /// ```text
    /// create table t(a int, b int, c int, d int, unique index idx_a(a),
    ///     unique index idx_b_c(b, c), unique index idx_b_c_a_d(b, c, a, d));
    /// select a, b, c from t where a = 1 and b = 2 and c in (1, 2, 3, 4, 5);
    /// ```
    ///
    /// "`idx_b_c_a_d` needs to access five points while `idx_a` only needs
    /// one point access and one table access" -- the refined path's five
    /// ranges are NOT under twice `uniqueBest`'s one, so the unique point
    /// wins even though the refined path covers.
    #[test]
    fn the_refined_path_loses_at_twice_the_unique_points_like_go() {
        let mut table = KvTable::with_storage(
            79,
            vec![
                long_column("a", 1),
                long_column("b", 2),
                long_column("c", 3),
                long_column("d", 4),
            ],
            Box::new(MemTableStorage::new()),
        );
        let context = crate::StmtContext::for_query();
        for (id, name, offsets) in [
            (1, "idx_a", vec![0]),
            (2, "idx_b_c", vec![1, 2]),
            (3, "idx_b_c_a_d", vec![1, 2, 0, 3]),
        ] {
            table
                .create_index_with_context(
                    KvIndex {
                        id,
                        name: name.to_owned(),
                        comment: String::new(),
                        unique: true,
                        column_offsets: offsets.clone(),
                        prefix_lengths: vec![
                            crate::ddl::index_prefix::UNSPECIFIED_LENGTH;
                            offsets.len()
                        ],
                        visible: true,
                        global: false,
                    },
                    &context,
                )
                .unwrap();
        }
        assert_eq!(
            heuristic_enumeration(
                &table,
                &[0, 1, 2],
                "a = 1 and b = 2 and c in (1, 2, 3, 4, 5)"
            ),
            vec![Some(1)]
        );
    }

    /// A point on a NON-unique index matches no heuristic arm -- Go's
    /// `else if path.IsSingleScan` collects only non-point single scans, and
    /// the point branch demands the table path or a unique index -- so the
    /// enumeration keeps every candidate and the choice stays with skyline
    /// pruning and the cost model.
    #[test]
    fn a_non_unique_point_does_not_fire_the_heuristic() {
        let table = table_with_index();
        assert!(heuristic_enumeration(&table, &[0, 1, 2], "b = 42").len() > 1);
    }

    /// What actually decides a double read is the TABLE SIDE, not the request
    /// term -- which is why [`DOUBLE_READ_REQUESTS_PER_ROW`] can stay at Go's
    /// batched value while our reader issues one round trip per row.
    ///
    /// The table side is charged per looked-up row at full rate, with no
    /// batching discount of any kind, so it grows with the rows the index
    /// returns while a full scan's cost is fixed by the table. That is the
    /// crossover, and it is Go's. The request term at Go's constant is a
    /// rounding error beside it: this test measures both, so a future change
    /// to either one has to say which of the two it is moving.
    #[test]
    fn the_table_side_not_the_request_term_is_what_costs_a_double_read() {
        let table = table_with_index();
        let index = table.plan_indexes().next().unwrap().clone();
        let realtime = crate::plan_trace::PSEUDO_ROW_COUNT;

        // `a` is not stored in the index on `b`, so this path pays a double
        // read; `b` is, so this one does not. The gap between them is the
        // whole price of the lookup.
        let non_covering = index_path(
            &table,
            &index,
            vec![point_range()],
            &[0],
            None,
            None,
            realtime,
            // A lone point range IS the whole predicate, so the data source's
            // own row count is that same one row and `adjustCountAfterAccess`
            // has nothing to equalize.
            1.0,
            None,
        );
        let covering = index_path(
            &table,
            &index,
            vec![point_range()],
            &[1],
            None,
            None,
            realtime,
            // A lone point range IS the whole predicate, so the data source's
            // own row count is that same one row and `adjustCountAfterAccess`
            // has nothing to equalize.
            1.0,
            None,
        );
        let request_term = |rows: f64| {
            rows * DOUBLE_READ_REQUESTS_PER_ROW * TIDB_REQUEST_FACTOR / INDEX_LOOKUP_CONCURRENCY
        };
        // SELECTIVE END: the request term is nearly the whole price of the
        // lookup. Scaling it by the batching factor therefore lands squarely
        // here -- which is exactly where the live differential saw a one-row
        // lookup lose to a 2000-row scan.
        let few_rows = non_covering.estimate.rows;
        let selective_price = non_covering.cost - covering.cost;
        assert!(
            request_term(few_rows) > selective_price / 2.0,
            "at {few_rows} rows the request term ({}) is most of the lookup's \
             price ({selective_price})",
            request_term(few_rows)
        );

        // The crossover itself: a point range keeps the index, a range
        // covering the whole table does not.
        //
        // Measured against the scan WITHOUT Go's table-scan penalty
        // (`table_scan_penalty_rows`, passed as `0.0` here), and deliberately
        // so: the penalty prices the RISK of a full scan under statistics
        // nobody trusts, not the double read, and it moves the crossover only
        // ever in the index path's favour. Excluding it is what keeps this
        // test measuring the term it names. The penalized scan is asserted
        // separately below.
        let all_columns: Vec<usize> = (0..table.columns.len()).collect();
        let scan = table_scan_path(
            &table,
            &all_columns,
            None,
            &NoResolver,
            None,
            realtime,
            None,
            None,
            false,
            0.0,
        );
        assert!(
            non_covering.cost < scan.cost,
            "a one-row lookup must beat a {realtime}-row scan: index {} vs \
             scan {}",
            non_covering.cost,
            scan.cost
        );
        let whole_table = index_path(
            &table,
            &index,
            vec![IndexRange {
                low: vec![Datum::MinNotNull],
                high: vec![Datum::MaxValue],
                low_exclusive: false,
                high_exclusive: false,
            }],
            &[0],
            None,
            None,
            realtime,
            realtime,
            None,
        );
        assert!(
            whole_table.cost > scan.cost,
            "and a double read of the whole table must lose to scanning it: \
             index {} vs scan {}",
            whole_table.cost,
            scan.cost
        );

        // BROAD END, where the crossover actually lives: the table side
        // dominates and the request term is the minority, so the constant
        // does NOT set the crossover -- Go's per-row table scan and net costs
        // do, and those are ported at full rate. This is why the live
        // differential agrees with Go on every broad case.
        let many_rows = whole_table.estimate.rows;
        let broad_price = whole_table.cost - scan.cost;
        assert!(
            request_term(many_rows) < broad_price,
            "at {many_rows} rows the table side, not the request term ({}), \
             is what puts the double read over the scan (by {broad_price})",
            request_term(many_rows)
        );

        // And the penalty, on top of all of it, only ever moves the scan
        // AWAY from being chosen -- it is added to the scan's own price and
        // to nothing else. The whole-table double read still loses, which is
        // the direction that matters: a rule that priced risk by making the
        // risky path cheaper would be backwards.
        let penalized = table_scan_path(
            &table,
            &all_columns,
            None,
            &NoResolver,
            None,
            realtime,
            None,
            None,
            false,
            table_scan_penalty_rows(None, realtime, false, false),
        );
        assert!(penalized.cost > scan.cost);
        assert!(whole_table.cost > penalized.cost);
    }

    /// Go `getTableScanPenalty` (`pkg/planner/core/plan_cost_ver2.go`), branch
    /// by branch. The rows are what Go returns, not a cost: the caller turns
    /// them into one by scanning them a second time.
    #[test]
    fn the_full_scan_penalty_is_gos_branch_table() {
        // Pseudo statistics -- `hasUnreliableStats` -- is the branch every
        // un-analyzed table in the replay takes. `max(MaxPenaltyRowCount,
        // rows)` with `modify_count` at zero.
        assert_eq!(
            table_scan_penalty_rows(None, 10000.0, false, false),
            10000.0
        );
        // The floor bites on a table smaller than it: an un-analyzed
        // hundred-row table is charged a thousand.
        assert_eq!(table_scan_penalty_rows(None, 100.0, false, false), 1000.0);

        // A table with real, fresh statistics and no hint pays nothing --
        // this is the arm that keeps the 1998 recorded `TableFullScan` lines
        // full scans.
        let analyzed = |modify_count: i64| TableStatistics {
            pseudo: false,
            row_count: 10000,
            modify_count,
            columns: BTreeMap::new(),
            indexes: BTreeMap::new(),
            load_state: Arc::default(),
        };
        // `analyze_row_count` of a table with no histogram is Go's `-1`, which
        // is itself `hasUnreliableStats`; a table that reaches the no-penalty
        // arm must have an analyzed count above zero, so this pins the
        // OTHER direction -- that a `pseudo: false` flag alone is not enough.
        assert_eq!(
            table_scan_penalty_rows(Some(&analyzed(0)), 10000.0, false, false),
            10000.0
        );

        // Go's `hasIndexForce`: a `USE`/`FORCE INDEX` anywhere in the
        // statement penalizes every full scan in it, whatever the statistics
        // say.
        assert_eq!(table_scan_penalty_rows(None, 10.0, true, false), 1000.0);

        // Go's `hasPartitionScan`: a pruned partition stops at the floor
        // instead of being raised to the whole table's `modify_count`.
        assert_eq!(
            table_scan_penalty_rows(Some(&analyzed(50000)), 10.0, false, true),
            1000.0
        );
        assert_eq!(
            table_scan_penalty_rows(Some(&analyzed(50000)), 10.0, false, false),
            50000.0
        );
    }
}
