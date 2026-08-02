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
//! `IndexFullScan`. [`covering_full_scan_candidate`] builds that candidate
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
//! What is still deferred is the ORDER a covering index can supply: Go reads
//! it backwards for an `ORDER BY ... DESC` and stops at the `LIMIT`
//! (`keep order:true, desc`), where this tier scans forwards and sorts.

use std::collections::BTreeMap;

use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode};
use tidb_planner::cardinality::pseudo::{
    pseudo_row_count_by_index_ranges, pseudo_row_count_by_scalar_ranges, pseudo_selectivity,
    IndexRange as PseudoIndexRange, PseudoBoundKind, PseudoColumn, PseudoFunctionKind, PseudoIndex,
    PseudoPredicate, ScalarRange,
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
use tidb_planner::selectivity_greedy::{
    combine_selectivity, ConditionKind, SelectivityDefaults, StatsNode, StatsNodeType,
};

use crate::kv_table::{IndexRange, KvColumn, KvIndex, KvTable};
use crate::plan_trace::PSEUDO_ROW_COUNT;
use crate::skyline::{skyline_pruning, Candidate, ColSet, PruningContext};

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
        }
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
    /// Whether an index over these column offsets, with this many ranges,
    /// establishes the order the `LIMIT` selects from -- Go's physical
    /// `SortItems` property on the index task.
    pub(crate) satisfied_by: &'a dyn Fn(&[usize], bool) -> bool,
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
    /// `plan_cost_ver2.go`'s cost of the reader this path lowers to.
    pub(crate) cost: f64,
    /// How far the estimator admits [`ScanEstimate::rows`] could be wrong, and
    /// the rows left after the index's own filters -- Go's `AccessPath`
    /// `MinCountAfterAccess` / `MaxCountAfterAccess` / `CountAfterIndex`. Only
    /// skyline pruning's risk dimension reads these; the cost formula does
    /// not, exactly as in Go.
    pub(crate) risk: RowRisk,
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

/// Go `getAvgRowSize`'s per-column input for one table column.
fn row_size_column(column: &KvColumn, stats: Option<&TableStatistics>) -> RowSizeColumn {
    let kind = row_size_type(&column.field_type);
    let width = kind.estimate_width(column.field_type.flen());
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
        FieldTypeCode::Date | FieldTypeCode::NewDate => RowSizeType::Date,
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
pub(crate) fn enumerate_paths(
    table: &KvTable,
    columns: &[(String, FieldType)],
    where_clause: Option<&tidb_ast::Expr>,
    needed_columns: &[usize],
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    limit: Option<&PushedLimit<'_>>,
    stats: Option<&TableStatistics>,
) -> Vec<Candidate<AccessPath>> {
    let realtime = realtime_row_count(stats);
    let source_rows = source_row_count(table, where_clause, resolver, stats, realtime);
    // Go's `deriveTablePathStats`: the table path's own ranges, over the
    // clustered integer handle. Nothing here narrows what is EVALUATED -- the
    // `WHERE` stays in the pipeline above the source -- so a range only ever
    // decides how much is read and what the path is costed at.
    let handle =
        where_clause.and_then(|clause| crate::handle_range::build_handle_ranges(table, clause));
    let handle_ranges = handle.as_ref().map(|built| built.ranges.clone());
    let table_scan = table_scan_path(
        table,
        where_clause,
        resolver,
        stats,
        realtime,
        handle_ranges.as_deref(),
    );
    // Go's `getTableCandidate`: a table path is always a single scan, and it
    // is the full range exactly when the ranger built nothing. Its access
    // column is the handle itself, which is what lets skyline pruning see
    // that a narrowed table path accesses a column an unrelated index does
    // not.
    let handle_access_columns: ColSet = handle
        .as_ref()
        .and_then(|_| table.pk_handle_offset())
        .into_iter()
        .collect();
    let mut candidates = vec![Candidate {
        access_columns: handle_access_columns,
        index_columns: ColSet::new(),
        single_scan: true,
        eq_or_in_count: handle.as_ref().map_or(0, |built| built.eq_or_in_count),
        full_range: handle_ranges.is_none(),
        count_after_access: table_scan.estimate.rows,
        max_count_after_access: table_scan.risk.max,
        min_count_after_access: table_scan.risk.min,
        count_after_index: table_scan.risk.after_index,
        pseudo: is_pseudo(stats),
        index_width: 0,
        empty_range: handle_ranges.as_ref().is_some_and(Vec::is_empty),
        index_filter_count: 0,
        table_filter_count: 0,
        path: table_scan,
    }];
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
    for index in table.plan_indexes() {
        // An index key part is named by the TABLE, not by the query's scope:
        // the hidden column an expression index was rewritten into is a
        // column of the table that no query can see, so the scope has no
        // entry at its offset.
        let index_columns: Vec<crate::index_range::RangeColumn> = index
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
        let built = range_clause.and_then(|range_clause| {
            crate::index_range::detach_cond_and_build_range_for_index(&index_columns, range_clause)
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
            if let Some(candidate) = covering_full_scan_candidate(
                table,
                index,
                where_clause,
                needed_columns,
                resolver,
                limit,
                stats,
                realtime,
            ) {
                candidates.push(candidate);
            }
            continue;
        };
        let empty_range = built.ranges.is_empty();
        let access_columns: ColSet = built
            .access_columns
            .iter()
            .filter_map(|position| index.column_offsets.get(*position).copied())
            .collect();
        let full_index_columns = full_index_columns(index, table);
        let (index_columns_map, index_filters, table_filter_count) =
            split_index_filter_conditions(&built.residual, &full_index_columns, resolver);
        let index_filter_count = index_filters.len();
        let index_filter_selectivity = (!index_filters.is_empty())
            .then(|| selectivity_of_conjuncts(&index_filters, table, resolver, stats));
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
            count_after_access: path.estimate.rows,
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
            path,
        });
    }
    candidates
}

/// The candidate for reading the WHOLE of a covering index: Go's access path
/// over `ranger.FullRange()`, kept by `skylinePruning`'s `path.IsSingleScan`
/// arm even though the ranger produced no access condition for it.
///
/// `None` when the index does not cover `needed_columns`, because such a path
/// reads every index entry AND looks up every row, which the table scan
/// dominates. Go reaches the same answer through `keepIndex`.
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
fn covering_full_scan_candidate(
    table: &KvTable,
    index: &KvIndex,
    where_clause: Option<&tidb_ast::Expr>,
    needed_columns: &[usize],
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    limit: Option<&PushedLimit<'_>>,
    stats: Option<&TableStatistics>,
    realtime: f64,
) -> Option<Candidate<AccessPath>> {
    if !is_covering(index, table, needed_columns) {
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
        .then(|| selectivity_of_conjuncts(&index_filters, table, resolver, stats));
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
        single_scan: true,
        eq_or_in_count: 0,
        full_range: true,
        count_after_access: path.estimate.rows,
        max_count_after_access: path.risk.max,
        min_count_after_access: path.risk.min,
        count_after_index: path.risk.after_index,
        pseudo: stats.is_none_or(|stats| !stats.indexes.contains_key(&index.id)),
        index_width: index.column_offsets.len(),
        empty_range: false,
        index_filter_count,
        table_filter_count,
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
    where_clause: Option<&tidb_ast::Expr>,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
    realtime: f64,
    ranges: Option<&[IndexRange]>,
) -> AccessPath {
    let row_columns: Vec<RowSizeColumn> = table
        .columns
        .iter()
        .map(|column| row_size_column(column, stats))
        .collect();
    let row_size = get_table_avg_row_size(
        &row_columns,
        is_pseudo(stats),
        realtime as i64,
        RowSizeStore::TiKv,
        table.pk_handle_offset().is_some(),
        false,
    );
    // Go `deriveTablePathStats`: with no access condition the range is the
    // full one and `CountAfterAccess` is the realtime count, taken without
    // consulting the estimator at all ("Skip the expensive
    // GetRowCountByIntColumnRanges call in this case"). With one, it is the
    // estimate over the CONVERTED ranges.
    let count_after_access = match ranges {
        None => realtime,
        Some(ranges) => crate::handle_range::handle_range_row_count(table, ranges, stats),
    };
    // Go costs the scan at the rows it READS and the reader's net transfer at
    // the rows that leave the cop task, which is after the pushed Selection.
    let scan_rows = count_after_access.max(MIN_NUM_ROWS);
    let scanned = scan_cost(scan_rows, row_size.max(MIN_ROW_SIZE));
    let after_filter = source_row_count(table, where_clause, resolver, stats, realtime);
    let transferred = net_cost(after_filter, row_size.max(MIN_ROW_SIZE));
    AccessPath {
        index: None,
        table_ranges: ranges.map(<[IndexRange]>::to_vec),
        estimate: ScanEstimate {
            rows: count_after_access,
            pseudo: is_pseudo(stats),
        },
        cost: (scanned + transferred) / DIST_SQL_SCAN_CONCURRENCY,
        // Go leaves all three at zero for a table path, and
        // `adjustCountAfterAccess` -- the only thing that could move them --
        // is not ported here (see [`source_row_count`]). `CountAfterIndex` is
        // documented as meaningless for a table path either way.
        risk: RowRisk::default(),
    }
}

/// The rows the whole data source is estimated to produce, Go's
/// `ds.StatsInfo().RowCount`: the table's realtime count narrowed by EVERY
/// pushed condition, estimated per column.
///
/// # Why `adjustCountAfterAccess` is NOT ported on top of this
///
/// Go's `adjustCountAfterAccess` (`pkg/planner/core/stats.go`) raises a path's
/// `CountAfterAccess` to `RowCount / SelectionFactor` whenever the path's own
/// access estimate falls BELOW this number, and keeps the pre-adjustment value
/// as `MinCountAfterAccess`. It is excluded here because THIS number is not
/// yet Go's.
///
/// Under pseudo statistics Go reaches its min-based `pseudoSelectivity` only
/// when the `HistColl` has no column and no index entry at all; otherwise it
/// runs the full `Selectivity`, which MULTIPLIES the pseudo rates. This tier
/// always takes the min. On `t(bucket, rare)` with 2000 rows and no analyzed
/// histogram, `WHERE bucket = 1 AND rare = 7` gives Go `0.01` rows for the
/// data source and this tier `10`, so a ported adjustment fires here where
/// Go's does not: the live differential showed the chosen path's `estRows`
/// going to `10 / 0.8 = 12.50` against Go's `0.10`, with both nodes still
/// choosing `idx_cover`.
///
/// So the adjustment is held out until the pseudo selectivity it reads is
/// Go's. Direction of the exclusion, for `compareRiskRatio`: a path's
/// `MinCountAfterAccess` stays at the estimator's own minimum instead of the
/// pre-adjustment estimate, which only narrows Go's second branch -- the one
/// that needs `MinCountAfterAccess > 0` AND a lower interval. It cannot make
/// this port prune a path Go keeps.
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
    let bounds = index_row_count(index, table, &ranges, stats, realtime);
    let estimated = bounds.est;
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
        Some(limit) if (limit.satisfied_by)(index.ordered_column_offsets(), ranges.len() == 1) => {
            estimated.min(limit.cap)
        }
        _ => estimated,
    };
    let index_row_columns: Vec<RowSizeColumn> = index
        .column_offsets
        .iter()
        .filter_map(|offset| table.columns.get(*offset))
        .map(|column| row_size_column(column, stats))
        .collect();
    let index_row_size = get_index_avg_row_size(
        &index_row_columns,
        is_pseudo(stats),
        realtime as i64,
        index.unique,
        false,
    );
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
    let index_net = net_cost(after_index.min(rows), index_row_size);
    let index_side = (index_scan + index_net) / DIST_SQL_SCAN_CONCURRENCY;

    let covering = is_covering(index, table, needed_columns);
    let cost = if covering {
        index_side
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
        index_side + (table_side + double_read_cpu + double_read_request) / INDEX_LOOKUP_CONCURRENCY
    };
    AccessPath {
        index: Some((index.id, ranges)),
        table_ranges: None,
        estimate: ScanEstimate {
            rows: estimated,
            pseudo: is_pseudo(stats),
        },
        cost,
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
/// path over such a table covers the handle column too.
///
/// A PREFIX key part does not count: its entry holds `'abc'` where the row
/// holds `'abcdef'`, so answering from it is the truncation this feature
/// exists to avoid. Go says the same in `isIndexColsCoveringCol`
/// (`pkg/planner/core/operator/logicalop/logical_datasource.go`), which
/// accepts a key part only when its length is unspecified or already reaches
/// the column's `Flen`. Captured: `explain select a from t where a =
/// 'abcdef'` over `key idx(a(3))` is an `IndexLookUp`, not an `IndexReader`.
fn is_covering(index: &KvIndex, table: &KvTable, needed_columns: &[usize]) -> bool {
    needed_columns.iter().all(|offset| {
        let flen = table
            .columns
            .get(*offset)
            .map_or(-1, |column| column.field_type.flen());
        index.covers(*offset, flen) || table.pk_handle_offset() == Some(*offset)
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
/// DEFERRED (documented): the index nodes of Go's greedy cover (a multi-column
/// index can be a better estimator than the product of its columns), and
/// `crossValidationSelectivity`.
pub(crate) fn selectivity(
    predicate: &tidb_ast::Expr,
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
) -> f64 {
    let mut conjuncts = Vec::new();
    crate::plan_trace::collect_and(predicate, &mut conjuncts);
    selectivity_of_conjuncts(&conjuncts, table, resolver, stats)
}

/// [`selectivity`] over conditions already split out of the `AND` tree, which
/// is the shape Go's `cardinality.Selectivity` takes and the shape the index
/// filters arrive in.
pub(crate) fn selectivity_of_conjuncts(
    conjuncts: &[&tidb_ast::Expr],
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
) -> f64 {
    let Some(stats) = stats.filter(|stats| !stats.pseudo) else {
        return pseudo_conjunct_selectivity(conjuncts, table, resolver, realtime_row_count(stats));
    };
    let realtime = realtime_row_count(Some(stats));
    if realtime <= 0.0 {
        return 1.0;
    }
    let mut total = 1.0;
    for conjunct in conjuncts {
        total *= match column_ranges(conjunct, table, resolver) {
            Some((column_id, collation, ranges)) => {
                let estimate = get_row_count_by_column_ranges(
                    stats.columns.get(&column_id),
                    &ranges,
                    collation,
                    stats.row_count,
                    stats.modify_count,
                    table
                        .pk_handle_offset()
                        .and_then(|offset| table.columns.get(offset))
                        .is_some_and(|column| column.id == column_id),
                    estimator_options(),
                );
                (estimate.est / realtime).clamp(0.0, 1.0)
            }
            None => crate::plan_trace::SELECTIVITY_FACTOR,
        };
    }
    total.clamp(0.0, 1.0)
}

/// `cardinality.Selectivity` for a table with NO loaded histograms -- Go's
/// `PseudoTable`.
///
/// A pseudo table is not a table without statistics as far as `Selectivity`
/// is concerned: `statistics.PseudoTable` fills a `NewPseudoHistogram` entry
/// for every public column and index (`pkg/statistics/table.go:1034-1061`), so
/// `coll.ColNum()` is non-zero and `Selectivity` takes its ORDINARY body --
/// build one statistics node per column the conditions constrain, greedily
/// cover the condition mask, multiply, then charge the leftover conditions
/// their defaults (`selectivity.go:69-73` is the only escape, and it needs
/// more than 63 conditions).
///
/// That is the whole difference from what this function replaced. A minimum
/// over per-operator rates -- Go's `pseudoSelectivity`, which is only reached
/// down the `len(exprs) > 63` arm -- says 10.00 rows for `a = 1 and b = 2`
/// where TiDB prints 1.00, and 3333.33 for `a > 1 and b > 2` where TiDB
/// prints 1111.11, because a minimum cannot compound. The product,
/// the one-row floor, and the string-match defaults all live in
/// [`combine_selectivity`]; the per-column numbers are the pseudo histogram's
/// own rates through [`pseudo_row_count_by_scalar_ranges`].
///
/// `realtime` is `coll.RealtimeCount`: 10000 for a table `mysql.stats_meta`
/// has never counted, and the real count when it has (which is also the state
/// that still prints `stats:pseudo`, decided independently in
/// [`crate::driver::access::full_scan_estimate`] -- this function does not
/// move that line).
fn pseudo_conjunct_selectivity(
    conjuncts: &[&tidb_ast::Expr],
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    realtime: f64,
) -> f64 {
    // `coll.RealtimeCount == 0 || len(exprs) == 0` (`selectivity.go:61`).
    if conjuncts.is_empty() || realtime <= 0.0 {
        return 1.0;
    }

    // `selectivity.go:69-73`: past 63 conditions the mask no longer fits an
    // int64, and the source gives up on nodes entirely.
    if conjuncts.len() > 63 {
        let predicates: Vec<PseudoPredicate> = conjuncts
            .iter()
            .map(|conjunct| pseudo_predicate(conjunct, table, resolver))
            .collect();
        return pseudo_selectivity(
            &predicates,
            &pseudo_unique_indexes(table),
            realtime as i64,
            crate::plan_trace::SELECTIVITY_FACTOR,
        );
    }

    let mut nodes = Vec::with_capacity(conjuncts.len());
    let mut conditions = Vec::with_capacity(conjuncts.len());
    for (index, conjunct) in conjuncts.iter().enumerate() {
        conditions.push(condition_kind(conjunct));
        let Some((column_id, _collation, ranges)) = column_ranges(conjunct, table, resolver) else {
            continue;
        };
        let scalar: Vec<ScalarRange> = ranges
            .iter()
            .map(|range| scalar_range(Some(&range.low), Some(&range.high)))
            .collect();
        nodes.push(StatsNode {
            // Go `getMaskAndSelectivity`: `GetRowCountByColumnRanges(...) /
            // coll.RealtimeCount`, which for a pseudo histogram is the
            // equal/less/between rate.
            selectivity: (pseudo_row_count_by_scalar_ranges(&scalar, realtime) / realtime)
                .clamp(0.0, 1.0),
            ..StatsNode::new(StatsNodeType::Column, column_id, 1_i64 << index, 1)
        });
    }
    combine_selectivity(
        &mut nodes,
        &conditions,
        1.0,
        realtime as i64,
        SelectivityDefaults::default(),
    )
}

/// Which leftover bucket one uncovered condition falls in
/// (`selectivity.go:238-304`).
///
/// The kind is computed for every condition, not just the uncovered ones,
/// because the mask decides which ones are read; a covered condition's kind
/// is never consulted.
fn condition_kind(conjunct: &tidb_ast::Expr) -> ConditionKind {
    match conjunct {
        // Go's `NOT LIKE` is `unaryNot(like(...))`, which the source unwraps
        // before classifying; this AST carries the negation on the node.
        tidb_ast::Expr::Like { not, .. } | tidb_ast::Expr::Regexp { not, .. } => {
            if *not {
                ConditionKind::NegatedStringMatch
            } else {
                ConditionKind::StringMatch
            }
        }
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, _, _) => ConditionKind::Disjunction,
        _ => ConditionKind::Other,
    }
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
    let Some(offsets) = crate::column_prune::expr_column_offsets(column, resolver) else {
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

/// One conjunct as single-column ranges over a table column, when it is one.
///
/// The extraction is [`crate::index_range`]'s -- the same detacher port the
/// access paths use -- run against a one-column "index" made of the column
/// the conjunct names, so there is exactly one range algebra in this crate.
fn column_ranges(
    conjunct: &tidb_ast::Expr,
    table: &KvTable,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> Option<(i64, Collation, Vec<ColumnRange>)> {
    let [offset] = crate::column_prune::expr_column_offsets(conjunct, resolver)?[..] else {
        return None;
    };
    let column = table.columns.get(offset)?;
    let built = crate::index_range::detach_cond_and_build_range_for_index(
        // A per-column range for the risk model, over the COLUMN itself: no
        // index and therefore no declared length is in play here.
        &[crate::index_range::RangeColumn::whole(
            column.name.clone(),
            column.field_type.clone(),
        )],
        conjunct,
    )?;
    let ranges = built
        .ranges
        .iter()
        .map(|range| ColumnRange {
            low: range.low.first().cloned().unwrap_or(Datum::MinNotNull),
            high: range.high.first().cloned().unwrap_or(Datum::MaxValue),
            low_exclude: range.low_exclusive,
            high_exclude: range.high_exclusive,
        })
        .collect();
    Some((column.id, column.field_type.collation(), ranges))
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
    use tidb_expr::rewriter::NoResolver;

    fn long_column(name: &str, id: i64) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            default_value: None,
            origin_default: None,
            generated: None,
        }
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
            default_value: None,
            origin_default: None,
            generated: None,
        };
        column.field_type.set_flen(20);
        let table = KvTable::with_storage(78, vec![column], Box::new(MemTableStorage::new()));
        let index = |prefix_len: i64| KvIndex {
            id: 1,
            name: "idx".to_owned(),
            unique: false,
            column_offsets: vec![0],
            prefix_lengths: vec![prefix_len],
            visible: true,
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
            .create_index(
                KvIndex {
                    id: 1,
                    name: "ib".to_owned(),
                    unique: false,
                    column_offsets: vec![1],
                    prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
                    visible: true,
                },
                &tidb_expr::NoColumns,
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
        let scan = table_scan_path(&table, None, &NoResolver, None, realtime, None);
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
    }
}
