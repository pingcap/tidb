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
//! An enumeration gap that pruning makes visible, and that this module still
//! has: Go keeps a covering index with NO access conditions
//! (`keepIndex := ... || path.IsSingleScan`), which then dominates the table
//! path, so `SELECT bucket FROM t` reads `IndexFullScan` on `idx_bucket`
//! where this tier reads the whole table. That is an enumeration refusal --
//! [`enumerate_paths`] only builds an index candidate the detacher ranged --
//! and it costs rows read, not correctness.

use std::collections::BTreeMap;

use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode};
use tidb_planner::cardinality::pseudo::{
    pseudo_row_count_by_index_ranges, IndexRange as PseudoIndexRange, PseudoBoundKind, ScalarRange,
};
use tidb_planner::cardinality::row_count_estimator::{
    get_index_row_count_for_stats_v2, get_row_count_by_column_ranges, ColumnRange, ColumnStats,
    EstimatorOptions, IndexRangeDatums, IndexStats,
};
use tidb_planner::cardinality::row_size::{
    get_index_avg_row_size, get_table_avg_row_size, RowSizeColumn, RowSizeColumnStats,
    RowSizeStore, RowSizeType,
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
    /// Go `HistColl.Pseudo`: the table has a `mysql.stats_meta` row -- so its
    /// ROW COUNT is real -- but not one analyzed column or index histogram,
    /// so its DISTRIBUTION is not.
    ///
    /// The two halves move independently and Go prints them independently: a
    /// table in this state shows a real `estRows` on its `TableFullScan` and
    /// still says `stats:pseudo`, because the scan's row count came from
    /// `stats_meta` while every selectivity below it came from the pseudo
    /// rates. Collapsing the two would either invent a distribution or throw
    /// away a row count the cluster really knows.
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
    /// The index this path reads, and the ranges it reads of it; `None` is
    /// the full table scan.
    pub(crate) index: Option<(i64, Vec<IndexRange>)>,
    /// The estimate `EXPLAIN` prints for the scan node.
    pub(crate) estimate: ScanEstimate,
    /// `plan_cost_ver2.go`'s cost of the reader this path lowers to.
    pub(crate) cost: f64,
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
        // An ANALYZED table that really is empty keeps its zero: it is not
        // pseudo, and `0` is then a measurement.
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
/// The full scan is always a candidate -- Go's `tablePath` is built
/// unconditionally in `DeriveStats` -- and each index contributes one
/// candidate when the detacher produced ranges for it.
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
    let table_scan = table_scan_path(table, where_clause, resolver, stats, realtime);
    // Go's `getTableCandidate`: a table path is always a single scan, its
    // access-condition map is empty because this tier builds no primary-key
    // range (a point read is settled before costing, see the module doc), and
    // its range is therefore always the full one.
    let mut candidates = vec![Candidate {
        access_columns: ColSet::new(),
        index_columns: ColSet::new(),
        single_scan: true,
        eq_or_in_count: 0,
        full_range: true,
        count_after_access: table_scan.estimate.rows,
        pseudo: is_pseudo(stats),
        index_width: 0,
        empty_range: false,
        index_filter_count: 0,
        table_filter_count: 0,
        path: table_scan,
    }];
    let Some(where_clause) = where_clause else {
        return candidates;
    };
    for index in table.plan_indexes() {
        let index_columns: Vec<(String, FieldType)> = index
            .column_offsets
            .iter()
            .filter_map(|offset| columns.get(*offset).cloned())
            .collect();
        if index_columns.len() != index.column_offsets.len() {
            continue;
        }
        let Some(built) =
            crate::index_range::detach_cond_and_build_range_for_index(&index_columns, where_clause)
        else {
            continue;
        };
        let empty_range = built.ranges.is_empty();
        let access_columns: ColSet = built
            .access_columns
            .iter()
            .filter_map(|position| index.column_offsets.get(*position).copied())
            .collect();
        let full_index_columns = full_index_columns(index, table);
        let (index_columns_map, index_filter_count, table_filter_count) =
            split_index_filter_conditions(&built.residual, &full_index_columns, resolver);
        let path = index_path(
            table,
            index,
            built.ranges,
            needed_columns,
            limit,
            stats,
            realtime,
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
/// Returns the index filters' columns, then how many index and table filters
/// there are. A condition whose columns [`crate::column_prune`] refuses to
/// classify counts as a table filter, which is the conservative side: it
/// cannot make an index look like it filters more than it does.
fn split_index_filter_conditions(
    residual: &[&tidb_ast::Expr],
    full_index_columns: &ColSet,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
) -> (ColSet, usize, usize) {
    let mut columns = ColSet::new();
    let (mut index_filters, mut table_filters) = (0, 0);
    for condition in residual {
        let read = crate::column_prune::expr_column_offsets(condition, resolver);
        match read {
            Some(read)
                if read
                    .iter()
                    .all(|offset| full_index_columns.contains(offset)) =>
            {
                columns.extend(read);
                index_filters += 1;
            }
            _ => table_filters += 1,
        }
    }
    (columns, index_filters, table_filters)
}

/// The full-scan candidate: a `PhysicalTableReader` over a
/// `PhysicalTableScan` reading every row.
fn table_scan_path(
    table: &KvTable,
    where_clause: Option<&tidb_ast::Expr>,
    resolver: &dyn tidb_expr::rewriter::ColumnResolver,
    stats: Option<&TableStatistics>,
    realtime: f64,
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
    // Go costs the scan at the rows it READS and the reader's net transfer at
    // the rows that leave the cop task, which is after the pushed Selection.
    let scan_rows = realtime.max(MIN_NUM_ROWS);
    let scanned = scan_cost(scan_rows, row_size.max(MIN_ROW_SIZE));
    let after_filter = match where_clause {
        Some(predicate) => realtime * selectivity(predicate, table, resolver, stats),
        None => realtime,
    };
    let transferred = net_cost(after_filter, row_size.max(MIN_ROW_SIZE));
    AccessPath {
        index: None,
        estimate: ScanEstimate {
            rows: realtime,
            pseudo: is_pseudo(stats),
        },
        cost: (scanned + transferred) / DIST_SQL_SCAN_CONCURRENCY,
    }
}

/// One index candidate, costed as the reader it lowers to.
fn index_path(
    table: &KvTable,
    index: &KvIndex,
    ranges: Vec<IndexRange>,
    needed_columns: &[usize],
    limit: Option<&PushedLimit<'_>>,
    stats: Option<&TableStatistics>,
    realtime: f64,
) -> AccessPath {
    let estimated = index_row_count(index, table, &ranges, stats, realtime);
    // Go costs the index side at `min(rows, PushedLimit.Count)` when the
    // reader stops at the cap; `estimated` is still what EXPLAIN prints for
    // the scan, because the cop-side `Limit` is its own operator there.
    let rows = match limit {
        Some(limit) if (limit.satisfied_by)(&index.column_offsets, ranges.len() == 1) => {
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
    let index_net = net_cost(rows, index_row_size);
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
        estimate: ScanEstimate {
            rows: estimated,
            pseudo: is_pseudo(stats),
        },
        cost,
    }
}

/// Go `isCoveringIndex`: every column the statement reads is stored in the
/// index, so no row lookup is needed.
///
/// An integer primary key is stored in every index entry as its handle, so a
/// path over such a table covers the handle column too.
fn is_covering(index: &KvIndex, table: &KvTable, needed_columns: &[usize]) -> bool {
    needed_columns.iter().all(|offset| {
        index.column_offsets.contains(offset) || table.pk_handle_offset() == Some(*offset)
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
) -> f64 {
    let Some(stats) = stats.filter(|stats| !stats.pseudo) else {
        return pseudo_index_row_count(index, ranges, realtime);
    };
    let Some(index_stats) = stats.indexes.get(&index.id) else {
        // A table WITH statistics whose index was never analyzed: Go falls
        // back to the pseudo rate for that path only, not for the table.
        return pseudo_index_row_count(index, ranges, realtime);
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
    let estimate = get_index_row_count_for_stats_v2(
        index_stats,
        &column_stats,
        &datum_ranges,
        stats.row_count,
        stats.modify_count,
        estimator_options(),
    );
    estimate.est.clamp(0.0, realtime.max(0.0))
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
    let Some(stats) = stats.filter(|stats| !stats.pseudo) else {
        return crate::plan_trace::pseudo_selectivity(predicate);
    };
    let realtime = realtime_row_count(Some(stats));
    if realtime <= 0.0 {
        return 1.0;
    }
    let mut conjuncts = Vec::new();
    crate::plan_trace::collect_and(predicate, &mut conjuncts);
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
        &[(column.name.clone(), column.field_type.clone())],
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
        }
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
            .create_index(KvIndex {
                id: 1,
                name: "ib".to_owned(),
                unique: false,
                column_offsets: vec![1],
                visible: true,
            })
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
        );
        let covering = index_path(
            &table,
            &index,
            vec![point_range()],
            &[1],
            None,
            None,
            realtime,
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
        let scan = table_scan_path(&table, None, &NoResolver, None, realtime);
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
