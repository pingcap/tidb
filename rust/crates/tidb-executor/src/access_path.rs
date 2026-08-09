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

//! The narrowed access paths as streaming source executors: Go's
//! `PointGet`/`Batch_Point_Get` and `IndexRangeScan` + row lookup.
//!
//! # Why these are executors and not a `Vec`
//!
//! Each of these paths reads a *subset* of a table rather than all of it, but
//! a subset is not a bound: `WHERE b > 4` over a ten-million-row index is an
//! index range, and materializing it into a `Vec<Vec<Datum>>` to hand to a
//! `MemTableSourceExec` costs the whole relation in decoded form before the
//! first row leaves the source. These executors hold the cursor instead
//! ([`KvTable::row_cursor`], [`KvTable::index_range_cursor`]) and decode one
//! row per pull, so the decoded rows alive at once are one chunk's worth
//! regardless of how many rows the range covers, and a pushed `LIMIT` never
//! decodes or looks up a row past its cap.
//!
//! # How far down the streaming reaches today
//!
//! The executors are fully pull-based, and so is the row decoding above the
//! [`TableStorage`](crate::storage::TableStorage) seam. Below it, neither
//! backend's `iter` is lazy yet: `MemStorage::iter` copies the range's
//! key/value bytes into a `Vec`, and `ClusterTableStorage::iter` scans the
//! snapshot range and merges the staged buffer into another one. So a scan
//! today holds the range's *packed bytes*, not its decoded rows -- which is
//! the smaller of the two by a wide margin (no per-row `Vec<Datum>`, no
//! per-value allocation), and on the index path the row lookups are avoided
//! outright: a capped scan performs `cap` point reads instead of one per
//! entry in the range.
//!
//! This is the seam, not a workaround: the executors pull one row at a time
//! through `StorageIterator`, so the day a backend's `iter` returns a real
//! lazy cursor (a TiKV region iterator, or a borrowing `MemIterator`) the
//! whole path streams end to end with no change above the seam.
//!
//! # `actRows`
//!
//! Both sources expose a live produced-row counter, which the plan trace
//! reads for the access operator's `actRows`. Without a pushed limit that
//! count is exactly the row count a materializing path would have reported;
//! with one it reports the truncation, as Go's does.

use std::cell::Cell;
use std::rc::Rc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, Decimal, FieldType, SessionTimeZone};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::truthy_of;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::kv_table::{IndexRange, IndexRangeCursor, KvTable, TableHandle};
use crate::remote_scan::{
    PushdownAggregateKind, PushdownPartialAggregate, PushdownRowStream, PushdownStatementContext,
};

/// What a statement declares about its whole read, before its first read
/// happens.
///
/// Go's counterpart is the answer `IsPointGetWithPKOrUniqueKeyByAutoCommit`
/// gives `AdviseOptimizeWithPlan` once per statement, with the statement's
/// ROOT plan. It is a statement-level fact, not a per-read one, and the
/// difference is the whole safety argument: `MaxUint64` reads the latest
/// committed version at the moment of the read, so two reads of one statement
/// at `MaxUint64` are two different snapshots.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatementReadShape {
    /// Nothing is claimed. The snapshot spends a timestamp at its first read,
    /// which is every statement's answer unless it earned the other one.
    #[default]
    Unknown,
    /// The statement's whole read is one point get on the clustered handle,
    /// reading one row once, with no second read of any kind.
    AutocommitPointGet,
}

/// Whether `stmt` is a statement whose WHOLE read is one point get on the
/// clustered handle -- the plan-shape half of Go's
/// `IsPointGetWithPKOrUniqueKeyByAutoCommit`
/// (`pkg/planner/core/common_plans.go`).
///
/// # What this decides, and what it deliberately does not
///
/// Go asks the question of the statement's ROOT plan, and its `switch` is what
/// makes the answer safe: only a `PhysicalProjection` over a reader, a bare
/// reader, or a `PointGetPlan` qualifies, and everything else falls to
/// `default: return false`. A `Selection`, `Apply`, `Limit`, `Sort`,
/// aggregation or join at the root is therefore already a refusal in Go, and
/// so is every statement that is not a query at all. This function is that
/// `switch`, expressed over the statement this tier plans from.
///
/// The two conditions it owns beyond the shape, both of them refusals Go makes
/// elsewhere:
///
/// * The unique-index arm is REFUSED outright. Go admits `PointGetPlan` on a
///   unique index only when `IndexInfo == nil || (Primary && IsCommonHandle)`
///   -- its `noSecondRead` -- because an index point get is a DOUBLE read, and
///   `MaxUint64` on a double read can pair an index entry with a row from a
///   different version. This tier's unique-index arm
///   (`driver::access::try_point_get`) is exactly that double read, and it
///   READS the index while deciding, which a declaration made before the first
///   read cannot do anyway. Refusing it is both halves at once.
/// * `LIMIT` is refused. Go allows `LIMIT n` (`n > 0`, no offset) inside
///   `tryPointGetPlan`, but evaluating the bound is work this predicate would
///   have to duplicate to stay read-free, and a refusal costs one timestamp
///   rather than a wrong row.
///
/// The conditions this function does NOT own, because the caller owns them
/// structurally -- see `ClusterSnapshot::declare_autocommit_point_get`:
///
/// * `IsAutoCommitTxn`: autocommit set and no open transaction. The snapshot
///   an explicit transaction binds refuses the declaration by inheriting the
///   trait's fail-closed default, so being inside `BEGIN` is not a check here
///   at all -- there is nothing to declare to.
/// * "The timestamp is not already spent." The deferred snapshot refuses a
///   declaration once it has opened, which is Go's `p.txn != nil` arm.
///
/// # Why an `UPDATE`'s read-before-write cannot reach this answer
///
/// It is not a `Stmt::Query`, so it is refused on the first line -- the same
/// place Go refuses it, since an `Update` plan is not in Go's `switch` either.
/// That matters because at the storage seam an `UPDATE`'s read-before-write
/// and a `SELECT`'s point get are the SAME `get` on the same key: the
/// difference lives only in the statement, which is why the declaration is
/// made from the statement and never inferred from a read.
#[must_use]
pub fn statement_read_shape(
    stmt: &tidb_ast::Stmt,
    catalog: &crate::driver::Catalog,
    current_db: &str,
    zone: &tidb_datatype::SessionTimeZone,
) -> StatementReadShape {
    let tidb_ast::Stmt::Query(query) = stmt else {
        return StatementReadShape::Unknown;
    };
    // A set operation reads every term, so it is several reads by
    // construction; Go's root plan for it is a `PhysicalUnionAll`, which its
    // `switch` refuses.
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        return StatementReadShape::Unknown;
    };
    if !select_is_bare_point_read(select) {
        return StatementReadShape::Unknown;
    }
    let Some(table) = crate::driver::access::single_kv_table(&select.from, catalog, current_db)
    else {
        return StatementReadShape::Unknown;
    };
    // A partitioned table's point get routes to a partition, which this
    // predicate does not resolve; a refusal costs one timestamp.
    if table.partition().is_some() {
        return StatementReadShape::Unknown;
    }
    let Some(handle_offset) = table.pk_handle_offset() else {
        return StatementReadShape::Unknown;
    };
    let columns: Vec<(String, FieldType)> = table
        .visible_columns()
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    let Some(where_clause) = select.where_clause.as_ref() else {
        return StatementReadShape::Unknown;
    };
    // The rest is `try_point_get`'s handle arm verbatim, through the same two
    // helpers, so a statement this predicate accepts is a statement that arm
    // accepts. It reads nothing: both helpers walk the AST only.
    let mut pairs = Vec::new();
    if !crate::driver::access::name_value_pairs(where_clause, &mut pairs, zone) || pairs.len() != 1
    {
        return StatementReadShape::Unknown;
    }
    if !crate::driver::access::convert_pairs_to_column_domain(&mut pairs, &columns) {
        return StatementReadShape::Unknown;
    }
    let handle_column = &columns[handle_offset].0;
    if !pairs[0].column().eq_ignore_ascii_case(handle_column) {
        return StatementReadShape::Unknown;
    }
    match pairs[0].value() {
        Datum::Int(_) | Datum::UInt(_) => StatementReadShape::AutocommitPointGet,
        _ => StatementReadShape::Unknown,
    }
}

/// Whether the query block carries nothing above the table read: Go's
/// `switch` admitting only a projection over a reader.
///
/// Every clause listed here would put an operator above the reader in Go's
/// root plan and so land in its `default` arm. The select list is held to
/// plain column references and wildcards for the same reason -- a scalar
/// subquery in it is a second read, and an aggregate is a root `HashAgg`.
fn select_is_bare_point_read(select: &tidb_ast::SelectStmt) -> bool {
    use tidb_ast::{SelectField, SelectStatementKind};

    select.kind == SelectStatementKind::Select
        && !select.is_in_braces
        && select.with.is_none()
        && select.values.is_empty()
        && !select.distinct
        && select.group_by.is_empty()
        && !select.rollup
        && select.having.is_none()
        && select.windows.is_empty()
        && select.order_by.is_empty()
        && select.limit.is_none()
        // `FOR UPDATE` locks the row, which needs a real timestamp to lock at.
        && select.lock.is_none()
        && select.into_outfile.is_none()
        && !select.calc_found_rows
        && select.fields.fields().iter().all(|field| match field {
            SelectField::Wildcard(_) => true,
            SelectField::Expr { expr, .. } => matches!(expr, tidb_ast::Expr::Column(_)),
        })
}

/// The prefix of a decoded table row that a source emits.
///
/// `KvTable::get_row_by_handle` decodes and materializes EVERY column of the
/// table, hidden ones included -- the hidden column an expression index was
/// rewritten into has to be computed so an index entry can be written from
/// the same row. A read never wants it: the schema these sources append into
/// is the visible one, and the hidden columns are the row's trailing tail by
/// construction (`KvTable::add_hidden_column`), so the visible row is a
/// prefix rather than a gather.
///
/// `TableScanExec` states the same rule as its `keep` list; this is that rule
/// for the two narrowed sources, which read their rows by handle instead.
fn visible_of<'a>(table: &KvTable, row: &'a [tidb_datatype::Datum]) -> &'a [tidb_datatype::Datum] {
    &row[..row.len().min(table.visible_column_count())]
}

/// Reads rows for an already-known handle list, one per pull: the source
/// behind Go's `PointGet` (one handle) and `Batch_Point_Get` (several).
///
/// The handle list is bounded by the statement text (`a IN (1, 2, 3)`), so it
/// is materialized; the *rows* are not. Go likewise leaves a `Limit` at the
/// root above a `Batch_Point_Get` rather than pushing into it (captured:
/// `Limit_10 | root` over `Batch_Point_Get_12`), which is why this source
/// takes no row cap.
pub struct HandleSourceExec {
    meta: ExecutorMeta,
    table: KvTable,
    handles: Vec<TableHandle>,
    /// The next handle to read.
    cursor: usize,
    /// Rows produced so far, which the trace reads as this node's `actRows`.
    produced: Rc<Cell<u64>>,
    /// The statement-class flags for an origin default plus the session zone
    /// a stored `TIMESTAMP` is read back into, captured where the statement's
    /// context is (`Executor` has none).
    decode_context: crate::kv_table::RowDecodeContext,
    /// Source-row offsets this complete point plan emits, in result order.
    /// `None` keeps the ordinary visible-row schema for residual root work.
    output_offsets: Option<Vec<usize>>,
}

impl HandleSourceExec {
    /// Builds a source over `handles` with an explicit row-decode context.
    #[must_use]
    pub fn new_with_context(
        meta: ExecutorMeta,
        table: KvTable,
        handles: Vec<TableHandle>,
        decode_context: crate::kv_table::RowDecodeContext,
    ) -> Self {
        HandleSourceExec {
            meta,
            table,
            handles,
            cursor: 0,
            produced: Rc::new(Cell::new(0)),
            decode_context,
            output_offsets: None,
        }
    }

    /// Legacy zone-only constructor retained for unmigrated callers. Origin
    /// defaults use the exact former `DEFAULT_STATEMENT_FLAGS` behavior.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        table: KvTable,
        handles: Vec<TableHandle>,
        zone: SessionTimeZone,
    ) -> Self {
        Self::new_with_context(
            meta,
            table,
            handles,
            crate::kv_table::RowDecodeContext::legacy_default(&zone),
        )
    }

    /// Builds Go's complete point plan, projecting source offsets while the
    /// row is read instead of leaving a root Projection above the lookup.
    #[must_use]
    pub fn new_projected_with_context(
        meta: ExecutorMeta,
        table: KvTable,
        handles: Vec<TableHandle>,
        output_offsets: Vec<usize>,
        decode_context: crate::kv_table::RowDecodeContext,
    ) -> Self {
        Self {
            meta,
            table,
            handles,
            cursor: 0,
            produced: Rc::new(Cell::new(0)),
            decode_context,
            output_offsets: Some(output_offsets),
        }
    }

    /// The live count of rows this source produced.
    #[must_use]
    pub fn produced_rows(&self) -> Rc<Cell<u64>> {
        Rc::clone(&self.produced)
    }
}

impl Executor for HandleSourceExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.cursor = 0;
        self.produced.set(0);
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let cap = self.meta.max_chunk_size();
        while req.num_rows() < cap {
            let Some(handle) = self.handles.get(self.cursor) else {
                return Ok(());
            };
            self.cursor += 1;
            // A handle with no row is Go's point get that finds nothing: the
            // plan is right, the row is simply absent.
            let row = self
                .table
                .get_row_by_handle_with_context(handle, &self.decode_context)
                .map_err(|_| ExecError::unsupported("table bytes failed to decode"))?;
            if let Some(row) = row {
                let visible = visible_of(&self.table, &row);
                if let Some(offsets) = &self.output_offsets {
                    for (output, source) in offsets.iter().copied().enumerate() {
                        let value = visible.get(source).ok_or_else(|| {
                            ExecError::unsupported("point-get output column is outside the row")
                        })?;
                        req.append_datum(output, value);
                    }
                } else {
                    for (column, value) in visible.iter().enumerate() {
                        req.append_datum(column, value);
                    }
                }
                self.produced.set(self.produced.get() + 1);
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }

    fn table_access(&mut self) -> Option<&mut dyn crate::table_access::TableAccess> {
        Some(self)
    }
}

/// This source refuses every offer, on purpose, by taking the fail-closed
/// defaults of [`crate::table_access`] wholesale.
///
/// It is listed as an access path rather than left unnegotiable so the refusal
/// is a stated position instead of an omission: the handle list is fixed by the
/// statement text, so a cap would duplicate the `LimitExec` above (Go likewise
/// keeps `Limit_10 | root` over `Batch_Point_Get_12`), the `WHERE` that pinned
/// the handles is still applied above by design (`TryFastPlan` narrows the
/// source, it does not replace the filter), and there is no scanned-row count
/// to report because nothing is scanned.
impl crate::table_access::TableAccess for HandleSourceExec {}

/// Walks a set of index ranges in index order, reading each row it finds:
/// Go's `IndexRangeScan` with the table-row lookup above it, collapsed into
/// one operator because this tier prints one node for the pair.
///
/// # Which order the rows leave in
///
/// The index is *walked* in index-key order within a range, and the ranges in
/// the order the plan lists them. The rows do not necessarily *leave* in that
/// order, and that is Go's rule, not an accident of this tier.
///
/// Go's `IndexLookUpExecutor` collects a BATCH of handles from the index
/// (`indexWorker.extractTaskHandles`, `w.batchSize` entries, doubling per
/// batch up to `@@tidb_index_lookup_size`), and the table read for that batch
/// goes through `buildTableReaderFromHandles(..., canReorderHandles = true)`,
/// which SORTS the batch by handle before turning it into key ranges
/// (`builder.go`: `slices.SortFunc(handles, ...i.Compare(j))`; the comment on
/// `lookupTableTask.indexOrder` states the same thing -- "the handles fetched
/// from index is originally ordered by index, but we need handles to be
/// ordered by itself to do table request"). So the rows of one batch come back
/// in HANDLE order.
///
/// Only then, and only when the plan asked for `keep order:true`, does
/// `tableWorker.executeTask` put the index order back: it looks each row's
/// handle up in `task.indexOrder` and `sort.Sort(task)`s by that rank
/// (`distsql.go`, under `if w.keepOrder`). An UNORDERED double read never
/// takes that second step, so its answer is handle-ascending per batch.
///
/// `can_reorder_handles` is this tier's `canReorderHandles`. The driver clears
/// it through [`Self::answer_in_index_order`] for the paths whose answer IS the
/// index walk, and through
/// [`crate::table_access::TableAccess::accept_keep_order`] when the statement's
/// `ORDER BY` is the one this index path already produces. Cleared, the walk
/// order IS the answer and no sort happens -- the same net effect as Go's
/// sort-then-restore, one pass cheaper.
///
/// # `UnionScan`: why a DIRTY table clears it too
///
/// Go puts a `UnionScanExec` above the reader whenever the open transaction
/// has written the table (`tableHasDirtyContent` ->
/// `session.HasDirtyContent`), and that operator MERGES the snapshot stream
/// with the staged rows by `compare()`. For a double read it orders on
/// `usedIndex` -- the offsets of the INDEX's own columns, filled in by
/// `builder.go`'s `*IndexLookUpExecutor` arm -- and falls through to
/// `handleCols.Compare` when they tie (`pkg/executor/union_scan.go:310`). So a
/// double read inside a dirty transaction answers in index-key-then-handle
/// order in Go, whatever the lookup below it did.
///
/// This tier has no `UnionScan` OPERATOR and does not need one: a transaction
/// stages into a private catalog copy, so the staged rows are already in the
/// one stream this source walks (which is why read-your-own-writes works here
/// without any of this). What the operator contributes that the storage seam
/// does not is the ORDER, and merging one index-ordered stream with another is
/// just that stream -- so the whole of `compare()` reduces, here, to leaving
/// the handle batch unsorted.
///
/// The gate is [`crate::kv_table::KvTable::has_dirty_content`], Go's
/// `HasDirtyContent` narrowed to this tier's staging. It is deliberately NOT
/// "inside a transaction": Go asks per TABLE, so a clean table read inside a
/// dirty transaction still gets no `UnionScan` and still answers in handle
/// order.
pub struct IndexRangeSourceExec {
    meta: ExecutorMeta,
    table: KvTable,
    index_id: i64,
    ranges: Vec<IndexRange>,
    /// The next range to open a cursor over.
    next_range: usize,
    /// The open cursor over `ranges[next_range - 1]`.
    cursor: Option<IndexRangeCursor>,
    /// Rows produced so far, which the trace reads as this node's `actRows`
    /// when no filter was pushed into it.
    produced: Rc<Cell<u64>>,
    /// Rows read from the range before any pushed filter -- the `actRows` the
    /// access operator reports once it filters internally.
    scanned: Rc<Cell<u64>>,
    /// Conjuncts this source took over from the `Selection` above it.
    filter: Option<crate::predicate_pushdown::ScanFilterProbe>,
    /// A pushed row cap (`offset + count`); see [`Executor::accept_scan_limit`].
    limit: Option<u64>,
    /// Go's `canReorderHandles` (`builder.go`): whether this read may answer
    /// in handle order. FALSE for the two reads whose answer is the index
    /// walk itself -- a COVERING path, which is Go's `PhysicalIndexReader`
    /// and builds no handle batch at all, and a `keep order:true` lookup,
    /// which sorts the batch back into index order after reading it. See the
    /// type doc.
    can_reorder_handles: bool,
    /// The current handle batch -- Go's `lookupTableTask.handles`, already
    /// sorted unless [`Self::keep_order`].
    batch: Vec<TableHandle>,
    /// How much of `batch` has been read.
    batch_at: usize,
    /// Go's `indexWorker.batchSize`: how many handles the next batch collects,
    /// doubling per batch up to [`MAX_HANDLE_BATCH`].
    batch_size: usize,
    /// The statement-class flags and session zone the row is decoded under;
    /// the zone also encodes the index probe. See [`HandleSourceExec`].
    decode_context: crate::kv_table::RowDecodeContext,
    /// Statement flags and warning sink carried into a remote DAG request.
    statement: PushdownStatementContext,
    /// Planner estimate used to avoid partial aggregation for point-like work.
    estimated_rows: Option<f64>,
    /// Partial aggregation accepted from the root aggregation executor.
    partial_aggregate: Option<PushdownPartialAggregate>,
    partial_remote: Option<Box<dyn PushdownRowStream>>,
    partial_rows: Option<std::vec::IntoIter<Vec<Datum>>>,
    partial_done: bool,
}

/// Go's `indexWorker.batchSize` at its first batch.
///
/// Go derives it from the chunk's `RequiredRows` and the plan's estimated row
/// count (`IndexLookUpExecutor.calculateBatchSize` -> `CalculateBatchSize`),
/// which for an unbounded read starts at `tidb_max_chunk_size` and is doubled
/// until it covers the estimate. This tier starts at the same 1,024 and does
/// the same doubling BETWEEN batches, but does not consult the row estimate
/// for the FIRST one: the estimate lives in the planner and the batch is only
/// observable as a row-order boundary past 1,024 rows of one index read.
const INIT_HANDLE_BATCH: usize = 1024;

/// Go's `@@tidb_index_lookup_size` default, the cap on the doubling above.
const MAX_HANDLE_BATCH: usize = 20000;

impl IndexRangeSourceExec {
    /// Builds a source over `ranges` with an explicit row-decode context.
    #[must_use]
    pub fn new_with_context(
        meta: ExecutorMeta,
        table: KvTable,
        index_id: i64,
        ranges: Vec<IndexRange>,
        decode_context: crate::kv_table::RowDecodeContext,
    ) -> Self {
        Self::new_with_statement(
            meta,
            table,
            index_id,
            ranges,
            decode_context,
            PushdownStatementContext::default(),
        )
    }

    /// Builds an index source carrying the statement context required by a
    /// real TiKV partial-aggregation request.
    #[must_use]
    pub fn new_with_statement(
        meta: ExecutorMeta,
        table: KvTable,
        index_id: i64,
        ranges: Vec<IndexRange>,
        decode_context: crate::kv_table::RowDecodeContext,
        statement: PushdownStatementContext,
    ) -> Self {
        IndexRangeSourceExec {
            meta,
            table,
            index_id,
            ranges,
            next_range: 0,
            cursor: None,
            produced: Rc::new(Cell::new(0)),
            scanned: Rc::new(Cell::new(0)),
            filter: None,
            limit: None,
            can_reorder_handles: true,
            batch: Vec::new(),
            batch_at: 0,
            batch_size: INIT_HANDLE_BATCH,
            decode_context,
            statement,
            estimated_rows: None,
            partial_aggregate: None,
            partial_remote: None,
            partial_rows: None,
            partial_done: false,
        }
    }

    /// Legacy zone-only constructor retained for unmigrated callers. Origin
    /// defaults use the exact former `DEFAULT_STATEMENT_FLAGS` behavior.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        table: KvTable,
        index_id: i64,
        ranges: Vec<IndexRange>,
        zone: SessionTimeZone,
    ) -> Self {
        Self::new_with_context(
            meta,
            table,
            index_id,
            ranges,
            crate::kv_table::RowDecodeContext::legacy_default(&zone),
        )
    }

    /// The live count of rows this source produced.
    #[must_use]
    pub fn produced_rows(&self) -> Rc<Cell<u64>> {
        Rc::clone(&self.produced)
    }

    /// Declares that this read's answer IS its index walk, so no handle batch
    /// is ever sorted. Two source rules reach it, and they are different
    /// rules about the same order:
    ///
    /// * a COVERING path -- Go's `path.IsSingleScan`, which lowers to a
    ///   `PhysicalIndexReader` that never builds a handle batch at all. This
    ///   tier reads the row through the handle either way, because it has no
    ///   index-only reader, so the declaration is what keeps the ORDER of the
    ///   two readers apart;
    /// * a DIRTY table -- Go's `UnionScanExec` above the reader, whose
    ///   `compare()` re-imposes index-key-then-handle order on whatever the
    ///   lookup below it produced.
    ///
    /// See the type doc for both.
    pub(crate) fn answer_in_index_order(&mut self) {
        self.can_reorder_handles = false;
    }

    /// The next handle to READ A ROW FOR: Go's `lookupTableTask` walk, which
    /// is the index walk regrouped into batches and each batch sorted.
    ///
    /// A pushed cap truncates the batch the way Go's `PushedLimit` truncates
    /// `extractTaskHandles`' output (`distsql.go`: `leftCnt :=
    /// w.PushedLimit.Offset + w.PushedLimit.Count - w.scannedKeys`) -- BEFORE
    /// the sort, so the rows a `LIMIT` keeps are the index-order prefix even
    /// though they are answered in handle order.
    fn next_lookup_handle(&mut self) -> Result<Option<TableHandle>, ExecError> {
        if self.batch_at == self.batch.len() {
            let mut want = self.batch_size;
            if let Some(limit) = self.limit {
                let remaining = limit.saturating_sub(self.produced.get());
                want = want.min(usize::try_from(remaining).unwrap_or(usize::MAX));
            }
            self.batch.clear();
            self.batch_at = 0;
            while self.batch.len() < want {
                let Some(handle) = self.next_handle()? else {
                    break;
                };
                self.batch.push(handle);
            }
            self.batch_size = (self.batch_size * 2).min(MAX_HANDLE_BATCH);
            if self.can_reorder_handles {
                self.batch.sort();
            }
            if self.batch.is_empty() {
                return Ok(None);
            }
        }
        let handle = self.batch[self.batch_at].clone();
        self.batch_at += 1;
        Ok(Some(handle))
    }

    /// The next handle in index order across all ranges, opening the next
    /// range's cursor when the current one runs out.
    fn next_handle(&mut self) -> Result<Option<TableHandle>, ExecError> {
        loop {
            if let Some(cursor) = self.cursor.as_mut() {
                let handle = cursor
                    .next_handle()
                    .map_err(|_| ExecError::unsupported("index bytes failed to decode"))?;
                if let Some(handle) = handle {
                    return Ok(Some(handle));
                }
                self.cursor = None;
            }
            let Some(range) = self.ranges.get(self.next_range).cloned() else {
                return Ok(None);
            };
            self.next_range += 1;
            self.cursor = Some(
                self.table
                    .index_range_cursor(self.index_id, &range, self.decode_context.zone())
                    .map_err(|_| ExecError::unsupported("index range is not scannable"))?,
            );
        }
    }

    /// The next index-range row surviving the pushed filter, for the local
    /// fallback of a partial aggregate request. A remote coprocessor takes
    /// the same request in [`Self::open`]; this path keeps in-memory storage
    /// and tests semantically identical.
    fn next_partial_input_row(&mut self) -> Result<Option<Vec<Datum>>, ExecError> {
        loop {
            let Some(handle) = self.next_lookup_handle()? else {
                return Ok(None);
            };
            let row = self
                .table
                .get_row_by_handle_with_context(&handle, &self.decode_context)
                .map_err(|_| ExecError::unsupported("table bytes failed to decode"))?;
            let Some(row) = row else {
                continue;
            };
            self.scanned.set(self.scanned.get() + 1);
            let row = visible_of(&self.table, &row);
            if let Some(filter) = self.filter.as_mut() {
                if !filter.admits(row)? {
                    continue;
                }
            }
            self.produced.set(self.produced.get() + 1);
            return Ok(Some(row.to_vec()));
        }
    }

    fn local_partial_rows(
        &mut self,
        aggregate: &PushdownPartialAggregate,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        match aggregate {
            PushdownPartialAggregate::Count { input_offset, .. } => {
                let mut count = 0_i64;
                while let Some(row) = self.next_partial_input_row()? {
                    if !matches!(row.get(*input_offset), None | Some(Datum::Null)) {
                        count += 1;
                    }
                }
                Ok(vec![vec![Datum::Int(count)]])
            }
            PushdownPartialAggregate::GroupedStream {
                group_offsets,
                group_types,
                functions,
            } => {
                if group_offsets.len() != group_types.len() || group_offsets.is_empty() {
                    return Err(ExecError::unsupported(
                        "index partial grouped StreamAgg requires typed group keys",
                    ));
                }

                enum PartialValue {
                    Count(i64),
                    Sum(Option<Decimal>),
                    Extreme { value: Option<Datum>, is_max: bool },
                }
                let new_values = || {
                    functions
                        .iter()
                        .map(|function| match function.kind {
                            PushdownAggregateKind::Count => PartialValue::Count(0),
                            PushdownAggregateKind::Sum => PartialValue::Sum(None),
                            PushdownAggregateKind::Min => PartialValue::Extreme {
                                value: None,
                                is_max: false,
                            },
                            PushdownAggregateKind::Max => PartialValue::Extreme {
                                value: None,
                                is_max: true,
                            },
                        })
                        .collect::<Vec<_>>()
                };
                let finish = |groups: Vec<Datum>, values: Vec<PartialValue>| {
                    values
                        .into_iter()
                        .map(|value| match value {
                            PartialValue::Count(count) => Datum::Int(count),
                            PartialValue::Sum(sum) => sum.map_or(Datum::Null, Datum::Decimal),
                            PartialValue::Extreme { value, .. } => value.unwrap_or(Datum::Null),
                        })
                        .chain(groups)
                        .collect::<Vec<_>>()
                };

                let mut rows = Vec::new();
                let mut current: Option<(Vec<u8>, Vec<Datum>, Vec<PartialValue>)> = None;
                while let Some(row) = self.next_partial_input_row()? {
                    let groups = group_offsets
                        .iter()
                        .map(|offset| {
                            row.get(*offset).cloned().ok_or_else(|| {
                                ExecError::unsupported(
                                    "index partial GROUP BY input is outside the scan row",
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let mut key = Vec::new();
                    for (group, field_type) in groups.iter().zip(group_types) {
                        key.extend_from_slice(&crate::hash_agg::group_key_part(
                            &field_type.collation(),
                            group,
                        ));
                        key.push(0xff);
                    }
                    if current
                        .as_ref()
                        .is_some_and(|(current_key, _, _)| current_key != &key)
                    {
                        let (_, previous_groups, previous_values) =
                            current.take().expect("current group exists");
                        rows.push(finish(previous_groups, previous_values));
                    }
                    let (_, _, values) = current.get_or_insert_with(|| (key, groups, new_values()));
                    for (function, value) in functions.iter().zip(values.iter_mut()) {
                        let input = function
                            .input_offset
                            .map(|offset| {
                                row.get(offset).cloned().ok_or_else(|| {
                                    ExecError::unsupported(
                                        "index partial aggregate input is outside the scan row",
                                    )
                                })
                            })
                            .transpose()?;
                        match (value, input) {
                            (PartialValue::Count(count), None) => *count += 1,
                            (PartialValue::Count(_), Some(Datum::Null)) => {}
                            (PartialValue::Count(count), Some(_)) => *count += 1,
                            (PartialValue::Sum(_), None) | (PartialValue::Extreme { .. }, None) => {
                                return Err(ExecError::unsupported(
                                    "only COUNT may omit an index partial aggregate input",
                                ));
                            }
                            (PartialValue::Sum(_), Some(Datum::Null))
                            | (PartialValue::Extreme { .. }, Some(Datum::Null)) => {}
                            (PartialValue::Sum(sum), Some(input)) => {
                                let addend = match input {
                                    Datum::Int(value) => Decimal::from_int(value),
                                    Datum::UInt(value) => Decimal::from_uint(value),
                                    Datum::Decimal(value) => value,
                                    _ => {
                                        return Err(ExecError::unsupported(
                                            "index partial SUM requires integer or decimal input",
                                        ));
                                    }
                                };
                                *sum = Some(match sum.take() {
                                    Some(current) => current.add(&addend),
                                    None => addend,
                                });
                            }
                            (PartialValue::Extreme { value, is_max }, Some(candidate)) => {
                                let replace = value.as_ref().is_none_or(|current| {
                                    tidb_expr::compare_datums(&candidate, current).is_ok_and(
                                        |ordering| {
                                            if *is_max {
                                                ordering.is_gt()
                                            } else {
                                                ordering.is_lt()
                                            }
                                        },
                                    )
                                });
                                if replace {
                                    *value = Some(candidate);
                                }
                            }
                        }
                    }
                }
                if let Some((_, groups, values)) = current {
                    rows.push(finish(groups, values));
                }
                Ok(rows)
            }
            _ => Err(ExecError::unsupported(
                "this index partial aggregation is not supported",
            )),
        }
    }
}

impl Executor for IndexRangeSourceExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.next_range = 0;
        self.cursor = None;
        self.produced.set(0);
        self.scanned.set(0);
        self.batch.clear();
        self.batch_at = 0;
        self.batch_size = INIT_HANDLE_BATCH;
        self.partial_remote = None;
        self.partial_rows = None;
        self.partial_done = false;
        if let Some(aggregate) = self.partial_aggregate.as_ref() {
            self.partial_remote = self
                .table
                .pushdown_index_partial_aggregate_cursor(
                    self.index_id,
                    &self.ranges,
                    aggregate,
                    self.decode_context.zone(),
                    &self.statement,
                )
                .map_err(|_| ExecError::unsupported("index aggregate request failed"))?;
        }
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let cap = self.meta.max_chunk_size();
        if let Some(remote) = self.partial_remote.as_mut() {
            while req.num_rows() < cap {
                let Some(row) = remote.next_row().map_err(|error| {
                    ExecError::unsupported(format!("index aggregate response failed: {error:?}"))
                })?
                else {
                    self.partial_remote = None;
                    self.partial_done = true;
                    break;
                };
                for (column, value) in row.iter().enumerate() {
                    req.append_datum(column, value);
                }
            }
            return Ok(());
        }
        if let Some(aggregate) = self.partial_aggregate.clone() {
            if self.partial_done {
                return Ok(());
            }
            if self.partial_rows.is_none() {
                self.partial_rows = Some(self.local_partial_rows(&aggregate)?.into_iter());
            }
            let rows = self.partial_rows.as_mut().expect("just initialized");
            while req.num_rows() < cap {
                let Some(row) = rows.next() else {
                    self.partial_rows = None;
                    self.partial_done = true;
                    break;
                };
                for (column, value) in row.iter().enumerate() {
                    req.append_datum(column, value);
                }
            }
            return Ok(());
        }
        while req.num_rows() < cap {
            if self.limit.is_some_and(|limit| self.produced.get() >= limit) {
                // Early stop: the cursor is dropped, so no entry past the cap
                // is read and no row past it is looked up.
                self.cursor = None;
                self.next_range = self.ranges.len();
                self.batch.clear();
                self.batch_at = 0;
                return Ok(());
            }
            let Some(handle) = self.next_lookup_handle()? else {
                return Ok(());
            };
            let row = self
                .table
                .get_row_by_handle_with_context(&handle, &self.decode_context)
                .map_err(|_| ExecError::unsupported("table bytes failed to decode"))?;
            // An index entry whose row is gone is not a row: the same
            // `if let Some(row)` the materializing path had.
            if let Some(row) = row {
                self.scanned.set(self.scanned.get() + 1);
                let row = visible_of(&self.table, &row);
                if let Some(filter) = self.filter.as_mut() {
                    if !filter.admits(row)? {
                        continue;
                    }
                }
                for (c, value) in row.iter().enumerate() {
                    req.append_datum(c, value);
                }
                self.produced.set(self.produced.get() + 1);
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.cursor = None;
        self.partial_remote = None;
        self.partial_rows = None;
        self.partial_done = false;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }

    fn table_access(&mut self) -> Option<&mut dyn crate::table_access::TableAccess> {
        Some(self)
    }
}

impl crate::table_access::TableAccess for IndexRangeSourceExec {
    fn accept_scan_estimate(&mut self, rows: f64) {
        self.estimated_rows = Some(rows);
    }

    fn accept_partial_aggregate(&mut self, aggregate: &PushdownPartialAggregate) -> bool {
        let supported = matches!(aggregate, PushdownPartialAggregate::Count { .. })
            || (matches!(aggregate, PushdownPartialAggregate::GroupedStream { .. })
                && !self.can_reorder_handles);
        if self.estimated_rows.is_none_or(|rows| rows <= 1.0)
            || !supported
            || self.partial_aggregate.is_some()
            || self.limit.is_some()
        {
            return false;
        }
        let columns = aggregate
            .output_types()
            .into_iter()
            .enumerate()
            .map(|(index, field_type)| {
                let mut column = tidb_expr::column::Column::new((index + 1) as i64, field_type);
                column.index = index as i64;
                column
            })
            .collect();
        self.meta = ExecutorMeta::new(
            Schema::new(columns),
            self.meta.id(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        );
        self.partial_aggregate = Some(aggregate.clone());
        true
    }

    /// The ranges are walked in plan order and each one in index order, so a
    /// cap truncates the same prefix the `LimitExec` above would have kept.
    /// The driver only offers one when nothing above this source filters the
    /// rows (see `run_select_traced`).
    fn accept_scan_limit(&mut self, cap: u64) -> bool {
        self.limit = Some(cap);
        true
    }

    /// The rows this source emits are read through the same storage seam a
    /// full scan reads -- the snapshot with the session's staged mutation
    /// buffer merged in, for both the index entries and the row lookups --
    /// and every one of them is tested here, which is how the staged-row
    /// promise in [`crate::table_access`] is kept.
    fn accept_scan_filter(
        &mut self,
        filter: &crate::predicate_pushdown::PushedScanFilter,
        ctx: &crate::StmtContext,
    ) -> bool {
        if filter.is_empty() {
            return false;
        }
        self.filter = Some(crate::predicate_pushdown::ScanFilterProbe::new(
            filter.clone(),
            ctx.clone(),
            self.meta.new_chunk(),
        ));
        true
    }

    fn scanned_rows_counter(&self) -> Option<Rc<Cell<u64>>> {
        Some(Rc::clone(&self.scanned))
    }

    /// Go's `keep order:true` on the `IndexRangeScan` of an `IndexLookUp`.
    /// Accepting means the handle batch is read in index order rather than
    /// handle order; see the type doc.
    fn accept_keep_order(&mut self) -> bool {
        self.can_reorder_handles = false;
        true
    }
}

/// Which object an index join's inner side probes once per distinct outer
/// key: an index of the table, or the clustered integer handle.
///
/// Go reaches the two through different builders --
/// `buildDataSource2IndexScanByIndexJoinProp` and
/// `buildDataSource2TableScanByIndexJoinProp` -- and prints them as
/// `IndexRangeScan` and `TableRangeScan`. The difference here is only which
/// cursor a probe opens; everything above is one path.
#[derive(Clone, Debug)]
pub enum LookupObject {
    /// Go's `IndexRangeScan ... range: decided by [eq(idx_col, outer_key)]`:
    /// a point range over the index's leading columns.
    Index(i64),
    /// Go's `TableRangeScan ... range: decided by [outer_key]`: the outer
    /// key IS the handle, so the probe is a point read.
    Handle,
    /// Go's clustered common-handle table path. Each probe supplies the
    /// complete primary-key tuple and addresses the record directly, without
    /// manufacturing a secondary PRIMARY index entry.
    CommonHandle,
}

/// One object-key component of an index-join probe.
///
/// Go's ranger can combine `eq(inner_col, outer_col)` with constant access
/// conditions on earlier key columns. `Dynamic` indexes the join-key tuple
/// supplied by `JoinExec`; `Constant` is copied into every probe.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LookupProbePart {
    Dynamic(usize),
    Constant(Datum),
}

/// The inner side of an index join: the rows of one table whose join-key
/// columns equal one of a batch of probe values.
///
/// This is Go's inner executor, the one `IndexJoinExecutorBuilder
/// .BuildExecutorForIndexJoin` rebuilds for every outer batch. Here it is
/// built once and *re-seeded* per batch ([`Self::set_probes`]), which is the
/// same contract -- the probe list is the only thing that changes between
/// batches -- without rebuilding the storage handle each time.
///
/// The probe list is the caller's promise: it must already be the DEDUPED,
/// SORTED, inner-column-typed key list Go's `sortAndDedupLookUpContents`
/// produces. This source does not re-check it, because the encoding that
/// makes two probes equal is the caller's (`constructDatumLookupKey`'s
/// `ConvertTo` + `Compare`), and a second answer here could only disagree.
pub struct IndexJoinLookupExec {
    meta: ExecutorMeta,
    table: KvTable,
    object: LookupObject,
    /// The complete object-key template. Empty preserves the legacy contract
    /// where the dynamic join-key tuple already is the complete probe.
    probe_parts: Vec<LookupProbePart>,
    /// The current outer batch's distinct probe tuples, in walk order.
    probes: Vec<Vec<Datum>>,
    /// The next probe to open a cursor over.
    next_probe: usize,
    /// The open cursor over `probes[next_probe - 1]` (index object only).
    cursor: Option<IndexRangeCursor>,
    /// Rows produced since `open`, which the trace reads as `actRows`.
    produced: Rc<Cell<u64>>,
    /// See [`HandleSourceExec`].
    decode_context: crate::kv_table::RowDecodeContext,
    /// Leaf-local predicates that Go places below the index join's inner
    /// reader. They are evaluated over the same full table row this source
    /// returns, so replacing the originally-built leaf cannot drop them.
    filters: Vec<Expression>,
    filter_context: Option<crate::StmtContext>,
    filter_chunk: Chunk,
}

impl IndexJoinLookupExec {
    /// Builds a lookup source over `object` of `table`, with no probes yet:
    /// before the first batch is seeded it is an empty relation, which is
    /// what an index join whose outer side produced no rows must read.
    #[must_use]
    pub fn new_with_context(
        meta: ExecutorMeta,
        table: KvTable,
        object: LookupObject,
        decode_context: crate::kv_table::RowDecodeContext,
    ) -> Self {
        let filter_chunk = meta.new_chunk();
        IndexJoinLookupExec {
            meta,
            table,
            object,
            probe_parts: Vec::new(),
            probes: Vec::new(),
            next_probe: 0,
            cursor: None,
            produced: Rc::new(Cell::new(0)),
            decode_context,
            filters: Vec::new(),
            filter_context: None,
            filter_chunk,
        }
    }

    /// Legacy zone-only constructor retained for unmigrated callers. Origin
    /// defaults use the exact former `DEFAULT_STATEMENT_FLAGS` behavior.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        table: KvTable,
        object: LookupObject,
        zone: SessionTimeZone,
    ) -> Self {
        Self::new_with_context(
            meta,
            table,
            object,
            crate::kv_table::RowDecodeContext::legacy_default(&zone),
        )
    }

    /// Seeds the next outer batch's probe list and rewinds the walk.
    ///
    /// `produced` is NOT reset: it accumulates across batches, because the
    /// operator EXPLAIN prints is the inner reader as a whole and Go's
    /// `actRows` for it is the total over every batch, not the last one's.
    pub(crate) fn set_probes(&mut self, probes: Vec<Vec<Datum>>) {
        self.probes = probes;
        self.next_probe = 0;
        self.cursor = None;
    }

    /// Installs the complete object-key shape built by the index ranger.
    pub(crate) fn set_probe_parts(&mut self, probe_parts: Vec<LookupProbePart>) {
        self.probe_parts = probe_parts;
    }

    /// Installs every predicate local to the looked-up leaf.
    pub(crate) fn set_filters(&mut self, filters: Vec<Expression>, context: crate::StmtContext) {
        self.filters = filters;
        self.filter_context = Some(context);
    }

    /// The live count of rows this source produced.
    #[must_use]
    pub fn produced_rows(&self) -> Rc<Cell<u64>> {
        Rc::clone(&self.produced)
    }

    /// The next handle the probe list reaches, opening the next probe's
    /// cursor when the current one runs out.
    fn next_handle(&mut self) -> Result<Option<TableHandle>, ExecError> {
        loop {
            if let Some(cursor) = self.cursor.as_mut() {
                let handle = cursor
                    .next_handle()
                    .map_err(|_| ExecError::unsupported("index bytes failed to decode"))?;
                if let Some(handle) = handle {
                    return Ok(Some(handle));
                }
                self.cursor = None;
            }
            let Some(dynamic_probe) = self.probes.get(self.next_probe).cloned() else {
                return Ok(None);
            };
            self.next_probe += 1;
            let probe = if self.probe_parts.is_empty() {
                dynamic_probe
            } else {
                let Some(probe) = self
                    .probe_parts
                    .iter()
                    .map(|part| match part {
                        LookupProbePart::Dynamic(offset) => dynamic_probe.get(*offset).cloned(),
                        LookupProbePart::Constant(value) => Some(value.clone()),
                    })
                    .collect::<Option<Vec<_>>>()
                else {
                    continue;
                };
                probe
            };
            match &self.object {
                LookupObject::Index(index_id) => {
                    // Go's `IndexRangeScan` over the range the outer row
                    // decided: a POINT range over the key columns, both
                    // bounds the probe itself and neither excluded.
                    let range = IndexRange {
                        low: probe.clone(),
                        high: probe,
                        low_exclusive: false,
                        high_exclusive: false,
                    };
                    self.cursor = Some(
                        self.table
                            .index_range_cursor(*index_id, &range, self.decode_context.zone())
                            .map_err(|_| ExecError::unsupported("index range is not scannable"))?,
                    );
                }
                LookupObject::Handle => {
                    // The probe IS the handle. A value that is not an
                    // integer handle reads nothing rather than erroring:
                    // Go's `BuildTableRange` produces an empty range for it,
                    // and an empty range is no rows.
                    let [value] = probe.as_slice() else {
                        continue;
                    };
                    let Some(handle) = handle_of(value) else {
                        continue;
                    };
                    return Ok(Some(handle));
                }
                LookupObject::CommonHandle => {
                    let handle = self
                        .table
                        .common_handle_of_values(&probe, self.decode_context.zone())
                        .map_err(|_| {
                            ExecError::unsupported("common handle probe failed to encode")
                        })?;
                    return Ok(Some(handle));
                }
            }
        }
    }

    fn row_passes_filters(&mut self, row: &[Datum]) -> Result<bool, ExecError> {
        if self.filters.is_empty() {
            return Ok(true);
        }
        let context = self
            .filter_context
            .as_ref()
            .expect("a filtered lookup source has a statement context");
        self.filter_chunk.reset();
        for (offset, value) in row.iter().enumerate() {
            self.filter_chunk.append_datum(offset, value);
        }
        let chunk_row = self.filter_chunk.get_row(0);
        for filter in &self.filters {
            if truthy_of(&filter.eval(context, chunk_row)?)? != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// The clustered integer handle a probe value names, or `None` when no row
/// can carry it.
fn handle_of(value: &Datum) -> Option<TableHandle> {
    match value {
        Datum::Int(v) => Some(TableHandle::Int(*v)),
        Datum::UInt(v) => i64::try_from(*v).ok().map(TableHandle::Int),
        _ => None,
    }
}

impl Executor for IndexJoinLookupExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.next_probe = 0;
        self.cursor = None;
        self.produced.set(0);
        self.filter_chunk.reset();
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let cap = self.meta.max_chunk_size();
        while req.num_rows() < cap {
            let Some(handle) = self.next_handle()? else {
                return Ok(());
            };
            let row = self
                .table
                .get_row_by_handle_with_context(&handle, &self.decode_context)
                .map_err(|_| ExecError::unsupported("table bytes failed to decode"))?;
            // An index entry whose row is gone is not a row, as in
            // [`IndexRangeSourceExec`].
            if let Some(row) = row {
                let visible = visible_of(&self.table, &row);
                if !self.row_passes_filters(&visible)? {
                    continue;
                }
                for (c, value) in visible.iter().enumerate() {
                    req.append_datum(c, value);
                }
                self.produced.set(self.produced.get() + 1);
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.cursor = None;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tidb_datatype::{Datum, FieldTypeCode};
    use tidb_txnkv::Key;

    use super::*;
    use crate::driver::{run_select_on, Catalog};
    use crate::explain::ExplainFormat;
    use crate::kv_table::{KvColumn, KvTable};
    use crate::storage::{MemTableStorage, StorageError, StorageIterator, TableStorage};

    /// A backend that counts the work a scan actually does: every entry an
    /// iterator advances past, and every point read.
    ///
    /// This is the early-stop and memory proof. A source that materializes
    /// its range walks every entry in it before returning a row, so these
    /// counters are the difference between "read the relation" and "read what
    /// the query needs" -- observable without guessing at allocator numbers.
    #[derive(Debug, Clone, Default)]
    struct CountingStorage {
        inner: MemTableStorage,
        entries: Arc<AtomicUsize>,
        gets: Arc<AtomicUsize>,
    }

    struct CountingIterator {
        inner: Box<dyn StorageIterator>,
        entries: Arc<AtomicUsize>,
    }

    impl StorageIterator for CountingIterator {
        fn valid(&self) -> bool {
            self.inner.valid()
        }
        fn key(&self) -> &Key {
            self.inner.key()
        }
        fn value(&self) -> &[u8] {
            self.inner.value()
        }
        fn next(&mut self) -> Result<(), StorageError> {
            self.entries.fetch_add(1, Ordering::Relaxed);
            self.inner.next()
        }
        fn close(&mut self) {
            self.inner.close();
        }
    }

    impl TableStorage for CountingStorage {
        fn get(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.get(key)
        }
        fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), StorageError> {
            self.inner.set(key, value)
        }
        fn delete(&mut self, key: Key) -> Result<(), StorageError> {
            self.inner.delete(key)
        }
        fn iter(
            &mut self,
            start: Option<&Key>,
            upper_bound: Option<&Key>,
        ) -> Result<Box<dyn StorageIterator>, StorageError> {
            Ok(Box::new(CountingIterator {
                inner: self.inner.iter(start, upper_bound)?,
                entries: Arc::clone(&self.entries),
            }))
        }
        fn key_count(&self) -> usize {
            self.inner.key_count()
        }
        fn clear(&mut self) {
            self.inner.clear();
        }
        fn clone_box(&self) -> Box<dyn TableStorage> {
            Box::new(self.clone())
        }
    }

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn column(name: &str, id: i64) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: long(),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            generated: None,
        }
    }

    /// The rows counters and a catalog holding `t(a, b, c)` with `n` rows and
    /// an index on `b`. Row `i` is `(i, i, n - i)`, so `b` is unique and
    /// ascending while `c` descends -- the two orders the push-down rule has
    /// to tell apart.
    fn table_of(n: i64, indexed: bool) -> (Catalog, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let store = CountingStorage::default();
        let entries = Arc::clone(&store.entries);
        let gets = Arc::clone(&store.gets);
        let mut table = KvTable::with_storage(
            77,
            vec![column("a", 1), column("b", 2), column("c", 3)],
            Box::new(store),
        );
        if indexed {
            table
                .create_index_with_context(
                    crate::kv_table::KvIndex {
                        id: 1,
                        name: "ib".to_owned(),
                        unique: false,
                        column_offsets: vec![1],
                        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH],
                        visible: true,
                        global: false,
                    },
                    &crate::StmtContext::for_query(),
                )
                .unwrap();
        }
        for i in 1..=n {
            table
                .insert_row(
                    &[Datum::Int(i), Datum::Int(i), Datum::Int(n - i)],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("t", table);
        // Only the reads the query performs are interesting.
        entries.store(0, Ordering::Relaxed);
        gets.store(0, Ordering::Relaxed);
        (catalog, entries, gets)
    }

    fn first_column(rows: &[Vec<Datum>]) -> Vec<i64> {
        rows.iter()
            .map(|row| match row[0] {
                Datum::Int(v) => v,
                ref other => panic!("expected an integer, got {other:?}"),
            })
            .collect()
    }

    const ROWS: i64 = 5000;

    /// A full scan under a `LIMIT` stops at the cap: the rows past it are
    /// never advanced past, let alone decoded into memory.
    #[test]
    fn a_limit_stops_the_full_scan_instead_of_reading_the_relation() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows = run_select_on("SELECT a FROM t LIMIT 10", &catalog, &ctx).unwrap();
        assert_eq!(first_column(&rows), (1..=10).collect::<Vec<_>>());
        assert_eq!(
            entries.load(Ordering::Relaxed),
            10,
            "the scan read exactly the capped rows, not the {ROWS}-row relation"
        );

        // The same query without the LIMIT is the control: the counter really
        // does climb to the whole relation when nothing caps it.
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows = run_select_on("SELECT a FROM t", &catalog, &ctx).unwrap();
        assert_eq!(rows.len(), ROWS as usize);
        assert_eq!(entries.load(Ordering::Relaxed), ROWS as usize);
    }

    /// `LIMIT offset, count` reads `offset + count` rows -- Go's cop-side
    /// `Limit` for `limit 2, 3` is `offset:0, count:5`.
    #[test]
    fn an_offset_is_added_to_the_pushed_cap() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows = run_select_on("SELECT a FROM t LIMIT 2, 3", &catalog, &ctx).unwrap();
        assert_eq!(first_column(&rows), vec![3, 4, 5]);
        assert_eq!(entries.load(Ordering::Relaxed), 5);
    }

    /// A scan with no cap still hands out one chunk at a time: after a single
    /// `next` only a chunk's worth of rows has been read, so the source's
    /// live memory is a chunk and not the relation.
    #[test]
    fn a_scan_reads_one_chunk_per_pull_not_the_whole_range() {
        let store = CountingStorage::default();
        let entries = Arc::clone(&store.entries);
        let mut table = KvTable::with_storage(
            78,
            vec![column("a", 1)],
            Box::new(store) as Box<dyn TableStorage>,
        );
        for i in 1..=ROWS {
            table
                .insert_row(&[Datum::Int(i)], &tidb_expr::NoColumns)
                .unwrap();
        }
        entries.store(0, Ordering::Relaxed);

        let mut schema_column = tidb_expr::column::Column::new(1, long());
        schema_column.index = 0;
        let meta = ExecutorMeta::new(
            tidb_expr::schema::Schema::new(vec![schema_column]),
            0,
            1,
            1024,
        );
        let mut scan = crate::kv_table::TableScanExec::new_with_context(
            meta,
            table,
            crate::RowDecodeContext::for_test_query_utc(),
            crate::remote_scan::PushdownStatementContext::default(),
        );
        scan.open().unwrap();
        let mut req = scan.new_chunk();
        scan.next(&mut req).unwrap();
        assert_eq!(req.num_rows(), 1024, "a full chunk, and no more");
        assert_eq!(
            entries.load(Ordering::Relaxed),
            1024,
            "the cursor advanced one chunk: the other {} rows are still \
             undecoded, so the source's live rows are a chunk, not the relation",
            ROWS - 1024
        );
        scan.close().unwrap();
    }

    /// An `ORDER BY` the index range already produces lets the cap through:
    /// the scan stops at `count` rows instead of reading the range and
    /// sorting it.
    ///
    /// The projection is covering on purpose. A non-covering one adds a
    /// double read, and this tier's double read issues one round trip per
    /// row, which [`crate::access_cost`] now prices honestly -- on a table
    /// this small the chooser then prefers the scan, and the cap would be
    /// tested through the wrong path. See
    /// `a_double_read_costs_more_than_the_scan_it_replaces` below.
    #[test]
    fn an_order_by_the_index_satisfies_pushes_the_limit() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, gets) = table_of(ROWS, true);
        let rows = run_select_on(
            "SELECT b FROM t WHERE b > 0 ORDER BY b LIMIT 5",
            &catalog,
            &ctx,
        )
        .unwrap();
        assert_eq!(first_column(&rows), vec![1, 2, 3, 4, 5]);
        assert_eq!(
            entries.load(Ordering::Relaxed),
            5,
            "only five index entries were walked"
        );
        // KNOWN GAP, asserted rather than hidden: the source looks the row up
        // even though every column this statement reads is IN the index. Go's
        // covering path lowers to a `PhysicalIndexReader` that never touches
        // the table, and [`crate::access_cost`] costs it that way -- but
        // `IndexRangeSourceExec` always calls `get_row_by_handle`. So a
        // covering path costs zero round trips in the model and five here.
        // Closing it is the same work as batching the double read.
        assert_eq!(
            gets.load(Ordering::Relaxed),
            5,
            "five rows were still looked up, though the index covers them"
        );
    }

    /// A common-handle table is stored in primary-key order. Equality on the
    /// leading handle columns therefore leaves the remaining suffix as the
    /// scan order, so `ORDER BY` that suffix can stop at the LIMIT. This is
    /// the TPC-C Delivery shape: `(w_id, d_id)` is fixed and `o_id` is ordered.
    #[test]
    fn a_common_handle_equality_prefix_satisfies_order_and_pushes_the_limit() {
        let store = CountingStorage::default();
        let entries = Arc::clone(&store.entries);
        let mut table = KvTable::with_storage(
            79,
            vec![column("w_id", 1), column("d_id", 2), column("o_id", 3)],
            Box::new(store),
        );
        table.set_common_handle_offsets(vec![0, 1, 2]);
        table
            .create_index(
                crate::kv_table::KvIndex {
                    id: 1,
                    name: "PRIMARY".to_owned(),
                    unique: true,
                    column_offsets: vec![0, 1, 2],
                    prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 3],
                    visible: true,
                    global: false,
                },
                &tidb_expr::NoColumns,
            )
            .unwrap();
        for o_id in 1..=100 {
            table
                .insert_row(
                    &[Datum::Int(1), Datum::Int(1), Datum::Int(o_id)],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("new_order", table);
        entries.store(0, Ordering::Relaxed);

        let rows = run_select_on(
            "SELECT o_id FROM new_order \
             WHERE w_id = 1 AND d_id = 1 \
             ORDER BY o_id LIMIT 1",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(first_column(&rows), vec![1]);
        assert_eq!(
            entries.load(Ordering::Relaxed),
            1,
            "the ordered common-handle range must stop after its first row"
        );
    }

    /// Row-valued `IN` over a leading common-handle prefix opens one record
    /// range per tuple instead of scanning the relation. This is the two slow
    /// TPC-C Delivery statements over `(w_id, d_id, o_id, line_number)`.
    #[test]
    fn a_common_handle_row_in_reads_only_the_named_prefixes() {
        let store = CountingStorage::default();
        let entries = Arc::clone(&store.entries);
        let mut table = KvTable::with_storage(
            80,
            vec![
                column("w_id", 1),
                column("d_id", 2),
                column("o_id", 3),
                column("line_number", 4),
                column("amount", 5),
            ],
            Box::new(store),
        );
        table.set_common_handle_offsets(vec![0, 1, 2, 3]);
        table
            .create_index(
                crate::kv_table::KvIndex {
                    id: 1,
                    name: "PRIMARY".to_owned(),
                    unique: true,
                    column_offsets: vec![0, 1, 2, 3],
                    prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 4],
                    visible: true,
                    global: false,
                },
                &tidb_expr::NoColumns,
            )
            .unwrap();
        for d_id in 1..=2 {
            for o_id in 1..=100 {
                for line_number in 1..=2 {
                    table
                        .insert_row(
                            &[
                                Datum::Int(1),
                                Datum::Int(d_id),
                                Datum::Int(o_id),
                                Datum::Int(line_number),
                                Datum::Int(o_id * 10 + line_number),
                            ],
                            &tidb_expr::NoColumns,
                        )
                        .unwrap();
                }
            }
        }
        let mut catalog = Catalog::default();
        catalog.register_kv("order_line", table);
        entries.store(0, Ordering::Relaxed);

        let rows = run_select_on(
            "SELECT amount FROM order_line \
             WHERE (w_id, d_id, o_id) IN ((1, 1, 42), (1, 2, 43))",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(
            entries.load(Ordering::Relaxed),
            4,
            "only the four records under the two named prefixes may be read"
        );
    }

    /// THE DOUBLE READ IS UNBATCHED: N index rows produce N point gets.
    ///
    /// This is the executor half of the model/executor seam, pinned so it
    /// cannot drift silently. Go's `IndexLookUpExecutor` gathers handles into
    /// batches of `IndexLookupSize` in `fetchHandles` and hands each whole
    /// batch to `buildTableReader`, which turns it into ONE distsql request
    /// (`buildTableReaderFromHandles` -> `RequestBuilder.SetTableHandles`,
    /// `pkg/executor/builder.go`). That is what earns Go's cost model its
    /// `doubleReadTasks = rows/IndexLookupSize*32` term in
    /// `getPlanCostVer24PhysicalIndexLookUpReader`.
    ///
    /// `IndexRangeSourceExec` calls `KvTable::get_row_by_handle` per index
    /// entry instead, so against a cluster it issues one snapshot round trip
    /// per row. The assertion below is `n`, and a batched reader would make it
    /// `ceil(n / batch)` -- so landing the batched double read is exactly the
    /// change that flips this number, and this test is what makes that
    /// visible rather than silent.
    #[test]
    fn the_double_read_issues_one_point_get_per_index_row() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, _, gets) = table_of(ROWS, true);
        // An `IN` list keeps `eq_or_in_count` non-zero, which is what makes
        // Go's `prefer_range` rule hold the index path under pseudo
        // statistics -- so this measures the READER, not the chooser.
        let list: Vec<String> = (1..=50).map(|value| value.to_string()).collect();
        let query = format!("SELECT a FROM t WHERE b IN ({})", list.join(", "));
        let rows = run_select_on(&query, &catalog, &ctx).unwrap();
        assert_eq!(rows.len(), 50);
        assert_eq!(
            gets.load(Ordering::Relaxed),
            50,
            "one point get per index row -- a batched reader would issue 1"
        );
    }

    /// An `ORDER BY` on a column no index orders must NOT push: a sort has to
    /// see every row before it can name the first one. Go turns this case
    /// into a `TopN` over a scan that still reports all the rows (captured:
    /// `TopN_8` over `TableFullScan_16 | 20.00`).
    #[test]
    fn an_order_by_the_access_path_does_not_satisfy_reads_everything() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, true);
        let rows = run_select_on("SELECT a FROM t ORDER BY c LIMIT 5", &catalog, &ctx).unwrap();
        // c = ROWS - a descends, so the smallest c values are the last rows.
        assert_eq!(
            first_column(&rows),
            vec![ROWS, ROWS - 1, ROWS - 2, ROWS - 3, ROWS - 4]
        );
        assert_eq!(
            entries.load(Ordering::Relaxed),
            ROWS as usize,
            "the sort saw the whole relation, as it must"
        );
    }

    /// Nor may a DESC order by an indexed column push: the cursor is
    /// forward-only, so the index's ascending walk is the wrong end.
    #[test]
    fn a_descending_order_by_does_not_push() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, true);
        let rows = run_select_on(
            "SELECT a FROM t WHERE b > 0 ORDER BY b DESC LIMIT 3",
            &catalog,
            &ctx,
        )
        .unwrap();
        assert_eq!(first_column(&rows), vec![ROWS, ROWS - 1, ROWS - 2]);
        assert_eq!(entries.load(Ordering::Relaxed), ROWS as usize);
    }

    /// A conjunct the source could not take stays in a `Selection` above it,
    /// which can drop rows the cap already counted -- so nothing is pushed.
    #[test]
    fn a_residual_predicate_blocks_the_cap() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows =
            run_select_on("SELECT a FROM t WHERE c + 1 > 1 LIMIT 3", &catalog, &ctx).unwrap();
        assert_eq!(first_column(&rows), vec![1, 2, 3]);
        assert_eq!(
            entries.load(Ordering::Relaxed),
            ROWS as usize,
            "the arithmetic conjunct is residual, so the scan may not stop early"
        );
    }

    /// A predicate the scan DID take is applied below the cap, so the cap
    /// counts rows that passed it -- and the answer is the unfiltered one.
    #[test]
    fn a_pushed_predicate_is_applied_below_the_cap() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows = run_select_on("SELECT a FROM t WHERE a > 100 LIMIT 3", &catalog, &ctx).unwrap();
        assert_eq!(first_column(&rows), vec![101, 102, 103]);
        assert_eq!(
            entries.load(Ordering::Relaxed),
            103,
            "the scan read past the 100 rejected rows and stopped at the third kept one"
        );
    }

    /// `DISTINCT` can collapse rows above the source, so the cap would count
    /// rows the user never sees.
    #[test]
    fn distinct_blocks_the_cap() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows = run_select_on("SELECT DISTINCT a FROM t LIMIT 3", &catalog, &ctx).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(entries.load(Ordering::Relaxed), ROWS as usize);
    }

    /// `EXPLAIN ANALYZE`'s `actRows` for the scan reports the truncation: the
    /// counter the trace reads is the source's own, so it cannot drift from
    /// what the scan really did.
    #[test]
    fn explain_analyze_act_rows_reflect_the_truncation() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, _, _) = table_of(ROWS, false);
        let stmt = tidb_parser::parse("SELECT a FROM t WHERE a > 0 LIMIT 4").unwrap();
        let tidb_ast::Stmt::Query(query) = &stmt else {
            panic!("the test statement must parse as a query");
        };
        let tidb_ast::QueryStmt::Select(select) = &**query else {
            panic!("the test statement must parse as a SELECT");
        };
        let (_, rows) = crate::explain::explain_analyze_select_stmt(
            select,
            &catalog,
            "test",
            &ctx,
            ExplainFormat::Row,
        )
        .unwrap();
        // EXPLAIN cells are the wire's text bytes.
        let text = |cell: &Datum| match cell {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        };
        let scan = rows
            .iter()
            .find(|row| text(&row[0]).contains("TableFullScan"))
            .unwrap_or_else(|| {
                panic!(
                    "the plan has a full scan, got {:?}",
                    rows.iter().map(|r| text(&r[0])).collect::<Vec<_>>()
                )
            });
        assert_eq!(
            text(&scan[2]),
            "4",
            "the scan reports the four rows it read, not {ROWS}"
        );
    }

    /// A table whose INDEX order and HANDLE order disagree in both
    /// directions: `t(a, b, c)` under `ibc(b, c)`, with the handle being the
    /// allocated `_tidb_rowid` (= insertion order = `a`).
    ///
    /// ```text
    ///   handle  a  b   c        index order (b, c, handle)
    ///        1  1  2  20          (1,30,h2) (1,40,h4) (2,10,h3) (2,20,h1)
    ///        2  2  1  30        handle order
    ///        3  3  2  10          h1 h2 h3 h4
    ///        4  4  1  40
    /// ```
    ///
    /// So an index walk answers `a = 2, 4, 3, 1` and a handle-ordered read
    /// answers `a = 1, 2, 3, 4`. The second key part is what makes the
    /// KEEP-ORDER case observable too: `ORDER BY b` leaves two pairs tied, and
    /// the index breaks those ties by `c` where a handle-ordered read breaks
    /// them by handle.
    fn crossed_order_table() -> KvTable {
        let mut table = KvTable::with_storage(
            81,
            vec![column("a", 1), column("b", 2), column("c", 3)],
            Box::new(MemTableStorage::default()),
        );
        table
            .create_index_with_context(
                crate::kv_table::KvIndex {
                    id: 1,
                    name: "ibc".to_owned(),
                    unique: false,
                    column_offsets: vec![1, 2],
                    prefix_lengths: vec![
                        crate::ddl::index_prefix::UNSPECIFIED_LENGTH,
                        crate::ddl::index_prefix::UNSPECIFIED_LENGTH,
                    ],
                    visible: true,
                    global: false,
                },
                &crate::StmtContext::for_query(),
            )
            .unwrap();
        for (a, b, c) in [(1, 2, 20), (2, 1, 30), (3, 2, 10), (4, 1, 40)] {
            table
                .insert_row(
                    &[Datum::Int(a), Datum::Int(b), Datum::Int(c)],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        table
    }

    /// Reads [`crossed_order_table`] through an `IndexRangeSourceExec` over
    /// the whole of `ibc`, returning the `a` column in the order the source
    /// emitted it. `cap` is a pushed row cap, Go's `PushedLimit`.
    fn read_through_index(covering: bool, keep_order: bool, cap: Option<u64>) -> Vec<i64> {
        let table = crossed_order_table();
        let columns: Vec<tidb_expr::column::Column> = (0..3i64)
            .map(|offset| {
                let mut column = tidb_expr::column::Column::new(offset + 1, long());
                column.index = offset;
                column
            })
            .collect();
        let mut exec = IndexRangeSourceExec::new_with_context(
            ExecutorMeta::new(tidb_expr::schema::Schema::new(columns), 0, 32, 1024),
            table,
            1,
            vec![IndexRange::full()],
            crate::RowDecodeContext::for_test_query_utc(),
        );
        if covering {
            exec.answer_in_index_order();
        }
        if keep_order {
            use crate::table_access::TableAccess;
            assert!(exec.accept_keep_order());
        }
        if let Some(cap) = cap {
            use crate::table_access::TableAccess;
            assert!(exec.accept_scan_limit(cap));
        }
        exec.open().unwrap();
        let mut out = Vec::new();
        loop {
            let mut req = exec.new_chunk();
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for row in 0..req.num_rows() {
                out.push(req.get_row(row).get_int64(0));
            }
        }
        exec.close().unwrap();
        out
    }

    /// THE RULE: an UNORDERED double read answers in HANDLE order, not in
    /// index order.
    ///
    /// Go's `IndexLookUpExecutor` sorts each task's handle batch before the
    /// table read (`buildTableReaderFromHandles(..., canReorderHandles=true)`
    /// -> `slices.SortFunc(handles, i.Compare(j))`) and only puts the index
    /// order back `if w.keepOrder`. Captured in the corpus as
    /// `executor/index_lookup_pushdown_partition`'s `select ... from tp3`,
    /// whose recorded rows are handle-ascending per partition rather than
    /// index-ascending.
    #[test]
    fn an_unordered_double_read_answers_in_handle_order() {
        assert_eq!(read_through_index(false, false, None), vec![1, 2, 3, 4]);
    }

    /// A COVERING path is Go's `PhysicalIndexReader`: it never builds a handle
    /// batch, so there is nothing to reorder and the rows leave in INDEX
    /// order.
    ///
    /// This is not a detail -- it is the boundary that keeps the rule above
    /// from being a blanket sort. Captured (v8.5): `SELECT b FROM t WHERE b
    /// LIKE '%'` over `t(a,b,c)` with `kb(b)` plans an `IndexFullScan` and
    /// answers `Yz, xy, z` -- index order, against handle order `xy, Yz, z`.
    #[test]
    fn a_covering_index_read_answers_in_index_order() {
        assert_eq!(read_through_index(true, false, None), vec![2, 4, 3, 1]);
    }

    /// `keep order:true` is the other half of the boundary: the plan asked for
    /// the index's own order, so Go re-sorts the batch back into it
    /// (`sort.Sort(task)` under `if w.keepOrder`) and this tier skips the
    /// reorder instead, which is the same answer one pass cheaper.
    #[test]
    fn a_kept_order_double_read_answers_in_index_order() {
        assert_eq!(read_through_index(false, true, None), vec![2, 4, 3, 1]);
    }

    /// THE ROW-SET HALF of the rule, and the only one whose failure loses
    /// rows rather than reordering them: a pushed cap truncates the INDEX
    /// stream, and the sort happens to what is left.
    ///
    /// Go's `extractTaskHandles` reads the index in index order and stops at
    /// `w.PushedLimit.Offset + w.PushedLimit.Count` (`distsql.go`: `leftCnt :=
    /// w.PushedLimit.Offset + w.PushedLimit.Count - w.scannedKeys`, and the
    /// per-row `if w.scannedKeys > (...) { return handles, nil, nil }`). The
    /// batch it returns is only THEN handed to `buildTableReaderFromHandles`,
    /// which sorts it. So the rows a capped double read keeps are the
    /// index-order prefix, and they are ANSWERED in handle order.
    ///
    /// Over [`crossed_order_table`] the two orders disagree on WHICH rows a
    /// cap of THREE keeps, not merely on their sequence. The index walk is
    /// `a = 2, 4, 3, 1`, so the prefix is the rows `{2, 4, 3}`, answered
    /// handle-ascending as `2, 3, 4`. Sorting the whole batch FIRST and
    /// truncating after would keep handles `1, 2, 3` -- `a = 1, 2, 3`, which
    /// DROPS row `a = 4` and invents row `a = 1`. That is why this assertion
    /// is the ordered vector and not a length: a length passes both ways.
    #[test]
    fn a_pushed_cap_keeps_the_index_prefix_and_answers_it_in_handle_order() {
        assert_eq!(read_through_index(false, false, Some(3)), vec![2, 3, 4]);
    }

    /// The same boundary on the ordered side, and the reason it is a SECOND
    /// test rather than the same one: `keep order:true` skips the reorder, so
    /// the same three rows leave in the index's own sequence, `2, 4, 3`. The
    /// two expectations are different vectors over the same row set, so
    /// neither can pass by agreeing with the other while both are wrong.
    #[test]
    fn a_capped_kept_order_double_read_answers_the_index_prefix_in_index_order() {
        assert_eq!(read_through_index(false, true, Some(3)), vec![2, 4, 3]);
    }

    /// The driver's own half of the rule: `keep order:true` has to be
    /// OFFERED, and an `ORDER BY` that TIES is what makes the offer
    /// observable.
    ///
    /// `t(a, b, c)` over `ibc(b, c)`, 200 rows in handle order, `b` DESCENDING
    /// in pairs and `c` reversed inside each pair:
    ///
    /// ```text
    ///   handle  b   c        index order inside b = 50: h4 h3 h2 h1
    ///        1  50   4        handle order inside b = 50: h1 h2 h3 h4
    ///        2  50   3
    ///        3  50   2
    ///        4  50   1
    ///        5  49   4
    /// ```
    ///
    /// `WHERE b = 50 ORDER BY b` is a single POINT range, which is what keeps
    /// the non-covering index path ahead of the scan (Go's `prefer_range`
    /// rule wants a non-zero `eq_or_in_count`), and every row it returns TIES
    /// on the `ORDER BY` key. The `Sort` above is this tier's STABLE one, so
    /// the four rows leave in exactly the order the source produced them:
    /// Go's `keep order:true` answer is the index's (`c` ascending), where an
    /// unordered double read would answer handle-ascending.
    #[test]
    fn the_driver_offers_keep_order_to_an_order_by_the_index_produces() {
        let ctx = crate::StmtContext::for_query();
        let mut catalog = Catalog::default();
        catalog.register_kv("t", paired_order_table(200));
        let rows =
            run_select_on("SELECT a FROM t WHERE b = 50 ORDER BY b", &catalog, &ctx).unwrap();
        assert_eq!(first_column(&rows), vec![4, 3, 2, 1]);
    }

    /// [`the_driver_offers_keep_order_to_an_order_by_the_index_produces`]'s
    /// table: `n` rows whose handle order, `b` order and `c` order all
    /// disagree. See that test for the layout.
    fn paired_order_table(n: i64) -> KvTable {
        let mut table = KvTable::with_storage(
            82,
            vec![column("a", 1), column("b", 2), column("c", 3)],
            Box::new(MemTableStorage::default()),
        );
        table
            .create_index_with_context(
                crate::kv_table::KvIndex {
                    id: 1,
                    name: "ibc".to_owned(),
                    unique: false,
                    column_offsets: vec![1, 2],
                    prefix_lengths: vec![
                        crate::ddl::index_prefix::UNSPECIFIED_LENGTH,
                        crate::ddl::index_prefix::UNSPECIFIED_LENGTH,
                    ],
                    visible: true,
                    global: false,
                },
                &crate::StmtContext::for_query(),
            )
            .unwrap();
        for handle in 1..=n {
            let b = n / 4 - (handle - 1) / 4;
            let c = 4 - (handle - 1) % 4;
            table
                .insert_row(
                    &[Datum::Int(handle), Datum::Int(b), Datum::Int(c)],
                    &tidb_expr::NoColumns,
                )
                .unwrap();
        }
        table
    }
}
