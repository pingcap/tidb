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

use std::cell::{Cell, RefCell};
use std::collections::{btree_set, BTreeMap, BTreeSet};
use std::rc::Rc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, Decimal, FieldType, SessionTimeZone};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::truthy_of;

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::kv_table::{
    IndexRange, IndexRangeCursor, KvTable, RemoteRowCursor, RowCursor, TableHandle,
};
use crate::predicate_pushdown::{
    ScanColumnComparison, ScanComparison, ScanComparisonOp, ScanPredicate,
};
use crate::remote_scan::{
    PushdownAggregateKind, PushdownPartialAggregate, PushdownRowStream, PushdownStatementContext,
};

/// Probe values shared by a composite index-join inner subtree and its
/// target table leaf. The outer join publishes one batch at a time; the leaf
/// refreshes its lookup source lazily when the generation changes.
#[derive(Debug, Default)]
pub(crate) struct SharedIndexJoinProbes {
    probes: Vec<Vec<Datum>>,
    generation: u64,
}

impl SharedIndexJoinProbes {
    pub(crate) fn publish(&mut self, probes: Vec<Vec<Datum>>) {
        self.probes = probes;
        self.generation = self.generation.wrapping_add(1);
    }
}

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
pub(crate) fn select_is_bare_point_read(select: &tidb_ast::SelectStmt) -> bool {
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
    /// One physical table id per handle for a partitioned batch point get.
    physical_ids: Option<Vec<i64>>,
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
            physical_ids: None,
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
            physical_ids: None,
            cursor: 0,
            produced: Rc::new(Cell::new(0)),
            decode_context,
            output_offsets: Some(output_offsets),
        }
    }

    /// Builds Go's partitioned `BatchPointGetExec`, retaining the current
    /// source-level projection while pairing every handle with its physical
    /// table id.
    #[must_use]
    pub(crate) fn new_partitioned_projected_with_context(
        meta: ExecutorMeta,
        table: KvTable,
        handles: Vec<TableHandle>,
        physical_ids: Vec<i64>,
        output_offsets: Option<Vec<usize>>,
        decode_context: crate::kv_table::RowDecodeContext,
    ) -> Self {
        assert_eq!(
            handles.len(),
            physical_ids.len(),
            "every batch-point handle needs one physical partition"
        );
        Self {
            meta,
            table,
            handles,
            physical_ids: Some(physical_ids),
            cursor: 0,
            produced: Rc::new(Cell::new(0)),
            decode_context,
            output_offsets,
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
            let row = match self
                .physical_ids
                .as_ref()
                .and_then(|physical_ids| physical_ids.get(self.cursor - 1))
            {
                Some(physical_id) => self.table.get_row_by_handle_in_physical_id_with_context(
                    handle,
                    *physical_id,
                    &self.decode_context,
                ),
                None => self
                    .table
                    .get_row_by_handle_with_context(handle, &self.decode_context),
            }
            .map_err(|error| {
                ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
            })?;
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
    /// Physical visible-column offsets emitted by this source. The mapping is
    /// composed when logical column pruning reaches a join leaf.
    keep: Vec<usize>,
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
    /// The same conjunct descriptions, lowered into an index-side
    /// coprocessor Selection before a handle TopN or table lookup.
    pushed: Vec<crate::predicate_pushdown::ScanPredicate>,
    /// Whether the driver proved the accepted filter is covered by this
    /// index. This is an execution hint; the ordinary row filter remains as
    /// a semantic check after the table lookup.
    index_filter: bool,
    /// A bounded TopN that can run on the index stream before table lookup.
    top_n: Option<crate::remote_scan::PushdownTopN>,
    /// A pushed row cap (`offset + count`); see [`Executor::accept_scan_limit`].
    limit: Option<u64>,
    /// Handles skipped before a limit embedded in an ordered IndexLookUp.
    lookup_offset: u64,
    skipped_handles: u64,
    /// Go's `canReorderHandles` (`builder.go`): whether this read may answer
    /// in handle order. FALSE for the two reads whose answer is the index
    /// walk itself -- a COVERING path, which is Go's `PhysicalIndexReader`
    /// and builds no handle batch at all, and a `keep order:true` lookup,
    /// which sorts the batch back into index order after reading it. See the
    /// type doc.
    can_reorder_handles: bool,
    /// Go's `PhysicalIndexScan.Desc`: the matching index range is walked from
    /// its exclusive high key toward its low key.
    descending: bool,
    /// Whether the selected columns are served by Go's single-read
    /// `PhysicalIndexReader`, which cannot accept an IndexLookUp-only limit.
    covering: bool,
    /// The current handle batch -- Go's `lookupTableTask.handles`, already
    /// sorted unless [`Self::keep_order`].
    batch: Vec<TableHandle>,
    /// How much of `batch` has been read.
    batch_at: usize,
    /// Go's `indexWorker.batchSize`: how many handles the next batch collects,
    /// doubling per batch up to [`MAX_HANDLE_BATCH`].
    batch_size: usize,
    /// A keep-order parent's output window, used only to seed the first
    /// lookup task. It neither caps the scan nor changes plan shape.
    initial_batch_size: usize,
    /// Rows decoded for the current handle batch, retained across output
    /// chunks when a batch is larger than the requested chunk size.
    lookup_rows: Vec<Option<Vec<Datum>>>,
    lookup_row_at: usize,
    /// Whether the current lookup batch was completely filtered by TiKV.
    /// When true, re-evaluating the same probe residual locally only adds
    /// expression work and cannot change the result.
    lookup_filter_complete: bool,
    /// A remote coprocessor index stream, when the TiKV backend can serve the
    /// selected index/filter/TopN shape.
    remote_index: Option<crate::kv_table::RemoteIndexHandleCursor>,
    /// The statement-class flags and session zone the row is decoded under;
    /// the zone also encodes the index probe. See [`HandleSourceExec`].
    decode_context: crate::kv_table::RowDecodeContext,
    /// Statement flags and warning sink carried into a remote DAG request.
    statement: PushdownStatementContext,
    /// Planner estimate used to avoid partial aggregation for point-like work.
    estimated_rows: Option<f64>,
    /// Partial aggregation accepted from the root aggregation executor.
    partial_aggregate: Option<PushdownPartialAggregate>,
    /// Input schema and statement context retained after `meta` becomes the
    /// partial-result schema, for expression-valued local fallback inputs.
    partial_input_types: Option<Vec<FieldType>>,
    partial_context: Option<crate::StmtContext>,
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
    /// Names the TABLE columns this source's schema stands for.
    ///
    /// Go's `DataSource` carries `Columns []*model.ColumnInfo` beside its
    /// schema, so a reader built after `rule_column_pruning` narrowed that
    /// DataSource still knows which STORED column each output slot is --
    /// `PhysicalIndexLookUpReader`'s table side reads exactly those. This
    /// tier hands the reader the narrowed schema ALONE, and the `keep` it
    /// decodes with defaults to `0..schema.len()`: the first n table columns,
    /// which is the right answer only when nothing was pruned.
    ///
    /// Without this, `select b from t partition(p0) use index(idx1) where
    /// b <= 2` answered with `a`'s value -- one output slot, decoded from
    /// table column 0. The partition clause is what made it visible: it is
    /// the shape whose leaf demand prunes the scope BEFORE the access path
    /// replaces the source, so the replacement was the first reader ever
    /// built over a narrowed schema.
    pub(crate) fn read_table_columns(&mut self, offsets: Vec<usize>) {
        debug_assert_eq!(offsets.len(), self.meta.schema().columns.len());
        self.keep = offsets;
    }

    pub fn new_with_statement(
        meta: ExecutorMeta,
        table: KvTable,
        index_id: i64,
        ranges: Vec<IndexRange>,
        decode_context: crate::kv_table::RowDecodeContext,
        statement: PushdownStatementContext,
    ) -> Self {
        // The IDENTITY default is only right when the schema is the table's
        // leading columns; a caller whose schema was narrowed by column
        // pruning must say so with [`Self::read_table_columns`].
        let keep = (0..meta.schema().columns.len()).collect();
        IndexRangeSourceExec {
            meta,
            table,
            keep,
            index_id,
            ranges,
            next_range: 0,
            cursor: None,
            produced: Rc::new(Cell::new(0)),
            scanned: Rc::new(Cell::new(0)),
            filter: None,
            pushed: Vec::new(),
            index_filter: false,
            top_n: None,
            limit: None,
            lookup_offset: 0,
            skipped_handles: 0,
            can_reorder_handles: true,
            descending: false,
            covering: false,
            batch: Vec::new(),
            batch_at: 0,
            batch_size: INIT_HANDLE_BATCH,
            initial_batch_size: INIT_HANDLE_BATCH,
            lookup_rows: Vec::new(),
            lookup_row_at: 0,
            lookup_filter_complete: false,
            remote_index: None,
            decode_context,
            statement,
            estimated_rows: None,
            partial_aggregate: None,
            partial_input_types: None,
            partial_context: None,
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

    /// Declares that this source represents a covering index reader rather
    /// than a table double read.
    pub(crate) fn mark_covering(&mut self) {
        self.covering = true;
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
                let Some(handle) = self.next_window_handle()? else {
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

    /// Fetches and decodes one whole index-lookup handle batch. This is the
    /// executor half of Go's `lookupTableTask`: the cluster storage receives
    /// one region-grouped batch request instead of one point read per handle.
    fn next_lookup_row(&mut self) -> Result<Option<Vec<Datum>>, ExecError> {
        loop {
            if self.lookup_row_at == self.lookup_rows.len() {
                self.lookup_rows.clear();
                self.lookup_row_at = 0;
                self.lookup_filter_complete = false;
                let target = self.limit.map_or(self.batch_size, |limit| {
                    self.batch_size.min(
                        usize::try_from(limit.saturating_sub(self.produced.get())).unwrap_or(0),
                    )
                });
                let mut handles = Vec::with_capacity(target);
                while handles.len() < target {
                    let Some(handle) = self.next_lookup_handle()? else {
                        break;
                    };
                    handles.push(handle);
                }
                if handles.is_empty() {
                    return Ok(None);
                }
                // A clean table can answer the whole lookup batch in one
                // coprocessor request even when no residual predicate was
                // accepted on the index. The old gate only enabled this for
                // non-empty `pushed`, leaving ordinary wide index lookups to
                // decode every row through the local BatchGet path.
                let remote = if self.partial_aggregate.is_none() {
                    self.table
                        .pushdown_rows_by_handles_filtered(
                            &handles,
                            &self.keep,
                            &self.pushed,
                            self.decode_context.zone(),
                            &self.statement,
                        )
                        .map_err(|error| {
                            ExecError::unsupported(format!("remote table lookup failed: {error:?}"))
                        })?
                } else {
                    None
                };
                self.lookup_rows = if let Some((rows, predicates_applied)) = remote {
                    self.lookup_filter_complete = predicates_applied;
                    rows.into_iter().map(|(_, row)| Some(row)).collect()
                } else {
                    self.table
                        .get_rows_by_handles_projected_with_context(
                            &handles,
                            Some(&self.keep),
                            &self.decode_context,
                        )
                        .map_err(|error| {
                            ExecError::unsupported(format!(
                                "table bytes failed to decode: {error:?}"
                            ))
                        })?
                };
            }
            let row = std::mem::take(&mut self.lookup_rows[self.lookup_row_at]);
            self.lookup_row_at += 1;
            if let Some(row) = row {
                return Ok(Some(row));
            }
        }
    }

    fn next_window_handle(&mut self) -> Result<Option<TableHandle>, ExecError> {
        while self.skipped_handles < self.lookup_offset {
            if self.next_handle()?.is_none() {
                return Ok(None);
            }
            self.skipped_handles += 1;
        }
        self.next_handle()
    }

    /// The next handle in index order across all ranges, opening the next
    /// range's cursor when the current one runs out.
    fn next_handle(&mut self) -> Result<Option<TableHandle>, ExecError> {
        if let Some(remote) = self.remote_index.as_mut() {
            return remote
                .next_handle()
                .map_err(|_| ExecError::unsupported("remote index row failed to decode"));
        }
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
            let range = if self.descending {
                let Some(next_range) = self.next_range.checked_sub(1) else {
                    return Ok(None);
                };
                self.next_range = next_range;
                self.ranges[next_range].clone()
            } else {
                let Some(range) = self.ranges.get(self.next_range).cloned() else {
                    return Ok(None);
                };
                self.next_range += 1;
                range
            };
            self.cursor = Some(
                self.table
                    .index_range_cursor_with_direction(
                        self.index_id,
                        &range,
                        self.decode_context.zone(),
                        self.descending,
                        // Go's `byItems`, the half of `needMergeSort` this
                        // tier was missing: non-empty exactly when the answer
                        // has to come back in index order, which is what
                        // `can_reorder_handles` being FALSE already records
                        // here -- a covering read, or a `keep order:true`
                        // lookup. An unordered read may answer partition by
                        // partition, and Go's index worker does.
                        !self.can_reorder_handles,
                    )
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
            let Some(row) = self.next_lookup_row()? else {
                return Ok(None);
            };
            self.scanned.set(self.scanned.get() + 1);
            if let Some(filter) = self.filter.as_mut() {
                if !filter.admits(&row)? {
                    continue;
                }
            }
            self.produced.set(self.produced.get() + 1);
            return Ok(Some(row));
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
                    if input_offset
                        .is_none_or(|offset| !matches!(row.get(offset), None | Some(Datum::Null)))
                    {
                        count += 1;
                    }
                }
                Ok(vec![vec![Datum::Int(count)]])
            }
            PushdownPartialAggregate::Global { functions } => {
                enum PartialValue {
                    Count(i64),
                    SumDecimal(Option<Decimal>),
                    SumReal(Option<f64>),
                    Extreme {
                        value: Option<Datum>,
                        is_max: bool,
                        collation: tidb_datatype::Collation,
                    },
                }

                let input_types = self.partial_input_types.clone().ok_or_else(|| {
                    ExecError::unsupported("index partial aggregation lost its input schema")
                })?;
                let context = self.partial_context.clone().ok_or_else(|| {
                    ExecError::unsupported("index partial aggregation lost its statement context")
                })?;
                let mut values = functions
                    .iter()
                    .map(|function| match function.kind {
                        PushdownAggregateKind::Count => PartialValue::Count(0),
                        PushdownAggregateKind::Sum
                            if function.output_type.eval_type()
                                == tidb_datatype::EvalType::Real =>
                        {
                            PartialValue::SumReal(None)
                        }
                        PushdownAggregateKind::Sum => PartialValue::SumDecimal(None),
                        PushdownAggregateKind::Min => PartialValue::Extreme {
                            value: None,
                            is_max: false,
                            collation: crate::remote_scan::extreme_collation(
                                function.input.as_ref(),
                            ),
                        },
                        PushdownAggregateKind::Max => PartialValue::Extreme {
                            value: None,
                            is_max: true,
                            collation: crate::remote_scan::extreme_collation(
                                function.input.as_ref(),
                            ),
                        },
                    })
                    .collect::<Vec<_>>();

                while let Some(row) = self.next_partial_input_row()? {
                    for (function, value) in functions.iter().zip(values.iter_mut()) {
                        let input = function
                            .input
                            .as_ref()
                            .map(|expression| {
                                crate::generated_column::eval_over_row(
                                    expression,
                                    &input_types,
                                    &row,
                                    &context,
                                )
                                .map_err(ExecError::Eval)
                            })
                            .transpose()?;
                        match (value, input) {
                            (PartialValue::Count(count), None) => *count += 1,
                            (PartialValue::Count(_), Some(Datum::Null)) => {}
                            (PartialValue::Count(count), Some(_)) => *count += 1,
                            (PartialValue::SumDecimal(_), None)
                            | (PartialValue::SumReal(_), None)
                            | (PartialValue::Extreme { .. }, None) => {
                                return Err(ExecError::unsupported(
                                    "only COUNT may omit an index partial aggregate input",
                                ));
                            }
                            (PartialValue::SumDecimal(_), Some(Datum::Null))
                            | (PartialValue::SumReal(_), Some(Datum::Null))
                            | (PartialValue::Extreme { .. }, Some(Datum::Null)) => {}
                            (PartialValue::SumDecimal(sum), Some(input)) => {
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
                            (PartialValue::SumReal(sum), Some(input)) => {
                                let addend = input.to_f64().map_err(|_| {
                                    ExecError::unsupported(
                                        "index partial SUM requires numeric input",
                                    )
                                })?;
                                *sum = Some(sum.unwrap_or(0.0) + addend.value);
                            }
                            (PartialValue::Extreme {
                                    value,
                                    is_max,
                                    collation,
                                }, Some(candidate)) => {
                                let replace = value.as_ref().is_none_or(|current| {
                                    crate::remote_scan::extreme_replaces(
                                        &candidate,
                                        current,
                                        *is_max,
                                        *collation,
                                    )
                                });
                                if replace {
                                    *value = Some(candidate);
                                }
                            }
                        }
                    }
                }

                Ok(vec![values
                    .into_iter()
                    .map(|value| match value {
                        PartialValue::Count(count) => Datum::Int(count),
                        PartialValue::SumDecimal(sum) => sum.map_or(Datum::Null, Datum::Decimal),
                        PartialValue::SumReal(sum) => sum.map_or(Datum::Null, Datum::Real),
                        PartialValue::Extreme { value, .. } => value.unwrap_or(Datum::Null),
                    })
                    .collect::<Vec<_>>()])
            }
            PushdownPartialAggregate::Grouped {
                group_offsets,
                group_types,
                functions,
                streamed,
            } => {
                if group_offsets.len() != group_types.len() || group_offsets.is_empty() {
                    return Err(ExecError::unsupported(
                        "index partial grouped aggregation requires typed group keys",
                    ));
                }

                let input_types = self.partial_input_types.clone().ok_or_else(|| {
                    ExecError::unsupported("index partial aggregation lost its input schema")
                })?;
                let context = self.partial_context.clone().ok_or_else(|| {
                    ExecError::unsupported("index partial aggregation lost its statement context")
                })?;

                enum PartialValue {
                    Count(i64),
                    SumDecimal(Option<Decimal>),
                    SumReal(Option<f64>),
                    Extreme {
                        value: Option<Datum>,
                        is_max: bool,
                        collation: tidb_datatype::Collation,
                    },
                }
                let new_values = || {
                    functions
                        .iter()
                        .map(|function| match function.kind {
                            PushdownAggregateKind::Count => PartialValue::Count(0),
                            PushdownAggregateKind::Sum
                                if function.output_type.eval_type()
                                    == tidb_datatype::EvalType::Real =>
                            {
                                PartialValue::SumReal(None)
                            }
                            PushdownAggregateKind::Sum => PartialValue::SumDecimal(None),
                            PushdownAggregateKind::Min => PartialValue::Extreme {
                                value: None,
                                is_max: false,
                                collation: crate::remote_scan::extreme_collation(
                                    function.input.as_ref(),
                                ),
                            },
                            PushdownAggregateKind::Max => PartialValue::Extreme {
                                value: None,
                                is_max: true,
                                collation: crate::remote_scan::extreme_collation(
                                    function.input.as_ref(),
                                ),
                            },
                        })
                        .collect::<Vec<_>>()
                };
                let finish = |groups: Vec<Datum>, values: Vec<PartialValue>| {
                    values
                        .into_iter()
                        .map(|value| match value {
                            PartialValue::Count(count) => Datum::Int(count),
                            PartialValue::SumDecimal(sum) => {
                                sum.map_or(Datum::Null, Datum::Decimal)
                            }
                            PartialValue::SumReal(sum) => sum.map_or(Datum::Null, Datum::Real),
                            PartialValue::Extreme { value, .. } => value.unwrap_or(Datum::Null),
                        })
                        .chain(groups)
                        .collect::<Vec<_>>()
                };
                let group = |row: &[Datum]| -> Result<(Vec<u8>, Vec<Datum>), ExecError> {
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
                    Ok((key, groups))
                };
                let update =
                    |values: &mut [PartialValue], row: &[Datum]| -> Result<(), ExecError> {
                        for (function, value) in functions.iter().zip(values.iter_mut()) {
                            let input = function
                                .input
                                .as_ref()
                                .map(|expression| {
                                    crate::generated_column::eval_over_row(
                                        expression,
                                        &input_types,
                                        row,
                                        &context,
                                    )
                                    .map_err(ExecError::Eval)
                                })
                                .transpose()?;
                            match (value, input) {
                                (PartialValue::Count(count), None) => *count += 1,
                                (PartialValue::Count(_), Some(Datum::Null)) => {}
                                (PartialValue::Count(count), Some(_)) => *count += 1,
                                (PartialValue::SumDecimal(_), None)
                                | (PartialValue::SumReal(_), None)
                                | (PartialValue::Extreme { .. }, None) => {
                                    return Err(ExecError::unsupported(
                                        "only COUNT may omit an index partial aggregate input",
                                    ));
                                }
                                (PartialValue::SumDecimal(_), Some(Datum::Null))
                                | (PartialValue::SumReal(_), Some(Datum::Null))
                                | (PartialValue::Extreme { .. }, Some(Datum::Null)) => {}
                                (PartialValue::SumDecimal(sum), Some(input)) => {
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
                                (PartialValue::SumReal(sum), Some(input)) => {
                                    let addend = input.to_f64().map_err(|_| {
                                        ExecError::unsupported(
                                            "index partial SUM requires numeric input",
                                        )
                                    })?;
                                    *sum = Some(sum.unwrap_or(0.0) + addend.value);
                                }
                                (PartialValue::Extreme {
                                        value,
                                        is_max,
                                        collation,
                                    }, Some(candidate)) => {
                                    let replace = value.as_ref().is_none_or(|current| {
                                        crate::remote_scan::extreme_replaces(
                                            &candidate,
                                            current,
                                            *is_max,
                                            *collation,
                                        )
                                    });
                                    if replace {
                                        *value = Some(candidate);
                                    }
                                }
                            }
                        }
                        Ok(())
                    };

                if !streamed {
                    let mut grouped = BTreeMap::<Vec<u8>, (Vec<Datum>, Vec<PartialValue>)>::new();
                    while let Some(row) = self.next_partial_input_row()? {
                        let (key, groups) = group(&row)?;
                        let (_, values) =
                            grouped.entry(key).or_insert_with(|| (groups, new_values()));
                        update(values, &row)?;
                    }
                    return Ok(grouped
                        .into_values()
                        .map(|(groups, values)| finish(groups, values))
                        .collect());
                }

                let mut rows = Vec::new();
                let mut current: Option<(Vec<u8>, Vec<Datum>, Vec<PartialValue>)> = None;
                while let Some(row) = self.next_partial_input_row()? {
                    let (key, groups) = group(&row)?;
                    if current
                        .as_ref()
                        .is_some_and(|(current_key, _, _)| current_key != &key)
                    {
                        let (_, previous_groups, previous_values) =
                            current.take().expect("current group exists");
                        rows.push(finish(previous_groups, previous_values));
                    }
                    let (_, _, values) = current.get_or_insert_with(|| (key, groups, new_values()));
                    update(values, &row)?;
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
        self.next_range = if self.descending {
            self.ranges.len()
        } else {
            0
        };
        self.cursor = None;
        self.produced.set(0);
        self.scanned.set(0);
        self.batch.clear();
        self.batch_at = 0;
        self.batch_size = self.initial_batch_size;
        self.lookup_rows.clear();
        self.lookup_row_at = 0;
        self.lookup_filter_complete = false;
        self.skipped_handles = 0;
        self.partial_remote = None;
        self.partial_rows = None;
        self.partial_done = false;
        self.remote_index = if self.covering || (!self.index_filter && self.top_n.is_none()) {
            None
        } else {
            self.table
                .pushdown_index_handle_cursor(
                    self.index_id,
                    &self.ranges,
                    &self.keep,
                    &self.pushed,
                    self.top_n.as_ref(),
                    self.decode_context.zone(),
                    &self.statement,
                    self.descending,
                )
                .map_err(|_| ExecError::unsupported("remote index scan failed to open"))?
        };
        if let Some(aggregate) = self.partial_aggregate.as_ref() {
            self.partial_remote = self
                .table
                .pushdown_index_partial_aggregate_cursor(
                    self.index_id,
                    &self.ranges,
                    &self.keep,
                    aggregate,
                    self.decode_context.zone(),
                    &self.statement,
                )
                .map_err(|error| {
                    ExecError::unsupported(format!("index aggregate request failed: {error:?}"))
                })?;
        }
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let cap = req.required_rows().clamp(1, self.meta.max_chunk_size());
        if self.produced.get() == 0
            && self.scanned.get() == 0
            && self.lookup_rows.is_empty()
            && self.batch.is_empty()
        {
            // Go seeds `indexWorker.batchSize` from the first output chunk's
            // RequiredRows. A LIMIT/TopN parent therefore starts a double read
            // at its requested window instead of decoding a full 1,024-row
            // chunk that the parent will immediately discard.
            self.batch_size = self.batch_size.min(cap).min(MAX_HANDLE_BATCH);
        }
        req.reset();
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
            let Some(row) = self.next_lookup_row()? else {
                return Ok(());
            };
            // An index entry whose row is gone is not a row: the same
            // `if let Some(row)` the materializing path had.
            self.scanned.set(self.scanned.get() + 1);
            if !self.lookup_filter_complete {
                if let Some(filter) = self.filter.as_mut() {
                    if !filter.admits(&row)? {
                        continue;
                    }
                }
            }
            for (c, value) in row.iter().enumerate() {
                req.append_datum(c, value);
            }
            self.produced.set(self.produced.get() + 1);
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.cursor = None;
        self.partial_remote = None;
        self.partial_rows = None;
        self.partial_done = false;
        self.remote_index = None;
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

    fn accept_partial_aggregate(
        &mut self,
        aggregate: &PushdownPartialAggregate,
        ctx: &crate::StmtContext,
    ) -> bool {
        let supported = matches!(
            aggregate,
            PushdownPartialAggregate::Count { .. } | PushdownPartialAggregate::Global { .. }
        ) || matches!(
            aggregate,
            PushdownPartialAggregate::Grouped {
                streamed: false,
                ..
            }
        ) || (matches!(
            aggregate,
            PushdownPartialAggregate::Grouped { streamed: true, .. }
        ) && !self.can_reorder_handles);
        if self.estimated_rows.is_none_or(|rows| rows <= 1.0)
            || aggregate
                .input_offsets()
                .into_iter()
                .any(|offset| offset >= self.keep.len())
            || !supported
            || self.partial_aggregate.is_some()
            || self.limit.is_some()
        {
            return false;
        }
        let input_types = self.meta.ret_field_types().to_vec();
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
        self.partial_input_types = Some(input_types);
        self.partial_context = Some(ctx.clone());
        true
    }

    fn accept_column_prune(&mut self, keep: &[usize]) -> bool {
        if keep.is_empty()
            || keep.iter().any(|offset| *offset >= self.keep.len())
            || self.partial_aggregate.is_some()
        {
            return false;
        }
        let columns = keep
            .iter()
            .enumerate()
            .map(|(index, offset)| {
                let mut column = self.meta.schema().columns[*offset].clone();
                column.index = index as i64;
                column.id = index as i64 + 1;
                column
            })
            .collect();
        let meta = ExecutorMeta::new(
            Schema::new(columns),
            self.meta.id(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        );
        let filter = match &self.filter {
            Some(filter) => {
                let Some(filter) = filter.remapped_columns(keep, meta.new_chunk()) else {
                    return false;
                };
                Some(filter)
            }
            None => None,
        };
        self.meta = meta;
        self.filter = filter;
        self.keep = keep.iter().map(|offset| self.keep[*offset]).collect();
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

    fn accept_embedded_lookup_limit(&mut self, offset: u64, count: u64) -> bool {
        if self.covering
            || self.table.has_dirty_content()
            // With a zero SQL offset, a pushed index-side Selection can be
            // evaluated by the lookup source while it continues collecting
            // handles until `count` qualifying rows are produced. A non-zero
            // offset must remain above that filter, so keep the conservative
            // refusal there.
            || (offset > 0 && self.filter.is_some())
            || self.partial_aggregate.is_some()
            || self.limit.is_some()
        {
            return false;
        }
        self.lookup_offset = offset;
        self.limit = Some(count);
        true
    }

    fn accept_lookup_batch_size(&mut self, size: u64) -> bool {
        if self.covering || size == 0 {
            return false;
        }
        self.initial_batch_size = usize::try_from(size)
            .unwrap_or(MAX_HANDLE_BATCH)
            .clamp(1, MAX_HANDLE_BATCH);
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
        self.pushed = filter.predicates().to_vec();
        true
    }

    fn accept_index_filter(&mut self) -> bool {
        if self.covering || self.filter.is_none() || self.table.has_dirty_content() {
            return false;
        }
        self.index_filter = true;
        true
    }

    fn accept_index_top_n(&mut self, order_by: &[(usize, bool)], limit: u64) -> bool {
        if self.covering
            || !self.index_filter
            || self.top_n.is_some()
            || self.table.has_dirty_content()
            || order_by.is_empty()
        {
            return false;
        }
        let Some(index) = self
            .table
            .indexes()
            .iter()
            .find(|index| index.id == self.index_id)
        else {
            return false;
        };
        if order_by.iter().any(|(offset, _)| {
            self.keep
                .get(*offset)
                .is_none_or(|physical| !index.column_offsets.contains(physical))
        }) {
            return false;
        }
        self.top_n = Some(crate::remote_scan::PushdownTopN {
            order_by: order_by
                .iter()
                .map(|(offset, desc)| crate::remote_scan::PushdownTopNOrder {
                    offset: *offset,
                    desc: *desc,
                })
                .collect(),
            limit,
        });
        true
    }

    fn scanned_rows_counter(&self) -> Option<Rc<Cell<u64>>> {
        Some(Rc::clone(&self.scanned))
    }

    /// Go's `keep order:true` on the `IndexRangeScan` of an `IndexLookUp`.
    /// Accepting means the handle batch is read in index order rather than
    /// handle order; see the type doc.
    fn accept_keep_order(&mut self, descending: bool) -> bool {
        self.can_reorder_handles = false;
        self.descending = descending;
        true
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexMergeKind {
    Union,
    Intersection,
}

/// A non-MV IndexMerge reader: each partial index produces row handles, then
/// the root reader unions or intersects those handles before fetching rows.
/// The TABLE column offset each of `columns` stands for, by name.
///
/// `None` when any of them is not a stored column of `table`, which leaves
/// the caller to keep whatever default its source already has rather than
/// decode the wrong slot. Hidden expression-index columns are appended after
/// the visible ones, so a visible column's position here is the offset the
/// row decoder wants.
#[must_use]
pub(crate) fn stored_column_offsets(
    table: &crate::KvTable,
    columns: &[(String, tidb_datatype::FieldType)],
) -> Option<Vec<usize>> {
    columns
        .iter()
        .map(|(name, _)| {
            table
                .columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(name))
        })
        .collect()
}

pub(crate) struct IndexMergeSourceExec {
    meta: ExecutorMeta,
    table: KvTable,
    kind: IndexMergeKind,
    partials: Vec<(i64, Vec<IndexRange>)>,
    partial_at: usize,
    cursor: Option<IndexRangeCursor>,
    seen: BTreeSet<TableHandle>,
    intersection: Option<btree_set::IntoIter<TableHandle>>,
    produced: Rc<Cell<u64>>,
    decode_context: crate::kv_table::RowDecodeContext,
}

impl IndexMergeSourceExec {
    #[must_use]
    pub(crate) fn new_with_context(
        meta: ExecutorMeta,
        table: KvTable,
        kind: IndexMergeKind,
        partials: Vec<(i64, Vec<IndexRange>)>,
        decode_context: crate::kv_table::RowDecodeContext,
    ) -> Self {
        Self {
            meta,
            table,
            kind,
            partials,
            partial_at: 0,
            cursor: None,
            seen: BTreeSet::new(),
            intersection: None,
            produced: Rc::new(Cell::new(0)),
            decode_context,
        }
    }

    #[must_use]
    pub(crate) fn produced_rows(&self) -> Rc<Cell<u64>> {
        Rc::clone(&self.produced)
    }

    fn next_union_handle(&mut self) -> Result<Option<TableHandle>, ExecError> {
        loop {
            if let Some(cursor) = self.cursor.as_mut() {
                let handle = cursor
                    .next_handle()
                    .map_err(|_| ExecError::unsupported("index bytes failed to decode"))?;
                if let Some(handle) = handle {
                    if self.seen.insert(handle.clone()) {
                        return Ok(Some(handle));
                    }
                    continue;
                }
                self.cursor = None;
            }
            let Some((index_id, ranges)) = self.partials.get(self.partial_at) else {
                return Ok(None);
            };
            self.partial_at += 1;
            self.cursor = Some(
                self.table
                    .index_ranges_cursor(*index_id, ranges, self.decode_context.zone())
                    .map_err(|_| ExecError::unsupported("index range is not scannable"))?,
            );
        }
    }

    fn partial_handles(
        &mut self,
        index_id: i64,
        ranges: &[IndexRange],
    ) -> Result<BTreeSet<TableHandle>, ExecError> {
        let mut handles = BTreeSet::new();
        let mut cursor = self
            .table
            .index_ranges_cursor(index_id, ranges, self.decode_context.zone())
            .map_err(|_| ExecError::unsupported("index range is not scannable"))?;
        while let Some(handle) = cursor
            .next_handle()
            .map_err(|_| ExecError::unsupported("index bytes failed to decode"))?
        {
            handles.insert(handle);
        }
        Ok(handles)
    }

    fn next_intersection_handle(&mut self) -> Result<Option<TableHandle>, ExecError> {
        if self.intersection.is_none() {
            let partials = self.partials.clone();
            let mut partials = partials.into_iter();
            let Some((index_id, ranges)) = partials.next() else {
                return Ok(None);
            };
            let mut handles = self.partial_handles(index_id, &ranges)?;
            for (index_id, ranges) in partials {
                let right = self.partial_handles(index_id, &ranges)?;
                handles.retain(|handle| right.contains(handle));
                if handles.is_empty() {
                    break;
                }
            }
            self.intersection = Some(handles.into_iter());
        }
        Ok(self
            .intersection
            .as_mut()
            .and_then(std::iter::Iterator::next))
    }

    fn next_handle(&mut self) -> Result<Option<TableHandle>, ExecError> {
        match self.kind {
            IndexMergeKind::Union => self.next_union_handle(),
            IndexMergeKind::Intersection => self.next_intersection_handle(),
        }
    }
}

impl Executor for IndexMergeSourceExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.partial_at = 0;
        self.cursor = None;
        self.seen.clear();
        self.intersection = None;
        self.produced.set(0);
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        while req.num_rows() < self.meta.max_chunk_size() {
            let Some(handle) = self.next_handle()? else {
                return Ok(());
            };
            let row = self
                .table
                .get_row_by_handle_with_context(&handle, &self.decode_context)
                .map_err(|error| {
                    ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
                })?;
            if let Some(row) = row {
                for (column, value) in visible_of(&self.table, &row).iter().enumerate() {
                    req.append_datum(column, value);
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
    /// Go's clustered common-handle table path. Each probe supplies a non-empty
    /// leading primary-key prefix and scans the matching record-key range,
    /// without manufacturing a secondary PRIMARY index entry. A complete
    /// tuple is the one-row special case of the same range.
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
    /// The open local fallback over one batch of common-handle ranges.
    record_cursor: Option<RowCursor>,
    /// The open remote scan over one index-join task's common-handle ranges.
    /// Go sends the task's complete range set through one table reader and
    /// leaves region concurrency to DistSQL.
    remote_cursor: Option<RemoteRowCursor>,
    /// Rows returned by one batched handle lookup. Keeping the batch across
    /// output chunks avoids one remote point read per row handle.
    lookup_rows: Vec<Option<Vec<Datum>>>,
    lookup_row_at: usize,
    /// Rows produced since `open`, which the trace reads as `actRows`.
    produced: Rc<Cell<u64>>,
    /// See [`HandleSourceExec`].
    decode_context: crate::kv_table::RowDecodeContext,
    /// Statement flags, warning sink, and time zone carried by a batched
    /// common-handle coprocessor request.
    statement: PushdownStatementContext,
    /// Leaf-local predicates that Go places below the index join's inner
    /// reader. They are evaluated over the same full table row this source
    /// returns, so replacing the originally-built leaf cannot drop them.
    filters: Vec<Expression>,
    filter_context: Option<crate::StmtContext>,
    filter_chunk: Chunk,
    /// Physical table offsets decoded from storage, sorted and unique.
    decode_offsets: Option<Vec<usize>>,
    /// Physical table offsets emitted by a bare lookup, in logical child
    /// order. `None` emits a physical-width row for retained aggregation.
    output_offsets: Option<Vec<usize>>,
    /// Optional probe channel used when this leaf is nested inside a
    /// composite IndexHashJoin inner subtree.
    shared_probes: Option<Rc<RefCell<SharedIndexJoinProbes>>>,
    shared_generation: u64,
}

/// Go's IndexHashJoin table reader receives the whole outer task's handles in
/// one request. The join above caps that task at the same 20k limit.
const INDEX_LOOKUP_BATCH_SIZE: usize = 20_000;

/// Go's index-lookup table reader sends one coprocessor request for the whole
/// lookup batch. The transport partitions its ranges into concurrent region
/// tasks, so splitting the same batch here only adds request setup and forces
/// the ordered cursor to wait at every artificial boundary.
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
        let statement = PushdownStatementContext::from_stmt(decode_context.expression());
        // Go evaluates the inner Selection before its final projection. Keep
        // that physical table shape here even when `meta` already describes
        // the compact row emitted to the join above.
        let filter_types = table
            .visible_columns()
            .iter()
            .map(|column| column.field_type.clone())
            .collect::<Vec<_>>();
        let filter_chunk = Chunk::new(&filter_types, meta.init_cap(), meta.max_chunk_size());
        IndexJoinLookupExec {
            meta,
            table,
            object,
            probe_parts: Vec::new(),
            probes: Vec::new(),
            next_probe: 0,
            cursor: None,
            record_cursor: None,
            remote_cursor: None,
            lookup_rows: Vec::new(),
            lookup_row_at: 0,
            produced: Rc::new(Cell::new(0)),
            decode_context,
            statement,
            filters: Vec::new(),
            filter_context: None,
            filter_chunk,
            decode_offsets: None,
            output_offsets: None,
            shared_probes: None,
            shared_generation: 0,
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
        self.record_cursor = None;
        self.remote_cursor = None;
        self.lookup_rows.clear();
        self.lookup_row_at = 0;
    }

    /// Forks one common-handle prefix task and starts its remote table reader.
    ///
    /// Go's `IndexNestedLoopHashJoin` rebuilds one inner executor per outer
    /// task and lets several of those executors fetch concurrently.  The Rust
    /// executor remains single-threaded, but a remote cursor is already backed
    /// by its own DistSQL worker.  Forking the immutable lookup description and
    /// opening that cursor therefore gives the same bounded request overlap
    /// without making statement-local `Rc` state cross a thread boundary.
    pub(crate) fn fork_prefetched_common_handle(
        &self,
        probes: Vec<Vec<Datum>>,
    ) -> Result<Option<Self>, ExecError> {
        let complete_common_handle = matches!(self.object, LookupObject::CommonHandle)
            && (self.probe_parts.is_empty()
                || self.probe_parts.len() == self.table.common_handle_offsets().len());
        if !matches!(self.object, LookupObject::CommonHandle)
            || complete_common_handle
            || self.shared_probes.is_some()
        {
            return Ok(None);
        }

        let filter_types = self
            .table
            .visible_columns()
            .iter()
            .map(|column| column.field_type.clone())
            .collect::<Vec<_>>();
        let mut task = Self {
            meta: self.meta.clone(),
            table: self.table.clone(),
            object: self.object.clone(),
            probe_parts: self.probe_parts.clone(),
            probes: Vec::new(),
            next_probe: 0,
            cursor: None,
            record_cursor: None,
            remote_cursor: None,
            lookup_rows: Vec::new(),
            lookup_row_at: 0,
            produced: Rc::clone(&self.produced),
            decode_context: self.decode_context.clone(),
            statement: self.statement.clone(),
            filters: self.filters.clone(),
            filter_context: self.filter_context.clone(),
            filter_chunk: Chunk::new(
                &filter_types,
                self.meta.init_cap(),
                self.meta.max_chunk_size(),
            ),
            decode_offsets: self.decode_offsets.clone(),
            output_offsets: self.output_offsets.clone(),
            shared_probes: None,
            shared_generation: 0,
        };
        task.set_probes(probes);
        task.open_prefetched_common_handle_cursor()
            .map(|opened| opened.then_some(task))
    }

    /// Connects this leaf to the outer join's shared probe channel.
    pub(crate) fn set_shared_probes(&mut self, probes: Rc<RefCell<SharedIndexJoinProbes>>) {
        self.shared_probes = Some(probes);
        self.shared_generation = 0;
    }

    fn refresh_shared_probes(&mut self) {
        let Some(shared) = self.shared_probes.as_ref().cloned() else {
            return;
        };
        let shared = shared.borrow();
        if self.shared_generation != shared.generation {
            self.set_probes(shared.probes.clone());
            self.shared_generation = shared.generation;
        }
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

    /// Narrows storage decoding while preserving the row shape required by
    /// the operator above the lookup. `required_offsets` are physical inputs
    /// retained by a grouped aggregation; filter inputs are added here from
    /// the executable expressions so they cannot drift from evaluation.
    pub(crate) fn set_column_projection(
        &mut self,
        output_offsets: Option<Vec<usize>>,
        required_offsets: impl IntoIterator<Item = usize>,
    ) {
        let mut decode_offsets = BTreeSet::new();
        decode_offsets.extend(required_offsets);
        if let Some(offsets) = &output_offsets {
            decode_offsets.extend(offsets.iter().copied());
        }
        decode_offsets.extend(expression_column_offsets(&self.filters));
        self.decode_offsets = Some(decode_offsets.into_iter().collect());
        self.output_offsets = output_offsets;
    }

    /// The live count of rows this source produced.
    #[must_use]
    pub fn produced_rows(&self) -> Rc<Cell<u64>> {
        Rc::clone(&self.produced)
    }

    /// Assembles the next object's leading key from one dynamic probe and its
    /// static template. An incomplete dynamic tuple is skipped like Go's
    /// failed `constructDatumLookupKey` conversion.
    fn next_probe(&mut self) -> Option<Vec<Datum>> {
        loop {
            let dynamic_probe = self.probes.get(self.next_probe)?.clone();
            self.next_probe += 1;
            if self.probe_parts.is_empty() {
                return Some(dynamic_probe);
            }
            if let Some(probe) = self
                .probe_parts
                .iter()
                .map(|part| match part {
                    LookupProbePart::Dynamic(offset) => dynamic_probe.get(*offset).cloned(),
                    LookupProbePart::Constant(value) => Some(value.clone()),
                })
                .collect::<Option<Vec<_>>>()
                .and_then(|probe| self.probe_in_key_domain(probe))
            {
                return Some(probe);
            }
        }
    }

    /// Go `innerWorker.constructDatumLookupKey`
    /// (`executor/join/index_lookup_join.go`): the outer value is converted to
    /// the INNER key column's type, and the lookup happens with the converted
    /// value -- `dLookupKey = append(dLookupKey, innerValue)`.
    ///
    /// Both of Go's refusals are kept, because each one is a row that must NOT
    /// match rather than an optimization: a conversion that overflows the
    /// inner type means no inner row can hold the value, and a converted value
    /// that no longer compares equal to the original means the conversion was
    /// lossy (`if cmp != 0 { return nil, nil, nil }`). A NULL outer key never
    /// probes at all under a plain `=`.
    ///
    /// Probing with the RAW outer value was a wrong-answer bug, not a slower
    /// path: an index key is encoded from the value, so
    /// `t(c1 decimal(4,1))` holding `0.0` never found `t1(c1 decimal(4,2))`
    /// holding `0.00`, and `select /*+ INL_JOIN(t1) */ * from t left join t1
    /// on t1.c1 = t.c1` null-extended a row TiDB matches. The hash join is
    /// unaffected because it compares numerically instead of by encoded key,
    /// so the same statement answered differently with and without the hint.
    fn probe_in_key_domain(&self, probe: Vec<Datum>) -> Option<Vec<Datum>> {
        let Some(types) = self.probe_key_types() else {
            return Some(probe);
        };
        probe
            .into_iter()
            .enumerate()
            .map(|(at, value)| match types.get(at) {
                Some(column) => crate::driver::point_get_key::point_get_value(column, &value),
                // A probe wider than the key it opens is left alone; the
                // cursor below refuses it on its own terms.
                None => Some(value),
            })
            .collect()
    }

    /// The field types of the object-key columns a probe is encoded against,
    /// or `None` for an object whose own arm already screens its probe.
    fn probe_key_types(&self) -> Option<Vec<FieldType>> {
        let columns = self.table.visible_columns();
        match &self.object {
            LookupObject::Index(index_id) => {
                let index = self
                    .table
                    .indexes()
                    .iter()
                    .find(|index| index.id == *index_id)?;
                Some(
                    index
                        .column_offsets
                        .iter()
                        .filter_map(|offset| columns.get(*offset))
                        .map(|column| column.field_type.clone())
                        .collect(),
                )
            }
            LookupObject::CommonHandle => Some(
                self.table
                    .common_handle_offsets()
                    .iter()
                    .filter_map(|offset| columns.get(*offset))
                    .map(|column| column.field_type.clone())
                    .collect(),
            ),
            // The integer-handle arm already refuses a value that is not an
            // integer handle, which is the same screen in that domain.
            LookupObject::Handle => None,
        }
    }

    /// The next handle the probe list reaches, opening the next secondary-index
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
            let Some(probe) = self.next_probe() else {
                return Ok(None);
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
                    unreachable!("common handles are record-key ranges, not handle lookups")
                }
            }
        }
    }

    /// The next row in the common-handle record ranges. Go uses the table path
    /// for both a complete composite key and a leading prefix; encoding equal
    /// low/high tuple bounds makes the former a one-record range and the latter
    /// a range over every suffix.
    fn next_common_handle_row(&mut self) -> Result<Option<Vec<Datum>>, ExecError> {
        loop {
            if let Some(cursor) = self.remote_cursor.as_mut() {
                let row = cursor.next_row().map_err(|error| {
                    ExecError::unsupported(format!("common-handle remote lookup failed: {error:?}"))
                })?;
                if row.is_some() {
                    return Ok(row);
                }
                self.remote_cursor = None;
            }
            if let Some(cursor) = self.record_cursor.as_mut() {
                let row = cursor.next_row().map_err(|error| {
                    ExecError::unsupported(format!("table bytes failed to decode: {error:?}"))
                })?;
                if let Some((_, row)) = row {
                    return Ok(Some(row));
                }
                self.record_cursor = None;
            }
            let mut ranges = Vec::with_capacity(INDEX_LOOKUP_BATCH_SIZE);
            while ranges.len() < INDEX_LOOKUP_BATCH_SIZE {
                let Some(probe) = self.next_probe() else {
                    break;
                };
                ranges.push(IndexRange {
                    low: probe.clone(),
                    high: probe,
                    low_exclusive: false,
                    high_exclusive: false,
                });
            }
            if ranges.is_empty() {
                return Ok(None);
            }
            let keep = self
                .decode_offsets
                .clone()
                .unwrap_or_else(|| (0..self.table.visible_column_count()).collect::<Vec<_>>());
            let remote_predicates = scan_predicates_for_filters(&self.filters, &keep);
            let remote_cursor = self
                .table
                .pushdown_row_cursor_with_context(
                    &keep,
                    &remote_predicates,
                    None,
                    None,
                    None,
                    Some(&ranges),
                    false,
                    false,
                    crate::remote_scan::DEFAULT_SCAN_READ_AHEAD_BATCHES,
                    &self.decode_context,
                    &self.statement,
                )
                .map_err(|error| {
                    ExecError::unsupported(format!(
                        "common-handle remote lookup is not scannable: {error:?}"
                    ))
                })?;
            if let Some(cursor) = remote_cursor {
                self.remote_cursor = Some(cursor);
            } else {
                self.record_cursor = Some(
                    self.table
                        .row_cursor_projected_with_context(
                            Some(&keep),
                            Some(&ranges),
                            &self.decode_context,
                        )
                        .map_err(|_| {
                            ExecError::unsupported("common handle range is not scannable")
                        })?,
                );
            }
        }
    }

    /// Opens the one remote reader for a complete common-handle prefix task.
    /// Returning `false` leaves the source untouched for the synchronous local
    /// fallback.
    fn open_prefetched_common_handle_cursor(&mut self) -> Result<bool, ExecError> {
        let first_probe = self.next_probe;
        let mut ranges = Vec::with_capacity(self.probes.len().saturating_sub(first_probe));
        while let Some(probe) = self.next_probe() {
            ranges.push(IndexRange {
                low: probe.clone(),
                high: probe,
                low_exclusive: false,
                high_exclusive: false,
            });
        }
        if ranges.is_empty() {
            self.next_probe = first_probe;
            return Ok(false);
        }
        let keep = self
            .decode_offsets
            .clone()
            .unwrap_or_else(|| (0..self.table.visible_column_count()).collect::<Vec<_>>());
        let remote_predicates = scan_predicates_for_filters(&self.filters, &keep);
        let remote_cursor = self
            .table
            .pushdown_row_cursor_with_context(
                &keep,
                &remote_predicates,
                None,
                None,
                None,
                Some(&ranges),
                false,
                false,
                crate::remote_scan::INDEX_JOIN_READ_AHEAD_BATCHES,
                &self.decode_context,
                &self.statement,
            )
            .map_err(|error| {
                ExecError::unsupported(format!(
                    "common-handle remote lookup is not scannable: {error:?}"
                ))
            })?;
        match remote_cursor {
            Some(cursor) => {
                self.remote_cursor = Some(cursor);
                Ok(true)
            }
            None => {
                self.next_probe = first_probe;
                Ok(false)
            }
        }
    }

    fn next_batched_handle(&mut self) -> Result<Option<TableHandle>, ExecError> {
        if !matches!(self.object, LookupObject::CommonHandle) {
            return self.next_handle();
        }
        loop {
            let Some(probe) = self.next_probe() else {
                return Ok(None);
            };
            if let Ok(handle) = self
                .table
                .common_handle_of_values(&probe, self.decode_context.zone())
            {
                return Ok(Some(handle));
            }
        }
    }

    fn next_lookup_row(&mut self) -> Result<Option<Vec<Datum>>, ExecError> {
        let complete_common_handle = matches!(self.object, LookupObject::CommonHandle)
            && (self.probe_parts.is_empty()
                || self.probe_parts.len() == self.table.common_handle_offsets().len());
        if !matches!(self.object, LookupObject::CommonHandle) || complete_common_handle {
            loop {
                if self.lookup_row_at == self.lookup_rows.len() {
                    self.lookup_rows.clear();
                    self.lookup_row_at = 0;
                    let mut handles = Vec::with_capacity(INDEX_LOOKUP_BATCH_SIZE);
                    while handles.len() < INDEX_LOOKUP_BATCH_SIZE {
                        let Some(handle) = self.next_batched_handle()? else {
                            break;
                        };
                        handles.push(handle);
                    }
                    if handles.is_empty() {
                        return Ok(None);
                    }
                    self.lookup_rows = self
                        .table
                        .get_rows_by_handles_projected_with_context(
                            &handles,
                            self.decode_offsets.as_deref(),
                            &self.decode_context,
                        )
                        .map_err(|error| {
                            ExecError::unsupported(format!(
                                "table bytes failed to decode: {error:?}"
                            ))
                        })?;
                }
                let row = self.lookup_rows[self.lookup_row_at].take();
                self.lookup_row_at += 1;
                if row.is_some() {
                    return Ok(row);
                }
            }
        }
        if matches!(self.object, LookupObject::CommonHandle) {
            return self.next_common_handle_row();
        }
        unreachable!("integer and secondary-index handles use the batched lookup above")
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

    fn physical_row(&self, decoded: &[Datum]) -> Result<Vec<Datum>, ExecError> {
        let Some(offsets) = &self.decode_offsets else {
            return Ok(visible_of(&self.table, decoded).to_vec());
        };
        if offsets.len() != decoded.len() {
            return Err(ExecError::unsupported(
                "index-join decoded row does not match its projection",
            ));
        }
        let mut physical = vec![Datum::Null; self.table.visible_column_count()];
        for (offset, value) in offsets.iter().copied().zip(decoded) {
            let target = physical.get_mut(offset).ok_or_else(|| {
                ExecError::unsupported("index-join decode column is outside the table")
            })?;
            *target = value.clone();
        }
        Ok(physical)
    }
}

pub(crate) fn expression_column_offsets(expressions: &[Expression]) -> Vec<usize> {
    fn collect(expression: &Expression, offsets: &mut BTreeSet<usize>) {
        match expression {
            Expression::Column(column) => {
                if let Ok(offset) = usize::try_from(column.index) {
                    offsets.insert(offset);
                }
            }
            Expression::ScalarFunction(function) => {
                for argument in &function.args {
                    collect(argument, offsets);
                }
            }
            Expression::Constant(_) | Expression::CorrelatedColumn(_) => {}
        }
    }

    let mut offsets = BTreeSet::new();
    for expression in expressions {
        collect(expression, &mut offsets);
    }
    offsets.into_iter().collect()
}

/// Describes the comparison forms Go lowers into a coprocessor Selection.
/// Index-join filters are already resolved executor expressions rather than
/// AST predicates, so this adapter keeps the same fail-closed shape:
/// unsupported expressions remain client-side filters.
fn scan_predicate_from_expression(expression: &Expression) -> Option<ScanPredicate> {
    let Expression::ScalarFunction(function) = expression else {
        return None;
    };
    let function_name = function.func_name.lowercase();
    let operation = match function_name.as_ref() {
        "eq" => ScanComparisonOp::Eq,
        "ne" => ScanComparisonOp::Ne,
        "lt" => ScanComparisonOp::Lt,
        "le" => ScanComparisonOp::Le,
        "gt" => ScanComparisonOp::Gt,
        "ge" => ScanComparisonOp::Ge,
        _ => return None,
    };
    let [left, right] = function.args.as_slice() else {
        return None;
    };
    if let (Expression::Column(left), Expression::Column(right)) = (left, right) {
        return Some(ScanPredicate::ColumnCompare(ScanColumnComparison {
            left_offset: u32::try_from(left.index).ok()?,
            left_type: left.get_static_type()?.clone(),
            right_offset: u32::try_from(right.index).ok()?,
            right_type: right.get_static_type()?.clone(),
            op: operation,
        }));
    }
    let (column, constant, column_on_left) = match (left, right) {
        (Expression::Column(column), Expression::Constant(constant)) => (column, constant, true),
        (Expression::Constant(constant), Expression::Column(column)) => (column, constant, false),
        _ => return None,
    };
    let literal = constant.value.clone();
    (!matches!(literal, Datum::Null)).then(|| {
        Some(ScanPredicate::Compare(ScanComparison {
            column_offset: u32::try_from(column.index).ok()?,
            // The column's, which is the derived collation whenever no
            // argument is explicit; `adopt_refined_literals` replaces it with
            // the built expression's for a conjunct that goes through
            // `split_scan_predicates`.
            collation: column.get_static_type()?.collation(),
            column_type: column.get_static_type()?.clone(),
            literal_type: constant.get_static_type()?.clone(),
            op: operation,
            literal,
            column_on_left,
        }))
    })?
}

fn scan_predicates_for_filters(filters: &[Expression], keep: &[usize]) -> Vec<ScanPredicate> {
    filters
        .iter()
        .filter_map(|filter| {
            let mut filter = filter.clone();
            crate::predicate_pushdown::remap_expression(&mut filter, keep)?;
            scan_predicate_from_expression(&filter)
        })
        .collect()
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
        self.record_cursor = None;
        self.remote_cursor = None;
        self.lookup_rows.clear();
        self.lookup_row_at = 0;
        self.produced.set(0);
        self.filter_chunk.reset();
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        self.refresh_shared_probes();
        req.reset();
        let cap = self.meta.max_chunk_size();
        while req.num_rows() < cap {
            let Some(row) = self.next_lookup_row()? else {
                return Ok(());
            };
            // An index entry whose row is gone is not a row, as in
            // [`IndexRangeSourceExec`].
            let physical = self.physical_row(&row)?;
            let passes = self.row_passes_filters(&physical)?;
            if !passes {
                continue;
            }
            if let Some(offsets) = &self.output_offsets {
                for (output, source) in offsets.iter().copied().enumerate() {
                    let value = physical.get(source).ok_or_else(|| {
                        ExecError::unsupported("index-join output column is outside the table")
                    })?;
                    req.append_datum(output, value);
                }
            } else {
                for (output, value) in physical.iter().enumerate() {
                    req.append_datum(output, value);
                }
            }
            self.produced.set(self.produced.get() + 1);
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.cursor = None;
        self.record_cursor = None;
        self.remote_cursor = None;
        self.lookup_rows.clear();
        self.lookup_row_at = 0;
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
    use std::collections::HashMap;
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
        fn batch_get(&mut self, keys: &[Key]) -> Result<HashMap<Key, Vec<u8>>, StorageError> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.inner.batch_get(keys)
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
        fn iter_reverse(
            &mut self,
            upper_bound: Option<&Key>,
            lower_bound: Option<&Key>,
        ) -> Result<Box<dyn StorageIterator>, StorageError> {
            Ok(Box::new(CountingIterator {
                inner: self.inner.iter_reverse(upper_bound, lower_bound)?,
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
            comment: String::new(),
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
        // The source still performs a table lookup for this covering shape,
        // but the handles are admitted as one batch at the storage seam.
        assert_eq!(
            gets.load(Ordering::Relaxed),
            1,
            "the five rows share one table lookup batch"
        );
    }

    /// Go seeds an IndexLookUp's first handle task from the parent's
    /// `RequiredRows`. A non-index residual prevents a pushed-down Limit, but
    /// the root Limit still asks for five rows, so the double read must not
    /// prefetch and decode a full 1,024-handle batch.
    #[test]
    fn an_ordered_double_read_starts_at_the_parent_required_rows() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, gets) = table_of(ROWS, true);
        let rows = run_select_on(
            "SELECT * FROM t USE INDEX (ib) WHERE b > 0 AND c >= 0 ORDER BY b LIMIT 5",
            &catalog,
            &ctx,
        )
        .unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(
            entries.load(Ordering::Relaxed),
            5,
            "the first index task follows the root Limit's RequiredRows"
        );
        assert_eq!(
            gets.load(Ordering::Relaxed),
            1,
            "one five-handle lookup task"
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
                    comment: String::new(),
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
                    comment: String::new(),
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

    /// THE DOUBLE READ IS BATCHED: N index rows produce one batch get.
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
    /// `IndexRangeSourceExec` now hands the whole handle batch to
    /// `KvTable::get_rows_by_handles_projected_with_context`, so against a
    /// cluster it issues one snapshot batch round trip for these rows. The
    /// assertion below is `1`; before the fix it was `n`.
    #[test]
    fn the_double_read_issues_one_batch_get_per_index_batch() {
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
            1,
            "the 50 index rows share one batch get"
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

    /// Go answers a DESC order by walking the matching index range backwards,
    /// so the pushed cap reads only the requested suffix.
    #[test]
    fn a_descending_order_by_reads_the_index_backwards() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, true);
        let rows = run_select_on(
            "SELECT a FROM t WHERE b > 0 ORDER BY b DESC LIMIT 3",
            &catalog,
            &ctx,
        )
        .unwrap();
        assert_eq!(first_column(&rows), vec![ROWS, ROWS - 1, ROWS - 2]);
        assert_eq!(entries.load(Ordering::Relaxed), 3);
    }

    /// Go pushes the cap below a cop `Selection`, including one whose
    /// arithmetic predicate cannot become an access range. The scan may stop
    /// after a storage batch once the Selection has produced enough rows.
    #[test]
    fn an_arithmetic_selection_allows_the_cap_to_push() {
        let ctx = crate::StmtContext::for_query();
        let (catalog, entries, _) = table_of(ROWS, false);
        let rows =
            run_select_on("SELECT a FROM t WHERE c + 1 > 1 LIMIT 3", &catalog, &ctx).unwrap();
        assert_eq!(first_column(&rows), vec![1, 2, 3]);
        let read = entries.load(Ordering::Relaxed);
        assert!(
            (3..ROWS as usize).contains(&read),
            "the pushed cap should stop the arithmetic Selection early, read {read} rows"
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
                    comment: String::new(),
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
            assert!(exec.accept_keep_order(false));
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
                    comment: String::new(),
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
