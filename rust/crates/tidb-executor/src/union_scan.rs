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

//! `pkg/executor/union_scan.go`: `UnionScanExec` -- the operator that makes a
//! transaction see its own uncommitted writes.
//!
//! # Completeness
//!
//! Every production symbol of the Go file is present: `UnionScanExec` (:38),
//! `Open` (:75), `open` (:85), `Next` (:139), `Close` (:190), `getOneRow`
//! (:201), `getSnapshotRow` (:242), `getAddedRow` (:283), `compareExec` (:294)
//! and `compareExec.compare` (:310).
//!
//! ONE part of `open` is a boundary rather than a port: the type switch at
//! :117-130 that picks WHICH memory reader supplies the added rows. It
//! dispatches on `*TableReaderExecutor` / `*IndexReaderExecutor` /
//! `*IndexLookUpExecutor` / `*IndexMergeReaderExecutor` / `*MPPGather` and
//! calls `buildMemTableReader` / `buildMemIndexReader` /
//! `buildMemIndexLookUpReader` / `buildMemIndexMergeReader`, all of which live
//! in `pkg/executor/mem_reader.go` and read private fields (`x.kvRanges`) of
//! executors that are not ported. The cursor those builders produce is
//! therefore a constructor input here -- see [`UnionScanExec::new`] -- and
//! [`crate::mem_reader`] is where the readers themselves already live. So:
//! this module lands the MERGE complete, and names its one builder-shaped
//! hole.
//!
//! # What the merge actually is
//!
//! Two sources, merged one row at a time by `getOneRow` (:201):
//!
//! * the **snapshot** side: the child executor's rows, i.e. what committed
//!   data says;
//! * the **added** side: `addedRowsIter`, the rows THIS transaction wrote,
//!   already decoded by [`crate::mem_reader`].
//!
//! ## Row ordering
//!
//! `getOneRow` (:201-240) is a two-way merge step, not a concatenation. It
//! peeks the head of each side and picks one:
//!
//! * added side exhausted (:213) -> take the snapshot row;
//! * snapshot side exhausted (:216) -> take the added row;
//! * both present (:219) -> `compare(snapshotRow, addedRow)`, and
//!   `isSnapshotRow := isSnapshotRowInt < 0` (:223). **Strictly less** -- so on
//!   a TIE the ADDED row is emitted first. That asymmetry is load-bearing and
//!   is reproduced exactly.
//!
//! Only the chosen side advances: `cursor4SnapshotRows++` (:235) or
//! `cursor4AddRows = nil` (:237). A snapshot row that tied is NOT consumed; it
//! is compared again against the NEXT added row.
//!
//! The order the merge PRESERVES is whatever `compareExec.compare` (:310)
//! defines: the index columns listed in `usedIndex`, in that order, compared
//! with `collators[colOff]` (note: indexed by the COLUMN offset, not by the
//! position within `usedIndex`), then the handle columns via
//! `handleCols.Compare`, with every result negated when `desc`. Both sides
//! must already be sorted that way; that is exactly what
//! `keepOrder && needExtraSorting` buys on the added side (see
//! `mem_reader.go` :100/:406) and what the child's `keepOrder` buys on the
//! snapshot side.
//!
//! ## Dirty-row merge, and where shadowing really happens
//!
//! A committed row that this transaction has UPDATEd or DELETEd must not
//! appear. That suppression is NOT done by the comparison above -- it is done
//! by KEY, in `getSnapshotRow` (:259-278):
//!
//! 1. build the snapshot row's handle (`handleCols.BuildHandle`, :261);
//! 2. encode its record key -- under the row's own `_tidb_tid` value when the
//!    chunk carries one (`physTblIDIdx >= 0`, :266-268, dynamic partition
//!    prune), else under the table's record prefix (:270);
//! 3. `memBufSnap.Get(checkKey)`; if it returns WITHOUT error the snapshot row
//!    is DROPPED (`continue`, :275).
//!
//! So a handle the transaction touched at all -- insert-over-existing, update
//! or delete -- loses its committed row, and whatever the transaction wrote
//! for that handle comes back through `addedRowsIter` instead (a DELETE wrote
//! a tombstone there, which the mem readers skip, so the row simply
//! disappears). Go's own comment at :273 records the deliberate gap: an
//! insert colliding with a committed handle "means there is conflict and the
//! transaction will fail to commit, but for simplicity, we don't handle it
//! here."
//!
//! `cacheTable != nil` short-circuits the snapshot side entirely (:243-246):
//! a cached-table read has no storage half, so `getSnapshotRow` returns
//! nothing and the added side is the whole answer.
//!
//! ## Batch refill, and the loop that must not spin
//!
//! `getSnapshotRow` refills `snapshotRows` from the child (:253-279) in a
//! `for len(us.snapshotRows) == 0` loop, because a whole child chunk can be
//! filtered away by step 3 above. It stops on error or on an EMPTY child
//! chunk. Reproduced exactly, including that an empty chunk from the child --
//! not an empty `snapshotRows` -- is what ends the scan.
//!
//! # Sequential here, sequential there
//!
//! `union_scan.go` contains no `go` statement. `Next` (:139) calls
//! `getOneRow` (:149), which calls `getSnapshotRow` (:202) -- which pulls the
//! child with `exec.Next(ctx, us.Children(0), ...)` (:254) -- and
//! `getAddedRow` (:206), which pulls `addedRowsIter` (:286). All on the
//! caller's goroutine. The only concurrency anywhere near this operator lives
//! INSIDE the child (a `TableReader`'s coprocessor workers), whose ordering
//! contract is the child's own and is unchanged by this file. A sequential
//! Rust port therefore loses nothing: the emitted order is fully determined by
//! the child's order, the added iterator's order, and `compare`.
//!
//! `memBuf.RLock()`/`RUnlock()` (:109, :140) guard the membuffer against a
//! concurrent writer on the same transaction. Not modelled: this tier hands
//! the buffer in through [`MemBufferSnapshotGetter`] and
//! [`crate::mem_reader::MemRowsIter`], both `&self`, so there is no shared
//! mutable buffer to lock.
//!
//! # Narrowings, every one named
//!
//! * **`plannerutil.HandleCols`** (`pkg/planner/util/handle_cols.go`) is
//!   [`HandleColumns`]: `BuildHandle` (:261) and `Compare` (:327) are its two
//!   uses, and it is a planner type, outside this Go package.
//! * **`kv.MemBuffer.SnapshotGetter()`** (`pkg/kv/kv.go:224`, implemented in
//!   `pkg/store/driver/txn/unionstore_driver.go:138`) is
//!   [`MemBufferSnapshotGetter`]. `us.memBuf` itself (:41) is only ever used
//!   for `RLock`/`RUnlock`, so it has no counterpart.
//! * **`buildMemTableReader` / `buildMemIndexReader` /
//!   `buildMemIndexLookUpReader` / `buildMemIndexMergeReader`** and the
//!   `reader.(type)` switch (:117-130), including the
//!   `SelectionExec` unwrap at :91 and the
//!   `fmt.Errorf("unexpected union scan children:%T")` default (:129): the
//!   added-rows cursor is a constructor input. See "Completeness" above.
//! * **`table.CastValue(ctx, datum, col, false, true)`** (:167) is
//!   [`crate::driver::write_cast::cast_table_value`], which already models Go's
//!   `forceIgnoreTruncate` switch and names this call site.
//! * **`table.GetZeroValue`** (:173) is [`crate::bad_null::zero_value`].
//! * **`Column.EvalVirtualColumn`** (:161) is `col.VirtualExpr.Eval(ctx, row)`
//!   in Go (`pkg/expression/column.go`); here it is
//!   [`tidb_expr::expression::Expression::eval`] over
//!   [`tidb_expr::column::Column::virtual_expr`], which is the same call.
//! * **`us.table.RecordPrefix()`** (:270) is a `table.Table`; this tier takes
//!   the physical table id instead and encodes with
//!   `tablecodec::encode_row_key_with_handle`, which produces the same bytes
//!   as `EncodeRecordKey(t{id}_r, handle)`.
//! * `tracing.StartRegionEx` / `trace.StartRegion` (:76, :95) are
//!   instrumentation; not ported.
//! * `us.partitionIDMap` (:68) and `us.keepOrder` (:70) are declared here but
//!   READ only by `mem_reader.go`'s builders (`buildMemIndexReader` etc.).
//!   They are kept as fields so the struct is complete, and
//!   [`UnionScanExec::keep_order`] / [`UnionScanExec::partition_id_map`]
//!   expose them to whoever builds the cursor.
//!
//! # Tests
//!
//! Upstream `pkg/executor/union_scan_test.go` is entirely `testkit` SQL
//! (`TestUnionScanForMemBufferReader`, `TestIssue53951`, `TestIssue28073`,
//! `TestIssue32422`, `TestSnapshotWithConcurrentWrite`) -- it drives a real
//! session and store, which this tier has no counterpart for, so it cannot be
//! transcreated row-for-row. `compareExec` has no unit test anywhere in the
//! tree. The tests below are therefore WRITTEN, and each one pins a rule this
//! header quotes a Go line for: the tie-goes-to-added rule (:223), the
//! key-based suppression (:275), the `cacheTable` short circuit (:243), the
//! refill loop's empty-chunk stop (:255), and `compare`'s desc negation
//! (:322/:330).

use std::cmp::Ordering;
use std::collections::BTreeSet;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::mutrow::MutRow;
use tidb_chunk::row::Row;
use tidb_datatype::{Collation, Datum, FieldType, FieldTypeFlags};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_tablecodec::table_key::encode_row_key_with_handle;
use tidb_txnkv::Key;

use crate::bad_null::zero_value;
use crate::driver::write_cast::cast_table_value;
use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::joiner::eval_bool;
use crate::kv_table::TableHandle;
use crate::mem_reader::{MemReaderError, MemRowsIter, RowComparator};
use crate::StmtContext;

/// Go's `err` from a memory-reader call, as an [`ExecError`].
///
/// `union_scan.go` propagates the mem readers' error unchanged (:132, :288);
/// the two error types are separate in Rust only because the mem readers form
/// their own module.
fn from_mem_reader_error(error: MemReaderError) -> ExecError {
    match error {
        MemReaderError::Eval(inner) => *inner,
        MemReaderError::Iteration(message) => {
            ExecError::internal(format!("union scan added rows: {message}"))
        }
        MemReaderError::Decode(message) => {
            ExecError::internal(format!("union scan added rows: {message}"))
        }
        MemReaderError::Unsupported(reason) => ExecError::unsupported(reason),
    }
}

/// boundary: Go `plannerutil.HandleCols`
/// (`pkg/planner/util/handle_cols.go`), reached as `compareExec.handleCols`
/// (`pkg/executor/union_scan.go:307`).
///
/// The handle is the identity a union scan merges on, but how a row's handle
/// is spelled -- one `PKIsHandle` column, a clustered tuple, or the hidden
/// `_tidb_rowid` -- is a planner fact carried down with the plan. Only the two
/// methods `union_scan.go` calls are declared.
pub trait HandleColumns {
    /// Go `HandleCols.BuildHandle(sc, row)`
    /// (`pkg/executor/union_scan.go:261`): the handle of a CHILD chunk row.
    fn build_handle(&self, row: Row<'_>) -> Result<TableHandle, ExecError>;

    /// Go `HandleCols.Compare(a, b, collators, typeCtx)`
    /// (`pkg/executor/union_scan.go:327`): orders two full datum rows by their
    /// handle columns alone. `collators` is the FULL-row collator slice, as in
    /// Go.
    fn compare_handles(
        &self,
        left: &[Datum],
        right: &[Datum],
        collators: &[Collation],
    ) -> Result<Ordering, ExecError>;
}

/// boundary: Go `kv.MemBuffer.SnapshotGetter()` (`pkg/kv/kv.go:224`),
/// stored as `UnionScanExec.memBufSnap` (`union_scan.go:42`).
///
/// "Snapshot" is the membuffer's own word for its content excluding an open
/// staging area; it is NOT a storage snapshot. `union_scan.go` uses it for
/// exactly one question, at :272: *is there an entry for this record key?*
///
/// The value is returned rather than a bare bool so an implementation cannot
/// quietly decide what a TOMBSTONE means. Go's rule at :272-276 is
/// `err == nil` -> drop the snapshot row, and a delete's empty-valued entry
/// satisfies that: the committed row must vanish. An implementation that
/// reported `Ok(None)` for a tombstone would resurrect deleted rows, which is
/// why this contract is written down here.
pub trait MemBufferSnapshotGetter {
    /// Go `kv.Getter.Get(ctx, key)`. `Ok(None)` is Go's `kv.ErrNotExist`;
    /// `Ok(Some(bytes))` is Go's `err == nil`, tombstones (empty `bytes`)
    /// included.
    fn get(&self, key: &Key) -> Result<Option<Vec<u8>>, ExecError>;
}

/// Go `compareExec` (`union_scan.go:294`): the order a union scan must merge
/// in.
pub struct CompareExec {
    /// Go `collators`, indexed by COLUMN OFFSET in the row (not by position in
    /// [`Self::used_index`]) -- see `compare` :315.
    pub collators: Vec<Collation>,
    /// Go `usedIndex`: "the column offsets of the index which Src executor has
    /// used."
    pub used_index: Vec<usize>,
    /// Go `desc`.
    pub desc: bool,
    /// Go `needExtraSorting`: whether the added side needs a sort of its own
    /// to satisfy `keepOrder`, because the required order is not the order of
    /// the kv ranges (partitioned `keepOrder`, or the planner's
    /// `PropMatchedNeedMergeSort`). READ by `mem_reader.go`, not by this file.
    pub need_extra_sorting: bool,
    /// Go `handleCols`: "the handle's position of the below scan plan."
    pub handle_cols: Box<dyn HandleColumns>,
}

impl CompareExec {
    /// Go `compareExec.compare` (:310).
    ///
    /// Index columns first, in `used_index` order; the first non-equal one
    /// decides. Then the handle columns. `desc` negates whichever answer wins,
    /// including the handle one (:322, :330).
    pub fn compare(&self, left: &[Datum], right: &[Datum]) -> Result<Ordering, ExecError> {
        for &col_off in &self.used_index {
            let left_column = left.get(col_off).ok_or_else(|| {
                ExecError::internal("union scan compare: used index offset out of row")
            })?;
            let right_column = right.get(col_off).ok_or_else(|| {
                ExecError::internal("union scan compare: used index offset out of row")
            })?;
            let collator = self.collators.get(col_off).copied().ok_or_else(|| {
                ExecError::internal("union scan compare: no collator for used index offset")
            })?;
            let ordering = left_column
                .compare(right_column, collator)
                .map_err(|error| ExecError::internal(format!("union scan compare: {error:?}")))?;
            if ordering == Ordering::Equal {
                continue;
            }
            if self.desc {
                return Ok(ordering.reverse());
            }
            return Ok(ordering);
        }
        let ordering = self
            .handle_cols
            .compare_handles(left, right, &self.collators)?;
        if self.desc {
            return Ok(ordering.reverse());
        }
        Ok(ordering)
    }
}

/// `compareExec` IS the comparator `mem_reader.go`'s `keepOrder &&
/// needExtraSorting` sort uses -- Go reaches it through the embedded
/// `compareExec` on the `UnionScanExec` the builders are handed
/// (`mem_reader.go:100`, `:406`). With this file ported, the trait's one real
/// implementation is right here.
impl RowComparator for CompareExec {
    fn compare(&self, left: &[Datum], right: &[Datum]) -> Result<Ordering, MemReaderError> {
        CompareExec::compare(self, left, right).map_err(MemReaderError::from)
    }
}

/// Go `*model.ColumnInfo` as `UnionScanExec.columns` (:46) uses it: the column
/// id that identifies `ExtraPhysTblID`, and the type/flags/name that the
/// virtual-column cast at :167-174 needs.
#[derive(Clone, Debug)]
pub struct UnionScanColumn {
    /// Go `ColumnInfo.ID`, compared against `model.ExtraPhysTblID` at :103.
    pub id: i64,
    /// Go `ColumnInfo.Name.O`, only used to name a cast failure.
    pub name: String,
    /// Go `ColumnInfo.FieldType`, carrying the flags read at :172.
    pub field_type: FieldType,
}

/// Go `UnionScanExec` (:38): "merges the rows from dirty table and the rows
/// from distsql request."
pub struct UnionScanExec<C: Columns> {
    /// Go embedded `exec.BaseExecutor`.
    meta: ExecutorMeta,
    /// Go `Children(0)`, the snapshot side.
    child: Box<dyn Executor>,
    /// Go `memBufSnap` (:42).
    mem_buf_snap: Box<dyn MemBufferSnapshotGetter>,
    /// Go `addedRowsIter` (:49). See "Completeness" for why it arrives built.
    added_rows_iter: Box<dyn MemRowsIter>,
    /// Go `conditionsWithVirCol` (:45), evaluated at :178.
    ///
    /// Go `conditions` (:44) is handed to the mem readers instead and is never
    /// read by this file; it belongs to the cursor above.
    conditions_with_vir_col: Vec<Expression>,
    /// Go `columns` (:46).
    columns: Vec<UnionScanColumn>,
    /// Go `table` (:47), narrowed to the physical id its `RecordPrefix()`
    /// encodes.
    table_record_id: i64,
    /// Go `virtualColumnIndex` (:57): "all the indices of virtual columns
    /// [...] sorted in definition to make sure we can compute the virtual
    /// column in right order."
    virtual_column_index: Vec<usize>,
    /// Go `cacheTable != nil` (:60): the read has no storage half.
    cache_table: bool,
    /// Go `physTblIDIdx` (:65), `-1` when unused.
    phys_tbl_id_idx: i64,
    /// Go `partitionIDMap` (:68).
    partition_id_map: BTreeSet<i64>,
    /// Go `keepOrder` (:70).
    keep_order: bool,
    /// Go embedded `compareExec` (:71).
    compare_exec: CompareExec,
    /// Go `cursor4AddRows` (:50). `None` is Go's nil.
    cursor4_add_rows: Option<Vec<Datum>>,
    /// Go `snapshotRows` (:51).
    snapshot_rows: Vec<Vec<Datum>>,
    /// Go `cursor4SnapshotRows` (:52).
    cursor4_snapshot_rows: usize,
    /// Go `snapshotChunkBuffer` (:53), allocated by `open` at :134.
    snapshot_chunk_buffer: Option<Chunk>,
    /// The evaluation context for the virtual columns and
    /// `conditionsWithVirCol`; Go reads it off the session
    /// (`us.Ctx().GetExprCtx().GetEvalCtx()`).
    ctx: C,
    /// Go `us.Ctx().GetSessionVars().StmtCtx`, needed by the virtual-column
    /// cast at :167.
    stmt: StmtContext,
}

/// Everything [`UnionScanExec::new`] needs; the Go builder
/// (`pkg/executor/builder.go:1473` `buildUnionScanExec`) fills the same set.
pub struct UnionScanSpec<C: Columns> {
    /// Go `exec.NewBaseExecutor(sctx, v.Schema(), v.ID(), reader)`.
    pub meta: ExecutorMeta,
    /// Go `Children(0)`.
    pub child: Box<dyn Executor>,
    /// Go `us.memBufSnap`.
    pub mem_buf_snap: Box<dyn MemBufferSnapshotGetter>,
    /// Go `us.addedRowsIter`, already built by a `mem_reader.go` builder.
    pub added_rows_iter: Box<dyn MemRowsIter>,
    /// Go `us.conditionsWithVirCol`.
    pub conditions_with_vir_col: Vec<Expression>,
    /// Go `us.columns`.
    pub columns: Vec<UnionScanColumn>,
    /// The physical id behind Go `us.table.RecordPrefix()`.
    pub table_record_id: i64,
    /// Go `us.virtualColumnIndex`.
    pub virtual_column_index: Vec<usize>,
    /// Go `us.cacheTable != nil`.
    pub cache_table: bool,
    /// Go `us.partitionIDMap`.
    pub partition_id_map: BTreeSet<i64>,
    /// Go `us.keepOrder`.
    pub keep_order: bool,
    /// Go the embedded `compareExec`.
    pub compare_exec: CompareExec,
    /// The expression evaluation context.
    pub ctx: C,
    /// Go `us.Ctx().GetSessionVars().StmtCtx`.
    pub stmt: StmtContext,
}

impl<C: Columns> UnionScanExec<C> {
    /// Builds the operator. `physTblIDIdx` is NOT set here: Go computes it in
    /// `open` (:101-107), and so does [`Executor::open`] below.
    #[must_use]
    pub fn new(spec: UnionScanSpec<C>) -> Self {
        UnionScanExec {
            meta: spec.meta,
            child: spec.child,
            mem_buf_snap: spec.mem_buf_snap,
            added_rows_iter: spec.added_rows_iter,
            conditions_with_vir_col: spec.conditions_with_vir_col,
            columns: spec.columns,
            table_record_id: spec.table_record_id,
            virtual_column_index: spec.virtual_column_index,
            cache_table: spec.cache_table,
            phys_tbl_id_idx: -1,
            partition_id_map: spec.partition_id_map,
            keep_order: spec.keep_order,
            compare_exec: spec.compare_exec,
            cursor4_add_rows: None,
            snapshot_rows: Vec::new(),
            cursor4_snapshot_rows: 0,
            snapshot_chunk_buffer: None,
            ctx: spec.ctx,
            stmt: spec.stmt,
        }
    }

    /// Go `us.keepOrder` (:70): read by `mem_reader.go`'s builders, not by
    /// this file.
    #[must_use]
    pub fn keep_order(&self) -> bool {
        self.keep_order
    }

    /// Go `us.partitionIDMap` (:68): "only required by union scan with global
    /// index", and read by `mem_reader.go`, not by this file.
    #[must_use]
    pub fn partition_id_map(&self) -> &BTreeSet<i64> {
        &self.partition_id_map
    }

    /// Go `us.physTblIDIdx` (:65), as computed by `open` (:101-107).
    #[must_use]
    pub fn phys_tbl_id_idx(&self) -> i64 {
        self.phys_tbl_id_idx
    }

    /// Go `getOneRow` (:201): "gets one result row from dirty table or child."
    ///
    /// `Ok(None)` is Go's `nil, nil` -- both sides exhausted.
    fn get_one_row(&mut self) -> Result<Option<Vec<Datum>>, ExecError> {
        let snapshot_row = self.get_snapshot_row()?;
        let added_row = self.get_added_row()?;

        // :211-229. `isSnapshotRow` starts false, exactly as Go's zero value,
        // so the `snapshotRow == nil` arm (:216) leaves it false.
        let (row, is_snapshot_row) = match (snapshot_row, added_row) {
            (snapshot, None) => (snapshot, true),
            (None, Some(added)) => (Some(added), false),
            (Some(snapshot), Some(added)) => {
                // :219-228. Strictly-less keeps the snapshot row; a TIE emits
                // the added row.
                let ordering = self.compare_exec.compare(&snapshot, &added)?;
                if ordering == Ordering::Less {
                    (Some(snapshot), true)
                } else {
                    (Some(added), false)
                }
            }
        };

        // :230-232.
        let Some(row) = row else {
            return Ok(None);
        };

        // :234-238: only the side that won advances.
        if is_snapshot_row {
            self.cursor4_snapshot_rows += 1;
        } else {
            self.cursor4_add_rows = None;
        }
        Ok(Some(row))
    }

    /// Go `getSnapshotRow` (:242).
    fn get_snapshot_row(&mut self) -> Result<Option<Vec<Datum>>, ExecError> {
        // :243-246. A cached table read has no storage half at all.
        if self.cache_table {
            return Ok(None);
        }
        // :247-249. Still inside the current batch.
        if self.cursor4_snapshot_rows < self.snapshot_rows.len() {
            return Ok(Some(self.snapshot_rows[self.cursor4_snapshot_rows].clone()));
        }
        // :251-252.
        self.cursor4_snapshot_rows = 0;
        self.snapshot_rows.clear();

        let mut buffer = self
            .snapshot_chunk_buffer
            .take()
            .ok_or_else(|| ExecError::internal("union scan read before open"))?;

        // :253. Refill until a batch survives the dirty-key filter, or the
        // child runs dry.
        let result = loop {
            if let Err(error) = self.child.next(&mut buffer) {
                break Err(error);
            }
            // :255. An EMPTY CHILD CHUNK ends the scan -- not an empty
            // `snapshotRows`, which only means this batch was fully shadowed.
            if buffer.num_rows() == 0 {
                break Ok(None);
            }
            match self.collect_visible_snapshot_rows(&buffer) {
                Err(error) => break Err(error),
                Ok(()) => {
                    if !self.snapshot_rows.is_empty() {
                        break Ok(Some(self.snapshot_rows[0].clone()));
                    }
                }
            }
        };

        self.snapshot_chunk_buffer = Some(buffer);
        result
    }

    /// Go's inner `for row := iter.Begin(); ...` loop (:258-278): the
    /// key-based suppression that IS the dirty-row merge.
    fn collect_visible_snapshot_rows(&mut self, buffer: &Chunk) -> Result<(), ExecError> {
        let child_types = self.child.ret_field_types().to_vec();
        for index in 0..buffer.num_rows() {
            let row = buffer.get_row(index);
            // :261.
            let snapshot_handle = self.compare_exec.handle_cols.build_handle(row)?;
            // :265-271.
            let check_key = if self.phys_tbl_id_idx >= 0 {
                let table_id =
                    row.get_int64(usize::try_from(self.phys_tbl_id_idx).map_err(|_| {
                        ExecError::internal("union scan: negative physical table id index")
                    })?);
                Key::from_bytes(encode_row_key_with_handle(
                    table_id,
                    &snapshot_handle.record_handle(),
                ))
            } else {
                Key::from_bytes(encode_row_key_with_handle(
                    self.table_record_id,
                    &snapshot_handle.record_handle(),
                ))
            };
            // :272-276. Present in the membuffer -> this transaction owns the
            // handle, so the COMMITTED row is dropped.
            if self.mem_buf_snap.get(&check_key)?.is_some() {
                continue;
            }
            // :277.
            self.snapshot_rows.push(row.get_datum_row(&child_types));
        }
        Ok(())
    }

    /// Go `getAddedRow` (:283): one-slot lookahead over `addedRowsIter`.
    fn get_added_row(&mut self) -> Result<Option<Vec<Datum>>, ExecError> {
        if self.cursor4_add_rows.is_none() {
            self.cursor4_add_rows = self
                .added_rows_iter
                .next_row()
                .map_err(from_mem_reader_error)?;
        }
        Ok(self.cursor4_add_rows.clone())
    }

    /// Go's virtual-column block inside `Next` (:160-176).
    fn fill_virtual_columns(&self, mutable_row: &mut MutRow) -> Result<(), ExecError> {
        for &index in &self.virtual_column_index {
            let column = self.meta.schema().columns.get(index).ok_or_else(|| {
                ExecError::internal("union scan: virtual column index outside schema")
            })?;
            // :161. Go's `EvalVirtualColumn` is `VirtualExpr.Eval(ctx, row)`;
            // a nil `VirtualExpr` would panic there, so an absent one is a
            // planning bug and is reported rather than skipped.
            let virtual_expr = column.virtual_expr.as_ref().ok_or_else(|| {
                ExecError::internal("union scan: virtual column has no generating expression")
            })?;
            let datum = virtual_expr.eval(&self.ctx, mutable_row.to_row())?;
            let info = self.columns.get(index).ok_or_else(|| {
                ExecError::internal("union scan: virtual column index outside column list")
            })?;
            // :167. Go passes `returnErr=false, forceIgnoreTruncate=true`.
            let mut cast_datum =
                cast_table_value(datum, &info.field_type, &info.name, &self.stmt, true).map_err(
                    |error| {
                        ExecError::internal(format!("union scan virtual column cast: {error:?}"))
                    },
                )?;
            // :172-174. A NOT NULL (or PreventNullInsert) column that cast to
            // NULL takes its type's zero value instead.
            let flags = info.field_type.flags();
            let not_null = flags & FieldTypeFlags::NOT_NULL != 0
                || flags & FieldTypeFlags::PREVENT_NULL_INSERT != 0;
            if not_null && matches!(cast_datum, Datum::Null) {
                cast_datum = zero_value(&info.field_type);
            }
            // :175.
            mutable_row.set_datum(index, &cast_datum);
        }
        Ok(())
    }
}

impl<C: Columns> Executor for UnionScanExec<C> {
    /// Go `Open` (:75) plus `open` (:85), minus the reader type switch.
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;

        // :101-107. LAST matching column wins, because Go walks backwards and
        // breaks.
        self.phys_tbl_id_idx = -1;
        for (index, column) in self.columns.iter().enumerate().rev() {
            if column.id == tidb_model::column::EXTRA_PHYS_TBL_ID {
                self.phys_tbl_id_idx = i64::try_from(index).map_err(|_| {
                    ExecError::internal("union scan: physical table id column index overflow")
                })?;
                break;
            }
        }

        // :134.
        self.snapshot_chunk_buffer = Some(Chunk::new(
            self.child.ret_field_types(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        ));
        Ok(())
    }

    /// Go `Next` (:139).
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        // :145. Go's own comment: "Assume req.Capacity() > 0 after
        // GrowAndReset(), if this assumption fail, the for-loop may exit
        // without read one single row!"
        req.grow_and_reset(self.meta.max_chunk_size());

        // :147. One scratch row per Next call, over THIS operator's types.
        let mut mutable_row = MutRow::from_types(self.meta.ret_field_types());
        let batch_size = req.capacity();
        while req.num_rows() < batch_size {
            // :149-156.
            let Some(row) = self.get_one_row()? else {
                return Ok(());
            };
            // :157.
            mutable_row.set_datums(&row);
            self.fill_virtual_columns(&mut mutable_row)?;
            // :178-184. A row failing the conditions is skipped, not emitted.
            let (matched, _) = eval_bool(
                &self.ctx,
                &self.conditions_with_vir_col,
                mutable_row.to_row(),
            )?;
            if matched {
                req.append_row(mutable_row.to_row());
            }
        }
        Ok(())
    }

    /// Go `Close` (:190).
    fn close(&mut self) -> Result<(), ExecError> {
        self.cursor4_add_rows = None;
        self.cursor4_snapshot_rows = 0;
        self.snapshot_rows.clear();
        self.added_rows_iter.close();
        self.child.close()
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
    use std::collections::BTreeSet;

    use super::*;
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::column::Column;
    use tidb_expr::NoColumns;

    use crate::mem_reader::DefaultRowsIter;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    /// A two-column `(handle, payload)` schema, which is all these tests need.
    fn meta() -> ExecutorMeta {
        let mut schema = Schema::default();
        schema.columns.push(Column::new(1, long()));
        schema.columns.push(Column::new(2, long()));
        ExecutorMeta::new(schema, 1, 4, 32)
    }

    fn row(handle: i64, payload: i64) -> Vec<Datum> {
        vec![Datum::Int(handle), Datum::Int(payload)]
    }

    /// A child that hands out one prebuilt BATCH per `next` call, then empty
    /// chunks -- the shape `getSnapshotRow`'s refill loop (:253) reacts to.
    struct BatchSource {
        types: Vec<FieldType>,
        batches: Vec<Vec<Vec<Datum>>>,
        cursor: usize,
        closed: bool,
    }

    impl BatchSource {
        fn new(batches: Vec<Vec<Vec<Datum>>>) -> Self {
            BatchSource {
                types: vec![long(), long()],
                batches,
                cursor: 0,
                closed: false,
            }
        }
    }

    impl Executor for BatchSource {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }

        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if self.cursor < self.batches.len() {
                for datums in &self.batches[self.cursor] {
                    req.append_row(MutRow::from_datums(datums).to_row());
                }
                self.cursor += 1;
            }
            Ok(())
        }

        fn close(&mut self) -> Result<(), ExecError> {
            self.closed = true;
            Ok(())
        }

        fn schema(&self) -> &Schema {
            unreachable!("union scan never asks its child for a schema")
        }

        fn ret_field_types(&self) -> &[FieldType] {
            &self.types
        }

        fn init_cap(&self) -> usize {
            4
        }

        fn max_chunk_size(&self) -> usize {
            32
        }

        fn new_chunk(&self) -> Chunk {
            Chunk::new(&self.types, 4, 32)
        }
    }

    /// Column 0 is the handle: Go's `PKIsHandle` shape.
    struct FirstColumnHandle;

    impl HandleColumns for FirstColumnHandle {
        fn build_handle(&self, row: Row<'_>) -> Result<TableHandle, ExecError> {
            Ok(TableHandle::Int(row.get_int64(0)))
        }

        fn compare_handles(
            &self,
            left: &[Datum],
            right: &[Datum],
            _collators: &[Collation],
        ) -> Result<Ordering, ExecError> {
            Ok(left[0]
                .compare(&right[0], Collation::Binary)
                .expect("int handles compare"))
        }
    }

    /// The membuffer keys this transaction has written.
    struct DirtyKeys(BTreeSet<Vec<u8>>);

    impl DirtyKeys {
        fn of(table_id: i64, handles: &[i64]) -> Self {
            DirtyKeys(
                handles
                    .iter()
                    .map(|handle| {
                        encode_row_key_with_handle(
                            table_id,
                            &TableHandle::Int(*handle).record_handle(),
                        )
                    })
                    .collect(),
            )
        }
    }

    impl MemBufferSnapshotGetter for DirtyKeys {
        fn get(&self, key: &Key) -> Result<Option<Vec<u8>>, ExecError> {
            if self.0.contains(key.as_bytes()) {
                // A tombstone is an EMPTY value with NO error, which is what
                // makes a DELETEd committed row vanish (:272-276).
                return Ok(Some(Vec::new()));
            }
            Ok(None)
        }
    }

    fn compare_exec(desc: bool) -> CompareExec {
        CompareExec {
            collators: vec![Collation::Binary, Collation::Binary],
            used_index: vec![0],
            desc,
            need_extra_sorting: false,
            handle_cols: Box::new(FirstColumnHandle),
        }
    }

    struct Case {
        snapshot: Vec<Vec<Vec<Datum>>>,
        added: Vec<Vec<Datum>>,
        dirty: Vec<i64>,
        cache_table: bool,
        desc: bool,
    }

    impl Case {
        fn new(snapshot: Vec<Vec<Vec<Datum>>>, added: Vec<Vec<Datum>>) -> Self {
            Case {
                snapshot,
                added,
                dirty: Vec::new(),
                cache_table: false,
                desc: false,
            }
        }

        /// Drives the operator to exhaustion and returns every emitted row.
        fn run(self) -> Vec<Vec<Datum>> {
            const TABLE_ID: i64 = 77;
            let meta = meta();
            let mut exec = UnionScanExec::new(UnionScanSpec {
                meta: meta.clone(),
                child: Box::new(BatchSource::new(self.snapshot)),
                mem_buf_snap: Box::new(DirtyKeys::of(TABLE_ID, &self.dirty)),
                added_rows_iter: Box::new(DefaultRowsIter::new(self.added)),
                conditions_with_vir_col: Vec::new(),
                columns: vec![
                    UnionScanColumn {
                        id: 1,
                        name: "h".to_owned(),
                        field_type: long(),
                    },
                    UnionScanColumn {
                        id: 2,
                        name: "v".to_owned(),
                        field_type: long(),
                    },
                ],
                table_record_id: TABLE_ID,
                virtual_column_index: Vec::new(),
                cache_table: self.cache_table,
                partition_id_map: BTreeSet::new(),
                keep_order: true,
                compare_exec: compare_exec(self.desc),
                ctx: NoColumns,
                stmt: StmtContext::for_query(),
            });
            exec.open().expect("open");
            let mut emitted = Vec::new();
            loop {
                let mut chunk = meta.new_chunk();
                exec.next(&mut chunk).expect("next");
                if chunk.num_rows() == 0 {
                    break;
                }
                for index in 0..chunk.num_rows() {
                    emitted.push(chunk.get_row(index).get_datum_row(meta.ret_field_types()));
                }
            }
            exec.close().expect("close");
            emitted
        }
    }

    /// `getOneRow` :223 -- `isSnapshotRow := isSnapshotRowInt < 0`. On a TIE
    /// the ADDED row is emitted, and the snapshot row is NOT consumed, so it
    /// is re-compared against the next added row.
    #[test]
    fn a_tie_emits_the_added_row_and_keeps_the_snapshot_row() {
        let emitted = Case::new(vec![vec![row(1, 100)]], vec![row(1, 900), row(2, 901)]).run();
        assert_eq!(
            emitted,
            vec![row(1, 900), row(1, 100), row(2, 901)],
            "added row wins the tie; the snapshot row survives to the next step"
        );
    }

    /// `getSnapshotRow` :272-276 -- a handle present in the membuffer drops
    /// the COMMITTED row. This is where dirty rows shadow, not in `compare`.
    #[test]
    fn a_dirty_key_suppresses_the_committed_row() {
        let mut case = Case::new(
            vec![vec![row(1, 100), row(2, 200), row(3, 300)]],
            vec![row(2, 222)],
        );
        // Handle 2 was updated in this transaction; handle 3 was deleted, so
        // its buffer entry is a tombstone and the added side has nothing.
        case.dirty = vec![2, 3];
        assert_eq!(case.run(), vec![row(1, 100), row(2, 222)]);
    }

    /// `getSnapshotRow` :253-255 -- a batch entirely shadowed does NOT end the
    /// scan; only an EMPTY CHILD CHUNK does.
    #[test]
    fn a_fully_shadowed_batch_does_not_end_the_scan() {
        let mut case = Case::new(
            vec![vec![row(1, 100), row(2, 200)], vec![row(3, 300)]],
            Vec::new(),
        );
        case.dirty = vec![1, 2];
        assert_eq!(case.run(), vec![row(3, 300)]);
    }

    /// `getSnapshotRow` :243-246 -- a cached table has no storage half at all.
    #[test]
    fn a_cache_table_read_never_touches_the_snapshot_side() {
        let mut case = Case::new(vec![vec![row(1, 100)]], vec![row(5, 500)]);
        case.cache_table = true;
        assert_eq!(case.run(), vec![row(5, 500)]);
    }

    /// The merge itself: both sides ascending, one interleaved result.
    #[test]
    fn the_two_sides_interleave_in_compare_order() {
        let emitted = Case::new(
            vec![vec![row(1, 100), row(4, 400)], vec![row(6, 600)]],
            vec![row(2, 200), row(5, 500), row(7, 700)],
        )
        .run();
        assert_eq!(
            emitted,
            vec![
                row(1, 100),
                row(2, 200),
                row(4, 400),
                row(5, 500),
                row(6, 600),
                row(7, 700)
            ]
        );
    }

    /// `desc` negates the answer (:322/:330), so a descending merge needs both
    /// sides descending and emits them descending.
    #[test]
    fn a_desc_merge_interleaves_the_other_way() {
        let mut case = Case::new(
            vec![vec![row(6, 600), row(4, 400)]],
            vec![row(5, 500), row(3, 300)],
        );
        case.desc = true;
        assert_eq!(
            case.run(),
            vec![row(6, 600), row(5, 500), row(4, 400), row(3, 300)]
        );
    }

    /// `compareExec.compare` :310-332 directly: index column first, handle as
    /// the tiebreak, every answer negated under `desc`.
    #[test]
    fn compare_orders_by_used_index_then_handle_and_negates_when_desc() {
        let ascending = CompareExec {
            collators: vec![Collation::Binary, Collation::Binary],
            used_index: vec![1],
            desc: false,
            need_extra_sorting: false,
            handle_cols: Box::new(FirstColumnHandle),
        };
        // Column 1 decides.
        assert_eq!(
            ascending.compare(&row(9, 1), &row(2, 5)).expect("compare"),
            Ordering::Less
        );
        // Column 1 ties, so the handle in column 0 decides.
        assert_eq!(
            ascending.compare(&row(9, 5), &row(2, 5)).expect("compare"),
            Ordering::Greater
        );

        let descending = CompareExec {
            collators: vec![Collation::Binary, Collation::Binary],
            used_index: vec![1],
            desc: true,
            need_extra_sorting: false,
            handle_cols: Box::new(FirstColumnHandle),
        };
        assert_eq!(
            descending.compare(&row(9, 1), &row(2, 5)).expect("compare"),
            Ordering::Greater
        );
        // The HANDLE answer is negated too (:330).
        assert_eq!(
            descending.compare(&row(9, 5), &row(2, 5)).expect("compare"),
            Ordering::Less
        );
    }

    /// `open` :101-107 walks the column list BACKWARDS and breaks, so the LAST
    /// `ExtraPhysTblID` column wins.
    #[test]
    fn open_finds_the_last_extra_phys_tbl_id_column() {
        let meta = meta();
        let mut exec = UnionScanExec::new(UnionScanSpec {
            meta: meta.clone(),
            child: Box::new(BatchSource::new(Vec::new())),
            mem_buf_snap: Box::new(DirtyKeys::of(1, &[])),
            added_rows_iter: Box::new(DefaultRowsIter::new(Vec::new())),
            conditions_with_vir_col: Vec::new(),
            columns: vec![
                UnionScanColumn {
                    id: tidb_model::column::EXTRA_PHYS_TBL_ID,
                    name: "_tidb_tid".to_owned(),
                    field_type: long(),
                },
                UnionScanColumn {
                    id: 2,
                    name: "v".to_owned(),
                    field_type: long(),
                },
                UnionScanColumn {
                    id: tidb_model::column::EXTRA_PHYS_TBL_ID,
                    name: "_tidb_tid".to_owned(),
                    field_type: long(),
                },
            ],
            table_record_id: 1,
            virtual_column_index: Vec::new(),
            cache_table: false,
            partition_id_map: BTreeSet::new(),
            keep_order: false,
            compare_exec: compare_exec(false),
            ctx: NoColumns,
            stmt: StmtContext::for_query(),
        });
        assert_eq!(exec.phys_tbl_id_idx(), -1, "not set before open");
        exec.open().expect("open");
        assert_eq!(exec.phys_tbl_id_idx(), 2);
    }
}
