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
//! ([`TableCommon::row_cursor_with_context`], [`TableCommon::index_range_cursor`]) and decode one
//! row per pull, so the decoded rows alive at once are one chunk's worth
//! regardless of how many rows the range covers, and a pushed `LIMIT` never
//! decodes or looks up a row past its cap.
//!
//! # How far down the streaming reaches today
//!
//! The executors are fully pull-based, and so is the row decoding above the
//! [`TableStorage`](tidb_kv::storage::TableStorage) seam. Below it, neither
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

#[allow(unused_imports)]
use crate::RowDecodeContextExt;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;

use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_driver_error::ExecError;
use tidb_hack::GoCaseCompare;

use tidb_table::tables::TableHandle;
use tidb_table::tables::TableCommon;
use tidb_ranger::types::Range;

/// A locking point read's lock (Go `PointGetExecutor.lock` with its
/// `LockWaitTime`), taken inside `Next`.
#[derive(Clone, Debug)]
pub(crate) struct PointLock {
    /// How long the lock request waits on a held key.
    pub(crate) wait: tidb_txnkv::transaction::LockWaitTime,
    /// Go `IsPessimisticReadConsistency`: lock only existing keys, after
    /// (batch) or while (`lockKeyIfExists`) reading them; repeatable read
    /// locks every named key before reading it.
    pub(crate) read_committed: bool,
    /// The transaction-state publisher, when the session has one.
    pub(crate) lock_state: Option<tidb_sessionctx::select_lock::LockStateObserver>,
    /// Go `TxnCtx.TemporaryTables`, whose keys `doLockKeys` skips.
    pub(crate) temporary_tables: tidb_tblctx::temporary_table::TemporaryTables,
}

impl PointLock {
    /// Go `doLockKeys` for a point read, publishing the lock wait around it.
    pub(crate) fn lock(
        &self,
        store: &mut dyn tidb_kv::storage::TableStorage,
        keys: &[tidb_txnkv::Key],
        only_if_exists: bool,
    ) -> Result<(), ExecError> {
        // Go `doLockKeys`: "Skip the temporary table keys."
        let keys = tidb_tblctx::temporary_table::filter_temporary_table_keys(&self.temporary_tables, keys.to_vec());
        let keys = keys.as_slice();
        if keys.is_empty() {
            return Ok(());
        }
        if let Some(state) = &self.lock_state {
            state.set(true);
        }
        let locked = store
            .lock_keys_for_read(keys, self.wait, only_if_exists)
            .map_err(tidb_table::tables::KvTableError::from);
        if let Some(state) = &self.lock_state {
            state.set(false);
        }
        locked.map_err(ExecError::from)
    }
}


/// What a statement's AST tells the session about its read before planning:
/// a locking read, or an autocommit write whose timestamped transaction can
/// open while it plans. Go's plan-time answer (`AdviseOptimizeWithPlan`'s
/// `MaxUint64` point get) is the optimistic provider's, after planning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatementReadShape {
    /// A locking SELECT needs a current statement snapshot in a pessimistic
    /// transaction, even when an older snapshot would find no rows to lock.
    LockingRead,
    /// Nothing is claimed.
    #[default]
    Unknown,
    /// An autocommit UPDATE or DELETE whose first storage read can overlap
    /// opening its ordinary timestamped snapshot with AST planning; the read
    /// and any eventual prewrite use the same real `start_ts`.
    AutocommitWrite,
}

/// Whether a set operation's terms take a `FOR UPDATE` lock (Go
/// `buildSelectLock` under a `PhysicalUnionAll`).
fn set_opr_has_update_lock(set_opr: &tidb_ast::SetOprStmt) -> bool {
    set_opr.terms.iter().any(|term| match &term.body {
        tidb_ast::SetOprTermBody::Select(select) => select
            .lock
            .as_ref()
            .is_some_and(|lock| lock.kind == tidb_ast::LockKind::Update),
        tidb_ast::SetOprTermBody::Nested(nested) => set_opr_has_update_lock(nested),
    })
}

/// The [`StatementReadShape`] of `stmt`.
#[must_use]
pub fn statement_read_shape(stmt: &tidb_ast::Stmt) -> StatementReadShape {
    if matches!(
        stmt,
        tidb_ast::Stmt::Dml(dml)
            if matches!(
                dml.as_ref(),
                tidb_ast::DmlStmt::Update(_) | tidb_ast::DmlStmt::Delete(_)
            )
    ) {
        return StatementReadShape::AutocommitWrite;
    }
    let tidb_ast::Stmt::Query(query) = stmt else {
        return StatementReadShape::Unknown;
    };
    // A `FOR UPDATE` term makes a set operation a locking read: Go
    // `ExecStmt.isSelectForUpdate` is `b.hasLock`, set by any
    // `buildSelectLock` in the tree.
    let locking = match &**query {
        tidb_ast::QueryStmt::Select(select) => select
            .lock
            .as_ref()
            .is_some_and(|lock| lock.kind == tidb_ast::LockKind::Update),
        tidb_ast::QueryStmt::SetOpr(set_opr) => set_opr_has_update_lock(set_opr),
    };
    if locking {
        StatementReadShape::LockingRead
    } else {
        StatementReadShape::Unknown
    }
}

#[cfg(test)]
mod statement_read_shape_tests {
    use super::*;

    /// DML gets its own ordinary-snapshot shape so the session can start the
    /// PD-backed open before the write read path is built.
    #[test]
    fn update_and_delete_use_the_prefetchable_write_shape() {
        for sql in [
            "UPDATE t SET v = 1 WHERE k = 'x'",
            "DELETE FROM t WHERE k = 'x'",
        ] {
            let stmt = tidb_parser::parse(sql).unwrap();
            assert_eq!(
                statement_read_shape(&stmt),
                StatementReadShape::AutocommitWrite,
                "{sql} should overlap its normal snapshot open with planning"
            );
        }
        let insert = tidb_parser::parse("INSERT INTO t VALUES (1, 2)").unwrap();
        assert_eq!(
            statement_read_shape(&insert),
            StatementReadShape::Unknown,
            "INSERT has no guaranteed first read to prefetch"
        );
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

/// The visible columns of a decoded table row, the ones a source emits.
///
/// `TableCommon::get_row_by_handle` decodes and materializes EVERY column of the
/// table, hidden ones included -- the hidden column an expression index was
/// rewritten into has to be computed so an index entry can be written from
/// the same row. The default output schema of these sources is the visible
/// one; an explicit physical output mapping may also request hidden columns
/// and uses the full row instead. A hidden column sits wherever Go's DDL put
/// it in `TableInfo.Columns`, so a table that has one gathers the rest.
///
/// `TableScanExec` states the same rule as its `keep` list; this is that rule
/// for the two narrowed sources, which read their rows by handle instead.
fn visible_of<'a>(
    table: &TableCommon,
    row: &'a [tidb_datatype::Datum],
) -> std::borrow::Cow<'a, [tidb_datatype::Datum]> {
    if table.visible_column_count() == table.columns().len() {
        return std::borrow::Cow::Borrowed(&row[..row.len().min(table.columns().len())]);
    }
    std::borrow::Cow::Owned(
        (0..row.len())
            .filter(|offset| !table.is_hidden(*offset))
            .map(|offset| row[offset].clone())
            .collect(),
    )
}


/// Go `rebuildIndexRanges` / `PhysicalTableScan.ResolveCorrelatedColumns`:
/// recomputes a scan's ranges from access conditions over correlated columns
/// each time an Apply reopens it for a new outer row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HandleOutputColumn {
    /// One stored table-column offset.
    Stored(usize),
    /// Go's synthetic `_tidb_rowid`, read from the record key.
    ExtraHandle,
    /// The physical record keyspace retained alongside each handle.
    PhysicalTableId,
    /// Go's `ExtraRowChecksumID` column of a fast point plan
    /// (`fillRowChecksum`), computed from the encoded row value.
    RowChecksum,
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
    table: TableCommon,
    /// The snapshot this reader reads, apart from the table metadata (Go's
    /// executor reads through the session transaction, never the table).
    store: Box<dyn tidb_kv::storage::TableStorage>,
    handles: Vec<TableHandle>,
    /// The next handle to read.
    cursor: usize,
    /// Rows produced so far, which the trace reads as this node's `actRows`.
    produced: tidb_executor_internal_exec::RowCount,
    /// The statement-class flags for an origin default plus the session zone
    /// a stored `TIMESTAMP` is read back into, captured where the statement's
    /// context is (`Executor` has none).
    decode_context: tidb_table::tables::RowDecodeContext,
    /// Source-row offsets this complete point plan emits, in result order.
    /// `None` keeps the ordinary visible-row schema for residual root work.
    output_columns: Option<Vec<HandleOutputColumn>>,
    /// Encoded values fetched once by the first Next, then decoded only to
    /// fill the requested chunk, like Go's BatchPointGetExec.values.
    preloaded: Option<tidb_table::tables::PointReadValues>,
    decoder: Option<tidb_table::tables::PointRowDecoder>,
    initialized: bool,
    /// Go's `PointGetExecutor` performs one direct record-key `Get`; the
    /// batch executor uses `BatchGet` even when its retained list happens to
    /// contain one handle.
    single_point_get: bool,
    /// One physical partition per handle, retained by the fast batch plan.
    partition_ids: Option<Vec<i64>>,
    /// Where `_tidb_rowid` sits in the source row, when the schema carries
    /// it. Go's extra handle column reports the record HANDLE rather than any
    /// stored value, so nothing in the decoded row fills this slot and the
    /// source has to write it -- the same contract
    /// [`crate::kv_table::table_scan`] has for a scan.
    extra_handle_slot: Option<usize>,
    /// Go `PointGetExecutor.lock` / `BatchPointGetExec.lock` in a
    /// repeatable-read pessimistic transaction: the record keys are locked
    /// with this wait before the first read, inside `Next`.
    lock: Option<PointLock>,
    /// The unique-index key each handle was found under, aligned with
    /// `handles`, which a batch read locks together with its record keys
    /// (Go `BatchPointGetExec.initialize`'s `indexKeys`).
    lock_index_keys: Vec<Option<tidb_txnkv::Key>>,
    /// Index keys that named no row, which repeatable read locks with the
    /// rest before reading (Go `BatchPointGetExec`: "lock all keys in
    /// repeatable read isolation").
    lock_missing_index_keys: Vec<tidb_txnkv::Key>,
}

impl HandleSourceExec {
    /// Builds a source over `handles` with an explicit row-decode context.
    #[must_use]
    pub fn new_with_context(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        handles: Vec<TableHandle>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        HandleSourceExec {
            meta,
            store,
            table,
            handles,
            cursor: 0,
            preloaded: None,
            decoder: None,
            initialized: false,
            produced: tidb_executor_internal_exec::RowCount::default(),
            decode_context,
            output_columns: None,
            extra_handle_slot: None,
            single_point_get: false,
            partition_ids: None,
            lock: None,
            lock_index_keys: Vec::new(),
            lock_missing_index_keys: Vec::new(),
        }
    }

    /// Locks the record keys before reading them (see
    /// [`HandleSourceExec::lock`]).
    pub(crate) fn with_lock_before_read(
        mut self,
        lock: Option<PointLock>,
    ) -> Self {
        self.lock = lock;
        self
    }

    /// The unique-index keys the handles were found under (see
    /// [`HandleSourceExec::lock_index_keys`]).
    pub(crate) fn with_lock_index_keys(
        mut self,
        keys: Vec<Option<tidb_txnkv::Key>>,
        missing: Vec<tidb_txnkv::Key>,
    ) -> Self {
        self.lock_index_keys = keys;
        self.lock_missing_index_keys = missing;
        self
    }

    /// The record and index keys of the handles at `positions`.
    fn lock_keys_at(
        &self,
        record_keys: &[tidb_txnkv::Key],
        positions: &[usize],
    ) -> Vec<tidb_txnkv::Key> {
        let mut keys = positions
            .iter()
            .filter_map(|position| record_keys.get(*position).cloned())
            .collect::<Vec<_>>();
        keys.extend(
            positions
                .iter()
                .filter_map(|position| self.lock_index_keys.get(*position).cloned().flatten()),
        );
        keys
    }

    /// Builds the final table reader for a retained physical plan whose
    /// output can mix stored columns and Go's synthetic `_tidb_rowid`.
    #[must_use]
    pub(crate) fn new_mapped_with_context(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        handles: Vec<TableHandle>,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        Self {
            output_columns: Some(output_columns),
            ..Self::new_with_context(meta, table, store, handles, decode_context)
        }
    }

    /// Builds Go's `PointGetExecutor`: one retained handle, one direct row
    /// `Get`, and the physical plan's exact output projection.
    #[must_use]
    pub(crate) fn new_point_mapped_with_context(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        handle: TableHandle,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        Self {
            single_point_get: true,
            ..Self::new_mapped_with_context(meta, table, store, vec![handle], output_columns, decode_context)
        }
    }

    pub(crate) fn with_partition_ids(mut self, ids: Option<Vec<i64>>) -> Self {
        self.partition_ids = ids;
        self
    }

    /// The live count of rows this source produced.
    #[must_use]
    pub fn produced_rows(&self) -> tidb_executor_internal_exec::RowCount {
        self.produced.clone()
    }

    fn initialize(&mut self) -> Result<(), ExecError> {
        // Go `PointGetExecutor.getAndLock` / `BatchPointGetExec.initialize`:
        // "Lock the key before get in RR isolation, then get will get the
        // value from the cache"; read committed's single point get locks the
        // key only if it exists (`lockKeyIfExists`) and reads the answer.
        let record_keys = self.lock.as_ref().and_then(|_| {
            self.table
                .record_lock_keys(&self.handles, self.partition_ids.as_deref())
        });
        let mut locked_before_read = false;
        if let (Some(lock), Some(record_keys)) = (self.lock.clone(), record_keys.as_ref()) {
            if !lock.read_committed {
                let positions = (0..self.handles.len()).collect::<Vec<_>>();
                let mut keys = self.lock_keys_at(record_keys, &positions);
                keys.extend(self.lock_missing_index_keys.iter().cloned());
                lock.lock(&mut *self.store, &keys, false)?;
                locked_before_read = true;
            } else if self.single_point_get {
                lock.lock(&mut *self.store, record_keys, true)?;
                locked_before_read = true;
            }
        }
        // Go's `BatchPointGetExec.initialize` reads every handle with ONE
        // `BatchGet` on the first `Next`. Prefetch all physical keys
        // together so the storage layer can schedule regions concurrently.
        let rows = if self.single_point_get {
            let handle = self
                .handles
                .first()
                .ok_or_else(|| ExecError::unsupported("a point get lost its retained handle"))?;
            tidb_table::tables::PointReadValues::Single(
                self.table
                    .stored_record(&mut *self.store, handle)
                    .map_err(ExecError::from)?
                    .map(|(_, bytes)| bytes),
            )
        } else {
            if self
                .partition_ids
                .as_ref()
                .is_some_and(|ids| ids.len() != self.handles.len())
            {
                return Err(ExecError::unsupported(
                    "batch point partition routes lost alignment",
                ));
            }
            self.table
                .stored_records_batched(&mut *self.store, &self.handles, self.partition_ids.as_deref())
                .map_err(ExecError::from)?
        };
        // Counted only on an IndexMerge reader's table side, whose copy
        // carries the statement budget; a point get's copy does not.
        let found = match &rows {
            tidb_table::tables::PointReadValues::Single(row) => usize::from(row.is_some()),
            tidb_table::tables::PointReadValues::Batch { values, .. } => values.len(),
        };
        self.table
            .note_keys_read(found as u64)
            .map_err(ExecError::from)?;
        // Go `BatchPointGetExec.initialize` under read committed: "Lock
        // exists keys only", the found rows and their index keys. A
        // partitioned read whose handles were not routed to one partition
        // could name no key before reading and locks what it found.
        if let Some(lock) = self.lock.clone().filter(|_| !locked_before_read) {
            let (record_keys, positions): (Vec<tidb_txnkv::Key>, Vec<usize>) = match &rows {
                tidb_table::tables::PointReadValues::Single(Some(_)) => {
                    match record_keys.as_ref().and_then(|keys| keys.first()) {
                        Some(key) => (vec![key.clone()], vec![0]),
                        None => (Vec::new(), Vec::new()),
                    }
                }
                tidb_table::tables::PointReadValues::Single(None) => (Vec::new(), Vec::new()),
                tidb_table::tables::PointReadValues::Batch { keys, .. } => {
                    let mut found_keys = vec![tidb_txnkv::Key::default(); keys.len()];
                    let mut positions = Vec::new();
                    for (position, key) in keys.iter().enumerate() {
                        if let Some(key) = key {
                            found_keys[position] = key.clone();
                            positions.push(position);
                        }
                    }
                    (found_keys, positions)
                }
            };
            let keys = self.lock_keys_at(&record_keys, &positions);
            lock.lock(&mut *self.store, &keys, false)?;
        }
        self.preloaded = Some(rows);
        self.decoder = Some(tidb_table::tables::PointRowDecoder::new(&self.table));
        Ok(())
    }
}

impl HandleSourceExec {
    fn append_handle_row(
        &self,
        req: &mut Chunk,
        index: usize,
        handle: &TableHandle,
        row: &[Datum],
        stored_value: Option<&[u8]>,
    ) -> Result<(), ExecError> {
        let visible = visible_of(&self.table, row);
        if let Some(columns) = &self.output_columns {
            for (output, source) in columns.iter().copied().enumerate() {
                match source {
                    HandleOutputColumn::Stored(source) => {
                        // The physical schema may request a hidden
                        // generated column for an UPDATE/index key.
                        // Stored offsets address the full decoded row,
                        // not the wildcard's visible-column prefix.
                        let value = row.get(source).ok_or_else(|| {
                            ExecError::unsupported("point-get output column is outside the row")
                        })?;
                        req.append_datum(output, value);
                    }
                    HandleOutputColumn::ExtraHandle => match handle {
                        TableHandle::Int(value) => {
                            req.append_datum(output, &Datum::Int(*value));
                        }
                        _ => {
                            return Err(ExecError::unsupported(
                                "an extra row handle is not an integer handle",
                            ));
                        }
                    },
                    HandleOutputColumn::PhysicalTableId => {
                        let physical_id = self
                            .partition_ids
                            .as_ref()
                            .and_then(|ids| ids.get(index))
                            .copied()
                            .or_else(|| match self.table.record_physical_ids().as_slice() {
                                [id] => Some(*id),
                                _ => None,
                            })
                            .ok_or_else(|| {
                                ExecError::internal(
                                    "physical-table ID output lost its record partition",
                                )
                            })?;
                        req.append_int64(output, physical_id);
                    }
                    HandleOutputColumn::RowChecksum => {
                        let stored_value = stored_value.ok_or_else(|| {
                            ExecError::unsupported(
                                "a row checksum needs the encoded row value of a point get",
                            )
                        })?;
                        let checksum = tidb_table::tables::fill_row_checksum(
                            &self.table,
                            handle,
                            stored_value,
                            &self.decode_context,
                        )
                        .map_err(ExecError::from)?;
                        req.append_datum(output, &checksum);
                    }
                }
            }
        } else {
            let owned;
            let visible: &[tidb_datatype::Datum] = match self.extra_handle_slot {
                Some(slot) => {
                    owned = crate::kv_table::insert_extra_handle(visible.to_vec(), slot, handle);
                    &owned
                }
                None => &visible,
            };
            for (column, value) in visible.iter().enumerate() {
                req.append_datum(column, value);
            }
        }
        Ok(())
    }
}

impl Executor for HandleSourceExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.cursor = 0;
        self.produced.set(0);
        self.preloaded = None;
        self.decoder = None;
        self.initialized = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !std::mem::replace(&mut self.initialized, true) {
            self.initialize()?;
        }
        while !req.is_full() {
            if self.cursor >= self.handles.len() {
                return Ok(());
            };
            let index = self.cursor;
            let handle = self.handles.get(index);
            // PointGet marks done before its read; BatchPointGet advances
            // only after successfully decoding the current stored value.
            if self.single_point_get {
                self.cursor += 1;
            }
            // A handle with no row is Go's point get that finds nothing: the
            // plan is right, the row is simply absent.
            let value = self.preloaded.as_ref().and_then(|values| values.get(index));
            if let Some(value) = value {
                let row = self
                    .decoder
                    .as_mut()
                    .expect("initialized point reader")
                    .decode(
                        &self.table,
                        handle.expect("cursor indexes handles"),
                        value,
                        &self.decode_context,
                    )
                    .map_err(ExecError::from)?;
                self.append_handle_row(
                    req,
                    index,
                    handle.expect("cursor indexes handles"),
                    &row,
                    Some(value),
                )?;
                self.produced.set(self.produced.get() + 1);
            }
            if !self.single_point_get {
                self.cursor += 1;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.preloaded = None;
        self.decoder = None;
        self.initialized = true;
        self.cursor = self.handles.len();
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

    fn table_access(&mut self) -> Option<&mut dyn tidb_executor_internal_exec::table_access::TableAccess> {
        Some(self)
    }
}

/// This source refuses every offer, on purpose, by taking the fail-closed
/// defaults of [`tidb_executor_internal_exec::table_access`] wholesale.
///
/// It is listed as an access path rather than left unnegotiable so the refusal
/// is a stated position instead of an omission: the handle list is fixed by the
/// statement text, so a cap would duplicate the `LimitExec` above (Go likewise
/// keeps `Limit_10 | root` over `Batch_Point_Get_12`), the `WHERE` that pinned
/// the handles is still applied above by design (`TryFastPlan` narrows the
/// source, it does not replace the filter), and there is no scanned-row count
/// to report because nothing is scanned.
impl tidb_executor_internal_exec::table_access::TableAccess for HandleSourceExec {}

/// Go's secondary-unique PointGet/BatchPointGet first resolves retained index
/// keys to record handles, then delegates the record reads to
/// [`HandleSourceExec`]. The single form uses two direct Gets; the batch form
/// uses one index BatchGet followed by one record BatchGet.
pub(crate) struct UniqueIndexPointSourceExec {
    meta: ExecutorMeta,
    table: TableCommon,
    /// The snapshot this reader reads, apart from the table metadata (Go's
    /// executor reads through the session transaction, never the table).
    store: Box<dyn tidb_kv::storage::TableStorage>,
    index_id: i64,
    index_values: Vec<Vec<Datum>>,
    output_columns: Vec<HandleOutputColumn>,
    decode_context: tidb_table::tables::RowDecodeContext,
    single_point: bool,
    /// Go `PartitionNames` resolved to physical ids: a global index entry
    /// whose row lives elsewhere answers nothing. `None` restricts nothing.
    allowed_partitions: Option<Vec<i64>>,
    /// Go `PointGetExecutor.lock` / `BatchPointGetExec.lock`.
    lock: Option<PointLock>,
    source: Option<HandleSourceExec>,
    initialized: bool,
}

impl UniqueIndexPointSourceExec {
    /// Restricts a global index's rows to the PARTITION clause's ids.
    #[must_use]
    pub(crate) fn with_allowed_partitions(mut self, ids: Option<Vec<i64>>) -> Self {
        self.allowed_partitions = ids;
        self
    }

    /// Locks the index and record keys this read names inside `Next`.
    #[must_use]
    pub(crate) fn with_lock(mut self, lock: Option<PointLock>) -> Self {
        self.lock = lock;
        self
    }

    #[must_use]
    pub(crate) fn new(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        index_id: i64,
        index_values: Vec<Vec<Datum>>,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
        single_point: bool,
    ) -> Self {
        Self {
            meta,
            store,
            table,
            index_id,
            index_values,
            output_columns,
            decode_context,
            single_point,
            allowed_partitions: None,
            lock: None,
            source: None,
            initialized: false,
        }
    }
}

impl UniqueIndexPointSourceExec {
    /// Go `matchPartitionNames(pid, partitionNames, pi)` for a global index
    /// entry; a local entry carries no partition and is already confined.
    fn partition_allowed(&self, partition: Option<i64>) -> bool {
        match (&self.allowed_partitions, partition) {
            (Some(allowed), Some(id)) => allowed.contains(&id),
            _ => true,
        }
    }

    fn initialize(&mut self) -> Result<(), ExecError> {
        // Go isCommonHandleRead: clustered PRIMARY has metadata but no
        // separate index entries. Its values directly encode record handles.
        let common_primary = !self.table.common_handle_offsets().is_empty()
            && self
                .table
                .plan_indexes()
                .any(|index| index.read().id == self.index_id && index.clustered_primary);
        let mut routed_partitions: Option<Vec<i64>> = None;
        let mut index_keys: Vec<Option<tidb_txnkv::Key>> = Vec::new();
        let mut missing_index_keys: Vec<tidb_txnkv::Key> = Vec::new();
        let mut handles: Vec<TableHandle> = if common_primary {
            self.index_values
                .iter()
                .filter(|values| !values.iter().any(Datum::is_null))
                .map(|values| {
                    self.table
                        .common_handle_from_values(values, self.decode_context.zone())
                        .map_err(|error| {
                            ExecError::unsupported(format!(
                                "common primary key encoding failed: {error:?}"
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else if self.single_point {
            let [values] = self.index_values.as_slice() else {
                return Err(ExecError::unsupported(
                    "a unique-index PointGet does not retain exactly one key",
                ));
            };
            // Go `PointGetExecutor.Next`: repeatable read locks the index
            // key whether or not it exists, then reads it from the lock
            // cache; read committed locks it only if it exists
            // (`lockKeyIfExists`).
            if let Some(lock) = self.lock.clone() {
                let keys = self
                    .table
                    .encode_unique_index_key(self.index_id, values, self.decode_context.zone())
                    .map_err(|error| {
                        ExecError::unsupported(format!(
                            "unique index key encoding failed: {error:?}"
                        ))
                    })?;
                lock.lock(&mut *self.store, &keys, lock.read_committed)?;
            }
            self.table
                .lookup_unique_with_partition(
                    &mut *self.store,
                    self.index_id,
                    values,
                    self.decode_context.zone(),
                )
                .map_err(|error| {
                    ExecError::unsupported(format!("unique index lookup failed: {error:?}"))
                })?
                .into_iter()
                .filter(|(_, partition)| self.partition_allowed(*partition))
                .map(|(handle, _)| handle)
                .collect()
        } else {
            let keys = self
                .table
                .unique_batch_index_keys(
                    self.index_id,
                    &self.index_values,
                    self.decode_context.zone(),
                    Some(self.decode_context.expression()),
                )
                .map_err(|error| {
                    ExecError::unsupported(format!("unique index batch lookup failed: {error:?}"))
                })?;
            let looked_up = self
                .table
                .lookup_unique_index_keys(&mut *self.store, &keys)
                .map_err(|error| {
                    ExecError::unsupported(format!("unique index batch lookup failed: {error:?}"))
                })?;
            let mut found = Vec::new();
            for (key, entry) in keys.into_iter().zip(looked_up) {
                let key = key.map(|(key, _)| key);
                match entry {
                    Some((handle, partition)) if self.partition_allowed(partition) => {
                        found.push((handle, partition));
                        index_keys.push(key);
                    }
                    _ => missing_index_keys.extend(key),
                }
            }
            // Each row reads from the partition its key routed to (or its
            // global entry recorded), as Go's second BatchGet does.
            if self.table.partition().is_some()
                && found.iter().all(|(_, partition)| partition.is_some())
            {
                routed_partitions = Some(found.iter().filter_map(|(_, id)| *id).collect());
            }
            found.into_iter().map(|(handle, _)| handle).collect()
        };
        let Some(first) = handles.first().cloned() else {
            // Go `BatchPointGetExec.initialize` with no row found still
            // locks every named index key under repeatable read.
            if let Some(lock) = self.lock.clone().filter(|lock| !lock.read_committed) {
                lock.lock(&mut *self.store, &missing_index_keys, false)?;
            }
            return Ok(());
        };
        let meta = ExecutorMeta::new(
            self.meta.schema().clone(),
            self.meta.id(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        )
        .with_alloc_pool(self.meta.alloc_pool().cloned());
        let mut source = if self.single_point {
            HandleSourceExec::new_point_mapped_with_context(
                meta,
                self.table.clone(),
                self.store.clone(),
                first,
                self.output_columns.clone(),
                self.decode_context.clone(),
            )
            .with_lock_before_read(self.lock.clone())
        } else {
            HandleSourceExec::new_mapped_with_context(
                meta,
                self.table.clone(),
                self.store.clone(),
                std::mem::take(&mut handles),
                self.output_columns.clone(),
                self.decode_context.clone(),
            )
            .with_partition_ids(routed_partitions)
            .with_lock_before_read(self.lock.clone())
            .with_lock_index_keys(index_keys, missing_index_keys)
        };
        source.open()?;
        self.source = Some(source);
        Ok(())
    }
}

impl Executor for UniqueIndexPointSourceExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.source = None;
        self.initialized = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !std::mem::replace(&mut self.initialized, true) {
            self.initialize()?;
        }
        match self.source.as_mut() {
            Some(source) => source.next(req),
            None => Ok(()),
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.initialized = true;
        if let Some(source) = self.source.as_mut() {
            source.close()?;
        }
        self.source = None;
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


pub(crate) type RangeRebuilder =
    std::sync::Arc<dyn Fn() -> Result<Vec<Range>, ExecError> + Send + Sync>;


pub(crate) fn calculate_lookup_batch_size_with_cap(
    estimated_rows: Option<f64>,
    initial_batch_size: usize,
    index_paging: bool,
    max_batch_size: usize,
) -> usize {
    let max_batch_size = max_batch_size.max(1);
    let mut batch_size = initial_batch_size.clamp(1, max_batch_size);
    if index_paging {
        return batch_size;
    }
    let estimated_rows = estimated_rows
        .filter(|rows| rows.is_finite() && *rows > 0.0)
        .map_or(0, |rows| rows as usize);
    if estimated_rows >= max_batch_size {
        return max_batch_size;
    }
    while batch_size < estimated_rows {
        batch_size = batch_size.saturating_mul(2);
        if batch_size >= max_batch_size {
            return max_batch_size;
        }
    }
    batch_size
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
    // Values at this column in each ranger template. Alternative columns
    // share the same ordinal, preserving correlated static range tuples.
    Alternatives(Vec<Datum>),
}
