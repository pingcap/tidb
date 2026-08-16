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

//! SEED of Go `pkg/table/tblctx`.
//!
//! `buffers.go` lands whole: `MutateBuffers` and the two row buffers it
//! hands out (`EncodeRowBuffer`, `CheckRowBuffer`), their reserve/reset
//! capacity contract, and both row-encoding paths (`WriteMemBufferEncoded`
//! into a mem buffer, `EncodeBinlogRowData` into a caller-owned slice).
//!
//! `table.go` lands only as far as the Rust workspace is dependency-closed.
//! The mutate-context interface surface is here, but three of its members
//! reach types that have no Rust owner yet and are named as deferrals in
//! [`MutateContext`]: `variable.RowIDShardGenerator`,
//! `stmtctx.ReservedRowIDAlloc`, and the whole `AllocatorContext` /
//! `autoid.Allocators` axis. Every remaining gap is a narrow local trait or
//! enum carrying a `// boundary:` comment that names the Go interface it
//! stands in for.
//!
//! The session-side buffers this package pools live in
//! [`crate::write_stmt_bufs`]; Go reaches them through a `*WriteStmtBufs`
//! pointer shared with the session, which is an `Rc<RefCell<..>>` here.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use tidb_chunk::mutrow::MutRow;
use tidb_codec::{Handle as ChecksumHandle, RowChecksumPolicy};
use tidb_datatype::{Datum, SessionTimeZone};
use tidb_error::errctx::{Context as ErrCtx, SharedError};
use tidb_expr::metabuild::{ExprContext, MetaOnlyInfoSchema};
use tidb_model::TableInfo;
use tidb_tablecodec::{encode_old_table_row, encode_table_row};
use tidb_txnkv::{FlagsOp, Getter, Handle, Key, MemBuffer as KvMemBuffer, Mutator};

use crate::write_stmt_bufs::WriteStmtBufs;

/// Go `ensureCapacityAndReset`: `make()` that reuses `slice` when it already
/// has enough capacity.
///
/// Go returns a reslice; Rust cannot hand back a second view of one `Vec`'s
/// allocation, so this resets in place. The observable contract is the same:
/// after the call the vector has length `size`, and its capacity is at least
/// `opt_cap.unwrap_or(size)` and never shrinks below what it already had.
///
/// The one Go behavior with no Rust counterpart is `slice[:size]` re-exposing
/// stale elements past the old length. Every caller here uses the grown region
/// as write-only scratch, so this fills it with `T::default()` instead of
/// resurrecting old values.
pub fn ensure_capacity_and_reset<T: Default>(
    slice: &mut Vec<T>,
    size: usize,
    opt_cap: Option<usize>,
) {
    let capacity = opt_cap.unwrap_or(size);
    if slice.capacity() < capacity {
        // Go's `make([]T, size, capacity)` abandons the old array.
        let mut fresh = Vec::with_capacity(capacity);
        fresh.resize_with(size, T::default);
        *slice = fresh;
        return;
    }
    // Go's `slice[:size]`: same allocation, new length.
    slice.truncate(size);
    slice.resize_with(size, T::default);
}

/// boundary: Go `pkg/util/rowcodec.Encoder`.
///
/// `tidb-codec` transcreated the row-v2 encoder as free functions
/// (`encode_row`, `encode_row_with_checksum`) rather than a struct with
/// reusable scratch fields, so the only part of Go's `*rowcodec.Encoder` that
/// `tblctx` reads -- `Enable`, the new-format switch -- is carried here.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowEncoder {
    /// Go `rowcodec.Encoder.Enable`: encode with the new row format.
    pub enable: bool,
}

/// Go `RowEncodingConfig` (`table.go:31`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowEncodingConfig {
    /// Go `IsRowLevelChecksumEnabled`.
    pub is_row_level_checksum_enabled: bool,
    /// Go `RowEncoder`.
    pub row_encoder: RowEncoder,
}

/// boundary: Go `pkg/kv.MemBuffer`, narrowed to the two writes `tblctx` makes.
///
/// `tidb_txnkv::MemBuffer` is the real port, but it carries five associated
/// types and a staging/snapshot surface that a row encoder has no business
/// naming -- Go's own `buffers_test.go` dodges the same breadth by embedding a
/// nil `kv.MemBuffer` in its mock. [`TxnMemBuffer`] adapts the real trait onto
/// this one.
pub trait MutateMemBuffer {
    /// Mutation error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Go `MemBuffer.Set`.
    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), Self::Error>;
    /// Go `MemBuffer.SetWithFlags`.
    fn set_with_flags(
        &mut self,
        key: Key,
        value: Vec<u8>,
        flags: &[FlagsOp],
    ) -> Result<(), Self::Error>;
}

/// Adapts a real [`tidb_txnkv::MemBuffer`] onto [`MutateMemBuffer`].
///
/// A blanket impl would be the natural shape, but it would collide with any
/// local implementation of [`MutateMemBuffer`], so the bridge is a newtype.
pub struct TxnMemBuffer<'a, M>(pub &'a mut M);

impl<M> MutateMemBuffer for TxnMemBuffer<'_, M>
where
    M: KvMemBuffer,
    <M as Getter>::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = <M as Getter>::Error;

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), Self::Error> {
        Mutator::set(self.0, key, value)
    }

    fn set_with_flags(
        &mut self,
        key: Key,
        value: Vec<u8>,
        flags: &[FlagsOp],
    ) -> Result<(), Self::Error> {
        KvMemBuffer::set_with_flags(self.0, key, value, flags)
    }
}

/// Go `EncodeRowBuffer` (`buffers.go:33`): the scratch a single row is
/// accumulated into before it is encoded.
#[derive(Debug)]
pub struct EncodeRowBuffer {
    /// Go `colIDs`.
    col_ids: Vec<i64>,
    /// Go `row`.
    row: Vec<Datum>,
    /// Go `writeStmtBufs`, which refs the session's `WriteStmtBufs`.
    write_stmt_bufs: Rc<RefCell<WriteStmtBufs>>,
}

impl EncodeRowBuffer {
    /// Go `EncodeRowBuffer.Reset`.
    pub fn reset(&mut self, capacity: usize) {
        ensure_capacity_and_reset(&mut self.col_ids, 0, Some(capacity));
        ensure_capacity_and_reset(&mut self.row, 0, Some(capacity));
    }

    /// Go `EncodeRowBuffer.AddColVal`.
    pub fn add_col_val(&mut self, col_id: i64, val: Datum) {
        self.col_ids.push(col_id);
        self.row.push(val);
    }

    /// Go `EncodeRowBuffer.colIDs`, for callers that inspect the staged row.
    #[must_use]
    pub fn col_ids(&self) -> &[i64] {
        &self.col_ids
    }

    /// Go `EncodeRowBuffer.row`, for callers that inspect the staged row.
    #[must_use]
    pub fn row(&self) -> &[Datum] {
        &self.row
    }

    /// Go `EncodeRowBuffer.writeStmtBufs`.
    #[must_use]
    pub fn write_stmt_bufs(&self) -> &Rc<RefCell<WriteStmtBufs>> {
        &self.write_stmt_bufs
    }

    /// Go `EncodeRowBuffer.WriteMemBufferEncoded`.
    ///
    /// Go threads a `codec.Encoder` through so `tablecodec` knows whether
    /// collation is enabled; `tidb-tablecodec`'s transcreation resolves that
    /// from the collation registry instead, so the parameter has no Rust
    /// counterpart and is dropped.
    // Go's method takes the same operands; splitting them into a struct would
    // put a shape between this port and its source for no behavioral gain.
    #[allow(clippy::too_many_arguments)]
    pub fn write_mem_buffer_encoded<M: MutateMemBuffer>(
        &self,
        cfg: RowEncodingConfig,
        loc: Option<&SessionTimeZone>,
        ec: &ErrCtx,
        mem_buffer: &mut M,
        key: Key,
        handle: &Handle,
        flags: &[FlagsOp],
    ) -> Result<(), SharedError> {
        let checksum = cfg
            .is_row_level_checksum_enabled
            .then(|| RowChecksumPolicy::RawHandle(raw_checksum_handle(handle)));

        let mut stmt_bufs = self.write_stmt_bufs.borrow_mut();

        // Adjust `AddRowValues` length. It holds the inserting values used by
        // `tablecodec.EncodeOldRow`, whose row format is
        // `id1, colval, id2, colval`, so the correct length is `rowLen * 2`.
        // A null value makes `AddRecord` skip a column, so the row length
        // varies between calls and has to be re-adjusted each time.
        ensure_capacity_and_reset(&mut stmt_bufs.add_row_values, self.row.len() * 2, None);

        let encoded = match encode_table_row(
            loc,
            &self.row,
            &self.col_ids,
            cfg.row_encoder.enable,
            checksum.as_ref(),
        ) {
            Ok(encoded) => encoded,
            Err(err) => {
                let shared: SharedError = Arc::new(err);
                if let Some(err) = ec.handle_error(Some(shared)) {
                    return Err(err);
                }
                Vec::new()
            }
        };

        // Go assigns the encoder's result back into `RowValBuf`, which reuses
        // the buffer it was handed. `tidb-tablecodec` returns a fresh `Vec`,
        // so the reuse is expressed as clear-then-extend: the observable
        // contract (`RowValBuf` holds the encoded row, and its capacity never
        // shrinks across statements) is preserved.
        stmt_bufs.row_val_buf.clear();
        stmt_bufs.row_val_buf.extend_from_slice(&encoded);
        let value = stmt_bufs.row_val_buf.clone();
        drop(stmt_bufs);

        if flags.is_empty() {
            mem_buffer.set(key, value).map_err(shared_error)
        } else {
            mem_buffer
                .set_with_flags(key, value, flags)
                .map_err(shared_error)
        }
    }

    /// Go `EncodeRowBuffer.EncodeBinlogRowData`.
    ///
    /// The returned bytes are not referenced by any inner buffer, so callers
    /// may cache and modify them freely.
    pub fn encode_binlog_row_data(
        &self,
        loc: Option<&SessionTimeZone>,
        ec: &ErrCtx,
    ) -> Result<Vec<u8>, SharedError> {
        match encode_old_table_row(loc, &self.row, &self.col_ids) {
            Ok(value) => Ok(value),
            Err(err) => {
                let shared: SharedError = Arc::new(err);
                match ec.handle_error(Some(shared)) {
                    Some(err) => Err(err),
                    None => Ok(Vec::new()),
                }
            }
        }
    }
}

fn shared_error<E: std::error::Error + Send + Sync + 'static>(err: E) -> SharedError {
    Arc::new(err)
}

/// Go `rowcodec.RawChecksum{Handle: handle}` feeds `handle.Encoded()` into the
/// CRC. `tidb-codec`'s `Handle::Common` concatenates its parts, so a
/// single-part `Common` is the byte-exact carrier for any handle kind --
/// including a partition handle, whose `Encoded` delegates to its inner
/// handle just as Go's does.
fn raw_checksum_handle(handle: &Handle) -> ChecksumHandle {
    ChecksumHandle::Common(vec![handle.encoded()])
}

/// Go `CheckRowBuffer` (`buffers.go:101`): the row staged for constraint
/// checks.
#[derive(Clone, Debug, Default)]
pub struct CheckRowBuffer {
    /// Go `rowToCheck`.
    row_to_check: Vec<Datum>,
}

impl CheckRowBuffer {
    /// Go `CheckRowBuffer.GetRowToCheck`.
    #[must_use]
    pub fn get_row_to_check(&self) -> MutRow {
        MutRow::from_datums(&self.row_to_check)
    }

    /// Go `CheckRowBuffer.AddColVal`.
    pub fn add_col_val(&mut self, val: Datum) {
        self.row_to_check.push(val);
    }

    /// Go `CheckRowBuffer.Reset`.
    pub fn reset(&mut self, capacity: usize) {
        ensure_capacity_and_reset(&mut self.row_to_check, 0, Some(capacity));
    }

    /// Go `CheckRowBuffer.rowToCheck`, for callers that inspect the staged row.
    #[must_use]
    pub fn row_to_check(&self) -> &[Datum] {
        &self.row_to_check
    }
}

/// Go `MutateBuffers` (`buffers.go:126`): a memory pool for table-related
/// allocation that aims to reuse memory and save allocations.
///
/// Used by `AddRecord`/`UpdateRecord`/`DeleteRecord`. Call a
/// `get_*_buffer_with_cap` method to take a buffer with its inner vectors
/// reset to a capacity. Because those vectors are reused, a second `get` call
/// before the previous usage finishes overwrites the previous data -- Rust's
/// `&mut` borrow makes that a compile error rather than Go's silent hazard.
#[derive(Debug)]
pub struct MutateBuffers {
    stmt_bufs: Rc<RefCell<WriteStmtBufs>>,
    encode_row: EncodeRowBuffer,
    check_row: CheckRowBuffer,
}

impl MutateBuffers {
    /// Go `NewMutateBuffers`.
    ///
    /// Go asserts the `*WriteStmtBufs` is non-nil under `intest`; an
    /// `Rc<RefCell<..>>` cannot be null, so the assertion has no Rust form.
    #[must_use]
    pub fn new(stmt_bufs: Rc<RefCell<WriteStmtBufs>>) -> Self {
        Self {
            encode_row: EncodeRowBuffer {
                col_ids: Vec::new(),
                row: Vec::new(),
                write_stmt_bufs: Rc::clone(&stmt_bufs),
            },
            check_row: CheckRowBuffer::default(),
            stmt_bufs,
        }
    }

    /// Go `MutateBuffers.GetEncodeRowBufferWithCap`.
    ///
    /// Usage:
    /// 1. call this to take the buffer,
    /// 2. call [`EncodeRowBuffer::add_col_val`] for every column,
    /// 3. call [`EncodeRowBuffer::write_mem_buffer_encoded`] to encode the row
    ///    and write it to the mem buffer.
    pub fn get_encode_row_buffer_with_cap(&mut self, capacity: usize) -> &mut EncodeRowBuffer {
        self.encode_row.reset(capacity);
        &mut self.encode_row
    }

    /// Go `MutateBuffers.GetCheckRowBufferWithCap`.
    ///
    /// Usage:
    /// 1. call this to take the buffer,
    /// 2. call [`CheckRowBuffer::add_col_val`] for every column,
    /// 3. call [`CheckRowBuffer::get_row_to_check`] for the constraint-check
    ///    row.
    pub fn get_check_row_buffer_with_cap(&mut self, capacity: usize) -> &mut CheckRowBuffer {
        self.check_row.reset(capacity);
        &mut self.check_row
    }

    /// Go `MutateBuffers.GetWriteStmtBufs`.
    #[must_use]
    pub fn get_write_stmt_bufs(&self) -> &Rc<RefCell<WriteStmtBufs>> {
        &self.stmt_bufs
    }
}

/// Go `StatisticsSupport` (`table.go:39`).
pub trait StatisticsSupport {
    /// Go `UpdatePhysicalTableDelta`.
    fn update_physical_table_delta(&self, physical_table_id: i64, delta: i64, count: i64);
}

/// Go `CachedTableSupport` (`table.go:45`).
pub trait CachedTableSupport {
    /// Go `AddCachedTableHandleToTxn`.
    ///
    /// The handle should implement Go's `table.CachedTable`, but Go types it
    /// `any` to avoid an import cycle; `&dyn Any` keeps that erasure.
    fn add_cached_table_handle_to_txn(&self, table_id: i64, handle: &dyn std::any::Any);
}

/// boundary: Go `pkg/util/tableutil.TempTable`, narrowed to what
/// [`TemporaryTableHandler`] reads. `tableutil` has no Rust owner yet.
pub trait TempTable {
    /// Go `TempTable.GetMeta`.
    fn get_meta(&self) -> &TableInfo;
    /// Go `TempTable.GetSize`.
    fn get_size(&self) -> i64;
    /// Go `TempTable.SetSize`.
    fn set_size(&self, size: i64);
}

/// boundary: Go `pkg/sessionctx/variable.TemporaryTableData`, narrowed to the
/// one method [`TemporaryTableHandler`] calls. The full interface is a
/// `kv.Retriever` plus staging, and has no Rust owner yet.
pub trait TemporaryTableData {
    /// Go `TemporaryTableData.GetTableSize`.
    fn get_table_size(&self, table_id: i64) -> i64;
}

/// Go `TemporaryTableHandler` (`table.go:53`): used by `table.Table` to
/// handle a temporary table.
pub struct TemporaryTableHandler<'a> {
    tbl_in_txn: &'a dyn TempTable,
    data: Option<&'a dyn TemporaryTableData>,
}

impl<'a> TemporaryTableHandler<'a> {
    /// Go `NewTemporaryTableHandler`.
    #[must_use]
    pub fn new(tbl: &'a dyn TempTable, data: Option<&'a dyn TemporaryTableData>) -> Self {
        Self {
            tbl_in_txn: tbl,
            data,
        }
    }

    /// Go `TemporaryTableHandler.Meta`.
    #[must_use]
    pub fn meta(&self) -> &TableInfo {
        self.tbl_in_txn.get_meta()
    }

    /// Go `TemporaryTableHandler.GetDirtySize`.
    #[must_use]
    pub fn get_dirty_size(&self) -> i64 {
        self.tbl_in_txn.get_size()
    }

    /// Go `TemporaryTableHandler.GetCommittedSize`.
    #[must_use]
    pub fn get_committed_size(&self) -> i64 {
        match self.data {
            None => 0,
            Some(data) => data.get_table_size(self.tbl_in_txn.get_meta().id),
        }
    }

    /// Go `TemporaryTableHandler.UpdateTxnDeltaSize`.
    pub fn update_txn_delta_size(&self, delta: i64) {
        self.tbl_in_txn.set_size(self.tbl_in_txn.get_size() + delta);
    }
}

/// Go `TemporaryTableSupport` (`table.go:90`).
pub trait TemporaryTableSupport {
    /// Go `GetTemporaryTableSizeLimit`.
    fn get_temporary_table_size_limit(&self) -> i64;
    /// Go `AddTemporaryTableToTxn`: marks the table modified so the txn
    /// handles it on commit, returning the extra info for it.
    fn add_temporary_table_to_txn(&self, tbl_info: &TableInfo)
        -> Option<TemporaryTableHandler<'_>>;
}

/// Go `ExchangePartitionDMLSupport` (`table.go:100`).
pub trait ExchangePartitionDMLSupport {
    /// Go `GetInfoSchemaToCheckExchangeConstraint`.
    fn get_info_schema_to_check_exchange_constraint(&self) -> &dyn MetaOnlyInfoSchema;
}

/// boundary: Go `pkg/sessionctx/variable.AssertionLevel`.
///
/// The transcreated owner is `tidb_session::varsutil::AssertionLevel`, but
/// `tidb-session` sits ABOVE `tidb-executor` in the crate graph exactly as
/// Go's `sessionctx` sits above `table`; depending on it here would invert
/// the workspace layering, so the three-state level is carried locally.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AssertionLevel {
    /// Go `AssertionLevelOff`.
    #[default]
    Off,
    /// Go `AssertionLevelFast`.
    Fast,
    /// Go `AssertionLevelStrict`.
    Strict,
}

/// Go `MutateContext` (`table.go:107`): the context a table mutation runs in.
///
/// Deferred members, each blocked on a Go type with no Rust owner:
/// - `GetRowIDShardGenerator` -- `variable.RowIDShardGenerator`.
/// - `GetReservedRowIDAlloc` -- `stmtctx.ReservedRowIDAlloc`.
/// - the embedded `AllocatorContext` -- `meta/autoid.Allocators`, which brings
///   the whole auto-ID allocator stack with it.
pub trait MutateContext {
    /// Go `MutateContext.GetExprCtx`.
    ///
    /// boundary: Go `exprctx.ExprContext`. `tidb-expr` publishes the narrowed
    /// `metabuild::ExprContext`; the full build-plus-eval interface is not
    /// transcreated yet.
    fn get_expr_ctx(&self) -> &dyn ExprContext;
    /// Go `MutateContext.ConnectionID`: 0 when not serving a client query.
    fn connection_id(&self) -> u64;
    /// Go `MutateContext.InRestrictedSQL`.
    fn in_restricted_sql(&self) -> bool;
    /// Go `MutateContext.TxnAssertionLevel`.
    fn txn_assertion_level(&self) -> AssertionLevel;
    /// Go `MutateContext.EnableMutationChecker`.
    fn enable_mutation_checker(&self) -> bool;
    /// Go `MutateContext.GetRowEncodingConfig`.
    fn get_row_encoding_config(&self) -> RowEncodingConfig;
    /// Go `MutateContext.GetMutateBuffers`.
    fn get_mutate_buffers(&mut self) -> &mut MutateBuffers;
    /// Go `MutateContext.GetStatisticsSupport`; `None` when unsupported.
    fn get_statistics_support(&self) -> Option<&dyn StatisticsSupport>;
    /// Go `MutateContext.GetCachedTableSupport`; `None` when unsupported.
    fn get_cached_table_support(&self) -> Option<&dyn CachedTableSupport>;
    /// Go `MutateContext.GetTemporaryTableSupport`; `None` when unsupported.
    fn get_temporary_table_support(&self) -> Option<&dyn TemporaryTableSupport>;
    /// Go `MutateContext.GetExchangePartitionDMLSupport`; `None` when
    /// unsupported.
    fn get_exchange_partition_dml_support(&self) -> Option<&dyn ExchangePartitionDMLSupport>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use tidb_datatype::{CoreTime, Time, TimeType};
    use tidb_error::errctx::STRICT_NO_WARNING_CONTEXT;
    use tidb_txnkv::IntHandle;

    /// Go's `mockMemBuffer`, which embeds a nil `kv.MemBuffer` and records the
    /// two calls the test asserts on.
    #[derive(Default)]
    struct MockMemBuffer {
        set_calls: Vec<(Key, Vec<u8>)>,
        set_with_flags_calls: Vec<(Key, Vec<u8>, Vec<FlagsOp>)>,
    }

    impl MutateMemBuffer for MockMemBuffer {
        type Error = Infallible;

        fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), Infallible> {
            self.set_calls.push((key, value));
            Ok(())
        }

        fn set_with_flags(
            &mut self,
            key: Key,
            value: Vec<u8>,
            flags: &[FlagsOp],
        ) -> Result<(), Infallible> {
            self.set_with_flags_calls.push((key, value, flags.to_vec()));
            Ok(())
        }
    }

    /// Go's `newMockMutateCtx`. Only the buffers matter to `buffers_test.go`,
    /// which embeds a nil `MutateContext` for everything else.
    fn new_mock_mutate_ctx() -> (Rc<RefCell<WriteStmtBufs>>, MutateBuffers) {
        let stmt_bufs = Rc::new(RefCell::new(WriteStmtBufs::default()));
        (Rc::clone(&stmt_bufs), MutateBuffers::new(stmt_bufs))
    }

    fn fixed_zone(name: &str, offset_secs: i32) -> SessionTimeZone {
        SessionTimeZone::Fixed {
            name: name.to_owned(),
            offset_secs,
        }
    }

    // Go TestEncodeRow. Expected values come from `tidb-tablecodec`, the
    // transcreated `tablecodec.EncodeRow`/`EncodeOldRow`, exactly as Go's test
    // takes them from the Go originals.
    #[test]
    fn encode_row() {
        let (stmt_bufs, mut buffers) = new_mock_mutate_ctx();
        let tm = Time::new(
            CoreTime::from_date(2021, 1, 1, 1, 2, 3, 4),
            TimeType::Timestamp,
            6,
        )
        .unwrap();
        let d1 = Datum::Bytes(vec![1, 2, 3]);
        let d2 = Datum::Int(20);
        let d3 = Datum::Time(tm);

        let buffer = buffers.get_encode_row_buffer_with_cap(3);
        assert!(Rc::ptr_eq(&stmt_bufs, buffer.write_stmt_bufs()));
        buffer.add_col_val(1, d1.clone());
        buffer.add_col_val(2, d2.clone());
        buffer.add_col_val(3, d3.clone());
        assert_eq!(buffer.col_ids(), &[1, 2, 3]);
        assert_eq!(buffer.row(), &[d1.clone(), d2.clone(), d3.clone()]);

        let row = [d1, d2, d3];
        let ids = [1i64, 2, 3];
        let utc = SessionTimeZone::utc();
        let fixed1 = fixed_zone("fixed1", 3600);
        let fixed2 = fixed_zone("fixed2", 3600 * 2);
        let cases: [(&SessionTimeZone, bool, bool, Vec<FlagsOp>); 3] = [
            (&utc, false, false, Vec::new()),
            (&fixed1, true, false, vec![FlagsOp::SetPresumeKeyNotExists]),
            (&fixed2, false, true, Vec::new()),
        ];

        for (loc, row_level_checksum, old_format, flags) in cases {
            let cfg = RowEncodingConfig {
                is_row_level_checksum_enabled: row_level_checksum,
                row_encoder: RowEncoder {
                    enable: !old_format,
                },
            };
            let handle: Handle = IntHandle::new(1).into();
            let checksum = row_level_checksum
                .then(|| RowChecksumPolicy::RawHandle(raw_checksum_handle(&handle)));
            let expected =
                encode_table_row(Some(loc), &row, &ids, !old_format, checksum.as_ref()).unwrap();

            let mut mem_buffer = MockMemBuffer::default();
            buffer
                .write_mem_buffer_encoded(
                    cfg,
                    Some(loc),
                    &STRICT_NO_WARNING_CONTEXT,
                    &mut mem_buffer,
                    Key::from_bytes(b"key1".to_vec()),
                    &handle,
                    &flags,
                )
                .unwrap();
            if flags.is_empty() {
                assert_eq!(
                    mem_buffer.set_calls,
                    vec![(Key::from_bytes(b"key1".to_vec()), expected.clone())]
                );
                assert!(mem_buffer.set_with_flags_calls.is_empty());
            } else {
                assert_eq!(
                    mem_buffer.set_with_flags_calls,
                    vec![(
                        Key::from_bytes(b"key1".to_vec()),
                        expected.clone(),
                        flags.clone()
                    )]
                );
                assert!(mem_buffer.set_calls.is_empty());
            }
            // the encoding result should be cached as a buffer
            assert_eq!(buffer.write_stmt_bufs().borrow().row_val_buf, expected);

            // test encode val for binlog
            let expected_old = encode_old_table_row(Some(loc), &row, &ids).unwrap();
            let encoded = buffer
                .encode_binlog_row_data(Some(loc), &STRICT_NO_WARNING_CONTEXT)
                .unwrap();
            assert_eq!(encoded, expected_old);
            // the encoded should not be referenced by any inner buffer. Go
            // compares `unsafe.SliceData`; safe Rust gets the same fact from
            // the fresh allocation's pointer.
            let bufs = buffer.write_stmt_bufs().borrow();
            assert!(!std::ptr::eq(encoded.as_ptr(), bufs.row_val_buf.as_ptr()));
            assert!(!std::ptr::eq(encoded.as_ptr(), bufs.index_key_buf.as_ptr()));
        }
    }

    // Go TestEncodeBufferReserve: the reserve-and-reuse contract.
    #[test]
    fn encode_buffer_reserve() {
        let (stmt_bufs, mut buffers) = new_mock_mutate_ctx();
        let mut mem_buffer = MockMemBuffer::default();

        let encode_row_ptr: *const EncodeRowBuffer = &buffers.encode_row;
        let buffer = buffers.get_encode_row_buffer_with_cap(6);
        assert!(std::ptr::eq(
            encode_row_ptr,
            buffer as *const EncodeRowBuffer
        ));
        assert!(Rc::ptr_eq(&stmt_bufs, buffer.write_stmt_bufs()));
        // data buffer should be reset to the capacity and length is 0
        assert_eq!(buffer.col_ids.capacity(), 6);
        assert_eq!(buffer.col_ids.len(), 0);
        assert_eq!(buffer.row.capacity(), 6);
        assert_eq!(buffer.row.len(), 0);

        // add some data and encode
        buffer.add_col_val(1, Datum::Int(1));
        buffer.add_col_val(2, Datum::Int(2));
        assert_eq!(buffer.col_ids.len(), 2);
        assert_eq!(buffer.row.len(), 2);
        buffer
            .write_mem_buffer_encoded(
                RowEncodingConfig {
                    is_row_level_checksum_enabled: false,
                    row_encoder: RowEncoder { enable: true },
                },
                Some(&SessionTimeZone::utc()),
                &STRICT_NO_WARNING_CONTEXT,
                &mut mem_buffer,
                Key::from_bytes(b"key1".to_vec()),
                &IntHandle::new(1).into(),
                &[],
            )
            .unwrap();
        assert_eq!(mem_buffer.set_calls.len(), 1);
        let (encoded_cap, add_row_values_cap) = {
            let bufs = buffer.write_stmt_bufs().borrow();
            assert!(bufs.row_val_buf.capacity() > 0);
            assert_eq!(bufs.add_row_values.len(), 4);
            (bufs.row_val_buf.capacity(), bufs.add_row_values.capacity())
        };

        // reset should not shrink the capacity
        buffer.reset(2);
        assert_eq!(buffer.col_ids.capacity(), 6);
        assert_eq!(buffer.col_ids.len(), 0);
        assert_eq!(buffer.row.capacity(), 6);
        assert_eq!(buffer.row.len(), 0);
        let bufs = buffer.write_stmt_bufs().borrow();
        assert_eq!(bufs.add_row_values.capacity(), add_row_values_cap);
        assert_eq!(bufs.row_val_buf.capacity(), encoded_cap);
    }

    // Go TestCheckRowBuffer.
    #[test]
    fn check_row_buffer() {
        let mut buffer = CheckRowBuffer::default();
        buffer.reset(6);
        assert_eq!(buffer.row_to_check.len(), 0);
        assert_eq!(buffer.row_to_check.capacity(), 6);
        buffer.add_col_val(Datum::Int(1));
        buffer.add_col_val(Datum::Int(2));
        assert_eq!(buffer.row_to_check(), &[Datum::Int(1), Datum::Int(2)]);
        let mut_row = buffer.get_row_to_check();
        let row_to_check = mut_row.to_row();
        assert_eq!(row_to_check.len(), 2);
        assert_eq!(row_to_check.get_int64(0), 1);
        assert_eq!(row_to_check.get_int64(1), 2);

        // reset should not shrink the capacity
        buffer.reset(2);
        assert_eq!(buffer.row_to_check.len(), 0);
        assert_eq!(buffer.row_to_check.capacity(), 6);
    }

    // Go TestMutateBuffersGetter.
    #[test]
    fn mutate_buffers_getter() {
        let stmt_bufs = Rc::new(RefCell::new(WriteStmtBufs::default()));
        let mut buffers = MutateBuffers::new(Rc::clone(&stmt_bufs));
        let add = buffers.get_encode_row_buffer_with_cap(6);
        assert_eq!(add.row.capacity(), 6);
        assert!(Rc::ptr_eq(&stmt_bufs, add.write_stmt_bufs()));

        let update = buffers.get_check_row_buffer_with_cap(6);
        assert_eq!(update.row_to_check.capacity(), 6);

        assert!(Rc::ptr_eq(&stmt_bufs, buffers.get_write_stmt_bufs()));
    }

    // Go TestEnsureCapacityAndReset.
    //
    // Go asserts aliasing by writing through the returned reslice and reading
    // the change back out of the ORIGINAL slice header, which no longer has a
    // meaning once the function mutates one owned `Vec` in place. Those four
    // assertions become pointer-identity checks: the allocation was reused,
    // which is the property Go's aliasing trick was proving.
    #[test]
    fn ensure_capacity_and_reset_matches_go() {
        let mut slice: Vec<i64> = Vec::new();
        ensure_capacity_and_reset(&mut slice, 0, None);
        assert!(slice.is_empty());
        assert_eq!(slice.capacity(), 0);

        let mut input = vec![1i64, 2, 3];
        let before = input.as_ptr();
        ensure_capacity_and_reset(&mut input, 0, None);
        assert_eq!(input.len(), 0);
        assert_eq!(input.capacity(), 3);
        // share the same underlying array
        assert!(std::ptr::eq(before, input.as_ptr()));

        let mut input = vec![1i64, 2, 3];
        let before = input.as_ptr();
        ensure_capacity_and_reset(&mut input, 2, None);
        assert_eq!(input.len(), 2);
        assert_eq!(input.capacity(), 3);
        // share the same underlying array, with the surviving prefix intact
        assert!(std::ptr::eq(before, input.as_ptr()));
        assert_eq!(input, vec![1, 2]);

        let mut input = vec![1i64, 2, 3];
        ensure_capacity_and_reset(&mut input, 4, None);
        assert_eq!(input.len(), 4);
        assert_eq!(input.capacity(), 4);

        let mut input = vec![1i64, 2, 3];
        let before = input.as_ptr();
        ensure_capacity_and_reset(&mut input, 1, Some(2));
        assert_eq!(input.len(), 1);
        // if cap < originalCap, keep the original capacity
        assert_eq!(input.capacity(), 3);
        // share the same underlying array
        assert!(std::ptr::eq(before, input.as_ptr()));
        assert_eq!(input, vec![1]);

        let mut input = vec![1i64, 2, 3];
        ensure_capacity_and_reset(&mut input, 2, Some(4));
        assert_eq!(input.len(), 2);
        assert_eq!(input.capacity(), 4);

        let mut input = vec![1i64, 2, 3];
        ensure_capacity_and_reset(&mut input, 4, Some(5));
        assert_eq!(input.len(), 4);
        assert_eq!(input.capacity(), 5);
    }
}
