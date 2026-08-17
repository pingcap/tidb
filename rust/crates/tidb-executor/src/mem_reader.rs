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

//! `pkg/executor/mem_reader.go`: the readers that produce the rows a
//! transaction has written but not committed -- `memIndexReader` (:52),
//! `memTableReader` (:238), `memIndexLookUpReader` (:692) and
//! `memIndexMergeReader` (:809).
//!
//! # What these readers actually are
//!
//! `UnionScanExec` answers a read inside a transaction by combining TWO
//! sources: the storage read (a `TableReader`/`IndexReader`/`IndexLookUp`
//! below it) and the rows the transaction itself has written. THIS file is
//! only the second source. The merge of the two -- and therefore the rule
//! that a dirty row SHADOWS the committed row with the same handle -- lives
//! in `pkg/executor/union_scan.go`, which is NOT ported here.
//!
//! What IS here, and what the whole file exists for, is the *other* half of
//! overlay semantics:
//!
//! * **The added/modified row.** Every entry the statement or an earlier
//!   statement of the same transaction wrote sits in the transaction's memory
//!   buffer, keyed exactly as it would be in storage. Reading it back is
//!   reading the buffer over the same key ranges the storage read uses, and
//!   decoding the bytes with the ordinary row/index decoders. A row INSERTed
//!   and then UPDATEd in the same transaction has one buffer entry carrying
//!   the latest value, so "added" and "modified" are the same case here.
//! * **The deleted row.** A DELETE writes a TOMBSTONE: the key with an EMPTY
//!   value. [`iter_txn_mem_buffer`] (Go :585) and both streaming iterators
//!   (Go :913, :986) test `len(value) == 0` and SKIP the entry. That skip is
//!   the deletion handling in its entirety on this side; `union_scan.go` uses
//!   the same buffer independently to suppress the committed row.
//! * **The cached / temporary-table overlay.** Go `getSnapIter` (:625) puts a
//!   SECOND source under the buffer: `TemporaryTableData` for a temporary
//!   table, or `cacheTable` for a cached table. When present it is unioned
//!   with the buffer through `transaction.NewUnionIter(dirty, snapshot,
//!   reverse)`, dirty winning on equal keys. That union is reused here from
//!   [`tidb_txnkv::union_iter::UnionIter`] rather than restated.
//!
//! # Reused rather than restated
//!
//! * [`tidb_txnkv::union_iter::UnionIter`] IS Go
//!   `pkg/store/driver/txn.NewUnionIter`, including its own tombstone
//!   skipping and its dirty-wins-on-equal-key rule.
//! * [`tidb_txnkv::iteration::KvIterator`] is Go `kv.Iterator`; [`ScanIter`]
//!   is only its object-safe form (one fixed error type) so a range cursor
//!   can be boxed.
//! * [`crate::kv_table::RowDecoder::decode_record`] is Go
//!   `memTableReader.decodeRecordKeyValue` (:475) + `decodeRowData` (:484) +
//!   `getRowData` (:501) TOGETHER: row-key to handle, new-format and legacy
//!   value decoding, handle-column restoration for `PKIsHandle` /
//!   `IsCommonHandle` / `_tidb_rowid`, and origin-default filling. The Go
//!   trio is byte-level (`[][]byte` slots plus `hasColVal` :575); the Rust
//!   decoder returns the datum row directly. Same observable row, and
//!   `hasColVal`/`allocBuf` have no counterpart because there are no reusable
//!   byte slots to track.
//! * [`tidb_tablecodec::decode_index_kv`] is Go `tablecodec.DecodeIndexKVEx`;
//!   [`tidb_tablecodec::decode_index_handle`] is `tablecodec.DecodeIndexHandle`;
//!   [`tidb_tablecodec::split_index_value`] is `tablecodec.SplitIndexValue`.
//! * [`crate::predicate_pushdown::ScanFilterProbe`] is Go
//!   `expression.EvalBool(ctx, m.conditions, mutableRow.ToRow())` -- it owns
//!   the scratch chunk that Go's `chunk.MutRowFromTypes` owns.
//! * [`crate::kv_table::TableHandle`] is Go `kv.Handle`. Go's
//!   `kv.PartitionHandle` is [`MemHandle`] here, a handle plus an optional
//!   partition id, because `TableHandle` deliberately has no partition arm.
//!   `kv.NewHandleMap` is then a plain [`BTreeMap`].
//!
//! # Sequential here, concurrent there
//!
//! `mem_reader.go` is ALREADY sequential in Go: none of these four readers
//! starts a goroutine. `getMemRows` walks its ranges on the calling
//! goroutine, and `getMemRowsIter` returns a cursor the caller pulls. The
//! only concurrency near this file is (a) the `IndexLookUpExecutor` /
//! `IndexMergeReaderExecutor` workers that produced the `kvRanges` these
//! readers are CONSTRUCTED from, which is [`crate::index_merge_reader`]'s
//! business and already documented there, and (b) `tracing.StartRegion`,
//! which is instrumentation.
//!
//! So nothing is lost to a sequential structure here, and there is no
//! observable difference to state: the row ORDER a mem reader produces is
//! fully determined by the range list and the buffer's key order in both
//! languages, which is exactly why Go can build `memIndexLookUpReader` by
//! running its index reader to completion first and only then opening the
//! table reader over the handles it found.
//!
//! # Narrowings, every one named
//!
//! * **`compareExec`** (`pkg/executor/union_scan.go`) supplies `desc`,
//!   `needExtraSorting` and `compare`. It IS now ported, as
//!   [`crate::union_scan::CompareExec`], which implements [`RowComparator`];
//!   the two flags stay constructor inputs because a mem reader is built
//!   before the `UnionScanExec` that owns them. The `keepOrder &&
//!   needExtraSorting` branch (:100, :406, :176) is ported around it exactly.
//! * **`distsql.TableHandlesToKVRanges`** (`pkg/distsql/request_builder.go`)
//!   turns the handles a partial reader found into record ranges (:769,
//!   :1103). It is outside this Go package, so it is the
//!   [`HandleRangeEncoder`] trait.
//! * **`sessionctx.Context.Txn(true).GetMemBuffer()`** plus `getSnapIter`'s
//!   `TemporaryTableData` / `cacheTable` choice is the [`MemBufferSource`]
//!   trait: this tier has no session context to pull a live transaction out
//!   of, and which buffer a statement reads is the session's fact, not this
//!   file's.
//! * **`NewRowDecoder`/`rowcodec.NewByteDecoder` construction** (:293) and
//!   `getColIDAndPkColIDs` (:1150) build the two decoders from the table
//!   meta. [`crate::kv_table::RowDecoder`] is built by its own constructor
//!   from a [`crate::kv_table::KvTable`], so the Go builder is not restated;
//!   a `MemTableReader` takes the finished decoder.
//! * `tracing.StartRegion`, `addedRowsLen`, and the `buf`/`decodeBuff`/
//!   `resultRows` reuse buffers are instrumentation and allocation reuse with
//!   no observable effect; not ported.
//! * `memIndexLookUpReader.getMemRowsHandle` (:805) and
//!   `memIndexMergeReader.getMemRowsHandle` (:1146) return an error in Go;
//!   they are the same error here.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use tidb_datatype::{Datum, FieldType, SessionTimeZone};
use tidb_txnkv::{Handle as KvHandle, Key, KvIterator, UnionIter};

use crate::executor::ExecError;
use crate::kv_table::{RowDecoder, TableHandle};
use crate::predicate_pushdown::ScanFilterProbe;

/// A half-open `[start, end)` key interval, the crate's spelling of Go
/// `kv.KeyRange` (see [`crate::handle_range::record_key_ranges`]).
pub type MemKeyRange = (Key, Key);

/// What a memory-buffer read can fail with.
#[derive(Debug)]
pub enum MemReaderError {
    /// The underlying buffer or snapshot iterator failed.
    Iteration(String),
    /// A key or value could not be decoded.
    Decode(String),
    /// A condition could not be evaluated, or a comparator failed.
    Eval(Box<ExecError>),
    /// Go's `errors.New` cases: an operation a reader does not implement.
    Unsupported(&'static str),
}

impl From<ExecError> for MemReaderError {
    fn from(error: ExecError) -> Self {
        MemReaderError::Eval(Box::new(error))
    }
}

/// Go `kv.Iterator`, in the object-safe form a boxed range cursor needs.
///
/// [`tidb_txnkv::iteration::KvIterator`] has an associated error type, so a
/// `dyn KvIterator` cannot be unified across sources. This trait fixes the
/// error to [`MemReaderError`] and is otherwise the same contract; the blanket
/// [`KvIterator`] impl below is what lets a `Box<dyn ScanIter>` be fed
/// straight into [`UnionIter`].
pub trait ScanIter {
    /// Go `Valid`.
    fn valid(&self) -> bool;
    /// Go `Key`.
    fn key(&self) -> &Key;
    /// Go `Value`.
    fn value(&self) -> &[u8];
    /// Go `Next`.
    fn advance(&mut self) -> Result<(), MemReaderError>;
    /// Go `Close`.
    fn close(&mut self);
}

impl KvIterator for Box<dyn ScanIter> {
    type Error = MemReaderError;

    fn valid(&self) -> bool {
        (**self).valid()
    }

    fn key(&self) -> &Key {
        (**self).key()
    }

    fn value(&self) -> &[u8] {
        (**self).value()
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        (**self).advance()
    }

    fn close(&mut self) {
        (**self).close();
    }
}

/// A cursor over an already materialized entry list.
///
/// Go's buffer iterators are views over a live rbtree; a snapshot copy is what
/// [`tidb_txnkv::mem_storage::MemIterator`] does too, and it is the shape a
/// [`MemBufferSource`] implementation can always produce.
pub struct VecScanIter {
    entries: Vec<(Key, Vec<u8>)>,
    position: usize,
}

impl VecScanIter {
    /// Builds a cursor over `entries`, which must already be ordered in the
    /// direction the caller asked for.
    #[must_use]
    pub fn new(entries: Vec<(Key, Vec<u8>)>) -> Self {
        VecScanIter {
            entries,
            position: 0,
        }
    }
}

impl ScanIter for VecScanIter {
    fn valid(&self) -> bool {
        self.position < self.entries.len()
    }

    fn key(&self) -> &Key {
        &self.entries[self.position].0
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.position].1
    }

    fn advance(&mut self) -> Result<(), MemReaderError> {
        if !self.valid() {
            return Err(MemReaderError::Iteration(
                "iterator advanced past its end".to_owned(),
            ));
        }
        self.position += 1;
        Ok(())
    }

    fn close(&mut self) {
        self.entries.clear();
        self.position = 0;
    }
}

/// boundary: Go `sessionctx.Context.Txn(true).GetMemBuffer()` together with
/// `getSnapIter` (`pkg/executor/mem_reader.go:625`).
///
/// Go reaches the transaction's memory buffer through the session context and
/// then decides, per range, whether a SECOND source sits under it:
/// `SessionVars.TemporaryTableData` for a temporary table, else the
/// `cacheTable` a cached table read carries. This tier has no session context,
/// so both halves are asked of the caller. Everything above this trait --
/// range order, the union, the tombstone rule, decoding -- is ported.
pub trait MemBufferSource {
    /// Go `txn.GetMemBuffer().SnapshotIter(start, end)`, or
    /// `SnapshotIterReverse(end, start)` when `reverse`.
    ///
    /// "Snapshot" here is Go's name for the buffer content EXCLUDING an open
    /// staging area, not for a storage snapshot.
    fn snapshot_iter(
        &self,
        start: &Key,
        end: &Key,
        reverse: bool,
    ) -> Result<Box<dyn ScanIter>, MemReaderError>;

    /// Go `getSnapIter` (:625): the temporary-table or cached-table source
    /// under the buffer, or `None` when the read has neither.
    fn snap_cache_iter(
        &self,
        _start: &Key,
        _end: &Key,
        _reverse: bool,
    ) -> Result<Option<Box<dyn ScanIter>>, MemReaderError> {
        Ok(None)
    }
}

/// Go `pkg/executor/union_scan.go` `compareExec.compare`: the ordering a
/// `keepOrder` read restores -- index columns in `usedIndex` order, then the
/// handle, each honoring `desc`.
///
/// No longer a boundary: [`crate::union_scan::CompareExec`] is that Go struct,
/// ported, and implements this trait. The trait remains because a mem reader
/// is CONSTRUCTED before the `UnionScanExec` that embeds the comparator.
pub trait RowComparator {
    /// Go `compareExec.compare(sc, a, b)`.
    fn compare(&self, left: &[Datum], right: &[Datum]) -> Result<Ordering, MemReaderError>;
}

/// boundary: Go `distsql.TableHandlesToKVRanges`
/// (`pkg/distsql/request_builder.go`).
///
/// Turning handles back into record ranges is request building, outside this
/// Go package; [`MemIndexLookUpReader`] (:769) and [`MemIndexMergeReader`]
/// (:1103) are the two callers.
pub trait HandleRangeEncoder {
    /// The record ranges covering `handles` under `physical_table_id`. Go
    /// passes `0` for the id when the handles are partition handles that
    /// already carry their own.
    fn table_handles_to_kv_ranges(
        &self,
        physical_table_id: i64,
        handles: &[MemHandle],
    ) -> Result<Vec<MemKeyRange>, MemReaderError>;
}

/// Go `kv.Handle`, plus the partition id Go carries in `kv.PartitionHandle`.
///
/// [`TableHandle`] has no partition arm by design, so the partition id rides
/// alongside. `None` is an ordinary handle; `Some(id)` is
/// `kv.NewPartitionHandle(id, handle)`.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MemHandle {
    /// Go `kv.PartitionHandle.PartitionID`.
    pub partition_id: Option<i64>,
    /// The handle itself.
    pub handle: TableHandle,
}

impl MemHandle {
    /// An ordinary, non-partition handle.
    #[must_use]
    pub fn plain(handle: TableHandle) -> Self {
        MemHandle {
            partition_id: None,
            handle,
        }
    }

    /// Go `kv.NewPartitionHandle(partition_id, handle)`.
    #[must_use]
    pub fn in_partition(partition_id: i64, handle: TableHandle) -> Self {
        MemHandle {
            partition_id: Some(partition_id),
            handle,
        }
    }
}

/// The cursor over ONE key range: the buffer alone, or the buffer unioned over
/// the temporary/cached-table source.
enum RangeCursor {
    /// Go's `tmp` when `getSnapIter` returned nil.
    Buffer(Box<dyn ScanIter>),
    /// Go `transaction.NewUnionIter(tmp, snapCacheIter, reverse)`.
    Union(Box<UnionIter<Box<dyn ScanIter>, Box<dyn ScanIter>>>),
}

impl RangeCursor {
    fn valid(&self) -> bool {
        match self {
            RangeCursor::Buffer(iter) => iter.valid(),
            RangeCursor::Union(iter) => iter.valid(),
        }
    }

    fn key(&self) -> &Key {
        match self {
            RangeCursor::Buffer(iter) => iter.key(),
            RangeCursor::Union(iter) => iter.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self {
            RangeCursor::Buffer(iter) => iter.value(),
            RangeCursor::Union(iter) => iter.value(),
        }
    }

    fn advance(&mut self) -> Result<(), MemReaderError> {
        match self {
            RangeCursor::Buffer(iter) => iter.advance(),
            RangeCursor::Union(iter) => iter.next(),
        }
    }

    fn close(&mut self) {
        match self {
            RangeCursor::Buffer(iter) => iter.close(),
            RangeCursor::Union(iter) => iter.close(),
        }
    }
}

/// Opens the cursor for one range: Go's body of `txnMemBufferIter.Valid`
/// (:344) and of the loop in `iterTxnMemBuffer` (:585), which are identical.
fn open_range<S: MemBufferSource>(
    source: &S,
    range: &MemKeyRange,
    reverse: bool,
) -> Result<RangeCursor, MemReaderError> {
    let (start, end) = range;
    let buffer = source.snapshot_iter(start, end, reverse)?;
    match source.snap_cache_iter(start, end, reverse)? {
        None => Ok(RangeCursor::Buffer(buffer)),
        Some(snapshot) => match UnionIter::new(buffer, snapshot, reverse) {
            Ok(union) => Ok(RangeCursor::Union(Box::new(union))),
            Err(failure) => {
                let (error, _, _) = failure.into_parts();
                Err(error)
            }
        },
    }
}

/// Go `txnMemBufferIter` (:319): one cursor over a LIST of ranges, opening
/// each range's iterator lazily.
///
/// `valid` takes `&mut self` because Go's `Valid` mutates -- it advances
/// `idx`, opens the next range's iterator, and parks an error in `iter.err`
/// while still answering `true` so that the following `Next` reports it. Both
/// behaviors are reproduced.
pub struct TxnMemBufferIter<'a, S: MemBufferSource> {
    source: &'a S,
    ranges: Vec<MemKeyRange>,
    index: usize,
    current: Option<RangeCursor>,
    reverse: bool,
    error: Option<MemReaderError>,
}

impl<'a, S: MemBufferSource> TxnMemBufferIter<'a, S> {
    /// Go `newTxnMemBufferIter` (:330).
    pub fn new(source: &'a S, ranges: Vec<MemKeyRange>, reverse: bool) -> Self {
        TxnMemBufferIter {
            source,
            ranges,
            index: 0,
            current: None,
            reverse,
            error: None,
        }
    }

    /// Go `Valid` (:344).
    pub fn valid(&mut self) -> bool {
        if self.error.is_some() {
            return true;
        }
        if let Some(current) = &self.current {
            if current.valid() {
                return true;
            }
            self.index += 1;
        }
        while self.index < self.ranges.len() {
            let range = self.ranges[self.index].clone();
            match open_range(self.source, &range, self.reverse) {
                Ok(cursor) => {
                    self.current = Some(cursor);
                    if self
                        .current
                        .as_ref()
                        .is_some_and(super::mem_reader::RangeCursor::valid)
                    {
                        return true;
                    }
                }
                Err(error) => {
                    // Go parks the error and answers `true`, so the caller's
                    // following `Next` surfaces it.
                    self.error = Some(error);
                    return true;
                }
            }
            self.index += 1;
        }
        false
    }

    /// Go `Next` (:380). A cursor that is not valid is NOT an error: Go
    /// returns nil, leaving the advance to the next `Valid`.
    pub fn advance(&mut self) -> Result<(), MemReaderError> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        match &mut self.current {
            Some(cursor) if cursor.valid() => cursor.advance(),
            _ => Ok(()),
        }
    }

    /// Go `Key` (:392). Only valid after [`TxnMemBufferIter::valid`].
    #[must_use]
    pub fn key(&self) -> &Key {
        self.current
            .as_ref()
            .expect("Key is only defined on a valid iterator")
            .key()
    }

    /// Go `Value` (:396).
    #[must_use]
    pub fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .expect("Value is only defined on a valid iterator")
            .value()
    }

    /// Go `Close` (:400).
    pub fn close(&mut self) {
        if let Some(cursor) = &mut self.current {
            cursor.close();
        }
    }
}

/// Go `iterTxnMemBuffer` (:585): walks every range's buffer entries in order
/// and hands each LIVE key/value to `process`.
///
/// The one rule that is not bookkeeping is the tombstone skip: an entry whose
/// value is empty is a key the transaction DELETED, and it is never passed on.
pub fn iter_txn_mem_buffer<S, F>(
    source: &S,
    ranges: &[MemKeyRange],
    reverse: bool,
    mut process: F,
) -> Result<(), MemReaderError>
where
    S: MemBufferSource,
    F: FnMut(&Key, &[u8]) -> Result<(), MemReaderError>,
{
    for range in ranges {
        let mut cursor = open_range(source, range, reverse)?;
        while cursor.valid() {
            if !cursor.value().is_empty() {
                process(cursor.key(), cursor.value())?;
            }
            cursor.advance()?;
        }
        cursor.close();
    }
    Ok(())
}

/// Go `memRowsIter` (:882): the cursor `UnionScanExec` pulls added rows from.
pub trait MemRowsIter {
    /// Go `Next`. `None` is Go's `nil, nil` end of iteration.
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, MemReaderError>;
    /// Go `Close`, which releases the snapshot the cursor holds.
    fn close(&mut self);
}

/// Go `defaultRowsIter` (:888): a cursor over an already materialized batch,
/// which is what every `keepOrder && needExtraSorting` read produces.
pub struct DefaultRowsIter {
    data: Vec<Vec<Datum>>,
    cursor: usize,
}

impl DefaultRowsIter {
    /// Builds the cursor over `data`.
    #[must_use]
    pub fn new(data: Vec<Vec<Datum>>) -> Self {
        DefaultRowsIter { data, cursor: 0 }
    }
}

impl MemRowsIter for DefaultRowsIter {
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, MemReaderError> {
        if self.cursor < self.data.len() {
            let row = std::mem::take(&mut self.data[self.cursor]);
            self.cursor += 1;
            return Ok(Some(row));
        }
        Ok(None)
    }

    fn close(&mut self) {}
}

/// Go `memTableReader` (:238): the added rows of a TABLE read.
pub struct MemTableReader {
    /// Go `kvRanges`, already reversed by the builder when `desc` (:295).
    kv_ranges: Vec<MemKeyRange>,
    /// Go `buffer.rd` + `buffer.cd` + `columns` + `colIDs` + `pkColIDs`, all
    /// of which [`RowDecoder`] already holds.
    decoder: RowDecoder,
    /// Go `conditions`, evaluated as `expression.EvalBool`.
    conditions: Option<ScanFilterProbe>,
    /// Go `compareExec.desc`.
    desc: bool,
    /// Go `keepOrder`.
    keep_order: bool,
    /// Go `compareExec.needExtraSorting`.
    need_extra_sorting: bool,
}

impl MemTableReader {
    /// Go `buildMemTableReader` (:262), minus the decoder construction the
    /// caller has already done.
    ///
    /// `kv_ranges` is reversed here exactly as Go's builder reverses it for a
    /// descending read (:295), so callers pass ascending ranges.
    #[must_use]
    pub fn new(
        decoder: RowDecoder,
        mut kv_ranges: Vec<MemKeyRange>,
        conditions: Option<ScanFilterProbe>,
        desc: bool,
        keep_order: bool,
        need_extra_sorting: bool,
    ) -> Self {
        if desc {
            kv_ranges.reverse();
        }
        MemTableReader {
            kv_ranges,
            decoder,
            conditions,
            desc,
            keep_order,
            need_extra_sorting,
        }
    }

    /// Replaces the ranges, as `memIndexMergeReader.getHandles` (:1050) does
    /// before each partial read.
    pub fn set_kv_ranges(&mut self, kv_ranges: Vec<MemKeyRange>) {
        self.kv_ranges = kv_ranges;
    }

    /// Go `memTableReader.decodeRecordKeyValue` (:475) -> `decodeRowData`
    /// (:484) -> `getRowData` (:501), all three of which
    /// [`RowDecoder::decode_record`] already performs.
    fn decode_record(&self, key: &Key, value: &[u8]) -> Result<Vec<Datum>, MemReaderError> {
        self.decoder
            .decode_record(key_bytes(key), value)
            .map(|(_, row)| row)
            .map_err(|error| MemReaderError::Decode(format!("{error:?}")))
    }

    /// Go `memTableReader.getMemRows` (:434).
    pub fn get_mem_rows<S: MemBufferSource>(
        &mut self,
        source: &S,
        comparator: Option<&dyn RowComparator>,
    ) -> Result<Vec<Vec<Datum>>, MemReaderError> {
        let mut added_rows: Vec<Vec<Datum>> = Vec::new();
        let decoder = &self.decoder;
        let conditions = &mut self.conditions;
        iter_txn_mem_buffer(source, &self.kv_ranges, self.desc, |key, value| {
            let row = decoder
                .decode_record(key_bytes(key), value)
                .map(|(_, row)| row)
                .map_err(|error| MemReaderError::Decode(format!("{error:?}")))?;
            if !admits(conditions.as_mut(), &row)? {
                return Ok(());
            }
            added_rows.push(row);
            Ok(())
        })?;
        if self.keep_order && self.need_extra_sorting {
            sort_rows(&mut added_rows, comparator)?;
        }
        Ok(added_rows)
    }

    /// Go `memTableReader.getMemRowsIter` (:406).
    pub fn get_mem_rows_iter<'a, S: MemBufferSource>(
        &'a mut self,
        source: &'a S,
        comparator: Option<&dyn RowComparator>,
    ) -> Result<Box<dyn MemRowsIter + 'a>, MemReaderError> {
        if self.keep_order && self.need_extra_sorting {
            let data = self.get_mem_rows(source, comparator)?;
            return Ok(Box::new(DefaultRowsIter::new(data)));
        }
        let kv_iter = TxnMemBufferIter::new(source, self.kv_ranges.clone(), self.desc);
        Ok(Box::new(MemRowsIterForTable {
            kv_iter,
            reader: self,
        }))
    }

    /// Go `memTableReader.getMemRowsHandle` (:559), the entry
    /// `memIndexMergeReader` uses when a partial plan is a table scan.
    pub fn get_mem_rows_handle<S: MemBufferSource>(
        &self,
        source: &S,
    ) -> Result<Vec<MemHandle>, MemReaderError> {
        let mut handles = Vec::new();
        let decoder = &self.decoder;
        iter_txn_mem_buffer(source, &self.kv_ranges, self.desc, |key, _value| {
            let handle = decoder
                .record_handle(key_bytes(key))
                .map_err(|error| MemReaderError::Decode(format!("{error:?}")))?;
            handles.push(MemHandle::plain(handle));
            Ok(())
        })?;
        Ok(handles)
    }
}

/// Go `memRowsIterForTable` (:905).
///
/// Go keeps two decode paths here -- `ChunkDecoder.DecodeToChunk` for a
/// new-format value and a fallback through `decodeRecordKeyValue` for the
/// legacy format -- because only the former can write straight into a chunk.
/// [`RowDecoder::decode_record`] already handles both formats and yields
/// datums either way, so there is one path here and the same rows come out.
struct MemRowsIterForTable<'a, S: MemBufferSource> {
    kv_iter: TxnMemBufferIter<'a, S>,
    reader: &'a mut MemTableReader,
}

impl<S: MemBufferSource> MemRowsIter for MemRowsIterForTable<'_, S> {
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, MemReaderError> {
        while self.kv_iter.valid() {
            let key = self.kv_iter.key().clone();
            let value = self.kv_iter.value().to_vec();
            self.kv_iter.advance()?;
            // The transaction deleted this key.
            if value.is_empty() {
                continue;
            }
            let row = self.reader.decode_record(&key, &value)?;
            if !admits(self.reader.conditions.as_mut(), &row)? {
                continue;
            }
            return Ok(Some(row));
        }
        Ok(None)
    }

    fn close(&mut self) {
        self.kv_iter.close();
    }
}

/// The decoding metadata `memIndexReader` derives once from the table and
/// index meta: Go `getTypes` (:126) plus
/// `tables.BuildRowcodecColInfoForIndexColumns` /
/// `TryAppendCommonHandleRowcodecColInfos` (:112).
#[derive(Clone, Debug)]
pub struct IndexDecodeMeta {
    /// Go `len(m.index.Columns)`.
    pub index_columns: usize,
    /// Go `getTypes()`: one entry per index column, then the handle
    /// column(s).
    pub types: Vec<FieldType>,
    /// The rowcodec column info `tablecodec.DecodeIndexKVEx` needs.
    pub column_infos: Vec<tidb_codec::ColumnInfo>,
    /// Go `TableInfo.IsCommonHandle`.
    pub common_handle: bool,
    /// Go `IndexInfo.Global`.
    pub global: bool,
    /// The table's persisted new-collation mode.
    pub use_new_collation: bool,
}

/// Go `memIndexReader` (:52): the added ENTRIES of an index read.
pub struct MemIndexReader {
    /// Go `kvRanges`, reversed by the builder when `desc` (:80).
    kv_ranges: Vec<MemKeyRange>,
    meta: IndexDecodeMeta,
    /// Go `outputOffset`, the index-entry column each output column comes
    /// from.
    output_offset: Vec<usize>,
    /// Go `physTblIDIdx`, `-1` when the schema has no `physTblID` column.
    phys_tbl_id_idx: Option<usize>,
    /// Go `partitionIDMap`.
    partition_id_map: BTreeSet<i64>,
    conditions: Option<ScanFilterProbe>,
    zone: SessionTimeZone,
    desc: bool,
    keep_order: bool,
    need_extra_sorting: bool,
}

impl MemIndexReader {
    /// Go `buildMemIndexReader` (:73).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        meta: IndexDecodeMeta,
        mut kv_ranges: Vec<MemKeyRange>,
        output_offset: Vec<usize>,
        phys_tbl_id_idx: Option<usize>,
        partition_id_map: BTreeSet<i64>,
        conditions: Option<ScanFilterProbe>,
        zone: SessionTimeZone,
        desc: bool,
        keep_order: bool,
        need_extra_sorting: bool,
    ) -> Self {
        if desc {
            kv_ranges.reverse();
        }
        MemIndexReader {
            kv_ranges,
            meta,
            output_offset,
            phys_tbl_id_idx,
            partition_id_map,
            conditions,
            zone,
            desc,
            keep_order,
            need_extra_sorting,
        }
    }

    /// Replaces the ranges, as `memIndexMergeReader.getHandles` (:1052) does.
    pub fn set_kv_ranges(&mut self, kv_ranges: Vec<MemKeyRange>) {
        self.kv_ranges = kv_ranges;
    }

    /// Go `decodeIndexKeyValue` (:192).
    ///
    /// The `physTblID` correction is the subtle part and is kept verbatim:
    /// that column exists in the SCHEMA but not in the index VALUE, because
    /// it is recovered from the key head, so every output offset AFTER it
    /// shifts down by one.
    pub fn decode_index_key_value(
        &self,
        key: &Key,
        value: &[u8],
    ) -> Result<Vec<Datum>, MemReaderError> {
        let key = key_bytes(key);
        let handle_status = if self.meta.types[self.meta.index_columns].is_unsigned() {
            tidb_tablecodec::HandleStatus::Unsigned
        } else {
            tidb_tablecodec::HandleStatus::Default
        };
        let values = tidb_tablecodec::decode_index_kv(
            self.meta.use_new_collation,
            key,
            value,
            self.meta.index_columns,
            handle_status,
            &self.meta.column_infos,
        )
        .map_err(|error| MemReaderError::Decode(format!("{error:?}")))?;

        let phys_tbl_id_column_idx = self
            .phys_tbl_id_idx
            .map_or(usize::MAX, |index| self.output_offset[index]);
        let mut row = Vec::with_capacity(self.output_offset.len());
        for (position, offset) in self.output_offset.iter().copied().enumerate() {
            if self.phys_tbl_id_idx == Some(position) {
                row.push(Datum::Int(decode_table_id(key)?));
                continue;
            }
            let offset = if offset > phys_tbl_id_column_idx {
                offset - 1
            } else {
                offset
            };
            let encoded = values.get(offset).ok_or_else(|| {
                MemReaderError::Decode(format!("index entry has no column at offset {offset}"))
            })?;
            let datum = tidb_tablecodec::decode_column_value(
                encoded,
                &self.meta.types[offset],
                Some(&self.zone),
            )
            .map_err(|error| MemReaderError::Decode(format!("{error:?}")))?;
            row.push(datum);
        }
        Ok(row)
    }

    /// Whether a GLOBAL index entry belongs to a partition this read wants:
    /// Go `memRowsIterForIndex.Next` (:995).
    fn global_entry_is_wanted(&self, value: &[u8]) -> Result<bool, MemReaderError> {
        if !self.meta.global {
            return Ok(true);
        }
        let segments = tidb_tablecodec::split_index_value(value)
            .map_err(|error| MemReaderError::Decode(format!("{error:?}")))?;
        let Some(encoded) = segments.partition_id else {
            return Ok(true);
        };
        let (_, partition_id) = tidb_codec::decode_int(&encoded)
            .map_err(|error| MemReaderError::Decode(format!("{error:?}")))?;
        Ok(self.partition_id_map.contains(&partition_id))
    }

    /// Go `memIndexReader.getMemRows` (:152).
    pub fn get_mem_rows<S: MemBufferSource>(
        &mut self,
        source: &S,
        comparator: Option<&dyn RowComparator>,
    ) -> Result<Vec<Vec<Datum>>, MemReaderError> {
        let mut added_rows: Vec<Vec<Datum>> = Vec::new();
        let ranges = self.kv_ranges.clone();
        let desc = self.desc;
        {
            let reader = &*self;
            let mut collected: Vec<Vec<Datum>> = Vec::new();
            iter_txn_mem_buffer(source, &ranges, desc, |key, value| {
                collected.push(reader.decode_index_key_value(key, value)?);
                Ok(())
            })?;
            for row in collected {
                if admits(self.conditions.as_mut(), &row)? {
                    added_rows.push(row);
                }
            }
        }
        if self.keep_order && self.need_extra_sorting {
            sort_rows(&mut added_rows, comparator)?;
        }
        Ok(added_rows)
    }

    /// Go `memIndexReader.getMemRowsIter` (:100).
    pub fn get_mem_rows_iter<'a, S: MemBufferSource>(
        &'a mut self,
        source: &'a S,
        comparator: Option<&dyn RowComparator>,
    ) -> Result<Box<dyn MemRowsIter + 'a>, MemReaderError> {
        if self.keep_order && self.need_extra_sorting {
            let data = self.get_mem_rows(source, comparator)?;
            return Ok(Box::new(DefaultRowsIter::new(data)));
        }
        let kv_iter = TxnMemBufferIter::new(source, self.kv_ranges.clone(), self.desc);
        Ok(Box::new(MemRowsIterForIndex {
            kv_iter,
            reader: self,
        }))
    }

    /// Go `memIndexReader.getMemRowsHandle` (:652).
    ///
    /// The YEAR fixup (issue 41827) is kept: `DecodeIndexHandle` hands back an
    /// INT handle for a YEAR-typed clustered key, which a common-handle table
    /// must re-encode as a common handle before the record key can be built.
    pub fn get_mem_rows_handle<S: MemBufferSource>(
        &self,
        source: &S,
    ) -> Result<Vec<MemHandle>, MemReaderError> {
        let mut handles = Vec::new();
        iter_txn_mem_buffer(source, &self.kv_ranges, self.desc, |key, value| {
            let decoded = tidb_tablecodec::decode_index_handle(
                key_bytes(key),
                value,
                self.meta.index_columns,
            )
            .map_err(|error| MemReaderError::Decode(format!("{error:?}")))?;
            let mut handle = convert_handle(&decoded)?;
            if self.meta.common_handle {
                if let TableHandle::Int(value) = handle.handle {
                    let encoded =
                        tidb_codec::encode_key_in_timezone(&self.zone, &[Datum::Int(value)])
                            .map_err(|error| MemReaderError::Decode(format!("{error:?}")))?;
                    handle.handle = TableHandle::Common(encoded);
                }
            }
            if let Some(partition_id) = handle.partition_id {
                if !self.partition_id_map.contains(&partition_id) {
                    return Ok(());
                }
            }
            handles.push(handle);
            Ok(())
        })?;
        Ok(handles)
    }
}

/// Go `memRowsIterForIndex` (:978).
struct MemRowsIterForIndex<'a, S: MemBufferSource> {
    kv_iter: TxnMemBufferIter<'a, S>,
    reader: &'a mut MemIndexReader,
}

impl<S: MemBufferSource> MemRowsIter for MemRowsIterForIndex<'_, S> {
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, MemReaderError> {
        while self.kv_iter.valid() {
            let key = self.kv_iter.key().clone();
            let value = self.kv_iter.value().to_vec();
            self.kv_iter.advance()?;
            // The transaction deleted this index entry.
            if value.is_empty() {
                continue;
            }
            if !self.reader.global_entry_is_wanted(&value)? {
                continue;
            }
            let row = self.reader.decode_index_key_value(&key, &value)?;
            if !admits(self.reader.conditions.as_mut(), &row)? {
                continue;
            }
            return Ok(Some(row));
        }
        Ok(None)
    }

    fn close(&mut self) {
        self.kv_iter.close();
    }
}

/// Go `memIndexLookUpReader` (:692): read the index's added entries to get
/// handles, then read the table's added rows at those handles.
pub struct MemIndexLookUpReader {
    /// Go `idxReader`, whose `outputOffset` is `[len(index.Columns)]` so it
    /// decodes ONLY the handle (:713).
    index_reader: MemIndexReader,
    /// Go `groupedKVRanges`: one range group per partition, or a single
    /// group with the table's own physical id.
    grouped_kv_ranges: Vec<(i64, Vec<MemKeyRange>)>,
    desc: bool,
}

impl MemIndexLookUpReader {
    /// Go `buildMemIndexLookUpReader` (:711).
    #[must_use]
    pub fn new(
        index_reader: MemIndexReader,
        grouped_kv_ranges: Vec<(i64, Vec<MemKeyRange>)>,
        desc: bool,
    ) -> Self {
        MemIndexLookUpReader {
            index_reader,
            grouped_kv_ranges,
            desc,
        }
    }

    /// Go `memIndexLookUpReader.getMemRowsIter` (:744), up to the point where
    /// it hands off to a freshly built `memTableReader`.
    ///
    /// Returns the record ranges to read, or `None` for Go's "no handles at
    /// all" short circuit, which returns an EMPTY `defaultRowsIter` (:772)
    /// rather than reading the table.
    pub fn table_kv_ranges<S: MemBufferSource>(
        &mut self,
        source: &S,
        encoder: &dyn HandleRangeEncoder,
    ) -> Result<Option<Vec<MemKeyRange>>, MemReaderError> {
        let mut table_ranges = Vec::new();
        let mut handle_count = 0usize;
        for (physical_table_id, ranges) in self.grouped_kv_ranges.clone() {
            self.index_reader.set_kv_ranges(ranges);
            let handles = self.index_reader.get_mem_rows_handle(source)?;
            if handles.is_empty() {
                continue;
            }
            handle_count += handles.len();
            table_ranges.extend(encoder.table_handles_to_kv_ranges(physical_table_id, &handles)?);
        }
        if handle_count == 0 {
            return Ok(None);
        }
        if self.desc {
            table_ranges.reverse();
        }
        Ok(Some(table_ranges))
    }

    /// Go `memIndexLookUpReader.getMemRowsHandle` (:805): an error in Go too.
    pub fn get_mem_rows_handle(&self) -> Result<Vec<MemHandle>, MemReaderError> {
        Err(MemReaderError::Unsupported(
            "getMemRowsHandle has not been implemented for memIndexLookUpReader",
        ))
    }
}

/// One partial reader of an index merge: Go's `memReader` interface (:41),
/// which only ever holds a `memTableReader` or a `memIndexReader` (:1054).
pub enum PartialMemReader {
    /// A partial plan that is a table scan. Boxed only because the two
    /// readers differ greatly in size.
    Table(Box<MemTableReader>),
    /// A partial plan that is an index scan.
    Index(Box<MemIndexReader>),
}

impl PartialMemReader {
    fn set_kv_ranges(&mut self, ranges: Vec<MemKeyRange>) {
        match self {
            PartialMemReader::Table(reader) => reader.set_kv_ranges(ranges),
            PartialMemReader::Index(reader) => reader.set_kv_ranges(ranges),
        }
    }

    fn get_mem_rows_handle<S: MemBufferSource>(
        &self,
        source: &S,
    ) -> Result<Vec<MemHandle>, MemReaderError> {
        match self {
            PartialMemReader::Table(reader) => reader.get_mem_rows_handle(source),
            PartialMemReader::Index(reader) => reader.get_mem_rows_handle(source),
        }
    }
}

/// Go `memIndexMergeReader` (:809).
pub struct MemIndexMergeReader {
    /// Go `memReaders`, one per partial plan.
    partial_readers: Vec<PartialMemReader>,
    /// Go `partialWorkerKVRanges`: per partial reader, the range groups it
    /// reads, each tagged with its physical table id.
    partial_kv_ranges: Vec<Vec<(i64, Vec<MemKeyRange>)>>,
    /// Go `isIntersection`.
    is_intersection: bool,
    /// Go `partitionMode`.
    partition_mode: bool,
    /// Go `keepOrder`.
    keep_order: bool,
}

impl MemIndexMergeReader {
    /// Go `buildMemIndexMergeReader` (:826), minus the per-partial reader
    /// construction the caller has already done.
    #[must_use]
    pub fn new(
        partial_readers: Vec<PartialMemReader>,
        partial_kv_ranges: Vec<Vec<(i64, Vec<MemKeyRange>)>>,
        is_intersection: bool,
        partition_mode: bool,
        keep_order: bool,
    ) -> Self {
        MemIndexMergeReader {
            partial_readers,
            partial_kv_ranges,
            is_intersection,
            partition_mode,
            keep_order,
        }
    }

    /// Go `memIndexMergeReader.getHandles` (:1044): the index-merge algebra
    /// over the ADDED rows.
    ///
    /// Each partial reader contributes its handles; a handle's count is how
    /// many partial readers produced it. A UNION keeps every handle, an
    /// INTERSECTION keeps only those seen by every partial reader. Go's
    /// `kv.NewHandleMap` iterates in handle order, and so does the
    /// [`BTreeMap`] here.
    ///
    /// Note the counting quirk Go has and this keeps: the count is per
    /// `(reader, range-group)` OCCURRENCE, so a handle a single partial
    /// reader produced twice counts twice. That only matters for the
    /// intersection test, and a well-formed index cannot produce the same
    /// handle twice for one partial plan.
    pub fn get_handles<S: MemBufferSource>(
        &mut self,
        source: &S,
    ) -> Result<Vec<MemHandle>, MemReaderError> {
        let mut counts: BTreeMap<MemHandle, usize> = BTreeMap::new();
        for (position, reader) in self.partial_readers.iter_mut().enumerate() {
            let groups = self
                .partial_kv_ranges
                .get(position)
                .cloned()
                .unwrap_or_default();
            for (physical_table_id, ranges) in groups {
                reader.set_kv_ranges(ranges);
                for mut handle in reader.get_mem_rows_handle(source)? {
                    if handle.partition_id.is_none() && self.partition_mode {
                        handle.partition_id = Some(physical_table_id);
                    }
                    *counts.entry(handle).or_insert(0) += 1;
                }
            }
        }

        let readers = self.partial_readers.len();
        let mut handles = Vec::new();
        for (handle, count) in counts {
            if self.is_intersection && count != readers {
                continue;
            }
            handles.push(handle);
        }
        Ok(handles)
    }

    /// Go `memIndexMergeReader.getMemRows` (:1091) up to the record-range
    /// hand-off.
    ///
    /// Go passes `0` as the physical id when `partitionMode`, because the
    /// partition handles already carry theirs.
    pub fn table_kv_ranges<S: MemBufferSource>(
        &mut self,
        source: &S,
        physical_table_id: i64,
        encoder: &dyn HandleRangeEncoder,
    ) -> Result<Option<Vec<MemKeyRange>>, MemReaderError> {
        let handles = self.get_handles(source)?;
        if handles.is_empty() {
            return Ok(None);
        }
        let id = if self.partition_mode {
            0
        } else {
            physical_table_id
        };
        Ok(Some(encoder.table_handles_to_kv_ranges(id, &handles)?))
    }

    /// Go's tail of `getMemRows` (:1132): the index-merge read sorts its rows
    /// whenever `keepOrder`, WITHOUT setting `keepOrder` on the table reader
    /// it built -- non-partitioned tables need reordering here too, because
    /// the handles came out of a handle map rather than out of the index.
    pub fn sort_rows_if_needed(
        &self,
        rows: &mut [Vec<Datum>],
        comparator: Option<&dyn RowComparator>,
    ) -> Result<(), MemReaderError> {
        if !self.keep_order {
            return Ok(());
        }
        sort_rows(rows, comparator)
    }

    /// Go `memIndexMergeReader.getMemRowsHandle` (:1146): an error in Go too.
    pub fn get_mem_rows_handle(&self) -> Result<Vec<MemHandle>, MemReaderError> {
        Err(MemReaderError::Unsupported(
            "getMemRowsHandle has not been implemented for memIndexMergeReader",
        ))
    }
}

/// Go `expression.EvalBool(evalCtx, m.conditions, mutableRow.ToRow())`: no
/// conditions admits every row.
fn admits(conditions: Option<&mut ScanFilterProbe>, row: &[Datum]) -> Result<bool, MemReaderError> {
    match conditions {
        None => Ok(true),
        Some(filter) => Ok(filter.admits(row)?),
    }
}

/// Go's `slices.SortFunc(m.addedRows, m.compare)` calls (:177, :459, :1133).
///
/// Go records the LAST comparator error and still returns the (partly sorted)
/// rows; a fallible comparator cannot drive `slice::sort_by` in Rust, so the
/// error is raised instead. That is the one behavior difference, and it can
/// only be reached on a comparison that Go would also have reported.
fn sort_rows(
    rows: &mut [Vec<Datum>],
    comparator: Option<&dyn RowComparator>,
) -> Result<(), MemReaderError> {
    let Some(comparator) = comparator else {
        return Ok(());
    };
    let mut failure = None;
    rows.sort_by(|left, right| match comparator.compare(left, right) {
        Ok(ordering) => ordering,
        Err(error) => {
            failure = Some(error);
            Ordering::Equal
        }
    });
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Go `tablecodec.DecodeKeyHead(key)`, of which only the table id is used
/// (:213).
fn decode_table_id(key: &[u8]) -> Result<i64, MemReaderError> {
    let encoded = key
        .get(1..9)
        .ok_or_else(|| MemReaderError::Decode("key is too short to carry a table id".to_owned()))?;
    let (_, table_id) = tidb_codec::decode_int(encoded)
        .map_err(|error| MemReaderError::Decode(format!("{error:?}")))?;
    Ok(table_id)
}

/// Bridges [`tidb_txnkv::Handle`] (what tablecodec decodes) to this crate's
/// [`TableHandle`] plus Go's `kv.PartitionHandle` partition id.
fn convert_handle(handle: &KvHandle) -> Result<MemHandle, MemReaderError> {
    match handle {
        KvHandle::Int(value) => Ok(MemHandle::plain(TableHandle::Int(value.value()))),
        KvHandle::Common(value) => Ok(MemHandle::plain(TableHandle::Common(
            value.encoded().to_vec(),
        ))),
        KvHandle::Partition(partition) => {
            let inner = convert_handle(partition.inner())?;
            Ok(MemHandle::in_partition(
                partition.partition_id(),
                inner.handle,
            ))
        }
    }
}

/// The raw bytes of a key, for the decoders that take `&[u8]`.
fn key_bytes(key: &Key) -> &[u8] {
    key.as_ref()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A [`MemBufferSource`] over a sorted map, with an optional second source
    /// standing in for Go's `cacheTable`/`TemporaryTableData`.
    ///
    /// WRITTEN test support: Go's own coverage of these readers runs entirely
    /// through `testkit` against a real transaction (`pkg/executor/union_scan_test.go`),
    /// which has no dependency-closed counterpart here.
    #[derive(Default)]
    struct TestBuffer {
        dirty: BTreeMap<Key, Vec<u8>>,
        cache: Option<BTreeMap<Key, Vec<u8>>>,
    }

    fn key(bytes: &[u8]) -> Key {
        Key::from_bytes(bytes.to_vec())
    }

    fn collect(map: &BTreeMap<Key, Vec<u8>>, start: &Key, end: &Key, reverse: bool) -> VecScanIter {
        let mut entries: Vec<(Key, Vec<u8>)> = map
            .range(start.clone()..end.clone())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if reverse {
            entries.reverse();
        }
        VecScanIter::new(entries)
    }

    impl MemBufferSource for TestBuffer {
        fn snapshot_iter(
            &self,
            start: &Key,
            end: &Key,
            reverse: bool,
        ) -> Result<Box<dyn ScanIter>, MemReaderError> {
            Ok(Box::new(collect(&self.dirty, start, end, reverse)))
        }

        fn snap_cache_iter(
            &self,
            start: &Key,
            end: &Key,
            reverse: bool,
        ) -> Result<Option<Box<dyn ScanIter>>, MemReaderError> {
            Ok(self
                .cache
                .as_ref()
                .map(|cache| Box::new(collect(cache, start, end, reverse)) as Box<dyn ScanIter>))
        }
    }

    fn drain(
        source: &TestBuffer,
        ranges: &[MemKeyRange],
        reverse: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut seen = Vec::new();
        iter_txn_mem_buffer(source, ranges, reverse, |key, value| {
            seen.push((key_bytes(key).to_vec(), value.to_vec()));
            Ok(())
        })
        .unwrap();
        seen
    }

    fn whole_range() -> Vec<MemKeyRange> {
        vec![(key(b"a"), key(b"z"))]
    }

    /// The overlay case the file exists for: within one transaction a row is
    /// ADDED, another is MODIFIED, and a third is DELETED.
    #[test]
    fn added_and_modified_rows_are_returned_and_deleted_rows_are_skipped() {
        let mut dirty = BTreeMap::new();
        dirty.insert(key(b"k1"), b"added".to_vec());
        dirty.insert(key(b"k2"), b"modified".to_vec());
        // A DELETE writes the key with an EMPTY value.
        dirty.insert(key(b"k3"), Vec::new());
        let source = TestBuffer { dirty, cache: None };

        assert_eq!(
            drain(&source, &whole_range(), false),
            vec![
                (b"k1".to_vec(), b"added".to_vec()),
                (b"k2".to_vec(), b"modified".to_vec()),
            ],
            "the tombstone at k3 is never handed to the row decoder"
        );
    }

    /// The streaming cursor must apply the same tombstone rule as the batch
    /// walk, since `UnionScanExec` may take either path.
    #[test]
    fn the_streaming_cursor_skips_tombstones_too() {
        let mut dirty = BTreeMap::new();
        dirty.insert(key(b"k1"), Vec::new());
        dirty.insert(key(b"k2"), b"live".to_vec());
        let source = TestBuffer { dirty, cache: None };

        let mut iter = TxnMemBufferIter::new(&source, whole_range(), false);
        let mut live = Vec::new();
        while iter.valid() {
            let value = iter.value().to_vec();
            let seen = key_bytes(iter.key()).to_vec();
            iter.advance().unwrap();
            if value.is_empty() {
                continue;
            }
            live.push((seen, value));
        }
        iter.close();
        assert_eq!(live, vec![(b"k2".to_vec(), b"live".to_vec())]);
    }

    /// Go's union puts the buffer OVER the cached/temporary-table source, so
    /// a key written by this transaction wins, and a key it deleted is gone
    /// even though the cache still has it.
    #[test]
    fn the_buffer_shadows_the_cached_table_source() {
        let mut dirty = BTreeMap::new();
        dirty.insert(key(b"k2"), b"dirty".to_vec());
        dirty.insert(key(b"k3"), Vec::new());
        let mut cache = BTreeMap::new();
        cache.insert(key(b"k1"), b"cached1".to_vec());
        cache.insert(key(b"k2"), b"cached2".to_vec());
        cache.insert(key(b"k3"), b"cached3".to_vec());
        let source = TestBuffer {
            dirty,
            cache: Some(cache),
        };

        assert_eq!(
            drain(&source, &whole_range(), false),
            vec![
                (b"k1".to_vec(), b"cached1".to_vec()),
                (b"k2".to_vec(), b"dirty".to_vec()),
            ],
            "k2 takes the transaction's value and k3's deletion hides the cached row"
        );
    }

    /// Ranges are walked in list order, and each range in the requested
    /// direction -- the property `buildMem*Reader`'s `slices.Reverse` of the
    /// range list depends on.
    #[test]
    fn ranges_are_walked_in_order_and_each_range_honors_the_direction() {
        let mut dirty = BTreeMap::new();
        for name in [&b"a1"[..], b"a2", b"b1", b"b2"] {
            dirty.insert(key(name), name.to_vec());
        }
        let source = TestBuffer { dirty, cache: None };
        let ascending = vec![(key(b"a"), key(b"b")), (key(b"b"), key(b"c"))];
        let keys: Vec<Vec<u8>> = drain(&source, &ascending, false)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(
            keys,
            vec![
                b"a1".to_vec(),
                b"a2".to_vec(),
                b"b1".to_vec(),
                b"b2".to_vec()
            ]
        );

        let descending = vec![(key(b"b"), key(b"c")), (key(b"a"), key(b"b"))];
        let keys: Vec<Vec<u8>> = drain(&source, &descending, true)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(
            keys,
            vec![
                b"b2".to_vec(),
                b"b1".to_vec(),
                b"a2".to_vec(),
                b"a1".to_vec()
            ],
            "a descending read reverses both the range list and each range"
        );
    }

    /// An empty range list and a range with no entries both yield nothing
    /// without opening a cursor the caller can read from.
    #[test]
    fn an_empty_read_produces_no_rows() {
        let source = TestBuffer::default();
        assert!(drain(&source, &whole_range(), false).is_empty());
        assert!(drain(&source, &[], false).is_empty());

        let mut iter = TxnMemBufferIter::new(&source, whole_range(), false);
        assert!(!iter.valid());
        iter.close();
    }

    fn int(value: i64) -> MemHandle {
        MemHandle::plain(TableHandle::Int(value))
    }

    /// Go's union keeps every handle any partial plan produced; the result is
    /// in handle order because `kv.HandleMap` iteration is.
    #[test]
    fn an_index_merge_union_keeps_every_handle_in_handle_order() {
        let handles = merge_handles(&[vec![int(3), int(1)], vec![int(2), int(3)]], false, false);
        assert_eq!(handles, vec![int(1), int(2), int(3)]);
    }

    /// An intersection keeps only the handles EVERY partial plan produced.
    #[test]
    fn an_index_merge_intersection_keeps_only_shared_handles() {
        let handles = merge_handles(&[vec![int(1), int(2)], vec![int(2), int(3)]], true, false);
        assert_eq!(handles, vec![int(2)]);
    }

    /// In partition mode a bare handle picks up the range group's physical id,
    /// which is what keeps two partitions' identical row ids distinct.
    #[test]
    fn partition_mode_tags_bare_handles_with_the_physical_table_id() {
        let handles = merge_handles(&[vec![int(7)]], false, true);
        assert_eq!(
            handles,
            vec![MemHandle::in_partition(100, TableHandle::Int(7))]
        );
    }

    /// Reproduces `getHandles` over stubbed partial readers, since building
    /// real ones needs encoded index entries.
    fn merge_handles(
        per_reader: &[Vec<MemHandle>],
        is_intersection: bool,
        partition_mode: bool,
    ) -> Vec<MemHandle> {
        let mut counts: BTreeMap<MemHandle, usize> = BTreeMap::new();
        for reader_handles in per_reader {
            for handle in reader_handles {
                let mut handle = handle.clone();
                if handle.partition_id.is_none() && partition_mode {
                    handle.partition_id = Some(100);
                }
                *counts.entry(handle).or_insert(0) += 1;
            }
        }
        let readers = per_reader.len();
        counts
            .into_iter()
            .filter(|(_, count)| !is_intersection || *count == readers)
            .map(|(handle, _)| handle)
            .collect()
    }

    struct ByFirstColumn;

    impl RowComparator for ByFirstColumn {
        fn compare(&self, left: &[Datum], right: &[Datum]) -> Result<Ordering, MemReaderError> {
            match (&left[0], &right[0]) {
                (Datum::Int(a), Datum::Int(b)) => Ok(a.cmp(b)),
                _ => Err(MemReaderError::Unsupported("only int rows are compared")),
            }
        }
    }

    /// `keepOrder && needExtraSorting` re-sorts the collected rows; without a
    /// comparator the buffer order is kept unchanged.
    #[test]
    fn extra_sorting_orders_the_collected_rows() {
        let mut rows = vec![
            vec![Datum::Int(3)],
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
        ];
        sort_rows(&mut rows, Some(&ByFirstColumn)).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(2)],
                vec![Datum::Int(3)]
            ]
        );

        let mut unchanged = vec![vec![Datum::Int(3)], vec![Datum::Int(1)]];
        sort_rows(&mut unchanged, None).unwrap();
        assert_eq!(unchanged, vec![vec![Datum::Int(3)], vec![Datum::Int(1)]]);
    }

    /// `defaultRowsIter` hands rows out once, in order, then reports the end.
    #[test]
    fn the_default_rows_iterator_drains_once() {
        let mut iter = DefaultRowsIter::new(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]);
        assert_eq!(iter.next_row().unwrap(), Some(vec![Datum::Int(1)]));
        assert_eq!(iter.next_row().unwrap(), Some(vec![Datum::Int(2)]));
        assert_eq!(iter.next_row().unwrap(), None);
        iter.close();
    }
}
