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

//! Go `pkg/store/mockstore/unistore/cophandler/mpp_exec.go`: the executors
//! every coprocessor DAG runs through (`handleCopDAGRequest` ->
//! `buildAndRunMPPExecutor`). Each one is opened, pulled chunk by chunk
//! (`next`) and stopped; each counts the rows it produced in its own
//! `execDetail`, which `genRespWithMPPExec` answers as the executor's
//! execution summary.
//!
//! # Narrowings, by name
//!
//! * `*chunk.Chunk`: a chunk is its rows of datums; "full" is Go's
//!   `NumRows() >= capacity` for the `DefaultBatchSize` capacity every
//!   executor allocates.
//! * `tableScanExec`'s scan goroutine and its one-slot result channel run
//!   cooperatively on the consumer's thread, in the order Go's scheduler runs
//!   them: the woken goroutine waits in the consumer's `runnext` slot until
//!   the consumer blocks (a receive on the empty channel, or `stop`'s
//!   `wg.Wait`). That order decides how many rows the goroutine processed
//!   before a `limitExec` above it stops pulling (`rowCnt`, the response's
//!   processed versions).
//! * `selExec` filters row by row through the request's conditions rather
//!   than `expression.VectorizedFilter`'s column-at-a-time evaluation.
//! * `indexLookUpExec`, `joinExec`, `exchSenderExec`, `exchRecvExec` and
//!   `expandExec` are MPP-only or need region-located extra readers; their
//!   builders refuse by name (`mpp.rs`).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use tidb_datatype::{Datum, FieldType, SessionTimeZone};
use tidb_proto::coprocessor;
use tidb_proto::tipb;
use tidb_tablecodec::table_key::RecordHandle;
use tidb_unistore_dbreader::{DbReader, KvPair, ScanReq};

use crate::eval_context::RequestEvalContext;
use crate::{condition_holds, CopExecError, RegionAggregator, SimpleExpr, TopNSpec};

/// Go `DefaultBatchSize`: the capacity of every chunk an executor allocates.
pub(crate) const DEFAULT_BATCH_SIZE: usize = 32;

/// One decoded row.
pub(crate) type Row = Vec<Datum>;

/// Go `*chunk.Chunk` as the executors hand it up: its rows.
pub(crate) type Chunk = Vec<Row>;

/// Go `chunk.IsFull` for a `NewChunkWithCapacity(fieldTypes, DefaultBatchSize)`.
fn is_full(chk: &Chunk) -> bool {
    chk.len() >= DEFAULT_BATCH_SIZE
}

/// Go `kv.Key.Next`: the key followed by a zero byte.
fn key_next(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0);
    next
}

/// The `error` an executor returns, by how `genRespWithMPPExec` answers it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MppExecError {
    /// An expression or executor error `toPBError` encodes into
    /// `SelectResponse.error`.
    Exec(CopExecError),
    /// Go `*ErrLocked` (`checkRangeLockForRange`): `SelectResponse.error`
    /// plus the response's `Locked`.
    Locked(Box<tidb_proto::kvrpcpb::LockInfo>),
    /// Go `*kverrors.ErrConflict` (an RC check-ts read meeting a newer
    /// write): `SelectResponse.error` plus the response's `OtherError`, whose
    /// "write conflict" text the client turns into `ErrWriteConflict`.
    Conflict(String),
    /// Any other store or decode failure: `SelectResponse.error` (code 1 and
    /// its text); one carrying `ErrExecutorNotSupportedMsg` answers as the
    /// response's `OtherError`.
    Other(String),
}

impl From<CopExecError> for MppExecError {
    fn from(error: CopExecError) -> Self {
        MppExecError::Exec(error)
    }
}

/// Go `execDetail` (`closure_exec.go`): what one executor reports in its
/// execution summary. The MPP executors only ever count rows
/// (`updateOnlyRows`), so the processed time and iterations stay zero.
#[derive(Debug, Default)]
pub(crate) struct ExecDetail {
    time_processed_ns: u64,
    num_produced_rows: u64,
    num_iterations: u64,
}

impl ExecDetail {
    /// Go `execDetail.updateOnlyRows`.
    pub(crate) fn update_only_rows(&mut self, got_row: usize) {
        self.num_produced_rows += got_row as u64;
    }

    /// Go `execDetail.buildSummary`.
    pub(crate) fn build_summary(&self) -> tipb::ExecutorExecutionSummary {
        tipb::ExecutorExecutionSummary {
            time_processed_ns: Some(self.time_processed_ns),
            num_produced_rows: Some(self.num_produced_rows),
            num_iterations: Some(self.num_iterations),
            ..tipb::ExecutorExecutionSummary::default()
        }
    }
}

/// Go `buildAndRunMPPExecutor`'s `counts` and `ndvs`: per request range, the
/// rows and distinct index values the scan read
/// (`DAGRequest.collect_range_counts`).
#[derive(Debug, Default)]
pub(crate) struct RangeCounts {
    pub(crate) counts: Vec<i64>,
    pub(crate) ndvs: Vec<i64>,
}

/// Go's `*coprocessor.KeyRange` the scans update for the paging protocol.
pub(crate) type SharedPaging = Rc<RefCell<coprocessor::KeyRange>>;

/// Go `baseMPPExec`: the session context, children, output field types and
/// execution detail every executor carries.
pub(crate) struct BaseMppExec<'a> {
    pub(crate) sctx: Arc<RequestEvalContext>,
    pub(crate) children: Vec<Box<dyn MppExec<'a> + 'a>>,
    pub(crate) field_types: Vec<FieldType>,
    pub(crate) exec_summary: ExecDetail,
}

/// Go `mppExec`.
pub(crate) trait MppExec<'a> {
    fn base(&self) -> &BaseMppExec<'a>;
    fn base_mut(&mut self) -> &mut BaseMppExec<'a>;
    fn open(&mut self) -> Result<(), MppExecError>;
    /// `Ok(None)` is Go's nil chunk.
    fn next(&mut self) -> Result<Option<Chunk>, MppExecError>;

    /// Go `baseMPPExec.stop`: stop every child.
    fn stop(&mut self) -> Result<(), MppExecError> {
        for child in &mut self.base_mut().children {
            child.stop()?;
        }
        Ok(())
    }

    /// Go `getChildren`.
    fn get_children<'s>(&'s self) -> Vec<&'s (dyn MppExec<'a> + 'a)>
    where
        'a: 's,
    {
        self.base().children.iter().map(|child| child.as_ref()).collect()
    }

    /// Go `getFieldTypes`.
    fn get_field_types<'s>(&'s self) -> &'s [FieldType]
    where
        'a: 's,
    {
        &self.base().field_types
    }

    /// Go `baseMPPExec.buildSummary`.
    fn build_summary(&self) -> tipb::ExecutorExecutionSummary {
        self.base().exec_summary.build_summary()
    }

    /// Go `genRespWithMPPExec`'s type switch over `*tableScanExec` and
    /// `*indexScanExec`: the scan's `rowCnt`, every version it processed.
    fn scan_row_cnt(&self) -> Option<i64> {
        None
    }
}

/// Go `checkRangeLockForRange` over a range's scanned pairs: the first lock
/// in key order answers. The store reports a blocking lock as the pair's
/// error; a descending scan lists the pairs from the top, so its first lock
/// in key order is the last one listed.
fn check_range_lock(pairs: &[KvPair], desc: bool) -> Result<(), MppExecError> {
    let lock = |pair: &KvPair| match pair.error.as_deref() {
        Some(tidb_unistore_kverrors::KvError::Locked(lock)) => Some(lock.clone()),
        _ => None,
    };
    let first = if desc {
        pairs.iter().rev().find_map(lock)
    } else {
        pairs.iter().find_map(lock)
    };
    match first {
        Some(lock) => Err(MppExecError::Locked(lock)),
        None => Ok(()),
    }
}

/// `DBReader.Scan` / `ReverseScan` over one range: Go's reverse contract
/// hands the bounds swapped (`start_key` the upper bound) and the store
/// swaps them back.
fn scan_range(
    db_reader: &dyn DbReader,
    range: &coprocessor::KeyRange,
    start_ts: u64,
    desc: bool,
) -> Vec<KvPair> {
    let (start_key, end_key) = if desc {
        (range.end.clone(), range.start.clone())
    } else {
        (range.start.clone(), range.end.clone())
    };
    db_reader.scan(&ScanReq {
        start_key,
        end_key,
        limit: u32::MAX,
        version: start_ts,
        sample_step: 0,
        reverse: desc,
    })
}

/// A pair's non-lock store error, as `DBReader.Scan` returns it.
fn pair_error(pair: &KvPair) -> Option<MppExecError> {
    pair.error.as_deref().map(|error| match error {
        tidb_unistore_kverrors::KvError::Locked(lock) => MppExecError::Locked(lock.clone()),
        conflict @ tidb_unistore_kverrors::KvError::Conflict { .. } => {
            MppExecError::Conflict(conflict.to_string())
        }
        other => MppExecError::Other(other.to_string()),
    })
}

/// Go `newRowDecoder` (`cop_handler.go`) and the `rowcodec.ChunkDecoder` it
/// builds: a row value decoded into the requested columns in request order.
///
/// A CLUSTERED table's primary key lives in the row KEY: when no column
/// carries `PkHandle` the decoder falls back to the request's
/// `PrimaryColumnIds` and fills those columns from the handle's datums,
/// matched positionally. Narrowing, by name: a partitioned record handle
/// refuses.
pub(crate) struct TableRowDecoder {
    columns: Vec<tipb::ColumnInfo>,
    column_types: BTreeMap<i64, FieldType>,
    primary_column_ids: Vec<i64>,
    time_zone: SessionTimeZone,
    /// Go `tableScanExec.physTblIDColIdx`: the `ExtraPhysTblID` column,
    /// filled from the key.
    phys_tbl_id_col_idx: Option<usize>,
}

impl TableRowDecoder {
    pub(crate) fn new(
        columns: &[tipb::ColumnInfo],
        primary_column_ids: &[i64],
        time_zone: SessionTimeZone,
    ) -> Self {
        const EXTRA_PHYSICAL_TABLE_ID: i64 = -3;
        let mut column_types = BTreeMap::new();
        let mut phys_tbl_id_col_idx = None;
        for (offset, column) in columns.iter().enumerate() {
            if column.column_id() == EXTRA_PHYSICAL_TABLE_ID {
                phys_tbl_id_col_idx = Some(offset);
            }
            column_types.insert(column.column_id(), crate::field_type_from_pb_column(column));
        }
        Self {
            columns: columns.to_vec(),
            column_types,
            primary_column_ids: primary_column_ids.to_vec(),
            time_zone,
            phys_tbl_id_col_idx,
        }
    }

    /// Go `tablecodec.DecodeRowKey` + `ChunkDecoder.DecodeToChunk` (+ the
    /// physical table id): one row of the requested columns.
    fn decode_to_row(&self, key: &[u8], value: &[u8]) -> Result<Row, MppExecError> {
        let other = |message: String| MppExecError::Other(message);
        // Go `tablecodec.DecodeRowKey`: an int handle answers directly, a
        // common handle carries the clustered primary key's datums.
        let (handle, common_handle) = match tidb_tablecodec::table_key::decode_row_key(key) {
            Ok(RecordHandle::Int(handle)) => (handle, None),
            Ok(RecordHandle::Common(encoded)) => {
                let count = self.primary_column_ids.len();
                match tidb_codec::decode(&encoded, count) {
                    Ok(datums) => (0, Some(datums)),
                    Err(err) => return Err(other(format!("invalid common handle: {err:?}"))),
                }
            }
            Ok(RecordHandle::Partition { .. }) => {
                return Err(other("partitioned scans are a later course of this port".to_owned()))
            }
            Err(err) => return Err(other(format!("invalid record key: {err:?}"))),
        };
        let decoded = tidb_tablecodec::decode_table_row_to_map(
            value,
            &self.column_types,
            Some(&self.time_zone),
        )
        .map_err(|err| other(format!("decode row failed: {err:?}")))?;
        let mut row = Vec::with_capacity(self.columns.len());
        for (offset, column) in self.columns.iter().enumerate() {
            // Go `ChunkDecoder.DecodeToChunk`: a non-NULL value in the row
            // wins; the handle is consulted "only when there is no
            // corresponding column in the value", because the handle may
            // hold a prefix or a collation sort key rather than the data.
            let stored = decoded.get(&column.column_id());
            if self.phys_tbl_id_col_idx == Some(offset) {
                row.push(Datum::Int(tidb_tablecodec::table_key::decode_table_id(key)));
            } else if let Some(datum) = stored.filter(|datum| !matches!(datum, Datum::Null)) {
                row.push(datum.clone());
            } else if column.pk_handle() {
                row.push(Datum::Int(handle));
            } else if let Some(datum) = common_handle.as_ref().and_then(|datums| {
                // Go `tryAppendHandleColumn` matches `PrimaryColumnIds`
                // positionally against the handle's datums (encoded in key
                // order), and refuses a column whose key bytes are not its
                // value (`NeedRestoredData`).
                let position = self
                    .primary_column_ids
                    .iter()
                    .position(|id| *id == column.column_id())?;
                if self.column_types[&column.column_id()].need_restored_data() {
                    return None;
                }
                datums.get(position)
            }) {
                // ... and decodes it with the column's type
                // (`DecodeOne(..., col.Ft)`): a DATETIME key part is a
                // packed integer until it is restored to its time.
                let datum = tidb_tablecodec::unflatten_datum(
                    datum.clone(),
                    &self.column_types[&column.column_id()],
                    Some(&self.time_zone),
                )
                .map_err(|err| other(format!("decode common handle failed: {err:?}")))?;
                row.push(datum);
            } else if stored.is_some() {
                row.push(Datum::Null);
            } else if !column.default_val().is_empty() {
                // Go's decoder default: a row written before the column
                // existed answers the encoded origin default.
                match tidb_codec::decode(column.default_val(), 1) {
                    Ok(mut datums) if !datums.is_empty() => row.push(datums.remove(0)),
                    _ => {
                        return Err(other(format!(
                            "invalid default value bytes for column {}",
                            column.column_id()
                        )))
                    }
                }
            } else {
                row.push(Datum::Null);
            }
        }
        Ok(row)
    }
}

/// Go `scanResult`: what the scan goroutine sends over `tableScanExec.result`.
#[derive(Default)]
struct ScanResult {
    chk: Option<Chunk>,
    last_processed_key: Option<Vec<u8>>,
    err: Option<MppExecError>,
}

/// Where `tableScanExec`'s scan goroutine is suspended.
enum ScanWorker {
    /// `wg.Run` started it; it has not run yet.
    NotStarted,
    /// About to process the next key.
    Running,
    /// In `Process`'s full-chunk `select { case e.result <- ...: case <-e.done: }`.
    ChunkSend(ScanResult),
    /// That send completed: next is `e.chk = NewChunk...` and the `done` check.
    AfterChunkSend,
    /// In the last chunk's `select { case e.result <- ...: case <-e.done: }`.
    FinalSend(ScanResult),
    /// In `e.result <- scanResult{err: err}`.
    ErrSend(ScanResult),
    /// Returned; `defer close(e.result)` ran.
    Exited,
}

/// How one send on `tableScanExec.result` went.
enum SendOutcome {
    Sent,
    Blocked(ScanResult),
    DoneChosen(ScanResult),
}

/// Go `tableScanExec`: a goroutine scans the ranges into `DefaultBatchSize`
/// chunks and sends each full one over a one-slot channel; `next` receives.
pub(crate) struct TableScanExec<'a> {
    base: BaseMppExec<'a>,
    kv_ranges: Vec<coprocessor::KeyRange>,
    start_ts: u64,
    db_reader: &'a dyn DbReader,
    counts: Option<Rc<RefCell<RangeCounts>>>,
    row_cnt: i64,
    decoder: TableRowDecoder,
    desc: bool,
    paging: Option<SharedPaging>,

    /// Each range's pairs, read at `open`.
    range_pairs: Vec<Vec<KvPair>>,
    range_idx: usize,
    pair_idx: usize,
    old_cnt: i64,
    chk: Chunk,
    worker: Option<ScanWorker>,
    /// `e.result`'s one buffered slot, and whether it is closed.
    result: Option<ScanResult>,
    result_closed: bool,
    /// Whether `e.done` is closed.
    done: bool,
}

impl<'a> TableScanExec<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        sctx: Arc<RequestEvalContext>,
        field_types: Vec<FieldType>,
        kv_ranges: Vec<coprocessor::KeyRange>,
        start_ts: u64,
        db_reader: &'a dyn DbReader,
        counts: Option<Rc<RefCell<RangeCounts>>>,
        decoder: TableRowDecoder,
        desc: bool,
        paging: Option<SharedPaging>,
    ) -> Self {
        Self {
            base: BaseMppExec {
                sctx,
                children: Vec::new(),
                field_types,
                exec_summary: ExecDetail::default(),
            },
            kv_ranges,
            start_ts,
            db_reader,
            counts,
            row_cnt: 0,
            decoder,
            desc,
            paging,
            range_pairs: Vec::new(),
            range_idx: 0,
            pair_idx: 0,
            old_cnt: 0,
            chk: Vec::new(),
            worker: None,
            result: None,
            result_closed: false,
            done: false,
        }
    }

    /// Go `tableScanExec.Process`'s decode half: the row joins `e.chk`.
    fn process(&mut self, pair_idx: usize) -> Result<Vec<u8>, MppExecError> {
        let pair = &self.range_pairs[self.range_idx][pair_idx];
        if let Some(error) = pair_error(pair) {
            return Err(error);
        }
        let row = self.decoder.decode_to_row(&pair.key, &pair.value)?;
        let key = pair.key.clone();
        self.chk.push(row);
        self.row_cnt += 1;
        Ok(key)
    }

    /// The range loop's bookkeeping after one `Scan` returns.
    fn range_scanned(&mut self) {
        if let Some(counts) = &self.counts {
            let mut counts = counts.borrow_mut();
            counts.counts[self.range_idx] += self.row_cnt - self.old_cnt;
            counts.ndvs[self.range_idx] += self.row_cnt - self.old_cnt;
        }
    }

    /// `Scan` returned (finished, or broke on `dbreader.ErrScanBreak`): on
    /// to the next range.
    fn next_range(&mut self) {
        self.range_scanned();
        self.range_idx += 1;
        self.pair_idx = 0;
        self.old_cnt = self.row_cnt;
    }

    fn close_result(&mut self) {
        self.result_closed = true;
    }

    /// One send on `e.result`. A receiver blocked on the empty channel takes
    /// the value directly; otherwise it fills the free slot; otherwise the
    /// sender blocks, or -- in a `select` beside `<-e.done` once `done` is
    /// closed -- takes the `done` case.
    fn send(
        &mut self,
        value: ScanResult,
        selects_done: bool,
        receiver: &mut Option<Option<ScanResult>>,
    ) -> SendOutcome {
        if self.result.is_none() {
            if let Some(slot @ None) = receiver {
                *slot = Some(value);
                return SendOutcome::Sent;
            }
            if selects_done && self.done {
                return SendOutcome::DoneChosen(value);
            }
            self.result = Some(value);
            return SendOutcome::Sent;
        }
        if selects_done && self.done {
            return SendOutcome::DoneChosen(value);
        }
        SendOutcome::Blocked(value)
    }

    /// Runs the scan goroutine until it blocks or returns. `receiver` is
    /// `Some(None)` while the consumer is blocked in `next`; the value the
    /// goroutine hands it lands there.
    fn run_worker(&mut self, receiver: &mut Option<Option<ScanResult>>) {
        loop {
            let state = self.worker.replace(ScanWorker::Exited).unwrap_or(ScanWorker::Exited);
            match state {
                ScanWorker::NotStarted => {
                    self.range_idx = 0;
                    self.pair_idx = 0;
                    self.old_cnt = self.row_cnt;
                    self.worker = Some(ScanWorker::Running);
                }
                ScanWorker::Running => {
                    if self.range_idx == self.range_pairs.len() {
                        // "handle the last chunk"
                        if self.chk.is_empty() {
                            self.close_result();
                        } else {
                            let chk = std::mem::take(&mut self.chk);
                            self.worker = Some(ScanWorker::FinalSend(ScanResult {
                                chk: Some(chk),
                                ..ScanResult::default()
                            }));
                        }
                        continue;
                    }
                    if self.pair_idx == self.range_pairs[self.range_idx].len() {
                        self.next_range();
                        self.worker = Some(ScanWorker::Running);
                        continue;
                    }
                    let pair_idx = self.pair_idx;
                    self.pair_idx += 1;
                    match self.process(pair_idx) {
                        Err(err) => {
                            self.range_scanned();
                            self.worker = Some(ScanWorker::ErrSend(ScanResult {
                                err: Some(err),
                                ..ScanResult::default()
                            }));
                        }
                        Ok(key) if is_full(&self.chk) => {
                            let chk = std::mem::take(&mut self.chk);
                            self.worker = Some(ScanWorker::ChunkSend(ScanResult {
                                chk: Some(chk),
                                last_processed_key: Some(key),
                                err: None,
                            }));
                        }
                        Ok(_) => {
                            if self.done {
                                self.next_range();
                            }
                            self.worker = Some(ScanWorker::Running);
                        }
                    }
                }
                ScanWorker::ChunkSend(value) => match self.send(value, true, receiver) {
                    SendOutcome::Sent => self.worker = Some(ScanWorker::AfterChunkSend),
                    SendOutcome::Blocked(value) => {
                        self.worker = Some(ScanWorker::ChunkSend(value));
                        return;
                    }
                    SendOutcome::DoneChosen(value) => {
                        // `return dbreader.ErrScanBreak` with `e.chk` still
                        // the unsent chunk.
                        self.chk = value.chk.unwrap_or_default();
                        self.next_range();
                        self.worker = Some(ScanWorker::Running);
                    }
                },
                ScanWorker::AfterChunkSend => {
                    self.chk = Vec::with_capacity(DEFAULT_BATCH_SIZE);
                    if self.done {
                        self.next_range();
                    }
                    self.worker = Some(ScanWorker::Running);
                }
                ScanWorker::FinalSend(value) => match self.send(value, true, receiver) {
                    SendOutcome::Sent | SendOutcome::DoneChosen(_) => self.close_result(),
                    SendOutcome::Blocked(value) => {
                        self.worker = Some(ScanWorker::FinalSend(value));
                        return;
                    }
                },
                ScanWorker::ErrSend(value) => match self.send(value, false, receiver) {
                    SendOutcome::Blocked(value) if !self.done => {
                        self.worker = Some(ScanWorker::ErrSend(value));
                        return;
                    }
                    _ => self.close_result(),
                },
                ScanWorker::Exited => return,
            }
        }
    }

    /// `<-e.result`.
    fn receive(&mut self) -> ScanResult {
        if let Some(value) = self.result.take() {
            // A sender blocked on the full slot has its value moved in and
            // is woken; it runs only once this goroutine blocks.
            match self.worker.take() {
                Some(ScanWorker::ChunkSend(blocked)) => {
                    self.result = Some(blocked);
                    self.worker = Some(ScanWorker::AfterChunkSend);
                }
                Some(ScanWorker::FinalSend(blocked) | ScanWorker::ErrSend(blocked)) => {
                    self.result = Some(blocked);
                    self.close_result();
                    self.worker = Some(ScanWorker::Exited);
                }
                other => self.worker = other,
            }
            return value;
        }
        if self.result_closed {
            return ScanResult::default();
        }
        let mut receiver = Some(None);
        self.run_worker(&mut receiver);
        receiver.flatten().unwrap_or_default()
    }
}

impl<'a> MppExec<'a> for TableScanExec<'a> {
    fn base(&self) -> &BaseMppExec<'a> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseMppExec<'a> {
        &mut self.base
    }

    /// Go `tableScanExec.open`: every range's locks are checked, then the
    /// scan goroutine starts. The store answers a range's versions at
    /// `startTS` in one read, which the goroutine then walks key by key.
    fn open(&mut self) -> Result<(), MppExecError> {
        self.range_pairs = self
            .kv_ranges
            .iter()
            .map(|range| scan_range(self.db_reader, range, self.start_ts, self.desc))
            .collect();
        for pairs in &self.range_pairs {
            check_range_lock(pairs, self.desc)?;
        }
        self.chk = Vec::with_capacity(DEFAULT_BATCH_SIZE);
        self.worker = Some(ScanWorker::NotStarted);
        Ok(())
    }

    /// Go `tableScanExec.next`.
    fn next(&mut self) -> Result<Option<Chunk>, MppExecError> {
        let result = self.receive();
        // Update the range for coprocessor paging protocol.
        if let Some(paging) = &self.paging {
            if result.err.is_none() {
                let last = &self.kv_ranges[self.kv_ranges.len() - 1];
                *paging.borrow_mut() = match (&result.last_processed_key, self.desc) {
                    (Some(key), true) => coprocessor::KeyRange {
                        start: key.clone(),
                        ..coprocessor::KeyRange::default()
                    },
                    (None, true) => coprocessor::KeyRange {
                        start: last.start.clone(),
                        ..coprocessor::KeyRange::default()
                    },
                    (Some(key), false) => coprocessor::KeyRange {
                        end: key_next(key),
                        ..coprocessor::KeyRange::default()
                    },
                    (None, false) => coprocessor::KeyRange {
                        end: last.end.clone(),
                        ..coprocessor::KeyRange::default()
                    },
                };
            }
        }
        if let Some(err) = result.err {
            return Err(err);
        }
        let Some(chk) = result.chk else {
            return Ok(None);
        };
        self.base.exec_summary.update_only_rows(chk.len());
        Ok(Some(chk))
    }

    /// Go `tableScanExec.stop`: `close(e.done)`, then `e.wg.Wait()` lets
    /// the goroutine run to its return.
    fn stop(&mut self) -> Result<(), MppExecError> {
        if self.worker.is_some() && !self.done {
            self.done = true;
            self.run_worker(&mut None);
        }
        Ok(())
    }

    fn scan_row_cnt(&self) -> Option<i64> {
        Some(self.row_cnt)
    }
}

/// Go `indexScanExec`: `open` scans every range into `DefaultBatchSize`
/// chunks; `next` hands them up one by one.
pub(crate) struct IndexScanExec<'a> {
    base: BaseMppExec<'a>,
    start_ts: u64,
    kv_ranges: Vec<coprocessor::KeyRange>,
    desc: bool,
    db_reader: &'a dyn DbReader,
    counts: Option<Rc<RefCell<RangeCounts>>>,
    prev_vals: Vec<Vec<u8>>,
    row_cnt: i64,
    ndv_cnt: i64,
    chk: Chunk,
    chk_idx: usize,
    chunks: Vec<Chunk>,

    col_infos: Vec<tidb_rowcodec::ColumnInfo>,
    num_idx_cols: usize,
    hdl_status: tidb_tablecodec::HandleStatus,
    /// The decoded entry's columns, then whether `ExtraPhysTblID` is the
    /// last requested column (filled from the key).
    num_columns: usize,
    has_phys_tbl_id: bool,
    time_zone: SessionTimeZone,
    paging: Option<SharedPaging>,
    chunk_last_processed_keys: Vec<Vec<u8>>,
}

impl<'a> IndexScanExec<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        sctx: Arc<RequestEvalContext>,
        field_types: Vec<FieldType>,
        start_ts: u64,
        kv_ranges: Vec<coprocessor::KeyRange>,
        desc: bool,
        db_reader: &'a dyn DbReader,
        counts: Option<Rc<RefCell<RangeCounts>>>,
        col_infos: Vec<tidb_rowcodec::ColumnInfo>,
        num_idx_cols: usize,
        hdl_status: tidb_tablecodec::HandleStatus,
        has_phys_tbl_id: bool,
        time_zone: SessionTimeZone,
        paging: Option<SharedPaging>,
    ) -> Self {
        let prev_vals = if counts.is_some() {
            vec![Vec::new(); num_idx_cols]
        } else {
            Vec::new()
        };
        let num_columns = field_types.len();
        Self {
            base: BaseMppExec {
                sctx,
                children: Vec::new(),
                field_types,
                exec_summary: ExecDetail::default(),
            },
            start_ts,
            kv_ranges,
            desc,
            db_reader,
            counts,
            prev_vals,
            row_cnt: 0,
            ndv_cnt: 0,
            chk: Vec::new(),
            chk_idx: 0,
            chunks: Vec::new(),
            col_infos,
            num_idx_cols,
            hdl_status,
            num_columns,
            has_phys_tbl_id,
            time_zone,
            paging,
            chunk_last_processed_keys: Vec::new(),
        }
    }

    /// Go `indexScanExec.isNewVals`.
    fn is_new_vals(&self, values: &[Vec<u8>]) -> bool {
        (0..self.num_idx_cols).any(|i| self.prev_vals[i] != values[i])
    }

    /// Go `indexScanExec.Process`.
    fn process(&mut self, key: &[u8], value: &[u8]) -> Result<(), MppExecError> {
        let other = |message: String| MppExecError::Other(message);
        let values = tidb_tablecodec::decode_index_kv(
            true,
            key,
            value,
            self.num_idx_cols,
            self.hdl_status,
            &self.col_infos,
        )
        .map_err(|error| other(format!("invalid index entry: {error:?}")))?;
        self.row_cnt += 1;
        if self.counts.is_some() && (self.prev_vals[0].is_empty() || self.is_new_vals(&values)) {
            self.ndv_cnt += 1;
            for i in 0..self.num_idx_cols {
                self.prev_vals[i].clone_from(&values[i]);
            }
        }
        let mut row = Vec::with_capacity(self.num_columns);
        for (offset, value) in values.iter().enumerate() {
            let Some(field_type) = self.base.field_types.get(offset) else {
                return Err(other("decoded index entry exceeds its executor schema".to_owned()));
            };
            row.push(
                tidb_tablecodec::decode_column_value(value, field_type, Some(&self.time_zone))
                    .map_err(|error| other(format!("invalid index column: {error:?}")))?,
            );
        }
        // The physical table id, when the entry does not already carry it.
        if self.has_phys_tbl_id && row.len() < self.num_columns {
            row.push(Datum::Int(tidb_tablecodec::table_key::decode_table_id(key)));
        }
        if row.len() != self.num_columns {
            return Err(other(
                "decoded index entry does not match its executor schema".to_owned(),
            ));
        }
        self.chk.push(row);
        if is_full(&self.chk) {
            self.chunks.push(std::mem::take(&mut self.chk));
            if self.paging.is_some() {
                self.chunk_last_processed_keys.push(key.to_vec());
            }
            self.chk = Vec::with_capacity(DEFAULT_BATCH_SIZE);
        }
        Ok(())
    }

    fn set_paging(&self, range: coprocessor::KeyRange) {
        if let Some(paging) = &self.paging {
            *paging.borrow_mut() = range;
        }
    }

    /// The paging range once every chunk is out: the whole request range.
    fn paging_to_range_end(&self) -> coprocessor::KeyRange {
        let last = &self.kv_ranges[self.kv_ranges.len() - 1];
        if self.desc {
            coprocessor::KeyRange {
                start: last.start.clone(),
                ..coprocessor::KeyRange::default()
            }
        } else {
            coprocessor::KeyRange {
                end: last.end.clone(),
                ..coprocessor::KeyRange::default()
            }
        }
    }
}

impl<'a> MppExec<'a> for IndexScanExec<'a> {
    fn base(&self) -> &BaseMppExec<'a> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseMppExec<'a> {
        &mut self.base
    }

    /// Go `indexScanExec.open`: the locks, then every range scanned.
    fn open(&mut self) -> Result<(), MppExecError> {
        let range_pairs = self
            .kv_ranges
            .iter()
            .map(|range| scan_range(self.db_reader, range, self.start_ts, self.desc))
            .collect::<Vec<_>>();
        for pairs in &range_pairs {
            check_range_lock(pairs, self.desc)?;
        }
        self.chk = Vec::with_capacity(DEFAULT_BATCH_SIZE);
        for (i, pairs) in range_pairs.iter().enumerate() {
            let old_cnt = self.row_cnt;
            self.ndv_cnt = 0;
            let mut scanned = Ok(());
            for pair in pairs {
                if let Some(error) = pair_error(pair) {
                    scanned = Err(error);
                    break;
                }
                if let Err(error) = self.process(&pair.key, &pair.value) {
                    scanned = Err(error);
                    break;
                }
            }
            if let Some(counts) = &self.counts {
                let mut counts = counts.borrow_mut();
                counts.counts[i] += self.row_cnt - old_cnt;
                counts.ndvs[i] += self.ndv_cnt;
            }
            scanned?;
        }
        if !self.chk.is_empty() {
            self.chunks.push(std::mem::take(&mut self.chk));
        }
        Ok(())
    }

    /// Go `indexScanExec.next`.
    fn next(&mut self) -> Result<Option<Chunk>, MppExecError> {
        if self.chk_idx < self.chunks.len() {
            self.chk_idx += 1;
            let chk = std::mem::take(&mut self.chunks[self.chk_idx - 1]);
            self.base.exec_summary.update_only_rows(chk.len());
            if self.paging.is_some() {
                let range = if self.chk_idx == self.chunks.len() {
                    self.paging_to_range_end()
                } else if self.desc {
                    coprocessor::KeyRange {
                        start: self.chunk_last_processed_keys[self.chk_idx - 1].clone(),
                        ..coprocessor::KeyRange::default()
                    }
                } else {
                    coprocessor::KeyRange {
                        end: key_next(&self.chunk_last_processed_keys[self.chk_idx - 1]),
                        ..coprocessor::KeyRange::default()
                    }
                };
                self.set_paging(range);
            }
            return Ok(Some(chk));
        }
        if self.paging.is_some() {
            self.set_paging(self.paging_to_range_end());
        }
        Ok(None)
    }

    fn scan_row_cnt(&self) -> Option<i64> {
        Some(self.row_cnt)
    }
}

/// Go `limitExec`.
pub(crate) struct LimitExec<'a> {
    pub(crate) base: BaseMppExec<'a>,
    pub(crate) limit: u64,
}

impl<'a> MppExec<'a> for LimitExec<'a> {
    fn base(&self) -> &BaseMppExec<'a> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseMppExec<'a> {
        &mut self.base
    }

    fn open(&mut self) -> Result<(), MppExecError> {
        self.base.children[0].open()
    }

    /// Go `limitExec.next`: the child's chunk truncated to what is left of
    /// the limit -- the child still hands up its whole chunk first.
    fn next(&mut self) -> Result<Option<Chunk>, MppExecError> {
        let Some(mut chk) = self.base.children[0].next()? else {
            return Ok(None);
        };
        if chk.is_empty() {
            return Ok(Some(chk));
        }
        if chk.len() as u64 <= self.limit {
            self.limit -= chk.len() as u64;
        } else {
            chk.truncate(usize::try_from(self.limit).unwrap_or(usize::MAX));
            self.limit = 0;
        }
        self.base.exec_summary.update_only_rows(chk.len());
        Ok(Some(chk))
    }
}

/// Go `topNExec`.
pub(crate) struct TopNExec<'a> {
    pub(crate) base: BaseMppExec<'a>,
    pub(crate) topn: u64,
    pub(crate) idx: u64,
    pub(crate) spec: TopNSpec,
    /// Go `heap.rows` after `sort.Sort`: the kept rows in order.
    pub(crate) rows: Vec<Row>,
    /// When dummy is true, topNExec just copies what it reads from its child
    /// to its parent.
    pub(crate) dummy: bool,
}

impl<'a> MppExec<'a> for TopNExec<'a> {
    fn base(&self) -> &BaseMppExec<'a> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseMppExec<'a> {
        &mut self.base
    }

    /// Go `topNExec.open`: the whole input runs through the heap, every
    /// consumed row counted as this executor's.
    fn open(&mut self) -> Result<(), MppExecError> {
        self.base.children[0].open()?;
        if self.dummy {
            return Ok(());
        }
        let mut candidates = Vec::new();
        loop {
            let chk = match self.base.children[0].next()? {
                Some(chk) if !chk.is_empty() => chk,
                _ => break,
            };
            self.base.exec_summary.update_only_rows(chk.len());
            for row in chk {
                let keys = self.spec.evaluate(&row).map_err(MppExecError::Other)?;
                candidates.push((row, keys));
            }
        }
        // Go `topNHeap.totalCount`, then `sort.Sort(&e.heap.topNSorter)`.
        self.rows = crate::go_topn_heap_finish(&self.spec.keys, candidates, self.spec.limit)
            .into_iter()
            .map(|(row, _)| row)
            .collect();
        Ok(())
    }

    /// Go `topNExec.next`: up to a chunk of the sorted heap per call.
    fn next(&mut self) -> Result<Option<Chunk>, MppExecError> {
        if self.dummy {
            return self.base.children[0].next();
        }
        let mut chk = Vec::with_capacity(DEFAULT_BATCH_SIZE);
        while !is_full(&chk) && self.idx < self.topn && (self.idx as usize) < self.rows.len() {
            chk.push(std::mem::take(&mut self.rows[self.idx as usize]));
            self.idx += 1;
        }
        Ok(Some(chk))
    }
}

/// Go `aggExec`.
pub(crate) struct AggExec<'a> {
    pub(crate) base: BaseMppExec<'a>,
    pub(crate) aggregator: Option<RegionAggregator>,
    pub(crate) processed: bool,
}

impl<'a> AggExec<'a> {
    /// Go `aggExec.processAllRows`: every child row into its group, then one
    /// chunk of every group's partial result followed by its group key.
    fn process_all_rows(&mut self) -> Result<Option<Chunk>, MppExecError> {
        let mut aggregator = self
            .aggregator
            .take()
            .expect("aggExec processes its rows once");
        loop {
            let chk = match self.base.children[0].next()? {
                Some(chk) if !chk.is_empty() => chk,
                _ => break,
            };
            for row in &chk {
                aggregator.update(row).map_err(MppExecError::Other)?;
            }
        }
        let chk = aggregator.finish();
        self.base.exec_summary.update_only_rows(chk.len());
        Ok(Some(chk))
    }
}

impl<'a> MppExec<'a> for AggExec<'a> {
    fn base(&self) -> &BaseMppExec<'a> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseMppExec<'a> {
        &mut self.base
    }

    fn open(&mut self) -> Result<(), MppExecError> {
        self.base.children[0].open()
    }

    /// Go `aggExec.next`.
    fn next(&mut self) -> Result<Option<Chunk>, MppExecError> {
        if !self.processed {
            self.processed = true;
            return self.process_all_rows();
        }
        Ok(None)
    }
}

/// Go `selExec`.
pub(crate) struct SelExec<'a> {
    pub(crate) base: BaseMppExec<'a>,
    pub(crate) conditions: Vec<SimpleExpr>,
}

impl<'a> MppExec<'a> for SelExec<'a> {
    fn base(&self) -> &BaseMppExec<'a> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseMppExec<'a> {
        &mut self.base
    }

    fn open(&mut self) -> Result<(), MppExecError> {
        self.base.children[0].open()
    }

    /// Go `selExec.next`: child chunks are filtered into one output chunk
    /// until it is full or the child runs out; every selected row counts.
    fn next(&mut self) -> Result<Option<Chunk>, MppExecError> {
        let mut ret = Vec::with_capacity(DEFAULT_BATCH_SIZE);
        while !is_full(&ret) {
            let chk = match self.base.children[0].next()? {
                Some(chk) if !chk.is_empty() => chk,
                _ => break,
            };
            // Go `expression.VectorizedFilter`: each condition over the
            // chunk's rows the earlier ones kept, so a later condition never
            // sees a rejected row and the first error is the earliest
            // condition's.
            let mut selected = vec![true; chk.len()];
            for condition in &self.conditions {
                for (row, keep) in chk.iter().zip(selected.iter_mut()) {
                    if *keep {
                        *keep = condition_holds(condition, row)?;
                    }
                }
            }
            for (row, keep) in chk.into_iter().zip(selected) {
                if keep {
                    ret.push(row);
                    self.base.exec_summary.update_only_rows(1);
                }
            }
        }
        Ok(Some(ret))
    }
}

/// Go `projExec`.
pub(crate) struct ProjExec<'a> {
    pub(crate) base: BaseMppExec<'a>,
    pub(crate) exprs: Vec<tidb_expr::expression::Expression>,
}

impl<'a> MppExec<'a> for ProjExec<'a> {
    fn base(&self) -> &BaseMppExec<'a> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseMppExec<'a> {
        &mut self.base
    }

    fn open(&mut self) -> Result<(), MppExecError> {
        self.base.children[0].open()
    }

    /// Go `projExec.next`: each child row's expressions, the child's rows
    /// counted.
    fn next(&mut self) -> Result<Option<Chunk>, MppExecError> {
        let chk = match self.base.children[0].next()? {
            Some(chk) if !chk.is_empty() => chk,
            _ => return Ok(None),
        };
        self.base.exec_summary.update_only_rows(chk.len());
        let mut new_chunk = Vec::with_capacity(chk.len());
        for row in &chk {
            let row = tidb_chunk::mutrow::MutRow::mut_row_from_datums(row);
            let mut new_row = Vec::with_capacity(self.exprs.len());
            for expr in &self.exprs {
                new_row.push(
                    expr.eval(self.base.sctx.as_ref(), row.to_row())
                        .map_err(CopExecError::from)?,
                );
            }
            new_chunk.push(new_row);
        }
        Ok(Some(new_chunk))
    }
}

/// Go `hash/fnv.New64` (FNV-1) over `bytes`.
fn fnv64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        hash ^= u64::from(byte);
    }
    hash
}

/// Go `exchSenderExec`: sends its child's rows through a tunnel to each
/// target task (all of them to each, or each row to one by its hash).
pub(crate) struct ExchSenderExec<'a> {
    pub(crate) base: BaseMppExec<'a>,
    pub(crate) tunnels: Vec<Arc<crate::mpp_task::ExchangerTunnel>>,
    pub(crate) output_offsets: Vec<u32>,
    pub(crate) exchange_tp: tipb::ExchangeType,
    pub(crate) hash_key_offsets: Vec<usize>,
    pub(crate) hash_key_types: Vec<FieldType>,
}

impl ExchSenderExec<'_> {
    /// Go `toTiPBChunk`: the rows' output columns, datum-encoded in the
    /// request's zone.
    fn to_tipb_chunk(&self, chk: &[Row]) -> Result<Vec<tipb::Chunk>, String> {
        let rows: Vec<Row> = chk
            .iter()
            .map(|row| self.output_offsets.iter().map(|&offset| row[offset as usize].clone()).collect())
            .collect();
        let mut chunks = Vec::new();
        crate::use_default_encoding(&rows, &self.base.sctx.zone, &mut chunks)?;
        Ok(chunks)
    }

    fn send_err(&self, err: String) {
        for tunnel in &self.tunnels {
            tunnel.send_err(err.clone());
        }
    }

    /// The loop of Go's `next`, before its deferred close.
    fn run(&mut self) {
        loop {
            let chk = match self.base.children[0].next() {
                Err(err) => {
                    self.send_err(format!("{err:?}"));
                    return;
                }
                Ok(chk) => match chk {
                    Some(chk) if !chk.is_empty() => chk,
                    _ => return,
                },
            };
            self.base.exec_summary.update_only_rows(chk.len());
            if self.exchange_tp == tipb::ExchangeType::Hash {
                let mut target_chunks: Vec<Chunk> = vec![Vec::new(); self.tunnels.len()];
                for row in chk {
                    // use hash values to get unique uint64 to mod.
                    // collect all the hash key datum.
                    let hashed = match tidb_codec::hash_row(&row, &self.hash_key_types, &self.hash_key_offsets) {
                        Ok(hashed) => hashed,
                        Err(err) => {
                            self.send_err(format!("{err:?}"));
                            return;
                        }
                    };
                    let hash_key = fnv64(&hashed) % self.tunnels.len() as u64;
                    target_chunks[hash_key as usize].push(row);
                }
                for (i, tunnel) in self.tunnels.iter().enumerate() {
                    if !target_chunks[i].is_empty() {
                        match self.to_tipb_chunk(&target_chunks[i]) {
                            Ok(tipb_chunks) => tipb_chunks.into_iter().for_each(|chunk| tunnel.send_chunk(chunk)),
                            Err(err) => {
                                self.send_err(err);
                                return;
                            }
                        }
                    }
                }
            } else {
                for tunnel in &self.tunnels {
                    match self.to_tipb_chunk(&chk) {
                        Ok(tipb_chunks) => tipb_chunks.into_iter().for_each(|chunk| tunnel.send_chunk(chunk)),
                        Err(err) => {
                            self.send_err(err);
                            return;
                        }
                    }
                }
            }
        }
    }
}

impl<'a> MppExec<'a> for ExchSenderExec<'a> {
    fn base(&self) -> &BaseMppExec<'a> {
        &self.base
    }
    fn base_mut(&mut self) -> &mut BaseMppExec<'a> {
        &mut self.base
    }
    fn open(&mut self) -> Result<(), MppExecError> {
        self.base.children[0].open()
    }
    /// Go `exchSenderExec.next`: sends every chunk, then (Go's defer) waits
    /// for each tunnel's receiver to connect, closes the tunnels and stops.
    fn next(&mut self) -> Result<Option<Chunk>, MppExecError> {
        self.run();
        for tunnel in &self.tunnels {
            tunnel.wait_connected();
            tunnel.close();
        }
        self.stop().unwrap_or_else(|err| panic!("{err:?}"));
        Ok(None)
    }
}

/// Go `exchRecvExec`: the rows every sender task of the receiver sent this
/// task, received once.
pub(crate) struct ExchRecvExec<'a> {
    pub(crate) base: BaseMppExec<'a>,
    pub(crate) exchange_receiver: tipb::ExchangeReceiver,
    pub(crate) mpp_ctx: Arc<crate::mpp_task::MppCtx>,
    pub(crate) chk: Option<Chunk>,
    pub(crate) inited: bool,
}

impl ExchRecvExec<'_> {
    /// Go `init`: one tunnel worker per sender task, all awaited.
    fn init(&mut self) -> Result<(), MppExecError> {
        let mut server_metas = Vec::with_capacity(self.exchange_receiver.encoded_task_meta.len());
        for encoded_meta in &self.exchange_receiver.encoded_task_meta {
            use prost::Message;
            let meta = tidb_proto::mpp::TaskMeta::decode(encoded_meta.as_slice())
                .map_err(|err| MppExecError::Other(err.to_string()))?;
            server_metas.push(meta);
        }
        let field_types = &self.base.field_types;
        let handler = &self.mpp_ctx.task_handler;
        // for receiver: open conn worker for every receive meta.
        let results: Vec<Result<Chunk, String>> = std::thread::scope(|scope| {
            let workers: Vec<_> = server_metas
                .into_iter()
                .map(|meta| scope.spawn(move || run_tunnel_worker(handler, &meta, field_types)))
                .collect();
            workers.into_iter().map(|worker| worker.join().unwrap_or_else(|_| Err("tunnel worker panicked".to_owned()))).collect()
        });
        let mut chk = Vec::new();
        let mut err = None;
        for result in results {
            match result {
                Ok(rows) => chk.extend(rows),
                Err(e) => err = Some(e),
            }
        }
        self.chk = Some(chk);
        err.map_or(Ok(()), |err| Err(MppExecError::Other(err)))
    }
}

/// Go `EstablishConnAndReceiveData` and `runTunnelWorker`: the rows the
/// sender task `meta` sends `h`'s task.
fn run_tunnel_worker(
    h: &crate::mpp_task::MppTaskHandler,
    meta: &tidb_proto::mpp::TaskMeta,
    field_types: &[FieldType],
) -> Result<Chunk, String> {
    let req = tidb_proto::mpp::EstablishMppConnectionRequest {
        receiver_meta: Some(h.meta.clone()),
        sender_meta: Some(meta.clone()),
        ..Default::default()
    };
    let resp = h.rpc_client.establish_mpp_conn(&meta.address, req)?;
    let mut rows = Vec::new();
    for mpp_data in resp {
        if let Some(error) = mpp_data.error {
            return Err(error.msg);
        }
        use prost::Message;
        let select_resp = tipb::SelectResponse::decode(mpp_data.data.as_slice()).map_err(|err| err.to_string())?;
        for tipb_chunk in select_resp.chunks {
            rows.extend(pb_chunk_to_chunk(&tipb_chunk, field_types)?);
        }
    }
    Ok(rows)
}

/// Go `pbChunkToChunk`: a datum-encoded chunk's rows, decoded in the
/// system location.
fn pb_chunk_to_chunk(pb_chk: &tipb::Chunk, field_types: &[FieldType]) -> Result<Chunk, String> {
    let mut rows_data = pb_chk.rows_data.as_deref().unwrap_or_default();
    let mut rows = Vec::new();
    while !rows_data.is_empty() {
        let mut row = Vec::with_capacity(field_types.len());
        for field_type in field_types {
            let (rest, datum) =
                tidb_codec::decode_one_typed_in_timezone(rows_data, field_type, Some(&SessionTimeZone::Local))
                    .map_err(|err| format!("{err:?}"))?;
            rows_data = rest;
            row.push(datum);
        }
        rows.push(row);
    }
    Ok(rows)
}

impl<'a> MppExec<'a> for ExchRecvExec<'a> {
    fn base(&self) -> &BaseMppExec<'a> {
        &self.base
    }
    fn base_mut(&mut self) -> &mut BaseMppExec<'a> {
        &mut self.base
    }
    fn open(&mut self) -> Result<(), MppExecError> {
        Ok(())
    }
    fn next(&mut self) -> Result<Option<Chunk>, MppExecError> {
        if !self.inited {
            self.inited = true;
            self.init()?;
        }
        let chk = self.chk.take();
        if let Some(chk) = &chk {
            self.base.exec_summary.update_only_rows(chk.len());
        }
        Ok(chk)
    }
}

/// Go `joinExec`: a hash join on the first key pair (the mock store's join
/// ignores other conditions), probing row by row and handing each joined
/// row up as its own chunk.
pub(crate) struct JoinExec<'a> {
    pub(crate) base: BaseMppExec<'a>,
    pub(crate) join_type: tipb::JoinType,
    pub(crate) hash_map: std::collections::HashMap<Vec<u8>, Vec<Row>>,
    /// Go `buildKey.Index` / `probeKey.Index`.
    pub(crate) build_key: usize,
    pub(crate) probe_key: usize,
    /// Go `buildSideIdx`: the build child's index in `base.children`.
    pub(crate) build_side_idx: usize,
    pub(crate) idx: usize,
    pub(crate) reserved_rows: Vec<Row>,
    pub(crate) default_inner: Row,
    pub(crate) inited: bool,
    /// Go `comKeyTp`: aligns the types of the join keys.
    pub(crate) com_key_tp: FieldType,
}

impl JoinExec<'_> {
    fn probe_side_idx(&self) -> usize {
        1 - self.build_side_idx
    }

    /// Go `getHashKey`.
    fn get_hash_key(&self, key_col: &Datum) -> Result<Vec<u8>, MppExecError> {
        use tidb_exprctx::Columns;
        let zone = self.base.sctx.time_zone();
        let context = tidb_datatype::ConversionContext::new(
            self.base.sctx.type_flags(),
            tidb_datatype::ConversionLocation::from_time_zone(&zone),
            &tidb_context::IGNORE_CONVERSION_WARNINGS,
        );
        let converted = key_col
            .convert_to_in_context(&self.com_key_tp, &context, &zone)
            .map_err(|err| MppExecError::Other(format!("{err:?}")))?;
        if let Some(err) = converted.error {
            return Err(MppExecError::Other(format!("{err:?}")));
        }
        converted.value.sql_bytes().map_err(|err| MppExecError::Other(format!("{err:?}")))
    }

    /// Go `buildHashTable`.
    fn build_hash_table(&mut self) -> Result<(), MppExecError> {
        loop {
            let chk = self.base.children[self.build_side_idx].next()?;
            let Some(chk) = chk.filter(|chk| !chk.is_empty()) else {
                return Ok(());
            };
            for row in chk {
                let key = self.get_hash_key(&row[self.build_key])?;
                self.hash_map.entry(key).or_default().push(row);
            }
        }
    }

    /// Go `fetchRows`: `true` at the probe side's end.
    fn fetch_rows(&mut self) -> Result<bool, MppExecError> {
        let probe = self.probe_side_idx();
        let chk = self.base.children[probe].next()?;
        let Some(chk) = chk.filter(|chk| !chk.is_empty()) else {
            return Ok(true);
        };
        self.idx = 0;
        self.reserved_rows = Vec::new();
        for row in chk {
            let key = self.get_hash_key(&row[self.probe_key])?;
            if let Some(row_set) = self.hash_map.get(&key) {
                for matched in row_set {
                    let new_row: Row = if self.build_side_idx == 0 {
                        matched.iter().chain(&row).cloned().collect()
                    } else {
                        row.iter().chain(matched).cloned().collect()
                    };
                    self.reserved_rows.push(new_row);
                }
            } else if self.join_type == tipb::JoinType::TypeLeftOuterJoin {
                self.reserved_rows.push(row.iter().chain(&self.default_inner).cloned().collect());
            } else if self.join_type == tipb::JoinType::TypeRightOuterJoin {
                self.reserved_rows.push(self.default_inner.iter().chain(&row).cloned().collect());
            }
        }
        Ok(false)
    }
}

impl<'a> MppExec<'a> for JoinExec<'a> {
    fn base(&self) -> &BaseMppExec<'a> {
        &self.base
    }
    fn base_mut(&mut self) -> &mut BaseMppExec<'a> {
        &mut self.base
    }
    fn open(&mut self) -> Result<(), MppExecError> {
        self.base.children[self.build_side_idx].open()?;
        let probe = self.probe_side_idx();
        self.base.children[probe].open()
    }
    fn stop(&mut self) -> Result<(), MppExecError> {
        self.base.children[self.build_side_idx].stop()?;
        let probe = self.probe_side_idx();
        self.base.children[probe].stop()
    }
    fn next(&mut self) -> Result<Option<Chunk>, MppExecError> {
        if !self.inited {
            self.inited = true;
            self.build_hash_table()?;
        }
        loop {
            if self.idx < self.reserved_rows.len() {
                let row = self.reserved_rows[self.idx].clone();
                self.idx += 1;
                self.base.exec_summary.update_only_rows(1);
                return Ok(Some(vec![row]));
            }
            if self.fetch_rows()? {
                return Ok(None);
            }
        }
    }
}
