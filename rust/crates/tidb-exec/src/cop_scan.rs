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

//! The wide SQL path's coprocessor-backed base-table scan: the production
//! implementation of [`PushdownScanner`].
//!
//! # What this closes
//!
//! The wide path reads its tables through
//! [`ClusterTableStorage`](tidb_executor::cluster_storage::ClusterTableStorage),
//! whose `iter` scans the record range on the session's transaction thread
//! and ships every key/value pair of it to the client. The predicate, the row
//! cap and the column projection are then applied here, after the bytes have
//! already crossed the network. This module makes the same scan a real
//! coprocessor request: TiKV evaluates the Selection and the Limit at the
//! region and returns rows, so a `WHERE` that rejects most of a table now
//! rejects it *before* the network.
//!
//! Everything above is already built. [`crate::dag_request`] lowers the scan,
//! the Selection and the cap into a `DAGRequest`;
//! [`crate::wide_scan_selection`] converts the wide path's pushed conjuncts
//! into that Selection's condition shape; `tidb-distsql` owns ranges, region
//! tasks, dispatch, retry and response decoding. This module is the seam
//! between them and the storage: it carries the request onto the wire and
//! streams the response back as rows.
//!
//! # The two properties that make it safe
//!
//! * **It answers from the statement's own snapshot.** The request carries
//!   [`PushdownScanRequest::snapshot_ts`], which the storage filled in from
//!   the snapshot the statement is bound to. A scan that read at any other
//!   timestamp would not be repeatable read.
//! * **It never has the last word.** The scan source applies the pushed
//!   conjuncts and the cap to every row it emits, and merges the session's
//!   staged buffer on top (see [`tidb_executor::pushdown_scan`]). So this
//!   module may lower all, some, or none of the predicate and the answer is
//!   the same; only the number of rows on the wire changes.
//!
//! # What it refuses
//!
//! A column whose coprocessor descriptor this module cannot build faithfully
//! -- anything but a signed or unsigned `BIGINT`/`INT` today -- makes the
//! whole scan fall back to the byte-level cursor. The refusal is
//! [`PushdownScannerError::Unsupported`], which the storage turns into "use
//! `iter`", so a refused shape is slower and never wrong.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread;

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::{
    CancelHandle, EncodeType, ExecutorKind, ExecutorShape, InjectedQueryRuntime,
    QueryResultContext, QueryTransport, RequestBuilder, RequestEnvelope, SelectInput,
    WarningCollector,
};
use tidb_executor::pushdown_scan::{
    PushdownRowStream, PushdownScanColumn, PushdownScanRequest, PushdownScanner,
    PushdownScannerError, EXTRA_HANDLE_COLUMN_ID,
};
use tidb_executor::scan_pushdown::ScanComparison;
use tidb_executor::storage::StorageError;
use tidb_planner::physical_selection::PhysicalSelectionPlan;
use tidb_planner::physical_table_scan::PhysicalTableScanPlan;
use tidb_planner::scan_pushdown::{ScanColumnInfo, TiKvTableScanSpec};
use tidb_txnkv::KeyRange;

use crate::dag_request::{construct_capped_read_only_dag_req, DagRequestContext, TiKvScanPlan};
use crate::real_tikv_read::RealTiKvSessionTransportFactory;
use crate::wide_scan_selection::wide_scan_selection_plan;

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: i32 = 1;
/// Go `mysql.PriKeyFlag`.
const PRI_KEY_FLAG: i32 = 2;
/// Go `mysql.UnsignedFlag`.
const UNSIGNED_FLAG: i32 = 32;
/// Go `charset.CollationBin`, the coprocessor collation of a numeric column.
const BINARY_COLLATION_ID: i32 = 63;
/// Go `mysql.TypeLonglong`.
const MYSQL_TYPE_LONGLONG: i32 = 8;
/// Go `mysql.TypeLong`.
const MYSQL_TYPE_LONG: i32 = 3;

/// How many decoded rows the reader thread may run ahead of the consumer.
///
/// The point of the bound is that it *is* a bound: a scan holds a few batches
/// of decoded rows, never the relation, so the streaming property the scan
/// source has above the seam survives the thread hop below it.
const BATCH_ROWS: usize = 1024;
const BATCHES_AHEAD: usize = 2;

/// One coprocessor scan capability for a node's sessions.
///
/// Each opened scan gets its own worker-local transport on its own thread,
/// because the production transport is deliberately not `Send` while the
/// storage that holds this scanner is shared between connection workers. What
/// crosses threads is the request and the decoded rows.
pub struct CopScanSource<F> {
    factory: Arc<F>,
    time_zone_name: String,
    time_zone_offset_secs: i64,
    /// Rows this node has received from coprocessor scans, for the receipt a
    /// live proof reads.
    rows_returned: Arc<AtomicU64>,
    /// Scans this node served remotely, against the ones it refused.
    scans_served: Arc<AtomicU64>,
    scans_refused: Arc<AtomicU64>,
}

impl<F> fmt::Debug for CopScanSource<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CopScanSource")
            .field("rows_returned", &self.rows_returned.load(Ordering::Relaxed))
            .field("scans_served", &self.scans_served.load(Ordering::Relaxed))
            .field("scans_refused", &self.scans_refused.load(Ordering::Relaxed))
            .finish()
    }
}

/// What a node's coprocessor scans have done so far, as plain counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CopScanStats {
    /// Rows the coprocessor sent to this node.
    pub rows_returned: u64,
    /// Scans served remotely.
    pub scans_served: u64,
    /// Scans refused, which fell back to the byte-level cursor.
    pub scans_refused: u64,
}

impl<F> CopScanSource<F> {
    /// Builds the capability over an already-running transport factory.
    #[must_use]
    pub fn new(factory: Arc<F>, time_zone_name: impl Into<String>, offset_secs: i64) -> Self {
        Self {
            factory,
            time_zone_name: time_zone_name.into(),
            time_zone_offset_secs: offset_secs,
            rows_returned: Arc::new(AtomicU64::new(0)),
            scans_served: Arc::new(AtomicU64::new(0)),
            scans_refused: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The node's live coprocessor-scan counters.
    #[must_use]
    pub fn stats(&self) -> CopScanStats {
        CopScanStats {
            rows_returned: self.rows_returned.load(Ordering::Relaxed),
            scans_served: self.scans_served.load(Ordering::Relaxed),
            scans_refused: self.scans_refused.load(Ordering::Relaxed),
        }
    }
}

impl<F> PushdownScanner for CopScanSource<F>
where
    F: RealTiKvSessionTransportFactory + 'static,
    <F::Transport as QueryTransport>::Response: 'static,
{
    fn open(
        &self,
        request: &PushdownScanRequest,
    ) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError> {
        let refuse = |reason: &str| {
            self.scans_refused.fetch_add(1, Ordering::Relaxed);
            PushdownScannerError::Unsupported(reason.to_owned())
        };
        if request.snapshot_ts == 0 {
            return Err(refuse("the statement's snapshot has no timestamp"));
        }
        let columns = request
            .columns
            .iter()
            .map(scan_column)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| refuse("a column has no bounded coprocessor descriptor"))?;
        let field_types: Vec<FieldType> = request
            .columns
            .iter()
            .map(|column| column.field_type.clone())
            .collect();
        let output_offsets: Vec<u32> = (0..columns.len() as u32).collect();

        // Every conjunct this lowering accepts travels; the rest simply stay
        // behind, because the scan source tests all of them locally anyway.
        let lowered: Vec<ScanComparison> = request
            .comparisons
            .iter()
            .filter(|comparison| wide_scan_selection_plan(std::slice::from_ref(comparison)).is_ok())
            .cloned()
            .collect();
        let selection: Option<PhysicalSelectionPlan> = if lowered.is_empty() {
            None
        } else {
            Some(wide_scan_selection_plan(&lowered).map_err(|error| {
                PushdownScannerError::Backend(StorageError::Backend(error.to_string()))
            })?)
        };

        let mut spec = TiKvTableScanSpec::new(request.table_id, columns);
        // The merge above reads the remote rows in record-key order, which is
        // the order it merges the staged buffer against.
        spec.keep_order = true;
        let scan = PhysicalTableScanPlan::init(0, 0, spec);
        let dag = construct_capped_read_only_dag_req(
            &DagRequestContext::new(
                self.time_zone_name.clone(),
                self.time_zone_offset_secs,
                0,
                EncodeType::Default,
            ),
            TiKvScanPlan::Table(&scan),
            selection.as_ref(),
            request.limit,
            &output_offsets,
        )
        .map_err(|error| PushdownScannerError::Unsupported(error.to_string()))?;

        let key_range = KeyRange::new(request.start.clone(), request.end.clone());
        let mut shapes = vec![ExecutorShape::new(ExecutorKind::TableScan)];
        if selection.is_some() {
            shapes.push(ExecutorShape::new(ExecutorKind::Other));
        }
        if request.limit.is_some() {
            shapes.push(ExecutorShape::new(ExecutorKind::Other));
        }
        let plan = RemoteScanPlan {
            dag,
            envelope: RequestEnvelope::new(shapes),
            key_range,
            snapshot_ts: request.snapshot_ts,
            field_types,
        };
        let (rows, batches) = sync_channel::<Result<Vec<Vec<Datum>>, String>>(BATCHES_AHEAD);
        let factory = Arc::clone(&self.factory);
        let node_rows = Arc::clone(&self.rows_returned);
        thread::Builder::new()
            .name("cop-scan".to_owned())
            .spawn(move || serve_scan(&factory, plan, &rows, &node_rows))
            .map_err(|error| {
                PushdownScannerError::Backend(StorageError::Backend(error.to_string()))
            })?;
        self.scans_served.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(CopRowStream {
            batches: Some(batches),
            pending: Vec::new().into_iter(),
            returned: 0,
        }))
    }
}

/// Everything the reader thread needs, owned independently of the caller.
struct RemoteScanPlan {
    dag: tidb_proto::tipb::DagRequest,
    /// The executor shapes the request builder reads for concurrency, which
    /// must match the DAG's own executor list.
    envelope: RequestEnvelope,
    key_range: KeyRange,
    snapshot_ts: u64,
    field_types: Vec<FieldType>,
}

/// Runs one coprocessor scan on its own thread, handing decoded rows back in
/// bounded batches.
fn serve_scan<F>(
    factory: &Arc<F>,
    plan: RemoteScanPlan,
    rows: &SyncSender<Result<Vec<Vec<Datum>>, String>>,
    node_rows: &Arc<AtomicU64>,
) where
    F: RealTiKvSessionTransportFactory,
    <F::Transport as QueryTransport>::Response: 'static,
{
    if let Err(error) = drain_scan(factory, plan, rows, node_rows) {
        let _ = rows.send(Err(error));
    }
}

fn drain_scan<F>(
    factory: &Arc<F>,
    plan: RemoteScanPlan,
    rows: &SyncSender<Result<Vec<Vec<Datum>>, String>>,
    node_rows: &Arc<AtomicU64>,
) -> Result<(), String>
where
    F: RealTiKvSessionTransportFactory,
    <F::Transport as QueryTransport>::Response: 'static,
{
    use prost::Message;

    let mut transport = factory.open_session_transport()?;
    let cancellation = Arc::new(CancelHandle::default());
    let mut builder = RequestBuilder::new();
    builder
        .set_start_ts(plan.snapshot_ts)
        .set_keep_order(true)
        .set_non_partitioned_key_ranges(vec![plan.key_range])
        .set_dag_request(plan.envelope, plan.dag.encode_to_vec());
    let request = builder
        .build_transport_request(Arc::clone(&cancellation))
        .map_err(|error| format!("{error:?}"))?;
    let mut runtime = InjectedQueryRuntime::new(&mut transport);
    let result = runtime
        .select_with_runtime_stats(
            &request,
            SelectInput::default(),
            QueryResultContext::new(plan.field_types, WarningCollector::new()),
            vec![0],
            0,
            true,
        )
        .map_err(|error| error.to_string())?;
    let mut iter = result.into_select_iter(Vec::new());
    let mut batch = Vec::with_capacity(BATCH_ROWS);
    loop {
        let row = iter.next_row().map_err(|error| error.to_string())?;
        let done = row.is_none();
        if let Some(row) = row {
            batch.push(row.row);
            if batch.len() < BATCH_ROWS {
                continue;
            }
        }
        if !batch.is_empty() {
            let sent = batch.len() as u64;
            // A consumer that stopped pulling -- an early-stopping `LIMIT`, or
            // a failed statement -- drops its receiver, and this is where the
            // scan learns it: the rest of the relation is never read.
            if rows.send(Ok(std::mem::take(&mut batch))).is_err() {
                break;
            }
            node_rows.fetch_add(sent, Ordering::Relaxed);
            batch = Vec::with_capacity(BATCH_ROWS);
        }
        if done {
            break;
        }
    }
    iter.close();
    Ok(())
}

/// The caller's end of one coprocessor scan.
struct CopRowStream {
    /// Dropping this is what tells the reader thread to stop; see
    /// `drain_scan`.
    batches: Option<Receiver<Result<Vec<Vec<Datum>>, String>>>,
    pending: std::vec::IntoIter<Vec<Datum>>,
    returned: u64,
}

impl PushdownRowStream for CopRowStream {
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
        loop {
            if let Some(row) = self.pending.next() {
                self.returned += 1;
                return Ok(Some(row));
            }
            let Some(batches) = self.batches.as_ref() else {
                return Ok(None);
            };
            match batches.recv() {
                Ok(Ok(batch)) => self.pending = batch.into_iter(),
                Ok(Err(error)) => {
                    self.batches = None;
                    return Err(StorageError::Backend(error));
                }
                // The reader thread finished and dropped its sender.
                Err(_) => {
                    self.batches = None;
                    return Ok(None);
                }
            }
        }
    }

    fn rows_returned(&self) -> u64 {
        self.returned
    }

    fn close(&mut self) {
        self.batches = None;
    }
}

impl Drop for CopRowStream {
    fn drop(&mut self) {
        self.close();
    }
}

/// One column's coprocessor descriptor, or `None` for a type this bounded
/// lowering will not describe.
///
/// The refusal is the honest half: a descriptor built from a guessed
/// collation, length or flag set would make TiKV decode a column differently
/// from the client, which is a wrong answer rather than a slow one.
fn scan_column(column: &PushdownScanColumn) -> Option<ScanColumnInfo> {
    let (tp, column_len) = match column.field_type.code() {
        FieldTypeCode::LongLong => (MYSQL_TYPE_LONGLONG, 20),
        FieldTypeCode::Long => (MYSQL_TYPE_LONG, 11),
        _ => return None,
    };
    let mut flag = 0;
    if column.field_type.is_unsigned() {
        flag |= UNSIGNED_FLAG;
    }
    if column.is_handle {
        flag |= NOT_NULL_FLAG | PRI_KEY_FLAG;
    }
    Some(ScanColumnInfo {
        column_id: column.id,
        tp,
        collation: BINARY_COLLATION_ID,
        column_len,
        decimal: 0,
        flag,
        pk_handle: column.is_handle,
        ..ScanColumnInfo::default()
    })
}

/// Whether a request names the implicit `_tidb_rowid` handle column, which is
/// the shape a table with no integer primary key scans with.
#[must_use]
pub fn requests_extra_handle(request: &PushdownScanRequest) -> bool {
    request
        .columns
        .get(request.handle_index)
        .is_some_and(|column| column.id == EXTRA_HANDLE_COLUMN_ID)
}
