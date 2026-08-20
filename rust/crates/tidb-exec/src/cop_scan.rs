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
//!   staged buffer on top (see [`tidb_executor::remote_scan`]). So this
//!   module may lower all, some, or none of the predicate and the answer is
//!   the same; only the number of rows on the wire changes.
//!
//! # What it refuses
//!
//! A column whose coprocessor descriptor this module cannot build faithfully
//! -- anything outside the signed and unsigned integer family
//! (`BIGINT`/`INT`/`MEDIUMINT`/`SMALLINT`/`TINYINT`) and the character-string
//! family (`VARCHAR`/`CHAR`/the `BLOB`s and their `BINARY` spellings) today --
//! makes the whole scan fall back to the byte-level cursor. Note that this is
//! a *projection* gate, separate from the predicate lowering's own type gate:
//! a table with one `DECIMAL` column in the `SELECT` list cannot be scanned
//! remotely at all, however pushable its `WHERE` is. The refusal is
//! [`PushdownScannerError::Unsupported`], which the storage turns into "use
//! `iter`", so a refused shape is slower and never wrong.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::{
    CancelHandle, DistSqlContext, EncodeType, ExecutorKind, ExecutorShape, InjectedQueryRuntime,
    QueryResultContext, QueryTransport, RequestBuilder, RequestEnvelope, SelectInput,
    WarningCollector,
};
use tidb_executor::predicate_pushdown::ScanPredicate;
use tidb_executor::remote_scan::{
    PushdownAggregateKind, PushdownPartialAggregate, PushdownRowStream, PushdownScanColumn,
    PushdownScanRequest, PushdownScanner, PushdownScannerError, EXTRA_HANDLE_COLUMN_ID,
};
use tidb_executor::storage::StorageError;
use tidb_planner::physical_table_scan::PhysicalTableScanPlan;
use tidb_planner::tikv_scan_spec::{ScanColumnInfo, TiKvTableScanSpec};
use tidb_proto::tipb::{Aggregation, ExecType, Expr, ExprType, ScalarFuncSig};
use tidb_txnkv::KeyRange;

use crate::dag_request::{
    construct_aggregated_read_only_dag_req_with_conditions,
    construct_capped_read_only_dag_req_with_conditions, DagRequestContext, TiKvScanPlan,
};
use crate::real_tikv_read::RealTiKvSessionTransportFactory;
use crate::wide_scan_selection::{accepts, wide_scan_selection_conditions};

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
/// Go `mysql.TypeInt24`.
const MYSQL_TYPE_INT24: i32 = 9;
/// Go `mysql.TypeShort`.
const MYSQL_TYPE_SHORT: i32 = 2;
/// Go `mysql.TypeTiny`.
const MYSQL_TYPE_TINY: i32 = 1;

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
    /// Rows this node has received from coprocessor scans, for the receipt a
    /// live proof reads.
    rows_returned: Arc<AtomicU64>,
    /// Scans this node served remotely, against the ones it refused.
    scans_served: Arc<AtomicU64>,
    scans_refused: Arc<AtomicU64>,
    /// The executor list of each DAG this node sent, read back from the
    /// encoded request. This is the receipt that the Selection and the cap
    /// really travelled, rather than a claim that they did.
    requests: Arc<Mutex<Vec<String>>>,
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopScanStats {
    /// Rows the coprocessor sent to this node.
    pub rows_returned: u64,
    /// Scans served remotely.
    pub scans_served: u64,
    /// Scans refused, which fell back to the byte-level cursor.
    pub scans_refused: u64,
    /// One line per sent request, naming its DAG executors.
    pub requests: Vec<String>,
}

impl<F> CopScanSource<F> {
    /// Builds the capability over an already-running transport factory.
    ///
    /// The scanner holds no `time_zone` of its own: one object serves every
    /// connection of a node, so a zone here would be a process-wide constant
    /// no `SET time_zone` could correct. Each request carries the zone of the
    /// statement that issued it
    /// (`tidb_executor::PushdownStatementContext::time_zone`), which is where
    /// Go reads it from too (`ConstructDAGReq` -> `SessionVars.Location()`).
    #[must_use]
    pub fn new(factory: Arc<F>) -> Self {
        Self {
            factory,
            rows_returned: Arc::new(AtomicU64::new(0)),
            scans_served: Arc::new(AtomicU64::new(0)),
            scans_refused: Arc::new(AtomicU64::new(0)),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// The node's live coprocessor-scan counters.
    #[must_use]
    pub fn stats(&self) -> CopScanStats {
        CopScanStats {
            rows_returned: self.rows_returned.load(Ordering::Relaxed),
            scans_served: self.scans_served.load(Ordering::Relaxed),
            scans_refused: self.scans_refused.load(Ordering::Relaxed),
            requests: self
                .requests
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone(),
        }
    }
}

impl<F> CopScanSource<F>
where
    F: RealTiKvSessionTransportFactory + 'static,
    <F::Transport as QueryTransport>::Response: 'static,
{
    /// The covering-index shape of [`PushdownScanner::open`]: the only
    /// index request the executor sends today is a partial aggregation
    /// over encoded index-key ranges (`pushdown_index_partial_aggregate_cursor`),
    /// so every other index shape refuses by name.
    fn open_index_aggregate(
        &self,
        request: &PushdownScanRequest,
        index: &tidb_executor::remote_scan::PushdownIndexScan,
        refuse: impl Fn(&str) -> PushdownScannerError,
    ) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError> {
        let Some(aggregate) = request.aggregate.as_ref() else {
            return Err(refuse(
                "a bare index scan is not lowered; only the covering aggregate shape is",
            ));
        };
        if !request.predicates.is_empty()
            || request.limit.is_some()
            || request.topn.is_some()
            || request.output_offsets.is_some()
            || request.desc
        {
            return Err(refuse(
                "an index aggregation composes with nothing else in this lowering",
            ));
        }
        let columns = request
            .columns
            .iter()
            .map(scan_column)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| refuse("a column has no bounded coprocessor descriptor"))?;
        let field_types: Vec<FieldType> = aggregate.output_types();
        let aggregation = aggregation_to_pb(aggregate, &columns)
            .ok_or_else(|| refuse("a pushed aggregate has no bounded lowering"))?;
        let index_scan = tidb_proto::tipb::IndexScan {
            table_id: Some(request.table_id),
            index_id: Some(index.index_id),
            columns: columns
                .iter()
                .map(crate::dag_request::column_to_pb)
                .collect(),
            desc: Some(false),
            unique: Some(index.declared_unique),
            primary_column_ids: Vec::new(),
        };
        let (time_zone_name, time_zone_offset_secs) = request.statement.time_zone.dag_zone();
        let dag = crate::dag_request::construct_index_aggregated_dag_req(
            &DagRequestContext::new(
                time_zone_name,
                time_zone_offset_secs,
                request.statement.push_down_flags,
                EncodeType::Default,
            ),
            index_scan,
            aggregation,
            field_types.len(),
        )
        .map_err(|error| PushdownScannerError::Unsupported(error.to_string()))?;
        let summary = dag_summary(&dag);
        let key_ranges: Vec<KeyRange> = request
            .ranges
            .iter()
            .map(|(start, end)| KeyRange::new(start.clone(), end.clone()))
            .collect();
        let shapes = vec![
            ExecutorShape::new(ExecutorKind::IndexScan),
            ExecutorShape::new(ExecutorKind::Other),
        ];
        let plan = RemoteScanPlan {
            dag,
            envelope: RequestEnvelope::new(shapes),
            key_ranges,
            snapshot_ts: request.snapshot_ts,
            field_types,
            time_zone: request.statement.time_zone.clone(),
            warnings: request.statement.warnings.clone(),
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
        self.requests
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(summary);
        Ok(Box::new(CopRowStream {
            batches: Some(batches),
            pending: Vec::new().into_iter(),
            returned: 0,
        }))
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
        // Every request shape this lowering does not build into the DAG must
        // be refused BY NAME, never ignored: the caller assumes an accepted
        // request was answered whole, so silently serving raw table rows for
        // an aggregate or index request is wrong data, not degraded service.
        // A refused `topn` or cap is different -- the contract names those
        // best-effort and the caller retains the local stage either way.
        if let Some(index) = request.index.as_ref() {
            return self.open_index_aggregate(request, index, refuse);
        }
        if request.output_offsets.is_some() {
            return Err(refuse(
                "this coprocessor lowering does not narrow output columns",
            ));
        }
        let columns = request
            .columns
            .iter()
            .map(scan_column)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| refuse("a column has no bounded coprocessor descriptor"))?;
        // With an aggregation the response rows ARE the aggregate schema --
        // results first, group keys last -- so the decoder's types come from
        // the aggregate, not the scanned columns.
        let field_types: Vec<FieldType> = match &request.aggregate {
            Some(aggregate) => aggregate.output_types(),
            None => request
                .columns
                .iter()
                .map(|column| column.field_type.clone())
                .collect(),
        };
        let output_offsets: Vec<u32> = (0..columns.len() as u32).collect();

        // Every conjunct this lowering accepts travels; the rest simply stay
        // behind, because the scan source tests all of them locally anyway.
        let lowered: Vec<ScanPredicate> = request
            .predicates
            .iter()
            .filter(|predicate| accepts(predicate, &columns))
            .cloned()
            .collect();
        let conditions: Vec<Expr> = if lowered.is_empty() {
            Vec::new()
        } else {
            wide_scan_selection_conditions(&lowered, &columns).map_err(|error| {
                PushdownScannerError::Backend(StorageError::Backend(error.to_string()))
            })?
        };

        // The cap may only travel with a predicate that travelled WHOLE. A
        // conjunct left behind means TiKV counts its `limit` rows against a
        // weaker filter, and the conjuncts applied here then remove some of
        // those -- fewer rows than the query asked for, with nothing to say so.
        // This is the same hazard the staged buffer already guards against by
        // dropping the cap whenever rows are merged in locally.
        let remote_limit = if lowered.len() == request.predicates.len() {
            request.limit
        } else {
            None
        };

        // The contract on [`PushdownScanRequest::aggregate`]: when present,
        // every predicate must be lowered -- an aggregated row cannot be
        // re-filtered locally -- and no cap may ride along, because TiKV
        // would cap SOURCE rows while the caller asked to cap nothing.
        let aggregation = match &request.aggregate {
            Some(aggregate) => {
                if lowered.len() != request.predicates.len() {
                    return Err(refuse(
                        "a partial aggregation requires every predicate to lower",
                    ));
                }
                if request.limit.is_some() {
                    return Err(refuse(
                        "a partial aggregation does not compose with a row cap",
                    ));
                }
                Some(
                    aggregation_to_pb(aggregate, &columns)
                        .ok_or_else(|| refuse("a pushed aggregate has no bounded lowering"))?,
                )
            }
            None => None,
        };

        let mut spec = TiKvTableScanSpec::new(request.table_id, columns.clone());
        // Go's `desc` on the TableScan executor: the region walks the
        // ranges backwards. The ranges themselves stay ascending.
        spec.desc = request.desc;
        // A CLUSTERED table's primary key lives in the row key, so the
        // coprocessor needs the ids to rebuild those columns from it -- Go's
        // `newRowDecoder` falls back to exactly this list when no column
        // carries `PkHandle`. Dropping it here left every clustered
        // primary-key column NULL in a pushed-down scan.
        spec.primary_column_ids = request.primary_column_ids.clone();
        spec.primary_prefix_column_ids = request.primary_prefix_column_ids.clone();
        // The merge above reads the remote rows in record-key order, which is
        // the order it merges the staged buffer against.
        spec.keep_order = true;
        let scan = PhysicalTableScanPlan::init(0, 0, spec);
        // Go `ConstructDAGReq`: the zone comes from the SESSION VARIABLES of
        // the statement that issued this request, read fresh every time.
        let (time_zone_name, time_zone_offset_secs) = request.statement.time_zone.dag_zone();
        let dag_context = DagRequestContext::new(
            time_zone_name,
            time_zone_offset_secs,
            // Go `builder_utils.go`'s `sc.PushDownFlags()`. The literal
            // `0` this replaced is TiKV's strictest branch: a truncation
            // TiDB degrades to a 1292 warning failed the whole region
            // request instead.
            request.statement.push_down_flags,
            EncodeType::Default,
        );
        let dag = match aggregation {
            Some(aggregation) => construct_aggregated_read_only_dag_req_with_conditions(
                &dag_context,
                TiKvScanPlan::Table(&scan),
                &conditions,
                aggregation,
                field_types.len(),
            ),
            None => construct_capped_read_only_dag_req_with_conditions(
                &dag_context,
                TiKvScanPlan::Table(&scan),
                &conditions,
                remote_limit,
                &output_offsets,
            ),
        }
        .map_err(|error| PushdownScannerError::Unsupported(error.to_string()))?;

        let summary = dag_summary(&dag);
        let key_ranges: Vec<KeyRange> = request
            .ranges
            .iter()
            .map(|(start, end)| KeyRange::new(start.clone(), end.clone()))
            .collect();
        let mut shapes = vec![ExecutorShape::new(ExecutorKind::TableScan)];
        if !conditions.is_empty() {
            shapes.push(ExecutorShape::new(ExecutorKind::Other));
        }
        if request.aggregate.is_some() {
            shapes.push(ExecutorShape::new(ExecutorKind::Other));
        }
        if remote_limit.is_some() {
            shapes.push(ExecutorShape::new(ExecutorKind::Other));
        }
        let plan = RemoteScanPlan {
            dag,
            envelope: RequestEnvelope::new(shapes),
            key_ranges,
            snapshot_ts: request.snapshot_ts,
            field_types,
            time_zone: request.statement.time_zone.clone(),
            warnings: request.statement.warnings.clone(),
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
        self.requests
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(summary);
        Ok(Box::new(CopRowStream {
            batches: Some(batches),
            pending: Vec::new().into_iter(),
            returned: 0,
        }))
    }
}

/// The DAG's executor list, read back out of the built request.
///
/// A receipt is worth more than an assertion here: this reads what is about to
/// be encoded, so it cannot claim a Selection the request does not carry.
fn dag_summary(dag: &tidb_proto::tipb::DagRequest) -> String {
    let executors: Vec<String> = dag
        .executors
        .iter()
        .map(|executor| match executor.tp {
            Some(tp) if tp == ExecType::TypeTableScan as i32 => {
                let columns = executor
                    .tbl_scan
                    .as_ref()
                    .map_or(0, |scan| scan.columns.len());
                let table = executor
                    .tbl_scan
                    .as_ref()
                    .and_then(|scan| scan.table_id)
                    .unwrap_or_default();
                format!("TableScan(table {table}, {columns} columns)")
            }
            Some(tp) if tp == ExecType::TypeIndexScan as i32 => {
                let (table, index, columns) =
                    executor.idx_scan.as_ref().map_or((0, 0, 0), |scan| {
                        (scan.table_id(), scan.index_id(), scan.columns.len())
                    });
                format!("IndexScan(table {table}, index {index}, {columns} columns)")
            }
            Some(tp) if tp == ExecType::TypeSelection as i32 => format!(
                "Selection({} conditions)",
                executor
                    .selection
                    .as_ref()
                    .map_or(0, |selection| selection.conditions.len())
            ),
            Some(tp) if tp == ExecType::TypeLimit as i32 => format!(
                "Limit({})",
                executor
                    .limit
                    .as_ref()
                    .and_then(|limit| limit.limit)
                    .unwrap_or_default()
            ),
            Some(tp)
                if tp == ExecType::TypeAggregation as i32
                    || tp == ExecType::TypeStreamAgg as i32 =>
            {
                let (functions, groups) = executor
                    .aggregation
                    .as_ref()
                    .map_or((0, 0), |agg| (agg.agg_func.len(), agg.group_by.len()));
                let name = if tp == ExecType::TypeStreamAgg as i32 {
                    "StreamAgg"
                } else {
                    "HashAgg"
                };
                format!("{name}({functions} functions, {groups} group keys)")
            }
            other => format!("executor {other:?}"),
        })
        .collect();
    format!(
        "{} -> output offsets {:?}",
        executors.join(" | "),
        dag.output_offsets
    )
}

/// Everything the reader thread needs, owned independently of the caller.
struct RemoteScanPlan {
    dag: tidb_proto::tipb::DagRequest,
    /// The executor shapes the request builder reads for concurrency, which
    /// must match the DAG's own executor list.
    envelope: RequestEnvelope,
    /// The record intervals to read, ascending and disjoint. A whole-table
    /// scan is one; a `TableRangeScan` over a clustered handle is one per
    /// handle range, and the coprocessor request carries them all.
    key_ranges: Vec<KeyRange>,
    snapshot_ts: u64,
    field_types: Vec<FieldType>,
    time_zone: tidb_datatype::SessionTimeZone,
    /// The statement's warning sink, carried onto the scan thread. It is an
    /// `Arc` handler, so a warning appended here lands in the buffer
    /// `SHOW WARNINGS` reads even though the decode happens off-thread.
    warnings: WarningCollector,
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
    // Go `SetFromSessionVars`, which EVERY read in `pkg/distsql` runs. The
    // zero-value builder this replaced sent `Concurrency: 0` and an EMPTY
    // `ResourceGroupName`, neither of which any TiDB sends: a stock session
    // is `tidb_distsql_scan_concurrency = 15` and resource group `default`.
    //
    // The context is the STOCK one, not this session's: the remaining
    // `SetFromSessionVars` fields (replica read, statement priority, paging,
    // request source, task id, max_execution_time, tidb_kv_read_timeout, the
    // runaway checker) are session variables no `StmtContext` carries yet, so
    // threading them is a session-tier change this seam cannot make on its
    // own. What it can do is stop sending values that correspond to no
    // session at all.
    let mut builder = RequestBuilder::from_context(&DistSqlContext::new());
    builder
        .set_start_ts(plan.snapshot_ts)
        .set_keep_order(true)
        .set_non_partitioned_key_ranges(plan.key_ranges)
        .set_dag_request(plan.envelope, plan.dag.encode_to_vec());
    let request = builder
        .build_transport_request(Arc::clone(&cancellation))
        .map_err(|error| format!("{error:?}"))?;
    let mut runtime = InjectedQueryRuntime::new(&mut transport);
    let result = runtime
        .select_with_runtime_stats(
            &request,
            SelectInput::default(),
            // THE SESSION'S collector, not a fresh one: `response_channel`
            // appends TiKV's warnings in Go's order into whatever it is
            // given, and a fresh collector is dropped with them inside.
            QueryResultContext::new(plan.field_types, plan.warnings).with_time_zone(plan.time_zone),
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
/// Lowers the pushed partial aggregate into the TiPB `Aggregation` message
/// -- Go `PhysicalHashAgg.ToPB`/`PhysicalStreamAgg.ToPB` over
/// `aggregation.AggFuncToPBExpr`, narrowed to the column-argument shapes
/// [`PushdownPartialAggregate`] models. `None` refuses the whole scan.
///
/// The aggregate leaves carry no `field_type`: every argument is a scan
/// column whose full descriptor already travels in the `TableScan`
/// executor, which is where the region reads types and collations from.
fn aggregation_to_pb(
    aggregate: &PushdownPartialAggregate,
    columns: &[ScanColumnInfo],
) -> Option<Aggregation> {
    let encode_signed = |value: i64| {
        let mut encoded = Vec::with_capacity(8);
        tidb_codec::encode_int(&mut encoded, value);
        encoded
    };
    let column_ref = |offset: usize| -> Option<Expr> {
        (offset < columns.len()).then(|| Expr {
            tp: Some(ExprType::ColumnRef as i32),
            val: Some(encode_signed(
                i64::try_from(offset).expect("scan width fits i64"),
            )),
            children: Vec::new(),
            sig: Some(ScalarFuncSig::Unspecified as i32),
            field_type: None,
            has_distinct: Some(false),
        })
    };
    let agg = |tp: ExprType, child: Expr| Expr {
        tp: Some(tp as i32),
        val: None,
        children: vec![child],
        sig: Some(ScalarFuncSig::Unspecified as i32),
        field_type: None,
        has_distinct: Some(false),
    };
    // Go lowers `COUNT(*)` as `COUNT(1)` before it reaches the coprocessor.
    let one = Expr {
        tp: Some(ExprType::Int64 as i32),
        val: Some(encode_signed(1)),
        children: Vec::new(),
        sig: Some(ScalarFuncSig::Unspecified as i32),
        field_type: None,
        has_distinct: Some(false),
    };
    let message = |group_by: Vec<Expr>, agg_func: Vec<Expr>, streamed: bool| Aggregation {
        group_by,
        agg_func,
        streamed: Some(streamed),
    };
    match aggregate {
        PushdownPartialAggregate::Count { input_offset, .. } => Some(message(
            Vec::new(),
            vec![agg(ExprType::Count, column_ref(*input_offset)?)],
            false,
        )),
        PushdownPartialAggregate::Sum { input_offset, .. } => Some(message(
            Vec::new(),
            vec![agg(ExprType::Sum, column_ref(*input_offset)?)],
            false,
        )),
        PushdownPartialAggregate::GroupBy { input_offset, .. } => {
            Some(message(vec![column_ref(*input_offset)?], Vec::new(), false))
        }
        PushdownPartialAggregate::GroupBySum {
            group_offset,
            sum_offset,
            ..
        } => Some(message(
            vec![column_ref(*group_offset)?],
            vec![agg(ExprType::Sum, column_ref(*sum_offset)?)],
            false,
        )),
        PushdownPartialAggregate::Grouped {
            group_offsets,
            functions,
            streamed,
            ..
        } => {
            let group_by = group_offsets
                .iter()
                .map(|offset| column_ref(*offset))
                .collect::<Option<Vec<_>>>()?;
            let agg_func = functions
                .iter()
                .map(|function| {
                    let argument = match function.input_offset {
                        Some(offset) => column_ref(offset)?,
                        None => one.clone(),
                    };
                    let tp = match function.kind {
                        PushdownAggregateKind::Count => ExprType::Count,
                        PushdownAggregateKind::Sum => ExprType::Sum,
                        PushdownAggregateKind::Min => ExprType::Min,
                        PushdownAggregateKind::Max => ExprType::Max,
                    };
                    Some(agg(tp, argument))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(message(group_by, agg_func, *streamed))
        }
    }
}

fn scan_column(column: &PushdownScanColumn) -> Option<ScanColumnInfo> {
    // The integer family, with MySQL's default display width for each. The
    // width is metadata TiKV does not evaluate with -- the value is an integer
    // either way -- but it is what the catalog declares, so it is what the
    // descriptor carries.
    let code = column.field_type.code();
    let (tp, column_len) = match code {
        FieldTypeCode::LongLong => (MYSQL_TYPE_LONGLONG, 20),
        FieldTypeCode::Long => (MYSQL_TYPE_LONG, 11),
        FieldTypeCode::Int24 => (MYSQL_TYPE_INT24, 9),
        FieldTypeCode::Short => (MYSQL_TYPE_SHORT, 6),
        FieldTypeCode::Tiny => (MYSQL_TYPE_TINY, 4),
        // The character-string family. Unlike the integer widths above, a
        // string column's declared LENGTH is not decoration TiKV ignores: it
        // is what a `VARCHAR(n)` value is checked and compared against, so it
        // is copied from the catalog rather than defaulted. Go's
        // `util.ColumnToProto` copies `c.GetFlen()` for every family alike.
        FieldTypeCode::Varchar
        | FieldTypeCode::VarString
        | FieldTypeCode::String
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::Blob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob => (
            i32::from(code.mysql_type()),
            i32::try_from(column.field_type.flen()).unwrap_or(-1),
        ),
        _ => return None,
    };
    let mut flag = 0;
    if column.field_type.is_unsigned() {
        flag |= UNSIGNED_FLAG;
    }
    if column.is_handle {
        flag |= NOT_NULL_FLAG | PRI_KEY_FLAG;
    }
    // Go `util.ColumnToProto`:
    // `collate.RewriteNewCollationIDIfNeeded(mysql.CollationNames[c.GetCollate()])`.
    // An integer column's collation is `binary`, which resolves to the same
    // constant this used to hard-code; a string column's is its own, and it is
    // what tells TiKV which collator to compare and case-fold with. The
    // predicate lowering reads this very field back
    // (`tidb_exec::wide_scan_selection`), so the leaf and the scan descriptor
    // cannot disagree about the collator by construction.
    let collation = if column.field_type.is_string() {
        tidb_datatype::collation_to_proto(column.field_type.collation_name())
    } else {
        BINARY_COLLATION_ID
    };
    // Go `util.ColumnToProto` encodes the origin default into
    // `ColumnInfo.default_val`; the region fills it in for a row written
    // before the column existed, which is the only place that knows.
    let default_val = match &column.origin_default {
        Some(datum) => Some(tidb_codec::encode_value(std::slice::from_ref(datum)).ok()?),
        None => None,
    };
    Some(ScanColumnInfo {
        column_id: column.id,
        tp,
        collation,
        column_len,
        decimal: 0,
        flag,
        pk_handle: column.is_handle,
        default_val,
        ..ScanColumnInfo::default()
    })
}

/// Whether a request names the implicit `_tidb_rowid` handle column, which is
/// the shape a table with no integer primary key scans with.
#[must_use]
pub fn requests_extra_handle(request: &PushdownScanRequest) -> bool {
    request
        .handle_index
        .and_then(|index| request.columns.get(index))
        .is_some_and(|column| column.id == EXTRA_HANDLE_COLUMN_ID)
}
