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

//! Go `TableReaderExecutor.buildResp` over the TiKV coprocessor: the
//! executor lowers a base-table (or index) read into a `tipb.DAGRequest`
//! (Go `builder_utils.go`'s `constructDAGReq`), encodes it into a
//! `kv.Request` with `distsql.RequestBuilder`, and sends it through the
//! store's `kv.Client` (`distsql.SelectWithRuntimeStats`), streaming the
//! response back as rows.
//!
//! [`crate::dag_request`] lowers the scan, the Selection and the cap into a
//! `DAGRequest`; `tidb-distsql` owns ranges, region
//! tasks, dispatch, retry and response decoding; the client below
//! ([`tidb_txnkv::Client`], Go `kv.Storage.GetClient()`) carries the encoded
//! request onto the wire.
//!
//! # The two properties that make it safe
//!
//! * **It answers from the statement's own snapshot.** The request carries
//!   [`PushdownScanRequest::snapshot_ts`], which the storage filled in from
//!   the snapshot the statement is bound to. A scan that read at any other
//!   timestamp would not be repeatable read.
//! * **It never has the last word.** The scan source applies the pushed
//!   conjuncts and the cap to every row it emits, and merges the session's
//!   staged buffer on top (see [`crate::remote_scan`]). So this module may
//!   lower all, some, or none of the predicate and the answer is the same;
//!   only the number of rows on the wire changes.
//!
//! # What it refuses
//!
//! The scan descriptor follows Go `util.ColumnToProto`: every concrete
//! MySQL/TiDB field type carries its wire type code, declared length/scale,
//! flags, collation, and (for `ENUM`/`SET`) element names. Only an unknown or
//! unresolved type is refused. This projection gate is separate from the
//! predicate lowering's own type gate: a supported column may still leave a
//! `WHERE` expression above the scan when TiKV cannot evaluate that function.
//! A refused shape falls back to the byte-level cursor, so it is slower but
//! never changes the answer.

use std::sync::Arc;

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_distsql::DistSqlContext;
use tidb_distsql::ExecutorKind;
use tidb_distsql::ExecutorShape;
use tidb_distsql::QueryResultContext;
use tidb_distsql::RequestBuilder;
use tidb_distsql::RequestEnvelope;
use tidb_distsql::SelectInput;
use tidb_distsql::select_response::SelectResponseIter;
use tidb_txnkv::CancelHandle;
use tidb_context::WarningCollector;
use tidb_planner_core_operator_logicalop::tikv_scan_spec::ScanColumnInfo;
use tidb_txnkv::KeyRange;

use tidb_distsql::remote_scan::PushdownScanColumn;
use tidb_kv::storage::StorageError;

/// The per-process wire receipt behind `cluster-session-smoke --cop`:
/// how many rows the coprocessor actually sent over the wire for the
/// current statement, and which executors the last admitted DAG carried.
/// The smoke binary resets before a statement and prints the pair after
/// it; the RealTiKV harness greps those lines as the pushdown receipt
/// instead of trusting a claimed number.
pub mod scan_receipt {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    static WIRE_ROWS: AtomicU64 = AtomicU64::new(0);
    static SHAPE: Mutex<String> = Mutex::new(String::new());

    /// Clears both halves for the next statement.
    pub fn reset() {
        WIRE_ROWS.store(0, Ordering::SeqCst);
        *SHAPE.lock().unwrap_or_else(|poison| poison.into_inner()) = String::new();
    }

    /// Records the executor list the last opened DAG carried.
    pub fn record_shape(shape: String) {
        *SHAPE.lock().unwrap_or_else(|poison| poison.into_inner()) = shape;
    }

    /// Adds the rows one wire chunk carried.
    pub fn add_wire_rows(rows: u64) {
        WIRE_ROWS.fetch_add(rows, Ordering::SeqCst);
    }

    /// The (wire rows, DAG shape) observed since the last reset.
    #[must_use]
    pub fn snapshot() -> (u64, String) {
        (
            WIRE_ROWS.load(Ordering::SeqCst),
            SHAPE
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone(),
        )
    }
}

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: i32 = 1;
/// Go `mysql.PriKeyFlag`.
const PRI_KEY_FLAG: i32 = 2;
/// The statement's side of a read's plan (what Go `SetFromSessionVars` and
/// the reader's context give every request), around `dag`; the read's own
/// ranges, order and result types are left empty.
fn statement_scan_plan(
    statement: &tidb_distsql::remote_scan::PushdownStatementContext,
    dag: tidb_proto::tipb::DagRequest,
    envelope: RequestEnvelope,
) -> RemoteScanPlan {
    RemoteScanPlan {
        cancellation: statement.memory.coprocessor_request_cancellation(),
        rpc_interceptor: statement.rpc_interceptor.clone(),
        kv_context: statement
            .kv_context
            .as_ref()
            .map(tidb_txnkv::StatementKvContextCell::get)
            .unwrap_or_default(),
        dag,
        envelope,
        key_ranges: Vec::new(),
        key_range_hints: Vec::new(),
        snapshot_ts: 0,
        keep_order: false,
        allow_unordered: false,
        desc: false,
        field_types: Vec::new(),
        intermediate_output_types: Vec::new(),
        paging: statement.paging,
        paging_min_size: None,
        time_zone: statement.time_zone.clone(),
        resource_group_name: statement.resource_group_name.clone(),
        request_source: statement.request_source.clone(),
        replica_read: statement.replica_read,
        read_replica_scope: statement.read_replica_scope.clone(),
        is_staleness: statement.is_staleness,
        rc_check_ts: statement.rc_check_ts,
        weak_consistency: statement.weak_consistency,
        read_policy: statement.read_policy.clone(),
        estimated_net_bytes: statement.estimated_net_bytes,
        max_execution_time_ms: statement.max_execution_time_ms,
        priority: statement.priority,
        not_fill_cache: statement.not_fill_cache,
        dist_sql_scan_concurrency: statement.dist_sql_scan_concurrency,
        store_batch_size: statement.store_batch_size,
        query_cop_store_limiter: statement.query_cop_store_limiter.clone(),
        runaway_checker: statement.runaway_checker.clone(),
        warnings: statement.warnings.clone(),
        cop_lite_worker: Arc::clone(&statement.cop_lite_worker),
        cop_plan_ids: Vec::new(),
        root_plan_id: 0,
        keys_read: statement.keys_read.clone(),
        cop_response_sink: statement.cop_response_sink.clone(),
        memory_tracker: None,
        session_mem_tracker: Arc::clone(statement.memory.session_tracker()),
        enabled_rate_limit_action: statement.enabled_rate_limit_action,
        tiflash: None,
    }
}

/// What a `kv.TiFlash` table read adds to its request (Go
/// `TableReaderExecutor.buildKVReq` / `buildKVReqForPartitionTableScan`).
pub(crate) struct TableRead {
    /// Go `kv.Request.StoreType`.
    pub store_type: tidb_kv::store_type::StoreType,
    /// Go `dagPB`.
    pub dag: tidb_proto::tipb::DagRequest,
    /// Go `SetHandleRanges` / `SetKeyRanges`' ranges; empty for a partition
    /// table scan.
    pub key_ranges: Vec<KeyRange>,
    /// Go `SetTableHandles`' row-count hints, aligned with `key_ranges`;
    /// empty for a request over ranges.
    pub key_range_hints: Vec<usize>,
    /// Go `SetPartitionIDAndRanges`.
    pub partition_id_and_ranges: Vec<tidb_txnkv::PartitionIdAndRanges>,
    /// Go `SetAllowBatchCop(e.batchCop)`.
    pub batch_cop: bool,
    /// Go `SetFromInfoSchema`'s `SchemaVar`.
    pub schema_version: i64,
    /// Go `SetConnIDAndConnAlias`.
    pub connection_id: u64,
    pub connection_alias: String,
    pub start_ts: u64,
    pub keep_order: bool,
    pub desc: bool,
    /// Go `SetPaging`.
    pub paging: bool,
    /// Go `exec.RetTypes(e)`.
    pub field_types: Vec<FieldType>,
    /// Go `getPhysicalPlanIDs(e.plans)` and `e.ID()`.
    pub cop_plan_ids: Vec<isize>,
    pub root_plan_id: isize,
    /// Go `SetMemTracker`: the tracker the select result charges its
    /// responses to, none for a request built without one.
    pub mem_tracker: Option<Arc<tidb_memory::Tracker>>,
    /// Go `SelectResult.IntoIter`'s `intermediateFieldTypes`: the row types
    /// of each intermediate channel the DAG declares.
    pub intermediate_output_types: Vec<Vec<FieldType>>,
}

/// Go `TableReaderExecutor.SelectResult`: the request through `client` (a
/// `kv.TiFlash` batch request goes to `CopClient.sendBatch`).
pub(crate) fn open_table_read(
    client: &dyn tidb_txnkv::Client,
    statement: &tidb_distsql::remote_scan::PushdownStatementContext,
    read: TableRead,
) -> Result<SelectResponseIter, String> {
    let memory_tracker = read.mem_tracker;
    let envelope = dag_envelope(&read.dag);
    let plan = RemoteScanPlan {
        key_ranges: read.key_ranges,
        key_range_hints: read.key_range_hints,
        snapshot_ts: read.start_ts,
        paging: read.paging,
        keep_order: read.keep_order,
        // Go `kv.Request.KeepOrder = false`: the copIterator answers each
        // region's response as it completes.
        allow_unordered: !read.keep_order,
        desc: read.desc,
        field_types: read.field_types,
        cop_plan_ids: read.cop_plan_ids,
        root_plan_id: read.root_plan_id,
        memory_tracker,
        intermediate_output_types: read.intermediate_output_types,
        tiflash: (read.store_type == tidb_kv::store_type::StoreType::TiFlash).then(|| TiFlashRequest {
            partition_id_and_ranges: read.partition_id_and_ranges,
            batch_cop: read.batch_cop,
            schema_version: read.schema_version,
            connection_id: read.connection_id,
            connection_alias: read.connection_alias,
        }),
        ..statement_scan_plan(statement, read.dag, envelope)
    };
    scan_result(|| open_scan(client, plan))
}

/// Go `RequestBuilder.SetDAGRequest`'s view of `dag`: its list executors'
/// kinds and a Limit's count (a tree-based request has none).
fn dag_envelope(dag: &tidb_proto::tipb::DagRequest) -> RequestEnvelope {
    use tidb_proto::tipb::ExecType;
    RequestEnvelope::new(
        dag.executors
            .iter()
            .map(|executor| match executor.tp() {
                ExecType::TypeTableScan => ExecutorShape::new(ExecutorKind::TableScan),
                ExecType::TypeIndexScan => ExecutorShape::new(ExecutorKind::IndexScan),
                ExecType::TypePartitionTableScan => ExecutorShape::new(ExecutorKind::PartitionTableScan),
                ExecType::TypeIndexLookUp => ExecutorShape::new(ExecutorKind::IndexLookup),
                ExecType::TypeLimit => ExecutorShape::limit(
                    executor.limit.as_ref().map_or(0, |limit| limit.limit()),
                    executor.parent_idx.map(|idx| idx as usize),
                ),
                _ => ExecutorShape::new(ExecutorKind::Other),
            })
            .collect(),
    )
}

/// The `kv.TiFlash` side of a request.
struct TiFlashRequest {
    partition_id_and_ranges: Vec<tidb_txnkv::PartitionIdAndRanges>,
    batch_cop: bool,
    schema_version: i64,
    connection_id: u64,
    connection_alias: String,
}

/// Everything needed to open one response on the query worker.
struct RemoteScanPlan {
    cancellation: Arc<CancelHandle>,
    rpc_interceptor: tidb_txnkv::RpcInterceptorOption,
    /// The issuing statement's ctx values: its `RUDetails` and its
    /// `StmtCtx.GetResourceGroupTagger`.
    kv_context: tidb_txnkv::StatementKvContext,
    dag: tidb_proto::tipb::DagRequest,
    /// The executor shapes the request builder reads for concurrency, which
    /// must match the DAG's own executor list.
    envelope: RequestEnvelope,
    /// The record intervals to read, ascending by encoded key. A whole-table
    /// scan is one; a `TableRangeScan` over a clustered handle is one per
    /// handle range, and the coprocessor request carries them all. Go's
    /// `SetTableHandles` can intentionally supply equal point intervals for
    /// duplicate index entries, so this list is not universally disjoint.
    key_ranges: Vec<KeyRange>,
    /// Go `RequestBuilder.SetTableHandles` row-count hints, aligned with
    /// `key_ranges`; empty for scans without grouped handle cardinalities.
    key_range_hints: Vec<usize>,
    snapshot_ts: u64,
    /// Whether region tasks and the response stream must preserve key order.
    keep_order: bool,
    /// Whether region answers may be consumed as they complete. Only set for
    /// reads whose caller re-orders (or ignores) row order.
    allow_unordered: bool,
    /// Whether DistSQL must visit region tasks in descending key order. Go
    /// carries this through `pkg/distsql/request_builder.go`'s
    /// `RequestBuilder.SetDesc`; `pkg/store/copr/coprocessor.go`'s
    /// `buildCopTasks` then reverses the task list. The TableScan executor
    /// separately carries the direction for rows inside each region.
    desc: bool,
    field_types: Vec<FieldType>,
    /// Go `SelectResult.IntoIter`'s `intermediateFieldTypes`: the row types
    /// of each intermediate channel the DAG declares.
    intermediate_output_types: Vec<Vec<FieldType>>,
    /// Go `RequestBuilder.SetPaging`.
    paging: bool,
    /// Optional Go IndexLookUp first-window paging floor for index scans.
    paging_min_size: Option<u64>,
    time_zone: tidb_datatype::SessionTimeZone,
    /// Go `StmtCtx.ResourceGroupName` for this request.
    resource_group_name: String,
    /// Go `DistSQLContext`'s request source for this request.
    request_source: tidb_txnkv::RequestSource,
    /// Go `SessionVars.GetReplicaRead()` for this request.
    replica_read: tidb_txnkv::ReplicaReadType,
    read_replica_scope: String,
    is_staleness: bool,
    rc_check_ts: bool,
    weak_consistency: bool,
    read_policy: tidb_distsql::remote_scan::CoprocessorReadPolicy,
    estimated_net_bytes: f64,
    max_execution_time_ms: u64,
    /// Go `RequestBuilder.getKVPriority(StmtCtx.Priority)` for this request.
    priority: tidb_mysql::Priority,
    /// Go `StmtCtx.NotFillCache` for this request.
    not_fill_cache: bool,
    /// Go `SessionVars.DistSQLScanConcurrency()` for this request.
    dist_sql_scan_concurrency: u64,
    /// Go `SessionVars.StoreBatchSize` for this request.
    store_batch_size: u64,
    /// Query-scoped per-store limiter shared by all region tasks for this
    /// statement.
    query_cop_store_limiter: Option<Arc<tidb_txnkv::QueryCopStoreLimiter>>,
    /// Go `StmtCtx.RunawayChecker` for this request.
    runaway_checker: Option<Arc<dyn tidb_txnkv::RunawayChecker>>,
    /// The statement's warning sink. It is an `Arc` handler, so warnings
    /// appended while the query worker decodes land in the buffer
    /// `SHOW WARNINGS` reads.
    warnings: WarningCollector,
    cop_lite_worker: Arc<std::sync::atomic::AtomicBool>,
    /// Physical IDs aligned with TiPB executors for runtime summaries.
    cop_plan_ids: Vec<isize>,
    root_plan_id: isize,
    /// The reader's memory tracker the select result charges.
    memory_tracker: Option<Arc<tidb_memory::Tracker>>,
    /// Go `kv.ClientSendOption.SessionMemTracker` / `EnabledRateLimitAction`.
    session_mem_tracker: Arc<tidb_memory::Tracker>,
    enabled_rate_limit_action: bool,
    /// The statement's `tidb_max_keys_read` budget and keys-examined counter.
    keys_read: crate::KeysReadBudget,
    /// The statement's consumer of per-response execution details.
    cop_response_sink: Option<Arc<dyn tidb_distsql::select_response::CopResponseSink>>,
    /// Set for a read of the `kv.TiFlash` store.
    tiflash: Option<TiFlashRequest>,
}

/// Go copIteratorWorker/liteCopIteratorWorker turns task panics into errors.
fn scan_result<T>(task: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)).unwrap_or_else(|payload| {
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic payload");
        Err(format!("coprocessor scan panicked: {message}"))
    })
}

/// Go executor.newClosestReadAdjuster. The number of tasks is known only
/// after region splitting; paging/retries retain the resulting request.
fn closest_read_adjuster(
    mode: tidb_txnkv::ReplicaReadType,
    bytes: f64,
    threshold: i64,
    zone: String,
) -> Option<Arc<dyn tidb_txnkv::CoprocessorRequestAdjuster>> {
    if mode != tidb_txnkv::ReplicaReadType::ClosestAdaptive {
        return None;
    }
    Some(Arc::new(
        move |request: &mut tidb_txnkv::Request, count: usize| {
            let per_task = bytes / count as f64;
            // Go amd64 converts non-finite/out-of-range float64 to MinInt64.
            let estimate = if !per_task.is_finite() || per_task >= 9_223_372_036_854_775_808.0 {
                i64::MIN
            } else {
                per_task as i64
            };
            if estimate >= threshold {
                request.match_store_labels.push(tidb_txnkv::StoreLabel {
                    key: "zone".to_owned(),
                    value: zone.clone(),
                });
                true
            } else {
                request.replica_read = tidb_txnkv::ReplicaReadType::Leader;
                false
            }
        },
    ))
}

fn open_scan(
    client: &dyn tidb_txnkv::Client,
    plan: RemoteScanPlan,
) -> Result<SelectResponseIter, String> {
    use prost::Message;

    let cancellation = plan.cancellation;
    let cop_response_sink = plan.cop_response_sink.clone();
    let memory_tracker = plan.memory_tracker.clone();
    // Go `SetFromSessionVars`, which EVERY read in `pkg/distsql` runs. The
    // zero-value builder this replaced sent `Concurrency: 0` and an EMPTY
    // `ResourceGroupName`, neither of which any TiDB sends: a stock session
    // is `tidb_distsql_scan_concurrency = 15` and resource group `default`.
    //
    // Copy the retained statement policy, including timeout/hint and busy
    // thresholds, rather than recreating session defaults on the worker.
    let mut context = DistSqlContext::new();
    context.request.dist_sql_concurrency = plan.dist_sql_scan_concurrency;
    context.request.store_batch_size = plan.store_batch_size;
    context.request.resource_group_name = plan.resource_group_name;
    context.request.in_restricted_sql = plan.request_source.internal;
    context.request.request_source_type = plan.request_source.source_type;
    context.request.explicit_request_source_type = plan.request_source.explicit_source_type;
    context.request.replica_read = plan.replica_read;
    context.request.load_based_replica_read_threshold_ns = plan.read_policy.busy_threshold_ns;
    context.request.tikv_client_read_timeout_ms = plan.read_policy.read_timeout_ms;
    context.request.max_execution_time_ms = plan.max_execution_time_ms;
    context.request.priority = plan.priority;
    context.request.not_fill_cache = plan.not_fill_cache;
    // Go `DistSQLContext.RCCheckTS`: the request's `kv.RCCheckTS` isolation.
    context.request.rc_check_ts = plan.rc_check_ts;
    context.request.weak_consistency = plan.weak_consistency;
    context.request.query_cop_store_limiter = plan.query_cop_store_limiter;
    context.request.runaway_checker = plan.runaway_checker;
    // Go `SetFromSessionVars` copies `MaxKeysRead` and the statement-wide
    // `MaxKeysReadCounter`; the counter is passed even without a limit so the
    // processed keys still reach `tidb_keys_examined`.
    context.request.max_keys_read = plan.keys_read.max();
    context.execution.max_keys_read_counter = Some(plan.keys_read.counter());
    context.request.paging.enabled = plan.paging;
    if let Some(min_size) = plan.paging_min_size {
        // Go's buildIndexSelectResultForRange raises both paging bounds to
        // the worker's first handle batch. Keep the normal session defaults
        // for every request without this IndexLookUp hint.
        context.request.paging.min_size = context.request.paging.min_size.max(min_size);
        context.request.paging.max_size = context.request.paging.max_size.max(min_size);
    }
    let mut builder = RequestBuilder::from_context(&context);
    builder
        .set_read_replica_scope(plan.read_replica_scope)
        .set_is_staleness(plan.is_staleness);
    // Go `SetFromSessionVars`: `SetResourceGroupTagger(sv.StmtCtx.
    // GetResourceGroupTagger())`.
    if let Some(tagger) = &plan.kv_context.resource_group_tagger {
        builder.set_resource_group_tagger(tagger());
    }
    builder.set_closest_replica_read_adjuster(closest_read_adjuster(
        plan.replica_read,
        plan.estimated_net_bytes,
        plan.read_policy.closest_read_threshold,
        tidb_config::config_tree::config::get_txn_scope_from_config(),
    ));
    // Go's `RequestBuilder.SetTableHandles` preserves one row-count hint per
    // grouped range. Keep the ordinary no-hint path for full/table scans and
    // refuse misaligned metadata rather than attaching a hint to the wrong
    // region task.
    if !plan.key_range_hints.is_empty() && plan.key_range_hints.len() == plan.key_ranges.len() {
        builder.set_key_ranges_with_hints(plan.key_ranges, plan.key_range_hints);
    } else {
        builder.set_non_partitioned_key_ranges(plan.key_ranges);
    }
    let is_tiflash = plan.tiflash.is_some();
    if let Some(tiflash) = plan.tiflash {
        builder
            .set_store_type(tidb_txnkv::StoreType::TiFlash)
            .set_allow_batch_cop(tiflash.batch_cop)
            .set_schema_version(tiflash.schema_version)
            .set_connection(tiflash.connection_id, tiflash.connection_alias)
            .set_partition_id_and_ranges(tiflash.partition_id_and_ranges);
    }
    builder
        .set_start_ts(plan.snapshot_ts)
        .set_keep_order(plan.keep_order)
        .set_allow_unordered_response(plan.allow_unordered)
        .set_desc(plan.desc)
        .set_mem_tracker(memory_tracker.clone())
        .set_dag_request(plan.envelope, plan.dag.encode_to_vec());
    let request = builder
        .build_transport_request(Arc::clone(&cancellation))
        .map_err(|error| format!("{error:?}"))?
        .with_cop_lite_worker(plan.cop_lite_worker)
        .with_session_mem_tracker(plan.session_mem_tracker, plan.enabled_rate_limit_action)
        .with_rpc_interceptor(plan.rpc_interceptor)
        .with_kv_context(plan.kv_context);
    // Go `distsql.Select`: `if kvReq.StoreType == kv.TiFlash {
    // option.AppendWarning = dctx.AppendWarning }`.
    let request = if is_tiflash {
        let warnings = plan.warnings.clone();
        request.with_append_warning(tidb_txnkv::AppendWarning(Arc::new(move |warning| {
            warnings.append_warning(warning);
        })))
    } else {
        request
    };
    let result = tidb_distsql::select_with_runtime_stats_over_client(
        client,
        &request,
        SelectInput::default(),
        // THE SESSION'S collector, not a fresh one: `response_channel`
        // appends TiKV's warnings in Go's order into whatever it is
        // given, and a fresh collector is dropped with them inside.
        QueryResultContext::new(plan.field_types.clone(), plan.warnings)
            .with_time_zone(plan.time_zone),
        plan.cop_plan_ids,
        plan.root_plan_id,
        true,
    )
    .map_err(|error| error.to_string())?;
    Ok(result
        .into_select_iter(plan.intermediate_output_types)
        .with_cop_response_sink(cop_response_sink)
        .with_memory_tracker(memory_tracker))
}

pub(crate) fn select_response_error(error: tidb_copr::response_channel::ResponseChannelError) -> StorageError {
    match error {
        // Go `selectResult.fetchResp`: a `SelectResponse` error is
        // `ClassTiKV.Synthesize`d; the iterator's own error is returned as is.
        tidb_copr::response_channel::ResponseChannelError::SelectResponse { code, message } => {
            StorageError::Sql(tidb_dbterror::ErrClass::synthesize_tikv(code as u16, message))
        }
        tidb_copr::response_channel::ResponseChannelError::Sql { code, message } => {
            StorageError::Sql(tidb_dbterror::ErrClass::response_iterator_error(code, message))
        }
        other => StorageError::Backend(other.to_string()),
    }
}

/// Go `util.ColumnToProto`: the coprocessor descriptor of one scanned column,
/// or `None` for a type with no stable TiPB interpretation.
pub fn column_to_proto(column: &PushdownScanColumn) -> Option<ScanColumnInfo> {
    let code = column.field_type.code();
    // `FieldTypeCode::Unknown` and `Unspecified` have no stable TiPB
    // interpretation. Every named code below is accepted by Go's
    // `ColumnToProto`, including the integer, temporal, floating-point,
    // JSON, ENUM/SET, BIT, geometry and vector families.
    if matches!(code, FieldTypeCode::Unknown(_) | FieldTypeCode::Unspecified) {
        return None;
    }
    let tp = i32::from(code.mysql_type());
    // Go writes `int32(c.GetFlen())`/`int32(c.GetDecimal())` verbatim.  The
    // parser deliberately leaves some concrete types (for example a bare
    // `GEOMETRY`) at `-1`, and very wide lengths such as JSON's max width also
    // narrow to `-1` in the protobuf field.  Preserve that metadata instead
    // of treating it as an unsupported column.
    let column_len = column.field_type.flen() as i32;
    let decimal = column.field_type.decimal() as i32;
    let mut flag = i32::try_from(column.field_type.flags()).ok()?;
    if column.is_handle {
        flag |= NOT_NULL_FLAG | PRI_KEY_FLAG;
    }
    // Go `util.ColumnToProto`:
    // `collate.RewriteNewCollationIDIfNeeded(mysql.CollationNames[c.GetCollate()])`.
    // An integer column's collation is `binary`, rewritten like any other
    // (`-63` under the new collation framework); a string column's is its
    // own, and it is what tells TiKV which collator to compare and case-fold
    // with.
    let collation = if matches!(
        code,
        FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::String
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::Blob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
            | FieldTypeCode::Enum
            | FieldTypeCode::Set
    ) {
        tidb_datatype::collation_to_proto(column.field_type.collation_name())
    } else {
        tidb_datatype::collation_to_proto("binary")
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
        decimal,
        flag,
        elems: column
            .field_type
            .elems_snapshot()
            .into_iter()
            .map(|elem| elem.to_string())
            .collect(),
        pk_handle: column.is_handle,
        default_val,
        array: column.field_type.is_array(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_read_threshold_is_per_task_and_preserves_existing_labels() {
        for (bytes, tasks, hit) in [
            (8191.9, 2, false),
            (8192.0, 2, true),
            (8192.0, 3, false),
            (0.0, 0, false),
        ] {
            let mut request = tidb_txnkv::Request::default();
            request.replica_read = tidb_txnkv::ReplicaReadType::ClosestAdaptive;
            request.match_store_labels.push(tidb_txnkv::StoreLabel {
                key: "rack".into(),
                value: "r1".into(),
            });
            let adjuster =
                closest_read_adjuster(request.replica_read, bytes, 4096, "zone-a".into()).unwrap();
            assert_eq!(adjuster.adjust(&mut request, tasks), hit);
            assert_eq!(request.match_store_labels.len(), if hit { 2 } else { 1 });
            assert_eq!(
                request.replica_read,
                if hit {
                    tidb_txnkv::ReplicaReadType::ClosestAdaptive
                } else {
                    tidb_txnkv::ReplicaReadType::Leader
                }
            );
            if hit {
                assert_eq!(request.match_store_labels[1].value, "zone-a");
            }
        }
        for mode in [
            tidb_txnkv::ReplicaReadType::Leader,
            tidb_txnkv::ReplicaReadType::Follower,
            tidb_txnkv::ReplicaReadType::Closest,
        ] {
            assert!(closest_read_adjuster(mode, 1e9, 1, "z".into()).is_none());
        }
    }

}

