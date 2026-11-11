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

//! Go `MPPGather` over `executor/internal/mpp/local_mpp_coordinator.go`,
//! narrowed to the single-fragment table scan the planner builds for a forced
//! TiFlash read (TableReader -> ExchangeSender(PassThrough) -> TableScan, mpp
//! version 3): which reads this tier dispatches to TiFlash at all, the task
//! metas of one gather, the root fragment's DAG (`appendMPPDispatchReq` over
//! `constructDAGReq`), the dispatch loop over the store's `kv.MPPClient`, the
//! gather of the root tasks' streams, Go's `mppErrRecovery`, and the rows.
//! Task locations, the RPCs and the streams themselves are the store's (Go
//! `copr.MPPClient`, `tidb_copr::mpp_client`).

use std::collections::VecDeque;
use std::sync::Arc;

use prost::Message;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_distsql::mpp_result_metadata;
use tidb_copr::response_channel::ResponseChannelError;
use tidb_distsql::select_response::SelectResponseIter;
use tidb_txnkv::{
    MppBuildTasksRequest, MppClient, MppContext, MppError, MppStreamResponse, QueryResponseError,
    QueryResultSubset,
};
use tidb_proto::mpp::TaskMeta;
use tidb_proto::tipb::{
    DagRequest, EncodeType, Endian, ExchangeSender as PbExchangeSender, ExchangeType, ExecType,
    Executor, TableScan,
};

use crate::cop_reader::column_to_proto;
use crate::dag_request::{column_to_pb, DagRequestContext, DEFAULT_DIV_PRECISION_INCREMENT};
use tidb_distsql::remote_scan::PushdownStatementContext;
use tidb_distsql::remote_scan::PushdownScannerError;
use tidb_distsql::remote_scan::PushdownScanRequest;
use tidb_distsql::remote_scan::PushdownRowStream;
use tidb_distsql::remote_scan::PushdownReadEngine;
use tidb_kv::storage::StorageError;
use crate::StatementMemory;
use crate::recovery_handler::{RecoveryHandler, RecoveryInfo};

/// The request shapes the MPP lowering builds: the single-fragment full
/// scan. Every carried operator is refused BY NAME rather than served from
/// row storage: the caller asked for the columnar engine, and a silent TiKV
/// fallback would be the wrong answer, not a slower one.
pub fn check_mpp_request(request: &PushdownScanRequest) -> Result<(), &'static str> {
    if request.read_engine != PushdownReadEngine::TiFlash {
        return Err("the request did not name the columnar engine");
    }
    if !request.conditions.is_empty() {
        return Err("pushed Selection is not lowered yet");
    }
    if request.topn.is_some() {
        return Err("pushed TopN is not lowered yet");
    }
    if request.aggregate.is_some() {
        return Err("partial aggregation is not lowered yet");
    }
    if request.desc || request.keep_order {
        return Err("ordered scans are not lowered yet");
    }
    if request.index.is_some() {
        return Err("index scans have no TiFlash path");
    }
    Ok(())
}

/// The first task's meta of one gather at `address`, and the TiDB-side
/// receiver's (Go `kv.MPPTask.ToPB` for the root task and for the
/// coordinator's pseudo task).
pub fn mpp_task_metadata(
    snapshot_ts: u64,
    statement: &PushdownStatementContext,
    address: &str,
) -> (TaskMeta, TaskMeta) {
    let query = statement.mpp_query_info.query_id(statement.mpp_server_id);
    let gather_id = statement.mpp_query_info.alloc_gather_id();

    // Go `EstablishMPPConns`' receiver meta: the TiDB coordinator-side
    // pseudo task, task_id -1. The SAME encoding rides the sender's
    // encoded_task_meta in the dispatch (Go `buildDAGRec` /
    // `appendMPPDispatchReq`), which is how TiFlash marks the task root.
    let receiver_meta = TaskMeta {
        start_ts: snapshot_ts,
        task_id: -1,
        gather_id,
        query_ts: query.query_ts,
        local_query_id: query.local_query_id,
        server_id: query.server_id,
        mpp_version: 3,
        resource_group_name: statement.resource_group_name.clone(),
        ..TaskMeta::default()
    };
    let meta = TaskMeta {
        start_ts: snapshot_ts,
        task_id: statement.mpp_query_info.alloc_task_id(),
        partition_id: -1,
        address: address.to_owned(),
        gather_id,
        query_ts: query.query_ts,
        local_query_id: query.local_query_id,
        server_id: query.server_id,
        mpp_version: 3,
        coordinator_address: "tidb-mpp-coordinator".to_owned(),
        report_execution_summary: false,
        resource_group_name: statement.resource_group_name.clone(),
        connection_id: 0,
        connection_alias: String::new(),
        sql_digest: String::new(),
        plan_digest: String::new(),
        ..TaskMeta::default()
    };
    (meta, receiver_meta)
}

/// Go `appendMPPDispatchReq`: the root fragment's DAG carries the exchange
/// sender over the scan, every column as the output offset, and TypeChunk
/// encoding for the coordinator's own fragment. The sender targets
/// `receiver_meta`, the coordinator's pseudo task.
pub fn append_mpp_dispatch_req(
    request: &PushdownScanRequest,
    receiver_meta: &TaskMeta,
) -> Result<DagRequest, String> {
    let (time_zone_name, time_zone_offset) = request.statement.time_zone.dag_zone();
    let mut context = DagRequestContext::new(
        time_zone_name,
        time_zone_offset,
        request.statement.push_down_flags,
        tidb_distsql::EncodeType::Chunk,
    );
    context.div_precision_increment = request.statement.div_precision_increment;
    let mut columns = request
        .columns
        .iter()
        .map(column_to_proto)
        .collect::<Option<Vec<tidb_planner_core_operator_logicalop::tikv_scan_spec::ScanColumnInfo>>>()
        .ok_or_else(|| "a column has no bounded coprocessor descriptor".to_owned())?;
    // Go `util.ColumnToProto` rewrites EVERY column's collation id for
    // the new-collation framework (binary included, so an INT reads
    // -63); the shared TiKV lowering keeps the un-rewritten binary
    // constant, which TiKV accepts. Match Go's wire bytes here.
    for column in &mut columns {
        column.collation = tidb_datatype::rewrite_new_collation_id_if_needed(column.collation);
    }
    // Go `PhysicalTableScan.ToPB` for TiFlash counts the scan; the scan
    // below never asks for fast scan.
    tidb_telemetry::CURRENT_TIFLASH_TABLE_SCAN_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let scan_executor = Executor {
        tp: Some(ExecType::TypeTableScan as i32),
        tbl_scan: Some(TableScan {
            table_id: Some(request.table_id),
            columns: columns.iter().map(column_to_pb).collect(),
            desc: Some(false),
            primary_column_ids: request.primary_column_ids.clone(),
            next_read_engine: Some(tidb_proto::tipb::EngineType::Local as i32),
            primary_prefix_column_ids: request.primary_prefix_column_ids.clone(),
            keep_order: Some(false),
            is_fast_scan: Some(false),
            max_wait_time_ms: Some(0),
            ..Default::default()
        }),
        idx_scan: None,
        selection: None,
        aggregation: None,
        top_n: None,
        limit: None,
        // Go carries each executor's ExplainID into the tree form; TiFlash
        // rejects duplicate empty ids ("executor id `` duplicate").
        executor_id: Some("TableFullScan_2".to_owned()),
        parent_idx: None,
        exchange_sender: None,
        ..Default::default()
    };
    let sender_executor = Executor {
        tp: Some(ExecType::TypeExchangeSender as i32),
        tbl_scan: None,
        idx_scan: None,
        selection: None,
        aggregation: None,
        top_n: None,
        limit: None,
        executor_id: Some("ExchangeSender_1".to_owned()),
        parent_idx: None,
        exchange_sender: Some(Box::new(PbExchangeSender {
            tp: Some(ExchangeType::PassThrough as i32),
            // Go fills the sender's target-task meta so TiFlash
            // registers the coordinator tunnel and marks the task root:
            // for a root fragment the target is the TiDB-side receiver
            // (task_id -1), exactly Go's EstablishMPPConns receiver meta.
            encoded_task_meta: vec![receiver_meta.encode_to_vec()],
            child: Some(Box::new(scan_executor)),
            all_field_types: columns
                .iter()
                .map(column_to_pb)
                .map(|info| tidb_proto::tipb::FieldType {
                    tp: info.tp,
                    flag: info.flag.map(|value| value as u32),
                    flen: info.column_len,
                    decimal: info.decimal,
                    collate: info.collation,
                    charset: Some(String::new()),
                    elems: Vec::new(),
                    array: Some(false),
                })
                .collect(),
            ..Default::default()
        })),
        ..Default::default()
    };
    let endian = if tidb_distsql::system_endian() == tidb_distsql::SystemEndian::Little {
        Endian::LittleEndian
    } else {
        Endian::BigEndian
    };
    Ok(DagRequest {
        executors: Vec::new(),
        time_zone_offset: Some(context.time_zone_offset),
        flags: Some(context.push_down_flags),
        output_offsets: (0..columns.len() as u32).collect(),
        encode_type: Some(EncodeType::TypeChunk as i32),
        time_zone_name: Some(context.time_zone_name.clone()),
        collect_execution_summaries: Some(false),
        chunk_memory_layout: Some(tidb_proto::tipb::ChunkMemoryLayout {
            endian: Some(endian as i32),
        }),
        div_precision_increment: (context.div_precision_increment
            != DEFAULT_DIV_PRECISION_INCREMENT)
            .then_some(context.div_precision_increment),
        root_executor: Some(sender_executor),
        ..Default::default()
    })
}

/// An MPP failure as the storage seam reports it.
fn scanner_error(error: MppError) -> PushdownScannerError {
    PushdownScannerError::Backend(match error {
        MppError::Sql(error) => StorageError::Sql(error),
        MppError::Other(message) => StorageError::Backend(message),
    })
}

/// Go `MPPGather.Open` + `Next`: dispatches `request`'s fragment through
/// `client` and returns the rows of its root tasks' streams. Every shape
/// [`check_mpp_request`] refuses is refused BY NAME, never served from row
/// storage.
pub fn open_mpp_scan(
    client: &Arc<dyn MppClient>,
    request: &PushdownScanRequest,
) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError> {
    check_mpp_request(request)
        .map_err(|reason| scanner_error(MppError::Other(format!("tiflash mpp: {reason}"))))?;
    let config = tidb_config::config_tree::config::get_global_config();
    let disagg_tiflash_with_auto_scaler = config.disaggregated_tiflash && config.use_auto_scaler;
    let response = open_gather(client, request).map_err(scanner_error)?;
    let retry_client = Arc::clone(client);
    let retry_request = request.clone();
    let response = ExecutorWithRetry::new(
        Box::new(response),
        disagg_tiflash_with_auto_scaler,
        request.statement.allow_tiflash_fallback,
        request.statement.memory.clone(),
        Box::new(move || {
            open_gather(&retry_client, &retry_request)
                .map(|gather| Box::new(gather) as Box<dyn MppGatherResponse>)
                .map_err(|error| QueryResponseError::Source(error.to_string()))
        }),
        Box::new(|node_count| {
            tidb_tiflashcompute::global_topo_fetcher()
                .ok_or_else(|| "TiFlash compute topology fetcher is not initialized".to_owned())?
                .recovery_and_get_topo(
                    tidb_tiflashcompute::RecoveryType::MEM_LIMIT,
                    node_count as i64,
                )
                .map(|_| ())
                .map_err(|error| error.to_string())
        }),
    );
    let field_types: Vec<FieldType> = request
        .columns
        .iter()
        .map(|column| column.field_type.clone())
        .collect();
    let iter = SelectResponseIter::from_query_response(
        Box::new(response),
        field_types.clone(),
        Vec::new(),
        request.statement.time_zone.clone(),
        request.statement.warnings.clone(),
        mpp_result_metadata(request.columns.len(), Vec::new(), request.statement.plan_id),
        None,
    );
    Ok(Box::new(MppRowStream {
        iter: Some(iter),
        pending: None,
        pending_row: 0,
        field_types,
        returned: 0,
        exhausted: false,
    }))
}

/// Go `localMppCoordinator.Execute`: the fragment's tasks from
/// `ConstructMPPTasks`, each dispatched with its own task meta, and the
/// coordinator's stream from every root task.
fn open_gather(
    client: &Arc<dyn MppClient>,
    request: &PushdownScanRequest,
) -> Result<MppTaskResponses, MppError> {
    let refuse = |reason: String| MppError::Other(format!("tiflash mpp: {reason}"));
    let ctx: Arc<dyn MppContext> = Arc::new(request.statement.memory.clone());
    let kv_context = request
        .statement
        .kv_context
        .as_ref()
        .map(tidb_txnkv::StatementKvContextCell::get)
        .unwrap_or_default();
    // Go `MPPClient.ConstructMPPTasks`: `ctx = context.WithValue(ctx,
    // tikv.TxnStartKey(), req.StartTS)` on the statement ctx, whose
    // `ExecDetails` the task building's region lookups charge.
    let build_kv_context = kv_context.clone().with_txn_start_ts(request.snapshot_ts);
    let policy =
        tidb_tiflashcompute::DispatchPolicy::parse(&request.statement.tiflash_compute_dispatch_policy)
            .map_err(refuse)?;
    let tasks = client.construct_mpp_tasks(
        &ctx,
        &MppBuildTasksRequest {
            key_ranges: Some(
                request
                    .ranges
                    .iter()
                    .map(|(start, end)| tidb_txnkv::KeyRange::new(start.clone(), end.clone()))
                    .collect(),
            ),
            start_ts: request.snapshot_ts,
            ..MppBuildTasksRequest::default()
        },
        &build_kv_context,
        policy,
        request.statement.tiflash_replica_read,
        &mut |warning| request.statement.warnings.append_warning(warning),
    )?;
    let first = tasks
        .first()
        .ok_or_else(|| refuse("no region covers the table's record range".to_owned()))?;
    let (mut meta, receiver_meta) =
        mpp_task_metadata(request.snapshot_ts, &request.statement, &first.address);
    let mut streams = MppTaskResponses {
        streams: VecDeque::new(),
        node_count: tasks.len(),
    };
    for (index, task) in tasks.into_iter().enumerate() {
        if index != 0 {
            meta.task_id = request.statement.mpp_query_info.alloc_task_id();
        }
        meta.address.clone_from(&task.address);
        let address = task.address.clone();
        let dag = append_mpp_dispatch_req(request, &receiver_meta).map_err(refuse)?;
        // Go `handleDispatchReq` and `receiveResults`: both calls repeat
        // while they ask for a retry, within one backoff budget.
        let mut bo = tidb_txnkv::RegionBackoffBudget::campaign_default();
        let encoded_plan = dag.encode_to_vec();
        let rpc_resp = loop {
            match client.dispatch_mpp_task(
                &ctx,
                &task,
                &meta,
                &encoded_plan,
                &kv_context,
                &request.statement.rpc_interceptor,
                &mut bo,
            ) {
                Err(tidb_txnkv::MppRpcError { retry: true, .. }) => {}
                Err(error) => break Err(error.err),
                Ok(rpc_resp) => break Ok(rpc_resp),
            }
        }?;
        if let Some(error) = rpc_resp.error {
            return Err(MppError::Other(error.msg));
        }
        let stream = loop {
            match client.establish_mpp_conns(
                Arc::clone(&ctx),
                &address,
                &meta,
                &receiver_meta,
                &kv_context,
                &request.statement.rpc_interceptor,
                &mut bo,
            ) {
                Err(tidb_txnkv::MppRpcError { retry: true, .. }) => {}
                Err(error) => break Err(error.err),
                Ok(stream) => break Ok(stream),
            }
        }?;
        streams.streams.push_back(stream);
    }
    Ok(streams)
}

fn mpp_memory_error(memory: &StatementMemory) -> Result<(), QueryResponseError> {
    memory.check().map_err(|error| {
        let sql = error.to_mysql_error();
        QueryResponseError::Sql {
            code: sql.code,
            message: sql.message,
        }
    })
}

/// One gather's raw task responses, below the shared `SelectResponse`
/// decoder: Go's coordinator over its root tasks' streams.
pub struct MppTaskResponses {
    /// The root tasks' streams, read in order.
    pub streams: VecDeque<Box<dyn MppStreamResponse>>,
    /// How many tasks the gather dispatched (Go `nodeCnt`).
    pub node_count: usize,
}

impl tidb_txnkv::Response for MppTaskResponses {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        while let Some(stream) = self.streams.front_mut() {
            match stream.next() {
                Ok(Some(packet)) => {
                    // The stream charged the packet while receiving it; the
                    // holder above charges what it buffers.
                    stream.release_packet_memory();
                    return Ok(Some(packet));
                }
                Ok(None) => {
                    stream.close()?;
                    self.streams.pop_front();
                }
                Err(error) => {
                    let _ = self.close();
                    return Err(error);
                }
            }
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), QueryResponseError> {
        let mut result = Ok(());
        for stream in &mut self.streams {
            let closed = stream.close();
            if result.is_ok() {
                result = closed;
            }
        }
        self.streams.clear();
        result
    }
}

impl Drop for MppTaskResponses {
    fn drop(&mut self) {
        let _ = tidb_txnkv::Response::close(self);
    }
}

/// Go `kv.MppCoordinator` as `ExecutorWithRetry` drives it: the gather's
/// response and `GetNodeCnt`.
pub trait MppGatherResponse: tidb_txnkv::Response + Send {
    /// Go `GetNodeCnt`: the nodes the gather's tasks run on.
    fn node_count(&self) -> usize;

    /// Go `localMppCoordinator.closeWithoutReport`, which a recovery closes
    /// the failed coordinator with: only report runtime stats when there is
    /// no error.
    fn close_without_report(&mut self) {
        let _ = self.close();
    }
}

impl MppGatherResponse for MppTaskResponses {
    fn node_count(&self) -> usize {
        self.node_count
    }
}

/// Reopens a gather for a recovery attempt.
pub type MppReopen = Box<dyn FnMut() -> Result<Box<dyn MppGatherResponse>, QueryResponseError> + Send>;
pub use crate::recovery_handler::MppRecover;

/// Go's failpoints in `executor_with_retry.go`, as seams.
#[cfg(any(test, feature = "test-util"))]
pub mod mock {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Mutex, PoisonError};

    static MPP_RECOVERY_TEST_MOCK_ENABLE: AtomicBool = AtomicBool::new(false);

    /// Go `failpoint.Enable("mpp_recovery_test_mock_enable", "return()")`:
    /// recovery is enabled wherever the TiFlash fallback is not allowed.
    pub fn set_mpp_recovery_test_mock_enable(enabled: bool) {
        MPP_RECOVERY_TEST_MOCK_ENABLE.store(enabled, Ordering::Release);
    }

    pub(super) fn mpp_recovery_test_mock_enable() -> bool {
        MPP_RECOVERY_TEST_MOCK_ENABLE.load(Ordering::Acquire)
    }

    static MPP_RECOVERY_TEST_IGNORE_RECOVERY_ERR: AtomicBool = AtomicBool::new(false);

    /// Go `failpoint.Enable("mpp_recovery_test_ignore_recovery_err",
    /// "return()")`: a mocked mpp error's missing handler is a recovery.
    pub fn set_mpp_recovery_test_ignore_recovery_err(enabled: bool) {
        MPP_RECOVERY_TEST_IGNORE_RECOVERY_ERR.store(enabled, Ordering::Release);
    }

    pub(super) fn mpp_recovery_test_ignore_recovery_err() -> bool {
        MPP_RECOVERY_TEST_IGNORE_RECOVERY_ERR.load(Ordering::Acquire)
    }

    static MPP_RECOVERY_TEST_MAX_ERR_TIMES: Mutex<Option<u32>> = Mutex::new(None);

    /// Go `failpoint.Enable("mpp_recovery_test_max_err_times", "return(n)")`:
    /// every held read fails until `n` recoveries ran.
    pub fn set_mpp_recovery_test_max_err_times(times: Option<u32>) {
        *MPP_RECOVERY_TEST_MAX_ERR_TIMES
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = times;
    }

    pub(super) fn mpp_recovery_test_max_err_times() -> Option<u32> {
        *MPP_RECOVERY_TEST_MAX_ERR_TIMES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    static MPP_RECOVERY_TEST_HOLD_SIZE: Mutex<Option<usize>> = Mutex::new(None);

    /// Go `failpoint.Enable("mpp_recovery_test_hold_size", "1*return(n)")`:
    /// the next hold must end with `n` responses. It executes once.
    pub fn set_mpp_recovery_test_hold_size(size: Option<usize>) {
        *MPP_RECOVERY_TEST_HOLD_SIZE
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = size;
    }

    pub(super) fn mpp_recovery_test_hold_size() -> Option<usize> {
        MPP_RECOVERY_TEST_HOLD_SIZE
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

/// Go `ExecutorWithRetry`: recovers an mpp error if possible and retries the
/// MPP task. It attempts to hold the results of MPP; while it holds, an
/// error starts a recovery, which discards the held results and re-executes
/// the task. Once results start being returned, error recovery cannot be
/// performed anymore.
pub struct ExecutorWithRetry {
    coord: Box<dyn MppGatherResponse>,
    memory: StatementMemory,
    mpp_err_recovery: RecoveryHandler,
    /// Go `setupMPPCoordinator(ctx, true)`'s new coordinator.
    open: Option<MppReopen>,
    node_cnt: usize,
    closed: bool,
}

impl ExecutorWithRetry {
    /// Go `NewExecutorWithRetry` over its first coordinator.
    /// `disagg_tiflash_with_auto_scaler` is the config's
    /// `DisaggregatedTiFlash && UseAutoScaler`, and `allow_tiflash_fallback`
    /// the session's `AllowFallbackToTiKV[kv.TiFlash]`.
    #[must_use]
    pub fn new(
        coord: Box<dyn MppGatherResponse>,
        disagg_tiflash_with_auto_scaler: bool,
        allow_tiflash_fallback: bool,
        memory: StatementMemory,
        open: MppReopen,
        recover: MppRecover,
    ) -> Self {
        // TODO: After add row info in tipb.DataPacket, we can use row count as capacity.
        // For now, use the number of tipb.DataPacket as capacity.
        const HOLD_CAP: usize = 2;

        // 1. For now, mpp err recovery only support MemLimit, which is only useful when AutoScaler is used.
        // 2. When enable fallback to tikv, the returned mpp err will be ErrTiFlashServerTimeout,
        //    which we cannot handle for now. Also there is no need to recovery because tikv will retry the query.
        // 3. For cached table, will not dispatch tasks to TiFlash, so no need to recovery.
        #[allow(unused_mut)]
        let mut enable_mpp_recovery = disagg_tiflash_with_auto_scaler && !allow_tiflash_fallback;

        #[cfg(any(test, feature = "test-util"))]
        if mock::mpp_recovery_test_mock_enable() && !allow_tiflash_fallback {
            enable_mpp_recovery = true;
        }

        let node_cnt = coord.node_count();
        Self {
            coord,
            mpp_err_recovery: RecoveryHandler::new(
                disagg_tiflash_with_auto_scaler,
                HOLD_CAP,
                enable_mpp_recovery,
                memory.clone(),
                recover,
            ),
            memory,
            open: Some(open),
            node_cnt,
            closed: false,
        }
    }

    /// Go `setupMPPCoordinator(ctx, true)`: the failed coordinator is closed
    /// without its report, then a new one is built and executed.
    fn setup_mpp_coordinator(&mut self) -> Result<(), QueryResponseError> {
        // Only report runtime stats when there is no error.
        self.coord.close_without_report();
        self.coord = (self.open.as_mut().expect("open recovery response"))()?;
        self.node_cnt = self.coord.node_count();
        Ok(())
    }

    /// Go `nextWithRecovery`.
    fn next_with_recovery(&mut self) -> Result<(), QueryResponseError> {
        if !self.mpp_err_recovery.enabled() {
            return Ok(());
        }

        while self.mpp_err_recovery.can_hold_result() {
            #[allow(unused_mut)]
            let mut resp = self.coord.next();

            // Mock recovery n times.
            #[cfg(any(test, feature = "test-util"))]
            if mock::mpp_recovery_test_max_err_times()
                .is_some_and(|force_err_cnt| self.mpp_err_recovery.recovery_cnt() < force_err_cnt)
            {
                resp = Err(QueryResponseError::Source("mock mpp error".to_owned()));
            }

            match resp {
                Err(mpp_err) => {
                    // The statement's own quota or KILL is terminal.
                    mpp_memory_error(&self.memory)?;
                    #[allow(unused_mut)]
                    let mut recovery_err = self
                        .mpp_err_recovery
                        .recovery(&RecoveryInfo {
                            mpp_err: &mpp_err,
                            node_cnt: self.node_cnt,
                        })
                        .err();

                    // Mock recovery succeed, ignore no recovery handler err.
                    #[cfg(any(test, feature = "test-util"))]
                    if mock::mpp_recovery_test_ignore_recovery_err() {
                        let recovery = recovery_err
                            .as_deref()
                            .expect("mocked mpp err should got recovery err");
                        if mpp_err.to_string().contains("mock mpp error")
                            && recovery.contains("no handler to recovery")
                        {
                            recovery_err = None;
                        }
                    }

                    if recovery_err.is_some() {
                        return Err(mpp_err);
                    }

                    if self.setup_mpp_coordinator().is_err() {
                        return Err(mpp_err);
                    }
                    self.mpp_err_recovery.reset_holder();
                }
                Ok(None) => break,
                Ok(Some(resp)) => {
                    self.mpp_err_recovery.hold_result(resp);
                    mpp_memory_error(&self.memory)?;
                }
            }
        }

        #[cfg(any(test, feature = "test-util"))]
        if let Some(num) = mock::mpp_recovery_test_hold_size() {
            // Note: this failpoint only execute once.
            let cur_rows = self.mpp_err_recovery.num_hold_resp();
            assert!(cur_rows == num, "unexpected holding rows, cur: {cur_rows}");
        }
        Ok(())
    }

    /// Go `ExecutorWithRetry.Next`.
    fn pull(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        mpp_memory_error(&self.memory)?;
        self.next_with_recovery()?;

        if self.mpp_err_recovery.num_hold_resp() != 0 {
            return self
                .mpp_err_recovery
                .pop_front_resp()
                .map(Some)
                .map_err(QueryResponseError::Source);
        }
        self.coord.next()
    }
}

impl tidb_txnkv::Response for ExecutorWithRetry {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        if self.closed {
            return Ok(None);
        }
        let result = self.pull();
        match &result {
            Ok(Some(_)) => {}
            // The drained gather is closed here, so Go `Close`'s error is
            // this pull's.
            Ok(None) => tidb_txnkv::Response::close(self)?,
            Err(_) => {
                let _ = tidb_txnkv::Response::close(self);
            }
        }
        result
    }

    fn close(&mut self) -> Result<(), QueryResponseError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.mpp_err_recovery.reset_holder();
        // Go `ExecutorWithRetry.Close`: `err := r.coord.Close()`.
        let result = self.coord.close();
        self.open = None;
        result
    }
}

impl Drop for ExecutorWithRetry {
    fn drop(&mut self) {
        let _ = tidb_txnkv::Response::close(self);
    }
}

/// The caller's end of one MPP-served scan, mirroring the coprocessor
/// reader's decoded-chunk queue.
struct MppRowStream {
    iter: Option<SelectResponseIter>,
    pending: Option<Chunk>,
    pending_row: usize,
    field_types: Vec<FieldType>,
    returned: u64,
    exhausted: bool,
}

impl MppRowStream {
    fn pull_chunk(&mut self, required_rows: usize) -> Result<Option<Chunk>, StorageError> {
        if self.exhausted {
            return Ok(None);
        }
        let Some(iter) = self.iter.as_mut() else {
            return Ok(None);
        };
        loop {
            let batch = iter
                .next_chunk_with_required_rows(required_rows.max(1))
                .map_err(|error| match error {
                    ResponseChannelError::SelectResponse { code, message } => StorageError::Sql(
                        tidb_dbterror::ErrClass::synthesize_tikv(code as u16, message),
                    ),
                    ResponseChannelError::Sql { code, message } => StorageError::Sql(
                        tidb_dbterror::ErrClass::response_iterator_error(code, message),
                    ),
                    other => StorageError::Backend(format!("tiflash mpp stream: {other}")),
                })?;
            let Some(batch) = batch else {
                self.exhausted = true;
                return Ok(None);
            };
            if batch.row.num_rows() == 0 {
                continue;
            }
            return Ok(Some(batch.row));
        }
    }
}

impl PushdownRowStream for MppRowStream {
    fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
        loop {
            if let Some(batch) = &self.pending {
                if self.pending_row < batch.num_rows() {
                    let row = batch
                        .get_row(self.pending_row)
                        .try_get_datum_row(&self.field_types)
                        .map_err(|error| StorageError::Backend(error.to_string()))?;
                    self.pending_row += 1;
                    if self.pending_row == batch.num_rows() {
                        self.pending = None;
                        self.pending_row = 0;
                    }
                    self.returned += 1;
                    return Ok(Some(row));
                }
                self.pending = None;
                self.pending_row = 0;
            }
            match self.pull_chunk(1)? {
                Some(batch) => self.pending = Some(batch),
                None => return Ok(None),
            }
        }
    }

    fn close(&mut self) {
        if let Some(iter) = self.iter.as_mut() {
            let _ = iter.close();
        }
        self.iter = None;
        self.pending = None;
        self.exhausted = true;
    }

    fn rows_returned(&self) -> u64 {
        self.returned
    }
}

#[cfg(test)]
mod task_meta_tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn mpp_queries_share_statement_identity_and_allocate_gathers_and_tasks() {
        use crate::StmtContext;

        let context = StmtContext::for_query();
        let first = crate::remote_scan::pushdown_statement_context(&context);
        let second = crate::remote_scan::pushdown_statement_context(&context.clone());
        let (a, receiver_a) = mpp_task_metadata(77, &first, "tiflash:3930");
        let (b, receiver_b) = mpp_task_metadata(77, &second, "tiflash-other:3930");
        assert_ne!(a.address, b.address);
        assert_eq!(a.server_id, b.server_id);
        assert_eq!(a.local_query_id, b.local_query_id);
        assert_eq!(a.query_ts, b.query_ts);
        assert_eq!((a.gather_id, b.gather_id), (1, 2));
        assert_eq!((a.task_id, b.task_id), (1, 2));
        for (sender, receiver) in [(a, receiver_a), (b, receiver_b)] {
            assert_eq!(receiver.task_id, -1);
            assert_eq!(sender.start_ts, 77);
            assert_eq!(sender.gather_id, receiver.gather_id);
            assert_eq!(sender.query_ts, receiver.query_ts);
            assert_eq!(sender.local_query_id, receiver.local_query_id);
        }
    }

    #[test]
    fn mpp_query_identity_is_unique_even_at_the_same_snapshot() {
        use crate::StmtContext;

        let a = crate::remote_scan::pushdown_statement_context(&StmtContext::for_query());
        let b = crate::remote_scan::pushdown_statement_context(&StmtContext::for_query());
        let (a, _) = mpp_task_metadata(77, &a, "tiflash:3930");
        let (b, _) = mpp_task_metadata(77, &b, "tiflash:3930");
        assert_ne!(a.local_query_id, b.local_query_id);
        assert_eq!((a.task_id, b.task_id), (1, 1));
    }

    #[test]
    fn mpp_query_timestamp_uses_unix_nanoseconds() {
        use crate::StmtContext;

        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let context = crate::remote_scan::pushdown_statement_context(&StmtContext::for_query());
        let (meta, _) = mpp_task_metadata(77, &context, "tiflash:3930");
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        assert!(
            (before..=after).contains(&meta.query_ts),
            "{} not in {before}..={after}",
            meta.query_ts
        );
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use tidb_txnkv::Response as _;

    fn packet(value: u8) -> QueryResultSubset {
        QueryResultSubset {
            data: vec![value].into(),
            runtime: None,
        }
    }
    fn gather(
        rows: Vec<Result<QueryResultSubset, QueryResponseError>>,
        closed: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Box<dyn MppGatherResponse> {
        Box::new(MppTaskResponses {
            streams: [Box::new(Packets {
                rows: rows.into(),
                closed,
            }) as Box<dyn MppStreamResponse>]
            .into(),
            node_count: 2,
        })
    }
    fn memory_error() -> QueryResponseError {
        QueryResponseError::Source("Memory limit exceeded".into())
    }

    #[test]
    fn mpp_failure_batch_discards_held_packets_and_closes_before_reopening() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let closed = Arc::new(AtomicUsize::new(0));
        let old_closed = closed.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let recovery_calls = calls.clone();
        let memory = crate::StatementMemory::default();
        let mut response = ExecutorWithRetry::new(
            gather(vec![Ok(packet(99)), Err(memory_error())], closed),
            true,
            false,
            memory.clone(),
            Box::new(move || {
                assert_eq!(old_closed.load(SeqCst), 1);
                Ok(gather(
                    vec![Ok(packet(1)), Ok(packet(2))],
                    old_closed.clone(),
                ))
            }),
            Box::new(move |nodes| {
                assert_eq!(nodes, 2);
                recovery_calls.fetch_add(1, SeqCst);
                Ok(())
            }),
        );
        assert_eq!(response.next().unwrap(), Some(packet(1)));
        assert_eq!(memory.bytes_consumed(), 1);
        assert_eq!(response.next().unwrap(), Some(packet(2)));
        assert!(response.next().unwrap().is_none());
        assert_eq!(calls.load(SeqCst), 1);
        assert_eq!(memory.bytes_consumed(), 0);
    }

    #[test]
    fn mpp_failure_batch_never_replays_delivered_packets_or_disabled_recovery() {
        for enabled in [true, false] {
            let mut response = ExecutorWithRetry::new(
                gather(
                    vec![Ok(packet(1)), Ok(packet(2)), Err(memory_error())],
                    Arc::default(),
                ),
                enabled,
                false,
                crate::StatementMemory::default(),
                Box::new(|| panic!("must not replay delivered packets")),
                Box::new(|_| panic!("must not recover after delivery")),
            );
            assert_eq!(response.next().unwrap(), Some(packet(1)));
            assert_eq!(response.next().unwrap(), Some(packet(2)));
            assert_eq!(response.next().unwrap_err(), memory_error());
        }
    }

    #[test]
    fn mpp_failure_batch_three_attempts_and_original_error() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let calls = Arc::new(AtomicUsize::new(0));
        let recoveries = calls.clone();
        let mut response = ExecutorWithRetry::new(
            gather(vec![Err(memory_error())], Arc::default()),
            true,
            false,
            crate::StatementMemory::default(),
            Box::new(|| Ok(gather(vec![Err(memory_error())], Arc::default()))),
            Box::new(move |_| {
                recoveries.fetch_add(1, SeqCst);
                Ok(())
            }),
        );
        assert_eq!(response.next().unwrap_err(), memory_error());
        assert_eq!(calls.load(SeqCst), 3);
        for recovery_fails in [true, false] {
            let memory = crate::StatementMemory::default();
            let mut response = ExecutorWithRetry::new(
                gather(vec![Ok(packet(5)), Err(memory_error())], Arc::default()),
                true,
                false,
                memory.clone(),
                Box::new(|| Err(QueryResponseError::Source("replacement failed".into()))),
                Box::new(move |_| {
                    if recovery_fails {
                        Err("autoscaler failed".into())
                    } else {
                        Ok(())
                    }
                }),
            );
            assert_eq!(response.next().unwrap_err(), memory_error());
            assert_eq!(memory.bytes_consumed(), 0);
        }
    }

    #[test]
    fn mpp_failure_batch_nonrecoverable_errors_and_short_results() {
        for error in [
            QueryResponseError::Cancelled,
            QueryResponseError::Sql {
                code: 1317,
                message: "Memory limit local kill".into(),
            },
            QueryResponseError::Source("memory limit lowercase is unrelated".into()),
        ] {
            let mut response = ExecutorWithRetry::new(
                gather(vec![Err(error.clone())], Arc::default()),
                true,
                false,
                crate::StatementMemory::default(),
                Box::new(|| panic!("terminal")),
                Box::new(|_| panic!("terminal")),
            );
            assert_eq!(response.next().unwrap_err(), error);
        }
        for count in 0..=2 {
            let memory = crate::StatementMemory::default();
            let mut response = ExecutorWithRetry::new(
                gather((0..count).map(|i| Ok(packet(i))).collect(), Arc::default()),
                true,
                false,
                memory.clone(),
                Box::new(|| panic!("no failure")),
                Box::new(|_| panic!("no failure")),
            );
            for i in 0..count {
                assert_eq!(response.next().unwrap(), Some(packet(i)));
            }
            assert!(response.next().unwrap().is_none());
            assert_eq!(memory.bytes_consumed(), 0);
        }
    }

    #[test]
    fn mpp_failure_batch_close_releases_retry_capabilities() {
        let capability = Arc::new(());
        let observer = Arc::downgrade(&capability);
        let mut response = ExecutorWithRetry::new(
            gather(vec![], Arc::default()),
            true,
            false,
            crate::StatementMemory::default(),
            Box::new(move || {
                let _borrow = &capability;
                panic!("no retry")
            }),
            Box::new(|_| panic!("no failure")),
        );
        response.close().unwrap();
        assert!(observer.upgrade().is_none());
        response.close().unwrap();
    }

    struct Packets {
        rows: std::collections::VecDeque<Result<QueryResultSubset, QueryResponseError>>,
        closed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl MppStreamResponse for Packets {
        fn release_packet_memory(&mut self) -> i64 {
            0
        }

        fn packet_error(&self) -> Option<&str> {
            None
        }
    }
    impl tidb_txnkv::Response for Packets {
        fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
            self.rows.pop_front().transpose()
        }

        fn close(&mut self) -> Result<(), QueryResponseError> {
            self.closed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// A gather whose close answers `error`, as Go's coordinator answers
    /// `handleAllReports`'s.
    struct FailingClose {
        error: QueryResponseError,
        closed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl MppStreamResponse for FailingClose {
        fn release_packet_memory(&mut self) -> i64 {
            0
        }

        fn packet_error(&self) -> Option<&str> {
            None
        }
    }
    impl MppGatherResponse for FailingClose {
        fn node_count(&self) -> usize {
            1
        }
    }
    impl tidb_txnkv::Response for FailingClose {
        fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
            Ok(None)
        }

        fn close(&mut self) -> Result<(), QueryResponseError> {
            self.closed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(self.error.clone())
        }
    }

    /// Go `ExecutorWithRetry.Close` returns `r.coord.Close()`'s error; here
    /// a drained gather is closed by the pull that drained it.
    #[test]
    fn a_gathers_close_error_is_the_responses() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let error = QueryResponseError::Source("report".into());
        let closed = Arc::new(AtomicUsize::new(0));
        let response = || {
            ExecutorWithRetry::new(
                Box::new(FailingClose {
                    error: error.clone(),
                    closed: closed.clone(),
                }),
                true,
                false,
                crate::StatementMemory::default(),
                Box::new(|| panic!("no retry")),
                Box::new(|_| panic!("no failure")),
            )
        };
        let mut abandoned = response();
        assert_eq!(abandoned.close(), Err(error.clone()));
        assert_eq!(abandoned.close(), Ok(()));
        assert_eq!(closed.load(SeqCst), 1);
        let mut drained = response();
        assert_eq!(drained.next(), Err(error.clone()));
        assert_eq!(drained.close(), Ok(()));
        assert_eq!(closed.load(SeqCst), 2);

        // Every task's stream is closed, and the first error is the close's.
        let failing = |message: &str| {
            Box::new(FailingClose {
                error: QueryResponseError::Source(message.into()),
                closed: closed.clone(),
            }) as Box<dyn MppStreamResponse>
        };
        let mut streams = MppTaskResponses {
            streams: [
                Box::new(Packets {
                    rows: [].into(),
                    closed: closed.clone(),
                }) as Box<dyn MppStreamResponse>,
                failing("first"),
                failing("second"),
            ]
            .into(),
            node_count: 3,
        };
        assert_eq!(streams.close(), Err(QueryResponseError::Source("first".into())));
        assert_eq!(closed.load(SeqCst), 5);
        let mut streams = MppTaskResponses {
            streams: [failing("drained")].into(),
            node_count: 1,
        };
        assert_eq!(streams.next(), Err(QueryResponseError::Source("drained".into())));
    }

    #[test]
    fn compute_topology_batch_fanout_error_closes_all_tasks() {
        let closed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut streams = MppTaskResponses {
            streams: [
                Box::new(Packets {
                    rows: [Ok(packet(1))].into(),
                    closed: closed.clone(),
                }) as Box<dyn MppStreamResponse>,
                Box::new(Packets {
                    rows: [Err(QueryResponseError::Source("failed".into()))].into(),
                    closed: closed.clone(),
                }),
                Box::new(Packets {
                    rows: [Ok(packet(3))].into(),
                    closed: closed.clone(),
                }),
            ]
            .into(),
            node_count: 3,
        };
        assert_eq!(streams.next().unwrap(), Some(packet(1)));
        assert!(streams.next().is_err());
        assert_eq!(closed.load(std::sync::atomic::Ordering::SeqCst), 3);
        streams.close().unwrap();
        assert_eq!(closed.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}
