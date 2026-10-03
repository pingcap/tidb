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

//! The TiFlash MPP pushdown lowering: Go `pkg/store/copr/mpp.go` +
//! `pkg/executor/internal/mpp/local_mpp_coordinator.go`, narrowed to the
//! single-fragment table scan the planner builds for a forced TiFlash read
//! (TableReader -> ExchangeSender(PassThrough) -> TableScan, mpp version 3).
//!
//! The flow per open scan is Go's dispatch choreography: pick a live TiFlash
//! store (`GetAllStores` + the `engine=tiflash` label, `addTiFlashStoreInfo`),
//! collect the table's record-range regions from PD
//! (`constructMPPTasksImpl`'s region split), marshal the tree-form DAG with
//! the scan under a PassThrough exchange sender (`appendMPPDispatchReq` ->
//! `ConstructDAGReq`, `dagReq.EncodeType = TypeChunk` for the root fragment),
//! `DispatchMPPTask` it to the store, then `EstablishMPPConnection` and wrap
//! the packet stream (`MPPDataPacket.data` = tipb `SelectResponse`) in the
//! same `SelectResponseIter` the TiKV cop path consumes.

use std::sync::Mutex;

use prost::Message as _;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_distsql::query_runtime::query_response::QueryResultSubset;
use tidb_distsql::{
    mpp_result_metadata, QueryResponseError, ResponseChannelError, SelectResponseIter,
};
use tidb_executor::remote_scan::{
    PushdownReadEngine, PushdownRowStream, PushdownScanRequest, PushdownScannerError,
};
use tidb_executor::storage::StorageError;
use tidb_pd_client::{PdClient, PdStoreState};
use tidb_proto::mpp::{
    DispatchTaskRequest, EstablishMppConnectionRequest, MppDataPacket, TaskMeta,
};
use tidb_proto::tikvpb::tikv_client::TikvClient;
use tidb_proto::tipb::{
    DagRequest, EncodeType, Endian, ExchangeSender as PbExchangeSender, ExchangeType, ExecType,
    Executor, TableScan,
};
use tidb_txnkv::region::{
    BackgroundRegionCache, BatchScanBackoff, BatchScanRetryReason, RegionBackoffBudget,
    RegionBackoffKind, RegionCache, RegionLoadError, RegionLoader, RegionLocation,
    RegionRouteError, RegionVerId,
};
use tidb_txnkv::PdRegionLoader;

use crate::cop_scan::scan_column;
use crate::dag_request::{column_to_pb, DagRequestContext, DEFAULT_DIV_PRECISION_INCREMENT};

/// The process-owned MPP dispatch capability.
///
/// The PD client is shared with the node's other workers through `Clone`; the
/// tokio runtime is this module's own because MPP dispatch and the result
/// stream are direct store RPCs outside the BatchCommands transport the
/// session transport factory owns (Go opens the MPPConn stream on the TiKV
/// client directly, `mpp.go:235` EstablishMPPConns).
pub struct TiFlashMppScanSource {
    pd: Mutex<PdClient>,
    regions: BackgroundRegionCache<PdRegionLoader>,
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
    /// Go `is.SchemaMetaVersion()`, read at dispatch time from the node's
    /// catalog watch: TiFlash resolves the request's table in the schema
    /// generation the coordinator names, so a stale or zero version makes
    /// even a synced table "not exist".
    schema_version: Box<dyn Fn() -> i64 + Send + Sync>,
}

impl std::fmt::Debug for TiFlashMppScanSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TiFlashMppScanSource")
    }
}

impl TiFlashMppScanSource {
    /// Builds the source over the node's PD membership and catalog watch.
    pub fn new(
        pd: PdClient,
        regions: BackgroundRegionCache<PdRegionLoader>,
        schema_version: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            regions,
            pd: Mutex::new(pd),
            runtime: std::sync::Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("the TiFlash MPP runtime starts"),
            ),
            schema_version: Box::new(schema_version),
        }
    }

    /// The address of one live TiFlash store, or why none is usable.
    fn tiflash_store_address(&self) -> Result<String, String> {
        let client = self
            .pd
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stores = client.all_stores().map_err(|error| error.to_string())?;
        stores
            .iter()
            .find(|store| {
                store.state == PdStoreState::Up
                    && store.labels.iter().any(|(key, value)| {
                        key.eq_ignore_ascii_case("engine") && value.eq_ignore_ascii_case("tiflash")
                    })
            })
            .map(|store| store.address.clone())
            .ok_or_else(|| {
                "no live TiFlash store answers the dispatch: check the replica placement".to_owned()
            })
    }

    /// Opens one MPP-served scan.
    pub(crate) fn open_mpp(
        &self,
        request: &PushdownScanRequest,
    ) -> Result<Box<dyn PushdownRowStream>, PushdownScannerError> {
        let refuse = |reason: String| {
            PushdownScannerError::Backend(StorageError::Backend(format!("tiflash mpp: {reason}")))
        };
        if request.read_engine != PushdownReadEngine::TiFlash {
            return Err(refuse(
                "the request did not name the columnar engine".to_owned(),
            ));
        }
        // This lowering is the single-fragment full scan. Every carried
        // operator is refused BY NAME rather than served from row storage:
        // the caller asked for the columnar engine, and a silent TiKV
        // fallback would be the wrong answer, not a slower one.
        if !request.predicates.is_empty() {
            return Err(refuse("pushed Selection is not lowered yet".to_owned()));
        }
        if request.topn.is_some() {
            return Err(refuse("pushed TopN is not lowered yet".to_owned()));
        }
        if request.aggregate.is_some() {
            return Err(refuse("partial aggregation is not lowered yet".to_owned()));
        }
        if request.desc || request.keep_order {
            return Err(refuse("ordered scans are not lowered yet".to_owned()));
        }
        if request.index.is_some() {
            return Err(refuse("index scans have no TiFlash path".to_owned()));
        }
        let address = self.tiflash_store_address().map_err(refuse)?;
        let region_lease = self
            .regions
            .open_lease()
            .map_err(|error| refuse(error.to_string()))?;
        let regions =
            locate_mpp_region_infos(&region_lease, &request.ranges, &request.statement.memory)
                .map_err(refuse)?;
        if regions.is_empty() {
            return Err(refuse(
                "no region covers the table's record range".to_owned(),
            ));
        }

        // Go `appendMPPDispatchReq`: the root fragment's DAG carries the
        // exchange sender over the scan, every column as the output offset,
        // and TypeChunk encoding for the coordinator's own fragment.
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
            .map(scan_column)
            .collect::<Option<Vec<tidb_planner::tikv_scan_spec::ScanColumnInfo>>>()
            .ok_or_else(|| refuse("a column has no bounded coprocessor descriptor".to_owned()))?;
        // Go `util.ColumnToProto` rewrites EVERY column's collation id for
        // the new-collation framework (binary included, so an INT reads
        // -63); the shared TiKV lowering keeps the un-rewritten binary
        // constant, which TiKV accepts. Match Go's wire bytes here.
        for column in &mut columns {
            column.collation = tidb_datatype::rewrite_new_collation_id_if_needed(column.collation);
        }
        let (meta, receiver_meta) =
            mpp_task_metadata(request.snapshot_ts, &request.statement, &address);

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
        let dag = DagRequest {
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
        };
        let encoded_plan = dag.encode_to_vec();

        // Region infos are clipped to the table's record range: PD answers
        // with whole regions whose bounds may straddle the prefix.
        let region_infos = regions;
        let dispatch_request = encode_dispatch_request(DispatchTaskRequest {
            meta: Some(meta.clone()),
            encoded_plan,
            timeout: 10,
            regions: region_infos,
            schema_ver: (self.schema_version)(),
            ..Default::default()
        });

        // Dispatch opens the live stream; the shared response consumer pulls
        // packets on demand. Keep the runtime alive through the response owner.
        let mut response = self
            .runtime
            .block_on(async {
                let endpoint = format!("http://{address}");
                let mut client = TikvClient::connect(endpoint)
                    .await
                    .map_err(|error| format!("tiflash mpp: dial {address}: {error}"))?;
                let response = client
                    .dispatch_mpp_task(dispatch_request)
                    .await
                    .map_err(|error| format!("tiflash mpp: dispatch: {error}"))?;
                let dispatch_response = response.into_inner();
                if let Some(error) = &dispatch_response.error {
                    return Err(format!(
                        "tiflash mpp: dispatch refused: {} ({})",
                        error.msg, error.code
                    ));
                }
                // Go `MPPClient.DispatchMPPTask`: retry regions
                // only invalidate the coordinator's region cache; they are NOT a
                // dispatch failure. The task itself has already registered and
                // will serve its regions through the learner read.
                region_lease
                    .with_cache(|cache| {
                        invalidate_mpp_retry_regions(cache, &dispatch_response.retry_regions)
                    })
                    .map_err(|error| error.to_string())?;
                establish_mpp_response(
                    &mut client,
                    meta.clone(),
                    receiver_meta.clone(),
                    std::sync::Arc::clone(&self.runtime),
                    request.statement.memory.clone(),
                )
                .await
            })
            .map_err(refuse)?;
        response.region_lease = Some(region_lease);

        let field_types: Vec<FieldType> = request
            .columns
            .iter()
            .map(|column| column.field_type.clone())
            .collect();
        let time_zone = request.statement.time_zone.clone();
        let warnings = request.statement.warnings.clone();
        let iter = SelectResponseIter::from_query_response(
            Box::new(response),
            field_types,
            Vec::new(),
            time_zone,
            warnings,
            mpp_result_metadata(request.columns.len(), Vec::new(), request.statement.plan_id),
            None,
        );
        Ok(Box::new(MppRowStream {
            iter: Some(iter),
            pending: None,
            pending_row: 0,
            field_types: request
                .columns
                .iter()
                .map(|column| column.field_type.clone())
                .collect(),
            returned: 0,
            exhausted: false,
        }))
    }
}

fn mpp_region_infos(
    ranges: &[(tidb_txnkv::Key, tidb_txnkv::Key)],
    regions: &[RegionLocation],
) -> Vec<tidb_proto::coprocessor::RegionInfo> {
    regions
        .iter()
        .filter_map(|region| {
            let ranges: Vec<_> = ranges
                .iter()
                .filter_map(|(start, end)| {
                    let start = start.as_slice().max(region.start_key.as_slice()).to_vec();
                    let end = match (end.is_empty(), region.end_key.is_empty()) {
                        (true, _) => region.end_key.clone(),
                        (_, true) => end.as_slice().to_vec(),
                        _ => end.as_slice().min(region.end_key.as_slice()).to_vec(),
                    };
                    (end.is_empty() || start < end)
                        .then_some(tidb_proto::coprocessor::KeyRange { start, end })
                })
                .collect();
            (!ranges.is_empty()).then_some(tidb_proto::coprocessor::RegionInfo {
                region_id: region.region.id,
                region_epoch: Some(tidb_proto::coprocessor::RegionEpoch {
                    conf_ver: region.region.epoch.conf_ver,
                    version: region.region.epoch.version,
                }),
                ranges,
            })
        })
        .collect()
}

fn locate_mpp_region_infos<L: RegionLoader>(
    cache: &BackgroundRegionCache<L>,
    ranges: &[(tidb_txnkv::Key, tidb_txnkv::Key)],
    memory: &tidb_executor::StatementMemory,
) -> Result<Vec<tidb_proto::coprocessor::RegionInfo>, String> {
    if ranges.is_empty() {
        return Err("the scan carries no key ranges".to_owned());
    }
    mpp_memory_error(memory).map_err(|error| error.to_string())?;
    let keys = ranges
        .iter()
        .map(|(start, end)| tidb_txnkv::region::KeyRange::new(start.as_slice(), end.as_slice()))
        .collect::<Vec<_>>();
    let mut backoff = MppRegionBackoff {
        budget: RegionBackoffBudget::new(std::time::Duration::from_secs(5)),
        memory,
    };
    let regions = cache
        .batch_locate_ranges_with_backoff(&keys, &mut backoff)
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
    Ok(mpp_region_infos(ranges, &regions))
}

struct MppRegionBackoff<'a> {
    budget: RegionBackoffBudget,
    memory: &'a tidb_executor::StatementMemory,
}
impl BatchScanBackoff for MppRegionBackoff<'_> {
    fn backoff(&mut self, reason: BatchScanRetryReason) -> Result<(), RegionRouteError> {
        let fail = |message: String| {
            RegionRouteError::Loader(RegionLoadError::new("mpp-region-lookup", message))
        };
        mpp_memory_error(self.memory).map_err(|error| fail(error.to_string()))?;
        let delay = self
            .budget
            .next_delay(RegionBackoffKind::PdRpc)
            .map_err(|error| fail(format!("{reason:?}: {error:?}")))?;
        let interrupted = self.memory.sleep_for(delay);
        self.budget.finish_wait(!interrupted);
        mpp_memory_error(self.memory).map_err(|error| fail(error.to_string()))
    }
}

fn invalidate_mpp_retry_regions<L>(
    cache: &mut RegionCache<L>,
    retries: &[tidb_proto::metapb::Region],
) {
    for region in retries {
        if let Some(epoch) = &region.region_epoch {
            cache.invalidate(RegionVerId::new(region.id, epoch.conf_ver, epoch.version));
        }
    }
}

async fn establish_mpp_response(
    client: &mut TikvClient<tonic::transport::Channel>,
    meta: TaskMeta,
    receiver_meta: TaskMeta,
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
    memory: tidb_executor::StatementMemory,
) -> Result<MppQueryResponse, String> {
    let connection = client
        .establish_mpp_connection(EstablishMppConnectionRequest {
            sender_meta: Some(meta.clone()),
            receiver_meta: Some(receiver_meta),
        })
        .await;
    match connection {
        Ok(connection) => Ok(MppQueryResponse {
            stream: Some(connection.into_inner()),
            client: Some(client.clone()),
            runtime,
            memory,
            cancel_meta: cancel_task_meta(&meta),
            held_bytes: 0,
            region_lease: None,
            completed: false,
            closed: false,
        }),
        Err(error) => {
            cancel_mpp_task(client, cancel_task_meta(&meta)).await;
            Err(format!("tiflash mpp: connect stream: {error}"))
        }
    }
}

fn cancel_task_meta(meta: &TaskMeta) -> TaskMeta {
    // Go CancelMPPTasks cancels the gather, not just its root task.
    TaskMeta {
        start_ts: meta.start_ts,
        gather_id: meta.gather_id,
        query_ts: meta.query_ts,
        local_query_id: meta.local_query_id,
        server_id: meta.server_id,
        mpp_version: meta.mpp_version,
        resource_group_name: meta.resource_group_name.clone(),
        sql_digest: meta.sql_digest.clone(),
        plan_digest: meta.plan_digest.clone(),
        ..Default::default()
    }
}

async fn cancel_mpp_task(client: &mut TikvClient<tonic::transport::Channel>, meta: TaskMeta) {
    // The source makes one best-effort request, without retry, under the
    // maintained client's ReadTimeoutShort. Never hide the original failure.
    let request = tidb_proto::mpp::CancelTaskRequest {
        meta: Some(meta),
        ..Default::default()
    };
    match tokio::time::timeout(
        tikv_client::tikv::READ_TIMEOUT_SHORT,
        client.cancel_mpp_task(request),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => eprintln!("tiflash mpp: cancel failed: {error}"),
        Err(error) => eprintln!("tiflash mpp: cancel timed out: {error}"),
    }
}

fn mpp_memory_error(memory: &tidb_executor::StatementMemory) -> Result<(), QueryResponseError> {
    memory.check().map_err(|error| {
        let sql = tidb_executor::DriverError::Exec(error).to_mysql_error();
        QueryResponseError::Sql {
            code: sql.code,
            message: sql.message,
        }
    })
}

/// Pull one packet at a time through the shared DistSQL decoder. No query-wide
/// packet queue is retained, and the canonical statement killer can interrupt
/// a stalled message without waiting for another network packet.
struct MppQueryResponse {
    stream: Option<tonic::Streaming<MppDataPacket>>,
    client: Option<TikvClient<tonic::transport::Channel>>,
    runtime: std::sync::Arc<tokio::runtime::Runtime>,
    memory: tidb_executor::StatementMemory,
    cancel_meta: TaskMeta,
    held_bytes: i64,
    region_lease: Option<BackgroundRegionCache<PdRegionLoader>>,
    completed: bool,
    closed: bool,
}

impl MppQueryResponse {
    fn release_packet(&mut self) {
        if self.held_bytes != 0 {
            self.memory.stmt_tracker().consume(-self.held_bytes);
            self.held_bytes = 0;
        }
    }
    fn pull(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.release_packet();
        mpp_memory_error(&self.memory)?;
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };
        let packet = self.runtime.block_on(async {
            let message = stream.message();
            tokio::pin!(message);
            loop {
                tokio::select! {
                    packet = &mut message => return packet.map_err(|error| QueryResponseError::Source(format!("tiflash mpp: stream: {error}"))),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => { mpp_memory_error(&self.memory)?; }
                }
            }
        })?;
        let Some(packet) = packet else {
            self.completed = true;
            self.stream = None;
            self.client = None;
            return Ok(None);
        };
        if let Some(ref error) = packet.error {
            return Err(QueryResponseError::Source(format!(
                "tiflash mpp: task error: {} ({})",
                error.msg, error.code
            )));
        }
        self.held_bytes = packet.encoded_len() as i64;
        self.memory.stmt_tracker().consume(self.held_bytes);
        mpp_memory_error(&self.memory)?;
        Ok(Some(QueryResultSubset {
            data: packet.data.into(),
            runtime: None,
        }))
    }
}

impl tidb_distsql::QueryResponse for MppQueryResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        if self.closed {
            return Ok(None);
        }
        let result = self.pull();
        if result.is_err() || matches!(result, Ok(None)) {
            self.close();
        }
        result
    }
    fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.stream = None;
        self.release_packet();
        if let Some(mut client) = self.client.take() {
            if !self.completed {
                self.runtime
                    .block_on(cancel_mpp_task(&mut client, self.cancel_meta.clone()));
            }
        }
        self.region_lease = None;
    }
}

impl Drop for MppQueryResponse {
    fn drop(&mut self) {
        tidb_distsql::QueryResponse::close(self);
    }
}

/// The caller's end of one MPP-served scan, mirroring `CopRowStream`'s
/// decoded-chunk queue.
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
                    ResponseChannelError::SelectResponse { code, message } => {
                        StorageError::Sql(tidb_executor::MysqlError::new(code as u16, message))
                    }
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
            iter.close();
        }
        self.iter = None;
        self.pending = None;
        self.exhausted = true;
    }

    fn rows_returned(&self) -> u64 {
        self.returned
    }
}

fn mpp_task_metadata(
    snapshot_ts: u64,
    statement: &tidb_executor::remote_scan::PushdownStatementContext,
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

fn encode_dispatch_request(mut request: DispatchTaskRequest) -> DispatchTaskRequest {
    use tikv_client::tikv::Request;

    // This scanner uses classic API-V1 record keys. Match client-go's
    // setAPICtx: the native keyspace codec owns both the wire API version
    // and the null-keyspace sentinel, rather than duplicating their numbers.
    let keyspace = tikv_client::request::Keyspace::Disable;
    request.set_api_version(keyspace.api_version());
    request.set_keyspace_id(keyspace.context_keyspace_id());
    request
}

#[cfg(test)]
mod dispatch_context_tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn mpp_queries_share_statement_identity_and_allocate_gathers_and_tasks() {
        use tidb_executor::{remote_scan::PushdownStatementContext, StmtContext};

        let context = StmtContext::for_query();
        let first = PushdownStatementContext::from_stmt(&context);
        let second = PushdownStatementContext::from_stmt(&context.clone());
        let (a, receiver_a) = mpp_task_metadata(77, &first, "tiflash:3930");
        let (b, receiver_b) = mpp_task_metadata(77, &second, "tiflash:3930");
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
        use tidb_executor::{remote_scan::PushdownStatementContext, StmtContext};

        let a = PushdownStatementContext::from_stmt(&StmtContext::for_query());
        let b = PushdownStatementContext::from_stmt(&StmtContext::for_query());
        let (a, _) = mpp_task_metadata(77, &a, "tiflash:3930");
        let (b, _) = mpp_task_metadata(77, &b, "tiflash:3930");
        assert_ne!(a.local_query_id, b.local_query_id);
        assert_eq!((a.task_id, b.task_id), (1, 1));
    }

    #[test]
    fn mpp_query_timestamp_uses_unix_nanoseconds() {
        use tidb_executor::{remote_scan::PushdownStatementContext, StmtContext};

        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let context = PushdownStatementContext::from_stmt(&StmtContext::for_query());
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

    #[test]
    fn mpp_metadata_carries_the_domain_server_id() {
        let statement = tidb_executor::remote_scan::PushdownStatementContext {
            mpp_server_id: 42,
            ..Default::default()
        };
        let (sender, receiver) = mpp_task_metadata(77, &statement, "tiflash:3930");
        assert_eq!(sender.server_id, 42);
        assert_eq!(receiver.server_id, 42);
        let dispatch = encode_dispatch_request(DispatchTaskRequest {
            meta: Some(sender.clone()),
            ..Default::default()
        });
        let dispatched = dispatch.meta.unwrap();
        assert_eq!(dispatched.local_query_id, sender.local_query_id);
        assert_eq!(dispatched.query_ts, receiver.query_ts);
        assert_eq!(dispatched.gather_id, receiver.gather_id);
        assert_eq!(dispatched.task_id, sender.task_id);
        assert_eq!(dispatched.server_id, 42);
    }

    #[test]
    fn classic_mpp_dispatch_uses_the_transactional_v1_codec() {
        let request = encode_dispatch_request(DispatchTaskRequest {
            meta: Some(TaskMeta::default()),
            ..Default::default()
        });
        let meta = request.meta.unwrap();
        assert_eq!(meta.api_version(), tidb_proto::kvrpcpb::ApiVersion::V1);
        assert_eq!(
            meta.keyspace,
            Some(tidb_proto::mpp::task_meta::Keyspace::KeyspaceId(u32::MAX))
        );
    }
}

#[cfg(test)]
mod mpp_read_batch_tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;
    use tidb_distsql::QueryResponse;
    use tidb_proto::tikvpb::tikv_server::{Tikv, TikvServer};
    use tidb_txnkv::region::BatchLoadOptions;

    fn region(id: u64, start: &[u8], end: &[u8]) -> RegionLocation {
        RegionLocation {
            region: RegionVerId::new(id, 1, 1),
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            ..Default::default()
        }
    }
    fn ranges() -> Vec<(tidb_txnkv::Key, tidb_txnkv::Key)> {
        vec![
            (b"b".to_vec().into(), b"c".to_vec().into()),
            (b"x".to_vec().into(), b"z".to_vec().into()),
        ]
    }
    #[test]
    fn exact_disjoint_ranges_do_not_scan_the_gap() {
        let infos = mpp_region_infos(&ranges(), &[region(1, b"a", b"zz")]);
        assert_eq!(
            infos[0]
                .ranges
                .iter()
                .map(|r| (r.start.clone(), r.end.clone()))
                .collect::<Vec<_>>(),
            vec![
                (b"b".to_vec(), b"c".to_vec()),
                (b"x".to_vec(), b"z".to_vec())
            ]
        );
    }
    #[test]
    fn unrelated_region_is_not_dispatched() {
        let infos = mpp_region_infos(&ranges(), &[region(1, b"d", b"w")]);
        assert!(infos.is_empty(), "the gap has no requested rows");
    }

    #[derive(Clone)]
    struct Service {
        cancels: Arc<AtomicUsize>,
        fail_tail: bool,
    }
    #[tonic::async_trait]
    impl Tikv for Service {
        async fn establish_mpp_connection(
            &self,
            _: tonic::Request<EstablishMppConnectionRequest>,
        ) -> Result<tonic::Response<tonic::codegen::BoxStream<MppDataPacket>>, tonic::Status>
        {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let fail_tail = self.fail_tail;
            tokio::spawn(async move {
                let _ = tx
                    .send(Ok(MppDataPacket {
                        data: b"first".to_vec(),
                        ..Default::default()
                    }))
                    .await;
                tokio::time::sleep(Duration::from_millis(500)).await;
                if fail_tail {
                    let _ = tx
                        .send(Err(tonic::Status::aborted("late stream failure")))
                        .await;
                }
            });
            Ok(tonic::Response::new(Box::pin(
                tokio_stream::wrappers::ReceiverStream::new(rx),
            )))
        }
        async fn cancel_mpp_task(
            &self,
            _: tonic::Request<tidb_proto::mpp::CancelTaskRequest>,
        ) -> Result<tonic::Response<tidb_proto::mpp::CancelTaskResponse>, tonic::Status> {
            self.cancels.fetch_add(1, Ordering::SeqCst);
            Ok(tonic::Response::new(Default::default()))
        }
    }
    struct Fixture {
        address: String,
        runtime: Arc<tokio::runtime::Runtime>,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        task: Option<tokio::task::JoinHandle<()>>,
        cancels: Arc<AtomicUsize>,
    }
    impl Fixture {
        fn new() -> Self {
            Self::with_tail(true)
        }
        fn with_tail(fail_tail: bool) -> Self {
            let runtime = Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap(),
            );
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let cancels = Arc::new(AtomicUsize::new(0));
            let service = Service {
                cancels: Arc::clone(&cancels),
                fail_tail,
            };
            let (shutdown, rx) = tokio::sync::oneshot::channel();
            let task = runtime.spawn(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                tonic::transport::Server::builder()
                    .add_service(TikvServer::new(service))
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        async {
                            let _ = rx.await;
                        },
                    )
                    .await
                    .unwrap();
            });
            Self {
                address,
                runtime,
                shutdown: Some(shutdown),
                task: Some(task),
                cancels,
            }
        }
        fn open(&self) -> Result<MppQueryResponse, String> {
            self.open_with_memory(tidb_executor::StatementMemory::default())
        }
        fn open_with_memory(
            &self,
            memory: tidb_executor::StatementMemory,
        ) -> Result<MppQueryResponse, String> {
            self.runtime.block_on(async {
                let mut client = TikvClient::connect(format!("http://{}", self.address))
                    .await
                    .unwrap();
                let response = tokio::time::timeout(
                    Duration::from_millis(250),
                    establish_mpp_response(
                        &mut client,
                        TaskMeta::default(),
                        TaskMeta::default(),
                        Arc::clone(&self.runtime),
                        memory,
                    ),
                )
                .await;
                response.map_err(|_| "opening drained a stalled stream".to_string())?
            })
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = self.shutdown.take().unwrap().send(());
            self.runtime.block_on(self.task.take().unwrap()).unwrap();
        }
    }
    #[test]
    fn stream_open_returns_before_tail_and_preserves_first_packet() {
        let fixture = Fixture::new();
        let mut response = fixture.open().expect("open must not drain the stream");
        assert_eq!(response.next().unwrap().unwrap().data.as_ref(), b"first");
        response.close();
    }
    #[test]
    fn early_close_and_drop_cancel_the_gather_once() {
        let fixture = Fixture::new();
        let mut response = fixture.open().unwrap();
        response.next().unwrap().unwrap();
        response.close();
        response.close();
        drop(response);
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
        drop(fixture.open().unwrap());
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn later_transport_error_preserves_first_packet_and_cancels() {
        let fixture = Fixture::new();
        let mut response = fixture.open().unwrap();
        assert_eq!(response.next().unwrap().unwrap().data.as_ref(), b"first");
        assert!(
            matches!(response.next(),Err(QueryResponseError::Source(message)) if message.contains("late stream failure"))
        );
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
        assert_eq!(response.next().unwrap(), None);
    }

    #[test]
    fn natural_completion_releases_memory_without_cancel() {
        let fixture = Fixture::with_tail(false);
        let memory = tidb_executor::StatementMemory::default();
        let mut response = fixture.open_with_memory(memory.clone()).unwrap();
        response.next().unwrap().unwrap();
        assert!(memory.bytes_consumed() > 0);
        assert_eq!(response.next().unwrap(), None);
        assert_eq!(memory.bytes_consumed(), 0);
        response.close();
        drop(response);
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn quota_exceeded_is_a_typed_sql_failure_and_cleans_up() {
        let fixture = Fixture::new();
        let memory = tidb_executor::StatementMemory::new(2, tidb_executor::OomAction::Cancel, 42);
        let mut response = fixture.open_with_memory(memory.clone()).unwrap();
        assert!(matches!(
            response.next(),
            Err(QueryResponseError::Sql { code: 8175, .. })
        ));
        assert_eq!(memory.bytes_consumed(), 0);
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn canonical_kill_interrupts_a_stalled_receive() {
        let fixture = Fixture::new();
        let memory = tidb_executor::StatementMemory::default();
        let mut response = fixture.open_with_memory(memory.clone()).unwrap();
        response.next().unwrap().unwrap();
        let killer = Arc::clone(memory.sql_killer());
        let thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            killer.send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
        });
        let started = std::time::Instant::now();
        assert!(matches!(
            response.next(),
            Err(QueryResponseError::Sql { code: 1317, .. })
        ));
        thread.join().unwrap();
        assert!(started.elapsed() < Duration::from_millis(250));
        assert_eq!(memory.bytes_consumed(), 0);
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn exact_ranges_clip_each_region_and_preserve_infinite_end() {
        let ranges = vec![
            (b"b".to_vec().into(), b"c".to_vec().into()),
            (b"x".to_vec().into(), Vec::<u8>::new().into()),
        ];
        let infos = mpp_region_infos(
            &ranges,
            &[
                region(1, b"", b"m"),
                region(2, b"m", b"z"),
                region(3, b"z", b""),
            ],
        );
        assert_eq!(
            infos.iter().map(|r| r.region_id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(infos[0].ranges[0].end, b"c");
        assert_eq!(infos[1].ranges[0].start, b"x");
        assert_eq!(infos[2].ranges[0].start, b"z");
        assert!(infos[2].ranges[0].end.is_empty());
    }
    struct PagedLoader {
        calls: Arc<AtomicUsize>,
        count: u16,
    }
    impl RegionLoader for PagedLoader {
        fn cluster_id(&self) -> u64 {
            1
        }
        fn load_region(&mut self, _: &[u8]) -> Result<RegionLocation, RegionLoadError> {
            Err(RegionLoadError::new("unexpected", "batch lookup required"))
        }
        fn batch_load_regions(
            &mut self,
            requested: &[tidb_txnkv::region::KeyRange],
            limit: usize,
            _: BatchLoadOptions,
        ) -> Result<Vec<RegionLocation>, RegionLoadError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok((0..self.count)
                .filter_map(|id| {
                    let start = id.to_be_bytes().to_vec();
                    let end = (id + 1).to_be_bytes().to_vec();
                    requested
                        .iter()
                        .any(|r| start < r.end && r.start < end)
                        .then(|| region(u64::from(id) + 1, &start, &end))
                })
                .take(limit)
                .collect())
        }
    }
    #[test]
    fn mpp_range_lookup_uses_shared_pagination_and_retained_cache() {
        let calls = Arc::new(AtomicUsize::new(0));
        let storage = tidb_txnkv::SharedReadRuntime::new_injected(
            (),
            RegionCache::new(PagedLoader {
                calls: Arc::clone(&calls),
                count: 150,
            }),
        );
        let cache = storage.region_cache_handle();
        let ranges = vec![(
            0u16.to_be_bytes().to_vec().into(),
            150u16.to_be_bytes().to_vec().into(),
        )];
        let memory = tidb_executor::StatementMemory::default();
        let infos = locate_mpp_region_infos(&cache, &ranges, &memory).unwrap();
        assert_eq!(infos.len(), 150);
        assert_eq!(infos.last().unwrap().ranges[0].end, 150u16.to_be_bytes());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            locate_mpp_region_infos(&cache, &ranges, &memory).unwrap(),
            infos
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        cache
            .with_cache(|cache| {
                invalidate_mpp_retry_regions(
                    cache,
                    &[tidb_proto::metapb::Region {
                        id: 1,
                        region_epoch: Some(tidb_proto::metapb::RegionEpoch {
                            conf_ver: 1,
                            version: 1,
                        }),
                        ..Default::default()
                    }],
                )
            })
            .unwrap();
        locate_mpp_region_infos(&cache, &ranges, &memory).unwrap();
        assert!(
            calls.load(Ordering::SeqCst) > 2,
            "retry_regions must invalidate the retained owner"
        );
    }

    #[test]
    fn remote_projection_uses_the_same_statement_memory_and_killer() {
        let memory = tidb_executor::StatementMemory::default();
        let context = tidb_executor::StmtContext::for_query_with_memory(memory.clone());
        let remote = tidb_executor::remote_scan::PushdownStatementContext::from_stmt(&context);
        assert!(Arc::ptr_eq(remote.memory.sql_killer(), memory.sql_killer()));
        remote.memory.stmt_tracker().consume(17);
        assert_eq!(memory.bytes_consumed(), 17);
        remote.memory.stmt_tracker().consume(-17);
    }
}
