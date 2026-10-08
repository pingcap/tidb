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
//! Disaggregated scans fetch compute topology, probe availability and group
//! regions into one task per node using the validated session policy. All root
//! tasks retain one gather and the existing cancellation/response lifetime.
//!
//! The classic flow per open scan is Go's dispatch choreography: pick a live TiFlash
//! store (`GetAllStores` + the `engine=tiflash` label, `addTiFlashStoreInfo`),
//! collect the table's record-range regions from PD
//! (`constructMPPTasksImpl`'s region split), marshal the tree-form DAG with
//! the scan under a PassThrough exchange sender (`appendMPPDispatchReq` ->
//! `ConstructDAGReq`, `dagReq.EncodeType = TypeChunk` for the root fragment),
//! `DispatchMPPTask` it to the store, then `EstablishMPPConnection` and wrap
//! the packet stream (`MPPDataPacket.data` = tipb `SelectResponse`) in the
//! same `SelectResponseIter` the TiKV cop path consumes.

use std::sync::{Arc, Mutex};
use tidb_distsql::QueryResponse;

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

struct MppHealthClient(tidb_txnkv::rpc::TonicCoprocessorClient);
impl tidb_txnkv::MppAliveClient for MppHealthClient {
    fn is_alive(&self, address: &str, timeout: std::time::Duration) -> Result<bool, String> {
        let runtime = tidb_txnkv::rpc::query_worker_runtime().map_err(|error| error.to_string())?;
        let memory = tidb_executor::StatementMemory::default();
        runtime.block_on(probe_mpp_store(address, &memory, &self.0, timeout))
    }
}
async fn probe_mpp_store(
    address: &str,
    memory: &tidb_executor::StatementMemory,
    transport: &tidb_txnkv::rpc::TonicCoprocessorClient,
    timeout: std::time::Duration,
) -> Result<bool, String> {
    mpp_setup(
        Some(memory),
        Some(timeout),
        "detect compute node",
        None,
        async {
            let mut connection = connect_mpp_client(address, memory, transport, None)
                .await
                .map_err(tonic::Status::unavailable)?;
            mpp_setup(
                Some(memory),
                Some(timeout),
                "detect compute node",
                Some(&connection.route),
                connection
                    .client
                    .is_alive(tidb_proto::mpp::IsAliveRequest::default()),
            )
            .await
            .map(|response| response.into_inner().available)
            .map_err(|error| tonic::Status::unavailable(error.to_string()))
        },
    )
    .await
    .map_err(|error| error.to_string())
}

/// The process-owned MPP dispatch capability.
///
/// PD, region cache and store channel fleet are borrowed from the process.
/// MPP retains no independent connection pool, transport worker or runtime.
#[derive(Clone)]
pub struct TiFlashMppScanSource {
    pd: Arc<Mutex<PdClient>>,
    regions: Arc<BackgroundRegionCache<PdRegionLoader>>,
    runtime: &'static tokio::runtime::Runtime,
    transport: Arc<Mutex<tidb_txnkv::rpc::TonicCoprocessorClient>>,
    /// Go `is.SchemaMetaVersion()`, read at dispatch time from the node's
    /// catalog watch: TiFlash resolves the request's table in the schema
    /// generation the coordinator names, so a stale or zero version makes
    /// even a synced table "not exist".
    schema_version: Arc<dyn Fn() -> i64 + Send + Sync>,
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
        transport: tidb_txnkv::rpc::TonicCoprocessorClient,
        schema_version: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            regions: Arc::new(regions),
            transport: Arc::new(Mutex::new(transport)),
            pd: Arc::new(Mutex::new(pd)),
            runtime: tidb_txnkv::rpc::query_worker_runtime()
                .expect("the shared query runtime starts"),
            schema_version: Arc::new(schema_version),
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
        let config = tidb_config::config_tree::config::get_global_config();
        let enabled = config.disaggregated_tiflash
            && config.use_auto_scaler
            && !request.statement.allow_tiflash_fallback;
        let schema_version = (self.schema_version)();
        let response = self.open_gather(request, schema_version)?;
        let source = self.clone();
        let retry_request = request.clone();
        let response = MppRecoveryResponse::new(
            response,
            enabled,
            request.statement.memory.clone(),
            Box::new(move || {
                source
                    .open_gather(&retry_request, schema_version)
                    .map_err(|error| QueryResponseError::Source(error.to_string()))
            }),
            Box::new(|node_count| {
                crate::tiflash_compute::global_topo_fetcher()
                    .ok_or_else(|| {
                        "TiFlash compute topology fetcher is not initialized".to_owned()
                    })?
                    .recovery_and_get_topo(
                        crate::tiflash_compute::RecoveryType::MEM_LIMIT,
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

    fn open_gather(
        &self,
        request: &PushdownScanRequest,
        schema_version: i64,
    ) -> Result<MppTaskResponses, PushdownScannerError> {
        let refuse = |reason: String| {
            PushdownScannerError::Backend(StorageError::Backend(format!("tiflash mpp: {reason}")))
        };
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

        let config = tidb_config::config_tree::config::get_global_config();
        let tasks = if config.disaggregated_tiflash {
            let policy = crate::tiflash_compute::DispatchPolicy::parse(
                &request.statement.tiflash_compute_dispatch_policy,
            )
            .map_err(refuse)?;
            let stores = self
                .compute_addresses(config.use_auto_scaler, &request.statement.memory)
                .map_err(|error| {
                    if request.statement.memory.check().is_err() {
                        mpp_open_error(&request.statement.memory, error.message)
                    } else {
                        PushdownScannerError::Backend(StorageError::Sql(
                            tidb_executor::MysqlError::new(error.code, error.message),
                        ))
                    }
                })?;
            compute_region_tasks(
                regions,
                &stores,
                policy,
                tidb_util::fastrand::uint64_n(stores.len() as u64) as usize,
            )
            .map_err(refuse)?
        } else {
            vec![(self.tiflash_store_address().map_err(refuse)?, regions)]
        };
        let (mut meta, receiver_meta) =
            mpp_task_metadata(request.snapshot_ts, &request.statement, &tasks[0].0);
        let mut streams = MppTaskResponses {
            streams: std::collections::VecDeque::new(),
            node_count: tasks.len(),
        };
        for (index, (address, regions)) in tasks.into_iter().enumerate() {
            if index != 0 {
                meta.task_id = request.statement.mpp_query_info.alloc_task_id();
            }
            meta.address.clone_from(&address);
            streams.streams.push_back(
                self.open_task(
                    request,
                    address,
                    regions,
                    region_lease
                        .open_lease()
                        .map_err(|error| refuse(error.to_string()))?,
                    meta.clone(),
                    receiver_meta.clone(),
                    schema_version,
                )?,
            );
        }
        Ok(streams)
    }

    fn compute_addresses(
        &self,
        auto_scaler: bool,
        memory: &tidb_executor::StatementMemory,
    ) -> Result<Vec<String>, crate::tiflash_compute::TopologyError> {
        let mut budget = RegionBackoffBudget::new(std::time::Duration::from_secs(20));
        loop {
            mpp_memory_error(memory).map_err(|error| error.to_string())?;
            let stores = if auto_scaler {
                crate::tiflash_compute::global_topo_fetcher()
                    .ok_or_else(|| {
                        "TiFlash compute topology fetcher is not initialized".to_owned()
                    })?
                    .fetch_and_get_topo()?
            } else {
                let cache = self.regions.tiflash_compute_store_cache();
                self.runtime
                    .block_on(cache.get_or_load(|| async {
                        self.pd
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .all_store_metadata()
                    }))
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .map(|store| store.address)
                    .collect()
            };
            let was_empty = stores.is_empty();
            let transport = self.transport.lock().expect("MPP transport lock").clone();
            let alive = self.runtime.block_on(async {
                let mut probes = tokio::task::JoinSet::new();
                for address in stores {
                    let transport = transport.clone();
                    let memory = memory.clone();
                    probes.spawn(async move {
                        let prober = tidb_txnkv::global_mpp_failed_store_prober();
                        // Go deprecated tidb_mpp_store_fail_ttl is always zero.
                        if !prober.is_recovery(&address, std::time::Duration::ZERO) {
                            return None;
                        }
                        match probe_mpp_store(
                            &address,
                            &memory,
                            &transport,
                            tidb_txnkv::DETECT_TIMEOUT_LIMIT,
                        )
                        .await
                        {
                            Ok(true) => Some(address),
                            _ => {
                                // Statement cancellation is not evidence that a store failed.
                                if memory.check().is_ok() {
                                    prober.add(address, Arc::new(MppHealthClient(transport)));
                                }
                                None
                            }
                        }
                    });
                }
                let mut alive = Vec::new();
                while let Some(result) = probes.join_next().await {
                    mpp_memory_error(memory).map_err(|error| error.to_string())?;
                    if let Some(address) = result.map_err(|error| error.to_string())? {
                        alive.push(address);
                    }
                }
                Ok::<_, String>(alive)
            })?;
            if !alive.is_empty() {
                return Ok(alive);
            }
            if !auto_scaler {
                self.regions.tiflash_compute_store_cache().invalidate();
            }
            let error = if !auto_scaler {
                "tiflash_compute node is unavailable"
            } else if was_empty {
                "Cannot find proper topo to dispatch MPPTask: topo from AutoScaler is empty"
            } else {
                "Cannot find proper topo to dispatch MPPTask: detect aliveness failed, no alive ComputeNode"
            };
            let delay = budget
                .next_delay(RegionBackoffKind::TiFlashRpc)
                .map_err(|_| error.to_owned())?;
            let waited = self.runtime.block_on(mpp_setup(
                Some(memory),
                None,
                "compute topology retry",
                None,
                async {
                    tokio::time::sleep(delay).await;
                    Ok(())
                },
            ));
            budget.finish_wait(waited.is_ok());
            waited.map_err(|error| error.to_string())?;
        }
    }

    fn open_task(
        &self,
        request: &PushdownScanRequest,
        address: String,
        regions: Vec<tidb_proto::coprocessor::RegionInfo>,
        region_lease: BackgroundRegionCache<PdRegionLoader>,
        meta: TaskMeta,
        receiver_meta: TaskMeta,
        schema_version: i64,
    ) -> Result<Box<dyn MppTaskResponse>, PushdownScannerError> {
        let refuse = |reason: String| {
            PushdownScannerError::Backend(StorageError::Backend(format!("tiflash mpp: {reason}")))
        };
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
            regions: region_infos,
            schema_ver: schema_version,
            ..Default::default()
        });

        // Dispatch opens the live stream; the shared response consumer pulls
        // packets on demand using a handle to the shared query runtime.
        let config = tidb_config::config_tree::config::get_global_config();
        let compute_cache = (config.disaggregated_tiflash && !config.use_auto_scaler)
            .then(|| self.regions.tiflash_compute_store_cache());
        let mut response = self
            .runtime
            .block_on(async {
                let memory = &request.statement.memory;
                let transport = self.transport.lock().expect("MPP transport lock").clone();
                let mut connection =
                    connect_mpp_client(&address, memory, &transport, compute_cache.clone()).await?;
                let mut dispatch_request = tonic::Request::new(dispatch_request);
                dispatch_request.set_timeout(tikv_client::tikv::READ_TIMEOUT_MEDIUM);
                let response = mpp_setup(
                    Some(memory),
                    Some(tikv_client::tikv::READ_TIMEOUT_MEDIUM),
                    "dispatch",
                    Some(&connection.route),
                    connection.client.dispatch_mpp_task(dispatch_request),
                )
                .await
                .map_err(|error| {
                    connection.invalidate_compute_cache();
                    error.to_string()
                })?;
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
                // Go SendRequest selects from the process fleet for each RPC.
                // Dispatch already registered the gather: clean it up if the
                // next channel cannot be acquired (including statement KILL).
                let mut connection =
                    match connect_mpp_client(&address, memory, &transport, compute_cache.clone())
                        .await
                    {
                        Ok(next) => next,
                        Err(error) => {
                            cancel_mpp_task(&mut connection, cancel_task_meta(&meta)).await;
                            return Err(error);
                        }
                    };
                establish_mpp_response(
                    &mut connection,
                    meta.clone(),
                    receiver_meta.clone(),
                    std::sync::Arc::new(self.runtime.handle().clone()),
                    request.statement.memory.clone(),
                )
                .await
            })
            .map_err(|message| mpp_open_error(&request.statement.memory, message))?;
        response.region_lease = Some(region_lease);

        Ok(Box::new(response))
    }
}

// Go buildBatchCopTasksConsistentHash groups clipped regions in encounter order.
fn compute_region_tasks(
    regions: Vec<tidb_proto::coprocessor::RegionInfo>,
    stores: &[String],
    policy: crate::tiflash_compute::DispatchPolicy,
    start: usize,
) -> Result<Vec<(String, Vec<tidb_proto::coprocessor::RegionInfo>)>, String> {
    use crate::tiflash_compute::DispatchPolicy;
    if stores.is_empty() {
        return Err("tiflash_compute node is unavailable".into());
    }
    let mut tasks: Vec<(String, Vec<tidb_proto::coprocessor::RegionInfo>)> = Vec::new();
    for (index, region) in regions.into_iter().enumerate() {
        let address = match policy {
            DispatchPolicy::RoundRobin => {
                stores[(start % stores.len() + index % stores.len()) % stores.len()].clone()
            }
            DispatchPolicy::ConsistentHash => {
                let mut max_hash = 0;
                let mut selected = String::new();
                for store in stores {
                    let hash = tidb_executor::shuffle::murmur3_sum32(
                        format!("{store}-{}", region.region_id).as_bytes(),
                    );
                    if hash > max_hash {
                        max_hash = hash;
                        selected.clone_from(store);
                    }
                }
                selected
            }
            DispatchPolicy::Invalid => return Err("unexpected dispatch policy 2".into()),
        };
        if let Some((_, grouped)) = tasks.iter_mut().find(|(store, _)| store == &address) {
            grouped.push(region);
        } else {
            tasks.push((address, vec![region]));
        }
    }
    Ok(tasks)
}

// The transport charges a packet while receiving it. Hand that charge to the
// gather holder before buffering, matching coordinator -> ExecutorWithRetry.
trait MppTaskResponse: QueryResponse + Send {
    fn release_packet_memory(&mut self);
}
impl MppTaskResponse for MppQueryResponse {
    fn release_packet_memory(&mut self) {
        self.release_packet();
    }
}

// One gather owns all raw task responses, below the shared SelectResponse decoder.
struct MppTaskResponses {
    streams: std::collections::VecDeque<Box<dyn MppTaskResponse>>,
    node_count: usize,
}
impl QueryResponse for MppTaskResponses {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        while let Some(stream) = self.streams.front_mut() {
            match stream.next() {
                Ok(Some(packet)) => {
                    stream.release_packet_memory();
                    return Ok(Some(packet));
                }
                Ok(None) => {
                    stream.close();
                    self.streams.pop_front();
                }
                Err(error) => {
                    self.close();
                    return Err(error);
                }
            }
        }
        Ok(None)
    }
    fn close(&mut self) {
        for stream in &mut self.streams {
            stream.close();
        }
        self.streams.clear();
    }
}
impl Drop for MppTaskResponses {
    fn drop(&mut self) {
        self.close();
    }
}

// Go ExecutorWithRetry holds two raw responses and retries at most three times.
// Once any response is exposed to SelectResponseIter, replay is permanently unsafe.
struct MppRecoveryResponse {
    gather: MppTaskResponses,
    recovering: bool,
    attempts: u32,
    held: std::collections::VecDeque<QueryResultSubset>,
    memory: tidb_executor::StatementMemory,
    open: Option<Box<dyn FnMut() -> Result<MppTaskResponses, QueryResponseError> + Send>>,
    recover: Box<dyn FnMut(usize) -> Result<(), String> + Send>,
    closed: bool,
}
impl MppRecoveryResponse {
    fn new(
        gather: MppTaskResponses,
        enabled: bool,
        memory: tidb_executor::StatementMemory,
        open: Box<dyn FnMut() -> Result<MppTaskResponses, QueryResponseError> + Send>,
        recover: Box<dyn FnMut(usize) -> Result<(), String> + Send>,
    ) -> Self {
        Self {
            gather,
            recovering: enabled,
            attempts: 0,
            held: Default::default(),
            memory,
            open: Some(open),
            recover,
            closed: false,
        }
    }
    fn clear_held(&mut self) {
        for packet in self.held.drain(..) {
            self.memory
                .stmt_tracker()
                .consume(-(packet.data.len() as i64));
        }
    }
    fn pull(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        mpp_memory_error(&self.memory)?;
        while self.recovering && self.held.len() < 2 {
            match self.gather.next() {
                Ok(Some(packet)) => {
                    self.memory.stmt_tracker().consume(packet.data.len() as i64);
                    self.held.push_back(packet);
                    mpp_memory_error(&self.memory)?;
                }
                Ok(None) => break,
                Err(error) => {
                    // Local quota/KILL errors are terminal, even if their text
                    // happens to contain the remote TiFlash memory error pattern.
                    mpp_memory_error(&self.memory)?;
                    let eligible = matches!(&error, QueryResponseError::Source(message)
                        if message.contains("Memory limit"));
                    if !eligible || self.attempts >= 3 {
                        return Err(error);
                    }
                    self.attempts += 1;
                    if (self.recover)(self.gather.node_count).is_err() {
                        return Err(error);
                    }
                    self.gather.close();
                    let replacement = (self.open.as_mut().expect("open recovery response"))()
                        .map_err(|_| error)?;
                    self.clear_held();
                    self.gather = replacement;
                }
            }
        }
        self.recovering = false;
        if let Some(packet) = self.held.pop_front() {
            self.memory
                .stmt_tracker()
                .consume(-(packet.data.len() as i64));
            return Ok(Some(packet));
        }
        self.gather.next()
    }
}
impl QueryResponse for MppRecoveryResponse {
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
        self.clear_held();
        self.gather.close();
        self.open = None;
    }
}
impl Drop for MppRecoveryResponse {
    fn drop(&mut self) {
        self.close();
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

// Query RPCs retain the canonical killer; cleanup RPCs use Go's background
// context but still honor the same physical scope retirement and deadline.
// Keep transport provenance until the retry decision is made.
// Statement cancellation and owner retirement have no remote RPC identity.
#[derive(Debug)]
struct MppSetupError {
    message: String,
    transport: Option<tidb_txnkv::rpc::DirectUnaryClientError>,
}

impl std::fmt::Display for MppSetupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl MppSetupError {
    fn stopped(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            transport: None,
        }
    }

    fn retryable(&self) -> bool {
        use tidb_txnkv::rpc::{DirectUnaryClientError as Error, DirectUnaryGrpcCode};
        matches!(
            &self.transport,
            Some(Error::Connection(_) | Error::Timeout { .. })
        ) && self.transport.as_ref().and_then(Error::grpc_code)
            != Some(DirectUnaryGrpcCode::Canceled)
    }
}

async fn mpp_setup<T>(
    memory: Option<&tidb_executor::StatementMemory>,
    timeout: Option<std::time::Duration>,
    stage: &str,
    route: Option<&tidb_txnkv::rpc::StoreRpcChannel>,
    future: impl std::future::Future<Output = Result<T, tonic::Status>>,
) -> Result<T, MppSetupError> {
    let check = || {
        if route.is_some_and(|route| route.is_closed()) {
            return Err(MppSetupError::stopped(
                "tiflash mpp: shared transport generation closed",
            ));
        }
        if let Some(memory) = memory {
            mpp_memory_error(memory).map_err(|error| MppSetupError::stopped(error.to_string()))?;
        }
        Ok(())
    };
    check()?;
    tokio::pin!(future);
    let deadline = async {
        match timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            biased;
            _ = &mut deadline => return Err(MppSetupError {
                message: format!("tiflash mpp: {stage}: timed out"),
                transport: route.map(|route| route.timeout_error(timeout.expect("armed deadline"))),
            }),
            result = &mut future => {
                check()?;
                return result.map_err(|error| MppSetupError {
                    message: format!("tiflash mpp: {stage}: {error}"),
                    transport: route.map(|route| route.rpc_error(error, timeout.unwrap_or(MPP_RECEIVE_TIMEOUT))),
                });
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => { check()?; }
        }
    }
}

// Go copr.TiFlashReadTimeoutUltraLong bounds each receive, not stream lifetime.
const MPP_RECEIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);

#[derive(Clone)]
struct MppRpcConnection {
    client: TikvClient<tonic::transport::Channel>,
    route: tidb_txnkv::rpc::StoreRpcChannel,
    transport: tidb_txnkv::rpc::TonicCoprocessorClient,
    address: String,
    compute_cache: Option<Arc<tikv_client::TiFlashComputeStoreCache>>,
}

impl MppRpcConnection {
    fn invalidate_compute_cache(&self) {
        if let Some(cache) = &self.compute_cache {
            cache.invalidate();
        }
    }
}

async fn connect_mpp_client(
    address: &str,
    memory: &tidb_executor::StatementMemory,
    transport: &tidb_txnkv::rpc::TonicCoprocessorClient,
    compute_cache: Option<Arc<tikv_client::TiFlashComputeStoreCache>>,
) -> Result<MppRpcConnection, String> {
    let result = async {
        mpp_memory_error(memory).map_err(|error| error.to_string())?;
        transport
            .store_rpc_channel(address)
            .await
            .map_err(|error| error.to_string())
    }
    .await;
    match result {
        Ok(route) => Ok(MppRpcConnection {
            client: route.client(),
            route,
            transport: transport.clone(),
            address: address.to_owned(),
            compute_cache,
        }),
        Err(error) => {
            if let Some(cache) = compute_cache {
                cache.invalidate();
            }
            Err(error)
        }
    }
}

async fn establish_mpp_response(
    connection: &mut MppRpcConnection,
    meta: TaskMeta,
    receiver_meta: TaskMeta,
    runtime: std::sync::Arc<tokio::runtime::Handle>,
    memory: tidb_executor::StatementMemory,
) -> Result<MppQueryResponse, String> {
    // This is the coordinator's effective CopNextMaxBackoff budget. Each
    // reservation and completed/interrupted wait uses the native retry owner.
    let mut backoff = RegionBackoffBudget::campaign_default();
    let result = loop {
        let attempt = async {
            let response = mpp_setup(
                Some(&memory),
                None,
                "connect stream",
                Some(&connection.route),
                connection
                    .client
                    .establish_mpp_connection(EstablishMppConnectionRequest {
                        sender_meta: Some(meta.clone()),
                        receiver_meta: Some(receiver_meta.clone()),
                    }),
            )
            .await?;
            let mut stream = response.into_inner();
            // client-go getMPPStreamResponse receives once inside SendRequest.
            // Only errors before this first packet belong to setup retry.
            let first = mpp_setup(
                Some(&memory),
                Some(MPP_RECEIVE_TIMEOUT),
                "first receive",
                Some(&connection.route),
                stream.message(),
            )
            .await?;
            Ok::<_, MppSetupError>((stream, first))
        }
        .await;
        match attempt {
            Ok(response) => break Ok(response),
            Err(error) => {
                connection.invalidate_compute_cache();
                if !error.retryable() {
                    break Err(error.to_string());
                }
                let delay = match backoff.next_delay(RegionBackoffKind::TiFlashRpc) {
                    Ok(delay) => delay,
                    Err(_) => break Err(error.to_string()),
                };
                let waited = mpp_setup(
                    Some(&memory),
                    None,
                    "retry wait",
                    Some(&connection.route),
                    async {
                        tokio::time::sleep(delay).await;
                        Ok(())
                    },
                )
                .await;
                backoff.finish_wait(waited.is_ok());
                if let Err(error) = waited {
                    break Err(error.to_string());
                }
                // Select from the same process fleet for each SendRequest.
                match connect_mpp_client(
                    &connection.address,
                    &memory,
                    &connection.transport,
                    connection.compute_cache.clone(),
                )
                .await
                {
                    Ok(next) => *connection = next,
                    Err(error) => break Err(error),
                }
            }
        }
    };
    match result {
        Ok((stream, first)) => {
            let completed = first.is_none();
            let held_bytes = first
                .as_ref()
                .map_or(0, |packet| packet.encoded_len() as i64);
            memory.stmt_tracker().consume(held_bytes);
            if let Err(error) = mpp_memory_error(&memory) {
                memory.stmt_tracker().consume(-held_bytes);
                drop(stream);
                cancel_mpp_task(connection, cancel_task_meta(&meta)).await;
                return Err(error.to_string());
            }
            Ok(MppQueryResponse {
                stream: (!completed).then_some(stream),
                first_packet: first,
                connection: (!completed).then(|| connection.clone()),
                runtime,
                memory,
                cancel_meta: cancel_task_meta(&meta),
                held_bytes,
                receive_timeout: MPP_RECEIVE_TIMEOUT,
                region_lease: None,
                completed,
                closed: false,
            })
        }
        Err(error) => {
            cancel_mpp_task(connection, cancel_task_meta(&meta)).await;
            Err(error)
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

async fn cancel_mpp_task(connection: &mut MppRpcConnection, meta: TaskMeta) {
    // Select from the current process fleet, not a retired stream generation.
    let route = match connection
        .transport
        .store_rpc_channel(&connection.address)
        .await
    {
        Ok(route) => route,
        Err(error) => {
            connection.invalidate_compute_cache();
            eprintln!("tiflash mpp: cancel unavailable: {error}");
            return;
        }
    };
    let mut client = route.client();
    let mut request = tonic::Request::new(tidb_proto::mpp::CancelTaskRequest {
        meta: Some(meta),
        ..Default::default()
    });
    request.set_timeout(tikv_client::tikv::READ_TIMEOUT_SHORT);
    if let Err(error) = mpp_setup(
        None,
        Some(tikv_client::tikv::READ_TIMEOUT_SHORT),
        "cancel",
        Some(&route),
        client.cancel_mpp_task(request),
    )
    .await
    {
        connection.invalidate_compute_cache();
        eprintln!("tiflash mpp: cancel failed: {error}");
    }
}

fn mpp_open_error(
    memory: &tidb_executor::StatementMemory,
    message: String,
) -> PushdownScannerError {
    // The canonical killer stays signalled until statement retirement. Keep
    // its SQL identity across the synchronous scanner boundary after cleanup.
    let error = match memory.check() {
        Err(error) => StorageError::Sql(tidb_executor::DriverError::Exec(error).to_mysql_error()),
        Ok(()) => StorageError::Backend(message),
    };
    PushdownScannerError::Backend(error)
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
    first_packet: Option<MppDataPacket>,
    stream: Option<tonic::Streaming<MppDataPacket>>,
    connection: Option<MppRpcConnection>,
    runtime: std::sync::Arc<tokio::runtime::Handle>,
    memory: tidb_executor::StatementMemory,
    cancel_meta: TaskMeta,
    held_bytes: i64,
    receive_timeout: std::time::Duration,
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
        if self
            .connection
            .as_ref()
            .is_some_and(|connection| connection.route.is_closed())
        {
            return Err(QueryResponseError::Source(
                "tiflash mpp: shared transport generation closed".to_owned(),
            ));
        }
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };
        let packet = if let Some(first) = self.first_packet.take() {
            Some(first)
        } else {
            self.runtime
                .block_on(mpp_setup(
                    Some(&self.memory),
                    Some(self.receive_timeout),
                    "stream receive",
                    self.connection.as_ref().map(|connection| &connection.route),
                    stream.message(),
                ))
                .map_err(|error| match mpp_memory_error(&self.memory) {
                    Err(sql) => sql,
                    Ok(()) => QueryResponseError::Source(error.to_string()),
                })?
        };
        let Some(packet) = packet else {
            self.completed = true;
            self.stream = None;
            self.connection = None;
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
        self.first_packet = None;
        self.stream = None;
        self.release_packet();
        if let Some(mut connection) = self.connection.take() {
            if !self.completed {
                self.runtime
                    .block_on(cancel_mpp_task(&mut connection, self.cancel_meta.clone()));
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
    // Go copr.MPPClient.DispatchMPPTask task lifetime, independent of the RPC deadline.
    request.timeout = 60;
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
        let wire = request.encode_to_vec();
        let request = DispatchTaskRequest::decode(wire.as_slice()).unwrap();
        assert_eq!(request.timeout, 60);
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
    use tidb_pd_client::ClusterSecurity;
    use tidb_proto::tikvpb::tikv_server::{Tikv, TikvServer};
    use tidb_txnkv::region::BatchLoadOptions;
    use tidb_txnkv::{DirectUnaryClient, LockWaitInfoClient};
    use tokio_stream::StreamExt as _;

    struct ComputePd {
        endpoint: String,
        address: String,
        reads: Arc<AtomicUsize>,
        fail: Arc<std::sync::atomic::AtomicBool>,
        empty_once: Arc<std::sync::atomic::AtomicBool>,
    }
    #[tonic::async_trait]
    impl tidb_proto::test_pd_server::Pd for ComputePd {
        async fn get_members(
            &self,
            _: tonic::Request<tidb_proto::pdpb::GetMembersRequest>,
        ) -> Result<tonic::Response<tidb_proto::pdpb::GetMembersResponse>, tonic::Status> {
            let member = tidb_proto::pdpb::Member {
                member_id: 1,
                client_urls: vec![self.endpoint.clone()],
                ..Default::default()
            };
            Ok(tonic::Response::new(tidb_proto::pdpb::GetMembersResponse {
                header: Some(tidb_proto::pdpb::ResponseHeader {
                    cluster_id: 1,
                    ..Default::default()
                }),
                members: vec![member.clone()],
                leader: Some(member),
                ..Default::default()
            }))
        }
        async fn get_all_stores(
            &self,
            _: tonic::Request<tidb_proto::pdpb::GetAllStoresRequest>,
        ) -> Result<tonic::Response<tidb_proto::pdpb::GetAllStoresResponse>, tonic::Status>
        {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err(tonic::Status::permission_denied(
                    "PD lookup denied after warmup",
                ));
            }
            Ok(tonic::Response::new(
                tidb_proto::pdpb::GetAllStoresResponse {
                    header: Some(tidb_proto::pdpb::ResponseHeader {
                        cluster_id: 1,
                        ..Default::default()
                    }),
                    stores: if self.empty_once.swap(false, Ordering::SeqCst) {
                        vec![]
                    } else {
                        [
                            (1, "tiflash_compute", 0),
                            (2, "tiflash", 0),
                            (3, "tiflash_compute", 1),
                            (4, "tiflash_compute", 2),
                        ]
                        .into_iter()
                        .map(|(id, engine, state)| tidb_proto::metapb::Store {
                            id,
                            address: self.address.clone(),
                            state,
                            labels: vec![tidb_proto::metapb::StoreLabel {
                                key: "engine".into(),
                                value: engine.into(),
                            }],
                            ..Default::default()
                        })
                        .collect()
                    },
                    ..Default::default()
                },
            ))
        }
    }
    struct ComputePdFixture {
        runtime: Arc<tokio::runtime::Runtime>,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        task: Option<tokio::task::JoinHandle<()>>,
        pd: PdClient,
        reads: Arc<AtomicUsize>,
        fail: Arc<std::sync::atomic::AtomicBool>,
        empty_once: Arc<std::sync::atomic::AtomicBool>,
    }
    impl ComputePdFixture {
        fn new(fixture: &Fixture) -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let reads = Arc::new(AtomicUsize::new(0));
            let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let empty_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let service = ComputePd {
                endpoint: endpoint.clone(),
                address: fixture.address.clone(),
                reads: reads.clone(),
                fail: fail.clone(),
                empty_once: empty_once.clone(),
            };
            let (shutdown, rx) = tokio::sync::oneshot::channel();
            let task = fixture.runtime.spawn(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                tonic::transport::Server::builder()
                    .add_service(tidb_proto::test_pd_server::PdServer::new(service))
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        async {
                            let _ = rx.await;
                        },
                    )
                    .await
                    .unwrap();
            });
            let pd = PdClient::connect(endpoint, Duration::from_secs(2)).unwrap();
            Self {
                runtime: fixture.runtime.clone(),
                shutdown: Some(shutdown),
                task: Some(task),
                pd,
                reads,
                fail,
                empty_once,
            }
        }
        fn source(
            &self,
            fixture: &Fixture,
            regions: BackgroundRegionCache<PdRegionLoader>,
        ) -> TiFlashMppScanSource {
            TiFlashMppScanSource::new(
                self.pd.clone(),
                regions,
                fixture.transport.lock().unwrap().clone(),
                || 1,
            )
        }
    }
    impl Drop for ComputePdFixture {
        fn drop(&mut self) {
            let _ = self.shutdown.take().unwrap().send(());
            self.runtime.block_on(self.task.take().unwrap()).unwrap();
        }
    }

    #[test]
    fn compute_cache_batch_pd_topology_shared_across_sources() {
        let fixture = Fixture::with_tail(false);
        let pd = ComputePdFixture::new(&fixture);
        let owner = BackgroundRegionCache::start_gc(
            RegionCache::new(PdRegionLoader::from_client(pd.pd.clone())),
            Duration::from_secs(3600),
            1,
        )
        .unwrap();
        let first = pd.source(&fixture, owner.open_lease().unwrap());
        let second = pd.source(&fixture, owner.open_lease().unwrap());
        let memory = tidb_executor::StatementMemory::default();
        for source in [&first, &second, &first.clone()] {
            assert_eq!(
                source.compute_addresses(false, &memory).unwrap(),
                vec![fixture.address.clone()]
            );
        }
        assert_eq!(
            pd.reads.load(Ordering::SeqCst),
            1,
            "one process compute cache must serve every source"
        );
    }

    #[test]
    fn compute_cache_batch_warm_topology_survives_pd_failure() {
        let fixture = Fixture::with_tail(false);
        let pd = ComputePdFixture::new(&fixture);
        let owner = BackgroundRegionCache::start_gc(
            RegionCache::new(PdRegionLoader::from_client(pd.pd.clone())),
            Duration::from_secs(3600),
            1,
        )
        .unwrap();
        let source = pd.source(&fixture, owner.open_lease().unwrap());
        let memory = tidb_executor::StatementMemory::default();
        assert_eq!(
            source.compute_addresses(false, &memory).unwrap(),
            vec![fixture.address.clone()]
        );
        pd.fail.store(true, Ordering::SeqCst);
        assert_eq!(
            source.compute_addresses(false, &memory).unwrap(),
            vec![fixture.address.clone()]
        );
        assert_eq!(pd.reads.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn compute_cache_batch_empty_topology_reloads_and_authorities_are_independent() {
        let fixture = Fixture::with_tail(false);
        let pd = ComputePdFixture::new(&fixture);
        pd.empty_once.store(true, Ordering::SeqCst);
        let memory = tidb_executor::StatementMemory::default();
        for expected_reads in [2, 3] {
            let owner = BackgroundRegionCache::start_gc(
                RegionCache::new(PdRegionLoader::from_client(pd.pd.clone())),
                Duration::from_secs(3600),
                1,
            )
            .unwrap();
            let source = pd.source(&fixture, owner.open_lease().unwrap());
            assert_eq!(
                source.compute_addresses(false, &memory).unwrap(),
                vec![fixture.address.clone()]
            );
            assert_eq!(pd.reads.load(Ordering::SeqCst), expected_reads);
        }
    }

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
    fn compute_cache_batch_socket_establishment_cancel_and_tail_policy() {
        for (opening, fail_tail, expect_reload) in [
            (Opening::Normal, false, false),
            (Opening::RetryHeaders, false, true),
            (Opening::RetryFirstPacket, false, true),
            (Opening::Canceled, false, true),
            (Opening::MemoryLimit, false, false),
            (Opening::Normal, true, false),
            (Opening::CancelFailure, false, true),
        ] {
            let fixture =
                Fixture::with_opening(fail_tail, false, false, b"first".to_vec(), opening);
            let cache = Arc::new(tikv_client::TiFlashComputeStoreCache::default());
            let reads = AtomicUsize::new(0);
            let read = || async {
                reads.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(Vec::<tidb_proto::metapb::Store>::new())
            };
            fixture.runtime.block_on(cache.get_or_load(read)).unwrap();
            let result = fixture.runtime.block_on(async {
                let mut connection = connect_mpp_client(
                    &fixture.address,
                    &tidb_executor::StatementMemory::default(),
                    &fixture.transport.lock().unwrap(),
                    Some(cache.clone()),
                )
                .await
                .unwrap();
                establish_mpp_response(
                    &mut connection,
                    TaskMeta::default(),
                    TaskMeta::default(),
                    Arc::new(fixture.runtime.handle().clone()),
                    tidb_executor::StatementMemory::default(),
                )
                .await
            });
            match result {
                Ok(mut response) => {
                    let first = response.next();
                    if matches!(opening, Opening::MemoryLimit) {
                        assert!(first.is_err());
                    } else {
                        assert!(first.unwrap().is_some());
                    }
                    if fail_tail {
                        assert!(response.next().is_err());
                    }
                    response.close();
                }
                Err(error) => assert!(matches!(opening, Opening::Canceled), "{error}"),
            }
            fixture.runtime.block_on(cache.get_or_load(read)).unwrap();
            assert_eq!(
                reads.load(Ordering::SeqCst),
                if expect_reload { 2 } else { 1 }
            );
        }
    }

    #[test]
    fn compute_cache_batch_dispatch_policy_and_application_errors() {
        let config = tidb_config::config_tree::config::get_global_config();
        struct Restore(Arc<tidb_config::config_tree::config::Config>);
        impl Drop for Restore {
            fn drop(&mut self) {
                tidb_config::config_tree::config::store_global_config(self.0.clone());
            }
        }
        let _restore = Restore(config.clone());
        for (disaggregated, autoscaler) in [(true, false), (true, true), (false, false)] {
            for application_error in [false, true] {
                let mut current = (*config).clone();
                current.disaggregated_tiflash = disaggregated;
                current.use_auto_scaler = autoscaler;
                tidb_config::config_tree::config::store_global_config(current);
                let fixture = Fixture::with_opening(
                    false,
                    false,
                    false,
                    b"first".to_vec(),
                    if application_error {
                        Opening::DispatchApplicationError
                    } else {
                        Opening::DispatchUnavailable
                    },
                );
                let pd = ComputePdFixture::new(&fixture);
                let owner = BackgroundRegionCache::start_gc(
                    RegionCache::new(PdRegionLoader::from_client(pd.pd.clone())),
                    Duration::from_secs(3600),
                    1,
                )
                .unwrap();
                let source = pd.source(&fixture, owner.open_lease().unwrap());
                let request = PushdownScanRequest {
                    table_id: 1,
                    index: None,
                    columns: vec![],
                    handle_index: None,
                    primary_column_ids: vec![],
                    primary_prefix_column_ids: vec![],
                    predicates: vec![],
                    output_offsets: None,
                    topn: None,
                    limit: None,
                    prefix_limit: None,
                    paging_min_size: None,
                    aggregate: None,
                    desc: false,
                    keep_order: false,
                    allow_unordered_response: false,
                    snapshot_ts: 1,
                    read_engine: PushdownReadEngine::TiFlash,
                    schema_version: 1,
                    ranges: vec![],
                    range_hints: vec![],
                    statement: Default::default(),
                };
                source
                    .compute_addresses(false, &request.statement.memory)
                    .unwrap();
                let result = source.open_task(
                    &request,
                    fixture.address.clone(),
                    vec![],
                    owner.open_lease().unwrap(),
                    TaskMeta::default(),
                    TaskMeta::default(),
                    1,
                );
                assert!(result.is_err());
                source
                    .compute_addresses(false, &request.statement.memory)
                    .unwrap();
                assert_eq!(
                    pd.reads.load(Ordering::SeqCst),
                    if disaggregated && !autoscaler && !application_error {
                        2
                    } else {
                        1
                    }
                );
            }
        }
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

    #[derive(Clone, Copy, Default)]
    enum Opening {
        #[default]
        Normal,
        RetryHeaders,
        RetryFirstPacket,
        MemoryLimit,
        TwoPackets,
        Unavailable,
        StallFirstPacket,
        Canceled,
        Empty,
        DispatchUnavailable,
        DispatchApplicationError,
        CancelFailure,
    }

    #[derive(Clone)]
    struct Service {
        opening: Opening,
        attempts: Arc<AtomicUsize>,
        cancels: Arc<AtomicUsize>,
        fail_tail: bool,
        stall_setup: bool,
        first_packet: Vec<u8>,
    }
    #[tonic::async_trait]
    impl Tikv for Service {
        async fn dispatch_mpp_task(
            &self,
            _: tonic::Request<DispatchTaskRequest>,
        ) -> Result<tonic::Response<tidb_proto::mpp::DispatchTaskResponse>, tonic::Status> {
            if matches!(self.opening, Opening::DispatchUnavailable) {
                return Err(tonic::Status::unavailable("dispatch unavailable"));
            }
            Ok(tonic::Response::new(
                tidb_proto::mpp::DispatchTaskResponse {
                    error: matches!(self.opening, Opening::DispatchApplicationError).then(|| {
                        tidb_proto::mpp::Error {
                            code: 1,
                            msg: "application rejection".into(),
                            ..Default::default()
                        }
                    }),
                    ..Default::default()
                },
            ))
        }
        async fn is_alive(
            &self,
            _: tonic::Request<tidb_proto::mpp::IsAliveRequest>,
        ) -> Result<tonic::Response<tidb_proto::mpp::IsAliveResponse>, tonic::Status> {
            Ok(tonic::Response::new(tidb_proto::mpp::IsAliveResponse {
                available: true,
                ..Default::default()
            }))
        }
        async fn get_lock_wait_info(
            &self,
            _: tonic::Request<tidb_proto::kvrpcpb::GetLockWaitInfoRequest>,
        ) -> Result<tonic::Response<tidb_proto::kvrpcpb::GetLockWaitInfoResponse>, tonic::Status>
        {
            Ok(tonic::Response::new(Default::default()))
        }
        async fn establish_mpp_connection(
            &self,
            _: tonic::Request<EstablishMppConnectionRequest>,
        ) -> Result<tonic::Response<tonic::codegen::BoxStream<MppDataPacket>>, tonic::Status>
        {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            match self.opening {
                Opening::RetryHeaders if attempt == 0 => {
                    return Err(tonic::Status::unavailable("opening unavailable"))
                }
                Opening::Unavailable => {
                    return Err(tonic::Status::unavailable("still unavailable"))
                }
                Opening::Canceled => return Err(tonic::Status::cancelled("remote canceled")),
                _ => {}
            }
            if self.stall_setup {
                std::future::pending::<()>().await;
            }
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let opening = self.opening;
            let fail_tail = self.fail_tail;
            let first_packet = self.first_packet.clone();
            tokio::spawn(async move {
                if matches!(opening, Opening::Empty) {
                    return;
                }
                if matches!(opening, Opening::StallFirstPacket) {
                    tx.closed().await;
                    return;
                }
                if matches!(opening, Opening::RetryFirstPacket) && attempt == 0 {
                    let _ = tx
                        .send(Err(tonic::Status::unavailable("first receive unavailable")))
                        .await;
                    return;
                }
                if matches!(opening, Opening::MemoryLimit) && attempt == 0 {
                    let _ = tx
                        .send(Ok(MppDataPacket {
                            error: Some(tidb_proto::mpp::Error {
                                code: 1,
                                msg: "Memory limit exceeded".into(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }))
                        .await;
                    return;
                }
                let _ = tx
                    .send(Ok(MppDataPacket {
                        data: first_packet.clone(),
                        ..Default::default()
                    }))
                    .await;
                if matches!(opening, Opening::TwoPackets) {
                    let _ = tx
                        .send(Ok(MppDataPacket {
                            data: first_packet,
                            ..Default::default()
                        }))
                        .await;
                }
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
            if matches!(self.opening, Opening::CancelFailure) {
                return Err(tonic::Status::unavailable("cancel unavailable"));
            }
            Ok(tonic::Response::new(Default::default()))
        }
    }
    struct Fixture {
        attempts: Arc<AtomicUsize>,
        address: String,
        runtime: Arc<tokio::runtime::Runtime>,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        task: Option<tokio::task::JoinHandle<()>>,
        cancels: Arc<AtomicUsize>,
        connections: Arc<AtomicUsize>,
        transport: Mutex<tidb_txnkv::rpc::TonicCoprocessorClient>,
    }
    impl Fixture {
        fn new() -> Self {
            Self::with_tail(true)
        }
        fn with_tail(fail_tail: bool) -> Self {
            Self::with_transport(fail_tail, false, false)
        }
        fn with_transport(fail_tail: bool, tls: bool, stall_setup: bool) -> Self {
            Self::with_packet(fail_tail, tls, stall_setup, b"first".to_vec())
        }
        fn with_packet(
            fail_tail: bool,
            tls: bool,
            stall_setup: bool,
            first_packet: Vec<u8>,
        ) -> Self {
            Self::with_opening(fail_tail, tls, stall_setup, first_packet, Opening::Normal)
        }
        fn with_opening(
            fail_tail: bool,
            tls: bool,
            stall_setup: bool,
            first_packet: Vec<u8>,
            opening: Opening,
        ) -> Self {
            if tls {
                // Configure the fixture through the same process TLS owner.
                ClusterSecurity::new(
                    concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/../tidb-pd-client/testdata/tls/ca.crt"
                    )
                    .to_owned(),
                    String::new(),
                    String::new(),
                    Vec::new(),
                )
                .client_tls_config()
                .unwrap();
            }
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
            let connections = Arc::new(AtomicUsize::new(0));
            let connections_for_server = Arc::clone(&connections);
            let security = if tls {
                ClusterSecurity::new(
                    concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/../tidb-pd-client/testdata/tls/ca.crt"
                    )
                    .to_owned(),
                    String::new(),
                    String::new(),
                    Vec::new(),
                )
            } else {
                ClusterSecurity::plaintext()
            };
            let transport =
                tidb_txnkv::rpc::TonicCoprocessorClient::with_security_and_connection_count(
                    Arc::new(security),
                    std::num::NonZeroUsize::new(1).unwrap(),
                )
                .unwrap();
            let attempts = Arc::new(AtomicUsize::new(0));
            let service = Service {
                opening,
                attempts: attempts.clone(),
                cancels: Arc::clone(&cancels),
                fail_tail,
                stall_setup,
                first_packet,
            };
            let (shutdown, rx) = tokio::sync::oneshot::channel();
            let task = runtime.spawn(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let mut server = tonic::transport::Server::builder();
                if tls {
                    server = server
                        .tls_config(tonic::transport::ServerTlsConfig::new().identity(
                            tonic::transport::Identity::from_pem(
                                include_bytes!("../../tidb-pd-client/testdata/tls/server.crt"),
                                include_bytes!("../../tidb-pd-client/testdata/tls/server.key"),
                            ),
                        ))
                        .unwrap();
                }
                server
                    .add_service(TikvServer::new(service))
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener).map(
                            move |socket| {
                                connections_for_server.fetch_add(1, Ordering::SeqCst);
                                socket
                            },
                        ),
                        async {
                            let _ = rx.await;
                        },
                    )
                    .await
                    .unwrap();
            });
            Self {
                attempts,
                address,
                runtime,
                shutdown: Some(shutdown),
                task: Some(task),
                cancels,
                connections,
                transport: Mutex::new(transport),
            }
        }
        fn open(&self) -> Result<MppQueryResponse, String> {
            self.open_with_memory(tidb_executor::StatementMemory::default())
        }
        fn open_with_memory(
            &self,
            memory: tidb_executor::StatementMemory,
        ) -> Result<MppQueryResponse, String> {
            self.open_with_cache(memory, None)
        }
        fn open_with_cache(
            &self,
            memory: tidb_executor::StatementMemory,
            compute_cache: Option<Arc<tikv_client::TiFlashComputeStoreCache>>,
        ) -> Result<MppQueryResponse, String> {
            self.runtime.block_on(async {
                let mut client = connect_mpp_client(
                    &self.address,
                    &memory,
                    &self.transport.lock().unwrap(),
                    compute_cache,
                )
                .await?;
                let response = tokio::time::timeout(
                    Duration::from_secs(2),
                    establish_mpp_response(
                        &mut client,
                        TaskMeta::default(),
                        TaskMeta::default(),
                        Arc::new(self.runtime.handle().clone()),
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
    fn mpp_failure_batch_socket_recovery_reuses_fleet_and_cancels_old_gather() {
        let fixture = Arc::new(Fixture::with_opening(
            false,
            false,
            false,
            b"new".to_vec(),
            Opening::MemoryLimit,
        ));
        let memory = tidb_executor::StatementMemory::default();
        let initial = fixture.open_with_memory(memory.clone()).unwrap();
        let next_fixture = fixture.clone();
        let next_memory = memory.clone();
        let mut response = MppRecoveryResponse::new(
            MppTaskResponses {
                streams: [Box::new(initial) as Box<dyn MppTaskResponse>].into(),
                node_count: 1,
            },
            true,
            memory.clone(),
            Box::new(move || {
                assert_eq!(next_fixture.cancels.load(Ordering::SeqCst), 1);
                let raw = next_fixture
                    .open_with_memory(next_memory.clone())
                    .map_err(QueryResponseError::Source)?;
                Ok(MppTaskResponses {
                    streams: [Box::new(raw) as Box<dyn MppTaskResponse>].into(),
                    node_count: 1,
                })
            }),
            Box::new(|nodes| {
                assert_eq!(nodes, 1);
                Ok(())
            }),
        );
        assert_eq!(response.next().unwrap().unwrap().data.as_ref(), b"new");
        assert!(response.next().unwrap().is_none());
        assert_eq!(fixture.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(fixture.connections.load(Ordering::SeqCst), 1);
        assert_eq!(memory.bytes_consumed(), 0);
    }

    #[test]
    fn mpp_failure_batch_socket_holder_transfers_packet_memory_once() {
        let fixture =
            Fixture::with_opening(false, false, false, b"raw".to_vec(), Opening::TwoPackets);
        let memory = tidb_executor::StatementMemory::default();
        let raw = fixture.open_with_memory(memory.clone()).unwrap();
        let mut response = MppRecoveryResponse::new(
            MppTaskResponses {
                streams: [Box::new(raw) as Box<dyn MppTaskResponse>].into(),
                node_count: 1,
            },
            true,
            memory.clone(),
            Box::new(|| panic!("no retry")),
            Box::new(|_| panic!("no recovery")),
        );
        assert_eq!(response.next().unwrap().unwrap().data.as_ref(), b"raw");
        assert_eq!(
            memory.bytes_consumed(),
            3,
            "only the remaining held payload is charged"
        );
        drop(response);
        assert_eq!(memory.bytes_consumed(), 0);
    }

    #[test]
    fn mpp_failure_batch_health_adapter_uses_existing_fleet() {
        let fixture = Fixture::with_tail(false);
        let mut response = fixture.open().unwrap();
        let client = MppHealthClient(fixture.transport.lock().unwrap().clone());
        assert!(tidb_txnkv::MppAliveClient::is_alive(
            &client,
            &fixture.address,
            Duration::from_secs(2)
        )
        .unwrap());
        assert_eq!(fixture.connections.load(Ordering::SeqCst), 1);
        response.close();
    }

    fn assert_opening_recovery(opening: Opening) {
        let fixture = Fixture::with_opening(false, false, false, b"first".to_vec(), opening);
        let mut response = fixture
            .open()
            .expect("opening failure must recover before delivery");
        assert_eq!(response.next().unwrap().unwrap().data.as_ref(), b"first");
        assert_eq!(fixture.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            fixture.cancels.load(Ordering::SeqCst),
            0,
            "retry canceled the live gather"
        );
        response.close();
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mpp_recovery_retries_headers_before_delivery() {
        assert_opening_recovery(Opening::RetryHeaders);
    }

    #[test]
    fn mpp_recovery_retries_first_receive_before_delivery() {
        assert_opening_recovery(Opening::RetryFirstPacket);
    }

    #[test]
    fn mpp_recovery_remote_canceled_is_terminal_without_retiring_the_fleet() {
        let fixture = Fixture::with_opening(false, false, false, Vec::new(), Opening::Canceled);
        let route = fixture
            .runtime
            .block_on(
                fixture
                    .transport
                    .lock()
                    .unwrap()
                    .store_rpc_channel(&fixture.address),
            )
            .unwrap();
        assert!(fixture.open().is_err());
        assert_eq!(
            fixture.attempts.load(Ordering::SeqCst),
            1,
            "Canceled must not retry"
        );
        assert!(
            !route.is_closed(),
            "MPP cancellation does not own fleet retirement"
        );
        let next = fixture
            .runtime
            .block_on(
                fixture
                    .transport
                    .lock()
                    .unwrap()
                    .store_rpc_channel(&fixture.address),
            )
            .unwrap();
        assert_eq!(route.version(), next.version());
        assert!(!next.is_closed());
    }

    #[test]
    fn mpp_recovery_first_eof_is_success_without_cancel_or_reopen() {
        let fixture = Fixture::with_opening(false, false, false, Vec::new(), Opening::Empty);
        let mut response = fixture.open().unwrap();
        response.close();
        assert_eq!(fixture.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture.cancels.load(Ordering::SeqCst),
            0,
            "first EOF already completed the task"
        );
    }

    #[test]
    fn mpp_recovery_kill_interrupts_first_receive_and_retry_wait() {
        for opening in [Opening::StallFirstPacket, Opening::Unavailable] {
            let fixture = Fixture::with_opening(false, false, false, Vec::new(), opening);
            let memory = tidb_executor::StatementMemory::default();
            let killer = memory.sql_killer().clone();
            let attempts = fixture.attempts.clone();
            let thread = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + Duration::from_secs(1);
                while attempts.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(1));
                }
                std::thread::sleep(Duration::from_millis(10));
                killer.send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
            });
            let cache = Arc::new(tikv_client::TiFlashComputeStoreCache::default());
            fixture
                .runtime
                .block_on(cache.get_or_load(|| async { Ok::<_, String>(Vec::new()) }))
                .unwrap();
            let result = fixture.open_with_cache(memory.clone(), Some(cache.clone()));
            thread.join().unwrap();
            let mut reloaded = false;
            fixture
                .runtime
                .block_on(cache.get_or_load(|| async {
                    reloaded = true;
                    Ok::<_, String>(Vec::new())
                }))
                .unwrap();
            assert!(
                reloaded,
                "interrupted establishment invalidates PD topology"
            );
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("KILL must terminate setup"),
            };
            assert!(matches!(mpp_open_error(&memory, error),
                PushdownScannerError::Backend(StorageError::Sql(error)) if error.code == 1317));
            assert_eq!(fixture.attempts.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
            assert_eq!(memory.bytes_consumed(), 0);
        }
    }

    #[test]
    fn shared_mpp_fleet_preserves_round_robin_and_stale_generation_guards() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let mut transport = tidb_txnkv::rpc::TonicCoprocessorClient::with_connection_count(
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .unwrap();
        runtime.block_on(async {
            let address = "127.0.0.1:65535";
            let mut routes = Vec::new();
            for _ in 0..6 {
                routes.push(transport.store_rpc_channel(address).await.unwrap());
            }
            assert_eq!(
                routes
                    .iter()
                    .map(|route| route.version())
                    .collect::<Vec<_>>(),
                vec![1, 2, 3, 1, 2, 3]
            );
            let remote =
                routes[0].rpc_error(tonic::Status::cancelled("remote"), Duration::from_secs(1));
            assert_eq!(
                remote.grpc_code(),
                Some(tidb_txnkv::rpc::DirectUnaryGrpcCode::Canceled)
            );
            let local = routes[0].rpc_error(
                tonic::Status::from_error(Box::new(tonic::TimeoutExpired(()))),
                Duration::from_secs(1),
            );
            assert!(matches!(
                local,
                tidb_txnkv::rpc::DirectUnaryClientError::Timeout { .. }
            ));
            assert!(!local.requires_generation_close());
            transport.close_address_version(address, 1).unwrap();
            assert!(routes[0].is_closed());
            assert!(!routes[1].is_closed());
            let replacement = transport.store_rpc_channel(address).await.unwrap();
            assert_eq!(replacement.version(), 4);
            transport.close_address_version(address, 1).unwrap();
            assert!(
                !replacement.is_closed(),
                "a stale failure retired a newer MPP channel"
            );
            transport.close().unwrap();
            assert!(replacement.is_closed());
            assert!(routes.iter().all(|route| route.is_closed()));
        });
    }

    #[test]
    fn shared_mpp_sessions_reuse_the_process_connection() {
        let fixture = Fixture::with_tail(false);
        for _ in 0..2 {
            let mut response = fixture.open().unwrap();
            response.next().unwrap().unwrap();
            response.close();
        }
        assert_eq!(fixture.connections.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shared_mpp_process_close_interrupts_a_stalled_receive() {
        let fixture = Fixture::new();
        let mut response = fixture.open().unwrap();
        response.next().unwrap().unwrap();
        fixture.transport.lock().unwrap().close().unwrap();
        let started = std::time::Instant::now();
        assert!(response.next().is_err());
        assert!(started.elapsed() < Duration::from_millis(250));
    }

    #[test]
    fn shared_mpp_closed_process_rejects_new_streams() {
        let fixture = Fixture::with_tail(false);
        fixture.transport.lock().unwrap().close().unwrap();
        assert!(
            fixture.open().is_err(),
            "an MPP request escaped the closed process owner"
        );
    }

    #[test]
    fn shared_mpp_address_retirement_ends_the_old_stream() {
        let fixture = Fixture::new();
        let mut response = fixture.open().unwrap();
        response.next().unwrap().unwrap();
        fixture
            .transport
            .lock()
            .unwrap()
            .close_address(&fixture.address)
            .unwrap();
        let started = std::time::Instant::now();
        assert!(response.next().is_err());
        assert!(started.elapsed() < Duration::from_millis(250));
        let mut replacement = fixture.open().unwrap();
        assert_eq!(replacement.next().unwrap().unwrap().data.as_ref(), b"first");
        replacement.close();
    }

    #[test]
    fn shared_store_credentials_reach_the_ordinary_rpc_fleet() {
        let fixture = Fixture::with_transport(false, true, false);
        let result = fixture
            .transport
            .lock()
            .unwrap()
            .get_lock_wait_info(&fixture.address, Duration::from_secs(1));
        assert!(
            result.is_ok(),
            "configured store TLS did not reach the ordinary RPC: {result:?}"
        );
    }

    #[test]
    fn mpp_receive_deadline_cancels_a_stalled_packet_without_a_stream_lifetime_limit() {
        let fixture = Fixture::new();
        let mut response = fixture.open().unwrap();
        assert_eq!(response.next().unwrap().unwrap().data.as_ref(), b"first");
        response.receive_timeout = Duration::from_millis(20);
        let started = std::time::Instant::now();
        assert!(
            matches!(response.next(), Err(QueryResponseError::Source(message)) if message.contains("stream receive: timed out"))
        );
        assert!(started.elapsed() < Duration::from_millis(250));
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mpp_transport_accepts_packets_larger_than_tonic_default() {
        let packet = vec![b'x'; 5 * 1024 * 1024];
        let fixture = Fixture::with_packet(false, false, false, packet.clone());
        let mut response = fixture.open().unwrap();
        let received = response.next().unwrap().unwrap();
        assert_eq!(received.data.as_ref(), packet.as_slice());
        response.close();
    }

    #[test]
    fn mpp_transport_uses_cluster_tls_for_a_real_rpc() {
        let fixture = Fixture::with_transport(false, true, false);
        fixture.runtime.block_on(async {
            let mut client = connect_mpp_client(
                &fixture.address,
                &tidb_executor::StatementMemory::default(),
                &fixture.transport.lock().unwrap(),
                None,
            )
            .await
            .unwrap();
            client
                .client
                .cancel_mpp_task(tidb_proto::mpp::CancelTaskRequest::default())
                .await
                .unwrap();
        });
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mpp_setup_deadline_drops_the_stalled_operation() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (owner, retired) = tokio::sync::oneshot::channel::<()>();
            let operation = async move {
                let _owner = owner;
                std::future::pending::<Result<(), tonic::Status>>().await
            };
            let result = tokio::time::timeout(
                Duration::from_millis(250),
                mpp_setup(
                    Some(&tidb_executor::StatementMemory::default()),
                    Some(Duration::from_millis(20)),
                    "dispatch",
                    None,
                    operation,
                ),
            )
            .await;
            let error = result
                .expect("the RPC setup deadline must run")
                .unwrap_err();
            assert!(error.to_string().contains("timed out"), "{error}");
            assert!(
                retired.await.is_err(),
                "the expired operation retained its owner"
            );
        });
    }

    #[test]
    fn mpp_kill_interrupts_establishment_and_cancels_registered_gather() {
        let fixture = Fixture::with_transport(false, false, true);
        let memory = tidb_executor::StatementMemory::default();
        let killer = Arc::clone(memory.sql_killer());
        let thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            killer.send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
        });
        let result = fixture.open_with_memory(memory.clone());
        thread.join().unwrap();
        let error = result.err().expect("KILL must abort setup");
        assert!(!error.contains("opening drained"), "{error}");
        assert!(matches!(mpp_open_error(&memory, error),
            PushdownScannerError::Backend(StorageError::Sql(error)) if error.code == 1317
        ));
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
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
        assert_eq!(
            fixture.attempts.load(Ordering::SeqCst),
            1,
            "late failure replayed delivered rows"
        );
    }

    #[test]
    fn natural_completion_releases_memory_without_cancel() {
        let fixture = Fixture::with_tail(false);
        let memory = tidb_executor::StatementMemory::default();
        let mut response = fixture.open_with_memory(memory.clone()).unwrap();
        assert!(
            memory.bytes_consumed() > 0,
            "setup's retained first packet is unaccounted"
        );
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
        let error = match fixture.open_with_memory(memory.clone()) {
            Err(error) => error,
            Ok(_) => panic!("the first packet must be accounted before setup returns"),
        };
        assert!(matches!(mpp_open_error(&memory, error),
            PushdownScannerError::Backend(StorageError::Sql(error)) if error.code == 8175));
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

#[cfg(test)]
mod compute_topology_batch_tests {
    use super::*;
    use crate::tiflash_compute::DispatchPolicy;

    #[test]
    fn compute_topology_batch_region_grouping() {
        let regions: Vec<_> = (1..=8)
            .map(|region_id| tidb_proto::coprocessor::RegionInfo {
                region_id,
                ranges: vec![tidb_proto::coprocessor::KeyRange {
                    start: vec![region_id as u8],
                    end: vec![region_id as u8 + 1],
                }],
                ..Default::default()
            })
            .collect();
        let stores = vec!["a:3930".into(), "b:3930".into(), "c:3930".into()];
        let rr =
            compute_region_tasks(regions.clone(), &stores, DispatchPolicy::RoundRobin, 1).unwrap();
        assert_eq!(
            rr.iter()
                .map(|(a, rs)| (
                    a.as_str(),
                    rs.iter().map(|r| r.region_id).collect::<Vec<_>>()
                ))
                .collect::<Vec<_>>(),
            vec![
                ("b:3930", vec![1, 4, 7]),
                ("c:3930", vec![2, 5, 8]),
                ("a:3930", vec![3, 6])
            ]
        );
        let hash =
            compute_region_tasks(regions.clone(), &stores, DispatchPolicy::ConsistentHash, 0)
                .unwrap();
        let reordered = compute_region_tasks(
            regions.clone(),
            &stores.iter().rev().cloned().collect::<Vec<_>>(),
            DispatchPolicy::ConsistentHash,
            0,
        )
        .unwrap();
        assert_eq!(hash, reordered);
        let mut flattened = hash.into_iter().flat_map(|(_, rs)| rs).collect::<Vec<_>>();
        flattened.sort_by_key(|r| r.region_id);
        assert_eq!(flattened, regions);
        assert!(
            compute_region_tasks(regions.clone(), &[], DispatchPolicy::ConsistentHash, 0).is_err()
        );
        assert!(compute_region_tasks(regions, &stores, DispatchPolicy::Invalid, 0).is_err());
    }

    fn packet(value: u8) -> QueryResultSubset {
        QueryResultSubset {
            data: vec![value].into(),
            runtime: None,
        }
    }
    fn gather(
        rows: Vec<Result<QueryResultSubset, QueryResponseError>>,
        closed: Arc<std::sync::atomic::AtomicUsize>,
    ) -> MppTaskResponses {
        MppTaskResponses {
            streams: [Box::new(Packets {
                rows: rows.into(),
                closed,
            }) as Box<dyn MppTaskResponse>]
            .into(),
            node_count: 2,
        }
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
        let memory = tidb_executor::StatementMemory::default();
        let mut response = MppRecoveryResponse::new(
            gather(vec![Ok(packet(99)), Err(memory_error())], closed),
            true,
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
            let mut response = MppRecoveryResponse::new(
                gather(
                    vec![Ok(packet(1)), Ok(packet(2)), Err(memory_error())],
                    Arc::default(),
                ),
                enabled,
                tidb_executor::StatementMemory::default(),
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
        let mut response = MppRecoveryResponse::new(
            gather(vec![Err(memory_error())], Arc::default()),
            true,
            tidb_executor::StatementMemory::default(),
            Box::new(|| Ok(gather(vec![Err(memory_error())], Arc::default()))),
            Box::new(move |_| {
                recoveries.fetch_add(1, SeqCst);
                Ok(())
            }),
        );
        assert_eq!(response.next().unwrap_err(), memory_error());
        assert_eq!(calls.load(SeqCst), 3);
        for recovery_fails in [true, false] {
            let memory = tidb_executor::StatementMemory::default();
            let mut response = MppRecoveryResponse::new(
                gather(vec![Ok(packet(5)), Err(memory_error())], Arc::default()),
                true,
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
            let mut response = MppRecoveryResponse::new(
                gather(vec![Err(error.clone())], Arc::default()),
                true,
                tidb_executor::StatementMemory::default(),
                Box::new(|| panic!("terminal")),
                Box::new(|_| panic!("terminal")),
            );
            assert_eq!(response.next().unwrap_err(), error);
        }
        for count in 0..=2 {
            let memory = tidb_executor::StatementMemory::default();
            let mut response = MppRecoveryResponse::new(
                gather((0..count).map(|i| Ok(packet(i))).collect(), Arc::default()),
                true,
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
        let mut response = MppRecoveryResponse::new(
            gather(vec![], Arc::default()),
            true,
            tidb_executor::StatementMemory::default(),
            Box::new(move || {
                let _borrow = &capability;
                panic!("no retry")
            }),
            Box::new(|_| panic!("no failure")),
        );
        response.close();
        assert!(observer.upgrade().is_none());
        response.close();
    }

    struct Packets {
        rows: std::collections::VecDeque<Result<QueryResultSubset, QueryResponseError>>,
        closed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl MppTaskResponse for Packets {
        fn release_packet_memory(&mut self) {}
    }
    impl QueryResponse for Packets {
        fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
            self.rows.pop_front().transpose()
        }
        fn close(&mut self) {
            self.closed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    #[test]
    fn compute_topology_batch_fanout_error_closes_all_tasks() {
        let closed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut streams = MppTaskResponses {
            streams: [
                Box::new(Packets {
                    rows: [Ok(packet(1))].into(),
                    closed: closed.clone(),
                }) as Box<dyn MppTaskResponse>,
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
        streams.close();
        assert_eq!(closed.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}
