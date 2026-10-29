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

//! Go `copr.MPPClient` (`pkg/store/copr/mpp.go`): the node's `kv.MPPClient`
//! over its `KVStore` -- where a fragment's tasks run, their dispatch, and the
//! coordinator's stream from a root task. The coordinator that drives it
//! (`local_mpp_coordinator.go`) is `tidb_executor::mpp_coordinator`.
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

use std::sync::Arc;

use prost::Message as _;
use tidb_proto::mpp::{
    DispatchTaskRequest, EstablishMppConnectionRequest, MppDataPacket, TaskMeta,
};
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::retry::{RegionBackoffBudget, RegionBackoffKind};
use tidb_txnkv::{
    MppBuildTasksRequest, MppContext, MppError, MppStoreTask, MppStreamResponse,
    QueryResponseError, QueryResultSubset,
};

/// Go `mpp_probe.go`'s `detectMPPStore`: `IsAlive` through the store's
/// TiKV client.
struct MppHealthClient<S: tikv_client::PdClient>(TikvStore<S>);
impl<S: tikv_client::PdClient> crate::mpp_probe::MppAliveClient for MppHealthClient<S> {
    fn is_alive(&self, address: &str, timeout: std::time::Duration) -> Result<bool, String> {
        // Go's background ctx: no statement can stop a health probe.
        self.0.block_on(probe_mpp_store(
            address,
            &tidb_txnkv::BackgroundMppContext,
            &self.0,
            timeout,
        ))
    }
}
async fn probe_mpp_store<S: tikv_client::PdClient>(
    address: &str,
    memory: &dyn MppContext,
    store: &TikvStore<S>,
    timeout: std::time::Duration,
) -> Result<bool, String> {
    mpp_setup(
        Some(memory),
        Some(timeout),
        "detect compute node",
        send_mpp::<_, _, tidb_proto::mpp::IsAliveResponse>(
            store,
            address,
            // Go `detectMPPStore`: `StoreTp: tikvrpc.TiFlash`.
            tikv_client::tikv::EndpointType::TiFlash,
            tidb_proto::mpp::IsAliveRequest::default(),
            timeout,
        ),
    )
    .await
    .map(|response| response.available)
    .map_err(|error| error.to_string())
}

/// Go `GetTiKVClient().SendRequest(ctx, addr, req, timeout)` for one MPP
/// command whose `StoreTp` is `store_type`: the store's TiKV client, outside
/// region routing.
async fn send_mpp<S, Req, Resp>(
    store: &TikvStore<S>,
    address: &str,
    store_type: tikv_client::tikv::EndpointType,
    mut request: Req,
    timeout: std::time::Duration,
) -> Result<Resp, tikv_client::Error>
where
    S: tikv_client::PdClient,
    Req: tikv_client::tikv::Request + Send + Sync,
    Resp: 'static,
{
    store
        .store()
        .send_request_with_store_type(address, &mut request, store_type, timeout)
        .await?
        .downcast::<Resp>()
        .map(|response| *response)
        .map_err(|_| tikv_client::Error::StringError("unexpected MPP response type".to_owned()))
}

/// Go `copr.MPPClient` over the node's `KVStore` -- its region cache, PD
/// client and TiKV client.
pub struct TiFlashMppClient<S: tikv_client::PdClient = tikv_client::PdRpcClient> {
    store: TikvStore<S>,
    /// Go `is.SchemaMetaVersion()`, read at dispatch time from the node's
    /// catalog watch: TiFlash resolves the request's table in the schema
    /// generation the dispatch names, so a stale or zero version makes even a
    /// synced table "not exist". Go's coordinator passes the statement's
    /// version; this tier's session catalog carries a mutation counter rather
    /// than the cluster's version, so the node's watch answers instead.
    schema_version: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl<S: tikv_client::PdClient> Clone for TiFlashMppClient<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            schema_version: Arc::clone(&self.schema_version),
        }
    }
}

impl<S: tikv_client::PdClient> std::fmt::Debug for TiFlashMppClient<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TiFlashMppClient")
    }
}

impl<S: tikv_client::PdClient> TiFlashMppClient<S> {
    /// Builds the client over the node's store and catalog watch.
    pub fn new(
        store: TikvStore<S>,
        schema_version: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            store,
            schema_version: Arc::new(schema_version),
        }
    }

    /// Go `buildBatchCopTasksForNonPartitionedTable` on classic TiFlash:
    /// each region on one of its own TiFlash replicas, balanced across the
    /// alive stores that hold them.
    fn classic_tasks(
        &self,
        ctx: &Arc<dyn MppContext>,
        ranges: &[(tidb_txnkv::Key, tidb_txnkv::Key)],
        replica_read: tidb_tiflash::ReplicaRead,
        kv_context: &tidb_txnkv::StatementKvContext,
        append_warning: &mut dyn FnMut(String),
    ) -> Result<Vec<MppStoreTask>, String> {
        let (tasks, warnings) =
            self.store
                .block_on(crate::cop_store::with_statement_kv_context(
                    kv_context,
                    crate::batch_cop_tasks::build_batch_cop_tasks_core(
                        &self.store,
                        ranges,
                        ctx,
                        replica_read,
                        kv_context,
                    ),
                ))?;
        for warning in warnings {
            append_warning(warning);
        }
        Ok(tasks
            .into_iter()
            .map(|task| MppStoreTask {
                address: task.store_addr,
                regions: task
                    .region_infos
                    .into_iter()
                    .map(|region| region.info)
                    .collect(),
            })
            .collect())
    }

    /// Go `RegionCache.InvalidateTiFlashComputeStores`.
    fn invalidate_compute_stores(&self) {
        self.store
            .store()
            .pd_client()
            .invalidate_tiflash_compute_stores();
    }

    /// The store a task at `address` runs on: Go `getEndPointType(kv.TiFlash)`
    /// and whether a send failure invalidates the compute-store cache.
    fn target(
        &self,
        address: &str,
        rpc_interceptor: &tidb_txnkv::RpcInterceptorOption,
    ) -> MppTarget<S> {
        let config = tidb_config::config_tree::config::get_global_config();
        MppTarget {
            store: self.store.clone(),
            address: address.to_owned(),
            store_type: if config.disaggregated_tiflash {
                tikv_client::tikv::EndpointType::TiFlashCompute
            } else {
                tikv_client::tikv::EndpointType::TiFlash
            },
            compute_cache: config.disaggregated_tiflash && !config.use_auto_scaler,
            rpc_interceptor: rpc_interceptor.clone(),
        }
    }

    fn compute_addresses(
        &self,
        auto_scaler: bool,
        memory: &Arc<dyn MppContext>,
    ) -> Result<Vec<String>, tidb_tiflashcompute::TopologyError> {
        let mut budget = RegionBackoffBudget::new(std::time::Duration::from_secs(20));
        loop {
            mpp_memory_error(memory.as_ref()).map_err(|error| error.to_string())?;
            let stores = if auto_scaler {
                tidb_tiflashcompute::global_topo_fetcher()
                    .ok_or_else(|| {
                        "TiFlash compute topology fetcher is not initialized".to_owned()
                    })?
                    .fetch_and_get_topo()?
            } else {
                // Go `RegionCache.GetTiFlashComputeStores`.
                let pd = self.store.store().pd_client();
                self.store
                    .block_on(async move { pd.get_tiflash_compute_stores().await })
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .map(|store| store.address)
                    .collect()
            };
            let was_empty = stores.is_empty();
            let alive = self.store.block_on(async {
                let mut probes = tokio::task::JoinSet::new();
                for address in stores {
                    let store = self.store.clone();
                    let memory = memory.clone();
                    probes.spawn(async move {
                        let prober = crate::mpp_probe::global_mpp_failed_store_prober();
                        // Go deprecated tidb_mpp_store_fail_ttl is always zero.
                        if !prober.is_recovery(&address, std::time::Duration::ZERO) {
                            return None;
                        }
                        match probe_mpp_store(
                            &address,
                            memory.as_ref(),
                            &store,
                            crate::mpp_probe::DETECT_TIMEOUT_LIMIT,
                        )
                        .await
                        {
                            Ok(true) => Some(address),
                            _ => {
                                // Statement cancellation is not evidence that a store failed.
                                if memory.check().is_ok() {
                                    prober.add(address, Arc::new(MppHealthClient(store)));
                                }
                                None
                            }
                        }
                    });
                }
                let mut alive = Vec::new();
                while let Some(result) = probes.join_next().await {
                    mpp_memory_error(memory.as_ref()).map_err(|error| error.to_string())?;
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
                self.invalidate_compute_stores();
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
            let waited = self.store.block_on(mpp_setup(
                Some(memory.as_ref()),
                None,
                "compute topology retry",
                async {
                    tokio::time::sleep(delay).await;
                    Ok(())
                },
            ));
            budget.finish_wait(waited.is_ok());
            waited.map_err(|error| error.to_string())?;
        }
    }

}

impl<S: tikv_client::PdClient + 'static> tidb_txnkv::MppClient for TiFlashMppClient<S> {
    fn construct_mpp_tasks(
        &self,
        ctx: &Arc<dyn MppContext>,
        request: &MppBuildTasksRequest,
        kv_context: &tidb_txnkv::StatementKvContext,
        dispatch_policy: tidb_tiflashcompute::DispatchPolicy,
        replica_read: tidb_tiflash::ReplicaRead,
        append_warning: &mut dyn FnMut(String),
    ) -> Result<Vec<MppStoreTask>, MppError> {
        let refuse = |reason: String| MppError::Other(format!("tiflash mpp: {reason}"));
        let ranges: Vec<(tidb_txnkv::Key, tidb_txnkv::Key)> = request
            .key_ranges
            .iter()
            .flatten()
            .map(|range| (range.start_key.clone(), range.end_key.clone()))
            .collect();
        let config = tidb_config::config_tree::config::get_global_config();
        if !config.disaggregated_tiflash {
            return self
                .classic_tasks(ctx, &ranges, replica_read, kv_context, append_warning)
                .map_err(|message| {
                    MppError::after_stop(ctx.as_ref(), format!("tiflash mpp: {message}"))
                });
        }
        let regions = locate_mpp_region_infos(&self.store, &ranges, ctx.as_ref(), kv_context)
            .map_err(refuse)?;
        if regions.is_empty() {
            return Err(refuse("no region covers the table's record range".to_owned()));
        }
        let stores = self
            .compute_addresses(config.use_auto_scaler, ctx)
            .map_err(|error| match ctx.check() {
                Err(stop) => MppError::Sql(stop),
                Ok(()) => MppError::Sql(tidb_terror::MysqlError::sql_error(
                    error.code,
                    error.message,
                )),
            })?;
        compute_region_tasks(
            regions,
            &stores,
            dispatch_policy,
            tidb_fastrand::uint64_n(stores.len() as u64) as usize,
        )
        .map_err(refuse)
    }

    fn dispatch_mpp_task(
        &self,
        ctx: &Arc<dyn MppContext>,
        task: MppStoreTask,
        meta: TaskMeta,
        encoded_plan: Vec<u8>,
        kv_context: &tidb_txnkv::StatementKvContext,
        rpc_interceptor: &tidb_txnkv::RpcInterceptorOption,
    ) -> Result<(), MppError> {
        let target = self.target(&task.address, rpc_interceptor);
        let dispatch_request = encode_dispatch_request(DispatchTaskRequest {
            meta: Some(meta),
            encoded_plan,
            regions: task.regions,
            schema_ver: (self.schema_version)(),
            ..Default::default()
        });
        // Go dispatches under the statement ctx, whose `RUDetails`
        // client-go's resource-control interceptor charges.
        self.store
            .block_on(crate::cop_store::with_statement_kv_context(
                kv_context,
                under_rpc_interceptor(&target.rpc_interceptor, async {
                    let dispatch_response = mpp_setup(
                        Some(ctx.as_ref()),
                        Some(tikv_client::tikv::READ_TIMEOUT_MEDIUM),
                        "dispatch",
                        send_mpp::<_, _, tidb_proto::mpp::DispatchTaskResponse>(
                            &target.store,
                            &target.address,
                            target.store_type,
                            dispatch_request,
                            tikv_client::tikv::READ_TIMEOUT_MEDIUM,
                        ),
                    )
                    .await
                    .map_err(|error| {
                        target.invalidate_compute_cache();
                        error.to_string()
                    })?;
                    if let Some(error) = &dispatch_response.error {
                        return Err(format!(
                            "tiflash mpp: dispatch refused: {} ({})",
                            error.msg, error.code
                        ));
                    }
                    // Go `MPPClient.DispatchMPPTask`: retry regions only
                    // invalidate the region cache; they are NOT a dispatch
                    // failure. The task itself has already registered and will
                    // serve its regions through the learner read.
                    invalidate_mpp_retry_regions(&target.store, &dispatch_response.retry_regions)
                        .await;
                    Ok(())
                }),
            ))
            .map_err(|message| MppError::after_stop(ctx.as_ref(), message))
    }

    fn establish_mpp_conns(
        &self,
        ctx: Arc<dyn MppContext>,
        address: &str,
        sender: TaskMeta,
        receiver: TaskMeta,
        kv_context: &tidb_txnkv::StatementKvContext,
        rpc_interceptor: &tidb_txnkv::RpcInterceptorOption,
    ) -> Result<Box<dyn MppStreamResponse>, MppError> {
        let target = self.target(address, rpc_interceptor);
        self.store
            .block_on(crate::cop_store::with_statement_kv_context(
                kv_context,
                establish_mpp_response(&target, sender, receiver, Arc::clone(&ctx)),
            ))
            .map(|response| Box::new(response) as Box<dyn MppStreamResponse>)
            .map_err(|message| MppError::after_stop(ctx.as_ref(), message))
    }
}

// Go buildBatchCopTasksConsistentHash groups clipped regions in encounter order.
fn compute_region_tasks(
    regions: Vec<tidb_proto::coprocessor::RegionInfo>,
    stores: &[String],
    policy: tidb_tiflashcompute::DispatchPolicy,
    start: usize,
) -> Result<Vec<MppStoreTask>, String> {
    use tidb_tiflashcompute::DispatchPolicy;
    if stores.is_empty() {
        return Err("tiflash_compute node is unavailable".into());
    }
    let mut tasks: Vec<MppStoreTask> = Vec::new();
    for (index, region) in regions.into_iter().enumerate() {
        let address = match policy {
            DispatchPolicy::RoundRobin => {
                stores[(start % stores.len() + index % stores.len()) % stores.len()].clone()
            }
            DispatchPolicy::ConsistentHash => {
                let mut max_hash = 0;
                let mut selected = String::new();
                for store in stores {
                    let hash = tidb_murmur3::sum32(
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
        if let Some(task) = tasks.iter_mut().find(|task| task.address == address) {
            task.regions.push(region);
        } else {
            tasks.push(MppStoreTask {
                address,
                regions: vec![region],
            });
        }
    }
    Ok(tasks)
}

fn mpp_region_infos(
    ranges: &[(tidb_txnkv::Key, tidb_txnkv::Key)],
    regions: &[tikv_client::RegionWithLeader],
) -> Vec<tidb_proto::coprocessor::RegionInfo> {
    regions
        .iter()
        .filter_map(|region| {
            let ranges: Vec<_> = ranges
                .iter()
                .filter_map(|(start, end)| {
                    let region_start = region.region.start_key.as_slice();
                    let region_end = &region.region.end_key;
                    let start = start.as_slice().max(region_start).to_vec();
                    let end = match (end.is_empty(), region_end.is_empty()) {
                        (true, _) => region_end.clone(),
                        (_, true) => end.as_slice().to_vec(),
                        _ => end.as_slice().min(region_end.as_slice()).to_vec(),
                    };
                    (end.is_empty() || start < end)
                        .then_some(tidb_proto::coprocessor::KeyRange { start, end })
                })
                .collect();
            let epoch = region.region.region_epoch.clone().unwrap_or_default();
            (!ranges.is_empty()).then_some(tidb_proto::coprocessor::RegionInfo {
                region_id: region.region.id,
                region_epoch: Some(tidb_proto::coprocessor::RegionEpoch {
                    conf_ver: epoch.conf_ver,
                    version: epoch.version,
                }),
                ranges,
            })
        })
        .collect()
}

/// The regions covering `ranges`, from the store's region cache (Go
/// `buildBatchCopTasks`' `SplitKeyRangesByLocations`), located under the
/// task building's ctx `kv_context`.
fn locate_mpp_region_infos<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    ranges: &[(tidb_txnkv::Key, tidb_txnkv::Key)],
    memory: &dyn MppContext,
    kv_context: &tidb_txnkv::StatementKvContext,
) -> Result<Vec<tidb_proto::coprocessor::RegionInfo>, String> {
    use crate::cop_store::NativeCopStore;
    if ranges.is_empty() {
        return Err("the scan carries no key ranges".to_owned());
    }
    mpp_memory_error(memory).map_err(|error| error.to_string())?;
    let keys = ranges
        .iter()
        .map(|(start, end)| (start.as_slice().to_vec(), end.as_slice().to_vec()))
        .collect::<Vec<_>>();
    let regions = store
        .locate(&keys, kv_context)
        .map_err(|error| error.to_string())?;
    Ok(mpp_region_infos(ranges, &regions))
}

/// Go `SplitKeyRangesByLocations` for a batch task: every region covering
/// `ranges`, with the ranges inside it, located under the task building's
/// ctx `kv_context`.
pub(crate) fn locate_mpp_regions<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    ranges: &[(tidb_txnkv::Key, tidb_txnkv::Key)],
    memory: &dyn MppContext,
    kv_context: &tidb_txnkv::StatementKvContext,
) -> Result<Vec<crate::batch_cop_tasks::LocatedRegion>, String> {
    use crate::cop_store::NativeCopStore;
    if ranges.is_empty() {
        return Err("the scan carries no key ranges".to_owned());
    }
    mpp_memory_error(memory).map_err(|error| error.to_string())?;
    let keys = ranges
        .iter()
        .map(|(start, end)| (start.as_slice().to_vec(), end.as_slice().to_vec()))
        .collect::<Vec<_>>();
    let regions = store
        .locate(&keys, kv_context)
        .map_err(|error| error.to_string())?;
    Ok(regions
        .into_iter()
        .filter_map(|region| {
            let info = mpp_region_infos(ranges, std::slice::from_ref(&region))
                .into_iter()
                .next()?;
            Some(crate::batch_cop_tasks::LocatedRegion { region, info })
        })
        .collect())
}

/// Go `filterAliveStoresHelper`: the stores the failed-store prober has not
/// written off whose `IsAlive` answers, probed concurrently; a store that
/// does not answer goes to the prober.
pub(crate) async fn filter_alive_stores<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    stores: &[tidb_proto::metapb::Store],
    memory: &Arc<dyn MppContext>,
) -> Vec<tidb_proto::metapb::Store> {
    let mut probes = tokio::task::JoinSet::new();
    for (index, candidate) in stores.iter().enumerate() {
        let address = candidate.address.clone();
        let store = store.clone();
        let memory = memory.clone();
        probes.spawn(async move {
            let prober = crate::mpp_probe::global_mpp_failed_store_prober();
            // Go deprecated tidb_mpp_store_fail_ttl is always zero.
            if !prober.is_recovery(&address, std::time::Duration::ZERO) {
                return None;
            }
            match probe_mpp_store(&address, memory.as_ref(), &store, crate::mpp_probe::DETECT_TIMEOUT_LIMIT).await
            {
                Ok(true) => Some(index),
                _ => {
                    if memory.check().is_ok() {
                        prober.add(address, Arc::new(MppHealthClient(store)));
                    }
                    None
                }
            }
        });
    }
    let mut alive = Vec::new();
    while let Some(result) = probes.join_next().await {
        if let Ok(Some(index)) = result {
            alive.push(index);
        }
    }
    alive.sort_unstable();
    alive
        .into_iter()
        .map(|index| stores[index].clone())
        .collect()
}

/// One backoff sleep while building batch tasks, interrupted by the
/// statement's killer.
pub(crate) async fn mpp_retry_wait(
    memory: &dyn MppContext,
    delay: std::time::Duration,
) -> Result<(), String> {
    mpp_setup(Some(memory), None, "batch task retry", async move {
        tokio::time::sleep(delay).await;
        Ok(())
    })
    .await
    .map_err(|error| error.to_string())
}

/// Go `MPPClient.DispatchMPPTask`'s retry regions: invalidated in the store's
/// region cache.
async fn invalidate_mpp_retry_regions<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    retries: &[tidb_proto::metapb::Region],
) {
    let pd = store.store().pd_client();
    for region in retries {
        if let Some(epoch) = &region.region_epoch {
            pd.invalidate_region_cache(tikv_client::tikv::new_region_ver_id(
                region.id,
                epoch.conf_ver,
                epoch.version,
            ))
            .await;
        }
    }
}

// Query RPCs retain the canonical killer; cleanup RPCs use Go's background
// context but still honor the deadline. Keep the transport error until the
// retry decision is made. Statement cancellation has no remote RPC identity.
#[derive(Debug)]
struct MppSetupError {
    message: String,
    transport: Option<tikv_client::Error>,
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

    /// client-go retries an MPP setup RPC that failed to connect or timed
    /// out, never a cancelled one.
    fn retryable(&self) -> bool {
        // `errors.Cause`: the connection annotation is not the kind.
        match self.transport.as_ref().map(tikv_client::Error::cause) {
            Some(tikv_client::Error::Grpc(_)) => true,
            Some(tikv_client::Error::GrpcAPI(status)) => matches!(
                status.code(),
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
            ),
            _ => false,
        }
    }
}

async fn mpp_setup<T>(
    memory: Option<&dyn MppContext>,
    timeout: Option<std::time::Duration>,
    stage: &str,
    future: impl std::future::Future<Output = Result<T, tikv_client::Error>>,
) -> Result<T, MppSetupError> {
    let check = || {
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
                transport: Some(tikv_client::Error::GrpcAPI(tonic::Status::deadline_exceeded(
                    format!("{stage} timed out"),
                ))),
            }),
            result = &mut future => {
                check()?;
                return result.map_err(|error| MppSetupError {
                    message: format!("tiflash mpp: {stage}: {error}"),
                    transport: Some(error),
                });
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => { check()?; }
        }
    }
}

// Go copr.TiFlashReadTimeoutUltraLong bounds each receive, not stream lifetime.
const MPP_RECEIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);

/// One MPP task's store: where it was dispatched and how it is cleaned up.
struct MppTarget<S: tikv_client::PdClient> {
    store: TikvStore<S>,
    address: String,
    /// Go `getEndPointType(kv.TiFlash)`: TiFlash compute under
    /// disaggregated TiFlash, TiFlash otherwise.
    store_type: tikv_client::tikv::EndpointType,
    /// Whether the task runs on a TiFlash compute node from the region
    /// cache's compute-store list, which a send failure invalidates.
    compute_cache: bool,
    rpc_interceptor: tidb_txnkv::RpcInterceptorOption,
}

impl<S: tikv_client::PdClient> Clone for MppTarget<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            address: self.address.clone(),
            store_type: self.store_type,
            compute_cache: self.compute_cache,
            rpc_interceptor: self.rpc_interceptor.clone(),
        }
    }
}

impl<S: tikv_client::PdClient> MppTarget<S> {
    fn invalidate_compute_cache(&self) {
        if self.compute_cache {
            self.store
                .store()
                .pd_client()
                .invalidate_tiflash_compute_stores();
        }
    }
}

/// Go `distsql.WithSQLKvExecCounterInterceptor(ctx, ...)` on the MPP
/// coordinator's ctx: the MPP RPCs `future` sends run the statement's
/// interceptor (client-go's `interceptedClient` reads it from the ctx).
async fn under_rpc_interceptor<F: std::future::Future>(
    interceptor: &tidb_txnkv::RpcInterceptorOption,
    future: F,
) -> F::Output {
    match interceptor.handle() {
        None => future.await,
        Some(interceptor) => {
            let ctx = tikv_client::with_rpc_interceptor(
                &tikv_client::trace::current_trace_context(),
                interceptor.clone(),
            );
            tikv_client::trace::with_trace_context(ctx, future).await
        }
    }
}

/// Go `MPPClient.EstablishMPPConns`: the coordinator's stream from the root
/// task, retried within the coprocessor's backoff budget until its first
/// packet arrives.
async fn establish_mpp_response<S: tikv_client::PdClient>(
    target: &MppTarget<S>,
    meta: TaskMeta,
    receiver_meta: TaskMeta,
    memory: Arc<dyn MppContext>,
) -> Result<MppQueryResponse<S>, String> {
    // This is the coordinator's effective CopNextMaxBackoff budget.
    let mut backoff = RegionBackoffBudget::campaign_default();
    let result = loop {
        // client-go getMPPStreamResponse receives once inside SendRequest;
        // only errors before the first packet belong to setup retry.
        let attempt = under_rpc_interceptor(
            &target.rpc_interceptor,
            mpp_setup(
            Some(memory.as_ref()),
            Some(MPP_RECEIVE_TIMEOUT),
            "connect stream",
            send_mpp::<_, _, tikv_client::tikv::MppStreamResponse>(
                &target.store,
                &target.address,
                target.store_type,
                tikv_client::tikv::MppStreamRequest(EstablishMppConnectionRequest {
                    sender_meta: Some(meta.clone()),
                    receiver_meta: Some(receiver_meta.clone()),
                }),
                MPP_RECEIVE_TIMEOUT,
            ),
        ),
        )
        .await;
        match attempt {
            Ok(stream) => break Ok(stream),
            Err(error) => {
                target.invalidate_compute_cache();
                if !error.retryable() {
                    break Err(error.to_string());
                }
                let delay = match backoff.next_delay(RegionBackoffKind::TiFlashRpc) {
                    Ok(delay) => delay,
                    Err(_) => break Err(error.to_string()),
                };
                let waited = mpp_setup(Some(memory.as_ref()), None, "retry wait", async {
                    tokio::time::sleep(delay).await;
                    Ok(())
                })
                .await;
                backoff.finish_wait(waited.is_ok());
                if let Err(error) = waited {
                    break Err(error.to_string());
                }
            }
        }
    };
    match result {
        Ok(mut stream) => {
            let first = stream.first.take();
            let completed = first.is_none();
            let held_bytes = first
                .as_ref()
                .map_or(0, |packet| packet.encoded_len() as i64);
            memory.consume(held_bytes);
            if let Err(error) = mpp_memory_error(memory.as_ref()) {
                memory.consume(-held_bytes);
                drop(stream);
                cancel_mpp_task(target, cancel_task_meta(&meta)).await;
                return Err(error.to_string());
            }
            Ok(MppQueryResponse {
                stream: (!completed).then_some(stream),
                first_packet: first,
                target: (!completed).then(|| target.clone()),
                memory,
                cancel_meta: cancel_task_meta(&meta),
                held_bytes,
                receive_timeout: MPP_RECEIVE_TIMEOUT,
                completed,
                closed: false,
            })
        }
        Err(error) => {
            cancel_mpp_task(target, cancel_task_meta(&meta)).await;
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

/// Go `MPPClient.CancelMPPTasks`: `CmdMPPCancel` to the task's store.
async fn cancel_mpp_task<S: tikv_client::PdClient>(target: &MppTarget<S>, meta: TaskMeta) {
    let (start_ts, mpp_version) = (meta.start_ts, meta.mpp_version);
    let request = tidb_proto::mpp::CancelTaskRequest {
        meta: Some(meta),
        ..Default::default()
    };
    if let Err(error) = mpp_setup(
        None,
        Some(tikv_client::tikv::READ_TIMEOUT_SHORT),
        "cancel",
        send_mpp::<_, _, tidb_proto::mpp::CancelTaskResponse>(
            &target.store,
            &target.address,
            target.store_type,
            request,
            tikv_client::tikv::READ_TIMEOUT_SHORT,
        ),
    )
    .await
    {
        target.invalidate_compute_cache();
        tidb_logutil::bg_logger().warn(
            "cancel task error",
            &[
                tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                tidb_log::Field::new("query id", tidb_log::Value::U64(start_ts)),
                tidb_log::Field::new("on addr", tidb_log::Value::Str(target.address.clone())),
                tidb_log::Field::new("mpp-version", tidb_log::Value::I64(mpp_version)),
            ],
        );
    }
}

/// The statement's stop as a response error, with its SQL identity.
fn mpp_memory_error(ctx: &dyn MppContext) -> Result<(), QueryResponseError> {
    ctx.check().map_err(|sql| QueryResponseError::Sql {
        code: sql.code,
        message: sql.message,
    })
}

/// Pull one packet at a time through the shared DistSQL decoder. No query-wide
/// packet queue is retained, and the canonical statement killer can interrupt
/// a stalled message without waiting for another network packet.
struct MppQueryResponse<S: tikv_client::PdClient> {
    first_packet: Option<MppDataPacket>,
    stream: Option<tikv_client::tikv::MppStreamResponse>,
    target: Option<MppTarget<S>>,
    memory: Arc<dyn MppContext>,
    cancel_meta: TaskMeta,
    held_bytes: i64,
    receive_timeout: std::time::Duration,
    completed: bool,
    closed: bool,
}

impl<S: tikv_client::PdClient> MppQueryResponse<S> {
    fn release_packet(&mut self) {
        if self.held_bytes != 0 {
            self.memory.consume(-self.held_bytes);
            self.held_bytes = 0;
        }
    }
    fn pull(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.release_packet();
        mpp_memory_error(self.memory.as_ref())?;
        let Some(target) = self.target.clone() else {
            return Ok(None);
        };
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };
        let packet = if let Some(first) = self.first_packet.take() {
            Some(first)
        } else {
            let memory = &self.memory;
            let timeout = self.receive_timeout;
            target
                .store
                .block_on(mpp_setup(
                    Some(memory.as_ref()),
                    Some(timeout),
                    "stream receive",
                    async { stream.message().await },
                ))
                .map_err(|error| match mpp_memory_error(self.memory.as_ref()) {
                    Err(sql) => sql,
                    Ok(()) => QueryResponseError::Source(error.to_string()),
                })?
        };
        let Some(packet) = packet else {
            self.completed = true;
            self.stream = None;
            self.target = None;
            return Ok(None);
        };
        if let Some(ref error) = packet.error {
            return Err(QueryResponseError::Source(format!(
                "tiflash mpp: task error: {} ({})",
                error.msg, error.code
            )));
        }
        self.held_bytes = packet.encoded_len() as i64;
        self.memory.consume(self.held_bytes);
        mpp_memory_error(self.memory.as_ref())?;
        Ok(Some(QueryResultSubset {
            data: packet.data.into(),
            runtime: None,
        }))
    }
}

/// The gather holds the packet it was handed and charges it itself.
impl<S: tikv_client::PdClient> MppStreamResponse for MppQueryResponse<S> {
    fn release_packet_memory(&mut self) {
        self.release_packet();
    }
}

impl<S: tikv_client::PdClient> tidb_txnkv::Response for MppQueryResponse<S> {
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
        if let Some(target) = self.target.take() {
            if !self.completed {
                target
                    .store
                    .block_on(cancel_mpp_task(&target, self.cancel_meta.clone()));
            }
        }
    }
}


impl<S: tikv_client::PdClient> Drop for MppQueryResponse<S> {
    fn drop(&mut self) {
        tidb_txnkv::Response::close(self);
    }
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
    use tidb_executor::mpp_coordinator::mpp_task_metadata;

    #[test]
    fn mpp_metadata_carries_the_domain_server_id() {
        let statement = tidb_distsql::remote_scan::PushdownStatementContext {
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
    use tidb_executor::mpp_coordinator::{MppRecoveryResponse, MppTaskResponses};
    use tidb_txnkv::Response as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tidb_pd_client::ClusterSecurity;
    use tidb_proto::tikvpb::tikv_server::{Tikv, TikvServer};
    use tikv_client::PdClient as _;
    use tokio_stream::StreamExt as _;

    /// The store the fixtures dispatch through: client-rust's PD client over
    /// unistore's in-process PD -- which lists the stores a test adds --
    /// with the real gRPC connector, so MPP requests reach the fake TiFlash
    /// service on its socket.
    type TestPd =
        tikv_client::PdRpcClient<tikv_client::tikv::TikvConnect, tidb_unistore::pd::PdClient>;

    fn test_store(
        security: &ClusterSecurity,
    ) -> (TikvStore<TestPd>, Arc<tidb_unistore::cluster::Cluster>) {
        let (_rpc, pd, cluster) = tidb_unistore::mock::new(Vec::new()).unwrap();
        let config = tidb_txnkv::driver::tikv_store::cluster_security_config(security);
        let runtime = tidb_txnkv::driver::tikv_store::store_runtime().unwrap();
        let store = runtime
            .block_on(async {
                let connect = tikv_client::tikv::TikvConnect::new_rpc_client(&config)?;
                let pd =
                    tikv_client::PdRpcClient::new_with_pd_client(config.clone(), connect, pd, true)
                        .await?;
                tikv_client::tikv::KvStore::new_with_pd_client(
                    Arc::new(pd),
                    Arc::new(tikv_client::tikv::MockSafePointKv::new()),
                    config,
                )
                .await
            })
            .unwrap();
        (TikvStore::new(runtime, store), cluster)
    }

    fn tls_security() -> ClusterSecurity {
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
    }

    fn engine_label(engine: &str) -> Vec<tidb_proto::metapb::StoreLabel> {
        vec![tidb_proto::metapb::StoreLabel {
            key: "engine".into(),
            value: engine.into(),
        }]
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

    /// One fake TiFlash node on a socket and the store that reaches it.
    struct Fixture {
        attempts: Arc<AtomicUsize>,
        address: String,
        runtime: Arc<tokio::runtime::Runtime>,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        task: Option<tokio::task::JoinHandle<()>>,
        cancels: Arc<AtomicUsize>,
        connections: Arc<AtomicUsize>,
        store: TikvStore<TestPd>,
        cluster: Arc<tidb_unistore::cluster::Cluster>,
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
                // The fake server's rustls needs the process crypto provider,
                // which the cluster TLS owner installs.
                tls_security().client_tls_config().unwrap();
                tls_security()
            } else {
                ClusterSecurity::plaintext()
            };
            let (store, cluster) = test_store(&security);
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
                store,
                cluster,
            }
        }
        /// The task's store as `open_task` builds it, for a classic TiFlash
        /// node.
        fn target(&self, compute_cache: bool) -> MppTarget<TestPd> {
            MppTarget {
                store: self.store.clone(),
                address: self.address.clone(),
                store_type: tikv_client::tikv::EndpointType::TiFlash,
                compute_cache,
                rpc_interceptor: Default::default(),
            }
        }
        fn open(&self) -> Result<MppQueryResponse<TestPd>, String> {
            self.open_with_memory(tidb_variable::mem_quota::StatementMemory::default())
        }
        fn open_with_memory(
            &self,
            memory: tidb_variable::mem_quota::StatementMemory,
        ) -> Result<MppQueryResponse<TestPd>, String> {
            self.open_target(&self.target(false), memory)
        }
        fn open_target(
            &self,
            target: &MppTarget<TestPd>,
            memory: tidb_variable::mem_quota::StatementMemory,
        ) -> Result<MppQueryResponse<TestPd>, String> {
            self.store.block_on(async {
                let response = tokio::time::timeout(
                    Duration::from_secs(2),
                    establish_mpp_response(
                        target,
                        TaskMeta::default(),
                        TaskMeta::default(),
                        Arc::new(memory),
                    ),
                )
                .await;
                response.map_err(|_| "opening drained a stalled stream".to_string())?
            })
        }
        /// Registers a TiFlash compute node in the cluster PD lists.
        fn add_compute_store(&self, id: u64, address: &str) {
            self.cluster
                .region_manager()
                .add_store(id, address, engine_label("tiflash_compute"));
        }
        /// Go `RegionCache.GetTiFlashComputeStores`: the cached list, or PD's
        /// after an invalidation.
        fn compute_stores(&self) -> usize {
            let pd = self.store.store().pd_client();
            self.store
                .block_on(async move { pd.get_tiflash_compute_stores().await })
                .unwrap()
                .len()
        }
        /// Warms the compute-store cache with this node, then registers a
        /// second node: a later [`Self::compute_stores`] of 2 means the cache
        /// was invalidated and reloaded, 1 that it was kept.
        fn warm_compute_cache(&self) {
            self.add_compute_store(1, &self.address);
            assert_eq!(self.compute_stores(), 1);
            self.add_compute_store(2, "127.0.0.1:1");
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = self.shutdown.take().unwrap().send(());
            self.runtime.block_on(self.task.take().unwrap()).unwrap();
        }
    }

    fn region(id: u64, start: &[u8], end: &[u8]) -> tikv_client::RegionWithLeader {
        tikv_client::RegionWithLeader {
            region: tidb_proto::metapb::Region {
                id,
                start_key: start.to_vec(),
                end_key: end.to_vec(),
                region_epoch: Some(tidb_proto::metapb::RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                }),
                ..Default::default()
            },
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
    fn kv_exec_counter_batch_mpp_socket_setup_marks_target() {
        use tidb_topsql_stmtstats::{create_statement_stats, KvExecCounterHandle};
        tidb_topsql_state::enable_top_sql();
        let fixture = Fixture::with_opening(
            false,
            false,
            false,
            b"first".to_vec(),
            Opening::RetryHeaders,
        );
        let stats = create_statement_stats();
        let counter = KvExecCounterHandle::default();
        counter.initialize(&stats, b"sql", b"plan");
        let mut target = fixture.target(false);
        target.rpc_interceptor = tidb_txnkv::RpcInterceptorOption::new(counter.rpc_interceptor());
        let mut response = fixture
            .open_target(&target, tidb_variable::mem_quota::StatementMemory::default())
            .unwrap();
        assert!(response.next().unwrap().is_some());
        response.close();
        tidb_topsql_state::disable_top_sql();
        let data = stats.take();
        let total: u64 = data
            .iter()
            .flat_map(|(_, v)| v.kv_stats_item.kv_exec_count.values())
            .sum();
        assert_eq!(fixture.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(total, 1);
    }

    /// Go `MPPClient.EstablishMPPConns`: a setup or cancel failure on a
    /// compute node invalidates the region cache's compute-store list; a
    /// task error or a late stream failure does not.
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
            fixture.warm_compute_cache();
            let result = fixture.open_target(
                &fixture.target(true),
                tidb_variable::mem_quota::StatementMemory::default(),
            );
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
            assert_eq!(
                fixture.compute_stores(),
                if expect_reload { 2 } else { 1 },
                "{}",
                match opening {
                    Opening::Normal => "normal",
                    Opening::RetryHeaders => "retry headers",
                    Opening::RetryFirstPacket => "retry first packet",
                    Opening::Canceled => "canceled",
                    Opening::MemoryLimit => "memory limit",
                    Opening::CancelFailure => "cancel failure",
                    _ => "other",
                }
            );
        }
    }

    /// Go `MPPClient.DispatchMPPTask`: a dispatch RPC failure invalidates the
    /// compute-store list only for a cached compute node (disaggregated
    /// TiFlash without the autoscaler); an application rejection never does.
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
                fixture.warm_compute_cache();
                let client = TiFlashMppClient::new(fixture.store.clone(), || 1);
                let ctx: Arc<dyn MppContext> =
                    Arc::new(tidb_variable::mem_quota::StatementMemory::default());
                let result = tidb_txnkv::MppClient::dispatch_mpp_task(
                    &client,
                    &ctx,
                    MppStoreTask {
                        address: fixture.address.clone(),
                        regions: vec![],
                    },
                    TaskMeta::default(),
                    Vec::new(),
                    &Default::default(),
                    &Default::default(),
                );
                assert!(result.is_err());
                assert_eq!(
                    fixture.compute_stores(),
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

    #[test]
    fn mpp_failure_batch_socket_recovery_reuses_connections_and_cancels_old_gather() {
        let fixture = Arc::new(Fixture::with_opening(
            false,
            false,
            false,
            b"new".to_vec(),
            Opening::MemoryLimit,
        ));
        let memory = tidb_variable::mem_quota::StatementMemory::default();
        let initial = fixture.open_with_memory(memory.clone()).unwrap();
        let connections = fixture.connections.load(Ordering::SeqCst);
        let next_fixture = fixture.clone();
        let next_memory = memory.clone();
        let mut response = MppRecoveryResponse::new(
            MppTaskResponses {
                streams: [Box::new(initial) as Box<dyn MppStreamResponse>].into(),
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
                    streams: [Box::new(raw) as Box<dyn MppStreamResponse>].into(),
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
        assert_eq!(
            fixture.connections.load(Ordering::SeqCst),
            connections,
            "the retry dialled a new connection instead of the store's"
        );
        assert_eq!(memory.bytes_consumed(), 0);
    }

    #[test]
    fn mpp_failure_batch_socket_holder_transfers_packet_memory_once() {
        let fixture =
            Fixture::with_opening(false, false, false, b"raw".to_vec(), Opening::TwoPackets);
        let memory = tidb_variable::mem_quota::StatementMemory::default();
        let raw = fixture.open_with_memory(memory.clone()).unwrap();
        let mut response = MppRecoveryResponse::new(
            MppTaskResponses {
                streams: [Box::new(raw) as Box<dyn MppStreamResponse>].into(),
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

    /// Go `detectMPPStore` reaches the node through the store's TiKV client,
    /// over the connection the dispatch already holds.
    #[test]
    fn mpp_failure_batch_health_adapter_uses_the_stores_connection() {
        let fixture = Fixture::with_tail(false);
        let mut response = fixture.open().unwrap();
        let connections = fixture.connections.load(Ordering::SeqCst);
        let client = MppHealthClient(fixture.store.clone());
        assert!(crate::mpp_probe::MppAliveClient::is_alive(
            &client,
            &fixture.address,
            Duration::from_secs(2)
        )
        .unwrap());
        assert_eq!(fixture.connections.load(Ordering::SeqCst), connections);
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
    fn mpp_recovery_remote_canceled_is_terminal_and_keeps_the_connection() {
        let fixture = Fixture::with_opening(false, false, false, Vec::new(), Opening::Canceled);
        assert!(fixture.open().is_err());
        assert_eq!(
            fixture.attempts.load(Ordering::SeqCst),
            1,
            "Canceled must not retry"
        );
        let connections = fixture.connections.load(Ordering::SeqCst);
        let client = MppHealthClient(fixture.store.clone());
        assert!(crate::mpp_probe::MppAliveClient::is_alive(
            &client,
            &fixture.address,
            Duration::from_secs(2)
        )
        .unwrap());
        assert_eq!(
            fixture.connections.load(Ordering::SeqCst),
            connections,
            "MPP cancellation does not own connection retirement"
        );
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
            fixture.warm_compute_cache();
            let memory = tidb_variable::mem_quota::StatementMemory::default();
            let killer = memory.sql_killer().clone();
            let attempts = fixture.attempts.clone();
            let thread = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + Duration::from_secs(1);
                while attempts.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(1));
                }
                std::thread::sleep(Duration::from_millis(10));
                killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
            });
            let result = fixture.open_target(&fixture.target(true), memory.clone());
            thread.join().unwrap();
            assert_eq!(
                fixture.compute_stores(),
                2,
                "interrupted establishment invalidates PD topology"
            );
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("KILL must terminate setup"),
            };
            assert!(matches!(MppError::after_stop(&memory, error),
            MppError::Sql(error) if error.code == 1317));
            assert_eq!(fixture.attempts.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
            assert_eq!(memory.bytes_consumed(), 0);
        }
    }

    #[test]
    fn mpp_sessions_reuse_the_stores_connection() {
        let fixture = Fixture::with_tail(false);
        let mut response = fixture.open().unwrap();
        response.next().unwrap().unwrap();
        response.close();
        let connections = fixture.connections.load(Ordering::SeqCst);
        let mut response = fixture.open().unwrap();
        response.next().unwrap().unwrap();
        response.close();
        assert_eq!(fixture.connections.load(Ordering::SeqCst), connections);
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

    /// The store's connector carries the cluster TLS settings, so an MPP
    /// command to a TLS node succeeds.
    #[test]
    fn mpp_transport_uses_cluster_tls_for_a_real_rpc() {
        let fixture = Fixture::with_transport(false, true, false);
        fixture
            .store
            .block_on(cancel_mpp_task(&fixture.target(false), TaskMeta::default()));
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mpp_setup_deadline_drops_the_stalled_operation() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (owner, retired) = tokio::sync::oneshot::channel::<()>();
            let operation = async move {
                let _owner = owner;
                std::future::pending::<Result<(), tikv_client::Error>>().await
            };
            let result = tokio::time::timeout(
                Duration::from_millis(250),
                mpp_setup(
                    Some(&tidb_variable::mem_quota::StatementMemory::default()),
                    Some(Duration::from_millis(20)),
                    "dispatch",
                    operation,
                ),
            )
            .await;
            let error = result
                .expect("the RPC setup deadline must run")
                .unwrap_err();
            assert!(error.to_string().contains("timed out"), "{error}");
            assert!(error.retryable(), "a setup deadline is retried");
            assert!(
                retired.await.is_err(),
                "the expired operation retained its owner"
            );
        });
    }

    #[test]
    fn mpp_kill_interrupts_establishment_and_cancels_registered_gather() {
        let fixture = Fixture::with_transport(false, false, true);
        let memory = tidb_variable::mem_quota::StatementMemory::default();
        let killer = Arc::clone(memory.sql_killer());
        let thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
        });
        let result = fixture.open_with_memory(memory.clone());
        thread.join().unwrap();
        let error = result.err().expect("KILL must abort setup");
        assert!(!error.contains("opening drained"), "{error}");
        assert!(matches!(MppError::after_stop(&memory, error),
            MppError::Sql(error) if error.code == 1317
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
        let memory = tidb_variable::mem_quota::StatementMemory::default();
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
        let memory = tidb_variable::mem_quota::StatementMemory::new(2, tidb_vardef::OomAction::Cancel, 42);
        let error = match fixture.open_with_memory(memory.clone()) {
            Err(error) => error,
            Ok(_) => panic!("the first packet must be accounted before setup returns"),
        };
        assert!(matches!(MppError::after_stop(&memory, error),
            MppError::Sql(error) if error.code == 8175));
        assert_eq!(memory.bytes_consumed(), 0);
        assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn canonical_kill_interrupts_a_stalled_receive() {
        let fixture = Fixture::new();
        let memory = tidb_variable::mem_quota::StatementMemory::default();
        let mut response = fixture.open_with_memory(memory.clone()).unwrap();
        response.next().unwrap().unwrap();
        let killer = Arc::clone(memory.sql_killer());
        let thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
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

    /// Go `buildBatchCopTasks`' `SplitKeyRangesByLocations`: every region the
    /// ranges cross, from the store's region cache, each clipped to the
    /// ranges; a retry region invalidated by dispatch is located again.
    #[test]
    fn mpp_range_lookup_splits_by_the_stores_regions() {
        let (store, cluster) = test_store(&ClusterSecurity::plaintext());
        let (_, region_ids, _) = tidb_unistore::cluster::bootstrap_with_multi_regions(
            cluster.region_manager(),
            &[b"m".to_vec(), b"t".to_vec()],
        );
        let ranges = vec![
            (b"b".to_vec().into(), b"c".to_vec().into()),
            (b"p".to_vec().into(), b"x".to_vec().into()),
        ];
        let memory = tidb_variable::mem_quota::StatementMemory::default();
        let kv_context = tidb_txnkv::StatementKvContext::default();
        let infos = locate_mpp_region_infos(&store, &ranges, &memory, &kv_context).unwrap();
        assert_eq!(
            infos.iter().map(|info| info.region_id).collect::<Vec<_>>(),
            region_ids
        );
        assert_eq!(infos[1].ranges.len(), 1, "the gap below p is not scanned");
        let epoch = infos[0].region_epoch.clone().unwrap();
        store.block_on(invalidate_mpp_retry_regions(
            &store,
            &[tidb_proto::metapb::Region {
                id: infos[0].region_id,
                region_epoch: Some(tidb_proto::metapb::RegionEpoch {
                    conf_ver: epoch.conf_ver,
                    version: epoch.version,
                }),
                ..Default::default()
            }],
        ));
        assert_eq!(
            locate_mpp_region_infos(&store, &ranges, &memory, &kv_context).unwrap(),
            infos
        );
    }

    #[test]
    fn remote_projection_uses_the_same_statement_memory_and_killer() {
        let memory = tidb_variable::mem_quota::StatementMemory::default();
        let context = tidb_sessionctx::StmtContext::for_query_with_memory(memory.clone());
        let remote = tidb_executor::remote_scan::pushdown_statement_context(&context);
        assert!(Arc::ptr_eq(remote.memory.sql_killer(), memory.sql_killer()));
        remote.memory.stmt_tracker().consume(17);
        assert_eq!(memory.bytes_consumed(), 17);
        remote.memory.stmt_tracker().consume(-17);
    }
}

#[cfg(test)]
mod compute_topology_batch_tests {
    use super::*;
    use tidb_tiflashcompute::DispatchPolicy;

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
                .map(|task| (
                    task.address.as_str(),
                    task.regions.iter().map(|r| r.region_id).collect::<Vec<_>>()
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
        let mut flattened = hash.into_iter().flat_map(|task| task.regions).collect::<Vec<_>>();
        flattened.sort_by_key(|r| r.region_id);
        assert_eq!(flattened, regions);
        assert!(
            compute_region_tasks(regions.clone(), &[], DispatchPolicy::ConsistentHash, 0).is_err()
        );
        assert!(compute_region_tasks(regions, &stores, DispatchPolicy::Invalid, 0).is_err());
    }
}
