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

use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;

use prost::Message as _;
use tidb_proto::mpp::{
    DispatchTaskRequest, EstablishMppConnectionRequest, MppDataPacket, TaskMeta,
};
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::retry::RegionBackoffBudget;
use tidb_txnkv::{
    MppBuildTasksRequest, MppContext, MppError, MppRpcError, MppStoreTask, MppStreamResponse,
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
pub struct MPPClient<S: tikv_client::PdClient = tikv_client::PdRpcClient> {
    store: TikvStore<S>,
    /// Go `is.SchemaMetaVersion()`, read at dispatch time from the node's
    /// catalog watch: TiFlash resolves the request's table in the schema
    /// generation the dispatch names, so a stale or zero version makes even a
    /// synced table "not exist". Go's coordinator passes the statement's
    /// version; this tier's session catalog carries a mutation counter rather
    /// than the cluster's version, so the node's watch answers instead.
    schema_version: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// Go `kvStore.mppStoreCnt`.
    mpp_store_cnt: Arc<MppStoreCnt>,
}

/// Go `GetMPPStoreCount`'s TTL: 120 seconds, in microseconds.
const MPP_STORE_COUNT_TTL: i64 = 120 * 1_000_000;

/// Go `mppStoreCnt`: the cached number of TiFlash stores.
#[derive(Debug, Default)]
pub struct MppStoreCnt {
    cnt: AtomicI32,
    last_update: AtomicI64,
    init_flag: AtomicI32,
    /// Go failpoint `mppStoreCountSetLastUpdateTimeP2`: the last update
    /// time another caller stores between this caller's read and its swap.
    #[cfg(test)]
    last_update_p2: std::sync::Mutex<Option<i64>>,
}

impl MppStoreCnt {
    /// Go `mppStoreCnt.getMPPStoreCount`: the cached count while it is
    /// younger than `ttl` microseconds, else the stores `get_all_stores`
    /// (Go `pdClient.GetAllStores(ctx, opt.WithExcludeTombstone())`) answers
    /// that pass `tikv.LabelFilterNoTiFlashWriteNode`.
    pub fn get_mpp_store_count(
        &self,
        get_all_stores: impl FnOnce() -> Result<Vec<tikv_client::proto::metapb::Store>, String>,
        ttl: i64,
    ) -> Result<usize, String> {
        let last_update = self.last_update.load(Ordering::SeqCst);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_micros() as i64);
        let is_init = self.init_flag.load(Ordering::SeqCst) != 0;
        let cached = || Ok(self.cnt.load(Ordering::SeqCst) as usize);

        if now - last_update < ttl && is_init {
            return cached();
        }

        #[cfg(test)]
        if let Some(value) = *self.last_update_p2.lock().unwrap() {
            self.last_update.store(value, Ordering::SeqCst);
        }

        // if has't initialized, always fetch latest mpp store info
        if self
            .last_update
            .compare_exchange(last_update, now, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
            && is_init
        {
            return cached();
        }

        // update mpp store cache
        let stores = match get_all_stores() {
            Ok(stores) => stores,
            Err(error) => {
                // always to update cache next time
                self.init_flag.store(0, Ordering::SeqCst);
                return Err(error);
            }
        };
        let cnt = stores
            .iter()
            .filter(|store| tikv_client::TiFlashLabelFilter::NoWriteNode.matches(&store.labels))
            .count();

        if !is_init || self.last_update.load(Ordering::SeqCst) == now {
            self.cnt.store(cnt as i32, Ordering::SeqCst);
            self.init_flag.store(1, Ordering::SeqCst);
        }

        Ok(cnt)
    }
}

impl<S: tikv_client::PdClient> Clone for MPPClient<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            schema_version: Arc::clone(&self.schema_version),
            mpp_store_cnt: Arc::clone(&self.mpp_store_cnt),
        }
    }
}

impl<S: tikv_client::PdClient> std::fmt::Debug for MPPClient<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MPPClient")
    }
}

impl<S: tikv_client::PdClient> MPPClient<S> {
    /// Builds the client over the node's store and catalog watch.
    pub fn new(
        store: TikvStore<S>,
        schema_version: impl Fn() -> i64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            store,
            schema_version: Arc::new(schema_version),
            mpp_store_cnt: Arc::default(),
        }
    }

    /// Go `buildBatchCopTasksForNonPartitionedTable` on classic TiFlash:
    /// each region on one of its own TiFlash replicas, balanced across the
    /// alive stores that hold them.
    /// Go `buildBatchCopTasksForNonPartitionedTable` (`partition_ids`
    /// empty, one range set) or `buildBatchCopTasksForPartitionedTable` (a
    /// range set per partition, then `convertRegionInfosToPartitionTableRegions`)
    /// on classic TiFlash.
    fn classic_tasks(
        &self,
        ctx: &Arc<dyn MppContext>,
        ranges_for_each_physical_table: &[Vec<(tidb_txnkv::Key, tidb_txnkv::Key)>],
        partition_ids: &[i64],
        replica_read: tidb_tiflash::ReplicaRead,
        kv_context: &tidb_txnkv::StatementKvContext,
        append_warning: &mut dyn FnMut(String),
    ) -> Result<Vec<MppStoreTask>, String> {
        let (mut tasks, warnings) =
            self.store
                .block_on(crate::cop_store::with_statement_kv_context(
                    kv_context,
                    crate::batch_cop_tasks::build_batch_cop_tasks_core(
                        &self.store,
                        ranges_for_each_physical_table,
                        ctx,
                        replica_read,
                        kv_context,
                    ),
                ))?;
        for warning in warnings {
            append_warning(warning);
        }
        let mut partition_table_regions = if partition_ids.is_empty() {
            vec![Vec::new(); tasks.len()]
        } else {
            crate::batch_cop_tasks::convert_region_infos_to_partition_table_regions(&mut tasks, partition_ids)
        };
        Ok(tasks
            .into_iter()
            .zip(partition_table_regions.drain(..))
            .map(|(task, partition_table_regions)| MppStoreTask {
                address: task.store_addr,
                store_id: task.store_id,
                region_peers: task
                    .region_infos
                    .iter()
                    .map(|region| region.region.region.peers.len())
                    .collect(),
                regions: task
                    .region_infos
                    .into_iter()
                    .map(|region| region.info)
                    .collect(),
                partition_table_regions,
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
                    probes.spawn(tidb_cpuprofile::labels::inherit_future(async move {
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
                    }));
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
                .next_delay(tikv_client::retry::BO_TIFLASH_RPC)
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

impl<S: tikv_client::PdClient + 'static> tidb_txnkv::MppClient for MPPClient<S> {
    fn get_mpp_store_count(&self) -> Result<usize, String> {
        self.mpp_store_cnt.get_mpp_store_count(
            || {
                let pd = self.store.store().pd_client();
                // `get_all_stores` answers tombstones too; Go asks PD to
                // leave them out (`opt.WithExcludeTombstone()`).
                let tombstone = tikv_client::proto::metapb::StoreState::Tombstone as i32;
                self.store
                    .block_on(async move { pd.get_all_stores().await })
                    .map(|stores| stores.into_iter().filter(|store| store.state != tombstone).collect())
                    .map_err(|error| error.to_string())
            },
            MPP_STORE_COUNT_TTL,
        )
    }

    fn tiflash_stores(&self) -> Vec<tikv_client::proto::metapb::Store> {
        self.store
            .store()
            .pd_client()
            .get_tiflash_stores(tikv_client::TiFlashLabelFilter::NoWriteNode)
    }

    fn tiflash_compute_stores(&self) -> Result<Vec<tikv_client::proto::metapb::Store>, String> {
        let pd = self.store.store().pd_client();
        self.store
            .block_on(async move { pd.get_tiflash_compute_stores().await })
            .map_err(|error| error.to_string())
    }

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
        let to_pairs = |ranges: &[tidb_txnkv::KeyRange]| -> Vec<(tidb_txnkv::Key, tidb_txnkv::Key)> {
            ranges
                .iter()
                .map(|range| (range.start_key.clone(), range.end_key.clone()))
                .collect()
        };
        let config = tidb_config::config_tree::config::get_global_config();
        if !config.disaggregated_tiflash {
            // Go `ConstructMPPTasks`: a partitioned request builds a range set
            // per partition; otherwise its key ranges must be set.
            let (ranges_for_each_physical_table, partition_ids): (Vec<_>, Vec<_>) =
                if request.partition_id_and_ranges.is_empty() {
                    let Some(key_ranges) = request.key_ranges.as_ref() else {
                        return Err(refuse("KeyRanges in MPPBuildTasksRequest is nil".to_owned()));
                    };
                    (vec![to_pairs(key_ranges)], Vec::new())
                } else {
                    request
                        .partition_id_and_ranges
                        .iter()
                        .map(|partition| (to_pairs(&partition.key_ranges), partition.id))
                        .unzip()
                };
            return self
                .classic_tasks(
                    ctx,
                    &ranges_for_each_physical_table,
                    &partition_ids,
                    replica_read,
                    kv_context,
                    append_warning,
                )
                .map_err(|message| {
                    MppError::after_stop(ctx.as_ref(), format!("tiflash mpp: {message}"))
                });
        }
        let ranges: Vec<(tidb_txnkv::Key, tidb_txnkv::Key)> =
            to_pairs(request.key_ranges.as_deref().unwrap_or_default());
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
        task: &MppStoreTask,
        meta: &TaskMeta,
        encoded_plan: &[u8],
        kv_context: &tidb_txnkv::StatementKvContext,
        rpc_interceptor: &tidb_txnkv::RpcInterceptorOption,
        bo: &mut RegionBackoffBudget,
    ) -> Result<tidb_proto::mpp::DispatchTaskResponse, MppRpcError> {
        let target = self.target(&task.address, rpc_interceptor);
        // Go: `mppReq.TableRegions = originalTask.PartitionTableRegions`, and a
        // partitioned task sends no plain regions.
        let (regions, table_regions) = if task.partition_table_regions.is_empty() {
            (task.regions.clone(), Vec::new())
        } else {
            (Vec::new(), task.partition_table_regions.clone())
        };
        // Go `originalTask != nil`: a task with regions goes through the
        // batch sender, whose send failure always invalidates the compute
        // stores; a task without regions keeps them when it was canceled.
        let has_regions = !(regions.is_empty() && table_regions.is_empty());
        let dispatch_request = encode_dispatch_request(DispatchTaskRequest {
            meta: Some(meta.clone()),
            encoded_plan: encoded_plan.to_vec(),
            regions,
            table_regions,
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
                    .await;
                    let dispatch_response = match dispatch_response {
                        Ok(dispatch_response) => dispatch_response,
                        Err(error) => {
                            if has_regions || !error.canceled() {
                                target.invalidate_compute_cache();
                            }
                            let retry = if has_regions {
                                on_send_fail_for_batch_regions(ctx.as_ref(), bo, &target, task, &error)
                                    .await
                            } else {
                                backoff_tiflash_rpc(ctx.as_ref(), bo, &error).await
                            };
                            return Err((retry, error.to_string()));
                        }
                    };
                    // Go: `if realResp.Error != nil { return realResp, false, nil }`.
                    if dispatch_response.error.is_none() {
                        // Retry regions only invalidate the region cache; they
                        // are NOT a dispatch failure. The task itself has
                        // already registered and will serve its regions
                        // through the learner read.
                        invalidate_mpp_retry_regions(
                            &target.store,
                            &dispatch_response.retry_regions,
                        )
                        .await;
                    }
                    Ok(dispatch_response)
                }),
            ))
            .map_err(|(retry, message)| MppRpcError {
                retry,
                err: MppError::after_stop(ctx.as_ref(), message),
            })
    }

    fn establish_mpp_conns(
        &self,
        ctx: Arc<dyn MppContext>,
        address: &str,
        sender: &TaskMeta,
        receiver: &TaskMeta,
        kv_context: &tidb_txnkv::StatementKvContext,
        rpc_interceptor: &tidb_txnkv::RpcInterceptorOption,
        bo: &mut RegionBackoffBudget,
    ) -> Result<Box<dyn MppStreamResponse>, MppRpcError> {
        let target = self.target(address, rpc_interceptor);
        self.store
            .block_on(crate::cop_store::with_statement_kv_context(
                kv_context,
                establish_mpp_response(&target, sender, receiver, Arc::clone(&ctx), bo),
            ))
            .map(|response| Box::new(response) as Box<dyn MppStreamResponse>)
            .map_err(|(retry, message)| MppRpcError {
                retry,
                err: MppError::after_stop(ctx.as_ref(), message),
            })
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
                partition_table_regions: Vec::new(),
                store_id: 0,
                region_peers: Vec::new(),
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
pub(crate) fn split_key_ranges_by_locations<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    ranges: &[(tidb_txnkv::Key, tidb_txnkv::Key)],
    memory: &dyn MppContext,
    kv_context: &tidb_txnkv::StatementKvContext,
) -> Result<Vec<crate::batch_cop_tasks::LocatedRegion>, String> {
    use crate::cop_store::NativeCopStore;
    if ranges.is_empty() {
        // No range lies in any region.
        return Ok(Vec::new());
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
            Some(crate::batch_cop_tasks::LocatedRegion {
                region,
                info,
                partition_index: 0,
            })
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
        probes.spawn(tidb_cpuprofile::labels::inherit_future(async move {
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
        }));
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
            pd.invalidate_cached_region_with_reason(
                tikv_client::tikv::new_region_ver_id(region.id, epoch.conf_ver, epoch.version),
                tikv_client::tikv::InvalidReason::EpochNotMatch,
            )
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

    /// Go `errors.Cause(err) == context.Canceled ||
    /// status.Code(errors.Cause(err)) == codes.Canceled`: the statement
    /// stopped the RPC, or the store answered that it was canceled. Such a
    /// send is never retried.
    fn canceled(&self) -> bool {
        // `errors.Cause`: the connection annotation is not the kind.
        match self.transport.as_ref().map(tikv_client::Error::cause) {
            None | Some(tikv_client::Error::ContextCanceled) => true,
            Some(tikv_client::Error::GrpcAPI(status)) => status.code() == tonic::Code::Cancelled,
            Some(_) => false,
        }
    }
}

/// Go `RegionBatchRequestSender.onSendFailForBatchRegions` with
/// `RegionCache.OnSendFailForBatchRegions`: after a send to the task's store
/// failed, every region of the task switches to its next TiFlash peer and
/// reloads, and the answer is whether the send is retried. A canceled send
/// and a TiDB that is shutting down do neither.
async fn on_send_fail_for_batch_regions<S: tikv_client::PdClient>(
    memory: &dyn MppContext,
    bo: &mut RegionBackoffBudget,
    target: &MppTarget<S>,
    task: &MppStoreTask,
    error: &MppSetupError,
) -> bool {
    if error.canceled() || tikv_client::load_shutting_down() > 0 {
        return false;
    }
    if target.store_type == tikv_client::tikv::EndpointType::TiFlash {
        // The reload region param is always true. Because that every time we try, we must
        // re-build the range then re-create the batch sender.
        let pd = target.store.store().pd_client();
        for (region, peers) in task.regions.iter().zip(&task.region_peers) {
            let epoch = region.region_epoch.clone().unwrap_or_default();
            pd.on_send_fail_for_tiflash(
                &tikv_client::tikv::new_region_ver_id(region.region_id, epoch.conf_ver, epoch.version),
                task.store_id,
                *peers,
                true,
                true,
            )
            .await;
        }
    }
    backoff_tiflash_rpc(memory, bo, error).await
}

/// Go `bo.Backoff(tikv.BoTiFlashRPC(), err) == nil` after an MPP send failed
/// with `error`: whether the coordinator sends it again. A canceled send
/// does not back off.
async fn backoff_tiflash_rpc(
    memory: &dyn MppContext,
    bo: &mut RegionBackoffBudget,
    error: &MppSetupError,
) -> bool {
    if error.canceled() {
        return false;
    }
    let Ok(delay) = bo.next_delay(tikv_client::retry::BO_TIFLASH_RPC) else {
        return false;
    };
    let waited = mpp_setup(Some(memory), None, "retry wait", async {
        tokio::time::sleep(delay).await;
        Ok(())
    })
    .await;
    bo.finish_wait(waited.is_ok());
    waited.is_ok()
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
/// task. A send that failed before its first packet and was not canceled
/// backs off within `bo` and answers `true`: the coordinator sends it again.
async fn establish_mpp_response<S: tikv_client::PdClient>(
    target: &MppTarget<S>,
    meta: &TaskMeta,
    receiver_meta: &TaskMeta,
    memory: Arc<dyn MppContext>,
    bo: &mut RegionBackoffBudget,
) -> Result<MppQueryResponse<S>, (bool, String)> {
    let result = {
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
            Ok(stream) => Ok(stream),
            Err(error) => {
                target.invalidate_compute_cache();
                if backoff_tiflash_rpc(memory.as_ref(), bo, &error).await {
                    return Err((true, error.to_string()));
                }
                Err(error.to_string())
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
                cancel_mpp_task(target, cancel_task_meta(meta)).await;
                return Err((false, error.to_string()));
            }
            Ok(MppQueryResponse {
                stream: (!completed).then_some(stream),
                first_packet: first,
                target: (!completed).then(|| target.clone()),
                memory,
                cancel_meta: cancel_task_meta(meta),
                held_bytes,
                receive_timeout: MPP_RECEIVE_TIMEOUT,
                completed,
                closed: false,
                packet_error: None,
            })
        }
        Err(error) => {
            cancel_mpp_task(target, cancel_task_meta(meta)).await;
            Err((false, error))
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
    /// Go `MPPDataPacket.Error.Msg` of the packet `pull` failed on.
    packet_error: Option<String>,
}

impl<S: tikv_client::PdClient> MppQueryResponse<S> {
    fn release_packet(&mut self) -> i64 {
        let held_bytes = std::mem::take(&mut self.held_bytes);
        self.memory.consume(-held_bytes);
        held_bytes
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
        if let Some(error) = packet.error {
            // Go `handleMPPStreamResponse`.
            let err = QueryResponseError::Source(format!("other error for mpp stream: {}", error.msg));
            self.packet_error = Some(error.msg);
            return Err(err);
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
    fn release_packet_memory(&mut self) -> i64 {
        self.release_packet()
    }

    fn packet_error(&self) -> Option<&str> {
        self.packet_error.as_deref()
    }
}

impl<S: tikv_client::PdClient> tidb_txnkv::Response for MppQueryResponse<S> {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        if self.closed {
            return Ok(None);
        }
        let result = self.pull();
        if result.is_err() || matches!(result, Ok(None)) {
            let _ = self.close();
        }
        result
    }

    fn close(&mut self) -> Result<(), QueryResponseError> {
        if self.closed {
            return Ok(());
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
        Ok(())
    }
}


impl<S: tikv_client::PdClient> Drop for MppQueryResponse<S> {
    fn drop(&mut self) {
        let _ = tidb_txnkv::Response::close(self);
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
mod mpp_store_cnt_tests {
    use super::*;
    use tikv_client::proto::metapb::{Store, StoreLabel};

    fn store(labels: &[(&str, &str)]) -> Store {
        Store {
            labels: labels
                .iter()
                .map(|(key, value)| StoreLabel {
                    key: (*key).to_owned(),
                    value: (*value).to_owned(),
                })
                .collect(),
            ..Store::default()
        }
    }

    /// Go failpoint `mppStoreCountSetMPPCnt`: PD answers `cnt` TiFlash stores.
    fn tiflash(cnt: usize) -> Result<Vec<Store>, String> {
        Ok(vec![store(&[("engine", "tiflash")]); cnt])
    }

    /// Go failpoint `mppStoreCountPDError`.
    fn pd_error() -> Result<Vec<Store>, String> {
        Err("failed to get mpp store count".to_owned())
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    }

    /// Go `TestMppStoreCntWithErrors`, with each failpoint's effect applied
    /// where Go injects it.
    #[test]
    fn test_mpp_store_cnt_with_errors() {
        let cnt = MppStoreCnt::default();
        let ttl = MPP_STORE_COUNT_TTL;
        // Three mock TiFlash stores beside a TiKV store and a write node.
        let stores = || {
            let mut stores = tiflash(3).unwrap();
            stores.push(store(&[]));
            stores.push(store(&[("engine", "tiflash"), ("engine_role", "write")]));
            Ok(stores)
        };
        assert_eq!(cnt.get_mpp_store_count(stores, ttl), Ok(3));
        // meet cache
        assert_eq!(cnt.get_mpp_store_count(|| tiflash(1000), ttl), Ok(3));
        // update cache
        cnt.last_update.store(0, Ordering::SeqCst);
        assert_eq!(cnt.get_mpp_store_count(|| tiflash(1000), ttl), Ok(1000));
        cnt.last_update.store(0, Ordering::SeqCst);
        assert_eq!(
            cnt.get_mpp_store_count(pd_error, ttl),
            Err("failed to get mpp store count".to_owned())
        );
        // set last update time to the latest: still update cache
        cnt.last_update.store(now(), Ordering::SeqCst);
        assert_eq!(cnt.get_mpp_store_count(|| tiflash(2222), ttl), Ok(2222));
        // fail to get lock and old cache
        cnt.last_update.store(1, Ordering::SeqCst);
        *cnt.last_update_p2.lock().unwrap() = Some(2);
        assert_eq!(cnt.get_mpp_store_count(pd_error, ttl), Ok(2222));
    }
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
    use tidb_executor::mpp_coordinator::{ExecutorWithRetry, MppGatherResponse, MppTaskResponses};
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
        let config = tidb_txnkv::driver::tikv_store::cluster_security(security);
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
                // Go `receiveResults`: the coordinator's retry loop.
                let memory: Arc<dyn MppContext> = Arc::new(memory);
                let mut bo = RegionBackoffBudget::campaign_default();
                let response = tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        let attempt = establish_mpp_response(
                            target,
                            &TaskMeta::default(),
                            &TaskMeta::default(),
                            Arc::clone(&memory),
                            &mut bo,
                        )
                        .await;
                        match attempt {
                            Err((true, _)) => {}
                            Err((false, error)) => break Err(error),
                            Ok(response) => break Ok(response),
                        }
                    }
                })
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
        response.close().unwrap();
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
                        // Go `handleMPPStreamResponse`.
                        assert_eq!(
                            first,
                            Err(QueryResponseError::Source(
                                "other error for mpp stream: Memory limit exceeded".into()
                            ))
                        );
                        assert_eq!(response.packet_error(), Some("Memory limit exceeded"));
                    } else {
                        assert!(first.unwrap().is_some());
                        assert_eq!(response.packet_error(), None);
                    }
                    if fail_tail {
                        assert!(response.next().is_err());
                    }
                    response.close().unwrap();
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
                let client = MPPClient::new(fixture.store.clone(), || 1);
                let ctx: Arc<dyn MppContext> =
                    Arc::new(tidb_variable::mem_quota::StatementMemory::default());
                // A budget with nothing left: the failed send is not retried.
                let mut bo = RegionBackoffBudget::new(Duration::ZERO);
                let result = tidb_txnkv::MppClient::dispatch_mpp_task(
                    &client,
                    &ctx,
                    &MppStoreTask {
                        address: fixture.address.clone(),
                        regions: vec![],
                        partition_table_regions: vec![],
                        store_id: 0,
                        region_peers: vec![],
                    },
                    &TaskMeta::default(),
                    &[],
                    &Default::default(),
                    &Default::default(),
                    &mut bo,
                );
                // Go: a response that carries the task's error is the
                // dispatch's answer, not its failure.
                match result {
                    Ok(response) => assert!(application_error && response.error.is_some()),
                    Err(error) => assert!(!application_error, "{error:?}"),
                }
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

    /// Go `RegionBatchRequestSender.onSendFailForBatchRegions`: a dispatch
    /// that fails on its store switches each of the task's regions to its
    /// next TiFlash peer and schedules the region's reload.
    #[test]
    fn a_failed_dispatch_switches_the_tasks_regions_to_the_next_tiflash_peer_like_go() {
        use tikv_client::PdClient as _;
        let fixture = Fixture::with_opening(
            false,
            false,
            false,
            b"first".to_vec(),
            Opening::DispatchUnavailable,
        );
        let manager = fixture.cluster.region_manager();
        let (_, _, region_id) = tidb_unistore::cluster::bootstrap_with_single_store(manager);
        let add_tiflash = |address: &str| {
            let store_id = manager.alloc_id();
            manager.add_store(store_id, address, engine_label("tiflash"));
            manager.add_peer(region_id, store_id, manager.alloc_id());
            store_id
        };
        let failed_store = add_tiflash(&fixture.address);
        let next_store = add_tiflash("127.0.0.1:1");
        let pd = fixture.store.store().pd_client();
        let key = tikv_client::Key::from(b"a".to_vec());
        let region = fixture.store.block_on(pd.region_for_key(&key)).unwrap();
        let selected = |region: &tikv_client::RegionWithLeader| {
            fixture
                .store
                .block_on(pd.get_tiflash_rpc_context(
                    region,
                    false,
                    tikv_client::TiFlashLabelFilter::NoWriteNode,
                ))
                .unwrap()
                .map(|(_, store)| store.id)
                .map_err(|detail| detail.reason)
        };
        assert_eq!(selected(&region), Ok(failed_store));

        let client = MPPClient::new(fixture.store.clone(), || 1);
        let ctx: Arc<dyn MppContext> =
            Arc::new(tidb_variable::mem_quota::StatementMemory::default());
        let mut bo = RegionBackoffBudget::new(Duration::ZERO);
        let epoch = region.region.region_epoch.clone().unwrap();
        tidb_txnkv::MppClient::dispatch_mpp_task(
            &client,
            &ctx,
            &MppStoreTask {
                address: fixture.address.clone(),
                regions: vec![tidb_proto::coprocessor::RegionInfo {
                    region_id,
                    region_epoch: Some(tidb_proto::metapb::RegionEpoch {
                        conf_ver: epoch.conf_ver,
                        version: epoch.version,
                    }),
                    ..Default::default()
                }],
                partition_table_regions: vec![],
                store_id: failed_store,
                region_peers: vec![region.region.peers.len()],
            },
            &TaskMeta::default(),
            &[],
            &Default::default(),
            &Default::default(),
            &mut bo,
        )
        .unwrap_err();
        // The cached region is due a reload,
        assert_eq!(
            selected(&region),
            Err(tikv_client::TiFlashRpcContextUnavailableReason::NeedReloadOnAccess)
        );
        // its next lookup reloads it from PD,
        add_tiflash("127.0.0.1:2");
        let reloaded = fixture.store.block_on(pd.region_for_key(&key)).unwrap();
        assert!(reloaded.region.peers.len() > region.region.peers.len());
        // and the reloaded region reads the next TiFlash peer.
        assert_eq!(selected(&reloaded), Ok(next_store));
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
        let mut response = ExecutorWithRetry::new(
            Box::new(MppTaskResponses {
                streams: [Box::new(initial) as Box<dyn MppStreamResponse>].into(),
                node_count: 1,
            }),
            true,
            memory.clone(),
            Box::new(move || {
                assert_eq!(next_fixture.cancels.load(Ordering::SeqCst), 1);
                let raw = next_fixture
                    .open_with_memory(next_memory.clone())
                    .map_err(QueryResponseError::Source)?;
                Ok(Box::new(MppTaskResponses {
                    streams: [Box::new(raw) as Box<dyn MppStreamResponse>].into(),
                    node_count: 1,
                }) as Box<dyn MppGatherResponse>)
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
        let mut response = ExecutorWithRetry::new(
            Box::new(MppTaskResponses {
                streams: [Box::new(raw) as Box<dyn MppStreamResponse>].into(),
                node_count: 1,
            }),
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
        response.close().unwrap();
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
        response.close().unwrap();
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
        response.close().unwrap();
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
        response.close().unwrap();
        let connections = fixture.connections.load(Ordering::SeqCst);
        let mut response = fixture.open().unwrap();
        response.next().unwrap().unwrap();
        response.close().unwrap();
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
        response.close().unwrap();
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
            assert!(!error.canceled(), "a setup deadline is retried");
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
        response.close().unwrap();
        response.close().unwrap();
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
        response.close().unwrap();
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
