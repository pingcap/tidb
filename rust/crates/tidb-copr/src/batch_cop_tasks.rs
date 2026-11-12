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

//! Go `pkg/store/copr/batch_coprocessor.go`'s task building for MPP over
//! classic TiFlash (`buildBatchCopTasksCore` and `balanceBatchCopTask`):
//! each region goes to one of its own TiFlash replicas -- the store
//! `GetTiFlashRPCContext` picks, among the stores `GetAllValidTiFlashStores`
//! lists -- the replicas are filtered by liveness and by
//! `tiflash_replica_read`, and the regions are balanced across the stores
//! that hold them.
//!
//! # Narrowings, by name
//!
//! * Partitioned tables (`buildBatchCopTasksForPartitionedTable`): the MPP
//!   lowering reads one physical table, so every region has partition index
//!   zero.
//! * Disaggregated TiFlash (`buildBatchCopTasksConsistentHash` and
//!   `buildBatchCopTasksConsistentHashForPD`): each region goes to the
//!   tiflash_compute node the dispatch policy picks.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use tidb_tiflashcompute::{DispatchPolicy, TopologyError};
use tidb_txnkv::MppContext;

use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::retry::RegionBackoffBudget;
use tidb_tiflash::ReplicaRead;
use tidb_tiflash::MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS;
use tikv_client::TiFlashLabelFilter;

/// Go `copBuildTaskMaxBackoff`.
const COP_BUILD_TASK_MAX_BACKOFF: Duration = Duration::from_millis(5_000);

/// Go `fetchTopoMaxBackoff`.
const FETCH_TOPO_MAX_BACKOFF: Duration = Duration::from_millis(20_000);

/// One physical table's ranges as the task builder takes them.
pub(crate) type Ranges = Vec<(tidb_txnkv::Key, tidb_txnkv::Key)>;

/// Go `balanceContinuousRegionCount` as `ConstructMPPTasks` passes it.
pub(crate) const BALANCE_CONTINUOUS_REGION_COUNT: usize = 20;

/// Go `maxBalanceScore`.
const MAX_BALANCE_SCORE: i64 = 100;
/// Go `balanceScoreThreshold`.
const BALANCE_SCORE_THRESHOLD: i64 = 85;

/// Go `RegionInfo`: one region of a batch task and the TiFlash stores that
/// may serve it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RegionInfo {
    /// Go `Region` and `Meta`: the region as located.
    pub(crate) region: tikv_client::RegionWithLeader,
    /// Go `Ranges`: the request ranges inside the region.
    pub(crate) info: tidb_proto::coprocessor::RegionInfo,
    /// Go `AllStores`: the stores that may serve it, the chosen one first.
    pub(crate) all_stores: Vec<u64>,
    /// Go `PartitionIndex`: the physical table, of the request's, it reads.
    pub(crate) partition_index: i64,
}

/// Go `batchCopTask`: the regions one TiFlash store serves.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BatchCopTask {
    /// Go `ctx.Store`'s id.
    pub(crate) store_id: u64,
    /// Go `storeAddr`.
    pub(crate) store_addr: String,
    /// Go `regionInfos`.
    pub(crate) region_infos: Vec<RegionInfo>,
}

/// Go `aliveStoresBundle`.
#[derive(Default)]
struct AliveStores {
    stores_in_all_zones: Vec<tidb_proto::metapb::Store>,
    store_ids_in_all_zones: BTreeSet<u64>,
    store_ids_in_tidb_zone: BTreeSet<u64>,
}

/// One located region and the requested ranges inside it.
pub(crate) struct LocatedRegion {
    pub(crate) region: tikv_client::RegionWithLeader,
    pub(crate) info: tidb_proto::coprocessor::RegionInfo,
    /// Go `copTask.partitionIndex`.
    pub(crate) partition_index: i64,
}

/// Go `buildBatchCopTasksForNonPartitionedTable` (`partition_ids` empty) and
/// `buildBatchCopTasksForPartitionedTable`: the tasks over each physical
/// table's ranges, and the warnings Go's `appendWarning` receives.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_batch_cop_tasks<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    ranges_for_each_physical_table: &[Ranges],
    partition_ids: &[i64],
    memory: &Arc<dyn MppContext>,
    is_mpp: bool,
    balance_with_continuity: bool,
    balance_continuous_region_count: usize,
    dispatch_policy: DispatchPolicy,
    tiflash_replica_read_policy: ReplicaRead,
    kv_context: &tidb_txnkv::StatementKvContext,
) -> Result<(Vec<tidb_txnkv::MppStoreTask>, Vec<String>), TopologyError> {
    let config = tidb_config::config_tree::config::get_global_config();
    let build = async {
        if !config.disaggregated_tiflash {
            return Ok(build_batch_cop_tasks_core(
                store,
                ranges_for_each_physical_table,
                memory,
                is_mpp,
                balance_with_continuity,
                balance_continuous_region_count,
                tiflash_replica_read_policy,
                kv_context,
            )
            .await?);
        }
        let tasks = if config.use_auto_scaler {
            build_batch_cop_tasks_consistent_hash(
                store,
                ranges_for_each_physical_table,
                memory,
                kv_context,
                dispatch_policy,
            )
            .await?
        } else {
            // todo: remove this after AutoScaler is stable.
            build_batch_cop_tasks_consistent_hash_for_pd(
                store,
                ranges_for_each_physical_table,
                memory,
                kv_context,
                dispatch_policy,
            )
            .await?
        };
        Ok::<_, TopologyError>((tasks, Vec::new()))
    };
    let (tasks, warnings) = crate::cop_store::with_statement_kv_context(kv_context, build).await?;
    Ok((store_tasks(tasks, partition_ids), warnings))
}

/// Go `bo.Backoff(tikv.BoTiFlashRPC(), err)`: `err` once the budget is spent
/// or the statement stops.
async fn backoff_tiflash_rpc(
    bo: &mut RegionBackoffBudget,
    memory: &dyn MppContext,
    err: String,
) -> Result<(), String> {
    let delay = bo.next_delay(tikv_client::retry::BO_TIFLASH_RPC).map_err(|_| err)?;
    let waited = crate::mpp_client::mpp_retry_wait(memory, delay).await;
    bo.finish_wait(waited.is_ok());
    waited
}

/// Go's `SplitKeyRangesByLocations` loop of the consistent-hash builders:
/// each physical table's regions, tagged with the table's index.
fn split_ranges_for_each_physical_table<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    ranges_for_each_physical_table: &[Ranges],
    memory: &Arc<dyn MppContext>,
    kv_context: &tidb_txnkv::StatementKvContext,
) -> Result<Vec<LocatedRegion>, String> {
    let mut tasks = Vec::new();
    for (i, ranges) in ranges_for_each_physical_table.iter().enumerate() {
        let locations =
            crate::mpp_client::split_key_ranges_by_locations(store, ranges, memory.as_ref(), kv_context)?;
        for mut lo in locations {
            lo.partition_index = i as i64;
            tasks.push(lo);
        }
    }
    Ok(tasks)
}

/// Go `getTiFlashComputeRPCContextByConsistentHash`: each region's
/// rendezvous-hash node.
fn get_tiflash_compute_rpc_context_by_consistent_hash(ids: &[u64], stores_str: &[String]) -> Vec<String> {
    ids.iter()
        .map(|id| {
            let mut max_hash = 0;
            let mut max_hash_store = String::new();
            for store in stores_str {
                let h = tidb_murmur3::sum32(format!("{store}-{id}").as_bytes());
                if h > max_hash {
                    max_hash = h;
                    max_hash_store.clone_from(store);
                }
            }
            max_hash_store
        })
        .collect()
}

/// Go `getTiFlashComputeRPCContextByRoundRobin`.
fn get_tiflash_compute_rpc_context_by_round_robin(ids: &[u64], stores_str: &[String]) -> Vec<String> {
    let mut start_idx = tidb_fastrand::uint64_n(stores_str.len() as u64) as usize;
    ids.iter()
        .map(|_| {
            let addr = stores_str[start_idx % stores_str.len()].clone();
            start_idx += 1;
            addr
        })
        .collect()
}

/// The dispatch policy's node for each region.
fn get_tiflash_compute_rpc_contexts(
    dispatch_policy: DispatchPolicy,
    tasks: &[LocatedRegion],
    stores_str: &[String],
) -> Result<Vec<String>, TopologyError> {
    let region_ids: Vec<u64> = tasks.iter().map(|task| task.info.region_id).collect();
    match dispatch_policy {
        DispatchPolicy::RoundRobin => Ok(get_tiflash_compute_rpc_context_by_round_robin(&region_ids, stores_str)),
        DispatchPolicy::ConsistentHash => {
            Ok(get_tiflash_compute_rpc_context_by_consistent_hash(&region_ids, stores_str))
        }
        DispatchPolicy::Invalid => Err("unexpected dispatch policy 2".into()),
    }
}

/// The consistent-hash builders' grouping: all regions sent to one
/// tiflash_compute node are one batch task, in first-use order.
fn batch_cop_tasks_by_addr(tasks: Vec<LocatedRegion>, rpc_ctxs: Vec<String>) -> Vec<BatchCopTask> {
    let mut res: Vec<BatchCopTask> = Vec::new();
    let mut task_map: HashMap<String, usize> = HashMap::new();
    for (task, addr) in tasks.into_iter().zip(rpc_ctxs) {
        // tasks and rpcCtxs are correspond to each other.
        let region_info = RegionInfo {
            region: task.region,
            info: task.info,
            all_stores: Vec::new(),
            partition_index: task.partition_index,
        };
        if let Some(&i) = task_map.get(&addr) {
            res[i].region_infos.push(region_info);
        } else {
            task_map.insert(addr.clone(), res.len());
            res.push(BatchCopTask {
                store_id: 0,
                store_addr: addr,
                region_infos: vec![region_info],
            });
        }
    }
    res
}

/// Go `buildBatchCopTasksConsistentHash`: the regions over the alive
/// tiflash_compute nodes of the AutoScaler's topology.
async fn build_batch_cop_tasks_consistent_hash<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    ranges_for_each_physical_table: &[Ranges],
    memory: &Arc<dyn MppContext>,
    kv_context: &tidb_txnkv::StatementKvContext,
    dispatch_policy: DispatchPolicy,
) -> Result<Vec<BatchCopTask>, TopologyError> {
    failpoint_check_which_policy(dispatch_policy);
    let mut fetch_topo_bo = RegionBackoffBudget::new(FETCH_TOPO_MAX_BACKOFF);
    let tasks = split_ranges_for_each_physical_table(store, ranges_for_each_physical_table, memory, kv_context)?;
    let mut retry_num = 0;
    let stores_str = loop {
        retry_num += 1;
        let fetcher = tidb_tiflashcompute::global_topo_fetcher()
            .ok_or_else(|| TopologyError::from("TiFlash compute topology fetcher is not initialized"))?;
        let stores_str = tokio::task::spawn_blocking(move || fetcher.fetch_and_get_topo())
            .await
            .map_err(|error| TopologyError::from(error.to_string()))??;
        let stores_bef_filter = stores_str.len();
        let stores_str = crate::mpp_client::filter_alive_stores_str(store, &stores_str, memory).await;
        if stores_str.is_empty() {
            let err_msg = format!(
                "Cannot find proper topo to dispatch MPPTask: {}",
                if stores_bef_filter == 0 {
                    "topo from AutoScaler is empty"
                } else {
                    "detect aliveness failed, no alive ComputeNode"
                }
            );
            if tidb_intest::IN_TEST && retry_num > 3 {
                return Err(err_msg.into());
            }
            backoff_tiflash_rpc(&mut fetch_topo_bo, memory.as_ref(), err_msg).await?;
            continue;
        }
        break stores_str;
    };
    let rpc_ctxs = get_tiflash_compute_rpc_contexts(dispatch_policy, &tasks, &stores_str)?;
    Ok(batch_cop_tasks_by_addr(tasks, rpc_ctxs))
}

/// Go `buildBatchCopTasksConsistentHashForPD`: the regions over PD's alive
/// tiflash_compute stores. Only called when UseAutoScaler is false.
async fn build_batch_cop_tasks_consistent_hash_for_pd<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    ranges_for_each_physical_table: &[Ranges],
    memory: &Arc<dyn MppContext>,
    kv_context: &tidb_txnkv::StatementKvContext,
    dispatch_policy: DispatchPolicy,
) -> Result<Vec<BatchCopTask>, TopologyError> {
    failpoint_check_which_policy(dispatch_policy);
    let pd = store.store().pd_client();
    let mut bo = RegionBackoffBudget::new(COP_BUILD_TASK_MAX_BACKOFF);
    loop {
        let tasks = split_ranges_for_each_physical_table(store, ranges_for_each_physical_table, memory, kv_context)?;
        let stores = pd
            .get_tiflash_compute_stores()
            .await
            .map_err(|error| TopologyError::from(error.to_string()))?;
        let stores = crate::mpp_client::filter_alive_stores(store, &stores, memory).await;
        if stores.is_empty() {
            if tidb_intest::IN_TEST {
                // To avoid keep retrying, which makes CI slow.
                return Err("tiflash_compute node is unavailable".into());
            }
            pd.invalidate_tiflash_compute_stores();
            backoff_tiflash_rpc(&mut bo, memory.as_ref(), "tiflash_compute node is unavailable".to_owned()).await?;
            continue;
        }
        let stores_str: Vec<String> = stores.into_iter().map(|store| store.address).collect();
        let rpc_ctxs = get_tiflash_compute_rpc_contexts(dispatch_policy, &tasks, &stores_str)?;
        if rpc_ctxs.is_empty() {
            backoff_tiflash_rpc(&mut bo, memory.as_ref(), "Cannot find region with TiFlash peer".to_owned()).await?;
            continue;
        }
        return Ok(batch_cop_tasks_by_addr(tasks, rpc_ctxs));
    }
}

/// Go `failpointCheckWhichPolicy`.
fn failpoint_check_which_policy(act: DispatchPolicy) {
    tidb_failpoint::inject("github.com/pingcap/tidb/pkg/store/copr/testWhichDispatchPolicy", |exp| {
        let exp_str = exp.string();
        let act_str = act.as_str();
        assert!(
            act_str == exp_str,
            "tiflash_compute dispatch should be {exp_str}, but got {act_str}"
        );
    });
}

/// Go `buildBatchCopTasksCore` on classic TiFlash over the regions covering
/// each physical table's ranges (`rangesForEachPhysicalTable`), each region
/// tagged with its table's index. Go's `ttl` is zero at every caller (the
/// deprecated `tidb_mpp_store_fail_ttl`). Answers the tasks and the warnings
/// Go's `appendWarning` receives.
#[allow(clippy::too_many_arguments)]
async fn build_batch_cop_tasks_core<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    ranges_for_each_physical_table: &[Vec<(tidb_txnkv::Key, tidb_txnkv::Key)>],
    memory: &Arc<dyn MppContext>,
    is_mpp: bool,
    balance_with_continuity: bool,
    balance_continuous_region_count: usize,
    mut tiflash_replica_read_policy: ReplicaRead,
    kv_context: &tidb_txnkv::StatementKvContext,
) -> Result<(Vec<BatchCopTask>, Vec<String>), String> {
    let mut warnings = Vec::new();
    let pd = store.store().pd_client();
    let config = tidb_config::config_tree::config::get_global_config();
    let tidb_zone = config.labels.get(tidb_placement::DC_LABEL_KEY).cloned();
    if tidb_zone.is_none() {
        tiflash_replica_read_policy = ReplicaRead::AllReplicas;
    }
    let tidb_zone = tidb_zone.unwrap_or_default();
    let mut budget = RegionBackoffBudget::new(COP_BUILD_TASK_MAX_BACKOFF);
    let mut retry_num = 0;
    loop {
        retry_num += 1;
        let mut tasks_for_partitions = Vec::with_capacity(ranges_for_each_physical_table.len());
        for (i, ranges) in ranges_for_each_physical_table.iter().enumerate() {
            let mut located =
                crate::mpp_client::split_key_ranges_by_locations(store, ranges, memory.as_ref(), kv_context)?;
            for task in &mut located {
                task.partition_index = i as i64;
            }
            tasks_for_partitions.push(located);
        }
        let tasks: Vec<LocatedRegion> = if tasks_for_partitions.len() == 1 {
            tasks_for_partitions.pop().unwrap_or_default()
        } else {
            tasks_for_partitions.sort_by(|a, b| match (a.first(), b.first()) {
                (None, _) => std::cmp::Ordering::Less,
                (_, None) => std::cmp::Ordering::Greater,
                (Some(a), Some(b)) => first_start_key(a).cmp(first_start_key(b)),
            });
            // The ranges corresponding to each partiton do not intersect, so
            // we can merge tasks directly.
            tasks_for_partitions.into_iter().flatten().collect()
        };
        let mut rpc_stores = Vec::with_capacity(tasks.len());
        let mut used_tiflash_stores = Vec::with_capacity(tasks.len());
        let mut used_tiflash_stores_map = BTreeSet::new();
        let mut need_retry = false;
        let mut min_replica_num = u64::MAX;
        for task in &tasks {
            let selected = pd
                .get_tiflash_rpc_context(&task.region, is_mpp, TiFlashLabelFilter::NoWriteNode)
                .await
                .map_err(|error| error.to_string())?;
            // Go: a missing TiFlash route means the region moved or its TiFlash
            // stores crashed; every region is marked and located again.
            let Ok((_, selected_store)) = selected else {
                need_retry = true;
                continue;
            };
            let (all_stores, _) = pd
                .get_all_valid_tiflash_stores(
                    &task.region.ver_id(),
                    selected_store.id,
                    TiFlashLabelFilter::NoWriteNode,
                )
                .await;
            used_tiflash_stores_map.extend(all_stores.iter().copied());
            min_replica_num = min_replica_num.min(all_stores.len() as u64);
            rpc_stores.push(selected_store);
            used_tiflash_stores.push(all_stores);
        }

        let mut alive_stores = AliveStores::default();
        let mut max_remote_read_count_allowed = 0;
        if !need_retry {
            alive_stores = get_alive_stores_and_store_ids(
                store,
                memory,
                pd.get_tiflash_stores(TiFlashLabelFilter::NoWriteNode),
                &used_tiflash_stores_map,
                tiflash_replica_read_policy,
                &tidb_zone,
            )
            .await;
            max_remote_read_count_allowed = alive_stores.store_ids_in_tidb_zone.len()
                * MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS;
            let invalid = check_alive_store(
                &mut alive_stores,
                &used_tiflash_stores,
                &used_tiflash_stores_map,
                tiflash_replica_read_policy,
                retry_num,
                &tasks,
                min_replica_num,
                max_remote_read_count_allowed,
            );
            if !invalid.is_empty() {
                need_retry = true;
                // Go `handleInvalidRegions`.
                for region in invalid {
                    pd.invalidate_region_cache(region).await;
                }
            }
        }
        if need_retry {
            backoff_tiflash_rpc(&mut budget, memory.as_ref(), "Cannot find region with TiFlash peer".to_owned()).await?;
            continue;
        }

        let mut regions_in_other_zones = Vec::new();
        let mut all_region_infos = Vec::with_capacity(tasks.len());
        let mut store_task_map: BTreeMap<String, BatchCopTask> = BTreeMap::new();
        let mut store_ids_union = BTreeSet::new();
        for (index, task) in tasks.into_iter().enumerate() {
            let region_info = filter_accessible_stores_and_build_region_info(
                &used_tiflash_stores[index],
                task,
                &alive_stores,
                tiflash_replica_read_policy,
                &mut regions_in_other_zones,
                max_remote_read_count_allowed,
                &tidb_zone,
            )?;
            store_ids_union.extend(region_info.all_stores.iter().copied());
            let address = rpc_stores[index].address.clone();
            store_task_map
                .entry(address.clone())
                .or_insert_with(|| BatchCopTask {
                    store_id: rpc_stores[index].id,
                    store_addr: address,
                    region_infos: Vec::new(),
                })
                .region_infos
                .push(region_info.clone());
            all_region_infos.push(region_info);
        }
        if !regions_in_other_zones.is_empty() {
            let ids = regions_in_other_zones
                .iter()
                .take(3)
                .map(|id| format!("{id}, "))
                .collect::<String>();
            warnings.push(format!(
                "total {} region(s) can not be accessed by TiFlash in the zone [{tidb_zone}]:{ids}etc",
                regions_in_other_zones.len()
            ));
        }
        let batch_tasks: Vec<BatchCopTask> = store_task_map.into_values().collect();
        let stores_union: Vec<_> = alive_stores
            .stores_in_all_zones
            .iter()
            .filter(|store| store_ids_union.contains(&store.id))
            .cloned()
            .collect();
        let tasks = balance_batch_cop_task(
            &stores_union,
            batch_tasks,
            balance_with_continuity,
            balance_continuous_region_count,
            all_region_infos,
        );
        return Ok((tasks, warnings));
    }
}

/// Go `ranges.RefAt(0).StartKey`: a located region's first range start.
fn first_start_key(task: &LocatedRegion) -> &[u8] {
    task.info.ranges.first().map_or(&[], |range| range.start.as_slice())
}

/// Go `convertRegionInfosToPartitionTableRegions`: each task's regions by
/// physical table (empty tables dropped), in place of its region list.
pub(crate) fn convert_region_infos_to_partition_table_regions(
    batch_tasks: &mut [BatchCopTask],
    partition_ids: &[i64],
) -> Vec<Vec<tidb_proto::coprocessor::TableRegions>> {
    batch_tasks
        .iter_mut()
        .map(|cop_task| {
            let mut table_regions: Vec<tidb_proto::coprocessor::TableRegions> = partition_ids
                .iter()
                .map(|&pid| tidb_proto::coprocessor::TableRegions {
                    physical_table_id: pid,
                    ..Default::default()
                })
                .collect();
            for ri in std::mem::take(&mut cop_task.region_infos) {
                table_regions[ri.partition_index as usize].regions.push(ri.info);
            }
            table_regions.retain(|table| !table.regions.is_empty());
            table_regions
        })
        .collect()
}

/// The built tasks as their sender takes them: a partitioned build
/// (`partition_ids` not empty) runs Go's
/// `convertRegionInfosToPartitionTableRegions`, which leaves a task its
/// table regions in place of its region list.
pub(crate) fn store_tasks(
    mut tasks: Vec<BatchCopTask>,
    partition_ids: &[i64],
) -> Vec<tidb_txnkv::MppStoreTask> {
    let mut partition_table_regions = if partition_ids.is_empty() {
        vec![Vec::new(); tasks.len()]
    } else {
        convert_region_infos_to_partition_table_regions(&mut tasks, partition_ids)
    };
    tasks
        .into_iter()
        .zip(partition_table_regions.drain(..))
        .map(|(task, partition_table_regions)| tidb_txnkv::MppStoreTask {
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
        .collect()
}

/// Go `getAliveStoresAndStoreIDs`.
async fn get_alive_stores_and_store_ids<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    memory: &std::sync::Arc<dyn tidb_txnkv::MppContext>,
    all_tiflash_stores: Vec<tidb_proto::metapb::Store>,
    used: &BTreeSet<u64>,
    policy: ReplicaRead,
    tidb_zone: &str,
) -> AliveStores {
    // Go `getAllUsedTiFlashStores`.
    let used_stores: Vec<_> = all_tiflash_stores
        .into_iter()
        .filter(|store| used.contains(&store.id))
        .collect();
    let stores_in_all_zones =
        crate::mpp_client::filter_alive_stores(store, &used_stores, memory).await;
    let store_ids_in_all_zones = stores_in_all_zones.iter().map(|store| store.id).collect();
    let mut store_ids_in_tidb_zone = BTreeSet::new();
    if !policy.is_all_replicas() {
        for store in &stores_in_all_zones {
            // A TiFlash store with no zone label is in another zone.
            if store
                .labels
                .iter()
                .any(|label| label.key == tidb_placement::DC_LABEL_KEY && label.value == tidb_zone)
            {
                store_ids_in_tidb_zone.insert(store.id);
            }
        }
    }
    AliveStores {
        stores_in_all_zones,
        store_ids_in_all_zones,
        store_ids_in_tidb_zone,
    }
}

/// Go `checkAliveStore`: the regions with no alive TiFlash store, which are
/// invalidated and located again.
fn check_alive_store(
    alive_stores: &mut AliveStores,
    used_tiflash_stores: &[Vec<u64>],
    used_tiflash_stores_map: &BTreeSet<u64>,
    policy: ReplicaRead,
    retry_num: usize,
    tasks: &[LocatedRegion],
    min_replica_num: u64,
    max_remote_read_count_allowed: usize,
) -> Vec<tikv_client::RegionVerId> {
    if can_skip_check_alive_stores(
        alive_stores,
        used_tiflash_stores_map,
        policy,
        retry_num,
        min_replica_num,
    ) {
        return Vec::new();
    }
    let mut invalid = Vec::new();
    if alive_stores.store_ids_in_all_zones.is_empty() {
        invalid.extend(
            tasks
                .iter()
                .take(used_tiflash_stores.len())
                .map(|task| task.region.ver_id()),
        );
    } else if policy.is_closest_replicas() {
        let mut remote = Vec::new();
        for (index, stores) in used_tiflash_stores.iter().enumerate() {
            let local = stores
                .iter()
                .any(|id| alive_stores.store_ids_in_tidb_zone.contains(id));
            if local {
                continue;
            }
            if stores
                .iter()
                .any(|id| alive_stores.store_ids_in_all_zones.contains(id))
            {
                remote.push(tasks[index].region.ver_id());
            } else {
                invalid.push(tasks[index].region.ver_id());
            }
        }
        if remote.len() > max_remote_read_count_allowed {
            invalid.extend(remote);
        }
    } else {
        for (index, stores) in used_tiflash_stores.iter().enumerate() {
            if !stores
                .iter()
                .any(|id| alive_stores.store_ids_in_all_zones.contains(id))
            {
                invalid.push(tasks[index].region.ver_id());
            }
        }
    }
    invalid
}

/// Go `canSkipCheckAliveStores`, the fast path for `checkAliveStore`: if
/// minReplicaNum > deadStoreNum, there is at least one alive store for each
/// region, and the check is skipped.
fn can_skip_check_alive_stores(
    alive_stores: &mut AliveStores,
    used_tiflash_stores_map: &BTreeSet<u64>,
    policy: ReplicaRead,
    retry_num: usize,
    min_replica_num: u64,
) -> bool {
    let mut skip_check = tidb_intest::IN_TEST;
    if let Some(val) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/store/copr/mockNoAliveTiFlash") {
        skip_check = false;
        if val.bool() && retry_num <= 1 {
            alive_stores.stores_in_all_zones = Vec::new();
        }
    }
    if skip_check {
        return true;
    }

    if !policy.is_closest_replicas() {
        let dead_store_num = used_tiflash_stores_map
            .len()
            .saturating_sub(alive_stores.store_ids_in_all_zones.len());
        return min_replica_num > dead_store_num as u64;
    }

    // For closest_replica, need to recompute minReplicaNum and other infos,
    // which is duplicated with the logic in checkAliveStore(), so return false directly.
    false
}

/// Go `filterAllStoresAccordingToTiFlashReplicaRead`: the stores the policy
/// admits, and whether the region must be read across zones.
fn filter_all_stores_according_to_tiflash_replica_read(
    all_stores: &[u64],
    alive_stores: &AliveStores,
    policy: ReplicaRead,
) -> (Vec<u64>, bool) {
    if policy.is_all_replicas() {
        let matched = all_stores
            .iter()
            .copied()
            .filter(|id| alive_stores.store_ids_in_all_zones.contains(id))
            .collect();
        return (matched, false);
    }
    let mut matched: Vec<u64> = all_stores
        .iter()
        .copied()
        .filter(|id| alive_stores.store_ids_in_tidb_zone.contains(id))
        .collect();
    let mut needs_cross_zone_access = false;
    if matched.is_empty() {
        needs_cross_zone_access = true;
        if policy == ReplicaRead::ClosestAdaptive {
            matched = all_stores
                .iter()
                .copied()
                .filter(|id| alive_stores.store_ids_in_all_zones.contains(id))
                .collect();
        } else if policy == ReplicaRead::ClosestReplicas {
            matched = alive_stores
                .store_ids_in_tidb_zone
                .iter()
                .copied()
                .collect();
        }
    }
    (matched, needs_cross_zone_access)
}

/// Go `filterAccessibleStoresAndBuildRegionInfo`.
fn filter_accessible_stores_and_build_region_info(
    all_stores: &[u64],
    task: LocatedRegion,
    alive_stores: &AliveStores,
    policy: ReplicaRead,
    regions_in_other_zones: &mut Vec<u64>,
    max_remote_read_count_allowed: usize,
    tidb_zone: &str,
) -> Result<RegionInfo, String> {
    let (all_stores, needs_cross_zone_access) =
        filter_all_stores_according_to_tiflash_replica_read(all_stores, alive_stores, policy);
    if needs_cross_zone_access {
        regions_in_other_zones.push(task.region.region.id);
        if policy.is_closest_replicas()
            && regions_in_other_zones.len() > max_remote_read_count_allowed
        {
            let ids = regions_in_other_zones
                .iter()
                .take(3)
                .map(|id| format!("{id}, "))
                .collect::<String>();
            return Err(format!(
                "no less than {} region(s) can not be accessed by TiFlash in the zone [{tidb_zone}]: \
                 {ids}etc",
                regions_in_other_zones.len()
            ));
        }
    }
    Ok(RegionInfo {
        region: task.region,
        info: task.info,
        all_stores,
        partition_index: task.partition_index,
    })
}

/// Go `balanceScore`.
fn balance_score(max_region_count: usize, min_region_count: usize, continuous: usize) -> i64 {
    if min_region_count == 0 {
        return i64::from(i32::MIN);
    }
    let unbalance = max_region_count - min_region_count;
    if unbalance <= continuous {
        return MAX_BALANCE_SCORE;
    }
    MAX_BALANCE_SCORE - (unbalance * 100 / min_region_count) as i64
}

/// Go `isBalance`.
fn is_balance(score: i64) -> bool {
    score >= BALANCE_SCORE_THRESHOLD
}

/// Go `checkBatchCopTaskBalance`.
fn check_batch_cop_task_balance(
    store_tasks: &BTreeMap<u64, BatchCopTask>,
    continuous: usize,
) -> i64 {
    if store_tasks.is_empty() {
        return 0;
    }
    let counts = store_tasks.values().map(|task| task.region_infos.len());
    let max = counts.clone().max().unwrap_or(0);
    let min = counts.min().unwrap_or(0);
    balance_score(max, min, continuous)
}

/// Go `selectRegion`: up to `count` unselected candidates of `store_id`.
fn select_region(
    store_id: u64,
    candidates: &[RegionInfo],
    selected: &mut [bool],
    store_to_region_index: &mut BTreeMap<u64, Vec<usize>>,
    count: usize,
) -> Vec<RegionInfo> {
    let Some(indexes) = store_to_region_index.get_mut(&store_id) else {
        return Vec::new();
    };
    let mut buffer = Vec::with_capacity(count);
    let mut consumed = 0;
    while consumed < indexes.len() && buffer.len() < count {
        let index = indexes[consumed];
        consumed += 1;
        if selected[index] {
            continue;
        }
        selected[index] = true;
        buffer.push(candidates[index].clone());
    }
    indexes.drain(..consumed);
    buffer
}

/// Go `balanceBatchCopTaskWithContinuity`: each store takes runs of
/// `continuous` neighbouring candidates in key order, which TiFlash stores
/// close together. `None` when it does not apply or fails.
fn balance_batch_cop_task_with_continuity(
    store_task_map: &BTreeMap<u64, BatchCopTask>,
    candidates: &[RegionInfo],
    continuous: usize,
) -> Option<(Vec<BatchCopTask>, i64)> {
    if candidates.len() < 500 {
        return None;
    }
    let region_count = candidates.len()
        + store_task_map
            .values()
            .map(|t| t.region_infos.len())
            .sum::<usize>();
    let mut store_tasks = store_task_map.clone();
    let mut store_to_region_index: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    for (index, region) in candidates.iter().enumerate() {
        for store_id in &region.all_stores {
            store_to_region_index
                .entry(*store_id)
                .or_default()
                .push(index);
        }
    }
    let mut selected = vec![false; candidates.len()];
    loop {
        let mut total = 0;
        let mut selected_this_round = 0;
        for (store_id, task) in &mut store_tasks {
            let picked = select_region(
                *store_id,
                candidates,
                &mut selected,
                &mut store_to_region_index,
                continuous,
            );
            selected_this_round += picked.len();
            task.region_infos.extend(picked);
            total += task.region_infos.len();
        }
        if total >= region_count {
            break;
        }
        if selected_this_round == 0 {
            return None;
        }
    }
    let score = check_batch_cop_task_balance(&store_tasks, continuous);
    let total: usize = store_tasks.values().map(|t| t.region_infos.len()).sum();
    if total != region_count {
        return None;
    }
    let tasks = store_tasks
        .into_values()
        .filter(|task| !task.region_infos.is_empty())
        .collect();
    Some((tasks, score))
}

/// Go `balanceBatchCopTask`: regions with one alive store stay on it; the
/// others are spread across the alive stores that hold them, by the
/// continuity strategy when it balances and the greedy weighted one
/// otherwise. Any inconsistency keeps `original_tasks`.
pub(crate) fn balance_batch_cop_task(
    alive_stores: &[tidb_proto::metapb::Store],
    original_tasks: Vec<BatchCopTask>,
    balance_with_continuity: bool,
    continuous: usize,
    all_region_infos: Vec<RegionInfo>,
) -> Vec<BatchCopTask> {
    if original_tasks.is_empty() {
        return original_tasks;
    }
    let mut store_task_map: BTreeMap<u64, BatchCopTask> = alive_stores
        .iter()
        .map(|store| {
            (
                store.id,
                BatchCopTask {
                    store_id: store.id,
                    store_addr: store.address.clone(),
                    region_infos: Vec::new(),
                },
            )
        })
        .collect();
    // Store -> region key -> region: every store a candidate region may use.
    let mut store_candidate_region_map: BTreeMap<u64, BTreeMap<(u64, u64, u64), RegionInfo>> =
        BTreeMap::new();
    let mut total_region_candidate_num = 0usize;
    let mut total_remaining_region_num = 0usize;
    let mut candidates = Vec::with_capacity(all_region_infos.len());
    for region in all_region_infos {
        let valid: Vec<u64> = region
            .all_stores
            .iter()
            .copied()
            .filter(|id| store_task_map.contains_key(id))
            .collect();
        match valid.as_slice() {
            [] => return original_tasks,
            [only] => store_task_map
                .get_mut(only)
                .expect("a valid store has a task")
                .region_infos
                .push(region),
            _ => {
                total_region_candidate_num += valid.len();
                total_remaining_region_num += 1;
                let key = region_key(&region);
                for store_id in &valid {
                    let map = store_candidate_region_map.entry(*store_id).or_default();
                    if map.insert(key, region.clone()).is_some() {
                        // A duplicated region: give up balancing, as Go does.
                        return original_tasks;
                    }
                }
                candidates.push(region);
            }
        }
    }

    let mut contiguous = None;
    if balance_with_continuity {
        contiguous =
            balance_batch_cop_task_with_continuity(&store_task_map, &candidates, continuous);
        if let Some((tasks, score)) = &contiguous {
            if is_balance(*score) {
                return tasks.clone();
            }
        }
    }

    if total_remaining_region_num > 0 {
        let mut avg_store_per_region =
            total_region_candidate_num as f64 / total_remaining_region_num as f64;
        let find_next_store =
            |candidate_stores: Option<&[u64]>,
             store_candidate_region_map: &BTreeMap<u64, BTreeMap<(u64, u64, u64), RegionInfo>>,
             store_task_map: &BTreeMap<u64, BatchCopTask>,
             avg_store_per_region: f64|
             -> Option<u64> {
                let weight = |store_id: &u64| -> Option<f64> {
                    let candidates = store_candidate_region_map.get(store_id)?;
                    Some(
                        candidates.len() as f64 / avg_store_per_region
                            + store_task_map[store_id].region_infos.len() as f64,
                    )
                };
                let lightest = |ids: &mut dyn Iterator<Item = &u64>| {
                    let mut best: Option<(u64, f64)> = None;
                    for id in ids {
                        if let Some(weight) = weight(id) {
                            if best.is_none_or(|(_, current)| weight < current) {
                                best = Some((*id, weight));
                            }
                        }
                    }
                    best.map(|(id, _)| id)
                };
                if let Some(candidate_stores) = candidate_stores {
                    if let Some(store) = lightest(&mut candidate_stores.iter()) {
                        return Some(store);
                    }
                }
                // Go ranges over `storeTaskMap`, a map: no store is always
                // the first of the equally light ones, so a task rebuilt
                // after its store failed does not keep landing on that store.
                let stores: Vec<&u64> = store_task_map.keys().collect();
                let first = tidb_fastrand::uint64_n(stores.len() as u64) as usize;
                lightest(&mut stores.iter().cycle().skip(first).take(stores.len()).copied())
            };
        let mut store = find_next_store(
            None,
            &store_candidate_region_map,
            &store_task_map,
            avg_store_per_region,
        );
        while total_remaining_region_num > 0 {
            let Some(store_id) = store else {
                break;
            };
            let Some((key, region)) = store_candidate_region_map
                .get(&store_id)
                .and_then(|map| map.iter().next())
                .map(|(key, region)| (*key, region.clone()))
            else {
                break;
            };
            total_remaining_region_num -= 1;
            for id in &region.all_stores {
                if let Some(map) = store_candidate_region_map.get_mut(id) {
                    if map.remove(&key).is_some() {
                        total_region_candidate_num -= 1;
                    }
                    if map.is_empty() {
                        store_candidate_region_map.remove(id);
                    }
                }
            }
            let affected = region.all_stores.clone();
            store_task_map
                .get_mut(&store_id)
                .expect("a candidate store has a task")
                .region_infos
                .push(region);
            if total_remaining_region_num > 0 {
                avg_store_per_region =
                    total_region_candidate_num as f64 / total_remaining_region_num as f64;
                // Go checks only the stores the region affected.
                store = find_next_store(
                    Some(&affected),
                    &store_candidate_region_map,
                    &store_task_map,
                    avg_store_per_region,
                );
            }
        }
        if total_remaining_region_num > 0 {
            return original_tasks;
        }
    }

    if let Some((tasks, _)) = contiguous {
        if !is_balance(check_batch_cop_task_balance(&store_task_map, continuous)) {
            return tasks;
        }
    }
    store_task_map
        .into_values()
        .filter(|task| !task.region_infos.is_empty())
        .collect()
}

/// Go `RegionVerID.String()` as `balanceBatchCopTask`'s region key.
fn region_key(region: &RegionInfo) -> (u64, u64, u64) {
    let ver = region.region.ver_id();
    (ver.id, ver.conf_ver, ver.ver)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(id: u64) -> tidb_proto::metapb::Store {
        tidb_proto::metapb::Store {
            id,
            address: format!("tiflash-{id}:3930"),
            ..Default::default()
        }
    }

    fn region(id: u64, all_stores: Vec<u64>) -> RegionInfo {
        RegionInfo {
            region: tikv_client::RegionWithLeader {
                region: tidb_proto::metapb::Region {
                    id,
                    region_epoch: Some(tidb_proto::metapb::RegionEpoch {
                        conf_ver: 1,
                        version: 1,
                    }),
                    ..Default::default()
                },
                ..Default::default()
            },
            info: tidb_proto::coprocessor::RegionInfo {
                region_id: id,
                ..Default::default()
            },
            all_stores,
            partition_index: 0,
        }
    }

    fn task(store_id: u64, regions: Vec<RegionInfo>) -> BatchCopTask {
        BatchCopTask {
            store_id,
            store_addr: format!("tiflash-{store_id}:3930"),
            region_infos: regions,
        }
    }

    fn counts(tasks: &[BatchCopTask]) -> BTreeMap<String, usize> {
        tasks
            .iter()
            .map(|task| (task.store_addr.clone(), task.region_infos.len()))
            .collect()
    }

    fn alive(all_zones: &[u64], tidb_zone: &[u64]) -> AliveStores {
        AliveStores {
            stores_in_all_zones: Vec::new(),
            store_ids_in_all_zones: all_zones.iter().copied().collect(),
            store_ids_in_tidb_zone: tidb_zone.iter().copied().collect(),
        }
    }

    /// Go `TestAliveStoreSkipCheck`.
    #[test]
    fn test_alive_store_skip_check() {
        let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/store/copr/mockNoAliveTiFlash", "return(false)");
        let used_tiflash_stores_map = BTreeSet::from([1, 2, 3]);
        let can_skip = |alive_stores: &mut AliveStores, policy, min_replica_num| {
            can_skip_check_alive_stores(alive_stores, &used_tiflash_stores_map, policy, 2, min_replica_num)
        };

        {
            // Non closest_replica; min replica num is 1.
            let mut alive_stores = alive(&[1, 2, 3], &[1]);
            // 1, 2, 3 is alive, can skip check.
            assert!(can_skip(&mut alive_stores, ReplicaRead::ClosestAdaptive, 1));
            assert!(can_skip(&mut alive_stores, ReplicaRead::AllReplicas, 1));

            // 1, 2 is alive, cannot skip check.
            alive_stores.store_ids_in_all_zones = BTreeSet::from([1, 2]);
            assert!(!can_skip(&mut alive_stores, ReplicaRead::ClosestAdaptive, 1));
            assert!(!can_skip(&mut alive_stores, ReplicaRead::AllReplicas, 1));
        }

        {
            // Non closest_replica; min replica num is 2.
            // 1, 2, 3 is alive, can skip check.
            let mut alive_stores = alive(&[1, 2, 3], &[1, 2, 3]);
            assert!(can_skip(&mut alive_stores, ReplicaRead::ClosestAdaptive, 2));
            assert!(can_skip(&mut alive_stores, ReplicaRead::AllReplicas, 2));

            // 1, 2 is alive, can skip check.
            alive_stores.store_ids_in_all_zones = BTreeSet::from([1, 2]);
            assert!(can_skip(&mut alive_stores, ReplicaRead::ClosestAdaptive, 2));
            assert!(can_skip(&mut alive_stores, ReplicaRead::AllReplicas, 2));
        }

        for min_replica_num in [1, 2] {
            // closest_replica(always need check).
            for tidb_zone in [&[1][..], &[1, 2], &[1, 2, 3]] {
                let mut alive_stores = alive(&[1, 2, 3], tidb_zone);
                assert!(!can_skip(&mut alive_stores, ReplicaRead::ClosestReplicas, min_replica_num));
            }
        }
    }

    /// Go `TestCheckAliveStore`.
    #[test]
    fn test_check_alive_store() {
        let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/store/copr/mockNoAliveTiFlash", "return(false)");
        let used_tiflash_stores_map = BTreeSet::from([1, 2, 3]);
        let used_tiflash_stores = [
            vec![1, 2], // region-1
            vec![2, 3], // region-2
            vec![3],    // region-3
        ];
        let tasks: Vec<LocatedRegion> = (1..=3)
            .map(|id| {
                let RegionInfo { region, info, .. } = region(id, Vec::new());
                LocatedRegion {
                    region,
                    info,
                    partition_index: 0,
                }
            })
            .collect();
        let min_replica_num = 0;
        let check = |mut alive_stores: AliveStores, max_allowed_remote| {
            check_alive_store(
                &mut alive_stores,
                &used_tiflash_stores,
                &used_tiflash_stores_map,
                ReplicaRead::ClosestReplicas,
                2,
                &tasks,
                min_replica_num,
                max_allowed_remote,
            )
            .into_iter()
            .map(|region| region.id)
            .collect::<Vec<_>>()
        };

        // Test closest_replica. 2 remote region, 1 tidb zone region.
        assert_eq!(check(alive(&[1, 2, 3], &[1]), 1), [2, 3]);
        assert_eq!(check(alive(&[1, 2, 3], &[1]), 3), [0_u64; 0]);
        // Store 3 is not alive: region-3 has no alive store.
        assert_eq!(check(alive(&[1, 2], &[1]), 3), [3]);
        assert_eq!(check(alive(&[], &[]), 3), [1, 2, 3]);
    }

    /// Go `TestBalanceBatchCopTaskWithEmptyTaskSet`.
    #[test]
    fn an_empty_task_set_stays_empty() {
        assert!(balance_batch_cop_task(&[store(1)], Vec::new(), false, 20, Vec::new()).is_empty());
    }

    /// Every region reaches one of its own stores, and regions with two
    /// replicas spread evenly across both.
    #[test]
    fn regions_are_balanced_across_the_stores_that_hold_them() {
        let regions: Vec<_> = (1..=10).map(|id| region(id, vec![1, 2])).collect();
        let original = vec![task(1, regions.clone())];
        let tasks = balance_batch_cop_task(&[store(1), store(2)], original, false, 20, regions);
        assert_eq!(
            counts(&tasks),
            BTreeMap::from([
                ("tiflash-1:3930".to_owned(), 5),
                ("tiflash-2:3930".to_owned(), 5)
            ])
        );
    }

    /// A region whose only replica is on one store goes there.
    #[test]
    fn a_region_with_one_alive_store_stays_on_it() {
        let regions = vec![region(1, vec![2]), region(2, vec![1, 2])];
        let original = vec![task(2, regions.clone())];
        let tasks = balance_batch_cop_task(&[store(1), store(2)], original, false, 20, regions);
        let placed: BTreeMap<u64, String> = tasks
            .iter()
            .flat_map(|task| {
                task.region_infos
                    .iter()
                    .map(|region| (region.region.region.id, task.store_addr.clone()))
            })
            .collect();
        assert_eq!(placed[&1], "tiflash-2:3930");
        assert_eq!(placed.len(), 2);
    }

    /// Go gives up balancing when a region has no alive store.
    #[test]
    fn a_region_without_an_alive_store_keeps_the_original_tasks() {
        let regions = vec![region(1, vec![3])];
        let original = vec![task(3, regions.clone())];
        let tasks = balance_batch_cop_task(&[store(1)], original.clone(), false, 20, regions);
        assert_eq!(tasks, original);
    }

    /// Go `balanceScore`.
    #[test]
    fn balance_scores_follow_go() {
        assert_eq!(balance_score(10, 0, 20), i64::from(i32::MIN));
        assert_eq!(balance_score(30, 15, 20), MAX_BALANCE_SCORE);
        assert_eq!(balance_score(150, 100, 20), 50);
        assert!(is_balance(85));
        assert!(!is_balance(84));
    }

    /// Go `filterAllStoresAccordingToTiFlashReplicaRead`.
    #[test]
    fn replica_read_policies_filter_stores_as_go_does() {
        let alive = AliveStores {
            stores_in_all_zones: vec![store(1), store(2), store(3)],
            store_ids_in_all_zones: BTreeSet::from([1, 2, 3]),
            store_ids_in_tidb_zone: BTreeSet::from([3]),
        };
        assert_eq!(
            filter_all_stores_according_to_tiflash_replica_read(
                &[1, 2, 4],
                &alive,
                ReplicaRead::AllReplicas
            ),
            (vec![1, 2], false)
        );
        assert_eq!(
            filter_all_stores_according_to_tiflash_replica_read(
                &[1, 2],
                &alive,
                ReplicaRead::ClosestAdaptive
            ),
            (vec![1, 2], true)
        );
        assert_eq!(
            filter_all_stores_according_to_tiflash_replica_read(
                &[1, 2],
                &alive,
                ReplicaRead::ClosestReplicas
            ),
            (vec![3], true)
        );
        assert_eq!(
            filter_all_stores_according_to_tiflash_replica_read(
                &[3, 1],
                &alive,
                ReplicaRead::ClosestReplicas
            ),
            (vec![3], false)
        );
    }
}
