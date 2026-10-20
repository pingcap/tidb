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
//! * Disaggregated TiFlash (`buildBatchCopTasksConsistentHash`) lives in
//!   `tiflash_mpp_scan`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::retry::{RegionBackoffBudget, RegionBackoffKind};
use tidb_txnkv::{ReplicaRead, MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS};
use tikv_client::TiFlashLabelFilter;

/// Go `copBuildTaskMaxBackoff`.
const COP_BUILD_TASK_MAX_BACKOFF: Duration = Duration::from_millis(5_000);

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
}

/// Go `batchCopTask`: the regions one TiFlash store serves.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BatchCopTask {
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
}

/// Go `buildBatchCopTasksCore` for one physical table on classic TiFlash
/// (`isMPP`, `balanceWithContinuity`, `balanceContinuousRegionCount = 20`)
/// over the regions covering `ranges`. Answers the tasks and the warnings
/// Go's `appendWarning` receives.
pub(crate) async fn build_batch_cop_tasks_core<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    ranges: &[(tidb_txnkv::Key, tidb_txnkv::Key)],
    memory: &tidb_executor::StatementMemory,
    mut tiflash_replica_read_policy: ReplicaRead,
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
    loop {
        let tasks = crate::tiflash_mpp_scan::locate_mpp_regions(store, ranges, memory)?;
        let mut rpc_stores = Vec::with_capacity(tasks.len());
        let mut used_tiflash_stores = Vec::with_capacity(tasks.len());
        let mut used_tiflash_stores_map = BTreeSet::new();
        let mut need_retry = false;
        let mut min_replica_num = u64::MAX;
        for task in &tasks {
            let selected = pd
                .get_tiflash_rpc_context(&task.region, true, TiFlashLabelFilter::NoWriteNode)
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
                &alive_stores,
                &used_tiflash_stores,
                &used_tiflash_stores_map,
                tiflash_replica_read_policy,
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
            let delay = budget
                .next_delay(RegionBackoffKind::TiFlashRpc)
                .map_err(|_| "Cannot find region with TiFlash peer".to_owned())?;
            let waited = crate::tiflash_mpp_scan::mpp_retry_wait(memory, delay).await;
            budget.finish_wait(waited.is_ok());
            waited?;
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
            true,
            BALANCE_CONTINUOUS_REGION_COUNT,
            all_region_infos,
        );
        return Ok((tasks, warnings));
    }
}

/// Go `getAliveStoresAndStoreIDs`.
async fn get_alive_stores_and_store_ids<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    memory: &tidb_executor::StatementMemory,
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
        crate::tiflash_mpp_scan::filter_alive_stores(store, &used_stores, memory).await;
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
    alive_stores: &AliveStores,
    used_tiflash_stores: &[Vec<u64>],
    used_tiflash_stores_map: &BTreeSet<u64>,
    policy: ReplicaRead,
    tasks: &[LocatedRegion],
    min_replica_num: u64,
    max_remote_read_count_allowed: usize,
) -> Vec<tikv_client::RegionVerId> {
    // Go `canSkipCheckAliveStores`: every region keeps an alive replica when
    // fewer stores died than the smallest replica count.
    if !policy.is_closest_replicas() {
        let dead = used_tiflash_stores_map
            .len()
            .saturating_sub(alive_stores.store_ids_in_all_zones.len());
        if min_replica_num > dead as u64 {
            return Vec::new();
        }
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
                lightest(&mut store_task_map.keys())
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
        }
    }

    fn task(store_id: u64, regions: Vec<RegionInfo>) -> BatchCopTask {
        BatchCopTask {
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
