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

//! Go `pkg/store/mockstore/unistore/cluster.go`: the test cluster over the
//! embedded store's region manager -- request delays a test schedules, raw
//! splits, and the bootstrap shapes tests start from.
//!
//! # Narrowings, by name
//!
//! * `SplitKeys` (`MockRegionManager.SplitKeys` walks the store's data to
//!   pick even split points): the region manager does not hold the store.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_proto::metapb;

use tidb_unistore_tikv::region_manager::{encode_region_key, new_peer_meta, InProcessRegionManager};

/// Go `Cluster`: the region manager plus the delays tests schedule.
#[derive(Debug)]
pub struct Cluster {
    rm: Arc<InProcessRegionManager>,
    /// Go `delayEvents`: `(start ts, region id)` -> delay.
    delay_events: Mutex<HashMap<(u64, u64), Duration>>,
}

impl Cluster {
    /// Go `newCluster`.
    #[must_use]
    pub fn new(rm: Arc<InProcessRegionManager>) -> Self {
        Self {
            rm,
            delay_events: Mutex::new(HashMap::new()),
        }
    }

    /// The region manager beneath.
    #[must_use]
    pub fn region_manager(&self) -> &Arc<InProcessRegionManager> {
        &self.rm
    }

    /// Go `ScheduleDelay`: the next request of transaction `start_ts` to
    /// region `region_id` waits `dur` first.
    pub fn schedule_delay(&self, start_ts: u64, region_id: u64, dur: Duration) {
        self.delay_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((start_ts, region_id), dur);
    }

    /// Go `handleDelay`, the lookup half: the scheduled delay, consumed. The
    /// RPC client sleeps it.
    pub(crate) fn take_delay(&self, start_ts: u64, region_id: u64) -> Option<Duration> {
        self.delay_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&(start_ts, region_id))
    }

    /// Go `Cluster.SplitRaw`: splits at a raw key, encoded first.
    pub fn split_raw(
        &self,
        region_id: u64,
        new_region_id: u64,
        raw_key: &[u8],
        peer_ids: &[u64],
        leader_peer_id: u64,
    ) -> metapb::Region {
        self.rm.split_raw(
            region_id,
            new_region_id,
            &encode_region_key(raw_key),
            peer_ids,
            leader_peer_id,
        )
    }
}

impl std::ops::Deref for Cluster {
    type Target = InProcessRegionManager;

    fn deref(&self) -> &InProcessRegionManager {
        &self.rm
    }
}

/// Go `unistore.New`'s region manager: `NewMockRegionManager` followed by
/// `BootstrapWithSingleStore` -- store 1 (`store1`), region 2 over the whole
/// keyspace, peer 3.
#[must_use]
pub fn new_region_manager() -> InProcessRegionManager {
    let manager = InProcessRegionManager::new_mock_region_manager();
    let (store_id, peer_id, region_id) = bootstrap_with_single_store(&manager);
    debug_assert_eq!(
        (store_id, region_id, peer_id),
        (
            tidb_unistore_tikv::region_manager::STORE_ID,
            tidb_unistore_tikv::region_manager::BOOTSTRAP_REGION_ID,
            tidb_unistore_tikv::region_manager::BOOTSTRAP_PEER_ID
        )
    );
    manager
}

/// Go `BootstrapWithSingleStore` (`unistore/cluster.go`): one store named
/// `store<id>`, one region over the whole keyspace with one peer on it.
/// Answers `(store_id, peer_id, region_id)`.
pub fn bootstrap_with_single_store(cluster: &InProcessRegionManager) -> (u64, u64, u64) {
    let store_id = cluster.alloc_id();
    let region_id = cluster.alloc_id();
    let peer_id = cluster.alloc_id();
    let store = metapb::Store {
        id: store_id,
        address: format!("store{store_id}"),
        ..metapb::Store::default()
    };
    let region = metapb::Region {
        id: region_id,
        region_epoch: Some(metapb::RegionEpoch {
            conf_ver: 1,
            version: 1,
        }),
        peers: vec![new_peer_meta(peer_id, store_id)],
        ..metapb::Region::default()
    };
    cluster.bootstrap(vec![store], region);
    (store_id, peer_id, region_id)
}

/// Go `BootstrapWithMultiStores` (`unistore/cluster.go`): `n` stores, one
/// region with a peer on each, the first peer leading. Answers
/// `(store_ids, peer_ids, region_id, leader_peer)`.
pub fn bootstrap_with_multi_stores(
    cluster: &InProcessRegionManager,
    n: usize,
) -> (Vec<u64>, Vec<u64>, u64, u64) {
    let store_ids = cluster.alloc_ids(n);
    let peer_ids = cluster.alloc_ids(n);
    let leader_peer = peer_ids[0];
    let region_id = cluster.alloc_id();
    let stores = store_ids
        .iter()
        .map(|id| metapb::Store {
            id: *id,
            address: format!("store{id}"),
            ..metapb::Store::default()
        })
        .collect();
    let peers = peer_ids
        .iter()
        .zip(&store_ids)
        .map(|(peer_id, store_id)| new_peer_meta(*peer_id, *store_id))
        .collect();
    let region = metapb::Region {
        id: region_id,
        region_epoch: Some(metapb::RegionEpoch {
            conf_ver: 1,
            version: 1,
        }),
        peers,
        ..metapb::Region::default()
    };
    cluster.bootstrap(stores, region);
    (store_ids, peer_ids, region_id, leader_peer)
}

/// Go `BootstrapWithMultiRegions` (`unistore/cluster.go`): one store and
/// `split_keys.len() + 1` regions cut at the raw `split_keys`. Answers
/// `(store_id, region_ids, peer_ids)`.
pub fn bootstrap_with_multi_regions(
    cluster: &InProcessRegionManager,
    split_keys: &[Vec<u8>],
) -> (u64, Vec<u64>, Vec<u64>) {
    let (store_id, first_peer_id, first_region_id) = bootstrap_with_single_store(cluster);
    let mut region_ids = vec![first_region_id];
    region_ids.extend(cluster.alloc_ids(split_keys.len()));
    let mut peer_ids = vec![first_peer_id];
    peer_ids.extend(cluster.alloc_ids(split_keys.len()));
    for (i, key) in split_keys.iter().enumerate() {
        // Go passes the left region's peer id as the new region's peer.
        cluster.split(
            region_ids[i],
            region_ids[i + 1],
            key,
            &[peer_ids[i]],
            peer_ids[i],
        );
    }
    (store_id, region_ids, peer_ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `ScheduleDelay` / `handleDelay`: a scheduled delay is taken once.
    #[test]
    fn a_scheduled_delay_is_taken_once() {
        let cluster = Cluster::new(Arc::new(crate::cluster::new_region_manager()));
        cluster.schedule_delay(10, 2, Duration::from_millis(5));
        assert_eq!(cluster.take_delay(10, 3), None);
        assert_eq!(cluster.take_delay(10, 2), Some(Duration::from_millis(5)));
        assert_eq!(cluster.take_delay(10, 2), None);
    }

    /// Go `Cluster.SplitRaw` encodes the raw key before splitting.
    #[test]
    fn split_raw_encodes_the_key() {
        let cluster = Cluster::new(Arc::new(crate::cluster::new_region_manager()));
        let ids = cluster.alloc_ids(2);
        let right = cluster.split_raw(2, ids[0], b"m", &[ids[1]], ids[1]);
        assert_eq!(right.start_key, encode_region_key(b"m"));
        assert_eq!(right.peers[0].id, ids[1]);
    }

    /// Go `BootstrapWithMultiRegions` and `BootstrapWithMultiStores`.
    #[test]
    fn bootstraps_multi_region_and_multi_store_clusters() {
        let manager = InProcessRegionManager::new_mock_region_manager();
        let (store_id, region_ids, peer_ids) =
            bootstrap_with_multi_regions(&manager, &[b"b".to_vec(), b"d".to_vec()]);
        assert_eq!(
            (store_id, region_ids, peer_ids),
            (1, vec![2, 4, 5], vec![3, 6, 7])
        );
        let ranges: Vec<_> = manager
            .scan_regions(b"", b"", 0)
            .iter()
            .map(|(region, leader)| (region.id, leader.id))
            .collect();
        assert_eq!(ranges, vec![(2, 3), (4, 3), (5, 6)]);

        let manager = InProcessRegionManager::new_mock_region_manager();
        let (store_ids, peer_ids, region_id, leader) = bootstrap_with_multi_stores(&manager, 3);
        assert_eq!(store_ids, vec![1, 2, 3]);
        assert_eq!(peer_ids, vec![4, 5, 6]);
        assert_eq!((region_id, leader), (7, 4));
        assert_eq!(manager.get_all_stores().len(), 3);
        let (region, _) = manager.get_region_by_key(&encode_region_key(b"")).unwrap();
        assert_eq!(region.peers.len(), 3);
    }
}
