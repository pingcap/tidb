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

//! Go `MockPD`'s region half (`tikv/mock_region.go`), the UNSPLIT slice: the
//! answer every region query gets from a freshly bootstrapped single-store
//! cluster — one region covering the whole keyspace, one store, one leader
//! peer.
//!
//! Paired with [`crate::client::InProcessClient`], this completes the read
//! transport's generic pair: `DirectUnaryQueryTransport<C, R>` can now be
//! instantiated with no network under either parameter.
//!
//! # Narrowings, by name
//!
//! * `MockRegionManager.SplitRegion` / `SplitTable` and everything epoch:
//!   splitting arrives with the region course; until then the epoch is the
//!   bootstrap `(1, 1)` and can never mismatch — exactly the unsplit store
//!   Go boots.
//! * Store labels, buckets, down/pending peers: none exist in-process.

use tidb_txnkv::region::{
    BatchLoadOptions, BatchRegionLoader, KeyRange, Peer, PeerRole, RegionLoadError, RegionLoader,
    RegionLocation, RegionMetadata, RegionQuery, RegionQueryLoader, RegionQueryOptions,
    RegionRecoveryLoader, RegionVerId, Store, StoreMetadata,
};

use crate::client::IN_PROCESS_ADDRESS;

/// The bootstrap identities: cluster 1, region 1 at epoch `(1, 1)`, store 1,
/// peer 1 — the ids Go's bootstrap hands the first region.
pub const IN_PROCESS_CLUSTER_ID: u64 = 1;
/// See [`IN_PROCESS_CLUSTER_ID`].
pub const IN_PROCESS_REGION_ID: u64 = 1;
/// See [`IN_PROCESS_CLUSTER_ID`].
pub const IN_PROCESS_STORE_ID: u64 = 1;
/// See [`IN_PROCESS_CLUSTER_ID`].
pub const IN_PROCESS_PEER_ID: u64 = 1;

/// The single-region answer, freshly built per query as Go's manager copies
/// its region out under lock.
fn whole_keyspace_region() -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(IN_PROCESS_REGION_ID, 1, 1),
        start_key: Vec::new(),
        end_key: Vec::new(),
        peers: vec![Peer {
            id: IN_PROCESS_PEER_ID,
            store_id: IN_PROCESS_STORE_ID,
            role: PeerRole::Voter,
            is_witness: false,
            store_epoch: 1,
        }],
        leader_peer_id: Some(IN_PROCESS_PEER_ID),
        stores: vec![in_process_store()],
        buckets: None,
        down_peer_ids: Vec::new(),
        pending_peer_ids: Vec::new(),
    }
}

fn in_process_store() -> Store {
    Store {
        id: IN_PROCESS_STORE_ID,
        address: IN_PROCESS_ADDRESS.to_owned(),
        epoch: 1,
    }
}

/// The in-process region control plane.
#[derive(Clone, Copy, Debug, Default)]
pub struct InProcessRegionLoader;

impl RegionLoader for InProcessRegionLoader {
    fn cluster_id(&self) -> u64 {
        IN_PROCESS_CLUSTER_ID
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        Ok(whole_keyspace_region())
    }
}

impl BatchRegionLoader for InProcessRegionLoader {
    fn batch_load_regions(
        &mut self,
        _ranges: &[KeyRange],
        _limit: usize,
        _options: BatchLoadOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        Ok(vec![whole_keyspace_region()])
    }
}

impl RegionQueryLoader for InProcessRegionLoader {
    fn query_region(
        &mut self,
        _query: RegionQuery<'_>,
        _options: RegionQueryOptions,
    ) -> Result<RegionLocation, RegionLoadError> {
        Ok(whole_keyspace_region())
    }

    fn scan_regions_once(
        &mut self,
        _range: &KeyRange,
        _limit: usize,
        _options: RegionQueryOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        Ok(vec![whole_keyspace_region()])
    }

    fn load_store(&mut self, store_id: u64) -> Result<Option<StoreMetadata>, RegionLoadError> {
        if store_id == IN_PROCESS_STORE_ID {
            Ok(Some(StoreMetadata {
                id: IN_PROCESS_STORE_ID,
                address: IN_PROCESS_ADDRESS.to_owned(),
                labels: Vec::new(),
            }))
        } else {
            // An unknown store is Go's tombstone answer.
            Ok(None)
        }
    }
}

impl RegionRecoveryLoader for InProcessRegionLoader {
    fn hydrate_region(
        &mut self,
        _metadata: &RegionMetadata,
        _leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        Ok(whole_keyspace_region())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // WRITTEN: mock_region.go's coverage rides the store suites.

    #[test]
    fn every_key_routes_to_the_one_region_and_its_one_store() {
        let mut loader = InProcessRegionLoader;
        let location = loader.load_region(b"anything").expect("routes");
        assert_eq!(location.region.id, IN_PROCESS_REGION_ID);
        assert!(location.start_key.is_empty() && location.end_key.is_empty());
        assert_eq!(location.leader_peer_id, Some(IN_PROCESS_PEER_ID));
        assert_eq!(location.stores[0].address, IN_PROCESS_ADDRESS);
        let store = loader
            .load_store(IN_PROCESS_STORE_ID)
            .expect("loads")
            .expect("exists");
        assert_eq!(store.address, IN_PROCESS_ADDRESS);
        assert!(loader.load_store(99).expect("loads").is_none(), "tombstone");
    }
}
