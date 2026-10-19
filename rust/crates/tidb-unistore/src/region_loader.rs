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

//! Go `MockPD`'s region half (`tikv/mock_region.go`) as client-go's codec PD
//! client sees it: every region query the embedded store's region cache
//! asks, with raw keys encoded on the way in and region bounds decoded on
//! the way out, answered from the store's [`InProcessRegionManager`].
//!
//! Paired with [`crate::client::InProcessClient`], this completes the read
//! transport's generic pair: `DirectUnaryQueryTransport<C, R>` can now be
//! instantiated with no network under either parameter.
//!
//! # Narrowings, by name
//!
//! * Buckets and down/pending peers: none exist in-process.
//! * An empty end-key query answers the last region; Go's
//!   `GetRegionByEndKey` answers none for it.

use std::sync::Arc;

use tidb_proto::metapb;
use tidb_txnkv::region::{
    BatchLoadOptions, BatchRegionLoader, KeyRange, Peer, PeerRole, RegionLoadError, RegionLoader,
    RegionLocation, RegionMetadata, RegionQuery, RegionQueryLoader, RegionQueryOptions,
    RegionRecoveryLoader, RegionVerId, Store, StoreMetadata,
};

use crate::region_manager::encode_region_key;
pub use crate::region_manager::InProcessRegionManager;

/// The cluster id Go's bootstrap uses.
pub const IN_PROCESS_CLUSTER_ID: u64 = 1;
/// The bootstrap region (Go `BootstrapWithSingleStore`'s second `AllocID`).
pub const IN_PROCESS_REGION_ID: u64 = crate::region_manager::BOOTSTRAP_REGION_ID;
/// The one store.
pub const IN_PROCESS_STORE_ID: u64 = crate::region_manager::STORE_ID;
/// The bootstrap region's peer.
pub const IN_PROCESS_PEER_ID: u64 = crate::region_manager::BOOTSTRAP_PEER_ID;

/// The in-process region control plane over the store's region manager.
#[derive(Clone, Debug, Default)]
pub struct InProcessRegionLoader {
    manager: Arc<InProcessRegionManager>,
}

/// client-go's `decodeRegionMetaKeyInPlace`: an empty bound stays empty.
fn decode_region_key(key: &[u8]) -> Result<Vec<u8>, RegionLoadError> {
    if key.is_empty() {
        return Ok(Vec::new());
    }
    tidb_codec::decode_bytes(key)
        .map(|(_, raw)| raw)
        .map_err(|error| RegionLoadError::new("DecodeRegionKey", error.to_string()))
}

fn peer_role(role: i32) -> PeerRole {
    match role {
        0 => PeerRole::Voter,
        1 => PeerRole::Learner,
        2 => PeerRole::IncomingVoter,
        3 => PeerRole::DemotingVoter,
        role => PeerRole::Unknown(role),
    }
}

fn region_not_found(what: impl std::fmt::Display) -> RegionLoadError {
    RegionLoadError::new("RegionNotFound", format!("region {what} not found"))
}

impl InProcessRegionLoader {
    /// A loader answering from `manager`, the store's regions.
    #[must_use]
    pub fn new(manager: Arc<InProcessRegionManager>) -> Self {
        Self { manager }
    }

    /// The region manager this loader answers from.
    #[must_use]
    pub fn manager(&self) -> &Arc<InProcessRegionManager> {
        &self.manager
    }

    /// A region and its leader as the region cache holds them: raw bounds,
    /// every peer's store resolved.
    fn location(
        &self,
        region: &metapb::Region,
        leader: &metapb::Peer,
    ) -> Result<RegionLocation, RegionLoadError> {
        let epoch = region.region_epoch.clone().unwrap_or_default();
        let mut stores: Vec<Store> = Vec::new();
        for peer in &region.peers {
            if stores.iter().any(|store| store.id == peer.store_id) {
                continue;
            }
            if let Ok(address) = self.manager.get_store_addr_by_store_id(peer.store_id) {
                stores.push(Store {
                    id: peer.store_id,
                    address,
                    epoch: 1,
                });
            }
        }
        Ok(RegionLocation {
            region: RegionVerId::new(region.id, epoch.conf_ver, epoch.version),
            start_key: decode_region_key(&region.start_key)?,
            end_key: decode_region_key(&region.end_key)?,
            peers: region
                .peers
                .iter()
                .map(|peer| Peer {
                    id: peer.id,
                    store_id: peer.store_id,
                    role: peer_role(peer.role),
                    is_witness: peer.is_witness,
                    store_epoch: 1,
                })
                .collect(),
            leader_peer_id: Some(leader.id),
            stores,
            buckets: None,
            down_peer_ids: Vec::new(),
            pending_peer_ids: Vec::new(),
        })
    }

    /// Go `MockPD.GetRegion` through the codec client: the region holding
    /// the raw `key`.
    pub fn region_by_key(&self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        let (region, leader) = self
            .manager
            .get_region_by_key(&encode_region_key(key))
            .ok_or_else(|| region_not_found(format!("for key {key:?}")))?;
        self.location(&region, &leader)
    }

    /// Go `MockPD.GetPrevRegion` through the codec client: the region whose
    /// `(start, end]` holds the raw `key`; an empty key is the last region.
    pub fn region_by_end_key(&self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        let found = if key.is_empty() {
            self.manager.scan_regions(b"", b"", 0).pop()
        } else {
            self.manager.get_region_by_end_key(&encode_region_key(key))
        };
        let (region, leader) =
            found.ok_or_else(|| region_not_found(format!("for end key {key:?}")))?;
        self.location(&region, &leader)
    }

    /// Go `MockPD.GetRegionByID`.
    pub fn region_by_id(&self, id: u64) -> Result<RegionLocation, RegionLoadError> {
        let region = self
            .manager
            .get_region(id)
            .ok_or_else(|| region_not_found(id))?;
        let leader = region.peers[0].clone();
        self.location(&region, &leader)
    }

    /// Go `MockPD.ScanRegions` through the codec client: raw bounds, an
    /// empty end unbounded.
    pub fn scan_regions(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        let end = if end.is_empty() {
            Vec::new()
        } else {
            encode_region_key(end)
        };
        self.manager
            .scan_regions(&encode_region_key(start), &end, limit)
            .iter()
            .map(|(region, leader)| self.location(region, leader))
            .collect()
    }
}

impl RegionLoader for InProcessRegionLoader {
    fn cluster_id(&self) -> u64 {
        IN_PROCESS_CLUSTER_ID
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.region_by_key(key)
    }
}

impl BatchRegionLoader for InProcessRegionLoader {
    fn batch_load_regions(
        &mut self,
        ranges: &[KeyRange],
        limit: usize,
        _options: BatchLoadOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        // Go `MockPD.BatchScanRegions`: every region overlapping a range,
        // once, in key order.
        let mut found: Vec<RegionLocation> = Vec::new();
        for range in ranges {
            for region in self.scan_regions(&range.start, &range.end, 0)? {
                if !found.iter().any(|seen| seen.region.id == region.region.id) {
                    found.push(region);
                }
            }
        }
        found.sort_by(|left, right| left.start_key.cmp(&right.start_key));
        if limit > 0 {
            found.truncate(limit);
        }
        Ok(found)
    }
}

impl RegionQueryLoader for InProcessRegionLoader {
    fn query_region(
        &mut self,
        query: RegionQuery<'_>,
        _options: RegionQueryOptions,
    ) -> Result<RegionLocation, RegionLoadError> {
        match query {
            RegionQuery::Key(key) => self.region_by_key(key),
            RegionQuery::EndKey(key) => self.region_by_end_key(key),
            RegionQuery::Id(id) => self.region_by_id(id),
        }
    }

    fn scan_regions_once(
        &mut self,
        range: &KeyRange,
        limit: usize,
        _options: RegionQueryOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        self.scan_regions(&range.start, &range.end, limit)
    }

    fn load_store(&mut self, store_id: u64) -> Result<Option<StoreMetadata>, RegionLoadError> {
        // Go `MockPD.GetStore`; an unknown store is Go's tombstone answer.
        Ok(self
            .manager
            .get_all_stores()
            .into_iter()
            .find(|store| store.id == store_id)
            .map(|store| StoreMetadata {
                id: store.id,
                address: store.address,
                labels: store
                    .labels
                    .into_iter()
                    .map(|label| (label.key, label.value))
                    .collect(),
            }))
    }
}

impl RegionRecoveryLoader for InProcessRegionLoader {
    fn hydrate_region(
        &mut self,
        metadata: &RegionMetadata,
        _leader_store_id: u64,
        _resolved_stores: &mut std::collections::BTreeMap<
            u64,
            Option<tidb_txnkv::region::StoreMetadata>,
        >,
    ) -> Result<RegionLocation, RegionLoadError> {
        self.region_by_id(metadata.region.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // WRITTEN: mock_region.go's coverage rides the store suites.

    #[test]
    fn every_key_routes_to_the_one_region_and_its_one_store() {
        let mut loader = InProcessRegionLoader::default();
        let location = loader.load_region(b"anything").expect("routes");
        assert_eq!(location.region.id, IN_PROCESS_REGION_ID);
        assert!(location.start_key.is_empty() && location.end_key.is_empty());
        assert_eq!(location.leader_peer_id, Some(IN_PROCESS_PEER_ID));
        assert_eq!(location.stores[0].address, "store1");
        let store = loader
            .load_store(IN_PROCESS_STORE_ID)
            .expect("loads")
            .expect("exists");
        assert_eq!(store.address, "store1");
        assert!(loader.load_store(99).expect("loads").is_none(), "tombstone");
    }
}

#[cfg(test)]
mod authority_tests {
    use super::*;
    use crate::client::InProcessClient;

    /// The integration pin: the in-process pair satisfies the REAL transport
    /// machinery's bounds — the shared read authority starts its maintenance
    /// worker over a region cache holding this loader, with this client as
    /// its `C`. This is the exact type the node will hold in place of
    /// `SharedReadAuthority<TonicCoprocessorClient, PdRegionLoader>`.
    #[test]
    fn the_in_process_pair_starts_the_shared_read_authority() {
        let cache = tidb_txnkv::region::RegionCache::new(InProcessRegionLoader::default());
        let authority: tidb_txnkv::SharedReadAuthority<InProcessClient, InProcessRegionLoader> =
            tidb_txnkv::SharedReadAuthority::start_with_lock_resolver(
                InProcessClient::new(),
                cache,
            )
            .expect("the maintenance worker starts over the in-process plane");
        drop(authority);
    }
}
