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

//! Go `MockPD`'s region half (`tikv/mock_region.go`): every region query
//! the embedded store's client asks, answered from the store's
//! [`InProcessRegionManager`] -- one region at bootstrap, and whatever
//! `SPLIT` cut since.
//!
//! Paired with [`crate::client::InProcessClient`], this completes the read
//! transport's generic pair: `DirectUnaryQueryTransport<C, R>` can now be
//! instantiated with no network under either parameter.
//!
//! # Narrowings, by name
//!
//! * Store labels, buckets, down/pending peers: none exist in-process.

use std::sync::Arc;

use tidb_txnkv::region::{
    BatchLoadOptions, BatchRegionLoader, KeyRange, RegionLoadError, RegionLoader, RegionLocation,
    RegionMetadata, RegionQuery, RegionQueryLoader, RegionQueryOptions, RegionRecoveryLoader,
    StoreMetadata,
};

use crate::client::IN_PROCESS_ADDRESS;
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
}

impl RegionLoader for InProcessRegionLoader {
    fn cluster_id(&self) -> u64 {
        IN_PROCESS_CLUSTER_ID
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        Ok(self.manager.region_by_key(key))
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
            for region in self.manager.scan_regions(&range.start, &range.end, 0) {
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
            RegionQuery::Key(key) => Ok(self.manager.region_by_key(key)),
            RegionQuery::EndKey(key) => Ok(self.manager.region_by_end_key(key)),
            RegionQuery::Id(id) => self.manager.region_by_id(id).ok_or_else(|| {
                RegionLoadError::new("RegionNotFound", format!("region {id} not found"))
            }),
        }
    }

    fn scan_regions_once(
        &mut self,
        range: &KeyRange,
        limit: usize,
        _options: RegionQueryOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        Ok(self.manager.scan_regions(&range.start, &range.end, limit))
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
        metadata: &RegionMetadata,
        _leader_store_id: u64,
        _resolved_stores: &mut std::collections::BTreeMap<
            u64,
            Option<tidb_txnkv::region::StoreMetadata>,
        >,
    ) -> Result<RegionLocation, RegionLoadError> {
        self.manager
            .region_by_id(metadata.region.id)
            .ok_or_else(|| {
                RegionLoadError::new(
                    "RegionNotFound",
                    format!("region {} not found", metadata.region.id),
                )
            })
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
        assert_eq!(location.stores[0].address, IN_PROCESS_ADDRESS);
        let store = loader
            .load_store(IN_PROCESS_STORE_ID)
            .expect("loads")
            .expect("exists");
        assert_eq!(store.address, IN_PROCESS_ADDRESS);
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
