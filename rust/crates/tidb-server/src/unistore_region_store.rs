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

//! The embedded store's regions as SQL reaches them: client-go's
//! `SplitRegions` over unistore's region manager (its `CmdSplitRegion`
//! handler), unistore's mock PD (every operator query answers SUCCESS, so no
//! region is ever scattering), and `LoadRegionsInKeyRange` over the same
//! manager the node's region cache loads from.

use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_executor::region_map::{RegionMeta, RegionStore};
use tidb_proto::{kvrpcpb, metapb};
use tidb_txnkv::region::{RegionLocation, RegionVerId};
use tidb_unistore::region_loader::InProcessRegionLoader;
use tidb_unistore::region_manager::InProcessRegionManager;

/// [`RegionStore`] over the embedded store's region manager.
pub(crate) struct UnistoreRegionStore {
    manager: Arc<InProcessRegionManager>,
    /// Drops a region the split made stale from the node's region cache, so
    /// later requests route by the new regions (client-go learns the same
    /// from the store's EpochNotMatch).
    invalidate: Box<dyn Fn(RegionVerId) + Send + Sync>,
}

impl std::fmt::Debug for UnistoreRegionStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UnistoreRegionStore")
            .field("manager", &self.manager)
            .finish_non_exhaustive()
    }
}

impl UnistoreRegionStore {
    pub(crate) fn new(
        manager: Arc<InProcessRegionManager>,
        invalidate: impl Fn(RegionVerId) + Send + Sync + 'static,
    ) -> Self {
        Self {
            manager,
            invalidate: Box::new(invalidate),
        }
    }
}

impl RegionStore for UnistoreRegionStore {
    fn split_regions(
        &self,
        mut keys: Vec<Vec<u8>>,
        _scatter: bool,
        _table_id: Option<i64>,
    ) -> (Vec<u64>, Option<String>) {
        // client-go `SplitRegions`: the keys sorted and de-duplicated,
        // grouped by the region holding each, a key that already starts its
        // region dropped; one `CmdSplitRegion` per region, addressed to it.
        keys.sort();
        keys.dedup();
        let loader = InProcessRegionLoader::new(Arc::clone(&self.manager));
        let mut groups: BTreeMap<Vec<u8>, (RegionLocation, Vec<Vec<u8>>)> = BTreeMap::new();
        for key in keys {
            let region = match loader.region_by_key(&key) {
                Ok(region) => region,
                Err(error) => return (Vec::new(), Some(error.to_string())),
            };
            if region.start_key == key {
                continue;
            }
            groups
                .entry(region.start_key.clone())
                .or_insert_with(|| (region, Vec::new()))
                .1
                .push(key);
        }
        let mut region_ids = Vec::new();
        for (region, keys) in groups.into_values() {
            let request = kvrpcpb::SplitRegionRequest {
                context: Some(kvrpcpb::Context {
                    region_id: region.region.id,
                    region_epoch: Some(metapb::RegionEpoch {
                        conf_ver: region.region.epoch.conf_ver,
                        version: region.region.epoch.version,
                    }),
                    peer: region
                        .peers
                        .iter()
                        .find(|peer| Some(peer.id) == region.leader_peer_id)
                        .map(|peer| metapb::Peer {
                            id: peer.id,
                            store_id: peer.store_id,
                            ..metapb::Peer::default()
                        }),
                    ..kvrpcpb::Context::default()
                }),
                split_keys: keys,
                ..kvrpcpb::SplitRegionRequest::default()
            };
            let response = self.manager.split_region(&request);
            if let Some(region_error) = response.region_error {
                return (region_ids, Some(region_error.message));
            }
            // client-go drops the split region from its cache.
            (self.invalidate)(region.region);
            // client-go keeps every region of the response but the last for
            // scattering: one per region the split created.
            let created = response.regions.len().saturating_sub(1);
            region_ids.extend(response.regions[..created].iter().map(|region| region.id));
        }
        (region_ids, None)
    }

    fn wait_scatter_region_finish(&self, _region_id: u64, _backoff_ms: i64) -> Result<(), String> {
        // Unistore's `MockPD.GetOperator` answers SUCCESS.
        Ok(())
    }

    fn check_region_in_scattering(&self, _region_id: u64) -> Result<bool, String> {
        Ok(false)
    }

    fn load_regions_in_key_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<RegionMeta>, String> {
        Ok(InProcessRegionLoader::new(Arc::clone(&self.manager))
            .scan_regions(start, end, 0)
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|region| {
                let leader = region.leader_peer_id.unwrap_or_default();
                RegionMeta {
                    id: region.region.id,
                    peer_id: leader,
                    store_id: region
                        .peers
                        .iter()
                        .find(|peer| peer.id == leader)
                        .map_or(0, |peer| peer.store_id),
                    peers: region.peers.iter().map(|peer| peer.id).collect(),
                    start_key: region.start_key,
                    end_key: region.end_key,
                    // Go's `getRegionInfo` asks PD over HTTP only for an
                    // `EtcdBackend` store; unistore is not one.
                    ..RegionMeta::default()
                }
            })
            .collect())
    }
}
