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
use tidb_txnkv::region::RegionVerId;
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
        // region dropped; one `CmdSplitRegion` per region.
        keys.sort();
        keys.dedup();
        let mut groups: BTreeMap<Vec<u8>, (u64, Vec<Vec<u8>>)> = BTreeMap::new();
        for key in keys {
            let region = self.manager.region_by_key(&key);
            if region.start_key == key {
                continue;
            }
            groups
                .entry(region.start_key.clone())
                .or_insert_with(|| (region.region.id, Vec::new()))
                .1
                .push(key);
        }
        let mut region_ids = Vec::new();
        for (region_id, keys) in groups.into_values() {
            match self.manager.split_region(region_id, &keys) {
                Ok((produced, stale)) => {
                    (self.invalidate)(stale);
                    // client-go keeps every region of the response but the
                    // last for scattering: one per region the split created.
                    let created = produced.len().saturating_sub(1);
                    region_ids.extend(produced[..created].iter().map(|region| region.region.id));
                }
                Err(error) => return (region_ids, Some(error)),
            }
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
        Ok(self
            .manager
            .scan_regions(start, end, 0)
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
