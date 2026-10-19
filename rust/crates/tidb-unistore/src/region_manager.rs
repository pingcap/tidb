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

//! Go unistore's `MockRegionManager` (`tikv/mock_region.go`): the embedded
//! store's regions, which its region loader answers from and its
//! `SplitRegion` handler cuts.
//!
//! The cluster bootstraps as Go's `BootstrapWithSingleStore`: `AllocID`
//! (a pre-increment from 0) hands out the store (1), the region (2) and its
//! peer (3). A split keeps the left part's id with the next epoch version and
//! gives every new region a fresh id and peer id at epoch `(1, 1)`.
//!
//! Boundaries are raw keys here: Go's manager stores memcomparable-encoded
//! ones and its client decodes them, so both answer the same raw bounds.

use std::collections::BTreeMap;
use std::sync::Mutex;

use tidb_txnkv::region::{Peer, PeerRole, RegionLocation, RegionVerId, Store};

use crate::client::IN_PROCESS_ADDRESS;

/// The store every region's one peer lives on.
pub const STORE_ID: u64 = 1;
/// The bootstrap region.
pub const BOOTSTRAP_REGION_ID: u64 = 2;
/// The bootstrap region's peer.
pub const BOOTSTRAP_PEER_ID: u64 = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
struct RegionState {
    id: u64,
    start: Vec<u8>,
    end: Vec<u8>,
    conf_ver: u64,
    version: u64,
    peer_id: u64,
}

impl RegionState {
    fn contains(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && (self.end.is_empty() || key < self.end.as_slice())
    }

    fn ver_id(&self) -> RegionVerId {
        RegionVerId::new(self.id, self.conf_ver, self.version)
    }

    fn location(&self) -> RegionLocation {
        RegionLocation {
            region: self.ver_id(),
            start_key: self.start.clone(),
            end_key: self.end.clone(),
            peers: vec![Peer {
                id: self.peer_id,
                store_id: STORE_ID,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 1,
            }],
            leader_peer_id: Some(self.peer_id),
            stores: vec![Store {
                id: STORE_ID,
                address: IN_PROCESS_ADDRESS.to_owned(),
                epoch: 1,
            }],
            buckets: None,
            down_peer_ids: Vec::new(),
            pending_peer_ids: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct State {
    /// Start key -> region.
    regions: BTreeMap<Vec<u8>, RegionState>,
    /// The last id `AllocID` handed out.
    last_id: u64,
}

impl State {
    /// Go `AllocID`.
    fn alloc_id(&mut self) -> u64 {
        self.last_id += 1;
        self.last_id
    }

    fn by_key(&self, key: &[u8]) -> &RegionState {
        self.regions
            .range::<[u8], _>((std::ops::Bound::Unbounded, std::ops::Bound::Included(key)))
            .next_back()
            .map(|(_, region)| region)
            .expect("the regions cover the keyspace")
    }
}

/// The embedded store's region manager.
#[derive(Debug)]
pub struct InProcessRegionManager {
    state: Mutex<State>,
}

impl Default for InProcessRegionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl InProcessRegionManager {
    /// Go `BootstrapWithSingleStore`: one store, one region over the whole
    /// keyspace, one peer.
    #[must_use]
    pub fn new() -> Self {
        let mut state = State {
            regions: BTreeMap::new(),
            last_id: 0,
        };
        let store_id = state.alloc_id();
        debug_assert_eq!(store_id, STORE_ID);
        let id = state.alloc_id();
        let peer_id = state.alloc_id();
        state.regions.insert(
            Vec::new(),
            RegionState {
                id,
                start: Vec::new(),
                end: Vec::new(),
                conf_ver: 1,
                version: 1,
                peer_id,
            },
        );
        Self {
            state: Mutex::new(state),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `GetRegionByKey`: the region containing `key`.
    #[must_use]
    pub fn region_by_key(&self, key: &[u8]) -> RegionLocation {
        self.state().by_key(key).location()
    }

    /// Go `GetRegionByEndKey`: the region whose `(start, end]` holds `key`;
    /// an empty key is the last region.
    #[must_use]
    pub fn region_by_end_key(&self, key: &[u8]) -> RegionLocation {
        let state = self.state();
        if key.is_empty() {
            return state
                .regions
                .values()
                .next_back()
                .expect("the regions cover the keyspace")
                .location();
        }
        state
            .regions
            .range::<[u8], _>((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(key)))
            .next_back()
            .map(|(_, region)| region.location())
            .expect("the first region starts at the empty key")
    }

    /// Go `GetRegion(id)`.
    #[must_use]
    pub fn region_by_id(&self, id: u64) -> Option<RegionLocation> {
        self.state()
            .regions
            .values()
            .find(|region| region.id == id)
            .map(RegionState::location)
    }

    /// Go `ScanRegions(start, end, limit)`: from the region containing
    /// `start`, every region that starts before `end` (an empty end is the
    /// keyspace's end), at most `limit` of them when `limit > 0`.
    #[must_use]
    pub fn scan_regions(&self, start: &[u8], end: &[u8], limit: usize) -> Vec<RegionLocation> {
        let state = self.state();
        let first = state.by_key(start).start.clone();
        let mut found = Vec::new();
        for region in state.regions.range(first..).map(|(_, region)| region) {
            if !end.is_empty() && region.start.as_slice() >= end {
                break;
            }
            found.push(region.location());
            if limit > 0 && found.len() == limit {
                break;
            }
        }
        found
    }

    /// Go `MockRegionManager.SplitRegion` for one region: `keys` are the
    /// split keys client-go grouped into region `region_id` (sorted,
    /// distinct, none equal to its start). Answers the regions the split
    /// produced -- the left part first -- and the pre-split identity of the
    /// region it cut, or `None` when the region is gone or the keys left it.
    pub fn split_region(
        &self,
        region_id: u64,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<RegionLocation>, RegionVerId), String> {
        let mut state = self.state();
        let Some(region) = state
            .regions
            .values()
            .find(|region| region.id == region_id)
            .cloned()
        else {
            return Err("region not found".to_owned());
        };
        // Go `splitKeys`: the keys inside this region, after its start.
        let mut splits: Vec<&Vec<u8>> = keys.iter().filter(|key| region.contains(key)).collect();
        splits.sort();
        splits.dedup();
        if splits.first().is_some_and(|key| **key == region.start) {
            splits.remove(0);
        }
        if splits.is_empty() {
            return Ok((vec![region.location()], region.ver_id()));
        }
        let mut produced = vec![RegionState {
            id: region.id,
            start: region.start.clone(),
            end: splits[0].clone(),
            conf_ver: region.conf_ver,
            version: region.version + 1,
            peer_id: region.peer_id,
        }];
        for (index, start) in splits.iter().enumerate() {
            let end = splits
                .get(index + 1)
                .map_or_else(|| region.end.clone(), |key| (*key).clone());
            let id = state.alloc_id();
            let peer_id = state.alloc_id();
            produced.push(RegionState {
                id,
                start: (*start).clone(),
                end,
                conf_ver: 1,
                version: 1,
                peer_id,
            });
        }
        for part in &produced {
            state.regions.insert(part.start.clone(), part.clone());
        }
        Ok((
            produced.iter().map(RegionState::location).collect(),
            region.ver_id(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `BootstrapWithSingleStore` and `splitKeys`: ids 2/3 at bootstrap,
    /// the left part keeps its id with version 2, new regions take fresh id
    /// pairs in key order at epoch (1, 1).
    #[test]
    fn splits_like_go_mock_region_manager() {
        let manager = InProcessRegionManager::new();
        let whole = manager.region_by_key(b"anything");
        assert_eq!(whole.region, RegionVerId::new(2, 1, 1));
        assert_eq!(whole.leader_peer_id, Some(3));
        let (produced, stale) = manager
            .split_region(2, &[b"t5".to_vec(), b"t1".to_vec()])
            .unwrap();
        assert_eq!(stale, RegionVerId::new(2, 1, 1));
        let ids: Vec<_> = produced
            .iter()
            .map(|region| {
                (
                    region.region.id,
                    region.region.epoch.version,
                    region.leader_peer_id,
                )
            })
            .collect();
        assert_eq!(ids, vec![(2, 2, Some(3)), (4, 1, Some(5)), (6, 1, Some(7))]);
        assert_eq!(manager.region_by_key(b"t3").region.id, 4);
        assert_eq!(manager.region_by_end_key(b"t5").region.id, 4);
        assert_eq!(manager.region_by_end_key(b"").region.id, 6);
        let scanned: Vec<_> = manager
            .scan_regions(b"t2", b"t6", 0)
            .iter()
            .map(|region| region.region.id)
            .collect();
        assert_eq!(scanned, vec![4, 6]);
        assert_eq!(manager.scan_regions(b"", b"", 2).len(), 2);
    }
}
