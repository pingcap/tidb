// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::time::Duration;

use tidb_codec::{decode_bytes, encode_bytes};
use tidb_pd_client::{PdClient, PdClientError, PdMemberSet, PdStore};

use crate::region::{
    Peer, PeerRole, RegionEpoch, RegionLoadError, RegionLoader, RegionLocation, RegionMetadata,
    RegionRecoveryLoader, RegionVerId, Store,
};

/// Concrete API-v1 region loader backed by the bounded PD control plane.
pub struct PdRegionLoader {
    client: PdClient,
}

impl PdRegionLoader {
    /// Bootstraps one plaintext PD endpoint and its nonzero cluster identity.
    pub fn connect(endpoint: impl Into<String>, timeout: Duration) -> Result<Self, PdClientError> {
        Self::connect_seeds([endpoint.into()], timeout)
    }

    /// Bootstraps one or more plaintext PD seeds and their discovered members.
    pub fn connect_seeds<I, S>(seeds: I, timeout: Duration) -> Result<Self, PdClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Ok(Self {
            client: PdClient::connect_seeds(seeds, timeout)?,
        })
    }

    /// Returns the first configured seed for diagnostics, not route authority.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        self.client.endpoint()
    }

    /// Returns the endpoint selected by the most recent successful PD request.
    #[must_use]
    pub fn active_endpoint(&self) -> String {
        self.client.active_endpoint()
    }

    /// Returns the latest discovered PD membership snapshot.
    #[must_use]
    pub fn member_set(&self) -> PdMemberSet {
        self.client.member_set()
    }
}

impl RegionLoader for PdRegionLoader {
    fn cluster_id(&self) -> u64 {
        self.client.cluster_id()
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        let mut encoded_key = Vec::new();
        encode_bytes(&mut encoded_key, key);
        let region = self
            .client
            .get_region(&encoded_key)
            .map_err(region_load_error)?;

        // Resolve every peer-referenced store exactly once before filtering.
        // This matches client-go's region construction: a tombstone/removed
        // store yields no usable address, while a malformed response fails the
        // load rather than silently producing a partial route.
        let mut resolved = HashMap::<u64, Option<PdStore>>::new();
        for peer in &region.peers {
            if let std::collections::hash_map::Entry::Vacant(entry) = resolved.entry(peer.store_id)
            {
                let store = self
                    .client
                    .get_store(peer.store_id)
                    .map_err(region_load_error)?;
                entry.insert(store);
            }
        }

        let mut peers = Vec::with_capacity(region.peers.len());
        let mut stores = Vec::with_capacity(region.peers.len());
        let mut store_indexes = HashMap::new();
        for peer in &region.peers {
            let is_leader = peer.id == region.leader.id && peer.store_id == region.leader.store_id;
            if region.down_peer_ids.contains(&peer.id) {
                continue;
            }
            let Some(store) = resolved
                .get(&peer.store_id)
                .expect("every peer store was resolved")
                .as_ref()
            else {
                continue;
            };
            if peer.is_witness && !is_leader {
                continue;
            }
            peers.push(Peer {
                id: peer.id,
                store_id: peer.store_id,
                role: map_peer_role(peer.role),
                is_witness: peer.is_witness,
                // This is client-go's local failure epoch, not PD's
                // start_timestamp. A freshly resolved store starts at zero.
                store_epoch: 0,
            });
            if let std::collections::hash_map::Entry::Vacant(entry) = store_indexes.entry(store.id)
            {
                entry.insert(stores.len());
                stores.push(Store {
                    id: store.id,
                    address: store.address.clone(),
                    epoch: 0,
                });
            }
        }
        if peers.is_empty() {
            return Err(loader_topology_error(
                "no_available_peers",
                format!("region {} has no usable peers", region.id),
            ));
        }
        if !peers.iter().any(|peer| peer.id == region.leader.id) {
            return Err(loader_topology_error(
                "missing_usable_leader",
                format!(
                    "region {} leader {} was down or on a removed store",
                    region.id, region.leader.id
                ),
            ));
        }

        Ok(RegionLocation {
            region: RegionVerId {
                id: region.id,
                epoch: RegionEpoch {
                    conf_ver: region.epoch.conf_ver,
                    version: region.epoch.version,
                },
            },
            start_key: decode_region_boundary(&region.start_key)?,
            end_key: decode_region_boundary(&region.end_key)?,
            peers,
            leader_peer_id: Some(region.leader.id),
            stores,
        })
    }
}

impl RegionRecoveryLoader for PdRegionLoader {
    fn hydrate_region(
        &mut self,
        metadata: &RegionMetadata,
        leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        let mut resolved = HashMap::<u64, Option<PdStore>>::new();
        for peer in &metadata.peers {
            if let std::collections::hash_map::Entry::Vacant(entry) = resolved.entry(peer.store_id)
            {
                entry.insert(
                    self.client
                        .get_store(peer.store_id)
                        .map_err(region_load_error)?,
                );
            }
        }

        let leader_peer_id = metadata
            .peers
            .iter()
            .find(|peer| peer.store_id == leader_store_id)
            .map(|peer| peer.id);
        let mut peers = Vec::with_capacity(metadata.peers.len());
        let mut stores = Vec::with_capacity(metadata.peers.len());
        let mut store_indexes = HashMap::new();
        for peer in &metadata.peers {
            let is_leader = leader_peer_id == Some(peer.id);
            let Some(store) = resolved
                .get(&peer.store_id)
                .expect("every current-region peer store was resolved")
                .as_ref()
            else {
                continue;
            };
            if peer.is_witness && !is_leader {
                continue;
            }
            peers.push(Peer {
                id: peer.id,
                store_id: peer.store_id,
                role: peer.role,
                is_witness: peer.is_witness,
                store_epoch: 0,
            });
            if let std::collections::hash_map::Entry::Vacant(entry) = store_indexes.entry(store.id)
            {
                entry.insert(stores.len());
                stores.push(Store {
                    id: store.id,
                    address: store.address.clone(),
                    epoch: 0,
                });
            }
        }
        if peers.is_empty() {
            return Err(loader_topology_error(
                "no_available_peers",
                format!("region {} has no usable peers", metadata.region.id),
            ));
        }
        let leader_peer_id = leader_peer_id
            .filter(|leader| {
                peers
                    .iter()
                    .any(|peer| peer.id == *leader && peer.store_id == leader_store_id)
            })
            .or_else(|| {
                peers
                    .iter()
                    .find(|peer| peer.role != PeerRole::Learner && !peer.is_witness)
                    .map(|peer| peer.id)
            });
        let Some(leader_peer_id) = leader_peer_id else {
            return Err(loader_topology_error(
                "missing_usable_leader",
                format!(
                    "region {} has no usable electable leader after observing store {}",
                    metadata.region.id, leader_store_id
                ),
            ));
        };

        Ok(RegionLocation {
            region: metadata.region,
            start_key: decode_region_boundary(&metadata.encoded_start_key)?,
            end_key: decode_region_boundary(&metadata.encoded_end_key)?,
            peers,
            leader_peer_id: Some(leader_peer_id),
            stores,
        })
    }
}

fn decode_region_boundary(encoded: &[u8]) -> Result<Vec<u8>, RegionLoadError> {
    if encoded.is_empty() {
        return Ok(Vec::new());
    }
    decode_bytes(encoded)
        .map(|(_, decoded)| decoded)
        .map_err(|error| RegionLoadError::new("invalid_region_key", error.to_string()))
}

const fn map_peer_role(role: i32) -> PeerRole {
    match role {
        0 => PeerRole::Voter,
        1 => PeerRole::Learner,
        2 => PeerRole::IncomingVoter,
        3 => PeerRole::DemotingVoter,
        role => PeerRole::Unknown(role),
    }
}

fn region_load_error(error: PdClientError) -> RegionLoadError {
    RegionLoadError::new(error.kind(), error.to_string())
}

fn loader_topology_error(kind: &'static str, message: impl Into<String>) -> RegionLoadError {
    RegionLoadError::new(kind, message)
}
