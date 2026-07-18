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
use tidb_pd_client::{PdClient, PdClientError, PdKeyRange, PdMemberSet, PdRegion, PdStore};

use crate::region::{
    BatchLoadOptions, BatchRegionLoader, BucketMetadata, BucketStats, KeyRange, Peer, PeerRole,
    RegionEpoch, RegionLoadError, RegionLoader, RegionLocation, RegionMetadata, RegionQuery,
    RegionQueryLoader, RegionQueryOptions, RegionQueryRoute, RegionRecoveryLoader, RegionVerId,
    Store, StoreMetadata,
};

/// Concrete API-v1 region loader backed by the bounded PD control plane.
pub struct PdRegionLoader {
    client: PdClient,
    store_labels: HashMap<u64, Vec<(String, String)>>,
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
            store_labels: HashMap::new(),
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

    /// Loads one region identity and decodes its exact optional bucket metadata.
    pub fn load_region_by_id(
        &mut self,
        region_id: u64,
        need_buckets: bool,
    ) -> Result<RegionLocation, RegionLoadError> {
        let region = self
            .client
            .get_region_by_id(region_id, need_buckets)
            .map_err(region_load_error)?;
        self.project_region(region)
    }

    /// Loads an ordered contiguous PD scan in the logical API-v1 key domain.
    pub fn scan_regions(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        let (start_key, end_key) = encode_region_range(start_key, end_key);
        let limit = pd_limit(limit)?;
        let regions = self
            .client
            .scan_regions(&start_key, &end_key, limit)
            .map_err(region_load_error)?;
        self.project_regions(regions)
    }

    fn project_regions(
        &mut self,
        regions: Vec<PdRegion>,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        regions
            .into_iter()
            .map(|region| self.project_region(region))
            .collect()
    }

    fn batch_scan_regions_fallback(
        &mut self,
        ranges: &[KeyRange],
        mut limit: usize,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        let mut result = Vec::new();
        let mut last_end_key: Option<Vec<u8>> = None;
        for range in ranges {
            let mut start_key = range.start.clone();
            if let Some(end_key) = &last_end_key {
                if end_key.is_empty() {
                    break;
                }
                if end_key.as_slice() >= range.end.as_slice() {
                    continue;
                }
                if end_key.as_slice() > start_key.as_slice() {
                    start_key.clone_from(end_key);
                }
            }
            let regions = self.scan_regions(&start_key, &range.end, limit)?;
            if let Some(last) = regions.last() {
                last_end_key = Some(last.end_key.clone());
            }
            let loaded = regions.len();
            result.extend(regions);
            if loaded >= limit {
                return Ok(result);
            }
            limit -= loaded;
        }
        Ok(result)
    }

    fn project_region(&mut self, region: PdRegion) -> Result<RegionLocation, RegionLoadError> {
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
                match &store {
                    Some(store) => {
                        self.store_labels.insert(store.id, store.labels.clone());
                    }
                    None => {
                        self.store_labels.remove(&peer.store_id);
                    }
                }
                entry.insert(store);
            }
        }

        let mut peers = Vec::with_capacity(region.peers.len());
        let mut stores = Vec::with_capacity(region.peers.len());
        let mut store_indexes = HashMap::new();
        for peer in &region.peers {
            let is_leader = region
                .leader
                .as_ref()
                .is_some_and(|leader| peer.id == leader.id && peer.store_id == leader.store_id);
            if region
                .down_peers
                .iter()
                .any(|down| down.id == peer.id && down.store_id == peer.store_id)
            {
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
        let leader_peer_id = region.leader.as_ref().and_then(|leader| {
            peers
                .iter()
                .any(|peer| peer.id == leader.id && peer.store_id == leader.store_id)
                .then_some(leader.id)
        });
        let buckets = region.buckets.map(decode_buckets).transpose()?;
        let down_peer_ids = region.down_peers.into_iter().map(|peer| peer.id).collect();
        let pending_peer_ids = region
            .pending_peers
            .into_iter()
            .map(|peer| peer.id)
            .collect();

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
            leader_peer_id,
            stores,
            buckets,
            down_peer_ids,
            pending_peer_ids,
        })
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
        self.project_region(region)
    }

    fn store_labels(&self, store_id: u64) -> &[(String, String)] {
        self.store_labels
            .get(&store_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

impl BatchRegionLoader for PdRegionLoader {
    fn batch_load_regions(
        &mut self,
        ranges: &[KeyRange],
        limit: usize,
        options: BatchLoadOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        let encoded_ranges = ranges
            .iter()
            .map(|range| {
                let (start_key, end_key) = encode_region_range(&range.start, &range.end);
                PdKeyRange { start_key, end_key }
            })
            .collect::<Vec<_>>();
        match self.client.batch_scan_regions(
            &encoded_ranges,
            pd_limit(limit)?,
            options.need_buckets,
            true,
        ) {
            Ok(regions) => self.project_regions(regions),
            Err(PdClientError::Transport { ref code, .. }) if code == "Unimplemented" => {
                self.batch_scan_regions_fallback(ranges, limit)
            }
            Err(error) => Err(region_load_error(error)),
        }
    }
}

impl RegionQueryLoader for PdRegionLoader {
    fn query_region(
        &mut self,
        query: RegionQuery<'_>,
        options: RegionQueryOptions,
    ) -> Result<RegionLocation, RegionLoadError> {
        let leader_only = options.route == RegionQueryRoute::LeaderOnly;
        match query {
            RegionQuery::Key(key) => {
                let mut encoded_key = Vec::new();
                encode_bytes(&mut encoded_key, key);
                let region = self
                    .client
                    .get_region_routed(&encoded_key, options.need_buckets, leader_only)
                    .map_err(region_load_error)?;
                self.project_region(region)
            }
            RegionQuery::EndKey(_) => Err(RegionLoadError::new(
                "get-prev-region-unavailable",
                "the checked PD wire projection does not expose GetPrevRegion",
            )),
            RegionQuery::Id(region_id) => {
                let region = self
                    .client
                    .get_region_by_id_routed(region_id, options.need_buckets, leader_only)
                    .map_err(region_load_error)?;
                self.project_region(region)
            }
        }
    }

    fn scan_regions_once(
        &mut self,
        range: &KeyRange,
        limit: usize,
        options: RegionQueryOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        let leader_only = options.route == RegionQueryRoute::LeaderOnly;
        let (start_key, end_key) = encode_region_range(&range.start, &range.end);
        let regions = self
            .client
            .scan_regions_routed(&start_key, &end_key, pd_limit(limit)?, leader_only)
            .map_err(region_load_error)?;
        self.project_regions(regions)
    }

    fn load_store(&mut self, store_id: u64) -> Result<Option<StoreMetadata>, RegionLoadError> {
        self.client
            .get_store(store_id)
            .map_err(region_load_error)
            .map(|store| {
                store.map(|store| StoreMetadata {
                    id: store.id,
                    address: store.address,
                    labels: store.labels,
                })
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
        for (store_id, store) in &resolved {
            match store {
                Some(store) => {
                    self.store_labels.insert(*store_id, store.labels.clone());
                }
                None => {
                    self.store_labels.remove(store_id);
                }
            }
        }

        // EpochNotMatch carries no leader. Pinned client-go first constructs
        // the region with its first usable TiKV peer, then switches to the
        // responding store when that exact peer survived hydration.
        let observed_peer_id = metadata
            .peers
            .iter()
            .find(|peer| peer.store_id == leader_store_id)
            .map(|peer| peer.id);
        let mut peers = Vec::with_capacity(metadata.peers.len());
        let mut stores = Vec::with_capacity(metadata.peers.len());
        let mut store_indexes = HashMap::new();
        for peer in &metadata.peers {
            let Some(store) = resolved
                .get(&peer.store_id)
                .expect("every current-region peer store was resolved")
                .as_ref()
            else {
                continue;
            };
            if peer.is_witness {
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
        let leader_peer_id = observed_peer_id
            .filter(|leader| {
                peers
                    .iter()
                    .any(|peer| peer.id == *leader && peer.store_id == leader_store_id)
            })
            .or_else(|| peers.first().map(|peer| peer.id));
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
            buckets: None,
            down_peer_ids: Vec::new(),
            pending_peer_ids: Vec::new(),
        })
    }
}

fn encode_region_range(start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut encoded_start = Vec::new();
    encode_bytes(&mut encoded_start, start);
    if end.is_empty() {
        return (encoded_start, Vec::new());
    }
    let mut encoded_end = Vec::new();
    encode_bytes(&mut encoded_end, end);
    (encoded_start, encoded_end)
}

fn decode_region_boundary(encoded: &[u8]) -> Result<Vec<u8>, RegionLoadError> {
    if encoded.is_empty() {
        return Ok(Vec::new());
    }
    decode_bytes(encoded)
        .map(|(_, decoded)| decoded)
        .map_err(|error| RegionLoadError::new("invalid_region_key", error.to_string()))
}

fn decode_buckets(buckets: tidb_pd_client::PdBuckets) -> Result<BucketMetadata, RegionLoadError> {
    let keys = buckets
        .keys
        .iter()
        .map(|key| decode_region_boundary(key))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BucketMetadata {
        region_id: buckets.region_id,
        version: buckets.version,
        keys,
        stats: buckets.stats.map(|stats| BucketStats {
            read_bytes: stats.read_bytes,
            write_bytes: stats.write_bytes,
            read_qps: stats.read_qps,
            write_qps: stats.write_qps,
            read_keys: stats.read_keys,
            write_keys: stats.write_keys,
        }),
        period_in_ms: buckets.period_in_ms,
    })
}

fn pd_limit(limit: usize) -> Result<i32, RegionLoadError> {
    i32::try_from(limit).map_err(|_| {
        RegionLoadError::new(
            "region_scan_limit_overflow",
            format!("region scan limit {limit} exceeds i32::MAX"),
        )
    })
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
