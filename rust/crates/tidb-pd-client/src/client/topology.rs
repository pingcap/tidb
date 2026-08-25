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

//! Projecting PD's protobuf answers into the topology this crate exposes:
//! member sets, regions with their peers and epochs, buckets, and stores.
//!
//! Go boundary: the `metapb`/`pdpb` field reads that client-go's region cache
//! performs at each PD call site. Every projection is total — a response PD
//! could not have produced (a region with no epoch, a peer with no store, an
//! endpoint that is not a URL) becomes an error here rather than a partially
//! filled topology that a later lookup would trip over.

use std::collections::HashSet;

use tidb_proto::metapb;
use tidb_proto::pdpb;
use tonic::transport::Endpoint;

use crate::{
    PdBucketStats, PdBuckets, PdClientError, PdMemberSet, PdNodeState, PdPeer, PdRegion,
    PdRegionEpoch, PdStore, PdStoreState,
};

pub(super) fn project_member_set(
    response: pdpb::GetMembersResponse,
) -> Result<PdMemberSet, PdClientError> {
    let cluster_id = response
        .header
        .as_ref()
        .expect("GetMembers header validated before projection")
        .cluster_id;
    let leader = response
        .leader
        .ok_or_else(|| invalid_topology("missing_pd_leader", "GetMembers omitted the PD leader"))?;
    let leader_url = leader
        .client_urls
        .first()
        .ok_or_else(|| {
            invalid_topology(
                "missing_pd_leader_url",
                format!("PD leader {} has no client URL", leader.member_id),
            )
        })
        .and_then(|url| normalize_plaintext_endpoint(url))?;
    let member_urls = normalize_endpoints(
        response
            .members
            .into_iter()
            .flat_map(|member| member.client_urls),
        true,
    )?;
    if member_urls.is_empty() {
        return Err(invalid_topology(
            "missing_pd_member_url",
            "GetMembers returned no member client URL",
        ));
    }
    if !member_urls.contains(&leader_url) {
        return Err(invalid_topology(
            "leader_not_in_members",
            format!("PD leader URL {leader_url} is absent from member URLs"),
        ));
    }
    Ok(PdMemberSet {
        cluster_id,
        leader_url,
        member_urls,
    })
}

pub(super) fn project_region(
    response: pdpb::GetRegionResponse,
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    let region = response
        .region
        .ok_or_else(|| invalid_topology("missing_region", "GetRegion omitted region"))?;
    project_region_parts(
        region,
        response.leader,
        response.down_peers,
        response.pending_peers,
        response.buckets,
        need_buckets,
    )
}

pub(super) fn project_extended_region(
    region: pdpb::Region,
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    let metadata = region
        .region
        .ok_or_else(|| invalid_topology("missing_region", "scan result omitted region"))?;
    project_region_parts(
        metadata,
        region.leader,
        region.down_peers,
        region.pending_peers,
        region.buckets,
        need_buckets,
    )
}

pub(super) fn project_scan_regions(
    response: pdpb::ScanRegionsResponse,
) -> Result<Vec<PdRegion>, PdClientError> {
    if !response.regions.is_empty() {
        return response
            .regions
            .into_iter()
            .map(|region| project_extended_region(region, false))
            .collect();
    }

    let leaders = response.leaders;
    response
        .region_metas
        .into_iter()
        .enumerate()
        .map(|(index, region)| {
            project_region_parts(
                region,
                leaders.get(index).cloned(),
                Vec::new(),
                Vec::new(),
                None,
                false,
            )
        })
        .collect()
}

pub(super) fn project_region_parts(
    region: metapb::Region,
    leader: Option<metapb::Peer>,
    down_peer_stats: Vec<pdpb::PeerStats>,
    pending_peer_metadata: Vec<metapb::Peer>,
    buckets: Option<metapb::Buckets>,
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    if region.id == 0 {
        return Err(invalid_topology("zero_region_id", "region ID is zero"));
    }
    let epoch = region.region_epoch.ok_or_else(|| {
        invalid_topology(
            "missing_region_epoch",
            format!("region {} omitted epoch", region.id),
        )
    })?;
    if region.peers.is_empty() {
        return Err(invalid_topology(
            "missing_peers",
            format!("region {} has no peers", region.id),
        ));
    }
    let peers = region
        .peers
        .into_iter()
        .map(project_peer)
        .collect::<Result<Vec<_>, _>>()?;
    let mut identities = HashSet::with_capacity(peers.len());
    if peers.iter().any(|peer| !identities.insert(peer.id)) {
        return Err(invalid_topology(
            "duplicate_peer_id",
            format!("region {} repeats a peer ID", region.id),
        ));
    }
    let leader = match leader {
        None => None,
        Some(leader) if leader.id == 0 => None,
        Some(leader) => {
            let returned_leader = project_peer(leader)?;
            Some(
                peers
                    .iter()
                    .find(|peer| same_peer_identity(peer, &returned_leader))
                    .cloned()
                    .ok_or_else(|| {
                        invalid_topology(
                            "leader_not_in_peers",
                            format!(
                                "region {} leader {} is not a region peer",
                                region.id, returned_leader.id
                            ),
                        )
                    })?,
            )
        }
    };
    let down_peers = down_peer_stats
        .into_iter()
        .map(|stats| {
            let peer = stats.peer.ok_or_else(|| {
                invalid_topology("missing_down_peer", "down peer stats omitted peer")
            })?;
            let peer = project_peer(peer)?;
            if !peers
                .iter()
                .any(|candidate| same_peer_identity(candidate, &peer))
            {
                return Err(invalid_topology(
                    "down_peer_not_in_peers",
                    format!("down peer {} is not an exact region peer", peer.id),
                ));
            }
            Ok(peer)
        })
        .collect::<Result<Vec<_>, PdClientError>>()?;
    let pending_peers = pending_peer_metadata
        .into_iter()
        .map(project_peer)
        .collect::<Result<Vec<_>, PdClientError>>()?;
    // PD can return batch-wide bucket metadata for a request that did not ask
    // for it. Enforce the per-request contract at the shared projection point.
    let buckets = buckets.filter(|_| need_buckets).map(project_buckets);

    Ok(PdRegion {
        id: region.id,
        start_key: region.start_key,
        end_key: region.end_key,
        epoch: PdRegionEpoch {
            conf_ver: epoch.conf_ver,
            version: epoch.version,
        },
        peers,
        leader,
        down_peers,
        pending_peers,
        buckets,
    })
}

pub(super) fn project_buckets(buckets: metapb::Buckets) -> PdBuckets {
    PdBuckets {
        region_id: buckets.region_id,
        version: buckets.version,
        keys: buckets.keys,
        stats: buckets.stats.map(|stats| PdBucketStats {
            read_bytes: stats.read_bytes,
            write_bytes: stats.write_bytes,
            read_qps: stats.read_qps,
            write_qps: stats.write_qps,
            read_keys: stats.read_keys,
            write_keys: stats.write_keys,
        }),
        period_in_ms: buckets.period_in_ms,
    }
}

pub(super) fn project_peer(peer: metapb::Peer) -> Result<PdPeer, PdClientError> {
    if peer.id == 0 {
        return Err(invalid_topology("zero_peer_id", "peer ID is zero"));
    }
    if peer.store_id == 0 {
        return Err(invalid_topology(
            "zero_peer_store_id",
            format!("peer {} references store zero", peer.id),
        ));
    }
    Ok(PdPeer {
        id: peer.id,
        store_id: peer.store_id,
        role: peer.role,
        is_witness: peer.is_witness,
    })
}

pub(super) const fn same_peer_identity(left: &PdPeer, right: &PdPeer) -> bool {
    left.id == right.id && left.store_id == right.store_id
}

pub(super) fn project_store(
    store: Option<metapb::Store>,
    requested_id: u64,
) -> Result<Option<PdStore>, PdClientError> {
    let store = store.ok_or_else(|| {
        invalid_topology(
            "missing_store",
            format!("GetStore({requested_id}) omitted store"),
        )
    })?;
    if store.id == 0 {
        return Err(invalid_topology("zero_store_id", "store ID is zero"));
    }
    if store.id != requested_id {
        return Err(invalid_topology(
            "store_id_mismatch",
            format!("requested store {requested_id}, received {}", store.id),
        ));
    }
    project_store_record(store)
}

/// Projects every store `GetAllStores` returned, dropping the ones PD reports
/// as tombstone or removed. Go boundary: `client.go` -> `GetAllStores`, whose
/// callers treat a decommissioned store as absent rather than as a failure.
pub(super) fn project_all_stores(
    stores: Vec<metapb::Store>,
) -> Result<Vec<PdStore>, PdClientError> {
    let mut projected = Vec::with_capacity(stores.len());
    let mut seen = HashSet::with_capacity(stores.len());
    for store in stores {
        if store.id == 0 {
            return Err(invalid_topology("zero_store_id", "store ID is zero"));
        }
        if !seen.insert(store.id) {
            return Err(invalid_topology(
                "duplicate_store_id",
                format!("GetAllStores repeated store {}", store.id),
            ));
        }
        if let Some(store) = project_store_record(store)? {
            projected.push(store);
        }
    }
    Ok(projected)
}

/// Shared projection of one PD store record. `None` means PD reports the store
/// as tombstone or removed, which every caller treats as absent.
fn project_store_record(store: metapb::Store) -> Result<Option<PdStore>, PdClientError> {
    let state = match metapb::StoreState::try_from(store.state) {
        Ok(metapb::StoreState::Up) => PdStoreState::Up,
        Ok(metapb::StoreState::Offline) => PdStoreState::Offline,
        Ok(metapb::StoreState::Tombstone) => return Ok(None),
        Err(_) => {
            return Err(invalid_topology(
                "invalid_store_state",
                format!("store {} has state discriminant {}", store.id, store.state),
            ))
        }
    };
    let node_state = match metapb::NodeState::try_from(store.node_state) {
        Ok(metapb::NodeState::Preparing) => PdNodeState::Preparing,
        Ok(metapb::NodeState::Serving) => PdNodeState::Serving,
        Ok(metapb::NodeState::Removing) => PdNodeState::Removing,
        Ok(metapb::NodeState::Removed) => return Ok(None),
        Err(_) => {
            return Err(invalid_topology(
                "invalid_node_state",
                format!(
                    "store {} has node-state discriminant {}",
                    store.id, store.node_state
                ),
            ))
        }
    };
    if store.address.is_empty() {
        return Err(invalid_topology(
            "empty_store_address",
            format!("store {} has an empty client address", store.id),
        ));
    }
    let address_uri = normalize_plaintext_endpoint(&store.address).map_err(|error| {
        invalid_topology(
            "invalid_store_address",
            format!("store {}: {error}", store.id),
        )
    })?;
    Endpoint::from_shared(address_uri).map_err(|error| {
        invalid_topology(
            "invalid_store_address",
            format!("store {}: {error}", store.id),
        )
    })?;
    Ok(Some(PdStore {
        id: store.id,
        address: store.address,
        state,
        node_state,
        labels: store
            .labels
            .into_iter()
            .map(|label| (label.key, label.value))
            .collect(),
    }))
}

pub(super) fn normalize_plaintext_endpoint(endpoint: &str) -> Result<String, PdClientError> {
    if endpoint.is_empty() {
        return Err(PdClientError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
            message: "endpoint is empty".to_owned(),
        });
    }
    if endpoint.starts_with("https://") {
        return Err(PdClientError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
            message: "TLS endpoints are outside this bounded client".to_owned(),
        });
    }
    if endpoint.contains("://") && !endpoint.starts_with("http://") {
        return Err(PdClientError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
            message: "only plaintext http endpoints are supported".to_owned(),
        });
    }
    let normalized = if endpoint.starts_with("http://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    };
    Endpoint::from_shared(normalized.clone()).map_err(|error| PdClientError::InvalidEndpoint {
        endpoint: endpoint.to_owned(),
        message: error.to_string(),
    })?;
    Ok(normalized)
}

pub(crate) fn normalize_endpoints<I, S>(
    endpoints: I,
    sort: bool,
) -> Result<Vec<String>, PdClientError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut normalized = Vec::new();
    let mut seen = HashSet::new();
    for endpoint in endpoints {
        let endpoint = normalize_plaintext_endpoint(endpoint.as_ref())?;
        if seen.insert(endpoint.clone()) {
            normalized.push(endpoint);
        }
    }
    if sort {
        normalized.sort();
    }
    Ok(normalized)
}

pub(super) fn invalid_topology(kind: &'static str, message: impl Into<String>) -> PdClientError {
    PdClientError::InvalidTopology {
        kind,
        message: message.into(),
    }
}
