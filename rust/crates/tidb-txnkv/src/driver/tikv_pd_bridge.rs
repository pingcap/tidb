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

//! Injects TiDB's own PD client into the transactional engine.
//!
//! Go boundary: `pkg/store/driver/tikv_driver.go`. TiDB constructs a
//! `pd.Client` from `github.com/tikv/pd/client` and hands it to
//! `tikv.NewKVStore`, so the leader-first routing, member-set walk and
//! membership refresh in `pd_service_discovery.go` stay TiDB-owned while
//! client-go keeps region caching and request retry. This module is the same
//! seam: [`tidb_pd_client::PdClient`] answers PD lookups and the engine's
//! `RegionCache` caches and routes on top of it.
//!
//! The engine's PD contract is async and TiDB's client is synchronous, so
//! every call crosses on `spawn_blocking`. Calling the blocking facade
//! directly from a runtime worker would stall unrelated futures on the same
//! thread for a whole PD round trip.

use std::sync::Arc;

use tidb_pd_client::{
    PdBucketStats, PdBuckets, PdClient as TidbPdClient, PdClientError, PdNodeState, PdPeer,
    PdRegion, PdStore, PdStoreState,
};
use tikv_client::pd::{RegionScanOptions, RetryClientTrait};
use tikv_client::proto::{keyspacepb, metapb, pdpb};
use tikv_client::region::{RegionId, RegionWithLeader, StoreId};
use tikv_client::{Error, Result, Timestamp};

/// Number of low bits a PD timestamp reserves for the logical counter.
///
/// Source `oracle.go` composes `physical << 18 | logical`; the engine wants the
/// pair back, so the bridge splits on the same boundary rather than inventing
/// its own encoding.
const PHYSICAL_SHIFT_BITS: u32 = 18;

/// TiDB's PD client presented under the engine's PD contract.
#[derive(Clone)]
pub struct TidbPdBridge {
    client: Arc<TidbPdClient>,
}

impl TidbPdBridge {
    /// Wraps a PD client TiDB already constructed and owns.
    pub fn new(client: Arc<TidbPdClient>) -> Self {
        Self { client }
    }

    /// The underlying TiDB client, for callers that still need PD facilities
    /// the engine contract does not express (etcd access, membership).
    pub fn client(&self) -> &Arc<TidbPdClient> {
        &self.client
    }

    /// Runs one blocking PD call on the blocking pool and maps its outcome
    /// into the engine's error space.
    async fn dispatch<T, F>(&self, call: F) -> Result<T>
    where
        F: FnOnce(&TidbPdClient) -> std::result::Result<T, PdClientError> + Send + 'static,
        T: Send + 'static,
    {
        let client = Arc::clone(&self.client);
        tokio::task::spawn_blocking(move || call(&client))
            .await?
            .map_err(map_pd_error)
    }
}

/// PD failures carry TiDB's own operation/endpoint context, which no engine
/// error variant models. Preserving the rendered message keeps that context
/// visible instead of collapsing every PD fault into one opaque variant.
fn map_pd_error(error: PdClientError) -> Error {
    Error::StringError(format!("PD request failed: {error}"))
}

fn peer(peer: PdPeer) -> metapb::Peer {
    metapb::Peer {
        id: peer.id,
        store_id: peer.store_id,
        role: peer.role,
        is_witness: peer.is_witness,
    }
}

fn bucket_stats(stats: PdBucketStats) -> metapb::BucketStats {
    metapb::BucketStats {
        read_bytes: stats.read_bytes,
        write_bytes: stats.write_bytes,
        read_qps: stats.read_qps,
        write_qps: stats.write_qps,
        read_keys: stats.read_keys,
        write_keys: stats.write_keys,
    }
}

fn buckets(buckets: PdBuckets) -> metapb::Buckets {
    metapb::Buckets {
        region_id: buckets.region_id,
        version: buckets.version,
        keys: buckets.keys,
        stats: buckets.stats.map(bucket_stats),
        period_in_ms: buckets.period_in_ms,
    }
}

/// Projects a TiDB PD region onto the engine's routing view.
///
/// `down_seconds` is not carried: TiDB's model keeps only the identity of a
/// down peer, and the engine uses `down_peers` solely to drop those peers from
/// every access mode, which identity alone decides.
fn region_with_leader(region: PdRegion) -> RegionWithLeader {
    RegionWithLeader {
        region: metapb::Region {
            id: region.id,
            start_key: region.start_key,
            end_key: region.end_key,
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: region.epoch.conf_ver,
                version: region.epoch.version,
            }),
            peers: region.peers.into_iter().map(peer).collect(),
            ..Default::default()
        },
        leader: region.leader.map(peer),
        buckets: region.buckets.map(buckets),
        pending_peers: region.pending_peers.into_iter().map(peer).collect(),
        down_peers: region
            .down_peers
            .into_iter()
            .map(|down| pdpb::PeerStats {
                peer: Some(peer(down)),
                down_seconds: 0,
            })
            .collect(),
    }
}

fn store(store: PdStore) -> metapb::Store {
    metapb::Store {
        id: store.id,
        address: store.address,
        state: match store.state {
            PdStoreState::Up => metapb::StoreState::Up as i32,
            PdStoreState::Offline => metapb::StoreState::Offline as i32,
        },
        node_state: match store.node_state {
            PdNodeState::Preparing => metapb::NodeState::Preparing as i32,
            PdNodeState::Serving => metapb::NodeState::Serving as i32,
            PdNodeState::Removing => metapb::NodeState::Removing as i32,
        },
        labels: store
            .labels
            .into_iter()
            .map(|(key, value)| metapb::StoreLabel { key, value })
            .collect(),
        ..Default::default()
    }
}

/// Splits a composed PD timestamp back into the physical/logical pair.
fn timestamp(composed: u64) -> Timestamp {
    Timestamp {
        physical: (composed >> PHYSICAL_SHIFT_BITS) as i64,
        logical: (composed & ((1 << PHYSICAL_SHIFT_BITS) - 1)) as i64,
        // PD reports `suffix_bits` so a client can derive its share of a
        // batched logical range; TiDB's TSO worker has already applied it, so
        // the logical value here is final and carries no remaining suffix.
        suffix_bits: 0,
    }
}

fn key_range(range: pdpb::KeyRange) -> tidb_pd_client::PdKeyRange {
    tidb_pd_client::PdKeyRange {
        start_key: range.start_key,
        end_key: range.end_key,
    }
}

#[async_trait::async_trait]
impl RetryClientTrait for TidbPdBridge {
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        self.dispatch(move |client| client.get_region(&key))
            .await
            .map(region_with_leader)
    }

    async fn get_region_with_buckets(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        self.dispatch(move |client| client.get_region_with_buckets(&key, true))
            .await
            .map(region_with_leader)
    }

    async fn get_prev_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        self.dispatch(move |client| client.get_prev_region(&key))
            .await
            .map(region_with_leader)
    }

    async fn get_prev_region_with_buckets(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<RegionWithLeader> {
        self.dispatch(move |client| client.get_prev_region_with_buckets(&key, true))
            .await
            .map(region_with_leader)
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader> {
        self.dispatch(move |client| client.get_region_by_id(region_id, false))
            .await
            .map(region_with_leader)
    }

    async fn get_region_by_id_with_buckets(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<RegionWithLeader> {
        self.dispatch(move |client| client.get_region_by_id(region_id, true))
            .await
            .map(region_with_leader)
    }

    async fn scan_regions(
        self: Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<RegionWithLeader>> {
        let limit = i32::try_from(limit).map_err(|_| {
            Error::StringError(format!(
                "ScanRegions limit {limit} exceeds the PD field width"
            ))
        })?;
        self.dispatch(move |client| client.scan_regions(&start_key, &end_key, limit))
            .await
            .map(|regions| regions.into_iter().map(region_with_leader).collect())
    }

    async fn batch_scan_regions(
        self: Arc<Self>,
        ranges: Vec<pdpb::KeyRange>,
        limit: usize,
        options: RegionScanOptions,
    ) -> Result<Vec<RegionWithLeader>> {
        let limit = i32::try_from(limit).map_err(|_| {
            Error::StringError(format!(
                "BatchScanRegions limit {limit} exceeds the PD field width"
            ))
        })?;
        let ranges: Vec<_> = ranges.into_iter().map(key_range).collect();
        self.dispatch(move |client| {
            client.batch_scan_regions(
                &ranges,
                limit,
                options.need_buckets,
                options.contain_all_key_range,
            )
        })
        .await
        .map(|regions| regions.into_iter().map(region_with_leader).collect())
    }

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<Option<metapb::Store>> {
        self.dispatch(move |client| client.get_store(id))
            .await
            .map(|found| found.map(store))
    }

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        self.dispatch(TidbPdClient::all_stores)
            .await
            .map(|stores| stores.into_iter().map(store).collect())
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        self.dispatch(TidbPdClient::get_timestamp)
            .await
            .map(timestamp)
    }

    async fn get_gc_state(self: Arc<Self>, keyspace_id: u32) -> Result<pdpb::GetGcStateResponse> {
        // Source `gc_client.go` scopes the null keyspace as an *unset* field,
        // which PD distinguishes from keyspace 0 (`DEFAULT`). The engine's
        // contract carries a bare `u32` and cannot express that difference, so
        // 0 is read as the null keyspace every non-keyspace TiDB reads under.
        let scope = (keyspace_id != 0).then_some(keyspace_id);
        self.dispatch(move |client| client.get_gc_state(scope))
            .await
            .map(|state| pdpb::GetGcStateResponse {
                gc_state: Some(pdpb::GcState {
                    keyspace_scope: scope.map(|keyspace_id| pdpb::KeyspaceScope {
                        keyspace: Some(pdpb::keyspace_scope::Keyspace::KeyspaceId(keyspace_id)),
                    }),
                    is_keyspace_level_gc: state.is_keyspace_level_gc,
                    txn_safe_point: state.txn_safe_point,
                    gc_safe_point: state.gc_safe_point,
                    // TiDB's client requests GC state without barriers, so an
                    // empty list here means "not requested", not "none exist".
                    gc_barriers: Vec::new(),
                }),
                ..Default::default()
            })
    }

    async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
        // Advancing a safe point belongs to the GC owner, not to a reading
        // client. TiDB's PD client deliberately exposes no writer, so this
        // stays an explicit refusal rather than a silent `false`, which a
        // caller would read as "PD declined the advance".
        Err(Error::StringError(
            "advancing the PD safe point is owned by the GC worker, not the TiDB PD client"
                .to_owned(),
        ))
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        Err(Error::StringError(format!(
            "TiDB's PD client does not serve keyspace metadata (requested {keyspace})"
        )))
    }
}

#[cfg(test)]
mod tests {
    use tidb_pd_client::PdRegionEpoch;

    use super::*;

    /// Compile-time proof that TiDB's PD client satisfies the engine's PD
    /// contract, which is the whole point of this module: the engine's region
    /// cache is generic over exactly this bound.
    fn assert_engine_pd_client<C: RetryClientTrait + Send + Sync + 'static>() {}

    #[test]
    fn bridge_satisfies_the_engine_pd_contract() {
        assert_engine_pd_client::<TidbPdBridge>();
    }

    fn pd_peer(id: u64, store_id: u64) -> PdPeer {
        PdPeer {
            id,
            store_id,
            role: metapb::PeerRole::Learner as i32,
            is_witness: true,
        }
    }

    #[test]
    fn region_projection_preserves_epoch_peers_and_leader() {
        let projected = region_with_leader(PdRegion {
            id: 7,
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            epoch: PdRegionEpoch {
                conf_ver: 3,
                version: 11,
            },
            peers: vec![pd_peer(1, 100), pd_peer(2, 200)],
            leader: Some(pd_peer(1, 100)),
            down_peers: vec![pd_peer(2, 200)],
            pending_peers: vec![pd_peer(3, 300)],
            buckets: None,
        });

        assert_eq!(projected.region.id, 7);
        assert_eq!(projected.region.start_key, b"a".to_vec());
        assert_eq!(projected.region.end_key, b"z".to_vec());
        let epoch = projected.region.region_epoch.expect("epoch is projected");
        assert_eq!((epoch.conf_ver, epoch.version), (3, 11));
        assert_eq!(projected.region.peers.len(), 2);
        // The forward-extensible role discriminant and the witness flag survive
        // the crossing; a selector reads both when choosing a route.
        assert_eq!(
            projected.region.peers[0].role,
            metapb::PeerRole::Learner as i32
        );
        assert!(projected.region.peers[0].is_witness);
        assert_eq!(projected.leader.expect("leader is projected").id, 1);
        assert_eq!(projected.pending_peers[0].store_id, 300);
        assert_eq!(
            projected.down_peers[0].peer.as_ref().expect("down peer").id,
            2
        );
    }

    #[test]
    fn region_projection_carries_bucket_topology() {
        let projected = region_with_leader(PdRegion {
            id: 1,
            start_key: Vec::new(),
            end_key: Vec::new(),
            epoch: PdRegionEpoch {
                conf_ver: 1,
                version: 1,
            },
            peers: Vec::new(),
            leader: None,
            down_peers: Vec::new(),
            pending_peers: Vec::new(),
            buckets: Some(PdBuckets {
                region_id: 1,
                version: 4,
                keys: vec![b"a".to_vec(), b"m".to_vec()],
                stats: Some(PdBucketStats {
                    read_bytes: vec![1],
                    write_bytes: vec![2],
                    read_qps: vec![3],
                    write_qps: vec![4],
                    read_keys: vec![5],
                    write_keys: vec![6],
                }),
                period_in_ms: 1_000,
            }),
        });

        let buckets = projected.buckets.expect("buckets are projected");
        assert_eq!(buckets.version, 4);
        assert_eq!(buckets.keys, vec![b"a".to_vec(), b"m".to_vec()]);
        assert_eq!(buckets.period_in_ms, 1_000);
        let stats = buckets.stats.expect("bucket stats are projected");
        assert_eq!(stats.read_bytes, vec![1]);
        assert_eq!(stats.write_keys, vec![6]);
    }

    #[test]
    fn store_projection_maps_both_lifecycle_fields() {
        let projected = store(PdStore {
            id: 5,
            address: "127.0.0.1:20160".to_owned(),
            state: PdStoreState::Offline,
            node_state: PdNodeState::Removing,
            labels: vec![("zone".to_owned(), "east".to_owned())],
        });

        assert_eq!(projected.id, 5);
        assert_eq!(projected.address, "127.0.0.1:20160");
        assert_eq!(projected.state, metapb::StoreState::Offline as i32);
        assert_eq!(projected.node_state, metapb::NodeState::Removing as i32);
        assert_eq!(projected.labels.len(), 1);
        assert_eq!(projected.labels[0].key, "zone");
        assert_eq!(projected.labels[0].value, "east");
    }

    #[test]
    fn timestamp_split_inverts_the_pd_composition() {
        // TiDB's TSO worker composes `physical << 18 | logical`; the engine
        // wants the pair back, so the split must invert exactly that.
        let physical = 1_700_000_000_000_i64;
        let logical = 4_095_i64;
        let composed = ((physical as u64) << PHYSICAL_SHIFT_BITS) + logical as u64;

        let split = timestamp(composed);

        assert_eq!(split.physical, physical);
        assert_eq!(split.logical, logical);
        assert_eq!(split.suffix_bits, 0);
    }

    #[test]
    fn timestamp_split_keeps_a_maximal_logical_counter() {
        let logical = (1_u64 << PHYSICAL_SHIFT_BITS) - 1;
        let split = timestamp((9_u64 << PHYSICAL_SHIFT_BITS) | logical);

        assert_eq!(split.physical, 9);
        assert_eq!(split.logical, logical as i64);
    }

    #[test]
    fn key_range_projection_preserves_both_bounds() {
        let projected = key_range(pdpb::KeyRange {
            start_key: b"a".to_vec(),
            end_key: b"b".to_vec(),
        });

        assert_eq!(projected.start_key, b"a".to_vec());
        assert_eq!(projected.end_key, b"b".to_vec());
    }
}
