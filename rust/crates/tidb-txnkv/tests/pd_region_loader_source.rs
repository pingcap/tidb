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

#![allow(missing_docs)]

use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_codec::encode_bytes;
use tidb_proto::metapb;
use tidb_proto::pdpb::{
    self,
    pd_server::{Pd, PdServer},
};
use tidb_txnkv::region::{BatchRegionLoader, KeyRange, RegionLoader};
use tidb_txnkv::region::{
    PeerRole, RegionMetadata, RegionMetadataPeer, RegionRecoveryLoader, RegionVerId,
};
use tidb_txnkv::PdRegionLoader;

const CLUSTER_ID: u64 = 84;

#[derive(Clone)]
struct MockPd {
    state: Arc<Mutex<State>>,
    member_url: String,
}

struct State {
    region: pdpb::GetRegionResponse,
    region_by_id: pdpb::GetRegionResponse,
    scan_regions: pdpb::ScanRegionsResponse,
    batch_scan_regions: pdpb::BatchScanRegionsResponse,
    batch_scan_unimplemented: bool,
    stores: HashMap<u64, pdpb::GetStoreResponse>,
    region_requests: Vec<pdpb::GetRegionRequest>,
    region_by_id_requests: Vec<pdpb::GetRegionByIdRequest>,
    scan_region_requests: Vec<pdpb::ScanRegionsRequest>,
    batch_scan_region_requests: Vec<pdpb::BatchScanRegionsRequest>,
    store_requests: Vec<pdpb::GetStoreRequest>,
}

#[tonic::async_trait]
impl Pd for MockPd {
    async fn get_members(
        &self,
        request: tonic::Request<pdpb::GetMembersRequest>,
    ) -> Result<tonic::Response<pdpb::GetMembersResponse>, tonic::Status> {
        assert!(request.into_inner().header.is_none());
        let member = pdpb::Member {
            name: "pd-1".to_owned(),
            member_id: 1,
            client_urls: vec![self.member_url.clone()],
            ..pdpb::Member::default()
        };
        Ok(tonic::Response::new(pdpb::GetMembersResponse {
            header: Some(header()),
            members: vec![member.clone()],
            leader: Some(member),
            ..pdpb::GetMembersResponse::default()
        }))
    }

    async fn get_region(
        &self,
        request: tonic::Request<pdpb::GetRegionRequest>,
    ) -> Result<tonic::Response<pdpb::GetRegionResponse>, tonic::Status> {
        let mut state = self.state.lock().unwrap();
        state.region_requests.push(request.into_inner());
        Ok(tonic::Response::new(state.region.clone()))
    }

    async fn get_region_by_id(
        &self,
        request: tonic::Request<pdpb::GetRegionByIdRequest>,
    ) -> Result<tonic::Response<pdpb::GetRegionResponse>, tonic::Status> {
        let mut state = self.state.lock().unwrap();
        state.region_by_id_requests.push(request.into_inner());
        Ok(tonic::Response::new(state.region_by_id.clone()))
    }

    async fn scan_regions(
        &self,
        request: tonic::Request<pdpb::ScanRegionsRequest>,
    ) -> Result<tonic::Response<pdpb::ScanRegionsResponse>, tonic::Status> {
        let mut state = self.state.lock().unwrap();
        state.scan_region_requests.push(request.into_inner());
        Ok(tonic::Response::new(state.scan_regions.clone()))
    }

    async fn batch_scan_regions(
        &self,
        request: tonic::Request<pdpb::BatchScanRegionsRequest>,
    ) -> Result<tonic::Response<pdpb::BatchScanRegionsResponse>, tonic::Status> {
        let mut state = self.state.lock().unwrap();
        state.batch_scan_region_requests.push(request.into_inner());
        if state.batch_scan_unimplemented {
            return Err(tonic::Status::unimplemented("legacy PD"));
        }
        Ok(tonic::Response::new(state.batch_scan_regions.clone()))
    }

    async fn get_store(
        &self,
        request: tonic::Request<pdpb::GetStoreRequest>,
    ) -> Result<tonic::Response<pdpb::GetStoreResponse>, tonic::Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().unwrap();
        state.store_requests.push(request.clone());
        Ok(tonic::Response::new(
            state.stores.get(&request.store_id).unwrap().clone(),
        ))
    }
}

struct Server {
    address: String,
    state: Arc<Mutex<State>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Server {
    fn start(state: State) -> Self {
        let state = Arc::new(Mutex::new(state));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let service = MockPd {
            state: Arc::clone(&state),
            member_url: format!("http://{address}"),
        };
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let server = tonic::transport::Server::builder()
                    .add_service(PdServer::new(service))
                    .serve_with_shutdown(address, async {
                        let _ = shutdown_rx.await;
                    });
                started_tx.send(()).unwrap();
                server.await.unwrap();
            });
        });
        started_rx.recv().unwrap();
        for _ in 0..100 {
            if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(10)).is_ok() {
                return Self {
                    address: address.to_string(),
                    state,
                    shutdown: Some(shutdown),
                    thread: Some(thread),
                };
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("mock PD did not accept connections");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

#[test]
fn loader_encodes_key_decodes_boundaries_and_filters_source_unusable_peers() {
    // client-go/internal/locate/pd_codec.go:107-112,197-203.
    // client-go/internal/locate/region_cache.go:362-430.
    let mut state = valid_state();
    state
        .stores
        .get_mut(&101)
        .unwrap()
        .store
        .as_mut()
        .unwrap()
        .labels = vec![
        metapb::StoreLabel {
            key: "zone".to_owned(),
            value: "shanghai".to_owned(),
        },
        metapb::StoreLabel {
            key: "disk".to_owned(),
            value: "ssd".to_owned(),
        },
    ];
    let server = Server::start(state);
    let mut loader =
        PdRegionLoader::connect_seeds([server.address.clone()], Duration::from_secs(2)).unwrap();
    assert_eq!(loader.cluster_id(), CLUSTER_ID);
    let location = loader.load_region(b"logical-key").unwrap();

    assert_eq!(location.start_key, b"logical-start");
    assert_eq!(location.end_key, b"logical-end");
    assert_eq!(location.region.id, 7);
    assert_eq!(location.region.epoch.conf_ver, 3);
    assert_eq!(location.region.epoch.version, 4);
    assert_eq!(location.leader_peer_id, Some(11));
    assert_eq!(location.down_peer_ids, [14]);
    assert_eq!(location.pending_peer_ids, [12]);
    let buckets = location.buckets.as_ref().unwrap();
    assert_eq!(buckets.region_id, 7);
    assert_eq!(buckets.version, 8);
    assert_eq!(
        buckets.keys,
        vec![
            b"logical-start".to_vec(),
            b"logical-split".to_vec(),
            b"logical-end".to_vec()
        ]
    );
    assert_eq!(buckets.stats.as_ref().unwrap().read_bytes, [1, 2]);
    assert_eq!(buckets.stats.as_ref().unwrap().write_keys, [11, 12]);
    assert_eq!(buckets.period_in_ms, 1_000);
    assert_eq!(
        location
            .peers
            .iter()
            .map(|peer| peer.id)
            .collect::<Vec<_>>(),
        [11, 12, 15]
    );
    assert_eq!(
        location
            .peers
            .iter()
            .map(|peer| peer.role)
            .collect::<Vec<_>>(),
        [PeerRole::Voter, PeerRole::Learner, PeerRole::IncomingVoter]
    );
    assert!(location.peers.iter().all(|peer| peer.store_epoch == 0));
    assert_eq!(
        location
            .stores
            .iter()
            .map(|store| store.id)
            .collect::<Vec<_>>(),
        [101, 102]
    );
    assert!(location.stores.iter().all(|store| store.epoch == 0));
    assert_eq!(
        loader.store_labels(101),
        [
            ("zone".to_owned(), "shanghai".to_owned()),
            ("disk".to_owned(), "ssd".to_owned()),
        ]
    );
    assert!(loader.store_labels(102).is_empty());

    let state = server.state.lock().unwrap();
    assert_eq!(state.region_requests.len(), 1);
    let mut expected_key = Vec::new();
    encode_bytes(&mut expected_key, b"logical-key");
    assert_eq!(state.region_requests[0].region_key, expected_key);
    assert!(state.region_requests[0].need_buckets);
    // Four unique referenced stores are all resolved once, including stores
    // later filtered because their peers are down or non-leader witnesses.
    assert_eq!(
        state
            .store_requests
            .iter()
            .map(|request| request.store_id)
            .collect::<Vec<_>>(),
        [101, 102, 103, 104]
    );
}

#[test]
fn loader_decodes_by_id_scan_and_batch_regions_once_without_synthetic_buckets() {
    // client-go/internal/locate/pd_codec.go:114-171.
    let server = Server::start(valid_state());
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();

    let by_id = loader.load_region_by_id(7, false).unwrap();
    assert_eq!(by_id.start_key, b"logical-start");
    assert_eq!(by_id.buckets.as_ref().unwrap().keys[1], b"logical-split");

    let scan = loader.scan_regions(b"a", b"z", 9).unwrap();
    assert_eq!(
        scan.iter()
            .map(|region| region.region.id)
            .collect::<Vec<_>>(),
        [21, 22]
    );
    assert_eq!(scan[0].start_key, b"a");
    assert_eq!(scan[0].end_key, b"m");
    assert_eq!(scan[0].pending_peer_ids, [212]);
    assert_eq!(
        scan[0].buckets.as_ref().unwrap().keys,
        vec![b"a".to_vec(), b"m".to_vec()]
    );
    assert!(scan[1].buckets.is_none());

    let ranges = [
        KeyRange::new(b"a".to_vec(), b"m".to_vec()),
        KeyRange::new(b"q".to_vec(), Vec::new()),
    ];
    let batch = loader.batch_load_regions(&ranges, 13, true).unwrap();
    assert_eq!(
        batch
            .iter()
            .map(|region| region.region.id)
            .collect::<Vec<_>>(),
        [31, 32]
    );
    assert_eq!(batch[0].buckets.as_ref().unwrap().version, 41);
    assert!(batch[1].buckets.is_none());

    let state = server.state.lock().unwrap();
    assert_eq!(state.region_by_id_requests.len(), 1);
    assert_eq!(state.region_by_id_requests[0].region_id, 7);
    assert!(!state.region_by_id_requests[0].need_buckets);

    assert_eq!(state.scan_region_requests.len(), 1);
    assert_eq!(state.scan_region_requests[0].start_key, encoded(b"a"));
    assert_eq!(state.scan_region_requests[0].end_key, encoded(b"z"));
    assert_eq!(state.scan_region_requests[0].limit, 9);

    assert_eq!(state.batch_scan_region_requests.len(), 1);
    let request = &state.batch_scan_region_requests[0];
    assert_eq!(request.limit, 13);
    assert!(request.need_buckets);
    assert!(request.contain_all_key_range);
    assert_eq!(request.ranges.len(), 2);
    assert_eq!(request.ranges[0].start_key, encoded(b"a"));
    assert_eq!(request.ranges[0].end_key, encoded(b"m"));
    assert_eq!(request.ranges[1].start_key, encoded(b"q"));
    assert!(request.ranges[1].end_key.is_empty());
}

#[test]
fn batch_loader_falls_back_only_from_unimplemented_and_does_not_invent_buckets() {
    // client-go/internal/locate/region_cache.go:2620-2625,2737-2768.
    let mut state = valid_state();
    state.batch_scan_unimplemented = true;
    for region in &mut state.scan_regions.regions {
        region.buckets = None;
    }
    let server = Server::start(state);
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();

    let regions = loader
        .batch_load_regions(&[KeyRange::new(b"a".to_vec(), b"z".to_vec())], 2, true)
        .unwrap();
    assert_eq!(
        regions
            .iter()
            .map(|region| region.region.id)
            .collect::<Vec<_>>(),
        [21, 22]
    );
    assert!(regions.iter().all(|region| region.buckets.is_none()));

    let state = server.state.lock().unwrap();
    assert_eq!(state.batch_scan_region_requests.len(), 1);
    assert_eq!(state.scan_region_requests.len(), 1);
    assert_eq!(state.scan_region_requests[0].limit, 2);
}

#[test]
fn leaderless_region_is_routable_and_malformed_boundary_fails_with_loader_identity() {
    let mut state = valid_state();
    state.region.leader = None;
    let server = Server::start(state);
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();
    let location = loader.load_region(b"logical-key").unwrap();
    assert_eq!(location.leader_peer_id, None);
    assert_eq!(
        location
            .peers
            .iter()
            .map(|peer| peer.id)
            .collect::<Vec<_>>(),
        [11, 12, 15]
    );

    let mut state = valid_state();
    state.region.region.as_mut().unwrap().start_key = vec![1, 2, 3];
    let server = Server::start(state);
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();
    let error = loader.load_region(b"k").unwrap_err();
    assert_eq!(error.identity(), "invalid_region_key");
}

#[test]
fn removed_follower_is_filtered_without_hiding_a_healthy_leader() {
    let mut state = valid_state();
    state.stores.insert(
        102,
        pdpb::GetStoreResponse {
            header: Some(pdpb::ResponseHeader {
                cluster_id: CLUSTER_ID,
                error: Some(pdpb::Error {
                    r#type: pdpb::ErrorType::StoreTombstone as i32,
                    message: "removed follower".to_owned(),
                }),
            }),
            store: None,
        },
    );
    let server = Server::start(state);
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();

    let location = loader.load_region(b"logical-key").unwrap();
    assert_eq!(location.leader_peer_id, Some(11));
    assert!(location.peers.iter().any(|peer| peer.id == 11));
    assert!(location.peers.iter().all(|peer| peer.store_id != 102));
    assert!(location.stores.iter().all(|store| store.id != 102));
}

#[test]
fn unknown_leader_role_reaches_the_raw_context_domain() {
    let mut state = valid_state();
    state.region.region.as_mut().unwrap().peers[0].role = 99;
    state.region.leader.as_mut().unwrap().role = 99;
    let server = Server::start(state);
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();

    let location = loader.load_region(b"logical-key").unwrap();
    let leader = location
        .peers
        .iter()
        .find(|peer| Some(peer.id) == location.leader_peer_id)
        .unwrap();
    assert_eq!(leader.role, PeerRole::Unknown(99));
    assert_eq!(leader.role.as_i32(), 99);
}

#[test]
fn current_region_hydration_reresolves_stores_and_preserves_unknown_roles() {
    let mut state = valid_state();
    state
        .stores
        .get_mut(&101)
        .unwrap()
        .store
        .as_mut()
        .unwrap()
        .address = "127.0.0.1:31001".to_owned();
    let server = Server::start(state);
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();
    let metadata = RegionMetadata {
        region: RegionVerId::new(8, 4, 5),
        encoded_start_key: encoded(b"split-start"),
        encoded_end_key: encoded(b"split-end"),
        peers: vec![
            RegionMetadataPeer {
                id: 21,
                store_id: 101,
                role: PeerRole::Unknown(99),
                is_witness: false,
            },
            RegionMetadataPeer {
                id: 22,
                store_id: 102,
                role: PeerRole::Learner,
                is_witness: false,
            },
        ],
    };

    let hydrated = loader.hydrate_region(&metadata, 101).unwrap();
    assert_eq!(hydrated.start_key, b"split-start");
    assert_eq!(hydrated.end_key, b"split-end");
    assert_eq!(hydrated.leader_peer_id, Some(21));
    assert_eq!(hydrated.peers.len(), 2);
    assert_eq!(hydrated.peers[0].role, PeerRole::Unknown(99));
    assert_eq!(hydrated.peers[1].role, PeerRole::Learner);
    assert_eq!(hydrated.stores[0].address, "127.0.0.1:31001");
    assert_eq!(
        server
            .state
            .lock()
            .unwrap()
            .store_requests
            .iter()
            .map(|request| request.store_id)
            .collect::<Vec<_>>(),
        [101, 102]
    );
}

#[test]
fn split_child_without_old_store_keeps_client_go_first_usable_peer() {
    let server = Server::start(valid_state());
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();
    let metadata = RegionMetadata {
        region: RegionVerId::new(9, 4, 5),
        encoded_start_key: encoded(b"middle"),
        encoded_end_key: Vec::new(),
        peers: vec![
            RegionMetadataPeer {
                id: 22,
                store_id: 102,
                role: PeerRole::Learner,
                is_witness: false,
            },
            RegionMetadataPeer {
                id: 23,
                store_id: 104,
                role: PeerRole::Voter,
                is_witness: false,
            },
        ],
    };

    let hydrated = loader.hydrate_region(&metadata, 101).unwrap();
    assert_eq!(hydrated.leader_peer_id, Some(22));
    assert_eq!(
        hydrated
            .peers
            .iter()
            .map(|peer| peer.id)
            .collect::<Vec<_>>(),
        [22, 23]
    );
}

#[test]
fn epoch_hydration_never_preserves_a_witness_as_the_observed_leader() {
    let server = Server::start(valid_state());
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();
    let metadata = RegionMetadata {
        region: RegionVerId::new(9, 4, 5),
        encoded_start_key: encoded(b"middle"),
        encoded_end_key: Vec::new(),
        peers: vec![
            RegionMetadataPeer {
                id: 13,
                store_id: 103,
                role: PeerRole::Voter,
                is_witness: true,
            },
            RegionMetadataPeer {
                id: 14,
                store_id: 104,
                role: PeerRole::DemotingVoter,
                is_witness: false,
            },
        ],
    };

    let hydrated = loader.hydrate_region(&metadata, 103).unwrap();
    assert_eq!(hydrated.leader_peer_id, Some(14));
    assert_eq!(
        hydrated
            .peers
            .iter()
            .map(|peer| peer.id)
            .collect::<Vec<_>>(),
        [14]
    );
}

fn valid_state() -> State {
    let peers = vec![
        peer(11, 101, metapb::PeerRole::Voter, false),
        peer(12, 102, metapb::PeerRole::Learner, false),
        peer(13, 103, metapb::PeerRole::Voter, true),
        peer(14, 104, metapb::PeerRole::DemotingVoter, false),
        peer(15, 101, metapb::PeerRole::IncomingVoter, false),
    ];
    let region = pdpb::GetRegionResponse {
        header: Some(header()),
        region: Some(metapb::Region {
            id: 7,
            start_key: encoded(b"logical-start"),
            end_key: encoded(b"logical-end"),
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: 3,
                version: 4,
            }),
            peers: peers.clone(),
        }),
        leader: Some(peers[0]),
        down_peers: vec![pdpb::PeerStats {
            peer: Some(peers[3]),
            down_seconds: 10,
        }],
        pending_peers: vec![peers[1]],
        buckets: Some(bucket_metadata(
            7,
            8,
            [
                b"logical-start".as_slice(),
                b"logical-split",
                b"logical-end",
            ],
        )),
    };
    State {
        region: region.clone(),
        region_by_id: region,
        scan_regions: pdpb::ScanRegionsResponse {
            header: Some(header()),
            region_metas: Vec::new(),
            leaders: Vec::new(),
            regions: vec![
                extended_region(21, b"a", b"m", true),
                extended_region(22, b"m", b"z", false),
            ],
        },
        batch_scan_regions: pdpb::BatchScanRegionsResponse {
            header: Some(header()),
            regions: vec![
                extended_region(31, b"a", b"m", true),
                extended_region(32, b"m", b"z", false),
            ],
        },
        batch_scan_unimplemented: false,
        stores: HashMap::from([
            (
                101,
                store_response(101, metapb::StoreState::Up, metapb::NodeState::Serving),
            ),
            (
                102,
                store_response(
                    102,
                    metapb::StoreState::Offline,
                    metapb::NodeState::Removing,
                ),
            ),
            (
                103,
                store_response(103, metapb::StoreState::Up, metapb::NodeState::Preparing),
            ),
            (
                104,
                store_response(104, metapb::StoreState::Up, metapb::NodeState::Serving),
            ),
        ]),
        region_requests: Vec::new(),
        region_by_id_requests: Vec::new(),
        scan_region_requests: Vec::new(),
        batch_scan_region_requests: Vec::new(),
        store_requests: Vec::new(),
    }
}

fn bucket_metadata<const N: usize>(
    region_id: u64,
    version: u64,
    keys: [&[u8]; N],
) -> metapb::Buckets {
    metapb::Buckets {
        region_id,
        version,
        keys: keys.into_iter().map(encoded).collect(),
        stats: Some(metapb::BucketStats {
            read_bytes: vec![1, 2],
            write_bytes: vec![3, 4],
            read_qps: vec![5, 6],
            write_qps: vec![7, 8],
            read_keys: vec![9, 10],
            write_keys: vec![11, 12],
        }),
        period_in_ms: 1_000,
    }
}

fn extended_region(id: u64, start: &[u8], end: &[u8], with_buckets: bool) -> pdpb::Region {
    let leader = peer(id * 10 + 1, 101, metapb::PeerRole::Voter, false);
    let pending = peer(id * 10 + 2, 102, metapb::PeerRole::Learner, false);
    pdpb::Region {
        region: Some(metapb::Region {
            id,
            start_key: encoded(start),
            end_key: encoded(end),
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: id + 1,
                version: id + 2,
            }),
            peers: vec![leader.clone(), pending.clone()],
        }),
        leader: Some(leader),
        down_peers: Vec::new(),
        pending_peers: vec![pending],
        buckets: with_buckets.then(|| bucket_metadata(id, id + 10, [start, end])),
    }
}

fn header() -> pdpb::ResponseHeader {
    pdpb::ResponseHeader {
        cluster_id: CLUSTER_ID,
        error: None,
    }
}

fn peer(id: u64, store_id: u64, role: metapb::PeerRole, witness: bool) -> metapb::Peer {
    metapb::Peer {
        id,
        store_id,
        role: role as i32,
        is_witness: witness,
    }
}

fn encoded(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encode_bytes(&mut encoded, key);
    encoded
}

fn store_response(
    id: u64,
    state: metapb::StoreState,
    node_state: metapb::NodeState,
) -> pdpb::GetStoreResponse {
    pdpb::GetStoreResponse {
        header: Some(header()),
        store: Some(metapb::Store {
            id,
            address: format!("127.0.0.1:{}", 20000 + id),
            state: state as i32,
            labels: Vec::new(),
            node_state: node_state as i32,
        }),
    }
}
