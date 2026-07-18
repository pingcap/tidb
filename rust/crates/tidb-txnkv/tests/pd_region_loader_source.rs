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
use tidb_txnkv::region::{PeerRole, RegionLoader};
use tidb_txnkv::PdRegionLoader;

const CLUSTER_ID: u64 = 84;

#[derive(Clone)]
struct MockPd {
    state: Arc<Mutex<State>>,
}

struct State {
    region: pdpb::GetRegionResponse,
    stores: HashMap<u64, pdpb::GetStoreResponse>,
    region_requests: Vec<pdpb::GetRegionRequest>,
    store_requests: Vec<pdpb::GetStoreRequest>,
}

#[tonic::async_trait]
impl Pd for MockPd {
    async fn get_members(
        &self,
        request: tonic::Request<pdpb::GetMembersRequest>,
    ) -> Result<tonic::Response<pdpb::GetMembersResponse>, tonic::Status> {
        assert!(request.into_inner().header.is_none());
        Ok(tonic::Response::new(pdpb::GetMembersResponse {
            header: Some(header()),
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
        let service = MockPd {
            state: Arc::clone(&state),
        };
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
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
    let server = Server::start(valid_state());
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();
    assert_eq!(loader.cluster_id(), CLUSTER_ID);
    let location = loader.load_region(b"logical-key").unwrap();

    assert_eq!(location.start_key, b"logical-start");
    assert_eq!(location.end_key, b"logical-end");
    assert_eq!(location.region.id, 7);
    assert_eq!(location.region.epoch.conf_ver, 3);
    assert_eq!(location.region.epoch.version, 4);
    assert_eq!(location.leader_peer_id, Some(11));
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
fn removed_leader_and_malformed_boundary_fail_with_loader_identity() {
    let mut state = valid_state();
    state.stores.insert(
        101,
        store_response(
            101,
            metapb::StoreState::Tombstone,
            metapb::NodeState::Serving,
        ),
    );
    let server = Server::start(state);
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();
    let error = loader.load_region(b"k").unwrap_err();
    assert_eq!(error.identity(), "missing_usable_leader");

    let mut state = valid_state();
    state.region.region.as_mut().unwrap().start_key = vec![1, 2, 3];
    let server = Server::start(state);
    let mut loader = PdRegionLoader::connect(&server.address, Duration::from_secs(2)).unwrap();
    let error = loader.load_region(b"k").unwrap_err();
    assert_eq!(error.identity(), "invalid_region_key");
}

fn valid_state() -> State {
    let peers = vec![
        peer(11, 101, metapb::PeerRole::Voter, false),
        peer(12, 102, metapb::PeerRole::Learner, false),
        peer(13, 103, metapb::PeerRole::Voter, true),
        peer(14, 104, metapb::PeerRole::DemotingVoter, false),
        peer(15, 101, metapb::PeerRole::IncomingVoter, false),
    ];
    State {
        region: pdpb::GetRegionResponse {
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
            pending_peers: Vec::new(),
        },
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
        store_requests: Vec::new(),
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
            node_state: node_state as i32,
        }),
    }
}
