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

use tidb_pd_client::{
    PdClient, PdNodeState, PdPeerRole, PdStoreState, GET_MEMBERS_PATH, GET_REGION_PATH,
    GET_STORE_PATH,
};
use tidb_proto::metapb;
use tidb_proto::pdpb::{
    self,
    pd_server::{Pd, PdServer},
};

const CLUSTER_ID: u64 = 42;

#[derive(Clone)]
enum Reply<T> {
    Value(T),
    Status(tonic::Code, &'static str),
    Delayed(Duration, T),
}

impl<T> Reply<T> {
    async fn send(&self) -> Result<tonic::Response<T>, tonic::Status>
    where
        T: Clone,
    {
        match self {
            Self::Value(value) => Ok(tonic::Response::new(value.clone())),
            Self::Status(code, message) => Err(tonic::Status::new(*code, *message)),
            Self::Delayed(delay, value) => {
                tokio::time::sleep(*delay).await;
                Ok(tonic::Response::new(value.clone()))
            }
        }
    }
}

struct State {
    members: Reply<pdpb::GetMembersResponse>,
    region: Reply<pdpb::GetRegionResponse>,
    stores: HashMap<u64, Reply<pdpb::GetStoreResponse>>,
    member_requests: Vec<pdpb::GetMembersRequest>,
    region_requests: Vec<pdpb::GetRegionRequest>,
    store_requests: Vec<pdpb::GetStoreRequest>,
}

#[derive(Clone)]
struct MockPd {
    state: Arc<Mutex<State>>,
}

#[tonic::async_trait]
impl Pd for MockPd {
    async fn get_members(
        &self,
        request: tonic::Request<pdpb::GetMembersRequest>,
    ) -> Result<tonic::Response<pdpb::GetMembersResponse>, tonic::Status> {
        let reply = {
            let mut state = self.state.lock().unwrap();
            state.member_requests.push(request.into_inner());
            state.members.clone()
        };
        reply.send().await
    }

    async fn get_store(
        &self,
        request: tonic::Request<pdpb::GetStoreRequest>,
    ) -> Result<tonic::Response<pdpb::GetStoreResponse>, tonic::Status> {
        let request = request.into_inner();
        let reply = {
            let mut state = self.state.lock().unwrap();
            state.store_requests.push(request.clone());
            state
                .stores
                .get(&request.store_id)
                .cloned()
                .unwrap_or_else(|| {
                    Reply::Value(pdpb::GetStoreResponse {
                        header: Some(header(CLUSTER_ID)),
                        store: None,
                    })
                })
        };
        reply.send().await
    }

    async fn get_region(
        &self,
        request: tonic::Request<pdpb::GetRegionRequest>,
    ) -> Result<tonic::Response<pdpb::GetRegionResponse>, tonic::Status> {
        let reply = {
            let mut state = self.state.lock().unwrap();
            state.region_requests.push(request.into_inner());
            state.region.clone()
        };
        reply.send().await
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

fn header(cluster_id: u64) -> pdpb::ResponseHeader {
    pdpb::ResponseHeader {
        cluster_id,
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

fn region_response() -> pdpb::GetRegionResponse {
    let peers = vec![
        peer(11, 101, metapb::PeerRole::Voter, false),
        peer(12, 102, metapb::PeerRole::Learner, true),
        peer(13, 103, metapb::PeerRole::IncomingVoter, false),
        peer(14, 104, metapb::PeerRole::DemotingVoter, false),
    ];
    pdpb::GetRegionResponse {
        header: Some(header(CLUSTER_ID)),
        region: Some(metapb::Region {
            id: 7,
            start_key: b"wire-start".to_vec(),
            end_key: b"wire-end".to_vec(),
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: 3,
                version: 4,
            }),
            peers: peers.clone(),
        }),
        // isSamePeer in client-go compares only peer and store identities;
        // region metadata remains authoritative for role and witness fields.
        leader: Some(peer(11, 101, metapb::PeerRole::Learner, true)),
        down_peers: vec![pdpb::PeerStats {
            peer: Some(peer(14, 104, metapb::PeerRole::Voter, true)),
            down_seconds: 9,
        }],
        pending_peers: vec![peers[2]],
    }
}

fn store_response(
    id: u64,
    state: metapb::StoreState,
    node_state: metapb::NodeState,
) -> pdpb::GetStoreResponse {
    pdpb::GetStoreResponse {
        header: Some(header(CLUSTER_ID)),
        store: Some(metapb::Store {
            id,
            address: format!("127.0.0.1:{}", 20000 + id),
            state: state as i32,
            node_state: node_state as i32,
        }),
    }
}

fn valid_state() -> State {
    State {
        members: Reply::Value(pdpb::GetMembersResponse {
            header: Some(header(CLUSTER_ID)),
            ..pdpb::GetMembersResponse::default()
        }),
        region: Reply::Value(region_response()),
        stores: HashMap::from([
            (
                101,
                Reply::Value(store_response(
                    101,
                    metapb::StoreState::Up,
                    metapb::NodeState::Serving,
                )),
            ),
            (
                102,
                Reply::Value(store_response(
                    102,
                    metapb::StoreState::Offline,
                    metapb::NodeState::Removing,
                )),
            ),
        ]),
        member_requests: Vec::new(),
        region_requests: Vec::new(),
        store_requests: Vec::new(),
    }
}

#[test]
fn exact_methods_headers_wire_key_roles_and_store_states_are_preserved_once() {
    // servicediscovery/service_discovery.go:960-994 getMembers.
    // client.go:714-764 GetRegion; client.go:1034-1091 GetStore.
    assert_eq!(GET_MEMBERS_PATH, "/pdpb.PD/GetMembers");
    assert_eq!(GET_REGION_PATH, "/pdpb.PD/GetRegion");
    assert_eq!(GET_STORE_PATH, "/pdpb.PD/GetStore");

    let server = Server::start(valid_state());
    let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();
    assert_eq!(client.cluster_id(), CLUSTER_ID);
    let wire_key = b"\x74\x80wire-key";
    let region = client.get_region(wire_key).unwrap();
    assert_eq!(region.start_key, b"wire-start");
    assert_eq!(region.end_key, b"wire-end");
    assert_eq!(
        region
            .peers
            .iter()
            .map(|peer| peer.role)
            .collect::<Vec<_>>(),
        [
            PdPeerRole::Voter,
            PdPeerRole::Learner,
            PdPeerRole::IncomingVoter,
            PdPeerRole::DemotingVoter,
        ]
    );
    assert!(region.peers[1].is_witness);
    assert_eq!(region.leader.role, PdPeerRole::Voter);
    assert!(!region.leader.is_witness);
    assert_eq!(region.down_peer_ids, [14]);

    let up = client.get_store(101).unwrap().unwrap();
    assert_eq!(up.state, PdStoreState::Up);
    assert_eq!(up.node_state, PdNodeState::Serving);
    let removing = client.get_store(102).unwrap().unwrap();
    assert_eq!(removing.state, PdStoreState::Offline);
    assert_eq!(removing.node_state, PdNodeState::Removing);

    let state = server.state.lock().unwrap();
    assert_eq!(state.member_requests.len(), 1);
    assert_eq!(state.member_requests[0].header, None);
    assert_eq!(state.region_requests.len(), 1);
    let region_request = &state.region_requests[0];
    assert_eq!(region_request.region_key, wire_key);
    assert!(region_request.need_buckets);
    assert_exact_header(region_request.header.as_ref().unwrap());
    assert_eq!(state.store_requests.len(), 2);
    assert_eq!(state.store_requests[0].store_id, 101);
    assert_eq!(state.store_requests[1].store_id, 102);
    assert!(state.store_requests.iter().all(|request| {
        assert_exact_header(request.header.as_ref().unwrap());
        true
    }));
}

#[test]
fn sync_client_is_safe_inside_an_existing_tokio_runtime() {
    let server = Server::start(valid_state());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();
        client.get_region(b"already-encoded").unwrap();
    });
    let state = server.state.lock().unwrap();
    assert_eq!(state.member_requests.len(), 1);
    assert_eq!(state.region_requests.len(), 1);
}

#[test]
fn present_zero_epoch_is_preserved_without_invented_validation() {
    let mut response = region_response();
    response.region.as_mut().unwrap().region_epoch = Some(metapb::RegionEpoch {
        conf_ver: 0,
        version: 0,
    });
    let mut state = valid_state();
    state.region = Reply::Value(response);
    let server = Server::start(state);
    let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();
    let region = client.get_region(b"wire").unwrap();
    assert_eq!(region.epoch.conf_ver, 0);
    assert_eq!(region.epoch.version, 0);
    assert_eq!(server.state.lock().unwrap().region_requests.len(), 1);
}

#[test]
fn bootstrap_timeout_transport_header_and_zero_cluster_never_retry() {
    let cases = [
        (
            Reply::Delayed(
                Duration::from_millis(100),
                pdpb::GetMembersResponse {
                    header: Some(header(CLUSTER_ID)),
                    ..pdpb::GetMembersResponse::default()
                },
            ),
            "timeout",
        ),
        (
            Reply::Status(tonic::Code::Unavailable, "unavailable"),
            "transport",
        ),
        (
            Reply::Value(pdpb::GetMembersResponse {
                header: None,
                ..pdpb::GetMembersResponse::default()
            }),
            "missing_header",
        ),
        (
            Reply::Value(pdpb::GetMembersResponse {
                header: Some(pdpb::ResponseHeader {
                    cluster_id: CLUSTER_ID,
                    error: Some(pdpb::Error {
                        r#type: pdpb::ErrorType::NotBootstrapped as i32,
                        message: "not bootstrapped".to_owned(),
                    }),
                }),
                ..pdpb::GetMembersResponse::default()
            }),
            "header_error",
        ),
        (
            Reply::Value(pdpb::GetMembersResponse {
                header: Some(header(0)),
                ..pdpb::GetMembersResponse::default()
            }),
            "zero_cluster_id",
        ),
    ];
    for (members, expected) in cases {
        let mut state = valid_state();
        state.members = members;
        let server = Server::start(state);
        let error = PdClient::connect(&server.address, Duration::from_millis(20))
            .err()
            .expect("bootstrap must fail");
        assert_eq!(error.kind(), expected, "unexpected error: {error}");
        assert_eq!(server.state.lock().unwrap().member_requests.len(), 1);
    }
}

#[test]
fn region_header_and_topology_errors_fail_after_exactly_one_attempt() {
    let server = Server::start(valid_state());
    let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();
    let cases = [
        (
            pdpb::GetRegionResponse {
                header: None,
                ..region_response()
            },
            "missing_header",
        ),
        (
            pdpb::GetRegionResponse {
                header: Some(header(CLUSTER_ID + 1)),
                ..region_response()
            },
            "cluster_mismatch",
        ),
        (
            pdpb::GetRegionResponse {
                header: Some(pdpb::ResponseHeader {
                    cluster_id: CLUSTER_ID,
                    error: Some(pdpb::Error {
                        r#type: pdpb::ErrorType::RegionNotFound as i32,
                        message: "not found".to_owned(),
                    }),
                }),
                ..region_response()
            },
            "header_error",
        ),
        (
            pdpb::GetRegionResponse {
                region: None,
                ..region_response()
            },
            "missing_region",
        ),
        (
            pdpb::GetRegionResponse {
                region: Some(metapb::Region {
                    id: 0,
                    ..region_response().region.unwrap()
                }),
                ..region_response()
            },
            "zero_region_id",
        ),
        (
            pdpb::GetRegionResponse {
                region: Some(metapb::Region {
                    peers: Vec::new(),
                    ..region_response().region.unwrap()
                }),
                ..region_response()
            },
            "missing_peers",
        ),
        (
            pdpb::GetRegionResponse {
                region: Some(metapb::Region {
                    region_epoch: None,
                    ..region_response().region.unwrap()
                }),
                ..region_response()
            },
            "missing_region_epoch",
        ),
        (
            pdpb::GetRegionResponse {
                leader: None,
                ..region_response()
            },
            "missing_leader",
        ),
        (
            pdpb::GetRegionResponse {
                leader: Some(peer(99, 101, metapb::PeerRole::Voter, false)),
                ..region_response()
            },
            "leader_not_in_peers",
        ),
        (
            pdpb::GetRegionResponse {
                region: Some(metapb::Region {
                    peers: vec![metapb::Peer {
                        role: 99,
                        ..peer(11, 101, metapb::PeerRole::Voter, false)
                    }],
                    ..region_response().region.unwrap()
                }),
                ..region_response()
            },
            "invalid_peer_role",
        ),
    ];
    for (response, expected) in cases {
        let before = server.state.lock().unwrap().region_requests.len();
        server.state.lock().unwrap().region = Reply::Value(response);
        let error = client.get_region(b"wire").unwrap_err();
        assert_eq!(error.kind(), expected, "unexpected error: {error}");
        assert_eq!(
            server.state.lock().unwrap().region_requests.len(),
            before + 1
        );
    }
}

#[test]
fn store_mismatch_unusable_and_unknown_states_fail_closed_without_retry() {
    let server = Server::start(valid_state());
    let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();
    let cases = [
        (
            pdpb::GetStoreResponse {
                header: None,
                store: store_response(201, metapb::StoreState::Up, metapb::NodeState::Serving)
                    .store,
            },
            "missing_header",
        ),
        (
            pdpb::GetStoreResponse {
                header: Some(pdpb::ResponseHeader {
                    cluster_id: CLUSTER_ID,
                    error: Some(pdpb::Error {
                        r#type: pdpb::ErrorType::StoreTombstone as i32,
                        message: "gone".to_owned(),
                    }),
                }),
                store: None,
            },
            "header_error",
        ),
        (
            pdpb::GetStoreResponse {
                header: Some(header(CLUSTER_ID + 1)),
                store: store_response(201, metapb::StoreState::Up, metapb::NodeState::Serving)
                    .store,
            },
            "cluster_mismatch",
        ),
        (
            pdpb::GetStoreResponse {
                store: Some(metapb::Store {
                    address: "invalid address\n".to_owned(),
                    ..store_response(201, metapb::StoreState::Up, metapb::NodeState::Serving)
                        .store
                        .unwrap()
                }),
                ..store_response(201, metapb::StoreState::Up, metapb::NodeState::Serving)
            },
            "invalid_store_address",
        ),
        (
            pdpb::GetStoreResponse {
                header: Some(header(CLUSTER_ID)),
                store: None,
            },
            "missing_store",
        ),
        (
            store_response(999, metapb::StoreState::Up, metapb::NodeState::Serving),
            "store_id_mismatch",
        ),
        (
            pdpb::GetStoreResponse {
                store: Some(metapb::Store {
                    address: String::new(),
                    ..store_response(201, metapb::StoreState::Up, metapb::NodeState::Serving)
                        .store
                        .unwrap()
                }),
                ..store_response(201, metapb::StoreState::Up, metapb::NodeState::Serving)
            },
            "empty_store_address",
        ),
        (
            pdpb::GetStoreResponse {
                store: Some(metapb::Store {
                    state: 99,
                    ..store_response(201, metapb::StoreState::Up, metapb::NodeState::Serving)
                        .store
                        .unwrap()
                }),
                ..store_response(201, metapb::StoreState::Up, metapb::NodeState::Serving)
            },
            "invalid_store_state",
        ),
        (
            pdpb::GetStoreResponse {
                store: Some(metapb::Store {
                    node_state: 99,
                    ..store_response(201, metapb::StoreState::Up, metapb::NodeState::Serving)
                        .store
                        .unwrap()
                }),
                ..store_response(201, metapb::StoreState::Up, metapb::NodeState::Serving)
            },
            "invalid_node_state",
        ),
    ];
    for (response, expected) in cases {
        server
            .state
            .lock()
            .unwrap()
            .stores
            .insert(201, Reply::Value(response));
        let before = server.state.lock().unwrap().store_requests.len();
        let error = client.get_store(201).unwrap_err();
        assert_eq!(error.kind(), expected, "unexpected error: {error}");
        assert_eq!(
            server.state.lock().unwrap().store_requests.len(),
            before + 1
        );
    }

    for response in [
        store_response(
            201,
            metapb::StoreState::Tombstone,
            metapb::NodeState::Serving,
        ),
        store_response(201, metapb::StoreState::Up, metapb::NodeState::Removed),
    ] {
        server
            .state
            .lock()
            .unwrap()
            .stores
            .insert(201, Reply::Value(response));
        assert_eq!(client.get_store(201).unwrap(), None);
    }
}

#[test]
fn region_and_store_transport_or_timeout_make_one_attempt_only() {
    let server = Server::start(valid_state());
    let mut client = PdClient::connect(&server.address, Duration::from_millis(20)).unwrap();

    server.state.lock().unwrap().region =
        Reply::Status(tonic::Code::Unavailable, "region unavailable");
    assert_eq!(client.get_region(b"wire").unwrap_err().kind(), "transport");
    assert_eq!(server.state.lock().unwrap().region_requests.len(), 1);

    server.state.lock().unwrap().region =
        Reply::Delayed(Duration::from_millis(100), region_response());
    assert_eq!(client.get_region(b"wire").unwrap_err().kind(), "timeout");
    assert_eq!(server.state.lock().unwrap().region_requests.len(), 2);

    server.state.lock().unwrap().stores.insert(
        301,
        Reply::Status(tonic::Code::Unavailable, "store unavailable"),
    );
    assert_eq!(client.get_store(301).unwrap_err().kind(), "transport");
    assert_eq!(server.state.lock().unwrap().store_requests.len(), 1);

    server.state.lock().unwrap().stores.insert(
        302,
        Reply::Delayed(
            Duration::from_millis(100),
            store_response(302, metapb::StoreState::Up, metapb::NodeState::Serving),
        ),
    );
    assert_eq!(client.get_store(302).unwrap_err().kind(), "timeout");
    assert_eq!(server.state.lock().unwrap().store_requests.len(), 2);
}

fn assert_exact_header(header: &pdpb::RequestHeader) {
    assert_eq!(header.cluster_id, CLUSTER_ID);
    assert_eq!(header.sender_id, 0);
    assert!(header.caller_id.is_empty());
    assert_eq!(header.caller_component, "codec-pd-client");
}
