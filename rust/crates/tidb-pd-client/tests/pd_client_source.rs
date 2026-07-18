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
    PdClient, PdNodeState, PdStoreState, GET_MEMBERS_PATH, GET_REGION_PATH, GET_STORE_PATH,
};
use tidb_proto::metapb;
use tidb_proto::pdpb::{
    self,
    pd_server::{Pd, PdServer},
};

const CLUSTER_ID: u64 = 42;
const SELF_URL: &str = "http://127.0.0.1:0";

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
    fn start(mut state: State) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        replace_self_urls(&mut state.members, &format!("http://{address}"));
        let state = Arc::new(Mutex::new(state));
        let service = MockPd {
            state: Arc::clone(&state),
        };
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

fn replace_self_urls(reply: &mut Reply<pdpb::GetMembersResponse>, address: &str) {
    let response = match reply {
        Reply::Value(response) | Reply::Delayed(_, response) => response,
        Reply::Status(_, _) => return,
    };
    for member in &mut response.members {
        for url in &mut member.client_urls {
            if url == SELF_URL {
                *url = address.to_owned();
            }
        }
    }
    if let Some(leader) = &mut response.leader {
        for url in &mut leader.client_urls {
            if url == SELF_URL {
                *url = address.to_owned();
            }
        }
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
            members: vec![pd_member(1, [SELF_URL])],
            leader: Some(pd_member(1, [SELF_URL])),
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

fn pd_member<const N: usize>(member_id: u64, urls: [&str; N]) -> pdpb::Member {
    pdpb::Member {
        name: format!("pd-{member_id}"),
        member_id,
        client_urls: urls.into_iter().map(str::to_owned).collect(),
        ..pdpb::Member::default()
    }
}

fn membership_response(
    cluster_id: u64,
    members: &[(u64, String)],
    leader_id: u64,
) -> pdpb::GetMembersResponse {
    let members = members
        .iter()
        .map(|(member_id, url)| pdpb::Member {
            name: format!("pd-{member_id}"),
            member_id: *member_id,
            client_urls: vec![url.clone()],
            ..pdpb::Member::default()
        })
        .collect::<Vec<_>>();
    let leader = members
        .iter()
        .find(|member| member.member_id == leader_id)
        .cloned();
    pdpb::GetMembersResponse {
        header: Some(header(cluster_id)),
        members,
        leader,
        ..pdpb::GetMembersResponse::default()
    }
}

fn http_url(server: &Server) -> String {
    format!("http://{}", server.address)
}

fn unused_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    address
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
    assert_eq!(client.endpoint(), server.address);
    assert_eq!(client.member_set().leader_url, http_url(&server));
    assert_eq!(client.member_set().member_urls, [http_url(&server)]);
    assert_eq!(client.active_endpoint(), http_url(&server));
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
        [0, 1, 2, 3]
    );
    assert!(region.peers[1].is_witness);
    assert_eq!(region.leader.role, 0);
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
fn one_or_more_seeds_bootstrap_and_validate_every_reachable_cluster() {
    // servicediscovery/service_discovery.go:840-864 initClusterID.
    let survivor = Server::start(valid_state());
    let unavailable = unused_address();
    let client = PdClient::connect_seeds(
        [unavailable, survivor.address.clone()],
        Duration::from_millis(30),
    )
    .unwrap();
    assert_eq!(client.cluster_id(), CLUSTER_ID);
    assert_eq!(client.member_set().leader_url, http_url(&survivor));
    assert_eq!(survivor.state.lock().unwrap().member_requests.len(), 1);

    let mismatched = Server::start(valid_state());
    let mismatched_url = http_url(&mismatched);
    mismatched.state.lock().unwrap().members = Reply::Value(membership_response(
        CLUSTER_ID + 1,
        &[(2, mismatched_url)],
        2,
    ));
    for seeds in [
        [survivor.address.clone(), mismatched.address.clone()],
        [mismatched.address.clone(), survivor.address.clone()],
    ] {
        let error = PdClient::connect_seeds(seeds, Duration::from_secs(2))
            .err()
            .expect("responsive seeds from different clusters must fail");
        assert_eq!(error.kind(), "cluster_mismatch");
    }
}

#[test]
fn bootstrap_retains_last_complete_same_cluster_snapshot() {
    let first = Server::start(valid_state());
    let second = Server::start(valid_state());
    let first_url = http_url(&first);
    let second_url = http_url(&second);
    first.state.lock().unwrap().members =
        Reply::Value(membership_response(CLUSTER_ID, &[(1, first_url)], 1));
    second.state.lock().unwrap().members = Reply::Value(membership_response(
        CLUSTER_ID,
        &[(2, second_url.clone())],
        2,
    ));

    let client = PdClient::connect_seeds(
        [first.address.clone(), second.address.clone()],
        Duration::from_secs(2),
    )
    .unwrap();
    assert_eq!(client.member_set().leader_url, second_url.clone());
    assert_eq!(client.member_set().member_urls, [second_url]);
}

#[test]
fn bootstrap_skips_bad_member_observations_in_both_seed_orders() {
    // servicediscovery/service_discovery.go:840-864 continues per-URL errors.
    let bad_replies = [
        Reply::Value(pdpb::GetMembersResponse {
            header: None,
            ..pdpb::GetMembersResponse::default()
        }),
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
        Reply::Value(pdpb::GetMembersResponse {
            header: Some(header(0)),
            ..pdpb::GetMembersResponse::default()
        }),
        Reply::Value(pdpb::GetMembersResponse {
            header: Some(header(CLUSTER_ID)),
            ..pdpb::GetMembersResponse::default()
        }),
    ];

    for bad_reply in bad_replies {
        for bad_first in [true, false] {
            let mut bad_state = valid_state();
            bad_state.members = bad_reply.clone();
            let bad = Server::start(bad_state);
            let healthy = Server::start(valid_state());
            let seeds = if bad_first {
                [bad.address.clone(), healthy.address.clone()]
            } else {
                [healthy.address.clone(), bad.address.clone()]
            };

            let client = PdClient::connect_seeds(seeds, Duration::from_secs(2)).unwrap();
            assert_eq!(client.cluster_id(), CLUSTER_ID);
            assert_eq!(client.member_set().leader_url, http_url(&healthy));
            assert_eq!(client.member_set().member_urls, [http_url(&healthy)]);
        }
    }
}

#[test]
fn membership_refresh_replaces_urls_and_normalizes_duplicates() {
    // servicediscovery/service_discovery.go:996-1012 updateURLs.
    let first = Server::start(valid_state());
    let survivor = Server::start(valid_state());
    let first_url = http_url(&first);
    let survivor_url = http_url(&survivor);
    first.state.lock().unwrap().members = Reply::Value(pdpb::GetMembersResponse {
        header: Some(header(CLUSTER_ID)),
        members: vec![pdpb::Member {
            member_id: 1,
            client_urls: vec![
                first.address.clone(),
                first_url.clone(),
                survivor_url.clone(),
            ],
            ..pdpb::Member::default()
        }],
        leader: Some(pd_member(1, [first_url.as_str()])),
        ..pdpb::GetMembersResponse::default()
    });
    let mut client = PdClient::connect(&first.address, Duration::from_secs(2)).unwrap();
    let mut expected_urls = vec![first_url.clone(), survivor_url.clone()];
    expected_urls.sort();
    assert_eq!(client.member_set().member_urls, expected_urls);

    first.state.lock().unwrap().members = Reply::Value(membership_response(
        CLUSTER_ID,
        &[(2, survivor_url.clone())],
        2,
    ));
    let refreshed = client.refresh_members().unwrap();
    assert_eq!(refreshed.member_urls, [survivor_url.clone()]);
    assert_eq!(refreshed.leader_url, survivor_url);
    assert_eq!(client.member_set(), refreshed);
}

#[test]
fn membership_refresh_rejects_cluster_mismatch_without_mutation() {
    // servicediscovery/service_discovery.go:840-864 same-cluster identity.
    let server = Server::start(valid_state());
    let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();
    let original = client.member_set();
    server.state.lock().unwrap().members = Reply::Value(membership_response(
        CLUSTER_ID + 1,
        &[(1, http_url(&server))],
        1,
    ));

    let error = client.refresh_members().unwrap_err();
    assert_eq!(error.kind(), "cluster_mismatch");
    assert_eq!(client.member_set(), original);
}

#[test]
fn failed_active_endpoint_refreshes_through_survivor_for_region() {
    // servicediscovery/service_discovery.go:891-922 tries known URLs.
    let active = Server::start(valid_state());
    let survivor = Server::start(valid_state());
    let active_url = http_url(&active);
    let survivor_url = http_url(&survivor);
    active.state.lock().unwrap().members = Reply::Value(membership_response(
        CLUSTER_ID,
        &[(1, active_url.clone()), (2, survivor_url.clone())],
        1,
    ));
    survivor.state.lock().unwrap().members = Reply::Value(membership_response(
        CLUSTER_ID,
        &[(2, survivor_url.clone())],
        2,
    ));
    let mut client = PdClient::connect(&active.address, Duration::from_millis(50)).unwrap();
    active.state.lock().unwrap().region = Reply::Status(tonic::Code::Unavailable, "removed");
    active.state.lock().unwrap().members = Reply::Status(tonic::Code::Unavailable, "removed");

    let region = client.get_region(b"wire").unwrap();
    assert_eq!(region.id, 7);
    assert_eq!(client.active_endpoint(), survivor_url.clone());
    assert_eq!(client.member_set().member_urls, [survivor_url]);
    let state = survivor.state.lock().unwrap();
    assert_eq!(state.member_requests.len(), 1);
    assert_eq!(state.region_requests.len(), 1);
}

#[test]
fn failed_auxiliary_refresh_preserves_known_survivor_for_region() {
    let active = Server::start(valid_state());
    let survivor = Server::start(valid_state());
    let active_url = http_url(&active);
    let survivor_url = http_url(&survivor);
    active.state.lock().unwrap().members = Reply::Value(membership_response(
        CLUSTER_ID,
        &[(1, active_url), (2, survivor_url.clone())],
        1,
    ));
    let mut client = PdClient::connect(&active.address, Duration::from_secs(2)).unwrap();
    let member_error = Reply::Value(pdpb::GetMembersResponse {
        header: Some(pdpb::ResponseHeader {
            cluster_id: CLUSTER_ID,
            error: Some(pdpb::Error {
                r#type: pdpb::ErrorType::NotBootstrapped as i32,
                message: "stale discovery".to_owned(),
            }),
        }),
        ..pdpb::GetMembersResponse::default()
    });
    active.state.lock().unwrap().region = Reply::Status(tonic::Code::Unavailable, "removed");
    active.state.lock().unwrap().members = member_error.clone();
    survivor.state.lock().unwrap().members = member_error;

    let original = client.member_set();
    let region = client.get_region(b"wire").unwrap();
    assert_eq!(region.id, 7);
    assert_eq!(client.member_set(), original);
    assert_eq!(client.active_endpoint(), survivor_url);
    assert_eq!(survivor.state.lock().unwrap().member_requests.len(), 1);
    assert_eq!(survivor.state.lock().unwrap().region_requests.len(), 1);
}

#[test]
fn failed_active_endpoint_refreshes_through_survivor_for_store() {
    let active = Server::start(valid_state());
    let survivor = Server::start(valid_state());
    let active_url = http_url(&active);
    let survivor_url = http_url(&survivor);
    active.state.lock().unwrap().members = Reply::Value(membership_response(
        CLUSTER_ID,
        &[(1, active_url), (2, survivor_url.clone())],
        1,
    ));
    survivor.state.lock().unwrap().members = Reply::Value(membership_response(
        CLUSTER_ID,
        &[(2, survivor_url.clone())],
        2,
    ));
    let mut client = PdClient::connect(&active.address, Duration::from_millis(50)).unwrap();
    active
        .state
        .lock()
        .unwrap()
        .stores
        .insert(101, Reply::Status(tonic::Code::Unavailable, "removed"));
    active.state.lock().unwrap().members = Reply::Status(tonic::Code::Unavailable, "removed");

    let store = client.get_store(101).unwrap().unwrap();
    assert_eq!(store.id, 101);
    assert_eq!(client.active_endpoint(), survivor_url);
    let state = survivor.state.lock().unwrap();
    assert_eq!(state.member_requests.len(), 1);
    assert_eq!(state.store_requests.len(), 1);
}

#[test]
fn grpc_application_status_is_terminal_after_confirming_the_endpoint_is_still_leader() {
    let active = Server::start(valid_state());
    let survivor = Server::start(valid_state());
    let active_url = http_url(&active);
    let survivor_url = http_url(&survivor);
    active.state.lock().unwrap().members = Reply::Value(membership_response(
        CLUSTER_ID,
        &[(1, active_url), (2, survivor_url)],
        1,
    ));
    let mut client = PdClient::connect(&active.address, Duration::from_secs(2)).unwrap();
    for (code, expected) in [
        (tonic::Code::InvalidArgument, "InvalidArgument"),
        (tonic::Code::PermissionDenied, "PermissionDenied"),
    ] {
        active.state.lock().unwrap().region = Reply::Status(code, "application error");
        let before = active.state.lock().unwrap().region_requests.len();
        let member_before = active.state.lock().unwrap().member_requests.len();

        let error = client.get_region(b"wire").unwrap_err();
        assert_eq!(error.kind(), "transport");
        assert!(error.to_string().contains(expected));
        assert_eq!(
            active.state.lock().unwrap().member_requests.len(),
            member_before + 1
        );
        assert_eq!(
            active.state.lock().unwrap().region_requests.len(),
            before + 1
        );
        assert!(survivor.state.lock().unwrap().member_requests.is_empty());
        assert!(survivor.state.lock().unwrap().region_requests.is_empty());
    }
}

#[test]
fn reachable_old_leader_header_errors_refresh_and_retry_the_new_leader() {
    for get_store in [false, true] {
        let old_leader = Server::start(valid_state());
        let new_leader = Server::start(valid_state());
        let old_url = http_url(&old_leader);
        let new_url = http_url(&new_leader);
        old_leader.state.lock().unwrap().members = Reply::Value(membership_response(
            CLUSTER_ID,
            &[(1, old_url.clone()), (2, new_url.clone())],
            1,
        ));
        let mut client = PdClient::connect(&old_leader.address, Duration::from_secs(2)).unwrap();

        old_leader.state.lock().unwrap().members = Reply::Value(membership_response(
            CLUSTER_ID,
            &[(1, old_url), (2, new_url.clone())],
            2,
        ));
        let error_header = Some(pdpb::ResponseHeader {
            cluster_id: CLUSTER_ID,
            error: Some(pdpb::Error {
                r#type: pdpb::ErrorType::Unknown as i32,
                message: "not leader".to_owned(),
            }),
        });
        if get_store {
            let mut response =
                store_response(101, metapb::StoreState::Up, metapb::NodeState::Serving);
            response.header = error_header;
            old_leader
                .state
                .lock()
                .unwrap()
                .stores
                .insert(101, Reply::Value(response));
            assert_eq!(client.get_store(101).unwrap().unwrap().id, 101);
        } else {
            let mut response = region_response();
            response.header = error_header;
            old_leader.state.lock().unwrap().region = Reply::Value(response);
            assert_eq!(client.get_region(b"wire").unwrap().id, 7);
        }

        assert_eq!(client.active_endpoint(), new_url);
        let old = old_leader.state.lock().unwrap();
        let new = new_leader.state.lock().unwrap();
        assert_eq!(old.member_requests.len(), 2);
        assert_eq!(new.member_requests.len(), 0);
        if get_store {
            assert_eq!(old.store_requests.len(), 1);
            assert_eq!(new.store_requests.len(), 1);
        } else {
            assert_eq!(old.region_requests.len(), 1);
            assert_eq!(new.region_requests.len(), 1);
        }
    }
}

#[test]
fn invalid_discovered_urls_fail_closed() {
    let mut state = valid_state();
    state.members = Reply::Value(pdpb::GetMembersResponse {
        header: Some(header(CLUSTER_ID)),
        members: vec![pd_member(1, ["https://127.0.0.1:2379"])],
        leader: Some(pd_member(1, ["https://127.0.0.1:2379"])),
        ..pdpb::GetMembersResponse::default()
    });
    let server = Server::start(state);
    let error = PdClient::connect(&server.address, Duration::from_secs(2))
        .err()
        .expect("TLS discovery is outside this slice");
    assert_eq!(error.kind(), "invalid_endpoint");
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
fn unknown_peer_role_is_preserved_for_forward_compatible_routing() {
    let mut state = valid_state();
    let response = match &mut state.region {
        Reply::Value(response) => response,
        _ => unreachable!(),
    };
    response.region.as_mut().unwrap().peers[0].role = 99;
    response.leader.as_mut().unwrap().role = 99;
    let server = Server::start(state);
    let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();

    let region = client.get_region(b"wire").unwrap();
    assert_eq!(region.peers[0].role, 99);
    assert_eq!(region.leader.role, 99);
}

#[test]
fn store_mismatch_unusable_and_unknown_states_fail_closed_without_retry() {
    let server = Server::start(valid_state());
    let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();
    for error in [
        pdpb::Error {
            r#type: pdpb::ErrorType::StoreTombstone as i32,
            message: "gone".to_owned(),
        },
        pdpb::Error {
            r#type: pdpb::ErrorType::Unknown as i32,
            message: "invalid store ID 201, not found".to_owned(),
        },
    ] {
        server.state.lock().unwrap().stores.insert(
            201,
            Reply::Value(pdpb::GetStoreResponse {
                header: Some(pdpb::ResponseHeader {
                    cluster_id: CLUSTER_ID,
                    error: Some(error),
                }),
                store: None,
            }),
        );
        assert_eq!(client.get_store(201).unwrap(), None);
    }
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
