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

use prost::Message;
use tidb_pd_client::{
    PdClient, PdKeyRange, PdNodeState, PdStoreState, BATCH_SCAN_REGIONS_PATH, GET_MEMBERS_PATH,
    GET_PREV_REGION_PATH, GET_REGION_BY_ID_PATH, GET_REGION_PATH, GET_STORE_PATH,
    SCAN_REGIONS_PATH,
};
use tidb_proto::metapb;
use tidb_proto::pdpb::{
    self,
    pd_server::{Pd, PdServer},
};

const CLUSTER_ID: u64 = 42;
const SELF_URL: &str = "http://127.0.0.1:0";

#[test]
fn store_labels_keep_the_pinned_kvproto_field_four_wire_contract() {
    // Pinned kvproto metapb.proto: StoreLabel key=1/value=2 and Store labels=4.
    let store = metapb::Store {
        id: 0,
        address: String::new(),
        state: metapb::StoreState::Up as i32,
        labels: vec![metapb::StoreLabel {
            key: "zone".to_owned(),
            value: "s1".to_owned(),
        }],
        node_state: metapb::NodeState::Preparing as i32,
    };
    assert_eq!(
        store.encode_to_vec(),
        [0x22, 0x0a, 0x0a, 0x04, b'z', b'o', b'n', b'e', 0x12, 0x02, b's', b'1']
    );
}

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
    prev_region: Reply<pdpb::GetRegionResponse>,
    region_by_id: Reply<pdpb::GetRegionResponse>,
    scan_regions: Reply<pdpb::ScanRegionsResponse>,
    batch_scan_regions: Reply<pdpb::BatchScanRegionsResponse>,
    stores: HashMap<u64, Reply<pdpb::GetStoreResponse>>,
    member_requests: Vec<pdpb::GetMembersRequest>,
    region_requests: Vec<pdpb::GetRegionRequest>,
    prev_region_requests: Vec<pdpb::GetRegionRequest>,
    region_by_id_requests: Vec<pdpb::GetRegionByIdRequest>,
    scan_region_requests: Vec<pdpb::ScanRegionsRequest>,
    batch_scan_region_requests: Vec<pdpb::BatchScanRegionsRequest>,
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

    async fn get_prev_region(
        &self,
        request: tonic::Request<pdpb::GetRegionRequest>,
    ) -> Result<tonic::Response<pdpb::GetRegionResponse>, tonic::Status> {
        let reply = {
            let mut state = self.state.lock().unwrap();
            state.prev_region_requests.push(request.into_inner());
            state.prev_region.clone()
        };
        reply.send().await
    }

    async fn get_region_by_id(
        &self,
        request: tonic::Request<pdpb::GetRegionByIdRequest>,
    ) -> Result<tonic::Response<pdpb::GetRegionResponse>, tonic::Status> {
        let reply = {
            let mut state = self.state.lock().unwrap();
            state.region_by_id_requests.push(request.into_inner());
            state.region_by_id.clone()
        };
        reply.send().await
    }

    async fn scan_regions(
        &self,
        request: tonic::Request<pdpb::ScanRegionsRequest>,
    ) -> Result<tonic::Response<pdpb::ScanRegionsResponse>, tonic::Status> {
        let reply = {
            let mut state = self.state.lock().unwrap();
            state.scan_region_requests.push(request.into_inner());
            state.scan_regions.clone()
        };
        reply.send().await
    }

    async fn batch_scan_regions(
        &self,
        request: tonic::Request<pdpb::BatchScanRegionsRequest>,
    ) -> Result<tonic::Response<pdpb::BatchScanRegionsResponse>, tonic::Status> {
        let reply = {
            let mut state = self.state.lock().unwrap();
            state.batch_scan_region_requests.push(request.into_inner());
            state.batch_scan_regions.clone()
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
        buckets: Some(bucket_metadata(
            7,
            8,
            [b"wire-start".as_slice(), b"split", b"wire-end"],
        )),
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
        keys: keys.into_iter().map(<[u8]>::to_vec).collect(),
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

fn extended_region(id: u64, start_key: &[u8], end_key: &[u8]) -> pdpb::Region {
    let leader = peer(id * 10 + 1, id * 100 + 1, metapb::PeerRole::Voter, false);
    let pending = peer(id * 10 + 2, id * 100 + 2, metapb::PeerRole::Learner, false);
    pdpb::Region {
        region: Some(metapb::Region {
            id,
            start_key: start_key.to_vec(),
            end_key: end_key.to_vec(),
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: id + 1,
                version: id + 2,
            }),
            peers: vec![leader, pending],
        }),
        leader: Some(leader),
        down_peers: vec![pdpb::PeerStats {
            peer: Some(leader),
            down_seconds: id,
        }],
        pending_peers: vec![pending],
        buckets: Some(bucket_metadata(id, id + 10, [start_key, end_key])),
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
            labels: Vec::new(),
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
        prev_region: Reply::Value(region_response()),
        region_by_id: Reply::Value(region_response()),
        scan_regions: Reply::Value(pdpb::ScanRegionsResponse {
            header: Some(header(CLUSTER_ID)),
            region_metas: Vec::new(),
            leaders: Vec::new(),
            regions: vec![
                extended_region(21, b"a", b"m"),
                extended_region(22, b"m", b"z"),
            ],
        }),
        batch_scan_regions: Reply::Value(pdpb::BatchScanRegionsResponse {
            header: Some(header(CLUSTER_ID)),
            regions: vec![
                extended_region(31, b"a", b"m"),
                extended_region(32, b"m", b"z"),
            ],
        }),
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
        prev_region_requests: Vec::new(),
        region_by_id_requests: Vec::new(),
        scan_region_requests: Vec::new(),
        batch_scan_region_requests: Vec::new(),
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
    assert_eq!(GET_PREV_REGION_PATH, "/pdpb.PD/GetPrevRegion");
    assert_eq!(GET_REGION_BY_ID_PATH, "/pdpb.PD/GetRegionByID");
    assert_eq!(SCAN_REGIONS_PATH, "/pdpb.PD/ScanRegions");
    assert_eq!(BATCH_SCAN_REGIONS_PATH, "/pdpb.PD/BatchScanRegions");
    assert_eq!(GET_STORE_PATH, "/pdpb.PD/GetStore");

    let mut state = valid_state();
    let store = match state.stores.get_mut(&101).unwrap() {
        Reply::Value(response) => response.store.as_mut().unwrap(),
        _ => unreachable!(),
    };
    store.labels = vec![
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
    assert_eq!(region.leader.as_ref().unwrap().role, 0);
    assert!(!region.leader.as_ref().unwrap().is_witness);
    assert_eq!(
        region
            .down_peers
            .iter()
            .map(|peer| peer.id)
            .collect::<Vec<_>>(),
        [14]
    );
    assert_eq!(
        region
            .pending_peers
            .iter()
            .map(|peer| peer.id)
            .collect::<Vec<_>>(),
        [13]
    );
    let buckets = region.buckets.as_ref().unwrap();
    assert_eq!(buckets.region_id, 7);
    assert_eq!(buckets.version, 8);
    assert_eq!(
        buckets.keys,
        vec![
            b"wire-start".to_vec(),
            b"split".to_vec(),
            b"wire-end".to_vec()
        ]
    );
    assert_eq!(buckets.stats.as_ref().unwrap().write_keys, [11, 12]);
    assert_eq!(buckets.period_in_ms, 1_000);

    let previous = client
        .get_prev_region_with_buckets(wire_key, false)
        .unwrap();
    assert_eq!(previous.id, 7);
    assert!(previous.buckets.is_none());

    let up = client.get_store(101).unwrap().unwrap();
    assert_eq!(up.state, PdStoreState::Up);
    assert_eq!(up.node_state, PdNodeState::Serving);
    assert_eq!(
        up.labels,
        [
            ("zone".to_owned(), "shanghai".to_owned()),
            ("disk".to_owned(), "ssd".to_owned()),
        ]
    );
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
    assert_eq!(state.prev_region_requests.len(), 1);
    let previous_request = &state.prev_region_requests[0];
    assert_eq!(previous_request.region_key, wire_key);
    assert!(!previous_request.need_buckets);
    assert_exact_header(previous_request.header.as_ref().unwrap());
    assert_eq!(state.store_requests.len(), 2);
    assert_eq!(state.store_requests[0].store_id, 101);
    assert_eq!(state.store_requests[1].store_id, 102);
    assert!(state.store_requests.iter().all(|request| {
        assert_exact_header(request.header.as_ref().unwrap());
        true
    }));
}

#[test]
fn by_id_scan_and_batch_scan_preserve_flags_ranges_limits_and_response_order() {
    // pd-client/client.go:GetRegionByID, ScanRegions, BatchScanRegions.
    // pd-client/clients/router/client.go:handleRegionsResponse.
    let server = Server::start(valid_state());
    let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();

    let by_id_without_buckets = client.get_region_by_id(7, false).unwrap();
    assert_eq!(by_id_without_buckets.id, 7);
    assert!(by_id_without_buckets.buckets.is_none());
    let by_id_with_buckets = client.get_region_by_id(7, true).unwrap();
    assert_eq!(by_id_with_buckets.buckets.as_ref().unwrap().version, 8);

    let scan = client.scan_regions(b"wire-a", b"wire-z", 17).unwrap();
    assert_eq!(
        scan.iter().map(|region| region.id).collect::<Vec<_>>(),
        [21, 22]
    );
    assert_eq!(scan[0].leader.as_ref().unwrap().id, 211);
    assert_eq!(scan[0].down_peers[0].store_id, 2_101);
    assert_eq!(scan[0].pending_peers[0].id, 212);
    assert!(scan.iter().all(|region| region.buckets.is_none()));

    let ranges = vec![
        PdKeyRange {
            start_key: b"wire-a".to_vec(),
            end_key: b"wire-m".to_vec(),
        },
        PdKeyRange {
            start_key: b"wire-q".to_vec(),
            end_key: Vec::new(),
        },
    ];
    let batch = client.batch_scan_regions(&ranges, 23, true, true).unwrap();
    assert_eq!(
        batch.iter().map(|region| region.id).collect::<Vec<_>>(),
        [31, 32]
    );
    assert_eq!(batch[1].pending_peers[0].store_id, 3_202);
    assert_eq!(batch[1].buckets.as_ref().unwrap().region_id, 32);
    let batch_without_buckets = client.batch_scan_regions(&ranges, 23, false, true).unwrap();
    assert!(batch_without_buckets
        .iter()
        .all(|region| region.buckets.is_none()));

    let state = server.state.lock().unwrap();
    assert_eq!(state.region_by_id_requests.len(), 2);
    for request in &state.region_by_id_requests {
        assert_exact_header(request.header.as_ref().unwrap());
        assert_eq!(request.region_id, 7);
    }
    assert!(!state.region_by_id_requests[0].need_buckets);
    assert!(state.region_by_id_requests[1].need_buckets);

    assert_eq!(state.scan_region_requests.len(), 1);
    let scan_request = &state.scan_region_requests[0];
    assert_exact_header(scan_request.header.as_ref().unwrap());
    assert_eq!(scan_request.start_key, b"wire-a");
    assert_eq!(scan_request.end_key, b"wire-z");
    assert_eq!(scan_request.limit, 17);

    assert_eq!(state.batch_scan_region_requests.len(), 2);
    let batch_request = &state.batch_scan_region_requests[0];
    assert_exact_header(batch_request.header.as_ref().unwrap());
    assert!(batch_request.need_buckets);
    assert_eq!(batch_request.limit, 23);
    assert!(batch_request.contain_all_key_range);
    assert_eq!(batch_request.ranges.len(), 2);
    assert_eq!(batch_request.ranges[0].start_key, ranges[0].start_key);
    assert_eq!(batch_request.ranges[0].end_key, ranges[0].end_key);
    assert_eq!(batch_request.ranges[1].start_key, ranges[1].start_key);
    assert_eq!(batch_request.ranges[1].end_key, ranges[1].end_key);
    let batch_without_buckets_request = &state.batch_scan_region_requests[1];
    assert_exact_header(batch_without_buckets_request.header.as_ref().unwrap());
    assert!(!batch_without_buckets_request.need_buckets);
    assert_eq!(batch_without_buckets_request.ranges, batch_request.ranges);
}

#[test]
fn bounded_worker_keeps_each_bucket_flag_and_owned_response_isolated() {
    // pd-client/clients/router/client_test.go:
    // TestRequestFinisherNoDataRace and TestRequestFinisherClearsUnrequestedBuckets.
    // This retained worker does not batch QueryRegion requests, so it enforces
    // the same per-request bucket filtering at its one owned projection point.
    let server = Server::start(valid_state());
    let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();

    let mut first = client.get_region_with_buckets(b"first", true).unwrap();
    first.buckets.as_mut().unwrap().keys[0].push(b'!');
    first.pending_peers[0].id = 999;
    let second = client.get_region_with_buckets(b"second", false).unwrap();

    assert!(second.buckets.is_none());
    assert_eq!(second.pending_peers[0].id, 13);
    let state = server.state.lock().unwrap();
    assert_eq!(state.region_requests.len(), 2);
    assert!(state.region_requests[0].need_buckets);
    assert!(!state.region_requests[1].need_buckets);
    assert_eq!(state.region_requests[0].region_key, b"first");
    assert_eq!(state.region_requests[1].region_key, b"second");
}

#[test]
fn legacy_scan_fallback_keeps_meta_order_and_missing_leader() {
    // pd-client/client.go:1005-1031 handleRegionsResponse compatibility path.
    let mut state = valid_state();
    let first = extended_region(41, b"a", b"m");
    let second = extended_region(42, b"m", b"z");
    state.scan_regions = Reply::Value(pdpb::ScanRegionsResponse {
        header: Some(header(CLUSTER_ID)),
        region_metas: vec![first.region.unwrap(), second.region.unwrap()],
        leaders: vec![first.leader.unwrap()],
        regions: Vec::new(),
    });
    let server = Server::start(state);
    let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();

    let regions = client.scan_regions(b"a", b"z", 2).unwrap();
    assert_eq!(
        regions.iter().map(|region| region.id).collect::<Vec<_>>(),
        [41, 42]
    );
    assert_eq!(regions[0].leader.as_ref().unwrap().id, 411);
    assert!(regions[1].leader.is_none());
    assert!(regions.iter().all(|region| region.down_peers.is_empty()));
    assert!(regions.iter().all(|region| region.pending_peers.is_empty()));
    assert!(regions.iter().all(|region| region.buckets.is_none()));
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
    assert_eq!(refreshed.member_urls, std::slice::from_ref(&survivor_url));
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
fn failed_active_endpoint_refreshes_through_survivor_for_previous_region() {
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
    active.state.lock().unwrap().prev_region = Reply::Status(tonic::Code::Unavailable, "removed");
    active.state.lock().unwrap().members = Reply::Status(tonic::Code::Unavailable, "removed");

    let region = client.get_prev_region(b"wire").unwrap();
    assert_eq!(region.id, 7);
    assert_eq!(client.active_endpoint(), survivor_url);
    assert_eq!(survivor.state.lock().unwrap().prev_region_requests.len(), 1);
}

#[test]
fn leader_only_previous_region_bypasses_the_active_follower() {
    let leader = Server::start(valid_state());
    let follower = Server::start(valid_state());
    let leader_url = http_url(&leader);
    let follower_url = http_url(&follower);
    let members = membership_response(
        CLUSTER_ID,
        &[(1, leader_url.clone()), (2, follower_url.clone())],
        1,
    );
    leader.state.lock().unwrap().members = Reply::Value(members.clone());
    follower.state.lock().unwrap().members = Reply::Value(members);
    let mut client = PdClient::connect(&leader.address, Duration::from_millis(50)).unwrap();

    leader.state.lock().unwrap().region =
        Reply::Status(tonic::Code::Unavailable, "stale active endpoint");
    leader.state.lock().unwrap().members =
        Reply::Status(tonic::Code::Unavailable, "stale active endpoint");
    client.get_region(b"wire").unwrap();
    assert_eq!(client.active_endpoint(), follower_url);

    client.get_prev_region_routed(b"wire", true, true).unwrap();
    assert_eq!(leader.state.lock().unwrap().prev_region_requests.len(), 1);
    assert!(follower
        .state
        .lock()
        .unwrap()
        .prev_region_requests
        .is_empty());
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
fn previous_region_reuses_header_and_topology_validation_without_retry() {
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
                region: None,
                ..region_response()
            },
            "missing_region",
        ),
    ];
    for (response, expected) in cases {
        let before = server.state.lock().unwrap().prev_region_requests.len();
        server.state.lock().unwrap().prev_region = Reply::Value(response);
        let error = client.get_prev_region(b"wire").unwrap_err();
        assert_eq!(error.kind(), expected, "unexpected error: {error}");
        assert_eq!(
            server.state.lock().unwrap().prev_region_requests.len(),
            before + 1
        );
    }
}

#[test]
fn leaderless_get_region_preserves_ordered_peers() {
    // client-go/internal/locate/region_cache_test.go:493-524.
    let mut state = valid_state();
    let response = match &mut state.region {
        Reply::Value(response) => response,
        _ => unreachable!(),
    };
    response.leader = None;
    let server = Server::start(state);
    let mut client = PdClient::connect(&server.address, Duration::from_secs(2)).unwrap();

    let region = client.get_region(b"wire").unwrap();
    assert!(region.leader.is_none());
    assert_eq!(
        region.peers.iter().map(|peer| peer.id).collect::<Vec<_>>(),
        [11, 12, 13, 14]
    );
    assert_eq!(server.state.lock().unwrap().region_requests.len(), 1);
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
    assert_eq!(region.leader.as_ref().unwrap().role, 99);
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
