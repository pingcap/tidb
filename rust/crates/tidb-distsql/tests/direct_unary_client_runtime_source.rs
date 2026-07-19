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

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use prost::Message;
use tidb_datatype::FieldType;
use tidb_distsql::cop_paging::RegionRetryWaiter;
use tidb_distsql::{
    DirectUnaryClient, DirectUnaryClientError, DirectUnaryQueryTransport, DirectUnaryRequest,
    DirectUnaryResponse, DirectUnaryRuntimeConfig, DirectUnaryTransportError, InjectedQueryRuntime,
    KvRequestMetadata, QueryResultContext, ReplicaReadType, RequestKeyRange, RequestKeyRanges,
    RequestType, SelectInput, StoreType, TransportRequest, WarningCollector,
};

fn transport_request(metadata: KvRequestMetadata) -> TransportRequest {
    TransportRequest::new(
        metadata,
        std::sync::Arc::new(tidb_distsql::CancelHandle::default()),
    )
}
use tidb_proto::{
    errorpb, metapb, CoprocessorExecDetailsV2, CoprocessorKeyRange, CoprocessorRequest,
    CoprocessorResponse, CoprocessorScanDetailV2,
};
use tidb_txnkv::region::{
    Peer, PeerRole, RegionCache, RegionLoadError, RegionLoader, RegionLocation, RegionMetadata,
    RegionRecoveryLoader, RegionRouteError, RegionVerId, Store, StoreLiveness,
};
use tidb_txnkv::rpc::{completion_pair, AsyncRequestDispatcher, CompletionPull, CompletionRunLoop};
use tidb_txnkv::UnaryCallContext;
use tidb_txnkv::{
    ClientReplicaReadType, DirectUnaryConnectionError, DirectUnaryGrpcCode,
    DirectUnaryTransportClass,
};

const OBSERVATION_TIME: Duration = Duration::from_secs(1_000);

fn observation_time() -> Duration {
    OBSERVATION_TIME
}

#[derive(Debug, Default)]
struct RecordingRetryControl {
    sleeps: RefCell<Vec<Duration>>,
    fail_next_sleep: Cell<bool>,
}

impl RegionRetryWaiter for RecordingRetryControl {
    fn wait(&self, cancellation: &tidb_txnkv::UnaryCancellation, delay: Duration) -> bool {
        self.sleeps.borrow_mut().push(delay);
        if self.fail_next_sleep.replace(false) {
            cancellation.cancel();
        }
        cancellation.is_cancelled()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedCall {
    address: String,
    timeout: Duration,
    region_id: u64,
    data: Vec<u8>,
    paging_size: u64,
    predicted_read_bytes: u64,
    cluster_id: u64,
    conf_ver: u64,
    version: u64,
    peer_id: u64,
    store_id: u64,
    peer_role: i32,
    is_witness: bool,
    task_id: u64,
    request_source: String,
    not_fill_cache: bool,
    replica_read_type: ClientReplicaReadType,
    replica_read: bool,
    stale_read: bool,
}

struct ScriptedLoader {
    cluster_id: u64,
    calls: Rc<RefCell<Vec<Vec<u8>>>>,
    regions: VecDeque<RegionLocation>,
}

impl RegionLoader for ScriptedLoader {
    fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.calls.borrow_mut().push(key.to_vec());
        self.regions
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("scripted-pd-empty", "no region"))
    }
}

impl RegionRecoveryLoader for ScriptedLoader {
    fn hydrate_region(
        &mut self,
        metadata: &RegionMetadata,
        _leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        self.load_region(&metadata.encoded_start_key)
    }
}

struct ScriptedClient {
    calls: Rc<RefCell<Vec<ObservedCall>>>,
    responses: VecDeque<Result<Vec<u8>, DirectUnaryClientError>>,
    events: Rc<RefCell<Vec<ClientEvent>>>,
    liveness: RefCell<VecDeque<Result<StoreLiveness, DirectUnaryClientError>>>,
    batch_errors: RefCell<VecDeque<DirectUnaryClientError>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ClientEvent {
    Send(String),
    ForceClose(String),
    CloseGeneration { address: String, version: u64 },
    Liveness { address: String, timeout: Duration },
}

impl DirectUnaryClient for ScriptedClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.events
            .borrow_mut()
            .push(ClientEvent::Send(address.to_owned()));
        let wire = CoprocessorRequest::decode(request.encoded_request.as_slice()).unwrap();
        assert!(wire.context.is_none());
        let epoch = request.context.region_epoch.as_ref().unwrap();
        let peer = request.context.peer.as_ref().unwrap();
        self.calls.borrow_mut().push(ObservedCall {
            address: address.to_owned(),
            timeout,
            region_id: request.context.region_id,
            data: wire.data,
            paging_size: wire.paging_size,
            predicted_read_bytes: request.predicted_read_bytes,
            cluster_id: request.context.cluster_id,
            conf_ver: epoch.conf_ver,
            version: epoch.version,
            peer_id: peer.id,
            store_id: peer.store_id,
            peer_role: peer.role,
            is_witness: peer.is_witness,
            task_id: request.context.task_id,
            request_source: request.context.request_source.clone(),
            not_fill_cache: request.context.not_fill_cache,
            replica_read_type: request.replica_read_type,
            replica_read: request.context.replica_read,
            stale_read: request.context.stale_read,
        });
        self.responses
            .pop_front()
            .expect("one scripted response per client call")
            .map(|encoded_response| DirectUnaryResponse::new(encoded_response, address, 1))
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        if call.cancellation().is_cancelled() {
            return Err(DirectUnaryClientError::CallerCancelled);
        }
        let result = self.send_request(address, request, call.timeout());
        if call.cancellation().is_cancelled() {
            Err(DirectUnaryClientError::CallerCancelled)
        } else {
            result
        }
    }

    fn send_request_with_route(
        &mut self,
        address: &str,
        _forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.send_request_with_context(address, request, call)
    }

    fn close_address(&mut self, address: &str) -> Result<(), DirectUnaryClientError> {
        self.events
            .borrow_mut()
            .push(ClientEvent::ForceClose(address.to_owned()));
        Ok(())
    }

    fn close_address_version(
        &mut self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        self.events.borrow_mut().push(ClientEvent::CloseGeneration {
            address: address.to_owned(),
            version,
        });
        Ok(())
    }

    fn liveness(
        &self,
        address: &str,
        timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        self.events.borrow_mut().push(ClientEvent::Liveness {
            address: address.to_owned(),
            timeout,
        });
        self.liveness
            .borrow_mut()
            .pop_front()
            .unwrap_or(Ok(StoreLiveness::Unknown))
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }
}

impl AsyncRequestDispatcher for ScriptedClient {
    type Pending = CompletionPull<DirectUnaryResponse, DirectUnaryClientError>;

    fn begin(
        &mut self,
        physical_address: &str,
        _forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<Self::Pending, DirectUnaryClientError> {
        let (completion, pull) = completion_pair(CompletionRunLoop::new(), || {});
        if let Some(error) = self.batch_errors.borrow_mut().pop_front() {
            completion.schedule(Err(error));
            return Ok(pull);
        }
        completion.schedule(self.send_request_with_context(physical_address, request, call));
        Ok(pull)
    }
}

impl tidb_txnkv::lock::LockRecoveryClient for ScriptedClient {
    fn check_txn_status_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcCheckTxnStatusRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "unexpected lock in scripted read".to_owned(),
        ))
    }

    fn resolve_lock_for_read(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcResolveLockRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcResolveLockResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "unexpected lock in scripted read".to_owned(),
        ))
    }
}

fn range(start: &str, end: &str) -> RequestKeyRange {
    RequestKeyRange {
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
    }
}

fn metadata(start: &str, end: &str) -> KvRequestMetadata {
    let mut metadata = KvRequestMetadata {
        request_type: RequestType::Dag,
        data: Some(b"dag-read".to_vec()),
        key_ranges: Some(RequestKeyRanges::new_non_partitioned(vec![range(
            start, end,
        )])),
        keep_order: true,
        store_type: StoreType::TiKv,
        start_ts: 42,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        ..KvRequestMetadata::default()
    };
    metadata.session.tikv_client_read_timeout_ms = 777;
    metadata.session.task_id = 29;
    metadata.session.request_source.internal = true;
    metadata.session.request_source.source_type = "ddl".to_owned();
    metadata.session.not_fill_cache = true;
    metadata
}

fn location(region_id: u64, start: &str, end: &str, address: &str) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(region_id, 1, 2),
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
        peers: vec![Peer {
            id: region_id + 100,
            store_id: region_id + 200,
            role: PeerRole::Voter,
            is_witness: false,
            store_epoch: 7,
        }],
        leader_peer_id: Some(region_id + 100),
        stores: vec![Store {
            id: region_id + 200,
            address: address.to_owned(),
            epoch: 7,
        }],
        ..RegionLocation::default()
    }
}

fn location_with_second_peer(
    region_id: u64,
    start: &str,
    end: &str,
    first_address: &str,
    second_address: &str,
) -> RegionLocation {
    let mut location = location(region_id, start, end, first_address);
    location.peers.push(Peer {
        id: region_id + 101,
        store_id: region_id + 201,
        role: PeerRole::Voter,
        is_witness: false,
        store_epoch: 7,
    });
    location.stores.push(Store {
        id: region_id + 201,
        address: second_address.to_owned(),
        epoch: 7,
    });
    location
}

fn location_with_three_peers(
    region_id: u64,
    start: &str,
    end: &str,
    address_prefix: &str,
) -> RegionLocation {
    let mut location = location(
        region_id,
        start,
        end,
        &format!("{address_prefix}-leader:20160"),
    );
    location.peers.push(Peer {
        id: region_id + 101,
        store_id: region_id + 201,
        role: PeerRole::Voter,
        is_witness: false,
        store_epoch: 7,
    });
    location.peers.push(Peer {
        id: region_id + 102,
        store_id: region_id + 202,
        role: PeerRole::Learner,
        is_witness: false,
        store_epoch: 7,
    });
    location.stores.push(Store {
        id: region_id + 201,
        address: format!("{address_prefix}-follower:20160"),
        epoch: 7,
    });
    location.stores.push(Store {
        id: region_id + 202,
        address: format!("{address_prefix}-learner:20160"),
        epoch: 7,
    });
    location
}

fn not_leader(region_id: u64, leader: Option<(u64, u64)>) -> Vec<u8> {
    CoprocessorResponse {
        region_error: Some(errorpb::Error {
            not_leader: Some(errorpb::NotLeader {
                region_id,
                leader: leader.map(|(id, store_id)| metapb::Peer {
                    id,
                    store_id,
                    role: 0,
                    is_witness: false,
                }),
            }),
            ..errorpb::Error::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

fn data_is_not_ready() -> Vec<u8> {
    CoprocessorResponse {
        region_error: Some(errorpb::Error {
            data_is_not_ready: Some(errorpb::DataIsNotReady::default()),
            ..errorpb::Error::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

fn region_not_found(region_id: u64) -> Vec<u8> {
    CoprocessorResponse {
        region_error: Some(errorpb::Error {
            region_not_found: Some(errorpb::RegionNotFound { region_id }),
            ..errorpb::Error::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

fn store_not_match(region_id: u64) -> Vec<u8> {
    CoprocessorResponse {
        region_error: Some(errorpb::Error {
            message: "store not match".to_owned(),
            store_not_match: Some(errorpb::StoreNotMatch {
                request_store_id: region_id + 200,
                actual_store_id: region_id + 201,
            }),
            ..errorpb::Error::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

fn raft_entry_too_large(region_id: u64) -> Vec<u8> {
    CoprocessorResponse {
        region_error: Some(errorpb::Error {
            raft_entry_too_large: Some(errorpb::RaftEntryTooLarge {
                region_id,
                entry_size: 1_048_576,
            }),
            ..errorpb::Error::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

fn undetermined_region_error(message: &str) -> Vec<u8> {
    CoprocessorResponse {
        region_error: Some(errorpb::Error {
            undetermined_result: Some(errorpb::UndeterminedResult {
                message: message.to_owned(),
            }),
            ..errorpb::Error::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

fn unknown_region_error(message: &str) -> Vec<u8> {
    CoprocessorResponse {
        region_error: Some(errorpb::Error {
            message: message.to_owned(),
            ..errorpb::Error::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

fn response(data: &[u8]) -> Vec<u8> {
    CoprocessorResponse {
        data: data.to_vec(),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

fn connection_failure(
    address: &str,
    version: u64,
    class: DirectUnaryTransportClass,
    grpc_code: Option<DirectUnaryGrpcCode>,
) -> DirectUnaryClientError {
    let message = "scripted transport failure".to_owned();
    let connection = match class {
        DirectUnaryTransportClass::Connection => {
            DirectUnaryConnectionError::connection(address, version, message)
        }
        DirectUnaryTransportClass::LocalDeadline => {
            DirectUnaryConnectionError::local_deadline(address, version, message)
        }
        DirectUnaryTransportClass::RemoteGrpc => DirectUnaryConnectionError::remote_grpc(
            address,
            version,
            grpc_code.expect("remote gRPC scripts require a code"),
            message,
        ),
        DirectUnaryTransportClass::CallerCancelled => {
            panic!("caller cancellation has no selected connection")
        }
    };
    DirectUnaryClientError::Connection(connection)
}

fn transport(
    calls: Rc<RefCell<Vec<ObservedCall>>>,
    responses: impl IntoIterator<Item = Result<Vec<u8>, String>>,
    regions: impl IntoIterator<Item = RegionLocation>,
) -> DirectUnaryQueryTransport<ScriptedClient, ScriptedLoader> {
    transport_with_cluster_id(calls, responses, regions, 9001)
}

fn transport_with_cluster_id(
    calls: Rc<RefCell<Vec<ObservedCall>>>,
    responses: impl IntoIterator<Item = Result<Vec<u8>, String>>,
    regions: impl IntoIterator<Item = RegionLocation>,
    cluster_id: u64,
) -> DirectUnaryQueryTransport<ScriptedClient, ScriptedLoader> {
    transport_with_loader_calls(
        calls,
        responses,
        regions,
        cluster_id,
        Rc::new(RefCell::new(Vec::new())),
    )
}

fn transport_with_loader_calls(
    calls: Rc<RefCell<Vec<ObservedCall>>>,
    responses: impl IntoIterator<Item = Result<Vec<u8>, String>>,
    regions: impl IntoIterator<Item = RegionLocation>,
    cluster_id: u64,
    loader_calls: Rc<RefCell<Vec<Vec<u8>>>>,
) -> DirectUnaryQueryTransport<ScriptedClient, ScriptedLoader> {
    transport_with_loader_calls_and_config(
        calls,
        responses,
        regions,
        cluster_id,
        loader_calls,
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(60),
            seed_read_bytes: 4096,
            observation_time,
            ..DirectUnaryRuntimeConfig::default()
        },
    )
}

fn transport_with_loader_calls_and_config(
    calls: Rc<RefCell<Vec<ObservedCall>>>,
    responses: impl IntoIterator<Item = Result<Vec<u8>, String>>,
    regions: impl IntoIterator<Item = RegionLocation>,
    cluster_id: u64,
    loader_calls: Rc<RefCell<Vec<Vec<u8>>>>,
    config: DirectUnaryRuntimeConfig,
) -> DirectUnaryQueryTransport<ScriptedClient, ScriptedLoader> {
    DirectUnaryQueryTransport::new_injected(
        ScriptedClient {
            calls,
            responses: responses
                .into_iter()
                .map(|response| response.map_err(DirectUnaryClientError::InvalidRequest))
                .collect(),
            events: Rc::new(RefCell::new(Vec::new())),
            liveness: RefCell::new(VecDeque::new()),
            batch_errors: RefCell::new(VecDeque::new()),
        },
        RegionCache::new(ScriptedLoader {
            cluster_id,
            calls: loader_calls,
            regions: regions.into_iter().collect(),
        }),
        config,
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap()
}

fn transport_with_transport_failures(
    calls: Rc<RefCell<Vec<ObservedCall>>>,
    responses: impl IntoIterator<Item = Result<Vec<u8>, DirectUnaryClientError>>,
    liveness: impl IntoIterator<Item = Result<StoreLiveness, DirectUnaryClientError>>,
    events: Rc<RefCell<Vec<ClientEvent>>>,
    regions: impl IntoIterator<Item = RegionLocation>,
    config: DirectUnaryRuntimeConfig,
) -> DirectUnaryQueryTransport<ScriptedClient, ScriptedLoader> {
    DirectUnaryQueryTransport::new_injected(
        ScriptedClient {
            calls,
            responses: responses.into_iter().collect(),
            events,
            liveness: RefCell::new(liveness.into_iter().collect()),
            batch_errors: RefCell::new(VecDeque::new()),
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::new(RefCell::new(Vec::new())),
            regions: regions.into_iter().collect(),
        }),
        config,
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap()
}

fn select_result(
    runtime: &mut InjectedQueryRuntime<DirectUnaryQueryTransport<ScriptedClient, ScriptedLoader>>,
    request: &TransportRequest,
) -> tidb_distsql::query_runtime::QuerySelectResult<
    tidb_distsql::DirectUnaryQueryResponse<ScriptedClient, ScriptedLoader>,
> {
    runtime
        .select_with_runtime_stats(
            request,
            SelectInput::default(),
            QueryResultContext::new(Vec::<FieldType>::new(), WarningCollector::new()),
            vec![1],
            2,
            true,
        )
        .unwrap()
}

#[test]
fn client_go_shaped_dispatch_is_lazy_address_directed_and_logically_ordered() {
    // client-go/internal/client/client.go:96-105 Client.SendRequest
    // pkg/store/copr/coprocessor.go:1723 handleTaskOnce
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(response(b"left")), Ok(response(b"right"))],
        [
            location(1, "a", "m", "tikv-1:20160"),
            location(2, "m", "z", "tikv-2:20160"),
        ],
    ));
    let request = transport_request(metadata("a", "z"));
    let mut result = select_result(&mut runtime, &request);

    assert!(calls.borrow().is_empty(), "send must stay response-lazy");
    assert_eq!(result.next_raw().unwrap(), Some(b"left".to_vec()));
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(result.next_raw().unwrap(), Some(b"right".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    let mut normalized_calls = calls.borrow().clone();
    assert!(
        normalized_calls[1].timeout <= normalized_calls[0].timeout,
        "all RPCs in one query must consume one absolute deadline"
    );
    assert!(normalized_calls.iter().all(|call| {
        call.timeout <= Duration::from_millis(777) && call.timeout > Duration::from_millis(700)
    }));
    for call in &mut normalized_calls {
        call.timeout = Duration::from_millis(777);
    }
    assert_eq!(
        normalized_calls.as_slice(),
        [
            ObservedCall {
                address: "tikv-1:20160".to_owned(),
                timeout: Duration::from_millis(777),
                region_id: 1,
                data: b"dag-read".to_vec(),
                paging_size: 0,
                predicted_read_bytes: 4096,
                cluster_id: 9001,
                conf_ver: 1,
                version: 2,
                peer_id: 101,
                store_id: 201,
                peer_role: 0,
                is_witness: false,
                task_id: 29,
                request_source: "internal_ddl".to_owned(),
                not_fill_cache: true,
                replica_read_type: ClientReplicaReadType::Leader,
                replica_read: false,
                stale_read: false,
            },
            ObservedCall {
                address: "tikv-2:20160".to_owned(),
                timeout: Duration::from_millis(777),
                region_id: 2,
                data: b"dag-read".to_vec(),
                paging_size: 0,
                predicted_read_bytes: 4096,
                cluster_id: 9001,
                conf_ver: 1,
                version: 2,
                peer_id: 102,
                store_id: 202,
                peer_role: 0,
                is_witness: false,
                task_id: 29,
                request_source: "internal_ddl".to_owned(),
                not_fill_cache: true,
                replica_read_type: ClientReplicaReadType::Leader,
                replica_read: false,
                stale_read: false,
            },
        ]
    );
}

#[test]
fn pd_peer_role_witness_and_cluster_fields_have_one_context_authority() {
    for (role, encoded) in [
        (PeerRole::Voter, 0),
        (PeerRole::IncomingVoter, 2),
        (PeerRole::DemotingVoter, 3),
    ] {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let mut candidate = location(7, "a", "z", "tikv-7:20160");
        candidate.peers[0].role = role;
        let mut runtime = InjectedQueryRuntime::new(transport(
            Rc::clone(&calls),
            [Ok(response(b"ok"))],
            [candidate],
        ));
        let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));
        assert!(calls.borrow().is_empty());
        assert_eq!(result.next_raw().unwrap(), Some(b"ok".to_vec()));

        let calls = calls.borrow();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].cluster_id, 9001);
        assert_eq!(calls[0].conf_ver, 1);
        assert_eq!(calls[0].version, 2);
        assert_eq!(calls[0].peer_id, 107);
        assert_eq!(calls[0].store_id, 207);
        assert_eq!(calls[0].peer_role, encoded);
        assert!(!calls[0].is_witness);
        assert_eq!(calls[0].task_id, 29);
        assert_eq!(calls[0].request_source, "internal_ddl");
        assert!(calls[0].not_fill_cache);
        assert!(!calls[0].replica_read);
        assert!(!calls[0].stale_read);
    }
}

#[test]
fn production_metadata_drives_supported_replica_policies_and_exact_request_flags() {
    struct Case {
        source: ReplicaReadType,
        address_suffix: &'static str,
        request_type: ClientReplicaReadType,
        replica_read: bool,
    }

    for case in [
        Case {
            source: ReplicaReadType::Leader,
            address_suffix: "-leader:20160",
            request_type: ClientReplicaReadType::Leader,
            replica_read: false,
        },
        Case {
            source: ReplicaReadType::Follower,
            address_suffix: "-learner:20160",
            request_type: ClientReplicaReadType::Follower,
            replica_read: true,
        },
        Case {
            source: ReplicaReadType::Mixed,
            address_suffix: "-follower:20160",
            request_type: ClientReplicaReadType::Mixed,
            replica_read: true,
        },
        Case {
            source: ReplicaReadType::PreferLeader,
            address_suffix: "-leader:20160",
            request_type: ClientReplicaReadType::PreferLeader,
            replica_read: false,
        },
        Case {
            source: ReplicaReadType::Learner,
            address_suffix: "-learner:20160",
            request_type: ClientReplicaReadType::Learner,
            replica_read: true,
        },
        Case {
            source: ReplicaReadType::Closest,
            address_suffix: "-follower:20160",
            request_type: ClientReplicaReadType::Mixed,
            replica_read: true,
        },
        Case {
            source: ReplicaReadType::ClosestAdaptive,
            address_suffix: "-follower:20160",
            request_type: ClientReplicaReadType::Mixed,
            replica_read: true,
        },
    ] {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let mut metadata = metadata("a", "z");
        metadata.session.replica_read = case.source;
        let mut runtime = InjectedQueryRuntime::new(transport(
            Rc::clone(&calls),
            [Ok(response(b"ok"))],
            [location_with_three_peers(1, "a", "z", "tikv-policy")],
        ));
        let mut result = select_result(&mut runtime, &transport_request(metadata));
        assert_eq!(result.next_raw().unwrap(), Some(b"ok".to_vec()));

        let calls = calls.borrow();
        assert_eq!(calls.len(), 1);
        assert!(
            calls[0].address.ends_with(case.address_suffix),
            "{:?} selected {}",
            case.source,
            calls[0].address
        );
        assert_eq!(calls[0].replica_read_type, case.request_type);
        assert_eq!(calls[0].replica_read, case.replica_read);
        assert!(!calls[0].stale_read);
    }
}

#[test]
fn labels_and_load_inputs_are_consumed_by_the_live_selector() {
    let configurations: [fn(&mut KvRequestMetadata); 2] = [
        |metadata: &mut KvRequestMetadata| {
            metadata.match_store_labels.push(tidb_distsql::StoreLabel {
                key: "zone".to_owned(),
                value: "z1".to_owned(),
            });
        },
        |metadata: &mut KvRequestMetadata| {
            metadata.session.store_busy_threshold_ms = 1;
        },
    ];
    for configure in configurations {
        let mut metadata = metadata("a", "z");
        metadata.session.replica_read = ReplicaReadType::Mixed;
        configure(&mut metadata);
        let calls = Rc::new(RefCell::new(Vec::new()));
        let mut runtime = InjectedQueryRuntime::new(transport(
            Rc::clone(&calls),
            [Ok(response(b"selected"))],
            [location_with_three_peers(1, "a", "z", "tikv-policy")],
        ));
        let mut result = runtime
            .select_with_runtime_stats(
                &transport_request(metadata),
                SelectInput::default(),
                QueryResultContext::new(Vec::new(), WarningCollector::new()),
                vec![1],
                2,
                true,
            )
            .expect("Campaign 14 selector metadata is supported");
        assert_eq!(result.next_raw().unwrap(), Some(b"selected".to_vec()));
        assert_eq!(calls.borrow().len(), 1);
    }
}

#[test]
fn fresh_queries_advance_the_transport_seed_once_each() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.session.replica_read = ReplicaReadType::Mixed;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [
            Ok(response(b"second-dispatched-first")),
            Ok(response(b"first-dispatched-second")),
        ],
        [location_with_three_peers(1, "a", "z", "tikv")],
    ));
    let request = transport_request(request_metadata);
    let mut first = select_result(&mut runtime, &request);
    let mut second = select_result(&mut runtime, &request);
    assert_eq!(
        second.next_raw().unwrap(),
        Some(b"second-dispatched-first".to_vec())
    );
    assert_eq!(second.next_raw().unwrap(), None);
    assert_eq!(
        first.next_raw().unwrap(),
        Some(b"first-dispatched-second".to_vec())
    );
    assert_eq!(first.next_raw().unwrap(), None);

    let addresses: Vec<_> = calls
        .borrow()
        .iter()
        .map(|call| call.address.clone())
        .collect();
    assert_eq!(
        addresses,
        ["tikv-learner:20160", "tikv-follower:20160"],
        "fresh query bindings must rotate before either response is pulled"
    );
}

#[test]
fn logical_tasks_in_one_query_share_the_bound_seed() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.session.replica_read = ReplicaReadType::Mixed;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(response(b"left")), Ok(response(b"right"))],
        [
            location_with_three_peers(1, "a", "m", "left"),
            location_with_three_peers(100, "m", "z", "right"),
        ],
    ));
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));
    assert_eq!(result.next_raw().unwrap(), Some(b"left".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"right".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    let addresses: Vec<_> = calls
        .borrow()
        .iter()
        .map(|call| call.address.clone())
        .collect();
    assert_eq!(
        addresses,
        ["left-follower:20160", "right-follower:20160"],
        "all logical tasks in one query must use the same immutable seed"
    );
}

#[test]
fn region_reload_reuses_the_bound_query_seed() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.session.replica_read = ReplicaReadType::Mixed;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(region_not_found(1)), Ok(response(b"fresh"))],
        [
            location_with_three_peers(1, "a", "z", "old"),
            location_with_three_peers(1, "a", "z", "new"),
        ],
    ));
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));
    assert_eq!(result.next_raw().unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    let addresses: Vec<_> = calls
        .borrow()
        .iter()
        .map(|call| call.address.clone())
        .collect();
    assert_eq!(
        addresses,
        ["old-follower:20160", "new-follower:20160"],
        "a rebuilt selector must retain the response-bound seed"
    );
}

#[test]
fn cached_leader_data_is_not_ready_falls_through_without_reload_or_backoff() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let mut initial =
        location_with_second_peer(1, "a", "z", "tikv-leader:20160", "tikv-follower:20160");
    initial.peers.swap(0, 1);
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(data_is_not_ready()), Ok(response(b"fresh"))],
        [initial],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            seed_read_bytes: 4096,
            observation_time,
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut request_metadata = metadata("a", "z");
    request_metadata.session.replica_read = ReplicaReadType::Leader;
    request_metadata.is_staleness = true;
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));
    assert_eq!(result.next_raw().unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    let calls = calls.borrow();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].address, "tikv-leader:20160");
    assert_eq!(calls[0].replica_read_type, ClientReplicaReadType::Mixed);
    assert!(!calls[0].replica_read);
    assert!(calls[0].stale_read);
    assert_eq!(calls[1].address, "tikv-follower:20160");
    assert_eq!(calls[1].replica_read_type, ClientReplicaReadType::Mixed);
    assert!(calls[1].replica_read);
    assert!(!calls[1].stale_read);
    assert_eq!(
        loader_calls.borrow().as_slice(),
        [b"a".to_vec()],
        "leader DataIsNotReady must not invalidate or reload the region"
    );
    assert!(
        retry_control.sleeps.borrow().is_empty(),
        "DataIsNotReady fallthrough must not back off"
    );
}

#[test]
fn stale_data_not_ready_then_known_leader_retries_one_selector_and_publishes_once() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut request_metadata = metadata("a", "z");
    request_metadata.session.replica_read = ReplicaReadType::Leader;
    request_metadata.is_staleness = true;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [
            Ok(data_is_not_ready()),
            Ok(not_leader(1, Some((102, 202)))),
            Ok(response(b"fresh")),
        ],
        [location_with_second_peer(
            1,
            "a",
            "z",
            "tikv-leader:20160",
            "tikv-follower:20160",
        )],
    ));
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));
    assert_eq!(result.next_raw().unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    let calls = calls.borrow();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0].address, "tikv-follower:20160");
    assert!(!calls[0].replica_read);
    assert!(calls[0].stale_read);
    assert_eq!(calls[1].address, "tikv-leader:20160");
    assert!(!calls[1].replica_read);
    assert!(!calls[1].stale_read);
    assert_eq!(calls[2].address, "tikv-follower:20160");
    assert!(!calls[2].replica_read);
    assert!(!calls[2].stale_read);
    assert_eq!(
        calls
            .iter()
            .map(|call| call.replica_read_type)
            .collect::<Vec<_>>(),
        [
            ClientReplicaReadType::Mixed,
            ClientReplicaReadType::Mixed,
            ClientReplicaReadType::Leader,
        ],
        "stale and ordinary fallback attempts stay Mixed until known-NotLeader transitions the selector to Leader"
    );
}

#[test]
fn known_leader_region_error_resends_immediately_in_the_same_query() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let first = location_with_second_peer(1, "a", "z", "tikv-old:20160", "tikv-new:20160");
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(not_leader(1, Some((102, 202)))), Ok(response(b"fresh"))],
        [first],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(60),
            seed_read_bytes: 4096,
            observation_time,
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let request = transport_request(metadata("a", "z"));

    let mut result = select_result(&mut runtime, &request);
    assert_eq!(result.next_raw().unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(
        loader_calls.borrow().as_slice(),
        [b"a".to_vec()],
        "known-leader retry must use the exact cache update without PD reload"
    );
    assert_eq!(calls.borrow()[0].address, "tikv-old:20160");
    assert_eq!(calls.borrow()[1].address, "tikv-new:20160");
    assert_eq!(calls.borrow()[1].peer_id, 102);
    assert_eq!(calls.borrow()[1].store_id, 202);
    assert!(retry_control.sleeps.borrow().is_empty());
}

#[test]
fn nil_leader_sleeps_then_invalidates_reloads_and_resends() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(not_leader(1, None)), Ok(response(b"reloaded"))],
        [
            location(1, "a", "z", "tikv-old:20160"),
            location(1, "a", "z", "tikv-new:20160"),
        ],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            seed_read_bytes: 4096,
            observation_time,
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(result.next_raw().unwrap(), Some(b"reloaded".to_vec()));
    assert_eq!(calls.borrow()[0].address, "tikv-old:20160");
    assert_eq!(calls.borrow()[1].address, "tikv-new:20160");
    assert_eq!(
        loader_calls.borrow().as_slice(),
        [b"a".to_vec(), b"a".to_vec()]
    );
    assert_eq!(
        retry_control.sleeps.borrow().as_slice(),
        [Duration::from_millis(2), Duration::from_millis(2)]
    );
}

#[test]
fn cancellation_during_nil_leader_sleep_keeps_cached_route_and_skips_pd_and_redispatch() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    retry_control.fail_next_sleep.set(true);
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(not_leader(1, None)), Ok(response(b"same-cached-route"))],
        [
            location(1, "a", "z", "tikv-old:20160"),
            location(1, "a", "z", "must-remain-unloaded:20160"),
        ],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            seed_read_bytes: 4096,
            observation_time,
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let request = transport_request(metadata("a", "z"));

    let mut cancelled = select_result(&mut runtime, &request);
    let error = cancelled.next_raw().unwrap_err().to_string();
    assert!(error.contains("query cancelled by caller"), "{error}");
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(loader_calls.borrow().as_slice(), [b"a".to_vec()]);

    let mut next_query = select_result(&mut runtime, &transport_request(metadata("a", "z")));
    assert_eq!(
        next_query.next_raw().unwrap(),
        Some(b"same-cached-route".to_vec())
    );
    assert_eq!(calls.borrow()[1].address, "tikv-old:20160");
    assert_eq!(loader_calls.borrow().as_slice(), [b"a".to_vec()]);
}

#[test]
fn retry_wait_never_crosses_the_bind_anchored_deadline() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_transport_failures(
        Rc::clone(&calls),
        [Err(connection_failure(
            "tikv-old:20160",
            1,
            DirectUnaryTransportClass::Connection,
            None,
        ))],
        [Ok(StoreLiveness::Reachable)],
        Rc::clone(&events),
        [location(1, "a", "z", "tikv-old:20160")],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut request_metadata = metadata("a", "z");
    request_metadata.session.tikv_client_read_timeout_ms = 50;
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));

    let error = result.next_raw().unwrap_err().to_string();
    assert!(error.contains("query deadline exceeded"), "{error}");
    assert!(!error.contains("cancelled by caller"), "{error}");
    assert!(retry_control.sleeps.borrow().is_empty());
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(
        events
            .borrow()
            .iter()
            .filter(|event| matches!(event, ClientEvent::Liveness { .. }))
            .count(),
        1
    );
}

#[test]
fn elapsed_deadline_blocks_zero_wait_dispatch_and_cancellation_wins() {
    for cancel_execution in [false, true] {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let retry_control = Rc::new(RecordingRetryControl::default());
        let execution = std::sync::Arc::new(tidb_distsql::CancelHandle::default());
        let mut request_metadata = metadata("a", "z");
        request_metadata.session.tikv_client_read_timeout_ms = 1;
        let request = TransportRequest::new(request_metadata, std::sync::Arc::clone(&execution));
        let transport = transport_with_loader_calls_and_config(
            Rc::clone(&calls),
            [Ok(response(b"must-not-dispatch"))],
            [location(1, "a", "z", "tikv-1:20160")],
            9001,
            Rc::new(RefCell::new(Vec::new())),
            DirectUnaryRuntimeConfig {
                region_retry_waiter: retry_control.clone(),
                ..DirectUnaryRuntimeConfig::default()
            },
        );
        let mut runtime = InjectedQueryRuntime::new(transport);
        let mut result = select_result(&mut runtime, &request);
        std::thread::sleep(Duration::from_millis(3));
        if cancel_execution {
            execution.cancel();
        }

        let error = result.next_raw().unwrap_err().to_string();
        if cancel_execution {
            assert!(error.contains("query cancelled by caller"), "{error}");
        } else {
            assert!(error.contains("query deadline exceeded"), "{error}");
        }
        assert!(calls.borrow().is_empty());
        assert!(retry_control.sleeps.borrow().is_empty());
    }
}

#[test]
fn rebuild_splits_failed_task_in_place_and_keeps_future_task_order_and_attempt() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [
            Ok(region_not_found(1)),
            Ok(response(b"split-left")),
            Ok(response(b"split-right")),
            Ok(response(b"future-original")),
        ],
        [
            location(1, "a", "m", "tikv-old-1:20160"),
            location(2, "m", "z", "tikv-old-2:20160"),
            location(10, "a", "g", "tikv-new-10:20160"),
            location(11, "g", "m", "tikv-new-11:20160"),
        ],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            seed_read_bytes: 4096,
            observation_time,
            region_retry_waiter: retry_control,
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(result.next_raw().unwrap(), Some(b"split-left".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"split-right".to_vec()));
    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"future-original".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    let calls = calls.borrow();
    assert_eq!(
        calls
            .iter()
            .map(|call| call.address.as_str())
            .collect::<Vec<_>>(),
        [
            "tikv-old-1:20160",
            "tikv-new-10:20160",
            "tikv-new-11:20160",
            "tikv-old-2:20160",
        ]
    );
    assert_eq!(calls[3].region_id, 2);
    assert_eq!(calls[3].task_id, 29);
}

#[test]
fn one_region_budget_is_shared_by_sender_and_outer_rebuild_backoff() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(not_leader(1, None))],
        [location(1, "a", "z", "tikv-old:20160")],
        9001,
        Rc::new(RefCell::new(Vec::new())),
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            region_retry_max_sleep: Duration::from_millis(1),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    let error = result.next_raw().unwrap_err().to_string();
    assert!(error.contains("terminal region error"), "{error}");
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(
        retry_control.sleeps.borrow().as_slice(),
        [Duration::from_millis(2)]
    );
}

#[test]
fn split_child_region_gets_an_independent_budget() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [Ok(region_not_found(1)), Ok(not_leader(10, None))],
        [
            location(1, "a", "z", "tikv-old:20160"),
            location(10, "a", "m", "tikv-new-10:20160"),
            location(11, "m", "z", "tikv-new-11:20160"),
        ],
        9001,
        Rc::new(RefCell::new(Vec::new())),
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            region_retry_max_sleep: Duration::from_millis(1),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    let error = result.next_raw().unwrap_err().to_string();
    assert!(error.contains("terminal region error"), "{error}");
    assert_eq!(calls.borrow().len(), 2);
    assert_eq!(calls.borrow()[1].region_id, 10);
    assert_eq!(
        retry_control.sleeps.borrow().as_slice(),
        [Duration::from_millis(2), Duration::from_millis(2)]
    );
}

#[test]
fn unknown_region_error_invalidates_and_rebuilds_under_outer_region_miss() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = transport_with_loader_calls_and_config(
        Rc::clone(&calls),
        [
            Ok(unknown_region_error("future kvproto field")),
            Ok(response(b"rebuilt")),
        ],
        [
            location(1, "a", "z", "tikv-old:20160"),
            location(1, "a", "z", "tikv-reloaded:20160"),
        ],
        9001,
        Rc::clone(&loader_calls),
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(result.next_raw().unwrap(), Some(b"rebuilt".to_vec()));
    assert_eq!(calls.borrow()[1].address, "tikv-reloaded:20160");
    assert_eq!(
        loader_calls.borrow().as_slice(),
        [b"a".to_vec(), b"a".to_vec()]
    );
    assert_eq!(
        retry_control.sleeps.borrow().as_slice(),
        [Duration::from_millis(2)]
    );
}

#[test]
fn only_successful_paging_creates_a_continuation_attempt() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let first = CoprocessorResponse {
        data: b"page-one".to_vec(),
        range: Some(CoprocessorKeyRange {
            start: b"a".to_vec(),
            end: b"m".to_vec(),
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec();
    let mut metadata = metadata("a", "z");
    metadata.session.paging.enabled = true;
    metadata.session.paging.min_size = 2;
    metadata.session.paging.max_size = 8;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(first), Ok(response(b"page-two"))],
        [location(1, "a", "z", "tikv-1:20160")],
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"page-one".to_vec()));
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(result.next_raw().unwrap(), Some(b"page-two".to_vec()));
    assert_eq!(calls.borrow().len(), 2);
    assert_eq!(result.next_raw().unwrap(), None);
}

#[test]
fn first_real_unary_response_replaces_the_seed_before_continuation() {
    // pkg/store/copr/ema.go:33-36 newRUEMA leaves lastObsAt at zero so the
    // first time.Now observation has unit alpha and replaces the byte seed.
    let calls = Rc::new(RefCell::new(Vec::new()));
    let first = CoprocessorResponse {
        data: b"page-one".to_vec(),
        range: Some(CoprocessorKeyRange {
            start: b"a".to_vec(),
            end: b"m".to_vec(),
        }),
        exec_details_v2: Some(CoprocessorExecDetailsV2 {
            scan_detail_v2: Some(CoprocessorScanDetailV2 {
                processed_versions_size: 1_000_000,
                total_versions_size: 1_000_000,
            }),
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec();
    let mut metadata = metadata("a", "z");
    metadata.session.paging.enabled = true;
    metadata.session.paging.min_size = 2;
    metadata.session.paging.max_size = 8;
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(first), Ok(response(b"page-two"))],
        [location(1, "a", "z", "tikv-1:20160")],
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"page-one".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"page-two".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    let calls = calls.borrow();
    assert_eq!(calls[0].predicted_read_bytes, 4096);
    assert_eq!(calls[1].predicted_read_bytes, 1_000_000);
}

#[test]
fn close_before_pull_stops_every_unsent_attempt() {
    let cancel = std::sync::Arc::new(tidb_distsql::CancelHandle::default());
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(response(b"never"))],
        [location(1, "a", "z", "tikv-1:20160")],
    ));
    let request = TransportRequest::new(metadata("a", "z"), std::sync::Arc::clone(&cancel));
    let mut result = select_result(&mut runtime, &request);
    result.close();
    result.close();
    assert!(
        !cancel.is_cancelled(),
        "closing one response must not cancel the outer execution"
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert!(calls.borrow().is_empty());
}

#[test]
fn remote_canceled_closes_exact_generation_before_resending_the_same_task() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        [
            Err(connection_failure(
                "tikv-1:20160",
                41,
                DirectUnaryTransportClass::RemoteGrpc,
                Some(DirectUnaryGrpcCode::Canceled),
            )),
            Ok(response(b"retried")),
        ],
        [Ok(StoreLiveness::Unreachable)],
        Rc::clone(&events),
        [location_with_second_peer(
            1,
            "a",
            "z",
            "tikv-1:20160",
            "tikv-2:20160",
        )],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(result.next_raw().unwrap(), Some(b"retried".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(calls.borrow().len(), 2);
    assert_eq!(calls.borrow()[0].region_id, calls.borrow()[1].region_id);
    assert_eq!(calls.borrow()[0].data, calls.borrow()[1].data);
    assert_eq!(
        events.borrow().as_slice(),
        [
            ClientEvent::Send("tikv-1:20160".to_owned()),
            ClientEvent::CloseGeneration {
                address: "tikv-1:20160".to_owned(),
                version: 41,
            },
            ClientEvent::Liveness {
                address: "tikv-1:20160".to_owned(),
                timeout: Duration::from_secs(1),
            },
            ClientEvent::Send("tikv-2:20160".to_owned()),
        ]
    );
    assert_eq!(retry_control.sleeps.borrow().len(), 1);
}

#[test]
fn unreachable_store_reselects_an_alternate_and_promotes_it_for_the_next_query() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        [
            Err(connection_failure(
                "tikv-old:20160",
                9,
                DirectUnaryTransportClass::Connection,
                None,
            )),
            Ok(response(b"alternate")),
            Ok(response(b"promoted")),
        ],
        [Ok(StoreLiveness::Unreachable)],
        Rc::clone(&events),
        [location_with_second_peer(
            1,
            "a",
            "z",
            "tikv-old:20160",
            "tikv-new:20160",
        )],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let request = transport_request(metadata("a", "z"));
    let mut first = select_result(&mut runtime, &request);

    assert_eq!(first.next_raw().unwrap(), Some(b"alternate".to_vec()));
    assert_eq!(first.next_raw().unwrap(), None);
    drop(first);
    let mut second = select_result(&mut runtime, &request);
    assert_eq!(second.next_raw().unwrap(), Some(b"promoted".to_vec()));
    assert_eq!(second.next_raw().unwrap(), None);
    assert_eq!(
        calls
            .borrow()
            .iter()
            .map(|call| call.address.as_str())
            .collect::<Vec<_>>(),
        ["tikv-old:20160", "tikv-new:20160", "tikv-new:20160"]
    );
    assert_eq!(
        events.borrow()[..2],
        [
            ClientEvent::Send("tikv-old:20160".to_owned()),
            ClientEvent::Liveness {
                address: "tikv-old:20160".to_owned(),
                timeout: Duration::from_secs(1),
            },
        ]
    );
    assert_eq!(retry_control.sleeps.borrow().len(), 1);
}

#[test]
fn one_store_failure_stales_later_bound_regions_without_reordering_them() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let shared_leader = Store {
        id: 201,
        address: "tikv-dead:20160".to_owned(),
        epoch: 7,
    };
    let first = RegionLocation {
        region: RegionVerId::new(1, 1, 2),
        start_key: b"a".to_vec(),
        end_key: b"m".to_vec(),
        peers: vec![
            Peer {
                id: 101,
                store_id: 201,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 7,
            },
            Peer {
                id: 102,
                store_id: 202,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 7,
            },
        ],
        leader_peer_id: Some(101),
        stores: vec![
            shared_leader.clone(),
            Store {
                id: 202,
                address: "tikv-first-alternate:20160".to_owned(),
                epoch: 7,
            },
        ],
        ..RegionLocation::default()
    };
    let second = RegionLocation {
        region: RegionVerId::new(2, 1, 2),
        start_key: b"m".to_vec(),
        end_key: b"z".to_vec(),
        peers: vec![
            Peer {
                id: 201,
                store_id: 201,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 7,
            },
            Peer {
                id: 202,
                store_id: 203,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 7,
            },
        ],
        leader_peer_id: Some(201),
        stores: vec![
            shared_leader,
            Store {
                id: 203,
                address: "tikv-second-alternate:20160".to_owned(),
                epoch: 7,
            },
        ],
        ..RegionLocation::default()
    };
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        [
            Err(connection_failure(
                "tikv-dead:20160",
                4,
                DirectUnaryTransportClass::Connection,
                None,
            )),
            Ok(response(b"first")),
            Ok(response(b"second")),
        ],
        [Ok(StoreLiveness::Unreachable)],
        Rc::clone(&events),
        [first, second],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control,
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(result.next_raw().unwrap(), Some(b"first".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"second".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(
        calls
            .borrow()
            .iter()
            .map(|call| (call.region_id, call.address.as_str()))
            .collect::<Vec<_>>(),
        [
            (1, "tikv-dead:20160"),
            (1, "tikv-first-alternate:20160"),
            (2, "tikv-second-alternate:20160"),
        ]
    );
    assert_eq!(
        events
            .borrow()
            .iter()
            .filter(|event| matches!(event, ClientEvent::Send(_)))
            .count(),
        3
    );
}

#[derive(Debug, Default)]
struct DelayedStoreMismatchState {
    active_generation: Option<u64>,
    next_generation: u64,
    close_requests: Vec<u64>,
    force_closed_generations: Vec<u64>,
    sent_generations: Vec<u64>,
}

struct DelayedStoreMismatchClient {
    state: Rc<RefCell<DelayedStoreMismatchState>>,
    replace_before_first_response: bool,
}

impl DirectUnaryClient for DelayedStoreMismatchClient {
    fn send_request(
        &mut self,
        address: &str,
        _request: &DirectUnaryRequest,
        _timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        assert_eq!(address, "shared-tikv:20160");
        let mut state = self.state.borrow_mut();
        let generation = match state.active_generation {
            Some(generation) => generation,
            None => {
                state.next_generation += 1;
                let generation = state.next_generation;
                state.active_generation = Some(generation);
                generation
            }
        };
        state.sent_generations.push(generation);
        if state.sent_generations.len() == 1 {
            assert_eq!(generation, 1);
            if self.replace_before_first_response {
                // Session B replaces the channel while session A's request is
                // in flight. The delayed StoreNotMatch below still belongs to
                // generation 1 and must not close this replacement.
                state.active_generation = Some(2);
                state.next_generation = 2;
            }
            return Ok(DirectUnaryResponse::new(
                store_not_match(1),
                address,
                generation,
            ));
        }
        assert_eq!(generation, 2);
        Ok(DirectUnaryResponse::new(
            response(b"replacement-survived"),
            address,
            generation,
        ))
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.send_request(address, request, call.timeout())
    }

    fn close_address(&mut self, address: &str) -> Result<(), DirectUnaryClientError> {
        assert_eq!(address, "shared-tikv:20160");
        let mut state = self.state.borrow_mut();
        if let Some(generation) = state.active_generation.take() {
            state.force_closed_generations.push(generation);
        }
        Ok(())
    }

    fn close_address_version(
        &mut self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        assert_eq!(address, "shared-tikv:20160");
        let mut state = self.state.borrow_mut();
        state.close_requests.push(version);
        if state
            .active_generation
            .is_some_and(|generation| generation == version)
        {
            let generation = state.active_generation.take().unwrap();
            state.force_closed_generations.push(generation);
        }
        Ok(())
    }

    fn liveness(
        &self,
        _address: &str,
        _timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        Ok(StoreLiveness::Reachable)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }
}

impl tidb_txnkv::lock::LockRecoveryClient for DelayedStoreMismatchClient {
    fn check_txn_status_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcCheckTxnStatusRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "unexpected lock in delayed StoreNotMatch read".to_owned(),
        ))
    }

    fn resolve_lock_for_read(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcResolveLockRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcResolveLockResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "unexpected lock in delayed StoreNotMatch read".to_owned(),
        ))
    }
}

fn run_store_not_match_with_channel_replacement(
    replace_before_first_response: bool,
) -> Rc<RefCell<DelayedStoreMismatchState>> {
    let state = Rc::new(RefCell::new(DelayedStoreMismatchState {
        active_generation: Some(1),
        next_generation: 1,
        ..DelayedStoreMismatchState::default()
    }));
    let transport = DirectUnaryQueryTransport::new_injected(
        DelayedStoreMismatchClient {
            state: Rc::clone(&state),
            replace_before_first_response,
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::new(RefCell::new(Vec::new())),
            regions: [
                location(1, "a", "z", "shared-tikv:20160"),
                location(1, "a", "z", "shared-tikv:20160"),
            ]
            .into_iter()
            .collect(),
        }),
        DirectUnaryRuntimeConfig::default(),
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = runtime
        .select_with_runtime_stats(
            &transport_request(metadata("a", "z")),
            SelectInput::default(),
            QueryResultContext::new(Vec::<FieldType>::new(), WarningCollector::new()),
            vec![1],
            2,
            true,
        )
        .unwrap();

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"replacement-survived".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    drop(result);
    state
}

#[test]
fn current_store_not_match_closes_exact_observed_channel() {
    let state = run_store_not_match_with_channel_replacement(false);
    let state = state.borrow();
    assert_eq!(state.sent_generations, [1, 2]);
    assert_eq!(state.close_requests, [1]);
    assert_eq!(state.force_closed_generations, [1]);
    assert_eq!(state.active_generation, Some(2));
}

#[test]
fn delayed_store_not_match_cannot_close_a_replacement_channel() {
    let state = run_store_not_match_with_channel_replacement(true);
    let state = state.borrow();
    assert_eq!(state.sent_generations, [1, 2]);
    assert_eq!(state.close_requests, [1]);
    assert!(state.force_closed_generations.is_empty());
    assert_eq!(state.active_generation, Some(2));
}

#[test]
fn forwarded_store_not_match_invalidates_target_without_closing_proxy_channel() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let location =
        location_with_second_peer(1, "a", "z", "logical-target:20160", "healthy-proxy:20160");
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        [
            Err(connection_failure(
                "logical-target:20160",
                1,
                DirectUnaryTransportClass::Connection,
                None,
            )),
            Ok(store_not_match(1)),
            Ok(response(b"logical-route-reloaded")),
        ],
        [Ok(StoreLiveness::Unreachable)],
        Rc::clone(&events),
        [location.clone(), location],
        DirectUnaryRuntimeConfig {
            enable_forwarding: true,
            region_retry_waiter: Rc::new(RecordingRetryControl::default()),
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"logical-route-reloaded".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(
        calls
            .borrow()
            .iter()
            .map(|call| call.address.as_str())
            .collect::<Vec<_>>(),
        [
            "logical-target:20160",
            "healthy-proxy:20160",
            "healthy-proxy:20160",
        ]
    );
    assert!(events.borrow().iter().all(|event| {
        !matches!(
            event,
            ClientEvent::ForceClose(address)
                | ClientEvent::CloseGeneration { address, .. }
                if address == "healthy-proxy:20160"
        )
    }));
}

#[test]
fn transport_attempt_exhaustion_rebuilds_through_region_miss_instead_of_returning_client() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let mut responses = Vec::new();
    for version in 1..=10 {
        responses.push(Err(connection_failure(
            "tikv-stuck:20160",
            version,
            DirectUnaryTransportClass::Connection,
            None,
        )));
    }
    responses.push(Ok(response(b"reloaded-after-exhaustion")));
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        responses,
        std::iter::repeat_n(Ok(StoreLiveness::Reachable), 10),
        Rc::clone(&events),
        [
            location(1, "a", "z", "tikv-stuck:20160"),
            location(1, "a", "z", "tikv-reloaded:20160"),
        ],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            region_retry_max_sleep: Duration::from_secs(60),
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let mut request_metadata = metadata("a", "z");
    request_metadata.session.tikv_client_read_timeout_ms = 60_000;
    let mut result = select_result(&mut runtime, &transport_request(request_metadata));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"reloaded-after-exhaustion".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    let calls = calls.borrow();
    assert_eq!(calls.len(), 11);
    assert!(calls[..10]
        .iter()
        .all(|call| call.address == "tikv-stuck:20160"));
    assert_eq!(calls[10].address, "tikv-reloaded:20160");
    assert_eq!(
        events
            .borrow()
            .iter()
            .filter(|event| matches!(event, ClientEvent::Liveness { .. }))
            .count(),
        10
    );
    assert_eq!(retry_control.sleeps.borrow().len(), 11);
}

#[test]
fn caller_cancellation_is_terminal_before_failure_consumption_or_retry_mutation() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let mut runtime = InjectedQueryRuntime::new(transport_with_transport_failures(
        Rc::clone(&calls),
        [
            Err(DirectUnaryClientError::CallerCancelled),
            Ok(response(b"same-cached-route-next-query")),
        ],
        [],
        Rc::clone(&events),
        [location(1, "a", "z", "tikv-1:20160")],
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
    ));
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    let error = result.next_raw().unwrap_err().to_string();
    assert!(error.contains("cancelled by caller"), "{error}");
    assert_eq!(result.next_raw().unwrap(), None);
    drop(result);
    let mut next = select_result(&mut runtime, &transport_request(metadata("a", "z")));
    assert_eq!(
        next.next_raw().unwrap(),
        Some(b"same-cached-route-next-query".to_vec())
    );
    assert_eq!(next.next_raw().unwrap(), None);
    assert_eq!(calls.borrow().len(), 2);
    assert!(calls
        .borrow()
        .iter()
        .all(|call| call.address == "tikv-1:20160"));
    assert_eq!(
        events.borrow().as_slice(),
        [
            ClientEvent::Send("tikv-1:20160".to_owned()),
            ClientEvent::Send("tikv-1:20160".to_owned()),
        ]
    );
    assert!(retry_control.sleeps.borrow().is_empty());
}

#[test]
fn local_batch_admission_busy_falls_back_without_route_failure_feedback() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let retry_control = Rc::new(RecordingRetryControl::default());
    let transport = DirectUnaryQueryTransport::new_injected_batch_first(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::from([Ok(response(b"sync-after-local-admission"))]),
            events: Rc::clone(&events),
            liveness: RefCell::new(VecDeque::new()),
            batch_errors: RefCell::new(VecDeque::from([DirectUnaryClientError::AdmissionBusy {
                address: "tikv-1:20160".to_owned(),
            }])),
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: Rc::new(RefCell::new(Vec::new())),
            regions: VecDeque::from([location(1, "a", "z", "tikv-1:20160")]),
        }),
        DirectUnaryRuntimeConfig {
            region_retry_waiter: retry_control.clone(),
            ..DirectUnaryRuntimeConfig::default()
        },
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap();
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));

    assert_eq!(
        result.next_raw().unwrap(),
        Some(b"sync-after-local-admission".to_vec())
    );
    assert_eq!(result.next_raw().unwrap(), None);
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(
        events.borrow().as_slice(),
        [ClientEvent::Send("tikv-1:20160".to_owned())]
    );
    assert!(retry_control.sleeps.borrow().is_empty());
}

#[test]
fn return_region_error_and_non_connection_failures_close_without_future_dispatch() {
    let cases = [
        (
            Ok(undetermined_region_error("ambiguous write")),
            "region_error",
        ),
        (Ok(raft_entry_too_large(1)), "terminal region error"),
        (Err("connection reset".to_owned()), "connection reset"),
        (Ok(vec![0x0a, 0x02, 0x01]), "invalid unary response"),
    ];
    for (scripted, expected) in cases {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let mut runtime = InjectedQueryRuntime::new(transport(
            Rc::clone(&calls),
            [scripted, Ok(response(b"must remain unsent"))],
            [
                location(1, "a", "m", "tikv-1:20160"),
                location(2, "m", "z", "tikv-2:20160"),
            ],
        ));
        let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));
        let error = result.next_raw().unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
        assert_eq!(result.next_raw().unwrap(), None);
        // A terminal error on the first logical task must not probe the next
        // address or consume its scripted response.
        assert_eq!(calls.borrow().len(), 1);
    }
}

#[test]
fn missing_cluster_loader_failure_and_empty_pd_address_fail_before_client_dispatch() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let missing_cluster = DirectUnaryQueryTransport::new_injected(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::new(),
            events: Rc::new(RefCell::new(Vec::new())),
            liveness: RefCell::new(VecDeque::new()),
            batch_errors: RefCell::new(VecDeque::new()),
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 0,
            calls: Rc::new(RefCell::new(Vec::new())),
            regions: VecDeque::new(),
        }),
        DirectUnaryRuntimeConfig::default(),
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .err()
    .unwrap();
    assert_eq!(
        missing_cluster,
        DirectUnaryTransportError::Route(RegionRouteError::MissingClusterId)
    );

    let mut empty = location(2, "a", "z", "ignored");
    empty.stores[0].address.clear();
    let mut runtime =
        InjectedQueryRuntime::new(transport(Rc::clone(&calls), std::iter::empty(), [empty]));
    let mut result = select_result(&mut runtime, &transport_request(metadata("a", "z")));
    let error = result.next_raw().unwrap_err().to_string();
    assert!(error.contains("MissingAddress(202)"), "{error}");
    assert!(calls.borrow().is_empty());

    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        std::iter::empty(),
        std::iter::empty(),
    ));
    let error = runtime
        .select_with_runtime_stats(
            &transport_request(metadata("a", "z")),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            Vec::new(),
            0,
            false,
        )
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("scripted-pd-empty"), "{error}");
    assert!(calls.borrow().is_empty());
}

#[test]
fn unsupported_operation_fails_before_preparing_or_sending() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport_with_loader_calls(
        Rc::clone(&calls),
        std::iter::empty(),
        [location(1, "a", "z", "one")],
        9001,
        Rc::clone(&loader_calls),
    ));
    let error = runtime
        .select(
            &transport_request(metadata("a", "z")),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
        )
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("unsupported direct unary operation Select"));
    assert!(calls.borrow().is_empty());
    assert!(loader_calls.borrow().is_empty());
}

#[test]
fn closest_replica_policy_with_labels_reaches_the_live_selector() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport_with_loader_calls(
        Rc::clone(&calls),
        [Ok(response(b"closest"))],
        [location(1, "a", "z", "one")],
        9001,
        Rc::clone(&loader_calls),
    ));
    let mut closest_with_labels = metadata("a", "z");
    closest_with_labels.session.replica_read = tidb_distsql::ReplicaReadType::Closest;
    closest_with_labels
        .match_store_labels
        .push(tidb_distsql::StoreLabel {
            key: "zone".to_owned(),
            value: "z1".to_owned(),
        });

    let mut result = runtime
        .select_with_runtime_stats(
            &transport_request(closest_with_labels),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            vec![1],
            2,
            true,
        )
        .expect("closest label policy is supported");
    assert_eq!(result.next_raw().unwrap(), Some(b"closest".to_vec()));
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(loader_calls.borrow().as_slice(), &[b"a".to_vec()]);
}

#[test]
fn unsupported_request_shape_fails_before_pd_or_tikv() {
    let mut tiflash = metadata("a", "z");
    tiflash.store_type = StoreType::TiFlash;
    let mut analyze = metadata("a", "z");
    analyze.request_type = RequestType::Analyze;
    let mut unordered = metadata("a", "z");
    unordered.keep_order = false;
    let mut batched = metadata("a", "z");
    batched.batch_cop = true;

    for invalid in [tiflash, analyze, unordered, batched] {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let loader_calls = Rc::new(RefCell::new(Vec::new()));
        let mut runtime = InjectedQueryRuntime::new(transport_with_loader_calls(
            Rc::clone(&calls),
            std::iter::empty(),
            [location(1, "a", "z", "one")],
            9001,
            Rc::clone(&loader_calls),
        ));

        assert!(runtime
            .select_with_runtime_stats(
                &transport_request(invalid),
                SelectInput::default(),
                QueryResultContext::new(Vec::new(), WarningCollector::new()),
                Vec::new(),
                0,
                false,
            )
            .is_err());
        assert!(calls.borrow().is_empty());
        assert!(loader_calls.borrow().is_empty());
    }
}

#[test]
fn batch_failure_retains_the_logical_request_selector_for_sync_fallback() {
    let source = include_str!("../src/cop_paging/direct_unary_query_transport.rs");
    let settle = source
        .find("fn settle_dispatch(")
        .expect("shared async and sync settlement owner");
    let record = source[settle..]
        .find("self.record_attempt_result(logical_task_id")
        .map(|offset| settle + offset)
        .expect("one selector records the failed batch attempt");
    let recover = source[record..]
        .find("self.recover_transport_failure(")
        .map(|offset| record + offset)
        .expect("same synchronous recovery loop");
    let recovery_owner = source[recover..]
        .find("fn recover_transport_failure(")
        .map(|offset| recover + offset)
        .expect("response-owned synchronous recovery method");
    let retry = source[recovery_owner..]
        .find("self.install_same_task_retry(replacement)")
        .map(|offset| recovery_owner + offset)
        .expect("same logical task is reinstalled");
    let next_dispatch = &source[retry..];
    assert!(record < recover);
    assert!(recover < retry);
    assert!(next_dispatch.contains("debug_assert!(self.request_selectors.contains_key"));
    assert!(!source[recovery_owner..retry].contains("request_selectors.remove(&logical_task_id)"));
}
