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

//! The scripted PD, TiKV client and request builders every
//! `direct_unary_*` runtime test is written against.
//!
//! Nothing here mocks the runtime under test: the region loader, the unary
//! client and the async dispatcher are *scripts* -- an ordered queue of
//! responses plus a recording of every call actually made -- so a test asserts
//! the sequence of requests the real
//! [`DirectUnaryQueryTransport`](tidb_distsql::DirectUnaryQueryTransport)
//! produced. The region-error encoders build the exact `errorpb` payloads TiKV
//! returns, so an error path is exercised through the bytes rather than
//! through a Rust enum a test invented.
//!
//! A plain sibling module of the aggregated integration-test root, reached by
//! path from each family file rather than re-included by them.

#![allow(missing_docs)]

pub use std::cell::{Cell, RefCell};
pub use std::collections::VecDeque;
pub use std::rc::Rc;
pub use std::sync::{Arc, Mutex};
pub use std::time::Duration;

pub use prost::Message;
pub use tidb_datatype::FieldType;
pub use tidb_distsql::cop_paging::RegionRetryWaiter;
pub use tidb_distsql::{
    CoprCache, CoprCacheConfig, DirectUnaryClient, DirectUnaryClientError,
    DirectUnaryQueryTransport, DirectUnaryRequest, DirectUnaryResponse, DirectUnaryRuntimeConfig,
    DirectUnaryTransportError, InjectedQueryRuntime, KvRequestMetadata, QueryResultContext,
    ReplicaReadType, RequestKeyRange, RequestKeyRanges, RequestType, SelectInput, StoreType,
    TransportRequest, WarningCollector,
};

pub fn transport_request(metadata: KvRequestMetadata) -> TransportRequest {
    TransportRequest::new(
        metadata,
        std::sync::Arc::new(tidb_distsql::CancelHandle::default()),
    )
}
pub use tidb_proto::{
    errorpb, metapb, CoprocessorExecDetailsV2, CoprocessorKeyRange, CoprocessorRequest,
    CoprocessorResponse, CoprocessorScanDetailV2, CoprocessorTimeDetailV2, KvrpcLockInfo,
};
pub use tidb_txnkv::region::{
    Peer, PeerRole, RegionCache, RegionLoadError, RegionLoader, RegionLocation, RegionMetadata,
    RegionRecoveryLoader, RegionRouteError, RegionVerId, Store, StoreLiveness,
};
pub use tidb_txnkv::rpc::{
    completion_pair, AsyncRequestDispatcher, AsyncRequestPublication, CompletionError,
    CompletionNotifier, CompletionPull, CompletionRequest, CompletionRunLoop, PendingRequest,
};
pub use tidb_txnkv::UnaryCallContext;
pub use tidb_txnkv::{
    ClientReplicaReadType, DirectUnaryConnectionError, DirectUnaryGrpcCode,
    DirectUnaryTransportClass,
};

pub const OBSERVATION_TIME: Duration = Duration::from_secs(1_000);

pub fn observation_time() -> Duration {
    OBSERVATION_TIME
}

#[derive(Debug, Default)]
pub struct RecordingRetryControl {
    pub sleeps: RefCell<Vec<Duration>>,
    pub fail_next_sleep: Cell<bool>,
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
pub struct ObservedCall {
    pub address: String,
    pub forwarded_host: Option<String>,
    pub timeout: Duration,
    pub region_id: u64,
    pub data: Vec<u8>,
    pub paging_size: u64,
    pub is_cache_enabled: bool,
    pub cache_if_match_version: u64,
    pub predicted_read_bytes: u64,
    pub cluster_id: u64,
    pub conf_ver: u64,
    pub version: u64,
    pub peer_id: u64,
    pub store_id: u64,
    pub peer_role: i32,
    pub is_witness: bool,
    pub task_id: u64,
    pub request_source: String,
    pub not_fill_cache: bool,
    pub replica_read_type: ClientReplicaReadType,
    pub replica_read: bool,
    pub stale_read: bool,
}

pub struct ScriptedLoader {
    pub cluster_id: u64,
    pub calls: Rc<RefCell<Vec<Vec<u8>>>>,
    pub regions: VecDeque<RegionLocation>,
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

pub struct ScriptedClient {
    pub calls: Rc<RefCell<Vec<ObservedCall>>>,
    pub responses: VecDeque<Result<Vec<u8>, DirectUnaryClientError>>,
    pub events: Rc<RefCell<Vec<ClientEvent>>>,
    pub liveness: RefCell<VecDeque<Result<StoreLiveness, DirectUnaryClientError>>>,
    pub batch_errors: RefCell<VecDeque<DirectUnaryClientError>>,
    pub batch_ready_immediately: RefCell<VecDeque<bool>>,
    pub batch_completion_gate: Option<Rc<Cell<bool>>>,
}

pub struct ScriptedPending {
    pub completion: CompletionPull<DirectUnaryResponse, DirectUnaryClientError>,
    pub deferred: Option<(
        CompletionRequest<DirectUnaryResponse, DirectUnaryClientError>,
        Result<DirectUnaryResponse, DirectUnaryClientError>,
    )>,
    pub publication: Option<AsyncRequestPublication>,
    pub completion_gate: Option<Rc<Cell<bool>>>,
}

impl PendingRequest for ScriptedPending {
    fn set_notifier(&mut self, notifier: CompletionNotifier, token: u64) {
        self.completion.set_notifier(notifier, token);
    }

    fn publication(&self) -> Option<AsyncRequestPublication> {
        self.publication.clone()
    }

    fn try_complete(
        &mut self,
    ) -> Result<Option<Result<DirectUnaryResponse, DirectUnaryClientError>>, CompletionError> {
        self.completion.try_complete()
    }

    fn complete(
        &mut self,
        call: &UnaryCallContext,
    ) -> Result<Result<DirectUnaryResponse, DirectUnaryClientError>, CompletionError> {
        if let Some(gate) = &self.completion_gate {
            assert!(
                gate.get(),
                "publication observer must run before pending completion"
            );
        }
        if let Some((completion, result)) = self.deferred.take() {
            completion.schedule(result);
        }
        self.completion.complete(call)
    }

    fn cancel(&mut self) {
        self.completion.cancel();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientEvent {
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
        self.send_request_recorded(address, None, request, timeout)
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
        let result = self.send_request_recorded(address, None, request, call.timeout());
        if call.cancellation().is_cancelled() {
            Err(DirectUnaryClientError::CallerCancelled)
        } else {
            result
        }
    }

    fn send_request_with_route(
        &mut self,
        address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        if call.cancellation().is_cancelled() {
            return Err(DirectUnaryClientError::CallerCancelled);
        }
        let result = self.send_request_recorded(address, forwarded_host, request, call.timeout());
        if call.cancellation().is_cancelled() {
            Err(DirectUnaryClientError::CallerCancelled)
        } else {
            result
        }
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

impl ScriptedClient {
    pub fn send_request_recorded(
        &mut self,
        address: &str,
        forwarded_host: Option<&str>,
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
            forwarded_host: forwarded_host.map(str::to_owned),
            timeout,
            region_id: request.context.region_id,
            data: wire.data,
            paging_size: wire.paging_size,
            is_cache_enabled: wire.is_cache_enabled,
            cache_if_match_version: wire.cache_if_match_version,
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
}

impl AsyncRequestDispatcher for ScriptedClient {
    type Pending = ScriptedPending;

    fn begin(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<Self::Pending, DirectUnaryClientError> {
        let (completion, pull) = completion_pair(CompletionRunLoop::new(), || {});
        if let Some(error) = self.batch_errors.borrow_mut().pop_front() {
            completion.schedule(Err(error));
            return Ok(ScriptedPending {
                completion: pull,
                deferred: None,
                publication: None,
                completion_gate: self.batch_completion_gate.clone(),
            });
        }
        let result = self.send_request_with_context(physical_address, request, call);
        let ready_immediately = self
            .batch_ready_immediately
            .borrow_mut()
            .pop_front()
            .unwrap_or(false);
        let deferred = if ready_immediately {
            completion.schedule(result);
            None
        } else {
            Some((completion, result))
        };
        Ok(ScriptedPending {
            completion: pull,
            deferred,
            publication: Some(AsyncRequestPublication::new(
                physical_address,
                7,
                11,
                forwarded_host.map(str::to_owned),
            )),
            completion_gate: self.batch_completion_gate.clone(),
        })
    }
}

impl tidb_txnkv::lock::LockRecoveryClient for ScriptedClient {

    fn check_secondary_locks_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcCheckSecondaryLocksRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
        panic!("this test does not resolve async-commit locks")
    }
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


    fn pessimistic_rollback_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcPessimisticRollbackRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
        panic!("runtime test does not clean pessimistic locks")
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

pub fn range(start: &str, end: &str) -> RequestKeyRange {
    RequestKeyRange {
        start_key: start.as_bytes().to_vec().into(),
        end_key: end.as_bytes().to_vec().into(),
    }
}

pub fn metadata(start: &str, end: &str) -> KvRequestMetadata {
    let mut metadata = KvRequestMetadata::default();
    metadata.request_type = RequestType::Dag;
    metadata.data = Some(b"dag-read".to_vec());
    metadata.key_ranges = Some(RequestKeyRanges::new_non_partitioned(vec![range(start, end)]));
    metadata.keep_order = true;
    // Keep the generic dispatch fixture focused on routing/order. Paging
    // tests opt in explicitly; production defaults are covered by
    // `paging_source`.
    metadata.paging.enabled = false;
    metadata.store_type = StoreType::TiKv;
    metadata.start_ts = 42;
    metadata.read_replica_scope = "global".to_owned();
    metadata.txn_scope = "global".to_owned();
    metadata.tikv_client_read_timeout_ms = 777;
    metadata.task_id = 29;
    metadata.request_source.internal = true;
    metadata.request_source.source_type = "ddl".to_owned();
    metadata.not_fill_cache = true;
    metadata
}

pub fn location(region_id: u64, start: &str, end: &str, address: &str) -> RegionLocation {
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

pub fn location_with_second_peer(
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

pub fn location_with_three_peers(
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

pub fn not_leader(region_id: u64, leader: Option<(u64, u64)>) -> Vec<u8> {
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

pub fn data_is_not_ready() -> Vec<u8> {
    CoprocessorResponse {
        region_error: Some(errorpb::Error {
            data_is_not_ready: Some(errorpb::DataIsNotReady::default()),
            ..errorpb::Error::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

pub fn region_not_found(region_id: u64) -> Vec<u8> {
    CoprocessorResponse {
        region_error: Some(errorpb::Error {
            region_not_found: Some(errorpb::RegionNotFound { region_id }),
            ..errorpb::Error::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

pub fn store_not_match(region_id: u64) -> Vec<u8> {
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

pub fn raft_entry_too_large(region_id: u64) -> Vec<u8> {
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

pub fn undetermined_region_error(message: &str) -> Vec<u8> {
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

pub fn unknown_region_error(message: &str) -> Vec<u8> {
    CoprocessorResponse {
        region_error: Some(errorpb::Error {
            message: message.to_owned(),
            ..errorpb::Error::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

pub fn response(data: &[u8]) -> Vec<u8> {
    CoprocessorResponse {
        data: data.to_vec(),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

pub fn locked_response(lock: KvrpcLockInfo) -> Vec<u8> {
    CoprocessorResponse {
        locked: Some(lock),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
}

pub fn connection_failure(
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

pub fn transport(
    calls: Rc<RefCell<Vec<ObservedCall>>>,
    responses: impl IntoIterator<Item = Result<Vec<u8>, String>>,
    regions: impl IntoIterator<Item = RegionLocation>,
) -> DirectUnaryQueryTransport<ScriptedClient, ScriptedLoader> {
    transport_with_cluster_id(calls, responses, regions, 9001)
}

pub fn transport_with_cluster_id(
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

pub fn batch_first_transport(
    calls: Rc<RefCell<Vec<ObservedCall>>>,
    responses: impl IntoIterator<Item = Result<Vec<u8>, String>>,
    regions: impl IntoIterator<Item = RegionLocation>,
    ready_immediately: impl IntoIterator<Item = bool>,
) -> DirectUnaryQueryTransport<ScriptedClient, ScriptedLoader> {
    batch_first_transport_with_config(
        calls,
        responses,
        regions,
        ready_immediately,
        Rc::new(RefCell::new(Vec::new())),
        DirectUnaryRuntimeConfig::default(),
    )
}

pub fn batch_first_transport_with_config(
    calls: Rc<RefCell<Vec<ObservedCall>>>,
    responses: impl IntoIterator<Item = Result<Vec<u8>, String>>,
    regions: impl IntoIterator<Item = RegionLocation>,
    ready_immediately: impl IntoIterator<Item = bool>,
    loader_calls: Rc<RefCell<Vec<Vec<u8>>>>,
    config: DirectUnaryRuntimeConfig,
) -> DirectUnaryQueryTransport<ScriptedClient, ScriptedLoader> {
    DirectUnaryQueryTransport::new_injected_batch_first(
        ScriptedClient {
            calls,
            responses: responses
                .into_iter()
                .map(|response| response.map_err(DirectUnaryClientError::InvalidRequest))
                .collect(),
            events: Rc::new(RefCell::new(Vec::new())),
            liveness: RefCell::new(VecDeque::new()),
            batch_errors: RefCell::new(VecDeque::new()),
            batch_ready_immediately: RefCell::new(ready_immediately.into_iter().collect()),
            batch_completion_gate: None,
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 9001,
            calls: loader_calls,
            regions: regions.into_iter().collect(),
        }),
        config,
        tidb_txnkv::lock::FixedTimestampSource::new(1 << 18),
    )
    .unwrap()
}

pub fn transport_with_loader_calls(
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

pub fn transport_with_loader_calls_and_config(
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
            batch_ready_immediately: RefCell::new(VecDeque::new()),
            batch_completion_gate: None,
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

pub fn transport_with_transport_failures(
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
            batch_ready_immediately: RefCell::new(VecDeque::new()),
            batch_completion_gate: None,
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

pub fn select_result(
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
