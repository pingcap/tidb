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

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use prost::Message;
use tidb_datatype::FieldType;
use tidb_distsql::{
    DirectUnaryClient, DirectUnaryClientError, DirectUnaryQueryTransport, DirectUnaryRequest,
    DirectUnaryResponse, DirectUnaryRuntimeConfig, DirectUnaryTransportError, InjectedQueryRuntime,
    KvRequestMetadata, QueryResultContext, RequestKeyRange, RequestKeyRanges, RequestType,
    SelectInput, StoreType, TransportRequest, WarningCollector,
};
use tidb_proto::{
    CoprocessorExecDetailsV2, CoprocessorKeyRange, CoprocessorRequest, CoprocessorResponse,
    CoprocessorScanDetailV2, RegionError,
};
use tidb_txnkv::region::{
    Peer, PeerRole, RegionCache, RegionLoadError, RegionLoader, RegionLocation, RegionRouteError,
    RegionVerId, Store,
};

const OBSERVATION_TIME: Duration = Duration::from_secs(1_000);

fn observation_time() -> Duration {
    OBSERVATION_TIME
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedCall {
    address: String,
    timeout: Duration,
    region_id: u64,
    data: Vec<u8>,
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

struct ScriptedClient {
    calls: Rc<RefCell<Vec<ObservedCall>>>,
    responses: VecDeque<Result<Vec<u8>, String>>,
}

impl DirectUnaryClient for ScriptedClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        let wire = CoprocessorRequest::decode(request.encoded_request.as_slice()).unwrap();
        assert!(wire.context.is_none());
        let epoch = request.context.region_epoch.as_ref().unwrap();
        let peer = request.context.peer.as_ref().unwrap();
        self.calls.borrow_mut().push(ObservedCall {
            address: address.to_owned(),
            timeout,
            region_id: request.context.region_id,
            data: wire.data,
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
            replica_read: request.context.replica_read,
            stale_read: request.context.stale_read,
        });
        self.responses
            .pop_front()
            .expect("one scripted response per client call")
            .map(|encoded_response| DirectUnaryResponse { encoded_response })
            .map_err(DirectUnaryClientError::InvalidRequest)
    }

    fn close_address(&mut self, _address: &str) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        Ok(())
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
    }
}

fn response(data: &[u8]) -> Vec<u8> {
    CoprocessorResponse {
        data: data.to_vec(),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec()
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
    DirectUnaryQueryTransport::new(
        ScriptedClient {
            calls,
            responses: responses.into_iter().collect(),
        },
        RegionCache::new(ScriptedLoader {
            cluster_id,
            calls: loader_calls,
            regions: regions.into_iter().collect(),
        }),
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(60),
            seed_read_bytes: 4096,
            observation_time,
            ..DirectUnaryRuntimeConfig::default()
        },
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
    let request = TransportRequest::new(metadata("a", "z"));
    let mut result = select_result(&mut runtime, &request);

    assert!(calls.borrow().is_empty(), "send must stay response-lazy");
    assert_eq!(result.next_raw().unwrap(), Some(b"left".to_vec()));
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(result.next_raw().unwrap(), Some(b"right".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);

    assert_eq!(
        calls.borrow().as_slice(),
        [
            ObservedCall {
                address: "tikv-1:20160".to_owned(),
                timeout: Duration::from_millis(777),
                region_id: 1,
                data: b"dag-read".to_vec(),
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
                replica_read: false,
                stale_read: false,
            },
            ObservedCall {
                address: "tikv-2:20160".to_owned(),
                timeout: Duration::from_millis(777),
                region_id: 2,
                data: b"dag-read".to_vec(),
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
                replica_read: false,
                stale_read: false,
            },
        ]
    );
}

#[test]
fn pd_peer_role_witness_and_cluster_fields_have_one_context_authority() {
    for (role, encoded, witness) in [
        (PeerRole::Voter, 0, false),
        (PeerRole::Learner, 1, true),
        (PeerRole::IncomingVoter, 2, false),
        (PeerRole::DemotingVoter, 3, true),
    ] {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let mut candidate = location(7, "a", "z", "tikv-7:20160");
        candidate.peers[0].role = role;
        candidate.peers[0].is_witness = witness;
        let mut runtime = InjectedQueryRuntime::new(transport(
            Rc::clone(&calls),
            [Ok(response(b"ok"))],
            [candidate],
        ));
        let mut result = select_result(&mut runtime, &TransportRequest::new(metadata("a", "z")));
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
        assert_eq!(calls[0].is_witness, witness);
        assert_eq!(calls[0].task_id, 29);
        assert_eq!(calls[0].request_source, "internal_ddl");
        assert!(calls[0].not_fill_cache);
        assert!(!calls[0].replica_read);
        assert!(!calls[0].stale_read);
    }
}

#[test]
fn region_error_invalidates_exact_version_for_next_query_without_same_query_retry() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let first = location(1, "a", "z", "tikv-old:20160");
    let mut replacement = location(1, "a", "z", "tikv-new:20160");
    replacement.region = RegionVerId::new(1, 1, 3);
    let region_error = CoprocessorResponse {
        region_error: Some(RegionError {
            message: "epoch not match".to_owned(),
            ..RegionError::default()
        }),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec();
    let transport = transport_with_loader_calls(
        Rc::clone(&calls),
        [Ok(region_error), Ok(response(b"fresh"))],
        [first, replacement],
        9001,
        Rc::clone(&loader_calls),
    );
    let mut runtime = InjectedQueryRuntime::new(transport);
    let request = TransportRequest::new(metadata("a", "z"));

    let mut stale = select_result(&mut runtime, &request);
    let error = stale.next_raw().unwrap_err().to_string();
    assert!(error.contains("region_error"), "{error}");
    assert_eq!(calls.borrow().len(), 1, "no same-query retry is allowed");

    let mut fresh = select_result(&mut runtime, &request);
    assert_eq!(fresh.next_raw().unwrap(), Some(b"fresh".to_vec()));
    assert_eq!(
        loader_calls.borrow().as_slice(),
        [b"a".to_vec(), b"a".to_vec()],
        "the exact invalidation must force the next query to reload"
    );
    assert_eq!(calls.borrow()[1].address, "tikv-new:20160");
    assert_eq!(calls.borrow()[1].version, 3);
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
    let mut result = select_result(&mut runtime, &TransportRequest::new(metadata));

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
    let mut result = select_result(&mut runtime, &TransportRequest::new(metadata));

    assert_eq!(result.next_raw().unwrap(), Some(b"page-one".to_vec()));
    assert_eq!(result.next_raw().unwrap(), Some(b"page-two".to_vec()));
    assert_eq!(result.next_raw().unwrap(), None);
    let calls = calls.borrow();
    assert_eq!(calls[0].predicted_read_bytes, 4096);
    assert_eq!(calls[1].predicted_read_bytes, 1_000_000);
}

#[test]
fn close_before_pull_stops_every_unsent_attempt() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        [Ok(response(b"never"))],
        [location(1, "a", "z", "tikv-1:20160")],
    ));
    let mut result = select_result(&mut runtime, &TransportRequest::new(metadata("a", "z")));
    result.close();
    result.close();
    assert_eq!(result.next_raw().unwrap(), None);
    assert!(calls.borrow().is_empty());
}

#[test]
fn retry_responses_and_client_or_decode_failures_are_terminal() {
    let cases = [
        (
            Ok(CoprocessorResponse {
                region_error: Some(RegionError {
                    message: "not leader".to_owned(),
                    ..RegionError::default()
                }),
                ..CoprocessorResponse::default()
            }
            .encode_to_vec()),
            "region_error",
        ),
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
        let mut result = select_result(&mut runtime, &TransportRequest::new(metadata("a", "z")));
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
    let missing_cluster = DirectUnaryQueryTransport::new(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::new(),
        },
        RegionCache::new(ScriptedLoader {
            cluster_id: 0,
            calls: Rc::new(RefCell::new(Vec::new())),
            regions: VecDeque::new(),
        }),
        DirectUnaryRuntimeConfig::default(),
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
    let error = runtime
        .select_with_runtime_stats(
            &TransportRequest::new(metadata("a", "z")),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            Vec::new(),
            0,
            false,
        )
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("MissingAddress(202)"), "{error}");
    assert!(calls.borrow().is_empty());

    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        std::iter::empty(),
        std::iter::empty(),
    ));
    let error = runtime
        .select_with_runtime_stats(
            &TransportRequest::new(metadata("a", "z")),
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
            &TransportRequest::new(metadata("a", "z")),
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
fn unsupported_replica_policy_fails_before_pd_or_tikv() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader_calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport_with_loader_calls(
        Rc::clone(&calls),
        std::iter::empty(),
        [location(1, "a", "z", "one")],
        9001,
        Rc::clone(&loader_calls),
    ));
    let mut follower = metadata("a", "z");
    follower.session.replica_read = tidb_distsql::ReplicaReadType::Follower;

    let error = runtime
        .select_with_runtime_stats(
            &TransportRequest::new(follower),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            Vec::new(),
            0,
            false,
        )
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("UnsupportedReadPolicy"), "{error}");
    assert!(calls.borrow().is_empty());
    assert!(loader_calls.borrow().is_empty());
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
                &TransportRequest::new(invalid),
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
