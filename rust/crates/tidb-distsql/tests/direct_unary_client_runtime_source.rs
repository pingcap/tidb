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
    KvRequestMetadata, QueryResultContext, RegionTaskEpoch, RegionTaskPeer, RegionTaskTopology,
    RequestKeyRange, RequestKeyRanges, RequestType, ResolvedRegionRoute, SelectInput, StoreType,
    TransportRequest, WarningCollector,
};
use tidb_proto::{
    CoprocessorExecDetailsV2, CoprocessorKeyRange, CoprocessorRequest, CoprocessorResponse,
    CoprocessorScanDetailV2, RegionError,
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
        self.calls.borrow_mut().push(ObservedCall {
            address: address.to_owned(),
            timeout,
            region_id: request.context.region_id,
            data: wire.data,
            predicted_read_bytes: request.predicted_read_bytes,
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
    metadata
}

fn route(region_id: u64, start: &str, end: &str, address: &str) -> ResolvedRegionRoute {
    ResolvedRegionRoute {
        topology: RegionTaskTopology {
            region_id,
            region_epoch: Some(RegionTaskEpoch {
                conf_ver: 1,
                version: 2,
            }),
            peer: Some(RegionTaskPeer {
                id: region_id + 100,
                store_id: region_id + 200,
                role: 0,
                is_witness: false,
            }),
            start_key: start.as_bytes().to_vec(),
            end_key: end.as_bytes().to_vec(),
            ..RegionTaskTopology::default()
        },
        address: address.to_owned(),
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
    routes: impl IntoIterator<Item = ResolvedRegionRoute>,
) -> DirectUnaryQueryTransport<ScriptedClient> {
    DirectUnaryQueryTransport::new(
        ScriptedClient {
            calls,
            responses: responses.into_iter().collect(),
        },
        routes,
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(60),
            seed_read_bytes: 4096,
            cluster_id: 9001,
            observation_time,
            ..DirectUnaryRuntimeConfig::default()
        },
    )
    .unwrap()
}

fn select_result(
    runtime: &mut InjectedQueryRuntime<DirectUnaryQueryTransport<ScriptedClient>>,
    request: &TransportRequest,
) -> tidb_distsql::query_runtime::QuerySelectResult<
    tidb_distsql::DirectUnaryQueryResponse<ScriptedClient>,
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
            route(1, "a", "m", "tikv-1:20160"),
            route(2, "m", "z", "tikv-2:20160"),
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
            },
            ObservedCall {
                address: "tikv-2:20160".to_owned(),
                timeout: Duration::from_millis(777),
                region_id: 2,
                data: b"dag-read".to_vec(),
                predicted_read_bytes: 4096,
            },
        ]
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
        [route(1, "a", "z", "tikv-1:20160")],
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
        [route(1, "a", "z", "tikv-1:20160")],
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
        [route(1, "a", "z", "tikv-1:20160")],
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
                route(1, "a", "m", "tikv-1:20160"),
                route(2, "m", "z", "tikv-2:20160"),
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
fn missing_duplicate_and_empty_routes_fail_before_client_dispatch() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let missing_cluster = DirectUnaryQueryTransport::new(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::new(),
        },
        [route(1, "a", "z", "one")],
        DirectUnaryRuntimeConfig::default(),
    )
    .err()
    .unwrap();
    assert_eq!(missing_cluster, DirectUnaryTransportError::MissingClusterId);

    let duplicate = DirectUnaryQueryTransport::new(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::new(),
        },
        [route(1, "a", "m", "one"), route(1, "m", "z", "duplicate")],
        DirectUnaryRuntimeConfig {
            cluster_id: 9001,
            ..DirectUnaryRuntimeConfig::default()
        },
    )
    .err()
    .unwrap();
    assert_eq!(duplicate, DirectUnaryTransportError::DuplicateRoute(1));

    let mut empty = route(2, "a", "z", "ignored");
    empty.address.clear();
    let missing_address = DirectUnaryQueryTransport::new(
        ScriptedClient {
            calls: Rc::clone(&calls),
            responses: VecDeque::new(),
        },
        [empty],
        DirectUnaryRuntimeConfig {
            cluster_id: 9001,
            ..DirectUnaryRuntimeConfig::default()
        },
    )
    .err()
    .unwrap();
    assert_eq!(
        missing_address,
        DirectUnaryTransportError::MissingAddress(2)
    );

    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        std::iter::empty(),
        [route(1, "a", "m", "one")],
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
    assert!(error.contains("no exact region route"), "{error}");
    assert!(calls.borrow().is_empty());
}

#[test]
fn unsupported_operation_fails_before_preparing_or_sending() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let mut runtime = InjectedQueryRuntime::new(transport(
        Rc::clone(&calls),
        std::iter::empty(),
        [route(1, "a", "z", "one")],
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
}
