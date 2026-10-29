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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use prost::Message;
use tidb_codec::encode_value_in_timezone;
use tidb_datatype::{parse_datetime, Datum, FieldType, FieldTypeCode, SessionTimeZone, TimeType};
use tidb_txnkv::QueryResponseError;
use tidb_txnkv::QueryResultSubset;
use tidb_distsql::InjectedQueryRuntime;
use tidb_distsql::KvRequestBuilder;
use tidb_copr::query_transport::QueryOperation;
use tidb_distsql::QueryResultContext;
use tidb_copr::query_transport::QueryRuntimeError;
use tidb_copr::query_transport::QueryTransport;
use tidb_copr::response_channel::ResponseChannel;
use tidb_distsql::SelectInput;
use tidb_distsql::ANALYZE_RESULT_LABEL;
use tidb_distsql::CHECKSUM_RESULT_LABEL;
use tidb_distsql::DAG_RESULT_LABEL;
use tidb_distsql::GENERAL_SQL_TYPE;
use tidb_distsql::INTERNAL_SQL_TYPE;
use tidb_distsql::INTERNAL_TXN_STATS_SOURCE;
use tidb_txnkv::ResponseRuntimeStats;
use tidb_txnkv::TransportBinding;
use tidb_txnkv::TransportRequest;
use tidb_context::WarningCollector;
use tidb_txnkv::StoreType;
use tidb_proto::tipb::SelectResponse;
use tidb_proto::{Chunk, EncodeType, KvrpcLockInfo};
use tidb_trxevents::wrap_cop_meet_lock;
use tidb_trxevents::CopMeetLock;
use tidb_trxevents::EventCallback;

fn transport_request(metadata: tidb_txnkv::KvRequestMetadata) -> TransportRequest {
    TransportRequest::new(
        metadata,
        std::sync::Arc::new(tidb_txnkv::CancelHandle::default()),
    )
}

#[derive(Default)]
struct ScriptedTransport {
    responses: VecDeque<Result<Option<ResponseChannel<prost::bytes::Bytes>>, String>>,
    operations: Vec<QueryOperation>,
    request_was_bound: bool,
    request_sources: Vec<tidb_txnkv::RequestSource>,
}

impl ScriptedTransport {
    fn returning(response: ResponseChannel<prost::bytes::Bytes>) -> Self {
        Self {
            responses: VecDeque::from([Ok(Some(response))]),
            operations: Vec::new(),
            request_was_bound: false,
            request_sources: Vec::new(),
        }
    }
}

impl QueryTransport for ScriptedTransport {
    type Response = ResponseChannel<prost::bytes::Bytes>;

    fn send(
        &mut self,
        request: &TransportRequest,
        operation: QueryOperation,
    ) -> Result<Option<Self::Response>, String> {
        self.operations.push(operation);
        self.request_was_bound = request.is_bound();
        self.request_sources
            .push(request.metadata().request_source.clone());
        self.responses
            .pop_front()
            .expect("one scripted result per send")
    }
}

struct TrackingResponse {
    subsets: VecDeque<QueryResultSubset>,
    closed: Arc<AtomicBool>,
    limiter_wait: tidb_txnkv::LimiterWaitStats,
}

impl tidb_txnkv::Response for TrackingResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        Ok(self.subsets.pop_front())
    }

    fn close(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.subsets.clear();
    }

    fn limiter_wait_stats(&self) -> tidb_txnkv::LimiterWaitStats {
        self.limiter_wait
    }
}


struct TrackingTransport(Option<TrackingResponse>);

impl QueryTransport for TrackingTransport {
    type Response = TrackingResponse;

    fn send(
        &mut self,
        _request: &TransportRequest,
        _operation: QueryOperation,
    ) -> Result<Option<Self::Response>, String> {
        Ok(self.0.take())
    }
}

fn response_with(data: impl IntoIterator<Item = Vec<u8>>) -> ResponseChannel<prost::bytes::Bytes> {
    let mut source = ResponseChannel::new();
    for subset in data {
        source.push_result(subset.into()).unwrap();
    }
    source.finish().unwrap();
    source
}

fn empty_response() -> ResponseChannel<prost::bytes::Bytes> {
    response_with(std::iter::empty())
}

fn field_types(count: usize) -> Vec<FieldType> {
    (0..count)
        .map(|_| FieldType::new(FieldTypeCode::Long))
        .collect()
}

fn request(store_type: StoreType) -> TransportRequest {
    let mut builder = KvRequestBuilder::new();
    builder.set_store_type(store_type);
    transport_request(builder.build().expect("built request"))
}

fn input() -> SelectInput {
    SelectInput {
        store_type: StoreType::TiKv,
        row_len: 2,
        mem_tracker_bound: true,
        paging_enabled: false,
        paging_size_bytes: 4096,
        dist_sql_concurrency: 15,
        ..SelectInput::default()
    }
}

#[test]
fn select_sends_the_built_request_and_returns_the_transport_iterator() {
    // pkg/distsql/distsql_test.go:42 TestSelectNormal
    // pkg/distsql/distsql_test.go:61 TestSelectMemTracker
    // pkg/distsql/distsql_test.go:73 TestSelectNormalChunkSize
    let mut builder = KvRequestBuilder::new();
    builder
        .set_store_type(StoreType::TiFlash)
        .set_paging(true)
        .set_concurrency(7);
    let request = transport_request(builder.build().expect("built request"));
    let mut runtime = InjectedQueryRuntime::new(ScriptedTransport::returning(empty_response()));
    let result = runtime
        .select(
            &request,
            input(),
            QueryResultContext::new(field_types(2), WarningCollector::new()),
        )
        .expect("select response");
    let mut iter = result.into_select_iter(Vec::new());
    assert!(iter.next_row().unwrap().is_none());
    let transport = runtime.into_transport();
    assert!(transport.request_was_bound);
    assert_eq!(transport.operations[0], QueryOperation::Select);
    let metadata = iter.result_metadata().expect("select metadata");
    assert_eq!(metadata.label, DAG_RESULT_LABEL);
    assert_eq!(metadata.sql_type, Some(GENERAL_SQL_TYPE));
    assert_eq!(metadata.store_type, StoreType::TiFlash);
    assert_eq!(metadata.row_len, 2);
    assert!(metadata.mem_tracker_bound);
    assert!(metadata.paging);
    assert_eq!(metadata.dist_sql_concurrency, 7);
}

#[test]
fn select_result_context_carries_the_statement_zone_into_default_decode() {
    let zone = SessionTimeZone::Fixed {
        name: "+08:00".to_owned(),
        offset_secs: 8 * 60 * 60,
    };
    let mut timestamp = parse_datetime("2026-08-12 14:30:00", &zone, true, false)
        .unwrap()
        .time;
    timestamp.set_kind(TimeType::Timestamp);
    let response = SelectResponse {
        encode_type: Some(EncodeType::TypeDefault as i32),
        chunks: vec![Chunk {
            rows_data: Some(
                encode_value_in_timezone(&zone, &[Datum::new_time(timestamp)])
                    .unwrap()
                    .into(),
            ),
            rows_meta: Vec::new(),
        }],
        ..SelectResponse::default()
    };
    let mut runtime = InjectedQueryRuntime::new(ScriptedTransport::returning(response_with([
        response.encode_to_vec(),
    ])));
    let result = runtime
        .select(
            &request(StoreType::TiKv),
            input(),
            QueryResultContext::new(
                vec![FieldType::new(FieldTypeCode::Timestamp)],
                WarningCollector::new(),
            )
            .with_time_zone(zone),
        )
        .unwrap();
    let mut iter = result.into_select_iter(Vec::new());

    assert_eq!(
        iter.next_row().unwrap().unwrap().row,
        vec![Datum::new_time(timestamp)]
    );
    assert!(iter.next_row().unwrap().is_none());
}

#[test]
fn select_with_runtime_stats_keeps_plan_identity_on_the_live_iterator() {
    // pkg/distsql/distsql_test.go:82 TestSelectWithRuntimeStats
    // pkg/distsql/distsql_test.go:106 TestSelectResultRuntimeStats
    let beyond_i32 = (i32::MAX as isize) + 1;
    let request = request(StoreType::TiKv);
    let mut source = ResponseChannel::new();
    source
        .push_result_with_runtime(
            prost::bytes::Bytes::new(),
            ResponseRuntimeStats {
                callee_address: "tikv-1".to_owned(),
                request_rpc_stats_present: false,
                backoff_sleep_ns: vec![("regionMiss".to_owned(), 9)],
            ..ResponseRuntimeStats::default()
        },
        )
        .unwrap();
    source.finish().unwrap();
    let mut runtime = InjectedQueryRuntime::new(ScriptedTransport::returning(source));
    let result = runtime
        .select_with_runtime_stats(
            &request,
            input(),
            QueryResultContext::new(field_types(2), WarningCollector::new()),
            vec![1, 2, beyond_i32],
            beyond_i32,
            true,
        )
        .expect("runtime-stat response");
    let mut iter = result.into_select_iter(Vec::new());
    assert!(iter.next_row().unwrap().is_none());
    let transport = runtime.into_transport();
    assert_eq!(transport.operations[0], QueryOperation::SelectWithRuntimeStats);
    let metadata = iter.result_metadata().expect("runtime-stats metadata");
    assert_eq!(metadata.cop_plan_ids, vec![1, 2, beyond_i32]);
    assert_eq!(metadata.root_plan_id, Some(beyond_i32));
    assert_eq!(iter.runtime_stats().backoff_sleep_ns("regionMiss"), 9);
}

#[test]
fn analyze_overrides_request_source_at_the_send_boundary() {
    // pkg/distsql/distsql_test.go:154 TestAnalyze
    let request = request(StoreType::TiKv);
    let first = vec![0xff, 0x00, 0x7f];
    let second = vec![0x01, 0x02];
    let mut runtime = InjectedQueryRuntime::new(ScriptedTransport::returning(response_with([
        first.clone(),
        second.clone(),
    ])));
    let mut result = runtime.analyze(&request, true).expect("analyze response");
    let metadata = result.result_metadata().clone();
    assert_eq!(result.next_raw().unwrap(), Some(first));
    assert_eq!(result.next_raw().unwrap(), Some(second));
    assert_eq!(result.next_raw().unwrap(), None);
    assert!(result.is_closed());
    result.close();
    let transport = runtime.into_transport();
    assert_eq!(transport.operations[0], QueryOperation::Analyze);
    assert_eq!(metadata.label, ANALYZE_RESULT_LABEL);
    assert_eq!(metadata.sql_type, Some(INTERNAL_SQL_TYPE));
    // The analyze request source overrides the request's own on the wire.
    let source = &transport.request_sources[0];
    assert!(source.internal);
    assert_eq!(source.source_type, INTERNAL_TXN_STATS_SOURCE);
}

#[test]
fn checksum_preserves_general_result_metadata() {
    // pkg/distsql/distsql_test.go:179 TestChecksum
    let request = request(StoreType::TiFlash);
    let raw = vec![0x08, 0x10, 0x18];
    let mut runtime =
        InjectedQueryRuntime::new(ScriptedTransport::returning(response_with([raw.clone()])));
    let mut result = runtime.checksum(&request).expect("checksum response");
    let metadata = result.result_metadata().clone();
    assert_eq!(result.next_raw().unwrap(), Some(raw));
    assert_eq!(result.next_raw().unwrap(), None);
    let transport = runtime.into_transport();
    assert_eq!(transport.operations[0], QueryOperation::Checksum);
    assert_eq!(metadata.label, CHECKSUM_RESULT_LABEL);
    assert_eq!(metadata.sql_type, Some(GENERAL_SQL_TYPE));
    assert_eq!(metadata.store_type, StoreType::TiFlash);
    // No override: the request goes out under its own source.
    assert_eq!(transport.request_sources[0], request.metadata().request_source);
}

#[test]
fn query_result_has_one_close_owner() {
    let closed = Arc::new(AtomicBool::new(false));
    let response = TrackingResponse {
        subsets: VecDeque::from([QueryResultSubset {
            data: vec![1, 2, 3].into(),
            runtime: None,
        }]),
        closed: Arc::clone(&closed),
        limiter_wait: tidb_txnkv::LimiterWaitStats::default(),
    };
    let mut runtime = InjectedQueryRuntime::new(TrackingTransport(Some(response)));
    let mut result = runtime
        .checksum(&request(StoreType::TiKv))
        .expect("raw checksum response");
    assert_eq!(result.next_raw().unwrap(), Some(vec![1, 2, 3]));
    assert!(!closed.load(Ordering::SeqCst));
    result.close();
    assert!(closed.load(Ordering::SeqCst));
    assert!(result.is_closed());
    assert_eq!(result.next_raw().unwrap(), None);
}

#[test]
fn select_client_options_deliver_typed_transaction_events() {
    struct EventTransport {
        callback: Option<EventCallback>,
        response: Option<ResponseChannel<prost::bytes::Bytes>>,
    }

    impl QueryTransport for EventTransport {
        type Response = ResponseChannel<prost::bytes::Bytes>;

        fn set_event_callback(&mut self, callback: Option<EventCallback>) {
            self.callback = callback;
        }

        fn send(
            &mut self,
            _request: &TransportRequest,
            _operation: QueryOperation,
        ) -> Result<Option<Self::Response>, String> {
            self.callback.as_ref().expect("send event callback")(wrap_cop_meet_lock(Some(
                Arc::new(CopMeetLock {
                    lock_info: Some(Arc::new(KvrpcLockInfo {
                        key: b"locked".to_vec(),
                        ..KvrpcLockInfo::default()
                    })),
                }),
            )));
            Ok(self.response.take())
        }
    }

    let observed = Arc::new(Mutex::new(Vec::new()));
    let callback_observation = Arc::clone(&observed);
    let callback: EventCallback = Arc::new(move |event| {
        callback_observation
            .lock()
            .expect("lock callback observation")
            .push(
                event
                    .get_cop_meet_lock()
                    .and_then(|event| event.lock_info.as_ref())
                    .expect("lock event")
                    .key
                    .clone(),
            );
    });
    let mut runtime = InjectedQueryRuntime::new(EventTransport {
        callback: None,
        response: Some(empty_response()),
    });
    let result = runtime
        .select(
            &request(StoreType::TiKv),
            input(),
            QueryResultContext::new(field_types(1), WarningCollector::new())
                .with_event_callback(callback),
        )
        .expect("select with event callback");
    drop(result);
    let transport = runtime.into_transport();
    assert!(
        transport.callback.is_none(),
        "send-scoped callback must be cleared"
    );
    assert_eq!(
        observed
            .lock()
            .expect("lock callback observation")
            .as_slice(),
        [b"locked".to_vec()]
    );
}

#[test]
fn raw_then_select_conversion_consumes_each_subset_once() {
    let closed = Arc::new(AtomicBool::new(false));
    let response = TrackingResponse {
        subsets: VecDeque::from([
            QueryResultSubset {
                data: vec![0xff, 0x00].into(),
                runtime: None,
            },
            QueryResultSubset {
                data: Vec::new().into(),
                runtime: None,
            },
        ]),
        closed: Arc::clone(&closed),
        limiter_wait: tidb_txnkv::LimiterWaitStats::default(),
    };
    let mut runtime = InjectedQueryRuntime::new(TrackingTransport(Some(response)));
    let mut result = runtime
        .select(
            &request(StoreType::TiKv),
            input(),
            QueryResultContext::new(field_types(2), WarningCollector::new()),
        )
        .expect("raw select response");

    assert_eq!(result.next_raw().unwrap(), Some(vec![0xff, 0x00]));
    let mut iter = result.into_select_iter(Vec::new());
    assert!(iter.next_row().unwrap().is_none());
    assert!(closed.load(Ordering::SeqCst));
}

#[test]
fn select_conversion_transfers_limiter_wait_stats_to_the_result_iterator() {
    let response = TrackingResponse {
        subsets: VecDeque::from([QueryResultSubset {
            data: Vec::new().into(),
            runtime: None,
        }]),
        closed: Arc::new(AtomicBool::new(false)),
        limiter_wait: tidb_txnkv::LimiterWaitStats {
            total_ns: 17,
            max_ns: 11,
        },
    };
    let mut runtime = InjectedQueryRuntime::new(TrackingTransport(Some(response)));
    let result = runtime
        .select(
            &request(StoreType::TiKv),
            input(),
            QueryResultContext::new(field_types(2), WarningCollector::new()),
        )
        .expect("select response");

    let iter = result.into_select_iter(Vec::new());
    assert_eq!(iter.runtime_stats().limiter_wait.total_ns, 17);
    assert_eq!(iter.runtime_stats().limiter_wait.max_ns, 11);
}

#[test]
fn nil_transport_response_is_not_reinterpreted_as_empty_results() {
    let request = request(StoreType::TiKv);
    let transport = ScriptedTransport {
        responses: VecDeque::from([Ok(None)]),
        operations: Vec::new(),
        request_was_bound: false,
        request_sources: Vec::new(),
    };
    let mut runtime = InjectedQueryRuntime::new(transport);
    assert!(matches!(
        runtime.select(
            &request,
            input(),
            QueryResultContext::new(field_types(2), WarningCollector::new()),
        ),
        Err(QueryRuntimeError::NilResponse)
    ));
}

#[test]
fn already_bound_request_never_reaches_a_second_transport() {
    let mut builder = KvRequestBuilder::new();
    let request = transport_request(builder.build().unwrap())
        .bind(TransportBinding::new())
        .unwrap();
    let mut runtime = InjectedQueryRuntime::new(ScriptedTransport::default());
    assert!(matches!(
        runtime.select(
            &request,
            input(),
            QueryResultContext::new(field_types(2), WarningCollector::new()),
        ),
        Err(QueryRuntimeError::Request(_))
    ));
    assert!(runtime.into_transport().operations.is_empty());
}

#[test]
fn transport_error_remains_the_first_error() {
    let request = request(StoreType::TiKv);
    let transport = ScriptedTransport {
        responses: VecDeque::from([Err("region request failed".to_owned())]),
        operations: Vec::new(),
        request_was_bound: false,
        request_sources: Vec::new(),
    };
    let mut runtime = InjectedQueryRuntime::new(transport);
    assert!(matches!(
        runtime.select(
            &request,
            input(),
            QueryResultContext::new(field_types(2), WarningCollector::new()),
        ),
        Err(QueryRuntimeError::Transport(message)) if message == "region request failed"
    ));
}

#[test]
fn registered_storage_error_survives_row_and_chunk_decoding() {
    struct Failure(Arc<AtomicBool>);
    impl tidb_txnkv::Response for Failure {
        fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
            Err(QueryResponseError::Sql {
                code: 9004,
                message: "[tikv:9004]Resolve lock timeout".to_owned(),
            })
        }

        fn close(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    for chunk in [false, true] {
        let closed = Arc::new(AtomicBool::new(false));
        let mut iter = tidb_distsql::select_response::SelectResponseIter::from_query_response(
            Box::new(Failure(Arc::clone(&closed))),
            field_types(1),
            Vec::new(),
            SessionTimeZone::utc(),
            WarningCollector::new(),
            tidb_distsql::select_result_metadata(input()),
            None,
        );
        let error = if chunk {
            iter.next_chunk_with_required_rows(1).unwrap_err()
        } else {
            iter.next_row().unwrap_err()
        };
        assert_eq!(
            error,
            tidb_copr::response_channel::ResponseChannelError::SelectResponse {
                code: 9004,
                message: "[tikv:9004]Resolve lock timeout".to_owned(),
            }
        );
        iter.close();
        assert!(closed.load(Ordering::SeqCst));
    }
}
