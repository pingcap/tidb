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

use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{
    InjectedQueryRuntime, KvRequestBuilder, QueryDispatch, QueryOperation, QueryResultContext,
    QueryRuntimeError, QueryTransport, ResponseChannel, ResponseRuntimeStats, SelectInput,
    StoreType, TransportBinding, TransportRequest, WarningCollector, ANALYZE_RESULT_LABEL,
    CHECKSUM_RESULT_LABEL, DAG_RESULT_LABEL, GENERAL_SQL_TYPE, INTERNAL_SQL_TYPE,
    INTERNAL_TXN_STATS_SOURCE,
};

fn transport_request(metadata: tidb_distsql::KvRequestMetadata) -> TransportRequest {
    TransportRequest::new(
        metadata,
        std::sync::Arc::new(tidb_distsql::CancelHandle::default()),
    )
}

#[derive(Default)]
struct ScriptedTransport {
    responses: VecDeque<Result<Option<ResponseChannel<Vec<u8>>>, String>>,
    dispatches: Vec<QueryDispatch>,
    request_was_bound: bool,
    request_sources: Vec<tidb_distsql::RequestSource>,
}

impl ScriptedTransport {
    fn returning(response: ResponseChannel<Vec<u8>>) -> Self {
        Self {
            responses: VecDeque::from([Ok(Some(response))]),
            dispatches: Vec::new(),
            request_was_bound: false,
            request_sources: Vec::new(),
        }
    }
}

impl QueryTransport for ScriptedTransport {
    type Response = ResponseChannel<Vec<u8>>;

    fn send(
        &mut self,
        request: &TransportRequest,
        dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        self.dispatches.push(dispatch.clone());
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
    closed: Rc<Cell<bool>>,
}

impl QueryResponse for TrackingResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        Ok(self.subsets.pop_front())
    }

    fn close(&mut self) {
        self.closed.set(true);
        self.subsets.clear();
    }
}

struct TrackingTransport(Option<TrackingResponse>);

impl QueryTransport for TrackingTransport {
    type Response = TrackingResponse;

    fn send(
        &mut self,
        _request: &TransportRequest,
        _dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        Ok(self.0.take())
    }
}

fn response_with(data: impl IntoIterator<Item = Vec<u8>>) -> ResponseChannel<Vec<u8>> {
    let mut source = ResponseChannel::new();
    for subset in data {
        source.push_result(subset).unwrap();
    }
    source.finish().unwrap();
    source
}

fn empty_response() -> ResponseChannel<Vec<u8>> {
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
    let dispatch = &transport.dispatches[0];
    assert_eq!(dispatch.operation, QueryOperation::Select);
    assert_eq!(dispatch.result.label, DAG_RESULT_LABEL);
    assert_eq!(dispatch.result.sql_type, Some(GENERAL_SQL_TYPE));
    assert_eq!(dispatch.result.store_type, StoreType::TiFlash);
    assert_eq!(dispatch.result.row_len, 2);
    assert!(dispatch.result.mem_tracker_bound);
    assert!(dispatch.result.paging);
    assert_eq!(dispatch.result.dist_sql_concurrency, 7);
    assert_eq!(iter.result_metadata(), Some(&dispatch.result));
}

#[test]
fn select_with_runtime_stats_keeps_plan_identity_on_the_live_iterator() {
    // pkg/distsql/distsql_test.go:82 TestSelectWithRuntimeStats
    // pkg/distsql/distsql_test.go:106 TestSelectResultRuntimeStats
    let request = request(StoreType::TiKv);
    let mut source = ResponseChannel::new();
    source
        .push_result_with_runtime(
            Vec::new(),
            ResponseRuntimeStats {
                callee_address: "tikv-1".to_owned(),
                request_rpc_stats_present: false,
                backoff_sleep_ns: vec![("regionMiss".to_owned(), 9)],
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
            vec![1, 2, 3],
            4,
            true,
        )
        .expect("runtime-stat response");
    let mut iter = result.into_select_iter(Vec::new());
    assert!(iter.next_row().unwrap().is_none());
    let transport = runtime.into_transport();
    let dispatch = &transport.dispatches[0];
    assert_eq!(dispatch.operation, QueryOperation::SelectWithRuntimeStats);
    assert_eq!(dispatch.result.cop_plan_ids, vec![1, 2, 3]);
    assert_eq!(dispatch.result.root_plan_id, Some(4));
    assert_eq!(iter.result_metadata(), Some(&dispatch.result));
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
    assert_eq!(result.next_raw().unwrap(), Some(first));
    assert_eq!(result.next_raw().unwrap(), Some(second));
    assert_eq!(result.next_raw().unwrap(), None);
    assert!(result.is_closed());
    result.close();
    let transport = runtime.into_transport();
    let dispatch = &transport.dispatches[0];
    assert_eq!(dispatch.operation, QueryOperation::Analyze);
    assert_eq!(dispatch.result.label, ANALYZE_RESULT_LABEL);
    assert_eq!(dispatch.result.sql_type, Some(INTERNAL_SQL_TYPE));
    let source = dispatch.request_source_override.as_ref().unwrap();
    assert!(source.internal);
    assert_eq!(source.source_type, INTERNAL_TXN_STATS_SOURCE);
    assert_eq!(transport.request_sources[0], *source);
}

#[test]
fn checksum_preserves_general_result_metadata() {
    // pkg/distsql/distsql_test.go:179 TestChecksum
    let request = request(StoreType::TiFlash);
    let raw = vec![0x08, 0x10, 0x18];
    let mut runtime =
        InjectedQueryRuntime::new(ScriptedTransport::returning(response_with([raw.clone()])));
    let mut result = runtime.checksum(&request).expect("checksum response");
    assert_eq!(result.next_raw().unwrap(), Some(raw));
    assert_eq!(result.next_raw().unwrap(), None);
    let transport = runtime.into_transport();
    let dispatch = &transport.dispatches[0];
    assert_eq!(dispatch.operation, QueryOperation::Checksum);
    assert_eq!(dispatch.result.label, CHECKSUM_RESULT_LABEL);
    assert_eq!(dispatch.result.sql_type, Some(GENERAL_SQL_TYPE));
    assert_eq!(dispatch.result.store_type, StoreType::TiFlash);
    assert!(dispatch.request_source_override.is_none());
}

#[test]
fn query_result_has_one_close_owner() {
    let closed = Rc::new(Cell::new(false));
    let response = TrackingResponse {
        subsets: VecDeque::from([QueryResultSubset {
            data: vec![1, 2, 3],
            runtime: None,
        }]),
        closed: Rc::clone(&closed),
    };
    let mut runtime = InjectedQueryRuntime::new(TrackingTransport(Some(response)));
    let mut result = runtime
        .checksum(&request(StoreType::TiKv))
        .expect("raw checksum response");
    assert_eq!(result.next_raw().unwrap(), Some(vec![1, 2, 3]));
    assert!(!closed.get());
    result.close();
    assert!(closed.get());
    assert!(result.is_closed());
    assert_eq!(result.next_raw().unwrap(), None);
}

#[test]
fn raw_then_select_conversion_consumes_each_subset_once() {
    let closed = Rc::new(Cell::new(false));
    let response = TrackingResponse {
        subsets: VecDeque::from([
            QueryResultSubset {
                data: vec![0xff, 0x00],
                runtime: None,
            },
            QueryResultSubset {
                data: Vec::new(),
                runtime: None,
            },
        ]),
        closed: Rc::clone(&closed),
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
    assert!(closed.get());
}

#[test]
fn nil_transport_response_is_not_reinterpreted_as_empty_results() {
    let request = request(StoreType::TiKv);
    let transport = ScriptedTransport {
        responses: VecDeque::from([Ok(None)]),
        dispatches: Vec::new(),
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
    assert!(runtime.into_transport().dispatches.is_empty());
}

#[test]
fn transport_error_remains_the_first_error() {
    let request = request(StoreType::TiKv);
    let transport = ScriptedTransport {
        responses: VecDeque::from([Err("region request failed".to_owned())]),
        dispatches: Vec::new(),
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
