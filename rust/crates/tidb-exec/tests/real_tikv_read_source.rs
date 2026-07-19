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

use prost::Message;
use tidb_datatype::Datum;
use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{
    CopPagingState, QueryDispatch, QueryOperation, QueryTransport, RequestKeyRange, RequestType,
    TimestampSource, TransportRequest,
};
use tidb_exec::real_tikv_read::{RealTiKvReadEngine, RealTiKvReadError};
use tidb_planner::read_only_scan::{
    ConfiguredTable, ReadOnlyScanError, UnsupportedReadOnlyFeature,
};
use tidb_proto::tipb::{Chunk, DagRequest, SelectResponse};

#[derive(Clone, Debug)]
struct ScriptedTimestampSource {
    values: Rc<RefCell<VecDeque<Result<u64, String>>>>,
    calls: Rc<Cell<usize>>,
}

impl ScriptedTimestampSource {
    fn new(values: impl IntoIterator<Item = u64>) -> Self {
        Self {
            values: Rc::new(RefCell::new(
                values.into_iter().map(Ok).collect::<VecDeque<_>>(),
            )),
            calls: Rc::new(Cell::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.get()
    }
}

impl TimestampSource for ScriptedTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        self.calls.set(self.calls.get() + 1);
        self.values
            .borrow_mut()
            .pop_front()
            .expect("one scripted timestamp per admitted query")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestObservation {
    start_ts: u64,
    request_type: RequestType,
    keep_order: bool,
    cop_paging_admission: Result<(), &'static str>,
    data: Vec<u8>,
    ranges: Vec<RequestKeyRange>,
    operation: QueryOperation,
}

#[derive(Default)]
struct SharedTransportState {
    sends: Cell<usize>,
    requests: RefCell<Vec<RequestObservation>>,
}

struct ScriptedResponse {
    subsets: VecDeque<QueryResultSubset>,
    next_count: Rc<Cell<usize>>,
    close_count: Rc<Cell<usize>>,
}

impl QueryResponse for ScriptedResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.next_count.set(self.next_count.get() + 1);
        Ok(self.subsets.pop_front())
    }

    fn close(&mut self) {
        self.close_count.set(self.close_count.get() + 1);
        self.subsets.clear();
    }
}

struct ScriptedTransport {
    responses: VecDeque<ScriptedResponse>,
    state: Rc<SharedTransportState>,
}

impl QueryTransport for ScriptedTransport {
    type Response = ScriptedResponse;

    fn send(
        &mut self,
        request: &TransportRequest,
        dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        assert!(request.is_bound());
        let metadata = request.metadata();
        let ranges = metadata
            .key_ranges
            .as_ref()
            .expect("read request carries table ranges");
        assert!(ranges.is_non_partitioned());
        let [ranges] = ranges.partitions.as_slice() else {
            panic!("one non-partitioned range group");
        };

        self.state.sends.set(self.state.sends.get() + 1);
        self.state.requests.borrow_mut().push(RequestObservation {
            start_ts: metadata.start_ts,
            request_type: metadata.request_type,
            keep_order: metadata.keep_order,
            cop_paging_admission: CopPagingState::validate_read_request(metadata)
                .map_err(|error| error.kind()),
            data: metadata.data.clone().expect("DAG request bytes"),
            ranges: ranges.clone(),
            operation: dispatch.operation,
        });
        Ok(Some(
            self.responses
                .pop_front()
                .expect("one scripted response per send"),
        ))
    }
}

fn configured_table() -> ConfiguredTable {
    ConfiguredTable::new("test", "accounts", 42, "id", 7)
}

fn encoded_rows(values: &[i64]) -> Vec<u8> {
    let mut rows_data = Vec::with_capacity(values.len() * 2);
    for value in values {
        assert!((0..64).contains(value));
        rows_data.extend_from_slice(&[8, u8::try_from(value * 2).unwrap()]);
    }
    SelectResponse {
        chunks: vec![Chunk {
            rows_data: Some(rows_data),
            rows_meta: Vec::new(),
        }],
        ..SelectResponse::default()
    }
    .encode_to_vec()
}

fn response(
    values: &[i64],
    next_count: Rc<Cell<usize>>,
    close_count: Rc<Cell<usize>>,
) -> ScriptedResponse {
    ScriptedResponse {
        subsets: VecDeque::from([QueryResultSubset {
            data: encoded_rows(values),
            runtime: None,
        }]),
        next_count,
        close_count,
    }
}

fn transport(
    responses: impl IntoIterator<Item = ScriptedResponse>,
    state: Rc<SharedTransportState>,
) -> ScriptedTransport {
    ScriptedTransport {
        responses: responses.into_iter().collect(),
        state,
    }
}

#[test]
fn exact_select_builds_one_timestamped_table_request_and_decodes_lazily() {
    let timestamps = ScriptedTimestampSource::new([4_242]);
    let state = Rc::new(SharedTransportState::default());
    let next_count = Rc::new(Cell::new(0));
    let close_count = Rc::new(Cell::new(0));
    let scripted_response = response(&[21], Rc::clone(&next_count), Rc::clone(&close_count));
    let mut engine = RealTiKvReadEngine::new(
        configured_table(),
        transport([scripted_response], Rc::clone(&state)),
        timestamps.clone(),
    );

    let query = engine
        .execute("SELECT id FROM test.accounts")
        .expect("the exact milestone query must reach the transport");
    assert_eq!(query.snapshot_ts(), 4_242);
    assert_eq!(query.table_id(), 42);
    assert_eq!(engine.last_snapshot_ts(), Some(4_242));
    assert_eq!(timestamps.calls(), 1);
    assert_eq!(state.sends.get(), 1);
    assert_eq!(next_count.get(), 0, "execute must not pull the response");
    assert_eq!(close_count.get(), 0, "the returned query owns the response");

    let requests = state.requests.borrow();
    let [request] = requests.as_slice() else {
        panic!("exactly one request must be sent");
    };
    assert_eq!(request.start_ts, 4_242);
    assert_eq!(request.request_type, RequestType::Dag);
    assert!(
        request.keep_order,
        "the production response runtime requires ordered publication"
    );
    assert_eq!(
        request.cop_paging_admission,
        Ok(()),
        "the exact request handed to the transport must pass production admission"
    );
    assert_eq!(request.operation, QueryOperation::SelectWithRuntimeStats);
    assert_eq!(
        request.ranges,
        [RequestKeyRange {
            start_key: vec![b't', 0x80, 0, 0, 0, 0, 0, 0, 42, b'_', b'r', 0, 0, 0, 0, 0, 0, 0, 0,],
            end_key: vec![
                b't', 0x80, 0, 0, 0, 0, 0, 0, 42, b'_', b'r', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0xff, 0xff, 0,
            ],
        }]
    );
    let dag = DagRequest::decode(request.data.as_slice()).expect("request data is a TiDB DAG");
    assert_eq!(dag.executors.len(), 1);
    assert_eq!(dag.output_offsets, [0]);
    assert!(dag.executors[0].tbl_scan.is_some());
    drop(requests);

    let mut record_set = query.into_record_set();
    assert_eq!(record_set.columns().len(), 1);
    assert_eq!(record_set.columns()[0].name, "id");
    assert_eq!(next_count.get(), 0, "ownership transfer remains lazy");
    assert_eq!(
        record_set.next_batch(1).unwrap(),
        vec![vec![Datum::Int(21)]]
    );
    assert_eq!(next_count.get(), 1);
    assert_eq!(close_count.get(), 0);
    record_set.close().unwrap();
    assert_eq!(close_count.get(), 1);
    record_set.close().unwrap();
    assert_eq!(close_count.get(), 1, "close is idempotent");
}

#[test]
fn unsupported_where_and_write_fail_before_tso_or_send() {
    let timestamps = ScriptedTimestampSource::new([99]);
    let state = Rc::new(SharedTransportState::default());
    let mut engine = RealTiKvReadEngine::new(
        configured_table(),
        transport([], Rc::clone(&state)),
        timestamps.clone(),
    );

    assert!(matches!(
        engine.execute("SELECT id FROM accounts WHERE id = 1"),
        Err(RealTiKvReadError::Plan(ReadOnlyScanError::Unsupported(
            UnsupportedReadOnlyFeature::Predicate
        )))
    ));
    assert!(matches!(
        engine.execute("UPDATE accounts SET id = 2"),
        Err(RealTiKvReadError::Plan(ReadOnlyScanError::Unsupported(
            UnsupportedReadOnlyFeature::WriteOrNonQueryStatement
        )))
    ));
    assert_eq!(timestamps.calls(), 0);
    assert_eq!(state.sends.get(), 0);
    assert!(state.requests.borrow().is_empty());
    assert_eq!(engine.last_snapshot_ts(), None);
}

#[test]
fn zero_timestamp_fails_before_send() {
    let timestamps = ScriptedTimestampSource::new([0]);
    let state = Rc::new(SharedTransportState::default());
    let mut engine = RealTiKvReadEngine::new(
        configured_table(),
        transport([], Rc::clone(&state)),
        timestamps.clone(),
    );

    let error = match engine.execute("SELECT id FROM accounts") {
        Ok(_) => panic!("zero is not a valid PD snapshot timestamp"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "TiKV query failed: PD returned a zero snapshot timestamp"
    );
    assert_eq!(timestamps.calls(), 1);
    assert_eq!(state.sends.get(), 0);
    assert!(state.requests.borrow().is_empty());
    assert_eq!(engine.last_snapshot_ts(), None);
}

#[test]
fn one_transport_is_retained_across_two_queries() {
    let timestamps = ScriptedTimestampSource::new([11, 12]);
    let state = Rc::new(SharedTransportState::default());
    let first_close = Rc::new(Cell::new(0));
    let second_close = Rc::new(Cell::new(0));
    let mut engine = RealTiKvReadEngine::new(
        configured_table(),
        transport(
            [
                response(&[1], Rc::new(Cell::new(0)), Rc::clone(&first_close)),
                response(&[2], Rc::new(Cell::new(0)), Rc::clone(&second_close)),
            ],
            Rc::clone(&state),
        ),
        timestamps.clone(),
    );

    let first = engine.execute("SELECT id FROM accounts").unwrap();
    let second = engine.execute("SELECT id FROM accounts").unwrap();
    assert_eq!(timestamps.calls(), 2);
    assert_eq!(state.sends.get(), 2);
    assert_eq!(
        state
            .requests
            .borrow()
            .iter()
            .map(|request| request.start_ts)
            .collect::<Vec<_>>(),
        [11, 12]
    );
    assert_eq!(engine.last_snapshot_ts(), Some(12));

    let mut first = first.into_record_set();
    let mut second = second.into_record_set();
    assert_eq!(first.next_batch(1).unwrap(), vec![vec![Datum::Int(1)]]);
    assert_eq!(second.next_batch(1).unwrap(), vec![vec![Datum::Int(2)]]);
    first.close().unwrap();
    second.close().unwrap();
    assert_eq!(first_close.get(), 1);
    assert_eq!(second_close.get(), 1);
}
