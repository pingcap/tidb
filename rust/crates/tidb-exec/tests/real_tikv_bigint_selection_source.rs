// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
use tidb_exec::real_tikv_read::{RealTiKvPlanExecutorKind, RealTiKvReadSession};
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};
use tidb_proto::tipb::{Chunk, DagRequest, ExecType, ExprType, ScalarFuncSig, SelectResponse};

#[derive(Clone, Debug)]
struct ScriptedTimestampSource {
    values: Rc<RefCell<VecDeque<u64>>>,
    calls: Rc<Cell<usize>>,
}

impl ScriptedTimestampSource {
    fn new(values: impl IntoIterator<Item = u64>) -> Self {
        Self {
            values: Rc::new(RefCell::new(values.into_iter().collect())),
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
        Ok(self
            .values
            .borrow_mut()
            .pop_front()
            .expect("one scripted timestamp per admitted query"))
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
}

impl QueryResponse for ScriptedResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        Ok(self.subsets.pop_front())
    }

    fn close(&mut self) {
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
    ConfiguredTable::new(
        "test",
        "accounts",
        42,
        vec![
            ConfiguredColumn::clustered_primary_key("id", 7),
            ConfiguredColumn::stored_not_null("balance", 8),
        ],
    )
}

fn encode_signed_varint(output: &mut Vec<u8>, value: i64) {
    let mut unsigned = (value as u64) << 1;
    if value < 0 {
        unsigned = !unsigned;
    }
    while unsigned >= 0x80 {
        output.push((unsigned as u8) | 0x80);
        unsigned >>= 7;
    }
    output.push(unsigned as u8);
}

fn response_rows(rows: &[&[i64]]) -> ScriptedResponse {
    let mut rows_data = Vec::new();
    for row in rows {
        for value in *row {
            rows_data.push(8);
            encode_signed_varint(&mut rows_data, *value);
        }
    }
    let response = SelectResponse {
        chunks: vec![Chunk {
            rows_data: Some(rows_data),
            rows_meta: Vec::new(),
        }],
        ..SelectResponse::default()
    };
    ScriptedResponse {
        subsets: VecDeque::from([QueryResultSubset {
            data: response.encode_to_vec(),
            runtime: None,
        }]),
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

fn full_table_range() -> RequestKeyRange {
    RequestKeyRange {
        start_key: vec![
            b't', 0x80, 0, 0, 0, 0, 0, 0, 42, b'_', b'r', 0, 0, 0, 0, 0, 0, 0, 0,
        ],
        end_key: vec![
            b't', 0x80, 0, 0, 0, 0, 0, 0, 42, b'_', b'r', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0,
        ],
    }
}

#[test]
fn predicate_only_column_lowers_to_selection_without_leaking_into_result() {
    let timestamps = ScriptedTimestampSource::new([7_777]);
    let state = Rc::new(SharedTransportState::default());
    let mut session = RealTiKvReadSession::new(
        configured_table(),
        transport([response_rows(&[&[42]])], Rc::clone(&state)),
        timestamps.clone(),
    );

    let query = session
        .execute("SELECT id FROM test.accounts WHERE balance > 100 AND -7 <= balance")
        .expect("supported BIGINT predicates must reach the real transport boundary");
    assert_eq!(
        query.plan_evidence().executor_kinds(),
        [
            RealTiKvPlanExecutorKind::TableScan,
            RealTiKvPlanExecutorKind::Selection,
        ]
    );
    assert_eq!(query.plan_evidence().predicate_count(), 2);
    assert_eq!(query.plan_evidence().output_offsets(), [0]);
    assert_eq!(query.snapshot_ts(), 7_777);
    assert_eq!(timestamps.calls(), 1);
    assert_eq!(state.sends.get(), 1);

    let requests = state.requests.borrow();
    let [request] = requests.as_slice() else {
        panic!("exactly one predicate request must be sent");
    };
    assert_eq!(request.start_ts, 7_777);
    assert_eq!(request.request_type, RequestType::Dag);
    assert!(request.keep_order);
    assert_eq!(request.cop_paging_admission, Ok(()));
    assert_eq!(request.operation, QueryOperation::SelectWithRuntimeStats);
    assert_eq!(request.ranges, [full_table_range()]);

    let dag = DagRequest::decode(request.data.as_slice()).expect("request data is a TiDB DAG");
    assert_eq!(dag.output_offsets, [0]);
    assert_eq!(dag.executors.len(), 2);
    assert_eq!(dag.executors[0].tp, Some(ExecType::TypeTableScan as i32));
    let scan = dag.executors[0]
        .tbl_scan
        .as_ref()
        .expect("first executor is the configured table scan");
    assert_eq!(
        scan.columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        [Some(7), Some(8)]
    );

    let selection = &dag.executors[1];
    assert_eq!(selection.tp, Some(ExecType::TypeSelection as i32));
    let conditions = &selection
        .selection
        .as_ref()
        .expect("second executor is the pushed-down Selection")
        .conditions;
    assert_eq!(conditions.len(), 2);
    assert_eq!(conditions[0].tp, Some(ExprType::ScalarFunc as i32));
    assert_eq!(conditions[0].sig, Some(ScalarFuncSig::GtInt as i32));
    assert_eq!(conditions[1].tp, Some(ExprType::ScalarFunc as i32));
    assert_eq!(conditions[1].sig, Some(ScalarFuncSig::LeInt as i32));
    assert_eq!(conditions[1].children[0].tp, Some(ExprType::Int64 as i32));
    assert_eq!(
        conditions[1].children[1].tp,
        Some(ExprType::ColumnRef as i32)
    );
    drop(requests);

    let mut record_set = query.into_record_set();
    assert_eq!(record_set.columns().len(), 1);
    assert_eq!(record_set.columns()[0].name, "id");
    assert_eq!(
        record_set.next_batch(1).unwrap(),
        vec![vec![Datum::Int(42)]]
    );
    record_set.close().unwrap();
}

#[test]
fn selection_result_is_not_filtered_again_after_tikv() {
    let state = Rc::new(SharedTransportState::default());
    let mut session = RealTiKvReadSession::new(
        configured_table(),
        // Deliberately inconsistent with the predicate: this fixture models
        // already-executed TiKV output and proves Rust does not post-filter it.
        transport([response_rows(&[&[-2_048]])], Rc::clone(&state)),
        ScriptedTimestampSource::new([8_888]),
    );

    let query = session
        .execute("SELECT balance FROM accounts WHERE balance > 100")
        .expect("supported BIGINT Selection must be sent");
    assert_eq!(
        query.plan_evidence().executor_kinds(),
        [
            RealTiKvPlanExecutorKind::TableScan,
            RealTiKvPlanExecutorKind::Selection,
        ]
    );
    assert_eq!(query.plan_evidence().predicate_count(), 1);
    assert_eq!(query.plan_evidence().output_offsets(), [0]);

    let request = &state.requests.borrow()[0];
    let dag = DagRequest::decode(request.data.as_slice()).expect("request data is a TiDB DAG");
    assert_eq!(dag.executors.len(), 2);
    assert_eq!(dag.executors[1].tp, Some(ExecType::TypeSelection as i32));

    let mut record_set = query.into_record_set();
    assert_eq!(
        record_set.next_batch(1).unwrap(),
        vec![vec![Datum::Int(-2_048)]]
    );
    record_set.close().unwrap();
}
