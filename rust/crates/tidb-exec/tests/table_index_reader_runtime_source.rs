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

#[path = "direct_unary_table_index_reader_source.rs"]
mod direct_unary_table_index_reader_source;

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use prost::Message;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{
    KvRequestBuilder, QueryDispatch, QueryOperation, QueryTransport, StoreType, TransportRequest,
};
use tidb_exec::storage_reader::{
    ReaderKind, ReaderPlan, ReaderState, StorageReaderError, TableIndexReader,
};
use tidb_proto::tipb::{Chunk, SelectResponse};

#[derive(Default)]
struct SharedTransportState {
    sends: Cell<usize>,
    dispatches: RefCell<Vec<QueryDispatch>>,
}

struct TrackingResponse {
    rows: VecDeque<i64>,
    required_rows: Rc<RefCell<Vec<usize>>>,
    close_count: Rc<Cell<usize>>,
}

impl QueryResponse for TrackingResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.next_with_required_rows(usize::MAX)
    }

    fn next_with_required_rows(
        &mut self,
        required_rows: usize,
    ) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        if self.rows.is_empty() {
            return Ok(None);
        }
        self.required_rows.borrow_mut().push(required_rows);
        let take = required_rows.min(self.rows.len());
        let rows = self.rows.drain(..take).collect::<Vec<_>>();
        Ok(Some(QueryResultSubset {
            data: encoded_rows(&rows),
            runtime: None,
        }))
    }

    fn close(&mut self) {
        self.close_count.set(self.close_count.get() + 1);
        self.rows.clear();
    }
}

struct ScriptedTransport {
    responses: VecDeque<Result<Option<TrackingResponse>, String>>,
    state: Rc<SharedTransportState>,
}

impl QueryTransport for ScriptedTransport {
    type Response = TrackingResponse;

    fn send(
        &mut self,
        request: &TransportRequest,
        dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        assert!(request.is_bound());
        self.state.sends.set(self.state.sends.get() + 1);
        self.state.dispatches.borrow_mut().push(dispatch.clone());
        self.responses
            .pop_front()
            .expect("one scripted response per send")
    }
}

fn field_types() -> Vec<FieldType> {
    vec![FieldType::new(FieldTypeCode::Long)]
}

fn request() -> TransportRequest {
    let mut builder = KvRequestBuilder::new();
    builder.set_store_type(StoreType::TiKv);
    TransportRequest::new(builder.build().expect("built request"))
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

fn response(values: &[i64], close_count: Rc<Cell<usize>>) -> TrackingResponse {
    response_with_required_rows(values, close_count, Rc::new(RefCell::new(Vec::new())))
}

fn response_with_required_rows(
    values: &[i64],
    close_count: Rc<Cell<usize>>,
    required_rows: Rc<RefCell<Vec<usize>>>,
) -> TrackingResponse {
    TrackingResponse {
        rows: values.iter().copied().collect(),
        required_rows,
        close_count,
    }
}

fn response_rows(
    total_rows: usize,
    close_count: Rc<Cell<usize>>,
    required_rows: Rc<RefCell<Vec<usize>>>,
) -> TrackingResponse {
    response_with_required_rows(&vec![1; total_rows], close_count, required_rows)
}

fn transport(
    responses: impl IntoIterator<Item = Result<Option<TrackingResponse>, String>>,
    state: Rc<SharedTransportState>,
) -> ScriptedTransport {
    ScriptedTransport {
        responses: responses.into_iter().collect(),
        state,
    }
}

fn ints(rows: Vec<Vec<Datum>>) -> Vec<i64> {
    rows.into_iter()
        .map(|row| match row.as_slice() {
            [Datum::Int(value)] => *value,
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}

#[test]
fn table_and_index_reader_honor_every_required_rows_bound() {
    // pkg/executor/table_readers_required_rows_test.go:172 TestTableReaderRequiredRows
    // pkg/executor/table_readers_required_rows_test.go:225 TestIndexReaderRequiredRows
    for kind in [ReaderKind::Table, ReaderKind::Index] {
        for (total_rows, required_rows, expected_rows) in [
            (10, vec![1, 5, 3, 10], vec![1, 5, 3, 1]),
            (1_025, vec![1, 5, 3, 10, 1_024], vec![1, 5, 3, 10, 1_006]),
            (3_073, vec![3, 10, 1_024], vec![3, 10, 1_024]),
        ] {
            let state = Rc::new(SharedTransportState::default());
            let closed = Rc::new(Cell::new(0));
            let observed_required_rows = Rc::new(RefCell::new(Vec::new()));
            let plan = ReaderPlan::new(kind, vec![request()], field_types());
            let mut reader = TableIndexReader::new(
                plan,
                transport(
                    [Ok(Some(response_rows(
                        total_rows,
                        Rc::clone(&closed),
                        Rc::clone(&observed_required_rows),
                    )))],
                    state,
                ),
            );
            reader.open().unwrap();
            for (required, expected) in required_rows.iter().copied().zip(expected_rows) {
                assert_eq!(reader.next(required).unwrap().len(), expected);
            }
            assert_eq!(
                observed_required_rows.borrow().as_slice(),
                required_rows.as_slice()
            );
            reader.close();
            assert_eq!(closed.get(), 1);
            assert_eq!(reader.state(), ReaderState::Closed);
        }
    }
}

#[test]
fn open_transfers_each_response_once_and_consumes_requests_serially() {
    let state = Rc::new(SharedTransportState::default());
    let first_closed = Rc::new(Cell::new(0));
    let second_closed = Rc::new(Cell::new(0));
    let plan = ReaderPlan::new(ReaderKind::Table, vec![request(), request()], field_types())
        .with_runtime_stats(vec![7, 8], 9, true);
    let mut reader = TableIndexReader::new(
        plan,
        transport(
            [
                Ok(Some(response(&[1, 2], Rc::clone(&first_closed)))),
                Ok(Some(response(&[3, 4], Rc::clone(&second_closed)))),
            ],
            Rc::clone(&state),
        ),
    );

    reader.open().unwrap();
    assert_eq!(state.sends.get(), 2);
    assert!(state
        .dispatches
        .borrow()
        .iter()
        .all(|dispatch| dispatch.operation == QueryOperation::SelectWithRuntimeStats));
    assert_eq!(ints(reader.next(3).unwrap()), vec![1, 2, 3]);
    assert_eq!(ints(reader.next(3).unwrap()), vec![4]);
    assert!(reader.next(3).unwrap().is_empty());
    assert_eq!(first_closed.get(), 1);
    assert_eq!(second_closed.get(), 1);
    reader.close();
    reader.close();
    assert_eq!(first_closed.get(), 1);
    assert_eq!(second_closed.get(), 1);
}

#[test]
fn temporary_table_reader_is_structurally_a_zero_send_path() {
    // The five owned temporary_table_test.go anchors all share this bounded
    // invariant. Their DDL, transaction, point-get, index-lookup, and
    // UnionScan behavior remains outside this slice.
    for kind in [ReaderKind::Table, ReaderKind::Index] {
        let state = Rc::new(SharedTransportState::default());
        let mut reader = TableIndexReader::new(
            ReaderPlan::dummy(kind, field_types()),
            transport([], Rc::clone(&state)),
        );
        assert_eq!(reader.kind(), kind);
        reader.open().unwrap();
        assert_eq!(reader.kind(), kind);
        assert!(reader.next(32).unwrap().is_empty());
        assert_eq!(state.sends.get(), 0);
        reader.close();
        reader.close();
        assert_eq!(reader.kind(), kind);
        assert_eq!(state.sends.get(), 0);
    }
}

#[test]
fn close_before_drain_closes_every_opened_response_once() {
    let state = Rc::new(SharedTransportState::default());
    let first_closed = Rc::new(Cell::new(0));
    let second_closed = Rc::new(Cell::new(0));
    let plan = ReaderPlan::new(ReaderKind::Index, vec![request(), request()], field_types());
    let mut reader = TableIndexReader::new(
        plan,
        transport(
            [
                Ok(Some(response(&[1], Rc::clone(&first_closed)))),
                Ok(Some(response(&[2], Rc::clone(&second_closed)))),
            ],
            state,
        ),
    );
    reader.open().unwrap();
    reader.close();
    reader.close();
    assert_eq!(first_closed.get(), 1);
    assert_eq!(second_closed.get(), 1);
}

#[test]
fn dropping_open_reader_closes_every_response_once() {
    let state = Rc::new(SharedTransportState::default());
    let first_closed = Rc::new(Cell::new(0));
    let second_closed = Rc::new(Cell::new(0));
    let plan = ReaderPlan::new(ReaderKind::Table, vec![request(), request()], field_types());
    {
        let mut reader = TableIndexReader::new(
            plan,
            transport(
                [
                    Ok(Some(response(&[1], Rc::clone(&first_closed)))),
                    Ok(Some(response(&[2], Rc::clone(&second_closed)))),
                ],
                state,
            ),
        );
        reader.open().unwrap();
    }
    assert_eq!(first_closed.get(), 1);
    assert_eq!(second_closed.get(), 1);
}

#[test]
fn later_open_error_closes_earlier_response_and_terminally_closes_reader() {
    let state = Rc::new(SharedTransportState::default());
    let first_closed = Rc::new(Cell::new(0));
    let plan = ReaderPlan::new(ReaderKind::Index, vec![request(), request()], field_types());
    let mut reader = TableIndexReader::new(
        plan,
        transport(
            [
                Ok(Some(response(&[1], Rc::clone(&first_closed)))),
                Err("second send failed".to_owned()),
            ],
            state,
        ),
    );
    assert_eq!(
        reader.open(),
        Err(StorageReaderError::Query(
            tidb_distsql::QueryRuntimeError::Transport("second send failed".to_owned())
        ))
    );
    assert_eq!(first_closed.get(), 1);
    assert_eq!(reader.state(), ReaderState::Closed);
    assert_eq!(
        reader.next(1),
        Err(StorageReaderError::NotOpen(ReaderState::Closed))
    );
    reader.close();
    assert_eq!(first_closed.get(), 1);
}

#[test]
fn non_dummy_reader_requires_a_real_built_request() {
    let state = Rc::new(SharedTransportState::default());
    let plan = ReaderPlan::new(ReaderKind::Table, Vec::new(), field_types());
    let mut reader = TableIndexReader::new(plan, transport([], Rc::clone(&state)));
    assert_eq!(reader.open(), Err(StorageReaderError::MissingRequest));
    assert_eq!(state.sends.get(), 0);
    assert_eq!(reader.state(), ReaderState::Created);
    reader.close();
}
