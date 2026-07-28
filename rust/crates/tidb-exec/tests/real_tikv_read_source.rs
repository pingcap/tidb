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
use std::sync::Arc;

use chrono::Utc;
use prost::Message;
use tidb_datatype::{parse_time, Datum, FieldTypeCode, MySqlDuration, TimeType};
use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{
    CancelHandle, CopPagingState, QueryDispatch, QueryOperation, QueryTransport, RequestKeyRange,
    RequestType, TimestampSource, TransportRequest,
};
use tidb_exec::real_tikv_read::{RealTiKvPlanExecutorKind, RealTiKvReadError, RealTiKvReadSession};
use tidb_planner::read_only_scan::{
    ConfiguredColumn, ConfiguredTable, ReadOnlyScanError, UnsupportedReadOnlyFeature,
    UnsupportedReadOnlyPredicate,
};
use tidb_proto::tipb::{Chunk, DagRequest, EncodeType, SelectResponse};

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
        let [ranges] = ranges.partitions() else {
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

fn encoded_rows(rows: &[&[i64]]) -> Vec<u8> {
    let mut rows_data = Vec::new();
    for row in rows {
        for value in *row {
            rows_data.push(8);
            encode_signed_varint(&mut rows_data, *value);
        }
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
    let rows = values.iter().map(std::slice::from_ref).collect::<Vec<_>>();
    response_rows(&rows, next_count, close_count)
}

fn response_rows(
    rows: &[&[i64]],
    next_count: Rc<Cell<usize>>,
    close_count: Rc<Cell<usize>>,
) -> ScriptedResponse {
    ScriptedResponse {
        subsets: VecDeque::from([QueryResultSubset {
            data: encoded_rows(rows),
            runtime: None,
        }]),
        next_count,
        close_count,
    }
}

#[test]
fn reordered_two_column_projection_preserves_scan_decode_and_mysql_metadata() {
    let timestamps = ScriptedTimestampSource::new([5_252]);
    let state = Rc::new(SharedTransportState::default());
    let next_count = Rc::new(Cell::new(0));
    let close_count = Rc::new(Cell::new(0));
    let scripted_response = response_rows(
        &[&[-7, 21]],
        Rc::clone(&next_count),
        Rc::clone(&close_count),
    );
    let mut engine = RealTiKvReadSession::new(
        configured_table(),
        transport([scripted_response], Rc::clone(&state)),
        timestamps,
    );

    let query = engine
        .execute("SELECT balance AS amount, id FROM test.accounts")
        .expect("direct stored-column projection must reach the transport");
    let requests = state.requests.borrow();
    let [request] = requests.as_slice() else {
        panic!("exactly one two-column request must be sent");
    };
    let dag = DagRequest::decode(request.data.as_slice()).expect("request data is a TiDB DAG");
    assert_eq!(dag.output_offsets, [0, 1]);
    let scan = dag.executors[0]
        .tbl_scan
        .as_ref()
        .expect("projection lowers to a table scan");
    assert_eq!(
        scan.columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>(),
        [Some(8), Some(7)]
    );
    assert_eq!(
        scan.columns
            .iter()
            .map(|column| column.pk_handle)
            .collect::<Vec<_>>(),
        [Some(false), Some(true)]
    );
    assert_eq!(
        scan.columns
            .iter()
            .map(|column| column.flag)
            .collect::<Vec<_>>(),
        [Some(0x0001), Some(0x0003)]
    );
    drop(requests);

    let mut record_set = query.into_record_set();
    let columns = record_set.columns();
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].name, "amount");
    assert_eq!(columns[0].org_name, "balance");
    assert_eq!(columns[0].flag, 0x0001);
    assert_eq!(columns[0].type_code, FieldTypeCode::LongLong.mysql_type());
    assert_eq!(columns[1].name, "id");
    assert_eq!(columns[1].org_name, "id");
    assert_eq!(columns[1].flag, 0x0003);
    assert_eq!(columns[1].type_code, FieldTypeCode::LongLong.mysql_type());
    assert_eq!(
        record_set.next_batch(1).unwrap(),
        vec![vec![Datum::Int(-7), Datum::Int(21)]]
    );
    assert_eq!(next_count.get(), 1);
    record_set.close().unwrap();
    assert_eq!(close_count.get(), 1);
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

/// Encodes one fixed-width `chunk.Column` (Go `Column.AppendInt64Vec`/
/// `Column.AppendFloat64` shape): a `(row_count, null_count)` header, no null
/// bitmap since every configured column is `NOT NULL`, then each row's native
/// little-endian bytes.
fn chunk_fixed_column(values_le_bytes: &[Vec<u8>]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(values_le_bytes.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    for value in values_le_bytes {
        encoded.extend_from_slice(value);
    }
    encoded
}

/// Encodes one variable-width `chunk.Column` (Go `Column.AppendString` shape):
/// a `(row_count, null_count)` header, a leading-zero offset table, then the
/// concatenated row bytes.
fn chunk_variable_column(rows: &[&[u8]]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    let mut offset = 0_i64;
    encoded.extend_from_slice(&offset.to_le_bytes());
    for row in rows {
        offset += row.len() as i64;
        encoded.extend_from_slice(&offset.to_le_bytes());
    }
    for row in rows {
        encoded.extend_from_slice(row);
    }
    encoded
}

/// Builds one `TypeChunk`-encoded `SelectResponse` over the given already-
/// encoded per-column byte regions, in declared column order (Go
/// `chunk.Codec.Encode` lays every requested column's region out back to
/// back inside one `Chunk.rows_data`).
fn chunk_response(columns: &[Vec<u8>]) -> Vec<u8> {
    let mut rows_data = Vec::new();
    for column in columns {
        rows_data.extend_from_slice(column);
    }
    SelectResponse {
        encode_type: Some(EncodeType::TypeChunk as i32),
        chunks: vec![Chunk {
            rows_data: Some(rows_data),
            rows_meta: Vec::new(),
        }],
        ..SelectResponse::default()
    }
    .encode_to_vec()
}

fn chunk_response_result(
    columns: &[Vec<u8>],
    next_count: Rc<Cell<usize>>,
    close_count: Rc<Cell<usize>>,
) -> ScriptedResponse {
    ScriptedResponse {
        subsets: VecDeque::from([QueryResultSubset {
            data: chunk_response(columns),
            runtime: None,
        }]),
        next_count,
        close_count,
    }
}

/// Regression test for the real-TiKV read path hardcoding every projected
/// column's coprocessor `FieldType` to signed `LONGLONG`
/// (`RealTiKvReadSession::execute_plan`'s former
/// `field_types = ... .map(|_| FieldType::new(FieldTypeCode::LongLong))`).
///
/// A real TiKV coprocessor commonly answers with `TypeChunk` (columnar)
/// encoding, whose physical layout — fixed 8-byte values versus an offset
/// table plus variable bytes — is chosen from the requested `FieldType`, not
/// discovered from the wire. Before the fix, an `UnsignedBigInt` column's high
/// bit was reinterpreted as a sign, and a `Double`/`CHAR` column's variable or
/// differently-shaped bytes were parsed as if they were a fixed 8-byte signed
/// integer. This proves every configured scalar type this milestone admits
/// now decodes through its own real coprocessor byte layout.
#[test]
fn configured_scalar_types_decode_their_own_coprocessor_chunk_layout() {
    let timestamps = ScriptedTimestampSource::new([9_001]);
    let state = Rc::new(SharedTransportState::default());
    let next_count = Rc::new(Cell::new(0));
    let close_count = Rc::new(Cell::new(0));

    let id_column = chunk_fixed_column(&[7_i64.to_le_bytes().to_vec()]);
    let unsigned_column = chunk_fixed_column(&[u64::MAX.to_le_bytes().to_vec()]);
    let double_column = chunk_fixed_column(&[3.5_f64.to_le_bytes().to_vec()]);
    let name_column = chunk_variable_column(&[b"ab"]);
    let scripted_response = chunk_response_result(
        &[id_column, unsigned_column, double_column, name_column],
        Rc::clone(&next_count),
        Rc::clone(&close_count),
    );

    let table = ConfiguredTable::new(
        "test",
        "wide",
        99,
        vec![
            ConfiguredColumn::clustered_primary_key("id", 1),
            ConfiguredColumn::stored_unsigned_bigint_not_null("visits", 2),
            ConfiguredColumn::stored_double_not_null("score", 3),
            ConfiguredColumn::stored_char_not_null("name", 4, 8),
        ],
    );
    let mut engine = RealTiKvReadSession::new(
        table,
        transport([scripted_response], Rc::clone(&state)),
        timestamps,
    );

    let query = engine
        .execute("SELECT id, visits, score, name FROM test.wide")
        .expect("a wide configured projection must reach the transport");
    let mut record_set = query.into_record_set();
    assert_eq!(
        record_set.next_batch(1).unwrap(),
        vec![vec![
            Datum::Int(7),
            Datum::UInt(u64::MAX),
            Datum::Real(3.5),
            Datum::new_collation_string(b"ab".to_vec(), tidb_datatype::Collation::Utf8Mb4Bin),
        ]]
    );
    record_set.close().unwrap();
}

/// Proves the DATE/DATETIME/TIMESTAMP/TIME(fsp) admission decodes each type's
/// own real coprocessor chunk layout: `Date`/`Datetime`/`Timestamp` from the
/// packed 8-byte Go `types.Time` value, `Duration` from a raw 8-byte `int64`
/// nanosecond count that (unlike the self-describing packed `Time`) needs the
/// column's own declared `fsp` to reconstruct a `MySqlDuration`.
#[test]
fn configured_temporal_types_decode_their_own_coprocessor_chunk_layout() {
    let timestamps = ScriptedTimestampSource::new([9_002]);
    let state = Rc::new(SharedTransportState::default());
    let next_count = Rc::new(Cell::new(0));
    let close_count = Rc::new(Cell::new(0));

    let date = parse_time("2024-05-06", TimeType::Date, 0, false, false, false, &Utc)
        .expect("valid DATE literal")
        .time;
    let datetime = parse_time(
        "2024-05-06 07:08:09.5",
        TimeType::DateTime,
        1,
        false,
        false,
        false,
        &Utc,
    )
    .expect("valid DATETIME literal")
    .time;
    let timestamp = parse_time(
        "2024-05-06 07:08:09.5",
        TimeType::Timestamp,
        1,
        false,
        false,
        false,
        &Utc,
    )
    .expect("valid TIMESTAMP literal")
    .time;
    let duration_nanoseconds = ((12 * 3_600 + 34 * 60 + 56) * 1_000_000_000) + 789 * 1_000_000;
    let duration =
        MySqlDuration::from_nanoseconds(duration_nanoseconds, 3).expect("valid TIME literal");

    let id_column = chunk_fixed_column(&[11_i64.to_le_bytes().to_vec()]);
    let date_column = chunk_fixed_column(&[date.go_raw().to_le_bytes().to_vec()]);
    let datetime_column = chunk_fixed_column(&[datetime.go_raw().to_le_bytes().to_vec()]);
    let timestamp_column = chunk_fixed_column(&[timestamp.go_raw().to_le_bytes().to_vec()]);
    let duration_column = chunk_fixed_column(&[duration.nanoseconds().to_le_bytes().to_vec()]);
    let scripted_response = chunk_response_result(
        &[
            id_column,
            date_column,
            datetime_column,
            timestamp_column,
            duration_column,
        ],
        Rc::clone(&next_count),
        Rc::clone(&close_count),
    );

    let table = ConfiguredTable::new(
        "test",
        "temporal",
        100,
        vec![
            ConfiguredColumn::clustered_primary_key("id", 1),
            ConfiguredColumn::stored_date_not_null("d", 2),
            ConfiguredColumn::stored_datetime_not_null("dt", 3, 1),
            ConfiguredColumn::stored_timestamp_not_null("ts", 4, 1),
            ConfiguredColumn::stored_duration_not_null("tm", 5, 3),
        ],
    );
    let mut engine = RealTiKvReadSession::new(
        table,
        transport([scripted_response], Rc::clone(&state)),
        timestamps,
    );

    let query = engine
        .execute("SELECT id, d, dt, ts, tm FROM test.temporal")
        .expect("a temporal configured projection must reach the transport");
    let mut record_set = query.into_record_set();
    assert_eq!(
        record_set.next_batch(1).unwrap(),
        vec![vec![
            Datum::Int(11),
            Datum::new_time(date),
            Datum::new_time(datetime),
            Datum::new_time(timestamp),
            Datum::new_duration(duration),
        ]]
    );
    record_set.close().unwrap();
}

#[test]
fn exact_select_builds_one_timestamped_table_request_and_decodes_lazily() {
    let timestamps = ScriptedTimestampSource::new([4_242]);
    let state = Rc::new(SharedTransportState::default());
    let next_count = Rc::new(Cell::new(0));
    let close_count = Rc::new(Cell::new(0));
    let scripted_response = response(&[21], Rc::clone(&next_count), Rc::clone(&close_count));
    let mut engine = RealTiKvReadSession::new(
        configured_table(),
        transport([scripted_response], Rc::clone(&state)),
        timestamps.clone(),
    );

    let query = engine
        .execute("SELECT id FROM test.accounts")
        .expect("the exact milestone query must reach the transport");
    assert_eq!(
        query.plan_evidence().executor_kinds(),
        [RealTiKvPlanExecutorKind::TableScan]
    );
    assert_eq!(query.plan_evidence().predicate_count(), 0);
    assert_eq!(query.plan_evidence().output_offsets(), [0]);
    assert_eq!(query.snapshot_ts(), Some(4_242));
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
            start_key: vec![
                b't', 0x80, 0, 0, 0, 0, 0, 0, 42, b'_', b'r', 0, 0, 0, 0, 0, 0, 0, 0,
            ]
            .into(),
            end_key: vec![
                b't', 0x80, 0, 0, 0, 0, 0, 0, 42, b'_', b'r', 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0xff, 0xff, 0,
            ]
            .into(),
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
fn caller_cancellation_remains_the_query_transport_authority() {
    let state = Rc::new(SharedTransportState::default());
    let mut engine = RealTiKvReadSession::new(
        configured_table(),
        transport(
            [response(
                &[21],
                Rc::new(Cell::new(0)),
                Rc::new(Cell::new(0)),
            )],
            Rc::clone(&state),
        ),
        ScriptedTimestampSource::new([4_242]),
    );
    let cancellation = Arc::new(CancelHandle::default());

    let query = engine
        .execute_with_cancellation("SELECT id FROM test.accounts", Arc::clone(&cancellation))
        .unwrap();
    assert!(!query.is_cancelled());
    cancellation.cancel();
    assert!(query.is_cancelled());
    assert_eq!(state.sends.get(), 1);
}

#[test]
fn unsupported_predicate_and_write_fail_before_tso_or_send() {
    let timestamps = ScriptedTimestampSource::new([99]);
    let state = Rc::new(SharedTransportState::default());
    let mut engine = RealTiKvReadSession::new(
        configured_table(),
        transport([], Rc::clone(&state)),
        timestamps.clone(),
    );

    assert!(matches!(
        engine.execute("SELECT id FROM accounts WHERE id = 1 OR balance = 2"),
        Err(RealTiKvReadError::Plan(
            ReadOnlyScanError::UnsupportedPredicate(UnsupportedReadOnlyPredicate::BooleanOperator)
        ))
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
    let mut engine = RealTiKvReadSession::new(
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
    let mut engine = RealTiKvReadSession::new(
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

/// Encodes one fixed-width `chunk.Column` whose row 1 of 2 is `NULL`.
///
/// The header carries `nullCount = 1` and a one-byte bitmap where bit `1`
/// means NON-null, so `0b01` marks row 0 present and row 1 null. Go still
/// reserves a full element slot for the null row and leaves the previous
/// row's `elemBuf` bytes there (verified against `pkg/util/chunk`'s own
/// `Codec.Encode` output), so this fixture repeats the live value in the null
/// slot -- a decoder that ignores the bitmap would return it.
fn chunk_fixed_column_with_trailing_null(live: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&2_u32.to_le_bytes());
    encoded.extend_from_slice(&1_u32.to_le_bytes());
    encoded.push(0b0000_0001);
    encoded.extend_from_slice(live);
    encoded.extend_from_slice(live);
    encoded
}

/// Encodes one variable-width `chunk.Column` whose row 1 of 2 is `NULL`. Go
/// gives a null row a zero-width offset span, so the data region holds only
/// the live row's bytes.
fn chunk_variable_column_with_trailing_null(live: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&2_u32.to_le_bytes());
    encoded.extend_from_slice(&1_u32.to_le_bytes());
    encoded.push(0b0000_0001);
    for offset in [0_i64, live.len() as i64, live.len() as i64] {
        encoded.extend_from_slice(&offset.to_le_bytes());
    }
    encoded.extend_from_slice(live);
    encoded
}

/// Every scalar type this node admits decodes a `NULL` cell from the real
/// coprocessor chunk layout, and each nullable result column drops
/// `NotNullFlag` so a client renders the cell as NULL rather than as the
/// stale bytes the null slot still carries.
#[test]
fn every_nullable_scalar_type_decodes_a_null_chunk_cell() {
    let timestamps = ScriptedTimestampSource::new([9_101]);
    let state = Rc::new(SharedTransportState::default());
    let next_count = Rc::new(Cell::new(0));
    let close_count = Rc::new(Cell::new(0));

    // A `DECIMAL(10,2)` cell is the fixed 40-byte `MyDecimal` binary form; the
    // live row is `12.34`, taken from `pkg/util/chunk`'s own encoder output.
    let mut decimal_live = vec![0_u8; 40];
    decimal_live[..8].copy_from_slice(&[0x02, 0x02, 0x02, 0x00, 0x0c, 0x00, 0x00, 0x00]);
    decimal_live[8..12].copy_from_slice(&[0x00, 0xfd, 0x43, 0x14]);

    let columns = vec![
        chunk_fixed_column(&[7_i64.to_le_bytes().to_vec(), 8_i64.to_le_bytes().to_vec()]),
        chunk_fixed_column_with_trailing_null(&11_i64.to_le_bytes()),
        chunk_fixed_column_with_trailing_null(&12_i64.to_le_bytes()),
        chunk_fixed_column_with_trailing_null(&u64::MAX.to_le_bytes()),
        chunk_fixed_column_with_trailing_null(&3.5_f64.to_le_bytes()),
        chunk_variable_column_with_trailing_null(b"ab"),
        chunk_variable_column_with_trailing_null(b"cd"),
        chunk_fixed_column_with_trailing_null(&decimal_live),
    ];
    let scripted_response =
        chunk_response_result(&columns, Rc::clone(&next_count), Rc::clone(&close_count));

    let table = ConfiguredTable::new(
        "test",
        "wide",
        99,
        vec![
            ConfiguredColumn::clustered_primary_key("id", 1),
            ConfiguredColumn::stored_not_null("big", 2).nullable(),
            ConfiguredColumn::stored_int_not_null("small", 3).nullable(),
            ConfiguredColumn::stored_unsigned_bigint_not_null("visits", 4).nullable(),
            ConfiguredColumn::stored_double_not_null("score", 5).nullable(),
            ConfiguredColumn::stored_char_not_null("name", 6, 8).nullable(),
            ConfiguredColumn::stored_varchar_not_null("tag", 7, 8, false).nullable(),
            ConfiguredColumn::stored_decimal_not_null("amount", 8, 10, 2).nullable(),
        ],
    );
    let mut engine = RealTiKvReadSession::new(
        table,
        transport([scripted_response], Rc::clone(&state)),
        timestamps,
    );

    let query = engine
        .execute("SELECT id, big, small, visits, score, name, tag, amount FROM test.wide")
        .expect("a nullable projection must reach the transport");
    // The handle keeps `NotNullFlag | PriKeyFlag`; every nullable column
    // reports no flag at all.
    let mut record_set = query.into_record_set();
    assert_eq!(
        record_set
            .columns()
            .iter()
            .map(|column| column.flag)
            .collect::<Vec<_>>(),
        [3, 0, 0, 0x0020, 0, 0, 0, 0],
        "only the handle keeps NotNullFlag; UnsignedFlag is independent of it"
    );

    let rows = record_set.next_batch(2).unwrap();
    assert_eq!(
        rows[0],
        vec![
            Datum::Int(7),
            Datum::Int(11),
            Datum::Int(12),
            Datum::UInt(u64::MAX),
            Datum::Real(3.5),
            Datum::new_collation_string(b"ab".to_vec(), tidb_datatype::Collation::Utf8Mb4Bin),
            Datum::new_collation_string(b"cd".to_vec(), tidb_datatype::Collation::Utf8Mb4Bin),
            rows[0][7].clone(),
        ]
    );
    assert_eq!(rows[0][7].to_bytes().unwrap(), b"12.34");
    assert_eq!(
        rows[1],
        vec![
            Datum::Int(8),
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ],
        "every nullable type decodes its null row as NULL, not as the stale slot bytes"
    );
    record_set.close().unwrap();
}
