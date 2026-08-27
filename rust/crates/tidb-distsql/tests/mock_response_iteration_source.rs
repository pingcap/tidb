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

//! Ports of the streaming halves of `pkg/distsql/distsql_test.go` and
//! `pkg/distsql/select_result_test.go`: the `mockResponse` state machine that
//! feeds typed rows through real Select-response bytes, the requiredRows
//! ladder contract of `testChunkSize`, and the timezone-aware
//! `selRespChannelIter.Read` walk across three output channels.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

use prost::Message;
use tidb_chunk::chunk::Chunk as DecodedChunk;
use tidb_chunk::codec::Codec as ChunkCodec;
use tidb_codec::{encode_value, encode_value_in_timezone};
use tidb_datatype::{
    parse_datetime, Datum, FieldType, FieldTypeCode, SessionTimeZone, TimeType,
};
use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{
    InjectedQueryRuntime, KvRequestBuilder, QueryDispatch, QueryOperation, QueryResultContext,
    QueryTransport, ResponseChannel, SelectInput, StoreType, TransportRequest, WarningCollector,
    DAG_RESULT_LABEL, GENERAL_SQL_TYPE,
};
use tidb_proto::{Chunk, EncodeType, IntermediateOutput, SelectResponse};

fn longlong_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

/// Go `createSelectNormal` columns: four binary `TypeLonglong` fields
/// (`distsql_test.go:createSelectNormal`, `mysql.TypeLonglong`).
fn four_longlong_columns() -> Vec<FieldType> {
    vec![
        longlong_type(),
        longlong_type(),
        longlong_type(),
        longlong_type(),
    ]
}

fn transport_request() -> TransportRequest {
    TransportRequest::new(
        KvRequestBuilder::new()
            .set_store_type(StoreType::TiKv)
            .build()
            .expect("built request"),
        Arc::new(tidb_distsql::CancelHandle::default()),
    )
}

/// One Go-shaped `kv.Response`: it batches `batch` rows per pull until
/// `total` rows have streamed, panics when pulled after close, and encodes
/// TypeChunk (four int64(123) cells via `chunk.NewCodec`) or TypeDefault
/// (four `int(1)` datums via `codec.EncodeValue`) depending on the session's
/// chunk-RPC policy — exactly the two branches of
/// `distsql_test.go::mockResponse.Next`.
struct MockSelectResponse {
    chunk_rpc: bool,
    batch: usize,
    remaining: usize,
    closed: Rc<Cell<bool>>,
}

impl QueryResponse for MockSelectResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        if self.closed.get() {
            panic!("closed");
        }
        // Go: numRows := max(0, min(resp.batch, resp.total-resp.count)).
        let num_rows = self.batch.min(self.remaining);
        if num_rows == 0 {
            return Ok(None);
        }
        self.remaining -= num_rows;
        let response = if self.chunk_rpc {
            let field_types = four_longlong_columns();
            let mut chk = DecodedChunk::new_with_capacity(&field_types, num_rows);
            for _row in 0..num_rows {
                for column in 0..field_types.len() {
                    chk.append_int64(column, 123);
                }
            }
            SelectResponse {
                encode_type: Some(EncodeType::TypeChunk as i32),
                chunks: vec![Chunk {
                    rows_data: Some(ChunkCodec::new(field_types).encode(&chk)),
                    ..Chunk::default()
                }],
                output_counts: vec![1],
                ..SelectResponse::default()
            }
        } else {
            let mut rows_data = Vec::new();
            for _row in 0..num_rows {
                rows_data.extend_from_slice(
                    &encode_value(&[Datum::Int(1), Datum::Int(1), Datum::Int(1), Datum::Int(1)])
                        .expect("int64 datums"),
                );
            }
            SelectResponse {
                encode_type: Some(EncodeType::TypeDefault as i32),
                chunks: vec![Chunk {
                    rows_data: Some(rows_data),
                    ..Chunk::default()
                }],
                output_counts: vec![1],
                ..SelectResponse::default()
            }
        };
        Ok(Some(QueryResultSubset {
            data: response.encode_to_vec(),
            runtime: None,
        }))
    }

    fn close(&mut self) {
        self.closed.set(true);
    }
}

struct MockSelectTransport(Option<MockSelectResponse>);

impl QueryTransport for MockSelectTransport {
    type Response = MockSelectResponse;

    fn send(
        &mut self,
        _request: &TransportRequest,
        dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        // createSelectNormal pins label/sqlType/rowLen on the built result.
        assert_eq!(dispatch.operation, QueryOperation::Select);
        assert_eq!(dispatch.result.label, DAG_RESULT_LABEL);
        assert_eq!(dispatch.result.sql_type, Some(GENERAL_SQL_TYPE));
        assert_eq!(dispatch.result.row_len, 4);
        Ok(self
            .0
            .take()
            .map(|response| Some(response))
            .expect("one mock response owner per select send"))
    }
}

fn mock_runtime(
    chunk_rpc: bool,
    batch: usize,
    total: usize,
) -> (
    InjectedQueryRuntime<MockSelectTransport>,
    Rc<Cell<bool>>,
) {
    let closed = Rc::new(Cell::new(false));
    let runtime = InjectedQueryRuntime::new(MockSelectTransport(Some(MockSelectResponse {
        chunk_rpc,
        batch,
        remaining: total,
        closed: Rc::clone(&closed),
    })));
    (runtime, closed)
}

/// Source: `pkg/distsql/distsql_test.go::TestSelectNormal` with
/// `createSelectNormal(t, 1, 2, nil, nil)`; the Go mock session keeps
/// `EnableChunkRPC = true` (`pkg/util/mock/context.go:717`).
///
/// Two responses each carry one TypeChunk row of four int64(123) cells; full
/// consumption yields exactly two rows, then draining terminates, and closing
/// publishes the response-owner flag. `memTracker.BytesConsumed() == 0` stays
/// with the unported tracker boundary.
#[test]
fn select_normal_reads_the_mock_response_rows_before_close() {
    let (mut runtime, closed) = mock_runtime(true, 1, 2);
    let result = runtime
        .select(
            &transport_request(),
            SelectInput {
                store_type: StoreType::TiKv,
                mem_tracker_bound: true,
                dist_sql_concurrency: 15,
                ..SelectInput::default()
            },
            QueryResultContext::new(four_longlong_columns(), WarningCollector::new()),
        )
        .expect("select result");
    let mut iter = result.into_select_iter(Vec::new());

    let mut num_all_rows = 0;
    while let Some(row) = iter.next_row().unwrap() {
        num_all_rows += 1;
        assert_eq!(row.row.len(), 4);
        for datum in &row.row {
            assert_eq!(datum, &Datum::Int(123));
        }
    }
    assert_eq!(num_all_rows, 2);

    iter.close();
    assert!(iter.is_closed());
    assert!(closed.get());
    assert!(iter.next_row().unwrap().is_none());
}

/// Source: `pkg/distsql/distsql_test.go::TestSelectMemTracker`
/// (`createSelectNormal(t, 2, 6, nil, nil)` consumed through
/// `chunk.New(colTypes, 3, 3)` and `chk.IsFull()`).
///
/// A required-rows budget of three must come back completely full although
/// each underlying response only carries two rows.
#[test]
fn select_mem_tracker_fills_a_smaller_required_chunk_across_responses() {
    let (mut runtime, closed) = mock_runtime(true, 2, 6);
    let result = runtime
        .select(
            &transport_request(),
            SelectInput {
                store_type: StoreType::TiKv,
                mem_tracker_bound: true,
                dist_sql_concurrency: 15,
                ..SelectInput::default()
            },
            QueryResultContext::new(four_longlong_columns(), WarningCollector::new()),
        )
        .expect("select result");
    let mut iter = result.into_select_iter(Vec::new());

    // Go asserts only that this first budgeted read fills the caller's
    // three-row capacity (`chk.IsFull()`), then closes.
    let chunk = iter
        .next_chunk_with_required_rows(3)
        .unwrap()
        .expect("a full first chunk");
    assert_eq!(chunk.row.num_rows(), 3);

    iter.close();
    assert!(closed.get());
}

/// Source: `pkg/distsql/distsql_test.go::TestSelectNormalChunkSize` running
/// `testChunkSize` over `createSelectNormal(t, 100, …, sctx)` where
/// `EnableChunkRPC=false` forces TypeDefault `int(1)` quadruples.
///
/// Ladder entries recompute the same clamp `chunk.SetRequiredRows(required,
/// 32)` applies client-side (`<=0 -> max`, `170 -> cap`), which is what Go's
/// executor forwards to the coprocessor path. Unconsumed remainder inside a
/// response stays buffered until the next smaller budget arrives.
#[test]
fn select_normal_chunk_size_honors_the_go_required_rows_ladder() {
    let (mut runtime, closed) = mock_runtime(false, 100, 400);
    let result = runtime
        .select(
            &transport_request(),
            SelectInput {
                store_type: StoreType::TiKv,
                mem_tracker_bound: true,
                dist_sql_concurrency: 15,
                ..SelectInput::default()
            },
            QueryResultContext::new(four_longlong_columns(), WarningCollector::new()),
        )
        .expect("select result");
    let mut iter = result.into_select_iter(Vec::new());

    // Next() x2 at full capacity, then requiredRows 1 / 2 / 17 / 170(clamped)
    // / 32 / SetRequiredRows(0 and -1 clamp to max).
    let expected_counts = [32usize, 32, 1, 2, 17, 32, 32];
    for expected_count in expected_counts {
        let chunk = iter
            .next_chunk_with_required_rows(expected_count)
            .unwrap()
            .unwrap_or_else(|| panic!("required {expected_count} returned nothing"));
        assert_eq!(chunk.row.num_rows(), expected_count);
        assert_eq!(chunk.row.get_row(0).get_int64(0), 1);
    }

    iter.close();
    assert!(closed.get());
}

// ---------------------------------------------------------------------------
// TestSelRespChannelIterRead.
// ---------------------------------------------------------------------------

fn statement_zone() -> SessionTimeZone {
    SessionTimeZone::Fixed {
        name: "+01:00".to_owned(),
        offset_secs: 3600,
    }
}

/// Builds the statement-zone wall clock used by every fixture row, matching
/// Go's `time.Date(2024, 1, 12, hh, mm, ss, 0, loc)` base times
/// (`select_result_test.go::TestSelRespChannelIterRead`).
fn zone_time(hour_minute_second: &str) -> tidb_datatype::Time {
    let mut time = parse_datetime(
        &format!("2024-01-12 {hour_minute_second}"),
        &statement_zone(),
        false,
        false,
    )
    .expect("parseable timestamp")
    .time;
    time.set_kind(TimeType::Timestamp);
    time
}

/// One decoded fixture row: `(text, int64, timestamp wall clock in +01:00)`.
///
/// rows0 has six rows (`baseTime.Add(time.Second)` steps with the +4s value
/// duplicated on hello5/hello6), rows1 adds one row, rows3 adds +1000s…
/// which land back inside the same hour.
fn all_fixture_rows() -> Vec<(String, i64, &'static str)> {
    vec![
        ("hello".to_owned(), 1, "13:14:15"),
        ("hello2".to_owned(), 2, "13:14:16"),
        ("hello3".to_owned(), 3, "13:14:17"),
        ("hello4".to_owned(), 4, "13:14:18"),
        ("hello5".to_owned(), 5, "13:14:19"),
        ("hello6".to_owned(), 6, "13:14:19"),
        ("hello30".to_owned(), 30, "13:14:45"),
        ("hello1000".to_owned(), 1000, "13:30:55"),
        ("hello1001".to_owned(), 1001, "13:30:56"),
        ("hello1002".to_owned(), 1002, "13:30:57"),
    ]
}

/// Go `mockChunk(loc, encodeType, colTypes, rows)`: encodes a fixture chunk
/// either as concatenated default-encoded datum rows or as one columnar
/// codec payload; an empty row list still yields present-but-empty row data.
fn fixture_chunk(
    encode_type: EncodeType,
    col_types: &[FieldType],
    rows: &[(String, i64, &'static str)],
) -> Chunk {
    match encode_type {
        EncodeType::TypeDefault => {
            let zone = statement_zone();
            let mut rows_data = Vec::new();
            for (text, value, when) in rows {
                rows_data.extend_from_slice(
                    &encode_value_in_timezone(
                        &zone,
                        &[
                            Datum::new_string(text.as_bytes()),
                            Datum::Int(*value),
                            Datum::Time(zone_time(when)),
                        ],
                    )
                    .expect("fixture row datums"),
                );
            }
            Chunk {
                rows_data: Some(rows_data),
                ..Chunk::default()
            }
        }
        EncodeType::TypeChunk => {
            let mut chk = DecodedChunk::new_with_capacity(col_types, rows.len());
            for (text, value, when) in rows {
                chk.append_string(0, text.as_bytes());
                chk.append_int64(1, *value);
                chk.append_time(2, zone_time(when));
            }
            Chunk {
                rows_data: Some(ChunkCodec::new(col_types.to_vec()).encode(&chk)),
                ..Chunk::default()
            }
        }
        other => panic!("unsupported fixture encode type {other:?}"),
    }
}

/// Source: `pkg/distsql/select_result_test.go::TestSelRespChannelIterRead`
/// running both `verifyIter(tipb.EncodeType_TypeDefault)` and
/// `verifyIter(tipb.EncodeType_TypeChunk)`.
///
/// The response holds three intermediate outputs: channel 0 without chunks,
/// channel 1 with `[rows0, rows1, encoded-empty, `{}`, rows3]`, channel 2
/// with one unset `{}` chunk. All ten channel-1 rows must decode string /
/// int64 / timestamp triples against the statement location and then drain;
/// channels 0 and 2 must contribute nothing. The Rust iterator installs every
/// output channel of each response and drains them in Go's reverse-priority
/// order instead of hand-selecting a channel per iterator, so its observable
/// sequence — channel 2 skipped as empty, ten channel-index-1 rows in order,
/// channel 0 skipped, termination — matches the Go assertions exactly.
#[test]
fn sel_resp_channel_iter_reads_three_typed_channels_across_empty_chunks() {
    for encode_type in [EncodeType::TypeDefault, EncodeType::TypeChunk] {
        let col_types = vec![
            FieldType::new(FieldTypeCode::String),
            FieldType::new(FieldTypeCode::Long),
            FieldType::new(FieldTypeCode::Timestamp),
        ];
        let rows = all_fixture_rows();
        let (rows0, rows1_and_3) = rows.split_at(6);
        let (rows1, rows3) = rows1_and_3.split_at(1);

        let source_response = SelectResponse {
            intermediate_outputs: vec![
                IntermediateOutput {
                    encode_type: Some(encode_type as i32),
                    // "no rows" iterator in Go reads this channel directly.
                    chunks: Vec::new(),
                },
                IntermediateOutput {
                    encode_type: Some(encode_type as i32),
                    chunks: vec![
                        fixture_chunk(encode_type, &col_types, rows0),
                        fixture_chunk(encode_type, &col_types, rows1),
                        // An encoded-but-empty piece plus an entirely unset
                        // `{}` chunk both pass through without error.
                        fixture_chunk(encode_type, &col_types, &[]),
                        Chunk::default(),
                        fixture_chunk(encode_type, &col_types, rows3),
                    ],
                },
                IntermediateOutput {
                    encode_type: Some(encode_type as i32),
                    // "one empty chunk" iterator in Go reads this channel.
                    chunks: vec![Chunk::default()],
                },
            ],
            ..SelectResponse::default()
        };

        let mut source =
            ResponseChannel::<Vec<u8>>::new();
        source.push_result(source_response.encode_to_vec()).unwrap();
        source.finish().unwrap();

        let mut iter = source.into_select_iter_in_timezone(
            Vec::new(),
            vec![
                vec![FieldType::new(FieldTypeCode::String)],
                col_types.clone(),
                vec![longlong_type()],
            ],
            statement_zone(),
            WarningCollector::new(),
        );

        for (row_index, (expected_text, expected_value, expected_when)) in
            rows.iter().enumerate()
        {
            let row = iter.next_row().unwrap().unwrap_or_else(|| {
                panic!("{encode_type:?}: row {row_index} missing before drain")
            });
            assert_eq!(
                row.channel_index, 1,
                "{encode_type:?}: every payload row belongs to intermediate 1"
            );
            assert_eq!(row.row.len(), 3);
            match &row.row[0] {
                Datum::Bytes(bytes) => assert_eq!(bytes.as_slice(), expected_text.as_bytes()),
                Datum::String(string) => assert_eq!(string.bytes(), expected_text.as_bytes()),
                other => panic!("{encode_type:?}: unexpected string datum {other:?}"),
            }
            assert_eq!(row.row[1], Datum::Int(*expected_value));
            assert_eq!(
                row.row[2],
                Datum::Time(zone_time(expected_when)),
                "{encode_type:?}: timestamp round trip through the session zone"
            );
        }
        assert!(iter.next_row().unwrap().is_none());
        iter.close();
        assert!(iter.is_closed());
    }
}
