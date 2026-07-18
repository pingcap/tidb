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

//! Connected source-derived tests for DistSQL select-response consumption.

use std::collections::BTreeMap;

use prost::Message;
use tidb_codec::{VALUE_COMPACT_BYTES_FLAG, VALUE_VARINT_FLAG};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::{
    select_with_runtime_stats, ChannelIter, ChannelIterError, ResponseChannel,
    ResponseChannelError, ResponseRuntimeStats, SelectInput, StoreType, WarningClass,
    WarningCollector,
};
use tidb_proto::{
    Chunk, EncodeType, Error as TipbError, ExecutorExecutionSummary, IntermediateOutput,
    SelectResponse,
};

#[derive(Clone, Debug)]
enum Cell<'a> {
    Int(i64),
    Str(&'a str),
}

fn int_type() -> FieldType {
    FieldType::new(FieldTypeCode::Long)
}

fn string_type() -> FieldType {
    FieldType::new(FieldTypeCode::String)
}

fn default_chunk(rows: &[Vec<Cell<'_>>]) -> Chunk {
    let mut data = Vec::new();
    for row in rows {
        for cell in row {
            match cell {
                Cell::Int(value) => {
                    assert!(*value >= 0 && *value < 64);
                    data.extend_from_slice(&[VALUE_VARINT_FLAG, (*value as u8) << 1]);
                }
                Cell::Str(value) => {
                    assert!(value.len() < 64);
                    data.extend_from_slice(&[VALUE_COMPACT_BYTES_FLAG, (value.len() as u8) << 1]);
                    data.extend_from_slice(value.as_bytes());
                }
            }
        }
    }
    Chunk {
        rows_data: Some(data),
        ..Default::default()
    }
}

fn type_chunk(columns: &[Vec<Cell<'_>>]) -> Chunk {
    let mut data = Vec::new();
    for column in columns {
        data.extend_from_slice(&(column.len() as u32).to_le_bytes());
        data.extend_from_slice(&0_u32.to_le_bytes());
        match column.first() {
            Some(Cell::Int(_)) => {
                for cell in column {
                    let Cell::Int(value) = cell else {
                        panic!("mixed test column")
                    };
                    data.extend_from_slice(&value.to_ne_bytes());
                }
            }
            Some(Cell::Str(_)) => {
                let mut offset = 0_i64;
                data.extend_from_slice(&offset.to_le_bytes());
                for cell in column {
                    let Cell::Str(value) = cell else {
                        panic!("mixed test column")
                    };
                    offset += value.len() as i64;
                    data.extend_from_slice(&offset.to_le_bytes());
                }
                for cell in column {
                    let Cell::Str(value) = cell else {
                        unreachable!()
                    };
                    data.extend_from_slice(value.as_bytes());
                }
            }
            None => {}
        }
    }
    Chunk {
        rows_data: Some(data),
        ..Default::default()
    }
}

fn response_source(
    responses: impl IntoIterator<Item = SelectResponse>,
) -> ResponseChannel<Vec<u8>> {
    let mut source = ResponseChannel::new();
    for response in responses {
        source.push_result(response.encode_to_vec()).unwrap();
    }
    source.finish().unwrap();
    source
}

fn intermediate(encode_type: EncodeType, chunks: Vec<Chunk>) -> IntermediateOutput {
    IntermediateOutput {
        encode_type: Some(encode_type as i32),
        chunks,
    }
}

fn complete_summary(value: u64) -> ExecutorExecutionSummary {
    ExecutorExecutionSummary {
        time_processed_ns: Some(value),
        num_produced_rows: Some(value),
        num_iterations: Some(value),
        ..Default::default()
    }
}

#[test]
fn update_cop_runtime_stats_preserves_source_gates_and_merge_order() {
    let input = SelectInput {
        store_type: StoreType::TiKv,
        ..SelectInput::default()
    };
    let mut metadata = select_with_runtime_stats(input, Vec::new(), 1234);
    let mut iter = response_source(Vec::<SelectResponse>::new()).into_select_iter(
        Vec::new(),
        Vec::new(),
        WarningCollector::new(),
    );

    iter.update_runtime_stats(
        &metadata,
        false,
        "a",
        false,
        [("RegionMiss".to_owned(), 100)],
        &[],
    );
    assert_eq!(iter.runtime_stats().backoff_sleep_ns("RegionMiss"), 0);

    iter.update_runtime_stats(
        &metadata,
        true,
        "callee",
        false,
        [("RegionMiss".to_owned(), 200)],
        &[complete_summary(1)],
    );
    assert_eq!(iter.runtime_stats().backoff_sleep_ns("RegionMiss"), 200);
    assert!(iter.runtime_stats().plan_summary(1234).is_none());

    metadata.cop_plan_ids = vec![1234];
    iter.update_runtime_stats(
        &metadata,
        true,
        "callee",
        false,
        [("RegionMiss".to_owned(), 300)],
        &[complete_summary(1)],
    );
    assert_eq!(iter.runtime_stats().backoff_sleep_ns("RegionMiss"), 500);
    assert_eq!(
        iter.runtime_stats().plan_summary(1234),
        Some(&complete_summary(1))
    );
}

#[test]
fn new_sel_resp_channel_iter_uses_each_channel_schema_and_rejects_invalid_layout() {
    let response = SelectResponse {
        encode_type: Some(EncodeType::TypeChunk as i32),
        chunks: vec![type_chunk(&[vec![Cell::Int(3)]])],
        intermediate_outputs: vec![
            intermediate(
                EncodeType::TypeDefault,
                vec![default_chunk(&[vec![Cell::Str("one")]])],
            ),
            intermediate(
                EncodeType::TypeChunk,
                vec![type_chunk(&[vec![Cell::Int(2)]])],
            ),
        ],
        ..Default::default()
    };
    let warnings = WarningCollector::new();
    let mut iter = response_source([response]).into_select_iter(
        vec![int_type()],
        vec![vec![string_type()], vec![int_type()]],
        warnings,
    );
    let channels = [2, 1, 0].map(|expected| {
        let row = iter.next_row().unwrap().unwrap();
        assert_eq!(row.channel_index, expected);
        row.channel_index
    });
    assert_eq!(channels, [2, 1, 0]);

    assert!(matches!(
        ChannelIter::<i32>::try_new(3, 3, Vec::<Vec<i32>>::new()),
        Err(ChannelIterError::InvalidChannel { channel: 3, .. })
    ));
}

#[test]
fn sel_resp_channel_iter_reads_default_and_type_chunk_rows_across_empty_chunks() {
    for encode_type in [EncodeType::TypeDefault, EncodeType::TypeChunk] {
        let rows = [
            ("hello", 1),
            ("hello2", 2),
            ("hello3", 3),
            ("hello4", 4),
            ("hello5", 5),
            ("hello6", 6),
            ("hello30", 30),
            ("hello40", 40),
            ("hello41", 41),
            ("hello42", 42),
        ];
        let chunks = if encode_type == EncodeType::TypeDefault {
            vec![
                default_chunk(
                    &rows[..6]
                        .iter()
                        .map(|(text, value)| vec![Cell::Str(text), Cell::Int(*value)])
                        .collect::<Vec<_>>(),
                ),
                default_chunk(
                    &rows[6..7]
                        .iter()
                        .map(|(text, value)| vec![Cell::Str(text), Cell::Int(*value)])
                        .collect::<Vec<_>>(),
                ),
                Chunk::default(),
                default_chunk(
                    &rows[7..]
                        .iter()
                        .map(|(text, value)| vec![Cell::Str(text), Cell::Int(*value)])
                        .collect::<Vec<_>>(),
                ),
            ]
        } else {
            vec![
                type_chunk(&[
                    rows[..6].iter().map(|(text, _)| Cell::Str(text)).collect(),
                    rows[..6]
                        .iter()
                        .map(|(_, value)| Cell::Int(*value))
                        .collect(),
                ]),
                type_chunk(&[vec![Cell::Str(rows[6].0)], vec![Cell::Int(rows[6].1)]]),
                Chunk::default(),
                type_chunk(&[
                    rows[7..].iter().map(|(text, _)| Cell::Str(text)).collect(),
                    rows[7..]
                        .iter()
                        .map(|(_, value)| Cell::Int(*value))
                        .collect(),
                ]),
            ]
        };
        let response = SelectResponse {
            intermediate_outputs: vec![intermediate(encode_type, chunks)],
            ..Default::default()
        };
        let mut iter = response_source([response]).into_select_iter(
            Vec::new(),
            vec![vec![string_type(), int_type()]],
            WarningCollector::new(),
        );
        let mut actual = Vec::new();
        while let Some(row) = iter.next_row().unwrap() {
            assert_eq!(row.channel_index, 0);
            actual.push(row.row);
        }
        assert_eq!(actual.len(), rows.len());
        for (actual, (text, value)) in actual.iter().zip(rows) {
            match &actual[0] {
                Datum::Bytes(bytes) => assert_eq!(bytes, text.as_bytes()),
                Datum::String(string) => assert_eq!(string.bytes(), text.as_bytes()),
                other => panic!("unexpected string datum {other:?}"),
            }
            assert_eq!(actual[1], Datum::new_int(value));
        }
    }
}

#[test]
fn select_result_iter_reads_final_then_intermediate_channels_across_responses() {
    let responses = vec![
        SelectResponse {
            encode_type: Some(EncodeType::TypeChunk as i32),
            chunks: vec![type_chunk(&[
                vec![Cell::Int(123), Cell::Int(123)],
                vec![Cell::Int(123), Cell::Int(123)],
                vec![Cell::Int(123), Cell::Int(123)],
                vec![Cell::Int(123), Cell::Int(123)],
            ])],
            intermediate_outputs: vec![
                intermediate(
                    EncodeType::TypeChunk,
                    vec![type_chunk(&[vec![
                        Cell::Int(1),
                        Cell::Int(2),
                        Cell::Int(3),
                        Cell::Int(4),
                        Cell::Int(5),
                    ]])],
                ),
                intermediate(
                    EncodeType::TypeChunk,
                    vec![type_chunk(&[vec![Cell::Str("aa"), Cell::Str("bb")]])],
                ),
            ],
            ..Default::default()
        },
        SelectResponse {
            encode_type: Some(EncodeType::TypeChunk as i32),
            chunks: vec![type_chunk(&[
                vec![Cell::Int(123)],
                vec![Cell::Int(123)],
                vec![Cell::Int(123)],
                vec![Cell::Int(123)],
            ])],
            intermediate_outputs: vec![
                intermediate(
                    EncodeType::TypeChunk,
                    vec![type_chunk(&[vec![Cell::Int(11)]])],
                ),
                intermediate(
                    EncodeType::TypeChunk,
                    vec![type_chunk(&[vec![
                        Cell::Str("1aa"),
                        Cell::Str("1bb"),
                        Cell::Str("1cc"),
                    ]])],
                ),
            ],
            ..Default::default()
        },
        SelectResponse {
            encode_type: Some(EncodeType::TypeChunk as i32),
            chunks: Vec::new(),
            intermediate_outputs: vec![
                intermediate(
                    EncodeType::TypeChunk,
                    vec![type_chunk(&[vec![Cell::Int(21), Cell::Int(22)]])],
                ),
                intermediate(
                    EncodeType::TypeChunk,
                    vec![type_chunk(&[vec![
                        Cell::Str("2aa"),
                        Cell::Str("2bb"),
                        Cell::Str("2cc"),
                        Cell::Str("2dd"),
                    ]])],
                ),
            ],
            ..Default::default()
        },
        SelectResponse {
            encode_type: Some(EncodeType::TypeChunk as i32),
            intermediate_outputs: vec![
                intermediate(EncodeType::TypeChunk, Vec::new()),
                intermediate(EncodeType::TypeChunk, Vec::new()),
            ],
            ..Default::default()
        },
    ];
    let mut iter = response_source(responses).into_select_iter(
        vec![int_type(), int_type(), int_type(), int_type()],
        vec![vec![int_type()], vec![string_type()]],
        WarningCollector::new(),
    );
    let mut channels = Vec::new();
    let mut rows = Vec::new();
    while let Some(row) = iter.next_row().unwrap() {
        channels.push(row.channel_index);
        rows.push(match row.channel_index {
            2 => row
                .row
                .iter()
                .map(|datum| match datum {
                    Datum::Int(value) => value.to_string(),
                    other => panic!("unexpected final datum {other:?}"),
                })
                .collect::<Vec<_>>()
                .join("_"),
            1 => match &row.row[0] {
                Datum::String(value) => value.as_utf8().unwrap().to_owned(),
                Datum::Bytes(value) => String::from_utf8(value.clone()).unwrap(),
                other => panic!("unexpected string datum {other:?}"),
            },
            0 => match &row.row[0] {
                Datum::Int(value) => value.to_string(),
                other => panic!("unexpected integer datum {other:?}"),
            },
            channel => panic!("unexpected channel {channel}"),
        });
    }
    assert_eq!(
        channels,
        [2, 2, 1, 1, 0, 0, 0, 0, 0, 2, 1, 1, 1, 0, 1, 1, 1, 1, 0, 0]
    );
    assert_eq!(
        rows,
        [
            "123_123_123_123",
            "123_123_123_123",
            "aa",
            "bb",
            "1",
            "2",
            "3",
            "4",
            "5",
            "123_123_123_123",
            "1aa",
            "1bb",
            "1cc",
            "11",
            "2aa",
            "2bb",
            "2cc",
            "2dd",
            "21",
            "22",
        ]
    );
}

#[test]
fn final_channel_rows_precede_a_lower_priority_decode_error() {
    let response = SelectResponse {
        encode_type: Some(EncodeType::TypeChunk as i32),
        chunks: vec![type_chunk(&[vec![Cell::Int(7)]])],
        intermediate_outputs: vec![intermediate(
            EncodeType::TypeChunk,
            vec![Chunk {
                rows_data: Some(vec![1]),
                ..Default::default()
            }],
        )],
        ..Default::default()
    };
    let mut iter = response_source([response]).into_select_iter(
        vec![int_type()],
        vec![vec![int_type()]],
        WarningCollector::new(),
    );

    let row = iter.next_row().unwrap().unwrap();
    assert_eq!(row.channel_index, 1);
    assert_eq!(row.row, vec![Datum::new_int(7)]);
    assert!(matches!(
        iter.next_row(),
        Err(ResponseChannelError::RowDecode(_))
    ));
}

#[test]
fn earlier_chunk_rows_precede_a_later_chunk_decode_error() {
    let response = SelectResponse {
        encode_type: Some(EncodeType::TypeChunk as i32),
        chunks: vec![
            type_chunk(&[vec![Cell::Int(8)]]),
            Chunk {
                rows_data: Some(vec![1]),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut iter = response_source([response]).into_select_iter(
        vec![int_type()],
        Vec::new(),
        WarningCollector::new(),
    );

    assert_eq!(
        iter.next_row().unwrap().unwrap().row,
        vec![Datum::new_int(8)]
    );
    assert!(matches!(
        iter.next_row(),
        Err(ResponseChannelError::RowDecode(_))
    ));
}

#[test]
fn open_empty_source_is_pending_and_can_receive_a_later_response() {
    let source = ResponseChannel::new();
    let mut iter = source.into_select_iter(vec![int_type()], Vec::new(), WarningCollector::new());

    assert_eq!(iter.next_row(), Err(ResponseChannelError::Pending));
    assert!(!iter.is_closed());
    iter.push_response(
        SelectResponse {
            encode_type: Some(EncodeType::TypeChunk as i32),
            chunks: vec![type_chunk(&[vec![Cell::Int(9)]])],
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .unwrap();
    assert_eq!(
        iter.next_row().unwrap().unwrap().row,
        vec![Datum::new_int(9)]
    );
    assert_eq!(iter.next_row(), Err(ResponseChannelError::Pending));
    iter.finish_source().unwrap();
    assert_eq!(iter.next_row().unwrap(), None);
    assert!(iter.is_closed());
}

#[test]
fn consumed_responses_only_record_present_runtime_samples_losslessly() {
    let metadata = select_with_runtime_stats(
        SelectInput {
            store_type: StoreType::TiKv,
            ..SelectInput::default()
        },
        vec![1234],
        1234,
    );
    let mut first_summary = complete_summary(1);
    first_summary.executor_id = Some("executor_1".to_owned());
    first_summary.ru_consumption = Some(vec![1]);
    let mut second_summary = complete_summary(2);
    second_summary.executor_id = Some("executor_2".to_owned());
    second_summary.ru_consumption = Some(vec![2]);
    let mut source = ResponseChannel::new();
    source
        .push_result(
            SelectResponse {
                execution_summaries: vec![complete_summary(100)],
                ..Default::default()
            }
            .encode_to_vec(),
        )
        .unwrap();
    source
        .push_result_with_runtime(
            SelectResponse {
                execution_summaries: vec![complete_summary(200)],
                ..Default::default()
            }
            .encode_to_vec(),
            ResponseRuntimeStats {
                callee_address: String::new(),
                request_rpc_stats_present: false,
                backoff_sleep_ns: vec![("Ignored".to_owned(), 20)],
            },
        )
        .unwrap();
    source
        .push_result_with_runtime(
            SelectResponse {
                execution_summaries: vec![first_summary.clone()],
                ..Default::default()
            }
            .encode_to_vec(),
            ResponseRuntimeStats {
                callee_address: String::new(),
                request_rpc_stats_present: true,
                backoff_sleep_ns: vec![("RegionMiss".to_owned(), 10)],
            },
        )
        .unwrap();
    source
        .push_result_with_runtime(
            SelectResponse {
                execution_summaries: vec![second_summary.clone()],
                ..Default::default()
            }
            .encode_to_vec(),
            ResponseRuntimeStats {
                callee_address: "callee".to_owned(),
                request_rpc_stats_present: false,
                backoff_sleep_ns: Vec::new(),
            },
        )
        .unwrap();
    source.finish().unwrap();
    let mut iter = source
        .into_select_iter(Vec::new(), Vec::new(), WarningCollector::new())
        .with_runtime_stats(metadata, true);

    assert_eq!(iter.next_row().unwrap(), None);
    let summaries = iter.runtime_stats().plan_summaries(1234);
    assert_eq!(summaries, [first_summary, second_summary]);
    assert_eq!(iter.runtime_stats().backoff_sleep_ns("RegionMiss"), 10);
    assert_eq!(iter.runtime_stats().backoff_sleep_ns("Ignored"), 0);
}

#[test]
fn select_result_test_file_obligations_publish_warnings_errors_mismatch_and_close() {
    let warnings = WarningCollector::new();
    let response = SelectResponse {
        warnings: vec![TipbError {
            code: Some(1265),
            msg: Some("truncated".to_owned()),
        }],
        ..Default::default()
    };
    let mut iter =
        response_source([response]).into_select_iter(Vec::new(), Vec::new(), warnings.clone());
    assert_eq!(iter.next_row().unwrap(), None);
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings.warnings()[0].message, "truncated");
    assert_eq!(warnings.warnings()[0].class, WarningClass::TiKv);
    assert_eq!(warnings.warnings()[0].code, Some(1265));

    let mismatch = SelectResponse {
        intermediate_outputs: vec![intermediate(EncodeType::TypeChunk, Vec::new())],
        ..Default::default()
    };
    let mut iter = response_source([mismatch]).into_select_iter(
        Vec::new(),
        Vec::new(),
        WarningCollector::new(),
    );
    assert!(matches!(
        iter.next_row(),
        Err(ResponseChannelError::IntermediateOutputCount {
            expected: 0,
            actual: 1
        })
    ));

    let error = SelectResponse {
        error: Some(TipbError {
            code: Some(1105),
            msg: Some("cop error".to_owned()),
        }),
        ..Default::default()
    };
    let mut iter =
        response_source([error]).into_select_iter(Vec::new(), Vec::new(), WarningCollector::new());
    assert!(matches!(
        iter.next_row(),
        Err(ResponseChannelError::SelectResponse { code: 1105, .. })
    ));
    iter.close();
    iter.close();
    assert!(iter.is_closed());

    let _source_table = BTreeMap::from([
        ("warnings", "published"),
        ("errors", "terminal"),
        ("layout", "validated"),
        ("close", "idempotent"),
    ]);
}
