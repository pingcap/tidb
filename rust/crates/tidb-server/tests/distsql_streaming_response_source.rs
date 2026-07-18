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

use std::io::Cursor;

use prost::Message;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_distsql::{
    select_with_runtime_stats, ResponseChannel, ResponseRuntimeStats, SelectInput, StoreType,
    WarningCollector,
};
use tidb_exec::distsql_recordset::DistSqlRecordSet;
use tidb_proto::{Chunk, EncodeType, IntermediateOutput, SelectResponse};
use tidb_protocol::{ColumnInfo, PacketReader, ResultSetOptions, TYPE_LONG, TYPE_VAR_STRING};
use tidb_server::connection_resultset::write_connection_result_set;

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

fn intermediate(encode_type: EncodeType, chunks: Vec<Chunk>) -> IntermediateOutput {
    IntermediateOutput {
        encode_type: Some(encode_type as i32),
        chunks,
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

fn column(name: &str, type_code: u8) -> ColumnInfo {
    ColumnInfo {
        schema: "test".to_owned(),
        table: "t".to_owned(),
        org_table: "t".to_owned(),
        name: name.to_owned(),
        org_name: name.to_owned(),
        column_length: 11,
        charset: 63,
        flag: 0,
        decimal: 0,
        type_code,
        default_value: None,
    }
}

fn packet_payloads(framed: Vec<u8>) -> Vec<Vec<u8>> {
    let mut reader = PacketReader::new(Cursor::new(framed));
    reader.set_sequence(1);
    let mut payloads = Vec::new();
    while reader.get_ref().position() < reader.get_ref().get_ref().len() as u64 {
        payloads.push(reader.read_packet().unwrap());
    }
    payloads
}

fn recordset() -> DistSqlRecordSet {
    // tipb.SelectResponse.chunks -> Chunk.rows_data. Values 7 and 8 use the
    // TiDB value codec varint tag (8) and positive zig-zag bytes 14 and 16.
    let response = vec![0x1a, 0x06, 0x1a, 0x04, 8, 14, 8, 16];
    let mut source = ResponseChannel::new();
    source
        .push_result_with_runtime(
            response,
            ResponseRuntimeStats {
                callee_address: "tikv-1".to_owned(),
                request_rpc_stats_present: false,
                backoff_sleep_ns: vec![("RegionMiss".to_owned(), 7)],
            },
        )
        .unwrap();
    source.finish().unwrap();
    let iter = source
        .into_select_iter(
            vec![FieldType::new(FieldTypeCode::Long)],
            Vec::new(),
            WarningCollector::new(),
        )
        .with_runtime_stats(
            select_with_runtime_stats(
                SelectInput {
                    store_type: StoreType::TiKv,
                    ..SelectInput::default()
                },
                Vec::new(),
                1,
            ),
            true,
        );
    DistSqlRecordSet::new(iter, vec![column("a", TYPE_LONG)])
}

#[test]
fn injected_select_iterator_streams_through_recordset_and_connection_frames() {
    let mut recordset = recordset();
    let response = write_connection_result_set(
        &mut recordset,
        ResultSetOptions {
            status_flags: 2,
            ..ResultSetOptions::default()
        },
        1,
    )
    .unwrap();
    assert_eq!(response.outcome.rows_written, 2);
    assert!(recordset.lifecycle().is_finished());
    assert!(recordset.lifecycle().is_closed());
    assert_eq!(recordset.runtime_stats().backoff_sleep_ns("RegionMiss"), 7);

    let payloads = packet_payloads(response.framed);
    assert_eq!(payloads[0], vec![1]);
    assert!(payloads[1].windows(1).any(|bytes| bytes == b"a"));
    assert_eq!(payloads[3], b"\x017".to_vec());
    assert_eq!(payloads[4], b"\x018".to_vec());
    assert_eq!(payloads[5], vec![0xfe, 0, 0, 2, 0]);
}

#[test]
fn generated_no_intermediate_rows_stream_through_connection_in_exact_order() {
    let response = SelectResponse {
        encode_type: Some(EncodeType::TypeChunk as i32),
        chunks: vec![type_chunk(&[
            vec![Cell::Int(123), Cell::Int(123), Cell::Int(123)],
            vec![Cell::Int(123), Cell::Int(123), Cell::Int(123)],
            vec![Cell::Int(123), Cell::Int(123), Cell::Int(123)],
            vec![Cell::Int(123), Cell::Int(123), Cell::Int(123)],
        ])],
        intermediate_outputs: vec![
            intermediate(EncodeType::TypeChunk, Vec::new()),
            intermediate(EncodeType::TypeChunk, Vec::new()),
        ],
        ..Default::default()
    };
    let iter = response_source([response]).into_select_iter(
        vec![int_type(), int_type(), int_type(), int_type()],
        vec![vec![int_type()], vec![string_type()]],
        WarningCollector::new(),
    );
    let mut recordset = DistSqlRecordSet::new(
        iter,
        (0..4)
            .map(|index| column(&format!("c{index}"), TYPE_LONG))
            .collect(),
    );
    let response = write_connection_result_set(
        &mut recordset,
        ResultSetOptions {
            status_flags: 2,
            ..ResultSetOptions::default()
        },
        1,
    )
    .unwrap();

    assert_eq!(response.outcome.rows_written, 3);
    assert!(recordset.lifecycle().is_finished());
    assert!(recordset.lifecycle().is_closed());
    let payloads = packet_payloads(response.framed);
    assert_eq!(
        &payloads[6..9],
        &[
            b"\x03123\x03123\x03123\x03123".to_vec(),
            b"\x03123\x03123\x03123\x03123".to_vec(),
            b"\x03123\x03123\x03123\x03123".to_vec(),
        ]
    );
}

#[test]
fn generated_no_main_rows_stream_intermediate_priorities_in_exact_order() {
    let responses = [
        SelectResponse {
            encode_type: Some(EncodeType::TypeChunk as i32),
            chunks: Vec::new(),
            intermediate_outputs: vec![
                intermediate(
                    EncodeType::TypeChunk,
                    vec![type_chunk(&[vec![Cell::Int(1), Cell::Int(2)]])],
                ),
                intermediate(EncodeType::TypeChunk, Vec::new()),
            ],
            ..Default::default()
        },
        SelectResponse {
            encode_type: Some(EncodeType::TypeChunk as i32),
            chunks: Vec::new(),
            intermediate_outputs: vec![
                intermediate(EncodeType::TypeChunk, Vec::new()),
                intermediate(
                    EncodeType::TypeChunk,
                    vec![type_chunk(&[vec![Cell::Str("1aa"), Cell::Str("1cc")]])],
                ),
            ],
            ..Default::default()
        },
    ];
    let iter = response_source(responses).into_select_iter(
        vec![int_type(), int_type(), int_type(), int_type()],
        vec![vec![int_type()], vec![string_type()]],
        WarningCollector::new(),
    );
    let mut recordset = DistSqlRecordSet::new(iter, vec![column("intermediate", TYPE_VAR_STRING)]);
    let response = write_connection_result_set(
        &mut recordset,
        ResultSetOptions {
            status_flags: 2,
            ..ResultSetOptions::default()
        },
        1,
    )
    .unwrap();

    assert_eq!(response.outcome.rows_written, 4);
    assert!(recordset.lifecycle().is_finished());
    assert!(recordset.lifecycle().is_closed());
    let payloads = packet_payloads(response.framed);
    assert_eq!(
        &payloads[3..7],
        &[
            b"\x011".to_vec(),
            b"\x012".to_vec(),
            b"\x031aa".to_vec(),
            b"\x031cc".to_vec(),
        ]
    );
}
