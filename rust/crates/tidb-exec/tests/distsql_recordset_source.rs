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

use prost::Message;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_distsql::{ResponseChannel, WarningCollector};
use tidb_exec::distsql_recordset::{DistSqlRecordSet, DistSqlRecordSetError};
use tidb_proto::{Chunk, EncodeType, SelectResponse};
use tidb_protocol::resultset_stream::ResultSetStream;
use tidb_protocol::{ColumnInfo, ResultSetOptions, TYPE_LONG, TYPE_NEW_DECIMAL};

fn column() -> ColumnInfo {
    ColumnInfo {
        schema: "test".to_owned(),
        table: "t".to_owned(),
        org_table: "t".to_owned(),
        name: "a".to_owned(),
        org_name: "a".to_owned(),
        column_length: 11,
        charset: 63,
        flag: 0,
        decimal: 0,
        type_code: TYPE_LONG,
        default_value: None,
    }
}

fn recordset(values: &[u8]) -> DistSqlRecordSet {
    let mut rows_data = Vec::with_capacity(values.len() * 2);
    for value in values {
        // TiDB value codec's varint tag followed by the positive zig-zag byte.
        rows_data.extend_from_slice(&[8, *value << 1]);
    }
    // tipb.SelectResponse.chunks (field 3) containing one Chunk.rows_data
    // (field 3). Omitted encode_type is protobuf's TypeDefault zero value.
    let mut chunk = vec![0x1a, rows_data.len() as u8];
    chunk.extend_from_slice(&rows_data);
    let mut response = vec![0x1a, chunk.len() as u8];
    response.extend_from_slice(&chunk);
    let mut source = ResponseChannel::new();
    source.push_result(response).unwrap();
    source.finish().unwrap();
    let iter = source.into_select_iter(
        vec![FieldType::new(FieldTypeCode::Long)],
        Vec::new(),
        WarningCollector::new(),
    );
    DistSqlRecordSet::new(iter, vec![column()])
}

fn typed_recordset(field_type: FieldType, type_code: u8, cell: &[u8]) -> DistSqlRecordSet {
    let mut rows_data = Vec::with_capacity(8 + cell.len());
    rows_data.extend_from_slice(&1_u32.to_le_bytes());
    rows_data.extend_from_slice(&0_u32.to_le_bytes());
    rows_data.extend_from_slice(cell);
    let response = SelectResponse {
        encode_type: Some(EncodeType::TypeChunk as i32),
        chunks: vec![Chunk {
            rows_data: Some(rows_data),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut source = ResponseChannel::new();
    source.push_result(response.encode_to_vec()).unwrap();
    source.finish().unwrap();
    let iter = source.into_select_iter(vec![field_type], Vec::new(), WarningCollector::new());
    let mut result_column = column();
    result_column.type_code = type_code;
    DistSqlRecordSet::new(iter, vec![result_column])
}

#[test]
fn select_response_rows_are_pulled_in_bounded_batches() {
    let mut recordset = recordset(&[1, 2, 3]);
    assert_eq!(
        recordset.next_batch(2).unwrap(),
        vec![
            vec![tidb_datatype::Datum::Int(1)],
            vec![tidb_datatype::Datum::Int(2)]
        ]
    );
    assert_eq!(
        recordset.next_batch(2).unwrap(),
        vec![vec![tidb_datatype::Datum::Int(3)]]
    );
    assert!(recordset.next_batch(2).unwrap().is_empty());
    assert!(recordset.lifecycle().has_advanced());
}

#[test]
fn select_response_chunk_writes_go_text_rows_before_advancing() {
    let mut recordset = recordset(&[1, 2, 3]);
    let batch = recordset.next_text_batch(2).unwrap().unwrap();
    let mut stream = ResultSetStream::new(vec![column()], ResultSetOptions::default());
    stream.metadata_packets().unwrap();

    // Go code: pkg/server/internal/column.DumpTextRow formats borrowed
    // chunk.Row values into length-encoded text while the chunk is retained.
    assert_eq!(
        batch.write_rows(&mut stream).unwrap(),
        vec![b"\x011".to_vec(), b"\x012".to_vec()]
    );
    let batch = recordset.next_text_batch(2).unwrap().unwrap();
    assert_eq!(
        batch.write_rows(&mut stream).unwrap(),
        vec![b"\x013".to_vec()]
    );
    assert!(recordset.next_text_batch(2).unwrap().is_none());
    assert_eq!(stream.row_count(), 3);
}

#[test]
fn typed_primitive_chunk_writes_go_text_without_rematerializing_a_datum() {
    let mut recordset = typed_recordset(
        FieldType::new(FieldTypeCode::Long),
        TYPE_LONG,
        &14_i64.to_ne_bytes(),
    );
    let batch = recordset.next_text_batch(1).unwrap().unwrap();
    let mut stream = ResultSetStream::new(vec![column()], ResultSetOptions::default());
    stream.metadata_packets().unwrap();

    assert_eq!(
        batch.write_rows(&mut stream).unwrap(),
        vec![b"\x0214".to_vec()]
    );
}

#[test]
fn typed_chunk_text_path_rejects_a_malformed_decimal_before_getter_use() {
    let mut invalid_decimal = vec![0; 40];
    invalid_decimal[3] = 2;
    let mut recordset = typed_recordset(
        FieldType::new(FieldTypeCode::NewDecimal),
        TYPE_NEW_DECIMAL,
        &invalid_decimal,
    );
    let batch = recordset.next_text_batch(1).unwrap().unwrap();
    let mut decimal_column = column();
    decimal_column.type_code = TYPE_NEW_DECIMAL;
    let mut stream = ResultSetStream::new(vec![decimal_column], ResultSetOptions::default());
    stream.metadata_packets().unwrap();

    let error = batch.write_rows(&mut stream).unwrap_err();
    assert!(error.contains("invalid payload"), "{error}");
}

#[test]
fn finish_and_close_are_idempotent_and_metadata_survives_close() {
    let mut recordset = recordset(&[]);
    recordset.finish().unwrap();
    recordset.finish().unwrap();
    recordset.close().unwrap();
    recordset.close().unwrap();
    assert!(recordset.lifecycle().is_finished());
    assert!(recordset.lifecycle().is_closed());
    assert_eq!(recordset.columns(), &[column()]);
}

#[test]
fn close_without_explicit_finish_still_runs_finish_cleanup() {
    let mut recordset = recordset(&[]);
    recordset.close().unwrap();
    assert!(recordset.lifecycle().is_finished());
    assert!(recordset.lifecycle().is_closed());
}

#[test]
fn first_source_error_is_preserved_without_fabricating_rows() {
    let mut source = ResponseChannel::new();
    source.fail("first next failed").unwrap();
    let iter = source.into_select_iter(Vec::new(), Vec::new(), WarningCollector::new());
    let mut recordset = DistSqlRecordSet::new(iter, Vec::new());
    assert_eq!(
        recordset.next_batch(32),
        Err(DistSqlRecordSetError::Source(
            "first next failed".to_owned()
        ))
    );
    assert!(recordset.lifecycle().has_advanced());
}
