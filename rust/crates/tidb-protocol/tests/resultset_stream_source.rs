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

use tidb_datatype::{Datum, Decimal};
use tidb_protocol::resultset_stream::{
    ResultSetStream, ResultSetStreamError, ResultSetStreamState,
};
use tidb_protocol::{
    ColumnInfo, ResultSetOptions, TextScalar, TYPE_LONG, TYPE_NEW_DECIMAL, TYPE_VAR_STRING,
};

fn column() -> ColumnInfo {
    ColumnInfo {
        schema: "test".to_owned(),
        table: "t".to_owned(),
        org_table: "t".to_owned(),
        name: "a".to_owned(),
        org_name: "a".to_owned(),
        column_length: 32,
        charset: 46,
        flag: 0,
        decimal: 0,
        type_code: TYPE_VAR_STRING,
        default_value: None,
    }
}

#[test]
fn incremental_sequence_matches_write_chunks_without_row_buffering() {
    let mut stream = ResultSetStream::new(
        vec![column()],
        ResultSetOptions {
            status_flags: 2,
            ..ResultSetOptions::default()
        },
    );
    assert_eq!(stream.state(), ResultSetStreamState::Initial);
    let metadata = stream.metadata_packets().unwrap();
    assert_eq!(metadata.len(), 3);
    assert_eq!(metadata[0], vec![1]);
    assert_eq!(metadata[2], vec![0xfe, 0, 0, 2, 0]);
    assert_eq!(
        stream.row_packet(&[Some(b"one".to_vec())]).unwrap(),
        b"\x03one".to_vec()
    );
    assert_eq!(stream.row_packet(&[None]).unwrap(), vec![0xfb]);
    assert_eq!(stream.row_count(), 2);
    assert_eq!(stream.finish_packet().unwrap(), metadata[2]);
    assert_eq!(stream.state(), ResultSetStreamState::Finished);
}

#[test]
fn owned_text_row_matches_borrowed_go_framing_and_encoding() {
    let options = ResultSetOptions {
        result_encoder: tidb_protocol::ResultEncoder::new("utf8mb4").unwrap(),
        ..ResultSetOptions::default()
    };
    let mut borrowed = ResultSetStream::new(vec![column()], options);
    let mut owned = ResultSetStream::new(vec![column()], options);
    borrowed.metadata_packets().unwrap();
    owned.metadata_packets().unwrap();

    let row = vec![Some("go-compatible".as_bytes().to_vec())];
    assert_eq!(
        borrowed.row_packet(&row).unwrap(),
        owned.row_packet_owned(row).unwrap()
    );
    assert_eq!(borrowed.row_count(), owned.row_count());
}

#[test]
fn owned_datum_row_matches_go_text_framing_without_cell_allocations() {
    let mut int = column();
    int.type_code = TYPE_LONG;
    let mut decimal = column();
    decimal.type_code = TYPE_NEW_DECIMAL;
    let mut stream = ResultSetStream::new(
        vec![int, column(), decimal, column()],
        ResultSetOptions::default(),
    );
    stream.metadata_packets().unwrap();

    let row = vec![
        Datum::new_int(-10),
        Datum::new_string("go"),
        Datum::new_decimal(Decimal::from_literal("1.25")),
        Datum::Null,
    ];
    assert_eq!(
        stream.row_packet_datums_owned(row).unwrap(),
        b"\x03-10\x02go\x041.25\xfb".to_vec()
    );
}

#[test]
fn owned_datum_row_preserves_source_type_errors() {
    let mut stream = ResultSetStream::new(vec![column()], ResultSetOptions::default());
    stream.metadata_packets().unwrap();
    assert!(matches!(
        stream.row_packet_datums_owned(vec![Datum::new_int(1)]),
        Err(ResultSetStreamError::TextFormat { column: 0, .. })
    ));
}

#[test]
fn borrowed_text_row_rejects_raw_bytes_for_unknown_column_type() {
    let mut unknown = column();
    unknown.type_code = 0xff;
    let mut stream = ResultSetStream::new(vec![unknown], ResultSetOptions::default());
    stream.metadata_packets().unwrap();
    let mut row = stream.text_row().unwrap();
    assert!(matches!(
        row.append(TextScalar::Bytes(b"not-a-valid-column")),
        Err(ResultSetStreamError::TextFormat { column: 0, .. })
    ));
}

#[test]
fn borrowed_chunk_row_matches_go_dump_text_row_framing() {
    let mut int = column();
    int.type_code = TYPE_LONG;
    let mut decimal = column();
    decimal.type_code = TYPE_NEW_DECIMAL;
    let columns = vec![int, column(), decimal, column()];
    let mut stream = ResultSetStream::new(columns.clone(), ResultSetOptions::default());
    let mut datum_stream = ResultSetStream::new(columns, ResultSetOptions::default());
    stream.metadata_packets().unwrap();
    datum_stream.metadata_packets().unwrap();

    // Go code: pkg/server/internal/column.DumpTextRow appends each borrowed
    // chunk cell to one packet before advancing the source chunk.
    let mut row = stream.text_row_with_capacity(32).unwrap();
    row.append(TextScalar::Signed(-10)).unwrap();
    row.append(TextScalar::Bytes(b"go")).unwrap();
    row.append(TextScalar::Decimal(b"1.25")).unwrap();
    row.append(TextScalar::Null).unwrap();
    let borrowed = row.finish().unwrap();
    let owned = datum_stream
        .row_packet_datums_owned(vec![
            Datum::new_int(-10),
            Datum::new_string("go"),
            Datum::new_decimal(Decimal::from_literal("1.25")),
            Datum::Null,
        ])
        .unwrap();

    assert_eq!(borrowed, owned);
    assert_eq!(borrowed, b"\x03-10\x02go\x041.25\xfb".to_vec());
    assert_eq!(stream.row_count(), 1);
}

#[test]
fn deprecate_eof_skips_metadata_eof_but_keeps_ok_shaped_terminal() {
    let mut stream = ResultSetStream::new(
        vec![column()],
        ResultSetOptions {
            status_flags: 2,
            deprecate_eof: true,
            ..ResultSetOptions::default()
        },
    );
    assert_eq!(stream.metadata_packets().unwrap().len(), 2);
    assert_eq!(
        stream.finish_packet().unwrap(),
        vec![0xfe, 0, 0, 2, 0, 0, 0]
    );
}

#[test]
fn lifecycle_and_row_width_are_checked_at_the_incremental_boundary() {
    let mut stream = ResultSetStream::new(vec![column()], ResultSetOptions::default());
    assert!(matches!(
        stream.row_packet(&[Some(b"x".to_vec())]),
        Err(ResultSetStreamError::InvalidState { .. })
    ));
    stream.metadata_packets().unwrap();
    assert_eq!(
        stream.row_packet(&[]),
        Err(ResultSetStreamError::RowColumnCount {
            row: 0,
            expected: 1,
            actual: 0,
        })
    );
    stream.finish_packet().unwrap();
    assert!(matches!(
        stream.finish_packet(),
        Err(ResultSetStreamError::InvalidState { .. })
    ));
}
