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

//! Source-derived checks for `pkg/distsql/select_result.go`'s raw response and
//! chunk boundary. These tests intentionally stop before typed Datum decode.

use prost::Message;
use tidb_codec::{ColumnLayout, VALUE_COMPACT_BYTES_FLAG, VALUE_NIL_FLAG, VALUE_VARINT_FLAG};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::{
    decode_chunk, decode_response_chunks, decode_select_response, ChunkDecodeError,
};
use tidb_proto::{Chunk, EncodeType, Error, RowMeta, SelectResponse};

#[test]
fn select_result_response_and_chunk_metadata_round_trip() {
    let response = SelectResponse {
        error: Some(Error {
            code: Some(1105),
            msg: Some("cop error".to_owned()),
        }),
        chunks: vec![Chunk {
            rows_data: Some(b"abcdef".to_vec()),
            rows_meta: vec![
                RowMeta {
                    handle: Some(11),
                    length: Some(2),
                },
                RowMeta {
                    handle: Some(12),
                    length: Some(4),
                },
            ],
        }],
        encode_type: Some(EncodeType::TypeChBlock as i32),
        ..Default::default()
    };

    let decoded = decode_select_response(&response.encode_to_vec()).expect("valid tipb response");
    assert_eq!(
        decoded.error.as_ref().and_then(|error| error.code),
        Some(1105)
    );

    let chunks = decode_response_chunks(&decoded).expect("valid row metadata");
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].rows[0].handle, Some(11));
    assert_eq!(chunks[0].rows[0].data, b"ab");
    assert_eq!(chunks[0].rows[1].data, b"cdef");
}

#[test]
fn row_metadata_rejects_missing_negative_overlong_and_trailing_lengths() {
    let cases = [
        (
            Chunk {
                rows_data: Some(b"a".to_vec()),
                rows_meta: vec![RowMeta {
                    handle: None,
                    length: None,
                }],
            },
            ChunkDecodeError::MissingRowLength { row_index: 0 },
        ),
        (
            Chunk {
                rows_data: Some(b"a".to_vec()),
                rows_meta: vec![RowMeta {
                    handle: None,
                    length: Some(-1),
                }],
            },
            ChunkDecodeError::NegativeRowLength {
                row_index: 0,
                length: -1,
            },
        ),
        (
            Chunk {
                rows_data: Some(b"a".to_vec()),
                rows_meta: vec![RowMeta {
                    handle: None,
                    length: Some(2),
                }],
            },
            ChunkDecodeError::RowLengthExceedsData {
                row_index: 0,
                length: 2,
                remaining: 1,
            },
        ),
        (
            Chunk {
                rows_data: Some(b"ab".to_vec()),
                rows_meta: vec![RowMeta {
                    handle: None,
                    length: Some(1),
                }],
            },
            ChunkDecodeError::RowDataLengthMismatch {
                declared: 1,
                actual: 2,
            },
        ),
    ];

    for (chunk, expected) in cases {
        assert_eq!(decode_chunk(&chunk, EncodeType::TypeChBlock), Err(expected));
    }
}

#[test]
fn opaque_encodings_are_inspectable_but_typed_decode_is_explicitly_unsupported() {
    for encode_type in [EncodeType::TypeDefault, EncodeType::TypeChunk] {
        let chunk = Chunk {
            rows_data: Some(b"opaque".to_vec()),
            rows_meta: vec![],
        };
        let raw = decode_chunk(&chunk, encode_type).expect("metadata-free chunk is opaque");
        assert_eq!(raw.rows_data, b"opaque");
        assert_eq!(raw.rows, Vec::new());
        assert_eq!(
            raw.decode_typed_rows(),
            Err(ChunkDecodeError::UnsupportedTypedRowDecoding { encode_type })
        );
    }
}

#[test]
fn response_rejects_unknown_encode_type_without_guessing() {
    let response = SelectResponse {
        encode_type: Some(99),
        ..Default::default()
    };
    assert_eq!(
        decode_response_chunks(&response),
        Err(ChunkDecodeError::InvalidEncodeType(99))
    );
}

#[test]
fn default_chunk_uses_source_value_framing_without_datum_guessing() {
    let chunk = Chunk {
        rows_data: Some(vec![
            VALUE_NIL_FLAG,
            VALUE_VARINT_FLAG,
            0x02,
            VALUE_COMPACT_BYTES_FLAG,
            0x06,
            b'a',
            b'b',
            b'c',
        ]),
        ..Default::default()
    };
    let raw = decode_chunk(&chunk, EncodeType::TypeDefault).expect("raw default chunk");
    let rows = raw
        .decode_default_values(3)
        .expect("source default value framing");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].flag, VALUE_NIL_FLAG);
    assert_eq!(rows[0][1].payload, &[0x02]);
    assert_eq!(rows[0][2].payload, &[0x06, b'a', b'b', b'c']);
}

#[test]
fn type_chunk_uses_explicit_column_layout_and_preserves_remainder() {
    let mut rows_data = Vec::new();
    rows_data.extend_from_slice(&2_u32.to_le_bytes());
    rows_data.extend_from_slice(&1_u32.to_le_bytes());
    rows_data.push(0b0000_0001);
    rows_data.extend_from_slice(&11_u64.to_le_bytes());
    rows_data.extend_from_slice(&22_u64.to_le_bytes());
    rows_data.extend_from_slice(b"suffix");

    let chunk = Chunk {
        rows_data: Some(rows_data),
        ..Default::default()
    };
    let raw = decode_chunk(&chunk, EncodeType::TypeChunk).expect("raw type chunk");
    let decoded = raw
        .decode_columnar(&[ColumnLayout::fixed(8)])
        .expect("explicit fixed column layout");
    assert_eq!(decoded.remainder, b"suffix");
    assert_eq!(decoded.columns[0].length, 2);
    assert_eq!(decoded.columns[0].is_null(0), Ok(false));
    assert_eq!(decoded.columns[0].is_null(1), Ok(true));
    assert_eq!(decoded.columns[0].data.len(), 16);
}

#[test]
fn type_chunk_typed_scalar_leaf_preserves_remainder_and_nulls() {
    let mut rows_data = Vec::new();
    rows_data.extend_from_slice(&2_u32.to_le_bytes());
    rows_data.extend_from_slice(&1_u32.to_le_bytes());
    rows_data.push(0b0000_0001);
    rows_data.extend_from_slice(&11_u64.to_ne_bytes());
    rows_data.extend_from_slice(&22_u64.to_ne_bytes());
    rows_data.extend_from_slice(b"suffix");

    let chunk = Chunk {
        rows_data: Some(rows_data),
        ..Default::default()
    };
    let raw = decode_chunk(&chunk, EncodeType::TypeChunk).expect("raw type chunk");
    let decoded = raw
        .decode_columnar(&[ColumnLayout::fixed(8)])
        .expect("explicit fixed column layout");
    let typed = decoded
        .decode_datums(&[FieldType::new(FieldTypeCode::Long)])
        .expect("source-proven scalar conversion");
    assert_eq!(typed.remainder, b"suffix");
    assert_eq!(typed.columns, vec![vec![Datum::new_int(11), Datum::Null]]);
}

#[test]
fn type_chunk_typed_scalar_leaf_rejects_opaque_field_type_without_consuming_more() {
    let mut rows_data = Vec::new();
    rows_data.extend_from_slice(&1_u32.to_le_bytes());
    rows_data.extend_from_slice(&0_u32.to_le_bytes());
    rows_data.extend_from_slice(&0_u64.to_ne_bytes());
    rows_data.extend_from_slice(b"suffix");

    let chunk = Chunk {
        rows_data: Some(rows_data),
        ..Default::default()
    };
    let raw = decode_chunk(&chunk, EncodeType::TypeChunk).expect("raw type chunk");
    let decoded = raw
        .decode_columnar(&[ColumnLayout::fixed(8)])
        .expect("explicit fixed column layout");
    assert_eq!(
        decoded.decode_datums(&[FieldType::new(FieldTypeCode::Datetime)]),
        Err(ChunkDecodeError::TypedColumnCodec(
            tidb_codec::TypedColumnError::UnsupportedFieldType(FieldTypeCode::Datetime)
        ))
    );
    assert_eq!(decoded.remainder, b"suffix");
}
