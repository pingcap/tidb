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

//! Source-derived checks for the bounded scalar leaf over
//! `pkg/util/chunk/codec.go`.

use tidb_codec::{decode_column_datums, decode_columns, ColumnLayout, TypedColumnError};
use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode};

fn fixed_column(values: &[u64], null_bitmap: Option<u8>) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(values.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&u32::from(null_bitmap.is_some()).to_le_bytes());
    if let Some(bitmap) = null_bitmap {
        encoded.push(bitmap);
    }
    for value in values {
        encoded.extend_from_slice(&value.to_ne_bytes());
    }
    encoded
}

#[test]
fn typed_chunk_scalar_fixed_values_follow_source_native_storage() {
    // Go source: Column.AppendInt64/AppendUint64 writes the native scalar
    // directly into elemBuf and Codec.Encode appends that byte region.
    let encoded = fixed_column(
        &[10, u64::from_ne_bytes((-2_i64).to_ne_bytes()), 30],
        Some(0b0000_0101),
    );
    let (remainder, columns) = decode_columns(&encoded, &[ColumnLayout::fixed(8)]).unwrap();
    assert!(remainder.is_empty());

    let signed = decode_column_datums(&columns[0], FieldType::new(FieldTypeCode::Long)).unwrap();
    assert_eq!(
        signed,
        vec![Datum::new_int(10), Datum::Null, Datum::new_int(30)]
    );

    let unsigned = decode_column_datums(
        &columns[0],
        FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
    )
    .unwrap();
    assert_eq!(
        unsigned,
        vec![Datum::new_uint(10), Datum::Null, Datum::new_uint(30)]
    );
}

#[test]
fn typed_chunk_scalar_float_values_preserve_float32_promotion() {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&1_u32.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    encoded.extend_from_slice(&1.25_f32.to_ne_bytes());
    let (_, columns) = decode_columns(&encoded, &[ColumnLayout::fixed(4)]).unwrap();
    let values = decode_column_datums(&columns[0], FieldType::new(FieldTypeCode::Float)).unwrap();
    assert_eq!(values, vec![Datum::new_real(1.25_f32 as f64)]);

    let mut encoded = Vec::new();
    encoded.extend_from_slice(&1_u32.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    encoded.extend_from_slice(&1.25_f64.to_ne_bytes());
    let (_, columns) = decode_columns(&encoded, &[ColumnLayout::fixed(8)]).unwrap();
    let values = decode_column_datums(&columns[0], FieldType::new(FieldTypeCode::Double)).unwrap();
    assert_eq!(values, vec![Datum::new_real(1.25)]);
}

#[test]
fn typed_chunk_scalar_variable_values_keep_collation_or_binary_bytes() {
    // Go source: Column.AppendString/AppendBytes append raw bytes and only
    // offsets carry the row boundaries. No UTF-8 validation happens here.
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&3_u32.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    for offset in [0_i64, 2, 2, 5] {
        encoded.extend_from_slice(&offset.to_ne_bytes());
    }
    encoded.extend_from_slice(b"abxyz");
    let (_, columns) = decode_columns(&encoded, &[ColumnLayout::variable()]).unwrap();

    let strings =
        decode_column_datums(&columns[0], FieldType::new(FieldTypeCode::Varchar)).unwrap();
    assert_eq!(strings[0], Datum::new_string(b"ab".to_vec()));
    assert_eq!(strings[1], Datum::new_string(Vec::<u8>::new()));
    assert_eq!(strings[2], Datum::new_string(b"xyz".to_vec()));

    let bytes = decode_column_datums(&columns[0], FieldType::new(FieldTypeCode::Blob)).unwrap();
    assert_eq!(bytes[0], Datum::new_bytes(b"ab".to_vec()));
    assert_eq!(bytes[1], Datum::new_bytes(Vec::<u8>::new()));
}

#[test]
fn typed_chunk_rejects_opaque_types_and_wrong_physical_layout() {
    let encoded = fixed_column(&[1], None);
    let (_, columns) = decode_columns(&encoded, &[ColumnLayout::fixed(8)]).unwrap();
    assert_eq!(
        decode_column_datums(&columns[0], FieldType::new(FieldTypeCode::Datetime)),
        Err(TypedColumnError::UnsupportedFieldType(
            FieldTypeCode::Datetime
        ))
    );

    let (_, columns) = decode_columns(&encoded, &[ColumnLayout::fixed(4)]).unwrap();
    assert!(matches!(
        decode_column_datums(&columns[0], FieldType::new(FieldTypeCode::Long)),
        Err(TypedColumnError::InvalidFixedDataLength { .. })
    ));
}

/// Go byte vectors from `pkg/util/chunk` (`Codec.Encode` over a 3-row chunk of
/// `BIGINT`, `VARCHAR`, `DOUBLE`, `DECIMAL(10,2)` whose row 1 is entirely
/// `NULL` and whose row 2 is `NULL` only in the variable-width and decimal
/// columns).
///
/// The layout the vectors pin down:
///   * A column header is `length` then `nullCount`, both `uint32` LE, and the
///     null bitmap is present only when `nullCount > 0`. Bit `1` means
///     NON-null, matching Go `Column.IsNull`.
///   * A fixed-width column still reserves a full element slot for a `NULL`
///     row, and Go leaves the previous row's `elemBuf` content there — row 1's
///     `BIGINT` slot holds a stale `7`, not a zero. A decoder must consult the
///     bitmap first and never interpret those bytes.
///   * A variable-width column gives a `NULL` row a zero-width offset span
///     (offsets `0,3,3,3` for `"abc"`, NULL, NULL), so its data region carries
///     nothing for the null rows.
const GO_CHUNK_WITH_NULLS: [u8; 239] = [
    0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05, 0x07, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0xf7, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x03, 0x00, 0x00,
    0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x61, 0x62, 0x63, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
    0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0xd0, 0xbf, 0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x02,
    0x02, 0x02, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0xfd, 0x43, 0x14, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x02, 0x02, 0x02, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00,
    0xfd, 0x43, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x02, 0x00, 0x0c,
    0x00, 0x00, 0x00, 0x00, 0xfd, 0x43, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

#[test]
fn typed_chunk_nulls_decode_across_fixed_and_variable_columns() {
    let layouts = [
        ColumnLayout::fixed(8),
        ColumnLayout::variable(),
        ColumnLayout::fixed(8),
        ColumnLayout::fixed(40),
    ];
    let (remainder, columns) = decode_columns(&GO_CHUNK_WITH_NULLS, &layouts).unwrap();
    assert!(remainder.is_empty(), "the Go chunk is fully consumed");

    // Bitmap bit 1 means non-null, so the encoded counts and the per-row
    // answers must agree with the Go chunk that produced them.
    assert_eq!(columns[0].null_count, 1);
    assert_eq!(columns[1].null_count, 2);
    assert_eq!(columns[3].null_count, 2);
    assert!(!columns[0].is_null(0).unwrap());
    assert!(columns[0].is_null(1).unwrap());
    assert!(!columns[0].is_null(2).unwrap());

    assert_eq!(
        decode_column_datums(&columns[0], FieldType::new(FieldTypeCode::LongLong)).unwrap(),
        vec![Datum::new_int(7), Datum::Null, Datum::new_int(-9)],
        "the stale `7` in the null row's fixed slot must not be read as a value"
    );
    assert_eq!(
        decode_column_datums(
            &columns[1],
            FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Utf8Mb4Bin),
        )
        .unwrap(),
        vec![
            Datum::new_collation_string(b"abc".to_vec(), Collation::Utf8Mb4Bin),
            Datum::Null,
            Datum::Null,
        ],
        "a zero-width offset span for a null row decodes as NULL, not as an empty string"
    );
    assert_eq!(
        decode_column_datums(&columns[2], FieldType::new(FieldTypeCode::Double)).unwrap(),
        vec![Datum::new_real(1.5), Datum::Null, Datum::new_real(-0.25)]
    );
    let decimals =
        decode_column_datums(&columns[3], FieldType::new(FieldTypeCode::NewDecimal)).unwrap();
    assert_eq!(decimals[1], Datum::Null);
    assert_eq!(decimals[2], Datum::Null);
    assert_eq!(decimals[0].to_bytes().unwrap(), b"12.34");
}

/// Go byte vector from `codec.EncodeValue(7, NULL, "abc", NULL)`, the other
/// read layout: the datum row carried in `Chunk.rows_data`. A `NULL` cell is
/// the bare `NilFlag` byte with no payload, so it costs one byte and cannot be
/// confused with a zero-length string (flag `0x02`, length `0`).
const GO_DATUM_ROW_WITH_NULLS: [u8; 9] = [
    0x08, 0x0e, 0x00, 0x02, 0x06, 0x61, 0x62, 0x63, 0x00,
];

#[test]
fn datum_row_nulls_decode_as_the_bare_nil_flag() {
    let (remainder, row) = tidb_codec::decode_default_row(&GO_DATUM_ROW_WITH_NULLS, 4).unwrap();
    assert!(remainder.is_empty(), "the Go datum row is fully consumed");
    let decoded = row
        .into_iter()
        .map(|value| value.decode_datum().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        decoded,
        vec![
            Datum::new_int(7),
            Datum::Null,
            Datum::new_bytes(b"abc".to_vec()),
            Datum::Null,
        ]
    );
}
