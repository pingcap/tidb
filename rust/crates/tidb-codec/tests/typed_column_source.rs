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
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

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
