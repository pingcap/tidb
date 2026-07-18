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

//! Source-shaped coverage for `pkg/util/chunk/codec.go`.

use tidb_codec::{decode_columns, ColumnCodecError, ColumnLayout};
use tidb_datatype::{FieldType, FieldTypeCode};

#[test]
fn chunk_get_fixed_len_source_mapping_is_explicit_and_exhaustive() {
    // Go source: pkg/util/chunk/codec.go:getFixedLen. These are physical
    // in-memory widths, not SQL display widths. Every source FieldTypeCode is
    // listed so a newly added code cannot silently inherit a fixed layout.
    let fixed = [
        (FieldTypeCode::Float, Some(4)),
        (FieldTypeCode::Tiny, Some(8)),
        (FieldTypeCode::Short, Some(8)),
        (FieldTypeCode::Int24, Some(8)),
        (FieldTypeCode::Long, Some(8)),
        (FieldTypeCode::LongLong, Some(8)),
        (FieldTypeCode::Double, Some(8)),
        (FieldTypeCode::Year, Some(8)),
        (FieldTypeCode::Duration, Some(8)),
        (FieldTypeCode::Date, Some(8)),
        (FieldTypeCode::Datetime, Some(8)),
        (FieldTypeCode::Timestamp, Some(8)),
        (FieldTypeCode::NewDecimal, Some(40)),
    ];
    for (code, width) in fixed {
        assert_eq!(ColumnLayout::for_field_type_code(code).fixed_width(), width);
        assert_eq!(
            ColumnLayout::for_field_type(&FieldType::new(code)).fixed_width(),
            width
        );
    }

    let variable = [
        FieldTypeCode::Unspecified,
        FieldTypeCode::NewDate,
        FieldTypeCode::Varchar,
        FieldTypeCode::Bit,
        FieldTypeCode::Json,
        FieldTypeCode::Enum,
        FieldTypeCode::Set,
        FieldTypeCode::TinyBlob,
        FieldTypeCode::MediumBlob,
        FieldTypeCode::LongBlob,
        FieldTypeCode::String,
        FieldTypeCode::Geometry,
        FieldTypeCode::VectorFloat32,
        FieldTypeCode::Null,
        FieldTypeCode::VarString,
        FieldTypeCode::Blob,
        FieldTypeCode::Unknown(0xdd),
    ];
    for code in variable {
        assert_eq!(ColumnLayout::for_field_type_code(code).fixed_width(), None);
        assert_eq!(
            ColumnLayout::for_field_type(&FieldType::new(code)).fixed_width(),
            None
        );
    }
}

#[test]
fn chunk_codec_source_layout_preserves_fixed_and_variable_columns() {
    let mut encoded = Vec::new();

    // Go `encodeColumn`: length=3, nullCount=1, bitmap 101b, then three
    // fixed-width eight-byte values. A zero bit denotes SQL NULL.
    encoded.extend_from_slice(&3_u32.to_le_bytes());
    encoded.extend_from_slice(&1_u32.to_le_bytes());
    encoded.push(0b0000_0101);
    encoded.extend_from_slice(&1_u64.to_le_bytes());
    encoded.extend_from_slice(&2_u64.to_le_bytes());
    encoded.extend_from_slice(&3_u64.to_le_bytes());

    // Variable column: length=3, nullCount=0, offsets [0,1,3,3], data "abc".
    encoded.extend_from_slice(&3_u32.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    for offset in [0_i64, 1, 3, 3] {
        encoded.extend_from_slice(&offset.to_le_bytes());
    }
    encoded.extend_from_slice(b"abc");
    encoded.extend_from_slice(b"tail");

    let (remainder, columns) = decode_columns(
        &encoded,
        &[ColumnLayout::fixed(8), ColumnLayout::variable()],
    )
    .expect("source-shaped chunk columns");
    assert_eq!(remainder, b"tail");

    assert_eq!(columns[0].length, 3);
    assert_eq!(columns[0].null_count, 1);
    assert_eq!(columns[0].null_bitmap, Some(&encoded[8..9]));
    assert_eq!(columns[0].data.len(), 24);
    assert_eq!(columns[0].is_null(0), Ok(false));
    assert_eq!(columns[0].is_null(1), Ok(true));
    assert_eq!(columns[0].is_null(2), Ok(false));

    assert_eq!(columns[1].offsets, Some(vec![0, 1, 3, 3]));
    assert_eq!(columns[1].value(0), Ok(&b"a"[..]));
    assert_eq!(columns[1].value(1), Ok(&b"bc"[..]));
    assert_eq!(columns[1].value(2), Ok(&b""[..]));
}

#[test]
fn chunk_codec_source_rejects_ambiguous_or_truncated_boundaries() {
    let mut null_count_exceeds_rows = Vec::new();
    null_count_exceeds_rows.extend_from_slice(&1_u32.to_le_bytes());
    null_count_exceeds_rows.extend_from_slice(&2_u32.to_le_bytes());
    assert_eq!(
        decode_columns(&null_count_exceeds_rows, &[ColumnLayout::fixed(8)]),
        Err(ColumnCodecError::InvalidNullCount {
            column: 0,
            null_count: 2,
            length: 1,
        })
    );

    let mut fixed_truncated = Vec::new();
    fixed_truncated.extend_from_slice(&2_u32.to_le_bytes());
    fixed_truncated.extend_from_slice(&0_u32.to_le_bytes());
    fixed_truncated.extend_from_slice(&[0; 7]);
    assert!(matches!(
        decode_columns(&fixed_truncated, &[ColumnLayout::fixed(8)]),
        Err(ColumnCodecError::InsufficientBytes {
            column: 0,
            section: "data",
            ..
        })
    ));

    let mut decreasing_offsets = Vec::new();
    decreasing_offsets.extend_from_slice(&2_u32.to_le_bytes());
    decreasing_offsets.extend_from_slice(&0_u32.to_le_bytes());
    for offset in [0_i64, 2, 1] {
        decreasing_offsets.extend_from_slice(&offset.to_le_bytes());
    }
    assert!(matches!(
        decode_columns(&decreasing_offsets, &[ColumnLayout::variable()]),
        Err(ColumnCodecError::InvalidOffset {
            column: 0,
            offset_index: 2,
            value: 1,
        })
    ));

    let mut data_overrun = Vec::new();
    data_overrun.extend_from_slice(&1_u32.to_le_bytes());
    data_overrun.extend_from_slice(&0_u32.to_le_bytes());
    for offset in [0_i64, 2] {
        data_overrun.extend_from_slice(&offset.to_le_bytes());
    }
    data_overrun.push(b'x');
    assert!(matches!(
        decode_columns(&data_overrun, &[ColumnLayout::variable()]),
        Err(ColumnCodecError::InsufficientBytes {
            column: 0,
            section: "data",
            needed: 2,
            available: 1,
        })
    ));
}
