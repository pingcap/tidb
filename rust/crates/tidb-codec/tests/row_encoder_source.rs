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

//! Source-shaped tests for `pkg/util/rowcodec/encoder.go`.

use tidb_codec::{
    encode_raw_int, encode_raw_row, encode_raw_uint, ColumnLookup, RawRowColumn, RowEncodeError,
    RowLayout,
};

#[test]
fn row_encoder_uses_source_compact_little_endian_integer_widths() {
    let mut encoded = Vec::new();
    encode_raw_int(&mut encoded, -1);
    encode_raw_int(&mut encoded, 128);
    encode_raw_int(&mut encoded, 32_768);
    encode_raw_int(&mut encoded, i64::MIN);
    assert_eq!(
        encoded,
        [0xff, 0x80, 0x00, 0x00, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x80,]
    );

    encoded.clear();
    encode_raw_uint(&mut encoded, 0xff);
    encode_raw_uint(&mut encoded, 256);
    encode_raw_uint(&mut encoded, 65_536);
    encode_raw_uint(&mut encoded, u64::MAX);
    assert_eq!(
        encoded,
        [
            0xff, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff,
        ]
    );
}

#[test]
fn row_encoder_sorts_partitions_and_keeps_opaque_value_boundaries() {
    let mut encoded = vec![0xaa];
    encode_raw_row(
        &[
            RawRowColumn {
                id: 5,
                value: Some(b"xy"),
            },
            RawRowColumn { id: 1, value: None },
            RawRowColumn {
                id: 3,
                value: Some(b"z"),
            },
            RawRowColumn { id: 2, value: None },
        ],
        &mut encoded,
    )
    .expect("source row encoder");

    assert_eq!(encoded[0], 0xaa);
    let (row, remainder) = RowLayout::parse(&encoded[1..]).expect("encoded row layout");
    assert_eq!(remainder, b"");
    assert!(!row.header().is_large());
    assert_eq!(row.not_null_column_ids(), &[3, 5]);
    assert_eq!(row.null_column_ids(), &[1, 2]);
    assert_eq!(row.offsets(), &[1, 3]);
    assert_eq!(row.value(0), Ok(&b"z"[..]));
    assert_eq!(row.value(1), Ok(&b"xy"[..]));
    assert_eq!(row.find_column(1), ColumnLookup::Null);
    assert_eq!(row.find_column(5), ColumnLookup::NotNull(1));
}

#[test]
fn row_encoder_selects_large_metadata_for_ids_or_payload() {
    let mut encoded = Vec::new();
    encode_raw_row(
        &[
            RawRowColumn {
                id: 300,
                value: Some(b"abc"),
            },
            RawRowColumn {
                id: 70_000,
                value: None,
            },
        ],
        &mut encoded,
    )
    .expect("large IDs select four-byte metadata");
    let (row, remainder) = RowLayout::parse(&encoded).expect("large row layout");
    assert!(remainder.is_empty());
    assert!(row.header().is_large());
    assert_eq!(row.column_ids(), &[300, 70_000]);
    assert_eq!(row.offsets(), &[3]);
    assert_eq!(row.value(0), Ok(&b"abc"[..]));

    let large_payload = vec![b'x'; usize::from(u16::MAX) + 1];
    encoded.clear();
    encode_raw_row(
        &[RawRowColumn {
            id: 1,
            value: Some(&large_payload),
        }],
        &mut encoded,
    )
    .expect("payload length selects four-byte offsets");
    let (row, _) = RowLayout::parse(&encoded).expect("large payload row layout");
    assert!(row.header().is_large());
    assert_eq!(row.offsets(), &[65_536]);
    assert_eq!(row.value(0).expect("large value").len(), 65_536);
}

#[test]
fn row_encoder_rejects_header_count_overflow() {
    let columns = vec![RawRowColumn { id: 1, value: None }; usize::from(u16::MAX) + 1];
    assert_eq!(
        encode_raw_row(&columns, &mut Vec::new()),
        Err(RowEncodeError::TooManyColumns {
            count: usize::from(u16::MAX) + 1,
        })
    );
}
