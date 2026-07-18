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

//! Source-shaped tests for `pkg/util/rowcodec/{common,row}.go` framing.

use tidb_codec::{
    is_new_format, is_row_key, ColumnLookup, RowCodecError, RowLayout, CHECKSUM_FLAG_EXTRA,
    ROW_CODEC_VERSION, ROW_FLAG_CHECKSUM, ROW_FLAG_LARGE,
};

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

#[test]
fn row_header_metadata_offsets_and_column_lookup_follow_source_layout() {
    // Go row.go's format is: version, flags, not-null count, null count,
    // sorted IDs (not-null first), sorted end offsets, then opaque data.
    let mut encoded = vec![ROW_CODEC_VERSION, 0];
    put_u16(&mut encoded, 2);
    put_u16(&mut encoded, 2);
    encoded.extend_from_slice(&[1, 3, 5, 9]);
    put_u16(&mut encoded, 2);
    put_u16(&mut encoded, 4);
    encoded.extend_from_slice(b"abcd");
    encoded.extend_from_slice(b"suffix");

    let (row, remainder) = RowLayout::parse(&encoded).expect("small row framing");
    assert_eq!(remainder, b"suffix");
    assert!(!row.header().is_large());
    assert_eq!(row.header().not_null_count(), 2);
    assert_eq!(row.header().null_count(), 2);
    assert_eq!(row.not_null_column_ids(), &[1, 3]);
    assert_eq!(row.null_column_ids(), &[5, 9]);
    assert_eq!(row.offsets(), &[2, 4]);
    assert_eq!(row.data(), b"abcd");
    assert_eq!(row.find_column(1), ColumnLookup::NotNull(0));
    assert_eq!(row.find_column(3), ColumnLookup::NotNull(1));
    assert_eq!(row.find_column(5), ColumnLookup::Null);
    assert_eq!(row.find_column(7), ColumnLookup::Missing);
    assert!(!row.column_is_null(1, true));
    assert!(row.column_is_null(5, false));
    assert!(row.column_is_null(7, true));
    assert!(!row.column_is_null(7, false));
    assert_eq!(row.value(0), Ok(&b"ab"[..]));
    assert_eq!(row.value(1), Ok(&b"cd"[..]));
    assert_eq!(
        row.value_range(2),
        Err(RowCodecError::ValueIndexOutOfRange { index: 2, count: 2 })
    );
}

#[test]
fn large_row_and_checksum_metadata_preserve_width_and_trailer_boundaries() {
    let mut encoded = vec![ROW_CODEC_VERSION, ROW_FLAG_LARGE | ROW_FLAG_CHECKSUM];
    put_u16(&mut encoded, 1);
    put_u16(&mut encoded, 1);
    put_u32(&mut encoded, 300);
    put_u32(&mut encoded, 70000);
    put_u32(&mut encoded, 3);
    encoded.extend_from_slice(b"xyz");
    encoded.push(CHECKSUM_FLAG_EXTRA | 2);
    put_u32(&mut encoded, 0x1122_3344);
    put_u32(&mut encoded, 0xaabb_ccdd);
    encoded.extend_from_slice(b"suffix");

    let (row, remainder) = RowLayout::parse(&encoded).expect("large row framing");
    assert_eq!(remainder, b"suffix");
    assert!(row.header().is_large());
    assert!(row.header().has_checksum());
    assert_eq!(row.header().column_id_width(), 4);
    assert_eq!(row.header().offset_width(), 4);
    assert_eq!(row.column_ids(), &[300, 70000]);
    assert_eq!(row.find_column(300), ColumnLookup::NotNull(0));
    assert_eq!(row.find_column(70000), ColumnLookup::Null);
    assert_eq!(row.value(0), Ok(&b"xyz"[..]));
    let checksum = row.checksum().expect("checksum metadata");
    assert_eq!(checksum.version(), 2);
    assert_eq!(checksum.checksum(), 0x1122_3344);
    assert_eq!(checksum.extra_checksum(), Some(0xaabb_ccdd));
}

#[test]
fn common_row_format_predicates_and_malformed_boundaries_are_explicit() {
    assert!(is_new_format(&[ROW_CODEC_VERSION]));
    assert!(!is_new_format(&[]));
    assert!(!is_new_format(&[ROW_CODEC_VERSION - 1]));
    assert!(!is_row_key(b"bt"));
    assert!(!is_row_key(b"tr"));
    let mut row_key = vec![b't'; 19];
    row_key[10] = b'r';
    assert!(is_row_key(&row_key));
    row_key[10] = b'i';
    assert!(!is_row_key(&row_key));

    assert_eq!(
        RowLayout::parse(&[ROW_CODEC_VERSION - 1, 0, 0, 0, 0, 0]),
        Err(RowCodecError::InvalidCodecVersion {
            found: ROW_CODEC_VERSION - 1
        })
    );
    assert!(matches!(
        RowLayout::parse(&[ROW_CODEC_VERSION, 0, 1, 0, 0]),
        Err(RowCodecError::InsufficientBytes {
            section: "row header",
            ..
        })
    ));

    let mut decreasing = vec![ROW_CODEC_VERSION, 0];
    put_u16(&mut decreasing, 2);
    put_u16(&mut decreasing, 0);
    decreasing.extend_from_slice(&[1, 2]);
    put_u16(&mut decreasing, 3);
    put_u16(&mut decreasing, 2);
    assert_eq!(
        RowLayout::parse(&decreasing),
        Err(RowCodecError::InvalidOffset { index: 1, value: 2 })
    );

    let mut bad_checksum = vec![ROW_CODEC_VERSION, ROW_FLAG_CHECKSUM, 0, 0, 0, 0];
    bad_checksum.push(3);
    assert_eq!(
        RowLayout::parse(&bad_checksum),
        Err(RowCodecError::InvalidChecksumVersion { version: 3 })
    );
}
