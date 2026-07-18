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

//! Source-shaped tests for the dependency-closed part of
//! `pkg/util/rowcodec/decoder.go`.

use tidb_codec::{
    decode_raw_int, decode_raw_uint, encode_raw_int, encode_raw_row, encode_raw_uint, RawRowValue,
    RowDecodeError, RowDecoder, RowLayout, ROW_CODEC_VERSION,
};

#[test]
fn raw_decoder_looks_up_sorted_partitions_without_typed_schema_state() {
    let mut encoded = Vec::new();
    encode_raw_row(
        &[
            tidb_codec::RawRowColumn {
                id: 8,
                value: Some(b"right"),
            },
            tidb_codec::RawRowColumn { id: 2, value: None },
            tidb_codec::RawRowColumn {
                id: 4,
                value: Some(b"left"),
            },
            tidb_codec::RawRowColumn { id: 1, value: None },
        ],
        &mut encoded,
    )
    .expect("source row encoding");
    encoded.extend_from_slice(b"suffix");

    let (decoder, remainder) = RowDecoder::parse(&encoded).expect("source row decoding");
    assert_eq!(remainder, b"suffix");
    assert!(matches!(
        decoder.column(4),
        Ok(RawRowValue::NotNull {
            index: 0,
            bytes
        }) if bytes == b"left"
    ));
    assert!(matches!(
        decoder.column(8),
        Ok(RawRowValue::NotNull {
            index: 1,
            bytes
        }) if bytes == b"right"
    ));
    assert_eq!(decoder.column(1), Ok(RawRowValue::Null));
    assert_eq!(decoder.column(2), Ok(RawRowValue::Null));
    assert_eq!(decoder.column(3), Ok(RawRowValue::Missing));
    assert_eq!(decoder.column(-1), Ok(RawRowValue::Missing));
    assert_eq!(
        decoder.layout().header(),
        RowLayout::parse(&encoded).unwrap().0.header()
    );
}

#[test]
fn raw_decoder_preserves_compact_signed_and_unsigned_widths() {
    let signed = [i64::MIN, -32_769, -129, -1, 0, 127, 128, 32_768, i64::MAX];
    for value in signed {
        let mut encoded = Vec::new();
        encode_raw_int(&mut encoded, value);
        assert_eq!(decode_raw_int(&encoded), Ok(value), "signed value {value}");
    }

    let unsigned = [0_u64, 255, 256, 65_535, 65_536, u64::MAX];
    for value in unsigned {
        let mut encoded = Vec::new();
        encode_raw_uint(&mut encoded, value);
        assert_eq!(
            decode_raw_uint(&encoded),
            Ok(value),
            "unsigned value {value}"
        );
    }
}

#[test]
fn raw_decoder_rejects_malformed_compact_integer_boundaries() {
    for width in [0, 3, 5, 6, 7, 9] {
        let payload = vec![0_u8; width];
        assert_eq!(
            decode_raw_int(&payload),
            Err(RowDecodeError::InvalidIntegerWidth {
                signed: true,
                width,
            })
        );
        assert_eq!(
            decode_raw_uint(&payload),
            Err(RowDecodeError::InvalidIntegerWidth {
                signed: false,
                width,
            })
        );
    }
}

#[test]
fn raw_decoder_keeps_row_layout_boundary_errors_typed() {
    assert_eq!(
        RowDecoder::parse(&[ROW_CODEC_VERSION - 1, 0, 0, 0, 0, 0]),
        Err(RowDecodeError::Layout(
            tidb_codec::RowCodecError::InvalidCodecVersion {
                found: ROW_CODEC_VERSION - 1,
            }
        ))
    );

    let mut truncated = vec![ROW_CODEC_VERSION, 0, 1, 0, 0, 0, 7];
    truncated.extend_from_slice(&1_u16.to_le_bytes());
    assert!(matches!(
        RowDecoder::parse(&truncated),
        Err(RowDecodeError::Layout(
            tidb_codec::RowCodecError::InsufficientBytes {
                section: "row data",
                ..
            }
        ))
    ));
}
