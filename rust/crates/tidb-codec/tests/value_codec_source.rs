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

//! Source-shaped byte-boundary coverage for `pkg/util/codec.EncodeValue`.

use tidb_codec::{
    decode_default_row, decode_default_rows, decode_value, CodecError, RawValue,
    VALUE_COMPACT_BYTES_FLAG, VALUE_FLOAT_FLAG, VALUE_INT_FLAG, VALUE_JSON_FLAG, VALUE_NIL_FLAG,
    VALUE_UVARINT_FLAG, VALUE_VARINT_FLAG,
};

#[test]
fn encode_value_source_rows_preserve_default_row_boundaries() {
    // Go source: pkg/util/codec/codec.go::{EncodeValue, encodeSignedInt,
    // encodeUnsignedInt, encodeBytes}. The stream is two rows of four values:
    // NULL, -1, 300, compact bytes "abc" and then NULL, 1, 0, compact bytes
    // empty. No Datum is constructed by the Rust boundary decoder.
    let encoded = [
        VALUE_NIL_FLAG,
        VALUE_VARINT_FLAG,
        1,
        VALUE_UVARINT_FLAG,
        0xac,
        0x02,
        VALUE_COMPACT_BYTES_FLAG,
        0x06,
        b'a',
        b'b',
        b'c',
        VALUE_NIL_FLAG,
        VALUE_VARINT_FLAG,
        0x02,
        VALUE_UVARINT_FLAG,
        0x00,
        VALUE_COMPACT_BYTES_FLAG,
        0x00,
    ];

    let (remain, first) = decode_default_row(&encoded, 4).expect("first default row");
    assert_eq!(
        first[0],
        RawValue {
            flag: 0,
            payload: &[]
        }
    );
    assert_eq!(
        first[1],
        RawValue {
            flag: 8,
            payload: &[1]
        }
    );
    assert_eq!(
        first[2],
        RawValue {
            flag: 9,
            payload: &[0xac, 0x02]
        }
    );
    assert_eq!(
        first[3],
        RawValue {
            flag: VALUE_COMPACT_BYTES_FLAG,
            payload: &[0x06, b'a', b'b', b'c'],
        }
    );

    let (remain, second) = decode_default_row(remain, 4).expect("second default row");
    assert!(remain.is_empty());
    assert_eq!(second[0].flag, VALUE_NIL_FLAG);
    assert_eq!(second[1].payload, &[0x02]);
    assert_eq!(second[2].payload, &[0x00]);
    assert_eq!(second[3].payload, &[0x00]);

    let rows = decode_default_rows(&encoded, 4).expect("all default rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], first);
    assert_eq!(rows[1], second);
}

#[test]
fn encode_value_source_fixed_float_and_leftover_are_framed_exactly() {
    // Go EncodeFloat(1.0) is the sortable uint bits 0xbff0... in big endian.
    let encoded = [
        VALUE_FLOAT_FLAG,
        0xbf,
        0xf0,
        0,
        0,
        0,
        0,
        0,
        0,
        VALUE_INT_FLAG,
        0x80,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0x7f,
    ];
    let (remain, value) = decode_value(&encoded).expect("float");
    assert_eq!(value.flag, VALUE_FLOAT_FLAG);
    assert_eq!(value.payload, &encoded[1..9]);
    let (remain, value) = decode_value(remain).expect("fixed integer");
    assert_eq!(value.flag, VALUE_INT_FLAG);
    assert_eq!(value.payload, &encoded[10..18]);
    assert_eq!(remain, &encoded[18..]);
}

#[test]
fn encode_value_source_errors_preserve_malformed_payloads() {
    assert_eq!(
        decode_value(&[VALUE_JSON_FLAG, 0xff]),
        Err(CodecError::InvalidEncoding("unknown JSON type code"))
    );
    assert_eq!(
        decode_value(&[VALUE_VARINT_FLAG, 0x80]),
        Err(CodecError::InsufficientBytes)
    );
    assert_eq!(
        decode_default_rows(&[VALUE_NIL_FLAG], 0),
        Err(CodecError::InvalidEncoding("zero-column default row"))
    );
}
