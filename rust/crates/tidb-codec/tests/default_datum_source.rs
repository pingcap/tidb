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

//! Source-backed typed scalar checks for `pkg/util/codec.DecodeOne`.

use tidb_codec::{
    decode_value, encode_bytes, encode_compact_bytes, encode_float, encode_int, encode_uint,
    encode_varint, CodecError, RawValue, VALUE_BYTES_FLAG, VALUE_COMPACT_BYTES_FLAG,
    VALUE_DECIMAL_FLAG, VALUE_FLOAT_FLAG, VALUE_INT_FLAG, VALUE_MAX_FLAG, VALUE_NIL_FLAG,
    VALUE_UINT_FLAG, VALUE_UVARINT_FLAG, VALUE_VARINT_FLAG,
};
use tidb_datatype::{Datum, Decimal};

fn decode(encoded: &[u8]) -> Result<Datum, CodecError> {
    let (remain, raw) = decode_value(encoded)?;
    assert!(remain.is_empty());
    raw.decode_datum()
}

#[test]
fn decode_one_source_scalar_rows_materialize_losslessly() {
    // Go source: pkg/util/codec/codec.go::DecodeOne (1342-1415).  The
    // schema-independent subset maps exactly to the currently ported Datum
    // variants; no temporal, JSON, vector, or enum/set conversion is guessed.
    assert_eq!(decode(&[VALUE_NIL_FLAG]).unwrap(), Datum::Null);
    assert_eq!(
        decode(&[VALUE_MAX_FLAG]),
        Err(CodecError::UnsupportedValueTag(VALUE_MAX_FLAG))
    );

    let mut encoded = vec![VALUE_INT_FLAG];
    encode_int(&mut encoded, -42);
    assert_eq!(decode(&encoded).unwrap(), Datum::new_int(-42));

    let mut encoded = vec![VALUE_UINT_FLAG];
    encode_uint(&mut encoded, u64::MAX);
    assert_eq!(decode(&encoded).unwrap(), Datum::new_uint(u64::MAX));

    let mut encoded = vec![VALUE_VARINT_FLAG];
    encode_varint(&mut encoded, -123);
    assert_eq!(decode(&encoded).unwrap(), Datum::new_int(-123));

    let encoded = vec![VALUE_UVARINT_FLAG, 0xac, 0x02];
    assert_eq!(decode(&encoded).unwrap(), Datum::new_uint(300));

    let mut encoded = vec![VALUE_FLOAT_FLAG];
    encode_float(&mut encoded, 1.25);
    assert_eq!(decode(&encoded).unwrap(), Datum::new_real(1.25));

    let mut encoded = vec![VALUE_BYTES_FLAG];
    encode_bytes(&mut encoded, b"binary\0bytes");
    assert_eq!(
        decode(&encoded).unwrap(),
        Datum::new_bytes(b"binary\0bytes")
    );

    let mut encoded = vec![VALUE_COMPACT_BYTES_FLAG];
    encode_compact_bytes(&mut encoded, b"compact");
    assert_eq!(decode(&encoded).unwrap(), Datum::new_bytes(b"compact"));

    let decimal = Decimal::from_literal("12.340");
    let mut encoded = vec![VALUE_DECIMAL_FLAG];
    tidb_codec::encode_decimal_fixed(&mut encoded, &decimal, 5, 3).unwrap();
    assert_eq!(decode(&encoded).unwrap(), Datum::new_decimal(decimal));
}

#[test]
fn decode_one_source_rejects_unported_and_malformed_typed_values() {
    assert_eq!(
        RawValue {
            flag: tidb_codec::VALUE_DURATION_FLAG,
            payload: &[0; 8],
        }
        .decode_datum(),
        Err(CodecError::UnsupportedValueTag(
            tidb_codec::VALUE_DURATION_FLAG
        ))
    );
    assert_eq!(
        RawValue {
            flag: tidb_codec::VALUE_JSON_FLAG,
            payload: &[],
        }
        .decode_datum(),
        Err(CodecError::UnsupportedValueTag(tidb_codec::VALUE_JSON_FLAG))
    );
    assert_eq!(
        RawValue {
            flag: VALUE_NIL_FLAG,
            payload: &[1],
        }
        .decode_datum(),
        Err(CodecError::InvalidEncoding("NULL value has trailing bytes"))
    );
    assert_eq!(
        RawValue {
            flag: VALUE_INT_FLAG,
            payload: &[0; 7],
        }
        .decode_datum(),
        Err(CodecError::InsufficientBytes)
    );
}
