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

use crate::*;
use std::cmp::Ordering;
use tidb_datatype::{Datum, Decimal};

#[test]
fn datum_key_round_trips_and_preserves_order() {
    let values = vec![
        Datum::new_int(-101),
        Datum::new_string("abc"),
        Datum::new_bytes(vec![0xff, 0]),
        Datum::new_decimal(Decimal::from_literal("12.340").negate()),
        Datum::new_real(1.25),
        Datum::Null,
    ];
    let encoded = encode_key(&values).unwrap();
    let mut remain = encoded.as_slice();
    let mut decoded = Vec::new();
    while !remain.is_empty() {
        let (column, next) = cut_one(remain).unwrap();
        let (column_remain, datum) = decode_one(column).unwrap();
        assert!(column_remain.is_empty());
        decoded.push(datum);
        remain = next;
    }
    assert_eq!(decoded[0], values[0]);
    assert_eq!(decoded[1].as_raw_bytes(), values[1].as_raw_bytes());
    assert_eq!(decoded[2].as_raw_bytes(), values[2].as_raw_bytes());
    assert_eq!(decoded[3], values[3]);
    assert_eq!(decoded[4], values[4]);
    assert_eq!(decoded[5], values[5]);

    let min = encode_key(&[Datum::new_int(i64::MIN)]).unwrap();
    let zero = encode_key(&[Datum::new_int(0)]).unwrap();
    let max = encode_key(&[Datum::new_int(i64::MAX)]).unwrap();
    assert_eq!(min.cmp(&zero), Ordering::Less);
    assert_eq!(zero.cmp(&max), Ordering::Less);
}

/// Exact sentinel row from `pkg/util/codec/codec_test.go::TestCodecKeyCompare`.
#[test]
fn test_codec_key_compare_sentinel_vector() {
    let min = encode_key(&[Datum::min_not_null()]).unwrap();
    let max = encode_key(&[Datum::max_value()]).unwrap();

    assert_eq!(min, [BYTES_FLAG]);
    assert_eq!(max, [MAX_FLAG]);
    assert_eq!(min.cmp(&max), Ordering::Less);
}

/// Exact terminal-special-value loop from
/// `pkg/util/codec/codec_test.go::TestDecodeRange`. A bytes tag before a
/// payload is still an ordinary byte datum; only a final bare tag is the
/// `MinNotNull` sentinel.
#[test]
fn test_decode_range_distinguishes_payload_bytes_from_terminal_sentinels() {
    let ordinary = Datum::new_bytes(b"abc".to_vec());
    let mut encoded = encode_key(std::slice::from_ref(&ordinary)).unwrap();
    encoded.push(BYTES_FLAG);
    let (values, remain) = decode_range(&encoded, 2).unwrap();
    assert!(remain.is_empty());
    assert_eq!(values, [ordinary, Datum::min_not_null()]);

    for (flag, expected) in [
        (NIL_FLAG, Datum::Null),
        (BYTES_FLAG, Datum::min_not_null()),
        (MAX_FLAG, Datum::max_value()),
        (MAX_FLAG + 1, Datum::max_value()),
    ] {
        let encoded = [flag];
        let (values, remain) = decode_range(&encoded, 1).unwrap();
        assert!(remain.is_empty());
        assert_eq!(values, [expected]);
    }

    assert_eq!(
        decode_one(&[BYTES_FLAG]),
        Err(CodecError::InsufficientBytes)
    );
}

#[test]
fn test_cut_one_error_source_rows_and_owned_failure_modes() {
    assert_eq!(cut_one(&[]), Err(CodecError::InvalidEncoding("empty key")));
    assert_eq!(
        cut_one(&[UINT_FLAG, 0, 0, 0]),
        Err(CodecError::InsufficientBytes)
    );

    let invalid_marker = [BYTES_FLAG, 0, 0, 0, 0, 0, 0, 0, 0, 246];
    assert_eq!(
        cut_one(&invalid_marker),
        Err(CodecError::InvalidEncoding("invalid bytes marker"))
    );
    let invalid_padding = [BYTES_FLAG, 1, 0, 0, 0, 0, 0, 0, 1, 249];
    assert_eq!(
        cut_one(&invalid_padding),
        Err(CodecError::InvalidEncoding("invalid bytes padding"))
    );
    assert_eq!(
        cut_one(&[DECIMAL_FLAG, 0, 0]),
        Err(CodecError::DecimalOutOfRange)
    );
    assert_eq!(
        cut_one(&[MAX_FLAG]),
        Err(CodecError::InvalidEncoding("unknown datum flag"))
    );
}
