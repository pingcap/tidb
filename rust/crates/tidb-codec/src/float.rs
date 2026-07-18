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

use crate::number::{decode_uint, decode_uint_desc, encode_uint, encode_uint_desc};
use crate::CodecError;

const SIGN_MASK: u64 = 1 << 63;

/// Appends an ascending mem-comparable IEEE-754 double.
pub fn encode_float(buffer: &mut Vec<u8>, value: f64) {
    encode_uint(buffer, encode_float_to_comparable_uint(value));
}

/// Decodes one ascending mem-comparable IEEE-754 double.
pub fn decode_float(input: &[u8]) -> Result<(&[u8], f64), CodecError> {
    decode_uint(input).map(|(remain, value)| (remain, decode_comparable_uint_to_float(value)))
}

/// Appends a descending mem-comparable IEEE-754 double.
pub fn encode_float_desc(buffer: &mut Vec<u8>, value: f64) {
    encode_uint_desc(buffer, encode_float_to_comparable_uint(value));
}

/// Decodes one descending mem-comparable IEEE-754 double.
pub fn decode_float_desc(input: &[u8]) -> Result<(&[u8], f64), CodecError> {
    decode_uint_desc(input).map(|(remain, value)| (remain, decode_comparable_uint_to_float(value)))
}

fn encode_float_to_comparable_uint(value: f64) -> u64 {
    let bits = value.to_bits();
    if value >= 0.0 {
        bits | SIGN_MASK
    } else {
        !bits
    }
}

fn decode_comparable_uint_to_float(mut value: u64) -> f64 {
    value = if value & SIGN_MASK != 0 {
        value & !SIGN_MASK
    } else {
        !value
    };
    f64::from_bits(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    /// Exact row translation of `pkg/util/codec/codec_test.go::TestFloatCodec`.
    #[test]
    fn float_codec_source_rows_round_trip_and_order() {
        let values = [
            -1.0,
            0.0,
            1.0,
            f64::MAX,
            f32::MAX as f64,
            f32::from_bits(1) as f64,
            f64::from_bits(1),
            f64::NEG_INFINITY,
            f64::INFINITY,
        ];
        for value in values {
            let mut ascending = Vec::new();
            encode_float(&mut ascending, value);
            let (remain, decoded) = decode_float(&ascending).unwrap();
            assert!(remain.is_empty());
            assert_eq!(decoded, value);

            let mut descending = Vec::new();
            encode_float_desc(&mut descending, value);
            let (remain, decoded) = decode_float_desc(&descending).unwrap();
            assert!(remain.is_empty());
            assert_eq!(decoded, value);
        }

        let comparisons = [
            (1.0, -1.0, Ordering::Greater),
            (1.0, 0.0, Ordering::Greater),
            (0.0, -1.0, Ordering::Greater),
            (0.0, 0.0, Ordering::Equal),
            (f64::MAX, 1.0, Ordering::Greater),
            (f32::MAX as f64, f64::MAX, Ordering::Less),
            (f64::MAX, 0.0, Ordering::Greater),
            (f64::MAX, f64::from_bits(1), Ordering::Greater),
            (f64::NEG_INFINITY, 0.0, Ordering::Less),
            (f64::INFINITY, 0.0, Ordering::Greater),
            (f64::NEG_INFINITY, f64::INFINITY, Ordering::Less),
        ];
        for (left, right, expected) in comparisons {
            let mut left_encoded = Vec::new();
            let mut right_encoded = Vec::new();
            encode_float(&mut left_encoded, left);
            encode_float(&mut right_encoded, right);
            assert_eq!(left_encoded.cmp(&right_encoded), expected);

            left_encoded.clear();
            right_encoded.clear();
            encode_float_desc(&mut left_encoded, left);
            encode_float_desc(&mut right_encoded, right);
            assert_eq!(left_encoded.cmp(&right_encoded), expected.reverse());
        }
    }
}
