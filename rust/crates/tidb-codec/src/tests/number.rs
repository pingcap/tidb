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

#[test]
fn test_number_codec_source_rows() {
    let values = [
        i64::MIN,
        i64::from(i32::MIN),
        i64::from(i16::MIN),
        i64::from(i8::MIN),
        0,
        i64::from(i8::MAX),
        i64::from(i16::MAX),
        i64::from(i32::MAX),
        i64::MAX,
        (1_i64 << 47) - 1,
        -(1_i64 << 47),
        (1_i64 << 23) - 1,
        -(1_i64 << 23),
        (1_i64 << 33) - 1,
        -(1_i64 << 33),
        (1_i64 << 55) - 1,
        -(1_i64 << 55),
        1,
        -1,
    ];
    let encoded: Vec<Vec<u8>> = values
        .iter()
        .map(|value| {
            let mut bytes = Vec::new();
            encode_int(&mut bytes, *value);
            let (remain, decoded) = decode_int(&bytes).unwrap();
            assert!(remain.is_empty());
            assert_eq!(decoded, *value);

            let mut descending = Vec::new();
            encode_int_desc(&mut descending, *value);
            let (remain, decoded) = decode_int_desc(&descending).unwrap();
            assert!(remain.is_empty());
            assert_eq!(decoded, *value);

            let mut variable = Vec::new();
            encode_varint(&mut variable, *value);
            let (remain, decoded) = decode_varint(&variable).unwrap();
            assert!(remain.is_empty());
            assert_eq!(decoded, *value);

            let mut comparable = Vec::new();
            encode_comparable_varint(&mut comparable, *value);
            let (remain, decoded) = decode_comparable_varint(&comparable).unwrap();
            assert!(remain.is_empty());
            assert_eq!(decoded, *value);
            bytes
        })
        .collect();
    assert_eq!(encoded.len(), values.len());

    let unsigned = [
        0,
        u64::from(u8::MAX),
        u64::from(u16::MAX),
        u64::from(u32::MAX),
        u64::MAX,
        (1_u64 << 24) - 1,
        (1_u64 << 48) - 1,
        (1_u64 << 56) - 1,
        1,
        i16::MAX as u64,
        i8::MAX as u64,
        i32::MAX as u64,
        i64::MAX as u64,
    ];
    for value in unsigned {
        let mut fixed = Vec::new();
        encode_uint(&mut fixed, value);
        assert_eq!(decode_uint(&fixed).unwrap(), (&[][..], value));

        let mut descending = Vec::new();
        encode_uint_desc(&mut descending, value);
        assert_eq!(decode_uint_desc(&descending).unwrap(), (&[][..], value));

        let mut variable = Vec::new();
        encode_uvarint(&mut variable, value);
        assert_eq!(decode_uvarint(&variable).unwrap(), (&[][..], value));

        let mut comparable = Vec::new();
        encode_comparable_uvarint(&mut comparable, value);
        assert_eq!(
            decode_comparable_uvarint(&comparable).unwrap(),
            (&[][..], value)
        );
    }

    let mut sequence = Vec::new();
    encode_comparable_varint(&mut sequence, -1);
    encode_comparable_uvarint(&mut sequence, 1);
    encode_comparable_varint(&mut sequence, 2);
    let (sequence, first) = decode_comparable_varint(&sequence).unwrap();
    let (sequence, second) = decode_comparable_uvarint(sequence).unwrap();
    let (sequence, third) = decode_comparable_varint(sequence).unwrap();
    assert_eq!((first, second, third), (-1, 1, 2));
    assert!(sequence.is_empty());
}

#[test]
fn test_number_order_source_rows() {
    let signed = [
        (-1, 1, Ordering::Less),
        (i64::MAX, i64::MIN, Ordering::Greater),
        (i64::MAX, i64::from(i32::MAX), Ordering::Greater),
        (i64::from(i32::MIN), i64::from(i16::MAX), Ordering::Less),
        (i64::MIN, i64::from(i8::MAX), Ordering::Less),
        (0, i64::from(i8::MAX), Ordering::Less),
        (i64::from(i8::MIN), 0, Ordering::Less),
        (i64::from(i16::MIN), i64::from(i16::MAX), Ordering::Less),
        (1, -1, Ordering::Greater),
        (1, 0, Ordering::Greater),
        (-1, 0, Ordering::Less),
        (0, 0, Ordering::Equal),
        (i64::from(i16::MAX), i64::from(i16::MAX), Ordering::Equal),
    ];
    for (left, right, expected) in signed {
        let mut left_fixed = Vec::new();
        let mut right_fixed = Vec::new();
        encode_int(&mut left_fixed, left);
        encode_int(&mut right_fixed, right);
        assert_eq!(left_fixed.cmp(&right_fixed), expected);

        left_fixed.clear();
        right_fixed.clear();
        encode_int_desc(&mut left_fixed, left);
        encode_int_desc(&mut right_fixed, right);
        assert_eq!(left_fixed.cmp(&right_fixed), expected.reverse());

        left_fixed.clear();
        right_fixed.clear();
        encode_comparable_varint(&mut left_fixed, left);
        encode_comparable_varint(&mut right_fixed, right);
        assert_eq!(left_fixed.cmp(&right_fixed), expected);
    }

    let unsigned = [
        (0, 0, Ordering::Equal),
        (1, 0, Ordering::Greater),
        (0, 1, Ordering::Less),
        (u64::from(u8::MAX), u64::from(u16::MAX), Ordering::Less),
        (u64::from(u32::MAX), i32::MAX as u64, Ordering::Greater),
        (u64::from(u8::MAX), i8::MAX as u64, Ordering::Greater),
        (u64::from(u16::MAX), i32::MAX as u64, Ordering::Less),
        (u64::MAX, i64::MAX as u64, Ordering::Greater),
        (i64::MAX as u64, u64::from(u32::MAX), Ordering::Greater),
        (u64::MAX, 0, Ordering::Greater),
        (0, u64::MAX, Ordering::Less),
    ];
    for (left, right, expected) in unsigned {
        let mut left_fixed = Vec::new();
        let mut right_fixed = Vec::new();
        encode_uint(&mut left_fixed, left);
        encode_uint(&mut right_fixed, right);
        assert_eq!(left_fixed.cmp(&right_fixed), expected);

        left_fixed.clear();
        right_fixed.clear();
        encode_uint_desc(&mut left_fixed, left);
        encode_uint_desc(&mut right_fixed, right);
        assert_eq!(left_fixed.cmp(&right_fixed), expected.reverse());

        left_fixed.clear();
        right_fixed.clear();
        encode_comparable_uvarint(&mut left_fixed, left);
        encode_comparable_uvarint(&mut right_fixed, right);
        assert_eq!(left_fixed.cmp(&right_fixed), expected);
    }
}

#[test]
fn comparable_varints_cover_source_boundaries() {
    for value in [
        i64::MIN,
        -0x1_0000,
        -256,
        -255,
        -1,
        0,
        239,
        240,
        255,
        256,
        i64::MAX,
    ] {
        let mut encoded = Vec::new();
        encode_comparable_varint(&mut encoded, value);
        let (remain, decoded) = decode_comparable_varint(&encoded).unwrap();
        assert!(remain.is_empty());
        assert_eq!(decoded, value);
    }
    for value in [0, 239, 240, 255, 256, u64::MAX] {
        let mut encoded = Vec::new();
        encode_comparable_uvarint(&mut encoded, value);
        let (remain, decoded) = decode_comparable_uvarint(&encoded).unwrap();
        assert!(remain.is_empty());
        assert_eq!(decoded, value);
    }
    let mut negative_255 = Vec::new();
    encode_comparable_varint(&mut negative_255, -255);
    assert_eq!(negative_255, [7, 1]);
    let mut negative_256 = Vec::new();
    encode_comparable_varint(&mut negative_256, -256);
    assert_eq!(negative_256, [6, 0xff, 0]);
}
