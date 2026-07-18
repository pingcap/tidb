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

//! Fixed-schema rows from `pkg/util/codec/codec_test.go::TestDecimal`.

use std::cmp::Ordering;

use tidb_codec::{
    decimal_encoded_len, decode_decimal, decode_one, encode_decimal_fixed, CodecError, DECIMAL_FLAG,
};
use tidb_datatype::Decimal;

#[derive(Clone, Copy)]
enum SourceDecimal {
    Literal(&'static str),
    Int(i64),
    UInt(u64),
}

impl SourceDecimal {
    fn decimal(self) -> Decimal {
        match self {
            Self::Literal(value) => parse_decimal(value),
            Self::Int(value) => Decimal::from_int(value),
            Self::UInt(value) => Decimal::from_uint(value),
        }
    }
}

#[test]
fn fixed_precision_decimal_comparison_and_value_size_source_rows() {
    use SourceDecimal::{Int, Literal, UInt};

    let rows = [
        (Literal("1234"), Literal("123400"), Ordering::Less),
        (Literal("12340"), Literal("123400"), Ordering::Less),
        (Literal("1234"), Literal("1234.5"), Ordering::Less),
        (Literal("1234"), Literal("1234.0000"), Ordering::Equal),
        (Literal("1234"), Literal("12.34"), Ordering::Greater),
        (Literal("12.34"), Literal("12.35"), Ordering::Less),
        (Literal("0.12"), Literal("0.1234"), Ordering::Less),
        (Literal("0.1234"), Literal("12.3400"), Ordering::Less),
        (Literal("0.1234"), Literal("0.1235"), Ordering::Less),
        (Literal("0.123400"), Literal("12.34"), Ordering::Less),
        (Literal("12.34000"), Literal("12.34"), Ordering::Equal),
        (Literal("0.01234"), Literal("0.01235"), Ordering::Less),
        (Literal("0.1234"), Literal("0"), Ordering::Greater),
        (Literal("0.0000"), Literal("0"), Ordering::Equal),
        (Literal("0.0001"), Literal("0"), Ordering::Greater),
        (Literal("0.0001"), Literal("0.0000"), Ordering::Greater),
        (Literal("0"), Literal("-0.0000"), Ordering::Equal),
        (Literal("-0.0001"), Literal("0"), Ordering::Less),
        (Literal("-0.1234"), Literal("0"), Ordering::Less),
        (Literal("-0.1234"), Literal("-0.12"), Ordering::Less),
        (Literal("-0.12"), Literal("-0.1234"), Ordering::Greater),
        (Literal("-0.12"), Literal("-0.1200"), Ordering::Equal),
        (Literal("-0.1234"), Literal("0.1234"), Ordering::Less),
        (Literal("-1.234"), Literal("-12.34"), Ordering::Greater),
        (Literal("-0.1234"), Literal("-12.34"), Ordering::Greater),
        (Literal("-12.34"), Literal("1234"), Ordering::Less),
        (Literal("-12.34"), Literal("-12.35"), Ordering::Greater),
        (Literal("-0.01234"), Literal("-0.01235"), Ordering::Greater),
        (Literal("-1234"), Literal("-123400"), Ordering::Greater),
        (Literal("-12340"), Literal("-123400"), Ordering::Greater),
        (Int(-1), Int(1), Ordering::Less),
        (Int(i64::MAX), Int(i64::MIN), Ordering::Greater),
        (Int(i64::MAX), Int(i32::MAX.into()), Ordering::Greater),
        (Int(i32::MIN.into()), Int(i16::MAX.into()), Ordering::Less),
        (Int(i64::MIN), Int(i8::MAX.into()), Ordering::Less),
        (Int(0), Int(i8::MAX.into()), Ordering::Less),
        (Int(i8::MIN.into()), Int(0), Ordering::Less),
        (Int(i16::MIN.into()), Int(i16::MAX.into()), Ordering::Less),
        (Int(1), Int(-1), Ordering::Greater),
        (Int(1), Int(0), Ordering::Greater),
        (Int(-1), Int(0), Ordering::Less),
        (Int(0), Int(0), Ordering::Equal),
        (Int(i16::MAX.into()), Int(i16::MAX.into()), Ordering::Equal),
        (UInt(0), UInt(0), Ordering::Equal),
        (UInt(1), UInt(0), Ordering::Greater),
        (UInt(0), UInt(1), Ordering::Less),
        (
            UInt(u64::from(u8::MAX)),
            UInt(u64::from(u16::MAX)),
            Ordering::Less,
        ),
        (
            UInt(u64::from(u32::MAX)),
            UInt(i32::MAX as u64),
            Ordering::Greater,
        ),
        (
            UInt(u64::from(u8::MAX)),
            UInt(i8::MAX as u64),
            Ordering::Greater,
        ),
        (
            UInt(u64::from(u16::MAX)),
            UInt(i32::MAX as u64),
            Ordering::Less,
        ),
        (UInt(u64::MAX), UInt(i64::MAX as u64), Ordering::Greater),
        (
            UInt(i64::MAX as u64),
            UInt(u64::from(u32::MAX)),
            Ordering::Greater,
        ),
        (UInt(u64::MAX), UInt(0), Ordering::Greater),
        (UInt(0), UInt(u64::MAX), Ordering::Less),
    ];

    for (left, right, expected) in rows {
        let left = left.decimal();
        let right = right.decimal();
        let left_key = fixed_key(&left, 30, 6).unwrap();
        let right_key = fixed_key(&right, 30, 6).unwrap();
        assert_eq!(left_key.cmp(&right_key), expected);
        assert_eq!(
            left_key.len(),
            1 + decimal_encoded_len(&left, 30, 6).unwrap()
        );
        assert_eq!(
            right_key.len(),
            1 + decimal_encoded_len(&right, 30, 6).unwrap()
        );

        let (remain, decoded) = decode_one(&left_key).unwrap();
        assert!(remain.is_empty());
        assert_eq!(decoded.as_decimal(), Some(&left));
    }
}

#[test]
fn fixed_precision_float_derived_rows_are_ordered_and_sized() {
    let values = [
        -123.45,
        -123.40,
        -23.45,
        -1.43,
        -0.93,
        -0.4333,
        -0.068,
        -0.0099,
        0.0,
        0.001,
        0.0012,
        0.12,
        1.2,
        1.23,
        123.3,
        2424.242424,
    ];
    let encoded: Vec<Vec<u8>> = values
        .into_iter()
        .map(|value| {
            let decimal = parse_decimal(&value.to_string());
            let encoded = fixed_key(&decimal, 20, 6).unwrap();
            assert_eq!(
                encoded.len(),
                1 + decimal_encoded_len(&decimal, 20, 6).unwrap()
            );
            encoded
        })
        .collect();
    assert!(encoded.windows(2).all(|pair| pair[0] <= pair[1]));
}

#[test]
fn truncation_and_overflow_are_typed_for_the_caller_error_context() {
    let decimal = parse_decimal("-123.123456789");
    assert_eq!(
        fixed_key(&decimal, 20, 5),
        Err(CodecError::DecimalTruncated)
    );
    assert_eq!(
        fixed_key(&decimal, 12, 10),
        Err(CodecError::DecimalOverflow)
    );

    // `MyDecimal.WriteBin` reports the 8 -> 9 scale regrouping because both
    // shapes occupy four bytes but the target changes a partial group into a
    // full group. Scale 10 grows the payload and is lossless without it.
    let regrouped = parse_decimal("0.12345678");
    let mut encoded = Vec::new();
    assert_eq!(
        encode_decimal_fixed(&mut encoded, &regrouped, 10, 9),
        Err(CodecError::DecimalTruncated)
    );
    assert_eq!(decode_decimal(&encoded).unwrap().1, regrouped);
    assert!(fixed_key(&regrouped, 11, 10).is_ok());
}

#[test]
fn decimal_codec_and_frac_source_rows_round_trip_metadata_and_remainder() {
    // `pkg/util/codec/decimal_test.go::TestDecimalCodec` and `TestFrac`.
    let literals = [
        "123400", "1234", "12.34", "0.1234", "0.01234", "-0.1234", "-0.01234", "12.34", "-12.34",
        "0", "0", "0", "0", "3", "0.03",
    ];
    for literal in literals {
        let decimal = parse_decimal(literal);
        let mut encoded = vec![0xaa, 0xbb];
        encode_decimal_fixed(&mut encoded, &decimal, 0, 0).unwrap();
        let expected_precision = natural_precision(&decimal);
        assert_eq!(&encoded[..2], &[0xaa, 0xbb]);
        assert_eq!(usize::from(encoded[2]), expected_precision);
        assert_eq!(u32::from(encoded[3]), decimal.storage_scale());
        assert_eq!(
            encoded.len() - 2,
            decimal_encoded_len(&decimal, 0, 0).unwrap(),
        );

        encoded.extend_from_slice(&[0xaa, 0xbb]);
        let (remain, decoded, precision, scale) = decode_decimal(&encoded[2..]).unwrap();
        assert_eq!(remain, &[0xaa, 0xbb]);
        assert_eq!(usize::from(precision), expected_precision);
        assert_eq!(u32::from(scale), decimal.storage_scale());
        assert_eq!(decoded, decimal);
        assert_eq!(decoded.to_string(), decimal.to_string());
    }
}

fn fixed_key(decimal: &Decimal, precision: usize, scale: usize) -> Result<Vec<u8>, CodecError> {
    let mut output = vec![DECIMAL_FLAG];
    encode_decimal_fixed(&mut output, decimal, precision, scale)?;
    Ok(output)
}

fn parse_decimal(literal: &str) -> Decimal {
    match literal.strip_prefix('-') {
        Some(magnitude) => Decimal::from_literal(magnitude).negate(),
        None => Decimal::from_literal(literal),
    }
}

fn natural_precision(decimal: &Decimal) -> usize {
    let integer_end = decimal.coefficient_digits().len() - decimal.storage_scale() as usize;
    (decimal.coefficient_digits()[..integer_end]
        .trim_start_matches('0')
        .len()
        + decimal.storage_scale() as usize)
        .max(1)
}
