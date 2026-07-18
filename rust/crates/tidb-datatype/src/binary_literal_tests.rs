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

use super::*;
use std::cmp::Ordering;

/// Complete translation of `pkg/types/binary_literal_test.go::TestBinaryLiteral`.
#[test]
fn binary_literal_executes_the_complete_original_source_table() {
    let trim_rows: &[(&[u8], &[u8])] = &[
        (&[], &[]),
        (&[0x0], &[0x0]),
        (&[0x1], &[0x1]),
        (&[0x1, 0x0], &[0x1, 0x0]),
        (&[0x0, 0x1], &[0x1]),
        (&[0x0, 0x0, 0x0], &[0x0]),
        (&[0x1, 0x0, 0x0], &[0x1, 0x0, 0x0]),
        (
            &[0x0, 0x1, 0x0, 0x0, 0x1, 0x0, 0x0],
            &[0x1, 0x0, 0x0, 0x1, 0x0, 0x0],
        ),
        (
            &[0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x1, 0x0, 0x0],
            &[0x1, 0x0, 0x0, 0x1, 0x0, 0x0],
        ),
    ];
    for &(input, expected) in trim_rows {
        assert_eq!(trim_leading_zero_bytes(input), expected, "{input:?}");
    }

    let bit_rows: &[(&str, Option<&[u8]>)] = &[
        ("b''", Some(&[])),
        ("B''", Some(&[])),
        ("0b''", None),
        ("0b0", Some(&[0x0])),
        ("b'0'", Some(&[0x0])),
        ("B'0'", Some(&[0x0])),
        ("0B0", None),
        ("0b123", None),
        ("b'123'", None),
        ("0b'1010'", None),
        ("0b0000000", Some(&[0x0])),
        ("b'0000000'", Some(&[0x0])),
        ("B'0000000'", Some(&[0x0])),
        ("0b00000000", Some(&[0x0])),
        ("b'00000000'", Some(&[0x0])),
        ("B'00000000'", Some(&[0x0])),
        ("0b000000000", Some(&[0x0, 0x0])),
        ("b'000000000'", Some(&[0x0, 0x0])),
        ("B'000000000'", Some(&[0x0, 0x0])),
        ("0b1", Some(&[0x1])),
        ("b'1'", Some(&[0x1])),
        ("B'1'", Some(&[0x1])),
        ("0b00000001", Some(&[0x1])),
        ("b'00000001'", Some(&[0x1])),
        ("B'00000001'", Some(&[0x1])),
        ("0b000000010", Some(&[0x0, 0x2])),
        ("b'000000010'", Some(&[0x0, 0x2])),
        ("B'000000010'", Some(&[0x0, 0x2])),
        ("0b000000001", Some(&[0x0, 0x1])),
        ("b'000000001'", Some(&[0x0, 0x1])),
        ("B'000000001'", Some(&[0x0, 0x1])),
        ("0b11111111", Some(&[0xff])),
        ("b'11111111'", Some(&[0xff])),
        ("B'11111111'", Some(&[0xff])),
        ("0b111111111", Some(&[0x1, 0xff])),
        ("b'111111111'", Some(&[0x1, 0xff])),
        ("B'111111111'", Some(&[0x1, 0xff])),
        (
            "0b1101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010",
            Some(b"hello world foo bar"),
        ),
        (
            "b'1101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'",
            Some(b"hello world foo bar"),
        ),
        (
            "B'1101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'",
            Some(b"hello world foo bar"),
        ),
        (
            "0b01101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010",
            Some(b"hello world foo bar"),
        ),
        (
            "b'01101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'",
            Some(b"hello world foo bar"),
        ),
        (
            "B'01101000011001010110110001101100011011110010000001110111011011110111001001101100011001000010000001100110011011110110111100100000011000100110000101110010'",
            Some(b"hello world foo bar"),
        ),
    ];
    for &(input, expected) in bit_rows {
        match expected {
            Some(expected) => assert_eq!(
                parse_bit_str(input).unwrap().as_bytes(),
                expected,
                "{input}"
            ),
            None => assert!(parse_bit_str(input).is_err(), "{input}"),
        }
    }

    let empty_bit = parse_bit_str("").unwrap_err();
    assert!(empty_bit.to_string().contains("invalid empty "));

    let hex_rows: &[(&str, Option<&[u8]>)] = &[
        ("x'1'", None),
        ("x'01'", Some(&[0x1])),
        ("X'01'", Some(&[0x1])),
        ("0x1", Some(&[0x1])),
        ("0x-1", None),
        ("0X11", None),
        ("x'01+'", None),
        ("0x123", Some(&[0x01, 0x23])),
        ("0x10", Some(&[0x10])),
        ("0x4D7953514C", Some(b"MySQL")),
        (
            "0x4920616D2061206C6F6E672068657820737472696E67",
            Some(b"I am a long hex string"),
        ),
        (
            "x'4920616D2061206C6F6E672068657820737472696E67'",
            Some(b"I am a long hex string"),
        ),
        (
            "X'4920616D2061206C6F6E672068657820737472696E67'",
            Some(b"I am a long hex string"),
        ),
        ("x''", Some(&[])),
    ];
    for &(input, expected) in hex_rows {
        match expected {
            Some(expected) => assert_eq!(
                parse_hex_str(input).unwrap().as_bytes(),
                expected,
                "{input}"
            ),
            None => assert!(parse_hex_str(input).is_err(), "{input}"),
        }
    }

    // The original second `TestParseHexStr` subtest calls ParseBitStr again.
    let duplicate_empty_bit = parse_bit_str("").unwrap_err();
    assert!(duplicate_empty_bit.to_string().contains("invalid empty "));

    let string_rows: &[(&[u8], &str)] = &[
        (&[], ""),
        (&[0x0], "0x00"),
        (&[0x1], "0x01"),
        (&[0xff, 0x01], "0xff01"),
    ];
    for &(input, expected) in string_rows {
        assert_eq!(BinaryLiteral::from(input).to_string(), expected);
    }

    let bit_string_rows: &[(&[u8], bool, &str)] = &[
        (&[], true, "b''"),
        (&[], false, "b''"),
        (&[0x0], true, "b'0'"),
        (&[0x0], false, "b'00000000'"),
        (&[0x0, 0x0], true, "b'0'"),
        (&[0x0, 0x0], false, "b'0000000000000000'"),
        (&[0x1], true, "b'1'"),
        (&[0x1], false, "b'00000001'"),
        (&[0xff, 0x01], true, "b'1111111100000001'"),
        (&[0xff, 0x01], false, "b'1111111100000001'"),
        (&[0x0, 0xff, 0x01], true, "b'1111111100000001'"),
        (&[0x0, 0xff, 0x01], false, "b'000000001111111100000001'"),
    ];
    for &(input, trim, expected) in bit_string_rows {
        assert_eq!(
            BinaryLiteral::from(input).to_bit_literal_string(trim),
            expected
        );
    }

    let to_int_rows = [
        ("x''", 0, false),
        ("0x00", 0x0, false),
        ("0xff", 0xff, false),
        ("0x10ff", 0x10ff, false),
        ("0x1010ffff", 0x1010ffff, false),
        ("0x1010ffff8080", 0x1010ffff8080, false),
        ("0x1010ffff8080ff12", 0x1010ffff8080ff12, false),
        ("0x1010ffff8080ff12ff", u64::MAX, true),
    ];
    for (input, expected, truncated) in to_int_rows {
        let outcome = parse_hex_str(input).unwrap().to_int();
        assert_eq!(outcome.value(), expected, "{input}");
        assert_eq!(outcome.is_truncated(), truncated, "{input}");
    }

    let uint_rows: &[(u64, Option<u8>, &[u8])] = &[
        (0x0, None, &[0x0]),
        (0x0, Some(1), &[0x0]),
        (0x0, Some(2), &[0x0, 0x0]),
        (0x1, None, &[0x1]),
        (0x1, Some(1), &[0x1]),
        (0x1, Some(2), &[0x0, 0x1]),
        (0x1, Some(3), &[0x0, 0x0, 0x1]),
        (0x10, None, &[0x10]),
        (0x123, None, &[0x1, 0x23]),
        (0x123, Some(2), &[0x1, 0x23]),
        (0x123, Some(1), &[0x23]),
        (0x123, Some(5), &[0x0, 0x0, 0x0, 0x1, 0x23]),
        (0x4D7953514C, None, &[0x4D, 0x79, 0x53, 0x51, 0x4C]),
        (
            0x4D7953514C,
            Some(8),
            &[0x0, 0x0, 0x0, 0x4D, 0x79, 0x53, 0x51, 0x4C],
        ),
        (
            0x4920616D2061206C,
            None,
            &[0x49, 0x20, 0x61, 0x6D, 0x20, 0x61, 0x20, 0x6C],
        ),
        (
            0x4920616D2061206C,
            Some(8),
            &[0x49, 0x20, 0x61, 0x6D, 0x20, 0x61, 0x20, 0x6C],
        ),
        (0x4920616D2061206C, Some(5), &[0x6D, 0x20, 0x61, 0x20, 0x6C]),
    ];
    for &(value, width, expected) in uint_rows {
        let width = width.map(|width| BinaryLiteralWidth::try_from(width).unwrap());
        assert_eq!(BinaryLiteral::from_uint(value, width).as_bytes(), expected);
    }

    // Go asserts that byteSize=-2 panics. The Rust API eliminates that panic
    // state: an invalid width cannot reach `BinaryLiteral::from_uint`.
    assert_eq!(
        BinaryLiteralWidth::try_from(-2_i8),
        Err(InvalidBinaryLiteralWidth::new(-2))
    );

    let compare_rows: &[(&[u8], &[u8], Ordering)] = &[
        (&[0, 0, 1], &[2], Ordering::Less),
        (&[0, 1], &[0, 0, 2], Ordering::Less),
        (&[0, 1], &[1], Ordering::Equal),
        (&[0, 2, 1], &[1, 2], Ordering::Greater),
    ];
    for &(left, right, expected) in compare_rows {
        assert_eq!(
            BinaryLiteral::from(left).compare(&BinaryLiteral::from(right)),
            expected
        );
    }

    let hex = HexLiteral::parse("x'3A3B'").unwrap();
    assert_eq!(hex.to_raw_bytes(), b":;");
    let bit = BitLiteral::parse("b'00101011'").unwrap();
    assert_eq!(bit.to_raw_bytes(), b"+");
}

/// Production boundaries visible in `binary_literal.go` but not separately
/// enumerated by the original table.
#[test]
fn binary_literal_source_boundaries_stay_explicit() {
    for empty in ["b", "b''''", "0b"] {
        assert!(
            parse_bit_str(empty).unwrap().as_bytes().is_empty(),
            "{empty}"
        );
    }
    for empty in ["x", "x''''", "0x"] {
        assert!(
            parse_hex_str(empty).unwrap().as_bytes().is_empty(),
            "{empty}"
        );
    }

    assert!(parse_bit_str("b'\u{00e9}'").is_err());
    assert!(parse_hex_str("x'zz'").is_err());

    let leading_zero_wide = BinaryLiteral::from(&[0, 0, 0, 0, 0, 0, 0, 0, 1]);
    assert_eq!(
        leading_zero_wide.to_int(),
        BinaryLiteralIntOutcome::Exact(1)
    );
    let significant_wide = BinaryLiteral::from(&[1, 0, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(
        significant_wide.to_int(),
        BinaryLiteralIntOutcome::Truncated { value: u64::MAX }
    );

    assert!(BinaryLiteralWidth::try_from(0_u8).is_err());
    assert!(BinaryLiteralWidth::try_from(9_u8).is_err());

    let raw = BinaryLiteral::from(&[0xff, 0x00, 0xfe]);
    assert_eq!(raw.to_raw_bytes(), &[0xff, 0x00, 0xfe]);
}
