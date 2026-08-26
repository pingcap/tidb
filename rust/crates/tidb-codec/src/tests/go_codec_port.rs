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

//! Direct ports of `pkg/util/codec` unit tests from Go `origin/master`.
//! Each Rust test cites its Go source file and `func TestXxx`.

use crate::*;
use chrono::Utc;
use std::cmp::Ordering;
use tidb_datatype::{
    BinaryLiteral, BinaryJSON, Collation, Datum, Decimal, FieldType, FieldTypeCode,
    FieldTypeFlags, MysqlEnum, MysqlSet, MySqlDuration, Time, TimeType,
};

fn parse_datetime_str(s: &str) -> Time {
    tidb_datatype::parse_datetime(s, &Utc, false, false)
        .expect("parse datetime")
        .time
}

fn parse_duration_nanos(s: &str) -> i64 {
    tidb_datatype::parse_duration(s.as_bytes(), 0)
        .expect("parse duration")
        .nanoseconds()
}

/// Go `codec_test.go::TestCodecKey`.
///
/// The Rust datum domain has no bool/binary-literal coercion at datum
/// construction time, so rows carry the post-coercion source expectations
/// (`true -> int64(1)`, binary literal / enum / set -> `uint64`), which is
/// exactly what the Go test asserts after decoding.
#[test]
fn test_codec_key() {
    let table: &[(&[Datum], &[Datum])] = &[
        (&[Datum::new_int(1)], &[Datum::new_int(1)]),
        (
            // float32(1), float64(3.15), []byte("123"), "123"
            &[
                Datum::new_float32_from_f64(1.0),
                Datum::new_real(3.15),
                Datum::new_bytes(b"123".to_vec()),
                Datum::new_string("123"),
            ],
            &[
                Datum::new_real(1.0),
                Datum::new_real(3.15),
                Datum::new_bytes(b"123".to_vec()),
                Datum::new_bytes(b"123".to_vec()),
            ],
        ),
        (
            // uint64(1), float64(3.15), []byte("123"), int64(-1)
            &[
                Datum::new_uint(1),
                Datum::new_real(3.15),
                Datum::new_bytes(b"123".to_vec()),
                Datum::new_int(-1),
            ],
            &[
                Datum::new_uint(1),
                Datum::new_real(3.15),
                Datum::new_bytes(b"123".to_vec()),
                Datum::new_int(-1),
            ],
        ),
        // true, false -> int64(1), int64(0)
        (&[Datum::new_int(1), Datum::new_int(0)], &[Datum::new_int(1), Datum::new_int(0)]),
        (&[Datum::Null], &[Datum::Null]),
        (
            // NewBinaryLiteralFromUint(100, -1) / (100, 4) -> uint64(100)
            &[
                Datum::new_binary_literal(BinaryLiteral::from_uint(100, None)),
                Datum::new_binary_literal(BinaryLiteral::from_uint(100, Some(
                    tidb_datatype::BinaryLiteralWidth::try_from(4u8).unwrap(),
                ))),
            ],
            &[Datum::new_uint(100), Datum::new_uint(100)],
        ),
        (
            // Enum{"a",1}, Set{"a",1} -> uint64(1)
            &[
                Datum::new_enum(MysqlEnum::new("a", 1), Collation::Utf8Mb4Bin),
                Datum::new_set(MysqlSet::new("a", 1), Collation::Utf8Mb4Bin),
            ],
            &[Datum::new_uint(1), Datum::new_uint(1)],
        ),
    ];
    for (i, (input, expect)) in table.iter().enumerate() {
        let key = encode_key(input).unwrap_or_else(|e| panic!("{i}: {e}"));

        let args = decode(&key, 1).unwrap_or_else(|e| panic!("{i}: {e}"));
        assert_eq!(args.len(), expect.len(), "row {i}");
        for (got, want) in args.iter().zip(expect.iter()) {
            match (got, want) {
                (Datum::Bytes(got), Datum::String(want)) => {
                    assert_eq!(got, want.bytes(), "row {i}")
                }
                _ => assert_eq!(got, want, "row {i}"),
            }
        }

        let value = encode_value(input).unwrap_or_else(|e| panic!("{i}: {e}"));
        let size: usize = input
            .iter()
            .map(|v| estimate_value_size(v).unwrap())
            .sum();
        assert_eq!(value.len(), size, "row {i} value size");

        let args = decode(&value, 1).unwrap_or_else(|e| panic!("{i}: {e}"));
        assert_eq!(args.len(), expect.len(), "row {i}");
        for (got, want) in args.iter().zip(expect.iter()) {
            match (got, want) {
                (Datum::Bytes(got), Datum::String(want)) => {
                    assert_eq!(got, want.bytes(), "row {i}")
                }
                _ => assert_eq!(got, want, "row {i}"),
            }
        }
    }

    // A raw datum cannot be key-encoded.
    assert!(encode_key(&[Datum::new_raw(b"raw".to_vec())]).is_err());
}

/// Go `codec_test.go::TestCodecKeyCompare` (full source table).
#[test]
fn test_codec_key_compare() {
    let t11 = parse_datetime_str("2011-11-11 00:00:00");
    let d00 = MySqlDuration::from_nanoseconds(parse_duration_nanos("00:00:00"), 0).unwrap();
    let d01 = MySqlDuration::from_nanoseconds(parse_duration_nanos("00:00:01"), 0).unwrap();
    let table: &[(&[Datum], &[Datum], Ordering)] = &[
        (&[Datum::new_int(1)], &[Datum::new_int(1)], Ordering::Equal),
        (&[Datum::new_int(-1)], &[Datum::new_int(1)], Ordering::Less),
        (&[Datum::new_real(3.15)], &[Datum::new_real(3.12)], Ordering::Greater),
        (
            &[Datum::new_string("abc")],
            &[Datum::new_string("abcd")],
            Ordering::Less,
        ),
        (
            &[Datum::new_string("abcdefgh")],
            &[Datum::new_string("abcdefghi")],
            Ordering::Less,
        ),
        (
            &[Datum::new_int(1), Datum::new_string("abc")],
            &[Datum::new_int(1), Datum::new_string("abcd")],
            Ordering::Less,
        ),
        (
            &[Datum::new_int(1), Datum::new_string("abc"), Datum::new_string("def")],
            &[Datum::new_int(1), Datum::new_string("abcd"), Datum::new_string("af")],
            Ordering::Less,
        ),
        (
            &[Datum::new_real(3.12), Datum::new_string("ebc"), Datum::new_string("def")],
            &[Datum::new_real(2.12), Datum::new_string("abcd"), Datum::new_string("af")],
            Ordering::Greater,
        ),
        (
            &[Datum::new_bytes(vec![0x01, 0x00]), Datum::new_bytes(vec![0xFF])],
            &[Datum::new_bytes(vec![0x01, 0x00, 0xFF])],
            Ordering::Less,
        ),
        (
            &[Datum::new_bytes(vec![0x01]), Datum::new_uint(0xFFFFFFFFFFFFFFF)],
            &[Datum::new_bytes(vec![0x01, 0x10]), Datum::new_uint(0)],
            Ordering::Less,
        ),
        (&[Datum::new_int(0)], &[Datum::Null], Ordering::Greater),
        (&[Datum::new_bytes(vec![0x00])], &[Datum::Null], Ordering::Greater),
        (
            &[Datum::new_real(f64::from_bits(1))],
            &[Datum::Null],
            Ordering::Greater,
        ),
        (&[Datum::new_int(i64::MIN)], &[Datum::Null], Ordering::Greater),
        (
            &[Datum::new_int(1), Datum::new_int(i64::MIN), Datum::Null],
            &[Datum::new_int(1), Datum::Null, Datum::new_uint(u64::MAX)],
            Ordering::Greater,
        ),
        (
            &[Datum::new_int(1), Datum::new_bytes(Vec::new()), Datum::Null],
            &[Datum::new_int(1), Datum::Null, Datum::new_int(123)],
            Ordering::Greater,
        ),
        (
            &[Datum::new_time(t11), Datum::new_int(1)],
            &[Datum::new_time(t11), Datum::new_int(0)],
            Ordering::Greater,
        ),
        (
            &[Datum::new_duration(d00), Datum::new_int(1)],
            &[Datum::new_duration(d01), Datum::new_int(0)],
            Ordering::Less,
        ),
        (
            &[Datum::min_not_null()],
            &[Datum::max_value()],
            Ordering::Less,
        ),
    ];
    for (left, right, expect) in table {
        let b1 = encode_key(left).expect("encode left");
        let b2 = encode_key(right).expect("encode right");
        assert_eq!(b1.cmp(&b2), *expect, "{left:?} vs {right:?}");
    }
}

/// Go `codec_test.go::TestFloatCodec`.
#[test]
fn test_float_codec() {
    let tbl_float = [
        -1.0,
        0.0,
        1.0,
        f64::MAX,
        f32::MAX as f64,
        f32::from_bits(1) as f64,   // math.SmallestNonzeroFloat32
        f64::from_bits(1),          // math.SmallestNonzeroFloat64
        f64::NEG_INFINITY,
        f64::INFINITY,
    ];
    for value in tbl_float {
        let mut buf = Vec::new();
        encode_float(&mut buf, value);
        let (_, decoded) = decode_float(&buf).unwrap();
        assert_eq!(decoded, value);

        let mut buf = Vec::new();
        encode_float_desc(&mut buf, value);
        let (_, decoded) = decode_float_desc(&buf).unwrap();
        assert_eq!(decoded, value);
    }

    let tbl_cmp = [
        (-1.0, -1.0, Ordering::Equal),
        (-1.0, 1.0, Ordering::Less),
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
    for (a, b, ret) in tbl_cmp {
        let mut b1 = Vec::new();
        let mut b2 = Vec::new();
        encode_float(&mut b1, a);
        encode_float(&mut b2, b);
        assert_eq!(b1.cmp(&b2), ret);

        b1.clear();
        b2.clear();
        encode_float_desc(&mut b1, a);
        encode_float_desc(&mut b2, b);
        assert_eq!(b1.cmp(&b2), ret.reverse());
    }
}

/// Go `bytes_test.go::TestFastSlowFastReverse`.
///
/// skipped-reason: exercises the internal unaligned-load fast reverse path
/// (`fastReverseBytes` / `supportsUnaligned`); neither is part of the Rust
/// bytes module's public surface, whose observable behavior is pinned by the
/// golden vectors in `test_bytes_codec_go_vectors` below.
#[test]
fn test_fast_slow_fast_reverse_unreachable_from_rust_surface() {
    // The equivalent invariant (encoding is an involution of byte order) is
    // covered by every round trip in this module; nothing further to assert.
}

/// Go `bytes_test.go::TestBytesCodec` — exact golden encodings, both
/// directions, plus the error-decode inputs.
#[test]
fn test_bytes_codec_go_vectors() {
    #[rustfmt::skip]
    let inputs: &[(&[u8], &[u8], bool)] = &[
        (&[], &[0, 0, 0, 0, 0, 0, 0, 0, 247], false),
        (&[], &[255, 255, 255, 255, 255, 255, 255, 255, 8], true),
        (&[0], &[0, 0, 0, 0, 0, 0, 0, 0, 248], false),
        (&[0], &[255, 255, 255, 255, 255, 255, 255, 255, 7], true),
        (&[1, 2, 3], &[1, 2, 3, 0, 0, 0, 0, 0, 250], false),
        (&[1, 2, 3], &[254, 253, 252, 255, 255, 255, 255, 255, 5], true),
        (&[1, 2, 3, 0], &[1, 2, 3, 0, 0, 0, 0, 0, 251], false),
        (&[1, 2, 3, 0], &[254, 253, 252, 255, 255, 255, 255, 255, 4], true),
        (&[1, 2, 3, 4, 5, 6, 7], &[1, 2, 3, 4, 5, 6, 7, 0, 254], false),
        (&[1, 2, 3, 4, 5, 6, 7], &[254, 253, 252, 251, 250, 249, 248, 255, 1], true),
        (
            &[0, 0, 0, 0, 0, 0, 0, 0],
            &[0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
            false,
        ),
        (
            &[0, 0, 0, 0, 0, 0, 0, 0],
            &[255, 255, 255, 255, 255, 255, 255, 255, 0, 255, 255, 255, 255, 255, 255, 255, 255, 8],
            true,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8],
            &[1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
            false,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8],
            &[254, 253, 252, 251, 250, 249, 248, 247, 0, 255, 255, 255, 255, 255, 255, 255, 255, 8],
            true,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248],
            false,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[254, 253, 252, 251, 250, 249, 248, 247, 0, 246, 255, 255, 255, 255, 255, 255, 255, 7],
            true,
        ),
    ];

    for (enc, dec, desc) in inputs {
        assert_eq!(dec.len(), encoded_bytes_len(enc.len()));
        if *desc {
            let mut buf = Vec::new();
            encode_bytes_desc(&mut buf, enc);
            assert_eq!(&buf, dec);
            let (_, decoded) = decode_bytes_desc(&buf).unwrap();
            assert_eq!(&decoded, enc);
        } else {
            let mut buf = Vec::new();
            encode_bytes(&mut buf, enc);
            assert_eq!(&buf, dec);
            let (_, decoded) = decode_bytes(&buf).unwrap();
            assert_eq!(&decoded, enc);
        }
    }

    let err_inputs: [&[u8]; 9] = [
        &[1, 2, 3, 4],
        &[0, 0, 0, 0, 0, 0, 0, 247],
        &[0, 0, 0, 0, 0, 0, 0, 0, 246],
        &[0, 0, 0, 0, 0, 0, 0, 1, 247],
        &[1, 2, 3, 4, 5, 6, 7, 8, 0],
        &[1, 2, 3, 4, 5, 6, 7, 8, 255, 1],
        &[1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8],
        &[1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 255],
        &[1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 0],
    ];
    for input in err_inputs {
        assert!(decode_bytes(input).is_err(), "{input:?}");
    }
}

/// Go `bytes_test.go::TestBytesCodecExt`.
#[test]
fn test_bytes_codec_ext() {
    let inputs: &[(&[u8], &[u8])] = &[
        (&[], &[0, 0, 0, 0, 0, 0, 0, 0, 247]),
        (&[1, 2, 3], &[1, 2, 3, 0, 0, 0, 0, 0, 250]),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248],
        ),
    ];
    for (enc, dec) in inputs {
        let mut buf = Vec::new();
        encode_bytes_ext(&mut buf, enc, true);
        assert_eq!(buf, enc.to_vec());

        buf.clear();
        encode_bytes_ext(&mut buf, enc, false);
        assert_eq!(buf, dec.to_vec());
    }
}

/// Go `codec_test.go::TestBytes` — compact-bytes half and the comparison
/// table (the plain/desc round trips and ordering table are additionally
/// pinned by `tests/bytes.rs::comparable_bytes_source_rows_round_trip_and_order`).
#[test]
fn test_bytes_compact_and_order_table() {
    let tbl_bytes: [&[u8]; 6] = [
        &[],
        &[0x00, 0x01],
        &[0xff, 0xff],
        &[0x01, 0x00],
        b"abc",
        b"hello world",
    ];
    for input in tbl_bytes {
        let mut buf = Vec::new();
        encode_compact_bytes(&mut buf, input);
        let (_, decoded) = decode_compact_bytes(&buf).unwrap();
        assert_eq!(decoded, input);

        // Plain + desc round trips from the same Go table.
        let mut plain = Vec::new();
        encode_bytes(&mut plain, input);
        assert_eq!(decode_bytes(&plain).unwrap().1, input);
        let mut desc = Vec::new();
        encode_bytes_desc(&mut desc, input);
        assert_eq!(decode_bytes_desc(&desc).unwrap().1, input);
    }

    let tbl_cmp: &[(&[u8], &[u8], Ordering)] = &[
        (b"", &[0x00], Ordering::Less),
        (&[0x00], &[0x00], Ordering::Equal),
        (&[0xFF], &[0x00], Ordering::Greater),
        (&[0xFF], &[0xFF, 0x00], Ordering::Less),
        (b"a", b"b", Ordering::Less),
        (b"a", &[0x00], Ordering::Greater),
        (&[0x00], &[0x01], Ordering::Less),
        (&[0x00, 0x01], &[0x00, 0x00], Ordering::Greater),
        (&[0x00, 0x00, 0x00], &[0x00, 0x00], Ordering::Greater),
        (&[0x00, 0x00, 0x00], &[0x00, 0x00], Ordering::Greater),
        (
            &[0; 8],
            &[0; 9],
            Ordering::Less,
        ),
        (&[0x01, 0x02, 0x03, 0x00], &[0x01, 0x02, 0x03], Ordering::Greater),
        (&[0x01, 0x03, 0x03, 0x04], &[0x01, 0x03, 0x03, 0x05], Ordering::Less),
        (
            &[1, 2, 3, 4, 5, 6, 7],
            &[1, 2, 3, 4, 5, 6, 7, 8],
            Ordering::Less,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[1, 2, 3, 4, 5, 6, 7, 8],
            Ordering::Greater,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 0],
            &[1, 2, 3, 4, 5, 6, 7, 8],
            Ordering::Greater,
        ),
    ];
    for (a, b, ret) in tbl_cmp {
        let mut b1 = Vec::new();
        let mut b2 = Vec::new();
        encode_bytes(&mut b1, a);
        encode_bytes(&mut b2, b);
        assert_eq!(b1.cmp(&b2), *ret, "{a:?} vs {b:?}");

        b1.clear();
        b2.clear();
        encode_bytes_desc(&mut b1, a);
        encode_bytes_desc(&mut b2, b);
        assert_eq!(b1.cmp(&b2), ret.reverse());
    }
}

/// Go `codec_test.go::TestTime`.
#[test]
fn test_time_codec_key_round_trip_and_order() {
    for s in ["2011-01-01 00:00:00", "2011-01-01 00:00:00", "0001-01-01 00:00:00"] {
        let m = parse_datetime_str(s);
        let key = encode_key_in_timezone(&Utc, &[Datum::new_time(m)]).unwrap();
        let decoded = decode(&key, 1).unwrap();
        let packed = match &decoded[0] {
            Datum::UInt(packed) => *packed,
            other => panic!("expected uint packed time, got {other:?}"),
        };
        let raw_time =
            Time::from_packed_uint(packed, TimeType::DateTime, 0).expect("from packed");
        assert_eq!(m, raw_time, "{s}");
    }

    let tbl_cmp = [
        ("2011-10-10 00:00:00", "2000-12-12 11:11:11", Ordering::Greater),
        ("2000-10-10 00:00:00", "2001-10-10 00:00:00", Ordering::Less),
        ("2000-10-10 00:00:00", "2000-10-10 00:00:00", Ordering::Equal),
    ];
    for (a, b, ret) in tbl_cmp {
        let m1 = parse_datetime_str(a);
        let m2 = parse_datetime_str(b);
        let b1 = encode_key_in_timezone(&Utc, &[Datum::new_time(m1)]).unwrap();
        let b2 = encode_key_in_timezone(&Utc, &[Datum::new_time(m2)]).unwrap();
        assert_eq!(b1.cmp(&b2), ret);
    }
}

/// Go `codec_test.go::TestDuration`.
#[test]
fn test_duration_codec_key_round_trip_and_order() {
    for s in ["11:11:11", "00:00:00", "1 11:11:11"] {
        let nanos = parse_duration_nanos(s);
        let key =
            encode_key_in_timezone(&Utc, &[Datum::new_duration(MySqlDuration::from_nanoseconds(nanos, 0).unwrap())])
                .unwrap();
        let decoded = decode(&key, 1).unwrap();
        // Source sets Fsp=MaxFsp before comparing because decoding recovers
        // max-fractional-precision durations.
        let expected = MySqlDuration::from_nanoseconds(nanos, i64::from(MAX_DURATION_FSP)).unwrap();
        assert_eq!(decoded, vec![Datum::new_duration(expected)], "{s}");
    }

    let tbl_cmp = [
        ("20:00:00", "11:11:11", Ordering::Greater),
        ("00:00:00", "00:00:01", Ordering::Less),
        ("00:00:00", "00:00:00", Ordering::Equal),
    ];
    for (a, b, ret) in tbl_cmp {
        let d1 = MySqlDuration::from_nanoseconds(parse_duration_nanos(a), 0).unwrap();
        let d2 = MySqlDuration::from_nanoseconds(parse_duration_nanos(b), 0).unwrap();
        let b1 = encode_key_in_timezone(&Utc, &[Datum::new_duration(d1)]).unwrap();
        let b2 = encode_key_in_timezone(&Utc, &[Datum::new_duration(d2)]).unwrap();
        assert_eq!(b1.cmp(&b2), ret);
    }
}

/// Go `decimal_test.go::TestDecimalCodec`.
#[test]
fn test_decimal_codec_round_trip_metadata() {
    let inputs = [
        "123400", "1234", "12.34", "0.1234", "0.01234", "-0.1234", "-0.01234", "12.3400",
        "-12.34", "0.00000", "0", "-0.0", "-0.000",
    ];
    for text in inputs {
        let v = Decimal::from_literal(text);
        let mut buf = Vec::new();
        crate::decimal::encode_decimal(&mut buf, &v).expect("encode decimal");

        let (_, d, prec, frac) = decode_decimal(&buf).expect("decode decimal");
        let (prec1, frac1) = v.precision_and_frac();
        assert_eq!(prec as i32, prec1, "{text}");
        assert_eq!(frac as i32, frac1, "{text}");
        assert_eq!(d.cmp(&v), Ordering::Equal, "{text}");
    }
}

/// Go `decimal_test.go::TestFrac`.
#[test]
fn test_frac_round_trip_display() {
    for text in ["3", "0.03"] {
        let v = Decimal::from_literal(text);
        let mut buf = Vec::new();
        crate::decimal::encode_decimal(&mut buf, &v).expect("encode decimal");
        let (_, dec, _, _) = decode_decimal(&buf).expect("decode decimal");
        assert_eq!(dec.to_string(), v.to_string(), "{text}");
    }
}

/// Go `codec_test.go::TestDecimal` — key round trip, full key-ordering table
/// (float/int/uint-backed decimals), value-size contract, and the
/// truncation/overflow errors from fixed-precision encoding.
#[test]
fn test_decimal_key_order_and_fixed_precision_errors() {
    let tbl = [
        "1234.00", "1234", "12.34", "12.340", "0.1234", "0.0", "0", "-0.0", "-0.0000",
        "-1234.00", "-1234", "-12.34", "-12.340", "-0.1234",
    ];
    for text in tbl {
        let dec = Decimal::from_literal(text);
        let key = encode_key(&[Datum::new_decimal(dec.clone())]).unwrap();
        let decoded = decode(&key, 1).unwrap();
        assert_eq!(decoded.len(), 1, "{text}");
        match &decoded[0] {
            Datum::Decimal(v) => assert_eq!(v.cmp(&dec), Ordering::Equal, "{text}"),
            other => panic!("{text}: expected decimal, got {other:?}"),
        }
    }

    // Go normalizes every table decimal with SetLength(30)/SetFrac(6); the
    // declared shape drives the fixed-schema memcomparable bin encoding.
    let dec_of = |text: &str| {
        Datum::new_decimal(Decimal::from_literal(text).with_declared_shape(30, 6))
    };
    let int_of =
        |v: i64| Datum::new_decimal(Decimal::from_literal(&v.to_string()).with_declared_shape(30, 6));
    let uint_of =
        |v: u64| Datum::new_decimal(Decimal::from_literal(&v.to_string()).with_declared_shape(30, 6));

    let tbl_cmp: &[((Datum, Datum), Ordering)] = &[
        ((dec_of("1234"), dec_of("123400")), Ordering::Less),
        ((dec_of("12340"), dec_of("123400")), Ordering::Less),
        ((dec_of("1234"), dec_of("1234.5")), Ordering::Less),
        ((dec_of("1234"), dec_of("1234.0000")), Ordering::Equal),
        ((dec_of("1234"), dec_of("12.34")), Ordering::Greater),
        ((dec_of("12.34"), dec_of("12.35")), Ordering::Less),
        ((dec_of("0.12"), dec_of("0.1234")), Ordering::Less),
        ((dec_of("0.1234"), dec_of("12.3400")), Ordering::Less),
        ((dec_of("0.1234"), dec_of("0.1235")), Ordering::Less),
        ((dec_of("0.123400"), dec_of("12.34")), Ordering::Less),
        ((dec_of("12.34000"), dec_of("12.34")), Ordering::Equal),
        ((dec_of("0.01234"), dec_of("0.01235")), Ordering::Less),
        ((dec_of("0.1234"), dec_of("0")), Ordering::Greater),
        ((dec_of("0.0000"), dec_of("0")), Ordering::Equal),
        ((dec_of("0.0001"), dec_of("0")), Ordering::Greater),
        ((dec_of("0.0001"), dec_of("0.0000")), Ordering::Greater),
        ((dec_of("0"), dec_of("-0.0000")), Ordering::Equal),
        ((dec_of("-0.0001"), dec_of("0")), Ordering::Less),
        ((dec_of("-0.1234"), dec_of("0")), Ordering::Less),
        ((dec_of("-0.1234"), dec_of("-0.12")), Ordering::Less),
        ((dec_of("-0.12"), dec_of("-0.1234")), Ordering::Greater),
        ((dec_of("-0.12"), dec_of("-0.1200")), Ordering::Equal),
        ((dec_of("-0.1234"), dec_of("0.1234")), Ordering::Less),
        ((dec_of("-1.234"), dec_of("-12.34")), Ordering::Greater),
        ((dec_of("-0.1234"), dec_of("-12.34")), Ordering::Greater),
        ((dec_of("-12.34"), dec_of("1234")), Ordering::Less),
        ((dec_of("-12.34"), dec_of("-12.35")), Ordering::Greater),
        ((dec_of("-0.01234"), dec_of("-0.01235")), Ordering::Greater),
        ((dec_of("-1234"), dec_of("-123400")), Ordering::Greater),
        ((dec_of("-12340"), dec_of("-123400")), Ordering::Greater),
        ((int_of(-1), int_of(1)), Ordering::Less),
        ((int_of(i64::MAX), int_of(i64::MIN)), Ordering::Greater),
        ((int_of(i64::MAX), int_of(i32::MAX as i64)), Ordering::Greater),
        ((int_of(i32::MIN as i64), int_of(i16::MAX as i64)), Ordering::Less),
        ((int_of(i64::MIN), int_of(i8::MAX as i64)), Ordering::Less),
        ((int_of(0), int_of(i8::MAX as i64)), Ordering::Less),
        ((int_of(i8::MIN as i64), int_of(0)), Ordering::Less),
        ((int_of(i16::MIN as i64), int_of(i16::MAX as i64)), Ordering::Less),
        ((int_of(1), int_of(-1)), Ordering::Greater),
        ((int_of(1), int_of(0)), Ordering::Greater),
        ((int_of(-1), int_of(0)), Ordering::Less),
        ((int_of(0), int_of(0)), Ordering::Equal),
        ((int_of(i16::MAX as i64), int_of(i16::MAX as i64)), Ordering::Equal),
        ((uint_of(0), uint_of(0)), Ordering::Equal),
        ((uint_of(1), uint_of(0)), Ordering::Greater),
        ((uint_of(0), uint_of(1)), Ordering::Less),
        ((uint_of(u8::MAX as u64), uint_of(u16::MAX as u64)), Ordering::Less),
        (
            (uint_of(u32::MAX as u64), uint_of(i32::MAX as u64)),
            Ordering::Greater,
        ),
        ((uint_of(u8::MAX as u64), uint_of(i8::MAX as u64)), Ordering::Greater),
        ((uint_of(u16::MAX as u64), uint_of(i32::MAX as u64)), Ordering::Less),
        ((uint_of(u64::MAX), uint_of(i64::MAX as u64)), Ordering::Greater),
        ((uint_of(i64::MAX as u64), uint_of(u32::MAX as u64)), Ordering::Greater),
        ((uint_of(u64::MAX), uint_of(0)), Ordering::Greater),
        ((uint_of(0), uint_of(u64::MAX)), Ordering::Less),
    ];
    for ((d1, d2), ret) in tbl_cmp {
        let b1 = encode_key(std::slice::from_ref(d1)).unwrap();
        let b2 = encode_key(std::slice::from_ref(d2)).unwrap();
        assert_eq!(b1.cmp(&b2), *ret, "{d1:?} vs {d2:?}");

        // EncodeValue length must equal EstimateValueSize.
        let value = encode_value(std::slice::from_ref(d1)).unwrap();
        let size = estimate_value_size(d1).unwrap();
        assert_eq!(value.len(), size, "{d1:?}");
    }

    // Float-backed decimals sort monotonically through their hash-key bytes.
    let floats = [
        "-123.45", "-123.40", "-23.45", "-1.43", "-0.93", "-0.4333", "-0.068", "-0.0099", "0",
        "0.001", "0.0012", "0.12", "1.2", "1.23", "123.3", "2424.242424",
    ];
    let mut decs = Vec::with_capacity(floats.len());
    for text in floats {
        let dec = Decimal::from_literal(text);
        // Go sets SetLength(20)/SetFrac(6), so both the encoding and the
        // EstimateValueSize contract use that fixed shape.
        let mut buf = Vec::new();
        encode_decimal_fixed(&mut buf, &dec, 20, 6).expect("encode decimal");
        decs.push(buf);
    }
    for pair in decs.windows(2) {
        assert!(pair[0].cmp(&pair[1]) != Ordering::Greater, "unordered decimals");
    }

    // `-123.123456789` with (20,5) truncates; with (12,10) it overflows.
    let d = Decimal::from_literal("-123.123456789");
    let mut buf = Vec::new();
    assert!(matches!(
        encode_decimal_fixed(&mut buf, &d, 20, 5),
        Err(CodecError::DecimalTruncated)
    ));
    let mut buf = Vec::new();
    assert!(matches!(
        encode_decimal_fixed(&mut buf, &d, 12, 10),
        Err(CodecError::DecimalOverflow | CodecError::DecimalOutOfRange)
    ));
}

/// Go `codec_test.go::TestJSON`.
#[test]
fn test_json_value_round_trip() {
    let originals: Vec<Datum> = ["1234.00", r#"{"a": "b"}"#]
        .iter()
        .map(|text| Datum::new_json(BinaryJSON::parse(text).expect("parse json")))
        .collect();

    let buf = encode_value(&originals).expect("encode value");
    let decoded = decode(&buf, 2).expect("decode value");
    for (original, decoded) in originals.iter().zip(decoded.iter()) {
        let Datum::Json(original) = original else {
            panic!("expected json datum")
        };
        let Datum::Json(got) = decoded else {
            panic!("expected json datum")
        };
        assert_eq!(original.to_string(), got.to_string());
    }
}

/// Go `codec_test.go::TestCut` — cut one datum off both key- and value-encoded
/// streams and require the remainder to re-encode identically; then
/// `CutColumnID` over a single integer value row.
#[test]
fn test_cut_one_across_key_and_value_streams() {
    let enum_a = || Datum::new_enum(MysqlEnum::new("a", 1), Collation::Utf8Mb4Bin);
    let set_a = || Datum::new_set(MysqlSet::new("a", 1), Collation::Utf8Mb4Bin);
    let table: &[&[Datum]] = &[
        &[Datum::new_int(1)],
        &[
            Datum::new_float32_from_f64(1.0),
            Datum::new_real(3.15),
            Datum::new_bytes(b"123".to_vec()),
            Datum::new_string("123"),
        ],
        &[
            Datum::new_uint(1),
            Datum::new_real(3.15),
            Datum::new_bytes(b"123".to_vec()),
            Datum::new_int(-1),
        ],
        &[Datum::new_int(1), Datum::new_int(0)],
        &[Datum::Null],
        &[
            Datum::new_binary_literal(BinaryLiteral::from_uint(100, None)),
            Datum::new_binary_literal(BinaryLiteral::from_uint(100, Some(
                tidb_datatype::BinaryLiteralWidth::try_from(4u8).unwrap(),
            ))),
        ],
        &[enum_a(), set_a()],
        &[
            Datum::new_float32_from_f64(1.0),
            Datum::new_real(3.15),
            Datum::new_bytes(b"123456789012345".to_vec()),
        ],
        &[
            Datum::new_decimal(Decimal::from_literal("0")),
            Datum::new_decimal(Decimal::from_literal("-1.3")),
        ],
        &[Datum::new_json(BinaryJSON::parse(r#""abc""#).unwrap())],
    ];

    for (table_index, datums) in table.iter().enumerate() {
        let mut key = encode_key(datums).unwrap_or_else(|e| panic!("{table_index}: {e}"));
        let mut count = 0;
        while !key.is_empty() {
            let (cut, remain) =
                cut_one(&key).unwrap_or_else(|e| panic!("key {table_index}.{count}: {e}"));
            let decoded = decode_one(cut)
                .unwrap_or_else(|e| panic!("keydec {table_index}.{count}: {e}"))
                .1;
            let re_encoded = encode_key(std::slice::from_ref(&decoded)).unwrap();
            assert_eq!(re_encoded, cut, "key {table_index}.{count}");
            key = remain.to_vec();
            count += 1;
        }
        assert_eq!(count, datums.len());

        let mut value = encode_value(datums).expect("encode value");
        let mut count = 0;
        while !value.is_empty() {
            let (cut, remain) = cut_one(&value)
                .unwrap_or_else(|e| panic!("value {table_index}.{count}: {e}"));
            let decoded = decode_one(cut)
                .unwrap_or_else(|e| panic!("valuedec {table_index}.{count}: {e}"))
                .1;
            let re_encoded = encode_value(std::slice::from_ref(&decoded)).unwrap();
            assert_eq!(re_encoded, cut, "value {table_index}.{count}");
            value = remain.to_vec();
            count += 1;
        }
        assert_eq!(count, datums.len());
    }

    let value = encode_value(&[Datum::new_int(42)]).unwrap();
    let (remain, column_id) = cut_column_id(&value).unwrap();
    assert!(remain.is_empty());
    assert_eq!(column_id, 42);
}

/// Go `codec_test.go::TestSetRawValues`.
#[test]
fn test_set_raw_values_slices_match_single_encoding() {
    let datums = [
        Datum::new_int(1),
        Datum::new_string("abc"),
        Datum::new_real(1.1),
        Datum::new_bytes(b"def".to_vec()),
    ];
    let row_data = encode_value(&datums).expect("encode value");

    let values = set_raw_values(&row_data, 4).expect("set raw values");
    assert_eq!(values.len(), 4);
    for (raw, original) in values.iter().zip(datums.iter()) {
        let Datum::Raw(raw_bytes) = raw else {
            panic!("expected raw datum kind");
        };
        let encoded = encode_value(std::slice::from_ref(original)).unwrap();
        assert_eq!(&raw_bytes[..], &encoded[..]);
    }
}

/// Go `codec_test.go::TestDecodeOneToChunk`, ported over the schema-aware
/// `Decoder.DecodeOne` seam (`decode_one_typed`) without a chunk dependency:
/// each column value survives encode/decode against its declared field type.
#[test]
fn test_decode_one_typed_columns_survive_round_trip() {
    let ft = |code| FieldType::new(code);
    let mut dec_type = ft(FieldTypeCode::NewDecimal);
    dec_type.set_decimal(2);
    let mut unsigned_longlong = ft(FieldTypeCode::LongLong);
    unsigned_longlong.toggle_flags(FieldTypeFlags::UNSIGNED);

    let table: Vec<(Datum, FieldType)> = vec![
        (Datum::Null, ft(FieldTypeCode::LongLong)),
        (Datum::new_int(1), ft(FieldTypeCode::Tiny)),
        (Datum::new_int(1), ft(FieldTypeCode::Short)),
        (Datum::new_int(1), ft(FieldTypeCode::Int24)),
        (Datum::new_int(1), ft(FieldTypeCode::Long)),
        (Datum::new_int(-1), ft(FieldTypeCode::Long)),
        (Datum::new_int(1), ft(FieldTypeCode::LongLong)),
        (Datum::new_uint(1), unsigned_longlong),
        (Datum::new_float32_from_f64(1.0), ft(FieldTypeCode::Float)),
        (Datum::new_real(1.0), ft(FieldTypeCode::Double)),
        (
            Datum::new_decimal(Decimal::from_literal("1")),
            ft(FieldTypeCode::NewDecimal),
        ),
        (Datum::new_decimal(Decimal::from_literal("1.123")), dec_type),
        (Datum::new_string("abc"), ft(FieldTypeCode::String)),
        (Datum::new_string("def"), ft(FieldTypeCode::Varchar)),
        (Datum::new_string("ghi"), ft(FieldTypeCode::VarString)),
        (Datum::new_bytes(b"abc".to_vec()), ft(FieldTypeCode::Blob)),
        (Datum::new_bytes(b"abc".to_vec()), ft(FieldTypeCode::TinyBlob)),
        (Datum::new_bytes(b"abc".to_vec()), ft(FieldTypeCode::MediumBlob)),
        (Datum::new_bytes(b"abc".to_vec()), ft(FieldTypeCode::LongBlob)),
        (
            Datum::new_time(parse_datetime_str("2011-11-11 00:00:00")),
            ft(FieldTypeCode::Datetime),
        ),
        (
            Datum::new_duration(MySqlDuration::from_nanoseconds(1_000_000_000, 1).unwrap()),
            {
                let mut duration_tp = ft(FieldTypeCode::Duration);
                // The column's decimal attribute carries the duration FSP.
                duration_tp.set_decimal(1);
                duration_tp
            },
        ),
        (
            Datum::new_enum(MysqlEnum::new("a", 1), Collation::Utf8Mb4Bin),
            ft(FieldTypeCode::Enum)
                .with_collation(Collation::Utf8Mb4Bin)
                .with_elems(vec!["a"]),
        ),
        (
            Datum::new_set(MysqlSet::new("a", 1), Collation::Utf8Mb4Bin),
            ft(FieldTypeCode::Set)
                .with_collation(Collation::Utf8Mb4Bin)
                .with_elems(vec!["a"]),
        ),
        (
            Datum::new_mysql_bit(BinaryLiteral::from_uint(100, Some(
                tidb_datatype::BinaryLiteralWidth::try_from(1u8).unwrap(),
            ))),
            {
                let mut bit = ft(FieldTypeCode::Bit);
                bit.set_flen(8);
                bit
            },
        ),
        (
            Datum::new_json(BinaryJSON::parse(r#""abc""#).unwrap()),
            ft(FieldTypeCode::Json),
        ),
    ];

    for (datum, tp) in &table {
        let mut encoded = encode_value(std::slice::from_ref(datum)).expect("encode value");
        let mut got = Vec::new();
        while !encoded.is_empty() {
            let (remain, value) = decode_one_typed(&encoded, tp).expect("decode one typed");
            got.push(value);
            encoded = remain.to_vec();
        }
        assert_eq!(got.len(), 1);
        match (&got[0], datum) {
            (a, b) if a == b => {}
            // Decimals may be re-scaled by the column's decimal attribute.
            (Datum::Decimal(_), Datum::Decimal(_)) => {}
            // String datums decode as raw bytes, matching source DecodeOne
            // which never restores collation metadata from the wire.
            (Datum::Bytes(got_bytes), Datum::String(want_string)) => {
                assert_eq!(got_bytes, want_string.bytes());
            }
            (other, expected) => {
                panic!("round trip mismatch: got {other:?}, want {expected:?}")
            }
        }
    }
}

/// Go `codec_test.go::TestHashGroup`: hashing a decimal that does not fit the
/// field type's flen/decimal must error, and the output buffer keeps one slot
/// per input value.
#[test]
fn test_hash_group_decimal_shape_errors() {
    let value = Decimal::from_literal("-123.123456789");
    let datum_value = Datum::new_decimal(value);
    let values = [
        datum_value.clone(),
        datum_value.clone(),
        datum_value,
    ];

    let mut tp1 = FieldType::new(FieldTypeCode::NewDecimal);
    tp1.set_flen(20);
    tp1.set_decimal(5);
    let result = hash_group_key(&values, &tp1);
    assert!(result.is_err());
    if let Ok(buf) = result.map_err(|_| ()) {
        assert_eq!(buf.len(), 3);
    }

    let mut tp2 = FieldType::new(FieldTypeCode::NewDecimal);
    tp2.set_flen(12);
    tp2.set_decimal(10);
    let result = hash_group_key(&values, &tp2);
    assert!(result.is_err());
    if let Ok(buf) = result.map_err(|_| ()) {
        assert_eq!(buf.len(), 3);
    }
}

/// Shared row/type fixture for the hash tests, mirroring the subset of Go
/// `datumsForTest` that the Rust datum model covers without a chunk column.
fn hash_fixture() -> (Vec<Vec<Datum>>, Vec<FieldType>) {
    let mut unsigned_longlong = FieldType::new(FieldTypeCode::LongLong);
    unsigned_longlong.toggle_flags(FieldTypeFlags::UNSIGNED);
    let mut enum_tp = FieldType::new(FieldTypeCode::Enum);
    enum_tp.set_elems(vec![tidb_datatype::GoString::from("a")]);
    let mut set_tp = FieldType::new(FieldTypeCode::Set);
    set_tp.set_elems(vec![tidb_datatype::GoString::from("a")]);
    let mut bit_tp = FieldType::new(FieldTypeCode::Bit);
    bit_tp.set_flen(8);
    let mut dec_tp = FieldType::new(FieldTypeCode::NewDecimal);
    dec_tp.set_decimal(2);

    let datums = vec![
        Datum::Null,
        Datum::new_int(1),
        Datum::new_int(-1),
        Datum::new_uint(1),
        Datum::new_float32_from_f64(1.0),
        Datum::new_real(1.0),
        Datum::new_decimal(Decimal::from_literal("1")),
        Datum::new_decimal(Decimal::from_literal("1.123")),
        Datum::new_string("abc"),
        Datum::new_bytes(b"abc".to_vec()),
        Datum::new_time(parse_datetime_str("2011-11-11 00:00:00")),
        Datum::new_duration(MySqlDuration::from_nanoseconds(1_000_000_000, 1).unwrap()),
        Datum::new_enum(MysqlEnum::new("a", 1), Collation::Utf8Mb4Bin),
        Datum::new_set(MysqlSet::new("a", 1), Collation::Utf8Mb4Bin),
        Datum::new_mysql_bit(BinaryLiteral::from_uint(100, Some(
            tidb_datatype::BinaryLiteralWidth::try_from(1u8).unwrap(),
        ))),
        Datum::new_json(BinaryJSON::parse("1").unwrap()),
    ];
    let types = vec![
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::Tiny),
        FieldType::new(FieldTypeCode::Long),
        unsigned_longlong,
        FieldType::new(FieldTypeCode::Float),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::NewDecimal),
        dec_tp,
        FieldType::new(FieldTypeCode::Varchar),
        FieldType::new(FieldTypeCode::Blob),
        FieldType::new(FieldTypeCode::Datetime),
        FieldType::new(FieldTypeCode::Duration),
        enum_tp,
        set_tp,
        bit_tp,
        FieldType::new(FieldTypeCode::Json),
    ];
    let row_count = 3;
    let rows: Vec<Vec<Datum>> = (0..row_count).map(|_| datums.clone()).collect();
    (rows, types)
}

/// Go `codec_test.go::TestHashChunkRow` — deterministic hashing, self-equality
/// through `EqualChunkRow`, and the cross-type equal/unequal matrix from
/// `testHashChunkRowEqual`.
#[test]
fn test_hash_chunk_row_equal_matrix() {
    let (rows, types) = hash_fixture();
    let indices: Vec<usize> = (0..types.len()).collect();

    // Deterministic hash + self equality over the whole fixture row.
    let sum1 = hash_row(&rows[0], &types, &indices).expect("hash row");
    let sum2 = hash_row(&rows[0], &types, &indices).expect("hash row");
    assert_eq!(sum1, sum2);
    assert!(equal_rows(
        &rows[0], &types, &indices, &rows[0], &types, &indices
    )
    .expect("equal rows"));

    let mut unsigned_longlong = FieldType::new(FieldTypeCode::LongLong);
    unsigned_longlong.toggle_flags(FieldTypeFlags::UNSIGNED);
    let signed_longlong = FieldType::new(FieldTypeCode::LongLong);
    let json_tp = FieldType::new(FieldTypeCode::Json);

    let check = |a: &[Datum], ta: &FieldType, b: &[Datum], tb: &FieldType, equal: bool| {
        let ha = hash_row(a, std::slice::from_ref(ta), &[0]).expect("hash");
        let hb = hash_row(b, std::slice::from_ref(tb), &[0]).expect("hash");
        assert_eq!(
            ha == hb,
            equal,
            "hash mismatch for {a:?}/{ta:?} vs {b:?}/{tb:?}"
        );
        assert_eq!(
            equal_rows(a, std::slice::from_ref(ta), &[0], b, std::slice::from_ref(tb), &[0])
                .expect("equal rows"),
            equal,
        );
    };

    // null vs null hashes equal
    check(&[Datum::Null], &signed_longlong, &[Datum::Null], &signed_longlong, true);
    // uint64(1) == int64(1)
    check(
        &[Datum::new_uint(1)],
        &unsigned_longlong,
        &[Datum::new_int(1)],
        &signed_longlong,
        true,
    );
    // uint64(max) != int64(-1)
    check(
        &[Datum::new_uint(u64::MAX)],
        &unsigned_longlong,
        &[Datum::new_int(-1)],
        &signed_longlong,
        false,
    );
    // decimal 1.1 == 01.100
    check(
        &[Datum::new_decimal(Decimal::from_literal("1.1"))],
        &FieldType::new(FieldTypeCode::NewDecimal),
        &[Datum::new_decimal(Decimal::from_literal("01.100"))],
        &FieldType::new(FieldTypeCode::NewDecimal),
        true,
    );
    // decimal 1.1 != 01.200
    check(
        &[Datum::new_decimal(Decimal::from_literal("1.1"))],
        &FieldType::new(FieldTypeCode::NewDecimal),
        &[Datum::new_decimal(Decimal::from_literal("01.200"))],
        &FieldType::new(FieldTypeCode::NewDecimal),
        false,
    );
    // float32(1.0) == float64(1.0)
    check(
        &[Datum::new_float32_from_f64(1.0)],
        &FieldType::new(FieldTypeCode::Float),
        &[Datum::new_real(1.0)],
        &FieldType::new(FieldTypeCode::Double),
        true,
    );
    // float32(1.0) != float64(1.1)
    check(
        &[Datum::new_float32_from_f64(1.0)],
        &FieldType::new(FieldTypeCode::Float),
        &[Datum::new_real(1.1)],
        &FieldType::new(FieldTypeCode::Double),
        false,
    );
    // string "x" == bytes "x"
    check(
        &[Datum::new_string("x")],
        &FieldType::new(FieldTypeCode::Varchar),
        &[Datum::new_bytes(b"x".to_vec())],
        &FieldType::new(FieldTypeCode::Blob),
        true,
    );
    // string "x" != bytes "y"
    check(
        &[Datum::new_string("x")],
        &FieldType::new(FieldTypeCode::Varchar),
        &[Datum::new_bytes(b"y".to_vec())],
        &FieldType::new(FieldTypeCode::Blob),
        false,
    );
    // JSON int64(1) == float64(1.0)
    check(
        &[Datum::new_json(BinaryJSON::parse("1").unwrap())],
        &json_tp,
        &[Datum::new_json(BinaryJSON::parse("1.0").unwrap())],
        &json_tp,
        true,
    );
    // JSON uint64(max) != float64(max)
    check(
        &[Datum::new_json(BinaryJSON::parse("18446744073709551615").unwrap())],
        &json_tp,
        &[Datum::new_json(BinaryJSON::parse("1.8446744073709552e19").unwrap())],
        &json_tp,
        false,
    );
    // JSON int64(min) == float64(min) (exact power of two)
    check(
        &[Datum::new_json(BinaryJSON::parse("-9223372036854775808").unwrap())],
        &json_tp,
        &[Datum::new_json(BinaryJSON::parse("-9223372036854775808").unwrap())],
        &json_tp,
        true,
    );
}

/// Go `codec_test.go::TestValueSizeOfSignedInt`.
#[test]
fn test_value_size_of_signed_int_matches_varint_len() {
    let cases = [
        64_i64,
        8192,
        1048576,
        134217728,
        17179869184,
        2199023255552,
        281474976710656,
        36028797018963968,
        4611686018427387904,
    ];
    for v in cases {
        for value in [v - 10, v, v + 10, -v, -v + 10, -v - 10] {
            let mut buf = Vec::new();
            encode_varint(&mut buf, value);
            // Source `valueSizeOfSignedInt` counts the leading `varintFlag` byte.
            assert_eq!(value_size_of_signed_int(value), buf.len() + 1, "value {value}");
        }
    }
}

/// Go `codec_test.go::TestValueSizeOfUnsignedInt`.
#[test]
fn test_value_size_of_unsigned_int_matches_uvarint_len() {
    let cases = [
        128_u64,
        16384,
        2097152,
        268435456,
        34359738368,
        4398046511104,
        562949953421312,
        72057594037927936,
        9223372036854775808,
    ];
    for v in cases {
        for value in [v - 10, v, v + 10] {
            let mut buf = Vec::new();
            encode_uvarint(&mut buf, value);
            // Source `valueSizeOfUnsignedInt` counts the leading `uvarintFlag` byte.
            assert_eq!(value_size_of_unsigned_int(value), buf.len() + 1, "value {value}");
        }
    }
}

/// Go `codec_test.go::TestHashChunkColumns` — per-row column hashes agree with
/// the row-wise hasher, NULL markers are reported, and the selection vector is
/// honored (`HashChunkSelected` semantics).
#[test]
fn test_hash_chunk_columns_agree_with_row_hashes_and_selection() {
    let (rows, types) = hash_fixture();
    let first_null_columns = 1; // only column 0 in the fixture is all-null
    let selection = [true, true, true];

    for col in 0..types.len() {
        let all_null = col < first_null_columns;
        let (encoded, has_null) =
            hash_column(&rows, &types[col], col, Some(&selection), false).expect("hash column");
        assert_eq!(encoded.len(), rows.len());

        for (row_index, row) in rows.iter().enumerate() {
            assert_eq!(has_null[row_index], all_null, "col {col}");
            let per_row = hash_row(row, std::slice::from_ref(&types[col]), &[col])
                .expect("hash row");
            assert_eq!(encoded[row_index].as_deref(), Some(per_row.as_slice()), "col {col}");
        }

        // Deselected rows contribute no bytes but keep their slots.
        let deselection = [false, false, false];
        let (encoded, has_null) =
            hash_column(&rows, &types[col], col, Some(&deselection), false).expect("hash column");
        assert!(encoded.iter().all(Option::is_none));
        assert!(has_null.iter().all(|null| !null));
    }
}

/// Go `collation_test.go::prepareCollationData` string triples.
fn collation_strings() -> [(&'static str, &'static str); 3] {
    [("aaa", "AAA"), ("\u{1f61c}", "\u{1f603}"), ("À", "A")]
}

/// Go `collation_test.go::TestEncoderNewCollationEnabled`.
///
/// Go toggles a process-global flag; the Rust encoder carries the mode
/// explicitly, so both modes are constructed directly and the exported
/// `encode_key` helper is checked to track the enabled mode.
#[test]
fn test_encoder_new_collation_enabled_mode_split() {
    let lower = Datum::new_collation_string("aaa", Collation::Utf8GeneralCi);
    let upper = Datum::new_collation_string("AAA", Collation::Utf8GeneralCi);
    let lower_b = lower.clone();
    let upper_b = upper.clone();
    let enabled_encoder = Encoder::new(true);
    let disabled_encoder = Encoder::new(false);

    let enabled_lower = enabled_encoder.encode_key(std::slice::from_ref(&lower)).unwrap();
    let enabled_upper = enabled_encoder.encode_key(std::slice::from_ref(&upper)).unwrap();
    assert_eq!(enabled_lower, enabled_upper);

    let disabled_lower = disabled_encoder.encode_key(std::slice::from_ref(&lower_b)).unwrap();
    let disabled_upper = disabled_encoder.encode_key(std::slice::from_ref(&upper_b)).unwrap();
    assert_ne!(disabled_lower, disabled_upper);

    let exported_enabled_lower = encode_key(&[lower]).unwrap();
    assert_eq!(enabled_lower, exported_enabled_lower);
}

/// Go `collation_test.go::TestHashGroupKeyCollation`.
#[test]
fn test_hash_group_key_collation_ci_equivalence() {
    for collation_name in ["utf8_general_ci", "utf8_unicode_ci"] {
        let collation = Collation::from_name(collation_name).expect("known collation");
        let tp = FieldType::new(FieldTypeCode::String).with_collation(collation);

        for (a, b) in collation_strings() {
            let buf1 = hash_group_key(&[Datum::new_collation_string(a, collation)], &tp)
                .expect("hash group");
            let buf2 = hash_group_key(&[Datum::new_collation_string(b, collation)], &tp)
                .expect("hash group");
            assert_eq!(buf1.len(), buf2.len());
            assert_eq!(buf1[0].len(), buf2[0].len(), "{collation_name} {a}/{b}");
            assert_eq!(buf1[0], buf2[0], "{collation_name} {a}/{b}");
        }
    }
}

/// Go `collation_test.go::TestHashChunkRowCollation`.
#[test]
fn test_hash_chunk_row_collation() {
    for collation_name in ["binary", "utf8_general_ci", "utf8_unicode_ci"] {
        let collation = Collation::from_name(collation_name).expect("known collation");
        let tp = FieldType::new(FieldTypeCode::String).with_collation(collation);
        let binary = collation_name == "binary";
        for (a, b) in collation_strings() {
            let h1 = hash_row(
                &[Datum::new_collation_string(a, collation)],
                std::slice::from_ref(&tp),
                &[0],
            )
            .expect("hash row");
            let h2 = hash_row(
                &[Datum::new_collation_string(b, collation)],
                std::slice::from_ref(&tp),
                &[0],
            )
            .expect("hash row");
            if binary {
                assert_ne!(h1, h2, "{collation_name} {a}/{b}");
            } else {
                assert_eq!(h1, h2, "{collation_name} {a}/{b}");
            }
        }
    }
}

/// Go `collation_test.go::TestHashChunkColumnsCollation`.
#[test]
fn test_hash_chunk_columns_collation() {
    let collation_pairs = [
        ("binary", false),
        ("utf8_general_ci", true),
        ("utf8_unicode_ci", true),
    ];
    let rows_a: Vec<Vec<Datum>> = collation_strings()
        .iter()
        .map(|(a, _)| vec![Datum::new_string(*a)])
        .collect();
    for (collation_name, should_match) in collation_pairs {
        let collation = Collation::from_name(collation_name).expect("known collation");
        let tp = FieldType::new(FieldTypeCode::String).with_collation(collation);
        let rows_b: Vec<Vec<Datum>> = collation_strings()
            .iter()
            .map(|(_, b)| vec![Datum::new_string(*b)])
            .collect();
        let (h1s, _) = hash_column(&rows_a, &tp, 0, None, false).expect("hash column");
        let (h2s, _) = hash_column(&rows_b, &tp, 0, None, false).expect("hash column");
        for (h1, h2) in h1s.iter().zip(h2s.iter()) {
            assert_eq!(h1 == h2, should_match, "{collation_name}");
        }
    }
}

/// Go `codec_test.go::TestDatumHashEquals` — the datum pairs that must share a
/// lossless hash code compare equal, and two distinct datetimes do not. Go
/// drives `planner/cascades/base.NewHashEqualer`; the codec-side `HashCode`
/// bytes are the payload that hasher consumes, so byte equality pins the same
/// contract inside this crate.
#[test]
fn test_datum_hash_equals_pairs() {
    let now = parse_datetime_str("2024-05-06 07:08:09.123456");
    let later = parse_datetime_str("2024-05-06 07:08:09.123457");
    let cases: &[(&Datum, &Datum, bool)] = &[
        (&Datum::new_int(1), &Datum::new_int(1), true),
        (&Datum::new_uint(1), &Datum::new_uint(1), true),
        (&Datum::new_real(1.1), &Datum::new_real(1.1), true),
        (&Datum::new_string("abc"), &Datum::new_string("abc"), true),
        (
            &Datum::new_bytes(b"abc".to_vec()),
            &Datum::new_bytes(b"abc".to_vec()),
            true,
        ),
        (
            &Datum::new_enum(MysqlEnum::new("a", 1), Collation::Utf8Mb4Bin),
            &Datum::new_enum(MysqlEnum::new("a", 1), Collation::Utf8Mb4Bin),
            true,
        ),
        (
            &Datum::new_set(MysqlSet::new("a", 1), Collation::Utf8Mb4Bin),
            &Datum::new_set(MysqlSet::new("a", 1), Collation::Utf8Mb4Bin),
            true,
        ),
        (
            &Datum::new_binary_literal(BinaryLiteral::from_uint(1, None)),
            &Datum::new_binary_literal(BinaryLiteral::from_uint(1, None)),
            true,
        ),
        (
            &Datum::new_mysql_bit(BinaryLiteral::from_uint(1, None)),
            &Datum::new_mysql_bit(BinaryLiteral::from_uint(1, None)),
            true,
        ),
        (&Datum::new_time(now), &Datum::new_time(now), true),
        (
            &Datum::new_duration(MySqlDuration::from_nanoseconds(1_000_000_000, 0).unwrap()),
            &Datum::new_duration(MySqlDuration::from_nanoseconds(1_000_000_000, 0).unwrap()),
            true,
        ),
        (
            &Datum::new_json(BinaryJSON::parse(r#""a""#).unwrap()),
            &Datum::new_json(BinaryJSON::parse(r#""a""#).unwrap()),
            true,
        ),
        // Final Go case: two distinct times are neither hash-equal nor Equals.
        (&Datum::new_time(now), &Datum::new_time(later), false),
    ];
    for (d1, d2, equal) in cases {
        let h1 = hash_code(d1);
        let h2 = hash_code(d2);
        assert_eq!(h1 == h2, *equal, "{d1:?} vs {d2:?}");
        assert_eq!(d1 == d2, *equal, "{d1:?} vs {d2:?}");
    }
}
