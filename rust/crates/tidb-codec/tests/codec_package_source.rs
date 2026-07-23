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

//! Exact named obligations from every `pkg/util/codec/*_test.go` file.

use std::cmp::Ordering;

use chrono::{FixedOffset, Utc};
use tidb_codec::*;
use tidb_datatype::{
    parse_datetime, BinaryJSON, BinaryLiteral, Collation, Datum, Decimal, FieldType,
    FieldTypeCode, FieldTypeFlags, MySqlDuration, TimeType, VectorFloat32,
};

fn varchar(collation: Collation) -> FieldType {
    FieldType::new(FieldTypeCode::Varchar)
        .with_flen(255)
        .with_collation(collation)
}

fn key(value: &Datum) -> Vec<u8> {
    encode_key(std::slice::from_ref(value)).unwrap()
}

#[test]
fn test_fast_slow_fast_reverse() {
    let source: [u8; 18] = [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247];
    let mut descending = Vec::new();
    encode_bytes_desc(&mut descending, &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(
        descending,
        source.iter().map(|byte| !byte).collect::<Vec<_>>()
    );
}

#[test]
fn test_bytes_codec() {
    for (input, expected) in [
        (&[][..], vec![0, 0, 0, 0, 0, 0, 0, 0, 247]),
        (&[0][..], vec![0, 0, 0, 0, 0, 0, 0, 0, 248]),
        (&[1, 2, 3][..], vec![1, 2, 3, 0, 0, 0, 0, 0, 250]),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8][..],
            vec![
                1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247,
            ],
        ),
    ] {
        let mut encoded = Vec::new();
        encode_bytes(&mut encoded, input);
        assert_eq!(encoded, expected);
        assert_eq!(encoded_bytes_len(input.len()), encoded.len());
        assert_eq!(decode_bytes(&encoded).unwrap(), (&[][..], input.to_vec()));
        let mut descending = Vec::new();
        encode_bytes_desc(&mut descending, input);
        assert_eq!(
            decode_bytes_desc(&descending).unwrap(),
            (&[][..], input.to_vec())
        );
    }
    for malformed in [
        vec![1, 2, 3, 4],
        vec![0, 0, 0, 0, 0, 0, 0, 0, 246],
        vec![0, 0, 0, 0, 0, 0, 0, 1, 247],
    ] {
        assert!(decode_bytes(&malformed).is_err());
    }
}

#[test]
fn test_bytes_codec_ext() {
    for input in [vec![], vec![1, 2, 3], (1_u8..=9).collect()] {
        let mut raw = Vec::new();
        encode_bytes_ext(&mut raw, &input, true);
        assert_eq!(raw, input);
        let mut comparable = Vec::new();
        encode_bytes_ext(&mut comparable, &input, false);
        assert_eq!(decode_bytes(&comparable).unwrap().1, input);
    }
}

#[test]
fn test_decimal_codec() {
    for text in [
        "123400", "1234", "12.34", "0.1234", "0.01234", "-0.1234", "-0.01234", "-12.34",
        "0",
    ] {
        let value = Decimal::from_signed_literal(text);
        let mut encoded = Vec::new();
        encode_decimal_fixed(&mut encoded, &value, 0, 0).unwrap();
        let (remain, decoded, _, _) = decode_decimal(&encoded).unwrap();
        assert!(remain.is_empty());
        assert_eq!(decoded, value);
    }
    assert_eq!(
        decode_decimal_with_fault(&[1, 0, 0], true),
        Err(CodecError::InjectedFailure("errorInDecodeDecimal"))
    );
}

#[test]
fn test_frac() {
    for value in [
        Decimal::from_int(3),
        Decimal::from_signed_literal("0.03"),
    ] {
        let mut encoded = Vec::new();
        encode_decimal_fixed(&mut encoded, &value, 0, 0).unwrap();
        assert_eq!(decode_decimal(&encoded).unwrap().1.to_string(), value.to_string());
    }
}

#[test]
fn test_codec_key() {
    let rows = vec![
        vec![Datum::Int(1)],
        vec![
            Datum::Float32(1.0),
            Datum::Real(3.15),
            Datum::new_bytes(b"123"),
            Datum::new_string("123"),
        ],
        vec![
            Datum::UInt(1),
            Datum::Real(3.15),
            Datum::new_bytes(b"123"),
            Datum::Int(-1),
        ],
        vec![Datum::Null],
        vec![
            Datum::new_binary_literal(BinaryLiteral::from_uint(100, None)),
            Datum::new_enum(
                tidb_datatype::MysqlEnum::new("a", 1),
                Collation::Binary,
            ),
        ],
    ];
    for row in rows {
        let encoded = encode_key(&row).unwrap();
        assert_eq!(decode(&encoded, row.len()).unwrap().len(), row.len());
        let value = encode_value(&row).unwrap();
        assert_eq!(
            value.len(),
            row.iter()
                .map(estimate_value_size)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .into_iter()
                .sum::<usize>()
        );
        assert_eq!(decode(&value, row.len()).unwrap().len(), row.len());
    }
    assert!(encode_key(&[Datum::new_raw(b"raw")]).is_err());
    assert!(encode_value(&[Datum::new_raw(b"raw")]).is_err());
}

#[test]
fn test_codec_key_compare() {
    let rows = [
        (Datum::Int(-1), Datum::Int(1), Ordering::Less),
        (Datum::Real(3.15), Datum::Real(3.12), Ordering::Greater),
        (
            Datum::new_string("abc"),
            Datum::new_string("abcd"),
            Ordering::Less,
        ),
        (Datum::Null, Datum::Int(0), Ordering::Less),
        (Datum::MinNotNull, Datum::MaxValue, Ordering::Less),
    ];
    for (left, right, expected) in rows {
        assert_eq!(key(&left).cmp(&key(&right)), expected);
    }
}

#[test]
fn test_number_codec() {
    for value in [i64::MIN, i32::MIN.into(), -1, 0, 1, i64::MAX] {
        let mut encoded = Vec::new();
        encode_int(&mut encoded, value);
        assert_eq!(decode_int(&encoded).unwrap(), (&[][..], value));
        encoded.clear();
        encode_varint(&mut encoded, value);
        assert_eq!(decode_varint(&encoded).unwrap(), (&[][..], value));
        encoded.clear();
        encode_comparable_varint(&mut encoded, value);
        assert_eq!(
            decode_comparable_varint(&encoded).unwrap(),
            (&[][..], value)
        );
    }
    for value in [0, 1, u64::from(u32::MAX), u64::MAX] {
        let mut encoded = Vec::new();
        encode_uint(&mut encoded, value);
        assert_eq!(decode_uint(&encoded).unwrap(), (&[][..], value));
        encoded.clear();
        encode_uvarint(&mut encoded, value);
        assert_eq!(decode_uvarint(&encoded).unwrap(), (&[][..], value));
    }
}

#[test]
fn test_number_order() {
    for (left, right, expected) in [
        (-1, 1, Ordering::Less),
        (i64::MAX, i64::MIN, Ordering::Greater),
        (0, 0, Ordering::Equal),
    ] {
        let mut a = Vec::new();
        let mut b = Vec::new();
        encode_int(&mut a, left);
        encode_int(&mut b, right);
        assert_eq!(a.cmp(&b), expected);
        a.clear();
        b.clear();
        encode_comparable_varint(&mut a, left);
        encode_comparable_varint(&mut b, right);
        assert_eq!(a.cmp(&b), expected);
    }
}

#[test]
fn test_float_codec() {
    for value in [
        -1.0,
        0.0,
        1.0,
        f64::MAX,
        f64::NEG_INFINITY,
        f64::INFINITY,
    ] {
        let mut encoded = Vec::new();
        encode_float(&mut encoded, value);
        assert_eq!(decode_float(&encoded).unwrap(), (&[][..], value));
        encoded.clear();
        encode_float_desc(&mut encoded, value);
        assert_eq!(decode_float_desc(&encoded).unwrap(), (&[][..], value));
    }
}

#[test]
fn test_bytes() {
    for input in [
        vec![],
        vec![0, 1],
        vec![0xff, 0xff],
        b"hello world".to_vec(),
    ] {
        let mut encoded = Vec::new();
        encode_compact_bytes(&mut encoded, &input);
        assert_eq!(decode_compact_bytes(&encoded).unwrap(), (&[][..], input.as_slice()));
    }
}

#[test]
fn test_time() {
    let value = parse_datetime("2011-11-11 11:11:11", &Utc, true, false)
        .unwrap()
        .time;
    let mut encoded = Vec::new();
    encode_mysql_time(&Utc, value, None, &mut encoded).unwrap();
    let mut tagged = vec![UINT_FLAG];
    tagged.extend_from_slice(&encoded);
    let (_, decoded) = decode_as_datetime(&tagged, TimeType::DateTime, Some(&Utc)).unwrap();
    assert_eq!(decoded, Datum::new_time(value));
    let value_encoded = encode_value(&[Datum::new_time(value)]).unwrap();
    assert_eq!(value_encoded[0], UINT_FLAG);
    assert_eq!(value_encoded.len(), 9);
    assert_eq!(estimate_value_size(&Datum::new_time(value)).unwrap(), 9);

    let mut timestamp = parse_datetime("2011-11-11 11:11:11", &Utc, true, false)
        .unwrap()
        .time;
    timestamp.set_kind(TimeType::Timestamp);
    let east_eight = FixedOffset::east_opt(8 * 60 * 60).unwrap();
    let encoded = encode_value_in_timezone(&east_eight, &[Datum::new_time(timestamp)]).unwrap();
    assert_eq!(
        decode_one_typed_in_timezone(
            &encoded,
            &FieldType::new(FieldTypeCode::Timestamp),
            Some(&east_eight)
        )
        .unwrap()
        .1,
        Datum::new_time(timestamp)
    );
}

#[test]
fn test_duration() {
    for nanos in [-1, 0, 1, 3_600_000_000_000] {
        let value = Datum::new_duration(MySqlDuration::from_nanoseconds(nanos, 6).unwrap());
        let encoded = encode_key(std::slice::from_ref(&value)).unwrap();
        assert_eq!(decode_one(&encoded).unwrap().1, value);
    }
}

#[test]
fn test_decimal() {
    for text in ["-123.45", "0", "12.340", "999999999.999"] {
        let value = Datum::new_decimal(Decimal::from_signed_literal(text));
        let encoded = encode_key(std::slice::from_ref(&value)).unwrap();
        assert_eq!(decode_one(&encoded).unwrap().1, value);
    }
}

#[test]
fn test_json() {
    for text in ["null", "true", "1", "1.5", "\"abc\"", "[1,2]", "{\"a\":1}"] {
        let value = Datum::new_json(BinaryJSON::parse(text).unwrap());
        let encoded = encode_value(std::slice::from_ref(&value)).unwrap();
        assert_eq!(decode_one(&encoded).unwrap().1, value);
    }
}

#[test]
fn test_cut() {
    let values = vec![
        Datum::Null,
        Datum::Int(-1),
        Datum::UInt(1),
        Datum::Real(1.25),
        Datum::new_bytes(b"abc"),
        Datum::new_decimal(Decimal::from_signed_literal("1.2")),
        Datum::new_duration(MySqlDuration::from_nanoseconds(1, 6).unwrap()),
        Datum::new_json(BinaryJSON::parse("[1]").unwrap()),
        Datum::new_vector_float32(VectorFloat32::parse("[1,2]").unwrap()),
    ];
    let encoded = encode_value(&values).unwrap();
    let mut remain = encoded.as_slice();
    for value in values {
        let (one, next) = cut_one(remain).unwrap();
        assert_eq!(decode_one(one).unwrap().1, value);
        remain = next;
    }
    assert!(remain.is_empty());
}

#[test]
fn test_cut_one_error() {
    assert!(cut_one(&[]).is_err());
    assert!(cut_one(&[UINT_FLAG, 0]).is_err());
    assert!(cut_one(&[COMPACT_BYTES_FLAG, 6, 1, 2]).is_err());
    assert!(cut_one(&[JSON_FLAG, 0x7f]).is_err());
}

#[test]
fn test_set_raw_values() {
    let encoded = encode_value(&[Datum::Int(1), Datum::new_string("abc")]).unwrap();
    let values = set_raw_values(&encoded, 2).unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(
        values.iter().map(Datum::as_raw_bytes).collect::<Vec<_>>(),
        vec![
            Some(&encoded[..2]),
            Some(&encoded[2..]),
        ]
    );
}

#[test]
fn test_decode_one_to_chunk() {
    let float = FieldType::new(FieldTypeCode::Float);
    let encoded = encode_value(&[Datum::Real(1.25)]).unwrap();
    assert_eq!(
        decode_one_typed(&encoded, &float).unwrap().1,
        Datum::Float32(1.25)
    );

    let enum_type = FieldType::new(FieldTypeCode::Enum)
        .with_elems(["a", "b"])
        .with_collation(Collation::Utf8Mb4Bin);
    let encoded = encode_value(&[Datum::UInt(2)]).unwrap();
    assert_eq!(
        decode_one_typed(&encoded, &enum_type).unwrap().1,
        Datum::new_enum(
            tidb_datatype::MysqlEnum::new("b", 2),
            Collation::Utf8Mb4Bin
        )
    );

    let decimal_type = FieldType::new(FieldTypeCode::NewDecimal).with_decimal(2);
    let encoded =
        encode_value(&[Datum::new_decimal(Decimal::from_signed_literal("1.235"))]).unwrap();
    assert_eq!(
        decode_one_typed(&encoded, &decimal_type).unwrap().1,
        Datum::new_decimal(Decimal::from_signed_literal("1.24"))
    );

    let duration_type = FieldType::new(FieldTypeCode::Duration).with_decimal(2);
    let encoded = encode_value(&[Datum::new_duration(
        MySqlDuration::from_nanoseconds(1_000_000_000, 6).unwrap(),
    )])
    .unwrap();
    assert_eq!(
        decode_one_typed(&encoded, &duration_type).unwrap().1,
        Datum::new_duration(MySqlDuration::from_nanoseconds(1_000_000_000, 2).unwrap())
    );
}

#[test]
fn test_hash_group() {
    let values = [Datum::Int(1), Datum::Null, Datum::Int(-1)];
    let encoded = hash_group_key(&values, &FieldType::new(FieldTypeCode::LongLong)).unwrap();
    assert_eq!(encoded.len(), values.len());
    assert_eq!(encoded[1], [NIL_FLAG]);
    assert_ne!(encoded[0], encoded[2]);
    assert_eq!(
        hash_group_key(
            &[Datum::UInt(u64::MAX)],
            &FieldType::new(FieldTypeCode::LongLong)
        )
        .unwrap()[0][0],
        VARINT_FLAG
    );
}

#[test]
fn test_decode_range() {
    let mut encoded = encode_key(&[Datum::Int(1)]).unwrap();
    encoded.push(MAX_FLAG);
    let (values, remain) = decode_range(&encoded, 2).unwrap();
    assert!(remain.is_empty());
    assert_eq!(values, [Datum::Int(1), Datum::MaxValue]);
}

#[test]
fn test_hash_chunk_row() {
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let string = varchar(Collation::Utf8Mb4GeneralCi);
    let left = [Datum::Int(1), Datum::new_string("a")];
    let equal = [Datum::Int(1), Datum::new_string("A ")];
    let different = [Datum::Int(2), Datum::new_string("a")];
    assert!(equal_rows(
        &left,
        &[integer.clone(), string.clone()],
        &[0, 1],
        &equal,
        &[integer.clone(), string.clone()],
        &[0, 1]
    )
    .unwrap());
    assert!(!equal_rows(
        &left,
        &[integer.clone(), string.clone()],
        &[0, 1],
        &different,
        &[integer, string],
        &[0, 1]
    )
    .unwrap());
}

#[test]
fn test_value_size_of_signed_int() {
    for value in [i64::MIN, -65, -64, -1, 0, 63, 64, i64::MAX] {
        let encoded = encode_value(&[Datum::Int(value)]).unwrap();
        assert_eq!(value_size_of_signed_int(value), encoded.len());
    }
}

#[test]
fn test_value_size_of_unsigned_int() {
    for value in [0, 127, 128, u64::MAX] {
        let encoded = encode_value(&[Datum::UInt(value)]).unwrap();
        assert_eq!(value_size_of_unsigned_int(value), encoded.len());
    }
}

#[test]
fn test_hash_chunk_columns() {
    let rows = vec![
        vec![Datum::Int(1)],
        vec![Datum::Null],
        vec![Datum::Int(2)],
    ];
    let (all, nulls) =
        hash_column(&rows, &FieldType::new(FieldTypeCode::LongLong), 0, None, false).unwrap();
    assert!(all.iter().all(Option::is_some));
    assert_eq!(nulls, [false, true, false]);
    let (selected, _) = hash_column(
        &rows,
        &FieldType::new(FieldTypeCode::LongLong),
        0,
        Some(&[true, false, true]),
        false,
    )
    .unwrap();
    assert!(selected[1].is_none());
}

#[test]
fn test_datum_hash_equals() {
    for (left, right, equal) in [
        (Datum::Int(1), Datum::Int(1), true),
        (
            Datum::new_decimal(Decimal::from_signed_literal("1.0")),
            Datum::new_decimal(Decimal::from_signed_literal("1.0")),
            true,
        ),
        (Datum::new_bytes(b"a"), Datum::new_bytes(b"b"), false),
    ] {
        assert_eq!(hash_code(&left) == hash_code(&right), equal);
    }
}

#[test]
fn test_encoder_new_collation_enabled() {
    let lower = Datum::new_collation_string("a", Collation::Utf8Mb4GeneralCi);
    let upper = Datum::new_collation_string("A ", Collation::Utf8Mb4GeneralCi);
    assert_eq!(
        Encoder::new(true).encode_key(std::slice::from_ref(&lower)),
        Encoder::new(true).encode_key(std::slice::from_ref(&upper))
    );
    assert_ne!(
        Encoder::new(false).encode_key(std::slice::from_ref(&lower)),
        Encoder::new(false).encode_key(std::slice::from_ref(&upper))
    );
}

#[test]
fn test_hash_group_key_collation() {
    let field_type = varchar(Collation::Utf8Mb4GeneralCi);
    assert_eq!(
        hash_group_key(&[Datum::new_string("a")], &field_type).unwrap(),
        hash_group_key(&[Datum::new_string("A ")], &field_type).unwrap()
    );
}

#[test]
fn test_hash_chunk_row_collation() {
    let field_type = varchar(Collation::Utf8Mb4GeneralCi);
    assert!(equal_rows(
        &[Datum::new_string("a")],
        std::slice::from_ref(&field_type),
        &[0],
        &[Datum::new_string("A ")],
        std::slice::from_ref(&field_type),
        &[0],
    )
    .unwrap());
}

#[test]
fn test_hash_chunk_columns_collation() {
    let rows = vec![
        vec![Datum::new_string("a")],
        vec![Datum::new_string("A ")],
    ];
    let (hashes, _) = hash_column(
        &rows,
        &varchar(Collation::Utf8Mb4GeneralCi),
        0,
        None,
        false,
    )
    .unwrap();
    assert_eq!(hashes[0], hashes[1]);
}

#[test]
fn source_serialize_keys_modes_and_nulls() {
    let rows = vec![
        vec![Datum::Int(-1), Datum::new_string("abc")],
        vec![Datum::Null, Datum::new_string("x")],
    ];
    let (keys, nulls) = serialize_keys(
        &rows,
        &[0, 1],
        &[
            FieldType::new(FieldTypeCode::LongLong),
            varchar(Collation::Utf8Mb4Bin),
        ],
        &[
            SerializeMode::NeedSignFlag,
            SerializeMode::KeepVarColumnLength,
        ],
        None,
    )
    .unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(nulls, [false, true]);
    assert_eq!(keys[0][0], INT_FLAG);
    assert_eq!(u32::from_le_bytes(keys[0][9..13].try_into().unwrap()), 3);
}

#[test]
fn source_hash_code_ignores_collation_mode() {
    let value = Datum::new_collation_string("A ", Collation::Utf8Mb4GeneralCi);
    let mut enabled = Vec::new();
    Encoder::new(true).hash_code(&mut enabled, &value);
    let mut disabled = Vec::new();
    Encoder::new(false).hash_code(&mut disabled, &value);
    assert_eq!(enabled, disabled);
}

#[test]
fn source_enum_set_hash_modes() {
    let enum_value = Datum::new_enum(
        tidb_datatype::MysqlEnum::new("A", 1),
        Collation::Utf8Mb4GeneralCi,
    );
    let enum_string = FieldType::new(FieldTypeCode::Enum)
        .with_elems(["A"])
        .with_collation(Collation::Utf8Mb4GeneralCi);
    let enum_integer = enum_string
        .clone()
        .with_flags(FieldTypeFlags::ENUM_SET_AS_INT);
    assert_eq!(
        encode_hash_datum(&enum_value, &enum_string).unwrap().0,
        COMPACT_BYTES_FLAG
    );
    assert_eq!(
        encode_hash_datum(&enum_value, &enum_integer).unwrap().0,
        UVARINT_FLAG
    );
}

/// Source `TestBenchDaily`: keep all six benchmark operations executable in
/// the ordinary test profile so benchmark drift is caught before measurement.
#[test]
fn test_bench_daily() {
    let values = (0..100).map(Datum::Int).collect::<Vec<_>>();
    let encoded = encode_value(&values).unwrap();
    assert_eq!(decode(&encoded, 100).unwrap().len(), 100);
    assert_eq!(decode(&encoded, 1).unwrap().len(), 100);

    let mut with_capacity = Vec::with_capacity(8);
    encode_int(&mut with_capacity, 10);
    let mut without_capacity = Vec::new();
    encode_int(&mut without_capacity, 10);
    assert_eq!(with_capacity, without_capacity);

    let decimal = Decimal::from_signed_literal("1211.1211113");
    let mut decimal_bytes = Vec::new();
    encode_decimal_fixed(&mut decimal_bytes, &decimal, 0, 0).unwrap();
    assert_eq!(decode_decimal(&decimal_bytes).unwrap().1, decimal);

    let mut raw = vec![BYTES_FLAG];
    encode_bytes(&mut raw, b"a");
    assert_eq!(
        decode_one_typed(&raw, &FieldType::new(FieldTypeCode::LongLong))
            .unwrap()
            .1,
        Datum::new_bytes(b"a")
    );
}
