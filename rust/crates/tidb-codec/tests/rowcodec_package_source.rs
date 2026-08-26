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

//! Exact package-level test obligations from `pkg/util/rowcodec`.

use std::collections::BTreeMap;

use tidb_codec::{
    append_datum_for_checksum, calculate_raw_checksum, decode_one, decode_row_to_datums,
    decode_row_to_map, decode_row_to_old_bytes, encode_row, encode_row_from_old,
    encode_row_with_checksum, encode_value, is_new_format, is_row_key, remove_keyspace_prefix,
    ColumnInfo, DatumColumn, DecodeRowOptions, Handle, RowChecksumPolicy, RowData, RowLayout,
};
use tidb_datatype::{
    BinaryJSON, BinaryLiteral, BinaryLiteralWidth, Collation, CoreTime, Datum, Decimal, FieldType,
    FieldTypeCode, MySqlDuration, MysqlEnum, MysqlSet, Time, TimeType, VectorFloat32,
};

fn field(code: FieldTypeCode) -> FieldType {
    FieldType::new(code)
}

fn column(id: i64, code: FieldTypeCode) -> ColumnInfo {
    ColumnInfo {
        id,
        is_pk_handle: false,
        virtual_generated: false,
        field_type: field(code),
    }
}

fn round_trip(ids: &[i64], values: &[Datum], columns: &[ColumnInfo]) -> Vec<Datum> {
    let utc = tidb_datatype::SessionTimeZone::utc();
    let mut encoded = Vec::new();
    encode_row(Some(&utc), ids, values, &mut encoded).unwrap();
    decode_row_to_datums(
        &encoded,
        columns,
        &DecodeRowOptions {
            timezone: Some(&utc),
            ..DecodeRowOptions::default()
        },
    )
    .unwrap()
    .values
}

/// Source: `common_test.go::TestRemoveKeyspacePrefix`.
#[test]
fn test_remove_keyspace_prefix() {
    let nextgen = hex("78000001748000fffffffffffe5f728000000000000002");
    let classic = &nextgen[4..];
    assert_eq!(remove_keyspace_prefix(&nextgen, true, true, false), nextgen);
    assert_eq!(
        remove_keyspace_prefix(&nextgen, false, true, false),
        classic
    );
    assert_eq!(
        remove_keyspace_prefix(&nextgen, false, false, false),
        nextgen
    );
    assert_eq!(
        remove_keyspace_prefix(&nextgen, false, false, true),
        classic
    );
    assert_eq!(remove_keyspace_prefix(classic, false, true, false), classic);
}

/// Source: `rowcodec_test.go::TestEncodeLargeSmallReuseBug`.
#[test]
fn test_encode_large_small_reuse_bug() {
    let mut buffer = Vec::new();
    encode_row(None, &[300], &[Datum::new_bytes([])], &mut buffer).unwrap();
    assert!(RowLayout::parse(&buffer).unwrap().0.header().is_large());

    buffer.clear();
    encode_row(None, &[1], &[Datum::Int(2)], &mut buffer).unwrap();
    let decoded = decode_row_to_map(&buffer, &[column(1, FieldTypeCode::LongLong)], None).unwrap();
    assert_eq!(decoded.get(&1), Some(&Datum::Int(2)));
    assert!(!RowLayout::parse(&buffer).unwrap().0.header().is_large());
}

/// Source: `rowcodec_test.go::TestDecodeRowWithHandle`.
#[test]
fn test_decode_row_with_handle() {
    for unsigned in [false, true] {
        let handle_type = field(FieldTypeCode::LongLong).with_unsigned(unsigned);
        let columns = [
            ColumnInfo {
                id: -1,
                is_pk_handle: true,
                virtual_generated: false,
                field_type: handle_type,
            },
            column(10, FieldTypeCode::LongLong),
        ];
        let mut encoded = Vec::new();
        encode_row(None, &[10], &[Datum::Int(1)], &mut encoded).unwrap();
        let decoded = decode_row_to_datums(
            &encoded,
            &columns,
            &DecodeRowOptions {
                handle_column_ids: &[-1],
                handle: Some(&Handle::Int(10_000)),
                ..DecodeRowOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            decoded.values[0],
            if unsigned {
                Datum::UInt(10_000)
            } else {
                Datum::Int(10_000)
            }
        );
        assert_eq!(decoded.values[1], Datum::Int(1));
    }
}

/// Source: `rowcodec_test.go::TestEncodeKindNullDatum`.
#[test]
fn test_encode_kind_null_datum() {
    let columns = [
        column(1, FieldTypeCode::LongLong),
        column(2, FieldTypeCode::LongLong),
    ];
    assert_eq!(
        round_trip(&[1, 2], &[Datum::Null, Datum::Int(2)], &columns),
        [Datum::Null, Datum::Int(2)]
    );
}

/// Source: `rowcodec_test.go::TestDecodeDecimalFspNotMatch`.
#[test]
fn test_decode_decimal_fsp_not_match() {
    let decimal = Decimal::from_literal("11.9900");
    let mut encoded = Vec::new();
    encode_row(None, &[1], &[Datum::Decimal(decimal)], &mut encoded).unwrap();
    let columns = [ColumnInfo {
        id: 1,
        is_pk_handle: false,
        virtual_generated: false,
        field_type: field(FieldTypeCode::NewDecimal)
            .with_flen(6)
            .with_decimal(3),
    }];
    let decoded = decode_row_to_datums(&encoded, &columns, &DecodeRowOptions::default()).unwrap();
    assert_eq!(
        decoded.values[0].as_decimal().unwrap().to_string(),
        "11.990"
    );
}

/// Source: `rowcodec_test.go::TestTypesNewRowCodec`.
#[test]
fn test_types_new_row_codec() {
    let decimal = Decimal::from_literal("11.9900");
    let json = BinaryJSON::parse(r#"{"a":2}"#).unwrap();
    let vector = VectorFloat32::must_create(vec![1.0, 2.5]);
    let duration = MySqlDuration::new(4, 0, 0, 0, 0).unwrap();
    let bit =
        BinaryLiteral::from_uint(3_223_600, Some(BinaryLiteralWidth::try_from(3_u8).unwrap()));
    let values = vec![
        Datum::Int(1),
        Datum::UInt(2),
        Datum::Real(2.0),
        Datum::Float32(6.0),
        Datum::new_collation_string(b"abc", Collation::Binary),
        Datum::Decimal(decimal),
        Datum::Duration(duration),
        Datum::Enum(MysqlEnum::new("n", 2), Collation::DEFAULT),
        Datum::Set(MysqlSet::new("n1", 1), Collation::DEFAULT),
        Datum::Bit(bit),
        Datum::Json(json),
        Datum::VectorFloat32(vector),
        Datum::Null,
    ];
    let ids = [1, 22, 3, 116, 24, 8, 16, 9, 117, 118, 14, 120, 11];
    let columns = vec![
        column(1, FieldTypeCode::LongLong),
        ColumnInfo {
            id: 22,
            field_type: field(FieldTypeCode::Short).with_unsigned(true),
            ..column(22, FieldTypeCode::Short)
        },
        column(3, FieldTypeCode::Double),
        column(116, FieldTypeCode::Float),
        column(24, FieldTypeCode::Blob),
        ColumnInfo {
            id: 8,
            field_type: field(FieldTypeCode::NewDecimal)
                .with_flen(6)
                .with_decimal(4),
            ..column(8, FieldTypeCode::NewDecimal)
        },
        ColumnInfo {
            id: 16,
            field_type: field(FieldTypeCode::Duration).with_decimal(0),
            ..column(16, FieldTypeCode::Duration)
        },
        ColumnInfo {
            id: 9,
            field_type: field(FieldTypeCode::Enum)
                .with_elems(["y", "n"])
                .with_collation(Collation::DEFAULT),
            ..column(9, FieldTypeCode::Enum)
        },
        ColumnInfo {
            id: 117,
            field_type: field(FieldTypeCode::Set)
                .with_elems(["n1", "n2"])
                .with_collation(Collation::DEFAULT),
            ..column(117, FieldTypeCode::Set)
        },
        ColumnInfo {
            id: 118,
            field_type: field(FieldTypeCode::Bit).with_flen(24),
            ..column(118, FieldTypeCode::Bit)
        },
        column(14, FieldTypeCode::Json),
        column(120, FieldTypeCode::VectorFloat32),
        column(11, FieldTypeCode::Null),
    ];

    for mutate in [0, 1, 2] {
        let mut case_ids = ids;
        let mut case_values = values.clone();
        if mutate == 1 {
            case_ids[0] = 300;
        } else if mutate == 2 {
            case_values[4] = Datum::new_bytes(vec![b'a'; 65_536]);
        }
        let mut case_columns = columns.clone();
        case_columns[0].id = case_ids[0];
        let decoded = round_trip(&case_ids, &case_values, &case_columns);
        if mutate == 2 {
            assert_eq!(
                decoded[4].as_raw_bytes(),
                Some(case_values[4].as_raw_bytes().unwrap())
            );
            assert_eq!(&decoded[..4], &case_values[..4]);
            assert_eq!(&decoded[5..], &case_values[5..]);
        } else {
            assert_eq!(decoded, case_values);
        }
    }
}

/// Source: `rowcodec_test.go::TestNilAndDefault`.
#[test]
fn test_nil_and_default() {
    let columns = [
        column(1, FieldTypeCode::LongLong),
        ColumnInfo {
            id: 2,
            field_type: field(FieldTypeCode::LongLong).with_unsigned(true),
            ..column(2, FieldTypeCode::LongLong)
        },
    ];
    let mut encoded = Vec::new();
    encode_row(None, &[1], &[Datum::Int(1)], &mut encoded).unwrap();
    assert_eq!(
        decode_row_to_map(&encoded, &columns, None).unwrap(),
        BTreeMap::from([(1, Datum::Int(1))])
    );
    let decoded = decode_row_to_datums(
        &encoded,
        &columns,
        &DecodeRowOptions {
            defaults: Some(&[Datum::Null, Datum::UInt(9)]),
            ..DecodeRowOptions::default()
        },
    )
    .unwrap();
    assert_eq!(decoded.values, [Datum::Int(1), Datum::UInt(9)]);
}

/// Source: `rowcodec_test.go::TestVarintCompatibility`.
#[test]
fn test_varint_compatibility() {
    let columns = [
        column(1, FieldTypeCode::LongLong),
        ColumnInfo {
            id: 2,
            field_type: field(FieldTypeCode::LongLong).with_unsigned(true),
            ..column(2, FieldTypeCode::LongLong)
        },
    ];
    let mut encoded = Vec::new();
    encode_row(
        None,
        &[1, 2],
        &[Datum::Int(1), Datum::UInt(1)],
        &mut encoded,
    )
    .unwrap();
    let old = decode_row_to_old_bytes(
        &encoded,
        &columns,
        &BTreeMap::from([(1, 0), (2, 1)]),
        &[],
        None,
        None,
    )
    .unwrap();
    assert_eq!(decode_one(&old[0]).unwrap().1, Datum::Int(1));
    assert_eq!(decode_one(&old[1]).unwrap().1, Datum::UInt(1));
}

/// Source: `rowcodec_test.go::TestCodecUtil`.
#[test]
fn test_codec_util() {
    let old = encode_value(&[
        Datum::Int(1),
        Datum::Int(1),
        Datum::Int(2),
        Datum::Int(2),
        Datum::Int(3),
        Datum::Int(3),
        Datum::Int(4),
        Datum::Null,
    ])
    .unwrap();
    let mut encoded = Vec::new();
    encode_row_from_old(None, &old, &mut encoded).unwrap();
    assert!(is_new_format(&encoded));
    assert!(!is_new_format(&old));
    let layout = RowLayout::parse(&encoded).unwrap().0;
    assert!(layout.column_is_null(4, true));
    assert!(!layout.column_is_null(1, true));
    assert!(layout.column_is_null(5, true));
    assert!(!layout.column_is_null(5, false));
    assert!(!is_row_key(b"bt"));
    assert!(!is_row_key(b"tr"));
}

/// Source: `rowcodec_test.go::TestOldRowCodec`.
#[test]
fn test_old_row_codec() {
    let columns = [
        column(1, FieldTypeCode::LongLong),
        column(2, FieldTypeCode::LongLong),
        column(3, FieldTypeCode::LongLong),
        column(4, FieldTypeCode::Null),
    ];
    let mut encoded = Vec::new();
    encode_row(
        None,
        &[1, 2, 3, 4],
        &[Datum::Int(1), Datum::Int(2), Datum::Int(3), Datum::Null],
        &mut encoded,
    )
    .unwrap();
    let old = decode_row_to_old_bytes(
        &encoded,
        &columns,
        &BTreeMap::from([(1, 0), (2, 1), (3, 2), (4, 3)]),
        &[],
        None,
        None,
    )
    .unwrap();
    assert_eq!(decode_one(&old[0]).unwrap().1, Datum::Int(1));
    assert_eq!(decode_one(&old[1]).unwrap().1, Datum::Int(2));
    assert_eq!(decode_one(&old[2]).unwrap().1, Datum::Int(3));
    assert_eq!(decode_one(&old[3]).unwrap().1, Datum::Null);

    let extra_handle_column = column(-1, FieldTypeCode::LongLong);
    let old = decode_row_to_old_bytes(
        &encoded,
        &[extra_handle_column],
        &BTreeMap::from([(-1, 0)]),
        &[-1],
        Some(&Handle::Int(42)),
        None,
    )
    .unwrap();
    assert_eq!(decode_one(&old[0]).unwrap().1, Datum::Int(42));
}

/// Source: `rowcodec_test.go::Test65535Bug`.
#[test]
fn test_65535_bug() {
    let value = vec![b'a'; 65_535];
    let decoded = round_trip(
        &[1],
        &[Datum::new_bytes(value.clone())],
        &[column(1, FieldTypeCode::String)],
    );
    assert_eq!(decoded[0].as_raw_bytes(), Some(value.as_slice()));
}

/// Source: `rowcodec_test.go::TestColumnEncode`.
#[test]
#[allow(clippy::approx_constant)]
fn test_column_encode() {
    fn length_value(value: &[u8]) -> Vec<u8> {
        [
            u32::try_from(value.len()).unwrap().to_le_bytes().as_slice(),
            value,
        ]
        .concat()
    }
    fn assert_encoding(field_type: FieldTypeCode, datum: Datum, expected: Vec<u8>) {
        let mut output = Vec::new();
        append_datum_for_checksum(None, &mut output, &datum, field_type).unwrap();
        assert_eq!(output, expected);
    }

    let integer_types = [
        FieldTypeCode::Tiny,
        FieldTypeCode::Short,
        FieldTypeCode::Long,
        FieldTypeCode::LongLong,
        FieldTypeCode::Int24,
    ];
    for field_type in integer_types {
        for value in [
            0_i64,
            42,
            -2,
            i8::MIN.into(),
            i16::MIN.into(),
            i32::MIN.into(),
            i64::MIN,
        ] {
            assert_encoding(
                field_type,
                Datum::Int(value),
                (value as u64).to_le_bytes().to_vec(),
            );
        }
    }
    for value in [
        i8::MAX as u64,
        u8::MAX as u64,
        i16::MAX as u64,
        u16::MAX as u64,
        i32::MAX as u64,
        u32::MAX as u64,
        i64::MAX as u64,
        u64::MAX,
        (1 << 23) - 1,
        (1 << 24) - 1,
    ] {
        assert_encoding(
            FieldTypeCode::LongLong,
            Datum::UInt(value),
            value.to_le_bytes().to_vec(),
        );
    }
    assert_encoding(
        FieldTypeCode::Year,
        Datum::Int(2023),
        2023_u64.to_le_bytes().to_vec(),
    );

    for field_type in [
        FieldTypeCode::Varchar,
        FieldTypeCode::VarString,
        FieldTypeCode::String,
        FieldTypeCode::Blob,
        FieldTypeCode::LongBlob,
        FieldTypeCode::MediumBlob,
        FieldTypeCode::TinyBlob,
    ] {
        assert_encoding(field_type, Datum::new_bytes(b"foo"), length_value(b"foo"));
        assert_encoding(field_type, Datum::new_bytes(b""), length_value(b""));
    }

    for (field_type, datum, expected) in [
        (
            FieldTypeCode::Float,
            Datum::Float32(f64::from(3.14_f32)),
            f64::from(3.14_f32).to_bits(),
        ),
        (FieldTypeCode::Double, Datum::Real(3.14), 3.14_f64.to_bits()),
    ] {
        assert_encoding(field_type, datum, expected.to_le_bytes().to_vec());
    }
    for field_type in [FieldTypeCode::Float, FieldTypeCode::Double] {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_encoding(field_type, Datum::Real(value), 0_f64.to_le_bytes().to_vec());
        }
    }

    assert_encoding(
        FieldTypeCode::Enum,
        Datum::Enum(MysqlEnum::new("", 0b010), Collation::Binary),
        0b010_u64.to_le_bytes().to_vec(),
    );
    assert_encoding(
        FieldTypeCode::Set,
        Datum::Set(MysqlSet::new("", 0b101), Collation::Binary),
        0b101_u64.to_le_bytes().to_vec(),
    );
    assert_encoding(
        FieldTypeCode::Bit,
        Datum::BinaryLiteral(BinaryLiteral::from(vec![0x12, 0x34])),
        0x1234_u64.to_le_bytes().to_vec(),
    );
    assert_encoding(
        FieldTypeCode::Bit,
        Datum::BinaryLiteral(BinaryLiteral::from(vec![
            0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0xff,
        ])),
        u64::MAX.to_le_bytes().to_vec(),
    );

    let core = CoreTime::from_date(2023, 1, 2, 3, 4, 5, 678_000);
    for (field_type, kind) in [
        (FieldTypeCode::Timestamp, TimeType::Timestamp),
        (FieldTypeCode::Datetime, TimeType::DateTime),
        (FieldTypeCode::Date, TimeType::Date),
        (FieldTypeCode::NewDate, TimeType::Date),
    ] {
        let value = Time::new(core, kind, 3).unwrap();
        assert_encoding(
            field_type,
            Datum::Time(value),
            length_value(value.to_string().as_bytes()),
        );
    }
    for (field_type, kind) in [
        (FieldTypeCode::Timestamp, TimeType::Timestamp),
        (FieldTypeCode::Datetime, TimeType::DateTime),
        (FieldTypeCode::Date, TimeType::Date),
        (FieldTypeCode::NewDate, TimeType::Date),
    ] {
        for core in [
            CoreTime::default(),
            CoreTime::from_date(1, 1, 1, 0, 0, 0, 0),
            CoreTime::from_date(9999, 12, 31, 23, 59, 59, 999_999),
        ] {
            let value = Time::new(core, kind, 6).unwrap();
            assert_encoding(
                field_type,
                Datum::Time(value),
                length_value(value.to_string().as_bytes()),
            );
        }
    }

    for duration in [
        MySqlDuration::new(8, 7, 0, 123_456, 6).unwrap(),
        MySqlDuration::from_nanoseconds(0, 0).unwrap(),
        MySqlDuration::maximum(3).unwrap(),
    ] {
        assert_encoding(
            FieldTypeCode::Duration,
            Datum::Duration(duration),
            length_value(duration.to_string().as_bytes()),
        );
    }
    for literal in ["0.000", "3.14", "-1.2", "-999999.999999", "999999.999999"] {
        let decimal = Decimal::from_literal(literal);
        assert_encoding(
            FieldTypeCode::NewDecimal,
            Datum::Decimal(decimal.clone()),
            length_value(decimal.to_string().as_bytes()),
        );
    }
    for literal in ["null", "42", r#"{"a":42,"foo":"bar"}"#] {
        let json = BinaryJSON::parse(literal).unwrap();
        assert_encoding(
            FieldTypeCode::Json,
            Datum::Json(json.clone()),
            length_value(json.to_string().as_bytes()),
        );
    }

    for field_type in [
        FieldTypeCode::Timestamp,
        FieldTypeCode::Datetime,
        FieldTypeCode::Date,
        FieldTypeCode::NewDate,
        FieldTypeCode::NewDecimal,
    ] {
        assert!(
            append_datum_for_checksum(None, &mut Vec::new(), &Datum::Int(1), field_type).is_err()
        );
    }
    for code in [FieldTypeCode::Unspecified, FieldTypeCode::Unknown(42)] {
        assert!(append_datum_for_checksum(None, &mut Vec::new(), &Datum::Int(1), code).is_err());
    }
    for code in [
        FieldTypeCode::Unspecified,
        FieldTypeCode::Tiny,
        FieldTypeCode::Short,
        FieldTypeCode::Long,
        FieldTypeCode::Float,
        FieldTypeCode::Double,
        FieldTypeCode::Null,
        FieldTypeCode::Timestamp,
        FieldTypeCode::LongLong,
        FieldTypeCode::Int24,
        FieldTypeCode::Date,
        FieldTypeCode::Duration,
        FieldTypeCode::Datetime,
        FieldTypeCode::Year,
        FieldTypeCode::NewDate,
        FieldTypeCode::Varchar,
        FieldTypeCode::Bit,
        FieldTypeCode::Json,
        FieldTypeCode::NewDecimal,
        FieldTypeCode::Enum,
        FieldTypeCode::Set,
        FieldTypeCode::TinyBlob,
        FieldTypeCode::MediumBlob,
        FieldTypeCode::LongBlob,
        FieldTypeCode::Blob,
        FieldTypeCode::VarString,
        FieldTypeCode::String,
        FieldTypeCode::Geometry,
        FieldTypeCode::Unknown(42),
    ] {
        let mut output = Vec::new();
        append_datum_for_checksum(None, &mut output, &Datum::Null, code).unwrap();
        assert!(output.is_empty());
    }
}

/// Source: `rowcodec_test.go::TestRowChecksum`.
#[test]
fn test_row_checksum() {
    let mut columns = vec![
        DatumColumn {
            id: 3,
            field_type: field(FieldTypeCode::Varchar),
            datum: Datum::new_bytes(b"foobar"),
        },
        DatumColumn {
            id: 1,
            field_type: field(FieldTypeCode::Null),
            datum: Datum::Null,
        },
        DatumColumn {
            id: 2,
            field_type: field(FieldTypeCode::Long),
            datum: Datum::Int(42),
        },
    ];
    columns.sort_by_key(|column| column.id);
    let mut row = RowData {
        columns,
        data: Vec::new(),
    };
    let checksum = row.checksum(None).unwrap();
    let encoded = row.encode(None).unwrap().to_vec();
    assert_eq!(checksum, crc32fast::hash(&encoded));
    assert_ne!(checksum, 0);
    assert_eq!(RowData::default().checksum(None).unwrap(), 0);
}

#[test]
fn row_data_preserves_caller_order() {
    let columns = vec![
        DatumColumn {
            id: 2,
            field_type: field(FieldTypeCode::Varchar),
            datum: Datum::new_bytes(b"second"),
        },
        DatumColumn {
            id: 1,
            field_type: field(FieldTypeCode::Varchar),
            datum: Datum::new_bytes(b"first"),
        },
    ];
    let mut row = RowData {
        columns,
        data: Vec::new(),
    };

    let encoded = row.encode(None).unwrap().to_vec();
    assert_eq!(row.columns[0].id, 2);
    assert_eq!(row.columns[1].id, 1);
    assert_eq!(encoded, b"\x06\0\0\0second\x05\0\0\0first");
    assert_eq!(row.checksum(None).unwrap(), crc32fast::hash(&encoded));
}

/// Source: `rowcodec_test.go::TestEncodeDecodeRowWithChecksum`.
#[test]
fn test_encode_decode_row_with_checksum() {
    let mut raw = Vec::new();
    encode_row(None, &[], &[], &mut raw).unwrap();
    let decoded = decode_row_to_datums(&raw, &[], &DecodeRowOptions::default()).unwrap();
    assert_eq!(decoded.checksum, None);

    raw.clear();
    encode_row_with_checksum(
        None,
        &[],
        &[],
        &RowChecksumPolicy::RawHandle(Handle::Int(1)),
        &mut raw,
    )
    .unwrap();
    let decoded = decode_row_to_datums(&raw, &[], &DecodeRowOptions::default()).unwrap();
    assert_ne!(decoded.checksum, Some(0));
    assert_eq!(decoded.checksum_version, 2);
    assert_eq!(
        calculate_raw_checksum(&raw, None, &[], &[], b"unused-key", &Handle::Int(1)).unwrap(),
        decoded.checksum.unwrap()
    );
}

/// Source: `rowcodec_test.go::TestDecodeWithCommitTS`.
#[test]
fn test_decode_with_commit_ts() {
    let columns = [
        column(1, FieldTypeCode::String),
        ColumnInfo {
            id: -10,
            field_type: field(FieldTypeCode::LongLong).with_unsigned(true),
            ..column(-10, FieldTypeCode::LongLong)
        },
        column(2, FieldTypeCode::String),
    ];
    let mut encoded = Vec::new();
    encode_row(
        None,
        &[1, 2],
        &[Datum::new_bytes(b"test1"), Datum::new_bytes(b"test2")],
        &mut encoded,
    )
    .unwrap();
    let decoded = decode_row_to_datums(
        &encoded,
        &columns,
        &DecodeRowOptions {
            commit_ts_column_id: Some(-10),
            commit_ts: 123_456,
            ..DecodeRowOptions::default()
        },
    )
    .unwrap();
    assert_eq!(
        decoded.values,
        [
            Datum::new_collation_string(b"test1", Collation::DEFAULT),
            Datum::UInt(123_456),
            Datum::new_collation_string(b"test2", Collation::DEFAULT),
        ]
    );
}

#[test]
fn enum_and_set_rows_preserve_non_utf8_element_bytes() {
    let columns = [
        ColumnInfo {
            id: 1,
            field_type: field(FieldTypeCode::Enum).with_elems([vec![0xff]]),
            ..column(1, FieldTypeCode::Enum)
        },
        ColumnInfo {
            id: 2,
            field_type: field(FieldTypeCode::Set).with_elems([vec![0xfe]]),
            ..column(2, FieldTypeCode::Set)
        },
    ];
    let decoded = round_trip(
        &[1, 2],
        &[
            Datum::Enum(MysqlEnum::new(vec![0xff], 1), Collation::Binary),
            Datum::Set(MysqlSet::new(vec![0xfe], 1), Collation::Binary),
        ],
        &columns,
    );
    match &decoded[0] {
        Datum::Enum(value, _) => assert_eq!(value.name_bytes(), &[0xff]),
        other => panic!("unexpected enum datum: {other:?}"),
    }
    match &decoded[1] {
        Datum::Set(value, _) => assert_eq!(value.name_bytes(), &[0xfe]),
        other => panic!("unexpected set datum: {other:?}"),
    }
}

/// Source: `bench_test.go::TestBenchDaily`.
#[test]
fn test_bench_daily() {
    let values = [
        Datum::Int(1),
        Datum::new_collation_string(b"abc", Collation::DEFAULT),
        Datum::Real(1.1),
    ];
    let columns = [
        column(1, FieldTypeCode::Long),
        column(2, FieldTypeCode::Varchar),
        column(3, FieldTypeCode::Double),
    ];
    for _ in 0..100 {
        let mut encoded = Vec::new();
        encode_row(None, &[1, 2, 3], &values, &mut encoded).unwrap();
        assert_eq!(
            decode_row_to_datums(&encoded, &columns, &DecodeRowOptions::default())
                .unwrap()
                .values,
            values
        );
    }
}

fn hex(input: &str) -> Vec<u8> {
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            (pair[0] as char).to_digit(16).unwrap() as u8 * 16
                + (pair[1] as char).to_digit(16).unwrap() as u8
        })
        .collect()
}

/// Source: `rowcodec_test.go::TestTypesNewRowCodec`.
///
/// Full-parity port of all three subtests (`small`, `largeColID`,
/// `largeData`) over the complete 18-entry `smallTestDataList`, including the
/// third "decode to old row bytes" phase whose converted output kinds
/// (`KindBytes`, packed-uint timestamps, nanosecond durations, numeric
/// enum/set/bit/year projections) the earlier partial port above omits.
#[test]
fn test_types_new_row_codec_full_table_with_old_format_outputs() {
    let utc = tidb_datatype::SessionTimeZone::utc();

    // Go td order and IDs, unchanged.
    let ids: &[i64] = &[
        1, 22, 3, 24, 25, 5, 16, 8, 12, 9, 14, 11, 2, 100, 116, 117, 118, 119,
    ];
    // Indices compared through their textual projection: decimal (Go compares
    // GetMysqlDecimal equality) and JSON.
    const TEXTUAL: [usize; 2] = [7, 10];
    const BLOB_ENTRY: usize = 3;

    let base_columns = vec![
        column(1, FieldTypeCode::LongLong),
        ColumnInfo {
            field_type: field(FieldTypeCode::Short).with_unsigned(true),
            ..column(22, FieldTypeCode::Short)
        },
        column(3, FieldTypeCode::Double),
        ColumnInfo {
            field_type: field(FieldTypeCode::Blob).with_collation(Collation::DEFAULT),
            ..column(24, FieldTypeCode::Blob)
        },
        ColumnInfo {
            field_type: field(FieldTypeCode::String).with_collation(Collation::DEFAULT),
            ..column(25, FieldTypeCode::String)
        },
        ColumnInfo {
            field_type: field(FieldTypeCode::Timestamp).with_decimal(6),
            ..column(5, FieldTypeCode::Timestamp)
        },
        ColumnInfo {
            field_type: field(FieldTypeCode::Duration).with_decimal(0),
            ..column(16, FieldTypeCode::Duration)
        },
        column(8, FieldTypeCode::NewDecimal),
        column(12, FieldTypeCode::Year),
        ColumnInfo {
            field_type: field(FieldTypeCode::Enum)
                .with_elems(["y", "n"])
                .with_collation(Collation::DEFAULT),
            ..column(9, FieldTypeCode::Enum)
        },
        column(14, FieldTypeCode::Json),
        column(11, FieldTypeCode::Null),
        column(2, FieldTypeCode::Null),
        column(100, FieldTypeCode::Null),
        column(116, FieldTypeCode::Float),
        ColumnInfo {
            field_type: field(FieldTypeCode::Set)
                .with_elems(["n1", "n2"])
                .with_collation(Collation::DEFAULT),
            ..column(117, FieldTypeCode::Set)
        },
        ColumnInfo {
            field_type: field(FieldTypeCode::Bit).with_flen(24),
            ..column(118, FieldTypeCode::Bit)
        },
        ColumnInfo {
            field_type: field(FieldTypeCode::VarString).with_collation(Collation::DEFAULT),
            ..column(119, FieldTypeCode::VarString)
        },
    ];

    let json = BinaryJSON::parse(r#"{"a":2}"#).unwrap();
    let base_inputs = vec![
        Datum::Int(1),
        Datum::UInt(1),
        Datum::Real(2.0),
        Datum::new_collation_string(b"abc", Collation::DEFAULT),
        Datum::new_collation_string(b"ab", Collation::DEFAULT),
        Datum::Time(
            Time::new(
                CoreTime::from_date(2011, 11, 10, 11, 11, 11, 999_999),
                TimeType::Timestamp,
                6,
            )
            .unwrap(),
        ),
        Datum::Duration(MySqlDuration::from_nanoseconds(14_400_000_000_000, 0).unwrap()),
        Datum::Decimal(Decimal::from_literal("11.9900")),
        Datum::Int(1999),
        Datum::Enum(MysqlEnum::new("n", 2), Collation::DEFAULT),
        Datum::Json(json.clone()),
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Float32(f64::from(6_f32)),
        Datum::Set(MysqlSet::new("n1", 1), Collation::DEFAULT),
        Datum::Bit(BinaryLiteral::from_uint(
            3_223_600,
            Some(BinaryLiteralWidth::try_from(3_u8).unwrap()),
        )),
        Datum::new_collation_string(b"", Collation::DEFAULT),
    ];
    // Expected value after `DecodeToBytes` + `codec.DecodeOne`: strings stay
    // octets, timestamps become their packed uint (Go: 1840446893366133311),
    // durations their nanosecond count, enum/set/bit their numeric values,
    // float32 is widened to the stored double, year stays a signed int.
    let base_old_expected = vec![
        Datum::Int(1),
        Datum::UInt(1),
        Datum::Real(2.0),
        Datum::Bytes(b"abc".to_vec()),
        Datum::Bytes(b"ab".to_vec()),
        Datum::UInt(1840446893366133311),
        Datum::Int(14_400_000_000_000),
        Datum::Decimal(Decimal::from_literal("11.9900")),
        Datum::Int(1999),
        Datum::UInt(2),
        Datum::Json(json),
        Datum::Null,
        Datum::Null,
        Datum::Null,
        Datum::Real(6.0),
        Datum::UInt(1),
        Datum::UInt(3_223_600),
        Datum::Bytes(Vec::new()),
    ];

    let expect_matches = |got: &Datum, want: &Datum, index: usize| {
        if index == TEXTUAL[0] {
            assert_eq!(
                got.as_decimal().unwrap().to_string(),
                want.as_decimal().unwrap().to_string()
            );
        } else if index == TEXTUAL[1] {
            match (got, want) {
                (Datum::Json(got), Datum::Json(want)) => {
                    assert_eq!(got.to_string(), want.to_string());
                }
                (got, _) => panic!("unexpected json datum: {got:?}"),
            }
        } else {
            assert_eq!(got, want);
        }
    };

    for case in 0..=2 {
        let mut case_ids = ids.to_vec();
        let mut case_columns = base_columns.clone();
        let mut case_inputs = base_inputs.clone();
        let mut case_old = base_old_expected.clone();
        if case == 1 {
            case_ids[0] = 300;
            case_columns[0].id = 300;
        } else if case == 2 {
            let oversized = vec![b'a'; u16::MAX as usize + 1];
            case_inputs[BLOB_ENTRY] =
                Datum::new_collation_string(oversized.clone(), Collation::DEFAULT);
            case_old[BLOB_ENTRY] = Datum::Bytes(oversized);
        }

        let mut encoded = Vec::new();
        encode_row(Some(&utc), &case_ids, &case_inputs, &mut encoded).unwrap();

        // Decode to datum map: every ID exists, including explicit NULLs.
        let map = decode_row_to_map(&encoded, &case_columns, Some(&utc)).unwrap();
        assert_eq!(map.len(), case_ids.len());
        for (index, &id) in case_ids.iter().enumerate() {
            let got = map
                .get(&id)
                .unwrap_or_else(|| panic!("missing map col {id}"));
            expect_matches(got, &case_inputs[index], index);
        }

        // Decode to chunk-equivalent datums.
        let decoded = decode_row_to_datums(
            &encoded,
            &case_columns,
            &DecodeRowOptions {
                timezone: Some(&utc),
                ..DecodeRowOptions::default()
            },
        )
        .unwrap();
        assert_eq!(decoded.values.len(), case_inputs.len());
        for (index, (got, want)) in decoded.values.iter().zip(&case_inputs).enumerate() {
            expect_matches(got, want, index);
        }

        // Decode to old row bytes, then back through `codec.DecodeOne`.
        let offsets: BTreeMap<i64, usize> = case_ids
            .iter()
            .enumerate()
            .map(|(index, &id)| (id, index))
            .collect();
        let old =
            decode_row_to_old_bytes(&encoded, &case_columns, &offsets, &[], None, None).unwrap();
        assert_eq!(old.len(), case_columns.len());
        for (index, bytes) in old.iter().enumerate() {
            let (remainder, got) = decode_one(bytes).unwrap();
            assert!(remainder.is_empty());
            expect_matches(&got, &case_old[index], index);
        }
    }
}

/// Source: `rowcodec_test.go::TestVarintCompatibility`.
///
/// Byte-exact counterpart of the existing value-level port above: Go asserts
/// each converted old-format column equals `tablecodec.EncodeValue(output)`.
#[test]
fn test_varint_compatibility_matches_encode_value_byte_for_byte() {
    let columns = [
        column(1, FieldTypeCode::LongLong),
        ColumnInfo {
            field_type: field(FieldTypeCode::LongLong).with_unsigned(true),
            ..column(2, FieldTypeCode::LongLong)
        },
    ];
    let mut encoded = Vec::new();
    encode_row(
        None,
        &[1, 2],
        &[Datum::Int(1), Datum::UInt(1)],
        &mut encoded,
    )
    .unwrap();
    let old = decode_row_to_old_bytes(
        &encoded,
        &columns,
        &BTreeMap::from([(1, 0), (2, 1)]),
        &[],
        None,
        None,
    )
    .unwrap();
    assert_eq!(old[0], encode_value(&[Datum::Int(1)]).unwrap());
    assert_eq!(old[1], encode_value(&[Datum::UInt(1)]).unwrap());
}

/// Source: `rowcodec_test.go::TestNilAndDefault`.
///
/// Covers the two branches the existing port skips: chunk decoding without a
/// default function fills missing columns with NULL, and byte decoding honours
/// per-column encoded defaults (`bdf`) verbatim.
#[test]
fn test_nil_and_default_missing_columns_take_null_or_encoded_defaults() {
    let columns = [
        column(1, FieldTypeCode::LongLong),
        ColumnInfo {
            field_type: field(FieldTypeCode::LongLong).with_unsigned(true),
            ..column(2, FieldTypeCode::LongLong)
        },
    ];
    let mut encoded = Vec::new();
    encode_row(None, &[1], &[Datum::Int(1)], &mut encoded).unwrap();

    let decoded = decode_row_to_datums(&encoded, &columns, &DecodeRowOptions::default()).unwrap();
    assert_eq!(decoded.values, [Datum::Int(1), Datum::Null]);

    let encoded_default = encode_value(&[Datum::UInt(9)]).unwrap();
    let old = decode_row_to_old_bytes(
        &encoded,
        &columns,
        &BTreeMap::from([(1, 0), (2, 1)]),
        &[],
        None,
        Some(&[None, Some(encoded_default.clone())]),
    )
    .unwrap();
    assert_eq!(old[0], encode_value(&[Datum::Int(1)]).unwrap());
    assert_eq!(old[1], encoded_default);
    let (remainder, value) = decode_one(&old[1]).unwrap();
    assert!(remainder.is_empty());
    assert_eq!(value, Datum::UInt(9));
}

/// Source: `rowcodec_test.go::TestDecodeRowWithHandle`.
///
/// Byte-decoder branch: the materialized handle column carries the declared
/// signedness (`Int(10000)` versus `UInt(10000)`) next to the stored column.
#[test]
fn test_decode_row_with_handle_materializes_typed_handle_into_old_bytes() {
    for unsigned in [false, true] {
        let columns = [
            ColumnInfo {
                id: -1,
                is_pk_handle: true,
                virtual_generated: false,
                field_type: field(FieldTypeCode::LongLong).with_unsigned(unsigned),
            },
            column(10, FieldTypeCode::LongLong),
        ];
        let mut encoded = Vec::new();
        encode_row(None, &[10], &[Datum::Int(1)], &mut encoded).unwrap();
        let old = decode_row_to_old_bytes(
            &encoded,
            &columns,
            &BTreeMap::from([(-1, 0), (10, 1)]),
            &[-1],
            Some(&Handle::Int(10_000)),
            None,
        )
        .unwrap();
        let (_, handle_value) = decode_one(&old[0]).unwrap();
        let (_, stored_value) = decode_one(&old[1]).unwrap();
        let expected_handle = if unsigned {
            Datum::UInt(10_000)
        } else {
            Datum::Int(10_000)
        };
        assert_eq!(handle_value, expected_handle);
        assert_eq!(stored_value, Datum::Int(1));
    }
}

/// Source: `rowcodec_test.go::TestColumnEncode` (`{"null", …}` and
/// `{"geometry", …}` rows).
///
/// TypeNull and TypeGeometry absorb any non-NULL datum into zero bytes.
#[test]
fn test_column_encode_type_null_and_geometry_absorb_any_datum() {
    for code in [FieldTypeCode::Null, FieldTypeCode::Geometry] {
        let mut output = Vec::new();
        append_datum_for_checksum(None, &mut output, &Datum::Int(1), code).unwrap();
        assert!(output.is_empty());
    }
}

/// Source: `rowcodec_test.go::TestRowChecksum`
/// (`unordered` subtest including its timestamp column).
///
/// Callers sort `RowData` by ID first; checksum equals CRC32 of the sorted
/// encoding even when the caller hands columns over out of order.
#[test]
fn test_row_checksum_unordered_columns_sort_before_crc() {
    let timestamp = Time::new(
        CoreTime::from_date(2023, 1, 2, 3, 4, 5, 678_000),
        TimeType::Timestamp,
        6,
    )
    .unwrap();
    let make_column = |id, datum| DatumColumn {
        id,
        field_type: match &datum {
            Datum::Null => field(FieldTypeCode::Null),
            Datum::Int(_) => field(FieldTypeCode::Long),
            Datum::Time(_) => field(FieldTypeCode::Timestamp),
            _ => field(FieldTypeCode::Varchar),
        },
        datum,
    };
    let mut row = RowData {
        columns: vec![
            make_column(3, Datum::new_collation_string(b"foobar", Collation::Binary)),
            make_column(1, Datum::Null),
            make_column(4, Datum::Time(timestamp)),
            make_column(2, Datum::Int(42)),
        ],
        data: Vec::new(),
    };
    assert_ne!(row.columns[0].id, row.columns[1].id);
    row.columns.sort_by_key(|column| column.id);
    let checksum = row.checksum(None).unwrap();
    let raw = row.encode(None).unwrap().to_vec();
    assert_eq!(checksum, crc32fast::hash(&raw));
}

/// Source: `main_test.go::EncodeFromOldRow`.
///
/// The package conversion helper short-circuits an already-new-format row
/// unchanged instead of re-encoding it.
#[test]
fn test_encode_from_old_row_passes_new_format_through_unchanged() {
    let mut new_format = Vec::new();
    encode_row(None, &[1], &[Datum::Int(1)], &mut new_format).unwrap();
    let mut output = Vec::new();
    encode_row_from_old(None, &new_format, &mut output).unwrap();
    assert_eq!(output, new_format);
}
