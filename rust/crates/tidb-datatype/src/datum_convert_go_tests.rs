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

//! Row-for-row translations of the original Go conversion tests.
//!
//! Go's `types.Convert` returns the best-effort value beside the error, and
//! `DefaultStmtNoWarningContext` turns truncation into an error. This crate
//! splits those: a recoverable condition is `Ok` with `Converted::event` set,
//! and only an unrecoverable one is `Err`. [`go_convert`] rejoins the two so
//! each row can assert Go's "value plus error" pair unchanged.

use chrono::Utc;

use super::*;
use crate::{parse_time, BinaryLiteralWidth, FieldTypeFlags, MysqlEnum, MysqlSet, TimeType};

/// Runs a conversion and reports it the way Go's `types.Convert` does:
/// the produced value (absent only when this crate could not produce one) and
/// whether `DefaultStmtNoWarningContext` would have returned a non-nil error.
fn go_convert(value: &Datum, target: &FieldType) -> (Option<Datum>, bool) {
    match value.convert_to(target, crate::DEFAULT_STATEMENT_FLAGS) {
        // `RoundedToScale` is Go's warning-only notice, never a returned error.
        Ok(converted) => (
            Some(converted.value),
            !matches!(
                converted.event,
                None | Some(ScalarConversionEvent::RoundedToScale)
            ),
        ),
        Err(_) => (None, true),
    }
}

/// Asserts a row that Go expects to succeed, returning the converted datum.
fn convert_ok(value: &Datum, target: &FieldType, row: &str) -> Datum {
    let (converted, failed) = go_convert(value, target);
    assert!(!failed, "{row}: expected no error");
    converted.unwrap_or_else(|| panic!("{row}: expected a value"))
}

/// Asserts a row that Go expects to fail, returning the best-effort datum.
fn convert_err(value: &Datum, target: &FieldType, row: &str) -> Option<Datum> {
    let (converted, failed) = go_convert(value, target);
    assert!(failed, "{row}: expected an error");
    converted
}

fn parse_datetime_fsp(input: &str, kind: TimeType, fsp: i64) -> Time {
    parse_time(input, kind, fsp, false, true, true, &Utc)
        .unwrap_or_else(|error| panic!("parse {input:?}: {error:?}"))
        .time
}

fn float_type(flen: i64, decimal: i64) -> FieldType {
    FieldType::new(FieldTypeCode::Float)
        .with_flen(flen)
        .with_decimal(decimal)
}

/// Complete translation of `pkg/types/convert_test.go::TestConvertTime`.
///
/// The three repeated DATETIME-to-TIMESTAMP rows are intentional: they are
/// repeated in the Go source table and remain separate parity obligations.
#[test]
fn test_convert_time() {
    let raw = CoreTime::from_date(2002, 3, 4, 4, 6, 7, 8);
    let zones = [
        SessionTimeZone::utc(),
        SessionTimeZone::Fixed {
            name: String::new(),
            offset_secs: 3 * 3_600,
        },
        SessionTimeZone::Local,
    ];
    let rows = [
        (
            TimeType::DateTime,
            FieldTypeCode::Timestamp,
            TimeType::Timestamp,
        ),
        (
            TimeType::DateTime,
            FieldTypeCode::Timestamp,
            TimeType::Timestamp,
        ),
        (
            TimeType::DateTime,
            FieldTypeCode::Timestamp,
            TimeType::Timestamp,
        ),
        (
            TimeType::Timestamp,
            FieldTypeCode::Datetime,
            TimeType::DateTime,
        ),
    ];
    assert_eq!(rows.len(), 4, "one entry per Go source row");

    for zone in zones {
        for (input_kind, target_code, expected_kind) in rows {
            let input = Datum::new_time(Time::new(raw, input_kind, 0).unwrap());
            let converted = input
                .convert_to_in(
                    &FieldType::new(target_code),
                    crate::DEFAULT_STATEMENT_FLAGS,
                    &zone,
                )
                .unwrap_or_else(|error| panic!("{input_kind:?} in {zone:?}: {error:?}"));
            assert_eq!(converted.event, None, "{input_kind:?} in {zone:?}");
            let Datum::Time(value) = converted.value else {
                panic!("{input_kind:?} in {zone:?}: expected a Time datum")
            };
            assert_eq!(value.kind(), expected_kind, "{input_kind:?} in {zone:?}");
            assert_eq!(value.core_time(), raw, "{input_kind:?} in {zone:?}");
        }
    }
}

/// Complete translation of `pkg/types/convert_test.go:44::TestConvertType`.
///
/// One Go row has no Rust counterpart and is named where it was dropped:
/// `Convert(&invalidMockType{}, ...)` exercises Go's `any` reflection fallback,
/// which a typed [`Datum`] makes unreachable.
#[test]
// The Go table's decimal rows literally use 3.1416; it is a table value, not
// an approximation of pi that should be spelled with the constant.
#[allow(clippy::approx_constant)]
fn test_convert_type() {
    // For TypeBlob and TypeString: over-length input truncates with
    // ErrDataTooLong.
    let blob4 = FieldType::new(FieldTypeCode::Blob)
        .with_flen(4)
        .with_collation(Collation::Utf8Bin);
    let value = convert_err(&Datum::new_string("123456"), &blob4, "blob(4) '123456'").unwrap();
    assert_eq!(value.as_raw_bytes(), Some(&b"1234"[..]));

    let binary_string4 = FieldType::new(FieldTypeCode::String)
        .with_flen(4)
        .with_collation(Collation::Binary);
    let value = convert_err(
        &Datum::new_string("12345"),
        &binary_string4,
        "binary char(4) '12345'",
    )
    .unwrap();
    assert_eq!(value.as_raw_bytes(), Some(&b"1234"[..]));

    // FLOAT(5,2) rounding and saturation.
    for (input, expected, fails) in [
        (111.114_f64, 111.11_f32, false),
        (999.999, 999.99, true),
        (-999.999, -999.99, true),
        (1111.11, 999.99, true),
        (999.916, 999.92, false),
        (999.914, 999.91, false),
        (999.9155, 999.92, false),
    ] {
        let row = format!("float(5,2) {input}");
        let (converted, failed) = go_convert(&Datum::Real(input), &float_type(5, 2));
        assert_eq!(failed, fails, "{row}: error expectation");
        assert_eq!(
            converted.unwrap(),
            Datum::Float32(f64::from(expected)),
            "{row}"
        );
    }

    // Nil converts to nil without error.
    let blob = FieldType::new(FieldTypeCode::Blob);
    assert_eq!(convert_ok(&Datum::Null, &blob, "blob NULL"), Datum::Null);

    // TypeDouble keeps the wider mantissa.
    let double = FieldType::new(FieldTypeCode::Double)
        .with_flen(5)
        .with_decimal(2);
    assert_eq!(
        convert_ok(&Datum::Real(999.9155), &double, "double(5,2) 999.9155"),
        Datum::Real(999.92)
    );

    // For TypeString: flen counts characters, and a binary charset returns
    // bytes rather than a collated string.
    let string3 = FieldType::new(FieldTypeCode::String).with_flen(3);
    let value = convert_err(&Datum::new_string("12345"), &string3, "char(3) '12345'").unwrap();
    assert_eq!(value.as_raw_bytes(), Some(&b"123"[..]));
    let binary_string3 = FieldType::new(FieldTypeCode::String)
        .with_flen(3)
        .with_collation(Collation::Binary);
    let value = convert_err(
        &Datum::new_string("12345"),
        &binary_string3,
        "binary char(3) '12345'",
    )
    .unwrap();
    assert_eq!(value.as_raw_bytes(), Some(&b"123"[..]));

    // For TypeDuration: the target's decimal rounds the fractional seconds,
    // including when the source is a DATETIME or a TIMESTAMP.
    let duration3 = FieldType::new(FieldTypeCode::Duration).with_decimal(3);
    let value = convert_ok(
        &Datum::new_string("10:11:12.123456"),
        &duration3,
        "time(3) '10:11:12.123456'",
    );
    assert_eq!(value.sql_string().unwrap(), "10:11:12.123");
    let duration1 = FieldType::new(FieldTypeCode::Duration).with_decimal(1);
    let rounded = convert_ok(&value, &duration1, "time(1) of time(3)");
    assert_eq!(rounded.sql_string().unwrap(), "10:11:12.1");

    let datetime = parse_datetime_fsp("2010-10-10 10:11:11.12345", TimeType::DateTime, 2);
    assert_eq!(datetime.to_string(), "2010-10-10 10:11:11.12");
    let value = convert_ok(
        &Datum::new_time(datetime),
        &duration1,
        "time(1) of datetime(2)",
    );
    assert_eq!(value.sql_string().unwrap(), "10:11:11.1");

    let timestamp = parse_datetime_fsp("2010-10-10 10:11:11.12345", TimeType::Timestamp, 2);
    assert_eq!(timestamp.to_string(), "2010-10-10 10:11:11.12");
    let value = convert_ok(
        &Datum::new_time(timestamp),
        &duration1,
        "time(1) of timestamp(2)",
    );
    assert_eq!(value.sql_string().unwrap(), "10:11:11.1");

    // For mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDate.
    let timestamp3 = FieldType::new(FieldTypeCode::Timestamp).with_decimal(3);
    let value = convert_ok(
        &Datum::new_string("2010-10-10 10:11:11.12345"),
        &timestamp3,
        "timestamp(3) string",
    );
    assert_eq!(value.sql_string().unwrap(), "2010-10-10 10:11:11.123");
    let timestamp1 = FieldType::new(FieldTypeCode::Timestamp).with_decimal(1);
    let value = convert_ok(&value, &timestamp1, "timestamp(1) of timestamp(3)");
    assert_eq!(value.sql_string().unwrap(), "2010-10-10 10:11:11.1");

    // For TypeLonglong.
    let bigint = FieldType::new(FieldTypeCode::LongLong);
    assert_eq!(
        convert_ok(&Datum::new_string("100"), &bigint, "bigint '100'"),
        Datum::Int(100)
    );
    // issue 4287.
    assert_eq!(
        convert_ok(
            &Datum::Real(2.0_f64.powi(63) - 1.0),
            &bigint,
            "bigint 2^63-1 as float"
        ),
        Datum::Int(i64::MAX)
    );
    let unsigned_bigint = bigint.clone().with_added_flags(FieldTypeFlags::UNSIGNED);
    assert_eq!(
        convert_ok(
            &Datum::new_string("100"),
            &unsigned_bigint,
            "bigint unsigned '100'"
        ),
        Datum::UInt(100)
    );
    // issue 3470: TIME and DATETIME convert through their numeric rendering.
    let duration = MySqlDuration::new(12, 59, 59, 555_000, 3).unwrap();
    assert_eq!(
        convert_ok(
            &Datum::new_duration(duration),
            &bigint,
            "bigint 12:59:59.555"
        ),
        Datum::Int(130_000)
    );
    let datetime = Time::from_date_checked(
        2017,
        1,
        1,
        12,
        59,
        59,
        555_000,
        TimeType::DateTime,
        crate::MAX_FSP,
    )
    .unwrap();
    assert_eq!(
        convert_ok(
            &Datum::new_time(datetime),
            &bigint,
            "bigint 2017-01-01 12:59:59.555"
        ),
        Datum::Int(20_170_101_130_000)
    );

    // For TypeBit.
    let bit24 = FieldType::new(FieldTypeCode::Bit).with_flen(24);
    assert_eq!(
        convert_ok(&Datum::new_string("100"), &bit24, "bit(24) '100'"),
        Datum::new_mysql_bit(BinaryLiteral::from_uint(
            3_223_600,
            Some(BinaryLiteralWidth::try_from(3_u8).unwrap())
        ))
    );
    assert_eq!(
        convert_ok(
            &Datum::new_binary_literal(BinaryLiteral::from_uint(100, None)),
            &bit24,
            "bit(24) b'100'"
        ),
        Datum::new_mysql_bit(BinaryLiteral::from_uint(
            100,
            Some(BinaryLiteralWidth::try_from(3_u8).unwrap())
        ))
    );

    let bit1 = FieldType::new(FieldTypeCode::Bit).with_flen(1);
    assert_eq!(
        convert_ok(&Datum::Int(1), &bit1, "bit(1) 1"),
        Datum::new_mysql_bit(BinaryLiteral::from_uint(
            1,
            Some(BinaryLiteralWidth::try_from(1_u8).unwrap())
        ))
    );
    convert_err(&Datum::Int(2), &bit1, "bit(1) 2");
    let bit0 = FieldType::new(FieldTypeCode::Bit).with_flen(0);
    convert_err(&Datum::Int(2), &bit0, "bit(0) 2");

    // For TypeNewDecimal.
    let decimal84 = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(8)
        .with_decimal(4);
    assert_eq!(
        convert_ok(&Datum::Real(3.1416), &decimal84, "decimal(8,4) 3.1416")
            .sql_string()
            .unwrap(),
        "3.1416"
    );
    assert_eq!(
        convert_ok(
            &Datum::new_string("3.1415926"),
            &decimal84,
            "decimal(8,4) '3.1415926'"
        )
        .sql_string()
        .unwrap(),
        "3.1416"
    );
    for (input, expected) in [
        ("99999", "9999.9999"),
        ("-10000", "-9999.9999"),
        ("1,999.00", "1.0000"),
        ("1,999,999.00", "1.0000"),
    ] {
        let row = format!("decimal(8,4) {input:?}");
        let value = convert_err(&Datum::new_string(input), &decimal84, &row).unwrap();
        assert_eq!(value.sql_string().unwrap(), expected, "{row}");
    }
    assert_eq!(
        convert_ok(
            &Datum::new_string("199.00 "),
            &decimal84,
            "decimal(8,4) '199.00 '"
        )
        .sql_string()
        .unwrap(),
        "199.0000"
    );

    // Go calls `Datum.ToDecimal` first with strict truncation and then with
    // `WithIgnoreTruncateErr(true)`. Rust returns the shared best-effort value
    // and recoverable event; the caller decides whether that event is an error.
    let malformed = Datum::new_string("hello").to_decimal().unwrap();
    assert_eq!(malformed.value, Decimal::from_int(0));
    assert_eq!(malformed.event, Some(ScalarConversionEvent::Truncated));

    // For TypeYear.
    let year = FieldType::new(FieldTypeCode::Year);
    assert_eq!(
        convert_ok(&Datum::new_string("2015"), &year, "year '2015'"),
        Datum::Int(2015)
    );
    assert_eq!(
        convert_ok(&Datum::Int(2015), &year, "year 2015"),
        Datum::Int(2015)
    );
    convert_err(&Datum::Int(1800), &year, "year 1800");
    let date = parse_datetime_fsp("2015-11-11", TimeType::Date, 0);
    assert_eq!(
        convert_ok(&Datum::new_time(date), &year, "year DATE 2015-11-11"),
        Datum::Int(2015)
    );
    assert_eq!(
        convert_ok(
            &Datum::new_duration(MySqlDuration::from_nanoseconds(0, 0).unwrap()),
            &year,
            "year ZeroDuration"
        ),
        Datum::Int(i64::from(chrono::Datelike::year(&Utc::now())))
    );
    assert_eq!(
        convert_ok(
            &Datum::new_json(BinaryJSON::parse("99").unwrap()),
            &year,
            "year JSON 99"
        ),
        Datum::Int(1999)
    );
    for json in ["-1", r#"{"key": 99}"#, "[99, 0, 1]"] {
        convert_err(
            &Datum::new_json(BinaryJSON::parse(json).unwrap()),
            &year,
            &format!("year JSON {json}"),
        );
    }

    // For enum.
    let enum_type = FieldType::new(FieldTypeCode::Enum).with_elems(["a", "b", "c"]);
    let collation = enum_type.collation();
    assert_eq!(
        convert_ok(&Datum::new_string("a"), &enum_type, "enum 'a'"),
        Datum::new_enum(parse_enum_value(&["a", "b", "c"], 1).unwrap(), collation)
    );
    assert_eq!(
        convert_ok(&Datum::Int(2), &enum_type, "enum 2"),
        Datum::new_enum(parse_enum_value(&["a", "b", "c"], 2).unwrap(), collation)
    );
    convert_err(&Datum::new_string("d"), &enum_type, "enum 'd'");
    convert_err(&Datum::Int(4), &enum_type, "enum 4");

    // For set.
    let set_type = FieldType::new(FieldTypeCode::Set).with_elems(["a", "b", "c"]);
    let collation = set_type.collation();
    for (input, expected) in [
        (Datum::new_string("a"), 1_u64),
        (Datum::Int(2), 2),
        (Datum::Int(3), 3),
    ] {
        let row = format!("set {input:?}");
        assert_eq!(
            convert_ok(&input, &set_type, &row),
            Datum::new_set(
                parse_set_value(&["a", "b", "c"], expected).unwrap(),
                collation
            ),
            "{row}"
        );
    }
    convert_err(&Datum::new_string("d"), &set_type, "set 'd'");
    convert_err(&Datum::Int(9), &set_type, "set 9");
}

/// Source: `pkg/types/convert_test.go::TestConvertToString`.
///
/// The final Go-only `invalidMockType` row exercises `ToString(any)`'s dynamic
/// reflection fallback. Rust's typed [`Datum`] cannot represent that input.
#[test]
fn test_convert_to_string() {
    let timestamp = parse_datetime_fsp("2011-11-10 11:11:11.999999", TimeType::Timestamp, 6);
    let parsed_duration = crate::parse_duration(b"11:11:11.999999", 6).unwrap();
    let duration =
        MySqlDuration::from_nanoseconds(parsed_duration.nanoseconds(), parsed_duration.fsp())
            .unwrap();
    let rows = [
        (Datum::new_string("0"), b"0".as_slice()),
        // Go's bool and native int both normalize to an integer Datum.
        (Datum::Int(1), b"1"),
        (Datum::new_string("false"), b"false"),
        (Datum::Int(0), b"0"),
        (Datum::Int(0), b"0"),
        (Datum::UInt(0), b"0"),
        (Datum::Float32(f64::from(1.6_f32)), b"1.6"),
        (Datum::Real(-0.6), b"-0.6"),
        (Datum::new_bytes([1]), b"\x01"),
        (
            Datum::new_binary_literal(BinaryLiteral::from_uint(0x004D_7953_514C, None)),
            b"MySQL",
        ),
        (
            Datum::new_binary_literal(BinaryLiteral::from_uint(0x41, None)),
            b"A",
        ),
        (
            Datum::new_enum(
                parse_enum_value(&["a"], 1).unwrap(),
                Collation::Utf8Mb4GeneralCi,
            ),
            b"a",
        ),
        (
            Datum::new_set(
                parse_set_value(&["a"], 1).unwrap(),
                Collation::Utf8Mb4GeneralCi,
            ),
            b"a",
        ),
        (Datum::new_time(timestamp), b"2011-11-10 11:11:11.999999"),
        (Datum::new_duration(duration), b"11:11:11.999999"),
        (
            Datum::new_decimal(Decimal::from_signed_literal("3.14159")),
            b"3.14159",
        ),
    ];
    assert_eq!(rows.len(), 16, "one entry per Go source value row");
    for (datum, expected) in rows {
        assert_eq!(datum.sql_bytes().unwrap(), expected, "{datum:?}");
    }

    let text = "你好，世界";
    for (flen, collation, expected) in [
        (5, Collation::Utf8Bin, "你好，世界"),
        (5, Collation::Utf8Mb4Bin, "你好，世界"),
        (4, Collation::Utf8Bin, "你好，世"),
        (4, Collation::Utf8Mb4Bin, "你好，世"),
        (15, Collation::Binary, "你好，世界"),
        (12, Collation::Binary, "你好，世"),
        (0, Collation::Binary, ""),
    ] {
        let target = FieldType::new(FieldTypeCode::Varchar)
            .with_flen(flen)
            .with_collation(collation);
        let (converted, failed) = go_convert(&Datum::new_string(text), &target);
        assert_eq!(failed, text != expected, "flen={flen} {collation:?}");
        assert_eq!(
            converted.unwrap().as_raw_bytes(),
            Some(expected.as_bytes()),
            "flen={flen} {collation:?}"
        );
    }
}

/// `convertToMysqlEnum`/`convertToMysqlSet` call `SetMysqlEnum`/`SetMysqlSet`
/// UNCONDITIONALLY and return the zero value *beside* `ErrTruncated`, so a
/// non-strict statement stores the empty ENUM/SET and only warns. Every
/// failing source kind takes that route: a name that is not a member, an
/// ordinal outside the declaration, and a number whose bits do not fit the
/// SET.
#[test]
fn go_test_convert_type_out_of_range_enum_and_set_keep_the_empty_value() {
    let enum_type = FieldType::new(FieldTypeCode::Enum).with_elems(["a", "b", "c"]);
    let collation = enum_type.collation();
    for input in [Datum::Int(4), Datum::Int(0), Datum::new_string("d")] {
        let row = format!("enum {input:?}");
        let value = convert_err(&input, &enum_type, &row)
            .expect("Go returns the zero Enum beside ErrTruncated");
        assert_eq!(
            value,
            Datum::new_enum(MysqlEnum::default(), collation),
            "{row}"
        );
    }

    let set_type = FieldType::new(FieldTypeCode::Set).with_elems(["a", "b", "c"]);
    let collation = set_type.collation();
    for input in [Datum::Int(9), Datum::new_string("d")] {
        let row = format!("set {input:?}");
        let value = convert_err(&input, &set_type, &row)
            .expect("Go returns the zero Set beside ErrTruncated");
        assert_eq!(
            value,
            Datum::new_set(MysqlSet::default(), collation),
            "{row}"
        );
    }

    // Go `ParseSetValue(elems, 0)` returns `zeroSet` with NO error, so the
    // zero SET is a legal stored value rather than a truncation.
    assert_eq!(
        convert_ok(&Datum::Int(0), &set_type, "set 0"),
        Datum::new_set(MysqlSet::default(), collation)
    );
}

/// Complete translation of `pkg/types/datum_test.go:124::TestToInt64`.
///
/// Go's helper runs with `WithIgnoreTruncateErr(true)` and requires no error,
/// so every row asserts the value while the recoverable event is ignored --
/// which is exactly what that flag means.
#[test]
fn test_to_int64() {
    fn to_int64(value: &Datum) -> i64 {
        value
            .to_i64_in(&crate::SessionTimeZone::utc())
            .unwrap_or_else(|error| panic!("{value:?}: {error:?}"))
            .value
    }

    let timestamp = parse_datetime_fsp("2011-11-10 11:11:11.999999", TimeType::Timestamp, 0);
    let parsed = crate::parse_duration(b"11:11:11.999999", 6).unwrap();
    let duration = MySqlDuration::from_nanoseconds(parsed.nanoseconds(), parsed.fsp()).unwrap();
    // Go's row is `Convert(3.1415926, decimal(_,5))`, whose stored value is
    // 3.14159.
    let decimal = Decimal::from_signed_literal("3.14159");

    let rows = [
        (Datum::new_string("0"), 0_i64),
        // Go's `int(0)` and `int64(0)` both become signed Datums in Rust,
        // but remain separate source rows.
        (Datum::Int(0), 0),
        (Datum::Int(0), 0),
        (Datum::UInt(0), 0),
        (Datum::Float32(f64::from(3.1_f32)), 3),
        (Datum::Real(3.1), 3),
        (
            Datum::new_binary_literal(BinaryLiteral::from_uint(100, None)),
            100,
        ),
        (
            Datum::new_enum(
                parse_enum_value(&["a"], 1).unwrap(),
                Collation::Utf8Mb4GeneralCi,
            ),
            1,
        ),
        (
            Datum::new_set(
                parse_set_value(&["a"], 1).unwrap(),
                Collation::Utf8Mb4GeneralCi,
            ),
            1,
        ),
        (Datum::new_json(BinaryJSON::parse("3").unwrap()), 3),
        (Datum::new_time(timestamp), 20_111_110_111_112),
        (Datum::new_duration(duration), 111_112),
        (Datum::new_decimal(decimal), 3),
    ];
    assert_eq!(rows.len(), 13, "one entry per Go source row");
    for (value, expected) in rows {
        assert_eq!(to_int64(&value), expected, "{value:?}");
    }

    // The second half of `toSignedInteger`'s own contract comment
    // (`datum.go:2010`, `:2023`), which the Go table above does not reach:
    // the carry is applied to the TEMPORAL value, so it propagates through
    // the sexagesimal fields rather than producing a 60th second.
    //   2011-11-10 11:59:59.999999 -> 20111110120000
    //   11:59:59.999999            -> 120000
    let carry = parse_datetime_fsp("2011-11-10 11:59:59.999999", TimeType::DateTime, 6);
    assert_eq!(to_int64(&Datum::new_time(carry)), 20_111_110_120_000);
    let midnight = parse_datetime_fsp("2011-12-31 23:59:59.999999", TimeType::DateTime, 6);
    assert_eq!(to_int64(&Datum::new_time(midnight)), 20_120_101_000_000);
    let parsed = crate::parse_duration(b"11:59:59.999999", 6).unwrap();
    let carry_duration =
        MySqlDuration::from_nanoseconds(parsed.nanoseconds(), parsed.fsp()).unwrap();
    assert_eq!(to_int64(&Datum::new_duration(carry_duration)), 120_000);
}

/// The session zone reaches `Datum.ConvertTo`'s integer and DURATION arms.
///
/// Go's `toSignedInteger` rounds a `KindMysqlTime` with
/// `RoundFrac(ctx, DefaultFsp)` and `StrToDuration(ctx, str, fsp)` parses a
/// 12-or-more-digit literal as a DATETIME first; both round through
/// `t.GoTime(ctx.Location())`. When the sub-second carry lands exactly on a
/// DST transition instant, the wall clock read back afterwards is the
/// SESSION zone's. Both arms used to hardcode UTC here.
///
/// Every expectation below is a verbatim capture from
/// `pkg/types` run against this tree:
///
/// ```text
/// Time.RoundFrac(ctx, DefaultFsp).ToNumber()
///   2011-03-13 01:59:59.999999  UTC=20110313020000  America/LA=20110313030000
///   2011-11-06 01:59:59.999999  UTC=20111106020000  America/LA=20111106010000
/// StrToDuration(ctx, str, fsp=0)
///   "20110313015959.999999"     UTC=2011-03-13 02:00:00  America/LA=2011-03-13 03:00:00
///   "20111106015959.999999"     UTC=2011-11-06 02:00:00  America/LA=2011-11-06 01:00:00
/// ```
///
/// `2011-03-13 02:00:00` does not exist in America/Los_Angeles (spring
/// forward), and `2011-11-06 01:00:00` is the repeated hour (fall back).
#[test]
fn session_zone_reaches_signed_and_duration_conversion() {
    let la = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
    let utc = SessionTimeZone::utc();
    let long_long = FieldType::new(FieldTypeCode::LongLong);

    for (input, in_utc, in_la) in [
        (
            "2011-03-13 01:59:59.999999",
            20110313020000_i64,
            20110313030000_i64,
        ),
        ("2011-11-06 01:59:59.999999", 20111106020000, 20111106010000),
    ] {
        let time = Datum::Time(parse_datetime_fsp(input, TimeType::DateTime, 6));
        for (zone, expected) in [(&utc, in_utc), (&la, in_la)] {
            let got = time
                .convert_to_in(&long_long, crate::DEFAULT_STATEMENT_FLAGS, zone)
                .unwrap_or_else(|error| panic!("{input} in {zone:?}: {error:?}"))
                .value;
            assert_eq!(got, Datum::Int(expected), "{input} in {zone:?}");
        }
    }

    // `StrToDuration`'s DATETIME branch: the literal is >= 12 digits, so the
    // fsp=0 rounding carry crosses the same transition instants.
    let duration_type = FieldType::new(FieldTypeCode::Duration).with_decimal(0);
    for (input, in_utc, in_la) in [
        ("20110313015959.999999", "02:00:00", "03:00:00"),
        ("20111106015959.999999", "02:00:00", "01:00:00"),
    ] {
        let text = Datum::new_string(input.to_string());
        for (zone, expected) in [(&utc, in_utc), (&la, in_la)] {
            let got = text
                .convert_to_in(&duration_type, crate::DEFAULT_STATEMENT_FLAGS, zone)
                .unwrap_or_else(|error| panic!("{input} in {zone:?}: {error:?}"))
                .value;
            let Datum::Duration(got) = got else {
                panic!("{input} in {zone:?}: expected a Duration, got {got:?}")
            };
            assert_eq!(got.to_string(), expected, "{input} in {zone:?}");
        }
    }
}
