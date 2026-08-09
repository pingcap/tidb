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

/// Complete translation of `pkg/types/convert_test.go::TestConvert`.
#[test]
fn test_convert() {
    fn check(input: Datum, code: FieldTypeCode, unsigned: bool, denied: bool, expected: &str) {
        let target = if unsigned {
            FieldType::new(code).with_added_flags(FieldTypeFlags::UNSIGNED)
        } else {
            FieldType::new(code)
        };
        let flags = if denied {
            crate::DEFAULT_STATEMENT_FLAGS
        } else {
            crate::DEFAULT_STATEMENT_FLAGS.with_ignore_truncate_err(true)
        };
        let label = format!("{input:?} -> {code:?} unsigned={unsigned}");
        let (value, failed) = match input.convert_to(&target, flags) {
            Ok(converted) => (
                Some(converted.value),
                !matches!(
                    converted.event,
                    None | Some(ScalarConversionEvent::RoundedToScale)
                ),
            ),
            Err(DatumValueError::IncorrectTemporal(value)) => (Some(Datum::new_time(value)), true),
            Err(error) => panic!("{label}: Go returns a value beside the error: {error:?}"),
        };
        if denied {
            assert!(failed, "{label}: expected an error");
        }
        let actual = value
            .unwrap_or_else(|| panic!("{label}: expected Go's error-side value"))
            .sql_string()
            .unwrap_or_else(|error| panic!("{label}: stringify failed: {error}"));
        assert_eq!(actual, expected, "{label}");
    }

    let mut source_rows = 0_usize;
    macro_rules! sa {
        ($code:ident, $value:expr, $expected:expr) => {{
            source_rows += 1;
            check($value, FieldTypeCode::$code, false, false, $expected);
        }};
    }
    macro_rules! sd {
        ($code:ident, $value:expr, $expected:expr) => {{
            source_rows += 1;
            check($value, FieldTypeCode::$code, false, true, $expected);
        }};
    }
    macro_rules! ua {
        ($code:ident, $value:expr, $expected:expr) => {{
            source_rows += 1;
            check($value, FieldTypeCode::$code, true, false, $expected);
        }};
    }
    macro_rules! ud {
        ($code:ident, $value:expr, $expected:expr) => {{
            source_rows += 1;
            check($value, FieldTypeCode::$code, true, true, $expected);
        }};
    }
    let literal = |value| Datum::new_binary_literal(BinaryLiteral::from_uint(value, None));

    // Integer ranges.
    sd!(Tiny, Datum::Int(-129), "-128");
    sa!(Tiny, Datum::Int(-128), "-128");
    sa!(Tiny, Datum::Int(127), "127");
    sd!(Tiny, Datum::Int(128), "127");
    sa!(Tiny, literal(127), "127");
    sd!(Tiny, literal(128), "127");
    ud!(Tiny, Datum::Int(-1), "255");
    ua!(Tiny, Datum::Int(0), "0");
    ua!(Tiny, Datum::Int(255), "255");
    ud!(Tiny, Datum::Int(256), "255");
    ua!(Tiny, literal(0), "0");
    ua!(Tiny, literal(255), "255");
    ud!(Tiny, literal(256), "255");

    sd!(Short, Datum::Int(i64::from(i16::MIN) - 1), "-32768");
    sa!(Short, Datum::Int(i64::from(i16::MIN)), "-32768");
    sa!(Short, Datum::Int(i64::from(i16::MAX)), "32767");
    sd!(Short, Datum::Int(i64::from(i16::MAX) + 1), "32767");
    sa!(Short, literal(i16::MAX as u64), "32767");
    sd!(Short, literal(i16::MAX as u64 + 1), "32767");
    ud!(Short, Datum::Int(-1), "65535");
    ua!(Short, Datum::Int(0), "0");
    ua!(Short, Datum::UInt(u64::from(u16::MAX)), "65535");
    ud!(Short, Datum::UInt(u64::from(u16::MAX) + 1), "65535");
    ua!(Short, literal(0), "0");
    ua!(Short, literal(u64::from(u16::MAX)), "65535");
    ud!(Short, literal(u64::from(u16::MAX) + 1), "65535");

    sd!(Int24, Datum::Int(-(1_i64 << 23) - 1), "-8388608");
    sa!(Int24, Datum::Int(-(1_i64 << 23)), "-8388608");
    sa!(Int24, Datum::Int((1_i64 << 23) - 1), "8388607");
    sd!(Int24, Datum::Int(1_i64 << 23), "8388607");
    sa!(Int24, literal((1_u64 << 23) - 1), "8388607");
    sd!(Int24, literal(1_u64 << 23), "8388607");
    ud!(Int24, Datum::Int(-1), "16777215");
    ua!(Int24, Datum::Int(0), "0");
    ua!(Int24, Datum::Int((1_i64 << 24) - 1), "16777215");
    ud!(Int24, Datum::Int(1_i64 << 24), "16777215");
    ua!(Int24, literal(0), "0");
    ua!(Int24, literal((1_u64 << 24) - 1), "16777215");
    ud!(Int24, literal(1_u64 << 24), "16777215");

    sd!(Long, Datum::Int(i64::from(i32::MIN) - 1), "-2147483648");
    sa!(Long, Datum::Int(i64::from(i32::MIN)), "-2147483648");
    sa!(Long, Datum::Int(i64::from(i32::MAX)), "2147483647");
    sd!(Long, Datum::UInt(u64::MAX), "2147483647");
    sd!(Long, Datum::Int(i64::from(i32::MAX) + 1), "2147483647");
    sd!(
        Long,
        Datum::new_string("1343545435346432587475"),
        "2147483647"
    );
    sa!(Long, literal(i32::MAX as u64), "2147483647");
    sd!(Long, literal(u64::MAX), "2147483647");
    sd!(Long, literal(i32::MAX as u64 + 1), "2147483647");
    ud!(Long, Datum::Int(-1), "4294967295");
    ua!(Long, Datum::Int(0), "0");
    ua!(Long, Datum::UInt(u64::from(u32::MAX)), "4294967295");
    ud!(Long, Datum::UInt(u64::from(u32::MAX) + 1), "4294967295");
    ua!(Long, literal(0), "0");
    ua!(Long, literal(u64::from(u32::MAX)), "4294967295");
    ud!(Long, literal(u64::from(u32::MAX) + 1), "4294967295");

    sd!(
        LongLong,
        Datum::Real(i64::MIN as f64 * 1.1),
        "-9223372036854775808"
    );
    sa!(LongLong, Datum::Int(i64::MIN), "-9223372036854775808");
    sa!(LongLong, Datum::Int(i64::MAX), "9223372036854775807");
    sd!(
        LongLong,
        Datum::Real(i64::MAX as f64 * 1.1),
        "9223372036854775807"
    );
    sa!(LongLong, literal(i64::MAX as u64), "9223372036854775807");
    sd!(
        LongLong,
        literal(i64::MAX as u64 + 1),
        "9223372036854775807"
    );
    ua!(LongLong, Datum::Int(-1), "18446744073709551615");
    ua!(LongLong, Datum::Int(0), "0");
    ua!(LongLong, Datum::UInt(u64::MAX), "18446744073709551615");
    ud!(
        LongLong,
        Datum::Real(u64::MAX as f64 * 1.1),
        "18446744073709551615"
    );
    ua!(LongLong, literal(0), "0");
    ua!(LongLong, literal(u64::MAX), "18446744073709551615");

    // Integer from string.
    for (input, expected) in [
        ("\t  234  ", "234"),
        (" 2.35e3  ", "2350"),
        (" 2.e3  ", "2000"),
        (" -2.e3  ", "-2000"),
        (" 2e2  ", "200"),
        (" 0.002e3  ", "2"),
        (" .002e3  ", "2"),
        (" 20e-2  ", "0"),
        (" -20e-2  ", "0"),
        (" +2.51 ", "3"),
        (" -9999.5 ", "-10000"),
        (" 999.4", "999"),
        (" -3.58", "-4"),
    ] {
        sa!(Long, Datum::new_string(input), expected);
    }
    sd!(Long, Datum::new_string(" 1a "), "1");
    sd!(Long, Datum::new_string(" +1+ "), "1");

    // Integer from float.
    sa!(Long, Datum::Real(234.5456), "235");
    sa!(Long, Datum::Real(-23.45), "-23");
    ua!(LongLong, Datum::Real(234.5456), "235");
    ud!(LongLong, Datum::Real(-23.45), "18446744073709551593");

    // Float from string and numeric kinds.
    sa!(Float, Datum::new_string("23.523"), "23.523");
    sa!(Float, Datum::Int(123), "123");
    sa!(Float, Datum::UInt(123), "123");
    sa!(Float, Datum::Int(123), "123");
    sa!(Float, Datum::Float32(f64::from(123_f32)), "123");
    sa!(Float, Datum::Real(123.0), "123");
    sa!(Double, Datum::new_string(" -23.54"), "-23.54");
    sd!(Double, Datum::new_string("-23.54a"), "-23.54");
    sd!(Double, Datum::new_string("-23.54e2e"), "-2354");
    sd!(Double, Datum::new_string("+.e"), "0");
    sa!(Double, Datum::new_string("1e+1"), "10");

    // YEAR.
    sd!(Year, Datum::Int(123), "1901");
    sd!(Year, Datum::Int(3000), "2155");
    sa!(Year, Datum::new_string("2000"), "2000");
    sa!(Year, Datum::new_string("abc"), "0");
    sa!(Year, Datum::new_string("00abc"), "2000");
    sa!(Year, Datum::new_string("0019"), "2019");
    sa!(Year, Datum::Int(2155), "2155");
    sa!(Year, Datum::Real(2155.123), "2155");
    sd!(Year, Datum::Int(2156), "2155");
    sd!(Year, Datum::Real(123.123), "1901");
    sd!(Year, Datum::Int(1900), "1901");
    sa!(Year, Datum::Int(1901), "1901");
    sa!(Year, Datum::Real(1900.567), "1901");
    sd!(Year, Datum::Real(1900.456), "1901");
    sa!(Year, Datum::Int(0), "0");
    for input in ["0", "00", " 0", " 00"] {
        sa!(Year, Datum::new_string(input), "2000");
    }
    sa!(Year, Datum::new_string(" 000"), "0");
    sa!(Year, Datum::new_string(" 0000 "), "2000");
    for input in [" 0ab", "00bc", "000a"] {
        sa!(Year, Datum::new_string(input), "0");
    }
    sa!(Year, Datum::new_string(" 000a "), "2000");
    sa!(Year, Datum::Int(1), "2001");
    sa!(Year, Datum::new_string("1"), "2001");
    sa!(Year, Datum::new_string("01"), "2001");
    sa!(Year, Datum::Int(69), "2069");
    sa!(Year, Datum::new_string("69"), "2069");
    sa!(Year, Datum::Int(70), "1970");
    sa!(Year, Datum::new_string("70"), "1970");
    sa!(Year, Datum::Int(99), "1999");
    sa!(Year, Datum::new_string("99"), "1999");
    sd!(Year, Datum::Int(100), "1901");
    sd!(
        Year,
        Datum::new_string("99999999999999999999999999999999999"),
        "0"
    );

    // Time from string and temporal/numeric zero values.
    let zero_datetime = Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap();
    let zero_duration = MySqlDuration::from_nanoseconds(0, 0).unwrap();
    let temporal_decimal = Decimal::from_literal("20010101100000.123456");
    sa!(Date, Datum::new_string("2012-08-23"), "2012-08-23");
    sa!(
        Datetime,
        Datum::new_string("2012-08-23 12:34:03.123456"),
        "2012-08-23 12:34:03"
    );
    sa!(
        Datetime,
        Datum::new_time(zero_datetime),
        "0000-00-00 00:00:00"
    );
    sa!(Datetime, Datum::Int(0), "0000-00-00 00:00:00");
    sa!(
        Datetime,
        Datum::new_decimal(temporal_decimal.clone()),
        "2001-01-01 10:00:00"
    );
    sa!(
        Timestamp,
        Datum::new_string("2012-08-23 12:34:03.123456"),
        "2012-08-23 12:34:03"
    );
    sa!(
        Timestamp,
        Datum::new_decimal(temporal_decimal),
        "2001-01-01 10:00:00"
    );
    sa!(Duration, Datum::new_string("10:11:12"), "10:11:12");
    sa!(Duration, Datum::new_time(zero_datetime), "00:00:00");
    sa!(Duration, Datum::new_duration(zero_duration), "00:00:00");
    sa!(Duration, Datum::Int(0), "00:00:00");

    sd!(Date, Datum::new_string("2012-08-x"), "0000-00-00");
    sd!(
        Datetime,
        Datum::new_string("2012-08-x"),
        "0000-00-00 00:00:00"
    );
    sd!(
        Timestamp,
        Datum::new_string("2012-08-x"),
        "0000-00-00 00:00:00"
    );
    sd!(Duration, Datum::new_string("2012-08-x"), "00:20:12");
    sd!(Duration, Datum::new_string("0000-00-00"), "00:00:00");
    sd!(Duration, Datum::new_string("1234abc"), "00:12:34");

    // String from string and the other source kinds in the Go table.
    sa!(String, Datum::new_string("abc"), "abc");
    sa!(String, Datum::Int(5678), "5678");
    sa!(String, Datum::new_duration(zero_duration), "00:00:00");
    sa!(
        String,
        Datum::new_time(zero_datetime),
        "0000-00-00 00:00:00"
    );
    sa!(String, Datum::new_bytes("123"), "123");

    // NewDecimal.
    sa!(NewDecimal, Datum::Int(123), "123");
    sa!(NewDecimal, Datum::Int(123), "123");
    sa!(NewDecimal, Datum::UInt(123), "123");
    sa!(NewDecimal, Datum::Float32(f64::from(123_f32)), "123");
    sa!(NewDecimal, Datum::Real(123.456), "123.456");
    sa!(NewDecimal, Datum::new_string("-123.456"), "-123.456");
    sa!(
        NewDecimal,
        Datum::new_decimal(Decimal::from_literal("12300000")),
        "12300000"
    );
    sa!(
        NewDecimal,
        Datum::new_decimal(Decimal::from_literal("-0.00123")),
        "-0.00123"
    );

    assert_eq!(source_rows, 163, "one entry per Go source row");
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
fn out_of_range_enum_and_set_keep_the_empty_value() {
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
