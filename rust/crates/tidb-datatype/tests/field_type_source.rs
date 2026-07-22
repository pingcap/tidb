// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-derived FieldType assertions from parser and runtime Go tests.

use tidb_datatype::{
    agg_field_type, aggregate_eval_type, default_field_type_for_value, enum_set_display_length,
    merge_field_type, EvalType, FieldType, FieldTypeCode as C, FieldTypeFlags as F,
    FieldTypeValue as V, UNSPECIFIED_LENGTH,
};

fn all_types() -> Vec<FieldType> {
    [
        C::Unspecified,
        C::Tiny,
        C::Short,
        C::Long,
        C::Float,
        C::Double,
        C::Null,
        C::Timestamp,
        C::LongLong,
        C::Int24,
        C::Date,
        C::Duration,
        C::Datetime,
        C::Year,
        C::NewDate,
        C::Varchar,
        C::Bit,
        C::Json,
        C::NewDecimal,
        C::Enum,
        C::Set,
        C::TinyBlob,
        C::MediumBlob,
        C::LongBlob,
        C::Blob,
        C::VarString,
        C::String,
        C::Geometry,
    ]
    .into_iter()
    .map(FieldType::new)
    .collect()
}

fn assert_field(
    field: &FieldType,
    code: C,
    flen: i64,
    decimal: i64,
    charset: &str,
    collate: &str,
    flags: u32,
) {
    assert_eq!(field.code(), code);
    assert_eq!(field.flen(), flen);
    assert_eq!(field.decimal(), decimal);
    assert_eq!(field.charset_name(), charset);
    assert_eq!(field.collation_name(), collate);
    assert_eq!(field.flags(), flags);
}

/// Go: pkg/parser/types/field_type_test.go:30 TestFieldType.
#[test]
fn parser_field_type() {
    let empty = FieldType::parser(C::Duration);
    assert_eq!(empty.flen(), UNSPECIFIED_LENGTH);
    assert_eq!(empty.decimal(), UNSPECIFIED_LENGTH);
    assert_eq!(
        FieldType::parser(C::Duration).with_decimal(5).to_string(),
        "time(5)"
    );
    let integer = FieldType::parser(C::Long)
        .with_flen(5)
        .with_flags(F::UNSIGNED | F::ZEROFILL);
    assert_eq!(integer.to_string(), "int(5) UNSIGNED ZEROFILL");
    assert_eq!(integer.info_schema_str(false), "int(5) unsigned");
    for code in [C::Float, C::Double] {
        assert_eq!(
            FieldType::parser(code)
                .with_flen(if code == C::Float { 12 } else { 22 })
                .with_decimal(3)
                .to_string(),
            if code == C::Float {
                "float(12,3)"
            } else {
                "double(22,3)"
            }
        );
        for flen in [if code == C::Float { 12 } else { 22 }, 5] {
            assert_eq!(
                FieldType::parser(code)
                    .with_flen(flen)
                    .with_decimal(-1)
                    .to_string(),
                if code == C::Float { "float" } else { "double" }
            );
        }
        assert_eq!(
            FieldType::parser(code)
                .with_flen(7)
                .with_decimal(3)
                .to_string(),
            if code == C::Float {
                "float(7,3)"
            } else {
                "double(7,3)"
            }
        );
    }
    let text = FieldType::parser(C::Blob)
        .with_flen(10)
        .with_charset_name("UTF8")
        .with_collation_name("UTF8_UNICODE_GI");
    assert_eq!(
        text.to_string(),
        "text CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI"
    );
    assert!(text.has_charset());
    assert_eq!(
        FieldType::parser(C::Varchar)
            .with_flen(10)
            .with_added_flags(F::BINARY)
            .to_string(),
        "varchar(10) BINARY"
    );
    assert_eq!(
        FieldType::parser(C::String)
            .with_charset_name("binary")
            .with_added_flags(F::BINARY)
            .to_string(),
        "binary(1)"
    );
    for (code, name) in [(C::Enum, "enum"), (C::Set, "set")] {
        assert_eq!(
            FieldType::parser(code).with_elems(["a", "b"]).to_string(),
            format!("{name}('a','b')")
        );
        assert_eq!(
            FieldType::parser(code)
                .with_elems(["'a'", "'b'"])
                .to_string(),
            format!("{name}('''a''','''b''')")
        );
        assert_eq!(
            FieldType::parser(code)
                .with_elems(["a\nb", "a'\t\r\nb", "a\rb"])
                .to_string(),
            format!("{name}('a\\nb','a''\t\\r\\nb','a\\rb')")
        );
        assert_eq!(
            FieldType::parser(code)
                .with_elems(["a\nb", "a\tb", "a\rb"])
                .to_string(),
            format!("{name}('a\\nb','a\tb','a\\rb')")
        );
    }
    assert_eq!(
        FieldType::parser(C::Set)
            .with_elems(["a'\nb", "a'b\tc"])
            .to_string(),
        "set('a''\\nb','a''b\tc')"
    );
    assert_eq!(
        FieldType::parser(C::Enum)
            .with_elems(["nul\0byte", r"raw\slash"])
            .to_string(),
        r"enum('nul\0byte','raw\slash')"
    );
    assert_eq!(
        FieldType::parser(C::String)
            .with_charset_name("BINARY")
            .to_string(),
        "char(1) CHARACTER SET BINARY"
    );
    assert_eq!(
        FieldType::parser(C::Timestamp)
            .with_flen(8)
            .with_decimal(2)
            .to_string(),
        "timestamp(2)"
    );
    assert_eq!(
        FieldType::parser(C::Timestamp)
            .with_flen(8)
            .with_decimal(0)
            .to_string(),
        "timestamp"
    );
    assert_eq!(
        FieldType::parser(C::Datetime)
            .with_flen(8)
            .with_decimal(2)
            .to_string(),
        "datetime(2)"
    );
    assert_eq!(
        FieldType::parser(C::Date)
            .with_flen(8)
            .with_decimal(2)
            .to_string(),
        "date"
    );
    assert_eq!(
        FieldType::parser(C::Date)
            .with_flen(8)
            .with_decimal(0)
            .to_string(),
        "date"
    );
    assert_eq!(
        FieldType::parser(C::Year)
            .with_flen(4)
            .with_decimal(0)
            .to_string(),
        "year(4)"
    );
    assert_eq!(
        FieldType::parser(C::Year)
            .with_flen(2)
            .with_decimal(2)
            .to_string(),
        "year(2)"
    );
    assert_eq!(
        FieldType::parser(C::Varchar)
            .with_flen(0)
            .with_decimal(0)
            .to_string(),
        "varchar(0)"
    );
    assert_eq!(
        FieldType::parser(C::String)
            .with_flen(0)
            .with_decimal(0)
            .to_string(),
        "char(0)"
    );
}

#[test]
fn parser_element_binary_literal_markers_follow_go_set_elems_ownership() {
    let mut field = FieldType::parser(C::Enum).with_elems(["a", "b"]);
    field.set_elem_with_binary_literal(1, "binary", true);
    assert!(field.elem_is_binary_literal(1));

    let mut field = field.with_elems(["c", "d"]);
    assert!(field.elem_is_binary_literal(1));
    field.set_elem_with_binary_literal(1, "plain", false);
    assert!(field.elem_is_binary_literal(1));
    field.clean_elem_binary_literals();
    assert!(!field.elem_is_binary_literal(1));
}

/// Go: pkg/parser/types/field_type_test.go:197 TestHasCharsetFromStmt.
#[test]
fn parser_has_charset_rows() {
    let rows = [
        (C::Long, false),
        (C::Double, false),
        (C::Float, false),
        (C::Bit, false),
        (C::String, true),
        (C::Varchar, true),
        (C::Year, false),
        (C::Date, false),
        (C::Duration, false),
        (C::Datetime, false),
        (C::Timestamp, false),
        (C::Blob, true),
        (C::TinyBlob, true),
        (C::MediumBlob, true),
        (C::LongBlob, true),
        (C::Json, false),
        (C::Enum, true),
        (C::Set, true),
    ];
    for (code, expected) in rows {
        assert_eq!(FieldType::parser(code).has_charset(), expected, "{code:?}");
    }
    for code in [
        C::String,
        C::Varchar,
        C::Blob,
        C::TinyBlob,
        C::MediumBlob,
        C::LongBlob,
    ] {
        assert!(!FieldType::parser(code)
            .with_added_flags(F::BINARY)
            .has_charset());
    }
}

/// Go: pkg/parser/types/field_type_test.go:245 TestEnumSetFlen.
#[test]
fn parser_enum_set_flen() {
    for (elems, expected) in [
        (vec!["a"], 1),
        (vec!["a", "b"], 1),
        (vec!["a", "bb"], 2),
        (vec![""], 0),
        (vec!["a", ""], 1),
    ] {
        assert_eq!(enum_set_display_length(C::Enum, &elems), expected);
    }
    for (elems, expected) in [
        (vec!["a"], 1),
        (vec!["a", "b"], 3),
        (vec!["a", "bb"], 4),
        (vec!["a", "b", "c"], 5),
        (vec!["a", "bb", "c"], 6),
        (vec![""], 0),
        (vec!["a", ""], 2),
    ] {
        assert_eq!(enum_set_display_length(C::Set, &elems), expected);
    }
}

/// Go: pkg/parser/types/field_type_test.go:276 TestFieldTypeEqual.
#[test]
fn parser_field_type_equal() {
    let mut first = FieldType::parser(C::Double);
    let mut second = FieldType::parser(C::Float);
    assert!(!first.equal(&second));
    second = FieldType::parser(C::Double).with_decimal(5);
    assert!(!first.equal(&second));
    first = first.with_decimal(5).with_flen(22);
    assert!(!first.equal(&second));
    second = second.with_flen(22);
    assert!(first.equal(&second));
    first = first.with_decimal(-1).with_flen(23);
    second = second.with_decimal(-1);
    assert!(first.equal(&second));
}

/// Go: pkg/parser/types/field_type_test.go:303 TestCompactStr.
#[test]
fn parser_compact_str() {
    for (code, flen, flags, loose, strict) in [
        (C::Tiny, 1, 0, "tinyint(1)", "tinyint(1)"),
        (C::Tiny, 2, 0, "tinyint(2)", "tinyint"),
        (C::Long, 10, 0, "int(10)", "int"),
        (C::Long, 10, F::ZEROFILL, "int(10)", "int(10)"),
    ] {
        let field = FieldType::parser(code).with_flen(flen).with_flags(flags);
        assert_eq!(field.compact_str(false), loose);
        assert_eq!(field.compact_str(true), strict);
    }
}

/// Go: pkg/types/field_type_test.go:25 TestFieldType.
#[test]
fn runtime_field_type() {
    let empty = FieldType::new(C::Duration);
    assert_eq!(empty.flen(), UNSPECIFIED_LENGTH);
    assert_eq!(empty.decimal(), UNSPECIFIED_LENGTH);
    assert_eq!(
        FieldType::new(C::Duration).with_decimal(5).to_string(),
        "time(5)"
    );
    assert_eq!(
        FieldType::new(C::Long)
            .with_flen(5)
            .with_flags(F::UNSIGNED | F::ZEROFILL)
            .to_string(),
        "int(5) UNSIGNED ZEROFILL"
    );
    assert_eq!(
        FieldType::new(C::Long)
            .with_flen(5)
            .with_flags(F::UNSIGNED | F::ZEROFILL)
            .info_schema_str(false),
        "int(5) unsigned"
    );
    for (code, default_flen, name) in [(C::Float, 12, "float"), (C::Double, 22, "double")] {
        assert_eq!(
            FieldType::new(code)
                .with_flen(default_flen)
                .with_decimal(3)
                .to_string(),
            format!("{name}({default_flen},3)")
        );
        for flen in [default_flen, 5] {
            assert_eq!(
                FieldType::new(code)
                    .with_flen(flen)
                    .with_decimal(UNSPECIFIED_LENGTH)
                    .to_string(),
                name
            );
        }
        assert_eq!(
            FieldType::new(code)
                .with_flen(7)
                .with_decimal(3)
                .to_string(),
            format!("{name}(7,3)")
        );
    }
    assert_eq!(
        FieldType::new(C::Blob)
            .with_flen(10)
            .with_charset_name("UTF8")
            .with_collation_name("UTF8_UNICODE_GI")
            .to_string(),
        "text CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI"
    );
    assert_eq!(
        FieldType::new(C::Varchar)
            .with_flen(10)
            .with_added_flags(F::BINARY)
            .to_string(),
        "varchar(10) BINARY CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
    );
    assert_eq!(
        FieldType::new(C::String)
            .with_charset_name("binary")
            .with_added_flags(F::BINARY)
            .to_string(),
        "binary(1) COLLATE utf8mb4_bin"
    );
    for (code, name) in [(C::Enum, "enum"), (C::Set, "set")] {
        for (elems, suffix) in [
            (vec!["a", "b"], "('a','b')"),
            (vec!["'a'", "'b'"], "('''a''','''b''')"),
            (vec!["a\nb", "a\tb", "a\rb"], "('a\\nb','a\tb','a\\rb')"),
            (
                vec!["a\nb", "a'\t\r\nb", "a\rb"],
                "('a\\nb','a''\t\\r\\nb','a\\rb')",
            ),
        ] {
            assert_eq!(
                FieldType::new(code).with_elems(elems).to_string(),
                format!("{name}{suffix}")
            );
        }
    }
    assert_eq!(
        FieldType::new(C::Set)
            .with_elems(["a'\nb", "a'b\tc"])
            .to_string(),
        "set('a''\\nb','a''b\tc')"
    );
    for (code, flen, decimal, expected) in [
        (C::Timestamp, 8, 2, "timestamp(2)"),
        (C::Timestamp, 8, 0, "timestamp"),
        (C::Datetime, 8, 2, "datetime(2)"),
        (C::Datetime, 8, 0, "datetime"),
        (C::Date, 8, 2, "date"),
        (C::Date, 8, 0, "date"),
        (C::Year, 4, 0, "year(4)"),
        (C::Year, 2, 2, "year(2)"),
    ] {
        assert_eq!(
            FieldType::new(code)
                .with_flen(flen)
                .with_decimal(decimal)
                .to_string(),
            expected
        );
    }
}

/// Go: pkg/types/field_type_test.go:159 TestDefaultTypeForValue.
#[test]
fn runtime_default_type_for_value() {
    let binary = F::BINARY;
    let not_null = F::NOT_NULL;
    let rows = [
        (V::Null, C::Null, 0, 0, "binary", "binary", binary),
        (
            V::Signed(1),
            C::LongLong,
            1,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Signed(0),
            C::LongLong,
            1,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Signed(432),
            C::LongLong,
            3,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Signed(4321),
            C::LongLong,
            4,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Signed(1_234_567),
            C::LongLong,
            7,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Signed(12_345_678),
            C::LongLong,
            8,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Signed(12_345_678_901_234_567),
            C::LongLong,
            17,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Signed(-42),
            C::LongLong,
            3,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Unsigned(1234),
            C::LongLong,
            4,
            0,
            "binary",
            "binary",
            binary | F::UNSIGNED | not_null,
        ),
        (
            V::Unsigned(1),
            C::LongLong,
            1,
            0,
            "binary",
            "binary",
            binary | F::UNSIGNED | not_null,
        ),
        (
            V::Unsigned(123),
            C::LongLong,
            3,
            0,
            "binary",
            "binary",
            binary | F::UNSIGNED | not_null,
        ),
        (
            V::Unsigned(1_234_567),
            C::LongLong,
            7,
            0,
            "binary",
            "binary",
            binary | F::UNSIGNED | not_null,
        ),
        (
            V::Unsigned(12_345_678),
            C::LongLong,
            8,
            0,
            "binary",
            "binary",
            binary | F::UNSIGNED | not_null,
        ),
        (
            V::Unsigned(12_345_678_901_234_567),
            C::LongLong,
            17,
            0,
            "binary",
            "binary",
            binary | F::UNSIGNED | not_null,
        ),
        (
            V::String("abc"),
            C::VarString,
            3,
            -1,
            "utf8mb4",
            "utf8mb4_bin",
            not_null,
        ),
        (
            V::Float64(1.1),
            C::Double,
            3,
            -1,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Bytes(b"abc"),
            C::Blob,
            3,
            -1,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::HexLiteral(b""),
            C::VarString,
            0,
            0,
            "binary",
            "binary",
            binary | F::UNSIGNED | not_null,
        ),
        (
            V::BitLiteral(b""),
            C::VarString,
            0,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Datetime { fsp: 0 },
            C::Datetime,
            19,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Datetime { fsp: 3 },
            C::Datetime,
            23,
            3,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Duration {
                display_len: 8,
                fsp: 0,
            },
            C::Duration,
            8,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Decimal {
                display_len: 1,
                fraction_digits: 0,
            },
            C::NewDecimal,
            2,
            0,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Enum("a"),
            C::Enum,
            1,
            -1,
            "binary",
            "binary",
            binary | not_null,
        ),
        (
            V::Set("a"),
            C::Set,
            1,
            -1,
            "binary",
            "binary",
            binary | not_null,
        ),
    ];
    for (value, code, flen, decimal, charset, collate, flags) in rows {
        assert_field(
            &default_field_type_for_value(value, "utf8mb4", "utf8mb4_bin"),
            code,
            flen,
            decimal,
            charset,
            collate,
            flags,
        );
    }
}

#[test]
fn runtime_merge_unknown_type_uses_go_map_zero_index() {
    let unknown = C::Unknown(0x11);
    assert_eq!(merge_field_type(unknown, unknown), C::NewDecimal);
    assert_eq!(merge_field_type(unknown, C::Tiny), C::NewDecimal);
    assert_eq!(merge_field_type(C::Tiny, unknown), C::NewDecimal);
}

#[test]
fn parser_decimal_delta_updates_flen_independently() {
    let old = FieldType::parser(C::NewDecimal)
        .with_flen(10)
        .with_decimal(UNSPECIFIED_LENGTH);
    let mut result = FieldType::parser(C::NewDecimal);
    result.update_flen_and_decimal_under_limit(&old, 7, 2);
    assert_eq!((result.flen(), result.decimal()), (42, 30));

    let old = old.with_decimal(3);
    result.update_flen_and_decimal_under_limit(&old, 7, 2);
    assert_eq!((result.flen(), result.decimal()), (12, 10));
}

/// Go: pkg/types/field_type_test.go:209 TestAggFieldType.
#[test]
fn runtime_agg_field_type() {
    for field in all_types() {
        assert_eq!(
            agg_field_type(std::slice::from_ref(&field)).code(),
            field.code()
        );
        let same_expected = match field.code() {
            C::Date => C::Date,
            C::Json => C::Json,
            C::Enum | C::Set | C::VarString => C::Varchar,
            C::Unspecified => C::NewDecimal,
            other => other,
        };
        assert_eq!(
            agg_field_type(&[field.clone(), field.clone()]).code(),
            same_expected
        );
        let long_expected = match field.code() {
            C::Tiny | C::Short | C::Long | C::Year | C::Int24 | C::Null => C::Long,
            C::LongLong => C::LongLong,
            C::Float | C::Double => C::Double,
            C::Timestamp
            | C::Date
            | C::Duration
            | C::Datetime
            | C::NewDate
            | C::Varchar
            | C::Json
            | C::Enum
            | C::Set
            | C::VarString
            | C::Geometry => C::Varchar,
            C::Bit => C::LongLong,
            C::String => C::String,
            C::Unspecified | C::NewDecimal => C::NewDecimal,
            C::TinyBlob => C::TinyBlob,
            C::Blob => C::Blob,
            C::MediumBlob => C::MediumBlob,
            C::LongBlob => C::LongBlob,
            other => other,
        };
        assert_eq!(
            agg_field_type(&[field.clone(), FieldType::new(C::Long)]).code(),
            long_expected
        );
        let json_expected = match field.code() {
            C::Json | C::Null => C::Json,
            C::LongBlob | C::MediumBlob | C::TinyBlob | C::Blob => C::LongBlob,
            C::String => C::String,
            _ => C::Varchar,
        };
        assert_eq!(
            agg_field_type(&[field, FieldType::new(C::Json)]).code(),
            json_expected
        );
    }
}

/// Go: pkg/types/field_type_test.go:303 TestAggFieldTypeForTypeFlag.
#[test]
fn runtime_agg_field_type_flags() {
    let plain = FieldType::new(C::LongLong);
    assert_eq!(agg_field_type(&[plain.clone(), plain.clone()]).flags(), 0);
    assert_eq!(
        agg_field_type(&[plain.clone().with_flags(F::NOT_NULL), plain.clone()]).flags(),
        0
    );
    assert_eq!(
        agg_field_type(&[plain.clone(), plain.clone().with_flags(F::NOT_NULL)]).flags(),
        0
    );
    assert_eq!(
        agg_field_type(&[
            plain.clone().with_flags(F::NOT_NULL),
            plain.with_flags(F::NOT_NULL)
        ])
        .flags(),
        F::NOT_NULL
    );
}

/// Go: pkg/types/field_type_test.go:330 TestAggFieldTypeForIntegralPromotion.
#[test]
fn runtime_agg_field_type_integral_promotion() {
    let codes = [
        C::Tiny,
        C::Short,
        C::Int24,
        C::Long,
        C::LongLong,
        C::NewDecimal,
    ];
    for index in 1..codes.len() - 1 {
        let left = FieldType::new(codes[index - 1]);
        let right = FieldType::new(codes[index]);
        assert_eq!(
            agg_field_type(&[left.clone(), right.clone()]).code(),
            codes[index]
        );
        assert_eq!(
            agg_field_type(&[left.clone().with_flags(F::UNSIGNED), right.clone()]).code(),
            codes[index]
        );
        let both = agg_field_type(&[
            left.clone().with_flags(F::UNSIGNED),
            right.clone().with_flags(F::UNSIGNED),
        ]);
        assert_eq!((both.code(), both.flags()), (codes[index], F::UNSIGNED));
        let promoted = agg_field_type(&[left, right.with_flags(F::UNSIGNED)]);
        assert_eq!((promoted.code(), promoted.flags()), (codes[index + 1], 0));
    }
}

/// Go: pkg/types/field_type_test.go:368 TestAggregateEvalType.
#[test]
fn runtime_aggregate_eval_type() {
    for field in all_types() {
        for arguments in [
            vec![field.clone()],
            vec![field.clone(), field.clone()],
            vec![field.clone(), FieldType::new(C::Long)],
        ] {
            let mut flags = 0;
            let result = aggregate_eval_type(&arguments, &mut flags);
            let paired_with_long = arguments.len() == 2 && arguments[1].code() == C::Long;
            let string_kind = if paired_with_long {
                matches!(
                    field.code(),
                    C::Timestamp
                        | C::Date
                        | C::Duration
                        | C::Datetime
                        | C::NewDate
                        | C::Varchar
                        | C::Json
                        | C::Enum
                        | C::Set
                        | C::TinyBlob
                        | C::MediumBlob
                        | C::LongBlob
                        | C::Blob
                        | C::VarString
                        | C::String
                        | C::Geometry
                )
            } else {
                matches!(
                    field.code(),
                    C::Unspecified
                        | C::Null
                        | C::Timestamp
                        | C::Date
                        | C::Duration
                        | C::Datetime
                        | C::NewDate
                        | C::Varchar
                        | C::Json
                        | C::Enum
                        | C::Set
                        | C::TinyBlob
                        | C::MediumBlob
                        | C::LongBlob
                        | C::Blob
                        | C::VarString
                        | C::String
                        | C::Geometry
                )
            };
            if string_kind {
                // The Go test intentionally accepts every string-kind EvalType
                // here; singleton temporal/JSON inputs retain their specific
                // EvalType rather than being normalized to ETString.
                assert!(result.is_string_kind(), "{:?}: {result:?}", field.code());
                assert_eq!(flags, 0);
            } else {
                let expected = match field.code() {
                    C::Float | C::Double => EvalType::Real,
                    C::NewDecimal => EvalType::Decimal,
                    _ => EvalType::Int,
                };
                assert_eq!(result, expected, "{:?}", field.code());
                assert_eq!(flags, F::BINARY);
            }
        }
    }
}
