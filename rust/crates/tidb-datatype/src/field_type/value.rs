// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

use super::{FieldType, FieldTypeCode, FieldTypeFlags, UNSPECIFIED_LENGTH};
use crate::{Datum, TimeType};

/// Go `types.InferParamTypeFromDatum`: execute-time parameter metadata, not
/// literal display widths. The datum remains byte-preserving and unchanged.
pub fn infer_param_type_from_datum(datum: &Datum) -> FieldType {
    // String/ENUM/SET payloads can contain non-UTF-8 bytes. Their parameter
    // width is unspecified, so inference needs no decoding or replacement.
    let mut field_type = match datum {
        Datum::Null | Datum::MinNotNull | Datum::MaxValue | Datum::Raw(_) => {
            // Internal range sentinels and raw encoded storage cells are
            // not SQL EXECUTE values and carry no typed parameter payload
            // in this representation.
            return FieldType::parser(FieldTypeCode::Null)
                .with_flen(0)
                .with_decimal(0)
                .with_charset_name("utf8mb4")
                .with_collation_name("utf8mb4_bin");
        }
        Datum::String(value) => FieldType::parser(FieldTypeCode::VarString)
            .with_added_flags(FieldTypeFlags::NOT_NULL)
            .with_charset_name(value.charset().name())
            .with_collation_name(value.collation().name()),
        Datum::Enum(..) | Datum::Set(..) => {
            FieldType::parser(if matches!(datum, Datum::Enum(..)) {
                FieldTypeCode::Enum
            } else {
                FieldTypeCode::Set
            })
            .with_added_flags(FieldTypeFlags::NOT_NULL | FieldTypeFlags::BINARY)
            .with_charset_name("binary")
            .with_collation_name("binary")
        }
        _ => {
            let value = match datum {
                Datum::Int(value) => FieldTypeValue::Signed(*value),
                Datum::UInt(value) => FieldTypeValue::Unsigned(*value),
                Datum::Real(value) => FieldTypeValue::Float64(*value),
                Datum::Float32(value) => FieldTypeValue::Float32(*value as f32),
                Datum::Bytes(value) => FieldTypeValue::Bytes(value),
                Datum::BinaryLiteral(value) | Datum::Bit(value) => {
                    FieldTypeValue::BinaryLiteral(value.as_bytes())
                }
                Datum::Decimal(value) => FieldTypeValue::Decimal {
                    display_len: value.to_string().len() as i64,
                    fraction_digits: i64::from(value.precision_and_frac().1),
                },
                Datum::Duration(value) => FieldTypeValue::Duration {
                    display_len: value.to_string().len() as i64,
                    fsp: value.fsp(),
                },
                Datum::Time(value) => match value.kind() {
                    TimeType::Date => FieldTypeValue::Date,
                    TimeType::DateTime => FieldTypeValue::Datetime {
                        fsp: i64::from(value.fsp()),
                    },
                    TimeType::Timestamp => FieldTypeValue::Timestamp {
                        fsp: i64::from(value.fsp()),
                    },
                },
                Datum::Json(_) => FieldTypeValue::Json,
                Datum::VectorFloat32(_) => FieldTypeValue::VectorFloat32,
                Datum::Null
                | Datum::MinNotNull
                | Datum::MaxValue
                | Datum::Raw(_)
                | Datum::String(_)
                | Datum::Enum(..)
                | Datum::Set(..) => {
                    unreachable!("handled non-scalar datum above")
                }
            };
            default_field_type_for_value(value, "utf8mb4", "utf8mb4_bin")
        }
    };
    if matches!(
        field_type.code(),
        FieldTypeCode::LongLong
            | FieldTypeCode::VarString
            | FieldTypeCode::Double
            | FieldTypeCode::Blob
            | FieldTypeCode::Bit
            | FieldTypeCode::Duration
            | FieldTypeCode::Enum
            | FieldTypeCode::Set
    ) {
        field_type.set_flen(UNSPECIFIED_LENGTH);
    }
    field_type
}

/// Source value shapes accepted by `DefaultTypeForValue`.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldTypeValue<'a> {
    /// SQL `NULL`.
    Null,
    /// A Boolean value.
    Bool(bool),
    /// A signed integer value.
    Signed(i64),
    /// An unsigned integer value.
    Unsigned(u64),
    /// A character string value.
    String(&'a str),
    /// A single-precision floating-point value.
    Float32(f32),
    /// A double-precision floating-point value.
    Float64(f64),
    /// An arbitrary byte string.
    Bytes(&'a [u8]),
    /// A bit-string literal's decoded bytes.
    BitLiteral(&'a [u8]),
    /// A hexadecimal literal's decoded bytes.
    HexLiteral(&'a [u8]),
    /// A binary literal's decoded bytes.
    BinaryLiteral(&'a [u8]),
    /// A calendar date.
    Date,
    /// A date and time with fractional-second precision.
    Datetime {
        /// Fractional-second precision.
        fsp: i64,
    },
    /// A timestamp with fractional-second precision.
    Timestamp {
        /// Fractional-second precision.
        fsp: i64,
    },
    /// A time duration and its display metadata.
    Duration {
        /// Display width before fractional-second adjustment.
        display_len: i64,
        /// Fractional-second precision.
        fsp: i64,
    },
    /// An exact decimal and its display metadata.
    Decimal {
        /// Display width of the decimal value.
        display_len: i64,
        /// Number of digits after the decimal point.
        fraction_digits: i64,
    },
    /// An enum member name.
    Enum(&'a str),
    /// A set member name.
    Set(&'a str),
    /// A JSON value.
    Json,
    /// A vector of single-precision floating-point values.
    VectorFloat32,
    /// A value shape with no supported default field type.
    Unsupported,
}

/// Mechanically mirrors `pkg/types.DefaultTypeForValue` metadata decisions.
pub fn default_field_type_for_value(
    value: FieldTypeValue<'_>,
    charset: &str,
    collation: &str,
) -> FieldType {
    let not_null = !matches!(&value, FieldTypeValue::Null);
    let mut field_type = FieldType::parser(FieldTypeCode::Unspecified);
    if not_null {
        field_type = field_type.with_added_flags(FieldTypeFlags::NOT_NULL);
    }
    let binary = |field_type: FieldType| {
        field_type
            .with_charset_name("binary")
            .with_collation_name("binary")
            .with_added_flags(FieldTypeFlags::BINARY)
    };
    match value {
        FieldTypeValue::Null => binary(
            field_type
                .with_code(FieldTypeCode::Null)
                .with_flen(0)
                .with_decimal(0),
        ),
        FieldTypeValue::Bool(_) => binary(
            field_type
                .with_code(FieldTypeCode::LongLong)
                .with_flen(1)
                .with_decimal(0)
                .with_added_flags(FieldTypeFlags::IS_BOOLEAN),
        ),
        FieldTypeValue::Signed(value) => binary(
            field_type
                .with_code(FieldTypeCode::LongLong)
                .with_flen(signed_display_len(value))
                .with_decimal(0),
        ),
        FieldTypeValue::Unsigned(value) => binary(
            field_type
                .with_code(FieldTypeCode::LongLong)
                .with_flen(unsigned_display_len(value))
                .with_decimal(0)
                .with_added_flags(FieldTypeFlags::UNSIGNED),
        ),
        FieldTypeValue::String(value) => field_type
            .with_code(FieldTypeCode::VarString)
            .with_flen(value.len() as i64)
            .with_decimal(UNSPECIFIED_LENGTH)
            .with_charset_name(charset)
            .with_collation_name(collation),
        FieldTypeValue::Float32(value) => binary(
            field_type
                .with_code(FieldTypeCode::Float)
                .with_flen(go_fixed_shortest_f32(value).len() as i64)
                .with_decimal(UNSPECIFIED_LENGTH),
        ),
        FieldTypeValue::Float64(value) => binary(
            field_type
                .with_code(FieldTypeCode::Double)
                .with_flen(go_fixed_shortest_f64(value).len() as i64)
                .with_decimal(UNSPECIFIED_LENGTH),
        ),
        FieldTypeValue::Bytes(value) => binary(
            field_type
                .with_code(FieldTypeCode::Blob)
                .with_flen(value.len() as i64)
                .with_decimal(UNSPECIFIED_LENGTH),
        ),
        FieldTypeValue::BitLiteral(value) => binary(
            field_type
                .with_code(FieldTypeCode::VarString)
                .with_flen((value.len() * 3) as i64)
                .with_decimal(0),
        ),
        FieldTypeValue::HexLiteral(value) => binary(
            field_type
                .with_code(FieldTypeCode::VarString)
                .with_flen((value.len() * 3) as i64)
                .with_decimal(0)
                .with_added_flags(FieldTypeFlags::UNSIGNED),
        ),
        FieldTypeValue::BinaryLiteral(value) => binary(
            field_type
                .with_code(FieldTypeCode::VarString)
                .with_flen(value.len() as i64)
                .with_decimal(0)
                .with_added_flags(FieldTypeFlags::UNSIGNED),
        )
        .with_removed_flags(FieldTypeFlags::BINARY),
        FieldTypeValue::Date => binary(
            field_type
                .with_code(FieldTypeCode::Date)
                .with_flen(10)
                .with_decimal(UNSPECIFIED_LENGTH),
        ),
        FieldTypeValue::Datetime { fsp } => binary(
            field_type
                .with_code(FieldTypeCode::Datetime)
                .with_flen(19 + if fsp > 0 { fsp + 1 } else { 0 })
                .with_decimal(fsp),
        ),
        FieldTypeValue::Timestamp { fsp } => binary(
            field_type
                .with_code(FieldTypeCode::Timestamp)
                .with_flen(19 + if fsp > 0 { fsp + 1 } else { 0 })
                .with_decimal(fsp),
        ),
        FieldTypeValue::Duration { display_len, fsp } => binary(
            field_type
                .with_code(FieldTypeCode::Duration)
                .with_flen(if fsp > 0 { fsp + 1 } else { display_len })
                .with_decimal(fsp),
        ),
        FieldTypeValue::Decimal {
            display_len,
            fraction_digits,
        } => binary(
            field_type
                .with_code(FieldTypeCode::NewDecimal)
                .with_flen((display_len + 1).min(super::MAX_DECIMAL_WIDTH))
                .with_decimal(fraction_digits.min(super::MAX_DECIMAL_SCALE)),
        ),
        FieldTypeValue::Enum(name) => binary(
            field_type
                .with_code(FieldTypeCode::Enum)
                .with_flen(name.len() as i64)
                .with_decimal(UNSPECIFIED_LENGTH),
        ),
        FieldTypeValue::Set(name) => binary(
            field_type
                .with_code(FieldTypeCode::Set)
                .with_flen(name.len() as i64)
                .with_decimal(UNSPECIFIED_LENGTH),
        ),
        FieldTypeValue::Json => field_type
            .with_code(FieldTypeCode::Json)
            .with_flen(UNSPECIFIED_LENGTH)
            .with_decimal(0)
            .with_charset_name("utf8mb4")
            .with_collation_name("utf8mb4_bin"),
        FieldTypeValue::VectorFloat32 => binary(
            field_type
                .with_code(FieldTypeCode::VectorFloat32)
                .with_flen(UNSPECIFIED_LENGTH)
                .with_decimal(0),
        ),
        FieldTypeValue::Unsupported => field_type
            .with_code(FieldTypeCode::Unspecified)
            .with_flen(UNSPECIFIED_LENGTH)
            .with_decimal(UNSPECIFIED_LENGTH)
            .with_charset_name("utf8mb4")
            .with_collation_name("utf8mb4_bin"),
    }
}

/// Mirrors `pkg/parser/test_driver.DefaultTypeForValue`.
///
/// This is intentionally separate from [`default_field_type_for_value`]: the
/// parser's lightweight driver and TiDB's runtime `types` package assign
/// different flags and widths to the same literal shapes.
pub fn parser_default_field_type_for_value(
    value: FieldTypeValue<'_>,
    charset: &str,
    collation: &str,
) -> FieldType {
    let field_type = FieldType::parser(FieldTypeCode::Unspecified);
    let binary = |field_type: FieldType| {
        field_type
            .with_charset_name("binary")
            .with_collation_name("binary")
            .with_added_flags(FieldTypeFlags::BINARY)
    };
    match value {
        FieldTypeValue::Null => binary(
            field_type
                .with_code(FieldTypeCode::Null)
                .with_flen(0)
                .with_decimal(0),
        ),
        FieldTypeValue::Bool(_) => binary(
            field_type
                .with_code(FieldTypeCode::LongLong)
                .with_flen(1)
                .with_decimal(0)
                .with_added_flags(FieldTypeFlags::IS_BOOLEAN),
        ),
        FieldTypeValue::Signed(value) => binary(
            field_type
                .with_code(FieldTypeCode::LongLong)
                .with_flen(signed_display_len(value))
                .with_decimal(0),
        ),
        FieldTypeValue::Unsigned(value) => binary(
            field_type
                .with_code(FieldTypeCode::LongLong)
                .with_flen(unsigned_display_len(value))
                .with_decimal(0)
                .with_added_flags(FieldTypeFlags::UNSIGNED),
        ),
        FieldTypeValue::String(value) => field_type
            .with_code(FieldTypeCode::VarString)
            .with_flen(value.len() as i64)
            .with_decimal(UNSPECIFIED_LENGTH)
            .with_charset_name(charset)
            .with_collation_name(collation),
        FieldTypeValue::Float32(value) => binary(
            field_type
                .with_code(FieldTypeCode::Float)
                .with_flen(go_fixed_shortest_f32(value).len() as i64)
                .with_decimal(UNSPECIFIED_LENGTH),
        ),
        FieldTypeValue::Float64(value) => binary(
            field_type
                .with_code(FieldTypeCode::Double)
                .with_flen(go_fixed_shortest_f64(value).len() as i64)
                .with_decimal(UNSPECIFIED_LENGTH),
        ),
        FieldTypeValue::Bytes(value) => binary(
            field_type
                .with_code(FieldTypeCode::Blob)
                .with_flen(value.len() as i64)
                .with_decimal(UNSPECIFIED_LENGTH),
        ),
        FieldTypeValue::BitLiteral(value) => binary(
            field_type
                .with_code(FieldTypeCode::VarString)
                .with_flen(value.len() as i64)
                .with_decimal(0),
        ),
        FieldTypeValue::HexLiteral(value) => binary(
            field_type
                .with_code(FieldTypeCode::VarString)
                .with_flen((value.len() * 3) as i64)
                .with_decimal(0)
                .with_added_flags(FieldTypeFlags::UNSIGNED),
        ),
        FieldTypeValue::BinaryLiteral(value) => binary(
            field_type
                .with_code(FieldTypeCode::Bit)
                .with_flen((value.len() * 8) as i64)
                .with_decimal(0)
                .with_added_flags(FieldTypeFlags::UNSIGNED),
        )
        .with_removed_flags(FieldTypeFlags::BINARY),
        FieldTypeValue::Decimal {
            display_len,
            fraction_digits,
        } => binary(
            field_type
                .with_code(FieldTypeCode::NewDecimal)
                .with_flen(display_len)
                .with_decimal(fraction_digits),
        ),
        _ => field_type
            .with_code(FieldTypeCode::Unspecified)
            .with_flen(UNSPECIFIED_LENGTH)
            .with_decimal(UNSPECIFIED_LENGTH),
    }
}

fn go_fixed_shortest_f32(value: f32) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f32::INFINITY {
        "+Inf".to_owned()
    } else if value == f32::NEG_INFINITY {
        "-Inf".to_owned()
    } else {
        value.to_string()
    }
}

fn go_fixed_shortest_f64(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "+Inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_owned()
    } else {
        value.to_string()
    }
}

const fn signed_display_len(value: i64) -> i64 {
    if value == 0 {
        return 1;
    }
    let negative = value < 0;
    let mut magnitude = value.unsigned_abs();
    let mut digits = if negative { 1 } else { 0 };
    while magnitude != 0 {
        digits += 1;
        magnitude /= 10;
    }
    digits
}

const fn unsigned_display_len(mut value: u64) -> i64 {
    if value == 0 {
        return 1;
    }
    let mut digits = 0;
    while value != 0 {
        digits += 1;
        value /= 10;
    }
    digits
}
