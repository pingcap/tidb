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

//! Source-shaped scalar formatting for the MySQL text protocol.
//!
//! This module ports the dependency-closed numeric, decimal, and raw-byte
//! branches of `pkg/format/textrow/textrow.go`. Charset conversion and TiDB
//! `Datum` formatting stay outside this protocol leaf: callers pass
//! already-owned bytes (including `Decimal::Display` output), and unsupported
//! branches remain explicit until their owning datatype/session crates are
//! connected.

use std::fmt;

use crate::column::{
    TYPE_BIT, TYPE_BLOB, TYPE_ENUM, TYPE_JSON, TYPE_LONG_BLOB, TYPE_MEDIUM_BLOB, TYPE_SET,
    TYPE_STRING, TYPE_TINY_BLOB, TYPE_VARCHAR, TYPE_VAR_STRING,
};

// MySQL field-type codes consumed by the source `FormatValueText` switch.
/// MySQL's `TINYINT` type code.
pub const TYPE_TINY: u8 = 1;
/// MySQL's `SMALLINT` type code.
pub const TYPE_SHORT: u8 = 2;
/// MySQL's `INT` type code.
pub const TYPE_LONG: u8 = 3;
/// MySQL's `MEDIUMINT` type code.
pub const TYPE_INT24: u8 = 9;
/// MySQL's `FLOAT` type code.
pub const TYPE_FLOAT: u8 = 4;
/// MySQL's `DOUBLE` type code.
pub const TYPE_DOUBLE: u8 = 5;
/// MySQL's `BIGINT` type code.
pub const TYPE_LONGLONG: u8 = 8;
/// MySQL's `YEAR` type code.
pub const TYPE_YEAR: u8 = 13;

/// MySQL's `GEOMETRY` type code, retained for explicit unsupported errors.
pub const TYPE_GEOMETRY: u8 = 0xff;

/// MySQL's `UNSIGNED` flag in a column's field flags.
pub const UNSIGNED_FLAG: u16 = 1 << 5;

/// MySQL's `NOT_FIXED_DEC` marker used by TiDB for an unspecified decimal.
pub const NOT_FIXED_DECIMAL: u8 = 31;

/// MySQL's `NEWDECIMAL` type code.
pub const TYPE_NEW_DECIMAL: u8 = 0xf6;

/// MySQL's `DATE` type code.
pub const TYPE_DATE: u8 = 10;

/// MySQL's `TIMESTAMP` type code.
pub const TYPE_TIMESTAMP: u8 = 7;

/// MySQL's `DATETIME` type code.
pub const TYPE_DATETIME: u8 = 12;

/// MySQL's `TIME` type code.
pub const TYPE_DURATION: u8 = 11;

/// A minimum source-shaped subset of values accepted by `FormatValueText`.
///
/// `Bytes` deliberately carries bytes rather than a Rust string. Go TiDB
/// permits arbitrary bytes in a string datum, and charset conversion belongs
/// to the session/result-encoder owner rather than this wire-format leaf.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TextScalar<'a> {
    /// SQL NULL. The row encoder represents it with the protocol NULL marker.
    Null,
    /// A signed integer datum.
    Signed(i64),
    /// An unsigned integer datum.
    Unsigned(u64),
    /// A floating-point datum and the source width used for formatting.
    Float {
        /// Floating-point value represented as an `f64` transport value.
        value: f64,
        /// Either 32 or 64, matching Go `strconv.AppendFloat`'s bitSize.
        bit_size: u8,
    },
    /// The already-rendered decimal text produced by `Decimal::Display`.
    ///
    /// Keeping this as a byte slice leaves `tidb-protocol` independent from
    /// the datatype crate while preserving the source `MyDecimal.String()`
    /// contract. The datatype/session owner can pass
    /// `decimal.to_string().as_bytes()` without an intermediate UTF-8
    /// validation or numeric conversion.
    Decimal(&'a [u8]),
    /// Already-encoded bytes for string/blob-like values.
    Bytes(&'a [u8]),
}

/// The column attributes needed by the dependency-closed subset of
/// `FormatValueText`.
///
/// `table_is_empty` is the source-level `ColumnInfo.Table == ""` test. It is
/// kept as a boolean so this leaf does not invent table-name or identifier
/// semantics while still preserving the float precision rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TextColumn {
    /// MySQL field type code.
    pub type_code: u8,
    /// MySQL field flags, including [`UNSIGNED_FLAG`].
    pub flag: u16,
    /// Column decimal precision (`NOT_FIXED_DECIMAL` means unspecified).
    pub decimal: u8,
    /// Whether the source `ColumnInfo.Table` is empty.
    pub table_is_empty: bool,
}

impl TextColumn {
    /// Creates a source-shaped column with unspecified precision and no flags.
    pub const fn new(type_code: u8) -> Self {
        Self {
            type_code,
            flag: 0,
            decimal: NOT_FIXED_DECIMAL,
            table_is_empty: true,
        }
    }
}

/// Errors returned when a scalar cannot be represented by this bounded leaf.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextFormatError {
    /// The Go formatter's type branch has not yet been ported to this owner.
    UnsupportedType(u8),
    /// The supplied scalar does not match the source type branch.
    ScalarTypeMismatch(u8),
}

impl fmt::Display for TextFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedType(type_code) => {
                write!(
                    formatter,
                    "invalid column type for text serialization: {type_code}"
                )
            }
            Self::ScalarTypeMismatch(type_code) => {
                write!(
                    formatter,
                    "scalar does not match text column type: {type_code}"
                )
            }
        }
    }
}

impl std::error::Error for TextFormatError {}

/// Appends Go TiDB's `AppendFormatFloat` representation to `buffer`.
///
/// The exponent thresholds, float32 five-digit exponent precision, shortest
/// precision handling, and trailing mantissa-zero removal mirror the Go
/// implementation. Rust's standard float formatter supplies the same
/// shortest-round-trip digits; this function only normalizes the exponent
/// spelling and MySQL-specific mantissa shape.
pub fn append_format_float(buffer: &mut Vec<u8>, value: f64, precision: i32, bit_size: u8) {
    if value.is_nan() || value.abs() > f64::MAX {
        buffer.push(b'0');
        return;
    }

    let text = if bit_size == 32 {
        let value = value as f32;
        let abs_value = value.abs();
        let exponential = abs_value >= 1e15 || (abs_value != 0.0 && abs_value < 1e-15);
        if exponential {
            // Go forces five digits after the decimal for float32 exponent
            // output, then removes insignificant trailing zeroes below.
            format!("{value:.5e}")
        } else if precision < 0 {
            value.to_string()
        } else {
            format!("{value:.precision$}", precision = precision as usize)
        }
    } else {
        let abs_value = value.abs();
        let exponential = abs_value >= 1e15 || (abs_value != 0.0 && abs_value < 1e-15);
        if exponential {
            if precision < 0 {
                format!("{value:e}")
            } else {
                format!("{value:.precision$e}", precision = precision as usize)
            }
        } else if precision < 0 {
            value.to_string()
        } else {
            format!("{value:.precision$}", precision = precision as usize)
        }
    };

    append_normalized_float(buffer, &text);
}

/// Formats one source-shaped scalar without claiming charset or Datum
/// semantics. The result is suitable for [`crate::encode_text_row`].
pub fn format_text_value(
    column: TextColumn,
    value: TextScalar<'_>,
) -> Result<Option<Vec<u8>>, TextFormatError> {
    if matches!(value, TextScalar::Null) {
        return Ok(None);
    }

    let mut formatted = Vec::new();
    match column.type_code {
        TYPE_TINY | TYPE_SHORT | TYPE_INT24 | TYPE_LONG => match value {
            TextScalar::Signed(value) => formatted.extend_from_slice(value.to_string().as_bytes()),
            _ => return Err(TextFormatError::ScalarTypeMismatch(column.type_code)),
        },
        TYPE_LONGLONG => {
            if column.flag & UNSIGNED_FLAG != 0 {
                match value {
                    TextScalar::Unsigned(value) => {
                        formatted.extend_from_slice(value.to_string().as_bytes())
                    }
                    _ => return Err(TextFormatError::ScalarTypeMismatch(column.type_code)),
                }
            } else {
                match value {
                    TextScalar::Signed(value) => {
                        formatted.extend_from_slice(value.to_string().as_bytes())
                    }
                    _ => return Err(TextFormatError::ScalarTypeMismatch(column.type_code)),
                }
            }
        }
        TYPE_NEW_DECIMAL => match value {
            TextScalar::Decimal(value) => formatted.extend_from_slice(value),
            _ => return Err(TextFormatError::ScalarTypeMismatch(column.type_code)),
        },
        TYPE_YEAR => match value {
            TextScalar::Signed(0) => formatted.extend_from_slice(b"0000"),
            TextScalar::Signed(value) => formatted.extend_from_slice(value.to_string().as_bytes()),
            _ => return Err(TextFormatError::ScalarTypeMismatch(column.type_code)),
        },
        TYPE_FLOAT | TYPE_DOUBLE => match value {
            TextScalar::Float { value, bit_size }
                if bit_size
                    == if column.type_code == TYPE_FLOAT {
                        32
                    } else {
                        64
                    } =>
            {
                append_format_float(&mut formatted, value, float_precision(column), bit_size);
            }
            _ => return Err(TextFormatError::ScalarTypeMismatch(column.type_code)),
        },
        TYPE_STRING | TYPE_VAR_STRING | TYPE_VARCHAR | TYPE_BIT | TYPE_TINY_BLOB
        | TYPE_MEDIUM_BLOB | TYPE_LONG_BLOB | TYPE_BLOB => match value {
            TextScalar::Bytes(value) => formatted.extend_from_slice(value),
            _ => return Err(TextFormatError::ScalarTypeMismatch(column.type_code)),
        },
        // These source branches require typed Datum conversion and charset
        // state. Keeping them explicit prevents a JSON/enum/set payload from
        // being mistaken for already-encoded bytes.
        TYPE_ENUM | TYPE_SET | TYPE_JSON => {
            return Err(TextFormatError::UnsupportedType(column.type_code));
        }
        type_code => return Err(TextFormatError::UnsupportedType(type_code)),
    }
    Ok(Some(formatted))
}

fn float_precision(column: TextColumn) -> i32 {
    if column.decimal > 0 && column.decimal != NOT_FIXED_DECIMAL && column.table_is_empty {
        i32::from(column.decimal)
    } else {
        -1
    }
}

fn append_normalized_float(buffer: &mut Vec<u8>, text: &str) {
    let Some(exponent) = text.find('e') else {
        buffer.extend_from_slice(text.as_bytes());
        return;
    };

    let mantissa = &text[..exponent];
    if let Some(dot) = mantissa.find('.') {
        let mut end = mantissa.len();
        while end > dot + 1 && mantissa.as_bytes()[end - 1] == b'0' {
            end -= 1;
        }
        if end == dot + 1 {
            end -= 1;
        }
        buffer.extend_from_slice(&mantissa.as_bytes()[..end]);
    } else {
        buffer.extend_from_slice(mantissa.as_bytes());
    }

    // Rust currently omits the exponent plus sign and leading zeroes, but
    // normalizing here keeps the contract explicit if the formatter changes.
    let mut exponent_text = &text.as_bytes()[exponent + 1..];
    if exponent_text.first() == Some(&b'+') {
        exponent_text = &exponent_text[1..];
    }
    buffer.push(b'e');
    buffer.extend_from_slice(exponent_text);
}
