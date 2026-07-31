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

//! The ONE rule set turning a declared SQL column type into a [`FieldType`]:
//! the type code, the flen/decimal the declaration's arguments imply, and the
//! charset/collation stamp.
//!
//! Mirrors Go `pkg/ddl/add_column.go`'s `setCharsetCollationFlenDecimal`
//! together with the flen/decimal assignment Go's own parser
//! (`pkg/parser/parser.y`'s `FieldOpts`/`FieldLen`) performs before it, and
//! `adjustBlobTypesFlen`.
//!
//! # Why this is shared rather than per-tier
//!
//! There are two `CREATE TABLE` metadata builders in this workspace --
//! `tidb_executor::ddl`'s runnable-path one and `tidb_exec::table_info_build`'s
//! `TableInfo` one -- and four of the accept-then-discard bugs this campaign
//! found were the two disagreeing about the same statement. Owning the
//! declared-type rules ONCE is what keeps the next `CREATE`-time feature from
//! having to be implemented twice, or from diverging again.
//!
//! This is a MOVE of `table_info_build`'s implementation, which is the one
//! checked field-for-field against the `TableInfo`s a real TiDB v8.5 builds
//! for its own `mysql.*` bootstrap DDL. The resolved charset/collation pair is
//! the caller's input: each tier expresses that precedence over its own
//! registry, and this decides only whether the type carries a charset at all.

use tidb_ast::{ColumnType, ColumnTypeArg};
use tidb_datatype::{
    enum_set_display_length_from_lengths, get_charset_info, FieldType, FieldTypeCode,
    FieldTypeFlags, UNSPECIFIED_LENGTH,
};

/// Go `charset.CharsetBin`.
pub const BINARY_CHARSET: &str = "binary";

/// Why a declared column type cannot be built as written.
///
/// The message names the column and the clause, because both tiers report it
/// through an error of their own that would otherwise lose the detail.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnTypeError {
    /// Exact, self-contained explanation naming the offending declaration.
    pub reason: String,
}

impl ColumnTypeError {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for ColumnTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

/// The `FieldTypeCode` one declared type name denotes.
///
/// The names are the canonical spellings `tidb_parser`'s `parse_field_type`
/// stores, which have already folded the SQL aliases (`INTEGER` -> `INT`,
/// `BOOL` -> `TINYINT`, `NUMERIC`/`DEC`/`FIXED` -> `DECIMAL`).
pub fn column_type_code(declared: &ColumnType) -> Result<FieldTypeCode, ColumnTypeError> {
    let code = match declared.name.as_str() {
        "BOOL" | "BOOLEAN" | "TINYINT" => FieldTypeCode::Tiny,
        "SMALLINT" => FieldTypeCode::Short,
        "MEDIUMINT" => FieldTypeCode::Int24,
        "INT" | "INTEGER" => FieldTypeCode::Long,
        "BIGINT" => FieldTypeCode::LongLong,
        "FLOAT" => FieldTypeCode::Float,
        "DOUBLE" | "REAL" => FieldTypeCode::Double,
        "DECIMAL" | "NUMERIC" => FieldTypeCode::NewDecimal,
        "BIT" => FieldTypeCode::Bit,
        "DATE" => FieldTypeCode::Date,
        "DATETIME" => FieldTypeCode::Datetime,
        "TIMESTAMP" => FieldTypeCode::Timestamp,
        "TIME" => FieldTypeCode::Duration,
        "YEAR" => FieldTypeCode::Year,
        "CHAR" | "BINARY" => FieldTypeCode::String,
        "VARCHAR" | "VARBINARY" => FieldTypeCode::Varchar,
        "TINYTEXT" | "TINYBLOB" => FieldTypeCode::TinyBlob,
        "TEXT" | "BLOB" => FieldTypeCode::Blob,
        "MEDIUMTEXT" | "MEDIUMBLOB" => FieldTypeCode::MediumBlob,
        "LONGTEXT" | "LONGBLOB" => FieldTypeCode::LongBlob,
        "JSON" => FieldTypeCode::Json,
        "ENUM" => FieldTypeCode::Enum,
        "SET" => FieldTypeCode::Set,
        other => {
            return Err(ColumnTypeError::new(format!(
                "type {other} is not one this node can store"
            )))
        }
    };
    Ok(code)
}

/// Whether the declared type NAME is intrinsically binary (`BINARY`,
/// `VARBINARY`, and the BLOB family) rather than character.
#[must_use]
pub fn is_intrinsically_binary(name: &str) -> bool {
    matches!(
        name,
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB"
    )
}

/// Go's parser flen/decimal assignment plus `setCharsetCollationFlenDecimal`.
///
/// `charset`/`collate` are the pair the caller already resolved for this
/// column (Go `getCharsetAndCollateInColumnDef` then
/// `OverwriteCollationWithBinaryFlag`); a type that carries no charset
/// overrides them with `binary`/`binary`, as Go does.
pub fn build_field_type(
    name: &str,
    declared: &ColumnType,
    charset: &str,
    collate: &str,
) -> Result<FieldType, ColumnTypeError> {
    let code = column_type_code(declared)
        .map_err(|error| ColumnTypeError::new(format!("column `{name}` has {error}")))?;
    let mut field_type = FieldType::new(code);
    if declared.unsigned {
        field_type.add_flags(FieldTypeFlags::UNSIGNED);
    }
    if declared.zerofill {
        field_type.add_flags(FieldTypeFlags::ZEROFILL);
    }

    // The parser's own job: read the declared arguments.
    let (mut flen, mut decimal) = (UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH);
    match code {
        FieldTypeCode::Enum | FieldTypeCode::Set => {
            if declared.args.is_empty() {
                return Err(ColumnTypeError::new(format!(
                    "column `{name}` declares {} with no members",
                    declared.name
                )));
            }
            let mut elems = Vec::with_capacity(declared.args.len());
            for argument in &declared.args {
                elems.push(argument.as_text_lossy());
            }
            flen = enum_set_display_length_from_lengths(
                code,
                declared.args.iter().map(ColumnTypeArg::byte_len),
            );
            field_type.set_elems(elems);
        }
        FieldTypeCode::NewDecimal => match declared.args.as_slice() {
            [] => {}
            [precision] => flen = type_argument(name, &declared.name, precision)?,
            [precision, scale] => {
                flen = type_argument(name, &declared.name, precision)?;
                decimal = type_argument(name, &declared.name, scale)?;
            }
            _ => {
                return Err(ColumnTypeError::new(format!(
                    "column `{name}` declares {} with more than two arguments",
                    declared.name
                )))
            }
        },
        // A fractional-seconds precision is the DECIMAL, not the flen; Go's
        // parser then derives the display width from it.
        FieldTypeCode::Timestamp | FieldTypeCode::Datetime | FieldTypeCode::Duration => {
            let fsp = match declared.args.as_slice() {
                [] => 0,
                [fsp] => type_argument(name, &declared.name, fsp)?,
                _ => {
                    return Err(ColumnTypeError::new(format!(
                        "column `{name}` declares {} with more than one argument",
                        declared.name
                    )))
                }
            };
            if fsp > 6 {
                return Err(ColumnTypeError::new(format!(
                    "column `{name}` declares {}({fsp}), whose fsp exceeds 6",
                    declared.name
                )));
            }
            decimal = fsp;
            let (base, _) = code.default_length_and_decimal();
            flen = if fsp == 0 { base } else { base + 1 + fsp };
        }
        _ => match declared.args.as_slice() {
            [] => {}
            [length] => flen = type_argument(name, &declared.name, length)?,
            _ => {
                return Err(ColumnTypeError::new(format!(
                    "column `{name}` declares {} with more than one argument",
                    declared.name
                )))
            }
        },
    }

    // Go's parser stamps `BinaryFlag` while building the type, so it is
    // already set by the time `setCharsetCollationFlenDecimal` asks whether
    // this type carries a charset at all -- `BLOB` does not, `TEXT` does.
    if is_intrinsically_binary(&declared.name) || declared.binary {
        field_type.add_flags(FieldTypeFlags::BINARY);
    }

    // Go `setCharsetCollationFlenDecimal`.
    if field_type.has_charset() {
        field_type.set_charset_name(charset);
        field_type.set_collation_name(collate);
    } else {
        field_type.set_charset_name(BINARY_CHARSET);
        field_type.set_collation_name(BINARY_CHARSET);
    }
    let (default_flen, default_decimal) = code.default_length_and_decimal();
    if decimal == UNSPECIFIED_LENGTH {
        decimal = default_decimal;
    }
    if flen == UNSPECIFIED_LENGTH {
        flen = default_flen;
        // Go issue #4684: an unsigned integer other than BIGINT is one digit
        // narrower, because it never prints a sign.
        if field_type.has_flag(FieldTypeFlags::UNSIGNED)
            && code != FieldTypeCode::LongLong
            && code.is_type_integer()
        {
            flen -= 1;
        }
        field_type.set_flen(flen);
    } else {
        let column_charset = field_type.charset_name().to_owned();
        adjust_blob_flen(&mut field_type, code, flen, &column_charset)?;
    }
    field_type.set_decimal(decimal);
    Ok(field_type)
}

/// Go `adjustBlobTypesFlen`: a declared `BLOB(n)`/`TEXT(n)` becomes whichever
/// member of the family actually holds `n` characters of this charset.
fn adjust_blob_flen(
    field_type: &mut FieldType,
    code: FieldTypeCode,
    flen: i64,
    charset: &str,
) -> Result<(), ColumnTypeError> {
    field_type.set_flen(flen);
    if code != FieldTypeCode::Blob {
        return Ok(());
    }
    let info = get_charset_info(charset)
        .map_err(|error| ColumnTypeError::new(format!("charset {charset}: {error}")))?;
    let length = flen.saturating_mul(i64::try_from(info.maxlen).unwrap_or(1));
    const TINY_BLOB_MAX: i64 = 255;
    const BLOB_MAX: i64 = 65535;
    const MEDIUM_BLOB_MAX: i64 = 16_777_215;
    const LONG_BLOB_MAX: i64 = 4_294_967_295;
    if length <= TINY_BLOB_MAX {
        field_type.set_code(FieldTypeCode::TinyBlob);
        field_type.set_flen(TINY_BLOB_MAX);
    } else if length <= BLOB_MAX {
        field_type.set_flen(BLOB_MAX);
    } else if length <= MEDIUM_BLOB_MAX {
        field_type.set_code(FieldTypeCode::MediumBlob);
        field_type.set_flen(MEDIUM_BLOB_MAX);
    } else {
        field_type.set_code(FieldTypeCode::LongBlob);
        field_type.set_flen(LONG_BLOB_MAX);
    }
    Ok(())
}

/// One numeric argument of a declared type, refusing a string literal where a
/// length belongs.
fn type_argument(
    column: &str,
    type_name: &str,
    argument: &ColumnTypeArg,
) -> Result<i64, ColumnTypeError> {
    let ColumnTypeArg::Text(text) = argument else {
        return Err(ColumnTypeError::new(format!(
            "column `{column}` declares {type_name} with a non-numeric argument"
        )));
    };
    text.parse().map_err(|_| {
        ColumnTypeError::new(format!(
            "column `{column}` declares {type_name}({text}), whose argument is not a \
             non-negative integer this node can store"
        ))
    })
}

