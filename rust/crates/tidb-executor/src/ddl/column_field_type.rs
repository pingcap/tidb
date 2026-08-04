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
//! found were the two disagreeing about the same statement. Their column-type
//! halves had drifted apart on five separate rules; captured from real TiDB
//! with `create table bt (a varchar(10) binary, b blob(100), c datetime(3),
//! d int unsigned, e int zerofill)`:
//!
//! ```text
//! `a` varchar(10)                    charset utf8mb4, collation utf8mb4_bin
//! `b` tinyblob                       CHARACTER_MAXIMUM_LENGTH 255
//! `c` datetime(3)                    DATETIME_PRECISION 3
//! `d` int(10) unsigned               flen 10, not 11
//! `e` int(10) unsigned zerofill      ZEROFILL implies UNSIGNED
//! ```
//!
//! so `BLOB(n)` promotes through the family, a fractional-seconds precision is
//! the DECIMAL and not the flen, an unsigned non-`BIGINT` integer is one digit
//! narrower (Go issue #4684), and `ZEROFILL` survives. Each of those was right
//! in exactly one of the two builders. Owning them once is what keeps the next
//! `CREATE`-time feature from having to be implemented twice.
//!
//! # `BINARY` is a collation modifier, NOT a charset
//!
//! Go's `typesNeedCharset` is a pure type-CODE test, so `VARCHAR(10) BINARY`
//! keeps its inherited charset and only takes that charset's default
//! collation (`OverwriteCollationWithBinaryFlag`). This workspace's
//! `field_type_has_charset` instead consults `BinaryFlag`, because Go tells
//! `BLOB` from `TEXT` by the charset its parser already stamped and these
//! share one type code. The two facts compose only if the flag tracks the
//! CHARSET -- an intrinsically binary type NAME, or a resolved charset of
//! `binary` -- and never the modifier. Captured: `varchar(10) CHARACTER SET
//! binary` restores as `varbinary(10)` reporting a NULL charset, while
//! `char(5) BINARY` under a `utf8mb4` table stays `char(5)` in
//! `utf8mb4`/`utf8mb4_bin`. The resolved pair itself is the caller's input.

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
///
/// This and a resolved charset of `binary` are the only two things that stamp
/// `BinaryFlag`; see the module doc for why the `BINARY` column MODIFIER
/// deliberately does not.
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
        // `FLOAT(M,D)` and `DOUBLE(M,D)` carry a precision AND a scale exactly
        // as DECIMAL does; captured, `float(10,2)` reports NUMERIC_PRECISION 10
        // / NUMERIC_SCALE 2 and restores as `float(10,2)`. A single-argument
        // `FLOAT(p)` never reaches here as a length: `ColumnType::
        // normalize_float_precision` has already turned it into a bare `FLOAT`
        // or a bare `DOUBLE`, so treating one argument as the flen is only the
        // conservative fallback.
        FieldTypeCode::NewDecimal | FieldTypeCode::Float | FieldTypeCode::Double => {
            match declared.args.as_slice() {
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
            }
        }
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
            // An out-of-range fsp is NOT refused here: Go's parser stores it
            // and `checkColumnAttributes` refuses it afterwards with 1426.
            // See [`check_column_attributes`].
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
    //
    // The flag tracks the CHARSET being binary, never the `BINARY` modifier:
    // captured, `varchar(10) CHARACTER SET binary` restores as `varbinary(10)`
    // reporting a NULL charset, while `char(5) BINARY` under a `utf8mb4` table
    // stays `char(5)` in `utf8mb4`/`utf8mb4_bin`.
    if is_intrinsically_binary(&declared.name) || charset.eq_ignore_ascii_case(BINARY_CHARSET) {
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

#[cfg(test)]
mod tests {
    use super::build_field_type;
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};

    /// The declared type of the one column of `CREATE TABLE probe (<decl>)`.
    fn parsed_type(declaration: &str) -> (String, tidb_ast::ColumnType) {
        let sql = format!("CREATE TABLE probe ({declaration})");
        let stmt = tidb_parser::parse(&sql).expect("the probe parses");
        let tidb_ast::Stmt::Ddl(ddl) = &stmt else {
            panic!("expected a DDL statement");
        };
        let tidb_ast::DdlStmt::CreateTable(create) = &**ddl else {
            panic!("expected CREATE TABLE");
        };
        let column = &create.columns[0];
        (column.name.clone(), column.ty.clone())
    }

    /// That column built under a `utf8mb4`/`utf8mb4_bin` table, which is what
    /// the captures below ran against.
    fn built(declaration: &str) -> FieldType {
        let (name, declared) = parsed_type(declaration);
        build_field_type(&name, &declared, "utf8mb4", "utf8mb4_bin")
            .expect("the probe declaration is buildable")
    }

    /// Captured from real TiDB with `create table bt (a varchar(10) binary,
    /// b blob(100), c datetime(3), d int unsigned, e int zerofill)`:
    ///
    /// ```text
    /// `a` varchar(10)                 charset utf8mb4, collation utf8mb4_bin
    /// `b` tinyblob                    CHARACTER_MAXIMUM_LENGTH 255
    /// `c` datetime(3)                 DATETIME_PRECISION 3
    /// `d` int(10) unsigned            flen 10
    /// `e` int(10) unsigned zerofill   ZEROFILL survives
    /// ```
    ///
    /// Each of these was right in exactly ONE of the two `CREATE TABLE`
    /// metadata builders before they shared this rule set.
    #[test]
    fn the_five_rules_the_two_builders_disagreed_about() {
        // The `BINARY` modifier is a collation modifier and NEVER a charset:
        // no `BinaryFlag`, so `has_charset` stays true and SHOW reports
        // `utf8mb4` rather than NULL.
        let a = built("a varchar(10) binary");
        assert_eq!(a.code(), FieldTypeCode::Varchar);
        assert!(a.has_charset());
        assert_eq!(a.charset_name(), "utf8mb4");
        assert_eq!(a.flen(), 10);

        // `adjustBlobTypesFlen` promotes through the family by BYTES, and a
        // BLOB's charset is `binary` (one byte per character), so `blob(100)`
        // fits a TINYBLOB while the same length of `text` (utf8mb4, four bytes
        // per character) does not.
        let b = built("b blob(100)");
        assert_eq!((b.code(), b.flen()), (FieldTypeCode::TinyBlob, 255));
        let text = built("b2 text(100)");
        assert_eq!((text.code(), text.flen()), (FieldTypeCode::Blob, 65535));

        // A fractional-seconds precision is the DECIMAL, not the flen.
        let c = built("c datetime(3)");
        assert_eq!((c.flen(), c.decimal()), (23, 3));

        // Go issue #4684: an unsigned non-BIGINT integer is one digit narrower.
        assert_eq!(built("d int unsigned").flen(), 10);
        assert_eq!(built("d2 int").flen(), 11);
        assert_eq!(built("d3 bigint unsigned").flen(), 20);

        assert!(built("e int zerofill").has_flag(FieldTypeFlags::ZEROFILL));
    }

    /// Captured with `create table b2 (a varchar(10) character set binary,
    /// c varchar(10) charset gbk binary, d char(5) binary) charset utf8mb4`:
    /// `a` restores as `varbinary(10)` reporting a NULL charset, `c` keeps
    /// `gbk` and takes `gbk_chinese_ci` -- the charset's DEFAULT collation,
    /// not the `_bin` the keyword suggests -- and `d` stays `char(5)` in
    /// `utf8mb4`. So `BinaryFlag` tracks the CHARSET, never the modifier.
    #[test]
    fn binary_flag_tracks_the_charset_and_not_the_modifier() {
        let (name, declared) = parsed_type("a varchar(10) character set binary");
        let explicit = build_field_type(&name, &declared, "binary", "binary").expect("buildable");
        assert!(explicit.has_flag(FieldTypeFlags::BINARY));
        assert!(!explicit.has_charset());

        let modifier = built("d char(5) binary");
        assert!(!modifier.has_flag(FieldTypeFlags::BINARY));
        assert_eq!(modifier.charset_name(), "utf8mb4");
    }

    /// `FLOAT(M,D)` and `DOUBLE(M,D)` carry a precision AND a scale; captured,
    /// `float(10,2)` reports NUMERIC_PRECISION 10 / NUMERIC_SCALE 2.
    #[test]
    fn float_and_double_take_a_precision_and_a_scale() {
        let a = built("a float(10,2)");
        assert_eq!((a.flen(), a.decimal()), (10, 2));
        let b = built("b double(12,3)");
        assert_eq!((b.flen(), b.decimal()), (12, 3));
    }
}

/// One column's declared type is refused by Go's `checkColumnAttributes`
/// (`pkg/ddl/create_table.go:743-754`) or its ENUM/SET member check
/// (`pkg/ddl/add_column.go:1190-1203`), with the error Go raises.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnAttributeError {
    /// Go `types.ErrMBiggerThanD` (1427): `DECIMAL`/`FLOAT`/`DOUBLE` with a
    /// scale larger than its precision.
    MBiggerThanD,
    /// Go `types.ErrTooBigPrecision` (1426): a `DATETIME`/`TIMESTAMP`/`TIME`
    /// fractional-seconds precision outside 0..=6.
    TooBigPrecision {
        /// The declared precision.
        precision: i64,
        /// `types.MaxFsp`.
        maximum: i64,
    },
    /// Go `types.ErrDuplicatedValueInType` (1291): an ENUM or SET names the
    /// same member twice.
    DuplicatedValueInType {
        /// The repeated member, in its DECLARED spelling.
        value: String,
        /// `ENUM` or `SET`, as Go spells it in the message.
        type_name: &'static str,
    },
}

/// Go `checkColumnAttributes` plus the ENUM/SET member check: what a built
/// `FieldType` still has to satisfy before the column can be created.
///
/// Splitting this out of [`build_field_type`] is Go's own division of
/// labour: the parser stores whatever was written, and the DDL builder is
/// what refuses it. That is why `DATETIME(7)` is 1426 and not a parse error.
///
/// The duplicate-member test uses the type's COLLATION key, exactly as Go's
/// `ctor.Key(elem)` does, so under a case-insensitive collation
/// `ENUM('x','X')` is a duplicate too.
///
/// Captured from TiDB:
///
/// ```text
/// create table a5(a decimal(2,5))    -> 1427 For float(M,D), double(M,D) or
///     decimal(M,D), M must be >= D (column 'a').
/// create table b1(a datetime(7))     -> 1426 Too-big precision 7 specified
///     for 'a'. Maximum is 6.
/// create table a6(a enum('x','x'))   -> 1291 Column 'a' has duplicated value
///     'x' in ENUM
/// create table a6b(a set('y','y'))   -> 1291 ... 'y' in SET
/// ```
pub fn check_column_attributes(field_type: &FieldType) -> Result<(), ColumnAttributeError> {
    match field_type.code() {
        FieldTypeCode::NewDecimal | FieldTypeCode::Double | FieldTypeCode::Float => {
            if field_type.flen() < field_type.decimal() {
                return Err(ColumnAttributeError::MBiggerThanD);
            }
        }
        FieldTypeCode::Datetime | FieldTypeCode::Duration | FieldTypeCode::Timestamp => {
            let decimal = field_type.decimal();
            if decimal != tidb_datatype::UNSPECIFIED_FSP
                && !(tidb_datatype::MIN_FSP..=tidb_datatype::MAX_FSP).contains(&decimal)
            {
                return Err(ColumnAttributeError::TooBigPrecision {
                    precision: decimal,
                    maximum: tidb_datatype::MAX_FSP,
                });
            }
        }
        code @ (FieldTypeCode::Enum | FieldTypeCode::Set) => {
            let collation = field_type.collation();
            let mut seen: Vec<Vec<u8>> = Vec::with_capacity(field_type.elems().len());
            for member in field_type.elems() {
                let key = collation.key(member.as_bytes());
                if seen.contains(&key) {
                    return Err(ColumnAttributeError::DuplicatedValueInType {
                        value: member.clone(),
                        type_name: if code == FieldTypeCode::Set {
                            "SET"
                        } else {
                            "ENUM"
                        },
                    });
                }
                seen.push(key);
            }
        }
        _ => {}
    }
    Ok(())
}
