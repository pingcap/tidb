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

//! Resolving a declared column into a [`FieldType`]: the type code and its
//! flen/decimal, and the charset/collation precedence that decides them.
//!
//! Inside: [`field_type_of`], the entry point, whose doc lists the five-level
//! precedence in order; [`resolve_pair`], Go `ResolveCharsetCollation` over
//! one `charset`/`collate` pair; [`table_charset_of`] for the table-level
//! default the columns fall back to; [`charset_named`] and
//! [`collation_named`], the name lookups that reject an unknown name rather
//! than falling back silently; and the two type-shape facts
//! [`is_intrinsically_binary`] and [`blob_family_flen`].
//!
//! Mirrors the column half of Go `pkg/ddl`'s `buildColumnAndConstraint` --
//! `types.StrToType` plus `ResolveCharsetCollation` in
//! `pkg/ddl/ddl_api.go`. The constraint half is in the sibling
//! `table_constraints` module, and `ALTER TABLE`'s column actions call
//! [`field_type_of`] from `alter_table`.

use super::{ColumnDef, ColumnTypeArg, DriverError, TableCharset};
use tidb_datatype::{
    str_to_type, Charset, Collation, FieldType, FieldTypeBuilder, FieldTypeCode, FieldTypeFlags,
};

/// Go `mysql.NotNullFlag`.
pub(crate) const NOT_NULL_FLAG: u32 = 1;
/// Go `ResolveCharsetCollation` over one `charset`/`collate` pair: either side
/// alone determines the other, and both together must agree.
fn resolve_pair(
    charset: Option<Charset>,
    collation: Option<Collation>,
    fallback: TableCharset,
) -> Result<TableCharset, DriverError> {
    match (charset, collation) {
        (Some(charset), Some(collation)) => {
            if collation.charset() != charset {
                return Err(DriverError::Unsupported(
                    "COLLATE is not valid for the declared CHARACTER SET",
                ));
            }
            Ok(TableCharset { charset, collation })
        }
        (Some(charset), None) => Ok(TableCharset {
            charset,
            collation: charset.default_collation(),
        }),
        (None, Some(collation)) => Ok(TableCharset {
            charset: collation.charset(),
            collation,
        }),
        (None, None) => Ok(fallback),
    }
}

/// Parses a charset name, rejecting one this tier does not carry.
fn charset_named(name: &str) -> Result<Charset, DriverError> {
    Charset::from_name(name).ok_or(DriverError::Unsupported("unknown character set"))
}

/// Parses a collation name, rejecting one this tier does not carry.
fn collation_named(name: &str) -> Result<Collation, DriverError> {
    Collation::from_name(name).ok_or(DriverError::Unsupported("unknown collation"))
}

/// The table's own `DEFAULT CHARSET=` / `DEFAULT COLLATE=` options, falling
/// back to the server default when neither is written.
pub(crate) fn table_charset_of(
    options: &[tidb_ast::TableOption],
) -> Result<TableCharset, DriverError> {
    let mut charset = None;
    let mut collation = None;
    for option in options {
        match option {
            tidb_ast::TableOption::CharacterSet(name) => charset = Some(charset_named(name)?),
            tidb_ast::TableOption::Collate(name) => collation = Some(collation_named(name)?),
            _ => {}
        }
    }
    resolve_pair(charset, collation, TableCharset::default())
}

/// Whether the declared type name is one whose representation IS binary --
/// Go's `BINARY`/`VARBINARY` and the BLOB family, which carry charset
/// `binary` no matter what the table's default is.
fn is_intrinsically_binary(type_name: &str) -> bool {
    matches!(
        type_name,
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB"
    )
}

/// Go's default `flen` for a BLOB/TEXT-family column, which has no written
/// length. Captured from `information_schema.columns`: `blob` reports 65535,
/// `tinytext` 255 and `longblob` 4294967295.
const fn blob_family_flen(code: FieldTypeCode) -> Option<i64> {
    match code {
        FieldTypeCode::TinyBlob => Some(255),
        FieldTypeCode::Blob => Some(65535),
        FieldTypeCode::MediumBlob => Some(16_777_215),
        FieldTypeCode::LongBlob => Some(4_294_967_295),
        _ => None,
    }
}

/// Builds the column's `FieldType` from its parsed SQL type: code via
/// `str_to_type`, flen/decimal from numeric type arguments, unsigned flag, and
/// Go `ResolveCharsetCollation`'s charset/collation precedence.
///
/// Precedence, highest first (Go `pkg/ddl/ddl_api.go`):
/// 1. `BINARY`/`VARBINARY`/BLOB and an explicit `CHARACTER SET binary` are
///    binary-charset columns carrying `BinaryFlag`; a `VARCHAR CHARACTER SET
///    binary` becomes `varbinary` exactly because of this (captured).
/// 2. the column's own `CHARACTER SET` and/or `COLLATE`,
/// 3. the column's `BINARY` attribute, which picks the charset's `_bin`
///    collation (captured: `VARCHAR(10) BINARY` reports `utf8mb4_bin`),
/// 4. the table's `DEFAULT CHARSET=`/`COLLATE=`,
/// 5. the server default (`utf8mb4` / `utf8mb4_bin`).
pub(crate) fn field_type_of(
    def: &ColumnDef,
    table: TableCharset,
) -> Result<FieldType, DriverError> {
    let code = str_to_type(&def.ty.name.to_lowercase());
    if code == FieldTypeCode::Unspecified {
        return Err(DriverError::Unsupported("unsupported column type"));
    }
    let mut builder = FieldTypeBuilder::new().with_code(code);

    let written_charset = def.ty.charset.as_deref().map(charset_named).transpose()?;
    let written_collation = def
        .options
        .iter()
        .filter_map(|option| match option {
            tidb_ast::ColumnOption::Collate(name) => Some(name.as_str()),
            _ => None,
        })
        .next_back()
        .map(collation_named)
        .transpose()?;

    // A JSON column is a BINARY-charset column too (Go
    // `setCharsetCollationFlenDecimal`'s `TypeJSON` arm), which is what makes
    // the wire report `charset=binary, flag=128` for it. The document's own
    // text is still UTF-8.
    let binary = code == FieldTypeCode::Json
        || is_intrinsically_binary(&def.ty.name)
        || written_charset == Some(Charset::Binary);
    let resolved = if binary {
        TableCharset {
            charset: Charset::Binary,
            collation: Collation::Binary,
        }
    } else if written_collation.is_none() && def.ty.binary {
        // The `BINARY` column attribute keeps the inherited charset and picks
        // that charset's `_bin` collation.
        let charset = written_charset.unwrap_or(table.charset);
        TableCharset {
            charset,
            collation: charset.default_collation(),
        }
    } else {
        resolve_pair(written_charset, written_collation, table)?
    };
    builder = builder
        .charset_set(resolved.charset.name())
        .collation_set(resolved.collation.name());
    if binary && code.is_string() {
        // `HasCharset` is false exactly for a string type carrying
        // `BinaryFlag`, which is what makes SHOW/information_schema report a
        // NULL charset and collation for it (captured).
        builder = builder.add_flags(FieldTypeFlags::BINARY);
    }

    if matches!(code, FieldTypeCode::Enum | FieldTypeCode::Set) {
        // Go stores the members on the field type and derives the display
        // length from them: captured, `enum('a','B')` reports
        // CHARACTER_MAXIMUM_LENGTH 1 and `set('a','B')` reports 3.
        let elems: Vec<String> = def
            .ty
            .args
            .iter()
            .map(ColumnTypeArg::as_text_lossy)
            .collect();
        if elems.is_empty() {
            return Err(DriverError::Unsupported("ENUM/SET needs members"));
        }
        if let Some(flen) = def.ty.enum_set_display_length() {
            builder = builder.flen_set(flen);
        }
        builder = builder.elems(elems);
    } else {
        let mut numeric_args = def.ty.args.iter().filter_map(|arg| match arg {
            ColumnTypeArg::Text(text) => text.parse::<i64>().ok(),
            ColumnTypeArg::Bytes(_) => None,
        });
        match numeric_args.next() {
            Some(flen) => builder = builder.flen_set(flen),
            // A BLOB/TEXT-family column carries no written length, so it takes
            // its type's fixed capacity.
            None => {
                if let Some(flen) = blob_family_flen(code) {
                    builder = builder.flen_set(flen);
                }
            }
        }
        if let Some(decimal) = numeric_args.next() {
            builder = builder.decimal_set(decimal);
        }
    }

    if def.ty.unsigned {
        builder = builder.add_flags(FieldTypeFlags::UNSIGNED);
    }
    Ok(builder.build())
}
