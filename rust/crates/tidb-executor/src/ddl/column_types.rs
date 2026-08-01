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
//! Inside: [`field_type_of`], the entry point, whose doc lists the precedence
//! in order; [`resolve_pair`], Go `ResolveCharsetCollation` over one
//! `charset`/`collate` pair; [`table_charset_of`] for the table-level default
//! the columns fall back to; and [`charset_named`]/[`collation_named`], the
//! name lookups that reject an unknown name rather than falling back
//! silently.
//!
//! Mirrors the charset half of Go `pkg/ddl`'s `buildColumnAndConstraint` --
//! `getCharsetAndCollateInColumnDef` then `ResolveCharsetCollation` and
//! `OverwriteCollationWithBinaryFlag` in `pkg/ddl/add_column.go`. The type
//! half (`types.StrToType` plus `setCharsetCollationFlenDecimal`) lives in
//! [`crate::ddl::column_field_type`] and is SHARED with
//! `tidb_exec::table_info_build`. The constraint half is in the sibling
//! `table_constraints` module, and `ALTER TABLE`'s column actions call
//! [`field_type_of`] from `alter_table`.

use super::{column_field_type, ColumnDef, DriverError, TableCharset};
use tidb_datatype::{Charset, Collation, FieldType};

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
                return Err(DriverError::unsupported(
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
    Charset::from_name(name).ok_or(DriverError::unsupported("unknown character set"))
}

/// Parses a collation name, rejecting one this tier does not carry.
fn collation_named(name: &str) -> Result<Collation, DriverError> {
    Collation::from_name(name).ok_or(DriverError::unsupported("unknown collation"))
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

/// Builds the column's `FieldType` from its parsed SQL type.
///
/// The type code and the flen/decimal its arguments imply come from
/// [`crate::ddl::column_field_type`], which is the SAME rule set
/// `tidb_exec::table_info_build`'s `TableInfo` builder uses -- the two once
/// disagreed about `BLOB(n)`, `DATETIME(n)`, unsigned integer widths and
/// `ZEROFILL`, and each disagreement was a wrong table rather than a missing
/// feature. What stays here is the charset/collation PRECEDENCE, which is the
/// part this tier expresses over its own `Charset`/`Collation` enums.
///
/// Precedence, highest first (Go `pkg/ddl/ddl_api.go`):
/// 1. the column's own `CHARACTER SET` and/or `COLLATE`,
/// 2. the column's `BINARY` attribute, which keeps the inherited charset and
///    takes that charset's DEFAULT collation -- captured, `char(5) BINARY`
///    under `utf8mb4` reports `utf8mb4_bin` while `varchar(10) CHARSET gbk
///    BINARY` reports `gbk_chinese_ci`, so it really is the default and not
///    the `_bin` the keyword suggests (Go `OverwriteCollationWithBinaryFlag`
///    calls `GetDefaultCollation`),
/// 3. the table's `DEFAULT CHARSET=`/`COLLATE=`,
/// 4. the server default (`utf8mb4` / `utf8mb4_bin`).
///
/// `BINARY`/`VARBINARY`/BLOB and an explicit `CHARACTER SET binary` are
/// binary-charset columns; the shared builder stamps `BinaryFlag` for them, so
/// a `VARCHAR CHARACTER SET binary` becomes `varbinary` (captured) and a JSON
/// column reports `charset=binary, flag=128` on the wire.
pub(crate) fn field_type_of(
    def: &ColumnDef,
    table: TableCharset,
) -> Result<FieldType, DriverError> {
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

    let mut resolved = resolve_pair(written_charset, written_collation, table)?;
    // Go `OverwriteCollationWithBinaryFlag`: the modifier is ignored only when
    // the column wrote BOTH an explicit charset and an explicit collation.
    if def.ty.binary && !(written_charset.is_some() && written_collation.is_some()) {
        resolved.collation = resolved.charset.default_collation();
    }

    column_field_type::build_field_type(
        &def.name,
        &def.ty,
        resolved.charset.name(),
        resolved.collation.name(),
    )
    .map_err(|error| DriverError::unsupported(error.reason))
}
