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

//! The charset/collation precedence a table and its columns are built under,
//! over Go's string-named charset registry:
//!
//! * [`resolve_charset_collation`] -- Go `ResolveCharsetCollation`
//!   (`pkg/ddl/executor.go:916`), the first non-empty pair of an ordered list
//!   wins, else the server default;
//! * [`get_charset_and_collate_in_table_option`] -- Go
//!   `GetCharsetAndCollateInTableOption` (`executor.go:1582`), the pair a
//!   statement's table options settle to;
//! * [`get_charset_and_collate_in_column_def`] and
//!   [`overwrite_collation_with_binary_flag`] -- Go `pkg/ddl/add_column.go`'s
//!   column-level halves;
//! * [`get_default_collation`] -- Go `GetDefaultCollation`, which is where
//!   `@@default_collation_for_utf8mb4` enters.
//!
//! The parser in Go refuses an unknown charset or collation name before DDL
//! sees it (1115 / 1273); this port's parser keeps the written name, so the
//! lookups here raise those same errors and return the CANONICAL name, which
//! is what Go's parser would have stored. The type half (`types.StrToType`
//! plus `setCharsetCollationFlenDecimal`) lives in
//! [`crate::column_field_type`].

use tidb_ast::{ColumnDef, ColumnOption, TableOption};
use tidb_datatype::{CharsetInfo, CollationInfo, FieldTypeCode};
use tidb_error::mysql::MysqlError;

/// Go `charset.GetCharsetInfo` as a DDL statement reaches it: a name Go's
/// parser would have refused is `ErrUnknownCharacterSet` (1115).
fn get_charset_info(name: &str) -> Result<CharsetInfo, MysqlError> {
    tidb_datatype::get_charset_info(name).map_err(|_| {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_UNKNOWN_CHARACTER_SET
                .generate_with_stack_by_args(&[name.into()]),
        )
    })
}

/// Go `collate.GetCollationByName`: an unknown name is `ErrUnknownCollation`
/// (1273), and with the new collation framework enabled a collation without
/// its own implementation is `ErrUnsupportedCollation` (1273).
pub fn get_collation_by_name(name: &str) -> Result<CollationInfo, MysqlError> {
    tidb_datatype::get_supported_collation_by_name(name).map_err(|error| match error {
        tidb_datatype::CollationError::UnsupportedCollation(name) => MysqlError::from_terror(
            &tidb_error::catalog::collate::ERR_UNSUPPORTED_COLLATION
                .generate_with_stack_by_args(&[name.into()]),
        ),
        tidb_datatype::CollationError::Registry(_) => MysqlError::from_terror(
            &tidb_dbterror::ERR_UNKNOWN_COLLATION.generate_with_stack_by_args(&[name.into()]),
        ),
    })
}

/// Go `getDefaultCollationForUTF8MB4`: `@@default_collation_for_utf8mb4` for
/// utf8mb4, empty for every other charset.
#[must_use]
pub fn get_default_collation_for_utf8mb4<'a>(cs: &str, default_utf8mb4_coll: &'a str) -> &'a str {
    if cs == "utf8mb4" {
        default_utf8mb4_coll
    } else {
        ""
    }
}

/// Go `GetDefaultCollation`: the default collation of `cs`, which for utf8mb4
/// is `@@default_collation_for_utf8mb4`.
///
/// # Errors
///
/// 1115 for an unknown charset.
pub fn get_default_collation(cs: &str, default_utf8mb4_collation: &str) -> Result<String, MysqlError> {
    let coll = get_default_collation_for_utf8mb4(cs, default_utf8mb4_collation);
    if !coll.is_empty() {
        return Ok(coll.to_owned());
    }
    Ok(get_charset_info(cs)?.default_collation)
}

/// Go `ResolveCharsetCollation`: the first `(charset, collate)` pair of
/// `charset_opts` with either side set decides -- a collation names its own
/// charset and must agree with a charset written beside it, a charset alone
/// takes its default collation -- and when every pair is empty the server
/// default (`utf8mb4`, `@@default_collation_for_utf8mb4` or `utf8mb4_bin`).
///
/// # Errors
///
/// 1273 for an unknown or unsupported collation, 1253 for a collation of
/// another charset, 1115 for an unknown charset.
pub fn resolve_charset_collation(
    charset_opts: &[(&str, &str)],
    utf8mb4_default_coll: &str,
) -> Result<(String, String), MysqlError> {
    for (chs, col) in charset_opts {
        if !col.is_empty() {
            let collation = get_collation_by_name(col)?;
            // Go compares against the parser's canonical charset name.
            let written_charset = tidb_datatype::get_charset_info(chs)
                .map_or_else(|_| (*chs).to_owned(), |info| info.name);
            if !chs.is_empty() && collation.charset_name != written_charset {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_COLLATION_CHARSET_MISMATCH
                        .generate_with_stack_by_args(&[col.into(), chs.into()]),
                ));
            }
            return Ok((collation.charset_name, collation.name));
        }
        if !chs.is_empty() {
            let chs = get_charset_info(chs)?.name;
            let coll = get_default_collation(&chs, utf8mb4_default_coll)?;
            return Ok((chs, coll));
        }
    }
    let (chs, coll) = tidb_datatype::get_default_charset_and_collate();
    let utf8mb4_coll = get_default_collation_for_utf8mb4(chs, utf8mb4_default_coll);
    if !utf8mb4_coll.is_empty() {
        return Ok((chs.to_owned(), utf8mb4_coll.to_owned()));
    }
    Ok((chs.to_owned(), coll.to_owned()))
}

/// Go `GetCharsetAndCollateInTableOption`: the charset and collation the
/// table options from `start_idx` on settle to, each empty when no option
/// names it. A second charset naming another set is 1302, a charset alone
/// takes its default collation (`@@default_collation_for_utf8mb4` for
/// utf8mb4), and a collation must belong to the charset already named
/// (1253).
///
/// # Errors
///
/// 1115, 1273, 1302 or 1253.
pub fn get_charset_and_collate_in_table_option(
    start_idx: usize,
    options: &[TableOption],
    default_utf8mb4_coll: &str,
) -> Result<(String, String), MysqlError> {
    let mut chs = String::new();
    let mut coll = String::new();
    for option in options.iter().skip(start_idx) {
        match option {
            // we set the charset to the last option. example: alter table t
            // charset latin1 charset utf8 collate utf8_bin; the charset will
            // be utf8, collate will be utf8_bin
            TableOption::CharacterSet(name) => {
                let info = get_charset_info(name)?;
                if chs.is_empty() {
                    chs.clone_from(&info.name);
                } else if chs != info.name {
                    return Err(MysqlError::from_terror(
                        &tidb_dbterror::ERR_CONFLICTING_DECLARATIONS
                            .generate_with_stack_by_args(&[chs.into(), info.name.into()]),
                    ));
                }
                if coll.is_empty() {
                    let default_coll = get_default_collation_for_utf8mb4(&chs, default_utf8mb4_coll);
                    coll = if default_coll.is_empty() {
                        info.default_collation.clone()
                    } else {
                        default_coll.to_owned()
                    };
                }
            }
            TableOption::Collate(name) => {
                let info = get_collation_by_name(name)?;
                if chs.is_empty() {
                    chs.clone_from(&info.charset_name);
                } else if chs != info.charset_name {
                    return Err(MysqlError::from_terror(
                        &tidb_dbterror::ERR_COLLATION_CHARSET_MISMATCH
                            .generate_with_stack_by_args(&[(&info.name).into(), (&chs).into()]),
                    ));
                }
                coll = info.name;
            }
            _ => {}
        }
    }
    Ok((chs, coll))
}

/// Go `getCharsetAndCollateInColumnDef` (`pkg/ddl/add_column.go:342`): the
/// column's declared charset, with its default collation, then the LAST
/// `COLLATE` option, which must belong to that charset.
///
/// # Errors
///
/// 1115, 1273 or 1253.
pub fn get_charset_and_collate_in_column_def(
    def: &ColumnDef,
    default_utf8mb4_coll: &str,
) -> Result<(String, String), MysqlError> {
    let mut chs = match def.ty.charset.as_deref() {
        Some(name) => get_charset_info(name)?.name,
        None => String::new(),
    };
    let mut coll = String::new();
    if !chs.is_empty() {
        coll = get_default_collation(&chs, default_utf8mb4_coll)?;
    }
    for option in &def.options {
        if let ColumnOption::Collate(name) = option {
            let info = get_collation_by_name(name)?;
            if chs.is_empty() {
                chs.clone_from(&info.charset_name);
            } else if chs != info.charset_name {
                return Err(MysqlError::from_terror(
                    &tidb_dbterror::ERR_COLLATION_CHARSET_MISMATCH
                        .generate_with_stack_by_args(&[(&info.name).into(), (&chs).into()]),
                ));
            }
            coll = info.name;
        }
    }
    Ok((chs, coll))
}

/// Go `OverwriteCollationWithBinaryFlag` (`pkg/ddl/add_column.go:372`): a
/// string column written with the `BINARY` modifier takes its charset's
/// default collation (`*_bin` for the charsets whose default is not), unless
/// the column named both its charset and a collation.
#[must_use]
pub fn overwrite_collation_with_binary_flag(
    col_def: &ColumnDef,
    chs: String,
    coll: String,
    default_utf8mb4_coll: &str,
) -> (String, String) {
    let ignore_bin_flag = col_def.ty.charset.is_some()
        && col_def
            .options
            .iter()
            .any(|option| matches!(option, ColumnOption::Collate(_)));
    if ignore_bin_flag {
        return (chs, coll);
    }
    let need_overwrite_bin_coll =
        col_def.ty.type_code().is_some_and(FieldTypeCode::is_string) && col_def.ty.binary;
    if need_overwrite_bin_coll {
        return match get_default_collation(&chs, default_utf8mb4_coll) {
            Ok(new_coll) => (chs, new_coll),
            Err(_) => (chs, coll),
        };
    }
    (chs, coll)
}

/// Go `isIntCol`'s type switch. Narrower than
/// [`FieldTypeCode::is_type_integer`]: a `YEAR` primary key is never the INT
/// row handle -- it is clustered as a common handle or not at all.
#[must_use]
pub fn is_int_column_type(code: FieldTypeCode) -> bool {
    matches!(
        code,
        FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
    )
}
