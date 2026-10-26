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

//! Go `pkg/executor/show.go`: the `ShowExec` fetchers that moved here from
//! the session.

use tidb_datatype::Datum;
use tidb_variable::sysvar;

use crate::sessionctx::{sys_var_hidden_for_sem, SessionContext};

/// Go `ShowExec.fetchShowVariables`: one `Variable_name | Value` row per
/// variable whose name matches `pattern` (already lowercased, Go
/// `ShowBaseExtractor`). GLOBAL lists no SESSION-only variable; neither form
/// lists a noop variable while `tidb_enable_noop_variables` is OFF, a
/// variable SEM hides, or an internal session variable. GLOBAL reads the
/// GLOBAL tier (a variable with no GLOBAL copy reports its default);
/// SESSION reads the session's own values, the `tidb_last_*_info` getters
/// computed from live session state.
pub fn fetch_show_variables(
    ctx: &dyn SessionContext,
    global: bool,
    pattern: Option<&str>,
) -> Vec<Vec<Datum>> {
    let vars = ctx.session_vars();
    let noop_enabled = vars.noop_variables_enabled();
    let mut rows = Vec::new();
    for definition in sysvar::SYS_VARS {
        if (global && definition.scope == sysvar::SCOPE_SESSION)
            || (definition.is_noop() && !noop_enabled)
        {
            continue;
        }
        if sys_var_hidden_for_sem(ctx, definition.name) || definition.is_internal_session_variable()
        {
            continue;
        }
        let matches = pattern.is_none_or(|pattern| {
            crate::like_match_with_collation(
                definition.name,
                pattern,
                None,
                tidb_datatype::Collation::Utf8Mb4Bin,
            )
        });
        if !matches {
            continue;
        }
        let live = if global {
            None
        } else {
            match definition.name {
                "tidb_last_ddl_info" => Some(vars.last_ddl_info_value()),
                "tidb_last_query_info" => Some(vars.last_query_info_value()),
                "tidb_last_txn_info" => Some(vars.last_txn_info_value()),
                _ => None,
            }
        };
        let value = match live {
            Some(live) => live,
            None if global => vars
                .get_global(definition.name)
                .unwrap_or_else(|_| sysvar::effective_default(definition)),
            None => vars
                .get_system(definition.name)
                .unwrap_or_else(|_| sysvar::effective_default(definition)),
        };
        rows.push(vec![
            Datum::Bytes(definition.name.as_bytes().to_vec()),
            Datum::Bytes(value.into_bytes()),
        ]);
    }
    rows
}

/// Go `ShowExec.fetchShowDatabases`: every schema the session can see (one
/// `DBIsVisible` per schema), `information_schema` first as the schema lists
/// it.
pub fn fetch_show_databases(ctx: &dyn SessionContext, is: &crate::Catalog) -> Vec<String> {
    is.database_names()
        .into_iter()
        .filter(|name| ctx.privilege_manager().db_is_visible(name))
        .collect()
}

/// Go `ShowExec.fetchShowTables`: `DBIsVisible` before the schema lookup (so
/// a schema the session cannot see is 1044 whether or not it exists), the
/// unknown schema, then every table the session holds any privilege on but
/// `CREATE TEMPORARY TABLES` (Go's standing TODO: column grants are not
/// consulted). With `full`, each name carries its `Table_type`.
pub fn fetch_show_tables(
    ctx: &dyn SessionContext,
    is: &crate::Catalog,
    database: &str,
    full: bool,
) -> Result<Vec<(String, Option<&'static str>)>, crate::DriverError> {
    use tidb_hack::GoCaseCompare;
    use tidb_mysql::privilege::{AllPrivMask, CreateTMPTablePriv, PrivilegeType};

    if !ctx.privilege_manager().db_is_visible(database) {
        let (user, host) = ctx
            .session_vars()
            .current_user
            .as_deref()
            .and_then(|identity| identity.split_once('@'))
            .unwrap_or_default();
        return Err(crate::DriverError::DbAccessDenied {
            user: user.to_owned(),
            host: host.to_owned(),
            database: database.to_owned(),
        });
    }
    let Some(names) = is.table_names(database) else {
        return Err(crate::DriverError::Schema(
            crate::SchemaErrorKind::UnknownDatabase(database.to_owned()),
        ));
    };
    let any_but_temporary = PrivilegeType(AllPrivMask.0 & !CreateTMPTablePriv.0);
    let is_virtual_schema = ["information_schema", "performance_schema", "metrics_schema"]
        .iter()
        .any(|schema| database.go_lower_eq(schema));
    Ok(names
        .into_iter()
        .filter(|name| {
            ctx.privilege_manager()
                .request_verification(database, name, any_but_temporary)
        })
        .map(|name| {
            let table_type = full.then(|| {
                if is_virtual_schema {
                    "SYSTEM VIEW"
                } else if is.is_view_in(database, &name) {
                    "VIEW"
                } else if is.is_sequence_in(database, &name) {
                    "SEQUENCE"
                } else {
                    "BASE TABLE"
                }
            });
            (name, table_type)
        })
        .collect())
}

/// The collations `SHOW COLLATION` reports, in mock TiDB's own capture order
/// (alphabetical by collation name). `Utf8Mb4ZhPinyinTiDbAsCs` is
/// deliberately excluded: it is a reserved stub collation, and Go's own
/// `SHOW COLLATION` capture omits it too.
pub const SHOW_COLLATION_ROWS: &[tidb_datatype::Collation] = &[
    tidb_datatype::Collation::AsciiBin,
    tidb_datatype::Collation::Binary,
    tidb_datatype::Collation::Gb18030Bin,
    tidb_datatype::Collation::Gb18030ChineseCi,
    tidb_datatype::Collation::GbkBin,
    tidb_datatype::Collation::GbkChineseCi,
    tidb_datatype::Collation::Latin1Bin,
    tidb_datatype::Collation::Utf8Bin,
    tidb_datatype::Collation::Utf8GeneralCi,
    tidb_datatype::Collation::Utf8UnicodeCi,
    tidb_datatype::Collation::Utf8Mb40900AiCi,
    tidb_datatype::Collation::Utf8Mb40900Bin,
    tidb_datatype::Collation::Utf8Mb4Bin,
    tidb_datatype::Collation::Utf8Mb4GeneralCi,
    tidb_datatype::Collation::Utf8Mb4UnicodeCi,
];

/// Whether `collation` is the one `SHOW COLLATION` marks `Default`.
///
/// Go reads `Collation.IsDefault`, which `switchDefaultCollation` keeps in
/// step with the owning charset's default collation, so this is exactly
/// "`collation` is its charset's default" and is derived rather than listed.
/// An explicit list here was a second place for the `gbk`/`gb18030` default to
/// be spelled, and it carried a doc claim -- that
/// [`tidb_datatype::Charset::default_collation`] returns the `_bin`
/// collations for those charsets -- that was already untrue when read.
fn is_default_show_collation(collation: tidb_datatype::Collation, utf8mb4_default: &str) -> bool {
    // Go `isUTF8MB4AndDefaultCollation`: utf8mb4's default is the session's
    // `default_collation_for_utf8mb4`.
    if collation.charset() == tidb_datatype::Charset::Utf8Mb4 {
        return collation.name() == utf8mb4_default;
    }
    collation.charset().default_collation() == collation
}

/// The session's `default_collation_for_utf8mb4`, which Go's
/// `isUTF8MB4AndDefaultCollation` and `GetSupportedCharsets` read.
fn utf8mb4_default_collation(ctx: &dyn SessionContext) -> String {
    ctx.session_vars()
        .get_system("default_collation_for_utf8mb4")
        .unwrap_or_else(|_| "utf8mb4_bin".to_owned())
}

/// Go `ShowExec.fetchShowCharset`: `charset.GetSupportedCharsets` (whose
/// rows carry the default collation `switchDefaultCollation` last wrote),
/// utf8mb4's default read from the session, filtered by `pattern`.
pub fn fetch_show_charset(ctx: &dyn SessionContext, pattern: Option<&str>) -> Vec<Vec<Datum>> {
    let utf8mb4_default = utf8mb4_default_collation(ctx);
    tidb_datatype::get_supported_charsets()
        .into_iter()
        .filter(|info| {
            pattern.is_none_or(|pattern| {
                crate::like_match_with_collation(
                    &info.name,
                    pattern,
                    None,
                    tidb_datatype::Collation::Utf8Mb4Bin,
                )
            })
        })
        .map(|info| {
            let default_collation = if info.name == "utf8mb4" {
                utf8mb4_default.clone()
            } else {
                info.default_collation
            };
            vec![
                Datum::Bytes(info.name.into_bytes()),
                Datum::Bytes(info.description.into_bytes()),
                Datum::Bytes(default_collation.into_bytes()),
                Datum::Int(info.maxlen as i64),
            ]
        })
        .collect()
}

/// Go `ShowExec.fetchShowEngines` as the mock/embedded single-engine server
/// answers it: the one `InnoDB` row, filtered by `pattern`.
pub fn fetch_show_engines(pattern: Option<&str>) -> Vec<Vec<Datum>> {
    let included = pattern.is_none_or(|pattern| {
        crate::like_match_with_collation(
            "InnoDB",
            pattern,
            None,
            tidb_datatype::Collation::Utf8Mb4Bin,
        )
    });
    included
        .then(|| {
            vec![
                Datum::Bytes(b"InnoDB".to_vec()),
                Datum::Bytes(b"DEFAULT".to_vec()),
                Datum::Bytes(
                    b"Supports transactions, row-level locking, and foreign keys".to_vec(),
                ),
                Datum::Bytes(b"YES".to_vec()),
                Datum::Bytes(b"YES".to_vec()),
                Datum::Bytes(b"YES".to_vec()),
            ]
        })
        .into_iter()
        .collect()
}

/// Go `ShowExec.fetchShowCollation`: one `Collation | Charset | Id | Default
/// | Compiled | Sortlen | Pad_attribute` row per collation Go lists, the
/// pattern already lowercased (Go `ShowBaseExtractor`).
pub fn fetch_show_collation(ctx: &dyn SessionContext, pattern: Option<&str>) -> Vec<Vec<Datum>> {
    let utf8mb4_default = utf8mb4_default_collation(ctx);
    let mut rows = Vec::new();
    for &collation in SHOW_COLLATION_ROWS {
        let name = collation.name();
        if let Some(pattern) = pattern {
            if !crate::like_match_with_collation(
                name,
                pattern,
                None,
                tidb_datatype::Collation::Utf8Mb4Bin,
            ) {
                continue;
            }
        }
        let (sortlen, pad_attribute): (i64, &str) = match collation {
            tidb_datatype::Collation::Utf8UnicodeCi
            | tidb_datatype::Collation::Utf8Mb4UnicodeCi => (8, "PAD SPACE"),
            tidb_datatype::Collation::Utf8Mb40900AiCi => (0, "NO PAD"),
            tidb_datatype::Collation::Binary | tidb_datatype::Collation::Utf8Mb40900Bin => {
                (1, "NO PAD")
            }
            _ => (1, "PAD SPACE"),
        };
        rows.push(vec![
            Datum::Bytes(name.as_bytes().to_vec()),
            Datum::Bytes(collation.charset().name().as_bytes().to_vec()),
            Datum::Int(i64::from(collation.id())),
            Datum::Bytes(if is_default_show_collation(collation, &utf8mb4_default) {
                b"Yes".to_vec()
            } else {
                Vec::new()
            }),
            Datum::Bytes(b"Yes".to_vec()),
            Datum::Int(sortlen),
            Datum::Bytes(pad_attribute.as_bytes().to_vec()),
        ]);
    }
    rows
}
