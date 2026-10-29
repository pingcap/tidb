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

pub mod columns;
pub mod create;
pub mod create_database;
pub mod create_placement_policy;
pub mod exec;
pub mod index;
pub mod placement;
pub mod stats;

/// The `ShowBaseExtractor` of a planned SHOW, as Go's fetchers read it.
pub type Extractor<'a> = Option<&'a tidb_planner::show_predicate_extractor::ShowBaseExtractor>;

/// Go's `fieldFilter` / `fieldPatternsLike` test, `field_name` against the
/// exact field and `pattern_name` against the wildcard: each fetcher passes
/// the spelling Go's own comparison uses.
pub fn extractor_admits(extractor: Extractor<'_>, field_name: &str, pattern_name: &str) -> bool {
    let Some(extractor) = extractor else {
        return true;
    };
    if !extractor.field().is_empty() && field_name != extractor.field() {
        return false;
    }
    extractor
        .field_pattern_like()
        .is_none_or(|pattern| pattern.is_match(pattern_name.as_bytes()))
}

/// Go `ShowExec.fetchShowVariables`: one `Variable_name | Value` row per
/// variable the extractor admits by its name. GLOBAL lists no SESSION-only variable; neither form
/// lists a noop variable while `tidb_enable_noop_variables` is OFF, a
/// variable SEM hides, or an internal session variable. GLOBAL reads the
/// GLOBAL tier (a variable with no GLOBAL copy reports its default);
/// SESSION reads the session's own values, the `tidb_last_*_info` getters
/// computed from live session state.
pub fn fetch_show_variables(
    ctx: &dyn SessionContext,
    global: bool,
    extractor: Extractor<'_>,
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
        if !extractor_admits(extractor, definition.name, definition.name) {
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
pub fn fetch_show_databases(
    ctx: &dyn SessionContext,
    is: &crate::Catalog,
    extractor: Extractor<'_>,
) -> Vec<String> {
    is.database_names()
        .into_iter()
        .filter(|name| ctx.privilege_manager().db_is_visible(name))
        .filter(|name| {
            let lower = tidb_hack::go_to_lower(name);
            extractor_admits(extractor, &lower, &lower)
        })
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
    extractor: Extractor<'_>,
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
        return Err(crate::DriverError::from(
            tidb_error::plannererrors::ERR_DBACCESS_DENIED.generate_with_stack_by_args(&[
                user.to_owned().into(),
                host.to_owned().into(),
                database.to_owned().into(),
            ]),
        ));
    }
    let Some(names) = is.table_names(database) else {
        return Err(crate::DriverError::from(
            tidb_error::catalog::infoschema::ERR_DATABASE_NOT_EXISTS
                .generate_with_stack_by_args(&[(database.to_owned()).into()]),
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
        .filter(|name| {
            let lower = tidb_hack::go_to_lower(name);
            extractor_admits(extractor, &lower, &lower)
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
/// utf8mb4's default read from the session. Go installs no extractor for
/// it: the plan's selection applies LIKE / WHERE.
pub fn fetch_show_charset(ctx: &dyn SessionContext) -> Vec<Vec<Datum>> {
    let utf8mb4_default = utf8mb4_default_collation(ctx);
    tidb_datatype::get_supported_charsets()
        .into_iter()
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
/// answers it: the one `InnoDB` row. Go installs no extractor for it: the
/// plan's selection applies LIKE / WHERE.
#[must_use]
pub fn fetch_show_engines() -> Vec<Vec<Datum>> {
    vec![vec![
        Datum::Bytes(b"InnoDB".to_vec()),
        Datum::Bytes(b"DEFAULT".to_vec()),
        Datum::Bytes(b"Supports transactions, row-level locking, and foreign keys".to_vec()),
        Datum::Bytes(b"YES".to_vec()),
        Datum::Bytes(b"YES".to_vec()),
        Datum::Bytes(b"YES".to_vec()),
    ]]
}

/// Go `ShowExec.fetchShowCollation`: one `Collation | Charset | Id | Default
/// | Compiled | Sortlen | Pad_attribute` row per collation Go lists and the
/// extractor admits: the lowercased name against its field, the name as is
/// against its pattern.
pub fn fetch_show_collation(ctx: &dyn SessionContext, extractor: Extractor<'_>) -> Vec<Vec<Datum>> {
    let utf8mb4_default = utf8mb4_default_collation(ctx);
    let mut rows = Vec::new();
    for &collation in SHOW_COLLATION_ROWS {
        let name = collation.name();
        if !extractor_admits(extractor, &tidb_hack::go_to_lower(name), name) {
            continue;
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


/// What follows `DEFAULT ` in `SHOW CREATE TABLE`, quoting included.
///
/// Go `pkg/executor/show.go`: the `CURRENT_TIMESTAMP` marker prints bare
/// with the column's fsp appended, while `CURRENT_DATE` and a
/// `DefaultIsExpr` default print parenthesised and unquoted; a literal
/// prints single-quoted.
#[must_use]
pub fn show_create_default_clause(
    default: &tidb_table::column_default::ColumnDefault,
    field_type: &tidb_datatype::FieldType,
    literal_text: &str,
) -> String {
    use tidb_datatype::FieldTypeCode;
    use tidb_table::column_default::{ColumnDefault, ComputedDefaultKind};

    match default {
        // Go's `if col.GetType() == mysql.TypeBit` arm: a BIT column's
        // default is already a `b'...'` literal and is NOT quoted again.
        ColumnDefault::Value(_) if field_type.code() == FieldTypeCode::Bit => {
            literal_text.to_owned()
        }
        // Go quotes the stored text through `format.OutputFormat`, which
        // doubles quotes and backslashes and escapes NUL/CR/LF -- without
        // it a default holding any of them prints a body that does not
        // re-parse.
        ColumnDefault::Value(_) => {
            format!("'{}'", tidb_format::output_format(literal_text))
        }
        ColumnDefault::Computed(computed)
            if computed.kind == ComputedDefaultKind::CurrentTimestamp =>
        {
            let fsp = field_type.decimal();
            if fsp > 0 {
                format!("{}({fsp})", computed.text)
            } else {
                computed.text.clone()
            }
        }
        ColumnDefault::Computed(computed) => format!("({})", computed.text),
    }
}

#[cfg(test)]
mod show_create_default_tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_table::column_default::ColumnDefault;

    #[test]
    fn literal_show_create_clause_uses_util_output_format() {
        let default = ColumnDefault::Value(Datum::new_string("unused"));
        let field_type = FieldType::new(FieldTypeCode::Varchar);
        let clause = show_create_default_clause(&default, &field_type, "slash\\quote'\0nul\nline\rcarriage");

        assert_eq!(clause, "'slash\\\\quote''\\0nul\\nline\\rcarriage'");
    }
}

/// Go `ShowExec.fetchShowMasterStatus`: one row naming TiDB's pseudo binlog
/// file and the CURRENT transaction's start timestamp as the position, with
/// the three replication columns empty. Tools that call this to fence a dump
/// read the position.
pub fn master_status_rows(position: i64) -> Vec<Vec<Datum>> {
    vec![vec![
        Datum::Bytes(b"tidb-binlog".to_vec()),
        Datum::Int(position),
        Datum::Bytes(Vec::new()),
        Datum::Bytes(Vec::new()),
        Datum::Bytes(Vec::new()),
    ]]
}

/// Go `ShowExec.fetchShowPrivileges`: the static MySQL privilege table
/// (`executor/show.go:2037` onward) followed by every registered dynamic
/// privilege as `("<NAME>", "Server Admin", "")` — `privileges.GetDynamicPrivileges()`
/// over `pkg/privilege/privileges/privileges.go:60`'s list, in stored order.
/// Go `ShowExec.fetchShowPrivileges` (`pkg/executor/show.go:2036`): the
/// static table, then every dynamic privilege.
pub fn show_privileges_rows() -> Vec<Vec<Datum>> {
    const STATIC_PRIVILEGES: &[(&str, &str, &str)] = &[
        ("Alter", "Tables", "To alter the table"),
        ("Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"),
        ("Config", "Server Admin", "To use SHOW CONFIG and SET CONFIG statements"),
        ("Create", "Databases,Tables,Indexes", "To create new databases and tables"),
        ("Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"),
        ("Create role", "Server Admin", "To create new roles"),
        ("Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"),
        ("Create view", "Tables", "To create new views"),
        ("Create user", "Server Admin", "To create new users"),
        ("Delete", "Tables", "To delete existing rows"),
        ("Drop", "Databases,Tables", "To drop databases, tables, and views"),
        ("Drop role", "Server Admin", "To drop roles"),
        ("Event", "Server Admin", "To create, alter, drop and execute events"),
        ("Execute", "Functions,Procedures", "To execute stored routines"),
        ("File", "File access on server", "To read and write files on the server"),
        ("Grant option", "Databases,Tables,Functions,Procedures", "To give to other users those privileges you possess"),
        ("Index", "Tables", "To create or drop indexes"),
        ("Insert", "Tables", "To insert data into tables"),
        ("Lock tables", "Databases", "To use LOCK TABLES (together with SELECT privilege)"),
        ("Process", "Server Admin", "To view the plain text of currently executing queries"),
        ("Proxy", "Server Admin", "To make proxy user possible"),
        ("References", "Databases,Tables", "To have references on tables"),
        ("Reload", "Server Admin", "To reload or refresh tables, logs and privileges"),
        ("Replication client", "Server Admin", "To ask where the slave or master servers are"),
        ("Replication slave", "Server Admin", "To read binary log events from the master"),
        ("Select", "Tables", "To retrieve rows from table"),
        ("Show databases", "Server Admin", "To see all databases with SHOW DATABASES"),
        ("Show view", "Tables", "To see views with SHOW CREATE VIEW"),
        ("Shutdown", "Server Admin", "To shut down the server"),
        ("Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."),
        ("Trigger", "Tables", "To use triggers"),
        ("Create tablespace", "Server Admin", "To create/alter/drop tablespaces"),
        ("Update", "Tables", "To update existing rows"),
        ("Usage", "Server Admin", "No privileges - allow connect only"),
    ];
    const DYNAMIC_PRIVILEGES: &[&str] = &[
        "BACKUP_ADMIN",
        "RESTORE_ADMIN",
        "SYSTEM_USER",
        "SYSTEM_VARIABLES_ADMIN",
        "ROLE_ADMIN",
        "CONNECTION_ADMIN",
        "PLACEMENT_ADMIN",
        "DASHBOARD_CLIENT",
        "RESTRICTED_TABLES_ADMIN",
        "RESTRICTED_STATUS_ADMIN",
        "RESTRICTED_VARIABLES_ADMIN",
        "RESTRICTED_USER_ADMIN",
        "RESTRICTED_CONNECTION_ADMIN",
        "RESTRICTED_REPLICA_WRITER_ADMIN",
        "RESTRICTED_PRIV_ADMIN",
        "RESTRICTED_SQL_ADMIN",
        "RESOURCE_GROUP_ADMIN",
        "RESOURCE_GROUP_USER",
        "TRAFFIC_CAPTURE_ADMIN",
        "TRAFFIC_REPLAY_ADMIN",
        "APPLICATION_PASSWORD_ADMIN",
    ];
    let mut rows: Vec<Vec<Datum>> = STATIC_PRIVILEGES
        .iter()
        .map(|(privilege, context, comment)| {
            vec![
                Datum::Bytes(privilege.as_bytes().to_vec()),
                Datum::Bytes(context.as_bytes().to_vec()),
                Datum::Bytes(comment.as_bytes().to_vec()),
            ]
        })
        .collect();
    rows.extend(DYNAMIC_PRIVILEGES.iter().map(|privilege| {
        vec![
            Datum::Bytes(privilege.as_bytes().to_vec()),
            Datum::Bytes(b"Server Admin".to_vec()),
            Datum::Bytes(Vec::new()),
        ]
    }));
    rows
}

/// The SHOW kinds Go's `buildShow` plans here (the session's `run_planned_show`);
/// the rest still answer in the session's SHOW arms.
pub fn is_planned_show(tp: tidb_ast::ShowStmtType) -> bool {
    use tidb_ast::ShowStmtType as T;
    matches!(
        tp,
        T::Charset
            | T::Collation
            | T::Databases
            | T::Engines
            | T::ProcedureStatus
            | T::FunctionStatus
            | T::Triggers
            | T::Events
            | T::Profiles
            | T::OpenTables
            | T::Plugins
            | T::Backups
            | T::Restores
            | T::Tables
            | T::Variables
            | T::MasterStatus
            | T::BinlogStatus
            | T::Privileges
            | T::Builtins
            | T::Warnings
            | T::Errors
            | T::Status
            | T::TableStatus
            | T::Columns
            | T::Index
            | T::CreateTable
            | T::CreateView
            | T::CreateSequence
            | T::CreateDatabase
            | T::CreatePlacementPolicy
            | T::CreateResourceGroup
            | T::Grants
            | T::CreateUser
            | T::StatsExtended
            | T::StatsMeta
            | T::StatsHealthy
            | T::ColumnStatsUsage
            | T::HistogramsInFlight
            | T::AnalyzeStatus
            | T::StatsTopN
            | T::StatsBuckets
            | T::StatsHistograms
            | T::StatsLocked
            | T::Bindings
            | T::BindingCacheStatus
            | T::ProcessList
            | T::SessionStates
            | T::Config
            | T::Affinity
            | T::ReplicaStatus
            | T::Placement
            | T::PlacementLabels
            | T::PlacementForDatabase
            | T::PlacementForTable
            | T::PlacementForPartition
            | T::Regions
            | T::Distributions
            | T::ImportJobs
            | T::ImportGroups
            | T::TableNextRowId
    )
}
