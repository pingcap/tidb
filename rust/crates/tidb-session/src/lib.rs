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

//! The session: the single entry point that owns catalog state and runs SQL
//! statements through the wired parse -> plan -> execute pipeline.
//!
//! This is the seam of Go's `pkg/session` `session.ExecuteStmt`: one object a
//! client holds, dispatching each statement kind to its executor path.
//!
//! SEED SCOPE: [`Session::run`] dispatches `SELECT` (rows), `INSERT` (affected
//! count), and `CREATE TABLE` over the session's [`Catalog`]. DEFERRED
//! (documented): transactions (autocommit is implicit and immediate --
//! `BEGIN`/`COMMIT`/`ROLLBACK` land with the txnkv integration), session
//! variables (`SET`), prepared statements, the MySQL wire protocol, privileges,
//! and every other statement kind. Statements are currently parsed twice (once
//! here for dispatch, once in the driver's runner) -- a wiring simplification
//! to remove when the driver's runners take parsed statements.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};

use tidb_ast::{DdlStmt, DmlStmt, SessionStmt, Stmt};
use tidb_datatype::{Datum, FieldType};
use tidb_executor::{Catalog, DriverError, MysqlRng};
use tidb_executor::{SchemaErrorKind, DEFAULT_DATABASE};

/// The result of running one statement.
#[derive(Debug, PartialEq)]
pub enum StmtResult {
    /// A query's result rows.
    Rows(Vec<Vec<Datum>>),
    /// A DML statement's affected-row count.
    Affected(u64),
    /// A DDL statement completed (`false` = `IF NOT EXISTS` no-op).
    Done(bool),
}

/// The result of running one statement, with wire-facing column metadata.
///
/// [`StmtResult::Rows`] loses column names/types; a server front end needs one
/// `(name, type)` per result column to build protocol column definitions, so
/// [`Session::run_with_columns`] returns this richer shape instead.
#[derive(Debug, PartialEq)]
pub enum StmtOutput {
    /// A query's result columns and rows.
    Rows {
        /// One `(display name, field type)` per output column.
        columns: Vec<(String, FieldType)>,
        /// The result rows (one `Datum` per column).
        rows: Vec<Vec<Datum>>,
    },
    /// A DML statement's affected-row count.
    Affected(u64),
    /// A DDL statement completed (`false` = `IF NOT EXISTS` no-op).
    Done(bool),
}

/// What kind of answer a statement produces, decided by parsing alone.
///
/// The MySQL text protocol answers a query with a result set and a write or
/// DDL with an OK packet, so a server front end must know which shape a
/// statement takes *before* running it (running it twice would duplicate the
/// write).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtKind {
    /// A query: answers with rows.
    Query,
    /// A DML write or DDL: answers with an affected-row count.
    Write,
}

/// A process-wide catalog shared by every session, as Go's domain-owned
/// `infoschema` is shared by every session of a TiDB instance.
pub type SharedCatalog = Arc<Mutex<Catalog>>;

/// A session: runs statements against a catalog shared with its peers.
///
/// Go sessions borrow the process's schema state rather than owning private
/// copies, so a table one connection creates is visible to the others. This
/// mirrors that with a shared, mutex-guarded catalog; the statement-level lock
/// stands in for Go's schema-version/lease machinery, which is a separate
/// tier (documented deferral).
pub struct Session {
    catalog: SharedCatalog,
    /// The open transaction, if any.
    txn: Option<Transaction>,
    /// The session's system and user variables.
    vars: SessionVars,
    /// The warnings the last statement produced, which Go keeps in
    /// `StmtCtx.warnings` and `SHOW WARNINGS` reads.
    warnings: Vec<SqlWarning>,
    /// Go `SessionVars.User` in its two spellings: the matched grant
    /// identity `CURRENT_USER()` reports and the login identity `USER()`
    /// reports. Empty until a front end authenticates one.
    current_user: Option<String>,
    login_user: Option<String>,
    /// Go `SessionVars.ConnectionID`, which `CONNECTION_ID()` reports.
    /// `None` for a session with no connection identity, where the builtin
    /// answers NULL like `CURRENT_USER()` does for an unauthenticated one.
    connection_id: Option<u64>,
    /// Go `SessionVars.PrevLastInsertID`: the id `LAST_INSERT_ID()` reports,
    /// which only a statement that ALLOCATED an auto value updates.
    last_insert_id: u64,
    /// The id the last statement allocated, which the OK packet carries and
    /// which is 0 for a statement that allocated nothing.
    statement_insert_id: u64,
    /// Go `SessionVars.CurrentDB`: the schema an unqualified name resolves in.
    /// Empty means no database is selected, which is Go's `ErrNoDB` case.
    current_db: String,
    /// This connection's registration in the server's process list, which the
    /// front end installs. `None` for a session with no server front; such a
    /// session still answers `SHOW PROCESSLIST` -- with the single row it can
    /// honestly report, itself.
    process: Option<process::ProcessGuard>,
    /// Whether this session holds the `PROCESS` privilege, which decides
    /// what `SHOW PROCESSLIST` and `information_schema.PROCESSLIST` let it
    /// see (Go `hasPriv(ctx, mysql.ProcessPriv)`).
    ///
    /// STUBBED: `GRANT`/`REVOKE` are not implemented yet (see
    /// `tidb_exec::admin_runtime::AdminStmt::Grant`), so there is no SQL path
    /// that sets this bit -- only [`Session::set_process_privilege`] does,
    /// which a front end or test calls directly. This is the minimal
    /// per-session privilege state needed to make the visibility rule
    /// testable ahead of a real grant table.
    has_process_priv: bool,
    /// The server's account/global-privilege registry, shared by every
    /// session a front end opens (see [`privilege::PrivilegeRegistry`]).
    /// `None` for a session with no front end (unit tests, internal use),
    /// which is why every check through it falls back to the pre-existing
    /// bit above rather than treating an absent registry as "no privilege".
    privileges: Option<privilege::PrivilegeRegistry>,
    /// Go `SessionVars.Rng`: the generator unseeded `RAND()` advances, shared
    /// across every statement of this session (unlike constant `RAND(N)`,
    /// which owns a fresh per-statement generator -- see `StmtContext`).
    rand: Rc<RefCell<MysqlRng>>,
}

impl Default for Session {
    /// A session on its own empty catalog, with `test` selected as a fresh
    /// TiDB connection has.
    fn default() -> Self {
        Session {
            catalog: SharedCatalog::default(),
            txn: None,
            vars: SessionVars::new(),
            warnings: Vec::new(),
            current_user: None,
            login_user: None,
            connection_id: None,
            last_insert_id: 0,
            statement_insert_id: 0,
            current_db: DEFAULT_DATABASE.to_owned(),
            process: None,
            has_process_priv: false,
            privileges: None,
            rand: new_time_seeded_rand(),
        }
    }
}

/// Go `mathutil.NewWithTime()`: seeds a session's unseeded-`RAND()` generator
/// from the wall clock, which is what makes two sessions' `RAND()` sequences
/// differ without either being told to.
fn new_time_seeded_rand() -> Rc<RefCell<MysqlRng>> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    Rc::new(RefCell::new(MysqlRng::new_with_seed(nanos)))
}

/// An open transaction's state.
///
/// Go stages a transaction's writes in a `kv.MemBuffer` over a read snapshot
/// and flushes them at commit; this stages them in a private copy of the
/// catalog taken at `BEGIN`, so the session reads its own writes while its
/// peers see nothing until commit.
///
/// `base_version` is the shared catalog's mutation counter at `BEGIN`. If it
/// moved by commit time, someone else wrote, and the commit is refused rather
/// than overwriting their work -- the outcome Go gets from TiKV's optimistic
/// conflict check, though Go compares the WRITTEN KEYS while this compares the
/// whole catalog, so this refuses some commits Go would allow (documented).
struct Transaction {
    working: Catalog,
    base_version: u64,
}

pub use tidb_executor::TxnErrorKind;

pub mod infoschema;
pub mod privilege;
pub mod process;
pub mod sysvar;
pub mod vars;
pub use vars::{SessionVars, VarError};

/// Maps a variable error onto the driver error the wire layer renders.
fn var_error(error: VarError) -> DriverError {
    DriverError::Var(match error {
        VarError::UnknownSystemVariable(name) => {
            tidb_executor::VarErrorKind::UnknownSystemVariable(name)
        }
        VarError::ReadOnlyVariable(name) => tidb_executor::VarErrorKind::ReadOnlyVariable(name),
        VarError::WrongTypeForVar(name) => tidb_executor::VarErrorKind::WrongTypeForVar(name),
        VarError::WrongValueForVar(name, value) => {
            tidb_executor::VarErrorKind::WrongValueForVar(name, value)
        }
    })
}

/// Go `mysql.DefaultCharset` and the collation `getDefaultCollate` returns for
/// it, which is what a table with no explicit charset reports.
const TABLE_CHARSET: &str = "utf8mb4";
const TABLE_COLLATE: &str = "utf8mb4_bin";

/// Go `stringutil.Escape` with a non-ANSI_QUOTES sql_mode: backtick-quoted,
/// with an embedded backtick doubled.
fn escape_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Go's `TABLE_TYPE` / `Table_type` value for an object.
fn table_type_of(is_view: bool) -> &'static str {
    if is_view {
        "VIEW"
    } else {
        "BASE TABLE"
    }
}

/// Go `ConstructResultOfShowCreateView`.
///
/// Go always prints the full preamble, including the defaults the statement
/// never wrote, and always prints an explicit column list even when the
/// `CREATE VIEW` had none -- the names come from the stored definition.
///
/// DIVERGENCE (documented): the definer is whatever the statement recorded,
/// which in this tier is the empty identity, printed as ``@``. A TiDB with
/// authentication prints the connected user there.
fn show_create_view_text(view: &tidb_executor::ViewDef) -> String {
    let mut out = format!(
        "CREATE ALGORITHM={} DEFINER={}@{} SQL SECURITY {} VIEW {} (",
        view.algorithm,
        escape_name(&view.definer_user),
        escape_name(&view.definer_host),
        view.security,
        escape_name(&view.name),
    );
    for (index, (name, _)) in view.columns.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        out.push_str(&escape_name(name));
    }
    out.push_str(") AS ");
    out.push_str(&view.select_sql);
    out
}

/// Go `constructResultOfShowCreateTable`, over the metadata this seed keeps.
///
/// The shape is Go's line for line: the header, two-space-indented column
/// clauses separated by ",\n", the clustered primary key when the handle is
/// one, then the indexes, then the closing paren with the engine and charset.
///
/// NOT MODELLED (documented, and each one rejected at DDL time so no table can
/// carry it): generated columns, AUTO_INCREMENT, AUTO_RANDOM, ON UPDATE
/// CURRENT_TIMESTAMP, column and index comments, foreign keys, check
/// constraints, partitioning, temporary tables, views and sequences.
/// Per-column CHARACTER SET / COLLATE clauses are omitted because this seed
/// stores no per-column charset, so every column takes the table's.
fn show_create_table_text(name: &str, table: &tidb_executor::KvTable) -> String {
    let mut out = format!("CREATE TABLE {} (\n", escape_name(name));
    let mut clauses: Vec<String> = Vec::with_capacity(table.columns.len() + 1);

    for (offset, column) in table.columns.iter().enumerate() {
        let mut clause = format!(
            "  {} {}",
            escape_name(&column.name),
            column.field_type.compact_str(false)
        );
        let not_null = column.field_type.flags() & NOT_NULL_FLAG != 0;
        if table.auto_increment_offset() == Some(offset) {
            // Go writes the pair together for an auto column and prints no
            // default for it.
            clause.push_str(" NOT NULL AUTO_INCREMENT");
            clauses.push(clause);
            continue;
        }
        if not_null {
            clause.push_str(" NOT NULL");
        }
        // Go prints nothing for a column carrying NoDefaultValueFlag, which is
        // a NOT NULL column with no DEFAULT clause; a nullable column with no
        // DEFAULT reports DEFAULT NULL, as MySQL does.
        match &column.default_value {
            Some(Datum::Null) => clause.push_str(" DEFAULT NULL"),
            Some(value) => {
                // Go quotes every non-bit default, integers included.
                let text = datum_text(value).unwrap_or_default();
                clause.push_str(&format!(" DEFAULT '{text}'"));
            }
            None if !not_null => clause.push_str(" DEFAULT NULL"),
            None => {}
        }
        clauses.push(clause);
    }

    // Go emits a clustered primary key here, because a clustered key -- an
    // int handle or a common handle -- is not in the index list.
    let clustered: Vec<usize> = match table.pk_handle_offset() {
        Some(offset) => vec![offset],
        None => table.common_handle_offsets().to_vec(),
    };
    if !clustered.is_empty() {
        let columns = clustered
            .iter()
            .map(|offset| escape_name(&table.columns[*offset].name))
            .collect::<Vec<_>>()
            .join(",");
        clauses.push(format!(
            "  PRIMARY KEY ({columns}) /*T![clustered_index] CLUSTERED */"
        ));
    }

    for index in table.indexes() {
        let columns = index
            .column_offsets
            .iter()
            .map(|offset| escape_name(&table.columns[*offset].name))
            .collect::<Vec<_>>()
            .join(",");
        if index.name.eq_ignore_ascii_case("PRIMARY") {
            // A primary key that is not the handle is non-clustered here,
            // since this seed builds no clustered common handle.
            clauses.push(format!(
                "  PRIMARY KEY ({columns}) /*T![clustered_index] NONCLUSTERED */"
            ));
        } else if index.unique {
            clauses.push(format!(
                "  UNIQUE KEY {} ({columns})",
                escape_name(&index.name)
            ));
        } else {
            clauses.push(format!("  KEY {} ({columns})", escape_name(&index.name)));
        }
    }

    out.push_str(&clauses.join(",\n"));
    out.push_str(&format!(
        "\n) ENGINE=InnoDB DEFAULT CHARSET={TABLE_CHARSET} COLLATE={TABLE_COLLATE}"
    ));
    out
}

/// Go `table.ColDescFieldNames(false)`: the columns `SHOW COLUMNS` and
/// `DESCRIBE` produce.
const COL_DESC_FIELD_NAMES: &[&str] = &["Field", "Type", "Null", "Key", "Default", "Extra"];

/// Go `table.ColDescFieldNames(true)`: the extra columns `SHOW FULL COLUMNS`
/// inserts between `Type` and `Null`, plus the trailing `Privileges` and
/// `Comment` columns.
const FULL_COL_DESC_FIELD_NAMES: &[&str] = &[
    "Field",
    "Type",
    "Collation",
    "Null",
    "Key",
    "Default",
    "Extra",
    "Privileges",
    "Comment",
];

/// Go's mock session's fixed grant string for every column of every table
/// (`fetchShowColumns`): this tier grants no per-column privileges of its
/// own, so it reports the same static capture MySQL/TiDB print for a column
/// the current user can select, insert, update, and reference.
const FULL_COL_DESC_PRIVILEGES: &str = "select,insert,update,references";

/// Go `table.NewColDesc`, restricted to the facts this seed's metadata holds.
///
/// `Null` is NO when the column carries `NotNullFlag`; `Key` is PRI for a
/// primary-key column, UNI for a column that is the whole of a unique index,
/// and MUL for one that leads a non-unique index -- Go reads those from the
/// column's key flags, which the DDL sets from the same index definitions.
///
/// `Default` is the column's stored `DEFAULT`, or NULL when none was written.
///
/// `Extra` reports `auto_increment` for the auto column.
///
/// NOT MODELLED (documented): the other `Extra` values -- ON UPDATE
/// CURRENT_TIMESTAMP and the generated-column markers -- because those column
/// kinds are rejected at DDL time, so no column can carry them.
fn column_description(
    column: &tidb_executor::KvColumn,
    offset: usize,
    table: &tidb_executor::KvTable,
    full: bool,
) -> Vec<Datum> {
    let null_flag = if column.field_type.flags() & NOT_NULL_FLAG != 0 {
        "NO"
    } else {
        "YES"
    };
    // Go `NewColDesc`: an auto-increment column reports auto_increment.
    let extra = if table.auto_increment_offset() == Some(offset) {
        "auto_increment"
    } else {
        ""
    };
    let key_flag = column_key_flag(table, offset);
    let default = match &column.default_value {
        Some(value) => match datum_text(value) {
            Some(text) => Datum::Bytes(text.into_bytes()),
            None => Datum::Null,
        },
        None => Datum::Null,
    };
    if !full {
        return vec![
            Datum::Bytes(column.name.clone().into_bytes()),
            Datum::Bytes(column.field_type.compact_str(false).into_bytes()),
            Datum::Bytes(null_flag.as_bytes().to_vec()),
            Datum::Bytes(key_flag.into_bytes()),
            default,
            Datum::Bytes(extra.as_bytes().to_vec()),
        ];
    }
    // Go `NewColDesc`: `Collation` is NULL for a non-string type (numerics,
    // temporals, ...), and the column's own collation name otherwise.
    //
    // NOT MODELLED (documented): a per-column charset/collation override.
    // This tier's DDL does not track one, so every string column reports the
    // schema default (`utf8mb4_bin`), which is what a plain `VARCHAR` column
    // with no explicit `CHARACTER SET`/`COLLATE` actually gets in Go too.
    let collation = if column.field_type.is_string() {
        Datum::Bytes(tidb_datatype::Collation::DEFAULT.name().as_bytes().to_vec())
    } else {
        Datum::Null
    };
    vec![
        Datum::Bytes(column.name.clone().into_bytes()),
        Datum::Bytes(column.field_type.compact_str(false).into_bytes()),
        collation,
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(key_flag.into_bytes()),
        default,
        Datum::Bytes(extra.as_bytes().to_vec()),
        Datum::Bytes(FULL_COL_DESC_PRIVILEGES.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()), // Comment: no per-column comments modelled.
    ]
}

/// A view column's `SHOW COLUMNS` row.
///
/// A view carries no storage metadata, so Go reports no key, no default and
/// no extra for every one of its columns; only the name, the type the body
/// produced, and nullability come from the definition. The body's columns are
/// nullable here because nothing propagates a base column's NOT NULL through
/// the view's stored types, which is what Go reports for these views too.
fn view_column_description(
    name: &str,
    field_type: &tidb_datatype::FieldType,
    full: bool,
) -> Vec<Datum> {
    let null_flag = if field_type.flags() & NOT_NULL_FLAG != 0 {
        "NO"
    } else {
        "YES"
    };
    if !full {
        return vec![
            Datum::Bytes(name.as_bytes().to_vec()),
            Datum::Bytes(field_type.compact_str(false).into_bytes()),
            Datum::Bytes(null_flag.as_bytes().to_vec()),
            Datum::Bytes(Vec::new()),
            Datum::Null,
            Datum::Bytes(Vec::new()),
        ];
    }
    let collation = if field_type.is_string() {
        Datum::Bytes(tidb_datatype::Collation::DEFAULT.name().as_bytes().to_vec())
    } else {
        Datum::Null
    };
    vec![
        Datum::Bytes(name.as_bytes().to_vec()),
        Datum::Bytes(field_type.compact_str(false).into_bytes()),
        collation,
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()),
        Datum::Null,
        Datum::Bytes(Vec::new()),
        Datum::Bytes(FULL_COL_DESC_PRIVILEGES.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()),
    ]
}

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;

/// Go `NewColDesc`'s key flag, shared by `SHOW COLUMNS` and
/// `information_schema.COLUMNS`: PRI for a primary key, UNI for a column that
/// is the whole of a unique index, MUL for one that leads a non-unique index.
pub(crate) fn column_key_flag(table: &tidb_executor::KvTable, offset: usize) -> String {
    let is_handle =
        table.pk_handle_offset() == Some(offset) || table.common_handle_offsets().contains(&offset);
    if is_handle
        || table.indexes().iter().any(|index| {
            index.name.eq_ignore_ascii_case("PRIMARY") && index.column_offsets == [offset]
        })
    {
        "PRI".to_owned()
    } else if table
        .indexes()
        .iter()
        .any(|index| index.unique && index.column_offsets == [offset])
    {
        "UNI".to_owned()
    } else if table
        .indexes()
        .iter()
        .any(|index| index.column_offsets.first() == Some(&offset))
    {
        "MUL".to_owned()
    } else {
        String::new()
    }
}

/// A one-column result set of strings, the shape SHOW DATABASES and SHOW
/// TABLES produce.
fn string_column_output(column: &str, values: Vec<String>) -> StmtOutput {
    let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
    StmtOutput::Rows {
        columns: vec![(column.to_owned(), field_type)],
        rows: values
            .into_iter()
            .map(|value| vec![Datum::Bytes(value.into_bytes())])
            .collect(),
    }
}

/// The text form a system variable stores for a datum (Go keeps every system
/// variable as a string).
fn datum_text(value: &Datum) -> Option<String> {
    match value {
        Datum::Null => None,
        Datum::Int(v) => Some(v.to_string()),
        Datum::UInt(v) => Some(v.to_string()),
        Datum::Real(v) => Some(v.to_string()),
        Datum::Decimal(d) => Some(d.to_string()),
        Datum::String(s) => Some(String::from_utf8_lossy(s.bytes()).into_owned()),
        Datum::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        // `BinaryJSON.String`: the canonical document text a JSON column
        // sends on the wire.
        Datum::Json(j) => Some(j.to_string()),
        _ => None,
    }
}

impl Session {
    /// A fresh session with its own empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Session::default()
    }

    /// Go `SessionVars.CurrentDB`. Empty when no database is selected.
    #[must_use]
    pub fn current_database(&self) -> &str {
        &self.current_db
    }

    /// Applies `USE`, `CREATE DATABASE`, `DROP DATABASE`, `SHOW DATABASES`
    /// and `SHOW TABLES`.
    ///
    /// Returns `Some(output)` for those statements and `None` for anything
    /// else, so a caller can dispatch without re-parsing.
    pub fn apply_schema_statement(&mut self, sql: &str) -> Result<Option<StmtOutput>, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        match &stmt {
            Stmt::Session(session_stmt) => match &**session_stmt {
                SessionStmt::Use(name) => {
                    self.use_database(name)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                _ => Ok(None),
            },
            Stmt::Ddl(ddl) => match &**ddl {
                tidb_ast::DdlStmt::CreateDatabase {
                    if_not_exists,
                    name,
                    options,
                } => {
                    if !options.is_empty() {
                        return Err(DriverError::Unsupported(
                            "database charset and collation options are not supported yet",
                        ));
                    }
                    let created =
                        self.with_catalog_mut(|catalog| Ok(catalog.create_database(name)))?;
                    // Go raises ErrDBCreateExists unless IF NOT EXISTS.
                    if !created && !*if_not_exists {
                        return Err(DriverError::Schema(SchemaErrorKind::DatabaseExists(
                            name.clone(),
                        )));
                    }
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::DdlStmt::DropDatabase { if_exists, name } => {
                    let dropped =
                        self.with_catalog_mut(|catalog| Ok(catalog.drop_database(name)))?;
                    // Go raises ErrDBDropExists unless IF EXISTS.
                    if !dropped && !*if_exists {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                            name.clone(),
                        )));
                    }
                    // Dropping the current database leaves the session with
                    // none selected, which is Go's ErrNoDB state for the next
                    // unqualified statement.
                    if dropped && self.current_db.eq_ignore_ascii_case(name) {
                        self.current_db.clear();
                    }
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::DdlStmt::CreateUser {
                    if_not_exists,
                    users,
                    tls_options,
                    resource_options,
                    password_options,
                    comment_or_attribute,
                    resource_group,
                } => Ok(Some(self.create_user_stmt(
                    *if_not_exists,
                    users,
                    tls_options,
                    resource_options,
                    password_options,
                    comment_or_attribute,
                    resource_group,
                )?)),
                tidb_ast::DdlStmt::DropUser {
                    is_role: false,
                    if_exists,
                    users,
                } => Ok(Some(self.drop_user_stmt(*if_exists, users)?)),
                _ => Ok(None),
            },
            Stmt::Admin(admin) => match &**admin {
                // `EXPLAIN <select>`: plan the statement and report the plan,
                // running nothing. Go's EXPLAIN plans without executing (an
                // `EXPLAIN INSERT` inserts no row, captured), and so does
                // this: `tidb_executor::explain_select_stmt` re-runs the
                // driver's own read-path decisions without touching storage.
                //
                // See `tidb_executor::explain`'s module doc for every place
                // this tier's plan text diverges from Go's and why.
                tidb_ast::AdminStmt::Explain(explain) => {
                    // Go's preprocessor rejects an unrecognized format name
                    // with this exact message before the statement is even
                    // planned (captured: `explain format = 'bogus' ...` ->
                    // `Unknown EXPLAIN format name: 'bogus'`).
                    let Some(format) = tidb_executor::ExplainFormat::parse(&explain.format) else {
                        return Err(DriverError::Unsupported("unknown EXPLAIN format name"));
                    };
                    let Some(target) = explain.statement() else {
                        return Err(DriverError::Unsupported(
                            "EXPLAIN of a plan digest is not supported yet",
                        ));
                    };
                    let current_db = self.current_db.clone();
                    if explain.analyze {
                        // Real `EXPLAIN ANALYZE` EXECUTES the wrapped
                        // statement to gather its runtime counters
                        // (confirmed by capture), so this tier does too --
                        // see `tidb_executor::explain_analyze_select_stmt`/
                        // `explain_analyze_insert_stmt`'s own docs for which
                        // operators get a real `actRows` and which print the
                        // honest `N/A` placeholder this tier uses for every
                        // counter (timing, memory, disk) it does not
                        // collect at all.
                        let ctx = self.statement_context(true);
                        let (columns, rows) = match target {
                            Stmt::Query(query) => {
                                let tidb_ast::QueryStmt::Select(select) = &**query else {
                                    return Err(DriverError::Unsupported(
                                        "EXPLAIN ANALYZE of a set operation is not supported yet",
                                    ));
                                };
                                self.with_catalog_mut(|catalog| {
                                    tidb_executor::explain_analyze_select_stmt(
                                        select,
                                        catalog,
                                        &current_db,
                                        &ctx,
                                        format,
                                    )
                                })?
                            }
                            Stmt::Dml(dml) => match &**dml {
                                tidb_ast::DmlStmt::Insert(insert) => {
                                    self.with_catalog_mut(|catalog| {
                                        tidb_executor::explain_analyze_insert_stmt(
                                            insert,
                                            catalog,
                                            &current_db,
                                            &ctx,
                                            format,
                                        )
                                    })?
                                }
                                tidb_ast::DmlStmt::Update(update) => {
                                    self.with_catalog_mut(|catalog| {
                                        tidb_executor::explain_analyze_update_stmt(
                                            update,
                                            catalog,
                                            &current_db,
                                            &ctx,
                                            format,
                                        )
                                    })?
                                }
                                tidb_ast::DmlStmt::Delete(delete) => {
                                    self.with_catalog_mut(|catalog| {
                                        tidb_executor::explain_analyze_delete_stmt(
                                            delete,
                                            catalog,
                                            &current_db,
                                            &ctx,
                                            format,
                                        )
                                    })?
                                }
                                _ => {
                                    return Err(DriverError::Unsupported(
                                        "only EXPLAIN ANALYZE of a SELECT, INSERT, UPDATE, or \
                                         DELETE is supported yet",
                                    ));
                                }
                            },
                            _ => {
                                return Err(DriverError::Unsupported(
                                    "only EXPLAIN ANALYZE of a SELECT, INSERT, UPDATE, or DELETE \
                                     is supported yet",
                                ));
                            }
                        };
                        self.drain_eval_warnings(&ctx);
                        return Ok(Some(StmtOutput::Rows { columns, rows }));
                    }
                    let (columns, rows) = match target {
                        Stmt::Query(query) => {
                            let tidb_ast::QueryStmt::Select(select) = &**query else {
                                return Err(DriverError::Unsupported(
                                    "EXPLAIN of a set operation is not supported yet",
                                ));
                            };
                            self.with_catalog_mut(|catalog| {
                                tidb_executor::explain_select_stmt(
                                    select,
                                    catalog,
                                    &current_db,
                                    format,
                                )
                            })?
                        }
                        // A write's plan is the same plan recorder run over
                        // the read path the driver's write executes; nothing
                        // is executed -- no row is read or written -- which
                        // is also what Go does (`EXPLAIN INSERT` inserts no
                        // row, captured).
                        Stmt::Dml(dml) => match &**dml {
                            tidb_ast::DmlStmt::Insert(insert) => {
                                self.with_catalog_mut(|catalog| {
                                    tidb_executor::explain_insert_stmt(
                                        insert,
                                        catalog,
                                        &current_db,
                                        format,
                                    )
                                })?
                            }
                            tidb_ast::DmlStmt::Update(update) => {
                                self.with_catalog_mut(|catalog| {
                                    tidb_executor::explain_update_stmt(
                                        update,
                                        catalog,
                                        &current_db,
                                        format,
                                    )
                                })?
                            }
                            tidb_ast::DmlStmt::Delete(delete) => {
                                self.with_catalog_mut(|catalog| {
                                    tidb_executor::explain_delete_stmt(
                                        delete,
                                        catalog,
                                        &current_db,
                                        format,
                                    )
                                })?
                            }
                            _ => {
                                return Err(DriverError::Unsupported(
                                    "only EXPLAIN of INSERT, UPDATE, or DELETE is supported yet",
                                ));
                            }
                        },
                        _ => {
                            return Err(DriverError::Unsupported(
                                "only EXPLAIN of a SELECT, INSERT, UPDATE, or DELETE is supported yet",
                            ));
                        }
                    };
                    Ok(Some(StmtOutput::Rows { columns, rows }))
                }
                tidb_ast::AdminStmt::Grant(grant) => Ok(Some(self.grant_stmt(grant)?)),
                tidb_ast::AdminStmt::Revoke(revoke) => Ok(Some(self.revoke_stmt(revoke)?)),
                tidb_ast::AdminStmt::ShowGrants(show) => Ok(Some(self.show_grants_stmt(show)?)),
                tidb_ast::AdminStmt::ShowDatabases(show) => {
                    if show.filter.is_some() {
                        return Err(DriverError::Unsupported(
                            "SHOW DATABASES filters are not supported yet",
                        ));
                    }
                    let names = self.with_catalog_mut(|catalog| Ok(catalog.database_names()))?;
                    Ok(Some(string_column_output("Database", names)))
                }
                // Go `fetchShowTableStatus`: one row per table in the
                // schema, with the columns MySQL's own SHOW TABLE STATUS
                // reports.
                //
                // NOT MODELLED, and each reported the way Go reports an
                // absent value rather than invented: every size and count
                // (Rows, Data_length, Index_length and friends) is 0, which
                // is also what TiDB itself answers without a statistics tier;
                // Create_time is NULL because this tier stores no per-table
                // creation timestamp; Update_time, Check_time and Checksum
                // are NULL or empty for the same reason.
                tidb_ast::AdminStmt::ShowTableStatus(show) => {
                    let database = match &show.database {
                        Some(database) => database.clone(),
                        None => self.require_current_database()?.to_owned(),
                    };
                    let pattern = match &show.filter {
                        Some(tidb_ast::ShowTableStatusFilter::Like(tidb_ast::Expr::String(
                            text,
                        ))) => Some(text.clone()),
                        Some(tidb_ast::ShowTableStatusFilter::Like(_)) => {
                            return Err(DriverError::Unsupported(
                                "SHOW TABLE STATUS LIKE takes a string pattern",
                            ))
                        }
                        Some(tidb_ast::ShowTableStatusFilter::Where(_)) | None => None,
                    };
                    let where_clause = match &show.filter {
                        Some(tidb_ast::ShowTableStatusFilter::Where(expr)) => Some(expr.clone()),
                        _ => None,
                    };
                    let rows = self.with_catalog_mut(|catalog| {
                        let mut rows = Vec::new();
                        let names = catalog.table_names(&database).ok_or_else(|| {
                            DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
                        })?;
                        for name in names {
                            if let Some(pattern) = &pattern {
                                if !tidb_executor::like_match_with_collation(
                                    &name,
                                    pattern,
                                    None,
                                    tidb_datatype::Collation::Utf8Mb4Bin,
                                ) {
                                    continue;
                                }
                            }
                            let entry = catalog.table_in(&database, &name);
                            let auto_increment = match entry {
                                Some(tidb_executor::TableEntry::Kv(table)) => {
                                    table.next_auto_increment()
                                }
                                _ => None,
                            };
                            let row = if entry.is_some_and(tidb_executor::TableEntry::is_view) {
                                show_table_status_view_row(&name)
                            } else {
                                show_table_status_row(&name, auto_increment)
                            };
                            if let Some(predicate) = &where_clause {
                                if !show_row_matches(
                                    predicate,
                                    &SHOW_TABLE_STATUS_COLUMNS
                                        .iter()
                                        .map(|(name, _)| *name)
                                        .collect::<Vec<_>>(),
                                    &row,
                                )? {
                                    continue;
                                }
                            }
                            rows.push(row);
                        }
                        Ok(rows)
                    })?;
                    let text =
                        || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    let number =
                        || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                    let columns = SHOW_TABLE_STATUS_COLUMNS
                        .iter()
                        .map(|(name, numeric)| {
                            ((*name).to_owned(), if *numeric { number() } else { text() })
                        })
                        .collect();
                    Ok(Some(StmtOutput::Rows { columns, rows }))
                }
                // Go `fetchShowIndex`: one row per index COLUMN, ordered
                // with the clustered primary key first, then the table's own
                // indexes in definition order.
                //
                // NOT MODELLED, and each reported the way Go reports an
                // absent value rather than invented: Cardinality is 0 (no
                // statistics tier), Sub_part and Packed are NULL (no prefix
                // or packed indexes here), Comment/Index_comment are empty,
                // Expression is NULL (no expression indexes), and Global is
                // NO (no partitioned global indexes).
                tidb_ast::AdminStmt::ShowIndex(show) => {
                    if show.filter.is_some() {
                        return Err(DriverError::Unsupported(
                            "SHOW INDEX filters are not supported yet",
                        ));
                    }
                    let current = self.require_current_database()?.to_owned();
                    let (database, table_name) = match show.table.as_slice() {
                        [table] => (current, table.clone()),
                        [database, table] => (database.clone(), table.clone()),
                        _ => return Err(DriverError::Unsupported("empty table name")),
                    };
                    let rows = self.with_catalog_mut(|catalog| {
                        let Some(entry) = catalog.table_in(&database, &table_name) else {
                            return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(
                                format!("{database}.{table_name}"),
                            )));
                        };
                        let tidb_executor::TableEntry::Kv(table) = entry else {
                            return Err(DriverError::Unsupported(
                                "SHOW INDEX needs a storage-backed table",
                            ));
                        };
                        Ok(show_index_rows(&table_name, table))
                    })?;
                    let text =
                        || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    let number =
                        || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                    let columns = SHOW_INDEX_COLUMNS
                        .iter()
                        .map(|(name, numeric)| {
                            ((*name).to_owned(), if *numeric { number() } else { text() })
                        })
                        .collect();
                    Ok(Some(StmtOutput::Rows { columns, rows }))
                }
                // Go `ShowExec` with `ShowVariables`: one row per variable,
                // as `Variable_name` and `Value`, filtered by LIKE.
                //
                // DEFERRED (documented): the GLOBAL/SESSION distinction,
                // which reads the same value here because this tier keeps no
                // persisted global tier (`SET GLOBAL` already documents it).
                tidb_ast::AdminStmt::ShowVariables(show) => {
                    let pattern = match &show.like {
                        Some(tidb_ast::Expr::String(text)) => Some(text.clone()),
                        Some(_) => {
                            return Err(DriverError::Unsupported(
                                "SHOW VARIABLES LIKE takes a string pattern",
                            ))
                        }
                        None => None,
                    };
                    let text =
                        || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    let mut rows = Vec::new();
                    for definition in sysvar::SYS_VARS {
                        let matches = match &pattern {
                            Some(pattern) => tidb_executor::like_match_with_collation(
                                definition.name,
                                pattern,
                                None,
                                tidb_datatype::Collation::Utf8Mb4Bin,
                            ),
                            None => true,
                        };
                        if !matches {
                            continue;
                        }
                        let value = self
                            .vars
                            .get_system(definition.name)
                            .unwrap_or_else(|_| definition.value.to_owned());
                        let row = vec![
                            Datum::Bytes(definition.name.as_bytes().to_vec()),
                            Datum::Bytes(value.into_bytes()),
                        ];
                        // Go plans the WHERE as a selection over the same
                        // virtual rows, which is what this filter is.
                        if let Some(predicate) = &show.where_clause {
                            if !show_row_matches(predicate, SHOW_VARIABLE_COLUMNS, &row)? {
                                continue;
                            }
                        }
                        rows.push(row);
                    }
                    Ok(Some(StmtOutput::Rows {
                        columns: vec![
                            (SHOW_VARIABLE_COLUMNS[0].to_owned(), text()),
                            (SHOW_VARIABLE_COLUMNS[1].to_owned(), text()),
                        ],
                        rows,
                    }))
                }
                // Go `fetchShowStatus`: one `Variable_name | Value` row per
                // status variable that `variable.GetStatusVars` collects from
                // the registered `Statistics` providers, with `GLOBAL` scope
                // skipping session-only variables.
                //
                // This tier serves only `SHOW_STATUS_VARS` (see its doc
                // comment for what is not modelled). As with the
                // `ShowVariables` arm above, GLOBAL and SESSION read the same
                // values here because this tier keeps no persisted global
                // tier; GLOBAL still drops session-only rows, which the Go
                // capture confirms (`SHOW GLOBAL STATUS` omits the
                // `Compression*` family).
                tidb_ast::AdminStmt::ShowStatus(show) => {
                    let pattern = match &show.filter {
                        Some(tidb_ast::ShowStatusFilter::Like(tidb_ast::Expr::String(text))) => {
                            Some(text.clone())
                        }
                        Some(tidb_ast::ShowStatusFilter::Like(_)) => {
                            return Err(DriverError::Unsupported(
                                "SHOW STATUS LIKE takes a string pattern",
                            ))
                        }
                        _ => None,
                    };
                    let predicate = match &show.filter {
                        Some(tidb_ast::ShowStatusFilter::Where(expr)) => Some(expr),
                        _ => None,
                    };
                    let text =
                        || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    let mut rows = Vec::new();
                    for &(name, value, session_only) in SHOW_STATUS_VARS {
                        if show.global && session_only {
                            continue;
                        }
                        if let Some(pattern) = &pattern {
                            if !tidb_executor::like_match_with_collation(
                                name,
                                pattern,
                                None,
                                tidb_datatype::Collation::Utf8Mb4Bin,
                            ) {
                                continue;
                            }
                        }
                        let row = vec![
                            Datum::Bytes(name.as_bytes().to_vec()),
                            Datum::Bytes(value.as_bytes().to_vec()),
                        ];
                        // Go plans the WHERE as a selection over the same
                        // virtual rows, which is what this filter is.
                        if let Some(predicate) = predicate {
                            if !show_row_matches(predicate, SHOW_VARIABLE_COLUMNS, &row)? {
                                continue;
                            }
                        }
                        rows.push(row);
                    }
                    Ok(Some(StmtOutput::Rows {
                        columns: vec![
                            (SHOW_VARIABLE_COLUMNS[0].to_owned(), text()),
                            (SHOW_VARIABLE_COLUMNS[1].to_owned(), text()),
                        ],
                        rows,
                    }))
                }
                // Go `fetchShowCharset`: one row per charset in the parser's
                // registry, captured from mock TiDB (`Charset | Description |
                // Default collation | Maxlen`).
                //
                // DEFERRED (documented, and refused rather than ignored):
                // `WHERE`, because honoring it needs the same virtual-row
                // selection machinery `SHOW STATUS` uses and this table is
                // static rather than session state.
                tidb_ast::AdminStmt::ShowCharset(show) => {
                    let pattern = match &show.filter {
                        Some(tidb_ast::ShowCharsetFilter::Like(tidb_ast::Expr::String(text))) => {
                            Some(text.clone())
                        }
                        Some(tidb_ast::ShowCharsetFilter::Like(_)) => {
                            return Err(DriverError::Unsupported(
                                "SHOW CHARSET LIKE takes a string pattern",
                            ))
                        }
                        Some(tidb_ast::ShowCharsetFilter::Where(_)) => {
                            return Err(DriverError::Unsupported(
                                "SHOW CHARSET WHERE is not supported yet",
                            ))
                        }
                        None => None,
                    };
                    let text =
                        || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    let number =
                        || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                    let mut rows = Vec::new();
                    for &(name, description, default_collation, maxlen) in SHOW_CHARSET_ROWS {
                        if let Some(pattern) = &pattern {
                            if !tidb_executor::like_match_with_collation(
                                name,
                                pattern,
                                None,
                                tidb_datatype::Collation::Utf8Mb4Bin,
                            ) {
                                continue;
                            }
                        }
                        rows.push(vec![
                            Datum::Bytes(name.as_bytes().to_vec()),
                            Datum::Bytes(description.as_bytes().to_vec()),
                            Datum::Bytes(default_collation.as_bytes().to_vec()),
                            Datum::Int(maxlen),
                        ]);
                    }
                    Ok(Some(StmtOutput::Rows {
                        columns: vec![
                            ("Charset".to_owned(), text()),
                            ("Description".to_owned(), text()),
                            ("Default collation".to_owned(), text()),
                            ("Maxlen".to_owned(), number()),
                        ],
                        rows,
                    }))
                }
                // Go `fetchShowEngines`: this tier is the mock/embedded
                // single-engine server, so the table is always the single
                // `InnoDB` row Go's mock session reports.
                //
                // DEFERRED (documented, refused rather than ignored): `WHERE`
                // /`LIKE`, for the same reason as `SHOW CHARSET` above --
                // there is exactly one row and no virtual-row selection path
                // wired up for it yet.
                tidb_ast::AdminStmt::ShowEngines(show) => {
                    if show.filter.is_some() {
                        return Err(DriverError::Unsupported(
                            "SHOW ENGINES filters are not supported yet",
                        ));
                    }
                    let text =
                        || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    Ok(Some(StmtOutput::Rows {
                        columns: vec![
                            ("Engine".to_owned(), text()),
                            ("Support".to_owned(), text()),
                            ("Comment".to_owned(), text()),
                            ("Transactions".to_owned(), text()),
                            ("XA".to_owned(), text()),
                            ("Savepoints".to_owned(), text()),
                        ],
                        rows: vec![vec![
                            Datum::Bytes(b"InnoDB".to_vec()),
                            Datum::Bytes(b"DEFAULT".to_vec()),
                            Datum::Bytes(
                                b"Supports transactions, row-level locking, and foreign keys"
                                    .to_vec(),
                            ),
                            Datum::Bytes(b"YES".to_vec()),
                            Datum::Bytes(b"YES".to_vec()),
                            Datum::Bytes(b"YES".to_vec()),
                        ]],
                    }))
                }
                // Go `fetchShowCollation`: one row per collation in the
                // parser's registry (`Collation | Charset | Id | Default |
                // Compiled | Sortlen | Pad_attribute`).
                //
                // NOT MODELLED (documented): `Utf8Mb4ZhPinyinTiDbAsCs`, TiDB's
                // reserved pinyin collation stub -- mock TiDB's own `SHOW
                // COLLATION` capture omits it too, so this table matches the
                // 15 collations Go actually lists rather than this crate's
                // full 16-variant registry.
                //
                // DEFERRED (documented, and refused rather than ignored):
                // `WHERE`, for the same reason as `SHOW CHARSET` above.
                tidb_ast::AdminStmt::ShowCollation(show) => {
                    let pattern = match &show.filter {
                        Some(tidb_ast::ShowCollationFilter::Like(tidb_ast::Expr::String(text))) => {
                            Some(text.clone())
                        }
                        Some(tidb_ast::ShowCollationFilter::Like(_)) => {
                            return Err(DriverError::Unsupported(
                                "SHOW COLLATION LIKE takes a string pattern",
                            ))
                        }
                        Some(tidb_ast::ShowCollationFilter::Where(_)) => {
                            return Err(DriverError::Unsupported(
                                "SHOW COLLATION WHERE is not supported yet",
                            ))
                        }
                        None => None,
                    };
                    let text =
                        || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    let number =
                        || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                    let mut rows = Vec::new();
                    for &collation in SHOW_COLLATION_ROWS {
                        let name = collation.name();
                        if let Some(pattern) = &pattern {
                            if !tidb_executor::like_match_with_collation(
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
                            tidb_datatype::Collation::Binary
                            | tidb_datatype::Collation::Utf8Mb40900Bin => (1, "NO PAD"),
                            _ => (1, "PAD SPACE"),
                        };
                        rows.push(vec![
                            Datum::Bytes(name.as_bytes().to_vec()),
                            Datum::Bytes(collation.charset().name().as_bytes().to_vec()),
                            Datum::Int(i64::from(collation.id())),
                            Datum::Bytes(if is_default_show_collation(collation) {
                                b"Yes".to_vec()
                            } else {
                                Vec::new()
                            }),
                            Datum::Bytes(b"Yes".to_vec()),
                            Datum::Int(sortlen),
                            Datum::Bytes(pad_attribute.as_bytes().to_vec()),
                        ]);
                    }
                    Ok(Some(StmtOutput::Rows {
                        columns: vec![
                            ("Collation".to_owned(), text()),
                            ("Charset".to_owned(), text()),
                            ("Id".to_owned(), number()),
                            ("Default".to_owned(), text()),
                            ("Compiled".to_owned(), text()),
                            ("Sortlen".to_owned(), number()),
                            ("Pad_attribute".to_owned(), text()),
                        ],
                        rows,
                    }))
                }
                // Go `ShowExec` with `ShowWarnings`/`ShowErrors`: the rows are
                // the statement-context warnings, whose `Level` column is
                // `Warning` or `Error`.
                //
                // DEFERRED (documented, and refused rather than ignored): the
                // optional filter Go's shared SHOW grammar accepts here.
                tidb_ast::AdminStmt::ShowWarnings(show) => {
                    if show.filter.is_some() {
                        return Err(DriverError::Unsupported(
                            "SHOW WARNINGS filters are not supported yet",
                        ));
                    }
                    Ok(Some(self.warning_output(show.count_only, false)))
                }
                tidb_ast::AdminStmt::ShowErrors(show) => {
                    if show.filter.is_some() {
                        return Err(DriverError::Unsupported(
                            "SHOW ERRORS filters are not supported yet",
                        ));
                    }
                    Ok(Some(self.warning_output(show.count_only, true)))
                }
                // Go `ShowExec.fetchShowProcessList`: one row per live
                // connection of this server, read from the session manager.
                tidb_ast::AdminStmt::ShowInspection(show) => {
                    if show.kind != tidb_ast::ShowInspectionKind::ProcessList {
                        return Ok(None);
                    }
                    if show.filter.is_some() || show.database.is_some() {
                        return Err(DriverError::Unsupported(
                            "SHOW PROCESSLIST filters are not supported yet",
                        ));
                    }
                    Ok(Some(self.process_list_output(show.full)))
                }
                // Go `SimpleExec.executeKillStmt`.
                tidb_ast::AdminStmt::Kill(kill) => {
                    let target = match &kill.target {
                        tidb_ast::KillTarget::ConnectionId(id) => *id,
                        // Go accepts `KILL CONNECTION_ID()` (kill my own
                        // connection) and rejects every other expression with
                        // this exact message.
                        tidb_ast::KillTarget::Expr(tidb_ast::Expr::Func { name, args, .. })
                            if name.eq_ignore_ascii_case("connection_id") && args.is_empty() =>
                        {
                            self.connection_id.unwrap_or(0)
                        }
                        tidb_ast::KillTarget::Expr(_) => {
                            return Err(DriverError::Unsupported(
                                "Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead",
                            ))
                        }
                    };
                    // Captured from TiDB: KILL of an id this server does not
                    // hold is NOT an error -- it answers OK, having done
                    // nothing. (1094 `Unknown thread id` belongs to EXPLAIN
                    // FOR CONNECTION, not to KILL.) A session with no server
                    // front holds no connection at all, which is Go's
                    // `sm == nil` early return: also a silent no-op.
                    if let Some(guard) = &self.process {
                        // Go `planbuilder.go`'s `*ast.KillStmt` case: everyone
                        // may KILL their own connection regardless of
                        // privilege; killing anyone else's requires SUPER (or
                        // the dynamic CONNECTION_ADMIN, not modelled in this
                        // tier), reported as `ErrSpecificAccessDenied` (1227)
                        // -- NOT the unused 1095 `ErrKillDenied` errno entry,
                        // which no code path in current Go ever raises.
                        let is_self = self.connection_id == Some(target);
                        if !is_self {
                            let owner = guard
                                .registry()
                                .snapshot()
                                .into_iter()
                                .find(|row| row.id == target)
                                .map(|row| row.user);
                            let same_user =
                                owner.as_deref() == Some(self.process_list_user().as_str());
                            let has_super = self.privileges.as_ref().is_some_and(|registry| {
                                self.current_identity().is_some_and(|(user, host)| {
                                    registry.has_global_priv(
                                        user,
                                        host,
                                        privilege::GlobalPriv::Super,
                                    )
                                })
                            });
                            if owner.is_some() && !same_user && !has_super {
                                return Err(DriverError::KillAccessDenied);
                            }
                        }
                        guard.registry().kill(target, kill.query);
                    }
                    Ok(Some(StmtOutput::Affected(0)))
                }
                // Go `fetchShowCreateTable`.
                tidb_ast::AdminStmt::ShowCreate { kind, name, .. } => {
                    let want_view = match kind {
                        tidb_ast::ShowCreateKind::Table => false,
                        tidb_ast::ShowCreateKind::View => true,
                        _ => return Ok(None),
                    };
                    let current = self.require_current_database()?.to_owned();
                    let (database, table_name) = match name.as_slice() {
                        [table] => (current, table.clone()),
                        [database, table] => (database.clone(), table.clone()),
                        _ => return Err(DriverError::Unsupported("empty table name")),
                    };
                    // A view answers either spelling with the same row, which
                    // is Go's own behaviour; only `SHOW CREATE VIEW` on a base
                    // table is refused.
                    let (text, reported, is_view) = self.with_catalog_mut(|catalog| {
                        let Some(entry) = catalog.table_in(&database, &table_name) else {
                            return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(
                                format!("{database}.{table_name}"),
                            )));
                        };
                        match entry {
                            tidb_executor::TableEntry::View(view) => {
                                Ok((show_create_view_text(view), table_name.clone(), true))
                            }
                            _ if want_view => Err(DriverError::Schema(SchemaErrorKind::NotView(
                                format!("{database}.{table_name}"),
                            ))),
                            tidb_executor::TableEntry::Kv(table) => Ok((
                                show_create_table_text(&table_name, table),
                                table_name.clone(),
                                false,
                            )),
                            tidb_executor::TableEntry::Mem(_) => Err(DriverError::Unsupported(
                                "SHOW CREATE TABLE needs a storage-backed table",
                            )),
                        }
                    })?;
                    let field_type =
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    // Go's view form carries its own header and the session's
                    // character set and collation.
                    if is_view {
                        return Ok(Some(StmtOutput::Rows {
                            columns: vec![
                                ("View".to_owned(), field_type.clone()),
                                ("Create View".to_owned(), field_type.clone()),
                                ("character_set_client".to_owned(), field_type.clone()),
                                ("collation_connection".to_owned(), field_type),
                            ],
                            rows: vec![vec![
                                Datum::Bytes(reported.into_bytes()),
                                Datum::Bytes(text.into_bytes()),
                                Datum::Bytes(b"utf8mb4".to_vec()),
                                Datum::Bytes(b"utf8mb4_bin".to_vec()),
                            ]],
                        }));
                    }
                    Ok(Some(StmtOutput::Rows {
                        columns: vec![
                            ("Table".to_owned(), field_type.clone()),
                            ("Create Table".to_owned(), field_type),
                        ],
                        rows: vec![vec![
                            Datum::Bytes(reported.into_bytes()),
                            Datum::Bytes(text.into_bytes()),
                        ]],
                    }))
                }
                // Go `fetchShowColumns`.
                tidb_ast::AdminStmt::ShowColumns(show) => {
                    if show.filter.is_some() || show.extended {
                        return Err(DriverError::Unsupported(
                            "SHOW EXTENDED COLUMNS and column filters are not supported yet",
                        ));
                    }
                    let database = match &show.database {
                        Some(name) => name.clone(),
                        None => self.require_current_database()?.to_owned(),
                    };
                    self.show_columns(&database, &show.table, None, show.full)
                        .map(Some)
                }
                // Go's parser rewrites `DESCRIBE tbl [col]` into a SHOW
                // COLUMNS statement; this parser keeps a node of its own, so
                // the same output is produced from it here.
                tidb_ast::AdminStmt::DescribeTable(describe) => {
                    let database = self.require_current_database()?.to_owned();
                    let column = describe.column.as_ref().and_then(|path| path.last());
                    self.show_columns(
                        &database,
                        &describe.table,
                        column.map(String::as_str),
                        false,
                    )
                    .map(Some)
                }
                tidb_ast::AdminStmt::ShowTables(show) => {
                    if show.filter.is_some() {
                        return Err(DriverError::Unsupported(
                            "SHOW TABLES filters are not supported yet",
                        ));
                    }
                    let database = match &show.database {
                        Some(name) => name.clone(),
                        None => self.require_current_database()?.to_owned(),
                    };
                    let full = show.full;
                    let listed = self.with_catalog_mut(|catalog| {
                        Ok(catalog.table_names(&database).map(|names| {
                            names
                                .into_iter()
                                .map(|name| {
                                    let is_view = catalog.is_view_in(&database, &name);
                                    (name, is_view)
                                })
                                .collect::<Vec<_>>()
                        }))
                    })?;
                    let listed = listed.ok_or_else(|| {
                        DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
                    })?;
                    // Go names the column after the schema being listed.
                    let name_column = format!("Tables_in_{database}");
                    if !full {
                        return Ok(Some(string_column_output(
                            &name_column,
                            listed.into_iter().map(|(name, _)| name).collect(),
                        )));
                    }
                    // Go's `SHOW FULL TABLES` adds the object kind.
                    let field_type =
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    Ok(Some(StmtOutput::Rows {
                        columns: vec![
                            (name_column, field_type.clone()),
                            ("Table_type".to_owned(), field_type),
                        ],
                        rows: listed
                            .into_iter()
                            .map(|(name, is_view)| {
                                vec![
                                    Datum::Bytes(name.into_bytes()),
                                    Datum::Bytes(table_type_of(is_view).as_bytes().to_vec()),
                                ]
                            })
                            .collect(),
                    }))
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    /// Runs a `SELECT` whose `FROM` names an `information_schema` table.
    ///
    /// The virtual rows are materialized into a scratch catalog and then run
    /// through the ordinary plan, so `WHERE`, `ORDER BY`, `LIMIT`, expressions
    /// and aggregates all behave as they do over a stored table. Go reaches
    /// the same place differently -- its memory tables are real tables to the
    /// planner -- but the requirement is the same: a predicate over a virtual
    /// table must filter it.
    ///
    /// Returns `None` when the statement is an ordinary one, so the caller
    /// falls through to the storage path.
    ///
    /// DEFERRED (documented): a join between a virtual table and a stored one,
    /// because the scratch catalog holds only the virtual side. Such a
    /// statement is rejected rather than answered from half the data.
    fn run_information_schema_select(
        &mut self,
        select: &tidb_ast::SelectStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        let Some(join) = &select.from else {
            return Ok(None);
        };
        let tidb_ast::JoinNode::Table(table_ref) = &join.left else {
            return Ok(None);
        };
        // `information_schema.X`, or a bare `X` while that schema is current.
        let (schema, table_name) = match table_ref.name.as_slice() {
            [name] => (self.current_db.clone(), name.clone()),
            [schema, name] => (schema.clone(), name.clone()),
            _ => return Ok(None),
        };
        if !infoschema::is_information_schema(&schema) {
            return Ok(None);
        }
        if join.right.is_some() {
            return Err(DriverError::Unsupported(
                "joining an information_schema table is not supported yet",
            ));
        }
        let Some(columns) = infoschema::table_schema(&table_name) else {
            return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                "{schema}.{table_name}"
            ))));
        };
        // `PROCESSLIST` is session/registry state, not catalog state, so it
        // is built directly rather than through `infoschema::table_rows`,
        // which only ever sees the catalog.
        let rows = if table_name.eq_ignore_ascii_case("PROCESSLIST") {
            self.process_list_table_rows()
        } else {
            self.with_catalog_mut(|catalog| {
                Ok(infoschema::table_rows(&table_name, catalog).unwrap_or_default())
            })?
        };

        // A scratch catalog holding just this table, so the ordinary plan runs
        // over it.
        let mut scratch = Catalog::default();
        scratch.register_mem_in(
            infoschema::INFORMATION_SCHEMA,
            &table_name,
            tidb_executor::MemTable { columns, rows },
        );
        let ctx = self.statement_context(false);
        let (columns, rows) = tidb_executor::run_select_meta_stmt(
            select,
            &scratch,
            infoschema::INFORMATION_SCHEMA,
            &ctx,
        )?;
        self.drain_eval_warnings(&ctx);
        Ok(Some(StmtOutput::Rows { columns, rows }))
    }

    /// The `SHOW COLUMNS` / `DESCRIBE` result for one table, optionally
    /// narrowed to a single column as Go's `DESCRIBE tbl col` narrows it.
    fn show_columns(
        &mut self,
        database: &str,
        table_path: &[String],
        column: Option<&str>,
        full: bool,
    ) -> Result<StmtOutput, DriverError> {
        // A `db.tbl` path names its own schema, as everywhere else.
        let (database, table_name) = match table_path {
            [name] => (database.to_owned(), name.clone()),
            [db, name] => (db.clone(), name.clone()),
            _ => return Err(DriverError::Unsupported("empty table name")),
        };
        let ctx = self.statement_context(false);
        let rows = self.with_catalog_mut(|catalog| {
            let Some(entry) = catalog.table_in(&database, &table_name) else {
                return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                    "{database}.{table_name}"
                ))));
            };
            if let tidb_executor::TableEntry::View(view) = entry {
                // Go re-plans the body here (`tryFillViewColumnType`), so the
                // types reported are the ones the base tables have now, and a
                // body that no longer resolves fails the statement with its
                // own error rather than with ErrViewInvalid.
                let view = view.clone();
                let columns = tidb_executor::view_column_list(&view, &database, catalog, &ctx)?;
                return Ok(columns
                    .iter()
                    .filter(|(candidate, _)| {
                        column.is_none_or(|name| candidate.eq_ignore_ascii_case(name))
                    })
                    .map(|(name, field_type)| view_column_description(name, field_type, full))
                    .collect::<Vec<_>>());
            }
            let tidb_executor::TableEntry::Kv(table) = entry else {
                return Err(DriverError::Unsupported(
                    "SHOW COLUMNS needs a storage-backed table",
                ));
            };
            Ok(table
                .columns
                .iter()
                .enumerate()
                .filter(|(_, candidate)| {
                    column.is_none_or(|name| candidate.name.eq_ignore_ascii_case(name))
                })
                .map(|(offset, candidate)| column_description(candidate, offset, table, full))
                .collect::<Vec<_>>())
        })?;
        let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let field_names = if full {
            FULL_COL_DESC_FIELD_NAMES
        } else {
            COL_DESC_FIELD_NAMES
        };
        Ok(StmtOutput::Rows {
            columns: field_names
                .iter()
                .map(|name| ((*name).to_owned(), field_type.clone()))
                .collect(),
            rows,
        })
    }

    /// Go `executeUse`: an unknown schema is `ErrDatabaseNotExists`, and the
    /// switch also updates `collation_database`.
    fn use_database(&mut self, name: &str) -> Result<(), DriverError> {
        let exists = self.with_catalog_mut(|catalog| Ok(catalog.has_database(name)))?;
        if !exists {
            return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                name.to_owned(),
            )));
        }
        self.current_db = name.to_owned();
        Ok(())
    }

    /// The current database, or Go's `ErrNoDB` when none is selected.
    fn require_current_database(&self) -> Result<&str, DriverError> {
        if self.current_db.is_empty() {
            return Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected));
        }
        Ok(&self.current_db)
    }

    /// Go `LAST_INSERT_ID()`: the first id the most recent ALLOCATING
    /// statement handed out. A statement that allocated nothing -- an explicit
    /// auto value, a table with no auto column, an UPDATE -- leaves it as it
    /// was, which is what MySQL and TiDB both do.
    #[must_use]
    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    /// The id the last statement allocated, which the OK packet reports and
    /// which is 0 when the statement allocated nothing.
    #[must_use]
    pub fn statement_insert_id(&self) -> u64 {
        self.statement_insert_id
    }

    /// The session's variables.
    #[must_use]
    pub fn vars(&self) -> &SessionVars {
        &self.vars
    }

    /// Applies a `SET` statement.
    ///
    /// Returns `Some(())` when the SQL is a `SET` this handles and `None`
    /// otherwise, so a caller can answer with an OK packet without
    /// re-parsing. Go's `SetExecutor` walks the assignments in source order
    /// and stops at the first error, which this reproduces.
    ///
    /// DEFERRED (documented): `SET GLOBAL` changes only this session here,
    /// because there is no persisted global tier yet; `SET PASSWORD`,
    /// resource groups, and the other non-variable `SET` forms stay
    /// unsupported.
    pub fn apply_set(&mut self, sql: &str) -> Result<Option<()>, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        let Stmt::Session(session_stmt) = &stmt else {
            return Ok(None);
        };
        match &**session_stmt {
            SessionStmt::Set(set) => {
                for assignment in &set.assignments {
                    self.apply_assignment(assignment)?;
                }
                Ok(Some(()))
            }
            SessionStmt::SetCharset {
                charset,
                collation,
                assignments,
                ..
            } => {
                self.apply_charset(charset.as_deref(), collation.as_deref())?;
                for assignment in assignments {
                    self.apply_assignment(assignment)?;
                }
                Ok(Some(()))
            }
            SessionStmt::SetMixed(items) => {
                for item in items {
                    match item {
                        tidb_ast::SetItem::System(assignment) => {
                            self.apply_assignment(assignment)?;
                        }
                        tidb_ast::SetItem::Charset {
                            charset, collation, ..
                        } => self.apply_charset(charset.as_deref(), collation.as_deref())?,
                    }
                }
                Ok(Some(()))
            }
            SessionStmt::SetUserVar(set) => {
                for assignment in &set.assignments {
                    let value = self.eval_literal(&assignment.value)?;
                    self.vars.set_user(&assignment.name, value);
                }
                Ok(Some(()))
            }
            _ => Ok(None),
        }
    }

    /// One `name = value` assignment.
    fn apply_assignment(
        &mut self,
        assignment: &tidb_ast::SystemVariableAssignment,
    ) -> Result<(), DriverError> {
        let value = match &assignment.value {
            // Go restores a variable to its registry default by clearing the
            // session override.
            tidb_ast::SetVariableValue::Default => {
                self.vars
                    .reset_system(&assignment.name)
                    .map_err(var_error)?;
                return Ok(());
            }
            tidb_ast::SetVariableValue::Expr(expr) => self.eval_literal(expr)?,
        };
        // Go stores every system variable as a string.
        self.vars
            .set_system(&assignment.name, value.unwrap_or_default())
            .map_err(var_error)
    }

    /// `SET NAMES` / `SET CHARACTER SET`.
    fn apply_charset(
        &mut self,
        charset: Option<&str>,
        collation: Option<&str>,
    ) -> Result<(), DriverError> {
        // `DEFAULT` restores the registry default, which is what the charset
        // variables already hold when nothing has overridden them.
        let charset = charset.unwrap_or("utf8mb4");
        self.vars.set_names(charset, collation).map_err(var_error)
    }

    /// Evaluates a `SET` right-hand side. Go runs it through the expression
    /// evaluator; this evaluates it as a constant expression, which covers the
    /// literals and simple arithmetic a `SET` carries.
    fn eval_literal(&mut self, expr: &tidb_ast::Expr) -> Result<Option<String>, DriverError> {
        // An unquoted identifier is a bare word value such as `SET sql_mode =
        // ANSI_QUOTES` or `SET autocommit = ON`, which MySQL takes literally.
        if let tidb_ast::Expr::Column(path) = expr {
            if let [word] = path.as_slice() {
                return Ok(Some(word.clone()));
            }
        }
        let sql = format!("SELECT {}", expr.restore());
        let ctx = self.statement_context(false);
        let rows =
            self.with_catalog_mut(|catalog| tidb_executor::run_select_on(&sql, catalog, &ctx))?;
        let value = rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .unwrap_or(Datum::Null);
        Ok(datum_text(&value))
    }

    /// Replaces every variable reference in `sql` with the session's value,
    /// so the driver plans against ordinary literals.
    ///
    /// Go resolves `@@x` and `@x` in the expression rewriter using the
    /// session's variables; the values live in the session here, so the
    /// substitution happens here too. An unknown `@@x` is Go's 1193, while an
    /// unset `@x` is NULL rather than an error, as in MySQL.
    fn bind_variables(&self, stmt: &mut Stmt) -> Result<(), DriverError> {
        let Stmt::Query(query) = stmt else {
            return Ok(());
        };
        let tidb_ast::QueryStmt::Select(select) = &mut **query else {
            return Ok(());
        };
        for field in select.fields.fields_mut() {
            if let tidb_ast::SelectField::Expr { expr, .. } = field {
                *expr = self.bind_variables_in(expr)?;
            }
        }
        if let Some(where_clause) = &select.where_clause {
            select.where_clause = Some(self.bind_variables_in(where_clause)?);
        }
        if let Some(having) = &select.having {
            select.having = Some(self.bind_variables_in(having)?);
        }
        for item in &mut select.order_by {
            item.expr = self.bind_variables_in(&item.expr)?;
        }
        for item in &mut select.group_by {
            item.expr = self.bind_variables_in(&item.expr)?;
        }
        Ok(())
    }

    /// Substitutes variable references inside one expression.
    fn bind_variables_in(&self, expr: &tidb_ast::Expr) -> Result<tidb_ast::Expr, DriverError> {
        use tidb_ast::Expr;
        Ok(match expr {
            Expr::SysVar { name, .. } => {
                // A scope prefix does not change the value here: there is no
                // separate global tier yet (documented in `vars`).
                match self.vars.get_system(name) {
                    Ok(value) => Expr::String(value),
                    Err(error) => return Err(var_error(error)),
                }
            }
            // `LAST_INSERT_ID()` reads session state, so it binds here for
            // the same reason `@@x` does.
            Expr::Func { name, args, .. }
                if name.eq_ignore_ascii_case("LAST_INSERT_ID") && args.is_empty() =>
            {
                Expr::Int(self.last_insert_id.to_string())
            }
            Expr::UserVar(name) => match self.vars.get_user(name) {
                Some(value) => Expr::String(value),
                None => Expr::Null,
            },
            Expr::Paren(inner) => Expr::Paren(Box::new(self.bind_variables_in(inner)?)),
            Expr::Unary(op, inner) => Expr::Unary(*op, Box::new(self.bind_variables_in(inner)?)),
            Expr::Binary(op, lhs, rhs) => Expr::Binary(
                *op,
                Box::new(self.bind_variables_in(lhs)?),
                Box::new(self.bind_variables_in(rhs)?),
            ),
            Expr::Is { expr, target, not } => Expr::Is {
                expr: Box::new(self.bind_variables_in(expr)?),
                target: *target,
                not: *not,
            },
            Expr::In { expr, list, not } => Expr::In {
                expr: Box::new(self.bind_variables_in(expr)?),
                list: list
                    .iter()
                    .map(|item| self.bind_variables_in(item))
                    .collect::<Result<_, _>>()?,
                not: *not,
            },
            other => other.clone(),
        })
    }

    /// Whether a transaction is open (the wire's `SERVER_STATUS_IN_TRANS`).
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.txn.is_some()
    }

    /// Applies `BEGIN`/`START TRANSACTION`, `COMMIT`, or `ROLLBACK`.
    ///
    /// Returns `Some(in_transaction)` for those statements and `None` for
    /// anything else, so a caller can answer with an OK packet carrying the
    /// right status flag without re-parsing.
    ///
    /// Go's `BEGIN` inside an open transaction implicitly commits the current
    /// one before starting the new one, which this reproduces. `COMMIT` and
    /// `ROLLBACK` with no open transaction are no-ops, as in MySQL.
    pub fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        let Stmt::Session(session_stmt) = &stmt else {
            return Ok(None);
        };
        match &**session_stmt {
            SessionStmt::Begin(_) => {
                // An open transaction is committed first (Go's implicit commit).
                if self.txn.is_some() {
                    self.commit()?;
                }
                let (working, base_version) = {
                    let catalog = self.lock_catalog()?;
                    (catalog.clone(), catalog.version())
                };
                self.txn = Some(Transaction {
                    working,
                    base_version,
                });
                Ok(Some(true))
            }
            SessionStmt::Commit(_) => {
                self.commit()?;
                Ok(Some(false))
            }
            SessionStmt::Rollback { savepoint, .. } => {
                if savepoint.is_some() {
                    return Err(DriverError::Unsupported(
                        "ROLLBACK TO SAVEPOINT is not supported yet",
                    ));
                }
                // Dropping the staged copy discards every staged write.
                self.txn = None;
                Ok(Some(false))
            }
            _ => Ok(None),
        }
    }

    /// Publishes the open transaction's staged writes, or refuses when the
    /// shared catalog moved under it. A refused commit ends the transaction,
    /// as an aborted Go transaction does -- the staged writes are gone either
    /// way, so the caller must retry the statements, not just the COMMIT.
    fn commit(&mut self) -> Result<(), DriverError> {
        let Some(txn) = self.txn.take() else {
            // COMMIT with no open transaction is a no-op, as in MySQL.
            return Ok(());
        };
        let mut shared = self.lock_catalog()?;
        if shared.version() != txn.base_version {
            return Err(DriverError::Txn(TxnErrorKind::WriteConflict));
        }
        *shared = txn.working;
        Ok(())
    }

    /// A session sharing `catalog` with its peers.
    #[must_use]
    pub fn with_catalog(catalog: SharedCatalog) -> Self {
        Session {
            catalog,
            txn: None,
            vars: SessionVars::new(),
            warnings: Vec::new(),
            current_user: None,
            login_user: None,
            connection_id: None,
            last_insert_id: 0,
            statement_insert_id: 0,
            current_db: DEFAULT_DATABASE.to_owned(),
            process: None,
            has_process_priv: false,
            privileges: None,
            rand: new_time_seeded_rand(),
        }
    }

    /// The shared catalog handle, for opening a peer session over the same
    /// schema state.
    #[must_use]
    pub fn shared_catalog(&self) -> SharedCatalog {
        Arc::clone(&self.catalog)
    }

    /// Borrows the shared catalog for one statement. The lock is held for the
    /// statement's duration only, which is the granularity Go's schema state
    /// is consumed at.
    fn lock_catalog(&self) -> Result<MutexGuard<'_, Catalog>, DriverError> {
        self.catalog
            .lock()
            .map_err(|_| DriverError::CatalogPoisoned)
    }

    /// Runs `body` over the catalog this statement sees: the transaction's
    /// staged copy when one is open (so it reads its own writes), otherwise
    /// the shared catalog directly (autocommit).
    fn with_catalog_mut<T>(
        &mut self,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        match &mut self.txn {
            Some(txn) => body(&mut txn.working),
            None => {
                let mut catalog = self
                    .catalog
                    .lock()
                    .map_err(|_| DriverError::CatalogPoisoned)?;
                body(&mut catalog)
            }
        }
    }

    /// Classifies a statement by parsing alone (no execution), so a caller can
    /// choose the protocol answer shape before running it.
    ///
    /// This decides the SHAPE of the answer, not whether the statement is
    /// supported: a `SHOW` this tier cannot answer still classifies as a
    /// query and reports its own error when it runs. Classifying it as
    /// unsupported here is what made every `SHOW` fail over the wire while
    /// `run` answered it in process -- the two callers of one session have to
    /// agree.
    pub fn statement_kind(&self, sql: &str) -> Result<StmtKind, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        Ok(match &stmt {
            // `KILL` is the one admin statement that answers with an OK
            // packet rather than a result set, as it does in Go.
            Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::Kill(_)) => {
                StmtKind::Write
            }
            // `SHOW`/`DESCRIBE`/`EXPLAIN` all answer with a result set.
            Stmt::Query(_) | Stmt::Admin(_) => StmtKind::Query,
            // `USE`, `SET` and the transaction controls answer with an OK
            // packet, the same shape a write uses.
            Stmt::Dml(_) | Stmt::Ddl(_) | Stmt::Session(_) => StmtKind::Write,
        })
    }

    /// Records the authenticated identity, which the builtins report.
    ///
    /// Go sets `SessionVars.User` once the connection authenticates; a
    /// front end that has no user leaves it unset and the builtins answer
    /// NULL, which is what Go does for a session without one.
    pub fn set_user(&mut self, current_user: String, login_user: String) {
        self.current_user = Some(current_user);
        self.login_user = Some(login_user);
    }

    /// Grants or revokes this session's `PROCESS` privilege.
    ///
    /// See the [`Session::has_process_priv`] field doc for why this exists
    /// as a direct setter rather than a `GRANT PROCESS ON *.* TO ...`
    /// statement: `GRANT` is not implemented in this tier yet.
    pub fn set_process_privilege(&mut self, granted: bool) {
        self.has_process_priv = granted;
    }

    /// Joins this session to the server's process list under `connection_id`.
    ///
    /// Go's server registers each connection with the `sessmgr.Manager` right
    /// after authentication; `guard` is that registration, and dropping the
    /// session removes the row.
    pub fn attach_process(&mut self, connection_id: u64, guard: process::ProcessGuard) {
        self.connection_id = Some(connection_id);
        self.process = Some(guard);
    }

    /// Joins this session to the server's account/global-privilege registry.
    ///
    /// Go's session reads `privilege.Manager` off the `Domain` every
    /// connection shares; this is the equivalent handle, installed by the
    /// front end the same way [`Session::attach_process`] installs the
    /// process-list registry.
    pub fn attach_privileges(&mut self, registry: privilege::PrivilegeRegistry) {
        self.privileges = Some(registry);
    }

    /// Splits the `CURRENT_USER()` identity (`user@host`) this session
    /// authenticated as, for privilege-registry lookups. `None` for a
    /// session with no front end.
    fn current_identity(&self) -> Option<(&str, &str)> {
        let identity = self.current_user.as_deref()?;
        identity.split_once('@')
    }

    /// `CREATE USER` at the GLOBAL scope this tier models: an account
    /// identity and nothing else. Go `simple.go`'s `executeCreateUser`,
    /// minus authentication, resource limits, and account annotations, which
    /// this tier has no storage for and therefore refuses rather than
    /// silently drops.
    #[allow(clippy::too_many_arguments)]
    fn create_user_stmt(
        &mut self,
        if_not_exists: bool,
        users: &[tidb_ast::CreateUserSpec],
        tls_options: &[tidb_ast::AlterUserTlsOption],
        resource_options: &[tidb_ast::AlterUserResourceOption],
        password_options: &[tidb_ast::CreateUserPasswordOption],
        comment_or_attribute: &Option<tidb_ast::CreateUserCommentOrAttribute>,
        resource_group: &Option<String>,
    ) -> Result<StmtOutput, DriverError> {
        if !tls_options.is_empty()
            || !resource_options.is_empty()
            || !password_options.is_empty()
            || comment_or_attribute.is_some()
            || resource_group.is_some()
        {
            return Err(DriverError::Unsupported(
                "CREATE USER options beyond the account list are not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "CREATE USER requires a server front end with a privilege registry",
            ));
        };
        for spec in users {
            if spec.auth.is_some() {
                return Err(DriverError::Unsupported(
                    "CREATE USER ... IDENTIFIED BY/WITH is not supported yet",
                ));
            }
            let user = spec.user.user.as_str();
            let host = spec.user.host.as_str();
            // Go processes each account in source order and fails on the
            // FIRST duplicate rather than batching, unlike DROP USER below.
            if !registry.create_user(user, host) && !if_not_exists {
                return Err(DriverError::CreateUserAlreadyExists {
                    user: user.to_owned(),
                    host: host.to_owned(),
                });
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// `DROP USER` at the GLOBAL scope this tier models. Go's
    /// `executeDropUser` checks every named account exists BEFORE dropping
    /// any of them, rolling the whole statement back and reporting every
    /// missing account together if one is missing.
    fn drop_user_stmt(
        &mut self,
        if_exists: bool,
        users: &[tidb_ast::UserSpec],
    ) -> Result<StmtOutput, DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "DROP USER requires a server front end with a privilege registry",
            ));
        };
        if !if_exists {
            let missing: Vec<String> = users
                .iter()
                .filter(|spec| !registry.user_exists(&spec.user, &spec.host))
                .map(|spec| format!("{}@{}", spec.user, spec.host))
                .collect();
            if !missing.is_empty() {
                return Err(DriverError::DropUserMissing {
                    accounts: missing.join(","),
                });
            }
        }
        for spec in users {
            registry.drop_user(&spec.user, &spec.host);
        }
        Ok(StmtOutput::Affected(0))
    }

    /// `GRANT <static privs> ON <level> TO <user>...` -- Go's `grant.go`
    /// GLOBAL/DATABASE/TABLE scopes. Roles, dynamic privileges,
    /// `WITH GRANT OPTION`, and column lists are refused rather than
    /// silently accepted or dropped.
    fn grant_stmt(&mut self, grant: &tidb_ast::GrantStmt) -> Result<StmtOutput, DriverError> {
        if grant.object_type.is_some() {
            return Err(DriverError::Unsupported(
                "GRANT ... ON FUNCTION/PROCEDURE is not supported yet",
            ));
        }
        if grant.with_grant {
            return Err(DriverError::Unsupported(
                "GRANT ... WITH GRANT OPTION is not supported yet",
            ));
        }
        if !grant.tls_options.is_empty() {
            return Err(DriverError::Unsupported(
                "GRANT ... REQUIRE is not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "GRANT requires a server front end with a privilege registry",
            ));
        };
        match &grant.level {
            tidb_ast::GrantLevel::Global => {
                let mask = self.resolve_global_priv_mask(&grant.privileges)?;
                for spec in &grant.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    // Go's default sql_mode forbids GRANT from implicitly
                    // creating the target account (captured:
                    // `ErrCantCreateUserWithGrant`, 1410).
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::GrantToUnknownUser);
                    }
                    registry.grant(user, host, mask);
                }
            }
            tidb_ast::GrantLevel::Database(database) => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let privs = self.resolve_scoped_privs(&grant.privileges, ScopeKind::Database)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit());
                for spec in &grant.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::GrantToUnknownUser);
                    }
                    registry.grant_db(user, host, &database, mask);
                }
            }
            tidb_ast::GrantLevel::Table { database, table } => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let privs = self.resolve_scoped_privs(&grant.privileges, ScopeKind::Table)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit());
                // Go allows granting on a table that does not exist only
                // when the privilege list includes `CREATE` (captured:
                // issues #28533/#29268); otherwise it reports
                // `ErrTableNotExists` (1146).
                let table_exists = self.lock_catalog()?.table_in(&database, table).is_some();
                if !table_exists && !privs.contains(&privilege::GlobalPriv::Create) {
                    return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                        "{database}.{table}"
                    ))));
                }
                for spec in &grant.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::GrantToUnknownUser);
                    }
                    registry.grant_table(user, host, &database, table, mask);
                }
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// `REVOKE <static privs> ON <level> FROM <user>...`. Go's `revoke.go`
    /// requires every named account to already exist (`errors.Errorf("Unknown
    /// user: %s", ...)`, captured); this tier does too.
    fn revoke_stmt(&mut self, revoke: &tidb_ast::RevokeStmt) -> Result<StmtOutput, DriverError> {
        if revoke.object_type.is_some() {
            return Err(DriverError::Unsupported(
                "REVOKE ... ON FUNCTION/PROCEDURE is not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "REVOKE requires a server front end with a privilege registry",
            ));
        };
        match &revoke.level {
            tidb_ast::GrantLevel::Global => {
                let mask = self.resolve_global_priv_mask(&revoke.privileges)?;
                for spec in &revoke.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::RevokeUnknownUser {
                            user: user.to_owned(),
                            host: host.to_owned(),
                        });
                    }
                    registry.revoke(user, host, mask);
                }
            }
            tidb_ast::GrantLevel::Database(database) => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let privs = self.resolve_scoped_privs(&revoke.privileges, ScopeKind::Database)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit());
                for spec in &revoke.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::RevokeUnknownUser {
                            user: user.to_owned(),
                            host: host.to_owned(),
                        });
                    }
                    if !registry.db_grant_row_exists(user, host, &database) {
                        return Err(DriverError::RevokeNoDbGrant {
                            user: user.to_owned(),
                            host: host.to_owned(),
                            database: database.clone(),
                        });
                    }
                    registry.revoke_db(user, host, &database, mask);
                }
            }
            tidb_ast::GrantLevel::Table { database, table } => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let privs = self.resolve_scoped_privs(&revoke.privileges, ScopeKind::Table)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit());
                for spec in &revoke.users {
                    let user = spec.user.user.as_str();
                    let host = spec.user.host.as_str();
                    if !registry.user_exists(user, host) {
                        return Err(DriverError::RevokeUnknownUser {
                            user: user.to_owned(),
                            host: host.to_owned(),
                        });
                    }
                    if !registry.table_grant_row_exists(user, host, &database, table) {
                        return Err(DriverError::RevokeNoTableGrant {
                            user: user.to_owned(),
                            host: host.to_owned(),
                            database: database.clone(),
                            table: table.clone(),
                        });
                    }
                    registry.revoke_table(user, host, &database, table, mask);
                }
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Resolves a DB/TABLE-scope `GRANT`/`REVOKE`'s database qualifier: the
    /// written name, or (Go's `getTargetSchemaName`) the session's current
    /// database when the statement wrote a bare `*`/table name.
    fn resolve_grant_database(&self, database: Option<&str>) -> Result<String, DriverError> {
        match database {
            Some(database) => Ok(database.to_owned()),
            None if !self.current_db.is_empty() => Ok(self.current_db.clone()),
            None => Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected)),
        }
    }

    /// Resolves a `GRANT`/`REVOKE` privilege list at DB or TABLE scope,
    /// validating that every privilege is one Go's `mysql.AllDBPrivs`/
    /// `mysql.AllTablePrivs` allows there. `ALL [PRIVILEGES]` expands to
    /// every privilege valid at that scope. A global-only privilege at DB
    /// scope is refused with the captured `ErrWrongUsage`/1221; any
    /// privilege outside the TABLE-scope set is refused with the captured
    /// `ErrIllegalGrantForTable`/1144 (Go checks the TABLE-scope validity
    /// before the table-existence check, so this runs first here too).
    fn resolve_scoped_privs(
        &self,
        privileges: &[tidb_ast::GrantPrivilege],
        scope: ScopeKind,
    ) -> Result<Vec<privilege::GlobalPriv>, DriverError> {
        let all_scoped: &[privilege::GlobalPriv] = match scope {
            ScopeKind::Database => privilege::ALL_DB_PRIVS,
            ScopeKind::Table => privilege::ALL_TABLE_PRIVS,
        };
        let mut result = Vec::new();
        for privilege in privileges {
            if privilege.name == "ALL" {
                result.extend_from_slice(all_scoped);
                continue;
            }
            if !privilege.columns.is_empty() {
                return Err(DriverError::Unsupported(
                    "GRANT/REVOKE with a column list is not supported yet",
                ));
            }
            let Some(priv_) = privilege::GlobalPriv::from_grant_name(&privilege.name) else {
                return Err(DriverError::DynamicPrivilegeNotRegistered(
                    privilege.name.clone(),
                ));
            };
            if privilege.dynamic {
                return Err(DriverError::DynamicPrivilegeNotRegistered(
                    privilege.name.clone(),
                ));
            }
            let valid = match scope {
                ScopeKind::Database => priv_.is_valid_at_db_scope(),
                ScopeKind::Table => priv_.is_valid_at_table_scope(),
            };
            if !valid {
                return Err(match scope {
                    ScopeKind::Database => DriverError::DbGrantGlobalOnlyPriv,
                    ScopeKind::Table => DriverError::IllegalGrantForTable,
                });
            }
            result.push(priv_);
        }
        Ok(result)
    }

    /// Resolves a `GRANT`/`REVOKE` privilege list to the bitmask this tier's
    /// registry stores. `ALL [PRIVILEGES]` expands to every modeled global
    /// privilege (Go: `mysql.AllGlobalPrivs`, minus the roles/GRANT OPTION
    /// this tier does not model). A name that is not one of the standard
    /// privileges this tier recognizes is refused with the same error Go
    /// raises for an unregistered dynamic privilege (captured: 3929),
    /// because `tidb-parser` accepts any bare identifier there through its
    /// `ExtendedPriv`/dynamic-privilege grammar branch.
    fn resolve_global_priv_mask(
        &self,
        privileges: &[tidb_ast::GrantPrivilege],
    ) -> Result<u64, DriverError> {
        let mut mask = 0u64;
        for privilege in privileges {
            if privilege.name == "ALL" {
                mask |= privilege::all_privs_mask();
                continue;
            }
            if !privilege.columns.is_empty() {
                return Err(DriverError::Unsupported(
                    "GRANT/REVOKE with a column list is not supported yet",
                ));
            }
            match privilege::GlobalPriv::from_grant_name(&privilege.name) {
                Some(priv_) if !privilege.dynamic => mask |= priv_.bit(),
                _ => {
                    return Err(DriverError::DynamicPrivilegeNotRegistered(
                        privilege.name.clone(),
                    ));
                }
            }
        }
        Ok(mask)
    }

    /// `SHOW GRANTS [FOR <user>]` at GLOBAL scope. `USING <roles>` is refused
    /// rather than silently ignored, since active-role expansion is not
    /// modeled here.
    fn show_grants_stmt(
        &mut self,
        show: &tidb_ast::ShowGrantsStmt,
    ) -> Result<StmtOutput, DriverError> {
        if !show.roles.is_empty() {
            return Err(DriverError::Unsupported(
                "SHOW GRANTS ... USING is not supported yet",
            ));
        }
        let (user, host) = match &show.user {
            None => {
                let Some((user, host)) = self.current_identity() else {
                    return Err(DriverError::Unsupported(
                        "SHOW GRANTS requires an authenticated session",
                    ));
                };
                (user.to_owned(), host.to_owned())
            }
            Some(spec) if spec.current_user => {
                let Some((user, host)) = self.current_identity() else {
                    return Err(DriverError::Unsupported(
                        "SHOW GRANTS requires an authenticated session",
                    ));
                };
                (user.to_owned(), host.to_owned())
            }
            Some(spec) => (spec.user.clone(), spec.host.clone()),
        };
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "SHOW GRANTS requires a server front end with a privilege registry",
            ));
        };
        let Some(lines) = registry.show_grants(&user, &host) else {
            return Err(DriverError::NonexistingGrant { user, host });
        };
        // Go: `fmt.Sprintf("Grants for %s", s.User)` -- `s.User.String()` is
        // unquoted `user@host`. One row per GLOBAL/DB/TABLE-scope line, in
        // that order (`registry.show_grants`'s captured ordering).
        Ok(string_column_output(
            &format!("Grants for {user}@{host}"),
            lines.split('\n').map(str::to_owned).collect(),
        ))
    }

    /// Records the connection identifier `CONNECTION_ID()` reports, which Go
    /// sets on `SessionVars.ConnectionID` when the front end opens the
    /// connection. `attach_process` sets it too; this exists for a front end
    /// that has an id but no process registry.
    pub fn set_connection_id(&mut self, connection_id: u64) {
        self.connection_id = Some(connection_id);
    }

    /// Go `SessionVars.ConnectionID`, which `CONNECTION_ID()` reports; zero
    /// for a session no front end opened.
    #[must_use]
    pub fn connection_id(&self) -> u64 {
        self.connection_id.unwrap_or(0)
    }

    /// Go `serverStatus2Str` over this session's status bits: the `State`
    /// column of `SHOW PROCESSLIST`.
    ///
    /// This tier's connections are always autocommit and set no other status
    /// bit, so the text is `in transaction; autocommit` inside an explicit
    /// transaction and `autocommit` outside one -- exactly the order Go's
    /// `ascServerStatus` produces for those bits.
    #[must_use]
    pub fn status_text(&self) -> String {
        if self.txn.is_some() {
            "in transaction; autocommit".to_owned()
        } else {
            "autocommit".to_owned()
        }
    }

    /// The number of `?` markers a statement carries, which
    /// `COM_STMT_PREPARE` reports to the client.
    pub fn parameter_count(&self, sql: &str) -> Result<usize, DriverError> {
        tidb_executor::parameter_count(sql)
    }

    /// Runs one statement with its prepared-statement parameters bound.
    ///
    /// Go installs the execute-time values on the parsed statement's own
    /// markers; this tier reaches execution through SQL text, so the markers
    /// become literals and the statement is restored before it runs. A byte
    /// string that is not UTF-8 becomes a hex literal, so no value is lost in
    /// that round trip.
    pub fn run_with_params(
        &mut self,
        sql: &str,
        params: &[Datum],
    ) -> Result<StmtOutput, DriverError> {
        // The count is checked even when no values were sent, so a statement
        // with an unbound marker is Go's ErrWrongParamCount rather than a
        // parse-time surprise deeper in.
        if params.is_empty() && self.parameter_count(sql)? == 0 {
            return self.run_with_columns(sql);
        }
        let bound = tidb_executor::bind_parameters(sql, params)?;
        self.run_with_columns(&bound)
    }

    /// Runs one SQL statement (Go `session.ExecuteStmt`): parses, dispatches by
    /// statement kind, and executes over the session catalog.
    pub fn run(&mut self, sql: &str) -> Result<StmtResult, DriverError> {
        Ok(match self.run_with_columns(sql)? {
            StmtOutput::Rows { rows, .. } => StmtResult::Rows(rows),
            StmtOutput::Affected(count) => StmtResult::Affected(count),
            StmtOutput::Done(created) => StmtResult::Done(created),
        })
    }

    /// Like [`Session::run`], but a query result also carries its column
    /// metadata (`(name, type)` per column) for wire-protocol fronts.
    ///
    /// Captured from TiDB: a statement that fails leaves its own error in the
    /// warning buffer as an `Error`-level row, so `SHOW WARNINGS` right after
    /// a failure reports it.
    pub fn run_with_columns(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        // A statement is visible to a peer's SHOW PROCESSLIST for exactly as
        // long as it runs, which is why the process list is updated here --
        // the one door every statement of this session goes through -- rather
        // than in one front end's command loop.
        if let Some(guard) = &self.process {
            guard
                .registry()
                .statement_started(guard.id(), sql, &self.status_text());
        }
        let result = self.execute_statement(sql);
        if let Some(guard) = &self.process {
            guard
                .registry()
                .statement_finished(guard.id(), &self.current_db, &self.status_text());
        }
        if let Err(error) = &result {
            let reported = error.clone().to_mysql_error();
            self.warnings.push(SqlWarning {
                level: WarningLevel::Error,
                code: reported.code,
                message: reported.message,
            });
        }
        result
    }

    fn execute_statement(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        // Go clears the warning buffer when a statement starts, so what
        // `SHOW WARNINGS` reports always belongs to the statement before it --
        // which is why those two statements must not clear it themselves.
        if !reports_warnings(sql) {
            self.warnings.clear();
        }
        // USE / CREATE DATABASE / DROP DATABASE / SHOW DATABASES / SHOW TABLES.
        if let Some(output) = self.apply_schema_statement(sql)? {
            return Ok(output);
        }
        // BEGIN / COMMIT / ROLLBACK and SET both have their own entry points
        // for the wire front, which answers them with an OK packet carrying
        // a status flag. Routing them here too makes `run` the single door
        // every statement can go through, which is what a client expects of
        // one connection.
        if self.control_transaction(sql)?.is_some() {
            return Ok(StmtOutput::Affected(0));
        }
        if self.apply_set(sql)?.is_some() {
            return Ok(StmtOutput::Affected(0));
        }
        let mut stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        // `@@x` / `@x` read the session's own state, so they are bound before
        // the statement reaches the driver.
        self.bind_variables(&mut stmt)?;
        // Only an allocating INSERT sets it; every other statement reports 0.
        self.statement_insert_id = 0;
        // Go's preprocessor runs before planning, so a gated clause is
        // refused before any table is touched.
        if let Stmt::Query(query) = &stmt {
            self.check_noop_functions(query)?;
            self.check_query_clauses(query)?;
        }
        // Go raises ErrNoDB where an unqualified NAME is resolved, not for
        // every statement: `SELECT 1` and `SELECT DATABASE()` both run with
        // no database selected (captured). The driver's own
        // `split_table_path` raises it at the resolution point, which is
        // where Go's does.
        match &stmt {
            Stmt::Query(query) => {
                let tidb_ast::QueryStmt::Select(select) = &**query else {
                    // A set operation runs through its own fold.
                    let tidb_ast::QueryStmt::SetOpr(set_opr) = &**query else {
                        unreachable!("a query is a SELECT or a set operation")
                    };
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(false);
                    let (columns, rows) = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_set_opr_stmt(set_opr, catalog, &current_db, &ctx)
                    })?;
                    self.drain_eval_warnings(&ctx);
                    return Ok(StmtOutput::Rows { columns, rows });
                };
                // An information_schema table is virtual: its rows are
                // computed from the catalog rather than read from storage.
                if let Some(output) = self.run_information_schema_select(select)? {
                    return Ok(output);
                }
                let current_db = self.current_db.clone();
                let ctx = self.statement_context(false);
                let (columns, rows) = self.with_catalog_mut(|catalog| {
                    tidb_executor::run_select_meta_stmt(select, catalog, &current_db, &ctx)
                })?;
                self.drain_eval_warnings(&ctx);
                Ok(StmtOutput::Rows { columns, rows })
            }
            Stmt::Dml(dml) => match &**dml {
                DmlStmt::Insert(_) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(true);
                    let (affected, allocated) = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_insert_reporting(sql, catalog, &current_db, &ctx)
                    })?;
                    self.drain_eval_warnings(&ctx);
                    self.statement_insert_id = allocated.unwrap_or(0).max(0) as u64;
                    if let Some(allocated) = allocated {
                        self.last_insert_id = allocated.max(0) as u64;
                    }
                    Ok(StmtOutput::Affected(affected))
                }
                DmlStmt::Update(_) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(true);
                    let output = self.with_catalog_mut(|catalog| {
                        Ok(StmtOutput::Affected(tidb_executor::run_update_in(
                            sql,
                            catalog,
                            &current_db,
                            &ctx,
                        )?))
                    });
                    self.drain_eval_warnings(&ctx);
                    output
                }
                DmlStmt::Delete(_) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(true);
                    let output = self.with_catalog_mut(|catalog| {
                        Ok(StmtOutput::Affected(tidb_executor::run_delete_in(
                            sql,
                            catalog,
                            &current_db,
                            &ctx,
                        )?))
                    });
                    self.drain_eval_warnings(&ctx);
                    output
                }
                _ => Err(DriverError::Unsupported(
                    "this DML statement kind is not supported yet",
                )),
            },
            Stmt::Ddl(ddl) => match &**ddl {
                DdlStmt::RenameTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_rename_table_in(sql, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::TruncateTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_truncate_table_in(sql, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::CreateIndex(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_create_index_in(sql, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::DropIndex(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_index_in(sql, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::AlterTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_alter_table_in(sql, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::DropTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_table_in(sql, catalog, &current_db)?;
                        // MySQL answers DDL with a zero affected-row count.
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::CreateTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        Ok(StmtOutput::Done(tidb_executor::run_create_table_in(
                            sql,
                            catalog,
                            &current_db,
                        )?))
                    })
                }
                DdlStmt::CreateView(create) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(false);
                    let create = create.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_create_view_in(&create, catalog, &current_db, &ctx)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::DropView { if_exists, names } => {
                    let current_db = self.current_db.clone();
                    let (if_exists, names) = (*if_exists, names.clone());
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_view_in(if_exists, &names, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                _ => Err(DriverError::Unsupported(
                    "this DDL statement kind is not supported yet",
                )),
            },
            _ => Err(DriverError::Unsupported(
                "this statement kind is not supported yet",
            )),
        }
    }
}

/// The `SHOW TABLE STATUS` header, with the columns Go reports as numbers
/// marked.
const SHOW_TABLE_STATUS_COLUMNS: &[(&str, bool)] = &[
    ("Name", false),
    ("Engine", false),
    ("Version", true),
    ("Row_format", false),
    ("Rows", true),
    ("Avg_row_length", true),
    ("Data_length", true),
    ("Max_data_length", true),
    ("Index_length", true),
    ("Data_free", true),
    ("Auto_increment", true),
    ("Create_time", false),
    ("Update_time", false),
    ("Check_time", false),
    ("Collation", false),
    ("Checksum", false),
    ("Create_options", false),
    ("Comment", false),
];

/// One `SHOW TABLE STATUS` row. The sizes and counts this tier has no source
/// for are zero, which is what TiDB itself reports without statistics.
fn show_table_status_row(name: &str, auto_increment: Option<i64>) -> Vec<Datum> {
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    vec![
        text(name),
        text("InnoDB"),
        Datum::Int(10),
        text("Compact"),
        Datum::Int(0), // Rows
        Datum::Int(0), // Avg_row_length
        Datum::Int(0), // Data_length
        Datum::Int(0), // Max_data_length
        Datum::Int(0), // Index_length
        Datum::Int(0), // Data_free
        match auto_increment {
            Some(next) => Datum::Int(next),
            None => Datum::Null,
        },
        Datum::Null, // Create_time: no per-table creation timestamp here.
        Datum::Null, // Update_time
        Datum::Null, // Check_time
        text(TABLE_COLLATE),
        text(""), // Checksum
        text(""), // Create_options
        text(""), // Comment
    ]
}

/// One `SHOW TABLE STATUS` row for a view. Captured from Go: a view answers
/// its name, NULL for every storage cell -- engine, version, row format,
/// counts, sizes, collation and create options alike -- an empty `Checksum`,
/// and the literal `VIEW` as its comment, which is how the two kinds of
/// object are told apart in this output.
fn show_table_status_view_row(name: &str) -> Vec<Datum> {
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let mut row = vec![text(name)];
    // Engine through Auto_increment: ten cells a view has no value for.
    row.extend(std::iter::repeat_n(Datum::Null, 10));
    // Create_time, which Go fills and this tier has no source for, then
    // Update_time, Check_time and Collation, which are NULL for a view in Go
    // too.
    row.extend(std::iter::repeat_n(Datum::Null, 4));
    row.push(text("")); // Checksum
    row.push(Datum::Null); // Create_options
    row.push(text("VIEW")); // Comment
    row
}

/// `SHOW CHARSET` rows: `(Charset, Description, Default collation, Maxlen)`,
/// captured verbatim from mock TiDB's `charset.GetSupportedCharsets`. Order
/// matches the capture (alphabetical by charset name).
const SHOW_CHARSET_ROWS: &[(&str, &str, &str, i64)] = &[
    ("ascii", "US ASCII", "ascii_bin", 1),
    ("binary", "binary", "binary", 1),
    (
        "gb18030",
        "China National Standard GB18030",
        "gb18030_chinese_ci",
        4,
    ),
    (
        "gbk",
        "Chinese Internal Code Specification",
        "gbk_chinese_ci",
        2,
    ),
    ("latin1", "Latin1", "latin1_bin", 1),
    ("utf8", "UTF-8 Unicode", "utf8_bin", 3),
    ("utf8mb4", "UTF-8 Unicode", "utf8mb4_bin", 4),
];

/// The collations `SHOW COLLATION` reports, in mock TiDB's own capture order
/// (alphabetical by collation name). `Utf8Mb4ZhPinyinTiDbAsCs` is
/// deliberately excluded: it is a reserved stub collation, and Go's own
/// `SHOW COLLATION` capture omits it too.
const SHOW_COLLATION_ROWS: &[tidb_datatype::Collation] = &[
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
/// This is NOT the same as [`tidb_datatype::Charset::default_collation`]:
/// mock TiDB's capture shows `gbk_chinese_ci`/`gb18030_chinese_ci` as the
/// default for their charsets, not the `_bin` collations that method
/// returns, so the SHOW COLLATION default is listed explicitly here rather
/// than derived from it.
fn is_default_show_collation(collation: tidb_datatype::Collation) -> bool {
    matches!(
        collation,
        tidb_datatype::Collation::AsciiBin
            | tidb_datatype::Collation::Binary
            | tidb_datatype::Collation::Gb18030ChineseCi
            | tidb_datatype::Collation::GbkChineseCi
            | tidb_datatype::Collation::Latin1Bin
            | tidb_datatype::Collation::Utf8Bin
            | tidb_datatype::Collation::Utf8Mb4Bin
    )
}

/// The `SHOW INDEX` header, with the columns Go reports as numbers marked.
const SHOW_INDEX_COLUMNS: &[(&str, bool)] = &[
    ("Table", false),
    ("Non_unique", true),
    ("Key_name", false),
    ("Seq_in_index", true),
    ("Column_name", false),
    ("Collation", false),
    ("Cardinality", true),
    ("Sub_part", true),
    ("Packed", false),
    ("Null", false),
    ("Index_type", false),
    ("Comment", false),
    ("Index_comment", false),
    ("Visible", false),
    ("Expression", false),
    ("Clustered", false),
    ("Global", false),
];

/// One `SHOW INDEX` row per index column, in Go's own order: the clustered
/// primary key first, then each index in definition order.
fn show_index_rows(table_name: &str, table: &tidb_executor::KvTable) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let mut push = |key_name: &str,
                    unique: bool,
                    clustered: bool,
                    sequence: usize,
                    column: &str,
                    nullable: bool| {
        rows.push(vec![
            text(table_name),
            Datum::Int(i64::from(!unique)),
            text(key_name),
            Datum::Int(sequence as i64),
            text(column),
            text("A"),
            // No statistics tier, so Go's estimate is simply absent.
            Datum::Int(0),
            Datum::Null,
            Datum::Null,
            text(if nullable { "YES" } else { "" }),
            text("BTREE"),
            text(""),
            text(""),
            text("YES"),
            Datum::Null,
            text(if clustered { "YES" } else { "NO" }),
            text("NO"),
        ]);
    };
    // The clustered primary key is not in the index list, the same way
    // SHOW CREATE TABLE prints it separately.
    if let Some(offset) = table.pk_handle_offset() {
        push("PRIMARY", true, true, 1, &table.columns[offset].name, false);
    }
    for index in table.indexes() {
        let clustered =
            index.name.eq_ignore_ascii_case("PRIMARY") && !table.common_handle_offsets().is_empty();
        for (position, offset) in index.column_offsets.iter().enumerate() {
            let column = &table.columns[*offset];
            let nullable = column.field_type.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL == 0;
            push(
                &index.name,
                index.unique,
                clustered,
                position + 1,
                &column.name,
                nullable,
            );
        }
    }
    rows
}

/// The column names a `SHOW VARIABLES` row carries, which its `WHERE` filter
/// resolves against.
const SHOW_VARIABLE_COLUMNS: &[&str; 2] = &["Variable_name", "Value"];

/// The status variables this tier truthfully reports for `SHOW STATUS`, as
/// `(name, value, session_only)`, in row order.
///
/// The values are Go's captured defaults for a plain (no-TLS, no-compression)
/// connection, which is exactly what this tier is: no wire compression, so
/// `Compression` is `OFF`, and no TLS, so the `Ssl_*` family is empty/`0`.
/// The `session_only` flag mirrors Go's `vardef.ScopeSession`, which
/// `fetchShowStatus` uses to drop rows from `SHOW GLOBAL STATUS`.
///
/// NOT modelled (this tier has no metrics/server tier to read them from):
/// the `Performance_schema_session_connect_attrs_*` counters,
/// `ddl_schema_version`, `server_id`, `last_plan_binding_update_time`, and
/// `tidb_keys_examined`.
const SHOW_STATUS_VARS: &[(&str, &str, bool)] = &[
    ("Compression", "OFF", true),
    ("Compression_algorithm", "", true),
    ("Compression_level", "0", true),
    ("Ssl_cipher", "", false),
    ("Ssl_cipher_list", "", false),
    ("Ssl_verify_mode", "0", false),
    ("Ssl_version", "", false),
];

/// A resolver over one row of a virtual `SHOW` result, so the statement's own
/// `WHERE` can be evaluated against it.
///
/// Go builds the same thing as a real selection over the show output; this
/// tier evaluates the predicate per row instead, which is the same filter
/// without a plan to carry it.
struct ShowRowResolver<'a> {
    columns: &'a [&'a str],
    row: &'a [Datum],
}

impl tidb_executor::Columns for ShowRowResolver<'_> {
    fn get(&self, path: &[String]) -> Option<Datum> {
        let name = path.last()?;
        let index = self
            .columns
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))?;
        self.row.get(index).cloned()
    }
}

/// Whether one virtual `SHOW` row satisfies the statement's `WHERE`.
fn show_row_matches(
    predicate: &tidb_ast::Expr,
    columns: &[&str],
    row: &[Datum],
) -> Result<bool, DriverError> {
    let resolver = ShowRowResolver { columns, row };
    let value = tidb_executor::eval_in(predicate, &resolver)
        .map_err(|e| DriverError::Exec(tidb_executor::ExecError::Eval(e)))?;
    let truthy = tidb_executor::truthy_of(&value)
        .map_err(|e| DriverError::Exec(tidb_executor::ExecError::Eval(e)))?;
    Ok(truthy.unwrap_or(false))
}

/// A statement warning, which Go keeps in `StmtCtx` and `SHOW WARNINGS`
/// reports as `Level | Code | Message`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlWarning {
    /// Whether the statement survived it.
    pub level: WarningLevel,
    /// The MySQL error code the warning carries.
    pub code: u16,
    /// The message text.
    pub message: String,
}

/// A warning's `Level` column, which Go fills from
/// `StmtCtx.warnings[i].Level`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarningLevel {
    /// The statement continued.
    Warning,
    /// The statement failed; Go records its error in the same buffer.
    Error,
}

impl WarningLevel {
    /// The text the `Level` column shows.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            WarningLevel::Warning => "Warning",
            WarningLevel::Error => "Error",
        }
    }
}

/// Which non-global `GRANT`/`REVOKE` scope a privilege list is being
/// validated against -- selects between Go's `mysql.AllDBPrivs` and
/// `mysql.AllTablePrivs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScopeKind {
    /// `ON db.*`.
    Database,
    /// `ON db.t`.
    Table,
}

/// Go `variable.NoopFuncsMode`: how a clause TiDB only implements as a
/// no-op is treated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NoopFuncsMode {
    /// `OFF` (the default): the statement is refused.
    Off,
    /// `ON`: the clause is accepted and does nothing.
    On,
    /// `WARN`: the clause is accepted with a warning.
    Warn,
}

impl Session {
    /// `SHOW WARNINGS` / `SHOW ERRORS` output: one row per buffered warning,
    /// or the count when the source wrote `SHOW COUNT(*) WARNINGS`.
    ///
    /// Captured from TiDB: the columns are `Level`, `Code`, `Message`; the
    /// count form returns a single `@@session.warning_count` column; and
    /// `SHOW ERRORS` shows only the `Error`-level rows.
    /// The rows of `SHOW [FULL] PROCESSLIST`.
    ///
    /// With a server front end this is the whole live connection list. A
    /// session with NO front end (in-process tests, the embedded driver) has
    /// no peers to report, so it lists exactly one row: itself, with the
    /// values it honestly knows -- its own connection id (0 when the front
    /// end never assigned one), no client host, its current schema, and the
    /// statement it is running, which is this SHOW.
    ///
    /// Filtered by the `PROCESS` privilege the same way Go's
    /// `setDataForProcessList` / `fetchShowProcessList` both filter: a
    /// session without it sees only its own connections.
    fn process_list_output(&self, full: bool) -> StmtOutput {
        let rows = self.visible_process_rows(full);
        let text = || FieldType::new(tidb_datatype::FieldTypeCode::Varchar);
        let nullable_text = |value: String| {
            if value.is_empty() {
                Datum::Null
            } else {
                Datum::Bytes(value.into_bytes())
            }
        };
        StmtOutput::Rows {
            columns: vec![
                (
                    "Id".to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                ),
                ("User".to_owned(), text()),
                ("Host".to_owned(), text()),
                ("db".to_owned(), text()),
                ("Command".to_owned(), text()),
                (
                    "Time".to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::Long),
                ),
                ("State".to_owned(), text()),
                (
                    "Info".to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::String),
                ),
            ],
            rows: rows
                .into_iter()
                .map(|row| {
                    vec![
                        Datum::UInt(row.id),
                        Datum::Bytes(row.user.into_bytes()),
                        Datum::Bytes(row.host.into_bytes()),
                        // Go reports an unselected schema as SQL NULL.
                        nullable_text(row.db),
                        Datum::Bytes(row.command.into_bytes()),
                        Datum::Int(i64::try_from(row.time).unwrap_or(i64::MAX)),
                        Datum::Bytes(row.state.into_bytes()),
                        // Go reports an idle connection's statement as NULL,
                        // and truncates a running one to 100 runes without
                        // FULL.
                        match row.info {
                            Some(info) => Datum::Bytes(
                                process::truncate_process_info(&info, full).into_bytes(),
                            ),
                            None => Datum::Null,
                        },
                    ]
                })
                .collect(),
        }
    }

    /// The `User` column: Go reports the bare user name, while this session
    /// stores the login identity as `user@host`.
    fn process_list_user(&self) -> String {
        match &self.login_user {
            Some(user) => user.split('@').next().unwrap_or_default().to_owned(),
            None => String::new(),
        }
    }

    /// Every connection this session is allowed to see for `SHOW
    /// PROCESSLIST` / `information_schema.PROCESSLIST`.
    ///
    /// Go (`setDataForProcessList`, `fetchShowProcessList`): "If you have the
    /// PROCESS privilege, you can see all threads. Otherwise, you can see
    /// only your own threads" -- and an internal session with no login user
    /// is not filtered at all, since there is nothing to compare against.
    fn visible_process_rows(&self, full: bool) -> Vec<process::ProcessRow> {
        let rows: Vec<process::ProcessRow> = match &self.process {
            Some(guard) => guard.registry().snapshot(),
            None => vec![process::ProcessRow {
                id: self.connection_id.unwrap_or(0),
                user: self.process_list_user(),
                host: String::new(),
                db: self.current_db.clone(),
                command: "Query".to_owned(),
                time: 0,
                state: self.status_text(),
                info: Some(if full {
                    "show full processlist".to_owned()
                } else {
                    "show processlist".to_owned()
                }),
            }],
        };
        let has_process_via_registry = self.privileges.as_ref().is_some_and(|registry| {
            self.current_identity().is_some_and(|(user, host)| {
                registry.has_global_priv(user, host, privilege::GlobalPriv::Process)
            })
        });
        if self.has_process_priv || has_process_via_registry || self.login_user.is_none() {
            return rows;
        }
        let me = self.process_list_user();
        rows.into_iter().filter(|row| row.user == me).collect()
    }

    /// `SELECT * FROM information_schema.PROCESSLIST` rows, in the exact
    /// column order Go's `tableProcesslistCols` / `ProcessInfo.ToRow` build
    /// (CAPTURED: `ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO, DIGEST,
    /// MEM, MEM_ARBITRATION, MEM_WAIT_ARBITRATE_START,
    /// MEM_WAIT_ARBITRATE_BYTES, DISK, TxnStart, RESOURCE_GROUP,
    /// SESSION_ALIAS, ROWS_AFFECTED, TIDB_CPU, TIKV_CPU`).
    ///
    /// `ToRow` builds on `ToRowForShow(true)`, i.e. `INFO` is never truncated
    /// here (unlike `SHOW PROCESSLIST` without `FULL`).
    ///
    /// NOT MODELLED (this tier tracks none of these per connection, so each
    /// is Go's own value for a connection with no live statement context --
    /// `RefCountOfStmtCtx` fails to increase -- rather than an invented one):
    /// `DIGEST` is `""`, `MEM`/`DISK`/`TIDB_CPU`/`TIKV_CPU` are `0`,
    /// `MEM_ARBITRATION`/`MEM_WAIT_ARBITRATE_START`/
    /// `MEM_WAIT_ARBITRATE_BYTES`/`ROWS_AFFECTED` are `NULL`, and
    /// `TxnStart`/`RESOURCE_GROUP`/`SESSION_ALIAS` are `""`.
    fn process_list_table_rows(&self) -> Vec<Vec<Datum>> {
        self.visible_process_rows(true)
            .into_iter()
            .map(|row| {
                vec![
                    Datum::UInt(row.id),
                    Datum::Bytes(row.user.into_bytes()),
                    Datum::Bytes(row.host.into_bytes()),
                    if row.db.is_empty() {
                        Datum::Null
                    } else {
                        Datum::Bytes(row.db.into_bytes())
                    },
                    Datum::Bytes(row.command.into_bytes()),
                    Datum::Int(i64::try_from(row.time).unwrap_or(i64::MAX)),
                    if row.state.is_empty() {
                        Datum::Null
                    } else {
                        Datum::Bytes(row.state.into_bytes())
                    },
                    match row.info {
                        Some(info) => Datum::Bytes(info.into_bytes()),
                        None => Datum::Null,
                    },
                    // DIGEST
                    Datum::Bytes(Vec::new()),
                    // MEM
                    Datum::UInt(0),
                    // MEM_ARBITRATION
                    Datum::Null,
                    // MEM_WAIT_ARBITRATE_START
                    Datum::Null,
                    // MEM_WAIT_ARBITRATE_BYTES
                    Datum::Null,
                    // DISK
                    Datum::UInt(0),
                    // TxnStart
                    Datum::Bytes(Vec::new()),
                    // RESOURCE_GROUP
                    Datum::Bytes(Vec::new()),
                    // SESSION_ALIAS
                    Datum::Bytes(Vec::new()),
                    // ROWS_AFFECTED
                    Datum::Null,
                    // TIDB_CPU
                    Datum::Int(0),
                    // TIKV_CPU
                    Datum::Int(0),
                ]
            })
            .collect()
    }

    fn warning_output(&self, count_only: bool, errors_only: bool) -> StmtOutput {
        let reported = self
            .warnings
            .iter()
            .filter(|warning| !errors_only || warning.level == WarningLevel::Error);
        if count_only {
            let count = reported.count() as i64;
            let name = if errors_only {
                "@@session.error_count"
            } else {
                "@@session.warning_count"
            };
            return StmtOutput::Rows {
                columns: vec![(
                    name.to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )],
                rows: vec![vec![Datum::Int(count)]],
            };
        }
        let text = || FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let rows = reported
            .map(|warning| {
                vec![
                    Datum::Bytes(warning.level.as_str().as_bytes().to_vec()),
                    Datum::Int(i64::from(warning.code)),
                    Datum::Bytes(warning.message.clone().into_bytes()),
                ]
            })
            .collect();
        StmtOutput::Rows {
            columns: vec![
                ("Level".to_owned(), text()),
                (
                    "Code".to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                ),
                ("Message".to_owned(), text()),
            ],
            rows,
        }
    }

    /// Go `timeutil.ParseTimeZone`: `SYSTEM` is the host zone, a named zone
    /// comes from the zone database, and a `+HH:MM`/`-HH:MM` string is a
    /// fixed offset bounded to `[-12:59, +14:00]`.
    ///
    /// An unparseable value falls back to the host zone rather than failing
    /// the statement, because this tier accepts the variable without
    /// validating it at SET time -- Go validates there instead, and that
    /// check is the deferred half of this port.
    fn session_time_zone(&self) -> tidb_executor::SessionTimeZone {
        use tidb_executor::SessionTimeZone;
        let written = self
            .vars
            .get_system("time_zone")
            .unwrap_or_else(|_| "SYSTEM".to_owned());
        if !written.eq_ignore_ascii_case("SYSTEM") {
            if let Ok(zone) = written.parse::<chrono_tz::Tz>() {
                return SessionTimeZone::Named(zone);
            }
            if let Some(rest) = written.strip_prefix(['+', '-']) {
                let negative = written.starts_with('-');
                let mut parts = rest.split(':');
                let hours: i32 = parts.next().unwrap_or_default().parse().unwrap_or(-1);
                let minutes: i32 = parts.next().unwrap_or("0").parse().unwrap_or(-1);
                if hours >= 0 && (0..60).contains(&minutes) {
                    let offset = hours * 3600 + minutes * 60;
                    let bounded = if negative {
                        offset <= 12 * 3600 + 59 * 60
                    } else {
                        offset <= 14 * 3600
                    };
                    if bounded {
                        return SessionTimeZone::Fixed {
                            name: written.clone(),
                            offset_secs: if negative { -offset } else { offset },
                        };
                    }
                }
            }
        }
        // SYSTEM: the host's own zone, which is what Go's SystemLocation is.
        let local = chrono::Local::now();
        SessionTimeZone::Fixed {
            name: "System".to_owned(),
            offset_secs: chrono::Offset::fix(local.offset()).local_minus_utc(),
        }
    }

    /// The instant every `NOW()` in one statement shares, which Go fixes on
    /// the statement context.
    fn statement_clock(&self, zone: &tidb_executor::SessionTimeZone) -> (i64, u32, i32) {
        use tidb_executor::SessionTimeZone;
        let utc = chrono::Utc::now();
        let seconds = utc.timestamp();
        let nanos = utc.timestamp_subsec_nanos();
        let offset = match zone {
            SessionTimeZone::Fixed { offset_secs, .. } => *offset_secs,
            SessionTimeZone::Named(zone) => {
                use chrono::TimeZone;
                chrono::Offset::fix(&zone.offset_from_utc_datetime(&utc.naive_utc()))
                    .local_minus_utc()
            }
        };
        (seconds, nanos, offset)
    }

    /// The evaluation context for one statement, which is Go's
    /// `StatementContext`.
    ///
    /// The division-by-zero level is the only group modelled so far: Go warns
    /// for a query, and for a DML statement resolves it from `sql_mode` --
    /// without `ERROR_FOR_DIVISION_BY_ZERO` the condition is ignored, a
    /// non-strict mode warns, and the default strict mode fails the statement.
    fn statement_context(&self, is_dml: bool) -> tidb_executor::StmtContext {
        // Go hands the same `SessionVars` to every expression, which is where
        // `DATABASE()` and `VERSION()` read from.
        let current_db = if self.current_db.is_empty() {
            None
        } else {
            Some(self.current_db.clone())
        };
        let version = self.vars.get_system("version").ok();
        let zone = self.session_time_zone();
        let clock = self.statement_clock(&zone);
        if !is_dml {
            return tidb_executor::StmtContext::for_query()
                .with_session_state(current_db, version)
                .with_user(self.current_user.clone(), self.login_user.clone())
                .with_connection_id(self.connection_id)
                .with_rand_session(Rc::clone(&self.rand))
                .with_clock(clock, zone);
        }
        let mode = self
            .vars
            .get_system("sql_mode")
            .unwrap_or_default()
            .to_ascii_uppercase();
        let has = |flag: &str| mode.split(',').any(|part| part.trim() == flag);
        tidb_executor::StmtContext::for_dml(
            has("ERROR_FOR_DIVISION_BY_ZERO"),
            has("STRICT_TRANS_TABLES") || has("STRICT_ALL_TABLES"),
        )
        .with_session_state(current_db, version)
        .with_user(self.current_user.clone(), self.login_user.clone())
        .with_connection_id(self.connection_id)
        .with_rand_session(Rc::clone(&self.rand))
        .with_clock(clock, zone)
    }

    /// Moves what evaluation recorded into the statement's warning buffer.
    fn drain_eval_warnings(&mut self, ctx: &tidb_executor::StmtContext) {
        for (code, message) in ctx.take_warnings() {
            self.warnings.push(SqlWarning {
                level: WarningLevel::Warning,
                code,
                message,
            });
        }
    }

    /// The warnings the last statement produced.
    #[must_use]
    pub fn warnings(&self) -> &[SqlWarning] {
        &self.warnings
    }

    /// The query clauses this tier parses but cannot execute.
    ///
    /// `INTO OUTFILE` writes a server-side file, which this seed has no path
    /// for; Go returns an empty result set after writing the file, so
    /// executing the query and returning rows instead would be silently
    /// wrong. It is refused rather than ignored.
    ///
    /// ACCEPTED WITH A DEFERRAL (documented): `FOR UPDATE`. TiDB's default
    /// `tidb_txn_mode` is pessimistic, where the clause takes row locks at
    /// read time; this seed's transactions are optimistic, where TiDB itself
    /// takes no read-time lock and resolves the conflict at COMMIT -- which
    /// is exactly what this seed does. The rows returned therefore match;
    /// what is missing is the pessimistic lock, not the result. `OF t`,
    /// `NOWAIT`, `SKIP LOCKED` and `WAIT n` all only shape that missing
    /// lock's waiting behavior, so they are accepted for the same reason.
    fn check_query_clauses(&self, query: &tidb_ast::QueryStmt) -> Result<(), DriverError> {
        let into_outfile = match query {
            tidb_ast::QueryStmt::Select(select) => select.into_outfile.is_some(),
            tidb_ast::QueryStmt::SetOpr(_) => false,
        };
        if into_outfile {
            return Err(DriverError::Unsupported(
                "SELECT ... INTO OUTFILE is not supported yet",
            ));
        }
        Ok(())
    }

    /// Go `preprocessor.checkNoopFuncs` + `checkGroupBy`: refuses the clauses
    /// TiDB parses but only implements as no-ops, unless
    /// `tidb_enable_noop_functions` says otherwise.
    ///
    /// Captured from TiDB with the variable at its `OFF` default:
    /// `SELECT SQL_CALC_FOUND_ROWS ...`, `... FOR SHARE` and `... LOCK IN
    /// SHARE MODE` all raise 1235; `FOR UPDATE` does not.
    ///
    /// DEFERRED (documented): `tidb_enable_shared_lock_promotion`, which
    /// turns `FOR SHARE` into `FOR UPDATE` before this check, and the
    /// `ForShareLockEnabledByNoop` statement flag that only a real locking
    /// layer would read.
    fn check_noop_functions(&mut self, query: &tidb_ast::QueryStmt) -> Result<(), DriverError> {
        let mode = match self
            .vars
            .get_system("tidb_enable_noop_functions")
            .unwrap_or_else(|_| "OFF".to_owned())
            .to_ascii_uppercase()
            .as_str()
        {
            "ON" | "1" => NoopFuncsMode::On,
            "WARN" => NoopFuncsMode::Warn,
            _ => NoopFuncsMode::Off,
        };
        let mut gated: Vec<&'static str> = Vec::new();
        collect_noop_clauses(query, &mut gated);
        if gated.is_empty() || mode == NoopFuncsMode::On {
            return Ok(());
        }
        for clause in gated {
            let message = format!(
                "function {clause} has only noop implementation in tidb now, use \
                 tidb_enable_noop_functions to enable these functions"
            );
            if mode == NoopFuncsMode::Off {
                return Err(DriverError::FunctionsNoopImpl(clause));
            }
            self.warnings.push(SqlWarning {
                level: WarningLevel::Warning,
                code: 1235,
                message,
            });
        }
        Ok(())
    }
}

/// Names every gated clause the query uses, in the order Go's preprocessor
/// would reach them.
///
/// Go walks the whole statement tree, so a gated clause inside a derived
/// table, a CTE or a subquery counts too; this walk covers the same
/// containers.
fn collect_noop_clauses(query: &tidb_ast::QueryStmt, out: &mut Vec<&'static str>) {
    match query {
        tidb_ast::QueryStmt::Select(select) => collect_noop_in_select(select, out),
        tidb_ast::QueryStmt::SetOpr(set_opr) => collect_noop_in_set_opr(set_opr, out),
    }
}

fn collect_noop_in_set_opr(set_opr: &tidb_ast::SetOprStmt, out: &mut Vec<&'static str>) {
    if let Some(with) = &set_opr.with {
        for cte in &with.ctes {
            collect_noop_clauses(&cte.query, out);
        }
    }
    for term in &set_opr.terms {
        match &term.body {
            tidb_ast::SetOprTermBody::Select(select) => collect_noop_in_select(select, out),
            tidb_ast::SetOprTermBody::Nested(nested) => collect_noop_in_set_opr(nested, out),
        }
    }
    // A set operation carries its own trailing locking clause, which the
    // grammar attaches to the whole statement rather than the last term.
    if share_lock(&set_opr.lock) || share_lock(&set_opr.outer_lock) {
        out.push("LOCK IN SHARE MODE");
    }
}

/// Whether the statement reports the warning buffer, and so must not clear it
/// before running. Go decides this on the parsed node; parsing here would mean
/// parsing the statement twice, so this reads the leading keywords the same
/// way the dispatcher's own fast paths do.
fn reports_warnings(sql: &str) -> bool {
    let mut words = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .filter(|word| !word.is_empty());
    if !words
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("SHOW"))
    {
        return false;
    }
    // `SHOW WARNINGS`, `SHOW ERRORS`, and the `SHOW COUNT(*) WARNINGS` form.
    words.any(|word| {
        let word = word.trim_end_matches(';');
        word.eq_ignore_ascii_case("WARNINGS") || word.eq_ignore_ascii_case("ERRORS")
    })
}

/// Whether a locking clause is the shared kind, which is the gated one --
/// `FOR UPDATE` is a real lock in TiDB and is never gated.
fn share_lock(lock: &Option<tidb_ast::SelectLock>) -> bool {
    matches!(
        lock,
        Some(tidb_ast::SelectLock {
            kind: tidb_ast::LockKind::Share,
            ..
        })
    )
}

fn collect_noop_in_select(select: &tidb_ast::SelectStmt, out: &mut Vec<&'static str>) {
    if select.calc_found_rows {
        out.push("SQL_CALC_FOUND_ROWS");
    }
    if share_lock(&select.lock) {
        out.push("LOCK IN SHARE MODE");
    }
    // Go's `checkGroupBy`: a written ASC/DESC on a GROUP BY item is a no-op,
    // because TiDB does not order groups.
    if select.group_by.iter().any(|item| item.desc.is_some()) {
        out.push("GROUP BY expr ASC|DESC");
    }
    if let Some(with) = &select.with {
        for cte in &with.ctes {
            collect_noop_clauses(&cte.query, out);
        }
    }
    if let Some(from) = &select.from {
        collect_noop_in_join(from, out);
    }
    for expr in select
        .where_clause
        .iter()
        .chain(select.having.iter())
        .chain(select.group_by.iter().map(|item| &item.expr))
        .chain(select.order_by.iter().map(|item| &item.expr))
    {
        collect_noop_in_expr(expr, out);
    }
}

/// The subqueries a `FROM` clause holds, which are derived tables.
fn collect_noop_in_join(join: &tidb_ast::Join, out: &mut Vec<&'static str>) {
    for node in std::iter::once(&join.left).chain(join.right.iter()) {
        match node {
            tidb_ast::JoinNode::Derived { subquery, .. } => collect_noop_clauses(subquery, out),
            tidb_ast::JoinNode::Join(nested) => collect_noop_in_join(nested, out),
            tidb_ast::JoinNode::Table(_) => {}
        }
    }
    if let Some(on) = &join.on {
        collect_noop_in_expr(on, out);
    }
}

/// The subqueries an expression holds.
fn collect_noop_in_expr(expr: &tidb_ast::Expr, out: &mut Vec<&'static str>) {
    match expr {
        tidb_ast::Expr::Subquery(query) => collect_noop_clauses(query, out),
        tidb_ast::Expr::Exists { subquery, .. } => collect_noop_clauses(subquery, out),
        tidb_ast::Expr::InSubquery { expr, subquery, .. } => {
            collect_noop_in_expr(expr, out);
            collect_noop_clauses(subquery, out);
        }
        tidb_ast::Expr::CompareSubquery { left, subquery, .. } => {
            collect_noop_in_expr(left, out);
            collect_noop_clauses(subquery, out);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `EXPLAIN <select>` reports the plan this tier would run, in Go's five
    /// columns, without executing anything.
    ///
    /// Every row here was compared against a `testkit.CreateMockStore`
    /// capture of real TiDB's `EXPLAIN` on the same schema with no analyzed
    /// statistics. Where a row differs, the divergence is named in the
    /// assertion's own comment and in `tidb_executor::explain`'s module doc.
    #[test]
    fn explain_select() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(64), c INT, INDEX ub(b))")
            .unwrap();

        // The Point_Get row is byte-identical to the TiDB capture:
        //   Point_Get_1 | 1.00 | root | table:t | handle:1
        // DIVERGENCE (explain module doc, items 3 and 7): TiDB's fast plan
        // REPLACES the whole pipeline, so it prints that one row. This tier's
        // point get only narrows the source -- `run_select_stmt` keeps the
        // WHERE as a Selection above it (deliberately: an extra conjunct the
        // handle did not pin still has to filter) and always builds a
        // Projection. Both re-check rows the handle lookup already returned,
        // so neither reduces the 1.00.
        assert_eq!(
            row_text(session.run("EXPLAIN SELECT * FROM t WHERE a = 1")),
            vec![
                vec![
                    "Projection_3".to_owned(),
                    "1.00".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "*".to_owned(),
                ],
                vec![
                    "└─Selection_2".to_owned(),
                    "1.00".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "eq(test.t.a, 1)".to_owned(),
                ],
                vec![
                    "  └─Point_Get_1".to_owned(),
                    "1.00".to_owned(),
                    "root".to_owned(),
                    "table:t".to_owned(),
                    "handle:1".to_owned(),
                ],
            ]
        );

        // Same shape, same reason. The Batch_Point_Get row itself matches the
        // capture byte for byte:
        //   Batch_Point_Get_1 | 3.00 | root | table:t |
        //     handle:[1 2 3], keep order:false, desc:false
        assert_eq!(
            row_text(session.run("EXPLAIN SELECT * FROM t WHERE a IN (1,2,3)"))[2],
            vec![
                "  └─Batch_Point_Get_1".to_owned(),
                "3.00".to_owned(),
                "root".to_owned(),
                "table:t".to_owned(),
                "handle:[1 2 3], keep order:false, desc:false".to_owned(),
            ]
        );

        // DIVERGENCE (explain module doc, items 1/3/5): TiDB prints
        //   TableReader_5 | 10000.00 | root | | data:TableFullScan_4
        //   └─TableFullScan_4 | 10000.00 | cop[tikv] | table:t | keep order:false, stats:pseudo
        // This tier has no coprocessor, so there is no TableReader and no
        // cop task; and the driver always builds a projection, which Go
        // elides here. The scan row's estRows/access/info match exactly.
        assert_eq!(
            row_text(session.run("EXPLAIN SELECT * FROM t")),
            vec![
                vec![
                    "Projection_2".to_owned(),
                    "10000.00".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "*".to_owned(),
                ],
                vec![
                    "└─TableFullScan_1".to_owned(),
                    "10000.00".to_owned(),
                    "root".to_owned(),
                    "table:t".to_owned(),
                    "keep order:false, stats:pseudo".to_owned(),
                ],
            ]
        );

        // An indexed column's range scan. TiDB prints the same 3333.33 and
        // the same `table:t, index:ub(b)` access object; it wraps the scan in
        // a TableReader/cop task (divergence 1) and its Selection sits in the
        // cop task rather than above the scan.
        assert_eq!(
            row_text(session.run("EXPLAIN SELECT * FROM t WHERE b > 'x'")),
            vec![
                vec![
                    "Projection_3".to_owned(),
                    "3333.33".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "*".to_owned(),
                ],
                vec![
                    "└─Selection_2".to_owned(),
                    "3333.33".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    // Go's own function-call rendering, captured:
                    // gt(test.t.b, "x").
                    "gt(test.t.b, \"x\")".to_owned(),
                ],
                vec![
                    "  └─IndexRangeScan_1".to_owned(),
                    "3333.33".to_owned(),
                    "root".to_owned(),
                    "table:t, index:ub(b)".to_owned(),
                    "range:(\"x\",+inf], keep order:false, stats:pseudo".to_owned(),
                ],
            ]
        );

        // ORDER BY + LIMIT. DIVERGENCE (item 2): TiDB merges these into one
        // TopN_7 (10.00). This tier builds a Sort and a Limit, so both show.
        // The Limit's 10.00 and its `offset:0, count:10` match Go's.
        assert_eq!(
            row_text(session.run("EXPLAIN SELECT * FROM t ORDER BY c LIMIT 10")),
            vec![
                vec![
                    "Limit_4".to_owned(),
                    "10.00".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "offset:0, count:10".to_owned(),
                ],
                vec![
                    "└─Projection_3".to_owned(),
                    "10000.00".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "*".to_owned(),
                ],
                vec![
                    "  └─Sort_2".to_owned(),
                    "10000.00".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "test.t.c".to_owned(),
                ],
                vec![
                    "    └─TableFullScan_1".to_owned(),
                    "10000.00".to_owned(),
                    "root".to_owned(),
                    "table:t".to_owned(),
                    "keep order:false, stats:pseudo".to_owned(),
                ],
            ]
        );

        // GROUP BY. The 8000.00 is Go's own stats-less distinctFactor result,
        // captured. DIVERGENCE (item 4): TiDB splits this into a cop-side
        // HashAgg_5 (`funcs:count(1)->Column#6`) and a root HashAgg_9 under a
        // Projection_4; this tier has one aggregate and no Column#N slots.
        assert_eq!(
            row_text(session.run("EXPLAIN SELECT c, COUNT(*) FROM t GROUP BY c")),
            vec![
                vec![
                    "HashAgg_2".to_owned(),
                    "8000.00".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    // The parser normalizes COUNT(*) to COUNT(1), so this
                    // half is byte-identical to the cop-side funcs: text.
                    "group by:test.t.c, funcs:test.t.c, count(1)".to_owned(),
                ],
                vec![
                    "└─TableFullScan_1".to_owned(),
                    "10000.00".to_owned(),
                    "root".to_owned(),
                    "table:t".to_owned(),
                    "keep order:false, stats:pseudo".to_owned(),
                ],
            ]
        );
    }

    /// `EXPLAIN ANALYZE <select>` really executes the query, and reports the
    /// REAL number of rows each operator produced -- not an estimate.
    ///
    /// Captured against `testkit.CreateMockStore`: real TiDB's
    /// `actRows` column for `explain analyze select * from t where v > 2`
    /// (table rows `(1,1),(2,2),(3,3),(4,10)`) is `4` for the
    /// `TableFullScan` (it reads every row), `2` for the `Selection` (only
    /// `v=3` and `v=10` pass `v > 2`), and `2` again for the `TableReader`
    /// root (a pass-through). This tier has no `TableReader` (`explain`
    /// module doc, divergence 1) and always builds a `Projection`
    /// (divergence 3), so the real shape here is `Projection` over
    /// `Selection` over `TableFullScan` -- the `Projection`'s `actRows` is
    /// the same real `2`, matching the real row set, not a guess.
    #[test]
    fn explain_analyze_select() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,10)")
            .unwrap();

        let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM t WHERE v > 2"));
        // Columns: id, estRows, actRows, task, access object, execution
        // info, operator info, memory, disk.
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], "Projection_3");
        assert_eq!(rows[0][2], "2"); // actRows: real, not the 3333.33 estimate.
        assert_eq!(rows[1][0], "└─Selection_2");
        assert_eq!(rows[1][2], "2");
        assert_eq!(rows[2][0], "  └─TableFullScan_1");
        assert_eq!(rows[2][2], "4");
        // Every operator here runs in-process (divergence 1), and this tier
        // collects no runtime timing/memory/disk counters at all.
        for row in &rows {
            assert_eq!(row[3], "root");
            assert_eq!(row[5], "N/A"); // execution info
            assert_eq!(row[7], "N/A"); // memory
            assert_eq!(row[8], "N/A"); // disk
        }
    }

    /// `EXPLAIN ANALYZE <insert>` really inserts -- captured: real TiDB's
    /// `EXPLAIN ANALYZE INSERT` leaves the row in the table afterward, the
    /// inverse of `EXPLAIN INSERT`, which inserts nothing (see the
    /// `explain_insert_never_executes` test below). The `Insert_1` row's
    /// `actRows` is `0` (captured), since the insert executor's own
    /// row-producing interface yields no rows -- the write is a side
    /// effect, not this operator's output.
    #[test]
    fn explain_analyze_insert_executes() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();

        let rows = row_text(session.run("EXPLAIN ANALYZE INSERT INTO t VALUES (1, 5)"));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], "Insert_1");
        assert_eq!(rows[0][2], "0");

        // The inverse of the EXPLAIN test: the row is really there now.
        assert_eq!(
            row_text(session.run("SELECT * FROM t")),
            vec![vec!["1".to_owned(), "5".to_owned()]]
        );
    }

    /// `EXPLAIN ANALYZE <update>` really updates -- captured against
    /// `testkit.CreateMockStore`: `explain analyze update t set b = 111
    /// where c = 200` on a 4-row table leaves `Update_3`'s own `actRows` at
    /// `0` (a write is a side effect, same as `Insert_1`), with a
    /// `Selection` (`actRows` `1`, the real number of `WHERE`-matching
    /// rows) over a `TableFullScan` (`actRows` `4`, the real pre-write row
    /// count) beneath it -- the write path always full-scans here (`explain`
    /// module doc, divergence 8), never a `Point_Get`, even for a
    /// primary-key equality.
    #[test]
    fn explain_analyze_update_executes() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,10,100),(2,20,200),(3,30,300),(4,40,400)")
            .unwrap();

        let rows = row_text(session.run("EXPLAIN ANALYZE UPDATE t SET b = 111 WHERE c = 200"));
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], "Update_3");
        assert_eq!(rows[0][2], "0");
        assert_eq!(rows[1][0], "└─Selection_2");
        assert_eq!(rows[1][2], "1");
        assert_eq!(rows[2][0], "  └─TableFullScan_1");
        assert_eq!(rows[2][2], "4");

        // The inverse of the plain-EXPLAIN test: the table really changed.
        assert_eq!(
            row_text(session.run("SELECT b FROM t WHERE a = 2")),
            vec![vec!["111".to_owned()]]
        );
    }

    /// `EXPLAIN ANALYZE <delete>` really deletes -- same real read-then-write
    /// shape as [`explain_analyze_update_executes`], over `Delete_N`.
    #[test]
    fn explain_analyze_delete_executes() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
            .unwrap();

        let rows = row_text(session.run("EXPLAIN ANALYZE DELETE FROM t WHERE a = 2"));
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], "Delete_3");
        assert_eq!(rows[0][2], "0");
        assert_eq!(rows[1][0], "└─Selection_2");
        assert_eq!(rows[1][2], "1");
        assert_eq!(rows[2][0], "  └─TableFullScan_1");
        assert_eq!(rows[2][2], "3");

        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY a")),
            vec![vec!["1".to_owned()], vec!["3".to_owned()]]
        );
    }

    /// `EXPLAIN ANALYZE` of a `Point_Get`/`Batch_Point_Get`/`IndexRangeScan`
    /// access path: real `actRows`, not `N/A` (divergence 7: the point get
    /// keeps its `Selection`/`Projection` above it here, so the access-path
    /// row is the LAST one, at the bottom of the tree). `Point_Get_1`'s
    /// `actRows` is `1` for a hit and `0` for a miss, `Batch_Point_Get_1`'s
    /// is the number of handles actually found, and `IndexRangeScan`'s is
    /// the real number of rows the range covers -- all confirmed by
    /// capture against `testkit.CreateMockStore`.
    #[test]
    fn explain_analyze_fast_paths_real_act_rows() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE pg (a INT PRIMARY KEY, b INT, KEY idx_b(b))")
            .unwrap();
        session
            .run("INSERT INTO pg VALUES (1,10),(2,20),(3,30),(4,40)")
            .unwrap();

        let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE a = 2"));
        assert_eq!(rows[2][0], "  └─Point_Get_1");
        assert_eq!(rows[2][2], "1");

        let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE a = 999"));
        assert_eq!(rows[2][0], "  └─Point_Get_1");
        assert_eq!(rows[2][2], "0");

        let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE a IN (1,2,3)"));
        assert_eq!(rows[2][0], "  └─Batch_Point_Get_1");
        assert_eq!(rows[2][2], "3");

        let rows =
            row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE b > 15 AND b < 35"));
        assert_eq!(rows[2][0], "  └─IndexRangeScan_1");
        assert_eq!(rows[2][2], "2");
    }

    /// `EXPLAIN ANALYZE` of a grouped aggregate/`DISTINCT`: real `actRows`
    /// -- captured: a `GROUP BY` on `(1,1),(1,2),(2,3),(2,4),(3,5)` groups
    /// into 3 real groups, and `SELECT DISTINCT a` over the same rows
    /// dedups to the same 3 real distinct values.
    #[test]
    fn explain_analyze_grouped_agg_and_distinct_real_act_rows() {
        let mut session = Session::new();
        session.run("CREATE TABLE g (a INT, b INT)").unwrap();
        session
            .run("INSERT INTO g VALUES (1,1),(1,2),(2,3),(2,4),(3,5)")
            .unwrap();

        let rows = row_text(session.run("EXPLAIN ANALYZE SELECT a, COUNT(*) FROM g GROUP BY a"));
        assert_eq!(rows[0][0], "HashAgg_2");
        assert_eq!(rows[0][2], "3");

        let rows = row_text(session.run("EXPLAIN ANALYZE SELECT DISTINCT a FROM g"));
        assert_eq!(rows[0][2], "3");
    }

    /// `EXPLAIN ANALYZE INSERT ... SELECT`'s source gets the SAME real
    /// `actRows` a plain `EXPLAIN ANALYZE SELECT` of that query would --
    /// captured: `insert into dst select * from src where a > 1` on
    /// `src = (1),(2),(3)` reports `2` for the `Projection`/`Selection`
    /// (the `WHERE`-matching rows) over the real `3`-row `TableFullScan`,
    /// computed before the insert writes anything.
    #[test]
    fn explain_analyze_insert_select_source_real_act_rows() {
        let mut session = Session::new();
        session.run("CREATE TABLE src (a INT)").unwrap();
        session.run("CREATE TABLE dst (a INT)").unwrap();
        session.run("INSERT INTO src VALUES (1),(2),(3)").unwrap();

        let rows =
            row_text(session.run("EXPLAIN ANALYZE INSERT INTO dst SELECT * FROM src WHERE a > 1"));
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][0], "Insert_4");
        assert_eq!(rows[0][2], "0");
        assert_eq!(rows[1][0], "└─Projection_3");
        assert_eq!(rows[1][2], "2");
        assert_eq!(rows[2][0], "  └─Selection_2");
        assert_eq!(rows[2][2], "2");
        assert_eq!(rows[3][0], "    └─TableFullScan_1");
        assert_eq!(rows[3][2], "3");

        assert_eq!(
            row_text(session.run("SELECT a FROM dst ORDER BY a")),
            vec![vec!["2".to_owned()], vec!["3".to_owned()]]
        );
    }

    /// `EXPLAIN` of a write: it must never run the statement. Captured
    /// against real TiDB: `EXPLAIN INSERT INTO t VALUES (1)` answers
    /// `Insert_1 | N/A | root | | N/A` and inserts nothing.
    #[test]
    fn explain_insert_plans_without_writing() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
            .unwrap();

        assert_eq!(
            row_text(session.run("EXPLAIN INSERT INTO t VALUES (1)")),
            vec![vec![
                "Insert_1".to_owned(),
                "N/A".to_owned(),
                "root".to_owned(),
                String::new(),
                "N/A".to_owned(),
            ]]
        );
        // The plan really did not write the row.
        assert_eq!(
            row_text(session.run("SELECT COUNT(*) FROM t")),
            vec![vec!["0".to_owned()]]
        );
    }

    /// `EXPLAIN UPDATE`/`EXPLAIN DELETE`: the write's plan is `Update_N`/
    /// `Delete_N` over the same read the write drivers actually build to
    /// find the target rows. Divergence 8 (`explain` module doc): those
    /// drivers always scan the whole table and filter row-by-row, with no
    /// point-get/index fast path, so the recorder always shows
    /// `TableFullScan` + `Selection` -- even for a primary-key equality,
    /// where Go's own planner would print `Point_Get`.
    #[test]
    fn explain_update_and_delete_plan_without_writing() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT)")
            .unwrap();
        session.run("INSERT INTO t VALUES (1, 1)").unwrap();

        assert_eq!(
            row_text(session.run("EXPLAIN UPDATE t SET b = 100 WHERE a = 1")),
            vec![
                vec![
                    "Update_3".to_owned(),
                    "N/A".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "N/A".to_owned(),
                ],
                vec![
                    "└─Selection_2".to_owned(),
                    "10.00".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "eq(test.t.a, 1)".to_owned(),
                ],
                vec![
                    "  └─TableFullScan_1".to_owned(),
                    "10000.00".to_owned(),
                    "root".to_owned(),
                    "table:t".to_owned(),
                    "keep order:false, stats:pseudo".to_owned(),
                ],
            ]
        );
        assert_eq!(
            row_text(session.run("EXPLAIN DELETE FROM t WHERE a = 1")),
            vec![
                vec![
                    "Delete_3".to_owned(),
                    "N/A".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "N/A".to_owned(),
                ],
                vec![
                    "└─Selection_2".to_owned(),
                    "10.00".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "eq(test.t.a, 1)".to_owned(),
                ],
                vec![
                    "  └─TableFullScan_1".to_owned(),
                    "10000.00".to_owned(),
                    "root".to_owned(),
                    "table:t".to_owned(),
                    "keep order:false, stats:pseudo".to_owned(),
                ],
            ]
        );
        // Neither plan wrote or removed the row.
        assert_eq!(
            row_text(session.run("SELECT * FROM t")),
            vec![vec!["1".to_owned(), "1".to_owned()]]
        );
    }

    /// `EXPLAIN FORMAT = 'brief'` prints the identical tree with every
    /// operator's `_N` build-order suffix stripped (captured: `explain
    /// format = 'brief' select * from t` strips the `Point_Get_1`/
    /// `Selection_2`/`Projection_3` ids down to `Point_Get`/`Selection`/
    /// `Projection`; `'row'`, the default, keeps them).
    #[test]
    fn explain_brief_format_strips_operator_ids() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
            .unwrap();
        session.run("INSERT INTO t VALUES (1)").unwrap();

        assert_eq!(
            row_text(session.run("EXPLAIN FORMAT = 'brief' SELECT * FROM t WHERE a = 1")),
            vec![
                vec![
                    "Projection".to_owned(),
                    "1.00".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "*".to_owned(),
                ],
                vec![
                    "└─Selection".to_owned(),
                    "1.00".to_owned(),
                    "root".to_owned(),
                    String::new(),
                    "eq(test.t.a, 1)".to_owned(),
                ],
                vec![
                    "  └─Point_Get".to_owned(),
                    "1.00".to_owned(),
                    "root".to_owned(),
                    "table:t".to_owned(),
                    "handle:1".to_owned(),
                ],
            ]
        );
        assert_eq!(
            row_text(session.run("EXPLAIN FORMAT = 'row' SELECT * FROM t WHERE a = 1"))[2],
            vec![
                "  └─Point_Get_1".to_owned(),
                "1.00".to_owned(),
                "root".to_owned(),
                "table:t".to_owned(),
                "handle:1".to_owned(),
            ]
        );
    }

    /// EXPLAIN still refuses the forms this tier cannot plan honestly:
    /// ANALYZE (Go executes the statement to gather runtime counters this
    /// tier does not collect, captured) and any format name Go itself does
    /// not recognize.
    #[test]
    fn explain_refuses_what_it_cannot_plan() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
            .unwrap();

        // `EXPLAIN ANALYZE` of a `SELECT`/`INSERT`/`UPDATE`/`DELETE` really
        // runs (see `explain_analyze_select`/`explain_analyze_insert_executes`/
        // `explain_analyze_update_executes`/`explain_analyze_delete_executes`);
        // only a set-operation query is refused.
        assert!(matches!(
            session.run("EXPLAIN ANALYZE (SELECT a FROM t) UNION (SELECT a FROM t)"),
            Err(DriverError::Unsupported(
                "EXPLAIN ANALYZE of a set operation is not supported yet"
            ))
        ));
        assert!(matches!(
            session.run("EXPLAIN FORMAT = 'bogus' SELECT * FROM t"),
            Err(DriverError::Unsupported("unknown EXPLAIN format name"))
        ));
    }

    /// A whole session lifecycle from SQL strings alone: DDL, writes, reads.
    #[test]
    fn session_runs_a_sql_lifecycle() {
        let mut session = Session::new();
        assert_eq!(
            session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap(),
            StmtResult::Done(true)
        );
        assert_eq!(
            session
                .run("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
                .unwrap(),
            StmtResult::Affected(3)
        );
        assert_eq!(
            session
                .run("SELECT a + b FROM t WHERE a >= 2 ORDER BY a DESC LIMIT 1")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(33)]])
        );
        // A second table coexists in the same catalog.
        session.run("CREATE TABLE u (x BIGINT)").unwrap();
        session.run("INSERT INTO u VALUES (42)").unwrap();
        assert_eq!(
            session.run("SELECT x FROM u").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(42)]])
        );
    }

    /// A handful of everyday string/date builtins that were previously
    /// refused by the chunk rewriter's return-type gate (`builtin_return_type`
    /// had no arm for them, even though `eval_func_values`/`time_fn::dispatch`
    /// already implement them). Expected values captured from upstream Go
    /// via `SELECT ...` in a mock-store testkit session.
    #[test]
    fn everyday_string_and_date_builtins() {
        let mut session = Session::new();
        assert_eq!(
            session
                .run("SELECT SUBSTRING_INDEX('a.b.c', '.', 2)")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::new_string("a.b")]])
        );
        assert_eq!(
            session.run("SELECT CHAR(77, 121, 83, 81, 76)").unwrap(),
            StmtResult::Rows(vec![vec![Datum::new_string("MySQL")]])
        );
        assert_eq!(
            session
                .run("SELECT INSERT('Quadratic', 3, 4, 'What')")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::new_string("QuWhattic")]])
        );
        assert_eq!(
            session
                .run("SELECT EXPORT_SET(5, 'Y', 'N', ',', 4)")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::new_string("Y,N,Y,N")]])
        );
        assert_eq!(
            session
                .run("SELECT DATE_FORMAT('2024-01-01 10:00:00', '%Y-%m-%d %H:%i:%s')")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::new_string("2024-01-01 10:00:00")]])
        );
        assert_eq!(
            session
                .run("SELECT STR_TO_DATE('01,5,2024','%d,%m,%Y')")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::new_string("2024-05-01")]])
        );
        assert_eq!(
            session.run("SELECT QUOTE('a''b')").unwrap(),
            StmtResult::Rows(vec![vec![Datum::new_string("'a\\'b'")]])
        );
    }

    /// UPDATE and DELETE run through the session like any other write, and
    /// report their affected-row counts.
    #[test]
    fn update_and_delete_through_the_session() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT)").unwrap();
        session.run("INSERT INTO t VALUES (1), (2), (3)").unwrap();
        assert_eq!(
            session.run("UPDATE t SET a = a * 10 WHERE a > 1").unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            session.run("DELETE FROM t WHERE a >= 20").unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        // Both are classified as writes, so the wire answers with an OK packet.
        assert_eq!(
            session.statement_kind("UPDATE t SET a = 1").unwrap(),
            StmtKind::Write
        );
        assert_eq!(
            session.statement_kind("DELETE FROM t").unwrap(),
            StmtKind::Write
        );
    }

    /// A transaction stages its writes: the session reads its own, a peer
    /// sharing the catalog sees nothing until COMMIT, and ROLLBACK discards.
    #[test]
    fn transaction_stages_writes_until_commit() {
        let mut writer = Session::new();
        writer.run("CREATE TABLE t (a BIGINT)").unwrap();
        writer.run("INSERT INTO t VALUES (1)").unwrap();
        let mut peer = Session::with_catalog(writer.shared_catalog());

        assert_eq!(writer.control_transaction("BEGIN").unwrap(), Some(true));
        assert!(writer.in_transaction());
        writer.run("INSERT INTO t VALUES (2)").unwrap();

        // The transaction reads its own write; the peer does not see it.
        assert_eq!(
            writer.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
        );
        assert_eq!(
            peer.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        assert_eq!(writer.control_transaction("COMMIT").unwrap(), Some(false));
        assert!(!writer.in_transaction());
        assert_eq!(
            peer.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
        );

        // ROLLBACK discards everything staged since BEGIN.
        writer.control_transaction("BEGIN").unwrap();
        writer.run("INSERT INTO t VALUES (3)").unwrap();
        writer.run("DELETE FROM t WHERE a = 1").unwrap();
        assert_eq!(writer.control_transaction("ROLLBACK").unwrap(), Some(false));
        assert_eq!(
            writer.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
        );
    }

    /// A commit that would discard a peer's writes is refused, rather than
    /// silently overwriting them. The refused transaction is over, so its
    /// staged writes are gone -- the statements must be retried, not the
    /// COMMIT alone.
    #[test]
    fn a_conflicting_commit_is_refused() {
        let mut first = Session::new();
        first.run("CREATE TABLE t (a BIGINT)").unwrap();
        let mut second = Session::with_catalog(first.shared_catalog());

        first.control_transaction("BEGIN").unwrap();
        first.run("INSERT INTO t VALUES (1)").unwrap();
        // The peer commits first, moving the shared catalog.
        second.run("INSERT INTO t VALUES (2)").unwrap();

        assert!(matches!(
            first.control_transaction("COMMIT"),
            Err(DriverError::Txn(TxnErrorKind::WriteConflict))
        ));
        assert!(!first.in_transaction(), "a refused commit ends the txn");
        // The peer's write survived; the refused one did not.
        assert_eq!(
            second.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(2)]])
        );
    }

    /// BEGIN inside an open transaction implicitly commits it, as in Go, and
    /// COMMIT/ROLLBACK outside one is a no-op, as in MySQL.
    #[test]
    fn nested_begin_commits_and_stray_commit_is_a_no_op() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT)").unwrap();
        assert_eq!(session.control_transaction("COMMIT").unwrap(), Some(false));
        assert_eq!(
            session.control_transaction("ROLLBACK").unwrap(),
            Some(false)
        );

        session.control_transaction("BEGIN").unwrap();
        session.run("INSERT INTO t VALUES (1)").unwrap();
        // The implicit commit publishes the first transaction's write.
        session.control_transaction("START TRANSACTION").unwrap();
        session.run("INSERT INTO t VALUES (2)").unwrap();
        session.control_transaction("ROLLBACK").unwrap();
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // A non-transaction statement is not claimed by the hook.
        assert_eq!(session.control_transaction("SELECT 1").unwrap(), None);
        assert!(session
            .control_transaction("ROLLBACK TO SAVEPOINT s")
            .is_err());
    }

    /// The single value a one-column, one-row query returns, as text.
    fn scalar_text(session: &mut Session, sql: &str) -> Option<String> {
        match session.run(sql).unwrap() {
            StmtResult::Rows(rows) => datum_text(&rows[0][0]),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// SET and the variable reads a connecting client performs.
    #[test]
    fn session_variables() {
        let mut session = Session::new();

        // A stock client's opening statements.
        assert_eq!(session.apply_set("SET NAMES utf8mb4").unwrap(), Some(()));
        assert_eq!(
            session.vars().get_system("character_set_client").unwrap(),
            "utf8mb4"
        );
        assert_eq!(session.apply_set("SET autocommit = 0").unwrap(), Some(()));
        // Go's checkBoolSystemVar canonicalizes 0/1 to OFF/ON.
        assert_eq!(session.vars().get_system("autocommit").unwrap(), "OFF");

        // Reading variables back through a query.
        assert_eq!(
            scalar_text(&mut session, "SELECT @@autocommit"),
            Some("OFF".to_owned())
        );
        let comment = scalar_text(&mut session, "SELECT @@version_comment").unwrap();
        assert!(
            comment.starts_with("TiDB Server (Apache License 2.0)"),
            "{comment}"
        );

        // DEFAULT restores the registry default.
        session.apply_set("SET autocommit = DEFAULT").unwrap();
        assert_eq!(session.vars().get_system("autocommit").unwrap(), "ON");

        // An unknown system variable is Go's 1193, on read and on write.
        assert!(matches!(
            session.apply_set("SET nonexistent_variable = 1"),
            Err(DriverError::Var(
                tidb_executor::VarErrorKind::UnknownSystemVariable(_)
            ))
        ));
        assert!(matches!(
            session.run("SELECT @@nonexistent_variable"),
            Err(DriverError::Var(
                tidb_executor::VarErrorKind::UnknownSystemVariable(_)
            ))
        ));
        // A read-only variable cannot be set.
        assert!(matches!(
            session.apply_set("SET version = '1'"),
            Err(DriverError::Var(
                tidb_executor::VarErrorKind::ReadOnlyVariable(_)
            ))
        ));

        // User variables: unset reads as NULL, never an error.
        assert_eq!(
            session.run("SELECT @nope").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Null]])
        );
        session.apply_set("SET @x = 41 + 1").unwrap();
        assert_eq!(
            scalar_text(&mut session, "SELECT @x"),
            Some("42".to_owned())
        );

        // A non-SET statement is not claimed by the hook.
        assert_eq!(session.apply_set("SELECT 1").unwrap(), None);
    }

    /// Transcreated from Go `pkg/executor/test/ddl/ddl_test.go`
    /// `TestCreateDropDatabase`, case for case, minus the parts that need
    /// tiers this seed does not have yet.
    ///
    /// NOT PORTED from that Go test (documented): every `charset`/`collate`
    /// database option and its `SHOW CREATE DATABASE` output, which need the
    /// charset tier; the `drop database mysql` rejection, which needs the
    /// system schemas; and the privilege/role cases.
    #[test]
    fn create_drop_database() {
        let mut session = Session::new();

        // tk.MustExec("create database if not exists drop_test;")
        session
            .run("CREATE DATABASE IF NOT EXISTS drop_test")
            .unwrap();
        // tk.MustExec("drop database if exists drop_test;")
        session.run("DROP DATABASE IF EXISTS drop_test").unwrap();
        // tk.MustExec("create database drop_test;")
        session.run("CREATE DATABASE drop_test").unwrap();
        // tk.MustExec("use drop_test;")
        session.run("USE drop_test").unwrap();
        assert_eq!(session.current_database(), "drop_test");
        // tk.MustExec("drop database drop_test;")
        session.run("DROP DATABASE drop_test").unwrap();

        // tk.MustGetDBError("drop table t;", plannererrors.ErrNoDB)
        // tk.MustGetDBError("select * from t;", plannererrors.ErrNoDB)
        // Dropping the current database leaves none selected.
        assert_eq!(session.current_database(), "");
        assert!(matches!(
            session.run("SELECT * FROM t"),
            Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected))
        ));
        assert!(matches!(
            session.run("INSERT INTO t VALUES (1)"),
            Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected))
        ));

        // Creating a database that exists is Go's ErrDBCreateExists unless
        // IF NOT EXISTS was written.
        session.run("CREATE DATABASE drop_test").unwrap();
        assert!(matches!(
            session.run("CREATE DATABASE drop_test"),
            Err(DriverError::Schema(SchemaErrorKind::DatabaseExists(_)))
        ));
        session
            .run("CREATE DATABASE IF NOT EXISTS drop_test")
            .unwrap();
        // Dropping one that does not exist is ErrDBDropExists unless IF EXISTS.
        session.run("DROP DATABASE drop_test").unwrap();
        assert!(matches!(
            session.run("DROP DATABASE drop_test"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(_)))
        ));
        session.run("DROP DATABASE IF EXISTS drop_test").unwrap();

        // USE on an unknown schema is Go's ErrDatabaseNotExists.
        assert!(matches!(
            session.run("USE no_such_database"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(_)))
        ));
    }

    /// SHOW DATABASES and SHOW TABLES, with Go's column naming and ordering.
    #[test]
    fn show_databases_and_tables() {
        let mut session = Session::new();
        session.run("CREATE TABLE zeta (a BIGINT)").unwrap();
        session.run("CREATE TABLE alpha (a BIGINT)").unwrap();
        session.run("CREATE DATABASE other").unwrap();

        // Go's fetchShowDatabases sorts the names, then moves
        // information_schema to the front; the column is "Database".
        match session.run_with_columns("SHOW DATABASES").unwrap() {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(columns[0].0, "Database");
                assert_eq!(
                    rows.iter()
                        .map(|row| datum_text(&row[0]).unwrap())
                        .collect::<Vec<_>>(),
                    vec![
                        "INFORMATION_SCHEMA".to_owned(),
                        "other".to_owned(),
                        "test".to_owned()
                    ]
                );
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // Go names the column Tables_in_<db> and sorts the table names.
        match session.run_with_columns("SHOW TABLES").unwrap() {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(columns[0].0, "Tables_in_test");
                assert_eq!(
                    rows.iter()
                        .map(|row| datum_text(&row[0]).unwrap())
                        .collect::<Vec<_>>(),
                    vec!["alpha".to_owned(), "zeta".to_owned()]
                );
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // SHOW TABLES IN <db> reports that schema, and an empty one is empty.
        match session.run_with_columns("SHOW TABLES IN other").unwrap() {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(columns[0].0, "Tables_in_other");
                assert!(rows.is_empty());
            }
            other => panic!("expected rows, got {other:?}"),
        }
        // An unknown schema is Go's ErrBadDB.
        assert!(matches!(
            session.run("SHOW TABLES IN nope"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(_)))
        ));
    }

    /// A table in another schema is reachable by qualifying it, which is what
    /// makes the schema tier more than a listing.
    #[test]
    fn a_qualified_name_resolves_across_schemas() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT)").unwrap();
        session.run("INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(
            session.run("SELECT a FROM test.t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
    }

    /// USE changes where unqualified names resolve, which is the point of the
    /// schema tier: the same table name in two schemas is two tables.
    #[test]
    fn use_changes_unqualified_name_resolution() {
        let mut session = Session::new();
        session.run("CREATE DATABASE other").unwrap();

        session.run("CREATE TABLE t (a BIGINT)").unwrap();
        session.run("INSERT INTO t VALUES (1)").unwrap();

        session.run("USE other").unwrap();
        // `t` here is a different table, in the other schema.
        session.run("CREATE TABLE t (a BIGINT)").unwrap();
        session.run("INSERT INTO t VALUES (2)").unwrap();
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(2)]])
        );
        // The first schema's table is still reachable by qualifying it.
        assert_eq!(
            session.run("SELECT a FROM test.t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        assert_eq!(
            session.run_with_columns("SHOW TABLES").unwrap(),
            StmtOutput::Rows {
                columns: vec![(
                    "Tables_in_other".to_owned(),
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString)
                )],
                rows: vec![vec![Datum::Bytes(b"t".to_vec())]],
            }
        );

        session.run("USE test").unwrap();
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        // Writes follow the current schema too.
        session.run("UPDATE t SET a = 10").unwrap();
        assert_eq!(
            session.run("SELECT a FROM other.t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(2)]])
        );
    }

    /// SHOW COLUMNS / DESCRIBE, with Go's ColDesc field names and key flags.
    #[test]
    fn show_columns_and_describe() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE t (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, \
                 tag VARCHAR(4), v BIGINT, KEY tag_idx (tag))",
            )
            .unwrap();

        let describe = |session: &mut Session, sql: &str| match session
            .run_with_columns(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        {
            StmtOutput::Rows { columns, rows } => (
                columns
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                rows.into_iter()
                    .map(|row| {
                        row.iter()
                            .map(|value| match value {
                                Datum::Null => "NULL".to_owned(),
                                other => datum_text(other).unwrap_or_default(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>(),
            ),
            other => panic!("expected rows, got {other:?}"),
        };

        let (names, rows) = describe(&mut session, "SHOW COLUMNS FROM t");
        assert_eq!(names, ["Field", "Type", "Null", "Key", "Default", "Extra"]);
        assert_eq!(
            rows,
            vec![
                // A handle primary key is NOT NULL and PRI, as Go marks it.
                vec!["id", "bigint(20)", "NO", "PRI", "NULL", ""],
                // A column that is the whole of a unique index is UNI.
                vec!["code", "varchar(8)", "YES", "UNI", "NULL", ""],
                // A column leading a non-unique index is MUL.
                vec!["tag", "varchar(4)", "YES", "MUL", "NULL", ""],
                // An unindexed column has no key flag.
                vec!["v", "bigint(20)", "YES", "", "NULL", ""],
            ]
        );

        // Go reports auto_increment in Extra; captured from TiDB's DESCRIBE:
        // [[id bigint(20) NO PRI <nil> auto_increment] [v bigint(20) YES  <nil> ]]
        session
            .run("CREATE TABLE ai (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)")
            .unwrap();
        assert_eq!(
            describe(&mut session, "DESCRIBE ai").1,
            vec![
                vec!["id", "bigint(20)", "NO", "PRI", "NULL", "auto_increment"],
                vec!["v", "bigint(20)", "YES", "", "NULL", ""],
            ]
        );

        // A column's stored DEFAULT shows in the Default column.
        session
            .run("CREATE TABLE withdef (a BIGINT DEFAULT 7, b VARCHAR(4) DEFAULT 'zz')")
            .unwrap();
        assert_eq!(
            describe(&mut session, "DESCRIBE withdef").1,
            vec![
                vec!["a", "bigint(20)", "YES", "", "7", ""],
                vec!["b", "varchar(4)", "YES", "", "zz", ""],
            ]
        );

        // DESCRIBE parses to the same node and answers identically.
        assert_eq!(describe(&mut session, "DESCRIBE t"), (names.clone(), rows));
        assert_eq!(describe(&mut session, "DESC t").0, names);

        // Another schema's table is reachable by qualifying the FROM.
        session.run("CREATE DATABASE other").unwrap();
        session.run("USE other").unwrap();
        assert_eq!(describe(&mut session, "SHOW COLUMNS FROM test.t").0, names);

        // Go's DESCRIBE takes an optional column, which narrows the output.
        session.run("USE test").unwrap();
        let (_, one) = describe(&mut session, "DESCRIBE t code");
        assert_eq!(
            one,
            vec![vec!["code", "varchar(8)", "YES", "UNI", "NULL", ""]]
        );

        // An unknown table is an error, not empty output.
        assert!(session.run("SHOW COLUMNS FROM nope").is_err());
    }

    /// SHOW FULL COLUMNS, checked against a capture from real TiDB
    /// (`SHOW FULL COLUMNS FROM t` over `create table t (a int, b
    /// varchar(20))`):
    /// `[a int(11) <nil> YES  <nil>  select,insert,update,references ]`
    /// `[b varchar(20) utf8mb4_bin YES  <nil>  select,insert,update,references ]`
    #[test]
    fn show_full_columns() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a INT, b VARCHAR(20))")
            .unwrap();

        let (names, rows) = match session
            .run_with_columns("SHOW FULL COLUMNS FROM t")
            .unwrap()
        {
            StmtOutput::Rows { columns, rows } => (
                columns
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                rows.into_iter()
                    .map(|row| {
                        row.iter()
                            .map(|value| match value {
                                Datum::Null => "NULL".to_owned(),
                                other => datum_text(other).unwrap_or_default(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>(),
            ),
            other => panic!("expected rows, got {other:?}"),
        };
        assert_eq!(
            names,
            [
                "Field",
                "Type",
                "Collation",
                "Null",
                "Key",
                "Default",
                "Extra",
                "Privileges",
                "Comment",
            ]
        );
        assert_eq!(
            rows,
            vec![
                // A numeric column's Collation is NULL.
                vec![
                    "a",
                    "int(11)",
                    "NULL",
                    "YES",
                    "",
                    "NULL",
                    "",
                    "select,insert,update,references",
                    "",
                ],
                // A string column's Collation is its own collation name.
                vec![
                    "b",
                    "varchar(20)",
                    "utf8mb4_bin",
                    "YES",
                    "",
                    "NULL",
                    "",
                    "select,insert,update,references",
                    "",
                ],
            ]
        );
    }

    /// SHOW CREATE TABLE, checked against output captured from real TiDB by
    /// running the same DDL through `pkg/executor/test/showtest` and printing
    /// `show create table`. Every expectation below is that captured text.
    #[test]
    fn show_create_table() {
        let mut session = Session::new();
        let create = |session: &mut Session, sql: &str, name: &str| {
            session.run(sql).unwrap();
            match session
                .run_with_columns(&format!("SHOW CREATE TABLE {name}"))
                .unwrap()
            {
                StmtOutput::Rows { columns, rows } => {
                    assert_eq!(
                        columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
                        ["Table", "Create Table"]
                    );
                    assert_eq!(datum_text(&rows[0][0]).unwrap(), name);
                    datum_text(&rows[0][1]).unwrap()
                }
                other => panic!("expected rows, got {other:?}"),
            }
        };

        // Captured from TiDB verbatim.
        assert_eq!(
            create(
                &mut session,
                "create table t1 (id bigint primary key, code varchar(8) unique, \
                 tag varchar(4), v bigint, key tag_idx (tag))",
                "t1"
            ),
            "CREATE TABLE `t1` (\n  \
             `id` bigint(20) NOT NULL,\n  \
             `code` varchar(8) DEFAULT NULL,\n  \
             `tag` varchar(4) DEFAULT NULL,\n  \
             `v` bigint(20) DEFAULT NULL,\n  \
             PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,\n  \
             KEY `tag_idx` (`tag`),\n  \
             UNIQUE KEY `code` (`code`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        assert_eq!(
            create(
                &mut session,
                "create table t2 (a bigint default 7, b varchar(4) default 'zz', \
                 c bigint not null, d bigint)",
                "t2"
            ),
            "CREATE TABLE `t2` (\n  \
             `a` bigint(20) DEFAULT '7',\n  \
             `b` varchar(4) DEFAULT 'zz',\n  \
             `c` bigint(20) NOT NULL,\n  \
             `d` bigint(20) DEFAULT NULL\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        assert_eq!(
            create(
                &mut session,
                "create table t4 (a bigint, b bigint, key ab (a,b))",
                "t4"
            ),
            "CREATE TABLE `t4` (\n  \
             `a` bigint(20) DEFAULT NULL,\n  \
             `b` bigint(20) DEFAULT NULL,\n  \
             KEY `ab` (`a`,`b`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        // Index order: table constraints first, then inline ones in column
        // order -- also captured from TiDB.
        assert_eq!(
            create(
                &mut session,
                "create table x1 (a bigint unique, b bigint unique, key kb (b))",
                "x1"
            ),
            "CREATE TABLE `x1` (\n  \
             `a` bigint(20) DEFAULT NULL,\n  \
             `b` bigint(20) DEFAULT NULL,\n  \
             KEY `kb` (`b`),\n  \
             UNIQUE KEY `a` (`a`),\n  \
             UNIQUE KEY `b` (`b`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        // A string primary key is now a clustered common handle, so this
        // matches TiDB's captured output exactly. The previous commit
        // reported NONCLUSTERED, truthfully, because no common handle
        // existed then.
        assert_eq!(
            create(
                &mut session,
                "create table t3 (k varchar(10) primary key)",
                "t3"
            ),
            "CREATE TABLE `t3` (\n  \
             `k` varchar(10) NOT NULL,\n  \
             PRIMARY KEY (`k`) /*T![clustered_index] CLUSTERED */\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        // AUTO_INCREMENT, captured from TiDB verbatim.
        assert_eq!(
            create(
                &mut session,
                "create table a1 (id bigint auto_increment primary key, v bigint)",
                "a1"
            ),
            "CREATE TABLE `a1` (\n  \
             `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  \
             `v` bigint(20) DEFAULT NULL,\n  \
             PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        );

        // An unknown table is an error, and another schema is reachable.
        assert!(session.run("SHOW CREATE TABLE nope").is_err());
        session.run("CREATE DATABASE other").unwrap();
        session.run("USE other").unwrap();
        assert!(session.run("SHOW CREATE TABLE test.t1").is_ok());
    }

    /// LAST_INSERT_ID, checked against a sequence captured from real TiDB:
    /// 0, 1, 2 (the FIRST id of a multi-row insert), unchanged by an explicit
    /// value, then 101 and 102, and unchanged by a non-allocating statement.
    #[test]
    fn last_insert_id() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE a (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)")
            .unwrap();
        let read = |session: &mut Session| match session.run("SELECT LAST_INSERT_ID()").unwrap() {
            StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };

        assert_eq!(read(&mut session), "0", "captured: start");
        session.run("INSERT INTO a (v) VALUES (10)").unwrap();
        assert_eq!(read(&mut session), "1", "captured: after single auto");
        session
            .run("INSERT INTO a (v) VALUES (20), (30), (40)")
            .unwrap();
        assert_eq!(
            read(&mut session),
            "2",
            "captured: a multi-row insert reports its FIRST id"
        );
        session.run("INSERT INTO a VALUES (100, 50)").unwrap();
        assert_eq!(
            read(&mut session),
            "2",
            "captured: an explicit value leaves it unchanged"
        );
        session.run("INSERT INTO a (v) VALUES (60)").unwrap();
        assert_eq!(read(&mut session), "101", "captured: after auto again");
        session.run("INSERT INTO a VALUES (NULL, 70)").unwrap();
        assert_eq!(read(&mut session), "102", "captured: NULL allocates");

        // A table with no auto column, and an UPDATE, both leave it alone.
        session
            .run("CREATE TABLE b (id BIGINT PRIMARY KEY)")
            .unwrap();
        session.run("INSERT INTO b VALUES (5)").unwrap();
        assert_eq!(read(&mut session), "102", "captured: non-auto insert");
        session.run("UPDATE a SET v = 0 WHERE id = 1").unwrap();
        assert_eq!(read(&mut session), "102", "captured: after update");

        // The OK packet's field is per statement, so it is 0 for a statement
        // that allocated nothing, unlike the sticky function value.
        session.run("INSERT INTO a (v) VALUES (80)").unwrap();
        assert_eq!(session.statement_insert_id(), 103);
        session.run("INSERT INTO b VALUES (6)").unwrap();
        assert_eq!(session.statement_insert_id(), 0);
        assert_eq!(session.last_insert_id(), 103);
    }

    /// information_schema, checked against output captured from a running
    /// TiDB: the column lists and the values for the same table definition.
    #[test]
    fn information_schema() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, \
                 code VARCHAR(8) UNIQUE, v BIGINT DEFAULT 7)",
            )
            .unwrap();

        let query = |session: &mut Session, sql: &str| match session.run_with_columns(sql).unwrap()
        {
            StmtOutput::Rows { columns, rows } => (
                columns
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                rows.into_iter()
                    .map(|row| {
                        row.iter()
                            .map(|value| match value {
                                Datum::Null => "<nil>".to_owned(),
                                Datum::Int(v) => v.to_string(),
                                other => datum_text(other).unwrap_or_default(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>(),
            ),
            other => panic!("expected rows, got {other:?}"),
        };

        // SCHEMATA: captured column list, and a row per schema.
        let (names, rows) = query(&mut session, "SELECT * FROM information_schema.schemata");
        assert_eq!(
            names,
            [
                "CATALOG_NAME",
                "SCHEMA_NAME",
                "DEFAULT_CHARACTER_SET_NAME",
                "DEFAULT_COLLATION_NAME",
                "SQL_PATH",
                "TIDB_PLACEMENT_POLICY_NAME"
            ]
        );
        // Captured: [def INFORMATION_SCHEMA utf8mb4 utf8mb4_bin <nil> <nil>]
        assert_eq!(
            rows[0],
            vec![
                "def",
                "INFORMATION_SCHEMA",
                "utf8mb4",
                "utf8mb4_bin",
                "<nil>",
                "<nil>"
            ]
        );
        assert!(rows.iter().any(|row| row[1] == "test"));

        // TABLES: the captured 28-column list, and the captured values.
        let (names, rows) = query(&mut session, "SELECT * FROM information_schema.tables");
        assert_eq!(names.len(), 28, "the captured TABLES column count");
        assert_eq!(names[0], "TABLE_CATALOG");
        assert_eq!(names[27], "TIDB_STORAGE_CLASS");
        let row = rows.iter().find(|row| row[2] == "t").expect("table t");
        // Captured: def test t BASE TABLE InnoDB 10 Compact ...
        assert_eq!(
            &row[..7],
            ["def", "test", "t", "BASE TABLE", "InnoDB", "10", "Compact"]
        );
        assert_eq!(row[17], "utf8mb4_bin", "TABLE_COLLATION");
        assert_eq!(row[22], "NOT_SHARDED(PK_IS_HANDLE)");
        assert_eq!(row[23], "CLUSTERED");
        assert_eq!(row[25], "Normal");

        // COLUMNS: the captured 22-column list and per-column values.
        let (names, rows) = query(&mut session, "SELECT * FROM information_schema.columns");
        assert_eq!(names.len(), 22, "the captured COLUMNS column count");
        assert_eq!(names[4], "ORDINAL_POSITION");
        assert_eq!(names[21], "SRS_ID");
        let of = |name: &str| {
            rows.iter()
                .find(|row| row[2] == "t" && row[3] == name)
                .expect("column")
                .clone()
        };
        // Captured: def test t id 1 <nil> NO bigint <nil> <nil> 19 0 <nil>
        //           <nil> <nil> bigint(20) PRI auto_increment ...
        let id = of("id");
        assert_eq!(
            &id[..8],
            ["def", "test", "t", "id", "1", "<nil>", "NO", "bigint"]
        );
        assert_eq!(
            &id[8..15],
            ["<nil>", "<nil>", "19", "0", "<nil>", "<nil>", "<nil>"]
        );
        assert_eq!(
            &id[15..19],
            [
                "bigint(20)",
                "PRI",
                "auto_increment",
                "select,insert,update,references"
            ]
        );
        // Captured: code ... 8 32 <nil> <nil> <nil> utf8mb4 utf8mb4_bin
        //           varchar(8) UNI
        let code = of("code");
        assert_eq!(code[7], "varchar");
        assert_eq!(
            &code[8..16],
            [
                "8",
                "32",
                "<nil>",
                "<nil>",
                "<nil>",
                "utf8mb4",
                "utf8mb4_bin",
                "varchar(8)"
            ]
        );
        assert_eq!(code[16], "UNI");
        // Captured: v ... 7 YES bigint, no key
        let v = of("v");
        assert_eq!(v[5], "7", "COLUMN_DEFAULT");
        assert_eq!(v[6], "YES");
        assert_eq!(v[16], "");

        // A projected column is named as WRITTEN, which is captured TiDB
        // behavior: `select table_name ...` reports `table_name`, while
        // `select TABLE_NAME ...` reports `TABLE_NAME`.
        assert_eq!(
            query(
                &mut session,
                "SELECT table_name FROM information_schema.tables"
            )
            .0,
            ["table_name"]
        );
        assert_eq!(
            query(
                &mut session,
                "SELECT TABLE_NAME FROM information_schema.tables"
            )
            .0,
            ["TABLE_NAME"]
        );
        // A bare name works while that schema is current.
        session.run("USE information_schema").unwrap();
        assert_eq!(
            query(&mut session, "SELECT schema_name FROM schemata").0,
            ["schema_name"]
        );

        // An unimplemented information_schema table is an error, not empty
        // output that would look like a table with no rows. (`views` is
        // implemented -- see `views_appear_in_the_metadata_statements`.)
        assert!(session
            .run("SELECT * FROM information_schema.engines")
            .is_err());
    }

    /// KEY_COLUMN_USAGE, STATISTICS, TABLE_CONSTRAINTS and
    /// REFERENTIAL_CONSTRAINTS -- the introspection tables JDBC/ORM drivers
    /// query -- checked against output captured from a running TiDB for a
    /// table with a BIGINT primary key, a UNIQUE column, and a two-column
    /// plain KEY.
    #[test]
    fn information_schema_jdbc_tables() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE t (id BIGINT PRIMARY KEY, u INT UNIQUE, a INT, b INT, \
                 KEY idx_ab (a, b))",
            )
            .unwrap();

        let query = |session: &mut Session, sql: &str| match session.run_with_columns(sql).unwrap()
        {
            StmtOutput::Rows { columns, rows } => (
                columns
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                rows.into_iter()
                    .map(|row| {
                        row.iter()
                            .map(|value| match value {
                                Datum::Null => "<nil>".to_owned(),
                                Datum::Int(v) => v.to_string(),
                                other => datum_text(other).unwrap_or_default(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>(),
            ),
            other => panic!("expected rows, got {other:?}"),
        };

        // KEY_COLUMN_USAGE: captured header, and one row per PRIMARY/UNIQUE
        // column -- the plain KEY idx_ab does not appear here.
        let (names, rows) = query(
            &mut session,
            "SELECT * FROM information_schema.key_column_usage WHERE table_schema = 'test'",
        );
        assert_eq!(
            names,
            [
                "CONSTRAINT_CATALOG",
                "CONSTRAINT_SCHEMA",
                "CONSTRAINT_NAME",
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "COLUMN_NAME",
                "ORDINAL_POSITION",
                "POSITION_IN_UNIQUE_CONSTRAINT",
                "REFERENCED_TABLE_SCHEMA",
                "REFERENCED_TABLE_NAME",
                "REFERENCED_COLUMN_NAME",
            ]
        );
        assert_eq!(rows.len(), 2, "PRIMARY and u, not idx_ab");
        // Captured: [def test PRIMARY def test t id 1 1 <nil> <nil> <nil>]
        assert_eq!(
            rows[0],
            [
                "def", "test", "PRIMARY", "def", "test", "t", "id", "1", "1", "<nil>", "<nil>",
                "<nil>"
            ]
        );
        // Captured: [def test u def test t u 1 <nil> <nil> <nil> <nil>]
        assert_eq!(
            rows[1],
            [
                "def", "test", "u", "def", "test", "t", "u", "1", "<nil>", "<nil>", "<nil>",
                "<nil>"
            ]
        );

        // STATISTICS: captured header, and one row per indexed column
        // (PRIMARY, then idx_ab's two columns, then u), matching SHOW INDEX's
        // population under this table's own column set.
        let (names, rows) = query(
            &mut session,
            "SELECT * FROM information_schema.statistics WHERE table_schema = 'test'",
        );
        assert_eq!(
            names,
            [
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "NON_UNIQUE",
                "INDEX_SCHEMA",
                "INDEX_NAME",
                "SEQ_IN_INDEX",
                "COLUMN_NAME",
                "COLLATION",
                "CARDINALITY",
                "SUB_PART",
                "PACKED",
                "NULLABLE",
                "INDEX_TYPE",
                "COMMENT",
                "INDEX_COMMENT",
                "IS_VISIBLE",
                "Expression",
            ]
        );
        assert_eq!(rows.len(), 4);
        // Captured: [def test t 0 test PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil>]
        assert_eq!(
            rows[0],
            [
                "def", "test", "t", "0", "test", "PRIMARY", "1", "id", "A", "0", "<nil>", "<nil>",
                "", "BTREE", "", "", "YES", "<nil>"
            ]
        );
        // Captured: [def test t 1 test idx_ab 1 a A 0 <nil> <nil> YES BTREE   YES <nil>]
        assert_eq!(
            rows[1],
            [
                "def", "test", "t", "1", "test", "idx_ab", "1", "a", "A", "0", "<nil>", "<nil>",
                "YES", "BTREE", "", "", "YES", "<nil>"
            ]
        );
        assert_eq!(rows[2][6], "2", "idx_ab's second column, SEQ_IN_INDEX");
        assert_eq!(rows[2][7], "b");
        // Captured: [def test t 0 test u 1 u A 0 <nil> <nil> YES BTREE   YES <nil>]
        assert_eq!(
            rows[3],
            [
                "def", "test", "t", "0", "test", "u", "1", "u", "A", "0", "<nil>", "<nil>", "YES",
                "BTREE", "", "", "YES", "<nil>"
            ]
        );

        // TABLE_CONSTRAINTS: captured header, one row per PRIMARY/UNIQUE
        // constraint (not per column).
        let (names, rows) = query(
            &mut session,
            "SELECT * FROM information_schema.table_constraints WHERE table_schema = 'test'",
        );
        assert_eq!(
            names,
            [
                "CONSTRAINT_CATALOG",
                "CONSTRAINT_SCHEMA",
                "CONSTRAINT_NAME",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "CONSTRAINT_TYPE",
            ]
        );
        assert_eq!(
            rows,
            vec![
                vec!["def", "test", "PRIMARY", "test", "t", "PRIMARY KEY"],
                vec!["def", "test", "u", "test", "t", "UNIQUE"],
            ]
        );

        // REFERENTIAL_CONSTRAINTS: captured header, always empty in this
        // tier (no foreign keys).
        let (names, rows) = query(
            &mut session,
            "SELECT * FROM information_schema.referential_constraints",
        );
        assert_eq!(
            names,
            [
                "CONSTRAINT_CATALOG",
                "CONSTRAINT_SCHEMA",
                "CONSTRAINT_NAME",
                "UNIQUE_CONSTRAINT_CATALOG",
                "UNIQUE_CONSTRAINT_SCHEMA",
                "UNIQUE_CONSTRAINT_NAME",
                "MATCH_OPTION",
                "UPDATE_RULE",
                "DELETE_RULE",
                "TABLE_NAME",
                "REFERENCED_TABLE_NAME",
            ]
        );
        assert!(rows.is_empty());

        // A WHERE filter runs through the ordinary plan path.
        let (_, rows) = query(
            &mut session,
            "SELECT * FROM information_schema.statistics WHERE table_name = 't' AND index_name = 'idx_ab'",
        );
        assert_eq!(rows.len(), 2);
    }

    /// The `information_schema` PRIVILEGES family: `SCHEMA_PRIVILEGES`,
    /// `TABLE_PRIVILEGES`, `COLUMN_PRIVILEGES`.
    ///
    /// The surprising part, and the reason this test exists: these three are
    /// DECLARED in Go's `pkg/infoschema/tables.go` but have NO retriever in
    /// `pkg/executor`, so real TiDB serves the header and NEVER a row --
    /// even when grants exist. CAPTURED from `testkit.CreateMockStore` after
    /// `GRANT SELECT, INSERT ON db1.* TO 'u1'@'%'`,
    /// `GRANT ALL PRIVILEGES ON db1.* TO 'u2'@'localhost'`,
    /// `GRANT SELECT ON db1.t1 TO 'u1'@'%' WITH GRANT OPTION` and
    /// `GRANT UPDATE, DELETE ON db1.t1 TO 'u2'@'localhost'`: every
    /// `SELECT *` came back empty and `SELECT COUNT(*)` came back `0`.
    ///
    /// So filling these in from the privilege registry -- which HAS all the
    /// grant data -- would be a DIVERGENCE from Go, not a completion. The
    /// emptiness is the transcreated behavior.
    #[test]
    fn infoschema_privileges_tables_are_header_only() {
        let mut session = Session::new();
        session.attach_privileges(privilege::PrivilegeRegistry::default());
        session.run("CREATE DATABASE db1").unwrap();
        session.run("CREATE TABLE db1.t1 (a INT)").unwrap();
        session.run("CREATE USER 'u1'@'%'").unwrap();
        session.run("CREATE USER 'u2'@'localhost'").unwrap();
        session
            .run("GRANT SELECT, INSERT ON db1.* TO 'u1'@'%'")
            .unwrap();
        session
            .run("GRANT ALL PRIVILEGES ON db1.* TO 'u2'@'localhost'")
            .unwrap();
        // Table scope too, so the emptiness is not just a DB-scope artifact.
        // (Go's capture also used `WITH GRANT OPTION` here; this tier does
        // not model that yet, and it makes no difference to the result --
        // the table is empty either way.)
        session.run("GRANT SELECT ON db1.t1 TO 'u1'@'%'").unwrap();

        let query = |session: &mut Session, sql: &str| match session.run_with_columns(sql).unwrap()
        {
            StmtOutput::Rows { columns, rows } => (
                columns
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                rows,
            ),
            other => panic!("expected rows, got {other:?}"),
        };

        let (names, rows) = query(
            &mut session,
            "SELECT * FROM information_schema.schema_privileges",
        );
        assert_eq!(
            names,
            [
                "GRANTEE",
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "PRIVILEGE_TYPE",
                "IS_GRANTABLE",
            ]
        );
        assert!(rows.is_empty(), "grants must NOT surface here");

        let (names, rows) = query(
            &mut session,
            "SELECT * FROM information_schema.table_privileges",
        );
        assert_eq!(
            names,
            [
                "GRANTEE",
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "PRIVILEGE_TYPE",
                "IS_GRANTABLE",
            ]
        );
        assert!(rows.is_empty(), "grants must NOT surface here");

        let (names, rows) = query(
            &mut session,
            "SELECT * FROM information_schema.column_privileges",
        );
        assert_eq!(
            names,
            [
                "GRANTEE",
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "COLUMN_NAME",
                "PRIVILEGE_TYPE",
                "IS_GRANTABLE",
            ]
        );
        assert!(rows.is_empty(), "grants must NOT surface here");

        // Go returns `0`, not an error, for the aggregate over the empty
        // body -- so the tables are real relations, not stubs that fail.
        for table in ["schema_privileges", "table_privileges", "column_privileges"] {
            let (_, rows) = query(
                &mut session,
                &format!("SELECT COUNT(*) FROM information_schema.{table}"),
            );
            assert_eq!(rows, vec![vec![Datum::Int(0)]], "COUNT(*) over {table}");
        }

        // A WHERE filter over the empty body also runs the ordinary plan
        // path rather than erroring on an unknown table.
        let (_, rows) = query(
            &mut session,
            "SELECT grantee FROM information_schema.schema_privileges WHERE table_schema = 'db1'",
        );
        assert!(rows.is_empty());
    }

    /// `JSON_TABLE` is REFUSED, and this test records WHY rather than
    /// leaving it looking like an unfinished port.
    ///
    /// The Go side of this branch does not parse it AT ALL. Captured from
    /// `testkit.CreateMockStore` on `hparser-integration`:
    ///
    /// ```text
    /// SQL:  select * from json_table('[1,2]', '$[*]' columns (v int path '$')) jt
    /// ERR:  [parser:1064]You have an error in your SQL syntax; ... near
    ///       "'[1,2]', '$[*]' columns (v int path '$')) jt"
    /// ```
    ///
    /// The `FOR ORDINALITY` and lateral (`FROM t, JSON_TABLE(t.j, ...)`)
    /// forms fail the same way, and `grep -rni json_table pkg/` finds only
    /// the UNRELATED statistics-dump `JSONTable` struct -- no grammar rule,
    /// no AST node, no executor. There is therefore no Go source to
    /// transcreate; this is a HARD SKIP, not a deferral.
    #[test]
    fn json_table_is_unsupported_upstream() {
        let mut session = Session::new();
        assert!(
            session
                .run(r#"SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS (v INT PATH '$')) t"#)
                .is_err(),
            "JSON_TABLE does not parse in Go either -- it must stay refused"
        );
        assert!(
            session
                .run(
                    r#"SELECT * FROM JSON_TABLE('[{"a":1}]', '$[*]' COLUMNS (o FOR ORDINALITY, a INT PATH '$.a')) AS jt"#
                )
                .is_err(),
            "FOR ORDINALITY form is a Go parse error too"
        );
    }

    /// DROP TABLE, checked against captured TiDB behavior: a missing name is
    /// 1051, IF EXISTS suppresses it, and a mixed list still drops the tables
    /// that exist BEFORE reporting the error.
    #[test]
    fn drop_table() {
        let mut session = Session::new();
        for name in ["d1", "d2", "d3"] {
            session
                .run(&format!("CREATE TABLE {name} (a BIGINT)"))
                .unwrap();
        }
        let tables = |session: &mut Session| match session.run_with_columns("SHOW TABLES").unwrap()
        {
            StmtOutput::Rows { rows, .. } => rows
                .into_iter()
                .map(|row| datum_text(&row[0]).unwrap())
                .collect::<Vec<_>>(),
            other => panic!("expected rows, got {other:?}"),
        };

        // Captured: [schema:1051]Unknown table 'test.nosuch'
        assert!(matches!(
            session.run("DROP TABLE nosuch"),
            Err(DriverError::Schema(SchemaErrorKind::BadTable(_)))
        ));
        // Captured: IF EXISTS is a no-op.
        session.run("DROP TABLE IF EXISTS nosuch").unwrap();

        // Captured: `drop table d1, nosuch` errors AND still drops d1.
        assert!(matches!(
            session.run("DROP TABLE d1, nosuch"),
            Err(DriverError::Schema(SchemaErrorKind::BadTable(_)))
        ));
        assert_eq!(tables(&mut session), vec!["d2".to_owned(), "d3".to_owned()]);

        // A multi-table drop removes them all.
        session.run("DROP TABLE d2, d3").unwrap();
        assert!(tables(&mut session).is_empty());

        // A dropped name can be recreated with a different shape, so the drop
        // removed the metadata rather than only the rows.
        session.run("CREATE TABLE d2 (b BIGINT)").unwrap();
        assert_eq!(
            session.run("SELECT b FROM d2").unwrap(),
            StmtResult::Rows(vec![])
        );
        // The rows are gone too: a recreated table starts empty.
        session.run("INSERT INTO d2 VALUES (1)").unwrap();
        session.run("DROP TABLE d2").unwrap();
        session.run("CREATE TABLE d2 (b BIGINT)").unwrap();
        assert_eq!(
            session.run("SELECT b FROM d2").unwrap(),
            StmtResult::Rows(vec![])
        );
    }

    /// ALTER TABLE ADD/DROP COLUMN, checked against captured TiDB behavior.
    /// The one that matters most: ADD COLUMN ... DEFAULT 7 makes rows written
    /// EARLIER read back 7, without rewriting them.
    #[test]
    fn alter_table_columns() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE a (id BIGINT PRIMARY KEY, v BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO a VALUES (1, 10), (2, 20)")
            .unwrap();

        // Captured: [[1 10 7] [2 20 7]] -- the existing rows take the default.
        session
            .run("ALTER TABLE a ADD COLUMN w BIGINT DEFAULT 7")
            .unwrap();
        assert_eq!(
            session.run("SELECT id, v, w FROM a").unwrap(),
            StmtResult::Rows(vec![
                vec![Datum::Int(1), Datum::Int(10), Datum::Int(7)],
                vec![Datum::Int(2), Datum::Int(20), Datum::Int(7)],
            ])
        );
        // Captured: without a default the existing rows read NULL.
        session.run("ALTER TABLE a ADD COLUMN x BIGINT").unwrap();
        assert_eq!(
            session.run("SELECT id, x FROM a").unwrap(),
            StmtResult::Rows(vec![
                vec![Datum::Int(1), Datum::Null],
                vec![Datum::Int(2), Datum::Null],
            ])
        );

        let columns = |session: &mut Session| match session.run_with_columns("DESCRIBE a").unwrap()
        {
            StmtOutput::Rows { rows, .. } => rows
                .into_iter()
                .map(|row| datum_text(&row[0]).unwrap())
                .collect::<Vec<_>>(),
            other => panic!("expected rows, got {other:?}"),
        };
        // Captured order after FIRST then AFTER v: y, id, v, z, w, x.
        session
            .run("ALTER TABLE a ADD COLUMN y BIGINT FIRST")
            .unwrap();
        session
            .run("ALTER TABLE a ADD COLUMN z BIGINT AFTER v")
            .unwrap();
        assert_eq!(columns(&mut session), ["y", "id", "v", "z", "w", "x"]);

        // A new column is written and read like any other, and the rows that
        // predate it still report their defaults.
        session
            .run("INSERT INTO a (id, v, w, x, y, z) VALUES (3, 30, 1, 2, 3, 4)")
            .unwrap();
        assert_eq!(
            session.run("SELECT w FROM a WHERE id = 3").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: DROP COLUMN removes it from the schema.
        session.run("ALTER TABLE a DROP COLUMN w").unwrap();
        assert_eq!(columns(&mut session), ["y", "id", "v", "z", "x"]);
        assert!(session.run("SELECT w FROM a").is_err());

        // Captured error codes.
        assert!(matches!(
            session.run("ALTER TABLE a ADD COLUMN v BIGINT"),
            Err(DriverError::DuplicateColumnName(_))
        ));
        assert!(matches!(
            session.run("ALTER TABLE a DROP COLUMN nosuch"),
            Err(DriverError::UnknownColumnInAlter(_))
        ));
        session.run("CREATE TABLE one (a BIGINT)").unwrap();
        assert!(matches!(
            session.run("ALTER TABLE one DROP COLUMN a"),
            Err(DriverError::CannotDropOnlyColumn { .. })
        ));
        assert!(matches!(
            session.run("ALTER TABLE a DROP COLUMN id"),
            Err(DriverError::UnsupportedDropIntegerPrimaryKey)
        ));

        // Captured: a SINGLE-column index is dropped along with its column,
        // while a COMPOSITE one refuses the drop with 8200.
        session
            .run("CREATE TABLE ix (a BIGINT, b BIGINT, KEY kb (b))")
            .unwrap();
        session.run("INSERT INTO ix VALUES (1, 2)").unwrap();
        session.run("ALTER TABLE ix DROP COLUMN b").unwrap();
        let create_ix = match session.run_with_columns("SHOW CREATE TABLE ix").unwrap() {
            StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };
        assert!(!create_ix.contains("kb"), "the index went with the column");
        assert_eq!(
            session.run("SELECT a FROM ix").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // A unique single-column index behaves the same way.
        session
            .run("CREATE TABLE uq (a BIGINT, b BIGINT, UNIQUE KEY ua (a))")
            .unwrap();
        session.run("ALTER TABLE uq DROP COLUMN a").unwrap();

        // A composite index refuses it, and the table is unchanged.
        session
            .run("CREATE TABLE comp (a BIGINT, b BIGINT, c BIGINT, KEY kab (a, b))")
            .unwrap();
        assert!(matches!(
            session.run("ALTER TABLE comp DROP COLUMN a"),
            Err(DriverError::CannotDropColumnWithCompositeIndex(_))
        ));
        let create_comp = match session.run_with_columns("SHOW CREATE TABLE comp").unwrap() {
            StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };
        assert!(create_comp.contains("KEY `kab` (`a`,`b`)"));

        // An unknown table is an error, and an action this tier does not
        // implement is still rejected rather than ignored. (RENAME TO used to
        // be this example; it is implemented now.)
        assert!(session
            .run("ALTER TABLE nosuch ADD COLUMN a BIGINT")
            .is_err());
        assert!(session.run("ALTER TABLE a ORDER BY v").is_err());
    }

    /// CREATE INDEX / DROP INDEX / ALTER TABLE ADD INDEX, checked against
    /// captured TiDB behavior -- including that CREATE INDEX backfills the
    /// rows that already exist.
    #[test]
    fn index_ddl() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE i1 (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO i1 VALUES (1, 10, 1), (2, 20, 1), (3, 10, 2)")
            .unwrap();

        // The index is backfilled, so it finds rows written before it existed.
        // Captured: select id from i1 where a = 10 -> [[1] [3]].
        session.run("CREATE INDEX ia ON i1 (a)").unwrap();
        assert_eq!(
            session.run("SELECT id FROM i1 WHERE a = 10").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(3)]])
        );
        // SHOW CREATE TABLE reports it, captured as KEY `ia` (`a`).
        let create =
            |session: &mut Session| match session.run_with_columns("SHOW CREATE TABLE i1").unwrap()
            {
                StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
                other => panic!("expected rows, got {other:?}"),
            };
        assert!(create(&mut session).contains("KEY `ia` (`a`)"));

        // Captured: a duplicate index name is 1061.
        assert!(matches!(
            session.run("CREATE INDEX ia ON i1 (b)"),
            Err(DriverError::DuplicateKeyName(_))
        ));
        // Captured: a unique index over data that already collides is 1062
        // naming table.index, and the index is NOT created.
        match session.run("CREATE UNIQUE INDEX ua ON i1 (a)") {
            Err(DriverError::DuplicateEntry { value, key }) => {
                assert_eq!(value, "10");
                assert_eq!(key, "i1.ua");
            }
            other => panic!("expected a duplicate-entry error, got {other:?}"),
        }
        assert!(!create(&mut session).contains("ua"));

        // A unique index over data that does not collide is created.
        session.run("CREATE UNIQUE INDEX ub ON i1 (b, a)").unwrap();
        assert!(create(&mut session).contains("UNIQUE KEY `ub` (`b`,`a`)"));
        // It is enforced from then on.
        assert!(session.run("INSERT INTO i1 VALUES (4, 10, 1)").is_err());

        // DROP INDEX removes it, and its entries with it: the same insert now
        // succeeds.
        session.run("DROP INDEX ub ON i1").unwrap();
        assert!(!create(&mut session).contains("ub"));
        session.run("INSERT INTO i1 VALUES (4, 10, 1)").unwrap();

        // Captured: dropping one that does not exist is 1091.
        assert!(matches!(
            session.run("DROP INDEX nosuch ON i1"),
            Err(DriverError::UnknownIndex(_))
        ));

        // ALTER TABLE ADD INDEX takes the same path.
        session.run("ALTER TABLE i1 ADD INDEX ic (b)").unwrap();
        assert!(create(&mut session).contains("KEY `ic` (`b`)"));
        session.run("ALTER TABLE i1 DROP INDEX ic").unwrap();
        assert!(!create(&mut session).contains("ic"));

        // An index over an unknown column is rejected.
        assert!(session.run("CREATE INDEX bad ON i1 (nosuch)").is_err());
    }

    /// TRUNCATE TABLE, checked against captured TiDB behavior: the rows go,
    /// the definition stays, and the auto-increment counter restarts.
    #[test]
    fn truncate_table() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT, KEY kv (v))")
            .unwrap();
        session
            .run("INSERT INTO t1 (v) VALUES (1), (2), (3)")
            .unwrap();
        assert_eq!(
            session.run("SELECT id FROM t1").unwrap(),
            StmtResult::Rows(vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(2)],
                vec![Datum::Int(3)]
            ])
        );

        session.run("TRUNCATE TABLE t1").unwrap();
        // Captured: no rows remain.
        assert_eq!(
            session.run("SELECT id FROM t1").unwrap(),
            StmtResult::Rows(vec![])
        );
        // Captured: the next auto-increment insert starts over at 1.
        session.run("INSERT INTO t1 (v) VALUES (9)").unwrap();
        assert_eq!(
            session.run("SELECT id FROM t1").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        // Captured: the definition, including the index, survives.
        let create = match session.run_with_columns("SHOW CREATE TABLE t1").unwrap() {
            StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };
        assert!(create.contains("AUTO_INCREMENT"));
        assert!(create.contains("KEY `kv` (`v`)"));

        // The index entries went with the rows: a read through the index sees
        // only what was written after the truncate.
        assert_eq!(
            session.run("SELECT id FROM t1 WHERE v = 1").unwrap(),
            StmtResult::Rows(vec![])
        );
        assert_eq!(
            session.run("SELECT id FROM t1 WHERE v = 9").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: truncating a table that does not exist is 1146.
        assert!(matches!(
            session.run("TRUNCATE TABLE nosuch"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownTable(_)))
        ));
    }

    /// RENAME TABLE, checked against captured TiDB behavior.
    /// A result's rows as text, so an assertion does not depend on which
    /// datum kind the codec hands back for a given column type.
    fn row_text(result: Result<StmtResult, DriverError>) -> Vec<Vec<String>> {
        match result.unwrap() {
            StmtResult::Rows(rows) => rows
                .into_iter()
                .map(|row| {
                    row.iter()
                        .map(|v| datum_text(v).unwrap_or_else(|| "NULL".to_owned()))
                        .collect()
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// `INSERT ... SET col = value`, checked against captured TiDB output.
    ///
    /// Go normalizes the `SET` list into a column list plus one VALUES row,
    /// so every rule the VALUES form obeys -- defaults, NOT NULL, the column
    /// cast, ON DUPLICATE KEY UPDATE and REPLACE -- applies unchanged.
    #[test]
    fn insert_set_syntax() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10) DEFAULT 'dd', \
                 c BIGINT NOT NULL DEFAULT 5)",
            )
            .unwrap();

        // Captured: the columns it names are assigned, the rest take their
        // defaults.
        assert_eq!(
            session.run("INSERT INTO t SET a = 1, b = 'x'").unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(row_text(session.run("SELECT * FROM t")), [["1", "x", "5"]]);
        session.run("INSERT INTO t SET a = 2").unwrap();
        // Captured: an assigned value may be an expression.
        session.run("INSERT INTO t SET a = 3, c = 1+1").unwrap();
        assert_eq!(
            row_text(session.run("SELECT * FROM t ORDER BY a")),
            [["1", "x", "5"], ["2", "dd", "5"], ["3", "dd", "2"]]
        );

        // Captured: a column with no default that the SET list omits is
        // 1364, the same as in the VALUES form.
        match session.run("INSERT INTO t SET b = 'nope'") {
            Err(error) => assert_eq!(error.to_mysql_error().code, 1364),
            Ok(other) => panic!("expected 1364, got {other:?}"),
        }
        // Captured: an unknown column names the field list.
        match session.run("INSERT INTO t SET nosuch = 1") {
            Err(error) => assert_eq!(error.to_mysql_error().code, 1054),
            Ok(other) => panic!("expected 1054, got {other:?}"),
        }

        // Captured: the conflict policies compose with it.
        assert_eq!(
            session
                .run("INSERT INTO t SET a = 1, b = 'dup' ON DUPLICATE KEY UPDATE b = 'updated'")
                .unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT b FROM t WHERE a = 1")),
            [["updated"]]
        );
        assert_eq!(
            session.run("REPLACE INTO t SET a = 2, b = 'repl'").unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
            [["1", "updated", "5"], ["2", "repl", "5"], ["3", "dd", "2"]]
        );
    }

    /// A DATETIME/DATE column compared with a string or a number, checked
    /// against captured TiDB output.
    ///
    /// This was a SILENT WRONG ANSWER before: the generic string-vs-numeric
    /// rule compared '2024-12-31' by its numeric prefix, so the WHERE clause
    /// every application writes returned the wrong rows without any error.
    #[test]
    fn time_compared_with_strings_and_numbers() {
        let mut session = Session::new();
        session.apply_set("SET time_zone = '+00:00'").unwrap();
        session
            .run("CREATE TABLE t (id BIGINT, created DATETIME, d DATE)")
            .unwrap();
        session
            .run(
                "INSERT INTO t VALUES (1,'2024-06-15 10:00:00','2024-06-15'),\
                 (2,'2024-12-30 23:59:59','2024-12-30'),(3,'2025-01-02 00:00:00','2025-01-02')",
            )
            .unwrap();

        // Captured: a bare date string means that date's midnight.
        assert_eq!(
            row_text(session.run("SELECT id FROM t WHERE created <= '2024-12-31'")),
            [["1"], ["2"]]
        );
        assert_eq!(
            row_text(session.run("SELECT id FROM t WHERE created > '2024-12-31'")),
            [["3"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT id FROM t WHERE created BETWEEN '2024-01-01' AND '2024-12-31 23:59:59'"
            )),
            [["1"], ["2"]]
        );
        // Captured: equality both ways, and against a DATE column.
        assert_eq!(
            row_text(session.run("SELECT id FROM t WHERE created = '2024-06-15 10:00:00'")),
            [["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT '2024-06-15 10:00:00' = created FROM t WHERE id = 1")),
            [["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT id FROM t WHERE d = '2024-06-15'")),
            [["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT id FROM t WHERE d < '2024-12-31'")),
            [["1"], ["2"]]
        );
        // Captured: a bare NUMBER parses as a date too.
        assert_eq!(
            row_text(session.run("SELECT id FROM t WHERE created <= 20241231")),
            [["1"], ["2"]]
        );
        // Captured: garbage filters every row with warning 1292, not an error.
        assert_eq!(
            row_text(session.run("SELECT id FROM t WHERE created <= 'garbage'")),
            Vec::<Vec<String>>::new()
        );
        // DOCUMENTED DIVERGENCE (the standing coprocessor-merge one): TiDB
        // reported ONE 1292 here because its coprocessor merges a batch's
        // warnings; this tier warns once per row compared.
        assert_eq!(session.warnings().len(), 3, "one warning per row compared");
        assert_eq!(session.warnings()[0].code, 1292);
        assert_eq!(
            session.warnings()[0].message,
            "Incorrect datetime value: 'garbage'"
        );
    }

    /// `GROUP_CONCAT`, checked against captured TiDB output.
    #[test]
    fn group_concat() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (g BIGINT, v VARCHAR(10), n BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'b',2),(1,'a',1),(2,'c',3),(2,NULL,4),(1,'a',5)")
            .unwrap();

        // Captured: every non-NULL value joined by a comma, in row order.
        assert_eq!(
            row_text(session.run("SELECT GROUP_CONCAT(v) FROM t")),
            [["b,a,c,a"]]
        );
        // Captured: per group, with the NULL contributing nothing.
        assert_eq!(
            row_text(session.run("SELECT g, GROUP_CONCAT(v) FROM t GROUP BY g ORDER BY g")),
            [["1", "b,a,a"], ["2", "c"]]
        );
        // Captured: an explicit separator.
        assert_eq!(
            row_text(
                session.run("SELECT g, GROUP_CONCAT(v SEPARATOR '-') FROM t GROUP BY g ORDER BY g")
            ),
            [["1", "b-a-a"], ["2", "c"]]
        );
        // Captured: DISTINCT folds the repeat. TiDB's own output for this
        // group is `a,b`; MySQL documents the order of a GROUP_CONCAT
        // without ORDER BY as undefined, so only the membership is asserted.
        let distinct = row_text(
            session.run("SELECT g, GROUP_CONCAT(DISTINCT v) FROM t GROUP BY g ORDER BY g"),
        );
        let mut first: Vec<&str> = distinct[0][1].split(',').collect();
        first.sort_unstable();
        assert_eq!(first, ["a", "b"]);
        assert_eq!(distinct[1][1], "c");
        // Captured: numbers are stringified.
        assert_eq!(
            row_text(session.run("SELECT GROUP_CONCAT(n) FROM t")),
            [["2,1,3,4,5"]]
        );
        // Captured: an empty group is NULL, not an empty string.
        assert_eq!(
            row_text(session.run("SELECT GROUP_CONCAT(v) FROM t WHERE g = 99")),
            [["NULL"]]
        );

        // Captured: the aggregate's own ORDER BY orders the rows WITHIN the
        // concatenation -- a separate scope from the query's ORDER BY.
        assert_eq!(
            row_text(
                session.run("SELECT g, GROUP_CONCAT(v ORDER BY v) FROM t GROUP BY g ORDER BY g")
            ),
            [["1", "a,a,b"], ["2", "c"]]
        );
        // Captured: it may order by a column the concatenation does not
        // contain, descending, with its own separator.
        assert_eq!(
            row_text(session.run(
                "SELECT g, GROUP_CONCAT(v ORDER BY n DESC SEPARATOR '|') FROM t \
                 GROUP BY g ORDER BY g"
            )),
            [["1", "a|b|a"], ["2", "c"]]
        );

        // The multi-argument form: captured from TiDB, the arguments are
        // concatenated PER ROW (like CONCAT) before the rows are joined, and
        // a row is dropped as soon as ANY of its arguments is NULL -- not
        // only when all of them are.
        session.run("INSERT INTO t VALUES (2,'d',NULL)").unwrap();
        session.run("INSERT INTO t VALUES (1,'a',1)").unwrap();
        // (2,NULL,4) and (2,'d',NULL) each have one NULL argument: both drop.
        assert_eq!(
            row_text(session.run("SELECT g, GROUP_CONCAT(v, n) FROM t GROUP BY g ORDER BY g")),
            [["1", "b2,a1,a5,a1"], ["2", "c3"]]
        );
        // ...while the one-argument form still keeps 'd' (its v is not NULL).
        assert_eq!(
            row_text(session.run("SELECT GROUP_CONCAT(v) FROM t WHERE g = 2")),
            [["c,d"]]
        );
        // Captured: DISTINCT dedupes over the CONCATENATED per-row value, so
        // the repeated ('a',1) folds while ('a',5) survives. Row order
        // without ORDER BY is undefined; assert membership only.
        let multi = row_text(
            session.run("SELECT g, GROUP_CONCAT(DISTINCT v, n) FROM t GROUP BY g ORDER BY g"),
        );
        let mut first: Vec<&str> = multi[0][1].split(',').collect();
        first.sort_unstable();
        assert_eq!(first, ["a1", "a5", "b2"]);
        assert_eq!(multi[1][1], "c3");
        // Captured: a literal argument concatenates like any other.
        assert_eq!(
            row_text(session.run("SELECT g, GROUP_CONCAT(v, '-', n) FROM t GROUP BY g ORDER BY g")),
            [["1", "b-2,a-1,a-5,a-1"], ["2", "c-3"]]
        );
        // Captured: multi-arg with the aggregate's own ORDER BY and separator.
        assert_eq!(
            row_text(session.run(
                "SELECT g, GROUP_CONCAT(v, n ORDER BY n DESC SEPARATOR '|') FROM t \
                 GROUP BY g ORDER BY g"
            )),
            [["1", "a5|b2|a1|a1"], ["2", "c3"]]
        );
    }

    /// Prepared-statement parameters: the marker count a PREPARE reports and
    /// the values an EXECUTE binds.
    ///
    /// This is the session half of the binary protocol -- what a JDBC or Go
    /// driver client needs to run anything at all. The wire half wires
    /// `COM_STMT_PREPARE`/`EXECUTE` to it.
    #[test]
    fn prepared_statement_parameters() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
            .unwrap();

        // The marker count is what PREPARE reports.
        assert_eq!(
            session
                .parameter_count("SELECT a FROM t WHERE a = ?")
                .unwrap(),
            1
        );
        assert_eq!(
            session
                .parameter_count("INSERT INTO t (a,b,c) VALUES (?,?,?)")
                .unwrap(),
            3
        );
        assert_eq!(session.parameter_count("SELECT 1").unwrap(), 0);
        assert_eq!(
            session
                .parameter_count("SELECT a FROM t WHERE b LIKE ? AND c BETWEEN ? AND ?")
                .unwrap(),
            3
        );

        // An INSERT binds its values positionally.
        session
            .run_with_params(
                "INSERT INTO t (a,b,c) VALUES (?,?,?)",
                &[Datum::Int(1), Datum::Bytes(b"one".to_vec()), Datum::Int(10)],
            )
            .unwrap();
        session
            .run_with_params(
                "INSERT INTO t (a,b,c) VALUES (?,?,?)",
                &[Datum::Int(2), Datum::Bytes(b"two".to_vec()), Datum::Int(20)],
            )
            .unwrap();

        // A SELECT binds in WHERE, and the markers keep their source order.
        let output = session
            .run_with_params("SELECT b FROM t WHERE a = ?", &[Datum::Int(2)])
            .unwrap();
        match output {
            StmtOutput::Rows { rows, .. } => {
                assert_eq!(datum_text(&rows[0][0]).unwrap(), "two");
            }
            other => panic!("expected rows, got {other:?}"),
        }
        let output = session
            .run_with_params(
                "SELECT a FROM t WHERE c BETWEEN ? AND ? ORDER BY a",
                &[Datum::Int(5), Datum::Int(15)],
            )
            .unwrap();
        match output {
            StmtOutput::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            other => panic!("expected rows, got {other:?}"),
        }

        // A value that is not UTF-8 survives the round trip as a hex literal
        // rather than being mangled by a lossy conversion.
        session
            .run_with_params(
                "INSERT INTO t (a,b,c) VALUES (?,?,?)",
                &[
                    Datum::Int(3),
                    Datum::Bytes(vec![0xff, 0xfe, b'z']),
                    Datum::Int(30),
                ],
            )
            .unwrap();
        match session
            .run_with_params("SELECT b FROM t WHERE a = ?", &[Datum::Int(3)])
            .unwrap()
        {
            StmtOutput::Rows { rows, .. } => {
                let stored = match &rows[0][0] {
                    Datum::Bytes(bytes) => bytes.clone(),
                    Datum::String(text) => text.bytes().to_vec(),
                    other => panic!("expected a string datum, got {other:?}"),
                };
                assert_eq!(stored, vec![0xff, 0xfe, b'z']);
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // A NULL parameter binds as NULL, not as the text "NULL".
        session
            .run_with_params(
                "INSERT INTO t (a,b,c) VALUES (?,?,?)",
                &[Datum::Int(4), Datum::Null, Datum::Int(40)],
            )
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE b IS NULL")),
            [["4"]]
        );

        // Too few or too many values is Go's ErrWrongParamCount (1210).
        match session.run_with_params("SELECT a FROM t WHERE a = ?", &[]) {
            Ok(_) => panic!("an unbound marker should fail"),
            Err(error) => assert_eq!(error.to_mysql_error().code, 1210),
        }
        match session.run_with_params(
            "SELECT a FROM t WHERE a = ?",
            &[Datum::Int(1), Datum::Int(2)],
        ) {
            Ok(_) => panic!("an extra value should fail"),
            Err(error) => assert_eq!(error.to_mysql_error().code, 1210),
        }
    }

    /// `SHOW TABLE STATUS`, checked against captured TiDB output -- the
    /// 18-column header GUI clients read to list a schema.
    ///
    /// NOT MODELLED, and reported the way Go reports an absent value rather
    /// than invented: every size and count is 0, which is also what TiDB
    /// answers without a statistics tier, and the three timestamps are NULL
    /// because this tier stores none.
    #[test]
    fn show_table_status() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10))")
            .unwrap();
        session.run("CREATE TABLE u (x BIGINT)").unwrap();
        session.run("INSERT INTO t VALUES (1,'p'),(2,'q')").unwrap();

        match session.run_with_columns("SHOW TABLE STATUS").unwrap() {
            StmtOutput::Rows { columns, .. } => assert_eq!(
                columns
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
                [
                    "Name",
                    "Engine",
                    "Version",
                    "Row_format",
                    "Rows",
                    "Avg_row_length",
                    "Data_length",
                    "Max_data_length",
                    "Index_length",
                    "Data_free",
                    "Auto_increment",
                    "Create_time",
                    "Update_time",
                    "Check_time",
                    "Collation",
                    "Checksum",
                    "Create_options",
                    "Comment",
                ]
            ),
            other => panic!("expected rows, got {other:?}"),
        }

        // Captured: one row per table, with the engine, version, row format
        // and collation TiDB reports.
        let rows = row_text(session.run("SHOW TABLE STATUS"));
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0][0], "t");
        assert_eq!(rows[1][0], "u");
        assert_eq!(rows[0][1], "InnoDB");
        assert_eq!(rows[0][2], "10");
        assert_eq!(rows[0][3], "Compact");
        assert_eq!(rows[0][14], "utf8mb4_bin");
        // Captured: Auto_increment is NULL for a table with no auto column.
        assert_eq!(rows[0][10], "NULL");

        // Captured: the LIKE filter narrows to one table.
        let filtered = row_text(session.run("SHOW TABLE STATUS LIKE 't'"));
        assert_eq!(filtered.len(), 1, "{filtered:?}");
        assert_eq!(filtered[0][0], "t");

        // A table with an auto column reports its next value there.
        session
            .run("CREATE TABLE g (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)")
            .unwrap();
        session.run("INSERT INTO g (v) VALUES (1), (2)").unwrap();
        let auto = row_text(session.run("SHOW TABLE STATUS LIKE 'g'"));
        assert_eq!(auto[0][10], "3", "{auto:?}");

        // The WHERE form filters the same virtual rows.
        let named = row_text(session.run("SHOW TABLE STATUS WHERE Name = 'u'"));
        assert_eq!(named.len(), 1, "{named:?}");
        assert_eq!(named[0][0], "u");
    }

    /// `SHOW INDEX` / `SHOW KEYS`, checked against captured TiDB output --
    /// the full 17-column header and one row per index column.
    #[test]
    fn show_index_reports_each_index_column() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT, \
                 UNIQUE KEY ub (b), KEY bc (b,c))",
            )
            .unwrap();

        match session.run_with_columns("SHOW INDEX FROM t").unwrap() {
            StmtOutput::Rows { columns, .. } => assert_eq!(
                columns
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
                [
                    "Table",
                    "Non_unique",
                    "Key_name",
                    "Seq_in_index",
                    "Column_name",
                    "Collation",
                    "Cardinality",
                    "Sub_part",
                    "Packed",
                    "Null",
                    "Index_type",
                    "Comment",
                    "Index_comment",
                    "Visible",
                    "Expression",
                    "Clustered",
                    "Global",
                ]
            ),
            other => panic!("expected rows, got {other:?}"),
        }

        // Captured: the clustered primary key first, then each index in
        // definition order, one row per index column with its 1-based
        // position. Non_unique is 0 for a unique index.
        let rows = row_text(session.run("SHOW INDEX FROM t"));
        let summary: Vec<Vec<&str>> = rows
            .iter()
            .map(|row| {
                vec![
                    row[1].as_str(),  // Non_unique
                    row[2].as_str(),  // Key_name
                    row[3].as_str(),  // Seq_in_index
                    row[4].as_str(),  // Column_name
                    row[9].as_str(),  // Null
                    row[15].as_str(), // Clustered
                ]
            })
            .collect();
        assert_eq!(
            summary,
            [
                ["0", "PRIMARY", "1", "a", "", "YES"],
                ["0", "ub", "1", "b", "YES", "NO"],
                ["1", "bc", "1", "b", "YES", "NO"],
                ["1", "bc", "2", "c", "YES", "NO"],
            ]
        );
        // Captured: SHOW KEYS is the same statement.
        assert_eq!(row_text(session.run("SHOW KEYS FROM t")), rows);
    }

    /// One connection sends everything through one door: the transaction
    /// controls, `SET`, and `SHOW VARIABLES` all answer from `run` now.
    ///
    /// Checked against captured TiDB output: the columns are
    /// `Variable_name` and `Value`, the LIKE pattern filters, and a SET is
    /// visible to the next SHOW.
    #[test]
    fn run_routes_session_statements() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT)").unwrap();

        // The transaction controls answer through `run`.
        session.run("BEGIN").unwrap();
        session.run("INSERT INTO t VALUES (1)").unwrap();
        session.run("COMMIT").unwrap();
        assert_eq!(row_text(session.run("SELECT a FROM t")), [["1"]]);
        session.run("BEGIN").unwrap();
        session.run("INSERT INTO t VALUES (2)").unwrap();
        session.run("ROLLBACK").unwrap();
        assert_eq!(row_text(session.run("SELECT a FROM t")), [["1"]]);

        // So does SET.
        session.run("SET autocommit = 0").unwrap();

        // Captured: SHOW VARIABLES reports Variable_name/Value, filtered.
        match session
            .run_with_columns("SHOW VARIABLES LIKE 'autocommit'")
            .unwrap()
        {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(
                    columns
                        .iter()
                        .map(|(name, _)| name.as_str())
                        .collect::<Vec<_>>(),
                    ["Variable_name", "Value"]
                );
                assert_eq!(rows.len(), 1);
                assert_eq!(datum_text(&rows[0][0]).unwrap(), "autocommit");
            }
            other => panic!("expected rows, got {other:?}"),
        }
        // Captured: sql_mode reports the session's own value.
        assert_eq!(
            row_text(session.run("SHOW VARIABLES LIKE 'sql_mode'")),
            [[
                "sql_mode".to_owned(),
                session.vars().get_system("sql_mode").unwrap()
            ]]
        );
        // Captured: a wildcard pattern matches a prefix family.
        let matched = row_text(session.run("SHOW VARIABLES LIKE 'max_allowed%'"));
        assert!(
            matched.iter().any(|row| row[0] == "max_allowed_packet"),
            "{matched:?}"
        );
        // A SET is visible to the next SHOW.
        session.run("SET autocommit = 1").unwrap();
        assert_eq!(
            row_text(session.run("SHOW VARIABLES LIKE 'autocommit'"))[0][1],
            session.vars().get_system("autocommit").unwrap()
        );

        // Captured: the scoped spellings a JDBC client sends read the same
        // session value here.
        assert_eq!(
            row_text(session.run("SELECT @@session.autocommit, @@global.autocommit")).len(),
            1
        );

        // Captured: the WHERE form filters the same virtual rows, including
        // over the Value column and with a case-insensitive column name.
        assert_eq!(
            row_text(session.run("SHOW VARIABLES WHERE variable_name = 'autocommit'"))[0][0],
            "autocommit"
        );
        let pair = row_text(
            session.run("SHOW VARIABLES WHERE Variable_name IN ('autocommit','sql_mode')"),
        );
        assert_eq!(pair.len(), 2, "{pair:?}");
        assert_eq!(pair[0][0], "autocommit");
        assert_eq!(pair[1][0], "sql_mode");
        let both = row_text(
            session.run("SHOW VARIABLES WHERE value = 'ON' AND variable_name LIKE 'auto%'"),
        );
        assert!(both.iter().any(|row| row[0] == "autocommit"), "{both:?}");
    }

    /// `SHOW STATUS`, checked against captured TiDB output: the columns are
    /// `Variable_name` and `Value`, `Ssl_cipher` is empty, `Compression` is
    /// `OFF`, LIKE and WHERE filter the rows, and GLOBAL scope drops the
    /// session-only `Compression*` family.
    #[test]
    fn show_status() {
        let mut session = Session::new();

        // Captured: COLUMNS [Variable_name Value], ROW [Ssl_cipher ].
        match session
            .run_with_columns("SHOW STATUS LIKE 'Ssl_cipher'")
            .unwrap()
        {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(
                    columns
                        .iter()
                        .map(|(name, _)| name.as_str())
                        .collect::<Vec<_>>(),
                    ["Variable_name", "Value"]
                );
                assert_eq!(rows.len(), 1);
                assert_eq!(datum_text(&rows[0][0]).unwrap(), "Ssl_cipher");
                assert_eq!(datum_text(&rows[0][1]).unwrap(), "");
            }
            other => panic!("expected rows, got {other:?}"),
        }
        // Captured: ROW [Compression OFF].
        assert_eq!(
            row_text(session.run("SHOW STATUS LIKE 'Compression'")),
            [["Compression", "OFF"]]
        );
        // Captured: SHOW GLOBAL STATUS LIKE 'Ssl%' lists the whole family.
        assert_eq!(
            row_text(session.run("SHOW GLOBAL STATUS LIKE 'Ssl%'")),
            [
                ["Ssl_cipher", ""],
                ["Ssl_cipher_list", ""],
                ["Ssl_verify_mode", "0"],
                ["Ssl_version", ""],
            ]
        );
        // Captured: the WHERE form filters the same virtual rows.
        assert_eq!(
            row_text(session.run("SHOW STATUS WHERE Variable_name = 'Compression'")),
            [["Compression", "OFF"]]
        );
        // Captured: GLOBAL scope drops the session-only Compression* rows,
        // and SESSION is the unscoped spelling.
        let session_rows = row_text(session.run("SHOW SESSION STATUS"));
        assert!(
            session_rows.iter().any(|row| row[0] == "Compression"),
            "{session_rows:?}"
        );
        let global_rows = row_text(session.run("SHOW GLOBAL STATUS"));
        assert!(
            global_rows
                .iter()
                .all(|row| !row[0].starts_with("Compression")),
            "{global_rows:?}"
        );
        assert!(
            global_rows.iter().any(|row| row[0] == "Ssl_version"),
            "{global_rows:?}"
        );
    }

    /// `SHOW CHARSET`, `SHOW ENGINES`, and `SHOW COLLATION`, checked against
    /// a mock-TiDB capture: 7 SHOW CHARSET rows, one InnoDB SHOW ENGINES row,
    /// and 15 SHOW COLLATION rows (LIKE 'utf8mb4%' narrows to 5).
    #[test]
    fn show_charset_engines_collation() {
        let mut session = Session::new();

        assert_eq!(
            row_text(session.run("SHOW CHARSET")),
            [
                ["ascii", "US ASCII", "ascii_bin", "1"],
                ["binary", "binary", "binary", "1"],
                [
                    "gb18030",
                    "China National Standard GB18030",
                    "gb18030_chinese_ci",
                    "4"
                ],
                [
                    "gbk",
                    "Chinese Internal Code Specification",
                    "gbk_chinese_ci",
                    "2"
                ],
                ["latin1", "Latin1", "latin1_bin", "1"],
                ["utf8", "UTF-8 Unicode", "utf8_bin", "3"],
                ["utf8mb4", "UTF-8 Unicode", "utf8mb4_bin", "4"],
            ]
        );

        assert_eq!(
            row_text(session.run("SHOW ENGINES")),
            [[
                "InnoDB",
                "DEFAULT",
                "Supports transactions, row-level locking, and foreign keys",
                "YES",
                "YES",
                "YES",
            ]]
        );

        let collation_rows = row_text(session.run("SHOW COLLATION"));
        assert_eq!(collation_rows.len(), 15);
        assert_eq!(
            collation_rows[0],
            ["ascii_bin", "ascii", "65", "Yes", "Yes", "1", "PAD SPACE"]
        );
        assert_eq!(
            collation_rows[1],
            ["binary", "binary", "63", "Yes", "Yes", "1", "NO PAD"]
        );

        assert_eq!(
            row_text(session.run("SHOW COLLATION LIKE 'utf8mb4%'")),
            [
                [
                    "utf8mb4_0900_ai_ci",
                    "utf8mb4",
                    "255",
                    "",
                    "Yes",
                    "0",
                    "NO PAD"
                ],
                [
                    "utf8mb4_0900_bin",
                    "utf8mb4",
                    "309",
                    "",
                    "Yes",
                    "1",
                    "NO PAD"
                ],
                [
                    "utf8mb4_bin",
                    "utf8mb4",
                    "46",
                    "Yes",
                    "Yes",
                    "1",
                    "PAD SPACE"
                ],
                [
                    "utf8mb4_general_ci",
                    "utf8mb4",
                    "45",
                    "",
                    "Yes",
                    "1",
                    "PAD SPACE"
                ],
                [
                    "utf8mb4_unicode_ci",
                    "utf8mb4",
                    "224",
                    "",
                    "Yes",
                    "8",
                    "PAD SPACE"
                ],
            ]
        );

        assert!(session.run("SHOW CHARSET WHERE Charset = 'utf8'").is_err());
    }

    /// The three conflict policies -- `REPLACE`, `INSERT IGNORE` and
    /// `ON DUPLICATE KEY UPDATE` -- checked against captured TiDB output,
    /// including the affected-row counts, which is how MySQL clients tell
    /// an insert from an update.
    #[test]
    fn insert_conflict_policies() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT, UNIQUE KEY ub (b))",
            )
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'p',10),(2,'q',20)")
            .unwrap();

        // Captured: an update that changes nothing affects no rows, and
        // raises no warning.
        assert_eq!(
            session
                .run("INSERT INTO t (a,b,c) VALUES (1,'p',10) ON DUPLICATE KEY UPDATE c = c")
                .unwrap(),
            StmtResult::Affected(0)
        );
        assert!(session.warnings().is_empty());

        // Captured: VALUES(c) is the value the insert would have written, and
        // a real update affects two rows.
        assert_eq!(
            session
                .run(
                    "INSERT INTO t (a,b,c) VALUES (1,'p',77) ON DUPLICATE KEY UPDATE c = VALUES(c)"
                )
                .unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT c FROM t WHERE a = 1")),
            [["77"]]
        );

        // Captured: the conflict is found on a UNIQUE INDEX too, and the
        // assignment updates THAT row -- the candidate's own key is never
        // inserted.
        assert_eq!(
            session
                .run("INSERT INTO t (a,b,c) VALUES (9,'q',5) ON DUPLICATE KEY UPDATE c = 42")
                .unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
            [["1", "p", "77"], ["2", "q", "42"]]
        );

        // Captured: the assignments read the EXISTING row.
        assert_eq!(
            session
                .run("INSERT INTO t (a,b,c) VALUES (1,'p',1000) ON DUPLICATE KEY UPDATE c = c + 1")
                .unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT c FROM t WHERE a = 1")),
            [["78"]]
        );

        // Captured: INSERT IGNORE skips the conflicting row with a 1062
        // warning and inserts the rest.
        assert_eq!(
            session
                .run("INSERT IGNORE INTO t (a,b,c) VALUES (1,'zzz',1),(5,'five',5)")
                .unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1062);
        assert_eq!(
            session.warnings()[0].message,
            "Duplicate entry '1' for key 't.PRIMARY'"
        );

        // Captured: REPLACE deletes EVERY row it collides with -- here one on
        // the primary key and another on the unique key -- and the affected
        // count is one per deleted row plus one for the inserted row.
        assert_eq!(
            session
                .run("REPLACE INTO t (a,b,c) VALUES (2,'five',99)")
                .unwrap(),
            StmtResult::Affected(3)
        );
        assert_eq!(
            row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
            [["1", "p", "78"], ["2", "five", "99"]]
        );
        // Captured: a REPLACE with no conflict is a plain insert.
        assert_eq!(
            session
                .run("REPLACE INTO t (a,b,c) VALUES (77,'new',1)")
                .unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY a")),
            [["1"], ["2"], ["77"]]
        );
    }

    /// `INSERT ... SELECT` and the `ORDER BY`/`LIMIT` forms of UPDATE and
    /// DELETE, checked against captured TiDB output.
    ///
    /// STILL REFUSED, each recorded at its gate: `REPLACE INTO`,
    /// `INSERT IGNORE`, `ON DUPLICATE KEY UPDATE` (all three need
    /// conflict-time row replacement), the `SET` insert syntax, and
    /// partitions. `RETURNING` is parsed and silently ignored, matching Go
    /// (testkit probe: the write succeeds with a plain OK, no result set,
    /// no warning).
    #[test]
    fn insert_select_and_ordered_dml() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT)")
            .unwrap();
        session
            .run("CREATE TABLE u (x BIGINT, y VARCHAR(10))")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'p',10),(2,'q',20),(3,'r',30)")
            .unwrap();
        session
            .run("INSERT INTO u VALUES (7,'seven'),(8,'eight')")
            .unwrap();

        // Captured: INSERT ... SELECT inserts the query's rows, and the
        // columns it does not name stay NULL.
        assert_eq!(
            session
                .run("INSERT INTO t (a,b) SELECT x, y FROM u")
                .unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
            [
                ["1", "p", "10"],
                ["2", "q", "20"],
                ["3", "r", "30"],
                ["7", "seven", "NULL"],
                ["8", "eight", "NULL"],
            ]
        );

        // Captured: UPDATE ... ORDER BY ... LIMIT updates that many rows, in
        // that order -- here the largest `a`.
        assert_eq!(
            session
                .run("UPDATE t SET c = 99 ORDER BY a DESC LIMIT 1")
                .unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(
            row_text(session.run("SELECT a, c FROM t ORDER BY a")),
            [
                ["1", "10"],
                ["2", "20"],
                ["3", "30"],
                ["7", "NULL"],
                ["8", "99"],
            ]
        );

        // Captured: DELETE ... ORDER BY ... LIMIT, and the WHERE + LIMIT form
        // whose cap counts rows DELETED rather than rows examined.
        assert_eq!(
            session
                .run("DELETE FROM t ORDER BY a DESC LIMIT 1")
                .unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY a")),
            [["1"], ["2"], ["3"], ["7"]]
        );
        assert_eq!(
            session.run("DELETE FROM t WHERE c > 0 LIMIT 2").unwrap(),
            StmtResult::Affected(2)
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY a")),
            [["3"], ["7"]]
        );

        // RETURNING parses but is silently ignored, exactly as in Go: the
        // planner and executor never read the AST's Returning list, so the
        // write lands and answers with a plain OK (affected rows), no result
        // set and no warning. Captured with a Go testkit probe.
        assert_eq!(
            session
                .run("INSERT INTO t (a) VALUES (42) RETURNING a")
                .unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(
            session
                .run("UPDATE t SET c = 0 WHERE a = 42 RETURNING a, c")
                .unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(
            session
                .run("DELETE FROM t WHERE a = 42 RETURNING a")
                .unwrap(),
            StmtResult::Affected(1)
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY a")),
            [["3"], ["7"]]
        );
    }

    /// `ORDER BY` resolved against the SELECT list, checked against captured
    /// TiDB output.
    ///
    /// A positional `ORDER BY 1` used to rewrite as a constant here, which
    /// silently produced UNSORTED rows -- the worst kind of divergence, and
    /// the reason this unit was picked.
    #[test]
    fn order_by_resolves_against_the_select_list() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
        session
            .run("INSERT INTO t VALUES (1,30),(2,20),(3,10)")
            .unwrap();

        // Captured: an alias names a projected expression.
        assert_eq!(
            row_text(session.run("SELECT a, a*2 AS twice FROM t ORDER BY twice DESC")),
            [["3", "6"], ["2", "4"], ["1", "2"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a AS z FROM t ORDER BY z DESC")),
            [["3"], ["2"], ["1"]]
        );
        // Captured: an expression BUILT on an alias resolves too.
        assert_eq!(
            row_text(session.run("SELECT a*2 AS twice FROM t ORDER BY twice+0 DESC")),
            [["6"], ["4"], ["2"]]
        );
        // Captured: a bare integer is a 1-based output position.
        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY 1 DESC")),
            [["3"], ["2"], ["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a, b FROM t ORDER BY 2")),
            [["3", "10"], ["2", "20"], ["1", "30"]]
        );
        // Captured: an alias SHADOWS a real column of the same name.
        assert_eq!(
            row_text(session.run("SELECT b AS a FROM t ORDER BY a")),
            [["10"], ["20"], ["30"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a+0 AS a FROM t ORDER BY a DESC")),
            [["3"], ["2"], ["1"]]
        );
        // Captured: a source column that is not projected still sorts.
        assert_eq!(
            row_text(session.run("SELECT a FROM t ORDER BY b DESC")),
            [["1"], ["2"], ["3"]]
        );

        // Captured: an unknown name and an out-of-range position are both
        // 1054 naming the order clause.
        for sql in [
            "SELECT a FROM t ORDER BY nosuch",
            "SELECT a FROM t ORDER BY 5",
        ] {
            match session.run(sql) {
                Err(error) => {
                    let reported = error.to_mysql_error();
                    assert_eq!(reported.code, 1054, "{sql}");
                    assert!(
                        reported.message.ends_with("in 'order clause'"),
                        "{sql}: {}",
                        reported.message
                    );
                }
                Ok(other) => panic!("expected 1054 from {sql}, got {other:?}"),
            }
        }
    }

    /// `GROUP BY` resolved against the SELECT list, checked against captured
    /// TiDB output.
    ///
    /// A positional `GROUP BY 1` used to rewrite as a constant here too --
    /// the same silent-wrong-rows bug `ORDER BY 1` once had, but for
    /// grouping: every row collapsed into one group instead of grouping by
    /// the first select field.
    #[test]
    fn group_by_resolves_against_the_select_list() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
        session
            .run("INSERT INTO t VALUES (1,30),(1,31),(2,20),(3,10)")
            .unwrap();

        // Captured: a bare integer is a 1-based output position, grouping by
        // the first select field (`a`) -- three groups, not one.
        assert_eq!(
            row_text(session.run("SELECT a, COUNT(*) FROM t GROUP BY 1")),
            [["1", "2"], ["2", "1"], ["3", "1"]]
        );

        // Captured: a position landing on an aggregate select field is
        // ErrWrongGroupField (1056), whether or not it carries an alias.
        for sql in [
            "SELECT a, COUNT(*) FROM t GROUP BY 2",
            "SELECT a, COUNT(*) AS c FROM t GROUP BY 2",
        ] {
            match session.run(sql) {
                Err(error) => {
                    let reported = error.to_mysql_error();
                    assert_eq!(reported.code, 1056, "{sql}");
                    assert!(
                        reported.message.starts_with("Can't group on"),
                        "{sql}: {}",
                        reported.message
                    );
                }
                Ok(other) => panic!("expected 1056 from {sql}, got {other:?}"),
            }
        }

        // Captured: an out-of-range position (including zero) is 1054 naming
        // the group statement.
        for sql in [
            "SELECT a, COUNT(*) FROM t GROUP BY 0",
            "SELECT a, COUNT(*) FROM t GROUP BY 3",
        ] {
            match session.run(sql) {
                Err(error) => {
                    let reported = error.to_mysql_error();
                    assert_eq!(reported.code, 1054, "{sql}");
                    assert!(
                        reported.message.ends_with("in 'group statement'"),
                        "{sql}: {}",
                        reported.message
                    );
                }
                Ok(other) => panic!("expected 1054 from {sql}, got {other:?}"),
            }
        }

        // An expression BUILT on a position (`1+1`) is arithmetic, not a
        // position: it groups every row into one bucket by the constant 2.
        assert_eq!(
            row_text(session.run("SELECT COUNT(*) FROM t GROUP BY 1+1")),
            [["4"]]
        );
    }

    /// The aggregates over each numeric domain, checked against captured
    /// TiDB output.
    ///
    /// The type is the load-bearing part: `SUM` over a BIGINT column is a
    /// DECIMAL in MySQL (captured type 246), not a BIGINT, so it sums in the
    /// decimal domain the way Go's `sum4Decimal` does. Only a real argument
    /// makes it a DOUBLE.
    #[test]
    fn aggregates_over_numeric_domains() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT, d DECIMAL(10,2), r DOUBLE)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,1.5,1.5),(2,2.25,2.5),(3,3.25,3.5)")
            .unwrap();

        // Captured: SUM over each domain, with the decimal column keeping
        // its own scale.
        assert_eq!(
            row_text(session.run("SELECT SUM(a), SUM(d), SUM(r) FROM t")),
            [["6", "7.00", "7.5"]]
        );
        // Captured: an empty SUM is NULL, not zero.
        assert_eq!(
            row_text(session.run("SELECT SUM(a) FROM t WHERE a > 100")),
            [["NULL"]]
        );
        // Captured: AVG and MIN/MAX over a decimal column.
        assert_eq!(
            row_text(session.run("SELECT MIN(d), MAX(d) FROM t")),
            [["1.50", "3.25"]]
        );
        assert_eq!(
            row_text(session.run("SELECT COUNT(DISTINCT a), COUNT(*) FROM t")),
            [["3", "3"]]
        );
        // Captured: grouped SUM over a decimal column.
        assert_eq!(
            row_text(session.run("SELECT a, SUM(d) FROM t GROUP BY a ORDER BY a")),
            [["1", "1.50"], ["2", "2.25"], ["3", "3.25"]]
        );
    }

    /// The math, conditional and TRIM builtins through the chunk executor,
    /// checked against captured TiDB output -- including the result TYPES,
    /// which are what size a chunk cell.
    ///
    /// The types are the subtle part and were read off TiDB's own result
    /// fields: `ABS` and `MOD` keep the argument's domain, `CEIL`/`FLOOR`
    /// return an integer for an integer OR decimal argument but stay real
    /// for a real one, `ROUND`/`TRUNCATE` keep the decimal domain, and the
    /// transcendental functions are always real.
    #[test]
    fn math_and_conditional_builtins() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'x',10),(2,'y',20)")
            .unwrap();

        // Captured: ABS keeps the argument's domain.
        assert_eq!(
            row_text(session.run("SELECT ABS(-3), ABS(-3.5)")),
            [["3", "3.5"]]
        );
        // Captured: CEIL/FLOOR of a decimal are integers, and of an integer
        // are the integer itself.
        assert_eq!(
            row_text(session.run("SELECT CEIL(1.2), FLOOR(1.8), CEIL(3), FLOOR(3)")),
            [["2", "1", "3", "3"]]
        );
        // Captured: ROUND keeps the decimal domain and rounds half away from
        // zero; TRUNCATE cuts instead.
        assert_eq!(
            row_text(
                session.run("SELECT ROUND(1.55,1), ROUND(1.55), ROUND(2.5), TRUNCATE(1.999,2)")
            ),
            [["1.6", "2", "3", "1.99"]]
        );
        // Captured: MOD follows its arguments.
        assert_eq!(
            row_text(session.run("SELECT MOD(7,3), MOD(7.5,3)")),
            [["1", "1.5"]]
        );
        // Captured: the always-real family.
        assert_eq!(
            row_text(session.run("SELECT POW(2,3), SQRT(9), LOG10(100)")),
            [["8", "3", "2"]]
        );
        // Captured: SIGN, CONV and CRC32.
        assert_eq!(
            row_text(session.run("SELECT SIGN(-2), CONV(255,10,16), CRC32('a')")),
            [["-1", "FF", "3904355907"]]
        );

        // Captured: GREATEST/LEAST take the merged argument type, and work
        // over strings as well as numbers.
        assert_eq!(
            row_text(session.run("SELECT GREATEST(1,2,3), LEAST(1,2,3), GREATEST('a','b')")),
            [["3", "1", "b"]]
        );
        // Captured: IF picks one branch, and NULLIF is NULL only on equality.
        assert_eq!(
            row_text(session.run("SELECT IF(1,'big','small'), NULLIF(1,1), NULLIF(1,2)")),
            [["big", "NULL", "1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a, IF(c>15,'big','small') FROM t")),
            [["1", "small"], ["2", "big"]]
        );

        // Captured: TRIM's three directions, and its implicit space.
        assert_eq!(
            row_text(session.run("SELECT TRIM(' x '), TRIM(LEADING 'x' FROM 'xxa')")),
            [["x", "a"]]
        );
        assert_eq!(
            row_text(session.run("SELECT TRIM(TRAILING 'a' FROM 'xaa'), SUBSTRING('abc',1,2)")),
            [["x", "ab"]]
        );

        // IF is lazy, so the branch not taken never runs -- a division by
        // zero there would otherwise warn.
        session.run("SELECT IF(1, 1, 1/0)").unwrap();
        assert!(session.warnings().is_empty());
    }

    /// `RAND(N)`/`RAND()` through the chunk executor and `ORDER BY RAND()`.
    ///
    /// Captured from Go (`pkg/executor`, a fresh mock session, table `t(a)`
    /// holding `(1),(2),(3),(4),(5)`): a constant `RAND(5)` evaluated once
    /// per row of a 5-row scan produces the EXACT sequence asserted below --
    /// one generator per AST occurrence, seeded once and advanced per row,
    /// not reseeded. `ORDER BY RAND()` only needs to permute the rows: Go's
    /// own captured order (`[4] [2] [5] [1] [3]`) is one specific shuffle
    /// among the seed's many possible ones, so only the SET is checked here,
    /// not the exact order.
    #[test]
    fn rand_constant_sequence_and_order_by_rand() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
            .unwrap();

        // A constant RAND(5) evaluated on the SAME row three times in one
        // statement returns the SAME value: MySQL's docs describe RAND(N)
        // as "producing a repeatable sequence", but a single implicit row
        // draws only the sequence's first value from each of these three
        // INDEPENDENT call sites -- they agree because they share both seed
        // and position, not because they are the same generator.
        assert_eq!(
            row_text(session.run("SELECT RAND(5), RAND(5), RAND(5)")),
            [[
                "0.40613597483014313",
                "0.40613597483014313",
                "0.40613597483014313"
            ]]
        );

        // The SAME call site advances across rows, producing Go's exact
        // captured sequence.
        assert_eq!(
            row_text(session.run("SELECT RAND(5) FROM t")),
            [
                ["0.40613597483014313"],
                ["0.8745439358749836"],
                ["0.15431178561813363"],
                ["0.1479271511993624"],
                ["0.276700429876056"],
            ]
        );

        // ORDER BY RAND() must not error and must produce a permutation of
        // every row -- the unseeded sequence itself is not pinned.
        let mut rows: Vec<String> = row_text(session.run("SELECT a FROM t ORDER BY RAND()"))
            .into_iter()
            .flatten()
            .collect();
        rows.sort();
        assert_eq!(rows, ["1", "2", "3", "4", "5"]);
    }

    /// The date/time family through the chunk executor, checked against
    /// captured TiDB output with `time_zone = '+00:00'`.
    ///
    /// Go fixes the statement clock once, so every `NOW()` in one statement
    /// agrees; the context carries that instant and the resolved session
    /// zone (Go `timeutil.ParseTimeZone`).
    ///
    /// DOCUMENTED DIVERGENCE, the same one the temporal casts carry: this
    /// crate's date/time builtins produce formatted STRINGS, so the reported
    /// column type is `VarString` where TiDB says `DATETIME`. The values
    /// match.
    /// `DATE_ADD`/`DATE_SUB`/`ADDDATE`/`SUBDATE`, `EXTRACT` and
    /// `TIMESTAMPDIFF` through the CHUNK path, checked against captured TiDB
    /// output with `time_zone = '+00:00'` (`pkg/executor`, a table holding
    /// `('2024-01-31 10:20:30', '2024-01-31')` and
    /// `('2025-03-15 23:59:59', '2025-03-15')` plus an all-NULL row).
    ///
    /// The INTERVAL unit is a build-time keyword, not a value, so the
    /// rewriter records it in the function NAME and the chunk evaluator
    /// reuses the same `date_add` implementation the row path calls.
    ///
    /// DOCUMENTED DIVERGENCE, the same one every other date/time builtin
    /// here carries: the result is a formatted STRING (`VarString`) where
    /// TiDB reports `DATE`/`DATETIME`. The values match.
    #[test]
    fn date_interval_extract_and_timestampdiff() {
        let mut session = Session::new();
        session.apply_set("SET time_zone = '+00:00'").unwrap();
        session
            .run("CREATE TABLE t (created VARCHAR(30), d VARCHAR(30))")
            .unwrap();
        session
            .run(
                "INSERT INTO t VALUES ('2024-01-31 10:20:30', '2024-01-31'), \
                 ('2025-03-15 23:59:59', '2025-03-15'), (NULL, NULL)",
            )
            .unwrap();

        // Captured: DAY arithmetic keeps the time-of-day, HOUR recomputes it
        // (and rolls the date over), and NULL propagates.
        assert_eq!(
            row_text(session.run("SELECT DATE_ADD(created, INTERVAL 1 DAY) FROM t")),
            [["2024-02-01 10:20:30"], ["2025-03-16 23:59:59"], ["NULL"]]
        );
        assert_eq!(
            row_text(session.run("SELECT DATE_ADD(created, INTERVAL 2 HOUR) FROM t")),
            [["2024-01-31 12:20:30"], ["2025-03-16 01:59:59"], ["NULL"]]
        );
        // Captured: the month-end CLAMP -- January 31 plus one month is
        // February 29 in a leap year, not March 3.
        assert_eq!(
            row_text(session.run("SELECT DATE_ADD(created, INTERVAL 1 MONTH) FROM t")),
            [["2024-02-29 10:20:30"], ["2025-04-15 23:59:59"], ["NULL"]]
        );
        assert_eq!(
            row_text(session.run("SELECT DATE_SUB(created, INTERVAL 1 DAY) FROM t")),
            [["2024-01-30 10:20:30"], ["2025-03-14 23:59:59"], ["NULL"]]
        );
        assert_eq!(
            row_text(session.run("SELECT DATE_SUB(created, INTERVAL 1 MONTH) FROM t")),
            [["2023-12-31 10:20:30"], ["2025-02-15 23:59:59"], ["NULL"]]
        );
        // Captured: a date-only column keeps no time component at all.
        assert_eq!(
            row_text(session.run("SELECT DATE_SUB(d, INTERVAL 1 MONTH) FROM t")),
            [["2023-12-31"], ["2025-02-15"], ["NULL"]]
        );

        // Captured: ADDDATE/SUBDATE's bare-number form is exactly the DAY
        // interval, and their explicit INTERVAL form agrees with it.
        assert_eq!(
            row_text(session.run("SELECT ADDDATE(d, 5), SUBDATE(d, 5) FROM t")),
            [
                ["2024-02-05", "2024-01-26"],
                ["2025-03-20", "2025-03-10"],
                ["NULL", "NULL"]
            ]
        );
        assert_eq!(
            row_text(session.run("SELECT ADDDATE(d, INTERVAL 5 DAY) FROM t")),
            [["2024-02-05"], ["2025-03-20"], ["NULL"]]
        );

        // Captured: EXTRACT of a simple unit is the same function that unit
        // already names.
        assert_eq!(
            row_text(session.run(
                "SELECT EXTRACT(YEAR FROM created), EXTRACT(MONTH FROM created), \
                 EXTRACT(DAY FROM d), EXTRACT(HOUR FROM created) FROM t"
            )),
            [
                ["2024", "1", "31", "10"],
                ["2025", "3", "15", "23"],
                ["NULL", "NULL", "NULL", "NULL"]
            ]
        );

        // Captured: TIMESTAMPDIFF counts WHOLE units -- January 31 to March 1
        // is 30 days but only 1 whole month, and a month whose day-of-month
        // is reached but whose clock time is not counts as 0.
        assert_eq!(
            row_text(session.run(
                "SELECT TIMESTAMPDIFF(DAY, '2024-01-31', '2024-03-01'), \
                 TIMESTAMPDIFF(MONTH, '2024-01-31', '2024-03-01')"
            )),
            [["30", "1"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT TIMESTAMPDIFF(MONTH, '2024-01-31 10:00:00', '2024-02-29 09:00:00'), \
                 TIMESTAMPDIFF(HOUR, '2024-01-31 10:00:00', '2024-02-01 09:00:00')"
            )),
            [["0", "23"]]
        );
        assert_eq!(
            row_text(session.run("SELECT TIMESTAMPDIFF(YEAR, d, created) FROM t")),
            [["0"], ["0"], ["NULL"]]
        );
        assert_eq!(
            row_text(session.run("SELECT TIMESTAMPDIFF(DAY, NULL, '2024-01-01')")),
            [["NULL"]]
        );

        // Captured: a filter is the same expression in predicate position.
        assert_eq!(
            row_text(
                session.run(
                    "SELECT d FROM t WHERE created >= DATE_SUB('2025-01-01', INTERVAL 1 MONTH)"
                )
            ),
            [["2025-03-15"]]
        );

        // Captured: an unparseable calendar date and a NULL amount are both
        // NULL, not an error.
        assert_eq!(
            row_text(session.run("SELECT DATE_ADD('2024-02-30', INTERVAL 1 DAY)")),
            [["NULL"]]
        );
        assert_eq!(
            row_text(session.run("SELECT DATE_ADD(created, INTERVAL NULL DAY) FROM t LIMIT 1")),
            [["NULL"]]
        );

        // Composite units -- ported from `parseTimeValue`/
        // `ExtractDatetimeNum` (`pkg/types/time.go`); captured against
        // `pkg/executor`: `'2024-01-31 10:20:30' + INTERVAL '1:30'
        // HOUR_MINUTE` is `2024-01-31 11:50:30`, and `EXTRACT(HOUR_MINUTE
        // FROM '2024-01-31 10:20:30')` is `1020`. Both the row path
        // (`time_fn::calendar::date_add`/`extract_composite`) and the chunk
        // rewriter build these now.
        assert_eq!(
            row_text(session.run("SELECT DATE_ADD(created, INTERVAL '1:30' HOUR_MINUTE) FROM t")),
            [["2024-01-31 11:50:30"], ["2025-03-16 01:29:59"], ["NULL"]]
        );
        assert_eq!(
            row_text(session.run("SELECT EXTRACT(HOUR_MINUTE FROM created) FROM t")),
            [["1020"], ["2359"], ["NULL"]]
        );
        assert_eq!(
            row_text(session.run("SELECT EXTRACT(DAY_SECOND FROM created) FROM t")),
            [["31102030"], ["15235959"], ["NULL"]]
        );
    }

    #[test]
    fn date_time_builtins() {
        let mut session = Session::new();
        session.apply_set("SET time_zone = '+00:00'").unwrap();
        session.run("CREATE TABLE t (d VARCHAR(30))").unwrap();
        session
            .run("INSERT INTO t VALUES ('2020-03-05 06:07:08')")
            .unwrap();

        // Captured: the field extractors.
        assert_eq!(
            row_text(session.run(
                "SELECT MONTH(d), DAY(d), YEAR(d), DAYOFWEEK(d), DAYOFYEAR(d), WEEKDAY(d), QUARTER(d) FROM t"
            )),
            [["3", "5", "2020", "5", "65", "3", "1"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT MONTHNAME(d), DAYNAME(d), LAST_DAY(d), TO_DAYS(d), TIME_TO_SEC(d) FROM t"
            )),
            [["March", "Thursday", "2020-03-31", "737854", "22028"]]
        );
        assert_eq!(
            row_text(session.run("SELECT WEEK(d), WEEKOFYEAR(d), YEARWEEK(d) FROM t")),
            [["9", "10", "202009"]]
        );
        assert_eq!(
            row_text(session.run("SELECT SEC_TO_TIME(3661), MAKEDATE(2020,10), MAKETIME(1,2,3)")),
            [["01:01:01", "2020-01-10", "01:02:03"]]
        );
        assert_eq!(
            row_text(session.run("SELECT PERIOD_ADD(202001, 2), PERIOD_DIFF(202003, 202001)")),
            [["202003", "2"]]
        );

        // Captured: the statement clock is fixed, so NOW() agrees with
        // itself and prints a full second-resolution datetime.
        assert_eq!(
            row_text(session.run("SELECT NOW() = NOW(), LENGTH(NOW()) = 19")),
            [["1", "1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CURDATE() = CURDATE(), LENGTH(CURDATE()) = 10")),
            [["1", "1"]]
        );

        // The session zone reaches the clock: UTC and a +10 offset differ by
        // ten hours in the hour NOW() reports for the same instant.
        let hour_at = |session: &mut Session, zone: &str| -> i64 {
            session
                .apply_set(&format!("SET time_zone = '{zone}'"))
                .unwrap();
            match session.run("SELECT HOUR(NOW())").unwrap() {
                StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap().parse().unwrap(),
                other => panic!("expected rows, got {other:?}"),
            }
        };
        let utc = hour_at(&mut session, "+00:00");
        let plus_ten = hour_at(&mut session, "+10:00");
        assert_eq!((utc + 10) % 24, plus_ten);
    }

    /// `CAST(expr AS type)` and its `CONVERT`/`BINARY` spellings through the
    /// chunk executor, checked against captured TiDB output.
    ///
    /// The target type IS the operation in Go (it picks a
    /// `builtinCast*As*Sig` from it), so the rewriter puts the target in the
    /// function's result type and evaluation reads it back from there.
    ///
    /// STILL REFUSED, for the reason `cast::eval_cast` already records:
    /// `TIME` and `JSON` targets have no value domain in this crate, and the
    /// `ARRAY` modifier is a JSON multi-valued index.
    #[test]
    fn cast_and_convert() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'12abc',10),(2,'zz',20)")
            .unwrap();

        // Captured: a number to CHAR, and the width truncating it.
        assert_eq!(
            row_text(session.run("SELECT CAST(c AS CHAR) FROM t")),
            [["10"], ["20"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CAST(c AS CHAR(1)) FROM t")),
            [["1"], ["2"]]
        );

        // Captured: a string to a number takes the leading digits, or zero.
        assert_eq!(
            row_text(session.run("SELECT CAST(b AS SIGNED) FROM t")),
            [["12"], ["0"]]
        );
        // Captured: the rounding asymmetry -- a string keeps only the integer
        // prefix while a decimal or a float rounds.
        assert_eq!(
            row_text(session.run("SELECT CAST('3.7' AS SIGNED), CAST(3.7 AS SIGNED)")),
            [["3", "4"]]
        );
        // Captured: UNSIGNED wraps a negative rather than clamping it.
        assert_eq!(
            row_text(session.run("SELECT CAST(-1 AS UNSIGNED)")),
            [["18446744073709551615"]]
        );

        // Captured: DECIMAL rounds to the written scale, and pads to it.
        assert_eq!(
            row_text(session.run("SELECT CAST('12.345' AS DECIMAL(6,2))")),
            [["12.35"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CAST(1 AS DECIMAL(6,2))")),
            [["1.00"]]
        );

        // Captured: the temporal targets.
        assert_eq!(
            row_text(session.run("SELECT CAST('2020-01-02' AS DATE)")),
            [["2020-01-02"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CAST('2020-1-2' AS DATE)")),
            [["2020-01-02"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CAST('2020-01-02 03:04:05' AS DATETIME)")),
            [["2020-01-02 03:04:05"]]
        );

        // Captured: BINARY(n) pads with NUL rather than truncating short.
        assert_eq!(
            row_text(session.run("SELECT CAST(b AS BINARY(3)) FROM t")),
            [["12a"], ["zz\u{0}"]]
        );

        // Captured: CONVERT and the BINARY operator are the same node.
        assert_eq!(
            row_text(session.run("SELECT CONVERT(c, CHAR), CONVERT('7', SIGNED) FROM t")),
            [["10", "7"], ["20", "7"]]
        );
        assert_eq!(
            row_text(session.run("SELECT BINARY b FROM t")),
            [["12abc"], ["zz"]]
        );

        // Captured: NULL casts to NULL, and a cast result is an ordinary
        // operand afterwards.
        assert_eq!(
            row_text(session.run("SELECT CAST(NULL AS SIGNED) IS NULL")),
            [["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CAST(c AS DOUBLE)/2 FROM t")),
            [["5"], ["10"]]
        );

        // The JSON target produces this tier's canonical JSON text -- see
        // `json_value_functions` for the whole slice and its divergence note.
        assert_eq!(
            row_text(session.run("SELECT CAST(c AS JSON) FROM t")),
            [["10"], ["20"]]
        );

        // The refusals are refusals, not wrong answers.
        assert!(session.run("SELECT CAST(c AS TIME) FROM t").is_err());
    }

    /// LIKE, BETWEEN, CASE and the ordinary builtins through the chunk
    /// executor, checked against captured TiDB output.
    ///
    /// These forms all existed in `tidb_expr`'s AST evaluator already; what
    /// was missing was the rewriter building them for chunk evaluation, so a
    /// query using any of them failed outright.
    ///
    /// STILL REFUSED, each for its own reason recorded at
    /// `tidb_expr::rewriter::builtin_return_type`: the session-state
    /// functions (`DATABASE`, `VERSION`, `CURRENT_USER`, `NOW`) need a
    /// resolver carrying session state into the chunk path, `CAST`/`CONVERT`
    /// take a target type rather than a value, `GROUP_CONCAT` is an
    /// aggregate, and the `DATE_ADD` family takes an `Expr::Interval`.
    #[test]
    fn like_between_case_and_builtins() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT, KEY kb (b))")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'xy',10),(2,'Yz',20),(3,'z',30)")
            .unwrap();

        // Captured: LIKE's wildcards, its negation and its escape.
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE b LIKE 'x%'")),
            [["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE b LIKE '%y%'")),
            [["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE b LIKE 'x_'")),
            [["1"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE b NOT LIKE 'x%'")),
            [["2"], ["3"]]
        );
        assert_eq!(row_text(session.run(r"SELECT 'a%b' LIKE 'a\%b'")), [["1"]]);
        assert_eq!(
            row_text(session.run("SELECT b FROM t WHERE b LIKE '%'")),
            [["xy"], ["Yz"], ["z"]]
        );

        // Captured: BETWEEN is inclusive, and its negation is the complement.
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE c BETWEEN 10 AND 20")),
            [["1"], ["2"]]
        );
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE c NOT BETWEEN 10 AND 20")),
            [["3"]]
        );

        // Captured: the searched CASE, the simple CASE, a NULL condition
        // (which is not a match), and a missing ELSE (which is NULL).
        assert_eq!(
            row_text(session.run("SELECT a, CASE WHEN c > 15 THEN 'hi' ELSE 'lo' END FROM t")),
            [["1", "lo"], ["2", "hi"], ["3", "hi"]]
        );
        assert_eq!(
            row_text(
                session.run("SELECT CASE c WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' END FROM t")
            ),
            [["ten"], ["twenty"], ["NULL"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CASE WHEN NULL THEN 'x' ELSE 'y' END")),
            [["y"]]
        );
        assert_eq!(
            row_text(session.run("SELECT CASE WHEN c > 100 THEN 'x' END FROM t")),
            [["NULL"], ["NULL"], ["NULL"]]
        );

        // Captured: the string builtins, including LENGTH counting bytes
        // while CHAR_LENGTH counts characters.
        assert_eq!(
            row_text(
                session.run(
                    "SELECT CONCAT(b,'!'), UPPER(b), LOWER(b), LENGTH(b), CHAR_LENGTH(b) FROM t"
                )
            ),
            [
                ["xy!", "XY", "xy", "2", "2"],
                ["Yz!", "YZ", "yz", "2", "2"],
                ["z!", "Z", "z", "1", "1"],
            ]
        );
        assert_eq!(
            row_text(session.run("SELECT LENGTH('héllo'), CHAR_LENGTH('héllo')")),
            [["6", "5"]]
        );

        // Captured: COALESCE and IFNULL over a column and a literal, whose
        // branch types Go merges to one string type.
        assert_eq!(
            row_text(
                session.run("SELECT COALESCE(NULL, b), IFNULL(b,'n'), IFNULL(NULL,'n') FROM t")
            ),
            [["xy", "xy", "n"], ["Yz", "Yz", "n"], ["z", "z", "n"],]
        );

        // Captured: DATABASE() and its SCHEMA() synonym report the current
        // database, and VERSION() reports the same string as @@version.
        assert_eq!(
            row_text(session.run("SELECT DATABASE(), SCHEMA()")),
            [["test", "test"]]
        );
        let version = match session.run("SELECT VERSION()").unwrap() {
            StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };
        assert_eq!(version, session.vars().get_system("version").unwrap());
        assert!(version.contains("TiDB"), "{version}");
        // Captured: with no database selected, DATABASE() is NULL.
        let mut fresh = Session::new();
        fresh.run("DROP DATABASE test").unwrap();
        assert_eq!(row_text(fresh.run("SELECT DATABASE()")), [["NULL"]]);

        // A session with no authenticated user answers NULL for the identity
        // builtins, which is what Go does for a session without one; a front
        // end that authenticates sets it (see the server's client test).
        assert_eq!(
            row_text(session.run("SELECT CURRENT_USER(), USER()")),
            [["NULL", "NULL"]]
        );
        session.set_user("bob@%".to_owned(), "bob@10.0.0.1".to_owned());
        assert_eq!(
            row_text(session.run("SELECT CURRENT_USER(), USER(), SESSION_USER()")),
            [["bob@%", "bob@10.0.0.1", "bob@10.0.0.1"]]
        );

        // CONNECTION_ID() is NULL until a front end attaches one (Go itself
        // errors here rather than reporting NULL, but that path is
        // unreachable in practice -- see `Columns::connection_id`'s doc); once
        // set, the same value keeps reporting on later statements.
        assert_eq!(row_text(session.run("SELECT CONNECTION_ID()")), [["NULL"]]);
        session.set_connection_id(42);
        assert_eq!(row_text(session.run("SELECT CONNECTION_ID()")), [["42"]]);
        assert_eq!(row_text(session.run("SELECT CONNECTION_ID()")), [["42"]]);

        // The refusals above are refusals, not wrong answers. (CAST,
        // GROUP_CONCAT, CURRENT_USER, GROUP_CONCAT's inner ORDER BY, and
        // multi-argument GROUP_CONCAT were each this example in turn; all of
        // them work now.) `COUNT(b, a)` without DISTINCT stays refused, but as
        // a parser-level SQL syntax error, not a driver limitation: captured
        // from TiDB, `COUNT(a, b)` is only valid SQL as `COUNT(DISTINCT a,
        // b)` (see `multi_argument_count` below) -- the grammar itself
        // rejects the non-DISTINCT, multi-argument form.
        assert!(session.run("SELECT COUNT(b, a) FROM t").is_err());
    }

    /// `COUNT(a, b, ...)` / `COUNT(DISTINCT a, b, ...)`, checked against
    /// captured TiDB output. Only the `DISTINCT` form is valid SQL for more
    /// than one argument (`pkg/parser` rejects a bare `COUNT(a, b)` at parse
    /// time, matched by `tidb_parser`'s own `parse_aggregate`), so this test
    /// only has `COUNT(DISTINCT ...)` to exercise: a row counts only when
    /// EVERY argument is non-NULL, and DISTINCT dedupes over the whole
    /// argument tuple rather than a single column.
    #[test]
    fn multi_argument_count() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (g INT, a INT, b INT)").unwrap();
        session
            .run(
                "INSERT INTO t VALUES \
                 (1, 1, 1), (1, 1, 1), (1, 1, NULL), (1, NULL, 1), (1, NULL, NULL), \
                 (2, 2, 2), (2, 2, 2), (2, 3, 3)",
            )
            .unwrap();

        // Captured: `count(distinct a, b)` over the whole table sees three
        // distinct non-NULL pairs -- (1,1), (2,2), (3,3) -- with every row
        // that has a NULL in either column excluded entirely.
        assert_eq!(
            row_text(session.run("SELECT COUNT(DISTINCT a, b) FROM t")),
            [["3"]]
        );
        // Captured: grouped, group 1 has one distinct non-NULL pair (1,1)
        // (its NULL-containing rows don't count), group 2 has two: (2,2) and
        // (3,3).
        assert_eq!(
            row_text(session.run("SELECT g, COUNT(DISTINCT a, b) FROM t GROUP BY g ORDER BY g")),
            [["1", "1"], ["2", "2"]]
        );
    }

    /// `[NOT] REGEXP` through the chunk (table-scan `WHERE`) path, checked
    /// against captured TiDB output. Before this test, the chunk rewriter had
    /// no `Expr::Regexp` arm, so `SELECT ... WHERE b REGEXP '...'` failed
    /// even though the same expression worked as a bare `SELECT`.
    #[test]
    fn regexp_through_the_chunk_path() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20))")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,'abc'),(2,'xyz'),(3,NULL)")
            .unwrap();

        // Captured: `abc` matches `^a`, `xyz` and the NULL row do not.
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE b REGEXP '^a'")),
            [["1"]]
        );
        // Captured: NOT REGEXP is the complement, still excluding the NULL
        // row -- a NULL operand is never TRUE for either polarity.
        assert_eq!(
            row_text(session.run("SELECT a FROM t WHERE b NOT REGEXP '^a'")),
            [["2"]]
        );
        // Captured: a bare SELECT REGEXP still works (the row path this
        // reused already handled it), so both paths agree.
        assert_eq!(row_text(session.run("SELECT 'abc' REGEXP '^a'")), [["1"]]);
        assert_eq!(
            row_text(session.run("SELECT 'abc' NOT REGEXP '^a'")),
            [["0"]]
        );
        // Captured: NULL propagates from either operand.
        assert_eq!(row_text(session.run("SELECT NULL REGEXP '^a'")), [["NULL"]]);
        assert_eq!(
            row_text(session.run("SELECT 'abc' REGEXP NULL")),
            [["NULL"]]
        );
        // Captured: an invalid pattern is a query error, not a NULL/false
        // result -- `[expression:1139]Got error 'error parsing regexp:
        // missing closing ): `(`' from regexp`.
        assert!(session.run("SELECT 'abc' REGEXP '('").is_err());
    }

    /// `MAKE_SET` regression, checked against mock TiDB. `1|4` evaluates to
    /// the UNSIGNED domain, which used to fall through the builtin's
    /// `Datum::Int`-only match and answer NULL instead of `'a,c'`.
    #[test]
    fn make_set_accepts_a_bitwise_or_result() {
        let mut session = Session::new();
        assert_eq!(
            row_text(session.run("SELECT MAKE_SET(1|4,'a','b','c')")),
            [["a,c"]]
        );
        assert_eq!(
            row_text(session.run("SELECT MAKE_SET(0,'a','b','c')")),
            [[""]]
        );
        assert_eq!(
            row_text(session.run("SELECT MAKE_SET(NULL,'a','b','c')")),
            [["NULL"]]
        );
        // A NULL string argument is skipped, not propagated.
        assert_eq!(
            row_text(session.run("SELECT MAKE_SET(1,'a',NULL,'c')")),
            [["a"]]
        );
        // More set bits than strings simply has nothing left to match.
        assert_eq!(
            row_text(session.run("SELECT MAKE_SET(31,'a','b','c')")),
            [["a,b,c"]]
        );
    }

    /// Go `getDefaultValue` + `checkDefaultValue`: a written DEFAULT is
    /// normalized and checked against the column's own type at DDL time,
    /// checked against captured TiDB output.
    ///
    /// NOT PORTED: the function-call defaults (`CURRENT_TIMESTAMP`), the
    /// ENUM/SET forms with their own index rules, and BIT columns -- each is
    /// its own arm of Go's `getDefaultValue` and none of those column types
    /// reaches this tier yet.
    #[test]
    fn column_default_is_normalized_and_checked() {
        let mut session = Session::new();
        session
            .run(
                "CREATE TABLE t (a BIGINT, d DECIMAL(10,3) DEFAULT 1.5, \
                 i INT DEFAULT 7.6, v VARCHAR(4) DEFAULT 'ab')",
            )
            .unwrap();

        // Captured: only the integer and float/double types normalize their
        // stored default, so SHOW CREATE reports 8 for the INT column while
        // the DECIMAL column keeps the literal as written.
        let created = show_create(&mut session, "t");
        assert!(
            created.contains("`d` decimal(10,3) DEFAULT '1.5'"),
            "{created}"
        );
        assert!(created.contains("`i` int(11) DEFAULT '8'"), "{created}");
        assert!(created.contains("`v` varchar(4) DEFAULT 'ab'"), "{created}");

        // Captured: a row that takes the defaults casts them to the column,
        // so the decimal reaches the column's own scale here.
        session.run("INSERT INTO t (a) VALUES (1)").unwrap();
        assert_eq!(
            row_text(session.run("SELECT a, d, i, v FROM t")),
            [["1", "1.500", "8", "ab"]]
        );

        // Captured: a default the column cannot hold is 1067 at DDL time.
        for sql in [
            "CREATE TABLE w (v VARCHAR(4) DEFAULT 'abcdefg')",
            "CREATE TABLE x (i INT DEFAULT 'zz')",
        ] {
            match session.run(sql) {
                Err(error) => {
                    let reported = error.to_mysql_error();
                    assert_eq!(reported.code, 1067, "{sql}");
                    assert!(
                        reported.message.starts_with("Invalid default value for "),
                        "{sql}: {}",
                        reported.message
                    );
                }
                Ok(other) => panic!("expected 1067 from {sql}, got {other:?}"),
            }
        }
        // A numeric string a column CAN hold is accepted and kept.
        session.run("CREATE TABLE y (i INT DEFAULT '12')").unwrap();
        session
            .run("INSERT INTO y (i) VALUES (DEFAULT)")
            .unwrap_or_else(|_| {
                // `VALUES (DEFAULT)` is not parsed at this tier; an omitted
                // column takes the same path.
                session.run("INSERT INTO y () VALUES ()").unwrap()
            });
        assert_eq!(row_text(session.run("SELECT i FROM y")), [["12"]]);

        // Captured: ALTER TABLE ADD COLUMN runs the same normalization and
        // check, and existing rows read the cast default.
        session
            .run("ALTER TABLE t ADD COLUMN e DECIMAL(6,2) DEFAULT 3.14159")
            .unwrap();
        let created = show_create(&mut session, "t");
        assert!(
            created.contains("`e` decimal(6,2) DEFAULT '3.14159'"),
            "{created}"
        );
        assert_eq!(row_text(session.run("SELECT e FROM t")), [["3.14"]]);
        assert!(matches!(
            session.run("ALTER TABLE t ADD COLUMN f VARCHAR(2) DEFAULT 'toolong'"),
            Err(DriverError::InvalidDefault(_))
        ));
    }

    /// The `Create Table` text of one table.
    fn show_create(session: &mut Session, table: &str) -> String {
        match session
            .run_with_columns(&format!("SHOW CREATE TABLE {table}"))
            .unwrap()
        {
            StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// Go `table.CastValue`: a written value takes its column's type, checked
    /// against captured TiDB output.
    ///
    /// NOT PORTED from Go's own suites: the temporal columns (a DATE/DATETIME
    /// column's zero-date handling is its own error path), ENUM/SET, and the
    /// `INSERT IGNORE` form, which Go treats like a non-strict mode.
    #[test]
    fn insert_casts_to_column_type() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (d DECIMAL(10,3), i INT, v VARCHAR(4))")
            .unwrap();

        // Captured: a decimal rounds to the column's scale, a float rounds to
        // the integer column, and a numeric string parses.
        session
            .run("INSERT INTO t VALUES (1.23456, 7.6, 'ab')")
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT d, i, v FROM t")),
            [["1.235", "8", "ab"]]
        );
        assert!(session.warnings().is_empty());
        session.run("INSERT INTO t (i) VALUES ('12')").unwrap();
        assert_eq!(row_text(session.run("SELECT i FROM t")), [["8"], ["12"]]);

        // Captured: under the default strict mode a value that does not fit
        // fails the statement, and the row is not written.
        assert!(matches!(
            session.run("INSERT INTO t (v) VALUES ('abcdefg')"),
            Err(DriverError::DataTooLong { row: 1, .. })
        ));
        assert!(matches!(
            session.run("INSERT INTO t (i) VALUES ('x')"),
            Err(DriverError::IncorrectValue { row: 1, .. })
        ));
        assert_eq!(row_text(session.run("SELECT i FROM t")).len(), 2);
        // The failure is reported with Go's own message.
        match session.run("INSERT INTO t (i) VALUES ('x')") {
            Err(error) => {
                let reported = error.to_mysql_error();
                assert_eq!(reported.code, 1366);
                assert_eq!(
                    reported.message,
                    "Incorrect int value: 'x' for column 'i' at row 1"
                );
            }
            Ok(other) => panic!("expected a failure, got {other:?}"),
        }

        // Captured: UPDATE casts an assigned value the same way.
        session.run("UPDATE t SET d = 9.87654 WHERE i = 8").unwrap();
        assert_eq!(
            row_text(session.run("SELECT d FROM t WHERE i = 8")),
            [["9.877"]]
        );
        assert!(matches!(
            session.run("UPDATE t SET v = 'abcdefg' WHERE i = 8"),
            Err(DriverError::DataTooLong { .. })
        ));

        // Captured: without a strict mode the converted value is stored and
        // the same message is a warning -- the string truncates to the
        // column's width and an unparseable number becomes zero.
        session.apply_set("SET sql_mode = ''").unwrap();
        session.run("INSERT INTO t (v) VALUES ('abcdefg')").unwrap();
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1406);
        assert_eq!(
            session.warnings()[0].message,
            "Data too long for column 'v' at row 1"
        );
        session.run("INSERT INTO t (i) VALUES ('x')").unwrap();
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1366);
        assert_eq!(
            row_text(session.run("SELECT v FROM t")),
            [["ab"], ["NULL"], ["abcd"], ["NULL"]]
        );
    }

    /// Decimal, hex and bit literals through the whole session path, checked
    /// against captured TiDB output.
    ///
    /// NOT PORTED: `-2.750` is one literal token in Go's parser, so its type
    /// carries the sign in its flen; this AST keeps the sign as a unary minus
    /// over the literal, so the sign shapes the value but not the literal's
    /// own flen. The printed value is the same.
    #[test]
    fn numeric_literals() {
        let mut session = Session::new();

        // Captured: a decimal literal keeps its written scale.
        assert_eq!(row_text(session.run("SELECT 1.5")), [["1.5"]]);
        assert_eq!(row_text(session.run("SELECT 0.10")), [["0.10"]]);
        assert_eq!(row_text(session.run("SELECT -2.750")), [["-2.750"]]);

        // Captured: decimal arithmetic keeps the wider scale, and division by
        // zero is still NULL plus a warning.
        assert_eq!(row_text(session.run("SELECT 1.5 + 1")), [["2.5"]]);
        assert_eq!(row_text(session.run("SELECT 1.5 * 2")), [["3.0"]]);
        assert_eq!(
            session.run("SELECT 1.5 / 0").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Null]])
        );
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1365);

        // Captured: a decimal comparison against an integer.
        assert_eq!(row_text(session.run("SELECT 1.5 > 1")), [["1"]]);

        // Captured: DIV and MOD truncate toward zero.
        assert_eq!(
            row_text(session.run("SELECT 7 DIV 2, 7 MOD 2, -7 DIV 2")),
            [["3", "1", "-3"]]
        );

        // Captured: a hex or bit literal prints as its bytes.
        assert_eq!(row_text(session.run("SELECT 0x41")), [["A"]]);
        assert_eq!(row_text(session.run("SELECT x'4142'")), [["AB"]]);
        assert_eq!(row_text(session.run("SELECT b'1010'")), [["\n"]]);

        // Captured: and reads as a number in arithmetic.
        assert_eq!(row_text(session.run("SELECT 0x41 + 0")), [["65"]]);
        assert_eq!(row_text(session.run("SELECT b'1010' + 0")), [["10"]]);

        // A decimal literal reaches a stored decimal column and compares
        // against it.
        session.run("CREATE TABLE t (d DECIMAL(10,3))").unwrap();
        session.run("INSERT INTO t VALUES (1.5), (2.25)").unwrap();
        assert_eq!(
            row_text(session.run("SELECT d FROM t WHERE d > 1.4")),
            [["1.500"], ["2.250"]]
        );
    }

    /// Division by zero, checked against captured TiDB output.
    ///
    /// The value is `NULL` in every case; what the SQL mode decides is whether
    /// the statement also warns, fails, or stays silent.
    ///
    /// NOT PORTED from Go's own suites: the coprocessor's own warning
    /// merging. TiDB pushes a `WHERE a/0 IS NULL` filter down and reports ONE
    /// warning for all the rows a region produced, while three zero divisors
    /// in a projection give three warnings; this tier has no coprocessor
    /// boundary, so it reports one warning per evaluation everywhere.
    #[test]
    fn division_by_zero() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();

        // Captured: a query returns NULL and warns 1365.
        assert_eq!(
            session.run("SELECT 1 / 0").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Null]])
        );
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1365);
        assert_eq!(session.warnings()[0].message, "Division by 0");
        assert_eq!(
            row_text(session.run("SHOW WARNINGS")),
            [[
                "Warning".to_owned(),
                "1365".to_owned(),
                "Division by 0".to_owned()
            ]]
        );

        // Captured: every zero divisor raises its own warning.
        assert_eq!(
            session.run("SELECT 1 / 0, 2 / 0").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Null, Datum::Null]])
        );
        assert_eq!(session.warnings().len(), 2);
        // DEFERRED (pre-existing rewriter gaps, not this channel's): `DIV`,
        // `MOD` and a decimal literal operand reach the same zero-divisor
        // check in `ops.rs`, but the rewriter does not build those expression
        // forms yet, so they cannot be asserted through the session here.

        // Captured: a zero dividend is ordinary arithmetic, not this case.
        session.run("SELECT 0 / 1").unwrap();
        assert!(session.warnings().is_empty());

        // Captured: under the default SQL mode an INSERT fails with 1365 and
        // writes nothing.
        assert!(matches!(
            session.run("INSERT INTO t VALUES (1 / 0, 1)"),
            Err(DriverError::Exec(tidb_executor::ExecError::Eval(
                tidb_executor::EvalError::DivisionByZero
            )))
        ));
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![])
        );

        // The same holds for UPDATE and DELETE, which Go gives the same level.
        session.run("INSERT INTO t VALUES (1, 1)").unwrap();
        assert!(session.run("UPDATE t SET a = a / 0").is_err());
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        assert!(session.run("DELETE FROM t WHERE a = 1 / 0").is_err());
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: without ERROR_FOR_DIVISION_BY_ZERO the condition is
        // ignored entirely -- NULL is written, with no warning at all.
        session.apply_set("SET sql_mode = ''").unwrap();
        session.run("INSERT INTO t VALUES (1 / 0, 2)").unwrap();
        assert!(session.warnings().is_empty());
        assert_eq!(
            session.run("SELECT a FROM t").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Null]])
        );
        // Captured: a strict mode without that flag ignores it too.
        session
            .apply_set("SET sql_mode = 'STRICT_TRANS_TABLES'")
            .unwrap();
        session.run("INSERT INTO t VALUES (1 / 0, 3)").unwrap();
        assert!(session.warnings().is_empty());

        // Non-strict with the flag warns instead of failing.
        session
            .apply_set("SET sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO'")
            .unwrap();
        session.run("INSERT INTO t VALUES (1 / 0, 4)").unwrap();
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1365);

        // A query keeps warning whatever the SQL mode says.
        session.apply_set("SET sql_mode = ''").unwrap();
        session.run("SELECT 1 / 0").unwrap();
        assert_eq!(session.warnings().len(), 1);
    }

    /// `GROUP BY ... WITH ROLLUP`, checked against captured TiDB output.
    ///
    /// Go's hash aggregation over Expand emits rollup rows in a
    /// NONDETERMINISTIC order (verified: the captured order changed across
    /// runs of the same query), so without `ORDER BY` only the row MULTISET
    /// is contractual. This tier's deterministic order is: full groups in
    /// first-seen order, then each shorter prefix's subtotals, then the
    /// grand total. The `ORDER BY` cases below match captured TiDB output
    /// row for row.
    #[test]
    fn with_rollup() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,1,10),(1,2,20),(2,1,30),(2,2,40),(1,1,5)")
            .unwrap();

        // Two-column rollup: every prefix (a,b), (a), () gets aggregate rows,
        // with the rolled-up columns NULL. Multiset captured from TiDB.
        assert_eq!(
            row_text(session.run("SELECT a, b, SUM(c) FROM t GROUP BY a, b WITH ROLLUP")),
            [
                ["1", "1", "15"],
                ["1", "2", "20"],
                ["2", "1", "30"],
                ["2", "2", "40"],
                ["1", "NULL", "35"],
                ["2", "NULL", "70"],
                ["NULL", "NULL", "105"],
            ]
        );
        // Single-column rollup.
        assert_eq!(
            row_text(session.run("SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP")),
            [["1", "35"], ["2", "70"], ["NULL", "105"]]
        );
        // COUNT(*) counts the replicated rows per grouping set.
        assert_eq!(
            row_text(session.run("SELECT a, b, COUNT(*) FROM t GROUP BY a, b WITH ROLLUP")),
            [
                ["1", "1", "2"],
                ["1", "2", "1"],
                ["2", "1", "1"],
                ["2", "2", "1"],
                ["1", "NULL", "3"],
                ["2", "NULL", "2"],
                ["NULL", "NULL", "5"],
            ]
        );
        // AVG: captured scale is 4 (decimal AVG over BIGINT).
        assert_eq!(
            row_text(session.run("SELECT a, b, AVG(c) FROM t GROUP BY a, b WITH ROLLUP")),
            [
                ["1", "1", "7.5000"],
                ["1", "2", "20.0000"],
                ["2", "1", "30.0000"],
                ["2", "2", "40.0000"],
                ["1", "NULL", "11.6667"],
                ["2", "NULL", "35.0000"],
                ["NULL", "NULL", "21.0000"],
            ]
        );
        // Captured row for row: ORDER BY sorts NULL first, so the grand
        // total leads and each subtotal precedes its group's rows.
        assert_eq!(
            row_text(
                session.run("SELECT a, b, SUM(c) FROM t GROUP BY a, b WITH ROLLUP ORDER BY a, b")
            ),
            [
                ["NULL", "NULL", "105"],
                ["1", "NULL", "35"],
                ["1", "1", "15"],
                ["1", "2", "20"],
                ["2", "NULL", "70"],
                ["2", "1", "30"],
                ["2", "2", "40"],
            ]
        );
        assert_eq!(
            row_text(session.run("SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP ORDER BY a")),
            [["NULL", "105"], ["1", "35"], ["2", "70"]]
        );

        // A genuinely-NULL data value is indistinguishable from a rollup
        // NULL in the output, exactly as in TiDB: a=1 has rows (b=1,c=10)
        // and (b=NULL,c=20), so both the data group [1 NULL 20] and the
        // subtotal [1 NULL 30] appear (captured). Only GROUPING() tells them
        // apart -- see `grouping_with_rollup`.
        session
            .run("CREATE TABLE tn (a BIGINT, b BIGINT, c BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO tn VALUES (1,1,10),(1,NULL,20),(NULL,1,30),(2,2,40)")
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT a, b, SUM(c) FROM tn GROUP BY a, b WITH ROLLUP")),
            [
                ["1", "1", "10"],
                ["1", "NULL", "20"],
                ["NULL", "1", "30"],
                ["2", "2", "40"],
                ["1", "NULL", "30"],
                ["NULL", "NULL", "30"],
                ["2", "NULL", "40"],
                ["NULL", "NULL", "100"],
            ]
        );

        // Deferred: a non-column grouping expression cannot be NULLed at the
        // source, so it is refused rather than answered wrongly.
        assert!(matches!(
            session.run("SELECT a+1, SUM(c) FROM t GROUP BY a+1 WITH ROLLUP"),
            Err(DriverError::Unsupported(_))
        ));

        // An empty source yields no rows at all -- not even the grand total
        // -- because Expand replicates zero rows (unlike a scalar aggregate).
        session.run("DELETE FROM t").unwrap();
        assert!(row_text(session.run("SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP")).is_empty());
    }

    /// `GROUPING()` under `WITH ROLLUP`, checked against captured TiDB output.
    ///
    /// `GROUPING(c)` is 1 when `c` is rolled up in the grouping set that
    /// produced the row and 0 otherwise, which is the ONLY way to tell a
    /// subtotal's NULL from a data NULL. With several arguments it returns a
    /// bitmask whose LEFTMOST argument owns the HIGHEST bit (captured:
    /// `GROUPING(a,b) = 1` and `GROUPING(b,a) = 2` on the `b`-rolled-up row).
    ///
    /// Rows whose whole `ORDER BY` key ties -- a data-NULL row and the
    /// subtotal that also reports `b = NULL` -- keep this tier's stable
    /// emission order (data rows first, then subtotals); Go's order for such
    /// ties is nondeterministic, so only the multiset is contractual there.
    #[test]
    fn grouping_with_rollup() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1,1,10),(1,NULL,20),(1,2,30),(2,1,40)")
            .unwrap();

        // Captured row for row. The two `1 NULL` rows are the point: the
        // first is a DATA NULL (grouping(b) = 0, sum 20), the second the
        // rollup subtotal over a=1 (grouping(b) = 1, sum 60).
        assert_eq!(
            row_text(session.run(
                "SELECT a, b, GROUPING(a), GROUPING(b), SUM(c) FROM t \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
            )),
            [
                ["NULL", "NULL", "1", "1", "100"],
                ["1", "NULL", "0", "0", "20"],
                ["1", "NULL", "0", "1", "60"],
                ["1", "1", "0", "0", "10"],
                ["1", "2", "0", "0", "30"],
                ["2", "NULL", "0", "1", "40"],
                ["2", "1", "0", "0", "40"],
            ]
        );

        // Multi-argument bitmask, captured row for row.
        assert_eq!(
            row_text(session.run(
                "SELECT a, b, GROUPING(a,b), GROUPING(b,a), SUM(c) FROM t \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
            )),
            [
                ["NULL", "NULL", "3", "3", "100"],
                ["1", "NULL", "0", "0", "20"],
                ["1", "NULL", "1", "2", "60"],
                ["1", "1", "0", "0", "10"],
                ["1", "2", "0", "0", "30"],
                ["2", "NULL", "1", "2", "40"],
                ["2", "1", "0", "0", "40"],
            ]
        );

        // HAVING reads a GROUPING() the select list does not project: the
        // column is computed, filtered on, and trimmed away. Captured.
        assert_eq!(
            row_text(session.run(
                "SELECT a, b, SUM(c) FROM t GROUP BY a, b WITH ROLLUP \
                 HAVING GROUPING(b) = 0 ORDER BY a, b"
            )),
            [
                ["1", "NULL", "20"],
                ["1", "1", "10"],
                ["1", "2", "30"],
                ["2", "1", "40"],
            ]
        );

        // ORDER BY reads one the same way.
        assert_eq!(
            row_text(session.run(
                "SELECT a, b, GROUPING(a), SUM(c) FROM t GROUP BY a, b WITH ROLLUP \
                 ORDER BY GROUPING(a), a, b"
            )),
            [
                ["1", "NULL", "0", "20"],
                ["1", "NULL", "0", "60"],
                ["1", "1", "0", "10"],
                ["1", "2", "0", "30"],
                ["2", "NULL", "0", "40"],
                ["2", "1", "0", "40"],
                ["NULL", "NULL", "1", "100"],
            ]
        );

        // Captured result type: BIGINT UNSIGNED, flen 20, binary flag.
        match session
            .run_with_columns("SELECT GROUPING(a) FROM t GROUP BY a WITH ROLLUP")
            .unwrap()
        {
            StmtOutput::Rows { columns, .. } => {
                let (name, ftype) = &columns[0];
                // Go names the column with the ORIGINAL text, `grouping(a)`;
                // this tier names every unaliased field by its restored form,
                // a pre-existing tier-wide naming gap rather than one this
                // function introduces.
                assert_eq!(name, "GROUPING(`a`)");
                assert_eq!(ftype.code(), tidb_datatype::FieldTypeCode::LongLong);
                assert!(ftype.is_unsigned());
                assert_eq!(ftype.flen(), 20);
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // Captured: GROUPING() without WITH ROLLUP is
        // "[planner:1111]Invalid use of group function", whether the query
        // groups or not.
        assert!(matches!(
            session.run("SELECT a, GROUPING(a) FROM t GROUP BY a"),
            Err(DriverError::InvalidGroupFuncUse)
        ));
        assert!(matches!(
            session.run("SELECT a, GROUPING(a) FROM t"),
            Err(DriverError::InvalidGroupFuncUse)
        ));

        // Captured: an argument that is not grouped is
        // "[planner:3602]Argument #0 of GROUPING function is not in GROUP BY".
        assert!(matches!(
            session.run("SELECT a, GROUPING(c) FROM t GROUP BY a, b WITH ROLLUP"),
            Err(DriverError::FieldInGroupingNotGroupBy(0))
        ));

        // Deferred: Go evaluates `GROUPING(a) + 1` in the projection above
        // the aggregation, which this tier does not build for select fields.
        assert!(matches!(
            session.run("SELECT GROUPING(a) + 1 FROM t GROUP BY a, b WITH ROLLUP"),
            Err(DriverError::Unsupported(_))
        ));
    }

    /// SHOW WARNINGS / SHOW ERRORS, checked against captured TiDB output.
    ///
    /// NOT PORTED from Go's own suites: the warnings raised by evaluation
    /// (`1/0` is 1365 there) and by write-time truncation, because this tier
    /// does not yet produce those warnings -- only the preprocessor gate and
    /// the failed-statement error reach the buffer here. The filter forms of
    /// both statements are refused, not ignored.
    /// Captured from TiDB (`show processlist` on a fresh testkit session):
    ///
    /// ```text
    /// Id  User  Host  db    Command  Time  State       Info
    /// 1               test  Query    0     autocommit  show processlist
    /// ```
    ///
    /// with column types `Id BIGINT`, `User/Host/db/Command/State VARCHAR`,
    /// `Time INT`, `Info STRING` -- and `show full processlist` differing only
    /// in that `Info` is not truncated to 100 runes.
    ///
    /// A session with no server front lists exactly itself, which is what
    /// this checks; the whole-server list is covered over TCP in
    /// `tidb-server`'s `pipeline_mysql_client_source` test.
    #[test]
    fn show_processlist_lists_this_session() {
        let mut session = Session::new();
        let StmtOutput::Rows { columns, rows } =
            session.run_with_columns("show processlist").unwrap()
        else {
            panic!("SHOW PROCESSLIST answers with rows");
        };
        assert_eq!(
            columns
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            vec!["Id", "User", "Host", "db", "Command", "Time", "State", "Info"]
        );
        let text: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| datum_text(v).unwrap_or_else(|| "NULL".to_owned()))
                    .collect()
            })
            .collect();
        assert_eq!(
            text,
            vec![vec![
                "0".to_owned(),
                String::new(),
                String::new(),
                "test".to_owned(),
                "Query".to_owned(),
                "0".to_owned(),
                "autocommit".to_owned(),
                "show processlist".to_owned(),
            ]]
        );
    }

    /// Captured from TiDB: `SHOW PROCESSLIST` truncates `Info` to 100 runes
    /// and `SHOW FULL PROCESSLIST` does not.
    #[test]
    fn show_full_processlist_does_not_truncate_info() {
        let registry = process::ProcessRegistry::default();
        let mut session = Session::new();
        let guard = registry.register(1, String::new(), String::new(), "test".to_owned(), None);
        session.attach_process(1, guard);
        // A peer connection, which is the row whose Info the SHOW truncates
        // (the running SHOW is this session's own Info).
        let _peer = registry.register(
            9,
            "alice".to_owned(),
            "10.0.0.1:33".to_owned(),
            "test".to_owned(),
            None,
        );
        let long = format!("select /* {} */ 1", "x".repeat(200));
        registry.statement_started(9, &long, "autocommit");
        let short = row_text(session.run("show processlist"));
        assert_eq!(short.len(), 2);
        assert_eq!(short[1][0], "9");
        assert_eq!(short[1][1], "alice");
        assert_eq!(short[1][2], "10.0.0.1:33");
        assert_eq!(short[1][4], "Query");
        assert_eq!(short[1][7].chars().count(), 100);
        // This session's own row reports the SHOW it is running.
        assert_eq!(short[0][7], "show processlist");
        let full = row_text(session.run("show full processlist"));
        assert_eq!(full[1][7], long);
        assert_eq!(full[0][7], "show full processlist");
    }

    /// Go `setDataForProcessList` / `fetchShowProcessList`: without the
    /// `PROCESS` privilege a session sees only its own connections, on both
    /// `SHOW PROCESSLIST` and `information_schema.PROCESSLIST`; with it, all
    /// of them.
    #[test]
    fn process_privilege_gates_visibility_on_both_surfaces() {
        let registry = process::ProcessRegistry::default();
        let mut session = Session::new();
        session.set_user("bob@%".to_owned(), "bob@10.0.0.1".to_owned());
        let guard = registry.register(
            1,
            "bob".to_owned(),
            "10.0.0.1:1".to_owned(),
            "test".to_owned(),
            None,
        );
        session.attach_process(1, guard);
        let _alice = registry.register(
            2,
            "alice".to_owned(),
            "10.0.0.2:2".to_owned(),
            "test".to_owned(),
            None,
        );

        // No PROCESS privilege: only bob's own row.
        let show = row_text(session.run("show processlist"));
        assert_eq!(show.len(), 1);
        assert_eq!(show[0][1], "bob");
        let table = row_text(session.run("select * from information_schema.processlist"));
        assert_eq!(table.len(), 1);
        assert_eq!(table[0][1], "bob");

        // With PROCESS: every connection, on both surfaces.
        session.set_process_privilege(true);
        let show = row_text(session.run("show processlist"));
        assert_eq!(show.len(), 2);
        let table = row_text(session.run("select * from information_schema.processlist"));
        assert_eq!(table.len(), 2);
    }

    /// CAPTURED (`pkg/infoschema/tables.go` `tableProcesslistCols`): the
    /// exact column list and order of `information_schema.PROCESSLIST`,
    /// which is 12 columns wider than `SHOW PROCESSLIST`'s 8.
    #[test]
    fn information_schema_processlist_has_the_captured_column_list() {
        let mut session = Session::new();
        let StmtOutput::Rows { columns, rows } = session
            .run_with_columns("select * from information_schema.processlist")
            .unwrap()
        else {
            panic!("PROCESSLIST answers with rows");
        };
        assert_eq!(
            columns
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "ID",
                "USER",
                "HOST",
                "DB",
                "COMMAND",
                "TIME",
                "STATE",
                "INFO",
                "DIGEST",
                "MEM",
                "MEM_ARBITRATION",
                "MEM_WAIT_ARBITRATE_START",
                "MEM_WAIT_ARBITRATE_BYTES",
                "DISK",
                "TxnStart",
                "RESOURCE_GROUP",
                "SESSION_ALIAS",
                "ROWS_AFFECTED",
                "TIDB_CPU",
                "TIKV_CPU",
            ]
        );
        assert_eq!(rows.len(), 1);
    }

    /// `WHERE` over the virtual table runs through the ordinary plan, exactly
    /// as it does for the other `information_schema` tables.
    #[test]
    fn information_schema_processlist_where_filters_by_user() {
        let registry = process::ProcessRegistry::default();
        let mut session = Session::new();
        session.set_user("root@%".to_owned(), "root@127.0.0.1".to_owned());
        session.set_process_privilege(true);
        let guard = registry.register(
            1,
            "root".to_owned(),
            "127.0.0.1:1".to_owned(),
            "test".to_owned(),
            None,
        );
        session.attach_process(1, guard);
        let _alice = registry.register(
            2,
            "alice".to_owned(),
            "10.0.0.2:2".to_owned(),
            "test".to_owned(),
            None,
        );
        let rows = row_text(
            session.run("select id, user from information_schema.processlist where user = 'alice'"),
        );
        assert_eq!(rows, vec![vec!["2".to_owned(), "alice".to_owned()]]);
    }

    /// Captured from TiDB: `KILL <unknown id>` is NOT an error -- it answers
    /// OK having done nothing (1094 belongs to EXPLAIN FOR CONNECTION).
    #[test]
    fn kill_answers_ok_and_reaches_only_live_connections() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Default)]
        struct Counter {
            queries: AtomicUsize,
            connections: AtomicUsize,
        }
        impl process::ProcessKillTarget for Counter {
            fn cancel_query(&self) {
                self.queries.fetch_add(1, Ordering::AcqRel);
            }
            fn kill_connection(&self) {
                self.connections.fetch_add(1, Ordering::AcqRel);
            }
        }
        let registry = process::ProcessRegistry::default();
        let target = Arc::new(Counter::default());
        let mut session = Session::new();
        let guard = registry.register(
            5,
            "alice".to_owned(),
            String::new(),
            "test".to_owned(),
            Some(target.clone()),
        );
        session.attach_process(5, guard);
        // KILL answers with an affected-row count, which the wire front turns
        // into the OK packet Go sends.
        assert_eq!(
            session.statement_kind("kill 999999").unwrap(),
            StmtKind::Write
        );
        assert_eq!(session.run("kill 999999").unwrap(), StmtResult::Affected(0));
        assert_eq!(target.connections.load(Ordering::Acquire), 0);
        // Killing one's own query is legal and only cancels the statement.
        assert_eq!(
            session.run("kill query 5").unwrap(),
            StmtResult::Affected(0)
        );
        assert_eq!(target.queries.load(Ordering::Acquire), 1);
        assert_eq!(
            session.run("kill connection 5").unwrap(),
            StmtResult::Affected(0)
        );
        assert_eq!(target.connections.load(Ordering::Acquire), 1);
        // Go accepts CONNECTION_ID() and rejects any other expression.
        assert_eq!(
            session.run("kill query connection_id()").unwrap(),
            StmtResult::Affected(0)
        );
        assert_eq!(target.queries.load(Ordering::Acquire), 2);
        assert!(session.run("kill query 1 + 1").is_err());
    }

    #[test]
    fn show_warnings() {
        let mut session = Session::new();
        session
            .apply_set("SET tidb_enable_noop_functions = 'WARN'")
            .unwrap();
        session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
        session.run("INSERT INTO t VALUES (1, 1)").unwrap();

        // Captured: the warning the statement raised, as Level/Code/Message.
        session.run("SELECT a FROM t LOCK IN SHARE MODE").unwrap();
        let expected = vec![vec![
            "Warning".to_owned(),
            "1235".to_owned(),
            "function LOCK IN SHARE MODE has only noop implementation in tidb now, use \
             tidb_enable_noop_functions to enable these functions"
                .to_owned(),
        ]];
        assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);
        // Captured: SHOW WARNINGS does not consume what it reports.
        assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);
        match session.run_with_columns("SHOW WARNINGS").unwrap() {
            StmtOutput::Rows { columns, .. } => assert_eq!(
                columns
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
                ["Level", "Code", "Message"]
            ),
            other => panic!("expected rows, got {other:?}"),
        }
        // Captured: a warning is not an error, so SHOW ERRORS is empty.
        assert!(row_text(session.run("SHOW ERRORS")).is_empty());

        // Captured: the buffer belongs to the last statement, so an ordinary
        // statement empties it.
        session.run("SELECT a FROM t").unwrap();
        assert!(row_text(session.run("SHOW WARNINGS")).is_empty());

        // Captured: a failed statement leaves its own error in the buffer,
        // which both SHOW WARNINGS and SHOW ERRORS report.
        session
            .apply_set("SET tidb_enable_noop_functions = 'OFF'")
            .unwrap();
        assert!(session.run("SELECT a FROM t LOCK IN SHARE MODE").is_err());
        let reported = row_text(session.run("SHOW WARNINGS"));
        assert_eq!(reported.len(), 1);
        assert_eq!(reported[0][0], "Error");
        assert_eq!(reported[0][1], "1235");
        assert_eq!(row_text(session.run("SHOW ERRORS")), reported);

        // Captured: the count form reports a single count column.
        match session.run_with_columns("SHOW COUNT(*) WARNINGS").unwrap() {
            StmtOutput::Rows { columns, rows } => {
                assert_eq!(columns[0].0, "@@session.warning_count");
                assert_eq!(rows, vec![vec![Datum::Int(1)]]);
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // A filter would silently report the wrong rows, so it is refused.
        assert!(matches!(
            session.run("SHOW WARNINGS WHERE 1"),
            Err(DriverError::Unsupported(_)) | Err(DriverError::Parse(_))
        ));
    }

    /// The clauses TiDB parses but only implements as no-ops, checked
    /// against captured TiDB output with `tidb_enable_noop_functions` at its
    /// `OFF` default.
    ///
    /// NOT PORTED from Go's own suites: `tidb_enable_shared_lock_promotion`
    /// (no locking layer here to promote to) and the `READ ONLY` /
    /// `OFFLINE MODE` / `sql_auto_is_null` gates, which belong to variable
    /// and transaction surfaces this tier does not have.
    #[test]
    fn noop_function_gate() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1, 10), (2, 20)")
            .unwrap();

        // Captured: FOR UPDATE runs and returns the rows.
        assert_eq!(
            session
                .run("SELECT b FROM t WHERE a = 1 FOR UPDATE")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(10)]])
        );
        // Its waiting options only shape a lock this tier does not take.
        session.run("SELECT b FROM t FOR UPDATE NOWAIT").unwrap();
        session.run("SELECT b FROM t FOR UPDATE OF t").unwrap();

        // Captured: the shared lock and SQL_CALC_FOUND_ROWS are 1235.
        for sql in [
            "SELECT b FROM t FOR SHARE",
            "SELECT b FROM t LOCK IN SHARE MODE",
            "SELECT SQL_CALC_FOUND_ROWS b FROM t LIMIT 1",
            "SELECT b FROM t GROUP BY b DESC",
        ] {
            assert!(
                matches!(session.run(sql), Err(DriverError::FunctionsNoopImpl(_))),
                "expected a noop-function error from {sql}"
            );
        }
        // An explicit ASC is written too, so it is gated the same way.
        assert!(matches!(
            session.run("SELECT b FROM t GROUP BY b ASC"),
            Err(DriverError::FunctionsNoopImpl("GROUP BY expr ASC|DESC"))
        ));
        // A GROUP BY with no direction is not.
        session.run("SELECT b FROM t GROUP BY b").unwrap();

        // The gate reaches a subquery, a derived table and a set operation.
        assert!(matches!(
            session.run("SELECT b FROM t WHERE a IN (SELECT a FROM t LOCK IN SHARE MODE)"),
            Err(DriverError::FunctionsNoopImpl(_))
        ));
        assert!(matches!(
            session.run("SELECT x.b FROM (SELECT b FROM t LOCK IN SHARE MODE) x"),
            Err(DriverError::FunctionsNoopImpl(_))
        ));
        assert!(matches!(
            session.run("SELECT b FROM t UNION SELECT a FROM t LOCK IN SHARE MODE"),
            Err(DriverError::FunctionsNoopImpl(_))
        ));

        // ON: the clause is accepted and does nothing, with no warning.
        session
            .apply_set("SET tidb_enable_noop_functions = 'ON'")
            .unwrap();
        session.run("SELECT b FROM t LOCK IN SHARE MODE").unwrap();
        assert!(session.warnings().is_empty());

        // WARN: accepted, with the same message as a warning.
        session
            .apply_set("SET tidb_enable_noop_functions = 'WARN'")
            .unwrap();
        session.run("SELECT b FROM t LOCK IN SHARE MODE").unwrap();
        assert_eq!(session.warnings().len(), 1);
        assert_eq!(session.warnings()[0].code, 1235);
        assert!(session.warnings()[0].message.contains("LOCK IN SHARE MODE"));
        // The warnings belong to the last statement only.
        session.run("SELECT b FROM t").unwrap();
        assert!(session.warnings().is_empty());

        // INTO OUTFILE writes a server-side file, which this tier cannot do,
        // so it is refused rather than answered with rows.
        session
            .apply_set("SET tidb_enable_noop_functions = 'OFF'")
            .unwrap();
        assert!(matches!(
            session.run("SELECT b FROM t INTO OUTFILE '/tmp/x'"),
            Err(DriverError::Unsupported(_))
        ));
    }

    /// ALTER TABLE MODIFY / CHANGE COLUMN, checked against captured TiDB
    /// output (`alter table t modify column ...` on a mock store).
    ///
    /// NOT PORTED from Go's own DDL suites: the concurrent/rollback schema
    /// states (this tier applies a DDL atomically), reorg-worker batching,
    /// and the type changes needing a full index rebuild across a partitioned
    /// table -- none of those surfaces exist here.
    #[test]
    fn modify_column() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT NOT NULL DEFAULT 5, KEY kb (b))")
            .unwrap();
        session
            .run("INSERT INTO t VALUES (1, 'xx', 7), (2, 'yy', 8)")
            .unwrap();

        // Captured: widening keeps the rows, and the index still reads.
        session
            .run("ALTER TABLE t MODIFY COLUMN b VARCHAR(20)")
            .unwrap();
        assert_eq!(
            session.run("SELECT a FROM t WHERE b = 'xx'").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: CHANGE renames the column, and the rows survive.
        session
            .run("ALTER TABLE t CHANGE COLUMN c d BIGINT")
            .unwrap();
        assert_eq!(
            session.run("SELECT a, d FROM t").unwrap(),
            StmtResult::Rows(vec![
                vec![Datum::Int(1), Datum::Int(7)],
                vec![Datum::Int(2), Datum::Int(8)],
            ])
        );
        assert!(session.run("SELECT c FROM t").is_err());

        // Captured: an unknown column is 1054 naming the table, unless the
        // statement says IF EXISTS.
        assert!(matches!(
            session.run("ALTER TABLE t MODIFY COLUMN nosuch BIGINT"),
            Err(DriverError::UnknownColumnInTable { .. })
        ));
        assert!(matches!(
            session.run("ALTER TABLE t CHANGE COLUMN nosuch e BIGINT"),
            Err(DriverError::UnknownColumnInTable { .. })
        ));
        session
            .run("ALTER TABLE t MODIFY COLUMN IF EXISTS nosuch BIGINT")
            .unwrap();

        // Captured: a value the new type cannot read is 1292, and the table is
        // left untouched.
        assert!(matches!(
            session.run("ALTER TABLE t MODIFY COLUMN b BIGINT"),
            Err(DriverError::TruncatedIncorrectValue { kind: "DOUBLE", .. })
        ));
        assert_eq!(
            row_text(session.run("SELECT b FROM t WHERE a = 1")),
            [["xx"]]
        );

        // Captured: a clustered handle cannot leave the integer domain (8200),
        // but may change to another integer type.
        assert!(matches!(
            session.run("ALTER TABLE t MODIFY COLUMN a VARCHAR(10)"),
            Err(DriverError::UnsupportedModifyColumn(_))
        ));
        session.run("ALTER TABLE t MODIFY COLUMN a INT").unwrap();

        // Captured: an index cannot cover a full BLOB/TEXT column (1170).
        assert!(matches!(
            session.run("ALTER TABLE t MODIFY COLUMN b TEXT"),
            Err(DriverError::BlobKeyWithoutLength(_))
        ));

        // Captured: NOT NULL and DEFAULT come from the new definition.
        session
            .run("ALTER TABLE t MODIFY COLUMN b VARCHAR(20) NOT NULL")
            .unwrap();
        assert!(session.run("INSERT INTO t (a, d) VALUES (3, 1)").is_err());
        session
            .run("ALTER TABLE t MODIFY COLUMN d BIGINT DEFAULT 3")
            .unwrap();

        // Captured: FIRST and AFTER move the column, rows and index included.
        session
            .run("ALTER TABLE t MODIFY COLUMN b VARCHAR(20) NOT NULL FIRST")
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT * FROM t")),
            [["xx", "1", "7"], ["yy", "2", "8"]]
        );
        session
            .run("ALTER TABLE t CHANGE COLUMN b bb VARCHAR(20) NOT NULL AFTER d")
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT * FROM t")),
            [["1", "7", "xx"], ["2", "8", "yy"]]
        );
        // The renamed, moved column still reads through its index.
        assert_eq!(
            session.run("SELECT a FROM t WHERE bb = 'xx'").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: renaming onto an existing column is 1060; renaming a
        // column to its own name is allowed.
        assert!(matches!(
            session.run("ALTER TABLE t CHANGE COLUMN bb a VARCHAR(20)"),
            Err(DriverError::DuplicateColumnName(_))
        ));
        session
            .run("ALTER TABLE t CHANGE COLUMN d d BIGINT")
            .unwrap();

        // Captured: a stored NULL is rejected by a new NOT NULL, with the
        // row's position; a convertible string becomes the new type.
        let mut session = Session::new();
        session
            .run("CREATE TABLE u (a BIGINT, b VARCHAR(10), c BIGINT)")
            .unwrap();
        session.run("INSERT INTO u VALUES (1, '12', NULL)").unwrap();
        assert!(matches!(
            session.run("ALTER TABLE u MODIFY COLUMN c BIGINT NOT NULL"),
            Err(DriverError::DataTruncatedAtRow { row: 1, .. })
        ));
        session.run("ALTER TABLE u MODIFY COLUMN b BIGINT").unwrap();
        assert_eq!(
            session.run("SELECT b FROM u").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(12)]])
        );

        // Captured: a value too wide for the narrowed type is 1265, and the
        // table keeps its old definition.
        session
            .run("CREATE TABLE w (a BIGINT, b VARCHAR(10))")
            .unwrap();
        session.run("INSERT INTO w VALUES (1, 'xxxxxxxx')").unwrap();
        assert!(matches!(
            session.run("ALTER TABLE w MODIFY COLUMN b VARCHAR(3)"),
            Err(DriverError::DataTruncatedValue { .. })
        ));
        assert_eq!(row_text(session.run("SELECT b FROM w")), [["xxxxxxxx"]]);
    }

    #[test]
    fn rename_table() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t1 (id BIGINT PRIMARY KEY, v BIGINT, KEY kv (v))")
            .unwrap();
        session.run("INSERT INTO t1 VALUES (1, 9)").unwrap();

        // Captured: the table is renamed and keeps its rows.
        session.run("RENAME TABLE t1 TO t2").unwrap();
        assert_eq!(
            session.run("SELECT id, v FROM t2").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1), Datum::Int(9)]])
        );
        assert!(session.run("SELECT id FROM t1").is_err());
        // Its indexes come along, so a read through one still works.
        assert_eq!(
            session.run("SELECT id FROM t2 WHERE v = 9").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // Captured: renaming onto an existing name is 1050.
        session.run("CREATE TABLE t3 (a BIGINT)").unwrap();
        assert!(matches!(
            session.run("RENAME TABLE t2 TO t3"),
            Err(DriverError::Schema(SchemaErrorKind::TableExists(_)))
        ));
        // Captured: renaming a table that does not exist is 1146.
        assert!(matches!(
            session.run("RENAME TABLE nosuch TO t9"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownTable(_)))
        ));

        // Captured: ALTER TABLE ... RENAME TO is the same operation.
        session.run("ALTER TABLE t2 RENAME TO t4").unwrap();
        match session.run_with_columns("SHOW TABLES").unwrap() {
            StmtOutput::Rows { rows, .. } => assert_eq!(
                rows.into_iter()
                    .map(|row| datum_text(&row[0]).unwrap())
                    .collect::<Vec<_>>(),
                vec!["t3".to_owned(), "t4".to_owned()]
            ),
            other => panic!("expected rows, got {other:?}"),
        }

        // A rename may move the table to another schema.
        session.run("CREATE DATABASE other").unwrap();
        session.run("RENAME TABLE t4 TO other.moved").unwrap();
        assert_eq!(
            session.run("SELECT id FROM other.moved").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );

        // The renamed table reports its NEW name in a duplicate-key error,
        // which is the table.index form TiDB uses.
        session
            .run("CREATE TABLE dup (a BIGINT, UNIQUE KEY ua (a))")
            .unwrap();
        session.run("INSERT INTO dup VALUES (1)").unwrap();
        session.run("RENAME TABLE dup TO dup2").unwrap();
        match session.run("INSERT INTO dup2 VALUES (1)") {
            Err(DriverError::DuplicateEntry { key, .. }) => assert_eq!(key, "dup2.ua"),
            other => panic!("expected a duplicate-entry error, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_kinds_error() {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a INT)").unwrap();
        // Shapes the write paths do not model yet. (ORDER BY and LIMIT used
        // to be the examples here; both work now -- see
        // `insert_select_and_ordered_dml`.)
        assert!(session.run("DELETE QUICK FROM t").is_err());
        // RETURNING is not one of them: Go parses it and silently ignores it,
        // so the insert lands with a plain OK.
        assert_eq!(
            session
                .run("INSERT INTO t (a) VALUES (1) RETURNING a")
                .unwrap(),
            StmtResult::Affected(1)
        );
    }

    /// A CORRELATED scalar subquery in the SELECT list: an Apply above the
    /// filter, one inner run per outer row.
    ///
    /// Every assertion is a capture of real TiDB on the same schema
    /// (`testkit.CreateMockStore`, `pkg/executor`).
    #[test]
    fn correlated_subquery_in_select_list() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t1 (id INT, name VARCHAR(20))")
            .unwrap();
        session
            .run("CREATE TABLE t2 (id INT, t1_id INT, v INT)")
            .unwrap();
        session
            .run("INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c')")
            .unwrap();
        session
            .run("INSERT INTO t2 VALUES (10,1,100),(11,1,200),(12,2,300)")
            .unwrap();

        // COUNT answers 0 for the outer row with no match -- the inner
        // aggregate over an empty group, NOT the "no rows" NULL.
        assert_eq!(
            row_text(session.run(
                "SELECT id, (SELECT COUNT(*) FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "2".to_owned()],
                vec!["2".to_owned(), "1".to_owned()],
                vec!["3".to_owned(), "0".to_owned()],
            ]
        );

        // MAX over an empty group is NULL, so the unmatched outer row is NULL.
        assert_eq!(
            row_text(session.run(
                "SELECT id, (SELECT MAX(v) FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "200".to_owned()],
                vec!["2".to_owned(), "300".to_owned()],
                vec!["3".to_owned(), "NULL".to_owned()],
            ]
        );

        // ORDER BY the subquery's alias sorts on the Apply's column, and NULL
        // sorts first ascending.
        assert_eq!(
            row_text(session.run(
                "SELECT id, (SELECT SUM(v) FROM t2 WHERE t2.t1_id = t1.id) AS s FROM t1 ORDER BY s"
            )),
            vec![
                vec!["3".to_owned(), "NULL".to_owned()],
                vec!["1".to_owned(), "300".to_owned()],
                vec!["2".to_owned(), "300".to_owned()],
            ]
        );

        // Go's max-one-row check: 1242, raised per outer row.
        assert!(matches!(
            session.run("SELECT id, (SELECT v FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"),
            Err(DriverError::SubqueryReturnsMoreThanOneRow)
        ));

        // An UNcorrelated subquery beside a correlated one still folds to a
        // constant, so both fields answer in the same row.
        assert_eq!(
            row_text(session.run(
                "SELECT id, (SELECT COUNT(*) FROM t2) AS u, \
                 (SELECT COUNT(*) FROM t2 WHERE t2.t1_id = t1.id) AS c FROM t1 ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "3".to_owned(), "2".to_owned()],
                vec!["2".to_owned(), "3".to_owned(), "1".to_owned()],
                vec!["3".to_owned(), "3".to_owned(), "0".to_owned()],
            ]
        );

        // Inside an expression: the Apply column is an ordinary operand.
        assert_eq!(
            row_text(session.run(
                "SELECT id, (SELECT COUNT(*) FROM t2 WHERE t2.t1_id = t1.id) + 1 FROM t1 ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "3".to_owned()],
                vec!["2".to_owned(), "2".to_owned()],
                vec!["3".to_owned(), "1".to_owned()],
            ]
        );
        // NULL + 1 is NULL, so the unmatched row stays NULL through the
        // arithmetic.
        assert_eq!(
            row_text(session.run(
                "SELECT id, (SELECT MAX(v) FROM t2 WHERE t2.t1_id = t1.id) + 1 FROM t1 ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "201".to_owned()],
                vec!["2".to_owned(), "301".to_owned()],
                vec!["3".to_owned(), "NULL".to_owned()],
            ]
        );

        // The outer column the inner query reads need not be the projected
        // one, and ORDER BY over it is unaffected.
        assert_eq!(
            row_text(session.run(
                "SELECT name, (SELECT COUNT(*) FROM t2 WHERE t2.t1_id = t1.id) FROM t1 \
                 ORDER BY name"
            )),
            vec![
                vec!["a".to_owned(), "2".to_owned()],
                vec!["b".to_owned(), "1".to_owned()],
                vec!["c".to_owned(), "0".to_owned()],
            ]
        );
    }

    /// The CORRELATED semi-join shapes: `[NOT] IN` and `<op> ANY|ALL` over a
    /// subquery that reads the outer row.
    ///
    /// Every assertion is a capture of real TiDB on this schema
    /// (`testkit.CreateMockStore`, `pkg/executor`). The rows are chosen for
    /// the three traps: an inner set holding NULL (id 2), an EMPTY inner set
    /// (id 4), and a NULL left operand (id 4 again).
    #[test]
    fn correlated_semi_join_subqueries() {
        let mut session = semi_join_session();

        // IN: matched is 1; an unmatched left operand against a set holding
        // NULL is NULL, not 0; an EMPTY set is 0 even for a NULL operand.
        assert_eq!(
            row_text(session.run(
                "SELECT id, v IN (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "1".to_owned()],
                vec!["2".to_owned(), "NULL".to_owned()],
                vec!["3".to_owned(), "1".to_owned()],
                vec!["4".to_owned(), "0".to_owned()],
            ]
        );

        // NOT IN is the negation, NULL included: the row whose inner set holds
        // a NULL stays NULL and is therefore filtered out by a WHERE.
        assert_eq!(
            row_text(session.run(
                "SELECT id, v NOT IN (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "0".to_owned()],
                vec!["2".to_owned(), "NULL".to_owned()],
                vec!["3".to_owned(), "0".to_owned()],
                vec!["4".to_owned(), "1".to_owned()],
            ]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT id FROM t1 WHERE v IN (SELECT w FROM t2 WHERE t2.t1_id = t1.id) \
                 ORDER BY id"
            )),
            vec![vec!["1".to_owned()], vec!["3".to_owned()]]
        );
        // The NULL trap in a WHERE: only the EMPTY-set row survives NOT IN --
        // the id-2 row is NULL (its set holds a NULL), and NULL is not true.
        assert_eq!(
            row_text(session.run(
                "SELECT id FROM t1 WHERE v NOT IN (SELECT w FROM t2 WHERE t2.t1_id = t1.id) \
                 ORDER BY id"
            )),
            vec![vec!["4".to_owned()]]
        );

        // `> ANY` is the OR chain: false OR NULL is NULL (id 2), and an empty
        // set is FALSE (id 4, whose left operand is NULL too).
        assert_eq!(
            row_text(session.run(
                "SELECT id, v > ANY (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "1".to_owned()],
                vec!["2".to_owned(), "NULL".to_owned()],
                vec!["3".to_owned(), "0".to_owned()],
                vec!["4".to_owned(), "0".to_owned()],
            ]
        );
        // `> ALL` is the AND chain: false AND NULL is FALSE, so id 2 answers 0
        // rather than NULL -- and the EMPTY set is vacuously TRUE (id 4).
        assert_eq!(
            row_text(session.run(
                "SELECT id, v > ALL (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "0".to_owned()],
                vec!["2".to_owned(), "0".to_owned()],
                vec!["3".to_owned(), "0".to_owned()],
                vec!["4".to_owned(), "1".to_owned()],
            ]
        );
        // `< ALL` keeps the NULL, because every comparison is true or NULL.
        assert_eq!(
            row_text(session.run(
                "SELECT id, v < ALL (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "0".to_owned()],
                vec!["2".to_owned(), "NULL".to_owned()],
                vec!["3".to_owned(), "0".to_owned()],
                vec!["4".to_owned(), "1".to_owned()],
            ]
        );
        // `= ANY` answers exactly as IN does, empty set included.
        assert_eq!(
            row_text(session.run(
                "SELECT id, v = ANY (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "1".to_owned()],
                vec!["2".to_owned(), "NULL".to_owned()],
                vec!["3".to_owned(), "1".to_owned()],
                vec!["4".to_owned(), "0".to_owned()],
            ]
        );
    }

    /// A CORRELATED scalar subquery in a GROUPED select list: the Apply sits
    /// ABOVE the aggregation, so the subquery is bound to the GROUP's value
    /// and runs once per output row.
    ///
    /// Captured from real TiDB on the same schema.
    #[test]
    fn correlated_subquery_in_aggregate_select() {
        let mut session = semi_join_session();

        assert_eq!(
            row_text(session.run(
                "SELECT id, (SELECT MAX(w) FROM t2 WHERE t2.t1_id = id) FROM t1 \
                 GROUP BY id ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "10".to_owned()],
                vec!["2".to_owned(), "25".to_owned()],
                vec!["3".to_owned(), "30".to_owned()],
                vec!["4".to_owned(), "NULL".to_owned()],
            ]
        );

        // Beside an ordinary aggregate, and in any field position.
        assert_eq!(
            row_text(session.run(
                "SELECT id, COUNT(*), (SELECT MAX(w) FROM t2 WHERE t2.t1_id = id) FROM t1 \
                 GROUP BY id ORDER BY id"
            )),
            vec![
                vec!["1".to_owned(), "1".to_owned(), "10".to_owned()],
                vec!["2".to_owned(), "1".to_owned(), "25".to_owned()],
                vec!["3".to_owned(), "1".to_owned(), "30".to_owned()],
                vec!["4".to_owned(), "1".to_owned(), "NULL".to_owned()],
            ]
        );

        // The NULL group binds a NULL into the inner comparison, which matches
        // nothing -- COUNT answers 0 rather than NULL.
        assert_eq!(
            row_text(session.run(
                "SELECT v, (SELECT COUNT(*) FROM t2 WHERE t2.w = v) FROM t1 \
                 GROUP BY v ORDER BY v"
            )),
            vec![
                vec!["NULL".to_owned(), "0".to_owned()],
                vec!["10".to_owned(), "1".to_owned()],
                vec!["20".to_owned(), "0".to_owned()],
                vec!["30".to_owned(), "1".to_owned()],
            ]
        );

        // The grouped column the subquery reads need not be projected: it
        // rides a hidden carrier out of the aggregation and is trimmed again.
        assert_eq!(
            row_text(session.run(
                "SELECT (SELECT MAX(w) FROM t2 WHERE t2.t1_id = id) FROM t1 \
                 GROUP BY id ORDER BY id"
            )),
            vec![
                vec!["10".to_owned()],
                vec!["25".to_owned()],
                vec!["30".to_owned()],
                vec!["NULL".to_owned()],
            ]
        );
    }

    /// The two tables the captured semi-join cases run against.
    fn semi_join_session() -> Session {
        let mut session = Session::new();
        session.run("CREATE TABLE t1 (id INT, v INT)").unwrap();
        session.run("CREATE TABLE t2 (t1_id INT, w INT)").unwrap();
        session
            .run("INSERT INTO t1 VALUES (1,10),(2,20),(3,30),(4,NULL)")
            .unwrap();
        session
            .run("INSERT INTO t2 VALUES (1,10),(1,5),(2,25),(2,NULL),(3,30)")
            .unwrap();
        session
    }

    /// A session seeded with the ranking-window fixture: duplicate `v` values
    /// inside each `g` group, so ties are exercised in every direction.
    fn window_session() -> Session {
        let mut session = Session::new();
        session.run("CREATE TABLE t (g BIGINT, v BIGINT)").unwrap();
        session
            .run("INSERT INTO t VALUES (1,10),(1,20),(1,20),(1,30),(1,40),(2,5),(2,5),(2,7)")
            .unwrap();
        session
    }

    /// A result's column names and rows as text, matching how the captured
    /// Go output above prints them.
    fn query_text(session: &mut Session, sql: &str) -> (Vec<String>, Vec<Vec<String>>) {
        match session.run_with_columns(sql).unwrap() {
            StmtOutput::Rows { columns, rows } => (
                columns
                    .into_iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                rows.into_iter()
                    .map(|row| {
                        row.iter()
                            .map(|value| match value {
                                Datum::Null => "<nil>".to_owned(),
                                Datum::Int(v) => v.to_string(),
                                other => datum_text(other).unwrap_or_default(),
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>(),
            ),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// A session with `t`, `s` and the views the captures were taken over.
    fn view_session() -> Session {
        let mut session = Session::new();
        session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
        session
            .run("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();
        session
            .run("CREATE TABLE s (a BIGINT, c VARCHAR(20))")
            .unwrap();
        session
            .run("INSERT INTO s VALUES (1, 'x'), (2, 'y')")
            .unwrap();
        session.run("CREATE VIEW v AS SELECT a, b FROM t").unwrap();
        session
            .run("CREATE VIEW v2(a2) AS SELECT a FROM t")
            .unwrap();
        session
            .run("CREATE VIEW v3 AS SELECT * FROM v WHERE b > 10")
            .unwrap();
        session
    }

    /// `ROW_NUMBER`/`RANK`/`DENSE_RANK` over ties, checked against captured
    /// TiDB output.
    ///
    /// The three differ only on peers: `ROW_NUMBER` numbers every row,
    /// `RANK` gives peers the same rank and then SKIPS to the next row's
    /// 1-based position, `DENSE_RANK` gives peers the same rank and never
    /// skips.
    #[test]
    fn window_ranking_functions_over_ties() {
        let mut session = window_session();

        assert_eq!(
            row_text(session.run(
                "SELECT g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn \
                 FROM t ORDER BY g, v, rn"
            )),
            [
                ["1", "10", "1"],
                ["1", "20", "2"],
                ["1", "20", "3"],
                ["1", "30", "4"],
                ["1", "40", "5"],
                ["2", "5", "1"],
                ["2", "5", "2"],
                ["2", "7", "3"],
            ]
        );

        // Captured: the tied 20s both rank 2, and 30 jumps to 4.
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, RANK() OVER (PARTITION BY g ORDER BY v) AS r \
                 FROM t ORDER BY g, v"
            )),
            [
                ["1", "10", "1"],
                ["1", "20", "2"],
                ["1", "20", "2"],
                ["1", "30", "4"],
                ["1", "40", "5"],
                ["2", "5", "1"],
                ["2", "5", "1"],
                ["2", "7", "3"],
            ]
        );

        // Captured: the same ties, but 30 is 3, not 4.
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, DENSE_RANK() OVER (PARTITION BY g ORDER BY v) AS r \
                 FROM t ORDER BY g, v"
            )),
            [
                ["1", "10", "1"],
                ["1", "20", "2"],
                ["1", "20", "2"],
                ["1", "30", "3"],
                ["1", "40", "4"],
                ["2", "5", "1"],
                ["2", "5", "1"],
                ["2", "7", "2"],
            ]
        );

        // No window ORDER BY at all: every row of the partition is a peer,
        // so both rank functions return 1 for all of them (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT g, RANK() OVER (PARTITION BY g) AS r, \
                 DENSE_RANK() OVER (PARTITION BY g) AS d FROM t ORDER BY g, r, d"
            )),
            [
                ["1", "1", "1"],
                ["1", "1", "1"],
                ["1", "1", "1"],
                ["1", "1", "1"],
                ["1", "1", "1"],
                ["2", "1", "1"],
                ["2", "1", "1"],
                ["2", "1", "1"],
            ]
        );

        // DESC reverses the window's own order, independently of the outer
        // ORDER BY (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC) AS rn \
                 FROM t WHERE g = 2 ORDER BY rn"
            )),
            [["2", "7", "1"], ["2", "5", "2"], ["2", "5", "3"]]
        );
    }

    /// `NTILE(n)`'s bucket sizing, checked against captured TiDB output.
    ///
    /// With `n` buckets over `rows` rows the FIRST `rows % n` buckets take
    /// one extra row (`quotient + 1`) and the rest take `quotient`; when
    /// `n > rows` the surplus buckets stay empty.
    #[test]
    fn window_ntile_bucket_distribution() {
        let mut session = window_session();

        // 5 rows into 2 buckets -> 3 then 2; 3 rows into 2 -> 2 then 1.
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, NTILE(2) OVER (PARTITION BY g ORDER BY v) AS b \
                 FROM t ORDER BY g, v"
            )),
            [
                ["1", "10", "1"],
                ["1", "20", "1"],
                ["1", "20", "1"],
                ["1", "30", "2"],
                ["1", "40", "2"],
                ["2", "5", "1"],
                ["2", "5", "1"],
                ["2", "7", "2"],
            ]
        );

        // 5 rows into 3 buckets -> 2, 2, 1 (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT v, NTILE(3) OVER (PARTITION BY g ORDER BY v) AS b \
                 FROM t WHERE g = 1 ORDER BY v"
            )),
            [
                ["10", "1"],
                ["20", "1"],
                ["20", "2"],
                ["30", "2"],
                ["40", "3"]
            ]
        );

        // More buckets than rows: one row each, the rest empty (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT v, NTILE(5) OVER (PARTITION BY g ORDER BY v) AS b \
                 FROM t WHERE g = 2 ORDER BY b"
            )),
            [["5", "1"], ["5", "2"], ["7", "3"]]
        );

        // One bucket holds everything (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT NTILE(1) OVER (PARTITION BY g ORDER BY v) AS b \
                 FROM t WHERE g = 2 ORDER BY b"
            )),
            [["1"], ["1"], ["1"]]
        );

        // Without PARTITION BY the whole result is one partition: 8 rows
        // into 2 buckets -> 4 then 4 (captured).
        assert_eq!(
            row_text(session.run("SELECT v, NTILE(2) OVER (ORDER BY v) AS b FROM t ORDER BY v")),
            [
                ["5", "1"],
                ["5", "1"],
                ["7", "1"],
                ["10", "1"],
                ["20", "2"],
                ["20", "2"],
                ["30", "2"],
                ["40", "2"],
            ]
        );
    }

    /// The empty and partition-less specs, plus named windows, checked
    /// against captured TiDB output.
    #[test]
    fn window_specs_and_named_windows() {
        let mut session = window_session();

        // `OVER ()`: one partition, no order -- the rows keep their source
        // order and are numbered through it (captured).
        assert_eq!(
            row_text(session.run("SELECT g, v, ROW_NUMBER() OVER () AS rn FROM t ORDER BY rn")),
            [
                ["1", "10", "1"],
                ["1", "20", "2"],
                ["1", "20", "3"],
                ["1", "30", "4"],
                ["1", "40", "5"],
                ["2", "5", "6"],
                ["2", "5", "7"],
                ["2", "7", "8"],
            ]
        );

        // No PARTITION BY, just an order: one partition across the table.
        assert_eq!(
            row_text(
                session.run("SELECT v, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM t ORDER BY rn")
            ),
            [
                ["5", "1"],
                ["5", "2"],
                ["7", "3"],
                ["10", "4"],
                ["20", "5"],
                ["20", "6"],
                ["30", "7"],
                ["40", "8"],
            ]
        );

        // One named window feeding two calls (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, ROW_NUMBER() OVER w AS rn, RANK() OVER w AS r \
                 FROM t WINDOW w AS (PARTITION BY g ORDER BY v) ORDER BY g, v, rn"
            )),
            [
                ["1", "10", "1", "1"],
                ["1", "20", "2", "2"],
                ["1", "20", "3", "2"],
                ["1", "30", "4", "4"],
                ["1", "40", "5", "5"],
                ["2", "5", "1", "1"],
                ["2", "5", "2", "1"],
                ["2", "7", "3", "3"],
            ]
        );

        // `OVER (w ...)`: a parenthesized reference may EXTEND the named
        // window with an ORDER BY the base does not have (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT v, ROW_NUMBER() OVER (w ORDER BY v) AS rn \
                 FROM t WHERE g = 2 WINDOW w AS (PARTITION BY g) ORDER BY rn"
            )),
            [["5", "1"], ["5", "2"], ["7", "3"]]
        );

        // A window function alongside plain columns and expressions
        // (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT g, v + 1, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn \
                 FROM t WHERE g = 2 ORDER BY rn"
            )),
            [["2", "6", "1"], ["2", "6", "2"], ["2", "8", "3"]]
        );
    }

    /// The outer `ORDER BY` runs AFTER the window is computed, checked
    /// against captured TiDB output: the ranking reflects the WINDOW's order,
    /// while the rows come out in the OUTER order.
    #[test]
    fn window_outer_order_by_applies_after_computation() {
        let mut session = window_session();

        assert_eq!(
            row_text(session.run(
                "SELECT v, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn \
                 FROM t ORDER BY v DESC, g, rn"
            )),
            [
                ["40", "1", "5"],
                ["30", "1", "4"],
                ["20", "1", "2"],
                ["20", "1", "3"],
                ["10", "1", "1"],
                ["7", "2", "3"],
                ["5", "2", "1"],
                ["5", "2", "2"],
            ]
        );

        // Ordering by the window column's POSITION works the same way
        // (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) \
                 FROM t ORDER BY 3 DESC, g"
            )),
            [
                ["1", "40", "5"],
                ["1", "30", "4"],
                ["1", "20", "3"],
                ["2", "7", "3"],
                ["1", "20", "2"],
                ["2", "5", "2"],
                ["1", "10", "1"],
                ["2", "5", "1"],
            ]
        );
    }

    /// The ranking functions' result types, checked against captured TiDB
    /// metadata: `BIGINT(21)` for all four, `NOT NULL` for the three ranking
    /// ones and `UNSIGNED`/binary for `NTILE`.
    #[test]
    fn window_result_types() {
        let mut session = window_session();

        match session
            .run_with_columns("SELECT ROW_NUMBER() OVER (ORDER BY v) FROM t")
            .unwrap()
        {
            StmtOutput::Rows { columns, .. } => {
                let (_, ftype) = &columns[0];
                assert_eq!(ftype.code(), tidb_datatype::FieldTypeCode::LongLong);
                assert_eq!(ftype.flen(), 21);
                assert!(!ftype.is_unsigned());
                assert_ne!(ftype.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL, 0);
            }
            other => panic!("expected rows, got {other:?}"),
        }

        match session
            .run_with_columns("SELECT NTILE(2) OVER (ORDER BY v) FROM t")
            .unwrap()
        {
            StmtOutput::Rows { columns, .. } => {
                let (_, ftype) = &columns[0];
                assert_eq!(ftype.code(), tidb_datatype::FieldTypeCode::LongLong);
                assert_eq!(ftype.flen(), 21);
                assert!(ftype.is_unsigned());
                assert_eq!(ftype.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL, 0);
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // The framed families' result TYPE CODES, captured over a `BIGINT v`:
        // an aggregate follows Go's `TypeInfer` (SUM/AVG are DECIMAL, COUNT a
        // NOT NULL BIGINT, MIN the argument's own type), and the value family
        // plus a defaultless LAG carry the argument's type. Go's display
        // WIDTHS on top (`DECIMAL(41,0)`, `DECIMAL(24,4)`) are the same
        // documented deferral the GROUP BY path this stage shares already has.
        use tidb_datatype::FieldTypeCode;
        for (sql, code) in [
            ("SELECT SUM(v) OVER () FROM t", FieldTypeCode::NewDecimal),
            ("SELECT AVG(v) OVER () FROM t", FieldTypeCode::NewDecimal),
            ("SELECT COUNT(v) OVER () FROM t", FieldTypeCode::LongLong),
            ("SELECT MIN(v) OVER () FROM t", FieldTypeCode::LongLong),
            (
                "SELECT FIRST_VALUE(v) OVER () FROM t",
                FieldTypeCode::LongLong,
            ),
            ("SELECT LAG(v) OVER () FROM t", FieldTypeCode::LongLong),
            // A default of the SAME type merges to that type (captured
            // `BIGINT`); a WIDENING default is refused, see
            // `window_errors_and_refusals`.
            (
                "SELECT LAG(v, 1, -1) OVER () FROM t",
                FieldTypeCode::LongLong,
            ),
        ] {
            match session.run_with_columns(sql).unwrap() {
                StmtOutput::Rows { columns, .. } => {
                    assert_eq!(columns[0].1.code(), code, "result type of {sql}");
                }
                other => panic!("expected rows for {sql}, got {other:?}"),
            }
        }

        // COUNT is the one framed function that is NOT NULL (an empty frame
        // counts 0 rather than yielding NULL).
        match session
            .run_with_columns("SELECT COUNT(v) OVER () FROM t")
            .unwrap()
        {
            StmtOutput::Rows { columns, .. } => {
                assert_ne!(
                    columns[0].1.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL,
                    0
                );
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// Every window error this slice reproduces, checked against captured
    /// TiDB errors.
    #[test]
    fn window_errors_and_refusals() {
        let mut session = window_session();

        // Captured: "[planner:3593]You cannot use the window function
        // 'row_number' in this context.'" -- WHERE and HAVING alike.
        assert!(matches!(
            session.run("SELECT g FROM t WHERE ROW_NUMBER() OVER (ORDER BY v) > 1"),
            Err(DriverError::WindowInvalidWindowFuncUse(ref name)) if name == "row_number"
        ));
        assert!(matches!(
            session.run("SELECT g FROM t GROUP BY g HAVING RANK() OVER (ORDER BY g) > 1"),
            Err(DriverError::WindowInvalidWindowFuncUse(ref name)) if name == "rank"
        ));

        // Captured: "[planner:1210]Incorrect arguments to ntile" for a zero,
        // a negative, and a non-constant bucket count.
        for sql in [
            "SELECT NTILE(0) OVER (ORDER BY v) FROM t",
            "SELECT NTILE(-1) OVER (ORDER BY v) FROM t",
            "SELECT NTILE(v) OVER (ORDER BY v) FROM t",
        ] {
            assert!(
                matches!(session.run(sql), Err(DriverError::WrongArguments("ntile"))),
                "expected ErrWrongArguments for {sql}"
            );
        }

        // Captured: "[planner:3579]Window name 'w' is not defined."
        assert!(matches!(
            session.run("SELECT ROW_NUMBER() OVER w FROM t"),
            Err(DriverError::WindowNoSuchWindow(ref name)) if name == "w"
        ));

        // Captured: "[planner:3581]A window which depends on another cannot
        // define partitioning."
        assert!(matches!(
            session.run(
                "SELECT ROW_NUMBER() OVER (w PARTITION BY g) FROM t \
                 WINDOW w AS (PARTITION BY g)"
            ),
            Err(DriverError::WindowNoChildPartitioning)
        ));

        // Captured: "[planner:3583]Window '<unnamed window>' cannot inherit
        // 'w' since both contain an ORDER BY clause."
        assert!(matches!(
            session.run(
                "SELECT ROW_NUMBER() OVER (w ORDER BY v) FROM t \
                 WINDOW w AS (PARTITION BY g ORDER BY v)"
            ),
            Err(DriverError::WindowNoRedefineOrderBy { ref window, ref base })
                if window == "<unnamed window>" && base == "w"
        ));

        // Captured with a NAMED extending window, which Go names in the same
        // message: "[planner:3583]Window 'w2' cannot inherit 'w' since both
        // contain an ORDER BY clause."
        assert!(matches!(
            session.run(
                "SELECT ROW_NUMBER() OVER w2 FROM t \
                 WINDOW w AS (PARTITION BY g ORDER BY v), w2 AS (w ORDER BY g)"
            ),
            Err(DriverError::WindowNoRedefineOrderBy { ref window, ref base })
                if window == "w2" && base == "w"
        ));

        // Captured: "[planner:3582]Window 'w' has a frame definition, so
        // cannot be referenced by another window."
        assert!(matches!(
            session.run(
                "SELECT ROW_NUMBER() OVER w2 FROM t \
                 WINDOW w AS (PARTITION BY g ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
                 w2 AS (w ORDER BY v)"
            ),
            Err(DriverError::WindowNoInheritFrame(ref base)) if base == "w"
        ));

        // Captured: "[planner:3580]There is a circularity in the window
        // dependency graph."
        assert!(matches!(
            session.run("SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (w2), w2 AS (w)"),
            Err(DriverError::WindowCircularity)
        ));

        // Captured: "[planner:1235]This version of TiDB doesn't yet support
        // 'group_concat as window function'" -- Go refuses GROUP_CONCAT
        // before it looks at any argument, and DISTINCT inside any window
        // call the same way.
        // GROUP_CONCAT: captured "[planner:1235]This version of TiDB doesn't
        // yet support 'group_concat as window function'", but this build's
        // parser does not accept an `OVER` clause on GROUP_CONCAT at all
        // (its AST node carries no window spec), so the statement fails
        // earlier, at parse time -- a documented parser-side deferral.
        assert!(matches!(
            session.run("SELECT GROUP_CONCAT(v) OVER (ORDER BY v) FROM t"),
            Err(DriverError::Parse(_))
        ));
        assert!(matches!(
            session.run("SELECT COUNT(DISTINCT v) OVER (PARTITION BY g) FROM t"),
            Err(DriverError::NotSupportedYet(
                "<window function>(DISTINCT ..)"
            ))
        ));

        // Outside this slice: the aggregates Go DOES allow OVER but this
        // build does not compute (APPROX_COUNT_DISTINCT, JSON_ARRAYAGG, ...).
        match session.run("SELECT g, APPROX_COUNT_DISTINCT(v) OVER (ORDER BY v) FROM t") {
            Err(DriverError::Unsupported(message)) => {
                assert!(
                    message.contains("ROW_NUMBER"),
                    "refusal should name this slice, got {message}"
                );
            }
            other => panic!("expected a slice refusal, got {other:?}"),
        }

        // Frame validation is the PLANNER's, so it fires for every function
        // -- including the ranking ones, whose frame is then ignored.
        // Captured: "[planner:3586]Window '<unnamed window>': frame start or
        // end is negative, NULL or of non-integral type".
        for sql in [
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM t",
            "SELECT ROW_NUMBER() OVER (PARTITION BY g ORDER BY v ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM t",
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 1.5 PRECEDING AND CURRENT ROW) FROM t",
        ] {
            assert!(
                matches!(session.run(sql), Err(DriverError::WindowFrameIllegal)),
                "expected 3586 for {sql}"
            );
        }

        // Captured: "[planner:3587]Window '<unnamed window>' with RANGE N
        // PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression,
        // of numeric or temporal type" -- and it OUTRANKS the RANGE-offset
        // deferral above, because Go checks the ORDER BY shape first.
        assert!(matches!(
            session.run("SELECT SUM(v) OVER (PARTITION BY g RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t"),
            Err(DriverError::WindowRangeFrameOrderType)
        ));

        // Captured: "[planner:3588]Window '<unnamed window>' with RANGE frame
        // has ORDER BY expression of datetime type. Only INTERVAL bound value
        // allowed." -- a numeric bound over a temporal key.
        session
            .run("CREATE TABLE rt (d DATE, v BIGINT)")
            .expect("create rt");
        session
            .run("INSERT INTO rt VALUES ('2020-01-01',1),('2020-01-02',2)")
            .expect("insert rt");
        assert!(matches!(
            session.run(
                "SELECT SUM(v) OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM rt"
            ),
            Err(DriverError::WindowRangeFrameTemporalType)
        ));

        // Captured: "[planner:3589]... of numeric type, INTERVAL bound value
        // not allowed." -- and, over a STRING key, 3587 wins over BOTH the
        // interval check and the interval refusal below.
        assert!(matches!(
            session.run(
                "SELECT SUM(v) OVER (ORDER BY v RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM t"
            ),
            Err(DriverError::WindowRangeFrameNumericType)
        ));
        session
            .run("CREATE TABLE rs (k VARCHAR(10), v BIGINT)")
            .expect("create rs");
        for sql in [
            "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM rs",
            "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM rs",
        ] {
            assert!(
                matches!(session.run(sql), Err(DriverError::WindowRangeFrameOrderType)),
                "expected 3587 for {sql}"
            );
        }

        // Captured: "[planner:1210]Incorrect arguments to nth_value" -- the
        // position must be a POSITIVE integer constant, like NTILE's count.
        assert!(matches!(
            session.run("SELECT NTH_VALUE(v, 0) OVER (PARTITION BY g ORDER BY v) FROM t"),
            Err(DriverError::WrongArguments("nth_value"))
        ));
    }

    /// The fixture the value-measured `RANGE` frame captures ran over: keys
    /// with a TIE (`3,3`) and a GAP (`3 -> 7`), which is what separates a
    /// value frame from a positional one.
    fn range_session() -> Session {
        let mut session = Session::new();
        session
            .run("CREATE TABLE ri (g BIGINT, k BIGINT, v BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO ri VALUES (1,1,10),(1,3,20),(1,3,30),(1,7,40),(1,8,50)")
            .unwrap();
        session
    }

    /// `RANGE BETWEEN N PRECEDING/FOLLOWING`: the boundary is a VALUE of the
    /// single `ORDER BY` key, so ties share a frame and a gap in the key
    /// SHRINKS the frame rather than shifting it.
    ///
    /// Every expectation is captured TiDB output over `k = 1,3,3,7,8`.
    #[test]
    fn window_range_value_bounds() {
        let mut session = range_session();

        for (frame, expected) in [
            (
                "RANGE BETWEEN 2 PRECEDING AND CURRENT ROW",
                [
                    ["10", "1"],
                    ["60", "3"],
                    ["60", "3"],
                    ["40", "1"],
                    ["90", "2"],
                ],
            ),
            (
                "RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING",
                [
                    ["10", "1"],
                    ["50", "2"],
                    ["50", "2"],
                    ["90", "2"],
                    ["90", "2"],
                ],
            ),
            (
                "RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING",
                [
                    ["60", "3"],
                    ["50", "2"],
                    ["50", "2"],
                    ["90", "2"],
                    ["50", "1"],
                ],
            ),
            // An empty frame: SUM is NULL but COUNT is 0, as under ROWS.
            (
                "RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING",
                [
                    ["50", "2"],
                    ["NULL", "0"],
                    ["NULL", "0"],
                    ["50", "1"],
                    ["NULL", "0"],
                ],
            ),
            (
                "RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING",
                [
                    ["NULL", "0"],
                    ["10", "1"],
                    ["10", "1"],
                    ["NULL", "0"],
                    ["40", "1"],
                ],
            ),
            (
                "RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING",
                [
                    ["10", "1"],
                    ["60", "3"],
                    ["60", "3"],
                    ["150", "5"],
                    ["150", "5"],
                ],
            ),
            (
                "RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING",
                [
                    ["150", "5"],
                    ["140", "4"],
                    ["140", "4"],
                    ["90", "2"],
                    ["90", "2"],
                ],
            ),
            // A zero-width value frame is still the whole PEER group.
            (
                "RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING",
                [
                    ["10", "1"],
                    ["50", "2"],
                    ["50", "2"],
                    ["40", "1"],
                    ["50", "1"],
                ],
            ),
        ] {
            assert_eq!(
                row_text(session.run(&format!(
                    "SELECT SUM(v) OVER (ORDER BY k {frame}) s, \
                     COUNT(*) OVER (ORDER BY k {frame}) c FROM ri"
                ))),
                expected,
                "frame {frame}"
            );
        }

        // A fractional offset is legal under RANGE (only ROWS demands an
        // integer): `1.5 PRECEDING` over `1,3,3,7,8` is `10,50,50,40,90`.
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) \
                 FROM ri"
            )),
            [["10"], ["50"], ["50"], ["40"], ["90"]]
        );

        // The frame is per PARTITION, as everywhere else.
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM ri"
            )),
            [["10"], ["50"], ["50"], ["90"], ["90"]]
        );

        // A DECIMAL key uses decimal arithmetic for the boundary value.
        session
            .run("CREATE TABLE rd (k DECIMAL(10,2), v BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO rd VALUES (1.00,1),(1.50,2),(2.25,3),(5.00,4)")
            .unwrap();
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM rd"
            )),
            [["1"], ["3"], ["5"], ["4"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING) \
                 FROM rd"
            )),
            [["3"], ["3"], ["3"], ["4"]]
        );
    }

    /// `RANGE` under `DESC`, and `RANGE` over NULL keys -- the two rules a
    /// positional reading of the frame gets wrong.
    #[test]
    fn window_range_desc_direction_and_nulls() {
        let mut session = range_session();

        // Under DESC, `N PRECEDING` reaches the LARGER keys (the ones that
        // sort EARLIER), so at `k = 7` the frame is `{8, 7}` and not `{7, 3}`.
        // Rows come out in source order `1,3,3,7,8`.
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) s, \
                 COUNT(*) OVER (ORDER BY k DESC RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) c FROM ri"
            )),
            [["60", "3"], ["50", "2"], ["50", "2"], ["90", "2"], ["50", "1"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) \
                 FROM ri"
            )),
            [["10"], ["60"], ["60"], ["40"], ["90"]]
        );

        // NULL keys form a frame of their OWN: they peer with each other and
        // with nothing else, so the two NULL rows see only each other (sum 3,
        // count 2) and no non-NULL row ever includes them.
        session.run("CREATE TABLE rn (k BIGINT, v BIGINT)").unwrap();
        session
            .run("INSERT INTO rn VALUES (NULL,1),(NULL,2),(1,10),(2,20),(5,50)")
            .unwrap();
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) s, \
                 COUNT(*) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) c FROM rn"
            )),
            [
                ["3", "2"],
                ["3", "2"],
                ["30", "2"],
                ["30", "2"],
                ["50", "1"]
            ]
        );
        // Under DESC the NULLs sort LAST, and still frame only each other.
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
                 FROM rn"
            )),
            [["3"], ["3"], ["30"], ["30"], ["50"]]
        );
    }

    /// A `LAG`/`LEAD` default that WIDENS the result type: Go merges the two
    /// argument types and reads BOTH operands through the merged one, so the
    /// VALUE argument changes domain too.
    #[test]
    fn window_lag_lead_widening_default() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE ll (id BIGINT, v BIGINT, d DECIMAL(10,2), s VARCHAR(10))")
            .unwrap();
        session
            .run("INSERT INTO ll VALUES (1,10,1.50,'a'),(2,20,2.50,'b'),(3,30,3.50,'c')")
            .unwrap();

        // Captured: the integers come back as STRINGS, not just the default.
        assert_eq!(
            row_text(session.run("SELECT LAG(v,1,'zz') OVER (ORDER BY id) FROM ll")),
            [["zz"], ["10"], ["20"]]
        );
        assert_eq!(
            row_text(session.run("SELECT LEAD(v,1,'zz') OVER (ORDER BY id) FROM ll")),
            [["20"], ["30"], ["zz"]]
        );
        // A DECIMAL default widens an integer argument to DECIMAL, and the
        // argument keeps its own scale (`10`, not the scale-padded `10.0`):
        // Go reads it through the merged type's EVAL kind, not through a
        // width-and-scale-applying conversion.
        assert_eq!(
            row_text(session.run("SELECT LAG(v,1,1.5) OVER (ORDER BY id) FROM ll")),
            [["1.5"], ["10"], ["20"]]
        );
        // The widening runs the other way too: an integer default over a
        // string argument merges to VARCHAR.
        assert_eq!(
            row_text(session.run("SELECT LAG(s,1,0) OVER (ORDER BY id) FROM ll")),
            [["0"], ["a"], ["b"]]
        );
        assert_eq!(
            row_text(session.run("SELECT LAG(d,1,'zz') OVER (ORDER BY id) FROM ll")),
            [["zz"], ["1.50"], ["2.50"]]
        );
        // Every position out of range takes the default.
        assert_eq!(
            row_text(session.run("SELECT LAG(v,5,'zz') OVER (ORDER BY id) FROM ll")),
            [["zz"], ["zz"], ["zz"]]
        );
        // A NULL default does NOT widen: Go's `InferType4ControlFuncs` drops
        // NULL-typed operands, so the result stays the argument's own type.
        assert_eq!(
            row_text(session.run("SELECT LAG(v,1,NULL) OVER (ORDER BY id) FROM ll")),
            [["NULL"], ["10"], ["20"]]
        );

        // The merged result TYPE, captured: VARCHAR for a string default,
        // DECIMAL for a decimal one, and the argument's own BIGINT when the
        // default is NULL or already an integer.
        use tidb_datatype::FieldTypeCode;
        for (sql, code) in [
            (
                "SELECT LAG(v,1,'zz') OVER (ORDER BY id) FROM ll",
                FieldTypeCode::Varchar,
            ),
            (
                "SELECT LAG(v,1,1.5) OVER (ORDER BY id) FROM ll",
                FieldTypeCode::NewDecimal,
            ),
            (
                "SELECT LAG(s,1,0) OVER (ORDER BY id) FROM ll",
                FieldTypeCode::Varchar,
            ),
            (
                "SELECT LAG(v,1,NULL) OVER (ORDER BY id) FROM ll",
                FieldTypeCode::LongLong,
            ),
            (
                "SELECT LAG(v,1,-1) OVER (ORDER BY id) FROM ll",
                FieldTypeCode::LongLong,
            ),
        ] {
            match session.run_with_columns(sql).unwrap() {
                StmtOutput::Rows { columns, .. } => {
                    assert_eq!(columns[0].1.code(), code, "result type for {sql}");
                }
                other => panic!("expected rows, got {other:?}"),
            }
        }
    }

    /// `PERCENT_RANK()` and `CUME_DIST()`: both are PEER-based, both ignore
    /// the frame, and `PERCENT_RANK` answers `0` rather than NaN when the
    /// partition holds a single row.
    #[test]
    fn window_percent_rank_and_cume_dist() {
        let mut session = Session::new();
        session.run("CREATE TABLE pr (g BIGINT, v BIGINT)").unwrap();
        session
            .run("INSERT INTO pr VALUES (1,10),(1,20),(1,20),(1,30),(2,5)")
            .unwrap();

        // Captured: the tied 20s SHARE both values, and the one-row partition
        // `g = 2` is PERCENT_RANK 0 / CUME_DIST 1.
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, PERCENT_RANK() OVER (PARTITION BY g ORDER BY v) p, \
                 CUME_DIST() OVER (PARTITION BY g ORDER BY v) c FROM pr ORDER BY g, v"
            )),
            [
                ["1", "10", "0", "0.25"],
                ["1", "20", "0.3333333333333333", "0.75"],
                ["1", "20", "0.3333333333333333", "0.75"],
                ["1", "30", "1", "1"],
                ["2", "5", "0", "1"],
            ]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT PERCENT_RANK() OVER (ORDER BY v) p, CUME_DIST() OVER (ORDER BY v) c \
                 FROM pr ORDER BY v"
            )),
            [
                ["0", "0.2"],
                ["0.25", "0.4"],
                ["0.5", "0.8"],
                ["0.5", "0.8"],
                ["1", "1"],
            ]
        );
        // With NO ORDER BY every row is a peer: rank 1 everywhere, so
        // PERCENT_RANK is 0 and CUME_DIST is 1.
        assert_eq!(
            row_text(session.run("SELECT PERCENT_RANK() OVER () p, CUME_DIST() OVER () c FROM pr")),
            [["0", "1"], ["0", "1"], ["0", "1"], ["0", "1"], ["0", "1"],]
        );
        // DESC reverses which peer group is first.
        assert_eq!(
            row_text(session.run(
                "SELECT PERCENT_RANK() OVER (ORDER BY v DESC) p, \
                 CUME_DIST() OVER (ORDER BY v DESC) c FROM pr ORDER BY v DESC"
            )),
            [
                ["0", "0.2"],
                ["0.25", "0.6"],
                ["0.25", "0.6"],
                ["0.75", "0.8"],
                ["1", "1"],
            ]
        );
        // A written frame is IGNORED by both (the values match the frameless
        // form above), though it is still VALIDATED.
        assert_eq!(
            row_text(session.run(
                "SELECT PERCENT_RANK() OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
                 FROM pr ORDER BY v"
            )),
            [["0"], ["0.25"], ["0.5"], ["0.5"], ["1"]]
        );
        assert!(matches!(
            session.run(
                "SELECT PERCENT_RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) \
                 FROM pr"
            ),
            Err(DriverError::WindowFrameIllegal)
        ));

        // Both are a NOT NULL DOUBLE (Go's `typeInfer4PercentRank` /
        // `typeInfer4CumeDist`).
        for sql in [
            "SELECT PERCENT_RANK() OVER (ORDER BY v) FROM pr",
            "SELECT CUME_DIST() OVER (ORDER BY v) FROM pr",
        ] {
            match session.run_with_columns(sql).unwrap() {
                StmtOutput::Rows { columns, .. } => {
                    let (_, ftype) = &columns[0];
                    assert_eq!(ftype.code(), tidb_datatype::FieldTypeCode::Double, "{sql}");
                    assert_ne!(ftype.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL, 0);
                }
                other => panic!("expected rows, got {other:?}"),
            }
        }
    }

    /// A window function combined with `GROUP BY`: the window computes over
    /// the POST-aggregation rows, so its `ORDER BY` may name an aggregate and
    /// `HAVING` has already removed the groups it never sees.
    #[test]
    fn window_over_group_by() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE gw (g BIGINT, h BIGINT, v BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO gw VALUES (1,1,10),(1,2,20),(2,1,30),(2,2,5),(3,1,15)")
            .unwrap();

        // Captured: the ranks follow the GROUP sums (15, 30, 35), not any
        // source row.
        assert_eq!(
            row_text(session.run(
                "SELECT g, SUM(v) s, RANK() OVER (ORDER BY SUM(v)) r FROM gw GROUP BY g \
                 ORDER BY g"
            )),
            [["1", "30", "2"], ["2", "35", "3"], ["3", "15", "1"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT g, ROW_NUMBER() OVER (ORDER BY SUM(v) DESC) r FROM gw GROUP BY g \
                 ORDER BY g"
            )),
            [["1", "2"], ["2", "1"], ["3", "3"]]
        );
        // An aggregate INSIDE a window aggregate: the running total of the
        // group sums.
        assert_eq!(
            row_text(
                session
                    .run("SELECT g, SUM(SUM(v)) OVER (ORDER BY g) t FROM gw GROUP BY g ORDER BY g")
            ),
            [["1", "30"], ["2", "65"], ["3", "80"]]
        );
        // HAVING runs BELOW the window, so the removed group never counts:
        // ranks are 1 and 2 over the two surviving groups.
        assert_eq!(
            row_text(session.run(
                "SELECT g, SUM(v) s, RANK() OVER (ORDER BY SUM(v)) r FROM gw GROUP BY g \
                 HAVING SUM(v) > 15 ORDER BY g"
            )),
            [["1", "30", "1"], ["2", "35", "2"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT g, SUM(v) s, RANK() OVER (ORDER BY g) r FROM gw GROUP BY g \
                 HAVING SUM(v) > 15 ORDER BY g"
            )),
            [["1", "30", "1"], ["2", "35", "2"]]
        );
        // A window PARTITION BY over an aggregate.
        assert_eq!(
            row_text(session.run(
                "SELECT g, COUNT(*) c, RANK() OVER (PARTITION BY COUNT(*) ORDER BY g) r \
                 FROM gw GROUP BY g ORDER BY g"
            )),
            [["1", "2", "1"], ["2", "2", "2"], ["3", "1", "1"]]
        );
        // LAG and PERCENT_RANK over the grouped rows.
        assert_eq!(
            row_text(
                session
                    .run("SELECT g, LAG(SUM(v)) OVER (ORDER BY g) l FROM gw GROUP BY g ORDER BY g")
            ),
            [["1", "NULL"], ["2", "30"], ["3", "35"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT g, PERCENT_RANK() OVER (ORDER BY SUM(v)) p FROM gw GROUP BY g ORDER BY g"
            )),
            [["1", "0.5"], ["2", "1"], ["3", "0"]]
        );
        // A window over an implicit single-group aggregation (no GROUP BY).
        assert_eq!(
            row_text(session.run("SELECT MAX(v) m, RANK() OVER (ORDER BY MAX(v)) r FROM gw")),
            [["30", "1"]]
        );
        // The outer ORDER BY sorts the already-computed window value, through
        // its select alias.
        assert_eq!(
            row_text(session.run(
                "SELECT g, SUM(v) s, RANK() OVER (ORDER BY SUM(v)) r FROM gw GROUP BY g \
                 ORDER BY r DESC"
            )),
            [["2", "35", "3"], ["1", "30", "2"], ["3", "15", "1"]]
        );
        // A window over a GROUPED column needs no aggregate at all.
        assert_eq!(
            row_text(
                session.run("SELECT g, RANK() OVER (ORDER BY g) r FROM gw GROUP BY g ORDER BY g")
            ),
            [["1", "1"], ["2", "2"], ["3", "3"]]
        );
        // A window in HAVING is still Go's 3593, wherever the query groups.
        assert!(matches!(
            session.run("SELECT g FROM gw GROUP BY g HAVING RANK() OVER (ORDER BY g) = 1"),
            Err(DriverError::WindowInvalidWindowFuncUse(ref name)) if name == "rank"
        ));
    }

    /// The DEFAULT frame, which is the single biggest divergence trap in
    /// window functions: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`,
    /// whose `CURRENT ROW` is PEER-INCLUSIVE.
    ///
    /// Every expectation is captured TiDB output over the fixture.
    #[test]
    fn window_default_frame_includes_every_peer() {
        let mut session = window_session();

        // The tied 20s BOTH show 50 -- the running sum that already includes
        // both of them -- and neither shows 30. A row-by-row running total
        // would print 30 then 50, which is the classic wrong answer.
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, SUM(v) OVER (PARTITION BY g ORDER BY v) AS s FROM t ORDER BY g, v"
            )),
            [
                ["1", "10", "10"],
                ["1", "20", "50"],
                ["1", "20", "50"],
                ["1", "30", "80"],
                ["1", "40", "120"],
                ["2", "5", "10"],
                ["2", "5", "10"],
                ["2", "7", "17"],
            ]
        );

        // COUNT and AVG see the same peer-inclusive frame.
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, COUNT(v) OVER (PARTITION BY g ORDER BY v) AS c FROM t ORDER BY g, v"
            )),
            [
                ["1", "10", "1"],
                ["1", "20", "3"],
                ["1", "20", "3"],
                ["1", "30", "4"],
                ["1", "40", "5"],
                ["2", "5", "2"],
                ["2", "5", "2"],
                ["2", "7", "3"],
            ]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, AVG(v) OVER (PARTITION BY g ORDER BY v) AS a FROM t ORDER BY g, v"
            )),
            [
                ["1", "10", "10.0000"],
                ["1", "20", "16.6667"],
                ["1", "20", "16.6667"],
                ["1", "30", "20.0000"],
                ["1", "40", "24.0000"],
                ["2", "5", "5.0000"],
                ["2", "5", "5.0000"],
                ["2", "7", "5.6667"],
            ]
        );

        // Writing the default frame out by hand is the same frame.
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING \
                 AND CURRENT ROW) AS s FROM t ORDER BY g, v"
            )),
            [
                ["10"],
                ["50"],
                ["50"],
                ["80"],
                ["120"],
                ["10"],
                ["10"],
                ["17"]
            ]
        );
        // ... and its mirror image runs the peers the other way.
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v RANGE BETWEEN CURRENT ROW AND \
                 UNBOUNDED FOLLOWING) AS s FROM t ORDER BY g, v"
            )),
            [
                ["120"],
                ["110"],
                ["110"],
                ["70"],
                ["40"],
                ["17"],
                ["17"],
                ["7"]
            ]
        );

        // DESC only reverses the order the peers are walked in.
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v DESC) AS s FROM t \
                 ORDER BY g, v DESC"
            )),
            [
                ["40"],
                ["70"],
                ["110"],
                ["110"],
                ["120"],
                ["7"],
                ["17"],
                ["17"]
            ]
        );

        // With NO window ORDER BY every row is a peer, so the frame is the
        // whole partition and every row shows the partition total.
        assert_eq!(
            row_text(session.run(
                "SELECT g, SUM(v) OVER (PARTITION BY g) AS s, COUNT(*) OVER (PARTITION BY g) AS c \
                 FROM t ORDER BY g, v"
            )),
            [
                ["1", "120", "5"],
                ["1", "120", "5"],
                ["1", "120", "5"],
                ["1", "120", "5"],
                ["1", "120", "5"],
                ["2", "17", "3"],
                ["2", "17", "3"],
                ["2", "17", "3"],
            ]
        );
        // No PARTITION BY either: the whole result set is one frame.
        assert_eq!(
            row_text(session.run("SELECT SUM(v) OVER () AS s FROM t ORDER BY g, v"))[0],
            ["137"]
        );
    }

    /// Explicit `ROWS BETWEEN` frames, including the ones that EXCLUDE the
    /// current row and so leave some rows with an empty frame.
    #[test]
    fn window_rows_frames_and_the_empty_frame() {
        let mut session = window_session();

        // A sliding window: unlike the default RANGE frame, ROWS gives the
        // two tied 20s DIFFERENT sums (30 and 40), because it counts physical
        // positions rather than peers.
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 1 PRECEDING AND \
                 CURRENT ROW) AS s FROM t ORDER BY g, v"
            )),
            [
                ["10"],
                ["30"],
                ["40"],
                ["50"],
                ["70"],
                ["5"],
                ["10"],
                ["12"]
            ]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN CURRENT ROW AND \
                 1 FOLLOWING) AS s FROM t ORDER BY g, v"
            )),
            [
                ["30"],
                ["40"],
                ["50"],
                ["70"],
                ["40"],
                ["10"],
                ["12"],
                ["7"]
            ]
        );
        // The unbounded ends, which clamp rather than error at the edges.
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING \
                 AND UNBOUNDED FOLLOWING) AS s FROM t ORDER BY g, v"
            )),
            [
                ["120"],
                ["120"],
                ["120"],
                ["120"],
                ["120"],
                ["17"],
                ["17"],
                ["17"]
            ]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN CURRENT ROW AND \
                 UNBOUNDED FOLLOWING) AS s FROM t ORDER BY g, v"
            )),
            [
                ["120"],
                ["110"],
                ["90"],
                ["70"],
                ["40"],
                ["17"],
                ["12"],
                ["7"]
            ]
        );

        // A frame that EXCLUDES the current row. The first row of each
        // partition has an EMPTY frame, and an empty frame is NULL for SUM
        // but ZERO for COUNT -- captured, and the trap this test exists for.
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 2 PRECEDING AND \
                 1 PRECEDING) AS s FROM t ORDER BY g, v"
            )),
            [
                ["NULL"],
                ["10"],
                ["30"],
                ["40"],
                ["50"],
                ["NULL"],
                ["5"],
                ["10"],
            ]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT COUNT(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 2 PRECEDING AND \
                 1 PRECEDING) AS c FROM t ORDER BY g, v"
            )),
            [["0"], ["1"], ["2"], ["2"], ["2"], ["0"], ["1"], ["2"]]
        );

        // `2 FOLLOWING AND 1 FOLLOWING` is empty for EVERY row -- not a
        // static error, just an all-NULL column (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 2 FOLLOWING AND \
                 1 FOLLOWING) AS s FROM t ORDER BY g, v"
            )),
            [["NULL"]; 8]
        );
    }

    /// `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`, which DO read the frame -- so
    /// `LAST_VALUE` under the default frame famously returns the current PEER
    /// GROUP's last row, not the partition's.
    #[test]
    fn window_value_functions_read_the_frame() {
        let mut session = window_session();

        assert_eq!(
            row_text(session.run(
                "SELECT FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY v) AS f FROM t ORDER BY g, v"
            )),
            [["10"], ["10"], ["10"], ["10"], ["10"], ["5"], ["5"], ["5"]]
        );

        // The default frame ends at the current PEER GROUP, so LAST_VALUE is
        // the row's own peer-group maximum -- 40 appears only on the last row.
        assert_eq!(
            row_text(session.run(
                "SELECT LAST_VALUE(v) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
            )),
            [["10"], ["20"], ["20"], ["30"], ["40"], ["5"], ["5"], ["7"]]
        );
        // Spelling out the whole partition is what returns the partition's
        // last row on EVERY row.
        assert_eq!(
            row_text(session.run(
                "SELECT LAST_VALUE(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN UNBOUNDED \
                 PRECEDING AND UNBOUNDED FOLLOWING) AS l FROM t ORDER BY g, v"
            )),
            [["40"], ["40"], ["40"], ["40"], ["40"], ["7"], ["7"], ["7"]]
        );

        // NTH_VALUE is NULL while the frame holds fewer than n rows.
        assert_eq!(
            row_text(session.run(
                "SELECT NTH_VALUE(v, 3) OVER (PARTITION BY g ORDER BY v) AS n FROM t ORDER BY g, v"
            )),
            [
                ["NULL"],
                ["20"],
                ["20"],
                ["20"],
                ["20"],
                ["NULL"],
                ["NULL"],
                ["7"],
            ]
        );
        // Counted from the FRAME's start, not the partition's.
        assert_eq!(
            row_text(session.run(
                "SELECT NTH_VALUE(v, 2) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 1 PRECEDING \
                 AND CURRENT ROW) AS n FROM t ORDER BY g, v"
            )),
            [
                ["NULL"],
                ["20"],
                ["20"],
                ["30"],
                ["40"],
                ["NULL"],
                ["5"],
                ["7"],
            ]
        );
    }

    /// `LAG`/`LEAD`, which address the sorted partition directly and IGNORE
    /// the frame entirely.
    #[test]
    fn window_lag_and_lead() {
        let mut session = window_session();

        // The default offset is 1, and the partition's first row is NULL.
        assert_eq!(
            row_text(
                session.run(
                    "SELECT LAG(v) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
                )
            ),
            [
                ["NULL"],
                ["10"],
                ["20"],
                ["20"],
                ["30"],
                ["NULL"],
                ["5"],
                ["5"],
            ]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT LAG(v, 2) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
            )),
            [
                ["NULL"],
                ["NULL"],
                ["10"],
                ["20"],
                ["20"],
                ["NULL"],
                ["NULL"],
                ["5"],
            ]
        );
        // The third argument fills EVERY out-of-range position.
        assert_eq!(
            row_text(session.run(
                "SELECT LAG(v, 2, -1) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
            )),
            [
                ["-1"],
                ["-1"],
                ["10"],
                ["20"],
                ["20"],
                ["-1"],
                ["-1"],
                ["5"]
            ]
        );
        // Offset 0 is the current row.
        assert_eq!(
            row_text(session.run(
                "SELECT LAG(v, 0) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
            )),
            [["10"], ["20"], ["20"], ["30"], ["40"], ["5"], ["5"], ["7"]]
        );

        // LEAD runs off the partition's END instead.
        assert_eq!(
            row_text(
                session.run(
                    "SELECT LEAD(v) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
                )
            ),
            [
                ["20"],
                ["20"],
                ["30"],
                ["40"],
                ["NULL"],
                ["5"],
                ["7"],
                ["NULL"],
            ]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT LEAD(v, 2, -7) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
            )),
            [
                ["20"],
                ["30"],
                ["40"],
                ["-7"],
                ["-7"],
                ["7"],
                ["-7"],
                ["-7"]
            ]
        );

        // A frame is written but IGNORED: the result is identical to the
        // frame-less LAG above (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT LAG(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 2 PRECEDING AND \
                 1 PRECEDING) AS l FROM t ORDER BY g, v"
            )),
            [
                ["NULL"],
                ["10"],
                ["20"],
                ["20"],
                ["30"],
                ["NULL"],
                ["5"],
                ["5"],
            ]
        );
    }

    /// NULL inputs and string arguments across the framed families.
    ///
    /// A window `ORDER BY` sorts NULLs FIRST ascending, and all NULL keys are
    /// peers -- so the NULL row's own default frame holds only itself.
    #[test]
    fn window_frames_over_nulls_and_strings() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE u (g BIGINT, v BIGINT, s VARCHAR(20))")
            .unwrap();
        session
            .run("INSERT INTO u VALUES (1,10,'a'),(1,NULL,'b'),(1,20,NULL),(2,5,'x')")
            .unwrap();

        // The NULL row sorts first; SUM over its lone-NULL frame is NULL,
        // COUNT(v) is 0 while COUNT(*) is 1.
        assert_eq!(
            row_text(session.run(
                "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY v) AS s, \
                 COUNT(v) OVER (PARTITION BY g ORDER BY v) AS c, \
                 COUNT(*) OVER (PARTITION BY g ORDER BY v) AS ca \
                 FROM u ORDER BY g, v"
            )),
            [
                ["NULL", "NULL", "0", "1"],
                ["10", "10", "1", "2"],
                ["20", "30", "2", "3"],
                ["5", "5", "1", "1"],
            ]
        );

        // FIRST_VALUE reads the frame's first ROW, NULL included -- it does
        // not skip to the first non-NULL value.
        assert_eq!(
            row_text(session.run(
                "SELECT FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY v) AS f FROM u ORDER BY g, v"
            )),
            [["NULL"], ["NULL"], ["NULL"], ["5"]]
        );

        // MIN/MAX over strings SKIP NULLs, as in ordinary aggregation.
        assert_eq!(
            row_text(session.run(
                "SELECT MIN(s) OVER (PARTITION BY g) AS lo, MAX(s) OVER (PARTITION BY g) AS hi \
                 FROM u ORDER BY g, v"
            )),
            [["a", "b"], ["a", "b"], ["a", "b"], ["x", "x"]]
        );

        // A string LAG default lands on the partition's first row.
        assert_eq!(
            row_text(session.run(
                "SELECT LAG(s, 1, 'zz') OVER (PARTITION BY g ORDER BY v) AS l FROM u \
                 ORDER BY g, v"
            )),
            [["zz"], ["b"], ["a"], ["zz"]]
        );
    }

    /// The pipeline ABOVE the window stage -- an `ORDER BY`-only window,
    /// `DISTINCT`, and `LIMIT` -- checked against captured TiDB output.
    #[test]
    fn window_feeds_the_ordinary_pipeline() {
        let mut session = window_session();

        // The window is never projected, only sorted by: `v` descending
        // through its ROW_NUMBER, so the two `g = 2` rows with the smallest
        // `v` come last (captured).
        assert_eq!(
            row_text(session.run("SELECT g FROM t ORDER BY ROW_NUMBER() OVER (ORDER BY v) DESC")),
            [["1"], ["1"], ["1"], ["1"], ["1"], ["2"], ["2"], ["2"]]
        );

        // DISTINCT deduplicates the already-computed window column
        // (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT DISTINCT g, NTILE(2) OVER (PARTITION BY g ORDER BY v) \
                 FROM t ORDER BY 1, 2"
            )),
            [["1", "1"], ["1", "2"], ["2", "1"], ["2", "2"]]
        );

        // LIMIT applies after the outer ORDER BY over the window column
        // (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) rn \
                 FROM t ORDER BY rn DESC, g LIMIT 3"
            )),
            [["1", "40", "5"], ["1", "30", "4"], ["1", "20", "3"]]
        );
    }

    /// The fixture the `RANGE ... INTERVAL` captures ran over: a DATETIME
    /// key with a sub-day step, a TIE, a multi-day GAP, and a second
    /// partition.
    fn interval_session() -> Session {
        let mut session = Session::new();
        session
            .run("CREATE TABLE td (g BIGINT, k DATETIME, v BIGINT)")
            .unwrap();
        session
            .run(
                "INSERT INTO td VALUES \
                 (1,'2020-01-01 00:00:00',10),(1,'2020-01-01 12:00:00',20), \
                 (1,'2020-01-02 00:00:00',30),(1,'2020-01-02 00:00:00',40), \
                 (1,'2020-01-05 00:00:00',50),(2,'2020-01-01 00:00:00',60)",
            )
            .unwrap();
        session
    }

    /// `RANGE BETWEEN INTERVAL n unit PRECEDING/FOLLOWING` over a temporal
    /// `ORDER BY` key: the boundary is the current row's key moved by
    /// `DATE_ADD`/`DATE_SUB`'s own CALENDAR arithmetic, so `INTERVAL 1 MONTH`
    /// is a month field increment rather than a fixed number of days, and the
    /// boundary is INCLUSIVE.
    ///
    /// Every expectation is captured TiDB output.
    #[test]
    fn window_range_interval_bounds() {
        let mut session = interval_session();

        // Captured: the `2020-01-02` rows see the whole day back to
        // `2020-01-01 00:00:00` INCLUSIVE (10+20+30+40 = 100), the tie shares
        // one frame, and the `2020-01-05` row's window reaches nothing.
        for sql in [
            "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k \
             RANGE INTERVAL 1 DAY PRECEDING) FROM td WHERE g = 1 ORDER BY k, v",
            "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k \
             RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM td \
             WHERE g = 1 ORDER BY k, v",
        ] {
            assert_eq!(
                row_text(session.run(sql)),
                [
                    ["10", "10"],
                    ["20", "30"],
                    ["30", "100"],
                    ["40", "100"],
                    ["50", "50"]
                ],
                "for {sql}"
            );
        }

        // Captured: `CURRENT ROW AND INTERVAL 1 DAY FOLLOWING` looks forward
        // over the same inclusive boundary.
        assert_eq!(
            row_text(session.run(
                "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN CURRENT ROW AND INTERVAL 1 DAY FOLLOWING) FROM td \
                 WHERE g = 1 ORDER BY k, v"
            )),
            [
                ["10", "100"],
                ["20", "90"],
                ["30", "70"],
                ["40", "70"],
                ["50", "50"]
            ]
        );

        // Captured: a two-sided interval frame, and a 2 HOUR step that
        // reaches NOTHING but the peer group for the first two rows.
        assert_eq!(
            row_text(session.run(
                "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND INTERVAL 1 DAY FOLLOWING) \
                 FROM td WHERE g = 1 ORDER BY k, v"
            )),
            [
                ["10", "100"],
                ["20", "100"],
                ["30", "100"],
                ["40", "100"],
                ["50", "50"]
            ]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN INTERVAL 2 HOUR PRECEDING AND INTERVAL 2 HOUR FOLLOWING) \
                 FROM td WHERE g = 1 ORDER BY k, v"
            )),
            [
                ["10", "10"],
                ["20", "20"],
                ["30", "70"],
                ["40", "70"],
                ["50", "50"]
            ]
        );

        // Captured: under DESC the sign FLIPS, so `INTERVAL 1 DAY PRECEDING`
        // reaches the LATER timestamps that sort earlier.
        assert_eq!(
            row_text(session.run(
                "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k DESC \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM td \
                 WHERE g = 1 ORDER BY k DESC, v"
            )),
            [
                ["50", "50"],
                ["30", "70"],
                ["40", "70"],
                ["20", "90"],
                ["10", "100"]
            ]
        );

        // COUNT counts the same frame (captured `1,2,4,4,1`), and
        // FIRST_VALUE reads it.
        assert_eq!(
            row_text(session.run(
                "SELECT COUNT(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM td \
                 WHERE g = 1 ORDER BY k, v"
            )),
            [["1"], ["2"], ["4"], ["4"], ["1"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND INTERVAL 1 DAY FOLLOWING) \
                 FROM td WHERE g = 1 ORDER BY k, v"
            )),
            [["10"], ["10"], ["10"], ["10"], ["50"]]
        );

        // Captured: MONTH and YEAR reach every row of the whole table, and a
        // composite `INTERVAL '1 2' DAY_HOUR` (26 hours) reaches back far
        // enough for the `2020-01-02` rows but not for `2020-01-05`.
        for sql in [
            "SELECT v, SUM(v) OVER (ORDER BY k \
             RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) FROM td ORDER BY k, v",
            "SELECT v, SUM(v) OVER (ORDER BY k \
             RANGE BETWEEN INTERVAL 1 YEAR PRECEDING AND CURRENT ROW) FROM td ORDER BY k, v",
        ] {
            assert_eq!(
                row_text(session.run(sql)),
                [
                    ["10", "70"],
                    ["60", "70"],
                    ["20", "90"],
                    ["30", "160"],
                    ["40", "160"],
                    ["50", "210"]
                ],
                "for {sql}"
            );
        }
        assert_eq!(
            row_text(session.run(
                "SELECT v, SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL '1 2' DAY_HOUR PRECEDING AND CURRENT ROW) \
                 FROM td ORDER BY k, v"
            )),
            [
                ["10", "70"],
                ["60", "70"],
                ["20", "90"],
                ["30", "160"],
                ["40", "160"],
                ["50", "50"]
            ]
        );

        // An interval frame whose start ranks after its end is EMPTY for
        // every row (captured: all NULL).
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND INTERVAL 2 DAY PRECEDING) \
                 FROM td ORDER BY k, v"
            )),
            [["NULL"], ["NULL"], ["NULL"], ["NULL"], ["NULL"], ["NULL"]]
        );
    }

    /// The same interval frame over the OTHER temporal key types, and over
    /// NULL keys -- which peer with each other and with nothing else.
    #[test]
    fn window_range_interval_over_dates_and_nulls() {
        let mut session = Session::new();

        // Captured over `NULL,NULL,'2020-01-01','2020-01-02'` with values
        // `1,2,3,4`: the two NULL keys form a frame of their own (3 = 1+2),
        // in BOTH directions.
        session
            .run("CREATE TABLE tdn (k DATETIME, v BIGINT)")
            .unwrap();
        session
            .run(
                "INSERT INTO tdn VALUES \
                 (NULL,1),(NULL,2),('2020-01-01 00:00:00',3),('2020-01-02 00:00:00',4)",
            )
            .unwrap();
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM tdn"
            )),
            [["3"], ["3"], ["3"], ["7"]]
        );
        // Under DESC the `2020-01-01` row's frame reaches FORWARD to
        // `2020-01-02` (3+4 = 7); the NULL rows still see only each other.
        // Ordered by `v` because a window's own sort is not an output order.
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k DESC \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM tdn ORDER BY v"
            )),
            [["3"], ["3"], ["7"], ["4"]]
        );

        // A DATE key reads as midnight, so a 2 HOUR step reaches nothing
        // outside the peer group while a 1 DAY step reaches the previous day
        // (captured `1,6,6,4` and `1,5,5,4`).
        session
            .run("CREATE TABLE tdate (k DATE, v BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO tdate VALUES ('2020-01-01',1),('2020-01-02',2),('2020-01-02',3),('2020-01-10',4)")
            .unwrap();
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM tdate"
            )),
            [["1"], ["6"], ["6"], ["4"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL 2 HOUR PRECEDING AND CURRENT ROW) FROM tdate"
            )),
            [["1"], ["5"], ["5"], ["4"]]
        );
        // A month either side reaches every row (captured `10` throughout).
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND INTERVAL 1 MONTH FOLLOWING) \
                 FROM tdate"
            )),
            [["10"], ["10"], ["10"], ["10"]]
        );
    }

    /// A window call nested inside a LARGER select expression, which Go
    /// evaluates in the projection ABOVE the window operator -- over a plain
    /// query and over a grouped one alike.
    ///
    /// Every expectation is captured TiDB output over `(1,10),(1,20),(1,20),
    /// (2,30),(2,40)`.
    #[test]
    fn window_nested_in_larger_expression() {
        let mut session = Session::new();
        session.run("CREATE TABLE tw (g BIGINT, v BIGINT)").unwrap();
        session
            .run("INSERT INTO tw VALUES (1,10),(1,20),(1,20),(2,30),(2,40)")
            .unwrap();

        // Arithmetic around a ranking function, and a string function over
        // one.
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, RANK() OVER (PARTITION BY g ORDER BY v) + 1 FROM tw \
                 ORDER BY g, v"
            )),
            [
                ["1", "10", "2"],
                ["1", "20", "3"],
                ["1", "20", "3"],
                ["2", "30", "2"],
                ["2", "40", "3"]
            ]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT CONCAT('#', ROW_NUMBER() OVER (PARTITION BY g ORDER BY v)) FROM tw \
                 ORDER BY g, v"
            )),
            [["#1"], ["#2"], ["#3"], ["#1"], ["#2"]]
        );

        // TWO window calls in one expression, both over the same named
        // window (captured `3,6,7,3,6`).
        assert_eq!(
            row_text(session.run(
                "SELECT RANK() OVER w * 2 + ROW_NUMBER() OVER w FROM tw \
                 WINDOW w AS (PARTITION BY g ORDER BY v) ORDER BY g, v"
            )),
            [["3"], ["6"], ["7"], ["3"], ["6"]]
        );

        // A window value inside a control function, and one under unary
        // minus (captured `-1,-2,-3,-1,-2`).
        assert_eq!(
            row_text(session.run(
                "SELECT IF(ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) = 1, 'first', 'rest') \
                 FROM tw ORDER BY g, v"
            )),
            [["first"], ["rest"], ["rest"], ["first"], ["rest"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT -ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) FROM tw ORDER BY g, v"
            )),
            [["-1"], ["-2"], ["-3"], ["-1"], ["-2"]]
        );

        // Two window calls divided by each other (captured 16.6667 / 35.0000
        // -- the division carries div_precision_increment's scale).
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (PARTITION BY g) / COUNT(*) OVER (PARTITION BY g) \
                 FROM tw ORDER BY g, v"
            )),
            [
                ["16.6667"],
                ["16.6667"],
                ["16.6667"],
                ["35.0000"],
                ["35.0000"]
            ]
        );

        // Over a GROUPED query: the window computes over the aggregation's
        // output rows, and the larger expression over THAT.
        assert_eq!(
            row_text(session.run(
                "SELECT g, SUM(v), RANK() OVER (ORDER BY SUM(v)) + 100 FROM tw \
                 GROUP BY g ORDER BY g"
            )),
            [["1", "50", "101"], ["2", "70", "102"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT g, CONCAT('g', RANK() OVER (ORDER BY SUM(v) DESC)) FROM tw \
                 GROUP BY g ORDER BY g"
            )),
            [["1", "g2"], ["2", "g1"]]
        );
        // An aggregate OUTSIDE the window call, added to the window's value
        // (captured `51` / `72`).
        assert_eq!(
            row_text(session.run(
                "SELECT g, SUM(v) + ROW_NUMBER() OVER (ORDER BY g) FROM tw GROUP BY g ORDER BY g"
            )),
            [["1", "51"], ["2", "72"]]
        );

        // The outer ORDER BY sorts the ALIASED nested expression (captured
        // `3,3,3,2,2`).
        assert_eq!(
            row_text(session.run(
                "SELECT g, v, RANK() OVER (PARTITION BY g ORDER BY v) + 1 AS r FROM tw \
                 ORDER BY r DESC, g, v"
            )),
            [
                ["1", "20", "3"],
                ["1", "20", "3"],
                ["2", "40", "3"],
                ["1", "10", "2"],
                ["2", "30", "2"]
            ]
        );
    }

    /// A window function over `GROUP BY ... WITH ROLLUP`: the window sees the
    /// rollup OUTPUT rows, supergroup rows included, and their NULLed columns
    /// participate in `PARTITION BY`/`ORDER BY` like any other NULL.
    ///
    /// Every expectation is captured TiDB output over
    /// `(1,1,10),(1,2,20),(2,1,30),(2,2,40)`.
    #[test]
    fn window_over_rollup() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE tr (a BIGINT, b BIGINT, v BIGINT)")
            .unwrap();
        session
            .run("INSERT INTO tr VALUES (1,1,10),(1,2,20),(2,1,30),(2,2,40)")
            .unwrap();

        // Seven output rows -- four groups, two subtotals, one grand total --
        // numbered in the window's own ORDER BY. The outer ORDER BY is
        // written because a rollup's own row order is nondeterministic in Go.
        assert_eq!(
            row_text(session.run(
                "SELECT a, b, SUM(v), ROW_NUMBER() OVER (ORDER BY a, b) FROM tr \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
            )),
            [
                ["NULL", "NULL", "100", "1"],
                ["1", "NULL", "30", "2"],
                ["1", "1", "10", "3"],
                ["1", "2", "20", "4"],
                ["2", "NULL", "70", "5"],
                ["2", "1", "30", "6"],
                ["2", "2", "40", "7"]
            ]
        );

        // PARTITION BY a puts each subtotal row in ITS OWN group's partition
        // (its `b` is NULL, which sorts first), and the grand total alone in
        // the NULL partition.
        assert_eq!(
            row_text(session.run(
                "SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b) FROM tr \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
            )),
            [
                ["NULL", "NULL", "1"],
                ["1", "NULL", "1"],
                ["1", "1", "2"],
                ["1", "2", "3"],
                ["2", "NULL", "1"],
                ["2", "1", "2"],
                ["2", "2", "3"]
            ]
        );

        // An aggregate INSIDE the window call sums the rollup rows of the
        // partition -- the subtotal row included, which is why `a = 1` totals
        // 60 rather than 30 (captured).
        assert_eq!(
            row_text(session.run(
                "SELECT a, b, SUM(SUM(v)) OVER (PARTITION BY a) FROM tr \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
            )),
            [
                ["NULL", "NULL", "100"],
                ["1", "NULL", "60"],
                ["1", "1", "60"],
                ["1", "2", "60"],
                ["2", "NULL", "140"],
                ["2", "1", "140"],
                ["2", "2", "140"]
            ]
        );

        // GROUPING() tells a rollup NULL from a data NULL, and a window may
        // partition by it (captured: the grand total alone has grouping(a) =
        // 1, so it is row 1 of its own partition).
        assert_eq!(
            row_text(session.run(
                "SELECT a, b, GROUPING(a), \
                 ROW_NUMBER() OVER (PARTITION BY GROUPING(a) ORDER BY a, b) \
                 FROM tr GROUP BY a, b WITH ROLLUP ORDER BY a, b"
            )),
            [
                ["NULL", "NULL", "1", "1"],
                ["1", "NULL", "0", "1"],
                ["1", "1", "0", "2"],
                ["1", "2", "0", "3"],
                ["2", "NULL", "0", "4"],
                ["2", "1", "0", "5"],
                ["2", "2", "0", "6"]
            ]
        );

        // RANK over the rollup's SUMs: the `a = 1` subtotal (30) ties with
        // the `(2,1)` group (30), so both are rank 4 and the next jumps to 6.
        assert_eq!(
            row_text(session.run(
                "SELECT a, b, SUM(v), RANK() OVER (ORDER BY SUM(v) DESC) FROM tr \
                 GROUP BY a, b WITH ROLLUP ORDER BY SUM(v) DESC, a, b"
            )),
            [
                ["NULL", "NULL", "100", "1"],
                ["2", "NULL", "70", "2"],
                ["2", "2", "40", "3"],
                ["1", "NULL", "30", "4"],
                ["2", "1", "30", "4"],
                ["1", "2", "20", "6"],
                ["1", "1", "10", "7"]
            ]
        );
    }

    /// The bitwise and variance/stddev aggregates AS window functions, which
    /// Go allows over any frame.
    ///
    /// Every expectation is captured TiDB output over
    /// `(1,3),(1,5),(1,6),(2,1)`.
    #[test]
    fn window_bit_and_variance_aggregates() {
        let mut session = Session::new();
        session.run("CREATE TABLE ta (g BIGINT, v BIGINT)").unwrap();
        session
            .run("INSERT INTO ta VALUES (1,3),(1,5),(1,6),(2,1)")
            .unwrap();

        // The default frame is the running peer-inclusive one, so each row
        // folds every value up to and including itself.
        assert_eq!(
            row_text(
                session.run(
                    "SELECT BIT_AND(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v"
                )
            ),
            [["3"], ["1"], ["0"], ["1"]]
        );
        assert_eq!(
            row_text(
                session
                    .run("SELECT BIT_OR(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v")
            ),
            [["3"], ["7"], ["7"], ["1"]]
        );
        assert_eq!(
            row_text(
                session.run(
                    "SELECT BIT_XOR(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v"
                )
            ),
            [["3"], ["6"], ["0"], ["1"]]
        );

        // POPULATION forms divide by the frame's row count (a single row is
        // 0, not NULL); SAMPLE forms divide by count - 1 and are NULL for a
        // single row. `STDDEV`/`STD`/`VARIANCE` are the population forms.
        for name in ["VAR_POP", "VARIANCE"] {
            assert_eq!(
                row_text(session.run(&format!(
                    "SELECT {name}(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v"
                ))),
                [["0"], ["1"], ["1.5555555555555554"], ["0"]],
                "for {name}"
            );
        }
        for name in ["STDDEV_POP", "STDDEV", "STD"] {
            assert_eq!(
                row_text(session.run(&format!(
                    "SELECT {name}(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v"
                ))),
                [["0"], ["1"], ["1.247219128924647"], ["0"]],
                "for {name}"
            );
        }
        assert_eq!(
            row_text(
                session.run(
                    "SELECT VAR_SAMP(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v"
                )
            ),
            [["NULL"], ["2"], ["2.333333333333333"], ["NULL"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT STDDEV_SAMP(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v"
            )),
            [
                ["NULL"],
                ["1.4142135623730951"],
                ["1.5275252316519465"],
                ["NULL"]
            ]
        );

        // With no window ORDER BY the frame is the whole partition, and an
        // explicit ROWS frame narrows it the same way it does for SUM.
        assert_eq!(
            row_text(session.run("SELECT BIT_AND(v) OVER (PARTITION BY g) FROM ta ORDER BY g, v")),
            [["0"], ["0"], ["0"], ["1"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT STDDEV_POP(v) OVER (PARTITION BY g \
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM ta ORDER BY g, v"
            )),
            [["0"], ["1"], ["0.5"], ["0"]]
        );
        // An EMPTY frame folds to the bit operator's IDENTITY (0 for XOR)
        // but is NULL for the sample variance -- captured.
        assert_eq!(
            row_text(session.run(
                "SELECT BIT_XOR(v) OVER (PARTITION BY g \
                 ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) FROM ta ORDER BY g, v"
            )),
            [["6"], ["0"], ["0"], ["0"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT VAR_SAMP(v) OVER (PARTITION BY g \
                 ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM ta ORDER BY g, v"
            )),
            [["NULL"], ["NULL"], ["NULL"], ["NULL"]]
        );

        // An all-NULL frame: the variance family is NULL, BIT_AND folds to
        // its all-ones identity -- which the SIGNED result column prints as
        // `-1` (captured) -- and BIT_OR/BIT_XOR to 0.
        session.run("CREATE TABLE tn (g BIGINT, v BIGINT)").unwrap();
        session
            .run("INSERT INTO tn VALUES (1,NULL),(1,4),(1,NULL)")
            .unwrap();
        assert_eq!(
            row_text(
                session.run(
                    "SELECT VAR_POP(v) OVER (PARTITION BY g ORDER BY v) FROM tn ORDER BY g, v"
                )
            ),
            [["NULL"], ["NULL"], ["0"]]
        );
        assert_eq!(
            row_text(
                session.run(
                    "SELECT BIT_AND(v) OVER (PARTITION BY g ORDER BY v) FROM tn ORDER BY g, v"
                )
            ),
            [["-1"], ["-1"], ["4"]]
        );
        assert_eq!(
            row_text(
                session
                    .run("SELECT BIT_OR(v) OVER (PARTITION BY g ORDER BY v) FROM tn ORDER BY g, v")
            ),
            [["0"], ["0"], ["4"]]
        );
    }

    /// A named window that EXTENDS another, including a chain of three and a
    /// forward reference -- Go resolves the `WINDOW` clause as a graph, not
    /// in written order.
    ///
    /// Every expectation is captured TiDB output.
    #[test]
    fn window_base_window_references() {
        let mut session = Session::new();
        session.run("CREATE TABLE tw (g BIGINT, v BIGINT)").unwrap();
        session
            .run("INSERT INTO tw VALUES (1,10),(1,20),(1,20),(2,30),(2,40)")
            .unwrap();

        // `w2 AS (w ORDER BY v)` inherits w's PARTITION BY and adds the
        // order; a chain of three and a FORWARD reference resolve the same.
        for sql in [
            "SELECT ROW_NUMBER() OVER w2 FROM tw \
             WINDOW w AS (PARTITION BY g), w2 AS (w ORDER BY v) ORDER BY g, v",
            "SELECT ROW_NUMBER() OVER w2 FROM tw \
             WINDOW w2 AS (w ORDER BY v), w AS (PARTITION BY g) ORDER BY g, v",
            "SELECT ROW_NUMBER() OVER w3 FROM tw \
             WINDOW w AS (PARTITION BY g), w2 AS (w ORDER BY v), w3 AS (w2) ORDER BY g, v",
        ] {
            assert_eq!(
                row_text(session.run(sql)),
                [["1"], ["2"], ["3"], ["1"], ["2"]],
                "for {sql}"
            );
        }

        // A bare `w2 AS (w)` inherits everything, and an extension may add
        // its OWN frame over an inherited order.
        assert_eq!(
            row_text(session.run(
                "SELECT ROW_NUMBER() OVER w2 FROM tw \
                 WINDOW w AS (PARTITION BY g ORDER BY v), w2 AS (w) ORDER BY g, v"
            )),
            [["1"], ["2"], ["3"], ["1"], ["2"]]
        );
        assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER w2 FROM tw \
                 WINDOW w AS (PARTITION BY g ORDER BY v), \
                 w2 AS (w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ORDER BY g, v"
            )),
            [["10"], ["30"], ["50"], ["30"], ["70"]]
        );
    }

    /// Reading through a view: the plain form, a pushed-down predicate, an
    /// explicit column list, a view of a view, and a view joined to a table.
    /// Every result captured from upstream Go on a mock store.
    #[test]
    fn views_are_read_as_their_query() {
        let mut session = view_session();

        // Captured: header [a b], rows 1/10, 2/20, 3/30.
        let (names, rows) = query_text(&mut session, "SELECT * FROM v");
        assert_eq!(names, ["a", "b"]);
        assert_eq!(rows, [["1", "10"], ["2", "20"], ["3", "30"]]);

        // The outer WHERE filters the view's rows.
        let (_, rows) = query_text(&mut session, "SELECT * FROM v WHERE a > 1");
        assert_eq!(rows, [["2", "20"], ["3", "30"]]);
        let (_, rows) = query_text(&mut session, "SELECT a FROM v ORDER BY a DESC");
        assert_eq!(rows, [["3"], ["2"], ["1"]]);

        // The column list renames the body's output, so `a2` is the only name
        // that resolves.
        let (names, rows) = query_text(&mut session, "SELECT * FROM v2");
        assert_eq!(names, ["a2"]);
        assert_eq!(rows, [["1"], ["2"], ["3"]]);
        let (_, rows) = query_text(&mut session, "SELECT a2 FROM v2 WHERE a2 = 2");
        assert_eq!(rows, [["2"]]);

        // A view over a view.
        let (names, rows) = query_text(&mut session, "SELECT * FROM v3");
        assert_eq!(names, ["a", "b"]);
        assert_eq!(rows, [["2", "20"], ["3", "30"]]);

        // A view joined to a base table.
        let (names, rows) = query_text(&mut session, "SELECT v.a, s.c FROM v JOIN s ON v.a = s.a");
        assert_eq!(names, ["a", "c"]);
        assert_eq!(rows, [["1", "x"], ["2", "y"]]);
    }

    /// `SHOW CREATE VIEW` and `SHOW CREATE TABLE` over a view, asserted
    /// against the exact captured text.
    #[test]
    fn show_create_view_prints_the_stored_definition() {
        let mut session = view_session();

        let (names, rows) = query_text(&mut session, "SHOW CREATE VIEW v");
        assert_eq!(
            names,
            [
                "View",
                "Create View",
                "character_set_client",
                "collation_connection"
            ]
        );
        assert_eq!(
            rows,
            [[
                "v",
                "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v` \
                 (`a`, `b`) AS SELECT `a` AS `a`,`b` AS `b` FROM `test`.`t`",
                "utf8mb4",
                "utf8mb4_bin",
            ]]
        );

        // The explicit column list is what the header prints; the body keeps
        // the names it was written with.
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW v2");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v2` (`a2`) \
             AS SELECT `a` AS `a` FROM `test`.`t`"
        );

        // A view of a view stores its body's columns fully qualified.
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW v3");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v3` \
             (`a`, `b`) AS SELECT `test`.`v`.`a` AS `a`,`test`.`v`.`b` AS `b` \
             FROM `test`.`v` WHERE `b`>10"
        );

        // SHOW CREATE TABLE over a view prints the view form, header and all.
        let (table_names, table_rows) = query_text(&mut session, "SHOW CREATE TABLE v");
        let (view_names, view_rows) = query_text(&mut session, "SHOW CREATE VIEW v");
        assert_eq!(table_names, view_names);
        assert_eq!(table_rows, view_rows);

        // Captured: [executor:1347]'test.t' is not VIEW.
        assert!(matches!(
            session.run("SHOW CREATE VIEW t"),
            Err(DriverError::Schema(SchemaErrorKind::NotView(ref name))) if name == "test.t"
        ));

        // An aliased body keeps the alias, both in the FROM and in the
        // column references. Captured from Go.
        session
            .run("CREATE VIEW valias AS SELECT x.a FROM t AS x")
            .unwrap();
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW valias");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `valias` (`a`) \
             AS SELECT `x`.`a` AS `a` FROM `test`.`t` AS `x`"
        );

        // A FROM-less body, whose single column is named after its text.
        session.run("CREATE VIEW vlit AS SELECT 1").unwrap();
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vlit");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vlit` (`1`) \
             AS SELECT 1 AS `1`"
        );
        let (names, rows) = query_text(&mut session, "SELECT * FROM vlit");
        assert_eq!(names, ["1"]);
        assert_eq!(rows, [["1"]]);
    }

    /// Which statements may name a view, and which report the other kind.
    #[test]
    fn view_and_table_statements_do_not_cross() {
        let mut session = view_session();

        // Captured: [ddl:1347]'test.t' is not VIEW.
        assert!(matches!(
            session.run("DROP VIEW t"),
            Err(DriverError::Schema(SchemaErrorKind::NotView(ref name))) if name == "test.t"
        ));
        // The refusal really did not drop the table.
        assert_eq!(
            row_text(session.run("SELECT COUNT(*) FROM t")),
            vec![vec!["3".to_owned()]]
        );

        // Captured: [schema:1051]Unknown table 'test.v' -- DROP TABLE does not
        // see a view at all.
        assert!(matches!(
            session.run("DROP TABLE v"),
            Err(DriverError::Schema(SchemaErrorKind::BadTable(ref name))) if name == "test.v"
        ));
        assert_eq!(row_text(session.run("SELECT COUNT(*) FROM v")).len(), 1);

        // Captured: [schema:1050]Table 'test.v' already exists.
        assert!(matches!(
            session.run("CREATE VIEW v AS SELECT 1"),
            Err(DriverError::Schema(SchemaErrorKind::TableExists(ref name))) if name == "test.v"
        ));
        // OR REPLACE overwrites it instead.
        session
            .run("CREATE OR REPLACE VIEW v AS SELECT a AS a, b AS b FROM t")
            .unwrap();
        let (_, rows) = query_text(&mut session, "SELECT * FROM v");
        assert_eq!(rows, [["1", "10"], ["2", "20"], ["3", "30"]]);

        // Captured: [schema:1051]Unknown table 'test.nosuch', suppressed by
        // IF EXISTS.
        session.run("DROP VIEW IF EXISTS nosuch").unwrap();
        assert!(matches!(
            session.run("DROP VIEW nosuch"),
            Err(DriverError::Schema(SchemaErrorKind::BadTable(ref name))) if name == "test.nosuch"
        ));

        // Captured: [ddl:1353], the column list and the select list disagree.
        assert!(matches!(
            session.run("CREATE VIEW vbad(x, y) AS SELECT a FROM t"),
            Err(DriverError::ViewWrongList)
        ));

        // Captured: a view is hidden from its own replacement's body, so
        // `SELECT ... FROM v` inside `CREATE OR REPLACE VIEW v` is
        // [planner:1146]Table 'test.v' doesn't exist -- which is also why no
        // directly recursive view can be built.
        assert!(matches!(
            session.run("CREATE OR REPLACE VIEW v AS SELECT * FROM v"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownTable(ref name))) if name == "test.v"
        ));

        // A comma-separated DROP VIEW drops them all.
        session.run("DROP VIEW v, v2, v3").unwrap();
        let (_, rows) = query_text(&mut session, "SHOW TABLES");
        assert_eq!(rows, [["s"], ["t"]]);
    }

    /// Writes through a view, which this tier refuses with Go's own messages.
    #[test]
    fn writes_through_a_view_are_refused() {
        let mut session = view_session();

        // Captured: "insert into view v is not supported now" -- a plain Go
        // error, so it carries no error class.
        assert!(matches!(
            session.run("INSERT INTO v VALUES (1, 2)"),
            Err(DriverError::InsertIntoViewUnsupported(ref name)) if name == "v"
        ));
        // Captured: [planner:1288]The target table v of the UPDATE is not
        // updatable.
        assert!(matches!(
            session.run("UPDATE v SET a = 1"),
            Err(DriverError::TableNotUpdatable(ref name)) if name == "v"
        ));
        // Captured: "delete view v is not supported now".
        assert!(matches!(
            session.run("DELETE FROM v"),
            Err(DriverError::DeleteViewUnsupported(ref name)) if name == "v"
        ));
        // None of the refusals touched the base table.
        assert_eq!(
            row_text(session.run("SELECT COUNT(*) FROM t")),
            vec![vec!["3".to_owned()]]
        );
    }

    /// A view whose base table is dropped: the definition survives, reading
    /// it does not.
    #[test]
    fn a_view_over_a_dropped_table_is_invalid() {
        let mut session = Session::new();
        session.run("CREATE TABLE base (x BIGINT)").unwrap();
        session.run("CREATE VIEW vb AS SELECT x FROM base").unwrap();
        assert_eq!(
            row_text(session.run("SELECT * FROM vb")),
            Vec::<Vec<String>>::new()
        );

        session.run("DROP TABLE base").unwrap();
        // Captured: [planner:1356]View 'test.vb' references invalid table(s)
        // or column(s) or function(s) or definer/invoker of view lack rights
        // to use them.
        assert!(matches!(
            session.run("SELECT * FROM vb"),
            Err(DriverError::Schema(SchemaErrorKind::ViewInvalid(ref name))) if name == "test.vb"
        ));
        // SHOW CREATE VIEW still answers from the stored definition.
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vb");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vb` (`x`) \
             AS SELECT `x` AS `x` FROM `test`.`base`"
        );
    }

    /// Where a view shows up in the metadata statements.
    #[test]
    fn views_appear_in_the_metadata_statements() {
        let mut session = view_session();

        // SHOW TABLES lists views beside tables, in one sorted list.
        let (names, rows) = query_text(&mut session, "SHOW TABLES");
        assert_eq!(names, ["Tables_in_test"]);
        assert_eq!(rows, [["s"], ["t"], ["v"], ["v2"], ["v3"]]);

        // SHOW FULL TABLES adds the kind.
        let (names, rows) = query_text(&mut session, "SHOW FULL TABLES");
        assert_eq!(names, ["Tables_in_test", "Table_type"]);
        assert_eq!(
            rows,
            [
                ["s", "BASE TABLE"],
                ["t", "BASE TABLE"],
                ["v", "VIEW"],
                ["v2", "VIEW"],
                ["v3", "VIEW"],
            ]
        );

        // information_schema.tables reports the same kinds.
        let (_, rows) = query_text(
            &mut session,
            "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_schema = 'test' ORDER BY table_name",
        );
        assert_eq!(
            rows,
            [
                ["s", "BASE TABLE"],
                ["t", "BASE TABLE"],
                ["v", "VIEW"],
                ["v2", "VIEW"],
                ["v3", "VIEW"],
            ]
        );

        // information_schema.views: the captured header, and the stored
        // definition as VIEW_DEFINITION.
        let (names, rows) = query_text(
            &mut session,
            "SELECT * FROM information_schema.views WHERE table_schema = 'test'",
        );
        assert_eq!(
            names,
            [
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "VIEW_DEFINITION",
                "CHECK_OPTION",
                "IS_UPDATABLE",
                "DEFINER",
                "SECURITY_TYPE",
                "CHARACTER_SET_CLIENT",
                "COLLATION_CONNECTION",
            ]
        );
        assert_eq!(
            rows[0],
            [
                "def",
                "test",
                "v",
                "SELECT `a` AS `a`,`b` AS `b` FROM `test`.`t`",
                "CASCADED",
                "NO",
                "@",
                "DEFINER",
                "utf8mb4",
                "utf8mb4_bin",
            ]
        );
        assert_eq!(
            rows[2][3],
            "SELECT `test`.`v`.`a` AS `a`,`test`.`v`.`b` AS `b` FROM `test`.`v` WHERE `b`>10"
        );

        // DESCRIBE reports the view's own columns, with no key, default or
        // extra -- captured from Go, where a view's columns carry none.
        let (names, rows) = query_text(&mut session, "DESCRIBE v");
        assert_eq!(names, ["Field", "Type", "Null", "Key", "Default", "Extra"]);
        assert_eq!(
            rows,
            [
                ["a", "bigint(20)", "YES", "", "<nil>", ""],
                ["b", "bigint(20)", "YES", "", "<nil>", ""],
            ]
        );
    }

    /// A view body that is a set operation, asserted against the captured
    /// `SHOW CREATE VIEW` text and the rows the view reads.
    #[test]
    fn a_view_body_may_be_a_set_operation() {
        let mut session = view_session();

        session
            .run("CREATE VIEW vu AS SELECT a FROM t UNION SELECT a FROM s")
            .unwrap();
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vu");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vu` (`a`) \
             AS SELECT `a` AS `a` FROM `test`.`t` UNION SELECT `a` AS `a` FROM `test`.`s`"
        );
        let (names, rows) = query_text(&mut session, "SELECT * FROM vu ORDER BY a");
        assert_eq!(names, ["a"]);
        assert_eq!(rows, [["1"], ["2"], ["3"]]);

        // A statement-level ORDER BY belongs to the whole set operation and
        // is stored with it.
        session
            .run("CREATE VIEW vua AS SELECT a FROM t UNION ALL SELECT a FROM s ORDER BY 1")
            .unwrap();
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vua");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vua` (`a`) \
             AS SELECT `a` AS `a` FROM `test`.`t` UNION ALL SELECT `a` AS `a` FROM `test`.`s` \
             ORDER BY 1"
        );
        let (_, rows) = query_text(&mut session, "SELECT * FROM vua");
        assert_eq!(rows, [["1"], ["1"], ["2"], ["2"], ["3"]]);

        // A nested term keeps its parentheses, and a statement-level LIMIT
        // its place after the last term.
        session
            .run("CREATE VIEW vun AS SELECT a FROM t UNION (SELECT a FROM s UNION ALL SELECT a FROM s)")
            .unwrap();
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vun");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vun` (`a`) \
             AS SELECT `a` AS `a` FROM `test`.`t` UNION (SELECT `a` AS `a` FROM `test`.`s` \
             UNION ALL SELECT `a` AS `a` FROM `test`.`s`)"
        );
        session
            .run("CREATE VIEW vus AS SELECT a FROM t UNION SELECT a FROM s LIMIT 2")
            .unwrap();
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vus");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vus` (`a`) \
             AS SELECT `a` AS `a` FROM `test`.`t` UNION SELECT `a` AS `a` FROM `test`.`s` LIMIT 2"
        );

        // The explicit column list renames the set operation's output; the
        // body keeps the first term's own field names.
        session
            .run("CREATE VIEW vuc(z) AS SELECT a FROM t UNION SELECT a FROM s")
            .unwrap();
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vuc");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vuc` (`z`) \
             AS SELECT `a` AS `a` FROM `test`.`t` UNION SELECT `a` AS `a` FROM `test`.`s`"
        );
        let (names, _) = query_text(&mut session, "SELECT * FROM vuc");
        assert_eq!(names, ["z"]);

        // Captured: [planner:1222]The used SELECT statements have a different
        // number of columns.
        assert!(matches!(
            session.run("CREATE VIEW vubad AS SELECT a FROM t UNION SELECT a, c FROM s"),
            Err(DriverError::WrongNumberOfColumnsInSelect)
        ));
    }

    /// A view body containing a derived table, and the derived tables a plain
    /// `SELECT` may write -- the same code path either way.
    #[test]
    fn a_view_body_may_contain_a_derived_table() {
        let mut session = view_session();

        session
            .run("CREATE VIEW vd AS SELECT * FROM (SELECT a, b FROM t WHERE b > 10) x")
            .unwrap();
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vd");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vd` \
             (`a`, `b`) AS SELECT `x`.`a` AS `a`,`x`.`b` AS `b` FROM (SELECT `a` AS `a`,\
             `b` AS `b` FROM `test`.`t` WHERE `b`>10) AS `x`"
        );
        let (_, rows) = query_text(&mut session, "SELECT * FROM vd");
        assert_eq!(rows, [["2", "20"], ["3", "30"]]);

        // A derived table joined to a base table: the derived side is named
        // by its alias, the base side stays schema-qualified.
        session
            .run("CREATE VIEW vd2 AS SELECT x.a FROM (SELECT a FROM t) AS x JOIN s ON x.a = s.a")
            .unwrap();
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vd2");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vd2` (`a`) \
             AS SELECT `x`.`a` AS `a` FROM (SELECT `a` AS `a` FROM `test`.`t`) AS `x` \
             JOIN `test`.`s` ON `x`.`a`=`s`.`a`"
        );
        let (_, rows) = query_text(&mut session, "SELECT * FROM vd2");
        assert_eq!(rows, [["1"], ["2"]]);

        // Captured: [ddl:1248]Every derived table must have its own alias --
        // in a view body and in a plain SELECT alike.
        assert!(matches!(
            session.run("CREATE VIEW vnd AS SELECT * FROM (SELECT a FROM t)"),
            Err(DriverError::DerivedMustHaveAlias)
        ));
        assert!(matches!(
            session.run("SELECT * FROM (SELECT a FROM t)"),
            Err(DriverError::DerivedMustHaveAlias)
        ));

        // Captured: [planner:1060]Duplicate column name 'a' -- a derived
        // table is a named relation, so its columns must be unique.
        assert!(matches!(
            session.run("SELECT * FROM (SELECT * FROM t JOIN s ON t.a = s.a) q"),
            Err(DriverError::DuplicateColumnName(ref name)) if name == "a"
        ));

        // Plain derived tables: the alias is the only qualifier they answer
        // to, an expression field keeps its written name, and a set
        // operation may sit inside one.
        let (names, rows) = query_text(&mut session, "SELECT * FROM (SELECT a FROM t) x");
        assert_eq!(names, ["a"]);
        assert_eq!(rows, [["1"], ["2"], ["3"]]);
        let (_, rows) = query_text(
            &mut session,
            "SELECT x.a FROM (SELECT a, b FROM t) x WHERE x.b > 10",
        );
        assert_eq!(rows, [["2"], ["3"]]);
        let (_, rows) = query_text(&mut session, "SELECT * FROM (SELECT a + 1 FROM t) x");
        assert_eq!(rows, [["2"], ["3"], ["4"]]);
        let (_, rows) = query_text(
            &mut session,
            "SELECT * FROM (SELECT a FROM t UNION SELECT a FROM s) u ORDER BY a",
        );
        assert_eq!(rows, [["1"], ["2"], ["3"]]);
        // Captured: [planner:1054]Unknown column 't.a' in 'field list' -- the
        // subquery's own tables are not visible outside it.
        assert!(session.run("SELECT t.a FROM (SELECT a FROM t) x").is_err());
    }

    /// `WITH CHECK OPTION`: stored and reported, never printed, and never
    /// reached -- writes through a view are refused before it would apply.
    #[test]
    fn a_view_check_option_is_stored_and_reported() {
        let mut session = view_session();
        session
            .run("CREATE VIEW vc AS SELECT a, b FROM t WHERE b > 10 WITH CHECK OPTION")
            .unwrap();
        session
            .run("CREATE VIEW vcl AS SELECT a, b FROM t WHERE b > 10 WITH LOCAL CHECK OPTION")
            .unwrap();
        session
            .run("CREATE VIEW vcc AS SELECT a, b FROM t WHERE b > 10 WITH CASCADED CHECK OPTION")
            .unwrap();

        // Captured: SHOW CREATE VIEW prints no check option at all, whichever
        // form was written.
        for view in ["vc", "vcl", "vcc"] {
            let (_, rows) = query_text(&mut session, &format!("SHOW CREATE VIEW {view}"));
            assert_eq!(
                rows[0][1],
                format!(
                    "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `{view}` \
                     (`a`, `b`) AS SELECT `a` AS `a`,`b` AS `b` FROM `test`.`t` WHERE `b`>10"
                )
            );
        }

        // information_schema.views is where it surfaces: LOCAL when written,
        // CASCADED otherwise -- including for a view with no check option at
        // all, which Go still records as CASCADED.
        let (_, rows) = query_text(
            &mut session,
            "SELECT table_name, check_option, is_updatable FROM information_schema.views \
             WHERE table_schema = 'test' AND table_name IN ('v', 'vc', 'vcl', 'vcc') \
             ORDER BY table_name",
        );
        assert_eq!(
            rows,
            [
                ["v", "CASCADED", "NO"],
                ["vc", "CASCADED", "NO"],
                ["vcc", "CASCADED", "NO"],
                ["vcl", "LOCAL", "NO"],
            ]
        );

        // The check would apply to a write, and a write never gets that far.
        assert!(matches!(
            session.run("INSERT INTO vc VALUES (4, 5)"),
            Err(DriverError::InsertIntoViewUnsupported(ref name)) if name == "vc"
        ));
    }

    /// `information_schema.columns` and `SHOW TABLE STATUS` for a view.
    #[test]
    fn a_view_reports_its_columns_and_status() {
        let mut session = view_session();

        // Captured: a view's columns carry no default, no key and no extra,
        // are nullable, and report the same PRIVILEGES string a base table's
        // columns do.
        let (_, rows) = query_text(
            &mut session,
            "SELECT table_name, column_name, ordinal_position, column_default, is_nullable, \
             data_type, character_maximum_length, numeric_precision, column_type, column_key, \
             extra, privileges FROM information_schema.columns \
             WHERE table_schema = 'test' AND table_name = 'v' ORDER BY ordinal_position",
        );
        assert_eq!(
            rows,
            [
                [
                    "v",
                    "a",
                    "1",
                    "<nil>",
                    "YES",
                    "bigint",
                    "<nil>",
                    "19",
                    "bigint(20)",
                    "",
                    "",
                    "select,insert,update,references",
                ],
                [
                    "v",
                    "b",
                    "2",
                    "<nil>",
                    "YES",
                    "bigint",
                    "<nil>",
                    "19",
                    "bigint(20)",
                    "",
                    "",
                    "select,insert,update,references",
                ],
            ]
        );

        // Captured: SHOW TABLE STATUS answers a view with NULLs and the
        // literal VIEW as its comment; a base table's row keeps its storage
        // metadata.
        let (names, rows) = query_text(&mut session, "SHOW TABLE STATUS LIKE 'v'");
        assert_eq!(names[0], "Name");
        assert_eq!(names[names.len() - 1], "Comment");
        assert_eq!(
            rows,
            [[
                "v", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>",
                "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "", "<nil>", "VIEW",
            ]]
        );
        let (_, rows) = query_text(&mut session, "SHOW TABLE STATUS LIKE 't'");
        assert_eq!(rows[0][1], "InnoDB");
        assert_eq!(rows[0][rows[0].len() - 1], "");
    }

    /// A view's column types are its base tables' types *now*, not the ones
    /// they had at `CREATE VIEW`.
    #[test]
    fn a_view_column_type_follows_the_base_column() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE bt (x BIGINT, y VARCHAR(10))")
            .unwrap();
        session.run("INSERT INTO bt VALUES (1, 'aa')").unwrap();
        session
            .run("CREATE VIEW vb AS SELECT x, y FROM bt")
            .unwrap();

        let columns_query = "SELECT column_name, data_type, column_type \
                             FROM information_schema.columns \
                             WHERE table_schema = 'test' AND table_name = 'vb' \
                             ORDER BY ordinal_position";
        let (_, rows) = query_text(&mut session, "DESCRIBE vb");
        assert_eq!(rows[0][1], "bigint(20)");
        assert_eq!(rows[1][1], "varchar(10)");
        let (_, rows) = query_text(&mut session, columns_query);
        assert_eq!(
            rows,
            [
                ["x", "bigint", "bigint(20)"],
                ["y", "varchar", "varchar(10)"],
            ]
        );

        // Captured: altering the base columns shows through immediately, with
        // no touch to the view -- Go re-plans the body for every answer.
        session
            .run("ALTER TABLE bt MODIFY COLUMN y VARCHAR(64)")
            .unwrap();
        session
            .run("ALTER TABLE bt MODIFY COLUMN x VARCHAR(32)")
            .unwrap();
        let (_, rows) = query_text(&mut session, "DESCRIBE vb");
        assert_eq!(rows[0][1], "varchar(32)");
        assert_eq!(rows[1][1], "varchar(64)");
        let (_, rows) = query_text(&mut session, columns_query);
        assert_eq!(
            rows,
            [
                ["x", "varchar", "varchar(32)"],
                ["y", "varchar", "varchar(64)"],
            ]
        );
        let (_, rows) = query_text(&mut session, "SELECT * FROM vb");
        assert_eq!(rows, [["1", "aa"]]);
        // The stored definition never changed.
        let (_, rows) = query_text(&mut session, "SHOW CREATE VIEW vb");
        assert_eq!(
            rows[0][1],
            "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `vb` (`x`, `y`) \
             AS SELECT `x` AS `x`,`y` AS `y` FROM `test`.`bt`"
        );

        // Dropping a base column breaks the view: the read is ErrViewInvalid,
        // DESCRIBE fails with the body's own error, and the view drops out of
        // information_schema.columns entirely -- all captured.
        session.run("ALTER TABLE bt DROP COLUMN y").unwrap();
        assert!(matches!(
            session.run("SELECT * FROM vb"),
            Err(DriverError::Schema(SchemaErrorKind::ViewInvalid(ref name))) if name == "test.vb"
        ));
        assert!(session.run("DESCRIBE vb").is_err());
        let (_, rows) = query_text(&mut session, columns_query);
        assert_eq!(rows, Vec::<Vec<String>>::new());
        // information_schema.views still answers from the stored definition.
        let (_, rows) = query_text(
            &mut session,
            "SELECT view_definition FROM information_schema.views \
             WHERE table_schema = 'test' AND table_name = 'vb'",
        );
        assert_eq!(rows, [["SELECT `x` AS `x`,`y` AS `y` FROM `test`.`bt`"]]);
    }

    /// A session with a GLOBAL-scope privilege registry attached, over a
    /// fresh catalog. Root is bootstrapped with every privilege, matching
    /// what `PipelineSessionFactory` gives every connection.
    fn session_with_privileges() -> Session {
        let mut session = Session::new();
        session.attach_privileges(privilege::PrivilegeRegistry::default());
        session
    }

    /// CAPTURED end to end (`pkg/executor/grant.go`, `revoke.go`,
    /// `simple.go`, `show.go`): `CREATE USER` -> fresh `SHOW GRANTS` reports
    /// `USAGE` -> `GRANT` in scrambled order prints in Go's fixed
    /// `mysql.AllGlobalPrivs` order -> `REVOKE` removes exactly the one
    /// privilege -> `DROP USER` then a missing-user error, matching the Go
    /// source's `ErrCannotUser`/1396 wording exactly (`user@host`, unquoted).
    #[test]
    fn grant_revoke_and_show_grants_round_trip() {
        let mut session = session_with_privileges();

        session.run("CREATE USER 'u1'@'%'").unwrap();
        assert_eq!(
            row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
            [["GRANT USAGE ON *.* TO 'u1'@'%'"]]
        );

        session
            .run("GRANT SELECT, PROCESS, INSERT, SUPER, UPDATE ON *.* TO 'u1'@'%'")
            .unwrap();
        assert_eq!(
            row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
            [["GRANT SELECT,INSERT,UPDATE,PROCESS,SUPER ON *.* TO 'u1'@'%'"]]
        );

        session.run("REVOKE SUPER ON *.* FROM 'u1'@'%'").unwrap();
        assert_eq!(
            row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
            [["GRANT SELECT,INSERT,UPDATE,PROCESS ON *.* TO 'u1'@'%'"]]
        );

        session.run("DROP USER 'u1'@'%'").unwrap();
        match session.run("DROP USER 'nosuchuser'@'%'") {
            Err(DriverError::DropUserMissing { accounts }) => {
                assert_eq!(accounts, "nosuchuser@%");
            }
            other => panic!("expected DropUserMissing, got {other:?}"),
        }
    }

    /// CAPTURED: `SHOW GRANTS` with no `FOR` reports the current session's
    /// own account, and a fresh cluster's bootstrap `root`@`%` carries
    /// `ALL PRIVILEGES ... WITH GRANT OPTION`.
    #[test]
    fn show_grants_for_current_user_reports_root_bootstrap() {
        let mut session = session_with_privileges();
        session.set_user("root@%".to_owned(), "root@127.0.0.1".to_owned());
        assert_eq!(
            row_text(session.run("SHOW GRANTS")),
            [["GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION"]]
        );
    }

    /// CAPTURED: re-creating an existing account is `ErrCannotUser`/1396,
    /// quoted `'user'@'host'` (unlike `DROP USER`'s unquoted form).
    #[test]
    fn create_user_rejects_a_duplicate_account() {
        let mut session = session_with_privileges();
        session.run("CREATE USER 'dup1'@'%'").unwrap();
        match session.run("CREATE USER 'dup1'@'%'") {
            Err(DriverError::CreateUserAlreadyExists { user, host }) => {
                assert_eq!(user, "dup1");
                assert_eq!(host, "%");
            }
            other => panic!("expected CreateUserAlreadyExists, got {other:?}"),
        }
    }

    /// CAPTURED: `GRANT ... TO` an account that was never created is
    /// `ErrCantCreateUserWithGrant`/1410 -- TiDB's default sql_mode refuses
    /// to implicitly create the target.
    #[test]
    fn grant_to_an_unknown_user_is_refused() {
        let mut session = session_with_privileges();
        assert!(matches!(
            session.run("GRANT SELECT ON *.* TO 'nouser'@'%'"),
            Err(DriverError::GrantToUnknownUser)
        ));
    }

    /// CAPTURED: an unrecognized privilege name parses (through
    /// `tidb-parser`'s dynamic-privilege grammar branch) but is refused at
    /// execution with `ErrDynamicPrivilegeNotRegistered`/3929, naming the
    /// privilege.
    #[test]
    fn granting_an_unregistered_privilege_name_is_refused() {
        let mut session = session_with_privileges();
        session.run("CREATE USER 'dup1'@'%'").unwrap();
        match session.run("GRANT FOOBAR ON *.* TO 'dup1'@'%'") {
            Err(DriverError::DynamicPrivilegeNotRegistered(name)) => assert_eq!(name, "FOOBAR"),
            other => panic!("expected DynamicPrivilegeNotRegistered, got {other:?}"),
        }
    }

    /// CAPTURED: `REVOKE ... FROM` an account that does not exist is Go's
    /// plain `errors.Errorf("Unknown user: %s", user)`.
    #[test]
    fn revoke_from_an_unknown_user_is_refused() {
        let mut session = session_with_privileges();
        match session.run("REVOKE SELECT ON *.* FROM 'nouser'@'%'") {
            Err(DriverError::RevokeUnknownUser { user, host }) => {
                assert_eq!(user, "nouser");
                assert_eq!(host, "%");
            }
            other => panic!("expected RevokeUnknownUser, got {other:?}"),
        }
    }

    /// `ALL PRIVILEGES` grants every modeled global privilege, which folds
    /// `SHOW GRANTS` to the `ALL PRIVILEGES` literal (Go `userPrivToString`).
    #[test]
    fn grant_all_privileges_collapses_show_grants() {
        let mut session = session_with_privileges();
        session.run("CREATE USER 'dup1'@'%'").unwrap();
        session
            .run("GRANT ALL PRIVILEGES ON *.* TO 'dup1'@'%'")
            .unwrap();
        assert_eq!(
            row_text(session.run("SHOW GRANTS FOR 'dup1'@'%'")),
            [["GRANT ALL PRIVILEGES ON *.* TO 'dup1'@'%'"]]
        );
    }

    /// OUT OF SCOPE, refused rather than faked: `WITH GRANT OPTION`, column
    /// lists, and roles. (Database/table-level grants themselves are now
    /// modeled -- see the `db_scope_*`/`table_scope_*` tests below.)
    #[test]
    fn out_of_scope_grant_forms_are_refused() {
        let mut session = session_with_privileges();
        session.run("CREATE USER 'dup1'@'%'").unwrap();
        assert!(matches!(
            session.run("GRANT SELECT ON *.* TO 'dup1'@'%' WITH GRANT OPTION"),
            Err(DriverError::Unsupported(_))
        ));
        assert!(matches!(
            session.run("DROP ROLE 'r1'"),
            Err(DriverError::Unsupported(_))
        ));
    }

    /// CAPTURED end to end (`pkg/executor/grant.go`/`revoke.go`,
    /// `pkg/privilege/privileges/cache.go`'s `showGrants`): DB-scope
    /// `GRANT`/`REVOKE`/`SHOW GRANTS`, including the `ALL PRIVILEGES`
    /// literal and the lexical (not insertion, not plain-name) sort order
    /// across multiple databases.
    #[test]
    fn db_scope_grant_revoke_and_show_grants_round_trip() {
        let mut session = session_with_privileges();
        session.run("CREATE USER 'u1'@'%'").unwrap();
        session.run("CREATE DATABASE db1").unwrap();
        session.run("CREATE DATABASE aaadb").unwrap();

        session.run("GRANT SELECT ON db1.* TO 'u1'@'%'").unwrap();
        assert_eq!(
            row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
            [
                vec!["GRANT USAGE ON *.* TO 'u1'@'%'".to_owned()],
                vec!["GRANT SELECT ON `db1`.* TO 'u1'@'%'".to_owned()],
            ]
        );

        // A second DB, granted later, still sorts before `db1` (captured:
        // Go sorts DB-scope lines lexically by their formatted text).
        session.run("GRANT SELECT ON aaadb.* TO 'u1'@'%'").unwrap();
        assert_eq!(
            row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
            [
                vec!["GRANT USAGE ON *.* TO 'u1'@'%'".to_owned()],
                vec!["GRANT SELECT ON `aaadb`.* TO 'u1'@'%'".to_owned()],
                vec!["GRANT SELECT ON `db1`.* TO 'u1'@'%'".to_owned()],
            ]
        );

        // Once `db1`'s line becomes `GRANT ALL PRIVILEGES ...`, it sorts
        // *before* `aaadb`'s `GRANT SELECT ...` line: the sort key is the
        // whole formatted string, which starts with the privilege text, not
        // the database name ('A' < 'S').
        session.run("GRANT ALL ON db1.* TO 'u1'@'%'").unwrap();
        assert_eq!(
            row_text(session.run("SHOW GRANTS FOR 'u1'@'%'"))[1],
            vec!["GRANT ALL PRIVILEGES ON `db1`.* TO 'u1'@'%'".to_owned()]
        );

        session.run("REVOKE ALL ON db1.* FROM 'u1'@'%'").unwrap();
        session.run("REVOKE SELECT ON db1.* FROM 'u1'@'%'").unwrap();
        // Back to `GRANT USAGE ...`, which sorts after `aaadb`'s `SELECT`
        // line again ('U' > 'S').
        assert_eq!(
            row_text(session.run("SHOW GRANTS FOR 'u1'@'%'"))[2],
            vec!["GRANT USAGE ON `db1`.* TO 'u1'@'%'".to_owned()]
        );
    }

    /// CAPTURED: `GRANT PROCESS ON db.*` (a global-only privilege) is Go's
    /// `ErrWrongUsage`/1221, "Incorrect usage of DB GRANT and GLOBAL
    /// PRIVILEGES".
    #[test]
    fn db_scope_grant_rejects_global_only_privilege() {
        let mut session = session_with_privileges();
        session.run("CREATE USER 'u1'@'%'").unwrap();
        session.run("CREATE DATABASE db1").unwrap();
        assert!(matches!(
            session.run("GRANT PROCESS ON db1.* TO 'u1'@'%'"),
            Err(DriverError::DbGrantGlobalOnlyPriv)
        ));
    }

    /// CAPTURED: `REVOKE ... ON db.*` for an account with no `mysql.DB` row
    /// for that database at all is Go's plain "There is no such grant
    /// defined for user '%s' on host '%s' on database %s".
    #[test]
    fn db_scope_revoke_without_any_grant_row_is_refused() {
        let mut session = session_with_privileges();
        session.run("CREATE USER 'u1'@'%'").unwrap();
        session.run("CREATE DATABASE emptydb").unwrap();
        match session.run("REVOKE SELECT ON emptydb.* FROM 'u1'@'%'") {
            Err(DriverError::RevokeNoDbGrant {
                user,
                host,
                database,
            }) => {
                assert_eq!(user, "u1");
                assert_eq!(host, "%");
                assert_eq!(database, "emptydb");
            }
            other => panic!("expected RevokeNoDbGrant, got {other:?}"),
        }
    }

    /// CAPTURED end to end: TABLE-scope `GRANT`/`REVOKE`/`SHOW GRANTS`,
    /// including the `ALL PRIVILEGES` literal, backtick-quoted
    /// `` `db`.`table` `` (both segments escaped, same as Go's
    /// `stringutil.Escape`), and the invalid-scope-privilege / missing-table
    /// error split (Go checks privilege validity before table existence).
    #[test]
    fn table_scope_grant_revoke_and_show_grants_round_trip() {
        let mut session = session_with_privileges();
        session.run("CREATE USER 'u1'@'%'").unwrap();
        session.run("CREATE DATABASE db1").unwrap();
        session.run("CREATE TABLE db1.t1 (a INT)").unwrap();

        session
            .run("GRANT SELECT, INSERT ON db1.t1 TO 'u1'@'%'")
            .unwrap();
        assert_eq!(
            row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
            [
                vec!["GRANT USAGE ON *.* TO 'u1'@'%'".to_owned()],
                vec!["GRANT SELECT,INSERT ON `db1`.`t1` TO 'u1'@'%'".to_owned()],
            ]
        );

        session.run("GRANT ALL ON db1.t1 TO 'u1'@'%'").unwrap();
        assert_eq!(
            row_text(session.run("SHOW GRANTS FOR 'u1'@'%'"))[1],
            vec!["GRANT ALL PRIVILEGES ON `db1`.`t1` TO 'u1'@'%'".to_owned()]
        );

        session.run("REVOKE ALL ON db1.t1 FROM 'u1'@'%'").unwrap();
        assert_eq!(
            row_text(session.run("SHOW GRANTS FOR 'u1'@'%'"))[1],
            vec!["GRANT USAGE ON `db1`.`t1` TO 'u1'@'%'".to_owned()]
        );

        // Invalid-scope privilege: refused before the table-existence
        // check runs (captured `ErrIllegalGrantForTable`/1144).
        assert!(matches!(
            session.run("GRANT PROCESS ON db1.t1 TO 'u1'@'%'"),
            Err(DriverError::IllegalGrantForTable)
        ));

        // A valid privilege on a table that does not exist: refused with
        // `ErrTableNotExists`/1146 (captured), unless `CREATE` is among the
        // granted privileges (Go's issue #28533/#29268 exception).
        assert!(matches!(
            session.run("GRANT SELECT ON db1.nosuchtable TO 'u1'@'%'"),
            Err(DriverError::Schema(SchemaErrorKind::UnknownTable(ref name)))
                if name == "db1.nosuchtable"
        ));
        session
            .run("GRANT CREATE ON db1.nosuchtable TO 'u1'@'%'")
            .unwrap();

        // REVOKE for an account with no `mysql.Tables_priv` row at all.
        session.run("CREATE TABLE db1.t2 (a INT)").unwrap();
        match session.run("REVOKE SELECT ON db1.t2 FROM 'u1'@'%'") {
            Err(DriverError::RevokeNoTableGrant {
                user,
                host,
                database,
                table,
            }) => {
                assert_eq!(user, "u1");
                assert_eq!(host, "%");
                assert_eq!(database, "db1");
                assert_eq!(table, "t2");
            }
            other => panic!("expected RevokeNoTableGrant, got {other:?}"),
        }
    }

    /// Go `planbuilder.go`'s `*ast.KillStmt` case: a session may always KILL
    /// its OWN connection, but killing a peer logged in as a DIFFERENT user
    /// is refused with `ErrSpecificAccessDenied` (1227) unless the caller
    /// holds SUPER. Granting SUPER then lets the same KILL through.
    #[test]
    fn kill_of_another_users_connection_requires_super() {
        let registry = process::ProcessRegistry::default();
        let mut victim = session_with_privileges();
        victim.set_user("root@%".to_owned(), "root@10.0.0.1".to_owned());
        let victim_guard = registry.register(
            1,
            "root".to_owned(),
            "10.0.0.1:1".to_owned(),
            "test".to_owned(),
            None,
        );
        victim.attach_process(1, victim_guard);

        let mut bob = session_with_privileges();
        bob.set_user("bob@%".to_owned(), "bob@10.0.0.2".to_owned());
        let bob_guard = registry.register(
            2,
            "bob".to_owned(),
            "10.0.0.2:2".to_owned(),
            "test".to_owned(),
            None,
        );
        bob.attach_process(2, bob_guard);
        bob.run("CREATE USER 'bob'@'%'").unwrap();

        // Killing one's own connection never needs a privilege.
        assert_eq!(
            bob.run("kill 2").unwrap(),
            StmtResult::Affected(0),
            "KILL of one's own connection is always allowed"
        );

        // Killing root's connection without SUPER is refused.
        match bob.run("kill 1") {
            Err(DriverError::KillAccessDenied) => {}
            other => panic!("expected KillAccessDenied, got {other:?}"),
        }

        // Granting SUPER lets the same KILL through.
        bob.run("GRANT SUPER ON *.* TO 'bob'@'%'").unwrap();
        assert_eq!(bob.run("kill 1").unwrap(), StmtResult::Affected(0));
    }

    /// `PROCESS` granted through `GRANT` (not the test-only
    /// [`Session::set_process_privilege`] override) gates `SHOW PROCESSLIST`
    /// visibility exactly the same way, wiring the registry all the way to
    /// the process-list filter.
    #[test]
    fn grant_process_gates_processlist_visibility() {
        let registry = process::ProcessRegistry::default();
        let mut session = session_with_privileges();
        session.set_user("bob@%".to_owned(), "bob@10.0.0.1".to_owned());
        let guard = registry.register(
            1,
            "bob".to_owned(),
            "10.0.0.1:1".to_owned(),
            "test".to_owned(),
            None,
        );
        session.attach_process(1, guard);
        let _alice = registry.register(
            2,
            "alice".to_owned(),
            "10.0.0.2:2".to_owned(),
            "test".to_owned(),
            None,
        );

        session.run("CREATE USER 'bob'@'%'").unwrap();
        assert_eq!(row_text(session.run("show processlist")).len(), 1);

        session.run("GRANT PROCESS ON *.* TO 'bob'@'%'").unwrap();
        assert_eq!(row_text(session.run("show processlist")).len(), 2);
    }

    /// The JSON family's first slice: JSON evaluated as VALUES.
    ///
    /// Every expectation below is a `testkit.CreateMockStore` capture of real
    /// TiDB on the same statements. Two facts are worth naming because they
    /// are easy to assume wrong:
    ///
    ///  * object keys print in PLAIN BYTE order, not length-then-bytes
    ///    (`buildBinaryJSONObject`'s `cmp.Compare`), so `{"b":1,"aa":2}`
    ///    prints `aa` first;
    ///  * a duplicate `JSON_OBJECT` key keeps the LAST value.
    ///
    /// DOCUMENTED DIVERGENCE: TiDB reports a JSON-returning column as type
    /// `JSON` (245); this tier has no BinaryJSON value, so the column is a
    /// string carrying `BinaryJSON.MarshalJSON`'s exact text. The VALUES here
    /// are byte-identical to TiDB's -- only the reported column type differs,
    /// the same trade the temporal casts make (see `tidb_expr::rewriter`).
    #[test]
    fn json_value_functions() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE t (a BIGINT PRIMARY KEY, j VARCHAR(200))")
            .unwrap();
        session
            .run(r#"INSERT INTO t VALUES (1, '{"b":1,"aa":2,"c":{"d q":"v"}}')"#)
            .unwrap();

        macro_rules! check {
            ($sql:expr, $want:expr) => {
                assert_eq!(
                    row_text(session.run($sql)),
                    vec![vec![($want).to_owned()]],
                    "{}",
                    $sql
                );
            };
        }

        // JSON_EXTRACT: one path returns the element, several wrap in an
        // array, an unmatched path is NULL, and a wildcard always wraps.
        check!(r#"SELECT JSON_EXTRACT('{"a":1,"b":"x"}', '$.a')"#, "1");
        check!(r#"SELECT JSON_EXTRACT('{"a":1,"b":"x"}', '$.b')"#, r#""x""#);
        check!(
            r#"SELECT JSON_EXTRACT('{"a":1,"b":"x"}', '$.a', '$.b')"#,
            r#"[1, "x"]"#
        );
        check!(r#"SELECT JSON_EXTRACT('{"a":1}', '$.zzz')"#, "NULL");
        check!("SELECT JSON_EXTRACT('[1,2,3]', '$[*]')", "[1, 2, 3]");
        check!("SELECT JSON_EXTRACT('[1,2,3]', '$[1]')", "2");
        check!(r#"SELECT JSON_EXTRACT('{"a":1,"b":2}', '$.*')"#, "[1, 2]");
        // A scalar auto-wraps for `$[0]`, and `**` walks recursively.
        check!("SELECT JSON_EXTRACT('3', '$[0]')", "3");
        check!(
            r#"SELECT JSON_EXTRACT('{"a":{"b":[1,2]}}', '$**.b')"#,
            "[[1, 2]]"
        );
        check!(r#"SELECT JSON_EXTRACT('{"a b":1}', '$."a b"')"#, "1");
        check!("SELECT JSON_EXTRACT(NULL, '$.a')", "NULL");
        check!(r#"SELECT JSON_EXTRACT('{"a":1}', NULL)"#, "NULL");

        // `->` is JSON_EXTRACT and `->>` wraps it in JSON_UNQUOTE, so `->>`
        // differs ONLY when the extracted value is a JSON string.
        check!("SELECT j->'$.b' FROM t", "1");
        check!("SELECT j->>'$.b' FROM t", "1");
        check!("SELECT j->'$.c' FROM t", r#"{"d q": "v"}"#);
        check!("SELECT j->>'$.c' FROM t", r#"{"d q": "v"}"#);
        check!(r#"SELECT j->'$.c."d q"' FROM t"#, r#""v""#);
        check!(r#"SELECT j->>'$.c."d q"' FROM t"#, "v");
        check!("SELECT j->'$.zz' FROM t", "NULL");
        check!("SELECT j->>'$.zz' FROM t", "NULL");
        check!(
            "SELECT j->'$' FROM t",
            r#"{"aa": 2, "b": 1, "c": {"d q": "v"}}"#
        );

        // JSON_TYPE names the BinaryJSON kind, which is why `1.0` is DOUBLE
        // and a value past int64 is UNSIGNED INTEGER.
        for (document, want) in [
            ("1", "INTEGER"),
            ("1.0", "DOUBLE"),
            ("-1", "INTEGER"),
            ("1e3", "DOUBLE"),
            (r#""s""#, "STRING"),
            ("true", "BOOLEAN"),
            ("false", "BOOLEAN"),
            ("null", "NULL"),
            ("{}", "OBJECT"),
            ("[]", "ARRAY"),
            ("18446744073709551615", "UNSIGNED INTEGER"),
            (r#"  {"a":1}  "#, "OBJECT"),
        ] {
            let sql = format!("SELECT JSON_TYPE('{document}')");
            assert_eq!(
                row_text(session.run(&sql)),
                vec![vec![want.to_owned()]],
                "{sql}"
            );
        }
        check!("SELECT JSON_TYPE(NULL)", "NULL");

        // JSON_OBJECT / JSON_ARRAY. The duplicate key keeps the LAST value
        // and the printed key order is plain byte order.
        check!("SELECT JSON_OBJECT('k',1,'k',2)", r#"{"k": 2}"#);
        check!("SELECT JSON_OBJECT('k',1,'k',2,'k',3)", r#"{"k": 3}"#);
        check!(
            "SELECT JSON_OBJECT('b',1,'aa',2,'c',3)",
            r#"{"aa": 2, "b": 1, "c": 3}"#
        );
        check!("SELECT JSON_OBJECT('k',NULL)", r#"{"k": null}"#);
        check!("SELECT JSON_OBJECT()", "{}");
        check!("SELECT JSON_OBJECT(1,1)", r#"{"1": 1}"#);
        check!(
            "SELECT JSON_ARRAY(1,'x',NULL,1.5)",
            r#"[1, "x", null, 1.5]"#
        );
        check!("SELECT JSON_ARRAY()", "[]");

        // JSON_QUOTE / JSON_UNQUOTE. Only a fully double-quoted document is
        // unquoted; anything else comes back unchanged.
        check!(r#"SELECT JSON_QUOTE('a"b')"#, r#""a\"b""#);
        check!("SELECT JSON_QUOTE('中')", r#""中""#);
        check!("SELECT JSON_QUOTE(NULL)", "NULL");
        check!(r#"SELECT JSON_UNQUOTE('"a\\"b"')"#, r#"a"b"#);
        check!(r#"SELECT JSON_UNQUOTE('"\\u4e2d"')"#, "中");
        check!(r#"SELECT JSON_UNQUOTE('"a\\/b"')"#, "a/b");
        check!("SELECT JSON_UNQUOTE('abc')", "abc");
        check!("SELECT JSON_UNQUOTE('[1,2]')", "[1,2]");
        check!(r#"SELECT JSON_UNQUOTE('"x')"#, r#""x"#);
        check!(r#"SELECT JSON_UNQUOTE(JSON_QUOTE('a"b'))"#, r#"a"b"#);
        check!("SELECT JSON_UNQUOTE(NULL)", "NULL");

        // JSON_CONTAINS: containment, not equality, plus the optional path.
        check!("SELECT JSON_CONTAINS('[1,2,3]','2')", "1");
        check!("SELECT JSON_CONTAINS('[1,2,3]','[1,3]')", "1");
        check!(r#"SELECT JSON_CONTAINS('{"a":1,"b":2}','{"a":1}')"#, "1");
        check!(r#"SELECT JSON_CONTAINS('{"a":{"b":1}}','1','$.a.b')"#, "1");
        check!(r#"SELECT JSON_CONTAINS('{"a":[1,2]}','2','$.a')"#, "1");
        check!("SELECT JSON_CONTAINS('[[1,2]]','[1]')", "1");
        check!("SELECT JSON_CONTAINS('1','1')", "1");
        check!("SELECT JSON_CONTAINS('[1]','2')", "0");
        check!("SELECT JSON_CONTAINS(NULL,'1')", "NULL");
        check!(r#"SELECT JSON_CONTAINS('{"a":1}','1','$.zz')"#, "NULL");

        // JSON_LENGTH / JSON_KEYS / JSON_DEPTH. Every scalar has length one
        // and depth one; JSON_KEYS is NULL for a non-object.
        check!(r#"SELECT JSON_LENGTH('{"a":1,"b":2}')"#, "2");
        check!("SELECT JSON_LENGTH('[1,2,3]')", "3");
        check!("SELECT JSON_LENGTH('1')", "1");
        check!("SELECT JSON_LENGTH('null')", "1");
        check!("SELECT JSON_LENGTH(NULL)", "NULL");
        check!(r#"SELECT JSON_LENGTH('{"a":{"b":1,"c":2}}','$.a')"#, "2");
        check!(r#"SELECT JSON_LENGTH('{"a":1}','$.zz')"#, "NULL");
        check!(
            r#"SELECT JSON_KEYS('{"z":1,"B":2,"a":3,"A":4,"_":5,"0":6}')"#,
            r#"["0", "A", "B", "_", "a", "z"]"#
        );
        check!(
            r#"SELECT JSON_KEYS('{"bb":1,"a":2,"ccc":3,"dd":4}')"#,
            r#"["a", "bb", "ccc", "dd"]"#
        );
        check!("SELECT JSON_KEYS('{}')", "[]");
        check!("SELECT JSON_KEYS('[1,2]')", "NULL");
        check!("SELECT JSON_KEYS('1')", "NULL");
        check!(
            r#"SELECT JSON_KEYS('{"a":{"z":1,"y":2}}','$.a')"#,
            r#"["y", "z"]"#
        );
        check!("SELECT JSON_DEPTH('1')", "1");
        check!("SELECT JSON_DEPTH('[]')", "1");
        check!("SELECT JSON_DEPTH('{}')", "1");
        check!("SELECT JSON_DEPTH('[1,[2,[3]]]')", "4");
        check!(r#"SELECT JSON_DEPTH('{"a":{"b":{"c":1}}}')"#, "4");
        check!("SELECT JSON_DEPTH(NULL)", "NULL");

        // JSON_VALID never raises: a malformed document, and every non-string
        // SQL value, is simply zero.
        check!("SELECT JSON_VALID('{}')", "1");
        check!("SELECT JSON_VALID('{')", "0");
        check!("SELECT JSON_VALID('abc')", "0");
        check!("SELECT JSON_VALID(' ')", "0");
        check!("SELECT JSON_VALID(1)", "0");
        check!("SELECT JSON_VALID(NULL)", "NULL");

        // CAST(x AS JSON): only the STRING signature parses, so `'abc'` is
        // error 3140 rather than the JSON string "abc" (asserted below).
        check!(
            r#"SELECT CAST('{"b":1,"aa":2,"c":3,"a":4}' AS JSON)"#,
            r#"{"a": 4, "aa": 2, "b": 1, "c": 3}"#
        );
        check!("SELECT CAST(1 AS JSON)", "1");
        check!("SELECT CAST(1.5 AS JSON)", "1.5");
        check!("SELECT CAST(NULL AS JSON)", "NULL");
        // `marshalFloat64To`'s cutoffs: at least one fractional digit inside
        // [1e-15, 1e15), a bare exponent outside it.
        check!(
            "SELECT CAST('[1.0, 1.5, 1e3, 100000000000000000000, -0.0]' AS JSON)",
            "[1.0, 1.5, 1000.0, 1e20, -0.0]"
        );
        check!(
            "SELECT CAST('[0.1,2.5e-10,1e100,3,-3,1.7976931348623157e308]' AS JSON)",
            "[0.1, 0.00000000025, 1e100, 3, -3, 1.7976931348623157e308]"
        );

        // The `json` error class reaches the wire with TiDB's own code.
        let mut code = |sql: &str| match session.run(sql) {
            Err(error) => error.to_mysql_error().code,
            Ok(output) => panic!("expected an error from {sql}, got {output:?}"),
        };
        assert_eq!(code("SELECT JSON_EXTRACT('x','$.a')"), 3140);
        assert_eq!(code("SELECT CAST('abc' AS JSON)"), 3140);
        assert_eq!(code("SELECT JSON_LENGTH('nope')"), 3140);
        assert_eq!(code(r#"SELECT JSON_EXTRACT('{"a":1}','xx')"#), 3143);
        assert_eq!(code(r#"SELECT JSON_EXTRACT('{"a":1}','$.')"#), 3143);
        assert_eq!(code("SELECT JSON_CONTAINS('[1,2]','1','$[*]')"), 3149);
        assert_eq!(code("SELECT JSON_TYPE(1)"), 3146);
        assert_eq!(code("SELECT JSON_QUOTE(1)"), 3064);
        assert_eq!(code("SELECT JSON_OBJECT(NULL,1)"), 3158);
        assert_eq!(
            session
                .run(r#"SELECT JSON_EXTRACT('{"a":1}','xx')"#)
                .unwrap_err()
                .to_mysql_error()
                .message,
            "Invalid JSON path expression. The error is around character position 1."
        );

        // REFUSED because UPSTREAM GO DOES NOT PARSE IT: `JSON_TABLE` has no
        // grammar rule, AST node, or executor anywhere in `pkg/`, so there is
        // no source to transcreate. Evidence and the captured Go parse error
        // live in `json_table_is_unsupported_upstream` below. The mutation
        // family graduated -- see `json_mutation_functions` and
        // `json_column_type` below.
        assert!(
            session
                .run(r#"SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS (v INT PATH '$')) t"#)
                .is_err(),
            "JSON_TABLE should still be refused"
        );
    }

    /// The JSON MUTATION family, captured from real TiDB through
    /// `testkit.CreateMockStore`.
    ///
    /// The rule that is easiest to get wrong -- and that many cases below
    /// exist to pin -- is that a mutation's path/value pairs are applied
    /// SEQUENTIALLY to the document the previous pair produced, not all
    /// against the original. `JSON_REMOVE('[1,2,3]','$[0]','$[0]')` therefore
    /// removes two DIFFERENT elements and leaves `[3]`.
    ///
    /// DOCUMENTED DIVERGENCE, unchanged from slice 1: a JSON-returning
    /// BUILTIN reports column type `VarString` where TiDB says `JSON`,
    /// because this tier's expression datum domain is textual. The VALUES are
    /// byte-identical. A JSON COLUMN is a different story -- see
    /// `json_column_type`.
    #[test]
    fn json_mutation_functions() {
        let mut session = Session::new();
        macro_rules! check {
            ($sql:expr, $want:expr) => {
                assert_eq!(
                    row_text(session.run($sql)),
                    vec![vec![($want).to_owned()]],
                    "{}",
                    $sql
                );
            };
        }

        // JSON_SET replaces an existing path and creates a missing one;
        // JSON_INSERT only creates; JSON_REPLACE only replaces.
        check!(r#"SELECT JSON_SET('{"a":1}','$.a',2)"#, r#"{"a": 2}"#);
        check!(
            r#"SELECT JSON_SET('{"a":1}','$.b',2)"#,
            r#"{"a": 1, "b": 2}"#
        );
        check!(r#"SELECT JSON_INSERT('{"a":1}','$.a',2)"#, r#"{"a": 1}"#);
        check!(
            r#"SELECT JSON_INSERT('{"a":1}','$.b',2)"#,
            r#"{"a": 1, "b": 2}"#
        );
        check!(r#"SELECT JSON_REPLACE('{"a":1}','$.a',2)"#, r#"{"a": 2}"#);
        check!(r#"SELECT JSON_REPLACE('{"a":1}','$.b',2)"#, r#"{"a": 1}"#);
        // `$` alone replaces the whole document.
        check!(r#"SELECT JSON_SET('{"a":1}','$',2)"#, "2");
        // A VALUE argument does NOT carry ParseToJSONFlag, so an SQL string
        // becomes a JSON STRING rather than a parsed document.
        check!(
            r#"SELECT JSON_SET('{}','$.a','{"x":1}')"#,
            r#"{"a": "{\"x\":1}"}"#
        );
        // NAMED BOUNDARY, and the reason the value rule above matters: a
        // JSON-typed value argument keeps its STRUCTURE in TiDB
        // (`JSON_SET('{}','$.a',CAST('{"x":1}' AS JSON))` is
        // `{"a": {"x": 1}}`), but this tier's CAST produces canonical TEXT,
        // which is indistinguishable from a string literal here and so
        // nests as a JSON string. A JSON COLUMN carries a real BinaryJSON
        // and does keep its structure -- see `json_column_type`.
        // DOCUMENTED DIVERGENCE (the `builtin_ext::json` module doc's typed
        // boolean boundary): TiDB reads `TRUE` through the argument's
        // `IsBooleanFlag` and stores the JSON boolean `true`. This tier's
        // value domain has no boolean datum, so `TRUE` arrives as the
        // integer 1 -- the same value a JSON COLUMN stores for `TRUE` in
        // TiDB itself (`json_column_type` captures that).
        check!(r#"SELECT JSON_SET('{"a":1}','$.a',TRUE)"#, r#"{"a": 1}"#);
        check!(r#"SELECT JSON_SET('{"a":1}','$.a',1.5)"#, r#"{"a": 1.5}"#);
        // An out-of-range array index appends rather than padding.
        check!("SELECT JSON_SET('[1,2,3]','$[5]',9)", "[1, 2, 3, 9]");
        // A scalar document indexes as a one-element array.
        check!("SELECT JSON_SET('1','$.a',2)", "1");
        check!("SELECT JSON_SET('1','$[0]',2)", "2");
        // A missing INTERMEDIATE leg is a no-op: only the LAST leg is
        // created, never a whole missing branch.
        check!(
            r#"SELECT JSON_SET('{"a":{"b":1}}','$.a.c.d',1)"#,
            r#"{"a": {"b": 1}}"#
        );

        // SEQUENTIAL evaluation: the second pair sees the first pair's
        // document. `$.b` does not exist for the first pair, so `$.b.c` is
        // reachable only because the first pair created `$.b` -- and when it
        // created a SCALAR there, `$.b.c` finds no object and does nothing.
        check!(
            r#"SELECT JSON_SET('{"a":1}','$.b',2,'$.b.c',3)"#,
            r#"{"a": 1, "b": 2}"#
        );
        check!("SELECT JSON_SET('[1,2]','$[0]',9,'$[0][0]',8)", "[8, 2]");

        // JSON_REMOVE, whose paths are also sequential: two identical `$[0]`
        // paths remove the FIRST and then the SECOND original element.
        check!("SELECT JSON_REMOVE('[1,2,3]','$[0]')", "[2, 3]");
        check!("SELECT JSON_REMOVE('[1,2,3]','$[0]','$[0]')", "[3]");
        check!("SELECT JSON_REMOVE('[1,2,3]','$[0]','$[1]')", "[2]");
        check!(r#"SELECT JSON_REMOVE('{"a":1,"b":2}','$.a','$.b')"#, "{}");
        check!(r#"SELECT JSON_REMOVE('{"a":1}','$.zz')"#, r#"{"a": 1}"#);
        check!("SELECT JSON_REMOVE('[1,2,3]','$[9]')", "[1, 2, 3]");

        // JSON_ARRAY_APPEND wraps a non-array target in an array first;
        // JSON_ARRAY_INSERT needs an existing array CELL.
        check!("SELECT JSON_ARRAY_APPEND('[1,2]','$',3)", "[1, 2, 3]");
        check!(
            r#"SELECT JSON_ARRAY_APPEND('{"a":[1]}','$.a',2)"#,
            r#"{"a": [1, 2]}"#
        );
        check!(
            r#"SELECT JSON_ARRAY_APPEND('{"a":1}','$.a',2)"#,
            r#"{"a": [1, 2]}"#
        );
        check!("SELECT JSON_ARRAY_APPEND('1','$',2)", "[1, 2]");
        check!(
            "SELECT JSON_ARRAY_APPEND('[[1],[2]]','$[0]',9)",
            "[[1, 9], [2]]"
        );
        check!(
            r#"SELECT JSON_ARRAY_APPEND('{"a":1}','$.zz',2)"#,
            r#"{"a": 1}"#
        );
        // Sequential again: `$` appended 3 first, and `$[0]` then wrapped
        // the ORIGINAL first element.
        check!(
            "SELECT JSON_ARRAY_APPEND('[1,2]','$',3,'$[0]',4)",
            "[[1, 4], 2, 3]"
        );
        check!(
            "SELECT JSON_ARRAY_INSERT('[1,2,3]','$[1]',9)",
            "[1, 9, 2, 3]"
        );
        check!(
            "SELECT JSON_ARRAY_INSERT('[1,2,3]','$[0]',9,'$[0]',8)",
            "[8, 9, 1, 2, 3]"
        );
        check!(
            "SELECT JSON_ARRAY_INSERT('[1,2,3]','$[9]',9)",
            "[1, 2, 3, 9]"
        );
        check!(
            "SELECT JSON_ARRAY_INSERT('[[1,2]]','$[0][1]',9)",
            "[[1, 9, 2]]"
        );

        // MERGE_PATCH deletes a key whose patch value is JSON null;
        // MERGE_PRESERVE wraps two values for the same key in an array.
        check!(
            r#"SELECT JSON_MERGE_PATCH('{"a":1,"b":2}','{"a":null}')"#,
            r#"{"b": 2}"#
        );
        check!(
            r#"SELECT JSON_MERGE_PATCH('{"a":1}','{"b":2}')"#,
            r#"{"a": 1, "b": 2}"#
        );
        check!("SELECT JSON_MERGE_PATCH('[1,2]','[3]')", "[3]");
        check!("SELECT JSON_MERGE_PRESERVE('[1,2]','[3]')", "[1, 2, 3]");
        check!(
            r#"SELECT JSON_MERGE_PRESERVE('{"a":1}','{"a":2}')"#,
            r#"{"a": [1, 2]}"#
        );
        // A MERGE argument IS parsed (unlike a mutation VALUE argument).
        check!("SELECT JSON_MERGE_PRESERVE('1','2')", "[1, 2]");
        check!(
            r#"SELECT JSON_MERGE('{"a":1}','{"b":2}')"#,
            r#"{"a": 1, "b": 2}"#
        );

        // NULL propagation, which differs PER ARGUMENT ROLE:
        //  * a NULL DOCUMENT or a NULL PATH makes the whole call NULL;
        //  * a NULL VALUE is the JSON null scalar and is stored;
        //  * JSON_MERGE* is NULL as soon as ANY argument is NULL.
        check!("SELECT JSON_SET(NULL,'$.a',1)", "NULL");
        check!(r#"SELECT JSON_SET('{"a":1}',NULL,1)"#, "NULL");
        check!(r#"SELECT JSON_SET('{"a":1}','$.a',NULL)"#, r#"{"a": null}"#);
        check!(
            r#"SELECT JSON_INSERT('{"a":1}','$.b',NULL)"#,
            r#"{"a": 1, "b": null}"#
        );
        check!(
            r#"SELECT JSON_REPLACE('{"a":1}','$.a',NULL)"#,
            r#"{"a": null}"#
        );
        check!("SELECT JSON_REMOVE(NULL,'$.a')", "NULL");
        check!(r#"SELECT JSON_REMOVE('{"a":1}',NULL)"#, "NULL");
        check!("SELECT JSON_ARRAY_APPEND(NULL,'$',1)", "NULL");
        check!("SELECT JSON_ARRAY_APPEND('[1]',NULL,1)", "NULL");
        check!("SELECT JSON_ARRAY_APPEND('[1]','$',NULL)", "[1, null]");
        check!("SELECT JSON_ARRAY_INSERT('[1]','$[0]',NULL)", "[null, 1]");
        check!("SELECT JSON_ARRAY_INSERT('[1]',NULL,1)", "NULL");
        check!("SELECT JSON_MERGE(NULL,'[1]')", "NULL");
        check!(r#"SELECT JSON_MERGE_PATCH('{"a":1}',NULL)"#, "NULL");
        check!(r#"SELECT JSON_MERGE_PATCH(NULL,'{"a":1}')"#, "NULL");
        check!(
            r#"SELECT JSON_MERGE_PATCH('{"a":1}',NULL,'{"b":2}')"#,
            "NULL"
        );
        check!(r#"SELECT JSON_MERGE_PRESERVE('{"a":1}',NULL)"#, "NULL");

        // The `json` error class, with TiDB's own codes.
        let mut code = |sql: &str| match session.run(sql) {
            Err(error) => error.to_mysql_error().code,
            Ok(output) => panic!("expected an error from {sql}, got {output:?}"),
        };
        assert_eq!(code(r#"SELECT JSON_SET('{"a":1}','xx',1)"#), 3143);
        assert_eq!(code(r#"SELECT JSON_SET('{"a":1}','$[*]',1)"#), 3149);
        assert_eq!(code(r#"SELECT JSON_SET('{"a":1}','$.*',1)"#), 3149);
        assert_eq!(code(r#"SELECT JSON_SET('{"a":1}','$**.a',1)"#), 3149);
        assert_eq!(code("SELECT JSON_REMOVE('[1]','$[*]')"), 3149);
        assert_eq!(code("SELECT JSON_ARRAY_APPEND('[1]','$[*]',1)"), 3149);
        // `$` is vacuous for REMOVE and not an array cell for ARRAY_INSERT.
        assert_eq!(code(r#"SELECT JSON_REMOVE('{"a":1}','$')"#), 3153);
        assert_eq!(code("SELECT JSON_ARRAY_INSERT('[1]','$',1)"), 3165);
        assert_eq!(code(r#"SELECT JSON_ARRAY_INSERT('{"a":1}','$.a',2)"#), 3165);
        assert_eq!(code(r#"SELECT JSON_SET('nope','$.a',1)"#), 3140);
        assert_eq!(code(r#"SELECT JSON_MERGE_PATCH('nope','{}')"#), 3140);
        // A MERGE argument must be a JSON string or a JSON value.
        assert_eq!(code("SELECT JSON_MERGE_PRESERVE('[1]',3)"), 3146);
        assert_eq!(code(r#"SELECT JSON_MERGE_PATCH('{"a":1}',3)"#), 3146);

        // JSON_MERGE is deprecated: it computes the same value as
        // JSON_MERGE_PRESERVE and adds warning 1681.
        assert_eq!(
            row_text(session.run("SELECT JSON_MERGE('[1]','[2]')")),
            vec![vec!["[1, 2]".to_owned()]]
        );
        assert_eq!(
            row_text(session.run("SHOW WARNINGS")),
            vec![vec![
                "Warning".to_owned(),
                "1681".to_owned(),
                "JSON_MERGE is deprecated and will be removed in a future release.".to_owned(),
            ]]
        );
        // A NULL argument returns before Go appends the warning.
        assert_eq!(
            row_text(session.run("SELECT JSON_MERGE(NULL,'[1]')")),
            vec![vec!["NULL".to_owned()]]
        );
        assert!(row_text(session.run("SHOW WARNINGS")).is_empty());
    }

    /// The JSON COLUMN TYPE, captured from real TiDB.
    ///
    /// NOT a divergence, unlike the JSON-returning BUILTINS above: a JSON
    /// column stores a real `BinaryJSON` in its row and its chunk cell, so
    /// the wire reports type `JSON` (245) with the binary charset exactly as
    /// TiDB does. The write path is Go `table.CastValue`, which PARSES and
    /// CANONICALIZES the written text -- which is why `{"b":2,"a":1}` reads
    /// back key-sorted and spaced.
    #[test]
    fn json_column_type() {
        let mut session = Session::new();
        session
            .run("CREATE TABLE tj (id BIGINT PRIMARY KEY, j JSON)")
            .unwrap();
        for sql in [
            r#"INSERT INTO tj VALUES (1,'{"b":2,"a":1}')"#,
            "INSERT INTO tj VALUES (2,'[1,2,3]')",
            "INSERT INTO tj VALUES (3,NULL)",
            "INSERT INTO tj VALUES (4,'null')",
            r#"INSERT INTO tj VALUES (5,'"str"')"#,
            // A non-string SQL value becomes its own JSON scalar; TRUE is
            // the INTEGER 1, not the JSON boolean.
            "INSERT INTO tj VALUES (6, 7)",
            "INSERT INTO tj VALUES (7, TRUE)",
            "INSERT INTO tj VALUES (8, 1.5)",
        ] {
            session.run(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        }

        assert_eq!(
            row_text(session.run("SELECT id, j FROM tj ORDER BY id")),
            vec![
                vec!["1".to_owned(), r#"{"a": 1, "b": 2}"#.to_owned()],
                vec!["2".to_owned(), "[1, 2, 3]".to_owned()],
                vec!["3".to_owned(), "NULL".to_owned()],
                vec!["4".to_owned(), "null".to_owned()],
                vec!["5".to_owned(), r#""str""#.to_owned()],
                vec!["6".to_owned(), "7".to_owned()],
                vec!["7".to_owned(), "1".to_owned()],
                vec!["8".to_owned(), "1.5".to_owned()],
            ]
        );
        assert_eq!(
            row_text(session.run("SELECT JSON_TYPE(j) FROM tj ORDER BY id")),
            ["OBJECT", "ARRAY", "NULL", "NULL", "STRING", "INTEGER", "INTEGER", "DOUBLE",]
                .map(|t| vec![t.to_owned()])
                .to_vec()
        );
        // A JSON column feeds the JSON builtins as a document.
        assert_eq!(
            row_text(session.run(r#"SELECT JSON_SET(j,'$.c',3) FROM tj WHERE id=1"#)),
            vec![vec![r#"{"a": 1, "b": 2, "c": 3}"#.to_owned()]]
        );
        assert_eq!(
            row_text(session.run(r#"SELECT JSON_EXTRACT(j,'$.a') FROM tj WHERE id=1"#)),
            vec![vec!["1".to_owned()]]
        );
        // A column VALUE argument keeps its structure, because it really is
        // a JSON value rather than the canonical text a CAST produces here.
        assert_eq!(
            row_text(session.run(r#"SELECT JSON_SET('{}','$.a',j) FROM tj WHERE id=1"#)),
            vec![vec![r#"{"a": {"a": 1, "b": 2}}"#.to_owned()]]
        );
        assert_eq!(
            row_text(session.run(r#"SELECT id FROM tj WHERE JSON_EXTRACT(j,'$.a') = 1"#)),
            vec![vec!["1".to_owned()]]
        );
        assert_eq!(
            row_text(session.run("SELECT id FROM tj WHERE j IS NULL")),
            vec![vec!["3".to_owned()]]
        );

        // The wire type: `JSON` (245), binary charset, like TiDB.
        let StmtOutput::Rows { columns, .. } = session
            .run_with_columns("SELECT j FROM tj WHERE id=1")
            .unwrap()
        else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::Json);
        assert_eq!(columns[0].1.charset_name(), "binary");

        // A malformed document is the PARSER's own 3140, not the generic
        // 1366 every other bad column value reports -- and it stays an
        // error, because there is no truncated document to store.
        macro_rules! failure {
            ($sql:expr) => {
                match session.run($sql) {
                    Err(error) => error.to_mysql_error(),
                    Ok(output) => panic!("expected an error from {}, got {output:?}", $sql),
                }
            };
        }
        assert_eq!(failure!("INSERT INTO tj VALUES (9,'nope')").code, 3140);
        assert_eq!(
            failure!("INSERT INTO tj VALUES (10,'')").message,
            "Invalid JSON text: The document is empty"
        );
        // A JSON column can be neither indexed nor given a default.
        assert_eq!(failure!("CREATE TABLE tj3 (j JSON, KEY(j))").code, 3152);
        assert_eq!(failure!("CREATE TABLE tj4 (j JSON PRIMARY KEY)").code, 3152);
        assert_eq!(
            failure!(r#"CREATE TABLE tj9 (j JSON DEFAULT '{}')"#).code,
            1101
        );
        // DEFAULT NULL is the one default a JSON column may carry.
        session
            .run("CREATE TABLE tj2 (j JSON DEFAULT NULL, k JSON NOT NULL)")
            .unwrap();

        // UPDATE writes a mutated document back through the same cast.
        session
            .run(r#"UPDATE tj SET j = JSON_SET(j,'$.z',1) WHERE id=1"#)
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT j FROM tj WHERE id=1")),
            vec![vec![r#"{"a": 1, "b": 2, "z": 1}"#.to_owned()]]
        );

        // SHOW reports the declared type.
        assert_eq!(
            row_text(session.run("SHOW COLUMNS FROM tj"))[1][..2],
            ["j".to_owned(), "json".to_owned()]
        );
        assert!(
            row_text(session.run("SHOW CREATE TABLE tj"))[0][1].contains("`j` json DEFAULT NULL")
        );
    }
}
