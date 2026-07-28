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
/// The one refusal every ROLE statement reports. Roles parse (Go's grammar
/// is transcreated) but nothing executes them: a role is an account that can
/// be GRANTed to other accounts, and resolving what a user may do then
/// depends on which of its roles are ACTIVE in the session -- a role graph
/// and per-session active-role set that no part of this tier models. Faking
/// any half of it would make privilege checks answer wrongly, so the
/// statements are refused whole.
const ROLES_UNSUPPORTED: &str =
    "roles (CREATE/DROP ROLE, GRANT/REVOKE <role>, SET ROLE) are not supported yet";

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
                SessionStmt::SetRole(_) | SessionStmt::SetDefaultRole(_) => {
                    Err(DriverError::Unsupported(ROLES_UNSUPPORTED))
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
                tidb_ast::DdlStmt::AlterUser(alter) => Ok(Some(self.alter_user_stmt(alter)?)),
                tidb_ast::DdlStmt::RenameUser { pairs } => Ok(Some(self.rename_user_stmt(pairs)?)),
                // ROLES parse but are refused by name rather than through the
                // generic DDL fallback, so the message says which feature is
                // missing. Go supports them fully (captured: `CREATE ROLE r1`
                // succeeds, `GRANT r1 TO 'u1'@'%'` adds a
                // `GRANT 'r1'@'%' TO 'u1'@'%'` line to `SHOW GRANTS` between
                // the table-scope and dynamic lines, and a role's own
                // privileges reach a user only through its ACTIVE roles);
                // modelling that needs a role graph and active-role state,
                // which is its own unit.
                tidb_ast::DdlStmt::CreateRole { .. }
                | tidb_ast::DdlStmt::DropUser { is_role: true, .. } => {
                    Err(DriverError::Unsupported(ROLES_UNSUPPORTED))
                }
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
                tidb_ast::AdminStmt::GrantRole(_) | tidb_ast::AdminStmt::RevokeRole(_) => {
                    Err(DriverError::Unsupported(ROLES_UNSUPPORTED))
                }
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
                        // privilege; killing anyone else's requires the
                        // DYNAMIC `CONNECTION_ADMIN`, reported as
                        // `ErrSpecificAccessDenied.GenWithStackByArgs("SUPER
                        // or CONNECTION_ADMIN")` (1227) -- NOT the unused
                        // 1095 `ErrKillDenied` errno entry, which no code
                        // path in current Go ever raises. SUPER still passes
                        // because it is the fallback for every dynamic
                        // privilege, which is exactly why Go's message names
                        // both.
                        //
                        // Go additionally requires `RESTRICTED_CONNECTION_ADMIN`
                        // to kill a connection owned by a
                        // `RESTRICTED_USER_ADMIN` user, but only under SEM
                        // (`appendVisitInfoIsRestrictedUser` returns early
                        // when `sem.IsEnabled()` is false); with no SEM in
                        // this tier that branch is unreachable, so it is
                        // deliberately absent rather than half-modelled.
                        let is_self = self.connection_id == Some(target);
                        if !is_self {
                            let owner = guard
                                .registry()
                                .snapshot()
                                .into_iter()
                                .find(|row| row.id == target)
                                .map(|row| row.user);
                            // Go compares the process's USERNAME against the
                            // logged-in username, ignoring host.
                            let same_user =
                                owner.as_deref() == Some(self.process_list_user().as_str());
                            let may_kill = self.privileges.as_ref().is_some_and(|registry| {
                                self.current_identity().is_some_and(|(user, host)| {
                                    registry.has_dynamic_priv(user, host, "CONNECTION_ADMIN", false)
                                })
                            });
                            if owner.is_some() && !same_user && !may_kill {
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
        } else if table_name.eq_ignore_ascii_case("USER_PRIVILEGES") {
            self.user_privileges_table_rows()
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
    /// because there is no persisted global tier yet; resource groups and
    /// the other non-variable `SET` forms stay unsupported.
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
            // `SET PASSWORD` shares the `SET` keyword and the front end's
            // OK-packet reply, but writes `mysql.user`, not a variable.
            SessionStmt::SetPassword(set_password) => {
                self.set_password_stmt(set_password)?;
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

    /// `CREATE USER [IF NOT EXISTS] <account> [IDENTIFIED BY '<password>']`.
    /// Go `simple.go`'s `executeCreateUser`, minus resource limits and
    /// account annotations, which this tier has no storage for and therefore
    /// refuses rather than silently drops.
    ///
    /// `IDENTIFIED BY` stores the account's
    /// `mysql.user.authentication_string` (see
    /// [`privilege::encode_password`]), which is the same row the wire front
    /// end verifies a login against -- so an account created here can
    /// immediately log in with that password.
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
            let auth_string = Self::resolve_auth_string(spec.auth.as_ref())?;
            if spec.dual_password.is_some() {
                return Err(DriverError::Unsupported(
                    "CREATE USER ... RETAIN CURRENT PASSWORD is not supported yet",
                ));
            }
            let user = spec.user.user.as_str();
            let host = spec.user.host.as_str();
            // Go processes each account in source order and fails on the
            // FIRST duplicate rather than batching, unlike DROP USER below.
            if !registry.create_user(user, host, &auth_string) && !if_not_exists {
                return Err(DriverError::CreateUserAlreadyExists {
                    user: user.to_owned(),
                    host: host.to_owned(),
                });
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// The `mysql.user.authentication_string` one account specification's
    /// authentication clause stores. `IDENTIFIED WITH <plugin>` is refused
    /// rather than silently downgraded, because only
    /// `mysql_native_password` is modelled; a missing clause means a
    /// passwordless account, whose `authentication_string` is empty.
    fn resolve_auth_string(auth: Option<&tidb_ast::CreateUserAuth>) -> Result<String, DriverError> {
        match auth {
            None => Ok(String::new()),
            Some(tidb_ast::CreateUserAuth::By(password)) => {
                Ok(privilege::encode_password(password))
            }
            Some(tidb_ast::CreateUserAuth::With { .. }) => Err(DriverError::Unsupported(
                "CREATE/ALTER USER ... IDENTIFIED WITH is not supported yet",
            )),
        }
    }

    /// `ALTER USER [IF EXISTS] <account> IDENTIFIED BY '<password>'`, the one
    /// `ALTER USER` action this tier stores: it rewrites the account's
    /// `mysql.user.authentication_string` in place, so the NEXT login uses
    /// the new password (Go `executeAlterUser`).
    fn alter_user_stmt(
        &mut self,
        alter: &tidb_ast::AlterUserStmt,
    ) -> Result<StmtOutput, DriverError> {
        if alter.user_function_auth.is_some()
            || alter.user_function_dual_password.is_some()
            || !alter.tls_options.is_empty()
            || !alter.resource_options.is_empty()
            || !alter.password_options.is_empty()
            || alter.comment_or_attribute.is_some()
            || alter.resource_group.is_some()
        {
            return Err(DriverError::Unsupported(
                "ALTER USER options beyond IDENTIFIED BY are not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "ALTER USER requires a server front end with a privilege registry",
            ));
        };
        for spec in &alter.users {
            if spec.auth.is_none() || spec.dual_password.is_some() {
                return Err(DriverError::Unsupported(
                    "ALTER USER options beyond IDENTIFIED BY are not supported yet",
                ));
            }
            let auth_string = Self::resolve_auth_string(spec.auth.as_ref())?;
            let (user, host) = self.resolve_account(&spec.user)?;
            if !registry.set_auth_string(&user, &host, &auth_string) && !alter.if_exists {
                return Err(DriverError::AlterUserMissing { user, host });
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// `SET PASSWORD [FOR <account>] = '<password>'`: the same
    /// `authentication_string` write as `ALTER USER ... IDENTIFIED BY`
    /// (captured: both leave the identical `*HEX` value), defaulting to the
    /// session's own account.
    fn set_password_stmt(
        &mut self,
        set_password: &tidb_ast::SetPasswordStmt,
    ) -> Result<StmtOutput, DriverError> {
        if set_password.retain_current_password {
            return Err(DriverError::Unsupported(
                "SET PASSWORD ... RETAIN CURRENT PASSWORD is not supported yet",
            ));
        }
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "SET PASSWORD requires a server front end with a privilege registry",
            ));
        };
        let (user, host) = match &set_password.user {
            Some(spec) => self.resolve_account(spec)?,
            None => self.own_account()?,
        };
        let auth_string = privilege::encode_password(&set_password.password);
        if !registry.set_auth_string(&user, &host, &auth_string) {
            return Err(DriverError::SetPasswordNoMatchingRow);
        }
        Ok(StmtOutput::Affected(0))
    }

    /// `RENAME USER <old> TO <new> [, ...]`. Go's `executeRenameUser` moves
    /// the `mysql.user` row -- authentication string included -- along with
    /// every `mysql.db`/`mysql.tables_priv` row keyed by the old identity
    /// (captured: after the rename the new account holds all three scoped
    /// grant lines and the old one reports `ErrNonexistingGrant`), and
    /// reports `ErrCannotUser` for a missing source or an occupied target.
    fn rename_user_stmt(
        &mut self,
        pairs: &[tidb_ast::RenameUserPair],
    ) -> Result<StmtOutput, DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Err(DriverError::Unsupported(
                "RENAME USER requires a server front end with a privilege registry",
            ));
        };
        for pair in pairs {
            let (old_user, old_host) = self.resolve_account(&pair.old_user)?;
            let (new_user, new_host) = self.resolve_account(&pair.new_user)?;
            let old_missing = !registry.user_exists(&old_user, &old_host);
            if !registry.rename_user(&old_user, &old_host, &new_user, &new_host) {
                return Err(DriverError::RenameUserFailed {
                    old_user,
                    old_host,
                    new_user,
                    new_host,
                    old_missing,
                });
            }
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Resolves one written account identity, expanding the `CURRENT_USER`
    /// pseudo-user to the session's own identity as Go does.
    fn resolve_account(&self, spec: &tidb_ast::UserSpec) -> Result<(String, String), DriverError> {
        if spec.current_user {
            return self.own_account();
        }
        Ok((spec.user.clone(), spec.host.clone()))
    }

    /// The session's own account identity. A session with no authenticated
    /// identity is an in-process one with no front end, which has no account
    /// to name.
    fn own_account(&self) -> Result<(String, String), DriverError> {
        let (user, host) = self.current_identity().ok_or(DriverError::Unsupported(
            "CURRENT_USER requires a session with an authenticated identity",
        ))?;
        Ok((user.to_owned(), host.to_owned()))
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

    /// `GRANT <static privs> ON <level> TO <user>... [WITH GRANT OPTION]` --
    /// Go's `grant.go` GLOBAL/DATABASE/TABLE scopes. Roles, dynamic
    /// privileges, and column lists are refused rather than silently
    /// accepted or dropped.
    ///
    /// `WITH GRANT OPTION` is just `mysql.GrantPriv` ORed into the same
    /// scope's privilege mask, which is why it works identically at all
    /// three scopes and why `REVOKE GRANT OPTION ON <level>` (an ordinary
    /// privilege name) clears exactly that scope's bit.
    fn grant_stmt(&mut self, grant: &tidb_ast::GrantStmt) -> Result<StmtOutput, DriverError> {
        if grant.object_type.is_some() {
            return Err(DriverError::Unsupported(
                "GRANT ... ON FUNCTION/PROCEDURE is not supported yet",
            ));
        }
        let with_grant = if grant.with_grant {
            privilege::GlobalPriv::GrantOption.bit()
        } else {
            0
        };
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
                let (static_mask, dynamic) = self.split_global_privs(&grant.privileges, true)?;
                // Go `containsNonDynamicPriv`: `WITH GRANT OPTION` sets the
                // account's `mysql.user.Grant_priv` only when the statement
                // named at least one NON-dynamic privilege. A grant of
                // dynamic privileges alone records the grant option on each
                // `global_grants` row instead, leaving the account's own
                // `GRANT OPTION` untouched -- "with DYNAMIC privileges the
                // GRANT OPTION is individually grantable, and not a global
                // property of the user".
                let names_static = grant.privileges.iter().any(|privilege| !privilege.dynamic);
                let mask = static_mask | if names_static { with_grant } else { 0 };
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
                    for name in &dynamic {
                        registry.grant_dynamic(user, host, name, grant.with_grant);
                    }
                }
            }
            tidb_ast::GrantLevel::Database(database) => {
                let database = self.resolve_grant_database(database.as_deref())?;
                let privs = self.resolve_scoped_privs(&grant.privileges, ScopeKind::Database)?;
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit()) | with_grant;
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
                let mask = privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit()) | with_grant;
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
        // Go's `checkDynamicPrivilegeUsage` runs before any row is touched
        // and names EVERY dynamic privilege in the statement, comma-joined,
        // in the one 3619 it raises.
        if !matches!(revoke.level, tidb_ast::GrantLevel::Global) {
            let dynamic: Vec<String> = revoke
                .privileges
                .iter()
                .filter(|privilege| privilege.dynamic)
                .map(|privilege| privilege.name.to_ascii_uppercase())
                .collect();
            if !dynamic.is_empty() {
                return Err(DriverError::IllegalPrivilegeLevel(dynamic.join(",")));
            }
        }
        match &revoke.level {
            tidb_ast::GrantLevel::Global => {
                let (mask, dynamic) = self.split_global_privs(&revoke.privileges, false)?;
                let revoke_all_dynamic = revoke
                    .privileges
                    .iter()
                    .any(|privilege| privilege.name == "ALL");
                let unregistered: Vec<String> = dynamic
                    .iter()
                    .filter(|name| !privilege::is_dynamic_privilege(name))
                    .cloned()
                    .collect();
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
                    if revoke_all_dynamic {
                        registry.revoke_all_dynamic(user, host);
                    }
                    for name in &dynamic {
                        registry.revoke_dynamic(user, host, name);
                    }
                }
                // An unregistered name is a WARNING here, not the error
                // `GRANT` raises for it, and the delete still runs
                // (captured: the statement reports OK with a 3929 warning).
                for name in unregistered {
                    self.warnings.push(SqlWarning {
                        level: WarningLevel::Warning,
                        code: 3929,
                        message: format!(
                            "Dynamic privilege '{name}' is not registered with the server."
                        ),
                    });
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
            // A DYNAMIC privilege is refused for being at the wrong LEVEL
            // before anything asks whether it is registered, so an
            // unregistered name outside `*.*` reports 3619 and not 3929
            // (Go: `grantDynamicPriv`'s level check precedes its registry
            // check; `REVOKE`'s `checkDynamicPrivilegeUsage` runs even
            // earlier).
            if privilege.dynamic {
                return Err(DriverError::IllegalPrivilegeLevel(privilege.name.clone()));
            }
            let Some(priv_) = privilege::GlobalPriv::from_grant_name(&privilege.name) else {
                return Err(DriverError::DynamicPrivilegeNotRegistered(
                    privilege.name.clone(),
                ));
            };
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
    /// Splits a GLOBAL-scope privilege list into the static bitmask and the
    /// DYNAMIC privilege names, which live in different tables
    /// (`mysql.user.Priv` vs `mysql.global_grants`) and so are applied
    /// separately.
    ///
    /// `ALL [PRIVILEGES]` expands to the static mask only: Go's `GRANT ALL`
    /// never confers a dynamic privilege. (`REVOKE ALL` DOES clear them, but
    /// through its own unqualified delete rather than through this list --
    /// see [`privilege::PrivilegeRegistry::revoke_all_dynamic`].)
    ///
    /// `reject_unregistered` distinguishes the two consumers: `GRANT` fails
    /// with `ErrDynamicPrivilegeNotRegistered`/3929 on an unknown name,
    /// while `REVOKE` only WARNS with the same error and proceeds, so it
    /// asks for the names unfiltered and warns itself.
    fn split_global_privs(
        &self,
        privileges: &[tidb_ast::GrantPrivilege],
        reject_unregistered: bool,
    ) -> Result<(u64, Vec<String>), DriverError> {
        let mut mask = 0u64;
        let mut dynamic = Vec::new();
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
            if privilege.dynamic {
                if reject_unregistered && !privilege::is_dynamic_privilege(&privilege.name) {
                    return Err(DriverError::DynamicPrivilegeNotRegistered(
                        privilege.name.clone(),
                    ));
                }
                dynamic.push(privilege.name.to_ascii_uppercase());
                continue;
            }
            match privilege::GlobalPriv::from_grant_name(&privilege.name) {
                Some(priv_) => mask |= priv_.bit(),
                None => {
                    return Err(DriverError::DynamicPrivilegeNotRegistered(
                        privilege.name.clone(),
                    ));
                }
            }
        }
        Ok((mask, dynamic))
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

    /// `SELECT * FROM information_schema.USER_PRIVILEGES` rows, in Go's
    /// `MySQLPrivilege.UserPrivilegesTable` order: EVERY account's static
    /// privileges first (one row per privilege, in `mysql.AllGlobalPrivs`
    /// print order, or a single `USAGE` row for an account with none), then
    /// EVERY account's DYNAMIC privileges. Accounts are visited in username
    /// order, since Go walks a B-tree keyed by username.
    ///
    /// `IS_GRANTABLE` means different things in the two halves (captured):
    /// a static row reports the account's `GRANT OPTION`, while a dynamic
    /// row reports that one privilege's own `with_grant_option`.
    ///
    /// Visibility (Go: "Seeing all users requires SELECT ON * FROM mysql.*.
    /// The SUPER privilege (or any other dynamic privilege) doesn't help
    /// here. This is verified against MySQL."): without global `SELECT`, a
    /// session sees only its own account's rows.
    fn user_privileges_table_rows(&self) -> Vec<Vec<Datum>> {
        let Some(registry) = &self.privileges else {
            return Vec::new();
        };
        let identity = self
            .current_identity()
            .map(|(user, host)| (user.to_owned(), host.to_owned()));
        let show_all = identity.as_ref().is_none_or(|(user, host)| {
            registry.has_global_priv(user, host, privilege::GlobalPriv::Select)
        });
        let visible = |account: &(String, String)| show_all || identity.as_ref() == Some(account);

        let grantee = |(user, host): &(String, String)| format!("'{user}'@'{host}'");
        let cell = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
        let flag = |grantable: bool| cell(if grantable { "YES" } else { "NO" });

        let mut static_accounts = registry.global_priv_masks();
        static_accounts.sort_by(|(left, _), (right, _)| left.cmp(right));
        let mut rows = Vec::new();
        for (account, privs) in &static_accounts {
            if !visible(account) {
                continue;
            }
            let grantable = flag(privs & privilege::GlobalPriv::GrantOption.bit() != 0);
            let named: Vec<&privilege::GlobalPriv> = privilege::ALL_GLOBAL_PRIVS
                .iter()
                .filter(|priv_| privs & priv_.bit() != 0)
                .collect();
            if named.is_empty() {
                rows.push(vec![
                    cell(&grantee(account)),
                    cell("def"),
                    cell("USAGE"),
                    grantable.clone(),
                ]);
                continue;
            }
            for priv_ in named {
                rows.push(vec![
                    cell(&grantee(account)),
                    cell("def"),
                    cell(priv_.print_name()),
                    grantable.clone(),
                ]);
            }
        }

        let mut dynamic_accounts = registry.accounts_with_dynamic_privs();
        dynamic_accounts.sort();
        for account in &dynamic_accounts {
            if !visible(account) {
                continue;
            }
            for (name, grantable) in registry.dynamic_priv_rows(&account.0, &account.1) {
                rows.push(vec![
                    cell(&grantee(account)),
                    cell("def"),
                    cell(&name),
                    flag(grantable),
                ]);
            }
        }
        rows
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
mod tests_core;
#[cfg(test)]
mod tests_explain;
#[cfg(test)]
mod tests_grants;
#[cfg(test)]
mod tests_json;
#[cfg(test)]
mod tests_show;
#[cfg(test)]
mod tests_subquery;
#[cfg(test)]
mod tests_support;
#[cfg(test)]
mod tests_views;
#[cfg(test)]
mod tests_window;
