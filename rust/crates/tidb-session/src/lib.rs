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

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};

use tidb_ast::{DdlStmt, DmlStmt, SessionStmt, Stmt};
use tidb_datatype::{Datum, FieldType};
use tidb_executor::{Catalog, DriverError, MysqlRng};
use tidb_executor::{SchemaErrorKind, DEFAULT_DATABASE};
pub use tidb_planner::txn_mode::{
    txn_mode_for_begin, txn_mode_for_statement, SessionTxnMode, StatementTxnModeInputs,
    OPTIMISTIC_TXN_MODE, PESSIMISTIC_TXN_MODE,
};

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

/// Which piece of somebody else's persistent state a statement would change.
///
/// See [`Session::statement_stored_state_change`] for why the schema half and
/// the account half are named apart rather than lumped into one boolean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredStateChange {
    /// Nothing persistent outside this process.
    None,
    /// The stored schema: every `ast.DDLNode`.
    Schema,
    /// The stored accounts: `mysql.user`, `mysql.db` and the role edges.
    Accounts,
    /// The stored `SET GLOBAL` overrides: `mysql.global_variables`.
    GlobalVars,
    /// The stored statistics: `mysql.stats_meta` and its histogram tables.
    /// `ANALYZE TABLE` is the only statement that writes them.
    Statistics,
}

/// Whether a `SET` statement carries at least one GLOBAL-scoped assignment.
///
/// A statement can mix `SESSION` and `GLOBAL` assignments in one `SET`
/// (Go allows this), so the whole statement is routed to the GLOBAL path the
/// moment any assignment is GLOBAL-scoped; the session-scoped assignments in
/// the same statement still run, against the same driver, once routed.
fn has_global_assignment(session: &tidb_ast::SessionStmt) -> bool {
    fn is_global(assignment: &tidb_ast::SystemVariableAssignment) -> bool {
        assignment.scope == tidb_ast::SystemVariableScope::Global
    }
    match session {
        tidb_ast::SessionStmt::Set(set) => set.assignments.iter().any(is_global),
        tidb_ast::SessionStmt::SetCharset { assignments, .. } => assignments.iter().any(is_global),
        tidb_ast::SessionStmt::SetMixed(items) => items.iter().any(|item| match item {
            tidb_ast::SetItem::System(assignment) => is_global(assignment),
            tidb_ast::SetItem::Charset { .. } => false,
        }),
        _ => false,
    }
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
    /// Go `SessionVars.ActiveRoles`: the roles this session has activated,
    /// which every privilege check widens through and which `CURRENT_ROLE()`
    /// reports. A fresh session starts with its account's DEFAULT roles
    /// (Go activates them in `Auth`); `SET ROLE` replaces the set wholesale.
    active_roles: Vec<privilege::Account>,
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
    /// Go `StmtCtx.AddSetVarHintRestore`: the session overrides a `SET_VAR`
    /// hint overwrote for the duration of ONE statement, put back when that
    /// statement finishes whether it succeeded or failed.
    set_var_hint_restore: Vec<(String, Option<String>)>,
    /// Go `StmtCtx.PrevAffectedRows`, which is all `ROW_COUNT()` reports: the
    /// preceding statement's affected rows, `-1` after a SELECT, `0`
    /// otherwise. Derived once at the statement boundary from
    /// [`Session::statement_kind`], so the function and the OK packet cannot
    /// disagree about what the statement did.
    prev_row_count: i64,
    /// The class of the statement currently running, which decides what
    /// `ROW_COUNT()` reports next (Go's `StmtCtx.InSelectStmt` /
    /// `InInsertStmt` / `InUpdateStmt` / `InDeleteStmt` bits, read by
    /// `ResetContextOfStmt`). It is recorded even for a statement that ends
    /// in an error, because Go's bits survive the failure too.
    statement_kind: StatementKind,
    /// Go `StmtCtx.LastInsertID`/`LastInsertIDSet`: the id the RUNNING
    /// statement publishes. The session owns the cell and lends it to every
    /// [`tidb_executor::StmtContext`] the statement builds, so an allocating
    /// INSERT and `LAST_INSERT_ID(expr)` write one place, not two.
    published_last_insert_id: Rc<Cell<Option<u64>>>,
    /// Go `SessionVars.userVars`: this session's user variables, keyed
    /// lowercased, each holding a TYPED value (`SetUserVarVal` stores a
    /// `types.Datum`, which is why `SET @i = 5` and `SET @s = '5'` differ).
    ///
    /// The session owns the map and lends the handle to every statement
    /// context, because `@x := expr` writes it from INSIDE expression
    /// evaluation -- once per row, visible to the next select-list item.
    user_vars: Rc<RefCell<HashMap<String, Datum>>>,
    /// Go `SessionVars.SequenceState`: the last value THIS SESSION took from
    /// each sequence, keyed by lowercase `db.name`, which is what `LASTVAL`
    /// reports. It is SESSION state, not the sequence's stored counter -- a
    /// fresh session reads `NULL` from a sequence other sessions have advanced
    /// (captured: `lastval` before any `nextval` is `<nil>`).
    sequence_last_values: Rc<RefCell<HashMap<String, i64>>>,
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
    /// Go `session.sandboxMode`: this connection logged in with an EXPIRED
    /// password while the server allowed it, so it may run nothing but the
    /// `SET PASSWORD` / `ALTER USER` that fixes the password. Set by the
    /// front end from the login's verdict, cleared by the statement that
    /// stores a new password.
    sandbox_mode: bool,
    /// Go `SessionVars.Rng`: the generator unseeded `RAND()` advances, shared
    /// across every statement of this session (unlike constant `RAND(N)`,
    /// which owns a fresh per-statement generator -- see `StmtContext`).
    rand: Rc<RefCell<MysqlRng>>,
}

/// The statement classes `ROW_COUNT()` distinguishes.
///
/// Go spells this as four independent `StmtCtx` bits (`InSelectStmt`,
/// `InInsertStmt`, `InUpdateStmt`, `InDeleteStmt`) and reads them in one
/// if/else chain in `ResetContextOfStmt` (`pkg/executor/select.go:1229-1237`);
/// one enum says the same thing without letting two of them be true at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatementKind {
    /// A SELECT or set operation, after which `ROW_COUNT()` is `-1`.
    Select,
    /// INSERT/REPLACE/UPDATE/DELETE, after which `ROW_COUNT()` is the
    /// affected-row count.
    Dml,
    /// Everything else -- DDL, SHOW, SET, transaction control -- after which
    /// `ROW_COUNT()` is `0`.
    Other,
}

/// Classifies a parsed statement for `ROW_COUNT()`, unwrapping a `WITH`
/// prefix the way Go does -- the CTE belongs to the mutation, so
/// `WITH x AS (...) DELETE ...` still sets `InDeleteStmt`.
fn statement_kind_of(stmt: &Stmt) -> StatementKind {
    fn dml_kind(dml: &DmlStmt) -> StatementKind {
        match dml {
            DmlStmt::With { statement, .. } => dml_kind(statement),
            DmlStmt::Insert(_) | DmlStmt::Update(_) | DmlStmt::Delete(_) => StatementKind::Dml,
            _ => StatementKind::Other,
        }
    }
    match stmt {
        Stmt::Query(_) => StatementKind::Select,
        Stmt::Dml(dml) => dml_kind(dml),
        _ => StatementKind::Other,
    }
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
            active_roles: Vec::new(),
            connection_id: None,
            last_insert_id: 0,
            statement_insert_id: 0,
            set_var_hint_restore: Vec::new(),
            prev_row_count: 0,
            statement_kind: StatementKind::Other,
            published_last_insert_id: Rc::default(),
            user_vars: Rc::default(),
            sequence_last_values: Rc::default(),
            current_db: DEFAULT_DATABASE.to_owned(),
            process: None,
            has_process_priv: false,
            privileges: None,
            sandbox_mode: false,
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
    /// The mode this transaction opened in, resolved from the `BEGIN` keyword
    /// and `@@tidb_txn_mode` exactly as Go resolves it.
    ///
    /// This tier's store is a catalog behind a mutex, not TiKV, so there is no
    /// lock to take and the mode changes nothing about how a statement runs
    /// here. It is still resolved and kept, because it is what the client
    /// asked for and what the real-TiKV tier consumes.
    mode: SessionTxnMode,
    /// The transaction's savepoint stack, oldest first -- Go's
    /// `TxnCtx.Savepoints` (`pkg/sessionctx/variable/session.go`).
    savepoints: Vec<Savepoint>,
}

/// One entry of a transaction's savepoint stack.
///
/// Go records a `tikv.MemDBCheckpoint` -- a position in the transaction's
/// membuffer that `RollbackMemDBToCheckpoint` truncates back to. This tier's
/// transaction stages its writes in a private catalog copy rather than a
/// membuffer, so the mark is an IMAGE of that copy, restored by assignment.
/// It is the same primitive [`Session::with_staged_catalog`] already uses for
/// statement-level rollback, just held under a name for longer than one
/// statement.
struct Savepoint {
    /// The name, lowercased: Go's `AddSavepoint`/`RollbackToSavepoint` match
    /// `strings.ToLower(name)`, so `SAVEPOINT SP1` and `ROLLBACK TO sp1` are
    /// the same savepoint.
    name: String,
    /// The transaction's working catalog as of this savepoint.
    ///
    /// Kept even after a `ROLLBACK TO` restores from it, because Go's
    /// `RollbackToSavepoint` truncates the stack to `[:idx+1]` -- the named
    /// savepoint SURVIVES its own rollback and can be rolled back to again.
    image: Catalog,
}

pub use tidb_executor::TxnErrorKind;

mod account;
mod explain_arm;
pub mod infoschema;
pub mod privilege;
pub mod process;
mod process_arm;
mod show;
pub mod sysvar;
pub mod vars;
pub use vars::{GlobalSysvars, SessionVars, VarError};

/// Go `SysVar.GetNativeValType` (`pkg/sessionctx/variable/variable.go:455`),
/// which `rewriteSystemVariable` applies to every `@@var` it folds into a
/// constant: the registry's `Type` -- not the variable's name -- decides the
/// value's domain. `TypeBool` becomes the signed `1`/`0` of `TiDBOptOn`, so
/// `SELECT @@autocommit` reports `1` and never the stored `ON`; `TypeUnsigned`
/// becomes a number; every other type stays the stored string.
///
/// Go builds a `Uint` datum for `TypeUnsigned`, which this AST has no literal
/// for: [`Expr::Int`] carries digits that later fail above `i64::MAX`. A value
/// that does not fit stays a string, which renders identically and keeps the
/// arithmetic gap where it already is rather than turning a readable variable
/// into an error.
fn sysvar_native_expr(name: &str, value: String) -> tidb_ast::Expr {
    use tidb_ast::Expr;
    match sysvar::get_sys_var(name).map(|def| def.var_type) {
        Some(sysvar::VarType::Bool) => {
            let on = value.eq_ignore_ascii_case("ON") || value == "1";
            Expr::Int(i32::from(on).to_string())
        }
        Some(sysvar::VarType::Unsigned) if value.parse::<i64>().is_ok() => Expr::Int(value),
        _ => Expr::String(value),
    }
}

/// The call `@name` becomes: Go's `BuildGetVarFunction` chooses one of its
/// typed `GETVAR` signatures from the type the session holds for the name, and
/// the choice rides in the function name so the rewriter -- which has no
/// session -- can type the node (see `getvar_*` in `tidb_expr`'s
/// `builtin_return_type`).
///
/// An UNSET variable has no type to read; Go's own answer is a string-typed
/// NULL, which `getvar_string` produces.
fn uservar_read_expr(name: &str, value: Option<&Datum>) -> tidb_ast::Expr {
    let kind = match value {
        Some(Datum::Int(_)) => "int",
        Some(Datum::UInt(_)) => "uint",
        Some(Datum::Real(_)) => "real",
        Some(Datum::Decimal(_)) => "decimal",
        _ => "string",
    };
    tidb_ast::Expr::Func {
        name: format!("getvar_{kind}"),
        args: vec![tidb_ast::Expr::String(name.to_owned())],
        origin_position: 0,
    }
}

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
        VarError::SessionOnlyVariable(name) => {
            tidb_executor::VarErrorKind::SessionOnlyVariable(name)
        }
        VarError::GlobalOnlyVariable(name) => tidb_executor::VarErrorKind::GlobalOnlyVariable(name),
        VarError::NoGlobalCopy(name) => tidb_executor::VarErrorKind::NoGlobalCopy(name),
    })
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

/// The image of the catalog a statement started from, restored on ANY exit
/// that is not an explicit disarm -- an `Err` returned by the statement, and
/// a panic unwinding out of it (see [`Session::with_staged_catalog`]).
struct CatalogStage<'a> {
    /// The catalog the statement mutates in place.
    catalog: &'a mut Catalog,
    /// The image to put back, taken away once the statement has succeeded.
    stage: Option<Catalog>,
}

impl Drop for CatalogStage<'_> {
    fn drop(&mut self) {
        if let Some(stage) = self.stage.take() {
            *self.catalog = stage;
        }
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
    ///
    /// This is also where DDL's IMPLICIT COMMIT lives, because every DDL
    /// statement passes through here before reaching its own arm --
    /// see the `Stmt::Ddl` arm below.
    pub fn apply_schema_statement(&mut self, sql: &str) -> Result<Option<StmtOutput>, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        if matches!(stmt, Stmt::Ddl(_)) {
            // Go commits the open transaction before running any DDL
            // (`session.ExecuteStmt`, which calls `sessiontxn`'s
            // `OnStmtStart` -> `checkBeforeNewTxn` for a DDL node), so the
            // DDL and everything staged before it are already durable when
            // it starts. Captured from TiDB: after
            // `INSERT; BEGIN; INSERT; TRUNCATE TABLE d; ROLLBACK` the table
            // is EMPTY -- the ROLLBACK takes nothing back, because the
            // TRUNCATE committed the insert that preceded it -- and the same
            // `ALTER TABLE ... AUTO_INCREMENT` sequence leaves the in-
            // transaction row stored.
            //
            // Doing this before the DDL runs is also what keeps the DDL off
            // the transaction's WORKING COPY of the catalog: with no open
            // transaction, `with_catalog_mut` reaches the shared catalog, so
            // a TRUNCATE's counter reset lands on the table that survives
            // rather than on a copy about to be discarded.
            self.commit()?;
        }
        match &stmt {
            Stmt::Session(session_stmt) => match &**session_stmt {
                SessionStmt::Use(name) => {
                    self.use_database(name)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                SessionStmt::SetRole(set_role) => Ok(Some(self.set_role_stmt(set_role)?)),
                SessionStmt::SetDefaultRole(set_default) => {
                    Ok(Some(self.set_default_role_stmt(set_default)?))
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
                    is_role,
                    if_exists,
                    users,
                } => Ok(Some(self.drop_user_stmt(*is_role, *if_exists, users)?)),
                tidb_ast::DdlStmt::AlterUser(alter) => Ok(Some(self.alter_user_stmt(alter)?)),
                tidb_ast::DdlStmt::RenameUser { pairs } => Ok(Some(self.rename_user_stmt(pairs)?)),
                tidb_ast::DdlStmt::CreateRole {
                    if_not_exists,
                    roles,
                } => Ok(Some(self.create_role_stmt(*if_not_exists, roles)?)),
                _ => Ok(None),
            },
            Stmt::Admin(admin) => self.dispatch_admin_stmt(admin),
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

    /// Go `clientConn.useDB`: selects a schema outside the statement path.
    ///
    /// The connection front end reaches this for the handshake's initial
    /// database and for `COM_INIT_DB`, which Go both route through `useDB`.
    /// Taking the name directly instead of re-rendering `use \`name\`` keeps
    /// backquotes and other identifier syntax out of the picture entirely.
    /// The process-list row is refreshed for the same reason a statement
    /// refreshes it: a peer's `SHOW PROCESSLIST` reports the schema now
    /// selected.
    pub fn select_database(&mut self, name: &str) -> Result<(), DriverError> {
        self.use_database(name)?;
        if let Some(guard) = &self.process {
            guard
                .registry()
                .statement_finished(guard.id(), &self.current_db, &self.status_text());
        }
        Ok(())
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
    /// `SET GLOBAL` writes straight into the shared [`vars::GlobalSysvars`]
    /// this call was given (see [`Self::attach_globals`],
    /// [`Self::swap_globals`]), which is this process's only copy unless a
    /// front end also persists it: the convergence node's
    /// `crate::cluster_sysvar_seam` (in `tidb-server`) is what makes that
    /// table itself a scratch read of `mysql.global_variables`, validates
    /// this call against it, and persists the result. A front end with no
    /// such seam (an in-process session, or a node that serves no cluster)
    /// keeps the in-memory-only behavior this always had.
    ///
    /// DEFERRED (documented): resource groups and the other non-variable
    /// `SET` forms stay unsupported.
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
                    let value = self.eval_value(&assignment.value)?;
                    let key = assignment.name.to_ascii_lowercase();
                    // Go's `SET @x = NULL` CLEARS the variable
                    // (`UnsetUserVar`), which is the opposite of the inline
                    // `@x := NULL` assignment expression -- that one leaves
                    // the existing value alone.
                    if matches!(value, Datum::Null) {
                        self.user_vars.borrow_mut().remove(&key);
                    } else {
                        self.user_vars.borrow_mut().insert(key, value);
                    }
                }
                Ok(Some(()))
            }
            _ => Ok(None),
        }
    }

    /// One `name = value` assignment.
    ///
    /// `GLOBAL` writes the shared table every session of this factory reads
    /// (see [`vars::GlobalSysvars`]), gated on Go's `ErrSpecificAccessDenied`
    /// (1227): SUPER or the dynamic `SYSTEM_VARIABLES_ADMIN` privilege.
    /// `SESSION`/`INSTANCE`/unqualified write this session's own copy, as
    /// today. Both directions reject a scope the variable does not have
    /// (1228/1229), matching Go's `validateScope`.
    fn apply_assignment(
        &mut self,
        assignment: &tidb_ast::SystemVariableAssignment,
    ) -> Result<(), DriverError> {
        let is_global = assignment.scope == tidb_ast::SystemVariableScope::Global;
        if is_global {
            self.require_set_global_privilege()?;
        }
        let value = match &assignment.value {
            // Go restores a variable to its registry default by clearing the
            // session (or global) override.
            tidb_ast::SetVariableValue::Default => {
                if is_global {
                    self.vars
                        .reset_global(&assignment.name)
                        .map_err(var_error)?;
                } else {
                    self.vars
                        .reset_system(&assignment.name)
                        .map_err(var_error)?;
                    // Go resolves DEFAULT to the registry's default STRING and
                    // then calls `SetSession` with it, so `SET rand_seed1 =
                    // DEFAULT` really does push 0 into the generator rather
                    // than leaving the seed where the last `SET` put it
                    // (captured: after `SET rand_seed1 = 19`, two DEFAULTs make
                    // the next `RAND()` exactly 0).
                    self.seed_rand_from_sysvar(&assignment.name)?;
                }
                return Ok(());
            }
            tidb_ast::SetVariableValue::Expr(expr) => self.eval_literal(expr)?,
        };
        // Go stores every system variable as a string.
        let value = value.unwrap_or_default();
        self.check_read_only_noop(&assignment.name, &value, is_global)?;
        if is_global {
            return self
                .vars
                .set_global(&assignment.name, value)
                .map_err(var_error);
        }
        let was_autocommit = self.is_autocommit();
        self.vars
            .set_system(&assignment.name, value)
            .map_err(var_error)?;
        self.seed_rand_from_sysvar(&assignment.name)?;
        // Go `sysvar.go`'s `AutoCommit.SetSession`: turning autocommit back
        // ON ends the ongoing transaction ("Implicitly commit the possible
        // ongoing transaction if mode is changed from off to on"). Only the
        // TRANSITION does it -- `SET autocommit = 1` while it is already on
        // leaves an explicit `BEGIN` running, which is why
        // `BEGIN; INSERT; SET autocommit = 1; ROLLBACK` still rolls back
        // (captured).
        if assignment.name.eq_ignore_ascii_case("autocommit")
            && !was_autocommit
            && self.is_autocommit()
        {
            self.commit()?;
        }
        Ok(())
    }

    /// Go's `rand_seed1`/`rand_seed2` `SetSession` hooks: the value SET is a
    /// raw seed for this session's `RAND()` generator, and is NOT retained as
    /// the variable's value.
    ///
    /// Both sysvars answer `GetSession` with the constant `"0"` in Go, so
    /// `@@rand_seed1`, `@@session.rand_seed1` and `SHOW VARIABLES LIKE
    /// 'rand_seed1'` all report 0 no matter what was set or what the generator
    /// has advanced to (captured on all three surfaces). Clearing the session
    /// override here reproduces that everywhere at once -- the variable table
    /// answers its own default -- instead of teaching each read path to special
    /// case these two names. Only `GetStateValue`, which serializes session
    /// state, ever exposes the live seeds, and this tier has no such surface.
    ///
    /// The value read back is the one `set_system` already NORMALIZED, so Go's
    /// clamping travels with it: `2147483648` arrives as `MaxInt32` and a
    /// negative arrives as 0, which is also what `tidbOptPositiveInt32` would
    /// have produced.
    fn seed_rand_from_sysvar(&mut self, name: &str) -> Result<(), DriverError> {
        let first = name.eq_ignore_ascii_case("rand_seed1");
        if !first && !name.eq_ignore_ascii_case("rand_seed2") {
            return Ok(());
        }
        let seed = self
            .vars
            .get_system(name)
            .map_err(var_error)?
            .parse::<u32>()
            .unwrap_or(0);
        let mut rand = self.rand.borrow_mut();
        if first {
            rand.set_seed1(seed);
        } else {
            rand.set_seed2(seed);
        }
        drop(rand);
        self.vars.reset_system(name).map_err(var_error)
    }

    /// Go `SessionVars.IsAutocommit()`: whether each statement stands on its
    /// own, or joins a transaction the session keeps open for it.
    fn is_autocommit(&self) -> bool {
        self.vars.get_system("autocommit").as_deref() != Ok("OFF")
    }

    /// Go's lazy transaction start: with autocommit OFF, a statement that
    /// touches data runs INSIDE a transaction the session opens for it, so a
    /// later `ROLLBACK` can discard it. `BEGIN` still opens one explicitly;
    /// this only covers the statements that would otherwise have none.
    fn begin_implicit_transaction(&mut self) -> Result<(), DriverError> {
        if self.txn.is_some() || self.is_autocommit() {
            return Ok(());
        }
        let (working, base_version) = {
            let catalog = self.lock_catalog()?;
            (catalog.clone(), catalog.version())
        };
        self.txn = Some(Transaction {
            working,
            base_version,
            mode: self.resolve_begin_txn_mode(tidb_ast::TransactionMode::Default),
            // A transaction opened lazily by `autocommit = 0` carries the same
            // savepoint stack an explicit BEGIN does -- Go makes no distinction
            // between the two once `InTxn()` holds, so SAVEPOINT works here too.
            savepoints: Vec::new(),
        });
        Ok(())
    }

    /// Go's privilege gate on `SET GLOBAL`: SUPER, or the dynamic
    /// `SYSTEM_VARIABLES_ADMIN` privilege (which `has_dynamic_priv` already
    /// falls back to SUPER for, so this one call covers "at least one of").
    /// No attached privilege registry (an in-process session with no
    /// front end) is treated as unrestricted, matching every other
    /// privilege check this session performs before a registry is attached.
    fn require_set_global_privilege(&self) -> Result<(), DriverError> {
        let Some(registry) = &self.privileges else {
            return Ok(());
        };
        let Some((user, host)) = self.current_identity() else {
            return Ok(());
        };
        if registry.has_dynamic_priv_with_roles(
            user,
            host,
            self.active_roles(),
            "SYSTEM_VARIABLES_ADMIN",
            false,
        ) {
            Ok(())
        } else {
            Err(DriverError::Var(
                tidb_executor::VarErrorKind::SetGlobalAccessDenied,
            ))
        }
    }

    /// Go `RequestVerification` at table scope: whether this session holds
    /// `global_priv` on `database`.`table`, through its own grants or a role.
    ///
    /// A session with no attached registry, or none with a front end, is
    /// unrestricted -- the same rule
    /// [`Self::require_set_global_privilege`] applies, and the same reason:
    /// an in-process session has no identity to check.
    ///
    /// The *error* a denied caller reports is the caller's, not this
    /// method's: Go words it per statement (`ANALYZE` reports
    /// `ErrTableaccessDenied` naming INSERT or SELECT), and only the caller
    /// knows which privilege it was asking about.
    #[must_use]
    pub fn has_table_privilege(
        &self,
        database: &str,
        table: &str,
        global_priv: privilege::GlobalPriv,
    ) -> bool {
        let Some(registry) = &self.privileges else {
            return true;
        };
        let Some((user, host)) = self.current_identity() else {
            return true;
        };
        registry.has_table_priv_with_roles(
            user,
            host,
            self.active_roles(),
            database,
            table,
            global_priv,
        )
    }

    /// The `user@host` this session authenticated as, as Go's
    /// `AuthUsername`/`AuthHostname` pair. `None` for a session with no front
    /// end.
    #[must_use]
    pub fn authenticated_identity(&self) -> Option<(&str, &str)> {
        self.current_identity()
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
        Ok(datum_text(&self.eval_value(expr)?))
    }

    /// Evaluates a `SET` right-hand side to its TYPED value, which is what a
    /// user variable stores (Go's `SetUserVarVal` takes a `types.Datum`). A
    /// system variable keeps only the text, so [`Self::eval_literal`] is this
    /// plus `datum_text`.
    ///
    /// The expression may itself reference variables (`SET @z = @x + 1`), so
    /// they are bound to their values first -- the same substitution a
    /// user-facing query gets, for the same reason: the rewriter behind
    /// `run_select_on` knows literals and columns, not session state.
    fn eval_value(&mut self, expr: &tidb_ast::Expr) -> Result<Datum, DriverError> {
        // An unquoted identifier is a bare word value such as `SET sql_mode =
        // ANSI_QUOTES` or `SET autocommit = ON`, which MySQL takes literally
        // (`SET @x = ANSI_QUOTES` stores the string too, confirmed via
        // `gorun`).
        if let tidb_ast::Expr::Column(path) = expr {
            if let [word] = path.as_slice() {
                return Ok(Datum::new_string(word.clone()));
            }
        }
        let bound = self.bind_variables_in(expr)?;
        let sql = format!("SELECT {}", bound.restore());
        let ctx = self.statement_context(false);
        let rows =
            self.with_catalog_mut(|catalog| tidb_executor::run_select_on(&sql, catalog, &ctx))?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .unwrap_or(Datum::Null))
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
            Expr::SysVar { scope, name } => {
                // `@@last_insert_id` and its `@@identity` alias are the SAME
                // value `LAST_INSERT_ID()` reports -- Go's
                // `StmtCtx.PrevLastInsertID` -- not an entry in the variable
                // table, which is why they are answered from the session's
                // publication rather than from `get_system`. `@@global.` on
                // either is still the variable table's error (captured).
                if *scope != Some(tidb_ast::SysVarScope::Global)
                    && (name.eq_ignore_ascii_case("last_insert_id")
                        || name.eq_ignore_ascii_case("identity"))
                {
                    return Ok(Expr::Int(self.last_insert_id.to_string()));
                }
                // `@@global.x` reads the shared table live; every other
                // scope (unqualified, `@@session.x`, `@@instance.x`) reads
                // this session's own copy.
                let result = if *scope == Some(tidb_ast::SysVarScope::Global) {
                    self.vars.get_global(name)
                } else {
                    self.vars.get_system(name)
                };
                match result {
                    Ok(value) => sysvar_native_expr(name, value),
                    Err(error) => return Err(var_error(error)),
                }
            }
            // A user variable's VALUE is not substituted -- it becomes a
            // `getvar_<kind>` call the evaluator resolves against the
            // session's own map, which is the only way `SELECT @last := v,
            // @last FROM t` can see the assignment made for the CURRENT row.
            // What IS decided here is the kind, from the value the session
            // holds now: Go's `BuildGetVarFunction` picks its typed signature
            // the same way, at build time.
            Expr::UserVar(name) => uservar_read_expr(
                name,
                self.user_vars.borrow().get(&name.to_ascii_lowercase()),
            ),
            // The assignment expression keeps its own shape (the rewriter
            // types it from the value), but its value may itself read
            // variables.
            Expr::Assign { name, value } => Expr::Assign {
                name: name.clone(),
                value: Box::new(self.bind_variables_in(value)?),
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

    /// The mode the open transaction runs in, if one is open.
    ///
    /// `BEGIN PESSIMISTIC` and `BEGIN OPTIMISTIC` are accepted here and their
    /// mode is reported faithfully, but this tier takes no row locks in either
    /// mode: its store is one shared catalog behind a mutex, so concurrent
    /// sessions already serialize and a committing session that lost the race
    /// is refused with a write conflict. `SELECT ... FOR UPDATE` returns the
    /// same rows it would under a real pessimistic lock; what is missing is
    /// the lock, not the result (see [`Self::check_query_clauses`]).
    #[must_use]
    pub fn txn_mode(&self) -> Option<SessionTxnMode> {
        self.txn.as_ref().map(|txn| txn.mode)
    }

    /// Go `newProviderWithRequest`: `BEGIN <mode>` wins over `@@tidb_txn_mode`.
    fn resolve_begin_txn_mode(&self, mode: tidb_ast::TransactionMode) -> SessionTxnMode {
        let variable = self
            .vars
            .get_system("tidb_txn_mode")
            .unwrap_or_else(|_| PESSIMISTIC_TXN_MODE.to_owned());
        txn_mode_for_begin(mode, &variable)
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
            SessionStmt::Begin(begin) => {
                // An open transaction is committed first (Go's implicit commit).
                if self.txn.is_some() {
                    self.commit()?;
                }
                let mode = self.resolve_begin_txn_mode(begin.mode);
                let (working, base_version) = {
                    let catalog = self.lock_catalog()?;
                    (catalog.clone(), catalog.version())
                };
                self.txn = Some(Transaction {
                    working,
                    base_version,
                    mode,
                    savepoints: Vec::new(),
                });
                Ok(Some(true))
            }
            SessionStmt::Commit(_) => {
                self.commit()?;
                Ok(Some(false))
            }
            SessionStmt::Rollback { savepoint, .. } => {
                if let Some(name) = savepoint {
                    // ROLLBACK TO does NOT end the transaction: it restores
                    // the data and leaves everything else running.
                    self.rollback_to_savepoint(name)?;
                    return Ok(Some(true));
                }
                // Dropping the staged copy discards every staged write.
                self.txn = None;
                Ok(Some(false))
            }
            SessionStmt::Savepoint(name) => {
                self.set_savepoint(name)?;
                Ok(Some(self.txn.is_some()))
            }
            SessionStmt::ReleaseSavepoint(name) => {
                self.release_savepoint(name)?;
                Ok(Some(true))
            }
            _ => Ok(None),
        }
    }

    /// `SAVEPOINT name` -- Go `SimpleExec.executeSavepoint`.
    ///
    /// The no-op arm is narrower than "no transaction open": Go returns `nil`
    /// only when `!sessVars.InTxn() && sessVars.IsAutocommit()`. With
    /// autocommit OFF and no transaction yet, `e.Ctx().Txn(true)` ACTIVATES
    /// the pending transaction, so `SAVEPOINT` is what opens it and a later
    /// `ROLLBACK TO` that name finds it. Only in autocommit does the
    /// statement succeed while recording nothing, leaving `ROLLBACK TO` to
    /// report 1305.
    ///
    /// Redefining an existing name is `AddSavepoint`: DELETE the old entry,
    /// then APPEND the new one. The distinction matters -- the redefinition
    /// moves the name to the END of the stack, so savepoints that were taken
    /// after the original are no longer "after" it, and a later `ROLLBACK TO`
    /// the redefined name no longer drops them.
    fn set_savepoint(&mut self, name: &str) -> Result<(), DriverError> {
        // Go's `Txn(true)`: with autocommit OFF this is the statement that
        // opens the pending transaction.
        self.begin_implicit_transaction()?;
        let Some(txn) = &mut self.txn else {
            return Ok(());
        };
        let name = name.to_lowercase();
        let image = txn.working.clone();
        txn.savepoints.retain(|savepoint| savepoint.name != name);
        txn.savepoints.push(Savepoint { name, image });
        Ok(())
    }

    /// `ROLLBACK TO [SAVEPOINT] name` -- Go's `executeRollback` savepoint arm
    /// plus `TxnCtx.RollbackToSavepoint`.
    ///
    /// Restores the transaction's data to the savepoint (Go:
    /// `RollbackMemDBToCheckpoint`) and truncates the stack to `[:idx+1]`, so
    /// the savepoint itself survives and every savepoint taken after it is
    /// gone. The transaction stays OPEN -- Go returns before the
    /// `SetInTxn(false)` that a plain `ROLLBACK` reaches.
    ///
    /// With no transaction open Go's `txn.Valid()` is false and the error is
    /// the same 1305 an unknown name gets.
    fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), DriverError> {
        let lowered = name.to_lowercase();
        let txn = self
            .txn
            .as_mut()
            .ok_or_else(|| DriverError::SavepointNotExists(name.to_owned()))?;
        let index = txn
            .savepoints
            .iter()
            .position(|savepoint| savepoint.name == lowered)
            .ok_or_else(|| DriverError::SavepointNotExists(name.to_owned()))?;
        txn.working = txn.savepoints[index].image.clone();
        txn.savepoints.truncate(index + 1);
        Ok(())
    }

    /// `RELEASE SAVEPOINT name` -- Go `SimpleExec.executeReleaseSavepoint`
    /// plus `TxnCtx.ReleaseSavepoint`: drops the named savepoint AND every
    /// savepoint taken after it (`Savepoints[:i]`), touching no data.
    fn release_savepoint(&mut self, name: &str) -> Result<(), DriverError> {
        let lowered = name.to_lowercase();
        let index = self
            .txn
            .as_ref()
            .and_then(|txn| {
                txn.savepoints
                    .iter()
                    .position(|savepoint| savepoint.name == lowered)
            })
            .ok_or_else(|| DriverError::SavepointNotExists(name.to_owned()))?;
        if let Some(txn) = &mut self.txn {
            txn.savepoints.truncate(index);
        }
        Ok(())
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
            active_roles: Vec::new(),
            connection_id: None,
            last_insert_id: 0,
            statement_insert_id: 0,
            set_var_hint_restore: Vec::new(),
            prev_row_count: 0,
            statement_kind: StatementKind::Other,
            published_last_insert_id: Rc::default(),
            user_vars: Rc::default(),
            sequence_last_values: Rc::default(),
            current_db: DEFAULT_DATABASE.to_owned(),
            process: None,
            has_process_priv: false,
            privileges: None,
            sandbox_mode: false,
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

    /// Runs one DML statement over a STAGE of the catalog this statement sees,
    /// so a statement that fails partway leaves the tables as it found them.
    ///
    /// This is Go's statement-level rollback. A statement opens a staging
    /// handle on the transaction's membuffer (`pkg/kv/union_store.go`:
    /// `MemBuffer.Staging()`), writes into it, and
    /// `pkg/executor/adapter.go` chooses between
    /// `pkg/session/session.go`'s `StmtCommit` -- `Release()`, folding the
    /// stage into the transaction -- and `StmtRollback` -- `Cleanup()`,
    /// dropping it. The transaction itself is untouched either way, which is
    /// why a failed statement inside `BEGIN` discards only its own writes and
    /// the statements around it survive to `COMMIT`.
    ///
    /// The stage here is an image of the catalog rather than an undo log,
    /// because this tier's tables ARE the buffer: `Catalog::clone` deep-copies
    /// the in-process bytes (`MemTableStorage::clone_box`). Restoring the
    /// image is the same observable effect as `Cleanup()`.
    ///
    /// # What the image does NOT undo
    ///
    /// The restore takes back exactly what `TableStorage::clone_box` copied by
    /// VALUE. `MemTableStorage` copies its bytes, so the rows come back. A
    /// storage whose `clone_box` clones a shared HANDLE does not: the image
    /// and the original write into the same place, so a failed statement's
    /// rows survive this restore. `tidb_executor::cluster_storage` is exactly
    /// that -- its `MutationBuffer` and snapshot are `Arc`s shared by every
    /// table of the session -- so on the cluster path the guard is NOT this
    /// function. It is the statement savepoint the convergence node takes over
    /// the buffer itself (`tidb_server`'s `ClusterServerSession::with_statement`:
    /// `MutationBuffer::staged()` before the statement, `restore()` on its
    /// error arm), which is the same `Staging()`/`Cleanup()` pair one tier
    /// down. Any future front end that drives a `Session` over cluster storage
    /// must bring such a savepoint with it; this restore alone will not roll
    /// its writes back.
    ///
    /// What the image DOES undo on either storage is the table state that is
    /// not bytes: `KvTable::next_handle`, the `_tidb_rowid` counter, is a
    /// plain field, so a failed statement gives back the handles it consumed.
    /// `AutoIdAllocator` is deliberately not, per the paragraph below.
    ///
    /// AUTO_INCREMENT deliberately survives the restore: Go allocates ids
    /// outside transaction semantics and never returns a consumed one, and
    /// `KvTable`'s `AutoIdAllocator` is a SHARED cell that a catalog copy
    /// keeps pointing at -- so the burn is retained with no exclusion rule
    /// here (captured: a failed one-row insert into an `AUTO_INCREMENT` table
    /// stores nothing and the next successful insert skips the burned id).
    ///
    /// Making this the one door every mutating statement goes through is the
    /// point: the restore lives in a guard's `Drop`, so no exit of `body` --
    /// and no DML arm added later -- can forget it.
    ///
    /// The guard rather than an error arm is what makes a PANIC take the same
    /// path as an `Err`. An `inspect_err` restore is skipped entirely when
    /// `body` unwinds, and inside `BEGIN` the catalog being mutated is the
    /// transaction's own working copy, held behind no lock -- so a caught
    /// panic would leave a HALF-APPLIED statement for `COMMIT` to publish.
    fn with_staged_catalog<T>(
        &mut self,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        self.with_catalog_mut(|catalog| {
            let mut guard = CatalogStage {
                stage: Some(catalog.clone()),
                catalog,
            };
            // `?` and an unwind both drop the guard while it is still armed;
            // only reaching the disarm below keeps the statement's writes.
            let value = body(guard.catalog)?;
            guard.stage = None;
            Ok(value)
        })
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

    /// Which persistent state `sql` would change: the stored schema (Go's
    /// `ast.DDLNode`), the stored accounts (the privilege and role statements
    /// TiDB's parser builds as administrative rather than DDL, plus `SET
    /// PASSWORD`), or neither.
    ///
    /// A front end whose catalog and account table are a *read* of somebody
    /// else's stored state needs this: running such a statement would change
    /// only this process's in-memory copy, which is a silently wrong answer
    /// rather than a slow one. The two halves are named apart because a front
    /// end can gain a route for one without gaining a route for the other --
    /// the convergence node writes the cluster's catalog but not its `mysql.*`
    /// rows. Classifying it here keeps that decision on the parse, next to
    /// [`Self::statement_kind`], instead of in each front end's own matcher.
    pub fn statement_stored_state_change(
        &self,
        sql: &str,
    ) -> Result<StoredStateChange, DriverError> {
        let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
        Ok(match &stmt {
            // The account statements the parser builds as DDL nodes, because
            // Go's `ast.DDLNode` covers them too: they write `mysql.user` and
            // the role edges, never the catalog.
            Stmt::Ddl(ddl)
                if matches!(
                    ddl.as_ref(),
                    tidb_ast::DdlStmt::CreateUser { .. }
                        | tidb_ast::DdlStmt::CreateRole { .. }
                        | tidb_ast::DdlStmt::AlterUser(_)
                        | tidb_ast::DdlStmt::DropUser { .. }
                        | tidb_ast::DdlStmt::RenameUser { .. }
                ) =>
            {
                StoredStateChange::Accounts
            }
            Stmt::Ddl(_) => StoredStateChange::Schema,
            // The privilege/role statements: everything under `Admin` that
            // writes `mysql.user`, `mysql.db`, or the role edges. `SHOW
            // GRANTS` and the other inspections read and are left alone.
            Stmt::Admin(admin)
                if matches!(
                    admin.as_ref(),
                    tidb_ast::AdminStmt::Grant(_)
                        | tidb_ast::AdminStmt::GrantProxy(_)
                        | tidb_ast::AdminStmt::GrantRole(_)
                        | tidb_ast::AdminStmt::Revoke(_)
                        | tidb_ast::AdminStmt::RevokeRole(_)
                ) =>
            {
                StoredStateChange::Accounts
            }
            // `SET PASSWORD` and `SET DEFAULT ROLE` write `mysql.user` and
            // `mysql.default_roles`; every other `SET` is session- or
            // process-local.
            Stmt::Session(session)
                if matches!(
                    session.as_ref(),
                    tidb_ast::SessionStmt::SetPassword(_)
                        | tidb_ast::SessionStmt::SetDefaultRole(_)
                ) =>
            {
                StoredStateChange::Accounts
            }
            // `SET GLOBAL x = v` (alone or mixed with SESSION assignments in
            // the same statement) writes `mysql.global_variables`. A `SET`
            // with no GLOBAL-scoped assignment at all changes only this
            // session's own copies, so it takes the ordinary path.
            Stmt::Session(session) if has_global_assignment(session) => {
                StoredStateChange::GlobalVars
            }
            // `ANALYZE TABLE` writes `mysql.stats_*`, which every node in the
            // cluster reads. Running it against this process's own loaded
            // statistics would answer OK to a client whose table's histograms
            // did not move anywhere.
            Stmt::Admin(admin)
                if matches!(
                    admin.as_ref(),
                    tidb_ast::AdminStmt::AnalyzeTable(_)
                        | tidb_ast::AdminStmt::AnalyzeIncremental(_)
                ) =>
            {
                StoredStateChange::Statistics
            }
            Stmt::Admin(_) | Stmt::Session(_) | Stmt::Query(_) | Stmt::Dml(_) => {
                StoredStateChange::None
            }
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
        // Go's `Auth` activates the account's DEFAULT roles the moment the
        // connection is authenticated (captured: a fresh session reports its
        // default roles from `CURRENT_ROLE()` with no `SET ROLE` at all).
        // The registry is what makes that answerable, so attaching it is the
        // one place that can do it -- the front end installs the identity
        // first and the registry second.
        if let Some((user, host)) = self.current_identity() {
            self.active_roles = registry.default_roles(&(user.to_owned(), host.to_owned()));
        }
        self.privileges = Some(registry);
    }

    /// Points this session at a different account table for one statement,
    /// answering the one it was using.
    ///
    /// Unlike [`Self::attach_privileges`] this touches nothing else: the
    /// session's active roles are its own state, and a front end that runs an
    /// account statement against a scratch copy of the table -- which is how
    /// a node whose registry is a read of somebody else's `mysql.*` validates
    /// the statement before persisting it -- must be able to put the live
    /// table back without a `SET ROLE` silently reverting to the defaults.
    pub fn swap_privileges(
        &mut self,
        registry: privilege::PrivilegeRegistry,
    ) -> Option<privilege::PrivilegeRegistry> {
        self.privileges.replace(registry)
    }

    /// Joins this session to the server's shared GLOBAL-scope sysvar table
    /// and snapshots its current overrides into this session's own copy --
    /// see [`vars::SessionVars::seed_from_globals`] for why that snapshot
    /// happens exactly once, here, rather than on every read.
    pub fn attach_globals(&mut self, globals: vars::GlobalSysvars) {
        self.vars.seed_from_globals(globals);
    }

    /// Points this session at a different shared GLOBAL-scope sysvar table
    /// for one statement, answering the one it was using.
    ///
    /// Unlike [`Self::attach_globals`] this does not reseed the session's own
    /// `@@x` copies: a front end running a `SET GLOBAL` against a scratch
    /// table read from the cluster (validate-then-persist, exactly like
    /// [`Self::swap_privileges`]) must be able to put the live table back
    /// unconditionally if the statement fails, without disturbing anything
    /// else this session has already seeded from it.
    pub fn swap_globals(&mut self, globals: vars::GlobalSysvars) -> vars::GlobalSysvars {
        self.vars.swap_globals(globals)
    }

    /// Go `SessionVars.ActiveRoles`, for the privilege checks and the
    /// `CURRENT_ROLE()` builtin.
    fn active_roles(&self) -> &[privilege::Account] {
        &self.active_roles
    }

    /// The text `CURRENT_ROLE()` reports: Go's `builtinCurrentRoleSig` joins
    /// each active role's `RoleIdentity.String()` (backtick-quoted
    /// ``\`role\`@\`host\```) with a bare comma, and answers the literal
    /// `NONE` when no role is active (captured, both forms).
    fn current_role_text(&self) -> String {
        if self.active_roles.is_empty() {
            return "NONE".to_owned();
        }
        self.active_roles
            .iter()
            .map(|(role, host)| format!("`{role}`@`{host}`"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Splits the `CURRENT_USER()` identity (`user@host`) this session
    /// authenticated as, for privilege-registry lookups. `None` for a
    /// session with no front end.
    fn current_identity(&self) -> Option<(&str, &str)> {
        let identity = self.current_user.as_deref()?;
        identity.split_once('@')
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
        self.check_sandbox_mode(sql)?;
        // A statement is visible to a peer's SHOW PROCESSLIST for exactly as
        // long as it runs, which is why the process list is updated here --
        // the one door every statement of this session goes through -- rather
        // than in one front end's command loop.
        if let Some(guard) = &self.process {
            guard
                .registry()
                .statement_started(guard.id(), sql, &self.status_text());
        }
        // Go's `ResetContextOfStmt` promotes the PRECEDING statement's
        // publication into the `Prev*` fields the next statement reads, so
        // the promotion happens at the boundary, once, for every statement.
        self.statement_kind = StatementKind::Other;
        self.published_last_insert_id.set(None);
        let result = self.execute_statement(sql);
        // Go `ExecStmt` puts a `SET_VAR` hint's variables back when the
        // statement finishes, from the restore list the optimizer built --
        // which is why an overlay survives neither a successful statement nor
        // a failing one.
        let restore = std::mem::take(&mut self.set_var_hint_restore);
        self.vars.restore_system(restore);
        self.publish_statement_status(&result);
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

    /// Whether this session logged in with an expired password and is
    /// therefore restricted to fixing it -- Go's `session.InSandBoxMode`.
    #[must_use]
    pub const fn in_sandbox_mode(&self) -> bool {
        self.sandbox_mode
    }

    /// Puts this session in sandbox mode. The front end calls this when the
    /// login reported an expired password the server chose to admit.
    pub const fn enable_sandbox_mode(&mut self) {
        self.sandbox_mode = true;
    }

    /// Go's `TiDBContext.checkSandBoxMode` (`pkg/server/driver_tidb.go`): a
    /// sandboxed session may run `SET PASSWORD` and `ALTER USER` and nothing
    /// else; everything else reports 1820. Go gates on the PARSED statement
    /// (its front end hands `ExecuteStmt` an `ast.StmtNode`), so a syntax
    /// error still surfaces as a syntax error -- which is why this parses
    /// first and lets a parse failure fall through to the normal path that
    /// reports it. The extra parse is paid only while sandboxed, a state one
    /// statement ends.
    fn check_sandbox_mode(&self, sql: &str) -> Result<(), DriverError> {
        if !self.sandbox_mode {
            return Ok(());
        }
        let Ok(stmt) = tidb_parser::parse(sql) else {
            return Ok(());
        };
        match stmt {
            Stmt::Session(session) if matches!(*session, SessionStmt::SetPassword(_)) => Ok(()),
            Stmt::Ddl(ddl) if matches!(*ddl, tidb_ast::DdlStmt::AlterUser(_)) => Ok(()),
            _ => Err(DriverError::MustChangePassword),
        }
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
        // A `SET_VAR` hint overlays the session BEFORE anything reads a
        // variable, which is where Go applies it too: the optimizer installs
        // it, and expression rewriting -- the `@@x` reads below -- happens
        // after.
        self.apply_set_var_hints(&stmt);
        self.bind_variables(&mut stmt)?;
        self.try_add_extra_limit(&mut stmt);
        // Only an allocating INSERT sets it; every other statement reports 0.
        self.statement_insert_id = 0;
        // Go sets the `InSelectStmt`/`In*Stmt` bits here, before execution,
        // so a statement that FAILS still classifies itself for the next
        // statement's `ROW_COUNT()` (captured: a failed SELECT leaves -1, a
        // failed INSERT leaves 0).
        self.statement_kind = statement_kind_of(&stmt);
        // With autocommit OFF a read or a write joins a transaction rather
        // than standing alone; DDL is left out because it commits the open
        // transaction instead of joining it.
        if self.statement_kind != StatementKind::Other {
            self.begin_implicit_transaction()?;
        }
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
                    let result = self.with_staged_catalog(|catalog| {
                        tidb_executor::run_insert_reporting(sql, catalog, &current_db, &ctx)
                    });
                    self.drain_eval_warnings(&ctx);
                    // Go `session.LastInsertID()`, the OK packet's field:
                    // `StmtCtx.LastInsertID` when the statement PUBLISHED an
                    // allocated id, `StmtCtx.InsertID` -- the last explicit
                    // value -- otherwise. Both come off the same context the
                    // publication above reads, so the wire and
                    // `LAST_INSERT_ID()` cannot drift apart: what differs is
                    // only the fallback Go itself applies.
                    //
                    // Captured from TiDB: an allocating insert reports the id
                    // on both; `INSERT INTO t (id,v) VALUES (50,2)` reports 50
                    // on the wire while `LAST_INSERT_ID()` stays where it was;
                    // an `INSERT IGNORE` whose only row is a duplicate burns
                    // an id but reports 0 on the wire.
                    // The publication itself is promoted at the statement
                    // boundary by `publish_statement_status`, off the same
                    // cell this reads -- one channel, two readers.
                    self.statement_insert_id = ctx
                        .published_last_insert_id()
                        .unwrap_or_else(|| ctx.given_insert_id());
                    let (affected, _) = result?;
                    Ok(StmtOutput::Affected(affected))
                }
                DmlStmt::Update(_) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(true);
                    let output = self.with_staged_catalog(|catalog| {
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
                    let output = self.with_staged_catalog(|catalog| {
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
                    let foreign_key_checks = self.foreign_key_checks();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_table_in(
                            sql,
                            catalog,
                            &current_db,
                            foreign_key_checks,
                        )?;
                        // MySQL answers DDL with a zero affected-row count.
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::CreateTable(create) => {
                    let current_db = self.current_db.clone();
                    let foreign_key_checks = self.foreign_key_checks();
                    let enable_check_constraint = self.enable_check_constraint();
                    // Go `pkg/ddl/create_table.go` and `add_column.go` warn
                    // once per CHECK constraint they discard, before the
                    // table is built; the constraint itself never reaches
                    // the stored `TableInfo`.
                    let discarded_checks = if enable_check_constraint {
                        0
                    } else {
                        tidb_executor::check_constraint_count(create)
                    };
                    let done = self.with_catalog_mut(|catalog| {
                        Ok(StmtOutput::Done(tidb_executor::run_create_table_in(
                            sql,
                            catalog,
                            &current_db,
                            foreign_key_checks,
                            enable_check_constraint,
                        )?))
                    });
                    if done.is_ok() {
                        for _ in 0..discarded_checks {
                            self.warnings.push(SqlWarning {
                                level: WarningLevel::Warning,
                                code: CHECK_CONSTRAINT_IS_OFF_CODE,
                                message: CHECK_CONSTRAINT_IS_OFF_MESSAGE.to_owned(),
                            });
                        }
                    }
                    done
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
                // Go answers every sequence DDL with a zero affected-row
                // count, as it does every other DDL.
                DdlStmt::CreateSequence(create) => {
                    let current_db = self.current_db.clone();
                    let create = create.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_create_sequence_in(&create, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::AlterSequence(alter) => {
                    let current_db = self.current_db.clone();
                    let alter = alter.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_alter_sequence_in(&alter, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::DropSequence(drop) => {
                    let current_db = self.current_db.clone();
                    let drop = drop.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_sequence_in(&drop, catalog, &current_db)?;
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

/// Go `ddl.errCheckConstraintIsOff` is built with `errors.NewNoStackError`, so
/// it carries no MySQL code of its own and `AppendWarning` files it under
/// `ER_UNKNOWN_ERROR`. Captured through testkit's `SHOW WARNINGS`:
/// `Warning | 1105 | tidb_enable_check_constraint is off`.
const CHECK_CONSTRAINT_IS_OFF_CODE: u16 = 1105;
/// See [`CHECK_CONSTRAINT_IS_OFF_CODE`]; the text is the variable name Go
/// interpolates, not a sentence, so it is reproduced verbatim.
const CHECK_CONSTRAINT_IS_OFF_MESSAGE: &str = "tidb_enable_check_constraint is off";

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

/// A TABLE-scope `GRANT`/`REVOKE` privilege list split into the two kinds of
/// row it writes: the `mysql.Tables_priv` privileges (those written without a
/// column list) and the `mysql.Columns_priv` ones. One statement may carry
/// both (captured: `GRANT SELECT, INSERT (a), UPDATE ON cg.t` writes a table
/// row of `SELECT,UPDATE` and a column row of `INSERT` on `a`, and
/// `SHOW GRANTS` prints them as two lines).
#[derive(Debug, Default)]
struct TableScopePrivs {
    /// The privileges written without a column list.
    table: Vec<privilege::GlobalPriv>,
    /// `(column, mask)` in the order the columns were first named, merged so
    /// that a column named by several privileges appears once.
    columns: Vec<(String, u64)>,
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
    ///
    /// Go `sessionexpr.getStmtTimestamp`: a `@@timestamp` left at its `0`
    /// default means the live clock, and any other value PINS the statement's
    /// whole time family (`NOW`, `CURDATE`, `UTC_TIMESTAMP`, ...) to that
    /// epoch instant. The split is `math.Modf` on a `float64`, kept here
    /// exactly: `SET timestamp = 1700000000.654321` really does land on
    /// 654320955ns, which is why the truncating readers report `.654320`
    /// while the rounding ones report `.654321`.
    fn statement_clock(&self, zone: &tidb_executor::SessionTimeZone) -> (i64, u32, i32) {
        use tidb_executor::SessionTimeZone;
        let pinned = self
            .vars
            .get_system("timestamp")
            .ok()
            .filter(|value| value != "0")
            .and_then(|value| value.parse::<f64>().ok());
        let utc = chrono::Utc::now();
        let (seconds, nanos) = match pinned {
            #[expect(clippy::cast_possible_truncation, reason = "Go's int64(seconds)")]
            #[expect(clippy::cast_sign_loss, reason = "@@timestamp's MinValue is 0")]
            Some(timestamp) => (
                timestamp.trunc() as i64,
                (timestamp.fract() * 1e9) as u32 % 1_000_000_000,
            ),
            None => (utc.timestamp(), utc.timestamp_subsec_nanos()),
        };
        let offset = match zone {
            SessionTimeZone::Fixed { offset_secs, .. } => *offset_secs,
            SessionTimeZone::Named(zone) => {
                use chrono::TimeZone;
                // A named zone's offset is a property of the INSTANT (DST), so
                // it has to be taken at the statement's own instant -- the
                // pinned one when `@@timestamp` fixes the clock.
                let at = chrono::DateTime::from_timestamp(seconds, nanos)
                    .unwrap_or(utc)
                    .naive_utc();
                chrono::Offset::fix(&zone.offset_from_utc_datetime(&at)).local_minus_utc()
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
    /// The sequences a statement of this session may read, over the catalog it
    /// sees (the transaction's working copy inside `BEGIN`).
    ///
    /// Only the NAMES are snapshotted: the allocators are `Arc` handles, so
    /// consuming a value through one moves the counter the catalog holds. That
    /// is deliberate and matches Go, where `NEXTVAL` allocates in its own meta
    /// transaction -- see `with_statement_stage`'s note about a storage whose
    /// clone shares a handle rather than copying by value.
    fn sequence_snapshot(&self) -> Rc<tidb_executor::SequenceSnapshot> {
        let by_name = match &self.txn {
            Some(txn) => txn.working.sequence_allocators(),
            None => match self.catalog.lock() {
                Ok(catalog) => catalog.sequence_allocators(),
                // A poisoned catalog is reported by the statement itself; an
                // empty map here just makes every name unknown.
                Err(_) => HashMap::new(),
            },
        };
        Rc::new(tidb_executor::SequenceSnapshot::new(
            by_name,
            &self.current_db,
            Rc::clone(&self.sequence_last_values),
        ))
    }

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
        let mode = self
            .vars
            .get_system("sql_mode")
            .unwrap_or_default()
            .to_ascii_uppercase();
        let has = |flag: &str| mode.split(',').any(|part| part.trim() == flag);
        // Go `GetDefaultWeekFormatMode` treats an unset or empty value as
        // "0"; `GetDivPrecisionIncrement` falls back to the default of 4.
        let week_format = self
            .vars
            .get_system("default_week_format")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let div_scale = self
            .vars
            .get_system("div_precision_increment")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(4);
        // Go `SessionVars.CTEMaxRecursionDepth`, the `WITH RECURSIVE` round
        // bound; the registry default is 1000.
        let cte_depth = self
            .vars
            .get_system("cte_max_recursion_depth")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(1000);
        if !is_dml {
            return tidb_executor::StmtContext::for_query()
                .with_cte_max_recursion_depth(cte_depth)
                .with_only_full_group_by(has("ONLY_FULL_GROUP_BY"))
                .with_session_state(current_db, version)
                .with_user(self.current_user.clone(), self.login_user.clone())
                .with_current_role(self.current_user.as_ref().map(|_| self.current_role_text()))
                .with_connection_id(self.connection_id)
                .with_rand_session(Rc::clone(&self.rand))
                .with_last_insert_id_channel(Rc::clone(&self.published_last_insert_id))
                .with_user_vars(Rc::clone(&self.user_vars))
                .with_previous_statement(self.last_insert_id, self.prev_row_count)
                .with_week_and_division_scale(week_format, div_scale)
                .with_sequences(self.sequence_snapshot())
                .with_clock(clock, zone);
        }
        tidb_executor::StmtContext::for_dml(
            has("ERROR_FOR_DIVISION_BY_ZERO"),
            has("STRICT_TRANS_TABLES") || has("STRICT_ALL_TABLES"),
        )
        .with_only_full_group_by(has("ONLY_FULL_GROUP_BY"))
        .with_session_state(current_db, version)
        .with_user(self.current_user.clone(), self.login_user.clone())
        .with_current_role(self.current_user.as_ref().map(|_| self.current_role_text()))
        .with_connection_id(self.connection_id)
        .with_rand_session(Rc::clone(&self.rand))
        .with_last_insert_id_channel(Rc::clone(&self.published_last_insert_id))
        .with_user_vars(Rc::clone(&self.user_vars))
        .with_previous_statement(self.last_insert_id, self.prev_row_count)
        .with_week_and_division_scale(week_format, div_scale)
        .with_sequences(self.sequence_snapshot())
        .with_clock(clock, zone)
        .with_auto_increment_step_default(self.auto_increment_step_is_default())
        .with_auto_increment_zero_explicit(has("NO_AUTO_VALUE_ON_ZERO"))
        .with_foreign_key_checks(self.foreign_key_checks())
        .with_cte_max_recursion_depth(cte_depth)
    }

    /// Go `SessionVars.ForeignKeyChecks`, read off `@@foreign_key_checks`.
    /// The registry stores a boolean as `ON`/`OFF`, and an unreadable value
    /// falls back to the ON default rather than silently disabling the
    /// checks.
    fn foreign_key_checks(&self) -> bool {
        !matches!(
            self.vars.get_system("foreign_key_checks").as_deref(),
            Ok("OFF") | Ok("off") | Ok("0")
        )
    }

    /// Go `vardef.EnableCheckConstraint`, which is a process-wide atomic that
    /// `SetGlobal` writes: the variable is GLOBAL-scope only, so the value a
    /// statement sees is the global one, not a session copy. The registry
    /// defaults it to OFF, and unlike `foreign_key_checks` the safe fallback
    /// for an unreadable value is OFF -- that is what a stock TiDB does and
    /// the only mode this engine models.
    fn enable_check_constraint(&self) -> bool {
        matches!(
            self.vars
                .get_global("tidb_enable_check_constraint")
                .as_deref(),
            Ok("ON") | Ok("on") | Ok("1")
        )
    }

    /// Whether `@@auto_increment_increment` and `@@auto_increment_offset` are
    /// both at their default of 1, which is the only step the allocator can
    /// answer; an insert into a table with an auto column is refused when
    /// they are not.
    fn auto_increment_step_is_default(&self) -> bool {
        ["auto_increment_increment", "auto_increment_offset"]
            .iter()
            .all(|name| self.vars.get_system(name).as_deref() == Ok("1"))
    }

    /// Go `ResetContextOfStmt`'s `Prev*` promotion, run at the statement
    /// boundary: what the statement just published becomes what the next one
    /// reads.
    ///
    /// This is the ONE place either value moves. `LAST_INSERT_ID()`,
    /// `@@last_insert_id`, `@@identity` and `ROW_COUNT()` all read the fields
    /// it writes, and the OK packet reads
    /// [`Session::statement_insert_id`]'s own fallback off the same
    /// publication -- so the function and the wire can differ only where Go
    /// itself makes them differ.
    fn publish_statement_status(&mut self, result: &Result<StmtOutput, DriverError>) {
        // The publication outlives a failing statement, exactly as Go's
        // `StmtCtx.LastInsertID` does: `SELECT LAST_INSERT_ID(17), bad()`
        // fails and still moves the id (captured).
        if let Some(published) = self.published_last_insert_id.get() {
            self.last_insert_id = published;
        }
        self.prev_row_count = match self.statement_kind {
            StatementKind::Select => -1,
            // Go reads `StmtCtx.AffectedRows()`, which a failed statement
            // leaves at whatever it managed to apply -- 0 for a statement
            // that never reached a row.
            StatementKind::Dml => match result {
                Ok(StmtOutput::Affected(rows)) => i64::try_from(*rows).unwrap_or(i64::MAX),
                _ => 0,
            },
            StatementKind::Other => 0,
        };
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

    /// Go `hint.go`'s `set_var` arm plus `optimize.go`'s application of
    /// `StmtHints.SetVars`: each `SET_VAR(name = value)` writes the session
    /// variable for the duration of THIS statement only, and where the same
    /// name appears twice the FIRST occurrence wins.
    ///
    /// The snapshot goes on [`Session::set_var_hint_restore`], which
    /// [`Session::run_with_columns`] puts back once the statement is over --
    /// so a statement that FAILS restores the overlay too, as Go's does.
    ///
    /// DEFERRED (documented): Go's two hint warnings. An unknown name is
    /// `ErrUnresolvedHintName` and a name whose registry entry is not
    /// `IsHintUpdatableVerified` is `ErrNotHintUpdatable` -- the second needs a
    /// registry field this tier's generated table does not carry. A name this
    /// registry rejects is skipped, which is the outcome Go reaches for an
    /// unknown name.
    fn apply_set_var_hints(&mut self, stmt: &Stmt) {
        let Stmt::Query(query) = stmt else { return };
        // Go attaches a statement's hints to its first SELECT, so a set
        // operation's hints are the first term's.
        let hints = match &**query {
            tidb_ast::QueryStmt::Select(select) => &select.hints,
            tidb_ast::QueryStmt::SetOpr(set_opr) => match set_opr.terms.first() {
                Some(term) => match &term.body {
                    tidb_ast::SetOprTermBody::Select(select) => &select.hints,
                    _ => return,
                },
                None => return,
            },
        };
        for hint in hints {
            let tidb_ast::HintKind::SetVar { var_name, value } = &hint.kind else {
                continue;
            };
            let name = var_name.to_ascii_lowercase();
            // The first hint for a name wins; a later one is ignored.
            if self
                .set_var_hint_restore
                .iter()
                .any(|(restored, _)| *restored == name)
            {
                continue;
            }
            let snapshot = self.vars.snapshot_system(&name);
            if self.vars.set_system(&name, value.clone()).is_ok() {
                self.set_var_hint_restore.extend(snapshot);
            }
        }
    }

    /// Go `preprocess.go:TryAddExtraLimit`: while `sql_select_limit` is not
    /// at its `MaxUint64` default, a SELECT or set operation that writes no
    /// LIMIT of its own is given one, so the variable caps the result the same
    /// way an explicit `LIMIT n` would. A statement that DOES write a LIMIT is
    /// left alone, even one asking for more rows than the cap.
    ///
    /// DEFERRED (documented): Go's `ShowStmt` arm, gated on `NeedLimitRSRow()`
    /// -- the subset of SHOW forms whose rows a LIMIT may cut -- and its
    /// `ExplainStmt` arm, which caps the wrapped statement rather than the
    /// EXPLAIN. `SELECT ... INTO OUTFILE` is excluded exactly as Go excludes
    /// it, even though this tier refuses that clause anyway.
    fn try_add_extra_limit(&self, stmt: &mut Stmt) {
        let cap = match self.vars.get_system("sql_select_limit") {
            Ok(value) => match value.parse::<u64>() {
                Ok(cap) if cap != u64::MAX => cap,
                _ => return,
            },
            Err(_) => return,
        };
        let limit = tidb_ast::Limit {
            offset: None,
            count: tidb_ast::Expr::Int(cap.to_string()),
        };
        if let Stmt::Query(query) = stmt {
            match &mut **query {
                tidb_ast::QueryStmt::Select(select) => {
                    if select.limit.is_none() && select.into_outfile.is_none() {
                        select.limit = Some(limit);
                    }
                }
                tidb_ast::QueryStmt::SetOpr(set_opr) => {
                    if set_opr.limit.is_none() {
                        set_opr.limit = Some(limit);
                    }
                }
            }
        }
    }

    /// Go `SessionVars.NoopFuncsMode`, read from this session's copy of
    /// `tidb_enable_noop_functions` or -- for a `SET GLOBAL` being validated
    /// -- from the shared table, which is the scope Go's `checkReadOnly`
    /// consults through `GlobalVarsAccessor`.
    fn noop_funcs_mode(&self, global: bool) -> NoopFuncsMode {
        let value = if global {
            self.vars.get_global("tidb_enable_noop_functions")
        } else {
            self.vars.get_system("tidb_enable_noop_functions")
        };
        match value
            .unwrap_or_else(|_| "OFF".to_owned())
            .to_ascii_uppercase()
            .as_str()
        {
            "ON" | "1" => NoopFuncsMode::On,
            "WARN" => NoopFuncsMode::Warn,
            _ => NoopFuncsMode::Off,
        }
    }

    /// Go `varsutil.go:checkReadOnly`, the `Validation` hook on the five
    /// `noop.go` read-only variables: turning one ON is refused with 1235
    /// unless `tidb_enable_noop_functions` allows it, because the server does
    /// not actually stop writes. Turning one OFF is always accepted, and so
    /// is a value the registry would reject -- that is
    /// [`vars::SessionSysvars::set_system`]'s job, and Go likewise validates
    /// the type before it runs this hook.
    fn check_read_only_noop(
        &mut self,
        name: &str,
        value: &str,
        is_global: bool,
    ) -> Result<(), DriverError> {
        let Some(clause) = sysvar::read_only_noop_clause(name) else {
            return Ok(());
        };
        let normalized = sysvar::get_sys_var(name)
            .and_then(|def| def.validate(value).ok())
            .map(|validated| validated.value);
        if normalized.as_deref() != Some("ON") {
            return Ok(());
        }
        match self.noop_funcs_mode(is_global) {
            NoopFuncsMode::On => Ok(()),
            NoopFuncsMode::Off => Err(DriverError::FunctionsNoopImpl(clause)),
            NoopFuncsMode::Warn => {
                self.warnings.push(SqlWarning {
                    level: WarningLevel::Warning,
                    code: 1235,
                    message: format!(
                        "function {clause} has only noop implementation in tidb now, use \
                         tidb_enable_noop_functions to enable these functions"
                    ),
                });
                Ok(())
            }
        }
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
        let mode = self.noop_funcs_mode(false);
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
mod tests_auto_increment;
#[cfg(test)]
mod tests_charset;
#[cfg(test)]
mod tests_coalesced_joins;
#[cfg(test)]
mod tests_collation;
#[cfg(test)]
mod tests_column_prune;
#[cfg(test)]
mod tests_core;
#[cfg(test)]
mod tests_dml_lock_keys;
#[cfg(test)]
mod tests_eval_bool;
#[cfg(test)]
mod tests_explain;
#[cfg(test)]
mod tests_foreign_key;
#[cfg(test)]
mod tests_global_vars;
#[cfg(test)]
mod tests_grants;
#[cfg(test)]
mod tests_harvested_relation_engine;
#[cfg(test)]
mod tests_json;
#[cfg(test)]
mod tests_multi_table_dml;
#[cfg(test)]
mod tests_recursive_cte;
#[cfg(test)]
mod tests_savepoint;
mod tests_sequence;
#[cfg(test)]
mod tests_show;
#[cfg(test)]
mod tests_statement_rollback;
mod tests_subquery;
#[cfg(test)]
mod tests_support;
#[cfg(test)]
mod tests_user_vars;
#[cfg(test)]
mod tests_views;
#[cfg(test)]
mod tests_window;
