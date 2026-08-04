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
use std::sync::{Arc, Mutex};

use tidb_ast::Stmt;
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
    /// Go `StatementContext.InShowWarning`: set for exactly the statements
    /// that inherit the buffer, and the reason `WarningCount()` reports 0 for
    /// them. See [`Session::wire_warning_count`].
    in_show_warning: bool,
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
    /// Go `SessionVars.RetryInfo`'s auto-increment half: the ids the statement
    /// running now has assigned, kept across a write-conflict replay so the
    /// replay writes the ids the losing attempt picked. It lives on the
    /// session because the retry loop is above the statement -- each attempt
    /// builds its own `StmtContext`, and this is the one thing that has to
    /// cross between them. See `tidb_executor::RetryAutoIds`.
    retry_auto_ids: Rc<RefCell<tidb_executor::RetryAutoIds>>,
    /// The session's non-prepared plan cache
    /// (`tidb_enable_non_prepared_plan_cache`). See
    /// [`non_prepared_plan_cache`] for what it does and does not store.
    non_prepared_plan_cache: non_prepared_plan_cache::NonPreparedPlanCache,
    /// Go `SessionVars.FoundInPlanCache`: whether the statement RUNNING now
    /// found its plan in the cache. Reset for every statement.
    found_in_plan_cache: bool,
    /// Go `SessionVars.PrevFoundInPlanCache`, which is what
    /// `@@last_plan_from_cache` reads -- the PRECEDING statement's value,
    /// promoted at the statement boundary, since the reading `SELECT` is
    /// itself never cacheable and would otherwise always answer 0.
    prev_found_in_plan_cache: bool,
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
    /// Go `SessionVars.PreparedStmtNameToID` / `PreparedStmts`: the SQL-level
    /// prepared statements this session holds. Per-session and not shared: a
    /// peer over the same catalog holds its own.
    prepared_statements: prepared_statements::PreparedStore,
    /// Go's `sessionBindingHandle` (`pkg/bindinfo/session_handle.go`): the
    /// SQL bindings created with `CREATE [SESSION] BINDING`. Session-scoped
    /// and unshared, exactly as Go's is; GLOBAL bindings would need
    /// `mysql.bind_info`, which this tier has no catalog entry for. See
    /// [`binding`].
    session_bindings: binding::SessionBindings,
    /// Go `SessionVars.FoundInBinding`: whether the statement RUNNING now
    /// took its hints from a binding.
    found_in_binding: bool,
    /// Go `SessionVars.PrevFoundInBinding`, which is what
    /// `@@last_plan_from_binding` reads -- the PRECEDING statement's value,
    /// promoted at the statement boundary for the same reason
    /// `@@last_plan_from_cache` is.
    prev_found_in_binding: bool,
}

impl Default for Session {
    /// A session on its own empty catalog, with `test` selected as a fresh
    /// TiDB connection has.
    ///
    /// This is the ONE place a [`Session`] is built. Everything a front end
    /// installs afterwards -- a shared catalog, an identity, a process
    /// registration, a privilege registry, a globals table -- arrives through a
    /// setter or through struct-update over this, so a field added to `Session`
    /// has exactly one place that must name it.
    fn default() -> Self {
        Session {
            catalog: SharedCatalog::default(),
            txn: None,
            vars: SessionVars::new(),
            warnings: Vec::new(),
            in_show_warning: false,
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
            retry_auto_ids: Rc::default(),
            non_prepared_plan_cache: non_prepared_plan_cache::NonPreparedPlanCache::default(),
            found_in_plan_cache: false,
            prev_found_in_plan_cache: false,
            user_vars: Rc::default(),
            sequence_last_values: Rc::default(),
            current_db: DEFAULT_DATABASE.to_owned(),
            process: None,
            has_process_priv: false,
            privileges: None,
            sandbox_mode: false,
            rand: new_time_seeded_rand(),
            prepared_statements: prepared_statements::PreparedStore::default(),
            session_bindings: binding::SessionBindings::default(),
            found_in_binding: false,
            prev_found_in_binding: false,
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

pub use tidb_executor::TxnErrorKind;

mod account;
mod admin_check_arm;
mod analyze_arm;
mod binding;
mod binding_arm;
mod classify;
mod dispatch;
mod explain_arm;
mod identity;
pub mod infoschema;
mod non_prepared_plan_cache;
mod noop;
mod prepared_statements;
mod stmt_ctx;
mod table_privilege;
mod txn;
mod variables;
mod warnings;
pub(crate) use classify::{statement_kind_of, StatementKind};
pub use classify::{StmtKind, StoredStateChange};
pub(crate) use txn::Transaction;
pub(crate) use variables::datum_text;
pub use warnings::{SqlWarning, WarningLevel};
pub(crate) use warnings::{CHECK_CONSTRAINT_IS_OFF_CODE, CHECK_CONSTRAINT_IS_OFF_MESSAGE};
pub mod privilege;
pub mod process;
mod process_arm;
mod show;
pub mod sysvar;
pub mod vars;
pub use vars::{GlobalSysvars, SessionVars, VarError};

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

    /// Selects NO schema, which is the state a connection that authenticated
    /// without an initial database is in: Go's `SessionVars.CurrentDB` is
    /// empty and every unqualified name is `ErrNoDB` (`Error 1046`) until a
    /// `USE` runs.
    ///
    /// [`Session::default`] selects `test` because that is what a `mysql`
    /// client's own default gives a fresh connection; a front end whose
    /// handshake carried no schema at all -- or a harness replaying one --
    /// needs to say so, and this is the one way to.
    pub fn deselect_database(&mut self) {
        self.current_db = String::new();
    }

    /// Go `executeUse`: an unknown schema is `ErrDatabaseNotExists`, and the
    /// switch also updates `collation_database`.
    fn use_database(&mut self, name: &str) -> Result<(), DriverError> {
        // Go `executeUse` (`executor/simple.go` around line 608) refuses an
        // invisible schema with 1044 BEFORE it looks the schema up, so a
        // schema the account cannot see is never distinguishable from one
        // that does not exist.
        self.require_visible_database(name)?;
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

    /// The auto-increment ids the running statement has assigned, which a
    /// caller that RUNS THE STATEMENT AGAIN rewinds between attempts and
    /// clears when the statement is finally over. See
    /// `tidb_executor::RetryAutoIds`.
    #[must_use]
    pub fn retry_auto_ids(&self) -> &Rc<RefCell<tidb_executor::RetryAutoIds>> {
        &self.retry_auto_ids
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

    /// A session sharing `catalog` with its peers.
    ///
    /// Every other field comes from [`Session::default`], which is the crate's
    /// one `Session` construction site -- see its doc.
    #[must_use]
    pub fn with_catalog(catalog: SharedCatalog) -> Self {
        Session {
            catalog,
            ..Session::default()
        }
    }

    /// The shared catalog handle, for opening a peer session over the same
    /// schema state.
    #[must_use]
    pub fn shared_catalog(&self) -> SharedCatalog {
        Arc::clone(&self.catalog)
    }

    /// The number of `?` markers a statement carries, which
    /// `COM_STMT_PREPARE` reports to the client.
    pub fn parameter_count(&self, sql: &str) -> Result<usize, DriverError> {
        tidb_executor::parameter_count(sql, self.scanner_sql_mode())
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
        let bound = tidb_executor::bind_parameters(sql, params, self.scanner_sql_mode())?;
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
        // Go promotes `FoundInPlanCache` into `PrevFoundInPlanCache` in
        // `ResetContextOfStmt`, at the same boundary as the other `Prev*`
        // fields above -- which is why `select @@last_plan_from_cache`
        // reports the PRECEDING statement rather than itself.
        self.prev_found_in_plan_cache = std::mem::take(&mut self.found_in_plan_cache);
        // Go promotes `FoundInBinding` at the same boundary, which is why
        // `select @@last_plan_from_binding` reports the statement BEFORE it
        // rather than itself (that SELECT matches no binding of its own).
        self.prev_found_in_binding = std::mem::take(&mut self.found_in_binding);
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
            self.append_warning(WarningLevel::Error, reported.code, reported.message);
        }
        result
    }
}

#[cfg(test)]
mod tests_admin_check;
#[cfg(test)]
mod tests_alter_column;
#[cfg(test)]
mod tests_analyze;
#[cfg(test)]
mod tests_auto_increment;
#[cfg(test)]
mod tests_bad_null;
#[cfg(test)]
mod tests_binding;
#[cfg(test)]
mod tests_cast_int_truncation;
#[cfg(test)]
mod tests_charset;
#[cfg(test)]
mod tests_coalesced_joins;
#[cfg(test)]
mod tests_collation;
#[cfg(test)]
mod tests_column_defaults;
#[cfg(test)]
mod tests_column_prune;
#[cfg(test)]
mod tests_compare_refinement;
#[cfg(test)]
mod tests_core;
#[cfg(test)]
mod tests_dml_lock_keys;
#[cfg(test)]
mod tests_eval_bool;
#[cfg(test)]
mod tests_explain;
#[cfg(test)]
mod tests_explain_derived;
mod tests_explain_merge_join;
mod tests_expression_indexes;
#[cfg(test)]
mod tests_foreign_key;
mod tests_generated_columns;
#[cfg(test)]
mod tests_global_vars;
#[cfg(test)]
mod tests_grants;
#[cfg(test)]
mod tests_harvested_relation_engine;
#[cfg(test)]
mod tests_in_list_full_evaluation;
#[cfg(test)]
mod tests_index_hints;
mod tests_index_key_length;
#[cfg(test)]
mod tests_join_predicate_placement;
#[cfg(test)]
mod tests_json;
#[cfg(test)]
mod tests_mem_quota;
mod tests_multi_table_dml;
#[cfg(test)]
mod tests_non_prepared_plan_cache;
#[cfg(test)]
mod tests_partition;
#[cfg(test)]
mod tests_prepared_statements;
#[cfg(test)]
mod tests_read_cast;
#[cfg(test)]
mod tests_recursive_cte;
#[cfg(test)]
mod tests_savepoint;
mod tests_sequence;
#[cfg(test)]
mod tests_show;
#[cfg(test)]
mod tests_sql_mode_scanner;
#[cfg(test)]
mod tests_statement_rollback;
mod tests_subquery;
#[cfg(test)]
mod tests_support;
#[cfg(test)]
mod tests_sysbench_access;
#[cfg(test)]
mod tests_system_schemas;
#[cfg(test)]
mod tests_timestamp_range;
#[cfg(test)]
mod tests_timezone_storage;
#[cfg(test)]
mod tests_topn;
#[cfg(test)]
mod tests_user_vars;
#[cfg(test)]
mod tests_views;
#[cfg(test)]
mod tests_window;
#[cfg(test)]
mod tests_write_conversion;
#[cfg(test)]
mod tests_zero_date;
