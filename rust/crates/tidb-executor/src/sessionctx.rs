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

//! Go `sessionctx.Context` as the executors of this crate reach the session:
//! the session variables and the session's privilege manager. It grows with
//! the executors that move here from the session.

use tidb_variable::SessionVars;

/// The result of running one statement, with wire-facing column metadata.
///
/// `StmtResult::Rows` loses column names/types; a server front end needs one
/// `(name, type)` per result column to build protocol column definitions, so
/// `Session::run_with_columns` returns this richer shape instead.
#[derive(Debug, PartialEq)]
pub enum StmtOutput {
    /// A query's result columns and rows.
    Rows {
        /// One `(display name, field type)` per output column.
        columns: Vec<(String, tidb_datatype::FieldType)>,
        /// The result rows (one `Datum` per column).
        rows: Vec<Vec<tidb_datatype::Datum>>,
    },
    /// A DML statement's affected-row count.
    Affected(u64),
    /// A DDL statement completed (`false` = `IF NOT EXISTS` no-op).
    Done(bool),
}

/// Go `sessionctx.Context`.
pub trait SessionContext {
    /// Go `GetSessionVars`.
    fn session_vars(&self) -> &SessionVars;
    /// Go `GetSessionVars`, for an executor that changes them.
    fn session_vars_mut(&mut self) -> &mut SessionVars;
    /// Go `privilege.GetPrivilegeManager(ctx)`.
    fn privilege_manager(&self) -> &dyn PrivilegeManager;
    /// Whether the session runs with no privilege manager to consult (Go's
    /// internal sessions): role statements change nothing.
    fn privilege_checks_bypassed(&self) -> bool;
    /// Whether a privilege registry backs [`Self::privilege_manager`].
    fn has_privilege_registry(&self) -> bool;
    /// Go `SessionVars.UserVars`.
    fn user_vars(&self) -> &tidb_expr::user_vars::UserVars;
    /// Go `SessionVars.StmtCtx.AppendWarning` / `AppendNote`.
    fn append_sql_warning(&mut self, warning: tidb_context::SqlWarn);
    /// Go `SessionVars.InTxn`.
    fn in_txn(&self) -> bool;
    /// Go `SessionVars.TxnCtx.IsStaleness`.
    fn txn_is_staleness(&self) -> bool;
    /// Go `SessionVars.SetSystemVar`: validate and store a SESSION value,
    /// running the variable's `Validation` and `SetSession` hooks.
    fn set_system_var(&mut self, name: &str, value: String) -> Result<(), crate::DriverError>;
    /// Go `GlobalVarsAccessor.SetGlobalSysVar`: validate and store a GLOBAL
    /// value, running the variable's hooks and notifying the domain.
    fn set_global_sys_var(&mut self, name: &str, value: String)
        -> Result<(), crate::DriverError>;
    /// Go `GlobalVarsAccessor.SetInstanceSysVar`.
    fn set_instance_sys_var(
        &mut self,
        name: &str,
        value: String,
    ) -> Result<(), crate::DriverError>;
    /// Go `SetExecutor`'s tail for `tidb_snapshot` / `tx_read_ts`:
    /// `ValidateSnapshotReadTS`, `gcutil.ValidateSnapshot` and
    /// `loadSnapshotInfoSchemaIfNeeded` for the timestamp just set, restoring
    /// `old_ts` when any of them fails.
    fn load_snapshot_info_schema_if_needed(
        &mut self,
        old_ts: u64,
        is_txn_read_ts: bool,
    ) -> Result<(), crate::DriverError>;
}

/// Go `privilege.Manager`, bound to the session's user and active roles as
/// Go's `UserPrivileges` is.
pub trait PrivilegeManager {
    /// Go `DBIsVisible(activeRoles, db)`; a session with no user sees every
    /// schema.
    fn db_is_visible(&self, database: &str) -> bool;
    /// Go `RequestDynamicVerification(activeRoles, privName, grantable)`;
    /// a session with no user passes.
    fn request_dynamic_verification(&self, privilege: &str, grantable: bool) -> bool;
    /// Go `RequestVerification(activeRoles, db, table, column, priv)`: whether
    /// the session holds any privilege of `privilege` on the object; a
    /// session with no user passes.
    fn request_verification(
        &self,
        database: &str,
        table: &str,
        privilege: tidb_mysql::privilege::PrivilegeType,
    ) -> bool;
    /// Go `GetAllRoles(user, host)`: the roles granted to the account.
    fn get_all_roles(&self, user: &str, host: &str) -> Vec<(String, String)>;
    /// Go `GetDefaultRoles(ctx, user, host)`: the account's default roles.
    fn get_default_roles(&self, user: &str, host: &str) -> Vec<(String, String)>;
    /// Go `FindEdge(ctx, role, user)`: whether `role` is granted to `user`,
    /// as the privilege cache has it.
    fn find_edge(&self, role: &(String, String), user: &(String, String)) -> bool;
}

/// Go `infoschema.SysVarHiddenForSem`: under SEM an invisible variable is
/// hidden from a session without `RESTRICTED_VARIABLES_ADMIN`.
pub fn sys_var_hidden_for_sem(ctx: &dyn SessionContext, name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    tidb_sem_compat::is_enabled()
        && tidb_sem_compat::is_invisible_sys_var(&name)
        && !ctx
            .privilege_manager()
            .request_dynamic_verification("RESTRICTED_VARIABLES_ADMIN", false)
}

/// The statement classes `ROW_COUNT()` distinguishes.
///
/// Go spells this as four independent `StmtCtx` bits (`InSelectStmt`,
/// `InInsertStmt`, `InUpdateStmt`, `InDeleteStmt`) and reads them in one
/// if/else chain in `ResetContextOfStmt` (`pkg/executor/select.go:1229-1237`);
/// one enum says the same thing without letting two of them be true at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementKind {
    /// A SELECT or set operation, after which `ROW_COUNT()` is `-1`.
    Select,
    /// INSERT/REPLACE/UPDATE/DELETE, after which `ROW_COUNT()` is the
    /// affected-row count.
    Dml,
    /// Everything else -- DDL, SHOW, SET, transaction control -- after which
    /// `ROW_COUNT()` is `0`.
    Other,
}

/// Classifies a parsed statement for `ROW_COUNT()` -- a `WITH` prefix belongs
/// to the mutation, so `WITH x AS (...) DELETE ...` still sets
/// `InDeleteStmt` -- unwrapping an EXPLAIN,
/// which `ResetContextOfStmt` replaces by its target before setting the bits
/// (`EXPLAIN SELECT` leaves `ROW_COUNT()` at -1).
pub fn statement_kind_of(stmt: &tidb_ast::Stmt) -> StatementKind {
    fn dml_kind(dml: &tidb_ast::DmlStmt) -> StatementKind {
        match dml {
            tidb_ast::DmlStmt::Insert(_)
            | tidb_ast::DmlStmt::Update(_)
            | tidb_ast::DmlStmt::Delete(_) => StatementKind::Dml,
            _ => StatementKind::Other,
        }
    }
    match stmt {
        tidb_ast::Stmt::Query(_) => StatementKind::Select,
        tidb_ast::Stmt::Dml(dml) => dml_kind(dml),
        tidb_ast::Stmt::Admin(admin) => match &**admin {
            tidb_ast::AdminStmt::Explain(explain) => explain
                .statement()
                .map_or(StatementKind::Other, statement_kind_of),
            _ => StatementKind::Other,
        },
        _ => StatementKind::Other,
    }
}

/// Go `ResetContextOfStmt`'s `sc.Priority` assignment: the statement's own
/// `LOW_PRIORITY`/`HIGH_PRIORITY`/`DELAYED` modifier. SELECT reads it from
/// `SelectStmtOpts`, UPDATE/DELETE from `ResetUpdateStmtCtx`/`ResetDeleteStmtCtx`,
/// and INSERT from its own arm; every other statement leaves the zero value.
/// A `WITH` prefix is unwrapped because Go sets the priority on the mutation
/// the CTE belongs to.
pub fn statement_priority_of(stmt: &tidb_ast::Stmt) -> tidb_ast::StatementPriority {
    fn dml_priority(dml: &tidb_ast::DmlStmt) -> tidb_ast::StatementPriority {
        match dml {
            tidb_ast::DmlStmt::Insert(insert) => insert.priority,
            tidb_ast::DmlStmt::Update(update) => update.priority,
            tidb_ast::DmlStmt::Delete(delete) => delete.priority,
            // Go's `*ast.LoadDataStmt` arm has a dedicated `LowPriority` word
            // rather than the shared `stmt.Priority` field.
            tidb_ast::DmlStmt::LoadData(load) if load.low_priority => {
                tidb_ast::StatementPriority::Low
            }
            _ => tidb_ast::StatementPriority::None,
        }
    }
    match stmt {
        tidb_ast::Stmt::Query(query) => match &**query {
            tidb_ast::QueryStmt::Select(select) => select.priority,
            tidb_ast::QueryStmt::SetOpr(_) => tidb_ast::StatementPriority::None,
        },
        tidb_ast::Stmt::Dml(dml) => dml_priority(dml),
        _ => tidb_ast::StatementPriority::None,
    }
}

/// Go `ResetContextOfStmt`'s `sc.NotFillCache = !SelectStmtOpts.SQLCache`.
/// Only a SELECT carries the modifier; every other statement leaves it false.
pub fn statement_not_fill_cache(stmt: &tidb_ast::Stmt) -> bool {
    matches!(
        stmt,
        tidb_ast::Stmt::Query(query) if matches!(&**query, tidb_ast::QueryStmt::Select(select) if select.sql_no_cache)
    )
}
