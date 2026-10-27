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
    fn append_sql_warning(&mut self, warning: tidb_util::context::SqlWarn);
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
    tidb_util::sem_compat::is_enabled()
        && tidb_util::sem_compat::is_invisible_sys_var(&name)
        && !ctx
            .privilege_manager()
            .request_dynamic_verification("RESTRICTED_VARIABLES_ADMIN", false)
}
