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
