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

//! The account and global-variable halves of Milestone 3's seam: like
//! [`crate::ddl_service`], the session owns what Go's executors decide.
//!
//! An account statement validates against the stored accounts and writes its
//! own rows as SQL on a system session, as Go's `simple.go` / `grant.go`
//! executors do; the live privilege cache reloads after the commit. Each
//! GLOBAL assignment of a `SET` runs against a scratch copy of the global
//! sysvar table read inside its own change
//! (`Session::set_node_wide_sys_var`), so an assignment that fails
//! validation never reaches storage or the live copy.

use crate::*;

/// What an account statement's executor reaches outside the session, as Go's
/// `executeCreateUser` and the other account executors do: the accounts it
/// validates against, the system session its `mysql.*` SQL runs on, and the
/// privilege reload after it commits.
pub trait AccountService {
    /// The stored accounts as of now, which the statement validates against
    /// and applies its meaning to before its rows are written.
    fn load_accounts(&self) -> Result<crate::privilege::PrivilegeRegistry, DriverError>;
    /// Go `GetSysSession` from the domain's pool: the session the statement's
    /// `BEGIN PESSIMISTIC` .. `COMMIT` and row writes run on.
    fn system_session(&self) -> Result<Box<dyn crate::user_table::AccountSqlSession>, DriverError>;
    /// Go `Domain.NotifyUpdatePrivilege` after the statement committed: the
    /// live privilege cache reloads from the tables and peers are told.
    fn notify_update_privilege(&self) -> Result<(), DriverError>;
}

/// Persists `SET GLOBAL` (Go's `SetGlobalSysVar` writing
/// `mysql.global_variables` through an internal session).
pub trait SysvarService {
    /// Opens one change: the stored overrides as of its own snapshot.
    fn begin(&self) -> Result<Box<dyn PendingSysvarChange>, DriverError>;
}

/// One GLOBAL assignment in flight. Dropping it abandons the change.
pub trait PendingSysvarChange {
    /// The scratch global table the assignment runs against.
    fn table(&self) -> crate::GlobalSysvars;
    /// Persists and publishes the change; answers the variables it touched.
    fn commit(self: Box<Self>) -> Result<Vec<String>, DriverError>;
}

impl Session {
    /// Go's account executors (`SimpleExec`'s user, role and password arms,
    /// `GrantExec`, `RevokeExec`) over the domain's privilege tables: the
    /// executor takes a system session, runs its SQL in one pessimistic
    /// transaction against the stored accounts, commits, then
    /// `NotifyUpdatePrivilege`. `Compile` has checked the statement's
    /// privileges against the live cache before this swaps the stored
    /// accounts in, and the executor's own checks read the live cache
    /// (`account_statement_privilege_cache`). Without domain services (an
    /// in-process session) the executor runs on the session's own registry.
    pub(crate) fn with_account_service<T>(
        &mut self,
        run: impl FnOnce(&mut Session) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        let Some(service) = self
            .domain_services
            .as_ref()
            .map(|services| std::sync::Arc::clone(&services.accounts))
        else {
            return run(self);
        };
        let scratch = service.load_accounts()?;
        let mut system = service.system_session()?;
        system.execute("BEGIN PESSIMISTIC")?;
        let live = self.swap_privileges(scratch);
        self.account_statement_privilege_cache = live.clone();
        self.account_sql = Some(system);
        let applied = run(self);
        let mut system = self
            .account_sql
            .take()
            .expect("the system session stays installed for the statement");
        self.account_statement_privilege_cache = None;
        // Restoring the live table is unconditional: a statement that failed
        // must not leave the connection reading the scratch copy.
        if let Some(live) = live {
            self.swap_privileges(live);
        }
        let output = match applied {
            Ok(output) => output,
            Err(error) => {
                // Go `defer` rolls the internal transaction back on any error.
                let _ = system.execute("ROLLBACK");
                return Err(error);
            }
        };
        system.execute("COMMIT")?;
        service.notify_update_privilege()?;
        Ok(output)
    }
}
