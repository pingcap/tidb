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
//! executors do; the live privilege cache reloads after the commit. A
//! `SET GLOBAL` runs against a scratch copy of the global sysvar table read
//! inside the change's own transaction, so a statement that fails validation
//! never reaches storage or the live copy.

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

/// One `SET GLOBAL` in flight. Dropping it abandons the change.
pub trait PendingSysvarChange {
    /// The scratch global table the statement runs against.
    fn table(&self) -> crate::GlobalSysvars;
    /// Persists and publishes the change; answers the variables it touched.
    fn commit(self: Box<Self>) -> Result<Vec<String>, DriverError>;
}

impl Session {
    /// Runs one account statement through `service`: the driver's own
    /// validation and errors against the scratch registry, then the commit.
    /// The caller has committed any open transaction first, as Go's
    /// implicit commit before such a statement requires.
    pub fn run_account_statement_with(
        &mut self,
        sql: &str,
        service: &dyn AccountService,
    ) -> Result<(), DriverError> {
        // Go's account executors take a system session, run their SQL in
        // one pessimistic transaction, commit, then `NotifyUpdatePrivilege`.
        let scratch = service.load_accounts()?;
        let mut system = service.system_session()?;
        system.execute("BEGIN PESSIMISTIC")?;
        let live = self.swap_privileges(scratch);
        self.account_statement_privilege_cache = live.clone();
        self.account_sql = Some(system);
        let applied = self.run(sql);
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
        if let Err(error) = applied {
            // Go `defer` rolls the internal transaction back on any error.
            let _ = system.execute("ROLLBACK");
            return Err(error);
        }
        system.execute("COMMIT")?;
        service.notify_update_privilege()
    }

    /// Runs one `SET GLOBAL` through `service`. An open transaction stays
    /// open: Go writes `mysql.global_variables` through an internal session,
    /// so the statement neither commits it nor loses `@@tidb_current_ts`.
    pub fn run_global_var_statement_with(
        &mut self,
        sql: &str,
        service: &dyn SysvarService,
    ) -> Result<Vec<String>, DriverError> {
        let pending = service.begin()?;
        let before = pending.table().overrides();
        let live = self.swap_globals(pending.table());
        let applied = self.apply_set(sql);
        // A session-scoped assignment mixed into the same `SET` must still
        // land on the connection's own live-seeded copies.
        self.swap_globals(live);
        // Go's SetExecutor routes every failure through `handleErr`: after a
        // failed SET both error_count and warning_count read 1.
        if let Err(error) = applied {
            // Go's SetExecutor persists each assignment as it runs, so the
            // ones before the failing one stay: the scratch table holds
            // exactly those, and its diff is what Go had written.
            if pending.table().overrides() != before {
                pending.commit()?;
            }
            let error = error.to_mysql_error();
            self.record_ddl_failure(error.code, error.message.clone());
            return Err(DriverError::Mysql(error));
        }
        pending.commit()
    }
}
