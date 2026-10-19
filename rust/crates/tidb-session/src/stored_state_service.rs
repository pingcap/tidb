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
//! [`crate::ddl_service`], the session owns what Go's executors decide, and
//! a service the caller supplies persists the outcome. An account statement
//! or a `SET GLOBAL` runs against a scratch copy (the account registry, the
//! global sysvar table) read inside the change's own transaction, so a
//! statement that fails validation never reaches storage or the live copy.

use crate::*;

/// Persists account statements (Go's executors writing `mysql.user` and the
/// privilege tables through an internal session).
pub trait AccountService {
    /// Opens one change: the stored accounts as of its own snapshot.
    fn begin(&self) -> Result<Box<dyn PendingAccountChange>, DriverError>;
}

/// One account statement in flight. Dropping it abandons the change.
pub trait PendingAccountChange {
    /// The scratch account table the statement runs against.
    fn registry(&self) -> crate::privilege::PrivilegeRegistry;
    /// What Go writes that the account image alone cannot say.
    fn set_write_hints(&mut self, hints: tidb_exec::cluster_account_write::AccountWriteHints);
    /// Persists and publishes the change; answers the accounts it touched.
    fn commit(self: Box<Self>) -> Result<Vec<String>, DriverError>;
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
    ) -> Result<Vec<String>, DriverError> {
        let mut pending = service.begin()?;
        pending.set_write_hints(self.account_write_hints(sql));
        let live = self.swap_privileges(pending.registry());
        let applied = self.run_with_delegated_account_storage(sql);
        // Restoring the live table is unconditional: a statement that failed
        // must not leave the connection reading the scratch copy.
        if let Some(live) = live {
            self.swap_privileges(live);
        }
        applied?;
        pending.commit()
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

    /// What Go's executor for this account statement writes that the account
    /// image alone cannot say: a GRANT stamps the session user as `Grantor`,
    /// and `SET DEFAULT ROLE` deletes and re-inserts the account's rows.
    pub fn account_write_hints(
        &self,
        sql: &str,
    ) -> tidb_exec::cluster_account_write::AccountWriteHints {
        let mut hints = tidb_exec::cluster_account_write::AccountWriteHints::default();
        let Ok(stmt) = self.parse_statement(sql) else {
            return hints;
        };
        match &stmt {
            tidb_ast::Stmt::Admin(admin)
                if matches!(admin.as_ref(), tidb_ast::AdminStmt::Grant(_)) =>
            {
                hints.grantor = Some(
                    self.current_identity()
                        .map_or_else(String::new, |(user, host)| format!("{user}@{host}")),
                );
            }
            tidb_ast::Stmt::Session(session)
                if matches!(session.as_ref(), tidb_ast::SessionStmt::SetDefaultRole(_)) =>
            {
                hints.rewrite_default_roles = true;
            }
            _ => {}
        }
        hints
    }
}
