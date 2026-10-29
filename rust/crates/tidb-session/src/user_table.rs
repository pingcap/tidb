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

//! `mysql.user` as a REAL table: the internal DML every account statement
//! runs against it, transcreating the SQL Go's `executor/simple.go`
//! (`executeCreateUser` / `executeAlterUser` / `executeSetPwd` /
//! `executeDropUser` / `executeRenameUser`) and `executor/grant.go` /
//! `executor/revoke.go` issue through their system session.
//!
//! # Where the statement's rows go
//!
//! On a node, as in Go, `mysql.user` IS the storage: an account statement
//! validates against the stored accounts, writes these rows on a system
//! session (an installed [`AccountSqlSession`]) inside its own pessimistic
//! transaction, and after the commit the node's live privilege cache reloads
//! from the tables (Go `NotifyUpdatePrivilege`). A session without one writes
//! the rows into its own catalog through [`Session::run_internal_write`].
//!
//! Consequences kept deliberately Go-shaped:
//!  * a session whose catalog was never bootstrapped (no `mysql.user` table)
//!    skips the rows -- reads there keep answering `UnknownTable`;
//!  * direct DML against `mysql.user` executes (it is a real table) but does
//!    not reach the privilege cache until `FLUSH PRIVILEGES`, Go's own
//!    behavior.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use crate::*;

/// Go `sysSession.GetSQLExecutor()` as an account executor uses it: the
/// system session its `mysql.*` SQL runs on, inside the statement's own
/// `BEGIN PESSIMISTIC` .. `COMMIT` (Go `executeCreateUser` and friends).
pub trait AccountSqlSession: Send {
    /// Runs one statement and answers its rows (none for a write).
    fn execute(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError>;
}

pub(crate) use tidb_executor::simple::account_tables::{datum_text, sql_str};

impl Session {
    /// Whether the shared catalog holds `mysql.user`, which a bootstrapped
    /// store's does.
    pub(crate) fn user_table_present(&mut self) -> bool {
        if self.account_sql.is_some() {
            return true;
        }
        self.with_catalog_mut(|catalog| Ok(catalog.contains_in("mysql", "user")))
            .unwrap_or(false)
    }

    /// Runs one internal `INSERT`/`UPDATE`/`DELETE` against the `mysql`
    /// schema, the way Go's account executors run theirs through the system
    /// session's `sqlExecutor`. A failure fails the statement, as Go's does;
    /// callers only reach this once [`Self::user_table_present`] said the
    /// table exists, so an error here is a real defect rather than a missing
    /// bootstrap.
    pub(crate) fn run_user_table_write(&mut self, sql: &str) -> Result<(), DriverError> {
        if let Some(system) = self.account_sql.as_mut() {
            return system.execute(sql).map(|_| ());
        }
        self.run_internal_write(sql).map(|_| ())
    }

    /// One statement of the internal SQL Go's handlers run against the
    /// `mysql` tables (`util.Exec`), through the session's own DML context:
    /// it carries the statement clock (`current_timestamp()` and column
    /// defaults evaluate NOW), the session time zone, and the JSON
    /// machinery's sql-mode inputs, and its system variables bind as a user
    /// statement's do (Go's internal session plans the same way, so
    /// `@@TIME_ZONE` in an assignment reads the session value). Answers an
    /// INSERT's first allocated id (`LAST_INSERT_ID()`).
    ///
    /// The memory budget is not the user statement's: Go runs this SQL on a
    /// pooled system session (`ExecRestrictedSQL`), whose
    /// `tidb_mem_quota_query` is the default, so a connection that lowered
    /// its own quota still persists a `SET GLOBAL` or account change.
    pub(crate) fn run_internal_write(&mut self, sql: &str) -> Result<Option<u64>, DriverError> {
        let (oom_action, _tmp_storage_on_oom) = self.vars.statement_memory_policy();
        let ctx = self
            .statement_context(true)
            .with_mem_quota(tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_QUERY, oom_action);
        let mut stmt = self.parse_internal(sql)?;
        self.bind_variables(&mut stmt)?;
        let tidb_ast::Stmt::Dml(dml) = &stmt else {
            return Err(DriverError::unsupported(
                "internal mysql write must be INSERT/REPLACE/UPDATE/DELETE",
            ));
        };
        self.with_staged_catalog(|catalog| match &**dml {
            tidb_ast::DmlStmt::Insert(insert) => {
                tidb_executor::driver::run_insert_stmt(insert, catalog, "mysql", &ctx)
                    .map(|(_, id)| id)
            }
            tidb_ast::DmlStmt::Update(update) => {
                tidb_executor::run_update_stmt(update, catalog, "mysql", &ctx).map(|_| None)
            }
            tidb_ast::DmlStmt::Delete(delete) => {
                tidb_executor::run_delete_stmt(delete, catalog, "mysql", &ctx).map(|_| None)
            }
            _ => Err(DriverError::unsupported(
                "internal mysql write must be INSERT/REPLACE/UPDATE/DELETE",
            )),
        })
    }

    /// Rows of one internal read of the `mysql` schema: inside an account
    /// statement, on its system session (seeing the statement's own writes,
    /// as Go's privilege executors read through it); otherwise Go's
    /// restricted SQL on an internal session.
    pub(crate) fn user_table_rows(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
        if let Some(system) = self.account_sql.as_mut() {
            return system.execute(sql);
        }
        self.exec_restricted_sql(sql)
    }
}

/// The primitives Go's account executors reach through `sessionctx` and the
/// system session's SQL executor.
impl tidb_executor::simple::account_tables::AccountSession for Session {
    fn user_table_present(&mut self) -> bool {
        Session::user_table_present(self)
    }

    fn run_user_table_write(&mut self, sql: &str) -> Result<(), DriverError> {
        Session::run_user_table_write(self, sql)
    }

    fn user_table_rows(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
        Session::user_table_rows(self, sql)
    }

    fn current_identity(&self) -> Option<(&str, &str)> {
        Session::current_identity(self)
    }

    fn privilege_registry(&self) -> Option<&tidb_privilege::PrivilegeRegistry> {
        self.privileges.as_ref()
    }

    fn account_statement_privilege_cache(&self) -> Option<&tidb_privilege::PrivilegeRegistry> {
        self.account_statement_privilege_cache.as_ref()
    }

    fn in_account_statement(&self) -> bool {
        self.account_sql.is_some()
    }

    fn has_scoped_privilege(
        &self,
        database: &str,
        table: &str,
        global_priv: tidb_privilege::PrivilegeType,
    ) -> bool {
        Session::has_scoped_privilege(self, database, table, global_priv)
    }

    fn has_dynamic_privilege(&self, name: &str, with_grant: bool) -> bool {
        Session::has_dynamic_privilege(self, name, with_grant)
    }

    fn target_has_dynamic_privilege(&self, user: &str, host: &str, name: &str) -> bool {
        Session::target_has_dynamic_privilege(self, user, host, name)
    }

    fn exec_restricted_sql(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
        Session::exec_restricted_sql(self, sql)
    }

    fn lock_catalog(&self) -> Result<std::sync::MutexGuard<'_, tidb_executor::Catalog>, DriverError> {
        Session::lock_catalog(self)
    }

    fn user_resource_group_attribute(&self, name: &str, is_role: bool) -> Result<String, DriverError> {
        Session::user_resource_group_attribute(self, name, is_role)
    }
}
