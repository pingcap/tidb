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

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use crate::*;

impl Session {
    /// Go `checkPasswordReusePolicy` reads `mysql.password_history` itself
    /// on every statement (`passwordVerification`'s SELECT), so the stored
    /// rows -- an `UPDATE` of their timestamps included -- are what the check
    /// sees. The registry's copy of the account's history is replaced from
    /// the table first; without the table there is only the registry.
    /// Go `FLUSH PRIVILEGES` (`domain.NotifyUpdatePrivilege`): the privilege
    /// cache is rebuilt from the `mysql.*` tables (`MySQLPrivilege.LoadAll`),
    /// so a row written there directly -- `INSERT INTO mysql.role_edges` --
    /// takes effect, and the live registry every session shares is replaced
    /// in place.
    /// Installs the domain's `NotifyUpdateAllUsersPrivilege`, which FLUSH
    /// PRIVILEGES runs: this node's reload and the announcement to the
    /// cluster's other nodes.
    pub fn set_privilege_change_notifier(
        &mut self,
        notifier: Arc<dyn Fn() -> Result<(), DriverError> + Send + Sync>,
    ) {
        self.privilege_change_notifier = Some(notifier);
    }

    /// Go `executeFlush`'s `NotifyUpdateAllUsersPrivilege`: the domain
    /// reloads this node and announces the change so every peer reloads
    /// too; a session without a domain reloads its own registry.
    pub(crate) fn flush_privileges(&mut self) -> Result<(), DriverError> {
        match &self.privilege_change_notifier {
            Some(notify) => notify(),
            None => self.reload_privileges_from_tables(),
        }
    }

    /// Go `NotifyUpdatePrivilege(users)` from an account executor: once the
    /// statement's system session commits ([`Session::with_account_service`]),
    /// or at once for an executor that writes no system session.
    pub(crate) fn notify_update_privilege(&mut self, users: Vec<String>) -> Result<(), DriverError> {
        if self.account_sql.is_some() {
            self.pending_privilege_users.get_or_insert_with(Vec::new).extend(users);
            return Ok(());
        }
        match self.domain_services.as_ref() {
            Some(services) => services.accounts.notify_update_privilege(&users),
            None => Ok(()),
        }
    }

    pub(crate) fn reload_privileges_from_tables(&mut self) -> Result<(), DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Ok(());
        };
        if !self.user_table_present() || self.account_sql.is_some() {
            return Ok(());
        }
        // Go `Handle.UpdateAll`: `MySQLPrivilege.LoadAll` over internal SQL
        // (`loadTable`'s `ExecRestrictedSQL`), reading the committed tables.
        let executor = self.restricted_sql_executor();
        let context = tikv_client::util::with_internal_source_type(
            &tikv_client::trace::TraceContext::new(),
            tidb_txnkv::INTERNAL_TXN_PRIVILEGE,
        );
        let loaded = tidb_privilege::cluster_privilege_load::load_all(
            &mut tidb_domain_core::cluster_privileges::SqlPrivilegeTables {
                exec: |sql: &str| {
                    executor
                        .as_deref()
                        .ok_or_else(|| -> tidb_sqlexec::SqlExecError {
                            "restricted SQL runs on the session's node".into()
                        })?
                        .exec_restricted_sql(&context, &[], sql, &[])
                },
            },
        );
        self.append_restricted_sql_warnings();
        let loaded =
            loaded.map_err(|error| DriverError::unsupported(format!("FLUSH PRIVILEGES: {error}")))?;
        registry.replace_from(&tidb_privilege::load::registry_from_cluster(&loaded).registry);
        Ok(())
    }
}
