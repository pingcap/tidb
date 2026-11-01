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
    /// Installs the announcement FLUSH PRIVILEGES makes to the cluster's
    /// other nodes after reloading this one.
    pub fn set_privilege_change_notifier(&mut self, notifier: Arc<dyn Fn() + Send + Sync>) {
        self.privilege_change_notifier = Some(notifier);
    }

    /// Go `executeFlush`'s `NotifyUpdateAllUsersPrivilege`: reload this
    /// node, then announce the change so every peer reloads too.
    pub(crate) fn flush_privileges(&mut self) -> Result<(), DriverError> {
        self.reload_privileges_from_tables()?;
        if let Some(notify) = &self.privilege_change_notifier {
            notify();
        }
        Ok(())
    }

    pub(crate) fn reload_privileges_from_tables(&mut self) -> Result<(), DriverError> {
        let Some(registry) = self.privileges.clone() else {
            return Ok(());
        };
        if !self.user_table_present() || self.account_sql.is_some() {
            return Ok(());
        }
        let ctx = self
            .statement_context(false)
            .with_time_zone(tidb_datatype::SessionTimeZone::utc());
        let decode = tidb_table::kv_table::RowDecodeContext::for_query(&ctx);
        let loaded = self.with_persistent_catalog_mut(|catalog| {
            tidb_privilege::cluster_privilege_load::load_privileges(
                &mut crate::privilege_tables::SessionPrivilegeTables { catalog, decode },
            )
            .map_err(|error| DriverError::unsupported(format!("FLUSH PRIVILEGES: {error}")))
        })?;
        registry.replace_from(&tidb_privilege::load::registry_from_cluster(&loaded).registry);
        Ok(())
    }
}
