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

//! Go `pkg/privilege/privileges/privileges.go` `UserPrivileges`: the
//! privilege manager of a session's user, over the shared handle.

use tidb_extension::AccessCheckFunc;
use tidb_hack::{GoCaseCompare, GoToLower};
use tidb_mysql::PrivilegeType;

use crate::{Account, PrivilegeRegistry};

/// Go `UserPrivileges`: the account a session authenticated as, the
/// privilege handle, and the access checks of the extensions set up when
/// the session bound it (`NewUserPrivileges`). A session that skips grant
/// tables (Go `SkipWithGrant`) has none.
pub struct UserPrivileges<'a> {
    /// Go `user`.
    pub user: &'a str,
    /// Go `host`.
    pub host: &'a str,
    /// Go `*Handle`.
    pub handle: &'a PrivilegeRegistry,
    /// Go `authPluginRequestVerification` / `authPluginRequestDynamicVerification`.
    pub auth_plugin: Option<&'a AuthPluginVerification>,
    /// Go `extensionAccessCheckFuncs`.
    pub extension_access_check_funcs: &'a [AccessCheckFunc],
}

impl UserPrivileges<'_> {
    /// Go `RequestDynamicVerification`.
    #[must_use]
    pub fn request_dynamic_verification(
        &self,
        active_roles: &[Account],
        priv_name: &str,
        grantable: bool,
    ) -> bool {
        if self.user.is_empty() && self.host.is_empty() {
            return true;
        }
        self.handle.has_dynamic_priv_with_roles(
            self.user,
            self.host,
            active_roles,
            priv_name,
            grantable,
        ) && self.auth_plugin.is_none_or(|p| {
            p.verify_dynamic(self.user, self.host, active_roles, priv_name, grantable)
        })
    }

    /// Go `RequestVerification`: SEM's hard rules, the virtual schemas,
    /// every extension's access check, then the grants (own or through
    /// `active_roles`). A multi-bit `privilege` asks for any of them.
    #[must_use]
    pub fn request_verification(
        &self,
        active_roles: &[Account],
        db: &str,
        table: &str,
        column: &str,
        privilege: PrivilegeType,
    ) -> bool {
        if self.user.is_empty() && self.host.is_empty() {
            return true;
        }
        // If SEM is enabled and the user does not have the
        // RESTRICTED_TABLES_ADMIN privilege, there are some hard rules which
        // overwrite system tables and schemas as read-only at most.
        let sem_enabled = tidb_sem_compat::is_enabled();
        if sem_enabled {
            let has_restricted_tables_admin =
                self.request_dynamic_verification(active_roles, "RESTRICTED_TABLES_ADMIN", false);
            if let Some(verdict) = crate::verification::sem_verdict_mask(
                db,
                table,
                privilege.0,
                has_restricted_tables_admin,
            ) {
                return verdict;
            }
        }
        if let Some(verdict) = crate::verification::mem_db_verdict_mask(db, privilege.0) {
            return verdict;
        }
        // PROCESS is the same with SELECT for metrics_schema.
        let privilege = if db.go_lower_eq("metrics_schema")
            && privilege == tidb_mysql::SelectPriv
            && tidb_infoschema::metric_table_def::is_metric_table(&table.go_to_lower())
        {
            privilege | tidb_mysql::ProcessPriv
        } else {
            privilege
        };
        for check in self.extension_access_check_funcs {
            for dyn_priv in check(db, table, column, privilege, sem_enabled) {
                if !self.request_dynamic_verification(active_roles, &dyn_priv, false) {
                    return false;
                }
            }
        }
        self.handle.has_priv_mask_with_roles(
            self.user,
            self.host,
            active_roles,
            db,
            table,
            privilege.0,
        ) && self.auth_plugin.is_none_or(|p| {
            p.verify_static(
                self.user,
                self.host,
                active_roles,
                db,
                table,
                column,
                privilege,
            )
        })
    }
}

/// Go `authenticateWithPlugin`'s two bound authorization closures.
#[derive(Clone, Debug)]
pub struct AuthPluginVerification {
    pub plugin: tidb_extension::AuthPlugin,
    pub conn_state: Option<tidb_variable::vars::TlsConnectionState>,
}

impl AuthPluginVerification {
    /// Go `authPluginRequestVerification`.
    pub fn verify_static(
        &self,
        user: &str,
        host: &str,
        roles: &[Account],
        db: &str,
        table: &str,
        column: &str,
        privilege: PrivilegeType,
    ) -> bool {
        self.plugin.verify_privilege.as_ref().is_none_or(|verify| {
            verify(tidb_extension::VerifyStaticPrivRequest {
                user,
                host,
                db,
                table,
                column,
                static_priv: privilege,
                conn_state: self.conn_state.as_ref(),
                active_roles: &roles
                    .iter()
                    .map(|(user, host)| tidb_parser::auth::RoleIdentity {
                        username: user.clone(),
                        hostname: host.clone(),
                    })
                    .collect::<Vec<_>>(),
            })
        })
    }
    /// Go `authPluginRequestDynamicVerification`.
    pub fn verify_dynamic(
        &self,
        user: &str,
        host: &str,
        roles: &[Account],
        name: &str,
        grantable: bool,
    ) -> bool {
        self.plugin
            .verify_dynamic_privilege
            .as_ref()
            .is_none_or(|verify| {
                verify(tidb_extension::VerifyDynamicPrivRequest {
                    user,
                    host,
                    dynamic_priv: name,
                    with_grant: grantable,
                    conn_state: self.conn_state.as_ref(),
                    active_roles: &roles
                        .iter()
                        .map(|(user, host)| tidb_parser::auth::RoleIdentity {
                            username: user.clone(),
                            hostname: host.clone(),
                        })
                        .collect::<Vec<_>>(),
                })
            })
    }
}
