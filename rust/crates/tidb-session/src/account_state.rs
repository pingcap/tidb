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

//! Go's `memTableRetriever`s for the `information_schema` tables that read
//! the viewing account (`USER_PRIVILEGES`, `USER_ATTRIBUTES`) and the session's
//! system variables (`VARIABLES_INFO`, `performance_schema.SESSION_VARIABLES`,
//! `PROFILING`). The executor owns them (a scan calls `retrieve` at its first
//! `Next`); what they read of the viewing session is snapshotted when a scan
//! first asks for one of them.

use tidb_privilege::PrivilegeTypeExt;
use tidb_datatype::Datum;
use tidb_executor::mem_table_retrieval::{MemTableRead, MemTableRetriever};
use tidb_executor::{Catalog, DriverError};
use tidb_hack::GoCaseCompare;

use tidb_privilege;
use tidb_variable::sysvar;
use tidb_variable::vars::SessionVars;

/// What the account tables read of the viewing session.
#[derive(Clone)]
pub(crate) struct AccountState {
    pub(crate) privileges: Option<tidb_privilege::PrivilegeRegistry>,
    pub(crate) privilege_bypassed: bool,
    pub(crate) login_user: Option<String>,
    pub(crate) current_user: Option<String>,
    pub(crate) active_roles: Vec<tidb_privilege::Account>,
    /// Go `GetRestrictedSQLExecutor` for the `mysql.user` read.
    pub(crate) restricted_sql: crate::restricted_sql::RestrictedSql,
}

impl AccountState {
    /// The `CURRENT_USER()` identity split into user and host.
    fn current_identity(&self) -> Option<(&str, &str)> {
        self.current_user.as_deref()?.split_once('@')
    }

    fn active_roles(&self) -> &[tidb_privilege::Account] {
        self.active_roles.as_slice()
    }

    /// `SELECT * FROM information_schema.USER_PRIVILEGES` rows, in Go's
    /// `MySQLPrivilege.UserPrivilegesTable` order: EVERY account's static
    /// privileges first (one row per privilege, in `mysql.AllGlobalPrivs`
    /// print order, or a single `USAGE` row for an account with none), then
    /// EVERY account's DYNAMIC privileges. Accounts are visited in username
    /// order, since Go walks a B-tree keyed by username.
    ///
    /// `IS_GRANTABLE` means different things in the two halves (captured):
    /// a static row reports the account's `GRANT OPTION`, while a dynamic
    /// row reports that one privilege's own `with_grant_option`.
    ///
    /// Visibility (Go: "Seeing all users requires SELECT ON * FROM mysql.*.
    /// The SUPER privilege (or any other dynamic privilege) doesn't help
    /// here. This is verified against MySQL."): without global or `mysql.*`
    /// `SELECT`, a session sees every one of its own host-pattern rows.
    pub(crate) fn user_privileges_rows(&self) -> Vec<Vec<Datum>> {
        let Some(registry) = &self.privileges else {
            return Vec::new();
        };
        let identity = self
            .login_user
            .as_deref()
            .and_then(|identity| identity.split_once('@'))
            .map(|(user, host)| (user.to_owned(), host.to_owned()));
        // Go's `UserPrivilegesTable` deliberately calls the raw privilege
        // cache rather than the SkipWithGrant-aware manager wrapper. A
        // bypassed no-row identity therefore sees no account rows here; it
        // does not gain an information-schema account inventory.
        let show_all = identity.as_ref().is_none_or(|(user, host)| {
            let own_raw_cache = registry.has_matching_global_or_schema_priv(
                user,
                host,
                tidb_mysql::consts::SystemDB,
                tidb_mysql::SelectPriv.bit(),
            );
            if self.privilege_bypassed {
                return own_raw_cache;
            }
            let roles = self
                .current_identity()
                .is_some_and(|(current_user, current_host)| {
                    registry
                        .effective_roles(
                            &(current_user.to_owned(), current_host.to_owned()),
                            self.active_roles(),
                        )
                        .into_iter()
                        .any(|(role_user, role_host)| {
                            registry.has_matching_global_or_schema_priv(
                                &role_user,
                                &role_host,
                                tidb_mysql::consts::SystemDB,
                                tidb_mysql::SelectPriv.bit(),
                            )
                        })
                });
            own_raw_cache || roles
        });
        let visible = |account: &(String, String)| {
            show_all
                || identity
                    .as_ref()
                    .is_some_and(|(user, host)| registry.account_matches(account, user, host))
        };

        let grantee = |(user, host): &(String, String)| format!("'{user}'@'{host}'");
        let cell = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
        let flag = |grantable: bool| cell(if grantable { "YES" } else { "NO" });

        let mut static_accounts = registry.global_priv_masks();
        static_accounts.sort_by(|(left, _), (right, _)| left.cmp(right));
        let mut rows = Vec::new();
        for (account, privs) in &static_accounts {
            if !visible(account) {
                continue;
            }
            let grantable = flag(privs & tidb_mysql::GrantPriv.bit() != 0);
            let named: Vec<&tidb_privilege::PrivilegeType> = tidb_mysql::ALL_GLOBAL_PRIVS
                .iter()
                .filter(|priv_| privs & priv_.bit() != 0)
                .collect();
            if named.is_empty() {
                // Go emits USAGE only for a truly empty static mask, and it
                // is never grantable. A lone mysql.GrantPriv bit is greater
                // than the Usage sentinel but is deliberately absent from
                // mysql.AllGlobalPrivs, so it produces no static row at all.
                if *privs == 0 {
                    rows.push(vec![
                        cell(&grantee(account)),
                        cell("def"),
                        cell("USAGE"),
                        flag(false),
                    ]);
                }
                continue;
            }
            for priv_ in named {
                rows.push(vec![
                    cell(&grantee(account)),
                    cell("def"),
                    cell(priv_.print_name()),
                    grantable.clone(),
                ]);
            }
        }

        let mut dynamic_accounts = registry.accounts_with_dynamic_privs();
        dynamic_accounts.sort();
        for account in &dynamic_accounts {
            if !visible(account) {
                continue;
            }
            for (name, grantable) in registry.dynamic_priv_rows(&account.0, &account.1) {
                rows.push(vec![
                    cell(&grantee(account)),
                    cell("def"),
                    cell(&name),
                    flag(grantable),
                ]);
            }
        }
        rows
    }

    /// Go `memtableRetriever.setDataForUserAttributes`: read the metadata
    /// object from the real `mysql.user` table, then apply MySQL 8.0.22's
    /// account-visibility rules before exposing the virtual rows. The query
    /// runs as Go's restricted SQL on an internal session, reading committed
    /// rows; its error is the table's error (`infoschema_reader.go:382`).
    pub(crate) fn user_attributes_rows(
        &self,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        let rows = crate::restricted_sql::exec_restricted_sql_over(
            self.restricted_sql.as_deref(),
            "SELECT User, Host, JSON_UNQUOTE(JSON_EXTRACT(User_attributes, '$.metadata')) \
             FROM mysql.user",
        )?;

        let Some((viewer_user, viewer_host)) = self.current_identity() else {
            return Ok(rows);
        };
        let Some(registry) = self.privileges.as_ref() else {
            return Ok(rows);
        };
        if self.privilege_bypassed {
            return Ok(rows);
        }

        let can_read_all = registry.has_priv_mask_with_roles(
            viewer_user,
            viewer_host,
            self.active_roles(),
            tidb_mysql::consts::SystemDB,
            "user",
            tidb_mysql::SelectPriv.bit() | tidb_mysql::UpdatePriv.bit(),
        );
        let can_create_user = registry.has_priv_mask_with_roles(
            viewer_user,
            viewer_host,
            self.active_roles(),
            "",
            "",
            tidb_mysql::CreateUserPriv.bit(),
        );
        let caller_is_system_user = registry.has_dynamic_priv_with_roles(
            viewer_user,
            viewer_host,
            self.active_roles(),
            "SYSTEM_USER",
            false,
        );

        Ok(rows.into_iter()
            .filter(|row| {
                if row.len() != 3 || can_read_all {
                    return row.len() == 3;
                }
                let Some(user) = crate::datum_text(&row[0]) else {
                    return false;
                };
                let Some(host) = crate::datum_text(&row[1]) else {
                    return false;
                };
                // `record.match(viewer)` in Go selects a stored account row
                // whose host pattern matches the viewer. The registry's
                // account matcher is the same host-pattern implementation.
                let is_self = registry.account_matches(
                    &(user.clone(), host.clone()),
                    viewer_user,
                    viewer_host,
                );
                if is_self {
                    return true;
                }
                if !can_create_user {
                    return false;
                }
                if caller_is_system_user {
                    return true;
                }
                // `SYSTEM_USER` is a dynamic privilege with SUPER fallback,
                // so a CREATE USER caller cannot inspect a protected target.
                !registry.has_dynamic_priv(&user, &host, "SYSTEM_USER", false)
            })
            .collect())
    }

}

/// What the variable tables read of the viewing session: its variables, and
/// whether SEM hides the invisible ones from it.
#[derive(Clone)]
pub(crate) struct VariablesState {
    pub(crate) vars: SessionVars,
    pub(crate) hide_invisible: bool,
}

impl VariablesState {
    fn hides(&self, name: &str) -> bool {
        self.hide_invisible && tidb_util::sem_compat::is_invisible_sys_var(&name.to_ascii_lowercase())
    }

    /// Go `memtableRetriever.setDataForVariablesInfo`: one row per system
    /// variable the viewer may see. DEFAULT_VALUE is what bootstrap seeds
    /// for a GLOBAL-scoped variable (`GlobalSystemVariableInitialValue`),
    /// CURRENT_VALUE is `GetSessionOrGlobalSystemVar` ("" when that read
    /// fails), MIN/MAX are reported only for the numeric types, and
    /// POSSIBLE_VALUES only for an enum.
    pub(crate) fn variables_info_rows(&self) -> Vec<Vec<tidb_datatype::Datum>> {
        use tidb_datatype::Datum;
        let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
        let mut rows = Vec::with_capacity(sysvar::SYS_VARS.len());
        for definition in sysvar::SYS_VARS {
            if self.hides(definition.name) {
                continue;
            }
            let current = self.vars.get_system(definition.name).unwrap_or_default();
            let default = if definition.has_global_scope() {
                tidb_variable::vars::global_system_variable_initial_value(definition)
            } else {
                sysvar::effective_default(definition)
            };
            let numeric = matches!(
                definition.var_type,
                sysvar::VarType::Unsigned | sysvar::VarType::Int | sysvar::VarType::Float
            );
            let (min, max) = if numeric {
                (
                    Datum::Int(definition.min_value),
                    Datum::UInt(definition.max_value),
                )
            } else {
                (Datum::Null, Datum::Null)
            };
            let possible = if definition.var_type == sysvar::VarType::Enum {
                text(&definition.possible_values.join(","))
            } else {
                Datum::Null
            };
            rows.push(vec![
                text(definition.name),
                text(&definition.scope_string()),
                text(&default),
                text(&current),
                min,
                max,
                possible,
                text(if definition.is_noop() { "YES" } else { "NO" }),
            ]);
        }
        rows
    }


    /// Go `infoschema.GetDataFromSessionVariables`, the rows of
    /// `performance_schema.session_variables`: every variable the viewer may
    /// see, with `GetSessionOrGlobalSystemVar`'s value. Unlike
    /// `VARIABLES_INFO`, a value that cannot be read fails the statement.
    pub(crate) fn session_variables_rows(&self) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        let text = |value: &str| tidb_datatype::Datum::Bytes(value.as_bytes().to_vec());
        let mut rows = Vec::with_capacity(sysvar::SYS_VARS.len());
        for definition in sysvar::SYS_VARS {
            if self.hides(definition.name) {
                continue;
            }
            let value = self
                .vars
                .get_system(definition.name)
                .map_err(crate::variables::var_error)?;
            rows.push(vec![text(definition.name), text(&value)]);
        }
        Ok(rows)
    }

}

/// The retriever for the account tables.
pub(crate) struct AccountRetriever {
    account: AccountState,
}

impl AccountRetriever {
    pub(crate) fn new(account: AccountState) -> Self {
        Self { account }
    }
}

impl MemTableRetriever for AccountRetriever {
    fn serves(&self, read: &MemTableRead) -> bool {
        !read.is_cluster_scan
            && read
                .db_name
                .go_lower_eq(tidb_executor::driver::infoschema_meta::INFORMATION_SCHEMA)
            && ["USER_PRIVILEGES", "USER_ATTRIBUTES"]
                .iter()
                .any(|name| read.table_name.go_lower_eq(name))
    }

    fn retrieve(
        &self,
        read: &MemTableRead,
        _catalog: &Catalog,
        _ctx: &tidb_executor::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        Ok(if read.table_name.go_lower_eq("USER_PRIVILEGES") {
            self.account.user_privileges_rows()
        } else {
            self.account.user_attributes_rows()?
        })
    }
}

/// The retriever for the variable tables.
pub(crate) struct VariablesRetriever {
    state: VariablesState,
}

impl VariablesRetriever {
    pub(crate) fn new(state: VariablesState) -> Self {
        Self { state }
    }
}

impl MemTableRetriever for VariablesRetriever {
    fn serves(&self, read: &MemTableRead) -> bool {
        if read.is_cluster_scan {
            return false;
        }
        (read
            .db_name
            .go_lower_eq(tidb_executor::driver::infoschema_meta::INFORMATION_SCHEMA)
            && ["VARIABLES_INFO", "PROFILING"]
                .iter()
                .any(|name| read.table_name.go_lower_eq(name)))
            || (read
                .db_name
                .go_lower_eq(tidb_executor::driver::perfschema::PERFORMANCE_SCHEMA)
                && read.table_name.go_lower_eq("SESSION_VARIABLES"))
    }

    fn retrieve(
        &self,
        read: &MemTableRead,
        _catalog: &Catalog,
        _ctx: &tidb_executor::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        if read.table_name.go_lower_eq("VARIABLES_INFO") {
            return Ok(self.state.variables_info_rows());
        }
        if read.table_name.go_lower_eq("SESSION_VARIABLES") {
            return self.state.session_variables_rows();
        }
        Ok(tidb_executor::memtable_retrievers::pseudo_profiling_rows(
            tidb_vardef::option_values::tidb_opt_on(
                &self.state.vars.get_system("profiling").unwrap_or_default(),
            ),
        ))
    }
}
