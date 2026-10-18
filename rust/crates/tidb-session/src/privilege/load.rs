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

//! Go `MySQLPrivilege.LoadAll`'s other half: building the live privilege
//! table from the rows `tidb_exec::cluster_privilege_load::load_privileges`
//! read out of `mysql.*`, whether a cluster's or this session's own.

use tidb_exec::cluster_privilege_load::{ClusterPrivileges, LoadedUser};

use super::{is_dynamic_privilege, Account, GlobalPriv, PrivilegeRegistry};

/// The plugin a `mysql.user` row with an empty `plugin` authenticates with.
const DEFAULT_AUTH_PLUGIN: &str = tidb_mysql::consts::AuthNativePassword;

/// One privilege a cluster row granted that this node does not model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkippedGrant {
    /// `mysql.<table>` the row came from.
    pub source: &'static str,
    /// The account the row named, as `'user'@'host'`.
    pub account: String,
    /// The privilege name, exactly as stored.
    pub privilege: String,
}

impl SkippedGrant {
    fn new(source: &'static str, user: &str, host: &str, privilege: &str) -> Self {
        Self {
            source,
            account: format!("'{user}'@'{host}'"),
            privilege: privilege.to_owned(),
        }
    }
}

/// What one cluster privilege load produced.
#[derive(Clone, Debug)]
pub struct LoadedRegistry {
    /// The live account table, ready to be shared with the session factory.
    pub registry: PrivilegeRegistry,
    /// Accounts loaded, including roles.
    pub account_count: usize,
    /// Grants named by the cluster that this node does not model.
    pub skipped: Vec<SkippedGrant>,
}

/// Builds one live account table from a cluster's `mysql.*` rows.
///
/// Go's loader keeps a `mysql.user` row's privileges on the row itself; this
/// registry stores an account's global mask, so the two are the same state
/// reached differently. Empty usernames are retained like any other account:
/// Go indexes them under the empty username and matches their host patterns.
#[must_use]
pub fn registry_from_cluster(loaded: &ClusterPrivileges) -> LoadedRegistry {
    let registry = PrivilegeRegistry::bootstrapped_from(Vec::new());
    let mut skipped = Vec::new();
    let mut account_count = 0;

    for user in &loaded.users {
        create_account(&registry, user);
        account_count += 1;
        let mut mask = 0u64;
        for privilege in &user.privileges {
            match GlobalPriv::from_grant_name(privilege) {
                Some(global) => mask |= priv_mask(global),
                None => skipped.push(SkippedGrant::new(
                    "mysql.user",
                    &user.user,
                    &user.host,
                    privilege,
                )),
            }
        }
        registry.grant(&user.user, &user.host, mask);
    }

    registry.load_global_priv(loaded.global_priv.clone());

    for grant in &loaded.db_grants {
        let mut mask = 0u64;
        for privilege in &grant.privileges {
            match GlobalPriv::from_grant_name(privilege) {
                Some(global) => mask |= priv_mask(global),
                None => skipped.push(SkippedGrant::new(
                    "mysql.db",
                    &grant.user,
                    &grant.host,
                    privilege,
                )),
            }
        }
        registry.grant_db(&grant.user, &grant.host, &grant.database, mask);
    }

    for grant in &loaded.table_grants {
        let mask = set_mask(
            "mysql.tables_priv",
            &grant.user,
            &grant.host,
            &grant.privileges,
            &mut skipped,
        );
        registry.grant_table(
            &grant.user,
            &grant.host,
            &grant.database,
            &grant.table,
            mask,
        );
    }

    for grant in &loaded.column_grants {
        let mask = set_mask(
            "mysql.columns_priv",
            &grant.user,
            &grant.host,
            &grant.privileges,
            &mut skipped,
        );
        registry.grant_column(
            &grant.user,
            &grant.host,
            &grant.database,
            &grant.table,
            &grant.column,
            mask,
        );
    }

    for grant in &loaded.dynamic_grants {
        if is_dynamic_privilege(&grant.privilege) {
            registry.grant_dynamic(
                &grant.user,
                &grant.host,
                &grant.privilege,
                grant.with_grant_option,
            );
        } else {
            skipped.push(SkippedGrant::new(
                "mysql.global_grants",
                &grant.user,
                &grant.host,
                &grant.privilege,
            ));
        }
    }

    for edge in &loaded.role_edges {
        registry.grant_role(
            &account(&edge.role_user, &edge.role_host),
            &account(&edge.grantee_user, &edge.grantee_host),
        );
    }

    // Go stores one `mysql.default_roles` row per default role, and
    // `SET DEFAULT ROLE` replaces every row for the account. Grouping the
    // rows back into one set per account before writing is what keeps
    // `set_default_roles`'s replace semantics from making each row erase the
    // one before it.
    let mut grouped: Vec<(Account, Vec<Account>)> = Vec::new();
    for default in &loaded.default_roles {
        let owner = account(&default.user, &default.host);
        let role = account(&default.role_user, &default.role_host);
        match grouped.iter_mut().find(|(existing, _)| *existing == owner) {
            Some((_, roles)) => roles.push(role),
            None => grouped.push((owner, vec![role])),
        }
    }
    for (owner, roles) in grouped {
        registry.set_default_roles(&owner, &roles);
    }

    registry.restore_password_history(loaded.password_history.clone());

    LoadedRegistry {
        registry,
        account_count,
        skipped,
    }
}

fn create_account(registry: &PrivilegeRegistry, user: &LoadedUser) {
    let plugin = if user.plugin.is_empty() {
        DEFAULT_AUTH_PLUGIN
    } else {
        user.plugin.as_str()
    };
    registry.create_user_with_plugin(&user.user, &user.host, &user.authentication_string, plugin);
    registry.set_locked(&user.user, &user.host, user.account_locked);
    registry.restore_account_policy(
        &user.user,
        &user.host,
        user.password_reuse_history,
        user.password_reuse_time,
        user.user_attributes.clone(),
        user.password_locking,
    );
    registry.set_max_user_connections(&user.user, &user.host, user.max_user_connections);
    registry.restore_password_expiry(
        &user.user,
        &user.host,
        user.password_expired,
        user.password_lifetime,
        user.password_last_changed,
    );
}

/// Resolves a `SET`-valued privilege list, naming anything unmodelled.
fn set_mask(
    source: &'static str,
    user: &str,
    host: &str,
    privileges: &[String],
    skipped: &mut Vec<SkippedGrant>,
) -> u64 {
    let mut mask = 0u64;
    for privilege in privileges {
        match GlobalPriv::from_grant_name(&set_element_grant_name(privilege)) {
            Some(global) => mask |= priv_mask(global),
            None => skipped.push(SkippedGrant::new(source, user, host, privilege)),
        }
    }
    mask
}

/// Spells one `SET` element the way `from_grant_name` resolves it.
///
/// `mysql.tables_priv`.`Table_priv` and `mysql.columns_priv`.`Column_priv`
/// declare their elements in title case (`Create View`, verified against a
/// live cluster's stored `FieldType.Elems`), so uppercasing is enough for all
/// but one: the element is spelled `Grant`, while the privilege it names is
/// `GRANT OPTION`.
fn set_element_grant_name(element: &str) -> String {
    let upper = tidb_mysql::to_uppercase(element);
    if upper == "GRANT" {
        return "GRANT OPTION".to_owned();
    }
    upper
}

fn priv_mask(global: GlobalPriv) -> u64 {
    global.mask()
}

fn account(user: &str, host: &str) -> Account {
    (user.to_owned(), host.to_owned())
}

/// This session's own `mysql.*` tables, read the way
/// `tidb_exec::cluster_privilege_load::load_privileges` reads a cluster's.
pub(crate) struct SessionPrivilegeTables<'a> {
    pub(crate) catalog: &'a mut tidb_executor::Catalog,
    /// TIMESTAMP columns decode in UTC, as the cluster reader decodes them.
    pub(crate) decode: tidb_executor::RowDecodeContext,
}

impl tidb_exec::cluster_privilege_load::PrivilegeTables for SessionPrivilegeTables<'_> {
    fn rows(
        &mut self,
        table: &str,
        columns: &[&str],
        _timezone: Option<&tidb_datatype::SessionTimeZone>,
    ) -> Result<
        Option<tidb_exec::cluster_privilege_load::PrivilegeTableRows>,
        tidb_exec::mysql_system_tables::SystemTableError,
    > {
        let name = format!("mysql.{table}");
        let Some(tidb_executor::TableEntry::Kv(stored)) = self.catalog.table_mut_in("mysql", table)
        else {
            return Ok(None);
        };
        let stored = std::sync::Arc::make_mut(stored);
        let view = tidb_exec::mysql_system_tables::SystemTableView::from_columns(
            &name,
            stored.table_id,
            stored
                .columns
                .iter()
                .map(|column| (column.name.clone(), column.id, column.field_type.clone())),
            columns,
        );
        let ids: Vec<i64> = stored.columns.iter().map(|column| column.id).collect();
        let rows = stored
            .scan_rows_with_context(&self.decode)
            .map_err(
                |error| tidb_exec::mysql_system_tables::SystemTableError::Decode {
                    name: name.clone(),
                    detail: format!("{error:?}"),
                },
            )?;
        Ok(Some((
            view,
            rows.into_iter()
                .map(|row| ids.iter().copied().zip(row).collect())
                .collect(),
        )))
    }
}
