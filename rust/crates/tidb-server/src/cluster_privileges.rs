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

//! Turning a cluster's `mysql.*` rows into this node's live account table.
//!
//! [`tidb_exec::cluster_privilege_load`] reads the rows; this turns them into
//! the one [`PrivilegeRegistry`] that the login path, `SHOW GRANTS`, and every
//! privilege check share. The two halves are deliberately separate crates:
//! `tidb-exec` owns cluster reads and has no idea what a registry is, and this
//! crate owns the registry and never speaks to TiKV.
//!
//! What crosses the bridge is exactly what this node can act on. A privilege
//! name the registry does not model — a dynamic privilege it has never heard
//! of, a table-scope privilege outside its grant vocabulary — is collected as
//! a named skip rather than dropped silently or turned into a startup failure:
//! a node that refuses to start because a Go TiDB granted `RESTRICTED_TABLES_ADMIN`
//! is useless, and one that silently swallows it is dishonest.

use tidb_exec::cluster_privilege_load::{ClusterPrivileges, LoadedUser};
use tidb_session::privilege::{is_dynamic_privilege, Account, GlobalPriv, PrivilegeRegistry};

use crate::auth_identity::DEFAULT_AUTH_PLUGIN;

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
/// reached differently. A row whose `User` is empty is Go's anonymous-user
/// row, which this node's host-matching login path has no way to select, so it
/// is not created at all rather than created and never reachable.
#[must_use]
pub fn registry_from_cluster(loaded: &ClusterPrivileges) -> LoadedRegistry {
    let registry = PrivilegeRegistry::bootstrapped_from(Vec::new());
    let mut skipped = Vec::new();
    let mut account_count = 0;

    for user in &loaded.users {
        if user.user.is_empty() {
            continue;
        }
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

    LoadedRegistry {
        registry,
        account_count,
        skipped,
    }
}

/// Creates one account row, preserving the distinction Go encodes in
/// `Account_locked`: a locked, passwordless row IS a role.
fn create_account(registry: &PrivilegeRegistry, user: &LoadedUser) {
    if user.account_locked && user.authentication_string.is_empty() {
        registry.create_role(&user.user, &user.host);
        return;
    }
    let plugin = if user.plugin.is_empty() {
        DEFAULT_AUTH_PLUGIN
    } else {
        user.plugin.as_str()
    };
    registry.create_user_with_plugin(&user.user, &user.host, &user.authentication_string, plugin);
    if user.account_locked {
        registry.set_locked(&user.user, &user.host, true);
    }
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
    let upper = element.to_uppercase();
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

#[cfg(test)]
mod tests {
    use tidb_exec::cluster_privilege_load::{
        LoadedDbGrant, LoadedDefaultRole, LoadedDynamicGrant, LoadedRoleEdge, LoadedTableGrant,
    };

    use super::*;

    fn user(
        user: &str,
        host: &str,
        auth: &str,
        locked: bool,
        privs: Vec<&'static str>,
    ) -> LoadedUser {
        LoadedUser {
            host: host.to_owned(),
            user: user.to_owned(),
            authentication_string: auth.to_owned(),
            plugin: "mysql_native_password".to_owned(),
            account_locked: locked,
            password_expired: false,
            privileges: privs,
        }
    }

    #[test]
    fn a_cluster_account_arrives_with_the_credential_and_privileges_its_row_carried() {
        let loaded = ClusterPrivileges {
            users: vec![user(
                "bridge",
                "%",
                "*756304EBF9898407899144B374EECB2CC1B94597",
                false,
                vec!["SELECT", "SUPER"],
            )],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert_eq!(built.account_count, 1);
        assert!(built.skipped.is_empty());
        assert_eq!(
            built.registry.auth_string("bridge", "%").as_deref(),
            Some("*756304EBF9898407899144B374EECB2CC1B94597")
        );
        assert!(built
            .registry
            .has_global_priv("bridge", "%", GlobalPriv::Select));
        assert!(built
            .registry
            .has_global_priv("bridge", "%", GlobalPriv::Super));
        assert!(!built
            .registry
            .has_global_priv("bridge", "%", GlobalPriv::Delete));
    }

    #[test]
    fn both_a_role_and_a_locked_user_arrive_locked_out_of_login() {
        // Go stores a ROLE and an `ACCOUNT LOCK`ed user under the SAME
        // `mysql.user.account_locked = 'Y'`, and the registry keeps that one
        // flag rather than inventing a second one. Loading must therefore
        // reach the locked state by either construction path, and the two
        // rows stay distinguishable only by the credential they carry.
        let loaded = ClusterPrivileges {
            users: vec![
                user("analyst", "%", "", true, Vec::new()),
                user("frozen", "%", "*ABC", true, Vec::new()),
            ],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert!(built.registry.is_role("analyst", "%"));
        assert!(built.registry.is_role("frozen", "%"));
        assert_eq!(
            built.registry.auth_string("analyst", "%").as_deref(),
            Some("")
        );
        assert_eq!(
            built.registry.auth_string("frozen", "%").as_deref(),
            Some("*ABC")
        );
    }

    #[test]
    fn the_anonymous_row_is_not_created_at_all() {
        // A `User = ''` row is Go's anonymous account; this node's host
        // matching cannot select it, so creating it would only leave a row
        // nothing can ever reach.
        let loaded = ClusterPrivileges {
            users: vec![user("", "localhost", "", false, Vec::new())],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert_eq!(built.account_count, 0);
        assert!(built.registry.accounts().is_empty());
    }

    #[test]
    fn a_table_grants_set_element_named_grant_becomes_grant_option() {
        // `mysql.tables_priv`.`Table_priv` spells it `Grant`, while the
        // privilege it names is `GRANT OPTION`; uppercasing alone would drop
        // it as unrecognized.
        let loaded = ClusterPrivileges {
            users: vec![user("bridge", "%", "", false, Vec::new())],
            table_grants: vec![LoadedTableGrant {
                host: "%".to_owned(),
                user: "bridge".to_owned(),
                database: "test".to_owned(),
                table: "rows".to_owned(),
                privileges: vec!["Select".to_owned(), "Grant".to_owned()],
            }],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert!(
            built.skipped.is_empty(),
            "unexpected skips: {:?}",
            built.skipped
        );
        assert!(built
            .registry
            .table_grant_row_exists("bridge", "%", "test", "rows"));
    }

    #[test]
    fn a_database_grant_arrives_at_database_scope_only() {
        let loaded = ClusterPrivileges {
            users: vec![user("bridge", "%", "", false, Vec::new())],
            db_grants: vec![LoadedDbGrant {
                host: "%".to_owned(),
                user: "bridge".to_owned(),
                database: "test".to_owned(),
                privileges: vec!["SELECT"],
            }],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert!(built.registry.db_grant_row_exists("bridge", "%", "test"));
        assert!(!built
            .registry
            .has_global_priv("bridge", "%", GlobalPriv::Select));
    }

    #[test]
    fn a_dynamic_privilege_this_node_does_not_model_is_named_rather_than_dropped() {
        let loaded = ClusterPrivileges {
            users: vec![user("bridge", "%", "", false, Vec::new())],
            dynamic_grants: vec![
                LoadedDynamicGrant {
                    host: "%".to_owned(),
                    user: "bridge".to_owned(),
                    privilege: "SYSTEM_VARIABLES_ADMIN".to_owned(),
                    with_grant_option: false,
                },
                LoadedDynamicGrant {
                    host: "%".to_owned(),
                    user: "bridge".to_owned(),
                    privilege: "NOT_A_REAL_PRIVILEGE".to_owned(),
                    with_grant_option: false,
                },
            ],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert!(built
            .registry
            .has_dynamic_priv("bridge", "%", "SYSTEM_VARIABLES_ADMIN", false));
        assert_eq!(built.skipped.len(), 1);
        assert_eq!(built.skipped[0].privilege, "NOT_A_REAL_PRIVILEGE");
        assert_eq!(built.skipped[0].account, "'bridge'@'%'");
    }

    #[test]
    fn every_default_role_row_for_one_account_survives_the_replace() {
        // `set_default_roles` replaces an account's whole set, so writing one
        // stored row at a time would leave only the last.
        let loaded = ClusterPrivileges {
            users: vec![
                user("bridge", "%", "", false, Vec::new()),
                user("r1", "%", "", true, Vec::new()),
                user("r2", "%", "", true, Vec::new()),
            ],
            role_edges: vec![
                LoadedRoleEdge {
                    role_host: "%".to_owned(),
                    role_user: "r1".to_owned(),
                    grantee_host: "%".to_owned(),
                    grantee_user: "bridge".to_owned(),
                },
                LoadedRoleEdge {
                    role_host: "%".to_owned(),
                    role_user: "r2".to_owned(),
                    grantee_host: "%".to_owned(),
                    grantee_user: "bridge".to_owned(),
                },
            ],
            default_roles: vec![
                LoadedDefaultRole {
                    host: "%".to_owned(),
                    user: "bridge".to_owned(),
                    role_host: "%".to_owned(),
                    role_user: "r1".to_owned(),
                },
                LoadedDefaultRole {
                    host: "%".to_owned(),
                    user: "bridge".to_owned(),
                    role_host: "%".to_owned(),
                    role_user: "r2".to_owned(),
                },
            ],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        let account = ("bridge".to_owned(), "%".to_owned());
        assert_eq!(built.registry.default_roles(&account).len(), 2);
        assert_eq!(built.registry.granted_roles(&account).len(), 2);
    }
}
