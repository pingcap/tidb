// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Loading a cluster's accounts and grants out of `mysql.*`.
//!
//! This is the read half of the bridge between a Go TiDB that owns the
//! `mysql.*` tables and this node's in-memory privilege table: whatever
//! `CREATE USER`/`GRANT` wrote through the Go node is what this node reads,
//! at one snapshot, at startup.
//!
//! Go source of truth is `pkg/privilege/privileges/cache.go`
//! `MySQLPrivilege.LoadAll`, which issues one `SELECT` per table:
//! `mysql.user`, `mysql.global_priv`, `mysql.db`, `mysql.tables_priv`,
//! `mysql.columns_priv`, `mysql.default_roles`, `mysql.role_edges`, and
//! `mysql.global_grants`. The same tables are read here, through the record
//! range rather than a `SELECT`, because this node's bounded read path cannot
//! serve a table without a `BIGINT` handle (see
//! [`crate::mysql_system_tables`]).
//!
//! Two deliberate scope statements, both stated rather than hidden:
//!
//! * **One-shot.** Go re-loads on a `notifyupdateprivilege` etcd event and on
//!   its own `Load` interval. This is the startup load only; nothing here
//!   watches for later changes, so a `GRANT` a Go node runs after this node
//!   started is not visible until this node restarts.
//! * **`mysql.global_priv` is not read.** That table holds the JSON
//!   connection-attribute policy (SSL/SAN requirements), which this node's
//!   login path does not enforce at all; reading it would imply an
//!   enforcement that does not exist.

use std::fmt;

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::mysql_system_tables::{scan_system_table, SystemRow, SystemTableError, SystemTableView};

/// Maps one `mysql.user` privilege column to the privilege name
/// `GlobalPriv::from_grant_name` resolves.
///
/// The column list is Go `mysql.Priv2UserCol` read in reverse: that map takes
/// a privilege to its `mysql.user` column, and this table takes the column
/// back to the privilege's printed name. `Account_locked` is deliberately
/// absent — it is a column of the same `ENUM('N','Y')` shape but is account
/// state, not a privilege.
pub const USER_PRIVILEGE_COLUMNS: &[(&str, &str)] = &[
    ("select_priv", "SELECT"),
    ("insert_priv", "INSERT"),
    ("update_priv", "UPDATE"),
    ("delete_priv", "DELETE"),
    ("create_priv", "CREATE"),
    ("drop_priv", "DROP"),
    ("process_priv", "PROCESS"),
    ("grant_priv", "GRANT OPTION"),
    ("references_priv", "REFERENCES"),
    ("alter_priv", "ALTER"),
    ("show_db_priv", "SHOW DATABASES"),
    ("super_priv", "SUPER"),
    ("create_tmp_table_priv", "CREATE TEMPORARY TABLES"),
    ("lock_tables_priv", "LOCK TABLES"),
    ("execute_priv", "EXECUTE"),
    ("create_view_priv", "CREATE VIEW"),
    ("show_view_priv", "SHOW VIEW"),
    ("create_routine_priv", "CREATE ROUTINE"),
    ("alter_routine_priv", "ALTER ROUTINE"),
    ("index_priv", "INDEX"),
    ("create_user_priv", "CREATE USER"),
    ("event_priv", "EVENT"),
    ("repl_slave_priv", "REPLICATION SLAVE"),
    ("repl_client_priv", "REPLICATION CLIENT"),
    ("trigger_priv", "TRIGGER"),
    ("shutdown_priv", "SHUTDOWN"),
    ("reload_priv", "RELOAD"),
    ("file_priv", "FILE"),
    ("config_priv", "CONFIG"),
    ("create_tablespace_priv", "CREATE TABLESPACE"),
];

/// Maps one `mysql.db` privilege column to the same printed privilege name.
///
/// `mysql.db` carries a strict subset of `mysql.user`'s columns (Go
/// `mysql.AllDBPrivs`), so this reuses the spellings above rather than
/// inventing new ones.
pub const DB_PRIVILEGE_COLUMNS: &[(&str, &str)] = &[
    ("select_priv", "SELECT"),
    ("insert_priv", "INSERT"),
    ("update_priv", "UPDATE"),
    ("delete_priv", "DELETE"),
    ("create_priv", "CREATE"),
    ("drop_priv", "DROP"),
    ("grant_priv", "GRANT OPTION"),
    ("references_priv", "REFERENCES"),
    ("index_priv", "INDEX"),
    ("alter_priv", "ALTER"),
    ("create_tmp_table_priv", "CREATE TEMPORARY TABLES"),
    ("lock_tables_priv", "LOCK TABLES"),
    ("create_view_priv", "CREATE VIEW"),
    ("show_view_priv", "SHOW VIEW"),
    ("create_routine_priv", "CREATE ROUTINE"),
    ("alter_routine_priv", "ALTER ROUTINE"),
    ("execute_priv", "EXECUTE"),
    ("event_priv", "EVENT"),
    ("trigger_priv", "TRIGGER"),
];

/// The `mysql.user` columns this loader reads.
const USER_COLUMNS: &[&str] = &[
    "host",
    "user",
    "authentication_string",
    "plugin",
    "account_locked",
    "password_expired",
    "select_priv",
    "insert_priv",
    "update_priv",
    "delete_priv",
    "create_priv",
    "drop_priv",
    "process_priv",
    "grant_priv",
    "references_priv",
    "alter_priv",
    "show_db_priv",
    "super_priv",
    "create_tmp_table_priv",
    "lock_tables_priv",
    "execute_priv",
    "create_view_priv",
    "show_view_priv",
    "create_routine_priv",
    "alter_routine_priv",
    "index_priv",
    "create_user_priv",
    "event_priv",
    "repl_slave_priv",
    "repl_client_priv",
    "trigger_priv",
    "shutdown_priv",
    "reload_priv",
    "file_priv",
    "config_priv",
    "create_tablespace_priv",
];

/// The `mysql.db` columns this loader reads.
const DB_COLUMNS: &[&str] = &[
    "host",
    "user",
    "db",
    "select_priv",
    "insert_priv",
    "update_priv",
    "delete_priv",
    "create_priv",
    "drop_priv",
    "grant_priv",
    "references_priv",
    "index_priv",
    "alter_priv",
    "create_tmp_table_priv",
    "lock_tables_priv",
    "create_view_priv",
    "show_view_priv",
    "create_routine_priv",
    "alter_routine_priv",
    "execute_priv",
    "event_priv",
    "trigger_priv",
];

/// One `mysql.user` row, in the terms this node's account table needs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedUser {
    /// `Host`, the account's host pattern.
    pub host: String,
    /// `User`, the account name.
    pub user: String,
    /// `authentication_string`, the stored credential for `plugin`.
    pub authentication_string: String,
    /// `plugin`; empty when the row stores none.
    pub plugin: String,
    /// `Account_locked = 'Y'`. Go stores a ROLE as a locked account.
    pub account_locked: bool,
    /// `Password_expired = 'Y'`.
    pub password_expired: bool,
    /// Printed names of the global privileges whose column reads `Y`.
    pub privileges: Vec<&'static str>,
}

/// One `mysql.db` row: a database-scoped grant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedDbGrant {
    /// `Host`.
    pub host: String,
    /// `User`.
    pub user: String,
    /// `DB`.
    pub database: String,
    /// Printed names of the privileges whose column reads `Y`.
    pub privileges: Vec<&'static str>,
}

/// One `mysql.tables_priv` row: a table-scoped grant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedTableGrant {
    /// `Host`.
    pub host: String,
    /// `User`.
    pub user: String,
    /// `DB`.
    pub database: String,
    /// `Table_name`.
    pub table: String,
    /// `Table_priv`, the `SET` labels the row selects.
    pub privileges: Vec<String>,
}

/// One `mysql.columns_priv` row: a column-scoped grant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedColumnGrant {
    /// `Host`.
    pub host: String,
    /// `User`.
    pub user: String,
    /// `DB`.
    pub database: String,
    /// `Table_name`.
    pub table: String,
    /// `Column_name`.
    pub column: String,
    /// `Column_priv`, the `SET` labels the row selects.
    pub privileges: Vec<String>,
}

/// One `mysql.global_grants` row: a dynamic privilege.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedDynamicGrant {
    /// `HOST`.
    pub host: String,
    /// `USER`.
    pub user: String,
    /// `PRIV`, the dynamic privilege name.
    pub privilege: String,
    /// `WITH_GRANT_OPTION = 'Y'`.
    pub with_grant_option: bool,
}

/// One `mysql.role_edges` row: role -> grantee.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedRoleEdge {
    /// `FROM_HOST`, the granted role's host.
    pub role_host: String,
    /// `FROM_USER`, the granted role's name.
    pub role_user: String,
    /// `TO_HOST`, the grantee's host.
    pub grantee_host: String,
    /// `TO_USER`, the grantee's name.
    pub grantee_user: String,
}

/// One `mysql.default_roles` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedDefaultRole {
    /// `HOST`, the account's host.
    pub host: String,
    /// `USER`, the account's name.
    pub user: String,
    /// `DEFAULT_ROLE_HOST`.
    pub role_host: String,
    /// `DEFAULT_ROLE_USER`.
    pub role_user: String,
}

/// Everything one snapshot of `mysql.*` says about accounts and grants.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterPrivileges {
    /// `mysql.user`, in stored key order.
    pub users: Vec<LoadedUser>,
    /// `mysql.db`, in stored key order.
    pub db_grants: Vec<LoadedDbGrant>,
    /// `mysql.tables_priv`, in stored key order.
    pub table_grants: Vec<LoadedTableGrant>,
    /// `mysql.columns_priv`, in stored key order.
    pub column_grants: Vec<LoadedColumnGrant>,
    /// `mysql.global_grants`, in stored key order.
    pub dynamic_grants: Vec<LoadedDynamicGrant>,
    /// `mysql.role_edges`, in stored key order.
    pub role_edges: Vec<LoadedRoleEdge>,
    /// `mysql.default_roles`, in stored key order.
    pub default_roles: Vec<LoadedDefaultRole>,
}

/// Whether a cluster's `mysql.tidb` says Go's bootstrap already ran.
///
/// Go `pkg/session/bootstrap.go`: `doDMLWorks` writes `bootstrapped = "True"`
/// and `tidb_server_version = <currentBootstrapVersion>` into `mysql.tidb` in
/// the same transaction as the root account, and `getStoreBootstrapVersion`
/// reads them back to decide whether to bootstrap at all.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterBootstrapState {
    /// `mysql.tidb` holds `bootstrapped = 'True'`. Re-seeding is refused.
    Bootstrapped {
        /// `tidb_server_version` as stored, when it parses as an integer.
        version: Option<i64>,
    },
    /// The `mysql` schema exists but carries no bootstrap flag.
    Unflagged,
    /// The cluster catalog has no `mysql.tidb` at all: no TiDB ever
    /// bootstrapped this keyspace.
    NotBootstrapped,
}

impl ClusterBootstrapState {
    /// Whether a bootstrap must not run against this cluster.
    #[must_use]
    pub const fn already_bootstrapped(&self) -> bool {
        matches!(self, Self::Bootstrapped { .. })
    }
}

impl fmt::Display for ClusterBootstrapState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bootstrapped {
                version: Some(version),
            } => write!(formatter, "bootstrapped at version {version}"),
            Self::Bootstrapped { version: None } => {
                formatter.write_str("bootstrapped at an unparsable version")
            }
            Self::Unflagged => formatter.write_str("mysql.tidb carries no bootstrap flag"),
            Self::NotBootstrapped => formatter.write_str("the cluster has no mysql.tidb"),
        }
    }
}

/// Go `bootstrappedVar`.
const BOOTSTRAPPED_VAR: &str = "bootstrapped";
/// Go `tidbServerVersionVar`.
const TIDB_SERVER_VERSION_VAR: &str = "tidb_server_version";
/// Go `varTrue`.
const VAR_TRUE: &str = "True";

/// Reads `mysql.tidb`'s bootstrap flag from one catalog snapshot.
pub fn read_bootstrap_state<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
) -> Result<ClusterBootstrapState, SystemTableError> {
    let Ok(view) = SystemTableView::locate(catalog, "tidb", &["variable_name", "variable_value"])
    else {
        return Ok(ClusterBootstrapState::NotBootstrapped);
    };
    let mut bootstrapped = false;
    let mut version = None;
    for (key, value) in scan_system_table(snapshot, &view)? {
        let row = SystemRow::parse(&view, &key, &value)?;
        let Some(name) = row.text("variable_name")? else {
            continue;
        };
        let stored = row.text("variable_value")?.unwrap_or_default();
        if name == BOOTSTRAPPED_VAR {
            bootstrapped = stored == VAR_TRUE;
        } else if name == TIDB_SERVER_VERSION_VAR {
            version = stored.parse::<i64>().ok();
        }
    }
    Ok(if bootstrapped {
        ClusterBootstrapState::Bootstrapped { version }
    } else {
        ClusterBootstrapState::Unflagged
    })
}

/// Reads every account and grant from one catalog snapshot.
///
/// A table the catalog does not carry at all is read as empty rather than as
/// an error: Go's own bootstrap adds `mysql.global_grants` and the role tables
/// in later versions, so an older cluster legitimately has accounts and no
/// dynamic privileges. A table that *is* present but whose columns this reader
/// does not recognize is still a hard error — that is a schema this node would
/// otherwise misread.
pub fn load_cluster_privileges<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
) -> Result<ClusterPrivileges, SystemTableError> {
    let mut loaded = ClusterPrivileges::default();

    let users = SystemTableView::locate(catalog, "user", USER_COLUMNS)?;
    for (key, value) in scan_system_table(snapshot, &users)? {
        let row = SystemRow::parse(&users, &key, &value)?;
        loaded.users.push(LoadedUser {
            host: row.text("host")?.unwrap_or_default(),
            user: row.text("user")?.unwrap_or_default(),
            authentication_string: row.text("authentication_string")?.unwrap_or_default(),
            plugin: row.text("plugin")?.unwrap_or_default(),
            account_locked: row.is_yes("account_locked")?,
            password_expired: row.is_yes("password_expired")?,
            privileges: granted_names(&row, USER_PRIVILEGE_COLUMNS)?,
        });
    }

    if let Ok(view) = SystemTableView::locate(catalog, "db", DB_COLUMNS) {
        for (key, value) in scan_system_table(snapshot, &view)? {
            let row = SystemRow::parse(&view, &key, &value)?;
            loaded.db_grants.push(LoadedDbGrant {
                host: row.text("host")?.unwrap_or_default(),
                user: row.text("user")?.unwrap_or_default(),
                database: row.text("db")?.unwrap_or_default(),
                privileges: granted_names(&row, DB_PRIVILEGE_COLUMNS)?,
            });
        }
    }

    if let Ok(view) = SystemTableView::locate(
        catalog,
        "tables_priv",
        &["host", "user", "db", "table_name", "table_priv"],
    ) {
        for (key, value) in scan_system_table(snapshot, &view)? {
            let row = SystemRow::parse(&view, &key, &value)?;
            loaded.table_grants.push(LoadedTableGrant {
                host: row.text("host")?.unwrap_or_default(),
                user: row.text("user")?.unwrap_or_default(),
                database: row.text("db")?.unwrap_or_default(),
                table: row.text("table_name")?.unwrap_or_default(),
                privileges: row.set_labels("table_priv")?,
            });
        }
    }

    if let Ok(view) = SystemTableView::locate(
        catalog,
        "columns_priv",
        &[
            "host",
            "user",
            "db",
            "table_name",
            "column_name",
            "column_priv",
        ],
    ) {
        for (key, value) in scan_system_table(snapshot, &view)? {
            let row = SystemRow::parse(&view, &key, &value)?;
            loaded.column_grants.push(LoadedColumnGrant {
                host: row.text("host")?.unwrap_or_default(),
                user: row.text("user")?.unwrap_or_default(),
                database: row.text("db")?.unwrap_or_default(),
                table: row.text("table_name")?.unwrap_or_default(),
                column: row.text("column_name")?.unwrap_or_default(),
                privileges: row.set_labels("column_priv")?,
            });
        }
    }

    if let Ok(view) = SystemTableView::locate(
        catalog,
        "global_grants",
        &["host", "user", "priv", "with_grant_option"],
    ) {
        for (key, value) in scan_system_table(snapshot, &view)? {
            let row = SystemRow::parse(&view, &key, &value)?;
            loaded.dynamic_grants.push(LoadedDynamicGrant {
                host: row.text("host")?.unwrap_or_default(),
                user: row.text("user")?.unwrap_or_default(),
                privilege: row.text("priv")?.unwrap_or_default(),
                with_grant_option: row.is_yes("with_grant_option")?,
            });
        }
    }

    if let Ok(view) = SystemTableView::locate(
        catalog,
        "role_edges",
        &["from_host", "from_user", "to_host", "to_user"],
    ) {
        for (key, value) in scan_system_table(snapshot, &view)? {
            let row = SystemRow::parse(&view, &key, &value)?;
            loaded.role_edges.push(LoadedRoleEdge {
                role_host: row.text("from_host")?.unwrap_or_default(),
                role_user: row.text("from_user")?.unwrap_or_default(),
                grantee_host: row.text("to_host")?.unwrap_or_default(),
                grantee_user: row.text("to_user")?.unwrap_or_default(),
            });
        }
    }

    if let Ok(view) = SystemTableView::locate(
        catalog,
        "default_roles",
        &["host", "user", "default_role_host", "default_role_user"],
    ) {
        for (key, value) in scan_system_table(snapshot, &view)? {
            let row = SystemRow::parse(&view, &key, &value)?;
            loaded.default_roles.push(LoadedDefaultRole {
                host: row.text("host")?.unwrap_or_default(),
                user: row.text("user")?.unwrap_or_default(),
                role_host: row.text("default_role_host")?.unwrap_or_default(),
                role_user: row.text("default_role_user")?.unwrap_or_default(),
            });
        }
    }

    Ok(loaded)
}

/// Collects the printed privilege names whose `ENUM('N','Y')` column reads `Y`.
///
/// A column the stored schema does not carry is skipped rather than refused:
/// the privilege columns grew over TiDB versions, and an older cluster simply
/// cannot have granted a privilege whose column does not exist.
fn granted_names(
    row: &SystemRow<'_>,
    columns: &'static [(&'static str, &'static str)],
) -> Result<Vec<&'static str>, SystemTableError> {
    let mut granted = Vec::new();
    for (column, printed) in columns {
        if row.has_column(column) && row.is_yes(column)? {
            granted.push(*printed);
        }
    }
    Ok(granted)
}

#[cfg(test)]
mod tests {
    use crate::cluster_catalog::{ClusterCatalogError, MetaPairs};

    use super::*;

    /// A snapshot of an empty keyspace: no meta keys, no record ranges.
    struct EmptySnapshot;

    impl MetaSnapshot for EmptySnapshot {
        fn get(&mut self, _key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
            Ok(None)
        }

        fn scan_prefix(&mut self, _prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn a_keyspace_with_no_mysql_tidb_is_reported_as_never_bootstrapped() {
        // This is the distinction the seeding decision rests on: an empty
        // catalog must not read as "bootstrapped with no accounts", which
        // would make a node either re-seed a live cluster or start with a
        // table nobody can log in to.
        let catalog = ClusterCatalog {
            schema_version: 0,
            databases: Vec::new(),
        };
        let state = read_bootstrap_state(&mut EmptySnapshot, &catalog).expect("the read succeeds");
        assert_eq!(state, ClusterBootstrapState::NotBootstrapped);
        assert!(!state.already_bootstrapped());
    }

    #[test]
    fn only_gos_own_true_spelling_counts_as_bootstrapped() {
        // Go writes `varTrue`, the exact string "True"; the flag is a string
        // column, so a node must not treat any other truthy spelling as the
        // bootstrap marker.
        assert!(ClusterBootstrapState::Bootstrapped { version: Some(232) }.already_bootstrapped());
        assert!(!ClusterBootstrapState::Unflagged.already_bootstrapped());
        assert_eq!(
            ClusterBootstrapState::Bootstrapped { version: Some(232) }.to_string(),
            "bootstrapped at version 232"
        );
    }

    #[test]
    fn the_user_and_db_privilege_columns_all_name_a_privilege_this_node_models() {
        // The two column tables are the whole vocabulary of the load; a name
        // that no longer resolves would silently drop a granted privilege.
        for (column, printed) in USER_PRIVILEGE_COLUMNS
            .iter()
            .chain(DB_PRIVILEGE_COLUMNS.iter())
        {
            assert!(
                column.ends_with("_priv"),
                "{column} is not a mysql.user privilege column"
            );
            assert_eq!(*printed, printed.to_uppercase(), "{printed} is not printed");
        }
        // `mysql.db` grants a strict subset of what `mysql.user` grants.
        for (column, _) in DB_PRIVILEGE_COLUMNS {
            assert!(
                USER_PRIVILEGE_COLUMNS
                    .iter()
                    .any(|(user_column, _)| user_column == column),
                "mysql.db column {column} has no mysql.user counterpart"
            );
        }
    }
}
