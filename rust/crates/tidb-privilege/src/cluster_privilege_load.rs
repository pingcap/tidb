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
//! `mysql.global_grants`. [`load_all`] reads the same tables through any
//! [`PrivilegeTables`]: the domain's system sessions running Go's `SELECT`s
//! (the node's startup load and `LoadPrivilegeLoop`).
//!
//! `password_last_changed` is returned in UTC so publication preserves the
//! stored instant rather than interpreting it in a session timezone.
//!
//! Connection-policy text is loaded independently, including orphan rows.

use std::collections::BTreeSet;

use tidb_syssession::mysql_system_tables::{SystemRow, SystemTableError, SystemTableView};

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
    ("operate_view_priv", "OPERATE VIEW"),
    ("create_role_priv", "CREATE ROLE"),
    ("drop_role_priv", "DROP ROLE"),
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
    ("operate_view_priv", "OPERATE VIEW"),
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
    "password_lifetime",
    "max_user_connections",
    "password_last_changed",
    "password_reuse_history",
    "password_reuse_time",
    "user_attributes",
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
    "operate_view_priv",
    "create_role_priv",
    "drop_role_priv",
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
    "operate_view_priv",
    "create_routine_priv",
    "alter_routine_priv",
    "execute_priv",
    "event_priv",
    "trigger_priv",
];

pub use crate::loaded::{LoadedUser, LoadedDbGrant, LoadedTableGrant, LoadedColumnGrant, LoadedDynamicGrant, LoadedRoleEdge, LoadedDefaultRole, LoadedGlobalPriv, ClusterPrivileges};

/// Where [`load_all`] reads `mysql.<table>` from: Go's `loadTable` statements
/// on a system session.
pub trait PrivilegeTables {
    /// `mysql.<table>` projected to `columns`, with each row's decoded column
    /// values keyed by column ID, or `None` when the table does not exist.
    /// `timezone` decodes TIMESTAMP columns. `users` is Go's
    /// `addUserFilterCondition`: a source that can narrow the read to those
    /// users' rows does; the loader keeps only their rows either way.
    ///
    /// # Errors
    ///
    /// A row the source cannot decode.
    fn rows(
        &mut self,
        table: &str,
        columns: &[&str],
        timezone: Option<&tidb_datatype::SessionTimeZone>,
        users: Option<&BTreeSet<String>>,
    ) -> Result<Option<PrivilegeTableRows>, SystemTableError>;
}

/// One `mysql.<table>`'s projected view and its rows' column values.
pub type PrivilegeTableRows = (
    SystemTableView,
    Vec<std::collections::BTreeMap<i64, tidb_datatype::Datum>>,
);

/// Go `MySQLPrivilege.LoadAll` over any [`PrivilegeTables`]: one read per
/// `mysql.*` table, `mysql.user` required and every other table optional.
///
/// # Errors
///
/// A missing `mysql.user`, or a row whose stored value is not what its
/// column declares.
pub fn load_all(
    tables: &mut (impl PrivilegeTables + ?Sized),
) -> Result<ClusterPrivileges, SystemTableError> {
    let mut loaded = load_tables(tables, None)?;
    if let Some(role_edges) = read_role_graph(tables)? {
        loaded.role_edges = role_edges;
    }
    Ok(loaded)
}

/// Go `MySQLPrivilege.loadSomeUsers`: the rows of `users` in every table but
/// `mysql.role_edges`, which Go's `updateUsers` reads whole beforehand
/// ([`load_role_graph`]); each table is required here.
///
/// # Errors
///
/// A missing table, or a row whose stored value is not what its column
/// declares.
pub fn load_some_users(
    tables: &mut (impl PrivilegeTables + ?Sized),
    users: &BTreeSet<String>,
) -> Result<ClusterPrivileges, SystemTableError> {
    load_tables(tables, Some(users))
}

/// Go `loadTable(exec, sqlLoadRoleGraph, p.decodeRoleEdgesTable)`: the whole
/// `mysql.role_edges`, which `updateUsers` reads before it picks the users
/// to load.
///
/// # Errors
///
/// A missing table, or a row that does not decode.
pub fn load_role_graph(
    tables: &mut (impl PrivilegeTables + ?Sized),
) -> Result<Vec<LoadedRoleEdge>, SystemTableError> {
    read_role_graph(tables)?.ok_or_else(|| SystemTableError::Missing {
        name: "mysql.role_edges".to_owned(),
    })
}

/// Go `findUserAndAllRoles`: `user_list` and, transitively, every role
/// granted to one of them.
#[must_use]
pub fn find_user_and_all_roles(user_list: &[String], role_edges: &[LoadedRoleEdge]) -> BTreeSet<String> {
    let mut all: BTreeSet<String> = user_list.iter().cloned().collect();
    let mut queue: std::collections::VecDeque<String> = user_list.iter().cloned().collect();
    while let Some(user) = queue.pop_front() {
        for edge in role_edges.iter().filter(|edge| edge.grantee_user == user) {
            if all.insert(edge.role_user.clone()) {
                queue.push_back(edge.role_user.clone());
            }
        }
    }
    all
}

/// Go `LoadRoleGraph`'s read; `None` for a missing table.
fn read_role_graph(
    tables: &mut (impl PrivilegeTables + ?Sized),
) -> Result<Option<Vec<LoadedRoleEdge>>, SystemTableError> {
    let Some((view, rows)) = tables.rows(
        "role_edges",
        &["from_host", "from_user", "to_host", "to_user"],
        None,
        None,
    )?
    else {
        return Ok(None);
    };
    let mut role_edges = Vec::with_capacity(rows.len());
    for values in rows {
        let row = SystemRow::from_values(&view, values);
        role_edges.push(LoadedRoleEdge {
            role_host: row.text("from_host")?.unwrap_or_default(),
            role_user: row.text("from_user")?.unwrap_or_default(),
            grantee_host: row.text("to_host")?.unwrap_or_default(),
            grantee_user: row.text("to_user")?.unwrap_or_default(),
        });
    }
    Ok(Some(role_edges))
}

/// Go `LoadAll`'s table reads without the role graph (`users` `None`), or
/// `loadSomeUsers`' (each `decode*TableRow(userList)` keeping only those
/// users' rows, every table required).
fn load_tables(
    tables: &mut (impl PrivilegeTables + ?Sized),
    users: Option<&BTreeSet<String>>,
) -> Result<ClusterPrivileges, SystemTableError> {
    let mut loaded = ClusterPrivileges::default();
    // Go: a `loadSomeUsers` read of a missing table is an error; `LoadAll`
    // skips every optional one.
    let mut rows_of = |table: &str,
                       columns: &[&str],
                       timezone: Option<&tidb_datatype::SessionTimeZone>|
     -> Result<Option<PrivilegeTableRows>, SystemTableError> {
        match tables.rows(table, columns, timezone, users)? {
            None if users.is_some() => Err(SystemTableError::Missing {
                name: format!("mysql.{table}"),
            }),
            rows => Ok(rows),
        }
    };
    let wanted = |row: &SystemRow<'_>| -> Result<bool, SystemTableError> {
        Ok(match users {
            Some(users) => users.contains(&row.text("user")?.unwrap_or_default()),
            None => true,
        })
    };

    let timezone = tidb_datatype::SessionTimeZone::utc();
    let Some((users_view, user_rows)) = rows_of("user", USER_COLUMNS, Some(&timezone))? else {
        return Err(SystemTableError::Missing {
            name: "mysql.user".to_owned(),
        });
    };
    for values in user_rows {
        let row = SystemRow::from_values(&users_view, values);
        if !wanted(&row)? {
            continue;
        }
        let user_attributes = if row.has_column("user_attributes") {
            match row.datum("user_attributes")? {
                Some(tidb_datatype::Datum::Json(json)) => Some(json.to_string()),
                None => None,
                Some(value) => {
                    return Err(SystemTableError::Decode {
                        name: "mysql.user".into(),
                        detail: format!("user_attributes is not JSON: {value:?}"),
                    })
                }
            }
        } else {
            None
        };
        let password_locking =
            crate::account_policy::PasswordLocking::from_attributes(user_attributes.as_deref())
                .map_err(|detail| SystemTableError::Decode {
                    name: "mysql.user".into(),
                    detail,
                })?;
        loaded.users.push(LoadedUser {
            host: row.text("host")?.unwrap_or_default(),
            user: row.text("user")?.unwrap_or_default(),
            authentication_string: row.text("authentication_string")?.unwrap_or_default(),
            plugin: row.text("plugin")?.unwrap_or_default(),
            account_locked: row.is_yes("account_locked")?,
            password_expired: row.is_yes("password_expired")?,
            password_lifetime: if row.has_column("password_lifetime") {
                row.i64("password_lifetime")?
            } else {
                None
            },
            max_user_connections: if row.has_column("max_user_connections") {
                row.i64("max_user_connections")?.unwrap_or(0)
            } else {
                0
            },
            password_last_changed: if row.has_column("password_last_changed") {
                match row.datum("password_last_changed")? {
                    Some(tidb_datatype::Datum::Time(value)) => Some(*value),
                    None => None,
                    Some(value) => {
                        return Err(SystemTableError::Decode {
                            name: "mysql.user".to_owned(),
                            detail: format!("password_last_changed is not a TIMESTAMP: {value:?}"),
                        })
                    }
                }
            } else {
                None
            },
            password_reuse_history: if row.has_column("password_reuse_history") {
                row.i64("password_reuse_history")?
            } else {
                None
            },
            password_reuse_time: if row.has_column("password_reuse_time") {
                row.i64("password_reuse_time")?
            } else {
                None
            },
            user_attributes,
            password_locking,
            privileges: granted_names(&row, USER_PRIVILEGE_COLUMNS)?,
        });
    }

    if let Some((view, rows)) = rows_of("global_priv", &["host", "user", "priv"], None)? {
        for values in rows {
            let row = SystemRow::from_values(&view, values);
            if !wanted(&row)? {
                continue;
            }
            loaded.global_priv.push(LoadedGlobalPriv {
                host: row.text("host")?.unwrap_or_default(),
                user: row.text("user")?.unwrap_or_default(),
                priv_json: row.text("priv")?.unwrap_or_default(),
            });
        }
    }

    if let Some((view, rows)) = rows_of("db", DB_COLUMNS, None)? {
        for values in rows {
            let row = SystemRow::from_values(&view, values);
            if !wanted(&row)? {
                continue;
            }
            loaded.db_grants.push(LoadedDbGrant {
                host: row.text("host")?.unwrap_or_default(),
                user: row.text("user")?.unwrap_or_default(),
                database: row.text("db")?.unwrap_or_default(),
                privileges: granted_names(&row, DB_PRIVILEGE_COLUMNS)?,
            });
        }
    }

    if let Some((view, rows)) = rows_of(
        "tables_priv",
        &["host", "user", "db", "table_name", "table_priv"],
        None,
    )? {
        for values in rows {
            let row = SystemRow::from_values(&view, values);
            if !wanted(&row)? {
                continue;
            }
            loaded.table_grants.push(LoadedTableGrant {
                host: row.text("host")?.unwrap_or_default(),
                user: row.text("user")?.unwrap_or_default(),
                database: row.text("db")?.unwrap_or_default(),
                table: row.text("table_name")?.unwrap_or_default(),
                privileges: row.set_labels("table_priv")?,
            });
        }
    }

    if let Some((view, rows)) = rows_of(
        "columns_priv",
        &[
            "host",
            "user",
            "db",
            "table_name",
            "column_name",
            "column_priv",
        ],
        None,
    )? {
        for values in rows {
            let row = SystemRow::from_values(&view, values);
            if !wanted(&row)? {
                continue;
            }
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

    if let Some((view, rows)) = rows_of(
        "global_grants",
        &["host", "user", "priv", "with_grant_option"],
        None,
    )? {
        for values in rows {
            let row = SystemRow::from_values(&view, values);
            if !wanted(&row)? {
                continue;
            }
            loaded.dynamic_grants.push(LoadedDynamicGrant {
                host: row.text("host")?.unwrap_or_default(),
                user: row.text("user")?.unwrap_or_default(),
                privilege: row.text("priv")?.unwrap_or_default(),
                with_grant_option: row.is_yes("with_grant_option")?,
            });
        }
    }

    if let Some((view, rows)) = rows_of(
        "default_roles",
        &["host", "user", "default_role_host", "default_role_user"],
        None,
    )? {
        for values in rows {
            let row = SystemRow::from_values(&view, values);
            if !wanted(&row)? {
                continue;
            }
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
    use super::*;

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
