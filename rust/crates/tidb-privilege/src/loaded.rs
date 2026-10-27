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

//! The `mysql.*` privilege rows Go's `MySQLPrivilege.LoadAll` reads, as
//! records.

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
    /// Nullable per-account expiry interval; NULL means the live global default.
    pub password_lifetime: Option<i64>,
    /// `max_user_connections` (zero means no per-account limit).
    pub max_user_connections: i64,
    /// Stored UTC TIMESTAMP, retained without replacing its epoch on reload.
    pub password_last_changed: Option<tidb_datatype::Time>,
    /// NULL selects the live global count/time policy; zero disables it.
    pub password_reuse_history: Option<i64>,
    /// Nullable password reuse interval in days.
    pub password_reuse_time: Option<i64>,
    /// Decoded locking cache; raw JSON is retained separately for writeback.
    pub password_locking: Option<crate::account_policy::PasswordLocking>,
    /// Original user attributes, including locking state and unrelated metadata.
    pub user_attributes: Option<String>,
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

/// One durable credential-history row, independent of current account existence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedPasswordHistory {
    /// Account host.
    pub host: String,
    /// Account username.
    pub user: String,
    /// Stored UTC TIMESTAMP(6), part of the row identity.
    pub timestamp: tidb_datatype::Time,
    /// Stored authentication string.
    pub password: Option<String>,
}

/// One durable connection-policy row; JSON text is retained for Go consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedGlobalPriv {
    /// Account host pattern.
    pub host: String,
    /// Account user.
    pub user: String,
    /// Unmodified mysql.global_priv.Priv text, including unknown properties.
    pub priv_json: String,
}

/// Everything one snapshot of `mysql.*` says about accounts and grants.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterPrivileges {
    /// Durable connection policy, independent of user-row existence.
    pub global_priv: Vec<LoadedGlobalPriv>,
    /// mysql.password_history, including orphan rows until explicitly removed.
    pub password_history: Vec<LoadedPasswordHistory>,
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
