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

//! The read-only view of the registry a caller can export, and the
//! privilege-list rendering `SHOW GRANTS` shares with it.
//!
//! Mirrors the row shapes Go reads back out of `mysql.user`, `mysql.db`,
//! `mysql.tables_priv` and `mysql.global_grants`, and the privilege-list
//! formatting of `MySQLPrivilege.showGrants`.

use super::{Account, GlobalPriv, ALL_GLOBAL_PRIVS};

/// The printed names of every static privilege a mask carries, in the order
/// `SHOW GRANTS` prints them -- the same spelling
/// [`GlobalPriv::from_grant_name`] resolves, so the export round-trips
/// through a `mysql.*` row.
pub(super) fn printed_privileges(mask: u64) -> Vec<&'static str> {
    ALL_GLOBAL_PRIVS
        .iter()
        .chain(std::iter::once(&GlobalPriv::GrantOption))
        .filter(|privilege| mask & privilege.bit() != 0)
        .map(|privilege| privilege.print_name())
        .collect()
}

/// One `mysql.user` row as this registry holds it.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ExportedUser {
    /// `User`.
    pub user: String,
    /// `Host`.
    pub host: String,
    /// `authentication_string`.
    pub auth_string: String,
    /// `plugin`.
    pub plugin: String,
    /// `Account_locked = 'Y'`, which is how Go stores a ROLE.
    pub account_locked: bool,
    /// `Password_expired = 'Y'`.
    pub password_expired: bool,
    /// Printed names of the global privileges this account holds.
    pub privileges: Vec<&'static str>,
}

/// One `mysql.db` / `mysql.tables_priv` / `mysql.columns_priv` row. The
/// unused scope columns are empty, which is how the three tables share one
/// shape without an enum whose variants nobody matches on.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ExportedScopedGrant {
    /// `User`.
    pub user: String,
    /// `Host`.
    pub host: String,
    /// `DB`.
    pub database: String,
    /// `Table_name`, empty for a database-scoped grant.
    pub table: String,
    /// `Column_name`, empty above column scope.
    pub column: String,
    /// Printed names of the privileges the row grants.
    pub privileges: Vec<&'static str>,
}

/// One `mysql.global_grants` row.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ExportedDynamicGrant {
    /// `User`.
    pub user: String,
    /// `Host`.
    pub host: String,
    /// `PRIV`, uppercase.
    pub privilege: String,
    /// `WITH_GRANT_OPTION = 'Y'`.
    pub with_grant_option: bool,
}

/// This registry's whole account table, as rows.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RegistryExport {
    /// `mysql.user`.
    pub users: Vec<ExportedUser>,
    /// `mysql.db`.
    pub db_grants: Vec<ExportedScopedGrant>,
    /// `mysql.tables_priv`.
    pub table_grants: Vec<ExportedScopedGrant>,
    /// `mysql.columns_priv`.
    pub column_grants: Vec<ExportedScopedGrant>,
    /// `mysql.global_grants`.
    pub dynamic_grants: Vec<ExportedDynamicGrant>,
    /// `mysql.role_edges`, as `(role, grantee)` -- the `FROM`/`TO` pair in
    /// the column order the stored table uses.
    pub role_edges: Vec<(Account, Account)>,
    /// `mysql.default_roles`, as `(account, role)`.
    pub default_roles: Vec<(Account, Account)>,
}

/// The ` WITH GRANT OPTION` suffix `SHOW GRANTS` appends to a line whose
/// privilege mask carries `mysql.GrantPriv`. Captured at all three scopes:
/// the suffix trails the whole `GRANT ... TO '<user>'@'<host>'` line, and
/// `GRANT OPTION` never appears inside the privilege list.
pub(super) fn grant_option_suffix(privs: u64) -> &'static str {
    if privs & GlobalPriv::GrantOption.bit() == 0 {
        ""
    } else {
        " WITH GRANT OPTION"
    }
}

/// The comma-joined privilege names of `privs` in `order`'s print order, or
/// the `USAGE` literal Go prints for a row with no printable privilege
/// ("this is a mysql convention"). `GRANT OPTION` is in no `order` list, so
/// it never lands here.
pub(super) fn priv_list(privs: u64, order: &[GlobalPriv]) -> String {
    let names: Vec<&str> = order
        .iter()
        .filter(|priv_| privs & priv_.bit() != 0)
        .map(|priv_| priv_.print_name())
        .collect();
    if names.is_empty() {
        "USAGE".to_owned()
    } else {
        names.join(",")
    }
}
