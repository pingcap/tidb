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

//! The privilege bits themselves: which privileges exist, at which scope,
//! and in which order `SHOW GRANTS` prints them.
//!
//! Go's scope lists (`mysql.AllGlobalPrivs`, `AllDBPrivs`, `AllTablePrivs`,
//! `AllColumnPrivs`) and print names live in `tidb-mysql`, as in Go's
//! `pkg/parser/mysql/privs.go`; this module adds the masks built from them
//! plus the `dynamicPrivs` registry of `pkg/privilege/privileges/privileges.go`.

/// Go `mysql.PrivilegeType`: one static privilege, as a bit of a privilege
/// mask. The session's registry masks are Go's bits.
use tidb_mysql::PrivilegeType;

/// Go `privileges.SSLType` (`privileges/cache.go` line 163): what a
/// `REQUIRE` clause demands of the connection's transport.
///
/// Go's `SslTypeNotSpecified` (-1) and `SslTypeNone` (0) are the same
/// admission answer and the same stored JSON (`ssl_type` is `omitempty`), so
/// they collapse into one variant here rather than being carried as two
/// values that never differ.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SslType {
    /// `REQUIRE NONE`, or no `REQUIRE` clause: every transport is admitted.
    #[default]
    None,
    /// `REQUIRE SSL` (`ssl_type` 1): the connection must be TLS, with no
    /// certificate validation. Go's `SslTypeAny`.
    Any,
    /// `REQUIRE X509` (`ssl_type` 2): TLS plus a VERIFIED client
    /// certificate chain. Go's `SslTypeX509`.
    X509,
    /// `REQUIRE CIPHER/ISSUER/SUBJECT/SAN` (`ssl_type` 3): TLS plus the
    /// named certificate or cipher property. Go's `SslTypeSpecified`.
    Specified,
}

impl SslType {
    /// Compatibility check without verified peer certificate evidence.
    #[must_use]
    pub fn admits(self, is_tls: bool) -> bool {
        self.admits_verified(is_tls, false)
    }

    /// Go checkSSL requires a nonempty verified chain for REQUIRE X509.
    #[must_use]
    pub fn admits_verified(self, is_tls: bool, verified_client: bool) -> bool {
        match self {
            Self::None => true,
            Self::Any => is_tls,
            Self::X509 => is_tls && verified_client,
            Self::Specified => false,
        }
    }

    /// The `REQUIRE <...>` clause `SHOW CREATE USER` prints
    /// (`executor/show.go`'s `fetchShowCreateUser`).
    #[must_use]
    pub fn show_create_user_clause(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Any => "SSL",
            Self::X509 => "X509",
            Self::Specified => "CIPHER",
        }
    }
}

/// One mask built from a variant list, so every named mask below is one
/// expression rather than a hand-maintained constant that could drift from
/// the enum.
fn mask_of(privs: &[PrivilegeType]) -> u64 {
    privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit())
}

/// Go `mysql.AllPrivMask`: every bit set, `GRANT OPTION` included. The
/// "does this account hold ANY privilege here" mask that `SHOW TABLES` and
/// the `information_schema` retrievers test with.
#[must_use]
pub fn any_priv_mask() -> u64 {
    all_privs_mask() | tidb_mysql::GrantPriv.bit()
}

/// Go's `mysql.AllPrivMask &^ mysql.CreateTMPTablePriv`, the exact mask
/// `fetchShowTables` (`executor/show.go` around line 613) filters with: a
/// `CREATE TEMPORARY TABLES` grant alone does not make a table listed.
#[must_use]
pub fn show_tables_priv_mask() -> u64 {
    any_priv_mask() & !tidb_mysql::CreateTMPTablePriv.bit()
}

/// Go `mysql.AllColumnPrivs`, the four privileges
/// `information_schema.COLUMNS` accepts (`infoschema_reader.go` around line
/// 1095).
#[must_use]
pub fn column_privs_mask() -> u64 {
    mask_of(&[
        tidb_mysql::SelectPriv,
        tidb_mysql::InsertPriv,
        tidb_mysql::UpdatePriv,
        tidb_mysql::ReferencesPriv,
    ])
}

/// Go's `globalDBVisible` (`privileges/cache.go` line 60): the GLOBAL
/// privileges that alone make every schema appear in `SHOW DATABASES`.
///
/// Note what is ABSENT and is not an oversight in either language:
/// `LOCK TABLES`, and every server-admin privilege (`PROCESS`, `SUPER`,
/// `CREATE USER`, `RELOAD`, `FILE`, ...). `PROCESS` gets its own
/// `metrics_schema`-only arm in [`super::PrivilegeRegistry::db_is_visible`].
pub(crate) fn global_db_visible_mask() -> u64 {
    mask_of(&[
        tidb_mysql::CreatePriv,
        tidb_mysql::SelectPriv,
        tidb_mysql::InsertPriv,
        tidb_mysql::UpdatePriv,
        tidb_mysql::DeletePriv,
        tidb_mysql::ShowDBPriv,
        tidb_mysql::DropPriv,
        tidb_mysql::AlterPriv,
        tidb_mysql::IndexPriv,
        tidb_mysql::CreateViewPriv,
        tidb_mysql::ShowViewPriv,
        tidb_mysql::OperateViewPriv,
        tidb_mysql::GrantPriv,
        tidb_mysql::TriggerPriv,
        tidb_mysql::ReferencesPriv,
        tidb_mysql::ExecutePriv,
        tidb_mysql::CreateTMPTablePriv,
    ])
}

/// The mask with every privilege in [`tidb_mysql::ALL_GLOBAL_PRIVS`] set, which is what
/// `ALL PRIVILEGES` grants and what makes `SHOW GRANTS` print the
/// `ALL PRIVILEGES` literal instead of an enumerated list (Go
/// `userPrivToString`).
pub fn all_privs_mask() -> u64 {
    tidb_mysql::ALL_GLOBAL_PRIVS
        .iter()
        .fold(0u64, |mask, priv_| mask | priv_.bit())
}

/// The privilege-type helpers the registry and its statements read beside
/// Go's own tables (`PrivilegeType::as_str` is `Priv2Str`, `set_string` is
/// `Priv2SetStr`, `column_string` is `Priv2UserCol`).
pub trait PrivilegeTypeExt: Copy {
    /// The privilege's bit mask.
    fn bit(self) -> u64;
    /// The privilege's bit mask, the unit every `grant`/`revoke` method on
    /// [`PrivilegeRegistry`] takes.
    fn mask(self) -> u64 {
        self.bit()
    }
    /// Go `mysql.PrivilegeType.String()` (`Priv2Str`), which
    /// `ErrPrivilegeCheckFail` (8121) names.
    fn check_fail_name(self) -> &'static str;
    /// Go `mysql.Priv2SetStr`.
    fn set_name(self) -> &'static str;
    /// Go `mysql.Priv2UserCol`.
    fn user_table_column(self) -> &'static str;
    /// Whether Go's `grantDBLevel` accepts this privilege.
    fn is_valid_at_db_scope(self) -> bool;
    /// Whether Go's TABLE-scope grant path accepts this privilege.
    fn is_valid_at_table_scope(self) -> bool;
    /// Whether this privilege may carry a COLUMN list.
    fn is_valid_at_column_scope(self) -> bool;
}

impl PrivilegeTypeExt for PrivilegeType {
    fn bit(self) -> u64 {
        self.0
    }

    fn check_fail_name(self) -> &'static str {
        self.as_str()
    }

    fn set_name(self) -> &'static str {
        self.set_string()
    }

    fn user_table_column(self) -> &'static str {
        self.column_string()
    }

    fn is_valid_at_db_scope(self) -> bool {
        self == tidb_mysql::GrantPriv || tidb_mysql::ALL_DB_PRIVS.contains(&self)
    }

    fn is_valid_at_table_scope(self) -> bool {
        self == tidb_mysql::GrantPriv || tidb_mysql::ALL_TABLE_PRIVS.contains(&self)
    }

    fn is_valid_at_column_scope(self) -> bool {
        tidb_mysql::ALL_COLUMN_PRIVS.contains(&self)
    }
}

/// Resolves the spelling `tidb-parser`'s `GrantPrivilege::name` restores for
/// a standard (non-dynamic) privilege token (Go's parser keyword); `None`
/// for `ALL`, `USAGE`, a role or a dynamic name, which the caller refuses or
/// expands itself. `GRANT OPTION` resolves like any other privilege.
#[must_use]
pub fn privilege_from_grant_name(name: &str) -> Option<PrivilegeType> {
    tidb_mysql::privilege_from_grant_name(name)
        .filter(|privilege| *privilege != tidb_mysql::AllPriv && *privilege != tidb_mysql::UsagePriv)
}

/// The mask with every privilege in [`tidb_mysql::ALL_DB_PRIVS`] set -- what
/// `ALL PRIVILEGES ON db.*` grants and what collapses `SHOW GRANTS`'s
/// DB-scope line to the `ALL PRIVILEGES` literal (Go `dbPrivToString`).
pub(crate) fn all_db_privs_mask() -> u64 {
    tidb_mysql::ALL_DB_PRIVS
        .iter()
        .fold(0u64, |mask, priv_| mask | priv_.bit())
}

/// The mask with every privilege in [`tidb_mysql::ALL_COLUMN_PRIVS`] set -- what
/// `GRANT ALL (col) ON db.t` expands to (captured: `ALL (a)` is accepted and
/// prints as all four column privileges on `a`).
pub fn all_column_privs_mask() -> u64 {
    tidb_mysql::ALL_COLUMN_PRIVS
        .iter()
        .fold(0u64, |mask, priv_| mask | priv_.bit())
}

/// The mask with every privilege in [`tidb_mysql::ALL_TABLE_PRIVS`] set -- the
/// TABLE-scope analogue of [`all_db_privs_mask`] (Go `tablePrivToString`).
pub(crate) fn all_table_privs_mask() -> u64 {
    tidb_mysql::ALL_TABLE_PRIVS
        .iter()
        .fold(0u64, |mask, priv_| mask | priv_.bit())
}

/// Go `pkg/privilege/privileges/privileges.go`'s `dynamicPrivs` slice --
/// the DYNAMIC privilege names TiDB registers at startup, in Go's source
/// order. A `GRANT`/`REVOKE` naming anything outside this list is not a
/// privilege at all (`ErrDynamicPrivilegeNotRegistered`/3929).
///
/// Go lets plugins append to this list at runtime
/// (`RegisterDynamicPrivilege`); this tier loads no plugins, so the built-in
/// set is the whole set and a `const` is the honest shape.
pub const DYNAMIC_PRIVS: &[&str] = &[
    "BACKUP_ADMIN",
    "RESTORE_ADMIN",
    "SYSTEM_USER",
    "SYSTEM_VARIABLES_ADMIN",
    "ROLE_ADMIN",
    "CONNECTION_ADMIN",
    "PLACEMENT_ADMIN",
    "DASHBOARD_CLIENT",
    "RESTRICTED_TABLES_ADMIN",
    "RESTRICTED_STATUS_ADMIN",
    "RESTRICTED_VARIABLES_ADMIN",
    "RESTRICTED_USER_ADMIN",
    "RESTRICTED_CONNECTION_ADMIN",
    "RESTRICTED_REPLICA_WRITER_ADMIN",
    "RESTRICTED_PRIV_ADMIN",
    "RESTRICTED_SQL_ADMIN",
    "RESOURCE_GROUP_ADMIN",
    "RESOURCE_GROUP_USER",
    "TRAFFIC_CAPTURE_ADMIN",
    "TRAFFIC_REPLAY_ADMIN",
    "APPLICATION_PASSWORD_ADMIN",
];

/// Go `UserPrivileges.IsDynamicPrivilege`: whether `name` -- matched
/// case-insensitively, since Go uppercases before the lookup -- is a
/// registered DYNAMIC privilege.
#[must_use]
pub fn is_dynamic_privilege(name: &str) -> bool {
    // Go `strings.ToUpper(privName) == priv`.
    let upper = tidb_hack::go_to_upper(name);
    DYNAMIC_PRIVS.iter().any(|registered| *registered == upper)
}
