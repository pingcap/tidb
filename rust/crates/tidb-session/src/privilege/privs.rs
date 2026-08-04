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
//! Mirrors Go `pkg/parser/mysql/privs.go` (`AllGlobalPrivs`, `AllDBPrivs`,
//! `AllTablePrivs`, `AllColumnPrivs` and their print names) plus the
//! `dynamicPrivs` registry of `pkg/privilege/privileges/privileges.go`.

/// One MySQL-standard privilege TiDB grants at global (`*.*`) scope.
///
/// Order matches Go's `mysql.AllGlobalPrivs`, which is also the order
/// `SHOW GRANTS` prints privileges in -- NOT insertion order, NOT
/// alphabetical order. Keep this list and [`ALL_GLOBAL_PRIVS`] in lockstep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)] // Each variant is one MySQL privilege; see `print_name`/`from_grant_name`.
pub enum GlobalPriv {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Process,
    References,
    Alter,
    ShowDatabases,
    Super,
    Execute,
    Index,
    CreateUser,
    CreateTablespace,
    Trigger,
    CreateView,
    ShowView,
    CreateRole,
    DropRole,
    CreateTemporaryTables,
    LockTables,
    CreateRoutine,
    AlterRoutine,
    Event,
    Shutdown,
    Reload,
    File,
    Config,
    ReplicationClient,
    ReplicationSlave,
    /// Go `mysql.GrantPriv`. Never a member of any `ALL_*` list; see the
    /// module doc.
    GrantOption,
}

/// Go `mysql.AllGlobalPrivs` -- the print/iteration order `SHOW GRANTS`
/// uses, and the exact list `GRANT ALL PRIVILEGES` expands to.
pub const ALL_GLOBAL_PRIVS: &[GlobalPriv] = &[
    GlobalPriv::Select,
    GlobalPriv::Insert,
    GlobalPriv::Update,
    GlobalPriv::Delete,
    GlobalPriv::Create,
    GlobalPriv::Drop,
    GlobalPriv::Process,
    GlobalPriv::References,
    GlobalPriv::Alter,
    GlobalPriv::ShowDatabases,
    GlobalPriv::Super,
    GlobalPriv::Execute,
    GlobalPriv::Index,
    GlobalPriv::CreateUser,
    GlobalPriv::CreateTablespace,
    GlobalPriv::Trigger,
    GlobalPriv::CreateView,
    GlobalPriv::ShowView,
    GlobalPriv::CreateRole,
    GlobalPriv::DropRole,
    GlobalPriv::CreateTemporaryTables,
    GlobalPriv::LockTables,
    GlobalPriv::CreateRoutine,
    GlobalPriv::AlterRoutine,
    GlobalPriv::Event,
    GlobalPriv::Shutdown,
    GlobalPriv::Reload,
    GlobalPriv::File,
    GlobalPriv::Config,
    GlobalPriv::ReplicationClient,
    GlobalPriv::ReplicationSlave,
];

/// One mask built from a variant list, so every named mask below is one
/// expression rather than a hand-maintained constant that could drift from
/// the enum.
fn mask_of(privs: &[GlobalPriv]) -> u64 {
    privs.iter().fold(0u64, |mask, priv_| mask | priv_.bit())
}

/// Go `mysql.AllPrivMask`: every bit set, `GRANT OPTION` included. The
/// "does this account hold ANY privilege here" mask that `SHOW TABLES` and
/// the `information_schema` retrievers test with.
#[must_use]
pub fn any_priv_mask() -> u64 {
    all_privs_mask() | GlobalPriv::GrantOption.bit()
}

/// Go's `mysql.AllPrivMask &^ mysql.CreateTMPTablePriv`, the exact mask
/// `fetchShowTables` (`executor/show.go` around line 613) filters with: a
/// `CREATE TEMPORARY TABLES` grant alone does not make a table listed.
#[must_use]
pub fn show_tables_priv_mask() -> u64 {
    any_priv_mask() & !GlobalPriv::CreateTemporaryTables.bit()
}

/// Go `mysql.AllColumnPrivs`, the four privileges
/// `information_schema.COLUMNS` accepts (`infoschema_reader.go` around line
/// 1095).
#[must_use]
pub fn column_privs_mask() -> u64 {
    mask_of(&[
        GlobalPriv::Select,
        GlobalPriv::Insert,
        GlobalPriv::Update,
        GlobalPriv::References,
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
        GlobalPriv::Create,
        GlobalPriv::Select,
        GlobalPriv::Insert,
        GlobalPriv::Update,
        GlobalPriv::Delete,
        GlobalPriv::ShowDatabases,
        GlobalPriv::Drop,
        GlobalPriv::Alter,
        GlobalPriv::Index,
        GlobalPriv::CreateView,
        GlobalPriv::ShowView,
        GlobalPriv::GrantOption,
        GlobalPriv::Trigger,
        GlobalPriv::References,
        GlobalPriv::Execute,
        GlobalPriv::CreateTemporaryTables,
    ])
}

/// The mask with every privilege in [`ALL_GLOBAL_PRIVS`] set, which is what
/// `ALL PRIVILEGES` grants and what makes `SHOW GRANTS` print the
/// `ALL PRIVILEGES` literal instead of an enumerated list (Go
/// `userPrivToString`).
pub(crate) fn all_privs_mask() -> u64 {
    ALL_GLOBAL_PRIVS
        .iter()
        .fold(0u64, |mask, priv_| mask | priv_.bit())
}

impl GlobalPriv {
    pub(crate) fn bit(self) -> u64 {
        1u64 << (self as u32)
    }

    /// This privilege as a one-bit mask, the unit every `grant`/`revoke`
    /// method on [`PrivilegeRegistry`] takes. Exposed so a caller that
    /// resolved a privilege NAME (a `GRANT` statement, or a `mysql.*` row a
    /// Go TiDB wrote) can build the same mask the parser path builds, rather
    /// than inventing a second privilege encoding.
    #[must_use]
    pub fn mask(self) -> u64 {
        self.bit()
    }

    /// Go `mysql.Priv2Str`, uppercased -- the exact text `SHOW GRANTS` prints
    /// for this privilege, and the verb an access-denied error names.
    #[must_use]
    pub fn print_name(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Create => "CREATE",
            Self::Drop => "DROP",
            Self::Process => "PROCESS",
            Self::References => "REFERENCES",
            Self::Alter => "ALTER",
            Self::ShowDatabases => "SHOW DATABASES",
            Self::Super => "SUPER",
            Self::Execute => "EXECUTE",
            Self::Index => "INDEX",
            Self::CreateUser => "CREATE USER",
            Self::CreateTablespace => "CREATE TABLESPACE",
            Self::Trigger => "TRIGGER",
            Self::CreateView => "CREATE VIEW",
            Self::ShowView => "SHOW VIEW",
            Self::CreateRole => "CREATE ROLE",
            Self::DropRole => "DROP ROLE",
            Self::CreateTemporaryTables => "CREATE TEMPORARY TABLES",
            Self::LockTables => "LOCK TABLES",
            Self::CreateRoutine => "CREATE ROUTINE",
            Self::AlterRoutine => "ALTER ROUTINE",
            Self::Event => "EVENT",
            Self::Shutdown => "SHUTDOWN",
            Self::Reload => "RELOAD",
            Self::File => "FILE",
            Self::Config => "CONFIG",
            Self::ReplicationClient => "REPLICATION CLIENT",
            Self::ReplicationSlave => "REPLICATION SLAVE",
            Self::GrantOption => "GRANT OPTION",
        }
    }

    /// Go `mysql.PrivilegeType.String()` -- the `Priv2Str` spelling, which is
    /// MIXED case and differs from [`Self::print_name`]'s `SHOW GRANTS` form
    /// for the multi-word privileges (`Show Databases`, `Create User`,
    /// `Grant Option`). Only one message uses it:
    /// `ErrPrivilegeCheckFail` (8121), which a denied `GRANT`/`REVOKE`
    /// reports.
    #[must_use]
    pub fn check_fail_name(self) -> &'static str {
        match self {
            Self::Select => "Select",
            Self::Insert => "Insert",
            Self::Update => "Update",
            Self::Delete => "Delete",
            Self::Create => "Create",
            Self::Drop => "Drop",
            Self::Process => "Process",
            Self::References => "References",
            Self::Alter => "Alter",
            Self::ShowDatabases => "Show Databases",
            Self::Super => "Super",
            Self::Execute => "Execute",
            Self::Index => "Index",
            Self::CreateUser => "Create User",
            Self::CreateTablespace => "Create Tablespace",
            Self::Trigger => "Trigger",
            Self::CreateView => "Create View",
            Self::ShowView => "Show View",
            Self::CreateRole => "Create Role",
            Self::DropRole => "Drop Role",
            Self::GrantOption => "Grant Option",
            // Go's `Priv2Str` spells the remaining entries in the same
            // uppercase form `SHOW GRANTS` prints.
            _ => self.print_name(),
        }
    }

    /// Resolves the exact spelling `tidb-parser`'s `GrantPrivilege::name`
    /// restores for a standard (non-dynamic) privilege token. Returns `None`
    /// for names this tier does not model as a static privilege (roles,
    /// anything dynamic) -- the caller decides how to refuse those.
    /// `GRANT OPTION` resolves here like any other privilege, which is what
    /// makes `GRANT`/`REVOKE GRANT OPTION ON <level>` work at every scope.
    pub fn from_grant_name(name: &str) -> Option<Self> {
        Some(match name {
            "SELECT" => Self::Select,
            "INSERT" => Self::Insert,
            "UPDATE" => Self::Update,
            "DELETE" => Self::Delete,
            "CREATE" => Self::Create,
            "DROP" => Self::Drop,
            "PROCESS" => Self::Process,
            "REFERENCES" => Self::References,
            "ALTER" => Self::Alter,
            "SHOW DATABASES" => Self::ShowDatabases,
            "SUPER" => Self::Super,
            "EXECUTE" => Self::Execute,
            "INDEX" => Self::Index,
            "CREATE USER" => Self::CreateUser,
            "CREATE TABLESPACE" => Self::CreateTablespace,
            "TRIGGER" => Self::Trigger,
            "CREATE VIEW" => Self::CreateView,
            "SHOW VIEW" => Self::ShowView,
            "CREATE ROLE" => Self::CreateRole,
            "DROP ROLE" => Self::DropRole,
            "CREATE TEMPORARY TABLES" => Self::CreateTemporaryTables,
            "LOCK TABLES" => Self::LockTables,
            "CREATE ROUTINE" => Self::CreateRoutine,
            "ALTER ROUTINE" => Self::AlterRoutine,
            "EVENT" => Self::Event,
            "SHUTDOWN" => Self::Shutdown,
            "RELOAD" => Self::Reload,
            "FILE" => Self::File,
            "CONFIG" => Self::Config,
            "REPLICATION CLIENT" => Self::ReplicationClient,
            "REPLICATION SLAVE" => Self::ReplicationSlave,
            "GRANT OPTION" => Self::GrantOption,
            _ => return None,
        })
    }
}

/// Go `mysql.AllDBPrivs` -- the privileges valid at DATABASE (`ON db.*`)
/// scope, in the exact order `dbPrivToString`/`PrivToString` print them
/// (its own fixed order, distinct from [`ALL_GLOBAL_PRIVS`]'s).
pub const ALL_DB_PRIVS: &[GlobalPriv] = &[
    GlobalPriv::Select,
    GlobalPriv::Insert,
    GlobalPriv::Update,
    GlobalPriv::Delete,
    GlobalPriv::Create,
    GlobalPriv::Drop,
    GlobalPriv::References,
    GlobalPriv::LockTables,
    GlobalPriv::CreateTemporaryTables,
    GlobalPriv::Event,
    GlobalPriv::CreateRoutine,
    GlobalPriv::AlterRoutine,
    GlobalPriv::Alter,
    GlobalPriv::Execute,
    GlobalPriv::Index,
    GlobalPriv::CreateView,
    GlobalPriv::ShowView,
    GlobalPriv::Trigger,
];

/// Go `mysql.AllTablePrivs` -- the privileges valid at TABLE (`ON db.t`)
/// scope, in `tablePrivToString`'s print order.
pub const ALL_TABLE_PRIVS: &[GlobalPriv] = &[
    GlobalPriv::Select,
    GlobalPriv::Insert,
    GlobalPriv::Update,
    GlobalPriv::Delete,
    GlobalPriv::Create,
    GlobalPriv::Drop,
    GlobalPriv::Index,
    GlobalPriv::References,
    GlobalPriv::Alter,
    GlobalPriv::CreateView,
    GlobalPriv::ShowView,
    GlobalPriv::Trigger,
];

/// The mask with every privilege in [`ALL_DB_PRIVS`] set -- what
/// `ALL PRIVILEGES ON db.*` grants and what collapses `SHOW GRANTS`'s
/// DB-scope line to the `ALL PRIVILEGES` literal (Go `dbPrivToString`).
pub(crate) fn all_db_privs_mask() -> u64 {
    ALL_DB_PRIVS
        .iter()
        .fold(0u64, |mask, priv_| mask | priv_.bit())
}

impl GlobalPriv {
    /// Whether this privilege is one Go's `grantDBLevel` accepts (a
    /// `mysql.StaticGlobalOnlyPrivs` member -- `PROCESS`, `SUPER`, etc. --
    /// is refused: captured `ErrWrongUsage`/1221, "Incorrect usage of DB
    /// GRANT and GLOBAL PRIVILEGES").
    #[must_use]
    pub fn is_valid_at_db_scope(self) -> bool {
        self == Self::GrantOption || ALL_DB_PRIVS.contains(&self)
    }

    /// Whether this privilege is one Go's TABLE-scope grant path accepts
    /// (anything outside `mysql.AllTablePrivs` is refused: captured
    /// `ErrIllegalGrantForTable`/1144).
    #[must_use]
    pub fn is_valid_at_table_scope(self) -> bool {
        self == Self::GrantOption || ALL_TABLE_PRIVS.contains(&self)
    }

    /// Whether this privilege may carry a COLUMN list (`GRANT SELECT (a) ON
    /// db.t`). Go checks `mysql.AllColumnPrivs` -- only the four privileges
    /// MySQL records per column -- and refuses anything else, `GRANT OPTION`
    /// included, with `ErrWrongUsage`/1221 "Incorrect usage of COLUMN GRANT
    /// and NON-COLUMN PRIVILEGES" (captured for `DELETE`, `DROP`, `ALTER`,
    /// `INDEX`, `CREATE` and `GRANT OPTION`).
    #[must_use]
    pub fn is_valid_at_column_scope(self) -> bool {
        ALL_COLUMN_PRIVS.contains(&self)
    }
}

/// Go `mysql.AllColumnPrivs` -- the privileges a column list may name, in
/// `privOnColumnsToString`'s print order, which is also the order the
/// per-privilege groups appear in one `SHOW GRANTS` column line (captured:
/// ``GRANT SELECT(a), INSERT(a, b), UPDATE(a), REFERENCES(a) ON `cg`.`t` ``).
pub const ALL_COLUMN_PRIVS: &[GlobalPriv] = &[
    GlobalPriv::Select,
    GlobalPriv::Insert,
    GlobalPriv::Update,
    GlobalPriv::References,
];

/// The mask with every privilege in [`ALL_COLUMN_PRIVS`] set -- what
/// `GRANT ALL (col) ON db.t` expands to (captured: `ALL (a)` is accepted and
/// prints as all four column privileges on `a`).
pub(crate) fn all_column_privs_mask() -> u64 {
    ALL_COLUMN_PRIVS
        .iter()
        .fold(0u64, |mask, priv_| mask | priv_.bit())
}

/// The mask with every privilege in [`ALL_TABLE_PRIVS`] set -- the
/// TABLE-scope analogue of [`all_db_privs_mask`] (Go `tablePrivToString`).
pub(crate) fn all_table_privs_mask() -> u64 {
    ALL_TABLE_PRIVS
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
    DYNAMIC_PRIVS
        .iter()
        .any(|registered| registered.eq_ignore_ascii_case(name))
}
