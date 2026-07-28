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

//! GLOBAL-scope account/privilege bookkeeping: `CREATE USER`/`DROP USER`,
//! `GRANT`/`REVOKE` of static privileges on `*.*`, and `SHOW GRANTS`.
//!
//! Seam of Go's `pkg/privilege/privileges` (`MySQLPrivilege`/
//! `UserPrivileges`), scoped down to the account/global-privilege slice this
//! tier models. Sharing pattern mirrors [`crate::process::ProcessRegistry`]:
//! one `Arc<Mutex<..>>` per server instance, cloned into every session that
//! front end opens, so a `GRANT` on one connection is visible to every peer
//! the moment it commits -- matching Go's single `privilege.Manager` per
//! `Domain`.
//!
//! Also models DATABASE (`ON db.*`) and TABLE (`ON db.t`) scope grants --
//! Go's `mysql.AllDBPrivs`/`mysql.AllTablePrivs` slices, `mysql.DB`/
//! `mysql.Tables_priv` records, and the DB/table slices of
//! `MySQLPrivilege.showGrants` -- on top of the GLOBAL registry above.
//!
//! Also models the account's `mysql.user.authentication_string` column --
//! the `mysql_native_password` stage-two hash `CREATE USER ... IDENTIFIED
//! BY` stores and the wire front end verifies a login against -- so this
//! registry is the single `mysql.user` Go has, not a privilege-only half of
//! one.
//!
//! `GRANT OPTION` is Go's `mysql.GrantPriv`: an ordinary privilege bit
//! living in the same `Priv` column as the rest, at every scope
//! (`mysql.user.Grant_priv`, `mysql.db.Grant_priv`, `mysql.tables_priv`'s
//! `Grant` member). It is deliberately absent from [`ALL_GLOBAL_PRIVS`] /
//! [`ALL_DB_PRIVS`] / [`ALL_TABLE_PRIVS`], which is what makes `GRANT ALL`
//! not confer it and makes `SHOW GRANTS` print it as the trailing
//! ` WITH GRANT OPTION` suffix instead of inside the privilege list.
//!
//! DYNAMIC privileges (Go's `dynamicPrivs` registry and the
//! `mysql.global_grants` table) live here too: they are NOT bits in the
//! `mysql.user` `Priv` mask but named rows in their own table, each carrying
//! its own `WITH GRANT OPTION` flag, which is why `SHOW GRANTS` prints them
//! on separate trailing lines.
//!
//! ROLES live here as well, because a role IS an account: Go's `CREATE ROLE`
//! writes an ordinary `mysql.user` row whose `account_locked` is `Y`
//! (captured), which is why `CREATE USER r` and `CREATE ROLE r` collide on the
//! same name and why `DROP USER` removes a role exactly as `DROP ROLE` does.
//! The two things a role adds on top of a locked account are the
//! `mysql.role_edges` graph (which accounts hold which roles) and the
//! `mysql.default_roles` table (which of them activate at login). Neither is a
//! privilege by itself: an account reaches a role's privileges only while that
//! role is ACTIVE in its session, and then TRANSITIVELY through roles granted
//! to that role (Go `FindAllUserEffectiveRoles`, a breadth-first walk of the
//! graph -- captured: activating `ra`, with `rb` granted to `ra`, confers
//! `rb`'s `SELECT` even though `SET ROLE ALL` never names `rb`).
//!
//! OUT OF SCOPE (refused rather than faked): column-level grants.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use sha1::{Digest, Sha1};
use tidb_executor::DriverError;
use tidb_mysql::consts::{
    AuthCachingSha2Password, AuthLDAPSASL, AuthLDAPSimple, AuthNativePassword, AuthSocket,
    AuthTiDBAuthToken, AuthTiDBSM3Password, PWDHashLen, SHAPWDHashLen, SM3PWDHashLen,
};

/// One account identity, `(user, host)` -- the key of every table here, and
/// the shape a role is named by too, since a role IS an account.
pub type Account = (String, String);

/// `mysql.DB` row key: `(user, host, database)`.
type DbPrivKey = (String, String, String);
/// `mysql.Tables_priv` row key: `(user, host, database, table)`.
type TablePrivKey = (String, String, String, String);

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

/// Go `mysql.AllGlobalPrivs`, minus `CreateRolePriv`/`DropRolePriv` (roles
/// are out of scope here) -- the print/iteration order `SHOW GRANTS` uses.
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

    /// Go `mysql.Priv2Str`, uppercased -- the exact text `SHOW GRANTS` prints
    /// for this privilege.
    pub(crate) fn print_name(self) -> &'static str {
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

/// `mysql.global_grants` rows for one account: privilege name (uppercase) ->
/// `with_grant_option`. Ordered so `SHOW GRANTS`'s alphabetical dynamic list
/// falls out of iteration (Go sorts explicitly; a `BTreeMap` makes the sort
/// structural).
type DynamicPrivs = BTreeMap<String, bool>;

/// One account's `mysql.user` row: its global privileges and the
/// `authentication_string` a login is verified against.
struct UserRecord {
    privs: u64,
    /// Go's `mysql.user.authentication_string`: `*` followed by 40 uppercase
    /// hexadecimal digits for a native-password account, and the EMPTY
    /// string for a passwordless one (captured: `CREATE USER 'nopw'@'%'`
    /// leaves `authentication_string` empty with plugin
    /// `mysql_native_password`).
    auth_string: String,
    /// Go's `mysql.user.account_locked`, which `CREATE ROLE` sets to `Y`
    /// (captured) and `CREATE USER` leaves `N`. A locked account cannot log
    /// in, which is the whole difference between a role and a user at the
    /// row level.
    is_role: bool,
    /// Go's `mysql.user.plugin`: the account's configured authentication
    /// plugin, defaulting to `mysql_native_password` when `CREATE USER`
    /// wrote no `IDENTIFIED WITH` clause. An account may be created and
    /// shown with any plugin `IDENTIFIED WITH` accepts (see
    /// `plugin::CREATE_USER_PLUGINS`), but the wire front end's login path
    /// only VERIFIES the `mysql_native_password` shape of `auth_string` --
    /// a non-native account still exists and prints correctly, it just
    /// cannot complete that plugin's real handshake yet (see
    /// `ConfiguredUserStore::authenticate_native`, which reports the
    /// server's honest, clean access-denied for it rather than a panic).
    plugin: String,
}

/// The server's account/global-privilege registry, shared by every session
/// of one TiDB instance -- Go's single `privilege.Manager` per `Domain`.
#[derive(Clone)]
pub struct PrivilegeRegistry {
    users: Arc<Mutex<HashMap<(String, String), UserRecord>>>,
    /// Go `mysql.DB` rows: one bitmask per `(user, host, database)`, keyed
    /// by the database's exact written name (matching Go's case-sensitive
    /// storage of the DB column).
    db_privs: Arc<Mutex<HashMap<DbPrivKey, u64>>>,
    /// Go `mysql.Tables_priv` rows: one bitmask per
    /// `(user, host, database, table)`.
    table_privs: Arc<Mutex<HashMap<TablePrivKey, u64>>>,
    /// Go `mysql.global_grants` rows: the DYNAMIC privileges of each
    /// account. An account with none has no entry here at all, which is what
    /// keeps `SHOW GRANTS` from printing an empty dynamic line.
    dynamic_privs: Arc<Mutex<HashMap<(String, String), DynamicPrivs>>>,
    /// Go `mysql.role_edges` / `MySQLPrivilege.roleGraph`, keyed by the
    /// GRANTEE (`TO_USER`/`TO_HOST`) so "which roles does this account hold"
    /// -- the question every reader asks -- is one lookup. The `BTreeSet`
    /// makes `SHOW GRANTS`'s sorted role list structural.
    ///
    /// Only edges are stored, never a closure: Go re-walks the graph on every
    /// check, so a role granted to a role takes effect immediately without
    /// any cache to invalidate.
    role_edges: Arc<Mutex<HashMap<Account, BTreeSet<Account>>>>,
    /// Go `mysql.default_roles`: the roles a session activates at login.
    /// An account with no row here starts with no active role at all
    /// (captured: a fresh session reports `CURRENT_ROLE()` = `NONE`).
    default_roles: Arc<Mutex<HashMap<Account, BTreeSet<Account>>>>,
}

impl std::fmt::Debug for PrivilegeRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PrivilegeRegistry")
            .finish_non_exhaustive()
    }
}

/// Go bootstraps `root`@`%` with every privilege plus `WITH GRANT OPTION`
/// (`mysql.CreateUserTable`'s bootstrap row). Captured:
/// `SHOW GRANTS` for a fresh cluster's root reports
/// `GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`.
const BOOTSTRAP_ROOT_USER: &str = "root";
const BOOTSTRAP_ROOT_HOST: &str = "%";

impl Default for PrivilegeRegistry {
    /// The fresh-cluster table: `root`@`%` alone, with no password.
    fn default() -> Self {
        Self::bootstrapped_from([(
            BOOTSTRAP_ROOT_USER.to_owned(),
            BOOTSTRAP_ROOT_HOST.to_owned(),
            String::new(),
        )])
    }
}

impl PrivilegeRegistry {
    /// Bootstraps a table holding EXACTLY the given
    /// `(user, host, authentication_string)` accounts.
    ///
    /// This is the deployable node's bootstrap: its auth file plays the role
    /// Go's `mysql.CreateUserTable` bootstrap does, so an account the file
    /// does not list has no row and therefore cannot log in -- `root`@`%`
    /// included. `root`@`%` receives the bootstrap
    /// `ALL PRIVILEGES ... WITH GRANT OPTION` only when it IS one of the
    /// accounts, which keeps "which accounts exist" and "what root may do"
    /// from being two independent decisions.
    #[must_use]
    pub fn bootstrapped_from(accounts: impl IntoIterator<Item = (String, String, String)>) -> Self {
        let users = accounts
            .into_iter()
            .map(|(user, host, auth_string)| {
                let privs = if user == BOOTSTRAP_ROOT_USER && host == BOOTSTRAP_ROOT_HOST {
                    all_privs_mask() | GlobalPriv::GrantOption.bit()
                } else {
                    0
                };
                (
                    (user, host),
                    UserRecord {
                        privs,
                        auth_string,
                        is_role: false,
                        plugin: tidb_mysql::consts::AuthNativePassword.to_owned(),
                    },
                )
            })
            .collect();
        Self {
            users: Arc::new(Mutex::new(users)),
            db_privs: Arc::new(Mutex::new(HashMap::new())),
            table_privs: Arc::new(Mutex::new(HashMap::new())),
            dynamic_privs: Arc::new(Mutex::new(HashMap::new())),
            role_edges: Arc::new(Mutex::new(HashMap::new())),
            default_roles: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl PrivilegeRegistry {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), UserRecord>> {
        self.users
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_db(&self) -> std::sync::MutexGuard<'_, HashMap<DbPrivKey, u64>> {
        self.db_privs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_table(&self) -> std::sync::MutexGuard<'_, HashMap<TablePrivKey, u64>> {
        self.table_privs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_dynamic(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), DynamicPrivs>> {
        self.dynamic_privs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_role_edges(&self) -> std::sync::MutexGuard<'_, HashMap<Account, BTreeSet<Account>>> {
        self.role_edges
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_default_roles(&self) -> std::sync::MutexGuard<'_, HashMap<Account, BTreeSet<Account>>> {
        self.default_roles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Whether an account with this exact user/host pair has been created
    /// (`CREATE USER`) and not since dropped.
    #[must_use]
    pub fn user_exists(&self, user: &str, host: &str) -> bool {
        self.lock()
            .contains_key(&(user.to_owned(), host.to_owned()))
    }

    /// Go's `userExists` + `ErrCannotUser("CREATE USER", ...)`: creating an
    /// account that already exists fails unless the caller handles
    /// `IF NOT EXISTS` itself. Returns `false` if the account already
    /// existed (no state changed), `true` if it was created.
    ///
    /// Defaults the account's plugin to `mysql_native_password`, Go's
    /// default when `CREATE USER` writes no `IDENTIFIED WITH` clause. Use
    /// [`Self::create_user_with_plugin`] for an explicit one.
    pub fn create_user(&self, user: &str, host: &str, auth_string: &str) -> bool {
        self.create_account(
            user,
            host,
            auth_string,
            tidb_mysql::consts::AuthNativePassword,
            false,
        )
    }

    /// [`Self::create_user`], recording an explicit authentication plugin --
    /// the `CREATE USER ... IDENTIFIED WITH <plugin>` path.
    pub fn create_user_with_plugin(
        &self,
        user: &str,
        host: &str,
        auth_string: &str,
        plugin: &str,
    ) -> bool {
        self.create_account(user, host, auth_string, plugin, false)
    }

    /// `CREATE ROLE`, which is `CREATE USER` writing the same `mysql.user`
    /// row with `account_locked = 'Y'` and no password. Roles and users share
    /// one namespace, so this collides with an existing account of either
    /// kind (captured: `CREATE USER r1` after `CREATE ROLE r1` reports
    /// `Operation CREATE USER failed for 'r1'@'%'`, and vice versa).
    pub fn create_role(&self, role: &str, host: &str) -> bool {
        self.create_account(role, host, "", tidb_mysql::consts::AuthNativePassword, true)
    }

    fn create_account(
        &self,
        user: &str,
        host: &str,
        auth_string: &str,
        plugin: &str,
        is_role: bool,
    ) -> bool {
        let key = (user.to_owned(), host.to_owned());
        let mut guard = self.lock();
        if guard.contains_key(&key) {
            return false;
        }
        guard.insert(
            key,
            UserRecord {
                privs: 0,
                auth_string: auth_string.to_owned(),
                is_role,
                plugin: plugin.to_owned(),
            },
        );
        true
    }

    /// Whether this account is a ROLE (`account_locked = 'Y'`). A role cannot
    /// log in, which is what keeps a passwordless role from being an open
    /// door.
    #[must_use]
    pub fn is_role(&self, user: &str, host: &str) -> bool {
        self.lock()
            .get(&(user.to_owned(), host.to_owned()))
            .is_some_and(|record| record.is_role)
    }

    /// The account's `authentication_string`, or `None` when no such account
    /// exists. An existing passwordless account answers `Some("")`, which is
    /// the distinction a login path needs: "no such user" and "user with no
    /// password" are different answers.
    #[must_use]
    pub fn auth_string(&self, user: &str, host: &str) -> Option<String> {
        self.lock()
            .get(&(user.to_owned(), host.to_owned()))
            .map(|record| record.auth_string.clone())
    }

    /// The account's configured authentication plugin, or `None` when no
    /// such account exists.
    #[must_use]
    pub fn plugin(&self, user: &str, host: &str) -> Option<String> {
        self.lock()
            .get(&(user.to_owned(), host.to_owned()))
            .map(|record| record.plugin.clone())
    }

    /// Replaces an existing account's `authentication_string`
    /// (`ALTER USER ... IDENTIFIED BY`, `SET PASSWORD`). Returns whether the
    /// account existed.
    pub fn set_auth_string(&self, user: &str, host: &str, auth_string: &str) -> bool {
        match self.lock().get_mut(&(user.to_owned(), host.to_owned())) {
            Some(record) => {
                record.auth_string = auth_string.to_owned();
                true
            }
            None => false,
        }
    }

    /// [`Self::set_auth_string`], also rewriting the account's `plugin` --
    /// `ALTER USER ... IDENTIFIED WITH '<plugin>' [BY '<password>' | AS
    /// '<hash>']`. Go always writes both columns together on any auth-clause
    /// ALTER USER, backfilling `AuthPlugin` from the row's current plugin
    /// when the statement wrote no explicit `WITH` (see
    /// `resolve_auth_string_and_plugin`), so this is the one write path both
    /// `IDENTIFIED BY` and `IDENTIFIED WITH` go through.
    pub fn set_auth_string_and_plugin(
        &self,
        user: &str,
        host: &str,
        auth_string: &str,
        plugin: &str,
    ) -> bool {
        match self.lock().get_mut(&(user.to_owned(), host.to_owned())) {
            Some(record) => {
                record.auth_string = auth_string.to_owned();
                record.plugin = plugin.to_owned();
                true
            }
            None => false,
        }
    }

    /// Every account identity currently in the table, for the front end's
    /// host-pattern matching at login time (Go resolves a login against the
    /// live `mysql.user` rows, not a startup snapshot).
    #[must_use]
    pub fn accounts(&self) -> Vec<(String, String)> {
        self.lock().keys().cloned().collect()
    }

    /// Go `RENAME USER`: moves the account row and every DB/TABLE grant row
    /// keyed by it to a new identity, keeping the `authentication_string`
    /// (captured). Returns `false` without changing anything when the old
    /// account is missing or the new identity already exists.
    pub fn rename_user(
        &self,
        old_user: &str,
        old_host: &str,
        new_user: &str,
        new_host: &str,
    ) -> bool {
        let old_key = (old_user.to_owned(), old_host.to_owned());
        let new_key = (new_user.to_owned(), new_host.to_owned());
        {
            let mut guard = self.lock();
            if guard.contains_key(&new_key) {
                return false;
            }
            let Some(record) = guard.remove(&old_key) else {
                return false;
            };
            guard.insert(new_key, record);
        }
        let mut db_guard = self.lock_db();
        *db_guard = db_guard
            .drain()
            .map(|((row_user, row_host, database), privs)| {
                if row_user == old_user && row_host == old_host {
                    ((new_user.to_owned(), new_host.to_owned(), database), privs)
                } else {
                    ((row_user, row_host, database), privs)
                }
            })
            .collect();
        drop(db_guard);
        let mut table_guard = self.lock_table();
        *table_guard = table_guard
            .drain()
            .map(|((row_user, row_host, database, table), privs)| {
                if row_user == old_user && row_host == old_host {
                    (
                        (new_user.to_owned(), new_host.to_owned(), database, table),
                        privs,
                    )
                } else {
                    ((row_user, row_host, database, table), privs)
                }
            })
            .collect();
        drop(table_guard);
        let mut dynamic_guard = self.lock_dynamic();
        if let Some(privs) = dynamic_guard.remove(&(old_user.to_owned(), old_host.to_owned())) {
            dynamic_guard.insert((new_user.to_owned(), new_host.to_owned()), privs);
        }
        drop(dynamic_guard);
        // Go's `executeRenameUser` also rewrites `mysql.role_edges` (both the
        // FROM_USER and TO_USER sides) and `mysql.default_roles` (both the
        // USER and DEFAULT_ROLE_USER sides): a renamed grantee keeps every
        // role it was granted, a renamed role keeps every grantee it was
        // granted to, and default-role membership follows the rename in both
        // directions (captured).
        let old_account = (old_user.to_owned(), old_host.to_owned());
        let new_account = (new_user.to_owned(), new_host.to_owned());
        let mut edges = self.lock_role_edges();
        if let Some(roles) = edges.remove(&old_account) {
            edges.insert(new_account.clone(), roles);
        }
        for roles in edges.values_mut() {
            if roles.remove(&old_account) {
                roles.insert(new_account.clone());
            }
        }
        drop(edges);
        let mut defaults = self.lock_default_roles();
        if let Some(roles) = defaults.remove(&old_account) {
            defaults.insert(new_account.clone(), roles);
        }
        for roles in defaults.values_mut() {
            if roles.remove(&old_account) {
                roles.insert(new_account.clone());
            }
        }
        true
    }

    /// `ALTER USER ... ACCOUNT LOCK` / `ACCOUNT UNLOCK`: flips
    /// `mysql.user.account_locked`. Reuses the same flag [`Self::is_role`]
    /// reads (Go stores both under one `account_locked` column; a role IS
    /// simply an account row with `account_locked = 'Y'` and no password), so
    /// a locked plain user refuses login exactly like a role does. Returns
    /// whether the account existed.
    pub fn set_locked(&self, user: &str, host: &str, locked: bool) -> bool {
        match self.lock().get_mut(&(user.to_owned(), host.to_owned())) {
            Some(record) => {
                record.is_role = locked;
                true
            }
            None => false,
        }
    }

    /// Go's `ErrCannotUser("DROP USER", ...)`: dropping a missing account
    /// fails unless the caller handles `IF EXISTS` itself. Returns whether
    /// the account existed (and was removed).
    pub fn drop_user(&self, user: &str, host: &str) -> bool {
        let removed = self
            .lock()
            .remove(&(user.to_owned(), host.to_owned()))
            .is_some();
        // Go deletes the account's `mysql.db`/`mysql.tables_priv` rows in the
        // same transaction (captured: after `DROP USER`, `mysql.db` has no
        // row left for the account), so a later account recreated under the
        // same name does not inherit the old scoped grants.
        self.lock_db()
            .retain(|(row_user, row_host, _), _| row_user != user || row_host != host);
        self.lock_table()
            .retain(|(row_user, row_host, _, _), _| row_user != user || row_host != host);
        self.lock_dynamic()
            .remove(&(user.to_owned(), host.to_owned()));
        // Go's `DROP USER`/`DROP ROLE` deletes the account's `role_edges`
        // rows in BOTH directions and every `default_roles` row naming it
        // (captured: after `DROP ROLE r1`, no edge mentions r1 and u1's
        // default-role row for r1 is gone), so no dangling grant survives to
        // be inherited by a later account of the same name.
        let account = (user.to_owned(), host.to_owned());
        let mut edges = self.lock_role_edges();
        edges.remove(&account);
        for roles in edges.values_mut() {
            roles.remove(&account);
        }
        edges.retain(|_, roles| !roles.is_empty());
        drop(edges);
        let mut defaults = self.lock_default_roles();
        defaults.remove(&account);
        for roles in defaults.values_mut() {
            roles.remove(&account);
        }
        defaults.retain(|_, roles| !roles.is_empty());
        removed
    }

    /// `GRANT <role> TO <account>`: one `mysql.role_edges` row. Go's
    /// `INSERT IGNORE` makes a repeat grant a silent no-op, and nothing
    /// rejects a self-grant or a cycle (captured: `GRANT r1 TO r1` reports
    /// OK), so neither is special-cased here -- the breadth-first walk in
    /// [`Self::effective_roles`] terminates on visited nodes regardless.
    pub fn grant_role(&self, role: &Account, grantee: &Account) {
        self.lock_role_edges()
            .entry(grantee.clone())
            .or_default()
            .insert(role.clone());
    }

    /// `REVOKE <role> FROM <account>`: deletes the edge, and with it any
    /// `default_roles` row that named the now-ungranted role (captured:
    /// after `REVOKE r1 FROM 'u1'@'%'`, u1's default roles keep only r3).
    /// Revoking an edge that was never there is a silent no-op, as in Go.
    pub fn revoke_role(&self, role: &Account, grantee: &Account) {
        let mut edges = self.lock_role_edges();
        if let Some(roles) = edges.get_mut(grantee) {
            roles.remove(role);
            if roles.is_empty() {
                edges.remove(grantee);
            }
        }
        drop(edges);
        let mut defaults = self.lock_default_roles();
        if let Some(roles) = defaults.get_mut(grantee) {
            roles.remove(role);
            if roles.is_empty() {
                defaults.remove(grantee);
            }
        }
    }

    /// The roles granted DIRECTLY to this account, sorted. This is the set
    /// `SET ROLE ALL` activates and the one `SHOW GRANTS`'s role line prints
    /// -- Go never expands the graph for either (captured: `SET ROLE ALL` for
    /// an account holding `ra`, which itself holds `rb`, activates `ra`
    /// alone).
    #[must_use]
    pub fn granted_roles(&self, account: &Account) -> Vec<Account> {
        self.lock_role_edges()
            .get(account)
            .map(|roles| roles.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Go `MySQLPrivilege.FindRole`: whether `role` is granted directly to
    /// `account`. `SET ROLE` and `SET DEFAULT ROLE` both gate on this, which
    /// is why naming an indirectly-held role reports `ErrRoleNotGranted`
    /// (3530) rather than activating it.
    #[must_use]
    pub fn has_role(&self, account: &Account, role: &Account) -> bool {
        self.lock_role_edges()
            .get(account)
            .is_some_and(|roles| roles.contains(role))
    }

    /// Go `MySQLPrivilege.FindAllUserEffectiveRoles`: the identities whose
    /// privileges `account` actually reaches through `active` -- each active
    /// role that is really granted to `account`, plus everything reachable
    /// from those by following further role grants (breadth-first, visiting
    /// each identity once so a cycle terminates).
    ///
    /// This is deliberately NOT [`Self::granted_roles`]: activation is
    /// direct-only, but INHERITANCE through an activated role is transitive.
    #[must_use]
    pub fn effective_roles(&self, account: &Account, active: &[Account]) -> Vec<Account> {
        let edges = self.lock_role_edges();
        let granted = edges.get(account);
        let mut queue: Vec<Account> = active
            .iter()
            .filter(|role| granted.is_some_and(|roles| roles.contains(*role)))
            .cloned()
            .collect();
        let mut visited: BTreeSet<Account> = BTreeSet::new();
        let mut effective = Vec::new();
        let mut head = 0;
        while head < queue.len() {
            let role = queue[head].clone();
            head += 1;
            if !visited.insert(role.clone()) {
                continue;
            }
            if let Some(inherited) = edges.get(&role) {
                queue.extend(inherited.iter().cloned());
            }
            effective.push(role);
        }
        effective
    }

    /// The account's `mysql.default_roles` rows, sorted -- the roles a fresh
    /// session activates and the ones `SET ROLE DEFAULT` restores.
    #[must_use]
    pub fn default_roles(&self, account: &Account) -> Vec<Account> {
        self.lock_default_roles()
            .get(account)
            .map(|roles| roles.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// `SET DEFAULT ROLE`: replaces the account's whole `default_roles` set
    /// (Go deletes every row for the account and re-inserts, so this is a
    /// replace and never a merge). An empty set leaves no row at all, which
    /// is the `SET DEFAULT ROLE NONE` state.
    pub fn set_default_roles(&self, account: &Account, roles: &[Account]) {
        let mut defaults = self.lock_default_roles();
        if roles.is_empty() {
            defaults.remove(account);
            return;
        }
        defaults.insert(account.clone(), roles.iter().cloned().collect());
    }

    /// Sets every bit in `mask`, on an account the caller has already
    /// confirmed exists (see [`Self::user_exists`]).
    pub fn grant(&self, user: &str, host: &str, mask: u64) {
        if let Some(record) = self.lock().get_mut(&(user.to_owned(), host.to_owned())) {
            record.privs |= mask;
        }
    }

    /// Clears every bit in `mask`. Go's `REVOKE` on a privilege the account
    /// never had is a silent no-op, not an error.
    pub fn revoke(&self, user: &str, host: &str, mask: u64) {
        if let Some(record) = self.lock().get_mut(&(user.to_owned(), host.to_owned())) {
            record.privs &= !mask;
        }
    }

    /// Whether the account holds `global_priv` IN ITS OWN RIGHT, ignoring
    /// roles -- the check for a caller that has no session and therefore no
    /// active-role set.
    #[must_use]
    pub fn has_global_priv(&self, user: &str, host: &str, global_priv: GlobalPriv) -> bool {
        self.lock()
            .get(&(user.to_owned(), host.to_owned()))
            .is_some_and(|record| record.privs & global_priv.bit() != 0)
    }

    /// Go `MySQLPrivilege.RequestVerification`'s global-scope arm: the
    /// account's own privileges OR those of every role it reaches through
    /// `active_roles`. Go checks the user's own record first and each
    /// effective role's afterwards; ORing the masks is the same answer in
    /// the same order, since a privilege held anywhere satisfies the check.
    #[must_use]
    pub fn has_global_priv_with_roles(
        &self,
        user: &str,
        host: &str,
        active_roles: &[Account],
        global_priv: GlobalPriv,
    ) -> bool {
        self.identities_for_check(user, host, active_roles)
            .into_iter()
            .any(|(role_user, role_host)| self.has_global_priv(&role_user, &role_host, global_priv))
    }

    /// The account itself followed by every role it effectively holds -- the
    /// identity list Go's `RequestVerification` walks, in Go's order (self
    /// first, then roles).
    fn identities_for_check(
        &self,
        user: &str,
        host: &str,
        active_roles: &[Account],
    ) -> Vec<Account> {
        let account = (user.to_owned(), host.to_owned());
        let mut identities = vec![account.clone()];
        identities.extend(self.effective_roles(&account, active_roles));
        identities
    }

    /// Go's `GRANT <dynamic> ON *.* TO`, which is a
    /// `REPLACE INTO mysql.global_grants` -- so re-granting a privilege the
    /// account already holds OVERWRITES its `with_grant_option` rather than
    /// ORing into it, and a plain re-grant therefore DOWNGRADES a previously
    /// grantable privilege.
    pub fn grant_dynamic(&self, user: &str, host: &str, name: &str, with_grant: bool) {
        self.lock_dynamic()
            .entry((user.to_owned(), host.to_owned()))
            .or_default()
            .insert(name.to_ascii_uppercase(), with_grant);
    }

    /// Go's `revokeDynamicPriv`: a `DELETE FROM mysql.global_grants` for the
    /// one privilege. Revoking one the account never held is a silent no-op
    /// (the 3929 warning for an UNREGISTERED name is raised by the caller,
    /// which still reaches this delete).
    pub fn revoke_dynamic(&self, user: &str, host: &str, name: &str) {
        if let Some(privs) = self
            .lock_dynamic()
            .get_mut(&(user.to_owned(), host.to_owned()))
        {
            privs.remove(&name.to_ascii_uppercase());
        }
    }

    /// Go's `REVOKE ALL ON *.*`, which additionally does an unqualified
    /// `DELETE FROM mysql.global_grants WHERE user = ? AND host = ?` --
    /// `ALL PRIVILEGES` revokes every DYNAMIC privilege even though
    /// `GRANT ALL` never grants one.
    pub fn revoke_all_dynamic(&self, user: &str, host: &str) {
        self.lock_dynamic()
            .remove(&(user.to_owned(), host.to_owned()));
    }

    /// Go `MySQLPrivilege.HasExplicitlyGrantedDynamicPrivilege`: whether the
    /// account holds this DYNAMIC privilege as its own `global_grants` row,
    /// with no SUPER fallback. `with_grant` additionally requires the row's
    /// `with_grant_option`.
    #[must_use]
    pub fn has_explicit_dynamic_priv(
        &self,
        user: &str,
        host: &str,
        name: &str,
        with_grant: bool,
    ) -> bool {
        self.lock_dynamic()
            .get(&(user.to_owned(), host.to_owned()))
            .and_then(|privs| privs.get(&name.to_ascii_uppercase()))
            .is_some_and(|grantable| !with_grant || *grantable)
    }

    /// Go `MySQLPrivilege.RequestDynamicVerification` -- the check every
    /// consumer of a DYNAMIC privilege actually runs.
    ///
    /// The rule is: an explicit `global_grants` row satisfies it, and
    /// FAILING THAT, SUPER does. Go keeps that fallback deliberately ("the
    /// SUPER privilege also has all DYNAMIC privileges granted to it ...
    /// otherwise tasks such as BACKUP and ROLE_ADMIN will start to fail"),
    /// so a SUPER account passes every dynamic check without holding a
    /// single dynamic row.
    ///
    /// The ONLY no-fallback case in Go is SEM (Security Enhanced Mode): when
    /// SEM is on, `sem.IsRestrictedPrivilege` -- true for exactly the names
    /// prefixed `RESTRICTED_` -- blocks the SUPER fallback. This tier has no
    /// SEM, so no name is exempt here; wiring SEM on later means adding that
    /// one branch and nothing else.
    ///
    /// `with_grant` mirrors Go: the explicit row must itself be grantable,
    /// and the SUPER fallback additionally requires `GRANT OPTION`.
    #[must_use]
    pub fn has_dynamic_priv(&self, user: &str, host: &str, name: &str, with_grant: bool) -> bool {
        if self.has_explicit_dynamic_priv(user, host, name, with_grant) {
            return true;
        }
        if with_grant && !self.has_global_priv(user, host, GlobalPriv::GrantOption) {
            return false;
        }
        self.has_global_priv(user, host, GlobalPriv::Super)
    }

    /// [`Self::has_dynamic_priv`] widened to the account's active roles, the
    /// same way [`Self::has_global_priv_with_roles`] widens the static check
    /// (captured: `SHOW GRANTS` merges a role's DYNAMIC privileges into the
    /// account's own dynamic line once the role is active).
    #[must_use]
    pub fn has_dynamic_priv_with_roles(
        &self,
        user: &str,
        host: &str,
        active_roles: &[Account],
        name: &str,
        with_grant: bool,
    ) -> bool {
        self.identities_for_check(user, host, active_roles)
            .into_iter()
            .any(|(role_user, role_host)| {
                self.has_dynamic_priv(&role_user, &role_host, name, with_grant)
            })
    }

    /// Sets every bit in `mask` on the account's `(database)` row, creating
    /// the row if this is its first DB-scope grant (Go's `checkAndInitDBPriv`
    /// inserting a fresh `mysql.DB` row before `grantDBLevel` sets bits on
    /// it).
    pub fn grant_db(&self, user: &str, host: &str, database: &str, mask: u64) {
        *self
            .lock_db()
            .entry((user.to_owned(), host.to_owned(), database.to_owned()))
            .or_insert(0) |= mask;
    }

    /// Go's `dbUserExists`: whether this account has a `mysql.DB` row for
    /// `database` at all (any privilege, even zero after later revokes).
    #[must_use]
    pub fn db_grant_row_exists(&self, user: &str, host: &str, database: &str) -> bool {
        self.lock_db()
            .contains_key(&(user.to_owned(), host.to_owned(), database.to_owned()))
    }

    /// Clears every bit in `mask` on the account's `(database)` row. Go's
    /// `revokeOneUser` requires the row to already exist (checked by the
    /// caller via [`Self::db_grant_row_exists`]); clearing a bit the row
    /// never had is a silent no-op, matching global-scope `revoke`.
    pub fn revoke_db(&self, user: &str, host: &str, database: &str, mask: u64) {
        if let Some(privs) =
            self.lock_db()
                .get_mut(&(user.to_owned(), host.to_owned(), database.to_owned()))
        {
            *privs &= !mask;
        }
    }

    /// Sets every bit in `mask` on the account's `(database, table)` row,
    /// creating the row if this is its first TABLE-scope grant (Go's
    /// `checkAndInitTablePriv`).
    pub fn grant_table(&self, user: &str, host: &str, database: &str, table: &str, mask: u64) {
        *self
            .lock_table()
            .entry((
                user.to_owned(),
                host.to_owned(),
                database.to_owned(),
                table.to_owned(),
            ))
            .or_insert(0) |= mask;
    }

    /// Go's `tableUserExists`: whether this account has a
    /// `mysql.Tables_priv` row for `(database, table)` at all.
    #[must_use]
    pub fn table_grant_row_exists(
        &self,
        user: &str,
        host: &str,
        database: &str,
        table: &str,
    ) -> bool {
        self.lock_table().contains_key(&(
            user.to_owned(),
            host.to_owned(),
            database.to_owned(),
            table.to_owned(),
        ))
    }

    /// Clears every bit in `mask` on the account's `(database, table)` row.
    /// See [`Self::revoke_db`] for the never-granted-bit no-op rule, which
    /// applies identically here.
    pub fn revoke_table(&self, user: &str, host: &str, database: &str, table: &str, mask: u64) {
        if let Some(privs) = self.lock_table().get_mut(&(
            user.to_owned(),
            host.to_owned(),
            database.to_owned(),
            table.to_owned(),
        )) {
            *privs &= !mask;
        }
    }

    /// Go `MySQLPrivilege.showGrants`: `None` when the account has no grant
    /// row at all (Go's `ErrNonexistingGrant`), `Some` with the newline-joined
    /// lines otherwise.
    ///
    /// `active_roles` is the session's active-role set for a `SHOW GRANTS`
    /// about the session's own account (and the `USING` list otherwise); the
    /// privileges of every role reachable through it are MERGED into the
    /// account's own lines, printed under the ACCOUNT's name (captured: with
    /// a role holding `SELECT ON roledb.*` active, `SHOW GRANTS` prints
    /// ``GRANT SELECT ON `roledb`.* TO 'u1'@'%'``). Pass an empty slice for
    /// `SHOW GRANTS FOR <other account>`, which merges nothing.
    #[must_use]
    pub fn show_grants(&self, user: &str, host: &str, active_roles: &[Account]) -> Option<String> {
        if !self.user_exists(user, host) {
            return None;
        }
        // Every identity whose rows fold into this output. Go reads the
        // account's own row and each effective role's, ORing the masks.
        let identities = self.identities_for_check(user, host, active_roles);
        let owns = |row_user: &str, row_host: &str| {
            identities
                .iter()
                .any(|(id_user, id_host)| id_user == row_user && id_host == row_host)
        };

        let global_line = {
            let guard = self.lock();
            let privs = identities
                .iter()
                .filter_map(|key| guard.get(key))
                .fold(0u64, |mask, record| mask | record.privs);
            let priv_text = if privs & !GlobalPriv::GrantOption.bit() == all_privs_mask() {
                "ALL PRIVILEGES".to_owned()
            } else {
                priv_list(privs, ALL_GLOBAL_PRIVS)
            };
            format!(
                "GRANT {priv_text} ON *.* TO '{user}'@'{host}'{}",
                grant_option_suffix(privs)
            )
        };

        // DB-scope lines: Go's showGrants sorts these lexically by the
        // formatted `GRANT ... ON db.* ...` string (captured: a grant on
        // `aaadb` prints before one on `db1`, even though `db1` was granted
        // first) -- not insertion order, not plain DB-name order.
        //
        // Rows of the account and of every effective role are merged PER
        // DATABASE before formatting (Go's `dbPrivTable` map), so a database
        // granted to both prints one line carrying the union.
        let mut db_masks: BTreeMap<String, u64> = BTreeMap::new();
        for ((row_user, row_host, database), privs) in self.lock_db().iter() {
            if owns(row_user, row_host) {
                *db_masks.entry(database.clone()).or_insert(0) |= *privs;
            }
        }
        let mut db_lines: Vec<String> = db_masks
            .into_iter()
            .map(|(database, privs)| {
                let priv_text = if privs & !GlobalPriv::GrantOption.bit() == all_db_privs_mask() {
                    "ALL PRIVILEGES".to_owned()
                } else {
                    priv_list(privs, ALL_DB_PRIVS)
                };
                format!(
                    "GRANT {priv_text} ON `{database}`.* TO '{user}'@'{host}'{}",
                    grant_option_suffix(privs)
                )
            })
            .collect();
        db_lines.sort_unstable();

        // TABLE-scope lines: same lexical-sort rule as DB-scope.
        let mut table_masks: BTreeMap<(String, String), u64> = BTreeMap::new();
        for ((row_user, row_host, database, table), privs) in self.lock_table().iter() {
            if owns(row_user, row_host) {
                *table_masks
                    .entry((database.clone(), table.clone()))
                    .or_insert(0) |= *privs;
            }
        }
        let mut table_lines: Vec<String> = table_masks
            .into_iter()
            .map(|((database, table), privs)| {
                let priv_text = if privs & !GlobalPriv::GrantOption.bit() == all_table_privs_mask()
                {
                    "ALL PRIVILEGES".to_owned()
                } else {
                    priv_list(privs, ALL_TABLE_PRIVS)
                };
                format!(
                    "GRANT {priv_text} ON `{database}`.`{table}` TO '{user}'@'{host}'{}",
                    grant_option_suffix(privs)
                )
            })
            .collect();
        table_lines.sort_unstable();

        // DYNAMIC lines close the output, AFTER every static scope (captured:
        // global, then `db`.*, then `db`.`t`, then dynamic). Go emits at most
        // two of them -- one for the non-grantable privileges and one for the
        // grantable ones -- each an alphabetically sorted, comma-joined list
        // on `*.*`, with the grantable line carrying the usual
        // ` WITH GRANT OPTION` suffix. An account with no dynamic row emits
        // neither, and the `GRANT USAGE ON *.*` global line is printed
        // regardless (captured: a dynamic-only account shows both).
        //
        // The ROLE line sits between the static scopes and the dynamic ones
        // (captured, and Go's `showGrants` appends it right after the
        // column-scope block). It lists the roles granted DIRECTLY to the
        // account -- all of them, active or not -- sorted by their printed
        // `'role'@'host'` text and joined with `", "`.
        let mut role_names: Vec<String> = self
            .granted_roles(&(user.to_owned(), host.to_owned()))
            .into_iter()
            .map(|(role, role_host)| format!("'{role}'@'{role_host}'"))
            .collect();
        role_names.sort_unstable();
        let role_line = (!role_names.is_empty())
            .then(|| format!("GRANT {} TO '{user}'@'{host}'", role_names.join(", ")));

        // A role's DYNAMIC privileges merge into the account's own dynamic
        // lines. Go keeps an already-grantable entry rather than letting a
        // non-grantable inherited one clobber it, so the account's own row
        // is written first and a role only fills in what is missing or
        // upgrades a non-grantable entry.
        let mut merged: BTreeMap<String, bool> = BTreeMap::new();
        {
            let dynamic = self.lock_dynamic();
            for key in &identities {
                let Some(privs) = dynamic.get(key) else {
                    continue;
                };
                for (name, with_grant) in privs {
                    match merged.get(name) {
                        Some(true) => {}
                        _ => {
                            merged.insert(name.clone(), *with_grant);
                        }
                    }
                }
            }
        }
        let mut plain: Vec<String> = Vec::new();
        let mut grantable: Vec<String> = Vec::new();
        for (name, with_grant) in merged {
            if with_grant {
                grantable.push(name);
            } else {
                plain.push(name);
            }
        }
        let mut dynamic_lines = Vec::new();
        if !plain.is_empty() {
            dynamic_lines.push(format!(
                "GRANT {} ON *.* TO '{user}'@'{host}'",
                plain.join(",")
            ));
        }
        if !grantable.is_empty() {
            dynamic_lines.push(format!(
                "GRANT {} ON *.* TO '{user}'@'{host}' WITH GRANT OPTION",
                grantable.join(",")
            ));
        }

        let mut lines =
            Vec::with_capacity(2 + db_lines.len() + table_lines.len() + dynamic_lines.len());
        lines.push(global_line);
        lines.extend(db_lines);
        lines.extend(table_lines);
        lines.extend(role_line);
        lines.extend(dynamic_lines);
        Some(lines.join("\n"))
    }

    /// Go `MySQLPrivilege.UserPrivilegesTable`'s dynamic half: one
    /// `(grantee, privilege_name, is_grantable)` triple per
    /// `mysql.global_grants` row of `(user, host)`.
    #[must_use]
    pub fn dynamic_priv_rows(&self, user: &str, host: &str) -> Vec<(String, bool)> {
        self.lock_dynamic()
            .get(&(user.to_owned(), host.to_owned()))
            .into_iter()
            .flatten()
            .map(|(name, grantable)| (name.clone(), *grantable))
            .collect()
    }

    /// Every account's global privilege mask, for
    /// `information_schema.USER_PRIVILEGES`'s static half.
    #[must_use]
    pub fn global_priv_masks(&self) -> Vec<((String, String), u64)> {
        self.lock()
            .iter()
            .map(|(key, record)| (key.clone(), record.privs))
            .collect()
    }

    /// The `(user, host)` of every account holding at least one DYNAMIC
    /// privilege, for `information_schema.USER_PRIVILEGES`'s dynamic half.
    #[must_use]
    pub fn accounts_with_dynamic_privs(&self) -> Vec<(String, String)> {
        self.lock_dynamic().keys().cloned().collect()
    }
}

/// The ` WITH GRANT OPTION` suffix `SHOW GRANTS` appends to a line whose
/// privilege mask carries `mysql.GrantPriv`. Captured at all three scopes:
/// the suffix trails the whole `GRANT ... TO '<user>'@'<host>'` line, and
/// `GRANT OPTION` never appears inside the privilege list.
fn grant_option_suffix(privs: u64) -> &'static str {
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
fn priv_list(privs: u64, order: &[GlobalPriv]) -> String {
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

/// Go `pkg/parser/auth.EncodePassword`: the
/// `mysql.user.authentication_string` of a `mysql_native_password` account is
/// `*` followed by the UPPERCASE hexadecimal SHA-1 of the SHA-1 of the
/// plaintext. An empty password encodes to the empty string, NOT to a hash of
/// the empty string.
#[must_use]
pub fn encode_password(password: &str) -> String {
    if password.is_empty() {
        return String::new();
    }
    let stage_one = Sha1::digest(password.as_bytes());
    let stage_two = Sha1::digest(stage_one);
    let mut encoded = String::with_capacity(1 + stage_two.len() * 2);
    encoded.push('*');
    for byte in stage_two {
        use std::fmt::Write;
        write!(encoded, "{byte:02X}").expect("writing to a String cannot fail");
    }
    encoded
}

/// Plugins `CREATE`/`ALTER USER ... IDENTIFIED WITH` accepts without a
/// registered extension auth plugin -- Go `simple.go`'s account executor
/// switch (`executor/simple.go`'s `executeCreateUser`). Any other name is
/// Go's `ErrPluginIsNotLoaded` (1524), `Plugin '<name>' is not loaded`,
/// since this tier registers no extension auth plugins to fall back to.
pub const CREATE_USER_PLUGINS: &[&str] = &[
    AuthNativePassword,
    AuthCachingSha2Password,
    AuthTiDBSM3Password,
    AuthSocket,
    AuthTiDBAuthToken,
    AuthLDAPSimple,
    AuthLDAPSASL,
];

/// Whether `plugin` is one [`CREATE_USER_PLUGINS`] accepts.
#[must_use]
pub fn is_create_user_plugin(plugin: &str) -> bool {
    CREATE_USER_PLUGINS.contains(&plugin)
}

/// One account specification's `IDENTIFIED WITH <plugin> [BY '<password>' |
/// AS '<hash>']` credential, already split into the shape Go's
/// `encodedPassword` switches on.
pub enum PluginCredential<'a> {
    /// `BY '<password>'`: the plaintext the plugin hashes.
    By(&'a str),
    /// `AS '<hash>'`: an already-computed hash, validated for shape only.
    As(&'a str),
    /// Neither clause: a passwordless account.
    None,
}

/// Go `executor/utils.go`'s `encodedPassword`, minus the extension-plugin
/// branch: this tier registers no extension auth plugins, so
/// `encodePasswordWithPlugin` always falls to this path.
///
/// Returns the `authentication_string` to store, or
/// [`DriverError::PasswordFormat`] for an `AS` hash that does not match the
/// plugin's expected shape (Go's `ErrPasswordFormat`, 1827).
///
/// DEFERRED (documented): `tidb_sm3_password`'s `BY` form, which needs an
/// SM3 hasher this crate does not have and is refused outright rather than
/// stored under a fabricated hash. Every other plugin's `BY`/`AS` form,
/// including `tidb_sm3_password`'s `AS` form (a length check only, no
/// hashing needed), is captured and implemented exactly -- ORDER matters: an
/// LDAP plugin's `AS` form stores the `dn` verbatim before the general
/// empty/length rules apply, but an LDAP plugin's `BY` form is NOT special
/// (Go's `switch` only special-cases it in the `AS`/plugin-only arm) and
/// falls to the same native SHA1 hash every other unlisted plugin's `BY`
/// form does.
pub fn encode_password_for_plugin(
    plugin: &str,
    credential: &PluginCredential<'_>,
) -> Result<String, DriverError> {
    match credential {
        PluginCredential::By(password) => {
            if plugin == AuthCachingSha2Password {
                Ok(hash_caching_sha2(password))
            } else if plugin == AuthTiDBSM3Password {
                Err(DriverError::Unsupported(
                    "IDENTIFIED WITH tidb_sm3_password BY '<password>' needs SM3 hashing, \
                     not supported yet",
                ))
            } else if plugin == AuthSocket {
                Ok(String::new())
            } else {
                // Go's `default` arm: every other accepted plugin (native,
                // both LDAP forms, the token plugins) hashes a `BY`
                // password the native way.
                Ok(encode_password(password))
            }
        }
        PluginCredential::As(hash) => {
            if plugin == AuthLDAPSimple || plugin == AuthLDAPSASL {
                return Ok((*hash).to_owned());
            }
            if hash.is_empty() {
                return Ok(String::new());
            }
            let shaped = if plugin == AuthCachingSha2Password {
                hash.len() == SHAPWDHashLen as usize
            } else if plugin == AuthTiDBSM3Password {
                hash.len() == SM3PWDHashLen as usize
            } else if plugin == AuthNativePassword {
                hash.len() == PWDHashLen as usize + 1 && hash.starts_with('*')
            } else {
                plugin == AuthSocket
            };
            if shaped {
                Ok((*hash).to_owned())
            } else {
                Err(DriverError::PasswordFormat)
            }
        }
        PluginCredential::None => Ok(String::new()),
    }
}

/// SHA-crypt mixing width: Go's `MIXCHARS`, and also `sha256::Sum256`'s
/// output width, which is why the loops below can reuse a whole digest as
/// one "chunk".
const SHA_CRYPT_MIXCHARS: usize = 32;
/// Go's `SALT_LENGTH`.
const SHA_CRYPT_SALT_LEN: usize = 20;
/// Go's `ITERATION_MULTIPLIER`.
const SHA_CRYPT_ITERATION_MULTIPLIER: u32 = 1000;
/// Go's custom base64 alphabet for `b64From24bit`, distinct from RFC 4648.
const SHA_CRYPT_B64_ALPHABET: &[u8; 64] =
    b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// Go `pkg/parser/auth.b64From24bit`: packs three bytes into 24 bits and
/// emits `n` base64 digits, LEAST-significant 6 bits first.
fn sha_crypt_b64_from_24bit(bytes: [u8; 3], n: usize, out: &mut String) {
    let mut word = (u32::from(bytes[0]) << 16) | (u32::from(bytes[1]) << 8) | u32::from(bytes[2]);
    for _ in 0..n {
        out.push(SHA_CRYPT_B64_ALPHABET[(word & 0x3f) as usize] as char);
        word >>= 6;
    }
}

/// Go `pkg/parser/auth.NewHashPassword` for `caching_sha2_password`: a
/// SHA256-crypt-family hash (<https://www.akkadia.org/drepper/SHA-crypt.txt>)
/// with a random 20-byte salt and 5000 iterations, stored
/// `$A$005$<20-byte salt><43-char digest>` -- 70 bytes total,
/// `SHAPWDHashLen`. The salt excludes NUL and `$` exactly as Go's generator
/// does (see [`tidb_util::fastrand::buf`]).
fn hash_caching_sha2(password: &str) -> String {
    let salt = tidb_util::fastrand::buf(SHA_CRYPT_SALT_LEN as isize);
    sha_crypt(
        password,
        &salt,
        5 * SHA_CRYPT_ITERATION_MULTIPLIER,
        |input| Sha256Hash::digest(input).into(),
    )
}

/// Go `pkg/parser/auth.hashCrypt`, ported 1:1 (see the numbered steps in
/// Go's own comment referencing the akkadia.org SHA-crypt description).
/// `hash` must be a 32-byte digest function (SHA-256 for
/// `caching_sha2_password`; Go also drives this with SM3 for
/// `tidb_sm3_password`, which this port does not implement -- see
/// [`encode_password_for_plugin`]'s deferral note).
fn sha_crypt(
    plaintext: &str,
    salt: &[u8],
    iterations: u32,
    hash: impl Fn(&[u8]) -> [u8; 32],
) -> String {
    let pt = plaintext.as_bytes();

    // Steps 4-8: sumB = hash(pt + salt + pt).
    let mut buf_b = Vec::with_capacity(pt.len() * 2 + salt.len());
    buf_b.extend_from_slice(pt);
    buf_b.extend_from_slice(salt);
    buf_b.extend_from_slice(pt);
    let sum_b = hash(&buf_b);

    // Steps 1-3, 9-11: bufA = pt + salt, then sumB chunks and pt/sumB
    // alternating by the bits of len(pt).
    let mut buf_a = Vec::new();
    buf_a.extend_from_slice(pt);
    buf_a.extend_from_slice(salt);
    let mut i = pt.len();
    while i > SHA_CRYPT_MIXCHARS {
        buf_a.extend_from_slice(&sum_b[..SHA_CRYPT_MIXCHARS]);
        i -= SHA_CRYPT_MIXCHARS;
    }
    buf_a.extend_from_slice(&sum_b[..i]);
    let mut i = pt.len();
    while i > 0 {
        if i.is_multiple_of(2) {
            buf_a.extend_from_slice(pt);
        } else {
            buf_a.extend_from_slice(&sum_b);
        }
        i >>= 1;
    }
    // Step 12: sumA.
    let mut sum_a = hash(&buf_a);

    // Steps 13-16: sumDP = hash(pt repeated len(pt) times), then `p` built
    // from sumDP chunks sized by len(pt).
    let mut buf_dp = Vec::with_capacity(pt.len() * pt.len());
    for _ in 0..pt.len() {
        buf_dp.extend_from_slice(pt);
    }
    let sum_dp = hash(&buf_dp);
    let mut p = Vec::new();
    let mut i = pt.len();
    while i > 0 {
        if i > SHA_CRYPT_MIXCHARS {
            p.extend_from_slice(&sum_dp);
        } else {
            p.extend_from_slice(&sum_dp[..i]);
        }
        i = i.saturating_sub(SHA_CRYPT_MIXCHARS);
    }

    // Steps 17-20: sumDS = hash(salt repeated 16+sumA[0] times), then `s`
    // built from sumDS chunks sized by len(salt).
    let mut buf_ds = Vec::new();
    for _ in 0..(16 + usize::from(sum_a[0])) {
        buf_ds.extend_from_slice(salt);
    }
    let sum_ds = hash(&buf_ds);
    let mut s = Vec::new();
    let mut i = salt.len();
    while i > 0 {
        if i > SHA_CRYPT_MIXCHARS {
            s.extend_from_slice(&sum_ds);
        } else {
            s.extend_from_slice(&sum_ds[..i]);
        }
        i = i.saturating_sub(SHA_CRYPT_MIXCHARS);
    }

    // Step 21: the iterated mixing loop.
    for round in 0..iterations {
        let mut buf_c = Vec::new();
        if round & 1 != 0 {
            buf_c.extend_from_slice(&p);
        } else {
            buf_c.extend_from_slice(&sum_a);
        }
        if round % 3 != 0 {
            buf_c.extend_from_slice(&s);
        }
        if round % 7 != 0 {
            buf_c.extend_from_slice(&p);
        }
        if round & 1 != 0 {
            buf_c.extend_from_slice(&sum_a);
        } else {
            buf_c.extend_from_slice(&p);
        }
        sum_a = hash(&buf_c);
    }
    let sum_c = sum_a;

    // Step 22: `$A$<rounds>$<salt><permuted base64 of sumC>`.
    let mut out = String::with_capacity(SHAPWDHashLen as usize);
    out.push_str("$A$");
    out.push_str(&format!(
        "{:03X}",
        iterations / SHA_CRYPT_ITERATION_MULTIPLIER
    ));
    out.push('$');
    for &byte in salt {
        out.push(byte as char);
    }
    sha_crypt_b64_from_24bit([sum_c[0], sum_c[10], sum_c[20]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[21], sum_c[1], sum_c[11]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[12], sum_c[22], sum_c[2]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[3], sum_c[13], sum_c[23]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[24], sum_c[4], sum_c[14]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[15], sum_c[25], sum_c[5]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[6], sum_c[16], sum_c[26]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[27], sum_c[7], sum_c[17]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[18], sum_c[28], sum_c[8]], 4, &mut out);
    sha_crypt_b64_from_24bit([sum_c[9], sum_c[19], sum_c[29]], 4, &mut out);
    sha_crypt_b64_from_24bit([0, sum_c[31], sum_c[30]], 3, &mut out);
    out
}

type Sha256Hash = sha2::Sha256;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_bootstrapped_with_all_privileges_and_grant_option() {
        let registry = PrivilegeRegistry::default();
        assert!(registry.user_exists("root", "%"));
        assert_eq!(
            registry.show_grants("root", "%", &[]).as_deref(),
            Some("GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
        );
    }

    #[test]
    fn fresh_user_reports_usage() {
        let registry = PrivilegeRegistry::default();
        assert!(registry.create_user("u1", "%", ""));
        assert_eq!(
            registry.show_grants("u1", "%", &[]).as_deref(),
            Some("GRANT USAGE ON *.* TO 'u1'@'%'")
        );
        // Creating it again is refused, not silently accepted.
        assert!(!registry.create_user("u1", "%", ""));
    }

    #[test]
    fn grant_prints_in_fixed_go_order_not_insertion_order() {
        let registry = PrivilegeRegistry::default();
        registry.create_user("u1", "%", "");
        // Granted in scrambled order: SELECT, PROCESS, INSERT, SUPER, UPDATE.
        let mask = GlobalPriv::Select.bit()
            | GlobalPriv::Process.bit()
            | GlobalPriv::Insert.bit()
            | GlobalPriv::Super.bit()
            | GlobalPriv::Update.bit();
        registry.grant("u1", "%", mask);
        // Captured from Go: SELECT,INSERT,UPDATE,PROCESS,SUPER.
        assert_eq!(
            registry.show_grants("u1", "%", &[]).as_deref(),
            Some("GRANT SELECT,INSERT,UPDATE,PROCESS,SUPER ON *.* TO 'u1'@'%'")
        );
        registry.revoke("u1", "%", GlobalPriv::Super.bit());
        assert_eq!(
            registry.show_grants("u1", "%", &[]).as_deref(),
            Some("GRANT SELECT,INSERT,UPDATE,PROCESS ON *.* TO 'u1'@'%'")
        );
    }

    #[test]
    fn drop_user_reports_whether_it_existed() {
        let registry = PrivilegeRegistry::default();
        assert!(!registry.drop_user("nosuchuser", "%"));
        registry.create_user("u1", "%", "");
        assert!(registry.drop_user("u1", "%"));
        assert!(!registry.user_exists("u1", "%"));
    }

    #[test]
    fn all_privileges_collapses_to_the_literal() {
        let registry = PrivilegeRegistry::default();
        registry.create_user("u1", "%", "");
        registry.grant("u1", "%", all_privs_mask());
        assert_eq!(
            registry.show_grants("u1", "%", &[]).as_deref(),
            Some("GRANT ALL PRIVILEGES ON *.* TO 'u1'@'%'")
        );
    }

    #[test]
    fn from_grant_name_recognizes_multiword_privileges() {
        assert_eq!(
            GlobalPriv::from_grant_name("SHOW DATABASES"),
            Some(GlobalPriv::ShowDatabases)
        );
        assert_eq!(
            GlobalPriv::from_grant_name("CREATE TEMPORARY TABLES"),
            Some(GlobalPriv::CreateTemporaryTables)
        );
        assert_eq!(GlobalPriv::from_grant_name("FOOBAR"), None);
        assert_eq!(GlobalPriv::from_grant_name("DROP ROLE"), None);
    }

    #[test]
    fn db_and_table_scope_lines_follow_global_then_db_then_table() {
        // Captured from Go: GLOBAL line first, then DB-scope lines sorted
        // lexically by their formatted text (not by DB name or grant
        // order), then TABLE-scope lines the same way.
        let registry = PrivilegeRegistry::default();
        registry.create_user("u", "%", "");
        registry.grant_db("u", "%", "db1", GlobalPriv::Select.bit());
        registry.grant_table(
            "u",
            "%",
            "db1",
            "t1",
            GlobalPriv::Select.bit() | GlobalPriv::Insert.bit(),
        );
        registry.grant("u", "%", GlobalPriv::Select.bit());
        registry.grant_db("u", "%", "aaadb", GlobalPriv::Select.bit());
        assert_eq!(
            registry.show_grants("u", "%", &[]).as_deref(),
            Some(
                "GRANT SELECT ON *.* TO 'u'@'%'\n\
                 GRANT SELECT ON `aaadb`.* TO 'u'@'%'\n\
                 GRANT SELECT ON `db1`.* TO 'u'@'%'\n\
                 GRANT SELECT,INSERT ON `db1`.`t1` TO 'u'@'%'"
            )
        );
    }

    #[test]
    fn db_scope_all_privileges_collapses_and_usage_prints_for_fresh_row() {
        let registry = PrivilegeRegistry::default();
        registry.create_user("u", "%", "");
        registry.grant_db("u", "%", "db1", all_db_privs_mask());
        assert_eq!(
            registry.show_grants("u", "%", &[]).as_deref(),
            Some(
                "GRANT USAGE ON *.* TO 'u'@'%'\n\
                 GRANT ALL PRIVILEGES ON `db1`.* TO 'u'@'%'"
            )
        );
    }

    #[test]
    fn table_scope_all_privileges_collapses() {
        let registry = PrivilegeRegistry::default();
        registry.create_user("u", "%", "");
        registry.grant_table("u", "%", "db1", "t1", all_table_privs_mask());
        assert_eq!(
            registry.show_grants("u", "%", &[]).as_deref(),
            Some(
                "GRANT USAGE ON *.* TO 'u'@'%'\n\
                 GRANT ALL PRIVILEGES ON `db1`.`t1` TO 'u'@'%'"
            )
        );
    }

    #[test]
    fn revoke_db_clears_bits_and_row_existence_is_tracked_separately() {
        let registry = PrivilegeRegistry::default();
        registry.create_user("u", "%", "");
        assert!(!registry.db_grant_row_exists("u", "%", "db1"));
        registry.grant_db("u", "%", "db1", GlobalPriv::Select.bit());
        assert!(registry.db_grant_row_exists("u", "%", "db1"));
        // Revoking a privilege the row never had is a silent no-op.
        registry.revoke_db("u", "%", "db1", GlobalPriv::Update.bit());
        assert_eq!(
            registry.show_grants("u", "%", &[]).as_deref(),
            Some(
                "GRANT USAGE ON *.* TO 'u'@'%'\n\
                 GRANT SELECT ON `db1`.* TO 'u'@'%'"
            )
        );
        registry.revoke_db("u", "%", "db1", GlobalPriv::Select.bit());
        // The row still exists (Go never deletes it), it just reports
        // USAGE like a fresh account would.
        assert!(registry.db_grant_row_exists("u", "%", "db1"));
        assert_eq!(
            registry.show_grants("u", "%", &[]).as_deref(),
            Some(
                "GRANT USAGE ON *.* TO 'u'@'%'\n\
                 GRANT USAGE ON `db1`.* TO 'u'@'%'"
            )
        );
    }

    #[test]
    fn db_and_table_scope_reject_out_of_scope_privileges() {
        assert!(!GlobalPriv::Process.is_valid_at_db_scope());
        assert!(!GlobalPriv::Super.is_valid_at_db_scope());
        assert!(GlobalPriv::Select.is_valid_at_db_scope());
        assert!(!GlobalPriv::Process.is_valid_at_table_scope());
        assert!(!GlobalPriv::LockTables.is_valid_at_table_scope());
        assert!(GlobalPriv::CreateView.is_valid_at_table_scope());
    }

    #[test]
    fn create_user_plugins_match_gos_accepted_set() {
        assert!(is_create_user_plugin("mysql_native_password"));
        assert!(is_create_user_plugin("caching_sha2_password"));
        assert!(is_create_user_plugin("tidb_sm3_password"));
        assert!(is_create_user_plugin("auth_socket"));
        assert!(is_create_user_plugin("tidb_auth_token"));
        assert!(is_create_user_plugin("authentication_ldap_simple"));
        assert!(is_create_user_plugin("authentication_ldap_sasl"));
        // Captured: `mysql_clear_password` and `tidb_session_token` are
        // built-in plugin NAMES (reserved against extensions), but neither
        // is in Go's CREATE USER switch, so they are NOT accepted here.
        assert!(!is_create_user_plugin("mysql_clear_password"));
        assert!(!is_create_user_plugin("tidb_session_token"));
        assert!(!is_create_user_plugin("no_such_plugin"));
    }

    #[test]
    fn native_password_as_form_validates_hash_shape() {
        let hash40 = format!("*{}", "A".repeat(40));
        assert_eq!(
            encode_password_for_plugin("mysql_native_password", &PluginCredential::As(&hash40))
                .unwrap(),
            hash40
        );
        // Missing the leading `*`.
        assert!(matches!(
            encode_password_for_plugin(
                "mysql_native_password",
                &PluginCredential::As(&"A".repeat(41))
            ),
            Err(DriverError::PasswordFormat)
        ));
        // Wrong length.
        assert!(matches!(
            encode_password_for_plugin("mysql_native_password", &PluginCredential::As("*short")),
            Err(DriverError::PasswordFormat)
        ));
        // `AS ''` (or no clause at all) is always a passwordless account,
        // regardless of plugin.
        assert_eq!(
            encode_password_for_plugin("mysql_native_password", &PluginCredential::As("")).unwrap(),
            ""
        );
        assert_eq!(
            encode_password_for_plugin("mysql_native_password", &PluginCredential::None).unwrap(),
            ""
        );
    }

    #[test]
    fn caching_sha2_as_form_validates_length_only() {
        let hash70 = "x".repeat(70);
        assert_eq!(
            encode_password_for_plugin("caching_sha2_password", &PluginCredential::As(&hash70))
                .unwrap(),
            hash70
        );
        assert!(matches!(
            encode_password_for_plugin(
                "caching_sha2_password",
                &PluginCredential::As(&"x".repeat(69))
            ),
            Err(DriverError::PasswordFormat)
        ));
    }

    #[test]
    fn tidb_auth_token_as_form_always_rejects_a_nonempty_hash() {
        // Go's `encodedPassword` has no case for `tidb_auth_token` in its
        // AS/plugin-only switch, so it falls to `default: return "", false`
        // -- captured: unlike LDAP, a token account can only be created
        // passwordless (`IDENTIFIED WITH tidb_auth_token`, no BY/AS).
        assert!(matches!(
            encode_password_for_plugin("tidb_auth_token", &PluginCredential::As("anything")),
            Err(DriverError::PasswordFormat)
        ));
        assert_eq!(
            encode_password_for_plugin("tidb_auth_token", &PluginCredential::None).unwrap(),
            ""
        );
    }

    #[test]
    fn ldap_as_form_stores_the_dn_unvalidated() {
        assert_eq!(
            encode_password_for_plugin(
                "authentication_ldap_simple",
                &PluginCredential::As("cn=foo,dc=example,dc=com")
            )
            .unwrap(),
            "cn=foo,dc=example,dc=com"
        );
    }

    #[test]
    fn ldap_by_form_is_not_special_and_hashes_natively() {
        // Captured: Go's `ByAuthString` switch only special-cases
        // `caching_sha2_password`/`tidb_sm3_password` (hash) and
        // `auth_socket` (empty); LDAP's `BY` form falls to the SAME
        // `default: EncodePassword` arm every other unlisted plugin's `BY`
        // form does, even though LDAP normally authenticates via `AS` (a
        // stored `dn`).
        assert_eq!(
            encode_password_for_plugin("authentication_ldap_simple", &PluginCredential::By("pw"))
                .unwrap(),
            encode_password("pw")
        );
    }

    #[test]
    fn auth_socket_by_form_is_always_empty() {
        assert_eq!(
            encode_password_for_plugin("auth_socket", &PluginCredential::By("ignored")).unwrap(),
            ""
        );
    }

    #[test]
    fn sm3_by_form_is_deferred_rather_than_fabricated() {
        assert!(matches!(
            encode_password_for_plugin("tidb_sm3_password", &PluginCredential::By("pw")),
            Err(DriverError::Unsupported(_))
        ));
        // The AS form needs no hashing, only a length check, so it works.
        let hash70 = "y".repeat(70);
        assert_eq!(
            encode_password_for_plugin("tidb_sm3_password", &PluginCredential::As(&hash70))
                .unwrap(),
            hash70
        );
    }

    #[test]
    fn caching_sha2_hash_is_70_bytes_shaped_like_go_and_round_trips_its_own_check() {
        let hash = hash_caching_sha2("hunter2");
        assert_eq!(hash.len(), SHAPWDHashLen as usize);
        assert!(hash.starts_with("$A$005$"));
        // Two hashes of the same password differ (random salt), the same
        // way two `CREATE USER ... IDENTIFIED WITH caching_sha2_password
        // BY` calls never store the same bytes twice.
        assert_ne!(hash, hash_caching_sha2("hunter2"));

        // Self-consistency: re-deriving the digest from the STORED salt and
        // iteration count reproduces the stored hash exactly -- the same
        // property Go's `CheckHashingPassword` relies on to verify a login,
        // even though this port's wire front end does not call it yet (see
        // `encode_password_for_plugin`'s deferral note).
        let parts: Vec<&str> = hash.split('$').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[1], "A");
        let salt = &parts[3].as_bytes()[..SHA_CRYPT_SALT_LEN];
        let iterations =
            u32::from_str_radix(parts[2], 16).unwrap() * SHA_CRYPT_ITERATION_MULTIPLIER;
        let rederived = sha_crypt("hunter2", salt, iterations, |input| {
            Sha256Hash::digest(input).into()
        });
        assert_eq!(rederived, hash);
    }
}
