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
//! OUT OF SCOPE (refused rather than faked): column-level grants, roles
//! (`CREATE ROLE`/`GRANT ROLE`/`DROP ROLE`), dynamic privileges, and
//! `WITH GRANT OPTION`. Root's bootstrap grant is the one place
//! `GRANT OPTION` appears, and it is a hardcoded display fact, never a
//! settable bit.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
    fn print_name(self) -> &'static str {
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
        }
    }

    /// Resolves the exact spelling `tidb-parser`'s `GrantPrivilege::name`
    /// restores for a standard (non-dynamic) privilege token. Returns `None`
    /// for names this tier does not model as a global static privilege
    /// (roles, `GRANT OPTION`, anything dynamic) -- the caller decides how to
    /// refuse those.
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
        ALL_DB_PRIVS.contains(&self)
    }

    /// Whether this privilege is one Go's TABLE-scope grant path accepts
    /// (anything outside `mysql.AllTablePrivs` is refused: captured
    /// `ErrIllegalGrantForTable`/1144).
    #[must_use]
    pub fn is_valid_at_table_scope(self) -> bool {
        ALL_TABLE_PRIVS.contains(&self)
    }
}

/// The mask with every privilege in [`ALL_TABLE_PRIVS`] set -- the
/// TABLE-scope analogue of [`all_db_privs_mask`] (Go `tablePrivToString`).
pub(crate) fn all_table_privs_mask() -> u64 {
    ALL_TABLE_PRIVS
        .iter()
        .fold(0u64, |mask, priv_| mask | priv_.bit())
}

/// One account's global-privilege state.
struct UserRecord {
    privs: u64,
    /// Set only for the bootstrap `root` account: a hardcoded display fact
    /// (Go's real `WITH GRANT OPTION`), never produced by a `GRANT` this
    /// tier executes, since `WITH GRANT OPTION` itself is refused.
    grant_option: bool,
}

impl UserRecord {
    fn fresh() -> Self {
        Self {
            privs: 0,
            grant_option: false,
        }
    }
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
    fn default() -> Self {
        let registry = Self {
            users: Arc::new(Mutex::new(HashMap::new())),
            db_privs: Arc::new(Mutex::new(HashMap::new())),
            table_privs: Arc::new(Mutex::new(HashMap::new())),
        };
        registry.lock().insert(
            (
                BOOTSTRAP_ROOT_USER.to_owned(),
                BOOTSTRAP_ROOT_HOST.to_owned(),
            ),
            UserRecord {
                privs: all_privs_mask(),
                grant_option: true,
            },
        );
        registry
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
    pub fn create_user(&self, user: &str, host: &str) -> bool {
        let key = (user.to_owned(), host.to_owned());
        let mut guard = self.lock();
        if guard.contains_key(&key) {
            return false;
        }
        guard.insert(key, UserRecord::fresh());
        true
    }

    /// Go's `ErrCannotUser("DROP USER", ...)`: dropping a missing account
    /// fails unless the caller handles `IF EXISTS` itself. Returns whether
    /// the account existed (and was removed).
    pub fn drop_user(&self, user: &str, host: &str) -> bool {
        self.lock()
            .remove(&(user.to_owned(), host.to_owned()))
            .is_some()
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

    /// Whether the account holds `global_priv`, for privilege checks like
    /// `SHOW PROCESSLIST`'s `PROCESS` gate.
    #[must_use]
    pub fn has_global_priv(&self, user: &str, host: &str, global_priv: GlobalPriv) -> bool {
        self.lock()
            .get(&(user.to_owned(), host.to_owned()))
            .is_some_and(|record| record.privs & global_priv.bit() != 0)
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

    /// Go `MySQLPrivilege.showGrants`'s global-scope slice: `None` when the
    /// account has no grant row at all (Go's `ErrNonexistingGrant`), `Some`
    /// with the one `GRANT ... ON *.* TO '<user>'@'<host>'` line otherwise
    /// (or the `GRANT USAGE ...` line Go prints for an account with zero
    /// privileges -- "this is a mysql convention").
    #[must_use]
    pub fn show_grants(&self, user: &str, host: &str) -> Option<String> {
        let global_line = {
            let guard = self.lock();
            let record = guard.get(&(user.to_owned(), host.to_owned()))?;
            let with_grant = if record.grant_option {
                " WITH GRANT OPTION"
            } else {
                ""
            };
            if record.privs == all_privs_mask() {
                format!("GRANT ALL PRIVILEGES ON *.* TO '{user}'@'{host}'{with_grant}")
            } else {
                let names: Vec<&str> = ALL_GLOBAL_PRIVS
                    .iter()
                    .filter(|priv_| record.privs & priv_.bit() != 0)
                    .map(|priv_| priv_.print_name())
                    .collect();
                if names.is_empty() {
                    format!("GRANT USAGE ON *.* TO '{user}'@'{host}'{with_grant}")
                } else {
                    format!(
                        "GRANT {} ON *.* TO '{user}'@'{host}'{with_grant}",
                        names.join(",")
                    )
                }
            }
        };

        // DB-scope lines: Go's showGrants sorts these lexically by the
        // formatted `GRANT ... ON db.* ...` string (captured: a grant on
        // `aaadb` prints before one on `db1`, even though `db1` was granted
        // first) -- not insertion order, not plain DB-name order.
        let mut db_lines: Vec<String> = self
            .lock_db()
            .iter()
            .filter(|((row_user, row_host, _), _)| row_user == user && row_host == host)
            .map(|((_, _, database), privs)| {
                let names: Vec<&str> = ALL_DB_PRIVS
                    .iter()
                    .filter(|priv_| privs & priv_.bit() != 0)
                    .map(|priv_| priv_.print_name())
                    .collect();
                let priv_text = if *privs == all_db_privs_mask() {
                    "ALL PRIVILEGES".to_owned()
                } else if names.is_empty() {
                    "USAGE".to_owned()
                } else {
                    names.join(",")
                };
                format!("GRANT {priv_text} ON `{database}`.* TO '{user}'@'{host}'")
            })
            .collect();
        db_lines.sort_unstable();

        // TABLE-scope lines: same lexical-sort rule as DB-scope.
        let mut table_lines: Vec<String> = self
            .lock_table()
            .iter()
            .filter(|((row_user, row_host, _, _), _)| row_user == user && row_host == host)
            .map(|((_, _, database, table), privs)| {
                let names: Vec<&str> = ALL_TABLE_PRIVS
                    .iter()
                    .filter(|priv_| privs & priv_.bit() != 0)
                    .map(|priv_| priv_.print_name())
                    .collect();
                let priv_text = if *privs == all_table_privs_mask() {
                    "ALL PRIVILEGES".to_owned()
                } else if names.is_empty() {
                    "USAGE".to_owned()
                } else {
                    names.join(",")
                };
                format!("GRANT {priv_text} ON `{database}`.`{table}` TO '{user}'@'{host}'")
            })
            .collect();
        table_lines.sort_unstable();

        let mut lines = Vec::with_capacity(1 + db_lines.len() + table_lines.len());
        lines.push(global_line);
        lines.extend(db_lines);
        lines.extend(table_lines);
        Some(lines.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_bootstrapped_with_all_privileges_and_grant_option() {
        let registry = PrivilegeRegistry::default();
        assert!(registry.user_exists("root", "%"));
        assert_eq!(
            registry.show_grants("root", "%").as_deref(),
            Some("GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
        );
    }

    #[test]
    fn fresh_user_reports_usage() {
        let registry = PrivilegeRegistry::default();
        assert!(registry.create_user("u1", "%"));
        assert_eq!(
            registry.show_grants("u1", "%").as_deref(),
            Some("GRANT USAGE ON *.* TO 'u1'@'%'")
        );
        // Creating it again is refused, not silently accepted.
        assert!(!registry.create_user("u1", "%"));
    }

    #[test]
    fn grant_prints_in_fixed_go_order_not_insertion_order() {
        let registry = PrivilegeRegistry::default();
        registry.create_user("u1", "%");
        // Granted in scrambled order: SELECT, PROCESS, INSERT, SUPER, UPDATE.
        let mask = GlobalPriv::Select.bit()
            | GlobalPriv::Process.bit()
            | GlobalPriv::Insert.bit()
            | GlobalPriv::Super.bit()
            | GlobalPriv::Update.bit();
        registry.grant("u1", "%", mask);
        // Captured from Go: SELECT,INSERT,UPDATE,PROCESS,SUPER.
        assert_eq!(
            registry.show_grants("u1", "%").as_deref(),
            Some("GRANT SELECT,INSERT,UPDATE,PROCESS,SUPER ON *.* TO 'u1'@'%'")
        );
        registry.revoke("u1", "%", GlobalPriv::Super.bit());
        assert_eq!(
            registry.show_grants("u1", "%").as_deref(),
            Some("GRANT SELECT,INSERT,UPDATE,PROCESS ON *.* TO 'u1'@'%'")
        );
    }

    #[test]
    fn drop_user_reports_whether_it_existed() {
        let registry = PrivilegeRegistry::default();
        assert!(!registry.drop_user("nosuchuser", "%"));
        registry.create_user("u1", "%");
        assert!(registry.drop_user("u1", "%"));
        assert!(!registry.user_exists("u1", "%"));
    }

    #[test]
    fn all_privileges_collapses_to_the_literal() {
        let registry = PrivilegeRegistry::default();
        registry.create_user("u1", "%");
        registry.grant("u1", "%", all_privs_mask());
        assert_eq!(
            registry.show_grants("u1", "%").as_deref(),
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
        registry.create_user("u", "%");
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
            registry.show_grants("u", "%").as_deref(),
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
        registry.create_user("u", "%");
        registry.grant_db("u", "%", "db1", all_db_privs_mask());
        assert_eq!(
            registry.show_grants("u", "%").as_deref(),
            Some(
                "GRANT USAGE ON *.* TO 'u'@'%'\n\
                 GRANT ALL PRIVILEGES ON `db1`.* TO 'u'@'%'"
            )
        );
    }

    #[test]
    fn table_scope_all_privileges_collapses() {
        let registry = PrivilegeRegistry::default();
        registry.create_user("u", "%");
        registry.grant_table("u", "%", "db1", "t1", all_table_privs_mask());
        assert_eq!(
            registry.show_grants("u", "%").as_deref(),
            Some(
                "GRANT USAGE ON *.* TO 'u'@'%'\n\
                 GRANT ALL PRIVILEGES ON `db1`.`t1` TO 'u'@'%'"
            )
        );
    }

    #[test]
    fn revoke_db_clears_bits_and_row_existence_is_tracked_separately() {
        let registry = PrivilegeRegistry::default();
        registry.create_user("u", "%");
        assert!(!registry.db_grant_row_exists("u", "%", "db1"));
        registry.grant_db("u", "%", "db1", GlobalPriv::Select.bit());
        assert!(registry.db_grant_row_exists("u", "%", "db1"));
        // Revoking a privilege the row never had is a silent no-op.
        registry.revoke_db("u", "%", "db1", GlobalPriv::Update.bit());
        assert_eq!(
            registry.show_grants("u", "%").as_deref(),
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
            registry.show_grants("u", "%").as_deref(),
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
}
