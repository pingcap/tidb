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
//! OUT OF SCOPE (refused rather than faked): database- and table-level
//! grants, roles (`CREATE ROLE`/`GRANT ROLE`/`DROP ROLE`), dynamic
//! privileges, and `WITH GRANT OPTION`. Root's bootstrap grant is the one
//! place `GRANT OPTION` appears, and it is a hardcoded display fact, never a
//! settable bit.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

    /// Go `MySQLPrivilege.showGrants`'s global-scope slice: `None` when the
    /// account has no grant row at all (Go's `ErrNonexistingGrant`), `Some`
    /// with the one `GRANT ... ON *.* TO '<user>'@'<host>'` line otherwise
    /// (or the `GRANT USAGE ...` line Go prints for an account with zero
    /// privileges -- "this is a mysql convention").
    #[must_use]
    pub fn show_grants(&self, user: &str, host: &str) -> Option<String> {
        let guard = self.lock();
        let record = guard.get(&(user.to_owned(), host.to_owned()))?;
        let with_grant = if record.grant_option {
            " WITH GRANT OPTION"
        } else {
            ""
        };
        if record.privs == all_privs_mask() {
            return Some(format!(
                "GRANT ALL PRIVILEGES ON *.* TO '{user}'@'{host}'{with_grant}"
            ));
        }
        let names: Vec<&str> = ALL_GLOBAL_PRIVS
            .iter()
            .filter(|priv_| record.privs & priv_.bit() != 0)
            .map(|priv_| priv_.print_name())
            .collect();
        if names.is_empty() {
            return Some(format!(
                "GRANT USAGE ON *.* TO '{user}'@'{host}'{with_grant}"
            ));
        }
        Some(format!(
            "GRANT {} ON *.* TO '{user}'@'{host}'{with_grant}",
            names.join(",")
        ))
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
}
