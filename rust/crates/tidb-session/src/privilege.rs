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
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

mod export;
mod password;
mod password_lock;
mod privs;
mod registry_ops;

pub use export::*;
pub use password::*;
pub use password_lock::*;
pub use privs::*;

/// One account identity, `(user, host)` -- the key of every table here, and
/// the shape a role is named by too, since a role IS an account.
pub type Account = (String, String);

/// `mysql.DB` row key: `(user, host, database)`.
type DbPrivKey = (String, String, String);
/// `mysql.Tables_priv` row key: `(user, host, database, table)`.
type TablePrivKey = (String, String, String, String);

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
    /// `plugin::CREATE_USER_PLUGINS`), and this column SELECTS THE LOGIN
    /// VERIFIER exactly as Go's does -- see [`login_plugin_verification`],
    /// which the wire front end's login path
    /// (`ConfiguredUserStore::authenticate_native`) consults before it
    /// compares anything. This matters because the plugins whose real
    /// handshake this tier cannot speak (`auth_socket`, `tidb_auth_token`,
    /// both LDAP forms) all store an EMPTY `auth_string`, so verifying them
    /// the native way would admit them with an empty password; they are
    /// refused instead, which is also Go's answer over TCP.
    plugin: String,
    /// Go's `mysql.user.user_attributes -> '$.Password_locking'`: the
    /// `FAILED_LOGIN_ATTEMPTS`/`PASSWORD_LOCK_TIME` policy AND the runtime
    /// failure counter tracked under it.
    ///
    /// `None` is Go's absent `Password_locking` key, and the invariant
    /// [`PrivilegeRegistry::set_password_locking_options`] maintains is that
    /// `None` holds exactly when both configured options are zero --
    /// captured: `ALTER USER u4 PASSWORD_LOCK_TIME 0 FAILED_LOGIN_ATTEMPTS 0`
    /// leaves `user_attributes` NULL, and a later `PASSWORD_LOCK_TIME 6`
    /// brings the whole object back as
    /// `{"failed_login_attempts": 0, "password_lock_time_days": 6}`. Because
    /// tracking requires BOTH options nonzero (Go's
    /// `IsAccountAutoLockEnabled`), an all-zero policy can never carry a
    /// nonzero counter, so collapsing it to `None` loses nothing.
    password_locking: Option<PasswordLocking>,
    /// Go's `mysql.user.Password_expired` ENUM('N','Y'): the account must
    /// change its password before it can do anything. `PASSWORD EXPIRE` sets
    /// it; storing a new password clears it (captured both ways).
    password_expired: bool,
    /// Go's `mysql.user.Password_lifetime` (SMALLINT UNSIGNED, nullable):
    /// `None` is NULL / `PASSWORD EXPIRE DEFAULT` (defer to the global
    /// `default_password_lifetime`), `Some(0)` is `PASSWORD EXPIRE NEVER`,
    /// and `Some(n)` is `PASSWORD EXPIRE INTERVAL n DAY` (all captured).
    password_lifetime: Option<i64>,
    /// Go's `mysql.user.Password_last_changed` TIMESTAMP, in Unix seconds:
    /// the instant an interval-based expiry counts from.
    password_last_changed: i64,
}

/// One Go `mysql.Columns_priv` row: the privileges an account holds on a
/// single column. Go's `columnsPrivRecord`.
#[derive(Clone, Debug)]
struct ColumnPrivRecord {
    user: String,
    host: String,
    database: String,
    table: String,
    column: String,
    privs: u64,
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
    /// Go `mysql.Columns_priv` rows. A `Vec` rather than a map because the
    /// row ORDER is observable: `SHOW GRANTS` lists a privilege's columns in
    /// the order Go's `columnsPriv` slice holds them, which is the order the
    /// rows were inserted -- captured, granting `SELECT` on `b`, then `a`,
    /// then `c` prints ``SELECT(b, a, c)``, not the sorted list.
    column_privs: Arc<Mutex<Vec<ColumnPrivRecord>>>,
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
    /// The wall clock `PASSWORD_LOCK_TIME` and `PASSWORD EXPIRE INTERVAL`
    /// are measured against. Shared with every clone of this registry, so a
    /// test that advances it moves the whole server's notion of "now".
    clock: Clock,
    /// Go's `vardef.IsSandBoxModeEnabled`, the server-wide inverse of the
    /// `disconnect_on_expired_password` option: with it OFF (Go's default) an
    /// expired password refuses the login with 1862, and with it ON the login
    /// succeeds into a sandbox session that may run nothing but
    /// `SET PASSWORD` / `ALTER USER` (both captured). Go reaches it through a
    /// process-global atomic set from server config, not from SQL; this
    /// registry is the equivalent server-wide home.
    sandbox_mode_enabled: Arc<AtomicBool>,
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
        let clock = Clock::default();
        let bootstrapped_at = clock.now_unix();
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
                        password_locking: None,
                        password_expired: false,
                        password_lifetime: None,
                        password_last_changed: bootstrapped_at,
                    },
                )
            })
            .collect();
        Self {
            users: Arc::new(Mutex::new(users)),
            db_privs: Arc::new(Mutex::new(HashMap::new())),
            table_privs: Arc::new(Mutex::new(HashMap::new())),
            column_privs: Arc::new(Mutex::new(Vec::new())),
            dynamic_privs: Arc::new(Mutex::new(HashMap::new())),
            role_edges: Arc::new(Mutex::new(HashMap::new())),
            default_roles: Arc::new(Mutex::new(HashMap::new())),
            clock,
            sandbox_mode_enabled: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[cfg(test)]
mod tests {
    use sha1::Digest;
    use tidb_executor::DriverError;
    use tidb_mysql::consts::{AuthTiDBSM3Password, SHAPWDHashLen};

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
        // The row still exists (Go never deletes it), but with no privilege
        // left it prints no line at all.
        assert!(registry.db_grant_row_exists("u", "%", "db1"));
        assert_eq!(
            registry.show_grants("u", "%", &[]).as_deref(),
            Some("GRANT USAGE ON *.* TO 'u'@'%'")
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
    fn sm3_by_form_hashes_and_verifies() {
        let stored =
            encode_password_for_plugin("tidb_sm3_password", &PluginCredential::By("pw")).unwrap();
        assert!(tidb_parser::auth::check_hashing_password_bytes(
            stored.as_bytes(),
            b"pw",
            AuthTiDBSM3Password,
        )
        .unwrap());
        assert!(!tidb_parser::auth::check_hashing_password_bytes(
            stored.as_bytes(),
            b"not-pw",
            AuthTiDBSM3Password,
        )
        .unwrap());

        // The AS form needs no hashing, only a length check, so it works.
        let hash70 = "y".repeat(70);
        assert_eq!(
            encode_password_for_plugin("tidb_sm3_password", &PluginCredential::As(&hash70))
                .unwrap(),
            hash70
        );
    }

    /// Go's `CREATE USER ... IDENTIFIED WITH tidb_sm3_password BY 'foobar'`
    /// stores an authentication string that Go's own `CheckHashingPassword`
    /// verifies; a captured golden hash (`pkg/parser/auth/tidb_sm3_test.go`'s
    /// `foobarPwdSM3Hash`) must verify identically through this crate's
    /// check function, proving the format (not just this crate's own round
    /// trip) matches Go.
    #[test]
    fn sm3_password_hash_captured_from_go_verifies_here() {
        let hex = "24412430303524031a69251c34295c4b35167c7f1e5a7b63091349536c72627066426a635061762e556e6c63533159414d7762317261324a5a3047756b4244664177434e3043";
        let stored: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        assert!(tidb_parser::auth::check_hashing_password_bytes(
            &stored,
            b"foobar",
            AuthTiDBSM3Password
        )
        .unwrap());
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

    #[test]
    fn replace_from_publishes_a_new_user_to_every_existing_clone() {
        // The live registry a session factory and the login path already
        // hold is `live`; `fresh` stands in for one reload pass's throwaway
        // build. `cloned` is what a session opened before the reload holds --
        // it must see the new account too, because it shares the same
        // Mutexes rather than a snapshot.
        let live = PrivilegeRegistry::bootstrapped_from(Vec::new());
        let cloned = live.clone();
        assert!(!live.user_exists("u1", "%"));

        let fresh = PrivilegeRegistry::bootstrapped_from(Vec::new());
        fresh.create_user("u1", "%", "");
        fresh.grant("u1", "%", GlobalPriv::Select.bit());

        live.replace_from(&fresh);

        assert!(live.user_exists("u1", "%"));
        assert!(cloned.user_exists("u1", "%"));
        assert!(cloned.has_global_priv("u1", "%", GlobalPriv::Select));
    }

    #[test]
    fn replace_from_revokes_and_drops_accounts_the_fresh_snapshot_no_longer_has() {
        let live = PrivilegeRegistry::bootstrapped_from(Vec::new());
        live.create_user("stale", "%", "");
        live.grant("stale", "%", GlobalPriv::Select.bit());
        live.create_user("kept", "%", "");
        live.grant("kept", "%", all_privs_mask());

        // The fresh snapshot dropped `stale` entirely and revoked one
        // privilege from `kept` -- exactly what a `DROP USER stale` and a
        // `REVOKE ... FROM kept` on the Go side would produce next tick.
        let fresh = PrivilegeRegistry::bootstrapped_from(Vec::new());
        fresh.create_user("kept", "%", "");
        fresh.grant("kept", "%", GlobalPriv::Select.bit());

        live.replace_from(&fresh);

        assert!(!live.user_exists("stale", "%"));
        assert!(live.user_exists("kept", "%"));
        assert!(live.has_global_priv("kept", "%", GlobalPriv::Select));
        assert!(!live.has_global_priv("kept", "%", GlobalPriv::Super));
    }

    #[test]
    fn a_role_dropped_by_replace_from_stops_granting_though_it_stays_named_active() {
        // Go's own behavior (`FindAllUserEffectiveRoles`): a session's active
        // role list is never edited by a reload. It just stops being backed
        // by a role_edges entry, so every later privilege check silently
        // filters it out.
        let live = PrivilegeRegistry::bootstrapped_from(Vec::new());
        live.create_user("bridge", "%", "");
        live.create_role("r1", "%");
        let bridge: Account = ("bridge".to_owned(), "%".to_owned());
        let role: Account = ("r1".to_owned(), "%".to_owned());
        live.grant_role(&role, &bridge);
        live.grant("r1", "%", GlobalPriv::Select.bit());

        let active_roles = vec![role.clone()];
        assert!(live.has_global_priv_with_roles("bridge", "%", &active_roles, GlobalPriv::Select));

        // The fresh snapshot is what a cluster-wide `DROP ROLE r1` produces:
        // no `r1` row, no edge granting it to `bridge`.
        let fresh = PrivilegeRegistry::bootstrapped_from(Vec::new());
        fresh.create_user("bridge", "%", "");
        live.replace_from(&fresh);

        // The session's own active-role list still names `r1` -- nothing in
        // `replace_from` reached into it -- but the role no longer grants
        // anything, because `effective_roles` filters `active_roles` against
        // the (now empty) role_edges on every check.
        assert!(!live.has_global_priv_with_roles("bridge", "%", &active_roles, GlobalPriv::Select));
        assert!(live.effective_roles(&bridge, &active_roles).is_empty());
    }
}
