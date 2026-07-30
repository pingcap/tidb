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

//! Every operation on [`PrivilegeRegistry`]: account creation and removal,
//! grant and revoke at each scope, role activation, login verification and
//! `SHOW GRANTS` rendering.
//!
//! Mirrors Go `pkg/privilege/privileges/cache.go` (`MySQLPrivilege`'s
//! request-verification and `showGrants`) and
//! `pkg/privilege/privileges/privileges.go` (`UserPrivileges`).

use std::sync::atomic::Ordering;

use super::*;

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

    fn lock_column(&self) -> std::sync::MutexGuard<'_, Vec<ColumnPrivRecord>> {
        self.column_privs
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
                password_locking: None,
                // Go's `CREATE ROLE` writes `Password_expired='Y'` while
                // `CREATE USER` leaves it `'N'` (captured: `SHOW CREATE USER`
                // for a role prints a bare `PASSWORD EXPIRE`, for a user
                // `PASSWORD EXPIRE DEFAULT`). Roles never log in, so this is
                // a display difference, but it is the row Go writes.
                password_expired: is_role,
                password_lifetime: None,
                password_last_changed: self.clock.now_unix(),
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

    /// Publishes `fresh`'s account and grant tables into `self`, in place.
    ///
    /// This is the live-refresh publish step: every clone of `self` shares
    /// these same `Mutex`es (every session, the login path, `SHOW GRANTS`),
    /// so replacing each table's contents under its own lock is visible to
    /// all of them immediately, with no second registry to swap in from
    /// outside. `fresh` is normally a throwaway registry built fresh by
    /// [`crate::privilege`]'s caller
    /// (`tidb_server::cluster_privileges::registry_from_cluster`) from one
    /// cluster snapshot, so each table is replaced whole rather than
    /// reconciled row by row -- the same "publish whole" philosophy
    /// [`crate::catalog_watch::CatalogReloader`] uses for the schema
    /// catalog, applied per table because this registry has no single outer
    /// handle to swap.
    ///
    /// `clock` and `sandbox_mode_enabled` are left alone: they are this
    /// server's own runtime settings, not `mysql.*` state a cluster snapshot
    /// carries.
    ///
    /// A session's own `active_roles` list is untouched by this call and may
    /// still name a role this replaces away. That is intentional and matches
    /// Go: `MySQLPrivilege.FindAllUserEffectiveRoles` filters the session's
    /// `activeRoles` down to whichever are still found in the reloaded role
    /// graph on every check, silently dropping the rest, rather than the
    /// reload reaching into live sessions to deactivate them. Because this
    /// registry's own [`Self::effective_roles`] performs exactly that same
    /// filter against `role_edges`, a role dropped elsewhere stops granting
    /// anything the moment this call removes its edges -- the active-role
    /// list is no longer changed, but it stops mattering.
    pub fn replace_from(&self, fresh: &Self) {
        *self.lock() = std::mem::take(&mut *fresh.lock());
        *self.lock_db() = std::mem::take(&mut *fresh.lock_db());
        *self.lock_table() = std::mem::take(&mut *fresh.lock_table());
        *self.lock_column() = std::mem::take(&mut *fresh.lock_column());
        *self.lock_dynamic() = std::mem::take(&mut *fresh.lock_dynamic());
        *self.lock_role_edges() = std::mem::take(&mut *fresh.lock_role_edges());
        *self.lock_default_roles() = std::mem::take(&mut *fresh.lock_default_roles());
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
        for row in self.lock_column().iter_mut() {
            if row.user == old_user && row.host == old_host {
                row.user = new_user.to_owned();
                row.host = new_host.to_owned();
            }
        }
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
        let now = self.clock.now_unix();
        match self.lock().get_mut(&(user.to_owned(), host.to_owned())) {
            Some(record) => {
                record.is_role = locked;
                // `ACCOUNT UNLOCK` also clears the failed-login counter and
                // the AUTO lock: Go's `alterUserFailedLoginJSON` writes
                // `auto_account_locked: "N"`, a fresh
                // `auto_locked_last_changed`, and `failed_login_count: 0`
                // whenever the statement's `lockAccount` is `"N"`. Captured:
                // after `ALTER USER L1 ACCOUNT UNLOCK` the account reports
                // count 0 / locked N and the next correct password works.
                if !locked {
                    if let Some(locking) = record.password_locking.as_mut() {
                        locking.failed_login_count = 0;
                        locking.auto_account_locked = false;
                        locking.auto_locked_last_changed = now;
                    }
                }
                true
            }
            None => false,
        }
    }

    /// This registry's shared wall clock. Handing it out (rather than hiding
    /// it) is what lets a test move `PASSWORD_LOCK_TIME`'s and
    /// `PASSWORD EXPIRE INTERVAL`'s notion of "now" without waiting days.
    #[must_use]
    pub fn clock(&self) -> Clock {
        self.clock.clone()
    }

    /// Whether the server admits an expired-password login into a sandbox
    /// session instead of refusing it -- Go's `vardef.IsSandBoxModeEnabled`.
    #[must_use]
    pub fn sandbox_mode_enabled(&self) -> bool {
        self.sandbox_mode_enabled.load(Ordering::Relaxed)
    }

    /// Sets [`Self::sandbox_mode_enabled`] for the whole server.
    pub fn set_sandbox_mode_enabled(&self, enabled: bool) {
        self.sandbox_mode_enabled.store(enabled, Ordering::Relaxed);
    }

    /// The account's `Password_locking` object, or `None` when it has none.
    #[must_use]
    pub fn password_locking(&self, user: &str, host: &str) -> Option<PasswordLocking> {
        self.lock()
            .get(&(user.to_owned(), host.to_owned()))
            .and_then(|record| record.password_locking)
    }

    /// Applies a statement's `FAILED_LOGIN_ATTEMPTS` / `PASSWORD_LOCK_TIME`
    /// clauses, each `None` when the statement wrote none.
    ///
    /// Go merges the written options over the account's CURRENT ones
    /// (`readPasswordLockingInfo` reads the row, `alterUserFailedLoginJSON`
    /// rewrites the whole object), so `ALTER USER u3 PASSWORD_LOCK_TIME 6`
    /// keeps u3's existing `FAILED_LOGIN_ATTEMPTS 3` -- captured. A merge
    /// leaving both at zero drops the object entirely, which is the same
    /// captured `user_attributes IS NULL` an explicit
    /// `PASSWORD_LOCK_TIME 0 FAILED_LOGIN_ATTEMPTS 0` produces.
    ///
    /// Returns whether the account existed.
    pub fn set_password_locking_options(
        &self,
        user: &str,
        host: &str,
        failed_login_attempts: Option<i64>,
        password_lock_time_days: Option<i64>,
    ) -> bool {
        match self.lock().get_mut(&(user.to_owned(), host.to_owned())) {
            Some(record) => {
                let mut locking = record.password_locking.unwrap_or_default();
                if let Some(attempts) = failed_login_attempts {
                    locking.failed_login_attempts = attempts;
                }
                if let Some(days) = password_lock_time_days {
                    locking.password_lock_time_days = days;
                }
                record.password_locking = (locking.failed_login_attempts != 0
                    || locking.password_lock_time_days != 0)
                    .then_some(locking);
                true
            }
            None => false,
        }
    }

    /// The account's `Password_expired` / `Password_lifetime` /
    /// `Password_last_changed` columns, or `None` when no such account.
    #[must_use]
    pub fn password_expiry(&self, user: &str, host: &str) -> Option<PasswordExpiry> {
        self.lock()
            .get(&(user.to_owned(), host.to_owned()))
            .map(|record| PasswordExpiry {
                expired: record.password_expired,
                lifetime: record.password_lifetime,
                last_changed: record.password_last_changed,
            })
    }

    /// Applies one `PASSWORD EXPIRE ...` clause. Go's `loadOptions` scans the
    /// option list BACKWARD and stops at the first expiry clause, so the LAST
    /// one written wins and the caller passes only that one; each variant
    /// writes exactly one of the two columns, leaving the other alone
    /// (captured: `PASSWORD EXPIRE` sets `password_expired='Y'` and does not
    /// touch `password_lifetime`, and `PASSWORD EXPIRE NEVER` sets
    /// `password_lifetime=0` and does not touch `password_expired`).
    ///
    /// Returns whether the account existed.
    pub fn set_password_expire(
        &self,
        user: &str,
        host: &str,
        setting: PasswordExpireSetting,
    ) -> bool {
        match self.lock().get_mut(&(user.to_owned(), host.to_owned())) {
            Some(record) => {
                match setting {
                    PasswordExpireSetting::Now => record.password_expired = true,
                    PasswordExpireSetting::Default => record.password_lifetime = None,
                    PasswordExpireSetting::Never => record.password_lifetime = Some(0),
                    PasswordExpireSetting::Interval(days) => record.password_lifetime = Some(days),
                }
                true
            }
            None => false,
        }
    }

    /// Records that the account's password was just replaced: Go's
    /// `ALTER USER ... IDENTIFIED BY` / `SET PASSWORD` writes
    /// `password_expired='N'` and a fresh `Password_last_changed` alongside
    /// the new hash, which is what lets a sandboxed session escape by
    /// changing its own password (captured: `ALTER USER e5 IDENTIFIED BY`
    /// flips `SHOW CREATE USER` from `PASSWORD EXPIRE` back to
    /// `PASSWORD EXPIRE DEFAULT`).
    ///
    /// Returns whether the account existed.
    pub fn mark_password_changed(&self, user: &str, host: &str) -> bool {
        let now = self.clock.now_unix();
        match self.lock().get_mut(&(user.to_owned(), host.to_owned())) {
            Some(record) => {
                record.password_expired = false;
                record.password_last_changed = now;
                true
            }
            None => false,
        }
    }

    /// Go's `pkg/session.verifyAccountAutoLock`, run BEFORE the password is
    /// compared: an account still inside its `PASSWORD_LOCK_TIME` window
    /// reports 3955 no matter which password arrived, and an account whose
    /// window has run out is auto-unlocked here so the very next correct
    /// password works.
    ///
    /// # Errors
    /// [`AccountLockout`] when the account is auto-locked and its lock window
    /// has not elapsed.
    pub fn verify_account_auto_lock(&self, user: &str, host: &str) -> Result<(), AccountLockout> {
        let now = self.clock.now_unix();
        let mut guard = self.lock();
        let Some(record) = guard.get_mut(&(user.to_owned(), host.to_owned())) else {
            return Ok(());
        };
        let Some(locking) = record.password_locking.as_mut() else {
            return Ok(());
        };
        if !locking.tracking_enabled() || !locking.auto_account_locked {
            return Ok(());
        }
        if locking.password_lock_time_days == -1 {
            return Err(lockout(user, host, locking, "unlimited".to_owned()));
        }
        let elapsed = now - locking.auto_locked_last_changed;
        if elapsed > locking.password_lock_time_days * SECONDS_PER_DAY {
            locking.auto_account_locked = false;
            locking.failed_login_count = 0;
            locking.auto_locked_last_changed = now;
            return Ok(());
        }
        // Go: `ceil(lockTime - d/86400)` -- a lock that has just been taken
        // still reports its full length remaining (captured: a freshly locked
        // 3-day account says "3 day(s) remaining").
        let remaining = (locking.password_lock_time_days as f64
            - elapsed as f64 / SECONDS_PER_DAY as f64)
            .ceil() as i64;
        Err(lockout(user, host, locking, remaining.to_string()))
    }

    /// Go's `pkg/session.authFailedTracking` -> `userAutoAccountLocked` ->
    /// `autolockAction`: one more consecutive wrong password. The attempt
    /// that REACHES `FAILED_LOGIN_ATTEMPTS` both locks the account and
    /// reports 3955; every attempt before it only bumps the counter and lets
    /// the caller report the ordinary 1045 (captured on a
    /// `FAILED_LOGIN_ATTEMPTS 2` account: first attempt 1045, second 3955).
    ///
    /// # Errors
    /// [`AccountLockout`] when this attempt locked the account (or found it
    /// already locked behind a stale cache).
    pub fn record_failed_login(&self, user: &str, host: &str) -> Result<(), AccountLockout> {
        let now = self.clock.now_unix();
        let mut guard = self.lock();
        let Some(record) = guard.get_mut(&(user.to_owned(), host.to_owned())) else {
            return Ok(());
        };
        let Some(locking) = record.password_locking.as_mut() else {
            return Ok(());
        };
        if !locking.tracking_enabled() {
            return Ok(());
        }
        if locking.auto_account_locked {
            let lock_days = locking.lock_days_text();
            return Err(lockout(user, host, locking, lock_days));
        }
        locking.failed_login_count += 1;
        if locking.failed_login_count < locking.failed_login_attempts {
            return Ok(());
        }
        locking.auto_account_locked = true;
        locking.auto_locked_last_changed = now;
        let lock_days = locking.lock_days_text();
        Err(lockout(user, host, locking, lock_days))
    }

    /// Go's `pkg/session.authSuccessClearCount`: the password was right, so
    /// the consecutive-failure counter goes back to zero -- unless the row
    /// says the account is locked after all, in which case the correct
    /// password is refused too (captured: the right password against a locked
    /// account reports 3955, not success).
    ///
    /// # Errors
    /// [`AccountLockout`] when the account is auto-locked.
    pub fn clear_failed_login_count(&self, user: &str, host: &str) -> Result<(), AccountLockout> {
        let mut guard = self.lock();
        let Some(record) = guard.get_mut(&(user.to_owned(), host.to_owned())) else {
            return Ok(());
        };
        let Some(locking) = record.password_locking.as_mut() else {
            return Ok(());
        };
        if locking.auto_account_locked {
            let lock_days = locking.lock_days_text();
            return Err(lockout(user, host, locking, lock_days));
        }
        locking.failed_login_count = 0;
        Ok(())
    }

    /// Go's `UserPrivileges.CheckPasswordExpired`, run after the password
    /// verifies. Answers whether the session must start in SANDBOX mode.
    ///
    /// `default_password_lifetime` is the global variable a `NULL`
    /// `Password_lifetime` defers to. This tier does not model GLOBAL-scope
    /// sysvar persistence (see `crate::vars`), so its callers pass the
    /// unset default `0` -- meaning a `PASSWORD EXPIRE DEFAULT` account never
    /// ages out, which is exactly what Go does on a cluster nobody has set
    /// the variable on.
    ///
    /// # Errors
    /// [`PasswordExpiredLogin`] (error 1862) when the password has expired
    /// and sandbox mode is off.
    pub fn check_password_expired(
        &self,
        user: &str,
        host: &str,
        default_password_lifetime: i64,
    ) -> Result<bool, PasswordExpiredLogin> {
        let Some(expiry) = self.password_expiry(user, host) else {
            return Ok(false);
        };
        let sandbox = self.sandbox_mode_enabled();
        let aged_out = || {
            let lifetime = expiry.lifetime.unwrap_or(default_password_lifetime);
            lifetime > 0 && self.clock.now_unix() > expiry.last_changed + lifetime * SECONDS_PER_DAY
        };
        if expiry.expired || aged_out() {
            if sandbox {
                return Ok(true);
            }
            return Err(PasswordExpiredLogin);
        }
        Ok(false)
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
        self.lock_column()
            .retain(|row| row.user != user || row.host != host);
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

    /// Go `MySQLPrivilege.RequestVerification` for a statement that names one
    /// table: the privilege is held when the account's *global*, *database*
    /// or *table* scope carries it.
    ///
    /// Go walks the same three scopes in that order and returns on the first
    /// that grants the privilege, so ORing the masks is the same answer. A
    /// wider scope subsuming a narrower one is the whole point: `GRANT SELECT
    /// ON *.*` satisfies a check on `db.t` without any `mysql.tables_priv`
    /// row.
    ///
    /// Column scope is deliberately not consulted: Go passes an empty column
    /// for a whole-table request, and a column-scope grant never satisfies
    /// one.
    #[must_use]
    pub fn has_table_priv(
        &self,
        user: &str,
        host: &str,
        database: &str,
        table: &str,
        global_priv: GlobalPriv,
    ) -> bool {
        if self.has_global_priv(user, host, global_priv) {
            return true;
        }
        let bit = global_priv.bit();
        // Schema and table names are stored as the `GRANT` (or the
        // `mysql.db` row) spelled them, while the statement spells them its
        // own way, so the scopes are matched case-insensitively rather than
        // by key -- which is how Go compares them too.
        let scoped = |row_user: &str, row_host: &str, row_database: &str| {
            row_user == user && row_host == host && row_database.eq_ignore_ascii_case(database)
        };
        if self
            .lock_db()
            .iter()
            .any(|((row_user, row_host, row_database), privs)| {
                scoped(row_user, row_host, row_database) && privs & bit != 0
            })
        {
            return true;
        }
        self.lock_table()
            .iter()
            .any(|((row_user, row_host, row_database, row_table), privs)| {
                scoped(row_user, row_host, row_database)
                    && row_table.eq_ignore_ascii_case(table)
                    && privs & bit != 0
            })
    }

    /// [`Self::has_table_priv`] over the account and every role it reaches,
    /// which is the identity list Go's `RequestVerification` walks.
    #[must_use]
    pub fn has_table_priv_with_roles(
        &self,
        user: &str,
        host: &str,
        active_roles: &[Account],
        database: &str,
        table: &str,
        global_priv: GlobalPriv,
    ) -> bool {
        self.identities_for_check(user, host, active_roles)
            .into_iter()
            .any(|(role_user, role_host)| {
                self.has_table_priv(&role_user, &role_host, database, table, global_priv)
            })
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

    /// Sets every bit in `mask` on the account's `(database, table, column)`
    /// row, appending the row if this is that column's first grant (Go's
    /// `checkAndInitColumnPriv` followed by `grantColumnLevel`).
    ///
    /// A `mask` of zero appends nothing: Go does insert an empty
    /// `mysql.Columns_priv` row for `GRANT USAGE (a)`, but `showGrants` skips
    /// a row with no privilege bit, so the row is unobservable (captured:
    /// `GRANT USAGE (a) ON cg.t` leaves `SHOW GRANTS` unchanged while
    /// `mysql.columns_priv` gains a row with an empty `Column_priv`).
    /// Not storing it removes the empty-row case instead of special-casing it
    /// everywhere it would otherwise have to be filtered out.
    pub fn grant_column(
        &self,
        user: &str,
        host: &str,
        database: &str,
        table: &str,
        column: &str,
        mask: u64,
    ) {
        if mask == 0 {
            return;
        }
        let mut rows = self.lock_column();
        if let Some(row) = rows.iter_mut().find(|row| {
            row.user == user
                && row.host == host
                && row.database == database
                && row.table == table
                && row.column == column
        }) {
            row.privs |= mask;
            return;
        }
        rows.push(ColumnPrivRecord {
            user: user.to_owned(),
            host: host.to_owned(),
            database: database.to_owned(),
            table: table.to_owned(),
            column: column.to_owned(),
            privs: mask,
        });
    }

    /// Clears every bit in `mask` on the account's `(database, table, column)`
    /// row, DELETING the row once no privilege is left -- Go's
    /// `revokeColumnPriv` issues a `DELETE` when the recomputed `Column_priv`
    /// set is empty rather than leaving a blank row behind. Revoking from a
    /// column that was never granted is a silent no-op; the "no such grant"
    /// error belongs to the TABLE row and is raised by the caller before this
    /// runs (captured: `REVOKE SELECT (a)` from an account with no grant on
    /// the table reports the table-level `ErrNonexistingGrant`, and does so
    /// even when the named column does not exist).
    pub fn revoke_column(
        &self,
        user: &str,
        host: &str,
        database: &str,
        table: &str,
        column: &str,
        mask: u64,
    ) {
        let mut rows = self.lock_column();
        rows.retain_mut(|row| {
            let matches = row.user == user
                && row.host == host
                && row.database == database
                && row.table == table
                && row.column == column;
            if !matches {
                return true;
            }
            row.privs &= !mask;
            row.privs != 0
        });
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
            .filter(|(_, privs)| *privs != 0)
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

        // An all-zero row prints NOTHING at DB and TABLE scope -- Go emits a
        // line only when the privilege list is non-empty, or (the USAGE
        // special case) when the only bit left is `GRANT OPTION`. Such rows
        // are ordinary: `REVOKE`ing the last privilege keeps the row, and a
        // column-only grant creates an empty TABLE row on the way past
        // (captured: after `REVOKE SELECT ON cg.*` plus `REVOKE SELECT ON
        // cg.t`, `SHOW GRANTS` reports the global USAGE line alone).
        //
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
            .filter(|(_, privs)| *privs != 0)
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

        // COLUMN-scope lines: one line per `db`.`table`, listing every
        // column privilege the account holds there. They form their own
        // block AFTER the table lines and are sorted lexically within it,
        // so a table carrying both table-level and column-level privileges
        // prints two separate lines (captured: `GRANT SELECT,UPDATE ON
        // `cg`.`t`` followed by ``GRANT INSERT(a) ON `cg`.`t``).
        //
        // Within a line the privileges follow `ALL_COLUMN_PRIVS` order and
        // each carries its own parenthesised column list in ROW order (Go
        // appends `record.ColumnName` while walking the account's
        // `columnsPriv` slice), joined with `", "`. There is no space
        // between the privilege name and its `(`.
        let mut column_groups: BTreeMap<(String, String), Vec<(String, u64)>> = BTreeMap::new();
        for row in self.lock_column().iter() {
            if owns(&row.user, &row.host) {
                column_groups
                    .entry((row.database.clone(), row.table.clone()))
                    .or_default()
                    .push((row.column.clone(), row.privs));
            }
        }
        let mut column_lines: Vec<String> = column_groups
            .into_iter()
            .filter_map(|((database, table), columns)| {
                let groups: Vec<String> = ALL_COLUMN_PRIVS
                    .iter()
                    .filter_map(|priv_| {
                        let named: Vec<&str> = columns
                            .iter()
                            .filter(|(_, privs)| privs & priv_.bit() != 0)
                            .map(|(column, _)| column.as_str())
                            .collect();
                        (!named.is_empty())
                            .then(|| format!("{}({})", priv_.print_name(), named.join(", ")))
                    })
                    .collect();
                (!groups.is_empty()).then(|| {
                    format!(
                        "GRANT {} ON `{database}`.`{table}` TO '{user}'@'{host}'",
                        groups.join(", ")
                    )
                })
            })
            .collect();
        column_lines.sort_unstable();

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

        let mut lines = Vec::with_capacity(
            2 + db_lines.len() + table_lines.len() + column_lines.len() + dynamic_lines.len(),
        );
        lines.push(global_line);
        lines.extend(db_lines);
        lines.extend(table_lines);
        lines.extend(column_lines);
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

    /// This whole account table, row by row -- the exact inverse of the
    /// [`Self::replace_from`] a cluster load publishes.
    ///
    /// A node whose registry is a *read* of somebody else's `mysql.*` needs
    /// this to write a change back: the statement handlers above have already
    /// validated and applied it here, and the persist step's whole job is
    /// making the stored rows say the same thing. Exporting the table rather
    /// than each statement's delta is what keeps the two halves from becoming
    /// two implementations of `GRANT`.
    ///
    /// Every row shape mirrors the `mysql.*` table it came from, so a caller
    /// can map it back one-to-one without re-deciding anything.
    #[must_use]
    pub fn export(&self) -> RegistryExport {
        let mut users: Vec<ExportedUser> = self
            .lock()
            .iter()
            .map(|((user, host), record)| ExportedUser {
                user: user.clone(),
                host: host.clone(),
                auth_string: record.auth_string.clone(),
                plugin: record.plugin.clone(),
                account_locked: record.is_role,
                password_expired: record.password_expired,
                privileges: printed_privileges(record.privs),
            })
            .collect();
        users.sort_by(|left, right| (&left.host, &left.user).cmp(&(&right.host, &right.user)));

        let mut db_grants: Vec<ExportedScopedGrant> = self
            .lock_db()
            .iter()
            .map(|((user, host, database), mask)| ExportedScopedGrant {
                user: user.clone(),
                host: host.clone(),
                database: database.clone(),
                table: String::new(),
                column: String::new(),
                privileges: printed_privileges(*mask),
            })
            .collect();
        db_grants.sort();

        let mut table_grants: Vec<ExportedScopedGrant> = self
            .lock_table()
            .iter()
            .map(
                |((user, host, database, table), mask)| ExportedScopedGrant {
                    user: user.clone(),
                    host: host.clone(),
                    database: database.clone(),
                    table: table.clone(),
                    column: String::new(),
                    privileges: printed_privileges(*mask),
                },
            )
            .collect();
        table_grants.sort();

        let mut column_grants: Vec<ExportedScopedGrant> = self
            .lock_column()
            .iter()
            .map(|record| ExportedScopedGrant {
                user: record.user.clone(),
                host: record.host.clone(),
                database: record.database.clone(),
                table: record.table.clone(),
                column: record.column.clone(),
                privileges: printed_privileges(record.privs),
            })
            .collect();
        column_grants.sort();

        let mut dynamic_grants: Vec<ExportedDynamicGrant> = self
            .lock_dynamic()
            .iter()
            .flat_map(|((user, host), privs)| {
                privs
                    .iter()
                    .map(|(name, grantable)| ExportedDynamicGrant {
                        user: user.clone(),
                        host: host.clone(),
                        privilege: name.clone(),
                        with_grant_option: *grantable,
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        dynamic_grants.sort();

        let mut role_edges: Vec<(Account, Account)> = self
            .lock_role_edges()
            .iter()
            .flat_map(|(grantee, roles)| {
                roles
                    .iter()
                    .map(|role| (role.clone(), grantee.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        role_edges.sort();

        let mut default_roles: Vec<(Account, Account)> = self
            .lock_default_roles()
            .iter()
            .flat_map(|(account, roles)| {
                roles
                    .iter()
                    .map(|role| (account.clone(), role.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        default_roles.sort();

        RegistryExport {
            users,
            db_grants,
            table_grants,
            column_grants,
            dynamic_grants,
            role_edges,
            default_roles,
        }
    }
}
