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

//! `mysql.user`'s password-locking and password-expiry columns, and the
//! clock they are evaluated against.
//!
//! Mirrors Go `pkg/privilege/privileges/cache.go`'s `UserRecord` password
//! locking/expiry members (the `User_attributes` JSON `Password_locking`
//! object) and `GenerateAccountAutoLockErr`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Go's `privileges.PasswordLocking`: one account's
/// `user_attributes -> '$.Password_locking'` object, policy and counter
/// together, because Go rewrites them as one JSON value on every update.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PasswordLocking {
    /// `FAILED_LOGIN_ATTEMPTS n`: consecutive wrong passwords that lock the
    /// account. Zero disables tracking.
    pub failed_login_attempts: i64,
    /// `PASSWORD_LOCK_TIME n` in days; `-1` is `UNBOUNDED` (captured), and
    /// zero disables tracking.
    pub password_lock_time_days: i64,
    /// Consecutive wrong passwords seen so far, reset to zero by a
    /// successful login or `ACCOUNT UNLOCK`.
    pub failed_login_count: i64,
    /// Whether the counter reached the limit and auto-locked the account.
    pub auto_account_locked: bool,
    /// When the auto-lock happened, in Unix seconds; `0` when it never has.
    pub auto_locked_last_changed: i64,
}

impl PasswordLocking {
    /// Go's `UserPrivileges.IsAccountAutoLockEnabled`: MySQL tracks failed
    /// logins only when BOTH options are nonzero
    /// (<https://dev.mysql.com/doc/refman/8.0/en/create-user.html>), so an
    /// account leaving either at zero authenticates with no counter at all --
    /// captured, `FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 0` reports the
    /// plain 1045 and writes no counter.
    #[must_use]
    pub const fn tracking_enabled(&self) -> bool {
        self.failed_login_attempts != 0 && self.password_lock_time_days != 0
    }

    /// The lock length Go interpolates into the 3955 message: `"unlimited"`
    /// for `PASSWORD_LOCK_TIME UNBOUNDED`, else the decimal day count.
    pub(super) fn lock_days_text(&self) -> String {
        if self.password_lock_time_days == -1 {
            "unlimited".to_owned()
        } else {
            self.password_lock_time_days.to_string()
        }
    }
}

/// Go's `mysql.user` password-expiry columns for one account, as
/// `SHOW CREATE USER` and the login path read them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PasswordExpiry {
    /// `Password_expired = 'Y'`.
    pub expired: bool,
    /// `Password_lifetime`; `None` is NULL / `DEFAULT`, `Some(0)` is `NEVER`,
    /// `Some(n)` is `INTERVAL n DAY`.
    pub lifetime: Option<i64>,
    /// `Password_last_changed`, in Unix seconds.
    pub last_changed: i64,
}

/// The `PASSWORD EXPIRE ...` policy a `CREATE`/`ALTER USER` clause writes.
/// Mirrors `tidb_ast::AlterUserPasswordExpire` without depending on it, which
/// keeps the account table a storage layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PasswordExpireSetting {
    /// `PASSWORD EXPIRE`: expire the password right now.
    Now,
    /// `PASSWORD EXPIRE DEFAULT`: defer to `default_password_lifetime`.
    Default,
    /// `PASSWORD EXPIRE NEVER`.
    Never,
    /// `PASSWORD EXPIRE INTERVAL n DAY`.
    Interval(i64),
}

/// Go's error 3955 (`ErUserAccessDeniedForUserAccountBlockedByPasswordLock`)
/// with its arguments already resolved, so every path that must report it
/// renders the identical sentence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountLockout {
    /// Account the login named, as Go prints it.
    pub user: String,
    /// Matched host pattern, as Go prints it.
    pub host: String,
    /// `FAILED_LOGIN_ATTEMPTS` of the account.
    pub failed_login_attempts: i64,
    /// Configured lock length, already rendered (`"unlimited"` or days).
    pub lock_days: String,
    /// Lock time still to run, already rendered (`"unlimited"` or days).
    pub remaining_days: String,
}

impl AccountLockout {
    /// Go `errno.ErUserAccessDeniedForUserAccountBlockedByPasswordLock`'s
    /// message template, captured verbatim from a locked login:
    /// `Access denied for user 'L1'@'%'. Account is blocked for 3 day(s) (3
    /// day(s) remaining) due to 2 consecutive failed logins.`
    #[must_use]
    pub fn message(&self) -> String {
        format!(
            "Access denied for user '{}'@'{}'. Account is blocked for {} day(s) ({} day(s) remaining) due to {} consecutive failed logins.",
            self.user, self.host, self.lock_days, self.remaining_days, self.failed_login_attempts
        )
    }
}

/// Go's error 1862 (`ErrMustChangePasswordLogin`): the account's password has
/// expired and the server is not in sandbox mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PasswordExpiredLogin;

impl PasswordExpiredLogin {
    /// Go `errno.ErrMustChangePasswordLogin`'s message, captured verbatim.
    #[must_use]
    pub const fn message(self) -> &'static str {
        "Your password has expired. To log in you must change it using a client that supports expired passwords."
    }
}

/// The wall clock every account-table timestamp is read from.
///
/// One representation and no modes: `now_unix()` is the system clock plus a
/// shared offset that starts at zero. A test that needs to be four days later
/// calls [`Clock::advance`]; nothing anywhere has to distinguish a "real"
/// clock from a "fake" one, so no code path can accidentally read an
/// untestable one. Cloning shares the offset, so the clock a
/// [`PrivilegeRegistry`] holds and the handle a test kept are one clock.
#[derive(Clone)]
pub struct Clock {
    offset_seconds: Arc<AtomicI64>,
}

impl Default for Clock {
    fn default() -> Self {
        Self {
            offset_seconds: Arc::new(AtomicI64::new(0)),
        }
    }
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Clock")
            .field(
                "offset_seconds",
                &self.offset_seconds.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl Clock {
    /// Seconds since the Unix epoch, as Go's `time.Now().Unix()` reports.
    #[must_use]
    pub fn now_unix(&self) -> i64 {
        let system = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| i64::try_from(elapsed.as_secs()).unwrap_or(0));
        system.saturating_add(self.offset_seconds.load(Ordering::Relaxed))
    }

    /// Moves this clock -- and every clone of it -- forward by `seconds`.
    /// Negative values move it back.
    pub fn advance(&self, seconds: i64) {
        self.offset_seconds.fetch_add(seconds, Ordering::Relaxed);
    }
}

/// Seconds in one day, the unit Go's `PASSWORD_LOCK_TIME` and
/// `PASSWORD EXPIRE INTERVAL` both count in.
pub(super) const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

/// Builds the 3955 report for one locked account. Go's
/// `GenerateAccountAutoLockErr` takes the two day counts as already-rendered
/// strings for exactly this reason: `UNBOUNDED` prints the word `unlimited`
/// in both slots, and no numeric type can carry that.
pub(super) fn lockout(
    user: &str,
    host: &str,
    locking: &PasswordLocking,
    remaining_days: String,
) -> AccountLockout {
    AccountLockout {
        user: user.to_owned(),
        host: host.to_owned(),
        failed_login_attempts: locking.failed_login_attempts,
        lock_days: locking.lock_days_text(),
        remaining_days,
    }
}
