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

// aggregate-test: standalone

#![allow(dead_code, missing_docs)]

#[path = "../src/auth_identity.rs"]
mod auth_identity;
#[path = "../src/configured_user_store.rs"]
mod configured_user_store;
#[path = "../src/native_password.rs"]
mod native_password;

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use configured_user_store::{ConfiguredUserStore, ConfiguredUserStoreError};
use sha1::{Digest, Sha1};

const ABC_HASH: &str = "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E";
const SOURCE_SALT: [u8; 20] = [
    85, 92, 45, 22, 58, 79, 107, 6, 122, 125, 58, 80, 12, 90, 103, 32, 90, 10, 74, 82,
];

#[test]
fn strict_file_load_authenticates_the_most_specific_canonical_host() {
    let file = AuthFile::new(&format!(
        "alice\t%\tmysql_native_password\t{ABC_HASH}\n\
         alice\tlocalhost\tmysql_native_password\t{ABC_HASH}\n\
         alice\t127.0.0.%\tmysql_native_password\t{ABC_HASH}\n"
    ));
    let store = ConfiguredUserStore::load(file.path()).expect("strict auth file");
    assert_eq!(store.len(), 3);
    assert!(!store.is_empty());

    let response = scramble(b"abc", &SOURCE_SALT);
    let loopback = store
        .authenticate_native("alice", "127.0.0.1", &SOURCE_SALT, &response)
        .expect("canonical loopback account");
    assert_eq!(loopback.username(), "alice");
    assert_eq!(loopback.host(), "localhost");
    assert_eq!(loopback.auth_plugin(), "mysql_native_password");
    assert_eq!(loopback.matched_identity().host(), "localhost");

    let wildcard = store
        .authenticate_native("alice", "192.0.2.9", &SOURCE_SALT, &response)
        .expect("fallback account");
    assert_eq!(wildcard.host(), "%");
}

#[test]
fn wrong_password_unknown_user_and_unknown_host_all_deny_authentication() {
    let store = ConfiguredUserStore::parse(&format!(
        "alice\t10.0.0.%\tmysql_native_password\t{ABC_HASH}\n"
    ))
    .expect("catalog");
    let correct = scramble(b"abc", &SOURCE_SALT);
    let wrong = scramble(b"wrong", &SOURCE_SALT);

    assert!(store
        .authenticate_native("alice", "10.0.0.7", &SOURCE_SALT, &correct)
        .is_ok());
    assert!(store
        .authenticate_native("alice", "10.0.0.7", &SOURCE_SALT, &wrong)
        .is_err());
    assert!(store
        .authenticate_native("unknown", "10.0.0.7", &SOURCE_SALT, &correct)
        .is_err());
    assert!(store
        .authenticate_native("alice", "192.0.2.7", &SOURCE_SALT, &correct)
        .is_err());
}

#[test]
fn parser_rejects_empty_malformed_unsupported_duplicate_and_invalid_hash_rows() {
    let cases = [
        ("", "contains no accounts"),
        ("\n", "record 1 is malformed"),
        ("alice\t%\tmysql_native_password\n", "record 1 is malformed"),
        (
            "\t%\tmysql_native_password\t*0000000000000000000000000000000000000000\n",
            "record 1 is malformed",
        ),
        (
            "alice\t\tmysql_native_password\t*0000000000000000000000000000000000000000\n",
            "record 1 is malformed",
        ),
        (
            "alice\t%\tcaching_sha2_password\t*0000000000000000000000000000000000000000\n",
            "record 1 uses an unsupported plugin",
        ),
        (
            "alice\t%\tmysql_native_password\t*not-a-hash\n",
            "record 1 has an invalid password hash",
        ),
        (
            "alice\t%\tmysql_native_password\t*0000000000000000000000000000000000000000\n\
             alice\t%\tmysql_native_password\t*1111111111111111111111111111111111111111\n",
            "record 2 duplicates an identity",
        ),
        (
            "alice\t%\tmysql_native_password\t*0000000000000000000000000000000000000000\textra\n",
            "record 1 is malformed",
        ),
    ];

    for (contents, message) in cases {
        let error = ConfiguredUserStore::parse(contents).expect_err("invalid catalog");
        assert_eq!(error.to_string(), format!("authentication file {message}"));
        assert!(!error.to_string().contains("0000000000"));
    }
}

#[test]
fn diagnostics_never_render_password_equivalent_material() {
    let store =
        ConfiguredUserStore::parse(&format!("alice\t%\tmysql_native_password\t{ABC_HASH}\n"))
            .expect("catalog");
    let rendered = format!("{store:?}");
    assert_eq!(rendered, "ConfiguredUserStore { account_count: 1, .. }");
    assert!(!rendered.contains(ABC_HASH));

    let invalid = format!("alice\t%\tmysql_native_password\t{ABC_HASH}Z\n");
    let error = ConfiguredUserStore::parse(&invalid).expect_err("invalid hash");
    assert!(!format!("{error:?}").contains(ABC_HASH));
    assert!(!error.to_string().contains(ABC_HASH));
}

#[cfg(unix)]
#[test]
fn file_permissions_must_be_exactly_0600() {
    use std::os::unix::fs::PermissionsExt;

    let file = AuthFile::new(&format!("alice\t%\tmysql_native_password\t{ABC_HASH}\n"));
    fs::set_permissions(file.path(), fs::Permissions::from_mode(0o640)).expect("chmod");
    assert!(matches!(
        ConfiguredUserStore::load(file.path()),
        Err(ConfiguredUserStoreError::InvalidPermissions)
    ));

    fs::set_permissions(file.path(), fs::Permissions::from_mode(0o1600)).expect("chmod");
    assert!(matches!(
        ConfiguredUserStore::load(file.path()),
        Err(ConfiguredUserStoreError::InvalidPermissions)
    ));

    fs::set_permissions(file.path(), fs::Permissions::from_mode(0o600)).expect("chmod");
    ConfiguredUserStore::load(file.path()).expect("exact mode");
}

fn scramble(password: &[u8], salt: &[u8]) -> [u8; 20] {
    let stage_one = Sha1::digest(password);
    let stage_two = Sha1::digest(stage_one);
    let mut hasher = Sha1::new();
    hasher.update(salt);
    hasher.update(stage_two);
    let challenge = hasher.finalize();
    let mut response = [0; 20];
    for ((destination, stage_one), challenge) in response
        .iter_mut()
        .zip(stage_one.iter())
        .zip(challenge.iter())
    {
        *destination = stage_one ^ challenge;
    }
    response
}

struct AuthFile {
    path: PathBuf,
}

impl AuthFile {
    fn new(contents: &str) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "tidb-rust-auth-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path).expect("create auth file");
        file.write_all(contents.as_bytes())
            .expect("write auth file");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for AuthFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// One `FAILED_LOGIN_ATTEMPTS n PASSWORD_LOCK_TIME d` account, plus the
/// scrambles for a right and a wrong password.
fn lockout_store(attempts: i64, lock_days: i64) -> (ConfiguredUserStore, [u8; 20], [u8; 20]) {
    let file = AuthFile::new(&format!("bob\t%\tmysql_native_password\t{ABC_HASH}\n"));
    let store = ConfiguredUserStore::load(file.path()).expect("strict auth file");
    store
        .accounts()
        .set_password_locking_options("bob", "%", Some(attempts), Some(lock_days));
    (
        store,
        scramble(b"abc", &SOURCE_SALT),
        scramble(b"nope", &SOURCE_SALT),
    )
}

/// Go's captured lockout sequence on a `FAILED_LOGIN_ATTEMPTS 2
/// PASSWORD_LOCK_TIME 3` account: the FIRST wrong password reports the plain
/// 1045 and leaves the counter at 1, the SECOND locks the account and reports
/// 3955 with the full lock time remaining, every later attempt -- including
/// one carrying the RIGHT password -- reports the same 3955, and
/// `ACCOUNT UNLOCK` clears both the lock and the counter.
#[test]
fn consecutive_wrong_passwords_auto_lock_the_account_and_unlock_clears_it() {
    let (store, right, wrong) = lockout_store(2, 3);
    let accounts = store.accounts();
    let attempt = |response: &[u8; 20]| {
        store
            .authenticate_native("bob", "127.0.0.1", &SOURCE_SALT, response)
            .map(|identity| identity.username().to_owned())
    };

    assert_eq!(
        attempt(&wrong),
        Err(configured_user_store::AuthenticationFailure::AccessDenied)
    );
    let locking = accounts.password_locking("bob", "%").expect("counter");
    assert_eq!(locking.failed_login_count, 1);
    assert!(!locking.auto_account_locked);

    let expected = "Access denied for user 'bob'@'%'. Account is blocked for 3 day(s) \
                    (3 day(s) remaining) due to 2 consecutive failed logins.";
    for response in [&wrong, &wrong, &right] {
        match attempt(response) {
            Err(configured_user_store::AuthenticationFailure::AutoLocked(lockout)) => {
                assert_eq!(lockout.message(), expected);
            }
            other => panic!("expected 3955, got {other:?}"),
        }
    }
    let locking = accounts.password_locking("bob", "%").expect("counter");
    // The counter stops at the limit: attempts made while locked never reach
    // the increment (captured -- Go reports count 2 after four failures).
    assert_eq!(locking.failed_login_count, 2);
    assert!(locking.auto_account_locked);

    accounts.set_locked("bob", "%", false);
    let locking = accounts.password_locking("bob", "%").expect("counter");
    assert_eq!(locking.failed_login_count, 0);
    assert!(!locking.auto_account_locked);
    assert_eq!(attempt(&right), Ok("bob".to_owned()));
}

/// `PASSWORD_LOCK_TIME UNBOUNDED` prints the word `unlimited` in both day
/// slots of the 3955 message, which is why Go passes them as strings
/// (captured from `TestFailedLoginTracking`).
#[test]
fn an_unbounded_lock_time_reports_unlimited_in_both_day_slots() {
    let (store, _right, wrong) = lockout_store(1, -1);
    match store.authenticate_native("bob", "127.0.0.1", &SOURCE_SALT, &wrong) {
        Err(configured_user_store::AuthenticationFailure::AutoLocked(lockout)) => assert_eq!(
            lockout.message(),
            "Access denied for user 'bob'@'%'. Account is blocked for unlimited day(s) \
             (unlimited day(s) remaining) due to 1 consecutive failed logins."
        ),
        other => panic!("expected 3955, got {other:?}"),
    }
}

/// Go tracks failed logins only when BOTH options are nonzero, so an account
/// with either at zero just reports the ordinary 1045 forever and writes no
/// counter (captured: `testu3`, `testu4`, `testu5`).
#[test]
fn a_zero_in_either_option_disables_tracking_entirely() {
    for (attempts, lock_days) in [(0, -1), (1, 0), (0, 0)] {
        let (store, _right, wrong) = lockout_store(attempts, lock_days);
        for _ in 0..3 {
            assert_eq!(
                store.authenticate_native("bob", "127.0.0.1", &SOURCE_SALT, &wrong),
                Err(configured_user_store::AuthenticationFailure::AccessDenied),
                "{attempts}/{lock_days}"
            );
        }
        assert_eq!(
            store
                .accounts()
                .password_locking("bob", "%")
                .map(|locking| locking.failed_login_count),
            (attempts != 0 || lock_days != 0).then_some(0),
            "{attempts}/{lock_days}"
        );
    }
}

/// Go auto-unlocks an account whose `PASSWORD_LOCK_TIME` window has run out
/// (`verifyAccountAutoLock`), and the remaining-day count it reports before
/// that is `ceil(lockTime - elapsed/86400)`.
#[test]
fn the_lock_expires_on_its_own_and_the_remaining_days_count_down() {
    let (store, right, wrong) = lockout_store(1, 3);
    let clock = store.accounts().clock();
    assert!(store
        .authenticate_native("bob", "127.0.0.1", &SOURCE_SALT, &wrong)
        .is_err());

    for (advance_days, remaining) in [(1, "2"), (1, "1")] {
        clock.advance(advance_days * 24 * 60 * 60);
        match store.authenticate_native("bob", "127.0.0.1", &SOURCE_SALT, &right) {
            Err(configured_user_store::AuthenticationFailure::AutoLocked(lockout)) => {
                assert_eq!(lockout.remaining_days, remaining);
            }
            other => panic!("expected 3955, got {other:?}"),
        }
    }
    // Past the window the account unlocks itself and the counter resets, so
    // the very next correct password gets in.
    clock.advance(2 * 24 * 60 * 60);
    assert!(store
        .authenticate_native("bob", "127.0.0.1", &SOURCE_SALT, &right)
        .is_ok());
    let locking = store
        .accounts()
        .password_locking("bob", "%")
        .expect("counter");
    assert!(!locking.auto_account_locked);
    assert_eq!(locking.failed_login_count, 0);
}

/// A correct password resets the counter, so failures have to be CONSECUTIVE
/// to lock an account (Go's `authSuccessClearCount`).
#[test]
fn a_successful_login_clears_the_failure_counter() {
    let (store, right, wrong) = lockout_store(3, 1);
    let accounts = store.accounts();
    assert!(store
        .authenticate_native("bob", "127.0.0.1", &SOURCE_SALT, &wrong)
        .is_err());
    assert_eq!(
        accounts
            .password_locking("bob", "%")
            .unwrap()
            .failed_login_count,
        1
    );
    assert!(store
        .authenticate_native("bob", "127.0.0.1", &SOURCE_SALT, &right)
        .is_ok());
    assert_eq!(
        accounts
            .password_locking("bob", "%")
            .unwrap()
            .failed_login_count,
        0
    );
}

/// `PASSWORD EXPIRE` refuses the login with 1862 while the server disconnects
/// expired passwords (Go's default), and admits it into a SANDBOX session
/// once sandbox mode is on -- both captured. An interval that has not yet
/// elapsed changes nothing.
#[test]
fn an_expired_password_reports_1862_or_opens_a_sandbox_session() {
    use tidb_session::privilege::PasswordExpireSetting;

    let file = AuthFile::new(&format!("bob\t%\tmysql_native_password\t{ABC_HASH}\n"));
    let store = ConfiguredUserStore::load(file.path()).expect("strict auth file");
    let accounts = store.accounts();
    let response = scramble(b"abc", &SOURCE_SALT);
    let login = || store.authenticate_native("bob", "127.0.0.1", &SOURCE_SALT, &response);

    assert!(!login().expect("unexpired login").in_sandbox_mode());

    accounts.set_password_expire("bob", "%", PasswordExpireSetting::Now);
    assert_eq!(
        login(),
        Err(configured_user_store::AuthenticationFailure::PasswordExpired)
    );
    accounts.set_sandbox_mode_enabled(true);
    assert!(login().expect("sandboxed login").in_sandbox_mode());
    accounts.set_sandbox_mode_enabled(false);

    // Storing a password unexpires the account.
    accounts.mark_password_changed("bob", "%");
    assert!(!login().expect("unexpired login").in_sandbox_mode());

    // An INTERVAL only bites once it has elapsed.
    accounts.set_password_expire("bob", "%", PasswordExpireSetting::Interval(2));
    assert!(login().is_ok());
    accounts.clock().advance(3 * 24 * 60 * 60);
    assert_eq!(
        login(),
        Err(configured_user_store::AuthenticationFailure::PasswordExpired)
    );

    // `PASSWORD EXPIRE NEVER` opts the account out for good, and so does the
    // NULL lifetime this tier always resolves against an unset
    // `default_password_lifetime`.
    accounts.set_password_expire("bob", "%", PasswordExpireSetting::Never);
    assert!(login().is_ok());
    accounts.set_password_expire("bob", "%", PasswordExpireSetting::Default);
    assert!(login().is_ok());
}

/// `default_password_lifetime` end to end: a `PASSWORD EXPIRE DEFAULT`
/// account (the one this store's login path resolves a `NULL`
/// `Password_lifetime` against) never ages out while the global stays at
/// its factory-default `0` -- and does, once `SET GLOBAL
/// default_password_lifetime` sets a nonzero value and enough time has
/// actually elapsed. `store.global_vars()` is the same table
/// `PipelineSessionFactory::with_accounts_and_globals` would share with the
/// SQL executor's `SET GLOBAL`, so writing through it here is exactly what
/// a real `SET GLOBAL default_password_lifetime = 1` on that shared
/// executor would do to this store's next login.
#[test]
fn default_password_lifetime_ages_out_a_password_expire_default_account() {
    use tidb_session::privilege::PasswordExpireSetting;

    let file = AuthFile::new(&format!("bob\t%\tmysql_native_password\t{ABC_HASH}\n"));
    let store = ConfiguredUserStore::load(file.path()).expect("strict auth file");
    let accounts = store.accounts();
    let response = scramble(b"abc", &SOURCE_SALT);
    let login = || store.authenticate_native("bob", "127.0.0.1", &SOURCE_SALT, &response);

    accounts.set_password_expire("bob", "%", PasswordExpireSetting::Default);
    // Nobody has run `SET GLOBAL default_password_lifetime` yet: the account
    // never ages out, matching a cluster fresh out of the box.
    assert!(login().is_ok());

    // `SET GLOBAL default_password_lifetime = 1` (one day).
    store
        .global_vars()
        .set("default_password_lifetime", "1".to_owned())
        .expect("default_password_lifetime accepts a small positive day count");
    // Not enough time has passed yet.
    assert!(login().is_ok());

    accounts.clock().advance(2 * 24 * 60 * 60);
    assert_eq!(
        login(),
        Err(configured_user_store::AuthenticationFailure::PasswordExpired)
    );

    // Restoring the global to its default (`SET GLOBAL
    // default_password_lifetime = DEFAULT`) stops the aging again.
    store
        .global_vars()
        .reset("default_password_lifetime")
        .unwrap();
    assert!(login().is_ok());
}
