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

//! Ports of Go `pkg/executor/test/passwordtest` (batch items 1051–1080).
//!
//! Two surfaces split the family. The password VALIDATION policy engine is
//! a dependency leaf this crate already owns transitively
//! (`tidb-util/src/password_validation.rs`, the transcreation of Go
//! `pkg/util/password-validation`), so `TestValidatePassword`'s policy
//! matrix ports as a RUNNING test driven through the same global-var
//! accessor seam Go's `variable.ValidatePassword` reads. The account
//! statements (`CREATE/ALTER/SET PASSWORD/DROP/RENAME USER`, `SHOW CREATE
//! USER`, session `Auth`, the `mysql.user` table) execute in Go's
//! `executor/simple.go`; the Rust implementation of those statements lives
//! in `tidb-session` (which depends on this crate, so it is unreachable
//! here), where `tidb-session::tests_grants::dual_password` and
//! `tidb-session::tests_grants::password_policy` already pin the same Go
//! corpus. Those Go tests therefore carry per-test gap rows below.

use tidb_util::password_validation::{
    validate_password, GlobalVarAccessor, PasswordUser, PwdError, VALIDATE_PASSWORD_CHECK_USER_NAME,
    VALIDATE_PASSWORD_DICTIONARY, VALIDATE_PASSWORD_ENABLE, VALIDATE_PASSWORD_LENGTH,
    VALIDATE_PASSWORD_MIXED_CASE_COUNT, VALIDATE_PASSWORD_NUMBER_COUNT,
    VALIDATE_PASSWORD_POLICY, VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT,
};

/// The `validate_password.*` globals Go's suite sets through
/// `SET GLOBAL`, keyed exactly as the accessor reads them.
#[derive(Default, Clone)]
struct Vars {
    values: std::collections::BTreeMap<String, String>,
}

impl Vars {
    fn set(&mut self, name: &str, value: &str) -> &mut Self {
        self.values.insert(name.to_owned(), value.to_owned());
        self
    }
}

impl GlobalVarAccessor for Vars {
    type Error = std::convert::Infallible;

    fn get_global_sys_var(&self, name: &str) -> Result<String, Self::Error> {
        Ok(self.values.get(name).cloned().unwrap_or_default())
    }
}

fn err_text<E: std::fmt::Display>(error: &PwdError<E>) -> String {
    match error {
        PwdError::NotValid(warning) => format!(
            "Your password does not satisfy the current policy requirements ({warning})"
        ),
        other => other.to_string(),
    }
}

/// The suite's caller is the authenticated root session; Go's
/// `ValidateUserNameInPassword` checks the password against the CURRENT
/// user (root), never the ALTERed account -- which is why `SET PASSWORD FOR
/// 'testuser' = 'testuser'` succeeds while `!Abcdroot1234` is rejected.
fn check_as_root(vars: &Vars, password: &str) -> Result<(), String> {
    validate_password(
        password,
        Some(PasswordUser {
            username: "root",
            auth_username: "",
        }),
        vars,
    )
    .map_err(|error| err_text(&error))
}

/// Go `pkg/executor/test/passwordtest/password_management_test.go:39
/// ::TestValidatePassword`'s policy matrix, executed per auth plugin in Go
/// (the plugin only decides the hash storage, never the validation): the
/// user-name and reversed user-name checks, `check_user_name = 0` disabling
/// both, LOW length messages, the MEDIUM per-class count messages with
/// `special_char_count = 0` acceptance, and the STRONG dictionary word
/// rejection. Messages are Go's exact `MustContainErrMsg` needles; the
/// `NotValid` wrapper text is `variable.ErrNotValidPassword`'s format.
#[test]
fn validate_password_policy_matrix_over_the_shared_validator() {
    let mut vars = Vars::default();
    vars.set(VALIDATE_PASSWORD_ENABLE, "1")
        .set(VALIDATE_PASSWORD_CHECK_USER_NAME, "1")
        .set(VALIDATE_PASSWORD_POLICY, "LOW")
        .set(VALIDATE_PASSWORD_LENGTH, "8")
        .set(VALIDATE_PASSWORD_MIXED_CASE_COUNT, "1")
        .set(VALIDATE_PASSWORD_NUMBER_COUNT, "1")
        .set(VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT, "1")
        .set(VALIDATE_PASSWORD_DICTIONARY, "");

    // check user name: contains, and byte-reversed ("toor") -- the CURRENT
    // user is root.
    assert_eq!(
        check_as_root(&vars, "!Abcdroot1234").unwrap_err(),
        "Your password does not satisfy the current policy requirements (Password Contains User Name)"
    );
    assert_eq!(
        check_as_root(&vars, "!Abcdtoor1234").unwrap_err(),
        "Your password does not satisfy the current policy requirements (Password Contains Reversed User Name)"
    );
    // A password equal to the ALTERed account's own name passes, because
    // Go checks the CALLING user (root), not the target account:
    // `SET PASSWORD FOR 'testuser' = 'testuser'` and
    // `ALTER USER testuser IDENTIFIED BY 'testuser'` both succeed.
    assert!(check_as_root(&vars, "testuser").is_ok());

    vars.set(VALIDATE_PASSWORD_CHECK_USER_NAME, "0");
    assert!(check_as_root(&vars, "!Abcdroot1234").is_ok());
    assert!(check_as_root(&vars, "!Abcdtoor1234").is_ok());
    vars.set(VALIDATE_PASSWORD_CHECK_USER_NAME, "1");

    // LOW: length only.
    assert_eq!(
        check_as_root(&vars, "1234567").unwrap_err(),
        "Your password does not satisfy the current policy requirements (Require Password Length: 8)"
    );
    vars.set(VALIDATE_PASSWORD_LENGTH, "12");
    assert_eq!(
        check_as_root(&vars, "!Abcdefg123").unwrap_err(),
        "Your password does not satisfy the current policy requirements (Require Password Length: 12)"
    );
    assert!(check_as_root(&vars, "!Abcdefg1234").is_ok());
    vars.set(VALIDATE_PASSWORD_LENGTH, "8");

    // MEDIUM: per-class counts.
    vars.set(VALIDATE_PASSWORD_POLICY, "MEDIUM");
    assert!(check_as_root(&vars, "!Abc1234567").is_ok());
    assert_eq!(
        check_as_root(&vars, "!ABC1234567").unwrap_err(),
        "Your password does not satisfy the current policy requirements (Require Password Lowercase Count: 1)"
    );
    assert_eq!(
        check_as_root(&vars, "!abc1234567").unwrap_err(),
        "Your password does not satisfy the current policy requirements (Require Password Uppercase Count: 1)"
    );
    assert_eq!(
        check_as_root(&vars, "!ABCDabcd").unwrap_err(),
        "Your password does not satisfy the current policy requirements (Require Password Digit Count: 1)"
    );
    assert_eq!(
        check_as_root(&vars, "Abc1234567").unwrap_err(),
        "Your password does not satisfy the current policy requirements (Require Password Non-alphanumeric Count: 1)"
    );
    vars.set(VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT, "0");
    assert!(check_as_root(&vars, "Abc1234567").is_ok());
    vars.set(VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT, "1");

    // STRONG: adds the dictionary check.
    vars.set(VALIDATE_PASSWORD_POLICY, "STRONG")
        .set(VALIDATE_PASSWORD_DICTIONARY, "1234;5678");
    assert!(check_as_root(&vars, "!Abc123567").is_ok());
    assert!(check_as_root(&vars, "!Abc43218765").is_ok());
    assert_eq!(
        check_as_root(&vars, "!Abc1234567").unwrap_err(),
        "Your password does not satisfy the current policy requirements (Password contains word in the dictionary)"
    );
    vars.set(VALIDATE_PASSWORD_DICTIONARY, "");
    assert!(check_as_root(&vars, "!Abc1234567").is_ok());

    // With every count at 0 even the empty password passes the policy
    // (Go: ALTER USER ''@'localhost' IDENTIFIED BY '' succeeds).
    let mut zeroed = Vars::default();
    zeroed
        .set(VALIDATE_PASSWORD_ENABLE, "1")
        .set(VALIDATE_PASSWORD_CHECK_USER_NAME, "1")
        .set(VALIDATE_PASSWORD_POLICY, "LOW")
        .set(VALIDATE_PASSWORD_LENGTH, "0")
        .set(VALIDATE_PASSWORD_MIXED_CASE_COUNT, "0")
        .set(VALIDATE_PASSWORD_NUMBER_COUNT, "0")
        .set(VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT, "0");
    let empty_user = validate_password(
        "",
        Some(PasswordUser {
            username: "",
            auth_username: "",
        }),
        &zeroed,
    );
    assert!(empty_user.is_ok(), "empty user name skips the name check");
}

/// Go `password_management_test.go:39::TestValidatePassword`'s statement
/// arms: `SET GLOBAL validate_password.*` round-trips (`SELECT
/// @@global...`), the sysvar's set-time minimum clamp (`SET ... length = 3`
/// reads back 4), the auth-plugin loop (`IDENTIFIED WITH <plugin> BY ...`),
/// the `CREATE USER`/`SET PASSWORD FOR`/`ALTER USER` call sites (1819
/// `ErrNotValidPassword`), the `IDENTIFIED WITH ... AS ''` bypass and the
/// `CREATE ROLE` bypass. Those statements execute above this tier.
#[test]
#[ignore = "go-parity-gap: CREATE/ALTER/SET PASSWORD statements and the sysvar set-time length clamp live in tidb-session (pinned there by tests_grants/password_policy::validate_password_enforces_account_writes_and_scores_sql_values)"]
fn validate_password_statement_surface_and_length_clamp() {}

/// Go `pkg/executor/test/passwordtest/dual_password_test.go:70
/// ::TestDualPasswordRetainAndDiscard`: `ALTER USER ... IDENTIFIED BY ...
/// RETAIN CURRENT PASSWORD` keeps the old hash authenticating and stores it
/// in `user_attributes.$.additional_password`; `DISCARD OLD PASSWORD`
/// removes it (old password stops authenticating, JSON path reads NULL).
/// Pinned in-workspace by
/// `tidb-session::tests_grants::dual_password::retain_and_discard_maintain_the_additional_password_attribute`.
#[test]
#[ignore = "go-parity-gap: account statements execute in tidb-session (unreachable from this crate); pinned by tidb-session::tests_grants::dual_password"]
fn dual_password_retain_and_discard() {}

/// Go `dual_password_test.go:99::TestDualPasswordSetPasswordRetain`: `SET
/// PASSWORD FOR ... = ... RETAIN CURRENT PASSWORD` rotates through the
/// SET-password call site with the same secondary promotion, plus its
/// `DISCARD OLD PASSWORD` tail. Pinned by
/// `tidb-session::tests_grants::dual_password::set_password_retain_and_per_spec_dual_clauses`.
#[test]
#[ignore = "go-parity-gap: SET PASSWORD executes in tidb-session; pinned by tests_grants::dual_password::set_password_retain_and_per_spec_dual_clauses"]
fn dual_password_set_password_retain() {}

/// Go `dual_password_test.go:154::TestDualPasswordCreateUserRejectsRetain`:
/// `CREATE USER ... IDENTIFIED BY ... RETAIN CURRENT PASSWORD` is rejected
/// (nothing to retain at creation). Pinned by
/// `tidb-session::tests_grants::dual_password::retain_validation_errors_match_go`.
#[test]
#[ignore = "go-parity-gap: CREATE USER executes in tidb-session; pinned by tests_grants::dual_password::retain_validation_errors_match_go"]
fn dual_password_create_user_rejects_retain() {}

/// Go `dual_password_test.go:169::TestDualPasswordRejectsEmptyNew`:
/// `ALTER USER ... IDENTIFIED BY '' RETAIN CURRENT PASSWORD` is rejected
/// (Go `ErrCurrentPasswordCannotBeRetained`, 3895). Pinned by
/// `tidb-session::tests_grants::dual_password::retain_validation_errors_match_go`.
#[test]
#[ignore = "go-parity-gap: ALTER USER executes in tidb-session; pinned by tests_grants::dual_password::retain_validation_errors_match_go"]
fn dual_password_rejects_empty_new_password_with_retain() {}

/// Go `dual_password_test.go:179::TestDualPasswordRejectsPluginChange`:
/// combining RETAIN CURRENT PASSWORD with an `IDENTIFIED WITH` plugin change
/// is rejected (Go `ErrPasswordCannotBeRetainedOnPluginChange`, 3894).
#[test]
#[ignore = "go-parity-gap: ALTER USER plugin-change arms execute in tidb-session"]
fn dual_password_rejects_plugin_change_with_retain() {}

/// Go `dual_password_test.go:195::TestDualPasswordLegacyEmptyPluginAcceptsNative`:
/// a legacy `mysql.user` row with empty `plugin` resolves to
/// `mysql_native_password`, so dual-password clauses are accepted and the
/// secondary is stored with the native format.
#[test]
#[ignore = "go-parity-gap: legacy plugin resolution runs in tidb-session's account statements"]
fn dual_password_legacy_empty_plugin_accepts_native() {}

/// Go `dual_password_test.go:221::TestDualPasswordLegacyEmptyPluginHonorsDefaultPlugin`:
/// with `@@global.default_authentication_plugin = caching_sha2_password`, a
/// legacy empty-plugin row resolves to that plugin and RETAIN stores the
/// secondary in its format.
#[test]
#[ignore = "go-parity-gap: @@global.default_authentication_plugin resolution runs in tidb-session"]
fn dual_password_legacy_empty_plugin_honors_default_plugin() {}

/// Go `dual_password_test.go:241::TestDualPasswordPluginChangeSilentlyDiscardsSecondary`:
/// changing the plugin without RETAIN silently drops an existing secondary
/// (MySQL 8.0 scenario 9/14).
#[test]
#[ignore = "go-parity-gap: ALTER USER plugin-change arms execute in tidb-session"]
fn dual_password_plugin_change_silently_discards_secondary() {}

/// Go `dual_password_test.go:256::TestDualPasswordCrossUserRequiresCreateUser`:
/// the privilege model — `CREATE USER` privilege is required to run
/// dual-password clauses for ANOTHER user, while the account itself can
/// self-serve with `APPLICATION_PASSWORD_ADMIN`.
#[test]
#[ignore = "go-parity-gap: privilege-gated account statements execute in tidb-session"]
fn dual_password_cross_user_requires_create_user() {}

/// Go `dual_password_test.go:304::TestDualPasswordRejectsEmptyPrimary`:
/// RETAIN on an account whose CURRENT primary password is empty is rejected
/// (Go `ErrSecondPasswordCannotBeEmpty`, 3878 — nothing to retain).
#[test]
#[ignore = "go-parity-gap: ALTER USER executes in tidb-session"]
fn dual_password_rejects_retaining_an_empty_primary() {}

/// Go `dual_password_test.go:315::TestDualPasswordShowCreateUserHidesSecondary`:
/// `SHOW CREATE USER` never prints the secondary hash (redaction of
/// `additional_password`).
#[test]
#[ignore = "go-parity-gap: SHOW CREATE USER executes in tidb-session"]
fn dual_password_show_create_user_hides_secondary() {}

/// Go `dual_password_test.go:333::TestDualPasswordSetPasswordSelfByExplicitName`:
/// a non-root user with `UPDATE` on `mysql.user` may `SET PASSWORD FOR
/// <self> ... RETAIN` — the self-service path resolves the account by
/// explicit name.
#[test]
#[ignore = "go-parity-gap: privilege-gated SET PASSWORD executes in tidb-session"]
fn dual_password_set_password_self_by_explicit_name() {}

/// Go `dual_password_test.go:370::TestDualPasswordCachingSha2PasswordStorage`:
/// retained secondaries keep each plugin's own hash format
/// (`$A$005$...` for caching_sha2, SM3 prefix for tidb_sm3), not re-hashed
/// copies.
#[test]
#[ignore = "go-parity-gap: per-plugin hash storage on ALTER USER executes in tidb-session"]
fn dual_password_caching_sha2_storage_format() {}

/// Go `dual_password_test.go:417::TestDualPasswordChainedRetain`: two
/// successive RETAIN rotations keep only the IMMEDIATELY previous hash as
/// the secondary (the first secondary is overwritten, MySQL 8.0 scenario
/// 1/2).
#[test]
#[ignore = "go-parity-gap: ALTER USER executes in tidb-session"]
fn dual_password_chained_retain_keeps_only_the_previous_hash() {}

/// Go `dual_password_test.go:442::TestDualPasswordAlterWithoutRetainPreservesSecondary`:
/// a plain `ALTER USER ... IDENTIFIED BY ...` (no RETAIN) leaves an existing
/// secondary untouched.
#[test]
#[ignore = "go-parity-gap: ALTER USER executes in tidb-session"]
fn dual_password_alter_without_retain_preserves_secondary() {}

/// Go `dual_password_test.go:465::TestDualPasswordRenameUserPreservesSecondary`:
/// `RENAME USER` carries the secondary hash to the new name (MySQL 8.0
/// scenario 4).
#[test]
#[ignore = "go-parity-gap: RENAME USER executes in tidb-session"]
fn dual_password_rename_user_preserves_secondary() {}

/// Go `dual_password_test.go:486::TestDualPasswordDropUserRemovesSecondary`:
/// `DROP USER` removes the row and with it the secondary (MySQL 8.0
/// scenario 5).
#[test]
#[ignore = "go-parity-gap: DROP USER executes in tidb-session"]
fn dual_password_drop_user_removes_secondary() {}

/// Go `dual_password_test.go:505::TestDualPasswordMultiUserAlter`: one
/// `ALTER USER IF EXISTS u1 ..., u2 ..., u3 ... IDENTIFIED BY ... RETAIN`
/// statement applies RETAIN only to the LAST account (Go pins that the
/// clause scopes per-statement, not per-user list).
#[test]
#[ignore = "go-parity-gap: multi-account ALTER USER executes in tidb-session"]
fn dual_password_multi_user_alter_scopes_retain_to_the_last_account() {}

/// Go `dual_password_test.go:566::TestDualPasswordSelfServiceDiscardWithExtraOptionsStillGated`:
/// self-service `DISCARD OLD PASSWORD` requires
/// `APPLICATION_PASSWORD_ADMIN` even when bundled with otherwise-permitted
/// options.
#[test]
#[ignore = "go-parity-gap: privilege-gated ALTER USER executes in tidb-session"]
fn dual_password_self_service_discard_with_extra_options_still_gated() {}

/// Go `dual_password_test.go:632::TestDualPasswordLegacyEmptyPluginRejectsLDAPDefault`:
/// with `default_authentication_plugin = authentication_ldap_simple`, a
/// legacy empty-plugin account resolves to the LDAP plugin and dual-password
/// clauses are refused (`Dual password is not supported for users with
/// plugin ...`).
#[test]
#[ignore = "go-parity-gap: default-plugin resolution and the plugin-capability gate execute in tidb-session"]
fn dual_password_legacy_empty_plugin_rejects_ldap_default() {}

/// Go `dual_password_test.go:660::TestDualPasswordSecondaryLoginWithEmptyPrimary`:
/// after RETAIN, clearing the primary (hash set to '') leaves the secondary
/// authenticating; the empty-primary login form also resolves correctly.
#[test]
#[ignore = "go-parity-gap: session Auth against mysql.user executes in tidb-session"]
fn dual_password_secondary_login_with_empty_primary() {}

/// Go `dual_password_test.go:689::TestDualPasswordDiscardNoopOnIncapablePlugin`:
/// `DISCARD OLD PASSWORD` on an LDAP account (no password capability)
/// succeeds and changes nothing.
#[test]
#[ignore = "go-parity-gap: ALTER USER executes in tidb-session"]
fn dual_password_discard_is_noop_on_incapable_plugin() {}

/// Go `dual_password_test.go:702::TestDualPasswordDiscardCollapsesEmptyAttributesToNull`:
/// DISCARD with no secondary leaves `user_attributes` NULL, not the literal
/// `'{}'`. Pinned by
/// `tidb-session::tests_grants::dual_password::retain_and_discard_maintain_the_additional_password_attribute`
/// (the `user_attributes is null` arm).
#[test]
#[ignore = "go-parity-gap: ALTER USER executes in tidb-session; pinned by tests_grants::dual_password::retain_and_discard_maintain_the_additional_password_attribute"]
fn dual_password_discard_collapses_empty_attributes_to_null() {}

/// Go `dual_password_test.go:734::TestDualPasswordSelfSetPasswordRetainAcceptsMysqlUpdate`:
/// a self-session holding `UPDATE ON mysql.*` may `SET PASSWORD ... RETAIN`
/// (MySQL 8.0 scenario 3's privilege variant).
#[test]
#[ignore = "go-parity-gap: privilege-gated SET PASSWORD executes in tidb-session"]
fn dual_password_self_set_password_retain_accepts_mysql_update() {}

/// Go `dual_password_test.go:753::TestDualPasswordAlterUserUserResolvesAuthUsername`
/// (issue 68937): `ALTER USER USER() IDENTIFIED BY ...` resolves USER()
/// through the AUTHENTICATED username (AuthUsername), not the login name, so
/// the password lands on the matched account.
#[test]
#[ignore = "go-parity-gap: USER() resolution in ALTER USER executes in tidb-session"]
fn dual_password_alter_user_user_resolves_auth_username() {}

/// Go `dual_password_test.go:780::TestDualPasswordAlterUserUserRetainAndDiscard`:
/// `ALTER USER USER() ... RETAIN CURRENT PASSWORD` and `ALTER USER USER()
/// DISCARD OLD PASSWORD` run through the USER() form for the logged-in
/// account (MySQL 8.0 scenario 13).
#[test]
#[ignore = "go-parity-gap: USER()-form ALTER USER executes in tidb-session"]
fn dual_password_alter_user_user_retain_and_discard() {}

/// Go `dual_password_test.go:825::TestDualPasswordSelfRetainWithExplicitSamePlugin`:
/// a self-session with `APPLICATION_PASSWORD_ADMIN` may
/// `ALTER USER ... IDENTIFIED WITH mysql_native_password BY ... RETAIN
/// CURRENT PASSWORD` when the plugin equals the current one (same-plugin
/// change is not the rejected plugin-change shape).
#[test]
#[ignore = "go-parity-gap: privilege-gated ALTER USER executes in tidb-session"]
fn dual_password_self_retain_with_explicit_same_plugin() {}

/// Go `dual_password_test.go:864::TestDualPasswordLegacyEmptyPluginEncodesWithResolvedPlugin`:
/// a legacy empty-plugin account under a caching_sha2 default
/// authenticates with the plaintext password over the wire while the stored
/// hashes use the resolved plugin's encoding.
#[test]
#[ignore = "go-parity-gap: wire-protocol auth + legacy plugin resolution execute in tidb-session/tidb-server"]
fn dual_password_legacy_empty_plugin_encodes_with_resolved_plugin() {}

/// Go `pkg/executor/test/passwordtest/main_test.go:23::TestMain`: goleak
/// bootstrap only.
#[test]
#[ignore = "go-parity-gap: passwordtest TestMain is goleak suite bootstrap; no statement behavior"]
fn passwordtest_main_is_bootstrap_only() {}
