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

use tidb_datatype::GoString;
use tidb_util::password_validation::{
    validate_password, GlobalVarAccessor, PasswordUser, PwdError,
};

const VALIDATE_PASSWORD_ENABLE: &str = "validate_password.enable";
const VALIDATE_PASSWORD_POLICY: &str = "validate_password.policy";
const VALIDATE_PASSWORD_CHECK_USER_NAME: &str = "validate_password.check_user_name";
const VALIDATE_PASSWORD_LENGTH: &str = "validate_password.length";
const VALIDATE_PASSWORD_MIXED_CASE_COUNT: &str = "validate_password.mixed_case_count";
const VALIDATE_PASSWORD_NUMBER_COUNT: &str = "validate_password.number_count";
const VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT: &str = "validate_password.special_char_count";
const VALIDATE_PASSWORD_DICTIONARY: &str = "validate_password.dictionary";

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
        PwdError::NotValid(warning) => {
            format!("Your password does not satisfy the current policy requirements ({warning})")
        }
        other => other.to_string(),
    }
}

/// The suite's caller is the authenticated root session; Go's
/// `ValidateUserNameInPassword` checks the password against the CURRENT
/// user (root), never the ALTERed account -- which is why `SET PASSWORD FOR
/// 'testuser' = 'testuser'` succeeds while `!Abcdroot1234` is rejected.
fn check_as_root(vars: &Vars, password: &str) -> Result<(), String> {
    let user = PasswordUser {
        username: "root".into(),
        auth_username: "".into(),
    };
    validate_password(&GoString::from(password), Some(&user), vars)
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
    let empty_user = PasswordUser {
        username: "".into(),
        auth_username: "".into(),
    };
    let empty_user = validate_password(&GoString::from(""), Some(&empty_user), &zeroed);
    assert!(empty_user.is_ok(), "empty user name skips the name check");
}
