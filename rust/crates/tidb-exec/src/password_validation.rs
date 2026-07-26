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

//! `pkg/util/password-validation` (Go package `validator`): checks a
//! plaintext password against the `validate_password.*` policy.
//!
//! Faithful adaptations:
//! - Go reads policy inputs through `variable.GlobalVarAccessor`; here the
//!   validators are generic over the [`GlobalVarAccessor`] trait so the
//!   production accessor and any mock satisfy them.
//! - The user-name check operates on raw bytes exactly like Go
//!   (`bytes.Contains`, byte-reversed name): it is case-sensitive and
//!   matches on the UTF-8 byte sequence, not on runes.
//! - The dictionary check filters words by their **byte** length
//!   (`len(word)`), lowercases both sides (Unicode-aware, like
//!   `strings.ToLower`), and tests substring containment.
//! - The low policy compares the **rune** count (`len([]rune(pwd))`); the
//!   medium policy classifies each rune with the Unicode upper/lower/digit
//!   properties.

use tidb_error::mysql;
use tidb_parser::auth::UserIdentity;

use crate::option_values::tidb_opt_on;

/// `validate_password.policy`.
pub const VALIDATE_PASSWORD_POLICY: &str = "validate_password.policy";
/// `validate_password.check_user_name`.
pub const VALIDATE_PASSWORD_CHECK_USER_NAME: &str = "validate_password.check_user_name";
/// `validate_password.length`.
pub const VALIDATE_PASSWORD_LENGTH: &str = "validate_password.length";
/// `validate_password.mixed_case_count`.
pub const VALIDATE_PASSWORD_MIXED_CASE_COUNT: &str = "validate_password.mixed_case_count";
/// `validate_password.number_count`.
pub const VALIDATE_PASSWORD_NUMBER_COUNT: &str = "validate_password.number_count";
/// `validate_password.special_char_count`.
pub const VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT: &str = "validate_password.special_char_count";
/// `validate_password.dictionary`.
pub const VALIDATE_PASSWORD_DICTIONARY: &str = "validate_password.dictionary";

const MAX_PWD_VALIDATION_LENGTH: usize = 100;
const MIN_PWD_VALIDATION_LENGTH: usize = 4;

/// Go `variable.GlobalVarAccessor`'s `GetGlobalSysVar`: reads a global
/// system variable's source-form string value.
pub trait GlobalVarAccessor {
    /// The accessor's read-error type (a TiDB error in production).
    type Error;
    /// Returns the global value of `name`, or the accessor's error.
    fn get_global_sys_var(&self, name: &str) -> Result<String, Self::Error>;
}

/// A password-validation error, unifying the accessor read error, the
/// integer-parse error Go surfaces from `strconv.ParseInt`, and the policy
/// rejection Go raises as `ErrNotValidPassword`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PwdError<E> {
    /// The global-variable accessor failed.
    Accessor(E),
    /// A numeric policy variable did not parse as an integer.
    ParseInt(std::num::ParseIntError),
    /// The password violates the policy (Go `ErrNotValidPassword`); carries
    /// the specific warning that Go passes as the message argument.
    NotValid(String),
}

impl<E: std::fmt::Display> std::fmt::Display for PwdError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PwdError::Accessor(e) => write!(f, "{e}"),
            PwdError::ParseInt(e) => write!(f, "{e}"),
            // Reproduces ErrNotValidPassword's message template
            // "Your password does not satisfy the current policy
            // requirements (%s)".
            PwdError::NotValid(warn) => write!(
                f,
                "Your password does not satisfy the current policy requirements ({warn})"
            ),
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for PwdError<E> {}

/// The MySQL error code of a [`PwdError::NotValid`] (`ErrNotValidPassword`,
/// 1819), matching Go's `variable.ErrNotValidPassword`.
#[must_use]
pub const fn not_valid_password_code() -> u16 {
    mysql::errcode::ErrNotValidPassword
}

/// Whether `needle` occurs as a contiguous byte subsequence of `haystack`
/// (Go `bytes.Contains`; `needle` is assumed non-empty by callers).
fn bytes_contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.len() > haystack.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Go `ValidateDictionaryPassword`: rejects a password containing any
/// dictionary word (returns `Ok(false)` when a word is contained).
pub fn validate_dictionary_password<A: GlobalVarAccessor>(
    pwd: &str,
    accessor: &A,
) -> Result<bool, PwdError<A::Error>> {
    let dictionary = accessor
        .get_global_sys_var(VALIDATE_PASSWORD_DICTIONARY)
        .map_err(PwdError::Accessor)?;
    let pwd = pwd.to_lowercase();
    for word in dictionary.split(';') {
        // Go filters by byte length: len(word) in [min, max].
        if word.len() >= MIN_PWD_VALIDATION_LENGTH
            && word.len() <= MAX_PWD_VALIDATION_LENGTH
            && pwd.contains(&word.to_lowercase())
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Go `ValidateUserNameInPassword`: returns a warning if the password
/// contains the (possibly byte-reversed) current user name.
pub fn validate_user_name_in_password<A: GlobalVarAccessor>(
    pwd: &str,
    current_user: Option<&UserIdentity>,
    accessor: &A,
) -> Result<String, PwdError<A::Error>> {
    let pwd_bytes = pwd.as_bytes();
    let check_user_name = accessor
        .get_global_sys_var(VALIDATE_PASSWORD_CHECK_USER_NAME)
        .map_err(PwdError::Accessor)?;
    if let Some(user) = current_user {
        if tidb_opt_on(&check_user_name) {
            // Go iterates [AuthUsername, Username] in that order.
            for username in [&user.auth_username, &user.username] {
                let username_bytes = username.as_bytes();
                if username_bytes.is_empty() {
                    continue;
                }
                if bytes_contains(pwd_bytes, username_bytes) {
                    return Ok("Password Contains User Name".to_owned());
                }
                let reversed: Vec<u8> = username_bytes.iter().rev().copied().collect();
                if bytes_contains(pwd_bytes, &reversed) {
                    return Ok("Password Contains Reversed User Name".to_owned());
                }
            }
        }
    }
    Ok(String::new())
}

/// Go `ValidatePasswordLowPolicy`: enforces the minimum length (in runes).
pub fn validate_password_low_policy<A: GlobalVarAccessor>(
    pwd: &str,
    accessor: &A,
) -> Result<String, PwdError<A::Error>> {
    let validate_length: i64 = accessor
        .get_global_sys_var(VALIDATE_PASSWORD_LENGTH)
        .map_err(PwdError::Accessor)?
        .parse()
        .map_err(PwdError::ParseInt)?;
    if (pwd.chars().count() as i64) < validate_length {
        return Ok(format!("Require Password Length: {validate_length}"));
    }
    Ok(String::new())
}

/// Go `ValidatePasswordMediumPolicy`: enforces mixed-case, digit, and
/// special-character minimum counts.
pub fn validate_password_medium_policy<A: GlobalVarAccessor>(
    pwd: &str,
    accessor: &A,
) -> Result<String, PwdError<A::Error>> {
    let (mut lower_case, mut upper_case, mut number, mut special) = (0i64, 0i64, 0i64, 0i64);
    // Classify each rune exactly as Go's unicode.IsUpper/IsLower/IsDigit
    // else-chain does; a rune matching none is a special character.
    for c in pwd.chars() {
        if c.is_uppercase() {
            upper_case += 1;
        } else if c.is_lowercase() {
            lower_case += 1;
        } else if c.is_numeric() {
            number += 1;
        } else {
            special += 1;
        }
    }

    let mixed_case_count: i64 = accessor
        .get_global_sys_var(VALIDATE_PASSWORD_MIXED_CASE_COUNT)
        .map_err(PwdError::Accessor)?
        .parse()
        .map_err(PwdError::ParseInt)?;
    if lower_case < mixed_case_count {
        return Ok(format!(
            "Require Password Lowercase Count: {mixed_case_count}"
        ));
    }
    if upper_case < mixed_case_count {
        return Ok(format!(
            "Require Password Uppercase Count: {mixed_case_count}"
        ));
    }

    let require_number_count: i64 = accessor
        .get_global_sys_var(VALIDATE_PASSWORD_NUMBER_COUNT)
        .map_err(PwdError::Accessor)?
        .parse()
        .map_err(PwdError::ParseInt)?;
    if number < require_number_count {
        return Ok(format!(
            "Require Password Digit Count: {require_number_count}"
        ));
    }

    let require_special_char_count: i64 = accessor
        .get_global_sys_var(VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT)
        .map_err(PwdError::Accessor)?
        .parse()
        .map_err(PwdError::ParseInt)?;
    if special < require_special_char_count {
        return Ok(format!(
            "Require Password Non-alphanumeric Count: {require_special_char_count}"
        ));
    }
    Ok(String::new())
}

/// Go `ValidatePassword`: full policy check (LOW -> MEDIUM -> STRONG).
pub fn validate_password<A: GlobalVarAccessor>(
    pwd: &str,
    current_user: Option<&UserIdentity>,
    accessor: &A,
) -> Result<(), PwdError<A::Error>> {
    let validate_policy = accessor
        .get_global_sys_var(VALIDATE_PASSWORD_POLICY)
        .map_err(PwdError::Accessor)?;

    let warn = validate_user_name_in_password(pwd, current_user, accessor)?;
    if !warn.is_empty() {
        return Err(PwdError::NotValid(warn));
    }
    let warn = validate_password_low_policy(pwd, accessor)?;
    if !warn.is_empty() {
        return Err(PwdError::NotValid(warn));
    }
    if validate_policy == "LOW" {
        return Ok(());
    }

    let warn = validate_password_medium_policy(pwd, accessor)?;
    if !warn.is_empty() {
        return Err(PwdError::NotValid(warn));
    }
    if validate_policy == "MEDIUM" {
        return Ok(());
    }

    if !validate_dictionary_password(pwd, accessor)? {
        return Err(PwdError::NotValid(
            "Password contains word in the dictionary".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::convert::Infallible;

    use super::*;

    /// A source-shaped test accessor: a plain name -> value map. TiDB's
    /// `MockGlobalAccessor4Tests` snapshots the sysvar registry defaults; we
    /// instead seed the relevant `validate_password.*` values directly, which
    /// is equivalent for these checks and keeps the test dependency-closed.
    #[derive(Default)]
    struct MapAccessor(BTreeMap<String, String>);

    impl MapAccessor {
        fn set(&mut self, name: &str, value: &str) {
            self.0.insert(name.to_owned(), value.to_owned());
        }
    }

    impl GlobalVarAccessor for MapAccessor {
        type Error = Infallible;
        fn get_global_sys_var(&self, name: &str) -> Result<String, Infallible> {
            Ok(self.0.get(name).cloned().unwrap_or_default())
        }
    }

    fn user(username: &str, auth_username: &str) -> UserIdentity {
        UserIdentity {
            username: username.to_owned(),
            auth_username: auth_username.to_owned(),
            ..UserIdentity::default()
        }
    }

    // Go TestValidateDictionaryPassword.
    #[test]
    fn dictionary_password() {
        let mut acc = MapAccessor::default();
        acc.set(
            VALIDATE_PASSWORD_DICTIONARY,
            "abc;123;1234;5678;HIJK;中文测试;。，；！",
        );
        for (pwd, expected) in [
            ("abcdefg", true),
            ("abcd123efg", true),
            ("abcd1234efg", false),
            ("abcd12345efg", false),
            ("abcd123efghij", true),
            ("abcd123efghijk", false),
            ("abcd123efghij中文测试", false),
            ("abcd123。，；！", false),
        ] {
            assert_eq!(
                validate_dictionary_password(pwd, &acc).unwrap(),
                expected,
                "{pwd}"
            );
        }
    }

    // Go TestValidateUserNameInPassword.
    #[test]
    fn user_name_in_password() {
        let u = user("user", "authuser");
        let cases = [
            ("", ""),
            ("user", "Password Contains User Name"),
            ("authuser", "Password Contains User Name"),
            ("resu000", "Password Contains Reversed User Name"),
            ("resuhtua", "Password Contains Reversed User Name"),
            ("User", ""),
            ("authUser", ""),
            ("Resu", ""),
            ("Resuhtua", ""),
        ];

        let mut acc = MapAccessor::default();
        acc.set(VALIDATE_PASSWORD_CHECK_USER_NAME, "ON");
        for (pwd, warn) in cases {
            assert_eq!(
                validate_user_name_in_password(pwd, Some(&u), &acc).unwrap(),
                warn,
                "{pwd}"
            );
        }

        // Disabled -> never warns.
        acc.set(VALIDATE_PASSWORD_CHECK_USER_NAME, "OFF");
        for (pwd, _) in cases {
            assert_eq!(
                validate_user_name_in_password(pwd, Some(&u), &acc).unwrap(),
                "",
                "{pwd}"
            );
        }
    }

    // Go TestValidatePasswordLowPolicy.
    #[test]
    fn low_policy() {
        let mut acc = MapAccessor::default();
        acc.set(VALIDATE_PASSWORD_LENGTH, "8");
        assert_eq!(
            validate_password_low_policy("1234", &acc).unwrap(),
            "Require Password Length: 8"
        );
        assert_eq!(validate_password_low_policy("12345678", &acc).unwrap(), "");

        acc.set(VALIDATE_PASSWORD_LENGTH, "12");
        assert_eq!(
            validate_password_low_policy("12345678", &acc).unwrap(),
            "Require Password Length: 12"
        );
    }

    // Go TestValidatePasswordMediumPolicy.
    #[test]
    fn medium_policy() {
        let mut acc = MapAccessor::default();
        acc.set(VALIDATE_PASSWORD_MIXED_CASE_COUNT, "1");
        acc.set(VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT, "2");
        acc.set(VALIDATE_PASSWORD_NUMBER_COUNT, "3");

        assert_eq!(
            validate_password_medium_policy("!@A123", &acc).unwrap(),
            "Require Password Lowercase Count: 1"
        );
        assert_eq!(
            validate_password_medium_policy("!@a123", &acc).unwrap(),
            "Require Password Uppercase Count: 1"
        );
        assert_eq!(
            validate_password_medium_policy("!@Aa12", &acc).unwrap(),
            "Require Password Digit Count: 3"
        );
        assert_eq!(
            validate_password_medium_policy("!Aa123", &acc).unwrap(),
            "Require Password Non-alphanumeric Count: 2"
        );
        assert_eq!(
            validate_password_medium_policy("!@Aa123", &acc).unwrap(),
            ""
        );
    }

    // Go TestValidatePassword.
    #[test]
    fn validate_password_full() {
        let u = user("user", "authuser");
        let mut acc = MapAccessor::default();
        // Registry defaults the test relies on (see sysvar.go).
        acc.set(VALIDATE_PASSWORD_CHECK_USER_NAME, "ON");
        acc.set(VALIDATE_PASSWORD_LENGTH, "8");
        acc.set(VALIDATE_PASSWORD_MIXED_CASE_COUNT, "1");
        acc.set(VALIDATE_PASSWORD_NUMBER_COUNT, "1");
        acc.set(VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT, "1");
        acc.set(VALIDATE_PASSWORD_DICTIONARY, "");

        acc.set(VALIDATE_PASSWORD_POLICY, "LOW");
        assert!(validate_password("1234", Some(&u), &acc).is_err());
        assert!(validate_password("user1234", Some(&u), &acc).is_err());
        assert!(validate_password("authuser1234", Some(&u), &acc).is_err());
        assert!(validate_password("User1234", Some(&u), &acc).is_ok());

        acc.set(VALIDATE_PASSWORD_POLICY, "MEDIUM");
        assert!(validate_password("User1234", Some(&u), &acc).is_err());
        assert!(validate_password("!User1234", Some(&u), &acc).is_ok());
        assert!(validate_password("！User1234", Some(&u), &acc).is_ok());

        acc.set(VALIDATE_PASSWORD_POLICY, "STRONG");
        acc.set(VALIDATE_PASSWORD_DICTIONARY, "User");
        assert!(validate_password("!User1234", Some(&u), &acc).is_err());
        assert!(validate_password("!ABcd1234", Some(&u), &acc).is_ok());

        // The rejection carries ErrNotValidPassword's code and message.
        let err = validate_password("1234", Some(&u), &acc).unwrap_err();
        assert_eq!(not_valid_password_code(), 1819);
        assert!(err
            .to_string()
            .starts_with("Your password does not satisfy the current policy requirements ("));
    }
}
