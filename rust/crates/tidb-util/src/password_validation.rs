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

use tidb_datatype::GoString;

const VALIDATE_PASSWORD_POLICY: &str = "validate_password.policy";
const VALIDATE_PASSWORD_CHECK_USER_NAME: &str = "validate_password.check_user_name";
const VALIDATE_PASSWORD_LENGTH: &str = "validate_password.length";
const VALIDATE_PASSWORD_MIXED_CASE_COUNT: &str = "validate_password.mixed_case_count";
const VALIDATE_PASSWORD_NUMBER_COUNT: &str = "validate_password.number_count";
const VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT: &str = "validate_password.special_char_count";
const VALIDATE_PASSWORD_DICTIONARY: &str = "validate_password.dictionary";

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

/// The two username spellings stored on Go's `auth.UserIdentity`.
pub struct PasswordUser {
    /// The connection login username (`UserIdentity.Username`).
    pub username: GoString,
    /// The matched grant username (`UserIdentity.AuthUsername`).
    pub auth_username: GoString,
}

/// A password-validation error, unifying the accessor read error, the
/// integer-parse error Go surfaces from `strconv.ParseInt`, and the policy
/// rejection Go raises as `ErrNotValidPassword`.
#[derive(Debug)]
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

/// Whether `needle` occurs as a contiguous byte subsequence of `haystack`
/// (Go `bytes.Contains`; `needle` is assumed non-empty by callers).
fn bytes_contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.len() > haystack.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn tidb_opt_on(value: &str) -> bool {
    value.eq_ignore_ascii_case("ON") || value == "1"
}

/// Go `ValidateDictionaryPassword`: rejects a password containing any
/// dictionary word (returns `Ok(false)` when a word is contained).
pub fn validate_dictionary_password<A: GlobalVarAccessor>(
    pwd: &GoString,
    accessor: &A,
) -> Result<bool, PwdError<A::Error>> {
    let dictionary = accessor
        .get_global_sys_var(VALIDATE_PASSWORD_DICTIONARY)
        .map_err(PwdError::Accessor)?;
    let pwd = tidb_mysql::to_lowercase(&pwd.to_utf8_lossy_go());
    for word in dictionary.split(';') {
        // Go filters by byte length: len(word) in [min, max].
        if word.len() >= MIN_PWD_VALIDATION_LENGTH
            && word.len() <= MAX_PWD_VALIDATION_LENGTH
            && pwd.contains(&tidb_mysql::to_lowercase(word))
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Go `ValidateUserNameInPassword`: returns a warning if the password
/// contains the (possibly byte-reversed) current user name.
pub fn validate_user_name_in_password<A: GlobalVarAccessor>(
    pwd: &GoString,
    current_user: Option<&PasswordUser>,
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
    pwd: &GoString,
    accessor: &A,
) -> Result<String, PwdError<A::Error>> {
    let validate_length: i64 = accessor
        .get_global_sys_var(VALIDATE_PASSWORD_LENGTH)
        .map_err(PwdError::Accessor)?
        .parse()
        .map_err(PwdError::ParseInt)?;
    if (pwd.to_utf8_lossy_go().chars().count() as i64) < validate_length {
        return Ok(format!("Require Password Length: {validate_length}"));
    }
    Ok(String::new())
}

/// Go `ValidatePasswordMediumPolicy`: enforces mixed-case, digit, and
/// special-character minimum counts.
pub fn validate_password_medium_policy<A: GlobalVarAccessor>(
    pwd: &GoString,
    accessor: &A,
) -> Result<String, PwdError<A::Error>> {
    let (mut lower_case, mut upper_case, mut number, mut special) = (0i64, 0i64, 0i64, 0i64);
    // Classify each rune exactly as Go's unicode.IsUpper/IsLower/IsDigit
    // else-chain does; a rune matching none is a special character.
    for c in pwd.to_utf8_lossy_go().chars() {
        if tidb_mysql::is_unicode_uppercase_letter(c) {
            upper_case += 1;
        } else if tidb_mysql::is_unicode_lowercase_letter(c) {
            lower_case += 1;
        } else if tidb_mysql::is_unicode_decimal_digit(c) {
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
    pwd: &GoString,
    current_user: Option<&PasswordUser>,
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

    fn user(username: &str, auth_username: &str) -> PasswordUser {
        PasswordUser {
            username: username.into(),
            auth_username: auth_username.into(),
        }
    }

    fn password(value: &str) -> GoString {
        value.into()
    }

    // Go TestValidateDictionaryPassword.
    #[test]
    fn test_validate_dictionary_password() {
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
                validate_dictionary_password(&password(pwd), &acc).unwrap(),
                expected,
                "{pwd}"
            );
        }
    }

    // Go TestValidateUserNameInPassword.
    #[test]
    fn test_validate_user_name_in_password() {
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
                validate_user_name_in_password(&password(pwd), Some(&u), &acc).unwrap(),
                warn,
                "{pwd}"
            );
        }

        // Disabled -> never warns.
        acc.set(VALIDATE_PASSWORD_CHECK_USER_NAME, "OFF");
        for (pwd, _) in cases {
            assert_eq!(
                validate_user_name_in_password(&password(pwd), Some(&u), &acc).unwrap(),
                "",
                "{pwd}"
            );
        }
    }

    // Go TestValidatePasswordLowPolicy.
    #[test]
    fn test_validate_password_low_policy() {
        let mut acc = MapAccessor::default();
        acc.set(VALIDATE_PASSWORD_LENGTH, "8");
        assert_eq!(
            validate_password_low_policy(&password("1234"), &acc).unwrap(),
            "Require Password Length: 8"
        );
        assert_eq!(
            validate_password_low_policy(&password("12345678"), &acc).unwrap(),
            ""
        );

        acc.set(VALIDATE_PASSWORD_LENGTH, "12");
        assert_eq!(
            validate_password_low_policy(&password("12345678"), &acc).unwrap(),
            "Require Password Length: 12"
        );
    }

    // Go TestValidatePasswordMediumPolicy.
    #[test]
    fn test_validate_password_medium_policy() {
        let mut acc = MapAccessor::default();
        acc.set(VALIDATE_PASSWORD_MIXED_CASE_COUNT, "1");
        acc.set(VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT, "2");
        acc.set(VALIDATE_PASSWORD_NUMBER_COUNT, "3");

        assert_eq!(
            validate_password_medium_policy(&password("!@A123"), &acc).unwrap(),
            "Require Password Lowercase Count: 1"
        );
        assert_eq!(
            validate_password_medium_policy(&password("!@a123"), &acc).unwrap(),
            "Require Password Uppercase Count: 1"
        );
        assert_eq!(
            validate_password_medium_policy(&password("!@Aa12"), &acc).unwrap(),
            "Require Password Digit Count: 3"
        );
        assert_eq!(
            validate_password_medium_policy(&password("!Aa123"), &acc).unwrap(),
            "Require Password Non-alphanumeric Count: 2"
        );
        assert_eq!(
            validate_password_medium_policy(&password("!@Aa123"), &acc).unwrap(),
            ""
        );
    }

    // Go TestValidatePassword.
    #[test]
    fn test_validate_password() {
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
        assert!(validate_password(&password("1234"), Some(&u), &acc).is_err());
        assert!(validate_password(&password("user1234"), Some(&u), &acc).is_err());
        assert!(validate_password(&password("authuser1234"), Some(&u), &acc).is_err());
        assert!(validate_password(&password("User1234"), Some(&u), &acc).is_ok());

        acc.set(VALIDATE_PASSWORD_POLICY, "MEDIUM");
        assert!(validate_password(&password("User1234"), Some(&u), &acc).is_err());
        assert!(validate_password(&password("!User1234"), Some(&u), &acc).is_ok());
        assert!(validate_password(&password("！User1234"), Some(&u), &acc).is_ok());

        acc.set(VALIDATE_PASSWORD_POLICY, "STRONG");
        acc.set(VALIDATE_PASSWORD_DICTIONARY, "User");
        assert!(validate_password(&password("!User1234"), Some(&u), &acc).is_err());
        assert!(validate_password(&password("!ABcd1234"), Some(&u), &acc).is_ok());
    }
}
