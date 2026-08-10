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

//! Complete transcreation of `pkg/util/naming`.
//!
//! Names are bounded ASCII identifiers containing letters, digits, hyphens,
//! and underscores. Empty names are valid, matching TiDB configuration rules.

use std::fmt;

const MAX_KEYSPACE_NAME_LENGTH: usize = 20;

/// The source validation failure with its exact valid-UTF-8 message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamingError {
    name: String,
    max_len: usize,
}

impl NamingError {
    /// Returns the rejected name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the configured maximum length.
    #[must_use]
    pub fn max_len(&self) -> usize {
        self.max_len
    }
}

impl fmt::Display for NamingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "the value '{}' is invalid. It must be {} characters or fewer and consist only of letters (a-z, A-Z), numbers (0-9), hyphens (-), and underscores (_)",
            self.name, self.max_len
        )
    }
}

impl std::error::Error for NamingError {}

/// Checks the shared 64-character service-scope/name contract.
pub fn check(name: &str) -> Result<(), NamingError> {
    check_with_max_len(name, 64)
}

/// Checks the 20-character keyspace-name contract.
pub fn check_keyspace_name(name: &str) -> Result<(), NamingError> {
    check_with_max_len(name, MAX_KEYSPACE_NAME_LENGTH)
}

/// Checks a name against a caller-supplied maximum length.
pub fn check_with_max_len(name: &str, max_len: usize) -> Result<(), NamingError> {
    if name.len() <= max_len
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(())
    } else {
        Err(NamingError {
            name: name.to_owned(),
            max_len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `pkg/util/naming/naming_test.go` `TestScope`.
    #[test]
    fn test_scope() {
        assert!(check("789z-_").is_ok());
        assert!(check("789z-_)").is_err());
        assert!(check(
            "78912345678982u7389217897238917389127893781278937128973812728397281378932179837"
        )
        .is_err());
        assert!(check("scope1").is_ok());
        assert!(check("").is_ok());
        assert!(check("-----").is_ok());
    }

    #[test]
    fn keyspace_length_ascii_and_error_contracts() {
        assert!(check_keyspace_name("12345678901234567890").is_ok());
        assert!(check_keyspace_name("123456789012345678901").is_err());
        assert!(check(&"a".repeat(64)).is_ok());
        assert!(check(&"a".repeat(65)).is_err());
        assert!(check("é").is_err());
        assert!(check_with_max_len("", 0).is_ok());
        assert!(check_with_max_len("a", 0).is_err());
        assert_eq!(
            check("bad name").expect_err("space is invalid").to_string(),
            "the value 'bad name' is invalid. It must be 64 characters or fewer and consist only of letters (a-z, A-Z), numbers (0-9), hyphens (-), and underscores (_)"
        );
    }
}
