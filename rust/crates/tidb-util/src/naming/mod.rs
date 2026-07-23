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
//! `naming.go` and `naming_test.go` map to this module and its source-named
//! test. `BUILD.bazel` maps to the `tidb-util` manifest. `OWNERS` is repository
//! review metadata and remains authoritative in the Go source directory; it
//! has no runtime Rust artifact. The package has no `TestMain`, benchmarks,
//! fuzz targets, examples, fixtures, generated files, or build-tag variants.

use regex::Regex;
use std::fmt;

const MAX_KEYSPACE_NAME_LENGTH: isize = 20;
const MAX_GO_REGEXP_REPEAT: isize = 1_000;

/// The source validation failure with its exact valid-UTF-8 message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamingError {
    name: String,
    max_len: isize,
}

impl NamingError {
    /// Returns the rejected name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the configured maximum length.
    #[must_use]
    pub fn max_len(&self) -> isize {
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

/// Checks a name against the source's dynamically constructed regular
/// expression.
///
/// # Panics
///
/// Go's `regexp.MustCompile` rejects repeat bounds outside `0..=1000`.
/// This function preserves that boundary before compiling the same pattern.
pub fn check_with_max_len(name: &str, max_len: isize) -> Result<(), NamingError> {
    assert!(
        (0..=MAX_GO_REGEXP_REPEAT).contains(&max_len),
        "regexp: Compile(`^[a-zA-Z0-9_-]{{0,{max_len}}}$`): error parsing regexp: invalid repeat count"
    );
    let pattern = format!("^[a-zA-Z0-9_-]{{0,{max_len}}}$");
    let name_regex = Regex::new(&pattern).expect("source-compatible naming regexp must compile");
    if name_regex.is_match(name) {
        Ok(())
    } else {
        Err(NamingError {
            name: name.to_owned(),
            max_len,
        })
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;

    #[test]
    fn TestScope() {
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
    fn source_uncovered_keyspace_and_regexp_boundaries_are_preserved() {
        assert!(check_keyspace_name("12345678901234567890").is_ok());
        assert!(check_keyspace_name("123456789012345678901").is_err());
        assert_eq!(
            check("bad name").expect_err("space is invalid").to_string(),
            "the value 'bad name' is invalid. It must be 64 characters or fewer and consist only of letters (a-z, A-Z), numbers (0-9), hyphens (-), and underscores (_)"
        );

        assert!(std::panic::catch_unwind(|| check_with_max_len("", -1)).is_err());
        assert!(std::panic::catch_unwind(|| check_with_max_len("", 1_001)).is_err());
    }
}
