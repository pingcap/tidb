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

//! Validation for TiDB keyspace and service-scope names.

use std::fmt;

const MAX_KEYSPACE_NAME_LENGTH: isize = 20;

/// A rejected TiDB name.
#[derive(Debug)]
pub struct NamingError {
    name: String,
    max_len: isize,
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
pub fn check_with_max_len(name: &str, max_len: isize) -> Result<(), NamingError> {
    if max_len > 1000 {
        let pattern = format!("^[a-zA-Z0-9_-]{{0,{max_len}}}$");
        panic!(
            "regexp: Compile(`{pattern}`): error parsing regexp: invalid repeat count: `{{0,{max_len}}}`"
        );
    }

    let is_name_byte = |byte: u8| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_');
    let valid = if max_len < 0 {
        let suffix = format!("{{0,{max_len}}}");
        name.len() == suffix.len() + 1
            && is_name_byte(name.as_bytes()[0])
            && &name.as_bytes()[1..] == suffix.as_bytes()
    } else {
        name.len() <= max_len as usize && name.bytes().all(is_name_byte)
    };
    if valid {
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
    fn check_with_max_len_matches_regexp_repeat_domain() {
        assert!(check_with_max_len("", 1000).is_ok());
        let panic = std::panic::catch_unwind(|| check_with_max_len("", 1001))
            .expect_err("Go regexp.MustCompile panics above 1000 repetitions");
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .expect("panic message");
        assert_eq!(
            message,
            "regexp: Compile(`^[a-zA-Z0-9_-]{0,1001}$`): error parsing regexp: invalid repeat count: `{0,1001}`"
        );

        // A negative maximum is not parsed as repetition syntax by Go's
        // regexp package. The generated text is matched literally instead.
        assert!(check_with_max_len("a{0,-1}", -1).is_ok());
        assert!(check_with_max_len("", -1).is_err());
        assert!(check_with_max_len("a", -1).is_err());
    }
}
