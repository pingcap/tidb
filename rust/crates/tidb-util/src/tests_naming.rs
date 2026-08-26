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

//! Ports of `pkg/util/naming` unit tests from `origin/master`.

use crate::naming::{check, check_keyspace_name, check_with_max_len};

/// Port of `naming_test.go` `TestScope`: names made of letters, digits,
/// hyphens and underscores are accepted; other bytes and over-long names
/// (beyond the 64-character `Check` limit) are rejected; the empty string is
/// valid.
#[test]
fn scope_check_accepts_only_alnum_hyphen_underscore_within_64_chars() {
    assert!(check("789z-_").is_ok());
    assert!(check("789z-_)").is_err());
    assert!(check(
        "78912345678982u7389217897238917389127893781278937128973812728397281378932179837"
    )
    .is_err());
    assert!(check("scope1").is_ok());
    assert!(check("").is_ok());
    assert!(check("-----").is_ok());

    // Derived from `CheckWithMaxLen`: the same contract applies to keyspace
    // names with the tighter 20-character bound.
    assert!(check_keyspace_name("12345678901234567890").is_ok());
    assert!(check_keyspace_name("123456789012345678901").is_err());
    assert!(check_with_max_len("a", 0).is_err());
}
