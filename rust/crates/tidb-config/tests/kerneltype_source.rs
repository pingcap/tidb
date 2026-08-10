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

//! Source tests for Go `pkg/config/kerneltype/type_test.go`.

use tidb_config::kerneltype::{is_classic, is_match, is_next_gen, name};

#[test]
fn kernel_type_predicates_match_source() {
    assert_eq!(!is_classic(), is_next_gen());
    assert_eq!(is_classic(), !is_next_gen());
}

#[test]
fn pd_kernel_type_matching_matches_source() {
    if is_classic() {
        assert_eq!(name(), "Classic");
        assert!(is_match(""));
        assert!(is_match("Classic"));
    } else {
        assert_eq!(name(), "Next Generation");
        assert!(is_match("Next Generation"));
        assert!(!is_match(""));
        assert!(!is_match("Classic"));
    }
    assert!(!is_match("Unknown"));
}
