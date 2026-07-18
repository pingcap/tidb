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

//! Source-backed tests for hash-join version selection.

use tidb_exec::hash_join_version::{
    is_optimized_version, HASH_JOIN_VERSION_LEGACY, HASH_JOIN_VERSION_OPTIMIZED,
    TIFLASH_HASH_JOIN_VERSION_DEFAULT,
};

#[test]
fn hash_join_version_preserves_literals_and_case_boundary() {
    // Source: pkg/executor/join/joinversion/join_version.go:20-44 and
    // pkg/sessionctx/variable/sysvar_test.go:1829-1851 (TestTiDBHashJoinVersion).
    assert_eq!(HASH_JOIN_VERSION_LEGACY, "legacy");
    assert_eq!(HASH_JOIN_VERSION_OPTIMIZED, "optimized");
    assert_eq!(TIFLASH_HASH_JOIN_VERSION_DEFAULT, HASH_JOIN_VERSION_LEGACY);

    assert!(!is_optimized_version(HASH_JOIN_VERSION_LEGACY));
    for value in ["optimized", "Optimized", "OPTIMIZED", "OptimiZed"] {
        assert!(is_optimized_version(value));
    }
    for value in ["legacy", "Legacy", "invalid", "optimized ", "v2"] {
        assert!(!is_optimized_version(value));
    }
}
