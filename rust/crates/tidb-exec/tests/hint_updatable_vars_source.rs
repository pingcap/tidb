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

//! Source-backed tests for SET_VAR hint-updatable variables.

use tidb_exec::hint_updatable_vars::{is_hint_updatable_verified, HINT_UPDATABLE_VARIABLES};

#[test]
fn hint_updatable_registry_preserves_source_membership() {
    // Source: pkg/sessionctx/variable/setvar_affect.go:17-149 and
    // pkg/sessionctx/variable/sysvar_test.go:138-164
    // (TestTiDBMaxKeysRead checks the registry-backed marker).
    assert_eq!(HINT_UPDATABLE_VARIABLES.len(), 128);
    for name in HINT_UPDATABLE_VARIABLES {
        assert!(is_hint_updatable_verified(name), "{name}");
    }
    assert!(is_hint_updatable_verified("tidb_max_keys_read"));
    assert!(is_hint_updatable_verified("sql_mode"));
    assert!(!is_hint_updatable_verified("tidb_read_staleness"));
}

#[test]
fn hint_updatable_registry_keeps_exact_case_and_boundary() {
    // Source: pkg/sessionctx/variable/setvar_affect.go:17-149.
    assert!(!is_hint_updatable_verified("TIDB_MAX_KEYS_READ"));
    assert!(!is_hint_updatable_verified("tidb_max_keys_read_suffix"));
    assert!(!is_hint_updatable_verified("unknown_system_variable"));
}
