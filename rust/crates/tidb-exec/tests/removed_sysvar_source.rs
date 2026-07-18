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

//! Source-shaped tests for removed session system-variable policy.

use tidb_exec::removed_sysvar::{is_removed, removal_reason, REMOVED_SYSTEM_VARIABLES};

#[test]
fn known_removed_variables_return_the_source_reason() {
    // Source: pkg/sessionctx/variable/removed.go:37-55.
    assert!(is_removed("tidb_enable_alter_placement"));
    assert_eq!(
        removal_reason("tidb_enable_alter_placement"),
        Some("alter placement is now always enabled")
    );
    assert_eq!(
        removal_reason("tidb_opt_broadcast_join"),
        Some("tidb_opt_broadcast_join is removed and use tidb_allow_mpp instead")
    );
    assert_eq!(REMOVED_SYSTEM_VARIABLES.len(), 13);
}

#[test]
fn unknown_and_differently_cased_names_are_not_silently_removed() {
    // Source: pkg/sessionctx/variable/removed.go:59-71.
    assert!(!is_removed("tidb_enable_1pc"));
    assert_eq!(removal_reason("tidb_enable_1pc"), None);
    assert!(!is_removed("TIDB_ENABLE_ALTER_PLACEMENT"));
    assert_eq!(
        removal_reason("placement_checks"),
        Some("placement_checks is removed and use tidb_placement_mode instead")
    );
}

#[test]
fn every_registry_entry_has_a_nonempty_exact_name_and_reason() {
    // Source: pkg/sessionctx/variable/removed.go:37-55.
    assert!(REMOVED_SYSTEM_VARIABLES
        .iter()
        .all(|variable| !variable.name.is_empty() && !variable.reason.is_empty()));
}
