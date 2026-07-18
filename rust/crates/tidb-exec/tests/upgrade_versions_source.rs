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

//! Source-backed tests for the ordered bootstrap upgrade registry.

use tidb_exec::upgrade_versions::{
    is_valid_upgrade_registry, upgrade_function_name, upgrade_versions, CURRENT_BOOTSTRAP_VERSION,
};

#[test]
fn upgrade_registry_preserves_order_gaps_and_function_names() {
    // Source: pkg/session/upgrade_def.go:524-710 and
    // pkg/session/upgrade_test.go:52-66 (TestUpgradeToVerFunctionsCheck).
    let versions = upgrade_versions();
    assert_eq!(versions.len(), 173);
    assert!(is_valid_upgrade_registry(&versions));
    assert_eq!(versions.first(), Some(&2));
    assert_eq!(versions.last(), Some(&CURRENT_BOOTSTRAP_VERSION));

    // Historical functions intentionally absent from the source registry.
    for skipped in [
        39, 48, 49, 51, 58, 61, 92, 96, 111, 129, 145, 147, 166, 180, 189, 199, 208, 219, 238,
    ] {
        assert!(
            !versions.contains(&skipped),
            "version {skipped} unexpectedly registered"
        );
    }
    for version in [2, 38, 40, 50, 110, 130, 146, 167, 218, 239, 263] {
        assert_eq!(
            upgrade_function_name(version),
            format!("upgradeToVer{version}")
        );
    }

    let mut out_of_order = versions.clone();
    out_of_order.swap(0, 1);
    assert!(!is_valid_upgrade_registry(&out_of_order));
}
