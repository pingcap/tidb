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

//! Source-backed tests for authentication-plugin resolution.

use tidb_exec::effective_auth_plugin::{effective_auth_plugin, AUTH_NATIVE_PASSWORD};

#[test]
fn effective_plugin_preserves_explicit_and_default_values() {
    // Source: pkg/executor/simple.go:2768-2788.
    // Direct Go coverage: pkg/executor/test/passwordtest/dual_password_test.go:
    // 195 (TestDualPasswordLegacyEmptyPluginAcceptsNative), 221
    // (TestDualPasswordLegacyEmptyPluginHonorsDefaultPlugin), and 632
    // (TestDualPasswordLegacyEmptyPluginRejectsLDAPDefault).
    assert_eq!(effective_auth_plugin("", ""), AUTH_NATIVE_PASSWORD);
    assert_eq!(
        effective_auth_plugin("", "caching_sha2_password"),
        "caching_sha2_password"
    );
    assert_eq!(
        effective_auth_plugin("", "authentication_ldap_simple"),
        "authentication_ldap_simple"
    );
    assert_eq!(
        effective_auth_plugin("caching_sha2_password", "authentication_ldap_simple"),
        "caching_sha2_password"
    );

    // The source does selection only: spelling and surrounding whitespace are
    // preserved for the later plugin comparison/capability boundary.
    assert_eq!(
        effective_auth_plugin(" CUSTOM_PLUGIN ", "other"),
        " CUSTOM_PLUGIN "
    );
}
