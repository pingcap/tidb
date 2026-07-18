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

//! Authentication-plugin resolution from `pkg/executor/simple.go`.
//!
//! Legacy `mysql.user` rows may carry an empty plugin name. The source resolves
//! that empty value through `default_authentication_plugin`, falling back to
//! `mysql_native_password` when the default is unavailable. SQL/auth storage,
//! plugin capability checks, password changes, and session policy remain
//! external to this dependency-closed value helper.

/// MySQL's default authentication plugin used when no configured default is
/// available.
pub const AUTH_NATIVE_PASSWORD: &str = "mysql_native_password";

/// Resolves an authentication plugin exactly as TiDB's executor helper does.
///
/// An explicit plugin always wins. An empty plugin uses the configured default;
/// an empty configured default falls back to [`AUTH_NATIVE_PASSWORD`]. Input
/// spelling is intentionally preserved because the Go helper only selects a
/// value and does not normalize its contents.
#[must_use]
pub fn effective_auth_plugin(plugin: &str, default_plugin: &str) -> String {
    if !plugin.is_empty() {
        return plugin.to_owned();
    }
    if default_plugin.is_empty() {
        return AUTH_NATIVE_PASSWORD.to_owned();
    }
    default_plugin.to_owned()
}
