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

//! Native Rust mapping of the Go `pkg/util/versioninfo` package.
//!
//! Go injects five version variables with `-ldflags -X` and may reassign them
//! at runtime. Rust's `option_env!` values are immutable compile-time
//! approximations, so only `CommunityEdition` is classified as fully ported.

/// The default edition for building (Go `CommunityEdition`).
pub const COMMUNITY_EDITION: &str = "Community";

/// Immutable compile-time approximation of Go `TiDBBuildTS`.
pub const TIDB_BUILD_TS: &str = match option_env!("TIDB_BUILD_TS") {
    Some(v) => v,
    None => "None",
};
/// Immutable compile-time approximation of Go `TiDBGitHash`.
pub const TIDB_GIT_HASH: &str = match option_env!("TIDB_GIT_HASH") {
    Some(v) => v,
    None => "None",
};
/// Immutable compile-time approximation of Go `TiDBGitBranch`.
pub const TIDB_GIT_BRANCH: &str = match option_env!("TIDB_GIT_BRANCH") {
    Some(v) => v,
    None => "None",
};
/// Immutable compile-time approximation of Go `TiDBEdition`.
pub const TIDB_EDITION: &str = match option_env!("TIDB_EDITION") {
    Some(v) => v,
    None => COMMUNITY_EDITION,
};
/// Immutable compile-time approximation of Go `TiDBEnterpriseExtensionGitHash`.
pub const TIDB_ENTERPRISE_EXTENSION_GIT_HASH: &str =
    match option_env!("TIDB_ENTERPRISE_EXTENSION_GIT_HASH") {
        Some(v) => v,
        None => "",
    };

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_storage_class_and_override_boundaries_are_exact() {
        assert_eq!(COMMUNITY_EDITION, "Community");
        assert_eq!(
            TIDB_BUILD_TS,
            option_env!("TIDB_BUILD_TS").unwrap_or("None")
        );
        assert_eq!(
            TIDB_GIT_HASH,
            option_env!("TIDB_GIT_HASH").unwrap_or("None")
        );
        assert_eq!(
            TIDB_GIT_BRANCH,
            option_env!("TIDB_GIT_BRANCH").unwrap_or("None")
        );
        assert_eq!(
            TIDB_EDITION,
            option_env!("TIDB_EDITION").unwrap_or(COMMUNITY_EDITION)
        );
        assert_eq!(
            TIDB_ENTERPRISE_EXTENSION_GIT_HASH,
            option_env!("TIDB_ENTERPRISE_EXTENSION_GIT_HASH").unwrap_or("")
        );
    }
}
