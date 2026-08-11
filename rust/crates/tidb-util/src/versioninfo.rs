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
//! Go injects the five build values with linker flags, then may replace the
//! edition from startup configuration. Rust captures the injected defaults at
//! compile time and carries one immutable [`VersionInfo`] snapshot from startup
//! into every connection. That keeps one coherent identity without adding a
//! mutable process global.

/// The default edition for building (Go `CommunityEdition`).
pub const COMMUNITY_EDITION: &str = "Community";

/// Build-time value of Go `TiDBBuildTS`.
pub const TIDB_BUILD_TS: &str = match option_env!("TIDB_BUILD_TS") {
    Some(v) => v,
    None => "None",
};
/// Build-time value of Go `TiDBGitHash`.
pub const TIDB_GIT_HASH: &str = match option_env!("TIDB_GIT_HASH") {
    Some(v) => v,
    None => "None",
};
/// Build-time value of Go `TiDBGitBranch`.
pub const TIDB_GIT_BRANCH: &str = match option_env!("TIDB_GIT_BRANCH") {
    Some(v) => v,
    None => "None",
};
/// Build-time default of Go `TiDBEdition`.
pub const TIDB_EDITION: &str = match option_env!("TIDB_EDITION") {
    Some(v) => v,
    None => COMMUNITY_EDITION,
};
/// Build-time value of Go `TiDBEnterpriseExtensionGitHash`.
pub const TIDB_ENTERPRISE_EXTENSION_GIT_HASH: &str =
    match option_env!("TIDB_ENTERPRISE_EXTENSION_GIT_HASH") {
        Some(v) => v,
        None => "",
    };

/// One server process's coherent build and edition identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionInfo {
    /// UTC build timestamp.
    pub build_ts: String,
    /// Git commit hash.
    pub git_hash: String,
    /// Git branch.
    pub git_branch: String,
    /// Product edition, overridden by a non-empty startup `tidb-edition`.
    pub edition: String,
    /// Enterprise extension commit hash, empty for builds without it.
    pub enterprise_extension_git_hash: String,
}

impl Default for VersionInfo {
    fn default() -> Self {
        Self::build_default()
    }
}

impl VersionInfo {
    /// Captures the five build-injected package values.
    #[must_use]
    pub fn build_default() -> Self {
        Self {
            build_ts: TIDB_BUILD_TS.to_owned(),
            git_hash: TIDB_GIT_HASH.to_owned(),
            git_branch: TIDB_GIT_BRANCH.to_owned(),
            edition: TIDB_EDITION.to_owned(),
            enterprise_extension_git_hash: TIDB_ENTERPRISE_EXTENSION_GIT_HASH.to_owned(),
        }
    }

    /// Applies Go startup's non-empty `cfg.TiDBEdition` override.
    #[must_use]
    pub fn with_configured_edition(mut self, edition: &str) -> Self {
        if !edition.is_empty() {
            self.edition = edition.to_owned();
        }
        self
    }

    /// Builds the read-only `version_comment` system-variable value.
    #[must_use]
    pub fn version_comment(&self) -> String {
        format!(
            "TiDB Server (Apache License 2.0) {} Edition, MySQL 8.0 compatible",
            self.edition
        )
    }
}

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

        let build = VersionInfo::build_default();
        assert_eq!(build.build_ts, TIDB_BUILD_TS);
        assert_eq!(build.git_hash, TIDB_GIT_HASH);
        assert_eq!(build.git_branch, TIDB_GIT_BRANCH);
        assert_eq!(build.edition, TIDB_EDITION);
        assert_eq!(
            build.enterprise_extension_git_hash,
            TIDB_ENTERPRISE_EXTENSION_GIT_HASH
        );
        assert_eq!(
            build.clone().with_configured_edition("").edition,
            build.edition
        );
        assert_eq!(
            build.with_configured_edition("Starter").version_comment(),
            "TiDB Server (Apache License 2.0) Starter Edition, MySQL 8.0 compatible"
        );
    }
}
