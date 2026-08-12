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
pub const TIDB_BUILD_TS: &str = env!("TIDB_BUILD_TS");
/// Build-time value of Go `TiDBGitHash`.
pub const TIDB_GIT_HASH: &str = env!("TIDB_GIT_HASH");
/// Build-time value of Go `TiDBGitBranch`.
pub const TIDB_GIT_BRANCH: &str = env!("TIDB_GIT_BRANCH");
/// Build-time default of Go `TiDBEdition`.
pub const TIDB_EDITION: &str = env!("TIDB_EDITION");
/// Build-time value of Go `TiDBEnterpriseExtensionGitHash`.
pub const TIDB_ENTERPRISE_EXTENSION_GIT_HASH: &str = env!("TIDB_ENTERPRISE_EXTENSION_GIT_HASH");

/// Compiler/runtime identity captured by this crate's build script.
pub const RUST_VERSION: &str = env!("TIDB_RUST_VERSION");

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
    /// TiDB release version used by the MySQL handshake and status surfaces.
    pub release_version: String,
    /// MySQL-compatible server version used by `VERSION()` and the handshake.
    pub server_version: String,
    /// Compiler/runtime version for this Rust binary.
    pub runtime_version: String,
    /// Whether startup's linker flag requires table validation before drop.
    pub check_table_before_drop: bool,
    /// Configured storage backend.
    pub store: String,
    /// Compile-time kernel type (`Classic` or `Next Generation`).
    pub kernel_type: String,
    /// Next-generation deployment mode; absent for the classic kernel.
    pub deploy_mode: Option<String>,
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
        let runtime_versions = tidb_mysql::runtime_versions();
        Self {
            build_ts: TIDB_BUILD_TS.to_owned(),
            git_hash: TIDB_GIT_HASH.to_owned(),
            git_branch: TIDB_GIT_BRANCH.to_owned(),
            edition: TIDB_EDITION.to_owned(),
            enterprise_extension_git_hash: TIDB_ENTERPRISE_EXTENSION_GIT_HASH.to_owned(),
            release_version: runtime_versions.tidb_release_version,
            server_version: runtime_versions.server_version,
            runtime_version: RUST_VERSION.to_owned(),
            check_table_before_drop: false,
            store: "tikv".to_owned(),
            kernel_type: "Classic".to_owned(),
            deploy_mode: None,
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

    /// Applies Go startup's non-empty classic-kernel version overrides.
    #[must_use]
    pub fn with_configured_versions(mut self, release_version: &str, server_version: &str) -> Self {
        if !release_version.is_empty() {
            self.release_version = release_version.to_owned();
        }
        if !server_version.is_empty() {
            self.server_version = server_version.to_owned();
        }
        self
    }

    /// Captures the startup settings printed beside the immutable build identity.
    #[must_use]
    pub fn with_runtime_environment(
        mut self,
        check_table_before_drop: bool,
        store: impl Into<String>,
        kernel_type: impl Into<String>,
        deploy_mode: Option<String>,
    ) -> Self {
        self.check_table_before_drop = check_table_before_drop;
        self.store = store.into();
        self.kernel_type = kernel_type.into();
        self.deploy_mode = deploy_mode;
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
            build.release_version,
            tidb_mysql::runtime_versions().tidb_release_version
        );
        assert_eq!(
            build.server_version,
            tidb_mysql::runtime_versions().server_version
        );
        assert_eq!(build.runtime_version, RUST_VERSION);
        assert!(!build.check_table_before_drop);
        assert_eq!(build.store, "tikv");
        assert_eq!(build.kernel_type, "Classic");
        assert_eq!(build.deploy_mode, None);
        assert_eq!(
            build.clone().with_configured_edition("").edition,
            build.edition
        );
        assert_eq!(
            build.with_configured_edition("Starter").version_comment(),
            "TiDB Server (Apache License 2.0) Starter Edition, MySQL 8.0 compatible"
        );

        let configured = VersionInfo::build_default().with_runtime_environment(
            true,
            "tikv",
            "Next Generation",
            Some("starter".to_owned()),
        );
        assert!(configured.check_table_before_drop);
        assert_eq!(configured.kernel_type, "Next Generation");
        assert_eq!(configured.deploy_mode.as_deref(), Some("starter"));

        let overridden = configured.with_configured_versions("v9.0.0", "8.0.11-TiDB-v9.0.0");
        assert_eq!(overridden.release_version, "v9.0.0");
        assert_eq!(overridden.server_version, "8.0.11-TiDB-v9.0.0");
    }

    #[test]
    fn repository_build_captures_the_source_identity() {
        let build = VersionInfo::build_default();
        if let Ok(build_ts) = std::env::var("TIDB_BUILD_TS") {
            assert_eq!(build.build_ts, build_ts);
        } else {
            assert!(
                chrono::NaiveDateTime::parse_from_str(&build.build_ts, "%Y-%m-%d %H:%M:%S").is_ok(),
                "build timestamp: {}",
                build.build_ts
            );
        }
        assert_eq!(
            build.git_hash,
            std::env::var("TIDB_GIT_HASH").unwrap_or_else(|_| {
                String::from_utf8(
                    std::process::Command::new("git")
                        .args(["rev-parse", "HEAD"])
                        .output()
                        .unwrap()
                        .stdout,
                )
                .unwrap()
                .trim()
                .to_owned()
            })
        );
        assert_eq!(
            build.git_branch,
            std::env::var("TIDB_GIT_BRANCH").unwrap_or_else(|_| {
                String::from_utf8(
                    std::process::Command::new("git")
                        .args(["rev-parse", "--abbrev-ref", "HEAD"])
                        .output()
                        .unwrap()
                        .stdout,
                )
                .unwrap()
                .trim()
                .to_owned()
            })
        );
    }
}
