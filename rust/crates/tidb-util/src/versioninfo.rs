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

//! Build identity globals from Go `pkg/util/versioninfo`.

use std::sync::{OnceLock, RwLock};

/// Go `CommunityEdition`.
pub const COMMUNITY_EDITION: &str = "Community";

/// Go `TiDBBuildTS`, injected when the binary is built.
pub const TIDB_BUILD_TS: &str = env!("TIDB_BUILD_TS");
/// Go `TiDBGitHash`, injected when the binary is built.
pub const TIDB_GIT_HASH: &str = env!("TIDB_GIT_HASH");
/// Go `TiDBGitBranch`, injected when the binary is built.
pub const TIDB_GIT_BRANCH: &str = env!("TIDB_GIT_BRANCH");
/// Go `TiDBEnterpriseExtensionGitHash`, injected when the binary is built.
pub const TIDB_ENTERPRISE_EXTENSION_GIT_HASH: &str = env!("TIDB_ENTERPRISE_EXTENSION_GIT_HASH");

static TIDB_EDITION: OnceLock<RwLock<String>> = OnceLock::new();

fn edition_state() -> &'static RwLock<String> {
    TIDB_EDITION.get_or_init(|| RwLock::new(env!("TIDB_EDITION").to_owned()))
}

/// Returns Go's process-wide mutable `TiDBEdition`.
#[must_use]
pub fn tidb_edition() -> String {
    edition_state()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Assigns Go's process-wide mutable `TiDBEdition`.
pub fn set_tidb_edition(edition: impl Into<String>) {
    *edition_state()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = edition.into();
}
