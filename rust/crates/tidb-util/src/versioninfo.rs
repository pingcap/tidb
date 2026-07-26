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

//! Transcreation of Go `pkg/util/versioninfo`: build-time version stamps.
//!
//! Go injects these via `-ldflags -X`; the Rust build injects them via
//! `TIDB_*` environment variables at compile time (`option_env!`), with the
//! same defaults when unset.

/// The default edition for building (Go `CommunityEdition`).
pub const COMMUNITY_EDITION: &str = "Community";

/// Build timestamp (Go `TiDBBuildTS`).
pub const TIDB_BUILD_TS: &str = match option_env!("TIDB_BUILD_TS") {
    Some(v) => v,
    None => "None",
};
/// Git commit hash (Go `TiDBGitHash`).
pub const TIDB_GIT_HASH: &str = match option_env!("TIDB_GIT_HASH") {
    Some(v) => v,
    None => "None",
};
/// Git branch (Go `TiDBGitBranch`).
pub const TIDB_GIT_BRANCH: &str = match option_env!("TIDB_GIT_BRANCH") {
    Some(v) => v,
    None => "None",
};
/// Edition (Go `TiDBEdition`).
pub const TIDB_EDITION: &str = match option_env!("TIDB_EDITION") {
    Some(v) => v,
    None => COMMUNITY_EDITION,
};
/// Enterprise extension commit hash (Go `TiDBEnterpriseExtensionGitHash`).
pub const TIDB_ENTERPRISE_EXTENSION_GIT_HASH: &str =
    match option_env!("TIDB_ENTERPRISE_EXTENSION_GIT_HASH") {
        Some(v) => v,
        None => "",
    };
