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

//! Hash-join version values from `pkg/executor/join/joinversion/join_version.go`.
//!
//! TiDB distinguishes the legacy hash join (v1) from the optimized hash join
//! (v2). This leaf preserves the source literals and the case-insensitive
//! optimized-version predicate; system-variable validation, session mutation,
//! planner GA gates, and runtime join implementation remain external.

/// Legacy hash-join version (v1).
pub const HASH_JOIN_VERSION_LEGACY: &str = "legacy";

/// Optimized hash-join version (v2).
pub const HASH_JOIN_VERSION_OPTIMIZED: &str = "optimized";

/// TiFlash's default hash-join version.
pub const TIFLASH_HASH_JOIN_VERSION_DEFAULT: &str = HASH_JOIN_VERSION_LEGACY;

/// Returns true when a version selects the optimized hash join.
#[must_use]
pub fn is_optimized_version(version: &str) -> bool {
    version.eq_ignore_ascii_case(HASH_JOIN_VERSION_OPTIMIZED)
}
