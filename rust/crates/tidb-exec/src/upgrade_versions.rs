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

//! Bootstrap upgrade-version registry from `pkg/session/upgrade_def.go`.
//!
//! TiDB runs versioned upgrade functions in ascending order, intentionally
//! leaving historical versions absent when a later version redoes their work.
//! This leaf preserves the declared versions, the ordered registry/gap
//! contract, and the `upgradeToVer<N>` naming boundary. It does not carry Go
//! function pointers, execute upgrade SQL, mutate bootstrap metadata, or
//! perform retries.

/// Current bootstrap version in the source registry.
pub const CURRENT_BOOTSTRAP_VERSION: i64 = 263;

/// Every top-level `version<N>` constant declared by the owning Go source.
///
/// This is deliberately wider than [`REGISTERED_UPGRADE_VERSIONS`]: versions
/// 92, 99, and 145 are declaration-only markers in Go.
pub const DECLARED_BOOTSTRAP_VERSIONS: &[i64] = &[
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
    28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 40, 41, 42, 43, 44, 45, 46, 47, 50, 52, 53, 54, 55,
    56, 57, 59, 60, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81,
    82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 97, 98, 99, 100, 101, 102, 103, 104,
    105, 106, 107, 108, 109, 110, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142,
    143, 144, 145, 146, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 190, 191,
    192, 193, 194, 195, 196, 197, 198, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 239, 240,
    241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259,
    260, 261, 262, 263,
];

/// Exact ordered `upgradeToVerFunctions` version projection from Go.
pub const REGISTERED_UPGRADE_VERSIONS: &[i64] = &[
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
    28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 40, 41, 42, 43, 44, 45, 46, 47, 50, 52, 53, 54, 55,
    56, 57, 59, 60, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81,
    82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 93, 94, 95, 97, 98, 100, 101, 102, 103, 104, 105, 106,
    107, 108, 109, 110, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144,
    146, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 190, 191, 192, 193, 194,
    195, 196, 197, 198, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 239, 240, 241, 242, 243,
    244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262,
    263,
];

/// Returns the ordered versions whose upgrade functions are registered.
#[must_use]
pub fn upgrade_versions() -> Vec<i64> {
    REGISTERED_UPGRADE_VERSIONS.to_vec()
}

/// Returns the source function name for one registered version.
#[must_use]
pub fn upgrade_function_name(version: i64) -> String {
    format!("upgradeToVer{version}")
}

/// Returns the source function name only when the version is registered.
#[must_use]
pub fn registered_upgrade_function_name(version: i64) -> Option<String> {
    REGISTERED_UPGRADE_VERSIONS
        .binary_search(&version)
        .ok()
        .map(|_| upgrade_function_name(version))
}

/// Returns true only for the exact source registry, including every gap.
#[must_use]
pub fn is_valid_upgrade_registry(versions: &[i64]) -> bool {
    versions == REGISTERED_UPGRADE_VERSIONS
}
