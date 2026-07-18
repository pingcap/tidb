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
//! This leaf preserves the ordered version/gap contract and the
//! `upgradeToVer<N>` naming boundary. It does not carry function pointers,
//! execute upgrade SQL, mutate bootstrap metadata, or perform retries.

/// Current bootstrap version in the source registry.
pub const CURRENT_BOOTSTRAP_VERSION: i64 = 263;

/// Returns the ordered versions whose upgrade functions are registered.
#[must_use]
pub fn upgrade_versions() -> Vec<i64> {
    let mut versions = Vec::with_capacity(174);
    versions.extend(2_i64..=38_i64);
    versions.extend(40_i64..=47_i64);
    versions.push(50);
    versions.extend(52_i64..=57_i64);
    versions.extend(59_i64..=60_i64);
    versions.extend(62_i64..=91_i64);
    versions.extend(93_i64..=95_i64);
    versions.extend([97, 98]);
    versions.extend(100_i64..=110_i64);
    versions.extend(130_i64..=144_i64);
    versions.push(146);
    versions.extend(167_i64..=179_i64);
    versions.extend(190_i64..=198_i64);
    versions.extend(209_i64..=218_i64);
    versions.extend(239_i64..=263_i64);
    versions
}

/// Returns the source function name for one registered version.
#[must_use]
pub fn upgrade_function_name(version: i64) -> String {
    format!("upgradeToVer{version}")
}

/// Returns true when the registry is strictly ascending and ends at current.
#[must_use]
pub fn is_valid_upgrade_registry(versions: &[i64]) -> bool {
    versions.last() == Some(&CURRENT_BOOTSTRAP_VERSION)
        && versions.windows(2).all(|pair| pair[0] < pair[1])
}
