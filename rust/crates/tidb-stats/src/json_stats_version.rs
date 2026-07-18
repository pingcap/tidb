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

//! Statistics-version fallback for JSON loading from
//! `pkg/statistics/handle/storage/json.go`.
//!
//! Older JSON statistics may omit `stats_ver`. TiDB treats an explicit value
//! as authoritative, infers legacy version 1 when histogram NDV or null-count
//! metadata is present, and otherwise retains version 0. This leaf owns only
//! that scalar compatibility rule; JSON decoding, histogram/sketch payloads,
//! schema matching, and storage/session lifecycle remain external.

/// Version used when no analyzed statistics metadata is available.
pub const JSON_STATS_VERSION_0: i64 = 0;

/// Legacy version inferred from old JSON metadata.
pub const JSON_STATS_VERSION_1: i64 = 1;

/// Resolves a JSON column/index statistics version.
///
/// An explicitly encoded version wins, including zero and negative values.
/// When absent, positive NDV or null-count metadata identifies the legacy
/// analyzed representation; all other inputs remain version zero.
#[must_use]
pub const fn json_stats_version(
    explicit_version: Option<i64>,
    histogram_ndv: i64,
    null_count: i64,
) -> i64 {
    match explicit_version {
        Some(version) => version,
        None if histogram_ndv > 0 || null_count > 0 => JSON_STATS_VERSION_1,
        None => JSON_STATS_VERSION_0,
    }
}
