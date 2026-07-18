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

//! Analyze-version matching policy from `pkg/statistics/table.go`.
//!
//! This leaf compares already-materialized table metadata only. The caller
//! owns the source assertion that the requested version is the current
//! Version2, and no analyze scheduler or statistics-handle state is hidden.

/// Returns whether existing table statistics match a requested analyze
/// version.
///
/// `None` models the source nil table. Nil or pseudo stats are considered a
/// match; otherwise only an analyzed, different version is a mismatch.
#[must_use]
pub fn analyze_version_matches(
    stats_version: Option<i64>,
    pseudo: bool,
    requested_version: i64,
) -> bool {
    let Some(stats_version) = stats_version else {
        return true;
    };
    if pseudo {
        return true;
    }
    if stats_version != 0 && stats_version != requested_version {
        return false;
    }
    true
}
