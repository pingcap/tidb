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

//! Source-backed tests for the legacy JSON statistics-version fallback.

use tidb_stats::{json_stats_version, JSON_STATS_VERSION_0, JSON_STATS_VERSION_1};

#[test]
fn source_json_stats_version_handles_old_metadata() {
    // TestLoadStatsFromOldVersion supplies no stats_ver and zero NDV/null
    // count, so the loaded objects retain version zero.
    assert_eq!(json_stats_version(None, 0, 0), JSON_STATS_VERSION_0);
    assert_eq!(json_stats_version(None, 3, 0), JSON_STATS_VERSION_1);
    assert_eq!(json_stats_version(None, 0, 1), JSON_STATS_VERSION_1);
}

#[test]
fn source_json_stats_version_prefers_explicit_values() {
    assert_eq!(json_stats_version(Some(2), 0, 0), 2);
    assert_eq!(json_stats_version(Some(0), 10, 10), 0);
    assert_eq!(json_stats_version(Some(-1), 10, 10), -1);
}

#[test]
fn source_json_stats_version_requires_positive_legacy_metadata() {
    assert_eq!(json_stats_version(None, -1, 0), JSON_STATS_VERSION_0);
    assert_eq!(json_stats_version(None, 0, -1), JSON_STATS_VERSION_0);
    assert_eq!(json_stats_version(None, -1, -1), JSON_STATS_VERSION_0);
}
