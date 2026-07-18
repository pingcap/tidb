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

//! Source-backed tests for statistics-cache version advancement.

use tidb_stats::max_stats_cache_version;

#[test]
fn source_stats_cache_version_is_monotonic() {
    assert_eq!(max_stats_cache_version(0, &[2_000], false), 2_000);
    assert_eq!(max_stats_cache_version(2_000, &[1_000], false), 2_000);
    assert_eq!(max_stats_cache_version(2_000, &[3_004], false), 3_004);
    assert_eq!(max_stats_cache_version(3_004, &[], false), 3_004);
}

#[test]
fn source_stats_cache_version_honors_skip_move_forward() {
    assert_eq!(max_stats_cache_version(2_000, &[4_000, 5_000], true), 2_000);
    assert_eq!(max_stats_cache_version(2_000, &[], true), 2_000);
}

#[test]
fn source_stats_cache_version_uses_the_largest_batch_value() {
    assert_eq!(max_stats_cache_version(10, &[11, 100, 42, 99], false), 100);
}
