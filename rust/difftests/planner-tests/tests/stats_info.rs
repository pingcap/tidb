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

//! Dependency-closed tests for `pkg/planner/property/stats_info.go:43`.
//!
//! The Go statistics anchor is `TestGroupNDVs` at
//! `pkg/planner/core/casetest/stats_test.go:35`; these vectors isolate the
//! row-count/NDV limit arithmetic from histogram and planner ownership.

use std::collections::BTreeMap;

use tidb_planner::stats_info::StatsInfo;

#[test]
fn derive_limit_stats_caps_rows_and_ndvs() {
    let stats = StatsInfo::new(100.0, [(1, 80.0), (2, 120.0)]);
    let limited = stats.derive_limit_stats(40.0);
    assert_eq!(limited.row_count(), 40.0);
    assert_eq!(limited.col_ndvs(), &BTreeMap::from([(1, 40.0), (2, 40.0)]));
    assert_eq!(stats.row_count(), 100.0);
    assert_eq!(stats.count(), 100);
}

#[test]
fn limit_above_rows_preserves_profile_and_count_truncates() {
    let stats = StatsInfo::new(42.75, [(7, 12.5)]);
    assert_eq!(stats.derive_limit_stats(100.0), stats);
    assert_eq!(stats.count(), 42);
}
