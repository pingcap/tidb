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

//! Source-backed tests for analyze-version matching policy.

use tidb_stats::analyze_version_matches;

#[test]
fn source_nil_and_pseudo_stats_always_match() {
    assert!(analyze_version_matches(None, false, 2));
    assert!(analyze_version_matches(Some(1), true, 2));
    assert!(analyze_version_matches(Some(0), true, 2));
}

#[test]
fn source_unanalyzed_or_requested_version_matches() {
    assert!(analyze_version_matches(Some(0), false, 2));
    assert!(analyze_version_matches(Some(2), false, 2));
    assert!(analyze_version_matches(Some(-1), false, -1));
}

#[test]
fn source_analyzed_different_version_does_not_match() {
    assert!(!analyze_version_matches(Some(1), false, 2));
    assert!(!analyze_version_matches(Some(3), false, 2));
}
