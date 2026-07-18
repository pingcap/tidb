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

//! Source-backed tests for the auto-analyze trigger policy.

use tidb_stats::need_analyze_table;

#[test]
fn source_unanalyzed_tables_always_trigger() {
    assert_eq!(
        need_analyze_table(false, 1, -1.0, 0, 0.0),
        (true, String::from("table unanalyzed"))
    );
    assert_eq!(
        need_analyze_table(false, 0, -1.0, 0, 0.3),
        (true, String::from("table unanalyzed"))
    );
}

#[test]
fn source_zero_ratio_disables_reanalysis_for_analyzed_tables() {
    assert_eq!(
        need_analyze_table(true, 1, 1.0, 1, 0.0),
        (false, String::new())
    );
}

#[test]
fn source_small_modify_count_does_not_trigger() {
    assert_eq!(
        need_analyze_table(true, 1, 1.0, 0, 0.3),
        (false, String::new())
    );
}

#[test]
fn source_large_modify_count_triggers_with_reason() {
    let (needed, reason) = need_analyze_table(true, 1, 1.0, 1, 0.3);
    assert!(needed);
    assert_eq!(reason, "too many modifications(1/1>0.3)");
}

#[test]
fn source_positive_analyze_count_replaces_realtime_count() {
    let (needed, reason) = need_analyze_table(true, 100, 10.0, 4, 0.3);
    assert!(needed);
    assert_eq!(reason, "too many modifications(4/10>0.3)");
}

#[test]
fn source_nonpositive_analyze_count_falls_back_to_realtime_count() {
    assert_eq!(
        need_analyze_table(true, 100, 0.0, 20, 0.3),
        (false, String::new())
    );
}
