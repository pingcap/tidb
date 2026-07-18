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

//! Source-backed tests for table analysis eligibility predicates.

use tidb_stats::{
    is_eligible_for_analysis, meets_auto_analyze_min_count, table_is_analyzed,
    DEFAULT_AUTO_ANALYZE_MIN_COUNT,
};

#[test]
fn source_table_analyzed_uses_positive_last_analyze_version() {
    assert_eq!(DEFAULT_AUTO_ANALYZE_MIN_COUNT, 1_000);
    assert!(!table_is_analyzed(0));
    assert!(table_is_analyzed(1));
    assert!(table_is_analyzed(u64::MAX));
}

#[test]
fn source_min_count_preserves_nil_and_threshold_boundaries() {
    assert!(!meets_auto_analyze_min_count(
        None,
        DEFAULT_AUTO_ANALYZE_MIN_COUNT
    ));
    assert!(!meets_auto_analyze_min_count(
        Some(999),
        DEFAULT_AUTO_ANALYZE_MIN_COUNT
    ));
    assert!(meets_auto_analyze_min_count(
        Some(1_000),
        DEFAULT_AUTO_ANALYZE_MIN_COUNT
    ));
    assert!(meets_auto_analyze_min_count(
        Some(1_001),
        DEFAULT_AUTO_ANALYZE_MIN_COUNT
    ));
    assert!(meets_auto_analyze_min_count(Some(-5), -10));
}

#[test]
fn source_eligibility_requires_size_and_nonpseudo_stats() {
    assert!(!is_eligible_for_analysis(
        None,
        false,
        DEFAULT_AUTO_ANALYZE_MIN_COUNT
    ));
    assert!(!is_eligible_for_analysis(
        Some(1_000),
        true,
        DEFAULT_AUTO_ANALYZE_MIN_COUNT
    ));
    assert!(!is_eligible_for_analysis(
        Some(999),
        false,
        DEFAULT_AUTO_ANALYZE_MIN_COUNT
    ));
    assert!(is_eligible_for_analysis(
        Some(1_000),
        false,
        DEFAULT_AUTO_ANALYZE_MIN_COUNT
    ));
}
