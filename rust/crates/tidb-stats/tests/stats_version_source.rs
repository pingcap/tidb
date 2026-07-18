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

//! Source-backed tests for statistics-version metadata predicates.

use tidb_stats::{is_analyzed, is_column_analyzed_or_synthesized, VERSION_0, VERSION_1, VERSION_2};

#[test]
fn source_version_constants_and_analyzed_predicate_match() {
    assert_eq!(VERSION_0, 0);
    assert_eq!(VERSION_1, 1);
    assert_eq!(VERSION_2, 2);
    assert!(!is_analyzed(VERSION_0));
    assert!(is_analyzed(VERSION_1));
    assert!(is_analyzed(VERSION_2));
    assert!(is_analyzed(-1));
    assert!(is_analyzed(99));
}

#[test]
fn source_column_predicate_accepts_analyzed_or_synthesized_stats() {
    assert!(!is_column_analyzed_or_synthesized(VERSION_0, 0, 0));
    assert!(is_column_analyzed_or_synthesized(VERSION_0, 1, 0));
    assert!(is_column_analyzed_or_synthesized(VERSION_0, 0, 1));
    assert!(is_column_analyzed_or_synthesized(VERSION_1, 0, 0));
    assert!(is_column_analyzed_or_synthesized(VERSION_2, -1, -1));
}

#[test]
fn source_nonpositive_synthetic_counts_do_not_mark_version_zero_available() {
    assert!(!is_column_analyzed_or_synthesized(VERSION_0, -1, 0));
    assert!(!is_column_analyzed_or_synthesized(VERSION_0, 0, -1));
    assert!(!is_column_analyzed_or_synthesized(VERSION_0, -1, -1));
}
