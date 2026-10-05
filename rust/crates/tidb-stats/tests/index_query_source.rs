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

//! Source-backed tests for index byte-query precedence.

use tidb_stats::query_index_bytes;

#[test]
fn source_index_query_prefers_topn_then_cms_then_histogram() {
    assert_eq!(query_index_bytes(Some(10), Some(20), 30), 10);
    assert_eq!(query_index_bytes(None, Some(20), 30), 20);
    assert_eq!(query_index_bytes(None, None, 30), 30);
}

#[test]
fn source_index_query_matches_histogram_test_fallback_values() {
    // TestIndexQueryBytes exercises histogram equal-row counts when both
    // TopN and CMSketch are nil: low -> 1, repeat -> 10.
    assert_eq!(query_index_bytes(None, None, 1), 1);
    assert_eq!(query_index_bytes(None, None, 10), 10);
}

#[test]
fn source_index_query_allows_unmatched_topn_to_fall_through() {
    assert_eq!(query_index_bytes(None, Some(7), 10), 7);
    assert_eq!(query_index_bytes(None, None, 10), 10);
}
