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

//! Source-backed tests for mock statistics fixture shapes.

use tidb_stats::MockStatisticsTableShape;

#[test]
fn source_mock_statistics_shape_matches_lfu_benchmark_fixture() {
    // BenchmarkLFUCachePutGet uses NewMockStatisticsTable(1, 1, true,
    // false, false) for each cache update.
    let shape = MockStatisticsTableShape::new(1, 1, true, false, false);
    assert_eq!(shape.columns, 1);
    assert_eq!(shape.indices, 1);
    assert_eq!(shape.item_count(), 2);
    assert!(shape.with_cms);
    assert!(!shape.with_top_n);
    assert!(!shape.with_hist);
}

#[test]
fn source_mock_statistics_shape_preserves_empty_counts_and_flags() {
    let shape = MockStatisticsTableShape::new(0, 0, false, true, true);
    assert_eq!(shape.item_count(), 0);
    assert_eq!(
        (shape.with_cms, shape.with_top_n, shape.with_hist),
        (false, true, true)
    );
}
