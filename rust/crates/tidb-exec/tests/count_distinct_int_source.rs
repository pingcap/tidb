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

//! Source-backed tests for bounded typed-int `COUNT(DISTINCT ...)` state.

use tidb_exec::aggregate::runtime::CountDistinctIntState;

#[test]
fn count_distinct_int_update_skips_null_and_deduplicates() {
    // Source: pkg/executor/aggfuncs/func_count_distinct.go:70-103.
    // Direct Go coverage: pkg/executor/aggfuncs/func_distinct_agg_test.go:26
    // (TestParallelDistinctCount, signed integer subcase).
    let mut state = CountDistinctIntState::new();
    state.update(&[None, Some(1), Some(2), Some(1), None, Some(-1)]);
    assert_eq!(state.result(), 3);
    assert_eq!(state.len(), 3);
}

#[test]
fn count_distinct_int_merge_preserves_source_and_union_cardinality() {
    // Source: pkg/executor/aggfuncs/func_count_distinct.go:105-124.
    // Direct Go coverage: pkg/executor/aggfuncs/func_distinct_agg_test.go:26
    // (TestParallelDistinctCount partial/final int path).
    let mut destination = CountDistinctIntState::new();
    destination.update(&[Some(1), Some(2)]);
    let mut source = CountDistinctIntState::new();
    source.update(&[Some(2), Some(3), Some(4)]);
    destination.merge_from(&source);
    assert_eq!(destination.result(), 4);
    assert_eq!(source.result(), 3);
    assert_eq!(destination.result(), source.result() + 1);
}

#[test]
fn count_distinct_int_reset_and_empty_result_match_source() {
    // Source: pkg/executor/aggfuncs/func_count_distinct.go:74-87.
    // Direct Go coverage: pkg/executor/aggfuncs/func_count_test.go:115
    // (TestMemCount, distinct integer state subcase).
    let mut state = CountDistinctIntState::new();
    assert_eq!(state.result(), 0);
    state.update(&[Some(9)]);
    state.reset();
    assert!(state.is_empty());
    assert_eq!(state.result(), 0);
    assert_eq!(
        CountDistinctIntState::partial_state_size(),
        std::mem::size_of::<CountDistinctIntState>()
    );
}

#[test]
fn count_distinct_int_merge_empty_source_is_noop() {
    // Source: pkg/executor/aggfuncs/func_count_distinct.go:105-124.
    let mut state = CountDistinctIntState::new();
    state.update(&[Some(1), Some(2)]);
    state.merge_from(&CountDistinctIntState::new());
    assert_eq!(state.result(), 2);
}
