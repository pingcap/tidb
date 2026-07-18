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

//! Source-backed tests for bounded integer/real `APPROX_PERCENTILE` state.

use tidb_exec::percentile::{PercentileIntState, PercentileRealState};

#[test]
fn percentile_int_update_and_empty_result_match_source() {
    // Source: pkg/executor/aggfuncs/func_percentile.go:139-174.
    // Direct Go coverage: pkg/executor/aggfuncs/func_percentile_test.go:35
    // (TestPercentile, integer subcase; source supplies P=50).
    let mut state = PercentileIntState::new();
    assert_eq!(state.finish(50), None);
    state.update(&[None, Some(0), Some(1), Some(2), Some(3), Some(4)]);
    assert_eq!(state.finish(50), Some(2));
    assert_eq!(state.len(), 5);
}

#[test]
fn percentile_real_update_and_empty_result_match_source() {
    // Source: pkg/executor/aggfuncs/func_percentile.go:192-227.
    // Direct Go coverage: pkg/executor/aggfuncs/func_percentile_test.go:35
    // (TestPercentile, float/real subcases; source supplies P=50).
    let mut state = PercentileRealState::new();
    state.update(&[None, Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
    assert_eq!(state.finish(50), Some(2.0));
    state.reset();
    assert_eq!(state.finish(50), None);
}

#[test]
fn percentile_merge_keeps_destination_then_source_and_clears_source() {
    // Source: pkg/executor/aggfuncs/func_percentile.go:156-164, 209-217.
    // Direct Go coverage: pkg/executor/aggfuncs/func_percentile_test.go:35
    // (TestPercentile's partial/final source shape).
    let mut destination = PercentileIntState::new();
    destination.update(&[Some(0), Some(1), Some(2)]);
    let mut source = PercentileIntState::new();
    source.update(&[Some(3), Some(4)]);
    destination.merge_from(&mut source);
    assert!(source.is_empty());
    assert_eq!(destination.finish(50), Some(2));

    let mut real_destination = PercentileRealState::new();
    real_destination.update(&[Some(0.0), Some(1.0), Some(2.0)]);
    let mut real_source = PercentileRealState::new();
    real_source.update(&[Some(3.0), Some(4.0)]);
    real_destination.merge_from(&mut real_source);
    assert!(real_source.is_empty());
    assert_eq!(real_destination.finish(100), Some(4.0));
}

#[test]
fn percentile_index_100_matches_fix26807_selection_boundary() {
    // Source: pkg/executor/aggfuncs/func_percentile.go:40-44.
    // Direct Go coverage: pkg/executor/aggfuncs/func_percentile_test.go:51
    // (TestFix26807), repeated P=100 selection of the largest element.
    let mut values: Vec<i64> = (1..=28).collect();
    for _ in 0..10 {
        let index = PercentileIntState::select_index(&mut values, 100).unwrap();
        assert_eq!(index, 27);
        assert_eq!(values[index], 28);
    }
}

#[test]
fn percentile_state_size_and_unsupported_variants_are_explicit() {
    // Direct Go coverage: pkg/executor/aggfuncs/func_percentile_test.go:63
    // (TestFix40463). Enum/set string routing returns NULL in Go's base
    // percentile owner and remains external to this typed leaf.
    assert_eq!(
        PercentileIntState::partial_state_size(),
        std::mem::size_of::<PercentileIntState>()
    );
    assert_eq!(
        PercentileRealState::partial_state_size(),
        std::mem::size_of::<PercentileRealState>()
    );
    assert!(PercentileIntState::new().is_empty());
    assert!(PercentileRealState::new().is_empty());
}
