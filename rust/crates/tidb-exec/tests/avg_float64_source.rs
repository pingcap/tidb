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

//! Source-backed tests for bounded non-DISTINCT float64 `AVG` state.

use tidb_exec::aggregate::runtime::AvgFloat64State;

#[path = "core_aggregate_runtime_source.rs"]
mod core_aggregate_runtime_source;

fn assert_close(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("non-empty average");
    assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
}

#[test]
fn avg_float64_update_and_empty_result_match_source() {
    // Source: pkg/executor/aggfuncs/func_avg.go:366-374, 400-415.
    // Direct Go coverage: pkg/executor/aggfuncs/func_avg_test.go:37
    // (TestAvg, float64 subcase).
    let mut state = AvgFloat64State::new();
    assert_eq!(state.result(), None);
    state.update(&[None]);
    assert_eq!(state.result(), None);
    state.update(&[Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
    assert_close(state.result(), 2.0);
}

#[test]
fn avg_float64_merge_preserves_sum_and_count() {
    // Source: pkg/executor/aggfuncs/func_avg.go:478-482.
    // Direct Go coverage: pkg/executor/aggfuncs/func_avg_test.go:27
    // (TestMergePartialResult4Avg, float64 subcase).
    let mut destination = AvgFloat64State::new();
    destination.update(&[Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
    let mut source = AvgFloat64State::new();
    source.update(&[Some(2.0), Some(3.0), Some(4.0)]);
    destination.merge_from(&source);
    assert_close(destination.result(), 2.375);
    destination.merge_from(&AvgFloat64State::new());
    assert_close(destination.result(), 2.375);
}

#[test]
fn avg_float64_slide_adds_incoming_before_outgoing() {
    // Source: pkg/executor/aggfuncs/func_avg.go:423-447.
    // This is the source sliding-order contract; the typed executor callback
    // and chunk lifecycle remain external.
    let mut state = AvgFloat64State::new();
    state.update(&[Some(1.0), Some(2.0)]);
    state.slide(&[Some(1.0)], &[Some(3.0)]);
    assert_close(state.result(), 2.5);
}

#[test]
fn avg_float64_reset_and_partial_state_size_match_source() {
    // Source: pkg/executor/aggfuncs/func_avg.go:351-364.
    // Direct Go coverage: pkg/executor/aggfuncs/func_avg_test.go:48
    // (TestMemAvg, non-DISTINCT float64 state subcase); spill/allocator
    // accounting remains external.
    assert_eq!(
        AvgFloat64State::partial_state_size(),
        std::mem::size_of::<AvgFloat64State>()
    );
    let mut state = AvgFloat64State::new();
    state.update(&[Some(5.0)]);
    state.reset();
    assert_eq!(state.result(), None);
}
