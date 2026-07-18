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

//! Source-backed tests for non-DISTINCT float64 `SUM` state.

use tidb_exec::aggregate::runtime::SumFloat64State;

fn assert_close(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("non-empty sum");
    assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
}

#[test]
fn sum_float64_update_and_final_vectors_match_source() {
    // Source: pkg/executor/aggfuncs/func_sum.go:75-112.
    // Direct Go coverage: pkg/executor/aggfuncs/func_sum_test.go:50
    // (TestSum), with generated values 0..4 and NULL-on-empty behavior.
    let mut state = SumFloat64State::new();
    assert_eq!(state.result(), None);
    state.update(&[None]);
    assert_eq!(state.result(), None);
    state.update(&[Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
    assert_close(state.result(), 10.0);
}

#[test]
fn sum_float64_partial_merge_and_empty_source_match_source() {
    // Source: pkg/executor/aggfuncs/func_sum.go:113-121.
    // Direct Go coverage: pkg/executor/aggfuncs/func_sum_test.go:33
    // (TestMergePartialResult4Sum), non-DISTINCT float64 case.
    let mut destination = SumFloat64State::new();
    destination.update(&[Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
    let mut source = SumFloat64State::new();
    source.update(&[Some(9.0)]);
    destination.merge_from(&source);
    assert_close(destination.result(), 19.0);
    destination.merge_from(&SumFloat64State::new());
    assert_close(destination.result(), 19.0);

    let mut empty = SumFloat64State::new();
    empty.merge_from(&destination);
    assert_close(empty.result(), 19.0);
}

#[test]
fn sum_float64_reset_and_partial_state_size_match_source() {
    // Source: pkg/executor/aggfuncs/func_sum.go:24-42.
    // Direct Go coverage: pkg/executor/aggfuncs/func_sum_test.go:66
    // (TestMemSum). Decimal/int/distinct state sizes remain external.
    assert_eq!(
        SumFloat64State::partial_state_size(),
        std::mem::size_of::<SumFloat64State>()
    );
    let mut state = SumFloat64State::new();
    state.update(&[Some(5.0)]);
    state.reset();
    assert_eq!(state.result(), None);
}
