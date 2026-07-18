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

//! Source-backed tests for bounded integer window-value state.

use tidb_exec::window_value_int::{FirstValueIntState, LastValueIntState, NthValueIntState};

#[test]
fn first_value_captures_first_row_once_including_null() {
    // Source: pkg/executor/aggfuncs/func_value.go:283-311.
    // Direct Go coverage: pkg/executor/aggfuncs/func_value_test.go:63
    // (TestMemValue, FIRST_VALUE integer evaluator subcase).
    let mut state = FirstValueIntState::new();
    state.update(&[]);
    assert!(!state.is_selected());
    state.update(&[None, Some(2)]);
    assert!(state.is_selected());
    assert_eq!(state.result(), None);
    state.update(&[Some(9)]);
    assert_eq!(state.result(), None);
    state.reset();
    state.update(&[Some(7)]);
    assert_eq!(state.result(), Some(7));
}

#[test]
fn last_value_overwrites_with_last_row_in_each_batch() {
    // Source: pkg/executor/aggfuncs/func_value.go:330-355.
    // Direct Go coverage: pkg/executor/aggfuncs/func_value_test.go:63
    // (TestMemValue, LAST_VALUE integer evaluator subcase).
    let mut state = LastValueIntState::new();
    state.update(&[]);
    assert!(!state.is_selected());
    state.update(&[Some(1), Some(2)]);
    assert_eq!(state.result(), Some(2));
    state.update(&[None]);
    assert!(state.is_selected());
    assert_eq!(state.result(), None);
}

#[test]
fn nth_value_counts_rows_across_batches_and_handles_unreached_or_zero() {
    // Source: pkg/executor/aggfuncs/func_value.go:375-404.
    // Direct Go coverage: pkg/executor/aggfuncs/func_value_test.go:63
    // (TestMemValue, NTH_VALUE integer evaluator subcases n=2 and n=5).
    let mut state = NthValueIntState::new(2);
    state.update(&[Some(10)]);
    assert_eq!(state.result(), None);
    assert_eq!(state.seen_rows(), 1);
    state.update(&[Some(20), Some(30)]);
    assert_eq!(state.result(), Some(20));
    state.reset();
    state.update(&[Some(10), None]);
    assert_eq!(state.result(), None);
    assert_eq!(state.seen_rows(), 2);

    let mut unreached = NthValueIntState::new(5);
    unreached.update(&[Some(1), Some(2), Some(3)]);
    assert_eq!(unreached.result(), None);
    let mut zero = NthValueIntState::new(0);
    zero.update(&[Some(1), Some(2)]);
    assert_eq!(zero.seen_rows(), 0);
    assert_eq!(zero.result(), None);
}

#[test]
fn window_value_partial_state_sizes_are_stable() {
    // Source: pkg/executor/aggfuncs/func_value.go:277-280, 324-327, 369-373.
    // Typed evaluator allocation and Go memory-delta accounting remain
    // external to this already-evaluated int state.
    assert_eq!(
        FirstValueIntState::partial_state_size(),
        std::mem::size_of::<FirstValueIntState>()
    );
    assert_eq!(
        LastValueIntState::partial_state_size(),
        std::mem::size_of::<LastValueIntState>()
    );
    assert_eq!(
        NthValueIntState::partial_state_size(),
        std::mem::size_of::<NthValueIntState>()
    );
}
