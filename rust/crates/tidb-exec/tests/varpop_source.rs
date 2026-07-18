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

//! Source-backed tests for `VAR_POP` float64 state.

use tidb_exec::varpop::VarPopState;

fn assert_close(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("non-empty variance");
    assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
}

#[test]
fn varpop_update_and_final_vectors_match_source() {
    // Source: pkg/executor/aggfuncs/func_varpop.go:58-82.
    // Direct Go coverage: pkg/executor/aggfuncs/func_varpop_test.go:37
    // (TestVarpop), with generated values 0..4 and NULL-on-empty behavior.
    let mut state = VarPopState::new();
    assert_eq!(state.result(), None);
    state.update(&[None]);
    assert_eq!(state.result(), None);
    state.update(&[Some(0.0), Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
    assert_close(state.result(), 2.0);
}

#[test]
fn varpop_partial_merge_and_zero_count_branches_match_source() {
    // Source: pkg/executor/aggfuncs/func_varpop.go:84-111.
    // Direct Go coverage: pkg/executor/aggfuncs/func_varpop_test.go:28
    // (TestMergePartialResult4Varpop).
    let mut destination = VarPopState::new();
    destination.update(&[Some(0.0), Some(1.0)]);
    let mut source = VarPopState::new();
    source.update(&[Some(2.0), Some(3.0), Some(4.0)]);
    destination.merge_from(&source);
    assert_close(destination.result(), 2.0);

    let mut empty = VarPopState::new();
    empty.merge_from(&destination);
    assert_close(empty.result(), 2.0);
    destination.merge_from(&VarPopState::new());
    assert_close(destination.result(), 2.0);
}

#[test]
fn varpop_reset_and_partial_state_size_match_source() {
    // Source: pkg/executor/aggfuncs/func_varpop.go:24-45.
    // Direct Go coverage: pkg/executor/aggfuncs/func_varpop_test.go:46
    // (TestMemVarpop). Distinct-set allocation and typed state variants remain
    // external to this non-DISTINCT float64 owner.
    // The compatibility facade carries the canonical mode/kind authority;
    // the represented Go ordinary partial tuple itself remains exactly
    // count(i64) + sum(f64) + variance(f64) = 24 bytes.
    assert_eq!(VarPopState::partial_state_size(), 24);
    let mut state = VarPopState::new();
    state.update(&[Some(1.0), Some(5.0)]);
    state.reset();
    assert_eq!(state.result(), None);
}
