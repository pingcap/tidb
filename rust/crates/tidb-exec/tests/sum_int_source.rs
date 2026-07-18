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

//! Source-backed tests for non-DISTINCT signed and unsigned integer `SUM`.

use tidb_exec::aggregate::runtime::{SumInt64State, SumIntError, SumUint64State};

#[test]
fn sum_int64_update_merge_and_empty_result_match_source() {
    // Source: pkg/executor/aggfuncs/func_sum_int.go:87-129.
    // Direct Go coverage: pkg/executor/aggfuncs/func_sum_test.go:33,50
    // (TestMergePartialResult4Sum/TestSum, signed SUM_INT subcases).
    let mut state = SumInt64State::new();
    assert_eq!(state.result(), None);
    state.update(&[None]).unwrap();
    assert_eq!(state.result(), None);
    state
        .update(&[Some(0), Some(1), Some(2), Some(3), Some(4)])
        .unwrap();
    assert_eq!(state.result(), Some(10));

    let mut source = SumInt64State::new();
    source.update(&[Some(9)]).unwrap();
    state.merge_from(&source).unwrap();
    assert_eq!(state.result(), Some(19));
    state.merge_from(&SumInt64State::new()).unwrap();
    assert_eq!(state.result(), Some(19));

    let mut empty = SumInt64State::new();
    empty.merge_from(&state).unwrap();
    assert_eq!(empty.result(), Some(19));
}

#[test]
fn sum_uint64_update_merge_and_empty_result_match_source() {
    // Source: pkg/executor/aggfuncs/func_sum_int.go:273-315.
    // Direct Go coverage: pkg/executor/aggfuncs/func_sum_test.go:33,50
    // (TestMergePartialResult4Sum/TestSum, unsigned SUM_INT subcases).
    let mut state = SumUint64State::new();
    state
        .update(&[None, Some(0), Some(1), Some(2), Some(3), Some(4)])
        .unwrap();
    assert_eq!(state.result(), Some(10));
    let mut source = SumUint64State::new();
    source.update(&[Some(9)]).unwrap();
    state.merge_from(&source).unwrap();
    assert_eq!(state.result(), Some(19));
    let mut empty = SumUint64State::new();
    empty.merge_from(&state).unwrap();
    assert_eq!(empty.result(), Some(19));
}

#[test]
fn sum_int_checked_overflow_matches_source_error_boundary() {
    // Source: pkg/executor/aggfuncs/func_sum_int.go:102-104, 122-124,
    // and 289-291.
    let mut signed = SumInt64State::new();
    signed.update(&[Some(i64::MAX)]).unwrap();
    assert_eq!(signed.update(&[Some(1)]), Err(SumIntError::Overflow));
    assert_eq!(signed.result(), Some(i64::MAX));

    let mut unsigned = SumUint64State::new();
    unsigned.update(&[Some(u64::MAX)]).unwrap();
    assert_eq!(unsigned.update(&[Some(1)]), Err(SumIntError::Overflow));
    assert_eq!(unsigned.result(), Some(u64::MAX));
}

#[test]
fn sum_int_sliding_removes_outgoing_before_incoming() {
    // Source: pkg/executor/aggfuncs/func_sum_int.go:151-183, 338-370.
    // Direct Go coverage: pkg/executor/aggfuncs/func_sum_test.go:89,133
    // (TestSlideSumUint/IntProcessOutWindowFirstToAvoidOverflow).
    let mut signed = SumInt64State::new();
    signed.update(&[Some(i64::MAX - 1)]).unwrap();
    signed.slide(&[Some(i64::MAX - 1)], &[Some(2)]).unwrap();
    assert_eq!(signed.result(), Some(2));

    let mut unsigned = SumUint64State::new();
    unsigned.update(&[Some(u64::MAX - 1)]).unwrap();
    unsigned.slide(&[Some(u64::MAX - 1)], &[Some(2)]).unwrap();
    assert_eq!(unsigned.result(), Some(2));
}

#[test]
fn sum_int_reset_and_partial_state_size_match_source() {
    // Source: pkg/executor/aggfuncs/func_sum_int.go:66-75, 252-261.
    // Direct Go coverage: pkg/executor/aggfuncs/func_sum_test.go:66
    // (TestMemSum, non-DISTINCT signed/unsigned state subcases).
    assert_eq!(
        SumInt64State::partial_state_size(),
        std::mem::size_of::<SumInt64State>()
    );
    assert_eq!(
        SumUint64State::partial_state_size(),
        std::mem::size_of::<SumUint64State>()
    );
    let mut signed = SumInt64State::new();
    signed.update(&[Some(5)]).unwrap();
    signed.reset();
    assert_eq!(signed.result(), None);
    let mut unsigned = SumUint64State::new();
    unsigned.update(&[Some(5)]).unwrap();
    unsigned.reset();
    assert_eq!(unsigned.result(), None);
}
