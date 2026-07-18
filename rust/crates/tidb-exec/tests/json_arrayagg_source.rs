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

//! Source-backed tests for `JSON_ARRAYAGG` partial-result state.

use tidb_exec::json_arrayagg::{JsonArrayAggState, JsonArrayError, JsonArrayValue};

#[test]
fn json_arrayagg_merge_preserves_source_order() {
    // Source: pkg/executor/aggfuncs/func_json_arrayagg.go:74-80.
    // Direct Go coverage: pkg/executor/aggfuncs/func_json_arrayagg_test.go:27
    // (TestMergePartialResult4JsonArrayagg), across numeric, textual, JSON,
    // date, and duration-shaped serialized values.
    let mut left = JsonArrayAggState::new();
    left.append(JsonArrayValue::Signed(0)).unwrap();
    left.append(JsonArrayValue::String("left".to_owned()))
        .unwrap();
    left.append_fragment("{\"kind\":\"date\"}");

    let mut right = JsonArrayAggState::new();
    right.append(JsonArrayValue::Real(2.5)).unwrap();
    right.append(JsonArrayValue::Boolean(true)).unwrap();
    right.append_fragment("\"duration\"");

    left.merge_from(&right);
    assert_eq!(left.len(), 6);
    assert_eq!(
        left.finish().as_deref(),
        Some("[0,\"left\",{\"kind\":\"date\"},2.5,true,\"duration\"]")
    );
}

#[test]
fn json_arrayagg_values_and_empty_result_match_source() {
    // Source: pkg/executor/aggfuncs/func_json_arrayagg.go:45-60.
    // Direct Go coverage: pkg/executor/aggfuncs/func_json_arrayagg_test.go:65
    // (TestJsonArrayagg), with NULL and scalar values retained in order.
    let mut state = JsonArrayAggState::new();
    assert!(state.finish().is_none());
    state.append(JsonArrayValue::Null).unwrap();
    state.append(JsonArrayValue::Unsigned(u64::MAX)).unwrap();
    state
        .append(JsonArrayValue::String("quote\nslash\\".to_owned()))
        .unwrap();
    state.append_fragment("[1,2]");
    assert_eq!(
        state.finish().as_deref(),
        Some("[null,18446744073709551615,\"quote\\nslash\\\\\",[1,2]]")
    );
}

#[test]
fn json_arrayagg_memory_shape_and_reset_match_source() {
    // Source: pkg/executor/aggfuncs/func_json_arrayagg.go:26-43.
    // Direct Go coverage: pkg/executor/aggfuncs/func_json_arrayagg_test.go:131
    // (TestMemJsonArrayagg), whose initial allocation is the partial state
    // plus an empty interface slice header.
    assert_eq!(
        JsonArrayAggState::partial_state_size(),
        std::mem::size_of::<JsonArrayAggState>()
    );
    assert_eq!(
        JsonArrayAggState::initial_allocation_size(),
        std::mem::size_of::<JsonArrayAggState>() + std::mem::size_of::<Vec<String>>()
    );

    let mut state = JsonArrayAggState::new();
    state.append(JsonArrayValue::Signed(1)).unwrap();
    state.reset();
    assert!(state.is_empty());
    assert!(state.finish().is_none());
}

#[test]
fn json_arrayagg_rejects_non_finite_real_before_mutation() {
    let mut state = JsonArrayAggState::new();
    let error = state.append(JsonArrayValue::Real(f64::NAN)).unwrap_err();
    assert_eq!(error, JsonArrayError::NonFiniteReal);
    assert!(state.is_empty());
}
