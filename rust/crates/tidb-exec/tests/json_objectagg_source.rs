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

//! Source-backed tests for `JSON_OBJECTAGG` partial-result state.

use tidb_exec::json_objectagg::{JsonObjectAggState, JsonObjectError, MAP_BUCKET_MEMORY};

#[test]
fn json_objectagg_merge_preserves_last_value_and_sorted_keys() {
    // Source: pkg/executor/aggfuncs/func_json_objectagg.go:182-190.
    // Direct Go coverage: pkg/executor/aggfuncs/func_json_objectagg_test.go:48
    // (TestMergePartialResult4JsonObjectagg). The BinaryJSON encoder sorts
    // map keys, while merge applies source values after destination values.
    let mut destination = JsonObjectAggState::new();
    destination.insert("b", "1");
    destination.insert("a", "\"old\"");

    let mut source = JsonObjectAggState::new();
    source.insert("a", "\"new\"");
    source.insert_fragment("c", "{\"nested\":true}");

    destination.merge_from(&source);
    assert_eq!(destination.len(), 3);
    assert_eq!(
        destination.finish().as_deref(),
        Some("{\"a\":\"new\",\"b\":1,\"c\":{\"nested\":true}}")
    );
}

#[test]
fn json_objectagg_values_and_empty_result_match_source() {
    // Source: pkg/executor/aggfuncs/func_json_objectagg.go:45-61.
    // Direct Go coverage: pkg/executor/aggfuncs/func_json_objectagg_test.go:110
    // (TestJsonObjectagg), with NULL values retained and keys escaped.
    let mut state = JsonObjectAggState::new();
    assert!(state.finish().is_none());
    state.insert("null", "null");
    state.insert("quote\nkey", "18446744073709551615");
    state.insert("json", "[1,2]");
    assert_eq!(
        state.finish().as_deref(),
        Some("{\"json\":[1,2],\"null\":null,\"quote\\nkey\":18446744073709551615}")
    );
}

#[test]
fn json_objectagg_memory_shape_and_reset_match_source() {
    // Source: pkg/executor/aggfuncs/func_json_objectagg.go:24-41.
    // Direct Go coverage: pkg/executor/aggfuncs/func_json_objectagg_test.go:163
    // (TestMemJsonObjectagg), including the initial map bucket allocation.
    assert!(JsonObjectAggState::partial_state_size() > 0);
    assert_eq!(
        JsonObjectAggState::initial_allocation_size(),
        JsonObjectAggState::partial_state_size() + MAP_BUCKET_MEMORY
    );

    let mut state = JsonObjectAggState::new();
    assert_eq!(state.len(), 0);
    state.insert("one", "1");
    assert_eq!(state.len(), 1);
    state.insert("one", "2");
    assert_eq!(state.len(), 1);
    state.reset();
    assert!(state.is_empty());
    assert_eq!(state.len(), 0);
}

#[test]
fn json_objectagg_rejects_null_and_binary_keys_before_mutation() {
    let mut state = JsonObjectAggState::new();
    assert_eq!(
        state.insert_optional(None, false, "null").unwrap_err(),
        JsonObjectError::NullKey
    );
    assert_eq!(
        state
            .insert_optional(Some("binary"), true, "null")
            .unwrap_err(),
        JsonObjectError::BinaryKeyCharset
    );
    assert!(state.is_empty());
}
