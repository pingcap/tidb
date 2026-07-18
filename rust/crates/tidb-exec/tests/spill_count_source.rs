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

//! Source-backed tests for the count aggregate spill wire boundary.

use tidb_exec::aggregate::runtime::spill::{CountDeserializer, SpillSerializer, COUNT_WIRE_SIZE};

#[test]
fn partial_result4_count_spill_round_trip_source_values() {
    // Source: pkg/executor/aggfuncs/spill_serialize_helper.go:33-35 and
    // pkg/executor/aggfuncs/spill_deserialize_helper.go:42-50.
    // Direct Go coverage: pkg/executor/aggfuncs/spill_helper_test.go:73
    // (TestPartialResult4Count), values -123, 0, and 123.
    let mut serializer = SpillSerializer::new();
    let expected = [-123_i64, 0, 123];
    let rows: Vec<Vec<u8>> = expected
        .iter()
        .map(|value| serializer.serialize_count(*value).to_vec())
        .collect();
    let row_views: Vec<&[u8]> = rows.iter().map(Vec::as_slice).collect();

    let mut decoder = CountDeserializer::new(&row_views);
    let mut actual = Vec::new();
    while let Some(value) = decoder.read_next().expect("source-shaped count row") {
        actual.push(value);
    }
    assert_eq!(actual, expected);
    assert_eq!(decoder.position(), expected.len());
}

#[test]
fn partial_result4_count_spill_reuses_source_helper_buffer() {
    // Source: pkg/executor/aggfuncs/spill_serialize_helper.go:21-35.
    // Direct Go coverage: pkg/executor/aggfuncs/spill_helper_test.go:73
    // (TestPartialResult4Count), one SerializeHelper serves every row.
    let mut serializer = SpillSerializer::new();
    let initial_capacity = serializer.capacity();
    let first = serializer.serialize_count(-123).to_vec();
    let second = serializer.serialize_count(123).to_vec();
    assert_eq!(first.len(), COUNT_WIRE_SIZE);
    assert_eq!(second.len(), COUNT_WIRE_SIZE);
    assert_ne!(first, second);
    assert_eq!(serializer.capacity(), initial_capacity);
}
