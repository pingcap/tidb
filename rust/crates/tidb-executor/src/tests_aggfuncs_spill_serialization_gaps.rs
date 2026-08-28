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

//! Gap tests for Go `pkg/executor/aggfuncs/spill_helper_test.go` (the
//! per-type partial-result spill round-trips this batch's enumeration
//! assigns, items 84-112).
//!
//! Every one of these Go tests is WHITE-BOX (package `aggfuncs`, not
//! `aggfuncs_test`): it hand-builds `partialResult4Xxx` values, serializes
//! each through `SerializeHelper.serializePartialResult4Xxx`
//! (`pkg/executor/aggfuncs/spill_serialize_helper.go:27` and the per-type
//! methods), appends the bytes to a chunk column, then drives
//! `DeserializeHelper.deserializePartialResult4Xxx`
//! (`pkg/executor/aggfuncs/spill_deserialize_helper.go`) until it reports
//! exhaustion, and requires value-for-value equality plus an exact record
//! count.
//!
//! The tier's parallel HashAgg spill serializes the equivalent per-group
//! partial states (`hash_agg/parallel.rs::write_partial`/`read_partial`,
//! one typed tag per aggregate kind) -- but those functions, and the
//! `Partial` states they encode, are private to the pipeline module, and the
//! wire format is this engine's own rather than Go's
//! `pkg/util/serialization` byte layout. The observable equivalence (a
//! spilled aggregation's groups answer exactly what the unspilled
//! aggregation answers) is already pinned at the executor level by
//! `hash_agg_spill_tests` (which also ports
//! `pkg/executor/aggregate/agg_spill_test.go::TestGetCorrectResult`,
//! `TestFallBackAction` and `TestRandomFail` from this batch's range). The
//! per-type white-box contracts below are recorded as gaps, each naming its
//! Go-pinned values, rather than approximated through the executor.

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:73::TestPartialResult4Count`:
/// `partialResult4Count{-123, 0, 123}` must survive serialize ->
/// deserialize with exact equality and exactly three records.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_count() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:114::TestPartialResult4MaxMinInt`:
/// `{val: -123, isNull: true}, {val: 0, isNull: false}, {val: 123,
/// isNull: true}` -- including isNull states whose val stays significant --
/// round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_max_min_int() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:159::TestPartialResult4MaxMinUint`:
/// `{val: 0, isNull: true}, {val: 1, isNull: false},
/// {val: 2, isNull: true}` -- including isNull states whose val stays
/// significant -- round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_max_min_uint() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:204::TestPartialResult4MaxMinDecimal`:
/// `{val: DecFromInt(0), isNull: true}, {val: DecFromUint(123456),
/// isNull: false}, {val: DecFromInt(99999), isNull: true}` round-trip
/// exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_max_min_decimal() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:249::TestPartialResult4MaxMinFloat32`:
/// `{val: -123.123, isNull: true}, {val: 0.0, isNull: false},
/// {val: 123.123, isNull: true}` round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_max_min_float32() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:294::TestPartialResult4MaxMinFloat64`:
/// `{val: -123.123, isNull: true}, {val: 0.0, isNull: false},
/// {val: 123.123, isNull: true}` round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_max_min_float64() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:339::TestPartialResult4MaxMinTime`:
/// `types.NewTime(123, 10, 9)` / `NewTime(0, 0, 0)` / `NewTime(9876, 12, 10)`
/// with their isNull states round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_max_min_time() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:384::TestPartialResult4MaxMinString`:
/// `{val: \"12312412312\", isNull: true}` and `{val: testLongStr1,
/// isNull: false}` (the 10x-doubled seed string) round-trip exactly, and the
/// serializer's buffer must have GROWN for the long string
/// (`bufferSizeChecker`).
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_max_min_string() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:430::TestPartialResult4MaxMinJSON`:
/// raw `BinaryJSON` states (`TypeCode: 3` with an empty value; `TypeCode: 6`
/// over `getLargeRandBuffer()`) with isNull states round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_max_min_json() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:476::TestPartialResult4MaxMinVectorFloat32`:
/// two max/min vector states, one non-null `[1, 2, 3]` and one null 1024-value
/// vector, must survive the helper round trip with exact values and null flags.
// go-parity-gap: Go's per-type SerializeHelper/DeserializeHelper vector codec is not exposed by the Rust executor; its private spill codec has a different aggregate-state format.
#[test]
#[ignore = "go-parity-gap: white-box vector partial-result spill round-trip is not reachable; Rust write_partial/read_partial is private and uses a different wire format"]
fn spill_round_trip_preserves_partial_result_4_max_min_vector_float32() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:525::TestPartialResult4MaxMinEnum`:
/// `{val: Enum{Name: \"\", Value: 123}, isNull: true}` and `{val:
/// Enum{Name: testLongStr1, Value: 0}, isNull: false}` round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_max_min_enum() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:571::TestPartialResult4MaxMinSet`:
/// `{val: Set{Name: \"\", Value: 123}, isNull: true}` and `{val:
/// Set{Name: testLongStr1, Value: 0}, isNull: false}` round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_max_min_set() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:617::TestPartialResult4AvgDecimal`:
/// `{sum: 0, count: 0}, {sum: 12345, count: 123}, {sum: 87654, count: -123}`
/// decimal AVG states round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_avg_decimal() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:662::TestPartialResult4AvgFloat64`:
/// `{sum: 0.0, count: 0}, {sum: 123.123, count: 123}, {sum: -123.123,
/// count: -123}` float AVG states round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_avg_float64() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:707::TestPartialResult4SumDecimal`:
/// `{val: 0, notNullRowCount: 0}, {val: 12345, notNullRowCount: 123},
/// {val: 87654, notNullRowCount: -123}` decimal SUM states round-trip
/// exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_sum_decimal() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:752::TestPartialResult4SumFloat64`:
/// `{val: 0.0, notNullRowCount: 0}, {val: 123.123, notNullRowCount: 123},
/// {val: -123.123, notNullRowCount: -123}` float SUM states round-trip
/// exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_sum_float64() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1100::TestBasePartialResult4GroupConcat`:
/// `basePartialResult4GroupConcat` states (`valsBuf` holding \"123\" or \"\"
/// paired with `buffer` \"\"/testLongStr1/testLongStr2) round-trip exactly,
/// with the same buffer-growth checker.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_base_partial_result_4_group_concat() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1153::TestPartialResult4BitFunc`:
/// `partialResult4BitFunc{0, 1, 2}` fold states round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_bit_func() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1194::TestPartialResult4JsonArrayagg`:
/// `partialResult4JsonArrayagg` value lists round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_json_arrayagg() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1241::TestPartialResult4JsonObjectAgg`:
/// `partialResult4JsonObjectAgg` key/value maps round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_json_object_agg() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1293::TestPartialResult4FirstRowDecimal`:
/// FIRST_ROW DECIMAL states (`isNull`/`gotFirstRow` base fields plus the
/// value 0/123/12345) round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_first_row_decimal() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1338::TestPartialResult4FirstRowInt`:
/// FIRST_ROW INT states (isNull/gotFirstRow over values -123/0/123)
/// round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_first_row_int() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1383::TestPartialResult4FirstRowTime`:
/// FIRST_ROW TIME states (isNull/gotFirstRow over `NewTime(0/123/456, 0,
/// 1)`) round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_first_row_time() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1428::TestPartialResult4FirstRowString`:
/// FIRST_ROW string states (`\"\"` with isNull, `testLongStr1` without)
/// round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_first_row_string() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1474::TestPartialResult4FirstRowFloat32`:
/// FIRST_ROW float32 states round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_first_row_float32() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1519::TestPartialResult4FirstRowFloat64`:
/// FIRST_ROW float64 states round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_first_row_float64() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1564::TestPartialResult4FirstRowDuration`:
/// FIRST_ROW duration states round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_first_row_duration() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:1609::TestPartialResult4FirstRowJSON`:
/// FIRST_ROW JSON states round-trip exactly.
#[test]
#[ignore = "go-parity-gap: white-box SerializeHelper/DeserializeHelper per-type round-trip; the Rust Partial spill codec (write_partial/read_partial) is private to the parallel pipeline with its own wire format"]
fn spill_round_trip_preserves_partial_result_4_first_row_json() {}

/// Go `pkg/executor/aggfuncs/spill_helper_test.go:797::TestPartialResult4DistinctAgg`:
/// the typed distinct partial-result sets and approximate-count-distinct
/// sketch survive `roundTripAggPartialResult`; the Go test covers integer,
/// real, decimal, duration, string, multi-argument count, approximate count,
/// AVG, SUM, variance, group-concat, percentile, and vector subcases.
// go-parity-gap: Rust's distinct state is private to hash aggregation and its spill codec is not the Go per-aggregate SerializeHelper contract.
#[test]
#[ignore = "go-parity-gap: distinct partial-result spill state is not exposed; Rust write_partial/read_partial uses a private aggregate-state format"]
fn spill_round_trip_preserves_distinct_aggregate_partial_results() {}
