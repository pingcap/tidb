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

//! Gap tests for Go `pkg/executor/aggfuncs/func_json_arrayagg_test.go` and
//! `func_json_objectagg_test.go`. The JSON aggregate builders are
//! transcreated in the sibling crate `tidb-exec` (`json_arrayagg.rs`,
//! `json_objectagg.rs`), which this crate does not depend on.

/// Go `pkg/executor/aggfuncs/func_json_arrayagg_test.go:27::TestMergePartialResult4JsonArrayagg`:
/// across longlong/double/float/string/JSON/date/duration args, ARRAYAGG
/// partials merge by APPENDING the source array's entries -- merge(0..5)
/// keeps entries 0..4, merge(2..5) keeps 2..4, and the merged array holds
/// entries1 followed by entries2 (`func_json_arrayagg.go`).
#[test]
#[ignore = "go-parity-gap: JSON_ARRAYAGG state lives in tidb-exec::json_arrayagg (sibling crate, no dependency edge); Go binary-JSON fixtures have no equivalent here"]
fn merge_partial_result_4_json_arrayagg_appends_entries() {}

/// Go `pkg/executor/aggfuncs/func_json_arrayagg_test.go:65::TestJsonArrayagg`:
/// streaming ARRAYAGG over the same seven arg types produces one array per
/// type holding the JSON-encoded row values (NULL final row appended to the
/// source chunk is skipped), and an empty first partial stays NULL.
#[test]
#[ignore = "go-parity-gap: JSON_ARRAYAGG update lives in tidb-exec::json_arrayagg (sibling crate); aggTest runner not modeled"]
fn json_arrayagg_collects_row_values_per_arg_type() {}

/// Go `pkg/executor/aggfuncs/func_json_arrayagg_test.go:131::TestMemJsonArrayagg`:
/// each appended entry charges `DefInterfaceSize` plus the type payload
/// (uint64/float64/string length/JSON bytes+1/duration/time), and the
/// partial result charges `DefPartialResult4JsonArrayagg + DefSliceSize`.
#[test]
#[ignore = "go-parity-gap: Go's memory-tracker harness and Def*Size constants are not modeled; state lives in tidb-exec (sibling crate)"]
fn mem_json_arrayagg_tracks_entry_payload_sizes() {}

/// Go `pkg/executor/aggfuncs/func_json_objectagg_test.go:48::TestMergePartialResult4JsonObjectagg`:
/// for every non-binary-charset (key,value) type pair, OBJECTAGG partials
/// merge by INSERTING the source map's entries -- merge(0..5) keeps keys
/// 0..4, merge(2..5) keys 2..4, and the merged map equals entries1
/// (later keys do not overwrite) (`func_json_objectagg.go`).
#[test]
#[ignore = "go-parity-gap: JSON_OBJECTAGG state lives in tidb-exec::json_objectagg (sibling crate); the multi-args merge runner and binary-charset filtering are not modeled"]
fn merge_partial_result_4_json_objectagg_inserts_source_entries() {}

/// Go `pkg/executor/aggfuncs/func_json_objectagg_test.go:110::TestJsonObjectagg`:
/// streaming OBJECTAGG over the same type pairs builds one JSON object per
/// pair keyed by the stringified first arg with the JSON-encoded second
/// arg, and an empty first partial stays NULL.
#[test]
#[ignore = "go-parity-gap: JSON_OBJECTAGG update lives in tidb-exec::json_objectagg (sibling crate); multi-args aggTest runner not modeled"]
fn json_objectagg_builds_objects_per_type_pair() {}

/// Go `pkg/executor/aggfuncs/func_json_objectagg_test.go:163::TestMemJsonObjectagg`:
/// every (key,value) type combination charges the entry key/value payload
/// sizes per update and `DefPartialResult4JsonObjectagg*` per partial,
/// including distinct-set bucket costs.
#[test]
#[ignore = "go-parity-gap: Go's memory-tracker harness and Def*Size constants are not modeled; state lives in tidb-exec (sibling crate)"]
fn mem_json_objectagg_tracks_entry_payload_sizes() {}
