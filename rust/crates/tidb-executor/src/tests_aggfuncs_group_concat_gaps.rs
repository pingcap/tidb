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

//! Gap tests for Go `pkg/executor/aggfuncs/func_group_concat_test.go`.
//! GROUP_CONCAT's buffer/order/distinct state machines are transcreated in
//! the sibling crate `tidb-exec` (`group_concat.rs`,
//! `aggregate/runtime/group_concat/`), which this crate does not depend on.

/// Go `pkg/executor/aggfuncs/func_group_concat_test.go:37::TestMergePartialResult4GroupConcat`
/// (separator " "): merge(0..5) joins to "0 1 2 3 4", merge(2..5) to
/// "2 3 4", and their merge concatenates byte-wise to
/// "0 1 2 3 4 2 3 4" (`func_group_concat.go`).
#[test]
#[ignore = "go-parity-gap: GROUP_CONCAT partial/merge state lives in tidb-exec::group_concat (sibling crate, no dependency edge)"]
fn merge_partial_result_4_group_concat_joins_with_separator() {}

/// Go `pkg/executor/aggfuncs/func_group_concat_test.go:42::TestGroupConcat`:
/// streaming concat yields "0 1 2 3 4"; the two-arg ORDER BY variant sorts
/// by the first arg and joins the second to "44 33 22 11 00"; lowering
/// `group_concat_max_len` from 7 to 4 truncates the result to the first i
/// characters, with values below 4 clamped up to 4
/// (`sessionctx/vardef.GroupConcatMaxLen` minimum).
#[test]
#[ignore = "go-parity-gap: needs the ordered GROUP_CONCAT runner and the session group_concat_max_len variable; state lives in tidb-exec (sibling crate)"]
fn group_concat_orders_then_truncates_at_max_len() {}

/// Go `pkg/executor/aggfuncs/func_group_concat_test.go:66::TestMemGroupConcat`:
/// memory deltas for the plain/distinct/order/order-distinct two-arg
/// variants match `DefPartialResult4GroupConcat*` + `DefBytesBufferSize` /
/// `DefTopNRowsSize` plus the string set/map bucket constants.
#[test]
#[ignore = "go-parity-gap: Go's memory-tracker harness and Def*Size constants are not modeled; state lives in tidb-exec (sibling crate)"]
fn mem_group_concat_tracks_buffer_order_and_distinct_sizes() {}
