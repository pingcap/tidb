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

//! Gap tests for Go `pkg/executor/aggfuncs/func_cume_dist_test.go` and
//! `func_first_row_test.go`. CUME_DIST's per-row counter and FIRST_ROW's
//! datum state are transcreated in the sibling crate `tidb-exec`
//! (`cume_dist.rs`, `first_row.rs`), which this crate does not depend on.

/// Go `pkg/executor/aggfuncs/func_cume_dist_test.go:25::TestMemCumeDist`: the
/// CUME_DIST partial result charges `DefPartialResult4CumeDistSize` once and
/// then only row-count deltas (1, 0 or 4 additional rows per the windowMem
/// cases) as the peer counter grows (`func_cume_dist.go`).
#[test]
#[ignore = "go-parity-gap: CUME_DIST state lives in tidb-exec::cume_dist (sibling crate); the windowMemTest memory harness is not modeled"]
fn mem_cume_dist_charges_size_plus_row_deltas() {}

/// Go `pkg/executor/aggfuncs/func_first_row_test.go:27::TestMergePartialResult4FirstRow`:
/// FIRST_ROW partials keep the FIRST physical row and ignore later ones --
/// merge(0..5) keeps 0, merge(2..5) keeps 2, and merged (0..5)+(2..5) still
/// keeps 0 -- across longlong/float/double/decimal/string/date/duration/
/// JSON and, for enum/set, the DECODED names: enum "e" beats "c" by
/// insertion position (first row wins regardless of value), set "e" over
/// "e,d" likewise (`func_first_row.go`).
#[test]
#[ignore = "go-parity-gap: FIRST_ROW datum state lives in tidb-exec::first_row (sibling crate); the aggTest merge harness and Go enum/set datums have no counterpart here"]
fn merge_partial_result_4_first_row_keeps_the_first_physical_row() {}

/// Go `pkg/executor/aggfuncs/func_first_row_test.go:52::TestMemFirstRow`: each
/// type-specific FIRST_ROW partial charges its `DefPartialResult4FirstRow*`
/// size constant once, with strings/JSON using the `firstRow` mem-delta
/// generators that add the payload length.
#[test]
#[ignore = "go-parity-gap: memory-tracker harness + Def*Size constants not modeled; state lives in tidb-exec::first_row (sibling crate)"]
fn mem_first_row_charges_one_partial_per_type() {}
