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

//! Ignored parity gaps for `pkg/distsql/distsql_test.go` /
//! `select_result_test.go` behaviors whose transcreated counterparts are not
//! part of this crate yet.

// go-parity-gap: `distsql_test.go::TestSelectResultRuntimeStats` asserts the
// composed output of `selectResultRuntimeStats.String()` (`pkg/distsql/
// select_result.go`), including tikv `RegionRequestRuntimeStats`
// (`cop_task: {…}`, rpc_info counters), percentile cop-resp/proc-keys
// histograms, and duration formatting with idempotence across calls. None of
// those transcreated counterparts exist in tidb-distsql today:
// `SelectResultRuntimeStats` owns only backoff totals and tipb execution
// summaries (`src/distsql_runtime.rs`).
#[test]
#[ignore = "go-parity-gap: selectResultRuntimeStats.String()/RegionRequestRuntimeStats formatting is unported"]
fn select_result_runtime_stats_string_composition_is_unported() {}

// go-parity-gap: the closing block of `select_result_test.go::
// TestSelectResultIter` calls r.Next/NextRaw/Close after IntoIter() and next()
// over a response containing intermediate outputs, expecting
// "selectResult is invalid after IntoIter()" and "If a response contains
// intermediate outputs, you should use the SelectResultIter to read the data".
// The Rust boundary removes the raw read path entirely by moving response
// ownership into `QuerySelectResult::into_select_iter`, so post-conversion
// misuse cannot be expressed and no runtime rejection exists to assert until a
// non-consuming select read path is added to this crate.
#[test]
#[ignore = "go-parity-gap: post-IntoIter misuse is prevented structurally by response ownership; no runtime error exists"]
fn select_next_rejects_intermediate_output_responses_structurally() {}
