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

//! Gap tests for the two Go package-private tests in
//! `pkg/executor/executor_pkg_test.go`. They call the unexported
//! `buildKvRangesForIndexJoin` helper (`pkg/executor/builder.go:5493`) with Go
//! ranger/distsql contexts and inspect encoded key ordering and memory
//! tracker accounting. The Rust planner has different range and storage
//! ownership, and this crate exposes no equivalent helper seam.

/// Go `pkg/executor/executor_pkg_test.go:49::TestBuildKvRangesForIndexJoinWithoutCwc`.
/// `buildKvRangesForIndexJoin` must return non-overlapping, ordered KV ranges
/// for the supplied lookup keys and index ranges (`pkg/executor/executor_pkg_test.go:49-76`).
#[test]
#[ignore = "go-parity-gap: Go-private buildKvRangesForIndexJoin and its ranger/distsql context are unported"]
fn build_kv_ranges_for_index_join_without_cwc_are_ordered() {}

/// Go `pkg/executor/executor_pkg_test.go:78::TestBuildKvRangesForIndexJoinWithoutCwcAndWithMemoryTracker`.
/// The same helper must charge the Go index-worker memory tracker linearly
/// when lookup rows double and match the asserted 23,640-byte baseline
/// (`pkg/executor/executor_pkg_test.go:78-133`).
#[test]
#[ignore = "go-parity-gap: Go-private index-join range memory accounting and memory tracker are unported"]
fn build_kv_ranges_for_index_join_charges_memory_linearly() {}
