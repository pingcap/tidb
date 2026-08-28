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

//! Gap tests for Go's `max_count`/`min_count` executor tests.
//!
//! Go implements these aggregates in `pkg/executor/aggfuncs/func_max_min_count.go`
//! (`baseMaxMinCountAggFunc` at lines 53-81 and the sliding implementation at
//! lines 83-182). The Rust executor has no corresponding aggregate names or
//! descriptor/build path, so these tests remain explicit ignored parity claims
//! rather than approximating the SQL behavior with ordinary MAX/MIN.

/// Go `pkg/executor/aggfuncs/func_max_min_count_test.go:310::TestMaxMinCountSQL`:
/// rows `(1),(1),(2),(2),(2),(NULL)` produce `max_count(a) = 3` and
/// `min_count(a) = 2`, NULL-only input produces zeroes, the window form keeps
/// the same counts, parallel hash aggregation is selected, and DISTINCT is a
/// syntax error for both aggregate names.
// go-parity-gap: max_count/min_count aggregate descriptors and execution are not transcreated in Rust.
#[test]
#[ignore = "go-parity-gap: max_count/min_count names, type inference, and executor implementations are absent from the Rust aggregate path"]
fn max_min_count_sql_and_parallel_hashagg_match_go() {}

/// Go `pkg/executor/aggfuncs/func_max_min_count_test.go:336::TestMaxMinCountSlidingWindow`:
/// over rows `(1,1),(2,1),(3,2),(4,2),(5,NULL),(6,2),(7,1)`, a one-row
/// preceding ROWS frame produces the paired max/min counts
/// `1/1, 2/2, 1/1, 2/2, 1/1, 1/1, 1/1`.
// go-parity-gap: max_count/min_count sliding-window state and frame execution are not transcreated in Rust.
#[test]
#[ignore = "go-parity-gap: max_count/min_count sliding aggregate implementations are absent from the Rust window path"]
fn max_min_count_sliding_window_matches_go() {}
