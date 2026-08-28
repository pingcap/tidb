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

//! Gap tests for Go `pkg/executor/aggfuncs/func_distinct_agg_test.go`: the
//! parallel (multi-worker) DISTINCT aggregate drivers. Go splits one chunk
//! stream across `partial`/`final` worker pairs keyed by encoded value, and
//! each test runs 10 randomized cases (empty input / some NULL / all NULL /
//! no NULL) against COUNT, SUM, SUM_INT, AVG, VAR_POP/VAR_SAMP/STDDEV_POP/
//! STDDEV_SAMP and GROUP_CONCAT. The distinct state machines live in the
//! sibling crate `tidb-exec` (`aggregate_distinct.rs`,
//! `aggregate/runtime/*`), which this crate does not depend on, and Go's
//! randomized `parallelDistinctAggTestCase` generator has no Rust
//! counterpart.

/// Go `pkg/executor/aggfuncs/func_distinct_agg_test.go:26::TestParallelDistinctCount`:
/// parallel DISTINCT COUNT over longlong/double/decimal/varstring/duration
/// inputs agrees with serial dedup (10 randomized shapes, up to 10000 rows),
/// and the two-arg string variant dedups on the (a,b) pair.
#[test]
#[ignore = "go-parity-gap: parallel-distinct driver + randomized case generator live in tidb-exec::aggregate_distinct (sibling crate); no worker-pair seam on this tier"]
fn parallel_distinct_count_matches_serial_dedup() {}

/// Go `pkg/executor/aggfuncs/func_distinct_agg_test.go:35::TestParallelDistinctSum`:
/// parallel DISTINCT SUM over double/decimal inputs equals the serial
/// distinct sum across the randomized empty/NULL/full shapes.
#[test]
#[ignore = "go-parity-gap: parallel-distinct SUM driver lives in tidb-exec (sibling crate); randomized harness not modeled"]
fn parallel_distinct_sum_matches_serial_dedup() {}

/// Go `pkg/executor/aggfuncs/func_distinct_agg_test.go:40::TestParallelDistinctSumInt`:
/// signed and unsigned `TypeLonglong` inputs keep SUM_INT's integer result
/// under parallel distinct dedup.
#[test]
#[ignore = "go-parity-gap: parallel-distinct SUM_INT driver lives in tidb-exec (sibling crate); randomized harness not modeled"]
fn parallel_distinct_sum_int_keeps_integer_result() {}

/// Go `pkg/executor/aggfuncs/func_distinct_agg_test.go:48::TestParallelDistinctAvg`:
/// parallel DISTINCT AVG over double/decimal recomposes sum/count after
/// dedup and matches the serial result.
#[test]
#[ignore = "go-parity-gap: parallel-distinct AVG driver lives in tidb-exec (sibling crate); randomized harness not modeled"]
fn parallel_distinct_avg_recomposes_sum_and_count() {}

/// Go `pkg/executor/aggfuncs/func_distinct_agg_test.go:53::TestParallelDistinctVarAndStddev`:
/// VAR_POP/VAR_SAMP/STDDEV_POP/STDDEV_SAMP under parallel distinct dedup
/// agree with their serial counterparts over double inputs.
#[test]
#[ignore = "go-parity-gap: parallel-distinct variance family lives in tidb-exec::aggregate::runtime::variance (sibling crate); randomized harness not modeled"]
fn parallel_distinct_var_and_stddev_match_serial() {}

/// Go `pkg/executor/aggfuncs/func_distinct_agg_test.go:62::TestParallelDistinctGroupConcat`:
/// parallel DISTINCT GROUP_CONCAT over two string args dedups on the pair
/// and joins with the separator, matching serial output (100-row cases).
#[test]
#[ignore = "go-parity-gap: parallel-distinct GROUP_CONCAT lives in tidb-exec::aggregate::runtime::group_concat (sibling crate); randomized harness not modeled"]
fn parallel_distinct_group_concat_dedupes_pairs() {}
