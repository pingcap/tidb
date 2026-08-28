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

//! Gap tests for the AVG, bit-function and COUNT aggregate-state contracts of
//! Go `pkg/executor/aggfuncs/func_avg_test.go`, `func_bitfuncs_test.go` and
//! `func_count_test.go`. The state machines themselves are transcreated in
//! the sibling crate `tidb-exec` (`aggregate/runtime/avg.rs`,
//! `bit_agg.rs`, `aggregate/runtime/count.rs`), which this crate does not
//! depend on; Go's shared `aggTest` harness (merge of partials over
//! `buildAggTester` row ranges, and the `testAggMemFunc` memory-delta
//! accounting) has no counterpart here either.

/// Go `pkg/executor/aggfuncs/func_avg_test.go:27::TestMergePartialResult4Avg`:
/// over rows 0..5, AVG partials merge per type -- decimal and float64 AVG of
/// the full range is 2.0, of rows 2..5 is 3.0, and merging (0..5) then
/// (2..5) still yields 2.375 (sum/count recomposition, `func_avg.go`
/// `partialResult4AvgDecimal`/`partialResult4AvgFloat64`).
#[test]
#[ignore = "go-parity-gap: AVG partial/merge state lives in tidb-exec::aggregate::runtime::avg (sibling crate, no dependency edge); the Go aggTest merge harness has no Rust counterpart"]
fn merge_partial_result_4_avg_recomposes_sum_and_count() {}

/// Go `pkg/executor/aggfuncs/func_avg_test.go:37::TestAvg`: streaming AVG over
/// rows 0..5 ends at 2.0 for both `TypeNewDecimal` and `TypeDouble` args,
/// and NULL input rows leave the result NULL.
#[test]
#[ignore = "go-parity-gap: AVG update-over-chunks lives in tidb-exec::aggregate::runtime::avg (sibling crate); no aggTest runner on this tier"]
fn avg_over_rows_zero_to_five_yields_two() {}

/// Go `pkg/executor/aggfuncs/func_avg_test.go:48::TestMemAvg`: memory
/// accounting matches `DefPartialResult4AvgDecimalSize` /
/// `DefPartialResult4AvgFloat64Size` per update and the distinct variants
/// add `DefBucketMemoryUsageForSetString`/`...SetFloat64` bucket costs
/// (`func_avg.go:29` size constants).
#[test]
#[ignore = "go-parity-gap: Go's per-partial-result memory tracker (testAggMemFunc + Def*Size constants) is not modeled; state lives in tidb-exec (sibling crate)"]
fn mem_avg_tracks_partial_and_distinct_bucket_sizes() {}

/// Go `pkg/executor/aggfuncs/func_bitfuncs_test.go:25::TestMergePartialResult4BitFuncs`:
/// BITAND/BITOR/BITXOR partials merge over rows 0..5 -- AND folds to 0,
/// OR to 7, XOR of (0..5) with (2..5) to 1 (`func_bitfuncs.go`).
#[test]
#[ignore = "go-parity-gap: bit aggregates are tidb-exec::bit_agg::BitAggregate (sibling crate, no dependency edge)"]
fn merge_partial_result_4_bit_funcs_fold_and_or_xor() {}

/// Go `pkg/executor/aggfuncs/func_bitfuncs_test.go:36::TestMemBitFunc`: each
/// bit aggregate update charges `DefPartialResult4BitFuncSize` once per
/// partial result, not per row.
#[test]
#[ignore = "go-parity-gap: Go's memory-tracker harness (testAggMemFunc) is not modeled; state lives in tidb-exec::bit_agg (sibling crate)"]
fn mem_bit_func_charges_one_partial_result_per_update() {}

/// Go `pkg/executor/aggfuncs/func_count_test.go:43::TestMergePartialResult4Count`:
/// COUNT partials merge additively -- (0..5)+(2..5) = 8, single (0..5) = 5;
/// the APPROX_COUNT_DISTINCT case merges serialized HLL sketches
/// (`NewPartialResult4ApproxCountDistinct` fed farm.Hash64 of little-endian
/// row ids) and the merged sketch still counts 5 distinct.
#[test]
#[ignore = "go-parity-gap: COUNT/HLL merge state lives in tidb-exec::aggregate::runtime::count (sibling crate); the serialized-sketch fixture needs aggfuncs.NewPartialResult4ApproxCountDistinct"]
fn merge_partial_result_4_count_adds_and_hll_dedupes() {}

/// Go `pkg/executor/aggfuncs/func_count_test.go:51::TestCount`: COUNT over 5
/// rows counts 5 (0 over an all-NULL first row set) for each of longlong,
/// float, double, decimal, string, date, duration and JSON single-arg
/// cases; two-arg COUNT and APPROX_COUNT_DISTINCT count rows where ANY arg
/// is non-NULL; a second single-arg sweep repeats the 5-row count.
#[test]
#[ignore = "go-parity-gap: COUNT update lives in tidb-exec::aggregate::runtime::count (sibling crate); the multi-args aggTest runner has no counterpart"]
fn count_sweep_across_arg_types_counts_non_null_rows() {}

/// Go `pkg/executor/aggfuncs/func_count_test.go:115::TestMemCount`: memory
/// deltas per COUNT/COUNT(DISTINCT)/APPROX_COUNT_DISTINCT variant match the
/// `Def*Size` constants plus set/map bucket costs
/// (`DefBucketMemoryUsageForSetInt64`/`...String`/`...Float64`,
/// `...MapStringToString`).
#[test]
#[ignore = "go-parity-gap: Go's memory-tracker harness and Def*Size constants are not modeled; state lives in tidb-exec (sibling crate)"]
fn mem_count_tracks_per_variant_sizes() {}

/// Go `pkg/executor/aggfuncs/func_count_test.go:159::TestWriteTime`: encoding
/// a `types.Time` date (`2020-11-11`) with
/// `aggfuncs.WriteTime` (`pkg/executor/aggfuncs/func_count_distinct.go:590`)
/// overwrites every byte of a 0xFF-filled 16-byte buffer.
#[test]
#[ignore = "go-parity-gap: aggfuncs.WriteTime (pkg/executor/aggfuncs/func_count_distinct.go:590) has no Rust counterpart; tidb-exec's count distinct path encodes via tidb-codec"]
fn write_time_overwrites_every_buffer_byte() {}
