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
// See the License for the specific language governing permissions and
// limitations under the License.

//! `pkg/expression/builtin_vectorized_test.go:878 TestVectorizedFilterConsiderNull`
//! and its two `Benchmark*` siblings on `origin/master`.
//!
//! Go runs a randomized five-column/16-round harness comparing
//! `VectorizedFilterConsiderNull` with vectorized evaluation forced OFF and ON,
//! including over a random selection (`SetSel`), then checks the selected mask
//! equals the second run ANDed with the unselected rows. The selection-buffer
//! machinery (`VecEvalBool`, `rowBasedFilter`, selected/nulls buffers) is
//! unported in this crate -- see sibling module
//! [`super::builtin_vectorized_time_infra_source`] for the adjacent
//! `TestVecEvalBool` / `TestRowBasedFilterAndVectorizedFilter` gap stubs from
//! the same file family.

/// go-parity-gap: VectorizedFilterConsiderNull's two paths converge on
/// VecEvalBool/EvalBool over chunk iterators plus Sel handling, none of which
/// exists here; the row-vs-vectorized equality claim cannot be driven.
#[test]
#[ignore = "go-parity-gap: VecEvalBool/rowBasedFilter/vectorizedFilter selection machinery (builtin_vectorized.go) is unported"]
fn test_vectorized_filter_consider_null() {}

/// `pkg/expression/builtin_vectorized_test.go:933 BenchmarkFloat32ColRow`
/// benchmarks float32-typed column scan-vs-row iteration.
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmark; float32 columnar storage is additionally unmodeled in this crate"]
fn benchmark_float32_col_row() {}

/// `pkg/expression/builtin_vectorized_test.go:947 BenchmarkFloat32ColVec`.
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmark; float32 columnar storage is additionally unmodeled in this crate"]
fn benchmark_float32_col_vec() {}
