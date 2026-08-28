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

//! Ports of Go `pkg/ddl/reorg_test.go` and the
//! `TestEstimateTableSizeByIDUsesMaxApproximateSizes` half of
//! `pkg/ddl/reorg_util_test.go` (pkg/ddl batch). All three exercise the
//! reorg runtime (`reorgCtx`, `estimateTableSizeByID`), which has no Rust
// carrier in this tier: each is recorded as an explicit gap with the
// contract re-derived from the Go source. Nothing is approximated.

/// Go `TestReorgCtxSetMaxProgress` (`pkg/ddl/reorg_test.go:24`): a fresh
/// `reorgCtx` reports max progress 0; `setMaxProgress(0.5)` returns the
/// NEW max 0.5 and stores it; `setMaxProgress(0.7)` returns 0.7;
/// `setMaxProgress(0.3)` returns the OLD max 0.7 and leaves the stored
/// value unchanged (monotonic, `pkg/ddl/reorg.go:312`); repeating the
/// current max returns it; a later higher value (0.9) is taken.
// go-parity-gap: the reorgCtx progress atom (pkg/ddl/reorg.go:312) is not
// transcreated; no Rust carrier of the DDL reorg runtime exists.
#[test]
#[ignore]
fn reorg_ctx_set_max_progress_is_monotonic() {
}

/// Go `TestReorgCtxSetMaxProgressConcurrent`
/// (`pkg/ddl/reorg_test.go:50`): 100 goroutines race
/// `setMaxProgress(i/100.0)` for i in 0..100; after the join the stored max
/// is exactly 0.99 -- the monotonic CAS keeps the largest value under
/// concurrency.
// go-parity-gap: same missing reorgCtx carrier as above.
#[test]
#[ignore]
fn reorg_ctx_set_max_progress_concurrent_keeps_the_largest_value() {
}

/// Go `TestEstimateTableSizeByIDUsesMaxApproximateSizes`
/// (`pkg/ddl/reorg_util_test.go:87`): `estimateTableSizeByID`
/// (`pkg/ddl/reorg_util.go:229`) asks PD for the regions of
/// `tablecodec.GetTableHandleKeyRange(pid)` (limit 128, keys prefixed with
/// the codec's region envelope, `k:` under the mock codec) and sums
/// `max(ApproximateSize, ApproximateKvSize)` MiB per region -- rows
/// (5,64)->64, (16,7)->16, (0,9)->9 give 89 MiB over two PD pages (the
/// second call returns empty and the loop stops). The sub-test pins
/// `estimateRowSizeFromRegion` the same way: row size =
/// max(ApproximateSize, ApproximateKvSize) MiB / ApproximateKeys over the
/// three max-vs-size rows, one PD page, limit 3.
// go-parity-gap: no PD HTTP client, store-helper or region-size estimator
// in the Rust tier (pkg/store/helper + pdhttp surfaces missing).
#[test]
#[ignore]
fn estimate_table_size_by_id_sums_max_approximate_sizes() {
}
