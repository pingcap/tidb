# `pkg/statistics/handle/cache` parity receipt

Pinned source: Go `master` snapshot `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

This receipt covers the complete root Go package. The `internal`, `metrics`,
and `internal/*` directories are separate Go packages with their own atomic
receipts.

## Atomic inventory

The pinned package has five artifacts and 1,051 lines. Every production,
test, benchmark, and build file was read before editing. There are no package
docs, fixtures, generated files, platform variants, or external support
artifacts.

| Artifact | Lines | Git blob | SHA-256 | Rust disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 56 | `5451ace13faff1d27aa2f398bc320382253dbb0e` | `9837eb9e9c1c0f2bc49c8049493e9957f3a81044bed87367b185b17dd65d3ea1` | Cargo crate and benchmark graph own the library/test dependencies. |
| `bench_test.go` | 186 | `aaf4e64fd433a408d7393ead26cb78d27acce80e` | `95599555482310f57c61548d242c54970bda9e9621d45b201fc899d711c5b641` | `benches/statscache.rs` retains all six LFU/map workload shapes. |
| `statscache.go` | 410 | `fd98ba1eb884e0b68b5daaa01ae8358a8d0d1c05` | `24512ba05feacebf4cbbce4902fe4348b28dabd3c2ad5eb12ae984ce565331d9` | `StatsCacheImpl` owns ordered refresh, batching, reuse/reload/delete, cancellation, health metrics, and lifecycle. |
| `statscache_test.go` | 202 | `602fe311ace06d4ecbfb62bd64ded6191a2ef2cf` | `56b0338289c8fb7ad273414e193ae78f31d15e33ea4850875945b6293e25647a` | Batch-boundary and exact health-bucket regressions map to owner tests. |
| `statscacheinner.go` | 197 | `68b6a08e8e21ce683d55b9ddd49777661f9ffd93` | `da23f0b514dc65114edcc3dadeb9f8a4c3007e2aa7bf046aa06a57b0f9ed80ed` | `StatsCache` owns LFU/map selection, counters, retry, COW/in-place update, version, capacity, eviction, wait, and close. |

## Production behavior

`rust/crates/tidb-stats-handle-cache/src/lib.rs` now owns only the five-file
root cache contract. The former `StatsTableRowCache`, its process-wide maps,
and its partition/index size estimators were removed because pinned Go deleted
`stats_table_row_cache.go` in favor of statement-local
`pkg/statistics/handle/storage/table_size_stats.go`.

The root cache preserves Go's LFU in-place versus map copy-on-write selection,
runtime quota backing, hit/miss/update/delete accounting, retrying `Put`,
lifecycle maximum version, targeted watermark suppression, dynamic capacity,
eviction/wait/close, ordered metadata refresh, ten-item independent
update/delete batches, cancellation, metadata-only reuse, storage reload,
DDL disappearance, per-table read-error continuation, and health gauges.

The replacement statement-local size path is implemented in
`tidb-exec::cluster_stats_load::TableSizeStats` and the server/session
information-schema boundary. Each statement reads `stats_meta`, reads
non-index `stats_histograms` only when a retained projection requests
`AVG_ROW_LENGTH`, `DATA_LENGTH`, or `INDEX_LENGTH`, computes fixed/variable
width and partition/global-index estimates, and overlays the result on a
scratch catalog without publishing a process-wide cache. A failed restricted
read produces Go's nil `TableSizeStats` behavior (zero size columns) for that
statement.

## Test and benchmark mapping

- The two original Go tests map to Rust's
  `source_batch_update_flushes_each_side_at_its_limit` and
  `source_healthy_metrics_use_exact_buckets`.
- Root owner tests additionally cover backend update modes, refresh reuse,
  deletion/error handling, cancellation, targeted watermark behavior, and the
  failpoint-gated cache miss.
- `tidb-exec::analyze_commit_size_source` covers the durable row-count and
  non-index column-length reads and the focused
  `table_size_stats_are_statement_local_and_clamp_negative_sizes` regression.
- `tidb-server::cluster_session::loaded_column_ndv_reaches_grouped_cluster_plans`
  proves TABLES projection pruning, CTE refresh, histogram-read skipping for
  TABLE_ROWS-only scans, no shared-catalog mutation, and zero-on-read-error.
- The benchmark target retains
  `BenchmarkStatsCacheLFUCopyAndUpdate`,
  `BenchmarkStatsCacheMapCacheCopyAndUpdate`, `BenchmarkLFUCachePutGet`,
  `BenchmarkMapCachePutGet`, `BenchmarkLFUCacheGet`, and `BenchmarkMapCacheGet`.

The server regression originally failed before the scratch-catalog overlay
fix with zero statistics instead of the loaded tuple; it passes after the
overlay is applied before information-schema rows are built.

## Validation

WIP checks while iterating:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache`
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-exec --test all table_size_stats_source_reads_requested_storage_rows -- --nocapture`
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-exec --test all table_size_stats_are_statement_local_and_clamp_negative_sizes -- --nocapture`
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-server --lib loaded_column_ndv_reaches_grouped_cluster_plans -- --nocapture`
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-cache -p tidb-exec -p tidb-executor -p tidb-session -p tidb-server -q`

Ready validation for this batch is required before its commit is reported:

- pinned Go package tests through `tools/check/failpoint-go-test.sh`;
- Rust owner/consumer tests and checks above;
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`;
- pinned `make lint`;
- `git diff --check`.

No Go or Bazel source was changed in this batch, so `make bazel_prepare` is
not required. This is one atomic root-package receipt, not a repository-wide
parity claim.
