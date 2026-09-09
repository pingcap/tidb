# `pkg/util/execdetails` parity receipt

Pinned source: TiDB Go master `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

This short receipt is retained as a compatibility pointer; the complete
8-artifact inventory, Go-master delta, Rust seed-owner comparison, and Ready
validation evidence live in `util_execdetails_audit.md`.

## Complete Go inventory

- `BUILD.bazel`
- `execdetails.go`
- `runtime_stats.go`
- `ruv2_metrics.go`
- `tiflash_stats.go`
- `util.go`
- `execdetails_test.go`
- `main_test.go`

## Rust ownership and integration

- `tidb-exec::{exec_details,runtime_stats,ruv2_metrics,tiflash_stats}` owns the
  package API and ordinary executor integration.
- `tidb-util::ruv2_metrics` is the single lower-level live RUv2 implementation;
  `tidb-exec` re-exports it so both executor accounting and Top-SQL consume the
  same counters, weights, labels, merge/drain behavior, and formatting.
- Commit, lock, scan, time, traffic, and RU details use the canonical
  `tikv-client` types. Columnar/TiFlash/TiCI detail fields use generated `tipb`
  types rather than duplicate Rust carriers.
- Runtime-stat collection getters retain shared live values and the TiKV
  details loader takes one coherent atomic snapshot.
- The Go package tests are represented by the corresponding Rust module tests;
  `main_test.go`'s process-global metric-label setup is naturally lazy in the
  Rust Prometheus owners.

## WIP validation

Commands run from `rust/`:

```text
cargo test --quiet --offline -p tidb-util --lib ruv2_metrics::tests
cargo test --quiet --offline -p tidb-util --lib topsql_stmtstats
cargo test --quiet --offline -p tidb-exec --lib exec_details::tests
cargo test --quiet --offline -p tidb-exec --lib runtime_stats::tests
cargo test --quiet --offline -p tidb-exec --lib tiflash_stats::tests
cargo test --quiet --offline -p tidb-stmtsummary --lib statement_summary::tests
cargo test --quiet --offline -p tidb-stmtsummary --lib v2::record::tests
```

Results: 9, 37, 3, 15, 3, 19, and 1 tests passed respectively. No test failed.
Warnings shown by Cargo predate this package batch.

Not run in this WIP gate: workspace-wide tests, `make lint`, or distributed
TiKV integration tests. Those belong to the Ready profile before overall task
completion or PR-readiness is claimed.

## Rust-only return-contract alignment (2026-09-07)

The complete current eight-artifact Go owner was re-read before this bounded
Rust follow-up: `BUILD.bazel` (50 lines), `execdetails.go` (699),
`execdetails_test.go` (1,371), `main_test.go` (32), `runtime_stats.go`
(1,458), `ruv2_metrics.go` (1,095), `tiflash_stats.go` (918), and `util.go`
(313), for 5,936 lines. The production declaration inventory covers the
execution-detail merge/string/zap methods; runtime-stat, hash-state, root/cop
snapshot, analyze-byte, concurrency, commit, and RU methods; RUv2 context,
counter, recorder, getter, calculation, and formatting methods; TiFlash
scan/columnar/wait/network methods; and context/percentile/duration helpers.
The test inventory covers every `Test*` in `execdetails_test.go` plus the
`TestMain` harness; there is no fixture, testdata, benchmark, fuzz, generated
input/output, platform-specific file, or build-tag variant in this package.

The Rust implementation owner is `tidb-util/src/ruv2_metrics.rs` (1,880
lines), re-exported by `tidb-exec/src/ruv2_metrics.rs` (17 lines) and consumed
by `tidb-exec/src/exec_details.rs` (1,520 lines). The 26 direct Go-shaped
returns were `RuV2Metrics::{new,bypass,result_chunk_cells,
executor_l5_insert_rows,plan_cnt,plan_derive_stats_paths,session_parser_total,
txn_cnt,resource_manager_read_cnt,resource_manager_write_cnt,write_keys,
write_size,tikv_kv_engine_cache_miss,tikv_coprocessor_executor_iterations,
tikv_coprocessor_response_bytes,tikv_raftstore_store_write_trigger_wb,
tikv_storage_processed_keys_batch_get,tikv_storage_processed_keys_get,is_zero,
calculate_ru_values}`, `total_ru`, `ExecutorMetricRecorder::available`,
`resolve_executor_metric`, and the three `format_ruv2_*` functions. Their
Rust-only `#[must_use]` diagnostics are removed; counter atomics, nil-option
handling, recorder resolution, formatting, and all consumer wiring are
unchanged.

`ruv2_returns_may_be_ignored_like_go` discards every corrected return under
`#[deny(unused_must_use)]`. The pre-fix compile failed with exactly 26
diagnostics; the post-fix focused test passes. The owner namespace passes all
10 tests, and `tidb-exec --all-targets` checks successfully. Pinned nightly
rustfmt, `git diff --check`, and the Ready `make lint` gate are required and
recorded for this package commit. No Go, Bazel, Cargo metadata, generated,
platform, or fixture artifact changed, so `make bazel_prepare` is not
required.
