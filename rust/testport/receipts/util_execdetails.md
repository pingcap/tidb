# `pkg/util/execdetails` parity receipt

Pinned source: TiDB `e2788410d8d696605e8cb002585877a063ccc909`.

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
