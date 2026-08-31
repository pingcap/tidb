# `pkg/statistics/handle/metrics` parity receipt

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 12 | `ca1dc316ff4ce0c18e9fc099ad7d18b21421b12e` |
| `metrics.go` | 86 | `58368d90ad8c60034f1d30057d4d39298a49cc20` |

All 98 lines were read. The package has no generated, platform-specific, test,
benchmark, fixture, or support artifacts.

## Go behavior and Rust integration

The package defines the ten ordered health buckets, validates their count, and
binds one gauge per label from the process-global
`tidb_statistics_stats_healthy` family. It also binds the `dump/success` and
`dump/fail` children of `tidb_statistics_historical_stats` and rebinds these
package-owned handles when `InitMetricsVars` is called.

`tidb-stats-handle-metrics` preserves the exact bucket indices, exclusive
bounds, compatibility labels, shared collectors, child order, and rebinding
behavior. The cache package consumes these same gauge handles when it publishes
health distributions. The complete separate Go `pkg/domain/metrics` package is
represented by a distinct module with its own initializer and binding cell;
calling the handle package initializer therefore cannot rebind another Go
package's metric variables.

## Validation

- `cargo fmt --all`
- `cargo test -p tidb-stats-handle-metrics -- --nocapture`
- `cargo test -p tidb-stats-handle-cache source_healthy_metrics_use_exact_buckets -- --nocapture`

No Go or Bazel source changed, so `make bazel_prepare` was not required.
