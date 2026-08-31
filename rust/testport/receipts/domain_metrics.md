# `pkg/domain/metrics` parity receipt

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 12 | `189665ed62dafedfd916f5895b644f61b8a7b401` |
| `metrics.go` | 52 | `2b19aaba8274219e6e20e61aa6be31b7eb20f6e0` |

All 64 lines were read. The package has no generated, platform-specific, test,
benchmark, fixture, or support artifacts.

## Go behavior and Rust integration

The package binds seven handles from process-global collector families: the
historical-stat generation success/failure counters, plan-replayer dump
success/failure counters, capture send/discard counters, and registered-task
gauge. `InitMetricsVars` rebinds all seven package variables together.

Rust maps the package to the distinct `domain_metrics` module within
`tidb-stats-handle-metrics`, where it shares the same process-global historical
collector as the statistics-handle package but owns a separate binding cell and
initializer. The server's historical-stat generator and the domain's
plan-replayer collector, channel, and dump paths consume the package handles at
the same success, failure, send, discard, and collection points as Go.

## Validation

- `cargo test -p tidb-stats-handle-metrics -- --nocapture`
- `cargo test -p tidb-domain plan_replayer -- --nocapture`
- `cargo check -p tidb-server --tests`
- `cargo fmt --all -- --check`
- `git diff --check`

No Go or Bazel source changed, so `make bazel_prepare` was not required.
