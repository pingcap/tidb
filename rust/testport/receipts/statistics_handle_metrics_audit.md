# `pkg/statistics/handle/metrics` atomic audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete package inventory

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `ca1dc316ff4ce0c18e9fc099ad7d18b21421b12e` | `f09ecfda96cdc200101b97b3239d7f8c32c41486c690956c4fa6d1aee327a6a5` | build metadata inventoried |
| `metrics.go` | 86 | `58368d90ad8c60034f1d30057d4d39298a49cc20` | `7e41be959ff5d3167985b4007cfe612d7b22f88ba675472de71acf07b9858204` | production behavior inventoried; unclaimed pending dependency |

All 98 lines were read. The package has no `doc.go`, original test, test
support, fixture, generated input or output, build-tag or platform variant,
benchmark, fuzz target, or example. `BUILD.bazel` defines one ordinary public
Go library and names the two runtime dependencies.

## Package behavior

The package declares ten ordered statistics-health bucket identities and the
matching exported configuration slice. The seven numeric buckets have
exclusive upper bounds `50`, `55`, `60`, `70`, `80`, `100`, and `101`; the
three special categories have non-positive bounds. Labels, including the
compatibility-preserving total label `[0,100]`, are observable Prometheus label
values.

Package initialization calls `InitMetricsVars`. That function panics when the
mutable configuration slice no longer contains exactly ten entries, creates
one gauge child per configured label in slice order, and binds the
`dump/success` and `dump/fail` children of the shared historical-statistics
counter. Calling it again rebinds all package variables against the current
shared parent collectors.

## Blocking atomic dependency

Both parent collectors belong to the separate pinned `pkg/metrics` package:
`StatsHealthyGauge` and `HistoricalStatsCounter` are constructed by
`InitStatsMetrics` in `pkg/metrics/stats.go`, recreated by
`pkg/metrics.InitMetrics`, and registered with the default registry by
`pkg/metrics.RegisterMetrics`. Collector identity, reset/reinitialization,
registration, gathering, and child sharing are therefore owned by that whole
package rather than this leaf.

The dependency has 60 pinned artifacts: 33 Go files, three build/ownership
artifacts, and 24 dashboards, rules, scripts, and documentation artifacts. The
two directly relevant production files are:

| Dependency artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `pkg/metrics/stats.go` | 198 | `3190fb120ab927985590a7b280bf7d9588218944` | `61939f2a426d04e7f1de1f0eb20e2a9fc370cd875931f73df02ebd02e6cfa38c` |
| `pkg/metrics/metrics.go` | 623 | `3bc6342ba45813c417e1c40b86a7c620abda5d81` | `a12468b525d8dca15f3d2f39b6241453928f9da7703fe9ecb8783995b1eb3b4a` |

Rust has no completed owner for that 60-artifact package. The existing
`tidb-stats-handle-metrics` crate privately constructs and immediately
registers only the parent collectors needed by this statistics leaf and by a
separate domain-metrics seed. Consequently it does not preserve Go's shared
owner, complete initialization/reset sequence, or registration lifecycle.
Its immutable fixed bucket array also cannot exercise Go's exported mutable
slice and length-mismatch panic. The wired cache, historical-dump, and domain
call sites remain useful seed integration, but they are not an atomic package
completion claim.

Constructing another leaf-local collector or extending this fragment would be
a workaround, not parity. The required next unit is the complete pinned
`pkg/metrics` package, including all 60 artifacts and its initialization,
registration, tests, dashboards/rules, generated/support inputs, and validation
gate. Only after that owner lands can this leaf bind to the shared parents and
be claimed complete.

## Removed non-parity tests

Pinned `pkg/statistics/handle/metrics` has no tests. The Rust-only
`source_bucket_configs_match_go_order_and_labels` and
`source_init_binds_all_gauges_and_historical_counters` tests asserted the
partial private-carrier representation and were removed. The test for the
separate `pkg/domain/metrics` seed remains outside this package's disposition.

## WIP validation

- PASS: `cargo test --locked -p tidb-stats-handle-metrics -- --nocapture`
  (the one remaining test belongs to the separate domain-metrics seed)
- PASS: `cargo check --locked -p tidb-stats-handle-cache -p tidb-domain -p tidb-server`
  (pre-existing warnings remain)
- PASS: `rustfmt --edition 2021 --check crates/tidb-stats-handle-metrics/src/lib.rs`
- PASS: `git diff --check`

No Go, Bazel, or module file changed, so `make bazel_prepare` is not required.
The primary integration batch owns the Ready-profile `make lint` run.
