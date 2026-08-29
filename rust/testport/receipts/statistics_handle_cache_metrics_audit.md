# `pkg/statistics/handle/cache/metrics` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 14 | `719f0a3357c85bfb7a938f5abce618808ffc1686` | build metadata inventoried |
| `metrics.go` | 55 | `c32e59ffcca683bed0884605d4ffe385849f9d43` | unclaimed: runtime dependency absent |

The package has no generated, platform-specific, test, fixture, or benchmark
artifacts.

## Behavior and blocker

Package initialization calls `InitMetricsVars`. That function binds six
exported counters (`miss`, `hit`, `update`, `del`, `evict`, and `reject`) and
two exported gauges (`track` and `capacity`) to child handles of the shared
`pkg/metrics.StatsCacheCounter` and `pkg/metrics.StatsCacheGauge` vectors.

The pinned `pkg/metrics` package is not a completed Rust owner. Its direct
package inventory contains 33 artifacts, including the construction and
default-registry registration of these two shared vectors. Creating private
vectors in this leaf would change collector identity, registration, resets,
gathering, and every caller that uses the shared parent handles. It would not
be Go parity.

The former Rust `cache_metrics_labels` module retained only the eight label
strings and added two tests that do not exist in the pinned package. It had no
counters, gauges, initialization, registration, or update behavior. The
module and both tests were removed. This Go package remains explicitly
unclaimed until the complete shared `pkg/metrics` dependency is available.

## Validation

WIP profile: removal of a disconnected carrier is checked through the affected
statistics owner gate.

- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `rustfmt --edition 2021 --check crates/tidb-stats/src/lib.rs`
- `git diff --check`
