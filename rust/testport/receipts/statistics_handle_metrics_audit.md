# `pkg/statistics/handle/metrics` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 12 | `ca1dc316ff4ce0c18e9fc099ad7d18b21421b12e` | build metadata inventoried |
| `metrics.go` | 86 | `58368d90ad8c60034f1d30057d4d39298a49cc20` | unclaimed: shared metric owner absent |

The package has no generated, platform-specific, test, benchmark, fixture, or
support artifacts.

## Package behavior and blockers

The package defines ten ordered health buckets, validates their count during
initialization, and binds one gauge per label from the process-global
`pkg/metrics.StatsHealthyGauge` vector. It also binds success and failure
historical-stat counters from the shared `HistoricalStatsCounter` vector and
rebinds all handles when `InitMetricsVars` is called.

Metric collector identity and registration are observable behavior. Rust does
not have a complete atomic `pkg/metrics` owner, so private constants, labels,
or detached collectors cannot represent this package. The earlier
metadata-only Rust carrier and its supplemental tests were removed. The
package remains explicitly unclaimed.

## Validation

The earlier removal was validated through the affected statistics owner WIP
gate; this receipt records the already-completed audit and exact pinned
inventory.

- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `git diff --check`
