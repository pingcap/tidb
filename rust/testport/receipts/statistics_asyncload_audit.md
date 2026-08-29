# `pkg/statistics/asyncload` package audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 23 | `c1bb9dc8c8ee2a2282387d0bc0eca93a1db61d9d` | `ef2c216b18c3077813f1b894b95e0fb57da24323489bdf588482ea6c77a4c967` | build metadata inventoried |
| `async_load.go` | 120 | `2a35e37483fa1ffe1d27b22a1a3af8fb24bcf87b` | `422c0649b2e9d10e42b0542a7c2cd44c5346b6beca5d11035770f0aeeff0bbe2` | unclaimed: integrated producers and consumers are absent |
| `async_load_test.go` | 267 | `ac4331b0cca610b7256f5088445defdff178e52d` | `649715b90eb3e2e96dfcfa6074168cb91ede3672d9be0d5955f39973f57bd362` | five integration tests inventoried; not ported |

The package has no generated, platform-specific, benchmark, fixture, or other
support artifacts.

## Package behavior and blockers

The package owns one process-global, 128-shard map keyed by
`model.TableItemID`. Statistics validity checks in the parent package insert
requests directly. Statistics-handle storage consumes and deletes them, while
DDL/schema changes remove obsolete table, column, and index requests. The five
external tests verify those cleanup paths through real domain, handle,
storage, and SQL execution.

Rust already has the source-owned `TableItemID` and `StatsLoadItem` in
`tidb-model`, but it does not have the integrated root statistics handle,
storage loader, or DDL cleanup paths required by this package. A standalone
global map would accumulate requests without Go's consumer and cleanup
behavior. The package therefore remains explicitly unclaimed.

## Removed non-parity carriers

The removed `tidb-stats::async_load` module duplicated both model types,
exposed a public constructor and `is_empty` API absent from Go, and created
only caller-owned maps instead of Go's process-global queue. Its five unit
tests were source-absent substitutes for Go's five integration tests.

The parent statistics carrier also returned a synthetic `load_request` from
`column_stats_validity` and `index_stats_validity`. Go has no such result or
caller contract: both methods mutate the global queue and return only a
boolean. Those alternate APIs and their source-absent tests were removed with
the disconnected map.

## Validation

WIP profile: removal of disconnected carriers is checked through the affected
statistics crate.

- `cargo check --locked -p tidb-stats`
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- changed-file `rustfmt --edition 2021 --check`
- `git diff --check`
