# `pkg/statistics/handle/types` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 23 | `9d9f114c895299ceddddb54287b1836f4142c2b5` | build metadata inventoried |
| `interfaces.go` | 537 | `6ad61fdcb4c10cc9609c951f0e6a5016578f71d6` | unclaimed: crate boundary incomplete |

The package has no generated, platform-specific, test, benchmark, fixture, or
other support artifacts.

## Package behavior and blockers

This package is the shared contract for the entire ordinary statistics
handle. It owns the GC, usage, history, analyze, cache, lock, read/write,
synchronous-load, global-statistics, and DDL interfaces plus their shared
request/result data structures, then composes them into `StatsHandle`. The
interfaces use common info-schema, session context, statement context,
statistics graph, storage JSON, notifier, pool, and SQL-executor types.

Rust currently places statistics values and several handle-like facilities in
`tidb-stats`. Making that crate depend on a handle-types crate which itself
must reference the statistics graph would create a dependency cycle. The
complete package therefore requires separating statistics core values from
the ordinary handle implementation and consolidating the existing scattered
runtime ports. It remains explicitly unclaimed until that structural boundary
is dependency-closed.

## Removed non-parity surface

The one concrete shared payload currently consumed by Rust lock execution is
`StatsLockTable`. Its fields directly preserve Go's full name and nil-versus-
allocated partition map distinction, so callers retain it and now construct
it as the plain struct Go defines. Rust's public `new` convenience API and two
source-absent tests were removed; neither exists in the pinned package.

## Validation

WIP profile: the API removal is checked through both affected owners.

- `cargo check --locked -p tidb-stats -p tidb-exec`
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `cargo nextest run --locked -p tidb-exec -E 'test(/lock_stats_exec_source/)' --no-fail-fast`
- `rustfmt --edition 2021 --check crates/tidb-stats/src/stats_lock_table.rs crates/tidb-exec/src/lock_stats_exec.rs`
- `git diff --check`
