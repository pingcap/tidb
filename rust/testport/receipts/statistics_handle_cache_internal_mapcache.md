# `pkg/statistics/handle/cache/internal/mapcache` → `tidb-stats-handle-cache-internal-mapcache`

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Rust owner |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 12 | `38c800e866f91ee3ceb92ad73acb1a8d9e7a7089` | workspace member and crate manifest |
| `map_cache.go` | 139 | `9970497512d339273e71d3745be74ad766725438` | `src/lib.rs` |

The package has no generated, platform-specific, test, fixture, or benchmark
artifacts.

## Behavior mapping

- The cache stores shared actual `tidb_stats::Table` values keyed by signed
  table ID.
- `put` derives cost from `Table::memory_usage().total_mem_usage`; replacement
  subtracts the stored old cost and adds the new cost with Go-style wrapping
  signed arithmetic.
- `del`, `cost`, `keys`, `values`, and `len` preserve the source map behavior
  and unspecified iteration order.
- `copy` creates an independent map and aggregate counter while retaining the
  same shared table pointers and per-item key/cost values.
- `set_capacity`, `close`, `trigger_evict`, and `wait_for_async_updates` are
  exact no-ops and the type implements the package-owned `StatsCacheInner`.

The former `tidb-stats::MapCache<V>` was removed because it accepted arbitrary
values and caller-supplied costs, exposed source-absent `Default` and
`is_empty` behavior, and carried three tests not present in the pinned Go
package.

## Validation

WIP profile: the source package has no tests, so the package gate compiles and
lints the complete production implementation and reruns the affected owner.

- `cargo check --locked -p tidb-stats-handle-cache-internal-mapcache`
- `cargo clippy --locked -p tidb-stats-handle-cache-internal-mapcache --no-deps -- -D warnings`
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `rustfmt --edition 2021 --check crates/tidb-stats-handle-cache-internal-mapcache/src/lib.rs crates/tidb-stats-handle-cache-internal/src/lib.rs crates/tidb-stats/src/lib.rs`
- `git diff --check`
