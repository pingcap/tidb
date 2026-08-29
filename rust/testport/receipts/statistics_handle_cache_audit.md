# `pkg/statistics/handle/cache` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 60 | `78a789a6e5f8577700aefd04a914ad402776b225` | build and test metadata inventoried |
| `stats_table_row_cache.go` | 212 | `682b9ba91df93b62c49e4e5c379758aafd0d2e23` | unclaimed |
| `statscache.go` | 410 | `fd98ba1eb884e0b68b5daaa01ae8358a8d0d1c05` | unclaimed |
| `statscacheinner.go` | 197 | `68b6a08e8e21ce683d55b9ddd49777661f9ffd93` | unclaimed |
| `statscache_test.go` | 202 | `602fe311ace06d4ecbfb62bd64ded6191a2ef2cf` | unclaimed with production package |
| `bench_test.go` | 186 | `aaf4e64fd433a408d7393ead26cb78d27acce80e` | unclaimed with production package |

There are no generated or platform-specific variants and no external fixture
files. The `internal` and `metrics` directories are distinct Go packages and
have separate atomic receipts.

## Integrated behavior and blockers

The root package owns both the process-global SQL row-count/column-length
cache and the published statistics cache. Its behavior includes:

- SQL loading, partial map copy, partition/global-index length estimation,
  variable-width column accounting, and sequence row-count handling;
- atomic cache replacement and closure, LFU-versus-map selection from global
  configuration, synchronous copy-on-write and asynchronous quota updates;
- hit, miss, update, delete, cost, capacity, delta-load, and healthy-bucket
  metrics on the shared registered metric families;
- ordered/deduplicated storage refresh, context cancellation, table metadata
  reuse, batched updates/deletes, failpoint behavior, retry and max-version
  publication, eviction, capacity, and wait/close lifecycle;
- `TestCacheOfBatchUpdate`, `TestUpdateStatsHealthyMetrics`, six cache
  benchmarks, and their daily benchmark entrypoint.

The completed internal interface, map cache, and test support are necessary
but insufficient. The complete LFU package is blocked on the pinned external
Ristretto package, and both cache metric packages are blocked on the complete
shared `pkg/metrics` owner. The ordinary stats-handle/session/storage runtime
needed by `StatsCacheImpl.Update` is also not present as a complete owner.
Implementing a map-only cache or private metrics would change Go behavior.

## Removed non-parity carriers

- `BatchUpdate<T, F>` genericized the package-private table batch and exposed
  pending slices. Its three Rust tests split one Go test and added a zero-size
  case absent from the source.
- `max_stats_cache_version` reduced concurrent atomic publication and the
  update path to caller-provided arithmetic. Its three Rust tests have no Go
  counterparts in this package.
- `build_in_table_ids_string` exposed a private SQL helper without the row
  cache, SQL execution, map merge, or estimation behavior. Its two Rust tests
  have no Go counterparts.

All three modules and all eight tests were removed. The root Go package remains
explicitly unclaimed until its dependency-closed production and test surface
can land atomically.

## Validation

WIP profile: removal of disconnected helpers is checked through the affected
statistics owner gate.

- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `rustfmt --edition 2021 --check crates/tidb-stats/src/lib.rs`
- `git diff --check`
