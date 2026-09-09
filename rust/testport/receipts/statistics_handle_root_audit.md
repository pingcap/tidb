# Root `pkg/statistics/handle` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 68 | `7cc4d3ca3fc5c4df3aad02f19256540f3a824b9c` | build metadata inventoried |
| `bootstrap.go` | 994 | `bf97d5ddd9a7dc24f9b2955ad6ad64d25f98b844` | unclaimed: ordinary handle/cache/storage owner absent |
| `bootstrap_test.go` | 74 | `f0e0d2e1a408f91205654379faf566fd0d0bc7be` | five tests inventoried; not ported atomically |
| `handle.go` | 341 | `dc3cb93805992080a66a1fe13218c7567178b90c` | unclaimed: composed runtime absent |
| `main_test.go` | 34 | `50e4289a822bc9e8712967b2b8e79e705e011db6` | test harness inventoried; not ported |

The package has no generated, platform-specific, benchmark, fixture, or other
support artifacts.

## Package behavior and blockers

The root package constructs the one ordinary statistics `Handle` and composes
its system-session pool, lease, metadata lookup, cache, GC, read/write,
history, usage, analyze, sync-load, locks, global stats, and DDL owners. It
registers the production notifier callback, owns collector attachment hooks,
flush/start/close lifecycle, and implements cache-or-pseudo table lookup with
temporary- and system-schema exclusions plus a system database-ID cache.

Bootstrap is a transactional, memory-aware cache build. It streams stats meta
and histograms, initializes existence and load status, decodes sketches,
selects tables from infoschema, pages work through the initstats worker,
loads index TopN and buckets, computes pre-scalars, synchronizes asynchronous
cache admission, and either replaces the global cache or publishes a targeted
refresh. Lite and full modes have distinct storage and lifecycle contracts.

Rust has individually audited lower-level statistics values and a complete
`initstats` worker crate, but it does not have this dependency-closed composed
handle. Several required child packages remain explicitly unclaimed,
including cache, storage, DDL, history, lockstats, globalstats, syncload, and
the shared handle interface family.

## Removed non-parity carriers

The `bootstrap_sql` module extracted only the two query builders exercised by
the five Go unit tests. It introduced a public `HistSqlOptions` whose invalid
states return `None`, whereas Go stores the private option shape and asserts
during generation. It did not execute queries, decode rows, populate a cache,
or participate in bootstrap. The `pseudo_cache_policy` module similarly
reduced `getStatsByPhysicalID` to three scalar inputs and omitted pseudo-table
construction, cache mutation, session/system-schema lookup, failpoints, and
errors. Both modules had no production consumer.

Those modules and their eight tests were removed. The package remains
explicitly unclaimed until the ordinary handle and its dependency graph can
land as one integrated owner.

## Validation

WIP profile: removal of disconnected carriers is checked through the affected
statistics crate.

- `cargo check --locked -p tidb-stats`
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- changed-file `rustfmt --edition 2021 --check`
- `git diff --check`
