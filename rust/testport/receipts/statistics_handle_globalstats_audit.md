# `pkg/statistics/handle/globalstats` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 73 | `fb61e9746f29b1d8b954fed30a4d86651dc6bc3a` | build metadata inventoried |
| `global_stats.go` | 389 | `c8d735bdbbd63db1e9e6f3d08970e30ad574631a` | unclaimed: handle/storage integration absent |
| `global_stats_async.go` | 548 | `d72e7e77cc1d23c8a4990e12a83738ab3b2ee481` | unclaimed: async storage pipeline absent |
| `global_stats_internal_test.go` | 477 | `74158a270a28c1dcc59912b54cb2b34523073dd3` | test support inventoried; not ported |
| `global_stats_test.go` | 999 | `e5282aa4f414f6203d8cd95813654bd039a35c40` | 26 integration tests inventoried; not ported |
| `main_test.go` | 34 | `4bc5c4791f0de4098074e85cb24cde54eaab10ee` | test harness inventoried; not ported |
| `merge_worker.go` | 170 | `6e5539f751c9691d29c81f62a8216f0aedc5f397` | unclaimed with the atomic package |
| `topn.go` | 211 | `19c4f25d147c46b21635d1ba0baaf2956c4da701` | unclaimed with the atomic package |
| `topn_bench_test.go` | 142 | `7b981381c0f9d457040e9182202e3a2384ce957d` | two benchmarks inventoried; not ported |
| `topn_test.go` | 113 | `8bd3898155979a788126599cdf2f0e67b7e359fc` | two tests inventoried; not ported |

The package has no generated, platform-specific, fixture, or other support
artifacts beyond the test harness and helpers above.

## Package behavior and blockers

The package is the ordinary statistics handle's complete partition-to-global
merge owner. It resolves table and partition metadata, selects columns or an
index, loads partition counts and sketches from storage, applies the strict or
skip-missing policy, and aggregates row and modify counts. The blocking path
loads all partition tables; the async path coordinates IO and CPU workers with
channels, panic recovery, failpoints, early-exit signals, and joined errors.
Both paths merge FMSketch NDV, CMSketch, TopN, and histograms in source order,
honor SQL cancellation and merge concurrency, clear bucket NDVs, and publish
each completed global histogram through the real stats handle.

The test package validates static/dynamic pruning, async and blocking modes,
worker panic/error coordination, counts and health, column/index types,
versions, DDL changes, NDV, global indexes, planner estimates, SQL bindings,
empty histograms, and sequential/concurrent TopN results. Rust lacks the
dependency-closed ordinary handle, storage readers/writers, session and
infoschema integration needed to execute that contract.

## Removed non-parity carriers

Rust exposed a synthetic zero-layout struct, a boolean-to-integer SQL helper,
a task range type, and a standalone TopN merge pipeline. None had a production
consumer. The TopN pipeline substituted caller-supplied MySQL type, collation,
concurrency, and killer values and replaced Go's pool/channels and error
behavior with Rust threads, mutex queues, clamped ranges, and new public result
and error types. Even where its arithmetic followed `topn.go`, it did not form
the complete `globalstats` package or exercise the original integration tests.

All four modules, their tests, two ignored empty tests, and stale receipt
claims were removed. The package remains explicitly unclaimed until the
ordinary handle/storage/session dependency chain can land and the whole source
test surface can execute.

## Validation

WIP profile: removal of disconnected carriers is checked through the affected
statistics crate.

- `cargo check --locked -p tidb-stats`
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- changed-file `rustfmt --edition 2021 --check`
- `git diff --check`
