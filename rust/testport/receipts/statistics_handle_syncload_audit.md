# `pkg/statistics/handle/syncload` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 55 | `0aa0a175d3573ce6e6c1e6d562ceb9d980594f3a` | build metadata inventoried |
| `stats_syncload.go` | 619 | `7e33bce5a591c0aa57ed5379c9551c49fbf23fe2` | unclaimed: runtime dependencies absent |
| `stats_syncload_test.go` | 446 | `8b703eb4951d10fea67c81201ca49608e49987d1` | six tests inventoried; not ported |

The package has no generated, platform-specific, benchmark, fixture, or other
support artifacts.

## Package behavior and blockers

The package derives worker concurrency from live process parallelism, creates
configuration-sized urgent and timeout queues, and deduplicates concurrent
loads through process-global singleflight. It filters requests against actual
cache load status, attaches result channels to statement context, prioritizes
urgent work, demotes timed-out work without blocking, recovers worker panics,
retries failed loads with jitter, and records shared metrics. Workers borrow
high-priority sessions, read real histogram, CMSketch, and TopN storage, handle
missing objects and partial/full load states, and publish serialized cache and
version/existence updates. Closing the package coordinates worker exit and
request completion.

Rust does not yet have the dependency-closed root statistics cache, ordinary
statistics handle/session and storage runtime, shared `pkg/metrics` owner, or
the statement-context execution seam required to preserve those behaviors.
The package therefore remains explicitly unclaimed.

## Removed non-parity carriers

`sync_load_concurrency_for_cpu` accepted a caller-supplied CPU count and
exposed only four threshold branches from an unexported implementation detail.
Pinned Go reads `GOMAXPROCS(0)` itself and uses the result to size the actual
worker system; its six tests exercise concurrent storage/cache behavior and do
not test this arithmetic independently. The scalar module, one source-absent
threshold test, and six ignored empty test functions were removed.

## Validation

WIP profile: removal of disconnected carriers is checked through the affected
statistics owner gate.

- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `rustfmt --edition 2021 --check crates/tidb-stats/src/lib.rs crates/tidb-stats/tests/statistics_part6_source.rs`
- `git diff --check`
