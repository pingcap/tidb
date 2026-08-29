# `pkg/statistics/handle/usage/indexusage` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full and byte-compared
against the pin:

- `BUILD.bazel` — one public library and one short, flaky, four-shard test
  target;
- `collector.go` — index identity, sample/bucket construction, pooled map
  merge, global/session/statement collectors, worker lifecycle, and
  `model.TableInfo`-driven garbage collection;
- `collector_test.go` — four tests and the three-case parallel
  `BenchmarkIndexCollector`.

There is no `doc.go`, fixture, generated source/input, or build/platform
variant.

## Rust ownership and behavior

`rust/crates/tidb-stats-handle-usage-indexusage` is the package owner:

- `GlobalIndexId` keys samples by the exact table/index pair;
- `Sample` preserves wrapping counters, seven percentage buckets, maximum
  last-use time, and Go's year-1 zero `time.Time`;
- `new_sample` preserves all boundary behavior, including the last bucket when
  table row count is zero;
- global, session, and statement collectors delegate to the completed generic
  collector and preserve nonblocking report, synchronous flush, and per-
  statement query-count deduplication;
- pending maps cross the channel as shared map headers, matching Go map sends
  without a full Rust map clone;
- GC accepts actual shared `tidb_model::TableInfo` values and scans their real
  nullable index-pointer slices, including the source nil-dereference boundary;
- all four source tests run under the owner with the exact 64 sessions ×
  100,000 operations concurrency workload;
- the source parallel benchmark is executable with report frequencies 1, 4,
  and 8.

The former flattened `tidb-stats::index_usage` module was removed together
with its public private-source constants, public map snapshot, public test
accessor, collector `Default`, three duplicate aggregate tests, and five
supplemental tests. The parent usage package's direct metadata-closure GC test
was also removed because it bypassed Go's DDL/testkit integration. Session and
server consumers now depend on this package directly.

## Validation

Profile: WIP. This completes one atomic package in the continuing parity audit,
not a repository-wide readiness claim.

- Complete pinned-package inventory/diff gate: passed.
- Pinned Go package test: passed.
- `cargo test -p tidb-stats-handle-usage-indexusage`: passed, 4 tests.
- Exact concurrent Rust source test: passed at 6,400,000 operations.
- Source benchmark compiled in optimized bench profile.
- `cargo check -p tidb-stats`, `cargo check -p tidb-session -p tidb-server`,
  and the targeted session information-schema test passed.
- Full `tidb-stats` translated integration suite passed after removal of the
  duplicate, supplemental, and workaround tests.
- Scoped Rust formatting and `git diff --check`: passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk and unverified boundaries

- Correctness: native shared map headers and actual model pointers preserve
  the source ownership and schema lookup behavior; time uses UTC instants and
  converts to local time at the existing information-schema consumer.
- Compatibility: session and server type paths changed to the package owner;
  all production consumers compile and the user-visible row test passes.
- Performance: report no longer clones the accumulated map; the exact source
  concurrency workload and optimized benchmark compilation cover the hot path.
- Repository-wide lint and integration suites remain deferred to the Ready
  profile after the full parity goal is complete.
