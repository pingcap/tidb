# `pkg/statistics/handle/usage/indexusage` — complete package transcreation

Pinned Go source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (Go `master` at
the audit boundary).

## Complete inventory

The package has exactly three artifacts, all read in full and byte-compared
against the pin:

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 22 | `731370c58dc675932c90b1b7a8995b2d52a7532c` | `33fc29495f98cdf5e845058063c3e2a998ed4b9fc5489ab94b72694e4413140c` |
| `collector.go` | 287 | `f5811f27d72cd9a0b0f48ddfafc4a568d5a8ed99` | `c6c646b6a9a0d54a426f0e101286dc25fefa56c17152b3cf16d84d8cf9a7de8c` |
| `collector_test.go` | 259 | `8cd6bc1b3d8c31cffb41a06f0fe00ce4daacf353` | `6708bcd4affef549741ee4eb2630183f9883567910fae0a9311d88876e2740ec` |

The production/test surface is 568 lines. Every function, test, benchmark,
and BUILD attribute was read; the current checkout is byte-identical to this
pin.

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
- the source parallel benchmark is represented by the owner benchmark with
  report frequencies 1, 4, and 8. Rust uses a fixed operation workload and
  native thread scope because Go's `testing.B`/`RunParallel` timing harness
  has no direct equivalent; benchmark timing is not a correctness claim.

The former flattened `tidb-stats::index_usage` module was removed together
with its public private-source constants, public map snapshot, public test
accessor, collector `Default`, three duplicate aggregate tests, and five
supplemental tests. The parent usage package's direct metadata-closure GC test
was also removed because it bypassed Go's DDL/testkit integration. Session and
server consumers now depend on this package directly.

## Validation

Profile: Ready. This completes one atomic package in the continuing parity
audit, not a repository-wide readiness claim.

- Complete pinned-package inventory/diff gate: passed.
- Current and detached exact-master Go package tests: passed.
- `cargo test --manifest-path rust/Cargo.toml --offline --locked -p
  tidb-stats-handle-usage-indexusage`: passed, 4 tests, including the exact
  6,400,000-operation concurrent source workload.
- `cargo check --manifest-path rust/Cargo.toml --offline --locked -p
  tidb-stats-handle-usage-indexusage --benches`: passed.
- Rust consumers (`tidb-stats`, `tidb-session`, and `tidb-server`) compile with
  the owner; the source-derived statistics integration suite remains covered.
- Ready Rust formatting, pinned repository lint, and `git diff --check`: passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk and unverified boundaries

- Correctness: native shared map headers and actual model pointers preserve
  the source ownership and schema lookup behavior; time uses UTC instants and
  converts to local time at the existing information-schema consumer.
- Compatibility: session and server type paths changed to the package owner;
  all production consumers compile and the user-visible row test passes.
- Performance: report no longer clones the accumulated map; the exact source
  concurrency workload and optimized benchmark compilation cover the hot path.
- Broader repository and integration suites remain outside this package-scoped
  gate and are tracked by the continuing parity ExecPlan.

## Follow-up: discardable index-usage API returns (2026-09-06)

The complete three-artifact, 568-line Go package was rechecked at current
`origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; it remains
byte-identical to the earlier pin. The production collector, four source
tests, three-case benchmark, and Bazel metadata remain the entire package,
with no docs, fixtures, generated inputs/outputs, fuzz targets, or platform
variants. The Rust owner inventory is `Cargo.toml`, `src/lib.rs`,
`src/tests.rs`, and `benches/collector.rs`.

Go callers may discard the results of `NewSample`, `NewCollector`,
`Collector.GetIndexUsage`, `Collector.SpawnSessionCollector`, and
`NewStmtIndexUsageCollector`. Rust had added `#[must_use]` to all five direct
counterparts. Those annotations were removed without changing sample
bucketing, pooled-map ownership, report/flush behavior, or statement-level
deduplication. The focused in-owner regression invokes all five APIs under
`#[deny(unused_must_use)]`; it failed before the production edit with exactly
five diagnostics and passes afterward.

Ready validation for this follow-up:

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-stats-handle-usage-indexusage --lib tests::source_return_values_may_be_ignored_like_go --offline --locked -- --exact --nocapture --test-threads=1
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-stats-handle-usage-indexusage --offline --locked -- --test-threads=1
PASS; 5 unit tests passed, 0 failed; doc tests had 0 tests.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-stats-handle-usage-indexusage --all-targets --offline --locked
PASS; includes the benchmark target.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

Only Rust source/tests and parity documentation changed. No Go, Bazel, Cargo
metadata, or module dependency changed, so `make bazel_prepare` is not
required.
