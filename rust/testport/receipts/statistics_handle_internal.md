# `pkg/statistics/handle/internal` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 12 | `60ab59d4a6312c6eaccb51277ecdc82d841a1fd5` | `0f7f5e706a9e7d5f5a16309908dd8339df7e2c1ff5a65851b28f1bc9cf558fce` | internal-library visibility and dependencies inventoried |
| `testutil.go` | 56 | `dfbc67eb07ce895f49e2e188eb1f16717b67bd40` | `e494eb866d5de5fc4b3583b27ff32e166d0d4fccae065d8aab2649913696c78f` | `AssertTableEqual` mapped below |

There is no `doc.go`, package test, fixture, benchmark, generated source/input,
or build/platform variant.

## Rust ownership and behavior

`rust/crates/tidb-stats-handle-internal` is the package owner and operates on
the ordinary `tidb_stats::Table` graph:

- row and modification counts and column/index cardinalities must match;
- every left-side column and index must exist on the right by ID;
- histograms compare through Go's `ToString(0)` projection, including
  per-bucket rather than cumulative counts and raw datum string bytes;
- CMSketch uses complete structural equality;
- TopN preserves Go's nil/empty equivalence and otherwise compares total
  count, length, encoded value, and entry count in order;
- both column/index existence maps must be present and equal.

The former `tidb-stats::stats_table_snapshot` module accepted caller-authored
opaque encodings instead of invoking any of those behaviors. It and its three
source-absent tests were removed. The Go package itself contains no tests, so
the Rust owner adds none.

## Validation

Profile: WIP. This completes one atomic support package in the continuing
parity audit, not a repository-wide readiness claim.

- Complete pinned-package inventory/diff gate: passed.
- Pinned Go package test: passed; the package has no test files.
- `cargo check -p tidb-stats-handle-internal`: passed.
- `cargo test -p tidb-stats-handle-internal`: passed; the package has no tests.
- Full `tidb-stats` translated integration suite: passed after removal of the
  three supplemental tests.
- Scoped Rust formatting and `git diff --check`: passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk and unverified boundaries

- Correctness: the helper intentionally retains Go's textual histogram
  comparison rather than Rust's stricter derived equality.
- Compatibility: this is test support and has no production runtime path;
  future audited statistics-handle tests can consume it directly.
- Performance: comparison allocates the same diagnostic projection Go does;
  it is not used in production.
- Repository-wide lint and integration suites remain deferred to the Ready
  profile after the full parity goal is complete.
