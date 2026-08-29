# `pkg/statistics/handle/internal` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly two artifacts, both read in full and byte-compared
against the pin:

- `BUILD.bazel` — one internal Go library over `testutil.go`, visible only to
  statistics-handle subpackages;
- `testutil.go` — `AssertTableEqual` over real `statistics.Table` values.

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
