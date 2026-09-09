# `pkg/statistics/handle/internal` — complete package transcreation

Pinned Go source: `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete inventory

The package has exactly two artifacts and 68 lines, both read in full from the
detached Go-master worktree before this authority refresh:

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 12 | `60ab59d4a6312c6eaccb51277ecdc82d841a1fd5` | internal-library visibility and dependencies inventoried |
| `testutil.go` | 56 | `dfbc67eb07ce895f49e2e188eb1f16717b67bd40` | `AssertTableEqual` mapped below |

There is no `doc.go`, package test, fixture, benchmark, fuzz target, generated
source/input, or build/platform variant.

## Rust ownership and behavior

`rust/crates/tidb-stats-handle-internal` is the single test-support owner and
operates on the ordinary `tidb_stats::Table` graph:

- row and modification counts and column/index cardinalities must match;
- every left-side column and index must exist on the right by ID;
- histograms compare through Go's `ToString(0)` projection, including
  per-bucket rather than cumulative counts and raw datum string bytes;
- CMSketch uses complete structural equality;
- TopN preserves Go's nil/empty equivalence and otherwise compares total
  count, encoded values, and entry counts in order;
- both column/index existence maps must be present and equal.

The former `tidb-stats::stats_table_snapshot` module accepted caller-authored
opaque encodings instead of invoking these behaviors. It and its three
source-absent tests remain removed. The Go package itself contains no tests,
so the Rust owner adds none.

## Validation

Profile: Ready. This is one atomic support-package authority refresh inside
the continuing repository-wide parity audit, not a whole-repository claim.

- Complete pinned-package inventory/diff gate passed; current source is byte
  identical to c605 for this package.
- Current and detached Go package probes passed (`[no test files]`).
- `cargo test -p tidb-stats-handle-internal`: passed (zero tests, matching the
  source inventory).
- `cargo check -p tidb-stats-handle-internal`: passed.
- Rust formatting, the pinned repository lint gate, scoped diff hygiene,
  commit integrity, push, pull, and remote SHA verification pass.

No Go, Bazel, or module file changed in this batch, so `make bazel_prepare`
was not required.

## Risk and unverified boundaries

- Correctness: textual histogram comparison intentionally retains Go's
  `ToString(0)` projection rather than replacing it with stricter derived
  equality; nil/empty TopN semantics are explicit.
- Compatibility: this remains test support with no production runtime path;
  opaque caller-encoded snapshots are not accepted.
- Performance: comparison allocates the same diagnostic projection Go does and
  is not used in production.
- Broad integration and RealTiKV suites were not run because this
  source-test-free helper is covered by its owner compile and downstream
  statistics consumer gates.
