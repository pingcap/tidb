# `pkg/statistics/handle/cache/internal/lfu` parity audit ExecPlan

## Objective

Audit the complete LFU package as one atomic Go unit: BUILD metadata, both
secondary-shard helpers, the Ristretto-backed production cache, and every
source test. Keep the native Rust owner aligned with Go's table-valued costs,
admission/callback lifecycle, metadata retention, and capacity controls without
claiming parity that cannot be proved for the external Ristretto dependency.

## Progress

- [x] Read all five Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: 782 lines, ten source tests,
  race-enabled/flaky ten-shard BUILD target, and no fixtures, generated
  inputs/outputs, benchmarks, or platform variants.
- [x] Read the complete Rust LFU crate, its Stretto callback/key semantics, the
  shared cache trait, direct consumers, and the test utility dependency.
- [x] Correct the signed shard-index behavior: Go's negative remainder now
  produces the same invalid-index panic instead of Rust's unsigned modulo
  routing. The regression failed before the fix and passes afterward.
- [x] Restore Go's test-mode zero-quota override (`5_000_000`) in the native
  test constructor. Its capacity regression failed before the fix and passes
  afterward.
- [x] Refresh the audit receipt, parent statistics plan, and top-level
  test-port plan with the exact Go-master hashes and validation evidence.

## Validation gate

Ready profile for the Rust code/test batch:

- [x] Current and detached Go suites pass with `-tags=intest,deadlock`.
- [x] Detached Go suite passes with `-race`, matching the BUILD target.
- [x] Eight focused Rust owner tests pass, including both regressions.
- [x] Offline locked crate clippy with `-D warnings`, Rust formatting, pinned
  repository lint, and `git diff --check` pass.
- [x] Commit, push, pull, and remote SHA verification complete.

`make bazel_prepare` is not required because no Go/Bazel source, import
section, test target, or module dependency changed.

## Open boundary

The Go package delegates its correctness-critical admission, TinyLFU counters,
buffering, callbacks, and eviction ordering to external
`github.com/dgraph-io/ristretto`. The Rust `stretto` owner is executable seed
and integration evidence, but package-complete parity remains unclaimed until
that external dependency has a complete pinned owner or an explicitly verified
compatibility boundary.

## Next boundary

Audit `pkg/statistics/handle/cache/internal/testutil` as the next independent
support package, then reconcile the parent cache package only after all three
internal implementations and their external dependency boundaries are
receipted.
