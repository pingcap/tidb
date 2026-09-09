# `pkg/util/zeropool` parity audit ExecPlan

## Objective

Keep the complete Go-master generic zero-allocation pool aligned with its
native Rust owner, source test suite, and benchmark translations.

## Progress

- [x] Read all three Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: `BUILD.bazel`, `pool.go`, and
  `pool_test.go` (281 lines; three production methods, one four-subtest
  `TestPool`, and four benchmarks).
- [x] Confirm there are no package docs, fixtures, generated/platform
  variants, or nested packages.
- [x] Compare the Rust owner and benchmark: valid zero value, optional factory,
  concurrent get/put, move-out clearing, no-copy semantics, all four source
  sub-behaviors, and all four benchmark workloads are preserved. Rust-only
  supplemental tests and divergent pointer-policy behavior remain removed.
- [x] Revalidate current and exact detached Go-master tests, the source-owned
  Rust test, all-target/benchmark compilation, formatting, and diff quality.
- [x] Commit, push, pull, and verify `origin/hparser-integration`.
- [x] Re-audit the complete package and remove the remaining Rust-only
  `#[must_use]` diagnostic from the Go-shaped `Pool::new` constructor. Prove
  the mismatch with a detached pre-fix deny-on-discard regression, then pass
  the owner, benchmark-target, formatting, diff, and Ready lint gates.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or Bazel
file changed, so `make bazel_prepare` is not required. No regression test is
added because this batch changes no behavior; `TestPool` remains the focused
source-derived regression. Exact commands and boundaries are recorded in
`rust/testport/receipts/util_zeropool.md`.

## Next boundary

Any future pool change must preserve zero-value/default behavior, factory
construction, concurrent get/put, pointer clearing after get, four source
subtests, and benchmark identities. Do not reintroduce source-absent APIs or
pointer-policy diagnostics, including a discard-only constructor warning.
