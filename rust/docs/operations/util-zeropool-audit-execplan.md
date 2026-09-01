# `pkg/util/zeropool` parity audit ExecPlan

## Objective

Keep the complete Go-master generic zero-allocation pool aligned with its
native Rust owner, source test suite, and benchmark translations.

## Progress

- [x] Read all three Go-master artifacts at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: `BUILD.bazel`, `pool.go`, and
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
- [ ] Commit, push, pull, and verify `origin/hparser-integration`.

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
pointer-policy diagnostics.
