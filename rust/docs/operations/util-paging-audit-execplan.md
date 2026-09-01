# `pkg/util/paging` parity audit ExecPlan

## Objective

Keep the complete Go-master paging-policy formulas aligned with the Rust
utility owner and DistSQL default-policy consumer.

## Progress

- [x] Read all four Go-master artifacts at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: `BUILD.bazel`, `main_test.go`,
  `paging.go`, and `paging_test.go` (162 lines; six constants, two production
  functions, TestMain, and two source tests).
- [x] Confirm there are no package docs, fixtures, generated/platform
  variants, benchmarks, fuzz targets, or nested packages.
- [x] Compare Rust `tidb-util::paging` and `tidb-distsql` default consumption:
  minimum/cap constants, wrapping growth, excess-page rounding, logarithmic
  seek count, and both source tests match Go. Redundant DistSQL copies,
  Rust-only `must_use` policy, and supplemental overflow tests remain removed.
- [x] Revalidate current and exact detached Go-master tests, both Rust owner
  tests, the consumer default-authority test, formatting, and diff quality.
- [ ] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or Bazel
file changed, so `make bazel_prepare` is not required. No regression test is
added because this batch changes no behavior; both source test translations
remain the focused regressions. Exact commands and boundaries are recorded in
`rust/testport/receipts/util_paging.md`.

## Next boundary

Any future paging change must preserve Go's unsigned wrapping, minimum maximum
size, growth cap, geometric seek formula, and source test vectors. Keep
consumer defaults in DistSQL and do not duplicate policy implementations.
