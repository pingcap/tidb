# `pkg/util/texttree` parity audit ExecPlan

## Objective

Keep the complete Go-master text-tree formatting package aligned with its
byte-preserving Rust owner and planner/plancodec consumers.

## Progress

- [x] Read all four Go-master artifacts at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: `BUILD.bazel`, `main_test.go`,
  `texttree.go`, and `texttree_test.go` (174 lines; five rune constants, two
  production functions, TestMain, and two source tests).
- [x] Confirm there are no package docs, fixtures, generated/platform
  variants, benchmarks, fuzz targets, or nested packages.
- [x] Compare Rust `tidb-util::texttree`, plancodec, and planner/explain:
  Unicode indentation, invalid-byte Go string iteration, byte-preserving IDs,
  and both source matrices match Go. Valid-UTF-8 narrowing, Rust-only
  `must_use` policy, and supplemental tests remain removed.
- [x] Revalidate current and exact detached Go-master tests, both Rust owner
  tests, the planner consumer library, formatting, and diff quality.
- [ ] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or Bazel
file changed, so `make bazel_prepare` is not required. No regression test is
added because this batch changes no behavior; both source test translations
remain the focused regressions. Exact commands and boundaries are recorded in
`rust/testport/receipts/util_texttree.md`.

## Next boundary

Any future tree-format change must preserve Go rune iteration over indentation,
raw identifier bytes, five tree characters, and both source matrices. Convert
to UTF-8 only at source-guaranteed consumer boundaries.
