# `pkg/util/slice` parity audit ExecPlan

## Objective

Keep the complete Go-master `pkg/util/slice` package aligned with its native
Rust owner while preserving the package's exact public surface and source
test identity.

## Progress

- [x] Read all four Go-master artifacts at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: `BUILD.bazel`, `main_test.go`,
  `slice.go`, and `slice_test.go` (149 lines total; three production
  functions, `TestMain`, and one four-row test).
- [x] Confirm there are no package docs, fixtures, generated/platform
  variants, benchmarks, fuzz targets, or nested packages.
- [x] Reconcile Rust `tidb-util::slice` with Go's empty-slice truth,
  short-circuiting predicate, signed decimal formatting, nil-versus-empty
  clone distinction, and source-derived `TestSlice` rows. No Rust-only
  behavior or missing Go behavior remains.
- [x] Revalidate current and exact detached Go-master tests, the source-derived
  Rust test, the only production consumer, Rust formatting, and diff quality.
- [ ] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or Bazel
file changed, so `make bazel_prepare` is not required. The receipt records the
exact commands and boundaries in `rust/testport/receipts/util_slice.md`.

## Next boundary

Any future slice change must preserve Go's nil/empty distinction, predicate
short-circuiting, signed base-ten conversion, and the four source test cases.
Do not add generic Rust-only slice algorithms or diagnostics to this owner.
