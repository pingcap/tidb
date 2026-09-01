# `pkg/util/size` parity audit ExecPlan

## Objective

Keep the complete Go-master size-constant package aligned with the native Rust
owner and preserve Go's ABI-oriented accounting values rather than Rust data
structure layouts.

## Progress

- [x] Read both Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: `BUILD.bazel` and `size.go`
  (86 lines total; five binary units and fifteen ABI constants).
- [x] Confirm there are no package docs, tests, fixtures, generated/platform
  variants, benchmarks, fuzz targets, or nested packages.
- [x] Compare `tidb-util::size`: architecture-width values derive from the
  target word size, while slice/string/interface/function/map constants retain
  Go header sizes. No Rust-only behavior or missing Go behavior remains.
- [x] Revalidate current and exact detached Go-master package checks, the Rust
  utility owner check, formatting, and diff quality.
- [x] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or Bazel
file changed, so `make bazel_prepare` is not required. No regression test is
added because the complete Go package has no source tests and this batch
changes no behavior. Exact commands and boundaries are recorded in
`rust/testport/receipts/util_size.md`.

## Next boundary

Any future size change must preserve the five binary units, signed `int64`
constant type, architecture-width rules, and Go ABI header assumptions. Do not
substitute Rust container sizes or add cache-only diagnostics.
