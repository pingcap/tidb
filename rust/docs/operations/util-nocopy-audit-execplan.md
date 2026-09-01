# `pkg/util/nocopy` parity audit ExecPlan

## Objective

Keep the complete Go-master no-copy marker package aligned with its native
Rust owner without adding behavior beyond Go's vet-oriented lock methods.

## Progress

- [x] Read both Go-master artifacts at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: `BUILD.bazel` and `nocopy.go`
  (32 lines total; one marker type and two methods).
- [x] Confirm there are no package docs, tests, fixtures, generated/platform
  variants, benchmarks, fuzz targets, or nested packages.
- [x] Compare `rust/crates/tidb-util/src/nocopy/mod.rs`: the zero-sized
  `NoCopy` marker is non-`Copy`/non-`Clone` and exposes only source-shaped
  no-op `lock`/`unlock` methods. Removed constructors, derives, compile-fail
  tests, and semantic manifests are not reintroduced.
- [x] Revalidate current and exact detached Go-master package checks, Rust
  owner check, formatting, and diff quality.
- [ ] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or Bazel
file changed, so `make bazel_prepare` is not required. No regression test is
added because the complete Go package has no source tests and this batch
changes no behavior. Exact commands and boundaries are recorded in
`rust/testport/receipts/util_nocopy.md`.

## Next boundary

Any future change must preserve the zero-sized marker, no-copy ownership
constraint, and empty lock methods. Do not add Rust-only constructors,
derives, diagnostics, or compile-fail policy.
