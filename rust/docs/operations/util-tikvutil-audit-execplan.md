# `pkg/util/tikvutil` parity audit ExecPlan

## Objective

Keep the complete Go-master process-wide TiKV committer-concurrency setting
aligned with its Rust owner and every configuration/session consumer.

## Progress

- [x] Read both Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: `BUILD.bazel` and
  `tikvutil.go` (31 lines total; one exported atomic setting).
- [x] Confirm there are no package docs, tests, fixtures, generated/platform
  variants, benchmarks, fuzz targets, or nested packages.
- [x] Trace all Go consumers: TiKV config construction, GLOBAL sysvar set/get,
  and deprecated upgrade import. The Rust atomic owner and config/session
  publication paths preserve the default 128, signed 32-bit width, SeqCst
  ordering, and 1–10,000 validation range. No Rust-only behavior or missing Go
  behavior remains.
- [x] Revalidate current and exact detached Go-master package checks, the Rust
  owner/config/session checks, formatting, and diff quality.
- [x] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or Bazel
file changed, so `make bazel_prepare` is not required. No regression test is
added because the complete Go package has no source tests and this batch
changes no behavior. Exact commands and boundaries are recorded in
`rust/testport/receipts/util_tikvutil.md`.

## Next boundary

Any future change must preserve one process-wide SeqCst atomic, default 128,
the signed 32-bit representation, and publication through both configuration
and GLOBAL sysvar paths. Do not add a private duplicate or wrapper-only API.
