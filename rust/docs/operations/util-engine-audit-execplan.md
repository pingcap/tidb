# `pkg/util/engine` parity audit ExecPlan

## Objective

Keep the complete Go-master engine-label classification package aligned with
its Rust PD-client owner and normalized store boundary, including Go's
diagnostic behavior.

## Progress

- [x] Read all three Go-master artifacts at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: `BUILD.bazel`, `engine.go`, and
  `engine_test.go` (253 lines; three production classifiers and two five-case
  source matrices).
- [x] Confirm there are no package docs, fixtures, generated inputs/outputs,
  platform/build-tag variants, benchmarks, fuzz targets, examples, or nested
  packages.
- [x] Compare protobuf and normalized PD-store ownership, label order,
  case-sensitive engine values, and write/compute role boundaries with the Go
  source. No engine-role inference is present.
- [x] Remove Rust-only `#[must_use]` from all three public boolean helpers and
  add `TestReturnValuesMayBeIgnoredLikeGo`; the pre-fix test failed with three
  `unused_must_use` errors and the post-fix suite passes.
- [x] Revalidate current and exact detached Go tests, focused Rust source
  tests, package checking, formatting, and diff quality.
- [ ] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This is a focused Ready parity fix. No Go or Bazel file changed, so
`make bazel_prepare` is not required. Exact commands and the pre-fix failure
are recorded in `rust/testport/receipts/util_engine.md`.

## Next boundary

Any future engine classifier must preserve exact case-sensitive label scans,
the `tiflash_compute` distinction for write nodes, protobuf/HTTP label order,
and Go's ability to ignore boolean return values. Do not infer roles from
unlisted labels.
