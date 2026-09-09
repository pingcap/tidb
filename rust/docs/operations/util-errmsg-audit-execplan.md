# `pkg/util/errmsg` parity audit ExecPlan

## Objective

Keep the complete Go-master SQL-error suffix package aligned with its Rust
error/config owners and the ordinary server packet writer.

## Progress

- [x] Read all three Go-master artifacts at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: `BUILD.bazel`, `errmsg.go`, and
  `errmsg_test.go` (288 lines; one production function plus helper, five
  source tests, and no package variants or fixtures).
- [x] Confirm there are no package docs, generated inputs/outputs,
  platform/build-tag files, benchmarks, fuzz targets, examples, or nested
  packages.
- [x] Compare `tidb-errmsg`, `tidb-config`, and the ordinary `tidb-server`
  connection writer: nil-error handling, prepared regex filtering and order,
  first-match behavior, punctuation, concurrent publication, and raw packet
  bytes match Go without Rust-only policy.
- [x] Revalidate current and exact detached Go tests, all five Rust source
  tests, owner/server checks, formatting, and diff quality.
- [ ] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or
Bazel file changed, so `make bazel_prepare` is not required. Exact commands
and boundaries are recorded in `rust/testport/receipts/util_errmsg.md`.

## Next boundary

Any future error-message extension must preserve nil safety, prepared
configuration ordering, invalid-regexp/empty-suffix skipping, first-match
return, trailing-period trimming, and the server's raw-byte protocol boundary.
