# `pkg/util/watcher` parity audit ExecPlan

## Objective

Keep the complete Go-master polling watcher package aligned with its native
Rust owner, including source event sequencing and platform file metadata.

## Progress

- [x] Read all four Go-master artifacts at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: `BUILD.bazel`, `event.go`,
  `watcher.go`, and `watcher_test.go` (605 lines; thirteen production
  functions/methods, `TestWatcher`, and its assertion helper).
- [x] Confirm there are no package docs, fixtures, generated files, platform
  variants, benchmarks, fuzz targets, or nested packages.
- [x] Compare the Rust owner: event/error delivery, poll lifecycle, operation
  priority, rename/move identity, symlink metadata, signed sizes, and Unix/
  Windows identity branches follow Go. Rust-only ticker injection, policy
  constructors/derives, accessors, diagnostics, and supplemental tests remain
  removed.
- [x] Revalidate current and exact detached Go-master tests, the source-derived
  Rust event test, formatting, and diff quality.
- [ ] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or Bazel
file changed, so `make bazel_prepare` is not required. No regression test is
added because this batch changes no behavior; `TestWatcher` remains the focused
source-derived regression. Exact commands and boundaries are recorded in
`rust/testport/receipts/util_watcher.md`.

## Next boundary

Any future watcher change must preserve the three lock phases, 10 ms polling
sequence, unbuffered event/error channels, same-file identity, and all source
operations. Keep platform metadata behavior explicit and do not reintroduce
Rust-only scheduling controls or public inspection APIs.
