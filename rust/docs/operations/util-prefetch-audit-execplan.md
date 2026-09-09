# `pkg/util/prefetch` parity audit ExecPlan

## Objective

Keep the complete Go-master background-prefetch reader aligned with its native
Rust owner, including explicit source closure and one-buffer-ahead behavior.

## Progress

- [x] Read all three Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: `BUILD.bazel`, `reader.go`,
  and `reader_test.go` (300 lines; four source tests).
- [x] Confirm there are no package docs, READMEs, fixtures/testdata, generated
  inputs/outputs, platform/build-tag variants, benchmarks, fuzz targets,
  examples, ownership files, or nested packages.
- [x] Compare buffer alternation, zero-capacity handoff, `ReadFull` terminal
  conversion, partial reads, explicit idempotent close, cancellation, join,
  and source-close error ordering against `tidb-util::prefetch`.
- [x] Retain the earlier removal of the source-absent no-close constructor,
  implicit Drop-time close, and two supplemental close tests.
- [x] Run current and exact detached Go tests, all four Rust owner tests,
  Ready formatting, and diff hygiene.
- [x] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This is a docs-only Ready authority refresh. No Go, Rust, Bazel, or module file
changed, so `make bazel_prepare` is not required. Exact commands and remaining
scope are recorded in `rust/testport/receipts/util_prefetch.md`.

## Next boundary

Any future change must preserve Go's explicit `io.ReadCloser` ownership,
unbuffered producer handoff, exact-range partial-EOF conversion, and close
ordering. Do not reintroduce implicit closure or a no-close constructor.
