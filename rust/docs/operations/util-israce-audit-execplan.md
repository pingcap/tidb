# `pkg/util/israce` parity audit ExecPlan

## Objective

Keep the complete Go-master race-build selector aligned with the native Rust
compile-time feature and its ordinary printer consumer.

## Progress

- [x] Read all three Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: `BUILD.bazel`, `israce.go`,
  and `norace.go` (51 lines total).
- [x] Confirm there are no package docs, tests, harnesses, fixtures, generated
  inputs/outputs, benchmarks, fuzz targets, examples, or nested packages.
- [x] Verify the default and race Go builds select complementary source files
  and expose the exact false/true `RaceEnabled` constant.
- [x] Verify `tidb-util::israce::RACE_ENABLED` under default and `race`
  feature builds and retain the printer as the source-shaped consumer.
- [x] Keep the earlier removal of two source-absent Rust unit tests and the
  retired semantic-gate manifest; add no replacement Rust-only behavior.
- [x] Run current and exact detached Go list/test probes, both Rust feature
  checks, Ready formatting, and diff hygiene.
- [x] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This is a docs-only Ready authority refresh. No Go, Rust, Bazel, or module file
changed, so `make bazel_prepare` is not required. Exact commands and remaining
scope are recorded in `rust/testport/receipts/util_israce.md`.

## Next boundary

Any future change must preserve both mutually exclusive Go build-tag arms, the
matching Rust feature selection, and the live printer consumer. Do not add
runtime toggles or source-absent behavioral tests.
