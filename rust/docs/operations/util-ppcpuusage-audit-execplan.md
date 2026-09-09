# `pkg/util/ppcpuusage` parity audit ExecPlan

## Objective

Keep the complete Go-master per-SQL CPU accumulator aligned with its native
Rust owner and statement-summary consumers.

## Progress

- [x] Read both Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: `BUILD.bazel` and
  `cpuusages.go` (94 lines; eight production methods).
- [x] Confirm there are no package docs, READMEs, source tests/support,
  fixtures/testdata, generated inputs/outputs, platform/build-tag variants,
  benchmarks, fuzz targets, examples, ownership files, or nested packages.
- [x] Compare value reset, mutex-protected state replacement, SQL-ID-gated
  TiDB time, unconditional TiKV time, snapshots, wrapping ID allocation, and
  CPU-time reset against `tidb-util::ppcpuusage`.
- [x] Preserve signed wrapping nanosecond arithmetic and poison recovery, and
  retain the earlier removal of source-absent tests and `must_use` behavior.
- [x] Verify the current and exact detached Go package, the zero-test Rust
  owner, the statement-summary consumer, Ready formatting, and diff hygiene.
- [x] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This is a docs-only Ready authority refresh. No Go, Rust, Bazel, or module file
changed, so `make bazel_prepare` is not required. Exact commands and remaining
scope are recorded in `rust/testport/receipts/util_ppcpuusage.md`.

## Next boundary

Any future change must retain Go's signed `time.Duration` representation,
wrapping addition and SQL-ID allocation, non-poisoning lock semantics, and
both statement-summary consumers. Do not add source-absent diagnostics or
tests.
