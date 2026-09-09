# `pkg/util/gcutil` parity audit ExecPlan

## Purpose

Audit the complete Go `pkg/util/gcutil` package against the current Go master
pin and determine whether Rust has a dependency-closed owner. Keep the parity
receipt and the repository plan synchronized with that decision.

## Progress

- Inventoried all two Go artifacts: `gcutil.go` (91 lines) and
  `BUILD.bazel` (18 lines), for 109 lines total. There are no Go tests,
  fixtures, generated/platform variants, benchmarks, fuzzers, examples, or
  nested package artifacts.
- Read every Go declaration. The package owns GC enable/disable helpers,
  restricted SQL loading of `tikv_gc_safe_point`, Oracle timestamp conversion,
  and snapshot validation.
- Compared the package with Go master
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`; there is no source delta.
- Read the Rust session adapter and searched the Rust workspace. The adapter
  implements the `tidb_gcutil::Context` trait, but no complete Rust owner
  exists; behavior remains split across session globals, vardef, model/error,
  and transaction/storage crates.

## Decision

The package remains explicitly unclaimed. No Rust implementation or Go
behavior change is justified without moving the dependency-closed owners as a
single unit. No regression test can be added to a package with no test surface
while the Rust owner is absent.

## Validation

- Current checkout: `go test ./pkg/util/gcutil -count=1` (no test files).
- Detached Go-master checkout: `go test ./pkg/util/gcutil -count=1` (no test
  files).
- Rust owner/adapter search completed; no dependency-closed owner test exists.
- Ready documentation gates: Rust fmt check, pinned `make lint` in the clean
  detached Go-master checkout, and `git diff --check`.

## Risks and follow-up

Restricted-SQL permissions, session-global state, and live TiKV GC behavior
remain unverified by this boundary-only audit. A future claim must first
establish a complete Rust owner and then add focused parity tests for those
contracts.
