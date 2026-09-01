# `pkg/util/breakpoint` parity audit ExecPlan

## Objective

Inventory the complete Go-master breakpoint hook and preserve its explicit
Go-only failpoint/session-context boundary.

## Completed

- Read both current Go-master artifacts (47 lines), the two declarations, and
  the complete Bazel target with session-context, string-key, and failpoint
  dependencies.
- Confirmed there are no package tests, fixtures, generated/platform
  variants, benchmarks, fuzz targets, or nested packages.
- Revalidated the current checkout and an exact detached Go-master checkout at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`; both failpoint-managed compile
  probes pass with no test files.
- Confirmed Rust has no failpoint runtime or session-context notification hook.
  Adding a callback registry would be Rust-only behavior, so no source or
  speculative replacement was added.

## Validation gate

- [x] Complete production/build inventory recorded in
      `rust/testport/receipts/util_breakpoint.md`.
- [x] Current and exact Go-master package compile probes pass.
- [x] Ready formatting, clean-tree repository lint, and diff checks pass.
- [x] Push this receipt/ExecPlan batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

Any future port must include the Go failpoint registry, session-context
callback storage, and every caller's fault-injection harness. Preserve the
typed `func(string)` callback and failpoint-name propagation; do not add a
detached Rust breakpoint API.
