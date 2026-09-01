# `pkg/util/injectfailpoint` parity audit ExecPlan

## Objective

Inventory the complete Go-master random-fault helper package and preserve its
Go-only failpoint boundary without introducing a Rust-only fault policy.

## Completed

- Read both current Go-master artifacts (90 lines), all six function
  declarations, and the complete Bazel target with errors/failpoint
  dependencies.
- Confirmed there are no package tests, fixtures, generated/platform
  variants, benchmarks, fuzz targets, or nested packages.
- Revalidated the current checkout and an exact detached Go-master checkout at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; both package compile probes pass
  with no test files.
- Confirmed all behavior is conditional DXF test fault injection: named
  failpoint callbacks, caller-name capture, probability thresholds, and
  partial-read errors. Rust has no dependency-closed registry or matching
  production consumer, so no Rust-only helper or missing behavior was added.

## Validation gate

- [x] Complete production/build inventory recorded in
      `rust/testport/receipts/util_injectfailpoint.md`.
- [x] Current and exact Go-master package compile probes pass.
- [x] Ready formatting, clean-tree repository lint, and diff checks pass.
- [ ] Push this receipt/ExecPlan batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

Any future port must include the Go failpoint registry, all DXF/import reader
consumers, and the failpoint-enabled integration harness. Preserve the 0.01,
0.001, and 0.2 probability boundaries and partial-read semantics; do not add
a detached probabilistic Rust hook.
