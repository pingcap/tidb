# `pkg/util/syncutil` parity audit ExecPlan

## Objective

Inventory the complete Go-master `pkg/util/syncutil` package and preserve its
explicit Go-only boundary while the `deadlock` build tag and package-wide lock
type identity remain part of the Go build contract.

## Completed

- Read all three current Go-master artifacts (84 lines), all two exported lock
  type declarations and the `EnableDeadlock`/initialization behavior, plus the
  complete Bazel target and its `go-deadlock` dependency.
- Audited both `deadlock` and `!deadlock` source variants as one package; there
  are no package tests, fixtures, generated/platform files, benchmarks, fuzz
  targets, or nested packages.
- Revalidated the working-tree package and an exact detached checkout of Go
  master at `c6054025ed4c32ab3672a2a24ea46892714d21ec`; both default and
  `deadlock` compile probes pass with no test files and select complementary
  variants.
- Confirmed Rust has no dependency-closed owner that can replace Go's
  package-wide wrapper identity, method promotion, or `go-deadlock` build-tag
  diagnostics. Kept the package explicitly unclaimed and added no Rust-only
  lock facade.

## Validation gate

- [x] Complete production/build-tag/Bazel inventory recorded in
      `rust/testport/receipts/util_syncutil.md`.
- [x] Current and exact Go-master default/deadlock package probes pass.
- [x] Ready formatting, repository lint, and diff checks pass on the clean
      committed tree.
- [x] Push this receipt/ExecPlan batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

Any future native implementation must move every Go consumer together and
prove zero-value locks, promoted methods, assignment/type identity, and both
deadlock-tag variants. A crate-local mutex wrapper would be Rust-only behavior
and cannot satisfy the existing Go import contract.
