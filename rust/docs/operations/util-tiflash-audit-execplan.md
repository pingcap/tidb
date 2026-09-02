# `pkg/util/tiflash` parity audit ExecPlan

## Objective

Keep the complete TiFlash replica-read policy aligned with Go while retaining
one native Rust owner and removing API/test surface that the source package does
not contain.

## Progress

- [x] Re-read both Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: the 72-line production policy
  and 9-line Bazel target. Confirmed no package docs, tests, fixtures,
  generated/platform variants, benchmarks, fuzz targets, or nested packages.
- [x] Verify `ReplicaRead` remains an open native-width integer domain with the
  three Go discriminants, exact predicates, case-sensitive string conversions,
  all-replicas fallback, threshold constant, and canonical vardef dependency.
- [x] Retain the dependency-closed `tidb-txnkv::tiflash` owner and its live
  distsql request projection. Keep the prior removal of the Rust-only alias,
  adapter methods, duplicate vardef constants, const-only capability, and
  supplemental tests for this test-free Go package.
- [x] Refresh the package receipt to Go master and Ready status; no new Rust
  behavior or duplicate regression carrier was introduced.

## Validation gate

This is a Ready authority refresh. No Go, Bazel, or module file changed, so
`make bazel_prepare` is not required.

- [x] Current and exact detached Go-master package compile probes pass (no Go
  test files), and file-selection probes confirm the complete source boundary.
- [x] The focused distsql consumer test passes (`1 passed`, `253 filtered`).
- [x] Rust formatting and diff checks pass.
- [x] Commit, push, pull, and remote SHA verification complete.

## Next boundary

Future changes must preserve the open integer policy and all fallback behavior,
consume vardef spellings from their canonical owner, and keep distsql request
propagation covered. Do not reintroduce Go-absent aliases, adapters, constants,
or tests into this source-test-free package.
