# `pkg/util/skip` parity audit ExecPlan

## Objective

Inventory the complete Go-master `pkg/util/skip` package and keep its
ownership boundary explicit while the Go test framework remains authoritative
for short/long test selection.

## Completed

- Read both current Go-master artifacts (46 lines), all two function
  declarations, and the complete public Bazel target: `BUILD.bazel` and
  `skip.go`.
- Confirmed there are no package tests, fixtures, generated/platform variants,
  benchmarks, fuzz targets, nested packages, or other build inputs.
- Revalidated both the working tree package and an exact detached checkout of
  Go master at `c6054025ed4c32ab3672a2a24ea46892714d21ec`; both compile probes
  pass with no test files.
- Confirmed Rust has no dependency-closed owner for Go's `testing.Short` and
  `testflag.Long` policy. Kept the package explicitly unclaimed and added no
  Rust-only helper or speculative behavior.

## Validation gate

- [x] Complete production/build inventory and no-test/no-fixture result
      recorded in `rust/testport/receipts/util_skip.md`.
- [x] Current and exact Go-master package compile probes pass.
- [x] Ready formatting, repository lint, and diff checks pass.
- [x] Push this receipt/ExecPlan batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

If test-selection policy is ever migrated, port `skip` together with
`pkg/testkit/testflag`, every importing Go suite, and the short/long CI matrix
as one dependency-closed test-infrastructure change. Do not add a Rust facade
that cannot control those Go consumers.
