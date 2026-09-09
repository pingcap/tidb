# `pkg/util/execdetails` parity audit ExecPlan

## Objective

Keep the complete Go-master execution-details package inventory current and
prevent the existing Rust seeds from being mistaken for a dependency-closed
transcreation.

## Completed

- Read all eight Go-master artifacts in full: five production files, two test
  files, and the Bazel target (5,919 lines total), including 333 declarations
  and 30 top-level test/benchmark/fuzz entries.
- Revalidated the current Go checkout and an exact detached Go-master checkout
  at `c6054025ed4c32ab3672a2a24ea46892714d21ec`; both package test suites pass.
- Compared the current three-file Go-master delta (580 additions, 17
  deletions): read-pool details, summary/row coverage, scan-byte estimation,
  hash-state lifecycle, and Explain-RU stats all cross execution, metrics,
  context, protobuf, and client-go seams.
- Ran the four existing Rust owner test surfaces: 3 `tidb-exec::exec_details`,
  16 runtime-stat, 9 `tidb-util::ruv2_metrics`, and 3 TiFlash tests. The owners
  remain `SEED`s; no isolated Rust field or duplicate regression carrier was
  added.

## Decision

Keep `pkg/util/execdetails` explicitly unclaimed until the context,
client-go/protobuf/resource-manager, Prometheus, zap, ordinary executor, and
new Go-master runtime-stat consumers can land atomically. Removing the seed
API now would break existing Rust callers, while adding only the new fields
would create Rust-only behavior with no complete Go consumer graph.

## Validation gate

- [x] Complete inventory and current-authority delta recorded in
      `rust/testport/receipts/util_execdetails_audit.md`.
- [x] Current and exact Go-master package tests pass.
- [x] Existing Rust owner tests pass with the pinned toolchain and OpenSSL
      runtime path.
- [x] Ready formatting, clean-tree repository lint, and diff checks pass.
- [x] Push this receipt/ExecPlan batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

The next implementation unit must include the complete dependency graph and
focused regressions for every new field and aggregation invariant. Do not
split read-pool, row-summary, scan-byte, hash-state, or Explain-RU behavior
into detached leaf ports.
