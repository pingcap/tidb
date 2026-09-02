# Mock TiKV LOAD DATA retry parity ExecPlan

## Objective

Complete the `pkg/store/mockstore/unistore/tikv` Go-master package and restore
the consumer rule that a one-shot `LOAD DATA LOCAL INFILE` stream must not be
reopened during pessimistic-lock retry. Keep the Rust owner aligned with the
package's private utility helpers as their in-memory TiKV callers are
transcreated.

## Scope and inventory

The package inventory contains 16 artifacts and 9,286 lines after restoration.
There are ten production Go files, five Go test/support files, and one
`BUILD.bazel`; no package doc, fixture, generated/platform variant, benchmark,
fuzz target, or nested package exists. Fifteen artifacts were already exact;
only `server.go` lacked the upstream failpoint. The executor and commontest
edits are supporting cross-package behavior and do not claim those packages.

## Implementation steps

1. Restore the deterministic mock TiKV retryable-deadlock failpoint.
2. Restore the executor guard that returns the original lock error for a
   client-local LOAD DATA plan.
3. Restore and run the focused connection-synchronization regression.
4. Run Ready validation and record the Bazel prerequisite result.
5. Implement `util.go`'s range, hash, de-duplication, and nil-preserving-copy
   helpers in `tidb-unistore`, reusing the source-backed FarmHash implementation
   from `tidb-txnkv`.
6. Turn the existing ignored Go-table utility stubs into live Rust regressions
   and run the focused Rust test set.

## Validation and exit criteria

The focused failpoint-wrapped commontest must fail before and pass after the
repair. `make lint` and `git diff --check` must pass. `make bazel_prepare` is
required because a top-level test was restored; an unavailable local `bazel`
binary must be recorded rather than hidden. Receipt:
`rust/testport/receipts/store_mockstore_unistore_tikv.md`.

## Progress

- The focused commontest failed before the repair because LOAD DATA returned no
  error, then passed after the failpoint and no-retry guard were restored.
- `make lint` and `git diff --check` pass.
- `make bazel_prepare` was attempted with the pinned Go environment and is
  blocked only because this workspace has no `bazel` executable.
- The four focused Rust utility regressions pass. The regular
  `tidb-unistore` test target remains blocked by the unrelated parent
  `InProcessClient` missing `SynchronousBatchRequestDispatcher`; the focused
  run used and removed a temporary validation-only implementation, leaving no
  parent-client production diff. `cargo check --lib` and the shared FarmHash
  test pass.
