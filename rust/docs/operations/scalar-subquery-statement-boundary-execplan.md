# Scalar-subquery statement-boundary restoration ExecPlan

## Objective

Restore current Go master's statement-local scalar-subquery registry semantics
across normal execution, fast/cached plan selection, multi-statement prefetch,
and transaction replay.

## Scope and inventory

The repair follows upstream commit
`2c5dbbe51bbe4809abc62b315857c274035547ba`. The complete top-level artifact
inventories were captured for `pkg/executor` (165 artifacts, 96,694 lines) and
`pkg/session` (24 artifacts, 17,521 lines) before editing. Only the five exact
upstream source/test/build artifacts are changed here; this is supporting seed
evidence and does not claim either large package complete.

## Implementation steps

1. Clear `MapScalarSubQ` in `ResetContextOfStmt` before fast or cached plan
   selection can bypass logical-plan construction.
2. Clear the registry before each transaction-history item is rebuilt.
3. Restore Go master's executor lifecycle and session retry regression tests
   and their BUILD dependencies.
4. Verify the previously failing server prefetch consumer, run the Ready
   profile, and record any unavailable local prerequisite.

## Validation and exit criteria

The executor lifecycle, session replay, and server prefetch consumer tests must
pass through `tools/check/failpoint-go-test.sh`. `make lint` and
`git diff --check` must pass. `make bazel_prepare` is required because imports,
BUILD metadata, and new top-level tests changed; if the workspace still lacks
`bazel`, that exact environmental failure must be recorded. Receipt:
`rust/testport/receipts/scalar_subquery_statement_boundary.md`.

## Progress

- The executor lifecycle, session transaction-replay, and server prefetched
  PointGet regressions pass with failpoints enabled and disabled by the wrapper.
- `make lint` and `git diff --check` pass.
- `make bazel_prepare` was attempted with the pinned Go environment and is
  blocked only because this workspace has no `bazel` executable.
