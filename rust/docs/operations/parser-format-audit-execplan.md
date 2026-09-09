# `pkg/parser/format` parity audit ExecPlan

## Objective

Inventory the complete Go parser format package, compare it with the Rust AST
restore owner, and record a dependency-closed parity result.

## Completed

- Read all three pinned artifacts: `format.go`, `format_test.go`, and
  `BUILD.bazel` (661 text lines, 39 production declarations, three tests).
- Verified the hparser branch is byte-identical to Go master.
- Confirmed Rust `tidb-ast` owns the corresponding restore context, flags,
  writer, CTE state, and source-derived tests.
- Added no speculative code because no behavior delta exists.

## Validation gate

- [x] Current failpoint-aware Go package suite passes; refcount returns to zero.
- [x] Ready Rust formatting, repository lint, and diff checks pass.
- [ ] Push the receipt/ExecPlan with the next batch, verify remote SHAs, and
      pull `origin/hparser-integration`.

## Remaining boundary

Future restore-flag or parent-expression-state changes must be coordinated
between this package, all AST nodes, and parser consumers.
