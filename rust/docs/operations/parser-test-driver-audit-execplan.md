# `pkg/parser/test_driver` parity audit ExecPlan

## Objective

Inventory the complete parser test-driver package, compare the Go-master
in-place visitor delta with Rust ownership, and record a dependency-closed
parity result.

## Completed

- Read all six pinned artifacts (1,274 lines, 72 production declarations and
  one source-derived test entry point), including BUILD metadata and embedded
  source-test inputs.
- Verified the hparser branch lacks the Go-master `AcceptInPlace` additions;
  the additions depend on the AST `InPlaceVisitor`/`Walk` migration.
- Confirmed no Rust crate currently closes the value/decimal driver plus
  in-place visitor dependency graph, and removed no behavior speculatively.
- Ran the current package compile and the exact Go-master package test in a
  detached worktree; both passed in their applicable trees.

## Validation gate

- [x] Current `test_driver` package compile passes.
- [x] Exact Go-master `test_driver` source regression passes.
- [x] Ready Rust formatting, repository lint, and diff checks pass.
- [ ] Push receipt/ExecPlan with the next batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Remaining boundary

Land `ast.InPlaceVisitor`, generated `AcceptInPlace` methods, and the
dependency-closed parser driver migration together. Until then this package
remains an explicit parity boundary rather than a partial port.
