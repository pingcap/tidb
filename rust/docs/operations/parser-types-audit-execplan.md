# `pkg/parser/types` parity audit ExecPlan

## Objective

Inventory the complete parser types package, compare its Go-master contract
with Rust's datatype owner, and record a dependency-closed parity result.

## Completed

- Read all six pinned artifacts (1,441 lines, 61 production declarations and
  six test entry points), including BUILD metadata and all parser type cases.
- Verified the hparser branch is byte-identical to Go master for this package.
- Confirmed `tidb-datatype` owns the corresponding FieldType, EvalType,
  conversion, restore, JSON, and error-prototype behavior with source-derived
  tests; no Rust-only behavior needed removal.
- Ran current and exact Go-master package suites plus focused Rust tests.

## Validation gate

- [x] Current parser types suite passes.
- [x] Exact Go-master parser types suite passes.
- [x] Focused Rust datatype source suite passes.
- [x] Ready Rust formatting, repository lint, and diff checks pass.
- [ ] Push receipt/ExecPlan with the next batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Remaining boundary

Future FieldType or EvalType changes must update parser and runtime datatype
consumers together; this receipt records the current complete boundary without
introducing a duplicate Rust type model.
