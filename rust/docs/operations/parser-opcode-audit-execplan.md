# `pkg/parser/opcode` parity audit ExecPlan

## Objective

Inventory all Go operator-package artifacts, remove stale test assumptions,
and verify the dependency-closed Rust AST owner remains aligned.

## Progress

- [x] (2026-09-02) Read all three artifacts (310 lines), including BUILD,
      production methods, test, and confirmed no fixtures/generated/platform
      variants.
- [x] (2026-09-02) Removed the obsolete fixed opcode-count assertion while
      retaining per-op formatting/string coverage.
- [x] (2026-09-02) Ran the focused failpoint-wrapped test and repository lint.
- [x] (2026-09-02) Committed only this package plus receipt/ExecPlan, pushed to
      `hparser-integration`, verified the remote SHA, and fast-forward pulled.

## Boundary

Rust `tidb-ast` owns the equivalent operator table. Future opcode additions or
removals must update both tables and source-derived behavior tests together.
