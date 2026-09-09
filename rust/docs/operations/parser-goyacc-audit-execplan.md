# `pkg/parser/goyacc` parity audit ExecPlan

## Objective

Inventory the complete Go-master goyacc generator package and record whether it
has a dependency-closed Rust owner after the hparser handwritten-parser
migration.

## Completed

- Read all three Go-master artifacts (1,443 lines), all 46 production function
  declarations, and the complete Bazel target; confirmed no package tests,
  fixtures, generated variants, or platform files exist.
- Compared the coupled formatter, modernc yacc processing, table generation,
  reports, examples, and CLI flags as one build-tool unit.
- Confirmed `tidb-parser` is the native handwritten parser owner and that no
  Rust goyacc generator or generated-output consumer closes this package's
  dependency graph.
- Recorded an explicit boundary without speculative Rust tooling or removal of
  the handwritten parser.

## Validation gate

- [x] Complete source/build inventory recorded.
- [x] Rust parser owner suite passes.
- [x] Ready formatting, repository lint, and diff checks pass.
- [ ] Exact Go-master generator compile; blocked by unavailable modernc module
      downloads (proxy EOF).
- [ ] Push this receipt/ExecPlan batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

Continue with parser consumers outside the generator tools; any root grammar
replacement remains an atomic parser/AST/visitor/test migration.

