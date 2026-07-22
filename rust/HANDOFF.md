# Active package: `pkg/parser/ast`

This is the living ExecPlan required by `PLANS.md`.

## Done when

- Every file under `pkg/parser/ast`, including every original test vector and
  support artifact, has a behaviorally equivalent Rust home.
- Restore, errors, flags, labels, read-only classification, SEM, and visitor
  traversal match Go.
- The original Go package tests and all owning Rust crate tests pass.
- The complete package is reviewed, committed, and pushed as one checkpoint.

## Now

- Completed owner: `pkg/parser/ast/base.go` and `base_test.go`. Rust preserves
  source text, offsets, binary-literal conversion, concurrent lazy reads,
  expression flags, marker-node categories, and all original test/benchmark
  shapes.
- Current Go owner: `pkg/parser/ast/ddl.go` and `ddl_test.go`.
- Rust owners: `crates/tidb-ast/src/ddl.rs`, `src/ddl/**`, and their parser call
  sites.
- Next: audit `ddl.go` in source order, fill every missing production type and
  restore/visitor behavior together with the complete original `ddl_test.go`.
  Do not start a new ledger or status artifact.

The package is open. Go is the inventory, tests are the proof, and Git is the
checkpoint.
