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

- Go owner: `pkg/parser/ast/base.go` and `base_test.go`.
- Rust owner: `crates/tidb-ast/src/base.rs` plus parser call sites.
- Current edit: source text/offset behavior, including ALTER interval rewrite
  text, is implemented and its owning Rust tests pass.
- Next: finish the remaining symbols in `base.go` and `base_test.go`, commit,
  then move directly to the next Go file. Do not start a new ledger or status
  artifact.

The package is open. Go is the inventory, tests are the proof, and Git is the
checkpoint.
