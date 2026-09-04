# Execution plan: UNION `inUnion`

- [x] Create an isolated worktree from the current `hparser-integration` tip.
- [x] Inventory Go `pkg/expression` (200 production/test/build files) and
  `pkg/planner` (586 production/test/build files), plus Rust owner crates and
  aggregate test/build metadata.
- [x] Read the Go cast signatures and UNION projection call sites used as the
  behavior oracle.
- [x] Add the AST carrier, expression dispatch/build helper, and planner call
  site for `inUnion` unsigned casts.
- [x] Add focused expression and planner regressions.
- [ ] Run the Ready validation profile and capture exact results.
- [ ] Update the package parity receipt and living ExecPlan.
- [ ] Create one meaningful batch commit and push it to `hparser-integration`.
