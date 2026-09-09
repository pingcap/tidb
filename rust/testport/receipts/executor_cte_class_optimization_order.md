# CTE class optimization order receipt

Status: bounded Rust parity fix implemented in the isolated worktree and
validated against fetched Go `master`. Go source and Bazel metadata were not
changed.

Comparison source: Go `origin/master` at `f5cf8f633761`.

## Inventory completed before editing

The CTE owner surface was rechecked before editing:

```text
pkg/planner/core/
  logical_plan_builder.go        buildCTE / buildCteTable / CTE inlining
  logical_cte.go                 LogicalCTE.DeriveStats, CTEClass
  logical_cte_table.go           LogicalCTETable.DeriveStats
  rule_derive_stats.go           logical rule stats entry points
  operator/physicalop/physical_cte.go   findBestTask4LogicalCTE

rust/crates/tidb-executor/src/driver/planner_bridge.rs
  optimize_built_logical, optimize_cte_classes, optimize_cte_class,
  optimize_cte_tree, cte_references, InitStats
rust/crates/tidb-planner/src/logical/rewrite.rs   LogicalCTE/LogicalCTETable DeriveStats
rust/crates/tidb-planner/src/physical/mod.rs      find_best_task_4_logical_cte
```

Go's behavior-bearing code is `LogicalCTE.DeriveStats`
(`pkg/planner/core/logical_cte.go`): the FIRST stats request optimizes the CTE
class (`seedPartLogicalPlan`/`seedPartPhysicalPlan`) and caches it, so any
later `LogicalCTETable.DeriveStats` reads the seed statistics. The Rust port
split that into `planner_bridge::optimize_cte_class`, which sets
`seed_part_physical_plan`, plus a read-only `LogicalCTE.DeriveStats`.

## Go behavior restored

A CTE referenced by exactly one query block is inlined during plan building,
so it carries no `LogicalCTE` node and never reaches the class optimization. A
CTE referenced by two or more query blocks is materialized: the logical plan
keeps a `LogicalCTE` producer whose class the references share.

Go optimizes that class lazily inside the first `LogicalCTE.DeriveStats`, which
logical rules invoke while `logicalOptimize` is still running. Rust ran
`optimize_cte_classes` only AFTER `logical_optimize`, so the first
`recursive_derive_stats` inside a rule (join reorder, for example) reached
`LogicalCTE.DeriveStats` with a nil `seed_part_physical_plan` and failed with
`LogicalCTE.DeriveStats: seed physical plan is nil`. `optimize_built_logical`
now optimizes the classes present in the initial plan BEFORE entering the
logical rule list; the existing post-`logical_optimize` call remains for
classes a rule introduces, and is a no-op for the already-optimized ones.

## Focused regressions

`driver::tests::set_operations::single_use_cte_explain_keeps_base_statistics_and_multiple_uses_materialize`
is the regression. Its first half (single use, inlined) already passed and
still does; its second half joins the same CTE twice and failed before this
change with `Unsupported("LogicalCTE.DeriveStats: seed physical plan is nil")`
at `set_operations.rs:180`. After the change the join returns `1, 2, 3`.

## Ready validation

Commands run from `rust/`:

```text
cargo test -p tidb-executor --lib set_operations
cargo test -p tidb-executor --lib -- --test-threads=1
cargo check --locked --all-targets -p tidb-executor
cargo fmt -p tidb-executor
cargo fmt -p tidb-executor -- --check
git diff --check -- rust
```

Results:

- `set_operations`: 6 passed, 1 pre-existing failure
  (`intersect_and_except_explain_as_go_semi_join_chains`).
- Serialized `tidb-executor` owner: 1,136 passed / 91 failed against the
  same-baseline 1,136 passed / 91 failed. The CTE test is the only newly
  passing test; the only newly listed failure,
  `hash_agg_spill_tests::each_round_gives_the_statements_budget_back`, is the
  documented global-budget flake.
- `cargo check --locked --all-targets -p tidb-executor`: PASS.
- Formatting: `cargo fmt -p tidb-executor -- --check` reports only the three
  pre-existing drift files (`ddl.rs`, `ddl/alter_table.rs`); `git diff --check`
  is clean.

## Risks and remaining boundaries

The class is now optimized from the plan BEFORE `logical_optimize` rather than
after it. The seed and recursive logical plans live on the class and are
deep-cloned before optimization, so the outer rule list cannot observe a
partially optimized producer; the post-rule-list call still covers classes
that appear only after optimization. No Go source or build artifact changed.
