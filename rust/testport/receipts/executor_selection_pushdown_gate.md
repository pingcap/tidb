# The driver must not re-decide predicate push-down

Go source: `origin/master` at
`f6c6d1adba7edae5786a4fffbacb31b1ec9c42c6` (2026-09-08).

## Go behavior (the oracle)

Go decides predicate push-down in the planner: `PredicatePushDown` splits the
conditions and leaves a `PhysicalSelection` above a reader only for the
conjuncts `CanExprsPushDown(ctx, ..., kv.TiKV)` refused
(`pkg/expression/infer_pushdown.go:589`, with `ast.Oct` commented out of the
TiKV whitelist at `:214` and `ast.Rand` admitted only for
`ScalarFuncSig_RandWithSeedFirstGen` at `:277`). The executor never revisits
that decision: a physical `Selection` becomes `SelectionExec`, whose `Open`
allocates `childResult` and charges it to the statement tracker
(`pkg/executor/select.go:724-729`).

## The Rust behavior removed

`driver::physical_builder`'s `PhysicalPlan::Selection` arm offered the
selection's filters to the child through `TableAccess::accept_scan_filter`
and, when accepted, returned the child WITHOUT a `SelectionExec`. For a
predicate the planner had deliberately refused, that:

- skipped Go's `SelectionExec` and its statement-memory accounting, so
  `tidb_mem_quota_query` could not stop a statement on the Selection's cached
  child chunk;
- handed the cop task a predicate the planner never admitted, contradicting
  the explain output that still showed a root `Selection`.

The offer is now gated on the planner's own admission receipt,
`tidb_planner::pushdown::can_exprs_push_down_tikv`, so only a condition Go
would have pushed may be fused into the reader. A refused condition keeps a
`SelectionExec` (Go's shape); a pushed one still fuses, which is the same
shape Go reaches by never leaving the Selection at root.

## Regression

`driver::tests::mem_quota::selection_cached_chunk_is_part_of_the_query_quota`
uses `SELECT a FROM t WHERE oct(a) > '0'` (Go's non-pushable `ast.Oct`). Before
the gate the statement returned all three rows with
`statement_memory().bytes_consumed() == 0` under a one-byte CANCEL quota;
after the gate it fails with `MemoryExceedForQuery` (8175), releases the
cached chunk (`bytes_consumed() == 0` again), and the 1GiB accept-control
still answers the three rows. The previous SQL, `a + 0 > 0`, was itself
pushed into the cop reader by the planner, so it never had a root
`SelectionExec` to account.

## Validation

Profile: **Ready** for this package batch.

```text
cargo test -p tidb-executor --lib driver::tests::mem_quota:: -- --test-threads=1
# 8 passed / 0 failed

cargo test -p tidb-executor --lib -- --test-threads=1
# 1224 passed / 28 failed; this test removed from the baseline, no additions

cargo check --locked --all-targets -p tidb-executor -p tidb-planner
rustfmt --edition 2021 --check crates/tidb-executor/src/driver/physical_builder.rs \
    crates/tidb-executor/src/driver/tests/mem_quota.rs
git diff --check -- rust
```
