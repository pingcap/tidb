# Uncorrelated subquery plan-time evaluation: Go-parity receipt

## Go source and oracle

Go comparison source: `origin/master` at
`f6c6d1adba7edae5786a4fffbacb31b1ec9c42c6` (2026-09-08). The behavior was
re-read from the worktree sources and captured from live Go EXPLAIN output for
the exact statements this batch pins.

| Go artifact | Function | Contract |
| --- | --- | --- |
| `pkg/planner/core/expression_rewriter.go` | `handleScalarSubquery` (`:1531`) | uncorrelated child → `DoOptimize` → allocate one `AllocPlanColumnID` per output column → register `ScalarSubqueryEvalCtx` → `EvalSubqueryFirstRow` → fold to `Constant{SubqueryRefID}` (or `row(...)` for >1 column) |
| `pkg/planner/core/expression_rewriter.go` | `handleExistSubquery` (`:1136`) | same guard; folds a plain signed 1/0, `(row != nil) != not` |
| `pkg/executor/select.go` | `EvalSubqueryFirstRow` (`:598`) | build the optimized child, pull ONE chunk, return its first row (nil when empty) |
| `pkg/planner/core/flat_plan.go` | `flattenScalarSubQRecursively` (`:553`) | each registered child is an extra EXPLAIN root named `ScalarSubQuery` |
| `pkg/planner/core/scalar_subq_expression.go` | `ScalarSubqueryEvalCtx.ExplainInfo` | root info is `Output: ScalarQueryCol#N[, ...]` |
| `pkg/expression/explain.go` | `ExplainExpressionList` (`:191`) / `Constant` (`:206`) | a re-projected plain column prints `expr->schemaCol`; an evaluated constant prints `ScalarQueryCol#N(value)` |
| `pkg/planner/core/logical_plan_builder.go` | `buildSelection` (`:1354`) | conjuncts are rewritten ONE AT A TIME in written order |
| `pkg/planner/core/logical_plan_builder.go` | trailing projection (`:4612`) | when auxiliary HAVING/ORDER BY fields were appended, the trimmed output columns get FRESH `AllocPlanColumnID` ids |
| `pkg/executor/aggregate/agg_hash_executor.go` | `unparallelExec` (`:695`) | serial group order is first-seen; `parallelExec` (`:649`) order is worker-scheduling dependent |
| `pkg/planner/core/logical_plan_builder.go` | `buildDataSource` (`:5244`) | every non-cluster DataSource appends `_tidb_commit_ts` |

## Gap

Rust lowered EVERY scalar subquery into a left-outer Apply and reported the
uncorrelated case as `EvaluateSeparately`, which `plan_builder` rejected with
"uncorrelated scalar-subquery evaluation is not available to the planner".
`EXISTS` took the same rejection in the filter path. Go instead optimizes and
runs the child at plan time, folds the first row into a constant, and keeps
the optimized child only as an EXPLAIN root. `docs/uncorrelated-scalar-subquery-divergence.md`
recorded the divergence and its implementation plan; this batch implements it.

## Implementation

- `tidb-planner`:
  - `PlanBuilder` gains an optional `SubqueryEvaluator` hook plus a
    `subquery_constants` side vector. `MarkerKind::Constant` selects from that
    vector, so the existing marker tail can carry a non-column expression.
  - `handle_scalar_subquery` now returns `EvaluateSeparately` whenever
    `must_build_apply` is false, matching Go's shared guard.
  - `evaluate_subquery` calls the hook with the builder's current `optFlag`,
    builds one `Constant` per output column with `subquery_ref_id` set (or the
    plain signed 1/0 for EXISTS), and emits the `#const#N` marker.
  - `build_selection` interleaves `lower_filter_subquery` and
    `lower_scalar_subqueries` per conjunct in written order, matching Go's
    per-conjunct rewrite and therefore its plan-column allocation order.
  - `build_trim_projection` allocates fresh ids for the kept columns.
  - `PlanErrorKind::SubqueryReturnsMoreThanOneRow` carries 1242 across the
    planner boundary.
- `tidb-executor`:
  - `planner_bridge` installs the hook: `DoOptimize` the child, allocate the
    output column ids, register the optimized tree, run it through the new
    `physical_builder::execute_first_row`, and report the first row.
  - `explain` appends one `ScalarSubQuery` root per registered child with
    `Output: ScalarQueryCol#N` and the child tree beneath it.
  - `TableScanExec` can emit `_tidb_commit_ts` (the zero version, matching
    table sampling), which a popped-EXISTS child's bare scan requires.
  - the parallel HashAgg emits groups sorted by the child position of each
    group's first row, i.e. the serial executor's first-seen order.

## Regression pins

`crates/tidb-executor/src/driver/tests/subqueries.rs` already contained the
Go-derived assertions; these now pass:

- `plain_explain_evaluates_and_labels_an_uncorrelated_scalar_subquery`:
  storage is read once, the child is a separate root, the predicate prints
  `ScalarQueryCol#N(value)`, and the q11 trailing projection prints
  `Column#14->Column#27`.
- `evaluated_scalar_predicate_is_pushed_below_a_sibling_anti_semi_join`:
  q22's folded constant participates in the DataSource Selection and the
  statement-wide allocator yields `ScalarQueryCol#14`.
- `explaining_a_correlated_scalar_type_reads_no_storage`: the uncorrelated
  children are roots `ScalarQueryCol#8`/`#10`/`#15` and plain EXPLAIN reads no
  storage for the correlated shapes.
- `subqueries`: EXISTS/NOT EXISTS fold to 1/0 and the HAVING fold keeps the
  serial group order.

## Validation

```text
cargo test -p tidb-executor --lib -- --test-threads=1
  → 1251 passed; 9 failed (all pre-existing baseline failures)
cargo test -p tidb-planner --lib
  → 1001 passed; 0 failed
cargo check --locked --all-targets -p tidb-planner -p tidb-executor
  → clean
rustfmt --edition 2021 --config skip_children=true --check <changed files>
  → clean
git diff --check
  → clean
```

## Risks and boundary

- The hook is optional; a `PlanBuilder` without it still refuses the
  `EvaluateSeparately` arms, exactly as Go is unusable without `pkg/executor`.
- The local storage seam has no MVCC version, so `_tidb_commit_ts` is the
  zero version; a real MVCC backend must fill the row's commit ts instead.
- The parallel HashAgg order is a deliberate deterministic choice. Go's own
  parallel order is worker-scheduling dependent; this port pins the serial
  first-seen order, which is what the Go-derived test expectations encode.

## Follow-up: the IN-rewrite join family is re-pinned to the ANALYZED fixture (2026-09-09)

The test's last two blocks asserted a `HashJoin` for
`SELECT k FROM outer_t WHERE k IN (SELECT k FROM inner_t)` and for the
`inner_u` (unique-key) variant. A pinned Go probe on the test's OWN fixture
(`outer_t` scaled to 10000 rows / `k` NDV 10000, `inner_t` scaled to 10000
rows / `k` NDV 500 / `v` NDV 10000, empty `inner_u`; `ANALYZE`d, then
`explain format='brief'`) records:

```text
SELECT k FROM outer_t WHERE k IN (SELECT k FROM inner_t)
  IndexJoin 500.00 -> HashAgg(Build) -> TableReader -> HashAgg(cop) ->
  TableFullScan(inner_t); TableReader(Probe) -> TableRangeScan(outer_t)

SELECT k FROM outer_t WHERE k IN (SELECT k FROM inner_u)
  MergeJoin 10000.00 -> TableReader(Build) -> TableFullScan(inner_u);
  TableReader(Probe) -> TableFullScan(outer_t)
```

The `HashJoin` expectations came from the correlate suite's PSEUDO-statistics
fixture, whose dedup aggregate is 7992 rows; at this fixture's 500-row
aggregate Go's own cost model prefers the index join (and a merge join over
the unique key's full scans). The assertions now pin `IndexJoin` /
`MergeJoin` and keep the dedup intent: the non-unique rewrite still carries a
`HashAgg`, the unique one still carries no `HashAgg`.

Known residual divergence, not asserted by this test: the port's unique-key
plan keeps a two-phase `StreamAgg` over `inner_u` where Go eliminates the
`buildDistinct` first-row aggregation entirely (`AggregationEliminator` +
the unique group key). The `!HashAgg` assertion holds either way.

```text
cargo test -p tidb-executor --lib -- --test-threads=1 explaining_a_correlated
# ok after; FAILED on the stale HashJoin assertions before

cargo test -p tidb-executor --lib -- --test-threads=1
# 1260 passed; 2 failed; no additions

cargo test -p tidb-planner
# 1004 + 268 + 6 + 3 passed; 0 failed

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check <changed files>
git diff --check
# clean
```
