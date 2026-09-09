# `pkg/executor/aggregate` cop partial group-key emission receipt

Status: bounded Rust parity fix implemented in the isolated worktree and
validated against fetched Go `master`. Go source and Bazel metadata were not
changed.

Comparison source: Go `origin/master` at `f5cf8f633761`.

## Inventory completed before editing

The `pkg/executor/aggregate` owner surface was rechecked before editing,
including every production file, `_test.go`, fixture/helper, generated or
platform variant, and build artifact:

```text
pkg/executor/aggregate/
BUILD.bazel OWNERS agg_hash_base_worker.go agg_hash_executor.go
agg_hash_final_worker.go agg_hash_partial_worker.go agg_spill.go
agg_spill_test.go agg_stream_executor.go agg_util.go
```

The behavior-bearing Go functions were read function-by-function:
`HashAggExec.unparallelExec` (`agg_hash_executor.go:695`) and the
`BuildFinalModeAggregation` split that feeds it
(`pkg/planner/core/operator/physicalop/base_physical_agg.go:600-842`,
including `getDistinctExpr` at `:681`). The Rust owners are
`tidb-executor::hash_agg::HashAggExec` (`emit_group`, `fold_chunk`, `next`),
`hash_agg::GroupedStreamAggExec`, the parallel pipeline gate in
`hash_agg::parallel::pipeline_eligibility`, and the round reset in
`hash_agg::spill::reset_spill_mode`.

## Go behavior restored

Go's `BuildFinalModeAggregation` moves a DISTINCT argument into the cop
partial's `GROUP BY` and, for a cop partial, deliberately omits the redundant
`firstrow()`: "if partial is a cop task, firstrow function is redundant since
group by items are outputted by group by schema". The partial's schema is
therefore the aggregate columns FOLLOWED by the group-by columns
(`partial.Schema.Append(partialGbySchema.Columns...)`). The cop executor
(TiKV) outputs those group-by columns; a bare-column key happens to reach the
same result in Rust through the scan's partial-aggregate pushdown
(`pushed_partial_aggregation`), which is why only a COMPUTED key exposed the
gap.

Rust's `HashAggExec` emitted exactly `agg_funcs.len()` columns and nothing for
the trailing group-by schema, so a cop partial with no aggregate functions
(`COUNT(DISTINCT CONCAT(s, ''))`) produced no values and the final aggregate
counted zero. The executor now retains each open group's evaluated key datums
and appends the trailing columns, and the same fix is applied to
`GroupedStreamAggExec`. The typed integer-key fast path is disabled when
trailing columns exist because it encodes keys without materializing a
`Datum`. The parallel pipeline only stages aggregate values, so an aggregation
with trailing group-by columns stays serial.

## Focused regressions

- `tidb_executor::driver::tests::aggregates::a_computed_distinct_argument_round_trips_through_the_cop_partial_aggregation`
  covers a function-only trailing column (`COUNT(DISTINCT CONCAT(s,'')), COUNT(*)`)
  and a group-by column plus a computed distinct argument
  (`s, COUNT(DISTINCT CONCAT(s,'')) GROUP BY s`). Before the fix the first
  query panicked in the parallel partial worker with
  `index out of bounds: the len is 0 but the index is 0`; after it returns
  `[2, 3]` and the grouped query returns the two groups.
- `driver::tests::aggregates::aggregates_read_the_arguments_collation` and
  `tests_executor_suite_statements_source::issue38756_sqrt_and_distinct` moved
  from failing to passing without edits to their expectations.

## Ready validation

Commands run from `rust/`:

```text
cargo test -p tidb-executor --lib aggregates::
cargo test -p tidb-executor --lib -- --test-threads=1
cargo check --locked --all-targets -p tidb-executor
cargo fmt -p tidb-executor
cargo fmt -p tidb-executor -- --check
git diff --check -- rust
```

Results:

- Focused aggregates module: 21 passed, 19 pre-existing plan-shape failures
  (baseline 21 failures; the two fixed tests and no new failure).
- Serialized `tidb-executor` owner: 1,136 passed / 91 failed against the
  `50ca4ba9cb` baseline of 1,132 passed / 94 failed. The fixed set is the new
  regression, `aggregates_read_the_arguments_collation`, and
  `issue38756_sqrt_and_distinct`. The only newly listed failure,
  `hash_agg_spill_tests::each_round_gives_the_statements_budget_back`, is the
  documented global-budget flake and passes three consecutive isolated runs.
- `cargo check --locked --all-targets -p tidb-executor`: PASS.
- Formatting: `cargo fmt -p tidb-executor -- --check` reports only the three
  pre-existing drift files (`ddl.rs`, `ddl/alter_table.rs`); `git diff --check`
  is clean.

## Risks and remaining boundaries

The retained key datums are not charged to the operator tracker, so a cop
partial with computed group keys can hold a small amount of unaccounted
memory; the spill tests, including the pressured round-boundary test, pass
serially. This closes the executor half of the cop-partial group-key boundary;
the planner's split (`final_mode_agg::get_distinct_expr`) was already
Go-faithful and is unchanged.
