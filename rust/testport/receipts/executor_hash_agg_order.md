# HashAgg output order: pin the serial path where a test asserts row order

Go source: `origin/master` at
`f6c6d1adba7edae5786a4fffbacb31b1ec9c42c6` (2026-09-08). This batch fixes two
Rust-authored executor tests whose assertions depended on an output order the
parallel HashAgg does not promise.

## Go behavior (the oracle)

`HashAggExec.unparallelExec` (`pkg/executor/aggregate/agg_hash_executor.go:695`)
walks `e.groupKeys`, which is the FIRST-SEEN order groups were opened, so the
serial executor's output order is deterministic. `parallelExec` (`:649`) reads
`finalOutputCh`, which the final workers fill from `partialResultMap.M` — a
`MemAwareMap` over a Go map — so the parallel executor's output order is
randomized. `tidb_hashagg_partial_concurrency` and
`tidb_hashagg_final_concurrency` both at 1 select the serial path
(`pkg/executor/builder.go:2258`).

## The Rust tests

`driver::tests::aggregates::select_distinct` and
`driver::tests::aggregates::aggregate_having_and_order_by` assert exact row
orders for statements with no `ORDER BY` (`SELECT DISTINCT a, b FROM d2`,
`SELECT DISTINCT a + b FROM d2`, `SELECT DISTINCT a FROM d2 LIMIT 1`,
`SELECT a, COUNT(*) FROM g GROUP BY a HAVING COUNT(*) > 1`). The Rust
`HashAggExec` default concurrency is `tidb_executor_concurrency` = 5, so those
statements took the parallel pipeline and its groups arrived in final-worker
order, not first-seen order.

Both tests now build their statement context with
`with_hashagg_concurrency(1, 1)`, which is Go's serial selection. The
order-sensitive assertions then pin the serial executor's first-seen order,
which IS deterministic in both engines. The `LIMIT`/`ORDER BY` arms keep
testing what they were written to test.

## Regressions

- `driver::tests::aggregates::select_distinct` fails before with
  `[[1,1],[2,2],[1,2]]` where the serial first-seen order is
  `[[1,1],[1,2],[2,2]]`, and passes after.
- `driver::tests::aggregates::aggregate_having_and_order_by` fails before on
  the unordered grouped output and passes after.

## Validation

Profile: **Ready** for this package batch.

```text
cargo test -p tidb-executor --lib driver::tests::aggregates::select_distinct
cargo test -p tidb-executor --lib driver::tests::aggregates::aggregate_having_and_order_by
# ok

cargo test -p tidb-executor --lib -- --test-threads=1
# 1219 passed / 33 failed; both tests removed from the baseline, no additions

cargo check --locked --all-targets -p tidb-executor
rustfmt --edition 2021 --check crates/tidb-executor/src/driver/tests/aggregates.rs
git diff --check -- rust
```

## Follow-up: the HashAgg cost reads the session's final concurrency (2026-09-09)

Go `getPlanCostVer24PhysicalHashAgg` (`plan_cost_ver2.go:675`) divides the
aggregation, grouping, hash-build and hash-probe CPU by
`HashAggFinalConcurrency()`, which is the session's resolved
`tidb_hashagg_final_concurrency` (an unset value falls back to
`tidb_executor_concurrency`). The Rust coster hard-coded `5.0`, so a SERIAL
session still costed a HashAgg as if five final workers ran and picked it over
the StreamAgg Go chooses.

`Ver2Coster` now reads `self.session.hashagg_final_concurrency`, and
`StmtContext::with_hashagg_concurrency` stamps the same value onto the
optimizer cost environment so the statement's resolved worker count reaches the
coster (the production session already builds `CostEnv` from the session vars).

`driver::tests::aggregates::joined_integer_sum_uses_root_stream_agg` was
authored from the tpcds comparison that ran every concurrency variable at 1; it
now pins that serial session instead of the five-worker default, which is what
makes Go's cost model choose the root StreamAgg.

Regression: with the hard-coded `5.0` the coster prices HashAgg at
2161701 and StreamAgg at 2534069 and picks HashAgg; with the session value (1)
it prices HashAgg above StreamAgg and the test passes. Ready validation:
`tidb-planner` lib 998 passed / 0 failed; `tidb-executor` lib serialized 1220
passed / 32 failed, this test removed from the baseline with no additions;
`cargo check --locked --all-targets -p tidb-planner -p tidb-executor` clean;
`rustfmt --edition 2021 --check` clean; `git diff --check -- rust` clean.
