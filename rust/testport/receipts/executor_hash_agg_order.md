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
