# `pkg/planner/core` — derived-table `ORDER BY` removal parity receipt

## Go behavior

`PlanBuilder.buildSelect` (`pkg/planner/core/logical_plan_builder.go:4583`)
builds a `LogicalSort` for a `SELECT`'s `ORDER BY` only when

* the query is top level (`len(b.qbOffset) == 1`), or
* the `SELECT` carries a `LIMIT` (`sel.Limit != nil`), or
* the session turned the removal off
  (`!b.ctx.GetSessionVars().RemoveOrderbyInSubquery`).

`RemoveOrderbyInSubquery` is `@@tidb_remove_orderby_in_subquery`, whose
default is `ON` (`vardef.DefTiDBRemoveOrderbyInSubquery = true`,
`pkg/sessionctx/vardef/tidb_vars.go`). A derived table's `ORDER BY` is
therefore dropped by default: relational semantics make it meaningless unless
a `LIMIT` consumes it.

The observable consequence is a plan-shape one. In TPCC condition 06 the
derived table is `(SELECT ol_w_id, ol_d_id, ol_o_id, count(*) FROM order_line
GROUP BY ... ORDER BY ...)`. Go drops that `ORDER BY`, so the aggregation
above the join is free to choose a `HashAgg`; keeping it lets the aggregation
choose a `StreamAgg` that absorbs the sort, which is what Rust did.

## Rust implementation

* `crates/tidb-planner/src/plan_builder.rs` now guards its `build_sort` call
  with Go's three-way condition and a new
  `remove_orderby_in_subquery` builder field (default `true`, matching
  `DefTiDBRemoveOrderbyInSubquery`).
* `crates/tidb-executor/src/stmt_context.rs` carries
  `@@tidb_remove_orderby_in_subquery` (`with_remove_orderby_in_subquery` /
  `remove_orderby_in_subquery`), and
  `crates/tidb-executor/src/driver/planner_bridge.rs` installs it on all four
  builder entry points.
* `crates/tidb-session/src/stmt_ctx.rs` reads the session variable into the
  statement snapshot and forwards it to `StmtContext`.

## Regression coverage

`driver::tests::aggregates::derived_table_order_by_is_removed_unless_top_level_limit_or_disabled`
asserts the four Go cases: a derived `ORDER BY` disappears by default, a
derived `ORDER BY ... LIMIT` keeps its sort, an explicit
`tidb_remove_orderby_in_subquery=OFF` context keeps it, and a top-level
`ORDER BY` is always built.

`driver::tests::aggregates::tpcc_condition_six_simplifies_and_pushes_through_derived_tables`
is the end-to-end regression: its analyzed plan is now Go's
`HashAgg -> IndexJoin -> [HashAgg(Build) -> TableReader -> HashAgg ->
TableRangeScan, TableReader(Probe) -> Selection -> TableRangeScan]`.

## Validation

```text
cargo test -p tidb-planner --lib -- --test-threads=1        # 1002 passed
cargo test -p tidb-session --lib -- --test-threads=1        # no regression
cargo test -p tidb-executor --lib -- --test-threads=1       # see the batch log
cargo check --locked --all-targets -p tidb-planner -p tidb-executor -p tidb-session
rustfmt --edition 2021 --config skip_children=true --check <changed files>
git diff --check
```

## Risk

Dropping a derived table's `ORDER BY` changes plan shape only: no ordering
guarantee is lost because the outer query never consumed it. The `LIMIT` and
top-level arms preserve the cases where the sort is semantically required, and
the session switch restores the previous behavior verbatim.
