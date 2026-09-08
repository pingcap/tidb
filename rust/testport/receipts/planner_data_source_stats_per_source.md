# DataSource statistics come from the source's OWN conditions

Comparison source: Go `pkg/planner/core/stats.go` (`initStats`,
`DeriveStats4DataSource`) and `pkg/planner/cardinality/selectivity.go`
(`Selectivity`), plus `pkg/planner/core/rule_join_reorder_greedy.go`.
No Go source was edited.

## Go behavior (the oracle)

`initStats` attaches a table's stored profile to every `DataSource`;
`DeriveStats4DataSource` then derives that source's statistics from its own
`PushedDownConds` through the histogram-aware `Selectivity`. A join's
equal-condition (`a.x = b.x`) is a join key, never a data-source filter, so it
is charged to neither side, and a derived null-rejection filter
(`not(isnull(a.x))`) is estimated from the column's null count, not a flat
factor.

`joinReorderGreedySolver.solve` sorts the group's nodes by
`baseNodeCumCost` (row count plus descendants) and starts the connected tree
with `s.curJoinGroup[0]`; it never retries another start.

## The Rust gap

`driver::planner_bridge::InitStats` precomputes each `DataSource`'s profile
before the logical rule list so join reorder can cost its inputs. It applied
the statement's whole `WHERE` to every source, which is only correct for a
single-source query, so it was gated on `source_count == 1`. Multi-source
sources therefore fell through to the planner's reduced
`analyzed_filter_selectivity`, which charges every predicate it does not
recognize (the cross-table equality, and the null-rejection filters predicate
push-down later derives from it) a flat `SelectionFactor` of 0.8.

For TPC-H q14 that produced `300,005,811 * 0.64 = 192,003,719.04` for the
filtered `lineitem` source instead of the histogram's `3,831,625.78`. The
advanced greedy reorder sorts by that cost, so `part` (10,000,000) came first,
the rebuilt join's schema differed from the original, and
`optimize_join_group` inserted the schema-restore projection the recorded Go
plan does not have (and the equal condition printed with its operands
swapped).

## Change

* `InitStats` now runs for every query. Before using the statement predicate,
  it splits the `WHERE` into top-level `AND` conjuncts and keeps only those
  whose every column resolves against that source's own scope
  (`single_table_predicate`). A source with no such conjunct keeps the
  unscaled stored profile, which is Go's empty-`PushedDownConds` case.
* Removed the Rust-only `choose_best_greedy_start(2, ...)`; the advanced
  greedy now starts at the cheapest node exactly as Go does.

## Regressions

* `driver::tests::aggregates::tpch_q14_matches_recorded_hash_join_plan`
  passes: the plan is the recorded `Projection -> HashAgg -> Projection ->
  HashJoin -> [TableReader(part), TableReader(Selection(lineitem))]` with
  `equal:[eq(test.lineitem.l_partkey, test.part.p_partkey)]` and no
  schema-restore projection.
* New
  `driver::tests::joins::a_join_filter_is_charged_only_to_the_filtered_side`
  explains `SELECT * FROM a, b WHERE a.x = b.x AND a.y > 50` over analyzed
  uniform histograms and asserts the filtered source estimates `51.00` (the
  histogram's range selectivity) and the unfiltered source stays `100.00`.
  It failed before with `80.00` for the filtered source (the 0.8 fallback).

## Ready validation

```text
cargo test -p tidb-executor --lib tpch_q14_matches_recorded_hash_join_plan -- --test-threads=1
# ok

cargo test -p tidb-executor --lib a_join_filter_is_charged_only_to_the_filtered_side -- --test-threads=1
# ok; 80.00 before the change

cargo test -p tidb-executor --lib -- --test-threads=1
# 1244 passed; 13 failed; q14 removed from the previous 14-failure set,
# no new failure

cargo test -p tidb-planner
# 1278 passed; 0 failed (one Rust-only unit test removed with its helper)

cargo test -p tidb-expr
# 1206 passed; 2 failed; the same two pre-existing failures

cargo check --locked --all-targets -p tidb-executor
# passed

rustfmt --edition 2021 --config skip_children=true --check \
  crates/tidb-executor/src/driver/planner_bridge.rs \
  crates/tidb-executor/src/driver/tests/joins.rs \
  crates/tidb-planner/src/joinorder.rs
# no new drift (three pre-existing joinorder.rs hunks only)

git diff --check
# passed
```

## Follow-up: a join equality propagates its constant into the split

Go's `PropagateConstantForJoin` (`ruleutil.ApplyPredicateSimplification`) adds
`a.x = 7` to `a`'s conditions for `a.x = b.x AND b.x = 7` before the data
source's statistics are derived. The pre-push-down split runs before that
rule, so a source whose point key is completed by a join-propagated constant
estimated with only its own conjuncts: the TPC-C NewOrder customer lookup
(c_w_id = w_id AND w_id = 1 AND c_d_id = 6 AND c_id = 629) estimated
`300,000 / (10 * 3,000) = 10` rows instead of Go's histogram point estimate
`1.17`.

`single_table_predicate` now also synthesizes, for every conjunct
`src.col = other.col`, the constant equality `src.col = <const>` when the
statement also carries `other.col = <const>` (the same one-level closure the
customer lookup needs; deeper equivalence classes remain a boundary).

Regression:
`driver::tests::joins::a_join_equality_propagates_its_constant_to_the_other_side`
plans `SELECT * FROM a, b WHERE a.x = b.x AND b.x = 7 AND a.y > 50` and pins
the a-side `Selection` to `eq(test.a.x, 7), gt(test.a.y, 50)` with a `1.00`
estimate (Go `testkit` oracle for the same 100-row analyzed fixture). It
failed before with `51.00`. `tpcc_customer_warehouse_join_uses_two_point_gets`
also passes now.

```text
cargo test -p tidb-executor --lib -- --test-threads=1
# 1248 passed; 11 failed; the customer/warehouse test left the failure set,
# no additions

cargo test -p tidb-planner
# 1278 passed; 0 failed

cargo test -p tidb-expr
# 1206 passed; 2 failed; the same two pre-existing failures
```
