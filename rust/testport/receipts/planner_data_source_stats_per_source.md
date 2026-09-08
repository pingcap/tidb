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

## Follow-up: `NOT (col IS NULL)` and subquery conjuncts (2026-09-09)

Two `single_table_predicate` / `Selectivity` gaps surfaced while evaluating
uncorrelated subqueries at plan time:

1. Go's `cardinality.Selectivity` has an `ast.UnaryNot` arm returning
   `1 - childSelectivity`; for `NOT (col IS NULL)` that is the same
   not-null fraction `col IS NOT NULL` filters. The port only recognized the
   `Is { not: true }` shape, so the parsed `Unary(NotKeyword, Paren(Is {
   not: false }))` fell through to the generic 0.8 fallback: a NOT NULL column
   with 10,000 analyzed rows estimated 8,000. `is_not_null_on_column` now
   unwraps the `NOT` shape too.
2. An unqualified conjunct that CONTAINS a subquery, such as
   `k IN (SELECT k FROM inner_t)`, resolved its column paths against the
   SUBQUERY's own source, so the pre-push-down per-source split charged that
   source the 0.8 fallback even though Go's `DataSource` has no
   `PushedDownConds` before predicate push-down. `single_table_predicate` now
   drops every conjunct containing a subquery node, so `inner_t`'s analyzed
   profile stays 10,000 rows / NDV 500 (Go's HashAgg over it estimates 500
   instead of 400).

Regression: new
`access_cost::tests::not_is_null_matches_is_not_null` asserts
`NOT (b IS NULL)` and `b IS NOT NULL` produce the same pseudo selectivity
(`0.001 * 0.999`). It failed before with the 0.8 fallback and passes after.
The subquery-conjunct half is covered by the existing
`driver::tests::subqueries::explaining_a_correlated_scalar_type_reads_no_storage`
fixture, whose inner `HashAgg` now estimates `500.00` (was `400.00`).

```text
cargo test -p tidb-executor --lib not_is_null_matches_is_not_null
# ok after; FAILED before (0.0008 vs 0.000999)

cargo test -p tidb-executor --lib -- --test-threads=1
# 1251 passed; 9 failed; no additions to the baseline set

cargo test -p tidb-planner
# 1001 + 268 + 6 + 3 passed; 0 failed

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check <changed files>
git diff --check
# clean
```

Both changes are executor-side selectivity parity fixes; the remaining
`tpcc_conditions_ten_and_twelve` gap (297.03 vs 300.00) is the planner's
`analyzed_filter_selectivity`, which has no histogram access and approximates
the `h_c_w_id = 1` equality as `1/NDV` instead of the histogram's
repeat-based `29,702/299,995`. Closing it needs the histogram collection on
the `DataSource`, which is a separate batch.

## Follow-up: the DataSource rule reads the loaded histograms (2026-09-09)

The gap above is closed. `HistColl` now carries the loaded column histograms
(Go `HistColl.Columns`), keyed by planner unique id, plus `ModifyCount` and
`PKIsHandle`; `InitStats` populates them from the catalog's `TableStatistics`
and `analyzed_filter_selectivity` estimates an `eq`/`in` condition through
`get_row_count_by_column_ranges` on the closed point ranges, dividing by the
source's row count. Without a loaded histogram for the column the NDV
approximation remains.

`PKIsHandle` matters: Go passes `pkIsHandle=true` only when the ESTIMATED
column is the single integer handle (`colStats.IsHandle`), not for every key
column of a common handle and not for a heap table's synthetic `_tidb_rowid`.
Passing the row-size `is_handle` flag alone made every point range on
`customer.c_w_id`/`orders.o_w_id` estimate one row and collapsed condition
twelve's plan to a root StreamAgg.

Result on condition ten: `h_c_w_id = 1` now estimates
`29_702 * 300_000/299_995 / 300_000 = 0.0990083`, and the predicate-column
loading model below makes the grouped history HashAgg estimate `297.03`,
which is Go's captured value.

## Follow-up: the predicate-column loading model (2026-09-09)

Go's lite statistics initialization loads the payloads for the statement's
PREDICATE columns and the indexes they cover; every other column is evicted
and `EstimateColumnNDV` borrows the first loaded, same-version index's
analyzed row count. `InitStats` had passed every column and index as loaded,
so the evicted `h_c_id` used its own 300,000-row histogram and estimated
`3000.0` instead of `3000.05`.

`predicate_column_names` walks the statement AST (all query blocks) and
collects every column a constant comparison or constant `IN`/`IS` names;
`InitStats::predicate_loaded_items` maps those names onto the source's columns
and marks an index loaded when its first column is loaded. A source with no
such predicate keeps the previous "everything loaded" approximation.

Red/green: `driver::tests::subqueries::tpcc_conditions_ten_and_twelve_decorrelate_scalar_sums`
failed at `297.02` before and passes after; new
`planner_bridge::predicate_column_tests::filter_columns_load_and_join_keys_do_not`
pins that a constant filter loads its column while a column=column join key
does not. Executor lib 1253 passed / 8 failed, the condition-ten test leaving
the baseline with no additions.

Regression: new
`logical::rewrite::analyzed_filter_selectivity_tests::equality_uses_the_loaded_histogram_repeat`
builds a 299,995-row histogram whose bucket repeats 29,702 times and asserts
the equality estimates the repeat, not `1/NDV`. It fails when the histogram
path is disabled and passes with it.

```text
cargo test -p tidb-planner --lib equality_uses_the_loaded_histogram_repeat
# ok after; FAILED with the histogram path disabled

cargo test -p tidb-planner
# 1002 + 268 + 6 + 3 passed; 0 failed

cargo test -p tidb-executor --lib -- --test-threads=1
# 1252 passed; 9 failed; no additions to the baseline set

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check <changed files>
git diff --check
# clean
```

## Follow-up: a loaded index's NDV is the source's group NDV (2026-09-09)

Go `initStats` ends with `ds.TableStats.GroupNDVs = getGroupNDVs(ds,
colGroups)` (`pkg/planner/core/stats.go:491`). For every asked column group
the pruner produced, an index whose ENTIRE column list matches that group
(and whose stats are essential-loaded) publishes its own NDV as the group's
exact NDV; `property.StatsInfo.Scale` then re-scales the group NDVs with the
source's filter selectivity. A join above a filtered source therefore
estimates from the composite NDV, not the largest single-column NDV.

Rust had no group NDVs at all. `HistColl` kept the loaded column histograms
but not the loaded indexes' column lists and NDVs, `InitStats` never
populated them, and `record_asked_groups` discarded the asked groups after
pruning. The `orders x order_line` join of `tpcc_check_seven` estimated from
`max(NDV(o_w_id)) = 300,000` instead of the `idx_order(o_w_id, o_d_id,
o_c_id, o_id)`-shaped group NDV, which kept the per-outer-row probe at 0.80
and the whole `IndexHashJoin` above plain `IndexJoin`.

Change:

* `HistColl` carries `index_ndvs: BTreeMap<i64, (Vec<i64>, f64)>` (Go
  `HistColl.Indices`' `Idx2ColUniqueIDs` plus each index's NDV) with
  `with_index_ndvs`/`index_ndvs()`.
* `InitStats` maps each loaded index's `columns[].offset` onto the planner
  schema's unique ids and attaches the index's NDV; an index with no loaded
  histogram or a non-positive NDV is skipped.
* `record_asked_groups` calls `refresh_group_ndvs` (Go `getGroupNDVs`). It
  matches each loaded index's whole sorted column set against the source's
  asked groups, sets the matching NDVs on the table profile, and re-scales
  them onto the live plan profile when `DeriveStats4DataSource` already ran
  — Go's single `ds.TableStats` object is scaled in place by
  `StatsInfo.Scale`, so the live copy must not keep the raw NDV.

Regression: `driver::tests::joins::tpcc_check_seven_propagates_the_warehouse_
range_to_both_leaves` passes. It failed at `joins.rs:647` (wanted
`IndexHashJoin`, got `IndexJoin`) before the change, and the pre-analyze
`MergeJoin` assertion at `joins.rs:585` is unchanged. With a temporary
candidate-cost trace the join's equal-condition output moved from 30,074.4
to 300,744 (Go's real-ANALYZE probe: 304,547.92) and the per-outer-row probe
from 0.801984 to 10.0248 (Go: ~10.15), so the analyzed plan is now Go's
`IndexHashJoin`.

```text
cargo test -p tidb-executor --lib -- --test-threads=1 tpcc_check_seven
# ok after; FAILED at joins.rs:647 before

cargo test -p tidb-executor --lib -- --test-threads=1
# 1255 passed; 7 failed; check_seven left the baseline set, no additions

cargo test -p tidb-planner
# 1002 + 268 + 6 + 3 passed; 0 failed

cargo check --locked --all-targets -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check \
  crates/tidb-planner/src/stats_info.rs \
  crates/tidb-planner/src/logical/rule_collect_plan_stats.rs \
  crates/tidb-executor/src/driver/planner_bridge.rs
git diff --check
# clean
```
