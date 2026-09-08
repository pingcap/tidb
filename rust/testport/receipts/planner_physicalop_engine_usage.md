# `pkg/planner/core/operator/physicalop` — storage-engine helper batch

Comparison source: Go `origin/master` at commit
`1c1a334d2be1dce64888b6e1f054462c566b0734` (2026-09-02), including the
alternative-round changes introduced by `a74cc59699`.

## Complete package inventory

The package boundary contains 57 tracked artifacts: `BUILD.bazel` plus 56 Go
files, totaling 17,900 lines in the working snapshot. The complete file list
was read before editing, including production, tests, generated output, and
build metadata:

```text
BUILD.bazel
base_physical_agg.go
base_physical_join.go
base_physical_plan.go
enforce.go
foreign_key.go
fragment.go
fragment_test.go
nominal_sort.go
physical_apply.go
physical_batch_point_get.go
physical_common_plans.go
physical_cte.go
physical_cte_table.go
physical_exchange_receiver.go
physical_exchange_sender.go
physical_expand.go
physical_hash_agg.go
physical_hash_join.go
physical_index_hash_join.go
physical_index_join.go
physical_index_merge_join.go
physical_index_reader.go
physical_index_scan.go
physical_indexlookup.go
physical_indexlookup_reader.go
physical_indexmerge_reader.go
physical_limit.go
physical_lock.go
physical_max_one_row.go
physical_mem_table.go
physical_merge_join.go
physical_plan_misc.go
physical_projection.go
physical_schema_producer.go
physical_selection.go
physical_sequence.go
physical_show.go
physical_shuffle.go
physical_sort.go
physical_stream_agg.go
physical_table_dual.go
physical_table_reader.go
physical_table_sample.go
physical_table_scan.go
physical_topn.go
physical_union_all.go
physical_union_scan.go
physical_utils.go
physical_utils_test.go
physical_window.go
plan_clone_generated.go
single_scan_index_join.go
storage_engine_usage.go
task.go
task_base.go
tiflash_predicate_push_down.go
```

There is no package `doc.go`, `OWNERS`, fixture/testdata directory, fuzz
corpus, platform-specific source, or generator input beyond the checked-in
`plan_clone_generated.go` output. The inventory contains 800 production
functions and six Go test functions. Existing failpoint hooks in `fragment.go`
are covered by the failpoint-aware package gate.

## Go behavior restored

`StorageEngineUsage` now walks physical operators while stopping at reader
boundaries, counts TiKV/TiFlash table readers, treats point/index readers as
TiKV, traverses both CTE seed and recursive plans, and leaves TiDB-side reads
unclassified. `HasSingleScanIndexJoin` recognizes plain index joins (including
embedded hash/merge variants), follows unary wrappers on the inner side, and
protects only TiKV handle or covering-index probes; double-read index lookup and
index-merge readers are excluded. Focused regressions cover nil, homogeneous,
mixed, CTE/wrapper, reader-boundary, and inner-side cases. BUILD metadata lists
both restored production files.

## Rust owner and boundary

`tidb-planner::storage_engine_usage` owns the same tree predicates over the
closed `PhysicalPlan` enum and has source-derived unit tests. The live
alternative-round optimizer integration (session isolation-engine mutation,
cost comparison, and round-driver cleanup) remains an explicit boundary for a
later dependency-closed planner batch; this commit does not claim the whole
planner package is transcreated.

## Validation (Ready profile)

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
./tools/check/failpoint-go-test.sh pkg/planner/core/operator/physicalop \
  -run 'Test(StorageEngineUsage|HasSingleScanIndexJoin)$' -count=1 -vet=off
# PASS; package 1.047s; failpoints disabled to refcount 0

cargo +nightly-2026-08-22 fmt --all -- --check
```

The Rust planner test target was attempted with
`cargo +nightly-2026-08-22 test --offline --locked -p tidb-planner --lib
storage_engine_usage -- --test-threads=1` but is blocked before compilation by
the local `openssl-sys` dependency because `pkg-config` and OpenSSL headers are
not installed. `make lint` passes with the pinned Go runtime. `make
bazel_prepare` is required because Go files and a top-level test body changed,
but is blocked locally by `make: bazel: No such file or directory`. `git diff
--check` passes.

Regression evidence: before the Go helper files and tests were restored, the
focused test symbols and helper APIs were absent; the focused failpoint-aware
run above passes after the change. The Rust source tests likewise compile only
with the new owner module present.

Risks are limited to engine classification and index-join shape detection;
the helpers are read-only tree walks. The optimizer round integration, full
Bazel shards, and Rust planner compilation remain unverified locally.

## Follow-up: `PhysicalTableScan.IsFullScan` reads the RANGES (2026-09-09)

Go `PhysicalTableScan.TP` (`physical_table_scan.go:440`) renders
`TableFullScan` iff `IsFullScan()` (`:666`), which is true when
`len(RangeInfo) == 0 && !haveCorCol()` and EVERY range `IsFullRange`. The Rust
port instead named the scan from whether an access condition existed, so a
predicate on a handle component whose per-partition ranges stayed full (a
partitioned common-handle table with `part > 199999`, whose explain range is
`[NULL,+inf]`) was rendered `TableRangeScan`.

`find_best_task/dispatch.rs` now computes the kind from the built ranges with
Go's unsigned-int-handle flag (`pk_is_handle && handle is unsigned`), and the
stale comment claiming `RangeInfo` is set for ordinary access conditions is
gone. `RangeInfo` is an INDEX JOIN string, and the index-join shape already
has its own `TableRangeScan` branch in `explain.rs`.

The same batch corrects `primary_keys::the_clustered_index_mode_decides_the_handle`:
Go skips the physical PRIMARY index only for `PKIsHandle`
(`create_table.go:1502`, `if tbInfo.PKIsHandle { continue }`), while a
clustered COMMON handle keeps a `PRIMARY` index record. The test asserted the
opposite and now checks `has_primary_index == !pk_is_handle`.

Regression: `a_common_handle_table_path_is_a_table_scan` failed with the
`TableRangeScan ... range:[NULL,+inf]` plan and passes after;
`the_clustered_index_mode_decides_the_handle` failed on the ON/VARCHAR arm and
passes after. Ready validation: `tidb-executor` lib serialized 1129 passed /
97 failed with those two as the only removals and no additions;
`tidb-planner` all four test targets green (990/268/6/3);
`cargo fmt --all -- --check` (three pre-existing drift files only);
`git diff --check -- rust`.

## Follow-up: the scan `desc` and IndexLookUp embedded-limit text (2026-09-09)

Two more `physicalop` explain clauses were missing.

- Go's `PhysicalTableScan.OperatorInfo` (`physical_table_scan.go:512`) and
  `PhysicalIndexScan.OperatorInfo` (`physical_index_scan.go:296`) append
  `, desc` after `keep order:<bool>` when the scan walks backwards. The Rust
  `explain.rs` printed only `keep order:<bool>`, so a reversed scan that
  answered the largest ids still looked forward in EXPLAIN.
  `scan_keep_order_text` now owns the clause for both arms.
- Go's `PhysicalIndexLookUpReader.ExplainInfo`
  (`physical_indexlookup_reader.go:189`) renders only
  `limit embedded(offset:o, count:c)` when `PushedLimit` is set (the children
  are implied by the relation symbol), and nothing otherwise. The Rust arm
  printed `index:<plan>, table:<plan>` instead.

Regression: `index_ranges::a_descending_handle_limit_answers_the_largest_ids`
failed on the missing `desc` and passes after;
`index_ranges::ordered_limit_adjusts_the_common_handle_scan_estimate` failed
on `index:Limit, table:TableRowIDScan` and passes after. Ready validation:
`tidb-executor` lib serialized 1131 passed / 95 failed with those two as the
only removals and no additions; `tidb-planner` all four test targets green;
`cargo fmt --all -- --check` (three pre-existing drift files only);
`git diff --check -- rust`.

## Follow-up: `EnforceProperty` Sort by-items keep the child column type (2026-09-09)

Go `EnforceProperty` (`enforce.go:41`) copies `prop.SortItems[i].Col` — a
complete `*expression.Column` — into `PhysicalSort.ByItems`, and `SortExec`
compiles `keyCmpFuncs[i] = chunk.GetCompareFunc(e.ByItems[i].Expr.GetType(ctx))`
(`sortexec/sort.go:778-786`). This port stores a `property.SortItem` as a
`UniqueID` plus direction while matching orders, so `enforce_property`
materialized `ByItems` from `item.col.clone()`, whose `ret_type` is `None`. The
executor then compiled no compare function and every such Sort failed with
`Get unexpected expression` (`sort.rs:399`), because the column fast path
requires `compare_funcs[index]` to be `Some`.

`enforce_property` now resolves each sort item against the child plan's schema
(`Schema::retrieve_column`) and falls back to the property column only when the
child does not expose it. Property equality/hashing still compare `UniqueID`,
so order matching is unchanged; the executor once again receives a typed column
exactly as Go does.

Regression: the new
`enforce::tests::the_enforced_sort_by_items_carry_the_child_column_type` fails
before the fix (`get_static_type()` was `None`) and passes after. Five executor
tests that lower a Sort enforcer over a forced merge join or aggregate were
fixed with no other additions:
`tests_merge_join_in_disk_source::vectorized_merge_join_smj_matches_hj_rows`,
`tests_parallel_apply_sql_source::apply_with_other_operators_source`,
`tests_partition_table_sql_source::partition_table_different_join_matches_regular`,
`driver::tests::aggregates::aggregation_hints_are_lowered_from_the_shared_physical_plan`,
and
`driver::tests::aggregates::distinct_aggregation_family_is_lowered_from_the_shared_physical_plan`.
Ready validation: `tidb-executor` lib serialized 1168 passed / 69 failed
versus 1163 / 74 at HEAD (the only flake,
`hash_agg_spill_tests::each_round_gives_the_statements_budget_back`, passes in
isolation); `tidb-planner --lib` 993 passed / 0 failed;
`rustfmt --edition 2021 --check` clean on the changed file;
`git diff --check -- rust`.

Known remaining divergence, recorded for a later naming batch:
`driver::tests::joins::a_forced_merge_lowers_the_planner_selected_sort_enforcers`
now lowers the Sort but still expects `test.ncl.k, test.ncl.o`. Go's
`FieldName.String()` (`pkg/types/field_name.go:45`) uses `TblName`, and
`buildDataSource` sets `TblName: tableInfo.Name` (the real table), while this
port passes the visible alias into the `table` slot; the recorded
`tests/integrationtest/r/executor/partition/issues.result` renders
`executor__partition__issues.uk_hp16726.col1` for a query aliased `t1`/`t2`.
The Rust plan instead prints `test.l.k, test.l.o`.

## Follow-up: the join left-side clause and NULL-safe key names (2026-09-09)

Two operator-text clauses were missing from every join.

- Go's `explainJoinLeftSide` (`physical_index_join.go:135`) appends
  `, left side:<child>` after the join type for every join that is NOT an
  inner join, rendering the child's `TP()` under a normalized (brief) explain
  and `ExplainID().String()` otherwise. `PhysicalHashJoin`,
  `PhysicalMergeJoin`, and `PhysicalIndexJoin` all call it. The Rust
  `join_info`/IndexJoin arm printed no such clause. It now does, and because
  the base plan's own `tp` is the logical name (`Join`), the child's PHYSICAL
  name comes from `physical_operator_name` via a new `plan_explain_id`.
- Go renders each equal condition's OWN function name. A set-operator semi
  join keys on `<=>` (`nulleq`, `buildSemiJoinForSetOperator`), but `join_info`
  hardcoded `eq`. It now selects `nulleq` from the join's `is_null_eq` flags,
  which the planner derives from the condition's function name.

Regression: the new
`explain::tests::a_non_inner_join_explains_its_left_side_like_go` and
`explain::tests::a_null_safe_join_key_explains_as_nulleq` fail before and pass
after. `driver::tests::set_operations::intersect_and_except_explain_as_go_semi_join_chains`
was fixed by both. Ready validation: `tidb-executor` lib serialized 1176
passed / 67 failed versus 1173 / 68, no additions;
`cargo check --locked --all-targets -p tidb-executor` clean;
`rustfmt --edition 2021 --check` clean on the changed file;
`git diff --check -- rust`.
