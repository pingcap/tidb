# `pkg/planner/core/operator/logicalop` — PARTITIONS pruning parity receipt

Comparison source: Go `origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.
The behavior authority is `65ac2fad58` (`executor, statistics: skip
stats_histograms read for TABLE_ROWS-only INFORMATION_SCHEMA queries`). No Go
source was edited.

## Inventory

The complete direct Go owner package was enumerated from `origin/master` before
editing: 43 tracked artifacts, 16,086 lines total. The walk included every
production file, nested test file, fixture/golden JSON, generated helper, and
Bazel build file:

```text
BUILD.bazel
base_logical_plan.go expression_util.go hash64_equals_generated.go
logical_aggregation.go logical_apply.go logical_cte.go logical_cte_table.go
logical_datasource.go logical_expand.go logical_index_scan.go logical_join.go
logical_limit.go logical_lock.go logical_max_one_row.go logical_mem_table.go
logical_mock.go logical_partition_union_all.go logical_plans_misc.go
logical_projection.go logical_schema_producer.go logical_selection.go
logical_sequence.go logical_show.go logical_show_ddl_jobs.go logical_sort.go
logical_table_dual.go logical_table_scan.go logical_tikv_single_gather.go
logical_top_n.go logical_union_all.go logical_union_scan.go logical_window.go
logicalop_test/BUILD.bazel
logicalop_test/hash64_equals_test.go
logicalop_test/logical_mem_table_predicate_extractor_test.go
logicalop_test/logical_operator_test.go logicalop_test/main_test.go
logicalop_test/plan_execute_test.go
logicalop_test/testdata/cascades_suite_in.json
logicalop_test/testdata/cascades_suite_out.json
logicalop_test/testdata/cascades_suite_xut.json
shallow_ref_generated.go
```

The relevant Go production surface was read function-by-function, including
`LogicalMemTable.Init`, `ExplainInfo`, `PredicatePushDown`, `PruneColumns`,
`BuildKeyInfo`, and `PushDownTopN`; its extractor and logical-operator tests,
fixtures, generated hash/equality code, and build target were checked as well.
The only behavior delta in this commit is the `TablePartitions` case added to
`PruneColumns` by `65ac2fad58`.

## Behavior and Rust owner

Go keeps an allow-list of information-schema memory tables whose output schema
may be narrowed. `PARTITIONS` was added to that list so a query retaining only
`TABLE_ROWS` removes `DATA_LENGTH`, `INDEX_LENGTH`, and other unused columns
before the physical stats detector runs. This is what lets the statement-local
stats path skip the expensive `mysql.stats_histograms` read for TABLE_ROWS-only
queries; the Rust physical detector already recognizes `PARTITIONS`.

Rust owner: `tidb-planner::logical::mem_table::PRUNABLE_MEM_TABLES` and
`LogicalMemTable::prune_columns`. The omission meant PARTITIONS retained its
full schema even though the downstream physical detector treated it as a
stats-backed table, so the logical and physical decisions could disagree.

## Change and regression

* Added `"PARTITIONS"` to `PRUNABLE_MEM_TABLES`, preserving the Go
  case-insensitive allow-list and last-column guard.
* Extended
  `logical::operator_tests::mem_table_prunes_only_the_listed_tables_and_keeps_one_column`
  to assert PARTITIONS is prunable and that selecting only `TABLE_ROWS` removes
  the unused columns in descending order while keeping the schema and table
  column vectors aligned.

## Ready validation

```text
cargo test --offline --locked -p tidb-planner --lib \
  logical::operator_tests::mem_table_prunes_only_the_listed_tables_and_keeps_one_column \
  -- --nocapture
# passed: 1 test

git diff --check
# passed
cargo fmt --all -- --check
# passed
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex make lint
# passed; unrelated pre-existing warnings only
```

The broader information-schema extractor tests still require the live Go
memtable/extractor harness and remain separate boundaries. This receipt covers
the Rust logical-pruning owner only.
