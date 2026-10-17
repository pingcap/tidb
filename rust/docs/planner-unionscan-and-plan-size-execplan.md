# Logical UnionScan and plan memory accounting

This ExecPlan is a living document. Keep the Progress, Surprises, Decision Log and Outcomes sections current at every stopping point.

## Purpose and outcome

Two pieces of Go's planner are still missing from the Rust planner, and each one changes answers a user can see.

First, Go wraps a table's DataSource in a `LogicalUnionScan` whenever the open transaction has written the table, whenever the table is a local temporary table, and whenever it is a cached table. The wrapper decides the plan: `EXPLAIN` shows `UnionScan` operators, a local temporary table's scans cost nothing (the `tidb_temp_table_factor` of 0.0), so an ordered read prefers an index to a sort, and `eliminateUnionScanAndLock` strips the operator again for point gets. Rust never builds it. `rust/crates/tidb-planner/src/plan_builder.rs` (`build_data_source`) ends with a marked boundary where Go's `tableHasDirtyContent` and `LogicalUnionScan` go, and the executor folds the dirty-row merge into its readers instead (`enable_dirty_union_scan_merge`). The corpus replay compares only access objects, so it hides this; the one visible symptom is `index_merge`'s local-temporary-table `ORDER BY` statement, where Go reads `IndexLookUp` over `IndexFullScan ... keep order:true` and Rust reads `TableFullScan` and sorts.

Second, `tidb_plan_cache_max_plan_size` admits plans Go refuses. Go charges a plan by summing every operator's `MemoryUsage()`, which counts each retained expression, schema, column name and partition-info copy. Rust's `PhysicalPlan::memory_usage` (`crates/tidb-planner/src/physical/mod.rs`) has a specific body for only nine operator kinds, and every other kind contributes just `Plan.MemoryUsage` (`PlanSize + len(tp)`). A UNION ALL of 30 two-table joins whose filters hold 2048-byte strings is 2,579,567 bytes to Go (over the 2 MiB default) and about 119 KB to Rust, so Rust caches it and `@@last_plan_from_cache` reads 1 where Go reads 0 (`planner/core/plan_cache`, `TestPreparedPlanCacheLargePlan`).

After this plan, `EXPLAIN` over a table written in the current transaction, over a local temporary table and over a cached table shows the same operators as Go, the `index_merge` temporary-table statement matches, and a plan's measured size equals Go's for the shapes below.

## Context for a reader new to the repository

Terms. A DataSource is the logical operator that reads one table. A cop task is the part of a physical plan that runs in the storage layer. `PhysPlanPartInfo` is a small Go struct (`pkg/planner/core/operator/physicalop/physical_plan_misc.go`) holding `PruningConds` (set to the DataSource's `AllConds` for every table, partitioned or not), `PartitionNames`, `Columns` and `ColumnNames`; Go attaches it to cop tasks, table scans and readers, and `PhysicalTableReader.MemoryUsage` counts it through three paths, which is why a filter is charged four times in a table reader's subtree (the Selection's conditions, the scan's partition info, the reader's partition info, and `TableScanAndPartitionInfos`). Rust does not model `PhysPlanPartInfo` yet (`crates/tidb-planner/src/task.rs` lists it as not ported).

Where the pieces are. Go: `buildDataSource` (`pkg/planner/core/logical_plan_builder.go:5335`), `tableHasDirtyContent` (`pkg/planner/core/util.go:156`), `logicalop/logical_union_scan.go`, `physicalop/physical_union_scan.go` (`ExhaustPhysicalPlans4LogicalUnionScan`, `Attach2Task`), `task.go` `attach2Task4PhysicalUnionScan` (pulls a projection above the UnionScan), `optimizer.go` `eliminateUnionScanAndLock`, `plan_cost_ver2.go` `getTaskScanFactorVer2` (the temp-table factor), and `plan_cache_utils.go` (the dirty-table set feeds the plan cache key). Rust: the logical operator and its rules already exist in `crates/tidb-planner/src/logical/union_scan.rs`; there is no physical UnionScan, no exhaust arm in `find_best_task/dispatch.rs`, and the executor has `UnionScanExec` in `crates/tidb-executor/src/union_scan.rs` plus the folded merge in the readers. Dirty detection exists only for the in-process engine (`KvTable::has_dirty_content` over `StagedWrites`); the production node's transaction state is the native membuffer.

How to measure against Go. Run Go in the oracle worktree (memory note go-oracle-probe-harness) with a `println("MEMDBG", pp.MemoryUsage())` at `plan_cacheable_checker.go:588`; `git checkout` the file afterwards. The numbers worth fixing in a test: a single table `select * from t where a=1` is 13,794 bytes, `... where c=space(1)` 13,914, `where c=space(2000)` 21,910, a two-table join with no condition 31,988, the same join with two `space(1)` conditions 42,840, with `space(2000)` 83,084, the 15-leg union 1,289,867 and the 30-leg union 2,579,567. For UnionScan, compare `explain format='verbose'` of the statements in `index_merge` after `create temporary table t1(c1 int, c2 int, c3 int, key(c1), key(c2))`.

## Plan of work

Milestone 1, plan memory accounting. Model `PhysPlanPartInfo` where Go sets it (`buildPhysPlanPartInfo` at `find_best_task.go:90` and its callers), on the cop task, the table scan and the readers, so the retained expressions exist to be counted. Then port each physicalop `MemoryUsage` body into `PhysicalPlan::memory_usage` as a match arm that sums the same terms with the same `size.SizeOf*` constants, reusing the already faithful `Expression::memory_usage`, `Schema::memory_usage`, `Column::memory_usage` and `FieldType::memory_usage`: Selection, Projection, TableScan, IndexScan, TableReader, IndexReader, IndexLookUpReader, IndexMergeReader, HashAgg, StreamAgg, TopN, Limit, UnionAll, Window, PointGet, BatchPointGet, Lock, MaxOneRow, TableDual, MemTable, CTE and CTETable, Sequence, UnionScan, Insert, Update, Delete, and the remaining ones in `pkg/planner/core/operator/physicalop`. Replace the hand-written "hidden children" walk in `cached_plan_memory_usage` (`physical_plan_cache.rs`) with the accounting Go does through each reader's own fields. Keep one test per operator kind that asserts Go's byte count for a probe statement.

Milestone 2, the logical UnionScan. Add a `dirty` input to the plan builder's table source (`TableSource`), implemented by the production node from its native membuffer and by the in-process engine from `StagedWrites`, and wrap the DataSource in `LogicalUnionScan` when `dirty || local temporary || cached`, adding the partition id column for partitioned tables exactly as Go does. Add `PhysicalUnionScan` (`ExplainInfo` is the sorted conditions; the operator contributes no normalized plan info but its type id is part of the plan digest), the exhaust arm (reject a TiFlash property with Go's warning, pass the property to the child), `attach2Task4PhysicalUnionScan` including the projection pull-up, `eliminateUnionScanAndLock`, resolve-indices, and the temp-table scan factor (the cost code already carries `tidb_temp`, it only needs the planner to mark scans as temporary). Make the executor build a `PhysicalUnionScan` by building its child (the readers already merge dirty rows). Add the dirty-table set to the plan cache key as Go does.

Milestone 3, prove it against Go. Differential probes (`difftool`) of EXPLAIN in a transaction after an insert, an update and a delete, over a local temporary table, over a cached table, and the plan-size numbers above; rerun the corpus replay and expect `index_merge` and `planner/core/plan_cache` to report zero divergences.

## Concrete steps

Work from the repository root. Probe with `bash <scratchpad>/difftool/difftool.sh <sql file> <Name>` and compare with `python3 <scratchpad>/cmp_seq.py <Name>`. After each milestone run the five-crate gate, then `cd rust && INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC=index_merge,planner/core/plan_cache cargo test -p tidb-server --lib corpus_unistore -- --ignored --nocapture` and expect zero divergences for the statements this plan covers. Before committing run `cd rust && cargo build --locked -p tidb-server`.

## Validation and acceptance

`select @@last_plan_from_cache` after the second `prepare ... execute ... execute` in `TestPreparedPlanCacheLargePlan` is 0 and after `set tidb_plan_cache_max_plan_size=1073741824` is 1; each probe byte count above is reproduced exactly; `EXPLAIN` of `select /*+ use_index_merge(t1) */ * from t1 where c1 < 10 or c2 < 10 and c3 < 10 order by 1` over a local temporary table prints `Projection / UnionScan / IndexLookUp / IndexFullScan(Build) keep order:true`, and its costs are Go's (the scans cost 0.00).

## Idempotence and recovery

Both milestones are additive until the builder starts wrapping; if the wrapper breaks results, gate it behind the temporary-table case first (always wrapped in Go) and widen to dirty and cached tables once the executor path is proven.

## Progress

- [x] (2026-10-17) Measured both gaps against Go and recorded the byte counts and the `UnionScan` shapes above.
- [x] (2026-10-17) Milestone 1: `PhysPlanPartInfo` (shared `Arc` on the cop task, table scan and readers), Go's `BasePhysicalPlan` / `PhysicalSchemaProducer` / `SimpleSchemaProducer` bases, Go's struct-size constants (`PlanSize` 72, table scan 672, index scan 568, point get 480, batch point get 544, `ColumnInfo` 344, physical property 200), and every operator in `crates/tidb-planner/src/physical/memory.rs`. Measured against Go with a per-operator dump (`PhysicalPlan::memory_breakdown`): twenty plan shapes agree to within 2.4%, most within 0.5%; `planner/core/plan_cache`, `integration`, `indexmerge_path`, `prepare/*`, `explainfor`, `prepared`, `parallel_apply`, `expression/plan_cache` replay with zero divergences. Regression tests: `plan_cache_admission_follows_go_plan_sizes`, `plan_cache_refuses_the_thirty_leg_union_by_default`.
  Known residuals, each smaller than the admission thresholds that matter: window-function plans under a Shuffle (Go counts the Shuffle's `Tails` and `DataSources` plans again; ported, but the Rust tree also carries a `ShuffleReceiver` stub Go's does not), point gets (-6% / -8%: their output names are rebuilt from the schema), index merge (-2.4%: the table-side scan carries no property), and Go's `cap()` growth for slices and property hash codes (lengths stand in).
- [ ] Milestone 2: logical and physical UnionScan.
- [ ] Milestone 3: differential proof and corpus replay.

## Surprises and discoveries

Go's `PhysicalIndexReader.MemoryUsage` computes `IndexPlan.MemoryUsage()` and drops the result (`physical_index_reader.go:600`); only the flattened `IndexPlans` are summed. The port reproduces that, since the number decides cache admission.

`PlanSize` is 72 bytes in Go; the Rust constant was `size_of::<BasePlan>()`, about 440 bytes heavier per operator, which is why every plan in the first measurement was over-charged before the field terms were even added.

The plan-size gap is not in the expression accounting: `Expression::memory_usage` and its constants already match Go. It is the operator layer. A string constant that appears once in the SQL is counted four times by Go because `PhysPlanPartInfo.PruningConds` is attached to the scan, the reader, and the reader's `TableScanAndPartitionInfos`, measured as exactly four copies of a 1,999-byte difference per condition on a single-table probe.

Go's `LOAD STATS` does nothing under `testkit` (the server connection layer performs it), which hid stats-dependent plan differences until the probe generator loaded the dump itself.

## Decision log

The UnionScan work is planned after the memory accounting only because the accounting needs no new executor behaviour; both are independent and either may go first.

## Outcomes and retrospective

Nothing yet.
