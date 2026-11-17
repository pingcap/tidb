# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-04; the [JSON register](structural-findings.json)
holds the same rows. A repaired finding is removed, not kept.

**4 unresolved: 1 open, 3 partial.**

A row states the remaining gap only. "Restate" marks a row whose earlier text
named no concrete mismatch ("complete package obligations remain"); it stays
unresolved until it is restated against its Go owner or closed.

## Unresolved

### System tables and startup

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| B01 | partial | First boot is decided by the bootstrap commit, not by DDL owner election: the node publishes the `mysql` bootstrap while connecting its store (`tidb-server` `cluster_stack.rs`), before the domain and its owner manager exist. A node whose publish loses to another's now waits for the winner, checking the store every 200ms as Go's non-owner does in `bootstrap`, and boots from the winner's tables through the upgrade path (two realtikv nodes booting a fresh cluster together both come up; before, one failed with `bootstrap encode failed: Write conflict`). Go lets only the DDL owner run `doDDLWorks`/`doDMLWorks`. | `pkg/session/bootstrap.go` (`bootstrap`), `pkg/session/session.go` (`runInBootstrapSession`) |

### DDL

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| D02 | partial | The ingest (fast-reorg) backfill runs: `ddl.Start` initializes Go's ingest environment (`InitGlobalLightningEnv` on TiKV, `CleanUpTempDirLoop`), ADD INDEX picks `ReorgType::INGEST` and, under `tidb_enable_dist_task`, Go's DXF backfill task (`tidb-ddl` `real_tikv_ddl/backfilling_dist.rs`, `backfilling_dist_{executor,scheduler}.rs`), whose local backend (`tidb-ddl-ingest`, `tidb-ingestor-ingestctrl`) sorts on disk and imports through TiKV's ImportSST, then merges the temporary index as its own DXF step and checks a unique index's remote duplicates; `autoPauseAddIndexJobOnKVDiskFull` (`tidb-ddl` `index.rs`), the status port's `/ingest/*` routes (`tidb-server` `ingest_handler.rs`) and the backfill executor's Top SQL attach are Go's; the testkit's mock store runs `InitDistTaskLoop` as Go's `BootstrapSession` does. A live TiKV ADD INDEX / UNIQUE INDEX / duplicate case answers as Go's (job comment `ingest, DXF, max_node_count=3`, Go's 1062 text). The local backend (`tidb-ingestor-ingestctrl`) follows Go's `ingestctrl` for split/scatter in batches with retries, region job generation, the store balancer, the retryer, write/ingest with MultiIngest detection and its single-SST fallback, the store write limiter, engine management and the duplicate iterator, SQL/TiKV checksum with GC TTL ownership, gzip and the TiKV mode switcher, pinned by 69 of Go's 79 `ingestctrl` tests; extsort replaces Pebble, so Go's Pebble-internal SST block-size test does not apply. `ADMIN ALTER DDL JOBS` (planner `buildAdminAlterDDLJob`, `AlterDDLJobExec`) rewrites the job row, `modifyTaskParamLoop` carries it to the DXF task, and `readIndexStepExecutor.ResourceModified` / `TaskMetaModified` retune the running pipeline's reader/writer pools, batch size and write limit. Go's `pkg/ddl/ingest` tests are all ported. `estimateTableRowSize` fills the task's `EstimateRowSize`; `Backend.ImportEngine` follows Go's order: TiKV's region-split config, the empty-engine skip, `forceTableSplitRange` at 100+ regions and the table-scope scheduler pause (`br/pkg/pdutil.PauseSchedulersByKeyRange`, `tidb-br` `pdutil`). Left: DDL's global sort (the backfill's merge-sort and external-engine ingest steps; `globalsort`, `engineapi` and `ImportEngine`'s external engines are ported with IMPORT INTO), which another agent owns; duplicate conflict conversion and lazy duplicate-controller init (Lightning backend/kv/errormanager); and Go's realtikv add-index inventory: a session-level harness boots the node's domain in-process against the PD `TIDB_REALTIKV_PD` names (`tidb-server` `realtikv_test_stack`, Go `realtikvtest.CreateMockStoreAndSetup`; the tests skip without it and, like Go's package, run one at a time), with 90 of the 111 `tests/realtikvtest/addindextest*` tests ported: all of `addindextest` and `addindextest4`, `addindextest1` but for its nextgen-only cross-keyspace tests and its global-sort test, and `addindextest3` but for its operator tests (Go's `pkg/disttask/operator` pipeline API) and the Pebble-only `TestAddIndexTempDirDataRemoved` (Go's skipped tests are ported as ignored); of `addindextest2` only `alter_job_test.go` (its `global_sort_test.go` needs global sort). Go's `index_presplit.go` (`PRE_SPLIT_REGIONS` on an index), `pickBackfillType`'s ingest disk precheck, the local ingest's `adjustWorkerCntAndMaxWriteSpeed` and `runReorgJob`'s asynchronous distributed reorganization (the DXF task runs on its own thread while each job step returns `ErrWaitReorgTimeout` at the progress tick) are ported | `pkg/ddl/backfilling*.go`, `pkg/ddl/ingest`, `pkg/ingestor/ingestctrl` |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| E29 | partial | Materialized views are ported only as metadata (`TableInfo` keeps Go master's `materialized_view*` fields byte for byte; phases in `rust/docs/go-shape-refactors-execplan.md`); the base table's DML does not write its MV log rows (Go `pkg/table/tables/mview_log.go`). The parser rejects `CREATE MATERIALIZED VIEW` and `CREATE MATERIALIZED VIEW LOG` as syntax errors (1064), where Go master parses them and gates them on `tidb_mview_enable` (8200 when off). The materialized-view DDL and its logs, `REFRESH MATERIALIZED VIEW`, the mview worker and its schedule, the planner's materialized-view rewrite and the executor's delta merge aggregation are absent. IMPORT INTO depends on them in one place: a next-gen IMPORT INTO ... FROM SELECT runs on a DXF worker (`ImportStepQuery`) only during materialized-view maintenance (`InMViewMaintenance`), so this port refuses every next-gen FROM SELECT, as Go does outside maintenance, and `ImportStepQuery` is not ported. | `pkg/ddl/materialized_view.go`, `pkg/ddl/mview_*.go`, `pkg/planner/mview`, `pkg/executor/mviewdeltamergeagg`, `pkg/util/mviewutil`, the parser's materialized-view grammar, `ImportStepQuery` in `pkg/dxf/importinto` (Go master) |

### Structure: Go's shape before more ports

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| S02 | open | `TableCommon` keeps the public columns and the non-public (adding or changing) columns as two lists, and an index's `column_offsets` index whichever list it was loaded against (public for a public index, public then non-public for a non-public one). Go's `TableCommon.Columns` is every `tblInfo.Columns` entry in metadata order, `Cols()` / `VisibleCols()` / `WritableCols()` / `DeletableCols()` are slices derived from it, and `IndexColumn.Offset` always indexes `tblInfo.Columns`; the two bases caused the MODIFY COLUMN mutation-checker panic | `pkg/table/tables/tables.go` (`TableCommon`, `initTableCommon`) |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
