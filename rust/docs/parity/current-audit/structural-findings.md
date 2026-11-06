# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-04; the [JSON register](structural-findings.json)
holds the same rows. A repaired finding is removed, not kept.

**27 unresolved: 10 open, 17 partial.**

A row states the remaining gap only. "Restate" marks a row whose earlier text
named no concrete mismatch ("complete package obligations remain"); it stays
unresolved until it is restated against its Go owner or closed.

## Unresolved

### System tables and startup

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| Y01 | partial | The DDL scheduler, systable manager, history and MDL readers scan records instead of running SQL on `DdlSessionPool::sess_pool`; the bootstrap stages raw rows; accounts load eagerly where Go loads per active user | `pkg/ddl` (`systable`, job scheduler), `pkg/session` bootstrap, `pkg/privilege/privileges` (`ensureActiveUser`) |
| I04 | partial | No lazy InfoSchema V2, schema-cache-size loader/evictor or InfoCache timestamp ranges | `pkg/infoschema` |
| O09 | partial | `ReportMinStartTS` skips sessions waiting on a queued DDL job, but lazy cursor fetch (`execStmtResult.TryDetach`, `tidb_enable_lazy_cursor_fetch`) is not ported, so no session owns a `CursorTracker` (the `tidb-session-cursor` port is unused) and a detached cursor's start TS is not reported; and there is no InfoCache recent-schema TS (`GetAndResetRecentInfoSchemaTS`, InfoSchema V2: I04) | `pkg/domain/infosync` |

### DDL

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| D01 | partial | 53 of Go's 86 `ddl.executor` methods (`pkg/ddl/executor.go`) have no counterpart, `DoDDLJob` / `DoDDLJobWrapper` among them: a statement is lowered into this port's own `DdlStatement` (`cluster_ddl.rs` `lower_ddl_with_context`, then a `lower_*` builder) and submitted by `submit_built_job` / `submit_persisted_job_with_retry`; the missing methods include `AlterSchema`, `CreateTableWithInfo` / `BatchCreateTableWithInfo`, `RebaseAutoID`, `ShardRowID`, `ChangeColumn`, `RenameColumn`, `RenameIndex`, `DropView`, `CreatePrimaryKey`, `LockTables`, `CleanupTableLock`, `RepairTable`, `CreateSequence` / `AlterSequence`, the placement-policy, check-constraint, masking-policy, TTL, cache and attribute alters | `pkg/ddl/executor.go`, `DoDDLJob` |
| D02 | partial | No ingest (fast-reorg) backfill: it falls back to the transactional merge (`tidb-ddl/src/cluster_ddl/add_index.rs`); with it the status port's `/ingest/*` routes (Go `IngestConcurrencyHandler` over `ingestctrl`'s rate-limit parameters) are absent | `pkg/ddl/backfilling*.go`, `pkg/ddl/ingest` |
| D05 | partial | Cancellation follows Go's `convertJob2RollbackJob` for every action the worker runs. A columnar (vector, inverted, full-text) index is checked against its table as Go's `createColumnarIndex` does (`add_index.rs` `create_columnar_index`, `checkTableTypeForColumnarIndex`), but on a table with a columnar replica this node builds no `ActionAddColumnarIndex` job: `onCreateColumnarIndex` (TiFlash schema sync, `checkColumnarIndexProcess`) and its `rollingbackAddColumanrIndex` are absent | `pkg/ddl` |
| D11 | partial | A multi-schema change has no `promoteProxyKVDiskFullPause`, whose only producer (`autoPauseAddIndexJobOnKVDiskFull`, the DXF add-index task) Rust lacks (D02, O10) | `pkg/ddl/multi_schema_change.go` |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| A01 | partial | A statement's privilege check plans it a second, logical-only time to collect `visitInfo` (`query_visit_info` / `dml_visit_info`, from `Session` in `identity.rs`), where Go's `optimize` checks the `visitInfo` of the one `PlanBuilder` that builds the statement before optimizing it (`CheckPrivilege`) | `pkg/planner/core` (`visitInfo`, `CheckPrivilege`) |
| E02 | partial | `foreign_key.rs` enforces a foreign key by reading the referencing rows (`lookup_handles` / `cascade_at_depth`), where Go's `FKCheckExec` checks the referenced keys against the transaction (`checkKeys` / `checkIndexKeys`, with prefetch and locks) and `FKCascadeExec` builds a DELETE/UPDATE sub-statement (`GenCascadeDeleteAST`, `buildFKCascadePlan`) and runs it as an executor; and neither reports `FKCheckRuntimeStats` / `FKCascadeRuntimeStats` | `pkg/executor/foreign_key.go` |
| E03 | partial | INSERT, UPDATE and DELETE run as Go's write executors (`DmlExec`: plan-ID `memTracker`, basic stats, Go's chunk accounting, `updatedRowKeys` / `mergedRowData` / `tblRowMap` charges, `insertRows` / `insertRowsFromSelect` streaming and `InsertRuntimeStat`), but the insert stats lack Go's `AllocatorRuntimeStats` (`auto_id_allocator: {alloc_cnt, Get, commit_txn ...}` in `prepare`), which this tier's auto-ID allocator does not record | `pkg/executor/insert_common.go`, `pkg/meta/autoid` |
| E10 | open | Parallel HashAgg is not Go's worker model (`tidb-executor-aggregate/src/parallel.rs`): partial workers hand their maps over at a barrier (`TakeMaps`) and final buckets merge as pooled tasks, only for non-empty buckets, where Go's partial workers `shuffleIntermData` one `AggPartialResultMapper` to every final worker and all M final workers merge and send (`final_worker` `task_num` 1 where Go counts 5); memory is charged per Rust group (`PipelineMap`), not Go's mapper bucket growth, empty mappers and worker chunks (`initPartialWorkers` / `initFinalWorkers`): one-group `sum(a)` is 2.79 KB, Go 12.6 KB | `pkg/executor/aggregate` (`agg_hash_executor.go`, `agg_hash_partial_worker.go`, `agg_hash_final_worker.go`) |
| E11 | open | `StreamAggExec` and `IndexLookUpExecutor` own no `memTracker`: the physical builder charges the chunk each returns, and only under EXPLAIN ANALYZE (`physical_builder.rs` `chunk_memory`), where Go's charge their child result, partial results (`AllocPartialResult`'s sizes, which need E10's aggregate-function port) and the lookup's task chunks on every run: a `count(*)` stream aggregate shows 380 Bytes where Go shows 388, an index lookup 1.36 KB where Go shows 2.50 KB, so their quota behaviour also differs | `pkg/executor/aggregate/agg_stream_executor.go`, `distsql.go` (`IndexLookUpExecutor`) |
| E12 | open | An index join's inner reader records no runtime stats (`IndexReader` under `IndexJoin` prints `actRows` N/A and no execution info where Go prints its cop task stats), and the join's memory is not Go's (`IndexJoin` 32.7 KB where Go's `IndexLookUpJoin` charges 3.09 KB: its outer task chunks and lookup map) | `pkg/executor/join/index_lookup_join.go` |
| E13 | partial | A CTE's producer reports its runtime and memory on the CTE's plan as Go's does, but a `UNION DISTINCT` recursion's deduplication builds a compact chunk of the admitted rows where Go's `deduplicate` keeps the chunk and marks them with a selection (`chk.SetSel`), so its tables charge less: 1.13 KB where Go's charge 1.41 KB for a five-step distinct recursion | `pkg/executor/cte.go` (`deduplicate`) |
| E14 | open | The embedded store's coprocessor reports its executors' summaries unlike Go's unistore `cophandler`: a pushed TopN prints `actRows` 2 and no `tikv_task` (Go: its input, 6, with the scan's detail, the scan only `tikv_task`), and an index scan under a pushed Limit prints 3 rows read where Go's reads 5 | `pkg/store/mockstore/unistore/cophandler` |
| E04 | partial | An Apply whose inner side holds a CTE correlated to the Apply's outer row runs serially (`cte_correlated_to` in `driver/physical_builder.rs`), where Go runs it on the parallel workers: Go's workers share the one CTE producer `buildApply` builds from the original inner plan (`CTEStorageMap`), whose correlated cells no worker binds, so Go answers every such row NULL (gorun: `SELECT a, (WITH c AS (SELECT t2.a FROM t t2 WHERE t2.a <= t.a) SELECT SUM(x.a) FROM c x JOIN c y ON x.a = y.a) FROM t` gives NULL on every row under `tidb_enable_parallel_apply`, 1, 3, 6, ... serially). This port keeps the correct serial answer; an uncorrelated CTE shares its producer across the workers as Go's does | `pkg/executor/builder.go` (`buildApply`, `buildCTE`), `pkg/planner/core/optimizer.go` (`enableParallelApply`) |
| E05 | open | IMPORT INTO refuses (`tidb-session/src/dispatch.rs`) | `pkg/executor/importer`, `pkg/disttask/importinto` |
| E07 | open | BRIE statements are classified but never executed | `pkg/executor/brie.go` |
| X01 | partial | SQL/PB signature construction and typed vector execution. The planner's `BuiltinFunctionUsageInc` (`expressionRewriter.newFunction`) is not called: it needs the function's `PbCode`, which a planner-built Rust function only gets at pushdown resolution. | `pkg/expression` |
| Q01 | open | No Cascades memo exploration/implementation in the production optimizer | `pkg/planner/cascades` |

### Privileges, wire and administration

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| O10 | open | No DXF scheduler/executor managers composed | `pkg/disttask/framework` |
| O11 | partial | TopSQL is ported over pprof-rs (`tidb-cpuprofile`, `tidb-topsql-collector`, `tidb-topsql-reporter`, `tidb-topsql`): the status port serves `TopSQLPubSub`, the single-target sink reports to `top-sql.receiver-address`, each command labels its thread `{connID}_{sqlID}`, each statement registers its SQL and plan and labels the thread with both digests, and the profiler's TiDB CPU reaches PROCESSLIST and the statement summary. The DDL worker attaches each job step and each backfill task to the job's SQL (`tidb-ddl` `job_ctx.rs`, Go `ReorgContext.attachTopProfilingInfo`). The step's transaction carries the job's resource-group tagger (`getResourceGroupTaggerForTopSQL`, published into its statement kv context once the step loads the job). Unported: the DXF backfill executor's attach (O10) and the fast-reorg skip checks' (`checkIfTableReorgWorkCanSkip` / `checkIfTempIndexReorgWorkCanSkip`, D02), and the multi-statement prefetch's plan hand-off: `prefetchPointPlanKeys` reads (or locks) the point writes' keys before the statements run (`cluster_session_node/prefetch.rs`, with its `setResourceGroupTaggerForMultiStmtPrefetch`), but its point plans are not handed to the statements (Go `PointPlanKey`), which plan again. Executors register the statement's SQL and plan on their first `Next` with Top profiling on (`RegisterSQLAndPlanInExecForTopProfiling`, `SqlKillerCheckExec`); profiler labels follow Go's goroutine inheritance (`labels::inherit`) into the shuffle, hash-join build, checksum, coprocessor, slow-log parse, cluster-table fan-out, analyze and global-stats merge, DDL analyze workers and the MPP client's store probes | `pkg/util/topsql` |
| O15 | open | No cross-keyspace runtime (null keyspace only); the next-gen-only surfaces built on it have no port either: the status port's `/dxf/*` and `/ddl/check/*`, and plan replayer's `tidb_cloud_storage_uri` storage (`pkg/planner/extstore` `createGlobalExtStorage`, local files on a classic kernel) | `pkg/keyspace`, `pkg/domain`, `pkg/planner/extstore` |

### Information schema

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| I07 | partial | A CLUSTER_* table read is a hand-built fan-out (`ClusterPeerClient::scan_projected`: a TableScan plus at most a DIGEST IN selection, no key ranges, no `Desc`) where Go's planner builds a TableReader whose plan pushed to the `kv.TiDB` stores carries the selections, limit, TopN and aggregation, the key ranges and the scan order; so the receiving handler (`peer_rpc.rs`) builds no `SlowQueryExtractor` for CLUSTER_SLOW_QUERY (Go `PBPlanBuilder.pbToTableScan`'s `Desc` and `buildTimeRangeFromKeyRange`, `predicatePushDown`'s `Extract`, `SetRowLimitHint`), reads every slow-log file forward, and charges no reader tracker; the requesting node reads its own share in-process (`local_cluster_table_rows_for`) instead of through its own coprocessor handler | `pkg/planner/core` (TiDB-store pushdown), `pkg/planner/core/pb_to_plan.go`, `pkg/executor/coprocessor.go` |

### Transport, PD and MPP

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| P03 | partial | PD's `serviceDiscovery` (`servicediscovery/service_discovery.go`) is not one owner here: member updates and the leader connection live in `pd/cluster.rs` / `pd/retry.rs`, with no per-member `serviceClient`, no `apiCandidateNodes` / `GetServiceClientByKind` (how Go picks a follower for a follower-handled region API) and no `checkServiceModeChanged` | PD client `servicediscovery` |
| M01 | open | The MPP coordinator lowers a hand-built single-fragment scan DAG and refuses every pushed Selection, TopN and aggregation, where Go builds the fragment/task graph from the physical plan (including disaggregated compute placement) | `pkg/planner/core` (MPP fragments), `pkg/executor/mpp_gather.go`, `pkg/store/copr/mpp.go` |
| M04 | partial | `pkg/executor/mppcoordmanager` (`MPPCoordinatorManager`: coordinator registration, `detectAndDelete` expiry, the server address) and the RPC server's `ReportMPPTaskStatus` have no port, so TiFlash's task-status reports reach no coordinator; Go's coordinator only asks TiFlash for reports when its plan has a Limit above the table reader (`needReportExecutionSummary`), which the single-fragment scan never builds (M01) | `pkg/executor/internal/mpp` |

