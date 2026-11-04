# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-04; the [JSON register](structural-findings.json)
holds the same rows. A repaired finding is removed, not kept.

**32 unresolved: 11 open, 21 partial.**

A row states the remaining gap only. "Restate" marks a row whose earlier text
named no concrete mismatch ("complete package obligations remain"); it stays
unresolved until it is restated against its Go owner or closed.

## Unresolved

### System tables and startup

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| Y01 | partial | The DDL scheduler, systable manager, history and MDL readers scan records instead of running SQL on `DdlSessionPool::sess_pool`; the bootstrap stages raw rows; accounts load eagerly where Go loads per active user | `pkg/ddl` (`systable`, job scheduler), `pkg/session` bootstrap, `pkg/privilege/privileges` (`ensureActiveUser`) |
| O02 | open | No versioned upgrade: startup knows only bootstrapped or not (`tidb-session/src/mysql_bootstrap`) | `pkg/session/upgrade*.go` |
| I04 | partial | No lazy InfoSchema V2, schema-cache-size loader/evictor or InfoCache timestamp ranges | `pkg/infoschema` |
| O09 | partial | `ReportMinStartTS` skips sessions waiting on a queued DDL job, but lazy cursor fetch (`execStmtResult.TryDetach`, `tidb_enable_lazy_cursor_fetch`) is not ported, so no session owns a `CursorTracker` (the `tidb-session-cursor` port is unused) and a detached cursor's start TS is not reported; and there is no InfoCache recent-schema TS (`GetAndResetRecentInfoSchemaTS`, InfoSchema V2: I04) | `pkg/domain/infosync` |

### DDL

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| D01 | partial | 53 of Go's 86 `ddl.executor` methods (`pkg/ddl/executor.go`) have no counterpart, `DoDDLJob` / `DoDDLJobWrapper` among them: a statement is lowered into this port's own `DdlStatement` (`cluster_ddl.rs` `lower_ddl_with_context`, then a `lower_*` builder) and submitted by `submit_built_job` / `submit_persisted_job_with_retry`; the missing methods include `AlterSchema`, `CreateTableWithInfo` / `BatchCreateTableWithInfo`, `RebaseAutoID`, `ShardRowID`, `ChangeColumn`, `RenameColumn`, `RenameIndex`, `DropView`, `CreatePrimaryKey`, `LockTables`, `CleanupTableLock`, `RepairTable`, `CreateSequence` / `AlterSequence`, the placement-policy, check-constraint, masking-policy, TTL, cache and attribute alters | `pkg/ddl/executor.go`, `DoDDLJob` |
| D02 | partial | No ingest (fast-reorg) backfill: it falls back to the transactional merge (`tidb-ddl/src/cluster_ddl/add_index.rs`) | `pkg/ddl/backfilling*.go`, `pkg/ddl/ingest` |
| D03 | partial | Backfill runs one batch per reorg step on the DDL worker (`worker.rs` `AddIndexReorgWork::Backfill` -> `KvTableBackfill::backfill_index_batch`), where Go's `txnBackfillScheduler` (`backfilling_scheduler.go`) runs `tidb_ddl_reorg_worker_cnt` backfill workers over split task ranges and resizes them (`adjustWorkerSize`) | `pkg/ddl/backfilling_scheduler.go` |
| D05 | partial | Cancellation follows Go's `convertJob2RollbackJob` for every action the worker runs; ADD COLUMNAR INDEX has no worker step here, so its `rollingbackAddColumanrIndex` is absent with it | `pkg/ddl` |
| D11 | partial | ADD INDEX / MODIFY COLUMN always skip the DDL-time ANALYZE that Go's `checkNeedAnalyze` / `doAnalyzeWithoutReorg` run when `tidb_stats_update_during_ddl` is ON (default OFF); and a multi-schema change has no `promoteProxyKVDiskFullPause`, whose only producer (`autoPauseAddIndexJobOnKVDiskFull`, the DXF add-index task) Rust lacks (D02, O10) | `pkg/ddl/multi_schema_change.go`, `pkg/ddl/index.go`, `pkg/ddl/modify_column.go` |
| F02 | partial | With TiFlash enabled, ADD PARTITION and REORGANIZE PARTITION of a table whose TiFlash replica is available refuse in the replica-only state instead of Go's `checkPartitionReplica` wait (PD stores' TiFlash engine labels and each added partition's region peers), its rollback when that check errs, and `UpdateTiFlashProgressCache` for the added partitions | `pkg/ddl/partition.go` |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| A01 | partial | A statement's privilege check plans it a second, logical-only time to collect `visitInfo` (`query_visit_info` / `dml_visit_info`, from `Session` in `identity.rs`), where Go's `optimize` checks the `visitInfo` of the one `PlanBuilder` that builds the statement before optimizing it (`CheckPrivilege`) | `pkg/planner/core` (`visitInfo`, `CheckPrivilege`) |
| E02 | partial | `foreign_key.rs` enforces a foreign key by reading the referencing rows (`lookup_handles` / `cascade_at_depth`), where Go's `FKCheckExec` checks the referenced keys against the transaction (`checkKeys` / `checkIndexKeys`, with prefetch and locks) and `FKCascadeExec` builds a DELETE/UPDATE sub-statement (`GenCascadeDeleteAST`, `buildFKCascadePlan`) and runs it as an executor; and neither reports `FKCheckRuntimeStats` / `FKCascadeRuntimeStats` | `pkg/executor/foreign_key.go` |
| E03 | partial | UPDATE and DELETE are driver functions that stream their physical child (`driver/dml.rs` `run_delete_with_physical` / the update runners, `multi_dml.rs`), not Go's `UpdateExec` / `DeleteExec` executors: no executor node owns their `memTracker`, `updatedRowKeys` (`kv.MemAwareHandleMap`, charged per handle) or runtime stats | `pkg/executor/update.go`, `delete.go` |
| E04 | partial | An Apply whose inner side holds a CTE correlated to the Apply's outer row runs serially (`cte_correlated_to` in `driver/physical_builder.rs`), where Go runs it on the parallel workers: Go's workers share the one CTE producer `buildApply` builds from the original inner plan (`CTEStorageMap`), whose correlated cells no worker binds, so Go answers every such row NULL (gorun: `SELECT a, (WITH c AS (SELECT t2.a FROM t t2 WHERE t2.a <= t.a) SELECT SUM(x.a) FROM c x JOIN c y ON x.a = y.a) FROM t` gives NULL on every row under `tidb_enable_parallel_apply`, 1, 3, 6, ... serially). This port keeps the correct serial answer; an uncorrelated CTE shares its producer across the workers as Go's does | `pkg/executor/builder.go` (`buildApply`, `buildCTE`), `pkg/planner/core/optimizer.go` (`enableParallelApply`) |
| E05 | open | IMPORT INTO refuses (`tidb-session/src/dispatch.rs`) | `pkg/executor/importer`, `pkg/disttask/importinto` |
| E07 | open | BRIE statements are classified but never executed | `pkg/executor/brie.go` |
| K03 | partial | Go's `Datum.ConvertTo(ctx, target)` reports every truncation and overflow through the statement's `ctx.HandleTruncate` (its error-level context) and returns `(Datum, error)`; here `Datum::convert_to` / `convert_to_in` return a value plus a `ScalarConversionEvent` (`tidb-datatype/src/convert.rs`) that each caller maps to a warning or an error itself (14 files: `tidb-table` `column.rs` / `column_default.rs`, `write_cast.rs`, `tidb-expr` `cast.rs`, the ranger, point-get keys, ...), beside a second `Diagnostics` path (`convert_to_reported`) used only for YEAR / ENUM / SET / BIT targets | `pkg/types/datum.go` (`ConvertTo`), `pkg/types/context.go` (`HandleTruncate`) |
| X01 | partial | SQL/PB signature construction and typed vector execution; Go's build-time argument casts (`WrapWithCastAsJSON`, `isnull`'s JSON-to-string cast) are applied per value at evaluation instead of appearing in the expression tree (and EXPLAIN) The planner's `BuiltinFunctionUsageInc` (`expressionRewriter.newFunction`) is not called: it needs the function's `PbCode`, which a planner-built Rust function only gets at pushdown resolution. | `pkg/expression` |
| X02 | open | No `EMBED_TEXT` runtime or inference provider | `pkg/expression` (embedding), `pkg/domain` |
| Q01 | open | No Cascades memo exploration/implementation in the production optimizer | `pkg/planner/cascades` |

### Privileges, wire and administration

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| N05 | partial | The status port serves `/status`, `/metrics`, `/settings`, `/config`, `/schema`, `/schema_storage`, `/db-table/{tableID}`, `/tables/{colID}/{colTp}/{colFlag}/{colLen}`, `/labels`, `/info`, `/info/all`, `/ddl/history`, `/ddl/owner/resign`, `/txn-gc-states` and `/tiflash/replica` (`tidb-server/src/http_status.rs`); Go's other routes are absent: `/mvcc/*`, `/regions/*`, `/tables/{db}/{table}/{regions,ranges,scatter,stop-scatter,disk-usage}` (no region-cache handler tool), `/stats/dump/*` and `/plan_replayer/dump` (no `DumpStatsToJSON` port), `/stats/priority-queue`, `/optimize_trace/dump`, `/extract_task/dump`, `/dxf/*`, `/ddl/check/*`, `/ingest/*`, `/tiflash/replica-deprecated` (needs the dropped/truncated-table history and `UpdateTableReplicaInfo`), `/upgrade/*`, `/owner_manager/auto_id_service`, `/metrics/profile`, `/debug/*`, test hooks | `pkg/server/http_status.go`, `pkg/server/handler` |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| O04 | open | No TTL job/task manager (`tidb-ttl` has helpers only) | `pkg/ttl/ttlworker` |
| O05 | open | `pkg/resourcegroup/runaway` (`RunawayChecker`, `Manager`, the `mysql.tidb_runaway_queries` flusher and the watch syncer) has no port: `kv_contract.rs`'s `RunawayChecker` trait has no implementation, so a resource group's QUERY_LIMIT is stored but no statement is checked, killed, cooled down or watched | `pkg/resourcegroup/runaway` |
| O08 | open | No plan-replayer owner composed | `pkg/domain/plan_replayer*.go` |
| O10 | open | No DXF scheduler/executor managers composed | `pkg/disttask/framework` |
| O11 | partial | TopSQL has its `state`, `stmtstats` and the reporter's data models only: `topsql.go` (`SetupTopSQL`, `AttachSQLAndPlanInfo`, `RegisterSQL` / `RegisterPlan`), `collector/cpu.go` (the SQL CPU collector) and the reporter (`reporter.go` `RemoteTopSQLReporter`, `datasink.go`, `pubsub.go`, `single_target.go`, `report_ticker.go`) have no port, so nothing collects or reports TopSQL | `pkg/util/topsql` |
| O15 | open | No cross-keyspace runtime (null keyspace only) | `pkg/keyspace`, `pkg/domain` |
| O18 | partial | Statement-summary records (`exec_stmt_observation.rs`) leave Go fields unset: `plan_cache_unqualified` (empty: a prepared statement's refusal is warned by the session (`prepare_bound_execution`) or recorded on a planning `StmtContext` whose `PlanCacheTracker` is not the one that publishes the process plan, so Go's `StmtCtx.PlanCacheUnqualified()` has no single owner), `mem_arbitration` (0) and every `CpuUsages` term but TiKV time (Go's `SQLCPUUsages` also records TiDB CPU, O11) | `pkg/util/stmtsummary` |

### Information schema

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| I07 | partial | A CLUSTER_* table read is a hand-built fan-out (`ClusterPeerClient::scan_projected`: a TableScan plus at most a DIGEST IN selection, no key ranges, no `Desc`) where Go's planner builds a TableReader whose plan pushed to the `kv.TiDB` stores carries the selections, limit, TopN and aggregation, the key ranges and the scan order; so the receiving handler (`peer_rpc.rs`) builds no `SlowQueryExtractor` for CLUSTER_SLOW_QUERY (Go `PBPlanBuilder.pbToTableScan`'s `Desc` and `buildTimeRangeFromKeyRange`, `predicatePushDown`'s `Extract`, `SetRowLimitHint`), reads every slow-log file forward, and charges no reader tracker; the requesting node reads its own share in-process (`local_cluster_table_rows_for`) instead of through its own coprocessor handler | `pkg/planner/core` (TiDB-store pushdown), `pkg/planner/core/pb_to_plan.go`, `pkg/executor/coprocessor.go` |

### Transport, PD and MPP

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| P03 | partial | PD's `serviceDiscovery` (`servicediscovery/service_discovery.go`) is not one owner here: member updates and the leader connection live in `pd/cluster.rs` / `pd/retry.rs`, with no per-member `serviceClient`, no `apiCandidateNodes` / `GetServiceClientByKind` (how Go picks a follower for a follower-handled region API) and no `checkServiceModeChanged` | PD client `servicediscovery` |
| M01 | open | The MPP coordinator lowers a hand-built single-fragment scan DAG and refuses every pushed Selection, TopN and aggregation, where Go builds the fragment/task graph from the physical plan (including disaggregated compute placement) | `pkg/planner/core` (MPP fragments), `pkg/executor/mpp_gather.go`, `pkg/store/copr/mpp.go` |
| M04 | partial | `pkg/executor/mppcoordmanager` (`MPPCoordinatorManager`: coordinator registration, `detectAndDelete` expiry, the server address) and the RPC server's `ReportMPPTaskStatus` have no port, so TiFlash's task-status reports reach no coordinator; and the native client's region cache drops an invalidated region (`invalidate_region_cache`) where client-go marks it with an `InvalidReason` (`InvalidateCachedRegionWithReason`, `EpochNotMatch` for TiFlash's stale regions) and keeps it, which `insertRegionToCache` reads to move a `NoLeader` region's reload to the next TiKV peer | `pkg/executor/internal/mpp` |

