# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-04; the [JSON register](structural-findings.json)
holds the same rows. A repaired finding is removed, not kept.

**39 unresolved: 11 open, 28 partial.**

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
| D01 | partial | DDL is submitted as jobs (`tidb-ddl/src/job_submitter.rs`); inventory any statement still outside the job path | `pkg/ddl/executor.go`, `DoDDLJob` |
| D02 | partial | No ingest (fast-reorg) backfill: it falls back to the transactional merge (`tidb-ddl/src/cluster_ddl/add_index.rs`) | `pkg/ddl/backfilling*.go`, `pkg/ddl/ingest` |
| D03 | partial | Backfill is one sequential batch loop, not Go's reorg worker pool | `pkg/ddl/backfilling_scheduler.go` |
| D05 | partial | Cancellation follows Go's `convertJob2RollbackJob` for every action the worker runs; ADD COLUMNAR INDEX has no worker step here, so its `rollingbackAddColumanrIndex` is absent with it | `pkg/ddl` |
| D11 | partial | ADD INDEX / MODIFY COLUMN always skip the DDL-time ANALYZE that Go's `checkNeedAnalyze` / `doAnalyzeWithoutReorg` run when `tidb_stats_update_during_ddl` is ON (default OFF); and a multi-schema change has no `promoteProxyKVDiskFullPause`, whose only producer (`autoPauseAddIndexJobOnKVDiskFull`, the DXF add-index task) Rust lacks (D02, O10) | `pkg/ddl/multi_schema_change.go`, `pkg/ddl/index.go`, `pkg/ddl/modify_column.go` |
| F02 | partial | With TiFlash enabled, ADD PARTITION and REORGANIZE PARTITION of a table whose TiFlash replica is available refuse in the replica-only state instead of Go's `checkPartitionReplica` wait (PD stores' TiFlash engine labels and each added partition's region peers), its rollback when that check errs, and `UpdateTiFlashProgressCache` for the added partitions | `pkg/ddl/partition.go` |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| A01 | partial | A statement's privilege check plans it a second, logical-only time to collect `visitInfo` (`query_visit_info` / `dml_visit_info`, from `Session` in `identity.rs`), where Go's `optimize` checks the `visitInfo` of the one `PlanBuilder` that builds the statement before optimizing it (`CheckPrivilege`) | `pkg/planner/core` (`visitInfo`, `CheckPrivilege`) |
| E02 | partial | Foreign keys: distributed locking, physical cascade sub-statements, runtime stats | `pkg/executor/foreign_key.go` |
| E03 | partial | UPDATE/DELETE: retained handle metadata and chunk streaming | `pkg/executor/update.go`, `delete.go` |
| E04 | partial | `enableParallelApply` follows Go's `SafeClone` (with its warning) and `KeepOrder`, but the executor runs an Apply whose inner child holds a CTE serially (`supports_apply_worker_clone`), where Go clones `PhysicalCTE` and runs its workers in parallel | `pkg/planner/core/optimizer.go` (`enableParallelApply`), `physicalop.SafeClone` |
| E05 | open | IMPORT INTO refuses (`tidb-session/src/dispatch.rs`) | `pkg/executor/importer`, `pkg/disttask/importinto` |
| E07 | open | BRIE statements are classified but never executed | `pkg/executor/brie.go` |
| K03 | partial | Datum conversion: source/error taxonomy, unsigned table overflow, ANALYZE adapters | `pkg/types`, `pkg/table/tables` |
| X01 | partial | SQL/PB signature construction and typed vector execution; Go's build-time argument casts (`WrapWithCastAsJSON`, `isnull`'s JSON-to-string cast) are applied per value at evaluation instead of appearing in the expression tree (and EXPLAIN) The planner's `BuiltinFunctionUsageInc` (`expressionRewriter.newFunction`) is not called: it needs the function's `PbCode`, which a planner-built Rust function only gets at pushdown resolution. | `pkg/expression` |
| X02 | open | No `EMBED_TEXT` runtime or inference provider | `pkg/expression` (embedding), `pkg/domain` |
| Q01 | open | No Cascades memo exploration/implementation in the production optimizer | `pkg/planner/cascades` |
| T01 | partial | Writes during non-public, temporary and global-index transitions | `pkg/table/tables` |
| S04 | partial | `tidb_read_staleness` reads at Go's `CalAppropriateTime` against the stores' min SafeTS, but `TIDB_BOUNDED_STALENESS` still evaluates without it: no statement's evaluation context supplies `GetStmtMinSafeTime` (`Columns::bounded_staleness_safe_time` has no production override), so it always falls back to its lower bound | `pkg/sessiontxn/staleread` |

### Privileges, wire and administration

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| N03 | partial | Configuration consumers: TSO/router options, RPC concurrency, stream readiness/prewarm, idle recovery | `pkg/config`, client-go |
| N05 | open | The status port serves only `/status`, `/metrics`, `/settings`, `/config`, `/schema` and `/schema_storage` (`tidb-server/src/http_status.rs`); Go's other routes are absent: `/info`, `/info/all`, `/labels`, `/ddl/history`, `/ddl/owner/resign`, `/mvcc/*`, `/regions/*`, `/tables/*`, `/db-table/{id}`, `/stats/dump/*`, `/plan_replayer/dump`, `/optimize_trace/dump`, `/extract_task/dump`, `/dxf/*`, `/ingest/*`, `/tiflash/replica`, `/upgrade/*`, `/txn-gc-states`, `/debug/*`, test hooks | `pkg/server/http_status.go`, `pkg/server/handler` |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| O04 | open | No TTL job/task manager (`tidb-ttl` has helpers only) | `pkg/ttl/ttlworker` |
| O05 | partial | Runaway-query policy (the resource-group controller is installed) | `pkg/resourcegroup/runaway` |
| O08 | open | No plan-replayer owner composed | `pkg/domain/plan_replayer*.go` |
| O10 | open | No DXF scheduler/executor managers composed | `pkg/disttask/framework` |
| O11 | partial | TopSQL: plan-digest attribution, DML write/commit attribution, CPU/RU profiling, reporter lifetime | `pkg/util/topsql` |
| O15 | open | No cross-keyspace runtime (null keyspace only) | `pkg/keyspace`, `pkg/domain` |
| O18 | partial | Statement summary: normalized plan digests, encoded plans, RU/CPU/network metadata | `pkg/util/stmtsummary` |
| O19 | partial | A text query is counted once its compile boundary planned it, but a binary-protocol EXECUTE counts its prepared statement before any of its plan paths (cached point get, cached SELECT, cached DML, rebuilt) binds or plans it, so an EXECUTE whose planning fails is counted where Go's `Compile` error is not | `pkg/executor/compiler.go` (`CountStmtNode`) |

### Information schema

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| I01 | partial | Every table Go's `buildMemTable` serves has a provider here (CLUSTER_* through their local sources) except the TiFlash system tables: TIFLASH_TABLES and TIFLASH_SEGMENTS answer no rows and TIFLASH_INDEXES has none, where Go's `tiflashSystemTableRetriever` queries each TiFlash store's system tables over HTTP | `pkg/executor/infoschema_reader.go` |
| I03 | partial | The memory-table rows a retriever builds charge no memory tracker, where Go's `memtableRetriever` accounts them to the reader's `memTracker` in batches of 1024 (`recordMemoryConsume`) | `pkg/executor/infoschema_reader.go` (`memtableRetriever`) |

### Transport, PD and MPP

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| T02 | partial | Two region-cache lifecycles (TiDB and the native client): coprocessor RequestSelector, store-health recovery, batch-scan follower reads | client-go `internal/locate` |
| P03 | partial | PD router transport; primaryless member publication; readiness/prewarm; idle recovery | PD client `pkg/client` |
| P06 | partial | TSO dispatcher options and stream construction; RPC concurrency | PD client `tso_dispatcher.go` |
| M01 | open | The MPP coordinator lowers a hand-built single-fragment scan DAG and refuses every pushed Selection, TopN and aggregation, where Go builds the fragment/task graph from the physical plan (including disaggregated compute placement) | `pkg/planner/core` (MPP fragments), `pkg/executor/mpp_gather.go`, `pkg/store/copr/mpp.go` |
| M04 | partial | MPP coordinator registration/reporting; exact-epoch RetryRegions invalidation | `pkg/executor/internal/mpp` |

