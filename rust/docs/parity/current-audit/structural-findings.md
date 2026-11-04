# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-04; the [JSON register](structural-findings.json)
holds the same rows. A repaired finding is removed, not kept.

**45 unresolved: 14 open, 31 partial.**

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
| O09 | partial | The min-start-TS reporter lacks V2 recent-schema reporting and DDL-queue exclusion | `pkg/domain/infosync` |

### DDL

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| D01 | partial | DDL is submitted as jobs (`tidb-ddl/src/job_submitter.rs`); inventory any statement still outside the job path | `pkg/ddl/executor.go`, `DoDDLJob` |
| D02 | partial | No ingest (fast-reorg) backfill: it falls back to the transactional merge (`tidb-ddl/src/cluster_ddl/add_index.rs`) | `pkg/ddl/backfilling*.go`, `pkg/ddl/ingest` |
| D03 | partial | Backfill is one sequential batch loop, not Go's reorg worker pool | `pkg/ddl/backfilling_scheduler.go` |
| D05 | partial | Restate (DDL error identity and rollback classification) | `pkg/ddl` |
| D11 | partial | A multi-schema change does not promote a sub-job's KV-disk-full pause to the parent job (Go `promoteProxyKVDiskFullPause`), and ADD INDEX / MODIFY COLUMN always skip the DDL-time ANALYZE that Go's `checkNeedAnalyze` runs when `tidb_stats_update_during_ddl` is ON (default OFF) | `pkg/ddl/multi_schema_change.go`, `pkg/ddl/index.go`, `pkg/ddl/modify_column.go` |
| F02 | partial | ADD PARTITION TiFlash readiness and rollback | `pkg/ddl/partition.go` |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| A01 | partial | Some query privilege visits are collected in the executor driver instead of the planner's `visitInfo` | `pkg/planner/core` (`visitInfo`, `CheckPrivilege`) |
| E02 | partial | Foreign keys: distributed locking, physical cascade sub-statements, runtime stats | `pkg/executor/foreign_key.go` |
| E03 | partial | UPDATE/DELETE: retained handle metadata and chunk streaming | `pkg/executor/update.go`, `delete.go` |
| E04 | partial | `enable_parallel_apply` decides clonability by `supports_apply_worker_clone` (refuses only CTE/CTETable/Shuffle) instead of Go's `SafeClone` over each operator's `Clone` (29 implement it; others such as BatchPointGet refuse, while PhysicalCTE clones), and never appends Go's `Some apply operators can not be executed in parallel` warning | `pkg/planner/core/optimizer.go` (`enableParallelApply`), `physicalop.SafeClone` |
| E05 | open | IMPORT INTO refuses (`tidb-session/src/dispatch.rs`) | `pkg/executor/importer`, `pkg/disttask/importinto` |
| E07 | open | BRIE statements are classified but never executed | `pkg/executor/brie.go` |
| K03 | partial | Datum conversion: source/error taxonomy, unsigned table overflow, ANALYZE adapters | `pkg/types`, `pkg/table/tables` |
| X01 | partial | SQL/PB signature construction and typed vector execution; Go's build-time argument casts (`WrapWithCastAsJSON`, `isnull`'s JSON-to-string cast) are applied per value at evaluation instead of appearing in the expression tree (and EXPLAIN) The planner's `BuiltinFunctionUsageInc` (`expressionRewriter.newFunction`) is not called: it needs the function's `PbCode`, which a planner-built Rust function only gets at pushdown resolution. | `pkg/expression` |
| X02 | open | No `EMBED_TEXT` runtime or inference provider | `pkg/expression` (embedding), `pkg/domain` |
| Q01 | open | No Cascades memo exploration/implementation in the production optimizer | `pkg/planner/cascades` |
| T01 | partial | Writes during non-public, temporary and global-index transitions | `pkg/table/tables` |
| S03 | partial | Session-state migration of DDL and hypothetical state | `pkg/sessionctx/sessionstates` |
| S04 | partial | SafeTS topology; the standalone fallback path | `pkg/sessiontxn/staleread` |

### Privileges, wire and administration

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| A02 | partial | Restate (account policy lifecycle; overlaps Y01's lazy account loading) | `pkg/privilege/privileges` |
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
| X03 | open | A plain EXPLAIN prepares the plan but builds no target executor, where Go `buildExplain` builds it (`explainExec.analyzeExec = b.build(v.TargetPlan)`): the builder's side effects (index-merge and table-lookup telemetry) do not happen. Blocked on memory tables: a Rust memtable reader needs its retrieved rows at build (`MemTableRetrievalNeeded`), where Go's retriever reads them at `Next` | `pkg/executor/builder.go` (`buildExplain`) |
| O15 | open | No cross-keyspace runtime (null keyspace only) | `pkg/keyspace`, `pkg/domain` |
| O18 | partial | Statement summary: normalized plan digests, encoded plans, RU/CPU/network metadata | `pkg/util/stmtsummary` |
| O19 | open | `StmtNodeCounter` is counted at the wire (`tidb-server/src/mysql_connection.rs`) instead of in `executor.Compile` after `Optimize` succeeds: sessions without a wire (internal, embedded) count nothing, and a statement whose planning fails is counted | `pkg/executor/compiler.go` (`CountStmtNode`) |

### Information schema

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| I01 | partial | Other dynamic memtable providers; internal transactions in TIDB_TRX | `pkg/executor/infoschema_reader.go` |
| I03 | partial | Other cluster tables and pushed operators; memory accounting | `pkg/executor/memtable_reader.go` |

### Transport, PD and MPP

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| T02 | partial | Two region-cache lifecycles (TiDB and the native client): coprocessor RequestSelector, store-health recovery, batch-scan follower reads | client-go `internal/locate` |
| T04 | partial | client-rust's committer fills `CommitDetails`' write keys/size and prewrite, latest-ts, commit-ts, commit and local-latch times (published through `CommitDetailCtxKey` and merged into the statement), but not client-go's prewrite/commit backoff time and types, per-request `MergePrewriteReqDetails`/`MergeCommitReqDetails`, prewrite region and request counts, resolve-lock detail or binlog wait | client-go `txnkv/transaction` (`twoPhaseCommitter` detail), `pkg/session` (`doCommit`), `pkg/util/execdetails` |
| T05 | open | client-rust's `LockKeysDetails` carries the locked-key count, total time and fair-locking counts, but not client-go's region count, lock RPC time/count, retry count, backoff time/types or resolve-lock detail | client-go `txnkv/transaction` (`pessimisticLockMutations`, `LockKeys`), `util.LockKeysDetails` |
| T06 | partial | `tidb-txnkv::retry::RegionBackoffBudget`/`RegionBackoffKind` wrap client-rust's Backoffer under their own names, where Go TiDB's copr/MPP/lock callers use client-go `backoff.NewBackofferWithVars` and its configs directly | client-go `config/retry`, `pkg/store/copr` |
| P03 | partial | PD router transport; primaryless member publication; readiness/prewarm; idle recovery | PD client `pkg/client` |
| P06 | partial | TSO dispatcher options and stream construction; RPC concurrency | PD client `tso_dispatcher.go` |
| M01 | open | The MPP coordinator lowers a hand-built single-fragment scan DAG and refuses every pushed Selection, TopN and aggregation, where Go builds the fragment/task graph from the physical plan (including disaggregated compute placement) | `pkg/planner/core` (MPP fragments), `pkg/executor/mpp_gather.go`, `pkg/store/copr/mpp.go` |
| M04 | partial | MPP coordinator registration/reporting; exact-epoch RetryRegions invalidation | `pkg/executor/internal/mpp` |

