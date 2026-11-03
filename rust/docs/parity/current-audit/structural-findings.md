# Structural findings register

Where the Rust rewrite's structure still differs from the Go owner it ports.
Re-audited against the tree on 2026-11-04; the [JSON register](structural-findings.json)
holds the same rows. A repaired finding is removed, not kept.

**48 unresolved: 12 open, 36 partial.**

A row states the remaining gap only. "Restate" marks a row whose earlier text
named no concrete mismatch ("complete package obligations remain"); it stays
unresolved until it is restated against its Go owner or closed.

## Unresolved

### System tables and startup

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| Y01 | partial | The global-stats merge after a partitioned ANALYZE reads partitions through the raw `ClusterStatsLoader` (`tidb-executor/src/real_tikv_analyze.rs`); the DDL scheduler, systable manager, history and MDL readers scan records instead of running SQL on `DdlSessionPool::sess_pool`; the bootstrap stages raw rows; accounts load eagerly where Go loads per active user | `pkg/statistics/handle/globalstats`, `pkg/ddl` (`systable`, job scheduler), `pkg/session` bootstrap, `pkg/privilege/privileges` (`ensureActiveUser`) |
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
| D11 | partial | Restate (multi-schema change durability and recovery) | `pkg/ddl/multi_schema_change.go` |
| F01 | partial | Restate: placement bundles are delivered by the DDL commit and the GC worker deletes retired rules; the TiFlash replica path is still polled | `pkg/ddl/placement*`, `pkg/domain/infosync` |
| F02 | partial | ADD PARTITION TiFlash readiness and rollback | `pkg/ddl/partition.go` |

### Executor, DML and expressions

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| A01 | partial | Some query privilege visits are collected in the executor driver instead of the planner's `visitInfo` | `pkg/planner/core` (`visitInfo`, `CheckPrivilege`) |
| E02 | partial | Foreign keys: distributed locking, physical cascade sub-statements, runtime stats | `pkg/executor/foreign_key.go` |
| E03 | partial | UPDATE/DELETE: retained handle metadata and chunk streaming | `pkg/executor/update.go`, `delete.go` |
| E04 | partial | Restate (parallel Apply) | `pkg/executor/parallel_apply.go` |
| E05 | open | IMPORT INTO refuses (`tidb-session/src/dispatch.rs`) | `pkg/executor/importer`, `pkg/disttask/importinto` |
| E07 | open | BRIE statements are classified but never executed | `pkg/executor/brie.go` |
| K03 | partial | Datum conversion: source/error taxonomy, unsigned table overflow, ANALYZE adapters | `pkg/types`, `pkg/table/tables` |
| X01 | partial | SQL/PB signature construction and typed vector execution; Go's build-time argument casts (`WrapWithCastAsJSON`, `isnull`'s JSON-to-string cast) are applied per value at evaluation instead of appearing in the expression tree (and EXPLAIN) | `pkg/expression` |
| X02 | open | No `EMBED_TEXT` runtime or inference provider | `pkg/expression` (embedding), `pkg/domain` |
| Q01 | open | No Cascades memo exploration/implementation in the production optimizer | `pkg/planner/cascades` |
| T01 | partial | Writes during non-public, temporary and global-index transitions | `pkg/table/tables` |
| S03 | partial | Session-state migration of DDL and hypothetical state | `pkg/sessionctx/sessionstates` |
| S04 | partial | SafeTS topology; the standalone fallback path | `pkg/sessiontxn/staleread` |

### Privileges, wire and administration

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| A02 | partial | Restate (account policy lifecycle; overlaps Y01's lazy account loading) | `pkg/privilege/privileges` |
| A03 | partial | Restate (certificate parsing and algorithm limits) | `pkg/util/misc.go`, `privileges.go` |
| N03 | partial | Configuration consumers: TSO/router options, RPC concurrency, stream readiness/prewarm, idle recovery | `pkg/config`, client-go |
| N05 | partial | Restate (status-port administrative handlers; check AutoID service hosting against `tidb-autoid/src/service_rpc.rs`) | `pkg/server/handler`, `pkg/autoid_service` |

### Domain and process services

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| O04 | open | No TTL job/task manager (`tidb-ttl` has helpers only) | `pkg/ttl/ttlworker` |
| O05 | partial | Runaway-query policy (the resource-group controller is installed) | `pkg/resourcegroup/runaway` |
| O06 | partial | The RU writer persists only read/write RU per group, computes the window on a fixed offset and stores end times in UTC | `pkg/domain` `requestUnitsWriterLoop` |
| O08 | open | No plan-replayer owner composed | `pkg/domain/plan_replayer*.go` |
| O10 | open | No DXF scheduler/executor managers composed | `pkg/disttask/framework` |
| O11 | partial | TopSQL: plan-digest attribution, DML write/commit attribution, CPU/RU profiling, reporter lifetime | `pkg/util/topsql` |
| O15 | open | No cross-keyspace runtime (null keyspace only) | `pkg/keyspace`, `pkg/domain` |
| O16 | open | No workload-learning worker | `pkg/domain/workloadlearning` |
| O17 | open | No telemetry collection/reporting | `pkg/telemetry` |
| O18 | partial | Statement summary: normalized plan digests, encoded plans, RU/CPU/network metadata | `pkg/util/stmtsummary` |

### Information schema

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| I01 | partial | Other dynamic memtable providers; internal transactions in TIDB_TRX | `pkg/executor/infoschema_reader.go` |
| I02 | partial | Restate (CLUSTER_INFO) | `pkg/executor/infoschema_reader.go` |
| I03 | partial | Other cluster tables and pushed operators; memory accounting | `pkg/executor/memtable_reader.go` |

### Transport, PD and MPP

| ID | Status | Remaining gap | Go owner |
| --- | --- | --- | --- |
| T02 | partial | Two region-cache lifecycles (TiDB and the native client): coprocessor RequestSelector, store-health recovery, batch-scan follower reads | client-go `internal/locate` |
| P03 | partial | PD router transport; primaryless member publication; readiness/prewarm; idle recovery | PD client `pkg/client` |
| P06 | partial | TSO dispatcher options and stream construction; RPC concurrency | PD client `tso_dispatcher.go` |
| O13 | partial | Restate (replica-read: local transaction scope, task-build-error counters, AZ/SafeTS) | `pkg/store/copr`, `pkg/sessiontxn` |
| M01 | open | The MPP coordinator refuses every pushed Selection, TopN and aggregation | `pkg/planner/core` (MPP fragments), `pkg/executor/mpp_gather.go` |
| M04 | partial | MPP coordinator registration/reporting; exact-epoch RetryRegions invalidation | `pkg/executor/internal/mpp` |
| M05 | partial | Restate (disaggregated compute topology coordinator) | `pkg/store/copr/mpp.go` |

