# Remaining structural mismatches, 2026-09-30

Compared integration `9f0a41b5db1c3ceaa9b7f2babe5ddf195a93e363` with freshly fetched
TiDB master `e953a09d9d5e29e60c62f42d3aacebb819af49a5`. The integration pull was
already current. Go means this master, including its selected external modules:
client-go `v2.0.8-0.20260928031501-8edb23f6c7ee`, kvproto
`v0.0.0-20260820070758-623e58e60fa9`, PD client
`v0.0.0-20260805103528-afa43111d149`, etcd API `v3.5.15`, and TiPB
`v0.0.0-20260908093239-fed7bc47c39d`.

This consolidates the **known open structural findings**, including the earlier
DDL and storage findings, rather than repeatedly listing only the latest fix.
It does **not** certify that all semantic mismatches have been discovered.
The complete artifact inventories cover 856 TiDB package directories, 41
client-go directories, 41 kvproto directories, 24 PD-client directories, 7
etcd-API directories and all 83 Rust manifests.
Their current-master package acceptance remains unreviewed. The 2,050 search
candidate lines are not a defect count. Historical receipts and ignored-test
comments can be stale; they are not substituted for current source review.

Paths below are relative to the repository root. Go function references refer
to the master above, not this branch's Go working tree. Unless marked as a
reproduction, findings are source comparisons and their stated consequences
are inferences; no live distributed failure or benchmark is claimed.

## DDL: migrate responsibility before deleting implementations

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| D01 | Ordinary SQL DDL still uses a direct metadata transaction. Only CHECK SQL submits to the persisted worker. CREATE/DROP/RENAME worker handlers being present does not mean SQL uses them. | `rust/crates/tidb-server/src/cluster_session_node/ddl.rs:1065`, `rust/crates/tidb-exec/src/real_tikv_ddl.rs:1280` | `pkg/ddl/executor.go`, `pkg/ddl/jobsubmit`, `pkg/ddl/job_worker.go`: submit typed jobs, schedule and execute persisted transitions, then wait for completion. Remove the direct publisher only after all live callers migrate. |
| D02 | Online index construction/removal remains a whole-table operation in the DDL transaction; its backfiller also synthesizes default statement context. It lacks the source's durable schema phases, resumable reorg and persisted evaluation context. This is a concurrency/scale boundary, not just duplicate helper code. | `rust/crates/tidb-server/src/cluster_session_node/ddl.rs:1155`, `rust/crates/tidb-exec/src/real_tikv_ddl.rs:1342`, `rust/crates/tidb-exec/src/cluster_ddl.rs:9349` | `pkg/ddl/index.go::onCreateIndex`, `pkg/ddl/reorg.go`: DeleteOnly, WriteOnly, WriteReorganization, reorg workers, rollback and delete-range registration. Shares D01's migration prerequisite. |
| D03 | The scheduler runs each supported job to completion serially; it skips unsupported persisted jobs. It has no equivalent of the complete running-job/conflict/reorg-worker lifecycle. This limits independent DDL and mixed Go/Rust owner recovery. | `rust/crates/tidb-server/src/cluster_session_node/ddl.rs:212` | `pkg/ddl/job_scheduler.go`: job admission, running jobs, worker pools and dependency checks must own dispatch. Do not merely add more cases to the current loop. |
| D04 | The common action path changes every non-ROLLINGBACK state to RUNNING. PAUSING/PAUSED/CANCELLING therefore do not follow Go's control-state gates. | `rust/crates/tidb-exec/src/cluster_ddl.rs:3251` | `pkg/ddl/job_worker.go::processJobPausingRequest`, `runOneJobStep` and rollback conversion. Preserve these states before action dispatch. |
| D05 | A non-cancelling action error returns before shared job persistence; error count, global retry limit and transition to CANCELLING are missing. The last repair covers fresh cancellation and explicit handled errors, not this path. | `rust/crates/tidb-exec/src/cluster_ddl.rs:3257` | `pkg/ddl/job_worker.go::countForError` and `transitOneJobStep`: discard action mutations as required, retain the error/update transaction, enforce the owner's limit. |
| D06 | Some CHECK missing-table/missing-constraint paths return errors without selecting CANCELLED. Repeated scheduler passes cannot reproduce Go's terminal outcome. | `rust/crates/tidb-exec/src/cluster_ddl.rs::plan_persisted_check_constraint_job_step` | `pkg/ddl/constraint.go`: decode/object validation and action state jointly determine cancellation. Correct the complete action validation, not only message strings. |
| D07 | Persisted planning always obtains MDL table information and uses the MDL barrier. There is no equivalent MDL-disabled branch in this owner. | `rust/crates/tidb-exec/src/cluster_ddl.rs:3229`, `rust/crates/tidb-server/src/cluster_session_node/ddl.rs::ClusterSchemaSync` | `pkg/ddl/job_worker.go`, `pkg/ddl/schema_version.go`: select MDL or lease-based schema synchronization from the real operation mode. |
| D08 | DROP completion does not register all of Go's typed delete-range work with its durable GC owner. The direct index/partition path can perform immediate local key deletion instead. | `rust/crates/tidb-exec/src/cluster_ddl.rs:4429`, `:9349`, `rust/crates/tidb-executor/src/kv_table/partition_maintenance.rs:20` | `pkg/ddl/delete_range.go`, `pkg/ddl/job_worker.go::finishDDLJob`, typed job arguments and `pkg/store/gcworker`. Registration and the worker in O03 are separate required responsibilities. |
| D09 | Materialized-view build evaluates a snapshot copied into memory and emits the built rows in the completion transaction; Go has distinct build/reorg sessions and transactions. **Seed only; live dispatch remains disabled.** | `rust/crates/tidb-exec/src/mview_build_engine.rs`, `rust/crates/tidb-exec/src/cluster_ddl.rs::plan_create_materialized_view_job_step` | `pkg/ddl/mview_worker.go` build/reorg lifecycle. Remove the seed evaluator as a live integration candidate until the owner is complete. |
| D10 | MV/MV-log actions maintain some base backreferences inline but lack the complete shared base-and-log dependent-ID lifecycle, affected-table publication and rollback fallback. **Seed only.** | `rust/crates/tidb-exec/src/cluster_ddl.rs:5011`, `:5712` | `pkg/ddl/mview_worker.go::updateMaterializedViewBaseInfoOnCreate`, `updateMaterializedViewBaseInfoOnDrop`: migrate create/drop/rollback together, including MLogTableIDs and DependentMViewIDs. |

The seven worker loops, action-owned history writers and 17 action-stage queue
writers removed in the previous commits are **not** open findings. Their
removal does not resolve D01–D10 or accept the whole Go DDL package.

## Transaction and routing ownership

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| T01 | Generic `BufferMutation::insert` chooses both presume-not-exists and AssertNotExist. The configured INSERT planner does not read a snapshot and its planning interface lacks transaction-mode policy. A lazy optimistic miss requires a different assertion from an eager/pessimistic insert. | `rust/crates/tidb-txnkv/src/transaction/mutation.rs:47`, `rust/crates/tidb-exec/src/real_tikv_dml.rs:402` | `pkg/table/tables` owns uniqueness checking and assertion selection; the KV buffer transports the flags. Review table, index and system-row callers together; blanket AssertUnknown is also incorrect. |
| T02 | TiDB and native client-rust both retain region/cache/recovery/RPC algorithms. `ClientPd` delegates routing back to TiDB, while DistSQL still needs those capabilities. This is confirmed competing ownership, **not by itself a proven runtime defect**. | `rust/crates/tidb-txnkv/src/driver/client_bridge.rs::ClientPd`, `rust/crates/tidb-txnkv/src/region`, `rust/third_party/tikv-client-rs/src/region_cache.rs` | Pinned client-go owns storage routing/recovery; TiDB coprocessor code consumes it. Move all consumers before deleting the TiDB owner. Native RetryBackoffer already owns the retry budget and must not be duplicated again. |
| T03 | Background request lifetime still partly depends on request type (TxnHeartBeat), rather than an explicit lifetime supplied by every operation owner. Pipelined and transaction-file cleanup remain part of the missing lifecycle. | `rust/crates/tidb-txnkv/src/driver/client_bridge.rs:569` and the native cleanup call sites recorded in the storage ExecPlan | Pinned client-go transaction/lock owners select the context. Carry lifetime explicitly; detaching every ResolveLock would break foreground cancellation. The previously rejected native patch remains unapplied; this audit does not retry it. |

The five earlier reviewer defects have their own regression receipts. Retry
limits, secondary retry history, locked-entry timestamps, mock normal wake-up
and detached read-resolution fixes are not reopened merely because T02/T03
remain. The native dependency has not changed in this audit.

## Domain and process services

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| O01 | Numeric server-ID allocation/lease renewal/loss recovery is absent. Server info defaults to zero and the connection allocator uses standalone ID 1. Independent nodes cannot rely on Go's cluster-wide identity guarantee. | `rust/crates/tidb-server/src/serverinfo_etcd.rs:303`, `rust/crates/tidb-server/src/sql_node.rs:50`, `:1499` | `pkg/domain/domain.go::acquireServerID`, `refreshServerIDTTL`, `globalconn` allocator. The already repaired MPP statement IDs must consume this shared identity. |
| O02 | Startup distinguishes only bootstrapped/not bootstrapped: any Bootstrapped version skips work. Partial bootstrap objects are refused. There is no versioned upgrade/recovery owner on this live boot path. | `rust/crates/tidb-server/src/cluster_session_node/boot.rs:89`, `rust/crates/tidb-exec/src/cluster_privilege_load.rs:334`, `rust/crates/tidb-exec/src/mysql_bootstrap.rs:444` | `pkg/session/session.go::runInBootstrapSession` chooses Bootstrap/Upgrade/Normal under the owner/version checks; `pkg/session/bootstrap.go` and upgrade files execute the required steps. Fresh empty-store tests do not validate upgrades. |
| O03 | A native GC client and metric families exist, but the Rust server does not start TiDB's store GC worker. Safe-point reads and region-cache eviction are not cluster GC leadership, scheduling and delete-range execution. | `rust/crates/tidb-server/src/cluster_session_node/boot.rs`, `rust/crates/tidb-txnkv/src/kv_api.rs:587` (trait declaration), `rust/crates/tidb-txnkv/src/client_go_metrics.rs` | `pkg/store/driver/tikv_driver.go::StartGCWorker`, `pkg/store/gcworker`, session startup. A Rust-only deployment lacks this server responsibility; a Go peer may mask it. |
| O04 | TTL metadata, variables and SQL/session/cache helpers exist, but no TTL job/task manager is composed into the server. TTL configuration therefore does not establish automatic expiry work. | `rust/crates/tidb-ttl/src/lib.rs:17`, `rust/crates/tidb-server/src/cluster_session_node/boot.rs`; worker implementation absent from the Rust source inventory | `pkg/domain/domain.go::StartTTLJobManager`, `pkg/ttl/ttlworker`, including current external-workload role gating and owned shutdown. Do not remove useful helpers; implement their missing owner. |
| O05 | Native resource-control callbacks exist but TiDB never installs a controller. Rust sysvars publish flags only. Resource-group names on requests do not implement PD token admission, settlement or runaway policy. | `rust/third_party/tikv-client-rs/src/resource_control.rs:323`, `:345`, `rust/crates/tidb-session/src/vars.rs:1315`; no Rust-crate caller of `set_resource_control_interceptor` | `pkg/domain/runaway.go::initResourceGroupsController` constructs/starts the PD controller and runaway manager, then calls `tikv.SetResourceControlInterceptor`. Preserve native hooks and wire the actual owner. |
| O06 | RU statistics helpers have no production writer-loop caller, owner gating or scheduled shutdown in the server. The existence of RuStatsWriter alone does not populate/expire the history. | `rust/crates/tidb-domain/src/ru_stats.rs:34`, `:442`, `rust/crates/tidb-server/src/cluster_session_node/boot.rs` | `pkg/domain/ru_stats.go::requestUnitsWriterLoop`, `pkg/domain/domain.go` startup. Shares O05's controller dependency but has a separate persistence/maintenance responsibility. |

Absence claims above are bounded to this checked-in Rust server and vendor
inventory, not claims that the concepts are missing from every crate or from
an external Go node. The domain crate's old introductory comments overstate
which leaf services remain absent; current boot code was inspected rather than
copying that old list into this report.

## TiFlash and MPP

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| F01 | Classic placement creation/repair/deletion and acceleration remain owned by each poll pass. Logical-table-only stale detection does not cover physical partitions/keyspaces. | `rust/crates/tidb-exec/src/tiflash_replica_manager.rs:167` | `pkg/ddl/tiflash_replica.go`, infosync and GC: create rules before metadata publication; GC handles retired tables. Periodic rule refresh is a NextGen path in Go. Migrate all DDL/partition/drop consumers before deleting reconciliation. |
| F02 | SetTiFlashReplica rebuilds unavailable metadata instead of preserving/resetting availability under Go's explicit policy; status/polling operate on logical IDs rather than the full physical partition lifecycle. | `rust/crates/tidb-exec/src/cluster_ddl.rs` SetTiFlashReplica/UpdateTiFlashReplicaStatus branches and the poller above | Go onSetTableFlashReplica, ResetAvailable and partition availability updates. Requires F01's complete DDL integration. |
| F03 | Polling lacks the shared progress cache, unavailable-table backoff and periodic PD HTTP store-discovery policy. It rediscovers/filter Up stores through gRPC each tick. | `rust/crates/tidb-exec/src/tiflash_replica_manager.rs:167`, `:318` | `pkg/ddl/tiflash_replica.go::refreshTiFlashTicker`, `pkg/domain/infosync` HTTP/security owner, including Down/Disconnected status handling. |
| M01 | MPP lowering is a separately built scan-only tree, with one selected Up TiFlash store and one task. Selection, TopN and aggregation are refused there. It does not consume Go's complete physical fragment/task graph or replica/balancing policy. | `rust/crates/tidb-exec/src/tiflash_mpp_scan.rs:94`, `:152`, `:219` | `pkg/planner/core` MPP fragment/task generation, `pkg/executor/internal/mpp/local_mpp_coordinator.go`, `pkg/store/copr/mpp.go::ConstructMPPTasks`. This constrains TPCH pushdown and parallelism; no benchmark measurement is claimed. |
| M02 | Range construction reduces all requested ranges to min(start)/max(end), issues one limited 10,000-region query and dispatches the envelope. Gaps between requested ranges are lost; continuation/completeness is not checked. | `rust/crates/tidb-exec/src/tiflash_mpp_scan.rs:117`, `:341` | `pkg/store/copr/batch_coprocessor.go::buildBatchCopTasksCore` splits and preserves each range with the shared region cache. Contract mismatch is confirmed; an end-to-end disjoint-range SQL reproduction has not run. |
| M03 | `open_mpp` drains the complete network stream into VecDeque before returning a row stream. Close only clears local packets. There is no coordinator memory accounting, statement cancellation while draining or remote cancel owner. | `rust/crates/tidb-exec/src/tiflash_mpp_scan.rs:378`, `:423`, `:495`, `:568` | Go local coordinator `sendToRespCh`, `receiveResults`, `Close`, `cancelMppTasks`: incremental delivery, tracker charging, finish signal, joined cancellation and remote task cancellation. First-row latency and memory scale with the entire Rust response. |
| M04 | MPP constructs a new plaintext client directly and handles RPC errors outside the shared client recovery policy; retry_regions are logged without cache invalidation. | `rust/crates/tidb-exec/src/tiflash_mpp_scan.rs:386`, `:405` | `pkg/store/copr/mpp.go::DispatchMPPTask`/`EstablishMPPConns` use the TiKV client security/context/backoff and invalidate stale regions. Reuse the storage transport/security owner; do not add another MPP-only retry/cache implementation. |

## Remaining protocol and PD owners

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| P01 | PD KeyspaceScope projects a oneof into a plain u32. Explicit keyspace zero loses its presence. It also omits keyspace_identity; GetGCState lacks newer global-barrier fields and GetStoreResponse lacks stats. | `rust/crates/tidb-proto/proto/pdpb.proto:19`, `rust/crates/tidb-pd-client/src/client/requests.rs:317` | Master's complete `kvproto/proto/pdpb.proto`. **Wire reproduction:** `keyspace_id: 0` encodes to empty local bytes versus upstream `08 00`. Replace the package owner and migrate consumers, rather than patching another enum/field by hand. |
| P02 | Four remaining projected protobuf packages omit 400 declarations/fields/RPCs in total; generation/checking only the selected surface cannot establish package completeness. The fifth inspected package, mvccpb, has no declaration/field differences in this comparison. | `rust/crates/tidb-proto/build.rs:37`, five local non-TiPB inputs; complete list in `protocol-projections.json` | Complete pinned PD, TiKV, BR and etcd inputs and generated owners. This is a declaration inventory; omitted unused RPCs are not automatically runtime failures. TiKV's 71 opaque message representations are deliberately listed separately and must retain their transport optimization or demonstrate an equivalent replacement. |
| P03 | The local PD client only follows PD member leadership and its PD Tso stream. It has no service-mode discovery/switching or independent TSO-service owner. | `rust/crates/tidb-pd-client/src/client/topology.rs:36`, `rust/crates/tidb-pd-client/src/tso.rs:207`, local PD service projection | Pinned `pd/client/servicediscovery/service_discovery.go::checkServiceModeChanged`, `tso_service_discovery.go` and `client.go`: discover PD/API mode, TSO URLs and fallback policy. Classic-PD success does not certify microservice mode. |

P01's oneof mismatch is additional to the 400 omissions. The declaration audit
covers all messages, nested messages, enums, fields and service methods in the
five compared packages, matching message fields by wire tag rather than by
case-sensitive spelling. Its 472 records comprise 400 omissions, one PD
contract mismatch and 71 TiKV opaque representations. It does not compare all
generator options/reserved ranges/runtime behavior or accept external packages.

| Package | Missing messages | Missing enums | Missing fields | Missing services | Missing methods within retained services | Other |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| pdpb | 127 | 7 | 4 | 0 | 45 | KeyspaceScope oneof mismatch |
| tikvpb | 3 | 0 | 0 | 1 | 66 | 71 message-to-opaque-bytes representations |
| backup (brpb) | 48 | 6 | 0 | 2 | 0 | Retained fields match the compared contracts |
| etcdserverpb | 78 | 5 | 1 | 3 | 4 | WatchRequest.progress_request is missing |
| mvccpb | 0 | 0 | 0 | 0 | 0 | No compared declaration differences |

Fields and RPCs of entirely missing messages/services are not counted again.
Complete before-images and input hashes are in `protocol-projections.json`.

## Review candidates, not yet established defects

| Candidate | Why it needs review before removal |
| --- | --- |
| Alternate lightweight storage-session dispatcher | Its SET parser is already shared. Complete transaction/autocommit lifecycle parity with the ordinary session owner is not yet established. Locate every configured/test caller before removal. |
| Multi-table DML's matrix interpreter and target identity | `tidb-executor/src/driver/multi_dml.rs:1262` retains a matrix-backed path, `:1337` passes FkPlanSpec::default(), and `:1351` reconstructs target widths/handles from catalog metadata. These differ from Go's plan-owned table/handle positions. Per-row FK enforcement exists; default plan metadata alone does not prove missing enforcement. |
| Multi-action in-memory ALTER atomicity | `tidb-executor/src/ddl/alter_table.rs:58` stages a catalog clone only for selected FK-containing action lists. Trace outer rollback owners and compare Go's complete multi-schema lifecycle before claiming a partial-commit defect or deleting the interpreter. |
| Error identity and required system-table errors | Fresh cancellation codes/history propagation were repaired. `record_ddl_plan_error` still maps other errors to 1105; Go error classes/identity and each required table failure need original-test comparison. A textual difference alone is insufficient. |
| Partition/catalog/statistics integration | Existing failures may share a catalog/publication cause. The newly rerun system-table test fails because p1 is missing after ADD PARTITION, not because a system event was emitted. Current source already filters system-schema events. Do not implement another notifier exclusion to hide this failure. |
| Remaining parser/planner/expression/executor behavior | Current candidate inventory and historical package receipts require source/test reconciliation. An unsupported branch or a different Rust representation is not automatically a mismatch. |
| Remaining client-go, PD, etcd, BR and other external packages | Inventoried module artifacts do not imply complete original-test/variant/integration review. PD/etcd now have complete module artifact inventories, but original-test/variant integration remains unreviewed. Dependencies beyond the recorded modules also remain acceptance work. |

## Validation and residual scope

No production SQL/storage behavior was edited. The audit script and generated
inventory are review artifacts. Source-reviewed ownership findings do not
replace concurrency, failure-injection, real TiKV/TiFlash, TLS, GC, upgrade,
TTL or workload validation.

Exact commands from the repository root:

    git pull --ff-only origin hparser-integration
    git fetch origin master
    python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master
    python3 rust/scripts/audit-protocol-projections.py --go-ref origin/master

The second Python command compiles the five local projections and complete
upstream schemas with protoc, compares their descriptors, and records the
keyspace-zero wire example. It requires protoc and Go module-cache access.
It only writes `protocol-projections.json`, never generated Rust or schemas.

Existing regression rerun from rust/:

    cargo test --locked -p tidb-server --lib system_table_ddl_does_not_publish_statistics_events_like_go

Result: **failed**, 0 passed / 1 failed, at
`cluster_session_node/tests/unistore_cop.rs:4049` (`p1 exists`). Local log:
`/private/tmp/tidb-all-audit-system-ddl.log`. This refines the earlier ten-failure
baseline; the other nine tests were not rerun in this audit. The complete list
remains in README.md. No new SQL regression or workload benchmark was run.

Publication checks and their final outcomes are recorded in the parent ExecPlan.
Prior successful package repairs remain valid at their recorded revisions;
none are promoted to repository-wide parity by this audit.
