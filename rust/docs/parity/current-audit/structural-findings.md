# Remaining structural mismatches, 2026-09-30

Latest follow-up compared integration `13689e0b133b32af45862f5a69ee99382471fc12` with freshly fetched
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
Their current-master package acceptance remains unreviewed. The 2,047 search
candidate lines are not a defect count. Historical receipts and ignored-test
comments can be stale; they are not substituted for current source review.

Paths below are relative to the repository root. Go function references refer
to the master above, not this branch's Go working tree. Unless marked as a
reproduction, findings are source comparisons and their stated consequences
are inferences; no live distributed failure or benchmark is claimed.

The register contains **54 tracked findings: 50 unresolved (including E02's
partial repair) and four repaired ownership/contract findings (C01, E01,
P01, P02)**. This is not a count of accepted packages. E02 has a runtime
repair with plan integration still open; see [the shared UPDATE repair receipt](shared-update-owner-repair.md).
The latest 13 additions are A01–A04, B01–B02, E05, N01–N03 and O07–O09;
their source comparisons, six groups of executable observations and controls
are in [the expanded ownership review](expanded-ownership-review.md).
The prior 12 additions were D11, C01–C02, E01–E04, S01–S02 and I01–I03. Five of those groups have SQL
reproductions in [the session/executor review](session-ownership-review.md):
D11, C01, E01, E02 and I01. The other seven are source-confirmed design or
integration differences with unmeasured runtime consequences.

## Privilege and account policy owners

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| A01 | **P1; reproduced:** privilege requests are collected from unresolved AST table names. A SELECT-only user is denied single/qualified UPDATE but successfully executes joined `SET x=13`. A registry column grant also cannot authorize `SELECT x` because the shared check demands a table grant. | `rust/crates/tidb-session/src/table_privilege.rs:267`, `:344`; `identity.rs:326`, `:340`; diagnostic output in `expanded-ownership-probe.txt` | Master `pkg/planner/core/logical_plan_builder.go` appends UPDATE privileges after resolving assignment columns; `optimizer.go::CheckPrivilege` and `pkg/privilege/privileges/cache.go::RequestVerification` consume resolved table/column requests. Migrate privilege compilation with name resolution, including prepared and multi-DML callers; do not add another AST qualifier guesser. |
| A02 | **P1; source-confirmed:** the durable account image omits `mysql.global_priv`, password-change time/lifetime, failed-login policy/state and other user attributes. Rebuilding the registry supplies `SslType::None`, a fresh password timestamp and no lock policy. The live authentication code therefore cannot enforce the policies loaded from a Go peer. Empty usernames are also discarded. | `rust/crates/tidb-exec/src/cluster_privilege_load.rs:120`, `:191`, `:294`, `:428`; `rust/crates/tidb-server/src/cluster_privileges.rs:88`, `:381`, `:702`; `rust/crates/tidb-session/src/privilege.rs:262` | `pkg/privilege/privileges/cache.go::LoadAll`, `decodeUserTableRow`, `decodeGlobalPrivTableRow`, and `privileges.go::ConnectionVerification`/`checkSSL`. Carry the complete policy image through read, publication and writeback; preserve expiry/lock epochs. Policy loss is source-confirmed; no live mixed-node authentication exploit was run. |
| A03 | **P2; partially reproduced:** MySQL TLS always uses `with_no_client_auth`; authentication receives only a TLS boolean. X509/specified requirements are refused, configured certificates are loaded once, and CA/min-version/reload policy has no corresponding transport lifecycle. | `rust/crates/tidb-server/src/mysql_tls.rs:118`, `:161`; `configured_user_store.rs:479`; `rust/crates/tidb-session/src/privilege/privs.rs:127`. `CREATE USER … REQUIRE X509` refusal reproduced. | `pkg/util/misc.go::LoadTLSCertificates` loads CA/client verification, minimum TLS version and reloadable certificates; `pkg/privilege/privileges/privileges.go::checkSSL` checks verified chains/cipher/issuer/subject/SAN. Preserve that state through the TLS/account boundary. This is separate from A02 losing stored policies. |
| A04 | **P1; reproduced:** account SQL accepts PASSWORD HISTORY and REUSE options as no-ops. Creating history=3, changing the password, then reusing the first password succeeds; `password_reuse_history` remains NULL. | `rust/crates/tidb-session/src/account.rs:145`; `expanded-ownership-probe.txt` | `pkg/executor/simple.go::whetherSavePasswordHistory`, `checkPasswordHistoryRule`, `checkPasswordReusePolicy` persist and check `mysql.password_history` and user policy. Move full account policy validation and storage together; parsing or SHOW output alone is insufficient. |

## Binding cache and maintenance

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| B01 | **P2; source-confirmed:** live BindingCache uses an insertion-order CostLruStore; reads never influence admission/eviction. Every fitting insertion is admitted. Go's Ristretto cache has frequency admission and asynchronous Set/Wait. This can retain different hints under pressure, affecting chosen plans. | `rust/crates/tidb-session/src/binding_cache.rs:283`, `:313`, `:317`, `:413`; cluster binding image publication | `pkg/bindinfo/binding_cache.go::newBindingCache`, `GetBinding`, `SetBinding`. Preserve the complete admission, access, replacement, accounting and publication contract. No particular probabilistic Go victim or measured workload slowdown is claimed. |
| B02 | **P2; source-confirmed:** the binding worker rebuilds the full table image every lease; it has no incremental watermark, binding owner GC timer or usage persistence loop. Tombstone GC instead runs on subsequent global-binding writes; usage helper code has no production caller. | `rust/crates/tidb-server/src/cluster_binding_seam.rs:76`, `:169`; `rust/crates/tidb-session/src/binding_arm.rs:684`; `binding_utils.rs:459` | `pkg/bindinfo/binding_cache.go::LoadFromStorageToCache`, `UpdateBindingUsageInfoToStorage`, `pkg/domain/domain.go::globalBindHandleWorkerLoop`. Keep the existing independent global-binding writer; migrate refresh/GC/usage scheduling to the shared owner before deleting write-triggered GC. |

## Wire and server admission/configuration

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| N01 | **P1; reproduced:** SQL ingress forces a Rust UTF-8 String and converts Latin-1 bytes into Unicode characters. Raw COM_QUERY `SELECT HEX('<E9>')` returns `C3A9`; Go's Latin-1 compatibility encoding preserves `E9`. Text rendering alone hides byte/length/storage differences. | `rust/crates/tidb-server/src/mysql_connection.rs:86`, `:1702`, `:2140`; `expanded-server-probe.txt` | `pkg/parser/charset/encoding_latin1.go::Transform` and the parser/server input boundary. Preserve source bytes/charset through lexing, literals, datum storage and result conversion; a HEX-specific patch would not fix the contract. |
| N02 | **P2; source-confirmed:** the main flag accepts token-limit and its gauge exists, but command dispatch has no shared command permit owner. Connection-count admission is implemented and is a different limit. | `rust/crates/tidb-server/src/main_flags.rs:281`, `lib.rs:420`, `mysql_connection.rs` command loop, `sql_node.rs:1904`, `:2090`; token-limit flag admission reproduced | `pkg/server/server.go::concurrentLimiter`/`getToken`/`releaseToken`, `pkg/server/conn.go` dispatch acquires and releases a token per command. Wire the existing configured limit to command lifetime. Concurrent workload effects were not measured. |
| N03 | **P2; reproduced:** executable configuration passes a complete SourceConfig through a second hand-maintained NodeConfig whitelist. Valid Go TOML such as token-limit, performance.stats-lease and security.ssl-ca is refused. NodeConfig also overrides Go's false auto-TLS default with true. | `rust/crates/tidb-server/src/bin/tidb-server.rs:60`; `node_config.rs:399`, `:441`, `:920`; `expanded-server-probe.txt` | `cmd/tidb-server/main.go` config/override/startup lifecycle and `pkg/config`. Use the complete validated configuration as the runtime authority, including CLI precedence and consumers; merely accepting ignored fields would preserve the gap. Removed performance.run-auto-analyze is correctly refused and is not a finding. |

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
| D11 | The in-process ALTER dispatcher stages only selected multi-action statements containing foreign keys. **Reproduced:** `ADD COLUMN added INT, ADD COLUMN id INT` errors on existing `id` but leaves `added` installed. | `rust/crates/tidb-executor/src/ddl/alter_table.rs:58`; ordinary Session::run probe | `pkg/ddl/executor.go::alterTable` collects all subjobs before submission; `multi_schema_change.go::onMultiSchemaChange` owns combined transitions and rollback. Atomicity must cover every action list. The reproduction is in-process, not proof of a partial cluster metadata commit. |

The seven worker loops, action-owned history writers and 17 action-stage queue
writers removed in the previous commits are **not** open findings. Their
removal does not resolve D01–D11 or accept the whole Go DDL package.

## Session plan-cache ownership

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| C01 | **Ownership repaired:** one session physical-entry LRU now serves prepared/non-prepared SELECT/DML, with shared capacity, recency, pressure, close and flush. Separate per-statement vectors and the DML-only metadata LRU are removed. See the [repair receipt](shared-session-plan-cache-repair.md) for regressions and limits; this is not whole-package acceptance. | `rust/crates/tidb-executor/src/driver/plan_cache.rs`, `rust/crates/tidb-planner/src/plan_cache_lru.rs`, `rust/crates/tidb-session/src/session_plan_cache.rs`; server factory and COM_STMT_CLOSE callers | `pkg/session/session.go::GetSessionPlanCache`, `pkg/planner/core/plan_cache_lru.go`, `pkg/executor/simple.go::executeAdminFlushPlanCache` and `prepared.go::DeallocateExec.Next`. C02 remains open. |
| C02 | Instance-plan-cache variables are registered, but no domain-scoped shared cache or instance hit/clone/eviction path is composed into production. Enabling the flag cannot select Go's instance owner. | `rust/crates/tidb-session/src/sysvar/catalog/optimizer.rs:256`; C01's owners; production reference audit in the review receipt | `pkg/planner/core/plan_cache.go::lookupPlanCache`/`clonePlanForInstancePlanCache`, `plan_cache_instance.go`, Domain: shared ownership plus per-execution cloning. Reuse admission/key/rebuild contracts; another ad hoc global map is insufficient. |

## DML and executor handoff

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| E01 | **Repaired in the shared UPDATE owner follow-up (historical evidence below).** Multi-update writes each alias's full row from the original join output, lacking shared merge state for aliases of one physical row. **Reproduced:** `a.x=11,b.y=21` through two aliases of `(1,10,20)` succeeds but leaves `(1,10,21)`. | `rust/crates/tidb-executor/src/driver/multi_dml.rs:897`, `:918`, `:1066` | `pkg/executor/update.go::mergeNonGenerated`, `mergeGenerated`, `updateRows`: merge by table ID/handle while retaining per-target-position changed-row rules. Changing only the deduplication key would lose legitimate assignments. |
| E02 | **Runtime bypass repaired; FK plan integration remains open.** Multi-update branches before the single-table FK checks/cascades and directly writes KvTable; its root also receives empty FK metadata. **Reproduced:** single-table `pid=999` raises an FK error, while joined UPDATE stores the orphan. | `rust/crates/tidb-executor/src/driver/dml.rs:2954`; `driver/multi_dml.rs:828`, `:1066`, `:1337`; contrast `driver/dml.rs:3434` | Go `buildUpdate` and `pkg/executor/update.go::exec` pass per-table FK plans into shared `updateRecord`. Move the complete UPDATE policy into that shared owner. Multi-DELETE already enforces referred checks/cascades; its control probe fails correctly. |
| E03 | The DML-source helper drains the entire physical read into datum vectors before writes; multi-DML then reconstructs target layouts/handles from catalog metadata. Multi-DML charges joined rows after collection; the collection vector is not charged during growth. Matrix sources retain a separate join/filter interpreter. | `rust/crates/tidb-executor/src/driver/physical_builder.rs:5514`, `rust/crates/tidb-executor/src/driver.rs:960`, `driver/multi_dml.rs:814`, `:1265`, `:1351` | Go's planner retains finalized TblColPosInfos/handle columns; `updateRows` consumes and charges chunks. Multi-DELETE also buffers, but deduplicates into tblRowMap as chunks arrive. Migrate the complete handoff. A USING-layout corruption is not established: that probe passes. |
| E04 | Physical Apply retains concurrency/keep-order metadata, but its builder always constructs serial NestedLoopApplyExec. The parallel-apply variable has no production execution consumer. | `rust/crates/tidb-planner/src/physical/mod.rs:1986`, `rust/crates/tidb-executor/src/driver/physical_builder.rs:3356`, `:3432`; optimizer sysvar catalog | `pkg/executor/builder.go::buildApply` clones eligible inner plans for ParallelNestedLoopApplyExec; `parallel_apply.go` owns ordered workers, cache, errors and cleanup. Rust needs worker-safe contexts and Go's serial fallback. Serial result tests do not validate this path; performance is unmeasured. |
| E05 | **P1; reproduced:** IMPORT INTO runs a private local-CSV parser and nested SQL INSERT loop, ignoring parsed import options and assignments. With `skip_rows=1`, a two-row file imports both rows. It has no import-job/task submit, status, detached/cancel/recovery owner and returns affected rows instead of file-import job info. | `rust/crates/tidb-session/src/dispatch.rs:2586`, `:3169`; `expanded-ownership-probe.txt` | `pkg/executor/import_into.go::ImportIntoExec.Next`/`submitTask`, `pkg/executor/importer` controller and `pkg/dxf/importinto` (including standalone tasks). Replace the complete import lifecycle and option/encoding ownership; SELECT import has its own Go path and is not used to infer file-import result shape. |

## Additional configured-server pipeline

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| S01 | Optional non-cluster-session mode selects a separate session/optimizer by configured table count. ASTs lower to ReadOnlyScanPlan/ConfiguredOrderedJoinPlan and a separate write planner, bypassing ordinary physical planning. It imposes one/two-table admission and refuses autocommit locking reads. | `rust/crates/tidb-server/src/lib.rs:291`, `:338`; `real_tikv_multi_node.rs:308`, `:575`; `rust/crates/tidb-exec/src/real_tikv_dml.rs:1940` | Go session ExecuteStmt, planner Optimize, executor compiler and TxnManager are shared across stores. Move these selectable entrypoints and consumers to regular session/storage adapters before deleting their configured planner/dispatcher family. This does not describe the default cluster-session path. |
| S02 | With two loaded tables in that mode, startup discards the catalog snapshot and retains static descriptors; only the one-table branch installs schema/stats reloaders. Peer DDL can leave the two-table route stale. | `rust/crates/tidb-server/src/real_tikv_node/mod.rs:1940`, `:1966`; RealTiKvMultiSessionFactory | Go Domain/InfoSchema synchronization and ordinary session schema/transaction lifecycle. Fold consumers into that owner with S01; adding another table-count-specific reloader preserves duplication. Peer-DDL reproduction not run. |

## Runtime information-schema providers

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| I01 | Dynamic virtual tables use captured fixtures, constant empty/default rows or unconditional captured errors. **Reproduced:** an existing sequence is absent from SEQUENCES; CLUSTER_CONFIG reports `127.0.0.1:15100` and a synthetic store1 warning without discovery; CLUSTER_LOG reports missing start time even with start/end/pattern predicates. Other affected providers are enumerated in the review receipt. | `rust/crates/tidb-session/src/dispatch.rs:604`, `:706`; `infoschema.rs:335`, `:2893`, `:2925`; `cluster_config_rows.rs:1` | Go typed config/log, sequence/resource/watch, inspection and TiFlash system-table retrievers own runtime rows, predicate extraction, privileges, errors and lifetime. Replace the fixture providers. Static column definitions/charset/metrics descriptions are not inherently wrong. |
| I02 | CLUSTER_INFO reads only TiDB records, turns discovery errors into empty results and unconditionally appends mock `tikv/store1` after a successful syncer read. It lacks the other real service/store retrievers. | `rust/crates/tidb-session/src/lib.rs:1425`, `:1483` | `pkg/infoschema/tables.go::GetClusterServerInfo` composes seven retrievers and propagates errors; `dataForTiDBClusterInfo` renders output. Keep mock facts in the mock provider. Discovery/fabricated rows differ from O01's numeric server-ID leasing. |
| I03 | CLUSTER_PROCESSLIST decorates only this process's rows with its address. Other listed cluster tables lack remote retrieval; table-name presence does not provide fanout. | `rust/crates/tidb-session/src/dispatch.rs:670`, `infoschema.rs:1034`; existing CLUSTER_STATEMENTS_SUMMARY_HISTORY test expects refusal | `pkg/infoschema/cluster.go`, `pkg/executor/table_reader.go`, `pkg/store/copr/coprocessor.go::buildTiDBMemCopTasks`: discover and dispatch to peers or DDL owner, with cancellation and memory tracking. Multi-node fanout not exercised. |

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
| O07 | **P2; source-confirmed:** statistics GC has a callable implementation and direct tests, but no production caller starts its periodic lifecycle. The existing analyze-job cleanup worker only removes job history; it does not run table/column statistics GC. | `rust/crates/tidb-server/src/cluster_session_node/mod.rs:1438`, `:2059`, `:2587`; boot.rs startup list; all other `gc_stats` callers are tests | `pkg/domain/domain.go::LoadAndUpdateStatsLoop`/`gcStatsWorker`: owner-gated GC, auto-analyze window checks, cache memory eviction and health updates. Keep existing workers; compose the missing lifecycle and shutdown. No long-running retention experiment was run. |
| O08 | **P2; source-confirmed:** plan-replayer collectors, dump workers and file-GC helpers exist, but the server constructs none of these owners and has no producer-to-dumper integration. Capture variables alone cannot produce replay archives. | `rust/crates/tidb-domain/src/plan_replayer.rs:802`, `:901`, `:1097`; no production server references; session stmt_ctx.rs only propagates capture flags | `pkg/domain/domain.go::StartPlanReplayerHandle`, `DumpFileGcCheckerLoop`, session capture submission and executor dump/load owners. Integrate the complete task/status/archive lifetime; do not substitute a token string for an archive. |
| O09 | **P1; source-confirmed:** server-info publication does not report the node's minimum active start timestamp to the cluster. Session, cursor, internal-transaction and recent schema timestamps never reach the shared reporting owner. | `rust/crates/tidb-domain/src/serverinfo_syncer.rs:40`, `:629`; Rust production/vendor reference search and boot wiring | `pkg/domain/infosync/info.go::ReportMinStartTS`/`storeMinStartTS` and server-info synchronization. Restore reporting under the leased owner, including shutdown and transaction/cursor lifetimes. This is independent of O03's missing GC worker: a Go peer's GC also needs Rust-node reporting. Actual premature collection was not reproduced. |

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

## Protocol and PD owners

| ID | Confirmed difference and impact | Rust evidence | Go owner and replacement boundary |
| --- | --- | --- | --- |
| P01 | **Repaired:** PD scope now shares the complete native oneof. Explicit zero, keyspace identity, global-barrier fields and store stats survive encoding. | `rust/crates/tidb-proto/src/lib.rs`, `rust/crates/tidb-pd-client/src/client/requests.rs` | Master's complete native `pdpb` owner; raw-wire regressions failed before removal. The PD mock-RPC test also distinguishes no scope from scope zero. See `complete-protocol-owner-repair.md`. |
| P02 | **Schema ownership repaired:** all five handwritten projections are removed; the compared 400 omissions are gone. | Native PD/BR re-exports; complete descriptor-derived TiKV service; all pinned etcd API inputs and source gate. | `protocol-contracts-after.json` compares seven protobuf packages with zero omissions/contract differences. All 71 opaque TiKV fields remain separate intentional representations, with presence and zero-copy tests. This does not accept external Go helpers or runtime behavior as complete packages. |
| P03 | The local PD client only follows PD member leadership and its PD Tso stream. It has no service-mode discovery/switching or independent TSO-service owner. | `rust/crates/tidb-pd-client/src/client/topology.rs:36`, `rust/crates/tidb-pd-client/src/tso.rs:207`, complete PD contract does not supply discovery | Pinned `pd/client/servicediscovery/service_discovery.go::checkServiceModeChanged`, `tso_service_discovery.go` and `client.go`: discover PD/API mode, TSO URLs and fallback policy. Classic-PD success does not certify microservice mode. |

The historical P01 oneof mismatch was additional to the 400 omissions. The historical declaration audit
covered all messages, nested messages, enums, fields and service methods in the
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
| Remaining configured-session transaction cases | S01/S02 establish the alternate owners and missing two-table refresh. Full transaction/autocommit semantics and all configured/test callers still need review before deletion. |
| DML identity edge cases | E01/E02 establish alias merging and multi-update FK failures; E03 records the materialized handoff. The USING probe passes. Derived/outer joins, pruning and partitioned handles still need a complete plan-schema comparison; do not infer failure from a different data representation alone. |
| Cluster multi-action ALTER failure paths | D11 reproduces leakage in the in-process dispatcher. Cluster persisted metadata/data rollback is a separate path requiring failure injection before any partial-commit claim. |
| Error identity and required system-table errors | Fresh cancellation codes/history propagation were repaired. `record_ddl_plan_error` still maps other errors to 1105; Go error classes/identity and each required table failure need original-test comparison. A textual difference alone is insufficient. |
| Partition/catalog/statistics integration | Existing failures may share a catalog/publication cause. The newly rerun system-table test fails because p1 is missing after ADD PARTITION, not because a system event was emitted. Current source already filters system-schema events. Do not implement another notifier exclusion to hide this failure. |
| Remaining parser/planner/expression/executor behavior | Current candidate inventory and historical package receipts require source/test reconciliation. An unsupported branch or a different Rust representation is not automatically a mismatch. |
| Remaining client-go, PD, etcd, BR and other external packages | Inventoried module artifacts do not imply complete original-test/variant/integration review. PD/etcd now have complete module artifact inventories, but original-test/variant integration remains unreviewed. Dependencies beyond the recorded modules also remain acceptance work. |

## Prior protocol/inventory checkpoint and residual scope

The commands below describe the earlier integration `9f0a41b5db` checkpoint.
The latest SQL probes and reviewed call paths are in
[session-ownership-review.md](session-ownership-review.md). Source inventories
and protocol receipts retain their own revisions; this follow-up does not
silently refresh or accept them.

No production SQL/storage behavior was edited. The audit script and generated
inventory are review artifacts. Source-reviewed ownership findings do not
replace concurrency, failure-injection, real TiKV/TiFlash, TLS, GC, upgrade,
TTL or workload validation.

Exact commands from the repository root:

    git pull --ff-only origin hparser-integration
    git fetch origin master
    python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master
    python3 rust/scripts/audit-protocol-projections.py --go-ref origin/master

At that checkpoint the second Python command compared the five local
projections and wrote `protocol-projections.json`. It now builds the complete
Rust owners and compares their generated descriptors with upstream, writing
`protocol-contracts-after.json`. The before-image is retained. It requires
Cargo, protoc and Go module-cache access.

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
