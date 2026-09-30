# Session, executor and runtime-provider review

This audit compares integration `960fa95b48e5b374648881404cac242551786de2`
against freshly fetched Go master `e953a09d9d5e29e60c62f42d3aacebb819af49a5`.
`git pull --ff-only origin hparser-integration` was already current. No native
client inputs, dependency pins, generated sources or production behavior change.

The 12 additions to [the finding register](structural-findings.md) are D11,
C01–C02, E01–E04, S01–S02 and I01–I03. This is source review of selected live
paths, with five groups reproduced through the ordinary in-process Session
API. It is not acceptance of pkg/session, pkg/executor, pkg/planner/core,
pkg/ddl, pkg/infoschema, pkg/domain or pkg/store/copr as whole packages.

## Reproducible SQL evidence

[session-ownership-probe.rs](session-ownership-probe.rs) prints each SQL command
and its actual result. It contains no assertion that the current results are
correct. [session-ownership-probe.txt](session-ownership-probe.txt) retains the
captured stdout; compiler diagnostics remain in the local log
`/private/tmp/tidb-session-ownership-probe.log`. The program exits successfully
even when a SQL command returns an error, so its exit code is not a parity gate.

Run from the repository root, using the existing crate's dependencies/lockfile.
The temporary example must not already exist. It is removed after the run;
the durable source is this audit's probe, not a new production API/test target.

```sh
test ! -e rust/crates/tidb-session/examples/audit_session_ownership.rs
mkdir -p rust/crates/tidb-session/examples
cp rust/docs/parity/current-audit/session-ownership-probe.rs rust/crates/tidb-session/examples/audit_session_ownership.rs
(cd rust && cargo run --locked -p tidb-session --example audit_session_ownership)
rm rust/crates/tidb-session/examples/audit_session_ownership.rs
```

Expected Go behavior below comes from the pinned source owners, not a new
execution of a Go server. Values are decoded from the retained Debug output.
All probes use fresh local tables, with no external cluster or persisted user
data. The in-process ALTER finding must not be generalized to cluster commit
atomicity without another reproduction.

| Finding | Probe | Observed Rust outcome | Go contract and root cause |
| --- | --- | --- | --- |
| E01 | Two aliases of `(id=1,x=10,y=20)` set `a.x=11,b.y=21` | Success; row is `(1,10,21)` | `update.go::mergeNonGenerated` combines writes by table ID/handle so the unrelated assignment survives. Rust writes each complete alias row from old join values. Keep the per-position update-once policy as well as the shared merge state. |
| E02 | Set FK child pid to absent 999, first singly then with a join | Single UPDATE returns ForeignKeyNoReferencedRow; joined UPDATE stores `(1,999)` | Go `UpdateExec.exec` calls updateRecord with table-keyed fkChecks/fkCascades. Rust's multi-update branch returns before the single-table check/cascade code and never calls it. Empty EXPLAIN metadata alone was insufficient evidence; this call trace and data reproduction establish the defect. |
| D11 | Add new `added` then duplicate `id` in one ALTER | DuplicateColumnName error; SHOW COLUMNS still contains `id` and `added` | Go alterTable collects specs into MultiSchemaInfo before one submission; validation/rollback belongs to the combined job. Rust only clones selected FK-containing multi-actions; ordinary actions mutate the current catalog incrementally. |
| C01 | Set session cache size 1; execute prepared A, B, A | Cache hit flags `0,0,1` | One Go session LRU evicts A on B's insertion, so the third lookup must miss. Rust retains A and B in separate prepared-definition vectors. The non-prepared statement LRUs are not a replacement for a physical-entry LRU. |
| C01 | Flush the enabled prepared cache, then execute A | ADMIN FlushPlanCache unsupported; A remains a hit | Go executeAdminFlushPlanCache clears the session owner and records instance invalidation time when requested. The missing shared owner includes invalidation, not only capacity. |
| I01 | CREATE SEQUENCE, NEXTVAL, query its information-schema row | NEXTVAL returns 7; SEQUENCES returns no rows | Go setDataFromSequences reads actual sequence metadata with visibility/extractor filtering; Rust's provider always returns an empty vector. |
| I01 | Read CLUSTER_CONFIG port in a bare Session | Fabricated `127.0.0.1:15100 / 15100`, warning about `tikv node store1` | Go fetchClusterConfig discovers servers, fetches actual status endpoints and reports only actual failures. Rust reads an oracle snapshot and appends the captured warning unconditionally. |
| I01 | Query CLUSTER_LOG with start time, end time and message pattern | Still reports missing start time | Go clusterLogRetriever.initialize applies the extracted predicates before its guard. Rust raises a captured error before planning/retrieval, regardless of predicates. No successful real log RPC is asserted here. |

Two controls prevent overbroad findings: UPDATE with `JOIN ... USING (id)`
correctly writes both target rows; after restoring the child FK, multi-DELETE
of its referenced parent correctly returns the FK restriction error. These do
not certify the entire DML package, but rule out the claim that all joined
row layouts or all multi-table FK enforcement are missing.

## Source ownership traced beyond the probes

**Cache (C01/C02).** `driver/access.rs::PreparedSelectPlan` and
`driver/dml.rs::PreparedDmlPlan` retain `Mutex<Vec<...>>` and search/append their
own entries. Neither owns global session recency/capacity or a memory-pressure
probe. `non_prepared_plan_cache.rs` has separate SELECT and DML definition
LRUs. `tidb-planner/src/plan_cache_lru.rs` exists, but these production consumers
do not use it. A production reference audit of instance-cache symbols finds
configuration/variable/metric declarations, not a Domain cache or hit/clone
path. Go session.GetSessionPlanCache owns the physical-entry LRU;
plan_cache.go selects that owner or Domain.GetInstancePlanCache and clones
instance hits before execution. Reuse the current key/admission/rebuild work
when moving ownership rather than introducing another cache implementation.

**DML (E01–E03).** Traced `run_update_with_physical` and
`run_delete_with_physical` through the multi-table branches, physical source
execution, row identity reconstruction and final KvTable writes. Go
logical_plan_builder.go retains finalized TblColPosInfos/HandleCols; Rust
computes widths and handle offsets from catalog tables after draining. Go
UpdateExec consumes one chunk at a time. Multi-DELETE does retain a row map,
but builds/deduplicates it while consuming chunks. Rust retains all joined rows
first and only then calls account_joined_rows. `drain_executor_rows` checks
existing tracker state but does not charge the Vec of copied datum rows as it
grows. These ownership differences matter for large joins and memory limits;
no allocation peak or throughput was benchmarked. Matrix-table DML still uses
its own join/selection/order interpreter; ordinary KvTable DML already uses
the shared physical reader, which must not be removed.

**Apply (E04).** PhysicalApply carries `concurrency` and `keep_order`, but
physical_builder::build_apply always returns NestedLoopApplyExec. The session
variable is registered; searching its production references finds no planner
or execution consumer. Go builder.buildApply attempts independent inner-plan
clones for parallel workers and falls back to serial when cloning/building
fails. Existing ordered-buffer helpers and serial SQL tests do not supply the
missing executor/worker lifecycle. Thread-safe Rust contexts are a necessary
implementation constraint, not permission to silently ignore the policy.

**Configured server (S01/S02).** Traced run_configured_node through
`cluster_session=false`, the one/two-table routing, LoadedCatalogAuthority,
RealTiKvMultiSessionFactory, prepare_configured_query and configured write
planning. This selectable path is still separate from the ordinary server
session's optimizer and transaction lifecycle. The two-loaded-table branch
passes static descriptors into from_authority; the one-table branch alone
starts SharedCatalog/schema-watch/stats reloaders. Go Domain/InfoSchema and
session ExecuteStmt do not change SQL engines according to table count.
Migration must include API/configuration/test callers; immediate deletion
without their replacement is not a root fix. The default cluster-session
mode is outside these two findings.

**Virtual tables (I01–I03).** Traced Session's materialize_information_schema_catalog,
infoschema::table_rows and CLUSTER_INFO assembly through their live callers.
Go's corresponding owners are pkg/executor's memtable/infoschema readers,
inspection_result.go, TiFlashSystemTableRetriever, pkg/infoschema/tables.go's
GetClusterServerInfo, and the TiDB coprocessor task construction in
pkg/store/copr/coprocessor.go. Specific I01 providers reviewed:

| Virtual table | Rust provider | Go runtime owner |
| --- | --- | --- |
| CLUSTER_CONFIG | Captured rows plus unconditional store1 warning | fetchClusterConfig: discovered status endpoints, predicates, CONFIG privilege and actual errors |
| SEQUENCES | Empty vector | setDataFromSequences: current visible catalog sequences |
| RESOURCE_GROUPS | One fixed default row | setDataFromResourceGroups: infosync.ListResourceGroups |
| RUNAWAY_WATCHES | Empty vector | setDataFromRunawayWatches: current RunawayManager watches; depends on O05 |
| INSPECTION_RESULT | Empty vector | inspectionResultRetriever: selected rules over a consistent inspection snapshot |
| TIFLASH_TABLES, TIFLASH_SEGMENTS | Empty vectors | TiFlashSystemTableRetriever and its extractor |
| TIKV_STORE_STATUS | Unconditional pd-http-client-unavailable error | memtableRetriever.dataForTiKVStoreStatus / PD status retrieval |
| CLUSTER_LOG | Unconditional missing-start-time error | clusterLogRetriever: extracted bounds/filters and diagnostics streams |

I02 is distinct: CLUSTER_INFO composes only TiDB records and fabricates store1;
Go composes TiDB, PD, stores, TiProxy, TiCDC, TSO and scheduling retrievers.
I03 is also distinct: CLUSTER_PROCESSLIST adds a local instance address but
does not dispatch to peers. Go classifies cluster tables, chooses peer/owner
destinations and builds remote coprocessor tasks. Other cluster table names
appearing in schema lists do not establish this transport. No claim is made
that every empty virtual table is wrong: static schemas, character sets,
metric descriptions and Go-intentionally-empty providers need their own
comparison before removal.

## Validation, risk and remaining scope

Only audit documentation and diagnostic evidence are changed. No production
fix, dependency update, generated-source edit or package acceptance is claimed.
The reproduced data/FK/DDL defects are correctness risks; cache/materialization
and parallel Apply differences affect memory/performance policy; configured
catalog and virtual-table gaps affect compatibility/cluster observability.
The audit itself changes no runtime behavior.

Commands executed from the root (the probe command is scoped to rust above):

```sh
git pull --ff-only origin hparser-integration
git fetch origin master
git diff --check
make lint
TERM=xterm git -c core.hooksPath=hooks commit -m "audit: trace session executor and runtime provider mismatches"
(cd rust && cargo build --locked -p tidb-server)
git push origin HEAD:hparser-integration
```

The living ExecPlan records publication outcomes. The pre-commit hook must
run its own locked server build, followed by the separate pre-push build.
No Go source/import/module/test/Bazel change means no bazel_prepare or Go
failpoint setup is needed. The protocol/inventory scripts were not rerun:
their inputs are unchanged and their receipts retain their own baselines.

Not verified: full original Go package tests/fixtures/platform variants;
cluster propagation/DDL rollback/fanout/TLS; cross-session instance caching;
parallel scheduling/cancellation under load; the previously recorded ten
embedded baseline failures; or sysbench/TPC-C/TPC-H/YCSB. Remaining planner,
expression, transaction/native-client and external package coverage stays
unreviewed at current master. Finding 41 is not an exhaustive semantic bound.
