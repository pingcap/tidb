# Workspace structural mismatch audit — 2026-09-29

The scope now includes **all 88 Rust workspace packages and all 855 Go package
directories**. This pass records **23 source-confirmed structural mismatch
families**. These are shared ownership, representation, composition or lifecycle
problems; they are not 23 completed package ports or an assertion that no other
mismatch exists. No production behavior was changed.

## Reference and coverage

- Go master: `12b639a1161cd5a60126a47277f5ad14c320fd4a`.
- Rust integration baseline: `2c120bc7face45b069dc197ab98a8f001724c140`.
- Git was fetched before the audit; integration was already up to date.
- Inventory: 7,726 Go tree blobs, including 5,826 package-owned artifacts and
  1,900 shared inputs/support/build artifacts. Every tracked Rust artifact is
  also accounted for, including 1,608 outside workspace crate ownership
  (vendored dependencies, shared scripts, documentation and build inputs).
- Source screening: 2,310 `rust/crates/*/src/` Rust files, including inline and
  source-based tests. The 1,229 textual candidate matches and 1,686 ignored-test
  annotations are **unverified leads**, not defect counts. Benchmark exclusions,
  Go-skipped tests and stale comments are present in those lists.
- Cargo graph: 70 workspace packages are possible non-dev dependencies of the
  server on `aarch64-apple-darwin`; 18 are outside that graph. This is neither
  symbol reachability nor proof that optional/platform variants work. Tools and
  test crates outside the server graph are normal.
- 390 Go packages are mentioned by Rust source comments/paths. These are mapping
  hints only; the remaining 465 must not be described as all missing, and a
  mention must not be described as a completed implementation.

[Complete crate coverage](coverage.md) distinguishes selected boundary review
from inventory-only screening. All 855 Go packages and all 88 Rust packages
remain `not_fully_verified`. External Go module versions are pinned by the
inventoried go.mod/go.sum; this pass does not claim exhaustive semantic review of
every package in those external modules. The local dependency graph is one target,
not all platform/build variants. Whole-package acceptance requires production,
generated/platform inputs, original tests/support/fixtures and required gates.

## Findings and repair order

Start with execution owners and context propagation: S01/S02 (DDL), S12 (storage
authority), S04–S06 (background owners), S07 (MPP). Then close wire/executor
contracts S13–S16/S21, typed expression contracts S17–S20, and the remaining SQL
entry points. S23 and S22 are concrete performance/scheduling discrepancies, but
no benchmark improvement is claimed. Repair units remain complete upstream Go
packages even when several Rust crates must change together.


| ID | Priority | Structural boundary |
| --- | --- | --- |
| [S01](#s01) | P1 | Cluster DDL has two execution owners |
| [S02](#s02) | P1 | DDL backfill discards the originating evaluation context |
| [S03](#s03) | P1 | Expression-index admission does not share hidden-column construction |
| [S04](#s04) | P1 | TTL and timer libraries have no server job-manager lifecycle |
| [S05](#s05) | P1 | Resource groups lack the controller and runaway-manager wiring |
| [S06](#s06) | P1 | GC safe-point readers are not a GC owner |
| [S07](#s07) | P1 | MPP physical properties are not backed by a fragment scheduler |
| [S08](#s08) | P1 | Historical reads stop at session admission instead of a transaction provider |
| [S09](#s09) | P1 | PREPARE suppresses plan failures and loses set-operation metadata |
| [S10](#s10) | P1 | IMPORT INTO bypasses distributed task ownership |
| [S11](#s11) | P2 | Information-schema tables use static substitutes for live providers |
| [S12](#s12) | P1 | SQL storage still owns a second transaction implementation |
| [S13](#s13) | P1 | Protocol validation checks a projection against the wrong TiPB baseline |
| [S14](#s14) | P1 | Complete scalar enums mask an incomplete PB builtin registry |
| [S15](#s15) | P1 | Unistore composes a flat scan pipeline instead of Go's executor tree |
| [S16](#s16) | P1 | Region aggregates emit partial states where Go materializes final results |
| [S17](#s17) | P1 | Projection conversion has competing scalar, batch and fast-path policy |
| [S18](#s18) | P1 | Generic argument collection replaces signature-specific evaluation order |
| [S19](#s19) | P1 | Temporal conversion splits policy across defaults and erased errors |
| [S20](#s20) | P1 | Regexp compatibility lacks a complete grammar and byte-range contract |
| [S21](#s21) | P1 | Coprocessor execution loses typed SQL errors at the response boundary |
| [S22](#s22) | P2 | Plan-derived request priority is implemented outside the live compiler |
| [S23](#s23) | P2 | Unistore TopN sorts all candidate keys instead of maintaining Go's heap |

## Evidence-backed register

<a id="s01"></a>

### S01: Cluster DDL has two execution owners

Only CHECK constraint statements use the submit/notify/wait route in RealClusterDdl.execute. Other admitted DDL calls commit_cluster_ddl_with_backfill, which executes Initial(statement) directly. Having persisted step planners and an owner scheduler does not make these client operations owner-scheduled. Restart recovery, concurrent schema transitions and job administration do not share Go's lifecycle.

**Repair boundary:** Route every supported DDL action through one persisted job submission and owner-worker lifecycle, preserving each Go state machine and schema barrier.

**Required validation:** Multi-node concurrent DDL/DML, owner loss at every phase, restart, cancellation, rollback and schema-version barriers; whole pkg/ddl inventory.

**Owning Go package units:** `pkg/ddl`.

**Pinned source evidence:** [rust: rust/crates/tidb-server/src/cluster_session_node/ddl.rs:869](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-server/src/cluster_session_node/ddl.rs#L869); [rust: rust/crates/tidb-exec/src/real_tikv_ddl.rs:742](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-exec/src/real_tikv_ddl.rs#L742); [go: pkg/ddl/executor.go:212](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/ddl/executor.go#L212); [go: pkg/ddl/executor.go:1276](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/ddl/executor.go#L1276).

<a id="s02"></a>

### S02: DDL backfill discards the originating evaluation context

The live KvTableIndexBackfiller invokes index creation with StmtContext::default(). Go reconstructs SQL mode, location, type flags, error levels and warning handler from persisted DDLReorgMeta. Generated-column backfill can therefore evaluate under different policy from the defining statement.

**Repair boundary:** Persist Go-equivalent reorg metadata and construct one reorg evaluation/mutation context for every backfill worker.

**Required validation:** Generated temporal/numeric index values under nondefault time zones, SQL modes and warning policies; compare backfill to subsequent DML and test owner restart.

**Owning Go package units:** `pkg/ddl`.

**Pinned source evidence:** [rust: rust/crates/tidb-server/src/cluster_session_node/ddl.rs:976](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-server/src/cluster_session_node/ddl.rs#L976); [go: pkg/ddl/reorg.go:135](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/ddl/reorg.go#L135); [go: pkg/ddl/backfilling.go:179](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/ddl/backfilling.go#L179).

<a id="s03"></a>

### S03: Expression-index admission does not share hidden-column construction

CREATE INDEX and ALTER ADD INDEX lower through a column-only gate and reject expression parts. Go constructs hidden generated columns before submitting the index job. Existing Rust generated-column evaluation does not remove this admission boundary.

**Repair boundary:** Use the shared Go-equivalent hidden-column/index metadata builder and carry it through catalog loading, writes and backfill.

**Required validation:** CREATE/ALTER expression indexes, duplicate hidden names, invalid functions, warnings, query access, DML maintenance and rollback.

**Owning Go package units:** `pkg/ddl`.

**Pinned source evidence:** [rust: rust/crates/tidb-exec/src/cluster_ddl.rs:2556](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-exec/src/cluster_ddl.rs#L2556); [rust: rust/crates/tidb-exec/src/cluster_ddl.rs:2589](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-exec/src/cluster_ddl.rs#L2589); [go: pkg/ddl/executor.go:5574](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/ddl/executor.go#L5574).

<a id="s04"></a>

### S04: TTL and timer libraries have no server job-manager lifecycle

The server registers TTL variables but does not construct a TTL JobManager. tidb-ttl and tidb-timer are outside the non-dev server dependency graph. The TTL crate exports cache/session/SQL helpers, whereas Go bootstrap starts the manager and the manager owns timer runtime, scheduling, scan/delete work and shutdown. No automatic row expiration is established by the helper ports.

**Repair boundary:** Integrate the complete TTL worker package and its timer runtime into domain startup/shutdown, following master's external-workload role and owner rules.

**Required validation:** Expiration, retries, cancellation, owner transfer, table changes, disabled schedules and external workload roles on a real cluster.

**Owning Go package units:** `pkg/domain`, `pkg/session`, `pkg/ttl/ttlworker`, `pkg/timer/runtime`.

**Pinned source evidence:** [rust: rust/crates/tidb-ttl/src/lib.rs:37](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-ttl/src/lib.rs#L37); [rust: rust/crates/tidb-server/src/cluster_session_node/mod.rs:985](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-server/src/cluster_session_node/mod.rs#L985); [rust: rust/crates/tidb-session/src/sysvar/catalog/ttl.rs:88](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-session/src/sysvar/catalog/ttl.rs#L88); [go: pkg/session/session.go:4757](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/session/session.go#L4757); [go: pkg/domain/domain.go:2914](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/domain/domain.go#L2914); [go: pkg/ttl/ttlworker/job_manager.go:221](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/ttl/ttlworker/job_manager.go#L221).

<a id="s05"></a>

### S05: Resource groups lack the controller and runaway-manager wiring

The vendored client exposes ResourceGroupController and an interceptor setter, but the Rust server does not construct/register a concrete PD resource controller or runaway manager. Carrying a resource-group name in requests is not RU admission/accounting or runaway enforcement. This finding applies to the TiKV cluster path; Go also skips its controller for nil PD.

**Repair boundary:** Follow Domain.initResourceGroupsController and bind its request/response accounting and runaway checker to the actual transport used by SQL.

**Required validation:** PD token grants/exhaustion, retries, cancellation, response accounting, kill/cooldown/switch-group actions and watch synchronization.

**Owning Go package units:** `pkg/domain`, `pkg/resourcegroup/runaway`.

**Pinned source evidence:** [rust: rust/third_party/tikv-client-rs/src/resource_control.rs:345](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/third_party/tikv-client-rs/src/resource_control.rs#L345); [rust: rust/crates/tidb-server/src/cluster_session_node/mod.rs:985](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-server/src/cluster_session_node/mod.rs#L985); [rust: rust/crates/tidb-txnkv/src/kv_contract.rs:744](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-txnkv/src/kv_contract.rs#L744); [go: pkg/domain/runaway.go:42](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/domain/runaway.go#L42); [go: pkg/domain/runaway.go:55](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/domain/runaway.go#L55).

<a id="s06"></a>

### S06: GC safe-point readers are not a GC owner

Rust has client-side safe-point validation and region-cache GC, but no server-owned TiDB GCWorker startup/loop was found. Go BootstrapSession starts the storage GC worker; it advances safe points and processes physical reclamation. An all-Rust deployment does not gain that lifecycle from read-side GC checks. Disk growth and cleanup behavior require cluster validation.

**Repair boundary:** Port and integrate the complete GC owner package, including election, barriers, lock resolution, delete-range work and shutdown; retain the separate client-side safety checks.

**Required validation:** All-Rust deployment, owner failover, service barriers, long transactions, dropped tables and physical reclamation; measure disk usage over repeated workload cycles.

**Owning Go package units:** `pkg/session`, `pkg/store/driver`, `pkg/store/gcworker`.

**Pinned source evidence:** [rust: rust/crates/tidb-txnkv/src/gc_state.rs:22](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-txnkv/src/gc_state.rs#L22); [rust: rust/crates/tidb-server/src/cluster_session_node/mod.rs:985](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-server/src/cluster_session_node/mod.rs#L985); [go: pkg/session/session.go:4762](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/session/session.go#L4762); [go: pkg/store/driver/tikv_driver.go:396](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/store/driver/tikv_driver.go#L396).

<a id="s07"></a>

### S07: MPP physical properties are not backed by a fragment scheduler

Generic find_best_task refuses every non-root task after leaf overrides; the executor treats ExchangeSender/Receiver as local child passthrough. A real TiFlash transport exists, but it constructs a single-fragment table scan. Go builds MPPGather over the retained physical plan and coordinates fragments. This is incomplete distributed execution/planning, not absence of all TiFlash support.

**Repair boundary:** Retain Go MPP task/property alternatives and lower the selected fragment tree through a shared coordinator, including exchanges and cancellation.

**Required validation:** Multi-store exchange, joins/aggregation, distribution/order properties, task failure and cancellation, result equivalence and TPCH plans/latency.

**Owning Go package units:** `pkg/planner/core`, `pkg/executor`, `pkg/executor/internal/mpp`, `pkg/store/copr`.

**Pinned source evidence:** [rust: rust/crates/tidb-planner/src/find_best_task/dispatch.rs:1387](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-planner/src/find_best_task/dispatch.rs#L1387); [rust: rust/crates/tidb-executor/src/driver/physical_builder.rs:5250](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-executor/src/driver/physical_builder.rs#L5250); [rust: rust/crates/tidb-exec/src/tiflash_mpp_scan.rs:17](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-exec/src/tiflash_mpp_scan.rs#L17); [go: pkg/executor/builder.go:4160](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/executor/builder.go#L4160); [go: pkg/executor/builder.go:4170](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/executor/builder.go#L4170).

<a id="s08"></a>

### S08: Historical reads stop at session admission instead of a transaction provider

The live cluster route refuses START TRANSACTION AS OF TIMESTAMP; ordinary session dispatch also refuses nonempty tidb_snapshot/tidb_read_staleness. Go passes the resolved stale timestamp into EnterNewTxn and obtains historical schema/snapshot state. Existing MVCC/client support is not integrated into this SQL lifecycle.

**Repair boundary:** Use one transaction-manager/provider boundary for explicit, prepared and ordinary historical reads, with the pinned historical schema and safe-point validation.

**Required validation:** AS OF, session snapshot/staleness, prepared statements, GC boundary, read-only enforcement, schema history and timestamp reuse.

**Owning Go package units:** `pkg/executor`, `pkg/sessiontxn`.

**Pinned source evidence:** [rust: rust/crates/tidb-planner/src/transaction_control.rs:80](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-planner/src/transaction_control.rs#L80); [rust: rust/crates/tidb-server/src/cluster_session_node/mod.rs:6842](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-server/src/cluster_session_node/mod.rs#L6842); [rust: rust/crates/tidb-session/src/dispatch.rs:3141](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-session/src/dispatch.rs#L3141); [go: pkg/executor/simple.go:655](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/executor/simple.go#L655).

**This audit’s runtime observation:** Rust-only probe accepts SET tidb_snapshot and then rejects SELECT 1. Historical Go execution was not run in this pass; its provider contract was source-traced.

<a id="s09"></a>

### S09: PREPARE suppresses plan failures and loses set-operation metadata

Cluster prepare_general calls a SELECT-only metadata planner and converts every non-variable error into an empty column list. Go GeneratePlanCacheStmtWithAST returns planning errors and obtains fields from the resulting query schema. UNION metadata and invalid queries can be acknowledged without Go's result/error contract.

**Repair boundary:** Build the complete prepared plan once through the common query planner, propagate errors, and derive wire fields from that schema without executing NULL-bound rows.

**Required validation:** COM_STMT_PREPARE/EXECUTE for UNION, missing columns/tables, invalid functions, parameter types, schema changes and plan-cache misses.

**Owning Go package units:** `pkg/executor`, `pkg/server`.

**Pinned source evidence:** [rust: rust/crates/tidb-session/src/lib.rs:1862](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-session/src/lib.rs#L1862); [rust: rust/crates/tidb-server/src/cluster_session_node/mod.rs:7111](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-server/src/cluster_session_node/mod.rs#L7111); [go: pkg/executor/prepared.go:143](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/executor/prepared.go#L143); [go: pkg/executor/prepared.go:159](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/executor/prepared.go#L159).

**This audit’s runtime observation:** Paired probes: Go UNION has 1 field, Rust 0; Go missing-column/table PREPARE returns 1054/1146, Rust acknowledges both with 0 fields. Ordinary SELECT has 1 field in both.

<a id="s10"></a>

### S10: IMPORT INTO bypasses distributed task ownership

The live session arm reads an entire local file, parses CSV and recursively runs INSERT SQL. Go's file import submits and waits for a task with persistent job identity. The local loop lacks that task lifecycle and substitutes its own input/options path. Go's SELECT-source path is separate and must not be indiscriminately treated as a distributed file task.

**Repair boundary:** Follow the Go file-import controller, persistent task/job lifecycle and input processing; use Go's separate SELECT-source implementation where applicable.

**Required validation:** Import options/column user variables, large input memory, progress/job rows, cancellation, restart, error handling and imported data/index equivalence.

**Owning Go package units:** `pkg/executor`, `pkg/dxf/importinto`.

**Pinned source evidence:** [rust: rust/crates/tidb-session/src/dispatch.rs:2586](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-session/src/dispatch.rs#L2586); [rust: rust/crates/tidb-session/src/dispatch.rs:2676](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-session/src/dispatch.rs#L2676); [go: pkg/executor/import_into.go:251](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/executor/import_into.go#L251).

<a id="s11"></a>

### S11: Information-schema tables use static substitutes for live providers

SEQUENCES returns an empty vector despite sequence execution support; CLUSTER_CONFIG returns a checked-in fixture; RESOURCE_GROUPS always returns default. Go retrieves runtime schema/config/controller state and applies visibility rules. These tables can describe a different system from the one executing SQL. Empty TRIGGERS/ROUTINES are not included merely because they are empty. The materialization layer also appends a fixed store1/status-address warning for every CLUSTER_CONFIG query.

**Repair boundary:** Bind each supported system table to the same runtime owner used by execution, with Go's extraction/filter/privilege/error lifecycle.

**Required validation:** Create sequence and inspect metadata; change live configuration/resource groups across nodes and compare information_schema with authoritative state.

**Owning Go package units:** `pkg/executor`, `pkg/infoschema`.

**Pinned source evidence:** [rust: rust/crates/tidb-session/src/infoschema.rs:2937](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-session/src/infoschema.rs#L2937); [rust: rust/crates/tidb-session/src/infoschema.rs:2893](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-session/src/infoschema.rs#L2893); [rust: rust/crates/tidb-session/src/infoschema.rs:2955](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-session/src/infoschema.rs#L2955); [go: pkg/executor/infoschema_reader.go:2795](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/executor/infoschema_reader.go#L2795); [rust: rust/crates/tidb-session/src/dispatch.rs:616](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-session/src/dispatch.rs#L616); [go: pkg/executor/memtable_reader.go:179](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/executor/memtable_reader.go#L179).

**This audit’s runtime observation:** Paired sequence probe: NEXTVAL returns 7 in both; information_schema sequence count is Go 1 versus Rust 0. Only Rust was probed for the hardcoded CLUSTER_CONFIG row; other system-table variants remain source findings.

<a id="s12"></a>

### S12: SQL storage still owns a second transaction implementation

Go's driver delegates Commit/GetSnapshot to client-go KVTxn. Rust ProductionOptimisticTransaction aliases the in-tree RealOptimisticTransaction, whose commit/retry/lock protocol is implemented separately from the vendored client-rust transaction. Thus updating client-rust alone does not update the live SQL commit protocol. This is a confirmed ownership/design split; it does not prove either implementation incorrect in every operation.

**Repair boundary:** Establish one client authority through a Go-equivalent driver boundary, or explicitly inventory and validate every duplicated external-module package before claiming parity; preserve Rust-native async/ownership safety.

**Required validation:** Trace every live get/scan/lock/prewrite/commit/cleanup call, then differential failure/retry/async-commit/1PC/pessimistic tests and sysbench/TPCC/YCSB benchmarks.

**Owning Go package units:** `pkg/store/driver/txn`, `pkg/kv`.

**Pinned source evidence:** [rust: rust/crates/tidb-txnkv/src/transaction/coordinator/mod.rs:306](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-txnkv/src/transaction/coordinator/mod.rs#L306); [rust: rust/crates/tidb-txnkv/src/transaction/coordinator/commit.rs:101](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-txnkv/src/transaction/coordinator/commit.rs#L101); [rust: rust/crates/tidb-server/src/cluster_session_node/transactions.rs:1076](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-server/src/cluster_session_node/transactions.rs#L1076); [go: pkg/store/driver/txn/txn_driver.go:119](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/store/driver/txn/txn_driver.go#L119); [go: pkg/store/driver/txn/txn_driver.go:135](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/store/driver/txn/txn_driver.go#L135).

<a id="s13"></a>

### S13: Protocol validation checks a projection against the wrong TiPB baseline

Master pins TiPB fed7bc47c39d while Rust select.proto names 5f9928e91afe. The descriptor checker iterates locally declared fields, allowing omissions, and resolves the integration branch go.mod. Enum regeneration fixed RegexpLikeSig, but a passing local guard does not establish master schema completeness or executor/decoder capability.

**Repair boundary:** Pin the authoritative master module graph in a reproducible receipt; generate complete schema inputs or maintain an explicit verified omission/capability manifest, checked in both directions.

**Required validation:** Descriptor diff for fields/messages/enums/services/presence/oneofs at master pins, regeneration determinism, malformed/unknown fields and every advertised decoder capability.

**Owning Go package units:** `pkg/expression`, `pkg/store/copr`.

**Pinned source evidence:** [rust: rust/crates/tidb-proto/proto/select.proto:1](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-proto/proto/select.proto#L1); [rust: rust/scripts/check-tipb-proto-projection.py:151](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/scripts/check-tipb-proto-projection.py#L151); [go: go.mod:106](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/go.mod#L106).

**Earlier differential evidence:** [preserved audit](../../planner/pb-expanded-audit-20260928/README.md). Earlier observations retain their original reference/coverage limits; they were not all replayed here.

<a id="s14"></a>

### S14: Complete scalar enums mask an incomplete PB builtin registry

PBToExpr accepts only PbBuiltin::new registrations. Go getSignatureByPB includes families such as LPAD/RPAD that remain absent there. A full ScalarFuncSig enum is not an implementation. Prior registry probes counted 331 missing of 565 Go registrations; that historical count is not a fresh runtime result in this audit and is not a count of planner-generated SQL failures.

**Repair boundary:** Maintain a master-owned signature-to-builder/evaluator/pushdown capability inventory; close the entire expression package rather than adding isolated enum arms.

**Required validation:** Complete registration comparison, typed literal/column decoding, planner eligibility and root-versus-pushed differential results/errors/warnings.

**Owning Go package units:** `pkg/expression`.

**Pinned source evidence:** [rust: rust/crates/tidb-expr/src/distsql_builtin.rs:45](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-expr/src/distsql_builtin.rs#L45); [rust: rust/crates/tidb-expr/src/distsql_builtin.rs:259](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-expr/src/distsql_builtin.rs#L259); [go: pkg/expression/distsql_builtin.go:1058](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/expression/distsql_builtin.go#L1058).

**Earlier differential evidence:** [preserved audit](../../planner/grammar-policy-audit-20260929/README.md). Earlier observations retain their original reference/coverage limits; they were not all replayed here.

<a id="s15"></a>

### S15: Unistore composes a flat scan pipeline instead of Go's executor tree

exec_dag fixes a scan at executors[0], collects predicates/caps/aggregation in a flat loop and rejects aggregation with a row cap. Go's live mock path uses buildAndRunMPPExecutor and recursively constructs the tree. Projection, nested order and root_executor composition cannot be repaired by another aggregate special case.

**Repair boundary:** Use one typed recursive executor builder for the complete cophandler package and preserve each operator's schema, child and context.

**Required validation:** Flat/tree DAG forms, selection/projection/TopN/limit/aggregation combinations, index/table scans, empty inputs, output offsets and SQL pushdown equivalence.

**Owning Go package units:** `pkg/store/mockstore/unistore/cophandler`.

**Pinned source evidence:** [rust: rust/crates/tidb-unistore/src/cophandler.rs:211](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-unistore/src/cophandler.rs#L211); [rust: rust/crates/tidb-unistore/src/cophandler.rs:250](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-unistore/src/cophandler.rs#L250); [go: pkg/store/mockstore/unistore/cophandler/cop_handler.go:190](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/store/mockstore/unistore/cophandler/cop_handler.go#L190); [go: pkg/store/mockstore/unistore/cophandler/mpp.go:620](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/store/mockstore/unistore/cophandler/mpp.go#L620).

<a id="s16"></a>

### S16: Region aggregates emit partial states where Go materializes final results

RegionAggregator.finish_group flattens partial_result for every function. Go aggExec.processAllRows calls GetResult per aggregate and materializes the declared field type. AVG exposes the incompatible state/result cardinality. The recent typed aggregate and group-key repairs do not close this output contract.

**Repair boundary:** Make aggregate phase and output schema explicit at the executor boundary; follow the live Go mock aggregate lifecycle and keep TiKV partial-aggregate contracts distinct.

**Required validation:** AVG/SUM/count and mixed groups, complete/partial/final modes, decimal scales, empty groups, output offsets and actual Go mock cop responses.

**Owning Go package units:** `pkg/store/mockstore/unistore/cophandler`, `pkg/expression/aggregation`.

**Pinned source evidence:** [rust: rust/crates/tidb-unistore/src/cophandler.rs:1014](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-unistore/src/cophandler.rs#L1014); [go: pkg/store/mockstore/unistore/cophandler/mpp_exec.go:1484](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/store/mockstore/unistore/cophandler/mpp_exec.go#L1484).

**Earlier differential evidence:** [preserved audit](../../planner/region-group-lifecycle-20260929/README.md). Earlier observations retain their original reference/coverage limits; they were not all replayed here.

<a id="s17"></a>

### S17: Projection conversion has competing scalar, batch and fast-path policy

convert_numeric_datum returns conversion errors before statement error-policy handling; the decimal fast path derives signedness only from the source and runs before the vectorization switch. Go typed scalar/vector casts share production/error semantics. Existing differential evidence shows optimization-dependent results, warning loss and unsigned-decimal disagreement.

**Repair boundary:** Share typed conversion, source/target metadata and error-policy handling across every scalar/vector/projection route; make fast-path eligibility semantic.

**Required validation:** Replay preserved 120-shape matrix and SQL probes, then all source/target types, context policies, selection vectors and full expression package gates.

**Owning Go package units:** `pkg/expression`.

**Pinned source evidence:** [rust: rust/crates/tidb-expr/src/scalar_function.rs:3859](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-expr/src/scalar_function.rs#L3859); [rust: rust/crates/tidb-expr/src/scalar_function.rs:4465](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-expr/src/scalar_function.rs#L4465); [rust: rust/crates/tidb-expr/src/evaluator.rs:408](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-expr/src/evaluator.rs#L408); [go: pkg/expression/builtin_cast_vec.go:1086](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/expression/builtin_cast_vec.go#L1086).

**Earlier differential evidence:** [preserved audit](../../planner/projection-positional-audit-20260929/README.md). Earlier observations retain their original reference/coverage limits; they were not all replayed here.

<a id="s18"></a>

### S18: Generic argument collection replaces signature-specific evaluation order

PB kernel evaluation eagerly collects JSON arguments and applies common NULL exits for other kernels. Go signatures evaluate/convert/check arguments in their own order. Prior tests show JSON path errors displaced by later value errors and numeric errors suppressed by NULL. Positional regexp also postpones pattern validation until optional-argument processing.

**Repair boundary:** Keep reusable typed primitives, but preserve each Go builtin's ordered argument evaluation, conversion, validation, NULL and error exits.

**Required validation:** Side-effecting/failing children at every argument position, NULL permutations, JSON path/value errors and scalar/vector/PB/native SQL comparisons.

**Owning Go package units:** `pkg/expression`.

**Pinned source evidence:** [rust: rust/crates/tidb-expr/src/scalar_function/pb_builtin.rs:606](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-expr/src/scalar_function/pb_builtin.rs#L606); [rust: rust/crates/tidb-expr/src/scalar_function/pb_builtin.rs:609](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-expr/src/scalar_function/pb_builtin.rs#L609); [go: pkg/expression/builtin_json.go:548](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/expression/builtin_json.go#L548).

**Earlier differential evidence:** [preserved audit](../../planner/pb-expanded-audit-20260928/README.md). Earlier observations retain their original reference/coverage limits; they were not all replayed here.

<a id="s19"></a>

### S19: Temporal conversion splits policy across defaults and erased errors

parse_time_by_source uses permissive parser flags and erases errors to (), while request Columns provides only part of the temporal context. Go carries type flags, SQL mode, time zone and typed invalid-time handling independently. Preserved flag-matrix evidence shows a cast accepting a zero date followed by DATE rejecting it under the same request.

**Repair boundary:** Carry one statement/request-owned context with distinct Go type/error/SQL-mode policy and original conversion errors; bind the statement clock through that context.

**Required validation:** PB flags, SQL modes, time zones/DST, zero/invalid dates, statement clock and every temporal producer/consumer route; replay prior policy corpus.

**Owning Go package units:** `pkg/expression`, `pkg/types`, `pkg/sessionctx/stmtctx`.

**Pinned source evidence:** [rust: rust/crates/tidb-expr/src/cast.rs:1883](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-expr/src/cast.rs#L1883); [rust: rust/crates/tidb-unistore/src/cophandler/eval_context.rs:64](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-unistore/src/cophandler/eval_context.rs#L64); [go: pkg/expression/builtin_cast.go:1134](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/expression/builtin_cast.go#L1134).

**Earlier differential evidence:** [preserved audit](../../planner/grammar-policy-audit-20260929/README.md). Earlier observations retain their original reference/coverage limits; they were not all replayed here.

<a id="s20"></a>

### S20: Regexp compatibility lacks a complete grammar and byte-range contract

The Go adapter rewrites syntax then uses Rust's parser/backend, leaving differing grammar acceptance and complexity limits. Positional builtins still require coerce_str and use str offsets, unlike Go's byte strings plus character positions. Compiler errors become Unsupported. Prior Go grammar/positional probes document valid-text differences and malformed-byte preservation failures; Go panics are excluded from desired parity.

**Repair boundary:** Make Go syntax/limits authoritative before backend lowering and share a byte-preserving match/range/replacement representation with typed regexp diagnostics.

**Required validation:** Upstream grammar tables, valid/invalid bytes, capture/replacement ranges, positions, argument-order rules, compile caching and SQL error 1139.

**Owning Go package units:** `pkg/expression`, `pkg/util/filter`, `pkg/util/table-filter`.

**Pinned source evidence:** [rust: rust/crates/tidb-util/src/go_regexp.rs:110](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-util/src/go_regexp.rs#L110); [rust: rust/crates/tidb-expr/src/builtin_ext/regexp.rs:138](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-expr/src/builtin_ext/regexp.rs#L138); [rust: rust/crates/tidb-expr/src/regexp.rs:55](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-expr/src/regexp.rs#L55); [go: pkg/expression/builtin_regexp.go:109](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/expression/builtin_regexp.go#L109).

**Earlier differential evidence:** [preserved audit](../../planner/grammar-policy-audit-20260929/README.md). Earlier observations retain their original reference/coverage limits; they were not all replayed here.

<a id="s21"></a>

### S21: Coprocessor execution loses typed SQL errors at the response boundary

Execution failures are converted to String and returned in coprocessor.Response.other_error. Go creates SelectResponse.Error through toPBError, retaining MySQL/terror identities, with warnings in the same response. SQL error class can change when a predicate is pushed down, independently of whether expression evaluation itself is correct.

**Repair boundary:** Preserve typed execution errors through the operator tree and encode the Go response envelope; keep transport/decode failures distinct from SQL evaluation failures.

**Required validation:** Identical failing expression at root and pushdown; error codes/messages/warnings, lock responses and cancellation across table/index/aggregate/TopN paths.

**Owning Go package units:** `pkg/store/mockstore/unistore/cophandler`, `pkg/distsql`.

**Pinned source evidence:** [rust: rust/crates/tidb-unistore/src/cophandler.rs:135](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-unistore/src/cophandler.rs#L135); [rust: rust/crates/tidb-unistore/src/cophandler.rs:449](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-unistore/src/cophandler.rs#L449); [go: pkg/store/mockstore/unistore/cophandler/cop_handler.go:598](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/store/mockstore/unistore/cophandler/cop_handler.go#L598).

<a id="s22"></a>

### S22: Plan-derived request priority is implemented outside the live compiler

need_lower_priority is a standalone port over PriorityPlanNode with no production callers found. Live session priority comes from AST modifiers and reaches cop request context directly. Go checks final-plan estimates when no priority is explicit, then carries LowerPriority into execution. Expensive-query coexistence can therefore differ despite the helper tests passing.

**Repair boundary:** Compute the priority decision from the actual retained final plan and attach it to the single statement/request context used by reads and DML.

**Required validation:** Threshold boundary, explicit overrides, prepared/cache hits, DML child plans and observed TiKV priority; mixed TPCH/OLTP load.

**Owning Go package units:** `pkg/executor`.

**Pinned source evidence:** [rust: rust/crates/tidb-exec/src/compiler.rs:99](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-exec/src/compiler.rs#L99); [rust: rust/crates/tidb-session/src/classify.rs:140](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-session/src/classify.rs#L140); [rust: rust/crates/tidb-exec/src/cop_scan.rs:815](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-exec/src/cop_scan.rs#L815); [go: pkg/executor/compiler.go:131](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/executor/compiler.go#L131).

<a id="s23"></a>

### S23: Unistore TopN sorts all candidate keys instead of maintaining Go's heap

Rust accumulates topn_rows and sorts before truncating. Go topNExec uses topNHeap.tryToAddRow while scanning. Rust therefore sorts O(N) candidate keys for a small K. Go also retains received chunks in this mock implementation, so this finding does not claim Go has O(K) total memory.

**Repair boundary:** Follow the shared comparator and bounded TopN heap lifecycle, preserving typed keys, ties, errors and output projection.

**Required validation:** N much larger than K, ties/NULL/collation/descending keys, comparison errors and benchmark allocation/CPU profiles; compare exact Go result contract.

**Owning Go package units:** `pkg/store/mockstore/unistore/cophandler`.

**Pinned source evidence:** [rust: rust/crates/tidb-unistore/src/cophandler.rs:505](https://github.com/pingcap/tidb/blob/2c120bc7face45b069dc197ab98a8f001724c140/rust/crates/tidb-unistore/src/cophandler.rs#L505); [go: pkg/store/mockstore/unistore/cophandler/mpp_exec.go:953](https://github.com/pingcap/tidb/blob/12b639a1161cd5a60126a47277f5ad14c320fd4a/pkg/store/mockstore/unistore/cophandler/mpp_exec.go#L953).

## Revalidated leads and limits

- The old separate eager `tidb-exec` SQL engine is retired. The live query route
  uses `tidb-session` → `tidb-executor` → shared `tidb-planner` physical plans.
  Do not classify configured read-only proof binaries as the deployed SQL engine.
- Merge/index-merge helper modules alone are not proof of an independent planning
  pipeline. Current candidates enter `find_best_task/dispatch.rs`; the earlier
  broad split-pipeline allegation is not reopened without a new counterexample.
  Candidate enumeration, pruning, ties, hints and cache properties still need a
  complete Go oracle audit. This does not certify planner parity.
- Sort has spill/merge workers, and hash-join V2 has a live physical-builder call.
  Claims that these implementations are entirely absent are stale.
- Index-lookup costing now uses the concurrency divisor despite an old header
  saying it is unported. Auto-analyze, statistics usage, historical statistics,
  system-session pooling and DDL notifier have live factory wiring.
- Materialized-view step/build helpers exist. That does not prove live worker
  integration: the DDL owner/submission split in S01 must be closed. The old
  comment that every view job simply stays queued is not used as proof.
- A RangerContext default found in a test fixture is not a production context
  leak. Likewise, the literal `unimplemented!()` hits found in the DDL schedule
  expression source are test mocks, not production stubs.
- The old charset-specific case, LIKE matcher and negative-fraction timestamp
  findings were repaired in `cec2e3f475`; this register does not reopen them.
- Privilege/authentication persistence and all plugin/TLS variants, statistics
  estimator/merge/cache concurrency, schema history/MDL, parser/AST completeness,
  BR and DXF package lifecycles, encodings/collators, and utility concurrency
  require deeper package review. An inventory-only row is not a clean bill of
  health. Absent source comments are not proof of absent implementations.

## Reproduction and validation

Run from the repository root:

```sh
cargo metadata --offline --locked --filter-platform aarch64-apple-darwin --format-version 1 --manifest-path rust/Cargo.toml > /private/tmp/tidb-structural-full-metadata.json
python3 rust/docs/audits/structural-20260929/inventory.py generate --metadata /private/tmp/tidb-structural-full-metadata.json
python3 rust/docs/audits/structural-20260929/inventory.py verify
python3 rust/docs/audits/structural-20260929/report.py
python3 rust/docs/audits/structural-20260929/run-probe.py
```

The metadata command must use the pinned Rust source/dependency inputs. The
inventory reads pinned Git blobs and records complete artifacts, not only files
that contain a mapping comment. Nested workspace crates have distinct ownership;
shared inputs remain explicit. The verifier rejects stale evidence, duplicate or
omitted artifact ownership, and incomplete crate coverage.

`run-probe.py` temporarily appends `rust-probe.txt` to the existing server test
fixture, runs one collector test, and restores the file in `finally`. Run it with
no concurrent source writers. It prints observations; a passing collector is
not a parity pass. `go-probe.txt` is the corresponding Go test overlay, run in the
pinned reference archive with the failpoint wrapper. Exact commands, outcomes and
limitations are in [validation.md](validation.md). Raw observations are retained
in the losslessly compressed probe logs. Source excerpts and hashes are in
[findings.json](findings.json).

Full runtime parity, every build/platform variant, distributed failure behavior,
complete package tests and sysbench/TPCC/TPCH/YCSB performance were not verified.
This is an audit and repair register; the open implementation risks remain.
