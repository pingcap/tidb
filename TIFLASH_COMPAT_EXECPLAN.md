# ExecPlan: Rust TiDB TiFlash compatibility (query path first, DDL surface second)

## Purpose and outcome

The `hparser-integration` branch carries a Rust port of TiDB (`rust/crates/*`, binary
`rust/target/release/tidb-server`). The user-visible outcome this plan delivers: after
loading a minimal dataset on a local `tiup playground nightly` cluster that includes a
TiFlash node, the **Rust** TiDB server can plan and execute a query that reads the table
from the TiFlash columnar replica, returning exactly the rows the Go nightly TiDB
returns, and the cluster can set the replica through TiDB DDL.

Today the Rust node can read TiKV but: the `READ_FROM_STORAGE(TIFLASH[t])` hint is
silently ignored, `SET SESSION tidb_isolation_read_engines='tiflash'` still plans
`cop[tikv]`, `information_schema.tiflash_replica` does not exist, and
`ALTER TABLE ... SET TIFLASH REPLICA` is refused by the cluster DDL admission list.
Evidence receipts for that baseline are in "Current state evidence" below.

All fixes must use **Go master** (`origin/master`, fetched; HEAD
`5316d43574169d1e78515b1511efc03c21952576`) as the source of truth, and be pushed to
`origin/hparser-integration`. Branch rules: before pushing run
`git fetch origin hparser-integration && git rebase FETCH_HEAD`; commit messages must
not contain backticks; keep diffs minimal and anchored with Go file/line references in
comments.

## Definitions (plain language)

- **TiFlash replica**: a columnar copy of a table maintained by a TiFlash node as a
  raft learner of TiKV. A table is readable from TiFlash only after
  `ALTER TABLE t SET TIFLASH REPLICA 1` creates a PD placement rule and the replica
  has finished syncing (`AVAILABLE=1`).
- **MPP**: Massively Parallel Processing. Go TiDB reads TiFlash by planning the scan
  into an *mpp fragment*: `TableFullScan (mpp[tiflash]) -> ExchangeSender(PassThrough)
  -> TableReader (root)`, dispatching the fragment as an MPP task to a TiFlash store
  (`MppVersion: 3` today), and streaming results back.
- **Isolation read engines**: session variable `tidb_isolation_read_engines`
  (default `tikv,tiflash,tidb`) restricting which storage engines the planner may use.
- **Cluster DDL admission**: the Rust node runs DDL only for an admitted action set
  (see `rust/crates/tidb-executor/src/ddl/alter_table.rs` catch-all arm and the
  cluster DDL framework in `rust/crates/tidb-exec/src/cluster_ddl.rs`); other DDL is
  refused with error 1105 telling the user to run it on a TiDB server.

## Current state evidence (2026-09-18)

Playground: `tiup playground nightly --tag tiflash-rust --db 1 --pd 1 --kv 1
--tiflash 1 --without-monitor --port-offset 45200` (Go nightly TiDB :49200, PD :47579,
TiFlash :49130; a second manual Rust node on :49300 with `--path 127.0.0.1:47579
--port 49300 --status 10030 --cluster-session --load-privileges`).

1. Go TiDB baseline (port 49200) — script `/tmp/tiflash-go-baseline.sh`:
   `CREATE TABLE tiflashv.t (id INT PRIMARY KEY, v VARCHAR(20))` + 8 rows,
   `ALTER TABLE tiflashv.t SET TIFLASH REPLICA 1`, AVAILABLE=1 after ~8s,
   `EXPLAIN SELECT /*+ READ_FROM_STORAGE(TIFLASH[t]) */ * FROM t` shows
   `TableReader -> ExchangeSender(PassThrough) -> TableFullScan mpp[tiflash]` with
   `MppVersion: 3`, and the forced read returns all 8 rows.
2. Rust node (port 49300), probe `/tmp/tiflash-rust-probe.sh`:
   - TiKV read: 8 rows, correct.
   - `information_schema.tiflash_replica`: ERROR 1146 (table does not exist).
   - Forced hint EXPLAIN: still `cop[tikv]` (hint silently ignored).
   - `SET SESSION tidb_isolation_read_engines='tiflash'`: still `cop[tikv]`, rows still
     served from TiKV (Go would refuse: no access path).
   - `ALTER TABLE ... SET TIFLASH REPLICA 1`: ERROR 1105 "this node changes the
     cluster's catalog for ... only" (outside the admitted DDL set).
   - Sysvars present: `tidb_allow_mpp=ON`, `tidb_enforce_mpp=OFF`,
     `tidb_isolation_read_engines=tikv,tiflash,tidb`; `tidb_enforce_mpp=1` does not
     change the plan.
3. Planner unit tier: `rust/crates/tidb-planner/src/find_best_task/dispatch.rs` has a
   tested TiFlash path-selection arm (test
   `read_from_storage_tiflash_selects_the_tiflash_table_path`) and
   `rust/crates/tidb-planner/src/task.rs` defines `MppTask`, but the **live** planning
   flow in `rust/crates/tidb-executor/src/driver/physical_builder.rs` hardcodes
   `store_type: StoreType::TiKv` (line ~4962) and never wraps scans in exchange
   operators. `rust/crates/tidb-planner/tests/casetest_mpp_integration_suite_source.rs`
   documents the missing MPP join/exchange tiers as ignored gap tests.
4. Existing Rust MPP groundwork to reuse (all under `rust/crates/`):
   `tidb-txnkv/src/mpp.rs` (MppVersion, MppQueryId, MppTask meta, dispatch request
   params), `tidb-txnkv/src/mpp_probe.rs` (Go `mpp_probe.go` port: MPP-alive probing),
   `tidb-distsql/src/chblock.rs` (TiFlash CHBlock decode), `tidb-distsql`
   request builder with `set_store_type` and batch-cop flags, `tidb-txnkv/src/tiflash.rs`
   (replica-read policy strings).

## Go master anchors (source of truth per milestone)

- Engine gating and hint resolution:
  `pkg/planner/core/find_best_task.go:1986-2170` (TiFlash candidate filtering,
  forced-path retention), `pkg/planner/core/operator/logicalop/logical_datasource.go:364`
  (isolation-read-engines tiflash membership), `pkg/planner/core/logical_plan_builder.go:608,634`
  (READ_FROM_STORAGE hint to store-type), `expression_rewriter.go:2536`
  (replica availability check shape).
- MPP physical shape: `pkg/planner/core/operator/physicalop/fragment.go`
  (`GenerateRootMPPTasks` :167, `constructMPPTasksImpl` :659,
  `constructMPPBuildTaskForNonPartitionTable` :774),
  `pkg/planner/core/exhaust_physical_plans.go` (mpp task creation for scans/joins),
  `pkg/planner/core/task.go` (attach2TaskForMpp* family).
- MPP dispatch/execution: `pkg/store/copr/mpp.go` (task build, addresses, client),
  `pkg/executor/internal/mpp/local_mpp_coordinator.go` (+`executor_with_retry.go`),
  `pkg/store/copr/mpp_probe.go` (already ported).
- DDL replica surface: `pkg/ddl/executor.go:625,4241` (ActionSetTiFlashReplica job
  construction), `pkg/ddl/ddl_tiflash_api.go` (TiFlashManagementContext,
  PollAvailableTableProgress :289, refreshTiFlashTicker :384,
  refreshTiFlashPlacementRules :549, PollTiFlashRoutine :638),
  `pkg/ddl/cluster.go:167` (action classification),
  `pkg/domain/infosync/tiflash_manager.go` (PD placement-rule manager; this worktree
  already carries the Go file for reference).
- Result surfacing: `information_schema.tiflash_replica`
  (`pkg/executor/infoschema/infoschema.go` `tiflashReplicaTableCols` family on master;
  search `tiflash_replica` under `pkg/executor` / `pkg/infoschema`).

## Milestones

### M0 — Baseline evidence (DONE)

Produce the playground receipts listed under "Current state evidence". Commands live in
`/tmp/tiflash-go-baseline.sh` and `/tmp/tiflash-rust-probe.sh`; re-run them after each
milestone as the regression gate.

### M1 — Live planner: single-table TiFlash MPP plan (DONE 2026-09-18)

Implemented and verified on the live playground:

- `rust/crates/tidb-planner/src/physical/mod.rs`: new `ExchangeType` +
  `PhysicalExchangeSender` plan node (Go `physical_exchange_sender.go`), and
  `PhysicalTableReader::explain_info` now renders `MppVersion: 3, data:<child>`
  for MPP readers (Go `physical_table_reader.go:258`, newest mpp version = V3,
  `pkg/kv/mpp.go:44-47`).
- `rust/crates/tidb-planner/src/task.rs`: the TiFlash cop-to-root conversion wraps
  the fragment in a PassThrough sender and selects `ReadReqType::Mpp`
  (Go `adjustReadReqType`, `physical_table_reader.go:299`).
- `rust/crates/tidb-executor/src/explain.rs`: the reader's pushed-down tier task is
  derived from `read_req_type`/`store_type` (`mpp[tiflash]` / `cop[tiflash]` /
  `cop[tikv]`) instead of a hardcoded `cop[tikv]`.
- `rust/crates/tidb-executor/src/driver/physical_builder.rs`: an ExchangeSender
  builds as a pass-through while M2 owns real dispatch (Decision Log).
- New golden tests `rust/crates/tidb-planner/tests/tiflash_mpp_root_shape_source.rs`;
  the dispatch test `read_from_storage_tiflash_selects_the_tiflash_table_path` now
  asserts the full reader -> sender -> scan tree.

Live receipt (Rust node :49300, playground `tiflash-rust`): hinted EXPLAIN renders

    TableReader_6  root  MppVersion: 3, data:ExchangeSender_5
    └─ExchangeSender_5  mpp[tiflash]  ExchangeType: PassThrough
      └─TableFullScan_4  mpp[tiflash]  table:t  keep order:false, stats:pseudo

which is byte-shape identical to the Go nightly baseline (only plan ids differ), and
the forced SELECT still returns the 8 rows. Plain (non-hinted) EXPLAIN stays
`cop[tikv]`.

Validation: `cargo test -p tidb-planner --lib` (941 pass; the single failure
`union_unsigned_widening_uses_the_in_union_cast_signature` also fails on pristine
`origin/hparser-integration` HEAD `2a8dd8ba56`, unrelated to this change),
`cargo test -p tidb-executor --lib` (1332 pass),
`cargo test -p tidb-planner --test tiflash_mpp_root_shape_source` (4 pass).

### M2 — Dispatch: run the MPP task against real TiFlash (DONE 2026-09-18)

Implemented and verified live:

- `rust/crates/tidb-proto/proto/mpp.proto`: DispatchTaskRequest /
  CancelTaskRequest / EstablishMPPConnectionRequest / MPPDataPacket projections
  (upstream kvproto mpp.proto:54-109); `tikvpb.proto`: DispatchMPPTask /
  CancelMPPTask / EstablishMPPConnection / ReportMPPTaskStatus RPCs; `select.proto`:
  tree-form `root_executor` (tipb select.proto: field 17), `ExchangeSender` +
  `ExchangeType` (executor.proto:77-99), `Executor.exchange_sender` (field 12),
  `ExecType.TypeExchangeSender` (executor.proto:32).
- `rust/crates/tidb-exec/src/tiflash_mpp_scan.rs` (new): the single-fragment MPP
  lowering — TiFlash store selection via PD `GetAllStores` + `engine=tiflash`, region
  collection via PD `BatchScanRegions` over the executor's wire record ranges,
  tree-form DAG marshal (`TableScan` under PassThrough sender, TypeChunk, little
  endian), TaskMeta with Go's field set (start_ts, task 1, store address, mpp version
  3, NULL keyspace 4294967295, api version 1), `DispatchMPPTask` then
  `EstablishMPPConnection` streaming, packets decoded through the shared
  `SelectResponseIter`.
- Engine plumbing: `PushdownScanRequest.read_engine`/`.schema_version` (Go
  `PhysicalTableScan.StoreType` + `SchemaMetaVersion`) carried from the physical scan
  through `TableScanExec` into the pushdown request; `CopScanSource` routes TiFlash
  requests to the MPP source and refuses by name (Backend error, never a silent TiKV
  answer) anything it cannot lower (Selection/TopN/aggregate/ordered/index shapes).
- Node wiring: `cluster_session_node/boot.rs` builds the source over the node's PD
  seeds plus the catalog schema-version watch (`is.SchemaMetaVersion` at dispatch
  time; a stale or zero version makes TiFlash treat a synced table as missing).

Live receipt (Rust node :49300, playground `tiflash-rust`, table `tiflashv.t`, 8 rows,
replica set by Go DDL, AVAILABLE=1):

    SELECT /*+ READ_FROM_STORAGE(TIFLASH[t]) */ * FROM t

returns exactly Go's 8 rows; TiFlash's own log records the task
(`finish with 0.026 seconds, 8 rows, 1 blocks, 144 bytes`) — the data crossed the
columnar engine. TiFlash also logged the exact failure ladder this slice debugged
through: duplicate executor ids -> missing DispatchTaskRequest.schema_ver -> keyspace
0 instead of the NULL keyspace 4294967295 -> retry_regions being advisory (Go
mpp.go:174-189 invalidates the cache; it does not fail the dispatch).

Refusals by name (Backend error, statement fails): pushed Selection / TopN /
partial aggregate / ordered scans / index scans — a later milestone lowers them.
Plain (non-hinted) queries are unchanged (`cop[tikv]`).

Validation: `cargo test -p tidb-proto -p tidb-distsql -p tidb-executor --lib --tests`
(all green: 30/249/1332/329/...), `cargo test -p tidb-exec --lib` (332 pass),
`cargo test -p tidb-planner --lib` (941 pass, one pre-existing unrelated failure),
release build clean; live dispatch transcript in `/tmp/tiflash-rust-node.log` and
`~/.tiup/data/tiflash-rust/tiflash-0/tiflash.log`.

Known boundaries (documented, not approximated): isolation-read-engines-only
enforcement is not wired into the executor request (hint or planner store assignment
is the trigger); `RetryRegions` responses are logged, not cache-invalidated; the
tidb-exec `--tests` targets carry six pre-existing compile errors present on pristine
`origin/hparser-integration` HEAD (verified by stashing).

### M3 — Rust-only cluster read acceptance (DONE 2026-09-18)

With the Go TiDB node STOPPED (process killed; PD/TiKV/TiFlash left running by the
playground), the Rust node on :49300 answered:

- plain TiKV read: `COUNT(*) = 8`; 
- forced TiFlash read: `SELECT /*+ READ_FROM_STORAGE(TIFLASH[t]) */ id, v FROM t
  ORDER BY 2 LIMIT 3` → the columnar rows, correct order;
- write + read-back: `INSERT (9,'nine')` then a plain read returned it (replica sync
  is asynchronous, so the forced TiFlash read legitimately does not see it yet);
  fixture restored to 8 rows afterwards.

This proves the MPP read path has no hidden dependency on a Go node: catalog, PD,
region and dispatch all resolve through the Rust node itself. Receipt: this session
transcript; the killed-process state is reproducible with `kill <go-tidb-pid>`.

### M4 — DDL surface: SET TIFLASH REPLICA on the Rust node + visibility (SCOPED, not started)

Implementation recipe with every Go anchor and live-learned pitfall pinned (verified
against the running v9.0.0-beta.2.pre-nightly TiFlash during M2 debugging):

1. **Admission**: `lower_alter_table_catalog` (cluster_ddl.rs ~1375) gains a
   `SetTiFlashReplica { hypo, count, labels }` arm (AST exists,
   tidb-ast/src/ddl.rs:1428): `hypo` refuses (Go ErrUnsupportedHypoTiFlashReplica);
   produces `DdlStatement::SetTiFlashReplica { schema, table, count, labels }`.
2. **Job executor**: after the `ModifyAutoIdCache` arm (cluster_ddl.rs ~7366) — the
   exact pure-meta-mutation template: `clone_like_go()`, set
   `tiflash_replica = Some(TiFlashReplicaInfo { count, location_labels, available:
   false, .. })` (or `None` on count 0), `meta_put(table_kv_key(db_id, id),
   serialize_table_info)`, `diff.action_type = ActionType::ACTION_SET_TI_FLASH_REPLICA`
   (model action_type.rs:102, value 30). Model types all exist
   (tidb-model/src/table.rs:1694).
3. **PD placement rules** (Go pkg/domain/infosync/tiflash_manager.go):
   - rule id `MakeRuleID(id)` = `table-{id}-r` (:400); API V1 does NOT encode the id
     (encodeRuleID :187);
   - group `tiflash` with group config index `RuleIndexTiFlash`, override false,
     set-if-different (`SetTiFlashGroupConfig` :243);
   - rule keys = `codec.EncodeRegionRange(GenTableRecordPrefix(id),
     EncodeTablePrefix(id+1))` — for API V1 the WIRE (flagged-int) record prefix, e.g.
     table 118 = `7480000000000000765F72...`; the un-flagged codec prefix selects the
     WRONG region (live-learned, M2);
   - role `learner`, count = number of Up TiFlash stores;
   - HTTP: POST `/pd/api/v1/config/rule`, DELETE
     `/pd/api/v1/config/rule/{group}/{id}`, group POST `/pd/api/v1/config/rules/bundle`;     
   - plus `PostAccelerateScheduleBatch` (`regions/batch-accelerate-schedule`) with the     same range.   
   reqwest is already a tidb-exec dependency (label_delivery.rs pattern).4. **Availability poller** (Go ddl_tiflash_api.go :470-527 + tiflash_manager.go :107-171):     every 2s per replica table:   - region count: PD HTTP `/stats/region?start_key={hex}&end_key={hex}` (`count` field);   - tiflash peer count: per Up TiFlash store, GET     `http://{status_address}/tiflash/sync-status/keyspace/{keyspace_id}/table/{table_id}`     (helper.go:906; NULL keyspace id = 4294967295; the response parses as region-id     lines);   - oneReplicaProgress = |regions with >=1 tiflash peer| / regionCount; available =     progress >= 1.0;   - flip by submitting Go's `ActionUpdateTiFlashReplicaStatus` job through the same     cluster DDL publication path (`UpdateTableReplicaInfo`).5. **information_schema.tiflash_replica**: memory table over `TableInfo.TiFlashReplica`     (columns per Go `tiflashReplicaTableCols`).Live acceptance: on a Rust-only cluster, `ALTER TABLE t SET TIFLASH REPLICA 1` returns;
PD rule visible at `/pd/api/v1/config/rule/group/tiflash`; AVAILABLE 0 -> 1 without a
Go node; forced read serves through TiFlash; `SET TIFLASH REPLICA 0` clears both.

Edit targets:

- `rust/crates/tidb-executor/src/ddl/alter_table.rs`: new action arm parsing the
  existing `tidb_ast::AlterTableAction::SetTiFlashReplica` (count, labels, hypo must
  error like Go: `ErrUnsupportedFlash hypo` shape) into a job.
- Cluster DDL framework: new persisted action executor registered beside
  `ACTION_DROP_TABLE` (prior work in `rust/crates/tidb-exec/src/cluster_ddl.rs`,
  `rust/crates/tidb-server/src/cluster_session_node/ddl.rs` is the template).
- PD rule client: HTTP client functions in `rust/crates/tidb-pd-client/`
  (currently no placement-rule API) mirroring
  `pkg/domain/infosync/tiflash_manager.go` endpoints
  (`/pd/api/v1/config/rules`, `config/rule`, group config, store count).
- Polling routine: port `ddl_tiflash_api.go` context + tick loop into
  `rust/crates/tidb-domain/` or the server node, driven by the existing periodic-task
  scaffolding.
- `information_schema.tiflash_replica`: memory-table definition in the session
  infoschema layer (`rust/crates/tidb-session/src/infoschema.rs` family) reading
  `TableInfo.TiFlashReplica` like Go.

Verify: unit tests per crate; live gate — on the Rust-only cluster (M3 shape):
`ALTER TABLE tiflashv.t SET TIFLASH REPLICA 1` returns, `information_schema.tiflash_replica`
transitions AVAILABLE 0 -> 1, forced read works, `SET TIFLASH REPLICA 0` clears the row.
Then re-run the original mixed-cluster probe to confirm no regression.

### M5 — Push and receipts (ongoing)

Rebase on `origin/hparser-integration`, push the milestone commits with anchors in the
messages, and record in this file: commit hashes, exact validation commands, and the
playground transcripts. The goal-level audit needs: Go-baseline receipt, Rust-forced
read receipt, Rust-only cluster receipt, DDL receipt, and the commit map.

Commit map so far: `cbfdc7cad4` (M1), `5265825c09` (M2), `31646df17e` (M3 receipt),
`fca6aad656` (isolation-engines 1815 errno identity), plus the M3/M4 plan commits.

## Constraints and non-goals

- Go master is authoritative; every behavior difference found on the way is either
  fixed to match or documented here in the Decision Log.
- No speculative behavior: if a Go master gate cannot be honored without machinery the
  Rust workspace lacks (e.g., full MPP join costing), keep the scope to the milestone
  contract and leave the documented-gap tests ignored rather than inventing plans.
- Non-goals for this plan: MPP joins/aggregates on TiFlash, BatchCop optimization
  path, disaggregated mode (`tiflash-disagg`), auto-adjust replica management
  (`tidb_auto_tiflash_replica...`), CTE/Window/TopN pushdown beyond what the
  single-table scan needs.

## Progress

- [x] M0: playground baseline receipts (Go green; Rust gaps captured).
- [x] M1: live planner TiFlash MPP plan (EXPLAIN shape parity).
- [x] M2: dispatch to real TiFlash, rows served through the columnar engine.
- [x] M3: Rust-only cluster read acceptance.
- [x] M4 (core): SET TIFLASH REPLICA admitted on the Rust node as a persisted job;
      PD rule synced by the poller; availability flipped through
      `ActionUpdateTiFlashReplicaStatus` (schema version 76 receipt).
- [ ] M4 remainder: `information_schema.tiflash_replica` (observability only; the
      planner reads availability from the table info directly).
- [x] M5 (partial): M1/M2/M3 pushes + receipts; re-audit after M4.

## Decision Log

- 2026-09-18: Chose MPP (not BatchCop) as the read path: Go nightly's default plan for
  a forced single-table TiFlash read is `MppVersion: 3` MPP with a PassThrough
  exchange (verified in the M0 Go baseline EXPLAIN), and the Rust tree already carries
  the MPP data-model/probe groundwork; BatchCop would diverge from the default shape.
- 2026-09-18: M1 edits the live `physical_builder.rs` flow rather than only the
  `find_best_task` tier because the live EXPLAIN proves the live flow bypasses the
  TiFlash arm entirely; reusing the planner arm is preferred where callable.
- 2026-09-18: DDL work is M4, after the read path, because replica compatibility can be
  demonstrated against a Go-set replica first, and DDL+PD-rule+polling is the largest
  remaining surface; the goal's "compatible with go ddl" clause is satisfied
  incrementally but the read-compatibility claim lands earlier.
- 2026-09-18 (M1): the executor builds an ExchangeSender as a local pass-through, so a
  forced TiFlash query still executes its fragment over the local TiKV cop path while
  EXPLAIN already shows the MPP shape. This interim is deliberately bounded: M2 must
  flip the reader's dispatch to DispatchMPPTask before any correctness claim about
  actually reading the columnar replica. Recorded here so no one mistakes the M1
  receipt for end-to-end TiFlash execution.
- 2026-09-18: pre-existing failure `union_unsigned_widening_uses_the_in_union_cast_signature`
  reproduces on pristine `origin/hparser-integration` HEAD (verified by stashing the
  change); it is outside this plan's scope and left for its owning surface.
- 2026-09-18 (M2): the single-fragment lowering computes the dispatch regions from
  the executor's wire record ranges (`PushdownScanRequest.ranges`) via PD
  `BatchScanRegions`, NOT from a recomputed table prefix — the raw table prefix lacks
  the flagged-int key encoding and selects the wrong region.
- 2026-09-18 (M2): the planner's read_engine trigger covers hint/planner store
  assignment; `tidb_isolation_read_engines` pruning is still absent from
  `DispatchContext`, so an engines-only forced read keeps planning TiKV. Recorded as
  the M4-adjacent gap; the goal's claim is demonstrated through the hint path.
- 2026-09-18 (M2): `PushdownScanColumn`-based collations keep the shared TiKV
  lowering's un-rewritten binary id (63); the MPP lowering rewrites ids to Go's
  new-collation form (-63) at its own boundary, matching Go `ColumnToProto` exactly.
- 2026-09-18: isolation-read-engines gating verified END-TO-END on the current binary
  (the M1-era "engines-only forcing keeps TiKV" conclusion was measured on the pre-M2
  binary and is now obsolete): engines='tiflash' + available replica serves the read
  through TiFlash without a hint; engines='tiflash' without a replica answers Go's
  exact 1815 diagnostic (`Internal : No access path for table ... Please check
  tiflash replica.`) after carrying the errno on `PlanErrorKind::InternalCoded`.
- 2026-09-18: `retry_regions` cache invalidation is intentionally NOT ported: the MPP
  source re-reads regions from PD on every open scan, so there is no stale cache to
  invalidate; the response is logged (`tiflash_mpp_stale_regions`).
