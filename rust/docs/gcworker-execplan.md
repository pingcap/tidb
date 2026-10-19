# Port Go's GC worker (`pkg/store/gcworker`) to Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

A Rust TiDB node running against real TiKV never collects garbage. Dropping or truncating a table writes its key ranges to `mysql.gc_delete_range` (the producer side is ported, `rust/crates/tidb-exec/src/ddl_delete_range.rs`), but nothing ever destroys those ranges, so the dropped data stays on disk forever. Nothing advances PD's transaction safe point or GC safe point either, so TiKV's compaction filter never drops old MVCC versions. Nothing writes the `tikv_gc_*` rows of `mysql.tidb` that operators and `gcutil` read (`tikv_gc_safe_point`, `tikv_gc_life_time`, ...).

After this plan, the node runs Go's GC worker. Every minute one TiDB becomes GC leader (a lease in `mysql.tidb`), computes a new safe point from `tikv_gc_life_time`, advances the txn safe point in PD, resolves every lock older than it, destroys the delete ranges (`UnsafeDestroyRange` on every TiKV store), re-destroys ranges finished 24 hours earlier, removes the dropped tables' placement and label rules, and advances the GC safe point. To observe it: on a TiUP playground, `DROP TABLE t` and wait past `tikv_gc_run_interval`. `mysql.gc_delete_range` is then empty, `mysql.gc_delete_range_done` holds the range, `SELECT variable_value FROM mysql.tidb WHERE variable_name='tikv_gc_safe_point'` advances, and `SHOW STATUS LIKE 'tidb_gc%'` shows the leader.

## Progress

- [x] (2026-10-19) Scouting: the Go package inventory, the Rust producer side and emulator, the vendored client's GC surface, and the server's integration points (see Context).
- [x] (2026-10-19) Milestones 1 and 2: `rust/crates/tidb-gcworker` ports every function of `gc_worker.go` (`seams.rs` the collaborators, `gc_worker.rs` the worker), and `gc_worker_test.rs` ports the 28 tests that need no MVCC store, on the client's mock PD and recording doubles (`cargo nextest run -p tidb-gcworker`: 28 passed).
- [ ] Store-backed tests, with the server adapters: `TestDoGCForOneRegion`, `TestResolveLockRangeInfine`, `TestResolveLockRangeMeetRegionCacheMiss`, `TestResolveLockRangeMeetRegionEnlargeCausedByRegionMerge`, `TestResolveLocksNearTxnSafePoint`, `TestRunGCJobAPI`, `TestRunDistGCJobAPI`, `TestGCWithPendingTxn`, `TestGCWithPendingTxn2`, `TestSkipGCAndOnlyResolveLock`, and the probe checks of `TestLeaderTick` / `TestRunGCJob`.
- [x] (2026-10-19) Milestone 3, client half: `ngaut/client-rust` 87b45a1 adds `update_service_gc_safe_point` (V2 for a keyspace client), `get_all_keyspaces`, keyspace routing of `UpdateGCSafePoint` to V2, and `pd_keyspace::is_keyspace_using_keyspace_level_gc`, which the crate now uses; 006593d pins protoc 36.2 (generated code byte-identical). Synced into `rust/third_party/tikv-client-rs`. The per-store `UnsafeDestroyRange` needs no client change: `PdClient::kv_client(address)` with `dispatch_with_timeout` is client-go's `GetTiKVClient().SendRequest`.
- [x] (2026-10-19) Milestone 3, server half: `tidb-server/src/cluster_session_node/gc_worker.rs` implements the seams (`NodeGcClient` over the client's `KvStore` and the node's PD client for `GetAllStores`; `NodeGcSessions` over internal sessions running Go's `pkg/ddl/util` statements; `NodeGcRules` over the placement and label rule services), and `boot.rs` starts the worker on the real-TiKV node beside the stats workers, dropping it before the stats maintenance at shutdown. `tidb-session/src/statusvar.rs` ports Go's statistics registry; `SHOW STATUS` lists the registered providers, and the worker registers its `tidb_gc_*` rows as Go's `NewGCWorker` does. `tidb-exec/src/placement_delivery.rs` ports `infosync.DeleteTiFlashPlacementRules` (`MakeNewRule` with a zero count through `SetPlacementRuleBatch`, after `SetTiFlashGroupConfig`). client-rust cb2f9a3 exports `ResolveLocksForRange`, 58727da adds `KvStore::send_request` (client-go `GetTiKVClient().SendRequest`). The session adapter's statements are pinned on the embedded store (`gc_worker_sessions_run_gos_system_table_statements`).
- [ ] Milestone 4: real TiKV acceptance (playground), then the package receipt.

## Surprises & Discoveries

- Observation: Go's `TestSetServiceSafePoint` expectations come from TiDB's own unistore PD (`pkg/store/mockstore/unistore/tikv/mock_region.go`, `gcStatesManagerSimulator.UpdateServiceGCSafePoint`), PD's compatibility wrapper: the "gc_worker" service advances the txn safe point, other services set or delete GC barriers, and the answer is the txn safe point. client-go's mock PD keeps plain service safe points. The test double follows TiDB's unistore; the Rust embedded store's PD (`tidb-unistore`) has neither and must port the simulator before the store-backed tests run on it.
  Evidence: the test failed on the client mock's semantics and passes on the ported ones.
- Observation: Go's `TestGCPlacementRules` asserts a counter behind a `gcDeletePlacementRuleCounter` failpoint that `gc_worker.go` on this branch never evaluates; the Rust test asserts the bundles PD receives instead (two for table 10, which Go's mock job names as both table and old partition).

- Observation: the vendored client (`rust/third_party/tikv-client-rs`, the client-go port) already has most of the KV half. It has `RangeTaskRunner` (`src/transaction/range_task.rs`), GC lock resolution (`KvStore::gc`, `src/tikv.rs:1261`, `GcController`), `Client::unsafe_destroy_range` (`src/transaction/client.rs:621`), and `delete_range_with_concurrency`. Its PD has `advance_transaction_safe_point` / `advance_gc_safe_point` / `get_gc_state`. Its mock PD (`src/mock/mocktikv/pd.rs`) models service safe points, GC barriers and both safe points.
  Evidence: grep of the vendored sources, 2026-10-19.
- Observation: the vendored PD client lacks `UpdateServiceGCSafePoint`, the legacy `UpdateGCSafePoint`, and `GetAllKeyspaces`, although the protos are vendored (`proto/pdpb.proto:85,87`). `tidb-pd-client` has only `all_stores` and `get_gc_state`. The production opener's `TidbPdBridge` refuses `update_safepoint` and `load_keyspace`.
- Observation: `unsafe_destroy_range` in the vendored client sends to every store in one plan. Go sends one request per store address and reports the failed stores (`doUnsafeDestroyRangeRequest`), filtering stores with `needsGCOperationForStore`. The worker needs a per-address send.
- Observation: there is no `variable.RegisterStatistics` registry in Rust. `SHOW STATUS` is a static table plus hard-coded dynamic rows (`rust/crates/tidb-session/src/show.rs:493`, `:1899-1966`).
- Observation: Go's own unit tests run the worker on a mock store whose PD client implements the GC calls. The Rust embedded store (unistore) answers a GC safe point of 0 and has no `UnsafeDestroyRange` handler (`rust/crates/tidb-unistore/src/tso.rs:178`). So the worker does not run on the embedded node, which keeps the delete-range emulator (Go `SupportDeleteRange() == false`).

## Decision Log

- Decision: the port unit is the whole Go package `pkg/store/gcworker`: `gc_worker.go` (1938 lines), `gc_worker_test.go` (2400 lines) and `main_test.go`. It goes into one new crate, `rust/crates/tidb-gcworker`.
  Rationale: AGENTS.md rule 6.
  Date/Author: 2026-10-19, Claude.
- Decision: the worker reaches the store, PD, sessions, and the placement and label services only through traits defined in the crate (`GcStore`, `GcPdClient`, `GcKvClient`, `GcSessionFactory`, `GcRuleClient`). Production adapters live in `tidb-server`; test doubles live in the crate's tests.
  Rationale: Go reaches them through interfaces (`kv.Storage`, `pd.Client`, `pdgc.InternalController`, `tikv.Storage`, `sessionapi.Session`, `infosync`). Its tests replace the KV client (`mockGCWorkerClient`) and the lock resolver (`mockGCWorkerLockResolver`) the same way, and `tidb-gcworker` must not depend on `tidb-server`.
  Date/Author: 2026-10-19, Claude.
- Decision: missing PD client calls (`UpdateServiceGCSafePoint`, `UpdateGCSafePoint`, `GetAllKeyspaces`, `IsKeyspaceUsingKeyspaceLevelGC`, a per-address `UnsafeDestroyRange` send) are added upstream in `ngaut/client-rust` (local checkout `~/projects/client-rust`, push authorized by the owner) and reach this repository through `scripts/sync-tikv-client-rs.sh`; the vendored tree is never edited directly, since the next sync would overwrite it. Until then the crate keeps a private port of `IsKeyspaceUsingKeyspaceLevelGC`.
  Rationale: they are PD client and client-go methods in Go; the vendored client is the client-go port.
  Date/Author: 2026-10-19, Claude.

## Outcomes & Retrospective

Not started.

## Context and Orientation

Go sources:
- `pkg/store/gcworker/gc_worker.go`. In run order:
  - `NewGCWorker`, `Start`/`start` (tick every minute), `tick`.
  - `checkLeader`: a lease in `mysql.tidb` under the keys `tikv_gc_leader_uuid` / `_desc` / `_lease`, inside a transaction.
  - `leaderTick`: `getGCConcurrency`, then the unified-GC keyspace path (`runKeyspaceGCJobInUnifiedGCMode`, `runKeyspaceDeleteRange`), or else the full path below.
  - `prepare` / `checkPrepare`: `checkGCEnable`, `checkGCInterval`, `calcNewTxnSafePoint` (`validateGCLifeTime`, `advanceTxnSafePoint`), then save `tikv_gc_last_run_time` and `tikv_gc_safe_point`.
  - `needsToWait`.
  - `runGCJob`:
    1. sleep `txnSafePointSyncWaitTime`;
    2. `resolveLocks` (range task runner over the null keyspace minus the excluded prefixes, then every unified-GC keyspace in batches of 50);
    3. `deleteRanges`: `LoadDeleteRanges`, then per range `doUnsafeDestroyRangeRequest` (or a delete-range task on raft-kv2), `doGCPlacementRules`, `doGCLabelRules`, `CompleteDeleteRange`, with `calcDeleteRangeConcurrency` workers;
    4. `redoDeleteRanges` (done ranges older than safe point minus 24 hours, then `DeleteDoneRecord`);
    5. `broadcastGCSafePoint`;
    6. `notifyGCV2AfterGC`.
  - Helpers: `needsGCOperationForStore`, `getStoresForGC`, `loadGCConcurrencyWithDefault`, `checkUseDistributedGC`, `doGCForRegion`, the `mysql.tidb` value, time and duration helpers, `getGCRules`, `Stats`/`GetScope` (status variables `tidb_gc_*`).
  - Exported test entry points: `RunGCJob`, `RunDistributedGCJob`, `RunResolveLocks`, `MockGCWorker`.
- `pkg/store/gcworker/gc_worker_test.go` holds about 40 tests (`TestPrepareGC`, `TestLeaderTick`, `TestDeleteRangesFailure`, `TestConcurrentDeleteRanges`, the `TestResolveLocksWithKeyspaces_*` family, `TestRunGCJob`, `TestGCPlacementRules`, `TestGCLabelRules`, `TestGCWithPendingTxn*`, `TestCalcDeleteRangeConcurrency`, ...).

Rust state:
- Producer: `rust/crates/tidb-exec/src/ddl_delete_range.rs`. `DelRangeTable::locate`/`load` work for either `gc_delete_range` or `gc_delete_range_done` (Go `LoadDeleteRanges` / `LoadDoneDeleteRanges`); `delete_mutations` is `DeleteDoneRecord`'s SQL.
- Emulator: `rust/crates/tidb-exec/src/real_tikv_ddl/delete_range.rs`. Its `complete_delete_range` is Go `CompleteDeleteRange` and is private.
- History jobs: `load_history_persisted_ddl_job` (`rust/crates/tidb-exec/src/real_tikv_ddl.rs`) is Go `ddl.GetHistoryJobByID`.
- `gcutil`: `rust/crates/tidb-gcutil`.
- Real-TiKV boot: `rust/crates/tidb-server/src/cluster_session_node/boot.rs:72`. Background workers start around `:605-620`, and their guards drop after `node.run()` (`:764-787`). `StatsMaintenanceWorker` (`stats_maintenance.rs`) is the model: a struct that owns a thread and joins it on `Drop`.
- Internal SQL with BEGIN / FOR UPDATE / COMMIT: `ClusterSessionFactory::advanced_sys_session_pool()` (`cluster_session_node/mod.rs:1305`), used as in `login_policy.rs:52-140`.
- A private `tikv_client::tikv::KvStore` built from the PD endpoints has precedent in `rust/crates/tidb-exec/src/tikv_region_store.rs:90-110`.
- Placement and label delivery: `rust/crates/tidb-exec/src/placement_delivery.rs`, `label_delivery.rs`.

Terms. The txn safe point is the timestamp below which no transaction may still commit; locks older than it are resolved. The GC safe point is the timestamp below which snapshots may be dropped. A delete range is a key range of a dropped object, queued in `mysql.gc_delete_range` until the GC safe point passes its timestamp. `UnsafeDestroyRange` is a TiKV RPC that deletes a range's data directly from RocksDB, bypassing raft.

## Plan of Work

Milestone 1. Create `rust/crates/tidb-gcworker` (license header, workspace member, `Cargo.toml` following `tidb-gcutil`).
- Port the constants, `gcVariableComments` and `gcConcurrency`.
- Port the pure functions with their tests: `needsGCOperationForStore` (`TestNeedsGCOperationForStore`), `calcDeleteRangeConcurrency` (`TestCalcDeleteRangeConcurrency`), `getGCRules`, and the time and duration codecs. Go `time.Duration.String` / `time.ParseDuration` reuse `tikv_client::util::duration` if it exposes them, or else are ported once in `tidb-util`. The GC time format reuses `tikv_client::util::misc::GC_TIME_FORMAT` and its compatible parser.
- Define the seam traits (see Interfaces).

Milestone 2. Port `GCWorker` and every method listed in Context, in Go's order and with Go's names: `tick`, `check_leader`, `leader_tick`, `prepare`, `check_prepare`, `run_gc_job`, `delete_ranges`, `redo_delete_ranges`, `resolve_locks`, `broadcast_gc_safe_point`, `do_gc_placement_rules`, `do_gc_label_rules`, `notify_gcv2_after_gc`, `stats`, `run_gc_job_api` (Go `RunGCJob`), `run_distributed_gc_job`, `run_resolve_locks`, `MockGcWorker`.
- `start` owns one thread with a one-minute ticker, a done channel and a cancellation token, as Go's goroutine does. `close` cancels it, and `Drop` joins it.
- Port `gc_worker_test.go` onto test doubles:
  - a `mysql.tidb` in memory behind `GcSessionFactory`, with transactions;
  - the vendored mock PD's GC state behind `GcPdClient`;
  - a recording `GcKvClient` (Go `mockGCWorkerClient`);
  - a scripted lock resolver (Go `mockGCWorkerLockResolver`).
- Tests that need a real MVCC store with region splits (`TestResolveLockRange*`, `TestGCWithPendingTxn*`) run against the vendored client's mock cluster (`src/mock/mocktikv`), which client-go's own `mocktikv` corresponds to.

Milestone 3.
- Add to `ngaut/client-rust`: `update_service_gc_safe_point`, `update_gc_safe_point`, `get_all_keyspaces`, `is_keyspace_using_keyspace_level_gc`, and a per-address `UnsafeDestroyRange` send; push, then sync the vendored tree.
- In `tidb-server`, add `cluster_session_node/gc_worker.rs`. It implements the seams over a private `KvStore` (as `TikvRegionStore` does), the advanced system session pool, `load_history_persisted_ddl_job`, the placement and label delivery clients, and `DelRangeTable`. Make the emulator's `complete_delete_range` public for reuse (Go `util.CompleteDeleteRange`).
- Start the worker in `boot.rs` after the stats workers and drop its guard next to `stats_maintenance`. The embedded node does not start it.
- Add a status-variable provider hook so `SHOW STATUS` lists `tidb_gc_last_run_time`, `tidb_gc_leader_desc`, `tidb_gc_leader_lease`, `tidb_gc_leader_uuid` and `tidb_gc_safe_point` as Go's `Stats` does.

Milestone 4. On a TiUP playground (see `docs/agents/testing-flow.md`, RealTiKV tests; the playground starts in the background and is cleaned up afterwards):
- set `tikv_gc_run_interval` and `tikv_gc_life_time` to their minimum of 10m;
- drop a table, wait out the interval, and check the acceptance in Validation;
- ask the Go oracle the same questions on the same playground, for the status rows and `mysql.tidb` values.
Then write the package receipt: the inventory of Go functions and tests, and where each lives.

## Concrete Steps

From `rust/`:

    cargo nextest run -p tidb-gcworker
    cargo nextest run -p tidb-server -E 'test(/gc_worker/)'
    cargo build --locked -p tidb-server

## Validation and Acceptance

- Every Go test of the package has a Rust counterpart with the same name in snake case, passing; any test that cannot be ported is listed here with the reason.
- On the playground, after `CREATE TABLE t(a int); INSERT INTO t VALUES (1); DROP TABLE t;` and one GC round:
  - `SELECT COUNT(*) FROM mysql.gc_delete_range` returns 0;
  - `mysql.gc_delete_range_done` holds the table's range;
  - `tikv_gc_safe_point` and `tikv_gc_last_run_time` are present and advance;
  - a raw scan of the dropped table's prefix through the vendored client returns nothing.
- `SHOW STATUS LIKE 'tidb_gc_%'` lists the five rows with the values in `mysql.tidb`.

## Idempotence and Recovery

The worker is idempotent by design, as Go's is. A delete range is completed only after its destroy succeeded, and a failed round leaves the rows for the next round. The redo pass destroys a finished range again. Milestones 1 and 2 add a crate with no caller, so they are safe to land alone. Milestone 3 starts the worker only on the real-TiKV node.

## Artifacts and Notes

None yet.

## Interfaces and Dependencies

In `rust/crates/tidb-gcworker/src/lib.rs`:

    pub trait GcStore: Send + Sync {
        fn current_version(&self) -> Result<u64, GcError>;              // kv.Storage.CurrentVersion
        fn keyspace_meta(&self) -> Option<KeyspaceMeta>;                // Codec.GetKeyspaceMeta
    }
    pub trait GcPdClient: Send + Sync {
        fn get_all_stores(&self) -> Result<Vec<metapb::Store>, GcError>;
        fn update_gc_safe_point(&self, safe_point: u64) -> Result<u64, GcError>;
        fn update_service_gc_safe_point(&self, service_id: &str, ttl: i64, safe_point: u64) -> Result<u64, GcError>;
        fn advance_txn_safe_point(&self, target: u64) -> Result<AdvanceTxnSafePointResult, GcError>;
        fn advance_gc_safe_point(&self, target: u64) -> Result<AdvanceGcSafePointResult, GcError>;
        fn get_all_keyspaces(&self, start_id: u32, limit: u32) -> Result<Vec<KeyspaceMeta>, GcError>;
    }
    pub trait GcKvClient: Send + Sync {
        fn unsafe_destroy_range(&self, address: &str, start: &[u8], end: &[u8], timeout: Duration) -> Result<String, GcError>;
        fn delete_range_task(&self, start: &[u8], end: &[u8], concurrency: usize) -> Result<(), GcError>;
        fn resolve_locks_for_range(&self, max_version: u64, start: &[u8], end: &[u8], scan_limit: u32) -> Result<RangeTaskStat, GcError>;
        fn is_raft_kv2(&self) -> Result<bool, GcError>;
    }
    pub trait GcSession {
        fn begin(&mut self) -> Result<(), GcError>;
        fn commit(&mut self) -> Result<(), GcError>;
        fn rollback(&mut self);
        fn load_value(&mut self, key: &str) -> Result<String, GcError>;          // SELECT ... FOR UPDATE
        fn save_value(&mut self, key: &str, value: &str, comment: &str) -> Result<(), GcError>;
        fn load_delete_ranges(&mut self, done: bool, safe_point: u64) -> Result<Vec<DelRangeTask>, GcError>;
        fn complete_delete_range(&mut self, range: &DelRangeTask, record_done: bool) -> Result<(), GcError>;
        fn delete_done_record(&mut self, range: &DelRangeTask) -> Result<(), GcError>;
        fn history_job(&mut self, job_id: i64) -> Result<Option<tidb_model::Job>, GcError>;
    }
    pub trait GcSessionFactory: Send + Sync {
        fn create_session(&self) -> Box<dyn GcSession + '_>;
    }
    pub trait GcRuleClient: Send + Sync {
        fn delete_tiflash_placement_rules(&self, table_ids: &[i64]) -> Result<(), GcError>;
        fn put_rule_bundles(&self, bundles: Vec<tidb_placement::Bundle>) -> Result<(), GcError>;
        fn get_label_rules(&self, rule_ids: &[String]) -> Result<Vec<LabelRule>, GcError>;
        fn delete_label_rules(&self, rule_ids: Vec<String>) -> Result<(), GcError>;
    }

The signatures are the plan's starting point. A change made while porting is recorded in the Decision Log.
