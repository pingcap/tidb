# One client stack: client-rust over TiKV and over the embedded store

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds. It records the current state only.

Reference: `PLANS.md` at repository root.

## Purpose / Big Picture

Go TiDB has one client stack. client-go drives real TiKV and, through unistore's in-process RPC and PD clients, the embedded store (`pkg/store/mockstore/unistore`: `unistore.New` returns an `RPCClient` implementing client-go's `tikv.Client` and a `pd.Client`, and `tikv.NewTestTiKVStore` builds a `KVStore` over them).

The Rust tree has two:
- `ngaut/client-rust`, vendored at `rust/third_party/tikv-client-rs`, is the Rust client-go (owner direction).
- `tidb-txnkv` is a second client-go-like stack. The embedded node (`tidb-unistore`) runs on it, and client-rust cannot reach that node.

That is why Go's store-backed GC worker tests cannot run, and it is one of the open structural mismatches in `structural-alignment-execplan.md`.

After this plan, client-rust builds a `KvStore` over any PD client and TiKV client, as client-go does, and `tidb-unistore` implements client-rust's `PdClient` and `KvClient` as Go's unistore implements client-go's. The GC worker then runs over the embedded store in tests, and Go's store-backed GC tests are ported onto it. TiDB paths that run on `tidb-txnkv` can afterwards move to client-rust one at a time.

## Progress

- [x] (2026-10-19) Scouting: Go's wiring, client-rust's generic and hard-wired parts, and `tidb-unistore`'s gaps (see Context).
- [x] (2026-10-19) Step 1, client-rust: `transaction::Client<PdC: PdClient = PdRpcClient>` and a constructor over any PD client; `PdClient::close` with a default (ngaut/client-rust 126987a).
- [x] (2026-10-19) Step 2, client-rust: `KvStore<PdC = PdRpcClient>` and its store runtime over any PD client, with the safe-point KV as a seam (client-go `NewKVStore(uuid, pdClient, spkv, tikvclient)`, with a mock safe-point KV for tests as `NewTestTiKVStore` uses); `PdClient::kv_client(address)` for store-level requests (ngaut/client-rust 398b52d, synced; the workspace compiles unchanged).
- [x] (2026-10-19) Step 3, tidb-unistore: Go unistore's request context (`newRequestCtx`: store, region id and epoch checks answering TiKV's region errors; reads bounded by the region) on every handler; the region manager restructured to Go's `MockRegionManager` (`metapb.Region` with memcomparable bounds, end-key ordered, `GetRegionFromCtx`, `SplitRegion` across regions, `Split`/`SplitRaw`/`SplitArbitrary`, stores) with `cluster.go`'s bootstrap helpers, and the loader as client-go's codec side; the missing handlers (`KvScanLock`, `PhysicalScanLock`, `KvGC`, `KvDeleteRange`, `UnsafeDestroyRange`, `KvCleanup`, `KvBufferBatchGet`, `MvccGetByKey`/`MvccGetByStartTs`, `SplitRegion`, `Coprocessor` with `StoreBatchCoprocessor`, Go's stub answers) and the `MvccStore` operations behind them (`ScanLock`, `UpdateSafePoint` with the GC compaction filter, `DeleteFileInRange`, `dbWriter.DeleteRange`, `PessimisticRollbackWithScanFirst`, `ReadBufferFromLock`, region-bounded `ResolveLock` with `TxnInfos`). Still unported in the Go package: `Flush`, MPP and batch coprocessor, the raw and debug services, `SplitTable`/`SplitIndex`/`SplitKeys`.
- [x] (2026-10-19) Step 4, tidb-unistore: Go's `MockPD` with the `gcStatesManagerSimulator` (`mock_pd.rs`, with `TestMockGCStatesManager`), the `unistore` package's `pdClient` with the `mockKeyspaceManager` as client-rust's `PdClient` (`pd.rs`, with `TestMockKeyspaceManager`), `RPCClient` as client-rust's store client (`rpc.rs`), `Cluster` with the bootstrap helpers (`cluster.rs`) and `New` (`mock.rs`). `tests/client_rust_store.rs`: client-rust's `KvStore::new_with_pd_client` over the embedded store runs transactions across a region split, reaches the GC-state simulator, and resolves an abandoned lock over two regions.
- [x] (2026-10-19) Step 5, part 1: client-rust 99fc4bc adds `PdClient::get_all_stores` (pd-client `GetAllStores`, PD's unfiltered `metapb.Store` list, which the worker's `needsGCOperationForStore` reads). The worker's store, PD and TiKV halves moved from `tidb-server` into `tidb-gcworker` (`tikv_store::TikvStoreClient<PdC>`, over any client-rust `KvStore`), as Go's worker calls `tikv.Storage` and `pd.Client` itself; the server builds it over its `KvStore`. `gc_worker_test.rs` runs on the embedded store as Go's suite does (three stores; the real PD and GC-state simulator replace the hand-written PD double; a hijacking client over the store's own records destroy-range, delete-range and, through the RPC client's send hook, Go's `UnistoreRPCClientSendHook`, scan-lock; real region splits replace the simulated ones). Go's package variable `txnSafePointSyncWaitTime` is process-wide again. Newly ported: `TestDoGCForOneRegion` (its first arm), `TestRunGCJobAPI`, `TestRunDistGCJobAPI`, and the probe half of `TestRunGCJob`.
- [x] (2026-10-19) Step 5a, client-rust a4f27c0: client-go's `KVStore.oracle` (`oracle::StoreOracle`; `Client::set_oracle` / `get_oracle`). Transactions, their committer, lock resolution and read validation take timestamps from it, and the PD client's TSO serves until one is installed. The GC suite installs `MockOracle` on the store, as Go's does, and drops its `FakeStore`. Newly ported: the probe half of `TestLeaderTick`, `TestGCWithPendingTxn`, `TestGCWithPendingTxn2`, `TestSkipGCAndOnlyResolveLock`. The vestigial `ServerMinStartTSPath` put of the last three is skipped, since the worker no longer reads it.
- [x] (2026-10-19) Step 5b, client-rust 9ed202e: client-go's `RegionLockResolver` (`transaction::gc`: the trait, `KeyLocation`, `BaseRegionLockResolver`, the resolver-driven `resolve_locks_for_range`, `new_gc_resolve_lock_max_backoff`). `Client::resolve_locks_for_range`, and so `KvStore`'s GC, runs client-go's loop over the store's own resolver. `GC_SCAN_LOCK_LIMIT` is client-go's 1024, not 2048; the worker's scan-lock requests carried 2048. The worker holds Go's `regionLockResolver` (`NewRegionLockResolver("gc-worker", store)`, replaceable as in Go's tests). Newly ported: `TestResolveLockRangeInfine`, `TestResolveLockRangeMeetRegionCacheMiss`, and `TestResolveLockRangeMeetRegionEnlargeCausedByRegionMerge` (on client-rust's mocktikv, as Go's runs on `MockTiKV`).
- [x] (2026-10-19) Step 5c, tidb-unistore: Go unistore's pessimistic lock waiting. A conflict under a waiting request parks a waiter (`handleCheckPessimisticErr`, `normalizeWaitTime`, the 1000ms store limit) and asks the in-process deadlock detector past the statement's first lock. Commits, pessimistic rollbacks, the 1PC prewrite, cleanups and `CheckSecondaryLocks`' pessimistic arm wake waiters, and rollbacks clean the detector, at Go's sites. `KvPessimisticLock` is split at `waiter.Wait()` (`kv_pessimistic_lock_begin` / `_finish`), so the clients wait with the store released: the in-process client on its caller's thread, the client-rust RPC client on a blocking task. After the wait, Go's arms apply: timeout, deadlock (with the wait chain `DeadlockResponse` now carries), the forced retry, and the write conflict at the store's `latestTS`. Newly ported: `TestResolveLocksNearTxnSafePoint`.
- [ ] Step 5d: the client failpoint arms of `TestDoGCForOneRegion` (client-go `tikvStoreSendReqResult`).
- [ ] Step 6: move TiDB paths from `tidb-txnkv` onto client-rust one at a time, each with its tests and a gate.

## Surprises & Discoveries

- Observation: Go's embedded store does not run the GC worker (`mockStorage.StartGCWorker` is a no-op; only `tikvStore.StartGCWorker` builds one). Go's GC tests construct the worker directly over a mock store.
- Observation: client-rust's own mocktikv (the counterpart of client-go's) panics on `UnsafeDestroyRange`, as client-go's does, so it cannot stand in for Go's unistore in the delete-range tests.

- Observation: the embedded node's handler served one whole-keyspace region however the store had been split, and the in-process client dropped the region context the transport routed (only batch-get copied it into the request). Go's client-go puts it in every request (`tikvrpc.SetContext`), and the gRPC path here does too (`rpc/batch/prewrite.rs`, `replace_top_level_context`).
  Evidence: after adding Go's epoch checks, the transport test failed with `RegionMiss` until the in-process client applied the routed context.
- Observation: client-go keeps the oracle on the `KVStore` (`SetOracle`), the lock resolver behind an interface (`RegionLockResolver`), and unistore parks conflicting pessimistic lockers server-side; client-rust takes timestamps from the PD client, `ResolveLocksForRange` has no resolver seam, and the embedded store answers lock conflicts at once. Go's GC tests mock all three (`MockOracle`, `mockGCWorkerLockResolver`, lock waits), so each is a structural gap with tests waiting on it (Steps 5a-5c).
- Observation: Go's `BootstrapWithSingleStore` names the store `store1`; the embedded store called itself `unistore-in-process`. It now uses Go's name.

## Decision Log

- Decision: `KvGC` runs badger's compaction over the whole in-memory engine at once: versions above the safe point stay, the newest at or below it stays unless Go's `GCCompactionFilter` drops it, older ones go.
  Rationale: badger compacts in the background at its own pace; reads at or after the safe point see the same data either way, and TiDB never reads below the GC safe point. Doing it at `KvGC` keeps the engine deterministic for tests.
  Date/Author: 2026-10-19, Claude.
- Decision: `KvPessimisticLock` is split at Go's `waiter.Wait()`, and the waiting happens in the RPC clients with the store's mutex released.
  Rationale: the store is single-writer behind one mutex; waiting inside it would block the lock holder's commit. Go's handler goroutine waits outside its latches the same way.
  Date/Author: 2026-10-19, Claude.
- Decision: the embedded store's PD client hands out its RPC client from `map_region_to_store` / `kv_client` / `all_stores`.
  Rationale: client-rust's `PdClient` owns the store-client pool (`PdRpcClient` holds the TiKV connections), where client-go keeps `tikv.Client` beside `pd.Client`; the pairing is client-rust's shape, the clients themselves are Go's.
  Date/Author: 2026-10-19, Claude.
- Decision: `dbWriter.DeleteRange`'s badger delete marker is recorded as a committed delete (empty value, no start ts) one version above the key's newest.
  Rationale: reads from then on find nothing and older snapshots still read their versions, as with badger's marker; the engine has no separate tombstone kind.
  Date/Author: 2026-10-19, Claude.

- Decision: client-rust is the one Rust client-go; the embedded store goes behind its seams. The alternatives were a second GC client over `tidb-unistore` inside TiDB, or extending `tidb-txnkv`; both were rejected because they deepen the duplicated client stack.
  Rationale: owner direction ("client-rust should be the rust version of client-go"; chose "client reaches unistore"; "keep align with go, remove the Structural mismatches").
  Date/Author: 2026-10-19, Claude.
- Decision: generics with the gRPC types as defaults (`Client<PdC = PdRpcClient>`, `KvStore<PdC = PdRpcClient>`), so every existing caller compiles unchanged.
  Rationale: client-go's types take interfaces; Rust's defaults give the same surface without touching callers.
  Date/Author: 2026-10-19, Claude.

## Outcomes & Retrospective

Steps 1 and 2: client-rust builds a transaction client and a `KvStore` over any PD client; `a_store_over_any_pd_client_runs_transactions_and_store_requests` runs one over mocktikv. The store's uuid stays derived from the PD cluster id, as in the gRPC path.

## Context and Orientation

Go:
- `pkg/store/mockstore/unistore.go:27-60`: `newUnistore` calls `tikv.NewTestTiKVStore(newClientRedirector(client), InterceptedPDClient{pdClient}, …)`.
- `pkg/store/mockstore/unistore/rpc.go`: `RPCClient.SendRequest` (:76) maps each `tikvrpc` command to the unistore `Server` (:144-341). `CmdScanLock`, `CmdResolveLock`, `CmdGC` and `CmdDeleteRange` are at :261-268; `CmdUnsafeDestroyRange` (:341) answers an empty response.
- `pkg/store/mockstore/unistore/tikv/server.go`: `newRequestCtx` (:120) resolves the region from the request context and records region errors. The handlers are `KvScanLock` (:557), `KvGC` (:600), `KvDeleteRange` (:611) and `UnsafeDestroyRange` (:1096).
- `pkg/store/mockstore/unistore/pd.go` and `tikv/mock_region.go`: `MockPD` (:736) with `gcStatesManagerSimulator` (:992).

client-rust:
- Generic already: `PdClient` (`src/pd/client.rs:69`), `KvConnect` / `KvClient` (`src/store/client.rs`), `PdRpcClient<KvC, Cl>`, `Transaction<PdC>`, `Snapshot<PdC>`, `Runner<PdC, H>`.
- Hard-wired: `transaction::Client` holds `Arc<PdRpcClient>` and has endpoint-only constructors (`src/transaction/client.rs:89-330`). `KvStore` and `StoreRuntime` hold `PdRpcClient` and build an etcd safe-point loader from the endpoints (`src/tikv.rs:573-610`, `:1143-1240`).

tidb-unistore (`rust/crates/tidb-unistore`):
- `KvHandler` (`kv_handler.rs`) has get, scan, batch-get, prewrite, commit, batch-rollback, the pessimistic calls, heartbeat, check-txn-status, check-secondary-locks and resolve-lock. It assumes a single region with no epoch checks.
- `MvccStore` has no scan-lock, safe-point, delete-range or destroy-range operation.
- `InProcessPd` (`tso.rs`) holds only the TSO; its GC safe point is always 0.
- `InProcessRegionManager` has region lookups and a Go-like split with epoch bumps.

## Plan of Work

The steps above run in order. Each client-rust step lands upstream (ngaut/client-rust), is synced in, and must leave TiDB compiling unchanged. Each TiDB step ends with the gate. Step 3 ports Go unistore's server pieces with their Go tests; Step 4 ports `MockPD` and the GC simulator with theirs.

## Concrete Steps

- client-rust: `cargo test --lib` in the worktree (pre-existing parallel flakes in `transaction::transaction::tests` are checked against pristine master).
- TiDB: `rust/scripts/sync-tikv-client-rs.sh`, then `cargo nextest run -p tidb-unistore -p tidb-gcworker -p tidb-server`.

## Validation and Acceptance

- `KvStore::new_with_pd_client` over client-rust's mocktikv runs a transaction (a client-rust test).
- The GC worker runs a whole job over the embedded store in a TiDB test (Go's unistore pretends `UnsafeDestroyRange`, since its delete-range emulator removed the keys at DDL time, so the job completes the range without destroying data).
- Go's store-backed GC tests pass on the embedded store.

## Idempotence and Recovery

Each step is a separate commit, upstream and here; defaults keep every caller unchanged, so a step can be reverted alone.

## Interfaces and Dependencies

In client-rust `src/transaction/client.rs`:

    pub struct Client<PdC: PdClient = PdRpcClient> { pd: Arc<PdC>, … }
    impl<PdC: PdClient> Client<PdC> {
        /// client-go `tikv.NewKVStore`'s client half over any PD client.
        pub async fn new_with_pd_client(pd: Arc<PdC>, config: Config) -> Result<Self>;
    }

In client-rust `src/tikv.rs`:

    pub struct KvStore<PdC: PdClient = PdRpcClient> { inner: Client<PdC>, runtime: Arc<StoreRuntime<PdC>> }
    impl<PdC: PdClient> KvStore<PdC> {
        /// client-go `NewKVStore(uuid, pdClient, spkv, tikvclient)`.
        pub async fn new_with_pd_client(pd: Arc<PdC>, safe_point_kv: Arc<dyn SafePointKv>, config: Config) -> Result<Self>;
        /// client-go `GetTiKVClient().SendRequest(ctx, addr, req, timeout)`.
        pub async fn send_request(&self, address: &str, request: &dyn Request, timeout: Duration) -> Result<Box<dyn Any>>;
    }

In client-rust `src/pd/client.rs`, trait `PdClient`:

    /// The store-level client for `address` (client-go `KVStore.GetTiKVClient()`).
    async fn kv_client(&self, address: &str) -> Result<Self::KvClient>;
