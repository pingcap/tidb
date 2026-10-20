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
- [x] (2026-10-19) Step 5d: the failpoint arms of `TestDoGCForOneRegion`. client-rust 2c4f2a0, 8300d82, b3c36c0: `send_req` answers from client-go's `tikvStoreSendReqResult` failpoint (`failpointSendReqResult`: timeout, `GCNotLeader`, `PessimisticLockNotLeader`, `GCServerIsBusy`, `busy`) under a new `failpoints` feature, takes client-go's per-RPC timeout, and `KvStore::send_req(backoffer, request, region, timeout)` is client-go's `KVStore.SendReq`; `GcRequest` is region-pinned. The worker's `doGCForRegion` sends through it (the seam takes the region's version id and returns the typed region error, as Go's does), and the test is ported whole.
- [ ] Step 6: move TiDB's paths from `tidb-txnkv`'s client stack onto client-rust, in this order. Each sub-step lands with its tests and a gate.
  - [x] (2026-10-19) 6a+6b, safe point and one native store per node. `tidb_txnkv::driver::tikv_store::TikvStore` is Go `tikvStore`'s `*tikv.KVStore`: the node's one client-rust store, built at boot (`TikvStore::connect_with_security`, which, like `NewKVStore`, fails when the txn safe point cannot be loaded). The read authority owns it; `TikvRegionStore` and the GC worker share it instead of each opening a PD connection, region cache and runtime. Transactions check reads with client-go's `KVStore.CheckVisibility` on that store (`StoreVisibility`); `gc_state`'s own loader and refresher (`TxnSafePointRefresher`, `GcStateCache`, the etcd fallback) are gone, and `gc_state` keeps only the error tiers TiDB's driver maps (`ErrTxnAbortedByGC`, the stale-cache PD timeout). The embedded node builds its store with `KvStore::new_with_pd_client` over unistore's `PdClient` (`tidb_unistore::mock::over`, sharing the node's handler, region manager and oracle), so its safe point comes from the mock PD's GC states as Go's does, not a static zero. Client-rust stores run on one process-wide runtime (`store_runtime`), never dropped, so releasing a store on one of its threads cannot drop the runtime under it. Playground acceptance (PD v9 nightly, 2026-10-19): the node boots on the shared store, and the GC worker on it waits Go's 100s `txnSafePointSyncWaitTime`, resolves locks and finishes delete ranges and redo-delete ranges for a dropped table. The `delete placement rules failed` error it logs (PD answers `404 null` for the missing `tiflash` rule group) is Go's behavior too on a cluster without TiFlash: `GetPlacementRuleGroupByID` returns any non-200 as an error.
  - [ ] 6c, transactions, point reads and scans: `ClientTransaction` generic over the PD client, so the opener runs client-rust's `Transaction` on the native store, not over `ClientPd` / `ClientKv` and `tidb-txnkv`'s region cache and BatchCommands transport. Embedded node first, then TiKV. The opener's public API stays.
    Inventory of what `ClientPd` adds beyond client-rust's `PdClient` (2026-10-19), with each item's Go counterpart and its home:
    - Visibility gating (`take_read_trace().rpc_count`): Go calls `KVStore.CheckVisibility` after each Get/BatchGet past the snapshot cache and each scan page (`txnsnapshot/snapshot.go`, `scan.go`). client-rust validates at those sites (`SnapshotVisibilityValidator`). Done in client-rust 7d59cff: the store installs its safe point cache as the validator of every transaction it begins; before, its transactions checked the safe point before the read was sent (inside `validate_read_timestamp`) and raised the abort wrapped as an oracle error. `last_get` (region and publication of the last Get) is read by one test only: delete.
    - Per-call context (`set_call`: deadline, cancellation, `kv_exec_counter`): Go's `ctx` through the Backoffer, and TiDB's `KvExecCounter.RPCInterceptor` (`kv.RPCInterceptor`). client-rust has `RpcInterceptor` and a snapshot read timeout, but no foreground cancellation (it uses `Cancellation::default()`). Home: an `RpcInterceptor` TiDB installs per call, which fails a request whose caller is cancelled or past its deadline and bounds the RPC by the remaining time. That is where Go enforces `ctx` (the RPC client), and the interceptor reaches every physical RPC including retries and lock resolution; client-rust needs no new API. The exec counter is the same interceptor, as in Go. Work client-go detaches from the caller runs on `store.Ctx()` or `context.Background()`, without the interceptor (secondary commits, async commit's secondaries, keep-alive, cleanup, pipelined resolution); client-rust ran the transaction's chain on all of them until client-rust af29baa (`Committer::detached`).
    - Scan pages (`capture_scan_pages`): feeds ANALYZE's per-region fragments (`cluster_analyze.rs`). Go has no KV-scan region list; its ANALYZE gets regions from coprocessor tasks. A structural mismatch of ANALYZE's own; until it moves to the coprocessor, an interceptor in TiDB records the pages.
    - Test-only: `observe_detached_commits`, `fill_receipt` (a no-op in production), `capture_attempts_for_test`, `last_lock_primary` (Go: `CommitterProbe`). Go's tests observe these through failpoints, probes and mock clients. Home: test interceptors over the native store, the receipt fields and `AcquiredLocks.primary_key` deleted.
    - Lock resolution for coprocessor reads (`new_resolver`): client-rust's `LockResolver` already works over any `PdClient`.
    - `ReplicaRouting`, region routing and `get_timestamp`: answered from `tidb-txnkv`'s region cache, store liveness and process oracle. `PdRpcClient` implements all of them over its own cache, and the store's oracle (Step 5a) is client-go's `KVStore.oracle`. The embedded store's PD client lacks `ReplicaRouting`.
    - Order: Go has one `RegionCache` per `KVStore`, shared by transactions and the coprocessor. 6e moved the coprocessor onto client-rust's cache first (done); 6c now completes the unification.
    - Staging (2026-10-19):
      - 6c-0, the test-only extensions go: `observe_detached_commits`, `fill_receipt`, `capture_attempts_for_test`, `last_lock_primary` (and the receipt fields only tests read). Their tests observe the same facts through an `RpcInterceptor` on the engine's transaction, as Go's tests use mock clients and probes.
      - 6c-1, per-call context as Go's `ctx`: TiDB's `RpcInterceptor` (deadline, cancellation, KV-exec counter) on the engine's transaction, replacing `ClientPd::set_call`.
      - 6c-2, visibility gating and scan pages without the trace: the store's post-read validator (client-rust 7d59cff) replaces `take_read_trace` gating; ANALYZE's per-region fragments record pages through an interceptor until ANALYZE moves to coprocessor fragments.
      - 6c-3, `ClientTransaction` over `Transaction<PdC>` on the node's store (the opener carries the typed store; its `C, L, P` generics become the store's `PdC`), embedded node first, then TiKV with a playground run. Lock resolution for transactions is then the store's `ResolveLocksContext` (6d).
  - [ ] 6d, lock resolution: one `ResolveLocksContext` on the native store, shared by transactions and coprocessor reads.
  - [ ] 6e, coprocessor reads: `DirectUnaryQueryTransport` and the region tasks on client-rust's region cache (`batch_locate_key_ranges`), plan retry and `KvClient` dispatch. Go's `pkg/store/copr` keeps the coprocessor cache and paging on TiDB's side. Gaps: streaming coprocessor, the async-completion and `CopMeetLock` hooks.
    Survey (2026-10-19). Today: `DirectUnaryQueryTransport` (`tidb-distsql/src/cop_paging/direct_unary_query_transport.rs`) locates regions with `tidb-txnkv`'s cache (`batch_locate_key_ranges`), builds tasks (`region_task.rs`, Go `SplitKeyRangesByLocations`), sends over BatchCommands or unary, and classifies region errors with that cache's `on_region_error` (retry selector / re-route / rebuild ranges). Store batching is built but refused, buckets are dropped, and lock errors already resolve through client-rust's `LockResolver`. On TiKV, region split and the GC worker already use the native store's cache, so production runs two caches. client-rust has the coprocessor request types (unary, BatchCommands, region-error and lock decoding), but `coprocessor::Request` is not `Shardable`, its region-error handling is `pub(crate)`, and it has no `RegionRequestSender`.
    Two structural gaps come first:
    - [x] (2026-10-19) 6e-1, client-go's `RegionCache` over any PD client. client-rust 332cda0, f377e05, 69f2f5f: `PdRpcClient<KvC, P>` takes any `RetryClientTrait` as `P` (client-go's `pd.Client`), `new_with_pd_client(config, kv_connect, pd, enable_codec)` builds one over a PD client the caller holds (with `config.keyspace`, its API V2 codec, as `NewTestKeyspaceTiKVStore`), and `KvConnect` is exported. tidb-unistore's `pd::PdClient` is now Go's `pdClient` as a `RetryClientTrait` (regions with encoded bounds, decoded by client-rust's codec as client-go's `CodecPDClient` does), `rpc::RpcConnect` is Go's `RPCClient` as the store connector, and `mock::store_pd_client` builds `StorePdClient = PdRpcClient<RpcConnect, PdClient>`: the embedded store runs under client-rust's own region cache, store clients and routing, as Go's runs under client-go's. The node's store, the GC suite and the store tests use it.
      Two client-rust bugs surfaced once the embedded store answered stale epochs to a real cache (both fixed, with regression tests): a scan's region error was swallowed by the snapshot-visibility layer (`region_error()` takes it; the scan returned an empty region instead of retrying; client-rust 5838170, `has_region_error`), which also reached TiDB's transaction scans through the bridge; and the GC lock scan backed off without applying the region error to the cache, so it located the same stale region until its backoff ran out (client-rust 8e7336c, as client-go's `SendReq` does).
    - [x] (2026-10-19) 6e-2, client-go's `RegionRequestSender.SendReq` in client-rust (02f9b54): `region_request::send_req(pd, keyspace, request, backoffer)` runs client-rust's region sender (replica selection, send failure, region-error classification) over a request pinned to the region its context names. `region_pinned!` gives such a request one shard, that region, answering client-go's pseudo `EpochNotMatch` when the cache no longer holds it at that epoch; `SenderBackoffer` returns terminal region errors to the caller instead of re-planning, as `onRegionError` answers `retry = false`. Coprocessor requests are region-pinned. Next users: the coprocessor transport, `doGCForRegion` (Step 5d), and the region-addressed requests that today go through `single_region_with_store` with no retry (resolve lock, scan lock).
    Then, with the sender in place, Go's division of `pkg/store/copr`: the coprocessor keeps building tasks (`buildCopTasks` over `SplitKeyRangesByLocations`), rebuilding them on a region error (`handleCopResponse`), resolving locks, paging and the coprocessor cache; sending is `SendReqCtx`. In `DirectUnaryQueryTransport` the sending half -- `dispatch_attempt`, `settle_dispatch`, `recover_transport_failure`, the sender-level arms of `recover_region_error` (busy, not-leader, store-not-match), route observations and selector state -- is a second client-go sender.
    - [x] (2026-10-19) 6e-3 and 6e-4 on the embedded node, together (tasks located with one cache and sent through another would disagree on region versions). `cop_paging::NativeCopTransport` is Go's `pkg/store/copr` read loop over a `NativeCopStore` -- the node's client-rust `TikvStore`: tasks are built over the store's region cache, sent with `KvStore::send_req` (client-rust 80221a4) charging the task's `Backoffer` (Go `copNextMaxBackoff`), a returned region error rebuilds the task's remaining ranges after `BoRegionMiss` (`handleCopResponse`), and a locked response resolves through `KvStore::resolve_locks_with_opts` (3f34dd6, Go `handleLockErr`) with the snapshot's resolved/committed locks stamped on later sends. Paging, the coprocessor cache and the task state machine (`CopReadTaskRuntime`) are the existing ones. The embedded node reads through it; its synchronous-only `DirectUnaryQueryTransport` workaround is gone. Narrowings (in the module doc): no buckets or store batching yet; the answering store's address is not reported to per-store runtime stats or TopSQL.
    - [ ] 6e-4b, the native transport's remaining gaps before TiKV. Done: concurrent tasks on Go's worker pool (`CopWorkerSource`, the snapshot's lock sets shared by its tasks), and client-rust's exhausted backoffs reaching the statement as Go's storage errors (`client_backoff_kind`, Go `ToTiDBErr`). Done too: the answering store's address (client-rust e38379e `send_req_ctx`, client-go `rpcCtx.Addr`) in per-task runtime stats and TopSQL's KV-exec counter. Open: the counter is marked once per task send; Go's `KvExecCounter.RPCInterceptor` marks every RPC, retries included, which needs the caller's interceptor carried into `send_req`.
    - [x] (2026-10-19) 6e-5, TiKV: the production node's coprocessor reads go through `NativeCopTransport` over the read authority's `TikvStore<PdRpcClient>` (`ProductionReadSessionFactory`). Playground acceptance (PD v9 nightly, tikv-slim): a 3000-row table split into 16 regions (record and index) answers count/sum/min/max, grouped counts, an index lookup with order and limit, a descending limit, a range count and a `LIKE` filter exactly. The run found two client-rust bugs on the region-split path, fixed with tests (f262b43 `GetOperator` returns PD's header error in the response; 2228f58 the jitter backoff caps its growing delay). Open, and the same with the old transport: `EXPLAIN ANALYZE` on the cluster node shows no `cop_task` summary and `N/A` / `actRows 0` on the TiKV operators.
    - [ ] 6e-6, buckets and store batching (`StoreBatchCoprocessor`), which client-go's tasks use and this path refuses today; task counts in tests change. Risks: synchronous `CopIterator` workers over async client-rust (no `block_on` on runtime threads), per-region backoff budgets, region-error parity between the sender and coprocessor levels, `tidb_proto` / `tikv_client::proto` conversion.
    - [x] (2026-10-19) 6e-7, ANALYZE's sampling and index requests through the native transport: `NativeCopStore` lives with `TikvStore` in `tidb-txnkv` (`driver::cop_store`), the transaction opener carries the node's store (`with_cop_store` / `cop_store`, Go's copr over the `KVStore`), and ANALYZE builds its transport from it. No coprocessor read uses `DirectUnaryQueryTransport` outside its own tests now.
  - [ ] 6f, MPP and peer RPC on client-rust's `DispatchTaskRequest`, `MppStreamRequest`, `BatchCoprocessorStreamRequest` and `KvStore::send_request`. The embedded store's RPC client then needs Go's MPP and batch-coprocessor arms.
  - [ ] 6g, delete the duplicate stack: `region/*`, `rpc/*`, `read_runtime`, `pd_loader`, `pd_capability`, `client_bridge`, `transaction/command_client`, the lock adapters, and the embedded store's `InProcessClient` / `InProcessRegionLoader`. The rest of `tidb-txnkv` is TiDB's `pkg/kv` and `pkg/store/driver` (the `tikvTxn` facade), which stays.

## Surprises & Discoveries

- Observation (survey of `tidb-txnkv`, 120 files, 43k lines): production's transactions and lock resolution already run client-rust's engine, but they reach the store through `tidb-txnkv`'s own region cache and BatchCommands transport (`driver/client_bridge.rs`: `ClientPd` / `ClientKv`). Coprocessor and MPP reads use no client-rust code. The GC worker and region split each open a separate native `KvStore`. About half of the crate is TiDB's `pkg/kv` / `pkg/store/driver` contracts (handles, keys, options, the transaction facade), which are not client-go and stay.

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
