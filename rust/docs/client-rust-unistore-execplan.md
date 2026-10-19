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
- [ ] Step 3, tidb-unistore: Go unistore's request context (`newRequestCtx`: region id and epoch checks, region errors); the missing server handlers (`KvScanLock`, `KvGC` / `UpdateSafePoint`, `KvDeleteRange`, `UnsafeDestroyRange` / `DeleteFileInRange`, `KvCleanup`, split region); the `MvccStore` primitives behind them.
- [ ] Step 4, tidb-unistore: Go's `MockPD` (region and store lookups, TSO) with the `gcStatesManagerSimulator` (txn and GC safe points, service safe points, barriers) and keyspaces, as a client-rust `PdClient`; Go's `RPCClient` as a client-rust `KvClient`.
- [ ] Step 5, server: the GC adapter generic over the client's `KvStore`; Go's store-backed GC tests (listed in `gcworker-execplan.md`) ported onto the embedded store.
- [ ] Step 6: move TiDB paths from `tidb-txnkv` onto client-rust one at a time, each with its tests and a gate.

## Surprises & Discoveries

- Observation: Go's embedded store does not run the GC worker (`mockStorage.StartGCWorker` is a no-op; only `tikvStore.StartGCWorker` builds one). Go's GC tests construct the worker directly over a mock store.
- Observation: client-rust's own mocktikv (the counterpart of client-go's) panics on `UnsafeDestroyRange`, as client-go's does, so it cannot stand in for Go's unistore in the delete-range tests.

## Decision Log

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
- The GC worker runs a whole job over the embedded store in a TiDB test, with a dropped table's keys gone afterwards.
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
