# root `tikv` source-artifact audit

This is the atomic completion receipt for client-go's root `tikv` package at pinned commit `52c1e76cec993571493c81de442bcbef90cdc106`. The primary Rust owner is the public `tikv_client::tikv` facade in `src/tikv.rs`; native owners remain in `src/{pd,region_cache,request,store}`, `src/transaction`, and the already-complete foundational modules. Validation uses `nightly-2026-08-22`. This receipt does not promote root `txnkv`, `internal/mockstore/mocktikv`, the external integration-test package, or final live-cluster differential validation.

## Complete source inventory

The claim contains exactly 17 Go artifacts and 3,895 lines: 12 ordinary production files, three always-built production test/support files, one package test, and one goleak harness.

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `tikv/backoff.go` | 101 | `a57c4aaed57dca7754b27d9c39690f49aa839ef2325ea08debc12a9a4b3ffd76` | root retry aliases/config constructors over `src/retry.rs` and cancellation-aware native helpers |
| `tikv/client.go` | 73 | `1ac9b0825826a282d51505d96ea3802836c92cf714bd8a32db092e1f15dbd8ca` | `Config`, `SecurityManager`, `TikvConnect`, `KvClient`, event listener, codec selection, and root timeout constants |
| `tikv/compatible_txn_safe_point_loader.go` | 139 | `5893b84c752ed93ec4368ec793c82d906bb3eb0d6069733d75620581c44a9c81` | lazy `EtcdSafePointKv`, keyspace-level path selection, sticky Unimplemented fallback, and runtime close ownership |
| `tikv/failpoint_export.go` | 23 | `593d9250cdeda7add0c70bdeef6b571a9a745080529a2adde2efa061dfc794bd` | root `enable_failpoints` re-export and completed `util` failpoint gate |
| `tikv/gc.go` | 423 | `adcf619ffb4e9a1451357771cc52c6951f751f07c0172aed93bc59dfd5a53957` | root GC controller, range-task lock cleanup, legacy GC-safe-point update, unsafe all-TiKV-store destruction, and failure metrics |
| `tikv/interface.go` | 91 | `809150bc0d90aff12a6e051f073812474aa9bdbc35b3a413b2d4c002422d0e74` | native `Storage`, concrete `KvStore`, `Deref<TransactionClient>`, typed PD/cache/transport owners, and lifecycle methods |
| `tikv/kv.go` | 1,093 | `800341f8c68f23332860bc5e705a7ce6d2458378fb4fda5decd863bfec16eb14` | `KvStore`, `StoreRuntime`, visibility cache, safe-TS updater, constructors/options, PD/TiKV metadata, lifecycle, transaction client, and resource-control facade |
| `tikv/kv_test.go` | 417 | `d1ad72be034694cac21751879755fe104caa2f0b70f3325650b90c0d7d7dff3a` | source-named root tests in `src/tikv.rs`, plus focused request/region/lock tests named below |
| `tikv/logutil.go` | 32 | `4a20745262ca4d49e24f230503ec2548fcd9790513fded671309f6af521a1aef` | typed `TraceContext`/`with_log_context`; no mutable untyped context key in Rust |
| `tikv/main_test.go` | 29 | `e1b74c4a1de19a3bae5a26dc83e5c69e3b1904c7e42c2adc3d1d93ec3a3a7f09` | joined runtime/transport owners, explicit close tests, and both complete library suites |
| `tikv/pool.go` | 45 | `7159539108770daffedbf748c02e85cab1ddbbf32d9b4734b6ea2662c0fca9e4` | bounded Tokio `Spool`, reusable async `Pool`, and explicit runtime task ownership |
| `tikv/region.go` | 281 | `67f2fe61e25e1d2c90c8f75c533aff0e92f5f3d53f3a28e29c6abc0b2d17a640` | root region/store/codec/filter aliases and constructors over completed `internal/locate`, API-codec, and transport owners |
| `tikv/safepoint.go` | 243 | `b1182a19c3093deaca1aba6d28ab39d046581bcb94676020d36e797960ec6820` | `SafePointKv`, source-shaped no-op-close mock, namespaced etcd v3 Tonic KV, parser/save/load helpers, and exact timeouts |
| `tikv/split_region.go` | 417 | `dc1e0c9698507a74402ae25922809f04539f006fad9c5f8cfb86498727b14eec` | grouped/concurrent split, shared cumulative retry ownership, lock-aware txn-file mode, scatter, wait, and status checking |
| `tikv/test_probe.go` | 267 | `32fd5442f72aefba7b114ec424bdb20ffd5dbaad311b1053d090e214f60ce93f` | typed root/cache/lock/safe-point/safe-TS accessors and completed transaction/snapshot/config probe owners |
| `tikv/test_util.go` | 153 | `b90ca5df83ee0fd90be8538291d7c73d021fa01ec34885c42db7651e69e9b2c7` | generic typed construction/rollback helpers, mock PD/TiKV clients, captured requests, and deterministic fixtures |
| `tikv/unionstore_export.go` | 68 | `741bf6d99202642b5b57b8a6a2c8f727d631c82f0ef5f05b621faaab17326e08` | public union-store aliases and reusable ART/RBT-backed `MemDB` facades |

There is no package `doc.go`, non-Go fixture, package-specific build file, generated source/input, benchmark, example test, `OWNERS`, platform variant, or build tag. Every artifact is always selected by the pinned source build.

## Production behavior and native integration decisions

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| construction and identity | `KvStore::{new,new_with_config}` builds the native transaction client, retains the canonical keyspace metadata, loads transaction safe point before returning, rolls back every partially built owner on failure, starts exactly two owned workers, exposes cluster ID, and uses source UUID `tikv-{cluster_id}`. `Config` replaces variadic untyped transport/store options. |
| visibility and compatible safe point | `TxnSafePointCache` retains the 100-second cache interval and 10-second CPU inaccuracy bound. Reads reject stale PD state or a timestamp below the transaction safe point with source-typed errors. Modern `GetGCState` is keyspace-scoped; only gRPC/typed Unimplemented switches permanently to the lazy etcd loader. Unified and keyspace-level paths, empty/invalid values, five-second read timeout, load-result metrics, and loader close are accounted for. |
| safe-point KV | The mock is synchronized, returns empty for missing keys, filters prefixes, and—like source—continues working after no-op `close`. The etcd owner applies namespaces, strips them from returned keys, uses five-second Get/Put and fifteen-second prefix deadlines, and closes its channel. Connection identity remains explicit runtime state rather than a private interface method. |
| safe TS | The two-second worker first accepts a valid global PD resolved TS, then falls back to per-store PD values and `StoreSafeTS`. TiFlash uses `peer_address`; ordinary requests retain the normal address. Store values never regress and skip MaxUint64; missing and zero values preserve source minimum rules; store-derived scopes may fall while the PD global fast path is monotonic. Zone aggregation and exact success/skip/fail/gap metric labels are present. |
| GC | Default concurrency is eight. Root GC advances the transaction safe point through the null-keyspace controller, uses the lower actual result, resolves every lock range through the completed range-task runner with `GCScanLockLimit = 2048`, stops on resolution failure, then calls the source legacy global GC-safe-point update and returns its actual value. No live control-plane operation is executed by the tests. |
| unsafe destroy range | Every non-tombstone ordinary TiKV store is targeted concurrently; TiFlash write/compute nodes are excluded while non-tombstone offline stores remain included. Requests use the source five-minute deadline, API-v2 range coding, retry transport failures, count `get_stores`/`send` failures, wait for all stores, and aggregate every terminal store error. Tests use only typed mocks. |
| split/scatter | Keys are sorted/deduplicated, region-start keys are skipped, and batches are capped at 2,048. Region batches run concurrently with source fork/last-finished accounting and one cumulative `min(keys*20s,120s)` owner. Region errors invalidate/re-locate/retry; legacy mode returns key errors unchanged; txn-file mode expands shared holders, resolves locks, retries, and never scatters. The unsplit right region is removed from returned IDs, successful sibling IDs survive later failures, scatter shares the operation budget/group, and PD operator polling retains source retry budgets. |
| transaction facade | `Deref<TransactionClient>` supplies begin/snapshot/timestamp/delete-range/request/cache/lock operations without duplicating concrete owners. `TransactionOptions::{scope,start_timestamp,pipelined}` maps root options; root helpers provide exact default `128/8/0.0` and parameterized pipelined values. `MAX_TXN_TIME_USE` is public and used by commit validation. |
| region/client/log/resource facade | Public aliases route to the completed typed codec, cache, selector, sender diagnostics, filters, store metadata, transport, resource-control, failpoint, and retry implementations. Every type required to implement the public injected `KvClient`/`PdClient` path is nameable through this facade, including `Store`; the ordinary downstream regression implements both traits and constructs a generic transaction without `internal-tests`. Rust cancellation and `TraceContext` replace untyped mutable `context.Context` keys; `Config`, `SecurityManager`, and `TikvConnect` replace `ClientOpt`. These native boundaries preserve observable protocol behavior without recreating Go's alias-only wrappers. |
| pool and lifecycle | `StoreRuntime` directly owns cancellation and joins both workers before closing compatible etcd and transport owners. `Spool` provides bounded nonblocking admission and source-compatible `Run` success/closed behavior for external users. The existing completed `internal/client` close test proves region-cache shutdown precedes PD retirement; the root close test proves workers stop before transport. |
| union-store exports | Getter/iterator/MemDB/buffer/checkpoint/metrics/snapshot types are public and reusable. Rust's source-default ART now implements the test-only `remove_from_buffer` physical removal instead of panicking, matching client-go's functional RBT `MemBuffer` contract; the facade regression proves the key and handle disappear. Pipelined MemDB retains its source-declared unsupported panic. |

## Original tests and support artifacts

`tikv/kv_test.go` contains the `TestKV` suite entry, 12 suite methods, and one top-level lifecycle test. Every declaration has an explicit Rust assertion owner:

| client-go test | Rust evidence |
| --- | --- |
| `TestSplitRegionsPreservesLegacyKeyErrorBehavior` | `tikv::tests::split_regions_preserves_legacy_key_error_behavior` |
| `TestSplitTxnFileRegionsResolvesLockAndRetries` | `tikv::tests::split_txn_file_regions_resolves_locks_and_retries` |
| `TestSplitTxnFileRegionsSplitsWithoutScattering` | `tikv::tests::split_txn_file_regions_never_scatters` |
| `TestMinSafeTsFromStores` | `tikv::tests::min_safe_ts_from_stores` |
| `TestHandleSplitRegionKeyErrorsExpandsSharedLockHolders` | `tikv::tests::split_key_errors_expand_shared_lock_holders` |
| `TestMinSafeTsFromStoresWithAllZeros` | `tikv::tests::min_safe_ts_from_stores_with_all_zeros` |
| `TestMinSafeTsFromStoresWithSomeZeros` | `tikv::tests::min_safe_ts_from_stores_with_some_zeros` |
| `TestMinSafeTsFromPD` | `tikv::tests::min_safe_ts_from_pd` |
| `TestMinSafeTsFromPDByStores` | `tikv::tests::min_safe_ts_from_pd_by_stores` |
| `TestMinSafeTsFromMixed1` | `tikv::tests::min_safe_ts_from_mixed_sources_uses_store_fallback_for_zero` |
| `TestMinSafeTsFromMixed2` | `tikv::tests::min_safe_ts_from_mixed_sources_uses_store_fallback_for_max` |
| `TestErrorHalfwayInNewKVStore` | `tikv::tests::constructor_failure_closes_the_partial_client_owner` |
| `TestKVStoreCloseCheckRegionCacheClosedBeforePDClose` | root `close_stops_runtime_workers_before_the_transport_owner` plus completed `internal/client` close-order regression |

The remaining root tests close production branches not directly isolated by `kv_test.go`: GC call order/lower actual point/error stop, safe-point mock/parser/compatible switch/path, visibility errors, split response retry/partial IDs, option defaults, and spool close. The unsafe-destroy merger and ART removal have focused tests in their native modules. `main_test.go`'s goleak contract maps to explicit cancel-and-join ownership, close-order tests, doctests, and both complete library suites rather than detached runtime work.

`test_probe.go` and `test_util.go` do not define production behavior independently. Their observable accesses are assigned to root hidden accessors, completed lock/snapshot/transaction/config probes, mock clients, construction rollback, request capture, and the source-named tests above. Rust uses typed ownership instead of unchecked field replacement. `failpoint_export.go` maps to the completed failpoint gate and deterministic dependency injection; no failpoint is required to test production control-plane calls.

## Dependencies and consumers

Every production dependency is complete: `config`, `config/retry`, `error`, `internal/{apicodec,client,kvrpc,latch,locate,resourcecontrol,unionstore}`, `kv`, `metrics`, `oracle/oracles`, `tikvrpc`, `txnkv/{rangetask,transaction,txnlock,txnsnapshot}`, and `util`. The directly used pinned PD/kvproto GC, store, operator, and split messages are present in the already-regenerated protocol bindings.

Exact quoted-import matching finds 35 direct external Go files: one GC-worker example, 31 files in `integration_tests`, two `rawkv` files, and root `txnkv/client.go`. The example maps to root `gc`; RawKV and the completed transaction/lock/snapshot packages consume the same native cache/transport owners; `txnkv/client.go` remains a separate root-package claim. The 31 integration consumers are assigned across the existing raw, transaction, lock, snapshot, split, safe-point, delete-range, resource, and optional-live gates. This root receipt establishes their complete public dependency surface but does not promote the external integration-test package or replace the final real-cluster differential milestone.

## Validation contract

Completion requires exact pinned hashes/line counts and 17/17 artifact identity; the 23-test root matrix; focused unsafe-destroy, ART, and ordinary downstream injected-client regressions; both complete default and all-feature library suites; all-target/all-feature compilation; Clippy; rustdoc and doctests; rustfmt/diff checks; and source inventory/test-name reconciliation on `nightly-2026-08-22-aarch64-apple-darwin`. Production GC, unsafe destruction, and split/scatter calls are validated only through pure state machines and mock/loopback transports. They are deliberately not executed against a live cluster during this package gate.

The final package gate mechanically verifies 17 artifacts, 3,895 lines, and all 14 test declarations (the suite entry, 12 methods, and lifecycle test). The root matrix passes 23/23 tests; `cargo test --no-default-features --test public_injected_client_tests` passes as an ordinary external crate after failing before the `Store` export with `E0603`; both complete unit matrices pass 757 and 750 tests respectively with one configured skip in each; all targets compile; strict Clippy and rustdoc succeed; and all 51 doctests pass. Rustfmt and whitespace checks are clean.
