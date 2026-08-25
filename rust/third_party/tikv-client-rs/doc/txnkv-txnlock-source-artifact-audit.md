# `txnkv/txnlock` source-artifact audit

This is the atomic completion receipt for client-go package `txnkv/txnlock`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The Rust implementation is in the reusable `tikv-client` crate and is validated with `nightly-2026-08-22`.

## Complete source inventory

`git ls-tree -r --name-only 52c1e76cec993571493c81de442bcbef90cdc106 txnkv/txnlock` contains exactly six files and 2,144 lines:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `lock.go` | 43 | `4157ffa6a50f11540424c847e57f2922a4b112829e493a71f769f55697734694` | plural/singular key-error extraction in `src/transaction/lock.rs` |
| `lock_resolver.go` | 1,724 | `b5205d9d0f77e8cc95543e8e93b25e09fe27a41e10322ca78468caa18c0f766d` | `src/transaction/lock.rs`, resolver request/result adapters in `requests.rs` and `request/plan.rs`, metrics in `src/stats.rs`, client ownership in `client.rs` |
| `lock_test.go` | 63 | `7061b234c5feab7f1bba1b2479ff36517702a1c7a3e65e32261bc85dc6c40bf1` | source-derived extraction tests in `src/transaction/lock.rs` |
| `lock_resolver_test.go` | 160 | `db4a4a1786abacef8cc3965b587bd628103e3bd541e2c55e33fa634a3c054ddc` | cache and bounded-pool regressions in `src/transaction/lock.rs` |
| `main_test.go` | 25 | `5c53aa90d1afd98d5dd5ad42eee7897264c8b9abffda6a3a6632473748d39cfd` | explicit resolver close/cancel/join tests and awaited full-suite gates |
| `test_probe.go` | 129 | `380a38d0f35589f567edd2a052d702aa7d9b3b498618f919cc8f8e46844c354d` | package-private constructors/state access plus public `ResolvingLock` diagnostics |

There is no package `doc.go`, platform/build-tag variant, generated source or input, fixture, benchmark/example, package build file, or non-Go runtime artifact. Kvproto messages are generated dependencies already owned by the protocol-input audit, not generated artifacts of this package.

## Production mapping

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| `ExtractLockFromKeyErr`, `ExtractLocksFromKeyErr`, `Lock`, `NewLock`, `IsPessimistic`, `IsShared`, formatting | Singular extraction preserves a shared wrapper; plural extraction expands holder locks in wire order. Exclusive locks are preserved, non-lock key errors remain typed errors, concrete shared-pessimistic holders use pessimistic rollback, and direct shared wrappers are rejected. Native `LockInfo` ownership avoids duplicating generated fields. |
| `TxnStatus`, determined-status predicates and cache | `TransactionStatus` retains live/committed/rolled-back state, action, expiration, primary metadata, commit TS, and TTL. Only determined outcomes enter a source-sized 2,048-entry FIFO; duplicate equal outcomes are idempotent and conflicting final outcomes fail immediately. Cached async-commit outcomes bypass secondary recovery. |
| `ResolveLocksWithOpts`, `ResolveLocks`, `ResolveLocksForRead`, `txnExpireTime` | The resolver checks every encountered transaction, handles txn-not-found expiry and pessimistic primary mismatch, recovers async commit or forced 2PC, returns the minimum non-negative remaining TTL, and classifies read-through versus ignorable transaction IDs. Snapshot contexts carry cumulative `resolved_locks` and `committed_locks` hints on retries. |
| Ordinary and lite `ResolveLock` | The configured source default threshold of 512 selects exact-key lite cleanup; an already-checked lite primary is skipped. Multi-key lite cleanup is batched once per current region. Non-lite resolution deduplicates cleaned regions and enables TiKV-side async resolution only for NextGen read cleanup. Region, leader, transport, and key failures preserve retry/error ownership. |
| Pessimistic and batch GC cleanup | Pessimistic locks use `PessimisticRollback` after status lookup, including shared-pessimistic holders and invalid-primary rollback. Batch GC forces status, recovers async commit, retains txn-file flags, suppresses empty batch RPCs, records cleaned regions, and sends the 20-second write execution limit. |
| Async-commit secondary checks and recovery | Secondary keys are grouped by region, checked concurrently, merged with missing-lock/commit-TS consistency checks, and fall back to forced 2PC for non-async locks. Recovery resolves the exact returned secondary-plus-primary key set per current region rather than broad whole-region scans. |
| `asyncResolveTaskPool` and process semaphore | Every read cleanup, secondary check, outer async-commit cleanup, and per-region recovery task uses one process-wide 10,000-permit nonblocking pool. Saturation executes the same future inline and records the source fallback label. Running gauges begin only when tasks run and are balanced by cancellation-safe drop guards. Nested fanout reuses the same pool. |
| `NewLockResolver`, `Close`, `KVStore` ownership | `ResolveLocksContext` is the shared resolver state carried by every transaction-client clone, snapshot, transaction, committer, and request plan. `TransactionClient::close` first cancels and joins resolver tasks, then closes shared transport. `LockResolver::close` is available to direct owners. This is the native owner counterpart of `KVStore.lockResolver` and `KVStore.Close`. |
| Resolving-lock observer | Stable source tokens support record, update, done, and flattened snapshots. `ResolvingLocksGuard` spans the complete retry future and removes state on success, error, or Rust future cancellation. Snapshot, prewrite, pessimistic, transaction-file, split, and pipelined-flush owners share the client resolver; a dedicated pipelined regression proves lifetime and cancellation. |
| Request context, metrics, detail, logging, tracing/failpoints | Resolver RPCs retain keyspace, request source, interceptor, resource-group, resource-control, RU details, txn-file state, and the source 20-second write duration. Action counters execute at physical-shard/retry boundaries; async gauges/fallbacks use every source label. Rust futures and deterministic hooks replace Go trace/context/failpoint plumbing without removing observable timing or error branches. |

The source `storage` interface maps to the existing `PdClient`, region cache, store client, and request-plan boundaries. Rust's type-safe request plans replace `SendReq` command assertions, while `SecondaryLocksStatus` is the native merge state corresponding to `asyncResolveData`. `Cancellation`, owned futures, and `oneshot` results replace context cancellation, goroutine pools, channels, and wait groups.

The source resolver intentionally returns TTL and lock classification; the caller owns its retry class. Snapshot Get/BatchGet/Scan already consume that output through the cumulative `txnLockFast` backoffer. Remaining scanner/callback work is charged to the separate incomplete `txnkv/txnsnapshot` package, not hidden in this receipt. Transaction prewrite and pipelined callers use their source `txnLock` policy and share the resolver observer.

## Original test and support mapping

Mechanical enumeration finds five ordinary test declarations plus `TestMain`, and no benchmark or example:

| Source declaration/support | Rust evidence |
| --- | --- |
| `TestExtractLocksFromKeyErrExpandsSharedLockHolders` | `source_key_error_lock_extraction_expands_shared_holders` |
| `TestExtractLocksFromKeyErrPreservesExclusiveLock` | `source_key_error_lock_extraction_expands_shared_holders` exclusive branch |
| `TestExtractLocksFromKeyErrReturnsKeyError` | `source_key_error_lock_extraction_expands_shared_holders` typed-error branch |
| `TestLockResolverCache` | `source_cached_async_commit_status_skips_secondary_recovery` and `source_resolved_status_cache_is_fifo_and_bounded` |
| `TestTryAsyncResolve` | `source_async_resolve_pool_releases_capacity_and_falls_back` and `source_read_cleanup_metrics_track_running_tasks_and_fallbacks`; these cover custom capacity, admission, saturation rejection/inline fallback, permit reuse, gauge balance, close cancellation/join, and post-close rejection |
| `TestMain` goleak harness | resolver close cancels and joins a deliberately blocked task; focused and full library suites finish with no retained resolver tasks |
| `test_probe.go` | direct package-private cache/pool construction, exact action/gauge accessors, deterministic mock dispatch hooks, `ResolvingLock`, and source-named focused tests cover every exposed probe capability |

Additional source-derived tests cover shared-wrapper misuse, resolving token lifetime, empty batch suppression, pessimistic rollback, region retries, lite thresholds and key scoping, detached read-through, per-region batching, NextGen async requests, TTL minima, committed/live status handling, async-commit recovery, resource/RU propagation, and pipelined observer ownership.

## Consumer and integration audit

Every direct pinned importer was inspected and assigned:

- Store owners are `tikv/interface.go`, `kv.go`, `gc.go`, `split_region.go`, `test_probe.go`, and `kv_test.go`. Rust integrates shared construction/close and GC/split request behavior here; the broader root `tikv` package retains its own incomplete inventory.
- Transaction owners are `txnkv/transaction/2pc.go`, `prewrite.go`, `pessimistic.go`, `pipelined_flush.go`, `txn_file.go`, `test_util.go`, and `txn_file_test.go`. The completed transaction receipt owns caller policy while this receipt owns resolver state; the pipelined observer seam has direct regression evidence.
- Snapshot owners are `txnkv/txnsnapshot/client_helper.go`, `snapshot.go`, and `scan.go`. Read-through hints, detached cleanup, and `txnLockFast` Get/BatchGet waits are integrated; the package's remaining scanner/iterator receipt stays separate.
- `txnkv/lock_export.go` is a root re-export façade and remains on the separate `txnkv` row.
- Direct integration importers are `integration_tests/2pc_test.go`, `async_commit_test.go`, `lock_test.go`, `pipelined_memdb_test.go`, `shared_lock_test.go`, and `snapshot_fail_test.go`. Their real-cluster orchestration remains a final repository/high-level-package gate rather than an uncounted local artifact.

Completed `config/retry`, `internal/client`, `internal/locate`, `internal/apicodec`, `internal/resourcecontrol`, `tikvrpc`, `txnkv/rangetask`, and `txnkv/transaction` dependencies provide the retry, transport, routing, codec, resource, range, and caller behavior used here. Completing `txnkv/txnlock` does not promote root `tikv`, `txnkv`, `txnkv/txnsnapshot`, or integration packages.

## Validation boundary

Final validation on `nightly-2026-08-22` used the exact batch code:

- `cargo +nightly-2026-08-22 test -p tikv-client --lib transaction:: --quiet`: 201 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 588 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 588 passed.
- `cargo +nightly-2026-08-22 check -p tikv-client --all-targets --all-features`: passed with the existing warning backlog.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short`: passed with the existing warning backlog.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features`: passed with the pre-existing `src/raw/client.rs` invalid-HTML warning.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 50 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.
- Mechanical source audit: exactly six artifacts, 2,144 lines, six declarations including `TestMain`, no benchmark/example, and all hashes match the receipt.

Package-local source tests require neither UniStore nor a live TiKV/PD cluster: the original cache test uses a nil store and the pool test uses in-process synchronization. Deterministic Rust PD/KV request mocks therefore cover the complete original local boundary; UniStore remains available for higher-level reusable integration tests when needed.

The host has no Go toolchain, so the pinned Go tests cannot run locally. End-to-end cross-client differential lock tests against one TiKV/PD cluster remain a repository completion gate owned by the high-level integration packages.
