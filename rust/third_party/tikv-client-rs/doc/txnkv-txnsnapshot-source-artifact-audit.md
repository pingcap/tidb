# `txnkv/txnsnapshot` source-artifact audit

This is the atomic completion receipt for client-go package `txnkv/txnsnapshot`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The Rust implementation is in the reusable `tikv-client` crate and is validated with `nightly-2026-08-22`.

## Complete source inventory

`git ls-tree -r --name-only 52c1e76cec993571493c81de442bcbef90cdc106 txnkv/txnsnapshot` contains exactly five files and 2,317 lines:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `client_helper.go` | 168 | `1c8fe55bca147827b9f1f0ae9c2f07480029657cfcb2a3350c925b288c4b04e6` | read-lock plans and cumulative retry ownership in `src/request/plan.rs`, `plan_builder.rs`, and `src/transaction/{lock,transaction}.rs` |
| `scan.go` | 360 | `624db2aaeff610817fcea812a2bb9a9f35b0fa383d93dea4702210f783bbb698` | `SnapshotIterator`, `SyncSnapshotIterator`, one-region scan sharding/collection, and pair-local lock recovery |
| `snapshot.go` | 1,414 | `19353fa5f4f0c50d1657b49047d270247fdd271f7c1c7610652381225322a456` | `src/transaction/{snapshot,sync_snapshot,transaction,buffer,snapshot_stats,requests}.rs`, request plans, request context, and metrics |
| `snapshot_async.go` | 301 | `1ed111669fb1bbf1b9f57e2f144170abd8a72d8129dec4cd90162f4d5ff0ed81` | native future-based concurrent multi-region dispatch, forked retry cancellation/merge, and response collectors in `src/request/plan.rs` |
| `test_probe.go` | 74 | `d130a33d8d18dc3cf4d2b7e85f5578dc67df787ce0a4aa4d48935f379d01fc17` | public probe constants, snapshot/runtime-stat diagnostics, deterministic mock dispatch, and source-named focused tests |

There is no package `doc.go`, colocated Go test, benchmark/example, fixture, generated source or input, package build file, build-tag/platform variant, or non-Go runtime artifact. Kvproto messages are generated dependencies of this package rather than its generated artifacts.

## Production mapping

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| `KVSnapshot`, constructor, timestamp reset, isolation/priority/task/cache/key-only/sample/replica/resource/scope/timeout/interceptor/request-source setters | Generic async `Snapshot<PdC>` and `SyncSnapshot<PdC>` retain all source state and apply it to every physical Get, BatchGet, BufferBatchGet, and Scan request and retry. Constructor/reset accept timestamps below `MaxInt64` plus the `MaxUint64` sentinel; reset clears cached reads and resolved-lock hints while preserving committed hints. Rust ownership provides the source mutex guarantee statically through mutable snapshot operations. |
| Request source | `RequestSource` emits exact `unknown`, `internal_<type>[_<explicit>]`, or `external_<type>[_<explicit>]` values; an internal flag with no type remains `unknown` and is not classified internal. Every snapshot plan writes the value into TiKV context, including scanner point reads and retries. |
| Point Get | Cache values and misses, commit-TS refetch/validation, configurable first-send timeout, replica adjustment, post-response visibility contract, runtime details/SLI, lock resolution, and region retry follow the source. Region and lock retry classes charge one cumulative source Backoffer. A latest-timestamp point read blocks on only the first encountered lock transaction and adds later transaction IDs to resolved hints without resolving them. |
| BatchGet and BufferBatchGet | Cached snapshot keys are removed before physical work; a fully cached request performs no BatchGet timing/region observation. Missing keys are grouped by current region and capped at 5,120 keys per shard. Pair-lock retries preserve clean pairs and resend only locked keys; response-level errors retry the incomplete request. The pipelined buffer tier has the source gate/error and does not cache results. Region cardinality is observed once from the initial grouping. |
| Async BatchGet | Rust futures and `JoinSet` are the native counterpart of callbacks/run loops. Multiple region shards execute concurrently under the shared limit, return results in source shard order, fork the cumulative backoffer, cancel siblings after the first error, await all owned tasks, and merge the last completed fork. Each initial concurrent shard increments the source `ok`, `region_error`, `lock_error`, or `other_error` counter exactly once; region and lock retries reuse the synchronous physical algorithm and do not increment it again. |
| Scanner | Construction eagerly fetches the first batch. Each refill selects exactly the first forward or last reverse boundary region, clamps request bounds to that region, crosses empty regions, advances with `NextKey(last)` or an exclusive reverse end, and shares one cumulative retry owner across region retries, response-lock waits, empty-region transitions, and pair-local point reads. Response-level locks discard incomplete pairs and retry; pair-level locks preserve clean pairs, recover an empty key from `LockInfo`, point-read only that key, skip a missing result, and advance from the raw response key/count. `is_valid` and `close` provide native iterator state. |
| Snapshot cache | Values, commit timestamps, and known misses are retained for Get/BatchGet only. Hit count is cumulative; size/content copies include misses; update/clean affect cache-only entries; timestamp reset clears entries while preserving the source byte counter; `MaxUint64` bypasses all cache fills. Byte accounting reproduces the source exactly, including cumulative key bytes on replacement and `CleanCache` charging an absent key, then applies the 10-GiB eviction boundary while preserving values supplied by the current fill. |
| Runtime stats | `SnapshotRuntimeStats` aggregates ordered Get/BatchGet/BufferBatchGet counts/durations, capped region errors and replica access, cumulative retry classes, resolve-lock time, V2/legacy time detail, complete `ScanDetailV2`, and `PoolTaskDetails`. Source scanners do not merge their physical Scan RPC/backoff stats; pair-local point Gets still merge their RPC/execution detail and lock-resolution time. Clone/merge are independent and formatting follows client-go duration, RPC, backoff, time, resolve-lock, scan/RocksDB/IA, and `read_pool` output. This corrects the earlier seed assumption: the pinned generated protobuf does contain `read_pool_task_details`. |
| Metrics | Get timing includes cache hits; BatchGet timing starts only when uncached keys require regional work. Internal/general command and snapshot-region histograms use source labels. Successful Get/BatchGet/BufferBatchGet execution detail feeds the source small-read versus throughput SLI boundary. |
| `kvstore.CheckVisibility` | `SnapshotVisibilityValidator` is the native form of the package-private store dependency. Get/BatchGet/BufferBatchGet and scanner reads invoke it only after a successful physical result and before caching/return. The concrete GC-state cache/updater remains owned by root package `tikv`; this receipt completes the snapshot call contract without falsely promoting that store package. Pre-dispatch PD timestamp-oracle validation remains a separate safety check. |
| `ClientHelper` | The completed `txnkv/txnlock`, `internal/locate`, `internal/client`, `tikvrpc`, and retry implementations provide read-through hints, resolving-lock observer lifetime, cumulative `txnLockFast`, region routing, replica fallback, resource/RU/interceptor context, deadlines, and typed response handling. Rust typed plans replace command assertions and direct/async send helpers. |

`ReplicaReadSeed` has no separate Rust state: the pinned source selector retains the seed in requests but does not consume it in the covered selection algorithm. Go contexts, goroutines, channels, wait groups, callbacks, and mutable request wrappers map to owned futures, cancellation, typed request plans, and closed request-kind callbacks. These are native integration decisions, not omitted behavioral branches.

## Test and support mapping

The package itself has no `*_test.go`; its only local support artifact is `test_probe.go`. Every probe is assigned:

| Probe | Rust evidence |
| --- | --- |
| Merge region stats, retry stats, execution detail, and format stats | direct `SnapshotRuntimeStats` collectors/accessors plus source-derived clone/merge/formatter, retry, region-error, execution-detail, SLI, and read-pool tests |
| Single-region BatchGet | typed shard collection and stale-region re-sharding tests, including the 5,120-key cap, response/pair-lock distinction, and locked-key-only retry |
| Construct scanner | generic mockable async/sync snapshot iterators and forward/reverse one-boundary-region tests |
| Scan/Get constants | public `DEFAULT_SCAN_BATCH_SIZE = 256` and `GET_MAX_BACKOFF_MS = 20_000` |

Source-derived Rust coverage additionally includes constructor/reset bounds, full request-source encoding, option and request-context propagation, cache hit/miss/content/10-GiB behavior, commit timestamps, latest-point lock omission, configurable/default retry deadlines, retry variables, read validation versus post-response visibility, one-region scanner routing across empty regions, response/pair lock recovery, forward/reverse continuation, runtime details, and async shard cancellation.

## Consumer and external-test audit

All ten direct pinned importers were inspected:

- `tikv/kv.go` constructs snapshots and provides the concrete `kvstore` dependency; `tikv/test_probe.go` re-exports the probe constants and snapshot wrapper. Root `tikv` retains its own GC-state/lifecycle receipt.
- `txnkv/snapshot_export.go` re-exports the snapshot, scanner, runtime-stat, isolation, and replica-adjuster APIs.
- `txnkv/transaction/txn.go`, `txn_test.go`, and `test_probe.go` own transaction delegation, pipelined buffer reads, direct single-region BatchGet, and scanner construction. The transaction package remains complete and this package supplies its snapshot dependency.
- `integration_tests/snapshot_test.go`, `snapshot_fail_test.go`, `scan_test.go`, and `pipelined_memdb_test.go` contain the direct behavioral suites.

Two indirect probe consumers were also inventoried rather than hidden: `integration_tests/scan_mock_test.go` exercises forward/reverse multi-region scanner construction, and `integration_tests/split_test.go` exercises stale-region single-region BatchGet plus concurrent async BatchGet success, lock, region-error, and transport-error branches.

The complete external snapshot matrices are assigned as follows:

- `snapshot_test.go`: BatchGet, Get/BatchGet commit timestamps, cache contents/hits/misses, nonexistent keys, large/point lock skipping, thread safety, runtime stats, RC reads, `MaxUint64` bypass, and replica adjustment map to the focused tests and native ownership described above.
- `snapshot_fail_test.go`: response-level BatchGet/Scan errors, latest point-lock omission, resolved-TS retries, required commit TS, timestamp reset, and resolve-for-read map to deterministic response/lock/cache regressions.
- `scan_test.go` and `scan_mock_test.go`: bounded/unbounded forward/reverse scans across regions, eager first fetch, ordering, validity, empty regions, and continuation map to iterator and transport-boundary regressions.
- `pipelined_memdb_test.go`: buffer-tier reads and self-lock skipping map to `batch_get_from_buffer`, pipelined gating, resource tagging, and resolved-lock context tests; flush/commit/rollback algorithms remain on the completed transaction receipt.
- `split_test.go`: stale-region relocation and async fanout map to shard re-resolution, ordered concurrent collection, sibling cancellation, and locked-key-only retry tests; unrelated split/store orchestration remains on root `tikv`.

Completing this package does not promote root `tikv`, root `txnkv`, or the integration-test packages. Their concrete store lifecycle, GC safe-point provider, orchestration, and live differential gates retain separate ledger ownership.

## Validation boundary

Final validation on `nightly-2026-08-22-aarch64-apple-darwin` passed:

- `cargo test --lib source_ --quiet`: 343 passed.
- `cargo test --lib --quiet`: 602 passed.
- `cargo test --lib --all-features --quiet`: 602 passed.
- `cargo check --all-targets --all-features`: passed with the repository's existing dead-code/deprecation warnings.
- `cargo clippy --lib --all-features --message-format short`: passed with the repository's existing 104-warning backlog.
- `cargo doc --no-deps --all-features`: passed with the existing `src/raw/client.rs` invalid-HTML warning.
- `cargo test --doc --all-features --quiet`: 50 passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.
- The source checkout HEAD is exactly `52c1e76cec993571493c81de442bcbef90cdc106`; `git ls-tree`, `wc -l`, and SHA-256 checks reproduce the five-file/2,317-line inventory above.

The pinned source package has no local test requiring UniStore. Deterministic Rust PD/KV mocks cover its complete local interface boundary; UniStore remains available for reusable high-level integration tests.

The host has no Go toolchain, and no TiKV/PD cluster is attached, so the pinned external Go suites and live cross-client differential tests are not executed locally. Those are repository-level completion gates, not uncounted artifacts of this package.
