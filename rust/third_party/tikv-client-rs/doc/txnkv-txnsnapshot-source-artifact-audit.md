# `txnkv/txnsnapshot` source-artifact audit

This is the atomic completion receipt for client-go package `txnkv/txnsnapshot`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The Rust implementation is in the reusable `tikv-client` crate and is validated with `nightly-2026-08-22`.

## Complete source inventory

`git ls-tree -r --name-only 52c1e76cec993571493c81de442bcbef90cdc106 txnkv/txnsnapshot` contains exactly five files and 2,317 lines:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `client_helper.go` | 168 | `1c8fe55bca147827b9f1f0ae9c2f07480029657cfcb2a3350c925b288c4b04e6` | read-lock plans and cumulative retry ownership in `src/request/plan.rs`, `plan_builder.rs`, and `src/transaction/{lock,transaction}.rs` |
| `scan.go` | 360 | `624db2aaeff610817fcea812a2bb9a9f35b0fa383d93dea4702210f783bbb698` | `SnapshotIterator`, `SyncSnapshotIterator`, one-region scan sharding/collection, and pair-local lock recovery |
| `snapshot.go` | 1,414 | `19353fa5f4f0c50d1657b49047d270247fdd271f7c1c7610652381225322a456` | `src/transaction/{snapshot,sync_snapshot,transaction,buffer,snapshot_stats,requests}.rs`, request plans, request context, and metrics |
| `snapshot_async.go` | 301 | `1ed111669fb1bbf1b9f57e2f144170abd8a72d8129dec4cd90162f4d5ff0ed81` | native future-based concurrent multi-region dispatch, fork lifetime/accounting, callback-result counters, and response collectors in `src/request/plan.rs` |
| `test_probe.go` | 74 | `d130a33d8d18dc3cf4d2b7e85f5578dc67df787ce0a4aa4d48935f379d01fc17` | public probe constants, snapshot/runtime-stat diagnostics, deterministic mock dispatch, and source-named focused tests |

There is no package `doc.go`, colocated Go test, benchmark/example, fixture, generated source or input, package build file, build-tag/platform variant, or non-Go runtime artifact. Kvproto messages are generated dependencies of this package rather than its generated artifacts.

## Production mapping

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| `KVSnapshot`, constructor, timestamp reset, isolation/priority/task/cache/key-only/sample/replica/resource/scope/timeout/interceptor/request-source setters | Generic async `Snapshot<PdC>` and `SyncSnapshot<PdC>` retain all source state and apply it to every physical Get, BatchGet, BufferBatchGet, and Scan request and retry. Constructor/reset accept timestamps below `MaxInt64` plus the `MaxUint64` sentinel; reset clears cached reads and resolved-lock hints while preserving committed hints. `Snapshot` is `Send + Sync`; Rust prevents unsynchronized aliasing of its mutable cache, and the exact source 5-task/30-iteration thread-safety workload executes through native `Arc<tokio::sync::Mutex<_>>` shared ownership. |
| Request source | `RequestSource` emits exact `unknown`, `internal_<type>[_<explicit>]`, or `external_<type>[_<explicit>]` values; an internal flag with no type remains `unknown` and is not classified internal. Every snapshot plan writes the value into TiKV context, including scanner point reads and retries. |
| Point Get | Cache values and misses, commit-TS refetch/validation, configurable first-send timeout, replica adjustment, post-response visibility contract, runtime details/SLI, lock resolution, and region retry follow the source. Region and lock retry classes charge one cumulative source Backoffer. A latest-timestamp point read blocks on only the first encountered lock transaction and adds later transaction IDs to resolved hints without resolving them. |
| BatchGet and BufferBatchGet | Cached snapshot keys are removed before physical work; a fully cached request performs no BatchGet timing/region observation. Missing keys are grouped by current region and capped at 5,120 keys per shard. Pair-lock retries preserve clean pairs and resend only locked keys; response-level errors discard incomplete pairs and retry the complete pending key set. The pipelined buffer tier has the source gate/error and does not cache results. Region cardinality is observed once from the initial grouping. |
| Async BatchGet | Rust futures and `JoinSet` are the native counterpart of callbacks/run loops. Multiple initial region shards execute without the generic worker cap, results retain source shard order, and the operation forks once, clones non-final sibling backoffers, waits for every shard even after errors, returns the last failing completion, and merges the last completed fork. `Config.enable_async_batch_get` now reaches every snapshot and pipelined buffer BatchGet path: both modes retain concurrent fanout, while only enabled multi-shard requests increment the source `ok`, `region_error`, `lock_error`, or `other_error` callback counter once per initial shard. Region and lock retries do not increment it again. |
| Scanner | Construction eagerly fetches the first batch. Iterator refills and eager `scan*` helpers select exactly the first forward or last reverse boundary region, clamp request bounds to that region, preserve short interior-region batches, cross empty regions, advance with `NextKey(last)` or an exclusive reverse end, and share one cumulative retry owner across region retries, response-lock waits, empty-region transitions, and pair-local point reads. Response-level locks discard incomplete pairs and retry; pair-level locks preserve clean pairs, recover an empty key from `LockInfo`, point-read only that key, skip a missing result, and advance from the raw response key/count. `is_valid` and `close` provide native iterator state. The external 1/256/257/768-row matrix caught and now guards the previous eager-scan loss at a short interior boundary. |
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

Source-derived Rust coverage additionally includes constructor/reset bounds, full request-source encoding, option and request-context propagation, cache hit/miss/content/10-GiB behavior, commit timestamps, latest-point lock omission, configurable/default retry deadlines, retry variables, read validation versus post-response visibility, one-region scanner routing across empty regions, response/pair lock recovery, forward/reverse continuation, runtime details, async callback selection/counters, wait-all error semantics, and fork accounting.

## Consumer and external-test audit

All ten direct pinned importers were inspected:

- `tikv/kv.go` constructs snapshots and provides the concrete `kvstore` dependency; `tikv/test_probe.go` re-exports the probe constants and snapshot wrapper. Root `tikv` retains its own GC-state/lifecycle receipt.
- `txnkv/snapshot_export.go` re-exports the snapshot, scanner, runtime-stat, isolation, and replica-adjuster APIs.
- `txnkv/transaction/txn.go`, `txn_test.go`, and `test_probe.go` own transaction delegation, pipelined buffer reads, direct single-region BatchGet, and scanner construction. The transaction package remains complete and this package supplies its snapshot dependency.
- `integration_tests/snapshot_test.go`, `snapshot_fail_test.go`, `scan_test.go`, and `pipelined_memdb_test.go` contain the direct behavioral suites.

Two indirect probe consumers were also inventoried rather than hidden: `integration_tests/scan_mock_test.go` exercises forward/reverse multi-region scanner construction, and `integration_tests/split_test.go` exercises stale-region single-region BatchGet plus concurrent async BatchGet success, lock, region-error, and transport-error branches.

The six external files contain 40 `Test*` declarations: six suite harnesses and 34 suite methods. The six top-level declarations only invoke `suite.Run` and map to Rust's module test harnesses. Every behavioral method has an independently discoverable Rust identity named `source_go_txnkv_txnsnapshot_<file>_<GoName>`. Mechanical comparison finds 34 methods and 34 unique Rust identities with no missing, extra, or duplicate name. All 34 are direct Rust test bodies: the former 16-entry transaction forwarding macro and five-entry unionstore forwarding macro are gone, and nine additional handwritten identity aliases were replaced. Eight of those called registered tests; the stale-epoch identity shared another package's scenario helper. Cache, runtime-stat, and commit-TS decomposition now uses private non-test subcase helpers owned by one source identity, and no identity calls another registered test. Their exact executable ownership is:

| External declaration/case matrix | Executable Rust port or disposition |
| --- | --- |
| `snapshot_test.go`: `TestBatchGet`, `TestBatchGetNotExist` | Both pinned methods iterate `testSnapshotSuite.rowNums`, but `SetupTest` never initializes it; each method has zero cases. The intended existing/missing behavior is still exercised by `source_async_batch_get_switch_defaults_off_then_counts_each_initial_shard`, `source_snapshot_batch_get_caches_missing_keys`, and the response/pair-lock tests rather than inventing source cases. |
| `TestGetAndBatchGetWithReturnCommitTS`, `TestSnapshotCache` | Their exact identities directly execute commit-TS refetch/validation and cache hit/miss/reset assertions; private subcase helpers are not tests and are used only by the one table-shaped commit-TS identity below. |
| `TestSkipLargeTxnLock`, `TestPointGetSkipTxnLock` | Their exact identities directly dispatch locked reads, check status, verify committed-lock hints, and prove the large-lock/secondary reads do not synchronously clean the lock. |
| `TestSnapshotThreadSafe` | Its exact identity executes the source 5 tasks × 30 Get/BatchGet loop at `MaxUint64`, including a missing key, and statically requires `Snapshot<MockPdClient>: Send + Sync`. |
| `TestSnapshotRuntimeStats` | Its exact identity directly executes clone, merge, RPC/backoff/time/scan/read-pool, and client-go formatting assertions. |
| `TestRCRead` | The pinned method unconditionally calls `Skip` before its loop, so it has no executable source case. Isolation-level wire propagation is covered by `source_snapshot_context_settings_reach_all_read_requests`. |
| `TestSnapshotCacheBypassMaxUint64`, `TestReplicaReadAdjuster` | Their exact identities directly cover repeated Get, BatchGet, and option reads without caching at `MaxUint64`, plus per-physical-region adjustment. The async true/false switch remains covered from client configuration through callback accounting. |
| `snapshot_fail_test.go`: `TestBatchGetResponseKeyError`, `TestScanResponseKeyError` | Their exact identities directly prove that incomplete pairs do not escape and the complete pending request is retried after the response-level error. |
| `TestRetryMaxTsPointGetSkipLock`, `TestRetryPointGetResolveTS` | Their exact identities directly execute latest point-lock omission, one-status-check behavior, committed/resolved hint propagation, and the `MaxUint64` caller sentinel. |
| `TestCommitTSRequiredAssertion` (seven named rows) | One exact async identity owns four private, non-test subcase helpers and directly executes ordinary Get/BatchGet zero-commit-TS acceptance, required-commit-TS errors, cache-before-error versus no-batch-cache behavior, and BufferBatchGet's option-ignoring behavior. |
| `TestResetSnapshotTS`, `TestSnapshotUseResolveForRead` (async commit false/true) | Their exact identities directly clear cached reads while preserving start TS and directly recover a committed secondary through read hints without synchronous cleanup. |
| `scan_test.go`: `TestScan` (rows 1, 256, 257, 768; split at 123/456) | Its exact identity ports the full row/split matrix and adds bounded, key-only toggle, and reverse assertions. It fails on the pre-fix eager multi-region implementation by returning 691/768 rows. |
| `scan_mock_test.go`: `TestScanMultipleRegions`, `TestReverseScan` | Two exact identities independently construct the source-shaped two-region alphabet fixture, then assert forward and reverse batch-size-10 results and bounds. |
| `split_test.go`: `TestSplitBatchGet`, `TestBatchGetUsingAsyncAPI`, `TestStaleEpoch` | Three exact identities directly execute stale relocation/epoch-cache behavior, multi-region existing/missing values, and enabled/disabled async dispatch. Callback result classes, response/pair locks, wait-all errors, and transport/region retries remain additional direct regressions. Root split/store orchestration remains owned by `tikv`. |
| `pipelined_memdb_test.go`: ten suite methods | All ten exact identities are direct. Five exercise generation flush/block, own-write visibility, remote prefetch/cache, and active-versus-flushing MemDB precedence; five exercise resolve-lock races, commit, rollback, PK failure, and max-TTL failure. Production transaction semantics remain atomically owned by the completed `txnkv/transaction` receipt. |

Completing this package does not promote root `tikv`, root `txnkv`, or the integration-test packages. Their concrete store lifecycle, GC safe-point provider, orchestration, and live differential gates retain separate ledger ownership.

## Validation boundary

Final independent-test validation on `nightly-2026-08-22-aarch64-apple-darwin` passed:

- From the nested `integration_tests` module, `/private/tmp/go1.25.12-full/bin/go test . -run '^(TestSnapshot|TestSnapshotFail|TestScan|TestScanMock|TestSplit|TestPipelinedMemDB)$' -count=1`: passed in 40.674 seconds.
- The same exact Go selection with `-race`: passed in 52.880 seconds; the linker emitted only its known malformed `LC_DYSYMTAB` warning.
- `cargo test --no-default-features --lib source_go_txnkv_txnsnapshot_ -- --nocapture`: 33 passed and the source's unconditional `TestRCRead` skip remained ignored.
- The same focused Rust gate with `--all-features`: 29 passed and five source-compatible NextGen/unconditional skips remained ignored.
- `cargo nextest run --config-file config/nextest.toml --all --no-default-features`: 1,275 passed and two tests were intentionally skipped.
- `cargo nextest run --config-file config/nextest.toml --all --all-features --lib`: 1,250 passed and six tests were intentionally skipped.
- `make check`: clean protocol generation, workspace all-target/all-feature checking, rustfmt, and strict workspace Clippy with warnings denied passed.
- `make doc`: strict private-item workspace rustdoc and all 51 doctests passed.
- `git diff --check`: passed.
- Mechanical method comparison: 34 source suite methods and 34 independently named direct Rust tests, with no missing, extra, duplicate, forwarding macro, or registered test-to-test call.
- The source checkout HEAD is exactly `52c1e76cec993571493c81de442bcbef90cdc106`; `git ls-tree`, `wc -l`, and SHA-256 checks reproduce the five-file/2,317-line inventory above.

The pinned source package has no local test requiring UniStore. Deterministic Rust PD/KV mocks cover its complete local interface boundary; UniStore remains available for reusable high-level integration tests.

The stronger one-to-one test audit found an evidence defect but no additional production divergence: 21 identities had been forwarding registrations and nine further identities were handwritten aliases. Eight aliases called registered tests and one reused another package's scenario helper. They are now direct bodies; eight duplicate registered cache/runtime subtests were collapsed into private single-owner helpers, while the source's two genuinely zero-case methods and unconditional skip remain literal. Package-owned behavior uses deterministic Rust request and state-machine tests; the exact pinned Go integration selections supply the ordinary and race baselines. Separately recorded full integration and cross-client cluster runs remain repository-level evidence rather than uncounted artifacts of this package.
