# `txnkv/transaction` source-artifact audit

This is the atomic completion receipt for client-go package `txnkv/transaction`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The Rust implementation is in the `tikv-client` crate and is validated with `nightly-2026-08-22`.

## Complete source inventory

`git ls-tree -r --name-only 52c1e76cec993571493c81de442bcbef90cdc106 txnkv/transaction` contains exactly 16 files and 11,766 lines:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `2pc.go` | 2,466 | `bab514fb1e829f882dd1288fdbc51978e8844e272fd82e4c24ba2709ae8613e2` | `src/transaction/transaction.rs`, `buffer.rs`, `requests.rs`, `lowering.rs`, `src/stats.rs` |
| `batch_getter.go` | 138 | `725b5da6c76b464c4f8e3298e48f29ebe4a4683fb8c4ee97c2e339e482b5331d` | `src/transaction/buffer.rs`, transaction read paths |
| `binlog.go` | 52 | `15a85c13386e9b44ce8c0a98b77a980bc65f05fc2d783e9720919322f6c5d244` | `src/transaction/transaction.rs` |
| `cleanup.go` | 117 | `0f57850bec99a537057bcbda9d17f099ea898e87d16040a95c443289e9087886` | `src/transaction/transaction.rs`, `requests.rs` |
| `commit.go` | 283 | `b0c450c67c3ea188dd729414b8776ed5afc35bf08a9f632d91f8b559ea5a7751` | `src/transaction/transaction.rs`, `requests.rs` |
| `pessimistic.go` | 708 | `566b3c5c6d1871422e07ecf94e82c0fff40e530da1251d43bc1b896e8a7f1f67` | `src/transaction/transaction.rs`, `buffer.rs`, `requests.rs`, `src/kv/mod.rs` |
| `pipelined_flush.go` | 537 | `1877c4732c9ed55d4ef90d8202e790447b56a1e9a2e823bb0fcc8bc97e2c55c4` | `src/transaction/transaction.rs`, `requests.rs`, completed unionstore dependency |
| `prewrite.go` | 666 | `62adff44f3979f87dfbfdb9c97d79c3b1038b6321056c1c120f2c29536aeda8a` | `src/transaction/transaction.rs`, `requests.rs`, request plans |
| `txn.go` | 2,146 | `b6f3276abf809c273a13b50bd5336dbee2aee35cb9f6c64b8e7cdf3b746ed004` | `src/transaction/transaction.rs`, `client.rs`, `buffer.rs`, root exports |
| `txn_file.go` | 1,501 | `894637946615fc26b67b3c830cf98ec98c9c2bbafb3c5cd764796e215876f427` | `src/transaction/txn_file.rs`, `transaction.rs`, `Cargo.toml` |
| `test_probe.go` | 478 | `4d2e9c7c7aca9845080a2ee1394f6620196fa7e7950dd6671fea6136ade31730` | package-private Rust helpers, public native diagnostics, deterministic mock hooks |
| `test_util.go` | 518 | `0450b9604a0a5c1bcf91aa6f6fcc7e3dd7af6a1605e332f505c04c6000bf188b` | `src/mock.rs`, `src/pd/client.rs`, package test constructors |
| `2pc_test.go` | 181 | `c6be68b6c846cc20fb0029bda0435e7e131d0cc5820af50c55a3d6c405c3ca4e` | source-derived tests in `transaction.rs` and `txn_file.rs` |
| `batch_getter_test.go` | 143 | `fe819e1638869b0250c7dae7907421a3063116b61d8fb75c9014511a5054bbae` | source-derived tests in `buffer.rs` |
| `txn_file_test.go` | 1,477 | `60883816e3aeca5a86703aaefe0dcaba1c7d491cc424a994283a94cf737a388b` | source-derived tests in `txn_file.rs` and `transaction.rs` |
| `txn_test.go` | 355 | `6677b3af3502ee1e2a49690a6841370b8b2b51fccac81f0d77ccda223e33e032` | source-derived tests in `transaction.rs` |

There is no package `doc.go`, build-tag or platform variant, generated source/input, fixture, benchmark/example, package build file, or non-Go runtime artifact. The package consumes generated kvproto messages through the already audited `tikvrpc` and `internal/apicodec` boundaries; those generated inputs are not duplicated into this package claim.

## Production mapping

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| Mutation collection, primary selection, sizing, lock TTL, min/max commit timestamps, schema checks, callbacks, binlog, resource tags, request source, disk-full and transaction-source fields | `Transaction`, `CommitSettings`, and `Committer` preserve the complete commit state machine and protocol inputs. Safe owned `Vec<kvrpcpb::Mutation>` replaces `CommitterMutations`/`PlainMutations`; source range-selection and transaction-file helpers operate on that native representation. |
| Mem-buffer flags and exported `GetMemBuffer` use | Rust keeps the buffer encapsulated and exposes `MutationOptions`, `MutationAssertion`, `MutationFlags`, `put/insert/delete_with_options`, and `set_mutation_options`. Assertions, lazy constraint checks, pessimistic-lock provenance and observed existence, insert/check-not-exists state, filter visibility, txn-file exclusion, and prewrite actions remain observable without exposing allocator handles. Ordinary writes preserve assertions while clearing the transient constraint flag. Pessimistic-lock-derived assertion failures disable async/1PC, wait for successful prewrite, and then surface as typed errors. |
| `BufferBatchGetter` and `BufferSnapshotBatchGetter` | `Buffer` point/batch read methods merge local writes, deletes, locks, and cached snapshot values before fetching missing keys. Cached misses, commit timestamps, update/no-update cache modes, and local precedence are retained. Rust iterators/futures replace Go callback interfaces. |
| Optimistic/pessimistic locking | Lock wait modes, timeout/deadline recalculation, return values, `LockOnlyIfExists`, shared locks, deadlock callbacks, force-lock results, farmhash deadlock keys, aggressive locking, rollback, and independently retained lock provenance are implemented. Shared-only transactions cannot select a primary. |
| Prewrite | Per-region sharding carries physical `TxnSize`, pessimistic/constraint actions, assertions, async/1PC fields, secondaries, min/max commit timestamps, keyspace/resource metadata, retry budgets, and typed key errors. Shared holders are expanded. `NoResolvePolicy` and newer optimistic locks return typed write conflicts before any resolver RPC. |
| Commit and cleanup | Primary ambiguity is tracked separately from secondary failure; definitive primary responses clear undetermined state. Check-only mutations are stripped after prewrite. Failed public attempts close the transaction, stop heartbeat ownership, and schedule cleanup; ordinary rollback suppresses cleanup failures as source does. Background work runs lifecycle hooks. |
| Async commit and 1PC | Eligibility, fallback flags, callback JSON, timestamp allocation, max-commit validation, zero-minimum fallback, one-shard requirement, and one-PC commit timestamp handling are covered. Shared locks, pipelined mode, binlog, and other incompatible inputs disable the protocol before it is reported as tried. |
| Pipelined transactions | Option validation, generation/delta flushing, source request marker, primary/range tracking, write throttling, concurrency limits, BufferBatchGet visibility, primary commit, range lock resolution, status broadcast, rollback, and lifecycle ownership are implemented. Shared locking and mode changes retain the source rejection behavior. |
| Transaction-file protocol | A single reusable `txn_file` module owns chunk serialization, file upload, HTTP/TLS client pooling, 90-second idle timeout, parallel budget, cancellation, sorting/deduplication, split keys, dedicated pre-split path, admission, primary-batch lookup, Prewrite/Commit/Rollback actions, keyspace/resource metadata, lock expansion/resolution, prepared timestamp retry, schema/upper-bound validation, undetermined normalization, and idle-pool replacement. OpenSSL is selected only for the source-compatible TLS client. |
| Large-transaction proactive split | The source mutation-count and byte thresholds are process-wide atomics. Region groups generate deterministic split keys, call PD's split path, and invalidate affected cache entries without making split failure fatal to commit. |
| Public transaction surface | Async native methods cover source reads, writes, scans, locks, commit/rollback, protocol switches, scope/causality, variables, request/interceptor/resource controls, callback/binlog/schema/filter/memory hooks, request source, commit-wait, diagnostics, and heartbeat. Typed futures replace synchronous context methods; `SyncTransaction` remains the existing optional blocking façade. |

Rust ownership intentionally consolidates source mechanics that do not carry distinct behavior. A transaction owns its pending committer settings until `commit`, tasks replace explicit `WaitGroup`/goroutine fields, immutable snapshots replace exposed unionstore pointers, and Rust error sources replace Go stack wrappers. `SetSessionID` is retained until committer construction rather than being silently ignored before construction. `GetTimestampForCommit` remains commit-internal because the native client exposes timestamp allocation through its PD owner. These decisions preserve capabilities while avoiding mutable internal handles.

The source's many failpoints are injection sites, not additional production protocols. Deterministic `MockKvClient`/`MockPdClient` response hooks, atomic threshold setters, lifecycle hooks, and focused Rust failpoints cover their observable branches: transport/region/key failures, lock responses, commit ambiguity, fallback, schema/binlog/resource failures, timing, cancellation, and cleanup. Go's `test_probe.go` field mutators map to direct package-private construction and assertions; exported configuration probes map to public constants/setters. `test_util.go`'s hundreds of panic-only interface stubs map to narrow Rust traits and mocks that implement only the method under test, so unsupported methods are unrepresentable instead of runtime panics.

The completed unionstore dependency remains reusable and includes a `unistore`-backed remote-buffer test. The source ART `RemoveFromBuffer` is itself an explicit unsupported test function and panics; Rust preserves that contract. Transaction production does not call it. No extra UniStore server is required by this package's original test boundary.

## Original test declaration mapping

Mechanical source enumeration finds exactly 33 `func Test...` declarations. Every name is assigned below; combined Rust regressions share setup only where the source tests exercise one state machine.

| Source declaration | Rust evidence |
| --- | --- |
| `TestMinCommitTsManager` | `source_min_commit_ts_manager_access_and_concurrency` |
| `TestMutationsHasDataInRange` | `source_mutations_has_data_in_range_matrix` |
| `TestBufferBatchGetter` | `source_buffer_batch_getter_local_precedence_delete_and_commit_ts` |
| `TestLockKeys` | `source_lock_keys_modes_wait_timeout_and_force_lock_results`, `source_lock_context_fields_results_callbacks_and_preflight_errors`, and `source_lock_context_lock_only_if_exists_and_deadlock_callback` |
| `TestSharedLockCommitterIncompatibilities` | `source_shared_lock_committer_incompatibilities` |
| `TestTxnFileCleanupContextUsesStoreContext` | package-owned detached cleanup/lifecycle tests plus `source_txn_file_primary_prewrite_cleanup_and_batch_selection`; Rust cleanup owns cloned client state rather than a cancelable caller context |
| `TestTxnFileMaxChunksInParallel` | `source_txn_file_parallel_budget_boundaries` |
| `TestCloseTxnFileIdleConnections` | `source_close_idle_connections_replaces_the_shared_pool` |
| `TestCloseTxnFileIdleConnectionsBeforeInitialization` | `close_before_http_client_initialization_is_safe` |
| `TestTxnFileHTTPClientHasIdleConnectionTimeout` | `source_txn_file_http_client_idle_connection_timeout_is_90_seconds` |
| `TestPrepareTxnFileCommitTS` | `source_prepare_txn_file_commit_timestamp_waits_and_checks_schema_first` |
| `TestTxnFileCommitTSExpiredRetryUsesPreparedTimestamp` | `source_txn_file_commit_ambiguity_and_expired_retry` |
| `TestTxnFilePrewriteUsesPrimaryKey` | `source_txn_file_primary_prewrite_cleanup_and_batch_selection` |
| `TestTxnFilePrewriteExpandsSharedLockHolders` | `source_txn_file_prewrite_expands_shared_lock_holders` |
| `TestTxnFilePrimaryBatchIndexFindsPrimaryRegion` | `source_txn_file_primary_prewrite_cleanup_and_batch_selection` |
| `TestTxnFilePrimaryRollbackPropagatesKeyError` | `source_txn_file_primary_prewrite_cleanup_and_batch_selection` |
| `TestTxnFileActionsApplyResourceGroupTagger` | `source_txn_file_actions_apply_dynamic_or_static_resource_group_tag` |
| `TestTxnFileActionsPreserveStaticResourceGroupTag` | `source_txn_file_actions_apply_dynamic_or_static_resource_group_tag` |
| `TestTxnFilePrewriteTaggerUsesFirstKeyWithoutSampleDataKeys` | `source_txn_file_tagger_uses_first_key_and_static_tag_wins` |
| `TestTxnFilePrewriteTaggerAppliesWithoutFirstKey` | `source_txn_file_tagger_uses_first_key_and_static_tag_wins` |
| `TestTxnFileCommitPrimaryRPCErrorMarksResultUndetermined` | `source_txn_file_commit_ambiguity_and_expired_retry` |
| `TestTxnFileCommitSecondaryRPCErrorIsNotResultUndetermined` | `source_txn_file_commit_ambiguity_and_expired_retry` |
| `TestTxnFileCommitClearsUndeterminedErrOnDefinitivePrimaryResponse` | `source_txn_file_commit_ambiguity_and_expired_retry` |
| `TestTxnFileCommitPrimaryUndeterminedRegionError` | `source_txn_file_commit_ambiguity_and_expired_retry` |
| `TestTxnFileCommitPrimaryRPCErrorIsNormalized` | `source_txn_file_commit_ambiguity_and_expired_retry` |
| `TestTxnFileCommitPreservesCommitOnResourceControlResponseError` | `source_txn_file_commit_survives_resource_accounting_response_error` |
| `TestChunkSliceSortAndDedup` | `source_chunk_slice_sort_and_dedup_preserves_ranges` |
| `TestIsRequestSourceUseTxnFile` | `source_request_source_whitelist` |
| `TestUseTxnFileExcludesPipelinedTxn` | `source_txn_file_admission_exclusions` |
| `TestUseTxnFileExcludesSharedLockTxn` | `source_txn_file_admission_exclusions` |
| `TestUseTxnFileExcludesMutationAssertions` | `source_txn_file_admission_exclusions` and `source_public_mutation_options_reach_filter_and_prewrite` |
| `TestPreSplitTxnFileRegionsUsesDedicatedSplitPath` | `source_pre_split_txn_file_regions_uses_dedicated_split_path` |
| `TestBuildTxnFilesEntryCounting` | `source_build_txn_files_counts_entries_and_matches_wire_format` |

Additional source-derived tests cover transaction validity after the first commit/rollback attempt, commit-wait retry/classification, all-check transactions, mutation assertion/constraint actions, `NoResolvePolicy`, normal and failed cleanup, large-2PC splitting, binlog lifecycle, schema/filter/callback/memory contracts, async/1PC fallback, pipelined generations, request context propagation, and keyspace/API coding. The integration failpoint test proves failed commit closes the public transaction while detached cleanup removes its lock.

## Consumer and integration audit

Every direct pinned importer was inspected and assigned:

- `txnkv/client.go` and `txnkv/transaction_export.go` own the root construction/export façade and retain the separate `txnkv` ledger status.
- `tikv/kv.go` and `tikv/test_probe.go` consume transaction methods/probes; the larger store/GC lifecycle remains on the separate root `tikv` row.
- Direct integration importers are `integration_tests/2pc_test.go`, `async_commit_test.go`, `isolation_test.go`, `lock_test.go`, `main_test.go`, `option_test.go`, `pipelined_memdb_test.go`, `prewrite_test.go`, `safepoint_test.go`, `scan_test.go`, `shared_lock_test.go`, `snapshot_test.go`, `split_test.go`, `txn_file_test.go`, and `util_test.go`.
- `integration_tests/assertion_test.go` does not import this package directly but exercises its `tikv.Transaction` implementation through the integration package; its assertion-level, pessimistic-lock, and deferred-constraint inputs are represented by the public mutation-options and prewrite-action regressions.

The complete `internal/locate`, `internal/client`, `internal/apicodec`, `tikvrpc`, `internal/latch`, `internal/unionstore`, `config/retry`, and error/config dependencies provide the routing, transport, codec, lock-buffer, retry, and typed-error behavior used here. Completing this package does not promote `txnkv`, `txnkv/txnlock`, `txnkv/txnsnapshot`, root `tikv`, or integration packages; they retain their own inventories and receipts.

## Validation boundary

Final validation on `nightly-2026-08-22` used the exact batch code:

- `cargo +nightly-2026-08-22 test -p tikv-client --lib transaction:: --quiet`: 198 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 585 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 585 passed.
- `cargo +nightly-2026-08-22 check -p tikv-client --all-targets --all-features`: passed with the existing warning backlog.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short`: passed with the existing warning backlog.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features`: passed with one pre-existing `src/raw/client.rs` invalid-HTML warning.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 50 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.
- Mechanical declaration comparison: 33 pinned source tests and 33 documented tests, with no missing or extra name.

Package-owned behavior is covered through deterministic request-level mocks and source-derived state-machine tests. The original Go tests were inspected at the pin but cannot be re-executed on this host because no Go toolchain is installed.

A live TiKV/PD cluster is not required by any of the four package-local source test files. End-to-end cross-client differential runs for transaction, snapshot, lock resolver, safe point, and root-store orchestration remain a repository completion gate owned by their high-level packages; they are not an omitted artifact of this atomic package receipt.
