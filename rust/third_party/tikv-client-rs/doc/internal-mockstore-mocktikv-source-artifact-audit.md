# `internal/mockstore/mocktikv` source-artifact audit

This was the atomic completion receipt for client-go's `internal/mockstore/mocktikv` package at pinned commit `52c1e76cec993571493c81de442bcbef90cdc106`. Runtime downstream testing reopened the claim: transactional Get fails to set `GetResponse.not_found` for an absent key, unlike real TiKV and the source contract. The Rust owner remains the hidden `tikv_client::mock::mocktikv` adapter plus the reusable standalone `unistore` engine crate, and validation uses `nightly-2026-08-22`. The row remains `in-progress` until the adapter fix and external regression pass.

## Complete source inventory

The claim contains exactly 14 Go artifacts and 6,689 lines: ten production files, three ordinary test files, and one goleak harness.

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `internal/mockstore/mocktikv/cluster.go` | 798 | `0fb39697558ea95fc373f13de8246c74d9e60b47ff7d5decc267249bc94c01a8` | `src/mock/mocktikv/cluster.rs` cluster/store/region topology, delay, split/merge, buckets, and exact `SplitKeys` grouping |
| `internal/mockstore/mocktikv/cluster_manipulate.go` | 108 | `38a77a17e92794185b0686fff9e0d9318870a10695a7ae39138958d030a9c4f3` | the four bootstrap helpers in `src/mock/mocktikv/cluster.rs` |
| `internal/mockstore/mocktikv/errors.go` | 158 | `574ae08799964062bcb4225b8a272986bc4bea4daeccac0247827ece3a1b481b` | typed `unistore::MockError` variants and exact kvproto conversion in `src/mock/mocktikv/rpc.rs` |
| `internal/mockstore/mocktikv/main_test.go` | 29 | `2330027c4f666771ddb2ef2cf7098353ba6455220172991b99b3ea374292ab5d` | no detached mock tasks; close tests, complete library gates, and explicit engine/handler close ownership |
| `internal/mockstore/mocktikv/marshal_test.go` | 88 | `63d1c2b1cbf8818c9401867c4342a509471da5978c261f25de4dd8a169627bc6` | `lock_and_write_binary_formats_round_trip` plus malformed/10-MiB decode guards |
| `internal/mockstore/mocktikv/mock.go` | 50 | `ac5611ddf0e01922d17a08d921c5fd84efad336c2a550b131d970c487dbe63d66` | `new_tikv_and_pd_client` in `src/mock/mocktikv/mod.rs` |
| `internal/mockstore/mocktikv/mock_tikv_test.go` | 792 | `abe3df78aca0c80d5e08d17a17e750f48f910c4c2991fd68beb8e158fbafe74a` | source-named MVCC tests in `unistore/src/mock.rs` and protocol-adapter tests in `src/mock/mocktikv` |
| `internal/mockstore/mocktikv/mvcc.go` | 344 | `89fd915abff7f2228b7f97c208ec3c34de9978513c8f3213431a6b0e18b7924a` | reusable records/interfaces in `unistore/src/mock.rs`; `MvccKey` in `src/mock/mocktikv/mod.rs` |
| `internal/mockstore/mocktikv/mvcc_leveldb.go` | 2,133 | `df5078a352256bb60b5dcf54e00a9efecc1df764a29579d783d83106abea7e5a` | `unistore::MockEngine`, including directory-backed close/reopen snapshots |
| `internal/mockstore/mocktikv/mvcc_test.go` | 55 | `6e365e784451aff8a48e01c0a48af0b47c74f7a2565d407f42ceb0e59889e330` | `source_region_boundaries_topology_and_bootstrap_helpers` |
| `internal/mockstore/mocktikv/pd.go` | 710 | `2a90521f8e5c2471795313de64ffbe8201dad89da4826e820569b567304c1b8e` | `src/mock/mocktikv/pd.rs` and native `PdClient` implementation |
| `internal/mockstore/mocktikv/rpc.go` | 1,143 | `6cb30eceedc7f964480f6eb8d1a54669efdd6bcac27b916bb78e031bf3a6f331` | `src/mock/mocktikv/rpc.rs` plus mock stream constructors/accessors in `src/store/request.rs` |
| `internal/mockstore/mocktikv/session.go` | 207 | `8a683becbca9f344006918c6625b2dc29a95657aeee6a385a0169531ced9ace6` | `src/mock/mocktikv/session.rs` |
| `internal/mockstore/mocktikv/utils.go` | 74 | `7941504552b9c2b6915c4753f30c3ca25d1a3e7667a7cf47a03dd0f8969c1ea3` | `put_mutations` and `must_prewrite` in `src/mock/mocktikv/rpc.rs` |

There is no package `doc.go`, non-Go fixture, package-specific build file, generated source/input, benchmark, example, `OWNERS`, platform variant, build tag, or `go:generate` directive. Every source artifact is always selected at the pinned revision.

## Production behavior and native integration decisions

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| MVCC records and key coding | Lock/write records preserve the source little-endian numeric layout, Go-uvarint slices, 10-MiB decode limit, operation types, short-value boundary, and malformed-input failures. `MvccKey` uses the completed memcomparable codec and preserves the empty-key sentinel. |
| store construction and persistence | Empty paths create an in-memory engine. Nonempty paths restore and atomically snapshot committed MVCC/raw-CF state on close, preserving the source constructor's reusable directory behavior without exposing LevelDB as a Rust API. `must_new_mvcc_store`, `new_mvcc_level_db`, and the client/cluster/PD factory retain the source failure boundaries. |
| optimistic MVCC | SI/RC visibility, point/batch/forward/reverse reads, resolved-lock bypass, primary latest-read behavior, optimistic prewrite, insert/check-not-exists assertions, rollback tombstones, write conflicts, idempotent commits, cleanup, check-status actions, min-commit-TS rejection, heartbeat, lock scans, resolve/batch-resolve, GC, and range deletion are stateful and source-compatible. |
| pessimistic MVCC | Pessimistic lock value/existence results, return-value mode, force-lock conflict metadata, wait-for deadlocks and key hashes, rollback ranges/key lists, wake-up modes, for-update timestamps, lock-only reads, and pessimistic status actions share the reusable engine. |
| raw KV and debugger | Every raw CF get/batch/put/delete/forward/reverse-scan/delete-range/CAS/checksum branch is present. Checksum uses Go `hash/crc64`'s reflected ECMA/complement parameters and XOR aggregation. Debug lookups preserve empty-info results for a missing start TS and expose locks, writes, and values. |
| cluster topology | Stores, addresses/peer addresses/labels, cancellation, offline/tombstone states, peers/learners/down peers, leaders, region epochs, scans, split/merge/buckets, delays, and all four bootstrap shapes are implemented. `SplitKeys` uses source quotient/remainder grouping, including `count > key_count`, and evacuates intersecting ranges before creating replacements. |
| request session | Store/peer/leader/TiFlash/epoch checks, current-region payloads, resolved locks, SI/RC selection, memcomparable and raw region bounds, and the inclusive 8-MiB raft-entry rejection match source behavior. Rust stores both encoded and decoded bounds once context validation succeeds so typed coprocessor handlers do not repeat decoding. |
| RPC matrix | Native dynamic dispatch covers Get, Scan, Prewrite, PessimisticLock/Rollback, Commit, Cleanup, CheckTxnStatus, Heartbeat, BatchGet/Rollback, ScanLock, ResolveLock, GC, DeleteRange, every raw command, unary Cop, BatchCop, CopStream, both MVCC debug commands, SplitRegion, and region properties. Source panics/unimplemented commands, close/no-op-close-address behavior, request sizing, key/region assertions, delays, and transaction/commit/batch-coprocessor failpoints are retained. Rust futures replace `SendRequestAsync` callbacks without changing typed response behavior. |
| public protocol boundary | Client-go consumers name handler request/response types through the shared public `kvproto` module. Rust exposes the complete generated namespace at `tikv_client::proto`, so external crates can implement `CoprocessorHandler` and use every protocol type leaked by the public cluster, PD, session, and RPC surfaces. A downstream integration test proves the trait implementation rather than relying on same-crate visibility. |
| PD client | One process-wide monotonic TSO prevents duplicate timestamps across mock clients. Region/store routing, previous-region lookup, buckets/down peers, external timestamp monotonicity, legacy service safe points, modern transaction/GC safe points and barriers, source no-op scatter/split/operator responses, keyspace absence, default RU resource group, and the optional 200-ms GetRegion delay are represented through the native `PdClient` trait plus focused helpers. Source interface methods that return nil/zero/no-op map to the corresponding native default or absence rather than untyped placeholders. |
| reusable crate boundary | `unistore` is one standalone crate and the normal dependency direction is `tikv-client -> unistore`; protocol conversion stays in `tikv-client`, avoiding a cycle and allowing other modules to reuse the engine. This package receipt covers only the client-go mocktikv behavior hosted there. TiDB's much larger server package is outside the pinned source and explicitly `not-applicable`, not partially claimed. |

## Original tests and support artifacts

All 23 ordinary source test declarations and the `TestMain` lifecycle contract have explicit Rust evidence:

| client-go test(s) | Rust evidence |
| --- | --- |
| `TestMarshalmvccLock`, `TestMarshalmvccValue` | `mock::tests::lock_and_write_binary_formats_round_trip` |
| `TestRegionContains` | `cluster::tests::source_region_boundaries_topology_and_bootstrap_helpers` |
| `TestGet`, `TestGetWithLock`, `TestDelete`, `TestCleanupRollback` | matching `source_test_*` engine tests |
| `TestReverseScan`, `TestScan` | `source_test_forward_and_reverse_scan_tables` |
| `TestBatchGet` | `source_test_batch_get` |
| `TestScanLock`, `TestScanWithResolvedLock` | `source_test_scan_lock_and_resolved_lock` |
| `TestCommitConflict` | `source_test_commit_conflict_and_idempotence` |
| `TestResolveLock`, `TestBatchResolveLock` | `source_test_resolve_and_batch_resolve_lock` |
| `TestGC` | `source_test_gc` |
| `TestRollbackAndWriteConflict` | `source_test_rollback_and_write_conflict` |
| `TestDeleteRange` | `source_test_delete_range` |
| `TestRC` | `source_test_read_committed` |
| `TestCheckTxnStatus`, `TestRejectCommitTS` | `source_test_check_txn_status_and_reject_commit_ts` |
| `TestMvccGetByKey`, `TestTxnHeartBeat` | `source_test_mvcc_debug_and_heartbeat` |
| `TestMain` goleak harness | no spawned engine/cluster/PD tasks; handler and store close are explicit; both complete library configurations and doctests are awaited |

The Rust matrix adds coverage needed by production files that have no dedicated source test: pessimistic result/deadlock/rollback paths, raw-KV operations and the Go CRC64 vector, nonempty-path restoration, `MvccKey`, uneven region grouping, session error responses, global TSO/resource groups/GC barriers/previous-region routing, all three coprocessor forms, downstream implementation of the public coprocessor trait, and transactional/raw/debug RPC adaptation.

## Dependencies and consumers

The package's source dependencies are already complete: API codec, async utilities, internal client/cluster/deadlock/logging, locate contracts, metrics, oracle helpers, `tikvrpc`, and root `util`; required kvproto/PD protocol inputs are already generated in the Rust tree. The reusable state owner adds only existing workspace-compatible serde/farmhash dependencies.

Exact source matching finds nine direct external Go consumers:

- six `internal/locate` tests: `pd_codec_test.go`, `region_cache_test.go`, `region_request3_test.go`, `region_request_state_test.go`, `region_request_test.go`, and `replica_selector_test.go`;
- `rawkv/rawkv_test.go` and `tikv/kv_test.go` test fixtures;
- `testutils/mockstore.go`, the public test-support alias/factory facade.

The completed locate, RawKV, and root TiKV receipts already own their production algorithms and consume equivalent native cluster/transport boundaries. `testutils` is completed by its separate alias/factory receipt; this receipt supplies its concrete mock dependency but does not promote it. Additional integration suites consume mocktikv indirectly through that facade and remain assigned to their own package/live-differential gates.

## Validation contract

Completion requires 14/14 pinned artifact identity and the 6,689-line total; all source test names accounted for; the complete reusable-engine and mock-adapter matrices; both default and all-feature library suites; all-target/all-feature compilation; all-target Clippy; rustdoc and doctests; rustfmt and whitespace checks on `nightly-2026-08-22-aarch64-apple-darwin`. A real TiKV/PD cluster does not apply to this deterministic in-process package; live interoperability remains on the final differential milestone.

The final gate satisfies that contract. `cargo test -p unistore` passes 22 tests, and the focused hidden adapter matrix passes 8 tests. The complete default and all-feature library configurations each pass 702 active tests with one intentional process-isolation ignore; the workspace doctest run passes all 51 tests. Workspace/all-target/all-feature `cargo check` and Clippy, all-feature rustdoc, rustfmt, and `git diff --check` pass. The source checkout is clean at `52c1e76cec993571493c81de442bcbef90cdc106`, and mechanical enumeration reconfirms 14 artifacts, 6,689 lines, 23 ordinary tests plus `TestMain`, and exactly nine direct external consumers.

Post-completion API remediation makes the generated protocol namespace public and adds an actual downstream `CoprocessorHandler` implementation. That external target passes with `internal-tests`; the ordinary downstream protocol target passes without default features; and the complete post-remediation gates pass 739 no-default workspace tests, 733 all-feature library tests, strict all-target/all-feature Clippy and rustdoc, and 51 doctests.
