# `internal/mockstore/mocktikv` source-artifact audit

This is the independently re-audited atomic completion receipt for client-go's `internal/mockstore/mocktikv` package at pinned commit `52c1e76cec993571493c81de442bcbef90cdc106`. The 2026-08-26 re-audit reran the exact Go normal/race package suites and replayed every source assertion. A repository-wide follow-up found that 22 of the 23 exact identities still came from a forwarding macro; a later whole-body scan then found that `TestRegionContains` was still a one-call helper alias despite the receipt's broader directness claim. The macro, redundant marshal aggregate, and final helper-only identity are now gone, so every source identity owns its Rust actions and assertions directly. The stronger ports found omitted assertion rows but no additional production divergence. Earlier runtime testing already corrected transactional Get to set `GetResponse.not_found` for an absent key, matching real TiKV, client-go's response contract, and client-rust's transaction response processor. The Rust owner remains the hidden `tikv_client::mock::mocktikv` adapter plus the reusable standalone `unistore` engine crate, and validation uses `nightly-2026-08-22`.

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
| `internal/mockstore/mocktikv/mvcc_test.go` | 55 | `6e365e784451aff8a48e01c0a48af0b47c74f7a2565d407f42ceb0e59889e330` | direct `source_go_internal_mockstore_mocktikv_TestRegionContains` plus supplemental topology/bootstrap coverage |
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
| optimistic MVCC | SI/RC visibility, point/batch/forward/reverse reads, resolved-lock bypass, primary latest-read behavior, optimistic prewrite, insert/check-not-exists assertions, rollback tombstones, write conflicts, idempotent commits, cleanup including source's unconditional `currentTS == 0` expiry, check-status actions, min-commit-TS rejection, heartbeat, lock scans, resolve/batch-resolve, GC, and range deletion are stateful and source-compatible. |
| pessimistic MVCC | Pessimistic lock value/existence results, return-value mode, force-lock conflict metadata, ordered multi-edge wait-for deadlocks and exact key hashes, rollback ranges/key lists, wake-up modes, for-update timestamps, lock-only reads, and pessimistic status actions share the reusable engine. The detector is the single public UniStore component completed by the `internal/mockstore/deadlock` receipt; commit, rollback, and cleanup remove edges even on error, while range resolve retains them exactly as the source does. |
| raw KV and debugger | Every raw CF get/batch/put/delete/forward/reverse-scan/delete-range/CAS/checksum branch is present. Checksum uses Go `hash/crc64`'s reflected ECMA/complement parameters and XOR aggregation. Debug lookups preserve empty-info results for a missing start TS and expose locks, writes, and values. |
| cluster topology | Stores, addresses/peer addresses/labels, cancellation, offline/tombstone states, peers/learners/down peers, leaders, region epochs, scans, split/merge/buckets, delays, and all four bootstrap shapes are implemented. `SplitKeys` uses source quotient/remainder grouping, including `count > key_count`, and evacuates intersecting ranges before creating replacements. |
| request session | Store/peer/leader/TiFlash/epoch checks, current-region payloads, resolved locks, SI/RC selection, memcomparable and raw region bounds, and the inclusive 8-MiB raft-entry rejection match source behavior. Rust stores both encoded and decoded bounds once context validation succeeds so typed coprocessor handlers do not repeat decoding. |
| RPC matrix | Native dynamic dispatch covers Get, Scan, Prewrite, PessimisticLock/Rollback, Commit, Cleanup, CheckTxnStatus, Heartbeat, BatchGet/Rollback, ScanLock, ResolveLock, GC, DeleteRange, every raw command, unary Cop, BatchCop, CopStream, both MVCC debug commands, SplitRegion, and region properties. Transactional Get explicitly sets `not_found` independently from its empty wire value, so client response processing distinguishes absence from an empty payload. Source panics/unimplemented commands, close/no-op-close-address behavior, request sizing, key/region assertions, delays, and transaction/commit/batch-coprocessor failpoints are retained. Rust futures replace `SendRequestAsync` callbacks without changing typed response behavior. |
| public protocol boundary | Client-go consumers name handler request/response types through the shared public `kvproto` module. Rust exposes the complete generated namespace at `tikv_client::proto`, so external crates can implement `CoprocessorHandler` and use every protocol type leaked by the public cluster, PD, session, and RPC surfaces. A downstream integration test proves the trait implementation rather than relying on same-crate visibility. |
| PD client | One process-wide monotonic TSO prevents duplicate timestamps across mock clients. Region/store routing, previous-region lookup, buckets/down peers, external timestamp monotonicity, legacy service safe points, modern transaction/GC safe points and barriers, source no-op scatter/split/operator responses, keyspace absence, default RU resource group, and the optional 200-ms GetRegion delay are represented through the native `PdClient` trait plus focused helpers. Source interface methods that return nil/zero/no-op map to the corresponding native default or absence rather than untyped placeholders. |
| reusable crate boundary | `unistore` is one standalone crate and the normal dependency direction is `tikv-client -> unistore`; protocol conversion stays in `tikv-client`, avoiding a cycle and allowing other modules to reuse the engine. This package receipt covers only the client-go mocktikv behavior hosted there. TiDB's much larger server package is outside the pinned source and explicitly `not-applicable`, not partially claimed. |

## Original tests and support artifacts

All 23 ordinary source test declarations have direct, independently selectable Rust identities named `source_go_internal_mockstore_mocktikv_<Go-name>`. There is no identity-generating macro, registered test-to-test call, or exact identity consisting only of a helper call. `TestMain` is the separate goleak/lifecycle disposition.

| client-go test | Direct Rust body |
| --- | --- |
| `TestMarshalmvccLock` | `source_go_internal_mockstore_mocktikv_TestMarshalmvccLock` |
| `TestMarshalmvccValue` | `source_go_internal_mockstore_mocktikv_TestMarshalmvccValue` |
| `TestRegionContains` | `source_go_internal_mockstore_mocktikv_TestRegionContains` |
| `TestGet` | `source_go_internal_mockstore_mocktikv_TestGet` |
| `TestGetWithLock` | `source_go_internal_mockstore_mocktikv_TestGetWithLock` |
| `TestDelete` | `source_go_internal_mockstore_mocktikv_TestDelete` |
| `TestCleanupRollback` | `source_go_internal_mockstore_mocktikv_TestCleanupRollback` |
| `TestReverseScan` | `source_go_internal_mockstore_mocktikv_TestReverseScan` |
| `TestScan` | `source_go_internal_mockstore_mocktikv_TestScan` |
| `TestBatchGet` | `source_go_internal_mockstore_mocktikv_TestBatchGet` |
| `TestScanLock` | `source_go_internal_mockstore_mocktikv_TestScanLock` |
| `TestScanWithResolvedLock` | `source_go_internal_mockstore_mocktikv_TestScanWithResolvedLock` |
| `TestCommitConflict` | `source_go_internal_mockstore_mocktikv_TestCommitConflict` |
| `TestResolveLock` | `source_go_internal_mockstore_mocktikv_TestResolveLock` |
| `TestBatchResolveLock` | `source_go_internal_mockstore_mocktikv_TestBatchResolveLock` |
| `TestGC` | `source_go_internal_mockstore_mocktikv_TestGC` |
| `TestRollbackAndWriteConflict` | `source_go_internal_mockstore_mocktikv_TestRollbackAndWriteConflict` |
| `TestDeleteRange` | `source_go_internal_mockstore_mocktikv_TestDeleteRange` |
| `TestRC` | `source_go_internal_mockstore_mocktikv_TestRC` |
| `TestCheckTxnStatus` | `source_go_internal_mockstore_mocktikv_TestCheckTxnStatus` |
| `TestRejectCommitTS` | `source_go_internal_mockstore_mocktikv_TestRejectCommitTS` |
| `TestMvccGetByKey` | `source_go_internal_mockstore_mocktikv_TestMvccGetByKey` |
| `TestTxnHeartBeat` | `source_go_internal_mockstore_mocktikv_TestTxnHeartBeat` |
| `TestMain` goleak harness | no spawned engine/cluster/PD tasks; handler and store close are explicit; both complete library configurations and doctests are awaited |

The former grouped mapping omitted source assertions even though its production conclusions were correct. A later exact-name layer made the tests selectable but still forwarded 22 names through `source_go_mocktikv_tests!`; that correction promoted those assertion bodies themselves to the exact identities and removed duplicate executions. The subsequent whole-body scan caught the one remaining false positive: `TestRegionContains` still only invoked `assert_source_test_region_contains`. Its exact body now executes all ten source boundary rows directly; the broader topology test may still reuse the helper as supplemental coverage. The direct ports retain every forward/reverse scan boundary and historical version, the distinct `ScanLock` start timestamps and four-lock inventory, resolved-lock setup, all resolve/batch-resolve keys and values, the second cleanup lock read, the post-delete timestamp, batch-pair errors, pre/post-GC visibility, intermediate delete-range snapshots, both RC timestamps, full transaction-status tuples/error metadata, exact marshal/region tables, and source heartbeat timestamps. All stronger tests pass without a production change.

The Rust matrix adds coverage needed by production files that have no dedicated
source test: pessimistic result/deadlock/rollback paths, ordered multiple wait
edges, graph cleanup on successful and failed terminal operations, retained
range-resolve edges, unconditional zero-current-TS cleanup, raw-KV operations
and the Go CRC64 vector, nonempty-path restoration, `MvccKey`, uneven region
grouping, session error responses, global TSO/resource groups/GC
barriers/previous-region routing, all three coprocessor forms, downstream
implementation of the public coprocessor trait, and transactional/raw/debug
RPC adaptation.

## Dependencies and consumers

The package's source dependencies are already complete: API codec, async utilities, internal client/cluster/deadlock/logging, locate contracts, metrics, oracle helpers, `tikvrpc`, and root `util`; required kvproto/PD protocol inputs are already generated in the Rust tree. The reusable state owner adds only existing workspace-compatible serde/farmhash dependencies.

Exact source matching finds nine direct external Go consumers:

- six `internal/locate` tests: `pd_codec_test.go`, `region_cache_test.go`, `region_request3_test.go`, `region_request_state_test.go`, `region_request_test.go`, and `replica_selector_test.go`;
- `rawkv/rawkv_test.go` and `tikv/kv_test.go` test fixtures;
- `testutils/mockstore.go`, the public test-support alias/factory facade.

The completed locate, RawKV, and root TiKV receipts already own their production algorithms and consume equivalent native cluster/transport boundaries. `testutils` is completed by its separate alias/factory receipt; this receipt supplies its concrete mock dependency but does not promote it. Additional integration suites consume mocktikv indirectly through that facade and remain assigned to their own package/live-differential gates.

## Validation contract

Completion requires 14/14 pinned artifact identity and the 6,689-line total; a 23-to-23 ordinary-test/Rust-identity bijection plus the `TestMain` disposition; the complete reusable-engine and mock-adapter matrices; both complete library and canonical workspace configurations; clean generation, all-target/all-feature compilation, strict Clippy, private rustdoc/doctests, rustfmt, and whitespace checks on `nightly-2026-08-22-aarch64-apple-darwin`. A real TiKV/PD cluster does not apply to this deterministic in-process package; live interoperability remains on the final differential milestone.

Independent re-audit evidence on 2026-08-26:

- exact source identity is `52c1e76cec993571493c81de442bcbef90cdc106`; all 14 hashes and 6,689 lines match, mechanical reconciliation finds 23 ordinary Go tests, 23 unique Rust identities, `TestMain`, and exactly nine direct consumers;
- Go 1.25.12 passes the package normally in 0.023s and under the race detector in 1.074s;
- all 23 direct ports pass in both feature selections, with no identity macro, test-to-test call, one-call-only helper identity, missing name, extra name, or duplicate name; UniStore passes its 22 package-owned identities and the client adapter passes `TestRegionContains`;
- canonical workspace matrices pass 1,274/1,249 tests with two/six configured skips;
- `make check` completes clean protocol generation, workspace all-target/all-feature checking, rustfmt, and strict Clippy; `make doc` completes private-item rustdoc and all 51 doctests; final formatting, source identity, inventory/declaration/consumer, and whitespace gates pass.

The public generated-protocol namespace, downstream `CoprocessorHandler` implementation, ordinary-build injected-client construction, authoritative-MemDB transaction path, and transactional missing-versus-empty regression remain covered by their external tests. This independent unit-test audit found no reason to alter those production remediations.
