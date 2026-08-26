# `rawkv` source-artifact audit

This is the atomic completion receipt for client-go's `rawkv` package and its owning external RawKV test directory at pinned commit `52c1e76cec993571493c81de442bcbef90cdc106`. The Rust owner is the public `tikv_client::RawClient` surface in `src/raw`, with native keyspace, request-plan, region-cache, metrics, and transport owners. Validation uses `nightly-2026-08-22`; this receipt does not promote root `tikv`, `internal/mockstore/mocktikv`, or any other package.

## Complete source inventory

The claim contains exactly eight source artifacts and 2,743 lines: the 1,008-line production file, 785 lines of package test/probe support, and 950 lines in the external RawKV matrix (927 Go lines plus 23 TiKV configuration lines).

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `rawkv/rawkv.go` | 1,008 | `39eda3088288623c8f1498996989828002f86c4a088a5e703ce10a4fb569d855` | `src/raw/{client,mod,lowering,requests}.rs`, request/keyspace/retry plans, and `src/stats.rs` |
| `rawkv/rawkv_test.go` | 731 | `aec46476bf5d0e46c70f2310d5b04b64aaf645998958e3d3ba1bf27dc2dea33e` | stateful source matrix in `src/raw/source_tests.rs`, focused RawKV request tests, and completed region-cache sender tests |
| `rawkv/test_prob.go` | 54 | `30bd2e1a4c0be9a390d6a0f546b7033c32a91c28c6646848ece819e7f239512f` | generic test constructor, public PD handle, native mock clients/regions, request capture, and boundary constants/tests |
| `integration_tests/raw/api_mock_test.go` | 339 | `da892feda2c4edb3ccca2a9a8e4c53e6eaa7734c34b2044469d1357b6a78e7dc` | deterministic multi-region stateful source matrix |
| `integration_tests/raw/api_test.go` | 531 | `bb5477578d0197f875714947c96f0e4503ea68431222959df809d1af5203c56b` | deterministic API/TTL/empty/CAS/checksum matrix; optional live smoke remains in `tests/integration_tests.rs` |
| `integration_tests/raw/util_test.go` | 57 | `bb5e46c8e368c63017c5bf23a7fcdcc336b307bfe4988eeab14d6578441c8502` | Cargo `integration-tests` opt-in plus joined native tasks/full-suite lifecycle gate |
| `integration_tests/raw/tikv-v1ttl.toml` | 11 | `e0d6f188785eca90ad5fe6d5bc61efbffe1266be8a98fbc22985e5a5bd6a50bc` | `RawApiVersion::V1Ttl` construction and captured V1/V1TTL request-context tests |
| `integration_tests/raw/tikv-v2.toml` | 12 | `84ecc668f9dfbfdb374db578ec5ef4dc62a23f9bfd7a93708eceb042b7a0b24d` | API V2 keyspace construction, boundary coding, response decoding, and captured context tests |

There is no package `doc.go`, `main_test.go`, benchmark, example test, generated input/output, package-local build file, metadata/`OWNERS`, platform variant, or other fixture. The three external Go test files carry `//go:build !nextgen`; the production/package-test/probe files have no build tag. Rust compiles the production surface in both feature selections and runs its deterministic matrix with and without `nextgen`, which is at least as strict as the source's exclusion of those external tests.

## Production mapping

| client-go surface | Rust behavior and native decision |
| --- | --- |
| options and construction | `Config` owns security, gRPC/PD settings, API selection, and canonical keyspace loading. V1 and V1TTL use raw V1 key coding; V2 resolves metadata and places the numeric ID plus canonical name in every request context. Unknown/identity keyspaces fail at construction. |
| `SetColumnFamily`, per-call column family, `ScanKeyOnly` | Mutating and clone-builder APIs retain arbitrary CF names and empty-name reset. Typed `scan_keys`/`scan_keys_reverse` are the native key-only forms; request lowering preserves source `cf` and `key_only` fields through shards/retries. |
| atomic mode and CAS | Mutating/clone APIs enable and disable atomic mode. Put/delete/batch flags, disabled-CAS rejection, nil-versus-empty previous values, failed/successful swaps, server errors, and returned previous values match source behavior. |
| client identity/lifecycle | Construction retains the PD cluster ID and shared PD handle. The ordinary-build generic `RawClient::new_with_pd_client` is the safe typed counterpart of source `ClientProbe` injection, so embedded stores need no internal-test feature. Consuming `close` releases this owner while independent Rust clones remain valid; final ownership drops PD/TiKV transports and their request stream. |
| Get/BatchGet | Missing versus present-empty values remain `None` versus `Some(Vec::new())`; BatchGet returns one positional value per input key, including missing and duplicate keys. As in the pinned source, RawBatchGet's legacy per-pair `KeyError` is ignored. The former unordered Rust pair API remains an explicitly named native extension. |
| Put/BatchPut/TTL | Per-key and per-pair TTLs, empty-TTL defaulting, exact validation text, 16-KiB payload partitioning, 512-key boundaries, legacy first-TTL wire field, and atomic flags survive sharding. Source key maps make every duplicate BatchPut occurrence use its final value/TTL; Rust now preserves that edge exactly. |
| Delete/BatchDelete/DeleteRange | Point/batch deletes, atomic flags, half-open ranges, bounded empty/reversed no-dispatch, source write execution budgets, response errors, and terminal keyspace boundaries match. DeleteRange is flagless and walks one boundary region at a time, with a fresh source backoffer per region and immediate stop on the first failure; it is not concurrent fan-out. |
| Scan/ReverseScan | The process-wide scan limit is an exported `AtomicU32`, retaining source mutability without a data race. Forward scans preserve V1 unbounded endpoints until wire lowering and stop at the terminal empty region boundary instead of restarting at the first region. Reverse scans route from upper bounds, walk region starts, retain source's unsupported-empty-upper result, enforce key-only and 10,240 limits, and preserve empty values. RawScan's legacy per-pair `KeyError` is ignored. TiKV is trusted to enforce the request limit: an overlong response is returned whole, not truncated or allowed to underflow a Rust counter. The signed negative-limit source case is unrepresentable in the native `u32` API and therefore needs no runtime branch. |
| Checksum | CRC64-ECMA per-pair values are XOR-reduced while walking one boundary region at a time; KV counts and byte counts (including V2 prefixes) are additive. Each region receives a fresh source backoffer. Pinned client-go does not inspect `RawChecksumResponse.error`, so Rust ignores it too. The source's unusual checksum shortcut remains attached to the RawKV size histogram. |
| retry/routing | Every point request and every scan/DeleteRange/Checksum region step creates a fresh cumulative 20-second retry budget. Batch operations fork parent/region states, cancel siblings after the first failure, and merge final-child accounting. Point/scan outer loops consume source region misses; completed `internal/locate` owns store address replacement, tombstone, liveness, peer, and sender error precedence. Single RawKV server errors are unwrapped at the public boundary so their text is exactly the source text. |
| high-level metrics | Get, BatchGet, Put/BatchPut, Delete, BatchDelete, scan/reverse, DeleteRange success/error, checksum, and logical pre-keyspace Put/GetTTL key/value sizes now update the exact source collectors and labels on success or error. Physical RPC metrics remain separately owned by transport dispatch. |
| test probes | Rust's ordinary-build generic injected constructor, `pd_client`, mock PD/regions, captured request hooks, public scan-limit atomic, and public hidden `RAW_BATCH_PUT_SIZE` replace unchecked pointer replacement while exposing every probe observation needed by source and downstream embedded tests. |

Rust's additional batch-scan and raw-coprocessor APIs remain compatible extensions; they do not weaken or replace any pinned client-go operation.

## Original test and support matrix

The inventory has 31 executable declarations: three top-level test entrypoints, 27 suite methods, and `TestMain`. The tables below map every suite method by source name; aggregate Rust tests deliberately retain complete source tables in one fixture instead of splitting assertions merely to mirror Go's suite syntax.

### Package suite: `rawkv/rawkv_test.go`

| Source method | Rust test evidence |
| --- | --- |
| `TestReplaceAddrWithNewStore` | `region_cache::tests::source_store_reresolve_updates_metadata_without_resetting_runtime_state`, `source_store_resolve_state_transition_matrix`, `request::plan::tests::source_store_identity_errors_stop_the_current_send_loop`, and `raw::client::tests::raw_get_retries_a_region_miss_with_its_cumulative_source_budget` |
| `TestUpdateStoreAddr` | the same re-resolution, store-identity, and RawGet retry tests, including address-version replacement after a store mismatch |
| `TestReplaceNewAddrAndOldOfflineImmediately` | the re-resolution/state-transition tests plus `region_cache::tests::source_replica_candidates_skip_tombstone_and_removed_stores` |
| `TestReplaceStore` | the state-transition, tombstone/removal candidate, and metadata-preserving re-resolution tests |
| `TestColumnFamilyForClient` | `raw::source_tests::source_package_column_family_client_and_option_cases` |
| `TestColumnFamilyForOptions` | `source_package_column_family_client_and_option_cases` plus clone-scoped CF writes/reads in `source_simple_batch_column_family_cas_and_empty_value_matrix` |
| `TestBatch` | `source_package_batch_and_compare_and_swap_cases`, including positional values and delete verification |
| `TestScan` | `source_package_scan_and_reverse_tables_hold_across_region_splits` plus the exact CF/key-only reverse assertion in `source_package_batch_and_compare_and_swap_cases` |
| `TestDeleteRange` | `source_package_delete_range_table_and_unbounded_multiregion_case` |
| `TestDeleteRangeEmptyKeysMultiRegion` | `source_package_delete_range_table_and_unbounded_multiregion_case` and `raw::client::tests::unbounded_delete_range_covers_every_mock_region` |
| `TestCompareAndSwap` | `source_package_batch_and_compare_and_swap_cases` |
| `TestRawChecksum` | `source_package_checksum_exact_pair_crc_count_and_bytes` |

### Mock integration suite: `integration_tests/raw/api_mock_test.go`

| Source method | Rust test evidence |
| --- | --- |
| `TestSimple` | `source_simple_batch_column_family_cas_and_empty_value_matrix` |
| `TestRawBatch` | `source_mock_api_raw_batch_exceeds_four_payload_windows`, plus exact 16-KiB/512-key request tests |
| `TestSplit` | the zero/one/two-split iterations in `source_package_scan_and_reverse_tables_hold_across_region_splits` |
| `TestScan` | the complete forward table in `source_package_scan_and_reverse_tables_hold_across_region_splits` |
| `TestReverseScan` | the complete reverse and bounded-reverse table in the same test |
| `TestDeleteRange` | the exact five-case mutation table in `source_package_delete_range_table_and_unbounded_multiregion_case` |

### Optional live suite: `integration_tests/raw/api_test.go`

| Source method | Rust deterministic port |
| --- | --- |
| `TestSimple` | `source_simple_batch_column_family_cas_and_empty_value_matrix` |
| `TestScan` | `source_live_api_scan_and_delete_range_scale_cases` writes all 20,480 pairs and checks the 10,240-result limit/prefix assertions across split regions |
| `TestReverseScan` | `source_package_scan_and_reverse_tables_hold_across_region_splits` executes the source reverse order, bounds, and limits over all three topologies |
| `TestBatchOp` | `source_simple_batch_column_family_cas_and_empty_value_matrix` |
| `TestCAS` | the nil/mismatch/success transitions in `source_simple_batch_column_family_cas_and_empty_value_matrix` |
| `TestTTL` | `source_live_api_ttl_uses_remaining_seconds_and_expires` uses a deterministic clock for the half-TTL and expiry assertions |
| `TestDeleteRange` | `source_live_api_scan_and_delete_range_scale_cases` writes and removes all 20,480 pairs across split regions |
| `TestRawChecksum` | `source_live_api_checksum_scale_counts_v1_and_v2_key_bytes` executes 20,480 pairs in V1 and V2 and checks CRC/count/exact prefix-aware bytes |
| `TestEmptyValue` | `source_live_api_empty_value_matrix_distinguishes_missing_everywhere` ports every Get/BatchGet/scan/reverse/delete/BatchPut/CAS assertion |

`TestRawKV`/`TestAPI` setup and teardown map to fresh typed fixtures per Rust test. Package failpoint-driven store changes map to the completed deterministic region-cache/sender transition tests. The mock suite's split helper maps to explicitly shaped zero/one/two/three-region fixtures; split is test setup, not a RawKV method. API-version HTTP discovery maps to captured V1/V1TTL/V2 construction and request-context tests. `test_prob.go` maps to `RawClient::new_with_pd_client`, `pd_client`, mock routing/transport owners, `MAX_RAW_KV_SCAN_LIMIT`, and `RAW_BATCH_PUT_SIZE`. `util_test.go`'s flags and `TestMain` goleak harness map to Cargo's opt-in live feature, joined request futures, consuming close/drop tests, and both full library configurations.

Four additional differential regressions preserve behaviors that the source implements but its suite does not isolate: DeleteRange is sequential and stops at the first error; Checksum is sequential and ignores its legacy string error; RawBatchGet/RawScan ignore legacy pair errors; and an overlong RawScan response is returned whole instead of panicking or being truncated.

The source live suite runs only when `-with-tikv` is explicitly enabled and otherwise skips before setup. Rust retains an analogous `integration-tests` live smoke surface, but package correctness no longer depends on an unavailable local cluster because every assertion in the source's optional suite runs deterministically in the library gate.

## Dependencies and consumers

Production dependencies on `config`, `config/retry`, `internal/client`, `internal/kvrpc`, `internal/locate`, `metrics`, `oracle`, and `tikvrpc` are complete. The source imports root `tikv` for codec construction; Rust's exact codec/PD boundary is already owned by the completed `internal/locate` and API-codec receipts, so that claim is not duplicated. The source tests import `internal/mockstore/mocktikv`; Rust uses typed in-memory transport/PD fakes and completed region-cache tests instead, while that large package retains its independent completed receipt.

Exact import matching finds three direct external files: `examples/rawkv/rawkv.go`, `integration_tests/raw/api_mock_test.go`, and `integration_tests/raw/api_test.go`. The example's constructor/Get/Put/Delete flow is covered by doctests and the simple stateful matrix; both integration importers are mapped above. No dependent package is promoted by this receipt.

## Validation contract

Completion requires exact pinned identity/hashes/line counts; declaration-level reconciliation of all 31 source test declarations and every support hook; the 48-test focused RawKV gate; the external ordinary-build injection test; all `source_` tests in both complete feature configurations; both complete library configurations; generated-code checking, all-target compilation, Clippy, rustdoc/doctests, rustfmt/diff checks, and `nightly-2026-08-22-aarch64-apple-darwin`. The optional live feature remains useful infrastructure smoke coverage but is not stronger evidence for these source assertions than the always-run stateful matrix and is not a default package gate. The host has no Go executable, so pinned Go tests are not rerun locally.

Final local evidence on 2026-08-26:

- exact source identity is `52c1e76cec993571493c81de442bcbef90cdc106`; all eight hashes and 2,743 lines match this receipt, and a mechanical check finds 31 executable declarations/27 suite methods with no name missing from the receipt;
- the focused `raw::` gate passes 48 tests, including 19 deterministic source-matrix tests; the ordinary no-feature external injection gate passes three tests;
- all `source_` tests pass in both configurations (499 each); the complete no-default workspace run has 831 active library tests, one intentional ignore, and every external/workspace crate test passing; the all-feature library run has the same 831 active tests and one intentional ignore;
- `make check` completes clean protocol regeneration, workspace all-target/all-feature checking, rustfmt, and strict all-target Clippy; `make doc` completes private-item rustdoc and all 51 doctests; `git diff --check` passes.
