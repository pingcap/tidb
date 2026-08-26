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

The inventory has 31 test declarations: three testify suite runners, 27 assertion-bearing suite methods, and `TestMain`. Every suite method has one independently selectable Rust identity named `source_go_<source-artifact>_<Go-name>`. Each identity owns its source scenario directly; there is no identity-generating macro, test-to-test call, or alias from multiple Go names to one aggregate Rust test. Shared helpers are limited to fixture construction and the two assertion tables that the Go mock suite itself shares. `rawkv.TestRawKV`, `integration_tests/raw.TestRawKV`, and `integration_tests/raw.TestAPI` remain runner dispositions; `integration_tests/raw.TestMain` remains the goleak/lifecycle disposition.

### Package suite: `rawkv/rawkv_test.go`

| Source method | Rust test evidence |
| --- | --- |
| `TestReplaceAddrWithNewStore` | `region_cache::test::source_go_rawkv_TestReplaceAddrWithNewStore` preloads the old store, removes it, installs the new store at the reused address, and checks tombstone/resolution state. |
| `TestUpdateStoreAddr` | `region_cache::test::source_go_rawkv_TestUpdateStoreAddr` swaps both addresses and proves the original store ID re-resolves to its new address. |
| `TestReplaceNewAddrAndOldOfflineImmediately` | `region_cache::test::source_go_rawkv_TestReplaceNewAddrAndOldOfflineImmediately` preloads both stores, removes the old one, and proves the surviving store changes address immediately. |
| `TestReplaceStore` | `region_cache::test::source_go_rawkv_TestReplaceStore` tombstones the old store and resolves a new store ID at the reused address. |
| `TestColumnFamilyForClient` | `raw::source_tests::source_go_rawkv_TestColumnFamilyForClient` owns the mutating-CF put/get/reset/delete sequence. |
| `TestColumnFamilyForOptions` | `raw::source_tests::source_go_rawkv_TestColumnFamilyForOptions` owns the clone-scoped per-operation CF sequence. |
| `TestBatch` | `raw::source_tests::source_go_rawkv_TestBatch` owns BatchPut, positional BatchGet, BatchDelete, and missing verification. |
| `TestScan` | `raw::source_tests::source_go_rawkv_TestScan` owns the exact CF forward-limit and key-only reverse results. |
| `TestDeleteRange` | `raw::source_tests::source_go_rawkv_TestDeleteRange` owns both suffix and unbounded deletion assertions. |
| `TestDeleteRangeEmptyKeysMultiRegion` | `raw::source_tests::source_go_rawkv_TestDeleteRangeEmptyKeysMultiRegion` owns the two-region unbounded deletion scenario. |
| `TestCompareAndSwap` | `raw::source_tests::source_go_rawkv_TestCompareAndSwap` owns disabled, mismatch, success, previous-value, and final-read assertions. |
| `TestRawChecksum` | `raw::source_tests::source_go_rawkv_TestRawChecksum` owns the exact six-pair CRC/count/byte calculation. |

### Mock integration suite: `integration_tests/raw/api_mock_test.go`

| Source method | Rust test evidence |
| --- | --- |
| `TestSimple` | `source_go_integration_raw_api_mock_TestSimple` |
| `TestRawBatch` | `source_go_integration_raw_api_mock_TestRawBatch`, including source-size generation, split-after-probe, four-plus payload windows, positional reads, and deletion. |
| `TestSplit` | `source_go_integration_raw_api_mock_TestSplit`, with writes before the source split and reads after it on the same client. |
| `TestScan` | `source_go_integration_raw_api_mock_TestScan`, with the exact ten-row forward table before either split and after each progressive split. |
| `TestReverseScan` | `source_go_integration_raw_api_mock_TestReverseScan`, with the exact eleven-row reverse table before either split and after each progressive split. |
| `TestDeleteRange` | `source_go_integration_raw_api_mock_TestDeleteRange`, with writes before three splits and the exact five-case mutation table. |

### Optional live suite: `integration_tests/raw/api_test.go`

| Source method | Rust deterministic port |
| --- | --- |
| `TestSimple` | `source_go_integration_raw_api_TestSimple` |
| `TestScan` | `source_go_integration_raw_api_TestScan` writes all 20,480 pairs, applies all 20 source split points after the write, and checks the 10,240-result limit/prefix assertions. |
| `TestReverseScan` | `source_go_integration_raw_api_TestReverseScan` preserves the source's lexical bounds and loop; because those bounds yield no rows, the same identity also carries a non-vacuous five-row reverse-limit check over the data. |
| `TestBatchOp` | `source_go_integration_raw_api_TestBatchOp` |
| `TestCAS` | `source_go_integration_raw_api_TestCAS` |
| `TestTTL` | `source_go_integration_raw_api_TestTTL` uses a deterministic clock for the half-TTL and expiry assertions. |
| `TestDeleteRange` | `source_go_integration_raw_api_TestDeleteRange` writes all 20,480 pairs, applies the source split after the write, and verifies the three sampled keys disappear. |
| `TestRawChecksum` | `source_go_integration_raw_api_TestRawChecksum` executes 20,480 pairs in V1 and V2 and checks CRC/count/exact prefix-aware bytes. |
| `TestEmptyValue` | `source_go_integration_raw_api_TestEmptyValue` owns every Get/BatchGet/scan/reverse/delete/BatchPut/CAS assertion. |

`TestRawKV`/`TestAPI` setup and teardown map to fresh typed fixtures per Rust test. Package failpoint-driven store changes map to four direct store-ID/address/tombstone transitions plus the completed sender retry tests. The mutable stateful mock reshapes one live fixture after writes, preserving source split order instead of substituting pre-split clients. API-version HTTP discovery maps to captured V1/V1TTL/V2 construction and request-context tests. `test_prob.go` maps to `RawClient::new_with_pd_client`, `pd_client`, mock routing/transport owners, `MAX_RAW_KV_SCAN_LIMIT`, and `RAW_BATCH_PUT_SIZE`. `util_test.go`'s flags and `TestMain` goleak harness map to Cargo's opt-in live feature, joined request futures, consuming close/drop tests, and both full library configurations.

Four additional differential regressions preserve behaviors that the source implements but its suite does not isolate: DeleteRange is sequential and stops at the first error; Checksum is sequential and ignores its legacy string error; RawBatchGet/RawScan ignore legacy pair errors; and an overlong RawScan response is returned whole instead of panicking or being truncated.

The source live suite runs only when `-with-tikv` is explicitly enabled and otherwise skips before setup. Rust retains an analogous `integration-tests` live smoke surface, but package correctness no longer depends on an unavailable local cluster because every assertion in the source's optional suite runs deterministically in the library gate.

## Dependencies and consumers

Production dependencies on `config`, `config/retry`, `internal/client`, `internal/kvrpc`, `internal/locate`, `metrics`, `oracle`, and `tikvrpc` are complete. The source imports root `tikv` for codec construction; Rust's exact codec/PD boundary is already owned by the completed `internal/locate` and API-codec receipts, so that claim is not duplicated. The source tests import `internal/mockstore/mocktikv`; Rust uses typed in-memory transport/PD fakes and completed region-cache tests instead, while that large package retains its independent completed receipt.

Exact import matching finds three direct external files: `examples/rawkv/rawkv.go`, `integration_tests/raw/api_mock_test.go`, and `integration_tests/raw/api_test.go`. The example's constructor/Get/Put/Delete flow is covered by doctests and the simple stateful matrix; both integration importers are mapped above. No dependent package is promoted by this receipt.

## Validation contract

Completion requires exact pinned identity/hashes/line counts; declaration-level reconciliation of all 31 source test declarations and every support hook; a 27-to-27 executable source/Rust identity bijection; the focused RawKV gate; the external ordinary-build injection test; all `source_` tests in both complete feature configurations; both complete library configurations; generated-code checking, all-target compilation, Clippy, rustdoc/doctests, rustfmt/diff checks, and `nightly-2026-08-22-aarch64-apple-darwin`. The optional live feature remains useful infrastructure smoke coverage but is not stronger evidence for these source assertions than the always-run stateful matrix and is not a default package gate.

Independent re-audit evidence on 2026-08-26:

- exact source identity is `52c1e76cec993571493c81de442bcbef90cdc106`; all eight hashes and 2,743 lines match this receipt, and mechanical reconciliation finds 31 declarations, 27 executable suite methods, 27 Rust identities, and no missing, extra, or duplicate identity;
- Go 1.25.12 package suites pass normal/race in legacy mode (8.776s/9.862s) and NextGen mode (8.803s/9.844s); the separate `integration_tests/raw` module passes normal/race (0.058s/1.327s), with its explicitly optional live suite skipped by source configuration;
- all 27 direct Rust ports pass in both no-default and all-feature selections; no identity macro, test forwarding call, missing name, extra name, or duplicate name remains;
- complete source-derived lists contain 1,049/1,040 no-default/all-feature tests; canonical workspace matrices pass 1,313/1,287 tests with two/six configured skips;
- `make check` completes clean protocol regeneration, workspace all-target/all-feature checking, rustfmt, and strict Clippy; `make doc` completes private-item rustdoc and all 51 doctests. Final rustfmt, source identity, inventory/declaration/importer, and whitespace gates pass on `nightly-2026-08-22-aarch64-apple-darwin`.
