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
| `integration_tests/raw/tikv-v2.toml` | 12 | `84ecc668f9dfbfdb374db578ec5ef4dc62a23f9bfd7a93708eceb042b7a0b24d` | API V2 keyspace construction, boundary coding, response truncation, and captured context tests |

There is no package `doc.go`, `main_test.go`, benchmark, example test, generated input/output, package-local build file, metadata/`OWNERS`, platform variant, or other fixture. The three external Go test files carry `//go:build !nextgen`; the production/package-test/probe files have no build tag. Rust compiles the production surface in both feature selections and runs its deterministic matrix with and without `nextgen`, which is at least as strict as the source's exclusion of those external tests.

## Production mapping

| client-go surface | Rust behavior and native decision |
| --- | --- |
| options and construction | `Config` owns security, gRPC/PD settings, API selection, and canonical keyspace loading. V1 and V1TTL use raw V1 key coding; V2 resolves metadata and places the numeric ID plus canonical name in every request context. Unknown/identity keyspaces fail at construction. |
| `SetColumnFamily`, per-call column family, `ScanKeyOnly` | Mutating and clone-builder APIs retain arbitrary CF names and empty-name reset. Typed `scan_keys`/`scan_keys_reverse` are the native key-only forms; request lowering preserves source `cf` and `key_only` fields through shards/retries. |
| atomic mode and CAS | Mutating/clone APIs enable and disable atomic mode. Put/delete/batch flags, disabled-CAS rejection, nil-versus-empty previous values, failed/successful swaps, server errors, and returned previous values match source behavior. |
| client identity/lifecycle | Construction retains the PD cluster ID and shared PD handle. Consuming `close` releases this owner while independent Rust clones remain valid; final ownership drops PD/TiKV transports and their request stream. Generic mock-backed clients now clone with identical semantics. |
| Get/BatchGet | Missing versus present-empty values remain `None` versus `Some(Vec::new())`; BatchGet returns one positional value per input key, including missing and duplicate keys. The former unordered Rust pair API remains an explicitly named native extension. |
| Put/BatchPut/TTL | Per-key and per-pair TTLs, empty-TTL defaulting, exact validation text, 16-KiB payload partitioning, 512-key boundaries, legacy first-TTL wire field, and atomic flags survive sharding. Source key maps make every duplicate BatchPut occurrence use its final value/TTL; Rust now preserves that edge exactly. |
| Delete/BatchDelete/DeleteRange | Point/batch deletes, atomic flags, half-open ranges, bounded empty/reversed no-dispatch, unbounded multi-region traversal, source write execution budgets, response errors, and terminal keyspace boundaries match. DeleteRange remains flagless. |
| Scan/ReverseScan | The process-wide scan limit is an exported `AtomicU32`, retaining source mutability without a data race. Forward scans preserve V1 unbounded endpoints until wire lowering and stop at the terminal empty region boundary instead of restarting at the first region. Reverse scans route from upper bounds, walk region starts, retain source's unsupported-empty-upper result, enforce key-only and 10,240 limits, and preserve empty values. |
| Checksum | CRC64-ECMA per-pair values are XOR-reduced across all region shards; KV counts and byte counts (including V2 prefixes) are additive. The source's unusual checksum shortcut remains attached to the RawKV size histogram. |
| retry/routing | Every operation creates a fresh cumulative 20-second retry budget. Batch operations fork parent/region states, cancel siblings after the first failure, and merge final-child accounting. Point/scan outer loops consume source region misses; completed `internal/locate` owns store address replacement, tombstone, liveness, peer, and sender error precedence. |
| high-level metrics | Get, BatchGet, Put/BatchPut, Delete, BatchDelete, scan/reverse, DeleteRange success/error, checksum, and logical pre-keyspace Put/GetTTL key/value sizes now update the exact source collectors and labels on success or error. Physical RPC metrics remain separately owned by transport dispatch. |
| test probes | Rust's generic typed client, `pd_client`, mock PD/regions, captured request hooks, public scan-limit atomic, and request batching tests replace unchecked pointer replacement while exposing every probe observation needed by the source tests. |

Rust's additional batch-scan and raw-coprocessor APIs remain compatible extensions; they do not weaken or replace any pinned client-go operation.

## Original test and support matrix

`rawkv/rawkv_test.go` has one suite entrypoint and 12 suite methods. All four store-address/replacement/offline/tombstone methods are assigned to the completed region-cache re-resolution/state-transition tests, store-identity sender tests, and RawGet region-miss retry test. Its two column-family, Batch, Scan, two DeleteRange, CAS, and checksum methods are covered by the stateful multi-region source matrix plus exact wire-boundary tests.

`api_mock_test.go` has one suite entrypoint and six methods (`Simple`, `RawBatch`, `Split`, `Scan`, `ReverseScan`, `DeleteRange`). `api_test.go` has one opt-in suite entrypoint and nine methods (`Simple`, `Scan`, `ReverseScan`, `BatchOp`, `CAS`, `TTL`, `DeleteRange`, `RawChecksum`, `EmptyValue`). The deterministic backend executes all of those behaviors over three regions, including empty-value identity, duplicate/missing positional reads, logical clock TTL expiry, key-only scans, complete/unbounded deletion, CAS transitions, and CRC64. Existing captured tests separately cover V1TTL, V2 context/keyspace coding, 20-second execution durations, response errors, retry/cancellation topology, custom CF reset, and payload/count boundaries.

The source live suite runs only when `-with-tikv` is explicitly enabled and otherwise skips before setup. Rust retains an analogous `integration-tests` live smoke surface, but package correctness no longer depends on an unavailable local cluster because every assertion in the source's optional suite runs deterministically in the library gate. `util_test.go`'s goleak harness maps to the absence of unowned RawKV workers, consuming close tests, joined request futures, and both full library suites.

## Dependencies and consumers

Production dependencies on `config`, `config/retry`, `internal/client`, `internal/kvrpc`, `internal/locate`, `metrics`, `oracle`, and `tikvrpc` are complete. The source imports root `tikv` for codec construction; Rust's exact codec/PD boundary is already owned by the completed `internal/locate` and API-codec receipts, so root `tikv` is not promoted. The source tests import `internal/mockstore/mocktikv`; Rust uses typed in-memory transport/PD fakes and completed region-cache tests instead, so that large package retains its independent incomplete claim.

Exact import matching finds three direct external files: `examples/rawkv/rawkv.go`, `integration_tests/raw/api_mock_test.go`, and `integration_tests/raw/api_test.go`. The example's constructor/Get/Put/Delete flow is covered by doctests and the simple stateful matrix; both integration importers are mapped above. No dependent package is promoted by this receipt.

## Validation contract

Completion requires exact pinned identity/hashes/line counts, all 33 focused RawKV tests in all-feature and no-default configurations, the affected keyspace codec suite, both complete library configurations, all-target compilation, Clippy, rustdoc/doctests, rustfmt/diff checks, and the deterministic lifecycle gate on `nightly-2026-08-22-aarch64-apple-darwin`. The optional live feature remains useful infrastructure smoke coverage but is not stronger evidence for these source assertions than the always-run stateful matrix and is not a default source gate.
