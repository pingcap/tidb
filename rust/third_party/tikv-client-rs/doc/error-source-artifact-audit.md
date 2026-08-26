# `error` source-artifact audit

This is the atomic completion receipt for client-go package `error`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. Its primary Rust owner is the public `tikv_client::error` module, with native wrapper, metric, and consumer integration in `src/common/errors.rs`, `src/stats.rs`, `src/tikv.rs`, and the ordered transaction buffers. Validation uses `nightly-2026-08-22`.

## Complete source inventory

The package is exactly two artifacts and 497 lines:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `error/error.go` | 429 | `19f231823e4eed0a30b52fbf025f94ada35e318ad56e345005aa18eebbe21a54` | `src/error.rs` plus native wrappers/consumers |
| `error/error_test.go` | 68 | `9c0615c45ec54aa7eae49af23eb7fe8fa84ce0911efa6481894ec223e020d991` | `source_test_extract_debug_info_str_from_key_err` |

There is no `doc.go`, `TestMain`, benchmark, example, fixture, generated source or input, build/platform variant, package metadata, package-specific build file, or leak harness. The generated kvproto messages consumed by the package are independently owned by the complete `kvproto` crate receipt.

Mechanical import inventory finds 58 direct Go importers: 34 production files and 24 tests. They span retry configuration, transport, region routing, mocktikv, unionstore ART/RBT, raw KV, root store/GC/split, range tasks, transaction commit/prewrite/pessimistic/file paths, lock resolution, snapshots, and integration suites. Their algorithms remain assigned to their own completed package receipts; this receipt owns the shared taxonomy, transformation, classification, formatting, redaction call, metric side effect, and public construction boundaries.

The complete direct-consumer inventory is:

| Owner | Production importers | Test importers |
| --- | --- | --- |
| `config/retry` | `backoff.go`, `config.go` | `backoff_test.go` |
| `integration_tests` | — | `1pc_test.go`, `2pc_test.go`, `assertion_test.go`, `async_commit_fail_test.go`, `async_commit_test.go`, `client_fp_test.go`, `isolation_test.go`, `lock_test.go`, `option_test.go`, `pipelined_memdb_test.go`, `safepoint_test.go`, `snapshot_fail_test.go`, `snapshot_test.go`, `ticlient_test.go` |
| `internal/client` | `client_batch.go`, `conn_pool.go` | — |
| `internal/locate` | `region_cache.go`, `region_request.go` | `region_request3_test.go`, `region_request_state_test.go`, `region_request_test.go` |
| `internal/mockstore/mocktikv` | `mvcc_leveldb.go`, `rpc.go` | — |
| `internal/unionstore` | `art/art.go`, `art/art_snapshot.go`, `memdb_art.go`, `memdb_rbt.go`, `mock.go`, `pipelined_memdb.go`, `rbt/rbt.go`, `rbt/rbt_snapshot.go`, `union_store.go` | `art/art_test.go`, `memdb_test.go`, `pipelined_memdb_test.go`, `union_store_test.go` |
| root KV/store | `kv/kv.go`, `rawkv/rawkv.go`, `tikv/gc.go`, `tikv/kv.go`, `tikv/split_region.go` | — |
| `txnkv/rangetask` | `delete_range.go` | — |
| `txnkv/transaction` | `2pc.go`, `commit.go`, `pessimistic.go`, `pipelined_flush.go`, `prewrite.go`, `txn.go`, `txn_file.go` | `batch_getter_test.go`, `txn_file_test.go` |
| `txnkv/txnlock` | `lock.go`, `lock_resolver.go` | — |
| `txnkv/txnsnapshot` | `scan.go`, `snapshot.go` | — |

## Production mapping and differential findings

| client-go surface | Rust behavior and correction |
| --- | --- |
| singleton errors and cluster-ID text | All 26 singleton identities and exact strings plus `MismatchClusterID` are public. Native predicates walk ordinary Rust sources and client-rust's own transparent/boxed wrappers. |
| structured errors | Query signal, deadlock, PD, key-exists/value, write conflict, latch conflict, retryable, transaction/key/entry size, PD timeout, both GC forms, token limit, assertion failure, and both `LockOnlyIfExists` errors retain source fields and text. Transaction/key size fields are now signed `isize`, matching Go `int`; real length consumers cast safely because Rust allocations cannot exceed `isize::MAX`. TSO-derived times use the native timezone-free `SystemTime` and source-style UTC text. |
| protobuf-backed `Error()` text | A descriptor-driven compact-text formatter emits declaration order, default omission, repeated values/messages, enum names, nested angle brackets, source trailing spaces, and gogo's three-digit octal byte/string escaping. This replaces hand-written partial renderers whose missing trailing spaces and hexadecimal non-ASCII escapes differed from source. |
| constructors and metrics | Both write-conflict constructors increment the source counter exactly once. Public `new_pd_server_timeout` restores the missing constructor and all three root-store consumers now use it. |
| key-error extraction | Ordering is failpoint override, redaction, conflict, retryable, assertion, abort, commit-TS-too-large, transaction-not-found, then fallback. Non-boolean failpoint values panic like Go's type assertion. The fallback now uses exact protobuf compact text instead of Rust `Debug`; source-selected mutation, typed results, warning sites, and conflict metrics remain intact. |
| error predicates | Not-found, undetermined, commit-TS-lag, key-exists, and write-conflict checks retain source chain behavior. `is_error_undetermined` and the other classifiers now recognize the actual native `Error` variants, including nested connection/API-codec/pessimistic wrappers, rather than only standalone inner values. |
| debug-info JSON | The original empty/unredacted/redacted outputs are byte-identical. Native serialization covers every current `MvccLock`, `MvccWrite`, and `MvccValue` field, default omission, numeric enums, repeated ordering, and standard base64; redaction works on a clone and leaves the response unchanged. |
| logging | `None` remains a no-op. Present errors log the source message plus a forced native backtrace, preserving client-go's error-and-stack observability through Rust's `log` facade. |

`src/common/errors.rs` is the native owned-error boundary rather than a second taxonomy. Its variants preserve concrete identities needed by public predicates and high-level callers. Keeping raw lock/key protobufs until their owning resolver/extractor consumes them is intentional and matches the corresponding client-go consumer branches.

## Complete original-test mapping

The source declares exactly one ordinary test:

| Source declaration | Rust evidence |
| --- | --- |
| `TestExtractDebugInfoStrFromKeyErr` | `source_test_extract_debug_info_str_from_key_err` |

The port retains every source setup, empty-debug branch, exact unredacted JSON byte, exact redacted JSON byte, and process-global redaction restore. Source-uncovered regressions additionally execute all 26 singletons, every structured type, negative Go-`int` sizes, exact generated-message text and fallback, non-ASCII/octal escaping, all extraction branches and priority, both failpoint result types, every native classifier wrapper, full debug-info schema serialization/redaction, write-conflict metrics, pre/post-epoch UTC formatting, and response immutability.

## Validation boundary

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- `/private/tmp/go1.25.12/bin/go test ./error -count=1`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib error::tests:: -- --nocapture`: 8 passed.
- Focused ART and RBT consumer modules: 7 passed in each.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib source_ --quiet`: 563 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features source_ --quiet`: 560 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 883 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 880 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 check --all-targets --all-features`: passed.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short -- -D warnings`: passed cleanly.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features --document-private-items`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 51 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.

The Rust baseline before this batch is `fd492b78f15e655ec34b17eef5a5d242918efa7f`; source identity, both line counts, both SHA-256 values, and all 58 imports were recomputed from the pinned checkout. No live cluster applies to this deterministic taxonomy/transformation package; physical retry, transport, transaction, and integration behavior remains proven by those completed consumer receipts and repository gates.
