# `internal/logutil` source-artifact audit

This is the atomic completion receipt for client-go package `internal/logutil`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The reusable implementation is public-but-hidden as `tikv_client::logutil`, validation uses `nightly-2026-08-22`, and no caller package is promoted by this receipt.

## Complete source inventory

The pinned package tree contains exactly three production artifacts and 237 lines:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `hex.go` | 99 | `a17526d2676cac2bff0aefc8b8b7ac332f8ac4e2108cd2910cd6aea0caabf069` | generic descriptor-backed `Hex` display in `src/logutil.rs` |
| `log.go` | 71 | `a017b9e70d4d198f0d0964f16f66540c0a3709f9a36e94826fcd1811967ec5ce` | background/context logger and test-sensitive warning assertion in `src/logutil.rs` |
| `tracing.go` | 67 | `1bcb70ce7b699dec4de4e87e7f9e9a2f34ec3b04ee910ad779a061aa44eea03e` | context span, replaceable event key, event formatting, and typed tags in `src/logutil.rs` |

There is no `doc.go`, source test, benchmark, example, fixture, metadata/`OWNERS`, generated input/output, package-local build file, build tag, or platform variant.

## Production mapping

| client-go surface | Rust behavior and native decision |
| --- | --- |
| `BgLogger` | `background_logger` returns the process-wide `log` facade logger. Existing production call sites continue to use the same facade macros and therefore the same registered owner. |
| `CtxLogKey`, `Logger` | `with_logger` stores an `Arc<dyn log::Log>` under a private typed `TraceContext` marker; `logger` returns that exact owner or the process-wide logger. Typed immutable derivation replaces Go's mutable `interface{}` context key without collisions or parent mutation. |
| `AssertWarn` | `assert_warn` panics in unit-test builds and otherwise emits a warning through the supplied logger. Rust callers format structured values into the message at the call site instead of transporting zap-specific fields. |
| `TraceEventKey` | A synchronized replaceable string defaults to `event`; reads clone one coherent value and replacements affect subsequent events. |
| `Event`, `Eventf` | `ContextSpan` is stored in `TraceContext`; absent spans are no-ops. Present spans receive the current event key and exact string, while `eventf` accepts native `fmt::Arguments` to retain formatting at the call boundary. |
| `SetTag` | The contextual span receives the original key and an arbitrary typed `Debug + Send + Sync` value. This keeps type information rather than narrowing tags to strings. |
| `Hex`, `hexStringer.String`, `prettyPrint` | `hex` accepts any generated `prost::Message + prost::Name`, encodes it once, resolves its exact descriptor, and recursively formats every schema field in declaration order. Direct/nested/repeated bytes always flow through `redact::key`; absent presence-bearing fields render `<nil>`; scalars, enums, lists, maps, messages, and oneofs have explicit deterministic branches. No message-specific table or derived `Debug` output is used. |

## Required generated dependency

The existing proto builder enables Prost type names and emits `kvproto/src/generated/file_descriptor_set.bin` from the complete checked-in schema set during the same regeneration that writes Rust messages. The clean generated surface contains exactly 977 `prost::Name` implementations across 39 files. The descriptor set contains 1,029 message declarations, is 728,127 bytes, and has SHA-256 `24f76f184559153b03af0ac526e7d9663771d5a8f9c592c1fb433d3ecc079853`; `prost-reflect` 0.15.3 decodes it once into the process-wide pool. Generation and compilation therefore prove that the formatter's type name and runtime descriptor come from one source graph.

## Direct consumers

Mechanical import search finds exactly 44 pinned Go files. The symbol inventory is 203 `Logger`, 162 `BgLogger`, five `Hex`, four `Event`, four `Eventf`, two `SetTag`, two `AssertWarn`, and two `CtxLogKey` references.

- Completed caller packages: `config/{config.go,retry/backoff.go,retry/config.go}`; `error/error.go`; `internal/apicodec/codec_v2.go`; `internal/client/{client.go,client_async.go,client_batch.go,client_test.go,conn_batch.go,conn_monitor.go,mockserver/mock_tikv_service.go}`; `internal/latch/latch.go`; `internal/locate/{region_cache.go,region_request.go,sorted_btree.go,store_cache.go}`; `internal/unionstore/{arena/arena.go,art/art_iterator.go,memdb_art.go,pipelined_memdb.go,union_iter.go}`; `oracle/oracles/pd.go`; `txnkv/rangetask/range_task.go`; `txnkv/transaction/{2pc.go,cleanup.go,commit.go,pessimistic.go,pipelined_flush.go,prewrite.go,txn.go,txn_file.go}`; `txnkv/txnlock/lock_resolver.go`; `txnkv/txnsnapshot/{scan.go,snapshot.go,snapshot_async.go}`; and `util/misc.go`. Their existing Rust `log`, `trace`, and `redact` owners remain authoritative for operational call-site placement. Both transaction commit-TS rejection paths now render the complete `CommitTsExpired` message through this generic `Hex` surface at the source logging boundary.
- `tikv/{gc.go,kv.go,logutil.go,safepoint.go,split_region.go}` and `internal/mockstore/mocktikv/{mvcc_leveldb.go,pd.go}` retain their separate completed receipts. This receipt supplies their reusable logger/assertion/span/Hex dependency but does not duplicate their algorithms or call-site integration claims.

## Test and validation mapping

The source package has no test declaration or support harness. Six native tests exercise the otherwise untested exported contract: contextual/global logger selection, test-build assertion panic, absent/present span events and typed tags, event-key replacement, nested/repeated protobuf bytes with redaction enabled and disabled, absent fields/oneofs, and descriptor availability for representative generated types.

Final validation on `nightly-2026-08-22-aarch64-apple-darwin` includes focused tests in both feature configurations, complete default/all-feature library suites, all-target compilation, Clippy, rustdoc/doctests, rustfmt/diff checks, clean proto regeneration, exact source identity/inventory/hashes, exact 977-name/1,029-descriptor-message reconciliation, descriptor size/hash, and exact 44-importer/symbol counts. No live TiKV or PD cluster is required: this package has no network behavior.
