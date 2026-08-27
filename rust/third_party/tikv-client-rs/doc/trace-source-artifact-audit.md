# `trace` source-artifact audit

This is the atomic completion receipt for client-go package `trace`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. Its Rust owner is the public `tikv_client::trace` module plus the five source consumer integrations, validated with `nightly-2026-08-22`.

## Complete source inventory

The package is exactly four files and 424 lines:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `flags.go` | 48 | `ee8afa668ed81d40e61e0d183d466e63db0455217c25dbf5f35ff32fcbdccb72` | `src/trace.rs` control flags and operators |
| `flags_test.go` | 82 | `310db6d5309347e511caca89a44b0659db6dcf690f1c15d796f06e5daa30d7d4` | four source-named tests in `src/trace.rs` |
| `trace.go` | 150 | `e754b02059f63eb711bccf14451f05f9d1593d08ebbc1bc250ef31146909b19c` | `src/trace.rs`, request dispatch, region cache, transaction batches, and lock resolver |
| `trace_test.go` | 144 | `9610976601822f8e406513b24e297d69af239f439834ff4ed83f4aaa733bfb40` | four source-named tests in `src/trace.rs` |

There is no `doc.go`, `TestMain`, benchmark, example, fixture, generated source or generator input, build/platform variant, package metadata, package-specific build file, or leak harness.

## Production mapping

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| `TraceControlFlags`, `Has`, `With` | The four non-overlapping `u64` bits occupy positions zero through three. Empty, individual, fluent, combined, direct bitwise, and idempotent operations match. |
| categories and global hooks | The known category values zero through three match, while transparent `Category(u32)` remains open to unknown/future values exactly like Go's public defined integer type; its zero/default value is transaction 2PC. Event, category-enabled, and control-extractor handlers are independently replaceable. `None` restores the source defaults: no-op events, disabled client categories, and TiKV request-category control. Handler locks are released before callbacks, preserving atomic replacement while allowing reentrant reconfiguration. |
| context values and trace IDs | Immutable, type-keyed `TraceContext` derivation is the native `context.Context` mapping. Trace IDs are owned, absent by default, and overridden only in derived contexts. `with_trace_context` supplies task-local operation scope; request plans and resolver state capture it before fan-out, and nested resolver RPCs reinstall it. |
| trace-control wire propagation | Every physical region request centrally clones its TiKV context, copies a non-empty trace ID, and always writes the extracted control flags before transport. This single typed boundary covers both source sender paths and every retry/shard without request-specific duplication. Store-scoped commands retain their intentional no-context behavior. |
| KV request events | Every physical send independently checks `CategoryKVRequest` and emits `kv.request.send` with command, region/version/conf-version, logical store/address (including forwarding routes), timeout, and redacted range. Send precedes admission/interceptors/transport; result follows response settlement, so admission and settlement failures produce the source send/failed-result envelope without dispatching or fabricating network bytes. Result and coprocessor-other-error checks are independent, matching source runtime handler replacement; result includes latency, success, error, and exact gogo-protobuf region-error text, while `cop.other_error` carries the complete logical route. |
| region-cache events | `BatchLocateKeyRanges` snapshots category enablement once, then emits the exact start, cache-summary, PD-request, PD-response, and merged event names at the source boundaries with typed range/region/location payloads and count/limit fields. |
| 2PC batch events | Retry ownership emits one prewrite or commit start/result lifecycle per logical physical batch, not once per transport retry. Region re-sharding creates the same recursive batch lifecycles as source. Start fields include start/commit timestamps, region, primary membership, and key count; result checks enablement independently. Prewrite reports request and response failures, while commit deliberately omits `commit.batch.result` for a key error carried by `CommitResponse`, matching the source's explicit request-error-only false event. |
| lock-resolution events | Non-empty resolution emits start before work and finish on success or error. Caller TS, lock count, read/lite classification, minimum TTL, ignored/read-through counts, and error text match the source fields. Resolver scope carries the originating trace context into status-check and cleanup RPCs. |

The five direct Go importers are `internal/locate/region_cache.go`, `internal/locate/region_request.go`, `txnkv/transaction/prewrite.go`, `txnkv/transaction/commit.go`, and `txnkv/txnlock/lock_resolver.go`. Their complete event and propagation call sites are integrated above rather than left as future consumer work. Rust's existing execution-detail and gRPC metadata tracing coexist in `src/trace.rs`; they are independently audited by their owning client-go packages and are not used to inflate this package claim.

## Complete unit-test mapping

The source declares exactly eight tests:

| Source declaration | Rust evidence |
| --- | --- |
| `TestTraceControlFlags_Has` | `source_go_trace_flags_test_TestTraceControlFlags_Has` |
| `TestTraceControlFlags_With` | `source_go_trace_flags_test_TestTraceControlFlags_With` |
| `TestTraceControlFlags_CombinedOperations` | `source_go_trace_flags_test_TestTraceControlFlags_CombinedOperations` |
| `TestTraceControlFlags_BitValues` | `source_go_trace_flags_test_TestTraceControlFlags_BitValues` |
| `TestTraceControlExtractor` | `source_go_trace_trace_test_TestTraceControlExtractor` |
| `TestTraceEventFunc` | `source_go_trace_trace_test_TestTraceEventFunc` |
| `TestIsCategoryEnabledFunc` | `source_go_trace_trace_test_TestIsCategoryEnabledFunc` |
| `TestTraceIDContext` | `source_go_trace_trace_test_TestTraceIDContext` |

Every source assertion is retained, including empty and partial flags, every false branch, context-sensitive extraction, immediate logging, nil resets, independent handlers, absent IDs, and nested override. Each exact identity is independently selectable and defined once. `task_local_trace_context_is_nested_and_restored` adds native scope restoration. Consumer regressions prove exact wire metadata and KV events, cache lifecycle, lock success/error lifecycle boundaries, and prewrite/commit batch lifecycle. Process-global tests use serialized mutation and unique trace-ID filtering so unrelated concurrent requests cannot contaminate assertions.

Five red/green differential corrections close source-uncovered public and integration boundaries. Unknown category `99` did not compile through the former closed enum; it now reaches both registered handlers. A CommitResponse key error formerly emitted an extra false result event. Forwarded KV events formerly reported the physical proxy address instead of the logical selected store. Region errors formerly exposed Rust's expanded `Debug` struct instead of gogo compact text. Finally, admission failure emitted no KV events and settlement failure was reported as success; send/result now enclose the complete intercepted call in source order. The two resource-control regressions also prove admission rejection performs no transport and no response settlement.

## Validation boundary

Completion requires exact source identity, hashes and line counts; all eight original tests; every direct consumer; exact Go execution under Go 1.25.12; focused/default/all-feature/source-derived Rust suites; all-target checking; strict Clippy; private-item rustdoc and doctests; rustfmt and whitespace checks.

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- `/private/tmp/go1.25.12/bin/go test ./trace -count=1` and the same command with `-tags nextgen`: passed.
- The normal and NextGen `-race` commands passed with an isolated fresh `GOCACHE`; the shared cache had first produced a pre-compilation `runtime/race: package testmain: cannot find package` after two race builds were launched concurrently.
- The exact `source_go_trace_` filter passed all eight direct source-test identities with both `--no-default-features` and `--all-features`.
- The focused `trace` filter passed 16 package/native-consumer tests in each feature configuration.
- All six red/green gates, including the forwarding source consumer, passed after their fixes.
- `make unit-test` passed 1,406 tests with two configured skips in the no-default workspace matrix and 1,370 tests with six configured skips in the all-feature library matrix.
- `make check` passed generation verification, all-workspace/all-target/all-feature checking, rustfmt, and Clippy with `-D warnings`.
- `make doc` passed private-item rustdoc with `-D warnings` and all 51 doctests.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.
- The Rust baseline before this reopened batch is `f69b83f83061ea9d1b2a3a84bb5b9b358bf34860`; the source checkout is exactly `52c1e76cec993571493c81de442bcbef90cdc106`, and recomputed line counts/SHA-256 values match all four inventory rows.

The source package itself requires no live TiKV/PD service. Typed mock transport and real request-plan execution validate the generated wire context and every event boundary deterministically; final repository live-cluster behavior remains covered by the independent repository receipt.
