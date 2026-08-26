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
| categories and global hooks | Category discriminants zero through three match. Event, category-enabled, and control-extractor handlers are independently replaceable. `None` restores the source defaults: no-op events, disabled client categories, and TiKV request-category control. Handler locks are released before callbacks, preserving atomic replacement while allowing reentrant reconfiguration. |
| context values and trace IDs | Immutable, type-keyed `TraceContext` derivation is the native `context.Context` mapping. Trace IDs are owned, absent by default, and overridden only in derived contexts. `with_trace_context` supplies task-local operation scope; request plans and resolver state capture it before fan-out, and nested resolver RPCs reinstall it. |
| trace-control wire propagation | Every physical region request centrally clones its TiKV context, copies a non-empty trace ID, and always writes the extracted control flags before transport. This single typed boundary covers both source sender paths and every retry/shard without request-specific duplication. Store-scoped commands retain their intentional no-context behavior. |
| KV request events | Every physical send independently checks `CategoryKVRequest` and emits `kv.request.send` with command, region/version/conf-version, store, address, timeout, and redacted range. Result and coprocessor-other-error checks are independent, matching source runtime handler replacement; result includes latency, success, transport error, and immutable region-error text, while `cop.other_error` carries the complete route. |
| region-cache events | `BatchLocateKeyRanges` snapshots category enablement once, then emits the exact start, cache-summary, PD-request, PD-response, and merged event names at the source boundaries with typed range/region/location payloads and count/limit fields. |
| 2PC batch events | Retry ownership emits one prewrite or commit start/result lifecycle per logical physical batch, not once per transport retry. Region re-sharding creates the same recursive batch lifecycles as source. Start fields include start/commit timestamps, region, primary membership, and key count; result checks enablement independently and reports terminal success. |
| lock-resolution events | Non-empty resolution emits start before work and finish on success or error. Caller TS, lock count, read/lite classification, minimum TTL, ignored/read-through counts, and error text match the source fields. Resolver scope carries the originating trace context into status-check and cleanup RPCs. |

The five direct Go importers are `internal/locate/region_cache.go`, `internal/locate/region_request.go`, `txnkv/transaction/prewrite.go`, `txnkv/transaction/commit.go`, and `txnkv/txnlock/lock_resolver.go`. Their complete event and propagation call sites are integrated above rather than left as future consumer work. Rust's existing execution-detail and gRPC metadata tracing coexist in `src/trace.rs`; they are independently audited by their owning client-go packages and are not used to inflate this package claim.

## Complete unit-test mapping

The source declares exactly eight tests:

| Source declaration | Rust evidence |
| --- | --- |
| `TestTraceControlFlags_Has` | `source_test_trace_control_flags_has` |
| `TestTraceControlFlags_With` | `source_test_trace_control_flags_with` |
| `TestTraceControlFlags_CombinedOperations` | `source_test_trace_control_flags_combined_operations` |
| `TestTraceControlFlags_BitValues` | `source_test_trace_control_flags_bit_values` |
| `TestTraceControlExtractor` | `source_test_trace_control_extractor` |
| `TestTraceEventFunc` | `source_test_trace_event_func` |
| `TestIsCategoryEnabledFunc` | `source_test_is_category_enabled_func` |
| `TestTraceIDContext` | `source_test_trace_id_context` |

Every source assertion is retained, including empty and partial flags, every false branch, context-sensitive extraction, immediate logging, nil resets, independent handlers, absent IDs, and nested override. `task_local_trace_context_is_nested_and_restored` adds native scope restoration. Four consumer regressions prove exact wire metadata and KV events, cache lifecycle, lock success/error lifecycle boundaries, and prewrite/commit batch lifecycle. Process-global tests use serialized mutation and unique trace-ID filtering so unrelated concurrent requests cannot contaminate assertions.

## Validation boundary

Completion requires exact source identity, hashes and line counts; all eight original tests; every direct consumer; exact Go execution under Go 1.25.12; focused/default/all-feature/source-derived Rust suites; all-target checking; strict Clippy; private-item rustdoc and doctests; rustfmt and whitespace checks.

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- `/private/tmp/go1.25.12/bin/go test ./trace -count=1`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib trace::tests -- --nocapture`: 11 passed, including all eight source declarations.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib source_ --quiet`: 542 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features source_ --quiet`: 539 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 866 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 863 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 check --all-targets --all-features`: passed.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short -- -D warnings`: passed cleanly.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features --document-private-items`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 51 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.
- The Rust baseline before this batch is `08019fd45edf10a62805510ae9d194c978ebee2c`; the source checkout is exactly `52c1e76cec993571493c81de442bcbef90cdc106`, and recomputed line counts/SHA-256 values match all four inventory rows.

The source package itself requires no live TiKV/PD service. Typed mock transport and real request-plan execution validate the generated wire context and every event boundary deterministically; final repository live-cluster behavior remains covered by the independent repository receipt.
