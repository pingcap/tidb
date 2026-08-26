# `internal/client/mockserver` source-artifact audit

This is the atomic completion receipt for client-go package `internal/client/mockserver`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. Its Rust owner is test-support module `src/store/mockserver.rs`, backed by the generated Tonic TiKV client/message surface and reused by parent transport tests. Validation uses `nightly-2026-08-22`.

## Complete source inventory

The package is exactly one 196-line production/test-support artifact:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `internal/client/mockserver/mock_tikv_service.go` | 196 | `04a517a353b368c25f4ffa23f45fd2167358287e7b8bf276df3902d4cf3c2585` | `src/store/mockserver.rs` and generated Tonic bindings |

There is no package-local test, `TestMain`, benchmark, example, fixture, generated source or input, build/platform variant, package metadata, package-specific build file, build tag, or generation directive. The generated `tikvpb`, `kvrpcpb`, and `coprocessor` types/server paths are owned by the independently complete kvproto crate receipt.

Mechanical import inventory finds exactly five direct Go consumers, all tests: `internal/client/client_async_test.go`, `internal/client/client_fail_test.go`, `internal/client/client_test.go`, `internal/locate/region_request_test.go`, and `internal/locate/replica_selector_test.go`. Their transport, reconnect, timeout, forwarding, BatchCommands, selector, and failure algorithms retain the completed parent-package receipts; this package owns only the reusable server behavior they exercise.

## Production mapping and differential findings

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| supported TiKV RPCs | The narrow Tonic router implements exactly `KvGet`, `KvPrewrite`, `CoprocessorStream`, and bidirectional `BatchCommands`. Unary calls return empty typed responses, CoprocessorStream emits exactly one empty response, and every other generated route returns gRPC Unimplemented. |
| metadata checker | A replaceable synchronized checker runs at all four source route boundaries and propagates its status. Tonic supplies a typed metadata map rather than Go's generic `context.Context`; forwarding metadata and rejection behavior used by every direct consumer are retained. |
| BatchCommands hook/default | A replaceable thread-safe handler owns the complete response and does not advance default feedback state. Without a handler, one empty response is emitted per request ID and IDs are echoed with health feedback `{store_id: 1, slow_score: 1}`. |
| feedback sequence | Client-go initializes `feedbackSeq := 1` inside each `BatchCommands` RPC, increments it only for default responses, and therefore resets every new stream. Rust previously kept one atomic server-wide sequence; a loopback regression failed with `[1, 2]` across two streams and now passes with `[1, 1]` after sequence ownership moved into each Tonic stream. The parent batch-only test service uses the same per-stream owner. |
| start/address | Empty input binds loopback port zero; explicit input binds exactly as requested. Client-go always advertises `127.0.0.1:<bound-port>` even after a wildcard bind. Rust previously advertised `0.0.0.0:<port>`; a red-then-green loopback regression now proves source-exact normalization while preserving the requested listener bind. |
| lifecycle | Running state, ephemeral constructor, address/port reporting, stop, same-address restart, and force-closing active HTTP/2 streams map the source lifecycle. Rust returns typed I/O errors instead of `-1`, rejects duplicate start rather than leaking the old grpc-go server, and makes stop/drop safe and cancellation-aware. These are native ownership decisions outside source consumer behavior. |

grpc-go's one-minute connection-preface timeout has no public Tonic server-builder counterpart. It affects only clients that never complete an HTTP/2 preface, not any supported RPC contract or direct package consumer.

## Test and validation boundary

The source package declares no Go tests. Eight Rust tests cover the core and real loopback transport: default response fields and within-stream increment, handler replacement/no increment, checker replacement/errors, all four RPC routes, ephemeral lifecycle, duplicate-start rejection, same-address restart, hook and metadata behavior over gRPC, active-stream force-stop, per-stream feedback reset, and wildcard-bind loopback advertisement. The latter two regressions were run before their fixes and failed with `[1, 2]` versus `[1, 1]` and `0.0.0.0:<port>` versus `127.0.0.1:<port>` respectively.

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- Exact Go package compilation with task-local caches: passed and reported `[no test files]`.
- `cargo +nightly-2026-08-22 test -p tikv-client store::mockserver::tests --all-features -- --nocapture`: 8 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib source_ --quiet`: 571 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features source_ --quiet`: 568 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 888 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 885 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 check --all-targets --all-features`: passed.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short -- -D warnings`: passed cleanly.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features --document-private-items`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 51 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.

The Rust baseline before this batch is `061822de2d583782b2406f6b3c3bef40bc62938e`; source identity, line count, no-test/support boundary, and all five direct imports were recomputed from the pinned checkout. No live TiKV/PD cluster is required because this package is deterministic loopback transport test support.
