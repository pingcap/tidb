# `oracle` source-artifact audit

This is the atomic completion receipt for client-go package `oracle`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. Its Rust owner is the public `tikv_client::oracle` module, validated with `nightly-2026-08-22`.

## Complete source inventory

The package is exactly one 157-line production artifact:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `oracle.go` | 157 | `f1a58d22b4e1a0f66d40d3048f34f92983bf8c3d74b00f62eb2672a2777aeb4d` | `src/oracle.rs` |

There is no package-local test, `TestMain`, benchmark, example, fixture, generated source or input, build/platform variant, package metadata, package-specific build file, or leak harness. The separate `oracle/oracles` child package has its own receipt.

Mechanical import inventory finds 48 Go files importing this package directly. They comprise the concrete child implementations plus config, latch, locate, mocktikv, raw KV, root store, RPC, transaction, lock, snapshot, example, and integration owners. Their uses are all nameable through the public Rust module; each consumer's algorithm remains assigned to its own package receipt.

## Production mapping

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| `Option` and `GlobalTxnScope` | `OracleOption` carries the transaction scope and the public default constant is exactly `"global"`. Empty-scope normalization remains the concrete PD oracle's responsibility, as in source. |
| `Oracle`, `ReadTSValidator`, `Future` | Object-safe async traits expose every source operation, including synchronous and asynchronous normal/low-resolution timestamps, stale timestamps, expiration, interval control, external timestamps, all-keyspace minimum TSO, close, and read validation. Dropping a Rust async operation is the native cancellation boundary for Go contexts. |
| timestamp layout helpers | Composition uses the exact 18-bit logical layout and wrapping signed arithmetic. Extraction, Unix-millisecond conversion, zero-logical conversion, and pre-epoch truncation match Go. |
| lower-limit timestamp | The signed millisecond input is converted through wrapping nanoseconds before changing the instant. This closes the prior large-duration divergence where Rust attempted an unbounded millisecond subtraction while Go's `time.Duration` multiplication wraps. |
| noop validator and typed errors | Validation always succeeds; future-read and latest-stale-read errors retain exact fields and text. |

`src/timestamp.rs` remains the native generated-PD-timestamp adapter and is not used to inflate this one-file package claim. Concrete allocation, caching, refresh, expiration, and validation behavior belongs to the independent `oracle/oracles` receipt.

## Test and validation boundary

The source package declares no Go tests. Rust therefore retains six deterministic source-derived tests covering trait object safety, ordinary and wrapping timestamp parts, pre/post-epoch millisecond conversion, lower-limit positive/negative and overflow behavior, noop validation, and exact typed errors.

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- `/private/tmp/go1.25.12/bin/go test ./oracle ./oracle/oracles -count=1`: parent reported no test files and child passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib oracle:: -- --nocapture`: 32 passed across the parent and child owners.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib source_ --quiet`: 558 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features source_ --quiet`: 555 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 879 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 876 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 check --all-targets --all-features`: passed.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short -- -D warnings`: passed cleanly.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features --document-private-items`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 51 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.

The Rust baseline before this batch is `b6ffad470e8f385fbdc125e4cdc123c032a88f24`; source identity, line count, and SHA-256 were recomputed from the pinned checkout. No live service is required for this interface/helper package.
