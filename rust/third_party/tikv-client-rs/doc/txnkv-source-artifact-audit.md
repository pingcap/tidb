# root `txnkv` source-artifact audit

This is the atomic completion receipt for client-go's root `txnkv` package at pinned commit `52c1e76cec993571493c81de442bcbef90cdc106`. The primary Rust owner is the public `tikv_client::txnkv` facade in `src/txnkv.rs`; the embedded store and complete native implementations remain in `src/tikv.rs` and `src/transaction`. Validation uses `nightly-2026-08-22`.

## Complete source inventory

The claim contains exactly seven package-level artifacts and 308 lines: five production/export files (277 lines), one compile-only external-package test (26 lines), and one metadata file (5 lines).

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `txnkv/OWNERS` | 5 | `bf7e68b3b2ef151a695510e94a36ab19dd7f0fb2055e3815c42d41befbc39373` | metadata-only ownership policy; no runtime or build mapping |
| `txnkv/client.go` | 138 | `c99ccb84fe02231da7b6a979bcbb38fc694b074a0d2728fcca327f4edd6f5b2c` | `txnkv::{Client,ClientOption,ClientOptions,new_client}`, embedded `tikv::KvStore`, global configuration, timestamp and close lifecycle |
| `txnkv/client_test.go` | 26 | `5b4a7ba990b810ba8c6175cb1b100956e6ea81f1e259d6e784b74e4fb2e8074c` | compile-only value/reference close-surface assertions and focused close-order test in `src/txnkv.rs` |
| `txnkv/lock_export.go` | 34 | `e5b763c7990457bfd5385a4bec7b70efb61084b9b366fd3c80ac09e2e7a1c0f6` | public parsed `transaction::Lock`, `LockResolver`, `TxnStatus`, and `txnkv::new_lock` |
| `txnkv/snapshot_export.go` | 42 | `1e27d2e7313ee6351a46a87a7427fcc27ca2e3862e0489214b146d7a9ed2b8d7` | `Scanner`, `KvSnapshot`, `SnapshotRuntimeStats`, `IsoLevel`, `ReplicaReadAdjuster`, and SI/RC/RC-check-TS constants |
| `txnkv/transaction_export.go` | 36 | `64792455f59a07580e8c66c138d73e49f03591bd2a9d79e85a2276e2a09b8b9d` | `KvTxn`, binlog/filter/schema traits, schema-version alias, and `MAX_TXN_TIME_USE` |
| `txnkv/util_export.go` | 27 | `60b5f840abc354d417fbd287887a78c84bda86054b3cf8c126fb0b15d96e6254` | `Priority` and high/normal/low constants |

There is no package `doc.go`, generated source/input, fixture, package build file, benchmark, example test, platform variant, or build tag. `OWNERS` is the only non-Go artifact and has no Rust runtime counterpart.

## Production behavior and native integration decisions

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| option application | `ClientOption` and `ClientOptions` preserve source order and last-option-wins behavior. V1 is the default and ignores a keyspace option; V2 carries the supplied keyspace and lets the completed codec canonicalize an empty name to `DEFAULT`; V1TTL is rejected with source text `unknown api version: 1`. Rust's closed generated enum replaces arbitrary integer API versions. |
| global configuration | Construction clones `config::get_global_config`, then applies the root package's API/keyspace selection without mutating the process global. TLS, PD timeout/forwarding, transport sizing, transaction-file settings, and local-latch capacity flow through the completed configuration and root-store constructors. |
| store topology | `Client` owns and dereferences to one complete `tikv::KvStore`, preserving the source embedded-store surface without duplicating PD, region cache, transport, lock resolver, visibility, or safe-TS owners. V1 and V2 use the completed transaction codec paths and source UUID `tikv-{cluster_id}`. |
| safe-point namespace | `with_safe_point_kv_prefix` reaches `StoreRuntime` and the lazily owned compatibility `EtcdSafePointKv`; namespaced reads therefore use the same prefix that source passes to `NewEtcdSafePointKV`. Modern PD GC-state reads remain keyspace-scoped and do not misuse the etcd prefix. |
| timestamp | `Client::get_timestamp` returns the packed global timestamp version from the shared PD TSO owner. Async future cancellation replaces Go's caller context while the completed PD retry/TSO transport supplies the source global scope. |
| close | `Client::close(&self)` closes the shared root store first and always drops the process-wide txn-file uploader's idle HTTP pool afterward, even when store close returns an error. Borrowed close plus shared idempotent owners maps both Go value and pointer method sets without consuming the visible client object. |
| lock exports | `new_lock` copies every source field into an owned `Lock`, preserving unknown numeric lock operations, pessimistic/shared classification, redacted display, and txn-file identity. Rust lock resolution continues to use complete protobuf `LockInfo` records internally so shared wrappers retain nested holder data. `TxnStatus` exposes committed/rolled-back/determined/cacheable/commit-TS/TTL/action/equality behavior over the completed resolver status state. |
| snapshot/transaction exports | Rust re-exports the existing generic scanner, snapshot, transaction, runtime stats, replica adjuster, binlog/filter/schema traits, priority, isolation levels, and lifetime constant under one package facade. Lifetimes, generics, snake-case fields, and upper-snake constants are native type-system mappings, not omitted behavior. |

## Test and support reconciliation

The only source test is `client_test.go`; it has no runtime test declaration and compile-checks that both `Client` and `*Client` satisfy `io.Closer`. Rust's focused compile witness proves close is callable from both an owned value and a reference. The same module also executes five source-derived tests covering ordered option application and all API branches, every exported alias/constant, complete `NewLock` conversion, all exported transaction-status predicates, and store-before-txn-file close ordering including the store-error path.

All production dependencies are complete: `config`, `config/retry`, `oracle`, root `tikv`, `txnkv/{transaction,txnlock,txnsnapshot,txnutil}`, `util`, and the directly used `kvrpcpb` input/generated binding. Rust's native async owner and option builder reuse those receipts rather than recreating their package internals.

## Direct consumers

Exact quoted-import matching finds 17 Go files: eight examples (`gcworker` and seven `examples/txnkv` programs), eight external integration files (`2pc`, async-commit, lock, pipelined-MemDB, scan, snapshot-failure, snapshot, and store), and the external-package compile test. Their used surfaces are `Client` construction/close, V2 keyspace options, `KvTxn`, `Lock`, `TxnStatus`, `SnapshotRuntimeStats`, and priority constants. The public Rust facade now supplies each surface; the examples and live integration workflows retain their separate final non-package validation gate and are not promoted by this receipt.

## Validation contract

Completion requires mechanical 7/7 artifact identity and 308-line reconciliation; the five focused root tests; both complete default and all-feature library suites; all-target/all-feature compilation; Clippy; rustdoc and doctests; rustfmt/diff checks; and exact 17-consumer reconciliation on `nightly-2026-08-22-aarch64-apple-darwin`. Constructor interoperability with a live PD/TiKV cluster remains assigned to the final differential matrix rather than being inferred from the compile-only upstream test.

The final package gate mechanically verifies 7/7 artifacts, 308 lines, and 17/17 consumers. The focused matrix passes 5/5 tests; both complete library configurations pass 694 active tests with one intentional process-isolation test ignored; all targets compile; all-target Clippy and rustdoc succeed; and all 51 doctests pass. Rustfmt and whitespace checks are clean. The warning-only check/Clippy output is the repository's existing backlog; the new root facade emits no lint warning.
