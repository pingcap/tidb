# `testutils` source-artifact audit

This is the atomic completion receipt for client-go's `testutils` package at pinned commit `52c1e76cec993571493c81de442bcbef90cdc106`. The Rust owner is the ordinary-build public `tikv_client::testutils` facade over the completed cluster contract and mocktikv implementation. Validation uses `nightly-2026-08-22`.

## Complete source inventory

The package contains exactly one 61-line production artifact:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `testutils/mockstore.go` | 61 | `3058f8c39ddf01c66c1e5dd14cbb781a8a25a4f5994a61f353020db2c353fbd4` | `src/testutils.rs`, with visibility support in `src/lib.rs`, `src/mock.rs`, and `src/mock/cluster.rs` |

There is no `doc.go`, Go test, test/support file, fixture, package-specific build file, generated source/input, benchmark, example, `OWNERS`, platform variant, build tag, or `go:generate` directive. The sole artifact is always selected at the pinned revision.

## Complete facade surface and integration decisions

| client-go symbol | Rust surface and behavior |
| --- | --- |
| `Cluster` | Public hidden `testutils::Cluster` re-export of the complete `internal/mockstore/cluster` trait. `MockCluster` implements it. |
| `CoprRPCHandler` | `testutils::CoprRpcHandler`, the complete mocktikv coprocessor handler trait used by unary, batch, and streaming requests. Its generated argument/return types are publicly nameable through `tikv_client::proto`, and an external-crate test implements the trait. Rust acronym casing is idiomatic; the trait contract is unchanged. |
| `MVCCStore` | `testutils::MvccStore`, the reusable concrete `unistore::MockEngine`. The source exposes an interface; Rust exposes its complete stateful implementation because no dynamic dispatch is needed by consumers. |
| `MVCCPair` | `testutils::MvccPair`, the mock engine pair with key, value, commit timestamp, and typed error. |
| `MockCluster` | `testutils::MockCluster`, the concrete completed mocktikv cluster. |
| `MockClient` | `testutils::MockClient`, the concrete completed mocktikv RPC client. |
| `RPCSession` | `testutils::RpcSession`, the concrete request session with idiomatic Rust acronym casing. |
| `NewMockTiKV` | `testutils::new_mock_tikv`, forwarding path and optional owned coprocessor handler to the mocktikv factory and returning the client, cluster, and native PD-trait implementor in source order. Its concrete `MockPdClient` support type is re-exported so Rust downstream crates can name and constrain the source interface return. Empty paths stay in memory; nonempty paths retain mocktikv persistence behavior. |
| `BootstrapWithSingleStore` | `testutils::bootstrap_with_single_store`, directly re-exported from mocktikv. |
| `BootstrapWithMultiStores` | `testutils::bootstrap_with_multi_stores`, directly re-exported from mocktikv. |
| `BootstrapWithMultiRegions` | `testutils::bootstrap_with_multi_regions`, directly re-exported from mocktikv. |
| `ErrLocked` | `testutils::ErrLocked`, the consolidated typed `MockError`; callers distinguish the source lock error through its field-preserving `Locked` variant. Rust keeps one exhaustive mock error enum instead of parallel downcast-only structs. |

The facade and its mocktikv dependency are compiled in ordinary builds, exactly as the source package is: `testutils/mockstore.go` has no `intest` build tag. The separate `internal-tests` feature still models client-go's `util/intest` behavior, but is not an admission gate for this package. The facade introduces no state, task, transport, or lifecycle owner of its own.

## Tests and support evidence

The source package has no test or support artifacts. The Rust conformance test `testutils::tests::source_facade_aliases_factory_and_bootstrap_helpers` is intentionally source-derived: it proves that `MockCluster` implements the exported cluster contract; constructs every type alias; distinguishes `ErrLocked::Locked`; calls the factory; and executes all three bootstrap exports, including their ID/shape results. The no-feature external-crate `public_proto_tests` target proves that the re-exported handler is actually implementable downstream. The no-feature `mocktikv_transaction_tests` target names `MockPdClient`, constructs the factory topology, distinguishes missing from empty, commits a write staged directly in the authoritative MemDB, and reads it from a fresh transaction. Concrete storage, PD, RPC, and topology behavior remains proved by the atomic mocktikv receipt rather than duplicated in this facade package.

## Dependencies and consumers

Both direct source dependencies are complete: `internal/mockstore/cluster` owns the interface and `internal/mockstore/mocktikv` owns every concrete behavior. The source PD client interface maps to `MockPdClient`, which implements the native Rust `PdClient` trait. No new third-party dependency or generated protocol input is introduced.

Exact source matching finds 14 direct consumers:

- eleven integration artifacts: `integration_tests/2pc_test.go`, `assertion_test.go`, `async_commit_test.go`, `delete_range_test.go`, `pipelined_memdb_test.go`, `prewrite_test.go`, `range_task_test.go`, `raw/api_mock_test.go`, `split_test.go`, `txn_file_test.go`, and `util_test.go`;
- `internal/locate/pd_codec_test.go`, `tikv/kv_test.go`, and `txnkv/transaction/txn_file_test.go`.

Those consumers use only the factory, cluster interface, or three bootstrap helpers. Their algorithms remain assigned to their owning package or final integration/differential gate; this facade receipt does not promote any consumer package.

## Validation contract

Completion requires exact pinned source identity; the 1/1 artifact and 61-line inventory; all 12 exported aliases/functions assigned; all 14 direct consumers assigned; the focused facade conformance test; both complete library configurations; all-target/all-feature compilation and Clippy; rustdoc and doctests; rustfmt; and whitespace checks on `nightly-2026-08-22-aarch64-apple-darwin`. No real cluster applies to an alias/factory-only package; live consumer behavior remains on the final differential milestone.

The final gate satisfies that contract. The focused facade test passes; `cargo test --no-default-features --test mocktikv_transaction_tests --test public_proto_tests` passes all four external tests after failing before the fix because `tikv_client::testutils` was configured out. The complete no-default workspace matrix passes 760 tests and the all-feature library matrix passes 750 tests, with one configured skip in each; the workspace doctest run passes all 51 tests. Workspace/all-target/all-feature `cargo check`, strict Clippy, all-feature rustdoc, rustfmt, and `git diff --check` pass. The source checkout is clean at current upstream `master`, `52c1e76cec993571493c81de442bcbef90cdc106`, and mechanical enumeration reconfirms one 61-line artifact, 12 source exports, no test/support artifacts, and exactly 14 direct consumers.

Post-completion API remediation proves the facade from a real downstream crate: `public_proto_tests` implements `CoprRpcHandler` using public `tikv_client::proto` request and response types. The facade additionally re-exports `Keyspace` and the native concrete `MockPdClient` support type so downstream code can use the injected generic transaction constructor. Both downstream targets now pass without default features; neither mock construction nor request handling depends on `internal-tests`.
