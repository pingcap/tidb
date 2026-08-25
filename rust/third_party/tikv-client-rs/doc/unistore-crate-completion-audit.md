# `unistore` native-crate completion audit

This is the atomic completion receipt for the reusable `unistore` workspace crate. Its behavioral source boundary is the storage portion of client-go's `internal/mockstore/mocktikv` package at pinned commit `52c1e76cec993571493c81de442bcbef90cdc106`; that complete 14-artifact/6,689-line Go inventory and its immutable hashes are recorded in [`internal-mockstore-mocktikv-source-artifact-audit.md`](internal-mockstore-mocktikv-source-artifact-audit.md). The crate is a native dependency/integration boundary, not a parity claim for TiDB's separately maintained `pkg/store/mockstore/unistore` server.

## Complete crate inventory

The standalone crate contains exactly five owned artifacts and 2,788 lines:

| Crate artifact | Lines | SHA-256 | Ownership |
| --- | ---: | --- | --- |
| `unistore/Cargo.toml` | 16 | `92ecf5beecbb04ca320b8dfe29fa77de419cb9335f446f3ad1b737abd69d6ee0` | independent package metadata and four direct dependencies |
| `unistore/src/lib.rs` | 19 | `39990643ba6075b2ddb5e67e79eecd48746521664f50161fb5e97f18ccf4e1ab` | crate contract, source boundary, and complete public re-exports |
| `unistore/src/mock.rs` | 2,453 | `75ab6886d30113ffa15efc2ff44c0388bcbdd8ba4441a9c282b757fef24e28a4` | source-mapped optimistic/pessimistic MVCC, raw KV, debug, persistence, records, errors, and 20 tests |
| `unistore/src/mvcc.rs` | 243 | `421cd7eee898b36fdfb0376b69c1caf96d220a46c20c004dc13f3a77bac86065` | native committed-version convenience facade and two tests |
| `unistore/tests/reuse.rs` | 57 | `1a609cdb776f33cf246da2fdded82fd94695766863eec5358473336ffb0d6c44` | two external-consumer tests proving independence from `tikv-client` |

There is no build script, feature split, generated input/output, example, benchmark, fixture, platform variant, unsafe code, or unpublished test-support artifact. The crate is a normal workspace member and `tikv-client -> unistore` is the only dependency direction.

## Complete public contract

Mechanical enumeration records 70 public type/function declaration points, with all fields and enum variants included through their owning definitions. The surface is grouped as follows:

- source types: operation, isolation, assertion, pessimistic action/wakeup, transaction mutation/request, pair, lock/write/debug records, status action, and exhaustive `MockError`;
- binary compatibility: lock and write marshal/unmarshal with source byte order, Go-uvarint lengths, malformed-input handling, and the 10-MiB slice guard;
- `MockEngine`: in-memory and directory-backed construction; SI/RC get, batch-get, forward/reverse scan; optimistic and pessimistic prewrite/lock/rollback/commit; cleanup/status/heartbeat; lock scans and resolution; GC/range deletion; every raw-CF operation, CAS, reflected Go CRC64 checksum; MVCC debugger lookup; and explicit close;
- native committed-version facade: `Timestamp`, `Mutation`, `VersionedValue`, `MvccError`, and cloneable `MvccStore` commit/get/scan/version-history operations.

Protocol/key encoding, kvproto conversion, cluster/PD/session behavior, and RPC dispatch remain in the consuming `tikv-client` crate. This keeps the state engine reusable and avoids a dependency cycle without moving behavior out of its completed source package receipt.

## Tests and consumers

The crate has 24 tests: 20 source-derived `MockEngine` tests, two native committed-version tests, and two integration tests compiled as an external crate consumer. The source-derived matrix covers record formats, Go CRC64, optimistic and pessimistic paths, SI/RC visibility, locks/resolution/rollback/conflicts, GC, delete range, status/heartbeat/debug, raw KV, and close/reopen persistence. The external tests prove both public APIs work with only `unistore` in scope.

Direct native consumption consists of the root Cargo dependency edge; five mocktikv adapter files (`cluster.rs`, `mod.rs`, `pd.rs`, `rpc.rs`, and `session.rs`); the hidden `testutils` facade; and the union-store integration test. The crate's own external test is deliberately included as consumer evidence. No consumer requires a reverse dependency from `unistore` to `tikv-client`.

## Scope boundary

TiDB's `pkg/store/mockstore/unistore` is a separate SQL/server-side implementation with its own packages, RPC service, row/index encoders, coprocessor executors, DDL/schema state, and TiDB test inventory. It is outside the pinned client-go repository and outside this parity goal. Treating this crate as a partial port of that package would violate the atomic-package rule, so the ledger marks the TiDB package `not-applicable` rather than `seed` or `complete`.

## Validation contract

Completion requires the exact five-artifact inventory and hashes; all public exports assigned; all 24 internal/external tests; independent crate compilation; both complete `tikv-client` library configurations; workspace/all-target/all-feature compilation and Clippy; rustdoc and doctests; rustfmt; whitespace checks; source identity; and the completed mocktikv source receipt on `nightly-2026-08-22-aarch64-apple-darwin`. Real TiKV/PD does not apply to this deterministic state crate; live protocol interoperability remains on the final differential milestone.

The final gate satisfies that contract. `cargo test -p unistore` passes all 22 unit tests and both external-consumer tests, with zero crate doctests; crate-only all-target Clippy passes under `-D warnings`. The complete default and all-feature `tikv-client` library configurations each pass 703 active tests with one intentional process-isolation ignore, and the workspace doctest run passes all 51 tests. Workspace/all-target/all-feature `cargo check` and Clippy, all-feature rustdoc, rustfmt, and `git diff --check` pass. The client-go source checkout is clean at `52c1e76cec993571493c81de442bcbef90cdc106`, and the linked mocktikv receipt reconfirms its exact source inventory.
