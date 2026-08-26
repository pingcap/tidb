# `unistore` native-crate completion audit

This is the atomic completion receipt for the reusable `unistore` workspace
crate. Its behavioral source boundary is the storage portion of client-go's
`internal/mockstore/mocktikv` package plus that package's
`internal/mockstore/deadlock` dependency at pinned commit
`52c1e76cec993571493c81de442bcbef90cdc106`. Their immutable source receipts are
[`internal-mockstore-mocktikv-source-artifact-audit.md`](internal-mockstore-mocktikv-source-artifact-audit.md)
and
[`internal-mockstore-deadlock-source-artifact-audit.md`](internal-mockstore-deadlock-source-artifact-audit.md).
The crate is a native dependency/integration boundary, not a parity claim for
TiDB's separately maintained `pkg/store/mockstore/unistore` server.

## Complete crate inventory

The standalone crate contains exactly six owned artifacts and 3,893 lines:

| Crate artifact | Lines | SHA-256 | Ownership |
| --- | ---: | --- | --- |
| `unistore/Cargo.toml` | 16 | `92ecf5beecbb04ca320b8dfe29fa77de419cb9335f446f3ad1b737abd69d6ee0` | independent package metadata and four direct dependencies |
| `unistore/src/lib.rs` | 21 | `90a2bc4864b40dc3369f693bb266db431bdc17ff8f8d66d3bc883cbf13b62b4c` | crate contract, source boundaries, and complete public re-exports |
| `unistore/src/deadlock.rs` | 241 | `c9b79ee5285b5fe0cd29d774ec4df18b7bfcf8fa5acdf4392009344d850aa1da` | single reusable client-go wait-for graph and four unit tests |
| `unistore/src/mock.rs` | 3,300 | `870be11706476d9a9e3c99d89e1cb9f1d53c9cd4e250b7f849b99bd0b55a1e38` | source-mapped optimistic/pessimistic MVCC, raw KV, debug, persistence, records, errors, and 37 direct/native tests |
| `unistore/src/mvcc.rs` | 243 | `421cd7eee898b36fdfb0376b69c1caf96d220a46c20c004dc13f3a77bac86065` | native committed-version convenience facade and two tests |
| `unistore/tests/reuse.rs` | 72 | `d62d5f471856bef71e1e9f867a6af2a8742786d97a40632fbb5f4c4d0513fa7b` | three external-consumer tests proving engine, facade, and detector independence from `tikv-client` |

There is no build script, feature split, generated input/output, example, benchmark, fixture, platform variant, unsafe code, or unpublished test-support artifact. The crate is a normal workspace member and `tikv-client -> unistore` is the only dependency direction.

## Complete public contract

Mechanical enumeration records 80 public declaration points, with all fields
and enum variants included through their owning definitions. The surface is
grouped as follows:

- source types: operation, isolation, assertion, pessimistic action/wakeup, transaction mutation/request, pair, lock/write/debug records, status action, and exhaustive `MockError`;
- deadlock graph: reusable detector/error types, detection, all-edge and
  exact-edge cleanup, and strict timestamp expiry;
- binary compatibility: lock and write marshal/unmarshal with source byte order, Go-uvarint lengths, malformed-input handling, and the 10-MiB slice guard;
- `MockEngine`: in-memory and directory-backed construction; SI/RC get, batch-get, forward/reverse scan; optimistic and pessimistic prewrite/lock/rollback/commit; cleanup/status/heartbeat; lock scans and resolution; GC/range deletion; every raw-CF operation, CAS, reflected Go CRC64 checksum; MVCC debugger lookup; and explicit close;
- native committed-version facade: `Timestamp`, `Mutation`, `VersionedValue`, `MvccError`, and cloneable `MvccStore` commit/get/scan/version-history operations.

Protocol/key encoding, kvproto conversion, cluster/PD/session behavior, and RPC dispatch remain in the consuming `tikv-client` crate. This keeps the state engine reusable and avoids a dependency cycle without moving behavior out of its completed source package receipt.

## Tests and consumers

The crate has 46 tests: four detector unit tests, 37 source-derived/native
`MockEngine` tests, two native committed-version tests, and three integration
tests compiled as an external crate consumer. The source-derived matrix covers
record formats, Go CRC64, optimistic and pessimistic paths, SI/RC visibility,
locks/resolution/rollback/conflicts, zero-timestamp unconditional cleanup,
multiple wait edges, terminal and range-resolve graph cleanup, GC, delete
range, status/heartbeat/debug, raw KV, and close/reopen persistence. The external tests prove all three public
subsystems work with only `unistore` in scope.

Direct native consumption consists of the root Cargo dependency edge; five
mocktikv adapter files (`cluster.rs`, `mod.rs`, `pd.rs`, `rpc.rs`, and
`session.rs`); the hidden `testutils` facade; and the union-store integration
test. The engine directly owns the exported detector, and the crate's external
test proves it is independently reusable. No consumer requires a reverse
dependency from `unistore` to `tikv-client`.

## Scope boundary

TiDB's `pkg/store/mockstore/unistore` is a separate SQL/server-side implementation with its own packages, RPC service, row/index encoders, coprocessor executors, DDL/schema state, and TiDB test inventory. It is outside the pinned client-go repository and outside this parity goal. Treating this crate as a partial port of that package would violate the atomic-package rule, so the ledger marks the TiDB package `not-applicable` rather than `seed` or `complete`.

## Validation contract

Completion requires the exact six-artifact inventory and hashes; all public
exports assigned; all 46 internal/external tests; independent crate
compilation; both complete `tikv-client` library configurations;
workspace/all-target/all-feature compilation and Clippy; rustdoc and doctests;
rustfmt; whitespace checks; source identity; and both completed source receipts
on `nightly-2026-08-22-aarch64-apple-darwin`. Real TiKV/PD does not apply to
this deterministic state crate; live protocol interoperability remains on the
final differential milestone.

The final gate passes all 43 unit tests and three external-consumer tests, with
zero crate doctests; crate-only all-target Clippy passes with warnings denied.
The complete source packages pass ordinary and race tests under Go 1.25.12,
and all direct downstream mocktikv adapter tests pass. The no-default workspace
matrix passes 1,302 tests with two configured skips; the all-feature library
matrix passes 1,276 tests with six configured skips. Workspace/all-target/all-feature check and strict
Clippy, private-item rustdoc, all 51 doctests, rustfmt, and whitespace checks
pass. The client-go checkout remains clean at
`52c1e76cec993571493c81de442bcbef90cdc106`.
