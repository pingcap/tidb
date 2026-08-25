# Repository source-artifact and live-validation audit

This receipt closes the non-package repository gate for the client-go parity
work. Package behavior remains owned by the 42 atomic package rows in
[`client-go-parity-ledger.md`](client-go-parity-ledger.md); this document
accounts for everything in the pinned source tree that is not already owned by
one of those Go packages and records the final cross-client/live validation.

## Immutable inputs

- client-go source: `52c1e76cec993571493c81de442bcbef90cdc106`
- client-go Go version: `1.25.12`
- canonical client-go module version:
  `v2.0.8-0.20260813104652-52c1e76cec99`
- kvproto source: `059694ae4472276644613acccefa24cbc89d959f`
- Rust toolchain: `nightly-2026-08-22`

The pinned client-go tree contains 309 tracked files. Package receipts account
for 235 files. The remaining inventory below is exactly 74 files and 19,306
lines; there are no unclassified tracked artifacts.

| Source group | Files | Lines | Immutable identity |
| --- | ---: | ---: | --- |
| root policy/module files | 10 | 846 | per-file SHA-256 table below |
| `.github` | 4 | 253 | tree `839016a692dcb4a2a820eddedd9255f9b1315604`; SHA-256 manifest `33570f78c709c3e70376570c6a1d0bffa058a2b6ec2cabc525a1cc729e4f5c98` |
| `examples` | 19 | 1,319 | tree `47ae42586d3420bf7b1cd21e32bc83ca322c8687`; SHA-256 manifest `e311b96ff2c4af773bd11680208f5086254e2f09fc625fcc9d51d0ee05a6115a` |
| `integration_tests` | 41 | 16,888 | tree `33bbed2a6133b8e5ae555d7359e3d7810b204324`; SHA-256 manifest `d4a7503f483fe03a24bffa092d2860d138de54444d9655616b4bd62915dd0341` |
| **total** | **74** | **19,306** | complete |

The group manifests are SHA-256 over sorted lines of the form
`<blob SHA-256><two spaces><path>`, with blob bytes read directly from the
pinned Git object. The Git tree IDs independently bind names, modes, and blob
IDs.

## Root inventory and decisions

| Artifact | Lines | SHA-256 | Rust decision |
| --- | ---: | --- | --- |
| `.gitignore` | 3 | `080381c92bedcc0923ab413f95ac2b5c4fc5e5dc8d874b9ada65768428c78011` | Native ignore policy; no runtime behavior. |
| `.golangci.yml` | 35 | `4cd38d6b78ae4dccadf577200f25346e3d755492750de91c17b3cd28dfd9b27e` | Mapped to strict pinned-nightly Clippy, rustfmt, and rustdoc gates. |
| `AGENTS.md` | 50 | `f2f11ff925a5abeee794c52040899323a0839376bb611396f30f601c78836769` | Development policy only; package-atomic inventory and validation requirements are enforced by the ExecPlan and ledger. |
| `CONTRIBUTING.md` | 34 | `ee0404594c71dbe5064612538a0ae31b48ccb9a8727dc322d4aa1242862a663e` | Contribution policy only; native Rust repository policy is retained. |
| `LICENSE` | 201 | `c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4` | Apache-2.0 is retained by client-rust and new source files. |
| `OWNERS` | 8 | `34dbbe65a0cdc55ebd470665fec7d9f8b0cca6748433c6361e3d1323feb8ce37` | Metadata only; native `OWNERS` remains authoritative for the Rust repository. |
| `OWNERS_ALIASES` | 107 | `bc20d0e2b150081b235bdb178127da4c2017104456b09d1da6fc6f4a2a52b1a9` | Metadata only; native aliases remain authoritative. |
| `README.md` | 98 | `5065c721fd5544d0fef070ac2c31695fc66126f7f969ac147aad080ab87db9a0` | User-facing Go syntax is represented by client-rust's README, getting-started guide, rustdoc, and checked examples. |
| `go.mod` | 61 | `431a0b7a99488380c9e8522fc0dc685f9a85a093c0b66b1db9ab8baaa8d0e61c` | Every behavior-bearing dependency is assigned to its owning package receipt. The kvproto revision is exact; the cross-client harness pins the complete client-go module. Cargo dependencies are native equivalents rather than a name-for-name Go graph. |
| `go.sum` | 249 | `4a80787683662bc448e5953d51dfb4478270c2b75ff9396c4fc19e9f8d5dbdc5` | Go checksum metadata is non-runtime. Cargo.lock pins the Rust graph; `tests/client_go_differential/go.sum` pins the final Go comparison graph. |

## CI inventory

The complete source list is:

```text
.github/stale.yml
.github/workflows/compatibility.yml
.github/workflows/integration.yml
.github/workflows/test.yml
```

Stale-issue policy is repository administration and has no client behavior.
The compatibility, integration, and test workflows map to client-rust's
`.github/workflows/ci.yml`, `Makefile`, Cargo feature matrix, pinned toolchain,
nextest configuration, generated-code gate, unit/doctest/example gates, and
the explicit live commands in this receipt. The local completion run executes
the superset relevant to behavior: default and all-feature library tests,
strict Clippy/rustdoc, all targets, examples, doctests, generated
reproducibility, both API codecs, multi-region raw/transaction tests, sync
facades, and failpoints.

## Example inventory

The complete source list is:

```text
examples/.gitignore
examples/README.md
examples/gcworker/gcworker.go
examples/gcworker/go.mod
examples/rawkv/go.mod
examples/rawkv/rawkv.go
examples/txnkv/1pc_txn/1pc_txn.go
examples/txnkv/1pc_txn/go.mod
examples/txnkv/async_commit/async_commit.go
examples/txnkv/async_commit/go.mod
examples/txnkv/delete_range/delete_range.go
examples/txnkv/delete_range/go.mod
examples/txnkv/go.mod
examples/txnkv/keyspace.go
examples/txnkv/pessimistic_txn/go.mod
examples/txnkv/pessimistic_txn/pessimistic_txn.go
examples/txnkv/txnkv.go
examples/txnkv/unsafedestoryrange/go.mod
examples/txnkv/unsafedestoryrange/unsafedestoryrange.go
```

The three native Rust executables `examples/raw.rs`,
`examples/transaction.rs`, and `examples/pessimistic.rs` cover ordinary RawKV,
optimistic transaction, and pessimistic transaction use. GC, safe-point,
delete-range, unsafe-destroy-range, keyspace, 1PC, and async-commit operations
are public compile-tested APIs with package-owned source-derived tests and live
coverage; duplicating each Go executable is not required to preserve behavior.
All Rust examples compile under all features.

## Integration-test inventory

The complete source list is:

```text
integration_tests/1pc_test.go
integration_tests/2pc_test.go
integration_tests/assertion_test.go
integration_tests/async_commit_fail_test.go
integration_tests/async_commit_test.go
integration_tests/client_fp_test.go
integration_tests/delete_range_test.go
integration_tests/gc_test.go
integration_tests/go.mod
integration_tests/go.sum
integration_tests/health_feedback_test.go
integration_tests/interceptor_test.go
integration_tests/isolation_test.go
integration_tests/lock_test.go
integration_tests/main_test.go
integration_tests/option_test.go
integration_tests/pd_api_test.go
integration_tests/pd_next_gen.toml
integration_tests/pipelined_memdb_test.go
integration_tests/prewrite_test.go
integration_tests/range_task_test.go
integration_tests/raw/api_mock_test.go
integration_tests/raw/api_test.go
integration_tests/raw/tikv-v1ttl.toml
integration_tests/raw/tikv-v2.toml
integration_tests/raw/util_test.go
integration_tests/resource_group_test.go
integration_tests/resource_tag_test.go
integration_tests/safepoint_test.go
integration_tests/scan_mock_test.go
integration_tests/scan_test.go
integration_tests/shared_lock_test.go
integration_tests/snapshot_fail_test.go
integration_tests/snapshot_test.go
integration_tests/split_test.go
integration_tests/store_test.go
integration_tests/ticlient_test.go
integration_tests/tikv.toml
integration_tests/tikv_next_gen.toml
integration_tests/txn_file_test.go
integration_tests/util_test.go
```

This is 34 Go files, two module files, and five server configuration fixtures.
The Go files expose 36 top-level `Test*`/`TestMain` entry points. Their suite
methods and support paths are assigned as follows:

| Source behavior | Owning Rust evidence |
| --- | --- |
| 1PC, 2PC, prewrite, async commit, assertions, pipelined MemDB, transaction-file failure and regrouping | `txnkv/transaction` package receipt; transaction unit tests; `integration_tests`; `failpoint_tests` |
| Lock, shared-lock, resolve-lock, heartbeat, cleanup | `txnkv/txnlock` receipt; transaction tests; `integration_tests`; `failpoint_tests` |
| Snapshot, scan, isolation, retry/failure | `txnkv/txnsnapshot` receipt; async and sync live targets |
| Raw V1/V1TTL/V2, TTL, CAS, batch and range behavior | `rawkv` receipt; deterministic codec tests; API-version differential; multi-region `integration_tests` |
| Delete range, range tasks, GC, safe points, split/scatter, store and PD APIs | `txnkv/rangetask`, `tikv`, and `internal/locate` receipts; loopback tests; multi-region live target |
| Options, interceptors, resource groups/tags, health feedback and client failpoints | owning package receipts plus all-feature/failpoint/live targets |
| Harness, module and TiKV/PD configuration lifecycle | `tests/common`, `config/*.toml`, the Cargo feature matrix, and the explicit isolated cluster lifecycle below |

The mapping is behavioral rather than a text-for-text test port. No source
integration file, entry point, module file, or configuration fixture remains
unassigned.

## Generated protocol inventory

Pinned kvproto has 38 top-level schemas, and every one is present byte-for-byte
under `proto/`. The only additional top-level input is
`proto/grpc_channelz.proto`, pinned by the `util/collectors` receipt. Seventeen
vendored imports under `proto/include/` complete the generator dependency
closure, for 56 `.proto` inputs total.

Generation produces 41 protocol Rust modules, `src/generated/mod.rs`, and the
728,127-byte `src/generated/file_descriptor_set.bin`. The four previously
missing kvproto families are now present and generated:
`db9_coprocessor`, `externalworkloadpb`, `keyspace_encryptionpb`, and
`routerpb`. Updated scheduling, TSO, CDC, auto-ID, and resource-manager fields
are protected by descriptor/tag tests.

The proto-build crate requires protoc 35.1, generates into a staging directory
before updating the checked-in output, sorts its root inputs, and removes stale
generated Rust or descriptor files. The version gate keeps descriptor encoding
identical across macOS and Linux. Clean generation removed the unreferenced
`span.rs` left by the deleted `span.proto` input. Two consecutive clean
generator runs yielded the same SHA-256 output manifest:

```text
b77f2aa05bc26eedd23c6b8ba1896edae9f5c72b7c5a090f1e5809999e31d8fa
```

The manifest hashes sorted `shasum -a 256` lines for every file below
`src/generated`. A mechanical comparison also reports all 38 pinned kvproto
top-level files byte-identical.

The complete generated namespace is public at `tikv_client::proto`, matching
client-go's use of the shared public `kvproto` module. This is a required API
boundary, not an implementation leak: downstream crates must be able to name
the request and response types exposed by public traits and test-support APIs.
`tests/public_proto_tests.rs` compiles as a downstream crate, round-trips a
generated context with ordinary features, and implements
`mocktikv::CoprocessorHandler` with `internal-tests` enabled.
Both focused modes pass; clean generation, all-target/all-feature compilation
and strict Clippy/rustdoc, 742 no-default workspace tests, 736 all-feature
library tests, and 51 doctests also pass on the pinned nightly toolchain. The
three additional source-derived request tests protect common multi-region
terminal resharding, nested cleanup-error propagation, and immediate
NotLeader-hint adoption.

## Direct client-go/client-rust differential

`tests/client_go_differential` is an isolated Go module pinned to the exact
source revision. `tests/api_version_live_tests.rs` is its ignored-by-default
Rust counterpart. Both run identical logical keys and assert:

- positional raw BatchGet including a missing key;
- bounded forward and reverse RawKV scans;
- raw delete-range visibility;
- optimistic transaction writes, existing/missing reads, ordered scan, commit,
  and cleanup;
- V1 identity coding and V2 `DEFAULT` keyspace coding.

The matching cluster binaries were PD
`v9.0.0-beta.2.pre-483-ga186e0cc6` at
`a186e0cc61def1408fc57ae7b3d1044a572c8ae3` and TiKV
`v9.0.0-beta.2` at `8e964719db0d2088d47a280a1dde3fefa1b31d6b`.
API v1 ran with TTL disabled; API v2 used the `DEFAULT` keyspace. All four
client/version combinations passed and emitted the same canonical result:

```text
client-parity api=<version> raw=batch_get:c,-,a scan:a,c reverse:c,a txn=get:b,- scan:b,d
```

The initial attempt against PD v8.5.5 correctly failed before data operations
because pinned client-go's 2026 PD dependency uses the newer `QueryRegion`
router RPC. The matching v9 differential avoids weakening or patching either
client to accommodate an older control plane.

## Complete pinned client-go integration workflow

The complete integration workflow from the pinned client-go revision was run
with Go `1.25.12 darwin/arm64`, not merely inventoried or represented by the
smaller differential harness. Its two source packages and every workflow
variant passed:

```text
# integration-local: integration_tests and integration_tests/raw
go test ./...
ok integration_tests      90.299s
ok integration_tests/raw   0.058s

# integration-local-race: integration_tests and integration_tests/raw
go test ./... -race
ok integration_tests      98.645s
ok integration_tests/raw   1.294s

# integration-tikv: API V1, matching PD/TiKV v9
go test --with-tikv
ok integration_tests     289.165s

# integration-raw-tikv: V1TTL and V2 matrix cases
go test --with-tikv
ok integration_tests/raw   2.648s  # tikv-v1ttl.toml
ok integration_tests/raw   2.657s  # tikv-v2.toml
```

The real-cluster runs used the same matching binaries as the direct
differential: PD `a186e0cc61def1408fc57ae7b3d1044a572c8ae3` and TiKV
`8e964719db0d2088d47a280a1dde3fefa1b31d6b`. Each run used clean data
directories and waited for PD to report one store and three regions before
starting tests. The first transactional launch was discarded as invalid setup
evidence: port 20160 was occupied by a stale TiKV process, while the newly
started PD still reported `NOT_BOOTSTRAPPED`. After the stale listener exited,
the unchanged command passed against a genuinely bootstrapped cluster. This
was an environment-readiness failure, not a client-go test failure.

The retained logs are under `/private/tmp/client-go-full-v9-v1-20260825-1`,
`/private/tmp/client-go-full-v9-v1ttl-20260825-1`, and
`/private/tmp/client-go-full-v9-v2-20260825-1`. All test clusters were stopped;
ports 2379, 2380, 20160, and 20180 were verified free afterward.

## Multi-region Rust live matrix

The deeper Rust matrix ran against one PD v8.5.5 and three TiKV v8.5.5 nodes
in API-v2 mode with 114 observed regions. It exercises raw, transaction,
sync-facade, region, retry, lock, async-commit, cleanup, error, BatchCommands,
and failpoint behavior:

```text
integration_tests:      27 passed
sync_transaction_tests: 28 passed
failpoint_tests:          7 passed
total:                   62 passed
```

The run exposed and fixed five integration-only boundaries: readiness before
gRPC health checks; publishing the BatchCommands outbound sender before
awaiting response headers; V2 decoded-lock re-encoding for cleanup and
async-commit secondary recovery; physical memcomparable PD region boundaries;
and unbounded reverse RawKV scan plus positional BatchGet behavior.

## Final local release gates

All commands use `nightly-2026-08-22` unless they invoke the pinned Go 1.25.12
toolchain:

```text
cargo test --workspace --lib
cargo test --workspace --lib --all-features
# each: tikv-client 710 passed/1 ignored; unistore 22 passed

cargo test -p unistore
# 22 unit + 2 external-consumer tests passed

cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features --message-format=short -- -D warnings
cargo check --workspace --examples --all-features
cargo test --workspace --doc --all-features
# 51 tikv-client doctests passed

RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --all-features --no-deps --document-private-items
cargo fmt --all -- --check
go test ./...  # from tests/client_go_differential
git diff --check
```

The generator was run immediately before these gates; its output manifest and
the 38/38 kvproto byte comparison remained unchanged.

## Final repository decision

Every client-go package row is `complete`, the unrelated TiDB server UniStore
package is explicitly `not-applicable`, all 74 non-package source artifacts are
listed and assigned here, all pinned generated inputs are exact and
reproducible, and the complete pinned Go integration workflow, direct V1/V2
differential, and deeper multi-region Rust gates pass. This closes the
repository source-artifact, upstream-integration, and live-differential gates
without claiming text-for-text API or test layout identity where idiomatic
Rust uses a different shape.
