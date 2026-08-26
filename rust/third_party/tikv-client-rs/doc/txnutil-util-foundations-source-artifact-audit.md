# `txnkv/txnutil` and utility-foundation source-artifact audits

Source of truth: `tikv/client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

Rust toolchain: `nightly-2026-08-22`.

This receipt records five independent package-atomic claims completed in one
implementation and validation batch. No package borrows another package's
inventory or completion status.

## Immutable inventories and original tests

The five packages contain exactly eight production/build artifacts and 755
lines. None contains `doc.go`, a `*_test.go` file, an external test package,
`main_test.go`, fixture, benchmark, example, generated input/output,
`go:generate` directive, package metadata, or package build file. Consequently
there are **zero original Go unit tests to port in this batch**. The Rust tests
below are source-derived contract and integration tests; the exact Go package
commands still compile every production and build-tag variant.

| Atomic package | Git tree | Artifact | Lines | SHA-256 |
| --- | --- | --- | ---: | --- |
| `txnkv/txnutil` | `8cf6a1c5585fdc6c2ae83fa9a143a1a7c86865b7` | `priority.go` | 34 | `9313ea834ba4f359eb3761f849facaf294897d24f70d201bc23f3d8611757d5d` |
| `util/codec` | `da1b6b31b500e66314cb0eeb42325315e4e4eeb9` | `bytes.go` | 195 | `7365bcd0c7761a1a88bf756293ef760a9ad0f75936c4ab808f63d002c8e1e7f1` |
| `util/codec` | same | `number.go` | 305 | `d3a5106c815517602cd16e570404ac6111dcd561db3c4f38ff239599d9019e74` |
| `util/intest` | `6e2efd6ae5f74e241701310d3d5f80c34c81bb1a` | `in_unittest.go` (`intest`) | 20 | `9fe83ce0ed6879cdb716696505b5359b369c79b6df61a837c339e635dc91218e` |
| `util/intest` | same | `not_in_unittest.go` (`!intest`) | 20 | `d032c71b69277a00caa95b8a10fdd1ede865d1276accfa3b39b9a4adc62b20aa` |
| `util/israce` | `d968d8e02dc911293282d0d2b10f521579fa5284` | `israce.go` (`race`) | 20 | `bafad80c26aa6fdc7daa77fba6dfa8929599fbfcf986d17a9ad9482f10e64dd4` |
| `util/israce` | same | `norace.go` (`!race`) | 20 | `bd23cb3b9da3f9e79a1fe88d373495eea46fe4dc46588f6409efdfccbea414dc` |
| `util/redact` | `d2befa157c8f07ab6daea060223fae94cf8a505f` | `redact.go` | 141 | `7fb44629fabc211d0de1e7b29314211f47da007acdb9b1674a83e2d671c674ce` |

## `txnkv/txnutil`

`src/transaction/priority.rs` is an integer-backed transparent Rust type with
the source constants `Normal`, `Low`, and `High`. Its numeric constructor and
`to_pb` retain arbitrary protobuf enum values exactly. This matters because the
Go definition is `type Priority kvrpcpb.CommandPri`, not a closed enum; the
previous Rust enum silently made future/unknown values unrepresentable.

`src/store/request.rs` now has a compatibility-preserving raw-priority method.
Every built-in context-bearing request writes the raw `i32`; stream and raw
wrappers delegate it; and `PlanBuilder` applies it before cloning, sharding, and
retry. Existing third-party `Request` implementations still receive known
values through the old enum method. The transaction regression sends a known
low read, source-normal heartbeat, and unknown value `99` through prewrite and
commit, proving end-to-end retention. Sync transaction/snapshot wrappers keep
their existing public delegation.

The three direct Go importers are `txnkv/util_export.go`,
`txnkv/transaction/txn.go`, and `txnkv/txnsnapshot/snapshot.go`. Their public
constants, mutable transaction/snapshot setters, normal defaults, read/write
contexts, and normal-heartbeat exception are assigned to the Rust priority,
request, plan, transaction, and wrapper tests.

## `util/codec`

`src/kv/codec.rs` covers every exported byte and number operation: ascending
memory-comparable bytes with caller-provided decode storage; signed/unsigned
fixed-width ascending and descending values; signed/unsigned ordinary
varints; and signed/unsigned comparable varints. Tests cover append and
leftover semantics, empty/exact-eight/multiple groups, ordering, all integer
boundaries, truncation, overflow, malformed sign encodings, and the pinned
source's unusual non-consuming leftover for one-byte signed comparable values.

The re-audit also restores the observable source error detail for malformed
byte groups: invalid markers and padding include the quoted nine-byte group,
instead of the former generic error. Safe uppercase/byte operations replace
only implementation details; wire bytes and error branches remain exact.

Mechanical import matching finds five direct Go files:
`integration_tests/util_test.go`, `internal/apicodec/mem_codec.go`,
`internal/apicodec/codec_v2_test.go`, and mocktikv's `mvcc.go` and
`mvcc_leveldb.go`. Their owning receipts retain integration completion; this
claim owns the deterministic codec operations they call.

## `util/intest`

`src/intest.rs` maps the `intest`/`!intest` source variants to Cargo's
`internal-tests` feature and retains source mutability through a sequentially
consistent atomic. Both initial states, mutation, reset, and downstream-crate
visibility are executable.

The seven direct source files are `tikv/kv.go`, `tikv/kv_test.go`,
`txnkv/transaction/txn.go`, `txnkv/txnlock/lock_resolver.go`,
`txnkv/txnsnapshot/snapshot.go`, `internal/apicodec/codec_v2.go`, and
`integration_tests/option_test.go`. Their branches are test instrumentation,
not production protocol choices: injected constructor/safe-point failures and
timestamp sources are typed in Rust; lock-pool impossible states remain hard
invariants; missing commit timestamps return errors without requiring a test
panic; and empty API-v2 keys are accepted without a production-only warning.
Those behaviors are tested by their owning package receipts. The flag itself
therefore has no artificial production consumer in Rust; its complete contract
is build initialization plus a mutable test override.

## `util/israce`

`src/israce.rs` maps Go's automatic `race` build tag to the explicit Cargo
`race-tests` feature. Stable/Rust nightly sanitizer instrumentation does not
provide a portable crate cfg equivalent, so sanitizer jobs must select this
feature. Both values and downstream visibility are tested. The sole source
importer is the race-sensitive test in
`internal/locate/replica_selector_test.go`; no production behavior depends on
the constant.

## `util/redact`

`src/redact.rs` covers the source mode rule (only empty and `OFF` disable),
uppercase hexadecimal `Key`/`KeyBytes`, and every protobuf key-error field:
locked primary/key/all secondaries, conflict key/primary, already-exists,
deadlock keys and non-empty wait-chain keys, commit-ts-expired,
transaction-not-found, assertion-failed, and primary-mismatch lock info. The
process mode uses sequentially consistent atomic access and serialized tests.

Go's exported `String` accepts arbitrary bytes and unsafely aliases its input.
Rust exposes the safe native equivalent as `string(&[u8]) -> &[u8]`, retaining
arbitrary bytes and pointer identity without claiming invalid UTF-8 is a Rust
`String`. Private Go hex/uppercase helpers fold into safe formatting.

Mechanical matching finds 20 direct Go files under `error`, `tikv`,
`internal/{apicodec,locate,logutil,mockstore,unionstore}` and
`txnkv/{rangetask,transaction,txnlock,txnsnapshot}` plus integration scan
support. Their owning receipts retain log/error-path integration; the complete
helper and protobuf mutation contract is owned here.

## Unit and downstream test mapping

Because the source has no package-local tests, Rust uses one focused test group
per contract and an ordinary downstream crate boundary:

- priority: known and unknown wire values, clone/shard timing, transaction
  read/heartbeat/prewrite/commit propagation, defaults, and public constants;
- codec: five focused tests covering every function and malformed branch;
- intest/israce: both Cargo feature states and mutable reset;
- redact: mode matrix, zero-copy arbitrary-byte view, complete key-error
  mutation, and disabled no-op;
- `tests/public_util_foundations_tests.rs`: all five public surfaces from a
  separate crate in no-default and all-feature builds.

## Validation

The exact pinned Go packages compile in ordinary, race, and `intest` modes;
each reports `[no test files]`, agreeing with the immutable inventory:

```text
/private/tmp/go1.25.12/bin/go test \
  ./txnkv/txnutil ./util/codec ./util/intest ./util/israce ./util/redact -count=1
/private/tmp/go1.25.12/bin/go test -race \
  ./txnkv/txnutil ./util/codec ./util/intest ./util/israce ./util/redact -count=1
/private/tmp/go1.25.12/bin/go test -tags intest ./util/intest -count=1
# all passed
```

Rust focused, downstream, complete-matrix, and strict gates:

```text
cargo +nightly-2026-08-22 test -p tikv-client priority --lib
cargo +nightly-2026-08-22 test -p tikv-client kv::codec::test --lib
cargo +nightly-2026-08-22 test -p tikv-client redact --lib
cargo +nightly-2026-08-22 test -p tikv-client intest --lib
cargo +nightly-2026-08-22 test -p tikv-client israce --lib
cargo +nightly-2026-08-22 test -p tikv-client \
  --test public_util_foundations_tests --no-default-features
cargo +nightly-2026-08-22 test -p tikv-client \
  --test public_util_foundations_tests --all-features
cargo +nightly-2026-08-22 test --workspace --no-default-features
cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features
cargo +nightly-2026-08-22 check --workspace --all-targets --all-features
cargo +nightly-2026-08-22 clippy --workspace --all-targets \
  --all-features -- -D warnings
RUSTDOCFLAGS='-Dwarnings --document-private-items' \
  cargo +nightly-2026-08-22 doc --workspace --all-features --no-deps
cargo +nightly-2026-08-22 test --workspace --doc --all-features
cargo +nightly-2026-08-22 fmt --all -- --check
git diff --check
```

Focused results are 9 priority tests, 5 codec tests, 4 redaction tests, one
`intest` test, one `israce` test, and two downstream tests in each feature
configuration. The no-default workspace runs 1,003 active library tests plus
all integration/member-crate targets (one intentional library ignore), and the
all-feature library runs 1,000 active tests (the same intentional ignore).
Strict check, Clippy, private rustdoc, 51 doctests, rustfmt, and diff hygiene all
pass.

No live TiKV/PD gate applies to the four deterministic utility contracts or
build-state flags. Priority wire propagation is captured at the exact
generated request boundary; full transaction/live-cluster behavior remains
owned by the transaction and repository-level differential receipts.
