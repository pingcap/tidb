# Transaction KV source and original-test coverage

This inventory records completed dependency-closed `pkg/kv` source domains,
their original tests, and every test and benchmark in the original key/version
source tests. It is deliberately stricter than a file-level parity claim: a
row is `COVERED` only when the complete anchored Go test is represented by a
production Rust API and executable Rust evidence. `BLOCKED` rows remain visible
until their owning production boundary exists.

## Completed keyspace predicate domain

`crates/tidb-txnkv/src/keyspace.rs` translates the portable part of
`pkg/kv/utils.go`: the next-generation gate and exact `SYSTEM` keyspace
comparison for `IsUserKS` and `IsSystemKS`. The test package preserves both
original anchors, including classic-mode behavior. `WalkMemBuffer` remains
open in the mixed source file and is intentionally reported as partial in the
source evidence rather than hidden behind a fake storage client. `IncInt64`
and `GetInt64` now have their own production counter leaf and source-owned
transaction tests below.

| Go anchor | Status | Rust evidence | Exact coverage |
| --- | --- | --- | --- |
| `pkg/kv/utils_test.go:136 TestIsUserKS` | `COVERED` | `tests/keyspace.rs::test_is_user_ks` plus `crates/tidb-txnkv/src/keyspace.rs` | Classic, user, and reserved `SYSTEM` cases execute with exhaustive empty/arbitrary-name and mode-gating checks. |
| `pkg/kv/utils_test.go:149 TestIsSystemKS` | `COVERED` | `tests/keyspace.rs::test_is_system_ks` plus `crates/tidb-txnkv/src/keyspace.rs` | Classic, user, and reserved `SYSTEM` cases execute with exhaustive arbitrary-name and mode-gating checks. |

The exact sparse rows live in
`difftests/corpus/coverage/evidence/{source,tests}/txnkv-keyspace-source-wave.tsv`.

## Completed integer counter domain

`crates/tidb-txnkv/src/counter.rs` translates the storage-independent
`IncInt64` and `GetInt64` helpers from `pkg/kv/utils.go` behind a deliberately
narrow `CounterStorage` contract. Missing keys initialize to zero, persisted
values are parsed as signed decimal integers, parse failures leave storage
untouched, and increment overflow is rejected instead of wrapping. The API
does not invent a mem-buffer or transaction client; those remain separate
owners until their real production boundary exists.

| Go anchor | Status | Rust evidence | Exact coverage |
| --- | --- | --- | --- |
| `pkg/kv/utils_test.go:29 TestIncInt64` | `COVERED` | `difftests/transaction-tests/tests/counter.rs::test_inc_int64` plus `crates/tidb-txnkv/src/counter.rs` | Original missing-key initialization, repeated increments, parse failure without mutation, and signed overflow boundary execute through the production storage contract. |
| `pkg/kv/utils_test.go:56 TestGetInt64` | `COVERED` | `difftests/transaction-tests/tests/counter.rs::test_get_int64` plus `crates/tidb-txnkv/src/counter.rs` | Original missing-key zero and persisted-value rows execute with source-compatible parse and storage-error behavior. |

The exact sparse rows live in
`difftests/corpus/coverage/evidence/{source,tests}/txnkv-counter-source-wave.tsv`.

## Completed request-support checker domain

`crates/tidb-txnkv/src/checker.rs` directly translates all of
`pkg/kv/checker.go`. The public method retains the source's raw `int64`
request/subtype boundary and then performs the observable Go conversion from
`int64` to TIPB's generated `int32` `ExprType` before expression matching.
Consequently, positive and negative values separated by `2^32` have the same
expression disposition, while unknown request types are rejected. Analyze is
the deliberate source exception: it accepts every subtype without inspecting
it.

The numeric expression identities were audited against the generated
`go-tipb/expression.pb.go` from the exact TIPB revision pinned in `go.mod`
(`v0.0.0-20260623093813-5f9928e91afe`). The Rust crate does not introduce a
typed-only wire boundary, a guessed protobuf enum, or a placeholder protocol
dependency. Its unit tests classify all 53 generated `ExprType` identities,
the extra Signature subtype, every request-specific branch, unknowns, and
integer wrap behavior.

| Go anchor | Status | Rust evidence | Exact coverage |
| --- | --- | --- | --- |
| `pkg/kv/checker_test.go:25 TestIsRequestTypeSupported` | `COVERED` | `tests/key_version.rs::test_is_request_type_supported` plus `crates/tidb-txnkv/src/checker.rs` unit tests | All eight original assertions execute in source order, including the repeated Signature assertion. Exhaustive crate tests additionally pin all source match arms and every pinned TIPB enum identity. |

### Source-owned exact sparse evidence rows

This wave owns `evidence/source/txnkv-checker-source-wave.tsv` and
`evidence/tests/txnkv-checker-source-wave.tsv` below the coverage directory:

```text
pkg/kv/checker.go\tCOVERED\ttxnkv-checker-source-wave\trust/crates/tidb-txnkv/src/checker.rs\tComplete request support checker preserves raw int64 request and subtype inputs Go int32 ExprType narrowing every supported scalar aggregate and window identity special Select Index DAG subtype behavior Analyze wildcard behavior and unknown request rejection
go_test\tpkg/kv/checker_test.go\t25\tTestIsRequestTypeSupported\tCOVERED\ttxnkv-checker-source-wave\trust/difftests/transaction-tests/tests/key_version.rs\tAll eight original assertions execute in source order including the repeated Signature assertion; exhaustive crate tests also cover all 53 pinned TIPB ExprType identities every request and special subtype branch unknowns Analyze wildcard and Go int32 wrap behavior
```

## Completed transaction-source bitfield domain

`crates/tidb-txnkv/src/txn_source.rs` directly translates the complete
transaction-source bitmap at `pkg/kv/option.go:243-295`. The API mutates the
source's raw `uint64` equivalent with OR-only setters and preserves every
observable edge rather than normalizing the field:

- CDC accepts `0..=8`, because Go compares against `cdcWriteSourceBits`, even
  though the error reports `[1, 15]` and the getter masks a full byte.
- Lossy-DDL accepts the full byte `0..=255`, shifts it by eight, and never
  clobbers CDC, Lightning, or reserved bits.
- Lossy-DDL presence deliberately does not mask its byte. A Lightning-only or
  reserved-upper-bit source reports set while the lossy getter returns zero.
- Invalid setters return the exact source message and do not mutate the input.

The crate tests exhaust all 2,304 valid CDC/lossy compositions in both setter
orders and verify repeated OR behavior, invalid boundaries, exact constants,
getters, and presence checks. This is not file-level completion: transaction
option IDs, replica-read modes, request-context helpers, internal source names,
size limits, and other option domains in `option.go` remain unported.

| Go anchor | Status | Rust evidence | Exact coverage |
| --- | --- | --- | --- |
| `pkg/kv/option_test.go:23 TestSetCDCWriteSource` | `COVERED` | `tests/key_version.rs::test_set_cdc_write_source` plus `crates/tidb-txnkv/src/txn_source.rs` unit tests | All three original rows and assertions execute; exhaustive coverage pins the surprising `0..=8` boundary, exact error, OR behavior, masking, and no mutation on failure. |
| `pkg/kv/option_test.go:63 TestSetLossyDDLReorgSource` | `COVERED` | `tests/key_version.rs::test_set_lossy_ddl_reorg_source` plus `crates/tidb-txnkv/src/txn_source.rs` unit tests | All four original rows and assertions execute; every accepted byte composes in both orders without clobbering adjacent domains, and upper-bit presence remains source-exact. |

### Source-owned sparse evidence rows

This wave owns `evidence/source/txnkv-txn-source-option-wave.tsv` and
`evidence/tests/txnkv-txn-source-option-wave.tsv` below the coverage directory:

```text
pkg/kv/option.go\tPARTIAL\ttxnkv-txn-source-option-wave\trust/crates/tidb-txnkv/src/txn_source.rs\tComplete lines 243 through 295 transaction-source bitfield domain preserves CDC zero-through-eight acceptance despite the printed one-through-fifteen range lossy DDL byte OR and mask semantics unmasked upper-bit presence behavior and the Lightning bit; transaction option IDs replica read request contexts internal source names and size limits remain unported
go_test\tpkg/kv/option_test.go\t23\tTestSetCDCWriteSource\tCOVERED\ttxnkv-txn-source-option-wave\trust/difftests/transaction-tests/tests/key_version.rs\tAll three original rows and assertions execute; exhaustive crate tests additionally preserve accepted values zero through eight exact misleading error text OR-only composition invalid no-mutation and low-byte masking
go_test\tpkg/kv/option_test.go\t63\tTestSetLossyDDLReorgSource\tCOVERED\ttxnkv-txn-source-option-wave\trust/difftests/transaction-tests/tests/key_version.rs\tAll four original rows and assertions execute; exhaustive crate tests additionally cover every value zero through 255 both composition orders repeated OR behavior invalid no-mutation upper-bit presence and Lightning non-clobbering
```

## Completed KV error-identity domain

`crates/tidb-txnkv/src/error.rs` directly translates all of
`pkg/kv/error.go`. The representation carries the exact registered `kv`/`tikv`
class, MySQL code, message template, redaction positions, and generated error
identity. Equality follows the root error identity, so retryability remains the
source classifier over `ErrTxnRetryable`, `ErrWriteConflict`, and
`ErrWriteConflictInTiDB`; it is not an invented boolean field. The
backward-compatible `TxnRetryableMark`, nil/foreign-error behavior,
`IsErrNotFound`, and `GenKeyExistsErr` join and MySQL character-precision
semantics all execute in Rust.

This is deliberately a per-crate authority for the complete KV source domain,
not a placeholder shared error registry. Other TiDB classes stay with their
own source crates until a real cross-crate consumer requires consolidation.
The generic `dbterror` stack-capture and global redaction-mode machinery is
outside `pkg/kv/error.go`; this slice preserves the KV templates and redaction
metadata it consumes without claiming that separate global subsystem.

| Go anchor | Status | Rust evidence | Exact coverage |
| --- | --- | --- | --- |
| `pkg/kv/error_test.go:25 TestError` | `COVERED` | `tests/key_version.rs::test_error_identity` | All nine original code/SQL-code assertions execute; the same table additionally pins every other error declared in `error.go`, both classes, all 13 codes/templates, redaction metadata, and RFC identities. |
| `pkg/kv/key_test.go:115 TestBasicFunc` | `COVERED` | `tests/key_version.rs::test_basic_func` | The original nil, retryable prototype, and foreign error assertions execute. The two additional source-accepted retry identities and not-found classifier execute too. |
| `pkg/kv/error.go:97 GenKeyExistsErr` | `COVERED` | `tests/key_version.rs::test_gen_key_exists_err` | Empty and multi-column joins, generated duplicate-entry identity/code/message, 64-character entry precision, 192-character key-name precision, and multibyte boundaries execute. |

### Source-owned exact sparse evidence rows

This wave owns `evidence/source/txnkv-error-source-wave.tsv` and
`evidence/tests/txnkv-error-source-wave.tsv` below the coverage directory.
The `TestBasicFunc` `COVERED` row replaces the earlier owner's `BLOCKED` row;
the ledger rejects leaving both anchors across fragments:

```text
pkg/kv/error.go\tCOVERED\ttxnkv-error-source-wave\trust/crates/tidb-txnkv/src/error.rs\tAll 13 KV source error identities preserve exact kv or tikv class MySQL code template redaction positions RFC equality and root-cause classification; TxnRetryableMark IsTxnRetryableError IsErrNotFound and GenKeyExistsErr execute without an invented retry flag
go_test\tpkg/kv/error_test.go\t25\tTestError\tCOVERED\ttxnkv-error-source-wave\trust/difftests/transaction-tests/tests/key_version.rs\tAll original SQL error-code assertions execute and the same table pins every remaining pkg kv error identity class template and redaction position
go_test\tpkg/kv/key_test.go\t115\tTestBasicFunc\tCOVERED\ttxnkv-error-source-wave\trust/difftests/transaction-tests/tests/key_version.rs\tOriginal nil retryable and foreign-error rows execute plus both other source retry identities root-cause equality and not-found classification
```

## Completed key-flag and assertion primitive domain

`crates/tidb-txnkv/src/{key_flags,assertion}.rs` directly translates the
complete dependency-closed semantics of `pkg/kv/{keyflags,assertion}.go`.
Instead of exposing Go's raw byte and unused bit patterns, Rust represents the
same 64 meaningful states as four booleans plus the closed
`AssertionState::{Unset, Exists, NotExists, Unknown}` enum. Exhaustive unit
tests execute all 64 states against every one of the four general flag
operations and every one of the four assertion operations. They also pin the
source-specific rule that `AssertNone` is a no-op, not a request to clear an
existing assertion.

The source and consumer audit covered every Go reference to the public query
and operation APIs. This slice does **not** contain a transaction buffer,
`pkg/table/tables.setAssertion`, table mutation, pessimistic locking, prewrite,
or client-go conversion. Consequently, no upstream integration test is marked
`COVERED`. Only the two original tests that contain a directly executed
primitive subset are proposed as `PARTIAL`; broader tests that merely consume
flags produced by absent table/session paths remain `UNTRIAGED`.

| Go anchor | Status | Rust evidence | Exact coverage |
| --- | --- | --- | --- |
| `pkg/session/test/tidb_test.go:44 TestKeysNeedLock` | `PARTIAL` | `crates/tidb-txnkv/src/key_flags.rs` exhaustive unit tests | The test's direct zero-to-`SetNeedLocked`, presume-key-not-exists, and query semantics execute for every containing state. `session.KeyNeedToLock`, table/index key classification, temporary-index decoding, and kernel-mode behavior do not exist in Rust. |
| `pkg/table/tables/assertion_test.go:29 TestSetAssertion` | `PARTIAL` | `crates/tidb-txnkv/src/{key_flags,assertion}.rs` exhaustive unit tests | All four assertion operations, all four query states, preservation of general flags, and `AssertNone` no-op behavior execute. The transaction-buffer first-assertion-wins contract, Set/Delete/LockKeys interaction, and rollback do not exist in Rust. |

### Source-owned sparse evidence rows

This wave owns
`evidence/source/txnkv-keyflags-assertion-source-wave.tsv` and
`evidence/tests/txnkv-keyflags-assertion-source-wave.tsv` below the coverage
directory. These exact rows are applied in those fragments:

```text
pkg/kv/assertion.go\tCOVERED\ttxnkv-keyflags-assertion-source-wave\trust/crates/tidb-txnkv/src/assertion.rs\tAll four typed assertion operations execute against every one of the 64 semantic KeyFlags states; AssertNone no-op and preservation of every nonassertion flag are exhaustive
pkg/kv/keyflags.go\tCOVERED\ttxnkv-keyflags-assertion-source-wave\trust/crates/tidb-txnkv/src/key_flags.rs\tAll 64 meaningful flag states every public query all four monotonic FlagsOp transitions empty variadic repeated and reordered operation semantics execute; raw unused bit patterns are eliminated by the typed representation
go_test\tpkg/session/test/tidb_test.go\t44\tTestKeysNeedLock\tPARTIAL\ttxnkv-keyflags-assertion-source-wave\trust/crates/tidb-txnkv/src/key_flags.rs\tDirect presume-key-not-exists and zero-to-SetNeedLocked flag semantics execute exhaustively; KeyNeedToLock table/index classification temporary-index decoding and kernel mode remain unimplemented
go_test\tpkg/table/tables/assertion_test.go\t29\tTestSetAssertion\tPARTIAL\ttxnkv-keyflags-assertion-source-wave\trust/crates/tidb-txnkv/src/assertion.rs\tAll four assertion operations and query states plus general flag preservation execute exhaustively; transaction buffer first-assertion-wins Set Delete LockKeys and rollback remain unimplemented
```

## Implemented source tests

| Go anchor | Status | Rust evidence | Exact coverage |
| --- | --- | --- | --- |
| `pkg/kv/key_test.go:38 TestPartialNext` | `PARTIAL` | `tests/key_version.rs::test_partial_next` plus `fixtures/partial_next.hex` | All three comparisons execute over exact Go-generated `EncodeValue` bytes. `tidb-codec` now owns comparable `EncodeKey`; the compact value codec used by this test remains unported. |
| `pkg/kv/key_test.go:63 TestIsPoint` | `COVERED` | `tests/key_version.rs::test_is_point` | All seven table rows execute in source order. |
| `pkg/kv/key_test.go:115 TestBasicFunc` | `COVERED` | `tests/key_version.rs::test_basic_func` | All three original rows execute through the typed source error identity; the classifier also executes both other retryable identities. |
| `pkg/kv/key_test.go:121 TestHandle` | `COVERED` | `tests/handle.rs::test_handle` plus `fixtures/handles.hex` | Every Int/Common/Partition assertion executes through production codec parsing, typed data decoding, successor/equality/ordering, and exact Go bytes. |
| `pkg/kv/key_test.go:161 TestPaddingHandle` | `COVERED` | `tests/handle.rs::test_padding_handle` | Short decimal encoding, nine-byte padding, exact encoded-column bounds, and padded reparse all execute against the Go oracle. |
| `pkg/kv/key_test.go:177 TestHandleMap` | `PARTIAL` | `tests/handle.rs::test_handle_map` | All portable Set/Get/Delete/Len/common-identity/Range early-stop semantics execute. Go map/string/interface layout-size assertions are deliberately not copied into Rust. |
| `pkg/kv/key_test.go:240 TestCommonHandlesFitIntHandleRange` | `PARTIAL` | `tests/handle.rs::test_common_handles_fit_int_handle_range` plus `fixtures/handles.hex` | Six source rows are byte-exact production encodes. The BinaryLiteral scalar is now source-complete in `tidb-datatype`, but the shared Datum kind and codec dispatch are absent; this row still enters Rust through the exact Go fixture and normalized UInt decoder, so it is not production-path coverage. |
| `pkg/kv/key_test.go:269 TestHandleMapWithPartialHandle` | `COVERED` | `tests/handle.rs::test_handle_map_with_partition_handle` | All five identities, partition separation, deletion, length transitions, and missing-partition deletion execute. |
| `pkg/kv/version_test.go:25 TestVersion` | `COVERED` | `tests/key_version.rs::test_version` | All four ordering/sentinel assertions execute. |

## Remaining key tests and benchmarks

| Go anchor | Owning implementation | Disposition |
| --- | --- | --- |
| `pkg/kv/key_test.go:325 TestMemAwareHandleMapWithPartialHandle` | `pkg/kv/key.go:589-700` and `pkg/util/hack.MemAwareMap` | `BLOCKED`: requires complete Handle identity plus a deliberate Rust memory-accounting design; Go map/slice capacity constants are not portable. |
| `pkg/kv/key_test.go:369 TestKeyRangeDefinition` | `pkg/kv/key.go:93-113` unsafe layout compatibility with kvproto | `PARTIAL` under `txnkv-copr-key-ranges`: safe generated-protobuf conversion now verifies both two-range payloads without layout or lifetime assumptions. The forbidden `unsafe.Pointer` alias and architecture-dependent 104-byte memory-size assertion remain deliberately unported. |
| `pkg/kv/key_test.go:390 BenchmarkIsPoint` | allocation benchmark for `KeyRange.IsPoint` | `BLOCKED`: correctness is covered, but the Go `ReportAllocs` benchmark needs the workspace's future reproducible benchmark/allocation harness before performance parity can be claimed. |
| `pkg/kv/key_test.go:437 BenchmarkMemAwareHandleMap` | MemAwareHandleMap | `BLOCKED`: Handle exists, but the portable memory-aware map and comparable benchmark harness are absent. |
| `pkg/kv/key_test.go:459 BenchmarkNativeHandleMap` | native-map comparison baseline | `BLOCKED`: HandleMap exists, but the paired reproducible benchmark harness is absent. |

## Remaining version-file tests

| Go anchor | Owning implementation | Disposition |
| --- | --- | --- |
| `pkg/kv/version_test.go:36 TestMppVersion` | `pkg/kv/mpp.go:31-83`, `pkg/kv/mpp.go:89-123`, `pkg/store/copr/mpp.go`, and kvproto `mpp.TaskMeta.mpp_version` (field 9) | `BLOCKED`: the parser/enum is only meaningful when `tidb-distsql` builds a real MPP task and serializes its `mpp.TaskMeta`; the wire contract is kvproto `mpp.proto`, not a KV primitive. Unblock by adding the checked-in prost `mpp.TaskMeta` contract to `tidb-proto`, then porting the source-owned `MPPTask::to_pb` consumer and its dispatch/round-trip tests in `tidb-distsql`. Do not add an enum-only `tidb-txnkv` facade. |
| `pkg/kv/version_test.go:70 TestExchangeCompressionMode` | `pkg/sessionctx/vardef/tidb_vars.go:2060-2111`, `pkg/sessionctx/variable/sysvar.go:3298-3320`, `pkg/planner/core/operator/physicalop/physical_exchange_sender.go:212`, and tipb `executor.proto` `CompressionMode` (values `NONE=0`, `FAST=1`, `HIGH_COMPRESSION=2`) | `BLOCKED`: this is session-variable parsing/defaulting plus tipb protobuf mapping, not a KV version. Unblock with the planned `tidb-session` variable owner, the generated `tipb.CompressionMode` in `tidb-proto`, and a real `tidb-distsql` ExchangeSender `to_pb` consumer/test. Duplicating the tipb enum or parser in `tidb-txnkv` would be a protocol stub. |

## Applied source-owned sparse evidence rows

The transaction waves own their matching fragments under
`rust/difftests/corpus/coverage/evidence/tests/`. The evidence steward owns the
generated inventory and merged check. The following rows remain mirrored here
so the workstream can audit its exact source-test dispositions:

```text
go_test\tpkg/kv/key_test.go\t121\tTestHandle\tCOVERED\ttxnkv-handle-codec-wave3\trust/difftests/transaction-tests/tests/handle.rs\tComplete safe Int Common and Partition Handle semantics execute through production comparable codec parsing and exact Go fixtures
go_test\tpkg/kv/key_test.go\t161\tTestPaddingHandle\tCOVERED\ttxnkv-handle-codec-wave3\trust/difftests/transaction-tests/tests/handle.rs\tShort decimal handle padding column boundaries and padded reparsing execute against exact Go bytes
go_test\tpkg/kv/key_test.go\t177\tTestHandleMap\tPARTIAL\ttxnkv-handle-codec-wave3\trust/difftests/transaction-tests/tests/handle.rs\tAll portable map identity mutation and early-stop range semantics execute; Go runtime layout memory constants are intentionally not copied
go_test\tpkg/kv/key_test.go\t240\tTestCommonHandlesFitIntHandleRange\tCOVERED\ttxnkv-handle-codec-wave3\trust/difftests/transaction-tests/tests/handle.rs\tAll seven source rows execute between exact min and max IntHandle bounds through production codec parsing
go_test\tpkg/kv/key_test.go\t269\tTestHandleMapWithPartialHandle\tCOVERED\ttxnkv-handle-codec-wave3\trust/difftests/transaction-tests/tests/handle.rs\tAll partition and nonpartition identities values deletion and length transitions execute
go_test\tpkg/kv/key_test.go\t325\tTestMemAwareHandleMapWithPartialHandle\tBLOCKED\ttxnkv-key-foundation-wave2\trust/difftests/transaction-tests/SOURCE_COVERAGE.md\tHandle identity now exists; the remaining dependency is a deliberate portable Rust memory-aware map instead of Go map and slice layout constants
go_test\tpkg/kv/key_test.go\t369\tTestKeyRangeDefinition\tPARTIAL\ttxnkv-copr-key-ranges\trust/crates/tidb-txnkv/tests/key_ranges_source.rs\tSafe field-by-field protobuf conversion is covered; the forbidden unsafe Go alias and Go-specific 104-byte layout assertion remain intentionally unported
go_test\tpkg/kv/key_test.go\t390\tBenchmarkIsPoint\tBLOCKED\ttxnkv-key-foundation-wave2\trust/difftests/transaction-tests/SOURCE_COVERAGE.md\tCorrectness is covered but allocation and performance parity require the workspace reproducible benchmark harness
go_test\tpkg/kv/key_test.go\t437\tBenchmarkMemAwareHandleMap\tBLOCKED\ttxnkv-key-foundation-wave2\trust/difftests/transaction-tests/SOURCE_COVERAGE.md\tHandle exists, but the portable memory-aware map and comparable benchmark harness are absent
go_test\tpkg/kv/key_test.go\t459\tBenchmarkNativeHandleMap\tBLOCKED\ttxnkv-key-foundation-wave2\trust/difftests/transaction-tests/SOURCE_COVERAGE.md\tHandleMap exists, but the paired reproducible native-map benchmark harness is absent
go_test\tpkg/kv/version_test.go\t36\tTestMppVersion\tBLOCKED\ttxnkv-key-foundation-wave2\trust/difftests/transaction-tests/SOURCE_COVERAGE.md\tNeeds kvproto mpp.TaskMeta field 9 plus a real tidb-distsql MPPTask to_pb and dispatch round trip; enum-only txnkv code would not prove serialization compatibility
go_test\tpkg/kv/version_test.go\t70\tTestExchangeCompressionMode\tBLOCKED\ttxnkv-key-foundation-wave2\trust/difftests/transaction-tests/SOURCE_COVERAGE.md\tNeeds tidb-session variable parsing/defaulting plus generated tipb CompressionMode NONE=0 FAST=1 HIGH_COMPRESSION=2 and a real tidb-distsql ExchangeSender to_pb consumer; do not duplicate a protocol stub in txnkv
```

The BinaryLiteral source audit supersedes the applied `COVERED` disposition
above. The integrating agent should replace it with this row (not add a
duplicate):

```text
go_test\tpkg/kv/key_test.go\t240\tTestCommonHandlesFitIntHandleRange\tPARTIAL\tdatatype-binary-literal-source-wave\trust/difftests/transaction-tests/tests/handle.rs\tSix source rows execute through production Datum and codec encoding; the BinaryLiteral row is byte-exact only through the Go fixture and normalized UInt decoder until the shared Datum kind and codec dispatch land atomically
```

## Next implementation order

1. Design the portable memory-aware Handle map without Go layout constants.
2. Add `tidb-proto` and a real MPP/session consumer before the two
   version-file protocol tests.
3. Establish the reproducible benchmark/allocation ring before claiming any
   source benchmark.
