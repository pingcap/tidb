# Comparable datum codec and Handle vertical slice

This ExecPlan is a living document. It follows `/Users/qiliu/projects/tidb/PLANS.md`
and owns only `rust/crates/tidb-codec`, `rust/crates/tidb-txnkv`, and the
transaction difftest leaves. The workspace member list, shared coverage ledger,
and rewrite handoff remain integration-owner files.

## Purpose

Translate the smallest real dependency-closed production boundary behind
`pkg/kv/key_test.go`'s Handle tests. `CommonHandle` must discover column
boundaries and decode data through a production codec; checked Go output is an
oracle, never a substitute for parsing. The Rust API uses a closed `Handle`
enum and typed fallible cross-kind comparison instead of Go interface panics.
It deliberately omits Go map/slice memory-size constants and unsafe layout
aliases because neither is a portable semantic contract.

## Source authority

- `pkg/util/codec/number.go` (complete file)
- `pkg/util/codec/{bytes,float,decimal,codec}.go` (comparable scalar paths,
  `DecodeOne`, `CutOne`, and `peek`)
- `pkg/types/mydecimal.go` (`WriteBin`, `FromBin`, `DecimalBinSize`)
- `pkg/kv/key.go:149-764` (Handle, HandleMap, PartitionHandle)
- `pkg/kv/key_test.go:121-323` (the five requested source tests)

## Progress

- [x] Read every source and test anchor above, including every table row.
- [x] Add `tidb-codec` comparable numeric/byte/decimal datum-key primitives.
- [x] Add complete safe `IntHandle`, `CommonHandle`, `PartitionHandle`, and
      `HandleMap<V>` public semantics to `tidb-txnkv`.
- [x] Generate and review exact Go codec fixtures for every requested handle
      vector, then execute direct Rust translations.
- [x] Run final WIP formatting, tests, and Clippy with 12 jobs.

## Discoveries

Round-trip tests alone masked a real translation defect in the first
`EncodeComparableVarint` implementation: choosing the shortest sign-extended
suffix encoded `-256` as a one-byte payload, while Go deliberately uses the
next width at `-(2^(8n))`. The implementation now uses the source magnitude
thresholds, and `number_boundaries.hex` checks both sides of every transition
through 8 bytes against a Go generator.

`tidb-datatype::Decimal` retains arithmetic digits beyond its SQL display
scale. Encoding `Display` would silently lose that precision. The datatype now
exposes semantic sign/coefficient/storage-scale accessors (not Go's word
layout), with an `8/7` regression proving hidden digits survive the boundary.

An independent post-port audit found a second real byte defect: the first
datum encoder ignored `StringDatum` collation, so default `utf8mb4_bin` keys
preserved trailing ASCII spaces even though Go's initialized new-collation
path removes them. A typed immutable `Encoder` now owns the persisted
new-versus-legacy collation mode; the default path uses new collation,
`utf8mb4_bin` applies its source key transform, and binary/legacy modes remain
raw. The pre-fix regression records the exact differing bytes. The same audit
completed all four public `float.go` routines and every original
`TestFloatCodec` round-trip/order row, including true smallest subnormals.

## Dependency boundary and deliberate exclusions

The codec accepts every datum variant currently exposed by `tidb-datatype`:
NULL, signed/unsigned integer, exact decimal, real, byte-preserving string, and
bytes. Go temporal/JSON/vector/enum/set/bit variants remain out because the
Rust datatype crate does not expose those representations. The one source
`BinaryLiteral` ordering row is preserved as a Go fixture and decoded as the
unsigned integer that Go's `EncodeKey` normalizes it into; inventing a second
opaque Rust datum kind here would put type ownership in the wrong crate.

`TestHandleMap` memory-size assertions and `TestMemAwareHandleMap...` are not
ported in this slice. Rust `HashMap`, `Vec`, and enum layouts are not Go
`map`/slice/interface layouts; copying those constants would be false evidence.
Map identity, overwrite/get/delete/length, partition separation, stored common
handle identity, and early-stop range semantics are all in scope.

## Validation contract

The Go generator is run with `GOFLAGS=-p=12`. Rust uses
`CARGO_BUILD_JOBS=12`. Unit tests cover codec failure paths as well as exact
vectors; transaction difftests translate the requested source assertions.
Completion here means this vertical slice passes scoped WIP checks, not that
all of `pkg/kv` or `pkg/util/codec` has been rewritten.

## Outcome

The dependency chain is now real: `tidb-txnkv` consumes `tidb-codec`, which
consumes the lossless `tidb-datatype` scalar representation. Five requested
Handle tests execute through production APIs. `TestHandleMap` remains
explicitly partial only for its Go runtime-layout memory assertions; every
portable behavior in that test is executable. The three Go generators were
rerun with 12-way package parallelism and diffed cleanly against their checked
fixtures. Scoped unit/differential tests, Clippy with warnings denied, and
rustfmt checks all pass.

The integration owner added `crates/tidb-codec` to the workspace member list.
A package-scoped root-workspace test then executed datatype, codec (including
both Go-oracle integration tests), txnkv, and transaction difftest packages
successfully with 12 build jobs.

The independent audit then reran 25 focused codec/txnkv/transaction tests plus
strict all-target Clippy after the collation and complete-float corrections.
General/unicode collation weights, hash paths, fixed-schema decimal
precision/truncation, and old-cluster session bootstrap wiring remain explicit
follow-on work rather than hidden compatibility branches.
