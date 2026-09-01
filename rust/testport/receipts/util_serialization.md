# `pkg/util/serialization` — complete Go-master package transcreation

Status: Ready for this atomic package batch. This is not a repository-wide
parity or PR-readiness claim.

Go source: `origin/master`
`db35d47066648fe73abce6318d53fc625df51490`.

Rust comparison branch: `origin/hparser-integration`
`5a005978dda57fbb3373a303660ea0a5f7990b38`.

## Complete inventory

The package has exactly four direct artifacts, all read in full:

- `common_util.go` — 52 lines; native-width constants and the nine interface
  type tags.
- `serialization_util.go` — 216 lines; native-endian primitive, decimal,
  time, duration, JSON, enum, set, opaque, string, bytes-buffer, interface,
  and Go-master `SerializeVectorFloat32` encoders.
- `deserialization_util.go` — 248 lines; `PosAndBuf`, reset/cursor helpers,
  native primitive and aggregate-value decoders, interface dispatch, and
  Go-master `DeserializeVectorFloat32`.
- `BUILD.bazel` — 16 lines; one public `go_library` over the three production
  files with `pkg/types` and `pkg/util/chunk` dependencies.

There is no `doc.go`, test or harness file, benchmark, fuzz test, fixture or
`testdata`, generated source/input, platform/build-tag variant, README, or
ownership artifact. The Go-master source delta is exactly the two vector
functions; the checkout package is the pre-delta copy.

## Function inventory and Rust mapping

The production function inventory is complete: `serializeBuffer`, all
`SerializeByte`/`Bool`/`Int`/`Int8`/`Uint8`/`Int32`/`Uint32`/`Uint64`/
`Int64`/`Float32`/`Float64`/`MyDecimal`/`Time`/`GoTimeDuration`/
`TypesDuration`/`JSONTypeCode`/`BinaryJSON`/`Set`/`Enum`/`Opaque`/`String`/
`BytesBuffer`/`Interface` encoders, the new `SerializeVectorFloat32`, and
`PosAndBuf.Reset`; `deserializeBuffer`, all corresponding primitive and
aggregate decoders, the new `DeserializeVectorFloat32`, and
`DeserializeInterface`.

The atomic Rust owner is `rust/crates/tidb-util/src/serialization.rs`,
exported by `rust/crates/tidb-util/src/lib.rs`. Its `Cursor` is the native
positional equivalent of `PosAndBuf`; existing aggregate spill consumers use
the same direct cursor/serialization path. The existing datatype owner
(`tidb-datatype::VectorFloat32`) supplies the source little-endian vector
wire image and checked decoder.

The new encoders preserve Go's length-prefixed spill framing: the vector's
serialized bytes are wrapped in the same native-width length prefix as every
other buffer. `Cursor::read_vector_float32` consumes that prefix, decodes the
vector, and deliberately ignores the decoder's unconsumed suffix just as Go
ignores the second return value. Rust's zero vector serializes to the same
four-byte zero-dimension image as Go's `ZeroVectorFloat32` special case.

The focused owner regression
`vector_float32_spill_round_trip_matches_go_layout` covers both the empty
zero vector and a non-empty `[1.25, -2.5, 0.0]` vector, asserts value equality,
and verifies that the native length prefix is fully consumed. No Rust-only
compression, validation, or alternate spill path was added.

Go's new callers live in `pkg/executor/aggfuncs` (outside this utility
package); the Rust executor's existing aggregate/value owners remain the
higher-level integration boundary. This receipt claims the complete utility
API and its source-shaped framing, not an invented duplicate executor spill
implementation.

## Validation

Profile: Ready. Commands run from the repository root:

- `git ls-tree -r --name-only origin/master -- pkg/util/serialization`, full
  file reads, declaration inventory, and `git diff origin/hparser-integration
  origin/master -- pkg/util/serialization` — passed; confirmed four
  artifacts and the exact two-function Go-master delta.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test
  ./pkg/util/serialization -count=1` — passed (`[no test files]`) on the
  pre-delta checkout; the Go-master package itself has no test artifact.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler
  DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib
  cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked
  -p tidb-util --lib serialization` — passed, including the focused
  vector regression (1 test).
- The same locked toolchain with
  `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --locked
  -p tidb-exec --lib` — passed for the existing spill consumers.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all --
  --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go source, import block, Bazel file, or module dependency changed in this
Rust-only batch, so `make bazel_prepare` is not required.

## Risks and unverified scope

- Correctness: both vector edge shapes round-trip with the source framing;
  native spill consumers compile. The Go-master package has no test to run;
  the pre-delta checkout package command passes with no tests.
- Compatibility: the new functions use the existing public Rust `Cursor`
  and vector type; no wire format or existing scalar behavior changes.
- Performance: encoding allocates the same vector byte image that the Rust
  datatype already owns; decoding returns an owned vector after consuming the
  source buffer, matching Go's explicit copy before zero-copy decode.
- Not verified locally: full Go-master `pkg/executor/aggfuncs` vector spill
  integration, non-host platform selections (the Go package has none), and
  full TiDB integration tests.
