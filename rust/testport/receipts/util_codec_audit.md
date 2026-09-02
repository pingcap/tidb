# `pkg/util/codec` — Go-master parity audit

Status: complete dependency-closed audit at the current Go-master authority;
the package-level value/hash paths now use the source-shaped non-collating
implementation while legacy encoder methods remain as compatibility wrappers
for existing in-tree callers.

Comparison source: Go `origin/master` at commit
`1c1a334d2be1dce64888b6e1f054462c566b0734` (2026-09-02).

## Complete inventory

The package has exactly 12 Go artifacts and 4,542 lines. All production,
test, benchmark, harness, and Bazel files were read before editing. The
production surface contains 90 functions; the tests contain 32 `Test*`
functions (including `TestMain`) and six `Benchmark*` functions.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 57 | library/test targets and dependency rows |
| `bench_test.go` | 118 | six codec benchmarks and daily harness |
| `bytes.go` | 227 | comparable/compact byte framing |
| `bytes_test.go` | 120 | byte codec vectors and malformed inputs |
| `codec.go` | 1,957 | datum key/value, hash, row, and chunk codecs |
| `codec_test.go` | 1,340 | scalar, row, range, hash, and size tests |
| `collation_test.go` | 189 | encoder and hash collation tests |
| `decimal.go` | 69 | decimal framing and decoding |
| `decimal_test.go` | 81 | decimal round-trip tests |
| `float.go` | 66 | sortable float framing |
| `main_test.go` | 33 | testkit/goleak bootstrap |
| `number.go` | 285 | fixed, compact, and comparable integers |

There is no `doc.go`, fixture tree, generated source, or platform-specific Go
variant. The Bazel target lists exactly the five production files and six test
files above. Relative to the hparser branch, current Go master changes only
`codec.go` and `collation_test.go`: `Encoder` is documented as a comparable-key
encoder, value/hash encoding is package-level and non-collating, and the
obsolete encoder hash-equality assertion is removed. This checkout keeps
source-compatible `Encoder.EncodeValue` and `Encoder.HashCode` wrappers for
the existing `pkg/tablecodec` and benchmark callers; both delegate to the
same non-collating paths.

## Follow-up current-master null-key fix

The `febee17ec7` Go-master regression exercises a `TypeNull` join key. The
pre-allocation pass must call its existing `canSkip` closure for TypeNull so
the closure marks `nullVector` before hash computation; omitting that call lets
an empty NULL key collide with an empty byte key. This batch restores that
per-row behavior in `codec.go`. The join package owns the focused regression
and is recorded separately; no Rust codec change is needed because the Rust
owner already marks null join keys before serialization.

## Rust ownership and parity decision

The package is owned by `rust/crates/tidb-codec`; its complete tracked owner,
test, benchmark, and manifest inventory is 52 files (14,222 lines), with the
workspace `aggregate-tests.rs` build script generating `OUT_DIR/all_tests.rs`.
There are no target-specific codec variants; `cfg(test)` blocks are ordinary
unit-test modules. The prior `b038` audit established the complete source-test
mapping, decimal/collation dependency boundary, row-codec integration, and
consumer inventory.

Go master makes `Encoder` key-only. Rust previously retained three
Rust-specific methods: `Encoder::encode_value`,
`Encoder::encode_value_in_timezone`, and `Encoder::hash_code`. They were
removed. The free value functions now own the non-collating compact encoding,
and the free `hash_code` function owns the lossless hash encoding. The
executor hash-group-key path and expression source regression use those
ordinary package functions, so no second collation-mode behavior remains.

The focused regressions compare a collated string datum with the same raw
string datum through both `encode_value` and `hash_code`, proving those
package-level paths do not consult the comparable-key encoder mode. The Go
compatibility assertion additionally proves the retained method wrapper emits
the same raw value bytes. The complete source-derived codec suite continues to
cover datum hash equality and all collation-aware group/row/column hash paths.

## Rust follow-up: TypeNull join-key parity

Go master `febee17ec716d86b1e355e5400ef9e4f4f190bad` fixes the hash-join-v2
NULL build-key regression by making the `TypeNull` branch invoke `canSkip` in
both key pre-allocation and serialization. Rust already treated an explicit
`Datum::Null` as a skipped key, but a row-backed typed-column path can carry a
non-NULL placeholder datum for `FieldTypeCode::Null`. Before this follow-up,
`serialize_keys` therefore emitted an empty key without setting its NULL
marker, allowing it to collide with an empty BLOB key. The Rust package now
marks every `FieldTypeCode::Null` row as NULL before encoding, matching the Go
field-type contract while preserving the existing empty-key bytes.

The focused regression is
`source_serialize_keys_marks_type_null_columns_as_null` in
`rust/crates/tidb-codec/tests/codec_package_source.rs`; it supplies an empty
byte placeholder under `FieldTypeCode::Null` and requires an empty serialized
key plus a true NULL marker. The package inventory above includes every Rust
production module, unit/integration source, benchmark, manifest, aggregate
test build script, generated `OUT_DIR/all_tests.rs` artifact, and platform
variant audit; no target-specific Rust codec source exists.

## Validation

Profile: Ready for this package batch; the repository-wide audit is still
continuing.

- Go source diff against the fetched `origin/master` commit above and complete
  artifact inventory — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/util/codec -run '^TestEncoderNewCollationEnabled$' -count=1` — passed, including the package-level/non-collating value guard.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/codec -count=1` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-codec --test all -- --test-threads=1` — 163 passed.
- Focused `codec_package_source` run including the raw-byte hash regression — 63 passed.
- Focused `tidb-expr` hash-group-key/EncodeValue regression — passed (1).
- Focused `tidb-executor` hash-group-key consumer regression — passed (1).
- Before the Rust follow-up,
  `source_serialize_keys_marks_type_null_columns_as_null` failed with
  `nulls == [false]`; after the fix it passes with `[true]`.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-codec --test all source_serialize_keys_marks_type_null_columns_as_null -- --nocapture` — passed.
- `cargo +nightly-2026-08-22 check --offline --locked -p tidb-codec -p tidb-expr -p tidb-executor --all-targets` — passed.
- `cargo +nightly-2026-08-22 check --offline --locked -p tidb-codec --benches` — passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, `make lint`, and
  `git diff --check` — passed.

Go production and test files changed, but no import section, new file, or Bazel
target changed, so `make bazel_prepare` is not required by the repository gate.
A broader `tidb-unistore --all-targets` check was not used as a package gate:
its existing `InProcessClient`/`SynchronousBatchRequestDispatcher` trait
failure is outside this codec change.

Follow-up validation for the current-master TypeNull path:

- Before the fix, the Go-master `TestAntiSemiJoinTypeNullBuildKey` regression
  failed with an empty result; after restoring the `canSkip` call it passes in
  the owning join package.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh pkg/util/codec -run '^Test' -count=1 -vet=off` — passed.
- `make bazel_prepare` — required by the Go production edit; blocked locally
  because the `bazel` executable is unavailable.

## Risk

- Correctness: low; comparable keys still use the immutable encoder mode,
  while values and hash codes now have one non-collating implementation just
  as Go master does.
- TypeNull join keys now follow the field type rather than any placeholder
  datum, so NULL keys cannot hash-match empty byte keys. Explicit datum NULL
  handling remains unchanged.
- Compatibility: the three removed methods were Rust-only; all searched
  workspace consumers now call the package-level functions.
- Performance: the executor's generic non-string hash branch now obtains a
  temporary `Vec<u8>` from the existing free `hash_code` adapter before
  appending. Emitted bytes are unchanged; if profiling shows this branch is
  hot, a source-shaped append API can be introduced as a separate, measured
  change.
