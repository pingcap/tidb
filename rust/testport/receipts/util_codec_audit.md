# `pkg/util/codec` — Go-master parity audit

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

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
files above. Relative to the Rust branch's Go source snapshot (and to the
historical `b038` receipt's older pin), this current Go-master delta changes
only `codec.go` and `collation_test.go`: `Encoder` is documented as a
comparable-key encoder, `EncodeValue`/`HashCode` are package-level only, and
the obsolete encoder hash-equality assertion is removed.

## Rust ownership and parity decision

The package is owned by `rust/crates/tidb-codec`; its complete tracked owner,
test, benchmark, and manifest inventory is 52 files (14,212 lines), with the
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
package-level paths do not consult the comparable-key encoder mode. The
source-derived method-based hash test was deleted because its Go counterpart
no longer exists; the complete source-derived codec suite continues to cover
datum hash equality and all collation-aware group/row/column hash paths. A
follow-up source regression also asserts the raw-byte hash invariant directly.

## Validation

Profile: Ready for this package batch; the repository-wide audit is still
continuing.

- Go source diff against the fetched `origin/master` commit above and complete
  artifact inventory — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/codec -count=1` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-codec --test all -- --test-threads=1` — 163 passed.
- Focused `codec_package_source` run including the raw-byte hash regression — 63 passed.
- Focused `tidb-expr` hash-group-key/EncodeValue regression — passed (1).
- Focused `tidb-executor` hash-group-key consumer regression — passed (1).
- `cargo +nightly-2026-08-22 check --offline --locked -p tidb-codec -p tidb-expr -p tidb-executor --all-targets` — passed.
- `cargo +nightly-2026-08-22 check --offline --locked -p tidb-codec --benches` — passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, `make lint`, and
  `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required. A
broader `tidb-unistore --all-targets` check was not used as a package gate:
its existing `InProcessClient`/`SynchronousBatchRequestDispatcher` trait
failure is outside this codec change.

## Risk

- Correctness: low; comparable keys still use the immutable encoder mode,
  while values and hash codes now have one non-collating implementation just
  as Go master does.
- Compatibility: the three removed methods were Rust-only; all searched
  workspace consumers now call the package-level functions.
- Performance: the executor's generic non-string hash branch now obtains a
  temporary `Vec<u8>` from the existing free `hash_code` adapter before
  appending. Emitted bytes are unchanged; if profiling shows this branch is
  hot, a source-shaped append API can be introduced as a separate, measured
  change.
