# `pkg/format/textrow` — Go-master parity audit receipt

Status: complete dependency-closed audit; the unknown-session-charset fallback
is now source-aligned for the package introduced by Go commit `7a93ade309`.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package has
five tracked artifacts and 732 lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 41 | library/test targets and shard metadata |
| `result_encoder.go` | 157 | result/session charset policy |
| `result_encoder_test.go` | 63 | encoder and string-type tests |
| `textrow.go` | 165 | typed scalar formatting and float rendering |
| `textrow_test.go` | 306 | value, invalid-type, and float tests |

There is no package `doc.go`, platform variant, generated Go source, fixture,
benchmark, or nested Go package. The production files contain 11 function or
method declarations; the tests contain seven declarations including the
one-column helper. Every Go artifact was read in full before comparing its
Rust owners.

## Rust owner and support-artifact inventory

The dependency-closed owner is `tidb-protocol`: `src/textrow.rs` (616 lines)
owns typed scalar/Datum formatting and `AppendFormatFloat`; `src/result_encoder.rs`
(488) owns metadata/data charset policy; `src/column.rs` (227) owns column
metadata framing; `src/result.rs` (102) owns length-encoded row framing; and
`src/resultset_stream.rs` (659) connects typed values and charset conversion to
the live result writer. Source tests cover `tests/textrow_source.rs` (214),
`tests/textrow_go_vectors.rs` (125), `tests/resultset_stream_source.rs` (226),
and `tests/column_metadata_source.rs` (201). The generated Go-backed fixture
`rust/difftests/transaction-tests/fixtures/textrow_vectors.tsv` has 1,516
rows, produced by its 197-line `generate_textrow_vectors.go` build-ignored
generator; the Rust vector test asserts that complete row count.

The Rust owner preserves signed/unsigned and year formatting, column-scoped
float precision, decimal/temporal/named-value branches, invalid-type errors,
the exact string-like type partition (including VECTOR), metadata charset
rewriting, and result-charset/column-charset precedence. It also routes both
borrowed and owned result rows through the same source-shaped formatter, so
the allocation optimization does not create a second wire behavior.

`ResultEncoder` now follows Go's `FindEncodingTakeUTF8AsNoop` for unknown
session charset spellings: it retains an `Unknown` binary-fallback state,
advertises charset number zero through `ColumnCharsetID`, and preserves source
bytes for metadata and row data. The old Rust-only construction refusal and
the unused `UnsupportedCharsetName` error variant are gone. Registered
collation IDs and all source-supported encodings remain covered by vectors and
owner tests.

## 2026-09-05 fix evidence

- Go `pkg/format/textrow/result_encoder_test.go::TestResultEncoder` constructs
  `NewResultEncoder("utf-8")`; Go's unknown encoding lookup returns the binary
  encoder and does not fail. Before this fix the Rust source regression
  `result_encoder_unknown_charset_keeps_go_binary_fallback` failed with
  `UnsupportedCharsetName("utf-8")`.
- After the fix the regression verifies all three source-visible effects:
  charset number `0`, byte-preserving metadata, and byte-preserving row data
  even after a registered source-column charset is selected.

## Validation

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/format/textrow -count=1`
- `LC_ALL=C LANG=C cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-protocol --test all textrow -- --test-threads=1`
- `cargo fmt --manifest-path rust/Cargo.toml --all`
- `git diff --check`
- Ready profile `make lint` (after the package commit is rebased onto the
  latest `hparser-integration`)

The Go suite passes; the Rust Go-vector/source tier passes six tests, including
the new unknown-name regression. No Go or generated artifact changed, so
`make bazel_prepare` and failpoint toggling remain inapplicable. Broader server
integration is covered by existing protocol/server consumers and remains
outside this leaf audit.

This receipt certifies the bounded `pkg/format/textrow` inventory and parity
check; it is not a repository-wide transcreation claim.
