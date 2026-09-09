# `pkg/param` — Go-master parity audit receipt

Status: complete dependency-closed audit; no current-master source behavior
delta or removable Rust-only policy was found.

Comparison source: Go `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). This package has
exactly two tracked artifacts and 72 lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `binary_params.go` | 43 | `BinaryParam` carrier and `ErrUnknownFieldType` identity |
| `BUILD.bazel` | 29 | one public library target and its errno/dbterror deps |

There is no `doc.go`, source test, benchmark, fixture/testdata directory,
generated or platform-specific variant, nested package, or test harness. The
production source has one exported variable, one exported type, and four
fields. Every source line and Bazel dependency was read in full before
comparing the Rust owners.

## Rust owner comparison

`rust/crates/tidb-protocol/src/binary_params.rs` owns the carrier shape as
`BinaryParam` (`tp`, `is_unsigned`, `is_null`, and raw `val`) and the complete
`parseBinaryParams` splitter. Its `BinaryParamError::UnknownFieldType` carries
the source's 8051 `ErrUnknownFieldType` errno, while malformed packets retain
the generic protocol-error boundary. The prepared-statement adapter forwards
that identity without inventing a second `param` crate or a cache-only value
decoder. All field-width, NULL-bitmap, long-data, string-charset, and unknown
type behavior is covered by the existing source-derived protocol suites.

The source package has no current-master delta. The Rust field names are native
snake-case carriers of the same wire data, and the error enum is the executable
equivalent of Go's `dbterror` value; no public Rust helper or policy exists to
remove.

## Validation

Profile: Ready for this no-change package audit; no production source changed.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/param -count=1` (package reports no test files)
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-protocol --test all binary_params_source -- --test-threads=1` (from `rust/`)
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-protocol --test all prepared_statement_protocol_source -- --test-threads=1` (from `rust/`)
- `cargo +nightly-2026-08-22 fmt --all -- --check` (from `rust/`)
- `git diff --check`

The Go package has no executable tests; both source-derived Rust protocol
suites pass. `make bazel_prepare`, failpoint toggling, and code-change lint are
not applicable because no Go, Bazel, or Rust production source changed.

This receipt certifies the bounded `pkg/param` inventory and owner mapping; it
is not a repository-wide transcreation claim.
