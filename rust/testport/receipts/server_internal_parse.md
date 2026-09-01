# `pkg/server/internal/parse` parity receipt

Status: complete inventory and parity fix. This receipt covers the complete
Go handshake parser package and does not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before editing, all four tracked artifacts were read in full: 512 total lines.
There is no package `doc.go`, fixture, generated source, platform variant,
benchmark, fuzz target, or nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 35 | parser target, test target, and source dependencies |
| `parse.go` | 315 | HandshakeResponse41 wire parser and attribute policy |
| `parse_test.go` | 114 | parser unit coverage and malformed-frame cases |
| `handshake_test.go` | 48 | complete client handshake fixture |

## Rust ownership and parity decision

`rust/crates/tidb-server/src/handshake.rs` owns the parser. Its prior
attribute pipeline returned a lossy `HashMap<String, String>`, a Rust-only raw
byte map, and a Rust-only warning vector. The pipeline now returns exact
`HashMap<WireString, WireString>` attributes plus transient warnings. Warnings
are emitted through the Rust `tidb-log` owner at the same parser boundaries
where Go logs them; malformed attributes remain non-fatal and preserve the
Go early-return behavior. Truncation markers and duplicate-key handling stay
aligned with Go's policy and metrics.

The source-derived tests were updated to query byte-owned attrs and retain
coverage for malformed lengths, NULL frames, truncation, duplicate keys,
reserved attributes, and metric boundaries.

## Validation

Profile: **Ready** for this package batch.

- `go test ./pkg/server/internal/parse ./pkg/server/internal/handshake -count=1`
  — passed.
- Focused Rust source tests were attempted with
  `cargo +nightly-2026-08-22 test --offline -p tidb-server --test all
  response41 -- --test-threads=1`; compilation was blocked by the host's
  missing OpenSSL development directory (`openssl-sys` cannot find pkg-config
  or headers). `cargo fmt --all -- --check` passed.
- `make lint` and `git diff --check` passed. `make bazel_prepare` was not
  required because no Go or Bazel artifact changed.

## Risks and unverified scope

The parser now exposes byte-owned attribute keys/values to Rust callers; this
is the required Go string contract but changes the previous Rust-only text
view. Logger initialization and live SQL handshake integration are not
verified locally because the focused Rust test binary cannot link without
OpenSSL development files. Go parser tests and static source coverage passed.
