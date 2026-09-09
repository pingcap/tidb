# `pkg/server/internal/handshake` parity receipt

Status: complete inventory and parity fix. This receipt covers the complete
Go handshake value package and does not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before editing, both tracked package artifacts were read in full: 41 total
lines. There is no package `doc.go`, test, fixture, generated source, platform
variant, benchmark, fuzz target, or nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 14 | package library target and dependencies |
| `handshake.go` | 27 | `Response41`, the eight-field handshake response value |

## Rust ownership and parity decision

`rust/crates/tidb-server/src/handshake_response.rs` is the native owner. The
Go response contains exactly eight fields: `Attrs`, `User`, `DBName`,
`AuthPlugin`, `Auth`, `ZstdLevel`, `Capability`, and `Collation`. Rust had two
source-absent fields (`raw_attrs` and `attr_warnings`) and represented attrs
through a lossy UTF-8 map. Those fields were removed. Attributes now use
`HashMap<WireString, WireString>`, preserving Go string byte identity for
arbitrary wire bytes while retaining zero-value and duplicate-key semantics.

The exact-field regression destructures the Rust response and therefore fails
if either Rust-only field is reintroduced. The byte lookup assertions cover
non-UTF-8-safe ownership without adding a second compatibility view.

## Validation

Profile: **Ready** for this package batch.

- `go test ./pkg/server/internal/handshake -count=1` — package compiled (no Go
  test files).
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-server --test all response41 -- --test-threads=1` — passed all four focused response tests.
- `cargo fmt --all -- --check` — passed.
- `make lint` and `git diff --check` passed. `make bazel_prepare` was not
  required because no Go or Bazel artifact changed.

## Risks and unverified scope

The public Rust field type changes from text keys/values to exact wire-owned
bytes; this is intentional Go parity but is an API compatibility consideration
for future Rust consumers. Live handshake integration remains outside this
focused source suite. No Go production behavior changed.
