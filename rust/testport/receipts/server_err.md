# `pkg/server/err` parity receipt

Status: complete inventory and parity boundary; no production edit was
required. This receipt covers the Go server error-prototype package and does
not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before editing, both tracked artifacts were read in full: 65 total lines and
15 `dbterror.ClassServer.NewStd` declarations. There is no package `doc.go`,
test, fixture, generated source, platform variant, benchmark, fuzz target, or
nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 12 | public library target and errno/dbterror dependencies |
| `error.go` | 53 | 15 server-class standard error prototypes |

## Rust ownership and boundary

`rust/crates/tidb-error/src/server_errors.rs` provides all 15 corresponding
`LazyLock<TerrorError>` values. Its shared errno/message catalogs preserve the
Go numeric code, message template, RFC identity (`server:<code>`), and SQL
state; the source-derived `server_err_source` matrix checks every prototype.
The package is a leaf error catalog, so no additional Rust transport or server
adapter is needed.

No Rust-only behavior or missing Go behavior was found. Adding a second
catalog would risk divergent error identity and messages.

## Validation

Profile: **Ready** for this documentation-only package boundary.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/server/err -count=1` — passed (no Go test files; package compiled).
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-error --test all server_error -- --test-threads=1` (from `rust/`) — passed the 15-row source matrix.
- `git diff --check` — passed.

No Bazel or Go source/build artifact changed, so `make bazel_prepare` was not
required. The full repository lint and non-server integration suites remain
outside this leaf-package boundary.

## Risks and unverified scope

Correctness and compatibility risk are low because this batch changes no
runtime code or catalog values. Unverified here are live server handshake
paths, generated Bazel execution, and non-host platform builds.
