# `pkg/ttl/session` parity receipt

Status: Ready for this scoped batch. This receipt covers the complete Go
package inventory; it is not a repository-wide parity claim.

Published commit: pending publication for this batch; local and remote SHAs
will be recorded after the push/pull synchronization.

Comparison source: Go `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Rust owner: `rust/crates/tidb-ttl/src/session.rs` and
`rust/crates/tidb-ttl/tests/session_test.rs`.

## Complete Go inventory

All five tracked artifacts in `pkg/ttl/session` were read in full before
editing: 527 lines total, including production code, tests, and Bazel
metadata. There is no package `doc.go`, fixture or `testdata` directory,
generated source or input, platform/build-tag variant, benchmark, fuzz target,
README, or ownership artifact.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 46 | Go library/test targets and seven-way sharding |
| `session.go` | 229 | TTL session interface and transaction/session wrapper |
| `main_test.go` | 34 | common test setup and goleak options |
| `session_test.go` | 93 | transaction, time-zone, and kill tests |
| `sysvar_test.go` | 125 | TTL system-variable integration tests |

The Go production source and BUILD metadata are byte-identical to current Go
master. The Rust owner module and its 12 source-shaped session tests were read
in full; the four system-variable tests and the live `TestSessionKill` half
remain server/system-variable boundaries outside this crate.

## Parity findings and implementation

The Rust `Session` trait omitted two Go-owned interface methods: the embedded
`GetSessionVars` accessor and `GetSQLExecutor`. `SessionContext` now exposes
both as opaque boundary handles, and `TtlSession` forwards them by reference,
preserving the identity that Go callers observe. The former Rust-only
`without_avoid_reuse` constructor and optional callback state were removed;
Go's sole `NewSession` constructor requires the callback, so `AvoidReuse`
always invokes it. The callback no longer carries an unnecessary Rust
`Send + Sync` restriction.

## Focused regression coverage

`test_session_forwards_session_handles` asserts both accessors are forwarded
with stable identity from the wrapped context. Before this change the
source-shaped test could not compile because those methods were absent from the
Rust `Session`/`SessionContext` contracts; after the change the test and the
complete owner suite pass. The existing `test_session_avoid_reuse` now covers
the source constructor's required callback path without the Rust-only absent
callback case.

## Validation

Profile: **Ready**.

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-ttl --all-targets`
- the same locked toolchain with `cargo ... test --offline --locked -p tidb-ttl --tests -- --test-threads=1` — 20 cache, 12 session, and 6 SQL tests passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/ttl/session -count=1` — passed (4.917s).
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — required Ready gate.
- `git diff --check` — passed.

The Go package was not changed, so `make bazel_prepare` is not applicable to
this Rust-only implementation.

## Risks and unverified scope

- Correctness risk is limited to interface forwarding and callback lifecycle;
  identity and invocation are covered by focused tests.
- Compatibility risk: Rust callers using the removed `without_avoid_reuse`
  helper must use the source-shaped `TtlSession::new` constructor. In-tree
  callers already provide a callback.
- Performance is unchanged; accessors add no allocation and callback storage
  is one non-optional closure instead of an `Option`.
- Not verified locally: live Rust/Go SQL-executor exchange, the four Go
  system-variable tests, the server-backed sleep/kill half, non-host
  platforms, and repository-wide integration suites.

The rolling repository audit continues with the remaining package checklist.
