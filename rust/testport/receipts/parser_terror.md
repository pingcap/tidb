# `pkg/parser/terror` — Rust parity receipt

Status: complete package inventory and Rust-only API alignment for the
current Go package.

## Go inventory

The nested parser module was read at the checked-out Go authority. Its complete
package boundary is exactly:

| Artifact | Role |
| --- | --- |
| `pkg/parser/terror/terror.go` (337 lines) | error classes/codes, registration and freeze state, RFC identities, MySQL conversion, equality, logging, and termination helpers |
| `pkg/parser/terror/terror_test.go` (173 lines) | five source tests plus the stack/location assertions |
| `pkg/parser/terror/BUILD.bazel` (27 lines) | library and test targets |

There is no `doc.go`, fixture/testdata directory, generated Go input/output,
benchmark, fuzz target, example, or platform/build-tag variant. The package is
inside `pkg/parser/go.mod`; its focused Go gate is `go test ./terror` from that
nested module.

## Rust owner inventory

The dependency-closed owner is `rust/crates/tidb-error`. `src/terror.rs`
transcreates the package's class/code registry, RFC identity, registration
freeze, generated errors, JSON compatibility, SQL conversion, equality,
logging, cleanup, and stack capture. Its source-backed carrier is
`tests/terror_source.rs`; the crate's other catalog modules provide the MySQL
and TiDB message/state inputs consumed by `NewStd` and are unchanged by this
batch. The owner has no target-specific Rust variant or generated build output;
Cargo's `tests/all.rs` aggregator is the only test build artifact.

## Alignment

Go permits callers to discard all return values in `terror.go`. Rust had added
`#[must_use]` to 23 public result-returning functions and methods, making those
call sites fail to compile under `deny(unused_must_use)` even though the Go
package has no equivalent diagnostic. Those annotations were removed from the
Rust owner; no runtime error, registration, formatting, SQLSTATE, JSON, or
stack behavior changed.

The focused regression
`terror_source::return_values_may_be_ignored_like_go` compiles and executes a
representative discard of every affected API family under
`#[deny(unused_must_use)]`. On the pre-fix owner it failed with 23 compiler
errors; after the alignment it passes.

## Validation

Validation profile: **Ready** for this package batch.

- Nested Go gate: `go test ./terror -count=1` — passed.
- Pre-fix proof in detached worktree `d2daedbb9f2`: the focused regression
  failed with 23 `unused return value` errors.
- Focused Rust gate:
  `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-error --test all terror_source::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed.
- Complete Rust source carrier:
  `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-error --test all terror_source -- --test-threads=1` — passed (13 tests).
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-error --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `make lint` — passed.
- `git diff --check` — passed.

No Go source, Go import section, top-level Go test, Bazel file, or module
dependency changed, so `make bazel_prepare` was not required. Failpoint
enable/disable is not applicable to this package.

## Risk

The change affects compile-time diagnostics only and deliberately leaves the
source error identity and protocol behavior intact. The unportable Go runtime
stack-file formatting assertion remains represented by the Rust-native
backtrace test; no fabricated source-location behavior was added.
