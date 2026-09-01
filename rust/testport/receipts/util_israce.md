# `pkg/util/israce` — complete Go-master package transcreation

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete inventory

The package has exactly three artifacts, all read in full: `israce.go`,
`norace.go`, and `BUILD.bazel`. There is no package doc, test, test harness,
benchmark, fixture, generated input/output, or ownership file. The local Go
package is byte-identical to the pin. The inventory is 51 lines: 11 build
lines and 20 lines in each mutually exclusive source variant.

The `race` build tag selects the constant `RaceEnabled = true`; its `!race`
complement selects `false`.

## Rust ownership and audit result

`rust/crates/tidb-util/src/israce/mod.rs` owns the constant and the crate's
empty `race` feature selects the same compile-time true/false variants. The
ordinary printer consumes the value just as Go's printer does.

The earlier audit removed two Rust-only unit tests and the retired semantic-
gate manifest. These
artifacts had no Go counterparts and duplicated compile-time selection that is
validated by compiling both feature variants. The current living ExecPlan is
`rust/docs/operations/util-israce-audit-execplan.md`.

## Validation

Profile: **Ready** for this focused docs-only authority refresh. No Go, Rust,
Bazel, or module source changed, so `make bazel_prepare` is not required.

- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/israce` — passed.
- Pinned-Go `go list` and `go list -race` selected `norace.go` and `israce.go`, respectively, ignored the complementary file, and found no tests; the current and exact detached Go-master worktrees produced identical results.
- Pinned-Go `go test ./pkg/util/israce -count=1` and `go test -race ./pkg/util/israce -count=1` — passed in the current and exact detached Go-master worktrees (`[no test files]`).
- With the pinned OpenSSL environment, `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-util` and the same command with `--features race` — passed.
- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` and the batch diff checks — passed.

The focused commands emitted only existing workspace warnings. Full workspace
tests and Bazel execution remain outside this leaf receipt.

## Risk

- Correctness: unchanged production constant and build selection.
- Compatibility: removes only Rust-only tests and retired audit machinery.
- Performance: unchanged compile-time constant.
