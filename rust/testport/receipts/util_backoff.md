# `pkg/util/backoff` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full: `backoff.go`,
`backoff_test.go`, and `BUILD.bazel`. There is no package doc, README, fixture,
generated or platform variant, benchmark, fuzz target, example, TestMain, or
ownership file. The local Go package is unchanged from the pin.

Production behavior is the one-method `Backoffer` interface and stateful
exponential backoff without jitter. Retry zero restores the base duration;
every other signed retry count advances once, converts the floating-point
product back to a signed nanosecond duration, and caps it at the configured
maximum.

## Rust ownership and audit result

`rust/crates/tidb-util/src/backoff.rs` owns the complete package. Signed Go
durations remain `i64`, Go `int` remains target-width `isize`, and the update
expression matches the pinned Go implementation, including checked NaN,
infinity, signed, and overflow probe results. `Default` and `Clone` retain Go's
zero-valued and copyable struct states.

The audit removed Rust-only debug formatting, compile-time constructor
evaluation, `must_use`, three supplemental tests, the retired semantic
manifest, and the stale ExecPlan that required those non-Go test artifacts.
The single Go `TestExponential` translation remains authoritative.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/backoff` — passed.
- `cargo test -q -p tidb-util backoff::tests --lib --locked --
  --test-threads=1` — passed; exactly one test ran.
- `cargo check -p tidb-util --all-targets --locked`, `cargo fmt --all --check`,
  and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the runtime update expression and source vector are unchanged.
- Compatibility: removes only repository-unused Rust diagnostics, formatting,
  const evaluation, and supplemental test artifacts.
- Performance: unchanged.
