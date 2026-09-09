# `pkg/util/mathutil` — complete package transcreation

Pinned Go source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete inventory

The package has exactly eight artifacts, all read in full: three production
files, their three test files, `main_test.go`, and `BUILD.bazel`. There is no
package doc, README, fixture, generated or platform variant, benchmark, fuzz
target, example, or ownership file. The local Go package is unchanged from the
pin.

Production behavior includes the exponential moving average, integer and
floating-point helpers, batch division, and MySQL's synchronized two-seed RNG.
The Go suite has exactly eight test functions; `TestMain` only installs common
test setup and leak verification.

## Rust ownership and audit result

The three modules under `rust/crates/tidb-util/src/mathutil` own the complete
package and retain one translation of each of the eight Go tests. `Default` on
`ExponentialMovingAverage` is the native construction path for Go's usable
zero value; the RNG deliberately has no `Default` because Go's zero RNG has a
nil mutex and is unusable.

The audit removed public Rust-only cloning/debug formatting and `must_use`
diagnostics, eleven supplemental tests, the obsolete semantic manifest, and a
stale ExecPlan that required those additions. It also removed `tidb-exec`'s
legacy public copy of `Divide2Batches` and wired that consumer to this package's
existing implementation.

## Validation

Profile: **Ready** for this complete package parity fix and authority refresh;
this is not a repository-wide readiness claim.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/mathutil -count=1` — passed in the active and exact detached Go-master checkouts.
- `git diff --stat c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/mathutil` — empty; Go source is unchanged at the current authority.
- `cargo test -q -p tidb-util mathutil --lib --locked -- --test-threads=1` —
  passed; exactly eight tests ran.
- `cargo test -q -p tidb-exec ddl_job_merge --lib --locked --
  --test-threads=1` — passed; nine focused consumer tests ran.
- `cargo check -p tidb-util -p tidb-exec --all-targets --locked`,
  `cargo check -q -p tidb-expr -p tidb-executor -p tidb-session -p tidb-stats
  --all-targets --locked`, `cargo fmt --all --check`, and `git diff --check` —
  passed. Checks emitted only pre-existing warnings outside the changed code.

- `cd rust && cargo +nightly-2026-08-22 fmt --all -- --check` and
  `git diff --check` — passed.
- Pinned `make lint` — passed in a clean detached Go-master checkout.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: source algorithms and exact Go test vectors remain unchanged;
  the DDL consumer now calls the authoritative shared implementation.
- Compatibility: intentionally removes only Rust-only public traits,
  diagnostics, and the duplicate helper API.
- Performance: one monomorphized shared helper replaces identical local code;
  no algorithm changes.
