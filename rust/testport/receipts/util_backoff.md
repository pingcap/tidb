# `pkg/util/backoff` — complete package transcreation

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The current Go
source is byte-for-byte unchanged from the pinned extraction.

## Complete inventory

The package has exactly three artifacts, all read in full. There is no package
doc, README, fixture, generated or platform variant, benchmark, fuzz target,
example, TestMain, or ownership file.

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 17 | `72f55941880341b84295308eff205ae06e5f7718` | `57a5d3e9598f7630495e88ba1ed259568ff6c2cbd19caa6909e0a3b4db0a3815` | library and flaky short test target inventoried |
| `backoff.go` | 58 | `454a66e593e9d9eabcd930bc0a919ca722b530d0` | `8d274f1da25d6f38ccd3ffebefc93106af25e29fda7baf7a24ca17821f967290` | `Backoffer`, stateful exponential update, reset-on-zero, and max cap inventoried |
| `backoff_test.go` | 38 | `0f87a4fd9e643d930d65f55da942d2eaeb0fa75d` | `de78b3b879b524b81d5a0a62fd43318c1479a87f8c08b719eb53045701eb63e5` | one source vector covering unit, constant, doubling, and cap cases inventoried |

Total: 113 textual lines. The package has three production declarations and
one source test identity.

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

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/backoff -count=1` — passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-util backoff::tests --lib -- --test-threads=1` — passed; exactly one test ran.
- `cargo +nightly-2026-08-22 check --offline --locked -p tidb-util --all-targets`, `cargo +nightly-2026-08-22 fmt --all -- --check`, and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: the runtime update expression and source vector are unchanged.
- Compatibility: removes only repository-unused Rust diagnostics, formatting,
  const evaluation, and supplemental test artifacts.
- Performance: unchanged.
