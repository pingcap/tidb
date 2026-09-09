# `pkg/util/regexpr-router` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly three artifacts, all read in full:

- `regexpr_router.go` — filter classification, router construction and rule
  addition, schema/table routing, rule enumeration, extend-column extraction,
  and regexp capture concatenation;
- `regexpr_router_test.go` — eight tests covering construction, addition,
  schema/table/regexp routing, extraction, enumeration, and ambiguous matches;
- `BUILD.bazel` — one library and one test target with dependencies on the
  filter and table-router packages.

There is no `doc.go`, README, ownership file, generated/platform source,
fixture, benchmark, example test, or additional harness. The checkout is
byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/regexpr_router` is the sole package owner. It uses
the complete `tidb-util` filter and table-router owners, and its eight Go-named
tests reproduce every source assertion. The former two supplemental ASCII
Perl-class regressions were removed because they are absent from this Go
package's test surface; the shared Go-regexp behavior remains owned and tested
by the filter dependency.

The audit removed a second 766-line implementation from `tidb-exec`. Nothing
outside that file imported its module; its only consumers were its own copy of
the source tests plus four Rust-written tests. Keeping two public routers for
one Go package created independent error, filter, and rule paths. The
`tidb-exec` export is removed so all callers have one behavior authority.

The remaining owner's error no longer derives Rust-only cloning or value
equality. Go returns ordinary wrapped errors and exposes neither behavior.

The source-shaped `RouteTable::all_rules` and `fetch_extend_column` methods
also carried explicit Rust-only `#[must_use]` diagnostics. The focused
`return_values_may_be_ignored_like_go` regression discards both under
`#[deny(unused_must_use)]`: the detached pre-fix owner failed with exactly two
diagnostics, and the corrected owner passes.

## Validation

Profile: Ready for this focused parity fix within the continuing repository
audit, not a repository-wide readiness claim.

- `go test ./pkg/util/regexpr-router` — passed (8 tests).
- `cargo test --offline --locked -p tidb-util --lib regexpr_router:: --no-fail-fast`
  — passed, exactly 8 tests.
- `cargo test --offline --locked -p tidb-util --no-run` — passed.
- `cargo fmt -p tidb-util -- --check` and `git diff --check` — passed.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib regexpr_router::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed after the two-error pre-fix failure.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib 'regexpr_router::tests' -- --test-threads=1` — passed; 9 tests including the discard-contract regression.
- `OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/2de586d1417ea8a2/out/openssl-build/install OPENSSL_STATIC=1 cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --all-targets` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint` — passed as the Ready gate.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: reduced; only the validated owner can provide routing behavior.
- Compatibility: the unused duplicate `tidb-exec::regexpr_router` public path
  is removed; repository consumers did not use it.
- Performance: unchanged in the retained owner; duplicate code is no longer
  compiled into `tidb-exec`.
