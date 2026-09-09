# `pkg/util/sqlescape` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All three pinned artifacts were read in full: `utils.go`, `utils_test.go`, and
`BUILD.bazel`. The package has one production file, one test/benchmark file,
and one Bazel library/test definition. The source test file contains five unit
tests and four benchmarks. There is no package doc, README, fixture, generated
source, platform or build-tag variant, fuzz target, test harness, example, or
ownership file. The checkout is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/sqlescape/mod.rs` owns production behavior and the
five source tests. `rust/crates/tidb-util/benches/sqlescape.rs` owns the four
source benchmarks, and the `tidb-util` manifest declares their harness-free
target. The byte-oriented SQL and argument boundaries preserve Go string
semantics while `SqlArg` represents Go's dynamic argument kinds.

The audit retained the complete formatter behavior and benchmark set, removed
the expanded transcreation narrative, two Rust-only internal edge tests, and
the separate Rust-only arbitrary-byte contract test. Go `TestMustUtils` checks
both panic messages exactly; the Rust source test now does the same instead of
checking only that `MustFormatSQL` panicked. Rust now has exactly the five
pinned unit tests and four pinned benchmarks.

## Rust follow-up: Go-discardable escape helpers

The Rust owner carried two explicit `#[must_use]` diagnostics on the direct Go
helpers `escape_string` and `must_escape_sql`. Go callers may discard either
string result, so these diagnostics were Rust-only and are now removed without
changing escaping, formatting, panic, or argument handling behavior.

The focused `return_values_may_be_ignored_like_go` regression discards both
results under `#[deny(unused_must_use)]`. On the pre-fix owner it failed to
compile with exactly two unused-return errors; the fixed test passes.

Ready validation for this follow-up:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib sqlescape::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib sqlescape::tests -- --test-threads=1` — passed; 6 tests.
- `cargo +nightly-2026-08-22 bench --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --bench sqlescape --no-run` — passed.
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-util -- --check`, repository `make lint`, and `git diff --check` — passed.

No Go, generated, fixture, platform, Bazel, or module artifact changed, so
`make bazel_prepare` is not required.

## Validation

Profile: WIP for the original package checkpoint; the follow-up above was
validated with the Ready profile within the continuing repository audit.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/sqlescape` — passed.
- `GOCACHE=/private/tmp/tidb-go-cache GOTOOLCHAIN=go1.25.10 go test -tags=intest,deadlock -count=1 ./pkg/util/sqlescape` — passed.
- `cargo test -p tidb-util --lib --offline sqlescape::tests:: -- --nocapture` — passed; 5 tests.
- `cargo bench -p tidb-util --bench sqlescape --offline --no-run` — passed.
- `cargo clippy -p tidb-util --lib --bench sqlescape --offline --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings` — passed; the allowed lints are existing package-wide blockers outside `sqlescape`.
- `rustfmt --edition 2021 --check crates/tidb-util/src/sqlescape/mod.rs crates/tidb-util/benches/sqlescape.rs` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: low; production logic is unchanged, every pinned source test
  passes, and the benchmark target compiles.
- Compatibility: the removed material was test-only or documentation-only;
  production consumers and public APIs are unchanged.
- Performance: none; runtime code and dependencies are unchanged.
