# `pkg/util/timeutil` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All seven pinned artifacts were read in full: `errors.go`, `time.go`,
`time_zone.go`, `time_test.go`, `time_zone_test.go`, `main_test.go`, and
`BUILD.bazel`. The package has three production files, two source-test files,
one common-test/goleak harness, and one Bazel library/test definition. It has
no package doc, README, fixture, benchmark, generated source, platform
variant, or ownership file. The checkout is byte-identical to the pin.

## Rust ownership and audit result

`rust/crates/tidb-util/src/timeutil/mod.rs` and `time_zone.rs` own the package.
They provide the typed unknown-timezone error, context-aware sleep, system
timezone inference and initialization, location loading, current-zone lookup,
zone naming, timezone construction, UTC day-period comparison, and MySQL
timezone parsing. `chrono-tz` is the native IANA location authority; unlike
Go's file-backed `time.LoadLocation`, it uses compiled timezone data and does
not need Go's observable-I/O-free location cache optimization.

The package audit retained the production behavior and removed the expanded
transcreation narrative plus four Rust-only supplemental tests and assertions
that have no pinned source-test counterpart. The exact missing named-zone
offset cases and invalid-name error check from Go `TestConstructTimeZone` were
added. The seven pinned source tests remain covered. `main_test.go` is a Go-only
common-test and goroutine-leak harness with no Rust production port.

## Rust follow-up: Go-discardable sleep-context returns

The Rust owner carried four explicit `#[must_use]` diagnostics on
`SleepContext::background`, `with_timeout`, `is_cancelled`, and `remaining`.
These constructors and observers carry Go context state, whose return values
callers may discard, so the diagnostics were Rust-only and are now removed
without changing cancellation, deadline, sleep, or timezone behavior.

The focused `timeutil::tests::return_values_may_be_ignored_like_go` regression
discards all four results under `#[deny(unused_must_use)]`. On the pre-fix owner
it failed to compile with exactly four unused-return errors; the fixed test
passes.

Ready validation for this follow-up:

- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib timeutil::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib timeutil:: -- --nocapture` — passed; 9 tests.
- `cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-util -- --check`, repository `make lint`, and `git diff --check` — passed.

No Go, generated, fixture, platform, Bazel, or module artifact changed, so
`make bazel_prepare` is not required.

## Validation

Profile: WIP; this is one package checkpoint in the continuing repository
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/timeutil` — passed.
- `GOCACHE=/private/tmp/tidb-go-cache GOTOOLCHAIN=go1.25.10 go test -tags=intest,deadlock -count=1 ./pkg/util/timeutil` — passed.
- `cargo test -p tidb-util --lib --offline timeutil:: -- --nocapture` — passed; 7 tests.
- `cargo clippy -p tidb-util --lib --offline --no-deps -- -A clippy::map-or-identity -A clippy::chunks-exact-to-as-chunks -A clippy::wrong-self-convention -A clippy::new-without-default -A clippy::len-without-is-empty -A clippy::should-implement-trait -D warnings` — passed; the allowed lints are existing package-wide blockers outside `timeutil`.
- `rustfmt --edition 2021 --check crates/tidb-util/src/timeutil/mod.rs crates/tidb-util/src/timeutil/time_zone.rs` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: low; production logic is unchanged and every pinned source test
  passes in both implementations.
- Compatibility: the removed material was test-only or documentation-only;
  production consumers are unchanged.
- Performance: none; runtime code and dependencies are unchanged.
