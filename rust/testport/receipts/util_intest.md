# `pkg/util/intest` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly seven artifacts, all read in full: `assert.go`,
`assert_common.go`, `assert_test.go`, `in_unittest.go`, `no_assert.go`,
`not_in_unittest.go`, and `BUILD.bazel`. There is no package doc, benchmark,
fixture, generated input/output, README, or ownership file. The local Go
package is byte-identical to the pin.

The `intest` and `enableassert` build tags select the active assertion
implementation; without either tag only `EnableInternalCheck` activates an
assertion. The `intest` tag independently selects `InTest`. Common behavior
provides condition, error, nil, and function assertions, exact formatted panic
messages, the two exported runtime switches, and the slash-prefixed startup
failpoint. The package has one source test and no benchmarks.

## Rust ownership and audit result

`rust/crates/tidb-util/src/intest/mod.rs` is the sole owner. Same-named Cargo
features map the four Go production build selections. Rust unit tests select
the repository's normal `intest` test behavior. Atomics replace Go's mutable
package booleans, `Option` represents nil-capable inputs, and caller-formatted
strings replace Go's untyped variadic formatting edge.

The audit fixed default-build behavior: Rust previously checked
`ENABLE_ASSERT || ENABLE_INTERNAL_CHECK` in every build, whereas pinned
`no_assert.go` ignores `EnableAssert` and checks only `EnableInternalCheck`.
It also stopped accepting a non-slash failpoint name that the source init
comment explicitly rejects.

The sole inline test retains exactly `TestAssert` and its source cases. The
extra disabled-build assertions appended to that test were removed, along
with the separate three-test `intest_contract.rs` suite that has no upstream
artifact. Production build variants were validated directly rather than kept
as additional repository tests.

## Validation

Profile: WIP; this completes one package in the continuing package-by-package
audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/intest` — passed.
- `go test -tags=intest ./pkg/util/intest -count=1` — passed, 1 test.
- `cargo test --offline --locked -p tidb-util --lib intest::tests::TestAssert -- --exact` — passed, 1 test.
- `cargo check --offline --locked -p tidb-util --no-default-features` — passed.
- `cargo check --offline --locked -p tidb-util --no-default-features --features enableassert` — passed.
- `cargo check --offline --locked -p tidb-util --no-default-features --features intest` — passed.
- Temporary default, `enableassert`, and `intest` executable probes verified the runtime build-selection behavior; an exact slash-prefixed `GO_FAILPOINTS` probe verified startup activation. The probe was removed after passing.
- `cargo fmt -p tidb-util` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: all four build selections are represented, the source test
  passes on both sides, and the corrected default path no longer enables
  assertions from the wrong switch.
- Compatibility: removes only Rust-only tests and one undocumented extra
  failpoint spelling; all in-tree assertion callers retain their APIs.
- Performance: the default production path remains one relaxed atomic load;
  assertion-enabled builds perform the same two-switch decision as Go.
