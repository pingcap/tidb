# `pkg/util/dbterror` rolling Go-master audit

This ExecPlan is a living document for the root package only. The nested
`pkg/util/dbterror/exeerrors` and `pkg/util/dbterror/plannererrors` directories
retain separate atomic receipts.

## Progress

- [x] (2026-09-06) Removed Rust-only `#[must_use]` diagnostics from the two
  source-shaped `ErrClass` constructors. The deny-on-discard regression failed
  with two diagnostics on the detached pre-fix owner and passes with all five
  Rust dbterror tests, the owner all-target check, formatting, lint, and diff
  hygiene. The package-scoped commit was rebased onto the latest remote tip,
  pushed to `hparser-integration`, and local/remote SHAs were verified equal.

- [x] (2026-09-02) Inventoried and read all five root artifacts: `BUILD.bazel`,
  `ddl_terror.go`, `terror.go`, `terror_test.go`, and `main_test.go`. There is
  no package `doc.go`, fixture, generated/platform variant, benchmark, fuzz
  target, or build-tagged source in this root package.
- [x] (2026-09-02) Compared every artifact with Go master at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`; production `ddl_terror.go` was
  missing exactly two current-master TiFlash error prototypes.
- [x] (2026-09-02) Added the exact Go definitions and a focused source/catalog
  regression. The regression failed before the fix (228 prototypes) and passes
  after restoration (230 prototypes plus code/message checks).
- [x] (2026-09-02) Ran the full Ready profile: focused/full/race Go tests,
  unchanged Rust dbterror tests, repository lint, Rust formatting, and diff
  hygiene. `make bazel_prepare` was attempted and is blocked only by the
  unavailable local `bazel` binary. This scoped commit is ready to publish.

## Decision log

- Keep the two Go-master `ErrTiFlashColumnarStorage*` prototypes in the Go
  catalog. They are public error identities used by current TiFlash admission
  behavior and are not Rust-only additions.
- Do not hand-edit `rust/crates/tidb-util/src/dbterror/ddl_errors.rs` or its
  generated fixture. Those files declare mechanical generation and currently
  represent the prior 228-entry Rust owner. Record the two-entry generated
  refresh as an explicit follow-up boundary.
- Keep the regression source-oriented: the package has no exported registry
  of all DDL prototypes, so parsing the package's own `ddl_terror.go` provides
  a deterministic guard against both omissions and accidental extra entries.

## Validation plan

Run the focused and complete Go package tests, including the race profile. The
new test's import and top-level test additions require `make bazel_prepare`; if
the local toolchain lacks Bazel, preserve that exact failure in the receipt.
Run the Rust `tidb-util` dbterror tests as an unchanged-owner gate, then run
Rust formatting, repository lint, and `git diff --check`. No failpoint wrapper
is needed because this package has no failpoint dependency.

## Risks and handoff

Correctness risk is limited to restoring two standard DDL error identities and
their Go-master message templates. Compatibility risk is that Rust's generated
228-entry catalog does not yet expose the two new names; callers crossing the
Go/Rust boundary must wait for the generator refresh. Performance is unchanged.
