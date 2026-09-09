# Align `pkg/util/stmtsummary` v1 return contracts in Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while the
work proceeds.

Reference: `PLANS.md` at repository root. This plan follows its requirements
and the package-atomic transcreation rule in `AGENTS.md`.

## Purpose / Big Picture

The native Rust owner for the direct Go package `pkg/util/stmtsummary` v1
should accept the same ordinary call patterns as Go. Go callers may discard
constructor, accessor, formatting, average, and collection results. Equivalent
Rust functions must therefore not add `#[must_use]` diagnostics that reject
those source-valid call sites. After this change, a focused
`#[deny(unused_must_use)]` regression can discard every affected result and
compile, while native Rust `Option` boundaries remain protected.

The atomic source boundary is the direct v1 Go package. Its three production
files, three test/harness files, and `BUILD.bazel` were already enumerated as
part of the complete 22-artifact `pkg/util/stmtsummary` inventory in
`rust/testport/receipts/util_stmtsummary_audit.md`. The nested Go package
`pkg/util/stmtsummary/v2` is a separate implementation unit and is not edited
by this batch.

## Progress

- [x] (2026-09-07) Reuse and verify the complete 22-artifact Go package
  inventory, then read every direct v1 production function, test, build target,
  corresponding Rust module, caller, manifest, and workspace/lock entry before
  editing. No direct fixture, generated source, or platform variant exists.
- [x] (2026-09-07) Classify the v1 Rust return surface. Remove 39 annotations
  from ordinary Go-shaped returns and retain eight annotations on native
  `Option` boundaries.
- [x] (2026-09-07) Add three focused deny-on-discard regressions. Restoring the
  removed annotations produces exactly 39 compile diagnostics; the corrected
  source passes all three tests.
- [x] (2026-09-07) Run the complete `tidb-stmtsummary` library suite (67 tests)
  and all-target compilation successfully.
- [x] (2026-09-07) Run scoped formatting, the Ready lint gate, and diff
  hygiene; update the receipt and root parity plan.
- [ ] Create one package-scoped commit and publish it to the latest
  `origin/hparser-integration` without force.
- [ ] Continue the rolling Rust-only audit at the next complete Go-package
  boundary.

## Surprises & Discoveries

- Observation: the runtime implementation already matches the represented Go
  behavior; the gap is entirely a Rust-only compile-time consumption policy.
  Evidence: temporarily restoring all 39 removed annotations makes the three
  focused tests fail with exactly 39 `unused return value` diagnostics, while
  the corrected source passes without changing runtime logic.
- Observation: eight remaining annotations protect values represented as
  `Option` in Rust and therefore do not model ordinary direct Go returns.
  Evidence: the focused regressions use `let _ =` at those boundaries and
  discard every corrected ordinary result directly.

## Decision Log

- Decision: treat only direct `pkg/util/stmtsummary` v1 as this package batch;
  leave nested `v2` code unchanged even though both surfaces compile in the
  same Rust crate.
  Rationale: Go package directories are the minimum atomic implementation and
  commit unit, and `pkg/util/stmtsummary/v2` is a distinct Go package.
  Date/Author: 2026-09-07 / Codex.
- Decision: remove only annotations on direct Go-shaped ordinary returns and
  retain all eight native `Option` contracts.
  Rationale: this restores source-compatible result consumption without
  weakening Rust ownership/error boundaries or changing runtime behavior.
  Date/Author: 2026-09-07 / Codex.
- Decision: do not run Go tests or `make bazel_prepare` for this batch.
  Rationale: the user requested Rust-only work, and no Go source/import,
  Bazel, module, generated input, or Cargo metadata changes.
  Date/Author: 2026-09-07 / Codex.

## Plan of Work

Milestone 1 inventories the complete package boundary and classifies every
return annotation. Acceptance is an explicit list of direct v1 artifacts and
a partition of the return surface into ordinary Go-shaped values versus
native Rust `Option` values.

Milestone 2 edits `rust/crates/tidb-stmtsummary/src/statement_summary.rs`,
`evicted.rs`, and `reader.rs`. It adds one focused regression per module under
`#[deny(unused_must_use)]`. Acceptance is a pre-fix compile failure with one
diagnostic for every removed annotation and a passing post-fix focused run.

Milestone 3 validates the entire Rust owner and records durable evidence in
this plan, `rust/testport/receipts/util_stmtsummary_audit.md`, and
`rust/testport/TESTPORT_EXECPLAN.md`. Acceptance is 67 passing owner tests,
successful all-target compilation, scoped formatting, Ready lint, clean diff
hygiene, and one normal push after rebasing on the latest remote branch.

## Concrete Steps

Run all commands from the repository root. OpenSSL-dependent commands use the
bundled native dependency root:

    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stmtsummary --lib go_v1_ -- --nocapture --test-threads=1

    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-stmtsummary --lib --no-fail-fast --test-threads=1

    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-stmtsummary --all-targets

    rustfmt +nightly-2026-08-22 --edition 2021 --check rust/crates/tidb-stmtsummary/src/statement_summary.rs rust/crates/tidb-stmtsummary/src/evicted.rs rust/crates/tidb-stmtsummary/src/reader.rs

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint

    git diff --check

The focused run must report three passing tests after the fix. The full owner
run must report 67 passing tests. The all-target check, scoped formatter,
Ready lint, and diff check must exit successfully. Existing warnings emitted
from the separately owned v2 surface do not fail this v1 batch.

## Validation and Acceptance

Acceptance requires observable compile-contract parity: all 39 ordinary v1
results can be ignored under `#[deny(unused_must_use)]`, while the eight native
`Option` annotations remain. The three focused tests, complete owner suite,
and all-target check must pass. The patch must contain no Go, v2, Bazel, Cargo,
generated, fixture, or platform-specific edit. The publication must be one
package commit pushed normally after integrating the latest remote head.

## Idempotence and Recovery

All validation commands are read-only and repeatable. If the remote branch
advances, fetch the explicit branch ref and rebase the unpublished commit,
resolve only package-local conflicts, rerun the focused tests and diff check,
then push normally. Never force-push. Leave the user's untracked `.zcode/`
directory and unrelated stashes untouched.

## Outcomes & Retrospective

The runtime-neutral correction, focused proof, complete owner suite,
all-target compile, scoped formatting, Ready lint, and durable evidence are
complete. One package commit and normal remote publication remain before
moving to the next package.
