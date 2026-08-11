# Certify `pkg/util/nocopy` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB embeds `nocopy.NoCopy` in mutable Go structs so `go vet` reports accidental value copies. The Rust rewrite already has the native ownership equivalent, but the package has no pinned completion receipt and the Go package itself has no unit test. After this plan is complete, the two direct Go artifacts, static copy-prevention contract, zero value, no-op locker methods, Rust compile-fail proof, and build integration will be represented by one atomic package claim. No production change is expected unless the audit finds a mismatch.

## Progress

- [x] (2026-08-11 13:54Z) Fixed the two-file direct inventory at source commit `318e82bbb791bfc2c74ecbb4f89666e072e9803b`; confirmed there is no `doc.go`, Go test, fixture, generated artifact, benchmark, build tag, platform variant, or failpoint use.
- [x] (2026-08-11 13:54Z) Ran the unchanged Go package, a zero-value/locker probe, and an intentional-copy `go vet` probe; the package has no test files, the marker is zero-sized, and copylocks reported both assignments.
- [x] (2026-08-11 13:54Z) Reviewed the Rust implementation and ran its focused unit and compile-fail doctest; both pass and no code change is required.
- [x] (2026-08-11 13:54Z) Added the atomic semantic receipt and this living plan without touching production or test code.
- [x] (2026-08-11 14:02Z) Completed the pre-sync Ready profile; receipt, complete owning-crate tests, formatting, all-target Clippy, repository lint, and the Bazel decision all pass.
- [x] (2026-08-11 14:09Z) Rebased without conflict onto remote `14002169f05b8a6dca90d5130a27d2c6e9d79d07` and repeated the Go probes plus the complete Ready profile; every expected result holds.
- [ ] Publish the one-package commit linearly and verify the remote SHA.

## Surprises & Discoveries

- Observation: the source package intentionally has no Go unit test.
  Evidence: its direct inventory contains only `nocopy.go` and `BUILD.bazel`, and `go test -tags=intest,deadlock -count=1` reports `[no test files]`. Production behavior therefore requires source review plus static-tool evidence rather than invented Go tests.

- Observation: Go permits a value assignment and relies on `go vet` to reject it, while Rust rejects use after a move in the type system.
  Evidence: the intentional Go copy probe produces two `assignment copies lock value` diagnostics; the existing Rust `compile_fail` doctest moves `NoCopy` and then fails when it tries to use the original binding again.

- Observation: the synchronized branch removed the shared semantic-package gate.
  Evidence: commit `3353b29fb4aa697665ed38586fe5f50ef87fba6a` deleted the script and older receipts. This receipt can be validated with that script's last version read directly from Git without restoring the removed global mechanism.

## Decision Log

- Decision: Keep the existing zero-sized Rust marker with neither `Copy` nor `Clone`, and make no production change.
  Rationale: Rust ownership is the native compile-time equivalent of Go's vet convention. The existing `Default`, constructor, no-op methods, unit test, and compile-fail doctest already preserve every source-observable behavior without adding runtime state.
  Date/Author: 2026-08-11 / Codex

- Decision: Use a receipt-only package commit rather than changing a comment or renaming a test to manufacture source churn.
  Rationale: the user requires package-atomic review and evidence, not code changes where parity already holds. The receipt and plan are the missing package artifacts.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The direct inventory, source pin, consumer context, Go runtime/static-tool evidence, Rust implementation review, unit test, compile-fail doctest, receipt, and pre- and post-sync Ready validation are complete. Only a final remote fetch, linear push, and remote-SHA verification remain. Correctness risk is low because the marker has no state or runtime branch; compatibility risk is limited to Rust being stricter at compile time than Go without vet, which is the intended native mapping; performance risk is none because both markers are zero-sized.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/nocopy/BUILD.bazel` and `pkg/util/nocopy/nocopy.go` at `318e82bbb791bfc2c74ecbb4f89666e072e9803b`. `NoCopy` is an empty struct whose pointer implements `sync.Locker` through no-op `Lock` and `Unlock` methods. The methods exist so the standard copylocks analyzer treats a containing struct as unsafe to copy. `pkg/sessionctx/stmtctx/stmtctx.go` embeds the marker because copying `StatementContext` would leave a callback referring to the wrong warning context; that consumer is orientation evidence, not part of this package claim.

Rust owns the native marker in `rust/crates/tidb-util/src/nocopy/mod.rs`, exported by the unchanged `rust/crates/tidb-util/src/lib.rs`. `NoCopy` is a zero-sized unit struct with `Default` but no `Copy` or `Clone`. `new`, `lock`, and `unlock` are constant no-ops. The module unit test proves the zero value and methods, and its compile-fail doctest proves that moving the value prevents a second use.

## Milestones

The oracle milestone fixes the package rather than assuming an absent test means no contract. Verify the two direct bytes, run the package, execute a zero-value `sync.Locker` probe, and retain the expected `go vet` copylocks failure. Acceptance is `[no test files]`, `size=0`, and diagnostics naming both copies.

The parity milestone reviews the existing Rust representation against every production symbol and runs its unit and compile-fail doctest. Acceptance is one passing focused unit test, one passing compile-fail doctest, and a documented no-change decision.

The publication milestone adds only the receipt and plan, runs Ready validation for `tidb-util`, checks the Bazel trigger rules, and publishes one linear commit. Acceptance is a clean worktree with matching local and freshly fetched remote SHAs.

## Plan of Work

Do not alter `rust/crates/tidb-util/src/nocopy/mod.rs` unless source evidence identifies a mismatch. Add `rust/crates/tidb-util/tests/nocopy.semantic.toml` with the direct package/source pin, the existing Rust owner files, and separate unit/doctest commands. Maintain this ExecPlan with exact evidence and validation results.

Run complete `tidb-util` tests because the package's compile-fail documentation is part of the owning crate. Run formatting, all-target Clippy with warnings denied, and repository lint. The final diff adds no Go source, Bazel metadata, module dependency, Rust production code, or top-level Go test, so `make bazel_prepare` is not required unless a later rebase introduces a trigger.

## Concrete Steps

From `pkg/util/nocopy`, run the unchanged package without failpoints:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -tags=intest,deadlock -count=1

From `rust`, run focused and complete Rust gates:

    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib 'nocopy::tests'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --doc 'nocopy::NoCopy'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings

From repository root, validate the receipt and lint recipe:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/nocopy.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint

## Validation and Acceptance

Go must report no test files, while the external probe must show `size=0` and `*nocopy.NoCopy` as the locker. `go vet` on the intentional copy probe must fail with copylocks diagnostics; that nonzero result is expected evidence, not a package failure. Rust unit and compile-fail documentation tests must pass, proving zero size/no-op calls and prohibition of use after move.

The receipt must accept exactly two pinned Go artifacts and two focused commands. Complete `tidb-util` unit, integration, and doctest surfaces, formatting, all-target Clippy, and repository lint must pass. The final diff must contain only this receipt and plan. Publication must be one package commit based on the latest `hparser-integration`, pushed without force, with matching local and remote SHAs.

## Idempotence and Recovery

All validation commands are safe to rerun. The two probes live at `/tmp/tidb-nocopy-probe.go` and `/tmp/tidb-nocopy-copy-probe.go` and do not modify the repository. The copy probe is supposed to make `go vet` exit nonzero. If the phony `revive` install target cannot reach the Go proxy, reuse a locally verified `revive v1.2.1` binary, suppress only the install target with Make's `-o` option, and remove the ignored copy afterward. If the remote advances, rebase only the one local package commit and repeat Ready validation.

## Artifacts and Notes

Go package and runtime probe:

    ? github.com/pingcap/tidb/pkg/util/nocopy [no test files]
    size=0 locker=*nocopy.NoCopy

Expected `go vet` evidence:

    assignment copies lock value to copied: github.com/pingcap/tidb/pkg/util/nocopy.NoCopy
    assignment copies lock value to _: github.com/pingcap/tidb/pkg/util/nocopy.NoCopy

Focused Rust evidence:

    nocopy unit test: 1 passed; 0 failed
    nocopy compile-fail doctest: 1 passed; 0 failed

Pre-sync Ready evidence:

    semantic receipt: 1 package, 2 unique commands
    cargo fmt --all --check: exit 0
    tidb-util library tests: 336 passed; 1 helper ignored
    tidb-util integration tests and doctest: all passed
    cargo clippy -p tidb-util --all-targets -- -D warnings: exit 0
    repository lint recipe: exit 0 using a locally verified revive v1.2.1
    make bazel_prepare: not required; the diff adds only this receipt and plan

Post-sync Ready evidence on remote base `14002169f05b8a6dca90d5130a27d2c6e9d79d07`:

    Go package/runtime/copylocks probes: unchanged expected results
    semantic receipt: 1 package, 2 unique commands
    cargo fmt --all --check: exit 0
    tidb-util library tests: 336 passed; 1 helper ignored
    tidb-util integration tests and doctest: all passed
    cargo clippy -p tidb-util --all-targets -- -D warnings: exit 0
    repository lint recipe: exit 0
    make bazel_prepare: not required; the rebased diff still adds only evidence files

Failpoint decision:

    no failpoint., testfailpoint., or Bazel failpoint dependency match

## Interfaces and Dependencies

`rust/crates/tidb-util/src/nocopy/mod.rs` retains `pub struct NoCopy`, `pub const fn NoCopy::new() -> Self`, `pub const fn NoCopy::lock(&self)`, and `pub const fn NoCopy::unlock(&self)`. It implements `Debug` and `Default`, intentionally does not implement `Copy` or `Clone`, remains zero-sized, and adds no dependency. `rust/crates/tidb-util/src/lib.rs` continues to export `pub mod nocopy`.

Plan revision note: created after current-source inventory pinning, consumer/source review, failpoint inspection, the absent-test Go oracle, runtime and copylocks probes, Rust owner review, focused unit/doctest validation, and the no-code-change decision; updated after complete pre-sync validation, remote rebase, repeated post-sync probes and Ready gates, and the final Bazel decision.
