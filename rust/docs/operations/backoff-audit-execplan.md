# Complete `pkg/util/backoff` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

The Rust rewrite already implements the ordinary exponential-backoff sequence tested by Go, but its package claim omits the source type's usable zero value and does not retain evidence for signed durations, unusual floating-point multipliers, copied state, or the current Go package revision. After this plan is complete, `tidb-util::backoff` will represent the complete direct Go package at `pkg/util/backoff`, including its interface, production domains, original test vectors, build declaration, and one atomic receipt. A caller can construct, reset, copy, or trait-dispatch the Rust backoff and observe the same signed nanosecond sequence as Go.

## Progress

- [x] (2026-08-11 13:34Z) Fixed the three-file direct inventory at source commit `62d4284bee67e64f23b0a7b7f89488b26667c0eb`; confirmed there is no `doc.go`, support file, fixture, generated artifact, benchmark, build tag, platform variant, or failpoint use.
- [x] (2026-08-11 13:34Z) Ran the unchanged Go `TestExponential`, the external current-source boundary probe, and the existing three Rust tests; all existing behavior passed.
- [x] (2026-08-11 13:34Z) Added the zero-value/copy regression before the implementation change and retained its E0599 failure because `Exponential::default` did not exist.
- [x] (2026-08-11 13:34Z) Added the native zero-value implementation, current-source boundary vectors, and semantic receipt; all four focused Rust tests pass.
- [x] (2026-08-11 13:41Z) Completed the pre-sync Ready profile; the current-source Go oracle, receipt, final focused and complete Rust tests, formatting, all-target Clippy, repository lint, and Bazel decision all pass.
- [x] (2026-08-11 13:47Z) Rebased the one-package commit without conflict onto remote `7297489ad947363f6f579b3b4025d3793191d492` and repeated the Ready profile; every gate passed.
- [ ] Publish the one-package commit linearly and verify the remote SHA.

## Surprises & Discoveries

- Observation: the current package source pin is newer than the earlier release-8.5 audit evidence.
  Evidence: commit `62d4284bee67e64f23b0a7b7f89488b26667c0eb` modernized the update to Go's built-in `min`; the three direct files at that commit are byte-identical to the current branch.

- Observation: Go's single unit test covers only positive finite values even though every constructor input type has a wider production domain.
  Evidence: `TestExponential` contains 30 assertions over base `1`, multipliers `1` and `2`, and maxima `1` and `10`; the external probe additionally exercised signed durations, all floating-point classes, retry reset, copied values, the zero value, and a nil receiver.

- Observation: Rust's existing multiply-cast-min expression already matches the current Go toolchain at the floating-point boundaries.
  Evidence: both implementations returned zero for NaN, saturated infinities/out-of-range conversion, and applied the signed maximum only after conversion. The implementation correction is therefore only the missing zero-value construction.

- Observation: the synchronized branch removed the shared semantic-package gate.
  Evidence: commit `3353b29fb4aa697665ed38586fe5f50ef87fba6a` deleted the script and older receipts. The package receipt can still be checked with that script's last version read directly from Git without restoring the deleted global machinery.

## Decision Log

- Decision: Keep Go `time.Duration` as the existing `i64` nanosecond alias and Go `int` as `isize`.
  Rationale: `std::time::Duration` cannot represent source-negative values, while retry count uses the target-width integer domain and only zero has special meaning.
  Date/Author: 2026-08-11 / Codex

- Decision: Derive `Default` and retain `Clone` on `Exponential` rather than introducing constructors for these source behaviors.
  Rationale: all Go struct fields have scalar zero values and value copying creates independent state. Rust derives express those contracts exactly and add no runtime branch.
  Date/Author: 2026-08-11 / Codex

- Decision: Extend the existing in-module test surface and leave the original Rust test name/style intact.
  Rationale: the package is dependency-closed, has no fixtures, and local Rust naming conventions use snake case; a new integration harness or non-snake lint exemption would add no coverage.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

Inventory pinning, source/history review, the unchanged Go oracle, the external boundary probe, regression-first evidence, implementation, receipt, and pre- and post-sync Ready validation are complete. The package adds no dependency and changes no consumer; its correctness risk is limited to the explicitly tested target-language float conversion boundary, with no compatibility or performance regression in the ordinary finite domain. Only a final remote fetch, linear push, and remote-SHA verification remain.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/backoff/BUILD.bazel`, `pkg/util/backoff/backoff.go`, and `pkg/util/backoff/backoff_test.go` at `62d4284bee67e64f23b0a7b7f89488b26667c0eb`. `Backoffer` is the one-method interface. `Exponential` stores a base duration, floating-point multiplier, maximum duration, and next duration. Retry count zero restores the base; every other integer advances the stored value once by multiplying in `float64`, converting back to signed nanoseconds, and selecting the smaller result and maximum.

Rust owns this package in `rust/crates/tidb-util/src/backoff.rs`, exported by the unchanged `rust/crates/tidb-util/src/lib.rs`. It represents Go duration as `i64`, exposes the interface as the `Backoffer` trait, and owns the tests in the same module. `rust/crates/tidb-util/tests/backoff.semantic.toml` pins the accepted source and the focused Cargo command. There are no consumers or dependency changes required for this completion.

## Milestones

The first milestone fixes the oracle. Verify the direct package inventory and bytes at the pinned commit, run `TestExponential`, and run a repository-external probe for production domains that Go's test omits. Acceptance is the recorded current-source sequences in `Artifacts and Notes` with no repository change.

The second milestone closes the Rust behavior. Add the zero-value/copy regression first and retain its missing-`Default` failure, derive `Default`, preserve the existing arithmetic, and add source-derived vectors in the nearest unit-test module. Acceptance is four passing focused tests, including all 30 original Go assertions.

The final milestone certifies and publishes one package. Add the receipt, run the complete owning-crate Ready gates, decide Bazel preparation from the final diff, fetch and rebase if needed, and push exactly one non-force commit. Acceptance is a clean worktree whose HEAD and freshly fetched remote SHA are identical.

## Plan of Work

Keep the production edit in `rust/crates/tidb-util/src/backoff.rs`: derive `Default` for `Exponential` and document why the existing float-to-integer cast is intentional. In that file's existing test module, retain the original Go vectors, extend the reset test with the observed signed and floating-point cases, and prove zero-valued and cloned state are independent. Do not change Go files, Bazel metadata, dependencies, callers, or unrelated central plans.

Add the narrow semantic receipt and this living plan. Run focused tests, then complete `tidb-util` tests, formatting, all-target Clippy with warnings denied, and repository lint. The final diff has no Go/Bazel/module changes, so `make bazel_prepare` is not required unless synchronization introduces a trigger. Rebase only the single package commit if the remote advances.

## Concrete Steps

From `pkg/util/backoff`, run the unchanged Go oracle without failpoints:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^TestExponential$' -tags=intest,deadlock -count=1

From `rust`, run the focused and complete Rust gates:

    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib 'backoff::tests'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings

From repository root, validate the receipt and repository lint:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/backoff.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint

## Validation and Acceptance

The unchanged Go test must pass. Rust must return `[1,2,4,8,10,10,10,10,10,10]` for the source's exponential vector, reset on any later zero retry, and advance once for every nonzero retry including negatives. It must match the recorded signed, NaN, infinity, overflow, zero-value, and copied-state sequences. Trait-object dispatch must mutate the same object.

The semantic receipt must accept exactly the three pinned direct Go files and one focused Rust command. The complete `tidb-util` unit, integration, and doctest surface, formatting, all-target Clippy, and repository lint must pass. The final diff must change no Go source, Bazel file, Go module, or Rust dependency. Publication must be one package commit, rebased onto the latest `hparser-integration`, pushed without force, with matching local and remote SHAs.

## Idempotence and Recovery

All tests, format checks, Clippy, receipt checks, and lint commands are safe to rerun. The external probe lives at `/tmp/tidb-backoff-probe.go` and is not part of the repository. If the phony `revive` install target cannot reach the Go proxy, reuse a locally verified `revive v1.2.1` binary and suppress only that install target with Make's `-o` option; remove the ignored copied binary afterward. If the remote advances, rebase the single local commit and repeat the Ready profile before pushing.

## Artifacts and Notes

Initial Go oracle:

    PASS
    ok github.com/pingcap/tidb/pkg/util/backoff 0.913s

Current-source probe:

    reset=[3 6 12 3 6]
    negative-base=[-5 -2 -1 0]
    zero-multiplier=[5 0 0]
    negative-multiplier=[5 -10 20 -40]
    negative-max=[3 -1 -2]
    nan=[5 0 0]
    positive-inf=[5 100 100]
    negative-inf=[5 -9223372036854775808 100]
    positive-overflow=[9223372036854775807 9223372036854775807 9223372036854775807]
    negative-overflow=[-9223372036854775808 -9223372036854775808 -9223372036854775808]
    zero-value=[0 0 0]
    copy-original=6,18 copied=6,18
    nil-receiver-panics=true

Pre-fix regression evidence:

    error[E0599]: no associated function or constant named `default` found for struct `Exponential`
    error: could not compile `tidb-util` (lib test) due to 1 previous error

Focused post-fix evidence:

    test result: ok. 4 passed; 0 failed; 0 ignored; 333 filtered out

Pre-sync Ready evidence:

    direct Go oracle: 1 test containing 30 source assertions passed
    semantic receipt: 1 package, 1 unique command
    cargo fmt --all --check: exit 0
    tidb-util library tests: 336 passed; 1 helper ignored
    tidb-util integration tests and doctest: all passed
    cargo clippy -p tidb-util --all-targets -- -D warnings: exit 0
    repository lint recipe: exit 0 using a locally verified revive v1.2.1 after the phony network install target timed out earlier in this audit session
    make bazel_prepare: not required; no Go, Bazel, Go module, import, or Go test-function diff

Post-sync Ready evidence on remote base `7297489ad947363f6f579b3b4025d3793191d492`:

    direct Go oracle: passed in 0.802s
    semantic receipt: 1 package, 1 unique command
    cargo fmt --all --check: exit 0
    tidb-util library tests: 336 passed; 1 helper ignored
    tidb-util integration tests and doctest: all passed
    cargo clippy -p tidb-util --all-targets -- -D warnings: exit 0
    repository lint recipe: exit 0
    make bazel_prepare: not required; the rebased diff still contains no trigger

Failpoint decision:

    no failpoint., testfailpoint., or Bazel failpoint dependency match

## Interfaces and Dependencies

`rust/crates/tidb-util/src/backoff.rs` retains `pub type Duration = i64`, `pub trait Backoffer`, `pub struct Exponential`, `pub const fn new_exponential`, and `Exponential::backoff`. `Exponential` implements `Backoffer`, `Clone`, and `Default`; its fields remain private and no dependency is added. `rust/crates/tidb-util/src/lib.rs` continues to export the existing `pub mod backoff`.

Plan revision note: created after current-source inventory pinning, source/history review, failpoint inspection, the unchanged Go oracle, the external boundary probe, the existing Rust baseline, retained pre-fix failure, implementation, focused tests, and receipt creation; updated after negative finite overflow closure, full-crate validation, lint, Clippy, formatting, the Bazel gate, remote rebase, and repeated post-sync Ready validation.
