# Certify `pkg/util/israce` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB exposes whether a binary was built with Go's race detector so diagnostic output can identify instrumented builds. The source package implements that behavior with two mutually exclusive build-tagged files. The Rust rewrite already maps those variants to a Cargo feature, but the current branch has no durable package receipt. After this plan is complete, all Go production and build inputs, both observable Go variants, the Rust feature declaration, owner module, tests, and integration decision will form one atomic package claim.

## Progress

- [x] (2026-08-11 14:36Z) Fixed the complete three-file source inventory at commit `318e82bbb791bfc2c74ecbb4f89666e072e9803b`; current Go bytes match that package-local source commit.
- [x] (2026-08-11 14:36Z) Confirmed the package has no `doc.go`, Go test, `TestMain`, benchmark, fuzz target, example, fixture, generated input, platform file beyond the two build-tag variants, or failpoint use.
- [x] (2026-08-11 14:36Z) Compiled both Go variants and observed `RaceEnabled=false` without `-race` and `RaceEnabled=true` with `-race` on `darwin/arm64`.
- [x] (2026-08-11 14:36Z) Reviewed the Rust feature mapping and passed focused tests with the default feature set and with `--features race`; no production or test change is required.
- [x] (2026-08-11 14:36Z) Added the current atomic semantic receipt and this living plan.
- [x] (2026-08-11 14:40Z) Completed the pre-sync Ready profile: the dual-command receipt, formatting, complete `tidb-util` suite, both all-target Clippy variants, and repository lint all pass; the Bazel prepare gate has no trigger.
- [x] (2026-08-11 14:51Z) Rebasing after a remote advance produced base `99a8379272bc7c8aa0c1653c42a2aaa32cbe885e`; the complete post-sync Ready profile passed again.
- [ ] Publish the linear commit without force and verify the freshly fetched remote SHA.

## Surprises & Discoveries

- Observation: Go provides no unit test for this package.
  Evidence: the source inventory contains only `BUILD.bazel`, `israce.go`, and `norace.go`; both package test commands report `[no test files]`. Structured `go list` and an external executable probe therefore provide the direct behavioral oracle without adding a test to accepted Go source.

- Observation: the build-tag choice is automatic in Go but explicit in stable Rust.
  Evidence: `go list` selects `norace.go` normally and `israce.go` under `-race`. Stable Rust has no portable predicate that this workspace can use to detect ThreadSanitizer, so `tidb-util/race` is the source-equivalent build policy flag. It reports configuration and does not itself enable instrumentation.

- Observation: an older completion receipt existed in commit `14ec70e9374f6156a2c5711ee293cb2e79166f56` but was later removed with the shared receipt mechanism.
  Evidence: the old artifact used the retired v3 schema and an older repository pin. This audit treats it only as seed evidence and independently rechecks current source bytes, both build variants, and the current gate format.

- Observation: `hparser-integration` advanced during the publication gate.
  Evidence: a fresh fetch moved the publish base from `5cc08d624e3fd92bbd57f96ad14ac52b660b99ac` to merge commit `99a8379272bc7c8aa0c1653c42a2aaa32cbe885e`. That range did not overlap the `israce` owner or audit artifacts; after rebasing the single package commit, all post-sync Ready checks passed again.

## Decision Log

- Decision: Preserve `RACE_ENABLED = cfg!(feature = "race")` and the two existing conditional tests.
  Rationale: the default and feature-enabled builds yield the same false/true table as Go's `!race` and `race` files. There is no portable stable-Rust sanitizer predicate that would improve the mapping, and the module accurately names this a source-equivalent configuration.
  Date/Author: 2026-08-11 / Codex

- Decision: Publish a receipt-only package commit.
  Rationale: all production behavior and both variant tests already align. The missing current artifacts are the package pin, complete inventory, explicit dual-build commands, integration decision, and validation record.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The direct inventory, source pin, build-tag selection, observable Go false/true table, Rust owner review, both focused Rust variants, semantic receipt, one-package commit, synchronization, and Ready validation before and after the latest rebase are complete. The non-force push and remote-SHA verification remain. Correctness risk is limited to a future build pipeline enabling ThreadSanitizer without the matching Cargo feature; compatibility risk is none because no API changes; performance risk is none because the value is a compile-time constant.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/israce/BUILD.bazel`, `pkg/util/israce/israce.go`, and `pkg/util/israce/norace.go` at `318e82bbb791bfc2c74ecbb4f89666e072e9803b`. The `race` build tag selects a single constant `RaceEnabled = true`; the complementary `!race` tag selects `false`. Go's `-race` option both enables instrumentation and defines the `race` build tag.

Rust owns this API in `rust/crates/tidb-util/src/israce/mod.rs`, exported unchanged by `rust/crates/tidb-util/src/lib.rs`. `rust/crates/tidb-util/Cargo.toml` declares the empty `race` feature that chooses the boolean at compile time. No Rust caller currently consumes `RACE_ENABLED`; keeping the public mapping is the package integration decision until a diagnostic consumer is ported.

## Milestones

The source-oracle milestone fixes every accepted byte and proves both source variants. Run structured Go package selection, compile both forms, and execute a repository-external probe. Acceptance is exactly `norace.go` plus `false` normally and `israce.go` plus `true` under `-race`.

The parity milestone reviews the feature declaration, owner module, export, and consumers. Run each focused Rust test in the build where it exists. Acceptance is one passing default test and one passing `race`-feature test, with no production mismatch.

The publication milestone adds only the receipt and this plan, runs the complete Ready profile for `tidb-util`, synchronizes one commit to the latest `hparser-integration`, and pushes without force. Acceptance is matching local and freshly fetched remote SHAs.

## Plan of Work

Do not alter `rust/crates/tidb-util/src/israce/mod.rs`, `rust/crates/tidb-util/src/lib.rs`, or `rust/crates/tidb-util/Cargo.toml` unless a source probe or focused test differs. Add `rust/crates/tidb-util/tests/israce.semantic.toml` with the current package pin, all three Rust ownership inputs, and two focused commands. Maintain this plan with exact pre-sync and post-sync validation evidence.

Run the complete default `tidb-util` unit, integration, and documentation suite, formatting, all-target Clippy with warnings denied, the `race`-feature Clippy variant, and repository lint. The diff adds no Go, Bazel, module, Rust manifest, or Rust production change, so `make bazel_prepare` is not required unless synchronization introduces a trigger into the one-package diff.

## Concrete Steps

From repository root, verify Go file selection and compilation without failpoints:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go list -f 'default GoFiles={{.GoFiles}} IgnoredGoFiles={{.IgnoredGoFiles}} TestGoFiles={{.TestGoFiles}} XTestGoFiles={{.XTestGoFiles}}' ./pkg/util/israce
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go list -race -f 'race GoFiles={{.GoFiles}} IgnoredGoFiles={{.IgnoredGoFiles}} TestGoFiles={{.TestGoFiles}} XTestGoFiles={{.XTestGoFiles}}' ./pkg/util/israce
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -tags=intest,deadlock -count=1 ./pkg/util/israce
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -tags=intest,deadlock -count=1 ./pkg/util/israce

Use `/tmp/tidb-israce-probe.go`, which imports `pkg/util/israce` and prints `RaceEnabled`, for the direct observations:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go run /tmp/tidb-israce-probe.go
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go run -race /tmp/tidb-israce-probe.go

From `rust`, run focused and Ready Rust gates:

    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib 'israce::tests'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib --features race 'israce::tests'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets --features race -- -D warnings

From repository root, validate the receipt and lint recipe:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/israce.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint

## Validation and Acceptance

The default Go selection must be `GoFiles=[norace.go]`, `IgnoredGoFiles=[israce.go]`, and output `false`. The `-race` selection must reverse the files and output `true`. Both lists must have no test files. The two focused Rust builds must each run exactly the matching conditional test and pass.

The receipt must accept exactly one pinned Go package and two unique commands. Complete owning-crate tests, formatting, default and feature-enabled all-target Clippy, and repository lint must pass. The final diff must contain only this receipt and plan. Publication must be one package commit based on the latest fetched `hparser-integration`, pushed without force, with matching local and remote SHAs.

## Idempotence and Recovery

All checks are safe to rerun. The Go probe lives under `/tmp` and never enters the repository. If `go run -race` is unavailable on a future host, retain the structured `go list -race` and compile evidence, but record the missing executable observation rather than hiding it. If the phony `revive` install cannot reach the Go proxy, reuse a verified `revive v1.2.1`, suppress only that install target with Make's `-o`, and remove the ignored copy afterward. If the remote advances, rebase the one local commit and repeat Ready validation.

## Artifacts and Notes

Go source evidence on Go 1.25.10 `darwin/arm64` with CGO enabled:

    default GoFiles=[norace.go] IgnoredGoFiles=[israce.go] TestGoFiles=[] XTestGoFiles=[]
    race GoFiles=[israce.go] IgnoredGoFiles=[norace.go] TestGoFiles=[] XTestGoFiles=[]
    default probe: false
    race probe: true
    both package tests: pass; no test files

Focused Rust evidence:

    default_build_reports_race_disabled: 1 passed; 0 failed
    race_build_reports_race_enabled: 1 passed; 0 failed

Pre-sync Ready evidence:

    semantic receipt: 1 package, 2 unique commands
    cargo fmt --all --check: pass
    complete tidb-util suite: 336 passed, 0 failed, 1 ignored; integration tests and doctest pass
    default cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    race-feature cargo clippy -p tidb-util --all-targets --features race -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; no Go, Bazel, module, target-list, or Rust production trigger

Post-sync Ready evidence on remote base `99a8379272bc7c8aa0c1653c42a2aaa32cbe885e`:

    default and -race Go package builds: pass; no test files
    default and -race Go probes: false, true
    semantic receipt: 1 package, 2 unique commands
    cargo fmt --all --check: pass
    complete tidb-util suite: 337 passed, 0 failed, 1 ignored; integration tests and doctest pass
    default and race-feature all-target Clippy with warnings denied: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; the one-package diff adds only the receipt and plan

Failpoint decision:

    no failpoint., testfailpoint., or Bazel failpoint dependency match

## Interfaces and Dependencies

`rust/crates/tidb-util/src/israce/mod.rs` retains the public constant `RACE_ENABLED: bool`. `rust/crates/tidb-util/Cargo.toml` retains `race = []`, and `rust/crates/tidb-util/src/lib.rs` retains `pub mod israce`. The package adds no runtime state or dependency. The Cargo feature is a build-policy signal; sanitizer build automation must enable it together with instrumentation.

Plan revision note: created after current-source inventory pinning, package-history review, failpoint inspection, both Go build selections and probes, Rust ownership/consumer review, both focused Rust variants, and the no-code-change decision. Updated after the publication base advanced to `99a8379272bc7c8aa0c1653c42a2aaa32cbe885e` and the full post-rebase Ready profile passed.
