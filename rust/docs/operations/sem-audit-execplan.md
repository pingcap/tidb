# Complete `pkg/util/sem` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

Security Enhanced Mode (SEM) hides sensitive schemas, tables, variables, and restricted privileges. The Rust rewrite already copies those predicate lists, but its `enable` and `disable` functions intentionally omit three Go effects: changing the process defaults for `tidb_enable_enhanced_security` and `hostname`, restoring the operating-system hostname on disable, and emitting the enable notice. Consequently a new Rust session can report SEM as disabled while the process flag says it is enabled.

After this plan is complete, the direct Go package at `pkg/util/sem` will have one atomic Rust claim. Calling `tidb_util::sem::enable` will make newly constructed sessions report `tidb_enable_enhanced_security=ON` and `hostname=localhost`; calling `disable` will restore `OFF` and the operating-system hostname when it can be read. All source predicate tests remain aligned, the enable notice uses the existing background logger, and one semantic receipt plus one commit represents the complete package.

## Progress

- [x] (2026-08-11 12:37Z) Fixed the four-file direct inventory at source commit `69f71e4ab14ab9ff9099fc5cf332734fde22ba6a`; confirmed there is no `doc.go`, no later direct-package change, and no failpoint use.
- [x] (2026-08-11 12:37Z) Ran all five unchanged direct Go tests; they passed.
- [x] (2026-08-11 12:37Z) Ran the existing six Rust SEM tests; they passed while the module still declared the package in progress.
- [x] (2026-08-11 12:44Z) Added the source-derived session-default regression; before the fix it failed because a session created after `enable` still read `OFF` instead of `ON`.
- [x] (2026-08-11 12:47Z) Implemented the missing default-value and logging behavior without creating a crate dependency cycle; both focused regressions and all six direct Rust SEM tests pass.
- [x] (2026-08-11 12:53Z) Added the semantic receipt and completed pre-sync Ready validation; one unrelated session test failure was reproduced unchanged on the remote base and every remaining test passed.
- [x] (2026-08-11 13:20Z) Rebased the one-package commit without conflict onto remote `7c3dc1398cb7f0261dccbbdfe33a99a5739bdf8c` and repeated the Ready profile; the same unrelated session failure reproduced on that exact clean base and every remaining gate passed.
- [ ] Publish the one-package commit linearly and verify the remote SHA.

## Surprises & Discoveries

- Observation: the Rust predicate inventory tracks the current Go source rather than the older January 2025 source snapshot used by several unchanged utility packages.
  Evidence: direct Go commits through `69f71e4ab14ab9ff9099fc5cf332734fde22ba6a` add memory variables, remove `tidb_slow_txn_log_threshold`, and add the uppercase assertion; the current Rust constants reflect those changes.

- Observation: Go's direct unit tests cover every predicate but never call `Enable`, `Disable`, or `IsEnabled`.
  Evidence: `pkg/util/sem/sem_test.go` contains exactly five top-level tests, while both omitted functions mutate the shared sysvar registry in production source.

- Observation: Rust sessions do not read a mutable process-default layer today.
  Evidence: `SessionVars::get_system`, `GlobalSysvars::get`, and the read-only-variable binder fall directly back to the immutable `SysVarDef.value` captured at build time.

- Observation: the existing Rust schema predicate used ASCII-only folding, but Go `strings.EqualFold` accepts Unicode simple-fold equivalents of ASCII letters.
  Evidence: a Go probe returned true for `IsInvisibleSchema("metricſ_schema")`; the added Rust vector failed before the fix because long-s was not folded to `s`.

- Observation: the complete `tidb-session` run has one pre-existing partition errno failure unrelated to SEM.
  Evidence: `tests_partition::the_ported_rejections_carry_tidbs_own_errno` returned 1064 instead of 1504 both in this worktree and in the unmodified remote-base worktree at `60a7451396a440048c16c9a8bb7976bfd91fb182`; the exact test has no failpoint dependency.

- Observation: installed Windows and FreeBSD Rust targets are insufficient for cross-checking this crate on the macOS host.
  Evidence: both target checks stopped in the unchanged `ring` dependency because the target C sysroot lacked `assert.h`, before compiling `tidb-util`; native compilation and all-target clippy pass.

- Observation: the synchronized remote base no longer contains the shared semantic-package gate script.
  Evidence: commit `3353b29fb4aa697665ed38586fe5f50ef87fba6a` deleted the script and the pre-existing receipts. This package's atomic receipt was validated with that script's last version read directly from the Git object, without restoring the removed global machinery to the branch.

## Decision Log

- Decision: Keep SEM state and the two SEM-controlled defaults in `tidb-util::sem`, then have `tidb-session` consult that package-owned effective-default function at every registry fallback.
  Rationale: `tidb-session` already depends on `tidb-util`, so this preserves one-way ownership and makes the state observable through real session reads. Keeping a private shadow map in `sem` would have no consumer, while making `tidb-util` depend on `tidb-session` would create a crate cycle. A new crate is unnecessary for two values.
  Date/Author: 2026-08-11 / Codex

- Decision: Reuse `tidb-util::logutil::bg_logger` for the enable notice and obtain the host name from the operating system rather than from an environment variable.
  Rationale: Go calls `logutil.BgLogger().Info` and `os.Hostname`; the Rust workspace already owns equivalent logging and OS APIs, so an environment-only approximation would invent different behavior.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

Implementation and post-sync Ready validation are complete, but publication is not. The direct source and test inventory, source revision, failpoint decision, Go oracle, two pre-fix failures, semantic receipt, focused regressions, full owning-crate coverage, formatting, clippy, lint, Bazel decision, and exact remote-base failure reproduction are fixed. Only a final remote fetch, linear push, and remote-SHA verification remain.

## Context and Orientation

The accepted direct Go package consists of `pkg/util/sem/BUILD.bazel`, `pkg/util/sem/main_test.go`, `pkg/util/sem/sem.go`, and `pkg/util/sem/sem_test.go` at `69f71e4ab14ab9ff9099fc5cf332734fde22ba6a`. The subdirectories `pkg/util/sem/compat` and `pkg/util/sem/v2` are separate Go packages and are outside this claim.

Go stores the SEM flag in an atomic integer. `Enable` sets the flag, changes the global sysvar definitions for `tidb_enable_enhanced_security` to `ON` and `hostname` to `localhost`, then writes an informational log. `Disable` clears the flag, changes enhanced security to `OFF`, and changes `hostname` to `os.Hostname()` only when that lookup succeeds. The remaining functions are pure string predicates.

Rust owns those predicates and the atomic flag in `rust/crates/tidb-util/src/sem.rs`. It owns the background logger in `rust/crates/tidb-util/src/logutil/mod.rs`. The effective SQL-variable read paths live in `rust/crates/tidb-session/src/vars.rs` and `rust/crates/tidb-session/src/variables.rs`; their immutable definitions live in `rust/crates/tidb-session/src/sysvar.rs`. `tidb-session` depends on `tidb-util`, so it can consume a package-owned default override without adding a dependency.

## Plan of Work

First extend the existing `vars.rs` test module with one serial regression. It will restore SEM state on exit, call `enable`, construct a new `SessionVars`, and assert the two source defaults. It will call `disable`, construct another new session, and assert `OFF` plus the operating-system hostname when available. Run only that exact test before implementation and retain its failure showing that the session still returns the immutable `OFF` default after `enable`.

Next replace the in-progress note in `sem.rs` with the complete contract. Store the two current defaults in nonpoisoning synchronized state, expose an effective-default lookup for `tidb-session`, and update `enable`/`disable` in Go source order. Use the existing background logger for the exact Go message. Keep the public predicate API unchanged and retain the existing uppercase assertion behavior under assertion-enabled builds.

Then add one `sysvar::effective_default` helper and route every direct fallback from `SysVarDef.value` through it, including `SessionVars`, `GlobalSysvars`, `SET ... DEFAULT`, and scope-none expression reads. This is integration for the same Go package, not a claim on `pkg/sessionctx/variable`; no unrelated sysvar behavior changes.

Finally add `rust/crates/tidb-util/tests/sem.semantic.toml`, run focused and complete tests for `tidb-util` and `tidb-session`, format, all-target clippy, and repository lint. The final diff changes no Go or Bazel file, so `make bazel_prepare` is not required. Fetch the latest `hparser-integration`, rebase the one package commit if needed, repeat validation, push without force, and verify the remote SHA.

## Concrete Steps

Run the unchanged Go oracle from `pkg/util/sem`:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^(TestInvisibleSchema|TestIsInvisibleTable|TestIsRestrictedPrivilege|TestIsInvisibleStatusVar|TestIsInvisibleSysVar)$' -tags=intest,deadlock -count=1

Run the focused Rust baseline and regression from `rust`:

    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib 'sem::tests'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-session --lib 'vars::tests::sem_enable_and_disable_change_new_session_defaults' -- --exact

Run the semantic receipt and Ready gates from repository root or `rust` as appropriate:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/sem.semantic.toml
    cd rust && cargo fmt --all --check
    cd rust && CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util -p tidb-session
    cd rust && CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util -p tidb-session -- --skip 'tests_partition::the_ported_rejections_carry_tidbs_own_errno'
    cd rust && CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util -p tidb-session --all-targets -- -D warnings
    make lint

## Validation and Acceptance

The unchanged Go tests must pass. The new Rust regression must fail before the fix because a session created after `enable` reads `OFF`, then pass after the fix with `ON` and `localhost`. After `disable`, the mode must be false, enhanced security must be `OFF`, and the default hostname must equal the operating-system hostname when the lookup succeeds. Existing SEM predicate tests must continue to pass, including exact case sensitivity and the complete invisible-variable set.

The semantic receipt must accept exactly `pkg/util/sem` at its pinned source and run focused tests in both owning crates. Because the shared gate was removed from the synchronized branch, its last tracked version may be executed directly from Git for this receipt without restoring the deleted global mechanism. Complete `tidb-util` and `tidb-session` tests must introduce no new failure: the one partition errno failure must reproduce identically on the clean remote base, and a run excluding only that test must pass. Formatting, all-target clippy, and the repository lint recipe must pass. The final diff must contain no Go source, accepted Go package, Bazel, or Go module edits. Publication must be a single package commit based on the latest remote `hparser-integration`, pushed linearly without force, with matching local and remote SHAs.

## Idempotence and Recovery

All Go, Cargo, semantic-gate, formatting, clippy, and lint commands are safe to rerun. The regression restores SEM mode/default state even after an assertion failure. If the shared Cargo target runs out of space, delete only `/tmp/tidb-package-audit.DnxFlT/rust/target/debug/incremental` by exact path and retry. If `make lint` creates `tools/bin/revive` or `tools/bin/failpoint-ctl`, unlink only those confirmed files. If the remote advances, rebase only the local one-package commit and repeat every post-rebase gate.

## Artifacts and Notes

Initial Go oracle:

    PASS
    ok github.com/pingcap/tidb/pkg/util/sem 0.464s

Initial Rust baseline:

    running 6 tests
    test result: ok. 6 passed; 0 failed; 0 ignored; 330 filtered out

Pre-fix regression evidence:

    vars::tests::sem_enable_and_disable_change_new_session_defaults ... FAILED
    assertion left == right failed: left "OFF", right "ON"
    test result: FAILED. 0 passed; 1 failed; 0 ignored; 1022 filtered out

Unicode-fold regression evidence:

    Go IsInvisibleSchema("metricſ_schema"): true
    sem::tests::invisible_schema ... FAILED
    test result: FAILED. 0 passed; 1 failed; 0 ignored; 335 filtered out

Focused post-fix evidence:

    tidb-util sem tests: 6 passed; 0 failed
    tidb-session SEM default regression: 1 passed; 0 failed
    enable emitted: tidb-server is operating with security enhanced mode (SEM) enabled

Pre-sync Ready evidence:

    semantic package gate: 1 packages, 2 unique commands
    cargo fmt --all --check: exit 0
    tidb-session complete run: 1013 passed; 1 failed; 9 ignored
    clean remote-base exact run: same partition failure, 1064 instead of 1504
    tidb-session excluding that one baseline failure: 1013 passed; 9 ignored
    tidb-util library tests: 335 passed; 1 helper ignored
    tidb-util integration tests and both crates' doctests: all passed
    cargo clippy -p tidb-util -p tidb-session --all-targets -- -D warnings: exit 0
    make lint: exit 0
    Windows/FreeBSD target checks: not reached; unchanged ring C build lacks target assert.h
    make bazel_prepare: not required; no Go, Bazel, Go module, import, or Go test-function diff

Post-sync Ready evidence on remote base `7c3dc1398cb7f0261dccbbdfe33a99a5739bdf8c`:

    direct Go oracle: 5 passed
    semantic receipt: 1 package, 2 unique commands
    cargo fmt --all --check: exit 0
    tidb-session complete run: 1014 passed; 1 failed; 9 ignored
    clean synchronized-base exact run: same partition failure, 1064 instead of 1504
    tidb-session excluding that one baseline failure: 1014 passed; 9 ignored
    tidb-util library tests: 335 passed; 1 helper ignored
    tidb-util integration tests and both crates' doctests: all passed
    cargo clippy -p tidb-util -p tidb-session --all-targets -- -D warnings: exit 0
    repository lint recipe: exit 0; a validated revive v1.2.1 binary was used after the phony install target timed out against proxy.golang.org
    make bazel_prepare: not required; no Go, Bazel, Go module, import, or Go test-function diff

Failpoint decision:

    no failpoint., testfailpoint., or Bazel failpoint dependency match

The direct inventory is:

    pkg/util/sem/BUILD.bazel
    pkg/util/sem/main_test.go
    pkg/util/sem/sem.go
    pkg/util/sem/sem_test.go

## Interfaces and Dependencies

`rust/crates/tidb-util/src/sem.rs` will retain `enable`, `disable`, `is_enabled`, and every predicate. It will add a narrow package-integration function that returns an owned effective default only for `hostname` and `tidb_enable_enhanced_security`; all other names return `None`.

`rust/crates/tidb-session/src/sysvar.rs` will add one helper that accepts `&SysVarDef` and returns the SEM override or the captured `SysVarDef.value`. `vars.rs` and `variables.rs` will use it wherever the current code falls directly back to the static field.

Plan revision note: created after inventory pinning, direct source/history review, dependency-boundary analysis, failpoint inspection, the unchanged Go oracle, and the existing Rust baseline; updated after retaining the pre-fix integration and Unicode-fold failures, implementing both fixes, passing focused and semantic gates, triaging the unrelated partition baseline failure, completing pre-sync Ready validation, rebasing onto the latest remote, and repeating Ready validation against that synchronized base.
