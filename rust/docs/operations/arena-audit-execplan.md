# Certify `pkg/util/arena` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's byte arena reduces allocations by handing callers slices of one reusable backing buffer. The Rust rewrite already preserves every behavior asserted by the two Go unit tests and adds source-derived boundary coverage, but the current branch has no durable package receipt after the shared semantic gate was retired. After this plan is complete, all Go production, test, harness, and build artifacts, the Rust owner and export, the explicit safe-Rust integration difference, and the focused test command will form one atomic package claim.

## Progress

- [x] (2026-08-11 15:02Z) Fixed the complete four-file source inventory at commit `a7ef9150527f153af4777ecdcc8de2e315fdd946`; current package bytes match that pin.
- [x] (2026-08-11 15:02Z) Confirmed the package has no `doc.go`, benchmark, fuzz target, example, fixture, testdata, build tag, platform variant, generated input, `go:generate`, `go:embed`, or failpoint use.
- [x] (2026-08-11 15:02Z) Passed both Go source tests and independently measured Reset reuse, allocate-before-panic order, and negative-input boundaries through the public API.
- [x] (2026-08-11 15:02Z) Reviewed the Rust owner, export, and consumer surface; all 18 Go assertions map to the two source-named Rust tests, all six focused Rust tests pass, and no production or test change is required.
- [x] (2026-08-11 15:02Z) Added the current atomic semantic receipt and this living plan.
- [x] (2026-08-11 15:11Z) Completed the pre-sync Ready profile: receipt, formatting, complete `tidb-util` suite, all-target Clippy, and repository lint pass; the Bazel prepare gate has no trigger.
- [x] (2026-08-11 15:17Z) Confirmed fetched remote base `641b4bb4792575acd164bdcf5a7fd2318c2cabb4` and repeated the complete post-sync Ready profile successfully.
- [ ] Publish the linear commit without force and verify the freshly fetched remote SHA.

## Surprises & Discoveries

- Observation: the Rust owner deliberately does not implement the Go arena's shared mutable backing.
  Evidence: the public Go probe writes `[7, 8]`, resets the allocator, and reads `[7, 8]` from the next fitting allocation. Rust returns an owned `Vec<u8>` and its corresponding test reads zeroed bytes. Preserving aliasing with the same return type would require invalid ownership or unsafe code, while the workspace sets `unsafe_code = "forbid"`.

- Observation: `AllocWithLen` mutates allocator state before rejecting an invalid length.
  Evidence: after seeding bytes `[1, 2, 3, 4]`, resetting, and calling `AllocWithLen(3, 2)`, the Go call panics and the next two-byte allocation reads `[3, 4]`. The Rust regression checks the same offset advance before its panic.

- Observation: Go's signed input failures are outside the Rust API domain.
  Evidence: both `NewAllocator(-1)` and `StdAllocator.Alloc(-1)` panic in Go. Rust accepts `usize`, so negative values are unrepresentable rather than a synthetic runtime branch.

- Observation: `main_test.go` is package test-harness policy rather than allocator behavior.
  Evidence: it installs TiDB common test setup and four Go process goleak exclusions. The Rust tests have no equivalent background goroutines, so the artifact is included in the source pin while its runtime mechanics are an explicit no-port integration decision.

- Observation: an older whole-package receipt existed with source pin `fb8490526c5e30408fd0a444057b5b2e7072ad61` and a retired schema.
  Evidence: the old audit did find and fix the invalid-length mismatch, but the current review independently checks present source bytes, all Go assertions, the public probe, current Rust code, and the current receipt format. Historical artifacts are seed evidence only.

## Decision Log

- Decision: Keep the existing owned-`Vec<u8>` API and document shared backing as an integration difference.
  Rationale: replacing it with a lifetime-bound buffer could reproduce reuse safely, but that is a public API redesign with no current Rust consumer to validate. The current API preserves every source-unit-test observation and explicitly avoids claiming Go aliasing and allocation reuse.
  Date/Author: 2026-08-11 / Codex

- Decision: Preserve all six existing Rust tests without adding duplicate assertions.
  Rationale: `simple_arena_allocator` and `std_allocator` reproduce all 18 assertions from `arena_test.go`. Four additional tests already cover strict exact-fit fallback, Reset state, invalid length and capacity ordering, standard allocator panic behavior, and the owned-buffer difference.
  Date/Author: 2026-08-11 / Codex

- Decision: Treat `a7ef9150527f153af4777ecdcc8de2e315fdd946` as the accepted Go package pin.
  Rationale: it is the last commit that touched any direct package artifact, contains all four current files, and every current byte matches it. The receipt gate independently rejects inventory or byte drift.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

Inventory, Go source tests, boundary probes, Rust owner review, exact assertion mapping, focused Rust validation, the receipt, the package integration decision, synchronization, and Ready validation before and after the fetched base are complete. Non-force publication and remote-SHA verification remain. Correctness risk is limited to a future Rust consumer expecting the Go allocator's shared backing; compatibility and performance impact are none in this receipt-only change.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/arena/BUILD.bazel`, `pkg/util/arena/arena.go`, `pkg/util/arena/arena_test.go`, and `pkg/util/arena/main_test.go` at `a7ef9150527f153af4777ecdcc8de2e315fdd946`. `arena.go` defines `Allocator`, `SimpleAllocator`, the unexported `stdAllocator`, the exported `StdAllocator` singleton, and `NewAllocator`. A fitting `SimpleAllocator.Alloc` request uses a strict `<` comparison, returns a zero-length slice of the requested capacity from the backing arena, and advances `off`; fallback allocation leaves `off` unchanged. `Reset` only resets `off` and does not clear bytes.

`arena_test.go` has two top-level tests. `TestSimpleArenaAllocator` contains 14 assertions covering fitting and fallback allocations, length, capacity, offset movement, `AllocWithLen`, Reset, and retained arena capacity. `TestStdAllocator` contains four assertions covering length and capacity. `main_test.go` supplies `TestMain`, common TiDB test setup, and four goleak ignores. `BUILD.bazel` owns the library and flaky short test target.

Rust owns the mapping in `rust/crates/tidb-util/src/arena.rs` and exports it through `rust/crates/tidb-util/src/lib.rs`. `simple_arena_allocator` and `std_allocator` map the source tests assertion for assertion. The four other unit tests cover source boundaries and the safe owned-buffer choice. No Rust file outside the owner imports these arena symbols.

## Milestones

The source-oracle milestone fixes every accepted file and executes both Go tests without failpoints. A public API probe measures the important source branches not asserted by those tests. Acceptance is two passing Go tests, reset reuse `[7 8]`, an invalid-length panic followed by `[3 4]`, and negative-input panics.

The parity milestone reviews every Go test assertion against named Rust tests, then runs only `arena::tests`. Acceptance is six passing tests, including the two direct source-test mappings, with no uncovered source assertion and no hidden claim that owned Rust buffers alias the arena.

The publication milestone adds only the receipt and this plan, runs the complete Ready profile for `tidb-util`, synchronizes one commit to the latest `hparser-integration`, and pushes without force. Acceptance is matching local and freshly fetched remote SHAs.

## Plan of Work

Do not alter `rust/crates/tidb-util/src/arena.rs` or `rust/crates/tidb-util/src/lib.rs` unless a source test, probe, or focused test differs. Add `rust/crates/tidb-util/tests/arena.semantic.toml` with the current package pin, both Rust ownership inputs, and the focused test filter. Maintain this plan with exact pre-sync and post-sync validation evidence.

Run the complete `tidb-util` unit, integration, and documentation suite, formatting, all-target Clippy with warnings denied, and repository lint. The diff adds no Go, Bazel, module, Rust manifest, Rust production, or Rust test code, so `make bazel_prepare` is not required unless synchronization introduces a trigger into the one-package diff.

## Concrete Steps

From repository root, run the Go source tests without failpoints:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^(TestSimpleArenaAllocator|TestStdAllocator)$' -tags=intest,deadlock -count=1 ./pkg/util/arena

Use `/tmp/tidb-arena-probe.go`, which imports the public Go package, for direct boundary observations:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go run /tmp/tidb-arena-probe.go

From `rust`, run focused and Ready Rust gates:

    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib 'arena::tests'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings

From repository root, validate the receipt and lint recipe:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/arena.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint

## Validation and Acceptance

The Go package must list only `arena.go` as production and `arena_test.go` plus `main_test.go` as tests. `go test -list .` must list exactly `TestSimpleArenaAllocator` and `TestStdAllocator`. Both must pass. The Rust focused command must pass exactly the six tests under `arena::tests`, including the two source-named mappings.

The receipt must accept exactly one pinned Go package and one unique focused command. Complete owning-crate tests, formatting, all-target Clippy, and repository lint must pass. The final diff must contain only this receipt and plan. Publication must be one package commit based on the latest fetched `hparser-integration`, pushed without force, with matching local and remote SHAs.

## Idempotence and Recovery

All checks are safe to rerun. The Go probe lives under `/tmp` and never enters the repository. If a future Rust consumer needs allocation reuse, stop treating the owned-Vec decision as sufficient and design a safe lifetime-bound buffer with consumer tests before changing this API. If the remote advances, rebase the one local commit and repeat Ready validation.

## Artifacts and Notes

Go source evidence on Go 1.25.10 `darwin/arm64`:

    TestSimpleArenaAllocator: pass
    TestStdAllocator: pass
    reset-reuse bytes=[7 8]
    simple-len-gt-cap panic=runtime error: slice bounds out of range [:3:2]
    post-panic-next bytes=[3 4]
    new-negative panic=runtime error: makeslice: cap out of range
    std-negative panic=runtime error: makeslice: cap out of range

Focused Rust evidence:

    arena::tests: 6 passed, 0 failed, 0 ignored

Pre-sync Ready evidence:

    semantic receipt: 1 package, 1 unique command
    cargo fmt --all --check: pass
    complete tidb-util suite: 337 passed, 0 failed, 1 ignored; integration tests and doctest pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; no Go, Bazel, module, target-list, Rust manifest, production, or test-code trigger

Post-sync Ready evidence on remote base `641b4bb4792575acd164bdcf5a7fd2318c2cabb4`:

    both Go source tests: pass
    Go public probe: reset reuse [7 8], allocate-before-panic [3 4], negative inputs panic
    semantic receipt: 1 package, 1 unique command
    cargo fmt --all --check: pass
    complete tidb-util suite: 337 passed, 0 failed, 1 ignored; integration tests and doctest pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; the one-package diff adds only the receipt and plan

Failpoint decision:

    no failpoint., testfailpoint., or Bazel failpoint dependency match

## Interfaces and Dependencies

`rust/crates/tidb-util/src/arena.rs` retains the public trait `Allocator`, `SimpleAllocator::new(capacity: usize)`, and unit struct `StdAllocator`. `alloc` and `alloc_with_len` retain `Vec<u8>` results, and `rust/crates/tidb-util/src/lib.rs` retains `pub mod arena`. The package adds no runtime state or dependency.

Plan revision note: created after the complete current-source inventory, package-history review, failpoint inspection, Go source tests and public probes, Rust ownership and consumer review, exact assertion mapping, focused Rust tests, and the no-code-change decision. Updated after the complete pre-sync Ready profile passed, then after synchronization to `641b4bb4792575acd164bdcf5a7fd2318c2cabb4` and the repeated Ready profile.
