# Complete and certify `pkg/util/bitmap` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's concurrent bitmap is used by hash join to record matches and null-key columns. The Go package is the semantic authority: concurrent `Set` calls must report exactly one winner for each transition from zero to one, bit numbering is most-significant-bit first within each 32-bit segment, Reset must clear and reuse or grow storage, Clone must be independent, and memory accounting is capacity based. This plan audits the complete Go package and makes its Rust owner and tests one atomic package claim.

## Progress

- [x] (2026-08-11 15:43Z) Fixed the complete four-file Go inventory at `6a85c6bbbd6cae7e0eea20a75ecd0853ac3545d6`; current bytes match that pin.
- [x] (2026-08-11 15:43Z) Confirmed there is no `doc.go`, build tag, platform variant, generated input, fixture, benchmark, fuzz target, example, or failpoint use.
- [x] (2026-08-11 15:46Z) Passed all three Go source tests and measured public bounds, clone independence, Reset reuse/growth, memory accounting, and signed-length behavior with a repository-external probe.
- [x] (2026-08-11 15:48Z) Reviewed the Rust owner, export, and consumer surface; no Rust consumer exists outside the owner.
- [x] (2026-08-11 15:48Z) Identified that the Rust unique-setter test serializes each clear after all setters, unlike the overlapping Go test, and that unchecked release arithmetic can create a bitmap with zero segments for an oversized length.
- [x] (2026-08-11 15:56Z) Added a release-mode regression, observed it fail against the old arithmetic, then observed the identical command pass after the fix.
- [x] (2026-08-11 15:59Z) Restored the source test's clear/set overlap, encoded the source non-concurrent read contract in Rust ownership, and rejected oversized lengths consistently.
- [x] (2026-08-11 15:56Z) Completed focused WIP checks and the pre-sync Ready profile on base `04a008ca13a3be1923ead6cf44fa49c7a8d01a1a`.
- [x] (2026-08-11 16:00Z) Confirmed the fetched remote remained at that base and repeated the complete Ready profile successfully.
- [x] (2026-08-11 16:06Z) Detected remote commit `3a42b225d1fb910d1d46345dd51740b2a87bd20c`, rebased the single package commit onto it, and repeated the complete Ready profile successfully.
- [ ] Push the single linear package commit without force and verify the freshly fetched remote SHA.

## Surprises & Discoveries

- Observation: the existing Rust unique-setter test does not reproduce the source concurrency relationship.
  Evidence: it uses start/done barriers and waits for all 50 workers before every following clear. The Go loop immediately attempts its next clear while goroutines from previous iterations may still call `Set(31)`.

- Observation: source tests cover all three main behaviors but omit several exported API boundaries.
  Evidence: the public probe observes negative/out-of-range indexes, serial winner reporting, Clone independence, capacity-based byte counts of 40 bytes for 33 bits, shrink reuse at 40 bytes, and growth to 44 bytes for 65 bits.

- Observation: signed Go length behavior is not a desirable Rust API contract.
  Evidence: `NewConcurrentBitmap(-1)` constructs an inert 32-byte bitmap, `NewConcurrentBitmap(-32)` and `NewConcurrentBitmap(math.MaxInt)` panic, while `Reset(math.MaxInt)` leaves a one-segment bitmap claiming `MaxInt` bits and a later `Set(32)` panics. Rust uses `usize`, so negative lengths are unrepresentable; oversized lengths should be rejected before arithmetic or allocation rather than reproduce the malformed Reset state.

- Observation: Go's `UnsafeIsSet` is an ordinary read and explicitly requires quiescence, but the Rust method accepts `&self` and performs an atomic relaxed load while claiming it is non-thread-safe.
  Evidence: no Rust caller depends on shared access. Requiring `&mut self` encodes the source precondition without introducing mixed atomic/non-atomic access, which would be undefined behavior in Rust.

- Observation: `main_test.go` is Go test-harness policy rather than bitmap behavior.
  Evidence: it installs common TiDB setup and process-level goleak exclusions. Rust tests do not create those Go background goroutines; the file remains part of the source inventory with an explicit no-port decision.

## Decision Log

- Decision: Replace per-round barriers with 50 persistent workers consuming a bounded queue of 500,000 progressively submitted set operations.
  Rationale: this preserves the Go test's total work, competitor count, clear-before-submit ordering, and overlap between future clears and unfinished setters without creating 500,000 operating-system threads.
  Date/Author: 2026-08-11 / Codex

- Decision: Change `unsafe_is_set` to require `&mut self` while retaining a relaxed atomic load.
  Rationale: exclusive borrowing enforces the source's quiescence requirement in safe Rust; the atomic representation must remain because the same segments are mutated atomically by `set`.
  Date/Author: 2026-08-11 / Codex

- Decision: Centralize segment-count calculation and reject lengths whose source-style rounding exceeds the platform `int` domain.
  Rationale: Go's constructor rejects the maximum signed length through overflow and allocation failure, while Go Reset can create a malformed bitmap. Rust should preserve valid-domain behavior and make the invalid boundary a deterministic panic in debug and release builds.
  Date/Author: 2026-08-11 / Codex

- Decision: Treat `6a85c6bbbd6cae7e0eea20a75ecd0853ac3545d6` as the accepted Go package pin.
  Rationale: it is the last commit changing direct package bytes, and all four current artifacts match it exactly.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The complete inventory, Go source baseline, boundary probe, Rust ownership review, semantic-gap fix, failing-then-passing regression, receipt, latest-remote rebase, and all Ready profiles are complete. Only non-force publication and remote-SHA verification remain. The implementation improves valid-domain correctness and test fidelity without adding dependencies or touching another package.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/bitmap/BUILD.bazel`, `pkg/util/bitmap/concurrent.go`, `pkg/util/bitmap/concurrent_test.go`, and `pkg/util/bitmap/main_test.go`. `concurrent.go` defines `ConcurrentBitmap`, constructor, Clone, Reset, BytesConsumed, concurrent Set, and the two single-owner accessors. `concurrent_test.go` contains `TestConcurrentBitmapSet`, `TestConcurrentBitmapUniqueSetter`, and `TestResetConcurrentBitmap`; `main_test.go` supplies `TestMain`; `BUILD.bazel` owns one public library and one flaky short test target.

Rust owns the package in `rust/crates/tidb-util/src/bitmap.rs` and exports it through `rust/crates/tidb-util/src/lib.rs`. No other Rust file imports `ConcurrentBitmap`. Go production callers are confined to `pkg/executor/join`, where concurrent match recording uses Set and post-join reads use UnsafeIsSet; null-aware join construction uses UnsafeSet, Clone, Reset, and BytesConsumed.

## Milestones

The source-oracle milestone inventories every direct artifact, proves current bytes equal the accepted pin, lists and runs every Go test without failpoints, and records uncovered public behavior with a probe. Acceptance is three listed and passing Go tests plus stable observations for valid bounds, clone/reset, and memory accounting.

The parity milestone maps each source test to a Rust test with equivalent assertions and scheduling intent. Acceptance requires the unique-setter clear and set operations to overlap, the exact source counter invariants to remain, and an explicit safe-Rust decision for signed and oversized lengths.

The publication milestone adds one atomic package receipt and this living plan, runs complete Ready validation, synchronizes one commit to current `hparser-integration`, pushes without force, and proves the local and freshly fetched remote SHAs match.

## Plan of Work

First add an overflow regression and run it in release mode against the old arithmetic so it fails. Then introduce one `segment_len` helper used by construction and Reset, make `unsafe_is_set` require exclusive access, adapt the source-mapped read phase to recover ownership after joining workers, and rewrite unique-setter scheduling around a bounded multi-consumer channel.

Run all focused bitmap tests in debug and release mode plus the Go source tests and race detector. Once WIP behavior is stable, validate the semantic receipt, full `tidb-util` tests, workspace formatting, all-target Clippy with warnings denied, and repository lint. The diff changes a Rust production/test file plus documentation and receipt only, so `make bazel_prepare` is not required unless synchronization introduces a trigger.

## Concrete Steps

From repository root, run the Go authority:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -list . -tags=intest,deadlock ./pkg/util/bitmap
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^(TestConcurrentBitmapSet|TestConcurrentBitmapUniqueSetter|TestResetConcurrentBitmap)$' -tags=intest,deadlock -count=1 ./pkg/util/bitmap
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -run '^(TestConcurrentBitmapSet|TestConcurrentBitmapUniqueSetter|TestResetConcurrentBitmap)$' -tags=intest,deadlock -count=1 ./pkg/util/bitmap

Run `/tmp/tidb-bitmap-probe.go` from repository root to observe public boundaries:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go run /tmp/tidb-bitmap-probe.go

From `rust`, run focused and Ready Rust gates:

    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib 'bitmap::tests'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked --release -j12 -p tidb-util --lib 'bitmap::tests'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings

From repository root, validate the receipt and lint recipe:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/bitmap.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint

## Validation and Acceptance

`go test -list .` must list exactly the three source tests, and normal plus race-enabled runs must pass. Focused Rust validation must pass exactly six tests: three source-named mappings, valid public boundaries/bit order/memory accounting, clone/reset behavior, and deterministic oversized-length rejection. The unique-setter test must retain the source constants, 500,000 total Set attempts, and both source counter assertions without placing a completion barrier before the next clear.

The receipt must accept exactly one pinned Go package and one unique focused command. Complete owning-crate tests, formatting, all-target Clippy, and repository lint must pass. The final commit may contain only the bitmap owner/test, its receipt, and this plan. Publication must be non-force, and a fresh fetch must show matching local and remote SHAs.

## Idempotence and Recovery

All checks are safe to rerun. The Go probe is outside the repository and can be moved to Trash after evidence is recorded. If remote advances, synchronize the one package commit onto the new remote tip, re-run Ready validation, and push normally. If the concurrency rewrite flakes, stop publication and tighten scheduling while retaining the source invariants; do not weaken or ignore the test.

## Artifacts and Notes

Initial Go evidence on Go 1.25.10 `darwin/arm64`:

    go test -list: exactly 3 tests
    all 3 source tests: pass
    public probe: new(-1) is inert; new(-32) and new(MaxInt) panic
    public probe: 33 bits consume 40 bytes; shrink retains 40; growth to 65 consumes 44
    public probe: bounds ignored, first Set wins, Clone independent, Reset clears
    public probe: Reset(MaxInt) leaves one segment and Set(32) later panics

Initial Rust evidence:

    bitmap::tests: 5 passed, 0 failed, 0 ignored

Regression evidence:

    old release arithmetic: oversized_length_is_rejected_without_release_wraparound failed because new(usize::MAX) returned normally
    fixed release arithmetic: the identical exact test passed

Pre-sync Ready evidence on base `04a008ca13a3be1923ead6cf44fa49c7a8d01a1a`:

    all 3 Go source tests: pass
    all 3 Go source tests under -race: pass
    focused Rust debug and release suites: 6 passed, 0 failed, 0 ignored
    semantic receipt: 1 package, 1 unique command
    cargo fmt --all --check: pass
    complete tidb-util suite: 338 passed, 0 failed, 1 ignored; integration tests and doctest pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; no Go, Bazel, module, target-list, or Rust manifest change triggers it

Post-sync Ready evidence on the unchanged remote base `04a008ca13a3be1923ead6cf44fa49c7a8d01a1a`:

    all 3 Go source tests: pass
    all 3 Go source tests under -race: pass
    Go public probe: valid bounds, clone/reset, byte counts, and signed boundary observations unchanged
    focused Rust debug and release suites: 6 passed, 0 failed, 0 ignored
    semantic receipt: 1 package, 1 unique command
    cargo fmt --all --check: pass
    complete tidb-util suite: 338 passed, 0 failed, 1 ignored; integration tests and doctest pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; the one-package diff has no trigger

Final pre-push Ready evidence after rebasing onto remote base `3a42b225d1fb910d1d46345dd51740b2a87bd20c`:

    all 3 Go source tests: pass
    all 3 Go source tests under -race: pass
    Go public probe: valid bounds, clone/reset, byte counts, and signed boundary observations unchanged
    focused Rust debug and release suites: 6 passed, 0 failed, 0 ignored
    semantic receipt: 1 package, 1 unique command
    cargo fmt --all --check: pass
    complete tidb-util suite: 338 passed, 0 failed, 1 ignored; integration tests and doctest pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; the one-package diff has no trigger

Failpoint decision:

    no failpoint, testfailpoint, or Bazel failpoint dependency match

## Interfaces and Dependencies

The public Rust module remains `tidb_util::bitmap`. `ConcurrentBitmap::new`, Reset, Clone, BytesConsumed, Set, UnsafeSet, and UnsafeIsSet remain represented as `new`, `reset`, `Clone`, `bytes_consumed`, `set`, `unsafe_set`, and `unsafe_is_set`. This package introduces no dependency or runtime service. `unsafe_is_set` deliberately tightens its receiver to exclusive access because no current Rust consumer exists and the Go source labels concurrent use unsafe.

Plan revision note: created after the current-source inventory, history and exact-byte review, failpoint decision, Go source tests and public probe, Rust owner/export/consumer review, focused Rust baseline, and change-instruction critique.
