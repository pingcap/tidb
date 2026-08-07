# Lock down the complete `pkg/util/arena` Go package in Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan implements root `AGENTS.md` non-negotiable 6: a Go-to-Rust claim must use one complete Go package as its atomic boundary. All evidence comes from dedicated worktree `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-346-wave10` at official integration base `fb8490526c5e30408fd0a444057b5b2e7072ad61`.

## Purpose / Big Picture

After this unit, reviewers can prove exactly which parts of the complete Go package `pkg/util/arena` are represented by `tidb_util::arena`, which source behavior safe Rust deliberately does not claim, and which boundary mismatch was fixed. Four package artifacts and all 48 generated Go AST obligations will be content-addressed. Source drift, an omitted artifact, a removed Rust owner, a stale decline, or a changed boundary rule will fail a named gate.

The observable behavioral fix is that `alloc_with_len(length, capacity)` panics when `length > capacity`, matching both Go allocators instead of letting Rust `Vec::resize` silently grow past the requested capacity. The lockdown also stops calling the whole module a complete transcreation: the current `Vec<u8>` API returns owned buffers and therefore cannot preserve Go's shared arena backing or stale-byte reuse after `Reset` without changing that API or using forbidden unsafe code.

## Progress

- [x] (2026-08-08) Published the preceding `pkg/util/queue` unit at official SHA `fb8490526c5e30408fd0a444057b5b2e7072ad61` and verified GitHub attribution to `dbsid`.
- [x] (2026-08-08) Selected the complete leaf package `pkg/util/arena`; confirmed no `tidb-util` remote owner, nested `AGENTS.md`, package `doc.go`, failpoint, build tag, platform variant, generated source, fixture, or testdata.
- [x] (2026-08-08) Read all four package artifacts, the 154-line Rust landing, and the direct `pkg/server` consumer surface.
- [x] (2026-08-08) Generated 48 content-addressed obligations and ran clean Go/Rust baselines: two Go tests and two Rust tests passed.
- [x] (2026-08-08) Measured exact-fit, disjoint allocation, Reset reuse, append reuse, invalid length/capacity, negative input, and signed-overflow boundaries from an exact scratch copy of `arena.go`.
- [x] (2026-08-08) Added the two invalid-length regressions, fixed both allocators while preserving SimpleAllocator's allocate-before-panic order, and kept the public Vec-returning API stable; the arena WIP set passes 7/7.
- [x] (2026-08-08) Checked in the four-artifact manifest, exact 48-row inventory, compiled owner/evidence gate, six-suite mutation plan/results, content-addressed receipt, and compiled mutation receipt gate; the checker reports `4 artifacts, 48 AST obligations, 40 PORTED, 8 DECLINED`.
- [x] (2026-08-08) Killed all 15 planned mutations from immutable baseline `4f47cc1b194b5438ae9f1d6950aa84a628b2ad3e` in disposable worktree `/tmp/tidb-arena-mutations.OSDAQY/worktree`; every source was restored byte-for-byte from `/tmp/tidb-arena-saved.EcnBkl` and the same named test passed after restoration.
- [x] (2026-08-08) Passed the code-bearing Ready gate at `4b84057276ba11a13420f13ad9b5eb10524c48dd`: exact Go tests, checker, 286/286 `tidb-util`, clean-detached 7129/7129 workspace tests with 41 skips, package/workspace Clippy with warnings denied, workspace fmt, direct ratchets, `git diff --check`, and `make -j12 lint`.
- [x] (2026-08-08) Published the validated arena chain by ordinary fast-forward to official `pingcap/tidb:hparser-integration`; `git ls-remote` returned `437cc3559ceb0c3def387066f8a509af0e8221f3`, and GitHub API returned author/committer login `dbsid` with verified email `huanshengchen@gmail.com` for all four arena commits.

## Surprises & Discoveries

- Observation: the Rust module's claim that shared backing is unobservable is false.
  Evidence: the Go probe printed `reset-reuse bytes=[7 8] off=3` and `append-reset-reuse bytes=[9] off=2`; writes through one allocation are visible to a new allocation after `Reset`, even when the old slice is no longer used.
- Observation: exact-fit allocation deliberately does not consume arena space.
  Evidence: `NewAllocator(4).Alloc(4)` printed `exact-fit len=0 cap=4 off=0`; the source uses strict `<`, not `<=`.
- Observation: both allocators reject `length > capacity`, but through different Go runtime paths.
  Evidence: the scratch probe printed `simple-len-gt-cap panic=runtime error: slice bounds out of range [:3:2]` and `std-len-gt-cap panic=runtime error: makeslice: cap out of range`.
- Observation: `SimpleAllocator.AllocWithLen` advances `off` before its invalid reslice panics when the requested capacity fits.
  Evidence: source order is `slice := s.Alloc(capacity)` followed by `slice[:length:capacity]`; the Rust fix must validate after calling `alloc` so this side effect remains.
- Observation: negative Go `int` inputs and signed-addition overflow are runtime panic boundaries outside Rust's `usize` input domain.
  Evidence: the probe printed distinct `makeslice`, negative full-slice, and `::-9223372036854775808` panic values; these are type-boundary evidence rather than synthetic Rust branches.
- Observation: the initial Rust `std_allocator` evidence test never called `reset`, so a mutation that made the stateless reset panic would survive despite the inventory mapping `stdAllocator.Reset` to that test.
  Evidence: mutation design review found no `allocator.reset()` call; the existing test was extended before any mutation run to reset and allocate again.
- Observation: the first clean workspace run failed only the two generated-source authority tests because the Ready command omitted the pinned Go toolchain from `PATH`.
  Evidence: both failures printed Python `FileNotFoundError: [Errno 2] No such file or directory: 'go'`; both targeted tests and both generators passed with `/tmp/tidb-lockdown-341-go.8LuDjg/go/bin` prepended, and the corrected full run passed 7129/7129.
- Observation: `make -j12 lint` exits zero on macOS while printing two pre-existing diagnostics.
  Evidence: the command reports the `rust/difftests/gobinaryrow` internal-package import and BSD `find` rejecting `-n`, then runs every dashboard-linter target and returns zero. This package changes neither affected path.

## Decision Log

- Decision: use the complete direct package `pkg/util/arena` as the atomic unit and keep ownership in `tidb-util`.
  Rationale: the package has one production file, two original test artifacts, one Bazel build artifact, and one existing Rust landing module. This satisfies the package-level completion boundary without touching `pkg/server` behavior.
  Date/Author: 2026-08-08 / Codex
- Decision: keep the existing `Vec<u8>` return API and classify the shared-backing portion of `SimpleAllocator.Alloc` plus `SimpleAllocator.AllocWithLen` as source-measured `DECLINED` behavior.
  Rationale: an owned Vec cannot alias the allocator's backing storage. Preserving mutable Go-slice aliasing with the same API requires unsafe ownership construction, while the workspace sets `unsafe_code = "forbid"`. A custom buffer type would be an unrelated public API replacement and would not remain Vec-compatible for future callers. The honest result is a complete lockdown with explicit decline, not a false transcreation claim.
  Date/Author: 2026-08-08 / Codex
- Decision: preserve the source order for invalid `AllocWithLen` requests by allocating capacity before asserting `length <= capacity`.
  Rationale: a fitting SimpleAllocator request increments `off` before Go panics on the final reslice. Validating earlier would fix the visible panic while introducing a state-transition divergence.
  Date/Author: 2026-08-08 / Codex

## Outcomes & Retrospective

The `pkg/util/arena` lockdown is complete. The representable invalid-length mismatch is fixed without changing the public Vec API or SimpleAllocator's allocate-before-panic order. All four Go artifacts and 48 AST obligations are content-addressed, with 40 PORTED and eight source-measured DECLINED rows. All 15 mutations are killed and receipt-gated, the clean Ready workspace passes, and the validated chain is published on the official integration branch with `dbsid` attribution. The shared-backing and stale-byte-reuse behavior remains an explicit safety-driven decline rather than a false transcreation claim.

## Context and Orientation

`pkg/util/arena/arena.go` defines the `Allocator` interface, a pre-allocating `SimpleAllocator`, the stateless `stdAllocator`, its `StdAllocator` singleton, and `NewAllocator`. The Simple allocator returns a zero-length slice whose capacity is the request. When `off + capacity` is strictly below the arena capacity, the slice points into the arena and advances `off`; otherwise Go returns a fresh allocation and leaves `off` unchanged. `AllocWithLen` reslices that buffer, and `Reset` only sets `off` to zero without clearing bytes.

`arena_test.go` has two tests, four support constants, and 18 assertions. `main_test.go` installs TiDB's common Go test setup and four process-global goleak ignores. Those Go harness mechanics are part of the atomic package inventory but are not production Rust behavior; they will be `DECLINED` with an exact test-runtime reason. `BUILD.bazel` is the fourth package artifact.

`rust/crates/tidb-util/src/arena.rs` currently exposes the source-shaped `Allocator`, `SimpleAllocator`, and `StdAllocator` names but returns independent Vec values. Its two tests port the source len/capacity/offset assertions. No other Rust module imports these arena symbols. The Go production consumer is `pkg/server/conn.go`, which allocates a 32 KiB arena, builds packet buffers with `AllocWithLen(4, capacity)`, and resets between command iterations.

## Plan of Work

First, add two Rust regressions beside the existing arena tests. One catches the Simple allocator's invalid length request and verifies that its offset already advanced by the requested capacity. The other proves the stateless allocator also panics. Run both before the production fix and record their failures.

Second, change both `alloc_with_len` implementations to call allocation first and then assert `length <= capacity` before resizing. Correct the module documentation so it describes the owned-Vec safety divergence rather than calling it unobservable. Add boundary tests for strict exact-fit fallback, Reset state, and the deliberately owned/zeroed Rust buffer behavior that backs the decline.

Third, add `arena.artifacts.tsv`, `arena.inventory.tsv`, `arena.mutation-plan.tsv`, a package-specific checker under `rust/scripts/`, and a mutation receipt integration test. The checker regenerates all four artifacts and 48 obligations. Production rows that represent len/capacity/offset and fresh allocation are `PORTED`; the shared backing true path and Simple `AllocWithLen` are `DECLINED` with the Go probe values above. The five `TestMain`/goleak rows are declined as Go-only test harness mechanics. Every other row resolves to a compiled Rust owner or named source-backed test.

Fourth, commit an immutable restored baseline. In a disposable detached worktree with a separate Cargo target, mutate each independent strict-fit, offset, fallback, invalid length, Reset, standard allocation, source-drift, decline, and symbol rule. Every mutation must fail its intended fully qualified test or gate, then restore to the saved source hash and rerun successfully.

Finally, run the `tidb-util` WIP profile and then the Ready profile: exact Go tests, package checker, full crate tests, package/workspace Clippy with warnings denied, literal workspace fmt, full workspace nextest, `git diff --check`, direct hashes/counts/ratchets, and `make -j12 lint`. The diff is expected to remain Rust/TSV/JSON/Python/Markdown only, so `make bazel_prepare` is not triggered unless the actual final change expands.

## Concrete Steps

Run Go evidence from repository root or `pkg/util/arena` as indicated:

    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 go test -run '^(TestSimpleArenaAllocator|TestStdAllocator)$' -tags=intest,deadlock ./pkg/util/arena
    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 python3 rust/scripts/pkg-arena-lockdown.py

Run Rust checks from `rust/` with the worktree-exclusive target:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-346-wave10/tgt cargo nextest run --offline --locked -p tidb-util -E 'test(/arena::tests/)' --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-346-wave10/tgt cargo nextest run --offline --locked -p tidb-util --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-346-wave10/tgt cargo clippy --offline --locked -p tidb-util --all-targets -- -D warnings -A clippy::needless_update
    cargo fmt --all -- --check

Ready additionally runs full workspace nextest, workspace Clippy with only the three existing lint classes allowed, and `make -j12 lint`. Never run `make bazel_lint_changed`; the user did not request it.

## Validation and Acceptance

Acceptance requires all four package artifacts and all 48 generated obligations to regenerate exactly. Each row has one non-pending status and concrete evidence. All PORTED production owners compile, all original source tests/assertions/constants resolve to named Rust tests, and every DECLINED row quotes a source-measured or Go-runtime-specific reason. Removing a source file, changing a Go AST node, deleting a Rust symbol, or erasing a decline must fail independently.

Behaviorally, both Rust allocators panic for `length > capacity`; a fitting Simple request advances offset before that panic. Valid requests retain their requested length and capacity, strict exact-fit uses fallback without offset movement, fallback does not move offset, and Reset returns offset to zero. The package report explicitly states that owned Rust Vec buffers do not preserve source shared backing or stale-byte reuse.

## Idempotence and Recovery

The Go inventory generator and package checker are deterministic. Inventory write mode may be rerun and reviewed as a normal diff; it must never hand-edit away missing rows. Cargo commands use only this worktree's `tgt/`.

Mutation probes start from an immutable commit. Every mutated file is copied to an exact temporary path outside the repository, restored byte-for-byte without Git checkout/reset/stash, compared by SHA-256, and followed by the same named test. Temporary source probes are placed only under `/tmp`, never under `pkg/`, and moved to trash after their output is recorded.

## Artifacts and Notes

Baseline source hashes at `fb8490526c` are:

    bd91b01a292d54a39aaea56ab375b2e11922edf8d6ad83b3c1644afc387e6818  pkg/util/arena/BUILD.bazel
    2e3dc44c7791b08740f594e961fb145852c0927db65c94b4cbf0cfe02683931b  pkg/util/arena/arena.go
    0130d21898acad7bc440ec720141664009828ec69bce7ff253dfccd74ec7b6dd  pkg/util/arena/arena_test.go
    b1536bcb5b0cd32422960eb01e1025e6b69a4559bb65392c9976564bc7c40c8c  pkg/util/arena/main_test.go

The AST census contains 48 obligations: seven functions, two branch outcomes, three declarations, five fields, two vars, two tests, 18 assertions, four test constants, one TestMain, and four TestMain rows. The planned status census is 40 PORTED and eight DECLINED: three shared-backing production obligations plus five Go-only TestMain/goleak obligations.

The clean Ready receipt at code-bearing commit `4b84057276ba11a13420f13ad9b5eb10524c48dd` is:

    go test -run '^(TestSimpleArenaAllocator|TestStdAllocator)$' -tags=intest,deadlock
      PASS
    python3 rust/scripts/pkg-arena-lockdown.py
      pkg/util/arena lockdown: 4 artifacts, 48 AST obligations, 40 PORTED, 8 DECLINED
    cargo nextest run --offline --locked -p tidb-util --no-fail-fast
      286 passed
    cargo nextest run --offline --locked --workspace -j12 --no-fail-fast
      7129 passed, 41 skipped
    cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings -A clippy::assertions_on_constants -A clippy::needless_update -A clippy::type_complexity
      passed
    cargo fmt --all -- --check
      passed
    make -j12 lint
      exit 0; all dashboard linters ran

## Interfaces and Dependencies

The public Rust interfaces remain `Allocator`, `SimpleAllocator::new`, and the unit struct `StdAllocator`; `alloc` and `alloc_with_len` continue returning `Vec<u8>`. No dependency or Cargo manifest change is planned. The checker uses the existing generic Go AST tool, Python standard library, and `sha2` already present as a `tidb-util` dev dependency.

Security extension review: this in-memory utility adds no network, authentication, persistence, deployment, IAM, secret, or dependency surface.

Revision note: created on 2026-08-08 after selecting the complete package, reading all direct artifacts and consumers, generating the 48-obligation census, running Go/Rust baselines, and measuring boundary behavior from an exact scratch source copy.

Revision note: updated on 2026-08-08 after the invalid-length fix, deterministic inventory generation, compiled owner/evidence gate, and WIP validation (`pkg/util/arena` Go tests, arena 7/7, package Clippy with warnings denied, and workspace fmt) all passed.

Revision note: updated on 2026-08-08 after the stdAllocator reset coverage correction, 15/15 mutation kills with byte-for-byte restoration, the compiled mutation receipt gate, and all 286 `tidb-util` tests passed.

Revision note: updated on 2026-08-08 after the clean Ready gate passed at `4b84057276`, including the corrected Go toolchain environment and 7129/7129 workspace result.

Revision note: closed on 2026-08-08 after ordinary fast-forward publication to official `hparser-integration` and GitHub API verification of the complete `dbsid` author/committer attribution.
