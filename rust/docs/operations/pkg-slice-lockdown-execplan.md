# Lock down the complete `pkg/util/slice` Go package in Rust

This ExecPlan is a living document maintained under `PLANS.md` and the root
`AGENTS.md`. The worktree is
`/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-352-wave16`, based on
official `hparser-integration` tip
`3bc65f2096f2b888f4119f04e45034c8f16f53ab`.

## Purpose / Big Picture

Upgrade the existing one-file `slice.go` evidence into an atomic claim for all
four direct artifacts in `pkg/util/slice`. The completed gate must fingerprint
the Bazel file, production source, source test, and Go test support; classify
all 41 generated AST obligations; compile-anchor the three Rust owners; retain
the complete original `TestSlice` structure; and preserve explicit declines
for the Go-only `TestMain` harness.

The existing Rust implementation appears source-compatible for observable
results, iteration order, predicate short-circuiting, decimal conversion, nil
versus empty cloning, allocation, and clone order. Production code remains
unchanged unless direct Go evidence falsifies that hypothesis.

## Progress

- [x] (2026-08-08) Selected complete leaf package `pkg/util/slice` at official
  tip `3bc65f2096`; confirmed no `doc.go`, nested `AGENTS.md`, failpoints,
  build tags, platform variants, generated source, fixtures, or testdata.
- [x] (2026-08-08) Read all four package artifacts, the Rust owner and legacy
  inventory, and direct Go/Rust consumers.
- [x] (2026-08-08) Passed no-failpoint baselines: exact Go `TestSlice` and all
  six existing Rust slice tests; generated 41 Go AST obligations.
- [x] (2026-08-08) Replaced the one-file inventory with the four-artifact,
  41-obligation package classification, deterministic checker, and compiled
  owner/evidence gate; committed the restored baseline as `d61d25c49c`.
- [x] (2026-08-08) Killed and byte-restored all 23 planned mutations in
  detached worktree `/tmp/tidb-slice-mutations.YwfJmN/worktree`; generated the
  content-addressed receipt and committed the proof as `9672736d7d`.
- [x] (2026-08-08) Passed the WIP gate: exact Go test, complete checker, seven
  slice gates, 294/294 `tidb-util` tests, package Clippy with warnings denied,
  workspace fmt, source-size, and `git diff --check`.
- [x] (2026-08-08) Passed the clean-detached Ready gate at code-bearing tip
  `9672736d7d`: 7137/7137 workspace tests with 41 configured skips, workspace
  Clippy with warnings denied, fmt, source-size, clean diff/status, and
  `make -j12 lint`.
- [ ] Publish by ordinary fast-forward to official `hparser-integration` and
  verify GitHub `dbsid` author and committer attribution.

## Surprises & Discoveries

- Observation: the legacy inventory has 19 hand-written production rows, but
  the complete package has 41 generated obligations.
  Evidence: the generated census contains three functions, one closure, two
  branches, four production loop outcomes, one test, one assertion, two test
  helper closures, two test loop outcomes, 24 test rows, one `TestMain`, and
  four `TestMain` rows.
- Observation: only `AllOf` has an original Go test.
  Evidence: `slice_test.go` contains `TestSlice`; source behavior for
  `Int64sToStrings` and `DeepClone` is covered by existing Rust boundary tests.
- Observation: Rust integrates `int64s_to_strings` in stats bootstrap SQL;
  `all_of` and `deep_clone` currently have no direct Rust production consumer.
- Observation: all 23 planned behavior, evidence, artifact, and symbol
  mutations were killed by their intended named gate without production-code
  changes or test hardening.
  Evidence: `slice.mutation-results.tsv` records one nonzero exit and exact
  byte restoration for every plan item against baseline `d61d25c49c`.
- Observation: `make -j12 lint` exits successfully on macOS while printing the
  repository's existing non-fatal `internal`-package and BSD `find -n`
  diagnostics.
  Evidence: the command continued through every dashboard linter and returned
  zero in the clean Ready worktree.

## Decision Log

- Decision: use the direct `pkg/util/slice` package as the atomic unit and keep
  ownership in `tidb-util`.
  Rationale: all artifacts and functions fit the existing module and the root
  policy forbids partial package claims.
  Date/Author: 2026-08-08 / Codex
- Decision: classify `TestMain` and its four goleak rows individually as
  `DECLINED` with Go source evidence.
  Rationale: Rust owns none of the Go common-test global state or named
  background goroutines.
  Date/Author: 2026-08-08 / Codex
- Decision: keep `all_of`'s idiomatic borrowed predicate boundary.
  Rationale: Go copies each `T` into the callback while Rust borrows `&T`; the
  established Rust API preserves the source's result, order, short-circuit,
  and side-effect sequence without requiring `T: Clone` or `T: Copy`.
  Date/Author: 2026-08-08 / Codex

## Outcomes & Retrospective

The complete `pkg/util/slice` package is locally locked down at its four-file
boundary. All 41 generated obligations, five Go-only declines, three compiled
Rust owners, and 23 killed/restored mutations are represented. WIP and clean
Ready gates pass without changing production behavior or a shared ratchet.
Only ordinary publication and remote attribution checks remain open.

## Context and Orientation

`slice.go` owns `AllOf`, `Int64sToStrings`, and generic `DeepClone`.
`slice_test.go` provides the four-row `TestSlice` table for `AllOf`.
`main_test.go` installs Go-only common-test and leak-check support, while
`BUILD.bazel` binds the three Go sources. `rust/crates/tidb-util/src/slice.rs`
owns the three compatible Rust APIs and six current tests.

## Plan of Work

Add `slice.artifacts.tsv`, replace `slice.inventory.tsv` with the generic AST
classification, and add a standard-library-only checker. Classify all 36
non-harness obligations as `PORTED`; classify the five `TestMain` obligations
as `DECLINED`. Replace the legacy Rust inventory parser with a four-artifact,
ten-column package gate and compile/test evidence anchors.

Commit a restored baseline, then run a disposable mutation sweep over AllOf,
integer conversion, deep clone, Go-test mapping, declined support, all four
artifacts, and all three compiled owners. Each mutant must fail its named check,
restore from saved bytes, match the baseline hash, and pass the same check.

Finally, run exact Go tests, the complete `tidb-util` WIP gate, then a new
clean-detached workspace Ready gate. The intended diff contains no Go, Bazel,
or module changes, so `make bazel_prepare` is not required unless scope moves.

## Concrete Steps

    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local go test -run '^TestSlice$' -tags=intest,deadlock ./pkg/util/slice
    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local python3 rust/scripts/pkg-slice-lockdown.py --inventory-only
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-352-wave16/tgt cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-352-wave16/tgt cargo +1.97 clippy --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --all-targets -- -D warnings -A clippy::needless_update
    cargo +1.97 fmt --manifest-path rust/Cargo.toml --all -- --check

Ready additionally runs full workspace nextest, workspace Clippy with the
existing allowed lint classes, source-size, diff/status cleanliness, and
`make -j12 lint`. Do not run `make bazel_lint_changed`.

The clean Ready replay used detached worktree
`/tmp/tidb-slice-ready.5vtmBB/repo` and exclusive target
`/tmp/tidb-slice-ready.5vtmBB/target`:

    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local go test -run '^TestSlice$' -tags=intest,deadlock ./pkg/util/slice
    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local python3 rust/scripts/pkg-slice-lockdown.py
    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/tmp/tidb-slice-ready.5vtmBB/target cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked --workspace -j12 --no-fail-fast
    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/tmp/tidb-slice-ready.5vtmBB/target cargo +1.97 clippy --manifest-path rust/Cargo.toml --offline --locked -j12 --workspace --all-targets -- -D warnings -A clippy::assertions_on_constants -A clippy::needless_update -A clippy::type_complexity
    cargo +1.97 fmt --manifest-path rust/Cargo.toml --all -- --check
    bash rust/scripts/check-source-size.sh
    git diff --check
    git status --porcelain=v1
    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local make -j12 lint

Results: 7137/7137 workspace tests passed with 41 configured skips; workspace
Clippy, fmt, source-size, diff/status cleanliness, and lint all returned zero.

## Validation and Acceptance

All four direct artifacts and all 41 AST obligations must regenerate exactly.
Every `PORTED` owner/evidence name must compile; all five declines must retain
source-backed Go-harness evidence. Every planned mutation must be killed and
restored. WIP and Ready gates, remote ref, and GitHub attribution must pass
before completion is reported.

## Idempotence and Recovery

The checker regenerates only its artifact manifest and inventory. Mutation
probes operate in a detached worktree, save exact bytes outside the repository,
restore without checkout/reset/stash, and rerun their named check. Temporary
targets are deleted only after the final commit is on the official remote.

## Artifacts and Notes

No shared oracle or ratchet should move. This in-memory generic helper has no
network, authentication, persistence, deployment, IAM, secret, logging, or
dependency surface.

Revision note: created on 2026-08-08 after package census and baseline tests;
updated after the 23-mutation proof and clean Ready replay completed.
