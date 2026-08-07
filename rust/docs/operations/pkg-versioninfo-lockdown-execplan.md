# Lock down the complete `pkg/util/versioninfo` Go package in Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

Reference: `PLANS.md` and the repository root `AGENTS.md`. The package worktree is `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-349-wave13`, based on official `hparser-integration` tip `6d99186063e6cda053d510ec59493e5956ffa04c`.

## Purpose / Big Picture

After this unit, reviewers can prove that Rust accounts for the complete direct Go package `pkg/util/versioninfo`, including its Bazel build artifact and all six generated Go AST obligations. The gate hashes both source artifacts, binds the one immutable Go constant to a compiled Rust symbol and named test, and records why the five mutable Go variables are not fully transcreated.

Go accepts `-ldflags -X` initialization for all five variables and permits runtime reassignment. Rust exposes immutable `option_env!` constants with the same defaults, but that is only a compile-time approximation. The lockdown must preserve the compatibility API while preventing it from being reported as equivalent mutable storage.

## Progress

- [x] (2026-08-08) Selected complete direct package `pkg/util/versioninfo`; confirmed two artifacts, no `doc.go`, tests, failpoints, build tags, platform variants, generated files, or testdata.
- [x] (2026-08-08) Measured default, link-time override, and runtime reassignment behavior with a disposable Go probe; all five `var` fields are both link-time initialized and runtime mutable.
- [x] (2026-08-08) Replaced the legacy inventory with a two-artifact manifest and exact six-row generated AST classification; checker, two named Rust tests, 288/288 `tidb-util`, 99/99 `difftest-result-tests` with five skips, package Clippy, and fmt pass.
- [x] (2026-08-08) Committed immutable baseline `cb71e788efeaa993714c44f1ab81fbc0db48bb5f` and killed all nine planned mutations in a detached worktree; every source was restored to its saved SHA-256 and passed the same check.
- [x] (2026-08-08) Added the compiled mutation receipt and content-addressed JSON receipt; the complete checker, three versioninfo tests, 289/289 `tidb-util`, package Clippy, fmt, and `git diff --check` pass.
- [ ] Pass the clean Ready workspace gate and record its exact receipt.
- [ ] Publish each completed commit by ordinary fast-forward to official `pingcap/tidb:hparser-integration` and verify GitHub attribution as `dbsid`.

## Surprises & Discoveries

- Observation: the existing Rust source has the same defaults and supports compile-time environment overrides, but cannot reproduce Go runtime reassignment.
  Evidence: the default Go probe printed `initial=community:"Community" build:"None" hash:"None" branch:"None" edition:"Community" enterprise:""` and then printed runtime replacements for all five variables. With `-ldflags -X`, it printed all five link values before the same successful runtime replacements.
- Observation: the old six-column inventory double-counted six declarations and six behavioral rules as `PORTED`, then represented mutability as one synthetic decline.
  Evidence: the generic AST census emits exactly one `const` and five `var` obligations, so the five source variables themselves must carry the declined classification.

## Decision Log

- Decision: classify `const:CommunityEdition:0` as `PORTED` and each of the five `var:*` obligations as `DECLINED` with `go-probe:linktime_and_runtime_mutability` evidence.
  Rationale: this is the narrowest classification that matches both languages' actual storage contracts. Matching defaults alone does not make an immutable Rust constant equivalent to a mutable Go variable.
  Date/Author: 2026-08-08 / Codex
- Decision: retain all existing public Rust build-stamp constants as explicitly documented compile-time approximations.
  Rationale: removing them would create an unrelated Rust compatibility break, while claiming them as full ports would be false.
  Date/Author: 2026-08-08 / Codex

## Outcomes & Retrospective

Pending mutation and Ready gates.

## Context and Orientation

`pkg/util/versioninfo/versioninfo.go` contains one string constant and five package variables. `pkg/util/versioninfo/BUILD.bazel` is the second and final direct artifact. No original tests or support artifacts exist.

`rust/crates/tidb-util/src/versioninfo.rs` owns the corresponding Rust API. `rust/scripts/pkg-versioninfo-lockdown.py` regenerates and checks the package boundary and generated AST census. The adjacent TSV files record artifact hashes, classifications, and mutation evidence; `rust/crates/tidb-util/tests/versioninfo_lockdown.rs` compiles the final mutation receipt.

## Plan of Work

First, regenerate `versioninfo.artifacts.tsv` and `versioninfo.inventory.tsv`. The checker requires exactly two artifacts, zero build/platform/generated/testdata classes, one `const` plus five `var` obligations, one `PORTED` row, and five `DECLINED` rows with exact evidence.

Second, update the in-module Rust tests to check both artifact hashes, the standard ten-column inventory, the exact classification map, the compiled `COMMUNITY_EDITION` owner, and immutable compile-time override boundaries. Commit this restored state as the mutation baseline.

Third, in a disposable detached worktree, mutate the community literal, each declined classification independently, each direct package artifact independently, and the compiled Rust owner. Each of the nine mutations must fail its named check, then the saved external bytes must restore and pass the same check.

Finally, add the mutation results, compiled receipt gate, and JSON receipt. Run the complete `tidb-util` WIP profile, then replay the Ready profile in a clean detached worktree. The final diff is Rust, TSV, JSON, Python, and Markdown only, so `make bazel_prepare` is not required unless scope expands.

## Concrete Steps

The no-failpoint Go oracle and deterministic checker are:

    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 go test -tags=intest,deadlock ./pkg/util/versioninfo
    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 python3 rust/scripts/pkg-versioninfo-lockdown.py --inventory-only

Rust checks use Rust 1.97 and the worktree-exclusive target:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-349-wave13/tgt cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-util -E 'test(/versioninfo::tests/)' --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-349-wave13/tgt cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-349-wave13/tgt cargo +1.97 clippy --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --all-targets -- -D warnings -A clippy::needless_update
    cargo +1.97 fmt --manifest-path rust/Cargo.toml --all -- --check

Ready additionally runs full workspace nextest, workspace Clippy with the existing allowed lint classes, direct ratchets, `git diff --check`, and `make -j12 lint`. Do not run `make bazel_lint_changed` because the user did not request it.

## Validation and Acceptance

Acceptance requires both direct package artifacts to hash exactly and all six generated obligations to regenerate with exact classifications and evidence. Removing an artifact, changing a Go AST node, deleting the Rust constant, or changing any declined row must fail independently.

Mutation acceptance requires all nine planned mutations to produce a nonzero exit from their named test or checker, followed by a passing restored run and exact SHA-256 match. Package tests, workspace tests, Clippy, fmt, lint, ratchets, remote ref, and GitHub attribution must pass before completion is reported.

## Idempotence and Recovery

The Python checker is deterministic; `--write` regenerates only the artifact manifest and standard inventory. Mutation probes save target bytes outside the repository, restore without Git checkout/reset/stash, compare SHA-256, and run the same named check after restoration. Temporary probes and targets are moved to Trash only after their evidence is committed and recoverable remotely.

## Artifacts and Notes

Baseline artifact hashes at official `6d99186063e6cda053d510ec59493e5956ffa04c` are:

    faa7c407b40308a834bfafc8cceef8e03b09467222d04b5ef2e49fda107ca988  pkg/util/versioninfo/BUILD.bazel
    daa224cf8308f7b9de126919839ed95de7028e67b989d3d1d772d60309603003  pkg/util/versioninfo/versioninfo.go

The generic census contains exactly one `const` and five `var` obligations. No Go test or shared ratchet is expected to move.

## Interfaces and Dependencies

All six existing public Rust constants remain available. No Go production source, Bazel file, Cargo dependency, workspace membership, or public function signature changes. The checker uses only Python's standard library and the existing generic Go inventory tool.
