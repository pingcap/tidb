# Lock down the complete `pkg/util/texttree` Go package in Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

Reference: `PLANS.md` and the repository root `AGENTS.md`. The package
worktree is `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-351-wave15`,
based on official `hparser-integration` tip
`b66916d368a22d24e4b6b8713372edb572915f06`.

## Purpose / Big Picture

After this unit, reviewers can prove that Rust accounts for the complete direct
Go package `pkg/util/texttree`, not only `texttree.go`. The gate will hash the
Go production source, both original test/support files, and the Bazel build
artifact; classify all 36 generated Go AST obligations; compile-anchor every
ported Rust owner; preserve explicit evidence for Go-only test harness setup;
and retain the three known arbitrary-byte versus Rust `&str` divergences.

The existing Rust formatting implementation appears source-compatible over its
valid UTF-8 domain. That is a hypothesis to falsify, not a completion claim.
Any representable mismatch found by direct Go evidence will receive a failing
Rust regression before its fix. If no mismatch is found, this remains a
completeness lockdown with no production or shared-ratchet movement.

## Progress

- [x] (2026-08-08) Selected complete leaf package `pkg/util/texttree` at
  official tip `b66916d368`; confirmed no `doc.go`, nested `AGENTS.md`,
  failpoints, build tags, platform variants, generated source, fixtures, or
  tracked testdata.
- [x] (2026-08-08) Read all four direct package artifacts, the complete Rust
  landing and legacy inventory, and direct consumers in profile, plancodec,
  and planner.
- [x] (2026-08-08) Ran the no-failpoint baseline: both exact Go tests and all
  four existing Rust texttree tests passed; the generic Go AST tool generated
  36 obligations.
- [x] (2026-08-08) Replaced the legacy one-file inventory with an exact
  four-artifact manifest and generated package classification: 31 `PORTED`,
  five Go-only `DECLINED`, and three separately hashed semantic divergences.
- [x] (2026-08-08) Added the deterministic checker, compiled owner/evidence
  gate, eight-suite/32-mutation plan, and content-addressed receipt inputs.
- [x] (2026-08-08) Committed the restored implementation baseline as
  `5a72db90c97c40d39ecb7d491ac5df66642ef51e`; after the first constant probe
  survived, tightened the named inventory gate with exact five-constant
  assertions and committed the corrected baseline as
  `a818661ca8eec4f3225946ebad2aefbe613248ac`.
- [x] (2026-08-08) Killed all 32 planned mutations in disposable worktree
  `/tmp/tidb-texttree-mutations.y6GPN5/worktree`; every mutation returned a
  nonzero named check, restored from a saved byte copy, and passed again with
  its exact baseline hash.
- [x] (2026-08-08) Passed the WIP gate: exact Go tests, checker, 6 texttree
  tests, 293/293 `tidb-util` tests, package Clippy with warnings denied,
  workspace fmt, source-size ratchet, and `git diff --check`.
- [ ] Commit mutation results and receipt, then pass the clean-detached Ready
  gate.
- [ ] Publish by ordinary fast-forward to official `hparser-integration` and
  verify GitHub `dbsid` author and committer attribution.

## Surprises & Discoveries

- Observation: the legacy inventory covers only `texttree.go` and uses 22
  hand-written rows, while the complete package has four artifacts and 36
  generated AST obligations.
  Evidence: the generic inventory emits five constants, two functions, ten
  branch outcomes, four loop outcomes, two tests, eight assertions, one
  `TestMain`, and four `TestMain` option rows.
- Observation: the package is small but user-visible and widely reused.
  Evidence: direct consumers render profile flamegraphs, encoded plans, binary
  plans, and planner explain trees.
- Observation: Rust has no production consumer yet, so the module currently
  provides parity evidence rather than an integrated replacement path.
  Evidence: no Rust source outside `texttree.rs` references either public
  function or any of the five constants.
- Observation: a compile anchor alone did not prove constant values.
  Evidence: the first `TREE_BODY` mutation survived the initial named inventory
  test; adding an exact five-constant tuple assertion killed all five constant
  mutations in the corrected sweep.

## Decision Log

- Decision: use the complete direct `pkg/util/texttree` package as the atomic
  unit and retain ownership in `tidb-util`.
  Rationale: root `AGENTS.md` requires package-level completeness, and all four
  direct artifacts land in one existing Rust module without an API expansion.
  Date/Author: 2026-08-08 / Codex
- Decision: classify `TestMain` and its four leak-exemption rows individually
  as `DECLINED`, with source-quoted Go-harness evidence.
  Rationale: Rust's test harness does not run TiDB's Go common-test setup or
  the four named Go goroutines; inventing equivalents would add unrelated
  behavior.
  Date/Author: 2026-08-08 / Codex
- Decision: preserve the three arbitrary-byte string divergences as explicit
  semantic evidence outside the generated AST obligation set.
  Rationale: Go strings admit invalid UTF-8 while the established Rust API
  accepts `&str`; the limitation is real even though it is not a distinct AST
  node and must remain visible after replacing the legacy inventory.
  Date/Author: 2026-08-08 / Codex
- Decision: keep production code unchanged unless a direct Go probe falsifies
  it over representable Rust inputs.
  Rationale: the existing Rust implementation already mirrors Go's rune scans,
  closest-body replacement, terminal-rune replacement, and concatenation.
  Date/Author: 2026-08-08 / Codex

## Outcomes & Retrospective

The complete package inventory and mutation proof are locally complete: all
four direct artifacts, 36 generated obligations, five Go-only declines, three
semantic divergences, and 32 killed/restored mutations are represented. WIP
tests and package checks pass. The clean workspace Ready replay, final lint,
and official publication remain open.

## Context and Orientation

`pkg/util/texttree/texttree.go` defines five tree glyph constants and two
functions. `Indent4Child` optionally replaces the closest body rune and always
appends a body plus gap. `PrettyIdentifier` returns an unchanged identifier for
empty indent, otherwise replaces the closest body with a middle/last glyph,
replaces the final indent rune with the node identifier, and appends the id.
`texttree_test.go` contains two source tests and eight assertions;
`main_test.go` configures Go-only common-test and leak-check support;
`BUILD.bazel` binds all three Go files.

`rust/crates/tidb-util/src/texttree.rs` owns the compatible Rust API and four
existing source-derived tests. The legacy `texttree.inventory.tsv` fingerprints
only the production file. The new checker and integration gate will bring this
package to the content-addressed receipt format used by the adjacent completed
`queue`, `arena`, `nocopy`, `size`, `versioninfo`, and `paging` units.

## Plan of Work

First, add `texttree.artifacts.tsv` and regenerate `texttree.inventory.tsv`
from the generic Go AST tool. All 21 production obligations and ten original
test/assertion obligations should be `PORTED`; the five `TestMain` support
obligations should be `DECLINED` with exact source evidence. Record the three
known arbitrary-byte divergences in a separately hashed evidence table.

Second, replace the legacy in-module inventory test with a strict four-artifact
and ten-column classification gate. Compile-anchor all five constants and two
functions, and require source-derived Rust test names for every ported branch,
loop, original test, and assertion.

Third, add a standard-library-only Python checker, a mutation plan, and an
integration receipt gate. Commit an immutable restored baseline. In a
disposable detached worktree, mutate each independent constant, formatting or
branch family, original-test mapping, declined support row, semantic
divergence, direct artifact, and compiled owner. Each mutation must fail its
intended named check, then pass after explicit byte restoration.

Finally, run the complete `tidb-util` WIP profile and a new clean-detached Ready
profile with its own Cargo target. The final diff is Rust, TSV, JSON, Python,
and Markdown only, so `make bazel_prepare` is not required unless scope changes.

## Concrete Steps

The no-failpoint Go oracle and deterministic checker use Go 1.26.0:

    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local go test -run '^(TestPrettyIdentifier|TestIndent4Child)$' -tags=intest,deadlock ./pkg/util/texttree
    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local go run ./rust/difftests/tools/go_package_lockdown_inventory --root . --package pkg/util/texttree
    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local python3 rust/scripts/pkg-texttree-lockdown.py --inventory-only

Rust WIP checks use Rust 1.97 and this worktree's exclusive target:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-351-wave15/tgt cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-util -E 'test(/texttree::tests/)' --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-351-wave15/tgt cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-351-wave15/tgt cargo +1.97 clippy --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --all-targets -- -D warnings -A clippy::needless_update
    cargo +1.97 fmt --manifest-path rust/Cargo.toml --all -- --check

Ready additionally runs full workspace nextest, workspace Clippy with only the
existing allowed lint classes, source-size and direct-count checks,
`git diff --check`, and `make -j12 lint`. Do not run
`make bazel_lint_changed`; the user did not request it.

## Validation and Acceptance

Acceptance requires all four direct package artifacts and all 36 generated AST
obligations to regenerate exactly. Every row must have one allowed status and
non-empty evidence; every `PORTED` owner must compile; every `DECLINED` row
must retain source-backed Go-harness evidence; and all three semantic
divergences must remain source-quoted and hash-bound.

Mutation acceptance requires every planned mutation to produce a nonzero exit
from its named test or checker, followed by a passing restored run and exact
SHA-256 match. Package tests, workspace tests, Clippy, fmt, lint, direct
ratchets, remote ref, and GitHub attribution must pass before completion is
reported.

## Idempotence and Recovery

The checker is deterministic; its write mode regenerates only the artifact
manifest and generated inventory. Mutation probes save exact source bytes
outside the repository, restore without checkout/reset/stash, compare hashes,
and rerun the same named check. Temporary targets are removed only after the
final commit is recoverable from the official remote.

## Artifacts and Notes

No shared oracle or ratchet is expected to move. The security extension is not
applicable: this pure in-memory formatting helper adds no network,
authentication, persistence, deployment, IAM, secret, logging, or dependency
surface.

Revision note: created on 2026-08-08 after selecting the complete package,
reading every direct artifact and consumer, running Go/Rust baselines, and
generating the 36-obligation census.
