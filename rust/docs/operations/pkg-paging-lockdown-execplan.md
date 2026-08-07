# Lock down the complete `pkg/util/paging` Go package in Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

Reference: `PLANS.md` and the repository root `AGENTS.md`. The package
worktree is `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-350-wave14`,
based on official `hparser-integration` tip
`589ea5ec8a20687604cc20a4ebe82fb56137b8b5`.

## Purpose / Big Picture

After this unit, reviewers can prove that Rust accounts for the complete direct
Go package `pkg/util/paging`, not only `paging.go`. The gate will hash the Go
production source, both original test/support files, and the Bazel build
artifact; classify all 28 generated Go AST obligations; compile-anchor every
ported Rust owner; and preserve explicit evidence for Go-only test harness
setup.

The existing Rust arithmetic appears source-compatible at the measured
boundaries. That is a hypothesis to falsify, not a completion claim. Any
representable mismatch found by direct Go evidence will receive a failing Rust
regression before its fix. If no mismatch is found, this remains a successful
completeness lockdown with no production or shared-ratchet movement.

## Progress

- [x] (2026-08-08) Selected complete leaf package `pkg/util/paging` at official
  tip `589ea5ec8a`; confirmed no `doc.go`, nested `AGENTS.md`, failpoints, build
  tags, platform variants, generated source, fixtures, or tracked testdata.
- [x] (2026-08-08) Read all four package artifacts, the complete Rust landing,
  the legacy inventory, and direct Go consumers in planner, coprocessor,
  distsql, session variables, and tests.
- [x] (2026-08-08) Ran the no-failpoint baseline: both exact Go tests and all
  six existing Rust paging tests passed; the generic Go AST tool generated 28
  obligations.
- [x] (2026-08-08) Replaced the legacy one-file inventory with an exact
  four-artifact manifest and 28-row generated classification: 23 `PORTED`, 5
  `DECLINED`; the deterministic checker, compiled owner/evidence gate, and
  seven-suite/28-mutation plan pass their WIP checks.
- [ ] Commit the restored implementation and proof artifacts as the immutable
  provisional mutation baseline.
- [ ] Kill every planned mutation in a disposable worktree, restore every
  source byte-for-byte, and commit the compiled receipt.
- [ ] Pass WIP and clean-detached Ready gates, publish by ordinary fast-forward
  to official `hparser-integration`, and verify GitHub `dbsid` attribution.

## Surprises & Discoveries

- Observation: the legacy inventory covers only `paging.go` and uses 21
  hand-written rows, while the complete package has four artifacts and 28
  generated AST obligations.
  Evidence: the generic inventory emits six constants, two functions, ten
  branch outcomes, two tests, three recognized assertions, one `TestMain`, and
  four `TestMain` option rows.
- Observation: the original `TestMain` is test infrastructure rather than
  paging behavior.
  Evidence: it invokes TiDB's Go-only common-test setup and lists four Go
  goroutine leak exemptions; Rust's test harness has no corresponding daemons
  or package-level hook.
- Observation: the package is widely consumed despite its small implementation.
  Evidence: `CalculateSeekCnt` affects planner cost, `GrowPagingSize` affects
  coprocessor request growth, and constants feed distsql and sysvar defaults.

## Decision Log

- Decision: use the complete direct `pkg/util/paging` package as the atomic
  unit and retain ownership in `tidb-util`.
  Rationale: root `AGENTS.md` requires package-level completeness, and all four
  direct artifacts land in one existing Rust module without an API expansion.
  Date/Author: 2026-08-08 / Codex
- Decision: classify `TestMain` and its four leak-exemption rows individually
  as `DECLINED`, with source-quoted Go-harness evidence.
  Rationale: silently omitting support code would overstate package parity,
  while inventing equivalent Rust daemons would add behavior unrelated to
  paging semantics.
  Date/Author: 2026-08-08 / Codex
- Decision: keep production code unchanged unless a direct Go probe falsifies
  it.
  Rationale: existing Rust already preserves Go's unsigned wrapping order and
  piecewise seek-count boundaries; a completeness lockdown does not require
  speculative churn.
  Date/Author: 2026-08-08 / Codex

## Outcomes & Retrospective

No completion outcome is claimed yet. Baseline Go and Rust tests pass, but the
complete package inventory, mutation receipt, clean Ready replay, and official
publication remain open.

## Context and Orientation

`pkg/util/paging/paging.go` defines six constants and two functions.
`GrowPagingSize` normalizes a too-small maximum, doubles with `uint64` wrap,
then caps the result. `CalculateSeekCnt` returns a three-region piecewise
estimate: zero, logarithmic growth through the geometric prefix, then a
rounded fixed-page excess. `paging_test.go` contains two source tests;
`main_test.go` configures Go-only common-test and leak-check support;
`BUILD.bazel` binds all three Go files.

`rust/crates/tidb-util/src/paging.rs` owns the compatible Rust API and six
existing source-derived tests. The legacy `paging.inventory.tsv` fingerprints
only the production file. The new checker and integration gate will bring this
package to the content-addressed receipt format used by the adjacent completed
`queue`, `arena`, `nocopy`, `size`, and `versioninfo` units.

## Plan of Work

First, add `paging.artifacts.tsv` and regenerate `paging.inventory.tsv` from the
generic Go AST tool. All 18 production obligations and five original
test/assertion obligations should be `PORTED`; the five `TestMain` support
obligations should be `DECLINED` with exact source evidence.

Second, replace the legacy in-module inventory test with a strict four-artifact
and ten-column classification gate. Compile-anchor all six constants and two
functions, and require the source-derived Rust test names for every ported
branch and original assertion.

Third, add a standard-library-only Python checker, a mutation plan, and an
integration receipt gate. Commit an immutable restored baseline. In a
disposable detached worktree, mutate each independent constant, arithmetic or
branch family, original-test mapping, declined support row, direct artifact,
and compiled owner. Each mutation must fail its intended named check, then pass
after explicit byte restoration.

Finally, run the complete `tidb-util` WIP profile and a new clean-detached Ready
profile with its own Cargo target. The final diff is Rust, TSV, JSON, Python,
and Markdown only, so `make bazel_prepare` is not required unless scope changes.

## Concrete Steps

The no-failpoint Go oracle and deterministic checker use Go 1.26.0:

    GOTOOLCHAIN=local /tmp/tidb-task325-go126.gEaI15/go/bin/go test -run '^(TestGrowPagingSize|TestCalculateSeekCnt)$' -tags=intest,deadlock ./pkg/util/paging
    GOTOOLCHAIN=local /tmp/tidb-task325-go126.gEaI15/go/bin/go run ./rust/difftests/tools/go_package_lockdown_inventory --root . --package pkg/util/paging
    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local python3 rust/scripts/pkg-paging-lockdown.py --inventory-only

Rust WIP checks use Rust 1.97 and this worktree's exclusive target:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-350-wave14/tgt cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-util -E 'test(/paging::tests/)' --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-350-wave14/tgt cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-350-wave14/tgt cargo +1.97 clippy --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --all-targets -- -D warnings -A clippy::needless_update
    cargo +1.97 fmt --manifest-path rust/Cargo.toml --all -- --check

Ready additionally runs full workspace nextest, workspace Clippy with only the
existing allowed lint classes, source-size and direct-count checks,
`git diff --check`, and `make -j12 lint`. Do not run
`make bazel_lint_changed`; the user did not request it.

## Validation and Acceptance

Acceptance requires all four direct package artifacts and all 28 generated AST
obligations to regenerate exactly. Every row must have one allowed status and
non-empty evidence; every `PORTED` owner must compile; every `DECLINED` row
must retain source-backed Go-harness evidence.

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

Baseline artifact byte/line counts at official `589ea5ec8a` are:

    546 bytes / 24 lines   pkg/util/paging/BUILD.bazel
    1191 bytes / 33 lines pkg/util/paging/main_test.go
    2950 bytes / 69 lines pkg/util/paging/paging.go
    1546 bytes / 36 lines pkg/util/paging/paging_test.go

No shared oracle or ratchet is expected to move. The security extension is not
applicable: this in-memory arithmetic helper adds no network, authentication,
persistence, deployment, IAM, secret, logging, or dependency surface.

Revision note: created on 2026-08-08 after selecting the complete package,
reading every direct artifact and consumer, running Go/Rust baselines, and
generating the 28-obligation census.
