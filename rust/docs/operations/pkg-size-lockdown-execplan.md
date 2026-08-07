# Lock down the complete `pkg/util/size` Go package in Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

Reference: `PLANS.md` and the repository root `AGENTS.md`. The package worktree is `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-348-wave12`, based on official `hparser-integration` tip `a9d37829e286e73d13b130711d959e10dbd6e9fc`.

## Purpose / Big Picture

After this unit, reviewers can prove that the Rust `tidb_util::size` constants account for the complete direct Go package `pkg/util/size`, including its Bazel build artifact and all 20 generated Go AST obligations. The gate hashes both source artifacts, binds every Go constant to a compiled Rust constant and named test, and preserves the source's target-width Go ABI values rather than substituting Rust container layouts.

The package has no Go tests. Its observable contract is the five binary unit constants and 15 `unsafe.Sizeof` results used throughout TiDB memory accounting. An exact disposable Go probe records the target platform and all 20 values; the existing Rust tests already match that oracle, so this is a completeness lockdown rather than a behavior fix.

## Progress

- [x] (2026-08-08) Selected complete direct package `pkg/util/size`; confirmed two artifacts, no `doc.go`, tests, failpoints, build tags, platform variants, generated files, or testdata.
- [x] (2026-08-08) Read both Go artifacts, the existing Rust landing and inventory, direct consumer list, and generic Go AST census; measured `go test` as no test files and existing Rust tests as 2/2.
- [x] (2026-08-08) Measured all 20 Go constants from a disposable exact consumer on `darwin/arm64`; every current Rust value matches.
- [x] (2026-08-08) Replaced the legacy inventory with a two-artifact manifest and exact 20-row standard AST classification; the compiled owner/evidence gate, deterministic checker, targeted tests, package Clippy, and fmt pass.
- [x] (2026-08-08) Killed all 24 planned mutations from immutable provisional commit `88f3d872a42208925072af4d8936eec0ee67473d`; every mutated source was restored to its saved SHA-256 and passed the same restored check.
- [x] (2026-08-08) Added the compiled mutation receipt gate and content-addressed JSON receipt; the complete checker, no-failpoint Go oracle, 288/288 `tidb-util`, package Clippy, workspace fmt, direct ratchets, and `git diff --check` pass.
- [x] (2026-08-08) Passed the clean code-bearing Ready gate at `5ad9031947e618a270bd616b3f31a8e1448af7f4`: exact Go oracle, checker, 288/288 `tidb-util`, 7131/7131 workspace tests with 41 skips, workspace Clippy with warnings denied, fmt, direct ratchets, `git diff --check`, and `make -j12 lint`.
- [x] (2026-08-08) Published the code-bearing size chain by ordinary fast-forward to official `pingcap/tidb:hparser-integration`; `git ls-remote` returned `5ad9031947e618a270bd616b3f31a8e1448af7f4`, and GitHub API mapped author and committer to `dbsid <huanshengchen@gmail.com>` for both size commits.

## Surprises & Discoveries

- Observation: the existing Rust implementation is already behaviorally exact on the current target.
  Evidence: the Go probe printed `target=darwin/arm64`, `units=1024,1048576,1073741824,1099511627776,1125899906842624`, and `sizes=slice:24 byte:1 string:16 bool:1 pointer:8 interface:16 float64:8 uint64:8 int32:4 int:8 uint8:1 uint:8 func:8 int64:8 map:8`; Rust's named source test asserts the same formulas and values.
- Observation: the old six-column inventory covered only `size.go` and did not hash `BUILD.bazel` or use generated AST identities.
  Evidence: the direct boundary has two artifacts, while the generic Go package inventory emits exactly 20 content-addressed `const` obligations.
- Observation: `make -j12 lint` exits zero on macOS while printing two pre-existing diagnostics.
  Evidence: it reports the `rust/difftests/gobinaryrow` internal-package import and BSD `find` rejecting `-n`, then runs every dashboard-linter target and returns zero; size changes neither affected path.

## Decision Log

- Decision: use the complete direct package `pkg/util/size` as the atomic unit and retain `tidb-util::size` ownership.
  Rationale: the package contains one production Go file and one Bazel build file; no test/support artifacts exist to split or omit.
  Date/Author: 2026-08-08 / Codex
- Decision: classify all 20 AST obligations as `PORTED` and keep the current production constants unchanged.
  Rationale: direct Go measurement matches every Rust constant, and the Rust API intentionally publishes Go ABI sizes rather than claiming Rust container-layout equivalence.
  Date/Author: 2026-08-08 / Codex

## Outcomes & Retrospective

The complete `pkg/util/size` package lockdown is published. Both direct artifacts and all 20 generated Go AST obligations are content-addressed and classified `PORTED`; the measured unit and target-width Go ABI values match the unchanged Rust constants. All 24 mutations are killed and receipt-gated, the clean Ready workspace passes, and GitHub attributes the official commits to `dbsid`.

## Context and Orientation

`pkg/util/size/size.go` defines five binary byte units and 15 target-dependent or fixed Go ABI sizes through `unsafe.Sizeof`. `pkg/util/size/BUILD.bazel` is the second and final artifact. The constants are imported widely by TiDB's executor, expression, DDL, planner, storage, and memory-accounting paths.

`rust/crates/tidb-util/src/size/mod.rs` exposes source-shaped uppercase constants. `WORD_SIZE` derives the target pointer width, while the exported slice, string, interface, pointer, integer, function, and map constants deliberately describe Go representations. The existing two Rust tests pin the constant table and legacy inventory.

## Plan of Work

First, add `size.artifacts.tsv` and regenerate `size.inventory.tsv` with the generic Go AST tool. The checker requires exactly two artifacts, zero build/platform/generated/testdata classes, 20 `const` obligations, and 20 `PORTED` rows with exact Rust symbol and evidence mappings.

Second, update the in-module lockdown test to verify both artifact hashes, the standard ten-column inventory schema, the exact 20-anchor symbol map, and compile-time references to every exported constant. Keep the existing source constant test as the named behavioral evidence.

Third, commit an immutable restored baseline. From a disposable detached worktree, mutate each of the five unit constants and 15 ABI size constants independently, drift one inventory row, drift both direct artifacts independently, and rename one Rust owner. Each of the 24 mutations must fail its intended named test or checker, then restore from an external byte copy and pass the same check.

Finally, add the compiled mutation receipt gate and content-addressed JSON receipt, run the `tidb-util` WIP profile, then replay the complete Ready profile in a clean detached worktree. The final diff remains Rust, TSV, JSON, Python, and Markdown only, so `make bazel_prepare` is not required unless scope expands.

## Concrete Steps

The exact Go oracle commands are:

    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 go test -tags=intest,deadlock ./pkg/util/size
    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 python3 rust/scripts/pkg-size-lockdown.py

Rust checks use Rust 1.97 and the worktree-exclusive target:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-348-wave12/tgt cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-util -E 'test(/size::tests/)' --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-348-wave12/tgt cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-348-wave12/tgt cargo +1.97 clippy --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --all-targets -- -D warnings -A clippy::needless_update
    cargo +1.97 fmt --manifest-path rust/Cargo.toml --all -- --check

Ready additionally runs full workspace nextest, workspace Clippy with only the three existing lint classes allowed, direct ratchets, `git diff --check`, and `make -j12 lint`. Never run `make bazel_lint_changed`; the user did not request it.

## Validation and Acceptance

Acceptance requires both direct package artifacts to hash exactly and all 20 generated obligations to regenerate with one `PORTED` classification and concrete Rust owner/evidence. Removing an artifact, changing a Go AST node, deleting a Rust constant, or erasing an inventory row must fail independently.

Behavioral acceptance requires the five unit constants and 15 Go ABI constants to match the measured target values and target-width formulas. Mutation acceptance requires every planned mutation to produce a nonzero exit from its named test or checker, followed by a passing restored run and exact source SHA-256 match. Package tests, workspace tests, Clippy, fmt, lint, ratchets, remote ref, and GitHub attribution must pass before publication is reported.

## Idempotence and Recovery

The Python checker is deterministic; `--write` regenerates only the artifact manifest and standard inventory. Mutation probes save target bytes outside the repository, restore without Git checkout/reset/stash, compare SHA-256, and run the same named check after restoration. Temporary probes and targets are moved to Trash only after their evidence is recorded and commits are recoverable remotely.

## Artifacts and Notes

Baseline artifact hashes at official `a9d37829e2` are:

    284f23a6ab10e49c683bbad3a0e202d6a819bfa52dcf7365fb3e26319902897c  pkg/util/size/BUILD.bazel
    12f3b382a01df93e5dd0a0d022f9fe965679d7cdc501c9c764e52074829b5375  pkg/util/size/size.go

The generic census contains exactly 20 `const` obligations. No oracle or shared ratchet is expected to move.

The clean Ready receipt at code-bearing commit `5ad9031947e618a270bd616b3f31a8e1448af7f4` is:

    go test -tags=intest,deadlock ./pkg/util/size
      PASS; no test files
    python3 rust/scripts/pkg-size-lockdown.py
      pkg/util/size lockdown: 2 artifacts, 20 AST obligations, 20 PORTED
    cargo nextest run --offline --locked -p tidb-util --no-fail-fast
      288 passed
    cargo nextest run --offline --locked --workspace -j12 --no-fail-fast
      7131 passed, 41 skipped
    cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings -A clippy::assertions_on_constants -A clippy::needless_update -A clippy::type_complexity
      passed
    cargo fmt --all -- --check
      passed
    make -j12 lint
      exit 0; all dashboard linters ran

## Interfaces and Dependencies

All existing public constants remain unchanged. No dependency or Cargo manifest change is planned. The checker uses the existing generic Go AST tool and Python standard library; the Rust gate uses `sha2`, already available as a `tidb-util` dev dependency.

Security extension review: this compile-time constant package adds no network, authentication, persistence, deployment, IAM, secret, or dependency surface.

Revision note: created on 2026-08-08 after selecting the complete package, reading both direct artifacts and Rust landing, generating the 20-obligation census, and measuring every Go constant on the current target.

Revision note: updated on 2026-08-08 after the 24 mutation kills, compiled receipt gate, clean Ready replay, ordinary official fast-forward, and GitHub `dbsid` attribution checks all passed.
