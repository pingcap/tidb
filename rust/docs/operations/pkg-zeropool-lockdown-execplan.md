# Lock down the complete `pkg/util/zeropool` Go package in Rust

This ExecPlan is a living document maintained under `PLANS.md` and the root
`AGENTS.md`. The worktree is
`/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-353-wave17`, based on
official `hparser-integration` tip
`e67e11b83b50a0a13ff59c80e6f524558585f48c`.

Receipt-extension work on 2026-08-08 uses accepted dual-remote tip
`5d4e8dccbe4e9b9a450f57b31db59f8e0447ffe4`. It changes only the existing
inventory checker, plan, mutation evidence, receipt, Rust receipt gate, and
this plan; `pkg/util/zeropool/pool.go` and the locked Rust implementation stay
unchanged.

## Purpose / Big Picture

Upgrade the existing one-file `pool.go` evidence into an atomic claim for all
three direct artifacts in `pkg/util/zeropool`. The completed gate must hash the
Bazel file, production source, and original test/benchmark source; classify all
55 generated Go AST obligations; compile-anchor the Rust `Pool` owners and all
four benchmark translations; and retain the established, explicit Go/Rust
semantic divergences instead of losing them during inventory replacement.

The current Rust implementation appears compatible for the represented
factory, zero-value, reuse, move, concurrency, allocation-reuse, and mutex
poisoning behavior. It intentionally does not reproduce Go's secondary pointer
pool, nullable factory, universal language zero value, or `sync.Pool` GC
eviction. Production code remains unchanged unless mutation testing exposes a
missing represented rule.

## Progress

- [x] (2026-08-08) Selected complete leaf package `pkg/util/zeropool` at
  official tip `e67e11b83b`; confirmed no `doc.go`, nested `AGENTS.md`,
  failpoints, build tags, platform variants, generated source, fixtures,
  tracked testdata, or Go test support file.
- [x] (2026-08-08) Read all three package artifacts, the complete Rust owner,
  legacy 30-rule inventory, Rust benchmark translation, and direct consumers.
- [x] (2026-08-08) Passed no-failpoint baselines: exact Go `TestPool`, all six
  existing Rust zeropool tests, and compilation of the Rust zeropool bench;
  generated 55 Go AST obligations.
- [x] (2026-08-08) Replaced the one-file inventory with the three-artifact,
  55-obligation package classification, nine-row semantic evidence table, and
  deterministic checker. Baseline commit: `0e633eeff8`.
- [x] (2026-08-08) Added the mutation plan, killed and restored all 27
  mutations from baseline `0e633eeff8`, and generated the content-addressed
  receipt in mutation-proof commit `937a34fda1`.
- [x] (2026-08-08) Passed WIP and clean-detached Ready gates: Go `TestPool`,
  7138/7138 workspace tests with 41 skips, workspace Clippy, fmt, source-size,
  diff/status cleanliness, and `make -j12 lint`.
- [x] (2026-08-08) Published by ordinary fast-forward to official
  `hparser-integration`; GitHub API verification confirms the two code commits
  use `dbsid <huanshengchen@gmail.com>`.
- [x] (2026-08-08) Falsified the publication gate: both the Python checker and
  Rust receipt test passed while plan rows Z003-Z005 named nonexistent files.
- [x] (2026-08-08) Upgraded the mutation plan to v2 with a baseline, source
  path, and SHA-256 evidence for every suite; both independent gates now check
  every `|`-separated path/hash pair before accepting results.
- [x] (2026-08-08) Killed and restored six boundary mutations: missing plan
  source, source/hash width mismatch, and stale source hash, once through the
  Python gate and once through the Rust gate. The receipt now records 33/33
  killed mutations across eight suites.
- [x] (2026-08-08) Complete the branch and clean-worktree Ready gates, verify
  unchanged ratchets, publish the exact SHA to both remotes, and reclaim only
  this unit's worktrees and target directories.

## Surprises & Discoveries

- Observation: the legacy inventory has 30 hand-written production rules,
  while the complete package has 55 generated AST obligations.
  Evidence: the generated census contains eleven production declaration,
  field, function, closure, and branch obligations; 25 `TestPool` obligations;
  and 19 obligations across four original benchmarks.
- Observation: three generated production nodes are intentionally not Rust
  ports: `Pool.pointers` and both branches choosing reusable versus newly
  allocated pointer containers in `Put`.
  Evidence: Rust owns a typed `Mutex<Vec<T>>` and moves `T` directly, so
  inventing a second pointer pool would add the Go interface-boxing workaround
  without preserving observable Rust behavior.
- Observation: generated AST rows alone cannot preserve the existing semantic
  evidence for nil factories, universal Go zero values, type assertion
  reachability, or GC eviction.
  Evidence: these contracts are language/runtime properties rather than
  distinct syntax nodes, so they require a separately hashed evidence table.
- Observation: content-addressing the plan file in the outer receipt did not
  make the paths inside the plan true.
  Evidence: `python3 rust/scripts/pkg-zeropool-lockdown.py` and the Rust
  `zeropool_lockdown` test both passed at accepted tip `5d4e8dccbe` although
  rows Z003-Z005 referred to files that did not exist.

## Decision Log

- Decision: use the direct `pkg/util/zeropool` package as the atomic unit and
  keep ownership in `tidb-util`.
  Rationale: all production/test/build artifacts and the existing benchmark
  translations fit one Rust crate, and root policy forbids a `pool.go`-only
  claim.
  Date/Author: 2026-08-08 / Codex
- Decision: classify all original `TestPool` and benchmark AST obligations as
  `PORTED`, with the Rust unit-test and bench source as their named evidence.
  Rationale: the Rust module retains the original test name and the existing
  harness-false bench translates all four original benchmark entry points.
  Date/Author: 2026-08-08 / Codex
- Decision: keep the eight real semantic differences `DECLINED` and the
  internal Go type assertion `UNREACHABLE` in a separate table.
  Rationale: this keeps the complete legacy contract visible without treating
  language/runtime differences as missing generated AST rows.
  Date/Author: 2026-08-08 / Codex
- Decision: validate source evidence uniformly for plan and result rows rather
  than special-casing the three bad paths.
  Rationale: one shared path/hash validator closes missing-file, list-width,
  and stale-hash variants for every current and future suite. The plan carries
  each suite's baseline explicitly because the original 27 probes and the six
  receipt-extension probes were measured at different accepted baselines.
  Date/Author: 2026-08-08 / Codex

## Outcomes & Retrospective

The complete three-artifact package claim is restored. The gate records 55 AST
obligations (52 `PORTED`, 3 explicit production `DECLINED`), nine semantic
rows (8 `DECLINED`, 1 `UNREACHABLE`), four benchmark owners, and 27/27 killed
and restored mutations. The clean Ready replay passed all 7138 workspace tests
with 41 configured skips, workspace Clippy, fmt, source-size, diff/status
checks, and `make -j12 lint`.

The first Ready replay had one unrelated flaky spill-file cleanup failure in
`tidb-executor`; its targeted rerun and the subsequent full replay passed.

The receipt extension found no Go/Rust behavior divergence and deliberately
moved no oracle. Its deliverable is that invalid mutation-plan evidence can no
longer publish: the old false-positive baseline passed, while all six
missing-path, width-drift, and hash-drift probes failed their named gate and
restored the exact plan bytes.

## Context and Orientation

`pool.go` defines generic `Pool`, `New`, `Get`, and `Put` over two Go
`sync.Pool` instances. `pool_test.go` contains the four-subtest `TestPool` plus
four benchmarks. `BUILD.bazel` binds the two Go files.
`rust/crates/tidb-util/src/zeropool/mod.rs` owns the compatible typed Rust pool
and six tests. `rust/crates/tidb-util/benches/zeropool.rs` owns executable
translations of all four benchmark names. The legacy inventory covers only
`pool.go` and must be replaced without dropping its semantic decisions.

## Plan of Work

Add `zeropool.artifacts.tsv`, regenerate `zeropool.inventory.tsv` from the
generic Go AST tool, and add `zeropool.semantic-divergences.tsv`. Classify 52
AST obligations as `PORTED`; classify `Pool.pointers` and both `Put` pointer
selection branches as `DECLINED`. Keep eight semantic declines and one
unreachable Go assertion as exact, separately validated evidence.

Replace the legacy Rust inventory parser with a three-artifact, twelve-category
package gate. Compile-anchor `Pool::default`, `Pool::new`, `Pool::get`, and
`Pool::put`; bind every original test row to `TestPool`; bind every benchmark
row to its named Rust bench source and require the bench target to compile.

Commit a restored baseline, then run a disposable mutation sweep over pool
behavior, ownership/safety, original-test mapping, all four benchmark mappings,
all nine semantic rows, all three direct artifacts, and the compiled Rust
owners. Each mutant must fail its named check, restore from saved bytes, match
the baseline hash, and pass the same check.

Finally, run exact Go tests, bench compilation, the complete `tidb-util` WIP
gate, then a new clean-detached workspace Ready gate. The intended diff
contains no Go, Bazel, or module changes, so `make bazel_prepare` is not
required unless scope moves.

## Concrete Steps

    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local go test -run '^TestPool$' -tags=intest,deadlock ./pkg/util/zeropool
    PATH=/tmp/tidb-task325-go126.gEaI15/go/bin:$PATH GOTOOLCHAIN=local python3 rust/scripts/pkg-zeropool-lockdown.py --inventory-only
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-353-wave17/tgt cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-util -E 'test(/zeropool::tests/)' --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-353-wave17/tgt cargo +1.97 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --bench zeropool
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-353-wave17/tgt cargo +1.97 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-util -E 'test(/zeropool_mutation_receipt/)' --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-353-wave17/tgt cargo +1.97 clippy --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --all-targets -- -D warnings -A clippy::needless_update
    cargo +1.97 fmt --manifest-path rust/Cargo.toml --all -- --check

Ready additionally runs full workspace nextest, workspace Clippy with the
existing allowed lint classes, source-size, diff/status cleanliness, and
`make -j12 lint`. Do not run `make bazel_lint_changed`.

## Validation and Acceptance

All three direct artifacts and all 55 AST obligations regenerate exactly.
Every `PORTED` owner/evidence name compiles; all three generated declines and
all nine semantic rows retain exact evidence. Every planned mutation is killed
and restored. WIP and Ready gates pass; the official remote ref was verified
after publication and the code commits are attributed to `dbsid` by GitHub.

## Idempotence and Recovery

The checker regenerates only its artifact manifest, inventory, and semantic
evidence. Mutation probes operate in a detached worktree, save exact bytes
outside the repository, restore without checkout/reset/stash, and rerun their
named check. Temporary targets are deleted only after the final commit is on
the official remote.

## Artifacts and Notes

No shared oracle or ratchet should move. This in-memory generic helper adds no
network, authentication, persistence, deployment, IAM, secret, logging, or
dependency surface.

Revision note: created on 2026-08-08 after package census and baseline tests.
Updated after the 27-mutation proof and clean Ready replay completed.
