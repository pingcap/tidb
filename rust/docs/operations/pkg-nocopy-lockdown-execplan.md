# Lock down the complete `pkg/util/nocopy` Go package in Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current as work proceeds.

Reference: `PLANS.md` and the repository root `AGENTS.md`. The package worktree is `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-347-wave11`, based on official `hparser-integration` tip `04cbb8663ce71b4bec4a1d01f657be157dea871b`.

## Purpose / Big Picture

After this unit, reviewers can prove that the Rust `tidb_util::nocopy::NoCopy` marker accounts for the entire direct Go package `pkg/util/nocopy`, including its Bazel build artifact and all three generated Go AST obligations. The gate will hash both source artifacts, bind every source owner to a Rust symbol and named test, and preserve the source's zero-sized no-op lock/unlock behavior.

The source's important behavioral contract is its `sync.Locker` marker: Go's `go vet` copylocks analyzer rejects passing `NoCopy` or a struct containing it by value. Rust's safe ownership model already makes `NoCopy` non-`Copy` and non-`Clone`; a rustdoc compile-fail test proves that copying the marker is rejected. The package has no Go unit tests, test harness, fixtures, generated files, or build-tag variants.

## Progress

- [x] (2026-08-08) Selected complete direct package `pkg/util/nocopy`; confirmed two artifacts, no `doc.go`, tests, failpoints, build tags, platform variants, generated files, or testdata.
- [x] (2026-08-08) Read the Go source, Bazel rule, existing Rust landing, direct `stmtctx` embedding consumer, and generic Go AST census; measured `go test` as no test files.
- [x] (2026-08-08) Measured the Go copylocks oracle in a disposable probe: `go vet` reports four diagnostics for copying `NoCopy` and a holder containing it; rustdoc's existing compile-fail test passes.
- [x] (2026-08-08) Replaced the legacy seven-row hand-written inventory with a content-addressed two-artifact manifest and exact three-row AST classification; inventory checker reports `2 artifacts, 3 AST obligations, 3 PORTED`.
- [x] (2026-08-08) Added the compiled owner/evidence gate, five-suite mutation plan, eight killed results, deterministic checker, compiled mutation receipt gate, and content-addressed receipt.
- [x] (2026-08-08) Killed all eight planned mutations from immutable provisional commit `0474a0242a5396e40bb43b8691c29bf4ce919694`; every mutated file was restored to its saved SHA-256 and passed its restored check.
- [x] (2026-08-08) Passed the WIP gate: no-failpoint Go package oracle, complete checker, all 287 `tidb-util` tests, rustdoc compile-fail test, package Clippy with warnings denied, workspace fmt, and `git diff --check`.
- [ ] Run WIP and Ready/full-workspace gates, publish by ordinary fast-forward, and verify GitHub attribution to `dbsid`.

## Surprises & Discoveries

- Observation: the package has no Go tests, so `go test -tags=intest,deadlock ./pkg/util/nocopy` reports `[no test files]`.
  Evidence: the exact command exited zero with `? github.com/pingcap/tidb/pkg/util/nocopy [no test files]`.
- Observation: Go's no-copy behavior is enforced by `go vet`, not by a runtime method body.
  Evidence: a temporary copied package plus `copyMarker` and `copyHolder` produced four `copylocks` diagnostics: parameter and return copies for both the marker and a holder containing it.
- Observation: the existing Rust landing had a seven-row manual inventory and did not hash `pkg/util/nocopy/BUILD.bazel`.
  Evidence: the generic Go AST tool reports exactly three obligations (two functions and one declaration), while the package boundary contains two tracked artifacts.

## Decision Log

- Decision: use the direct package boundary `pkg/util/nocopy` as the atomic completion unit and retain `tidb-util::nocopy` ownership.
  Rationale: the package contains one production Go file and one Bazel build file; no test/support artifacts exist to split or omit.
  Date/Author: 2026-08-08 / Codex
- Decision: classify all three AST obligations as `PORTED`, while keeping the Go vet copylocks oracle as a separate rustdoc compile-fail evidence gate.
  Rationale: the Rust marker is deliberately neither `Copy` nor `Clone`, and the doc test rejects the same source-level copy operation that Go vet forbids; no source behavior needs to be declined.
  Date/Author: 2026-08-08 / Codex
- Decision: preserve the existing public Rust API (`NoCopy::new`, `lock`, `unlock`) and no-op bodies.
  Rationale: current Rust consumers already use this safe marker shape, and changing it would expand scope without improving source fidelity.
  Date/Author: 2026-08-08 / Codex

## Outcomes & Retrospective

No completion outcome is claimed yet. Source reading, Go/Rust boundary measurements, artifact manifest, generic inventory, compiled owner/evidence gate, mutation proof, receipt, and WIP gate are complete; the clean Ready gate and publication remain unfinished.

## Context and Orientation

`pkg/util/nocopy/nocopy.go` defines the zero-field `NoCopy` type and pointer-receiver no-op `Lock` and `Unlock` methods. `pkg/util/nocopy/BUILD.bazel` is the package's second and final artifact. `pkg/sessionctx/stmtctx/stmtctx.go` embeds the marker in a statement context, which is why accidental value copies would be a correctness warning even though the marker has no runtime state.

`rust/crates/tidb-util/src/nocopy/mod.rs` defines the zero-sized `NoCopy` type, its constructor, no-op methods, source behavior test, and a rustdoc `compile_fail` example. The current `nocopy.inventory.tsv` is legacy evidence and will be replaced by the standard ten-column content-addressed inventory generated from the repository's Go AST tool.

## Plan of Work

First, add `nocopy.artifacts.tsv` and regenerate `nocopy.inventory.tsv` from the generic Go AST inventory tool. The checker will require exactly two artifacts, zero build/platform/generated/testdata classes, three obligations, and `PORTED` rows for the type plus both methods. It will also verify every stored source hash and reject raw AST drift.

Second, update `nocopy/mod.rs`'s lockdown test to validate both artifact hashes, the standard inventory schema, the exact three-row status/symbol map, and compile-time anchors for `NoCopy`, `NoCopy::lock`, and `NoCopy::unlock`. The existing source behavior test remains the named runtime evidence, and the rustdoc compile-fail example remains the copy-prevention evidence.

Third, add `nocopy.mutation-plan.tsv` with eight independent mutations: add `Copy` to kill the rustdoc compile-fail contract, give the marker a byte field, make `lock` panic, make `unlock` panic, alter one inventory status, drift the Go source, rename the marker owner, and rename one method owner. Run each from an immutable provisional commit in a detached worktree, restore from an external byte copy, and record exit status plus restoration hash in `nocopy.mutation-results.tsv`.

Finally, add the compiled mutation receipt gate and deterministic Python checker, generate `nocopy.receipt.json`, and run the package WIP profile followed by the clean Ready profile. The final diff remains Rust, TSV, JSON, Python, and Markdown only, so `make bazel_prepare` is not required unless the actual scope expands to Go/Bazel/module/generated inputs.

## Concrete Steps

From the repository root, the exact source oracle is:

    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 go test -tags=intest,deadlock ./pkg/util/nocopy

The expected output is a successful package with `[no test files]`. The Rust behavior and copy-prevention checks are:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-347-wave11/tgt cargo nextest run --offline --locked -p tidb-util -E 'test(/nocopy/)' --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-347-wave11/tgt cargo test --offline --locked -p tidb-util --doc nocopy

The package checker will print `2 artifacts, 3 AST obligations, 3 PORTED`. Mutation work uses a separate target and never writes a scratch file under `pkg/`.

## Validation and Acceptance

Acceptance requires the two Go package artifacts to hash exactly, the generic AST inventory to remain at three rows with all rows `PORTED`, and the compiled gate to fail if any artifact, row, symbol, or source hash drifts. The source behavior test must show a zero-sized marker and callable no-op methods. The rustdoc compile-fail test must continue to reject a second use of a moved marker.

Mutation acceptance requires all eight planned mutations to produce a nonzero exit from the named rustdoc test, unit test, checker, or compile gate, followed by a passing restored run and exact source SHA-256 match. Package tests, full workspace tests, workspace Clippy, fmt, `git diff --check`, direct counts, `make -j12 lint`, remote ref, and GitHub author attribution must pass before publication is reported.

## Idempotence and Recovery

The Python checker is deterministic and its `--write` mode regenerates only the two manifest/inventory inputs. Mutation probes must save each target outside the repository, restore with `cp`, compare SHA-256, and never use `git stash`, `git reset`, or `git checkout` for restoration. Temporary probe directories are moved to `~/.Trash` after their output is recorded.

## Artifacts and Notes

Baseline source hashes at official `04cbb8663c` are:

    84ded6ce1ef07c137634b415e5aec1a39ae7b481a889e177fd5c7b05064c4561  pkg/util/nocopy/BUILD.bazel
    e02781234846ccc78f8ad9e88486d1e26c99a0ab3d113cd06639cc339cbd992d  pkg/util/nocopy/nocopy.go

The Go vet probe output was:

    copyMarker passes lock by value: probe.local/nocopy.NoCopy
    return copies lock value: probe.local/nocopy.NoCopy
    copyHolder passes lock by value: probe.local/nocopy.holder contains probe.local/nocopy.NoCopy
    return copies lock value: probe.local/nocopy.holder contains probe.local/nocopy.NoCopy

## Interfaces and Dependencies

The public Rust interface remains `tidb_util::nocopy::{NoCopy::new, NoCopy::lock, NoCopy::unlock}`. The marker is zero-sized, has no mutable state, and intentionally implements neither `Copy` nor `Clone`. The checker uses the existing generic Go AST tool and Python standard library; the Rust gate uses `sha2`, already available as a `tidb-util` dev dependency.

Security extension review: this marker adds no network, authentication, persistence, deployment, IAM, secret, or dependency surface.

Revision note: created on 2026-08-08 after selecting the complete package, measuring the Go `go test` and `go vet` boundaries, and confirming the existing Rust unit and rustdoc baselines.
