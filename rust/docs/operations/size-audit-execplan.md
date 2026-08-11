# Certify `pkg/util/size` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB uses `pkg/util/size` constants throughout memory accounting. Those constants describe Go's ABI, including target-word-sized headers, and must not be replaced with the layout of superficially similar Rust containers. The Rust rewrite already carries explicit Go ABI formulas, but the package has no pinned completion receipt and Go provides no unit test. After this plan is complete, both direct Go artifacts, every exported constant, 64-bit and 32-bit evidence, Rust ownership, and build integration will form one atomic package claim. No production change is expected unless an audited value differs.

## Progress

- [x] (2026-08-11 14:17Z) Fixed the two-file direct inventory at source commit `51e1e13494f5e547be10601a302d6cb9cf88ae64`; confirmed there is no `doc.go`, Go test, fixture, generated artifact, benchmark, build tag, platform variant, or failpoint use.
- [x] (2026-08-11 14:17Z) Ran the unchanged Go package and native probe; it reports no test files and the complete 64-bit constant table expected by the Rust implementation.
- [x] (2026-08-11 14:17Z) Cross-compiled the Go source plus symbol arrays for `linux/386`; object symbol sizes prove the complete 32-bit layout table.
- [x] (2026-08-11 14:17Z) Reviewed the Rust formulas, passed the focused test, and compiled dependency-free native and Windows 64-bit assertions; no production or test change is required.
- [x] (2026-08-11 14:17Z) Added the atomic semantic receipt and this living plan.
- [x] (2026-08-11 14:22Z) Completed the pre-sync Ready profile: the receipt, formatting, complete `tidb-util` suite, all-target Clippy with warnings denied, and repository lint all pass; the Bazel prepare gate has no trigger.
- [x] (2026-08-11 14:26Z) Rebased the one-package commit onto remote `0d3eaa40ef449568529a836a2717ccc811fa4695` and repeated the complete post-sync Ready profile successfully.
- [ ] Publish the linear commit without force and verify the freshly fetched remote SHA.

## Surprises & Discoveries

- Observation: the source package has no Go unit test despite its wide use in memory accounting.
  Evidence: direct inventory contains only `size.go` and `BUILD.bazel`; `go test -tags=intest,deadlock -count=1` reports `[no test files]`. The audit therefore uses source-derived probes instead of inventing an accepted test artifact.

- Observation: Go's 32-bit ABI can be verified without executing a foreign binary.
  Evidence: compiling package-level byte arrays whose lengths are each source constant for `GOOS=linux GOARCH=386`, then reading their object symbol sizes, yields slice 12, string/interface 8, pointer/int/uint/function/map 4, byte/bool/uint8 1, int32 4, and float64/uint64/int64 8.

- Observation: the local Rust target listing and compiler sysroot disagree for FreeBSD.
  Evidence: `rustup target list --installed` lists `x86_64-unknown-freebsd`, but direct `rustc --target x86_64-unknown-freebsd` reports E0463 because it cannot find `std`. Native and Windows metadata compilation pass; the formulas contain no OS branch, so the unverified dimension is toolchain packaging rather than package logic.

- Observation: the synchronized branch removed the shared semantic-package gate.
  Evidence: commit `3353b29fb4aa697665ed38586fe5f50ef87fba6a` deleted the script and older receipts. This receipt can be checked with the last script version read directly from Git without restoring the removed global mechanism.

## Decision Log

- Decision: Preserve the existing explicit Go ABI constants and make no production change.
  Rationale: using `usize` only to derive Go's word size, then applying Go header formulas, matches native 64-bit and cross-compiled 32-bit evidence. Replacing these with `size_of` on Rust slices, strings, trait objects, maps, or functions would silently measure the wrong runtime.
  Date/Author: 2026-08-11 / Codex

- Decision: Publish a receipt-only package commit.
  Rationale: every production constant and the focused Rust test already align. The missing artifacts are the current source pin, complete inventory, cross-width evidence, integration decision, and validation record.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The direct inventory, source pin, no-test Go oracle, complete native constant table, complete 386 object evidence, Rust source review, focused test, two 64-bit compile-time checks, receipt, one-package commit, synchronization, and both Ready validation passes are complete. The non-force push and remote-SHA verification remain. Correctness risk is limited to future ABI changes in Go, which the source pin and width-derived tests make explicit; compatibility risk is none because no API changes; performance risk is none because every value is compile-time constant.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/size/BUILD.bazel` and `pkg/util/size/size.go` at `51e1e13494f5e547be10601a302d6cb9cf88ae64`. It exports binary units from `KB` through `PB` and 15 `unsafe.Sizeof` results used by memory accounting. Slice is three machine words; string and interface are two; pointer, architecture-width integers, function values, and map values are one. Byte, bool, uint8, int32, float64, uint64, and int64 have fixed sizes.

Rust owns the mapping in `rust/crates/tidb-util/src/size/mod.rs`, exported by the unchanged `rust/crates/tidb-util/src/lib.rs`. `WORD_SIZE` comes from Rust target `usize`, but every exported constant describes the Go type named in its documentation. The unit test checks all five binary units and all 15 layout constants against the word-size formulas.

## Milestones

The oracle milestone fixes accepted bytes and proves both width domains. Run the empty Go package, the native value probe, and the 386 object-symbol probe. Acceptance is the exact tables recorded below, with no repository change.

The parity milestone reviews every Rust constant rather than spot-checking consumers. Run the focused unit test and dependency-free compile-time assertions for available 64-bit targets. Acceptance is no mismatch and a documented no-code-change decision; unavailable FreeBSD `std` remains explicit.

The publication milestone adds only the receipt and plan, runs complete Ready gates for `tidb-util`, checks Bazel triggers, synchronizes, and pushes one linear commit. Acceptance is matching local and freshly fetched remote SHAs.

## Plan of Work

Do not alter `rust/crates/tidb-util/src/size/mod.rs` unless a probe differs. Add `rust/crates/tidb-util/tests/size.semantic.toml` with the two accepted source artifacts, current source commit, existing owner files, and focused test command. Maintain this plan with exact validation evidence.

Run complete `tidb-util` unit/integration/doctests, formatting, all-target Clippy with warnings denied, and repository lint. The diff adds no Go/Bazel/module/Rust production change, so `make bazel_prepare` is not required unless synchronization creates a trigger.

## Concrete Steps

From `pkg/util/size`, run the unchanged package without failpoints:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -tags=intest,deadlock -count=1

From `rust`, run focused and complete Rust gates:

    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib 'size::tests'
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings

From repository root, validate the receipt and lint recipe:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/size.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint

## Validation and Acceptance

The native Go probe must report units `1024,1048576,1073741824,1099511627776,1125899906842624` and layout `24,1,16,1,8,16,8,8,4,8,1,8,8,8,8`. The 386 object must report `12,1,8,1,4,8,8,8,4,4,1,4,4,8,4` in the same source order. Rust formulas and focused test must match both width tables.

The receipt must accept exactly two pinned Go artifacts and one focused command. Complete owning-crate tests, formatting, all-target Clippy, and lint must pass. The final diff must contain only this receipt and plan. Publication must be one package commit based on latest `hparser-integration`, pushed without force, with matching local and remote SHAs.

## Idempotence and Recovery

All checks are safe to rerun. Probe inputs and objects live under `/tmp/tidb-size-*` and never enter the repository. FreeBSD compilation is not retried unless a working target `std` becomes available. If the phony `revive` install cannot reach the Go proxy, reuse a verified `revive v1.2.1`, suppress only that install target with Make's `-o`, and remove the ignored copy afterward. If remote advances, rebase the one local commit and repeat Ready validation.

## Artifacts and Notes

Go package and native probe:

    ? github.com/pingcap/tidb/pkg/util/size [no test files]
    units=1024,1048576,1073741824,1099511627776,1125899906842624
    layout=24,1,16,1,8,16,8,8,4,8,1,8,8,8,8

Go 386 object symbol sizes in source order:

    layout=12,1,8,1,4,8,8,8,4,4,1,4,4,8,4

Focused Rust evidence:

    size unit test: 1 passed; 0 failed
    native and x86_64-pc-windows-msvc compile-time assertions: pass
    x86_64-unknown-freebsd assertion: not compiled; target std unavailable (E0463)

Pre-sync Ready evidence:

    semantic receipt: 1 package, 1 unique command
    cargo fmt --all --check: pass
    complete tidb-util suite: 336 passed, 0 failed, 1 ignored; integration tests and doctest pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; no Go, Bazel, module, or target-list trigger

Post-sync Ready evidence on remote base `0d3eaa40ef449568529a836a2717ccc811fa4695`:

    Go package: pass; no test files
    semantic receipt: 1 package, 1 unique command
    cargo fmt --all --check: pass
    complete tidb-util suite: 336 passed, 0 failed, 1 ignored; integration tests and doctest pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; the one-package diff adds only the receipt and plan

Failpoint decision:

    no failpoint., testfailpoint., or Bazel failpoint dependency match

## Interfaces and Dependencies

`rust/crates/tidb-util/src/size/mod.rs` retains `KB`, `MB`, `GB`, `TB`, `PB`, and the 15 `SIZE_OF_*` public constants. It derives target word size from `usize`, adds no runtime state and no dependency, and remains exported by `rust/crates/tidb-util/src/lib.rs`.

Plan revision note: created after current-source inventory pinning, source/consumer review, failpoint inspection, the absent-test Go oracle, native and 386 probes, Rust target compile checks, focused unit validation, and the no-code-change decision.
