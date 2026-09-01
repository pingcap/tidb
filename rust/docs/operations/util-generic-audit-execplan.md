# `pkg/util/generic` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while the
rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

Audit the complete generic heap and synchronized-map package, including all
Go tests and Bazel inputs, and certify the Rust owner plus stats TopN consumer
at current Go master.

## Progress

- [x] (2026-09-02) Read all five Go-master artifacts in full (478 lines): two
      production files, seven heap tests, one map test, and `BUILD.bazel`.
      Confirmed no harness, fixture, generated/platform variant, nested
      package, benchmark, or other build input.
- [x] (2026-09-02) Confirmed all five files are byte-identical to Go-master
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Read the Rust heap/map owner and TopN consumer; no
      Rust-only behavior or missing Go behavior remained.
- [x] (2026-09-02) Ran current and detached Go suites and all eight Rust owner
      tests; all passed.
- [x] (2026-09-02) Ran the Ready formatting, pinned lint, and diff-hygiene
      gates. No production edit or new regression was warranted.
- [ ] Push this receipt/plan refresh to `origin/hparser-integration`, verify
      local/remote SHAs, and fetch the newest target branch before the next
      package boundary.

## Surprises & Discoveries

- `ToSortedSlice` uses signed comparator negation; preserving comparator
  magnitude avoids narrowing a Go `int` result to an ordering enum.
- `SyncMap` has no Go copy/clone policy; Rust lock-poison recovery is the
  closest native behavior and does not add a public API.

## Decision Log

- Decision: make this a receipt/plan refresh only. Rationale: source and owner
  are unchanged at current Go master and all prior behavior fixes are already
  covered by the owner suite. Date/Author: 2026-09-02, Codex.
- Decision: retain the eight source-derived Rust tests and no new regression.
  Rationale: they exactly mirror Go's seven heap cases and one map case; this
  batch introduces no code change. Date/Author: 2026-09-02, Codex.

## Validation

Exact commands and outcomes are recorded in
`rust/testport/receipts/util_generic.md`. This batch changes only Markdown, so
`make bazel_prepare` is not required.

## Outcomes & Retrospective

The generic package inventory is current at Go master with every source, test,
and build artifact accounted for. Rust remains aligned on heap/map semantics
and the canonical TopN consumer; continue with the next package.
