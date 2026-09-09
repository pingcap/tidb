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
- [x] (2026-09-05) Removed six Rust-only `#[must_use]` diagnostics from the
      source-shaped heap/map constructors and observers. The deny-on-discard
      regression failed with exactly six diagnostics on the detached pre-fix
      owner and passes with nine focused owner tests.
- [x] (2026-09-05) Published the one-package commit to
      `origin/hparser-integration`, verified matching local/remote SHAs, and
      fetched the newest target branch before the next package boundary.

## Surprises & Discoveries

- `ToSortedSlice` uses signed comparator negation; preserving comparator
  magnitude avoids narrowing a Go `int` result to an ordering enum.
- `SyncMap` has no Go copy/clone policy; Rust lock-poison recovery is the
  closest native behavior and does not add a public API.

## Decision Log

- Decision: remove explicit `#[must_use]` from the six source-shaped heap/map
  methods while retaining the eight source-derived tests and adding one
  package-level discard-contract regression. Rationale: Go allows callers to
  discard these results, and the regression verifies the Rust diagnostics are
  gone without changing runtime behavior. Date/Author: 2026-09-05, Codex.

## Validation

Exact commands and outcomes are recorded in
`rust/testport/receipts/util_generic.md`. The Rust-only source change leaves
Go/Bazel/module inputs untouched, so `make bazel_prepare` is not required.

## Outcomes & Retrospective

The generic package inventory is current at Go master with every source, test,
and build artifact accounted for. Rust remains aligned on heap/map semantics,
the discardable-return contract, and the canonical TopN consumer; continue
with the next package.
