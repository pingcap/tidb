# `pkg/util/deadlockhistory` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while the
rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

Audit all production, test, harness, and build artifacts in Go's
`pkg/util/deadlockhistory` as one atomic package and verify the Rust executor
owner at the same package boundaries.

## Progress

- [x] (2026-09-02) Read all four Go-master artifacts in full:
      `deadlock_history.go`, `deadlock_history_test.go`, `main_test.go`, and
      `BUILD.bazel` (669 lines total). Confirmed four source test identities
      and no fixtures, generated/platform variants, nested package, or other
      build input.
- [x] (2026-09-02) Confirmed all four files are byte-identical to Go-master
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Read the Rust executor owner and traced its session and
      executor consumers; no Rust-only behavior or missing Go behavior
      remained.
- [x] (2026-09-02) Ran current and detached Go suites and the four source-
      derived Rust owner tests; all passed.
- [x] (2026-09-02) Ran the Ready formatting, pinned lint, and diff-hygiene
      gates. No production edit or new regression was warranted.
- [ ] Push this receipt/plan refresh to `origin/hparser-integration`, verify
      local/remote SHAs, and fetch the newest target branch before the next
      package boundary.

## Surprises & Discoveries

- Go's `main_test.go` supplies common setup and leak checking but no additional
  behavior; the Rust aggregate suite needs no runtime-worker analogue.
- Package-level datum rendering intentionally leaves `CURRENT_SQL_DIGEST_TEXT`
  and `KEY_INFO` for the ordinary session information-schema owner; retaining
  a second Rust renderer would cross the Go package boundary.

## Decision Log

- Decision: make this a receipt/plan refresh only. Rationale: the complete
  source and Rust owner review found no current drift or missing behavior;
  changing production code would be speculative. Date/Author: 2026-09-02,
  Codex.
- Decision: retain the four existing source-derived Rust tests as the
  regression carrier. Rationale: they mirror Go's collection, datum,
  conversion, and resize identities without creating duplicate package
  policy. Date/Author: 2026-09-02, Codex.

## Validation

Exact commands and outcomes are recorded in
`rust/testport/receipts/util_deadlockhistory.md`. This batch changes only
Markdown, so `make bazel_prepare` is not required.

## Outcomes & Retrospective

The deadlock-history inventory is current at Go master and records every source,
test, harness, and build artifact. The Rust owner remains aligned on bounded
history semantics and package ownership; further work continues at the next
un-audited boundary.
