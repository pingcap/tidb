# `pkg/util/domainutil` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while the
rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

Audit Go's process-global repair registry as one atomic package, account for
its sole production and build artifacts, and verify the Rust domain owner at
the same infoschema/planner/DDL boundaries.

## Progress

- [x] (2026-09-02) Read both Go-master artifacts in full: `repair_vars.go` and
      `BUILD.bazel` (207 lines total). Confirmed no package test, harness,
      fixture, generated/platform variant, nested package, or other build
      input.
- [x] (2026-09-02) Confirmed both files are byte-identical to Go-master
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Read `tidb-domain::domainutil` and traced its ordinary
      startup, infoschema, planner, and DDL seams; no Rust-only behavior or
      missing Go behavior remained.
- [x] (2026-09-02) Ran current and detached Go no-test package checks and the
      Rust domain owner compile; all passed.
- [x] (2026-09-02) Ran the Ready formatting, pinned lint, and diff-hygiene
      gates. No production edit or new regression was warranted.
- [ ] Push this receipt/plan refresh to `origin/hparser-integration`, verify
      local/remote SHAs, and fetch the newest target branch before the next
      package boundary.

## Surprises & Discoveries

- The package has no Go tests; behavior is exercised through infoschema and
  DDL consumers, so a second test-only registry would violate package
  ownership.
- Go lowercases the caller's repair list in place before locking it; the Rust
  owner preserves the same observable stored-list semantics with its owned
  vector.

## Decision Log

- Decision: make this a receipt/plan refresh only. Rationale: source and Rust
  owner are unchanged at current Go master and no missing behavior was found;
  a code edit would be speculative. Date/Author: 2026-09-02, Codex.
- Decision: rely on existing downstream session/infoschema/DDL regressions
  rather than add a package-local test absent from Go. Rationale: the package
  has no source test and its contract is process-global integration state.
  Date/Author: 2026-09-02, Codex.

## Validation

Exact commands and outcomes are recorded in
`rust/testport/receipts/util_domainutil.md`. This batch changes only Markdown,
so `make bazel_prepare` is not required.

## Outcomes & Retrospective

The domainutil inventory is current at Go master and records its production and
build boundary. Rust remains aligned on repair-mode state, lowercasing,
quarantine and removal behavior; continue with the next package.
