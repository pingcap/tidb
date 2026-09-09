# `pkg/util/sli` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while the
rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

Audit the complete Go `pkg/util/sli` package as one atomic unit, compare its
production accumulator with the Rust owner and all ordinary integration seams,
and leave an explicit receipt even when the audit finds no code delta.

## Progress

- [x] (2026-09-02) Read both Go-master artifacts in full: `sli.go` and
      `BUILD.bazel` (132 lines total). Confirmed no package test, fixture,
      testdata, benchmark, fuzz target, generated/platform variant, nested
      package, or extra build input.
- [x] (2026-09-02) Confirmed the two Go files are byte-identical to
      `origin/master` authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Read the Rust `tidb-util` owner and traced the session,
      executor, cluster, real-TiKV, and text/prepared dispatch seams used by
      the source-derived SLI regression.
- [x] (2026-09-02) Ran current and detached Go package tests, then the existing
      source-derived Rust SLI integration regression; all passed.
- [x] (2026-09-02) Ran the Ready formatting, pinned lint, and diff-hygiene
      gates. No source change was needed, so no package-local regression was
      added.
- [ ] Push this documentation batch to `origin/hparser-integration`, verify
      local/remote SHAs, and fetch the newest target branch before the next
      package boundary.

## Surprises & Discoveries

- The package is test-free in Go, but its production `BUILD.bazel` declares a
  failpoint dependency. That affects runtime ownership, not Go unit-test
  wrapper selection.
- The Rust owner had already removed the earlier simulator and synthetic
  observation surface; the real integration regression exercises the ordinary
  commit/scan reporting path.

## Decision Log

- Decision: make this a receipt/plan refresh only. Rationale: complete source
  and integration review found no Rust-only behavior or missing Go behavior;
  changing code or inventing a regression would diverge from the source.
  Date/Author: 2026-09-02, Codex.
- Decision: retain the existing source-derived integration regression as the
  focused proof. Rationale: the Go package has no package-local tests, while
  the observable contract is consumed by session and executor owners. Date/
  Author: 2026-09-02, Codex.

## Validation

The exact Ready commands and outcomes are recorded in
`rust/testport/receipts/util_sli.md`. Because this batch changes only Markdown,
`make bazel_prepare` is not required.

## Outcomes & Retrospective

The complete Go SLI inventory is now current at Go master and its Rust owner
has an explicit no-delta parity result. The receipt captures the package's
failpoint/build boundary, source-free test surface, existing cross-package
regression, and repository Ready gates. Remaining work continues with the
next un-audited package.
