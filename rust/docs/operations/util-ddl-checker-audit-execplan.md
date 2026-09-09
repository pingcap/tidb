# `pkg/util/ddl-checker` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while the
rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

Audit the complete Go DDL-checker utility, including its tagged source tests
and Bazel target, and record the explicit Rust ownership boundary when no
dependency-closed owner exists.

## Progress

- [x] (2026-09-02) Read all four Go-master artifacts in full (351 lines): two
      production files, two source tests, and `BUILD.bazel`. Confirmed the
      `intest` tag requirement and no fixtures, generated/platform variants,
      nested package, or other build input.
- [x] (2026-09-02) Confirmed all four files are byte-identical to Go-master
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Reviewed parser, mockstore/session, DDL, and DB-syncer Rust
      seams; none form a dependency-closed owner for this helper package.
- [x] (2026-09-02) Ran current and detached tagged Go source tests; both
      `TestParse` and `TestExecute` passed.
- [x] (2026-09-02) Ran the Ready formatting, pinned lint, and diff-hygiene
      gates. No Rust production edit or replacement regression was warranted.
- [ ] Push this receipt/plan refresh to `origin/hparser-integration`, verify
      local/remote SHAs, and fetch the newest target branch before the next
      package boundary.

## Surprises & Discoveries

- The untagged Go package test intentionally fails its repository test guard;
  `-tags=intest` is the source-defined prerequisite and both tagged tests pass.
- The checker crosses several package boundaries (parser/session/mockstore,
  AST, and upstream `database/sql`), so a partial Rust port would be a
  Rust-only execution path rather than package parity.

## Decision Log

- Decision: keep this package explicitly unclaimed. Rationale: no
  dependency-closed Rust owner exists, and inventing a checker facade would
  violate the atomic package rule. Date/Author: 2026-09-02, Codex.
- Decision: retain the Go tagged tests as the only regression evidence.
  Rationale: there is no Rust implementation to test and the source matrix
  already covers parser/classifier/executor behavior. Date/Author: 2026-09-02,
  Codex.

## Validation

Exact commands and outcomes are recorded in
`rust/testport/receipts/util_ddl_checker.md`. This batch changes only Markdown,
so `make bazel_prepare` is not required.

## Outcomes & Retrospective

The DDL-checker inventory is current at Go master and its cross-package Rust
boundary is explicit. No speculative checker or upstream synchronizer was
added; future work must land the complete parser/session/mockstore/DB graph as
one atomic package unit.
