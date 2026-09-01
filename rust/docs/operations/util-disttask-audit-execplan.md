# `pkg/util/disttask` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while the
rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

Audit the complete distributed-task executor-ID helper package and certify its
Rust domain owner against all Go production, test, and build artifacts.

## Progress

- [x] (2026-09-02) Read all three Go-master artifacts in full: `idservice.go`,
      `idservice_test.go`, and `BUILD.bazel` (133 lines total). Confirmed one
      source test and no harness, fixtures, generated/platform variants,
      nested package, or other build input.
- [x] (2026-09-02) Confirmed all three files are byte-identical to Go-master
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Read `tidb-domain::disttask` and its infosync/server-info
      seams; no Rust-only behavior or missing Go behavior remained.
- [x] (2026-09-02) Ran current and detached Go tests and the Rust source-vector
      test; all passed.
- [x] (2026-09-02) Ran the Ready formatting, pinned lint, and diff-hygiene
      gates. No production edit or new regression was warranted.
- [ ] Push this receipt/plan refresh to `origin/hparser-integration`, verify
      local/remote SHAs, and fetch the newest target branch before the next
      package boundary.

## Surprises & Discoveries

- `GenerateSubtaskExecID` is a live infosync lookup while the `4Test` variant
  reads an explicit mock map; keeping those seams separate avoids a Rust-only
  global manager.
- `net.JoinHostPort` brackets any colon-containing host, including the source's
  deliberately unusual IPv6 test vector.

## Decision Log

- Decision: make this a receipt/plan refresh only. Rationale: the complete
  source and owner review found no current drift or missing behavior; a code
  edit would be speculative. Date/Author: 2026-09-02, Codex.
- Decision: retain the existing one source-derived vector test. Rationale: it
  covers every Go test case while owner consumers remain outside this atomic
  package claim. Date/Author: 2026-09-02, Codex.

## Validation

Exact commands and outcomes are recorded in
`rust/testport/receipts/util_disttask.md`. This batch changes only Markdown, so
`make bazel_prepare` is not required.

## Outcomes & Retrospective

The disttask inventory is current at Go master, with production, source test,
and Bazel target fully accounted for. Rust remains aligned on ID formatting,
lookup sentinels, and infosync boundaries; continue with the next package.
