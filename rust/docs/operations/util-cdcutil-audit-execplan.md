# `pkg/util/cdcutil` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while the
rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

Audit the complete Go `pkg/util/cdcutil` package as one atomic unit, including
its embedded-etcd test and test-only export, and verify the Rust owner against
the same key/state/checkpoint contract.

## Progress

- [x] (2026-09-02) Read all four Go-master artifacts in full: `cdc.go`,
      `cdc_test.go`, `export_for_test.go`, and `BUILD.bazel` (489 lines total).
      Confirmed no fixture, testdata, benchmark, fuzz target, example,
      generated/platform variant, nested package, or extra build input.
- [x] (2026-09-02) Confirmed all four files are byte-identical to Go-master
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Read the Rust `tidb-domain::cdcutil` owner and its ordinary
      `EtcdOps` integration in full; no Rust-only behavior or missing Go
      behavior remained.
- [x] (2026-09-02) Ran current and detached Go embedded-etcd tests and the
      source-derived Rust matrix; all passed.
- [x] (2026-09-02) Ran the Ready formatting, pinned lint, and diff-hygiene
      gates. No production edit or new regression was warranted.
- [ ] Push this receipt/plan refresh to `origin/hparser-integration`, verify
      local/remote SHAs, and fetch the newest target branch before the next
      package boundary.

## Surprises & Discoveries

- `BUILD.bazel` marks the Go embedded-etcd test flaky, but the package has no
  failpoint use; the canonical failpoint wrapper is therefore not applicable.
- The Rust owner already uses the production etcd abstraction rather than a
  CDC-specific fake; its in-memory test is source-shaped and covers both key
  generations.

## Decision Log

- Decision: make this a receipt/plan refresh only. Rationale: the complete
  source and owner review found no Rust-only behavior or missing Go behavior;
  changing code would be speculative. Date/Author: 2026-09-02, Codex.
- Decision: retain the existing source-derived Rust matrix as the regression
  carrier. Rationale: it mirrors Go's sole embedded-etcd test while avoiding a
  second production or test-only CDC facade. Date/Author: 2026-09-02, Codex.

## Validation

Exact commands and outcomes are recorded in
`rust/testport/receipts/util_cdcutil.md`. This batch changes only Markdown, so
`make bazel_prepare` is not required.

## Outcomes & Retrospective

The cdcutil inventory is current at Go master and its receipt now records all
production, test, support, and build artifacts plus Ready evidence. The Rust
owner remains source-aligned; continuing work proceeds with the next package.
