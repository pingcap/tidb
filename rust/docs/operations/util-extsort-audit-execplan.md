# `pkg/util/extsort` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while the
rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

Audit the complete Pebble-backed external sorter package and record why it
remains explicitly unclaimed until its Lightning/Pebble dependency graph can
land atomically.

## Progress

- [x] (2026-09-02) Read all five Go-master artifacts in full (2,667 lines):
      `disk_sorter.go`, `external_sorter.go`, both source-test files, and
      `BUILD.bazel`. Confirmed 16 source tests and no fixtures, generated or
      platform variants, nested package, benchmark, fuzz target, or extra
      harness.
- [x] (2026-09-02) Confirmed the package is byte-identical to Go-master
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Reviewed Rust crates and consumers; executor row-spill
      sorting and BR range merging are not substitutes for Pebble SST sorting
      or the Lightning duplicate-detector contract.
- [x] (2026-09-02) Ran current and detached complete Go source suites; all 16
      tests passed in both checkouts.
- [x] (2026-09-02) Ran the Ready formatting, pinned lint, and diff-hygiene
      gates. No Rust source owner or regression could be added safely.
- [ ] Push this receipt/plan refresh to `origin/hparser-integration`, verify
      local/remote SHAs, and fetch the newest target branch before the next
      package boundary.

## Surprises & Discoveries

- The package's contract includes SST file naming/markers, reader reference
  counts, crash recovery, histogram-guided compaction, and duplicate-key
  semantics; a generic row sorter would miss all of these.
- The Go source has no generated/platform variants despite its Pebble and
  Lightning integration; all five direct artifacts form one atomic unit.

## Decision Log

- Decision: keep `pkg/util/extsort` explicitly unclaimed. Rationale: Rust has
  no Pebble/SSTable or Lightning importer owner, and a partial sorter would be
  Rust-only behavior. Date/Author: 2026-09-02, Codex.
- Decision: retain the complete Go suite as evidence and add no Rust test.
  Rationale: there is no dependency-closed Rust implementation to exercise.
  Date/Author: 2026-09-02, Codex.

## Validation

Exact commands and outcomes are recorded in
`rust/testport/receipts/util_extsort.md`. This batch changes only Markdown, so
`make bazel_prepare` is not required.

## Outcomes & Retrospective

The external-sort inventory is current at Go master and the ownership boundary
is explicit. Future work must land Pebble/SST, Lightning importer, duplicate
detector, and crash-recovery behavior together before any Rust production port.
