# Close the accepted Chunk direct-test artifact

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root.

## Purpose / Big Picture

The accepted `pkg/util/chunk/chunk_test.go` is the direct source test for the Chunk public contract. This increment closes that complete artifact without reproducing Go's benchmark runner. Normal tests must prove row and batch append, projection, truncation, required-row control, selection extension, all-type comparison and copy, decimal shape, memory accounting, shared-column identity, and text output. Benchmark-only allocation reporting, timer control, `testing.B.N`, and benchmark case-name machinery are classified as evidence-backed DECLINED because they do not affect TiDB behavior; the same production operations remain covered by deterministic semantic tests.

This remains one test artifact inside the incomplete `pkg/util/chunk` package and is not a whole-package completion claim.

## Progress

- [x] (2026-08-10) Read all 1,204 accepted source lines and all 642 ledger obligations at source commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`.
- [x] (2026-08-10) Mapped all 14 normal tests to existing Rust production methods, public contract tests, and previously killed semantic rules.
- [ ] Add the compact source-shaped public contract and prove the unchanged implementation passes it.
- [ ] Classify all 642 obligations and advance the incremental checker to the next artifact.
- [ ] Run Ready validation, integrate current `hparser-integration`, and push the checkpoint to both authorized remotes.

## Surprises & Discoveries

- Observation: all 14 accepted normal-test identities already have production support and focused Rust coverage, but that coverage is split among `chunk.rs`, `chunk_identity_tests.rs`, `chunk_core_contract.rs`, and `compare_vectors.rs`.
  Evidence: the Rust test inventory covers required rows, physical append despite selection, truncate, all Go field types, decimal metadata, exact memory usage, column identity/swap, projection, and rendering.

- Observation: 20 benchmark identities and their helper structs/closures dominate the artifact ledger without defining a user-visible result.
  Evidence: every benchmark-only root is reached exclusively from `testing.B`, and its extra operations are allocation reporting, timer control, iteration-count control, case naming, or printing an anti-optimization accumulator. The underlying append/access/grow/memory operations are exercised by normal semantic tests.

## Decision Log

- Decision: preserve deterministic semantic workload boundaries but decline Go benchmark runtime mechanics.
  Rationale: the goal is semantic equality, not rebuilding Go's standard library or test runner. No query result, wire/disk image, error, quota, lifecycle, or public package return value depends on `testing.B.N`, `ReportAllocs`, `ResetTimer`, or sub-benchmark names.
  Date/Author: 2026-08-10 / Codex.

- Decision: reuse the existing killed production rules rather than create duplicate mutations for the direct-test artifact.
  Rationale: the direct test is evidence for the same Chunk, Row, compare, and render behaviors already registered under `R_CHUNK_CORE_SEMANTICS`, `R_ROW_CORE_DATUM`, `R_CHUNK_COMPARE_FIELD_DISPATCH`, and `R_CHUNK_RENDER`. New source-shaped public tests broaden the boundary evidence; duplicating identical mutations would add runtime without new fault-detection power.
  Date/Author: 2026-08-10 / Codex.

## Outcomes & Retrospective

Pending classification and validation.

## Context and Orientation

The accepted authority is `pkg/util/chunk/chunk_test.go` at `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`. Its 14 normal tests are `TestAppendRow`, `TestAppendChunk`, `TestTruncateTo`, `TestChunkSizeControl`, `TestCompare`, `TestCopyTo`, `TestGetDecimalDatum`, `TestChunkMemoryUsage`, `TestSwapColumn`, `TestAppendSel`, `TestMakeRefTo`, `TestToString`, `TestAppendRows`, and `TestAppendRowsByColIdxs`.

The Rust production owner is `rust/crates/tidb-chunk/src/chunk.rs`, with identity tests in `chunk_identity_tests.rs`, comparison behavior in `compare.rs`, and datum conversion in `row.rs`. Public boundary targets are `chunk_core_contract`, `compare_vectors`, and the new `chunk_source_test_contract`.

## Plan of Work

Add one compact public integration target with three source-shaped groups: row/batch append and projection across NULL, integer, arbitrary bytes, decimal, and JSON values; required-row, selection, append, and truncation state transitions; and copy/decimal/memory/identity behavior. Keep comparison and text evidence in their existing exact public targets.

Classify normal-test identities, assertions, rows, entered loops, reachable branches, switch cases, and semantic helpers as PORTED. Classify zero-iteration arms under fixed-positive accepted bounds as UNREACHABLE with one structural proof. Classify benchmark-only roots and support as DECLINED with a proof that records both their lack of production reachability and the deterministic semantic tests that cover their underlying operations.

## Concrete Steps

From `rust/`:

    cargo fmt --all -- --check
    cargo test --offline --locked -j12 -p tidb-chunk --test chunk_source_test_contract -- --nocapture
    cargo test --offline --locked -j12 -p tidb-chunk --test chunk_core_contract
    cargo test --offline --locked -j12 -p tidb-chunk --test compare_vectors
    cargo clippy --offline --locked -j12 -p tidb-chunk --all-targets -- -D warnings

The incremental receipt checker must advance past `chunk_test.go.tsv` and stop only on the next unrelated UNCLASSIFIED artifact.

## Validation and Acceptance

Every normal-test obligation has a final PORTED or structurally justified UNREACHABLE verdict. Every benchmark-only obligation has a DECLINED verdict tied to the accepted source quote and checked-in benchmark-boundary proof. No benchmark runtime mechanism is described as production parity. Public tests prove the underlying production values and state transitions, and previously executed mutations remain valid for each reused semantic rule.

## Idempotence and Recovery

This artifact needs no production mutation execution. Ledger rewriting is mechanical and must preserve obligation identity columns byte-for-byte. Before commit, compare the first seven TSV columns against `HEAD`, verify final counts, run the receipt checker, and restore the ledger from Git if any identity mismatch appears.

## Artifacts and Notes

Initial ledger:

    obligations   642
    UNCLASSIFIED  642

## Interfaces and Dependencies

No production interface or crate dependency changes. The new test uses only public `tidb-chunk` and `tidb-datatype` APIs already available to the crate's integration tests.
