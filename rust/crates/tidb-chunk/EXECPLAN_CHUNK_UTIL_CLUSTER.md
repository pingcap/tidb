# Close the accepted chunk-util semantic cluster

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root.

## Purpose / Big Picture

Close accepted `pkg/util/chunk/chunk_util.go` together with its direct `chunk_util_test.go` artifact. The semantic unit is the bulk selected-row copy contract, shared-column swap authority, and checksum/AES spill-file adapter, not two independent files. Successful completion gives every production and direct-test obligation a final verdict, proves the direct Rust consumers, and does not reproduce Go's benchmark runner or runtime allocation details.

This remains one cluster inside the incomplete `pkg/util/chunk` whole-package claim.

## Progress

- [x] (2026-08-10) Read all 410 production lines, all 271 direct-test lines, and all 235 ledger obligations at source commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`.
- [x] (2026-08-10) Mapped the Rust production owner, direct `tidb-expr`/`tidb-executor` swap consumers, spill callers, existing tests, and receipt state.
- [x] (2026-08-10) Added a deterministic partial-write regression: baseline accepted 2 of 4 bytes as success; the root fix returns the subsequent storage error while retaining the accepted two-byte logical offset.
- [ ] Add one public cluster contract for column copies, join copies, alias-aware swaps, and plaintext/AES spill-file behavior.
- [ ] Classify all 235 obligations and direct helper contracts, execute any new mutations/probes, and advance the incremental checker.
- [ ] Include the cluster in the next batched Ready validation and authorized dual-remote push.

## Surprises & Discoveries

- Observation: the Rust checksum and encryption writers correctly follow `std::io::Write` by returning partial progress and reporting a latched error on the next call. `DiskFileReaderWriter::write` previously called them only once, so row spill could accept a short payload as success.
  Evidence: the fail-before unit regression returned `Ok(2)` for a four-byte payload; after the loop fix it returns `StorageFull` and `off_write == 2`.

- Observation: `chunk_util.go` and `chunk_util_test.go` are one semantic cluster. The test's normal cases cover the production copy and alias paths; only Go `testing.B` timing/allocation machinery is outside the behavior contract.
  Evidence: all three benchmarks invoke the same copy/append operations as the normal tests, adding only `B.N`, timer reset, and allocation reporting.

## Decision Log

- Decision: make `DiskFileReaderWriter::write` an exact logical write boundary.
  Rationale: Go can return `(partial, error)` in one call; Rust reports those across two `Write::write` calls. Looping until completion or error preserves the observable spill result and exact accepted-prefix offset without emulating Go's interface mechanics.
  Date/Author: 2026-08-10 / Codex.

- Decision: close production and direct-test ledgers in one guarded transformation.
  Rationale: this eliminates duplicate source mapping, test construction, checker cycles, and validation while retaining immutable obligation identities and separate evidence per semantic rule.
  Date/Author: 2026-08-10 / Codex.

- Decision: decline benchmark-runner mechanics while porting their workload results.
  Rationale: `testing.B.N`, timer and allocation reporting do not change SQL results, public package returns, spill state, or consumer behavior. Deterministic tests execute the same production operations.
  Date/Author: 2026-08-10 / Codex.

## Outcomes & Retrospective

Pending cluster tests, receipt closure, and validation.

## Context and Orientation

Production authority is `pkg/util/chunk/chunk_util.go`. Rust ownership is `rust/crates/tidb-chunk/src/chunk_util.rs`, with shared-column identity in `column_slot.rs`/`chunk.rs`, spill layers in `tidb-util`, and callers in row/chunk disk containers. Direct swap consumers are `tidb-expr/src/evaluator.rs` and `tidb-executor/src/limit.rs`.

The accepted direct tests are `TestCopySelectedJoinRows`, `TestCopySelectedJoinRowsWithoutSameOuters`, `TestCopySelectedJoinRowsDirect`, `TestCopySelectedVirtualNum`, and `TestMergeInputIdxToOutputIdxes`, plus three benchmarks.

## Plan of Work

Create one public target with three boundaries: fixed/variable column copies including row-id remapping and expected-result inversion; join copies covering empty, selection-error, virtual, direct, and same-outer paths; and shared-owner swap plus plaintext/AES spill-file behavior. Keep the injected partial-write regression as a unit test because failure injection is intentionally private.

Register semantic symbols/rules for selected copies, swap identity/cache, spill-file initialization/read/write/error behavior, and direct source tests. Reuse killed rules only where the same behavior is already registered; add a mutation for the new exact-write loop. Use one measured probe for benchmark-only obligations and structural proofs only for source-test arms that fixed positive inputs cannot reach.

## Concrete Steps

From `rust/`:

    cargo fmt --all -- --check
    cargo test --offline --locked -j12 -p tidb-chunk --test chunk_util_contract -- --nocapture
    cargo test --offline --locked -j12 -p tidb-chunk chunk_util::disk_writer_failure_tests::a_partial_spill_write_is_followed_until_the_latched_error --lib -- --exact
    cargo clippy --offline --locked -j12 -p tidb-chunk --all-targets -- -D warnings

Then run the incremental package checker at accepted source commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`. It must pass both ledgers and stop only at the next unrelated artifact/helper boundary.

## Validation and Acceptance

The exact-write regression must fail before and pass after. Fixed and variable copies preserve order, NULLs, row-id mapping, and virtual row counts. Selection errors are atomic. Swap output aliases remain one live owner and the cached mapping is safe under concurrent first use. Plaintext and AES authorities preserve exact live-cache reads, offsets, cleanup, and unopened errors. Every obligation has a final evidence-backed verdict; benchmark mechanics are never claimed as production parity.

## Idempotence and Recovery

Ledger transformation must compare the first seven TSV columns byte-for-byte before commit. Evidence runners are idempotent and their ignored logs must be force-added explicitly. Restore either ledger from `HEAD` if identity or count checks fail.

## Interfaces and Dependencies

No new runtime dependency. `DiskFileReaderWriter::write` retains `io::Result<usize>` and now guarantees `Ok(n)` means the entire input was accepted. Existing callers become safer without API migration.
