# Spill merge-join duplicate groups without materializing them as datum vectors

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan must be maintained according to it.

## Purpose / Big Picture

After this change, a merge join whose inner side contains one very large run of an equal key can finish under a bounded statement memory quota by moving that run to the configured spill store. The result rows, ordering, residual `ON` filtering, outer-join padding, disk errors, quota accounting, and cleanup remain the same as an unspilled merge join. The observable proof is a duplicate-key join that currently fails with the query-memory error under a 64 KiB quota and afterwards spills, returns the exact unbounded-control result, and releases its spill action and trackers on close.

The implementation preserves TiDB semantics without copying Go's iterator ABI, slice growth, allocator, or goroutine mechanics. Rust lending/guard types may differ as long as rows are streamed safely, errors latch at the same package boundary, and direct consumers receive the same behavior.

## Progress

- [x] (2026-08-10) Audited pinned Go `pkg/executor/join/merge_join.go`, Rust `tidb-chunk` row-container iteration, and Rust merge-join materialization.
- [x] (2026-08-10) Added and ran the fail-before duplicate-group spill regression at baseline `cfd547a8aa95a9d22dd8b0dd30bbd32fc1938f0a`; it failed with `MemoryExceedForQuery { conn_id: 1 }` before output.
- [x] (2026-08-10) Ran the large-outer duplicate regression as a test-only baseline diff; it also failed with `MemoryExceedForQuery { conn_id: 1 }`, proving the independent outer materialization bug before the fix.
- [x] (2026-08-10) Added a Rust lending multi-source iterator; selected row-container plus trailing-chunk composition passes before/after spill, a nonempty failed source stops before the following source, and an empty source with historical error state is omitted like Go.
- [x] (2026-08-10) Stored the merge join's inner equal-key group in a tracked `RowContainer`, wired its action to the session tracker, streamed candidates from it, bounded the Rust adapter's outer side to one row, and reused the loaded inner group across duplicate outer keys.
- [x] (2026-08-10) Added sticky row-container disk-quota, selection-plus-trailing-chunk, spill/result, required-row, gate, right-side, and cleanup regressions.
- [x] (2026-08-10) Charged both live child chunks and pending output until release; early close drops merge state, closes spill storage, unbinds the action, and leaves the join tracker at zero.
- [x] (2026-08-10) Ran focused/full crate tests, strict Clippy, all-target workspace compilation, source-size, formatting, and diff checks.
- [x] (2026-08-10) Committed and pushed the verified merge checkpoint.
- [x] (2026-08-10) Reopened the readback boundary: a test-only baseline regression at `10a833e1a6` proved unspilled hash reads left one row in the disk scratch chunk. Hash and merge now consume `GetRowAndAppendToChunkIfInDisk`, so only spilled rows enter scratch.
- [x] (2026-08-10) Validated the conditional-read follow-up with its focused red-to-green regression, the row-container/hash/merge suites, both full affected crates, strict affected-crate Clippy, and all-target workspace check. The full workspace test sweep stops only at the independently baseline-proven stale `tidb-exec` lease-source assertion.

## Surprises & Discoveries

- Observation: `Iterator4RowContainer` already exists and is behaviorally capable of reading memory or disk, but it is excluded from the common `ChunkIterator<'a>` because a spilled row borrows scratch storage owned by the iterator rather than the container.
  Evidence: `rust/crates/tidb-chunk/src/iterator.rs` documents the omission, while `rust/crates/tidb-chunk/src/row_container.rs` owns the separate lending iterator.

- Observation: Rust merge join accounts every datum row into one operator tracker but never releases prior group charges, then calls `StatementMemory::check` only after the entire group is materialized.
  Evidence: `rust/crates/tidb-executor/src/join.rs::fetch_group` calls `tracker.consume(row_bytes(&row))`, pushes into `Vec<Vec<Datum>>`, and checks only after the loop.

- Observation: spilling only the inner group is insufficient if one `Next` recreates its full cross product in the output chunk.
  Evidence: the first implementation returned 5,000 rows despite `required_rows=137`; `spilled_merge_cross_product_honors_required_rows` now proves 5,000 rows arrive in multiple batches of at most 137.

- Observation: a symmetric whole-group merge makes the OUTER equal-key run unbounded. Accepted TiDB bounds the outer input and reuses the loaded inner group across adjacent outer batches.
  Evidence: `fetchNextOuterGroup` selects one child-chunk group while `fetchNextInnerGroup` alone owns `RowContainer`. The Rust state must retain the inner group after an equal comparison and consume bounded outer rows incrementally.

- Observation: batching a child chunk after first converting it to owned Datum rows duplicates the chunk's memory and can cross a quota that accepted TiDB stays under with row views.
  Evidence: the 64 KiB large-outer regression failed when the Rust adapter retained one converted child chunk and passed when it streamed one converted row. The one-row boundary preserves the observable bounded-memory contract without recreating Go's row-view ABI.

- Observation: length-zero sources are removed before multi-iterator traversal even when their container retains historical error state.
  Evidence: accepted `NewMultiIterator` filters solely on `Len() > 0`; retaining an empty failed source caused an unsigned decrement/panic and masked later rows in Rust.

- Observation: converting the selected row to owned datums does not make an intermediate scratch copy semantically free.
  Evidence: accepted hash/merge consumers preserve `GetRowAndAppendToChunkIfInDisk`: the container's memory row feeds datum conversion directly, while only a spill read lands in `chkBuf`. The baseline Rust path populated scratch in both states; the new regression distinguishes zero rows before spill from exactly one after spill.

- Observation: ownership transfer into pending output does not end its memory lifetime.
  Evidence: required-row batching keeps the pending outer row across `Next` calls. Its charge now moves with that state and is released only when drained or closed.

## Decision Log

- Decision: Treat spill/quota/error/cleanup and multi-source iterator composition as semantic obligations; treat Go goroutine launch, slice backing geometry, allocator layout, and exact iterator interface shape as implementation details.
  Rationale: These are the observable TiDB/package and direct-consumer boundaries named by the project goal.
  Date/Author: 2026-08-10 / Codex.

- Decision: Use `RowContainer` as the sole authority for the inner equal-key group and retain the outer side as a bounded streamed group.
  Rationale: This matches TiDB's ownership and spill boundary and avoids inventing a second spill format or duplicating tracker/action logic.
  Date/Author: 2026-08-10 / Codex.

- Decision: Materialize datums through the row-container's conditional guarded row, not through the always-append convenience API.
  Rationale: Both memory and disk paths still produce owned datums, but only disk decoding needs a scratch chunk. Retaining that distinction preserves accepted transient memory/accounting and gives diagnostics an exact spill observer.
  Date/Author: 2026-08-10 / Codex.

## Outcomes & Retrospective

The dependency-closed merge/lending tranche is implemented. The baseline spill regression is red at `cfd547a8aa` and green in the current tree; a 5,000-row inner run spills and matches the roomy control, a 5,000-row outer duplicate run streams successfully in both LEFT and RIGHT orientations, required-row batches stay at or below 137, temporary-storage disablement preserves memory cancellation, and early close returns the join tracker to zero.

Focused evidence is green: all 176 `tidb-chunk` unit tests plus its integration tests, all 566 active `tidb-executor` unit tests plus integration tests, strict Clippy for both changed crates, and the all-target workspace check. Final self-review, commit, and push remain before this checkpoint is shippable. This does not claim whole `pkg/util/chunk` completion; server cursor integration and receipt classification remain separate package-level work.

The full workspace test sweep reached one unrelated `tidb-exec` receipt assertion, `nextgen_readonly_vars_source::declined_lease_runtime_seams_are_explicit`. The exact test fails identically at clean baseline `cfd547a8aa` because it expects the stale source fragment `let schema_lease = Duration::from_millis`; this tranche changes neither file. It is baseline validation debt, not a merge/lending regression.

The conditional-read follow-up is red-before/green-after: `an_unspilled_probe_does_not_materialize_build_scratch` failed at `10a833e1a6` with one scratch row and now passes with zero; the existing spilled probe test is strengthened to require exactly one scratch row. The merge spill suite also remains green because its guarded memory row is converted before the guard is released. The combined `tidb-chunk`/`tidb-executor` run, strict Clippy, and all-target workspace check pass; the full workspace run again reaches only the unrelated baseline failure documented above.

## Context and Orientation

`rust/crates/tidb-executor/src/join.rs` implements hash, merge, index, and nested-loop strategies. Its `MergeSide.group` currently owns every equal-key row as `Vec<Vec<Datum>>`. `rust/crates/tidb-chunk/src/row_container.rs` implements tracked memory-to-disk storage and `Iterator4RowContainer`; `rust/crates/tidb-chunk/src/iterator.rs` implements ordinary chunk/list iterators and `MultiIterator`, but its lifetime-bound trait cannot contain the spilled-row iterator.

The inner group is the non-preserved side: the right child for inner/left joins and the left child for a right join. It is the only group that needs to span chunks in accepted TiDB. The outer group may remain bounded to its current input batch while the same inner group is reused across adjacent outer batches with the same key.

## Plan of Work

First add an exact regression in `join.rs` proving the existing behavior fails before production changes. Next introduce a Rust lending cursor in `tidb-chunk` that returns a row borrowing the cursor call, not a fixed container lifetime. Adapt chunk and row-container sources and a multi-source cursor that concatenates them while latching the first disk error.

Then change `MergeState` in `join.rs`: initialize one `RowContainer` for the inner side, attach its memory and disk trackers to the merge operator, register its spill action only when temporary storage on OOM is enabled, and reset it between keys. Fill the container in chunks rather than converting every row to `Datum`. Compare keys from the input row, and read candidate rows lazily through the lending cursor/readback buffer. Preserve residual-condition evaluation and outer padding.

Finally close the container and unbind its exact action in `JoinExec::close`, latch spill diagnostics before cleanup, and add direct iterator/error/selection tests plus merge-join spill and quota-gate tests.

## Concrete Steps

From `/private/tmp/task325-chunk-ee558/rust` run the fail-before test at the baseline and expect exit 101 with query-memory cancellation or the explicit `must spill` assertion:

    cargo test --offline --locked -j12 -p tidb-executor 'join::merge_path_tests::a_large_duplicate_group_spills_and_matches_the_unspilled_result' --lib -- --exact --nocapture

After implementation, run:

    cargo test --offline --locked -j12 -p tidb-chunk iterator --lib
    cargo test --offline --locked -j12 -p tidb-chunk row_container --lib
    cargo test --offline --locked -j12 -p tidb-executor merge_path_tests --lib
    cargo clippy --offline --locked -j12 -p tidb-chunk -p tidb-executor --all-targets -- -D warnings
    cargo check --offline --locked -j12 --workspace --all-targets
    cargo test --offline --locked -j12 --workspace --quiet

Use the repository source-size and formatting gates before commit:

    cargo fmt --all -- --check
    scripts/check-source-size.sh
    git diff --check

## Validation and Acceptance

Acceptance requires all of the following: the fail-before test is red at `cfd547a8aa`; the same test is green after implementation and proves real disk bytes; a roomy control does not spill; output equals the roomy control; disabling temporary storage yields errno 8175 rather than spilling; iterator composition preserves selection order before and after spill; an injected/read disk error stops iteration and remains observable; close deletes storage, detaches trackers, and removes the exact session action.

## Idempotence and Recovery

All tests and static gates are safe to rerun. Spill tests use isolated storage authorities and must close containers. If a test aborts, remove only its test-owned temporary directory after confirming the path. Do not reset or overwrite unrelated worktree changes.

## Artifacts and Notes

Accepted Go authority is commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`, principally `pkg/executor/join/merge_join.go` and `pkg/util/chunk/iterator.go`. The Rust baseline for the fail-before proof is `cfd547a8aa95a9d22dd8b0dd30bbd32fc1938f0a`.

Fail-before transcript: the exact test command above selected one test and exited 101 at `join.rs` after `run` unwrapped `Err(MemoryExceedForQuery { conn_id: 1 })`; zero tests passed and one failed. An earlier abbreviated filter selected zero tests and is not evidence.

The outer-streaming fail-before used the same clean baseline plus only `a_large_outer_duplicate_run_remains_streaming` in the existing join test module. Its exact filtered command selected one test and exited 101 with `MemoryExceedForQuery { conn_id: 1 }`; `0 passed; 1 failed; 564 filtered out`. The current implementation passes that test for both LEFT and RIGHT joins with 15,000 rows each.

Pass-after evidence:

- `cargo test --offline --locked -j12 -p tidb-chunk`: 176 unit tests and all integration tests passed.
- `cargo test --offline --locked -j12 -p tidb-executor`: 566 passed, 4 ignored, then every integration test passed.
- `cargo clippy --offline --locked -j12 -p tidb-chunk -p tidb-executor --all-targets -- -D warnings`: exit 0.
- `cargo check --offline --locked -j12 --workspace --all-targets`: exit 0.
- `cargo test --offline --locked -j12 --workspace --quiet`: all preceding suites passed, then exited 101 on the unrelated receipt assertion above. Its exact filtered command also exits 101 in a fresh clean worktree at `cfd547a8aa` with the identical assertion and `0 passed; 1 failed; 562 filtered out`.

## Interfaces and Dependencies

`tidb-chunk` remains the owner of chunks, row containers, spill cursors, and multi-source iteration. `tidb-executor` owns merge-join strategy and `StatementMemory`. `tidb-util` owns memory/disk trackers and configured spill storage. No new third-party dependency is required.

Revision note (2026-08-10): initial plan created after the complete package audit identified merge-join duplicate groups as the highest-impact remaining direct-consumer seam.
