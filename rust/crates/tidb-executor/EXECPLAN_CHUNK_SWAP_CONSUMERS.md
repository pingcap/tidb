# Integrate chunk column ownership into expression and executor consumers

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan is maintained according to that file.

## Purpose / Big Picture

After this change, a serial projection and a limit operator preserve the row demand supplied by their parent and use `tidb-chunk`'s column-identity operations for whole batches. A direct-column projection transfers the input column owner instead of rebuilding every datum, while calculated expressions finish before any owner is moved. A limit can also trim, reorder, or duplicate child columns as an inline projection: a batch crossing the offset boundary is copied because it is only a range, while every later whole batch is handed off through `Chunk::swap_columns` or `ColumnSwapHelper`.

The observable proof is in focused Rust tests. A child that records each incoming `Chunk::required_rows()` must see the exact demand sequence Go's `ProjectionExec` and `LimitExec` produce. Reordered and duplicated inline projections must return the same values in both the partial-copy and whole-batch paths, exact-boundary offsets must not leak an empty result, reopening must reset all execution state, and an expression error must leave the input column owners in place.

## Progress

- [x] (2026-08-09) Read `PLANS.md`, repository policy, the applicable skills, accepted Go `pkg/expression/evaluator.go`, `pkg/executor/projection.go`, `pkg/executor/select.go`, builder wiring, and the corresponding Rust modules.
- [x] (2026-08-09) Confirmed fail-before evidence at commit `8b28739bf921a34f3c3dc98035aa9dab46641d02`: Projection returned eight rows and requested `[8]` instead of two rows and `[2]`; Limit returned `[[10,11,12,13], []]` and requested `[8,8]` instead of `[[10,11],[12,13]]` and `[8,3,2]`.
- [x] (2026-08-09) Selected the ownership architecture and recorded the decisions below.
- [x] (2026-08-09) Added `tidb-expr::evaluator::EvaluatorSuite` and focused evaluation-order and ownership tests.
- [x] (2026-08-09) Migrated serial `ProjectionExec` to `EvaluatorSuite` and required-row propagation while keeping its public constructor source-compatible.
- [x] (2026-08-09) Migrated `LimitExec` to exact required-row negotiation and one derived inline-projection state, with partial, full, boundary, duplicate, invalid-schema, and reopen coverage.
- [x] (2026-08-09) Set Limit's executor initial capacity to `min(count, MAX_CHUNK_SIZE)` in both driver pipelines and added a boundary unit test.
- [x] (2026-08-09) `cargo fmt --all -- --check`, `scripts/check-source-size.sh`, and `git diff --check` pass in the writer worktree.
- [x] (2026-08-09) Root-orchestrated compile, both exact fail-after regressions, focused evaluator/Projection/Limit tests, strict two-crate Clippy, full two-crate tests, and workspace all-targets check all pass.
- [x] (2026-08-09) Strict Clippy exposed two same-crate baseline diagnostics; boxed `PreparedInsertValue::Expression` and removed the redundant `&zone` borrow without adding lint allowances, then reran all static gates successfully.
- [x] (2026-08-09) The full workspace differential found a new fail-before in `user_var_scalar`: Go returned `a|a;b|b;c|c`, while Rust returned `a|c;b|c;c|c` because the new evaluator always ran expression-major.
- [x] (2026-08-09) Added the accepted-Go vectorizability classifier, row-major fallback, and deterministic user-variable plus sequence-order regressions while retaining direct-column transfer as the final phase.
- [x] (2026-08-09) Reran `cargo fmt --all -- --check`, `scripts/check-source-size.sh`, and `git diff --check` after the row-major follow-up; all pass.
- [x] (2026-08-09) Root reran the exact evaluator user-variable unit and exact `difftest-result-tests --test table_diff table_execution_matches_go_engine`; both pass, and the table differential returned to its one unrelated known divergence.
- [x] (2026-08-09) Root reran `cargo test --offline --locked -j12 --workspace --quiet` on the corrected tree; it exits zero.

## Surprises & Discoveries

- Observation: `Chunk::grow_and_reset` resets `required_rows` to the maximum only when it grows, exactly like Go. Projection must therefore call it before reading and forwarding the output chunk's row demand; reversing those operations changes the request sequence.
  Evidence: `pkg/util/chunk/chunk.go` `GrowAndReset`, `rust/crates/tidb-chunk/src/chunk.rs` `grow_and_reset`, and accepted Go `ProjectionExec.Next`/`unParallelExecute`.

- Observation: Go Limit deliberately asks for an offset remainder plus the current parent demand, bounded by both the remaining LIMIT window and maximum chunk size. A broad child pull changes output batching even when the final flattened rows happen to match.
  Evidence: accepted Go `LimitExec.adjustRequiredRows` and the fail-before sequence `[8,8]` versus `[8,3,2]`.

- Observation: a partial first Limit batch cannot be transferred by ownership because only a physical row range belongs to the result. Later whole batches can and should be transferred.
  Evidence: accepted Go `LimitExec.Next` uses `Chunk.Append` for the first overlap and `SwapColumns`/`ColumnSwapHelper` after `meetFirstBatch`.

- Observation: the Rust `Schema::columns_indices` API preserves requested order and duplicates, which is stronger and simpler than reconstructing Go builder maps at each `next` call.
  Evidence: `rust/crates/tidb-expr/src/schema.rs` and its duplicate-friendly iteration over the requested column list.

- Observation: the first broad Cargo attempt encountered `ENOSPC`; this was a validation-environment failure, not a code failure. The root orchestrator removed only recoverable build-cache artifacts, preserved every source worktree and fail-before record, and reran the same gates successfully.
  Evidence: the later focused, crate-wide, Clippy, and workspace commands below all completed with exit status zero.

- Observation: strict Clippy reached two pre-existing same-crate diagnostics only after the consumer code compiled: `PreparedInsertValue::Expression(Expression)` made its local enum unnecessarily large, and `KvTable::drop_index_in` passed `&&SessionTimeZone`.
  Evidence: boxing only the expression payload and passing `zone` directly made the unchanged strict command pass without a lint suppression.

- Observation: expression-major evaluation is not semantically interchangeable with row-major evaluation when select-list expressions read and assign session user variables. The first assignment column completed all rows, leaving the read column to observe only the final value.
  Evidence: the full workspace differential produced Go `a|a;b|b;c|c` and Rust `a|c;b|c;c|c` for `user_var_scalar`.

- Observation: accepted Go does not disable expression-major evaluation for every stateful-looking function. It recursively rejects user-variable reads/assignments, but its sequence rule counts only top-level `nextval`, `lastval`, and `setval`, falling back when `nextval` is mixed with another sequence function or occurs more than once.
  Evidence: `pkg/expression/chunk_executor.go` `Vectorizable`, `HasGetSetVarFunc`, and `checkSequenceFunction`.

## Decision Log

- Decision: Put expression partitioning and column-owner transfer in a new `tidb-expr::evaluator::EvaluatorSuite`, not in `ProjectionExec`.
  Rationale: accepted Go gives the expression package this authority, multiple consumers can reuse it, and the helper must run only after every calculated expression succeeds.
  Date/Author: 2026-08-09 / Codex.

- Decision: Keep `ProjectionExec::new(meta, exprs, child, ctx)` source-compatible and build the suite once inside it.
  Rationale: callers should not need to know the suite's partitioning or ownership mechanics, and no second constructor path is necessary.
  Date/Author: 2026-08-09 / Codex.

- Decision: Model Limit projection as one constructor-derived state: identity, projected, or invalid.
  Rationale: this removes repeated schema checks and makes full-batch behavior exhaustive. The projected state owns both the ordered/duplicate child indexes needed for partial copies and the matching `ColumnSwapHelper` needed for whole-batch transfers.
  Date/Author: 2026-08-09 / Codex.

- Decision: Fail closed when Limit's output schema names a column absent from the child schema, using an internal executor error that renders as MySQL 1105 with the exact message.
  Rationale: silently using identity would return the wrong columns, while classifying a broken executor invariant as an unsupported feature would lie about the failure class.
  Date/Author: 2026-08-09 / Codex.

- Decision: Clamp every `u64` row-demand calculation to the executor maximum and then to `isize::MAX` before calling `Chunk::set_required_rows`.
  Rationale: this preserves the observable bounded request without reproducing integer wrapping or relying on a platform-sized cast.
  Date/Author: 2026-08-09 / Codex.

- Decision: Store the accepted-Go vectorizability classification in `EvaluatorSuite`; evaluate calculated expressions expression-major when safe and row-major only for the rejected user-variable/sequence shapes, then run the direct-column helper last in either mode.
  Rationale: this restores side-effect ordering without penalizing or semantically changing ordinary projections, and it preserves the ownership-transfer atomicity boundary.
  Date/Author: 2026-08-09 / Codex.

## Outcomes & Retrospective

The initial consumer gates completed, but the full workspace differential correctly reopened the increment after exposing the `user_var_scalar` ordering defect; the accepted row-major fallback and the final full-workspace rerun now close that defect. The implementation has one expression-suite authority with both accepted evaluation modes, one Limit inline-projection state, exact parent-to-child demand negotiation, and focused regressions for the mapped failure classes. The deterministic required-row tests remain green with Projection values/requests `([1,2], [2])` and Limit batches/requests `([[10,11],[12,13]], [8,3,2])`. Reordered/duplicate partial and full paths, exact offset boundaries, reopen behavior, evaluation-error ownership atomicity, the internal 1105 message, and Limit initial-capacity boundaries passed their focused suites before the evaluator-order follow-up.

The root orchestrator reported exit status zero for test compilation, the two exact fail-after commands, all initially focused modules, strict Clippy, full `tidb-expr` plus `tidb-executor` tests, and the workspace all-targets check. The only interruption was recoverable build-cache exhaustion; after cache cleanup, the same commands passed. After the row-major follow-up, writer-owned static checks, root-owned exact evaluator plus table-differential pass-after gates, and `cargo test --offline --locked -j12 --workspace --quiet` all pass. No Go source, generated/Bazel artifact, receipt, commit, or remote was changed by this writer.

## Context and Orientation

`rust/crates/tidb-chunk/src/chunk.rs` owns `Chunk`, including `required_rows`, physical-range append, whole-chunk column swaps, pruning, and shared column identity. `rust/crates/tidb-chunk/src/chunk_util.rs` owns `ColumnSwapHelper`, which maps one input-column owner to one or more output slots and merges aliases safely.

`rust/crates/tidb-expr/src/expression.rs` owns the closed `Expression` enum and row evaluation. The new `rust/crates/tidb-expr/src/evaluator.rs` will split a projection expression list into direct `Expression::Column` entries and all other expressions. "Direct column" means an expression whose result is exactly one input column; it may transfer that column's owner. Every other expression must calculate and append result datums before a direct-column transfer mutates the input chunk.

`rust/crates/tidb-executor/src/projection.rs` is the serial Rust `ProjectionExec`. It owns one reusable child chunk. Its output row count equals its child's row count, so it must forward the parent's requested row count before calling the child.

`rust/crates/tidb-executor/src/limit.rs` owns the LIMIT window. `cursor` counts child rows consumed, `begin` is the offset, `end` is offset plus the planner-clamped count, and `meet_first_batch` records whether the cursor has reached the window. An "inline projection" is Limit's ability to emit only the output schema's ordered child columns without inserting a separate Projection executor.

`rust/crates/tidb-executor/src/driver.rs` and `rust/crates/tidb-executor/src/driver/agg_select.rs` build Limit executors. Go initializes a Limit result chunk with `min(count, max_chunk_size)` rows, so these two call sites must do the same rather than always using the global initial capacity.

## Plan of Work

Add `evaluator.rs` to `tidb-expr` and export it from `lib.rs`. Define an `EvaluatorSuite` that owns non-column expressions, their output indexes, their accepted-Go vectorizability mode, and an optional `ColumnSwapHelper`. Its constructor partitions and classifies once. Its `run` method evaluates safe expressions column by column and order-sensitive user-variable/sequence shapes row by row, returning evaluation errors immediately; only after all calculated expressions succeed does it call the helper. Define a small error enum that keeps expression failures distinct from exact chunk-helper invariant messages.

Replace Projection's raw expression vector with the suite. In `next`, call `req.grow_and_reset(max_chunk_size)` first, clamp and forward `req.required_rows()` to the reusable child chunk, pull the child, return early for EOF, then run the suite. Convert evaluation failures to `ExecError::Eval` and chunk invariant failures to the new internal 1105 executor failure.

Replace Limit's documented copy-only path with one derived inline-projection state. Derive ordered child indexes from `child.schema().columns_indices(meta.schema().columns)` in the existing constructor. Identity means every child column appears once in the same order. Projected means one ordered index vector plus one helper. Invalid means `next` returns an internal error before pulling rows.

In Limit `next`, reset the caller's rows while preserving its demand. Before every child pull, compute the exact request: remaining LIMIT rows, plus the offset distance while still before `begin`, bounded by the parent's demand and `max_chunk_size`. When the first fetched batch overlaps the offset, copy only its physical in-window range, applying the ordered projection indexes. If the offset lands exactly at that batch's end, continue and fetch the next batch in the same call. Once past the offset, truncate only a final short batch, then transfer the whole batch using identity swap or the helper. `open` resets the child chunk, cursor, and first-batch flag so reopening behaves like a new execution.

Extend focused tests in the nearest modules and add `rust/crates/tidb-executor/tests/required_rows_contract.rs` from the deterministic fail-before worktree. Tests must record child request sequences, inspect output values and duplicate identity, force both partial and whole inline-projection paths, cover an exact offset boundary, and execute the same Limit twice across reopen.

Finally change the two production driver call sites to construct Limit metadata with `usize::try_from(count).unwrap_or(usize::MAX).min(MAX_CHUNK_SIZE)` as the initial capacity. Leave TopN's test-only Limit oracle constructor unchanged because it supplies explicit metadata rather than building a SQL Limit plan.

## Concrete Steps

All commands run from `rust/` unless stated otherwise. The writer first applies source edits, then runs only formatting and static checks as directed by the root orchestrator:

    cargo fmt --all -- --check
    scripts/check-source-size.sh
    git diff --check
    git status --short

The root orchestrator runs the behavioral and compile gates after receiving the writer handoff:

    cargo test --offline --locked -j12 -p tidb-expr -p tidb-executor --no-run
    cargo test --offline --locked -j12 -p tidb-executor --test required_rows_contract projection_propagates_parent_required_rows_to_child -- --exact
    cargo test --offline --locked -j12 -p tidb-executor --test required_rows_contract limit_preserves_parent_batches_while_skipping_offset -- --exact
    cargo test --offline --locked -j12 -p tidb-expr evaluator -- --nocapture
    cargo test --offline --locked -j12 -p tidb-executor projection -- --nocapture
    cargo test --offline --locked -j12 -p tidb-executor limit -- --nocapture
    cargo clippy --offline --locked -j12 -p tidb-expr -p tidb-executor --all-targets -- -D warnings
    cargo test --offline --locked -j12 -p tidb-expr -p tidb-executor
    cargo check --offline --locked -j12 --workspace --all-targets
    cargo test --offline --locked -j12 --workspace --quiet

Expected: both fail-before tests now pass with Projection values/requests `([1,2], [2])` and Limit batches/requests `([[10,11],[12,13]], [8,3,2])`; all focused ownership, boundary, and lifecycle tests pass; Clippy emits no warnings.

After the full-workspace discovery, the root orchestrator reruns the focused evaluator regression and the exact `difftest-result-tests --test table_diff table_execution_matches_go_engine` differential. Expected and observed: each source row reads the value assigned earlier in the same select list, producing `a|a;b|b;c|c`, while the table differential returns to its one unrelated known divergence.

## Validation and Acceptance

Acceptance requires behavior, not merely compilation:

Projection must forward a parent demand of two to its child and return exactly two rows. A mixed evaluator suite must populate calculated output columns before moving direct-column owners. If calculated evaluation errors, the direct input column and untouched output slot must retain their original identities. User-variable expressions must preserve select-list order within every row, and the vectorizability classifier must recursively reject both lowered typed `getvar_*` calls and source-shaped `getvar` calls while applying the accepted top-level-only sequence rule.

Limit with offset nine, count four, maximum batch eight, and parent demands two then two must request `[8,3,2]` from its child and return `[10,11]` then `[12,13]`. An offset exactly equal to a child batch end must fetch and return the next batch in the same `next` call. A reordered and duplicated output schema must return the same column order and aliases after a partial range copy and a whole-batch helper transfer. Closing and reopening must reset cursor, overlap state, child data, and request negotiation.

Both SQL driver paths must expose Limit metadata whose initial capacity is count capped at the maximum chunk size. The broader root-owned workspace checks must find no consumer still relying on Projection or Limit's former copy-only behavior.

## Idempotence and Recovery

All source edits and checks are repeatable. The tests use in-memory deterministic sources and leave no external state. Formatting may be rerun. If a behavioral test fails, retain the focused test and repair the owning implementation rather than weakening its expected request sequence or replacing identity assertions with value-only assertions.

No generated Go or Bazel source is changed, so `make bazel_prepare` is not required. No cleanup, checkout reset, commit, or push belongs to this writer worktree; the root orchestrator owns those steps.

## Artifacts and Notes

Fail-before worktree: `/private/tmp/task325-swap-consumers-fail-before`, exact base `8b28739bf921a34f3c3dc98035aa9dab46641d02`. The test-only file is `rust/crates/tidb-executor/tests/required_rows_contract.rs`.

Accepted Go authorities are `pkg/expression/evaluator.go` (`EvaluatorSuite.Run`), `pkg/executor/projection.go` (`ProjectionExec.unParallelExecute`), `pkg/executor/select.go` (`LimitExec.Next` and `adjustRequiredRows`), and `pkg/executor/builder.go` (`buildLimit`). The Go files do not differ from accepted commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f` in these regions.

## Interfaces and Dependencies

In `tidb-expr`, add public module `evaluator` with these conceptual interfaces (exact Rust lifetime syntax may follow the implementation):

    pub fn has_get_set_var_func(expression: &Expression) -> bool;
    pub fn vectorizable(expressions: &[Expression]) -> bool;

    pub enum EvaluatorError {
        Eval(EvalError),
        Chunk(&'static str),
    }

    pub struct EvaluatorSuite { ... }

    impl EvaluatorSuite {
        pub fn new(exprs: Vec<Expression>, avoid_column_evaluator: bool) -> Self;
        pub fn vectorizable(&self) -> bool;
        pub fn run<C: Columns>(
            &self,
            ctx: &C,
            input: &mut Chunk,
            output: &mut Chunk,
        ) -> Result<(), EvaluatorError>;
    }

`EvaluatorSuite` depends only on existing `tidb-expr` expression/context APIs and `tidb-chunk::ColumnSwapHelper`. Projection converts `EvaluatorError::Eval` to `ExecError::Eval` and `EvaluatorError::Chunk` to the internal executor error.

`LimitExec::new` retains its current public signature. Its private derived state owns any `ColumnSwapHelper`; callers do not supply a mapping and cannot accidentally let the partial-copy and full-swap paths drift apart.

Revision note (2026-08-09): created this plan after source audit and deterministic fail-before reproduction, before production edits.

Revision note (2026-08-09): recorded completed implementation and passing writer-owned formatting/source-size/diff checks; left Cargo gates explicitly pending for the root orchestrator.

Revision note (2026-08-09): recorded the root-orchestrated pass-after, strict Clippy, full-crate, and workspace evidence plus the recoverable ENOSPC validation-environment interruption.

Revision note (2026-08-09): reopened after the full workspace `user_var_scalar` differential exposed expression-major side-effect reordering; recorded the accepted classifier and pending pass-after gates.

Revision note (2026-08-09): recorded the exact focused evaluator and table-differential pass-after; left the root-owned full workspace rerun pending.

Revision note (2026-08-09): recorded the corrected tree's zero-exit full workspace test rerun; no validation item remains pending in this plan.
