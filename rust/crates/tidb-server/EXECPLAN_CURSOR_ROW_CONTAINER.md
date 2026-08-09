# Make prepared cursors quota-bound and spill-backed

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan follows that format and the repository rule that bug fixes need a regression which fails before the fix and passes afterward.

## Purpose / Big Picture

A client using a read-only prepared cursor should not make the server duplicate the complete result in an untracked `Vec<Vec<Datum>>`. The cursor must retain rows in `tidb_chunk::RowContainer`, honor the executing statement's memory quota and temporary-storage policy, spill through the server-owned spill authority, and release its trackers, action, reader, and file on every terminal path. A fetch must advance only after the corresponding row packet is written successfully. A read or encoding error is written at the current MySQL packet sequence and closes the cursor; an actual transport failure is fatal because the server cannot know what reached the client.

The observable proof is through the real MySQL TCP path. With `tidb_enable_tmp_storage_on_oom=OFF` and a one-byte query quota, cursor execute returns errno 8175 and no cursor exists. With spilling enabled, the same rows remain fetchable in order. A fetch encoding failure resets the cursor atomically, and final exhaustion, RESET, CLOSE, re-execute, and connection teardown release cursor resources.

## Progress

- [x] (2026-08-10) Mapped the current `QueryResult` producers, prepared cursor command path, row-container APIs, memory tracker hierarchy, and accepted Go cursor behavior at commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`.
- [x] (2026-08-10) Added deterministic red/green TCP coverage for tmp-storage OFF/ON, atomic fetch-error cleanup, and prepared parameter-type survival across RESET.
- [x] (2026-08-10) Captured the exact hinted result materialization policy inside `tidb-session` before `SET_VAR` restoration and carried exact field types plus that policy on `QueryResult`.
- [x] (2026-08-10) Added the source-shaped cursor tracker label and priority wrapper, plus the required statement disk-root accessor.
- [x] (2026-08-10) Replaced `CursorState`'s row vector with `RowContainer` and `RowContainerReader`; materialization, fetch, and teardown now follow one ownership path.
- [x] (2026-08-10) Verified focused server/session/util tests, all changed-crate tests, strict Clippy over the changed surfaces, all-target workspace compilation, and the workspace suite with three independently reproduced baseline failures skipped.
- [x] (2026-08-10) Self-reviewed the diff, deleted the stale vector materialization/fetch helpers, and updated this plan with exact evidence and remaining boundaries.
- [x] (2026-08-10) Prepared the verified checkpoint for commit and two-remote delivery; the final commit identity is recorded in the task handoff.

## Surprises & Discoveries

- Observation: `QueryResult` currently retains only wire `ColumnInfo`, status, and warnings. Reconstructing chunk `FieldType` from that metadata is lossy because result conversion changes type codes and lengths.
  Evidence: `rust/crates/tidb-server/src/sql_node.rs` and `rust/crates/tidb-server/src/pipeline_session.rs::select_columns`.

- Observation: deriving the cursor policy after `Session::run_with_params` returns is wrong. `run_with_columns` restores `SET_VAR` overlays immediately after execution, while `tidb_mem_quota_query`, `tidb_init_chunk_size`, and `tidb_max_chunk_size` can affect the cursor.
  Evidence: `rust/crates/tidb-session/src/lib.rs::run_with_columns` restores `set_var_hint_restore` after `execute_statement`; hint installation occurs inside `dispatch.rs` before statement contexts are built.

- Observation: the cursor-specific action priority already exists as `DEF_CURSOR_FETCH_SPILL_PRIORITY = 3`, but Rust lacks Go's transparent priority wrapper and cursor tracker label.
  Evidence: accepted `pkg/server/conn_stmt.go::executeWithCursor` wraps `rowContainer.ActionSpill()` with priority 3; Rust `SpillDiskAction` reports priority 2.

- Observation: the server's broader memory architecture creates a new session root for each `StmtContext`, unlike Go's persistent session root. This checkpoint can make cursor-local quota/spill and cleanup correct, but cannot make cursor bytes count against unrelated later statements without a separate session-root redesign.
  Evidence: `rust/crates/tidb-executor/src/mem_quota.rs::StatementMemory::new`.

- Observation: accepted cursor materialization obtains each chunk from the result set, preserving the statement's TiDB-level initial and maximum chunk sizes. Those settings affect retained capacity and therefore quota/spill admission, while Go's particular slice-growth classes do not define the contract.
  Evidence: accepted `pkg/server/conn_stmt.go::executeWithCursor`, `pkg/executor/adapter.go::recordSet.NewChunk`, and `pkg/util/chunk/chunk.go::New`.

- Observation: unfiltered workspace and strict-Clippy gates contain unrelated parent-checkpoint failures. The workspace failures are the partition errno assertion, the stale nextgen lease-source assertion, and `builtin_compare::a_truncating_inexact_string_warns_three_times`; all three were independently reproduced before this cursor diff. Strict Clippy first stops on untouched `tidb-util/filter/tests.rs`, `tidb-exec/cluster_analyze.rs`, `tidb-exec/system_row_write.rs`, `tidb-session/infoschema.rs`, and `tidb-session/show.rs` diagnostics.
  Evidence: unfiltered commands recorded below; none of those files are changed by this plan.

## Decision Log

- Decision: carry exact `Vec<FieldType>`, initial/maximum chunk sizes, and `StatementMemory` as a private cursor materialization authority on `QueryResult`; never infer types from wire metadata and never create a default memory policy in the connection layer.
  Rationale: the producing session owns SQL variables, statement hints, connection identity, and the startup spill authority. The connection owns only protocol and cursor lifetime.
  Date/Author: 2026-08-10 / Codex.

- Decision: capture the authority inside the session statement lifecycle after execution but before restoring `SET_VAR`, via a parallel run method that returns output plus authority while leaving existing callers source-compatible.
  Rationale: this preserves hinted quota/chunk policy without adding stale last-statement state or modifying every `StmtOutput` constructor.
  Date/Author: 2026-08-10 / Codex.

- Decision: take `CursorState` out of the prepared-statement registry at FETCH start and reinsert it only after a wholly successful non-final response.
  Rationale: error and exhaustion cleanup become the normal ownership path. The cursor cannot remain open after advancing past an unwritten row.
  Date/Author: 2026-08-10 / Codex.

- Decision: preserve semantic results, quota, protocol, and cleanup behavior; do not reproduce Go slice capacity growth, goroutine prefetch, or allocator/GC details.
  Rationale: those mechanisms are not public behavior unless they change the named semantic boundaries.
  Date/Author: 2026-08-10 / Codex.

- Decision: construct each retained Rust chunk with the captured TiDB initial and maximum row bounds, then let Rust manage buffer growth.
  Rationale: `tidb_init_chunk_size` and `tidb_max_chunk_size` are SQL-visible policy and affect quota/spill behavior; Go runtime growth size classes are incidental machinery.
  Date/Author: 2026-08-10 / Codex.

## Outcomes & Retrospective

The cursor checkpoint now removes the second retained all-row vector, installs a typed spill-backed owner, captures hinted memory/chunk policy before restoration, closes the result source, and advances FETCH only after packet write success. A one-byte quota returns 8175 with tmp storage disabled and physically spills with tmp storage enabled; the spill test observes non-lock files and disk/global accounting while open and zero accounting/files after drop. Encoding failure yields 1105 and the next FETCH yields 1326. RESET retains prepared parameter types, matching accepted `TiDBStatement.Reset`.

The initial architecture audit found no representation blocker. Two integration boundaries remain explicit and are not claimed closed: Rust still creates a fresh session memory root per statement, so an open cursor does not count against unrelated later statements; and specialized prepared PointRead execution still does not enter this general-cursor materialization path. Pipeline row production is already eager, so this checkpoint removes duplicate retention but does not claim end-to-end lazy execution.

## Context and Orientation

`rust/crates/tidb-server/src/mysql_connection.rs` owns prepared-statement protocol state. At the starting checkpoint its `CursorState` contained `Vec<Vec<Datum>>` plus an index. Cursor execute called `drain_result_rows`; fetch cloned a requested slice and advanced the index before encoding. This checkpoint moves that owner into `cursor_state.rs` and deletes both stale vector helpers.

`rust/crates/tidb-chunk/src/row_container.rs` provides a shared spillable row store. It owns memory/disk trackers and exposes `action_spill`, `add`, `num_row`, and cleanup. `rust/crates/tidb-chunk/src/row_container_reader.rs` walks it chunk-by-chunk and remains valid across a spill.

`rust/crates/tidb-executor/src/mem_quota.rs::StatementMemory` owns the statement tracker roots, quota action, temporary-storage decision, and immutable spill storage. `rust/crates/tidb-session/src/stmt_ctx.rs` constructs it from the live session variables. `rust/crates/tidb-server/src/sql_node.rs::QueryResult` is the handoff from that session to the connection.

Only `PipelineServerSession::execute_general` and `ClusterServerSession::execute_general` currently produce general prepared results that enter this cursor path. Specialized real-TiKV point reads do not currently honor the cursor flag; that is a separate server prepared-statement integration gap and must not be claimed as closed by this checkpoint.

## Plan of Work

First add the fail-before TCP tests. The tmp-storage-OFF test creates and fills a small table, sets CANCEL, disables temporary storage, sets a one-byte quota, executes a read-only cursor, and requires 8175 followed by 1326 on FETCH. The fetch-error test uses a deterministic fake general session whose advertised integer column receives an incompatible bytes datum; the first FETCH must error and the next must report a closed cursor.

In `tidb-session`, add a result materialization authority containing `StatementMemory` plus initial and maximum chunk sizes. Factor the existing statement lifecycle so `run_with_params_and_result_authority` returns the normal `StmtOutput` and this authority captured before hint restoration. Existing `run_with_params`, `run_with_columns`, and `run` keep their signatures and behavior.

In `tidb-server`, extend `QueryResult` with an optional private authority that combines the session policy with the exact field types from `StmtOutput::Rows`. Both general prepared producers attach it before converting fields to protocol columns. Cursor execute refuses to guess if a row result lacks authority.

In `tidb-util`, port the transparent `ActionWithPriority` wrapper and `LABEL_FOR_CURSOR_FETCH`. In `tidb-executor`, expose the retained statement disk root needed for the exact tracker hierarchy. Add focused tests for priority/fallback identity and tracker labeling.

In `mysql_connection.rs`, define `CursorState` as the sole owner of the exact fields, `RowContainer`, `RowContainerReader`, retained statement memory, and registered wrapper action. Cursor materialization pulls bounded source batches, appends datums to typed chunks, adds each chunk to the container, checks the memory killer after accounting, and always calls source finish and close. It publishes metadata and installs the cursor only after the complete materialization and metadata terminator succeed.

Cursor fetch writes rows directly from the reader. It computes the final status from retained total/position, writes a row, then advances. The fetch EOF uses live connection status and zero warnings. The registry uses take/reinsert ownership, and `CursorState::drop` performs reader close, exact action unbind, and container close. A read/encoding failure writes one ERR at the current sequence and leaves no cursor; a transport failure is fatal and the same drop path still runs.

## Concrete Steps

Run commands from `rust/` in `/private/tmp/task325-chunk-ee558` and always use twelve jobs.

Fail-before tests:

    cargo test --offline --locked -j12 -p tidb-server --test all pipeline_mysql_client_source::cursor_materialization_without_tmp_storage_is_8175_and_never_opens_cursor -- --exact --nocapture
    cargo test --offline --locked -j12 -p tidb-server --test all mysql_client_lifecycle_source::cursor_fetch_encoding_error_resets_cursor_atomically -- --exact --nocapture

Focused pass-after gates, all exit 0:

    cargo test --offline --locked -j12 -p tidb-util memory::tracker::tests::priority_wrapper_delegates_state_and_unbinds_by_wrapper_identity --lib -- --exact --nocapture
    cargo test --offline --locked -j12 -p tidb-session tests_mem_quota::cursor_authority_is_captured_before_set_var_restoration --lib -- --exact --nocapture
    cargo test --offline --locked -j12 -p tidb-server cursor_state::tests::quota_spills_cursor_rows_and_drop_releases_every_resource --lib -- --exact --nocapture
    cargo test --offline --locked -j12 -p tidb-server --test all pipeline_mysql_client_source::cursor_materialization_without_tmp_storage_is_8175_and_never_opens_cursor -- --exact --nocapture
    cargo test --offline --locked -j12 -p tidb-server --test all mysql_client_lifecycle_source::cursor_fetch_encoding_error_resets_cursor_atomically -- --exact --nocapture
    cargo test --offline --locked -j12 -p tidb-server --test all mysql_client_lifecycle_source::real_tcp_prepared_lifecycle_reports_exact_eight_binary_executes_and_type_reuse -- --exact --nocapture
    cargo test --offline --locked -j12 -p tidb-util -p tidb-executor -p tidb-session -p tidb-server --all-targets --quiet -- --skip tests_partition::the_ported_rejections_carry_tidbs_own_errno

Completion gates:

    cargo fmt --all -- --check
    scripts/check-source-size.sh
    cargo clippy --offline --locked -j12 -p tidb-util --lib -- -D warnings
    cargo clippy --offline --locked -j12 -p tidb-util -p tidb-executor -p tidb-session -p tidb-server --all-targets -- -D warnings -A clippy::needless-update -A clippy::useless-conversion -A clippy::needless-borrow -A clippy::needless-question-mark
    cargo check --offline --locked -j12 --workspace --all-targets
    cargo test --offline --locked -j12 --workspace --quiet -- --skip tests_partition::the_ported_rejections_carry_tidbs_own_errno --skip nextgen_readonly_vars_source::declined_lease_runtime_seams_are_explicit --skip builtin_compare::tests::a_truncating_inexact_string_warns_three_times
    git diff --check

The unfiltered changed-crate suite stops only at the partition baseline. The unfiltered workspace suite then stops at the nextgen and builtin-compare baselines. The strict all-targets Clippy command stops on the untouched diagnostics listed in `Surprises & Discoveries`; the allowlisted sweep is evidence for this diff, not a claim that the parent checkpoint is lint-clean.

## Validation and Acceptance

The OFF test must fail at baseline because current cursor rows are untracked, then pass with errno 8175/HY000 and a subsequent 1326/24000. The ON companion must return all rows in order, preserve fetch-size boundaries, advertise `CURSOR_EXISTS` on execute/non-final EOF, and advertise `LAST_ROW_SEND` only on the final EOF.

The fake encoder test must show that a first-row mismatch yields one ordinary 1105 response and closes the cursor. A protocol failure after earlier rows uses the current response sequence; only a transport failure is fatal.

The direct resource test observes source finish/close, a real spill file, disk/global tracker consumption while open, and zero consumption/files after the cursor owner drops. TCP tests cover final exhaustion and fetch-error removal, while RESET, CLOSE, re-execute, and connection teardown all remove the same unique `CursorState` owner. Per-terminal physical tracker instrumentation remains desirable follow-up evidence; this checkpoint does not claim those paths have distinct cleanup implementations. The cursor spill action is registered at priority 3 ahead of normal spill and CANCEL.

## Idempotence and Recovery

All test and formatting commands are safe to rerun. Cursor materialization builds resources in locals and publishes them only at the final success boundary, so any intermediate error drops the owner and closes resources. If a gate uncovers an unrelated baseline failure, reproduce it at the parent commit in a detached clean worktree before classifying it; do not weaken the gate or update receipts to hide it.

## Artifacts and Notes

Accepted Go authority is commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`, especially `pkg/server/conn_stmt.go::executeWithCursor` and `handleStmtFetch`, `pkg/server/driver_tidb.go::TiDBStatement.Reset`, and `pkg/util/memory/action.go::NewActionWithPriority`.

The starting Rust checkpoint is `fcf304506fa3109eb26104ad8e78015c1261aa62` on branch `codex/task325-util-chunk-package-v1`.

## Interfaces and Dependencies

At completion, `tidb-session` exposes a result materialization authority with `StatementMemory` and initial/maximum chunk sizes captured inside the statement lifecycle. `tidb-server::QueryResult` privately carries `CursorMaterializationAuthority { field_types, init_chunk_size, max_chunk_size, memory }`. `tidb-util::memory` exposes the source-shaped priority wrapper and cursor label. `StatementMemory` exposes its disk session root without creating another tracker layer. `tidb-server` depends directly on `tidb-chunk` because cursor storage is a server-owned direct consumer of that package.

Revision note (2026-08-10): initial plan created after the complete accepted-source, producer, tracker, and lifecycle audit; no implementation change had yet been made.

Revision note (2026-08-10): implementation, red/green tests, physical spill proof, and root gates recorded; semantic boundaries and unrelated parent-checkpoint failures made explicit before commit.
