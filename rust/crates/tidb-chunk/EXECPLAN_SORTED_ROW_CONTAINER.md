# Port the sorted spill container contract

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan follows that file and the repository-wide `AGENTS.md`.

## Purpose / Big Picture

`pkg/util/chunk.SortedRowContainer` is the exported in-memory-to-disk row store that seals its input, orders row pointers by one or more key columns, and preserves that order after the underlying chunks spill. The Rust `tidb-chunk` crate currently has the plain `RowContainer` and its quota action but no sorted container at all. After this work, Rust callers can append chunks, sort them with TiDB's existing field comparators, read rows in sorted order before or after spill, receive the exact late-add rejection, and attach a quota action that sorts before spilling while preserving spill errors and fallback order.

The accepted Go source is commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`, especially `pkg/util/chunk/row_container.go` lines 490-690 and the direct tests in `row_container_test.go`. The authoritative Rust worktree starts at `8b0b046e9203fa0d82679cda1ace2e408824f098`.

## Progress

- [x] (2026-08-10 09:35Z) Re-read `PLANS.md`, repository policy, the accepted Go implementation/tests, current Rust spill coordinator, trackers, comparators, and package receipt.
- [x] (2026-08-10 09:42Z) Captured fail-before at exact baseline: the focused integration test exits 101 because `tidb_chunk::sorted_row_container` is absent.
- [x] (2026-08-10 10:18Z) Added one reusable pre-spill preparation hook and guarded Add seam to `RowContainer` without changing plain-container values, order, accounting, or lifecycle behavior.
- [x] (2026-08-10 10:46Z) Implemented `SortedRowContainer`, `SortedRow`, and `SortAndSpillDiskAction` with pointer accounting, sealing/order, strict threshold admission, stored errors, cleanup, and kill polling.
- [x] (2026-08-10 11:09Z) Ported the original behavior matrix plus deterministic Add/Sort, close/spill, reentrant-trigger, concurrent-second-action, and public real-spill regressions. Focused unit result: 9 passed; public integration result: 1 passed.
- [x] (2026-08-10 11:52Z) Ran WIP targeted/package tests, strict `tidb-chunk` Clippy, workspace all-target compile, formatting/source-size/diff checks, and final read-only adversarial review. No proven P0/P1 remains.
- [x] (2026-08-10 12:01Z) Prepared the coherent reviewed checkpoint for commit and the same-SHA push to `origin` and `ngaut`; shipping evidence is reported in the task handoff.

## Surprises & Discoveries

- Observation: accepted commit `665fc02e` has no production constructor consumer for `SortedRowContainer`; only the exported implementation and three direct package tests refer to it.
  Evidence: `git grep -n 'NewSortedRowContainer\|SortedRowContainer' 665fc02e -- '*.go'` returns the implementation and `row_container_test.go` cases only. This keeps the implementation dependency-closed in `tidb-chunk`, but the exported package contract still belongs to the whole-package claim.

- Observation: the existing Rust `SpillDiskAction` already solves unrelated-trigger spill, reset/fallback serialization, shared ownership, and error replay. A second spill state machine would duplicate authority and reopen resolved races.
  Evidence: `rust/crates/tidb-chunk/src/row_container.rs` owns `Coordinator`, `SpillDiskAction`, `RowContainerShared::perform_spill`, and the tracker action chain.

- Observation: the accepted sort checkpoint calls `memTracker.Consume(1)` every 10,240 comparisons only to poll the query killer. Repeating that mechanism in synchronous Rust can recursively re-enter the active spill action.
  Evidence: accepted `keyColumnsLess` comment says the consume exists “for checking the NeedKill signal”; Rust `Tracker::handle_kill_signal` exposes that semantic boundary directly.

- Observation: accepted promoted `Reset` leaves a sealed pointer vector and its charge behind, so a reset container still rejects Add and can leak the pointer charge on Close.
  Evidence: this follows from Go embedding rather than an intentional sorted-container branch, has no production consumer at the pinned commit, and is not asserted by the original tests beyond `Reset` returning no error. Rust clears both storage and pointer accounting so reset is reusable; the sticky/leaking behavior is classified `DECLINED`.

- Observation: resetting storage and clearing pointers as two independent operations leaves a quota-action race: an action may precompute admission before reset and reseal the empty container afterward.
  Evidence: the deterministic regression pauses the action after admission, starts reset, then releases the action. The root fix makes reset an admission epoch that drains the serialized action, routes reentrant reset accounting to the non-spill path, and performs a final pointer-state invalidation.

- Observation: the broad workspace test sweep reaches one unrelated source-text assertion failure in `tidb-exec` (`nextgen_readonly_vars_source::declined_lease_runtime_seams_are_explicit`).
  Evidence: the exact filtered test exits 101 with the same assertion at the clean committed baseline `8b0b046e9203fa0d82679cda1ace2e408824f098`; this diff does not touch `node_config` or that test. The all-target workspace compile is green.

## Decision Log

- Decision: reuse `RowContainerShared` as the only spill, disk, error, and lifecycle authority; add one optional pre-spill callback that the sorted wrapper installs.
  Rationale: every direct and quota-triggered spill then sorts through the same coordinator. Preparation failures become the same stored spill error that reads replay, while the already-sealed outer Add retains the source sentinel priority.
  Date/Author: 2026-08-10 / Codex.

- Decision: represent sorted order as `Option<Vec<RowPtr>>` behind a mutex. `None` is open for Add; `Some`, including an empty vector, is sealed and sorted.
  Rationale: this is the observable source invariant. It linearizes Add versus Sort without recreating Go's RWMutex implementation.
  Date/Author: 2026-08-10 / Codex.

- Decision: retain row values/chunks in insertion order and sort only row pointers with `sort_unstable_by` plus TiDB `CompareFunc` values.
  Rationale: callers observe comparator ordering and sorted reads, not Go standard library's particular unstable-sort algorithm or equal-key permutation.
  Date/Author: 2026-08-10 / Codex.

- Decision: poll `Tracker::handle_kill_signal` every 10,240 comparisons instead of accounting a synthetic byte.
  Rationale: query interruption is the intended TiDB behavior; the `Consume(1)` transport and its incidental byte are an implementation mechanism and would recursively invoke the synchronous Rust action.
  Date/Author: 2026-08-10 / Codex.

- Decision: enter the plain-container Add coordinator before acquiring the sorted-state guard, retain that guard through the records mutation, and release it before any pending spill runs.
  Rationale: this makes Add versus Sort linearizable without holding the order lock while waiting for spill lifecycle state or exposing a row that the pointer snapshot omitted.
  Date/Author: 2026-08-10 / Codex.

- Decision: expose a guard-backed `SortedRow` whose `row()` method borrows the live in-memory chunk or owned decoded disk chunk; it does not retain the sorted-state mutex.
  Rationale: the copied row pointer plus the plain container's records guard provides the required lifetime. Extending the order-lock lifetime into close/action paths adds deadlock risk without changing observable row validity.
  Date/Author: 2026-08-10 / Codex.

- Decision: serialize sorted action admission, then delegate spill/fallback generation handling to the existing plain action. Admission is strictly `sorted_bytes > trigger_limit / 10`.
  Rationale: the package observes one spill generation, fallback chain, finished state, and priority; a second state machine or Go goroutine timing would duplicate authority without adding semantics.
  Date/Author: 2026-08-10 / Codex.

- Decision: track logical sorted-row pointer charge independently from retained List allocation capacity and use it as an admission prerequisite.
  Rationale: allocator capacity may remain charged after reset, but it no longer represents sortable rows. This preserves reentrant first-Add spill while preventing an empty reset container from being sealed by a cached action.
  Date/Author: 2026-08-10 / Codex.

- Decision: reject mismatched key, direction, and comparator vector lengths at construction.
  Rationale: truncating zipped vectors silently drops sort keys and returns wrong order; a failed invariant is preferable to publishing semantically incomplete ordering.
  Date/Author: 2026-08-10 / Codex.

## Outcomes & Retrospective

Implementation and WIP validation are complete. The checkpoint adds the exported sorted spill contract, exact typed late-Add error, pointer-only ordering across real disk spill, shared quota/fallback authority, reusable reset, cleanup, and deterministic race coverage. It advances the whole `pkg/util/chunk` package but does not claim package completion: receipt classification and the remaining common-iterator and cross-crate spill-consumer obligations stay explicit.

## Context and Orientation

`rust/crates/tidb-chunk/src/row_container.rs` owns `RowContainer`, the shared records, the in-memory `List`, row-addressed disk store, memory/disk trackers, and the quota action. Its coordinator serializes Add, spill, reset, close, and fallback without reproducing Go goroutine mechanics.

`rust/crates/tidb-chunk/src/compare.rs` defines `CompareFunc`, a boxed thread-safe function that compares two `Row` values at chosen columns using TiDB type and collation rules. `rust/crates/tidb-chunk/src/list.rs` defines the eight-byte `RowPtr { chk_idx, row_idx }` address used by both source implementations.

A “pre-spill preparation hook” means a callback run after the spill coordinator owns the spill generation but before it takes the records write lock. The sorted container uses it to seal and sort its row-pointer vector. If it returns an error or panics, `RowContainer` still creates the disk-side marker, stores the error, and makes reads and adds return that error, matching accepted `spillToDisk(preSpillError)`.

## Plan of Work

First, modify `rust/crates/tidb-chunk/src/row_container.rs`. Add an optional thread-safe pre-spill callback to `RowContainerShared`. `perform_spill` must invoke it before locking records, catch callback panics, create the disk container, and either store the preparation error without copying rows or perform the existing copy/clear path. Add crate-private shared-handle forms of Add, action creation, spill, reset, and close; existing public `&mut self` methods remain and delegate to them. Add a crate-private Add entry that runs pointer accounting after the coordinator enters `AddingMemory` and before the List mutation, so a reentrant quota action becomes a pending spill instead of spilling an incomplete input.

Second, add `rust/crates/tidb-chunk/src/sorted_row_container.rs` and export it from `lib.rs`. The sorted inner state owns one plain `RowContainer`, an outer memory tracker, comparator configuration, a mutex-protected optional row-pointer vector, and a cached sorted action. Construction attaches the plain container tracker to the outer tracker and installs a weak pre-spill callback. Add checks the seal, charges `ROW_PTR_SIZE * rows`, and delegates through the coordinated Add entry. Sort builds pointers once and orders them by all non-`None` compare functions, reversing each non-equal ordering for descending keys. Every 10,240 comparisons it polls the query killer. `SortedRow` copies the selected pointer and retains only the plain container's guarded/owned chunk.

Third, make `SortAndSpillDiskAction` wrap the existing `SpillDiskAction`. It serializes its observable action call, delegates fallback/priority/finished state, admits a first spill only when sorted-container consumption is strictly greater than one tenth of the triggering tracker's hard limit, and otherwise invokes the fallback. Admitted actions use the existing coordinator; the installed preparation callback guarantees sort-before-spill.

Finally, port focused tests for construction, multi-key ascending/descending order, idempotent sort, direct spill, quota-triggered threshold behavior, eight-byte pointer accounting, late-add rejection, comparator/preparation error replay, and cleanup. Keep the public integration regression as the end-to-end boundary.

## Concrete Steps

All commands run from `/private/tmp/task325-chunk-ee558/rust` unless stated otherwise.

The captured fail-before command at detached baseline `8b0b046e92` is:

    cargo test --offline --locked -j12 -p tidb-chunk --test sorted_row_container_contract sorted_container_orders_rows_across_spill_and_rejects_late_adds -- --exact --nocapture

Expected baseline result: exit 101 with unresolved import `tidb_chunk::sorted_row_container`.

During implementation, run the narrow loop first:

    cargo test --offline --locked -j12 -p tidb-chunk sorted_row_container --lib
    cargo test --offline --locked -j12 -p tidb-chunk --test sorted_row_container_contract

Then run the WIP package gates:

    cargo fmt --all -- --check
    scripts/check-source-size.sh
    cargo test --offline --locked -j12 -p tidb-chunk
    cargo clippy --offline --locked -j12 -p tidb-chunk --all-targets -- -D warnings
    cargo check --offline --locked -j12 --workspace --all-targets
    git diff --check

The whole-package goal remains active after this checkpoint; Ready-profile `make -j12 lint` belongs to the eventual package-completion gate, not this WIP tranche.

## Validation and Acceptance

The public integration test must pass and produce sorted values `[1, 1, 2, 3]` after a real spill, report `already_spilled == true`, and reject an added chunk with exactly `can not add because sorted`.

Unit tests must prove that two-key ascending/descending comparison uses the configured TiDB comparator order, sorting is idempotent, pointer accounting is exactly eight bytes per input row in addition to chunk memory, the action does not spill at or below one tenth of the trigger quota, the next eligible trigger sorts and spills, a concurrent second action waits and then checks fallback, preparation errors are replayed by reads, and later outer Add returns the typed sorted sentinel.

The WIP checkpoint is acceptable only if targeted red-to-green evidence, all `tidb-chunk` tests, strict crate Clippy, workspace all-target check, formatting, source-size, and diff hygiene are green. A final completion claim is explicitly out of scope until every package receipt obligation and direct consumer is classified and verified.

## Idempotence and Recovery

All test and formatting commands are safe to rerun. Spill tests use isolated temporary directories whose `SpillStorage` authority owns cleanup. If a Cargo command fails from build-cache exhaustion, remove only the inactive task-owned target cache after checking active processes and disk usage; never delete source worktrees or accepted evidence.

The implementation is confined to the authoritative task branch. The detached fail-before worktree remains uncommitted evidence and must not be merged. If the design is falsified before commit, use `apply_patch` to revise the task worktree; do not reset or overwrite unrelated state.

## Artifacts and Notes

Fail-before transcript, baseline `8b0b046e9203fa0d82679cda1ace2e408824f098`:

    error[E0432]: unresolved import `tidb_chunk::sorted_row_container`
    error: could not compile `tidb-chunk` (test "sorted_row_container_contract") due to 1 previous error
    exit 101

Pass-after evidence on the final tree:

    cargo test --offline --locked -j12 -p tidb-chunk sorted_row_container --lib
    11 passed; 0 failed

    cargo test --offline --locked -j12 -p tidb-chunk
    171 library + 2 compare + 2 fixture-probe + 1 public sorted-contract tests passed

    cargo clippy --offline --locked -j12 -p tidb-chunk --all-targets -- -D warnings
    exit 0

    cargo check --offline --locked -j12 --workspace --all-targets
    exit 0

The optional broader `cargo test --offline --locked -j12 --workspace --quiet` sweep exits 101 only at the pre-existing `tidb-exec` source-text assertion described above; the exact test reproduces at the clean baseline.

## Interfaces and Dependencies

In `rust/crates/tidb-chunk/src/sorted_row_container.rs`, define these public surfaces:

    pub struct SortedRowContainer;
    pub struct SortedRow<'a>;
    pub struct SortAndSpillDiskAction;

    impl SortedRowContainer {
        pub fn new(
            field_types: &[FieldType],
            chunk_size: usize,
            by_items_desc: Vec<bool>,
            key_columns: Vec<usize>,
            key_compare_funcs: Vec<Option<CompareFunc>>,
            storage: Arc<SpillStorage>,
        ) -> Self;
        pub fn add(&mut self, chunk: Chunk) -> Result<(), DiskError>;
        pub fn sort(&self) -> Result<(), DiskError>;
        pub fn spill_to_disk(&mut self);
        pub fn get_sorted_row(&self, index: usize) -> Result<SortedRow<'_>, DiskError>;
        pub fn get_sorted_row_and_always_append_to_chunk(
            &self,
            index: usize,
            chunk: &mut Chunk,
        ) -> Result<usize, DiskError>;
        pub fn action_spill(&mut self) -> Arc<SortAndSpillDiskAction>;
        pub fn mem_tracker(&self) -> &Arc<Tracker>;
        pub fn disk_tracker(&self) -> &Arc<DiskTracker>;
        pub fn num_row(&self) -> usize;
        pub fn num_chunks(&self) -> usize;
        pub fn num_rows_of_chunk(&self, chunk_index: usize) -> usize;
        pub fn alloc_chunk(&mut self) -> Chunk;
        pub fn field_types(&self) -> Vec<FieldType>;
        pub fn close(&mut self);
    }

`SortedRow::row(&self) -> Row<'_>` creates the cursor on demand from its guarded chunk. `SortAndSpillDiskAction` implements `tidb_util::memory::ActionOnExceed` and delegates its fallback chain and priority to the shared plain-container action.

Revision note (2026-08-10): initial plan created after source/Rust audit and fail-before capture. It explicitly scopes semantics at the TiDB package boundary and rejects Go runtime/stdlib reconstruction.
