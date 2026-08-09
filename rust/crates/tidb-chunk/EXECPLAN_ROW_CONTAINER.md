# Make the plain row container safe under reentrant and concurrent spill

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan is maintained according to that file.

## Purpose / Big Picture

`pkg/util/chunk.RowContainer` is the storage root used by a memory-limited operator such as hash join. Rows first live in a `List`; after the statement memory tracker exceeds its quota, all rows move to a row-addressed temporary file and subsequent reads and writes use that file. The current Rust seed can only spill when a later `RowContainer::add` observes an atomic flag. That misses an observable case: the last row chunk can be added below quota, then an unrelated allocation on the shared session tracker can exceed the quota. Go spills the existing container immediately; the Rust seed remains in memory forever because there is no later add.

After this change, the plain `RowContainer` has one `Arc`-owned state shared by shallow copies and by a weak reference held by `SpillDiskAction`. An action invoked by any allocation can therefore spill the container synchronously when it is idle. An action invoked reentrantly from `List::add` arms a pending spill and returns; the add releases the records write lock and then performs that spill. Readers, reset, close, disk appends, and later actions serialize through an explicit phase coordinator without holding its mutex during record locking, disk I/O, or tracker/fallback callbacks. Observable row order, row pointers, memory accounting, disk accounting, spill errors, kill polling, reset rearming, and fallback semantics remain intact.

The visible proof is a set of Rust regressions in `rust/crates/tidb-chunk/src/row_container.rs` and `rust/crates/tidb-chunk/src/row_container_reader.rs`: unrelated session allocation spills after the final add; add-triggered spill does not deadlock; shallow copies and a reader retain every value across a mid-read spill; reset and close wait for a spill and wake all waiters; kill and fallback behavior remain observable.

This plan covers only the plain Go `RowContainer`, `SpillDiskAction`, `iterator4RowContainer`, and the ownership adjustment needed by `RowContainerReader`. It deliberately does not implement `SortedRowContainer`, `SortAndSpillDiskAction`, receipt ledgers, fixture evidence, Go goroutine timing, `WaitForTest`, lock fan-out/cache padding, or Go scheduler and panic-order details.

## Progress

- [x] (2026-08-09 11:58Z) Verified the sole-writer worktree is clean at `dcdd211a26ceeaa1a0fb49a06e6373768a1098e3` on `codex/task325-util-chunk-package-v1`.
- [x] (2026-08-09 11:58Z) Read Go `row_container.go`, `row_container_reader.go`, their source tests, Rust `row_container.rs`, `row_container_reader.rs`, `list.rs`, row-addressed disk storage, memory tracker/action code, and direct Rust consumers.
- [x] (2026-08-09 11:58Z) Identified the semantic root: an action only owns an atomic trigger, so an unrelated post-add quota exceed cannot reach the container and spill it.
- [x] (2026-08-09 12:20Z) Replaced container-local records with one `Arc<RowContainerShared>` holding a poison-recovering records `RwLock`, coordinator `Mutex`/`Condvar`, shared trackers, and lazily created weak-backed action.
- [x] (2026-08-09 12:20Z) Implemented the explicit phase machine, repeated reentrant pending-spill handoff, direct idle spill, wait/fallback behavior, phase leases, explicit close-once semantics, and final shared-inner cleanup.
- [x] (2026-08-09 12:44Z) Kept public `get_chunk` as a no-copy guard-backed live memory view (owned only for disk), while isolating the owned in-memory snapshot required by `RowContainerReader` behind `get_chunk_snapshot`.
- [x] (2026-08-09 12:44Z) Added required source regressions, including the baseline-failing unrelated allocation case, same-thread repeated and cross-thread reentrancy, both reset/fallback race orders, first/later fallback order, shallow-copy reading, mid-read spill, deterministic reset/close serialization, and retained kill/error behavior.
- [x] (2026-08-09 12:44Z) Restored baseline `&mut self` receivers on public mutation/lifecycle methods so a same-handle `RowContainerChunk` live view cannot be followed by a safe self-deadlocking spill/reset call.
- [x] (2026-08-09 12:44Z) Reran the final writer-lane static checks after the generation/live-view review fixes: exact-file `rustfmt`, source-size ratchet, `git diff --check`, full diff/status review, direct-callsite scan, and stale atomic-trigger/borrowed-reader/per-handle-drop scans all pass.
- [x] (2026-08-09 12:44Z) Created one bounded semantic checkpoint for handoff with the exact root-owned executable gate commands below; the coordinator owns Cargo/workspace execution and remote operations.
- [x] (2026-08-10) Reopened the row-read boundary after proving the hash join copied every unspilled build row into its disk scratch chunk. The exact baseline regression failed at `10a833e1a6` with scratch rows `1`, expected `0`.
- [x] (2026-08-10) Added guard-backed `GetRowAndAppendToChunkIfInDisk`, made the always-append helper build on it, and migrated hash and merge join to preserve the conditional copy boundary.
- [x] (2026-08-10) Completed the follow-up validation: focused red-to-green regressions, all row-container/hash/merge suites, both affected crates, strict affected-crate Clippy, and all-target workspace check pass. The full workspace test sweep stops only at the independently baseline-proven stale `tidb-exec` lease-source assertion.

## Surprises & Discoveries

- Observation: `List::add` deliberately calls `Tracker::consume` while it owns the new chunk, and `Tracker::consume` invokes the quota action synchronously.
  Evidence: `rust/crates/tidb-chunk/src/list.rs` charges at `List::add`; `rust/crates/tidb-util/src/memory/tracker.rs` calls `try_action` before `consume` returns. A spill action that takes the records write lock from this call stack would self-deadlock.

- Observation: `List::reset` can also make a positive tracker consumption when it accounts the formerly unaccounted tail chunk.
  Evidence: `List::reset` calls `consume` before moving chunks to the freelist. Reset therefore needs a distinct `ResettingMemory` phase whose first reentrant action arms a pending spill, just as `AddingMemory` does.

- Observation: a bare borrowed chunk cannot escape the shared records `RwLock`, but deep-cloning every in-memory `get_chunk` loses Go's live/no-extra-memory contract.
  Evidence: `RowContainerChunk` carries the records read guard and chunk index and dereferences to the live list chunk. Disk reads remain owned. Only `RowContainerReader`, which must keep a chunk after releasing the guard so another handle can spill mid-read, uses the explicitly named owned snapshot seam.

- Observation: the old `Drop for RowContainer` calls `close`, which would close shared state as soon as any shallow handle drops.
  Evidence: the new handles are `Arc` clones. Explicit `close` is the shared lifecycle operation; automatic detach/close belongs to `Drop for RowContainerShared`, which runs only after the last handle/action upgrade is gone.

- Observation: Go `Reset` does not clear `records.spillError`, and a later successful spill does not overwrite it with nil.
  Evidence: `pkg/util/chunk/row_container.go` only assigns `spillError` on a pre-spill error, disk-add error, or recovered panic. `Reset` closes/removes `inDisk` and rearms the action without assigning the error field. The Rust state therefore preserves the first stored error across reset and re-spill.

- Observation: fallback and reset need one terminal ordering decision. A waiter cannot run an old-generation fallback after reset has rearmed the action, and reset cannot close/rearm while a claimed fallback callback is active.
  Evidence: the coordinator generation increments only when reset publishes `MemoryIdle, armed=true`. A later action claims `fallback_active` for its observed generation or, if reset won first, re-enters the action machine in the new generation. `FallbackLease` clears/notifies on unwind and reset waits for an active claim.

- Observation: the always-append Rust helper erased an accepted ownership distinction even though every caller immediately converted the row to owned datums.
  Evidence: Go `GetRowAndAppendToChunkIfInDisk` returns the live list row and a nil chunk before spill. The baseline executor regression left one row in `hashRowContainer.chkBuf`; the new path leaves zero in memory and exactly one after spill while returning identical datums.

- Observation: branch-local `rust/HANDOFF.md` and `rust/PARALLEL.md` named by older rewrite guidance are absent at this checkpoint.
  Evidence: direct reads returned `No such file or directory`; the current repository `AGENTS.md`, `PLANS.md`, design document, source, and parent task contract are used instead.

- Observation: the focused concurrency regressions pushed `row_container.rs` 15 lines over the unlisted-file ratchet.
  Evidence: the first final `check-source-size.sh` reported 2215 lines against 2200. Moving the reusable barrier-hook helpers to the test-only sibling `row_container_test_hooks.rs` reduced production/test source ownership cleanly; the rerun reports `source-size ratchet: OK` with `row_container.rs` at 2172 lines.

## Decision Log

- Decision: represent all plain-container ownership as `RowContainer { shared: Arc<RowContainerShared> }`, and make `Clone`/`shallow_copy` the Rust equivalent of Go `ShallowCopyWithNewMutex`.
  Rationale: all handles, the spill action, reset, and close must observe one records/tracker/lifecycle state. Go's extra read mutexes and write-lock fan-out are contention machinery, not observable behavior; `Arc<RwLock<_>>` gives the required shared semantics directly.
  Date/Author: 2026-08-09, revised 2026-08-10 / Codex

- Decision: use `CoordinatorPhase::{MemoryIdle, AddingMemory, AddingDisk, Spilling, DiskIdle, Failed, ResettingMemory, ResettingDisk, Closing, Closed}` plus `pending_spill` and `armed`.
  Rationale: the phases distinguish operations with different reentrant action behavior. Only `AddingMemory` and `ResettingMemory` may arm a pending spill; `AddingDisk` must wait/fallback rather than recursively request a second spill. Terminal disk and failed states let every waiter make the same decision.
  Date/Author: 2026-08-09 / Codex

- Decision: the coordinator mutex is an ordering lock only. Code releases it before records locks, disk operations, tracker attachment/detachment, or fallback invocation.
  Rationale: this prevents lock inversion and reentrant deadlock by construction. State transitions are settled afterward under the coordinator and every terminal publication notifies the condition variable.
  Date/Author: 2026-08-09 / Codex

- Decision: the first action consumes `armed` and never invokes its fallback. When memory mutation is active it sets `pending_spill`; when `MemoryIdle` it claims `Spilling` and performs the spill synchronously. Later actions wait for active phases to settle and only then check the triggering tracker and invoke fallback outside locks.
  Rationale: this preserves Go's first-trigger spill and later-trigger fallback contract without reproducing its goroutine. It also fixes the post-last-add quota exceed because the action owns a weak route to the shared container.
  Date/Author: 2026-08-09 / Codex

- Decision: serialize reset/fallback races with `generation` and `fallback_active` rather than Go's action mutex/runtime schedule.
  Rationale: if fallback claims the terminal state first, reset waits for its no-lock callback and then rearms. If reset publishes the next generation first, the waiter never invokes stale fallback and re-enters as a new-generation action. Pending spill handed off by `ResettingMemory` remains in the old generation because reset did not publish an armed idle state.
  Date/Author: 2026-08-09 / Codex

- Decision: use a no-I/O `PhaseLease` to restore a non-busy phase and notify waiters if add, reset, close, or a claimed spill unwinds. Use `catch_unwind` only inside the accepted spill region, where Go converts panic into observable `spillError`.
  Rationale: lifecycle phases cannot strand waiters, but Go panic order outside spill is not a required runtime contract. The phase lease is lock hygiene rather than panic emulation.
  Date/Author: 2026-08-09 / Codex

- Decision: `get_chunk` returns `RowContainerChunk`, and conditional row reads return `RowContainerRow`; each owns the decoded disk position or holds a records read guard over the live in-memory storage. Public mutations retain `&mut self`. The iterator and reader continue to own only cursor data that must cross lock boundaries.
  Rationale: this preserves the public live/no-copy memory contract without letting a bare borrow escape the guard. Hash and merge join materialize datums while the short guard is live, so their scratch chunks are touched only by disk reads. The mutable receivers preserve same-handle borrow exclusion. `RowContainerReader` still takes an owned in-memory snapshot because its current chunk must survive another shallow handle spilling the shared records.
  Date/Author: 2026-08-09 / Codex

- Decision: remove per-handle `Drop for RowContainer`; explicit `close` closes/detaches the shared state exactly once and publishes `Closed` to every clone. Add final cleanup to `Drop for RowContainerShared`.
  Rationale: dropping one shallow handle must not close the shared container, while dropping the final handle must not leak consumption into parent trackers. The inner destructor marks the action finished, detaches trackers, and releases owned disk/memory resources once.
  Date/Author: 2026-08-09 / Codex

- Decision: preserve a stored spill error across reset and later spill attempts.
  Rationale: this is the direct Go record contract. Reset changes storage/action readiness, not the historical error field; later access after another spill must still report it.
  Date/Author: 2026-08-09 / Codex

## Outcomes & Retrospective

The plain semantic root is implemented and static-clean. `RowContainer` is now a cheap shared handle, independent post-add quota actions can reach and spill it, same-thread repeated reentrant actions return while other-thread later actions wait, and generation-scoped fallback claims serialize both reset race orders. Public `get_chunk` keeps the live/no-copy memory contract through a guard-backed view; only the reader's cross-spill cursor uses an explicit owned snapshot. Reset, close, final cleanup, kill/error persistence, and deterministic spill serialization are represented by focused source regressions.

The writer lane intentionally did not run Cargo or broader rewrite gates. Executable proof remains for the coordinator/root lane using the commands below. This checkpoint does not claim the still-separate `SortedRowContainer`, receipt/evidence work, or whole Go-package completion.

The 2026-08-10 conditional-read follow-up now restores the remaining live-row boundary for direct hash/merge consumers. Its test-only baseline failed with `left: 1, right: 0`; the focused row-container suite and both hash/merge spill suites pass after the change. The combined affected-crate tests, strict Clippy, and all-target workspace check pass. The full workspace test sweep reached only the unrelated `nextgen_readonly_vars_source::declined_lease_runtime_seams_are_explicit` failure already reproduced at a clean baseline.

## Context and Orientation

The repository root is the Go TiDB source. The Rust workspace is under `rust/`. Go `pkg/util/chunk/row_container.go` defines the authoritative container and action behavior; `pkg/util/chunk/row_container_test.go` includes spill, reset, deadlock, concurrent reader, spill panic, and kill regressions. `rust/crates/tidb-chunk/src/row_container.rs` is the current Rust seed. `rust/crates/tidb-chunk/src/list.rs` owns in-memory chunks and deliberately preserves Go's lagging memory-accounting rule. `rust/crates/tidb-chunk/src/row_in_disk.rs` owns the row-addressed spill files and disk tracker. `rust/crates/tidb-util/src/memory/tracker.rs` synchronously invokes `ActionOnExceed` while `consume` is still on the caller's stack.

A “reentrant action” is an action invoked from inside `List::add` or `List::reset` because that operation called `Tracker::consume`. A “pending spill” is a coordinator bit set by that action; it tells the outer operation to release the records lock and then claim the spill. “Armed” means the first quota trigger is still available. A “later action” is any action after armed has been consumed; it waits for the spill and may invoke fallback if the triggering tracker still exceeds its limit.

`rust/crates/tidb-executor/src/hash_join.rs` and the merge-group storage in `join.rs` are the direct production Rust row-read consumers. Each stores a `RowContainer`, indexes rows by `RowPtr`, and registers `SpillDiskAction` on the session tracker. Their scratch chunks now remain empty while the container is in memory and hold only decoded disk rows after spill.

## Plan of Work

First, rewrite `rust/crates/tidb-chunk/src/row_container.rs` around one shared inner object. Add poison-recovering helpers for `Mutex`, `RwLock`, and `Condvar`. Define the coordinator phases and state. Put `RowContainerRecord`, trackers, action slot, coordinator, and condition variable in `RowContainerShared`; make `SpillDiskAction` hold `Weak<RowContainerShared>` plus its fallback/finished state.

Second, implement action arbitration. A first action in `AddingMemory` or `ResettingMemory` clears `armed`, sets `pending_spill`, and returns. A first action in `MemoryIdle` clears `armed`, publishes `Spilling`, drops the coordinator guard, spills synchronously, and returns without fallback. Actions encountering `AddingDisk`, `Spilling`, reset/close activity, or a consumed arm wait on the condition variable; after the operation settles they check `Tracker::check_exceed` and invoke fallback outside all container locks.

Third, implement operations. `add` claims either `AddingMemory` or `AddingDisk`, mutates records under a write guard protected by a phase lease, drops that guard, and settles the phase. A successful memory add with pending spill transitions directly to `Spilling` and performs it before returning. Spill creates/attaches the disk container, copies chunks in order, polls the memory kill signal after each chunk, records ordinary disk errors or caught panic text, clears memory only after complete success, publishes `DiskIdle` or `Failed`, and wakes waiters. `reset` waits for add/spill, distinguishes memory/disk reset, preserves `spill_error`, rearms the action, processes a reentrant reset spill after releasing records, and publishes `MemoryIdle`. `close` waits, claims `Closing`, detaches trackers and closes/clears records once, marks the action finished, publishes `Closed`, and wakes waiters. Final inner drop repeats cleanup idempotently for callers that omit explicit close.

Fourth, keep `get_chunk` live and no-copy in memory by returning a guard-backed dereferenceable view; disk chunks remain owned. Row materialization appends into caller-owned storage while the records guard exists. Use a clearly named internal owned snapshot only for `RowContainerReader`, whose current chunk must survive another handle spilling between chunks, and a one-row owned scratch chunk for the iterator. Preserve public mutable receivers so a view and a same-handle mutation remain borrow-checker exclusive.

Finally, add deterministic tests. Use a test-only spill-start hook invoked without coordinator or records locks to pause a real spill. This permits reset and close threads to prove they wait and then finish, without scheduler sleeps. Add a counting fallback action, a parent/session allocation spill test, an add reentrancy test, shallow-copy and mid-read-reader tests, kill/error tests, and phase assertions. Then inspect callsites and docs for stale atomic-trigger, no-lock, borrowed-chunk, goroutine-timing, or per-handle-close descriptions.

## Concrete Steps

All edits happen in `/private/tmp/task325-chunk-ee558`. The writer lane does not run Cargo, Go, make, probes, receipt generation/checks, or push. After editing, run only:

    rustfmt --edition 2021 rust/crates/tidb-chunk/src/lib.rs rust/crates/tidb-chunk/src/row_container.rs rust/crates/tidb-chunk/src/row_container_reader.rs rust/crates/tidb-chunk/src/row_container_test_hooks.rs
    rust/scripts/check-source-size.sh
    git diff --check
    rg -n "AtomicBool|triggered|take_trigger|Cow<'|no lock|nothing to clone|spill goroutine|WaitForTest|ShallowCopyWithNewMutex|RowContainer" rust/crates/tidb-chunk/src rust/crates/tidb-executor/src --glob '*.rs'
    git status --short --branch
    git diff --stat
    git diff -- rust/crates/tidb-chunk/EXECPLAN_ROW_CONTAINER.md rust/crates/tidb-chunk/src/row_container.rs rust/crates/tidb-chunk/src/row_container_reader.rs rust/crates/tidb-executor/src/hash_join.rs

The coordinator/root lane should then run, from `rust/`, at least:

    cargo fmt --all -- --check
    cargo test --offline --locked -j12 -p tidb-chunk row_container --lib
    cargo test --offline --locked -j12 -p tidb-chunk row_container_reader --lib
    cargo test --offline --locked -j12 -p tidb-chunk
    cargo clippy --offline --locked -j12 -p tidb-chunk --all-targets -- -D warnings
    cargo check --offline --locked -j12 --workspace --all-targets

Broader workspace tests and rewrite gates remain coordinator-owned.

## Validation and Acceptance

Acceptance requires behavior, not only compilation. A container attached to a parent tracker must spill when an unrelated positive consumption takes that parent over quota after the final add. After the action returns, container memory usage is zero, disk usage is positive, and every original value is readable in order. An action reentered from `List::add` or `List::reset` must return without taking records and the outer operation must synchronously complete the pending spill.

A shallow copy and the original must report the same phase, rows, reset state, and closed state. A `RowContainerReader` that already owns a chunk must read the remaining rows correctly after another handle spills the container. A real spill paused by the test hook must keep reset/close waiting; releasing it must let each operation publish its terminal phase and wake/join every waiter. A kill panic during spill must become `spill_error`, make later reads/adds fail, and remain stored across reset/re-spill. A fallback must not run on the first trigger, must wait during spill, and may run on a later trigger only when that trigger's tracker still exceeds quota.

No coordinator guard may coexist with a records guard or disk/tracker/fallback call. Static review must find no old atomic-trigger path, borrowed chunk escaping records, stale “no lock/nothing to clone” documentation, or `Drop for RowContainer` explicit close.

## Idempotence and Recovery

Formatting, source-size, diff checks, and searches are safe to rerun. Test temp files are owned by existing `DataInDiskByRows` cleanup. A panic in add/reset/close settles the phase and resumes after locks are normally released; a panic in spill is converted to the stored spill error; a fallback panic releases its coordinator claim. If a static check fails, edit the owning source rather than suppressing the check. If compilation later exposes a direct-consumer type mismatch, migrate that callsite to the guard-backed view or the explicitly scoped snapshot as its lifetime requires; do not reintroduce a bare borrow escaping records.

To abandon this tranche before commit, restore only the files named in this plan from `dcdd211a26ceeaa1a0fb49a06e6373768a1098e3`; do not reset the whole worktree because unrelated user work may exist. Once committed, ordinary `git revert <checkpoint>` is the recoverable rollback.

## Artifacts and Notes

Baseline evidence:

    git rev-parse HEAD
    dcdd211a26ceeaa1a0fb49a06e6373768a1098e3

    git status --short --branch
    ## codex/task325-util-chunk-package-v1

The old Rust action only set `AtomicBool triggered`; `RowContainer::add` alone called `take_trigger()` and `spill_to_disk()`. Therefore a quota action invoked after the final add had no execution path to the records. This is the root failure class, not a missing condition in `add`.

Final writer-lane evidence:

    rustfmt --edition 2021 rust/crates/tidb-chunk/src/lib.rs rust/crates/tidb-chunk/src/row_container.rs rust/crates/tidb-chunk/src/row_container_reader.rs rust/crates/tidb-chunk/src/row_container_test_hooks.rs
    # exit 0

    rust/scripts/check-source-size.sh
    source-size ratchet: OK

    git diff --check
    # exit 0

The exhaustive stale-shape scan found no implementation/callsite `set_status`, `broadcast`, `take_trigger`, lifetime-parameterized `RowContainerReader`, `in_memory_row`, or per-handle `Drop for RowContainer`. Remaining matches in this plan describe the removed baseline failure or the scan command itself.

## Interfaces and Dependencies

`RowContainer` remains the public type in `tidb_chunk::row_container`. It is `Clone`; `shallow_copy(&self) -> RowContainer` returns an `Arc` clone. `new`, `mem_tracker`, `disk_tracker`, `action_spill`, `add`, `spill_to_disk`, `reset`, `close`, counts, spill state, chunk retrieval, and caller-owned row materialization remain recognizable. Baseline public mutation/lifecycle methods retain `&mut self`; the weak action route uses the shared inner directly. `Drop` exists only on the final shared inner, never on a handle.

`SpillDiskAction` remains `ActionOnExceed + Send + Sync` and retains `status`, priority, fallback, and finished behavior. Its weak shared-state reference prevents the tracker action chain from owning a closed container forever.

`RowContainerRecord` continues to own exactly one `List`, an optional `DataInDiskByRows`, and an optional spill error. `List` remains the sole authority for memory accounting; this tranche does not alter `List::add`, `reset`, or `clear`. `DataInDiskByRows` remains the sole authority for row spill format and disk accounting.

`RowContainerReader` owns a shallow container handle and an optional owned snapshot so its current chunk can survive a mid-read spill by another handle. This snapshot is the remaining non-production/cross-consumer ownership seam; direct `get_chunk` callers retain the live view. `Iterator4RowContainer` borrows the handle and owns only the one-row scratch chunk needed across its records guard. Neither returns a bare row borrow through the lock.

Revision note (2026-08-09): created the plan after mapping the authoritative Go flow, Rust seed, tracker reentrancy, ownership boundaries, tests, and direct consumers. It records the accepted shared-state phase architecture and the static-only writer-lane boundary. Updated it after implementation/review to narrow panic catching to spill, move automatic cleanup to the final shared inner, preserve `spillError`, serialize reset/fallback generations, restore public mutable receivers, keep the live `get_chunk` view, and name the reader snapshot seam.
