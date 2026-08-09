# Preserve Go column and slice identity in `tidb-chunk`

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root. This plan must be maintained according to that file.

## Purpose / Big Picture

`pkg/util/chunk` is TiDB's columnar batch implementation. The accepted Go implementation stores columns through pointers and stores every byte, offset, and bitmap buffer through Go slice headers. Pointer identity and slice backing identity are observable behavior: `MakeRef` makes two positions see the same column, `MutRow.ShallowCopyPartialRow` makes one destination cell see later writes to a source backing array, and the allocator records the identity of each original column even after chunk slots are rearranged.

The Rust crate currently stores `Chunk.columns` as `Vec<Column>` and each `Column` buffer as an independent `Vec`. That representation deep-copies where Go aliases and cannot implement the original tests without special cases. This plan replaces the representation at its root. After the work, callers can create, reorder, share, mutate, copy, swap, pool, encode, and spill chunks while observing the same identity, capacity, and detachment rules as the accepted Go source. The change is observable through the original Go boundary tests: mutation through a referenced column is visible through every alias; a deep copy breaks aliases; a shallow MutRow cell follows source writes within capacity but detaches on growth; and allocator reset never inserts one column identity twice.

The authority is Go commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`, especially:

- `pkg/util/chunk/column.go`
- `pkg/util/chunk/chunk.go`
- `pkg/util/chunk/chunk_util.go`
- `pkg/util/chunk/mutrow.go`
- `pkg/util/chunk/alloc.go`
- `pkg/util/chunk/pool.go`
- `pkg/util/chunk/codec.go`
- their direct `*_test.go` files

The starting Rust checkpoint is `996e712c676297ea125e569d2224fc9f2ca54ef7`. It is already pushed to `origin/hparser-integration` and `ngaut/hparser-integration`, and it passed `tidb-chunk` tests, `clippy -D warnings`, workspace check, and the full workspace test suite. This plan does not claim the whole Go package is complete; row-container, disk, receipt, probe, and cross-crate work remains after this alias tranche.

## Progress

- [x] (2026-08-09) Audited Go whole-column pointer identity, MutRow data-only shallow aliases, allocator registration, typed mutable views, and consumer blast radius.
- [x] (2026-08-09) Established a green and pushed starting checkpoint at `996e712c67`.
- [x] (2026-08-09) Wrote and committed this living ExecPlan before semantic edits.
- [ ] Prototype the owned/shared slice header and prove within-capacity sharing, growth detachment, independent headers, capacity accounting, and `Send` without unsafe code.
- [ ] Replace `Column`'s four `Vec` fields with the slice-header abstraction while preserving the unshared no-lock path and all existing scalar/vector/codec behavior.
- [ ] Add alignment-safe typed mutable views and port the original mutable-slice tests.
- [ ] Replace `Chunk.columns: Vec<Column>` with stable column slots and port exact reference, prune, set, swap, helper, copy, and selection behavior.
- [ ] Implement MutRow's exact shared-buffer constructors and `ShallowCopyPartialRow` data-only alias semantics.
- [ ] Tie allocator registration to original column-object identity and make pool/codec ownership handle-aware without duplicate recycling.
- [ ] Migrate every in-crate row, iterator, disk, spill, comparator, and codec consumer to guarded/owned access; remove the old direct-`Vec` path.
- [ ] Run scoped package tests and clippy in the coordinator gate worktree, correct every diagnostic, then run workspace check and full workspace tests.
- [ ] Update this plan's outcome evidence and hand the green semantic checkpoint to the package receipt tranche.

## Surprises & Discoveries

- Observation: whole-column sharing and MutRow sharing are different alias layers. `Chunk.columns []*Column` shares the complete `Column`, while `MutRow.ShallowCopyPartialRow` replaces only `dstCol.data` with a sub-slice of a source cell and retains independent destination metadata.
  Evidence: Go `chunk.go:35-54,214-285,689-697`; `mutrow.go:394-414`; `mutrow_test.go:169-194`.

- Observation: a plain `Arc<Column>` or copy-on-write representation is incorrect. Go writes through every column alias, and a Go slice header independently carries start, length, and capacity while sharing its backing array.
  Evidence: Go `TestMakeRefTo`, `TestSwapColumn`, and `TestMutRowShallowCopyPartialRow`; current Rust `chunk.rs` and `mutrow.rs` document the omitted alias behavior.

- Observation: the dependency leaf already contains proven Go 1.25 slice-growth calculations in `tidb-datatype/src/go_runtime.rs::GoSharedSlice`, but direct use would put an `RwLock` on every unaliased chunk access and it does not expose all chunk-specific subheader operations. This crate may use the public capacity helper as the growth authority but must retain an owned fast path.
  Evidence: `GoSharedSlice<T>` always stores `Arc<RwLock<Vec<T>>>`; `Column` access is on expression and executor hot paths.

- Observation: the allocator's source registry is independent of mutable chunk slots. Recording slot metadata and later zipping it with current slots loses overwritten originals and can duplicate aliases after `MakeRef`.
  Evidence: Go `alloc.go:60-76,88-137,168-175,206-235`; `alloc_test.go:122-155`.

- Observation: Rust cannot safely return a bare `&[u8]` from a shared backing after the lock guard is dropped. Byte and typed views need guard-backed return types or callback access.
  Evidence: current `Row::get_bytes` and `Column::get_bytes` return borrowed slices tied only to the chunk borrow.

- Observation: reference operations do not share one validation policy. `MakeRef` and `SetCol` allow selections, while `MakeRefTo` and single-column swap reject even a non-nil empty selection. The latter must use the exact shared error text `The selection vector of Chunk is not nil. Please file a bug to the TiDB Team` and perform the selection check before index access.
  Evidence: accepted `chunk.go` reference/swap methods and the source-derived ordering audit.

- Observation: ordinary per-slot operations deliberately do not deduplicate aliases. `MemoryUsage` counts repeated column slots repeatedly, and `Reset` visits a repeated alias once per slot. Identity deduplication belongs only to allocator recycling and alias-group reconstruction.
  Evidence: accepted `Chunk.MemoryUsage`, `Chunk.Reset`, `TestSwapColumn`, and `TestNoDuplicateColumnReuse`.

## Decision Log

- Decision: model the root as two layers: stable whole-column slots and Go-style slice headers/backings inside each column.
  Rationale: one layer cannot express both complete-column aliases and MutRow's data-only aliases. The two-layer model makes both ordinary rather than special cases.
  Date/Author: 2026-08-09, Codex.

- Decision: keep an owned no-lock fast path and promote only when sharing is requested.
  Rationale: every expression and executor row read is a hot path. Making all chunks pay an `RwLock` cost to support relatively rare aliases would be an avoidable performance regression.
  Date/Author: 2026-08-09, Codex.

- Decision: do not use copy-on-write, pointer-number surrogates, or `Arc::make_mut`.
  Rationale: Go mutation must remain visible through aliases. These alternatives preserve identity labels while silently breaking shared mutation.
  Date/Author: 2026-08-09, Codex.

- Decision: do not use unsafe typed casts over the byte backing.
  Rationale: the crate forbids unsafe code and the shared byte slice may be arbitrarily aligned. Typed mutable views will decode into an aligned temporary and write native-endian bytes back while their column write guard remains held.
  Date/Author: 2026-08-09, Codex.

- Decision: treat the public Go 1.25 capacity helper in `tidb-datatype` as the growth oracle and do not duplicate allocator size-class tables in `tidb-chunk`.
  Rationale: one tested capacity authority prevents silent drift while keeping this unit within its sole `tidb-chunk` ownership boundary.
  Date/Author: 2026-08-09, Codex.

- Decision: implement deep `Chunk` copy manually rather than deriving `Clone` over handles.
  Rationale: Go `CopyConstruct` creates one fresh column object per source slot and breaks all source aliases. Cloning handles would silently preserve them.
  Date/Author: 2026-08-09, Codex.

- Decision: keep allocator origin and recycle-once state outside the modeled Go `Column` payload and carry it with the physical identity handle.
  Rationale: replacing or swapping a slot must neither lose its displaced original nor admit an external column into the wrong allocator. Keeping Rust-only ownership metadata outside the payload also preserves the accepted Go `Column` memory-accounting geometry.
  Date/Author: 2026-08-09, Codex.

- Decision: replace escaping byte borrows with guard-backed views or borrow-scoped callbacks; do not hide aliasing behind unsafe reference extension.
  Rationale: a shared backing can be mutated concurrently and a reference cannot outlive its lock guard. Guard-backed APIs make the lifetime and synchronization contract explicit while retaining the no-lock owned path.
  Date/Author: 2026-08-09, Codex.

- Decision: keep the allocator's live-reset ownership difference explicit until the prototype proves a safe source-shaped handle API.
  Rationale: Go can reuse a raw pointer while an old pointer variable still exists. Safe Rust must not create two unguarded mutable owners. The original source precondition and observable tests determine whether a generation-discard lease is sufficient or whether allocator-created chunks need a separate shared handle.
  Date/Author: 2026-08-09, Codex.

## Outcomes & Retrospective

The plan is established; semantic outcomes are not yet claimed. At each milestone, record the commit SHA, exact tests, any rejected design, and remaining semantic seams here. Completion of this plan means the alias representation and its original tests are green, not that the whole `pkg/util/chunk` package receipt is complete.

## Context and Orientation

The Rust crate is `rust/crates/tidb-chunk`. Its principal types are:

- `src/column.rs::Column`, currently a length plus four independent `Vec` buffers.
- `src/chunk.rs::Chunk`, currently a selection, `Vec<Column>`, row/capacity state, and incomplete-chunk state.
- `src/row.rs::Row`, a borrowed chunk plus physical row index.
- `src/mutrow.rs::MutRow`, an owned one-row chunk.
- `src/alloc.rs`, an ownership-native chunk/column allocator.
- `src/pool.rs`, the global capacity-bucket pool.
- `src/codec.rs`, the response codec and stateful decoder.
- `src/chunk_util.rs`, selected-row copying and `ColumnSwapHelper` ownership.

A column slot is one element of Go's `[]*Column`. Two slots may point to the same column object. A slice header is Go's `(data pointer, length, capacity)` value. Two slice headers may point into the same backing array while having different visible ranges and capacities. Growing one header beyond its capacity allocates and detaches only that header.

The planned Rust types are conceptually:

    enum ColumnSlot {
        Owned(ColumnObject),
        Shared(Arc<RwLock<ColumnObject>>),
    }

    struct ColumnObject {
        length: usize,
        null_bitmap: GoSlice<u8>,
        offsets: GoSlice<i64>,
        data: GoSlice<u8>,
        elem_buf: GoSlice<u8>,
        avoid_reusing: bool,
        origin: Option<ColumnOrigin>,
    }

    enum GoSliceBacking<T> {
        Owned(Vec<T>),
        Shared(Arc<RwLock<Vec<T>>>),
    }

    struct GoSlice<T> {
        backing: GoSliceBacking<T>,
        start: usize,
        len: usize,
        capacity: usize,
    }

The concrete implementation may split these into `column_slot.rs` and `go_slice.rs` to keep production files below the repository's source-size gate. The names may change only if the plan's interfaces and tests are updated at the same time.

`GoSlice` stores fully initialized elements through its header capacity so safe reslicing is possible. Cloning a shared header copies start, length, and capacity. Promoting an owned slice moves its backing into one `Arc<RwLock<_>>` and updates the source header before returning a sibling header. Appends within capacity update the shared backing; growth copies only the visible range into a fresh owned backing using `go_64_next_slice_capacity_for_element` and resets start to zero.

`ColumnSlot` similarly starts owned. `MakeRef`, `MakeRefTo`, `Prune`, `SetCol`, and swap-helper setup promote the relevant object once and clone the shared handle. Reads and writes return enum guards implementing `Deref` and `DerefMut`: the owned variant borrows directly without locking, and the shared variant holds the appropriate `RwLock` guard. Multi-column operations must either snapshot one source or acquire shared objects in stable identity order so self-aliasing cannot deadlock.

Allocator registration follows `ColumnObject`, not its current slot. An origin records the weak allocator state, generation, expected type size, and whether cache quota was reserved. An original allocation can be registered once and enqueued once. Moving or aliasing a slot does not change the origin. Replacing a slot does not lose the original object: its final owning handle triggers or exposes one recycling event. A stale generation cannot be recycled into a new generation while a live alias can still mutate it.

## Plan of Work

Milestone 1 adds a private `go_slice.rs` prototype and direct tests without migrating `Column`. Implement owned/shared backing, promotion, exact subheaders, reslicing, native Go growth capacity, bulk append/copy, clear/truncate, snapshots, guarded visible access, backing identity, and capacity-based accounting. The milestone is accepted when tests prove sibling writes, independent lengths, nonzero-start capacity, within-cap reset/reappend visibility, growth detachment, nil versus allocated-empty state where required, and `Send`.

Milestone 2 migrates `Column` to `GoSlice`. Preserve constructors, reset, reserve, resize, append, copy, reconstruct, codec, memory usage, and `avoidReusing` behavior. Add `ColumnReadBytes`/`CellBytes` guards where bytes cannot escape a shared backing. Port mutable typed views for integers, floats, durations, decimals, and times. Each typed guard holds exclusive column access, exposes an aligned typed vector, and writes native-endian bytes back on drop. The existing scalar getters and all original mutable-view tests must agree.

Milestone 3 introduces `ColumnSlot` and migrates `Chunk`. Implement exact identity APIs and source error order: `Prune`, `MakeRef`, `MakeRefTo`, `SetCol`, single-column `swapColumn`, whole `SwapColumns`, and `ColumnSwapHelper`. Preserve selection restrictions and rebuild all alias groups after swaps. Replace derived `Clone` with a deep-copy implementation that makes every destination slot independent, even when source slots shared one object. Update memory usage to sum every slot's header capacities, including repeated aliases, as Go does.

The milestone must preserve the non-obvious field boundaries. `Prune` deep-copies selection state but shares the requested column identities, including duplicates; nil and empty requested lists both produce initialized zero-column chunks. Whole `SwapColumns` swaps only selection, column slice/header nilness, and virtual-row count; capacity, required rows, and incomplete state stay with their original chunk objects. `SetCol` compares identity before mutation and returns the displaced live handle only when the identity changes. Nil column slots are represented explicitly rather than collapsed into an empty column.

Milestone 4 ports MutRow's backing rules. Fixed MutRow construction makes `elemBuf` and `data` headers over one allocation. Variable MutRow construction makes `data` and `nullBitmap` headers over one allocation. `ShallowCopyPartialRow` installs only a data subheader with source-cell start, length, and `cap(source)-start`; destination length, bitmap, offsets, and element buffer remain independent. Tests mutate the source after reset/reappend within capacity and then force source growth to prove detachment.

Milestone 5 migrates allocator, pool, codec, disk, row, iterator, comparator, and chunk-util consumers. Registration is object-bound and deduplicated by identity. Codec decoded buffers preserve zero-copy backing and `avoidReusing`; pool admission sees the object rather than a slot copy. Borrowed byte APIs become guard-backed or scoped callbacks. Delete obsolete direct-`Vec`, deep-copy shallow-row, slot-zipping allocator, and pointer-identity comments once no references remain.

For codec ownership, add an owned/shareable response-buffer entry point. A lifetime-free `decode(&[u8])` cannot return a mutable zero-copy alias safely, so it remains an explicitly named copying convenience if compatibility requires it. The source-shaped decoder owns a response buffer, installs clipped slice headers, advances start/length/capacity together, and keeps the backing alive through the decoded chunk.

Milestone 6 runs the verification ladder from a clean coordinator gate worktree. First run focused unit filters for the new types and original Go boundary names, then all `tidb-chunk` tests, clippy with warnings denied, workspace check, and full workspace tests. Any failure updates `Surprises & Discoveries` and the relevant milestone before code changes. Only a coherent green checkpoint is pushed to both remotes.

## Concrete Steps

All semantic edits happen in `/private/tmp/task325-chunk-ee558`. The coordinator gates commits in `/private/tmp/task325-chunk-gates-7e6`. Build commands use 12 jobs.

1. Confirm the writer worktree and source authority:

       cd /private/tmp/task325-chunk-ee558
       git status --short
       git rev-parse HEAD
       git show 665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f:pkg/util/chunk/chunk.go

   Expect a clean worktree before milestone 1 and a HEAD descended from `996e712c67`.

2. After each milestone, format only touched Rust files and check the diff:

       rustfmt --edition 2021 <touched-rust-files>
       rustfmt --edition 2021 --check <touched-rust-files>
       git diff --check
       scripts/check-source-size.sh

   Expect every command to exit zero. Split production modules before exceeding the source-size limit.

3. Commit each independently testable milestone. Do not combine receipt artifacts with semantic representation commits.

4. In the gate worktree, switch to the exact commit and run focused package evidence:

       cd /private/tmp/task325-chunk-gates-7e6
       git switch --detach <milestone-sha>
       cd rust
       CARGO_TARGET_DIR=/private/tmp/task325-chunk-gates-target cargo test --offline --locked -j12 -p tidb-chunk --quiet
       CARGO_TARGET_DIR=/private/tmp/task325-chunk-gates-target cargo clippy --offline --locked -j12 -p tidb-chunk --all-targets -- -D warnings

   Expect all tests to pass and clippy to emit no warnings.

5. For a pushable checkpoint, broaden to the workspace:

       CARGO_TARGET_DIR=/private/tmp/task325-chunk-gates-target cargo check --offline --locked -j12 --workspace --all-targets
       CARGO_TARGET_DIR=/private/tmp/task325-chunk-gates-target GOCACHE=/private/tmp/task325-chunk-go-cache cargo test --offline --locked -j12 --workspace --quiet

   The full test command must run outside the filesystem/network sandbox because PD-client mock tests bind localhost. Expect exit zero. A linker `ENOSPC` is an environmental failure; clean only the isolated gate target and rerun rather than changing source.

6. Push only after exact remote verification:

       git push origin HEAD:hparser-integration
       git push ngaut HEAD:hparser-integration
       git ls-remote origin refs/heads/hparser-integration
       git ls-remote ngaut refs/heads/hparser-integration

   Both remote object IDs must equal the gated commit.

## Validation and Acceptance

The representation is accepted only when all of the following observable behaviors are pinned:

1. `MakeRef`, `MakeRefTo`, and `Prune` create exact identity aliases. Mutating through any alias is visible through every peer, including cross-chunk peers.
2. `SetCol` with the same identity returns no old column; replacing it returns the old live identity.
3. Single-column swap preserves every pointer-equal group on both chunks. Selection on either side produces Go's exact error before mutation. `ColumnSwapHelper` merges input/output alias classes once and reproduces the four-output original case.
4. Deep chunk copy breaks all aliases while preserving values, selection, capacity, required rows, virtual rows, and incomplete state exactly as the source method specifies.
5. MutRow shallow copies fixed and variable cells at row zero and nonzero offsets. Source reset/reappend within capacity is visible. Source growth beyond capacity detaches. Destination bitmap/offset metadata remains independent.
6. A shallow data header reports `capacity == source.capacity - cell_start`. Column and chunk memory usage count header capacities per slot and do not deduplicate repeated backing arrays.
7. Mutable typed views write through to scalar getters, `Row`, and every whole-column alias for Int64, Uint64, Float32, Float64, duration, decimal, and time data.
8. Allocator tests prove every original column is registered once, overwritten originals remain reclaimable, no alias identity appears twice in a free list, cross-allocator aliases retain the originating allocator, oversized and `avoidReusing` columns are rejected, and stale generations never reuse a live object.
9. Compile-time assertions prove `Chunk: Send`, allocator implementations are `Send + Sync`, and pool workers can transfer chunks. Concurrency tests prove shared reads/writes and swap-helper setup do not deadlock.
10. Existing vector, codec, disk, row, iterator, executor, and workspace tests remain green. No temporary compatibility path, old `Vec` storage, or false parity comment remains.

The minimum named original Go tests to reproduce are `TestSwapColumn`, `TestMakeRefTo`, `TestMergeInputIdxToOutputIdxes`, `TestNoDuplicateColumnReuse`, the typed mutable-column tests in `column_test.go`, and `TestMutRowShallowCopyPartialRow`. Boundary additions cover same-identity `SetCol`, repeated aliases, selection error order, within-capacity sharing, growth detachment, allocator origin, and Send/Sync.

## Idempotence and Recovery

Formatting, diff checks, source-size checks, and Cargo gates are safe to rerun. Each milestone is a separate commit so a failed prototype can be replaced without resetting unrelated work. Never use `git reset --hard` or overwrite the owner worktree. If a representation prototype proves wrong, record the falsification in `Surprises & Discoveries`, commit or discard only the uncommitted prototype with an explicit patch, and update the Decision Log before trying the next design.

The gate worktree is disposable but source worktrees are not. If disk space is exhausted, run `cargo clean --target-dir` only against `/private/tmp/task325-chunk-gates-7e6/rust/target` or the explicitly named exclusive target. Do not delete source worktrees, receipt artifacts, or another unit's target directory.

If an external crate cannot compile with the guard-backed API and fixing it would require editing that crate, stop at a clean `tidb-chunk` commit and report the exact consumer path and required migration. Do not violate one-owner-per-crate by broadening the edit silently.

## Artifacts and Notes

The starting checkpoint's verified evidence was:

- `cargo test --offline --locked -j12 -p tidb-chunk --quiet`: 120 library tests plus integration/doc targets passed.
- `cargo clippy --offline --locked -j12 -p tidb-chunk --all-targets -- -D warnings`: passed.
- `cargo check --offline --locked -j12 --workspace --all-targets`: passed.
- `cargo test --offline --locked -j12 --workspace --quiet`: passed outside the sandbox with an isolated Go cache.
- `origin/hparser-integration` and `ngaut/hparser-integration`: both resolved to `996e712c676297ea125e569d2224fc9f2ca54ef7`.

The package lockdown inventory already exists under `rust/crates/tidb-chunk/tests/pkg_util_chunk_lockdown`, but its obligations remain unclassified and are intentionally outside this semantic plan. After this plan is complete, the package owner must update the all-verdict receipt rather than inferring parity from green tests.

## Interfaces and Dependencies

This plan may add private modules under `rust/crates/tidb-chunk/src` and re-export only source-shaped public APIs. It may read but must not edit `tidb-datatype` or `tidb-util` in this unit.

Use `tidb_datatype::go_64_next_slice_capacity_for_element` and `GoSliceElementLayout` as the Go 1.25 growth authority if their visibility permits. If the helper is not publicly reachable through the crate root, keep the first prototype's capacity API local and report the dependency export seam rather than copying the size-class table without ownership approval.

The expected core interfaces are:

    enum ColumnRead<'a> {
        Owned(&'a ColumnObject),
        Shared(RwLockReadGuard<'a, ColumnObject>),
    }

    enum ColumnWrite<'a> {
        Owned(&'a mut ColumnObject),
        Shared(RwLockWriteGuard<'a, ColumnObject>),
    }

    impl Deref for ColumnRead<'_> { type Target = ColumnObject; }
    impl Deref for ColumnWrite<'_> { type Target = ColumnObject; }
    impl DerefMut for ColumnWrite<'_> {}

    impl ColumnSlot {
        fn read(&self) -> ColumnRead<'_>;
        fn write(&mut self) -> ColumnWrite<'_>;
        fn share(&mut self) -> ColumnSlot;
        fn ptr_eq(&self, other: &ColumnSlot) -> bool;
        fn deep_copy(&self) -> ColumnSlot;
    }

    impl<T> GoSlice<T> {
        fn len(&self) -> usize;
        fn capacity(&self) -> usize;
        fn share_subheader(&mut self, start: usize, len: usize) -> GoSlice<T>;
        fn with_visible<R>(&self, read: impl FnOnce(&[T]) -> R) -> R;
        fn with_visible_mut<R>(&mut self, write: impl FnOnce(&mut [T]) -> R) -> R;
        fn append_go(&mut self, values: &[T], element_size: usize, layout: GoSliceElementLayout);
    }

The concrete APIs must avoid returning references after a shared guard is dropped. `CellBytes` and typed mutable-view guards own the necessary read/write guard for their entire lifetime. Two-object operations compare stable identities before locking and never acquire the same `RwLock` twice.

Revision note (2026-08-09): initial plan created from the accepted Go source audits and the green `996e712c67` checkpoint. It establishes the root representation, milestone order, and acceptance matrix before semantic edits.
