# Preserve whole-column identity in `tidb-chunk`

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current. It follows `PLANS.md` at the repository root.

## Purpose / Big Picture

TiDB executors use more than the values stored in a chunk column. `pkg/util/chunk` can place one column owner in several slots, move that owner between chunks, and rebuild every alias when a projected column is swapped. After this change the Rust `tidb-chunk` crate preserves those observable ownership semantics: a write through any aliased slot is visible through every sibling, cross-chunk references remain live after the source variable is dropped, `Prune` reuses owners rather than copying values, and allocator or global-pool reuse never publishes one live owner twice.

The source authority is accepted Go commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`, especially `pkg/util/chunk/chunk.go` (`Prune`, `MakeRef`, `MakeRefTo`, `swapColumn`, `SetCol`), `chunk_util.go` (`ColumnSwapHelper`), `chunk_test.go`, `chunk_util_test.go`, and `alloc_test.go::TestNoDuplicateColumnReuse`. The Rust worktree is `/private/tmp/task325-chunk-ee558`, based on `3012bf210eaa33f215be4fda06db5ded39ed1f94`.

This plan preserves semantic equality, not Go runtime implementation details. It does not reproduce pointer addresses, garbage-collector reachability, Go slice growth, allocator size classes, or incidental panic order.

## Progress

- [x] (2026-08-09) Read root `PLANS.md`, the prior packed-byte ownership ExecPlan, the accepted Go implementation/tests, and current Rust `Chunk`, `Row`, codec/spill, MutRow, pool, and allocator ownership paths.
- [x] (2026-08-09) Chose a lazy owned-or-shared whole-column slot so ordinary columns remain borrowed and lock-free.
- [x] (2026-08-09) Added the private slot/guard authority and migrated `Chunk`, `Row`, MutRow, codec, spill, and bulk-copy callers inside `tidb-chunk`.
- [x] (2026-08-09) Implemented `Prune`, `MakeRef`, `MakeRefTo`, `SetCol`, alias-class-preserving same-chunk/cross-chunk swaps, and `ColumnSwapHelper`.
- [x] (2026-08-09) Redesigned allocator registration as provenance attached to the allocated owner, independent of the chunk's current slot positions.
- [x] (2026-08-09) Added Go-derived boundary tests, including a real-`MakeRef` pass-after form of the coordinator's fail-before mutation-visibility vector.
- [x] (2026-08-09) Ran exact-file rustfmt, source-size validation, and `git diff --check`; the coordinator ran every runtime and consumer gate.
- [x] (2026-08-09) Prepared one static-clean `tidb-chunk` checkpoint; its final SHA is recorded in the handoff rather than self-referenced here.
- [x] (2026-08-09) Corrected global-pool return bucketing to use each unique owner's physical width after aliases can displace a differently typed slot, and added a mixed fixed-8/fixed-40 regression.

## Surprises & Discoveries

- Observation: the Rust workspace currently has no direct `tidb-chunk::Chunk::column` or `column_mut` caller outside this crate.
  Evidence: a repository-wide Rust call-site scan found only `tidb-chunk` methods; similarly named calls elsewhere belong to unrelated schema/codec types. A guard return type therefore does not force this checkpoint to edit executor or expression crates.

- Observation: returning a borrowed cell from a shared whole-column slot would require the column lock guard to escape with the cell view.
  Evidence: `Column::get_bytes` returns a `ColumnBytes` borrowing the `Column`; a temporary `ColumnRead` guard cannot be dropped first. The shared slot path must copy the one cell into an owned `ColumnBytes`, while the ordinary owned slot continues returning the existing borrowed/guard-backed view.

- Observation: the current allocator zips allocation-time registration flags with whatever columns remain in the chunk at drop time.
  Evidence: `AllocatedChunk::drop` calls `drain_columns_for_allocator()` and zips that vector with `column_registrations`. `MakeRef` can duplicate one current slot and displace another original, so positional recycling can lose the displaced owner and enqueue one shared identity twice.

- Observation: holding a source guard while acquiring a destination guard is unsafe even when the two call-site indices differ.
  Evidence: whole-column aliases can make those indices the same `RwLock`, and two helpers can otherwise acquire different shared owners in reverse order. Cell append, decoder transfer, MutRow row assignment, and shallow-copy paths now prepare bytes/metadata under the source guard, release it, and only then acquire the destination guard.

- Observation: allocator provenance can remain lazy without a separate allocation ledger.
  Evidence: an `OwnedColumn` carries its optional generation-checked recycle registration directly. Promotion moves both the `Column` and that registration into one `SharedOwner`; the final owner drop recycles once. This also preserves displaced originals and cross-chunk aliases without putting ordinary allocator chunks behind locks.

- Observation: the initial direct-consumer gate exhausted the shared build volume rather than finding a code defect.
  Evidence: after inactive target data was removed, the coordinator reran the direct-consumer command with `CARGO_TARGET_DIR=/private/tmp/task325-chunk-gates-7e6/rust/target`; it and the subsequent workspace check passed.

- Observation: a surviving owner's slot index is not type authority after `MakeRef`.
  Evidence: with fields `[LongLong, NewDecimal]`, aliasing the decimal owner into slot 0 displaces the 8-byte owner and leaves one unique 40-byte owner in slots 0 and 1. At checkpoint `c58baacbac`, the coordinator's isolated `pool::tests::put_chunk_buckets_aliased_owner_by_physical_width` fail-before test exited 101 because the next integer slot had physical width 40 instead of 8. Bucketing by `fields[0]` had corrupted the fixed-8 pool. `take_columns_for_reuse` therefore returns owner values without historical indices, and `Pool::put_chunk` selects the bucket from `Column::type_size()`.

## Decision Log

- Decision: represent each chunk slot privately as `ColumnSlot::Owned(OwnedColumn)` or `ColumnSlot::Shared(Arc<SharedOwner>)`, where both owner wrappers contain the `Column` and its optional recycle provenance, promoting only when an alias is requested.
  Rationale: ordinary construction, reads, and writes retain the current lock-free `Column` path. Stable shared ownership is introduced only at `Prune`/`MakeRef`/`MakeRefTo` boundaries, where cross-chunk identity requires it.
  Date/Author: 2026-08-09, Codex.

- Decision: expose public `ColumnRead` and `ColumnWrite` guards that dereference to `Column`, while keeping `ColumnSlot` private.
  Rationale: method-call ergonomics stay idiomatic and safe for both variants without making column fields internally synchronized or extending a lock guard unsafely.
  Date/Author: 2026-08-09, Codex.

- Decision: use separate same-chunk and cross-chunk single-column swap implementations.
  Rationale: Go can pass the same pointer twice. Safe Rust cannot construct two simultaneous mutable references to one `Chunk`; a dedicated same-chunk implementation preserves the behavior without unsafe aliasing.
  Date/Author: 2026-08-09, Codex.

- Decision: deep `Clone`, `CopyConstruct`, and selected copies break whole-column identity deliberately.
  Rationale: these APIs promise independent data. Only explicitly named reference/prune operations retain owner identity. Duplicated source slots are copied per slot, matching Go `CopyConstruct`'s loop.
  Date/Author: 2026-08-09, Codex.

- Decision: attach allocator recycling provenance to the column owner itself; do not clone or promote every allocator column at allocation time.
  Rationale: an owned slot stays lock-free. Replacing an owned slot drops that owner into its originating allocator exactly once; promotion moves the provenance into the shared owner, whose final drop performs the same recycle. Cross-chunk aliases and swaps carry provenance with the owner, and a live alias prevents final-owner recycling naturally. This is Rust ownership enforcement of the source's no-duplicate-reuse contract, not GC emulation.
  Date/Author: 2026-08-09, Codex.

- Decision: exact selection failures use `The selection vector of Chunk is not nil. Please file a bug to the TiDB Team` and validate both chunks before mutation.
  Rationale: this string and the no-mutation failure boundary are package-observable source behavior.
  Date/Author: 2026-08-09, Codex.

## Outcomes & Retrospective

The checkpoint now preserves TiDB's observable whole-column identity inside `tidb-chunk` without rebuilding Go pointers, garbage collection, capacity behavior, or allocator size classes. New chunks stay on the existing owned, borrowed, lock-free path. `Prune`, `MakeRef`, `MakeRefTo`, `SetCol`, and swap/helper operations promote only identities that actually acquire aliases. Shared mutations remain visible across slots and chunks, source drop does not invalidate an alias, and deep-copy APIs deliberately produce independent columns.

Allocator safety is tied to the owner rather than the slot position. A displaced registered owner is recycled when that owner dies; a promoted owner carries one registration across every `Arc` alias; a live cross-chunk alias prevents premature reset; and generation checks reject stale returns. Owner destruction occurs outside the allocator-state lock, avoiding recycle-on-drop reentrancy. The global pool extracts one unique owner per identity and skips an identity that still has an external alias.

Global-pool width selection now follows that same owner-based rule. Slot metadata is used only for the existing field-count assertion; every unique reusable column is returned according to its physical `type_size`. A regression aliases a 40-byte decimal owner over an 8-byte integer slot, proves the pool receives zero fixed-8 owners and exactly one fixed-40 owner, and proves the next mixed-width chunk has independent physical widths 8 and 40.

The new identity suite contains 13 tests for mutation visibility, cross-chunk lifetime, exact selection errors and atomicity, duplicate/reordered prune metadata, `SetCol`, deep copies, per-slot memory accounting, complete alias-class swaps, helper mapping/cache/concurrency behavior, and duplicate codec/spill slots. Allocator filters run 9 tests, including no-duplicate reuse, displaced originals, and live cross-chunk aliases. The coordinator's post-amend full crate run passed 157 unit tests plus two compare tests and two probe tests, 161 total; the full workspace test and doctest run also completed with exit 0.

This is intentionally a `tidb-chunk` checkpoint, not the whole `pkg/util/chunk` completion claim. No executor or expression implementation was changed, no receipt was advanced, and no claim is made for unported Go runtime implementation details. Direct consumers and every workspace target compile against the new guard/handle surface, which closes the current integration seam without expanding this tranche.

## Context and Orientation

`rust/crates/tidb-chunk/src/column.rs` stores one physical column. Its packed data bytes already have a separate lazy alias mechanism for `MutRow::ShallowCopyPartialRow`; this plan does not restructure `Column` or conflate that data-only alias with whole-column identity.

`rust/crates/tidb-chunk/src/chunk.rs` previously stored `Vec<Column>` and now stores private `Vec<ColumnSlot>`. A whole-column alias means two or more indices designate the same mutable `Column` owner, including across two chunks. A slot identity is therefore distinct from equality of column values.

`rust/crates/tidb-chunk/src/alloc.rs` returns an `AllocatedChunk` lease. A registration is decided when a column is allocated, but the old implementation recycled from the lease's current slot vector. Registration now follows the allocated owner itself, so later reference replacement cannot change what is eligible for recycling.

`rust/crates/tidb-chunk/src/chunk_util.rs` owns bulk chunk helpers and is the natural home of `ColumnSwapHelper`. The helper caches a mapping after it first sees the runtime identity graph of its input chunk: mappings for input indices that are aliases are merged, one owner is swapped to the first output, and all remaining outputs reference that owner.

## Plan of Work

Add `src/column_slot.rs` with the private owned/shared enum, read/write guards, lazy alias creation, stable identity comparison, deep copy, and unique extraction. Locks recover poisoned guards consistently with the rest of this crate. `ColumnSlot` remains crate-private; `ColumnRead` and `ColumnWrite` are public because they appear in `Chunk::column` signatures.

Change `Chunk.columns` to `Vec<ColumnSlot>` and wrap every constructor input. Convert scalar and mutation operations to `read()`/`write()` guards. For operations that can read and write the same aliased identity, deep-snapshot the source before acquiring the destination write guard. Add `Prune`, `MakeRef`, `MakeRefTo`, `SetCol`, and both swap shapes. `Prune` takes `&mut self` because safe lazy promotion changes the source slot representation while preserving its owner. `SetCol` accepts and returns a public `ColumnHandle`, an opaque stable owner transfer type, rather than exposing `Arc<RwLock<Column>>`.

Extend `ColumnBytes` with an owned variant. `Row::get_bytes` and `get_raw` ask `Chunk` for a cell; owned slots use the existing borrowed view, shared slots copy only that cell while holding a read guard. Scalar accessors hold the read guard only for the getter call.

Migrate direct internal users in MutRow, codec, spill serialization/deserialization, and bulk-copy helpers. Serialization iterates every slot, so duplicate aliases produce duplicate encoded columns. Deserialization writes every slot in image order; when duplicate destination slots designate one identity, each block is applied sequentially just as repeated Go pointers are.

Implement `ColumnSwapHelper` with `OnceLock<HashMap<usize, Vec<usize>>>`. Detect identity classes from the first input chunk, merge all output lists under the leftmost representative, validate selection state before any swap, move the owner into the first destination, and alias the remaining destinations.

Change allocator allocation to attach a generation-checked recycle registration only to admitted owners. `Owned` carries that registration without synchronization; lazy promotion moves it into the one `SharedOwner`. Owner drop queues the raw column into its originating allocator once. Lease drop clears owners before taking the allocator-state lock for its reusable shell, avoiding a reentrant lock when owner drops enqueue columns. Add tests proving displaced originals are not lost, aliases are not duplicated in reuse, and a live cross-chunk reference retains its value across allocator reset.

## Concrete Steps

All edits occur in `/private/tmp/task325-chunk-ee558` and use `apply_patch`.

The coordinator has already captured fail-before evidence at baseline `3012bf210e`:

    cargo test --offline --locked -j12 -p tidb-chunk chunk::tests::duplicated_column_slot_shares_mutations

The baseline exits 101 because the destination clone remains `7` instead of observing `42`. The pass-after test will use the real `Chunk::make_ref` API.

The sole-writer lane may run only:

    rustfmt --edition 2021 rust/crates/tidb-chunk/src/{alloc.rs,chunk.rs,chunk_in_disk.rs,chunk_util.rs,codec.rs,column.rs,column_slot.rs,column_view.rs,lib.rs,mutrow.rs,pool.rs,row.rs,row_in_disk.rs,chunk_identity_tests.rs}
    rustfmt --edition 2021 --check rust/crates/tidb-chunk/src/{alloc.rs,chunk.rs,chunk_in_disk.rs,chunk_util.rs,codec.rs,column.rs,column_slot.rs,column_view.rs,lib.rs,mutrow.rs,pool.rs,row.rs,row_in_disk.rs,chunk_identity_tests.rs}
    rust/scripts/check-source-size.sh
    git diff --check

Those static commands passed. The largest touched production source is `chunk.rs` at 1,504 lines, below the 2,200-line source-size cap.

The coordinator ran these commands from `/private/tmp/task325-chunk-ee558/rust`; all passed:

    cargo test --offline --locked -j12 -p tidb-chunk --no-run
    cargo test --offline --locked -j12 -p tidb-chunk chunk_identity_tests --lib -- --nocapture
    cargo test --offline --locked -j12 -p tidb-chunk allocator_ --lib -- --nocapture
    cargo test --offline --locked -j12 -p tidb-chunk
    cargo clippy --offline --locked -j12 -p tidb-chunk --all-targets -- -D warnings
    CARGO_TARGET_DIR=/private/tmp/task325-chunk-gates-7e6/rust/target cargo check --offline --locked -j12 -p tidb-expr -p tidb-executor -p tidb-exec -p tidb-server -p tidb-session --tests
    cargo check --offline --locked -j12 --workspace --all-targets
    cargo test --offline --locked -j12 --workspace
    cargo test --offline --locked -j12 --workspace --quiet

The two focused results were 13/13 identity tests and 9/9 allocator-filtered tests. The post-amend full crate result was 161 tests across its unit, compare, and probe surfaces. The final quiet workspace test command completed with exit 0, including all workspace tests and doctests. The first direct-consumer attempt stopped with `ENOSPC`; it produced no semantic failure, and the isolated-target rerun shown above passed after inactive build data was cleaned.

The mixed-width global-pool correction was found during final root review after the initial executable gates. The sole-writer lane ran only the permitted formatting, source-size, and diff checks for that amendment; the coordinator then reran the executable gates successfully, ending with `cargo test --offline --locked -j12 --workspace --quiet` at exit 0.

## Validation and Acceptance

Focused tests must demonstrate all of the following observable behavior:

- `make_ref` and `make_ref_to` preserve identity and immediate mutation visibility, including after the source binding is dropped.
- selection errors match the accepted source string and leave both chunks unchanged.
- `prune` preserves selection, virtual-row, capacity, required-row, and incomplete metadata; duplicate indices produce duplicate owner slots.
- same- and cross-chunk swaps preserve complete alias classes for owner/reference, reference/reference, reference/other, and self swaps.
- `set_col` returns no old handle for the same identity and returns the displaced handle for a different identity.
- `Clone`, `copy_construct`, and selected copies are deep and break identity.
- memory usage is summed per slot even for one shared owner.
- codec and spill images contain one column block per slot and restore duplicate-alias values correctly.
- `ColumnSwapHelper` merges two mapped input indices when their slots alias and makes all mapped outputs one identity.
- allocator/pool reuse never publishes the same identity twice, never loses an eligible displaced original, and never resets a still-live cross-chunk alias.
- zero-column initialized and literal-default states retain their existing reset/renew behavior.

Runtime acceptance requires the focused `tidb-chunk` tests, strict crate clippy, direct-consumer checks, workspace check/test, rustfmt, source-size, and diff gates to pass under the coordinator. Static-only success from this lane is insufficient.

## Idempotence and Recovery

All static checks are safe to rerun. Lazy promotion is internal and deterministic. No generated artifacts or dependency files are hand-edited. If a runtime gate identifies a consumer seam, repair the slot boundary in this crate rather than adding an unsafe lifetime extension or universal locking to `Column`.

## Artifacts and Notes

The accepted source error text is:

    The selection vector of Chunk is not nil. Please file a bug to the TiDB Team

The key source regression is issue 29554's `TestNoDuplicateColumnReuse`: `MakeRef` can make two slots designate one owner, so reuse must deduplicate identities rather than assume slots are owners.

## Interfaces and Dependencies

The intended core interfaces are:

    enum ColumnSlot {
        Owned(OwnedColumn),
        Shared(Arc<SharedOwner>),
    }

    struct OwnedColumn {
        column: Column,
        recycle: Option<ColumnRecycleRegistration>,
    }

    struct SharedOwner {
        column: RwLock<Column>,
        recycle: Mutex<Option<ColumnRecycleRegistration>>,
    }

    impl ColumnSlot {
        fn read(&self) -> ColumnRead<'_>;
        fn write(&mut self) -> ColumnWrite<'_>;
        fn alias(&mut self) -> ColumnSlot;
        fn same_identity(&self, other: &ColumnSlot) -> bool;
        fn deep_copy(&self) -> ColumnSlot;
        fn into_unique_column(self) -> Result<Column, Box<ColumnSlot>>;
    }

    impl Chunk {
        pub fn column(&self, index: usize) -> ColumnRead<'_>;
        pub fn column_mut(&mut self, index: usize) -> ColumnWrite<'_>;
        pub fn prune(&mut self, used: &[usize]) -> Chunk;
        pub fn make_ref(&mut self, source: usize, destination: usize);
        pub fn make_ref_to(
            &mut self,
            destination: usize,
            source: &mut Chunk,
            source_index: usize,
        ) -> Result<(), &'static str>;
        pub fn set_col(&mut self, index: usize, column: ColumnHandle) -> Option<ColumnHandle>;
    }

`std::sync::{Arc, Mutex, RwLock, OnceLock}` are sufficient; no new crate dependency is required. Locking is confined to promoted whole-column aliases and their recycle provenance. No unsafe code, Go runtime carrier, or allocator-policy emulation is introduced.

Revision note (2026-08-09): initial plan created for the whole-column identity and allocator-safety tranche after the user clarified that observable semantics, not Go standard-library mechanics, are the goal.

Revision note (2026-08-09): completed the implementation, updated the allocator decision from a positional ledger to owner-attached provenance, recorded the no-nested-lock rule and the exact green coordinator/static evidence, and bounded the outcome to this `tidb-chunk` checkpoint.

Revision note (2026-08-09): made physical column width, rather than a displaced slot's field index, authoritative for global-pool return bucketing and added the fixed-8/fixed-40 alias regression.
