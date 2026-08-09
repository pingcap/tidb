# Preserve package-observable `Column` ownership semantics in `tidb-chunk`

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current. It follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/util/chunk` is TiDB's packed columnar row container. This checkpoint preserves the behavior its Rust callers can observe: values and nulls, deep-copy isolation, byte-cell mutation, `MutRow`'s shallow data alias, safe typed mutation, codec/disk round trips, and the public `MemoryUsage` formula.

The first design attempted to reproduce Go slice headers, allocator size classes, nil headers, and `growslice` capacity transitions. The user corrected the goal to semantic equivalence at the package boundary, not a reimplementation of the Go runtime. This revision therefore removes the general `GoSlice<T>` abstraction. Bitmap, offset, and fixed-element scratch storage remain ordinary Rust `Vec`s. Only packed data bytes can be shallow-aliased, through a small byte-specific abstraction that retains a lock-free owned path and promotes to shared storage on demand.

The source authority is accepted Go commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`, especially `pkg/util/chunk/column.go`, `column_test.go`, `mutrow.go`, `codec.go`, and their callers. The Rust checkpoint began at `152d3ecf450ce485c98ce281c90ea24743145901` in `/private/tmp/task325-chunk-ee558`.

## Progress

- [x] (2026-08-09) Read the accepted Go `column.go` and `column_test.go`, the previous `GoSlice` prototype, current Rust consumers, this plan, and root `PLANS.md`.
- [x] (2026-08-09) Recorded the user correction: package-observable semantics are REQUIRED; Go-runtime implementation details are DECLINED.
- [x] (2026-08-09) Deleted `go_slice.rs` and restored `Vec<u8>` bitmap, `Vec<i64>` offsets, and `Option<Vec<u8>>` element scratch storage.
- [x] (2026-08-09) Added byte-specific `SharedBytes` with an owned `Vec<u8>` fast path, lazy promotion for a shallow alias, guarded reads, and Rust-native detachment on growth.
- [x] (2026-08-09) Replaced escaping byte slices with public guard-backed `ColumnBytes` / `CellBytes` views and migrated direct `tidb-chunk` consumers.
- [x] (2026-08-09) Added borrow-scoped, alignment-safe typed mutation callbacks for integers, floats, Go durations, decimals, and times; no unsafe casts and no lock held over caller code.
- [x] (2026-08-09) Made `MutRow::shallow_copy_partial_row` take a mutable source and install data-only aliases, allowing ordinary columns to remain lock-free.
- [x] (2026-08-09) Split `column_tests.rs` out of production `column.rs` before the source-size ratchet.
- [x] (2026-08-09) Finished the static call-site/stale-symbol audit, focused semantic tests, formatting, source-size validation, and diff validation.
- [x] (2026-08-09) Prepared one bounded semantic checkpoint; its resulting SHA is reported in the handoff because embedding a commit's own SHA is recursive.
- [x] (2026-08-09) Made shared-byte mutation contention-aware so a live sibling read guard triggers detachment instead of a blocking self-deadlock, including the append initialization path.
- [x] (2026-08-09) Kept `AppendString` byte-authoritative without allocating a transient `GoString`, by accepting `GoStringSource` and copying directly from `as_go_bytes()`.

## Surprises & Discoveries

- Observation: only `Column.data` needs backing alias behavior for this checkpoint. `MutRow.ShallowCopyPartialRow` shares cell data but retains independent bitmap, offsets, scratch buffer, and row count. General sharing of bitmap/offset/scratch storage added runtime machinery without a package-observable need.
  Evidence: accepted `mutrow.go` assigns `dstCol.data = srcCol.data[start:end]` and updates destination metadata separately.

- Observation: a universal `Arc<RwLock<Vec<u8>>>` would tax every expression-row read. Requiring mutable source ownership for alias creation lets `SharedBytes` move an ordinary `Vec` into an `Arc<RwLock<_>>` only at the rare shallow-copy boundary.
  Evidence: the only current Rust `shallow_copy_partial_row` callers are local tests; ordinary `Column` reads dominate the crate's hot paths.

- Observation: a guard-backed cell read and a mutation through a sibling header are valid safe Rust even though they refer to the same promoted allocation. A blocking shared write lock self-deadlocks in that sequence.
  Evidence: `shared_bytes::tests::live_sibling_reader_detaches_mutating_header` pins direct mutation, while `mutrow::tests::shallow_copy_partial_row_matches_go` holds a public cell guard across an append that initializes more backing bytes.

- Observation: a bare `&[u8]` cannot safely escape a potentially shared backing after its read guard drops. A small view that owns either an ordinary borrow or a shared read guard retains slice ergonomics without unsafe lifetime extension.
  Evidence: `ColumnBytes` implements `Deref<Target=[u8]>`, `AsRef<[u8]>`, debug, and useful equality traits.

- Observation: typed mutable Go slices cannot be reproduced by casting packed bytes safely because byte alignment is not guaranteed. Decoding to an aligned temporary inside a callback and writing native-endian bytes back after normal return makes each operation transactional and holds no shared lock across user code; panic discards the staged values.
  Evidence: `with_int64s_mut`, `with_uint64s_mut`, `with_float32s_mut`, `with_float64s_mut`, `with_go_durations_mut`, `with_decimals_mut`, and `with_times_mut` all use one scoped decode/callback/encode operation, and the MutRow alias test observes the committed bytes through a sibling after return.

- Observation: the accepted `GetVectorFloat32` ignores deserializer remainder. A valid vector image with trailing cell bytes therefore succeeds.
  Evidence: `get_vector_float32` now discards the returned remainder and has a focused suffix test.

- Observation: Go `SetRaw` uses `copy`, so it copies `min(input length, cell width)`. Short writes preserve the cell tail, long writes truncate, and empty input changes nothing.
  Evidence: `SharedBytes::copy_from_slice` returns the copied prefix length and the Column test pins all three cases.

- Observation: constructing an owned `GoString` before every append adds an avoidable Arc allocation even though `Column` immediately copies the bytes into packed storage.
  Evidence: `Column::append_string` and `Chunk::append_string` now accept `GoStringSource`; UTF-8 strings, arbitrary byte containers, and existing `GoString` values all expose their bytes without a transient carrier.

## Decision Log

- Decision: DECLINE exact Go-runtime slice emulation and runtime mechanism matching.
  Rationale: exact `growslice` capacities, allocator size classes, spare-capacity bytes, nil-versus-allocated-empty slice headers, internal malformed-state panic order, `sync.Pool` scheduling, and GC reachability mechanisms do not change valid package-level results required by this checkpoint. Rust uses its native `Vec` allocation policy. Observable bounded-reuse, admission, quota, and memory behavior remains a semantic obligation whenever allocator/pool work is in scope.
  Date/Author: 2026-08-09, user correction implemented by Codex.

- Decision: keep `null_bitmap`, `offsets`, and `elem_buf` Rust-native; use `Option<Vec<u8>>` only to distinguish fixed from variable shape, including a zero-width fixed column.
  Rationale: these fields do not need shallow backing aliases. `Option` expresses the logical fixed/variable discriminator without Go nil-header machinery.
  Date/Author: 2026-08-09, Codex.

- Decision: use `SharedBytes::{Owned(Vec<u8>), Shared(Arc<RwLock<Vec<u8>>>), start, len}` only for packed data.
  Rationale: ordinary columns remain lock-free. `share_range(&mut self)` promotes once, and uncontended sibling writes within the existing allocation remain visible. Shared mutation uses `try_write`; lock availability writes through, poison recovers the guard, and contention snapshots this header into owned storage before mutation. This keeps a live reader stable and prevents reentrant self-deadlock without promising behavior for concurrent Go data races.
  Date/Author: 2026-08-09, Codex.

- Decision: change the Rust shallow-copy API to require `&mut Chunk` plus a row index.
  Rationale: safe lazy promotion requires mutable ownership of the source header. Preserving the exact immutable Go call shape would force synchronization overhead onto every column or require unsafe interior mutation.
  Date/Author: 2026-08-09, Codex.

- Decision: return `ColumnBytes` / `CellBytes` guards for `GetBytes` and `GetRaw`, and use callbacks for mutation.
  Rationale: views retain the storage guard for their borrow; callbacks never expose a reference after a guard drops and never retain a write lock across caller code.
  Date/Author: 2026-08-09, Codex.

- Decision: ACCEPT transactional typed and cell mutation callbacks; DECLINE Go's live mutable-slice visibility during the callback.
  Rationale: staged aligned values commit to packed storage only after normal return, so a panic rolls back and siblings see the new bytes after commit. No current production Rust consumer depends on mid-callback visibility; reproducing a live typed slice over potentially unaligned shared bytes would require unsafe casts or runtime-emulation machinery outside the corrected goal.
  Date/Author: 2026-08-09, Codex.

- Decision: `Column::memory_usage` uses the accepted Go 64-bit payload constant `112` plus the current logical backing capacities.
  Rationale: Rust struct layout and synchronization bookkeeping are private implementation details. Capacity values themselves follow Rust's allocator, because exact Go capacity transitions are declined.
  Date/Author: 2026-08-09, Codex.

- Decision: `Clone` and `CopyConstruct` deep-copy packed data; shallow sharing is created only through the explicitly named MutRow operation.
  Rationale: ordinary Rust clone/copy must not silently retain writable backing aliases.
  Date/Author: 2026-08-09, Codex.

## Outcomes & Retrospective

The previous milestone-1 `GoSlice<T>` experiment was useful falsification evidence but is not retained. The corrected checkpoint is smaller: three metadata/scratch fields use standard collections, one byte-specific module handles the only required backing alias, and public byte/typed access is safe. Exact-path `rustfmt` and `rustfmt --check` passed, `rust/scripts/check-source-size.sh` reported `source-size ratchet: OK`, `git diff --check` passed, and the final stale-symbol scan found no `GoSlice` module/import/API reference under `tidb-chunk` source or tests. Production `column.rs` is 1,331 lines, `shared_bytes.rs` is 387, and `column_view.rs` is 85. Cargo, Go tests, clippy, runtime probes, and behavioral execution are intentionally not part of this writer checkpoint and must not be inferred from formatting/static success.

Final review found two Rust-native boundary gaps in that outcome. First, a public read guard could outlive an attempted mutation through a sibling header and make a blocking write lock wait on itself. The contention-aware copy-on-write fallback now covers both ordinary writes and backing initialization during append. Uncontended reset/reappend still writes through the shared allocation; a guarded reader instead retains its stable snapshot while the mutating header detaches. Second, the byte-authoritative string append path no longer constructs an Arc-backed `GoString` merely to copy its bytes into the column. Typed and byte-cell callbacks intentionally commit only after normal return and roll back on panic; this safe transactional boundary replaces, rather than claims, Go live-slice semantics.

Known cross-crate carrier seams remain explicit:

- Go `GetString`, enum names, and set names can alias column bytes; `tidb_datatype::GoString` currently owns/copies its bytes.
- Go `GetVectorFloat32` may expose vector storage backed by the cell; Rust `VectorFloat32` deserialization owns/copies.
- Go stamps arbitrary signed `fillFsp` into `types.Duration`; `MySqlDuration` validates/normalizes and cannot represent the full unchecked domain.
- Go reinterprets arbitrary decimal/time struct bits; `MyDecimal::from_raw_bytes` and `Time::from_go_raw` validate and can reject bit patterns the Go unsafe view would carry.

These are datatype ownership questions outside the sole-writer boundary for `tidb-chunk`; this checkpoint preserves current valid-value behavior and does not claim those alias/raw-domain branches exact.

One dependency-closed integration seam also remains: accepted join/MPP callers pass an immutable `Row` to source-shaped `ShallowCopyPartialRow`, while safe lazy promotion currently requires `MutRow::shallow_copy_partial_row` to borrow a mutable source `Chunk` plus row index. The current Rust workspace has no production caller, so this WIP checkpoint does not reintroduce universal locks and does not claim source-shaped shallow-copy API completion. Future join/MPP ownership or iterator migration is required before whole-package completion.

## Context and Orientation

The changed Rust files are under `rust/crates/tidb-chunk`:

- `src/column.rs`: logical Column shape, packed operations, byte views, typed callbacks, memory accounting.
- `src/shared_bytes.rs`: private owned-or-shared packed byte storage.
- `src/column_view.rs`: public read guard wrappers.
- `src/column_tests.rs`: the split Column unit tests.
- `src/mutrow.rs`: mutable row writes and the explicit shallow-alias boundary.
- `src/row.rs`, `compare.rs`, `codec.rs`, `chunk.rs`, `chunk_in_disk.rs`, and `row_in_disk.rs`: direct guard/storage consumers.

`SharedBytes` is not a Go slice header. `start` and `len` exist only so a shallow cell can address a range in a shared byte allocation. The owned variant is a normal `Vec`; promotion moves it into one `Arc<RwLock<Vec<u8>>>`. Reads return `SharedBytesRead::{Owned(&[u8]), Shared{guard,...}}`. Any operation that needs to exceed the view's available capacity snapshots visible bytes into a new owned `Vec` using Rust's allocator.

## Plan of Work

1. Keep Column metadata and scratch storage on ordinary vectors and migrate every direct operation back to standard slice/vector code.
2. Route only packed data through `SharedBytes`. Snapshot a source before any same-backing destination mutation so no read guard overlaps a write lock.
3. Return guard-backed public byte views and adapt callers that need `&[u8]` by binding the guard and passing `as_ref()`.
4. Provide callback-based cell and typed mutation. Decode before the callback, hold no backing lock while user code runs, and write through before returning.
5. Make MutRow shallow copying explicitly borrow a mutable source, lazily promote the selected data allocation, and test immediate shared visibility plus growth detachment.
6. Pin public semantics: values/nulls, clone isolation, `SetRaw` prefix copy, arbitrary string bytes, vector suffix acceptance, public memory accounting, codec/disk consumer shape, and valid zero-column identity already covered by existing tests.
7. Format exact touched Rust files, check formatting, run the source-size ratchet, run `git diff --check`, commit, and report everything not executed.

## Concrete Steps

All edits occur in `/private/tmp/task325-chunk-ee558`. File changes use `apply_patch`.

The only permitted validation commands for this checkpoint are:

    rustfmt --edition 2021 <exact touched Rust paths>
    rustfmt --edition 2021 --check <exact touched Rust paths>
    rust/scripts/check-source-size.sh
    git diff --check

No Cargo command, Go command, clippy invocation, runtime probe, or test execution is permitted in this writer worktree.

## Validation and Acceptance

Focused semantic tests for this checkpoint are:

- `shared_bytes::tests::aliases_promote_lazily_and_detach_on_growth`
- `shared_bytes::tests::clone_is_an_owned_deep_copy`
- `shared_bytes::tests::ordinary_growth_stays_on_the_owned_vec_fast_path`
- `shared_bytes::tests::live_sibling_reader_detaches_mutating_header`
- `column::tests::memory_usage_uses_the_public_go_payload_constant`
- `column::tests::append_string_accepts_arbitrary_bytes_and_set_raw_copies_to_cell_width`
- `column::tests::guarded_cell_mutation_and_clone_isolation_are_immediate`
- `column::tests::typed_mutation_callbacks_write_through_without_unsafe_casts`
- `column::tests::vector_decoder_accepts_a_valid_image_with_a_suffix`
- `mutrow::tests::shallow_copy_partial_row_matches_go` (including uncontended reset/reappend visibility, guard-held append detachment, typed write-through, and destination growth detachment)

Static acceptance requires every touched Rust path to format cleanly, every production source to remain below the source-size cap, and `git diff --check` to pass. Runtime acceptance is explicitly NOT verified in this checkpoint because this writer-lane assignment prohibits Cargo, Go, and probes; the coordinator/root gate owns Cargo, clippy, and workspace validation.

## Idempotence and Recovery

Formatting and static checks are safe to rerun. `SharedBytes` promotion is deterministic and changes only the affected data field. No destructive reset or checkout command is needed. If a guard-return-type seam exists outside `tidb-chunk`, leave this crate coherent and report the exact consumer instead of editing another crate.

## Interfaces and Dependencies

The concrete core interfaces are:

    struct Column {
        length: usize,
        null_bitmap: Vec<u8>,
        offsets: Vec<i64>,
        data: SharedBytes,
        elem_buf: Option<Vec<u8>>,
        avoid_reusing: bool,
    }

    enum SharedBytesRead<'a> {
        Owned(&'a [u8]),
        Shared { backing: RwLockReadGuard<'a, Vec<u8>>, start: usize, len: usize },
    }

    impl SharedBytes {
        fn read(&self) -> SharedBytesRead<'_>;
        fn share_range(&mut self, start: usize, end: usize) -> SharedBytes;
        fn copy_from_slice(&mut self, range: Range<usize>, source: &[u8]) -> usize;
    }

    struct ColumnBytes<'a> { /* owns SharedBytesRead plus cell bounds */ }
    type CellBytes<'a> = ColumnBytes<'a>;

    impl Column {
        fn get_bytes(&self, row: usize) -> ColumnBytes<'_>;
        fn get_raw(&self, row: usize) -> ColumnBytes<'_>;
        fn with_cell_bytes_mut<R>(&mut self, row: usize, f: impl FnOnce(&mut [u8]) -> R) -> R;
        fn with_int64s_mut<R>(&mut self, f: impl FnOnce(&mut [i64]) -> R) -> R;
        // Corresponding uint64, float32, float64, duration, decimal, and time callbacks.
    }

No dependency crate is edited. In particular, this checkpoint does not use `tidb_datatype::go_runtime`, any Go size-class table, unsafe casts, or lifetime extension.

Revision note (2026-08-09): replaced the exact-Go-runtime growth/header plan with the user-authorized package-semantic design; removed the obsolete GoSlice milestone and recorded its runtime-internal boundary as DECLINED.
