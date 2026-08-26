# `internal/unionstore/rbt` source-artifact audit

Source of truth: `tikv/client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

Rust toolchain: `nightly-2026-08-22`.

## Atomic inventory

The pinned package contains exactly five artifacts and 1,661 lines:

| Artifact | Role | Lines | SHA-256 |
| --- | --- | ---: | --- |
| `internal/unionstore/rbt/rbt.go` | red-black-tree storage, value history, staging, flags, handles, limits, and accounting | 968 | `6eb048260e540347926c01a99c53e21b7d013d6d4f03f40699148c3bf35cfdbe` |
| `internal/unionstore/rbt/rbt_arena.go` | arena-backed node allocation and reuse | 101 | `2886b19757ea14a61d6c7e761d699a717c939ce74a1fa5039d43d7621deecc10` |
| `internal/unionstore/rbt/rbt_iterator.go` | live forward/reverse and flags-only iteration | 259 | `4097f6f64c765d997a56c2aebff0db8ccde1f3322800fc1690baceebfdfe2268` |
| `internal/unionstore/rbt/rbt_snapshot.go` | checkpoint-backed snapshot reads and iteration | 163 | `b4adcccd48c9cfe78b7ce13c5c17f4c35bea18cde09f8a94cc67c6a440924f79` |
| `internal/unionstore/rbt/rbt_test.go` | all three package unit tests | 170 | `919992613ce39a13c934efeb08f8b0488dc4d5b4b83c641eefe3e631d20c633d` |

There is no package-local `doc.go`, build-tag/platform variant, generated input
or output, fixture, benchmark, example, metadata, `OWNERS`, build file, or leak
harness.

## Production surface and Rust integration

`src/transaction/rbt.rs` owns the complete tree contract. The parent
`RbtMemDb` adapter in `src/transaction/unionstore.rs` exposes that contract to
the authoritative transaction `MemDb`; parent buffer composition and ART/RBT
selection remain owned by the separate `internal/unionstore` claim.

| client-go surface | Rust owner and decision |
| --- | --- |
| `New`, `Reset` | `Rbt::new`, `reset`; both initialize unlimited limits and clear all tree, history, staging, cache, and accounting state. |
| `IsStaging`, `Staging`, `Release`, `Cleanup` | exact nested stage stack in `Rbt`; cleanup reverts values and rollbackable flags while retaining persistent flags and their keys. `RbtMemDb::is_staging` now exposes the source query. |
| `Checkpoint`, `RevertToCheckpoint`, `RevertVAddr`, `InspectStage` | undo-log indices replace raw value-log addresses. Revert restores the same visible history; inspection walks current records in reverse append order and skips obsolete records. |
| `Get`, `SelectValueHistory`, `Set`, `RemoveFromBuffer` | ordered entries plus an undo/value log preserve point reads, tombstones, same-size in-place updates, history selection (including flags-only `ErrNotExist`), physical removal, and source error/limit behavior. |
| `GetFlags`, parent `UpdateFlags` | `flags` and key-based `update_flags`; updates use the parent adapter's exact `Set(key, nil, ops)` path, including dirty state, ignored oversized keys, and the post-`DiscardValues` guard. |
| `GetKeyByHandle`, `GetValueByHandle`, iterator `Handle` | stable monotonic `RbtHandle` identities index live keys and values without exposing arena pointers. |
| `Len`, `Size`, `Dirty` | exact logical entry count, key-plus-value size, and mutation state, including persistent flags-only keys. |
| `DiscardValues` | releases current/history/undo value storage, notifies the memory hook, invalidates old iterators and snapshots, and makes later value access or mutation panic while preserving key metadata and logical size. `RbtMemDb::discard_values` now forwards it. |
| `SetMemoryFootprintChangeHook`, `Mem`, `MemHookSet` | native allocation accounting and hook delivery retain the observable memory-footprint contract; raw Go arena capacity is representation-specific. |
| `GetEntrySizeLimit`, `SetEntrySizeLimit` | exact entry/buffer limit checks; `RbtMemDb::entry_size_limit` now exposes the source getter. |
| `GetCacheHitCount`, `GetCacheMissCount` | one-key lookup-cache counters, now forwarded by `RbtMemDb`. |
| `Iter`, `IterReverse`, iterator accessors/advance/`UpdateFlags` | owned ordered traversal preserves value filtering and bytewise bounds. Source empty seek endpoints are normalized as unbounded in both directions. Mutable iterator flag changes map to the key-based parent operation because a copied Rust iterator cannot retain a mutable tree borrow; `Close` is unnecessary. |
| `IterWithFlags`, `IterReverseWithFlags` | flags-only entries are included with exact forward/reverse bounds; both operations are now exposed by `RbtMemDb`. |
| `GetSnapshot`, `Snapshot.Get`, `SnapshotIter`, `SnapshotIterReverse` | a per-snapshot owned key/value-log-version view replaces checkpoint-plus-arena references. Matching equal-size in-place writes remain visible; appended versions, new keys, and physical removals preserve checkpoint visibility and stable forward/reverse traversal. |
| allocator, node colors/links, rotations, delete fixup, successor/predecessor | deliberate representation mapping to Rust's safe `BTreeMap`. It supplies the same ordered key and deletion behavior without unsafe arena addresses, manual balancing, or free-list reuse. |

## Unit-test port and differential fixes

Every original Go test is an independently named Rust case with the same scale
and assertion boundaries:

| client-go test | Rust test and preserved coverage |
| --- | --- |
| `TestDiscard` | `source_test_discard`: 10,000 base writes, staged replacements, cleanup, exact size, every point read, complete forward/reverse traversal, base cleanup, every miss, and empty forward/reverse/seek behavior. |
| `TestEmptyDB` | `source_test_empty_db`: missing point read and invalid forward, reverse, and seek iterators. |
| `TestFlags` | `source_test_flags`: 10,000 keys, persistent versus rollbackable flag cleanup, exact 5,000 length/20,000-byte size, empty ordinary iteration, exact 5,000 even flags-only keys, updates on all retained/missing keys, and final flag reads. |

Eight `source_uncovered_*` tests make production branches not reached by the
three source tests executable: snapshots and history, checkpoints and stage
inspection, handles, tombstones, limits, hooks, cache counters, reverse bounds,
equal-size updates, discard storage/invalidation, and native flag-update
adaptation. The focused module therefore contains 11 tests total.

Five source comparisons produced red-then-green regressions and production
fixes:

1. `UpdateFlags` previously edited flags directly. A new ordinary key did not
   mark the RBT dirty, an oversized key was inserted, and updating after
   `DiscardValues` did not panic. It now follows source `Set(key, nil, ops)`.
2. an explicit empty reverse upper bound produced an invalid iterator. Source
   tests `len(k) == 0` and seeks to the last key; live, flags-only, and snapshot
   reverse iterators now do the same.
3. `InspectStage` previously returned sorted-key order. It now follows source
   reverse value-log order and omits superseded records, including same-size
   in-place and appended replacement cases.
4. `DiscardValues` previously set only a guard while retaining current,
   historical, and undo-log allocations; it did not notify the memory hook and
   snapshots/history remained readable. It now releases storage and invalidates
   every value reader while preserving key metadata and logical size.
5. `SelectValueHistory` previously returned `Ok(None)` for a flags-only key.
   Source returns `ErrNotExist` before touching the value log; Rust now follows
   that branch exactly.

The later atomic parent `internal/unionstore` audit refined snapshot storage
from immutable clones to per-snapshot value-log-version views. This retains
RBT checkpoint behavior across physical removal while matching the parent
source test for equal-size pre-stage in-place updates.

The parent `RbtMemDb` facade test exercises the newly forwarded flags iterators,
staging state, limits, cache counters, and discard guard. The complete 17-test
Rust unionstore parent suite and the source parent consumer tests pass.

## Direct client-go consumers

Mechanical exact-import search finds one file:

- `internal/unionstore/memdb_rbt.go`, the complete parent `MemBuffer`
  adaptation.

No other pinned Go file imports
`github.com/tikv/client-go/v2/internal/unionstore/rbt`. The targeted parent
consumer gate executes `TestInspectStage`, `TestDirty`, `TestFlags`,
`TestBufferLimit`, `TestSnapshotGetIter`, `TestMemBufferCache`,
`TestSetMemoryFootprintChangeHook`, `TestBatchedSnapshotIter`, and
`TestBatchedSnapshotIterEdgeCase`. Passing those tests validates the package
through its sole direct integration edge without promoting the complete parent
package in this receipt.

## Validation

Exact pinned Go package, race, and parent-consumer suites:

```text
env GOCACHE=/private/tmp/client-go-rbt-build-cache \
    GOMODCACHE=/private/tmp/client-go-rbt-module-cache \
    /private/tmp/go1.25.12/bin/go test ./internal/unionstore/rbt -count=1
# passed

env GOCACHE=/private/tmp/client-go-rbt-race-build-cache \
    GOMODCACHE=/private/tmp/client-go-rbt-module-cache \
    /private/tmp/go1.25.12/bin/go test -race \
    ./internal/unionstore/rbt -count=1
# passed

env GOCACHE=/private/tmp/client-go-rbt-parent-build-cache \
    GOMODCACHE=/private/tmp/client-go-rbt-module-cache \
    /private/tmp/go1.25.12/bin/go test ./internal/unionstore \
    -run '^(TestInspectStage|TestDirty|TestFlags|TestBufferLimit|TestSnapshotGetIter|TestMemBufferCache|TestSetMemoryFootprintChangeHook|TestBatchedSnapshotIter|TestBatchedSnapshotIterEdgeCase)$' \
    -count=1
# passed
```

Focused and parent Rust gates:

```text
cargo +nightly-2026-08-22 test -p tikv-client \
    transaction::rbt::tests::source_ --lib --no-default-features
cargo +nightly-2026-08-22 test -p tikv-client \
    transaction::rbt::tests::source_ --lib --all-features
# 11 passed in each configuration

cargo +nightly-2026-08-22 test -p tikv-client \
    transaction::unionstore::tests --lib --all-features
# 17 passed
```

Complete matrices and strict gates:

```text
cargo +nightly-2026-08-22 test -p tikv-client source_ --lib \
    --no-default-features --quiet
# 598 passed
cargo +nightly-2026-08-22 test -p tikv-client source_ --lib \
    --all-features --quiet
# 595 passed

cargo +nightly-2026-08-22 test -p tikv-client --lib \
    --no-default-features --quiet
# 899 passed; 1 unrelated intentional ignore
cargo +nightly-2026-08-22 test -p tikv-client --lib \
    --all-features --quiet
# 896 passed; 1 unrelated intentional ignore

cargo +nightly-2026-08-22 test --workspace --no-default-features --quiet
cargo +nightly-2026-08-22 check --workspace --all-targets --all-features
cargo +nightly-2026-08-22 clippy --workspace --all-targets \
    --all-features -- -D warnings
env RUSTDOCFLAGS='-Dwarnings --document-private-items' \
    cargo +nightly-2026-08-22 doc --workspace --all-features --no-deps
cargo +nightly-2026-08-22 test --workspace --doc --all-features --quiet
# all passed; 51 doctests

cargo +nightly-2026-08-22 fmt --all -- --check
git diff --check
# passed
```

No live TiKV/PD cluster or unistore fixture is required for this deterministic
in-process storage package. The sole parent integration edge and exact source
race suite provide the relevant concurrency and adapter gates; repository-wide
live behavior remains owned by its completed differential receipt.
