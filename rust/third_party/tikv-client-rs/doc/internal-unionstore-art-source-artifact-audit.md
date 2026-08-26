# `internal/unionstore/art` source-artifact audit

Source of truth: `tikv/client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

Rust toolchain: `nightly-2026-08-22`.

## Atomic inventory

The pinned package contains exactly nine artifacts and 3,474 lines:

| Artifact | Role | Lines | SHA-256 |
| --- | --- | ---: | --- |
| `internal/unionstore/art/art.go` | ART reads/writes, staging, history, flags, handles, limits, cache, and accounting | 682 | `3004135e1c764b31aefdf14456ecab3cb44d7b775acd807ccd53f36943480e5b` |
| `internal/unionstore/art/art_arena.go` | node allocation, typed arena access, free lists, and snapshot references | 211 | `f2f202f0173c4b912d2dccbfa1b8f9b0f30609eff3c27936ca29127747deaca1` |
| `internal/unionstore/art/art_iterator.go` | live forward/reverse, flags-only, seek, and iterator-position traversal | 581 | `173400d971c4c983ae50eb85adedc64513ad1f44368a5a7d3c86f2465acbebc6` |
| `internal/unionstore/art/art_iterator_test.go` | all ten iterator tests | 369 | `3312c3d52515b962b2512e5c70ae8f264d8525408c7c1d6e3a42102ae05aedbf` |
| `internal/unionstore/art/art_node.go` | node4/16/48/256 topology, compressed prefixes, child lookup, and growth | 722 | `1d9627305528360b3cc9a01adb92f74e182c8eb846896145b90f530366e3cdea` |
| `internal/unionstore/art/art_node_test.go` | all ten node/topology tests | 428 | `ed5cbef62f46004ae9f85dfe0371e0b9498cad881e3b514d5f9fe0057f4872b0` |
| `internal/unionstore/art/art_snapshot.go` | immutable checkpoint reads and snapshot iteration | 127 | `efbf90f02ddf1170041f9554bbb4493e0a7ab3f1a57f22d04793ba5bd9c39dc3` |
| `internal/unionstore/art/art_snapshot_test.go` | both snapshot lifetime/concurrency tests | 90 | `bd25dc29914ee2fc85e7ae7245a9c7c262489fef50b2c489e49485796b3834e3` |
| `internal/unionstore/art/art_test.go` | thirteen ordinary tests plus the package benchmark | 264 | `85021217f4be6acbc9e8e76a2c5492c90be9fd604a873d35b34d732c8fc497bb` |

There is no package-local `doc.go`, build-tag/platform variant, generated input
or output, fixture, example, metadata, `OWNERS`, package build file, or leak
harness. The package has 35 ordinary tests, one benchmark, and no subtests.

## Production surface and Rust integration

`src/transaction/art.rs` owns the complete ART contract. The source-default
`MemDb` adapter in `src/transaction/unionstore.rs` exposes that contract to the
authoritative transaction buffer. Parent union-store composition remains an
independent package claim.

| client-go surface | Rust owner and decision |
| --- | --- |
| `New`, `Reset`, write/snapshot sequence numbers | `Art::new`, `reset`, atomic write and snapshot sequences; reset clears entries, values, stages, cache state, and accounting and invalidates every prior reader. |
| `Get`, `GetFlags`, `Set`, `setValue`, `trySwapValue` | ordered entries plus explicit current/history records preserve misses, tombstones, flags-only keys, same-size in-place behavior, key/entry/buffer limit errors, and source cache counters. Buffer-size validation also runs for flags-only writes. |
| `IsStaging`, `Stages`, `Staging`, `Release`, `Cleanup` | a nested stage stack and undo/value log preserve value rollback, non-rollbackable persistent flags, rollbackable flags, durable handles, and source snapshot-sequence invalidation. `MemDb::stages` exposes the source checkpoints. |
| `Checkpoint`, `RevertToCheckpoint`, `RevertVAddr`, `InspectNode`, `InspectStage` | undo indices replace raw value-log addresses. Stage inspection walks the current value-log records in reverse append order and skips obsolete updates, as the source does. |
| `Dirty`, `Len`, `Size` | exact mutation state, live-key count, and logical key-plus-value size, including flags-only keys and tombstones. |
| `Mem`, memory hook, hook query | native owned-allocation accounting replaces arena capacity while preserving change notifications, including value-log release during `DiscardValues`. |
| cache hit/miss counters and last traversed node | one-key lookup cache behavior is preserved using the last key instead of a raw leaf address. |
| `SelectValueHistory` | current and historical values are searched newest first; missing and flags-only keys return `ErrNotExist`. |
| key/value handle methods | stable monotonic `ArtHandle` identities replace arena addresses while preserving key lifetime and value availability. |
| entry-size limit getter/setter | exact key/entry/total-buffer guards; `MemDb::entry_size_limit` exposes the source getter. |
| `DiscardValues` | releases current, historical, and undo value storage; calls the memory hook; preserves key/flag metadata; and invalidates all old/new value readers through a shared epoch while retaining source miss/flags-only branch ordering. |
| `Iter`, `IterReverse`, accessors, handle, and advance | owned bytewise ordered traversal preserves inclusive lower/exclusive upper bounds, value filtering, and write-sequence invalidation. Empty byte slices map to the source's unbounded convention; `Close` is ownership-driven. |
| `IterWithFlags`, `IterReverseWithFlags` | flags-only entries are included in exact forward/reverse order; both are exposed by the parent `MemDb`. |
| `GetSnapshot`, snapshot `Get`, `SnapIter` | per-snapshot owned key/value-log-version views replace arena checkpoints/reference counts. Matching equal-size in-place writes remain visible; appended versions and new keys remain checkpoint-hidden. Stable concurrent traversal, bounds, value-discard invalidation, and idempotent `Next` after completion are preserved. |
| node4/16/48/256, compressed-prefix internals, bitmaps, growth, raw addresses, and free lists | deliberate native mapping to Rust's safe `BTreeMap`. It supplies identical bytewise order, prefix/bound behavior, replacement, growth-visible capacity sequences, minimum-key behavior, and snapshot lifetime without unsafe arena topology. |
| `RemoveFromBuffer` | client-go deliberately leaves this parent test helper as `panic("unimplemented")`; Rust retains the already completed physical removal required by its public authoritative `MemDb` and downstream transaction tests. This is an explicit parent integration decision, not an ART behavioral default. |

Parent `MemDb::UpdateFlags` now follows the exact source adapter path:
`Set(key, nil, ops)` errors are ignored. This avoids panics for oversized or
over-limit flag updates while preserving mutations made before the total-buffer
error. The adapter also exposes reverse flags iteration, entry limits, and stage
checkpoints, matching the already available RBT facade.

## Complete unit-test port

Every original Go test is an independently named Rust case in
`src/transaction/art_source_tests.rs`; the benchmark's functional workload is
also an executable test contract.

| Source artifact | Complete client-go declarations | Rust ports |
| --- | --- | --- |
| `art_test.go` | `TestSimple`, `TestSubNode`, `BenchmarkReadAfterWriteArt`, `TestBenchKey`, `TestLeafWithCommonPrefix`, `TestUpdateInplace`, `TestFlag`, `TestLongPrefix1`, `TestLongPrefix2`, `TestFlagOnlyKey`, `TestSearchPrefixMisatch`, `TestSearchOptimisticMismatch`, `TestExpansion`, `TestDiscardValues` | `source_test_simple`, `source_test_sub_node`, `source_benchmark_read_after_write_art_contract`, `source_test_bench_key`, `source_test_leaf_with_common_prefix`, `source_test_update_inplace`, `source_test_flag`, `source_test_long_prefix_1`, `source_test_long_prefix_2`, `source_test_flag_only_key`, `source_test_search_prefix_mismatch`, `source_test_search_optimistic_mismatch`, `source_test_expansion_native_mapping`, `source_test_discard_values` |
| `art_node_test.go` | `TestAllocNode`, `TestNodePrefix`, `TestOrderedChild`, `TestNextPrevPresentIdx`, `TestLCP`, `TestNodeAddChild`, `TestNodeGrow`, `TestReplaceChild`, `TestMinimumNode`, `TestKey2Chunk` | `source_test_alloc_node_native_storage`, `source_test_node_prefix_native_mapping`, `source_test_ordered_child_native_mapping`, `source_test_next_prev_present_index_native_mapping`, `source_test_lcp_native_mapping`, `source_test_node_add_child_native_mapping`, `source_test_node_grow_native_mapping`, `source_test_replace_child_native_mapping`, `source_test_minimum_node_native_mapping`, `source_test_key_to_chunk_native_mapping` |
| `art_iterator_test.go` | `TestIterateNodeCapacity`, `TestIterSeekLeaf`, `TestMultiLevelIterate`, `TestSeekMeetLeaf`, `TestSeekInExistNode`, `TestSeekToIdx`, `TestIterateHandle`, `TestSeekPrefixMismatch`, `TestIterPositionCompare`, `TestIterSeekNoResult` | `source_test_iterate_node_capacity`, `source_test_iter_seek_leaf`, `source_test_multi_level_iterate`, `source_test_seek_meet_leaf`, `source_test_seek_in_existing_node_native_mapping`, `source_test_seek_to_index_native_mapping`, `source_test_iterate_handle`, `source_test_seek_prefix_mismatch`, `source_test_iter_position_compare_native_mapping`, `source_test_iter_seek_no_result` |
| `art_snapshot_test.go` | `TestSnapshotIteratorPreventFreeNode`, `TestConcurrentSnapshotIterNoRace` | `source_test_snapshot_iterator_prevent_free_node_native_mapping`, `source_test_concurrent_snapshot_iter_no_race` |

The 100,000-key `TestBenchKey` scale and all 100,000 point reads are retained.
The snapshot concurrency port retains the source's 100 concurrent iterators for
the 4-, 16-, and 48-child shapes. Representation-specific node assertions map
to safe-storage allocation, order, prefix, handle, replacement, growth,
minimum, and snapshot-lifetime contracts rather than recreating unsafe layout.

Eleven `source_uncovered_*` tests execute cross-cutting production branches not
isolated by the original package tests. Together with the 36 direct ports, the
focused ART module contains 47 tests.

## Differential findings and fixes

Five source comparisons produced red-then-green regressions:

1. `DiscardValues` only set a Boolean and retained current/history/undo values;
   it emitted no memory update and left old readers usable. Rust now releases
   storage, notifies the hook, and invalidates every value-bearing reader.
2. `InspectStage` returned sorted-key order instead of reverse value-log append
   order and did not model the source's obsolete-record skip.
3. flags-only `Set` skipped the total-buffer limit check even though client-go
   checks it after `setValue` for every successful key insertion.
4. finished snapshot iterator `Next` returned an error; source `SnapIter.Next`
   is idempotently successful after completion.
5. parent `MemDb::UpdateFlags` unwrapped `Set` errors and panicked. Source
   intentionally ignores those errors; Rust now does likewise and covers both
   over-limit mutation retention and oversized-key rejection.

The later atomic parent `internal/unionstore` audit refined snapshot storage
from immutable clones to per-snapshot value-log-version views. This preserves
the child package's new-key and allocator-lifetime tests while also matching
the parent source test in which retained pre-stage readers observe equal-size
in-place updates.

The full matrix then exposed one stale downstream expectation:
`source_schema_filter_callback_and_memory_contracts` expected the pre-commit
memory hook value after commit. Client-go's `DiscardValues` resets the value-log
arena, whose reset calls the hook. The test now asserts the smaller post-commit
node-only footprint.

## Direct client-go consumer

Mechanical exact-import search finds one direct consumer:

- `internal/unionstore/memdb_art.go`, the complete source-default `MemBuffer`
  adapter.

No other pinned Go file imports
`github.com/tikv/client-go/v2/internal/unionstore/art`. Both the complete parent
package suite and its race suite pass, covering all adapter tests rather than a
selected subset.

## Validation

Exact pinned Go package, race, and complete parent-consumer suites:

```text
env GOCACHE=/private/tmp/client-go-art-build-cache \
    GOMODCACHE=/private/tmp/client-go-art-module-cache \
    /private/tmp/go1.25.12/bin/go test \
    ./internal/unionstore/art -count=1

env GOCACHE=/private/tmp/client-go-art-build-cache \
    GOMODCACHE=/private/tmp/client-go-art-module-cache \
    /private/tmp/go1.25.12/bin/go test -race \
    ./internal/unionstore/art -count=1

env GOCACHE=/private/tmp/client-go-art-build-cache \
    GOMODCACHE=/private/tmp/client-go-art-module-cache \
    /private/tmp/go1.25.12/bin/go test ./internal/unionstore -count=1

env GOCACHE=/private/tmp/client-go-art-build-cache \
    GOMODCACHE=/private/tmp/client-go-art-module-cache \
    /private/tmp/go1.25.12/bin/go test -race \
    ./internal/unionstore -count=1
# all passed
```

Focused and parent Rust gates:

```text
cargo +nightly-2026-08-22 test -p tikv-client \
    transaction::art --lib --no-default-features
cargo +nightly-2026-08-22 test -p tikv-client \
    transaction::art --lib --all-features
# 47 passed in each configuration

cargo +nightly-2026-08-22 test -p tikv-client \
    transaction::unionstore::tests --lib --no-default-features
cargo +nightly-2026-08-22 test -p tikv-client \
    transaction::unionstore::tests --lib --all-features
# 18 passed in each configuration
```

Complete matrices and strict gates:

```text
cargo +nightly-2026-08-22 test -p tikv-client source_ --lib \
    --no-default-features --quiet
# 643 passed
cargo +nightly-2026-08-22 test -p tikv-client source_ --lib \
    --all-features --quiet
# 640 passed

cargo +nightly-2026-08-22 test -p tikv-client --lib \
    --no-default-features --quiet
# 940 passed; 1 unrelated intentional ignore
cargo +nightly-2026-08-22 test -p tikv-client --lib \
    --all-features --quiet
# 937 passed; 1 unrelated intentional ignore

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
in-process index. The exact race suites cover source allocator/snapshot
concurrency, and the complete parent package gates cover the sole integration
edge. End-to-end transaction behavior remains owned by its separate completed
differential receipt.
